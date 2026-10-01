import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import RPC, Unavailable, canonical_record
from comparison import Comparison, Remote, migrate
from rotate_logs import rotate
from status_bridge import public_status
from adapter import fork_anchor


def record(height, fork=0):
    return {"height": height, "hash": f"{height + fork:064x}",
            "pools": {pool: {"root": "ab" * 32, "frontier": "00ff"}
                      for pool in ("sapling", "orchard", "ironwood")}}


def receipt():
    return {"bootstrap_height": 10, "bootstrap_record": record(10), "deployed_at": 0,
            "binary_sha256": "a" * 64, "config_sha256": "b" * 64}


class Chain:
    def __init__(self):
        self.height = 30
        self.records = {}

    def tip(self):
        return {"height": self.height, "hash": self.block(self.height)["hash"]}

    def block(self, height):
        return copy.deepcopy(self.records.get(height, record(height)))


class Mac(Chain):
    def status(self):
        return {"schema_version": 1, "sample_time": self.now, "receipt": receipt(),
                "binary_sha256": "a" * 64, "config_sha256": "b" * 64, "architecture": "arm64",
                "tip": self.tip(), "resources": {"free_disk_bytes": 80 * 10**9,
                                                  "memory_free_percent": 50,
                                                  "node_rss_bytes": 1024**3}}


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.linux, self.mac = Chain(), Mac()
        self.monitor = Comparison(self.temp.name, receipt(), self.linux, self.mac)

    def step(self, now=30):
        self.mac.now = now
        self.monitor.step(now)


    def test_every_height_and_batch_limit(self):
        self.step()
        self.assertEqual(self.monitor.state["cursor"], 26)
        self.step(60)
        self.assertEqual(self.monitor.state["cursor"], 27)
        events = [json.loads(line) for line in (Path(self.temp.name) / "audit.jsonl").read_text().splitlines()]
        self.assertEqual([x["height"] for x in events], list(range(11, 28)))


    def test_reorg_rewinds_and_replays_after_restart(self):
        self.step()
        self.step(60)
        for chain in (self.linux, self.mac):
            for height in range(20, 31):
                chain.records[height] = record(height, 100)
        self.monitor = Comparison(self.temp.name, receipt(), self.linux, self.mac)
        self.step(90)
        self.assertEqual(self.monitor.state["cursor"], 27)
        events = (Path(self.temp.name) / "audit.jsonl").read_text()
        self.assertIn('"event": "reorg"', events)
        self.assertEqual(self.monitor.state["history"]["20"], record(20, 100)["hash"])

    def test_restart_never_changes_receipt_implicitly(self):
        self.step()
        changed = receipt()
        changed["config_sha256"] = "c" * 64
        with self.assertRaises(ValueError):
            Comparison(self.temp.name, changed, self.linux, self.mac)

    def test_coverage_gap_latches(self):
        self.monitor.state["cursor"] = 1020
        self.monitor.state["history"] = {str(h): record(h)["hash"] for h in range(20, 1021)}
        for chain in (self.linux, self.mac):
            chain.height = 1030
            chain.records = {h: record(h, 2000) for h in range(20, 1031)}
        self.step()
        self.assertTrue(self.monitor.state["coverage_gap"])
        self.assertEqual(self.monitor.state["cursor"], 1020)

    def test_mismatch_recovers_but_retains_evidence(self):
        self.mac.records[12] = record(12)
        self.mac.records[12]["pools"]["ironwood"]["root"] = "cc" * 32
        self.step()
        self.assertEqual(self.monitor.state["cursor"], 11)
        self.assertEqual(json.loads((Path(self.temp.name) / "status.json").read_text())["condition"], "tree_mismatch")
        evidence = list((Path(self.temp.name) / "incidents").glob("*.json"))
        self.assertEqual(len(evidence), 1)
        self.mac.records.clear()
        self.step(60)
        self.assertTrue(json.loads((Path(self.temp.name) / "status.json").read_text())["caught_up"])
        self.assertTrue(evidence[0].exists())

    def test_failed_or_wrong_identity_samples_do_not_advance(self):
        for sample in [None, [], {"receipt": {}}, {}]:
            with patch.object(self.mac, "status", return_value=sample):
                self.step()
                self.assertEqual(self.monitor.state["cursor"], 10)
        with patch.object(self.mac, "status", side_effect=Unavailable("private endpoint")):
            self.step()
            self.assertNotIn("private endpoint", (Path(self.temp.name) / "status.json").read_text())

    def test_disagreement_does_not_skip_a_height(self):
        self.mac.records[11] = record(11, 100)
        self.step()
        self.assertEqual(self.monitor.state["cursor"], 10)
        self.assertEqual(json.loads((Path(self.temp.name) / "status.json").read_text())["condition"], "chain_disagreement")

    def test_migration_archives_queue_and_preserves_coverage(self):
        old = dict(self.monitor.state, schema_version=1, outbox=[{"text": "historical message"}],
                   incidents={"coverage gap: rebootstrap required": {}})
        path = Path(self.temp.name) / "cursor.json"
        path.write_text(json.dumps(old))
        migrate(self.temp.name, receipt())
        new = json.loads(path.read_text())
        self.assertNotIn("outbox", new)
        self.assertTrue(new["coverage_gap"])
        self.assertEqual(new["cursor"], old["cursor"])
        self.assertEqual(new["history"], old["history"])
        self.assertEqual(json.loads((path.parent / "legacy-cursor.json").read_text()), old)
        migrate(self.temp.name, receipt())

    def test_racing_tree_read_is_not_recorded_as_confirmed_mismatch(self):
        original = self.mac.block
        calls = 0
        def changing(height):
            nonlocal calls
            value = original(height)
            if height == 12:
                calls += 1
                if calls == 1:
                    value["pools"]["orchard"]["root"] = "cd" * 32
            return value
        with patch.object(self.mac, "block", side_effect=changing):
            self.step()
        self.assertEqual(self.monitor.state["cursor"], 11)
        self.assertFalse((Path(self.temp.name) / "incidents").exists())
        self.step(60)
        self.assertEqual(self.monitor.state["cursor"], 27)

    def test_reorg_search_resumes_after_budget_exhaustion(self):
        self.step()
        self.step(60)
        for chain in (self.linux, self.mac):
            for height in range(15, 31):
                chain.records[height] = record(height, 100)
        original = self.monitor.pair
        calls = 0
        def limited(height):
            nonlocal calls
            calls += 1
            if calls == 4:
                raise Unavailable("time budget")
            return original(height)
        with patch.object(self.monitor, "pair", side_effect=limited):
            self.step(90)
        self.assertEqual(self.monitor.state["cursor"], 27)
        self.assertIn("reorg_search", self.monitor.state)
        self.monitor = Comparison(self.temp.name, receipt(), self.linux, self.mac)
        self.step(120)
        self.assertNotIn("reorg_search", self.monitor.state)
        self.assertEqual(self.monitor.state["history"]["15"], record(15, 100)["hash"])

    def test_transport_budget_prevents_further_requests(self):
        from common import Transport
        transport = Transport(deadline=0)
        with patch.object(transport.opener, "open") as request, self.assertRaises(Unavailable):
            transport.json("http://127.0.0.1:8232")
        request.assert_not_called()


class ForkSampleTests(unittest.TestCase):
    def test_anchor_is_ten_ancestors_back_and_tip_race_fails(self):
        from unittest.mock import Mock
        rpc = Mock()
        rpc.call.side_effect = ["a" * 64, "b" * 64]
        self.assertEqual(fork_anchor(rpc, {"height": 100, "hash": "b" * 64}),
                         {"height": 90, "hash": "a" * 64})
        self.assertEqual(rpc.call.call_args_list[0].args, ("getblockhash", 90))
        rpc.call.side_effect = ["a" * 64, "c" * 64]
        with self.assertRaises(Unavailable):
            fork_anchor(rpc, {"height": 100, "hash": "b" * 64})
        self.assertIsNone(fork_anchor(rpc, {"height": 9, "hash": "b" * 64}))

    def test_bridge_drops_malformed_anchor_and_private_fields(self):
        identifier = "verifier-" + "a" * 32
        result = public_status({"verifier": {"fork_anchor": {"height": 90, "hash": "b" * 64,
                                                               "host": "192.0.2.10"}}}, identifier)
        self.assertEqual(result["fork_anchor"], {"height": 90, "hash": "b" * 64})
        for anchor in (None, [], {"height": True, "hash": "b" * 64}, {"height": 90, "hash": "bad"}):
            self.assertNotIn("fork_anchor", public_status({"verifier": {"fork_anchor": anchor}}, identifier))


class BoundaryTests(unittest.TestCase):

    def test_comparison_health_excludes_muting_but_includes_verification_failures(self):
        base = {"caught_up": True, "error": None, "incidents": {"alert delivery unavailable": {}}}
        identifier = "verifier-" + "a" * 32
        self.assertTrue(public_status(base, identifier)["comparison_healthy"])
        self.assertTrue(public_status(base, identifier)["alerts_muted"])
        for change in [{"caught_up": False}, {"error": "unexpected build or configuration"},
                       {"incidents": {"confirmed tree state mismatch": {}}}]:
            with self.subTest(change=change):
                self.assertFalse(public_status({**base, **change}, identifier)["comparison_healthy"])

    def test_dashboard_allowlist_excludes_private_identity_everywhere(self):
        private = {"host": "192.0.2.10", "error": "ssh to 192.0.2.10 failed",
                   "verifier": {"receipt": {"os": "private-host.local", "peer_id": "secret-peer"}},
                   "incidents": {"host-private": {"message": "private-host.local"}},
                   "coverage_start": 11, "compared_through": 40, "qualified": True,
                   "pending_alerts": 0, "sample_time": "192.0.2.10"}
        result = public_status(private, "verifier-" + "a" * 32)
        encoded = json.dumps(result)
        for forbidden in ("192.0.2.10", "private-host", "secret-peer", "receipt", "error"):
            self.assertNotIn(forbidden, encoded)
        self.assertEqual(result["active_incidents"], 1)
        self.assertEqual(result["compared_through"], 40)
        self.assertIsNone(result["sample_time"])
        with self.assertRaises(ValueError):
            public_status(private, "192.0.2.10")


    def test_dashboard_tip_fields_are_integer_only(self):
        result = public_status({"verifier": {"tip": {"height": 100}}, "reference": {"height": 105}},
                               "verifier-" + "a" * 32)
        self.assertEqual((result["mac_tip"], result["linux_tip"]), (100, 105))
        for value in ("192.0.2.10", True, -1, None):
            result = public_status({"verifier": {"tip": {"height": value}}, "reference": {"height": value}},
                                   "verifier-" + "a" * 32)
            self.assertIsNone(result["mac_tip"])
            self.assertIsNone(result["linux_tip"])


    def test_missing_pool_and_malformed_hex(self):
        value = record(10)
        del value["pools"]["ironwood"]
        with self.assertRaises(Unavailable):
            canonical_record(value, 10)
        value = record(10)
        value["pools"]["orchard"]["frontier"] = "0"
        with self.assertRaises(Unavailable):
            canonical_record(value, 10)

    def test_hex_comparison_is_decoded(self):
        value = record(10)
        value["pools"]["orchard"]["root"] = "AB" * 32
        self.assertEqual(canonical_record(value, 10), record(10))

    def test_rpc_chain_race(self):
        rpc = RPC("http://127.0.0.1:8232")
        r = record(10)
        tree = {"hash": r["hash"], "height": 10,
                **{p: {"commitments": {"finalRoot": d["root"], "finalState": d["frontier"]}}
                   for p, d in r["pools"].items()}}
        with patch.object(rpc, "call", side_effect=[r["hash"], tree, record(11)["hash"]]):
            with self.assertRaises(Unavailable):
                rpc.block(10)


    def test_rotation_preserves_open_child_descriptor_and_bounds_backups(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "child.log"
            with path.open("a") as child:
                for _ in range(8):
                    child.write("diagnostic\n")
                    child.flush()
                    rotate(path, 1)
                child.write("still writing\n")
                child.flush()
            self.assertEqual(path.read_text(), "still writing\n")
            self.assertEqual(len(list(Path(directory).glob("child.log.*"))), 4)


class MalformedBoundaryTests(unittest.TestCase):
    def test_rpc_rejects_non_object_info_and_tree_state(self):
        rpc = RPC("http://127.0.0.1:8232")
        for value in (None, [], "invalid"):
            with self.subTest(value=value), patch.object(rpc, "call", return_value=value):
                with self.assertRaises(Unavailable):
                    rpc.tip()
            with self.subTest(value=value), patch.object(rpc, "call", side_effect=["aa" * 32, value]):
                with self.assertRaises(Unavailable):
                    rpc.block(10)

    def test_bridge_rejects_non_object_status(self):
        for value in (None, [], "invalid"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                public_status(value, "verifier-" + "a" * 32)


if __name__ == "__main__":
    unittest.main()
