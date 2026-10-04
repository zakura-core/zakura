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
from common import RPC, Unavailable, canonical_record, public_status, atomic_json
from comparison import Comparison, Remote, publish_result
from rotate_logs import rotate
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "runner"))
from ssh_probe import ancestors


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
        self.assertEqual(json.loads((Path(self.temp.name) / "status.json").read_text())["condition"], "matching")
        self.assertTrue(evidence[0].exists())

    def test_confirmed_mismatch_survives_evidence_and_status_storage_failures(self):
        for at_cursor in (False, True):
            for fault in ("evidence", "audit", "cursor", "status", "all"):
                with self.subTest(at_cursor=at_cursor, fault=fault), tempfile.TemporaryDirectory() as directory:
                    linux, mac = Chain(), Mac()
                    mac.now = 30
                    outcomes = []
                    monitor = Comparison(directory, receipt(), linux, mac,
                                         report=lambda *value: outcomes.append(value))
                    if at_cursor:
                        monitor.step(30)
                        height = monitor.state["cursor"]
                    else:
                        height = 11
                    mac.records[height] = record(height)
                    mac.records[height]["pools"]["ironwood"]["root"] = "cc" * 32
                    def write(path, value, **kwargs):
                        path = Path(path)
                        if (fault == "all" or fault == "evidence" and path.parent.name == "incidents"
                                or fault == "cursor" and path.name == "cursor.json"
                                or fault == "status" and path.name == "status.json"):
                            self.assertEqual(outcomes, [("tree_mismatch", 30)])
                            raise OSError("fixture storage failure")
                        atomic_json(path, value, **kwargs)
                    with patch("comparison.atomic_json", side_effect=write), \
                            patch.object(monitor, "audit", side_effect=OSError("audit") if fault == "audit" else monitor.audit):
                        result = monitor.step(30)
                    self.assertEqual(outcomes, [("tree_mismatch", 30)])
                    self.assertEqual(result["condition"], "tree_mismatch")
                    self.assertEqual(result["compared_through"], height if at_cursor else 10)

    def test_matching_requires_successful_cursor_and_private_status_writes(self):
        self.mac.now = 30
        for method in ("save",):
            with patch.object(self.monitor, method, side_effect=OSError("fixture")):
                self.assertEqual(self.monitor.step(30)["condition"], "unavailable")
        original = atomic_json
        def fail_status(path, value, **kwargs):
            if Path(path).name == "status.json":
                raise OSError("fixture")
            return original(path, value, **kwargs)
        with patch("comparison.atomic_json", side_effect=fail_status):
            self.assertEqual(self.monitor.step(30)["condition"], "unavailable")

    def test_validated_sample_drops_unused_fields_and_rejects_bad_ancestry(self):
        self.mac.now = 30
        sample = self.mac.status()
        sample['ancestor_hashes'] = {'10': 'a' * 64}
        normalized = self.monitor.validate_status(sample, 30)
        self.assertNotIn('memory_free_percent', normalized['resources'])
        self.assertEqual(normalized['ancestor_hashes'], {'10': 'a' * 64})
        for ancestors_sample in ([], {'0': 'a' * 64}, {'10': 'bad'}, {'32': 'a' * 64}):
            with self.subTest(ancestors=ancestors_sample), self.assertRaises(Unavailable):
                self.monitor.validate_status(dict(sample, ancestor_hashes=ancestors_sample), 30)

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
    def test_ancestors_reuse_dashboard_depths_and_tip_race_fails(self):
        rpc = MagicMock()
        rpc.call.side_effect = ["a" * 64] * 5 + ["b" * 64]
        self.assertEqual(ancestors(rpc, {"height": 100, "hash": "b" * 64}),
                         {str(depth): "a" * 64 for depth in (1, 2, 5, 10, 32)})
        self.assertEqual([call.args for call in rpc.call.call_args_list],
                         [("getblockhash", height) for height in (99, 98, 95, 90, 68, 100)])
        rpc.call.side_effect = ["a" * 64] * 5 + ["c" * 64]
        with self.assertRaises(Unavailable):
            ancestors(rpc, {"height": 100, "hash": "b" * 64})
        rpc.call.side_effect = ["a" * 64] * 3 + ["b" * 64]
        self.assertEqual(set(ancestors(rpc, {"height": 9, "hash": "b" * 64})), {"1", "2", "5"})

    def test_public_status_drops_malformed_ancestors_and_private_fields(self):
        identifier = "verifier-" + "a" * 32
        raw = {"verifier": {"ancestor_hashes": {"10": "b" * 64, "host": "192.0.2.10", "2": "bad"}}}
        self.assertEqual(public_status(raw, identifier)["ancestor_hashes"], {"10": "b" * 64})
        for value in (None, [], {"0": "b" * 64}, {"9" * 5000: "b" * 64}):
            self.assertEqual(public_status({"verifier": {"ancestor_hashes": value}}, identifier)["ancestor_hashes"], {})


class BoundaryTests(unittest.TestCase):

    def test_condition_is_the_only_comparison_health_signal(self):
        identifier = "verifier-" + "a" * 32
        for condition in ("matching", "catching_up", "tree_mismatch", "unavailable", "chain_disagreement", "coverage_gap"):
            result = public_status({"condition": condition}, identifier)
            self.assertEqual(result["condition"], condition)
            self.assertTrue(result["alerts_muted"])
            for field in ("caught_up", "error", "incidents", "comparison_healthy"):
                self.assertNotIn(field, result)
        with self.assertRaises(ValueError):
            public_status({"condition": "192.0.2.10"}, identifier)

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
        self.assertNotIn("active_incidents", result)
        self.assertEqual(result["compared_through"], 40)
        self.assertIsNone(result["sample_time"])
        with self.assertRaises(ValueError):
            public_status(private, "192.0.2.10")


    def test_dashboard_tip_fields_are_integer_only(self):
        result = public_status({"verifier": {"tip": {"height": 100}}, "reference": {"height": 105}},
                               "verifier-" + "a" * 32)
        self.assertEqual(result["mac_tip"], 100)
        self.assertNotIn("linux_tip", result)
        for value in ("192.0.2.10", True, -1, None):
            result = public_status({"verifier": {"tip": {"height": value}}, "reference": {"height": value}},
                                   "verifier-" + "a" * 32)
            self.assertIsNone(result["mac_tip"])
            self.assertNotIn("linux_tip", result)


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

    def test_public_status_rejects_non_object_status(self):
        for value in (None, [], "invalid"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                public_status(value, "verifier-" + "a" * 32)


class PublicFileTests(unittest.TestCase):
    def test_file_contains_only_allowlisted_fields_and_is_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = root / "identity.json"
            identity.write_text(json.dumps({"verifier_id": "verifier-" + "a" * 32}))
            target = root / "public.json"
            private = "198.51.100.42 secret-key"
            raw = {"sample_time": 123, "caught_up": True, "condition": "matching",
                   "verifier": {"tip": {"height": 15, "hash": "a" * 64},
                                "receipt": {"host": private}, "resources": {"host": private}},
                   "reference": {"height": 15, "host": private}, "private": private}
            self.assertEqual(publish_result(raw, identity, target, False), "matching")
            content = target.read_text()
            self.assertNotIn(private, content)
            self.assertNotIn("receipt", content)
            self.assertEqual(json.loads(content)["condition"], "matching")
            self.assertEqual(target.stat().st_mode & 0o777, 0o644)
            self.assertFalse(list(root.glob(".pending-*")))
            # A failed write cannot truncate the previously published sample.
            with patch("common.os.replace", side_effect=OSError("fixture")):
                self.assertEqual(publish_result(raw, identity, target, False), "unavailable")
                self.assertEqual(publish_result(dict(raw, condition="tree_mismatch"), identity, target, False), "tree_mismatch")
            self.assertEqual(target.read_text(), content)
            self.assertFalse(list(root.glob(".pending-*")))


if __name__ == "__main__":
    unittest.main()
