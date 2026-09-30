import copy
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bootstrap import finalized_member, verify_manifest
from common import RPC, Unavailable, canonical_record
from monitor import Monitor, Remote, Slack
from rotate_logs import rotate
import identity
from dashboard import public_status
from private_deploy import validate_host, SSH


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


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.linux, self.mac = Chain(), Mac()
        self.monitor = Monitor(self.temp.name, receipt(), self.linux, self.mac)

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

    def test_small_peer_propagation_lag_is_healthy_after_all_eligible_heights(self):
        self.mac.height = 28
        self.step()
        self.assertEqual(self.monitor.state["cursor"], 25)
        self.assertTrue(self.monitor.state["caught_up"])
        self.assertIsNotNone(self.monitor.state["healthy_since"])

    def test_confirmed_root_mismatch_latches_until_ack(self):
        self.mac.records[12] = record(12)
        self.mac.records[12]["pools"]["ironwood"]["root"] = "cc" * 32
        self.step()
        self.assertEqual(self.monitor.state["cursor"], 11)
        self.assertIn("confirmed tree state mismatch", self.monitor.state["incidents"])
        evidence = list((Path(self.temp.name) / "incidents").glob("*.json"))
        self.assertEqual(len(evidence), 1)
        saved = json.loads(evidence[0].read_text())
        self.assertEqual(saved["height"], 12)
        self.assertNotEqual(saved["linux"], saved["mac"])
        self.mac.records.clear()
        self.step(60)
        self.assertIn("confirmed tree state mismatch", self.monitor.state["incidents"])
        self.monitor.ack("confirmed tree state mismatch")
        self.assertNotIn("confirmed tree state mismatch", self.monitor.state["incidents"])

    def test_chain_disagreement_three_polls(self):
        self.mac.records[11] = record(11, 100)
        for now in (30, 60, 90):
            self.step(now)
        self.assertIn("persistent chain disagreement", self.monitor.state["incidents"])
        self.assertEqual(self.monitor.state["cursor"], 10)

    def test_reorg_rewinds_and_replays_after_restart(self):
        self.step()
        self.step(60)
        for chain in (self.linux, self.mac):
            for height in range(20, 31):
                chain.records[height] = record(height, 100)
        self.monitor = Monitor(self.temp.name, receipt(), self.linux, self.mac)
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
            Monitor(self.temp.name, changed, self.linux, self.mac)

    def test_coverage_gap_latches(self):
        self.monitor.state["cursor"] = 1020
        self.monitor.state["history"] = {str(h): record(h)["hash"] for h in range(20, 1021)}
        for chain in (self.linux, self.mac):
            chain.height = 1030
            chain.records = {h: record(h, 2000) for h in range(20, 1031)}
        self.step()
        self.assertIn("coverage gap: rebootstrap required", self.monitor.state["incidents"])
        self.assertEqual(self.monitor.state["cursor"], 1020)

    def test_unavailable_three_minutes_then_two_good_samples(self):
        with patch.object(self.mac, "status", side_effect=Unavailable("offline")):
            self.step(30)
            self.step(210)
        self.assertIn("verifier unavailable", self.monitor.state["incidents"])
        self.step(240)
        self.assertIn("verifier unavailable", self.monitor.state["incidents"])
        self.step(270)
        self.assertNotIn("verifier unavailable", self.monitor.state["incidents"])

    def test_gap_breaks_qualification_clock(self):
        self.step()
        self.step(60)
        self.step(200)
        self.assertEqual(self.monitor.state["healthy_since"], 200)
        self.assertFalse(self.monitor.state["qualified"])

    def test_qualification_needs_new_blocks_and_healthy_window(self):
        self.step()
        self.step(60)
        self.monitor.state["healthy_since"] = 0
        self.monitor.state["last_sample"] = 86400
        self.monitor.state["initial_tip"] = 0
        self.linux.height = self.mac.height = 131
        self.monitor.state["cursor"] = 127
        self.monitor.state["history"]["127"] = record(127)["hash"]
        self.step(86430)
        self.assertTrue(self.monitor.state["qualified"])
        self.assertEqual(len([x for x in self.monitor.state["outbox"] if "qualified" in x["text"]]), 1)
        self.step(86460)
        self.assertEqual(len([x for x in self.monitor.state["outbox"] if "qualified" in x["text"]]), 1)

    def test_wrong_binary_and_missing_memory_cannot_qualify(self):
        self.mac.now = 30
        bad = self.mac.status()
        bad["binary_sha256"] = "c" * 64
        with patch.object(self.mac, "status", return_value=bad):
            self.step()
        self.assertIn("unexpected build or configuration", self.monitor.state["incidents"])
        self.assertIsNone(self.monitor.state["healthy_since"])

    def test_outbox_persisted_before_delivery_and_deduplicated(self):
        class Delivery:
            def send(inner, item, now):
                state = json.loads((Path(self.temp.name) / "cursor.json").read_text())
                self.assertEqual(state["outbox"][0]["id"], item["id"])
                return False
        self.monitor.slack = Delivery()
        self.monitor.incident("test", True, 0)
        self.step()
        self.step(60)
        self.assertEqual(len([x for x in self.monitor.state["outbox"] if x["text"].endswith("test")]), 1)
        self.assertEqual(len(self.monitor.state["outbox"]), 1)


class BoundaryTests(unittest.TestCase):
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

    def test_secret_host_rejects_options_and_shell_injection(self):
        self.assertEqual(validate_host("192.0.2.10"), "192.0.2.10")
        for value in ("-oProxyCommand=bad", "host; command", "user@192.0.2.10", "192.0.2.10\n"):
            with self.assertRaises(ValueError):
                validate_host(value)

    def test_ssh_captures_endpoint_bearing_output_and_never_relays_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            environment = {"MAC_VERIFIER_HOST": "192.0.2.10", "MAC_VERIFIER_USER": "operator",
                           "MAC_VERIFIER_SSH_KEY": "fixture-key", "MAC_VERIFIER_KNOWN_HOSTS": "fixture-host"}
            with patch.dict("os.environ", environment):
                ssh = SSH("MAC_VERIFIER_", directory)
            with patch("subprocess.run") as run:
                run.return_value.returncode = 1
                run.return_value.stderr = "192.0.2.10 private diagnostic"
                with self.assertRaises(Unavailable) as error:
                    ssh.run("true")
                self.assertNotIn("192.0.2.10", str(error.exception))
                self.assertTrue(run.call_args.kwargs["capture_output"])
                self.assertIn("StrictHostKeyChecking=yes", run.call_args.args[0])

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

    def test_archive_excludes_identity_and_nonfinalized_state(self):
        self.assertIsNone(finalized_member(tarfile.TarInfo("peers/mainnet.peers")))
        self.assertIsNone(finalized_member(tarfile.TarInfo("non_finalized_state/backup")))
        self.assertEqual(str(finalized_member(tarfile.TarInfo("state/v29/mainnet/CURRENT"))),
                         "state/v29/mainnet/CURRENT")
        for name in ("../state/v29/mainnet/CURRENT", "/state/v29/mainnet/CURRENT"):
            with self.assertRaises(Unavailable):
                finalized_member(tarfile.TarInfo(name))
        symlink = tarfile.TarInfo("state/v29/mainnet/sst")
        symlink.type = tarfile.SYMTYPE
        with self.assertRaises(Unavailable):
            finalized_member(symlink)

    def test_wrong_snapshot_family_rejected(self):
        with self.assertRaises(Unavailable):
            verify_manifest({"network": "mainnet", "snapshot_kind": "archive"})

    def test_slack_workspace_guard_and_429_retry(self):
        slack = Slack("test-fixture", "Ufixture", "Tfixture")
        item = {"id": "fixed", "text": "fixture"}
        with patch.object(slack, "call", return_value={"team_id": "wrong"}) as api:
            self.assertFalse(slack.send(item, 0))
            self.assertEqual(api.call_count, 1)
        slack.channel = "Dfixture"
        rate_limit = urllib.error.HTTPError("https://slack.com", 429, "limited", {"Retry-After": "120"}, io.BytesIO())
        with patch.object(slack, "call", side_effect=rate_limit) as api:
            self.assertFalse(slack.send(item, 60))
            self.assertFalse(slack.send(item, 90))
            self.assertEqual(api.call_count, 1)
        rate_limit.close()
        with patch.object(slack, "call", return_value={"ok": True}) as api:
            self.assertTrue(slack.send(item, 180))
            self.assertEqual(api.call_args[0][1]["client_msg_id"], "fixed")

    def test_identity_definitive_quota_rejection_can_retry_after_resolution(self):
        for code, expected_attempt in [(400, False), (503, True)]:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "identity-receipt.json"
                rejection = urllib.error.HTTPError("https://app.infisical.com", code,
                                                    "fixture", {}, io.BytesIO())
                class API:
                    def json(inner, url, data=None, headers=None, method=None):
                        if data is None:
                            return {"identities": [], "totalCount": 0}
                        raise rejection
                with patch.object(identity, "Transport", return_value=API()), \
                        patch.object(identity, "user_token", return_value="fixture"), \
                        patch.dict("os.environ", {"MAC_VERIFIER_REFERENCE_CIDR": "192.0.2.1/32"}), \
                        patch.object(sys, "argv", ["identity.py", "create", "--receipt", str(path)]):
                    with self.assertRaises(urllib.error.HTTPError):
                        identity.main()
                self.assertEqual(json.loads(path.read_text())["create_attempted"], expected_attempt)
                rejection.close()

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



if __name__ == "__main__":
    unittest.main()
