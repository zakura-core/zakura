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
from alert_preflight import prepare as prepare_alerts
from common import RPC, Unavailable, canonical_record
from monitor import Monitor, Remote, ChannelWebhook
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


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.linux, self.mac = Chain(), Mac()
        self.monitor = Monitor(self.temp.name, receipt(), self.linux, self.mac)

    def step(self, now=30):
        self.mac.now = now
        self.monitor.step(now)

    def test_malformed_status_becomes_incomplete_coverage(self):
        for sample in (None, [], {"resources": []}):
            with self.subTest(sample=sample), patch.object(self.mac, "status", return_value=sample):
                self.step()
                self.assertIn("coverage incomplete", self.monitor.state["incidents"])
                self.assertFalse(self.monitor.state["caught_up"])
                self.assertEqual(self.monitor.state["cursor"], 10)

    def test_malformed_remote_blocks_become_incomplete_coverage(self):
        for sample in (None, [], "invalid"):
            with self.subTest(sample=sample), patch.object(self.mac, "block", side_effect=lambda h: canonical_record(sample, h)):
                self.step()
                self.assertIn("coverage incomplete", self.monitor.state["incidents"])
                self.assertEqual(self.monitor.state["cursor"], 10)

    def test_every_height_and_batch_limit(self):
        self.step()
        self.assertEqual(self.monitor.state["cursor"], 26)
        self.step(60)
        self.assertEqual(self.monitor.state["cursor"], 27)
        events = [json.loads(line) for line in (Path(self.temp.name) / "audit.jsonl").read_text().splitlines()]
        self.assertEqual([x["height"] for x in events], list(range(11, 28)))

    def test_observation_compares_but_never_qualifies_without_alert_delivery(self):
        self.monitor.observation_only = True
        self.monitor.state["qualified"] = True
        self.step()
        self.step(60)
        self.assertEqual(self.monitor.state["cursor"], 27)
        self.assertIn("alert delivery unavailable", self.monitor.state["incidents"])
        self.assertIsNone(self.monitor.state["healthy_since"])
        self.assertFalse(self.monitor.state["qualified"])
        self.assertEqual(len(self.monitor.state["outbox"]), 1)

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

    def test_unavailable_three_minutes_then_three_good_samples(self):
        with patch.object(self.mac, "status", side_effect=Unavailable("offline")):
            self.step(30)
            self.step(210)
        self.assertIn("verifier unavailable", self.monitor.state["incidents"])
        self.step(240)
        self.assertIn("verifier unavailable", self.monitor.state["incidents"])
        self.step(270)
        self.assertIn("verifier unavailable", self.monitor.state["incidents"])
        self.step(300)
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
        self.monitor.state["healthy_start_height"] = 0
        self.linux.height = self.mac.height = 131
        self.monitor.state["cursor"] = 127
        self.monitor.state["history"]["127"] = record(127)["hash"]
        self.step(86430)
        self.assertTrue(self.monitor.state["qualified"])
        self.assertEqual(len([x for x in self.monitor.state["outbox"] if "qualified" in x["text"]]), 1)
        self.step(86460)
        self.assertEqual(len([x for x in self.monitor.state["outbox"] if "qualified" in x["text"]]), 1)

    def test_qualification_does_not_count_blocks_before_healthy_window(self):
        self.step()
        self.step(60)
        self.monitor.state["healthy_since"] = 0
        self.monitor.state["last_sample"] = 86400
        self.monitor.state["healthy_start_height"] = 100
        self.linux.height = self.mac.height = 202
        self.monitor.state["cursor"] = 198
        self.monitor.state["history"]["198"] = record(198)["hash"]
        self.step(86430)
        self.assertFalse(self.monitor.state["qualified"])
        self.linux.height = self.mac.height = 203
        self.step(86460)
        self.assertTrue(self.monitor.state["qualified"])

    def test_wrong_binary_and_missing_memory_cannot_qualify(self):
        self.mac.now = 30
        bad = self.mac.status()
        bad["binary_sha256"] = "c" * 64
        with patch.object(self.mac, "status", return_value=bad):
            self.step()
        self.assertIn("unexpected build or configuration", self.monitor.state["incidents"])
        self.assertIsNone(self.monitor.state["healthy_since"])

    def test_malformed_resource_values_cannot_start_a_healthy_window(self):
        invalid = {
            "free_disk_bytes": (True, -1, "80000000000", float("inf")),
            "node_rss_bytes": (True, -1, 0, "1024", float("nan")),
            "memory_free_percent": (True, -1, 101, "50", float("nan")),
        }
        for field, values in invalid.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    self.mac.now = 30
                    sample = self.mac.status()
                    sample["resources"][field] = value
                    with tempfile.TemporaryDirectory() as directory:
                        monitor = Monitor(directory, receipt(), self.linux, self.mac)
                        with patch.object(self.mac, "status", return_value=sample):
                            monitor.step(30)
                        self.assertIsNone(monitor.state["healthy_since"])
                        self.assertFalse(monitor.state["qualified"])
                        self.assertTrue(monitor.state["incidents"])

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


class ChannelDeliveryTests(unittest.TestCase):
    def client(self):
        return ChannelWebhook("https://hooks.slack.com/services/fixture")

    def test_success_requires_slack_acknowledgement(self):
        client = self.client()
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"ok"
        with patch.object(client.opener, "open", return_value=response) as send:
            self.assertTrue(client.send({"id": "fixture", "text": "qualification fixture"}, 30))
            self.assertEqual(send.call_args.kwargs["timeout"], 10)
        response.__enter__.return_value.read.return_value = b"rejected"
        with patch.object(client.opener, "open", return_value=response):
            self.assertFalse(client.send({"text": "fixture"}, 60))
            self.assertEqual(client.retry_at, 120)

    def test_rate_limit_and_retry_are_bounded(self):
        client = self.client()
        error = urllib.error.HTTPError("fixture", 429, "rate limited", {"Retry-After": "9000"}, None)
        with patch.object(client.opener, "open", side_effect=error) as send:
            self.assertFalse(client.send({"text": "fixture"}, 30))
            self.assertEqual(client.retry_at, 3630)
            self.assertFalse(client.send({"text": "fixture"}, 60))
            self.assertEqual(send.call_count, 1)

    def test_network_failure_remains_pending(self):
        client = self.client()
        with patch.object(client.opener, "open", side_effect=OSError("fixture")):
            self.assertFalse(client.send({"text": "fixture"}, 30))
        self.assertEqual(client.retry_at, 90)

    def test_rejects_untrusted_destinations(self):
        for url in ("http://hooks.slack.com/services/fixture", "https://example.com/services/fixture", "https://user@hooks.slack.com/services/fixture"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                ChannelWebhook(url)

    def test_redirect_cannot_forward_an_incident(self):
        import http.server
        import threading
        requests = []
        class Redirect(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                requests.append(self.path)
                self.send_response(302)
                self.send_header('Location', '/forwarded')
                self.end_headers()
            def do_GET(self):
                requests.append(self.path)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'ok')
            def log_message(self, *args):
                pass
        server = http.server.HTTPServer(('127.0.0.1', 0), Redirect)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            client = self.client()
            # Substitute only the fixture server after production destination validation.
            client.url = f'http://127.0.0.1:{server.server_port}/incident'
            self.assertFalse(client.send({'text': 'fixture incident'}, 30))
            self.assertEqual(requests, ['/incident'])
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)


class GroupedChannelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.monitor = Monitor(self.temp.name, receipt(), Chain(), Mac(),
                               ChannelWebhook("https://hooks.slack.com/services/fixture"))

    def test_one_alert_and_one_recovery_for_multiple_findings(self):
        m = self.monitor
        m.incident("disk below 20 GB", True, 0)
        m.channel_episode(0, {})
        m.incident("memory pressure", True, 30)
        m.channel_episode(30, {})
        self.assertEqual(len(m.state["outbox"]), 1)
        m.incident("disk below 20 GB", False, 60)
        m.incident("disk below 20 GB", False, 90)
        m.incident("disk below 20 GB", False, 120)
        m.channel_episode(90, {})
        self.assertEqual(len(m.state["outbox"]), 1)
        m.incident("memory pressure", False, 120)
        m.incident("memory pressure", False, 150)
        m.incident("memory pressure", False, 180)
        m.channel_episode(180, {})
        self.assertEqual(len(m.state["outbox"]), 2)
        self.assertIn("recovered", m.state["outbox"][1]["text"])

    def test_availability_and_quorum_forks_are_owned_by_fleet(self):
        m = self.monitor
        for name in ("coverage incomplete", "verifier unavailable", "verifier stalled", "persistent chain disagreement"):
            m.incident(name, True, 0)
        m.channel_episode(300, None)
        m.channel_episode(300, {})
        self.assertEqual(m.state["outbox"], [])

    def test_coverage_requires_three_minutes_and_a_live_status(self):
        m = self.monitor
        m.incident("coverage incomplete", True, 0)
        m.channel_episode(179, {})
        m.channel_episode(180, None)
        self.assertEqual(m.state["outbox"], [])
        m.channel_episode(180, {})
        self.assertEqual(len(m.state["outbox"]), 1)

    def test_missing_status_cannot_clear_an_active_coverage_episode(self):
        m = self.monitor
        m.incident("coverage incomplete", True, 0)
        m.channel_episode(180, {})
        m.channel_episode(210, None)
        self.assertEqual(len(m.state["outbox"]), 1)
        m.incident("coverage incomplete", False, 240)
        m.incident("coverage incomplete", False, 270)
        m.incident("coverage incomplete", False, 300)
        m.channel_episode(300, {})
        self.assertEqual(len(m.state["outbox"]), 2)

    def test_episode_survives_restart_without_duplicate_page(self):
        m = self.monitor
        m.incident("disk below 20 GB", True, 0)
        m.channel_episode(0, {})
        m.save()
        restarted = Monitor(self.temp.name, receipt(), Chain(), Mac(), m.slack)
        restarted.channel_episode(30, {})
        self.assertEqual(len(restarted.state["outbox"]), 1)

    def test_historical_observation_notifications_are_coalesced(self):
        old = Monitor(self.temp.name, receipt(), Chain(), Mac(), observation_only=True)
        old.notify("Zakura Mac verifier: alert delivery unavailable")
        old.notify("Zakura Mac verifier recovered: coverage incomplete")
        old.save()
        current = Monitor(self.temp.name, receipt(), Chain(), Mac(), self.monitor.slack)
        self.assertEqual(current.state["outbox"], [])
        self.assertIsNone(current.state["healthy_since"])


class QuietEnablementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.mac = Mac()
        self.monitor = Monitor(self.temp.name, receipt(), Chain(), self.mac, observation_only=True)
        for now in (30, 60, 90, 120, 150, 180):
            self.mac.now = now
            self.monitor.step(now)
        anchor = {"height": 20, "hash": "a" * 64}
        self.fleet = {"network": "mainnet", "last_poll": 180, "rows": [{"name": "zakura-mac-os", "health": "healthy", "fork_anchor": anchor}]
                      + [{"name": "other-" + str(n), "health": "healthy", "fork_anchor": anchor} for n in range(12)]}

    def test_read_only_preflight_reports_zero_messages_without_network_or_mutation(self):
        before = (Path(self.temp.name) / "cursor.json").read_bytes()
        with patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden")):
            report = prepare_alerts(self.temp.name, receipt(), 181, self.fleet)
        self.assertTrue(report["allowed_to_enable"])
        self.assertEqual(report["pending_messages_on_enable"], 0)
        self.assertEqual((Path(self.temp.name) / "cursor.json").read_bytes(), before)

    def test_apply_archives_stale_messages_and_resets_window_without_delivery(self):
        with patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden")):
            report = prepare_alerts(self.temp.name, receipt(), 181, self.fleet, apply=True)
        state = json.loads((Path(self.temp.name) / "cursor.json").read_text())
        self.assertGreater(report["historical_messages_to_archive"], 0)
        self.assertEqual(state["outbox"], [])
        self.assertEqual((Path(self.temp.name) / "cursor.json").stat().st_uid, __import__("os").getuid())
        self.assertIsNone(state["healthy_since"])
        self.assertNotIn("alert delivery unavailable", state["incidents"])
        self.assertTrue(list((Path(self.temp.name) / "incidents").glob("enablement-*.json")))

    def test_stale_insufficient_samples_and_latched_incident_block_enablement(self):
        for kind in ("stale", "samples", "incident", "overflow", "unknown queue"):
            with self.subTest(kind=kind):
                state = copy.deepcopy(self.monitor.state)
                status_path = Path(self.temp.name) / "status.json"
                status = json.loads(status_path.read_text())
                if kind == "samples": status["enablement_good_samples"] = 4
                if kind == "incident": state["incidents"]["confirmed tree state mismatch"] = {"latched": True}
                if kind == "overflow": state["outbox_overflow"] = True
                if kind == "unknown queue": state["outbox"].append({"text": "unknown", "id": "fixture"})
                (Path(self.temp.name) / "cursor.json").write_text(json.dumps(state))
                status_path.write_text(json.dumps(status))
                with self.assertRaises(Unavailable):
                    prepare_alerts(self.temp.name, receipt(), 241 if kind == "stale" else 181, self.fleet)
                status_path.write_text(json.dumps({**status, "enablement_good_samples": 5}))

    def test_missing_quorum_or_wrong_identity_blocks_enablement(self):
        bad = copy.deepcopy(self.fleet)
        for row in bad["rows"][1:5]:row.pop("fork_anchor")
        with self.assertRaises(Unavailable):
            prepare_alerts(self.temp.name, receipt(), 181, bad)
        wrong = receipt();wrong["config_sha256"] = "c" * 64
        with self.assertRaises(Unavailable):
            prepare_alerts(self.temp.name, wrong, 181, self.fleet)

    def test_fleet_preflight_preserves_other_nodes_and_refuses_pending_batches(self):
        path = Path(self.temp.name) / "fleet.json"
        fleet_state = {"nodes": {"mainnet/zakura-mac-os": {"alerting": True}, "mainnet/other": {"alerting": True}},
                       "mac_forks": {"mainnet": {"alerting": True}}, "pending_delivery": {"mainnet": {"messages": ["fixture"]}}}
        path.write_text(json.dumps(fleet_state))
        with self.assertRaises(Unavailable):
            prepare_alerts(self.temp.name, receipt(), 181, self.fleet, True, path)
        fleet_state["pending_delivery"] = {}
        path.write_text(json.dumps(fleet_state))
        prepare_alerts(self.temp.name, receipt(), 181, self.fleet, True, path)
        current = json.loads(path.read_text())
        self.assertEqual(current["nodes"], {"mainnet/other": {"alerting": True}})
        self.assertEqual(current["mac_forks"], {})

    def test_wrong_dashboard_schema_network_and_stale_poll_are_rejected(self):
        for changes in ({"network": "testnet"}, {"last_poll": 0}, {"rows": None}):
            with self.subTest(changes=changes), self.assertRaises(Unavailable):
                prepare_alerts(self.temp.name, receipt(), 181, {**self.fleet, **changes})

    def test_flap_gap_and_duplicate_samples_cannot_produce_recovery(self):
        m = self.monitor
        m.incident("fixture", True, 0)
        for now in (30, 30, 60):m.incident("fixture", False, now)
        self.assertIn("fixture", m.state["incidents"])
        m.incident("fixture", True, 70)
        for now in (90, 120, 240, 270):m.incident("fixture", False, now)
        self.assertIn("fixture", m.state["incidents"])
        m.incident("fixture", False, 300)
        self.assertNotIn("fixture", m.state["incidents"])


if __name__ == "__main__":
    unittest.main()
