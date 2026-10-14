#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import io
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from unittest import mock


SCRIPT_PATH = Path(__file__).with_name("zakura-cluster-status.py")
SPEC = importlib.util.spec_from_file_location("zakura_cluster_status", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
status = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = status
SPEC.loader.exec_module(status)


def node(name: str = "node-a"):
    return status.Node(
        name=name,
        ssh_string=f"root@{name}",
        probe_kind="zebra",
        service_name="zakurad",
        bin_path="/usr/local/bin/zakurad",
        log_file="",
        rpc_listen_addr="127.0.0.1:8232",
        rpc_auth="cookie",
        rpc_config_path="/tmp/cookie",
        rpc_user="",
        rpc_password="",
        process_pattern="",
        container_name="",
        node_id="",
    )


def collector(network: str = "testnet"):
    return status.ClusterCollector(
        [node()],
        interval=10,
        stale_after=300,
        network=network,
    )


def public_row(
    *,
    name: str = "node-a",
    height: int = 4_201_000,
    block_hash: str = "a" * 64,
    balance_zat: str = "123456789012345",
    observed_at: float = 1_000,
    network: str = "testnet",
    client_name: str = "zakurad",
    client_version: str = "1.0.4-rc0+g061af8a",
    healthy: bool = True,
):
    return {
        "name": name,
        "healthy": healthy,
        "height": height,
        "block_hash": block_hash,
        "rpc_chain": status.RPC_CHAIN_NAMES[network],
        "rpc_testnet": network == "testnet",
        "ironwood_chain_balance_zat": balance_zat,
        "client_name": client_name,
        "client_version": client_version,
        "last_seen_at": observed_at,
        "rpc_metadata_error": None,
    }


class MacCraneliftTests(unittest.TestCase):
    def read(self, payload):
        with mock.patch.object(Path, "open", return_value=io.BytesIO(json.dumps(payload).encode())):
            return status.mac_cranelift_status()

    def test_existing_dashboard_does_not_publish_private_endpoint_or_diagnostics(self):
        payload = {"verifier_id": "verifier-" + "a" * 32, "sample_time": time.time(),
                   "host": "192.0.2.10", "error": "private-host.local failed",
                   "receipt": {"peer_id": "private-peer"}, "compared_through": 100,
                   "coverage_start": 11, "active_incidents": 0, "qualified": True}
        result = self.read(payload)
        self.assertTrue(result["available"])
        self.assertEqual(result["compared_through"], 100)
        for private in ("192.0.2.10", "private-host", "private-peer", "receipt", "error"):
            self.assertNotIn(private, json.dumps(result))

    def test_public_condition_drives_health_without_redundant_flags(self):
        for condition in ('matching', 'unavailable', 'tree_mismatch', 'chain_disagreement'):
            result = self.read({'verifier_id': 'verifier-' + 'a' * 32, 'sample_time': time.time(),
                                'condition': condition, 'comparison_healthy': True,
                                'ancestor_hashes': {'10': 'b' * 64, 'host': '192.0.2.10'}})
            self.assertEqual(result['comparison_healthy'], condition == 'matching')
            self.assertEqual(result['ancestor_hashes'], {'10': 'b' * 64})

    def test_stale_or_malformed_file_never_reports_fresh_status(self):
        self.assertFalse(self.read({"verifier_id": "verifier-" + "a" * 32,
                                    "sample_time": time.time() - 1000})["available"])
        self.assertEqual(self.read({"verifier_id": "192.0.2.10"}), {"available": False})
        self.assertEqual(self.read(["private-host"]), {"available": False})

    def test_private_mac_is_thirteenth_node_with_detail_and_no_direct_probe(self):
        collector = status.ClusterCollector([node("node-%02d" % n) for n in range(12)], 10, 300, "mainnet")
        sample = {"available": True, "verifier_id": "verifier-" + "a" * 32,
                  "mac_tip": 100, "mac_tip_hash": "b" * 64, "source_sha": "c" * 40,
                  "comparison_healthy": True,
                  "compared_through": 90, "pending_alerts": 2, "node_rss_bytes": 1000}
        with mock.patch.dict(os.environ, {"ZAKURA_MAC_CRANELIFT_STATUS": "1"}), \
                mock.patch.object(status, "mac_cranelift_status", return_value=sample), \
                mock.patch.object(status, "probe_node", return_value={}) as probe:
            collector.poll_once()
        snapshot = collector.snapshot()
        self.assertEqual(snapshot["total"], 13)
        self.assertEqual(probe.call_count, 12)
        self.assertNotIn("verifiers", snapshot)
        mac = next(row for row in snapshot["rows"] if row["name"] == "mac-os-cranelift")
        self.assertEqual(mac["height"], 100)
        self.assertTrue(mac["healthy"])
        self.assertEqual(mac["ssh"], "")
        self.assertEqual(mac["node_id"], sample["verifier_id"])
        detail = collector.node_snapshot("mac-os-cranelift")
        self.assertEqual(detail["node"]["host"]["rss_bytes"], 1000)
        self.assertEqual(len(detail["history"]), 1)

    def test_dashboard_mac_row_reaches_watchdog_fork_and_offline_policies(self):
        spec = importlib.util.spec_from_file_location(
            "dashboard_watchdog", SCRIPT_PATH.with_name("zakura-cluster-watchdog.py"))
        watchdog = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = watchdog
        spec.loader.exec_module(watchdog)
        collector = status.ClusterCollector([node()], 10, 300, "mainnet")
        sample = {"available": True, "mac_tip": 110, "mac_tip_hash": "c" * 64,
                  "comparison_healthy": True,
                  "ancestor_hashes": {"10": "a" * 64}}
        with mock.patch.dict(os.environ, {"ZAKURA_MAC_CRANELIFT_STATUS": "1"}), \
                mock.patch.object(status, "mac_cranelift_status", return_value=sample), \
                mock.patch.object(status, "probe_node", return_value={}):
            collector.poll_once()
        mac = next(row for row in collector.snapshot()["rows"]
                   if row["name"] == "mac-os-cranelift")
        args = argparse.Namespace(down_after=600, dry_run=False)
        agent = watchdog.Watchdog([], args)
        sent = []
        agent.notify = lambda text, args: (sent.append(text), True)[1]
        others = [dict(mac, name=f"linux-{i}",
                       ancestor_hashes={"10": "b" * 64}) for i in range(12)]
        fleet = watchdog.Fleet("mainnet", "http://localhost/data", "https://example.com/")
        with mock.patch.dict(os.environ, {"ZAKURA_MAC_CRANELIFT_ALERTS_MUTED": "0"}):
            agent.handle_mac_fork({}, fleet, [mac, *others], time.time(), False)
        self.assertEqual(len(sent), 1)
        self.assertIn("mac-os-cranelift", sent[0])
        self.assertEqual(watchdog.node_condition(dict(mac, health="down"),
                                               time.time(), 0, args)[2], 180)

    def test_canonical_dashboard_setting_enables_the_node(self):
        collector = status.ClusterCollector([node()], 10, 300, "mainnet")
        with mock.patch.dict(os.environ, {"ZAKURA_MAC_CRANELIFT_STATUS": "1"}, clear=True), \
                mock.patch.object(status, "mac_cranelift_status", return_value={"available": False}), \
                mock.patch.object(status, "probe_node", return_value={}):
            collector.poll_once()
        self.assertEqual(collector.snapshot()["total"], 2)

    def test_private_mac_is_mainnet_only_and_unavailable_sample_is_unhealthy(self):
        for network, enabled in (("mainnet", True), ("testnet", False)):
            collector = status.ClusterCollector([node()], 10, 300, network)
            with mock.patch.dict(os.environ, {"ZAKURA_MAC_CRANELIFT_STATUS": "1"}), \
                    mock.patch.object(status, "mac_cranelift_status", return_value={"available": False}), \
                    mock.patch.object(status, "probe_node", return_value={}):
                collector.poll_once()
            snapshot = collector.snapshot()
            self.assertEqual(snapshot["total"], 2 if enabled else 1)
            if enabled:
                mac = next(row for row in snapshot["rows"] if row["name"] == "mac-os-cranelift")
                self.assertFalse(mac["healthy"])
                self.assertIsNone(mac["height"])
            self.assertNotIn("verifiers", snapshot)

    def test_advancing_mac_with_failed_comparison_is_not_healthy(self):
        collector = status.ClusterCollector([node()], 10, 300, "mainnet")
        sample = {"available": True, "mac_tip": 100, "mac_tip_hash": "b" * 64,
                  "comparison_healthy": False, "compared_through": 90, "alerts_muted": True}
        with mock.patch.dict(os.environ, {"ZAKURA_MAC_CRANELIFT_STATUS": "1"}), \
                mock.patch.object(status, "mac_cranelift_status", return_value=sample), \
                mock.patch.object(status, "probe_node", return_value={}):
            collector.poll_once()
        mac = next(row for row in collector.snapshot()["rows"] if row["name"] == "mac-os-cranelift")
        self.assertEqual(mac["height"], 100)
        self.assertFalse(mac["healthy"])
        self.assertEqual(mac["health"], "verification_error")
        self.assertIn("Alerts muted", mac["detail"])

    def test_separate_verifier_section_is_removed(self):
        source = SCRIPT_PATH.read_text()
        self.assertNotIn('id="verifier-panel"', source)
        self.assertNotIn("Native macOS consensus verifier</h2>", source)


class NodeConfigTests(unittest.TestCase):
    def test_blank_metrics_endpoint_stays_disabled(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml") as config:
            config.write(
                '[defaults]\nmetrics_endpoint = "127.0.0.1:9999"\n'
                '[[nodes]]\nname = "node-a"\nssh_string = "root@node-a"\n'
                'metrics_endpoint = ""\n'
            )
            config.flush()

            [configured] = status.load_nodes(Path(config.name))

        self.assertEqual(configured.metrics_endpoint, "")

    def test_explicit_metrics_endpoint_is_preserved(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml") as config:
            config.write(
                '[[nodes]]\nname = "node-a"\nssh_string = "root@node-a"\n'
                'metrics_endpoint = "127.0.0.1:9999"\n'
            )
            config.flush()

            [configured] = status.load_nodes(Path(config.name))

        self.assertEqual(configured.metrics_endpoint, "127.0.0.1:9999")


class RemoteProbeTests(unittest.TestCase):
    """Run the probe the way a node does: as a standalone script over stdin.

    The probe is a string executed on the far side of ssh, so the only faithful
    way to exercise it is to run it as its own process against stub endpoints.
    """

    EXPOSITION = b"""# a comment the exporter never actually emits
zakura_build_info{version="1.1.0-rc1"} 1
zcash_net_peers 74
zakura_p2p_conn_active 12
sync_header_verification_lag 3
zcash_net_in_messages{command="inv",addr="redacted"} 5
zcash_net_in_bytes_total 12345
not_an_allowlisted_metric 999
sync_stage_duration_seconds{quantile="0.5"} 0.02
zcash_net_peers_connected{user_agent="/Zakura:1.1.0/",remote_version="170160"} 40
zcash_net_peers_connected{user_agent="/Zakura:1.1.0/",remote_version="170140"} 2
zcash_net_peers_connected{user_agent="/MagicBean:6.2.0/",remote_version="170160"} 33
zcash_net_peers_connected{user_agent="/Gone:1.0/",remote_version="170160"} 0
"""

    def setUp(self):
        self.exposition = self.EXPOSITION
        self.rpc_results = {}
        outer = self

        class StubHandler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, code, body):
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path == "/metrics":
                    return self.reply(200, outer.exposition)
                if self.path == "/healthy":
                    return self.reply(200, b"ok")
                if self.path == "/ready":
                    return self.reply(503, b"lag=47 blocks")
                return self.reply(404, b"nope")

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode())
                method = payload.get("method")
                if method in outer.rpc_results:
                    body = json.dumps({
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "result": outer.rpc_results[method],
                    }).encode()
                    return self.reply(200, body)
                body = json.dumps({
                    "jsonrpc": "2.0",
                    "id": payload.get("id"),
                    "error": {"code": -32601, "message": method},
                }).encode()
                return self.reply(200, body)

        self.server = status.ThreadingHTTPServer(("127.0.0.1", 0), StubHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def stub_journalctl(self, stack, *, exit_code: int, stdout: str = "") -> dict:
        """Put a fake journalctl first on PATH so OOM counting is hermetic."""
        directory = stack.enter_context(tempfile.TemporaryDirectory())
        script = Path(directory) / "journalctl"
        script.write_text(
            "#!/bin/sh\n"
            + ("printf '%s'\n" % stdout.replace("'", "") if stdout else "")
            + f"exit {exit_code}\n",
            encoding="utf-8",
        )
        script.chmod(0o755)
        return {"PATH": f"{directory}:{os.environ.get('PATH', '')}"}

    def run_probe(self, env: dict | None = None, **overrides) -> dict:
        args = {
            "service": "",
            "bin_path": "/bin/true",
            "log_file": "",
            "rpc_url": "",
            "probe_kind": "zebra",
            "process_pattern": "",
            "rpc_auth": "",
            "rpc_user": "",
            "rpc_password": "",
            "rpc_config_path": "",
            "container_name": "",
            "metrics_endpoint": "",
            "health_listen_addr": "",
            "state_cache_dir": tempfile.gettempdir(),
            "want_metrics": "1",
        }
        args.update(overrides)
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as handle:
            handle.write(status.REMOTE_PROBE)
            script = handle.name
        try:
            completed = subprocess.run(
                [sys.executable, script, *args.values()],
                capture_output=True,
                text=True,
                timeout=120,
                env={**os.environ, **(env or {})},
            )
        finally:
            Path(script).unlink()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def test_metrics_scrape_keeps_only_allowlisted_series(self):
        probe = self.run_probe(metrics_endpoint=self.endpoint)

        self.assertNotIn("metrics_error", probe)
        self.assertEqual(probe["metrics_version"], "1.1.0-rc1")
        self.assertEqual(
            probe["metrics"],
            {
                "zcash_net_peers": 74.0,
                "zakura_p2p_conn_active": 12.0,
                "sync_header_verification_lag": 3.0,
                "zcash_net_in_bytes_total": 12345.0,
            },
        )

    def test_metrics_scrape_keeps_stall_alert_series(self):
        self.exposition += b"""checkpoint_processing_next_height 101
checkpoint_verified_height 99
state_finalized_block_height 98
state_vct_root_stalled_height 100
state_vct_root_repair_requested 4
state_vct_root_retry_count 3
state_vct_aux_sweep_frontier_height 97
sync_header_vct_repair_requested_total 8
sync_header_vct_repair_scheduled_total 7
sync_header_vct_repair_admitted_total 6
sync_header_vct_repair_context_unavailable_total 5
sync_header_vct_repair_timed_out_total 4
sync_header_vct_repair_resource_stalled_total 3
sync_header_vct_repair_no_supplier_total 999
sync_block_applying 0
sync_block_best_header_tip_height 102
sync_block_verified_tip_height 98
sync_block_fill_stop 1
sync_block_outstanding 0
sync_block_missing_bodies 4000
"""

        probe = self.run_probe(metrics_endpoint=self.endpoint)

        expected = {
            "checkpoint_processing_next_height": 101.0,
            "checkpoint_verified_height": 99.0,
            "state_finalized_block_height": 98.0,
            "state_vct_root_stalled_height": 100.0,
            "state_vct_root_repair_requested": 4.0,
            "state_vct_root_retry_count": 3.0,
            "state_vct_aux_sweep_frontier_height": 97.0,
            "sync_header_vct_repair_requested_total": 8.0,
            "sync_header_vct_repair_scheduled_total": 7.0,
            "sync_header_vct_repair_admitted_total": 6.0,
            "sync_header_vct_repair_context_unavailable_total": 5.0,
            "sync_header_vct_repair_timed_out_total": 4.0,
            "sync_header_vct_repair_resource_stalled_total": 3.0,
            "sync_block_applying": 0.0,
            "sync_block_best_header_tip_height": 102.0,
            "sync_block_verified_tip_height": 98.0,
            "sync_block_fill_stop": 1.0,
            "sync_block_outstanding": 0.0,
            "sync_block_missing_bodies": 4_000.0,
        }
        for name, value in expected.items():
            self.assertEqual(probe["metrics"].get(name), value)
        self.assertNotIn(
            "sync_header_vct_repair_no_supplier_total", probe["metrics"]
        )

    def test_peer_versions_come_from_the_user_agent_label(self):
        # Legacy fallback: older zakurad omits getpeerinfo.subver, so the
        # exporter label still fills the panel until that RPC field exists.
        probe = self.run_probe(metrics_endpoint=self.endpoint)

        self.assertEqual(
            probe["peer_user_agents"],
            [["/Zakura:1.1.0/", 42], ["/MagicBean:6.2.0/", 33]],
        )
        self.assertNotIn("zcash_net_peers_connected", probe["metrics"])

    def test_peer_versions_are_absent_without_user_agent_labels(self):
        self.exposition = b"""zakura_build_info{version="1.1.0-rc1"} 1
zcash_net_peers 74
zcash_net_in_bytes_total 12345
"""
        probe = self.run_probe(metrics_endpoint=self.endpoint)

        self.assertNotIn("peer_user_agents", probe)

    def test_peer_versions_come_from_getpeerinfo_subver(self):
        # Durable source: once zakurad exposes subver, the panel restores
        # from RPC even when the exporter no longer labels by user_agent.
        self.rpc_results = {
            "getblockchaininfo": {
                "blocks": 10,
                "headers": 10,
                "bestblockhash": "aa",
                "chain": "test",
            },
            "getpeerinfo": [
                {"addr": "127.0.0.1:1", "inbound": False, "subver": "/Zakura:1.1.0/"},
                {"addr": "127.0.0.1:2", "inbound": True, "subver": "/Zakura:1.1.0/"},
                {"addr": "127.0.0.1:3", "inbound": False, "subver": "/MagicBean:6.2.0/"},
                {"addr": "127.0.0.1:4", "inbound": False},
            ],
        }
        probe = self.run_probe(rpc_url=f"http://{self.endpoint}/")

        self.assertEqual(
            probe["peer_subversions"],
            [["/Zakura:1.1.0/", 2], ["/MagicBean:6.2.0/", 1], ["unknown", 1]],
        )
        self.assertNotIn("peer_user_agents", probe)

    def test_metrics_scrape_can_be_skipped(self):
        probe = self.run_probe(metrics_endpoint=self.endpoint, want_metrics="")

        self.assertTrue(probe["metrics_skipped"])
        self.assertNotIn("metrics", probe)
        self.assertNotIn("metrics_error", probe)

    def test_no_oom_kills_reports_zero_rather_than_unknown(self):
        # journalctl exits 1 when its grep matches nothing, which is the common
        # case on a healthy node and must not read as "unknown".
        with contextlib.ExitStack() as stack:
            env = self.stub_journalctl(stack, exit_code=1)
            probe = self.run_probe(env=env)

        self.assertEqual(probe["host"]["oom_kills_24h"], 0)

    def test_oom_kills_are_counted_by_line(self):
        with contextlib.ExitStack() as stack:
            env = self.stub_journalctl(
                stack,
                exit_code=0,
                stdout="oom-kill: one\\noom-kill: two\\n",
            )
            probe = self.run_probe(env=env)

        self.assertEqual(probe["host"]["oom_kills_24h"], 2)

    def test_health_probe_keeps_status_and_body(self):
        probe = self.run_probe(health_listen_addr=self.endpoint)

        self.assertEqual(probe["health"]["healthy"], {"status": 200, "body": "ok"})
        # The body is the diagnostic: /ready explains *why* it is not ready.
        self.assertEqual(
            probe["health"]["ready"],
            {"status": 503, "body": "lag=47 blocks"},
        )

    def test_unconfigured_endpoints_report_rather_than_fail(self):
        probe = self.run_probe()

        self.assertEqual(probe["metrics_error"], "metrics endpoint not configured")
        self.assertEqual(probe["health_error"], "health endpoint not configured")
        self.assertNotIn("host_error", probe)

    def test_host_vitals_are_collected_without_any_endpoint(self):
        probe = self.run_probe()

        host = probe["host"]
        self.assertEqual(host["disk_path"], tempfile.gettempdir())
        self.assertGreater(host["disk_total_bytes"], 0)
        self.assertIn("mem_total_bytes", host)
        self.assertIn("load1", host)
        self.assertIn("uptime_seconds", host)

    def test_log_tail_redacts_addresses_and_bounds_line_length(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "zakura.log"
            log.write_text(
                "INFO fine\n"
                "ERROR peer 203.0.113.10 timed out\n"
                "WARN " + ("x" * 500) + "\n",
                encoding="utf-8",
            )

            probe = self.run_probe(log_file=str(log))

        lines = probe["log_errors"]
        self.assertEqual(len(lines), 2)
        self.assertIn("x.x.x.x", lines[0])
        self.assertNotIn("203.0.113.10", lines[0])
        self.assertLessEqual(len(lines[1]), 303)

    def test_unreachable_metrics_endpoint_is_reported_not_raised(self):
        # Port 1 is reserved and never listening, so the scrape must fail.
        probe = self.run_probe(metrics_endpoint="127.0.0.1:1")

        self.assertIn("metrics_error", probe)
        self.assertNotIn("metrics", probe)
        self.assertIn("host", probe)


class IronwoodStatusTests(unittest.TestCase):
    def test_remote_probe_is_valid_python_and_uses_required_rpcs(self):
        compile(status.REMOTE_PROBE, "<remote-probe>", "exec")
        self.assertIn('rpc_call("getblockchaininfo")', status.REMOTE_PROBE)
        self.assertIn('rpc_call("getinfo")', status.REMOTE_PROBE)
        self.assertIn('blockchain_info.get("headers")', status.REMOTE_PROBE)
        self.assertIn('"getblockhash"', status.REMOTE_PROBE)
        self.assertIn('rpc_call("getblockheader"', status.REMOTE_PROBE)

    def test_peer_version_panel_prefers_rpc_subver(self):
        # When getpeerinfo.subver is present, the live RPC mix wins over the
        # legacy exporter user_agent label.
        self.assertIn(
            "(row.peer_subversions || []).length",
            status.PAGE,
        )
        self.assertLess(
            status.PAGE.find("(row.peer_subversions || []).length"),
            status.PAGE.find("(row.peer_user_agents || [])"),
        )

    def test_success_response_has_the_stable_public_shape(self):
        subject = collector()
        subject.rows = [public_row()]

        http_status, payload = subject.ironwood_status(now=1_010)

        self.assertEqual(http_status, 200)
        self.assertEqual(
            payload,
            {
                "schema_version": 1,
                "network": "testnet",
                "activation_height": 4_134_000,
                "activated": True,
                "tip_height": 4_201_000,
                "blocks_since_activation": 67_000,
                "ironwood_chain_balance_zat": "123456789012345",
                "updated_at": "1970-01-01T00:16:40Z",
                "source": {
                    "client_name": "zakurad",
                    "client_version": "1.0.4-rc0+g061af8a",
                },
            },
        )
        self.assertNotIn("ssh", json.dumps(payload))
        self.assertNotIn("block_hash", payload)

    def test_mainnet_response_is_pre_activation(self):
        subject = collector("mainnet")
        subject.rows = [
            public_row(
                height=3_425_000,
                network="mainnet",
                balance_zat="0",
            )
        ]

        http_status, payload = subject.ironwood_status(now=1_010)

        self.assertEqual(http_status, 200)
        self.assertEqual(payload["activation_height"], 3_428_143)
        self.assertFalse(payload["activated"])
        self.assertIsNone(payload["blocks_since_activation"])

    def test_selects_zakurad_from_the_most_agreed_tip(self):
        subject = collector()
        subject.rows = [
            public_row(
                name="ahead",
                height=4_201_001,
                block_hash="b" * 64,
                balance_zat="999",
            ),
            public_row(
                name="agreed-zcashd",
                client_name="zcashd",
                client_version="v6.20.0",
            ),
            public_row(name="agreed-zakurad"),
        ]

        http_status, payload = subject.ironwood_status(now=1_010)

        self.assertEqual(http_status, 200)
        self.assertEqual(payload["tip_height"], 4_201_000)
        self.assertEqual(payload["ironwood_chain_balance_zat"], "123456789012345")
        self.assertEqual(payload["source"]["client_name"], "zakurad")

    def test_failure_codes_are_generic_and_specific(self):
        cases = {
            "network_mismatch": public_row(network="mainnet"),
            "ironwood_pool_unavailable": public_row(balance_zat="1.5"),
            "source_stale": public_row(observed_at=800),
            "upstream_unavailable": public_row(client_version=""),
        }

        for expected_code, row in cases.items():
            with self.subTest(expected_code):
                subject = collector()
                subject.rows = [row]

                http_status, payload = subject.ironwood_status(now=1_000)

                self.assertEqual(http_status, 503)
                self.assertEqual(payload["error"]["code"], expected_code)
                self.assertEqual(
                    payload["error"]["message"],
                    status.PUBLIC_ERROR_MESSAGE,
                )
                self.assertNotIn("node-a", json.dumps(payload))

    def test_existing_data_snapshot_keeps_fleet_details(self):
        subject = collector()
        subject.rows = [{"name": "node-a", "ssh": "root@192.0.2.1"}]

        payload = subject.snapshot()

        self.assertEqual(payload["rows"][0]["ssh"], "root@192.0.2.1")
        self.assertIn("chain", payload)
        self.assertEqual(payload["chain"]["status"], "unknown")


class TipAgreementTests(unittest.TestCase):
    def test_classify_tip_event_detects_reorg_signals(self):
        self.assertEqual(
            status.classify_tip_event(None, None, 10, "a" * 64),
            "initial",
        )
        self.assertEqual(
            status.classify_tip_event(10, "a" * 64, 11, "b" * 64),
            "advanced",
        )
        self.assertEqual(
            status.classify_tip_event(10, "a" * 64, 10, "a" * 64),
            "unchanged",
        )
        self.assertEqual(
            status.classify_tip_event(10, "a" * 64, 10, "b" * 64),
            "tip_switch",
        )
        self.assertEqual(
            status.classify_tip_event(10, "a" * 64, 8, "c" * 64),
            "reorg_height_drop",
        )

    def test_chain_summary_agreed_lagging_and_split(self):
        agreed = status.compute_chain_summary(
            [
                {"name": "a", "height": 100, "block_hash": "aa", "client_name": "zakurad"},
                {"name": "b", "height": 100, "block_hash": "aa", "client_name": "zakurad"},
            ]
        )
        self.assertEqual(agreed["status"], "agreed")
        self.assertFalse(agreed["split"])
        self.assertEqual(agreed["majority_height"], 100)

        lagging = status.compute_chain_summary(
            [
                {"name": "a", "height": 100, "block_hash": "aa", "client_name": "zakurad"},
                {"name": "b", "height": 99, "block_hash": "bb", "client_name": "zakurad"},
            ]
        )
        self.assertEqual(lagging["status"], "lagging")
        self.assertFalse(lagging["split"])
        lagging_group = next(
            group for group in lagging["tip_groups"] if group["height"] == 99
        )
        self.assertEqual(lagging_group["fork_depth"], 1)
        self.assertEqual(lagging_group["fork_depth_label"], "1 behind")

        split = status.compute_chain_summary(
            [
                {"name": "a", "height": 100, "block_hash": "aa", "client_name": "zakurad"},
                {"name": "b", "height": 100, "block_hash": "bb", "client_name": "zcashd"},
            ]
        )
        self.assertEqual(split["status"], "split")
        self.assertTrue(split["split"])

    def test_enrich_chain_roles(self):
        rows = [
            {"name": "a", "height": 100, "block_hash": "aa"},
            {"name": "b", "height": 100, "block_hash": "aa"},
            {"name": "c", "height": 100, "block_hash": "ff"},
            {"name": "d", "height": 99, "block_hash": "dd"},
            {"name": "e", "height": None, "block_hash": ""},
        ]
        chain = status.compute_chain_summary(rows)
        status.enrich_chain_roles(rows, chain)

        roles = {row["name"]: row["chain_role"] for row in rows}
        self.assertEqual(chain["status"], "split")
        self.assertEqual(roles["a"], "majority")
        self.assertEqual(roles["b"], "majority")
        self.assertEqual(roles["c"], "fork")
        self.assertEqual(roles["d"], "behind")
        self.assertEqual(roles["e"], "unknown")

        ahead_rows = [
            {"name": "a", "height": 100, "block_hash": "aa"},
            {"name": "b", "height": 100, "block_hash": "aa"},
            {"name": "c", "height": 101, "block_hash": "cc"},
        ]
        ahead_chain = status.compute_chain_summary(ahead_rows)
        status.enrich_chain_roles(ahead_rows, ahead_chain)
        self.assertEqual(ahead_chain["status"], "lagging")
        self.assertEqual(
            {row["name"]: row["chain_role"] for row in ahead_rows},
            {"a": "majority", "b": "majority", "c": "ahead"},
        )
        ahead_group = next(
            group for group in ahead_chain["tip_groups"] if group["height"] == 101
        )
        self.assertEqual(ahead_group["fork_depth"], 1)
        self.assertEqual(ahead_group["fork_depth_label"], "1 ahead")

    def test_row_for_records_reorg_and_headers(self):
        subject = collector()
        subject.last_height["node-a"] = 100
        subject.last_block_hash["node-a"] = "a" * 64
        subject.last_advanced_at["node-a"] = 1_000.0

        row = subject.row_for(
            node(),
            {
                "height": 98,
                "headers": 101,
                "block_hash": "b" * 64,
                "active_state": "active",
                "process_running": True,
                "client_name": "zakurad",
                "client_version": "v1",
            },
            now=1_100.0,
        )

        self.assertEqual(row["tip_event"], "reorg_height_drop")
        self.assertEqual(row["headers"], 101)
        self.assertEqual(row["header_lag"], 3)
        self.assertEqual(len(subject.recent_reorgs), 1)
        self.assertEqual(subject.recent_reorgs[0]["kind"], "reorg_height_drop")

        subject.rows = [row]
        subject.chain = status.compute_chain_summary(
            subject.rows,
            list(subject.recent_reorgs),
        )
        status.enrich_chain_roles(subject.rows, subject.chain)
        snapshot = subject.snapshot()
        self.assertEqual(snapshot["chain"]["recent_reorgs"][0]["node"], "node-a")
        self.assertIn("height dropped", snapshot["rows"][0]["detail"])
        self.assertEqual(snapshot["chain"]["recent_reorgs"][0]["depth"], 2)
        self.assertEqual(
            snapshot["chain"]["recent_reorgs"][0]["discarded_hash"],
            "a" * 64,
        )

    def test_orphan_pairs_persist_across_collector_restarts(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "orphan-pairs.json"
            first = status.ClusterCollector(
                [node()],
                interval=10,
                stale_after=300,
                network="testnet",
                state_file=state_file,
            )
            first.last_height["node-a"] = 100
            first.last_block_hash["node-a"] = "a" * 64
            first.last_ancestors["node-a"] = {"1": "p" * 64}
            first.last_advanced_at["node-a"] = 1_000.0
            first.row_for(
                node(),
                {
                    "height": 100,
                    "block_hash": "b" * 64,
                    "ancestor_hashes": {"1": "p" * 64},
                    "active_state": "active",
                    "process_running": True,
                },
                now=1_100.0,
            )
            self.assertTrue(state_file.exists())

            second = status.ClusterCollector(
                [node()],
                interval=10,
                stale_after=300,
                network="testnet",
                state_file=state_file,
            )
            self.assertEqual(len(second.recent_reorgs), 1)
            event = second.recent_reorgs[0]
            self.assertEqual(event["kind"], "tip_switch")
            self.assertEqual(event["depth"], 1)
            self.assertEqual(event["discarded_hash"], "a" * 64)
            self.assertEqual(event["canonical_hash"], "b" * 64)

    def test_stall_timer_survives_a_collector_restart(self):
        # A restart used to reseed last_advanced_at to "now", reporting every
        # node as freshly advanced and clearing in-flight stall alerts.
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            first = status.ClusterCollector(
                [node()],
                interval=10,
                stale_after=300,
                network="testnet",
                state_file=state_file,
            )
            first.last_height["node-a"] = 4_129_396
            first.last_advanced_at["node-a"] = 1_000.0
            first.persist_state()

            second = status.ClusterCollector(
                [node()],
                interval=10,
                stale_after=300,
                network="testnet",
                state_file=state_file,
            )
            self.assertEqual(second.last_advanced_at["node-a"], 1_000.0)
            self.assertEqual(second.last_height["node-a"], 4_129_396)

            # The node is still pinned at the same height long after the
            # restart, so the row must still report a large stall age.
            row = second.row_for(
                node(),
                {
                    "height": 4_129_396,
                    "active_state": "active",
                    "process_running": True,
                },
                now=4_000.0,
            )
            self.assertEqual(row["seconds_since_advanced"], 3_000.0)
            self.assertEqual(row["health"], "stale")

    def test_progress_state_tolerates_a_missing_or_corrupt_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "absent.json"
            self.assertEqual(status.load_progress(missing), {})
            self.assertEqual(status.load_progress(None), {})

            corrupt = Path(tmp) / "corrupt.json"
            corrupt.write_text("{not json", encoding="utf-8")
            self.assertEqual(status.load_progress(corrupt), {})

            # A pre-existing file written before progress was persisted.
            legacy = Path(tmp) / "legacy.json"
            legacy.write_text(json.dumps({"orphan_pairs": []}), encoding="utf-8")
            self.assertEqual(status.load_progress(legacy), {})

    def test_fork_depth_from_ancestor_samples(self):
        depth = status.estimate_fork_depth_from_ancestors(
            {"1": "x", "2": "y", "5": "same"},
            {"1": "a", "2": "b", "5": "same"},
        )
        self.assertEqual(depth["depth"], 5)
        self.assertEqual(depth["label"], "depth 5")

        split = status.compute_chain_summary(
            [
                {
                    "name": "a",
                    "height": 100,
                    "block_hash": "aa",
                    "ancestor_hashes": {"1": "p1", "2": "shared"},
                    "client_name": "zakurad",
                },
                {
                    "name": "b",
                    "height": 100,
                    "block_hash": "bb",
                    "ancestor_hashes": {"1": "q1", "2": "shared"},
                    "client_name": "zakurad",
                },
            ]
        )
        other = next(
            group
            for group in split["tip_groups"]
            if group["block_hash"] != split["majority_hash"]
        )
        self.assertEqual(other["fork_depth"], 2)
        self.assertEqual(other["fork_depth_label"], "depth 2")


class ViewSwitchingTests(unittest.TestCase):
    """The fleet and node views share one page and are toggled with [hidden].

    A class that sets its own `display` beats the UA stylesheet's `[hidden]`
    rule, so without an authoritative override a hidden section stays on screen
    showing its unrendered `...` placeholders.
    """

    def setUp(self):
        self.page = status.PAGE
        self.markup = self.page[: self.page.index("<script>")]

    def test_page_forces_hidden_elements_to_stay_hidden(self):
        self.assertIn("[hidden] { display: none !important; }", self.page)

    def test_every_toggled_section_is_covered_by_the_override(self):
        toggled = re.findall(
            r'<(?:section|div) class="([^"]+)"[^>]*data-view="[^"]+"', self.markup
        )
        self.assertTrue(toggled, "expected the view switcher to tag some sections")

        override = self.page.index("[hidden] { display: none !important; }")
        for classes in toggled:
            primary = classes.split()[0]
            rule = re.search(
                r"\n\.%s \{(.*?)\}" % re.escape(primary), self.page, re.S
            )
            if rule is None or "display:" not in rule.group(1):
                continue
            # !important wins regardless of order, but keep the override early
            # so the intent stays readable.
            self.assertLess(
                override,
                rule.start(),
                f".{primary} sets display and must be covered by the override",
            )

    def test_both_views_are_present_in_one_template(self):
        self.assertIn('data-view="fleet"', self.markup)
        self.assertIn('data-view="node"', self.markup)

    def test_tip_group_badges_use_the_height_relationship(self):
        self.assertIn(
            "if (group.height === chain.majority_height) return 'fork';",
            self.page,
        )
        self.assertIn(
            "if (group.height > chain.majority_height) return 'ahead';", self.page
        )
        self.assertIn("return 'behind';", self.page)
        self.assertIn("badge(role, tone(CHAIN_TONE, role))", self.page)
        self.assertNotIn("badge(isMajority ? 'majority' : 'fork'", self.page)
        self.assertIn("lagging: 'Nodes report different tip heights.'", self.page)


class NodeDetailTests(unittest.TestCase):
    def probe(self, **overrides) -> dict:
        base = {
            "height": 4_200_000,
            "headers": 4_200_000,
            "block_hash": "a" * 64,
            "active_state": "active",
            "process_running": True,
            "peer_count": 74,
            "metrics": {"zcash_net_peers": 74.0},
            "health": {"healthy": {"status": 200, "body": "ok"}},
            "log_errors": ["ERROR something"],
            "host": {
                "disk_total_bytes": 1000,
                "disk_free_bytes": 250,
                "rss_bytes": 4096,
                "restart_count": 2,
            },
        }
        base.update(overrides)
        return base

    def test_disk_free_pct_needs_both_totals(self):
        self.assertEqual(
            status.disk_free_pct({"disk_total_bytes": 1000, "disk_free_bytes": 250}),
            25.0,
        )
        self.assertIsNone(status.disk_free_pct({"disk_free_bytes": 250}))
        self.assertIsNone(status.disk_free_pct({"disk_total_bytes": 0, "disk_free_bytes": 0}))

    def test_fleet_payload_drops_deep_fields_but_keeps_vitals(self):
        collected = collector()
        collected.rows = [collected.row_for(node(), self.probe(), now=1_000.0)]

        row = collected.snapshot()["rows"][0]

        for key in status.NODE_DETAIL_KEYS:
            self.assertNotIn(key, row)
        self.assertEqual(row["vitals"]["disk_free_pct"], 25.0)
        self.assertEqual(row["vitals"]["restart_count"], 2)
        self.assertEqual(row["peer_count"], 74)

    def test_fleet_payload_carries_atomic_bounded_alert_diagnostics(self):
        collected = collector()
        metrics = {
            "checkpoint_verified_height": 4_199_999.0,
            "sync_block_applying": 0.0,
            "sync_zakura_apply_operations": 5.0,
            "sync_zakura_apply_in_flight": 5.0,
            "sync_zakura_apply_oldest_seconds": 630.0,
            "sync_zakura_apply_phase": 2.0,
            "sync_block_outstanding": 0.0,
            "sync_block_missing_bodies": 4_000.0,
            "sync_block_fill_stop": float("nan"),
            "sync_block_verified_tip_height": float("inf"),
            "not_an_alert_metric": 123.0,
        }
        collected.rows = [
            collected.row_for(node(), self.probe(metrics=metrics), now=1_000.0)
        ]
        collected.last_poll = 1_001.0

        snapshot = collected.snapshot()
        diagnostics = snapshot["rows"][0]["alert_diagnostics"]

        self.assertEqual(snapshot["last_poll"], 1_001.0)
        self.assertEqual(diagnostics["last_poll"], snapshot["last_poll"])
        self.assertEqual(diagnostics["metrics_at"], 1_000.0)
        self.assertTrue(diagnostics["metrics_available"])
        self.assertEqual(
            diagnostics["metrics"],
            {
                "checkpoint_verified_height": 4_199_999.0,
                "sync_block_applying": 0.0,
                "sync_zakura_apply_operations": 5.0,
                "sync_zakura_apply_in_flight": 5.0,
                "sync_zakura_apply_oldest_seconds": 630.0,
                "sync_zakura_apply_phase": 2.0,
                "sync_block_outstanding": 0.0,
                "sync_block_missing_bodies": 4_000.0,
            },
        )
        self.assertNotIn("not_an_alert_metric", json.dumps(diagnostics))
        self.assertNotIn("log_errors", json.dumps(diagnostics))
        self.assertNotIn("root@node-a", json.dumps(diagnostics))
        self.assertLess(len(json.dumps(diagnostics)), 1_000)

    def test_whole_probe_failure_marks_metrics_unavailable(self):
        collected = collector()
        error = "ssh exited 255: connection refused"

        row = collected.row_for(node(), {"error": error}, now=1_000.0)
        collected.rows = [row]
        collected.last_poll = 1_001.0
        diagnostics = collected.snapshot()["rows"][0]["alert_diagnostics"]

        self.assertEqual(row["detail"], error)
        self.assertEqual(row["metrics_error"], error)
        self.assertIsNone(row["metrics_at"])
        self.assertFalse(diagnostics["metrics_available"])
        self.assertIsNone(diagnostics["metrics_at"])
        self.assertEqual(diagnostics["metrics"], {})

    def test_missing_metrics_timestamp_marks_metrics_unavailable(self):
        diagnostics = status.alert_diagnostics(
            {
                "metrics": {"sync_block_missing_bodies": 4_000.0},
                "metrics_at": None,
            },
            last_poll=1_001.0,
        )

        self.assertFalse(diagnostics["metrics_available"])
        self.assertIsNone(diagnostics["metrics_at"])
        self.assertEqual(
            diagnostics["metrics"],
            {"sync_block_missing_bodies": 4_000.0},
        )

    def test_node_payload_carries_the_deep_fields(self):
        collected = collector()
        collected.rows = [collected.row_for(node(), self.probe(), now=1_000.0)]

        payload = collected.node_snapshot("node-a")

        self.assertEqual(payload["node"]["metrics"], {"zcash_net_peers": 74.0})
        self.assertEqual(payload["node"]["health_endpoint"]["healthy"]["status"], 200)
        self.assertEqual(payload["config"]["metrics_endpoint"], "")
        self.assertIsNone(collected.node_snapshot("does-not-exist"))

    def test_log_lines_are_withheld_unless_expose_logs_is_set(self):
        guarded = collector()
        guarded.rows = [guarded.row_for(node(), self.probe(), now=1_000.0)]
        self.assertEqual(guarded.node_snapshot("node-a")["node"]["log_errors"], [])
        self.assertTrue(guarded.node_snapshot("node-a")["node"]["log_errors_suppressed"])

        exposed = status.ClusterCollector(
            [node()],
            interval=10,
            stale_after=300,
            network="testnet",
            expose_logs=True,
        )
        exposed.rows = [exposed.row_for(node(), self.probe(), now=1_000.0)]

        self.assertEqual(
            exposed.node_snapshot("node-a")["node"]["log_errors"],
            ["ERROR something"],
        )

    def test_skipped_scrape_reuses_the_last_metrics(self):
        collected = collector()
        collected.rows = [collected.row_for(node(), self.probe(), now=1_000.0)]

        # A poll that skipped the scrape must not blank the panels.
        skipped = collected.row_for(
            node(),
            self.probe(metrics=None, metrics_skipped=True),
            now=1_010.0,
        )

        self.assertEqual(skipped["metrics"], {"zcash_net_peers": 74.0})
        self.assertEqual(skipped["metrics_at"], 1_000.0)

    def test_first_poll_always_scrapes(self):
        collected = collector()

        self.assertTrue(collected.should_scrape_metrics("node-a", 1_000.0))

    def test_scrape_interval_scales_with_the_last_scrape_cost(self):
        collected = collector()
        collected.last_metrics["node-a"] = {
            "metrics_at": 1_000.0,
            "metrics_scrape_seconds": 0.03,
        }

        # A cheap endpoint (0.03s * 30 = 0.9s) refreshes on every poll.
        self.assertTrue(collected.should_scrape_metrics("node-a", 1_010.0))

        # An expensive one (0.6s * 30 = 18s) backs off instead.
        collected.last_metrics["node-a"]["metrics_scrape_seconds"] = 0.6
        self.assertFalse(collected.should_scrape_metrics("node-a", 1_010.0))
        self.assertTrue(collected.should_scrape_metrics("node-a", 1_019.0))

    def test_scrape_backoff_is_capped(self):
        collected = collector()
        collected.last_metrics["node-a"] = {
            "metrics_at": 1_000.0,
            "metrics_scrape_seconds": 60.0,
        }

        self.assertFalse(collected.should_scrape_metrics("node-a", 1_100.0))
        self.assertTrue(
            collected.should_scrape_metrics("node-a", 1_000.0 + status.MAX_METRICS_INTERVAL)
        )

    def test_explicit_interval_overrides_the_adaptive_one(self):
        collected = status.ClusterCollector(
            [node()],
            interval=10,
            stale_after=300,
            network="testnet",
            metrics_min_interval=60.0,
        )
        collected.last_metrics["node-a"] = {
            "metrics_at": 1_000.0,
            "metrics_scrape_seconds": 0.01,
        }

        self.assertFalse(collected.should_scrape_metrics("node-a", 1_030.0))
        self.assertTrue(collected.should_scrape_metrics("node-a", 1_060.0))

    def test_history_drops_samples_older_than_the_window(self):
        collected = status.ClusterCollector(
            [node()],
            interval=10,
            stale_after=300,
            network="testnet",
            history_window=100,
        )
        for offset in range(0, 30):
            rows = [collected.row_for(node(), self.probe(height=4_200_000 + offset), now=1_000.0 + offset * 10)]
            collected.rows = rows
            collected.record_node_history(1_000.0 + offset * 10, rows)

        samples = list(collected.history["node-a"])

        # 100s of retention at a 10s cadence keeps the newest 11 samples.
        self.assertEqual(len(samples), 11)
        self.assertEqual(samples[-1]["height"], 4_200_029)
        self.assertGreaterEqual(samples[0]["t"], samples[-1]["t"] - 100)


class RateLimiterTests(unittest.TestCase):
    def test_limits_each_client_until_the_window_expires(self):
        limiter = status.RateLimiter(limit=2, window=10)

        self.assertTrue(limiter.allow("client-a", now=0))
        self.assertTrue(limiter.allow("client-a", now=1))
        self.assertFalse(limiter.allow("client-a", now=2))
        self.assertTrue(limiter.allow("client-b", now=2))
        self.assertTrue(limiter.allow("client-a", now=11))


class AddressPrivacyTests(unittest.TestCase):
    def test_nested_addresses_and_keys_are_redacted(self):
        addresses = ["192.0.2.17", "2001:db8::17", "::1", "::ffff:192.0.2.17", "fe80::17%en0"]
        for address in addresses:
            with self.subTest(address=address):
                payload = {address: [{"error": f"connection to [{address}]:8232 failed"}]}
                encoded = json.dumps(status.redact_private_addresses(payload, {status.address_key(address)}))
                self.assertNotIn(address, encoded)
                self.assertIn("[redacted-address]", encoded)
                self.assertNotIn(address, status.redact_private_addresses(f"peer={address}.", {status.address_key(address)}))

    def test_canonical_mac_address_forms_are_redacted_without_hiding_linux(self):
        for private, variants in [
            ("2001:db8::17", ["2001:0db8:0:0:0:0:0:0017", "2001:DB8::17", "2001:db8::17%en0"]),
            ("192.0.2.17", ["::ffff:192.0.2.17", "::ffff:c000:211", "192.0.2.17"]),
        ]:
            for variant in variants:
                with self.subTest(variant=variant):
                    value = {variant: [f"peer=[{variant}]:8232; linux=192.0.2.18"]}
                    result = json.dumps(status.redact_private_addresses(value, {status.address_key(private)}))
                    self.assertNotIn(variant, result)
                    self.assertIn("192.0.2.18", result)
                    self.assertIn("[redacted-address]", result)

    def test_multiple_protected_addresses_in_one_detail_are_removed(self):
        protected = {status.address_key('2001:db8:1:2:3:4:5:17'), status.address_key('192.0.2.17')}
        value = 'dead:beef:2001:db8:1:2:3:4:5:17 / peer:::ffff:c000:211 / ::ffff:192.0.2.17:8233'
        result = status.redact_private_addresses(value, protected)
        self.assertEqual(result.count('[redacted-address]'), 3)
        self.assertNotIn('2001:db8:1:2:3:4:5:17', result)
        self.assertNotIn('192.0.2.17', result)

    def test_private_addresses_in_invalid_outer_tokens_are_redacted(self):
        for private, variants in [
            ("192.0.2.17", ["peer:::ffff:192.0.2.17", "::ffff:192.0.2.17:8233",
                            "peer:::ffff:c000:211", "::ffff:c000:211:8233"]),
            ("2001:db8::17", ["peer:2001:db8::17", "2001:db8::17:8233",
                             "face:2001:0db8:0:0:0:0:0:0017"]),
            ("2001:db8:1:2:3:4:5:17", ["dead:beef:2001:db8:1:2:3:4:5:17",
                                        "dead:beef:2001:0db8:0001:0002:0003:0004:0005:0017:8233"]),
        ]:
            for variant in variants:
                with self.subTest(variant=variant):
                    value = {"diagnostic": variant, "linux": "192.0.2.18"}
                    result = json.dumps(status.redact_private_addresses(value, {status.address_key(private)}))
                    self.assertNotIn(variant, result)
                    self.assertIn("[redacted-address]", result)
                    self.assertIn("192.0.2.18", result)

    def test_missing_or_invalid_config_fails_closed_for_mac_dashboard(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private.json"
            with mock.patch.object(status, "PRIVATE_ADDRESS_FILE", path), \
                    mock.patch.dict(os.environ, {"ZAKURA_MAC_CRANELIFT_STATUS": "1"}):
                with self.assertRaises(ValueError):
                    status.private_addresses()
                for value in [[], {}, ["not an address"], [1]]:
                    path.write_text(json.dumps(value))
                    with self.assertRaises(ValueError):
                        status.private_addresses()
                path.write_text(json.dumps(["192.0.2.17"]))
                self.assertEqual(status.private_addresses(), {status.address_key("192.0.2.17")})

    def test_non_address_values_survive(self):
        payload = {"time": "2026-09-30T11:25:27Z", "version": "1.97.1", "height": 3500000,
                   "hash": "a" * 64, "bad": "999.999.999.999", "available": True}
        self.assertEqual(status.redact_private_addresses(payload, {status.address_key("192.0.2.17")}), payload)



class HttpHandlerTests(unittest.TestCase):
    def setUp(self):
        self.original_collector = status.COLLECTOR
        self.original_limiter = status.RATE_LIMITER
        status.COLLECTOR = collector()
        status.COLLECTOR.rows = [public_row(observed_at=time.time())]
        status.RATE_LIMITER = status.RateLimiter(limit=100, window=60)
        self.server = status.ThreadingHTTPServer(
            ("127.0.0.1", 0),
            status.Handler,
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        status.COLLECTOR = self.original_collector
        status.RATE_LIMITER = self.original_limiter

    def test_synthetic_mac_detail_page_and_data_are_available(self):
        sample = {"available": True, "verifier_id": "verifier-" + "a" * 32,
                  "mac_tip": 100, "mac_tip_hash": "b" * 64,
                  "comparison_healthy": True}
        status.COLLECTOR.network = "mainnet"
        with mock.patch.dict(os.environ, {"ZAKURA_MAC_CRANELIFT_STATUS": "1"}), \
                mock.patch.object(status, "mac_cranelift_status", return_value=sample), \
                mock.patch.object(status, "probe_node", return_value={}):
            status.COLLECTOR.poll_once()
        self.assertNotIn("mac-os-cranelift", status.COLLECTOR.nodes_by_name)
        with urllib.request.urlopen(f"{self.base_url}/node/mac-os-cranelift") as response:
            self.assertEqual(response.read(), status.PAGE.encode())
        with urllib.request.urlopen(f"{self.base_url}/data/node/mac-os-cranelift") as response:
            detail = json.load(response)
        self.assertEqual(detail["node"]["name"], "mac-os-cranelift")
        self.assertEqual(detail["node"]["ssh"], "")
        self.assertTrue(all(value == "" for value in detail["config"].values()))
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(f"{self.base_url}/node/unknown")
        self.assertEqual(caught.exception.code, 404)

    def test_get_status_sets_public_headers(self):
        request = urllib.request.Request(
            f"{self.base_url}/ironwood-status.json",
            headers={"Origin": "https://zakura.com"},
        )

        with urllib.request.urlopen(request) as response:
            payload = json.load(response)

        self.assertEqual(response.status, 200)
        self.assertEqual(
            response.headers["Content-Type"],
            "application/json; charset=utf-8",
        )
        self.assertEqual(
            response.headers["Access-Control-Allow-Origin"],
            "https://zakura.com",
        )
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(payload["network"], "testnet")

    def test_public_response_preserves_linux_but_redacts_mac_in_any_row(self):
        status.COLLECTOR.rows[0]["ssh"] = "operator@192.0.2.18"
        status.COLLECTOR.rows[0]["rpc_metadata_error"] = "peer 192.0.2.17 and [2001:db8::17] failed; Linux [2001:db8::18]"
        protected = {status.address_key(value) for value in ["192.0.2.17", "2001:db8::17"]}
        with mock.patch.object(status, "private_addresses", return_value=protected):
            with urllib.request.urlopen(f"{self.base_url}/data") as response:
                body = response.read().decode()
        self.assertNotIn("192.0.2.17", body)
        self.assertNotIn("2001:db8::17", body)
        self.assertIn("operator@192.0.2.18", body)
        self.assertIn("2001:db8::18", body)
        self.assertIn("[redacted-address]", body)

    def test_colon_prefixed_peer_details_do_not_expose_private_addresses(self):
        status.COLLECTOR.rows[0]["peer_subversions"] = [
            ["peer:::ffff:192.0.2.17", 1], ["peer:2001:db8::17", 1]]
        protected = {status.address_key("192.0.2.17"), status.address_key("2001:db8::17")}
        with mock.patch.object(status, "private_addresses", return_value=protected):
            with urllib.request.urlopen(f"{self.base_url}/data/node/node-a") as response:
                body = response.read().decode()
        self.assertNotIn("192.0.2.17", body)
        self.assertNotIn("2001:db8::17", body)
        self.assertIn("[redacted-address]", body)

    def test_private_configuration_failure_never_serves_unfiltered_json(self):
        status.COLLECTOR.rows[0]["rpc_metadata_error"] = "private 192.0.2.17"
        with mock.patch.object(status, "private_addresses", side_effect=ValueError("private fixture")):
            with urllib.request.urlopen(f"{self.base_url}/data") as response:
                self.assertEqual(response.status, 200)
                body = response.read().decode()
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(f"{self.base_url}/data/node/node-a")
        self.assertEqual(caught.exception.code, 503)
        caught.exception.close()
        self.assertEqual(json.loads(body)["rows"][0]["height"], 4_201_000)
        self.assertNotIn("192.0.2.17", body)
        self.assertNotIn("private fixture", body)

    def test_privacy_failure_keeps_real_http_watchdog_node_alerts_working(self):
        spec = importlib.util.spec_from_file_location(
            "degraded_watchdog", SCRIPT_PATH.with_name("zakura-cluster-watchdog.py"))
        watchdog = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = watchdog
        spec.loader.exec_module(watchdog)
        with mock.patch.object(sys, "argv", ["watchdog", "--config", "unused", "--dry-run"]):
            args = watchdog.parse_args()
        agent = watchdog.Watchdog([], args)
        messages = []
        agent.notify = lambda text, _: (messages.append(text), True)[1]
        fleet = watchdog.Fleet("mainnet", f"{self.base_url}/data", self.base_url)
        state = {}
        status.COLLECTOR.rows[0].update(health="down", detail="private 192.0.2.17")
        with mock.patch.object(status, "private_addresses", side_effect=OSError("fixture")):
            for now in (1000, 1600):
                status.COLLECTOR.last_poll = now
                agent.observe_fleet(state, fleet, now, False)
        self.assertEqual(len(messages), 1)
        self.assertIn("node-a", messages[0])
        self.assertNotIn("192.0.2.17", messages[0])
        self.assertTrue(state["nodes"]["mainnet/node-a"]["alerting"])
        self.assertEqual(state["fleets"]["mainnet"]["condition"], "ok")

    def test_degraded_snapshot_rejects_untyped_and_diagnostic_fields(self):
        value = {"network": "testnet", "last_poll": 1000, "chain": {"error": "private 192.0.2.17"},
                 "rows": [{"name": "node-a", "health": "healthy", "height": 100,
                           "block_hash": "a" * 64, "seconds_since_advanced": 30,
                           "ancestor_hashes": {"10": "b" * 64, "host": "192.0.2.17"},
                           "ssh": "192.0.2.17", "detail": "192.0.2.17"},
                          {"name": "mac-os-cranelift", "health": "private 192.0.2.17",
                           "height": "192.0.2.17", "block_hash": "192.0.2.17",
                           "seconds_since_advanced": float("inf")},
                          {"name": "192.0.2.17", "health": "healthy"}]}
        result = status.monitoring_snapshot(value, {"node-a", "192.0.2.17"})
        self.assertNotIn("192.0.2.17", json.dumps(result))
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["rows"][0]["ancestor_hashes"], {"10": "b" * 64})
        self.assertEqual(result["rows"][1]["health"], "down")
        self.assertNotIn("height", result["rows"][1])
        self.assertNotIn("seconds_since_advanced", result["rows"][1])

    def test_options_returns_204_for_allowed_origin(self):
        request = urllib.request.Request(
            f"{self.base_url}/ironwood-status.json",
            method="OPTIONS",
            headers={
                "Origin": "http://localhost:1111",
                "Access-Control-Request-Method": "GET",
            },
        )

        with urllib.request.urlopen(request) as response:
            body = response.read()

        self.assertEqual(response.status, 204)
        self.assertEqual(body, b"")
        self.assertEqual(
            response.headers["Access-Control-Allow-Origin"],
            "http://localhost:1111",
        )
        self.assertEqual(
            response.headers["Access-Control-Allow-Methods"],
            "GET, OPTIONS",
        )

    def test_mainnet_rejects_testnet_development_origin(self):
        status.COLLECTOR = collector("mainnet")
        status.COLLECTOR.rows = [
            public_row(
                height=3_425_000,
                network="mainnet",
                observed_at=time.time(),
            )
        ]
        request = urllib.request.Request(
            f"{self.base_url}/ironwood-status.json",
            headers={"Origin": "http://localhost:1111"},
        )

        with urllib.request.urlopen(request) as response:
            response.read()

        self.assertIsNone(response.headers["Access-Control-Allow-Origin"])
        self.assertEqual(response.headers["Vary"], "Origin")

    def test_healthz_is_a_small_liveness_response(self):
        with urllib.request.urlopen(f"{self.base_url}/healthz") as response:
            body = response.read()

        self.assertEqual(response.status, 200)
        self.assertEqual(body, b"ok\n")

    def test_unknown_route_returns_404(self):
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(f"{self.base_url}/unknown")

        self.assertEqual(context.exception.code, 404)
        context.exception.close()

    def test_node_route_serves_the_same_page_as_the_fleet(self):
        with urllib.request.urlopen(f"{self.base_url}/") as response:
            fleet = response.read()
        with urllib.request.urlopen(f"{self.base_url}/node/node-a") as response:
            detail = response.read()

        self.assertEqual(response.status, 200)
        self.assertEqual(
            response.headers["Content-Type"],
            "text/html; charset=utf-8",
        )
        # One template, two routes: the client branches on location.pathname.
        self.assertEqual(fleet, detail)

    def test_page_and_data_are_never_cached(self):
        # The HTML has no fingerprint, so a cached copy keeps showing an old
        # build after a dashboard deploy.
        for path in ("/", "/node/node-a", "/data", "/data/node/node-a"):
            with self.subTest(path=path):
                with urllib.request.urlopen(f"{self.base_url}{path}") as response:
                    response.read()

                self.assertEqual(
                    response.headers["Cache-Control"],
                    "no-store, must-revalidate",
                )

    def test_node_route_rejects_an_unknown_name(self):
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(f"{self.base_url}/node/not-a-node")

        self.assertEqual(context.exception.code, 404)
        context.exception.close()

    def test_node_data_route_returns_the_detail_payload(self):
        with urllib.request.urlopen(f"{self.base_url}/data/node/node-a") as response:
            payload = json.load(response)

        self.assertEqual(response.status, 200)
        self.assertEqual(payload["node"]["name"], "node-a")
        self.assertEqual(payload["network"], "testnet")
        self.assertIn("history", payload)
        self.assertIn("config", payload)

    def test_node_data_route_404s_for_an_unknown_name(self):
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(f"{self.base_url}/data/node/not-a-node")

        self.assertEqual(context.exception.code, 404)
        context.exception.close()


# --------------------------------------------------------------------------- #
# NU7 configured-network status
# --------------------------------------------------------------------------- #

NU7_SAMPLE = json.loads((Path(__file__).parent / "testdata" / "nu7-status-v1-sample.json")
                        .read_text())
NU7_ACTIVATION = 10
NU7_BRANCH = "77190ad9"


def nu7_node(name, **overrides):
    values = dict(
        name=name, ssh_string=f"root@{name}", probe_kind="zebra", service_name="zakurad",
        bin_path="/usr/local/bin/zakurad", log_file="", rpc_listen_addr="",
        rpc_auth="", rpc_config_path="", rpc_user="", rpc_password="",
        process_pattern="", container_name="", node_id="",
    )
    values.update(overrides)
    return status.Node(**values)


def nu7_validators():
    return [
        nu7_node("fork-1", local=True, label="primary", rpc_listen_addr="127.0.0.1:18232",
                 internal_miner=True),
        nu7_node("fork-2", local=True, label="local observer",
                 rpc_listen_addr="127.0.0.1:18242"),
        nu7_node("us", status_url="http://203.0.113.1:8094/v1/miner", miner_id="us",
                 region="San Francisco, US", internal_miner=True),
        nu7_node("eu", status_url="http://203.0.113.2:8094/v1/miner", miner_id="eu",
                 region="Amsterdam, NL", internal_miner=True),
        nu7_node("ap", status_url="http://203.0.113.3:8094/v1/miner", miner_id="ap",
                 region="Singapore, SG", internal_miner=True),
    ]


def chain_hash(number):
    return f"{number:064x}"


def nu7_row(name, height=12, *, block_hash=None, local=True, miner_active=True, **extra):
    row = {
        "name": name, "healthy": True, "rpc_ok": True, "active_state": "active",
        "rpc_chain": "test",
        "height": height, "block_hash": block_hash or chain_hash(height),
        "ancestor_hashes": {"1": chain_hash(height - 1), "2": chain_hash(height - 2)},
        "nu7": {"branch_id": NU7_BRANCH, "activation_height": NU7_ACTIVATION},
        "peer_count": 5 if local else None, "peer_external": 4 if local else None,
        "nsm_value_balance_zat": 125, "nsm_reissuance_known": False,
        "nsm_reissuance_height": None,
        "miner": None if local else {"active": miner_active, "accepted_blocks_24h": 3,
                                     "observed_at": 1000.0},
    }
    row.update(extra)
    return row


def nu7_rows(**overrides):
    rows = {name: nu7_row(name, local=name.startswith("fork"))
            for name in ("fork-1", "fork-2", "us", "eu", "ap")}
    rows.update(overrides)
    return list(rows.values())


class Nu7StatusTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = Path(self.temp.name) / "zakura.toml"
        self.config.write_text(
            "[network]\n"
            'network = { network_name = "Nu7StagingV3", network_magic = [122, 107, 117, 57], '
            "initial_nsm_value_balance = 55768414957, inherit_activation_heights = true, "
            f"activation_heights = {{ NU7 = {NU7_ACTIVATION} }} }}\n"
        )
        self.view = status.Nu7Status(nu7_validators(), self.config)
        self.rpc_calls = []
        self.rpc_patch = mock.patch.object(self.view, "rpc", side_effect=self.fake_rpc)
        self.rpc_patch.start()
        self.addCleanup(self.rpc_patch.stop)

    def fake_rpc(self, method, params=None):
        self.rpc_calls.append((method, params))
        if method == "getblockhash":
            return chain_hash(params[0])
        if method == "getblockheader":
            number = int(params[0], 16)
            return {"height": number, "hash": params[0], "time": 1000 + 30 * number,
                    "difficulty": 3.0}
        raise AssertionError(method)

    def test_payload_keeps_the_schema_version_1_contract(self):
        payload = self.view.build(nu7_rows(), 2000)

        def keys(value):
            return set(value) if isinstance(value, dict) else set()

        self.assertEqual(payload["schemaVersion"], 1)
        self.assertLessEqual(keys(NU7_SAMPLE), keys(payload))
        for section in ("network", "chain", "nsm", "observation", "mining"):
            self.assertLessEqual(keys(NU7_SAMPLE[section]), keys(payload[section]), section)
        self.assertLessEqual(keys(NU7_SAMPLE["mining"]["remoteMiners"][0]),
                             keys(payload["mining"]["remoteMiners"][0]))
        self.assertLessEqual(keys(NU7_SAMPLE["nodes"][0]), keys(payload["nodes"][0]))
        self.assertEqual(keys(NU7_SAMPLE["recentBlocks"][0]), keys(payload["recentBlocks"][0]))
        # The fields the website parser requires, with the types it checks.
        chain = payload["chain"]
        self.assertTrue(re.fullmatch(r"[0-9a-f]{64}", chain["hash"]))
        self.assertGreater(chain["height"], 0)
        self.assertGreater(chain["blockTime"], 0)
        self.assertGreater(chain["difficulty"], 0)
        self.assertIsInstance(chain["intervalSampleBlocks"], int)
        self.assertIsInstance(payload["observation"]["reorgs24h"], int)

    def test_network_identity_and_nsm_balance(self):
        payload = self.view.build(nu7_rows(), 2000)

        self.assertEqual(payload["network"], {
            "name": "Nu7StagingV3", "magic": "7a6b7539", "activationHeight": NU7_ACTIVATION,
            "nsmSeedZat": 55768414957, "targetSpacingSeconds": 25, "daaWindowBlocks": 102,
            "branchId": NU7_BRANCH, "reissuanceHeight": None, "reissuanceKnown": False,
        })
        self.assertEqual(payload["nsm"], {"balanceZat": 125, "available": True,
                                          "seedZat": 55768414957})

    def test_five_validators_agree_and_remote_miners_are_healthy(self):
        payload = self.view.build(nu7_rows(), 2000)

        self.assertEqual(payload["status"], "live")
        self.assertTrue(payload["observation"]["validatorsAgree"])
        self.assertEqual(payload["observation"]["validatorsAgreeing"], 5)
        self.assertEqual(payload["observation"]["validatorsConfigured"], 5)
        self.assertEqual([node["name"] for node in payload["nodes"]],
                         ["primary", "local observer", "us", "eu", "ap"])
        self.assertEqual(payload["mining"]["operatorMinersActive"], 4)
        self.assertEqual(payload["mining"]["operatorMinersConfigured"], 4)
        self.assertEqual([miner["id"] for miner in payload["mining"]["remoteMiners"]],
                         ["us", "eu", "ap"])
        self.assertTrue(all(m["healthy"] for m in payload["mining"]["remoteMiners"]))

    def test_one_remote_validator_on_another_chain_degrades_the_network(self):
        payload = self.view.build(nu7_rows(eu=nu7_row("eu", local=False,
                                                      block_hash="f" * 64)), 2000)

        self.assertEqual(payload["status"], "degraded")
        self.assertTrue(payload["observation"]["localNodesAgree"])
        self.assertFalse(payload["observation"]["validatorsAgree"])
        self.assertEqual(payload["observation"]["validatorsAgreeing"], 4)
        eu = next(m for m in payload["mining"]["remoteMiners"] if m["id"] == "eu")
        self.assertFalse(eu["healthy"])
        self.assertEqual(payload["mining"]["operatorMinersActive"], 3)

    def test_lagging_and_leading_validators_are_compared_on_the_common_chain(self):
        rows = nu7_rows(
            us=nu7_row("us", 10, local=False),      # two behind, same chain
            eu=nu7_row("eu", 13, local=False),      # one ahead, extends the tip
            ap=nu7_row("ap", 9, local=False),       # three behind: too far
        )
        payload = self.view.build(rows, 2000)

        lags = {m["id"]: (m["healthy"], m["lagBlocks"]) for m in payload["mining"]["remoteMiners"]}
        self.assertEqual(lags, {"us": (True, 2), "eu": (True, -1), "ap": (False, 3)})

    def test_an_inactive_remote_miner_is_not_counted(self):
        payload = self.view.build(nu7_rows(ap=nu7_row("ap", local=False, miner_active=False)),
                                  2000)

        self.assertEqual(payload["status"], "live")
        self.assertEqual(payload["mining"]["operatorMinersActive"], 3)

    def test_a_stalled_chain_still_publishes_its_tip(self):
        # Fleet health also needs recent progress; the public feed must not turn a
        # stalled but reachable primary into an RPC outage.
        rows = nu7_rows(**{name: nu7_row(name, local=name.startswith("fork"), healthy=False)
                           for name in ("fork-1", "fork-2", "us", "eu", "ap")})
        payload = self.view.build(rows, 9000)

        self.assertEqual(payload["status"], "live")
        self.assertEqual(payload["chain"]["height"], 12)
        self.assertEqual(payload["chain"]["tipAgeSeconds"], 9000 - (1000 + 30 * 12))

    def test_a_stopped_or_unreachable_primary_is_unavailable(self):
        for broken in ({"active_state": "inactive"}, {"rpc_ok": False}):
            rows = nu7_rows(**{"fork-1": nu7_row("fork-1", **broken)})
            self.assertEqual(self.view.build(rows, 2000)["status"], "unavailable", broken)

    def test_mismatched_activation_is_unavailable(self):
        rows = nu7_rows()
        rows[0]["nu7"] = {"branch_id": NU7_BRANCH, "activation_height": NU7_ACTIVATION + 1}
        payload = self.view.build(rows, 2000)

        self.assertEqual(payload["status"], "unavailable")
        self.assertEqual(payload["error"], "Primary node RPC unavailable")
        self.view.update(rows, 2000)
        self.assertEqual(self.view.response(2000)[0], 503)

    def test_interval_window_uses_300_intervals_and_caches_headers(self):
        first = self.view.build(nu7_rows(**{"fork-1": nu7_row("fork-1", 400)}), 2000)
        fetched = len(self.rpc_calls)
        self.assertEqual(first["chain"]["intervalSampleBlocks"], 300)
        self.assertEqual(first["chain"]["medianIntervalSeconds"], 30)
        self.assertEqual(len(first["recentBlocks"]), 8)

        self.rpc_calls.clear()
        self.view.build(nu7_rows(**{"fork-1": nu7_row("fork-1", 401)}), 2010)
        header_calls = [call for call in self.rpc_calls if call[0] == "getblockheader"]
        self.assertEqual(len(header_calls), 1)
        self.assertGreater(fetched, 600)

    def test_interval_window_starts_at_nu7_activation(self):
        payload = self.view.build(nu7_rows(), 2000)

        self.assertEqual(payload["chain"]["intervalSampleBlocks"], 2)

    def test_generation_boundary_counters_exclude_the_previous_network(self):
        view = status.Nu7Status(nu7_validators(), self.config, generation_start=5000)
        view.started_at = 3000
        with mock.patch.object(view, "rpc", side_effect=self.fake_rpc):
            view.build(nu7_rows(), 4000)
            payload = view.build(nu7_rows(**{"fork-1": nu7_row("fork-1", 13)}), 4500)
            self.assertEqual(payload["observation"]["blocks24h"], 0)
            self.assertEqual(payload["observation"]["since"], 5000)
            payload = view.build(nu7_rows(**{"fork-1": nu7_row("fork-1", 14)}), 5100)
        self.assertEqual(payload["observation"]["blocks24h"], 1)

    def test_a_lower_tip_is_a_reorg_that_drops_cached_headers(self):
        self.view.build(nu7_rows(**{"fork-1": nu7_row("fork-1", 12)}), 2000)
        payload = self.view.build(nu7_rows(**{"fork-1": nu7_row("fork-1", 11)}), 2010)

        self.assertEqual(payload["observation"]["reorgs24h"], 1)

    def test_failures_are_logged_and_the_public_payload_stays_generic(self):
        self.rpc_patch.stop()
        with mock.patch.object(self.view, "rpc", side_effect=OSError("private host 10.0.0.1")), \
                self.assertLogs(level="ERROR") as logs:
            self.view.update(nu7_rows(), time.time())
        self.rpc_patch.start()
        code, payload = self.view.response()
        self.assertEqual(code, 503)
        self.assertNotIn("10.0.0.1", json.dumps(payload))
        self.assertIn("10.0.0.1", "\n".join(logs.output))

    def test_stale_observations_are_unavailable(self):
        self.view.update(nu7_rows(), 1000)

        self.assertEqual(self.view.response(1000)[0], 200)
        self.assertEqual(self.view.response(1000 + status.PUBLIC_STATUS_MAX_AGE + 1)[0], 503)

    def test_the_primary_must_be_local_with_rpc(self):
        nodes = nu7_validators()
        nodes[0].local = False
        with self.assertRaises(SystemExit):
            status.Nu7Status(nodes, self.config)


class Nu7SourceTests(unittest.TestCase):
    REPORT = {"observedAt": 1000.0, "minerActive": True, "nodeActive": True,
              "acceptedBlocks24h": 7, "nodeHealthy": True, "height": 12,
              "hash": chain_hash(12), "branchId": NU7_BRANCH, "activationHeight": 10,
              "recentHashes": {"10": chain_hash(10), "11": chain_hash(11), "12": chain_hash(12)}}

    def test_public_and_obsolete_network_configs_are_rejected(self):
        parameters = ('network_name = "Nu7StagingV3", network_magic = [122, 107, 117, 57], '
                      'initial_nsm_value_balance = 55768414957, activation_heights = { NU7 = 10 }')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "zakura.toml"
            for extra in ("", f"testnet_parameters = {{ {parameters} }}\n"):
                with self.subTest(obsolete=bool(extra)):
                    path.write_text('[network]\nnetwork = "Testnet"\n' + extra)
                    with self.assertRaisesRegex(ValueError, "explicit network parameter table"):
                        status.nu7_network_parameters(path)

    def test_a_remote_report_becomes_probe_fields(self):
        probe = status.status_report_probe(self.REPORT, 1010)

        self.assertEqual(probe["height"], 12)
        self.assertEqual(probe["active_state"], "active")
        self.assertEqual(probe["ancestor_hashes"], {"1": chain_hash(11), "2": chain_hash(10)})
        self.assertEqual(probe["nu7"], {"branch_id": NU7_BRANCH, "activation_height": 10})
        self.assertEqual(probe["miner"]["accepted_blocks_24h"], 7)

    def test_stale_or_malformed_reports_are_errors(self):
        self.assertIn("error", status.status_report_probe(self.REPORT, 1000 + 91))
        for key, value in (("height", "12"), ("height", -1), ("hash", "x"),
                           ("recentHashes", [])):
            with self.subTest(key=key):
                self.assertIn("error", status.status_report_probe({**self.REPORT, key: value},
                                                                  1010))
        self.assertIn("error", status.status_report_probe("not an object", 1010))

    def test_a_node_without_rpc_reports_its_service_state_only(self):
        probe = status.status_report_probe({**self.REPORT, "nodeHealthy": False}, 1010)

        self.assertIn("rpc_error", probe)
        self.assertNotIn("height", probe)

    def test_monitor_tables_select_local_and_status_endpoint_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.toml"
            path.write_text(
                "[defaults]\ninternal_miner = false\n"
                'testnet_parameters = { network_name = "Nu7StagingV3" }\n'
                '[[nodes]]\nname = "fork-1"\nssh_string = "root@primary"\ncommit = "main"\n'
                "internal_miner = true\n"
                'monitor = { local = true, label = "primary" }\n'
                '[[nodes]]\nname = "eu"\nssh_string = "root@203.0.113.2"\ncommit = "main"\n'
                'monitor = { status_url = "http://203.0.113.2:8094/v1/miner", id = "eu", '
                'region = "Amsterdam, NL" }\n'
            )
            primary, remote = status.load_nodes(path)
            path.write_text(path.read_text().replace(
                'monitor = { local = true, label = "primary" }',
                'monitor = { local = true, status_url = "http://x/v1/miner" }'))
            with self.assertRaises(SystemExit):
                status.load_nodes(path)

        self.assertTrue(primary.local and primary.internal_miner)
        self.assertEqual(primary.ssh_cmd("bash", "-s"), ["bash", "-s"])
        self.assertEqual(remote.status_url, "http://203.0.113.2:8094/v1/miner")
        self.assertEqual((remote.miner_id, remote.region), ("eu", "Amsterdam, NL"))
        with mock.patch.object(status, "probe_status_endpoint", return_value={"ok": 1}) as probe:
            self.assertEqual(status.probe_node(remote), {"ok": 1})
        probe.assert_called_once_with(remote)

    def test_the_probe_reports_nu7_nsm_and_external_peers(self):
        probe = RemoteProbeTests("run_probe")
        probe.setUp()
        self.addCleanup(probe.tearDown)
        probe.rpc_results = {
            "getblockchaininfo": {
                "blocks": 12, "bestblockhash": chain_hash(12), "chain": "test",
                "nsmValueBalanceZat": 125,
                "upgrades": {NU7_BRANCH: {"name": "NU7", "activationheight": 10}},
            },
            "getpeerinfo": [{"addr": "127.0.0.1:18333"}, {"addr": "203.0.113.1:18233"},
                            {"addr": "[2001:db8::1]:18233"}],
        }
        out = probe.run_probe(rpc_url=f"http://{probe.endpoint}/")

        self.assertEqual(out["nu7"], {"branch_id": NU7_BRANCH, "activation_height": 10})
        self.assertEqual(out["nsm_value_balance_zat"], 125)
        self.assertNotIn("nsm_reissuance_height", out)
        self.assertEqual(out["peer_count"], 3)
        self.assertEqual(out["peer_external"], 2)


class Nu7HttpTests(unittest.TestCase):
    def setUp(self):
        self.original_collector = status.COLLECTOR
        self.original_limiter = status.RATE_LIMITER
        status.RATE_LIMITER = status.RateLimiter(limit=100, window=60)
        self.server = status.ThreadingHTTPServer(("127.0.0.1", 0), status.Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.addCleanup(self.restore)

    def restore(self):
        self.server.shutdown()
        self.server.server_close()
        status.COLLECTOR = self.original_collector
        status.RATE_LIMITER = self.original_limiter

    def use(self, nu7):
        status.COLLECTOR = collector()
        status.COLLECTOR.nu7 = nu7

    def get(self, path, origin="https://zakura.com"):
        request = urllib.request.Request(self.base_url + path, headers={"Origin": origin})
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, response.headers, json.load(response)
        except urllib.error.HTTPError as error:
            body = error.read()
            try:
                return error.code, error.headers, json.loads(body)
            except ValueError:
                return error.code, error.headers, body

    def test_status_is_served_with_the_zakura_cors_allowlist(self):
        view = mock.Mock()
        view.response.return_value = (200, {"schemaVersion": 1, "status": "live"})
        self.use(view)

        code, headers, payload = self.get("/v1/status")
        self.assertEqual((code, payload["status"]), (200, "live"))
        self.assertEqual(headers["Access-Control-Allow-Origin"], "https://zakura.com")
        self.assertEqual(headers["Access-Control-Allow-Headers"], "Content-Type")
        self.assertEqual(headers["Cache-Control"], "public, max-age=10")
        self.assertIsNone(self.get("/v1/status", "https://untrusted.example")[1]
                          ["Access-Control-Allow-Origin"])

    def test_unavailable_status_is_503_and_not_cached(self):
        view = mock.Mock()
        view.response.return_value = (503, {"schemaVersion": 1, "status": "unavailable"})
        self.use(view)

        code, headers, _ = self.get("/v1/status")
        self.assertEqual(code, 503)
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Access-Control-Allow-Origin"], "https://zakura.com")

    def test_status_route_is_absent_without_the_nu7_view(self):
        self.use(None)

        self.assertEqual(self.get("/v1/status")[0], 404)



class BoundedServerTests(unittest.TestCase):
    def serve(self, handler, max_concurrent):
        server = status.BoundedHTTPServer(("127.0.0.1", 0), handler, max_concurrent=max_concurrent)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def test_concurrent_requests_are_capped(self):
        release, lock = threading.Event(), threading.Lock()
        state = {"active": 0, "peak": 0}

        class Slow(BaseHTTPRequestHandler):
            def do_GET(self):
                with lock:
                    state["active"] += 1
                    state["peak"] = max(state["peak"], state["active"])
                release.wait(5)
                with lock:
                    state["active"] -= 1
                self.send_response(204)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = self.serve(Slow, 2)

        def request():
            with socket.create_connection(server.server_address, timeout=10) as conn:
                conn.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                conn.recv(64)

        threads = [threading.Thread(target=request) for _ in range(6)]
        for thread in threads:
            thread.start()
        time.sleep(0.5)
        release.set()
        for thread in threads:
            thread.join(10)
        self.assertEqual(state["peak"], 2)

    def test_a_stalled_client_frees_its_slot_after_the_timeout(self):
        quick = type("Quick", (status.Handler,), {"timeout": 0.3})
        server = self.serve(quick, 1)
        original = status.COLLECTOR
        status.COLLECTOR = collector()
        self.addCleanup(setattr, status, "COLLECTOR", original)

        with socket.create_connection(server.server_address, timeout=5) as stalled:
            stalled.sendall(b"GET /healthz HTTP/1.1\r\n")  # never finishes its headers
            started = time.monotonic()
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/healthz", timeout=5) as response:
                self.assertEqual(response.status, 200)
            # The only slot was held until the stalled client timed out.
            self.assertGreaterEqual(time.monotonic() - started, 0.25)

    def test_the_entry_point_uses_the_bounded_server_and_timeout(self):
        self.assertEqual(status.Handler.timeout, status.REQUEST_TIMEOUT_SECONDS)
        self.assertIn("BoundedHTTPServer((args.host, args.port), Handler)",
                      SCRIPT_PATH.read_text())


if __name__ == "__main__":
    unittest.main()
