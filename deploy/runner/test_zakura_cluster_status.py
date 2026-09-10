#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import re
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


if __name__ == "__main__":
    unittest.main()
