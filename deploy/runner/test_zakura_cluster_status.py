#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path


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
        upgrade_height=0,
        target_spacing=7.5,
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


class IronwoodStatusTests(unittest.TestCase):
    def test_remote_probe_is_valid_python_and_uses_required_rpcs(self):
        compile(status.REMOTE_PROBE, "<remote-probe>", "exec")
        self.assertIn('rpc_call("getblockchaininfo")', status.REMOTE_PROBE)
        self.assertIn('rpc_call("getinfo")', status.REMOTE_PROBE)
        self.assertIn('blockchain_info.get("headers")', status.REMOTE_PROBE)
        self.assertIn('"getblockhash"', status.REMOTE_PROBE)
        self.assertIn('rpc_call("getblockheader"', status.REMOTE_PROBE)

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
        self.assertTrue(split["compat_split"])

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
                upgrade_height=0,
                target_spacing=7.5,
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
                upgrade_height=0,
                target_spacing=7.5,
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
                upgrade_height=0,
                target_spacing=7.5,
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
                upgrade_height=0,
                target_spacing=7.5,
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


if __name__ == "__main__":
    unittest.main()
