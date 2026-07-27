#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import sys
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
