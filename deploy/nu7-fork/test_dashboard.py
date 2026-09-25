"""Focused checks for the public status feed's network and metric claims."""

import io
import json
import socket
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from unittest import mock

import dashboard


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        config = Path(self.temp.name) / "zakura.toml"
        config.write_text(
            '[network.testnet_parameters]\n'
            'network_name = "Nu7Fork"\n'
            'network_magic = [122, 107, 117, 55]\n'
            'initial_nsm_value_balance = 100\n'
            '[network.testnet_parameters.activation_heights]\n'
            'NU7 = 10\n',
            encoding="utf-8",
        )
        self.collector = dashboard.Collector(config, 18232, 18242)

    def test_live_metrics_distinguish_external_peers_and_nsm(self):
        def fake_rpc(port, method, params=None):
            if method == "getblockchaininfo":
                return {
                    "chain": "test", "blocks": 11, "bestblockhash": "hash11",
                    "difficulty": 2.0, "nsmValueBalanceZat": 125,
                    "upgrades": {"77190ad9": {"name": "NU7", "activationheight": 10}},
                }
            if method == "getpeerinfo":
                return [{"addr": "127.0.0.1:18333"}, {"addr": "203.0.113.1:18233"}]
            if method == "getblockhash":
                return f"hash{params[0]}"
            if method == "getblockheader":
                number = int(params[0].removeprefix("hash"))
                return {"height": number, "hash": params[0], "time": 1000 + 30 * number}
            raise AssertionError(method)

        with mock.patch.object(dashboard, "rpc", side_effect=fake_rpc), mock.patch.object(
            dashboard, "active_miners", return_value=1
        ):
            result = self.collector.collect()

        self.assertEqual(result["status"], "live")
        self.assertEqual(result["chain"]["medianIntervalSeconds"], 30)
        self.assertEqual(result["chain"]["intervalSampleBlocks"], 1)
        self.assertEqual(result["nsm"]["balanceZat"], 125)
        self.assertEqual(result["nodes"][0]["externalPeers"], 1)
        self.assertTrue(result["observation"]["localNodesAgree"])
        self.assertEqual(result["mining"]["operatorMinersConfigured"], 1)
        self.assertEqual(result["mining"]["operatorMinersActive"], 1)

    def test_mismatched_activation_is_unavailable_without_rpc_details(self):
        def fake_rpc(_port, method, _params=None):
            if method == "getblockchaininfo":
                return {"chain": "test", "upgrades": {
                    "wrong": {"name": "NU7", "activationheight": 12}
                }}
            raise AssertionError(method)

        with mock.patch.object(dashboard, "rpc", side_effect=fake_rpc):
            result = self.collector.collect()

        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["nodes"], [
            {"name": "primary", "healthy": False},
            {"name": "local observer", "healthy": False},
        ])

    def test_explorer_details_omit_raw_hex_and_link_transactions(self):
        transaction = {
            "txid": "a" * 64, "hex": "private raw bytes", "height": 11,
            "blockhash": "b" * 64, "confirmations": 3,
            "vin": [{"coinbase": "abcd", "sequence": 1}],
            "vout": [{"n": 0, "valueZat": 125, "scriptPubKey": {
                "type": "pubkeyhash", "addresses": ["test-address"]}}],
            "orchard": {"actions": [{"nullifier": "hidden detail"}]},
        }
        block = {
            "hash": "b" * 64, "height": 11, "time": 1000,
            "solution": "large proof", "tx": [transaction],
        }
        detail = dashboard.block_detail(block)
        self.assertEqual(detail["transactions"][0]["txid"], "a" * 64)
        self.assertEqual(detail["transactions"][0]["orchardActions"], 1)
        self.assertNotIn("solution", detail)
        self.assertNotIn("hex", detail["transactions"][0])

        tx_detail = dashboard.transaction_detail(transaction)
        self.assertEqual(tx_detail["outputs"][0]["valueZat"], 125)
        self.assertEqual(tx_detail["outputs"][0]["addresses"], ["test-address"])
        self.assertNotIn("hex", tx_detail)
        self.assertNotIn("actions", tx_detail)

    def test_remote_miner_requires_fresh_matching_chain_and_active_services(self):
        miner = {"id": "eu", "region": "Amsterdam", "url": "http://example/v1/miner"}
        primary = {"height": 11, "hash": "hash11", "branch": "77190ad9"}
        sample = {"observedAt": 1000, "minerActive": True, "nodeActive": True,
                  "nodeHealthy": True, "height": 11, "hash": "hash11",
                  "recentHashes": {"11": "hash11"},
                  "branchId": "77190ad9", "activationHeight": 10,
                  "acceptedBlocks24h": 3}

        with mock.patch.object(dashboard.time, "time", return_value=1002), mock.patch.object(
            dashboard.urllib.request, "urlopen", return_value=io.BytesIO(json.dumps(sample).encode())
        ):
            result = self.collector.sample_remote_miner(miner, primary)
        self.assertTrue(result["healthy"])
        self.assertEqual(result["acceptedBlocks24h"], 3)

        sample["branchId"] = "wrong"
        with mock.patch.object(dashboard.time, "time", return_value=1002), mock.patch.object(
            dashboard.urllib.request, "urlopen", return_value=io.BytesIO(json.dumps(sample).encode())
        ):
            self.assertFalse(self.collector.sample_remote_miner(miner, primary)["healthy"])

        sample["branchId"] = "77190ad9"
        sample["recentHashes"]["11"] = "different-fork"
        with mock.patch.object(dashboard.time, "time", return_value=1002), mock.patch.object(
            dashboard.urllib.request, "urlopen", return_value=io.BytesIO(json.dumps(sample).encode())
        ):
            self.assertFalse(self.collector.sample_remote_miner(miner, primary)["healthy"])

    def test_malformed_remote_miner_report_is_unhealthy_without_rpc(self):
        miner = {"id": "eu", "region": "Amsterdam", "url": "http://example/v1/miner"}
        primary = {"height": 11, "hash": "hash11", "branch": "77190ad9", "port": 1}
        base = {"observedAt": 1000, "minerActive": True, "nodeActive": True,
                "nodeHealthy": True, "height": 11, "recentHashes": {"11": "hash11"},
                "branchId": "77190ad9", "activationHeight": 10}

        for field, value in (("height", True), ("height", 1.5), ("height", -9),
                             ("recentHashes", ["hash11"])):
            sample = {**base, field: value}
            with mock.patch.object(dashboard.time, "time", return_value=1002), mock.patch.object(
                dashboard.urllib.request, "urlopen",
                return_value=io.BytesIO(json.dumps(sample).encode()),
            ), mock.patch.object(dashboard, "rpc") as rpc:
                result = self.collector.sample_remote_miner(miner, primary)
            self.assertFalse(result["healthy"], (field, value))
            rpc.assert_not_called()


class ChainRpc:
    """A fake primary and observer RPC over a chain of `{height: hash}`."""

    def __init__(self, chain: dict[int, str]):
        self.chain = chain

    def __call__(self, port, method, params=None):
        tip = max(self.chain)
        if method == "getblockchaininfo":
            return {"chain": "test", "blocks": tip, "bestblockhash": self.chain[tip],
                    "upgrades": {"77190ad9": {"name": "NU7", "activationheight": 10}}}
        if method == "getpeerinfo":
            return []
        if method == "getblockhash":
            return self.chain[params[0]]
        if method == "getblockheader":
            number = next(n for n, h in self.chain.items() if h == params[0])
            return {"height": number, "hash": params[0], "time": 1000 + 30 * number}
        raise AssertionError(method)


class CollectorChainTests(unittest.TestCase):
    def collector(self, config_text: str) -> dashboard.Collector:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        config = Path(temp.name) / "zakura.toml"
        config.write_text(config_text, encoding="utf-8")
        return dashboard.Collector(config, 18232, 18242)

    CONFIGURED_TESTNET = (
        '[network.network]\n'
        'network_name = "Nu7Fork"\n'
        'network_magic = [122, 107, 117, 55]\n'
        'initial_nsm_value_balance = 100\n'
        '[network.network.activation_heights]\n'
        'NU7 = 10\n'
    )

    def collect(self, collector, rpc):
        with mock.patch.object(dashboard, "rpc", side_effect=rpc), mock.patch.object(
            dashboard, "active_miners", return_value=1
        ):
            return collector.collect()

    def test_reads_the_configured_testnet_form(self):
        # The deployer writes a configured testnet as `network = { ... }` since #1147.
        collector = self.collector(self.CONFIGURED_TESTNET)

        self.assertEqual(collector.network["name"], "Nu7Fork")
        self.assertEqual(collector.network["activationHeight"], 10)
        self.assertEqual(collector.network["nsmSeedZat"], 100)

    def test_reports_the_tip_before_nu7_activates(self):
        collector = self.collector(self.CONFIGURED_TESTNET)

        result = self.collect(collector, ChainRpc({7: "a7", 8: "a8"}))

        self.assertEqual(result["chain"]["height"], 8)
        self.assertEqual([block["hash"] for block in result["recentBlocks"]], ["a8"])
        self.assertIsNone(result["chain"]["medianIntervalSeconds"])

    def test_a_lower_tip_is_a_reorg_that_drops_cached_headers(self):
        collector = self.collector(self.CONFIGURED_TESTNET)
        self.collect(collector, ChainRpc({10: "a10", 11: "a11", 12: "a12"}))

        result = self.collect(collector, ChainRpc({10: "b10", 11: "b11"}))

        self.assertEqual(result["observation"]["reorgs24h"], 1)
        self.assertEqual([block["hash"] for block in result["recentBlocks"]], ["b11", "b10"])

    def test_collector_failures_are_logged_with_their_cause(self):
        collector = self.collector(self.CONFIGURED_TESTNET)

        with mock.patch.object(collector, "collect", side_effect=KeyError("blocks")), \
                mock.patch.object(dashboard.time, "sleep", side_effect=KeyboardInterrupt), \
                self.assertLogs(level="ERROR") as logs, self.assertRaises(KeyboardInterrupt):
            collector.run(15)

        self.assertIn("KeyError: 'blocks'", "\n".join(logs.output))
        self.assertEqual(collector.payload["status"], "unavailable")
        self.assertNotIn("blocks", collector.payload["error"])


def max_concurrent_handlers(server_class, limit: int, clients: int) -> int:
    """Serve `clients` parallel requests through `server_class` and count the peak."""
    release = threading.Event()
    lock = threading.Lock()
    active = peak = 0

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            release.wait(5)
            with lock:
                active -= 1
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = server_class(("127.0.0.1", 0), Handler, max_concurrent=limit)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    def request():
        with socket.create_connection(server.server_address, timeout=10) as conn:
            conn.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            conn.recv(64)

    threads = [threading.Thread(target=request) for _ in range(clients)]
    for thread in threads:
        thread.start()
    time.sleep(0.5)
    release.set()
    for thread in threads:
        thread.join(10)
    server.shutdown()
    server.server_close()
    return peak


class BoundedServerTests(unittest.TestCase):
    def test_concurrent_requests_are_capped(self):
        self.assertEqual(max_concurrent_handlers(dashboard.BoundedHTTPServer, 2, 6), 2)

    def test_stalled_clients_time_out(self):
        self.assertEqual(dashboard.Handler.timeout, dashboard.REQUEST_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()
