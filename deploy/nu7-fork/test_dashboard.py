"""Focused checks for the public status feed's network and metric claims."""

import io
import json
import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
