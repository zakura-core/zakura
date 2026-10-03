"""Activation boundary and durable selection tests with deterministic RPCs."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from nu7_activation import ActivationFeed, atomic_json, rules


def parameters(height):
    return {"network": "Testnet", "effectiveHeight": height, "networkMagic": "fa1af9bf", "activationHeight": 100,
            "branchId": "77190ad9", "nu7BranchId": "77190ad9", "targetSpacingSeconds": 25 if height >= 100 else 75,
            "difficulty": {"averagingWindowBlocks": 102 if height >= 100 else 17,
                           "minimumDifficultyGapMultiplier": 18 if height >= 100 else 6,
                           "minimumDifficultyGapSeconds": 450, "minimumDifficultyStrictlyGreater": True}}


def manifest(magic="fa1af9bf"):
    config = '[network]\nnetwork = "Testnet"\n'
    return {"schemaVersion": 1, "nodeRevision": "a" * 40, "config": config,
            "configSha256": hashlib.sha256(config.encode()).hexdigest(),
            "network": {"name": "Testnet", "magic": magic,
                        "activationHeight": 100, "branchId": "77190ad9"}}


class FakeRPC:
    def __init__(self):
        self.height = 102
        self.bad = set()
        self.offline = set()
        self.calls = []

    def __call__(self, url, method, params=None):
        self.calls.append((url, method, params))
        if url in self.offline:
            raise OSError("offline")
        if method == "getblockchaininfo":
            return {"chain": "test", "blocks": self.height, "bestblockhash": f"{self.height:064x}",
                    "nsmValueBalanceZat": 700, "upgrades": {
                        "77190ad9": {"activationheight": 100, "status": "active" if self.height >= 100 else "pending"}}}
        if method == "getblockhash":
            if url in self.bad and params[0] >= 100:
                return "f" * 64
            return f"{params[0]:064x}"
        if method == "getblockheader":
            return {"height": int(params[0], 16), "hash": params[0], "time": 990 - (102 - int(params[0], 16)) * 25, "difficulty": 3}
        if method == "getnetworkparameters":
            return parameters(params[0])
        raise AssertionError(method)


class Staging:
    def response(self):
        return 200, {"status": "live", "observedAt": 1000, "chain": {"height": 102}}


class ActivationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.now = 1000
        self.rpc = FakeRPC()
        self.config = {"armed": True, "stateFile": str(self.root / "state.json"),
                       "staging": {"manifest": manifest(), "rpcUrl": "staging"},
                       "public": {"manifest": manifest(), "identityCheckpoint": {"height": 90, "hash": f"{90:064x}"},
                                  "nodes": [{"name": str(i), "rpcUrl": str(i)} for i in range(3)],
                                  "reference": {"name": "reference", "rpcUrl": "reference"}}}
        self.config_path = self.root / "config.json"
        self.config_path.write_text(json.dumps(self.config))
        self.feed = ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

    def tearDown(self):
        self.feed.owner.close()
        self.temporary.cleanup()

    def poll(self):
        self.feed.poll()
        self.now += 10

    def test_three_consecutive_passes_select_once(self):
        self.poll()
        self.poll()
        self.assertEqual(self.feed.response()[1]["selectedProfile"], "staging")
        self.poll()
        payload = self.feed.response()[1]
        self.assertEqual(payload["selectedProfile"], "public-testnet")
        self.assertEqual(payload["status"]["nsm"]["balanceZat"], 700)
        self.assertEqual(payload["rules"]["atTip"]["minimumDifficulty"]["thresholdSeconds"], 450)
        self.assertEqual(payload["network"]["configSha256"], manifest()["configSha256"])

    def test_no_selection_before_confirmation_height(self):
        for height in (99, 100, 101):
            self.rpc.height = height
            for _ in range(3):
                self.poll()
            self.assertEqual(self.feed.state["selectedProfile"], "staging")

    def test_reference_disagreement_resets_consecutive_passes(self):
        self.poll()
        self.rpc.bad.add("reference")
        self.poll()
        self.assertEqual(self.feed.state["consecutivePasses"], 0)
        self.rpc.bad.clear()
        self.poll()
        self.poll()
        self.assertEqual(self.feed.state["selectedProfile"], "staging")
        self.poll()
        self.assertEqual(self.feed.state["selectedProfile"], "public-testnet")

    def test_two_nodes_suffice_but_reference_is_mandatory(self):
        self.rpc.offline.add("2")
        for _ in range(3):
            self.poll()
        self.assertEqual(self.feed.state["selectedProfile"], "public-testnet")
        self.rpc.offline.add("reference")
        self.poll()
        self.assertEqual(self.feed.response()[1]["selectedProfile"], "public-testnet")
        self.assertEqual(self.feed.response()[1]["status"]["status"], "degraded")

    def test_restart_and_outage_never_restore_staging(self):
        for _ in range(3):
            self.poll()
        self.feed.owner.close()
        self.feed = ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)
        self.rpc.offline.update({"0", "1", "2", "reference"})
        self.poll()
        payload = self.feed.response()[1]
        self.assertEqual(payload["selectedProfile"], "public-testnet")
        self.assertEqual(payload["status"]["status"], "unavailable")
        self.assertEqual(payload["generatedAt"], 1020)

    def test_stalled_sources_cannot_select(self):
        self.poll()
        self.now += 301
        self.poll()
        self.assertEqual(self.feed.state["consecutivePasses"], 0)

    def test_process_lock_and_corrupt_state_fail_closed(self):
        with self.assertRaises(BlockingIOError):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)
        self.feed.owner.close()
        self.root.joinpath("state.json").write_text("bad")
        with self.assertRaises(json.JSONDecodeError):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

    def test_wrong_identity_and_manifest_checksum_rejected(self):
        self.config["public"]["manifest"]["configSha256"] = "bad"
        self.config_path.write_text(json.dumps(self.config))
        self.feed.owner.close()
        with self.assertRaisesRegex(ValueError, "checksum"):
            ActivationFeed(self.config_path, Staging(), lambda: self.now, self.rpc)

    def test_public_selection_persists_even_when_parameters_missing(self):
        self.feed.public_status = mock.Mock(side_effect=ValueError("failed"))
        for _ in range(3):
            self.poll()
        self.assertEqual(json.loads(self.feed.path.read_text())["selectedProfile"], "public-testnet")
        self.assertEqual(self.feed.response()[1]["status"]["status"], "unavailable")

    def test_dispatch_failure_retries_then_stops_after_success(self):
        self.feed.config["dispatch"] = True
        self.feed.state["selectionId"] = "test"
        self.feed.state["lastPublicEnvelope"] = {"test": True}
        with mock.patch.dict("os.environ", {"NU7_DISPATCH_TOKEN": "test-only"}), \
                mock.patch("urllib.request.urlopen", side_effect=OSError):
            self.feed.dispatch()
        self.assertNotIn("publicationDispatched", self.feed.state)
        response = mock.MagicMock()
        response.__enter__.return_value.status = 204
        with mock.patch.dict("os.environ", {"NU7_DISPATCH_TOKEN": "test-only"}), \
                mock.patch("urllib.request.urlopen", return_value=response) as request:
            self.feed.dispatch()
            self.feed.dispatch()
        self.assertEqual(request.call_count, 1)
        self.assertTrue(self.feed.state["publicationDispatched"])

    def test_next_block_rule_boundary(self):
        self.assertEqual(rules(parameters(99))["daaWindowBlocks"], 17)
        self.assertEqual(rules(parameters(100))["daaWindowBlocks"], 102)
        self.assertEqual(rules(parameters(100))["minimumDifficulty"]["comparison"], "strictly-greater")


if __name__ == "__main__":
    unittest.main()
