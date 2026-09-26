"""The public join manifest must match live consensus and omit operator data."""

import hashlib
import tomllib
import unittest
from pathlib import Path

import publish_network


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.config = tomllib.loads((Path(__file__).parent / "miner/testdata/fork-node.toml").read_text())
        self.activation = self.config["network"]["network"]["activation_heights"]["NU7"]
        self.info = {"chain": "test", "upgrades": {
            "77190ad9": {"name": "NU7", "activationheight": self.activation}}}
        self.seed = {"height": self.activation - 3, "hash": "a" * 64, "time": 1000}

    def make(self):
        return publish_network.manifest(self.config, self.info, "b" * 40,
                                        ["seed.nu7.valargroup.dev:18233"], self.seed)

    def test_exports_actual_consensus_without_operator_settings(self):
        self.config["mining"] = {"miner_address": "operator-only"}
        self.config["state"]["cache_dir"] = "/private/operator/state"
        result = self.make()
        public = tomllib.loads(result["config"])
        self.assertEqual(public["network"]["network"], self.config["network"]["network"])
        self.assertEqual(result["network"]["activationHeight"], self.activation)
        self.assertNotIn("mining", public)
        self.assertNotIn("operator", result["config"])
        self.assertEqual(result["configSha256"], hashlib.sha256(result["config"].encode()).hexdigest())

    def test_reconfigured_height_is_derived_and_rpc_mismatch_is_rejected(self):
        self.config["network"]["network"]["activation_heights"]["NU7"] += 3
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.make()
        self.info["upgrades"]["77190ad9"]["activationheight"] += 3
        self.assertEqual(self.make()["network"]["activationHeight"], self.activation + 3)

    def test_seed_at_activation_is_rejected(self):
        self.seed["height"] = self.activation
        with self.assertRaisesRegex(ValueError, "precede"):
            self.make()


if __name__ == "__main__":
    unittest.main()
