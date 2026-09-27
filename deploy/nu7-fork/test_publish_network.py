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

    def test_snapshot_metadata(self):
        snapshot = {"url": "https://api.nu7.valargroup.dev/snapshots/seed.tar.zst",
                    "sha256": "c" * 64, "height": self.seed["height"],
                    "sizeBytes": 9618204704, "publishedAt": 1790450304,
                    "storageMode": "pruned", "dbVersion": "29.1.0"}
        def publish(value):
            return publish_network.manifest(self.config, self.info, "b" * 40,
                                            ["seed.nu7.valargroup.dev:18233"], self.seed, value)
        self.assertEqual(publish(snapshot)["snapshot"], snapshot)
        for key, value in [("url", "https://api.nu7.valargroup.dev/snapshots/../private.tar.zst"),
                           ("url", snapshot["url"] + "?download=1"),
                           ("sha256", "bad"), ("sizeBytes", -1), ("sizeBytes", True),
                           ("sizeBytes", 2**53), ("publishedAt", 0), ("publishedAt", 2**53 - 1),
                           ("storageMode", "archive"), ("dbVersion", "v29"),
                           ("height", self.seed["height"] + 1)]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                publish(dict(snapshot, **{key: value}))
        for key in snapshot:
            incomplete = dict(snapshot)
            del incomplete[key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                publish(incomplete)


if __name__ == "__main__":
    unittest.main()
