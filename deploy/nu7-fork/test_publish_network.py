"""The public join manifest must match live consensus and omit operator data."""

import hashlib
import tomllib
import unittest
from pathlib import Path

import publish_network

FORK_NODE_FIXTURE = (Path(__file__).resolve().parents[2] / "crates" / "zakura-network" / "src"
                     / "config" / "tests" / "data" / "nu7-fork-node.toml")


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.config = tomllib.loads(FORK_NODE_FIXTURE.read_text())
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

    def test_the_published_v3_participant_config_is_reproduced_byte_for_byte(self):
        # Published at https://api.nu7.valargroup.dev/v1/network for Nu7StagingV3.
        heights = {"BeforeOverwinter": 1, "Overwinter": 207500, "Sapling": 280000,
                   "Blossom": 584000, "Heartwood": 903800, "Canopy": 1028500,
                   "NU5": 1842420, "NU6": 2976000, "NU6.1": 3536500, "NU6.2": 4052000,
                   "NU6.3": 4134000, "NU7": 4420652}
        config = {"network": {"network": {
            "network_name": "Nu7StagingV3", "network_magic": [122, 107, 117, 57],
            "checkpoints": True, "initial_nsm_value_balance": 55768414957,
            "activation_heights": heights}}}
        info = {"chain": "test", "upgrades": {
            "77190ad9": {"name": "NU7", "activationheight": 4420652}}}
        peers = ["seed.nu7.valargroup.dev:18233", "134.199.239.83:18233",
                 "157.245.69.251:18233", "165.22.255.181:18233"]
        result = publish_network.manifest(config, info, "61efe76c62645e22ca7d29a8cacfbfe77e35059a",
                                          peers, {"height": 4420648, "hash": "a" * 64, "time": 1})
        self.assertEqual(result["configSha256"],
                         "12c94fe866bf4de38e187aba6526b5623559db82b55de1cc6e3960d503835ce1")
        self.assertIn('[network.network.activation_heights]\n', result["config"])

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
