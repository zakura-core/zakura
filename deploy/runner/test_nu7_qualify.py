"""Read-only qualification distinguishes preparation from activation."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("nu7_qualify", Path(__file__).with_name("nu7-qualify.py"))
qualifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualifier)


class SourceObservations:
    def observe(self, source, now, include_build=True, profile="public-testnet"):
        return {"name": source["name"], "height": 100, "fresh": True, "active": False,
                "buildVersion": "v1.8.1+10.g" + "a" * 12}

    def source_hash(self, source, height):
        return "b" * 64


class QualifyTests(unittest.TestCase):
    def setUp(self):
        self.settings = {"armed": False,
            "reference": {"name": "reference", "rpcUrl": "http://reference"},
            "nodes": [{"name": "node"}], "manifest": {"nodeRevision": "a" * 40}}
        self.config = {"armed": False, "dispatch": False, "public": copy.deepcopy(self.settings),
                       "mainnet": copy.deepcopy(self.settings)}
        self.config["mainnet"]["reference"].update(expectedBuild="v6.20.0-44f29cd", minimumProtocolVersion=170190)

    def receipt(self, profile="public-testnet", build="v7.0.0-rc.0", protocol=170190):
        with patch.object(qualifier, "ActivationFeed", SourceObservations), patch.object(
                qualifier, "rpc", return_value={"build": build, "protocolversion": protocol}):
            return qualifier.qualify(self.config, profile)

    def test_rpc_reference_verifies_testnet_schedule(self):
        receipt = self.receipt()
        self.assertTrue(receipt["activationReferencePrepared"])
        self.assertEqual(receipt["referenceBuildVersion"], "v7.0.0-rc.0")
        self.assertNotIn("grpcurlSha256", receipt)

    def test_grpc_agreement_does_not_claim_nu7_schedule(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "grpcurl"
            binary.write_bytes(b"pinned binary")
            self.config["public"]["reference"].update(adapter="grpcurl", grpcurlPath=str(binary))
            receipt = self.receipt()
        self.assertTrue(receipt["preparedChainAgreement"])
        self.assertFalse(receipt["activationReferencePrepared"])
        self.assertIn("grpcurlSha256", receipt)

    def test_old_testnet_reference_cannot_claim_readiness(self):
        receipt = self.receipt(build="v6.3.0")
        self.assertTrue(receipt["preparedChainAgreement"])
        self.assertFalse(receipt["activationReferencePrepared"])

    def test_mainnet_is_prepared_before_activation_without_arming(self):
        receipt = self.receipt("mainnet", "v6.20.0-44f29cd")
        self.assertTrue(receipt["prepared"])
        self.assertFalse(receipt["referenceNu7Active"])
        self.assertFalse(receipt["armed"])
        self.assertEqual(receipt["profile"], "mainnet")

    def test_mainnet_rejects_wrong_reference_build_protocol_and_node_revision(self):
        self.assertFalse(self.receipt("mainnet", "v6.3.0")["prepared"])
        self.assertFalse(self.receipt("mainnet", "v6.20.0-44f29cd", 170180)["prepared"])
        self.config["mainnet"]["manifest"]["nodeRevision"] = "c" * 40
        self.assertFalse(self.receipt("mainnet", "v6.20.0-44f29cd")["prepared"])

    def test_qualification_does_not_change_input_configuration(self):
        before = json.dumps(self.config, sort_keys=True)
        self.receipt("mainnet", "v6.20.0-44f29cd")
        self.assertEqual(json.dumps(self.config, sort_keys=True), before)


if __name__ == "__main__":
    unittest.main()
