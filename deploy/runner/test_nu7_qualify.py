"""Transport and activation-readiness claims for the read-only qualifier."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


class QualifyTests(unittest.TestCase):
    def receipt(self, adapter, build='v7.0.0-rc.0'):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            binary = path / 'grpcurl'
            binary.write_bytes(b'pinned binary')
            reference = {'name': 'reference', 'rpcUrl': 'http://reference'}
            if adapter == 'grpcurl':
                reference.update(adapter=adapter, grpcurlPath=str(binary))
            config = {'armed': False, 'dispatch': False, 'public': {
                'reference': reference, 'nodes': [{'name': 'node'}],
                'manifest': {'nodeRevision': 'a' * 40}}}
            config_file = path / 'config.json'
            config_file.write_text(json.dumps(config))

            class Feed:
                def observe(self, source, now, parameters=True):
                    return {'name': source['name'], 'height': 100, 'fresh': True, 'active': False}

                def source_hash(self, source, height):
                    return 'b' * 64

            module = types.ModuleType('nu7_activation')
            module.ActivationFeed = Feed
            module.rpc = lambda *args: {'build': build, 'protocolversion': 170180}
            source = Path(__file__).with_name('nu7-qualify.py').read_text()
            source = source.replace("Path('/etc/zakura/nu7-activation.json')", f"Path({str(config_file)!r})")
            output = io.StringIO()
            with patch.dict(sys.modules, {'nu7_activation': module}), patch.object(sys, 'argv', ['qualify']), contextlib.redirect_stdout(output):
                with self.assertRaises(SystemExit) as result:
                    exec(compile(source, 'qualify', 'exec'), {})
                self.assertEqual(result.exception.code, 0)
            return json.loads(output.getvalue())

    def test_grpc_agreement_does_not_claim_nu7_schedule(self):
        receipt = self.receipt('grpcurl')
        self.assertTrue(receipt['preparedChainAgreement'])
        self.assertFalse(receipt['activationReferencePrepared'])
        self.assertIn('grpcurlSha256', receipt)

    def test_rpc_reference_has_no_grpc_path_and_verifies_schedule(self):
        receipt = self.receipt('json-rpc')
        self.assertTrue(receipt['activationReferencePrepared'])
        self.assertEqual(receipt['referenceBuildVersion'], 'v7.0.0-rc.0')
        self.assertNotIn('grpcurlSha256', receipt)

    def test_old_reference_version_cannot_claim_activation_readiness(self):
        receipt = self.receipt('json-rpc', 'v6.3.0')
        self.assertTrue(receipt['preparedChainAgreement'])
        self.assertFalse(receipt['activationReferencePrepared'])


if __name__ == '__main__':
    unittest.main()
