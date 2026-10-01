"""Artifact acceptance and receipt-transition regressions for Mac deployment."""
import copy
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import mac_verifier as deploy
from common import digest


class CandidateTests(unittest.TestCase):
    def test_private_mac_address_is_never_printed(self):
        for address, leak in [('198.51.100.42', '198.51.100.42'),
                              ('2001:db8::42', '2001:0db8:0000:0000:0000:0000:0000:0042')]:
            output = io.StringIO()
            with self.subTest(address=address), patch.dict(os.environ, {'MAC_VERIFIER_HOST': address}), \
                    contextlib.redirect_stdout(output):
                with self.assertRaises(ValueError):
                    deploy.public_report({'compiler': 'unexpected address: ' + leak})
                self.assertEqual(output.getvalue(), '')
                deploy.public_report({'architecture': 'arm64'})
                self.assertNotIn(address, output.getvalue())

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        (self.path / 'zakurad').write_bytes(b'candidate binary')
        self.source = json.loads((deploy.PACKAGE / 'corpus.json').read_text())['source_sha']
        names = ['unwind-probe-build', 'unwind-probe', 'double-panic', 'native-node-build']
        names += ['zakura-consensus-' + str(i) for i in range(8)]
        names += ['zakura-network-' + str(i) for i in range(2)]
        self.receipt = dict(source_sha=self.source, passed=True,
            binary_sha256=digest(self.path / 'zakurad'), binary_architecture='Mach-O 64-bit executable arm64',
            patch_sha256=digest(deploy.PACKAGE / 'cranelift/macos-unwind.patch'),
            configuration=dict(panic='unwind', lto=False, build_jobs=1,
                linker='apple-classic', standard_library='cranelift-static'),
            checks=[dict(name=name, passed=True) for name in names],
            toolchain='pinned nightly', cargo_lock_sha256='a' * 64)

    def validate(self, receipt=None):
        (self.path / 'receipt.json').write_text(json.dumps(receipt or self.receipt))
        return deploy.validate_candidate(self.path, self.source)

    def test_complete_candidate_accepts_only_the_recorded_binary(self):
        self.assertEqual(self.validate(), self.receipt)
        (self.path / 'zakurad').write_bytes(b'another binary')
        with self.assertRaises(ValueError):
            self.validate()

    def test_partial_or_different_acceptance_is_rejected(self):
        changes = [dict(passed=False), dict(source_sha='b' * 40),
                   dict(patch_sha256='c' * 64), dict(binary_architecture='x86_64'),
                   dict(checks=self.receipt['checks'][:-1]),
                   dict(configuration={**self.receipt['configuration'], 'panic': 'abort'})]
        failed = copy.deepcopy(self.receipt['checks'])
        failed[-1]['passed'] = False
        changes.append(dict(checks=failed))
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.validate({**self.receipt, **change})

    def test_upgrade_preserves_bootstrap_and_configuration(self):
        old = dict(source_sha=self.source, cargo_lock_sha256='a' * 64,
                   bootstrap_height=100, bootstrap_record={'hash': 'original anchor'},
                   config_sha256='d' * 64, binary_sha256='e' * 64, deployed_at=1,
                   snapshot={'original': 'snapshot'}, compiler='previous compiler')
        new = deploy.transitioned_receipt(old, self.receipt, 2)
        for key in ['bootstrap_height', 'bootstrap_record', 'config_sha256', 'snapshot']:
            self.assertEqual(new[key], old[key])
        self.assertEqual(new['binary_sha256'], self.receipt['binary_sha256'])
        self.assertEqual(old['binary_sha256'], 'e' * 64)
        for key in ['source_sha', 'cargo_lock_sha256']:
            with self.subTest(key=key), self.assertRaises(ValueError):
                deploy.transitioned_receipt({**old, key: 'different'}, self.receipt, 2)


if __name__ == '__main__':
    unittest.main()
