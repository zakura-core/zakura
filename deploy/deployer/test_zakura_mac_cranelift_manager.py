"""Artifact acceptance and receipt-transition regressions for Mac deployment."""
import copy
import contextlib
import io
import json
import os
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import importlib.util

spec = importlib.util.spec_from_file_location(
    "zakura_mac_cranelift_manager", Path(__file__).with_name("zakura-mac-cranelift-manager.py"))
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)
from common import digest


class CandidateTests(unittest.TestCase):
    def test_private_mac_address_is_never_printed(self):
        for address, leak in [('198.51.100.42', '198.51.100.42'),
                              ('2001:db8::42', '2001:0db8:0000:0000:0000:0000:0000:0042')]:
            output = io.StringIO()
            with self.subTest(address=address), patch.dict(os.environ, {'ZAKURA_MAC_CRANELIFT_HOST': address}), \
                    contextlib.redirect_stdout(output):
                with self.assertRaises(ValueError):
                    deploy.public_report({'compiler': 'unexpected address: ' + leak})
                self.assertEqual(output.getvalue(), '')
                deploy.public_report({'architecture': 'arm64'})
                self.assertNotIn(address, output.getvalue())

    def test_renamed_and_historical_candidate_runs_remain_usable(self):
        paths = ['zakura-mac-cranelift.yml', 'build-zakura-mac-cranelift.yml',
                 'mac-verifier.yml', 'build-mac-verifier.yml', 'deploy-mac-verifier.yml',
                 'zakura-mainnet-deploy.yml']
        for workflow in paths:
            for prefix in ['zakura-mac-cranelift-candidate-', 'mac-verifier-cranelift-']:
                run = dict(head_repository={'full_name': 'zakura-core/zakura'}, head_branch='main',
                           path='.github/workflows/' + workflow, status='completed', conclusion='success')
                artifacts = {'artifacts': [dict(name=prefix + 'a' * 40, expired=False),
                    dict(name='zakura-mac-cranelift-diagnostics-' + 'a' * 40, expired=False)]}
                with self.subTest(workflow=workflow, prefix=prefix), tempfile.TemporaryDirectory() as tmp, \
                        patch.object(deploy.subprocess, 'check_output',
                                     side_effect=[json.dumps(run), json.dumps(artifacts)]), \
                        patch.object(deploy.subprocess, 'run') as download:
                    deploy.download_candidate('123', Path(tmp))
                    self.assertIn(prefix + 'a' * 40, download.call_args.args[0])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        (self.path / 'zakurad').write_bytes(b'candidate binary')
        self.source = 'f' * 40
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
        return deploy.validate_candidate(self.path, self.source, 'a' * 64)

    def test_complete_candidate_accepts_only_the_recorded_binary(self):
        self.assertEqual(self.validate(), self.receipt)
        (self.path / 'zakurad').write_bytes(b'another binary')
        with self.assertRaises(ValueError):
            self.validate()

    def test_partial_or_different_acceptance_is_rejected(self):
        changes = [dict(passed=False), dict(source_sha='b' * 40),
                   dict(cargo_lock_sha256='b' * 64), dict(patch_sha256='c' * 64), dict(binary_architecture='x86_64'),
                   dict(checks=self.receipt['checks'][:-1]),
                   dict(configuration={**self.receipt['configuration'], 'panic': 'abort'})]
        failed = copy.deepcopy(self.receipt['checks'])
        failed[-1]['passed'] = False
        changes.append(dict(checks=failed))
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.validate({**self.receipt, **change})

    def test_upgrade_preserves_bootstrap_and_configuration(self):
        old = dict(source_sha='b' * 40, cargo_lock_sha256='c' * 64,
                   bootstrap_height=100, bootstrap_record={'hash': 'original anchor'},
                   config_sha256='d' * 64, binary_sha256='e' * 64, deployed_at=1,
                   snapshot={'original': 'snapshot'}, compiler='previous compiler')
        new = deploy.transitioned_receipt(old, self.receipt, 2)
        for key in ['bootstrap_height', 'bootstrap_record', 'config_sha256', 'snapshot']:
            self.assertEqual(new[key], old[key])
        self.assertEqual(new['binary_sha256'], self.receipt['binary_sha256'])
        self.assertEqual(old['binary_sha256'], 'e' * 64)
        for key in ['source_sha', 'cargo_lock_sha256']:
            self.assertEqual(new[key], self.receipt[key])
            self.assertNotEqual(new[key], old[key])



class MigrationTests(unittest.TestCase):
    class Host:
        def __init__(self, fail_install=False):
            self.scripts = []
            self.files = {}
            self.height = 100
            self.fail_install = fail_install
        def put(self, data, path):
            self.files[path] = data
        def run(self, script, **kwargs):
            self.scripts.append(script)
            subprocess.run(['bash', '-n'], input=script, text=True, check=True, capture_output=True)
            if script.startswith('mktemp'):
                return '/var/tmp/zakura-ssh-ci.fixture'
            if script.endswith('/ssh/id_ed25519.pub'):
                return 'ssh-ed25519 AAAA fixture'
            if script == 'sudo -n cat /var/lib/zakura-mac-verifier/status.json':
                self.height += 1
                return json.dumps(dict(condition='matching', verifier={'node_active': True}, compared_through=self.height))
            if self.fail_install and 'install -m 755' in script:
                raise RuntimeError('fixture install failed')
            return ''

    def setUp(self):
        self.env = patch.dict(os.environ, {'ZAKURA_MAC_CRANELIFT_HOST': '198.51.100.42',
                             'ZAKURA_MAC_CRANELIFT_USER': 'fixture', 'ZAKURA_MAC_CRANELIFT_SSH_PORT': '22',
                             'ZAKURA_MAC_CRANELIFT_KNOWN_HOSTS': 'fixture-host-key'})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_migration_shadow_checks_before_cutover_and_disables_old_owner(self):
        mac, linux = self.Host(), self.Host()
        with patch.object(deploy.time, 'sleep'):
            deploy.migrate_ssh(mac, linux)
        joined = '\n'.join(linux.scripts)
        self.assertLess(joined.index('/shadow/cursor.json'), joined.index('systemctl stop'))
        self.assertIn('bootout system/dev.valargroup.zakura-verifier-adapter', '\n'.join(mac.scripts))
        self.assertIn('restrict,command=', '\n'.join(mac.scripts))
        self.assertNotIn('SSH_KEY', ''.join(linux.files))
        self.assertNotIn('198.51.100.42', '\n'.join(linux.scripts + mac.scripts))
        self.assertIn(b'198.51.100.42', linux.files['/etc/zakura-mac-verifier/ssh/config'])
        for script in mac.scripts + linux.scripts:
            if "<<'REMOTE'\n" in script:
                code = script.split("<<'REMOTE'\n", 1)[1].split('\nREMOTE', 1)[0]
                compile(code, '<remote>', 'exec')

    def test_failed_cutover_restores_transport_without_rewinding_coverage(self):
        mac, linux = self.Host(), self.Host(fail_install=True)
        with self.assertRaises(RuntimeError):
            deploy.migrate_ssh(mac, linux)
        rollback = linux.scripts[-1]
        self.assertIn('/backup/comparison.py /opt/zakura-mac-verifier/comparison.py', rollback)
        self.assertNotIn('cursor.json', rollback)
        self.assertIn('launchctl bootstrap', mac.scripts[-1])

    def test_assembled_probe_runs_in_isolated_python_without_repo_imports(self):
        import sys
        result = subprocess.run([sys.executable, '-I', '-c', deploy.probe_program().decode()],
                                input='', text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '')


if __name__ == '__main__':
    unittest.main()
