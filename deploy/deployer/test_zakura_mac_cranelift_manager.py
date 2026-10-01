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



class DashboardTests(unittest.TestCase):
    class Host:
        def __init__(self, fail_install=False):
            self.scripts, self.files = [], {}
            self.fail_install = fail_install
        def put(self, data, path):
            self.files[path] = data
            if self.fail_install:
                raise RuntimeError("fixture install failure")
        def run(self, script, **kwargs):
            self.scripts.append(script)
            subprocess.run(["bash", "-n"], input=script, text=True, check=True, capture_output=True)
            if "<<'REMOTE'" in script:
                program = script.split("<<'REMOTE'\n", 1)[1].split("\nREMOTE", 1)[0]
                compile(program, "remote-check", "exec")
            if script.startswith('mktemp'):
                return '/var/tmp/zakura-tools-ci.fixture' if 'zakura-tools-ci' in script else '/var/tmp/zakura-dashboard-ci.fixture'
            if script.startswith('systemctl is-active'):
                return 'active'
            if "with opener.open" in script:
                return 'true'
            return ''

    def test_bridge_retired_only_after_file_and_dashboard_validation(self):
        host = self.Host()
        mac = self.Host()
        deploy.dashboard(mac, host)
        commands = "\n".join(host.scripts)
        self.assertLess(commands.index("with opener.open"), commands.index("disable --now"))
        self.assertIn('/opt/zakura-fleet-watchdog/mac_cranelift_status.py', host.files)
        self.assertNotIn('receipt.json', ''.join(host.files))
        self.assertNotIn('cursor.json', commands)
        self.assertNotIn('launchctl', commands)
        self.assertIn('/opt/zakura-mac-verifier/common.py', host.files)
        self.assertIn('/var/tmp/zakura-tools-ci.fixture/ssh_probe.py', mac.files)
        self.assertIn('/var/tmp/zakura-tools-ci.fixture/rotate_logs.py', mac.files)
        self.assertIn('sudo -n mv', '\n'.join(mac.scripts))

    def test_failed_mac_update_rolls_back_both_hosts(self):
        mac, linux = self.Host(), self.Host()
        original = mac.run
        failed = False
        def run(script, **kwargs):
            nonlocal failed
            result = original(script, **kwargs)
            if not failed and 'sudo -n mv' in script:
                failed = True
                raise RuntimeError('fixture Mac replacement failure')
            return result
        with patch.object(mac, 'run', side_effect=run), self.assertRaises(RuntimeError):
            deploy.dashboard(mac, linux)
        self.assertIn('else sudo -n cp -p', '\n'.join(mac.scripts))
        self.assertIn('common.py', '\n'.join(linux.scripts))
        self.assertIn('systemctl start zakura-fleet-watchdog', linux.scripts[-1])
        self.assertNotIn('cursor.json', '\n'.join(linux.scripts))

    def test_failed_install_restores_services_without_rewinding_state(self):
        host = self.Host(fail_install=True)
        mac = self.Host()
        with self.assertRaises(RuntimeError):
            deploy.dashboard(mac, host)
        commands = "\n".join(host.scripts)
        self.assertIn('.previous', commands)
        self.assertIn('enable --now zakura-mac-verifier-dashboard', commands)
        self.assertIn('systemctl start zakura-fleet-watchdog', commands)
        self.assertNotIn('cursor.json', commands)
        self.assertIn('ssh_probe.py.previous', '\n'.join(mac.scripts))


class HealthTests(unittest.TestCase):
    def setUp(self):
        self.mac = {name: True for name in ['receipt_present', 'binary_matches_receipt',
            'full_verification_enabled', 'node_running', 'adapter_listener_closed']}
        self.mac.update(architecture='arm64', adapter_running=False, tunnel_running=False,
                        compiler_acceptance=[{'passed': True}])
        self.reference = {name: True for name in ['monitoring_config_private', 'monitoring_key_restricted',
            'reverse_listener_closed', 'dashboard_bridge_closed', 'dashboard_file_present',
            'dashboard_file_fresh', 'dashboard_file_healthy', 'dashboard_identity_matches',
            'dashboard_row_healthy', 'dashboard_mac_enabled', 'dashboard_supports_mac']}
        self.reference.update({'zakura-fleet-watchdog': 'active', 'zakura-mainnet-dashboard': 'active',
            'zakura-mac-verifier': 'inactive', 'zakura-mac-verifier-dashboard': 'inactive',
            'status': {'comparison_healthy': True, 'condition': 'matching', 'sample_time': deploy.time.time()}})

    def test_only_healthy_fresh_checks_pass(self):
        deploy.require_healthy(self.mac, self.reference)
        for target, fixture in [('mac', self.mac), ('reference', self.reference)]:
            for key in fixture:
                with self.subTest(target=target, key=key):
                    broken = dict(fixture)
                    broken.pop(key)
                    with self.assertRaises(RuntimeError):
                        deploy.require_healthy(broken if target == 'mac' else self.mac,
                                               broken if target == 'reference' else self.reference)
        for change in [{'comparison_healthy': False}, {'condition': 'catching_up'},
                       {'sample_time': 1}, {'sample_time': float('nan')},
                       {'sample_time': deploy.time.time() + 1000}]:
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                deploy.require_healthy(self.mac, {**self.reference,
                    'status': {**self.reference['status'], **change}})

    def test_status_command_does_not_return_success_for_unhealthy_hosts(self):
        class Host:
            def __init__(self, data): self.data = data
            def run(self, *args, **kwargs): return json.dumps(self.data)
        mac = Host({'compiler_metadata': None, 'node_running': False})
        linux = Host({'status': None, 'zakura-fleet-watchdog': 'inactive'})
        with patch.object(deploy, 'public_report') as report, self.assertRaises(RuntimeError):
            deploy.status(mac, linux, 'verifier-' + 'a' * 32)
        report.assert_called_once()

    def test_installed_probe_is_standalone_and_read_only(self):
        import sys
        program = deploy.probe_program().decode()
        result = subprocess.run([sys.executable, '-I', '-c', program],
            input='{"operation":"exec","command":"id"}\n', text=True,
            capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'error': 'sample unavailable'})


if __name__ == '__main__':
    unittest.main()
