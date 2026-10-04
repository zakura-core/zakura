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
                 'mac-verifier.yml',
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

    def test_unused_former_workflow_paths_are_rejected(self):
        for workflow in ('build-mac-verifier.yml', 'deploy-mac-verifier.yml'):
            run = dict(head_repository={'full_name': 'zakura-core/zakura'}, head_branch='main',
                       path='.github/workflows/' + workflow, status='completed', conclusion='success')
            with self.subTest(workflow=workflow), tempfile.TemporaryDirectory() as tmp, \
                    patch.object(deploy.subprocess, 'check_output', return_value=json.dumps(run)), \
                    patch.object(deploy.subprocess, 'run') as download:
                with self.assertRaises(ValueError):
                    deploy.download_candidate('123', Path(tmp))
                download.assert_not_called()

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
    def deployment_hosts(self, fail_receipt=False):
        old = {'bootstrap_height': 10, 'bootstrap_record': {'hash': 'a' * 64},
               'config_sha256': 'b' * 64, 'binary_sha256': 'c' * 64}
        class Host:
            def __init__(host, mac=False):
                host.mac, host.scripts, host.files, host.samples = mac, [], {}, 0
            def put(host, data, path):
                host.files[path] = data
                if fail_receipt and path == '/etc/zakura-mac-verifier/receipt.json':
                    raise RuntimeError('fixture receipt transfer failed')
            def run(host, script, **kwargs):
                host.scripts.append(script)
                subprocess.run(['bash', '-n'], input=script, text=True, check=True, capture_output=True)
                if script.startswith('mktemp'):
                    return '/var/tmp/zakura-verifier-ci.fixture'
                if script.startswith('sudo -n cat') and 'status.json' not in script:
                    return json.dumps(old)
                if script == 'sudo -n cat /var/lib/zakura-mac-verifier/status.json':
                    host.samples += 1
                    new = json.loads(host.files['/etc/zakura-mac-verifier/receipt.json'])
                    return json.dumps({'sample_time': deploy.time.time(), 'condition': 'matching',
                        'verifier': {'receipt': new, 'binary_sha256': new['binary_sha256'],
                                     'tip': {'height': 100 + host.samples}}})
                return ''
        return Host(mac=True), Host()

    def test_binary_deploy_accepts_condition_only_progress_and_rebinds_cursor(self):
        mac, linux = self.deployment_hosts()
        with patch.object(deploy.time, 'sleep'):
            deploy.deploy_candidate(mac, linux, self.path, self.receipt)
        self.assertEqual(linux.samples, 2)
        commands = '\n'.join(linux.scripts)
        self.assertEqual(commands.count("state['receipt_digest']"), 1)
        self.assertNotIn('state["cursor"]', commands)
        self.assertIn('systemctl stop zakura-fleet-watchdog', commands)
        self.assertIn('systemctl start zakura-fleet-watchdog', commands)
        self.assertTrue(mac.scripts[-1].startswith('rm -rf -- '))

    def test_binary_deploy_failure_restores_both_receipts_and_rebinds_cursor(self):
        mac, linux = self.deployment_hosts(fail_receipt=True)
        with self.assertRaises(RuntimeError):
            deploy.deploy_candidate(mac, linux, self.path, self.receipt)
        self.assertIn('receipt.json.previous', '\n'.join(mac.scripts))
        commands = '\n'.join(linux.scripts)
        self.assertIn('receipt.json.previous /etc/zakura-mac-verifier/receipt.json', commands)
        self.assertEqual(commands.count("state['receipt_digest']"), 1)
        self.assertTrue(mac.scripts[-1].startswith('rm -rf -- '))




class DashboardTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {'ZAKURA_MAC_CRANELIFT_HOST': '198.51.100.42'})
        environment.start()
        self.addCleanup(environment.stop)

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

    def test_only_mac_helpers_are_replaced_and_linux_is_read_only(self):
        linux, mac = self.Host(), self.Host()
        deploy.dashboard(mac, linux)
        self.assertEqual(linux.files, {})
        self.assertEqual(len(linux.scripts), 1)
        self.assertIn('with opener.open', linux.scripts[0])
        self.assertNotIn('systemctl', linux.scripts[0])
        for name in ('ssh_probe.py', 'rotate_logs.py'):
            self.assertIn('/var/tmp/zakura-tools-ci.fixture/' + name, mac.files)
        commands = '\n'.join(mac.scripts)
        self.assertIn('sudo -n mv', commands)
        self.assertNotIn('launchctl', commands)
        self.assertNotIn('cursor.json', commands)
        self.assertTrue(mac.scripts[-1].startswith('rm -rf -- '))

    def test_failed_mac_update_restores_helpers_without_linux_mutation(self):
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
        self.assertIn('sudo -n cp -p', '\n'.join(mac.scripts))
        self.assertEqual(linux.scripts, [])
        self.assertEqual(linux.files, {})

    def test_failed_transfer_restores_only_backed_up_files(self):
        mac, linux = self.Host(fail_install=True), self.Host()
        with self.assertRaises(RuntimeError):
            deploy.dashboard(mac, linux)
        commands = '\n'.join(mac.scripts)
        self.assertIn('elif sudo -n test -f', commands)
        self.assertIn('ssh_probe.py.previous', commands)
        self.assertEqual(linux.scripts, [])
        self.assertNotIn('cursor.json', commands)

    def test_failed_dashboard_health_rolls_back_mac_helpers(self):
        mac, linux = self.Host(), self.Host()
        with patch.object(linux, 'run', side_effect=RuntimeError('health check failed')), self.assertRaises(RuntimeError):
            deploy.dashboard(mac, linux)
        self.assertIn('sudo -n cp -p', '\n'.join(mac.scripts))
        self.assertTrue(mac.scripts[-1].startswith('rm -rf -- '))


class PublicAuditTests(unittest.TestCase):
    def test_private_address_leak_fails_without_echoing_address(self):
        for private, leaked in [('198.51.100.42', '198.51.100.42'),
                                ('198.51.100.42', '::ffff:c633:642a'),
                                ('2001:db8::42', '2001:0db8:0000:0000:0000:0000:0000:0042')]:
            with self.subTest(leaked=leaked), patch.dict(os.environ, {'ZAKURA_MAC_CRANELIFT_HOST': private}), \
                    patch('urllib.request.urlopen', return_value=io.BytesIO(json.dumps({'error':leaked}).encode())):
                with self.assertRaises(ValueError) as caught:
                    deploy.audit_public_privacy()
                self.assertNotIn(private, str(caught.exception))
                self.assertNotIn(leaked, str(caught.exception))

    def test_audit_visits_linux_detail_pages_and_detects_peer_detail_leaks(self):
        def response(url, **kwargs):
            if url.endswith('/data'):
                body = {'rows': [{'name': 'linux-reference'}, {'name': 'mac-os-cranelift'}]}
            elif url.endswith('/data/node/linux-reference'):
                body = {'peer_subversions': ['peer:::ffff:198.51.100.42']}
            else:
                body = {}
            return io.BytesIO(json.dumps(body).encode())
        with patch.dict(os.environ, {'ZAKURA_MAC_CRANELIFT_HOST': '198.51.100.42'}), \
                patch('urllib.request.urlopen', side_effect=response) as request:
            with self.assertRaises(ValueError) as raised:
                deploy.audit_public_privacy()
            self.assertNotIn('198.51.100.42', str(raised.exception))
            self.assertIn('https://status.mainnet.zakura.valargroup.dev/node/linux-reference',
                          [call.args[0] for call in request.call_args_list])

    def test_linux_addresses_are_allowed_by_private_audit(self):
        with patch.dict(os.environ, {'ZAKURA_MAC_CRANELIFT_HOST': '198.51.100.42'}), \
                patch('urllib.request.urlopen', side_effect=lambda *a, **k: io.BytesIO(b'{"linux":"192.0.2.17"}')):
            deploy.audit_public_privacy()


class RemoteProgramTests(unittest.TestCase):
    def test_remote_programs_are_checked_in_and_shell_arguments_are_quoted(self):
        arguments = {
            'mac_status.py': (deploy.BASE,), 'reference_status.py': ('verifier-' + 'a' * 32,),
            'check_dashboard.py': (123,), 'binary_digest.py': ("/tmp/a 'quoted' path", 'a' * 64),
            'rebind_cursor.py': (),
        }
        for name, args in arguments.items():
            with self.subTest(name=name):
                script = deploy.remote_program(name, *args)
                subprocess.run(['bash', '-n'], input=script, text=True, check=True, capture_output=True)
                program = script.split("<<'REMOTE'\n", 1)[1].split('\nREMOTE', 1)[0]
                compile(program, name, 'exec')
                if name != 'rebind_cursor.py':
                    self.assertNotIn('receipt_digest', program)

    def test_cursor_rebinding_preserves_coverage_history_permissions_and_owner(self):
        spec = importlib.util.spec_from_file_location('rebind_cursor', deploy.PACKAGE / 'remote/rebind_cursor.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cursor, receipt = root / 'cursor.json', root / 'receipt.json'
            state = {'cursor': 123, 'history': {'122': 'a' * 64}, 'coverage_gap': True,
                     'bootstrap': 10, 'reorg_search': {'next': 121}, 'receipt_digest': 'old'}
            cursor.write_text(json.dumps(state)); cursor.chmod(0o640)
            receipt.write_text('{"binary_sha256":"new"}')
            before = cursor.stat()
            module.rebind_cursor(cursor, receipt)
            result = json.loads(cursor.read_text())
            expected = module.hashlib.sha256(json.dumps(json.loads(receipt.read_text()), sort_keys=True).encode()).hexdigest()
            self.assertEqual(result.pop('receipt_digest'), expected)
            state.pop('receipt_digest')
            self.assertEqual(result, state)
            self.assertEqual(cursor.stat().st_mode & 0o777, 0o640)
            self.assertEqual((cursor.stat().st_uid, cursor.stat().st_gid), (before.st_uid, before.st_gid))


class HealthTests(unittest.TestCase):
    def setUp(self):
        self.mac = {name: True for name in ['receipt_present', 'binary_matches_receipt',
            'full_verification_enabled', 'node_running']}
        self.mac.update(architecture='arm64',
                        compiler_acceptance=[{'passed': True}])
        self.reference = {name: True for name in ['private_address_config_private', 'monitoring_config_private', 'monitoring_key_restricted',
            'dashboard_file_present',
            'dashboard_file_fresh', 'dashboard_file_healthy', 'dashboard_identity_matches',
            'dashboard_row_healthy', 'dashboard_mac_enabled', 'dashboard_supports_mac']}
        self.reference.update({'zakura-fleet-watchdog': 'active', 'zakura-mainnet-dashboard': 'active',
            'status': {'condition': 'matching', 'sample_time': deploy.time.time()}})

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
        for change in [{'condition': 'catching_up'},
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
    def test_installed_rotator_has_no_repo_import_dependency(self):
        import sys
        program = deploy.rotate_program().decode()
        result = subprocess.run([sys.executable, '-I', '-c', program], text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('from common import', program)



if __name__ == '__main__':
    unittest.main()
