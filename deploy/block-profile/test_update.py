"""Exercise main provenance and update conflicts with disposable Git repositories."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('profile_update', Path(__file__).with_name('update.py'))
update = importlib.util.module_from_spec(spec)
spec.loader.exec_module(update)
host_spec = importlib.util.spec_from_file_location("profile_update_host", Path(__file__).with_name("update_host.py"))
host = importlib.util.module_from_spec(host_spec)
host_spec.loader.exec_module(host)


class UpdateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tools = tempfile.TemporaryDirectory()
        source = ROOT / 'crates/zakurad/build/metadata.rs'
        harness = Path(cls.tools.name) / 'metadata.rs'
        harness.write_text('\n'.join(line for line in source.read_text().splitlines() if not line.startswith('//!')) + '\nfn main() { emit_git_metadata().unwrap(); }\n')
        cls.metadata = Path(cls.tools.name) / 'metadata'
        subprocess.run(['rustc', '--edition=2021', '-A', 'dead_code', str(harness), '-o', str(cls.metadata)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.tools.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.prior = Path.cwd()
        self.root = Path(self.temp.name)
        self.remote = self.root / 'remote.git'
        subprocess.run(['git', 'init', '--bare', '--initial-branch=main', str(self.remote)], check=True, capture_output=True)
        subprocess.run(['git', 'clone', str(self.remote), str(self.root / 'work')], check=True, capture_output=True)
        os.chdir(self.root / 'work')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.invalid')
        self.write('node', 'base')
        self.base = self.commit('main base')
        self.git('push', 'origin', 'main')
        self.git('checkout', '-b', 'adam/profile')
        self.write('profiling', 'instrumented')
        self.commit('profiling')
        self.git('push', '-u', 'origin', 'adam/profile')

    def tearDown(self):
        os.chdir(self.prior)
        self.temp.cleanup()

    def git(self, *args):
        return subprocess.run(['git', *args], check=True, text=True, capture_output=True).stdout.strip()

    def write(self, name, value):
        Path(name).write_text(value)

    def commit(self, message):
        self.git('add', '.')
        self.git('commit', '-m', message)
        return self.git('rev-parse', 'HEAD')

    def advance_main(self, conflict=False):
        self.git('checkout', 'main')
        self.write('node', 'new main')
        if conflict:
            self.write('profiling', 'conflicting main file')
        head = self.commit('advance main')
        self.git('push', 'origin', 'main')
        self.git('checkout', 'adam/profile')
        return head

    def emitted(self):
        output = subprocess.run([str(self.metadata)], check=True, text=True, capture_output=True).stdout
        return dict(line.removeprefix('cargo:rustc-env=').split('=', 1) for line in output.splitlines() if line.startswith('cargo:rustc-env='))

    def test_main_base_changes_only_when_main_is_included(self):
        current = self.git('rev-parse', 'HEAD')
        newest = self.advance_main()
        self.assertEqual(self.emitted()['ZAKURA_PROFILE_BASE_COMMIT'], self.base)
        revision, base = update.prepare('adam/profile', 'origin/main')
        self.assertEqual(base, newest)
        self.assertEqual(self.emitted()['ZAKURA_PROFILE_BASE_COMMIT'], newest)
        self.assertEqual(self.emitted()['VERGEN_GIT_SHA'], revision)
        self.git('merge-base', '--is-ancestor', current, revision)
        self.assertEqual(self.git('rev-parse', 'origin/adam/profile'), revision)

    def test_keep_base_and_dirty_checkout(self):
        self.advance_main()
        _, base = update.prepare('adam/profile', 'origin/main', keep_base=True)
        self.assertEqual(base, self.base)
        self.write('node', 'uncommitted')
        with self.assertRaisesRegex(RuntimeError, 'local changes'):
            update.prepare('adam/profile', 'origin/main')

    def test_conflict_aborts_without_changing_pushed_profile(self):
        before = self.git('rev-parse', 'HEAD')
        self.advance_main(conflict=True)
        with self.assertRaisesRegex(RuntimeError, 'conflicts'):
            update.prepare('adam/profile', 'origin/main')
        self.assertEqual(self.git('rev-parse', 'HEAD'), before)
        self.assertEqual(self.git('status', '--porcelain'), '')
        self.assertEqual(self.git('rev-parse', 'origin/adam/profile'), before)

    def test_failed_remote_tests_never_stop_services(self):
        calls = []
        revision = self.git('rev-parse', 'HEAD')
        args = SimpleNamespace(hostname='fixture', repo=str(Path.cwd()), base=self.base,
                               revision=revision, branch='adam/profile', target=str(self.root / 'target'))
        def remote_command(argv, **kwargs):
            calls.append(argv)
            if argv[:2] == ['cargo', 'test']:
                raise subprocess.CalledProcessError(1, argv)
            output = ''
            if '-p' in argv and 'MainPID' in argv:
                output = '0'
            if '-p' in argv and 'ExecStart' in argv:
                binary = 'zakurad' if argv[2] == 'zakurad' else 'zakura-profile-explorer'
                output = f'{{ path=/usr/local/bin/{binary} ; }}'
            return subprocess.CompletedProcess(argv, 0, stdout=output)
        def remote_git(*argv):
            if argv[:2] == ('merge-base', '--all'):
                return self.base
            return ''
        with patch.object(host.os, 'geteuid', return_value=0), \
             patch.object(host.socket, 'gethostname', return_value='fixture'), \
             patch.object(host, 'open', lambda *a, **kw: open(self.root / 'lock', 'w'), create=True), \
             patch.object(host, 'git', side_effect=remote_git), \
             patch.object(host, 'home', return_value={'runs': []}), \
             patch.object(host, 'command', side_effect=remote_command), \
             patch.dict(os.environ):
            with self.assertRaises(subprocess.CalledProcessError):
                host.deploy(args, lambda stage: None)
        self.assertFalse(any(argv[:2] == ['systemctl', 'stop'] for argv in calls))
        self.assertFalse(any(argv[:2] == ['cargo', 'build'] for argv in calls))

    def test_unknown_main_is_not_reported_as_profile_commit(self):
        self.git('update-ref', '-d', 'refs/remotes/origin/main')
        self.assertNotIn('ZAKURA_PROFILE_BASE_COMMIT', self.emitted())


if __name__ == '__main__':
    unittest.main()
