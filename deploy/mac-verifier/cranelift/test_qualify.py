"""Regression checks for exact-case qualification acceptance."""
import unittest
from pathlib import Path
import subprocess
import tempfile
from qualify import qualifies, verify_backend_patch


class AcceptanceTests(unittest.TestCase):
    def test_exact_case_requires_one_executed_success(self):
        self.assertTrue(qualifies(0, 'test result: ok. 1 passed; 0 failed; 0 ignored;', True))
        for output in [
            'test result: ok. 0 passed; 0 failed; 0 ignored;',
            'test result: ok. 2 passed; 0 failed; 0 ignored;',
            'test result: ok. 1 passed; 0 failed; 1 ignored;',
            'test result: FAILED. 0 passed; 1 failed; 0 ignored;',
            '',
        ]:
            self.assertFalse(qualifies(0, output, True))

    def test_process_failure_cannot_be_accepted(self):
        self.assertFalse(qualifies(1, 'test result: ok. 1 passed; 0 failed; 0 ignored;', True))
        self.assertFalse(qualifies(1, '', False))

    def test_backend_rejects_extra_staged_and_unstaged_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            backend = root / 'backend'
            backend.mkdir()
            def git(*args):
                return subprocess.check_output(['git', '-C', str(backend), *args])
            git('init', '-q')
            for name in ['unwind.rs', 'other.rs']:
                (backend / name).write_text('original\n')
            git('add', 'unwind.rs', 'other.rs')
            git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                'commit', '-qm', 'fixture')
            (backend / 'unwind.rs').write_text('accepted\n')
            accepted = root / 'accepted.patch'
            accepted.write_bytes(git('diff', 'HEAD', '--'))
            verify_backend_patch(backend, accepted)
            for key, value in [('core.abbrev', '12'), ('diff.context', '5')]:
                git('config', key, value)
                self.assertNotEqual(git('diff', 'HEAD', '--'), accepted.read_bytes())
                verify_backend_patch(backend, accepted)
            git('add', 'unwind.rs')
            verify_backend_patch(backend, accepted)
            (backend / 'unwind.rs').chmod(0o755)
            with self.assertRaises(ValueError):
                verify_backend_patch(backend, accepted)
            (backend / 'unwind.rs').chmod(0o644)
            (backend / 'new.rs').write_text('unapproved new file\n')
            git('add', 'new.rs')
            with self.assertRaises(ValueError):
                verify_backend_patch(backend, accepted)
            git('reset', '-q', 'HEAD', '--', 'new.rs')
            (backend / 'new.rs').unlink()
            (backend / 'other.rs').write_text('unapproved\n')
            with self.assertRaises(ValueError):
                verify_backend_patch(backend, accepted)
            git('add', 'other.rs')
            with self.assertRaises(ValueError):
                verify_backend_patch(backend, accepted)


if __name__ == '__main__':
    unittest.main()
