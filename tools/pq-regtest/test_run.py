"""Reject ambiguous or substituted dependency sources before building the demo."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("pq_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class SourceTests(unittest.TestCase):
    def test_manifest_pin_is_the_authoritative_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.toml").write_text(
                '[workspace.dependencies]\nengine = {package = "zcash_script", git = "https://example.test/script", rev = "abc"}\n'
            )
            self.assertEqual(runner.expected_sources(root), {
                "zcash_script": "git+https://example.test/script?rev=abc#abc"
            })

    def test_missing_duplicate_registry_and_wrong_revision_sources_are_rejected(self):
        expected = {"zcash_script": "git+https://example.test/script?rev=abc#abc"}
        correct = {"name": "zcash_script", "source": expected["zcash_script"]}
        runner.verify_sources([correct], expected)
        for packages in (
            [], [correct, correct],
            [{"name": "zcash_script", "source": None}],
            [{"name": "zcash_script", "source": "registry+https://github.com/rust-lang/crates.io-index"}],
            [{"name": "zcash_script", "source": correct["source"].replace("abc", "def")}],
        ):
            with self.subTest(packages=packages), self.assertRaises(ValueError):
                runner.verify_sources(packages, expected)


@unittest.skipUnless(os.name == "posix", "process-group cleanup uses Unix signals")
class ScenarioTests(unittest.TestCase):
    def test_timeout_stops_the_harness_child_too(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "child.pid"
            program = """
import signal
import subprocess
import sys
import time
from pathlib import Path

child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
def stop(signum, frame):
    child.wait(timeout=3)
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
Path(sys.argv[1]).write_text(str(child.pid))
time.sleep(60)
"""
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.run_scenario([sys.executable, "-c", program, str(pid_file)], timeout=2)
            child_pid = int(pid_file.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)


if __name__ == "__main__":
    unittest.main()
