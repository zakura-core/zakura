import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("build_inputs", Path(__file__).parents[1] / "build_inputs.py")
build_inputs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_inputs)


class BuildInputsTests(unittest.TestCase):
    def test_monitoring_changes_skip_native_acceptance(self):
        self.assertFalse(build_inputs.needs_native_build([
            "deploy/runner/zakura-cluster-watchdog.py",
            "deploy/runner/test_zakura_cluster_status.py",
            "deploy/zakura-mac-cranelift/comparison.py",
            "deploy/deployer/zakura-mac-cranelift-manager.py"]))

    def test_native_inputs_require_acceptance(self):
        for path in ("Cargo.lock", "crates/zakurad/src/main.rs",
                     "deploy/zakura-mac-cranelift/corpus.json",
                     "deploy/zakura-mac-cranelift/cranelift/macos-unwind.patch",
                     ".github/workflows/build-zakura-mac-cranelift.yml"):
            with self.subTest(path=path):
                self.assertTrue(build_inputs.needs_native_build([path]))

    def test_event_revisions_keep_native_acceptance_for_full_pr(self):
        event = {"pull_request": {"base": {"sha": "base"}, "head": {"sha": "head"}}}
        self.assertEqual(build_inputs.event_revisions("pull_request", event), ("base", "head"))
        event["before"] = "previous-head"
        self.assertEqual(build_inputs.event_revisions("pull_request", event), ("base", "head"))
        self.assertEqual(build_inputs.event_revisions("push", {"before": "old", "after": "new"}),
                         ("old", "new"))
        self.assertIsNone(build_inputs.event_revisions("workflow_dispatch", {}))

    def test_cli_uses_real_git_diff_and_fails_closed_without_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.DEVNULL).decode().strip()
            git("init", "-q")
            git("config", "user.email", "test@example.invalid")
            git("config", "user.name", "Test")
            (root / "Cargo.lock").write_text("initial")
            git("add", "Cargo.lock")
            git("commit", "-qm", "initial")
            base = git("rev-parse", "HEAD")
            (root / "monitor.py").write_text("monitoring")
            git("add", "monitor.py")
            git("commit", "-qm", "monitoring")
            monitor = git("rev-parse", "HEAD")
            (root / "Cargo.lock").write_text("changed")
            git("add", "Cargo.lock")
            git("commit", "-qm", "compiler input")
            native = git("rev-parse", "HEAD")
            for before, after, expected in ((base, monitor, "false"), (monitor, native, "true"),
                                            ("f" * 40, monitor, "true"), ("0" * 40, monitor, "true")):
                event = root / "event.json"
                output = root / "output"
                output.write_text("")
                event.write_text(json.dumps({"before": before, "after": after}))
                subprocess.run([sys.executable, str(Path(build_inputs.__file__))], cwd=root,
                               env={**os.environ, "GITHUB_EVENT_PATH": str(event),
                                    "GITHUB_EVENT_NAME": "push", "GITHUB_OUTPUT": str(output)},
                               check=True, capture_output=True, timeout=10)
                self.assertEqual(output.read_text().strip(), f"native={expected}")
