"""Local regressions for paired archive deployments. No network or node access."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import unittest
from unittest import mock


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("release_state_deploy", HERE / "deploy.py")
deploy = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = deploy
SPEC.loader.exec_module(deploy)
SHA = "a" * 40


class PublisherTests(unittest.TestCase):
    def load(self, overrides=""):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.toml"
            path.write_text('''[[nodes]]
name = "archive"
ssh_string = "root@example.invalid"
commit = "main"
manage_config = false
''' + overrides)
            return deploy.load_nodes(path, None)

    def publisher(self):
        node = self.load("release_state_publisher = true\n")[0]
        node.sha = SHA
        return node

    def test_publisher_requires_explicit_boolean_opt_in(self):
        self.assertFalse(self.load()[0].release_state_publisher)
        self.assertTrue(self.publisher().release_state_publisher)
        for value in ('"true"', '1'):
            with self.subTest(value=value), self.assertRaises(deploy.DeployError):
                self.load(f"release_state_publisher = {value}\n")

    def test_incompatible_publisher_modes_rejected(self):
        for setting in ('deploy_kind = "docker"', 'network = "Testnet"',
                        'storage_mode = "pruned"'):
            with self.subTest(setting=setting), self.assertRaises(deploy.DeployError):
                self.load("release_state_publisher = true\n" + setting)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.toml"
            path.write_text('''[[nodes]]
name = "archive"
ssh_string = "root@example.invalid"
commit = "main"
release_state_publisher = true
manage_config = true
''')
            with self.assertRaises(deploy.DeployError):
                deploy.load_nodes(path, None)

    def test_no_restart_rejected_before_any_build(self):
        args = argparse.Namespace(config="unused", node=None, force=False, no_restart=True)
        with mock.patch.object(deploy, "load_nodes", return_value=[self.publisher()]), \
                mock.patch.object(deploy, "build_nodes") as nodes, \
                mock.patch.object(deploy, "build_publishers") as exporters:
            with self.assertRaisesRegex(deploy.DeployError, "no-restart"):
                deploy.cmd_deploy(args)
            nodes.assert_not_called()
            exporters.assert_not_called()

    def test_exporter_build_uses_resolved_node_sha_and_deduplicates(self):
        ordinary = self.load()[0]
        ordinary.sha = "b" * 40
        with mock.patch.object(deploy, "repo_root", return_value=Path("/repo")), \
                mock.patch.object(deploy, "run", return_value=mock.Mock(returncode=0)), \
                mock.patch.object(deploy, "build_commit", return_value=Path("/exporter")) as build:
            result = deploy.build_publishers([self.publisher(), ordinary, self.publisher()], force=True)
            build.assert_called_once_with(Path("/repo"), SHA, force=True, exporter=True)
            self.assertEqual(result, {SHA: Path("/exporter")})

    def test_unmerged_exporter_commit_is_rejected_before_build(self):
        with mock.patch.object(deploy, "repo_root", return_value=Path("/repo")), \
                mock.patch.object(deploy, "run", return_value=mock.Mock(returncode=1)), \
                mock.patch.object(deploy, "build_commit") as build:
            with self.assertRaisesRegex(deploy.DeployError, "ancestor"):
                deploy.build_publishers([self.publisher()])
            build.assert_not_called()

    def test_public_pointer_must_match_completed_publication(self):
        for height in (3500000, 3490000):
            calls = []

            def fake_run(command, **kwargs):
                calls.append(command)
                if command[0] == "ssh":
                    if "mktemp" in command[-1]:
                        return mock.Mock(stdout="/tmp/zakura-release-deploy.abc123\n")
                    if command[-1].startswith("cat "):
                        return mock.Mock(stdout="3500000\n")
                if "--metadata-out" in command:
                    Path(command[command.index("--metadata-out") + 1]).write_text(json.dumps({"height": height}))
                return mock.Mock(returncode=0, stdout="")

            with self.subTest(height=height), \
                    mock.patch.object(deploy, "run", side_effect=fake_run), \
                    mock.patch.object(deploy.subprocess, "run", return_value=mock.Mock(returncode=0)):
                if height == 3500000:
                    deploy.deploy_publisher(self.publisher(), Path("/node"), Path("/exporter"))
                else:
                    with self.assertRaisesRegex(deploy.DeployError, "pointer"):
                        deploy.deploy_publisher(self.publisher(), Path("/node"), Path("/exporter"))
                self.assertEqual(calls[-1][-1], "rm -rf -- /tmp/zakura-release-deploy.abc123")

    def test_node_cache_cannot_satisfy_exporter_build(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp)
            node = cache / f"zakurad-{SHA}"
            node.write_text("node")
            calls = []

            def fake_run(command, *, cwd=None, **kwargs):
                calls.append(command)
                if command[:2] == ["cargo", "build"]:
                    built = Path(cwd) / "target/release/zakura-checkpoints"
                    built.parent.mkdir(parents=True)
                    built.write_text("exporter")
                return mock.Mock(returncode=0, stdout="")

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: str(cache)}, clear=True), \
                    mock.patch.object(deploy, "run", side_effect=fake_run), \
                    mock.patch.object(deploy, "binary_is_runnable", return_value=True):
                exporter = deploy.build_commit(cache, SHA, exporter=True)
            self.assertEqual(exporter.name, f"zakura-checkpoints-{SHA}")
            self.assertEqual(exporter.read_text(), "exporter")
            self.assertEqual(node.read_text(), "node")
            self.assertIn(["cargo", "build", "--release", "--locked", "-p", "zakura-utils",
                           "--features", "zakura-checkpoints-offline", "--bin", "zakura-checkpoints"], calls)
            self.assertTrue(any(command[:3] == ["git", "worktree", "add"] and command[-1] == SHA
                                for command in calls))

    def test_publisher_failure_does_not_enter_generic_install_or_rollback(self):
        args = argparse.Namespace(config="unused", node=None, force=False, no_restart=False)
        for failure in (None, deploy.DeployError("publication failed")):
            with self.subTest(failure=failure), \
                    mock.patch.object(deploy, "load_nodes", return_value=[self.publisher()]), \
                    mock.patch.object(deploy, "build_nodes", return_value={SHA: Path("/node")}), \
                    mock.patch.object(deploy, "build_publishers", return_value={SHA: Path("/exporter")}), \
                    mock.patch.object(deploy, "deploy_publisher", side_effect=failure) as paired, \
                    mock.patch.object(deploy, "ssh_with_stdin") as generic, \
                    mock.patch.object(deploy, "run") as run:
                self.assertEqual(deploy.cmd_deploy(args), int(failure is not None))
                paired.assert_called_once()
                generic.assert_not_called()
                run.assert_not_called()


class PairedShellTests(unittest.TestCase):
    """Execute the deployment script with isolated paths and fake host commands."""

    def run_pair(self, failure="", *, resume_marker=False, timer_active=True, reboot_after_failure=False, historical_restarts=0):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            script = (HERE.parent / "release-state/deploy-archive-pair.sh").read_text()
            # Host paths and long health windows are replaced only in this local copy.
            for prefix in ("/opt/", "/etc/", "/run/", "/usr/local/"):
                script = script.replace(prefix, f"{root}{prefix}")
            script = script.replace("SECONDS + 90", "SECONDS + 0")
            for directory in ("opt/zakura-release-state/bin", "etc", "run", "usr/local/bin", "stage", "mocks"):
                (root / directory).mkdir(parents=True)
            (root / "etc/zakura-release-state.env").touch()
            marker = root / "opt/zakura-release-state/deploy.resume-timer"
            if resume_marker:
                marker.touch()
                (root / "opt/zakura-release-state/deploy.paused").touch()
            (root / "opt/zakura-release-state/profile.env").write_text(
                "RELEASE_STATE_EXPECTED_HOST=roman-zakura-archive-vct-off\n"
                "RELEASE_STATE_NODE_UNIT=zakurad.service\n")
            for name in ("stage/zakurad", "stage/zakura-checkpoints", "opt/zakura-release-state/bin/publish-from-archive-host.sh"):
                path = root / name
                path.write_text(f"#!/bin/sh\necho zakurad+g{SHA[:12]}\n")
                path.chmod(0o755)
            wrapper = root / "mocks/command"
            wrapper.write_text(f'''#!{sys.executable}
import os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["TEST_LOG"], "a") as log:
    log.write(name + " " + " ".join(args) + "\\n")
failure = os.environ["TEST_FAILURE"]
if name == "hostname":
    print("roman-zakura-archive-vct-off")
elif name == "flock":
    if failure == "lock" and args[-1] == "9" and "-u" not in args:
        sys.exit(1)
elif name == "systemctl":
    if args == ["start", "zakura-release-state.service"]:
        guard = pathlib.Path(os.environ["TEST_ROOT"]) / "etc/systemd/system/zakura-release-state.service.d/deployment-pause.conf"
        if guard.exists():
            blocked = guard.read_text().split("ConditionPathExists=!", 1)[1].strip()
            if pathlib.Path(blocked).exists():
                print("publisher skipped")
                sys.exit(0)
    if args == ["is-active", "--quiet", "zakura-release-state.timer"] and os.environ["TEST_TIMER_ACTIVE"] == "false":
        sys.exit(3)
    counter = pathlib.Path(os.environ["TEST_ROOT"]) / "restart-count"
    if args == ["reset-failed", "zakurad"]:
        counter.write_text("0")
    if args == ["start", "zakurad"] and failure == "restart":
        counter.write_text("1")
    if args == ["show", "-p", "NRestarts", "--value", "zakurad"]:
        print(counter.read_text() if counter.exists() else os.environ["TEST_HISTORICAL_RESTARTS"])
        sys.exit(0)
    if args == ["start", "zakurad"] and failure == "node":
        sys.exit(1)
    if args == ["start", "zakura-release-state.service"] and failure == "publish":
        sys.exit(1)
    if args[0] == "show":
        print({{"ActiveState": "inactive", "NRestarts": "0", "Result": "success", "MainPID": "123"}}[args[2]])
elif name == "journalctl":
    if "--show-cursor" in args:
        print("-- cursor: test-cursor")
    elif failure != "skip":
        print("pointer now at height 3500000")
elif name == "readlink":
    print(os.environ["TEST_BIN_PATH"])
elif name == "curl":
    print('{{"result": {{"blocks": 3500000}}, "error": null}}')
''')
            wrapper.chmod(0o755)
            for command in ("hostname", "flock", "systemctl", "curl", "journalctl", "readlink"):
                (root / "mocks" / command).symlink_to(wrapper)
            log = root / "events"
            result = subprocess.run(["bash", "-s", "--", str(root / "stage"),
                                     str(root / "usr/local/bin/zakurad"), "zakurad", SHA],
                                    input=script, text=True, capture_output=True, timeout=10,
                                    env={**os.environ, "PATH": f"{root}/mocks:{os.environ['PATH']}",
                                         "TEST_ROOT": str(root), "TEST_LOG": str(log), "TEST_FAILURE": failure,
                                         "TEST_BIN_PATH": str(root / "usr/local/bin/zakurad"),
                                         "TEST_TIMER_ACTIVE": str(timer_active).lower(),
                                         "TEST_HISTORICAL_RESTARTS": str(historical_restarts)})
            if reboot_after_failure:
                self.assertNotEqual(result.returncode, 0)
                shutil.rmtree(root / "run")
                (root / "run").mkdir()
                reboot = subprocess.run([str(root / "mocks/systemctl"), "start", "zakura-release-state.service"],
                                        env={**os.environ, "TEST_ROOT": str(root), "TEST_LOG": str(log),
                                             "TEST_FAILURE": ""}, capture_output=True)
                self.assertEqual(reboot.returncode, 0)
                self.assertEqual(reboot.stdout.strip(), b"publisher skipped",
                                 "reboot must not bypass the publisher guard")
            events = log.read_text().splitlines()
            installed = (root / "opt/zakura-release-state/EXPORTER_REVISION").exists()
            return result, events, installed, marker.exists()

    def test_success_waits_for_publisher_then_installs_and_publishes(self):
        result, events, installed, marker = self.run_pair()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(installed)
        order = ["systemctl stop zakura-release-state.timer", "flock -w 600 9",
                 "systemctl stop zakurad", "systemctl start zakurad", "flock -u 9",
                 "systemctl start zakura-release-state.service", "systemctl start zakura-release-state.timer"]
        self.assertEqual(sorted(events.index(event) for event in order), [events.index(event) for event in order])

    def test_busy_publisher_preserves_node_and_restores_timer(self):
        result, events, installed, marker = self.run_pair("lock")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(installed)
        self.assertNotIn("systemctl stop zakurad", events)
        self.assertIn("systemctl start zakura-release-state.timer", events)

    def test_failed_node_restart_keeps_publisher_disabled(self):
        result, events, installed, marker = self.run_pair("node")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(installed)
        self.assertNotIn("systemctl start zakura-release-state.timer", events)
        self.assertNotIn("systemctl start zakura-release-state.service", events)
        self.assertTrue(marker)

    def test_historical_restarts_are_cleared_before_new_start(self):
        result, events, installed, marker = self.run_pair(historical_restarts=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertLess(events.index("systemctl stop zakurad"), events.index("systemctl reset-failed zakurad"))
        self.assertLess(events.index("systemctl reset-failed zakurad"), events.index("systemctl start zakurad"))

    def test_restart_before_readiness_poll_rejects_new_deployment(self):
        result, events, installed, marker = self.run_pair("restart", historical_restarts=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(installed)
        self.assertTrue(marker)
        self.assertNotIn("systemctl start zakura-release-state.service", events)
        self.assertNotIn("systemctl start zakura-release-state.timer", events)

    def test_failed_deployment_pause_survives_reboot(self):
        for timer_active in (True, False):
            with self.subTest(timer_active=timer_active):
                self.run_pair("node", timer_active=timer_active, reboot_after_failure=True)

    def test_successful_retry_restores_previously_active_timer(self):
        result, events, installed, marker = self.run_pair(resume_marker=True, timer_active=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(installed)
        self.assertIn("systemctl start zakura-release-state.timer", events)
        self.assertFalse(marker)

    def test_failed_retry_keeps_previously_suspended_timer_stopped(self):
        result, events, installed, marker = self.run_pair(
            "lock", resume_marker=True, timer_active=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(installed)
        self.assertTrue(marker)
        self.assertNotIn("systemctl start zakura-release-state.timer", events)

    def test_intentionally_inactive_timer_stays_inactive(self):
        result, events, installed, marker = self.run_pair(timer_active=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(installed)
        self.assertNotIn("systemctl start zakura-release-state.timer", events)
        self.assertFalse(marker)

    def test_skipped_publication_is_not_reported_as_success(self):
        result, events, installed, marker = self.run_pair("skip")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(installed)
        self.assertIn("systemctl start zakura-release-state.timer", events)

    def test_publication_failure_preserves_pair_and_restores_retry_timer(self):
        result, events, installed, marker = self.run_pair("publish")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(installed)
        self.assertEqual(events.count("systemctl start zakurad"), 1)
        self.assertIn("systemctl start zakura-release-state.timer", events)


if __name__ == "__main__":
    unittest.main()
