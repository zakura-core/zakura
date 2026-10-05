#!/usr/bin/env python3
"""Tests for zakura-monitoring-deploy.py with every host operation replaced."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "zakura_monitoring_deploy", HERE / "zakura-monitoring-deploy.py"
)
assert SPEC is not None and SPEC.loader is not None
tool = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = tool
SPEC.loader.exec_module(tool)

SHA = "c" * 40
WEBHOOK = "https://hooks.slack.invalid/services/T/B/s3cr3t"


class Recorder:
    """Replaces Context host operations and records what would have run."""

    def __init__(self, ctx, state=None, compat_current=SHA, fleet_current=SHA):
        self.calls = []
        self.state = state or {}
        self.compat_current = compat_current
        self.fleet_current = fleet_current
        ctx.local = self.local
        ctx.remote_install = self.remote_install
        ctx.systemctl = self.systemctl
        ctx.ssh = self.ssh
        ctx.remote_json = lambda argv, timeout: (0, {"agree": True})

    def local(self, *arguments, stdin=None, timeout=300):
        self.calls.append(("local", arguments, stdin))
        if arguments[0] == "status":
            return {"current": self.fleet_current}
        return {"operation": arguments[0]}

    def remote_install(self, *arguments, timeout=300):
        self.calls.append(("remote", arguments, None))
        if arguments[0] == "status":
            return {"current": self.compat_current}
        return {"operation": arguments[0]}

    def systemctl(self, *arguments, check=True):
        self.calls.append(("systemctl", arguments, None))
        return "active"

    def ssh(self, remote, stdin=None, timeout=300):
        self.calls.append(("ssh", (remote,), None))
        return subprocess.CompletedProcess([], 0, b"{}", b"")

    def operations(self):
        return [(kind, arguments[0]) for kind, arguments, _ in self.calls]


class DeployToolCase(unittest.TestCase):
    def context(self, stage, **overrides):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        values = {"stage": stage, "sha": SHA, "target": tool.COMPAT_TARGET,
                  "known_hosts": None, "deploy_config": None, "work": self.tmp.name,
                  "evidence": None, "run_url": "", "soak_seconds": 60, "restore_state": None}
        values.update(overrides)
        return tool.Context(tool.argparse.Namespace(**values))


class TargetTests(DeployToolCase):
    def test_target_must_be_the_known_compat_host(self):
        tool.validate_target(self.context("status"))
        with self.assertRaises(tool.StageError):
            tool.validate_target(self.context("status", target="root@10.0.0.1"))

    def test_deploy_config_must_agree(self):
        config = Path(tempfile.mkdtemp()) / "nodes.toml"
        config.write_text('[[nodes]]\nname = "zakura-compat"\nssh_string = "root@10.0.0.9"\n')
        with self.assertRaises(tool.StageError):
            tool.validate_target(self.context("status", deploy_config=str(config)))
        config.write_text(f'[[nodes]]\nname = "zakura-compat"\nssh_string = "{tool.COMPAT_TARGET}"\n')
        tool.validate_target(self.context("status", deploy_config=str(config)))

    def test_short_sha_is_rejected(self):
        with self.assertRaises(SystemExit):
            tool.main(["status", "--sha", "abc"])


class WorkflowHostKeyTests(DeployToolCase):
    def test_monitoring_uses_only_the_configured_trust_anchor(self):
        workflow = (HERE.parent.parent / ".github/workflows/zakura-mainnet-deploy.yml").read_text()
        step = workflow.split("      - name: Load the independently verified compatibility host key\n", 1)[1]
        step = step.split("      - name:", 1)[0]
        self.assertIn("secrets.ZAKURA_COMPAT_SSH_KNOWN_HOSTS", step)
        self.assertNotIn("vars.ZAKURA_COMPAT_SSH_KNOWN_HOSTS", step)
        self.assertNotIn("ssh-keyscan", step)
        script = step.split("        run: |\n", 1)[1]
        script = "\n".join(line.removeprefix("          ") for line in script.splitlines())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = root / "test_key"
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)],
                           check=True, capture_output=True, timeout=10)
            public = key.with_suffix(".pub").read_text().strip()
            for value, passed in (("", False), ("159.203.113.196 invalid invalid", False),
                                  (f"10.0.0.1 {public}", False),
                                  (f"159.203.113.196 {public}", True)):
                with self.subTest(value=value):
                    result = subprocess.run(["bash", "-c", script], capture_output=True,
                                            timeout=10, env={**os.environ, "RUNNER_TEMP": directory,
                                                            "COMPAT_KNOWN_HOSTS": value})
                    self.assertEqual(result.returncode == 0, passed, result.stderr)
                    if passed:
                        self.assertEqual((root / "monitoring_known_hosts").read_text(), value + "\n")
        ctx = self.context("status", known_hosts="/trusted/known_hosts")
        with mock.patch.object(tool.subprocess, "run", return_value=
                               subprocess.CompletedProcess([], 0, b"", b"")) as run:
            ctx.ssh("true")
        self.assertIn("StrictHostKeyChecking=yes", run.call_args.args[0])
        self.assertIn("UserKnownHostsFile=/trusted/known_hosts", run.call_args.args[0])


class FleetDeployTests(DeployToolCase):
    def run_fleet_deploy(self, lane_enabled):
        ctx = self.context("fleet-deploy")
        recorder = Recorder(ctx)
        with mock.patch.object(tool, "lane_enabled", return_value=lane_enabled), \
                mock.patch.dict(tool.os.environ, {"SLACK_WEB_HOOK": WEBHOOK}), \
                mock.patch.object(tool, "install_compat") as compat:
            tool.stage_fleet_deploy(ctx)
        return recorder, compat

    def test_regular_deploy_installs_in_place_and_restarts_only_the_watchdog(self):
        recorder, compat = self.run_fleet_deploy(lane_enabled=False)
        self.assertEqual(
            recorder.operations(),
            [("local", "stage"), ("local", "activate"), ("local", "ensure-unit"),
             ("local", "fleet-env"), ("systemctl", "daemon-reload"),
             ("systemctl", "enable"), ("systemctl", "restart"), ("systemctl", "is-active")],
        )
        restarts = [args for kind, args, _ in recorder.calls if kind == "systemctl"
                    and args[0] in ("restart", "enable")]
        self.assertTrue(all(args[1] == tool.FLEET_UNIT for args in restarts))
        env_call = next(call for call in recorder.calls if call[1][0] == "fleet-env")
        self.assertEqual(env_call[2], WEBHOOK.encode())
        self.assertNotIn("s3cr3t", json.dumps([list(map(str, c[1])) for c in recorder.calls]))
        compat.assert_not_called()

    def test_enabled_lane_refreshes_the_checker(self):
        _recorder, compat = self.run_fleet_deploy(lane_enabled=True)
        compat.assert_called_once()


class GuardTests(DeployToolCase):
    def test_finalize_requires_an_enabled_lane_and_fresh_pass(self):
        ctx = self.context("finalize")
        Recorder(ctx)
        with mock.patch.object(tool, "lane_enabled", return_value=False):
            with self.assertRaises(tool.StageError):
                tool.stage_finalize(ctx)
        stale = {"valid": True, "status": "pass", "completed_at": time.time() - 3600}
        with mock.patch.object(tool, "lane_enabled", return_value=True), \
                mock.patch.object(tool, "last_probe", return_value=stale):
            with self.assertRaises(tool.StageError):
                tool.stage_finalize(ctx)
        fresh = {**stale, "completed_at": time.time()}
        recorder = Recorder(ctx)
        with mock.patch.object(tool, "lane_enabled", return_value=True), \
                mock.patch.object(tool, "last_probe", return_value=fresh):
            tool.stage_finalize(ctx)
        self.assertEqual(recorder.operations(), [("local", "status"),
                                               ("remote", "status"),
                                               ("remote", "retire-rust")])

    def test_finalize_rejects_either_wrong_active_release(self):
        for fleet, compat in (("d" * 40, SHA), (SHA, "d" * 40), (None, SHA)):
            with self.subTest(fleet=fleet, compat=compat):
                ctx = self.context("finalize")
                recorder = Recorder(ctx, fleet_current=fleet, compat_current=compat)
                with mock.patch.object(tool, "lane_enabled", return_value=True), \
                        mock.patch.object(tool, "last_probe", return_value={
                            "valid": True, "status": "pass", "completed_at": time.time()}):
                    with self.assertRaises(tool.StageError):
                        tool.stage_finalize(ctx)
                self.assertNotIn(("remote", "retire-rust"), recorder.operations())

    def test_cutover_requires_the_installed_commit(self):
        ctx = self.context("cutover")
        Recorder(ctx, compat_current="d" * 40)
        with self.assertRaises(tool.StageError):
            tool.stage_cutover(ctx)

    def test_rollback_attempts_every_step_and_never_touches_nodes(self):
        ctx = self.context("rollback")
        recorder = Recorder(ctx)
        original = recorder.local

        def failing_local(*arguments, **kwargs):
            if arguments[0] == "disable-lane":
                raise tool.StageError("boom")
            return original(*arguments, **kwargs)

        ctx.local = failing_local
        with self.assertRaises(tool.StageError):
            tool.stage_rollback(ctx)
        self.assertIn(("remote", "restore-rust"), recorder.operations())
        self.assertIn(("remote", "rollback"), recorder.operations())
        self.assertIn(("systemctl", "restart"), recorder.operations())

    def test_rollback_retry_keeps_completed_release_changes(self):
        from zakura_monitoring import install
        ctx = self.context("rollback")
        recorder = Recorder(ctx)
        roots = [Path(self.tmp.name) / name for name in ("fleet", "compat")]
        for root in roots:
            for sha in (SHA, "d" * 40):
                release = root / "releases" / sha
                release.mkdir(parents=True)
            (root / "current").symlink_to("releases/" + SHA)
            (root / "rollback.json").write_text(json.dumps({"active": SHA, "previous": "d" * 40}))
        attempts = 0
        def local(*arguments, **kwargs):
            if arguments[0] == "rollback":
                return install.rollback(roots[0], {})
            return recorder.local(*arguments, **kwargs)
        def remote(*arguments, **kwargs):
            nonlocal attempts
            if arguments[0] == "restore-rust":
                attempts += 1
                if attempts == 1:
                    raise tool.StageError("Rust service restart failed")
            if arguments[0] == "rollback":
                return install.rollback(roots[1], {})
            return recorder.remote_install(*arguments, **kwargs)
        ctx.local, ctx.remote_install = local, remote
        with mock.patch.object(install, "verify_release"):
            with self.assertRaisesRegex(tool.StageError, "Rust service restart failed"):
                tool.stage_rollback(ctx)
            tool.stage_rollback(ctx)
        self.assertEqual([install.current_release(root) for root in roots], ["d" * 40] * 2)


class SourceTests(unittest.TestCase):
    def test_tooling_never_controls_node_services(self):
        for name in ("zakura-monitoring-deploy.py", "zakura_monitoring/install.py",
                     "zakura-monitoring-acceptance.py"):
            source = (HERE / name).read_text()
            for unit in re.findall(r"systemctl[^\n]*", source):
                self.assertNotRegex(unit, r"zakurad|zcashd")
            self.assertNotRegex(source, r"(restart|stop|start)[\"', ]+zakurad")


if __name__ == "__main__":
    unittest.main()
