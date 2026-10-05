#!/usr/bin/env python3
"""Tests for zakura-monitoring-deploy.py with every host operation replaced."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shlex
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
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

    def test_checker_refresh_precedes_fleet_activation(self):
        ctx = self.context("fleet-deploy")
        recorder = Recorder(ctx)
        def refreshed(_ctx):
            recorder.calls.append(("remote", ("refresh-checker",), None))
        with mock.patch.object(tool, "lane_enabled", return_value=True), \
                mock.patch.object(tool, "install_compat", side_effect=refreshed):
            tool.stage_fleet_deploy(ctx)
        operations = recorder.operations()
        self.assertLess(operations.index(("remote", "refresh-checker")),
                        operations.index(("local", "activate")))

    def test_failed_checker_refresh_leaves_fleet_and_service_unchanged(self):
        for error in (tool.StageError("install failed"), OSError("upload failed"),
                      subprocess.TimeoutExpired("ssh", 30)):
            with self.subTest(error=type(error).__name__):
                ctx = self.context("fleet-deploy")
                recorder = Recorder(ctx)
                with mock.patch.object(tool, "lane_enabled", return_value=True), \
                        mock.patch.object(tool, "install_compat", side_effect=error):
                    with self.assertRaises(type(error)):
                        tool.stage_fleet_deploy(ctx)
                self.assertEqual(recorder.operations(), [("local", "stage")])


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
                                               ("systemctl", "is-active"),
                                               ("local", "acceptance"),
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

    def test_finalize_cannot_retire_without_acceptance_proof(self):
        ctx = self.context("finalize")
        recorder = Recorder(ctx)
        def local(*arguments, **kwargs):
            if arguments[0] == "acceptance":
                raise tool.StageError("soak has not passed")
            return recorder.local(*arguments, **kwargs)
        ctx.local = local
        with mock.patch.object(tool, "lane_enabled", return_value=True), \
                mock.patch.object(tool, "last_probe", return_value={
                    "valid": True, "status": "pass", "completed_at": time.time()}):
            with self.assertRaisesRegex(tool.StageError, "soak has not passed"):
                tool.stage_finalize(ctx)
        self.assertNotIn(("remote", "retire-rust"), recorder.operations())

    def test_soak_rejects_short_runs_and_does_not_record_failed_runs(self):
        ctx = self.context("soak", soak_seconds=1799)
        Recorder(ctx)
        with self.assertRaises(tool.StageError), mock.patch.object(tool.subprocess, "run") as run:
            tool.stage_soak(ctx)
        run.assert_not_called()
        ctx = self.context("soak", soak_seconds=1800)
        recorder = Recorder(ctx)
        original = recorder.local
        def local(*args, **kwargs):
            if args[0] == "acceptance":
                return {"generation": "run-1"}
            return original(*args, **kwargs)
        ctx.local = local
        for code in (1, 0):
            with mock.patch.object(tool.subprocess, "run", return_value=
                                   subprocess.CompletedProcess([], code, b'{"passed": true}', b"")):
                if code:
                    with self.assertRaises(tool.StageError):
                        tool.stage_soak(ctx)
                else:
                    tool.stage_soak(ctx)
        accepted = [entry for entry in ctx.evidence["steps"] if entry["step"] == "accepted soak"]
        self.assertEqual(len(accepted), 1)

    def test_finalize_rejects_inactive_service_despite_fresh_pass(self):
        for service in ("inactive", "failed", "activating", ""):
            with self.subTest(service=service):
                ctx = self.context("finalize")
                recorder = Recorder(ctx)
                ctx.systemctl = lambda *args, **kwargs: service
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


class UploadTests(DeployToolCase):
    def test_upload_ignores_predictable_symlinks_and_uses_private_unique_paths(self):
        ctx = self.context("install")
        recorder = Recorder(ctx)
        root = Path(self.tmp.name)
        victim = root / "victim"
        victim.write_bytes(b"operator file")
        old = root / "zakura-monitoring"
        old.mkdir()
        (old / (SHA + ".tar.gz")).symlink_to(victim)
        paths = []
        def ssh(command, stdin=None, timeout=300):
            result = subprocess.run(["bash", "-c", command], input=stdin, capture_output=True,
                                    timeout=timeout, check=False)
            if command.startswith("python3") and result.returncode == 0:
                tarball = Path(json.loads(result.stdout)["tarball"])
                paths.append(tarball)
                self.assertEqual(tarball.read_bytes(), stdin)
                self.assertEqual(stat.S_IMODE(tarball.stat().st_mode), 0o600)
                self.assertEqual(stat.S_IMODE(tarball.parent.stat().st_mode), 0o700)
            return result
        ctx.ssh = ssh
        with mock.patch.object(tool, "REMOTE_STAGING", str(old)):
            for _ in range(2):
                tool.install_compat(ctx)
        self.assertEqual(victim.read_bytes(), b"operator file")
        self.assertNotEqual(paths[0], paths[1])
        self.assertTrue(all(not path.parent.exists() for path in paths))
        self.assertIn(("remote", "stage"), recorder.operations())

    def test_failed_stage_cleans_up_uploaded_directory(self):
        ctx = self.context("install")
        Recorder(ctx)
        root = Path(self.tmp.name)
        ctx.ssh = lambda command, stdin=None, timeout=300: subprocess.run(
            shlex.split(command), input=stdin, capture_output=True, timeout=timeout, check=False)
        ctx.remote_install = mock.Mock(side_effect=tool.StageError("stage rejected"))
        with mock.patch.object(tool, "REMOTE_STAGING", str(root)):
            with self.assertRaisesRegex(tool.StageError, "stage rejected"):
                tool.install_compat(ctx)
        self.assertFalse(any(path.is_dir() for path in root.glob("zakura-monitoring-*")))

    def test_oversized_upload_fails_and_removes_its_private_directory(self):
        ctx = self.context("install")
        root = Path(self.tmp.name)
        result = subprocess.run([sys.executable, "-c", tool.install.UPLOAD_SCRIPT, str(root), "4"],
                                input=b"12345", capture_output=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(root.glob("zakura-monitoring-*")))

    def test_bad_upload_result_never_runs_remote_installer(self):
        for output, code in ((b'{"tarball":"/etc/passwd"}', 0), (b'{}', 0), (b'{}', 1)):
            ctx = self.context("install")
            recorder = Recorder(ctx)
            ctx.ssh = lambda *args, **kwargs: subprocess.CompletedProcess([], code, output, b"")
            with self.assertRaises(tool.StageError):
                tool.install_compat(ctx)
            self.assertFalse(any(kind == "remote" for kind, *_ in recorder.calls))


class SourceTests(unittest.TestCase):
    def test_monitoring_dispatch_cannot_run_any_mac_job(self):
        workflow = (HERE.parent.parent / ".github/workflows/zakura-mainnet-deploy.yml").read_text()
        expressions = {}
        for job in ("mac-source", "mac-build", "zakura-mac-cranelift"):
            block = re.split(r"\n  [a-z][a-z-]+:\n", workflow.split(f"  {job}:\n", 1)[1], maxsplit=1)[0]
            expression = re.search(r"^    if: (.+)$", block, re.M).group(1)
            expression = expression.removeprefix("${{ ").removesuffix(" }}")
            expression = expression.replace("&&", " and ").replace("||", " or ")
            expression = expression.replace("!cancelled()", "not cancelled()")
            expression = expression.replace("needs.mac-source", "needs.mac_source")
            expression = expression.replace("needs.mac-build", "needs.mac_build")
            expressions[job] = expression
        for operation in ("monitoring", "deploy"):
            for mac_operation in ("deploy", "status", "dashboard"):
                values = {"inputs": SimpleNamespace(operation=operation, node="zakura-mac-os",
                          mac_operation=mac_operation, mac_candidate_run_id=""),
                          "github": SimpleNamespace(ref="refs/heads/main", ref_name="main"),
                          "vars": SimpleNamespace(MAC_VERIFIER_DEPLOY_BRANCH="allowed"),
                          "needs": SimpleNamespace(mac_source=SimpleNamespace(result="success"),
                                                   mac_build=SimpleNamespace(result="success")),
                          "cancelled": lambda: False}
                for job, expression in expressions.items():
                    with self.subTest(operation=operation, mac_operation=mac_operation, job=job):
                        allowed = eval(expression, {"__builtins__": {}}, values)
                        if operation == "monitoring":
                            self.assertFalse(allowed)
                        elif mac_operation == "deploy":
                            self.assertTrue(allowed)
                        elif job == "zakura-mac-cranelift":
                            self.assertTrue(allowed)

    def test_tooling_never_controls_node_services(self):
        for name in ("zakura-monitoring-deploy.py", "zakura_monitoring/install.py",
                     "zakura-monitoring-acceptance.py"):
            source = (HERE / name).read_text()
            for unit in re.findall(r"systemctl[^\n]*", source):
                self.assertNotRegex(unit, r"zakurad|zcashd")
            self.assertNotRegex(source, r"(restart|stop|start)[\"', ]+zakurad")


if __name__ == "__main__":
    unittest.main()
