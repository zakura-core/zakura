#!/usr/bin/env python3
"""Tests for versioned monitoring installs (zakura_monitoring.install).

They run from the repository and from an installed release, using temporary
roots only; systemctl is replaced, so no service is touched.
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from zakura_monitoring import install  # noqa: E402

SHA_A = "a" * 40
SHA_B = "b" * 40
WEBHOOK = "https://hooks.slack.invalid/services/T000/B000/s3cr3t"
LINKS = {"zakura-cluster-watchdog.py": "zakura-cluster-watchdog.py",
         "fleets.toml": "fleet-watchdog.toml"}


class InstallCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.root = self.dir / "root"
        self.packages = {}

    def package(self, sha):
        if sha not in self.packages:
            self.packages[sha] = install.build_package(HERE, sha, self.dir / "out" / sha)
        return self.packages[sha]

    def stage(self, sha):
        package = self.package(sha)
        return install.stage(self.root, sha, Path(package["tarball"]), package["digest"])


class PackageTests(InstallCase):
    def test_package_is_reproducible_and_complete(self):
        first = install.build_package(HERE, SHA_A, self.dir / "one")
        second = install.build_package(HERE, SHA_A, self.dir / "two")
        self.assertEqual(first["digest"], second["digest"])
        with tarfile.open(first["tarball"]) as archive:
            names = set(archive.getnames())
        modules = {f"zakura_monitoring/{path.name}"
                   for path in (HERE / "zakura_monitoring").glob("*.py")}
        self.assertTrue(modules <= names)
        for required in ("MANIFEST.json", "zakura-compat-check", "zakura-cluster-watchdog.py",
                         "zakura-monitoring-acceptance.py", "fleet-watchdog.toml",
                         "zakura-fleet-watchdog.service", "zakura_monitoring/install.py"):
            self.assertIn(required, names)

    def test_rejects_short_sha(self):
        with self.assertRaises(install.InstallError):
            install.build_package(HERE, "abc", self.dir)


class ReleaseTests(InstallCase):
    def test_installed_slack_acceptance_posts_labeled_failure_and_recovery(self):
        self.stage(SHA_A)
        install.activate(self.root, SHA_A, LINKS)
        received = []

        class Receiver(BaseHTTPRequestHandler):
            def do_POST(self):
                size = int(self.headers["Content-Length"])
                received.append(json.loads(self.rfile.read(size)))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        env_file = self.dir / "slack.env"
        env_file.write_text(f"SLACK_WEB_HOOK=http://127.0.0.1:{server.server_port}/\n")
        result = subprocess.run(
            [sys.executable, "-I",
             str(self.root / "current" / "zakura-monitoring-acceptance.py"),
             "slack-test", "--env-file", str(env_file), "--confirm-real-slack"],
            cwd="/", env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["accepted"], [True, True])
        self.assertTrue(report["temporary_state_only"])
        self.assertEqual(len(received), 2)
        for payload in received:
            self.assertTrue(payload["text"].startswith(report["label"] + "\n"))
        self.assertIn("zcashd has *2 peers*; expected *1*.", received[0]["text"])
        self.assertIn("compatibility problem", received[0]["text"])
        self.assertIn("compatibility restored", received[1]["text"])
        for payload in received:
            self.assertEqual(len(payload["text"].splitlines()), 4)
            self.assertEqual(payload["text"].count("Observed"), 1)
            for omitted in ("root", "159.203.113.196", "zcashd_compat_sync", "height:", "drift:"):
                self.assertNotIn(omitted, payload["text"])

    def test_stage_activate_and_run_outside_the_repository(self):
        self.assertFalse(self.stage(SHA_A)["reused"])
        self.assertTrue(self.stage(SHA_A)["reused"])
        result = install.activate(self.root, SHA_A, LINKS)
        self.assertEqual((result["active"], result["previous"]), (SHA_A, None))
        current = self.root / "current"
        self.assertEqual(os.readlink(current), f"releases/{SHA_A}")
        self.assertEqual(os.readlink(self.root / "fleets.toml"), "current/fleet-watchdog.toml")
        mode = (current / "zakura-compat-check").stat().st_mode
        self.assertTrue(mode & stat.S_IXUSR)

        isolated = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        version = subprocess.run(
            [sys.executable, "-I", str(current / "zakura-compat-check"), "version"],
            capture_output=True, text=True, cwd="/", env=isolated, timeout=60,
        )
        self.assertEqual(json.loads(version.stdout)["sha"], SHA_A)
        modules = sorted(path.stem for path in (current / "zakura_monitoring").glob("*.py"))
        imports = subprocess.run(
            [sys.executable, "-I", "-c",
             "import sys; sys.path.insert(0, sys.argv[1]); import importlib\n"
             "for name in sys.argv[2:]: importlib.import_module('zakura_monitoring.' + name)",
             str(current), *modules],
            capture_output=True, text=True, cwd="/", env=isolated, timeout=60,
        )
        self.assertEqual(imports.returncode, 0, imports.stderr)
        watchdog_help = subprocess.run(
            [sys.executable, "-I", str(self.root / "zakura-cluster-watchdog.py"), "--help"],
            capture_output=True, text=True, cwd="/", env=isolated, timeout=60,
        )
        self.assertEqual(watchdog_help.returncode, 0, watchdog_help.stderr)
        self.assertIn("--compat-monitoring", watchdog_help.stdout)

    def test_activation_records_and_rolls_back_to_the_previous_release(self):
        self.stage(SHA_A)
        self.stage(SHA_B)
        install.activate(self.root, SHA_A, LINKS)
        install.activate(self.root, SHA_B, LINKS)
        self.assertEqual(install.current_release(self.root), SHA_B)
        result = install.rollback(self.root, LINKS)
        self.assertEqual((result["active"], result["previous"]), (SHA_A, SHA_B))
        self.assertEqual(install.current_release(self.root), SHA_A)

    def test_rollback_retries_preserve_the_original_target(self):
        self.stage(SHA_A)
        self.stage(SHA_B)
        install.activate(self.root, SHA_A, LINKS)
        install.activate(self.root, SHA_B, LINKS)
        record = (self.root / "rollback.json").read_bytes()
        for _ in range(3):
            install.rollback(self.root, LINKS)
            self.assertEqual(install.current_release(self.root), SHA_A)
            self.assertEqual((self.root / "rollback.json").read_bytes(), record)
        install.activate(self.root, SHA_B, LINKS)
        install.rollback(self.root, LINKS)
        self.assertEqual(install.current_release(self.root), SHA_A)

    def test_rollback_retries_repair_links_after_partial_failure(self):
        self.stage(SHA_A)
        self.stage(SHA_B)
        install.activate(self.root, SHA_A, LINKS)
        install.activate(self.root, SHA_B, LINKS)
        with mock.patch.object(install, "point_links", side_effect=OSError("interrupted")):
            with self.assertRaises(OSError):
                install.rollback(self.root, LINKS)
        for _ in range(2):
            install.rollback(self.root, LINKS)
            self.assertEqual(install.current_release(self.root), SHA_A)
            self.assertEqual(os.readlink(self.root / "fleets.toml"),
                             "current/fleet-watchdog.toml")

    def test_first_activation_preserves_a_legacy_install(self):
        self.root.mkdir()
        (self.root / "zakura-cluster-watchdog.py").write_text("# legacy script\n")
        (self.root / "fleets.toml").write_text("# legacy config\n")
        self.stage(SHA_A)
        result = install.activate(self.root, SHA_A, LINKS)
        legacy = result["legacy_preserved"]
        self.assertRegex(legacy, install.LEGACY)
        self.assertTrue((self.root / "zakura-cluster-watchdog.py").is_symlink())
        install.rollback(self.root, LINKS)
        install.rollback(self.root, LINKS)
        self.assertEqual(install.current_release(self.root), legacy)
        self.assertEqual((self.root / "zakura-cluster-watchdog.py").read_text(),
                         "# legacy script\n")
        self.assertEqual((self.root / "fleets.toml").read_text(), "# legacy config\n")

    def test_rollback_without_previous_release_deactivates(self):
        self.stage(SHA_A)
        install.activate(self.root, SHA_A, {})
        self.assertIsNone(install.rollback(self.root, {})["active"])
        self.assertIsNone(install.rollback(self.root, {})["active"])
        self.assertFalse((self.root / "current").exists())

    def test_digest_and_content_tampering_are_rejected(self):
        package = self.package(SHA_A)
        with self.assertRaises(install.InstallError):
            install.stage(self.root, SHA_A, Path(package["tarball"]), "0" * 64)
        with self.assertRaises(install.InstallError):
            install.stage(self.root, SHA_B, Path(package["tarball"]), package["digest"])
        self.stage(SHA_A)
        script = self.root / "releases" / SHA_A / "zakura-compat-check"
        script.chmod(0o755)
        script.write_text("tampered\n")
        with self.assertRaises(install.InstallError):
            install.activate(self.root, SHA_A, {})

    def test_unsafe_archive_members_are_rejected(self):
        evil = self.dir / "evil.tar.gz"
        with tarfile.open(evil, "w:gz") as archive:
            info = tarfile.TarInfo("../escape")
            info.size = 1
            archive.addfile(info, io.BytesIO(b"x"))
        with self.assertRaises(install.InstallError):
            install.stage(self.root, SHA_A, evil, install.sha256_file(evil))
        self.assertFalse((self.dir / "escape").exists())


class HostSettingsTests(InstallCase):
    def test_fleet_env_preserves_operator_settings_and_mode(self):
        env = self.dir / "env"
        env.write_text("SLACK_WEB_HOOK=old\nZAKURA_MAC_CRANELIFT_COMPARISON=1\n# note\n")
        output = io.StringIO()
        with redirect_stdout(output):
            result = install.fleet_env(env, WEBHOOK)
        self.assertEqual(result["slack_webhook"], "replaced")
        self.assertNotIn("s3cr3t", json.dumps(result) + output.getvalue())
        text = env.read_text()
        self.assertIn("ZAKURA_MAC_CRANELIFT_COMPARISON=1", text)
        self.assertIn("# note", text)
        self.assertIn(f"SLACK_WEB_HOOK={WEBHOOK}", text)
        self.assertNotIn("SLACK_WEB_HOOK=old", text)
        self.assertEqual(stat.S_IMODE(env.stat().st_mode), 0o600)
        self.assertEqual(install.fleet_env(env)["slack_webhook"], "preserved")
        self.assertIn(f"SLACK_WEB_HOOK={WEBHOOK}", env.read_text())
        self.assertEqual(install.fleet_env(self.dir / "new")["slack_webhook"], "missing")

    def test_existing_unit_is_never_replaced(self):
        unit = self.dir / "unit.service"
        template = HERE / "zakura-fleet-watchdog.service"
        self.assertTrue(install.ensure_unit(unit, template)["installed"])
        unit.write_text("[Service]\nExecStart=/live --mac-args\n")
        self.assertFalse(install.ensure_unit(unit, template)["installed"])
        self.assertIn("--mac-args", unit.read_text())

    def test_compat_env_seeds_checker_keys_only(self):
        source = self.dir / "rust.env"
        source.write_text("SENTRY_DSN=https://dsn.invalid/1\nHEIGHT_MAX_DRIFT=12\n"
                          "ZCASHD_RPC_PASSWORD=s3cr3t\n")
        destination = self.dir / "etc" / "compat.env"
        result = install.compat_env(HERE, source, destination)
        self.assertTrue(result["seeded"])
        self.assertNotIn("s3cr3t", json.dumps(result))
        self.assertEqual(destination.read_text(),
                         "HEIGHT_MAX_DRIFT=12\nZCASHD_RPC_PASSWORD=s3cr3t\n")
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o600)
        destination.write_text("HEIGHT_MAX_DRIFT=5\n")
        self.assertFalse(install.compat_env(HERE, source, destination)["seeded"])
        self.assertEqual(destination.read_text(), "HEIGHT_MAX_DRIFT=5\n")

    def test_lane_drop_in_and_known_hosts(self):
        drop_in = self.dir / "drop-in" / "80-compat-monitoring.conf"
        install.set_lane(True, drop_in)
        self.assertIn("ZAKURA_COMPAT_MONITORING=1", drop_in.read_text())
        install.set_lane(False, drop_in)
        self.assertFalse(drop_in.exists())
        hosts = self.dir / "known_hosts"
        install.known_hosts(hosts, b"|1|abc=|def= ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA\n")
        self.assertEqual(len(hosts.read_text().splitlines()), 1)
        with self.assertRaises(install.InstallError):
            install.known_hosts(hosts, b"159.203.113.196 ssh-ed25519 AAA; rm -rf /\n")

    def test_state_backup_and_restore(self):
        state = self.dir / "state.json"
        state.write_text('{"version": 1, "compatibility": {}}\n')
        backup = install.backup_state(state, self.dir / "backups")["state_backup"]
        state.write_text('{"version": 1}\n')
        install.restore_state(Path(backup), state)
        self.assertIn("compatibility", state.read_text())


class RustWatchdogTests(InstallCase):
    def test_retire_moves_artifacts_aside_and_restore_brings_them_back(self):
        artifacts = (self.dir / "etc/systemd/system/zakura-watchdog.service",
                     self.dir / "usr/local/bin/zakura-watchdog",
                     self.dir / "etc/zakura-watchdog/env")
        for path in artifacts:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"content of {path.name}\n")
        calls = []
        fake = lambda *args: (calls.append(args),
                              subprocess.CompletedProcess(args, 0, "", ""))[1]
        backups = self.dir / "backups"
        with mock.patch.object(install, "RUST_ARTIFACTS", artifacts), \
                mock.patch.object(install, "systemctl", side_effect=fake):
            retired = install.retire_rust(backups)
            self.assertTrue(retired["retired"])
            self.assertFalse(any(path.exists() for path in artifacts))
            self.assertIn(("disable", "--now", "zakura-watchdog.service"), calls)
            restored = install.restore_rust(backups)
        self.assertTrue(restored["restored"])
        self.assertTrue(all(path.exists() for path in artifacts))
        self.assertIn(("enable", "--now", "zakura-watchdog.service"), calls)
        self.assertNotIn("systemctl", json.dumps(calls).replace("zakura-watchdog", ""))

    def artifacts(self):
        paths = tuple(self.dir / name for name in ("unit", "binary", "env"))
        for path in paths:
            path.write_text(path.name)
        return paths

    def test_failed_stop_does_not_move_artifacts_or_create_backup(self):
        artifacts = self.artifacts()
        backups = self.dir / "backups"
        with mock.patch.object(install, "RUST_ARTIFACTS", artifacts), \
                mock.patch.object(install, "systemctl", return_value=
                                  subprocess.CompletedProcess([], 1, "", "")):
            with self.assertRaises(install.InstallError):
                install.retire_rust(backups)
            with redirect_stdout(io.StringIO()) as output:
                self.assertEqual(install.main(["retire-rust", "--backup-root", str(backups)]), 1)
            self.assertIn("InstallError", output.getvalue())
        self.assertTrue(all(path.exists() for path in artifacts))
        self.assertFalse(backups.exists())

    def test_failed_restart_is_reported_and_restore_can_be_retried(self):
        artifacts = self.artifacts()
        backups = self.dir / "backups"
        def service(*args):
            return subprocess.CompletedProcess(args, int(args[0] == "enable"), "", "")
        with mock.patch.object(install, "RUST_ARTIFACTS", artifacts), \
                mock.patch.object(install, "systemctl", side_effect=service):
            install.retire_rust(backups)
            with self.assertRaises(install.InstallError):
                install.restore_rust(backups)
            with redirect_stdout(io.StringIO()) as output:
                self.assertEqual(install.main(["restore-rust", "--backup-root", str(backups)]), 1)
            self.assertIn("InstallError", output.getvalue())
        self.assertTrue(all(path.exists() for path in artifacts))
        self.assertFalse(list(backups.glob("*.restored")))
        with mock.patch.object(install, "systemctl", return_value=
                              subprocess.CompletedProcess([], 0, "", "")):
            for _ in range(2):
                self.assertTrue(install.restore_rust(backups)["service_started"])

    def test_partial_restore_can_be_retried_without_overwriting_files(self):
        artifacts = self.artifacts()
        backups = self.dir / "backups"
        with mock.patch.object(install, "RUST_ARTIFACTS", artifacts), \
                mock.patch.object(install, "systemctl", return_value=
                                  subprocess.CompletedProcess([], 0, "", "")):
            install.retire_rust(backups)
            original_move = install.shutil.move
            count = 0
            def interrupted_move(source, destination):
                nonlocal count
                count += 1
                if count == 2:
                    raise OSError("interrupted")
                return original_move(source, destination)
            with mock.patch.object(install.shutil, "move", side_effect=interrupted_move):
                with self.assertRaises(OSError):
                    install.restore_rust(backups)
            self.assertTrue(install.restore_rust(backups)["service_started"])
            artifacts[0].write_text("operator changed the unit")
            with self.assertRaises(install.InstallError):
                install.restore_rust(backups)
            self.assertEqual(artifacts[0].read_text(), "operator changed the unit")

    def test_daemon_reload_failure_is_not_reported_as_success(self):
        artifacts = self.artifacts()
        backups = self.dir / "backups"
        with mock.patch.object(install, "RUST_ARTIFACTS", artifacts), \
                mock.patch.object(install, "systemctl", side_effect=lambda *args:
                    subprocess.CompletedProcess(args, int(args[0] == "daemon-reload"), "", "")):
            with self.assertRaises(install.InstallError):
                install.retire_rust(backups)
            with self.assertRaises(install.InstallError):
                install.restore_rust(backups)

    def test_retire_without_artifacts_is_a_no_op(self):
        with mock.patch.object(install, "RUST_ARTIFACTS", (self.dir / "absent",)):
            self.assertFalse(install.retire_rust(self.dir / "backups")["retired"])


class AcceptanceReceiptTests(InstallCase):
    def setUp(self):
        super().setUp()
        self.stage(SHA_A)
        install.activate(self.root, SHA_A, {})
        with mock.patch.object(install.time, "time", return_value=1000):
            self.record = install.acceptance(self.root, SHA_A, "begin")
        self.report = {"duration": 1800, "elapsed": 1800, "started_at": 1000,
                       "finished_at": 2800, "release": {"sha": SHA_A}, "passed": True,
                       "checks": dict.fromkeys(install.SOAK_CHECKS, True)}

    def accept(self, report=None, generation=None):
        with mock.patch.object(install.time, "time", return_value=4000):
            return install.acceptance(self.root, SHA_A, "soak",
                                      generation or self.record["generation"], report or self.report)

    def test_require_rejects_missing_proof_and_accepts_persisted_success(self):
        with self.assertRaises(install.InstallError):
            install.acceptance(self.root, SHA_A, "require")
        self.accept()
        self.assertTrue(install.acceptance(self.root, SHA_A, "require")["soak"])
        result = subprocess.run(
            [sys.executable, "-I", str(self.root / "current/zakura_monitoring/install.py"),
             "acceptance", "--root", str(self.root), "--sha", SHA_A, "--action", "require"],
            cwd="/", capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["generation"], self.record["generation"])
        self.assertEqual(stat.S_IMODE((self.root / "acceptance.json").stat().st_mode), 0o600)

    def test_failed_short_stale_or_wrong_release_soaks_never_qualify(self):
        changes = [
            {"passed": False}, {"duration": 1799}, {"elapsed": 1799},
            {"elapsed": float("nan")}, {"release": None}, {"started_at": 999}, {"finished_at": 2799},
            {"finished_at": 5000}, {"release": {"sha": SHA_B}},
            {"checks": {}}, {"checks": {**self.report["checks"], "height_advanced": False}},
        ]
        for change in changes:
            with self.subTest(change=change):
                with self.assertRaises(install.InstallError):
                    self.accept({**self.report, **change})
                with self.assertRaises(install.InstallError):
                    install.acceptance(self.root, SHA_A, "require")

    def test_new_cutover_invalidates_proof_and_rejects_inflight_old_soak(self):
        self.accept()
        new = install.acceptance(self.root, SHA_A, "begin")
        self.assertNotEqual(new["generation"], self.record["generation"])
        with self.assertRaises(install.InstallError):
            self.accept()
        with self.assertRaises(install.InstallError):
            install.acceptance(self.root, SHA_A, "require")
        self.stage(SHA_B)
        install.activate(self.root, SHA_B, {})
        with self.assertRaises(install.InstallError):
            install.acceptance(self.root, SHA_A, "require")
        with self.assertRaises(install.InstallError):
            install.acceptance(self.root, SHA_B, "require")

    def test_malformed_persisted_proof_cannot_qualify_retirement(self):
        for proof in (True, {}, {"started_at": 1000, "finished_at": 2800, "elapsed": 1799},
                      {"started_at": 999, "finished_at": 2800, "elapsed": 1800}):
            (self.root / "acceptance.json").write_text(json.dumps({**self.record, "soak": proof}))
            with self.assertRaises(install.InstallError):
                install.acceptance(self.root, SHA_A, "require")

    def test_missing_and_malformed_records_are_rejected(self):
        for data in (None, "not json", "null", '{}'):
            path = self.root / "acceptance.json"
            if data is None:
                path.unlink(missing_ok=True)
            else:
                path.write_text(data)
            with self.assertRaises(install.InstallError):
                install.acceptance(self.root, SHA_A, "require")


class AcceptanceToolTests(InstallCase):
    def setUp(self):
        super().setUp()
        spec = importlib.util.spec_from_file_location(
            "monitoring_acceptance_tests", HERE / "zakura-monitoring-acceptance.py")
        self.tool = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.tool)
        self.args = argparse.Namespace(rust_bin=self.dir / "zakura-watchdog",
                                       rust_env=self.dir / "missing-env", env_file=None,
                                       rust_backups=self.dir / "backups", timeout=1,
                                       height_max_drift=10)

    def parity(self, timed_out=False):
        from zakura_monitoring import remote
        result = remote.BoundedResult(0, b'{"status":"pass"}', timed_out, False, 0)
        with mock.patch.object(self.tool.remote, "run_bounded", return_value=result) as run, \
                redirect_stdout(io.StringIO()) as output:
            code = self.tool.parity(self.args)
        return code, json.loads(output.getvalue()), run

    def test_parity_uses_verified_retired_binary_after_finalization(self):
        self.args.rust_bin.write_text("reference executable")
        self.assertEqual(self.parity()[0], 0)
        with mock.patch.object(install, "RUST_ARTIFACTS", (self.args.rust_bin,)), \
                mock.patch.object(install, "systemctl", return_value=
                                  subprocess.CompletedProcess([], 0, "", "")):
            retired = install.retire_rust(self.args.rust_backups)
        code, report, run = self.parity()
        self.assertEqual(code, 0)
        self.assertEqual(report["rust"]["reference"], "retired")
        self.assertTrue(str(run.call_args_list[0].args[0][0]).startswith(retired["backup"]))
        for call in run.call_args_list:
            self.assertNotIn("SENTRY_DSN", call.kwargs["env"])
        saved = Path(run.call_args_list[0].args[0][0])
        saved.write_text("tampered")
        self.assertEqual(self.parity()[0], 1)

    def test_missing_reference_and_timeout_cannot_pass_parity(self):
        self.assertEqual(self.parity()[0], 1)
        self.args.rust_bin.write_text("reference executable")
        self.assertEqual(self.parity(timed_out=True)[0], 1)

    def test_soak_reports_actual_elapsed_and_release(self):
        from zakura_monitoring import monitor
        clock = [0.0]
        def sleep(seconds):
            clock[0] += seconds
        def state(_path):
            completed = int(clock[0] // 60)
            return {monitor.COMPAT_PROBES: {"zakura-compat": {
                "completed": completed, "passed": completed, "unavailable": 0,
                "last": {"status": "pass", "details": {
                    "zakura_height": 100 + completed, "zcashd_height": 100 + completed}}}}}
        self.args = argparse.Namespace(state=self.dir / "state.json", target="zakura-compat",
                                       unit="fake.service", duration=1800, interval=60, report=None)
        self.args.state.write_text('{}')
        self.stage(SHA_A)
        install.activate(self.root, SHA_A, {})
        with mock.patch.object(self.tool.time, "monotonic", side_effect=lambda: clock[0]), \
                mock.patch.object(self.tool.time, "time", side_effect=lambda: 1000 + clock[0]), \
                mock.patch.object(self.tool.time, "sleep", side_effect=sleep), \
                mock.patch.object(self.tool.state_module, "load_state", side_effect=state), \
                mock.patch.object(self.tool, "service_state", return_value="active"), \
                mock.patch.object(self.tool.compat, "release_info", return_value={"sha": SHA_A}), \
                redirect_stdout(io.StringIO()) as output:
            record = install.acceptance(self.root, SHA_A, "begin")
            self.assertEqual(self.tool.soak(self.args), 0)
            report = json.loads(output.getvalue())
            install.acceptance(self.root, SHA_A, "soak", record["generation"], report)
            self.assertTrue(install.acceptance(self.root, SHA_A, "require")["soak"])
        report = json.loads(output.getvalue())
        self.assertTrue(report["passed"])
        self.assertEqual(report["elapsed"], 1800)
        self.assertEqual(report["release"]["sha"], SHA_A)
        self.assertEqual(report["finished_at"] - report["started_at"], 1800)
        for invalid in (0, -1, float("nan"), float("inf")):
            self.args.duration = invalid
            with redirect_stdout(io.StringIO()):
                self.assertEqual(self.tool.soak(self.args), 2)


if __name__ == "__main__":
    unittest.main()
