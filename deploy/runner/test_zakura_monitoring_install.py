#!/usr/bin/env python3
"""Tests for versioned monitoring installs (zakura_monitoring.install).

They run from the repository and from an installed release, using temporary
roots only; systemctl is replaced, so no service is touched.
"""

from __future__ import annotations

import io
import json
import os
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from contextlib import redirect_stdout
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
        self.assertEqual((self.root / "zakura-cluster-watchdog.py").read_text(),
                         "# legacy script\n")
        self.assertEqual((self.root / "fleets.toml").read_text(), "# legacy config\n")

    def test_rollback_without_previous_release_deactivates(self):
        self.stage(SHA_A)
        install.activate(self.root, SHA_A, {})
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

    def test_retire_without_artifacts_is_a_no_op(self):
        with mock.patch.object(install, "RUST_ARTIFACTS", (self.dir / "absent",)):
            self.assertFalse(install.retire_rust(self.dir / "backups")["retired"])


if __name__ == "__main__":
    unittest.main()
