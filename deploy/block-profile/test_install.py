"""Verify quota preflight without invoking any host mutation commands."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

INSTALLER = Path(__file__).with_name("install.sh")


class Installation(unittest.TestCase):
    def run_installer(self, state, status):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "mutations"
            scripts = {
                "explorer": "exit 0\n",
                "id": 'if [ "$1" = -u ]; then echo 0; fi\n',
                "mountpoint": "exit 0\n",
                "findmnt": 'case "$*" in *FSTYPE*) echo ext4;; *) echo rw,grpquota;; esac\n',
                "getent": "exit 0\n",
                "logrotate": "exit 0\n",
                "quotaon": 'echo "group quota on /srv/zakura-profile (/dev/test) is $QUOTA_STATE"; exit "$QUOTA_STATUS"\n',
            }
            for command in ["groupadd", "useradd", "usermod", "install", "setquota", "systemctl"]:
                scripts[command] = f'echo "{command} $*" >> "$MUTATION_LOG"\n'
            for name, script in scripts.items():
                file = root / name
                file.write_text("#!/bin/sh\n" + script)
                file.chmod(0o755)
            result = subprocess.run(
                ["bash", str(INSTALLER), str(root / "explorer"), "zakura"],
                env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}", "QUOTA_STATE": state,
                     "QUOTA_STATUS": str(status), "MUTATION_LOG": str(log)},
                capture_output=True, text=True, timeout=5,
            )
            return result, log.read_text() if log.exists() else ""

    def test_enabled_quota_returning_one_allows_install(self):
        result, mutations = self.run_installer("on", 1)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("setquota -g zakura-profile 87890625 97656250 0 0 /srv/zakura-profile", mutations)
        self.assertIn("systemctl daemon-reload", mutations)
        self.assertIn("/etc/zakura-profile-logrotate.conf", mutations)
        self.assertIn("systemctl enable --now zakura-profile-logrotate.timer", mutations)

    def test_disabled_quota_returning_zero_prevents_changes(self):
        result, mutations = self.run_installer("off", 0)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(mutations, "")

    def test_unrecognized_quota_state_prevents_changes(self):
        result, mutations = self.run_installer("unknown", 1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(mutations, "")


if __name__ == "__main__":
    unittest.main()
