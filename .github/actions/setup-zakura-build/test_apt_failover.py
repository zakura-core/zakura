#!/usr/bin/env python3
"""Exercise mirror-file failover with real apt and an isolated local repository."""

import functools
import hashlib
import http.server
import os
from pathlib import Path
import pwd
import subprocess
import tempfile
import threading
import unittest


INSTALLER = Path(__file__).with_name("install-ubuntu-packages.sh")
PACKAGE = "zakura-apt-fixture"
DEB = f"{PACKAGE}_1_all.deb"


class MirrorServer(http.server.ThreadingHTTPServer):
    """Serve the healthy mirror and keep the primary download stalled until cleanup."""

    def __init__(self, directory):
        super().__init__(
            ("127.0.0.1", 0), functools.partial(MirrorHandler, directory=directory)
        )
        self.requests = []
        self.release_stall = threading.Event()


class MirrorHandler(http.server.SimpleHTTPRequestHandler):
    """Record which mirror apt uses, stalling only the primary package request."""

    def do_GET(self):
        self.server.requests.append(self.path)
        if self.path == f"/azure.archive.ubuntu.com/{DEB}":
            self.server.release_stall.wait(60)
            return
        super().do_GET()

    def log_message(self, format, *args):
        pass


def make_repository(root):
    """Build a disposable package and identical flat repositories for both mirrors."""
    package_dir = root / "package"
    (package_dir / "DEBIAN").mkdir(parents=True)
    control = (
        f"Package: {PACKAGE}\nVersion: 1\nArchitecture: all\n"
        "Maintainer: Zakura CI <ci@example.invalid>\n"
        "Description: Disposable apt failover fixture\n"
    )
    (package_dir / "DEBIAN/control").write_text(control)
    deb = root / DEB
    subprocess.run(["dpkg-deb", "--root-owner-group", "--build", str(package_dir), str(deb)], check=True)
    payload = deb.read_bytes()
    index = (
        control + f"Filename: {DEB}\nSize: {len(payload)}\n"
        f"SHA256: {hashlib.sha256(payload).hexdigest()}\n\n"
    )
    for mirror in ("azure.archive.ubuntu.com", "archive.ubuntu.com"):
        directory = root / mirror
        directory.mkdir()
        (directory / DEB).write_bytes(payload)
        (directory / "Packages").write_text(index)
    return payload


def configure_apt(root, mirrors):
    """Keep sources, installed-package status, lists and downloads off the host."""
    for directory in ("lists/partial", "archives/partial", "empty", "log"):
        (root / directory).mkdir(parents=True)
    (root / "status").touch()
    (root / "sources.list").write_text(f"deb [trusted=yes] mirror+file:{mirrors} ./\n")
    settings = {
        "Dir::Etc::sourcelist": root / "sources.list",
        "Dir::Etc::sourceparts": root / "empty",
        "Dir::Etc::parts": root / "empty",
        "Dir::Etc::main": "-",
        "Dir::Log": root / "log",
        "Dir::State::status": root / "status",
        "Dir::State::lists": root / "lists",
        "Dir::Cache::archives": root / "archives",
        "Dir::Cache::pkgcache": "",
        "Dir::Cache::srcpkgcache": "",
        "APT::Sandbox::User": pwd.getpwuid(os.getuid()).pw_name,
        "Acquire::Languages": "none",
        "Acquire::Retries": "0",
        "Acquire::http::Proxy": "DIRECT",
        "Acquire::http::Timeout": "30",
    }
    config = root / "apt.conf"
    config.write_text("".join(f'{key} "{value}";\n' for key, value in settings.items()))
    return config


class AptFailoverTests(unittest.TestCase):
    def test_download_reloads_mirrors_after_timeout(self):
        with tempfile.TemporaryDirectory(prefix="zakura-apt-") as tmp:
            root = Path(tmp)
            payload = make_repository(root)
            server = MirrorServer(str(root))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                address = f"http://127.0.0.1:{server.server_port}"
                # The production helper identifies the Azure entry by this label.
                # Local URL paths let us exercise it without DNS or public mirrors.
                primary = f"{address}/azure.archive.ubuntu.com/\tpriority:1\n"
                fallback = f"{address}/archive.ubuntu.com/\tpriority:2\n"
                mirrors = root / "mirrors.txt"
                mirrors.write_text(fallback)
                env = {
                    **os.environ,
                    "APT_CONFIG": str(configure_apt(root, mirrors)),
                    "APT_MIRRORS_FILE": str(mirrors),
                    "APT_BUDGET_SECONDS": "120",
                    "APT_ATTEMPT_SECONDS": "3",
                }
                update = subprocess.run(
                    ["apt-get", "update"], env=env, capture_output=True, text=True, timeout=30
                )
                self.assertEqual(update.returncode, 0, update.stdout + update.stderr)
                mirrors.write_text(primary + fallback)
                result = subprocess.run(
                    [
                        "bash", "-c",
                        # No privilege is needed for the isolated apt directories.
                        'source "$1"\nsudo() { "$@"; }\n'
                        'apt_deadline=$((SECONDS + APT_BUDGET_SECONDS))\n'
                        f'run_apt_network download apt-get install -y --download-only {PACKAGE}\n',
                        "apt-failover", str(INSTALLER),
                    ],
                    env=env, capture_output=True, text=True, timeout=45,
                )
                output = result.stdout + result.stderr
                self.assertEqual(result.returncode, 0, output)
                self.assertIn("attempt 1 reached its 3s limit and was killed", output)
                self.assertIn("attempt 2 succeeded", output)
                self.assertEqual(mirrors.read_text(), fallback)
                package_requests = [path for path in server.requests if path.endswith(DEB)]
                self.assertEqual(package_requests, [
                    f"/azure.archive.ubuntu.com/{DEB}",
                    f"/archive.ubuntu.com/{DEB}",
                ], output)
                self.assertEqual((root / "archives" / DEB).read_bytes(), payload)
                self.assertEqual((root / "status").read_text(), "")
                print(output)
            finally:
                server.release_stall.set()
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


def main(argv=None):
    """Run the Linux apt integration test; missing tools are a failure, not a skip."""
    unittest.main(argv=argv)


if __name__ == "__main__":
    main()
