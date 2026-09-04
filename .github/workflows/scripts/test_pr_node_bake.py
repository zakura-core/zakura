#!/usr/bin/env python3
"""Exercise bake download recovery against a local HTTP range server."""

import hashlib
import http.server
import io
import pathlib
import socket
import subprocess
import tarfile
import tempfile
import threading
import unittest


def fetch_function():
    # Exercise the shipped function without installing or building a node.
    source = pathlib.Path(__file__).with_name("pr-node-bake.sh").read_text()
    return (
        "fetch_state() {"
        + source.split("fetch_state() {", 1)[1].split("\n}\n", 1)[0]
        + "\n}\n"
    )


class BakeDownload(unittest.TestCase):
    def test_deadline_caps_connections_backoff_and_later_downloads(self):
        for budget, expected_timeouts in ((1000, ["600", "385"]), (605, ["600"])):
            with self.subTest(budget=budget):
                with tempfile.TemporaryDirectory() as tmp:
                    root = pathlib.Path(tmp)
                    (root / "clock").write_text("0")
                    script = (
                        r"""
        set -euo pipefail
        cd "$1"
        BAKE_DOWNLOAD_DEADLINE=$2
        date() { cat clock; }
        sleep() { echo "$(( $(cat clock) + $1 ))" > clock; }
        curl() {
          while [ "$1" != --max-time ]; do shift; done
          echo "$2" >> timeouts
          echo "$(( $(cat clock) + $2 ))" > clock
          return 28
        }
        """
                        + fetch_function()
                        + r"""
        fetch_state https://example.invalid/first '' "$1/first" mainnet && exit 1
        fetch_state https://example.invalid/second '' "$1/second" mainnet && exit 1
        exit 0
        """
                    )
                    result = subprocess.run(
                        ["bash", "-c", script, "test", str(root), str(budget)],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(
                        (root / "timeouts").read_text().splitlines(), expected_timeouts
                    )
                    self.assertEqual((root / "clock").read_text().strip(), str(budget))
                    self.assertEqual(
                        result.stderr.count("state download deadline reached"), 2
                    )
                    self.assertFalse((root / "first").exists())
                    self.assertFalse((root / "second").exists())

    def test_interrupted_download_resumes_and_verifies_archive(self):
        self.check_interrupted_download(1)

    def test_download_can_resume_more_than_nine_times(self):
        self.check_interrupted_download(10)

    def check_interrupted_download(self, interruptions):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as archive:
                payload = b"validated chain fixture"
                info = tarfile.TarInfo("state/v1/mainnet/fixture")
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
            data = subprocess.run(
                ["zstd", "-c"], input=buffer.getvalue(), capture_output=True, check=True
            ).stdout
            cut = len(data) // (interruptions + 1)
            ranges = []

            class Handler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    ranges.append(self.headers.get("Range"))
                    byte_range = self.headers.get("Range")
                    offset = int((byte_range or "bytes=0-").split("=")[1].split("-")[0])
                    self.send_response(206 if byte_range else 200)
                    self.send_header("Content-Length", str(len(data) - offset))
                    if byte_range:
                        self.send_header(
                            "Content-Range",
                            f"bytes {offset}-{len(data) - 1}/{len(data)}",
                        )
                    self.end_headers()
                    if len(ranges) <= interruptions:
                        self.wfile.write(data[offset : offset + cut])
                        self.wfile.flush()
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                    else:
                        self.wfile.write(data[offset:])

                def log_message(self, *args):
                    pass

            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            script = (
                "set -euo pipefail\nsleep() { :; }\n"
                + "BAKE_DOWNLOAD_DEADLINE=$(( $(date +%s) + 60 ))\n"
                + fetch_function()
                + '\nfetch_state "$1" "$2" "$3" mainnet\n'
            )
            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    script,
                    "test",
                    f"http://127.0.0.1:{server.server_port}/fixture",
                    hashlib.sha256(data).hexdigest(),
                    str(root / "tip"),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                ranges,
                [None] + [f"bytes={cut * n}-" for n in range(1, interruptions + 1)],
            )
            self.assertEqual(
                (root / "tip/state/v1/mainnet/fixture").read_bytes(), payload
            )
            self.assertFalse((root / "tip.tar.zst").exists())


if __name__ == "__main__":
    unittest.main()
