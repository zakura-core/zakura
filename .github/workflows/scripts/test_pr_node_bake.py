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


class BakeDownload(unittest.TestCase):
    def test_interrupted_download_resumes_and_verifies_archive(self):
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
            cut = len(data) // 2
            ranges = []

            class Handler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    ranges.append(self.headers.get("Range"))
                    if len(ranges) == 1:
                        self.send_response(200)
                        self.send_header("Content-Length", str(len(data)))
                        self.end_headers()
                        self.wfile.write(data[:cut])
                        self.wfile.flush()
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                    else:
                        byte_range = self.headers.get("Range")
                        offset = int(
                            (byte_range or "bytes=0-").split("=")[1].split("-")[0]
                        )
                        self.send_response(206 if byte_range else 200)
                        self.send_header("Content-Length", str(len(data) - offset))
                        if byte_range:
                            self.send_header(
                                "Content-Range",
                                f"bytes {offset}-{len(data) - 1}/{len(data)}",
                            )
                        self.end_headers()
                        self.wfile.write(data[offset:])

                def log_message(self, *args):
                    pass

            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            source = pathlib.Path(__file__).with_name("pr-node-bake.sh").read_text()
            # Exercise the shipped function without installing or building a node.
            function = (
                "fetch_state() {"
                + source.split("fetch_state() {", 1)[1].split("\n}\n", 1)[0]
                + "\n}\n"
            )
            script = (
                "set -euo pipefail\nsleep() { :; }\n"
                + function
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
            self.assertEqual(ranges, [None, f"bytes={cut}-"])
            self.assertEqual(
                (root / "tip/state/v1/mainnet/fixture").read_bytes(), payload
            )
            self.assertFalse((root / "tip.tar.zst").exists())


if __name__ == "__main__":
    unittest.main()
