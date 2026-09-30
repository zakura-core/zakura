#!/usr/bin/env python3
"""Fixed read-only loopback adapter for a native Mac verifier."""
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import platform
import re
import shutil
import subprocess
import time

from common import RPC, Unavailable, digest, read_json


def resources(base):
    result = {"free_disk_bytes": shutil.disk_usage(base).free,
              "node_rss_bytes": None, "memory_free_percent": None}
    try:
        pid = int((Path(base) / "run/node.pid").read_text())
        result["node_rss_bytes"] = int(subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)], timeout=3).strip()) * 1024
        if platform.system() == "Darwin":
            output = subprocess.check_output(["/usr/bin/memory_pressure", "-Q"], timeout=3).decode()
            match = re.search(r"System-wide memory free percentage: (\d+)%", output)
            if match:
                result["memory_free_percent"] = int(match.group(1))
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return result


def serve(base, rpc=None):
    base = Path(base)
    receipt = read_json(base / "receipt.json")
    rpc = rpc or RPC("http://127.0.0.1:28232")
    binary = base / "bin/zakurad"
    signature = None
    binary_digest = None

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            nonlocal signature, binary_digest
            try:
                if self.path == "/v1/status":
                    stat = binary.stat()
                    current = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
                    if current != signature:
                        binary_digest, signature = digest(binary), current
                    result = {"schema_version": 1, "sample_time": time.time(),
                              "receipt": receipt, "tip": rpc.tip(),
                              "resources": resources(base),
                              "binary_sha256": binary_digest,
                              "config_sha256": digest(base / "zakurad.toml"),
                              "architecture": platform.machine()}
                elif re.fullmatch(r"/v1/block/[0-9]{1,10}", self.path):
                    height = int(self.path.rsplit("/", 1)[1])
                    if height < receipt["bootstrap_height"] or height > rpc.tip()["height"]:
                        raise Unavailable("height outside verifier coverage")
                    result = rpc.block(height)
                else:
                    self.send_error(404)
                    return
                payload = json.dumps(result).encode()
                self.send_response(200)
            except (Unavailable, OSError, ValueError, KeyError, TypeError):
                payload = b'{"error":"sample unavailable"}'
                self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    class BoundedServer(HTTPServer):
        def get_request(self):
            connection, address = super().get_request()
            connection.settimeout(10)
            return connection, address

    server = BoundedServer(("127.0.0.1", 28233), Handler)
    server.timeout = 10
    server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="/Library/Application Support/ZakuraVerifier")
    serve(parser.parse_args().base)
