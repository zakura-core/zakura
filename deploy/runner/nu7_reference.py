"""Expose only the chain reads needed to qualify the independent NU7 reference.

The RPC cookie stays on the reference host. Both this handler and the host
firewall restrict callers; the upstream remains bound to loopback.
"""
from __future__ import annotations

import argparse
import base64
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path

from nu7_activation import read_json

MAX_REQUEST_BYTES = 4096


class ReferenceServer(HTTPServer):
    """Serve bounded, serial chain reads using a locally rotated RPC cookie."""

    def __init__(self, address, *, upstream, cookie, collector):
        self.upstream = upstream
        self.cookie = Path(cookie)
        self.allowed = {collector, "127.0.0.1"}
        super().__init__(address, ReferenceHandler)


class ReferenceHandler(BaseHTTPRequestHandler):
    timeout = 5

    def handle(self):
        # Reject other callers before reading their headers or request body.
        if self.client_address[0] not in self.server.allowed:
            self.connection.sendall(b"HTTP/1.0 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
            return
        super().handle()

    def log_message(self, *_args):
        """Keep upstream responses and authentication details out of logs."""

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if self.path != "/" or not 0 < length <= MAX_REQUEST_BYTES or self.headers.get("Transfer-Encoding"):
                raise ValueError("invalid request size or path")
            request = json.loads(self.rfile.read(length))
            if not isinstance(request, dict):
                raise ValueError("batch requests are not supported")
            method, params = request.get("method"), request.get("params", [])
            valid = (method in ("getinfo", "getblockchaininfo") and params == []) or (
                method == "getblockhash" and isinstance(params, list) and len(params) == 1
                and type(params[0]) is int and 0 <= params[0] <= 0xFFFFFFFF)
            if not valid:
                raise ValueError("method or parameters are not allowed")
        except (ValueError, TypeError, TimeoutError):
            self.send_error(400, "Invalid reference read")
            return
        try:
            with self.server.cookie.open() as stream:
                cookie = stream.read(MAX_REQUEST_BYTES + 1).strip()
            if ":" not in cookie or len(cookie) > MAX_REQUEST_BYTES:
                raise ValueError("invalid cookie")
            response = read_json(self.server.upstream,
                json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
                {"Content-Type": "application/json", "Authorization": "Basic " + base64.b64encode(cookie.encode()).decode()})
            body = json.dumps(response, allow_nan=False).encode()
        except (OSError, ValueError, TypeError):
            self.send_error(502, "Reference unavailable")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8097)
    parser.add_argument("--collector", default="167.99.103.111")
    args = parser.parse_args(argv)
    with ReferenceServer((args.host, args.port), upstream="http://[::1]:8232/",
                         cookie="/mnt/data/runtime/zcashd/.cookie", collector=args.collector) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
