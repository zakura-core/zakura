#!/usr/bin/env python3
"""Loopback dashboard with an allowlisted, opaque verifier identity."""
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import re
from pathlib import Path

from common import read_json


def public_status(status, identifier):
    if not re.fullmatch(r"verifier-[a-f0-9]{32}", identifier):
        raise ValueError("invalid opaque identifier")
    result = {"verifier_id": identifier, "schema_version": 1}
    # Never forward receipts, diagnostic strings, OS metadata, peer IDs or hosts.
    for key in ("sample_time", "coverage_start", "compared_through", "healthy_since"):
        value = status.get(key)
        result[key] = value if type(value) in (int, float) else None
    for key in ("caught_up", "qualified"):
        result[key] = status.get(key) is True
    result["active_incidents"] = len(status.get("incidents", {}))
    result["pending_alerts"] = status.get("pending_alerts") if type(status.get("pending_alerts")) is int else None
    return result


def serve(directory, identifier):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.path not in ("/", "/v1/status"):
                self.send_error(404)
                return
            try:
                data = public_status(read_json(Path(directory) / "status.json"), identifier)
                payload = json.dumps(data).encode()
                content_type = "application/json"
                if self.path == "/":
                    payload = (b'<!doctype html><title>Verifier status</title><h1>Verifier status</h1>'
                               b'<pre id="status"></pre><script>async function refresh(){'
                               b'const r=await fetch("/v1/status");document.getElementById("status").textContent='
                               b'JSON.stringify(await r.json(),null,2)}refresh();setInterval(refresh,30000)</script>')
                    content_type = "text/html; charset=utf-8"
                self.send_response(200)
            except (OSError, ValueError, TypeError):
                payload, content_type = b'{"error":"status unavailable"}', "application/json"
                self.send_response(503)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    class Server(HTTPServer):
        def get_request(self):
            connection, address = super().get_request()
            connection.settimeout(10)
            return connection, address
    Server(("127.0.0.1", 28236), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", default="/var/lib/zakura-mac-verifier")
    parser.add_argument("--identity", default="/etc/zakura-mac-verifier/dashboard.json")
    args = parser.parse_args()
    serve(args.directory, read_json(args.identity)["verifier_id"])
