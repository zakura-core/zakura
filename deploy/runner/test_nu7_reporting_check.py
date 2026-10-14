#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import nu7_reporting_check as check  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
REVISION = "0123456789abcdef0123456789abcdef01234567"
PINNED = "f6f458641c035ac9440d5aa8f78d704733218101"
TIP = 4492960


class FakeNode(BaseHTTPRequestHandler):
    """A public-Testnet node RPC and the public dashboard on one port."""

    build = f"v1.7.0-rc0+g{REVISION[:12]}"
    serves_parameters = True
    dashboard: dict = {}

    def do_GET(self):
        self.reply(self.dashboard)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        method, params = request["method"], request["params"]
        if method == "getinfo":
            result = {"build": self.build}
        elif method == "getblockchaininfo":
            result = {"blocks": TIP}
        elif method == "getnetworkparameters" and self.serves_parameters:
            result = {"effectiveHeight": params[0], "networkMagic": "fa1af9bf"}
        else:
            return self.reply({"result": None, "error": {"code": -32601, "message": "Method not found"}})
        self.reply({"result": result, "error": None})

    def reply(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class Nu7ReportingCheckTest(unittest.TestCase):
    def setUp(self):
        handler = type("Node", (FakeNode,), {})
        handler.dashboard = {
            "selectedProfile": "public-testnet", "generatedAt": 1000.0,
            "network": {"nodeRevision": REVISION}, "status": {"status": "live"},
        }
        self.node = handler
        self.server = HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.tmp = Path(tempfile.mkdtemp())
        self.config = self.tmp / "nodes.ci.toml"
        self.config.write_text(
            '[defaults]\nrpc_listen_addr = "0.0.0.0:%d"\n'
            '[[nodes]]\nname = "zakura-testnet-1"\nssh_string = "root@127.0.0.1"\n'
            '[[nodes]]\nname = "zakura-testnet-eu"\nssh_string = "root@192.0.2.1"\n'
            % self.server.server_port
        )

    def run_nodes(self, now=1030.0):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = check.main(
                ["nodes", "--deployer-config", str(self.config), "--node", "zakura-testnet-1",
                 "--revision", REVISION, "--attempts", "2", "--retry-seconds", "0",
                 "--dashboard-url", f"http://127.0.0.1:{self.server.server_port}/v1/dashboard"],
                now=lambda: now, sleep=lambda _: None)
        return code, output.getvalue()

    def test_checkout_without_the_rpc_fails_and_this_checkout_passes(self):
        methods = self.tmp / check.METHODS_FILE
        methods.parent.mkdir(parents=True)
        methods.write_text('("getblockchaininfo", RpcAccess::Unauthenticated),\n')
        self.assertEqual(len(check.source_problems(self.tmp)), 2)
        self.assertEqual(check.source_problems(REPO), [])

    def test_node_without_the_rpc_fails_like_the_outage(self):
        self.node.serves_parameters = False
        code, output = self.run_nodes()
        self.assertEqual(code, 1)
        self.assertIn("::error::zakura-testnet-1: getnetworkparameters failed: Method not found", output)

    def test_node_running_another_revision_fails(self):
        self.node.build = "v1.7.0-rc0+gd875ccf9239f"
        code, output = self.run_nodes()
        self.assertEqual(code, 1)
        self.assertIn("not revision 012345678", output)

    def test_matching_node_and_fresh_dashboard_pass_quietly(self):
        code, output = self.run_nodes()
        self.assertEqual(code, 0)
        self.assertNotIn("::", output)
        self.assertEqual(check.node_rpc_urls(self.config, None)["zakura-testnet-eu"],
                         f"http://192.0.2.1:{self.server.server_port}/")

    def test_stale_pin_and_frozen_dashboard_are_reported(self):
        self.node.dashboard["network"]["nodeRevision"] = PINNED
        self.node.dashboard["status"]["status"] = "unavailable"
        code, output = self.run_nodes(now=1000.0 + 3600)
        self.assertEqual(code, 0, "the nodes themselves are correct")
        self.assertIn(f"::warning::public NU7 manifest pins nodeRevision {PINNED}", output)
        self.assertIn("::warning::public NU7 dashboard generation is 3600s old", output)
        self.assertIn("::warning::public NU7 dashboard status is 'unavailable'", output)


if __name__ == "__main__":
    unittest.main()
