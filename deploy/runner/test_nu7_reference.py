"""Real HTTP coverage for the private, cookie-authenticated reference bridge."""
import base64
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

from nu7_reference import ReferenceServer


class UpstreamHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.calls.append((request, self.headers.get("Authorization")))
        body = json.dumps({"result": {"chain": "main"}, "error": None, "id": 1}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ReferenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.cookie = Path(self.directory.name) / "cookie"
        self.cookie.write_text("test:cookie-one")
        self.upstream = HTTPServer(("127.0.0.1", 0), UpstreamHandler)
        self.upstream.calls = []
        self.server = ReferenceServer(("127.0.0.1", 0),
            upstream="http://127.0.0.1:%s/" % self.upstream.server_port,
            cookie=self.cookie, collector="192.0.2.1")
        self.threads = [threading.Thread(target=s.serve_forever, kwargs={"poll_interval": 0.01})
                        for s in (self.upstream, self.server)]
        for thread in self.threads:
            thread.start()
        self.url = "http://127.0.0.1:%s/" % self.server.server_port

    def tearDown(self):
        for server in (self.server, self.upstream):
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)
        self.directory.cleanup()

    def request(self, method="getblockchaininfo", params=None, body=None):
        data = body if body is not None else json.dumps({"method": method, "params": params or []}).encode()
        request = urllib.request.Request(self.url, data, {"Content-Type": "application/json"})
        return urllib.request.urlopen(request, timeout=2)

    def test_only_chain_reads_reach_upstream_and_cookie_rotation_is_used(self):
        for method, params in (("getinfo", []), ("getblockchaininfo", []), ("getblockhash", [3540000])):
            with self.request(method, params) as response:
                self.assertEqual(json.load(response)["result"]["chain"], "main")
        expected = "Basic " + base64.b64encode(b"test:cookie-one").decode()
        self.assertEqual(self.upstream.calls[-1][1], expected)
        self.cookie.write_text("test:cookie-two")
        with self.request() as response:
            self.assertNotIn(b"cookie", response.read())
        self.assertNotEqual(self.upstream.calls[-1][1], expected)

    def test_invalid_methods_batches_parameters_and_oversized_bodies_are_rejected(self):
        for body in (b'{"method":"stop"}', b'[]', b'{"method":"getblockhash","params":[true]}',
                     b'{"method":"getblockhash","params":[-1]}', b'x' * 4097):
            with self.subTest(body=body[:60]), self.assertRaises(urllib.error.HTTPError) as result:
                self.request(body=body)
            self.assertEqual(result.exception.code, 400)
            result.exception.close()
        self.assertEqual(self.upstream.calls, [])

    def test_unapproved_peer_is_rejected_before_forwarding(self):
        self.server.allowed.clear()
        with self.assertRaises(urllib.error.HTTPError) as result:
            self.request()
        self.assertEqual(result.exception.code, 403)
        result.exception.close()
        self.assertEqual(self.upstream.calls, [])

    def test_cookie_failure_and_upstream_timeout_do_not_expose_authentication(self):
        self.cookie.unlink()
        with self.assertRaises(urllib.error.HTTPError) as result:
            self.request()
        self.assertEqual(result.exception.code, 502)
        self.assertNotIn(b"cookie", result.exception.read())
        result.exception.close()
        self.cookie.write_text("test:cookie-one")
        with patch("nu7_reference.read_json", side_effect=TimeoutError("private details")):
            with self.assertRaises(urllib.error.HTTPError) as result:
                self.request()
            self.assertEqual(result.exception.code, 502)
            self.assertNotIn(b"private details", result.exception.read())
            result.exception.close()


if __name__ == "__main__":
    unittest.main()
