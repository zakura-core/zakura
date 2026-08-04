#!/usr/bin/env python3
"""Tests for the Zakura public submit gateway."""

from __future__ import annotations

import importlib.util
import json
import socket
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable


SCRIPT_PATH = Path(__file__).with_name("broadcast.py")
SPEC = importlib.util.spec_from_file_location("zakura_broadcast_gateway", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
gateway = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = gateway
SPEC.loader.exec_module(gateway)


def make_fake_backend_handler(
    responses: dict[str, tuple[int, dict[str, Any]]],
) -> type[BaseHTTPRequestHandler]:
    class FakeBackendHandler(BaseHTTPRequestHandler):
        forwarded_for_values: list[str | None] = []
        post_count = 0

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            if self.path != "/healthz":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "3")
            self.end_headers()
            self.wfile.write(b"ok\n")

        def do_POST(self) -> None:
            type(self).post_count += 1
            type(self).forwarded_for_values.append(self.headers.get("X-Forwarded-For"))
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode())
            method = payload["method"]
            status, body = responses.get(
                method,
                (
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "error": {"code": -32601, "message": "unknown"},
                    },
                ),
            )
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return FakeBackendHandler


def start_fake_backend(
    responses: dict[str, tuple[int, dict[str, Any]]],
) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_fake_backend_handler(responses))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}/"


class LoadBackendsTest(unittest.TestCase):
    def test_loads_backends_toml(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "backends.toml"
            path.write_text(
                '[[backends]]\nname = "a"\nurl = "http://127.0.0.1:8237/"\n',
                encoding="utf-8",
            )
            backends = gateway.load_backends(path)
            self.assertEqual(
                backends,
                [gateway.Backend(name="a", url="http://127.0.0.1:8237/")],
            )


class RateLimiterTest(unittest.TestCase):
    def test_limits_per_client(self) -> None:
        limiter = gateway.RateLimiter(limit=2, window=60.0)
        self.assertTrue(limiter.allow("1.1.1.1", now=100.0))
        self.assertTrue(limiter.allow("1.1.1.1", now=100.1))
        self.assertFalse(limiter.allow("1.1.1.1", now=100.2))
        self.assertTrue(limiter.allow("2.2.2.2", now=100.2))


class InflightLimiterTest(unittest.TestCase):
    def test_total_and_client_caps(self) -> None:
        limiter = gateway.InflightLimiter(total_limit=3, client_limit=2)
        self.assertIsNone(limiter.acquire("a"))
        self.assertIsNone(limiter.acquire("a"))
        self.assertEqual(limiter.acquire("a"), "client")
        self.assertIsNone(limiter.acquire("b"))
        self.assertEqual(limiter.acquire("c"), "total")
        limiter.release("b")
        self.assertIsNone(limiter.acquire("c"))

    def test_release_clears_client_state(self) -> None:
        limiter = gateway.InflightLimiter(total_limit=2, client_limit=1)
        self.assertIsNone(limiter.acquire("a"))
        limiter.release("a")
        self.assertEqual(limiter.per_client, {})
        self.assertEqual(limiter.total, 0)


class BackendPoolTest(unittest.TestCase):
    def test_does_not_retry_an_ambiguous_backend_failure(self) -> None:
        good_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": "abcd" * 16,
        }
        server_good, url_good = start_fake_backend({"sendrawtransaction": (200, good_payload)})
        try:
            pool = gateway.BackendPool(
                [
                    # Reserved port with nothing listening.
                    gateway.Backend("bad", "http://127.0.0.1:1/"),
                    gateway.Backend("good", url_good),
                ],
                timeout=1.0,
                health_interval=60.0,
            )
            # Pretend both are healthy so the unreachable backend is selected.
            with pool.lock:
                pool.healthy = {"bad": True, "good": True}
                pool.next_index = 0
            status, body, name = pool.forward(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "sendrawtransaction",
                    "params": ["00"],
                },
                "203.0.113.9",
            )
            self.assertEqual(status, 502)
            self.assertEqual(name, "")
            self.assertEqual(json.loads(body)["error"]["code"], -32000)
            self.assertEqual(server_good.RequestHandlerClass.post_count, 0)
        finally:
            server_good.shutdown()
            server_good.server_close()

    def test_forwards_the_sanitized_client_ip(self) -> None:
        submit_ok = {"jsonrpc": "2.0", "id": 1, "result": "ab" * 32}
        server, url = start_fake_backend({"sendrawtransaction": (200, submit_ok)})
        try:
            pool = gateway.BackendPool(
                [gateway.Backend("local", url)],
                timeout=1.0,
                health_interval=60.0,
            )
            status, _, name = pool.forward(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "sendrawtransaction",
                    "params": ["00"],
                },
                "203.0.113.9",
            )
            self.assertEqual(status, 200)
            self.assertEqual(name, "local")
            self.assertEqual(
                server.RequestHandlerClass.forwarded_for_values,
                ["203.0.113.9"],
            )
        finally:
            server.shutdown()
            server.server_close()


class ServerLoggingTest(unittest.TestCase):
    def test_handler_errors_redact_client_details_and_tracebacks(self) -> None:
        server = gateway.RedactingThreadingHTTPServer(
            ("127.0.0.1", 0), gateway.SubmitHandler
        )
        try:
            with self.assertLogs(gateway.LOGGER, level="DEBUG") as captured:
                try:
                    raise ConnectionResetError("attacker-controlled detail")
                except ConnectionResetError:
                    server.handle_error(object(), ("203.0.113.9", 12345))
        finally:
            server.server_close()

        output = "\n".join(captured.output)
        self.assertIn("ConnectionResetError", output)
        self.assertNotIn("203.0.113.9", output)
        self.assertNotIn("12345", output)
        self.assertNotIn("attacker-controlled detail", output)


class HandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        submit_ok = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": "ab" * 32,
        }
        self.backend, backend_url = start_fake_backend(
            {"sendrawtransaction": (200, submit_ok)}
        )
        pool = gateway.BackendPool(
            [gateway.Backend("local", backend_url)],
            timeout=2.0,
            health_interval=60.0,
        )
        pool.refresh_health()
        gateway.GATEWAY = pool
        gateway.RATE_LIMITER = gateway.RateLimiter(limit=100, window=60.0)
        gateway.INFLIGHT_LIMITER = gateway.InflightLimiter()
        gateway.BODY_READ_TIMEOUT = gateway.DEFAULT_BODY_READ_TIMEOUT
        gateway.MAX_BODY_BYTES = 1024
        self.server = gateway.RedactingThreadingHTTPServer(
            ("127.0.0.1", 0), gateway.SubmitHandler
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.host = host
        self.port = port
        self.base = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.backend.shutdown()
        self.backend.server_close()
        gateway.GATEWAY = None

    def _post(self, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            self.base + "/",
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                return int(resp.status), json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return int(exc.code), json.loads(exc.read().decode())

    def _raw_request(self, request: bytes) -> bytes:
        with socket.create_connection((self.host, self.port), timeout=3) as sock:
            sock.settimeout(3)
            sock.sendall(request)
            response = b""
            while chunk := sock.recv(4096):
                response += chunk
            return response

    def test_allowlists_sendrawtransaction(self) -> None:
        status, body = self._post(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": gateway.ALLOWED_METHOD,
                "params": ["00"],
            }
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["result"], "ab" * 32)

    def test_rejects_other_methods(self) -> None:
        status, body = self._post(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getblockchaininfo",
                "params": [],
            }
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["error"]["code"], -32601)

    def test_healthz(self) -> None:
        with urllib.request.urlopen(self.base + "/healthz", timeout=3) as resp:
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.read(), b"ok\n")

    def test_rejections_with_unread_bodies_close_the_connection(self) -> None:
        for headers, expected_status in (
            ("Content-Type: text/plain\r\nContent-Length: 4", 415),
            (
                "Content-Type: application/json\r\n"
                "Content-Encoding: gzip\r\nContent-Length: 4",
                400,
            ),
        ):
            with self.subTest(expected_status=expected_status):
                response = self._raw_request(
                    (
                        "POST / HTTP/1.1\r\n"
                        f"Host: {self.host}\r\n"
                        f"{headers}\r\n\r\n"
                        "junk"
                        "GET /healthz HTTP/1.1\r\n"
                        f"Host: {self.host}\r\n\r\n"
                    ).encode()
                )

                self.assertTrue(
                    response.startswith(f"HTTP/1.1 {expected_status} ".encode())
                )
                self.assertIn(b"Connection: close\r\n", response)
                self.assertEqual(response.count(b"HTTP/1.1 "), 1)


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


class SlowBodyTest(unittest.TestCase):
    """Slow and incomplete uploads: in-flight caps and body-read deadlines."""

    def setUp(self) -> None:
        health_ok = {
            "jsonrpc": "2.0",
            "id": "health",
            "result": {"blocks": 1},
        }
        submit_ok = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": "ab" * 32,
        }
        self.backend, backend_url = start_fake_backend(
            {
                "getblockchaininfo": (200, health_ok),
                "sendrawtransaction": (200, submit_ok),
            }
        )
        pool = gateway.BackendPool(
            [gateway.Backend("local", backend_url)],
            timeout=2.0,
            health_interval=60.0,
        )
        pool.refresh_health()
        gateway.GATEWAY = pool
        gateway.RATE_LIMITER = gateway.RateLimiter(limit=100, window=60.0)
        gateway.INFLIGHT_LIMITER = gateway.InflightLimiter(total_limit=8, client_limit=2)
        gateway.BODY_READ_TIMEOUT = 30.0
        gateway.MAX_BODY_BYTES = 1024
        self.server = gateway.RedactingThreadingHTTPServer(
            ("127.0.0.1", 0), gateway.SubmitHandler
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.host, self.port = self.server.server_address
        self.sockets: list[socket.socket] = []

    def tearDown(self) -> None:
        for sock in self.sockets:
            try:
                sock.close()
            except OSError:
                pass
        # Closed uploads unblock their handler threads and free their slots.
        wait_until(lambda: gateway.INFLIGHT_LIMITER.total == 0)
        self.server.shutdown()
        self.backend.shutdown()
        gateway.GATEWAY = None
        gateway.INFLIGHT_LIMITER = gateway.InflightLimiter()
        gateway.BODY_READ_TIMEOUT = gateway.DEFAULT_BODY_READ_TIMEOUT

    def open_incomplete_post(self, forwarded_for: str, length: int = 64) -> socket.socket:
        """Send headers plus one body byte, then withhold the rest."""
        sock = socket.create_connection((self.host, self.port), timeout=5)
        self.sockets.append(sock)
        request = (
            "POST / HTTP/1.1\r\n"
            f"Host: {self.host}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {length}\r\n"
            f"X-Forwarded-For: {forwarded_for}\r\n"
            "\r\n"
            "{"
        )
        sock.sendall(request.encode())
        return sock

    def read_status(self, sock: socket.socket, timeout: float = 5.0) -> int:
        sock.settimeout(timeout)
        data = b""
        while b"\r\n" not in data:
            chunk = sock.recv(1024)
            if not chunk:
                break
            data += chunk
        status_line = data.split(b"\r\n", 1)[0].decode()
        return int(status_line.split(" ")[1])

    def test_rate_window_expiry_does_not_admit_more_slow_uploads(self) -> None:
        # Regression for the slow-body DoS: stalled uploads stop counting
        # against the fixed rate window once it expires, so only the
        # in-flight cap prevents one client from accumulating blocked
        # handler threads across windows.
        gateway.RATE_LIMITER = gateway.RateLimiter(limit=2, window=0.2)
        self.open_incomplete_post("9.9.9.9")
        self.open_incomplete_post("9.9.9.9")
        self.assertTrue(wait_until(lambda: gateway.INFLIGHT_LIMITER.total == 2))
        # Let the rate window expire while both uploads stay blocked; the
        # limiter has forgotten them, so only the in-flight cap now stands
        # between this client and a third blocked body read.
        time.sleep(0.4)
        third = self.open_incomplete_post("9.9.9.9")
        self.assertEqual(self.read_status(third), 429)
        self.assertEqual(gateway.INFLIGHT_LIMITER.total, 2)
        # Other clients are unaffected by the stalled client's slots.
        self.open_incomplete_post("8.8.8.8")
        self.assertTrue(wait_until(lambda: gateway.INFLIGHT_LIMITER.total == 3))

    def test_global_inflight_cap_rejects_when_saturated(self) -> None:
        gateway.INFLIGHT_LIMITER = gateway.InflightLimiter(total_limit=2, client_limit=2)
        self.open_incomplete_post("1.1.1.1")
        self.open_incomplete_post("2.2.2.2")
        self.assertTrue(wait_until(lambda: gateway.INFLIGHT_LIMITER.total == 2))
        rejected = self.open_incomplete_post("3.3.3.3")
        self.assertEqual(self.read_status(rejected), 503)
        self.assertEqual(gateway.INFLIGHT_LIMITER.total, 2)

    def test_stalled_body_read_returns_408_and_frees_slot(self) -> None:
        gateway.BODY_READ_TIMEOUT = 0.4
        sock = self.open_incomplete_post("7.7.7.7")
        self.assertEqual(self.read_status(sock), 408)
        self.assertTrue(wait_until(lambda: gateway.INFLIGHT_LIMITER.total == 0))

    def test_dripped_body_hits_wall_clock_deadline(self) -> None:
        # Feeding bytes faster than any per-recv timeout must not extend
        # the overall body deadline.
        gateway.BODY_READ_TIMEOUT = 0.5
        sock = self.open_incomplete_post("6.6.6.6")

        def drip() -> None:
            try:
                for _ in range(20):
                    sock.sendall(b"x")
                    time.sleep(0.1)
            except OSError:
                pass

        threading.Thread(target=drip, daemon=True).start()
        self.assertEqual(self.read_status(sock), 408)

    def test_complete_upload_succeeds_and_releases_slot(self) -> None:
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendrawtransaction",
                "params": ["00"],
            }
        ).encode()
        sock = socket.create_connection((self.host, self.port), timeout=5)
        self.sockets.append(sock)
        request = (
            "POST / HTTP/1.1\r\n"
            f"Host: {self.host}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            "X-Forwarded-For: 4.4.4.4\r\n"
            "\r\n"
        ).encode() + payload
        sock.sendall(request)
        self.assertEqual(self.read_status(sock), 200)
        self.assertTrue(wait_until(lambda: gateway.INFLIGHT_LIMITER.total == 0))


if __name__ == "__main__":
    unittest.main()
