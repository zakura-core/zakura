#!/usr/bin/env python3
"""Public Zakura JSON-RPC broadcast gateway.

Accepts only `sendrawtransaction`, rate-limits by client IP, caps concurrent
requests globally and per client, bounds body-read time, and load-balances
across healthy Zakura backends. Request bodies are never logged.

Only the Python stdlib is used.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import logging
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ALLOWED_METHOD = "sendrawtransaction"
DEFAULT_RATE_LIMIT = 30
DEFAULT_RATE_WINDOW = 60.0
DEFAULT_RATE_CLIENT_LIMIT = 4_096
DEFAULT_MAX_BODY_BYTES = 1 * 1024 * 1024
DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024
DEFAULT_BACKEND_TIMEOUT = 10.0
DEFAULT_HEALTH_INTERVAL = 15.0
DEFAULT_SOCKET_TIMEOUT = 30.0
DEFAULT_BODY_READ_TIMEOUT = 30.0
DEFAULT_MAX_INFLIGHT_TOTAL = 64
DEFAULT_MAX_INFLIGHT_PER_CLIENT = 8
HEALTH_METHOD = "getblockchaininfo"

LOGGER = logging.getLogger("zakura-broadcast-gateway")


@dataclass(frozen=True)
class Backend:
    name: str
    url: str


class RateLimiter:
    """Per-client fixed-window limiter (same shape as the status dashboard)."""

    def __init__(
        self,
        limit: int = DEFAULT_RATE_LIMIT,
        window: float = DEFAULT_RATE_WINDOW,
        client_limit: int = DEFAULT_RATE_CLIENT_LIMIT,
    ):
        self.limit = limit
        self.window = window
        self.client_limit = client_limit
        self.lock = threading.Lock()
        self.events: dict[str, deque[float]] = {}

    def allow(self, client: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        cutoff = now - self.window
        with self.lock:
            if client not in self.events and len(self.events) >= self.client_limit:
                self.events = {
                    key: events
                    for key, events in self.events.items()
                    if events and events[-1] > cutoff
                }
                if len(self.events) >= self.client_limit:
                    return False

            events = self.events.setdefault(client, deque())
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= self.limit:
                return False
            events.append(now)
            return True


class InflightLimiter:
    """Global and per-client caps on requests currently being served.

    The fixed-window RateLimiter alone cannot stop slow uploads from piling
    up: a request still blocked reading its body stops counting against the
    window once the window expires. An in-flight slot is held for the whole
    request, however slow, so stalled uploads cannot accumulate handler
    threads across rate windows.
    """

    def __init__(
        self,
        total_limit: int = DEFAULT_MAX_INFLIGHT_TOTAL,
        client_limit: int = DEFAULT_MAX_INFLIGHT_PER_CLIENT,
    ):
        self.total_limit = total_limit
        self.client_limit = client_limit
        self.lock = threading.Lock()
        self.total = 0
        # Bounded by total_limit entries: every key holds at least one slot.
        self.per_client: dict[str, int] = {}

    def acquire(self, client: str) -> str | None:
        """Take a slot; returns None on success or the exhausted scope."""
        with self.lock:
            if self.total >= self.total_limit:
                return "total"
            if self.per_client.get(client, 0) >= self.client_limit:
                return "client"
            self.total += 1
            self.per_client[client] = self.per_client.get(client, 0) + 1
            return None

    def release(self, client: str) -> None:
        with self.lock:
            self.total = max(0, self.total - 1)
            remaining = self.per_client.get(client, 0) - 1
            if remaining > 0:
                self.per_client[client] = remaining
            else:
                self.per_client.pop(client, None)


class BackendPool:
    """Round-robin pool that skips backends failing cheap liveness checks."""

    def __init__(
        self,
        backends: list[Backend],
        *,
        timeout: float = DEFAULT_BACKEND_TIMEOUT,
        health_interval: float = DEFAULT_HEALTH_INTERVAL,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    ):
        if not backends:
            raise ValueError("at least one backend is required")
        self.backends = backends
        self.timeout = timeout
        self.health_interval = health_interval
        self.max_response_bytes = max_response_bytes
        self.lock = threading.Lock()
        self.next_index = 0
        self.healthy = {backend.name: True for backend in backends}
        self._stop = threading.Event()
        self._health_thread: threading.Thread | None = None

    def start(self) -> None:
        self.refresh_health()
        self._health_thread = threading.Thread(
            target=self._health_loop,
            name="broadcast-gateway-health",
            daemon=True,
        )
        self._health_thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._health_thread is not None:
            self._health_thread.join(timeout=2.0)

    def _health_loop(self) -> None:
        while not self._stop.wait(self.health_interval):
            try:
                self.refresh_health()
            except Exception:
                LOGGER.exception("backend health refresh failed")

    def refresh_health(self) -> None:
        for backend in self.backends:
            ok = self._probe(backend)
            with self.lock:
                previous = self.healthy.get(backend.name, True)
                self.healthy[backend.name] = ok
            if ok != previous:
                LOGGER.info(
                    "backend health changed name=%s healthy=%s",
                    backend.name,
                    ok,
                )

    def _probe(self, backend: Backend) -> bool:
        payload = {
            "jsonrpc": "2.0",
            "id": "health",
            "method": HEALTH_METHOD,
            "params": [],
        }
        try:
            status, body = self._post(backend, payload)
        except Exception:
            return False
        if status != 200:
            return False
        try:
            decoded = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False
        return isinstance(decoded, dict) and "result" in decoded and "error" not in decoded

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "backends": [
                    {
                        "name": backend.name,
                        "url": backend.url,
                        "healthy": self.healthy.get(backend.name, False),
                    }
                    for backend in self.backends
                ],
                "healthy_count": sum(
                    1 for backend in self.backends if self.healthy.get(backend.name, False)
                ),
            }

    def _ordered_candidates(self) -> list[Backend]:
        with self.lock:
            start = self.next_index % len(self.backends)
            self.next_index = (start + 1) % len(self.backends)
            ordered = self.backends[start:] + self.backends[:start]
            healthy = [b for b in ordered if self.healthy.get(b.name, False)]
            if healthy:
                return healthy
            # All marked unhealthy: still try everyone once so the gateway can recover.
            return ordered

    def mark_unhealthy(self, backend: Backend) -> None:
        with self.lock:
            self.healthy[backend.name] = False

    def forward(self, payload: dict[str, Any]) -> tuple[int, bytes, str]:
        errors: list[str] = []
        for backend in self._ordered_candidates():
            try:
                status, body = self._post(backend, payload)
            except Exception as exc:
                self.mark_unhealthy(backend)
                errors.append(f"{backend.name}: {exc}")
                LOGGER.warning(
                    "backend request failed name=%s error=%s",
                    backend.name,
                    type(exc).__name__,
                )
                continue
            if status >= 500:
                self.mark_unhealthy(backend)
                errors.append(f"{backend.name}: HTTP {status}")
                continue
            return status, body, backend.name
        detail = "; ".join(errors) if errors else "no backends available"
        body = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": payload.get("id"),
                "error": {
                    "code": -32000,
                    "message": f"All submit backends unavailable: {detail}",
                },
            },
            separators=(",", ":"),
        ).encode()
        return 502, body, ""

    def _post(self, backend: Backend, payload: dict[str, Any]) -> tuple[int, bytes]:
        data = json.dumps(payload, separators=(",", ":")).encode()
        request = urllib.request.Request(
            backend.url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "zakura-broadcast-gateway/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read(self.max_response_bytes + 1)
                if len(body) > self.max_response_bytes:
                    raise ValueError("backend response exceeds size limit")
                return int(response.status), body
        except urllib.error.HTTPError as exc:
            body = exc.read(self.max_response_bytes + 1)
            if len(body) > self.max_response_bytes:
                raise ValueError("backend response exceeds size limit") from exc
            return int(exc.code), body


GATEWAY: BackendPool | None = None
RATE_LIMITER = RateLimiter()
INFLIGHT_LIMITER = InflightLimiter()
MAX_BODY_BYTES = DEFAULT_MAX_BODY_BYTES
RATE_WINDOW = DEFAULT_RATE_WINDOW
BODY_READ_TIMEOUT = DEFAULT_BODY_READ_TIMEOUT


def load_backends(path: Path) -> list[Backend]:
    with path.open("rb") as handle:
        data = tomllib.load(handle)
    backends = data.get("backends")
    if not isinstance(backends, list) or not backends:
        raise ValueError(f"{path}: expected non-empty [[backends]] list")
    loaded: list[Backend] = []
    for index, entry in enumerate(backends):
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: backends[{index}] must be a table")
        name = entry.get("name")
        url = entry.get("url")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"{path}: backends[{index}].name is required")
        if not isinstance(url, str) or not url.strip():
            raise ValueError(f"{path}: backends[{index}].url is required")
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"{path}: backends[{index}].url must be http(s)")
        loaded.append(Backend(name=name.strip(), url=url.strip()))
    return loaded


def jsonrpc_error(req_id: Any, code: int, message: str) -> bytes:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": code, "message": message},
        },
        separators=(",", ":"),
    ).encode()


class SubmitHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "zakura-broadcast-gateway/1.0"
    # Per-recv socket timeout: a peer that stops sending is disconnected
    # instead of pinning this handler thread forever.
    timeout = DEFAULT_SOCKET_TIMEOUT

    def log_message(self, fmt: str, *args: Any) -> None:
        # Replace BaseHTTPRequestHandler's stderr logger; never include bodies.
        LOGGER.info("http client=%s %s", self.rate_limit_client(), fmt % args)

    def rate_limit_client(self) -> str:
        peer = self.client_address[0]
        try:
            peer_address = ipaddress.ip_address(peer)
        except ValueError:
            return peer

        # headers is unset when logging a timeout that fired before a
        # request line was ever parsed on this connection.
        headers = getattr(self, "headers", None)
        forwarded_for = headers.get("X-Forwarded-For") if headers is not None else None
        if peer_address.is_loopback and forwarded_for:
            candidate = forwarded_for.rsplit(",", 1)[-1].strip()
            try:
                return str(ipaddress.ip_address(candidate))
            except ValueError:
                pass
        return str(peer_address)

    def send_bytes(
        self,
        status: int,
        body: bytes,
        content_type: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/healthz":
            assert GATEWAY is not None
            snap = GATEWAY.snapshot()
            if snap["healthy_count"] <= 0:
                return self.send_bytes(
                    503,
                    b"no healthy backends\n",
                    "text/plain; charset=utf-8",
                )
            return self.send_bytes(200, b"ok\n", "text/plain; charset=utf-8")
        if parsed.path == "/backends":
            assert GATEWAY is not None
            body = json.dumps(GATEWAY.snapshot(), separators=(",", ":")).encode()
            return self.send_bytes(200, body, "application/json; charset=utf-8")
        return self.send_bytes(404, b"not found\n", "text/plain; charset=utf-8")

    def do_HEAD(self) -> None:
        self.do_GET()

    def read_body_with_deadline(self, length: int) -> bytes | None:
        """Read exactly `length` body bytes within BODY_READ_TIMEOUT seconds.

        Enforces a wall-clock deadline rather than a per-recv timeout, so a
        client dripping one byte per timeout cannot hold this thread open
        indefinitely. Raises TimeoutError when the deadline expires; returns
        None if the client disconnects first. Either way the body was not
        consumed, so the caller must close the connection.
        """
        deadline = time.monotonic() + BODY_READ_TIMEOUT
        chunks: list[bytes] = []
        outstanding = length
        try:
            while outstanding > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("body read deadline exceeded")
                self.connection.settimeout(remaining)
                # read1 issues at most one recv, so the deadline is
                # re-checked at least once per received chunk.
                chunk = self.rfile.read1(min(outstanding, 65536))
                if not chunk:
                    return None
                chunks.append(chunk)
                outstanding -= len(chunk)
        finally:
            self.connection.settimeout(self.timeout)
        return b"".join(chunks)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path not in {"", "/"}:
            # The body was not read; close so leftover bytes cannot desync
            # a reused keep-alive connection.
            return self.send_bytes(
                404,
                b"not found\n",
                "text/plain; charset=utf-8",
                {"Connection": "close"},
            )

        client = self.rate_limit_client()
        exhausted = INFLIGHT_LIMITER.acquire(client)
        if exhausted == "client":
            body = jsonrpc_error(None, -32029, "Too many concurrent requests")
            return self.send_bytes(
                429,
                body,
                "application/json; charset=utf-8",
                {"Retry-After": "1", "Connection": "close"},
            )
        if exhausted is not None:
            body = jsonrpc_error(None, -32000, "Gateway is at capacity")
            return self.send_bytes(
                503,
                body,
                "application/json; charset=utf-8",
                {"Retry-After": "1", "Connection": "close"},
            )
        try:
            self.handle_submit(client)
        finally:
            INFLIGHT_LIMITER.release(client)

    def handle_submit(self, client: str) -> None:
        if not RATE_LIMITER.allow(client):
            body = jsonrpc_error(None, -32029, "Request rate limit exceeded")
            return self.send_bytes(
                429,
                body,
                "application/json; charset=utf-8",
                {"Retry-After": str(int(RATE_WINDOW)), "Connection": "close"},
            )

        length_header = self.headers.get("Content-Length")
        if length_header is None:
            body = jsonrpc_error(None, -32700, "Content-Length required")
            return self.send_bytes(
                411, body, "application/json; charset=utf-8", {"Connection": "close"}
            )
        try:
            length = int(length_header)
        except ValueError:
            body = jsonrpc_error(None, -32700, "Invalid Content-Length")
            return self.send_bytes(
                400, body, "application/json; charset=utf-8", {"Connection": "close"}
            )
        if length < 0 or length > MAX_BODY_BYTES:
            body = jsonrpc_error(None, -32600, "Request body too large")
            return self.send_bytes(
                413, body, "application/json; charset=utf-8", {"Connection": "close"}
            )

        # Read exactly the declared body; do not retain it beyond this request.
        try:
            raw = self.read_body_with_deadline(length)
        except TimeoutError:
            body = jsonrpc_error(None, -32000, "Timed out reading request body")
            return self.send_bytes(
                408, body, "application/json; charset=utf-8", {"Connection": "close"}
            )
        if raw is None:
            # Client disconnected mid-upload; there is nobody to answer.
            self.close_connection = True
            return
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = jsonrpc_error(None, -32700, "Parse error")
            return self.send_bytes(400, body, "application/json; charset=utf-8")
        finally:
            del raw

        if not isinstance(payload, dict):
            body = jsonrpc_error(None, -32600, "Invalid Request")
            return self.send_bytes(400, body, "application/json; charset=utf-8")

        req_id = payload.get("id")
        method = payload.get("method")
        if method != ALLOWED_METHOD:
            LOGGER.info(
                "rejected method client=%s method=%r",
                client,
                method if isinstance(method, str) else type(method).__name__,
            )
            body = jsonrpc_error(req_id, -32601, "Method not allowed")
            return self.send_bytes(200, body, "application/json; charset=utf-8")

        params = payload.get("params")
        if not isinstance(params, list) or len(params) < 1 or not isinstance(params[0], str):
            body = jsonrpc_error(
                req_id,
                -32602,
                "Invalid params: expected [hex_tx, ...]",
            )
            return self.send_bytes(200, body, "application/json; charset=utf-8")

        assert GATEWAY is not None
        started = time.monotonic()
        status, body, backend_name = GATEWAY.forward(payload)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        LOGGER.info(
            "submit client=%s backend=%s status=%s bytes=%s elapsed_ms=%s",
            client,
            backend_name or "-",
            status,
            len(body),
            elapsed_ms,
        )
        return self.send_bytes(status, body, "application/json; charset=utf-8")


def main() -> None:
    global GATEWAY, RATE_LIMITER, INFLIGHT_LIMITER, MAX_BODY_BYTES, RATE_WINDOW
    global BODY_READ_TIMEOUT

    parser = argparse.ArgumentParser(description="Serve a Zakura broadcast-only JSON-RPC gateway.")
    parser.add_argument("--backends", required=True, type=Path, help="path to backends TOML")
    parser.add_argument("--host", default="127.0.0.1", help="bind host")
    parser.add_argument("--port", default=8092, type=int, help="bind port")
    parser.add_argument("--rate-limit", default=DEFAULT_RATE_LIMIT, type=int)
    parser.add_argument("--rate-window", default=DEFAULT_RATE_WINDOW, type=float)
    parser.add_argument("--max-body-bytes", default=DEFAULT_MAX_BODY_BYTES, type=int)
    parser.add_argument("--backend-timeout", default=DEFAULT_BACKEND_TIMEOUT, type=float)
    parser.add_argument("--health-interval", default=DEFAULT_HEALTH_INTERVAL, type=float)
    parser.add_argument(
        "--body-read-timeout",
        default=DEFAULT_BODY_READ_TIMEOUT,
        type=float,
        help="wall-clock seconds allowed to upload a request body",
    )
    parser.add_argument(
        "--max-inflight",
        default=DEFAULT_MAX_INFLIGHT_TOTAL,
        type=int,
        help="total concurrent requests served at once",
    )
    parser.add_argument(
        "--max-inflight-per-client",
        default=DEFAULT_MAX_INFLIGHT_PER_CLIENT,
        type=int,
        help="concurrent requests served at once per client IP",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    MAX_BODY_BYTES = args.max_body_bytes
    RATE_WINDOW = args.rate_window
    BODY_READ_TIMEOUT = args.body_read_timeout
    RATE_LIMITER = RateLimiter(limit=args.rate_limit, window=args.rate_window)
    INFLIGHT_LIMITER = InflightLimiter(
        total_limit=args.max_inflight,
        client_limit=args.max_inflight_per_client,
    )
    backends = load_backends(args.backends)
    GATEWAY = BackendPool(
        backends,
        timeout=args.backend_timeout,
        health_interval=args.health_interval,
    )
    GATEWAY.start()

    server = ThreadingHTTPServer((args.host, args.port), SubmitHandler)
    LOGGER.info(
        "listening on http://%s:%s backends=%s rate=%s/%ss inflight=%s/%s body_timeout=%ss",
        args.host,
        args.port,
        len(backends),
        args.rate_limit,
        args.rate_window,
        args.max_inflight,
        args.max_inflight_per_client,
        args.body_read_timeout,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        GATEWAY.stop()
        server.server_close()


if __name__ == "__main__":
    main()
