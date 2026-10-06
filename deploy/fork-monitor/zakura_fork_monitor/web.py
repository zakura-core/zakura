"""Dashboard page and JSON API on a stdlib `ThreadingHTTPServer`.

Routes (GET and HEAD): `/` (page.html), `/healthz`, and under `/api/`:
`snapshot?full=`, `summary`, `forks?limit=&since=`, `orphans/stats`,
`miners?since=`, `sawtooth?n=`, `resets?limit=&since=`, `propagation?since=`,
`probes?since=`, `peers`, `crosscheck`. Response shapes are the `analysis`
function results (see their docstrings). Unknown paths are 404; unknown,
repeated or malformed parameters are 400 (`{"error": code, "message": text}`);
numeric parameters are clamped into range. `since` is unix seconds, or a
negative number of seconds before now (`since=-86400` is "the last 24 h"),
which also lets repeated dashboard polls share one cache entry.

Threading: handler threads never touch the `Chain`. Chain-based routes run on
the monitor's event loop (`monitor.loop`, via `call_soon_threadsafe`) with a
bounded number of queued jobs and a timeout; when the monitor has no running
loop (tests, one-shot CLI use) they run inline under a lock. Store-only work
(propagation, probe counts) runs on the handler thread through its own
`Store.reader()`. `/api/snapshot` and `/api/peers` serve `monitor.snapshot`,
the dict the loop last published (replaced wholesale, never mutated). Results
are cached per (route, normalized parameters) for a few seconds and computed
once per key under concurrent requests.

The page's Content-Security-Policy allows only the page's own inline `<script>`
and `<style>` blocks (by SHA-256), same-origin fetches and `data:` images.

Monitor contract: `snapshot` (dict | None), `chain`, `store` (may be None),
`loop` (the asyncio loop that owns the chain; service.py must set it), and
optionally `p2p`, `config` and `rpc_status()` as `analysis.live_snapshot` uses.

Notes:
- `/api/snapshot` leaves out the per-source `peers` list (served by
  `/api/peers`) unless `?full=1`, and adds `peer_count`, `served_at` and
  `old_rules` = {"peers": peers in state "old-rules", "fork_height": the
  lowest fork height among them | None}.
- List results are wrapped in objects: `/api/forks` -> `{"forks": [...]}`,
  `/api/resets` -> `{"resets": [...]}`, `/api/peers` -> `{"generated_at", "peers"}`.
- `since` also takes relative (negative) values; `/api/resets` accepts `since`.
- `/healthz` returns JSON and is 503 until the first snapshot or when the
  snapshot is older than SNAPSHOT_STALE_AFTER.
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import hashlib
import json
import logging
import math
import re
import socket
import threading
import time
import urllib.parse
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from . import __version__, analysis

LOG = logging.getLogger(__name__)

PAGE_FILE = Path(__file__).with_name("page.html")
# Seconds a handler waits for its job on the event loop before answering 503.
LOOP_TIMEOUT = 15.0
# Handler jobs queued on the event loop at once; more wait (then 503) so the loop keeps collecting.
MAX_LOOP_JOBS = 4
# Distinct (route, params) results kept; the page uses a handful of keys.
CACHE_ENTRIES = 256
# The loop refreshes the snapshot every 2 s; this much older means it is stalled.
SNAPSHOT_STALE_AFTER = 30.0
# Socket timeout per connection, so a slow client cannot hold a handler thread forever.
REQUEST_TIMEOUT = 20.0
MAX_QUERY_LENGTH = 1_024
MAX_QUERY_PARAMS = 8
# Relative `since` values reach back at most this far (longer than any retention window).
MAX_LOOKBACK = 400 * 86_400.0
MAX_TIMESTAMP = 4_102_444_800.0  # 2100-01-01
DEFAULT_FORKS = 50
DEFAULT_RESETS = 100
DEFAULT_SAWTOOTH = 1_500
# Peer view state of a peer whose headers or blocks fail our consensus rules (e.g. pre-NU7 nodes).
OLD_RULES_STATE = "old-rules"

JSON_TYPE = "application/json; charset=utf-8"
HTML_TYPE = "text/html; charset=utf-8"
API_POLICY = "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
COMMON_HEADERS = (
    ("Cache-Control", "no-store"),
    ("X-Content-Type-Options", "nosniff"),
    ("Referrer-Policy", "no-referrer"),
    ("X-Frame-Options", "DENY"),
    ("Cross-Origin-Opener-Policy", "same-origin"),
    ("Cross-Origin-Resource-Policy", "same-origin"),
)

_INT_RE = re.compile(r"-?[0-9]{1,12}")
_NUMBER_RE = re.compile(r"-?[0-9]{1,12}(?:\.[0-9]{1,6})?")
_FLAGS = {"1": True, "true": True, "yes": True, "0": False, "false": False, "no": False}
_INLINE_RE = {tag: re.compile(rf"<{tag}>(.*?)</{tag}>", re.S) for tag in ("script", "style")}


class HttpError(Exception):
    """An error answered as JSON `{"error": code, "message": message}` with an HTTP status."""

    def __init__(self, status: int, code: str, message: str) -> None:
        """Keep the status, a stable error code and a human-readable message."""
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


@dataclass(frozen=True, slots=True)
class Response:
    """One HTTP response: status, body, content type, CSP and extra headers."""

    status: int
    body: bytes
    content_type: str = JSON_TYPE
    policy: str = API_POLICY
    headers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class Page:
    """The dashboard HTML and the Content-Security-Policy that admits its inline code."""

    body: bytes
    policy: str

    @classmethod
    def load(cls, path: Path = PAGE_FILE) -> Page:
        """Read page.html and derive its CSP from the SHA-256 of each inline script and style."""
        html = path.read_text(encoding="utf-8")
        return cls(html.encode("utf-8"), page_policy(html))


def page_policy(html: str) -> str:
    """Return a CSP allowing exactly the attribute-less inline `<script>`/`<style>` blocks of `html`."""

    def sources(tag: str) -> str:
        """Hash sources for every inline block of `tag`, or 'none'."""
        hashes = [
            "'sha256-" + base64.b64encode(hashlib.sha256(block.encode("utf-8")).digest()).decode() + "'"
            for block in _INLINE_RE[tag].findall(html)
        ]
        return " ".join(hashes) or "'none'"

    return "; ".join((
        "default-src 'none'",
        f"script-src {sources('script')}",
        f"style-src {sources('style')}",
        "img-src data:",
        "connect-src 'self'",
        "base-uri 'none'",
        "form-action 'none'",
        "frame-ancestors 'none'",
    ))


# -- parameters ------------------------------------------------------------------------------

# (parameter name, raw value or None when absent) -> parsed value; raises HttpError(400).
Parser = Callable[[str, str | None], Any]


def _int_param(low: int, high: int, default: int) -> Parser:
    """Return a parser for a decimal integer clamped into [low, high]."""

    def parse(name: str, text: str | None) -> int:
        """Parse `text` (None -> the default)."""
        if text is None:
            return default
        if not _INT_RE.fullmatch(text):
            raise HttpError(400, "bad_request", f"{name} must be an integer")
        return max(low, min(high, int(text)))

    return parse


def _since_param(name: str, text: str | None) -> float | None:
    """Parse a `since`: unix seconds, or negative seconds relative to now (clamped either way)."""
    if text is None:
        return None
    if not _NUMBER_RE.fullmatch(text):
        raise HttpError(400, "bad_request", f"{name} must be unix seconds or a negative number of seconds")
    value = float(text)
    return max(-MAX_LOOKBACK, value) if value < 0 else min(MAX_TIMESTAMP, value)


def _flag_param(name: str, text: str | None) -> bool:
    """Parse a boolean flag (1/0, true/false, yes/no; absent is false)."""
    if text is None:
        return False
    try:
        return _FLAGS[text.lower()]
    except KeyError:
        raise HttpError(400, "bad_request", f"{name} must be 1 or 0") from None


def _resolve_since(since: float | None, now: float) -> float | None:
    """Turn a relative (negative) `since` into unix seconds."""
    return now + since if since is not None and since < 0 else since


@dataclass(frozen=True, slots=True)
class _Route:
    """An API route: cache TTL (0 = uncached), accepted parameters and the handler."""

    ttl: float
    compute: Callable[[WebApp, dict[str, Any]], Any]
    params: Mapping[str, Parser] = field(default_factory=dict)


# -- the application -------------------------------------------------------------------------


class WebApp:
    """Routes, parameter validation, the response cache and event-loop hops for one monitor."""

    def __init__(self, monitor: Any, *, page: Page | None = None, clock: Callable[[], float] = time.time) -> None:
        """Bind to `monitor`; `page` defaults to page.html next to this module."""
        self.monitor = monitor
        self.page = page if page is not None else Page.load()
        self.clock = clock
        self._cache: OrderedDict[tuple[Any, ...], tuple[float, bytes]] = OrderedDict()
        self._cache_lock = threading.Lock()
        self._inflight: dict[tuple[Any, ...], threading.Lock] = {}
        self._loop_slots = threading.BoundedSemaphore(MAX_LOOP_JOBS)
        self._inline_lock = threading.Lock()

    def handle(self, target: str) -> Response:
        """Answer a GET for request target `target` (path plus query)."""
        try:
            try:
                parts = urllib.parse.urlsplit(target)
            except ValueError:  # e.g. "http://[x/": an unterminated IPv6 host
                raise HttpError(400, "bad_request", "malformed request target") from None
            if parts.path == "/":
                return Response(200, self.page.body, HTML_TYPE, self.page.policy)
            if parts.path == "/healthz":
                return self._health()
            route = ROUTES.get(parts.path)
            if route is None:
                raise HttpError(404, "not_found", "no such route")
            params = self._params(route, parts.query)
            key = (parts.path, *sorted(params.items()))
            return Response(200, self._cached(key, route.ttl, lambda: _encode(route.compute(self, params))))
        except HttpError as err:
            return _error(err)
        except Exception:
            LOG.exception("request %.200r failed", target)
            return _error(HttpError(500, "internal", "internal error; see the monitor log"))

    def _params(self, route: _Route, query: str) -> dict[str, Any]:
        """Validate the query string against `route` and return every parameter's parsed value."""
        if len(query) > MAX_QUERY_LENGTH:
            raise HttpError(400, "bad_request", "query string too long")
        try:
            pairs = urllib.parse.parse_qsl(
                query, keep_blank_values=True, strict_parsing=True, max_num_fields=MAX_QUERY_PARAMS
            ) if query else []
        except ValueError:
            raise HttpError(400, "bad_request", "malformed query string") from None
        given: dict[str, str] = {}
        for name, value in pairs:
            if name not in route.params:
                raise HttpError(400, "bad_request", f"unknown parameter {name[:40]!r}")
            if name in given:
                raise HttpError(400, "bad_request", f"parameter {name!r} given twice")
            given[name] = value
        return {name: parse(name, given.get(name)) for name, parse in route.params.items()}

    def _cached(self, key: tuple[Any, ...], ttl: float, compute: Callable[[], bytes]) -> bytes:
        """Return the cached body for `key`, computing it at most once at a time when stale."""
        if ttl <= 0:
            return compute()
        with self._cache_lock:
            hit = self._fresh(key)
            if hit is not None:
                return hit
            lock = self._inflight.setdefault(key, threading.Lock())
        with lock:
            with self._cache_lock:
                hit = self._fresh(key)
            if hit is not None:
                return hit
            try:
                body = compute()
            finally:
                with self._cache_lock:
                    self._inflight.pop(key, None)
            with self._cache_lock:
                self._cache[key] = (time.monotonic() + ttl, body)
                self._cache.move_to_end(key)
                while len(self._cache) > CACHE_ENTRIES:
                    self._cache.popitem(last=False)
            return body

    def _fresh(self, key: tuple[Any, ...]) -> bytes | None:
        """Return an unexpired cache entry (caller holds the cache lock)."""
        entry = self._cache.get(key)
        if entry is None or entry[0] <= time.monotonic():
            return None
        self._cache.move_to_end(key)
        return entry[1]

    def on_loop(self, fn: Callable[[], Any]) -> Any:
        """Run `fn` on the monitor's event loop and return its result (inline without a running loop)."""
        loop = getattr(self.monitor, "loop", None)
        if loop is None or loop.is_closed() or not loop.is_running():
            with self._inline_lock:
                return fn()
        try:
            if asyncio.get_running_loop() is loop:
                return fn()
        except RuntimeError:
            pass  # not on any loop: the normal handler-thread case
        if not self._loop_slots.acquire(timeout=LOOP_TIMEOUT):
            raise HttpError(503, "busy", "too many requests are waiting for the monitor")
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()

        def run() -> None:
            """Execute `fn` on the loop thread unless the waiting handler gave up."""
            if not future.set_running_or_notify_cancel():
                return
            try:
                future.set_result(fn())
            except BaseException as err:  # re-raised in the handler thread
                future.set_exception(err)

        try:
            loop.call_soon_threadsafe(run)
            return future.result(timeout=LOOP_TIMEOUT)
        except TimeoutError:
            future.cancel()
            raise HttpError(503, "busy", "the monitor did not answer in time") from None
        except RuntimeError as err:
            if future.done():
                raise  # fn itself raised RuntimeError
            raise HttpError(503, "stopping", "the monitor is shutting down") from err
        finally:
            self._loop_slots.release()

    def chain(self) -> Any:
        """Return the monitor's block tree (only touch it inside `on_loop`)."""
        return self.monitor.chain

    def conn(self) -> Any:
        """Return this thread's read-only store connection, or None without a store."""
        store = getattr(self.monitor, "store", None)
        return store.reader() if store is not None else None

    def require_conn(self) -> Any:
        """Return this thread's store connection, or raise 503 when the monitor has no store."""
        conn = self.conn()
        if conn is None:
            raise HttpError(503, "no_store", "the monitor has no database")
        return conn

    def snapshot(self) -> Mapping[str, Any] | None:
        """Return the live snapshot the loop last published, or None before the first one."""
        snap = getattr(self.monitor, "snapshot", None)
        return snap if isinstance(snap, Mapping) else None

    def _health(self) -> Response:
        """Report whether the loop publishes fresh snapshots (200) or not (503)."""
        now = self.clock()
        snap = self.snapshot()
        generated = snap.get("generated_at") if snap is not None else None
        age = now - generated if isinstance(generated, (int, float)) and math.isfinite(generated) else None
        tip = snap.get("tip") if snap is not None else None
        status = "starting" if age is None else "ok" if age <= SNAPSHOT_STALE_AFTER else "stale"
        body = {
            "status": status,
            "version": __version__,
            "snapshot_age_s": round(age, 3) if age is not None else None,
            "tip_height": tip.get("height") if isinstance(tip, Mapping) else None,
        }
        return Response(200 if status == "ok" else 503, _encode(body))


# -- route handlers ---------------------------------------------------------------------------


def _api_snapshot(app: WebApp, params: dict[str, Any]) -> dict[str, Any]:
    """The live snapshot, without the per-source list unless `full`."""
    snap = app.snapshot()
    if snap is None:
        raise HttpError(503, "starting", "the monitor has not published a snapshot yet")
    out = dict(snap)
    peers = out.get("peers")
    peers = peers if isinstance(peers, list) else []
    out["peer_count"] = len(peers)
    out["old_rules"] = _old_rules(peers)
    if not params["full"]:
        out.pop("peers", None)
    out["served_at"] = app.clock()
    return out


def _old_rules(peers: list[Any]) -> dict[str, Any]:
    """Count peer views in state "old-rules" and return the lowest fork height their relations give."""
    count, heights = 0, []
    for view in peers:
        if not isinstance(view, Mapping) or view.get("state") != OLD_RULES_STATE:
            continue
        count += 1
        relation = view.get("relation")
        height = relation.get("fork_height") if isinstance(relation, Mapping) else None
        if isinstance(height, int) and not isinstance(height, bool):
            heights.append(height)
    return {"peers": count, "fork_height": min(heights, default=None)}


def _api_summary(app: WebApp, params: dict[str, Any]) -> Any:
    """`analysis.summary` (loop)."""
    now = app.clock()
    config = getattr(app.monitor, "config", None)
    return app.on_loop(lambda: analysis.summary(app.chain(), app.conn(), now, config=config))


def _api_forks(app: WebApp, params: dict[str, Any]) -> dict[str, Any]:
    """`analysis.fork_events` wrapped as {"limit", "since", "forks"} (loop)."""
    since = _resolve_since(params["since"], app.clock())
    forks = app.on_loop(lambda: analysis.fork_events(app.chain(), app.conn(), params["limit"], since))
    return {"limit": params["limit"], "since": since, "forks": forks}


def _api_orphan_stats(app: WebApp, params: dict[str, Any]) -> Any:
    """`analysis.orphan_stats` (loop)."""
    now = app.clock()
    return app.on_loop(lambda: analysis.orphan_stats(app.chain(), app.conn(), now=now))


def _api_miners(app: WebApp, params: dict[str, Any]) -> Any:
    """`analysis.miner_stats` (loop)."""
    since = _resolve_since(params["since"], app.clock())
    return app.on_loop(lambda: analysis.miner_stats(app.chain(), app.conn(), since))


def _api_sawtooth(app: WebApp, params: dict[str, Any]) -> Any:
    """`analysis.sawtooth` (loop)."""
    return app.on_loop(lambda: analysis.sawtooth(app.chain(), params["n"]))


def _api_resets(app: WebApp, params: dict[str, Any]) -> dict[str, Any]:
    """`analysis.resets` wrapped as {"limit", "since", "resets"} (loop)."""
    since = _resolve_since(params["since"], app.clock())
    resets = app.on_loop(lambda: analysis.resets(app.chain(), params["limit"], since))
    return {"limit": params["limit"], "since": since, "resets": resets}


def _api_propagation(app: WebApp, params: dict[str, Any]) -> Any:
    """`analysis.propagation` on this thread's reader (store only)."""
    now = app.clock()
    return analysis.propagation(app.require_conn(), _resolve_since(params["since"], now), now=now)


def _api_probes(app: WebApp, params: dict[str, Any]) -> Any:
    """`analysis.probe_stats` on this thread's reader; incident heights and canonicity come from the loop."""
    now = app.clock()
    stats = analysis.probe_stats(app.require_conn(), _resolve_since(params["since"], now), now=now)
    hashes = sorted({incident["hash"] for incident in stats["incidents"]})
    if hashes:
        marks = app.on_loop(lambda: _chain_marks(app.chain(), hashes))
        for incident in stats["incidents"]:
            height, canonical = marks.get(incident["hash"], (None, None))
            if height is not None:
                incident["height"], incident["canonical"] = height, canonical
    return stats


def _chain_marks(chain: Any, hashes: list[str]) -> dict[str, tuple[int, bool]]:
    """Map each known block hash to (height, is canonical), as `probe_stats(chain=...)` does."""
    out = {}
    for block_hash in hashes:
        node = chain.get(block_hash)
        if node is not None:
            out[block_hash] = (node.height, chain.canonical_hash_at(node.height) == node.hash)
    return out


def _api_peers(app: WebApp, params: dict[str, Any]) -> dict[str, Any]:
    """Every source's view from the live snapshot, else computed on the loop."""
    snap = app.snapshot()
    if snap is not None and isinstance(snap.get("peers"), list):
        return {"generated_at": snap.get("generated_at"), "peers": snap["peers"]}
    now = app.clock()
    return {"generated_at": now, "peers": app.on_loop(lambda: analysis.peers(app.monitor, now))}


def _api_crosscheck(app: WebApp, params: dict[str, Any]) -> Any:
    """`analysis.external_crosscheck` (loop)."""
    return app.on_loop(lambda: analysis.external_crosscheck(app.chain(), app.conn()))


ROUTES: dict[str, _Route] = {
    "/api/snapshot": _Route(0, _api_snapshot, {"full": _flag_param}),
    "/api/summary": _Route(10, _api_summary),
    "/api/forks": _Route(
        10, _api_forks,
        {"limit": _int_param(1, analysis.MAX_FORK_LIMIT, DEFAULT_FORKS), "since": _since_param},
    ),
    "/api/orphans/stats": _Route(30, _api_orphan_stats),
    "/api/miners": _Route(30, _api_miners, {"since": _since_param}),
    "/api/sawtooth": _Route(15, _api_sawtooth, {"n": _int_param(1, analysis.MAX_SAWTOOTH, DEFAULT_SAWTOOTH)}),
    "/api/resets": _Route(
        30, _api_resets,
        {"limit": _int_param(1, analysis.MAX_RESET_LIMIT, DEFAULT_RESETS), "since": _since_param},
    ),
    "/api/propagation": _Route(30, _api_propagation, {"since": _since_param}),
    "/api/probes": _Route(15, _api_probes, {"since": _since_param}),
    "/api/peers": _Route(2, _api_peers),
    "/api/crosscheck": _Route(60, _api_crosscheck),
}


def _encode(value: Any) -> bytes:
    """Serialize strict, compact JSON (NaN/inf raise instead of producing invalid JSON)."""
    return json.dumps(value, allow_nan=False, separators=(",", ":")).encode()


def _error(err: HttpError) -> Response:
    """Return the JSON response for an HttpError."""
    return Response(err.status, _encode({"error": err.code, "message": err.message}))


# -- HTTP plumbing ----------------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    """Serves one request through the class's `app`; access logs are silenced."""

    app: WebApp
    server_version = f"zakura-fork-monitor/{__version__}"
    timeout = REQUEST_TIMEOUT
    _policy = API_POLICY

    def do_GET(self) -> None:
        """Answer GET."""
        self._respond(self.app.handle(self.path))

    def do_HEAD(self) -> None:
        """Answer HEAD with GET's status and headers but no body."""
        self._respond(self.app.handle(self.path))

    def _not_allowed(self) -> None:
        """Answer any other method with 405."""
        body = _encode({"error": "method_not_allowed", "message": "only GET and HEAD are supported"})
        self._respond(Response(405, body, headers=(("Allow", "GET, HEAD"),)))

    do_POST = do_PUT = do_PATCH = do_DELETE = do_OPTIONS = _not_allowed

    def _respond(self, response: Response) -> None:
        """Send `response`, ignoring clients that hang up."""
        self._policy = response.policy
        try:
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(response.body)))
            for name, value in response.headers:
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(response.body)
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            self.close_connection = True

    def end_headers(self) -> None:
        """Add the security and caching headers to every response, including stdlib error pages."""
        for name, value in COMMON_HEADERS:
            self.send_header(name, value)
        self.send_header("Content-Security-Policy", self._policy)
        super().end_headers()

    def version_string(self) -> str:
        """Name the server without the Python version."""
        return self.server_version

    def log_message(self, format: str, *args: Any) -> None:
        """Drop access and stdlib error logs (failures are logged by WebApp)."""


class _Server(ThreadingHTTPServer):
    """ThreadingHTTPServer whose handler threads never block interpreter exit."""

    daemon_threads = True
    block_on_close = False


class _Server6(_Server):
    """The same, bound to an IPv6 address."""

    address_family = socket.AF_INET6


def make_handler(monitor: Any, *, page: Page | None = None, clock: Callable[[], float] = time.time) -> type:
    """Return a request handler class bound to a new `WebApp` for `monitor`."""
    return type("ForkMonitorHandler", (_Handler,), {"app": WebApp(monitor, page=page, clock=clock)})


def make_server(
    monitor: Any, host: str, port: int, *, page: Page | None = None, clock: Callable[[], float] = time.time
) -> ThreadingHTTPServer:
    """Bind (but do not start) the dashboard server; port 0 picks a free port."""
    server_class = _Server6 if ":" in host else _Server
    return server_class((host.strip("[]"), port), make_handler(monitor, page=page, clock=clock))


def start_server(
    monitor: Any, host: str, port: int, *, page: Page | None = None, clock: Callable[[], float] = time.time
) -> ThreadingHTTPServer:
    """Bind the dashboard server and serve it from a daemon thread.

    Stop it with `server.shutdown()` (not from a handler thread) and `server.server_close()`.
    """
    server = make_server(monitor, host, port, page=page, clock=clock)
    threading.Thread(target=server.serve_forever, name="fork-monitor-web", daemon=True).start()
    return server
