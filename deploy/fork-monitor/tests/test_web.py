"""Tests for the dashboard HTTP server: routes, parameters, headers, caching and loop hops."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import http.client
import io
import json
import re
import shutil
import subprocess
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from zakura_fork_monitor import analysis, service, web
from zakura_fork_monitor.consensus import TESTNET

from .test_analysis import BASE, T0, StoreCase, sawtooth_tree, source_rows, split_tree

NOW = T0 + 3_100.0


def strict_json(body: bytes):
    """Parse `body` as strict JSON (NaN and Infinity rejected)."""

    def reject(token: str):
        """Refuse non-standard constants."""
        raise ValueError(f"non-standard JSON constant {token}")

    return json.loads(body, parse_constant=reject)


class ServerCase(StoreCase):
    """A server on port 0 over the sawtooth tree, a populated store and a live snapshot."""

    def setUp(self) -> None:
        """Populate the store, build the monitor and start the server."""
        super().setUp()
        self.tree = sawtooth_tree()
        tree, store = self.tree, self.store
        store.upsert_source("rpc:z1", kind="rpc", impl="zakura", impl_version="1.5.0", status="ok",
                            tip_hash=tree.hash("s30"), last_ok_at=NOW)
        store.upsert_source("p2p:10.0.0.3:18233", kind="p2p", impl="zebra", impl_version="6.4.2",
                            status="connected", tip_hash=tree.hash("z"), last_ok_at=NOW,
                            user_agent="<script>alert(1)</script>")
        store.upsert_source("p2p:10.0.0.4:18233", kind="p2p", impl="zebra", impl_version="6.3.0",
                            status="connected", tip_hash=tree.hash("s29"), last_ok_at=NOW)
        store.record_tip_change(source="rpc:z1", at=NOW - 60, old_hash=tree.hash("y"), new_hash=tree.hash("s10"),
                                fork_hash=tree.hash("s9"), is_reorg=1, disconnected=1, connected=1)
        store.record_sighting(tree.hash("s30"), "p2p:10.0.0.3:18233", "inv", NOW - 10)
        store.record_sighting(tree.hash("s30"), "rpc:z1", "rpc_tip", NOW - 9)
        store.record_probe(at=NOW - 30, source="p2p:10.0.0.3:18233", impl="zebra", hash=tree.hash("y"),
                           reason="announce", result="notfound", announced_by_same_peer=1)
        store.record_probe(at=NOW - 20, source="p2p:10.0.0.4:18233", impl="zebra", hash=tree.hash("s30"),
                           reason="announce", result="block", latency_ms=40)
        store.upsert_external_orphan(source="cipherscan", hash=tree.hash("x1"), height=BASE + 7)
        self.add_block(tree.hash("s30"), BASE + 66, NOW - 11)
        store.commit_if_due(force=True)
        self.monitor = types.SimpleNamespace(chain=tree.chain, store=store, config=None, params=TESTNET, snapshot=None)
        self.monitor.snapshot = analysis.live_snapshot(self.monitor, NOW)
        self.server = web.make_server(self.monitor, "127.0.0.1", 0, clock=lambda: NOW)
        thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.app = self.server.RequestHandlerClass.app

    def request(self, path: str, method: str = "GET") -> tuple[int, http.client.HTTPMessage, bytes]:
        """Send one request and return (status, headers, body)."""
        conn = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=10)
        try:
            conn.request(method, path)
            response = conn.getresponse()
            return response.status, response.headers, response.read()
        finally:
            conn.close()

    def get_json(self, path: str, status: int = 200):
        """GET `path`, assert the status and JSON headers, and return the parsed body."""
        code, headers, body = self.request(path)
        self.assertEqual(code, status, (path, body[:300]))
        self.assertEqual(headers["Content-Type"], web.JSON_TYPE)
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Content-Security-Policy"], web.API_POLICY)
        return strict_json(body)


class RouteTests(ServerCase):
    """Every route answers with the documented JSON shape."""

    def test_snapshot_drops_peers_unless_full(self) -> None:
        """The snapshot omits the per-source list by default and adds peer_count and served_at."""
        snap = self.get_json("/api/snapshot")
        self.assertNotIn("peers", snap)
        self.assertEqual(snap["peer_count"], len(self.monitor.snapshot["peers"]))
        self.assertEqual(snap["served_at"], NOW)
        self.assertEqual(snap["tip"]["hash"], self.tree.hash("s30"))
        self.assertIn("peers", self.monitor.snapshot, "the published snapshot must not be mutated")
        full = self.get_json("/api/snapshot?full=1")
        self.assertEqual(len(full["peers"]), full["peer_count"])

    def test_history_routes(self) -> None:
        """Chain-based routes return the analysis results for the tree."""
        summary = self.get_json("/api/summary")
        self.assertEqual(summary["tip"]["height"], BASE + 66)
        self.assertEqual(summary["periods"]["24h"]["reorgs"], 1)
        forks = self.get_json("/api/forks")
        self.assertEqual((forks["limit"], forks["since"]), (web.DEFAULT_FORKS, None))
        self.assertEqual([e["height"] for e in forks["forks"]], [BASE + 65, BASE + 46, BASE + 31, BASE + 26, BASE + 7])
        stats = self.get_json("/api/orphans/stats")
        self.assertEqual(stats["totals"]["orphans"], 5)
        miners = self.get_json("/api/miners")
        self.assertEqual(miners["stale"], 5)
        saw = self.get_json("/api/sawtooth?n=20")
        self.assertEqual(len(saw["blocks"]), 20)
        self.assertEqual(saw["to_height"], BASE + 66)
        resets = self.get_json("/api/resets")
        self.assertEqual([r["height"] for r in resets["resets"]], [BASE + 6])
        check = self.get_json("/api/crosscheck")
        self.assertEqual(check["totals"]["both"], 1)

    def test_store_routes(self) -> None:
        """Propagation and probes read the store; probe incidents get chain heights and canonicity."""
        propagation = self.get_json("/api/propagation?since=-3600")
        self.assertEqual(propagation["since"], NOW - 3_600)
        self.assertEqual(propagation["blocks"], 1)
        probes = self.get_json("/api/probes")
        self.assertEqual(probes["probes"], 2)
        (incident,) = probes["incidents"]
        self.assertEqual((incident["hash"], incident["height"], incident["canonical"]),
                         (self.tree.hash("y"), BASE + 46, False))

    def test_peers(self) -> None:
        """Peers come from the snapshot, or from the loop before the first snapshot."""
        peers = self.get_json("/api/peers")
        self.assertEqual(peers["peers"], self.monitor.snapshot["peers"])
        ua = {p["source"]: p["user_agent"] for p in peers["peers"]}["p2p:10.0.0.3:18233"]
        self.assertEqual(ua, "<script>alert(1)</script>")  # escaping is the page's job; JSON is nosniff
        self.monitor.snapshot = None
        peers = self.get_json("/api/peers")
        self.assertEqual({p["source"] for p in peers["peers"]},
                         {"rpc:z1", "p2p:10.0.0.3:18233", "p2p:10.0.0.4:18233"})

    def test_health(self) -> None:
        """Healthy with a fresh snapshot; 503 before the first one and when it goes stale."""
        self.assertEqual(self.get_json("/healthz")["status"], "ok")
        self.monitor.snapshot = None
        self.assertEqual(self.get_json("/healthz", 503)["status"], "starting")
        self.assertEqual(self.get_json("/api/snapshot", 503)["error"], "starting")
        self.monitor.snapshot = {"generated_at": NOW - web.SNAPSHOT_STALE_AFTER - 1, "tip": {"height": 7}}
        health = self.get_json("/healthz", 503)
        self.assertEqual((health["status"], health["tip_height"]), ("stale", 7))


class ParameterTests(ServerCase):
    """Validation (400), clamping and relative `since`."""

    def test_bad_parameters(self) -> None:
        """Malformed, unknown, repeated and oversized parameters are rejected with a JSON 400."""
        for path in ("/api/forks?limit=abc", "/api/forks?limit=1&limit=2", "/api/forks?bogus=1",
                     "/api/forks?since=nan", "/api/forks?since=1e5", "/api/forks?since=", "/api/sawtooth?n=1.5",
                     "/api/sawtooth?n=%00", "/api/snapshot?full=maybe", "/api/summary?x=1", "/api/forks?limit",
                     "/api/resets?limit=" + "9" * 13, "/api/forks?since=" + "1" * 2_000,
                     "/api/miners?" + "&".join(f"since={i}" for i in range(20))):
            with self.subTest(path=path[:60]):
                body = self.get_json(path, 400)
                self.assertEqual(body["error"], "bad_request")
                self.assertIsInstance(body["message"], str)
        # http.client refuses to send this target, so it goes to the app directly; it must not log a traceback.
        with self.assertNoLogs(web.LOG, "ERROR"):
            response = self.app.handle("http://[x/api/summary")
        self.assertEqual((response.status, strict_json(response.body)["error"]), (400, "bad_request"))

    def test_clamping(self) -> None:
        """Numbers outside their range are clamped rather than rejected."""
        self.assertEqual(self.get_json("/api/forks?limit=100000")["limit"], analysis.MAX_FORK_LIMIT)
        self.assertEqual(self.get_json("/api/resets?limit=-5")["limit"], 1)
        self.assertEqual(len(self.get_json("/api/sawtooth?n=0")["blocks"]), 1)
        self.assertEqual(len(self.get_json("/api/sawtooth?n=999999")["blocks"]), 67)
        self.assertEqual(self.get_json("/api/forks?since=-999999999999")["since"], NOW - web.MAX_LOOKBACK)
        self.assertEqual(self.get_json("/api/forks?since=999999999999")["since"], web.MAX_TIMESTAMP)

    def test_relative_since(self) -> None:
        """A negative since is seconds before now and selects the same events as the absolute time."""
        cutoff = self.tree.node("f25").time
        absolute = self.get_json(f"/api/forks?since={cutoff}")
        relative = self.get_json(f"/api/forks?since={cutoff - NOW}")
        self.assertEqual(relative["since"], cutoff)
        self.assertEqual(absolute["forks"], relative["forks"])
        self.assertEqual([e["height"] for e in relative["forks"]], [BASE + 65, BASE + 46, BASE + 31])
        self.assertEqual(self.get_json("/api/miners?since=0")["stale"], 5)


class HttpTests(ServerCase):
    """The page, headers, 404/405, HEAD and silent logs."""

    def test_page_is_served_with_a_strict_csp(self) -> None:
        """The page comes with a CSP that admits exactly its inline script and style by hash."""
        status, headers, body = self.request("/")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], web.HTML_TYPE)
        self.assertEqual(body, web.PAGE_FILE.read_bytes())
        policy = headers["Content-Security-Policy"]
        html = body.decode()
        for tag, directive in (("script", "script-src"), ("style", "style-src")):
            (block,) = re.findall(rf"<{tag}>(.*?)</{tag}>", html, re.S)
            digest = base64.b64encode(hashlib.sha256(block.encode()).digest()).decode()
            self.assertIn(f"{directive} 'sha256-{digest}'", policy)
        for part in ("default-src 'none'", "connect-src 'self'", "img-src data:", "frame-ancestors 'none'",
                     "base-uri 'none'", "form-action 'none'"):
            self.assertIn(part, policy)
        self.assertNotIn("unsafe", policy)
        for name, value in web.COMMON_HEADERS:
            self.assertEqual(headers[name], value)
        self.assertEqual(headers["Server"], f"zakura-fork-monitor/{web.__version__}")

    def test_page_is_self_contained_and_never_injects_html(self) -> None:
        """One inline script and style, no external loads, no HTML sinks, no inline handlers or styles."""
        html = web.PAGE_FILE.read_text(encoding="utf-8")
        self.assertEqual(len(re.findall(r"<script\b", html)), 1)
        self.assertEqual(len(re.findall(r"<style\b", html)), 1)
        self.assertIn("<script>", html)
        for sink in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function",
                     "setAttribute('style'", "cssText"):
            self.assertNotIn(sink, html)
        self.assertIsNone(re.search(r"\son[a-z]+\s*=", html), "inline event handler")
        self.assertIsNone(re.search(r"\sstyle\s*=", html), "inline style attribute")
        urls = set(re.findall(r"https?://[^\s'\"<>)]+", html)) - {"http://www.w3.org/2000/svg"}
        self.assertEqual(urls, set(), "the page must not reference external resources")
        for section in ("sawtooth-chart", "phase-chart", "hourly-chart", "probes-chart", "groups-table",
                        "forks-table", "miners-table", "resets-table", "incidents-table", "propagation-table",
                        "peers-table", "crosscheck-facts", "banner"):
            self.assertIn(f'id="{section}"', html)

    @unittest.skipUnless(shutil.which("node"), "node is not installed")
    def test_inline_script_parses(self) -> None:
        """The inline script is syntactically valid JavaScript (node --check)."""
        (script,) = re.findall(r"<script>(.*?)</script>", web.PAGE_FILE.read_text(encoding="utf-8"), re.S)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "page.js"
            path.write_text(script, encoding="utf-8")
            result = subprocess.run([shutil.which("node"), "--check", str(path)], capture_output=True, text=True,
                                    timeout=60, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_not_found_and_methods(self) -> None:
        """Unknown paths are JSON 404s; other methods are 405 with Allow; HEAD has headers only."""
        for path in ("/nope", "/api/", "/api/summary/", "/api/orphans", "/index.html", "/api/summary%2F"):
            with self.subTest(path=path):
                self.assertEqual(self.get_json(path, 404)["error"], "not_found")
        status, headers, body = self.request("/api/summary", "POST")
        self.assertEqual((status, headers["Allow"]), (405, "GET, HEAD"))
        self.assertEqual(strict_json(body)["error"], "method_not_allowed")
        status, headers, body = self.request("/", "HEAD")
        self.assertEqual((status, body), (200, b""))
        self.assertEqual(int(headers["Content-Length"]), len(web.PAGE_FILE.read_bytes()))

    def test_access_logs_are_silenced(self) -> None:
        """Requests, including errors, write nothing to stderr."""
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            self.request("/")
            self.request("/nope")
            self.request("/api/forks?limit=x")
            conn = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=10)
            conn.putrequest("GET", "/" + "a" * 70_000, skip_accept_encoding=True)  # request line too long
            conn.endheaders()
            self.assertEqual(conn.getresponse().status, 414)
            conn.close()
        self.assertEqual(buffer.getvalue(), "")

    def test_internal_errors(self) -> None:
        """An analysis failure is a logged JSON 500 that does not leak details."""
        with mock.patch.object(analysis, "summary", side_effect=RuntimeError("secret detail")):
            with self.assertLogs("zakura_fork_monitor.web", "ERROR"):
                body = self.get_json("/api/summary", 500)
        self.assertEqual(body["error"], "internal")
        self.assertNotIn("secret", body["message"])

    def test_start_server(self) -> None:
        """start_server serves from a daemon thread until shut down."""
        server = web.start_server(self.monitor, "127.0.0.1", 0, clock=lambda: NOW)
        try:
            conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=10)
            conn.request("GET", "/healthz")
            self.assertEqual(conn.getresponse().status, 200)
            conn.close()
        finally:
            server.shutdown()
            server.server_close()

    def test_without_store(self) -> None:
        """Chain routes still work without a store; store-only routes answer 503."""
        self.monitor.store = None
        self.assertIsNone(self.get_json("/api/summary")["periods"]["1h"]["reorgs"])
        self.assertEqual(self.get_json("/api/propagation", 503)["error"], "no_store")


class CacheTests(ServerCase):
    """Per-route TTL cache and single-flight computation."""

    def test_results_are_cached_per_normalized_params(self) -> None:
        """Within the TTL a route is served from cache; clamped parameters share one entry."""
        first = self.get_json("/api/forks?limit=500")
        self.tree.add("late", "s30", dt=5)
        self.tree.add("late2", "s30", dt=6, miner="B")
        self.assertEqual(self.get_json("/api/forks?limit=200"), first)
        self.assertNotEqual(self.get_json("/api/forks?limit=199")["forks"], first["forks"])

    def test_single_flight(self) -> None:
        """Concurrent misses for one key compute once; errors are not cached."""
        calls = []
        release = threading.Event()

        def compute() -> bytes:
            """Count the call and block until released."""
            calls.append(1)
            release.wait(5)
            return b"{}"

        results = []
        threads = [threading.Thread(target=lambda: results.append(self.app._cached(("k",), 60, compute)))
                   for _ in range(8)]
        for thread in threads:
            thread.start()
        time.sleep(0.1)
        release.set()
        for thread in threads:
            thread.join(5)
        self.assertEqual((len(calls), results), (1, [b"{}"] * 8))

        def failing() -> bytes:
            """Fail once."""
            raise web.HttpError(503, "busy", "try again")

        with self.assertRaises(web.HttpError):
            self.app._cached(("e",), 60, failing)
        self.assertEqual(self.app._cached(("e",), 60, lambda: b"ok"), b"ok")

    def test_cache_is_bounded(self) -> None:
        """Old entries are evicted beyond CACHE_ENTRIES."""
        for index in range(web.CACHE_ENTRIES + 10):
            self.app._cached(("n", index), 60, lambda: b"x")
        self.assertEqual(len(self.app._cache), web.CACHE_ENTRIES)
        self.assertNotIn(("n", 0), self.app._cache)


class LoopTests(ServerCase):
    """Chain access happens on the monitor's event loop when one is running."""

    def setUp(self) -> None:
        """Run an asyncio loop in a background thread and record which threads touch the chain."""
        super().setUp()
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.loop_thread.start()
        self.addCleanup(self._stop_loop)
        chain = self.monitor.chain
        self.touched: set[int] = set()
        test = self

        class Monitor(types.SimpleNamespace):
            """Records the thread of every chain access."""

            @property
            def chain(self):
                """Return the chain, noting the calling thread."""
                test.touched.add(threading.get_ident())
                return chain

        self.monitor_on_loop = Monitor(store=self.store, config=None, params=TESTNET, snapshot=None, loop=self.loop)
        self.app = web.WebApp(self.monitor_on_loop, clock=lambda: NOW)

    def _stop_loop(self) -> None:
        """Stop and close the background loop (idempotent)."""
        if self.loop.is_closed():
            return
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.loop_thread.join(5)
        self.loop.close()

    def test_chain_routes_run_on_the_loop(self) -> None:
        """Every chain-based route touches the chain only from the loop thread."""
        for path in ("/api/summary", "/api/forks", "/api/orphans/stats", "/api/miners", "/api/sawtooth",
                     "/api/resets", "/api/probes", "/api/peers", "/api/crosscheck"):
            with self.subTest(path=path):
                response = self.app.handle(path)
                self.assertEqual(response.status, 200, response.body[:200])
        self.assertEqual(self.touched, {self.loop_thread.ident})

    def test_busy_loop_times_out(self) -> None:
        """A handler gives up with 503 when the loop does not run its job in time."""
        blocked = threading.Event()
        self.loop.call_soon_threadsafe(lambda: blocked.wait(2))
        with mock.patch.object(web, "LOOP_TIMEOUT", 0.2):
            response = self.app.handle("/api/summary")
        blocked.set()
        self.assertEqual((response.status, strict_json(response.body)["error"]), (503, "busy"))

    def test_stopped_loop_runs_inline(self) -> None:
        """Without a running loop (tests, CLI) the analysis runs inline on the caller's thread."""
        self._stop_loop()
        self.assertEqual(self.app.handle("/api/summary").status, 200)
        self.assertIn(threading.get_ident(), self.touched)


# Runs the page script under node with a minimal DOM whose fetch serves one snapshot, then prints
# the split pill, the tab title and the classes and text of the split callout and groups table.
PAGE_HARNESS = r"""
'use strict';
const fs = require('fs');
const vm = require('vm');
const [scriptPath, snapPath] = process.argv.slice(2);
const snap = JSON.parse(fs.readFileSync(snapPath, 'utf8'));
class Node {}
class El extends Node {
  constructor(tag) {
    super();
    this.tagName = tag; this.kids = []; this.attrs = {}; this.style = {}; this.className = ''; this.title = ''; this.hidden = false;
    this.classList = {toggle() {}, add() {}, remove() {}, contains() { return false; }};
  }
  get textContent() { return this.kids.map((k) => (typeof k === 'string' ? k : k.textContent)).join(''); }
  set textContent(v) { this.kids = [String(v)]; }
  append(...kids) { this.kids.push(...kids); }
  replaceChildren(...kids) { this.kids = []; this.append(...kids); }
  setAttribute(k, v) { this.attrs[k] = v; if (k === 'title') this.title = v; }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  addEventListener() {}
  querySelector() { return new El('div'); }
  querySelectorAll() { return []; }
  closest() { return null; }
}
const classes = (el) => (typeof el === 'string' ? [] : [...el.className.split(' ').filter(Boolean), ...el.kids.flatMap(classes)]);
const els = {};
const errors = [];
Object.assign(globalThis, {
  Node,
  document: {
    hidden: false, title: '', body: new El('body'), addEventListener() {},
    getElementById: (id) => els[id] || (els[id] = new El('div')),
    createElement: (tag) => new El(tag), createElementNS: (ns, tag) => new El(tag),
  },
  window: {addEventListener() {}},
  setInterval: () => 0,
  fetch: async (path) => (path === '/api/snapshot'
    ? {ok: true, status: 200, json: async () => snap}
    : {ok: false, status: 404, json: async () => ({error: 'not_found'})}),
});
console.error = (...args) => errors.push(args.map(String).join(' '));
vm.runInThisContext(fs.readFileSync(scriptPath, 'utf8'));
setTimeout(() => {
  const pill = els['split-pill'];
  const callout = els['split-callout'];
  process.stdout.write(JSON.stringify({
    title: document.title, errors,
    pill: {className: pill.className, text: pill.textContent, title: pill.title},
    callout: {classes: classes(callout), text: callout.textContent},
    groups: classes(els['groups-table']),
  }));
  process.exit(0);
}, 100);
"""


@unittest.skipUnless(shutil.which("node"), "node is not installed")
class PageSplitTests(StoreCase):
    """The page alarms on the service's gated `split_event`, never on a bare `split_candidate`."""

    def setUp(self) -> None:
        """Build a live snapshot over the split tree, whose candidate has zebra 6.4 on a side branch."""
        super().setUp()
        self.now = T0 + 1_000.0
        tree = split_tree(self.now)
        for row in source_rows(tree, self.now):
            self.store.upsert_source(row["source"], **{k: v for k, v in row.items() if k != "source"})
        self.store.commit_if_due(force=True)
        monitor = types.SimpleNamespace(chain=tree.chain, store=self.store, config=None, params=TESTNET)
        self.snap = analysis.live_snapshot(monitor, self.now)
        self.candidate = self.snap["split_candidate"]
        self.assertIsNotNone(self.candidate)

    def render(self, event: service._Split | None) -> dict:
        """Render the snapshot with `event` as its `split_event` and return what the harness saw."""
        snap = dict(self.snap, split_event=event.view() if event is not None else None, served_at=self.now)
        (script,) = re.findall(r"<script>(.*?)</script>", web.PAGE_FILE.read_text(encoding="utf-8"), re.S)
        with tempfile.TemporaryDirectory() as tmp:
            paths = [Path(tmp) / name for name in ("harness.js", "page.js", "snap.json")]
            for path, content in zip(paths, (PAGE_HARNESS, script, json.dumps(snap)), strict=True):
                path.write_text(content, encoding="utf-8")
            result = subprocess.run([shutil.which("node"), *map(str, paths)], capture_output=True, text=True,
                                    timeout=60, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        seen = json.loads(result.stdout)
        self.assertEqual(seen["errors"], [])
        return seen

    def split(self, since: float, event_id: int | None) -> service._Split:
        """Return a tracked split over the candidate, opened as `event_id` or still pending."""
        c = self.candidate
        return service._Split(c["fork_hash"], c["fork_height"], since, self.now, 2, 3, c, event_id)

    def test_candidate_without_event_is_lag(self) -> None:
        """A candidate the service did not admit (e.g. a one-block orphan) leaves the header green."""
        seen = self.render(None)
        self.assertEqual((seen["pill"]["className"], seen["pill"]["text"]), ("state-pill is-ok", "No split"))
        self.assertIn("lag", seen["pill"]["title"])
        self.assertFalse(seen["title"].startswith("SPLIT"))
        self.assertNotIn("is-bad", seen["callout"]["classes"])
        self.assertIn("lag, not a split", seen["callout"]["text"])
        self.assertIn("2 blocks past its fork at 1,000,005", seen["callout"]["text"])
        self.assertIn("row-warn", seen["groups"])
        self.assertNotIn("row-bad", seen["groups"])

    def test_pending_event_warns(self) -> None:
        """A split that has not lasted 30 s yet is a warning, not an alarm."""
        seen = self.render(self.split(self.now - 10, None))
        self.assertEqual((seen["pill"]["className"], seen["pill"]["text"]),
                         ("state-pill is-warn", "Possible split · fork at 1,000,005"))
        self.assertFalse(seen["title"].startswith("SPLIT"))
        self.assertIn("is-warn", seen["callout"]["classes"])
        self.assertNotIn("is-bad", seen["callout"]["classes"])

    def test_open_event_alarms(self) -> None:
        """An open split event turns the pill, the tab title, the callout and the side's group red."""
        seen = self.render(self.split(self.now - 95, 1))
        self.assertEqual((seen["pill"]["className"], seen["pill"]["text"]),
                         ("state-pill is-bad", "Split · fork at 1,000,005"))
        self.assertIn("split for 1m 35s", seen["pill"]["title"])
        self.assertIn("longest side 2 blocks past its fork (max 3)", seen["pill"]["title"])
        self.assertTrue(seen["title"].startswith("SPLIT · 1,000,010"))
        self.assertIn("is-bad", seen["callout"]["classes"])
        self.assertIn("row-bad", seen["groups"])


class PolicyTests(unittest.TestCase):
    """page_policy on small documents."""

    def test_hashes_every_inline_block(self) -> None:
        """Each attribute-less script/style block is hashed; none gives 'none'."""
        html = "<style>a{}</style><script>1</script><script>2</script>"
        policy = web.page_policy(html)
        for tag, content in (("style", "a{}"), ("script", "1"), ("script", "2")):
            digest = base64.b64encode(hashlib.sha256(content.encode()).digest()).decode()
            self.assertIn(f"'sha256-{digest}'", policy, tag)
        self.assertIn("script-src 'none'", web.page_policy("<p>no code</p>"))


if __name__ == "__main__":
    unittest.main()
