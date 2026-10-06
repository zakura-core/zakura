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
from zakura_fork_monitor.config import parse_config
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
        self.assertEqual(snap["old_rules"], {"peers": 0, "fork_height": None})

    def test_snapshot_summarizes_old_rules_peers(self) -> None:
        """`old_rules` counts peers in state "old-rules" and keeps their lowest known fork height."""
        old = [{"source": f"p2p:10.0.1.{i}:18233", "state": "old-rules", "relation": {"kind": "fork", "fork_height": h}}
               for i, h in enumerate((4_465_082, 4_465_025, None))]
        others = [{"state": "synced", "relation": {"fork_height": 7}}, {"state": "old-rules", "relation": None}, "junk"]
        self.monitor.snapshot = dict(self.monitor.snapshot, peers=old + others)
        snap = self.get_json("/api/snapshot")
        self.assertEqual(snap["old_rules"], {"peers": 4, "fork_height": 4_465_025})
        self.assertEqual(snap["peer_count"], 6)
        self.assertNotIn("peers", snap)

    def test_summary_leaves_out_retired_sources(self) -> None:
        """`/api/summary` applies the monitor's config to source health, as the peer views do."""
        self.monitor.config = parse_config({"rpc": [{"name": "z2", "url": "http://10.0.0.2:18232/"}]})
        self.assertEqual(self.get_json("/api/summary")["sources"]["rpc"], [])

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


# Runs the page script under node with a minimal DOM. `fetch` answers each route path (query string
# dropped) from the input's `routes`, `Date.now` is fixed at `now`, and click `actions` run after the
# first render. Prints the split pill, the tab title, the split callout and groups table classes, and
# for each element id in `dump` its text, its titles and the text of its descendants by class.
PAGE_HARNESS = r"""
'use strict';
const fs = require('fs');
const vm = require('vm');
const [scriptPath, inputPath] = process.argv.slice(2);
const input = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
class Node {}
class El extends Node {
  constructor(tag) {
    super();
    this.tagName = tag; this.kids = []; this.attrs = {}; this.style = {}; this.className = ''; this.title = ''; this.hidden = false;
    this.handlers = {}; this.queried = {};
    this.classList = {toggle() {}, add() {}, remove() {}, contains() { return false; }};
  }
  get textContent() { return this.kids.map((k) => (typeof k === 'string' ? k : k.textContent)).join(''); }
  set textContent(v) { this.kids = [String(v)]; }
  append(...kids) { this.kids.push(...kids); }
  replaceChildren(...kids) { this.kids = []; this.append(...kids); }
  setAttribute(k, v) { this.attrs[k] = v; if (k === 'title') this.title = v; }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  addEventListener(type, fn) { (this.handlers[type] = this.handlers[type] || []).push(fn); }
  querySelector(sel) {
    if (!this.queried[sel]) this.append(this.queried[sel] = new El('div'));
    return this.queried[sel];
  }
  querySelectorAll() { return []; }
  closest() { return null; }
}
const walk = (el) => (typeof el === 'string' ? [] : [el, ...el.kids.flatMap(walk)]);
// SVG elements get their class as an attribute.
const classOf = (e) => (e.className || e.attrs.class || '').split(' ').filter(Boolean);
const classes = (el) => walk(el).flatMap(classOf);
const els = {};
const byId = (id) => els[id] || (els[id] = new El('div'));
byId('peers-details').open = Boolean(input.open_peers);
const errors = [];
Object.assign(globalThis, {
  Node,
  document: {
    hidden: false, title: '', body: new El('body'), addEventListener() {},
    getElementById: byId,
    createElement: (tag) => new El(tag), createElementNS: (ns, tag) => new El(tag),
  },
  window: {addEventListener() {}},
  setInterval: () => 0,
  fetch: async (path) => {
    const route = path.split('?')[0];
    return route in input.routes
      ? {ok: true, status: 200, json: async () => input.routes[route]}
      : {ok: false, status: 404, json: async () => ({error: 'not_found'})};
  },
});
Date.now = () => input.now * 1000;
console.error = (...args) => errors.push(args.map(String).join(' '));
vm.runInThisContext(fs.readFileSync(scriptPath, 'utf8'));
function click(el) {
  const event = {target: el, currentTarget: el, key: 'Enter', preventDefault() {}, stopPropagation() {}};
  for (const fn of el.handlers.click || []) fn(event);
}
setTimeout(() => {
  for (const action of input.actions || []) {
    const root = byId(action.id);
    click(action.cls ? walk(root).find((e) => classOf(e).includes(action.cls)) : root);
  }
  const pill = byId('split-pill');
  const callout = byId('split-callout');
  const dump = {};
  for (const id of input.dump || []) {
    const nodes = walk(byId(id));
    const byClass = {};
    for (const e of nodes) for (const cls of classOf(e)) (byClass[cls] = byClass[cls] || []).push(e.textContent);
    dump[id] = {text: byId(id).textContent, titles: nodes.map((e) => e.title).filter(Boolean), byClass};
  }
  process.stdout.write(JSON.stringify({
    title: document.title, errors,
    pill: {className: pill.className, text: pill.textContent, title: pill.title},
    callout: {classes: classes(callout), text: callout.textContent},
    groups: classes(byId('groups-table')),
    dump,
  }));
  process.exit(0);
}, 100);
"""


def render_page(test: unittest.TestCase, routes: dict, *, now: float, dump=(), actions=(),
                open_peers: bool = False) -> dict:
    """Run the page script on `routes` (path -> JSON body) under node and return what the harness saw."""
    (script,) = re.findall(r"<script>(.*?)</script>", web.PAGE_FILE.read_text(encoding="utf-8"), re.S)
    data = {"routes": routes, "now": now, "dump": list(dump), "actions": list(actions), "open_peers": open_peers}
    with tempfile.TemporaryDirectory() as tmp:
        paths = [Path(tmp) / name for name in ("harness.js", "page.js", "input.json")]
        for path, content in zip(paths, (PAGE_HARNESS, script, json.dumps(data)), strict=True):
            path.write_text(content, encoding="utf-8")
        result = subprocess.run([shutil.which("node"), *map(str, paths)], capture_output=True, text=True,
                                timeout=60, check=False)
    test.assertEqual(result.returncode, 0, result.stderr)
    seen = json.loads(result.stdout)
    test.assertEqual(seen["errors"], [])
    return seen


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
        return render_page(self, {"/api/snapshot": snap}, now=self.now)

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


PAGE_NOW = 1_791_300_000.0
STEADY_PHASE = {"height": 4_471_000, "k": 6_972, "reset_height": 4_464_670, "d_pre": None, "difficulty": 10_000.0,
                "d_ratio": None, "fast": False, "min_diff": False, "label": "steady", "era": "nu7",
                "k_bucket": "no reset since NU7", "ratio_bucket": "no reset since NU7"}


def page_block(name: str, **fields) -> dict:
    """Return an API block dict for a block named `name` with a body, overridden by `fields`."""
    block = {"hash": analysis_hash(name), "height": 4_470_840, "time": int(PAGE_NOW) - 100, "miner": "zkcodexcoder",
             "template": "zakura", "first_seen_at": PAGE_NOW - 100, "min_diff": False, "body": True}
    return {**block, **fields}


def analysis_hash(name: str) -> str:
    """Return a 64-hex-digit hash derived from `name`."""
    return hashlib.sha256(name.encode()).hexdigest()


def peer_view(source: str, **fields) -> dict:
    """Return a peer view (see `analysis.peers`) of a synced, active source, overridden by `fields`."""
    view = {"source": source, "kind": source.partition(":")[0], "fleet": False, "impl": "zebra", "version": "7.0.0",
            "group": "zebra 7.0", "user_agent": None, "status": "connected", "active": True, "state": "synced",
            "stuck": False, "tip_hash": analysis_hash("tip"), "tip_height": 4_471_000, "tip_at": PAGE_NOW - 5,
            "tip_age_s": 5.0, "behind": 0, "relation": {"kind": "same", "n": 0}, "start_height": 4_470_000,
            "first_seen_at": PAGE_NOW - 9_000, "last_ok_at": PAGE_NOW - 2, "last_error": None, "last_error_at": None,
            "previous_error": None, "previous_error_at": None, "live": None}
    return {**view, **fields}


def old_rules_view(source: str, fork_height: int) -> dict:
    """Return the view of a connected peer rejected for pre-NU7 headers, forked at `fork_height`."""
    return peer_view(source, group="zebra 6.4", version="6.4.2", status="old-rules", state="old-rules",
                     tip_hash=None, tip_height=None, tip_at=None, tip_age_s=None, behind=None, start_height=4_467_324,
                     relation={"kind": "fork", "n": None, "fork_hash": None, "fork_height": fork_height,
                               "depth_ours": None, "depth_theirs": None, "tip_height": 4_467_324},
                     last_error="header 0090e649 has the wrong difficulty", last_error_at=PAGE_NOW - 2_900)


@unittest.skipUnless(shutil.which("node"), "node is not installed")
class PageFindingTests(unittest.TestCase):
    """The page renders the audit's new API values (old-rules peers, steady phase, tie-breaks, ...) in plain words.

    The payloads are hand-built in the documented shapes, so these tests do not depend on the analysis.
    """

    def test_live_view(self) -> None:
        """Old-rules peers, tip-less stuck peers, the steady phase pill, fleet hosts and current vs previous errors."""
        stuck = peer_view("p2p:45.76.52.93:18233", group="zebra 6.0", state="stuck", stuck=True, tip_hash=None,
                          tip_height=None, tip_at=None, tip_age_s=None, behind=150_622, start_height=4_320_378,
                          relation={"kind": "unknown"}, live={"tip_note": "no common block in window"})
        peers = [
            old_rules_view("p2p:178.105.92.0:18233", 4_465_082),
            old_rules_view("p2p:69.30.210.162:18233", 4_465_025),
            peer_view("rpc:tazminer", previous_error="tip: Connection refused", previous_error_at=PAGE_NOW - 180_000),
            peer_view("p2p:10.0.0.1:18233", last_error="timed out", last_error_at=PAGE_NOW - 10),
            peer_view("p2p:10.0.0.2:18233", active=False, state="inactive", last_error="connect timeout",
                      last_error_at=PAGE_NOW - 7_200),
            stuck,
            peer_view("p2p:10.0.0.9:18233", group="zebra 6.4", active=False, state="inactive", stale=True,
                      last_seen_at=PAGE_NOW - 180_000, last_seen_age_s=180_000.0, behind=1_879, tip_age_s=35_280.0,
                      relation={"kind": "behind", "n": 56_966}, live={"rtt_ms": 143.0}),
        ]
        canonical = {"key": "canonical", "tip_hash": analysis_hash("tip"), "tip_height": 4_471_000, "fork_hash": None,
                     "fork_height": None, "relation": {"kind": "same"}, "members": 6, "sources": []}
        groups = [
            {"key": "zakura 1.6", "impl": "zakura", "version": "1.6", "members": 6, "active": 6, "stuck": 0, "fleet": 3,
             "old_rules": 0, "states": {"synced": 6}, "branch": canonical, "branches": [canonical],
             "stuck_sources": []},
            {"key": "zebra 6.4", "impl": "zebra", "version": "6.4", "members": 2, "active": 2, "stuck": 0, "fleet": 0,
             "stale": 1, "old_rules": 2, "old_rules_relation": peers[0]["relation"], "states": {"old-rules": 2},
             "branch": None, "branches": [], "stuck_sources": []},
        ]
        snap = {"generated_at": PAGE_NOW, "network": "testnet", "phase": STEADY_PHASE,
                "tip": {**page_block("tip", height=4_471_000), "age_s": 5.0},
                "chain": {"blocks": 30_000, "from_height": 4_441_000, "to_height": 4_471_000, "settle_depth": 3,
                          "missing_parents": 0},
                "peers": peers, "groups": groups, "split_candidate": None, "split_event": None, "stuck": [stuck],
                "collectors": {"rpc": [], "rpc_error": None,
                               "p2p": {"enabled": True, "error": None, "peers": 5, "connected": 5,
                                       "connected_by_group": {}}},
                "recent_reorgs": []}
        # The snapshot route goes through web.py, which derives `old_rules` from the peers.
        app = web.WebApp(types.SimpleNamespace(snapshot=snap), clock=lambda: PAGE_NOW)
        routes = {"/api/snapshot": strict_json(app.handle("/api/snapshot").body),
                  "/api/peers": {"generated_at": PAGE_NOW, "peers": peers}}
        ids = ("network-note", "split-callout", "groups-table", "phase-pill", "stuck-list", "peers-table")
        seen = render_page(self, routes, now=PAGE_NOW, dump=ids, open_peers=True)
        dump = seen["dump"]
        self.assertIn("8 active · 1 more not seen in 24 h · 2 peers on pre-NU7 rules (fork at 4,465,025) · "
                      "30,000 blocks", dump["network-note"]["text"])
        self.assertIn("Left out: 2 peers on pre-NU7 rules (fork at 4,465,025).", dump["split-callout"]["text"])
        groups_seen = dump["groups-table"]
        self.assertIn("fleet 3", groups_seen["text"])
        self.assertIn("fleet hosts (RPC + P2P vantage points)", groups_seen["titles"])
        self.assertIn("2 on pre-NU7 rules", groups_seen["text"])
        self.assertIn("≈4,467,324", groups_seen["text"])  # an old-rules group's tip and relation come from its peers
        self.assertIn("fork at 4,465,082", groups_seen["text"])
        self.assertIn("seg-old", groups_seen["byClass"])
        self.assertEqual(dump["phase-pill"]["text"], "No reset since NU7 · k 6,972")
        stuck_text = dump["stuck-list"]["text"]
        self.assertIn("tip ≈4,320,378 (version height) · no common block in window · 150,622 behind the best tip",
                      stuck_text)
        self.assertNotIn("unknown", stuck_text)
        table = dump["peers-table"]
        self.assertIn("old rules", table["byClass"]["badge"])
        self.assertIn("≈4,467,324 (version height)", table["text"])
        self.assertIn("fork at 4,465,082", table["text"])
        self.assertNotIn("vs", table["text"])
        self.assertIn("earlier: tip: Connection refused · 2d 2h ago", table["byClass"]["sub"])
        self.assertIn("timed out · 10s ago", table["byClass"]["bad-text"])
        # A source silent for 24 h shows when it was last seen, not its old tip, lag, tip age or RTT.
        self.assertIn("last seen 2d 2h ago", table["text"])
        for old in ("1,879", "56,966", "9h 48m", "143 ms"):
            self.assertNotIn(old, table["text"])

        issues = render_page(self, routes, now=PAGE_NOW, dump=["peers-table"], open_peers=True,
                             actions=[{"id": "peer-issues"}])["dump"]["peers-table"]["text"]
        for source in ("p2p:178.105.92.0:18233", "p2p:10.0.0.1:18233", "p2p:45.76.52.93:18233"):
            self.assertIn(source, issues)
        for source in ("rpc:tazminer", "p2p:10.0.0.2:18233"):  # a superseded error; an inactive row's error
            self.assertNotIn(source, issues)

    def test_history_sections(self) -> None:
        """Tie-break labels, the fork drawer, steady-phase tables, resets, probes and the cross-check facts."""
        loser = {"block": page_block("loser", miner=None, template=None, body=False, first_seen_at=PAGE_NOW - 99.76),
                 "tip_hash": analysis_hash("loser"), "length": 1, "work": 1, "blocks": 1, "miners": ["no body"],
                 "classification": "no_body", "same_job": False, "seen_first": False, "seen_gap_s": 0.24,
                 "greater_raw_hash": False, "equal_work": True, "winner_len": 1,
                 "probes": {"block": 0, "notfound": 2}, "adopted_by": {"count": 0, "by_group": {}, "sources": []}}
        reorg = {"source": "p2p:9.9.9.9:18233", "group": "zebra 7.0", "at": PAGE_NOW - 90, "disconnected": 1,
                 "connected": 1, "reorgs": 3}
        forks = [
            {"fork_hash": analysis_hash(f"fork{i}"), "fork_height": 4_470_839 - i, "fork_time": int(PAGE_NOW) - 200,
             "height": 4_470_840 - i, "winner": {**page_block(f"w{i}", miner_tag="mined by zkcodexcoder"),
                                                 "probes": None, "adopted_by": None},
             "losers": [loser], "loser_count": 1, "depth": 1, "depth_work": 1, "classification": "no_body",
             "same_job": False, "winner_first_seen": True, "winner_greater_raw_hash": True, "equal_work": True,
             "tiebreak": tiebreak, "settled": True, "phase": STEADY_PHASE,
             "reorgs": {"count": 1, "sources": [reorg]}}
            for i, tiebreak in enumerate(("late", "unresolved", "work", "hash", "first_seen", "both"))
        ]
        miner = {"canonical": 0, "share": None, "stale": 0, "stale_rate": None, "self_orphans": 0, "races_lost": 0,
                 "races_won": 0, "unattributed_losses": 0, "resets": 0, "templates": {}}
        steady = {"fast": {"canonical": 0, "stale": 0}, "slow": {"canonical": 0, "stale": 0}}
        miners = {"blocks": 3_000, "stale": 26, "unobserved": 0, "pairs": [
            {"loser": "no body", "winner": "zkcodexcoder", "kind": "no_body", "n": 5}], "miners": [
            {**miner, "miner": "zkcodexcoder", "canonical": 2_900, "stale": 20, "tag": "mined by zkcodexcoder",
             "by_phase": {**steady, "steady": {"canonical": 2_900, "stale": 20}}},
            {**miner, "miner": "no body", "stale": 5, "templates": {"no body": 5},
             "by_phase": {**steady, "steady": {"canonical": 0, "stale": 5}}},
            {**miner, "miner": "shielded:notag", "canonical": 100, "stale": 1,
             "by_phase": {**steady, "steady": {"canonical": 100, "stale": 1}}},
        ]}
        reset = {"hash": analysis_hash("reset"), "time": int(PAGE_NOW) - 160_000, "miner": "shielded:notag",
                 "template": "zakura", "gap": 451, "next_dt": 3, "forward_dating": 150.0, "d_pre": 9.69,
                 "orphans": 0, "unobserved": 0}
        routes = {
            "/api/summary": {"periods": {"24h": {"resets": 0, "forks": 1, "reorgs": 0}}, "phase": STEADY_PHASE,
                             "last_reset": {**reset, "height": 4_464_670, "fast_blocks": 356, "cycle_blocks": None,
                                            "era": "pre-nu7"},
                             "deepest_reorg_24h": None},
            "/api/sawtooth": {"blocks": [{"height": 4_471_000 - i, "time": int(PAGE_NOW) - 25 * i, "dt": 25,
                                          "difficulty": 10_000.0, "k": 6_972 - i, "fast": False, "min_diff": False,
                                          "orphans": 0} for i in (1, 0)],
                              "resets": [], "tip_phase": STEADY_PHASE, "averaging_window": 102, "target_spacing": 25},
            "/api/orphans/stats": {"totals": {}, "by_k": [
                {"key": ">=401", "blocks": 900, "orphans": 70, "rate": 0.0778, "share": 0.77, "forks": 60,
                 "forks_per_1000": 66.7},
                {"key": "no reset since NU7", "blocks": 6_600, "orphans": 21, "rate": 0.0032, "share": 0.23,
                 "forks": 20, "forks_per_1000": 3.0}], "by_hour": []},
            "/api/forks": {"forks": forks},
            "/api/miners": miners,
            "/api/resets": {"resets": [
                {**reset, "height": 4_464_670, "fast_blocks": None, "cycle_blocks": None},
                {**reset, "height": 4_464_635, "fast_blocks": 35, "cycle_blocks": 35, "never_slowed": True},
                {**reset, "height": 4_464_600, "fast_blocks": 12, "cycle_blocks": 35, "never_slowed": False}]},
            "/api/probes": {"probes": 10, "since": PAGE_NOW - 86_400, "by_group": [], "reprobe_by_group": [],
                            "by_reason": [{"reason": "fetch", "probes": 5, "block": 1, "notfound": 4, "timeout": 0}],
                            "incident_count": 2, "incidents": [
                                {"at": PAGE_NOW - 60, "source": "p2p:9.9.9.9:18233", "group": "zebra 7.0",
                                 "hash": analysis_hash(f"incident{i}"), "height": height, "canonical": None,
                                 "reason": "announce", "peer_tip_hash": None}
                                for i, height in enumerate((None, 4_400_000))]},
            "/api/propagation": {"blocks": 1, "sightings": 10, "since": PAGE_NOW - 21_600, "recent": [], "by_group": [
                {"group": "zebra 7.0", "kind": "p2p", "sources": 3, "sightings": 10, "first": 1, "p50_s": 0.5,
                 "p90_s": 1.0, "max_s": 2.0, "coverage": 0.33}]},
            "/api/crosscheck": {"sources": ["cipherscan"], "from_height": 4_441_000, "to_height": 4_470_997,
                                "totals": {"both": 9, "only_theirs": 0, "only_ours": 10, "theirs_canonical": 1,
                                           "out_of_window": 852, "unwatched": 0, "seen_unfetched": 4},
                                "by_day": [{"day": "2026-10-05", "start": 1_791_158_400, "both": 1, "only_theirs": 0,
                                            "seen_unfetched": 4, "only_ours": 2, "theirs_canonical": 0,
                                            "unwatched": 0}],
                                "only_theirs": [{"source": "cipherscan", "hash": analysis_hash("theirs"),
                                                 "height": 4_470_000, "time": None, "miner_address": "tmMinerAddress"}],
                                "only_ours": []},
        }
        ids = ("tile-resets", "saw-facts", "phase-chart", "forks-table", "miners-table", "miner-pairs",
               "resets-table", "probes-reasons", "incidents-table", "propagation-table", "crosscheck-facts",
               "crosscheck-days", "crosscheck-theirs")
        seen = render_page(self, routes, now=PAGE_NOW, dump=ids, actions=[{"id": "forks-table", "cls": "clickable"}])
        dump = seen["dump"]
        self.assertIn("last 4,464,670 · 1d 20h ago (pre-NU7)", dump["tile-resets"]["text"])
        facts = dump["saw-facts"]["text"]
        for part in ("no reset since NU7", "k = 6,972 blocks since the last (pre-NU7) reset",
                     "none: no reset since NU7", "4,464,670before the plotted range (pre-NU7)"):
            self.assertIn(part, facts)
        self.assertIn("no reset", dump["phase-chart"]["byClass"]["axis-text"])

        forks_seen = dump["forks-table"]
        for label in ("work on arrival", "unresolved", "more work", "greater hash", "first seen", "hash + first seen"):
            self.assertIn(label, forks_seen["byClass"]["badge"])
        self.assertIn("longest losing branch, in blocks", forks_seen["titles"])
        self.assertIn("mined by zkcodexcoder", forks_seen["titles"])
        drawer = forks_seen["byClass"]["drawer-inner"]
        self.assertEqual(len(drawer), 1)
        for part in ("Loser 1 · body not served", "(240 ms after the winner)", "not adopted by any vantage point",
                     "1 out, 1 in · 1m 30s ago · 3 reorgs"):
            self.assertIn(part, drawer[0])
        self.assertIn("body not served", forks_seen["byClass"]["cell-main"][1])

        miners_text = dump["miners-table"]["text"]
        self.assertIn("Steady stale", miners_text)
        self.assertIn("mined by zkcodexcoder", dump["miners-table"]["titles"])
        self.assertNotIn("Fast stale", miners_text)
        self.assertNotIn("Slow stale", miners_text)
        self.assertIn("body not served", miners_text)
        self.assertIn("unidentified shielded (Zakura template)", miners_text)
        self.assertNotIn("shielded:notag", miners_text)
        self.assertIn("body not served → zkcodexcoder × 5", dump["miner-pairs"]["text"])

        resets_seen = dump["resets-table"]["byClass"]
        self.assertIn("all 35", resets_seen["col-num"])
        self.assertIn("12", resets_seen["col-num"])
        self.assertEqual(resets_seen["warn-text"].count("fast"), 1)  # only the open cycle
        self.assertIn("body fetch (not availability)", dump["probes-reasons"]["text"])
        self.assertEqual(dump["incidents-table"]["byClass"]["badge"], ["never received", "unknown"])
        self.assertIn("1/3 of their peers", " ".join(dump["propagation-table"]["titles"]))

        facts = dump["crosscheck-facts"]["text"]
        for part in ("Seen, not fetched4", "not seen here at all", "CipherScan’s list"):
            self.assertIn(part, facts)
        self.assertNotIn("Out of window", facts)
        self.assertNotIn("852", facts)
        self.assertIn("Seen, not fetched", dump["crosscheck-days"]["text"])
        self.assertNotIn("Miner address", dump["crosscheck-theirs"]["text"])
        self.assertNotIn("tmMinerAddress", dump["crosscheck-theirs"]["text"])


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
