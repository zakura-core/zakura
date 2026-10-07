#!/usr/bin/env python3
"""Read-only Zakura dashboard. Browsers only read bounded, cached public data."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent
MAX_RESPONSE = 16 * 1024 * 1024
HASH = re.compile(r"^[0-9a-f]{64}$")
SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\S+)?$')
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')
WINDOWS = {"15m": 900, "1h": 3600, "6h": 21600, "24h": 86400}
GAUGES = {
    "applying": "sync_block_applying",
    "unsubmitted": "sync_block_applying_unsubmitted",
    "outstanding": "sync_block_outstanding",
    "missing": "sync_block_missing_bodies",
    "reserved_bytes": "sync_block_budget_reserved_bytes",
    "reorder_bytes": "sync_block_reorder_buffered_bytes",
    "native_peers": "zakura_p2p_conn_active",
    "legacy_peers": "zcash_net_peers",
    "ready_peers": "pool_num_ready",
    "mempool_count": "zcash_mempool_size_transactions",
    "mempool_bytes": "zcash_mempool_size_bytes",
    "mempool_queued": "mempool_currently_queued_transactions",
    "db_bytes": "zakura_state_rocksdb_total_disk_size_bytes",
    "db_live_bytes": "zakura_state_rocksdb_live_data_size_bytes",
    "db_memory_bytes": "zakura_state_rocksdb_total_memory_size_bytes",
    "cache_bytes": "zakura_state_rocksdb_block_cache_usage_bytes",
    "compactions": "zakura_state_rocksdb_compaction_running",
    "rpc_active": "rpc_active_requests",
    "support_blocks": "end_of_support_remaining_blocks",
    "support_height": "end_of_support_last_supported_height",
    "support_enforced": "end_of_support_enforced",
}
COUNTERS = {
    "legacy_in_bps": "zcash_net_in_bytes_total",
    "legacy_out_bps": "zcash_net_out_bytes_total",
    "download_bps": "sync_block_payload_received_bytes",
    "commit_bps": "sync_block_payload_committed_bytes",
    "halo2_ps": "proofs_halo2_verified",
    "sapling_ps": "proofs_sapling_verified",
    "tx_verified_ps": "mempool_verified_transactions_total",
    "tx_policy_rejected_ps": "mempool_rejected_transactions_total",
    "tx_relayed_ps": "mempool_gossiped_transactions_total",
    "rpc_rps": "rpc_requests_total",
    "rpc_errors_ps": "rpc_errors_total",
}
TIMINGS = {
    "writer_queue_ms": "state_block_writer_queue_duration_seconds",
    "contextual_ms": "state_contextual_total_duration_seconds",
    "write_ms": "zakura_state_rocksdb_batch_commit_duration_seconds",
}
WANTED = set(GAUGES.values()) | set(COUNTERS.values()) | set(TIMINGS.values())
WANTED |= {"zakura_build_info", "zakurad_build_info", "sync_block_first_received_count",
           "zakura_consensus_batch_duration_seconds", "rpc_request_duration_seconds"}


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(value) else None


def fetch(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(url, data, {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as response:
        body = response.read(MAX_RESPONSE + 1)
    if len(body) > MAX_RESPONSE:
        raise ValueError("upstream response too large")
    return body


def metrics_parse(raw):
    """Drop all non-allowlisted series, including peer address labels."""
    result = {}
    for line in raw.splitlines():
        if not line or line[0] == "#":
            continue
        match = SAMPLE.match(line)
        if not match or match[1] not in WANTED:
            continue
        try:
            value = number(float(match[3]))
        except ValueError:
            continue
        if value is not None:
            labels = dict(LABEL.findall(match[2] or ""))
            result.setdefault(match[1], []).append((labels, value))
    return result


def metric(metrics, name, **labels):
    values = [v for tags, v in metrics.get(name, [])
              if all(tags.get(k) == str(w) for k, w in labels.items())]
    return sum(values) if values else None


def quantile(metrics, name, q="0.95", **labels):
    # Never sum quantiles across method/verifier labels. Each row is one series.
    # metrics-exporter-prometheus renders an empty rolling summary as all zeroes.
    maxima = [v for tags, v in metrics.get(name, [])
              if tags.get("quantile") == "1"
              and all(tags.get(k) == str(w) for k, w in labels.items())]
    if maxima == [0]:
        return None
    values = [v for tags, v in metrics.get(name, [])
              if tags.get("quantile") == q
              and all(tags.get(k) == str(w) for k, w in labels.items())]
    return values[0] * 1000 if len(values) == 1 else None


def rate(previous, current, seconds):
    if previous is None or current is None or not 0 < seconds <= 120:
        return None
    return (current - previous) / seconds if current >= previous else None


def chain_public(info):
    if info.get("chain") != "main" or not HASH.fullmatch(str(info.get("bestblockhash", ""))):
        raise ValueError("expected mainnet chain with a block hash")
    height = number(info.get("blocks"))
    headers = number(info.get("headers"))
    if height is None or headers is None:
        raise ValueError("missing chain heights")
    frontier = info.get("header_chain") or {}
    finalized = (frontier.get("finalized") or {}).get("height")
    upgrades = list((info.get("upgrades") or {}).values())
    active = [u for u in upgrades if u.get("status") == "active"]
    pending = [u for u in upgrades if u.get("status") == "pending"]
    return {
        "height": height, "headers": headers, "hash": info["bestblockhash"],
        "finalized": number(finalized), "lag": max(0, headers - height),
        "pruned": info.get("pruned"), "prune_height": number(info.get("pruneheight")),
        "db_bytes": number(info.get("size_on_disk")),
        "difficulty": number(info.get("difficulty")),
        "supply": number((info.get("chainSupply") or {}).get("chainValue")),
        "pools": [{"name": p["id"], "zec": number(p.get("chainValue"))}
                  for p in info.get("valuePools", [])
                  if p.get("id") in {"transparent", "sprout", "sapling", "orchard", "ironwood", "lockbox"}],
        "upgrade": max(active, key=lambda u: u.get("activationheight", 0)).get("name") if active else None,
        "next_upgrade": ({k: min(pending, key=lambda u: u.get("activationheight", 0)).get(k)
                          for k in ("name", "activationheight")} if pending else None),
        "resource_stalled": bool((frontier.get("alarms") or {}).get("resource_stalled")),
        "body_unavailable": bool((frontier.get("alarms") or {}).get("header_best_body_unavailable")),
    }


def block_public(block, observed=None):
    if not HASH.fullmatch(str(block.get("hash", ""))):
        raise ValueError("missing block hash")
    txs = block.get("tx", [])
    return {"hash": block["hash"], "height": number(block.get("height")),
            "time": number(block.get("time")), "transactions": len(txs),
            "size": number(block.get("size")), "observed_at": observed,
            "previous": block.get("previousblockhash"), "canonical": None,
            "trees": {k: number(v.get("size")) for k, v in (block.get("trees") or {}).items()
                      if k in {"sapling", "orchard", "ironwood"}}}


def local_host(disk_path, service):
    """Read Linux host counters without opening the node's database or logs."""
    memory = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        if key in {"MemTotal", "MemAvailable"}:
            memory[key] = int(value.split()[0]) * 1024
    disk = os.statvfs(disk_path)
    load = os.getloadavg()
    host = {"mem_total_bytes": memory.get("MemTotal"),
            "mem_available_bytes": memory.get("MemAvailable"),
            "disk_total_bytes": disk.f_blocks * disk.f_frsize,
            "disk_free_bytes": disk.f_bavail * disk.f_frsize,
            "load1": load[0], "load5": load[1], "load15": load[2],
            "uptime_seconds": float(Path("/proc/uptime").read_text().split()[0]),
            "rss_bytes": None, "restart_count": None, "oom_kills_24h": None}
    status = None
    try:
        result = subprocess.run(
            ["systemctl", "show", "--property=LoadState,ActiveState,MainPID,NRestarts", "--", service],
            capture_output=True, text=True, check=True, timeout=3)
        unit = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if unit.get("LoadState") == "loaded":
            if unit.get("ActiveState") in {"active", "activating", "deactivating", "inactive", "failed", "reloading"}:
                status = unit["ActiveState"]
            host["restart_count"] = int(unit["NRestarts"])
            pid = int(unit["MainPID"])
            if pid > 0:
                for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        host["rss_bytes"] = int(line.split()[1]) * 1024
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        # Service queries and process reads can race a node restart.
        pass
    return {"host": host, "service": status}


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.db.execute("CREATE TABLE IF NOT EXISTS samples (t REAL PRIMARY KEY, body TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS blocks (hash TEXT PRIMARY KEY, height INTEGER, body TEXT NOT NULL)")
        self.db.commit()

    def save(self, sample, blocks):
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO samples VALUES (?, ?)", (sample["t"], json.dumps(sample)))
            self.db.execute("DELETE FROM samples WHERE t < ?", (time.time() - 86400,))
            for block in blocks:
                self.db.execute("INSERT OR REPLACE INTO blocks VALUES (?, ?, ?)",
                                (block["hash"], block["height"], json.dumps(block)))
            self.db.execute("DELETE FROM blocks WHERE hash NOT IN (SELECT hash FROM blocks ORDER BY height DESC LIMIT 100)")

    def history(self, window):
        with self.lock:
            rows = self.db.execute("SELECT body FROM samples WHERE t >= ? ORDER BY t", (time.time() - window,)).fetchall()
        return [json.loads(r[0]) for r in rows]

    def blocks(self):
        with self.lock:
            return [json.loads(r[0]) for r in self.db.execute("SELECT body FROM blocks ORDER BY height DESC LIMIT 100")]


class Collector:
    def __init__(self, args):
        self.args = args
        self.store = Store(args.history)
        self.lock = threading.Lock()
        self.state = {"node": args.node, "chain": {}, "metrics": {}, "host": {},
                      "host_mode": "fleet" if getattr(args, "fleet", None) else "local",
                      "node_service": None,
                      "peers": [], "fleet": {}, "reorgs": [], "rpc_methods": [], "verifiers": [],
                      "version": None, "sources": {}, "blocks": self.store.blocks()}
        self.previous_metrics = None
        self.previous_metrics_at = None
        self.metric_attempt = self.host_attempt = self.peer_attempt = 0
        self.last_save = 0
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.stop = threading.Event()

    def rpc(self, method, params=None):
        response = json.loads(fetch(self.args.rpc, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}))
        if response.get("error") or response.get("result") is None:
            raise ValueError("RPC unavailable")
        return response["result"]

    def source(self, name, ok, observed=None):
        old = self.state["sources"].get(name, {})
        self.state["sources"][name] = {"ok": ok, "attempt_at": time.time(),
                                      "at": observed if ok else old.get("at")}

    def collect_metrics(self):
        return metrics_parse(fetch(self.args.metrics).decode("utf-8", "replace"))

    def collect_host(self):
        if self.args.fleet:
            return json.loads(fetch(self.args.fleet + "/data/node/" + urllib.parse.quote(self.args.node)))
        return local_host(self.args.node_disk, self.args.node_service)

    def update_metrics(self, metrics, now):
        values = {key: metric(metrics, name) for key, name in GAUGES.items()}
        seconds = now - self.previous_metrics_at if self.previous_metrics_at else 0
        old = self.previous_metrics or {}
        for key, name in COUNTERS.items():
            values[key] = rate(metric(old, name), metric(metrics, name), seconds)
        for source in ("zakura", "legacy"):
            values[source + "_first_ps"] = rate(
                metric(old, "sync_block_first_received_count", source=source),
                metric(metrics, "sync_block_first_received_count", source=source), seconds)
        for key, name in TIMINGS.items():
            values[key] = quantile(metrics, name)
        self.state["metrics"] = values
        verifiers = sorted({tags.get("verifier") for tags, _ in metrics.get("zakura_consensus_batch_duration_seconds", []) if tags.get("verifier")})
        self.state["verifiers"] = [{"name": name, "p50_ms": quantile(metrics, "zakura_consensus_batch_duration_seconds", "0.5", verifier=name, result="success"),
                                    "p95_ms": quantile(metrics, "zakura_consensus_batch_duration_seconds", verifier=name, result="success")}
                                   for name in verifiers[:20]]
        methods = sorted({tags.get("method") for tags, _ in metrics.get("rpc_request_duration_seconds", []) if tags.get("method")})
        self.state["rpc_methods"] = [{"name": name, "p95_ms": quantile(metrics, "rpc_request_duration_seconds", method=name),
                                     "rps": rate(metric(old, "rpc_requests_total", method=name), metric(metrics, "rpc_requests_total", method=name), seconds)} for name in methods[:60]]
        for name in ("zakura_build_info", "zakurad_build_info"):
            if metrics.get(name):
                self.state["version"] = metrics[name][0][0].get("version", "")[:80]
        self.previous_metrics, self.previous_metrics_at = metrics, now
        self.source("metrics", True, now)

    def update_fleet(self, data, now):
        if data.get("network") != "mainnet":
            raise ValueError("wrong network")
        node = data.get("node") or {}
        observed = number(node.get("last_seen_at"))
        if observed is None or now - observed > 120 or observed > now + 30:
            raise ValueError("stale fleet observation")
        host = node.get("host") or {}
        self.state["host"] = {key: number(host.get(key)) for key in (
            "rss_bytes", "disk_total_bytes", "disk_free_bytes", "mem_total_bytes",
            "mem_available_bytes", "load1", "load5", "load15", "uptime_seconds", "restart_count", "oom_kills_24h")}
        self.state["fleet"] = {"height": number(data.get("majority_height")), "hash": data.get("majority_hash"), "observed_at": observed}
        self.state["reorgs"] = [{key: e.get(key) for key in ("at", "from_height", "to_height", "depth", "discarded_hash", "canonical_hash")}
                                for e in data.get("reorgs", [])[:20] if not e.get("demo")]
        self.source("host", True, observed)

    def update_peers(self, peers, now):
        groups = {}
        inbound = outbound = 0
        for peer in peers:
            agent = str(peer.get("subver") or "Unknown")[:100]
            groups[agent] = groups.get(agent, 0) + 1
            if peer.get("inbound"):
                inbound += 1
            else:
                outbound += 1
        self.state["peers"] = [{"agent": k, "count": v} for k, v in sorted(groups.items(), key=lambda x: -x[1])[:30]]
        self.state["peer_summary"] = {"inbound": inbound, "outbound": outbound, "total": len(peers)}
        self.source("peers", True, now)

    def update_blocks(self, chain, now):
        with self.lock:
            blocks = deepcopy(self.state["blocks"])
        known = {b["hash"]: b for b in blocks}
        tip = chain["hash"]
        ancestry = set()
        fetched = 0
        started = time.monotonic()
        # Walk only the observed tip's ancestry. A browser never triggers RPC work.
        # On deep reorgs or catch-up, older unlinked blocks have unknown membership.
        for _ in range(100):
            if not HASH.fullmatch(str(tip)):
                break
            if tip not in known:
                if fetched >= 4 or time.monotonic() - started >= 2:
                    break
                block = block_public(self.rpc("getblock", [tip, 1]), now if tip == chain["hash"] else None)
                if block["hash"] != tip:
                    raise ValueError("block hash mismatch")
                known[tip] = block
                fetched += 1
            block = known[tip]
            ancestry.add(tip)
            tip = block["previous"]
        heights = {known[h]["height"] for h in ancestry}
        for block in known.values():
            block["canonical"] = (True if block["hash"] in ancestry else
                                  False if block["height"] in heights or block["height"] > chain["height"] else None)
        with self.lock:
            self.state["blocks"] = sorted(known.values(), key=lambda b: (b["height"], b["canonical"] is True), reverse=True)[:100]

    def tick(self):
        now = time.time()
        tasks = {"chain": self.pool.submit(self.rpc, "getblockchaininfo")}
        if now - self.metric_attempt >= 15:
            self.metric_attempt = now
            tasks["metrics"] = self.pool.submit(self.collect_metrics)
        if now - self.host_attempt >= 30:
            self.host_attempt = now
            tasks["host"] = self.pool.submit(self.collect_host)
        if now - self.peer_attempt >= 30:
            self.peer_attempt = now
            tasks["peers"] = self.pool.submit(self.rpc, "getpeerinfo")
        results = {}
        for key, future in tasks.items():
            try:
                results[key] = future.result(timeout=6)
            except Exception:
                results[key] = None
        # All mutation happens on this one worker. HTTP reads copy under the lock.
        with self.lock:
            for key, result in results.items():
                try:
                    if result is None:
                        raise ValueError("source unavailable")
                    if key == "chain":
                        self.state["chain"] = chain_public(result)
                        self.source("chain", True, now)
                    elif key == "metrics":
                        self.update_metrics(result, now)
                    elif key == "host":
                        if self.args.fleet:
                            self.update_fleet(result, now)
                        else:
                            self.state["host"] = result["host"]
                            self.state["node_service"] = result["service"]
                            self.source("host", True, now)
                    elif key == "peers":
                        self.update_peers(result, now)
                except Exception:
                    self.source(key, False)
        # Block RPCs stay outside the state lock so slow upstreams don't block readers.
        if self.state["sources"].get("chain", {}).get("ok"):
            try:
                self.update_blocks(self.state["chain"], now)
                with self.lock:
                    self.source("blocks", True, now)
            except Exception:
                with self.lock:
                    self.source("blocks", False)
        if now - self.last_save >= 15:
            sample = self.sample(now)
            self.store.save(sample, self.state["blocks"])
            self.last_save = now

    def sample(self, now):
        state = self.snapshot()
        fresh = lambda source: state["sources"].get(source, {}).get("fresh", False)
        sample = {"t": now, "height": state["chain"].get("height") if fresh("chain") else None,
                  "lag": state["chain"].get("lag") if fresh("chain") else None}
        sample.update({k: v if fresh("metrics") else None for k, v in state["metrics"].items()})
        sample.update({k: v if fresh("host") else None for k, v in state["host"].items()})
        return sample

    def snapshot(self):
        with self.lock:
            state = deepcopy(self.state)
        now = time.time()
        for name, source in state["sources"].items():
            age = now - source["at"] if source.get("at") else None
            source["age"] = age
            source["fresh"] = bool(source["ok"] and age is not None and 0 <= age < (25 if name == "chain" else 120))
        # The node refreshes its remaining-block gauge on a slower support-check loop.
        # Derive this display from the supported height and the current verified tip.
        height = state["chain"].get("height")
        supported = state["metrics"].get("support_height")
        state["metrics"]["support_blocks"] = (supported - height
            if state["sources"].get("chain", {}).get("fresh")
            and number(height) is not None and number(supported) is not None else None)
        state["generated_at"] = now
        state["sample_interval"] = 15
        state["build"] = self.args.build
        return state

    def run(self):
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                self.tick()
            except Exception:
                logging.exception("collector cycle failed")
            self.stop.wait(max(1, 5 - (time.monotonic() - started)))


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 32

    def __init__(self, address, collector):
        self.slots = threading.BoundedSemaphore(24)
        self.collector = collector
        super().__init__(address, Handler)

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


class Handler(BaseHTTPRequestHandler):
    server_version = "ZakuraDashboard"

    def setup(self):
        super().setup()
        self.connection.settimeout(5)

    def send(self, status, body, content_type="application/json"):
        if not isinstance(body, bytes):
            body = json.dumps(body, allow_nan=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store" if content_type == "application/json" else "public, max-age=60")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        collector = self.server.collector
        if parsed.path == "/api/overview":
            state = collector.snapshot()
            state["blocks"] = state["blocks"][:30]
            self.send(200, state)
        elif parsed.path == "/api/history":
            window = urllib.parse.parse_qs(parsed.query).get("window", ["1h"])[0]
            if window not in WINDOWS:
                return self.send(400, {"error": "invalid window"})
            samples = collector.store.history(WINDOWS[window])
            self.send(200, {"window": window, "samples": samples})
        elif parsed.path == "/healthz":
            state = collector.snapshot()
            fresh = state["sources"].get("chain", {}).get("fresh", False)
            self.send(200 if fresh else 503, {"ready": fresh, "build": state["build"]})
        elif parsed.path in ("/", "/index.html", "/app.js", "/style.css", "/favicon.svg"):
            name = "index.html" if parsed.path == "/" else parsed.path[1:]
            kind = {"html": "text/html", "js": "text/javascript", "css": "text/css", "svg": "image/svg+xml"}[name.rsplit(".", 1)[1]]
            self.send(200, (ROOT / "static" / name).read_bytes(), kind)
        else:
            self.send(404, {"error": "not found"})

    def log_message(self, *_):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8095)
    parser.add_argument("--rpc", default="http://127.0.0.1:8232")
    parser.add_argument("--metrics", default="http://127.0.0.1:9999/metrics")
    parser.add_argument("--fleet", help="Optional fleet host observations instead of local Linux counters")
    parser.add_argument("--node", default="dashboard-node")
    parser.add_argument("--node-service", default="zakura-dashboard-node.service")
    parser.add_argument("--node-disk", default="/", help="Mount containing the node state")
    parser.add_argument("--history", default="dashboard.sqlite3")
    parser.add_argument("--build", default="development")
    args = parser.parse_args()
    collector = Collector(args)
    threading.Thread(target=collector.run, daemon=True).start()
    server = Server((args.host, args.port), collector)
    try:
        server.serve_forever()
    finally:
        collector.stop.set()
        server.server_close()


if __name__ == "__main__":
    main()
