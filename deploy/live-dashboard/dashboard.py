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
MAX_BLOCKS = 4096
CUMULATIVE_KEYS = ("halo2_ps", "sapling_ps", "tx_verified_ps", "tx_failed_ps", "zakura_first_ps", "legacy_first_ps")
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
    "legacy_unready": "pool_num_unready",
    "handshakes": "crawler_in_flight_handshakes",
    "compaction_pending_bytes": "zakura_state_rocksdb_compaction_pending_bytes",
    "pipeline_memory_bytes": "sync_block_active_pipeline_decoded_attributed_memory_bytes",
    "header_budget_used": "sync_header_chunk_budget_owned",
    "header_budget_capacity": "sync_header_chunk_budget_capacity",
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
    "tx_queued_ps": "mempool_queued_transactions_total",
    "tx_downloaded_ps": "mempool_downloaded_transactions_total",
    "tx_pushed_ps": "mempool_pushed_transactions_total",
    "tx_failed_ps": "mempool_failed_verify_tasks_total",
    "blocks_verified_ps": "zcash_chain_verified_block_total",
    "native_requests_ps": "sync_block_request_sent",
    "native_bodies_ps": "sync_block_body_received",
    "dial_started_ps": "zakura_p2p_discovery_dial_started",
    "dial_succeeded_ps": "zakura_p2p_discovery_dial_succeeded",
    "dial_failed_ps": "zakura_p2p_discovery_dial_failed",
    "native_accepted_ps": "zakura_p2p_conn_accepted",
    "native_closed_ps": "zakura_p2p_conn_closed_neutral",
    "native_duplicate_ps": "zakura_p2p_conn_duplicate",
    "legacy_handshake_failed_ps": "zcash_net_peer_handshake_failures_total",
    "messages_in_ps": "zcash_net_in_messages",
    "messages_out_ps": "zcash_net_out_messages",
}
TIMINGS = {
    "writer_queue_ms": "state_block_writer_queue_duration_seconds",
    "contextual_ms": "state_contextual_total_duration_seconds",
    "write_ms": "zakura_state_rocksdb_batch_commit_duration_seconds",
    "submit_queue_ms": "sync_block_submit_queue_wait_seconds",
}
STAGE_TIMINGS = {
    "Submit queue": "sync_block_submit_queue_wait_seconds",
    "Writer queue": "state_block_writer_queue_duration_seconds",
    "Contextual validation": "state_contextual_total_duration_seconds",
    "Initial checks": "state_contextual_initial_checks_duration_seconds",
    "Transparent spends": "state_contextual_transparent_spend_duration_seconds",
    "Shielded anchors": "state_contextual_shielded_anchors_duration_seconds",
    "Parallel state update": "state_contextual_parallel_update_duration_seconds",
    "RocksDB write": "zakura_state_rocksdb_batch_commit_duration_seconds",
}
WANTED = set(GAUGES.values()) | set(COUNTERS.values()) | set(TIMINGS.values())
WANTED |= {"zakura_build_info", "zakurad_build_info", "sync_block_first_received_count",
           "zakura_consensus_batch_duration_seconds", "rpc_request_duration_seconds",
           "zakura_p2p_queue_depth", "zakura_p2p_stream_accepted"}
WANTED.update(STAGE_TIMINGS.values())
WANTED.update(name + "_count" for name in set(STAGE_TIMINGS.values()) | set(TIMINGS.values()))
WANTED.add("zakura_consensus_batch_duration_seconds_count")


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


def event_interval(previous, current, start, end, same_instance=True):
    """Only count observed increases across a bounded, uninterrupted interval."""
    usable = same_instance and start is not None and 0 < end - start <= 45
    counts = {key: value - previous[key] if usable and number(value) is not None
              and number(previous.get(key)) is not None and value >= previous[key] else None
              for key, value in current.items()}
    return {"start": start, "end": end, "counts": counts}


def period_activity(samples, start, end):
    """Sum whole observed intervals, without estimating at boundaries or over gaps."""
    totals, coverage, seen = {}, {}, {}
    for sample in samples:
        for key in CUMULATIVE_KEYS:
            sample["count_" + key] = None
        for source, interval in sample.pop("event_intervals", {}).items():
            first, last = interval.get("start"), interval.get("end")
            if (number(first) is None or number(last) is None or first < start or last > end
                    or not 0 < last - first <= 45 or first < seen.get(source, start)):
                continue
            seen[source] = last
            for key, count in interval["counts"].items():
                if number(count) is None or count < 0:
                    continue
                totals[key] = totals.get(key, 0) + count
                item = coverage.setdefault(key, {"seconds": 0, "first": first, "last": last})
                item["seconds"] += last - first
                item["last"] = last
                if key in CUMULATIVE_KEYS:
                    sample["count_" + key] = totals[key]
    return {"start": start, "end": end, "totals": totals, "coverage": coverage}


def node_instance(service):
    """Keep systemd's activation identity private; it distinguishes node restarts."""
    try:
        result = subprocess.run(["systemctl", "show", "--property=InvocationID", "--value", "--", service],
                                capture_output=True, text=True, check=True, timeout=1)
        identity = result.stdout.strip()
        return identity if re.fullmatch(r"[0-9a-f]{32}", identity) else None
    except (OSError, subprocess.SubprocessError):
        return None


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


def chain_activity(blocks, tip):
    """Measure up to 30 linked block intervals, excluding the oldest boundary block."""
    by_hash = {b["hash"]: b for b in blocks if b.get("canonical") is True}
    chain = []
    for _ in range(31):
        block = by_hash.get(tip)
        if not block or (chain and block["height"] != chain[-1]["height"] - 1):
            break
        chain.append(block)
        tip = block.get("previous")
    if len(chain) < 3 or any(number(b.get("time")) is None for b in chain):
        return None
    seconds = chain[0]["time"] - chain[-1]["time"]
    if seconds <= 0:
        return None
    measured = chain[:-1]
    transactions = sum(b["transactions"] for b in measured)
    user_transactions = sum(max(0, b["transactions"] - 1) for b in measured)
    return {"tps": transactions / seconds, "user_tps": user_transactions / seconds,
            "transactions": transactions, "blocks": len(measured), "seconds": seconds,
            "block_interval": seconds / len(measured), "from_height": chain[-1]["height"],
            "to_height": chain[0]["height"],
            "mean_block_bytes": sum(b.get("size") or 0 for b in measured) / len(measured)}


def host_counters():
    """Keep raw CPU and interface counters private until two observations exist."""
    result = {}
    try:
        result["boot_id"] = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        pass
    try:
        ticks = [int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
        result["cpu"] = {"total": sum(ticks), "idle": ticks[3] + ticks[4], "iowait": ticks[4]}
        result["cores"] = os.cpu_count()
    except (OSError, ValueError, IndexError):
        pass
    try:
        interfaces = {}
        for line in Path("/proc/net/dev").read_text().splitlines()[2:]:
            name, data = line.split(":", 1)
            if name.strip() == "lo":
                continue
            fields = [int(v) for v in data.split()]
            interfaces[name.strip()] = {"rx": fields[0], "tx": fields[8],
                                       "drops": fields[3] + fields[11],
                                       "errors": fields[2] + fields[10]}
        result["interfaces"] = interfaces
    except (OSError, ValueError, IndexError):
        pass
    return result


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
    return {"host": host, "service": status, "counters": host_counters()}


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.db.execute("CREATE TABLE IF NOT EXISTS samples (t REAL PRIMARY KEY, body TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS blocks (hash TEXT PRIMARY KEY, height INTEGER, body TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS latest_processing (id INTEGER PRIMARY KEY CHECK (id = 1), body TEXT NOT NULL)")
        self.db.commit()

    def save(self, sample, blocks):
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO samples VALUES (?, ?)", (sample["t"], json.dumps(sample)))
            self.db.execute("DELETE FROM samples WHERE t < ?", (time.time() - 86400,))
            for block in blocks:
                self.db.execute("INSERT OR REPLACE INTO blocks VALUES (?, ?, ?)",
                                (block["hash"], block["height"], json.dumps(block)))
            self.db.execute("DELETE FROM blocks WHERE hash NOT IN (SELECT hash FROM blocks ORDER BY height DESC LIMIT ?)", (MAX_BLOCKS,))

    def history(self, window, end=None):
        end = time.time() if end is None else end
        with self.lock:
            rows = self.db.execute("SELECT body FROM samples WHERE t >= ? AND t <= ? ORDER BY t", (end - window, end)).fetchall()
        samples = [json.loads(r[0]) for r in rows]
        # Older samples did not record counts, so their duplicate status is unknowable.
        for sample in samples:
            if sample.get("timing_observations_version") != 2:
                for key in list(sample):
                    if key.startswith(("stage_", "crypto_")) or key in TIMINGS:
                        sample[key] = None
        return samples

    def blocks(self):
        with self.lock:
            return [json.loads(r[0]) for r in self.db.execute("SELECT body FROM blocks ORDER BY height DESC LIMIT ?", (MAX_BLOCKS,))]

    def processing(self):
        with self.lock:
            row = self.db.execute("SELECT body FROM latest_processing WHERE id = 1").fetchone()
        return json.loads(row[0]) if row else {"stages": [], "verifiers": []}

    def save_processing(self, readings):
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO latest_processing VALUES (1, ?)",
                            (json.dumps(readings, allow_nan=False),))


class Collector:
    def __init__(self, args):
        self.args = args
        self.store = Store(args.history)
        self.lock = threading.Lock()
        self.state = {"node": args.node, "chain": {}, "metrics": {}, "host": {},
                      "host_mode": "fleet" if getattr(args, "fleet", None) else "local",
                      "node_service": None,
                      "transaction_flow": [], "stage_timings": [], "messages": [],
                      "event_intervals": {},
                      "last_processing": self.store.processing(),
                      "streams": [], "peer_details": [], "peer_latency": {},
                      "peers": [], "fleet": {}, "reorgs": [], "rpc_methods": [], "verifiers": [],
                      "version": None, "sources": {}, "blocks": self.store.blocks()}
        self.previous_metrics = None
        self.previous_metrics_at = None
        self.previous_instance = None
        self.previous_events = {}
        self.previous_host = None
        self.previous_host_at = None
        self.metric_attempt = self.host_attempt = self.peer_attempt = 0
        self.last_save = 0
        self.last_timing_sample_at = None
        self.new_timing_series = set()
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
        service = getattr(self.args, "node_service", "zakura-dashboard-node.service")
        before = node_instance(service) if not getattr(self.args, "fleet", None) else None
        metrics = metrics_parse(fetch(self.args.metrics).decode("utf-8", "replace"))
        after = node_instance(service) if not getattr(self.args, "fleet", None) else None
        return metrics, before if before == after else None

    def collect_host(self):
        if self.args.fleet:
            return json.loads(fetch(self.args.fleet + "/data/node/" + urllib.parse.quote(self.args.node)))
        return local_host(self.args.node_disk, self.args.node_service)

    def update_metrics(self, metrics, now, instance=None):
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
        values["tx_timeout_ps"] = rate(metric(old, "mempool_failed_verify_tasks_total", reason="timeout"),
                                       metric(metrics, "mempool_failed_verify_tasks_total", reason="timeout"), seconds)
        self.state["transaction_flow"] = [
            {"name": title, "rate": values[key], "total": metric(metrics, COUNTERS[key])}
            for title, key in (("Queued", "tx_queued_ps"), ("Downloaded", "tx_downloaded_ps"),
                               ("Pushed directly", "tx_pushed_ps"), ("Verified", "tx_verified_ps"),
                               ("Advertised", "tx_relayed_ps"), ("Failed tasks", "tx_failed_ps"),
                               ("Oversize rejection", "tx_policy_rejected_ps"))]
        self.state["stage_timings"] = [
            {"name": title, "p50_ms": quantile(metrics, name, "0.5"), "p95_ms": quantile(metrics, name)}
            for title, name in STAGE_TIMINGS.items()]
        # Only protocol command labels leave the collector, never arbitrary error strings.
        commands = sorted({tags["command"] for name in ("zcash_net_in_messages", "zcash_net_out_messages")
                           for tags, _ in metrics.get(name, []) if re.fullmatch(r"[a-z0-9_]{1,32}", tags.get("command", ""))})
        self.state["messages"] = [{"name": command, **{
            direction: rate(metric(old, name, command=command), metric(metrics, name, command=command), seconds)
            for direction, name in (("in_ps", "zcash_net_in_messages"), ("out_ps", "zcash_net_out_messages"))}}
            for command in commands[:40]]
        self.state["streams"] = [{"name": name,
            "last_depth": metric(metrics, "zakura_p2p_queue_depth", stream_kind=name),
            "accepted_ps": rate(metric(old, "zakura_p2p_stream_accepted", stream_kind=name),
                                metric(metrics, "zakura_p2p_stream_accepted", stream_kind=name), seconds)}
            for name in ("header_sync", "block_sync", "gossip", "discovery", "legacy_request")]
        self.state["metrics"] = values
        verifiers = sorted({tags.get("verifier") for tags, _ in metrics.get("zakura_consensus_batch_duration_seconds", []) if tags.get("verifier")})
        self.state["verifiers"] = [{"name": name, "p50_ms": quantile(metrics, "zakura_consensus_batch_duration_seconds", "0.5", verifier=name, result="success"),
                                    "p95_ms": quantile(metrics, "zakura_consensus_batch_duration_seconds", verifier=name, result="success")}
                                   for name in verifiers[:20]]
        # Counter increases distinguish new work from repeated rolling summaries.
        self.new_timing_series = set()
        if instance is not None and instance == self.previous_instance and 0 < seconds <= 45:
            series = [(name, {}) for name in set(STAGE_TIMINGS.values()) | set(TIMINGS.values())]
            series += [("zakura_consensus_batch_duration_seconds", {"verifier": name, "result": "success"})
                       for name in verifiers[:20]]
            for name, labels in series:
                before = metric(old, name + "_count", **labels)
                after = metric(metrics, name + "_count", **labels)
                if before is not None and after is not None and after > before:
                    self.new_timing_series.add((name, labels.get("verifier")))
        self.remember_processing(now)
        methods = sorted({tags.get("method") for tags, _ in metrics.get("rpc_request_duration_seconds", []) if tags.get("method")})
        self.state["rpc_methods"] = [{"name": name, "p95_ms": quantile(metrics, "rpc_request_duration_seconds", method=name),
                                     "rps": rate(metric(old, "rpc_requests_total", method=name), metric(metrics, "rpc_requests_total", method=name), seconds)} for name in methods[:60]]
        events = {key: metric(metrics, name) for key, name in COUNTERS.items() if not key.endswith("_bps")}
        events.update({source + "_first_ps": metric(metrics, "sync_block_first_received_count", source=source)
                       for source in ("zakura", "legacy")})
        events.update({f"message.{direction}.{command}": metric(metrics, name, command=command)
                       for command in commands[:40]
                       for direction, name in (("in", "zcash_net_in_messages"), ("out", "zcash_net_out_messages"))})
        events.update({"stream." + row["name"]: metric(metrics, "zakura_p2p_stream_accepted", stream_kind=row["name"])
                       for row in self.state["streams"]})
        events.update({"rpc." + method: metric(metrics, "rpc_requests_total", method=method)
                       for method in methods[:60] if re.fullmatch(r"[A-Za-z0-9_]{1,64}", method)})
        self.state["event_intervals"]["metrics"] = event_interval(
            self.previous_events, events, self.previous_metrics_at, now,
            instance is not None and instance == self.previous_instance)
        self.previous_events, self.previous_instance = events, instance
        for name in ("zakura_build_info", "zakurad_build_info"):
            if metrics.get(name):
                self.state["version"] = metrics[name][0][0].get("version", "")[:80]
        self.previous_metrics, self.previous_metrics_at = metrics, now
        self.source("metrics", True, now)

    def remember_processing(self, now):
        """Keep historical readings separate from live summaries and chart samples."""
        saved = self.state["last_processing"]
        latest = {}
        for kind, rows in (("stages", self.state["stage_timings"]), ("verifiers", self.state["verifiers"])):
            retained = {row["name"]: row for row in saved[kind]
                        if 0 <= now - row["observed_at"] < 86400}
            for row in rows:
                if number(row.get("p95_ms")) is not None and row["p95_ms"] >= 0:
                    retained[row["name"]] = {**row, "observed_at": now}
            latest[kind] = sorted(retained.values(), key=lambda row: -row["observed_at"])[:20]
        if latest != saved:
            self.store.save_processing(latest)
            self.state["last_processing"] = latest

    def update_host(self, result, now):
        host = result["host"]
        current = result.get("counters", {})
        old = self.previous_host or {}
        seconds = now - self.previous_host_at if self.previous_host_at else 0
        host.update({"cpu_percent": None, "iowait_percent": None,
                     "host_rx_bps": None, "host_tx_bps": None,
                     "host_drops_ps": None, "host_errors_ps": None,
                     "cpu_cores": current.get("cores")})
        if 0 < seconds <= 120 and "cpu" in old and "cpu" in current:
            delta = {k: current["cpu"][k] - old["cpu"][k] for k in ("total", "idle", "iowait")}
            if delta["total"] > 0 and all(v >= 0 for v in delta.values()):
                host["cpu_percent"] = max(0, min(100, 100 * (1 - delta["idle"] / delta["total"])))
                host["iowait_percent"] = min(100, 100 * delta["iowait"] / delta["total"])
        interfaces = current.get("interfaces", {})
        previous = old.get("interfaces", {})
        if interfaces and interfaces.keys() == previous.keys():
            for field, key in (("rx", "host_rx_bps"), ("tx", "host_tx_bps"),
                               ("drops", "host_drops_ps"), ("errors", "host_errors_ps")):
                rates = [rate(previous[name][field], data[field], seconds) for name, data in interfaces.items()]
                if all(v is not None for v in rates):
                    host[key] = sum(rates)
        events, old_events = {}, {}
        for field in ("drops", "errors"):
            key = "host_" + field
            if interfaces and interfaces.keys() == previous.keys() and all(
                    data[field] >= previous[name][field] for name, data in interfaces.items()):
                events[key] = sum(data[field] for data in interfaces.values())
                old_events[key] = sum(data[field] for data in previous.values())
        self.state["event_intervals"]["host"] = event_interval(old_events, events, self.previous_host_at, now,
            bool(current.get("boot_id")) and current.get("boot_id") == old.get("boot_id"))
        self.previous_host, self.previous_host_at = current, now
        self.state["host"] = host
        self.state["node_service"] = result["service"]
        self.source("host", True, now)

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
        details = [{"agent": str(p.get("subver") or "Unknown")[:100],
                    "inbound": bool(p.get("inbound")), "version": number(p.get("version")),
                    "ping_ms": number(p.get("pingtime")) * 1000 if number(p.get("pingtime")) is not None and p["pingtime"] >= 0 else None,
                    "ping_wait_ms": number(p.get("pingwait")) * 1000 if number(p.get("pingwait")) is not None and p["pingwait"] >= 0 else None}
                   for p in peers]
        pings = sorted(p["ping_ms"] for p in details if p["ping_ms"] is not None)
        percentile = lambda q: pings[max(0, math.ceil(len(pings) * q) - 1)] if pings else None
        self.state["peer_latency"] = {"p50_ms": percentile(0.5), "p95_ms": percentile(0.95),
                                      "measured": len(pings), "unknown": len(peers) - len(pings)}
        self.state["peer_details"] = sorted(details, key=lambda p: (p["ping_ms"] is None, -(p["ping_ms"] or 0)))[:200]
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
        expected_height = chain["height"]
        for _ in range(MAX_BLOCKS):
            if not HASH.fullmatch(str(tip)):
                break
            if tip not in known:
                if number(chain.get("prune_height")) is not None and expected_height < chain["prune_height"]:
                    break
                if fetched >= 4 or time.monotonic() - started >= 2:
                    break
                block = block_public(self.rpc("getblock", [tip, 1]), now if tip == chain["hash"] else None)
                if block["hash"] != tip:
                    raise ValueError("block hash mismatch")
                known[tip] = block
                fetched += 1
            block = known[tip]
            ancestry.add(tip)
            expected_height = block["height"] - 1
            tip = block["previous"]
        heights = {known[h]["height"] for h in ancestry}
        for block in known.values():
            block["canonical"] = (True if block["hash"] in ancestry else
                                  False if block["height"] in heights or block["height"] > chain["height"] else None)
        with self.lock:
            self.state["blocks"] = sorted(known.values(), key=lambda b: (b["height"], b["canonical"] is True), reverse=True)[:MAX_BLOCKS]

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
                        self.update_metrics(result[0], now, result[1])
                    elif key == "host":
                        if self.args.fleet:
                            self.update_fleet(result, now)
                        else:
                            self.update_host(result, now)
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
        activity = state.get("chain_activity") or {}
        sample.update({key: activity.get(key) for key in ("tps", "user_tps", "block_interval")})
        sample["peer_p50_ms"] = state.get("peer_latency", {}).get("p50_ms") if fresh("peers") else None
        new_scrape = fresh("metrics") and self.previous_metrics_at != self.last_timing_sample_at
        self.last_timing_sample_at = self.previous_metrics_at
        timing_observed = lambda name, verifier=None: new_scrape and (name, verifier) in self.new_timing_series
        sample["timing_observations_version"] = 2
        for key, name in TIMINGS.items():
            if not timing_observed(name):
                sample[key] = None
        for stage in state["stage_timings"]:
            if stage["name"] in STAGE_TIMINGS:
                key = stage["name"].lower().replace(" ", "_")
                for percentile in ("p50_ms", "p95_ms"):
                    sample[f"stage_{key}_{percentile}"] = (
                        stage.get(percentile) if timing_observed(STAGE_TIMINGS[stage["name"]]) else None)
        for verifier in state["verifiers"]:
            if verifier["name"] in ("halo2", "groth16_sapling", "ed25519", "redpallas", "redjubjub"):
                for percentile in ("p50_ms", "p95_ms"):
                    sample[f"crypto_{verifier['name']}_{percentile}"] = (
                        verifier.get(percentile) if timing_observed("zakura_consensus_batch_duration_seconds", verifier["name"]) else None)
        sample["event_intervals"] = {key: value for key, value in state["event_intervals"].items() if fresh(key)}
        return sample

    def snapshot(self):
        with self.lock:
            state = deepcopy(self.state)
        now = time.time()
        for kind, readings in state["last_processing"].items():
            state["last_processing"][kind] = [row for row in readings
                if 0 <= now - row["observed_at"] < 86400]
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
        state["chain_activity"] = (chain_activity(state["blocks"], state["chain"].get("hash"))
                                   if state["sources"].get("chain", {}).get("fresh")
                                   and state["sources"].get("blocks", {}).get("fresh") else None)
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
            state.pop("event_intervals", None)
            self.send(200, state)
        elif parsed.path == "/api/history":
            query = urllib.parse.parse_qs(parsed.query)
            window = query.get("window", ["1h"])[0]
            if window not in WINDOWS:
                return self.send(400, {"error": "invalid window"})
            now = time.time()
            try:
                end = float(query.get("end", [now])[0])
                if number(end) is None or end < 0 or end > now + 30:
                    raise ValueError("invalid end")
            except (ValueError, TypeError):
                return self.send(400, {"error": "invalid end"})
            end = min(end, now)
            start = end - WINDOWS[window]
            samples = collector.store.history(WINDOWS[window], end)
            activity = period_activity(samples, start, end)
            blocks = [b for b in collector.store.blocks() if b.get("canonical") is True
                      and number(b.get("time")) is not None and start <= b["time"] <= end]
            self.send(200, {"window": window, "samples": samples, "activity": activity, "blocks": blocks})
        elif parsed.path == "/healthz":
            state = collector.snapshot()
            fresh = state["sources"].get("chain", {}).get("fresh", False)
            self.send(200 if fresh else 503, {"ready": fresh, "build": state["build"]})
        elif parsed.path in ("/", "/index.html", "/app.js", "/charts.js", "/style.css", "/favicon.svg",
                             "/vendor/uplot/uPlot.iife.min.js", "/vendor/uplot/uPlot.min.css"):
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
