#!/usr/bin/env python3
"""Zakura fleet dashboard and narrow public Ironwood and NU7 status APIs.

Reads a deploy/deployer nodes TOML, polls each node over SSH, and serves a small
HTML dashboard showing the running commit, Zakura node ID, restart time, current
height, latest block hash, tip agreement across the fleet, per-node reorg
candidates, and whether the node has advanced recently. It also serves a
deliberately small public Ironwood status response for zakura.com.

With --nu7-config it also serves the NU7 fork's public `/v1/status` (schema
version 1) for zakura.com/nu7/. A node's optional `monitor` table selects how it
is reached: `local = true` probes it on this host without SSH, and `status_url`
reads a remote node's deploy/nu7-fork/miner/remote-status.py report instead.
`profiler_url` names the node's continuous CPU profiler (deploy/profiler/): the
fleet table marks such a node as profiled and its `/node/<name>` page shows its
flame graphs, read through `/profile/<name>/api/`.

Only the Python stdlib is used.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import hmac
import html
import ipaddress
import json
import logging
import math
import os
import statistics
import re
import secrets
import shlex
import subprocess
import sys
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


IRONWOOD_ACTIVATION_HEIGHTS = {
    "mainnet": 3_428_143,
    "testnet": 4_134_000,
}
RPC_CHAIN_NAMES = {
    "mainnet": "main",
    "testnet": "test",
}
PUBLIC_STATUS_MAX_AGE = 120.0
PUBLIC_RATE_LIMIT = 120
PUBLIC_RATE_WINDOW = 60.0
PUBLIC_RATE_CLIENT_LIMIT = 4_096
PUBLIC_SCHEMA_VERSION = 1
PUBLIC_ERROR_MESSAGE = "A fresh Ironwood status is not available."
# The public routes are reachable from the internet through a reverse proxy, on
# hosts that also run nodes. Bound the threads they can hold, and how long a client
# that stalls mid-request keeps one.
MAX_CONCURRENT_REQUESTS = 16
REQUEST_TIMEOUT_SECONDS = 10
# A node's continuous profiler (deploy/profiler/) answers on loopback. Its status is
# read on every poll; the rest is forwarded only when someone opens the profile page.
PROFILER_STATUS_TIMEOUT_SECONDS = 2
PROFILER_REQUEST_TIMEOUT_SECONDS = 8
PROFILER_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
PROFILER_MAX_PARAM_LENGTH = 400
# The cluster view (`/propagation`) asks every profiler for this many recent blocks, and
# one round of answers serves every request for this long.
PROPAGATION_BLOCKS = 200
PROPAGATION_CACHE_SECONDS = 10
PROPAGATION_PAGE = Path(__file__).resolve().with_name("propagation.html")
# The profiler requests the profile page may make, and the parameters each accepts.
PROFILER_API = {
    "status": (),
    "flame": ("range", "from", "to", "view", "lens", "focus"),
    "timeline": ("range", "from", "to"),
    "blocks": ("limit",),
}
PUBLIC_ORIGINS = {
    "mainnet": frozenset({"https://zakura.com"}),
    "testnet": frozenset({
        "https://zakura.com",
        "http://127.0.0.1:1111",
        "http://localhost:1111",
    }),
}
# Per-node sparkline retention. Kept in memory only: --state-file carries the
# durable orphan-pair and stall history, and losing sparklines across a
# dashboard restart is acceptable.
DEFAULT_NODE_HISTORY_WINDOW = 3 * 60 * 60
# Rendering the exposition costs the node real CPU, and that cost grows with
# peer-labelled counter cardinality: ~0.03s on a freshly restarted node, ~0.6s
# after several days. Rather than a fixed cadence, back off in proportion to
# what the last scrape actually cost, so a cheap endpoint refreshes every poll
# and an expensive one cannot burn more than about 1/COST_FACTOR of a core.
METRICS_COST_FACTOR = 30
MAX_METRICS_INTERVAL = 120.0
# Deep per-node fields. Carried on every row so the node view can read them, but
# stripped from the fleet-wide /data payload, which the Slack watchdog polls.
NODE_DETAIL_KEYS = (
    "host",
    "metrics",
    "metrics_error",
    "metrics_version",
    "metrics_bytes",
    "metrics_series",
    "metrics_scrape_seconds",
    "metrics_at",
    "health_endpoint",
    "health_endpoint_error",
    "log_errors",
    "peer_subversions",
    "peer_user_agents",
    "peer_info_error",
    "mempool_size",
    "mempool_bytes",
    "node_errors",
    "node_errors_at",
)
# Fixed numeric fields copied into each fleet row for stall alerts. The
# watchdog gets these fields from the same collector snapshot as the node
# condition, so it never needs to fetch the larger per-node detail payload.
ALERT_DIAGNOSTIC_METRICS = (
    "sync_estimated_distance_to_tip",
    "sync_estimated_network_tip_height",
    "checkpoint_processing_next_height",
    "checkpoint_verified_height",
    "state_finalized_block_height",
    "sync_header_chain_frontier_header_best_height",
    "sync_header_chain_frontier_verified_best_height",
    "sync_header_chain_frontier_finalized_height",
    "sync_header_work_last_progress_age_seconds",
    "sync_header_work_oldest_missing_height",
    "sync_header_work_in_flight_count",
    "sync_block_applying",
    "sync_zakura_apply_operations",
    "sync_zakura_apply_in_flight",
    "sync_zakura_apply_oldest_seconds",
    "sync_zakura_apply_phase",
    "sync_block_best_header_tip_height",
    "sync_block_verified_tip_height",
    "sync_block_fill_stop",
    "sync_block_outstanding",
    "sync_block_missing_bodies",
    "state_vct_root_stalled_height",
    "state_vct_root_repair_requested",
    "state_vct_root_retry_count",
    "state_vct_aux_sweep_frontier_height",
    "sync_header_vct_repair_requested_total",
    "sync_header_vct_repair_scheduled_total",
    "sync_header_vct_repair_admitted_total",
    "sync_header_vct_repair_context_unavailable_total",
    "sync_header_vct_repair_timed_out_total",
    "sync_header_vct_repair_resource_stalled_total",
)
RECENT_REORG_LIMIT = 40
# Sampled best-chain ancestor offsets used to estimate fork depth between tips.
ANCESTOR_DEPTHS = (1, 2, 5, 10, 32)

SSH_COMMON_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=15",
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ServerAliveInterval=30",
]

DEFAULTS = {
    "probe_kind": "zebra",
    "service_name": "zakurad",
    "bin_path": "/usr/local/bin/zakurad",
    "config_path": "/etc/zakura/zakura.toml",
    "log_file": "/var/log/zakura/zakura.log",
    "state_cache_dir": "/var/lib/zakura",
    "network": "Mainnet",
    "listen_addr": "[::]:8233",
    "rpc_listen_addr": "",
    "rpc_auth": "",
    "rpc_config_path": "",
    "rpc_user": "",
    "rpc_password": "",
    "process_pattern": "",
    "container_name": "",
    "port": None,
    # Node-local observability endpoints, both loopback-bound and unauthenticated.
    # The probe reads them from inside the node, so they never need exposing.
    "metrics_endpoint": "",
    "health_listen_addr": "",
    # Monitoring-only metadata, ignored by deploy.py. See the module docstring.
    "monitor": None,
    "internal_miner": False,
}
MONITOR_KEYS = {"local", "status_url", "label", "id", "region", "profiler_url"}


@dataclass
class Node:
    name: str
    ssh_string: str
    probe_kind: str
    service_name: str
    bin_path: str
    log_file: str
    rpc_listen_addr: str
    rpc_auth: str
    rpc_config_path: str
    rpc_user: str
    rpc_password: str
    process_pattern: str
    container_name: str
    node_id: str
    metrics_endpoint: str = ""
    health_listen_addr: str = ""
    state_cache_dir: str = ""
    port: object = None
    # On the collector's own host: probed without SSH.
    local: bool = False
    # A remote node's health report URL, polled instead of SSH.
    status_url: str = ""
    label: str = ""
    miner_id: str = ""
    region: str = ""
    internal_miner: bool = False
    # Where this node's continuous CPU profiler answers, when it runs one.
    profiler_url: str = ""

    def ssh_cmd(self, *remote: str) -> list[str]:
        if self.local:
            return list(remote)
        cmd = ["ssh", *SSH_COMMON_OPTS]
        if self.port:
            cmd += ["-p", str(self.port)]
        return [*cmd, self.ssh_string, *remote]


def load_nodes(config_path: Path) -> list[Node]:
    with config_path.open("rb") as fh:
        data = tomllib.load(fh)

    defaults = dict(DEFAULTS)
    defaults.update(data.get("defaults", {}))
    node_ids_by_host = zakura_node_ids_by_host(defaults.get("zakura"))

    nodes = []
    seen = set()
    for raw in data.get("nodes", []):
        for required in ("name", "ssh_string"):
            if required not in raw:
                raise SystemExit(f"node missing required field '{required}': {raw}")
        name = raw["name"]
        if name in seen:
            raise SystemExit(f"duplicate node name: {name}")
        seen.add(name)

        merged = dict(defaults)
        merged.update(raw)
        monitor = merged.get("monitor") or {}
        if not isinstance(monitor, dict) or set(monitor) - MONITOR_KEYS:
            raise SystemExit(f"{name}: monitor must be a table of {sorted(MONITOR_KEYS)}")
        status_url = monitor.get("status_url", "")
        if status_url and (monitor.get("local")
                           or not re.fullmatch(r"https?://[A-Za-z0-9.:\[\]-]+/[A-Za-z0-9/._-]*",
                                               status_url)):
            raise SystemExit(f"{name}: status_url must be an http(s) URL on a remote node")
        profiler_url = monitor.get("profiler_url", "")
        if profiler_url and not re.fullmatch(r"https?://[A-Za-z0-9.:\[\]-]+", profiler_url):
            raise SystemExit(f"{name}: profiler_url must be an http(s) origin without a path")
        nodes.append(
            Node(
                name=name,
                ssh_string=merged["ssh_string"],
                probe_kind=merged["probe_kind"],
                service_name=merged["service_name"],
                bin_path=merged["bin_path"],
                log_file=merged["log_file"],
                rpc_listen_addr=merged["rpc_listen_addr"],
                rpc_auth=merged["rpc_auth"],
                rpc_config_path=merged["rpc_config_path"],
                rpc_user=merged["rpc_user"],
                rpc_password=merged["rpc_password"],
                process_pattern=merged["process_pattern"],
                container_name=merged["container_name"],
                node_id=node_ids_by_host.get(ssh_host(merged["ssh_string"]), ""),
                metrics_endpoint=merged["metrics_endpoint"],
                health_listen_addr=merged["health_listen_addr"],
                state_cache_dir=merged["state_cache_dir"],
                port=merged["port"],
                local=monitor.get("local") is True,
                status_url=status_url,
                label=str(monitor.get("label") or ""),
                miner_id=str(monitor.get("id") or ""),
                region=str(monitor.get("region") or ""),
                internal_miner=merged["internal_miner"] is True,
                profiler_url=profiler_url,
            )
        )

    if not nodes:
        raise SystemExit(f"no [[nodes]] defined in {config_path}")
    return nodes


def fetch_profiler(profiler_url: str, path: str, timeout: float) -> tuple[int, bytes, str]:
    """Ask a node's profiler for `path`; returns `(status, body, content_type)`.

    A profiler that cannot be reached, or answers with more than the size cap, is
    reported as a 502 so the page can say so instead of hanging.
    """
    try:
        with urllib.request.urlopen(profiler_url + path, timeout=timeout) as response:
            body = response.read(PROFILER_MAX_RESPONSE_BYTES + 1)
            status, content_type = response.status, response.headers.get("Content-Type", "")
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read(PROFILER_MAX_RESPONSE_BYTES), "application/json"
    except (OSError, ValueError):
        return 502, b'{"error":"profiler unreachable"}', "application/json"
    if len(body) > PROFILER_MAX_RESPONSE_BYTES:
        return 502, b'{"error":"profiler response too large"}', "application/json"
    return status, body, content_type


def profiler_summary(profiler_url: str) -> dict:
    """The few facts about a node's profiler that the fleet table shows."""
    status, body, _ = fetch_profiler(profiler_url, "/api/status", PROFILER_STATUS_TIMEOUT_SECONDS)
    try:
        report = json.loads(body) if status == 200 else {}
    except ValueError:
        report = {}
    if not isinstance(report, dict) or not isinstance(report.get("state"), str):
        return {"state": "unreachable"}
    summary = {"state": report["state"][:40]}
    for key in ("rate_hz", "overhead_pct_last_hour", "last_window_end"):
        value = report.get(key)
        if type(value) in (int, float) and math.isfinite(value):
            summary[key] = value
    return summary


class PropagationView:
    """Each block's path through the fleet, joined from the profilers' own records.

    Every node with a profiler is asked for its recent blocks, and `join`
    (`propagation.join`, in the module beside this script) puts them on one time axis.
    Requests that arrive while a round is under way wait for it instead of starting
    another.

    The profilers' records name the peers a node downloaded from by address. The join
    is told which addresses are private (`private_addresses`), and keeps neither the
    address nor anything else about such a peer.
    """

    def __init__(self, nodes: list[Node], join):
        self.nodes = [node for node in nodes if node.profiler_url]
        # Every node in the list is ours, with a profiler or not, so a download from
        # any of them is shown under its name.
        self.fleet_addresses = {ssh_host(node.ssh_string): node.name for node in nodes}
        self.join = join
        self._lock = threading.Lock()
        self._joined: dict | None = None
        self._fetched_at = 0.0
        # The private list the cached view was built with: a peer is classified once,
        # so a view built before an address became private must not be served after.
        self._joined_for: frozenset | None = None

    def fetch_report(self, node: Node) -> dict | None:
        """One node's recent blocks from its profiler, or `None` when it did not answer."""
        status, body, _ = fetch_profiler(
            node.profiler_url,
            f"/api/propagation?limit={PROPAGATION_BLOCKS}",
            PROFILER_REQUEST_TIMEOUT_SECONDS,
        )
        try:
            report = json.loads(body) if status == 200 else None
        except ValueError:
            report = None
        return report if isinstance(report, dict) else None

    def data(self) -> dict:
        """The joined view, at most `PROPAGATION_CACHE_SECONDS` old and built with the
        current private address list.

        Raises `ValueError` or `OSError` when the list of private addresses cannot be
        read: without it, a private peer cannot be told from any other.
        """
        with self._lock:
            # A queued request must not rebuild the cache with a privacy list it
            # read before another request installed a newer view.
            protected = private_addresses()

            def is_private(ip: str) -> bool:
                return address_key(ip) in protected

            if (
                self._joined is None
                or self._joined_for != protected
                or time.monotonic() - self._fetched_at >= PROPAGATION_CACHE_SECONDS
            ):
                reports: dict[str, dict | None] = {}
                if self.nodes:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(self.nodes))) as pool:
                        reports = dict(zip(
                            (node.name for node in self.nodes), pool.map(self.fetch_report, self.nodes)
                        ))
                self._joined = self.join(
                    reports,
                    {node.name: node.node_id for node in self.nodes},
                    limit=PROPAGATION_BLOCKS,
                    fleet_addresses=self.fleet_addresses,
                    is_private=is_private,
                )
                self._fetched_at, self._joined_for = time.monotonic(), protected
            return self._joined


def ssh_host(ssh_string: str) -> str:
    destination = ssh_string.rsplit("@", 1)[-1]
    destination = destination.rsplit(":", 1)[0]
    return destination.strip("[]")


def zakura_node_ids_by_host(zakura: object) -> dict[str, str]:
    if not isinstance(zakura, dict):
        return {}

    node_ids = {}
    for peer in zakura.get("bootstrap_peers", []):
        if not isinstance(peer, str) or "@" not in peer:
            continue

        node_id, address = peer.split("@", 1)
        host = address.rsplit(":", 1)[0].strip("[]")
        node_ids[host] = node_id
        try:
            node_ids[str(ipaddress.ip_address(host))] = node_id
        except ValueError:
            pass

    return node_ids


def rpc_url_for(listen_addr: str) -> str:
    if not listen_addr:
        return ""
    if listen_addr.startswith("[") and "]:" in listen_addr:
        host, _, port = listen_addr.partition("]:")
        port = port.lstrip(":")
        if not port:
            return ""
        return f"http://{host}]:{port}/"
    if ":" in listen_addr:
        host, port = listen_addr.rsplit(":", 1)
        return f"http://{host}:{port}/"
    return ""


REMOTE_PROBE = r"""
import base64
import ipaddress
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

(
    service,
    bin_path,
    log_file,
    rpc_url,
    probe_kind,
    process_pattern,
    rpc_auth,
    rpc_user,
    rpc_password,
    rpc_config_path,
    container_name,
    metrics_endpoint,
    health_listen_addr,
    state_cache_dir,
    want_metrics,
) = sys.argv[1:16]
ancestor_height = int(sys.argv[16]) if len(sys.argv) > 16 and sys.argv[16] else None

out = {
    "service": service,
    "bin_path": bin_path,
    "log_file": log_file,
    "rpc_url": rpc_url,
    "probe_kind": probe_kind,
}

# Prometheus series the per-node view actually renders. The exporter emits the
# full ~350-name surface, so filtering here keeps the ssh response a few KB
# instead of a few hundred. Names are the exporter's sanitized form ('.' -> '_').
WANTED_METRICS = frozenset((
    "sync_estimated_distance_to_tip",
    "sync_estimated_network_tip_height",
    "checkpoint_processing_next_height",
    "checkpoint_verified_height",
    "state_finalized_block_height",
    "state_vct_root_stalled_height",
    "state_vct_root_repair_requested",
    "state_vct_root_retry_count",
    "state_vct_aux_sweep_frontier_height",
    "sync_downloads_in_flight",
    "sync_downloaded_block_count",
    "sync_verified_block_count",
    "sync_header_verification_lag",
    "sync_header_headers_per_second",
    "sync_header_headers_received_total",
    "sync_header_failure_total",
    # The header-sync work-queue family, as emitted by the deployed 1.1 binaries.
    # Both families are listed so the allowlist spans a rolling upgrade; names
    # that a given build does not emit simply never match.
    "sync_header_work_last_progress_age_seconds",
    "sync_header_work_oldest_missing_age_seconds",
    "sync_header_work_oldest_missing_height",
    "sync_header_work_in_flight_count",
    "sync_header_work_pending_count",
    "sync_header_work_buffered_count",
    "sync_header_work_buffered_headers",
    "sync_header_work_committing_count",
    "sync_header_work_epoch",
    "sync_header_root_auth_lead_blocks",
    "sync_header_root_auth_work_in_flight_batches",
    "sync_header_root_auth_work_pending_batches",
    "sync_header_peer_violation",
    "sync_header_fill_stop",
    "sync_header_vct_repair_requested_total",
    "sync_header_vct_repair_scheduled_total",
    "sync_header_vct_repair_admitted_total",
    "sync_header_vct_repair_context_unavailable_total",
    "sync_header_vct_repair_timed_out_total",
    "sync_header_vct_repair_resource_stalled_total",
    "sync_block_applying",
    "sync_block_outstanding",
    "sync_block_backlog_at_cap",
    "sync_block_missing_bodies",
    "sync_block_best_header_tip_height",
    "sync_block_verified_tip_height",
    "sync_block_fill_stop",
    "sync_block_budget_reserved_bytes",
    "sync_block_reorder_buffered_bytes",
    "state_memory_best_chain_length",
    "state_memory_chain_count",
    "sync_header_chain_dag_nodes",
    "sync_header_chain_dag_leaf_tips",
    "sync_header_chain_dag_eligible_tips",
    "sync_header_chain_frontier_header_best_height",
    "sync_header_chain_frontier_verified_best_height",
    "sync_header_chain_frontier_finalized_height",
    "sync_header_chain_frontier_divergence",
    "sync_header_chain_reorg_depth",
    "sync_zakura_apply_operations",
    "sync_zakura_apply_in_flight",
    "sync_zakura_apply_oldest_seconds",
    "sync_zakura_apply_phase",
    "sync_zakura_apply_epoch",
    "sync_zakura_legacy_fallback_active",
    "zcash_net_peers",
    "zcash_net_in_bytes_total",
    "zcash_net_out_bytes_total",
    "zcash_net_peer_handshake_failures_total",
    "zakura_p2p_conn_active",
    "zakura_p2p_connected_peers",
    "zakura_p2p_healthy_peers",
    "zakura_p2p_reactor_active_connections",
    "zakura_p2p_handshake_upgrade_error",
    "pool_num_ready",
    "pool_num_unready",
    "crawler_in_flight_handshakes",
    "candidate_set_recently_live",
    "candidate_set_responded",
    "candidate_set_gossiped",
    "candidate_set_pending",
    "candidate_set_failed",
    "candidate_set_disconnected",
    "zakura_state_rocksdb_total_disk_size_bytes",
    "zcash_chain_verified_block_total",
    "zcash_mempool_size_transactions",
))
# A live mainnet/testnet exposition runs to ~12 MB because several net counters
# are labelled per peer address. This is only a runaway guard, not a budget.
MAX_METRICS_BYTES = 64 * 1024 * 1024
# Legacy peer-software breakdown. Newer zakurad omits this label; getpeerinfo
# subver is the durable source once the node exposes it.
PEER_AGENT_METRIC = "zcash_net_peers_connected"
IPV4 = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")

def run(cmd, timeout=6):
    return subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)

def process_is_running(pattern):
    if not pattern:
        return None
    proc = run(["pgrep", "-f", pattern])
    return proc.returncode == 0

def process_start_time(pattern):
    if not pattern:
        return ""
    proc = run(["pgrep", "-f", pattern])
    if proc.returncode != 0:
        return ""
    pid = proc.stdout.strip().splitlines()[0].strip()
    if not pid.isdigit():
        return ""
    ps_proc = run(["ps", "-o", "lstart=", "-p", pid])
    if ps_proc.returncode != 0:
        return ""
    return ps_proc.stdout.strip()

def container_state(name):
    if not name:
        return None, ""
    proc = run(["docker", "inspect", "--format",
                "{{.State.Status}}|{{.State.StartedAt}}", name])
    if proc.returncode != 0:
        return "missing", ""
    status, _, started_at = proc.stdout.strip().partition("|")
    return status, started_at

def container_logs(name):
    if not name:
        return ""
    proc = run(["docker", "logs", "--tail", "1000", name])
    return (proc.stdout or "") + (proc.stderr or "")

def http_get(url, timeout=4, limit=4096):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read(limit).decode("utf-8", "replace").strip()
    except urllib.error.HTTPError as error:
        try:
            body = error.read(limit).decode("utf-8", "replace").strip()
        except Exception:
            body = ""
        return error.code, body

def scrape_metrics(endpoint):
    with urllib.request.urlopen("http://" + endpoint + "/metrics", timeout=5) as response:
        raw = response.read(MAX_METRICS_BYTES).decode("utf-8", "replace")

    values = {}
    agents = {}
    version = ""
    series = 0
    for line in raw.splitlines():
        series += 1
        if not line or line.startswith("#"):
            continue
        name, brace, rest = line.partition("{")
        if brace:
            labels, _, value = rest.rpartition("}")
        else:
            name, _, value = line.partition(" ")
            labels = ""
        name = name.strip()
        if name == "zakura_build_info" and 'version="' in labels:
            version = labels.split('version="', 1)[1].split('"', 1)[0]
            continue
        if name == PEER_AGENT_METRIC and 'user_agent="' in labels:
            # Fallback for older zakurad builds that omit getpeerinfo.subver.
            agent = labels.split('user_agent="', 1)[1].split('"', 1)[0]
            try:
                agents[agent] = agents.get(agent, 0.0) + float(value)
            except ValueError:
                pass
            continue
        if name not in WANTED_METRICS:
            continue
        try:
            number = float(value)
        except ValueError:
            continue
        # A few wanted series carry labels; summing collapses them to one number.
        values[name] = values.get(name, 0.0) + number
    top_agents = sorted(
        ((agent, int(count)) for agent, count in agents.items() if count > 0),
        key=lambda item: (-item[1], item[0]),
    )[:8]
    return values, version, len(raw), series, top_agents

def read_meminfo():
    fields = {}
    with open("/proc/meminfo", encoding="utf-8") as fh:
        for line in fh:
            key, _, rest = line.partition(":")
            if key in ("MemTotal", "MemAvailable"):
                fields[key] = int(rest.split()[0]) * 1024
    return fields

def process_rss_bytes(pid):
    with open("/proc/{}/status".format(pid), encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return None

def oom_kill_count():
    proc = run([
        "journalctl", "-k", "--since", "-24 hours", "--no-pager",
        "-o", "cat", "-n", "200", "-g", "Out of memory|oom-kill|oom_reaper",
    ], timeout=12)
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if lines:
        return len(lines)
    # journalctl exits 1 when nothing matched, which is exactly the zero case.
    # Only a real failure (missing journal, bad pattern) writes to stderr.
    if proc.returncode not in (0, 1) or proc.stderr.strip():
        return None
    return 0

def log_error_tail(path, limit=5):
    if not path:
        return []
    command = "tail -c 2000000 {} 2>/dev/null | grep -aE '(ERROR|WARN)' | tail -{}".format(
        shlex.quote(path), limit
    )
    proc = run(["bash", "-lc", command], timeout=12)
    lines = []
    for raw_line in proc.stdout.splitlines():
        # Redact here rather than at the dashboard: these lines are served on a
        # public page, and peer addresses must not leave the node.
        line = IPV4.sub("x.x.x.x", raw_line.strip())
        if len(line) > 300:
            line = line[:300] + "..."
        if line:
            lines.append(line)
    return lines

def parse_zcash_conf(path):
    values = {}
    if not path:
        return values
    with open(path, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values

def rpc_headers():
    headers = {"Content-Type": "application/json"}
    user = rpc_user
    password = rpc_password
    if rpc_auth == "zcash_conf":
        try:
            config = parse_zcash_conf(rpc_config_path)
            user = config.get("rpcuser", user)
            password = config.get("rpcpassword", password)
        except Exception as error:
            out["rpc_auth_error"] = str(error)
    elif rpc_auth == "cookie":
        try:
            with open(rpc_config_path, encoding="utf-8") as fh:
                token = fh.read().strip()
            if ":" in token:
                user, password = token.split(":", 1)
            else:
                user, password = token, ""
        except Exception as error:
            out["rpc_auth_error"] = str(error)
    if rpc_auth in ("basic", "zcash_conf", "cookie") and user and password:
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    return headers

def rpc_call(method, params=None):
    body = json.dumps({
        "jsonrpc": "2.0",
        "id": "zakura-cluster-status",
        "method": method,
        "params": list(params or []),
    }).encode()
    request = urllib.request.Request(
        rpc_url,
        data=body,
        headers=rpc_headers(),
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=6) as response:
        payload = json.loads(response.read().decode())
    if payload.get("error"):
        raise RuntimeError(str(payload["error"]))
    return payload.get("result")

try:
    running = process_is_running(process_pattern)
    if running is not None:
        out["process_running"] = running
except Exception as error:
    out["process_error"] = str(error)

try:
    if container_name:
        status, started_at = container_state(container_name)
        out["active_state"] = "active" if status == "running" else status
        out["last_restarted"] = started_at
    elif service:
        proc = run(["systemctl", "show", service, "--no-pager",
                    "-p", "ActiveState",
                    "-p", "ActiveEnterTimestamp",
                    "-p", "ExecMainStartTimestamp",
                    "-p", "MainPID",
                    "-p", "NRestarts"])
        props = {}
        for line in proc.stdout.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                props[key] = value
        out["active_state"] = props.get("ActiveState") or "unknown"
        out["last_restarted"] = (
            props.get("ExecMainStartTimestamp")
            or props.get("ActiveEnterTimestamp")
            or ""
        )
        if (props.get("MainPID") or "0").isdigit() and props["MainPID"] != "0":
            out["main_pid"] = int(props["MainPID"])
        if (props.get("NRestarts") or "").isdigit():
            out["restart_count"] = int(props["NRestarts"])
    elif out.get("process_running") is True:
        out["active_state"] = "active"
        out["last_restarted"] = process_start_time(process_pattern)
    elif out.get("process_running") is False:
        out["active_state"] = "inactive"
        out["last_restarted"] = ""
    else:
        out["active_state"] = "unknown"
        out["last_restarted"] = ""
except Exception as error:
    out["active_state"] = "unknown"
    out["systemd_error"] = str(error)

try:
    command = (
        ["docker", "exec", container_name, bin_path, "--version"]
        if container_name
        else [bin_path, "--version"]
    )
    proc = run(command)
    out["version"] = (proc.stdout or proc.stderr).splitlines()[0].strip()
except Exception as error:
    out["version_error"] = str(error)

try:
    if container_name:
        logs = container_logs(container_name)
        matches = re.findall(r"git commit: ([0-9a-f]+)", logs)
        if matches:
            out["commit"] = matches[-1]
    elif log_file:
        grep = "grep -aoE 'git commit: [0-9a-f]+' {} 2>/dev/null | tail -1".format(
            shlex.quote(log_file)
        )
        proc = run(["bash", "-lc", grep])
        line = proc.stdout.strip()
        out["commit"] = line.rsplit(" ", 1)[-1] if line else ""
    if not out.get("commit") and service:
        proc = run([
            "journalctl", "-u", service, "-g", "git commit:",
            "-n", "1", "--no-pager", "-o", "cat",
        ])
        if proc.returncode == 0:
            match = re.search(r"git commit: ([0-9a-f]+)", proc.stdout)
            if match:
                out["commit"] = match.group(1)
    if not out.get("commit") and out.get("version"):
        match = re.search(r"\b([0-9a-f]{7,40})(?:-dirty)?\b", out["version"])
        if match:
            out["commit"] = match.group(1)
except Exception as error:
    out["commit_error"] = str(error)

try:
    if container_name:
        logs = container_logs(container_name)
        matches = re.findall(r'node_id=([^, ]+)', logs)
        if matches:
            out["node_id"] = matches[-1].strip('"')
    elif log_file:
        grep = "grep -aoE 'node_id=[^, ]+' {} 2>/dev/null | tail -1".format(
            shlex.quote(log_file)
        )
        proc = run(["bash", "-lc", grep])
        line = proc.stdout.strip()
        out["node_id"] = line.split("=", 1)[-1].strip('"') if line else ""
    if not out.get("node_id") and service:
        proc = run([
            "journalctl", "-u", service, "-g", "node_id=",
            "-n", "1", "--no-pager", "-o", "cat",
        ])
        if proc.returncode == 0:
            match = re.search(r"node_id=([^, ]+)", proc.stdout)
            if match:
                out["node_id"] = match.group(1).strip('"')
except Exception as error:
    out["node_id_error"] = str(error)

try:
    pid = out.get("main_pid")
    if pid is None and container_name:
        proc = run(["docker", "inspect", "--format", "{{.State.Pid}}", container_name])
        candidate = proc.stdout.strip()
        if proc.returncode == 0 and candidate.isdigit() and candidate != "0":
            pid = int(candidate)
    if pid is None and process_pattern:
        proc = run(["pgrep", "-f", process_pattern])
        if proc.returncode == 0 and proc.stdout.strip():
            candidate = proc.stdout.strip().splitlines()[0].strip()
            if candidate.isdigit():
                pid = int(candidate)

    host = {}
    if pid is not None:
        host["pid"] = pid
        try:
            host["rss_bytes"] = process_rss_bytes(pid)
        except Exception as error:
            host["rss_error"] = str(error)

    disk_path = state_cache_dir or "/"
    host["disk_path"] = disk_path
    try:
        usage = shutil.disk_usage(disk_path)
        host["disk_total_bytes"] = usage.total
        host["disk_free_bytes"] = usage.free
    except Exception as error:
        # Surface rather than silently falling back: a wrong state_cache_dir
        # would otherwise report the root filesystem's free space as the node's.
        host["disk_error"] = str(error)

    try:
        memory = read_meminfo()
        host["mem_total_bytes"] = memory.get("MemTotal")
        host["mem_available_bytes"] = memory.get("MemAvailable")
    except Exception as error:
        host["mem_error"] = str(error)

    try:
        load1, load5, load15 = os.getloadavg()
        host["load1"] = load1
        host["load5"] = load5
        host["load15"] = load15
    except Exception as error:
        host["load_error"] = str(error)

    try:
        with open("/proc/uptime", encoding="utf-8") as fh:
            host["uptime_seconds"] = float(fh.read().split()[0])
    except Exception as error:
        host["uptime_error"] = str(error)

    try:
        host["oom_kills_24h"] = oom_kill_count()
    except Exception as error:
        host["oom_error"] = str(error)

    if "restart_count" in out:
        host["restart_count"] = out["restart_count"]
    out["host"] = host
except Exception as error:
    out["host_error"] = str(error)

try:
    out["log_errors"] = log_error_tail(log_file)
except Exception as error:
    out["log_errors_error"] = str(error)

if metrics_endpoint and want_metrics:
    try:
        started = time.monotonic()
        metric_values, metric_version, raw_bytes, series, agents = scrape_metrics(
            metrics_endpoint
        )
        out["metrics"] = metric_values
        out["metrics_bytes"] = raw_bytes
        out["metrics_series"] = series
        if agents:
            out["peer_user_agents"] = agents
        out["metrics_scrape_seconds"] = round(time.monotonic() - started, 3)
        if metric_version:
            out["metrics_version"] = metric_version
    except Exception as error:
        out["metrics_error"] = str(error)
elif metrics_endpoint:
    # Throttled: rendering the exposition costs the node real CPU, so the
    # dashboard reuses its previous scrape between refreshes.
    out["metrics_skipped"] = True
else:
    out["metrics_error"] = "metrics endpoint not configured"

if health_listen_addr:
    health = {}
    for probe_path in ("healthy", "ready"):
        try:
            status, body = http_get(
                "http://" + health_listen_addr + "/" + probe_path
            )
            health[probe_path] = {"status": status, "body": body[:200]}
        except Exception as error:
            health[probe_path] = {"error": str(error)}
    out["health"] = health
else:
    out["health_error"] = "health endpoint not configured"

if rpc_url:
    try:
        blockchain_info = rpc_call("getblockchaininfo")
        if not isinstance(blockchain_info, dict):
            raise RuntimeError("getblockchaininfo returned a non-object result")

        out["height"] = blockchain_info.get("blocks")
        out["headers"] = blockchain_info.get("headers")
        out["block_hash"] = blockchain_info.get("bestblockhash")
        out["rpc_chain"] = blockchain_info.get("chain")

        ancestor_hashes = {}
        try:
            tip_height = int(blockchain_info.get("blocks"))
        except (TypeError, ValueError):
            tip_height = None
        if tip_height is not None:
            for depth in (1, 2, 5, 10, 32):
                if tip_height >= depth:
                    try:
                        ancestor_hashes[str(depth)] = rpc_call(
                            "getblockhash", [tip_height - depth]
                        )
                    except Exception:
                        pass
        out["ancestor_hashes"] = ancestor_hashes
        if ancestor_height is not None and tip_height is not None and 0 <= ancestor_height <= tip_height:
            depth = str(tip_height - ancestor_height)
            if depth not in ancestor_hashes:
                try:
                    ancestor_hashes[depth] = rpc_call("getblockhash", [ancestor_height])
                except Exception:
                    pass
        if tip_height is not None:
            try:
                if rpc_call("getblockhash", [tip_height]) != out["block_hash"]:
                    out["ancestor_hashes"] = {}
            except Exception:
                out["ancestor_hashes"] = {}

        best_hash = out.get("block_hash")
        if best_hash:
            try:
                header = rpc_call("getblockheader", [best_hash, True])
                if isinstance(header, dict):
                    out["previous_hash"] = header.get("previousblockhash") or ""
                    # Bind the target to the accepted tip, rather than a header
                    # observed at another height while the chain advances.
                    if header.get("hash") == best_hash and header.get("height") == tip_height:
                        out["difficulty_bits"] = header.get("bits")
            except Exception as error:
                out["previous_hash_error"] = str(error)

        value_pools = blockchain_info.get("valuePools")
        if not isinstance(value_pools, list):
            out["ironwood_pool_error"] = "valuePools is unavailable"
        else:
            ironwood_pool = next(
                (
                    pool
                    for pool in value_pools
                    if isinstance(pool, dict) and pool.get("id") == "ironwood"
                ),
                None,
            )
            if (
                ironwood_pool is None
                or "chainValueZat" not in ironwood_pool
            ):
                out["ironwood_pool_error"] = "Ironwood value pool is unavailable"
            else:
                out["ironwood_chain_balance_zat"] = str(
                    ironwood_pool["chainValueZat"]
                )

        # Configured-network observations for the NU7 status view.
        upgrades = blockchain_info.get("upgrades")
        for branch_id, upgrade in (upgrades.items() if isinstance(upgrades, dict) else ()):
            if isinstance(upgrade, dict) and upgrade.get("name") == "NU7":
                out["nu7"] = {
                    "branch_id": branch_id,
                    "activation_height": upgrade.get("activationheight"),
                }
        if "nsmValueBalanceZat" in blockchain_info:
            out["nsm_value_balance_zat"] = blockchain_info.get("nsmValueBalanceZat")
        if "nsmReissuanceHeight" in blockchain_info:
            out["nsm_reissuance_height"] = blockchain_info.get("nsmReissuanceHeight")
    except Exception as error:
        out["rpc_error"] = str(error)

    try:
        info = rpc_call("getinfo")
        if not isinstance(info, dict):
            raise RuntimeError("getinfo returned a non-object result")

        out["rpc_testnet"] = info.get("testnet")
        out["client_name"] = (
            "zcashd" if probe_kind == "zcashd" else "zakurad"
        )
        out["client_version"] = str(
            info.get("build") or info.get("version") or ""
        )
        # Not a curated health field: LastWarnErrorLayer publishes the most
        # recent WARN/ERROR log message from anywhere in the process, and
        # getinfo returns it verbatim. Keep the timestamp so the page can say
        # how old it is rather than implying it is a current verdict.
        out["node_errors"] = str(info.get("errors") or "")
        out["node_errors_at"] = info.get("errorstimestamp")
    except Exception as error:
        out["rpc_metadata_error"] = str(error)

    try:
        peers = rpc_call("getpeerinfo")
        if not isinstance(peers, list):
            raise RuntimeError("getpeerinfo returned a non-array result")

        # Reduce on the node: a mainnet peer set runs to hundreds of entries and
        # the dashboard only needs counts.
        subversions = {}
        inbound = 0
        external = 0
        for peer in peers:
            if not isinstance(peer, dict):
                continue
            if peer.get("inbound"):
                inbound += 1
            peer_host = str(peer.get("addr") or "").rsplit(":", 1)[0].strip("[]")
            try:
                external += not ipaddress.ip_address(peer_host).is_loopback
            except ValueError:
                pass
            key = str(peer.get("subver") or "unknown")[:48]
            subversions[key] = subversions.get(key, 0) + 1
        out["peer_count"] = len(peers)
        out["peer_inbound"] = inbound
        out["peer_external"] = external
        # Live peer software mix. zakurad omitted subver historically; once the
        # node exposes it, this restores the dashboard panel without metrics
        # labels. Until then the exporter user_agent fallback still applies.
        if set(subversions) != {"unknown"}:
            out["peer_subversions"] = sorted(
                subversions.items(), key=lambda item: (-item[1], item[0])
            )[:8]
    except Exception as error:
        out["peer_info_error"] = str(error)

    try:
        mempool = rpc_call("getmempoolinfo")
        if isinstance(mempool, dict):
            out["mempool_size"] = mempool.get("size")
            out["mempool_bytes"] = mempool.get("bytes")
    except Exception as error:
        out["mempool_error"] = str(error)
else:
    out["rpc_error"] = "RPC disabled in deployer config"

print(json.dumps(out, separators=(",", ":")))
"""


def ssh_capture_script(node: Node, script: str) -> subprocess.CompletedProcess:
    return subprocess.run(node.ssh_cmd("bash", "-s"), input=script, text=True, capture_output=True)


STATUS_REPORT_TIMEOUT = 3
STATUS_REPORT_LIMIT = 64 * 1024
STATUS_REPORT_MAX_AGE = 90
BLOCK_HASH = re.compile(r"[0-9a-f]{64}")


def status_report_probe(report: object, now: float) -> dict:
    """Map a remote-status.py report onto the probe fields the collector reads.

    The report comes from another host, so anything malformed or stale is an error
    rather than a partial observation.
    """
    try:
        observed_at = report["observedAt"]
        if (not isinstance(observed_at, (int, float))
                or not 0 <= now - observed_at <= STATUS_REPORT_MAX_AGE):
            return {"error": "stale status report"}
        accepted = report.get("acceptedBlocks24h")
        probe = {
            "active_state": "active" if report["nodeActive"] is True else "inactive",
            "client_name": "zakurad",
            "miner": {
                "active": report["minerActive"] is True,
                "accepted_blocks_24h": accepted if type(accepted) is int else None,
                "observed_at": observed_at,
            },
        }
        if report["nodeHealthy"] is not True:
            probe["rpc_error"] = "remote node RPC unavailable"
            return probe
        height = report["height"]
        recent = report["recentHashes"]
        if (type(height) is not int or height < 0 or not isinstance(recent, dict)
                or not BLOCK_HASH.fullmatch(str(report["hash"]))):
            raise ValueError("malformed tip")
        probe.update({
            "height": height,
            "block_hash": report["hash"],
            "ancestor_hashes": {
                str(depth): recent[str(height - depth)] for depth in (1, 2)
                if BLOCK_HASH.fullmatch(str(recent.get(str(height - depth), "")))
            },
            "rpc_chain": "test",
            "nu7": {"branch_id": str(report["branchId"]),
                    "activation_height": report["activationHeight"]},
        })
        return probe
    except (KeyError, TypeError, ValueError):
        return {"error": "malformed status report"}


def probe_status_endpoint(node: Node) -> dict:
    try:
        with urllib.request.urlopen(node.status_url, timeout=STATUS_REPORT_TIMEOUT) as response:
            report = json.loads(response.read(STATUS_REPORT_LIMIT))
    except (OSError, ValueError) as error:
        return {"error": f"status report unavailable: {type(error).__name__}"}
    return status_report_probe(report, time.time())


def probe_node(node: Node, want_metrics: bool = True, ancestor_height: int | None = None) -> dict:
    if node.status_url:
        return probe_status_endpoint(node)
    rpc_url = rpc_url_for(node.rpc_listen_addr)
    script = (
        "python3 - "
        f"{shlex.quote(node.service_name)} "
        f"{shlex.quote(node.bin_path)} "
        f"{shlex.quote(node.log_file)} "
        f"{shlex.quote(rpc_url)} "
        f"{shlex.quote(node.probe_kind)} "
        f"{shlex.quote(node.process_pattern)} "
        f"{shlex.quote(node.rpc_auth)} "
        f"{shlex.quote(node.rpc_user)} "
        f"{shlex.quote(node.rpc_password)} "
        f"{shlex.quote(node.rpc_config_path)} "
        f"{shlex.quote(node.container_name)} "
        f"{shlex.quote(node.metrics_endpoint)} "
        f"{shlex.quote(node.health_listen_addr)} "
        f"{shlex.quote(node.state_cache_dir)} "
        f"{shlex.quote('1' if want_metrics else '')} "
        f"{shlex.quote(str(ancestor_height) if ancestor_height is not None else '')} <<'PY'\n"
        f"{REMOTE_PROBE}\n"
        "PY\n"
    )
    proc = ssh_capture_script(node, script)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        return {"error": detail or f"ssh exited {proc.returncode}"}
    try:
        return json.loads(proc.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as error:
        return {"error": f"invalid probe output: {error}", "raw": proc.stdout.strip()}


class RateLimiter:
    """A small per-client fixed-window limiter for the public JSON endpoint."""

    def __init__(
        self,
        limit: int = PUBLIC_RATE_LIMIT,
        window: float = PUBLIC_RATE_WINDOW,
    ):
        self.limit = limit
        self.window = window
        self.lock = threading.Lock()
        self.events: dict[str, deque[float]] = {}

    def allow(self, client: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        cutoff = now - self.window
        with self.lock:
            if (
                client not in self.events
                and len(self.events) >= PUBLIC_RATE_CLIENT_LIMIT
            ):
                self.events = {
                    key: events
                    for key, events in self.events.items()
                    if events and events[-1] > cutoff
                }
                if len(self.events) >= PUBLIC_RATE_CLIENT_LIMIT:
                    return False

            events = self.events.setdefault(client, deque())
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= self.limit:
                return False
            events.append(now)
            return True


PRIVATE_ADDRESS_FILE = Path("/etc/zakura-mainnet-dashboard/private/addresses.json")


def address_key(value):
    address = ipaddress.ip_address(value)
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    return address.version, address.packed


def private_addresses():
    try:
        with PRIVATE_ADDRESS_FILE.open("rb") as stream:
            data = json.loads(stream.read(4097))
    except FileNotFoundError:
        if os.environ.get("ZAKURA_MAC_CRANELIFT_STATUS") == "1":
            raise ValueError("private address configuration required") from None
        return frozenset()
    if not isinstance(data, list) or not 1 <= len(data) <= 16 or any(not isinstance(item, str) for item in data):
        raise ValueError("invalid private address configuration")
    return frozenset(address_key(item) for item in data)


def monitoring_snapshot(value, node_names):
    """Retain typed fleet observations when address redaction cannot be configured.

    Arbitrary strings and nested diagnostic objects never cross this boundary.
    Detailed endpoints remain unavailable until privacy configuration is repaired.
    """
    def number(item):
        return type(item) in (int, float) and math.isfinite(item) and item >= 0

    def block_hash(item):
        return isinstance(item, str) and re.fullmatch(r"[0-9a-fA-F]{64}", item) is not None

    names = {name for name in node_names if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", name)}
    names.add("mac-os-cranelift")
    rows = []
    for row in value.get("rows", []):
        if not isinstance(row, dict) or row.get("name") not in names:
            continue
        health = row.get("health")
        if health not in {"healthy", "stale", "verification_error", "rpc_error", "down", "starting"}:
            health = "down"
        safe = {"name": row["name"], "health": health, "healthy": health == "healthy",
                "detail": "Limited status: privacy configuration unavailable"}
        if type(row.get("height")) is int and row["height"] >= 0:
            safe["height"] = row["height"]
        for field in ("seconds_since_advanced", "observed_at"):
            if number(row.get(field)):
                safe[field] = row[field]
        for field in ("block_hash", "previous_hash"):
            if block_hash(row.get(field)):
                safe[field] = row[field]
        bits = row.get("difficulty_bits")
        if isinstance(bits, str) and re.fullmatch(r"[0-9a-fA-F]{8}", bits):
            safe["difficulty_bits"] = bits
        safe["rpc_ok"] = row.get("rpc_ok") is True
        if row.get("rpc_chain") in {"main", "test", "regtest"}:
            safe["rpc_chain"] = row["rpc_chain"]
        for field in ("last_seen_at", "last_advanced_at"):
            if number(row.get(field)):
                safe[field] = row[field]
        ancestors = row.get("ancestor_hashes")
        if isinstance(ancestors, dict):
            safe["ancestor_hashes"] = {depth: value for depth, value in ancestors.items()
                if isinstance(depth, str) and depth.isdecimal() and len(depth) <= 10
                and 0 < int(depth) <= 0xFFFFFFFF and block_hash(value)}
        rows.append(safe)
    result = {"rows": rows, "total": len(rows),
              "healthy": sum(row["healthy"] for row in rows), "chain": {}}
    for field in ("last_poll", "generated_at", "stale_after"):
        if number(value.get(field)):
            result[field] = value[field]
    if value.get("network") in {"mainnet", "testnet"}:
        result["network"] = value["network"]
    return result


def redact_private_addresses(value, protected):
    """Remove only protected Mac addresses, including from Linux peer diagnostics."""
    if isinstance(value, dict):
        return {redact_private_addresses(key, protected): redact_private_addresses(item, protected)
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact_private_addresses(item, protected) for item in value]
    if not isinstance(value, str):
        return value

    def ipv4(match):
        literal = match.group()
        try:
            if address_key(literal) in protected:
                return "[redacted-address]"
        except ValueError:
            pass
        return literal

    # An invalid outer IPv6 token must not swallow a protected dotted address.
    value = re.sub(r"(?:\d{1,3}\.){3}\d{1,3}", ipv4, value)

    # Overlapping bounded windows prevent a long hexadecimal prefix from
    # consuming the beginning of an expanded protected IPv6 address.
    pattern = r"(?<![a-fA-F0-9])(?=((?:[a-fA-F0-9]{0,4}:){2,8}[a-fA-F0-9]{0,4}(?:%[\w.-]{1,32})?))"
    parts, previous = [], 0
    for match in re.finditer(pattern, value):
        if match.start() < previous:
            continue
        literal = match.group(1)
        starts = [0] + [i + 1 for i, char in enumerate(literal) if char == ":"]
        ends = sorted([len(literal)] + [i for i, char in enumerate(literal) if char == ":"], reverse=True)
        found = None
        for start in starts:
            for end in ends:
                candidate = literal[start:end]
                if end <= start or candidate.count(":") < 2:
                    continue
                try:
                    if address_key(candidate) in protected:
                        found = (match.start() + start, match.start() + end)
                        break
                except ValueError:
                    pass
            if found is not None:
                break
        if found is not None:
            start, end = found
            parts.extend((value[previous:start], "[redacted-address]"))
            previous = end
    parts.append(value[previous:])
    return "".join(parts)


def mac_cranelift_status() -> dict:
    """Read only the comparator's sanitized file, never the private comparison state."""
    try:
        with Path("/var/lib/zakura-mac-cranelift-public/status.json").open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError("oversized status")
        data = json.loads(raw)
        identifier = data.get("verifier_id")
        if not isinstance(identifier, str) or not re.fullmatch(r"verifier-[a-f0-9]{32}", identifier):
            raise ValueError("invalid verifier identifier")
        result = {"verifier_id": identifier, "available": True}
        for key in ("sample_time", "compared_through", "mac_tip", "node_rss_bytes", "free_disk_bytes"):
            value = data.get(key)
            result[key] = value if type(value) in (int, float) and math.isfinite(value) else None
        for key in ("alerts_muted",):
            result[key] = data.get(key) is True
        for key in ("mac_tip_hash", "source_sha"):
            value = data.get(key)
            size = 64 if key == "mac_tip_hash" else 40
            result[key] = value if isinstance(value, str) and re.fullmatch(r"[a-fA-F0-9]{%d}" % size, value) else ""
        result["comparison_healthy"] = data.get("condition") == "matching"
        ancestors = data.get("ancestor_hashes") or {}
        result["ancestor_hashes"] = {depth: value for depth, value in ancestors.items()
            if isinstance(depth, str) and depth.isdecimal() and len(depth) <= 10
            and 0 < int(depth) <= 0xFFFFFFFF
            and isinstance(value, str) and re.fullmatch(r"[a-fA-F0-9]{64}", value)} if isinstance(ancestors, dict) else {}
        stamp = result["sample_time"]
        result["available"] = stamp is not None and 0 <= time.time() - stamp <= 90
        return result
    except (OSError, ValueError, TypeError, AttributeError):
        return {"available": False}


class ClusterCollector:
    def __init__(
        self,
        nodes: list[Node],
        interval: float,
        stale_after: float,
        network: str,
        state_file: Path | None = None,
        history_window: float = DEFAULT_NODE_HISTORY_WINDOW,
        expose_logs: bool = False,
        metrics_min_interval: float | None = None,
        nu7: "Nu7Status | None" = None,
    ):
        self.nodes = nodes
        self.nu7 = nu7
        self.interval = interval
        self.stale_after = stale_after
        self.network = network
        self.state_file = state_file
        self.history_window = history_window
        self.expose_logs = expose_logs
        # None selects the adaptive interval; a number pins it.
        self.metrics_min_interval = metrics_min_interval
        self.nodes_by_name = {node.name: node for node in nodes}
        self.history: dict[str, deque[dict]] = {node.name: deque() for node in nodes}
        # Last successful scrape per node, reused on the polls that skip it.
        self.last_metrics: dict[str, dict] = {}
        self.ironwood_activation_height = IRONWOOD_ACTIVATION_HEIGHTS[network]
        self.lock = threading.Lock()
        restored_progress = load_progress(state_file)
        self.private_mac_progress = restored_progress.get("mac-os-cranelift", {})
        self.last_height: dict[str, int | None] = {
            node.name: restored_progress.get(node.name, {}).get("height") for node in nodes
        }
        self.last_block_hash: dict[str, str | None] = {node.name: None for node in nodes}
        self.last_ancestors: dict[str, dict[str, str]] = {
            node.name: {} for node in nodes
        }
        self.last_advanced_at: dict[str, float | None] = {
            node.name: restored_progress.get(node.name, {}).get("last_advanced_at")
            for node in nodes
        }
        self.recent_reorgs: deque[dict] = deque(
            load_orphan_pairs(state_file),
            maxlen=RECENT_REORG_LIMIT,
        )
        self.chain: dict = empty_chain_summary()
        self.rows: list[dict] = [
            {
                "name": node.name,
                "ssh": node.ssh_string,
                "node_id": node.node_id,
                "health": "starting",
                "healthy": False,
            }
            for node in nodes
        ]
        self.last_poll = None

    def loop(self) -> None:
        while True:
            self.poll_once()
            time.sleep(self.interval)

    def poll_once(self) -> None:
        rows = []
        started = time.time()
        private_enabled = self.network == "mainnet" and os.environ.get("ZAKURA_MAC_CRANELIFT_STATUS") == "1"
        sample = mac_cranelift_status() if private_enabled else {}
        tip = sample.get("mac_tip") if sample.get("available") else None
        ancestor_height = max(0, tip - 10) if type(tip) is int else None
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(self.nodes))) as pool:
            futures = {
                pool.submit(probe_node, node, self.should_scrape_metrics(node.name, started), ancestor_height): node
                for node in self.nodes
            }
            profilers = {
                node.name: pool.submit(profiler_summary, node.profiler_url)
                for node in self.nodes
                if node.profiler_url
            }
            for future in concurrent.futures.as_completed(futures):
                node = futures[future]
                try:
                    probe = future.result()
                except Exception as error:
                    probe = {"error": str(error)}
                row = self.row_for(node, probe, time.time())
                if node.name in profilers:
                    row["profiler"] = profilers[node.name].result()
                rows.append(row)

        if private_enabled:
            self.last_height.setdefault("mac-os-cranelift", self.private_mac_progress.get("height"))
            self.last_advanced_at.setdefault("mac-os-cranelift", self.private_mac_progress.get("last_advanced_at"))
            self.history.setdefault("mac-os-cranelift", deque())
            mac = Node(name="mac-os-cranelift", ssh_string="", node_id=sample.get("verifier_id", ""), probe_kind="private-loopback",
                       service_name="", bin_path="", log_file="", rpc_listen_addr="", rpc_auth="",
                       rpc_config_path="", rpc_user="", rpc_password="", process_pattern="", container_name="")
            probe = {"height": sample.get("mac_tip") if sample.get("available") else None,
                     "block_hash": sample.get("mac_tip_hash"), "commit": sample.get("source_sha"),
                     "ancestor_hashes": sample.get("ancestor_hashes") or {},
                     "previous_hash": (sample.get("ancestor_hashes") or {}).get("1", ""),
                     "active_state": "active" if sample.get("available") else "unknown",
                     "client_name": "Zakura macOS ARM64", "rpc_chain": "main", "rpc_testnet": False,
                     "host": {"disk_free_bytes": sample.get("free_disk_bytes"),
                              "rss_bytes": sample.get("node_rss_bytes")}}
            row = self.row_for(mac, probe, time.time())
            row["comparison_healthy"] = sample.get("comparison_healthy") is True
            if row["healthy"] and not row["comparison_healthy"]:
                row.update(healthy=False, health="verification_error", detail="Verification incomplete or inconsistent")
            row["detail"] += " · Compared through " + str(sample.get("compared_through") or "—")
            if sample.get("alerts_muted"):
                row["detail"] += " · Alerts muted"
            rows.append(row)
        rows.sort(key=lambda row: row["name"])
        now = time.time()
        with self.lock:
            recent_reorgs = list(self.recent_reorgs)
            chain = compute_chain_summary(rows, recent_reorgs)
            enrich_chain_roles(rows, chain)
            self.rows = rows
            self.chain = chain
            self.last_poll = now
            self.record_node_history(now, rows)
            snapshot = self.progress_snapshot()
        # Snapshot under the lock, write outside it: the stall timers must
        # survive a restart or every node reports as freshly advanced on the
        # next process start.
        self.persist_state(snapshot)
        if self.nu7 is not None:
            # Outside the lock: the NU7 view samples headers from the primary's RPC.
            self.nu7.update(rows, now)

    def progress_snapshot(self) -> dict[str, dict]:
        return {
            name: {
                "height": self.last_height.get(name),
                "last_advanced_at": self.last_advanced_at.get(name),
            }
            for name in self.last_advanced_at
        }

    def persist_state(self, snapshot: dict[str, dict] | None = None) -> None:
        save_orphan_pairs(
            self.state_file,
            list(self.recent_reorgs),
            self.progress_snapshot() if snapshot is None else snapshot,
        )

    def record_orphan_pair(self, event: dict) -> None:
        self.recent_reorgs.appendleft(event)
        self.persist_state()

    def should_scrape_metrics(self, name: str, now: float) -> bool:
        """Decide per node, from what its last scrape cost."""
        snapshot = self.last_metrics.get(name)
        scraped_at = (snapshot or {}).get("metrics_at")
        if scraped_at is None:
            return True
        if self.metrics_min_interval is not None:
            interval = self.metrics_min_interval
        else:
            cost = (snapshot or {}).get("metrics_scrape_seconds") or 0.0
            interval = min(cost * METRICS_COST_FACTOR, MAX_METRICS_INTERVAL)
        return now - scraped_at >= interval

    def record_node_history(self, now: float, rows: list[dict]) -> None:
        cutoff = now - self.history_window
        for row in rows:
            samples = self.history.get(row["name"])
            if samples is None:
                continue
            host = row.get("host") or {}
            metrics = row.get("metrics") or {}
            samples.append({
                "t": round(now, 1),
                "height": row.get("height"),
                "header_lag": row.get("header_lag"),
                "peers": row.get("peer_count"),
                "disk_free_bytes": host.get("disk_free_bytes"),
                "rss_bytes": host.get("rss_bytes"),
                "load1": host.get("load1"),
                "sync_lag": metrics.get("sync_estimated_distance_to_tip"),
            })
            while samples and samples[0]["t"] < cutoff:
                samples.popleft()

    def node_snapshot(self, name: str) -> dict | None:
        with self.lock:
            row = next((row for row in self.rows if row["name"] == name), None)
            if row is None:
                return None
            row = dict(row)
            samples = [dict(sample) for sample in self.history.get(name, ())]
            reorgs = [
                dict(event) for event in self.recent_reorgs if event.get("node") == name
            ]
            chain = dict(self.chain)
            last_poll = self.last_poll

        if not self.expose_logs:
            # Off by default: the dashboard is public and log lines can carry
            # operational detail even after the probe redacts addresses.
            row["log_errors"] = []
            row["log_errors_suppressed"] = True

        node = self.nodes_by_name.get(name)
        return {
            "generated_at": time.time(),
            "last_poll": last_poll,
            "stale_after": self.stale_after,
            "network": self.network,
            "history_window": self.history_window,
            "majority_height": chain.get("majority_height"),
            "majority_hash": chain.get("majority_hash"),
            "config": {
                "metrics_endpoint": node.metrics_endpoint if node else "",
                "health_listen_addr": node.health_listen_addr if node else "",
                "state_cache_dir": node.state_cache_dir if node else "",
                "probe_kind": node.probe_kind if node else "",
            },
            "node": row,
            "history": samples,
            "reorgs": reorgs,
        }

    def row_for(self, node: Node, probe: dict, now: float) -> dict:
        previous_height = self.last_height.get(node.name)
        previous_hash = self.last_block_hash.get(node.name)
        previous_ancestors = dict(self.last_ancestors.get(node.name) or {})
        height = coerce_int(probe.get("height"))
        headers = coerce_int(probe.get("headers"))
        block_hash = str(probe.get("block_hash") or "")
        ancestor_hashes = normalize_ancestor_hashes(probe.get("ancestor_hashes"))
        previous_block_hash = str(probe.get("previous_hash") or "")

        advanced = False
        tip_event = None
        if height is not None:
            tip_event = classify_tip_event(
                previous_height,
                previous_hash,
                height,
                block_hash or None,
            )
            if previous_height is None and self.last_advanced_at.get(node.name) is None:
                self.last_advanced_at[node.name] = now
            elif previous_height is not None and height > previous_height:
                self.last_advanced_at[node.name] = now
                advanced = True
            self.last_height[node.name] = height
            if block_hash:
                self.last_block_hash[node.name] = block_hash
            self.last_ancestors[node.name] = ancestor_hashes
            if tip_event in ("reorg_height_drop", "tip_switch"):
                depth_info = estimate_reorg_depth(
                    tip_event,
                    previous_height,
                    height,
                    previous_ancestors,
                    ancestor_hashes,
                )
                self.record_orphan_pair(
                    {
                        "node": node.name,
                        "kind": tip_event,
                        "from_height": previous_height,
                        "from_hash": previous_hash or "",
                        "to_height": height,
                        "to_hash": block_hash,
                        "discarded_hash": previous_hash or "",
                        "canonical_hash": block_hash,
                        "depth": depth_info.get("depth"),
                        "depth_label": depth_info.get("label") or "unknown",
                        "durable": True,
                        "demo": False,
                        "at": now,
                    }
                )

        last_advanced_at = self.last_advanced_at.get(node.name)
        seconds_since_advanced = (
            now - last_advanced_at if last_advanced_at is not None else None
        )

        active_state = probe.get("active_state") or "unknown"
        process_running = probe.get("process_running")
        service_active = active_state == "active" and process_running is not False
        rpc_ok = height is not None and not probe.get("rpc_error")
        recent = (
            seconds_since_advanced is not None
            and seconds_since_advanced <= self.stale_after
        )
        healthy = service_active and rpc_ok and recent

        if probe.get("error"):
            health = "down"
            detail = probe["error"]
        elif process_running is False:
            health = "down"
            detail = f"process not found: {node.process_pattern}"
        elif not service_active:
            health = "down"
            detail = f"systemd state: {active_state}"
        elif not rpc_ok:
            health = "rpc_error"
            detail = str(probe.get("rpc_error") or "RPC height unavailable")
        elif not recent:
            health = "stale"
            detail = "height has not advanced within stale window"
        else:
            health = "healthy"
            detail = "advanced this poll" if advanced else "height recently advanced"

        if tip_event == "reorg_height_drop":
            detail = (
                f"height dropped {previous_height} → {height} "
                f"(possible reorg); {detail}"
            )
        elif tip_event == "tip_switch":
            detail = (
                f"tip hash changed at height {height} "
                f"(possible reorg); {detail}"
            )

        header_lag = None
        if headers is not None and height is not None:
            header_lag = headers - height

        # Metrics are scraped on a slower cadence than the poll, so a skipped
        # poll reuses the last successful scrape instead of blanking the panels.
        if probe.get("metrics_skipped"):
            metrics_snapshot = self.last_metrics.get(node.name, {})
        else:
            probe_error = probe.get("error")
            metrics_snapshot = {
                "metrics": probe.get("metrics") or {},
                "metrics_error": probe.get("metrics_error") or probe_error,
                "metrics_version": probe.get("metrics_version") or "",
                "metrics_bytes": coerce_int(probe.get("metrics_bytes")),
                "metrics_series": coerce_int(probe.get("metrics_series")),
                "metrics_scrape_seconds": probe.get("metrics_scrape_seconds"),
                "peer_user_agents": probe.get("peer_user_agents") or [],
                "metrics_at": None if probe_error else now,
            }
            self.last_metrics[node.name] = metrics_snapshot

        host = probe.get("host") or {}
        vitals = {
            "disk_free_pct": disk_free_pct(host),
            "disk_free_bytes": host.get("disk_free_bytes"),
            "restart_count": host.get("restart_count"),
            "oom_kills_24h": host.get("oom_kills_24h"),
            "peer_count": coerce_int(probe.get("peer_count")),
        }

        return {
            "name": node.name,
            "ssh": node.ssh_string,
            "healthy": healthy,
            "health": health,
            "detail": detail,
            "commit": probe.get("commit") or "",
            "block_hash": block_hash,
            "previous_hash": previous_block_hash,
            "difficulty_bits": probe.get("difficulty_bits"),
            "ancestor_hashes": ancestor_hashes,
            "node_id": node.node_id or probe.get("node_id") or "",
            "version": probe.get("version") or "",
            "last_restarted": probe.get("last_restarted") or "",
            "height": height,
            "headers": headers,
            "header_lag": header_lag,
            "tip_event": tip_event,
            "chain_role": "unknown",
            "active_state": active_state,
            "rpc_ok": rpc_ok,
            "last_seen_at": now,
            "last_advanced_at": last_advanced_at,
            "seconds_since_advanced": seconds_since_advanced,
            "rpc_chain": probe.get("rpc_chain"),
            "rpc_testnet": probe.get("rpc_testnet"),
            "ironwood_chain_balance_zat": probe.get(
                "ironwood_chain_balance_zat"
            ),
            "ironwood_pool_error": probe.get("ironwood_pool_error"),
            "client_name": probe.get("client_name") or "",
            "client_version": probe.get("client_version") or "",
            "rpc_metadata_error": probe.get("rpc_metadata_error"),
            "peer_count": coerce_int(probe.get("peer_count")),
            "peer_inbound": coerce_int(probe.get("peer_inbound")),
            "vitals": vitals,
            # Deep fields; see NODE_DETAIL_KEYS. Served from /data/node/<name>.
            "host": host,
            "metrics": metrics_snapshot.get("metrics") or {},
            "metrics_error": metrics_snapshot.get("metrics_error"),
            "metrics_version": metrics_snapshot.get("metrics_version") or "",
            "metrics_bytes": metrics_snapshot.get("metrics_bytes"),
            "metrics_series": metrics_snapshot.get("metrics_series"),
            "metrics_scrape_seconds": metrics_snapshot.get("metrics_scrape_seconds"),
            "metrics_at": metrics_snapshot.get("metrics_at"),
            "peer_user_agents": metrics_snapshot.get("peer_user_agents") or [],
            # Distinct from "health" above, which is this dashboard's own
            # classification; this is what the node's /healthy and /ready say.
            "health_endpoint": probe.get("health") or {},
            "health_endpoint_error": probe.get("health_error"),
            "log_errors": probe.get("log_errors") or [],
            "peer_subversions": probe.get("peer_subversions") or [],
            "peer_info_error": probe.get("peer_info_error"),
            "mempool_size": coerce_int(probe.get("mempool_size")),
            "mempool_bytes": coerce_int(probe.get("mempool_bytes")),
            "node_errors": probe.get("node_errors") or "",
            "node_errors_at": coerce_int(probe.get("node_errors_at")),
            "peer_external": coerce_int(probe.get("peer_external")),
            "nu7": probe.get("nu7"),
            "nsm_value_balance_zat": coerce_int(probe.get("nsm_value_balance_zat")),
            "nsm_reissuance_known": "nsm_reissuance_height" in probe,
            "nsm_reissuance_height": coerce_int(probe.get("nsm_reissuance_height")),
            "miner": probe.get("miner"),
        }

    def snapshot(self) -> dict:
        """Fleet-wide payload for the table and the Slack watchdog.

        Deep per-node fields are dropped here so this stays small at a 10s poll;
        the node view reads them from /data/node/<name> instead.
        """
        with self.lock:
            rows = [
                fleet_row(row, self.last_poll)
                for row in self.rows
            ]
            last_poll = self.last_poll
            chain = dict(self.chain)
            chain["tip_groups"] = [dict(group) for group in chain.get("tip_groups", [])]
            chain["recent_reorgs"] = [
                dict(event) for event in chain.get("recent_reorgs", [])
            ]
        healthy = sum(1 for row in rows if row.get("healthy"))
        return {
            "generated_at": time.time(),
            "last_poll": last_poll,
            "stale_after": self.stale_after,
            "network": self.network,
            "healthy": healthy,
            "total": len(rows),
            "chain": chain,
            "rows": rows,
        }

    def ironwood_status(self, now: float | None = None) -> tuple[int, dict]:
        now = time.time() if now is None else now
        with self.lock:
            rows = [dict(row) for row in self.rows]

        candidates = []
        rejected = set()
        for row in rows:
            candidate, error_code = self.public_candidate(row, now)
            if candidate is not None:
                candidates.append(candidate)
            elif error_code is not None:
                rejected.add(error_code)

        if not candidates:
            error_code = next(
                (
                    code
                    for code in (
                        "network_mismatch",
                        "ironwood_pool_unavailable",
                        "source_stale",
                        "upstream_unavailable",
                    )
                    if code in rejected
                ),
                "upstream_unavailable",
            )
            return 503, public_error(self.network, error_code)

        groups = defaultdict(list)
        for candidate in candidates:
            groups[(candidate["height"], candidate["block_hash"])].append(
                candidate
            )

        _, agreed_sources = max(
            groups.items(),
            key=lambda item: (len(item[1]), item[0][0], item[0][1]),
        )
        source = min(
            agreed_sources,
            key=lambda candidate: (
                candidate["client_name"] != "zakurad",
                candidate["name"],
            ),
        )

        tip_height = source["height"]
        activated = tip_height >= self.ironwood_activation_height
        blocks_since_activation = (
            tip_height - self.ironwood_activation_height
            if activated
            else None
        )
        return 200, {
            "schema_version": PUBLIC_SCHEMA_VERSION,
            "network": self.network,
            "activation_height": self.ironwood_activation_height,
            "activated": activated,
            "tip_height": tip_height,
            "blocks_since_activation": blocks_since_activation,
            "ironwood_chain_balance_zat": source["balance_zat"],
            "updated_at": rfc3339_utc(source["observed_at"]),
            "source": {
                "client_name": source["client_name"],
                "client_version": source["client_version"],
            },
        }

    def public_candidate(
        self,
        row: dict,
        now: float,
    ) -> tuple[dict | None, str | None]:
        observed_at = row.get("last_seen_at")
        if not isinstance(observed_at, (int, float)):
            return None, "upstream_unavailable"

        expected_testnet = self.network == "testnet"
        if (
            row.get("rpc_chain") != RPC_CHAIN_NAMES[self.network]
            or row.get("rpc_testnet") is not expected_testnet
        ):
            return None, "network_mismatch"

        balance_zat = row.get("ironwood_chain_balance_zat")
        if (
            not isinstance(balance_zat, str)
            or re.fullmatch(r"0|[1-9][0-9]*", balance_zat) is None
        ):
            return None, "ironwood_pool_unavailable"

        if now - observed_at > PUBLIC_STATUS_MAX_AGE:
            return None, "source_stale"

        height = row.get("height")
        block_hash = row.get("block_hash")
        client_name = row.get("client_name")
        client_version = row.get("client_version")
        if (
            not row.get("healthy")
            or not isinstance(height, int)
            or height < 0
            or not isinstance(block_hash, str)
            or not block_hash
            or not isinstance(client_name, str)
            or not client_name
            or not isinstance(client_version, str)
            or not client_version
            or row.get("rpc_metadata_error")
        ):
            return None, "upstream_unavailable"

        return {
            "name": str(row.get("name") or ""),
            "height": height,
            "block_hash": block_hash,
            "balance_zat": balance_zat,
            "observed_at": float(observed_at),
            "client_name": client_name,
            "client_version": client_version,
        }, None

def disk_free_pct(host: dict) -> float | None:
    total = host.get("disk_total_bytes")
    free = host.get("disk_free_bytes")
    if not isinstance(total, (int, float)) or not total:
        return None
    if not isinstance(free, (int, float)):
        return None
    return round(100.0 * free / total, 1)


def coerce_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def finite_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return value if math.isfinite(value) else None
    except OverflowError:
        return None


def alert_diagnostics(row: dict, last_poll: float | None) -> dict:
    metrics = row.get("metrics")
    metrics_at = finite_number(row.get("metrics_at"))
    selected = {}
    if isinstance(metrics, dict):
        for name in ALERT_DIAGNOSTIC_METRICS:
            value = finite_number(metrics.get(name))
            if value is not None:
                selected[name] = value

    return {
        "last_poll": finite_number(last_poll),
        "metrics_at": metrics_at,
        "metrics_available": isinstance(metrics, dict)
        and metrics_at is not None
        and not bool(row.get("metrics_error")),
        "metrics": selected,
    }


def fleet_row(row: dict, last_poll: float | None) -> dict:
    summary = {
        key: value for key, value in row.items() if key not in NODE_DETAIL_KEYS
    }
    summary["alert_diagnostics"] = alert_diagnostics(row, last_poll)
    return summary


def empty_chain_summary() -> dict:
    return {
        "status": "unknown",
        "split": False,
        "majority_height": None,
        "majority_hash": "",
        "max_height": None,
        "observed_tips": 0,
        "tip_groups": [],
        "recent_reorgs": [],
    }


def normalize_ancestor_hashes(value) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out = {}
    for key, hash_value in value.items():
        if hash_value:
            out[str(key)] = str(hash_value)
    return out


def load_orphan_pairs(path: Path | None) -> list[dict]:
    if path is None or not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(payload, dict):
        events = payload.get("orphan_pairs") or payload.get("recent_reorgs") or []
    else:
        events = payload
    if not isinstance(events, list):
        return []
    cleaned = []
    for event in events:
        if isinstance(event, dict):
            cleaned.append(dict(event))
    return cleaned[:RECENT_REORG_LIMIT]


def save_orphan_pairs(
    path: Path | None,
    events: list[dict],
    progress: dict[str, dict] | None = None,
) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    payload = {
        "updated_at": time.time(),
        "orphan_pairs": list(events)[:RECENT_REORG_LIMIT],
        "progress": progress or {},
    }
    tmp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def load_progress(path: Path | None) -> dict[str, dict]:
    """Restore per-node height/last-advanced timers written by a previous process.

    Without this the stall clock restarts from zero on every dashboard restart,
    which reports every node as freshly advanced regardless of its real height.
    """
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    progress = payload.get("progress")
    if not isinstance(progress, dict):
        return {}
    restored: dict[str, dict] = {}
    for name, record in progress.items():
        if not isinstance(record, dict):
            continue
        height = record.get("height")
        advanced_at = record.get("last_advanced_at")
        restored[str(name)] = {
            "height": int(height) if isinstance(height, (int, float)) else None,
            "last_advanced_at": (
                float(advanced_at) if isinstance(advanced_at, (int, float)) else None
            ),
        }
    return restored


def estimate_fork_depth_from_ancestors(
    left: dict[str, str] | None,
    right: dict[str, str] | None,
    sampled_depths: tuple[int, ...] = ANCESTOR_DEPTHS,
) -> dict:
    """Estimate fork depth from sampled best-chain ancestor hashes.

    Returns the smallest sampled depth where both tips share the same ancestor.
    """
    left = left or {}
    right = right or {}
    for depth in sampled_depths:
        key = str(depth)
        left_hash = left.get(key)
        right_hash = right.get(key)
        if left_hash and right_hash and left_hash == right_hash:
            return {
                "depth": depth,
                "exact": True,
                "label": f"depth {depth}",
            }
    if left and right:
        ceiling = max(sampled_depths)
        return {
            "depth": None,
            "exact": False,
            "label": f"> {ceiling} or unknown",
        }
    return {"depth": None, "exact": False, "label": "unknown"}


def estimate_reorg_depth(
    kind: str,
    from_height: int | None,
    to_height: int | None,
    previous_ancestors: dict[str, str] | None,
    current_ancestors: dict[str, str] | None,
) -> dict:
    if (
        kind == "reorg_height_drop"
        and from_height is not None
        and to_height is not None
        and from_height > to_height
    ):
        depth = from_height - to_height
        return {"depth": depth, "exact": True, "label": f"depth {depth}"}
    if kind == "tip_switch":
        estimated = estimate_fork_depth_from_ancestors(
            previous_ancestors,
            current_ancestors,
        )
        if estimated.get("depth") is not None or estimated.get("label") != "unknown":
            return estimated
        return {"depth": 1, "exact": False, "label": "depth ~1"}
    return {"depth": None, "exact": False, "label": "unknown"}


def classify_tip_event(
    previous_height: int | None,
    previous_hash: str | None,
    height: int,
    block_hash: str | None,
) -> str | None:
    """Classify a tip change relative to the previous poll for one node."""
    if previous_height is None:
        return "initial"
    if height < previous_height:
        return "reorg_height_drop"
    if (
        block_hash
        and previous_hash
        and height == previous_height
        and block_hash != previous_hash
    ):
        return "tip_switch"
    if height > previous_height:
        return "advanced"
    return "unchanged"


def tipped_rows(rows: list[dict]) -> list[dict]:
    return [
        row
        for row in rows
        if row.get("height") is not None and row.get("block_hash")
    ]


def compute_chain_summary(
    rows: list[dict],
    recent_reorgs: list[dict] | None = None,
) -> dict:
    """Summarize tip agreement and recent reorg candidates across the fleet."""
    tipped = tipped_rows(rows)
    groups: dict[tuple[int, str], list[str]] = defaultdict(list)
    representatives: dict[tuple[int, str], dict] = {}
    for row in tipped:
        key = (row["height"], row["block_hash"])
        groups[key].append(row["name"])
        representatives.setdefault(key, row)

    tip_groups = [
        {
            "height": height,
            "block_hash": block_hash,
            "nodes": sorted(names),
            "count": len(names),
            "ancestor_hashes": normalize_ancestor_hashes(
                representatives[(height, block_hash)].get("ancestor_hashes")
            ),
            "fork_depth": None,
            "fork_depth_label": "",
        }
        for (height, block_hash), names in groups.items()
    ]
    tip_groups.sort(
        key=lambda group: (group["count"], group["height"], group["block_hash"]),
        reverse=True,
    )

    majority = tip_groups[0] if tip_groups else None
    max_height = max((group["height"] for group in tip_groups), default=None)
    at_tip = (
        [group for group in tip_groups if group["height"] == max_height]
        if max_height is not None
        else []
    )
    split = len(at_tip) > 1

    if not tip_groups:
        status = "unknown"
    elif split:
        status = "split"
    elif len(tip_groups) > 1:
        status = "lagging"
    else:
        status = "agreed"

    if majority is not None:
        majority["fork_depth"] = 0
        majority["fork_depth_label"] = "majority"
        for group in tip_groups[1:]:
            if group["height"] == majority["height"]:
                depth_info = estimate_fork_depth_from_ancestors(
                    majority.get("ancestor_hashes"),
                    group.get("ancestor_hashes"),
                )
                group["fork_depth"] = depth_info.get("depth")
                group["fork_depth_label"] = depth_info.get("label") or "unknown"
            else:
                height_delta = majority["height"] - group["height"]
                group["fork_depth"] = abs(height_delta)
                group["fork_depth_label"] = (
                    f"{height_delta} behind"
                    if height_delta > 0
                    else f"{-height_delta} ahead"
                )

    return {
        "status": status,
        "split": split,
        "majority_height": majority["height"] if majority else None,
        "majority_hash": majority["block_hash"] if majority else "",
        "max_height": max_height,
        "observed_tips": len(tipped),
        "tip_groups": tip_groups,
        "recent_reorgs": list(recent_reorgs or []),
    }


def enrich_chain_roles(rows: list[dict], chain: dict) -> None:
    majority_height = chain.get("majority_height")
    majority_hash = chain.get("majority_hash") or ""
    for row in rows:
        height = row.get("height")
        block_hash = row.get("block_hash") or ""
        if height is None or not block_hash:
            row["chain_role"] = "unknown"
        elif majority_height is None:
            row["chain_role"] = "unknown"
        elif height == majority_height and block_hash == majority_hash:
            row["chain_role"] = "majority"
        elif height == majority_height:
            row["chain_role"] = "fork"
        elif height > majority_height:
            row["chain_role"] = "ahead"
        else:
            row["chain_role"] = "behind"


def public_error(network: str, code: str) -> dict:
    return {
        "schema_version": PUBLIC_SCHEMA_VERSION,
        "network": network,
        "error": {
            "code": code,
            "message": PUBLIC_ERROR_MESSAGE,
        },
    }


def rfc3339_utc(timestamp: float) -> str:
    return (
        datetime.fromtimestamp(timestamp, timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


# --------------------------------------------------------------------------- #
# NU7 configured-network status (/v1/status, schema version 1)
# --------------------------------------------------------------------------- #

NU7_SCHEMA_VERSION = 1
NU7_TARGET_SPACING_SECONDS = 25
NU7_DAA_WINDOW_BLOCKS = 102
# 300 adjacent post-NU7 block intervals.
NU7_INTERVAL_HEADERS = 301
NU7_RECENT_BLOCKS = 8
# A validator agrees when it is on the primary's chain within this many blocks.
NU7_MAX_LAG_BLOCKS = 2
NU7_RPC_TIMEOUT = 5


def nu7_network_parameters(config_path: Path) -> dict:
    """The NU7 fork's public identity, read from the primary node's own config."""
    with config_path.open("rb") as stream:
        network = tomllib.load(stream)["network"]
    parameters = network["network"]
    if not isinstance(parameters, dict):
        raise ValueError("--nu7-config requires an explicit network parameter table")
    return {
        "name": parameters["network_name"],
        "magic": "".join(f"{byte:02x}" for byte in parameters["network_magic"]),
        "activationHeight": parameters["activation_heights"]["NU7"],
        "nsmSeedZat": parameters["initial_nsm_value_balance"],
        "targetSpacingSeconds": NU7_TARGET_SPACING_SECONDS,
        "daaWindowBlocks": NU7_DAA_WINDOW_BLOCKS,
    }


class Nu7Status:
    """Builds the public NU7 status from the collector's validator rows.

    Validators are the configured nodes, in config order; the first is the
    primary, whose RPC on this host also supplies block headers. Remote validators
    report through their status endpoint, so this host needs no SSH keys for them.
    """

    def __init__(self, nodes: list[Node], config_path: Path, generation_start: float = 0.0):
        if not nodes or not nodes[0].local or not rpc_url_for(nodes[0].rpc_listen_addr):
            raise SystemExit("--nu7-config needs a first node with monitor.local and an RPC address")
        self.network = nu7_network_parameters(config_path)
        self.nodes = nodes
        self.primary = nodes[0]
        self.rpc_url = rpc_url_for(self.primary.rpc_listen_addr)
        # Counters never include a previous network generation's observations.
        self.generation_start = generation_start
        self.started_at = time.time()
        self.headers: dict[int, dict] = {}
        self.last_tip: tuple[int, str] | None = None
        self.observed_blocks: deque[float] = deque()
        self.observed_reorgs: deque[float] = deque()
        self.lock = threading.Lock()
        self.payload: dict | None = None

    def rpc(self, method: str, params: list | None = None):
        request = urllib.request.Request(
            self.rpc_url,
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                        "params": params or []}).encode(),
            {"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=NU7_RPC_TIMEOUT) as response:
            result = json.load(response)
        if result.get("error") is not None:
            raise RuntimeError(f"{method} failed")
        return result["result"]

    def sample_headers(self, height: int) -> list[dict]:
        # Post-NU7 blocks only, but always at least the tip: a freshly seeded fork
        # sits below activation until its first blocks are mined.
        start = max(min(self.network["activationHeight"], height),
                    height - NU7_INTERVAL_HEADERS + 1)
        for number in range(start, height + 1):
            if number not in self.headers:
                block_hash = self.rpc("getblockhash", [number])
                self.headers[number] = self.rpc("getblockheader", [block_hash, True])
        self.headers = {number: header for number, header in self.headers.items()
                        if number >= start}
        return [self.headers[number] for number in range(start, height + 1)]

    def primary_hash_at(self, height: int, tip: tuple[int, str]) -> str | None:
        if height == tip[0]:
            return tip[1]
        cached = self.headers.get(height)
        return cached["hash"] if cached else self.rpc("getblockhash", [height])

    def valid_validator(self, row: dict) -> bool:
        """Whether a validator answers on the configured NU7 network.

        Deliberately not the fleet `healthy` flag, which also requires recent tip
        progress: a stalled chain must still publish its tip and age.
        """
        nu7 = row.get("nu7") or {}
        return (row.get("rpc_ok") is True and row.get("active_state") == "active"
                and row.get("rpc_chain") == "test"
                and isinstance(row.get("height"), int) and bool(row.get("block_hash"))
                and nu7.get("activation_height") == self.network["activationHeight"])

    def agrees(self, row: dict, primary: dict) -> tuple[bool, int | None]:
        """Whether a validator is on the primary's chain, and how far it lags."""
        if not self.valid_validator(row):
            return False, None
        if (row["nu7"] or {}).get("branch_id") != (primary["nu7"] or {}).get("branch_id"):
            return False, None
        tip = (primary["height"], primary["block_hash"])
        lag = tip[0] - row["height"]
        if not -NU7_MAX_LAG_BLOCKS <= lag <= NU7_MAX_LAG_BLOCKS:
            return False, lag
        if lag >= 0:
            same = self.primary_hash_at(row["height"], tip) == row["block_hash"]
        else:
            # Ahead of the primary: compare its ancestor at the primary's height.
            same = (row.get("ancestor_hashes") or {}).get(str(-lag)) == tip[1]
        return same, lag

    def record_tip(self, height: int, block_hash: str, now: float) -> None:
        if self.last_tip and self.last_tip != (height, block_hash):
            old_height, old_hash = self.last_tip
            # A lower tip can only come from a reorganization, and its old height may
            # no longer exist to compare hashes at.
            if old_height > height or self.rpc("getblockhash", [old_height]) != old_hash:
                self.observed_reorgs.append(now)
                self.headers.clear()
            if height > old_height:
                self.observed_blocks.extend([now] * (height - old_height))
        self.last_tip = (height, block_hash)
        cutoff = max(now - 86400, self.generation_start)
        for observed in (self.observed_blocks, self.observed_reorgs):
            while observed and observed[0] < cutoff:
                observed.popleft()

    def unavailable(self, now: float, error: str, nodes: list[dict] | None = None) -> dict:
        payload = {"schemaVersion": NU7_SCHEMA_VERSION, "observedAt": now,
                   "status": "unavailable", "network": self.network, "error": error}
        if nodes is not None:
            payload["nodes"] = nodes
        return payload

    def build(self, rows: list[dict], now: float) -> dict:
        by_name = {row["name"]: row for row in rows}
        validators = [(node, by_name.get(node.name, {})) for node in self.nodes]
        primary = validators[0][1]
        if not self.valid_validator(primary):
            return self.unavailable(now, "Primary node RPC unavailable", [
                {"name": node.label or node.name, "healthy": self.valid_validator(row)}
                for node, row in validators])

        height, block_hash = primary["height"], primary["block_hash"]
        self.record_tip(height, block_hash, now)
        headers = self.sample_headers(height)
        intervals = [max(0, right["time"] - left["time"])
                     for left, right in zip(headers, headers[1:])]

        observed = [(node, row, *self.agrees(row, primary)) for node, row in validators]
        local_agree = all(agree for node, _, agree, _ in observed if node.local)
        all_agree = all(agree for _, _, agree, _ in observed)
        remote_miners = []
        for node, row, agree, lag in observed:
            if node.local or not node.internal_miner:
                continue
            miner = row.get("miner") or {}
            entry = {"id": node.miner_id or node.name, "region": node.region,
                     "healthy": agree and miner.get("active") is True}
            if miner:
                entry.update({"minerActive": miner.get("active") is True,
                              "nodeHealthy": row.get("rpc_ok") is True,
                              "acceptedBlocks24h": miner.get("accepted_blocks_24h"),
                              "observedAt": miner.get("observed_at")})
            if lag is not None:
                entry.update({"height": row["height"], "lagBlocks": lag})
            remote_miners.append(entry)
        local_miners = sum(1 for node, _, agree, _ in observed
                           if node.local and node.internal_miner and agree)

        balance = primary.get("nsm_value_balance_zat")
        tip_header = headers[-1]
        return {
            "schemaVersion": NU7_SCHEMA_VERSION, "observedAt": now,
            "status": "live" if all_agree else "degraded",
            "network": {**self.network, "branchId": primary["nu7"]["branch_id"],
                        "reissuanceHeight": primary.get("nsm_reissuance_height"),
                        "reissuanceKnown": primary.get("nsm_reissuance_known") is True},
            "chain": {
                "height": height, "hash": block_hash, "blockTime": tip_header["time"],
                "tipAgeSeconds": max(0, int(now - tip_header["time"])),
                "difficulty": tip_header.get("difficulty"),
                "meanIntervalSeconds": round(statistics.mean(intervals), 1) if intervals else None,
                "medianIntervalSeconds": (round(statistics.median(intervals), 1)
                                          if intervals else None),
                "intervalSampleBlocks": len(intervals),
            },
            "nsm": {"balanceZat": balance, "available": balance is not None,
                    "seedZat": self.network["nsmSeedZat"]},
            "observation": {
                "localNodesAgree": local_agree,
                "validatorsAgree": all_agree,
                "validatorsAgreeing": sum(1 for *_, agree, _ in observed if agree),
                "validatorsConfigured": len(observed),
                "blocks24h": len(self.observed_blocks),
                "reorgs24h": len(self.observed_reorgs),
                "reorgRate24h": (len(self.observed_reorgs) / len(self.observed_blocks)
                                 if self.observed_blocks else None),
                "since": max(self.started_at, self.generation_start),
                "scope": (f"{len(observed)} validators; tip replacements observed "
                          "on the primary only"),
            },
            "mining": {
                "operatorMinersActive": local_miners + sum(m["healthy"] for m in remote_miners),
                "operatorMinersConfigured": sum(node.internal_miner for node, _ in validators),
                "remoteMiners": remote_miners,
            },
            "nodes": [
                {key: value for key, value in (
                    ("name", node.label or node.name), ("healthy", agree),
                    ("height", row.get("height")), ("hash", row.get("block_hash")),
                    ("peers", row.get("peer_count")), ("externalPeers", row.get("peer_external")),
                ) if value is not None}
                for node, row, agree, _ in observed
            ],
            "recentBlocks": [{"height": header["height"], "hash": header["hash"],
                              "time": header["time"], "difficulty": header.get("difficulty")}
                             for header in reversed(headers[-NU7_RECENT_BLOCKS:])],
        }

    def update(self, rows: list[dict], now: float) -> None:
        try:
            payload = self.build(rows, now)
        except Exception:
            # The public payload stays generic; the journal keeps the cause.
            logging.exception("NU7 status collection failed")
            payload = self.unavailable(now, "Collector failed to sample node RPC")
        with self.lock:
            self.payload = payload

    def response(self, now: float | None = None) -> tuple[int, dict]:
        now = time.time() if now is None else now
        with self.lock:
            payload = self.payload
        if payload is None or now - payload["observedAt"] > PUBLIC_STATUS_MAX_AGE:
            payload = {"schemaVersion": NU7_SCHEMA_VERSION, "status": "unavailable",
                       "error": "No fresh observation", "network": self.network}
        return (503 if payload["status"] == "unavailable" else 200), payload


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Zakura Ironwood cluster status">
<title>Zakura cluster status</title>
<link rel="icon" href="https://avatars.githubusercontent.com/u/272444516?s=200&v=4" type="image/png">
<style>
:root {
  color-scheme: dark;
  --void: #07060e;
  --base: #0d0b16;
  --surface: #12101d;
  --raised: #191627;
  --line: #221f33;
  --line-hi: #322d49;
  --ink: #ebe8f7;
  --ink-2: #b6b0d0;
  --muted: #837ca0;
  --dim: #4f4870;
  --pink: #c2457a;
  --pink-hi: #e8709f;
  --pink-soft: rgba(194, 69, 122, 0.12);
  --ok: #5fd7a6;
  --ok-soft: rgba(95, 215, 166, 0.10);
  --ok-line: rgba(95, 215, 166, 0.32);
  --warn: #efc255;
  --warn-soft: rgba(239, 194, 85, 0.10);
  --warn-line: rgba(239, 194, 85, 0.32);
  --bad: #ff7b72;
  --bad-soft: rgba(255, 123, 114, 0.10);
  --bad-line: rgba(255, 123, 114, 0.32);
  --r-lg: 16px;
  --r-md: 12px;
  --r-sm: 8px;
  --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
}
* { box-sizing: border-box; }
/* The view switcher toggles [hidden]; without this any class that sets a
   display value (.stats is grid, .view is flex) silently overrides the UA
   stylesheet and leaves an unrendered section on screen. */
[hidden] { display: none !important; }
body {
  margin: 0;
  min-height: 100vh;
  color: var(--ink);
  background:
    radial-gradient(1100px 520px at 12% -8%, rgba(194, 69, 122, 0.14), transparent 70%),
    radial-gradient(900px 480px at 92% 0%, rgba(88, 74, 173, 0.12), transparent 70%),
    var(--void);
  background-attachment: fixed;
  font: 15px/1.6 'Inter', ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  -webkit-font-smoothing: antialiased;
}
h1, h2, h3, p { margin: 0; }
a { color: var(--pink-hi); }
button { font: inherit; }

/* ---------- primitives ---------- */
.shell {
  width: min(100% - 40px, 1560px);
  margin: 0 auto;
  padding: 24px 0 56px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.panel {
  min-width: 0;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.022), transparent 120px), var(--surface);
  border: 1px solid var(--line);
  border-radius: var(--r-lg);
}
.pad { padding: 20px 22px; }
.eyebrow {
  color: var(--muted);
  font-size: 0.68rem;
  font-weight: 650;
  letter-spacing: 0.10em;
  text-transform: uppercase;
}
.label {
  color: var(--muted);
  font-size: 0.66rem;
  font-weight: 650;
  letter-spacing: 0.09em;
  text-transform: uppercase;
}
.mono { font-family: var(--mono); font-variant-ligatures: none; }
.num { font-variant-numeric: tabular-nums; }
.muted { color: var(--muted); }
.dim { color: var(--dim); }

/* ---------- header ---------- */
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  flex-wrap: wrap;
  padding: 16px 22px;
}
.brand { display: flex; align-items: center; gap: 14px; min-width: 0; }
.brand img {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  border: 1px solid rgba(194, 69, 122, 0.55);
  box-shadow: 0 0 0 4px var(--pink-soft);
  flex-shrink: 0;
}
.brand h1 {
  font-size: clamp(1.15rem, 2.2vw, 1.5rem);
  font-weight: 680;
  letter-spacing: -0.02em;
  line-height: 1.15;
}
.brand .eyebrow { display: flex; align-items: center; gap: 8px; }
.chip {
  display: inline-flex;
  align-items: center;
  height: 18px;
  padding: 0 7px;
  border: 1px solid var(--line-hi);
  border-radius: 5px;
  background: var(--base);
  color: var(--ink-2);
  font-size: 0.62rem;
  font-weight: 650;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.header-right {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}
.state-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  height: 34px;
  padding: 0 15px;
  border-radius: 999px;
  border: 1px solid var(--line-hi);
  background: var(--base);
  color: var(--ink-2);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  white-space: nowrap;
}
.state-pill::before {
  content: '';
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: currentColor;
  box-shadow: 0 0 9px currentColor;
  animation: pulse 2.6s ease-in-out infinite;
}
.state-pill.is-ok { border-color: var(--ok-line); background: var(--ok-soft); color: var(--ok); }
.state-pill.is-warn { border-color: var(--warn-line); background: var(--warn-soft); color: var(--warn); }
.state-pill.is-bad { border-color: var(--bad-line); background: var(--bad-soft); color: var(--bad); }
@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.25; }
}
.ghost-button {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  height: 34px;
  padding: 0 13px;
  border: 1px solid var(--line-hi);
  border-radius: 999px;
  background: var(--base);
  color: var(--ink-2);
  font-size: 0.76rem;
  font-weight: 550;
  text-decoration: none;
  cursor: pointer;
  transition: border-color 0.15s, color 0.15s, background 0.15s;
  white-space: nowrap;
}
.ghost-button:hover { border-color: var(--pink); color: var(--pink-hi); background: var(--pink-soft); }
.ghost-button.is-on { border-color: var(--warn-line); background: var(--warn-soft); color: var(--warn); }
.freshness { font-size: 0.76rem; color: var(--muted); white-space: nowrap; }
.freshness b { color: var(--ink-2); font-weight: 600; }

/* ---------- fleet health card ---------- */
.card-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 14px;
  flex-wrap: wrap;
  margin-bottom: 14px;
}
.big-value {
  font-size: clamp(2rem, 4.6vw, 3rem);
  font-weight: 690;
  letter-spacing: -0.035em;
  line-height: 1;
  font-variant-numeric: tabular-nums;
}
.big-sub { margin-top: 7px; color: var(--muted); font-size: 0.86rem; }
.facts {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(128px, 1fr));
  gap: 10px;
  margin-top: 18px;
}
.fact {
  padding: 11px 13px;
  min-width: 0;
  background: var(--base);
  border: 1px solid var(--line);
  border-radius: var(--r-sm);
}
.fact strong {
  display: block;
  margin-top: 4px;
  font-size: 0.94rem;
  font-weight: 620;
  font-variant-numeric: tabular-nums;
  overflow-wrap: anywhere;
}
.fact small { display: block; margin-top: 2px; color: var(--dim); font-size: 0.7rem; }

/* fleet card */
.donut-row { display: flex; align-items: center; gap: 18px; }
.health-bar {
  display: flex;
  gap: 3px;
  margin-top: 16px;
  height: 10px;
}
.health-bar span {
  border-radius: 3px;
  min-width: 4px;
  transition: flex-grow 0.4s ease;
}
.seg-ok { background: var(--ok); }
.seg-warn { background: var(--warn); }
.seg-bad { background: var(--bad); }
.seg-neutral { background: var(--dim); }
.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 14px;
  margin-top: 12px;
  font-size: 0.76rem;
  color: var(--muted);
}
.legend span { display: inline-flex; align-items: center; }
.legend i {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 2px;
  margin-right: 7px;
  font-style: normal;
}
.legend b { color: var(--ink); font-weight: 620; font-variant-numeric: tabular-nums; }

/* ---------- stat strip ---------- */
.stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: 12px;
}
.stat {
  padding: 13px 15px;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: var(--r-md);
  min-width: 0;
}
.stat-value {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 6px;
  font-size: 0.95rem;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  min-width: 0;
}
.stat-value > span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.stat small { display: block; margin-top: 3px; color: var(--dim); font-size: 0.74rem; }

/* ---------- badges ---------- */
.badge {
  display: inline-flex;
  align-items: center;
  height: 21px;
  padding: 0 8px;
  border-radius: 6px;
  border: 1px solid var(--line-hi);
  background: var(--base);
  color: var(--ink-2);
  font-size: 0.66rem;
  font-weight: 680;
  letter-spacing: 0.045em;
  text-transform: uppercase;
  white-space: nowrap;
}
.badge.tone-ok { border-color: var(--ok-line); background: var(--ok-soft); color: var(--ok); }
.badge.tone-warn { border-color: var(--warn-line); background: var(--warn-soft); color: var(--warn); }
.badge.tone-bad { border-color: var(--bad-line); background: var(--bad-soft); color: var(--bad); }
.badge.tone-info { border-color: rgba(194, 69, 122, 0.32); background: var(--pink-soft); color: var(--pink-hi); }

/* ---------- section header ---------- */
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
  margin-bottom: 14px;
}
.section-head h2 { font-size: 1rem; font-weight: 640; letter-spacing: -0.01em; margin-top: 3px; }
.section-note { color: var(--muted); font-size: 0.83rem; }

/* ---------- tip groups ---------- */
.tip-groups { display: grid; gap: 10px; }
.tip-group {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 6px 16px;
  padding: 13px 15px;
  background: var(--base);
  border: 1px solid var(--line);
  border-left: 3px solid var(--dim);
  border-radius: var(--r-md);
}
.tip-group.is-majority { border-left-color: var(--ok); }
.tip-group.is-behind, .tip-group.is-ahead { border-left-color: var(--warn); }
.tip-group.is-fork { border-left-color: var(--bad); }
.tip-group-top { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.tip-group-top .height { font-weight: 640; font-variant-numeric: tabular-nums; }
.tip-group-count { color: var(--muted); font-size: 0.8rem; white-space: nowrap; text-align: right; }
.tip-group-nodes {
  grid-column: 1 / -1;
  color: var(--ink-2);
  font-size: 0.79rem;
  line-height: 1.5;
  overflow-wrap: anywhere;
}
.subhead {
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 22px 0 10px;
}
.subhead::after { content: ''; flex: 1; height: 1px; background: var(--line); }
.reorg-list { display: grid; gap: 8px; }
.reorg-item {
  padding: 11px 14px;
  background: var(--base);
  border: 1px solid var(--line);
  border-left: 3px solid var(--warn);
  border-radius: var(--r-md);
  font-size: 0.82rem;
  color: var(--ink-2);
}
.reorg-top { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.reorg-detail { margin-top: 5px; color: var(--muted); font-size: 0.78rem; overflow-wrap: anywhere; }
.empty {
  padding: 18px;
  border: 1px dashed var(--line-hi);
  border-radius: var(--r-md);
  color: var(--muted);
  font-size: 0.84rem;
  text-align: center;
}

/* ---------- toolbar ---------- */
.toolbar { display: flex; align-items: center; gap: 9px; flex-wrap: wrap; }
.search {
  display: flex;
  align-items: center;
  gap: 8px;
  height: 34px;
  padding: 0 12px;
  border: 1px solid var(--line-hi);
  border-radius: 999px;
  background: var(--base);
}
.search:focus-within { border-color: var(--pink); }
.search svg { width: 14px; height: 14px; stroke: var(--muted); fill: none; stroke-width: 2; }
.search input {
  width: 190px;
  border: 0;
  background: transparent;
  color: var(--ink);
  font-size: 0.82rem;
  outline: none;
}
.search input::placeholder { color: var(--dim); }

/* ---------- table ---------- */
.table-wrap {
  border: 1px solid var(--line);
  border-radius: var(--r-md);
  background: var(--base);
  overflow: auto;
  max-height: 78vh;
}
table { width: 100%; min-width: 1080px; border-collapse: separate; border-spacing: 0; }
thead th {
  position: sticky;
  top: 0;
  z-index: 2;
  padding: 9px 10px;
  background: var(--raised);
  border-bottom: 1px solid var(--line-hi);
  color: var(--muted);
  font-size: 0.63rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  text-align: left;
  white-space: nowrap;
  user-select: none;
}
thead th.sortable { cursor: pointer; }
thead th.sortable:hover { color: var(--ink-2); }
thead th .arrow { margin-left: 4px; opacity: 0.35; }
thead th.is-sorted { color: var(--pink-hi); }
thead th.is-sorted .arrow { opacity: 1; }
tbody td {
  padding: 9px 10px;
  border-bottom: 1px solid var(--line);
  font-size: 0.79rem;
  vertical-align: middle;
}
tbody tr.node-row { cursor: pointer; }
tbody tr.node-row:hover td { background: rgba(50, 45, 73, 0.28); }
tbody tr.is-open td { background: rgba(50, 45, 73, 0.34); }
tbody tr.row-warn td:first-child { box-shadow: inset 3px 0 0 var(--warn); }
tbody tr.row-bad td:first-child { box-shadow: inset 3px 0 0 var(--bad); }
th.col-num, td.col-num { text-align: right; }
.col-node { width: 16%; }
.col-health { width: 7.5%; }
.col-chain { width: 9%; }
.col-height { width: 10%; }
.col-hdr { width: 4.5%; }
.col-adv { width: 5%; }
.col-commit { width: 9%; }
.col-tip { width: 11%; }
.col-restarted { width: 9%; }
.col-detail { width: auto; }
.node-cell { display: flex; align-items: center; gap: 8px; min-width: 0; }
.twisty {
  flex-shrink: 0;
  width: 12px;
  color: var(--dim);
  font-size: 0.6rem;
  transition: transform 0.15s;
}
tr.is-open .twisty { transform: rotate(90deg); color: var(--pink-hi); }
.node-name { font-weight: 620; font-size: 0.82rem; overflow-wrap: anywhere; }
.node-sub { color: var(--dim); font-size: 0.68rem; font-family: var(--mono); }
.stack { display: flex; flex-direction: column; align-items: flex-start; gap: 3px; min-width: 0; }
.col-num .stack { align-items: flex-end; }
.delta { font-size: 0.68rem; color: var(--muted); font-variant-numeric: tabular-nums; }
.detail-text {
  color: var(--muted);
  font-size: 0.74rem;
  line-height: 1.4;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
  overflow-wrap: anywhere;
}
tbody tr.drawer > td { padding: 0; border-bottom: 1px solid var(--line); background: var(--surface); }
.drawer-inner {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
  gap: 12px 22px;
  padding: 16px 18px 18px 30px;
}
.field { min-width: 0; }
.field .label { font-size: 0.61rem; }
.field-value {
  display: flex;
  align-items: flex-start;
  gap: 6px;
  margin-top: 3px;
  font-size: 0.78rem;
  overflow-wrap: anywhere;
}
.field-value.is-bad { color: var(--bad); }
.drawer-inner .full { grid-column: 1 / -1; }
.ancestors { display: grid; gap: 3px; margin-top: 4px; }
.ancestors div { display: flex; gap: 8px; font-size: 0.73rem; font-family: var(--mono); color: var(--ink-2); }
.ancestors span:first-child { color: var(--dim); min-width: 3.2rem; }

/* ---------- copy ---------- */
.copyable { display: inline-flex; align-items: center; gap: 5px; min-width: 0; white-space: nowrap; }
.copy-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  flex-shrink: 0;
  padding: 0;
  border: 0;
  border-radius: 5px;
  background: transparent;
  color: var(--dim);
  cursor: pointer;
}
.copy-button:hover { background: var(--pink-soft); color: var(--pink-hi); }
.copy-button.is-done { color: var(--ok); }
.copy-button svg {
  width: 12px;
  height: 12px;
  fill: none;
  stroke: currentColor;
  stroke-width: 1.9;
  stroke-linecap: round;
  stroke-linejoin: round;
}
.banner {
  padding: 11px 16px;
  border: 1px solid var(--bad-line);
  border-radius: var(--r-md);
  background: var(--bad-soft);
  color: var(--bad);
  font-size: 0.84rem;
}
footer {
  color: var(--muted);
  font-size: 0.8rem;
  text-align: center;
  padding-top: 6px;
}

/* ---------- node view ---------- */
.view { display: flex; flex-direction: column; gap: 16px; }
.node-link { color: inherit; text-decoration: none; border-radius: 4px; }
.node-link:hover { color: var(--pink-hi); text-decoration: underline; }
.node-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 18px; flex-wrap: wrap; }
.node-head h2 { font-size: clamp(1.1rem, 2.4vw, 1.45rem); font-weight: 680; letter-spacing: -0.02em; }
.node-head .eyebrow { display: flex; align-items: center; gap: 8px; margin-bottom: 5px; }
.node-badges { display: flex; align-items: center; gap: 7px; flex-wrap: wrap; }
.vitals { display: grid; grid-template-columns: repeat(auto-fit, minmax(148px, 1fr)); gap: 11px; margin-top: 14px; }
.vital {
  background: var(--raised);
  border: 1px solid var(--line);
  border-radius: var(--r-md);
  padding: 11px 13px;
  min-width: 0;
}
.vital.is-ok { border-color: var(--ok-line); background: var(--ok-soft); }
.vital.is-warn { border-color: var(--warn-line); background: var(--warn-soft); }
.vital.is-bad { border-color: var(--bad-line); background: var(--bad-soft); }
.vital.is-neutral { border-color: var(--line-hi); }
.vital strong {
  display: block;
  margin-top: 5px;
  font-size: 1.12rem;
  font-weight: 650;
  font-variant-numeric: tabular-nums;
  overflow-wrap: anywhere;
}
.vital small { display: block; margin-top: 3px; color: var(--dim); font-size: 0.71rem; }
.meter { height: 5px; border-radius: 3px; background: var(--line); overflow: hidden; margin-top: 8px; }
.meter i { display: block; height: 100%; background: var(--ok); }
.meter.is-warn i { background: var(--warn); }
.meter.is-bad i { background: var(--bad); }
.sparks { display: grid; grid-template-columns: repeat(auto-fit, minmax(216px, 1fr)); gap: 12px; margin-top: 14px; }
.spark {
  background: var(--raised);
  border: 1px solid var(--line);
  border-radius: var(--r-md);
  padding: 10px 12px 8px;
  min-width: 0;
}
.spark-head { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
.spark-head b { font-size: 0.9rem; font-weight: 640; font-variant-numeric: tabular-nums; }
.spark svg { display: block; width: 100%; height: 42px; margin-top: 7px; overflow: visible; }
.spark-empty { margin-top: 12px; color: var(--dim); font-size: 0.74rem; }
.node-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(316px, 1fr)); gap: 16px; }
.kv { display: grid; gap: 7px; margin-top: 13px; }
.kv-row { display: flex; align-items: baseline; justify-content: space-between; gap: 14px; font-size: 0.83rem; }
.kv-row > span { color: var(--muted); white-space: nowrap; }
.kv-row > b {
  font-weight: 620;
  text-align: right;
  min-width: 0;
  overflow-wrap: anywhere;
  font-variant-numeric: tabular-nums;
}
.kv-row.is-bad > b { color: var(--bad); }
.kv-row.is-warn > b { color: var(--warn); }
.note-off { margin-top: 12px; color: var(--dim); font-size: 0.79rem; }
.log-lines { display: grid; gap: 6px; margin-top: 12px; }
.log-lines code {
  display: block;
  font-family: var(--mono);
  font-size: 0.72rem;
  line-height: 1.5;
  color: var(--ink-2);
  background: var(--base);
  border: 1px solid var(--line);
  border-radius: var(--r-sm);
  padding: 7px 9px;
  overflow-wrap: anywhere;
}

@media (max-width: 720px) {
  .shell { width: calc(100% - 22px); padding-top: 16px; }
  .pad { padding: 16px; }
  .topbar { align-items: flex-start; }
  .search input { width: 130px; }
}
/* ---------- node page: section jump bar ---------- */
.node-nav {
  position: sticky;
  top: 8px;
  z-index: 4;
  display: flex;
  gap: 6px;
  padding: 6px;
  overflow-x: auto;
  scrollbar-width: none;
  background: rgba(18, 16, 29, 0.92);
  backdrop-filter: blur(8px);
}
.node-nav button {
  flex: none;
  color: var(--ink-2);
  background: transparent;
  border: 1px solid transparent;
  border-radius: var(--r-sm);
  padding: 5px 12px;
  font-size: 0.82rem;
  cursor: pointer;
}
.node-nav button:hover { color: var(--ink); background: var(--raised); }
.node-nav button[aria-current="true"] { color: var(--ink); border-color: var(--pink); background: var(--pink-soft); }
.node-nav button:focus-visible { outline: 2px solid var(--pink-hi); outline-offset: 2px; }
#node-view [data-jump-target] { scroll-margin-top: 64px; }

/* ---------- node page: CPU profile ---------- */
/* Category colours: seven hues checked for colour-blind separation on --surface,
   plus two greys for time outside the node's own subsystems. */
:root {
  --c-verification: #3987e5; --c-propagation: #d95926; --c-arrival: #199e70;
  --c-rpc: #c98500; --c-mempool: #d55181; --c-network: #008300; --c-commit: #9085e9;
  --c-kernel: #8d899e; --c-other: #6b6780;
  --on-mark: #07060e;
}
.pf { display: grid; gap: 16px; min-width: 0; }
.pf-panel { display: grid; gap: 12px; }
.pf .section-head { margin-bottom: 0; }
.pf h3 { font-size: 0.95rem; font-weight: 640; margin: 0; }
.pf-note { color: var(--muted); font-size: 0.83rem; max-width: 86ch; }
.pf-tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }
.pf-tile { background: var(--raised); border: 1px solid var(--line); border-radius: var(--r-md); padding: 10px 12px; display: grid; gap: 2px; }
.pf-tile b { font-family: var(--mono); font-size: 1.12rem; font-weight: 500; font-variant-numeric: tabular-nums; }
.pf-tile span { color: var(--muted); font-size: 0.75rem; }
.pf-controls { display: flex; flex-wrap: wrap; gap: 8px 18px; align-items: center; }
.pf-group { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.pf-group > span { color: var(--muted); font-size: 0.75rem; margin-right: 2px; }
.pf button, .pf input[type="search"] {
  font: inherit; font-size: 0.8rem; color: var(--ink-2); background: var(--raised);
  border: 1px solid var(--line-hi); border-radius: var(--r-sm); padding: 4px 10px;
}
.pf button { cursor: pointer; display: inline-flex; align-items: center; gap: 6px; }
.pf button:hover { color: var(--ink); border-color: var(--muted); }
.pf button[aria-pressed="true"] { color: var(--ink); border-color: var(--pink); background: var(--pink-soft); }
.pf button:focus-visible, .pf input:focus-visible { outline: 2px solid var(--pink-hi); outline-offset: 2px; }
.pf input[type="search"] { width: 240px; max-width: 100%; }
.pf-swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; flex: none; }
.pf-legend { display: flex; flex-wrap: wrap; gap: 4px 14px; font-size: 0.75rem; color: var(--ink-2); }
.pf-legend span { display: inline-flex; align-items: center; gap: 6px; }
.pf-chart { overflow-x: auto; min-width: 0; }
.pf svg { display: block; }
.pf svg text { font-family: var(--mono); font-size: 11px; fill: var(--muted); }
#pf-timeline { cursor: crosshair; user-select: none; touch-action: pan-y; }
#pf-flame rect.pf-f { cursor: pointer; stroke: var(--surface); stroke-width: 1; }
#pf-flame rect.pf-f:hover { filter: brightness(1.18); }
#pf-flame text { fill: var(--on-mark); pointer-events: none; }
#pf-flame .pf-dim { opacity: 0.28; }
#pf-flame .pf-hit { stroke: var(--ink); stroke-width: 1.5; }
.pf-crumb { font-family: var(--mono); font-size: 0.75rem; color: var(--muted); overflow-wrap: anywhere; }
.pf-detail { background: var(--raised); border: 1px solid var(--line); border-radius: var(--r-md); padding: 10px 12px; display: grid; gap: 8px; }
.pf-detail .pf-name { font-family: var(--mono); font-size: 0.75rem; overflow-wrap: anywhere; max-height: 7.5em; overflow-y: auto; }
.pf-detail .pf-facts { display: flex; flex-wrap: wrap; gap: 4px 18px; color: var(--ink-2); font-size: 0.8rem; }
.pf-detail .pf-facts b { font-family: var(--mono); font-weight: 500; color: var(--ink); }
/* The fleet table's rules (a 1080px minimum width, separate borders) are not for these. */
.pf table { width: 100%; min-width: 0; border-collapse: collapse; font-size: 0.8rem; }
.pf th, .pf td { text-align: left; padding: 5px 8px; border-bottom: 1px solid var(--line); vertical-align: middle; font-size: 0.8rem; }
.pf th { font-weight: 600; color: var(--muted); font-size: 0.68rem; letter-spacing: 0.06em; text-transform: uppercase; }
.pf td.pf-num { font-family: var(--mono); font-variant-numeric: tabular-nums; text-align: right; white-space: nowrap; }
.pf td.pf-fn { font-family: var(--mono); font-size: 0.75rem; overflow-wrap: anywhere; }
.pf td.pf-fn button { all: unset; cursor: pointer; color: var(--ink); }
.pf td.pf-fn button:hover { color: var(--pink-hi); }
.pf-bar { height: 6px; border-radius: 3px; min-width: 2px; }
#pf-stages text.pf-label { fill: var(--ink-2); font-family: inherit; font-size: 13px; }
#pf-stages text.pf-value { fill: var(--ink); }
#pf-stages rect.pf-hit { fill: transparent; }
#pf-stages rect.pf-hit:hover { fill: var(--pink-soft); }
#pf-block-table td button { all: unset; cursor: pointer; color: var(--pink-hi); font-family: var(--mono); }
#pf-block-table td button:hover { text-decoration: underline; }
#pf-block-table td button:focus-visible { outline: 2px solid var(--pink-hi); outline-offset: 2px; }
#pf-block-table tr[aria-selected="true"] td { background: var(--pink-soft); }
.pf-mini { display: flex; height: 8px; min-width: 120px; gap: 1px; }
.pf-mini i { display: block; height: 100%; min-width: 1px; border-radius: 1px; }
.pf-tip { position: fixed; pointer-events: none; z-index: 5; max-width: min(560px, 92vw); background: var(--raised); border: 1px solid var(--line-hi); border-radius: var(--r-sm); padding: 8px 10px; font-size: 0.75rem; box-shadow: 0 6px 24px rgba(0, 0, 0, 0.45); }
.pf-tip .pf-name { font-family: var(--mono); overflow-wrap: anywhere; color: var(--ink); display: -webkit-box; -webkit-line-clamp: 6; -webkit-box-orient: vertical; overflow: hidden; }
.pf-tip .pf-row { display: flex; gap: 8px; align-items: center; color: var(--ink-2); }
.pf-tip .pf-row b { font-family: var(--mono); font-weight: 500; color: var(--ink); margin-left: auto; padding-left: 16px; }
.pf-empty { color: var(--muted); padding: 28px 0; text-align: center; }
.pf-about summary { cursor: pointer; font-weight: 640; font-size: 0.95rem; }
.pf-about[open] summary { margin-bottom: 4px; }
.pf-notes { margin: 0; padding-left: 18px; display: grid; gap: 4px; color: var(--ink-2); font-size: 0.8rem; max-width: 96ch; }
</style>
</head>
<body>
<main class="shell">
  <header class="panel topbar">
    <div class="brand">
      <img src="https://avatars.githubusercontent.com/u/272444516?s=200&v=4" alt="Valar Group" width="40" height="40">
      <div>
        <p class="eyebrow"><span id="network-chip" class="chip">mainnet</span> Ironwood observability</p>
        <h1>Zakura Cluster Status</h1>
      </div>
    </div>
    <div class="header-right">
      <span class="freshness" id="freshness">connecting...</span>
      <span class="state-pill" id="state-pill">Connecting</span>
    </div>
  </header>

  <div class="banner" id="banner" hidden></div>

  <section class="panel pad" data-view="fleet">
    <div class="card-head">
      <div>
        <p class="eyebrow">Fleet health</p>
        <h2>Nodes reporting</h2>
      </div>
      <span>
        <a class="badge" id="propagation-link" href="/propagation" hidden>Block propagation</a>
        <span class="badge" id="client-mix">...</span>
      </span>
    </div>
    <div class="big-value" id="healthy-count">...</div>
    <p class="big-sub" id="healthy-sub">waiting for the first poll</p>
    <div class="health-bar" id="health-bar"></div>
    <div class="legend" id="health-legend"></div>
    <div class="facts">
      <div class="fact">
        <span class="label">Leading height</span>
        <strong id="fleet-max-height">...</strong>
        <small id="fleet-spread">...</small>
      </div>
      <div class="fact">
        <span class="label">Slowest advance</span>
        <strong id="fleet-slowest">...</strong>
        <small id="fleet-slowest-node">...</small>
      </div>
      <div class="fact">
        <span class="label">Max header lag</span>
        <strong id="fleet-max-lag">...</strong>
        <small id="fleet-max-lag-node">...</small>
      </div>
    </div>
  </section>

  <section class="stats" data-view="fleet">
    <div class="stat">
      <span class="label">Tip agreement</span>
      <div class="stat-value" id="tip-agreement">...</div>
      <small id="tip-agreement-detail">...</small>
    </div>
    <div class="stat">
      <span class="label">Majority tip</span>
      <div class="stat-value mono" id="majority-tip">...</div>
      <small id="majority-height">...</small>
    </div>
    <div class="stat">
      <span class="label">Recorded reorgs</span>
      <div class="stat-value" id="reorg-count">...</div>
      <small id="reorg-latest">...</small>
    </div>
    <div class="stat">
      <span class="label">Last poll</span>
      <div class="stat-value" id="last-poll">...</div>
      <small id="stale-window">...</small>
    </div>
  </section>

  <section class="panel pad" data-view="fleet">
    <div class="section-head">
      <div>
        <p class="eyebrow">Chain tips</p>
        <h2>Tip agreement across the fleet</h2>
      </div>
      <p class="section-note" id="chain-summary">Waiting for first poll...</p>
    </div>
    <div class="tip-groups" id="tip-groups"></div>
    <div class="subhead"><p class="eyebrow">Orphan pairs</p></div>
    <div class="reorg-list" id="reorg-list"></div>
  </section>

  <section class="panel pad" data-view="fleet">
    <div class="section-head">
      <div>
        <p class="eyebrow">Live node status</p>
        <h2 id="table-title">Fleet</h2>
      </div>
      <div class="toolbar">
        <label class="search">
          <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="7"></circle><path d="m20 20-3.6-3.6"></path></svg>
          <input id="filter-input" type="search" placeholder="Filter node, commit, hash, detail" autocomplete="off" spellcheck="false">
        </label>
        <button class="ghost-button" id="issues-toggle" type="button">Issues only</button>
        <button class="ghost-button" id="expand-toggle" type="button">Expand all</button>
      </div>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th class="col-node sortable" data-sort="name">Node<span class="arrow"></span></th>
            <th class="col-health sortable" data-sort="health">Health<span class="arrow"></span></th>
            <th class="col-chain sortable" data-sort="chain_role">Chain<span class="arrow"></span></th>
            <th class="col-height col-num sortable" data-sort="height">Height<span class="arrow"></span></th>
            <th class="col-hdr col-num sortable" data-sort="header_lag" title="headers minus blocks">Hdr<span class="arrow"></span></th>
            <th class="col-adv col-num sortable" data-sort="seconds_since_advanced" title="time since the tip last advanced">Adv<span class="arrow"></span></th>
            <th class="col-commit sortable" data-sort="commit">Commit<span class="arrow"></span></th>
            <th class="col-tip">Tip</th>
            <th class="col-restarted sortable" data-sort="last_restarted">Restarted<span class="arrow"></span></th>
            <th class="col-detail">Details</th>
          </tr>
        </thead>
        <tbody id="rows"></tbody>
      </table>
    </div>
  </section>

  <div class="view" id="node-view" data-view="node" hidden>
    <section class="panel pad">
      <div class="node-head">
        <div>
          <p class="eyebrow"><a class="node-link" href="/">&#8592; Fleet</a>
            <span class="chip" id="node-network-chip">mainnet</span></p>
          <h2 id="node-title">node</h2>
          <p class="section-note" id="node-subtitle">Loading node detail...</p>
        </div>
        <div class="node-badges" id="node-badges"></div>
      </div>
    </section>

    <nav class="panel node-nav" id="node-nav" aria-label="Sections of this page">
      <button data-jump="node-overview">Overview</button>
      <button data-jump="node-details">Chain, sync and peers</button>
      <button data-jump="node-events">Events</button>
      <button data-jump="profile" id="node-nav-profile" hidden>CPU profile</button>
    </nav>

    <section class="panel pad" id="node-overview" data-jump-target>
      <div class="section-head">
        <div>
          <p class="eyebrow">Host</p>
          <h2>Vitals</h2>
        </div>
        <p class="section-note" id="node-vitals-note"></p>
      </div>
      <div class="vitals" id="node-vitals"></div>
    </section>

    <section class="panel pad">
      <div class="section-head">
        <div>
          <p class="eyebrow">Trend</p>
          <h2 id="node-sparks-title">Recent history</h2>
        </div>
        <p class="section-note" id="node-sparks-note"></p>
      </div>
      <div class="sparks" id="node-sparks"></div>
    </section>

    <div class="node-grid" id="node-details" data-jump-target>
      <article class="panel pad">
        <div class="section-head"><div><p class="eyebrow">Consensus</p><h2>Chain</h2></div></div>
        <div class="kv" id="node-chain"></div>
      </article>
      <article class="panel pad">
        <div class="section-head"><div><p class="eyebrow">Pipeline</p><h2>Sync</h2></div></div>
        <div id="node-sync"></div>
      </article>
      <article class="panel pad">
        <div class="section-head"><div><p class="eyebrow">Connectivity</p><h2>Peers and network</h2></div></div>
        <div id="node-peers"></div>
      </article>
    </div>

    <section class="panel pad" id="node-events" data-jump-target>
      <div class="section-head">
        <div>
          <p class="eyebrow">Recent</p>
          <h2>Events</h2>
        </div>
      </div>
      <div class="subhead"><p class="eyebrow">Orphan pairs seen on this node</p></div>
      <div class="reorg-list" id="node-reorgs"></div>
      <div class="subhead"><p class="eyebrow">Last warning or error log line</p></div>
      <div id="node-last-log"></div>
      <div class="subhead"><p class="eyebrow">Recent log errors</p></div>
      <div id="node-logs"></div>
    </section>

    <section class="pf" id="profile" data-jump-target aria-labelledby="pf-h" hidden>
      <div class="panel pad pf-panel">
        <div class="section-head">
          <div>
            <p class="eyebrow">Continuous CPU profile</p>
            <h2 id="pf-h">Where this node spends its CPU</h2>
          </div>
          <span class="badge" id="pf-state">not loaded</span>
        </div>
        <p class="section-note" id="pf-freshness">Loads when you scroll here.</p>
        <div class="pf-tiles" id="pf-tiles"></div>
      </div>

      <div class="panel pad pf-panel" role="region" aria-labelledby="pf-tl-h">
        <div class="pf-controls">
          <h3 id="pf-tl-h">CPU over time</h3>
          <div class="pf-group" id="pf-ranges" role="group" aria-label="Time range"></div>
          <button id="pf-live" hidden>Back to live</button>
        </div>
        <p class="pf-note" id="pf-tl-note">Cores the node kept busy, by what the innermost node code in each sample was doing. Drag across the chart to look at just that stretch.</p>
        <div class="pf-chart"><svg id="pf-timeline" role="img" aria-label="CPU cores busy over time by category"></svg></div>
        <div class="pf-legend" id="pf-legend"></div>
      </div>

      <div class="panel pad pf-panel" role="region" aria-labelledby="pf-fl-h">
        <div class="pf-controls">
          <h3 id="pf-fl-h">Where the CPU time goes</h3>
          <div class="pf-group" role="group" aria-label="Samples to include">
            <button data-pf-lens="all">All the time</button>
            <button data-pf-lens="block">While a block lands</button>
          </div>
          <input type="search" id="pf-search" placeholder="Highlight functions…" aria-label="Highlight functions by name">
          <span class="pf-note" id="pf-search-note"></span>
        </div>
        <div class="pf-group" id="pf-views" role="group" aria-label="View"></div>
        <p class="pf-note" id="pf-fl-note"></p>
        <div class="pf-crumb" id="pf-crumb"></div>
        <div class="pf-chart"><svg id="pf-flame" role="img" aria-label="Flame graph of CPU time by call stack"></svg></div>
        <div class="pf-empty" id="pf-flame-empty" hidden></div>
        <div class="pf-detail" id="pf-detail" hidden>
          <div class="pf-name" id="pf-detail-name"></div>
          <div class="pf-facts" id="pf-detail-facts"></div>
          <div class="pf-group">
            <button id="pf-focus-fn">Show this function everywhere it runs</button>
            <button id="pf-zoom-out">Zoom out</button>
          </div>
        </div>
      </div>

      <div class="panel pad pf-panel" role="region" aria-labelledby="pf-st-h" id="pf-stages-panel" hidden>
        <div class="pf-controls">
          <h3 id="pf-st-h">A block's path through the node</h3>
          <div class="pf-group" id="pf-paths" role="group" aria-label="How the blocks arrived"></div>
          <div class="pf-group" role="group" aria-label="Stages to show">
            <button data-pf-scale="all">Whole path</button>
            <button data-pf-scale="inside">After the download</button>
          </div>
          <button id="pf-stage-median" hidden>Back to the typical block</button>
        </div>
        <p class="pf-note" id="pf-st-note"></p>
        <div class="pf-chart"><svg id="pf-stages" role="img" aria-label="Time a block spends in each stage"></svg></div>
        <div class="pf-chart"><table id="pf-block-table"></table></div>
      </div>

      <div class="panel pad pf-panel" role="region" aria-labelledby="pf-top-h">
        <h3 id="pf-top-h">Functions with the most time of their own</h3>
        <p class="pf-note">Time sampled in the function itself, not in what it calls, added up over every place it appears in this view. Click one to see everything under it.</p>
        <div class="pf-chart"><table id="pf-top"></table></div>
      </div>

      <details class="panel pad pf-panel pf-about">
        <summary>What the profile shows and what it leaves out</summary>
        <ul class="pf-notes" id="pf-notes"></ul>
      </details>
      <div class="pf-tip" id="pf-tip" hidden></div>
    </section>
  </div>

  <footer>For snapshots and install instructions, see
    <a href="https://zakura.com/snapshots/" target="_blank" rel="noopener">https://zakura.com/snapshots/</a>
  </footer>
</main>
<script>
const REFRESH_MS = 10000;

const state = {
  data: null,
  query: '',
  issuesOnly: false,
  sortKey: 'name',
  sortDir: 1,
  expanded: new Set(),
  expandAll: false,
  fetchedAt: null,
  error: '',
};

const el = (id) => document.getElementById(id);

/* ---------- routing ---------- */
// One template serves both routes: / renders the fleet, /node/<name> renders one
// node. Keeping a single PAGE means the CSS and formatters are shared verbatim.
const NODE_ROUTE = /^\/node\/(.+)$/.exec(location.pathname);
const NODE_NAME = NODE_ROUTE ? decodeURIComponent(NODE_ROUTE[1]) : null;
for (const section of document.querySelectorAll('[data-view]')) {
  section.hidden = (section.dataset.view === 'node') !== Boolean(NODE_NAME);
}

/* ---------- formatting ---------- */
function esc(value) {
  return String(value == null ? '' : value).replace(/[&<>"']/g, (match) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[match]));
}
function num(value) {
  return value == null ? '—' : Number(value).toLocaleString('en-US');
}
function age(seconds) {
  if (seconds == null) return '—';
  if (seconds < 60) return Math.round(seconds) + 's';
  if (seconds < 3600) return Math.round(seconds / 60) + 'm';
  if (seconds < 86400) return Math.round(seconds / 3600) + 'h';
  return Math.round(seconds / 86400) + 'd';
}
function bytes(value) {
  if (value == null || !isFinite(value)) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let scaled = Number(value);
  let unit = 0;
  while (scaled >= 1024 && unit < units.length - 1) {
    scaled /= 1024;
    unit += 1;
  }
  return (unit > 0 && scaled < 100 ? scaled.toFixed(1) : Math.round(scaled)) + ' ' + units[unit];
}
function pctText(value) {
  return value == null || !isFinite(value) ? '—' : Number(value).toFixed(1) + '%';
}
function duration(seconds) {
  if (seconds == null || !isFinite(seconds)) return '—';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return days + 'd ' + hours + 'h';
  if (hours > 0) return hours + 'h ' + minutes + 'm';
  return minutes + 'm';
}
function decimal(value, places) {
  return value == null || !isFinite(value) ? '—' : Number(value).toFixed(places == null ? 2 : places);
}
function metric(metrics, name) {
  const value = (metrics || {})[name];
  return typeof value === 'number' && isFinite(value) ? value : null;
}
function clockTime(epochSeconds) {
  if (!epochSeconds) return '—';
  return new Date(epochSeconds * 1000).toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
  });
}
function middleHash(value, left, right) {
  if (!value) return '—';
  if (value.length <= left + right + 2) return value;
  return value.slice(0, left) + '…' + value.slice(-right);
}
function shortHash(hash) { return middleHash(hash, 8, 8); }
function tinyHash(hash) { return middleHash(hash, 6, 6); }
function shortCommit(commit) { return commit ? commit.slice(0, 9) : '—'; }
function formatRestarted(value) {
  if (!value) return '—';
  const raw = String(value).trim();
  // ISO / docker: 2026-07-28T01:33:37.986819514Z
  let match = raw.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::\d{2})?(?:\.\d+)?Z?$/);
  if (match) return match[2] + '-' + match[3] + ' ' + match[4] + ':' + match[5];
  // systemctl: Tue 2026-07-28 01:20:42 UTC
  match = raw.match(/^[A-Za-z]{3}\s+(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})(?::\d{2})?\s+UTC$/);
  if (match) return match[2] + '-' + match[3] + ' ' + match[4] + ':' + match[5];
  // ps / zcashd: Tue Jul 28 01:33:18 2026
  match = raw.match(/^[A-Za-z]{3}\s+([A-Za-z]{3})\s+(\d{1,2})\s+(\d{2}):(\d{2})(?::\d{2})?\s+(\d{4})$/);
  if (match) {
    const months = {
      Jan: '01', Feb: '02', Mar: '03', Apr: '04', May: '05', Jun: '06',
      Jul: '07', Aug: '08', Sep: '09', Oct: '10', Nov: '11', Dec: '12',
    };
    const month = months[match[1]] || match[1];
    return month + '-' + String(match[2]).padStart(2, '0') + ' ' + match[3] + ':' + match[4];
  }
  return raw;
}

/* ---------- tone mapping ---------- */
const HEALTH_TONE = {
  healthy: 'ok', stale: 'warn', verification_error: 'bad', rpc_error: 'bad', down: 'bad', starting: 'neutral',
};
const CHAIN_TONE = {
  majority: 'ok', behind: 'warn', ahead: 'warn', fork: 'bad', unknown: 'neutral',
};
const STATUS_TONE = { agreed: 'ok', lagging: 'warn', split: 'bad', unknown: 'neutral' };
const HEALTH_LABEL = { rpc_error: 'rpc error' };
function tone(map, value) { return map[value] || 'neutral'; }
function badge(text, toneName) {
  const cls = toneName && toneName !== 'neutral' ? ' tone-' + toneName : '';
  return '<span class="badge' + cls + '">' + esc(text) + '</span>';
}
// A link to the node's CPU flame graphs, for nodes that run the continuous profiler.
function profilerBadge(row) {
  const profiler = row.profiler;
  if (!profiler) return '';
  const working = profiler.state === 'ok';
  const title = working
    ? 'Continuous CPU profile: sampling at ' + profiler.rate_hz + ' Hz, using '
      + (profiler.overhead_pct_last_hour == null ? 'under 1' : profiler.overhead_pct_last_hour)
      + '% of one core. Open the flame graph.'
    : 'Continuous CPU profiler is ' + profiler.state + '. Open the CPU profile.';
  return '<a class="node-link badge tone-' + (working ? 'info' : 'warn') + '" href="/node/'
    + encodeURIComponent(row.name) + '#profile" title="' + esc(title) + '">'
    + (working ? 'profiled' : 'profiler ' + esc(profiler.state)) + '</a>';
}
function tipEventLabel(event) {
  if (event === 'reorg_height_drop') return 'height drop';
  if (event === 'tip_switch') return 'tip switch';
  return '';
}
function chainStatusLabel(status) {
  return { agreed: 'Agreed', lagging: 'Lagging', split: 'Split' }[status] || 'Unknown';
}

/* ---------- copy to clipboard ---------- */
const copyIcon = '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
const checkIcon = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 6 9 17l-5-5"></path></svg>';
function copyButton(value, label) {
  if (!value) return '';
  return '<button class="copy-button" type="button" data-copy="' + esc(value) + '"'
    + ' aria-label="' + esc(label) + '" title="' + esc(label) + '">' + copyIcon + '</button>';
}
async function copyValue(button) {
  const value = button.dataset.copy || '';
  if (!value) return;
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(value);
    } else {
      const textarea = document.createElement('textarea');
      textarea.value = value;
      textarea.setAttribute('readonly', '');
      textarea.style.cssText = 'position:fixed;top:-1000px;left:-1000px';
      document.body.appendChild(textarea);
      textarea.select();
      try { document.execCommand('copy'); } finally { textarea.remove(); }
    }
    button.classList.add('is-done');
    button.innerHTML = checkIcon;
  } catch (error) {
    button.innerHTML = '!';
  }
  setTimeout(() => {
    button.classList.remove('is-done');
    button.innerHTML = copyIcon;
  }, 1400);
}
function copyCell(value, shortened, label) {
  if (!value) return '<span class="dim">—</span>';
  return '<span class="copyable"><span title="' + esc(value) + '">' + esc(shortened) + '</span>'
    + copyButton(value, label) + '</span>';
}

/* ---------- rendering ---------- */
function renderHeader(data) {
  const chain = data.chain || {};
  const network = data.network || 'mainnet';
  el('network-chip').textContent = network;
  document.title = 'Zakura ' + network + ' cluster status';

  let label = 'Healthy';
  let toneName = 'ok';
  if (chain.status === 'split') {
    label = 'Tip split';
    toneName = 'bad';
  } else if (data.healthy !== data.total) {
    label = 'Degraded';
    toneName = data.healthy === 0 ? 'bad' : 'warn';
  } else if (chain.status === 'lagging') {
    label = 'Lagging';
    toneName = 'warn';
  }
  const pill = el('state-pill');
  pill.textContent = label;
  pill.className = 'state-pill is-' + toneName;
}
function renderFreshness() {
  const node = el('freshness');
  if (state.error) {
    node.innerHTML = '<b>offline</b>';
    return;
  }
  if (!state.fetchedAt) {
    node.textContent = 'connecting...';
    return;
  }
  const seconds = Math.max(0, Math.round((Date.now() - state.fetchedAt) / 1000));
  const next = Math.max(0, Math.ceil(REFRESH_MS / 1000) - seconds);
  node.innerHTML = 'updated <b>' + seconds + 's</b> ago &middot; next in ' + next + 's';
}
function renderFleet(data) {
  const rows = data.rows || [];
  const counts = {};
  for (const row of rows) {
    const health = row.health || 'unknown';
    counts[health] = (counts[health] || 0) + 1;
  }
  el('healthy-count').textContent = data.healthy + ' / ' + data.total;
  const unhealthy = data.total - data.healthy;
  el('healthy-sub').textContent = unhealthy === 0
    ? 'every node is active, serving RPC, and advancing'
    : unhealthy + ' node' + (unhealthy === 1 ? '' : 's') + ' need attention';

  const order = ['healthy', 'stale', 'verification_error', 'rpc_error', 'down', 'starting'];
  const present = order.filter((key) => counts[key]);
  for (const key of Object.keys(counts)) {
    if (!present.includes(key)) present.push(key);
  }
  el('health-bar').innerHTML = present.map((key) =>
    '<span class="seg-' + tone(HEALTH_TONE, key) + '" style="flex-grow:' + counts[key] + '"'
    + ' title="' + esc(key + ': ' + counts[key]) + '"></span>'
  ).join('');
  el('health-legend').innerHTML = present.map((key) =>
    '<span><i class="seg-' + tone(HEALTH_TONE, key) + '"></i>'
    + '<b>' + counts[key] + '</b>&nbsp;' + esc(HEALTH_LABEL[key] || key) + '</span>'
  ).join('');

  const clients = {};
  for (const row of rows) {
    const client = row.client_name || 'unknown';
    clients[client] = (clients[client] || 0) + 1;
  }
  const profiled = rows.filter((row) => row.profiler).length;
  el('client-mix').textContent = (Object.keys(clients).sort()
    .map((client) => clients[client] + ' ' + client).join(' · ') || 'no clients')
    + (profiled ? ' · CPU profile on ' + profiled + ' of ' + rows.length : '');
  el('propagation-link').hidden = !profiled;

  const heights = rows.map((row) => row.height).filter((height) => height != null);
  el('fleet-max-height').textContent = heights.length ? num(Math.max(...heights)) : '—';
  el('fleet-spread').textContent = heights.length
    ? (Math.max(...heights) - Math.min(...heights)) + ' block spread'
    : 'no heights reported';

  const slowest = pickWorst(rows, (row) => row.seconds_since_advanced);
  el('fleet-slowest').textContent = slowest ? age(slowest.seconds_since_advanced) : '—';
  el('fleet-slowest-node').textContent = slowest ? slowest.name : 'no node advancing yet';

  const laggiest = pickWorst(rows, (row) => row.header_lag);
  el('fleet-max-lag').textContent = laggiest ? num(laggiest.header_lag) : '—';
  el('fleet-max-lag-node').textContent = laggiest
    ? (laggiest.header_lag ? laggiest.name : 'every node has caught up')
    : 'no headers reported';
}
function pickWorst(rows, pick) {
  let worst = null;
  for (const row of rows) {
    const value = pick(row);
    if (typeof value !== 'number') continue;
    if (worst === null || value > pick(worst)) worst = row;
  }
  return worst;
}
function renderStats(data) {
  const chain = data.chain || {};
  const status = chain.status || 'unknown';
  const tipGroups = chain.tip_groups || [];
  const reorgs = chain.recent_reorgs || [];

  el('tip-agreement').innerHTML = badge(chainStatusLabel(status), tone(STATUS_TONE, status));
  el('tip-agreement-detail').textContent =
    tipGroups.length + ' tip group' + (tipGroups.length === 1 ? '' : 's')
    + ' · ' + (chain.observed_tips || 0) + ' node' + (chain.observed_tips === 1 ? '' : 's') + ' observed';

  el('majority-tip').innerHTML = copyCell(
    chain.majority_hash, shortHash(chain.majority_hash), 'Copy majority tip hash');
  el('majority-height').textContent = chain.majority_height == null
    ? 'no tip yet'
    : 'height ' + num(chain.majority_height);

  el('reorg-count').textContent = String(reorgs.length);
  el('reorg-latest').textContent = reorgs.length
    ? 'latest ' + clockTime(reorgs[0].at)
    : 'none recorded';

  el('last-poll').textContent = data.last_poll
    ? new Date(data.last_poll * 1000).toLocaleTimeString()
    : 'not yet polled';
  el('stale-window').textContent =
    'stale after ' + Math.round(data.stale_after) + 's without a new block';
}
function tipGroupRole(group, chain) {
  const isMajority = group.height === chain.majority_height
    && group.block_hash === chain.majority_hash;
  if (isMajority) return 'majority';
  if (group.height === chain.majority_height) return 'fork';
  if (group.height > chain.majority_height) return 'ahead';
  return 'behind';
}
function renderChain(data) {
  const chain = data.chain || {};
  const status = chain.status || 'unknown';
  const tipGroups = chain.tip_groups || [];
  const reorgs = chain.recent_reorgs || [];

  el('chain-summary').textContent = {
    split: 'Nodes disagree on the tip hash at the leading height.',
    lagging: 'Nodes report different tip heights.',
    agreed: 'All observed nodes share the same tip.',
  }[status] || 'Waiting for tip observations.';

  el('tip-groups').innerHTML = tipGroups.length
    ? tipGroups.map((group) => {
      const role = tipGroupRole(group, chain);
      const isMajority = role === 'majority';
      const depth = group.fork_depth_label && !isMajority
        ? badge(group.fork_depth_label, 'warn')
        : '';
      return '<div class="tip-group is-' + role + '">'
        + '<div class="tip-group-top">'
        + badge(role, tone(CHAIN_TONE, role))
        + '<span class="height num">height ' + num(group.height) + '</span>'
        + '<span class="mono muted" style="font-size:0.78rem">'
        + copyCell(group.block_hash, shortHash(group.block_hash), 'Copy tip hash') + '</span>'
        + depth
        + '</div>'
        + '<div class="tip-group-count">' + group.count
        + ' node' + (group.count === 1 ? '' : 's') + '</div>'
        + '<div class="tip-group-nodes">' + esc((group.nodes || []).join(', ')) + '</div>'
        + '</div>';
    }).join('')
    : '<div class="empty">No tip hashes observed yet.</div>';

  el('reorg-list').innerHTML = reorgs.length
    ? reorgs.slice(0, 12).map((event) => {
      const discarded = event.discarded_hash || event.from_hash || '';
      const canonical = event.canonical_hash || event.to_hash || '';
      const depth = event.depth_label
        || (event.depth != null ? 'depth ' + event.depth : 'depth unknown');
      return '<div class="reorg-item">'
        + '<div class="reorg-top">'
        + '<strong>' + esc(event.node) + '</strong>'
        + badge(tipEventLabel(event.kind) || event.kind, 'warn')
        + badge(depth, 'neutral')
        + (event.demo ? badge('demo', 'info') : '')
        + '<span class="muted num" style="font-size:0.78rem">' + num(event.from_height)
        + ' → ' + num(event.to_height) + '</span>'
        + '</div>'
        + '<div class="reorg-detail mono">orphan ' + esc(tinyHash(discarded))
        + ' → tip ' + esc(tinyHash(canonical))
        + '<span class="dim"> · ' + esc(clockTime(event.at)) + '</span></div>'
        + '</div>';
    }).join('')
    : '<div class="empty">No orphan pairs yet. Height drops and tip switches are'
      + ' recorded here and survive dashboard restarts.</div>';
}

function matchesQuery(row, query) {
  if (!query) return true;
  return [
    row.name, row.commit, row.node_id, row.block_hash, row.detail,
    row.ssh, row.version, row.health, row.chain_role, row.client_name,
  ].some((value) => String(value || '').toLowerCase().includes(query));
}
function sortRows(rows) {
  const key = state.sortKey;
  const dir = state.sortDir;
  return rows.slice().sort((left, right) => {
    const a = left[key];
    const b = right[key];
    if (a == null && b == null) return left.name.localeCompare(right.name);
    if (a == null) return 1;
    if (b == null) return -1;
    if (typeof a === 'number' && typeof b === 'number') {
      return a === b ? left.name.localeCompare(right.name) : (a - b) * dir;
    }
    const compared = String(a).localeCompare(String(b));
    return compared === 0 ? left.name.localeCompare(right.name) : compared * dir;
  });
}
function fieldHtml(label, value, options) {
  const settings = options || {};
  if (value == null || value === '') return '';
  const text = settings.short ? settings.short : String(value);
  const copy = settings.copy ? copyButton(String(value), 'Copy ' + label.toLowerCase()) : '';
  const cls = 'field-value' + (settings.bad ? ' is-bad' : '') + (settings.mono ? ' mono' : '');
  return '<div class="field' + (settings.full ? ' full' : '') + '">'
    + '<span class="label">' + esc(label) + '</span>'
    + '<div class="' + cls + '"><span>' + esc(text) + '</span>' + copy + '</div>'
    + '</div>';
}
function drawerHtml(row) {
  const ancestors = row.ancestor_hashes || {};
  const depths = Object.keys(ancestors).sort((a, b) => Number(a) - Number(b));
  const ancestorHtml = depths.length
    ? '<div class="field full"><span class="label">Sampled best-chain ancestors</span>'
      + '<div class="ancestors">' + depths.map((depth) =>
        '<div><span>-' + esc(depth) + '</span><span>' + esc(ancestors[depth]) + '</span></div>'
      ).join('') + '</div></div>'
    : '';
  return '<div class="drawer-inner">'
    + fieldHtml('SSH target', row.ssh, { mono: true, copy: true })
    + fieldHtml('Client', (row.client_name || 'unknown') + ' ' + (row.client_version || ''))
    + fieldHtml('Version banner', row.version, { mono: true })
    + fieldHtml('Service state', row.active_state)
    + fieldHtml('RPC chain', String(row.rpc_chain || 'unknown')
      + (row.rpc_testnet ? ' (testnet)' : ''))
    + fieldHtml('Headers / blocks', num(row.headers) + ' / ' + num(row.height))
    + fieldHtml('Commit', row.commit, { mono: true, copy: true })
    + fieldHtml('Tip event', tipEventLabel(row.tip_event) || row.tip_event)
    + fieldHtml('Tip last advanced', row.last_advanced_at
      ? new Date(row.last_advanced_at * 1000).toLocaleString() : '')
    + fieldHtml('Last probed', row.last_seen_at
      ? new Date(row.last_seen_at * 1000).toLocaleString() : '')
    + fieldHtml('Restarted', row.last_restarted)
    + fieldHtml('Ironwood pool (zat)', row.ironwood_chain_balance_zat, { mono: true })
    + fieldHtml('Node ID', row.node_id, { mono: true, copy: true, full: true })
    + fieldHtml('Tip hash', row.block_hash, { mono: true, copy: true, full: true })
    + fieldHtml('Parent hash', row.previous_hash, { mono: true, copy: true, full: true })
    + ancestorHtml
    + fieldHtml('Detail', row.detail, { full: true })
    + fieldHtml('Ironwood pool error', row.ironwood_pool_error, { bad: true, full: true })
    + fieldHtml('RPC metadata error', row.rpc_metadata_error, { bad: true, full: true })
    + '</div>';
}
// Silent-failure classes that the fleet table had no way to show before: a node
// filling its disk, or one the kernel has been killing.
function vitalBadges(row) {
  const vitals = row.vitals || {};
  const badges = [];
  const diskPct = vitals.disk_free_pct;
  if (diskPct != null && diskPct < 20) {
    badges.push(badge('disk ' + pctText(diskPct), diskPct < 10 ? 'bad' : 'warn'));
  }
  if (vitals.oom_kills_24h) {
    badges.push(badge('oom ' + vitals.oom_kills_24h, 'bad'));
  }
  return badges.join('');
}
function renderTable(data) {
  const chain = data.chain || {};
  const majorityHeight = chain.majority_height;
  const all = data.rows || [];
  const visible = sortRows(all.filter((row) =>
    (!state.issuesOnly || !row.healthy) && matchesQuery(row, state.query)));

  el('table-title').textContent = visible.length === all.length
    ? all.length + ' nodes'
    : visible.length + ' of ' + all.length + ' nodes';

  for (const th of document.querySelectorAll('thead th[data-sort]')) {
    const active = th.dataset.sort === state.sortKey;
    th.classList.toggle('is-sorted', active);
    th.querySelector('.arrow').textContent = active ? (state.sortDir === 1 ? '↑' : '↓') : '';
  }

  if (!visible.length) {
    el('rows').innerHTML = '<tr><td colspan="10"><div class="empty">'
      + (all.length ? 'No nodes match the current filter.' : 'Waiting for the first poll.')
      + '</div></td></tr>';
    return;
  }

  el('rows').innerHTML = visible.map((row) => {
    const healthTone = tone(HEALTH_TONE, row.health);
    const chainTone = tone(CHAIN_TONE, row.chain_role);
    const open = state.expandAll || state.expanded.has(row.name);
    const tipLabel = tipEventLabel(row.tip_event);
    const behind = (majorityHeight != null && row.height != null)
      ? row.height - majorityHeight
      : null;
    const deltaHtml = behind
      ? '<span class="delta">' + (behind > 0 ? '+' : '') + behind + ' vs majority</span>'
      : '';
    const rowTone = healthTone === 'bad' || chainTone === 'bad'
      ? 'row-bad'
      : (healthTone === 'warn' || chainTone === 'warn' ? 'row-warn' : '');
    const lag = row.header_lag;
    const lagHtml = lag == null
      ? '<span class="dim">—</span>'
      : (lag > 0 ? '<span style="color:var(--warn)">' + num(lag) + '</span>' : '0');

    return '<tr class="node-row ' + rowTone + (open ? ' is-open' : '') + '"'
      + ' data-node="' + esc(row.name) + '">'
      + '<td class="col-node"><div class="node-cell"><span class="twisty">&#9654;</span>'
      + '<div class="stack"><a class="node-link node-name" href="/node/'
      + encodeURIComponent(row.name) + '" title="Open node detail">' + esc(row.name) + '</a>'
      + '<span class="node-sub" title="' + esc(row.node_id || '') + '">'
      + esc(row.node_id ? tinyHash(row.node_id) : 'node id unknown') + '</span>'
      + profilerBadge(row) + '</div></div></td>'
      + '<td class="col-health"><div class="stack">'
      + badge(HEALTH_LABEL[row.health] || row.health, healthTone) + vitalBadges(row) + '</div></td>'
      + '<td class="col-chain"><div class="stack">'
      + badge(row.chain_role || 'unknown', chainTone)
      + (tipLabel ? badge(tipLabel, 'bad') : '') + '</div></td>'
      + '<td class="col-height col-num mono"><div class="stack">'
      + '<span>' + num(row.height) + '</span>' + deltaHtml + '</div></td>'
      + '<td class="col-hdr col-num mono">' + lagHtml + '</td>'
      + '<td class="col-adv col-num mono" title="'
      + esc(row.seconds_since_advanced == null ? 'never' : row.seconds_since_advanced + 's') + '">'
      + esc(age(row.seconds_since_advanced)) + '</td>'
      + '<td class="col-commit mono">'
      + copyCell(row.commit, shortCommit(row.commit), 'Copy full commit hash') + '</td>'
      + '<td class="col-tip mono">'
      + copyCell(row.block_hash, tinyHash(row.block_hash), 'Copy full tip hash') + '</td>'
      + '<td class="col-restarted mono num" title="' + esc(row.last_restarted || '') + '">'
      + esc(formatRestarted(row.last_restarted)) + '</td>'
      + '<td class="col-detail"><div class="detail-text" title="' + esc(row.detail || '') + '">'
      + esc(row.detail || '') + '</div></td>'
      + '</tr>'
      + (open
        ? '<tr class="drawer"><td colspan="10">' + drawerHtml(row) + '</td></tr>'
        : '');
  }).join('');
}
/* ---------- node view ---------- */
function kvRow(label, text, toneName) {
  return kvRowHtml(label, esc(text), toneName);
}
function kvRowHtml(label, html, toneName) {
  return '<div class="kv-row' + (toneName ? ' is-' + toneName : '') + '">'
    + '<span>' + esc(label) + '</span><b>' + html + '</b></div>';
}
function vitalTile(label, value, options) {
  const settings = options || {};
  const toneClass = settings.tone ? ' is-' + settings.tone : '';
  const meter = settings.fill == null
    ? ''
    : '<div class="meter' + toneClass + '"><i style="width:'
      + Math.max(0, Math.min(100, settings.fill)).toFixed(1) + '%"></i></div>';
  return '<div class="vital' + toneClass + '">'
    + '<span class="label">' + esc(label) + '</span>'
    + '<strong>' + esc(value) + '</strong>'
    + (settings.sub ? '<small>' + esc(settings.sub) + '</small>' : '')
    + meter + '</div>';
}
function sparkline(values, stroke) {
  const points = values.filter((value) => value != null && isFinite(value));
  if (points.length < 2) return '<div class="spark-empty">not enough samples yet</div>';
  let min = Infinity;
  let max = -Infinity;
  for (const value of points) {
    if (value < min) min = value;
    if (value > max) max = value;
  }
  const span = (max - min) || 1;
  const width = 100;
  const height = 30;
  const step = width / (points.length - 1);
  const coords = points.map((value, index) =>
    (index * step).toFixed(2) + ',' + (height - ((value - min) / span) * height).toFixed(2));
  return '<svg viewBox="0 0 ' + width + ' ' + height + '" preserveAspectRatio="none" aria-hidden="true">'
    + '<polyline points="' + coords.join(' ') + '" fill="none" stroke="' + stroke + '"'
    + ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"'
    + ' vector-effect="non-scaling-stroke"></polyline></svg>';
}
function sparkCard(title, current, values, stroke) {
  return '<div class="spark"><div class="spark-head"><span class="label">' + esc(title) + '</span>'
    + '<b>' + esc(current) + '</b></div>' + sparkline(values, stroke) + '</div>';
}
function advanceRate(history) {
  const points = history.filter((sample) => sample.height != null);
  if (points.length < 2) return null;
  const first = points[0];
  const last = points[points.length - 1];
  const elapsed = last.t - first.t;
  if (elapsed <= 0) return null;
  return ((last.height - first.height) / elapsed) * 60;
}
function endpointState(entry) {
  if (!entry) return { tone: 'neutral', text: 'unknown' };
  if (entry.error) return { tone: 'bad', text: 'unreachable' };
  if (entry.status === 200) return { tone: 'ok', text: entry.body || 'ok' };
  return { tone: 'warn', text: String(entry.status || '?') + ' ' + (entry.body || '') };
}
function renderNodeHeader(data) {
  const row = data.node || {};
  const network = data.network || 'mainnet';
  const name = row.name || NODE_NAME;
  el('node-network-chip').textContent = network;
  document.title = name + ' - Zakura ' + network + ' node';
  el('node-title').textContent = name;
  const polledAge = data.last_poll == null ? null : (Date.now() / 1000) - data.last_poll;
  el('node-subtitle').textContent = [
    (row.client_name || 'node') + ' ' + (row.client_version || row.version || ''),
    row.commit ? 'commit ' + shortCommit(row.commit) : '',
    // Ticks on every refresh, so a frozen page is obvious at a glance.
    data.last_poll == null ? 'waiting for the first poll' : 'probed ' + age(polledAge) + ' ago',
  ].filter(Boolean).join(' · ');

  const healthTone = tone(HEALTH_TONE, row.health);
  el('node-badges').innerHTML = badge(HEALTH_LABEL[row.health] || row.health || 'unknown', healthTone)
    + badge(row.chain_role || 'unknown', tone(CHAIN_TONE, row.chain_role))
    + badge('service ' + (row.active_state || 'unknown'),
      row.active_state === 'active' ? 'ok' : 'bad')
    + (row.tip_event ? badge(tipEventLabel(row.tip_event) || row.tip_event, 'bad') : '')
    + profilerBadge(row);

  const pill = el('state-pill');
  pill.textContent = { healthy: 'Healthy', stale: 'Stale', verification_error: 'Verification error', rpc_error: 'RPC error', down: 'Down' }[row.health]
    || 'Starting';
  pill.className = 'state-pill is-' + (healthTone === 'neutral' ? 'warn' : healthTone);
}
function renderNodeVitals(data) {
  const row = data.node || {};
  const host = row.host || {};
  const health = row.health_endpoint || {};
  const tiles = [];

  const diskPct = (row.vitals || {}).disk_free_pct;
  const diskTone = diskPct == null ? null : (diskPct < 10 ? 'bad' : (diskPct < 20 ? 'warn' : null));
  tiles.push(vitalTile('Disk free', pctText(diskPct), {
    tone: diskTone,
    fill: diskPct,
    sub: host.disk_error
      ? 'error: ' + host.disk_error
      : bytes(host.disk_free_bytes) + ' of ' + bytes(host.disk_total_bytes)
        + (host.disk_path ? ' on ' + host.disk_path : ''),
  }));

  const memTotal = host.mem_total_bytes;
  const memFree = host.mem_available_bytes;
  const memPct = memTotal && memFree != null ? (100 * memFree) / memTotal : null;
  tiles.push(vitalTile('Memory available', pctText(memPct), {
    tone: memPct == null ? null : (memPct < 8 ? 'bad' : (memPct < 15 ? 'warn' : null)),
    fill: memPct,
    sub: bytes(memFree) + ' of ' + bytes(memTotal),
  }));

  tiles.push(vitalTile('Process RSS', bytes(host.rss_bytes), {
    sub: host.pid ? 'pid ' + host.pid : (host.rss_error || 'pid unknown'),
  }));
  tiles.push(vitalTile('Load (1m)', decimal(host.load1), {
    sub: '5m ' + decimal(host.load5) + ' · 15m ' + decimal(host.load15),
  }));
  tiles.push(vitalTile('Host uptime', duration(host.uptime_seconds), {
    sub: 'since boot',
  }));
  tiles.push(vitalTile('Service restarts', host.restart_count == null ? '—' : num(host.restart_count), {
    tone: host.restart_count ? 'warn' : null,
    // systemd NRestarts counts Restart= policy restarts, so this is a crash
    // counter rather than a deploy counter.
    sub: row.last_restarted ? 'last start ' + formatRestarted(row.last_restarted) : 'automatic restarts',
  }));
  tiles.push(vitalTile('OOM kills (24h)', host.oom_kills_24h == null ? '—' : num(host.oom_kills_24h), {
    tone: host.oom_kills_24h ? 'bad' : null,
    sub: host.oom_error ? 'error: ' + host.oom_error : 'kernel log',
  }));

  if (row.health_endpoint_error) {
    tiles.push(vitalTile('Readiness', 'not enabled', { sub: row.health_endpoint_error }));
  } else {
    const healthy = endpointState(health.healthy);
    const ready = endpointState(health.ready);
    tiles.push(vitalTile('/healthy', healthy.text, { tone: healthy.tone, sub: 'peer threshold' }));
    tiles.push(vitalTile('/ready', ready.text, { tone: ready.tone, sub: 'close to tip' }));
  }

  el('node-vitals').innerHTML = tiles.join('');
  el('node-vitals-note').textContent = host.disk_path
    ? 'State directory ' + host.disk_path
    : 'Collected over the dashboard ssh probe';
}
function renderNodeSparks(data) {
  const history = data.history || [];
  const row = data.node || {};
  const host = row.host || {};
  const rate = advanceRate(history);

  el('node-sparks-title').textContent = 'Last ' + duration(data.history_window);
  el('node-sparks-note').textContent = history.length
    ? history.length + ' samples · ' + (rate == null ? 'rate unknown' : decimal(rate, 1) + ' blocks/min')
    : 'no samples yet';

  el('node-sparks').innerHTML = [
    sparkCard('Height', num(row.height), history.map((s) => s.height), 'var(--ok)'),
    sparkCard('Header lag', row.header_lag == null ? '—' : num(row.header_lag),
      history.map((s) => s.header_lag), 'var(--warn)'),
    sparkCard('Distance to tip', row.metrics_error ? 'n/a' : num(metric(row.metrics, 'sync_estimated_distance_to_tip')),
      history.map((s) => s.sync_lag), 'var(--pink-hi)'),
    sparkCard('Peers', row.peer_count == null ? '—' : num(row.peer_count),
      history.map((s) => s.peers), 'var(--ok)'),
    sparkCard('Disk free', bytes(host.disk_free_bytes),
      history.map((s) => s.disk_free_bytes), 'var(--warn)'),
    sparkCard('Process RSS', bytes(host.rss_bytes),
      history.map((s) => s.rss_bytes), 'var(--pink-hi)'),
    sparkCard('Load (1m)', decimal(host.load1), history.map((s) => s.load1), 'var(--ink-2)'),
  ].join('');
}
function renderNodeChain(data) {
  const row = data.node || {};
  const majority = data.majority_height;
  const delta = (majority != null && row.height != null) ? row.height - majority : null;
  const rate = advanceRate(data.history || []);
  const rows = [
    kvRow('Height', num(row.height)),
    kvRow('Headers', num(row.headers)),
    kvRow('Header lag', row.header_lag == null ? '—' : num(row.header_lag),
      row.header_lag ? 'warn' : null),
    kvRow('vs fleet majority', delta == null ? '—' : (delta > 0 ? '+' : '') + num(delta),
      delta ? 'warn' : null),
    kvRow('Advance rate', rate == null ? '—' : decimal(rate, 1) + ' blocks/min'),
    kvRow('Tip last advanced', age(row.seconds_since_advanced) + ' ago'),
    kvRowHtml('Tip hash', copyCell(row.block_hash, shortHash(row.block_hash), 'Copy tip hash')),
    kvRowHtml('Parent hash', copyCell(row.previous_hash, shortHash(row.previous_hash), 'Copy parent hash')),
    kvRow('RPC chain', String(row.rpc_chain || 'unknown')),
    kvRow('Ironwood pool (zat)', row.ironwood_chain_balance_zat || '—'),
    kvRow('Mempool', row.mempool_size == null ? '—' : num(row.mempool_size) + ' tx / ' + bytes(row.mempool_bytes)),
  ];
  if (row.detail) rows.push(kvRow('Detail', row.detail));
  el('node-chain').innerHTML = rows.join('');
}
function metricsUnavailable(row) {
  return '<p class="note-off">' + esc(row.metrics_error || 'metrics endpoint not enabled')
    + '. Set <code>[metrics] endpoint_addr</code> on this node to populate this panel.</p>';
}
// Builds only the rows whose series this build actually emits. The header-sync
// metric family was renamed between releases, so a fixed row list would leave a
// column of em-dashes on one build or the other.
function metricRows(metrics, spec) {
  return spec
    .filter((entry) => metric(metrics, entry[1]) != null)
    .map((entry) => {
      const value = metric(metrics, entry[1]);
      const format = entry[2];
      return kvRow(entry[0], format ? format(value) : num(Math.round(value)), entry[3]);
    })
    .join('');
}
const SYNC_ROWS = [
  ['Distance to tip', 'sync_estimated_distance_to_tip'],
  ['Network tip height', 'sync_estimated_network_tip_height'],
  ['Finalized height', 'state_finalized_block_height'],
  ['Header sync last progress', 'sync_header_work_last_progress_age_seconds', (v) => age(v)],
  ['Oldest missing header age', 'sync_header_work_oldest_missing_age_seconds', (v) => age(v)],
  ['Oldest missing height', 'sync_header_work_oldest_missing_height'],
  ['Header work in flight', 'sync_header_work_in_flight_count'],
  ['Header work pending', 'sync_header_work_pending_count'],
  ['Header work buffered', 'sync_header_work_buffered_count'],
  ['Header work committing', 'sync_header_work_committing_count'],
  ['Header work epoch', 'sync_header_work_epoch'],
  ['Root auth lead', 'sync_header_root_auth_lead_blocks'],
  ['Root auth batches in flight', 'sync_header_root_auth_work_in_flight_batches'],
  ['Header verification lag', 'sync_header_verification_lag'],
  ['Headers/sec', 'sync_header_headers_per_second', (v) => decimal(v, 1)],
  ['Headers received', 'sync_header_headers_received_total'],
  ['Header failures', 'sync_header_failure_total'],
  ['Header peer violations', 'sync_header_peer_violation'],
  ['Best header tip', 'sync_block_best_header_tip_height'],
  ['Verified tip', 'sync_block_verified_tip_height'],
  ['Blocks applying', 'sync_block_applying'],
  ['Blocks outstanding', 'sync_block_outstanding'],
  ['Backlog at cap', 'sync_block_backlog_at_cap'],
  ['Missing bodies', 'sync_block_missing_bodies'],
  ['Reorder buffered', 'sync_block_reorder_buffered_bytes', (v) => bytes(v)],
  ['Block budget reserved', 'sync_block_budget_reserved_bytes', (v) => bytes(v)],
  ['DAG nodes', 'sync_header_chain_dag_nodes'],
  ['DAG leaf tips', 'sync_header_chain_dag_leaf_tips'],
  ['DAG eligible tips', 'sync_header_chain_dag_eligible_tips'],
  ['Frontier divergence', 'sync_header_chain_frontier_divergence'],
  ['Reorg depth', 'sync_header_chain_reorg_depth'],
  ['Non-finalized chains', 'state_memory_chain_count'],
  ['Best chain length', 'state_memory_best_chain_length'],
  ['Apply phase', 'sync_zakura_apply_phase'],
  ['Legacy fallback', 'sync_zakura_legacy_fallback_active',
    (v) => (v ? 'engaged' : 'inactive')],
];
function renderNodeSync(data) {
  const row = data.node || {};
  if (row.metrics_error) {
    el('node-sync').innerHTML = metricsUnavailable(row);
    return;
  }
  const m = row.metrics || {};
  el('node-sync').innerHTML = '<div class="kv">' + metricRows(m, SYNC_ROWS) + '</div>'
    + '<p class="note-off">'
    + (row.metrics_version ? 'exporter ' + esc(row.metrics_version) + ' · ' : '')
    + 'scraped ' + age(row.metrics_at == null ? null : (Date.now() / 1000) - row.metrics_at) + ' ago'
    + (row.metrics_series ? ' · ' + num(row.metrics_series) + ' series, ' + bytes(row.metrics_bytes) : '')
    + (row.metrics_scrape_seconds ? ' in ' + decimal(row.metrics_scrape_seconds) + 's' : '')
    + '</p>';
}
const PEER_ROWS = [
  ['Legacy peer set', 'zcash_net_peers'],
  ['Native connections', 'zakura_p2p_conn_active'],
  ['Native connected peers', 'zakura_p2p_connected_peers'],
  ['Native healthy peers', 'zakura_p2p_healthy_peers'],
  ['Reactor connections', 'zakura_p2p_reactor_active_connections'],
  ['Pool ready', 'pool_num_ready'],
  ['Pool unready', 'pool_num_unready'],
  ['Candidates responded', 'candidate_set_responded'],
  ['Candidates recently live', 'candidate_set_recently_live'],
  ['Candidates gossiped', 'candidate_set_gossiped'],
  ['Candidates failed', 'candidate_set_failed'],
  ['Handshakes in flight', 'crawler_in_flight_handshakes'],
  ['Handshake failures', 'zcash_net_peer_handshake_failures_total'],
  ['Mempool transactions', 'zcash_mempool_size_transactions'],
];
// zcashd names itself "MagicBean" when it connects; show the name people know it by.
function peerSoftwareName(agent) { return String(agent).replace('MagicBean', 'zcashd'); }
function renderNodePeers(data) {
  const row = data.node || {};
  const m = row.metrics || {};
  // Prefer getpeerinfo.subver (live peers). Fall back to the exporter
  // user_agent label on older zakurad builds that omit subver.
  const subversions = (row.peer_subversions || []).length
    ? row.peer_subversions
    : (row.peer_user_agents || []);
  let html = '<div class="kv">'
    + kvRow('Peers (RPC)', row.peer_count == null ? '—' : num(row.peer_count))
    + kvRow('Inbound', row.peer_inbound == null ? '—' : num(row.peer_inbound));
  if (!row.metrics_error) {
    html += metricRows(m, PEER_ROWS)
      + kvRow('Bytes in / out', bytes(metric(m, 'zcash_net_in_bytes_total'))
        + ' / ' + bytes(metric(m, 'zcash_net_out_bytes_total')));
  }
  html += '</div>';
  if (row.metrics_error) html += metricsUnavailable(row);
  if (subversions.length) {
    html += '<div class="subhead"><p class="eyebrow">Peer versions</p></div><div class="kv">'
      + subversions.map((entry) => kvRow(peerSoftwareName(entry[0]), num(entry[1]))).join('')
      + '</div>';
  } else if (row.peer_info_error) {
    html += '<p class="note-off">getpeerinfo unavailable: ' + esc(row.peer_info_error) + '</p>';
  }
  el('node-peers').innerHTML = html;
}
function renderNodeEvents(data) {
  const row = data.node || {};
  const reorgs = data.reorgs || [];
  el('node-reorgs').innerHTML = reorgs.length
    ? reorgs.slice(0, 12).map((event) => {
      const depth = event.depth_label
        || (event.depth != null ? 'depth ' + event.depth : 'depth unknown');
      return '<div class="reorg-item"><div class="reorg-top">'
        + badge(tipEventLabel(event.kind) || event.kind, 'warn')
        + badge(depth, 'neutral')
        + '<span class="muted num" style="font-size:0.78rem">' + num(event.from_height)
        + ' → ' + num(event.to_height) + '</span>'
        + '<span class="muted" style="font-size:0.78rem">' + esc(clockTime(event.at)) + '</span>'
        + '</div><div class="reorg-detail mono">' + esc(tinyHash(event.discarded_hash))
        + ' → ' + esc(tinyHash(event.canonical_hash)) + '</div></div>';
    }).join('')
    : '<div class="empty">No tip switches recorded for this node.</div>';

  // getinfo.errors is the node's most recent WARN/ERROR log line, not a health
  // verdict, so label it as such and show its age: a routine per-peer warning
  // overwrites this field continuously (see issue #655).
  const lastLog = row.node_errors || '';
  const lastLogAge = row.node_errors_at == null
    ? null
    : Math.max(0, (Date.now() / 1000) - row.node_errors_at);
  el('node-last-log').innerHTML = lastLog
    ? '<div class="kv">'
      + kvRow('Logged', age(lastLogAge) + ' ago', lastLogAge != null && lastLogAge < 300 ? 'warn' : null)
      + '</div><div class="log-lines"><code>' + esc(lastLog) + '</code></div>'
      + '<p class="note-off">The node\'s most recent warning or error log line, as reported by'
      + ' <code>getinfo.errors</code>. It is whatever logged last, not a health verdict.</p>'
    : '<div class="empty">No warning or error reported by getinfo.</div>';

  const logs = row.log_errors || [];
  if (logs.length) {
    el('node-logs').innerHTML = '<div class="log-lines">'
      + logs.map((line) => '<code>' + esc(line) + '</code>').join('') + '</div>';
  } else if (row.log_errors_suppressed) {
    el('node-logs').innerHTML = '<p class="note-off">Log lines are not served on this public page.'
      + ' Start the dashboard with <code>--expose-logs</code> to show the redacted tail.</p>';
  } else {
    el('node-logs').innerHTML = '<div class="empty">No recent errors or warnings in the node log.</div>';
  }
}
function renderNode(data) {
  renderNodeHeader(data);
  renderNodeVitals(data);
  renderNodeSparks(data);
  renderNodeChain(data);
  renderNodeSync(data);
  renderNodePeers(data);
  renderNodeEvents(data);
  renderFreshness();
  showProfile(data.node || {});
}

function render() {
  if (!state.data) return;
  if (NODE_NAME) {
    renderNode(state.data);
    return;
  }
  renderHeader(state.data);
  renderFleet(state.data);
  renderStats(state.data);
  renderChain(state.data);
  renderTable(state.data);
  renderFreshness();
}

/* ---------- data ---------- */
async function tick() {
  const endpoint = NODE_NAME ? '/data/node/' + encodeURIComponent(NODE_NAME) : '/data';
  try {
    const response = await fetch(endpoint, { cache: 'no-store' });
    if (!response.ok) throw new Error('HTTP ' + response.status);
    state.data = await response.json();
    state.fetchedAt = Date.now();
    state.error = '';
    el('banner').hidden = true;
  } catch (error) {
    state.error = String(error);
    const banner = el('banner');
    banner.hidden = false;
    banner.textContent = 'Dashboard data endpoint is unreachable (' + state.error + '). '
      + (state.data ? 'Showing the last successful poll.' : 'No poll has succeeded yet.');
    const pill = el('state-pill');
    pill.textContent = 'Unreachable';
    pill.className = 'state-pill is-bad';
    renderFreshness();
    return;
  }
  render();
}

/* ---------- node page: jump bar ---------- */
// The bar scrolls instead of linking, so the URL fragment stays free for the CPU
// profile's own view (#profile&range=...).
function setupNodeNav() {
  const nav = el('node-nav');
  nav.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-jump]');
    if (button) el(button.dataset.jump).scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
  const targets = [...document.querySelectorAll('#node-view [data-jump-target]')];
  const mark = () => {
    // The section being read is the last one whose top has passed just under the bar.
    let current = targets[0];
    for (const target of targets) {
      if (!target.hidden && target.getBoundingClientRect().top <= 96) current = target;
    }
    for (const button of nav.querySelectorAll('button')) {
      button.setAttribute('aria-current', String(button.dataset.jump === current.id));
    }
  };
  addEventListener('scroll', mark, { passive: true });
  mark();
}

/* ---------- node page: CPU profile ---------- */
// Shown only for a node that runs the continuous profiler. Its data loads when the
// section comes near the screen, or at once when the URL asks for it.
let profileStarted = false;
function showProfile(row) {
  if (profileStarted || !row.profiler) return;
  profileStarted = true;
  el('profile').hidden = false;
  el('node-nav-profile').hidden = false;
  const begin = () => startProfile(row.name || NODE_NAME);
  if (location.hash.startsWith('#profile')) {
    begin();
    el('profile').scrollIntoView({ block: 'start' });
    return;
  }
  const observer = new IntersectionObserver((entries) => {
    if (!entries.some((entry) => entry.isIntersecting)) return;
    observer.disconnect();
    begin();
  }, { rootMargin: '600px 0px' });
  observer.observe(el('profile'));
}


// The node page's CPU profile section. It reads the node's profiler through the
// dashboard's /profile/<node>/api/ forwarder. Its view (range, category, lens,
// function) is kept in the URL fragment after "#profile", so a link can open it.
function startProfile(node) {
const BASE = "/profile/" + encodeURIComponent(node);
const ROW = 19, REFRESH_MS = 60000;
const VIEW_LABEL = { all: "Whole process" };
const el = id => document.getElementById("pf-" + id);
const esc = s => String(s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const color = cat => `var(--c-${cat})`;

const state = { range: "1h", from: null, to: null, view: "all", lens: "all", focus: "", zoom: [], search: "", scale: "all", block: null, path: null };
let meta = null, status = null, flame = null, timeline = null, selected = null, labels = {}, blockStages = null;
// The colour of each stage is the colour of the subsystem that does the work.
const STAGE_CATEGORY = {
  header_fetch: "arrival", header_check: "arrival", body_request: "arrival", body_download: "arrival", submit: "arrival",
  verify: "verification", writer_wait: "commit", writer_commit: "commit", status_fanout: "propagation",
};
const DOWNLOAD_STAGES = ["header_fetch", "header_check", "body_request", "body_download"];
const PATH_LABEL = { legacy: "legacy network", zakura: "Zakura block sync", unknown: "not recorded" };
const PATH_PHRASE = { legacy: "the legacy Zcash network", zakura: "Zakura's own block sync", unknown: "a path the trace did not record" };
const BLOCK_ROWS = 12;

function readHash() {
  if (!location.hash.startsWith("#profile")) return;
  const params = new URLSearchParams(location.hash.slice("#profile".length));
  for (const key of ["range", "view", "lens", "focus"]) if (params.get(key)) state[key] = params.get(key);
  if (params.get("from") && params.get("to")) { state.from = +params.get("from"); state.to = +params.get("to"); }
}
let changed = false;
function writeHash() {
  const params = new URLSearchParams();
  if (state.from) { params.set("from", state.from); params.set("to", state.to); } else params.set("range", state.range);
  params.set("view", state.view); params.set("lens", state.lens);
  if (state.focus) params.set("focus", state.focus);
  // Leave the URL alone until someone changes the view, so loading the section
  // while scrolling past it does not rewrite the address.
  if (location.hash.startsWith("#profile") || changed) history.replaceState(null, "", "#profile&" + params);
}
function rangeParams() {
  return state.from ? { from: state.from, to: state.to } : { range: state.range };
}
async function api(path, params) {
  const query = params ? "?" + new URLSearchParams(params) : "";
  const response = await fetch(`${BASE}/api/${path}${query}`, { cache: "no-store" });
  if (response.status === 503) throw new Error("the profiler is busy; try again in a minute");
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

// ---- formatting ----
function cpu(us) {
  if (us >= 60e6) return (us / 60e6).toFixed(1) + " min";
  if (us >= 1e6) return (us / 1e6).toFixed(us >= 1e7 ? 1 : 2) + " s";
  if (us >= 1e3) return (us / 1e3).toFixed(us >= 1e5 ? 0 : 1) + " ms";
  return Math.round(us) + " µs";
}
function ms(value) {
  if (value == null) return "–";
  if (value >= 1000) return (value / 1000).toFixed(2) + " s";
  if (value >= 100) return Math.round(value) + " ms";
  if (value >= 1) return value.toFixed(value >= 10 ? 1 : 2) + " ms";
  return Math.round(value * 1000) + " µs";
}
const pct = (part, whole) => whole > 0 ? (100 * part / whole).toFixed(part / whole < 0.01 ? 2 : 1) + "%" : "–";
const cores = value => value >= 1 ? value.toFixed(2) : value.toFixed(3);
const clockSeconds = t => new Date(t * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
function clock(t, withDate) {
  const d = new Date(t * 1000);
  const hm = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  return withDate ? d.toLocaleDateString([], { month: "short", day: "numeric" }) + " " + hm : hm;
}
function ago(seconds) {
  if (seconds < 90) return Math.round(seconds) + " s ago";
  if (seconds < 5400) return Math.round(seconds / 60) + " min ago";
  return (seconds / 3600).toFixed(1) + " h ago";
}
// A short label for a frame: drop generic arguments, keep the last path segments.
function shortName(full) {
  if (full.startsWith("thread: ") || full.startsWith("[") || full.startsWith("(")) return full;
  let out = "", depth = 0;
  for (const ch of full) {
    if (ch === "<") { depth++; if (depth > 1) continue; }
    if (ch === ">") { depth--; if (depth >= 1) continue; }
    if (depth <= 1) out += ch;
  }
  out = out.replace(/<([^<>]*?) as [^<>]*>/g, "$1").replace(/[<>]/g, "").replace(/::\{closure#\d+\}/g, "{closure}");
  const parts = out.split("::");
  return parts.length > 3 ? parts.slice(-3).join("::") : out;
}

// ---- tooltip ----
const tip = el("tip");
function showTip(event, html) {
  tip.innerHTML = html; tip.hidden = false;
  const box = tip.getBoundingClientRect();
  tip.style.left = Math.max(8, Math.min(event.clientX + 14, innerWidth - box.width - 8)) + "px";
  tip.style.top = Math.max(8, Math.min(event.clientY + 16, innerHeight - box.height - 8)) + "px";
}
const hideTip = () => { tip.hidden = true; };

// ---- header ----
function renderStatus() {
  const tone = { ok: "tone-ok", stale: "tone-warn", starting: "tone-warn", "waiting for node": "tone-warn", error: "tone-bad" }[status.state] || "tone-warn";
  const badge = el("state");
  badge.className = "badge " + tone;
  badge.textContent = status.state === "ok" ? "profiling" : status.state;
  badge.title = status.error || "";
  el("freshness").textContent = status.last_window_end
    ? `Last sample window ended ${ago(Date.now() / 1000 - status.last_window_end)}. Refreshes every minute.`
    : "Collecting the first sample window…";
}
function renderTiles() {
  const tiles = [];
  if (flame && flame.covered_seconds) {
    tiles.push([cores(flame.whole_us / 1e6 / flame.covered_seconds), state.lens === "block" ? "cores busy, averaged over the whole range" : "cores the node kept busy"]);
    if (state.view !== "all" || state.focus) tiles.push([pct(flame.total_us, flame.whole_us), "of that CPU is in this view"]);
    tiles.push([flame.samples.toLocaleString(), "samples in range"]);
    tiles.push([flame.blocks.toLocaleString(), "blocks announced in range"]);
  }
  tiles.push([status.rate_hz + " Hz", "sampling rate right now"]);
  tiles.push([status.overhead_pct_last_hour == null ? "–" : status.overhead_pct_last_hour + "%", "of one core used by the profiler, last hour"]);
  if (status.lost_windows_last_hour || status.dropped_windows) tiles.push([status.lost_windows_last_hour + " / " + status.dropped_windows, "windows with lost samples (1 h) / dropped"]);
  el("tiles").innerHTML = tiles.map(([value, label]) => `<div class="pf-tile"><b>${esc(value)}</b><span>${esc(label)}</span></div>`).join("");
}

// ---- controls ----
function renderControls() {
  el("ranges").innerHTML = meta.ranges.map(r => `<button data-pf-range="${r}" aria-pressed="${!state.from && state.range === r}">${r}</button>`).join("");
  el("views").innerHTML = "<span>View</span>" + meta.views.map(v =>
    `<button data-pf-view="${v}" aria-pressed="${!state.focus && state.view === v}">${v === "all" ? "" : `<i class="pf-swatch" style="background:${color(v)}"></i>`}${esc(VIEW_LABEL[v] || labels[v])}</button>`).join("")
    + (state.focus ? `<button data-pf-view="" aria-pressed="true" title="${esc(state.focus)}">Function: ${esc(shortName(state.focus)).slice(0, 60)} ✕</button>` : "");
  document.querySelectorAll("[data-pf-lens]").forEach(b => b.setAttribute("aria-pressed", b.dataset.pfLens === state.lens));
  el("live").hidden = !state.from;
  el("legend").innerHTML = meta.categories.map(c => `<span><i class="pf-swatch" style="background:${color(c.id)}"></i>${esc(c.label)}</span>`).join("");
}
document.getElementById("profile").addEventListener("click", event => {
  const button = event.target.closest("button");
  if (!button) return;
  changed = true;
  if (button.dataset.pfRange) { state.range = button.dataset.pfRange; state.from = state.to = null; reload(); }
  else if (button.dataset.pfView !== undefined) { state.view = button.dataset.pfView || state.view; state.focus = ""; state.zoom = []; reload(); }
  else if (button.dataset.pfLens) { state.lens = button.dataset.pfLens; reload(); }
  else if (button.id === "live") { state.from = state.to = null; reload(); }
  else if (button.id === "focus-fn" && selected) { state.focus = selected.name.slice(0, 300); state.zoom = []; reload(); }
  else if (button.id === "zoom-out") { state.zoom.pop(); selected = null; drawFlame(); }
  else if (button.dataset.pfScale) { state.scale = button.dataset.pfScale; drawStages(); }
  else if (button.id === "stage-median") { state.block = null; drawStages(); }
  else if (button.dataset.pfPath) { state.path = button.dataset.pfPath; state.block = null; drawStages(); }
  else if (button.dataset.pfBlock) { state.block = state.block === button.dataset.pfBlock ? null : button.dataset.pfBlock; drawStages(); }
  else if (button.dataset.pfCpu) {
    // The minute of samples that contains this block, narrowed to the moments around blocks.
    const when = +button.dataset.pfCpu;
    state.from = Math.floor(when - 65); state.to = Math.ceil(when + 1); state.lens = "block"; state.zoom = [];
    reload().then(() => el("fl-h").scrollIntoView({ behavior: "smooth", block: "start" }));
  }
  else if (button.dataset.pfFn !== undefined) { state.focus = flame.top[+button.dataset.pfFn][0].slice(0, 300); state.zoom = []; reload(); }
});
el("search").addEventListener("input", event => { state.search = event.target.value.trim().toLowerCase(); drawFlame(); });

// ---- timeline ----
function drawTimeline() {
  const svg = el("timeline");
  const W = Math.max(560, svg.parentElement.clientWidth), H = 150, left = 44, bottom = 22, top = 8;
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`); svg.setAttribute("width", W); svg.setAttribute("height", H);
  const t = timeline;
  if (!t || !t.buckets.length) { svg.innerHTML = `<text x="${W / 2}" y="${H / 2}" text-anchor="middle">No samples in this range yet</text>`; return; }
  const order = meta.categories.map(c => c.id);
  const totals = t.buckets.map(b => order.reduce((sum, c) => sum + (b.us[c] || 0), 0) / 1e6 / b.seconds);
  const peak = Math.max(...totals, 0.01);
  const x = time => left + (time - t.from) / (t.to - t.from) * (W - left - 6);
  const y = value => top + (1 - value / peak) * (H - top - bottom);
  const barW = Math.max(1, (W - left - 6) * t.step / (t.to - t.from) - 1);
  let out = "";
  for (const fraction of [0, 0.5, 1]) {
    const yy = y(peak * fraction);
    out += `<line x1="${left}" x2="${W - 6}" y1="${yy}" y2="${yy}" stroke="var(--line)"/><text x="${left - 6}" y="${yy + 4}" text-anchor="end">${cores(peak * fraction)}</text>`;
  }
  t.buckets.forEach((b, i) => {
    let base = 0;
    for (const c of order) {
      const value = (b.us[c] || 0) / 1e6 / b.seconds;
      if (!value) continue;
      const y1 = y(base + value), y0 = y(base);
      out += `<rect x="${x(b.t).toFixed(1)}" y="${y1.toFixed(1)}" width="${barW.toFixed(1)}" height="${Math.max(0.5, y0 - y1).toFixed(1)}" fill="${color(c)}"/>`;
      base += value;
    }
  });
  if (t.blocks && t.blocks.length <= 400) for (const when of t.blocks) out += `<line x1="${x(when).toFixed(1)}" x2="${x(when).toFixed(1)}" y1="${H - bottom + 1}" y2="${H - bottom + 6}" stroke="var(--ink-2)"/>`;
  const long = t.to - t.from > 86400;
  [[t.from, "start"], [(t.from + t.to) / 2, "middle"], [t.to, "end"]].forEach(([when, anchor]) => {
    out += `<text x="${x(when)}" y="${H - 4}" text-anchor="${anchor}">${clock(when, long)}</text>`;
  });
  out += `<rect id="pf-brush" y="${top}" height="${H - top - bottom}" width="0" fill="var(--pink-soft)" stroke="var(--pink)" visibility="hidden"/>`;
  svg.innerHTML = out;
  el("tl-note").textContent = "Cores the node kept busy, by what the innermost node code in each sample was doing. "
    + (t.blocks && t.blocks.length <= 400 ? "Ticks under the chart are block announcements. " : `${t.block_count.toLocaleString()} blocks were announced in this range. `)
    + "Drag across the chart to look at just that stretch.";
  svg._geometry = { left, W, x, bucketAt: px => { const time = t.from + (px - left) / (W - left - 6) * (t.to - t.from); return t.buckets.find(b => time >= b.t && time < b.t + t.step); }, timeAt: px => t.from + (Math.min(Math.max(px, left), W - 6) - left) / (W - left - 6) * (t.to - t.from) };
}
(function timelineInteraction() {
  const svg = el("timeline");
  let dragStart = null;
  const px = event => (event.clientX - svg.getBoundingClientRect().left) * (svg.viewBox.baseVal.width / svg.getBoundingClientRect().width);
  svg.addEventListener("pointerdown", event => { if (svg._geometry) { dragStart = px(event); svg.setPointerCapture(event.pointerId); } });
  svg.addEventListener("pointermove", event => {
    const g = svg._geometry; if (!g) return;
    const at = px(event);
    if (dragStart != null) {
      const brush = svg.querySelector("#pf-brush");
      const a = Math.max(g.left, Math.min(dragStart, at)), b = Math.min(g.W - 6, Math.max(dragStart, at));
      brush.setAttribute("x", a); brush.setAttribute("width", Math.max(0, b - a)); brush.setAttribute("visibility", "visible");
      hideTip(); return;
    }
    const bucket = g.bucketAt(at);
    if (!bucket) return hideTip();
    const rows = meta.categories.filter(c => bucket.us[c.id]).map(c => [c, bucket.us[c.id] / 1e6 / bucket.seconds]).sort((a, b) => b[1] - a[1]);
    const total = rows.reduce((sum, row) => sum + row[1], 0);
    showTip(event, `<div class="pf-row"><span>${clock(bucket.t, true)}</span><b>${cores(total)} cores</b></div>`
      + rows.map(([c, value]) => `<div class="pf-row"><i class="pf-swatch" style="background:${color(c.id)}"></i><span>${esc(c.label)}</span><b>${cores(value)}</b></div>`).join(""));
  });
  svg.addEventListener("pointerup", event => {
    const g = svg._geometry;
    if (g && dragStart != null) {
      const a = g.timeAt(Math.min(dragStart, px(event))), b = g.timeAt(Math.max(dragStart, px(event)));
      if (b - a >= 2 * timeline.step) { changed = true; state.from = Math.floor(a); state.to = Math.ceil(b); reload(); }
      else svg.querySelector("#pf-brush").setAttribute("visibility", "hidden");
    }
    dragStart = null;
  });
  svg.addEventListener("pointerleave", hideTip);
})();

// ---- flame graph ----
let tree = null, drawn = [];
function buildTree() {
  const root = { name: "root", v: flame.total_us, s: 0, cat: "other", children: [], parent: null, depth: 0, x0: 0 };
  const nodes = new Array(flame.n.length);
  for (let i = 0; i < nodes.length; i++) {
    const parent = flame.p[i] < 0 ? root : nodes[flame.p[i]];
    nodes[i] = { name: flame.names[flame.n[i]], cat: flame.categories[flame.n[i]], v: flame.v[i], s: flame.s[i], children: [], parent, depth: parent.depth + 1, x0: 0 };
    parent.children.push(nodes[i]);
  }
  const pending = [root];
  while (pending.length) {
    const node = pending.pop();
    node.children.sort((a, b) => b.v - a.v);
    let x = node.x0;
    for (const child of node.children) { child.x0 = x; x += child.v; pending.push(child); }
  }
  root.name = state.focus ? "Everything under the chosen function" : state.view === "all" ? "All CPU time" : labels[state.view];
  tree = root;
}
function zoomNode() {
  let node = tree;
  for (const name of state.zoom) {
    const next = node.children.find(child => child.name === name);
    if (!next) break;
    node = next;
  }
  return node;
}
function pathOf(node) { const path = []; for (let n = node; n && n.parent; n = n.parent) path.unshift(n.name); return path; }
function drawFlame() {
  const svg = el("flame"), empty = el("flame-empty");
  if (!flame || !flame.total_us) {
    svg.innerHTML = ""; svg.setAttribute("height", 0); empty.hidden = false; el("detail").hidden = true; el("crumb").textContent = "";
    empty.textContent = !flame ? "Loading…" : state.lens === "block" ? "No samples fell inside a block's window in this range and view. Try a longer range." : "No samples in this range and view yet.";
    return;
  }
  empty.hidden = true;
  const focus = zoomNode();
  const W = Math.max(560, svg.parentElement.clientWidth);
  const scale = W / focus.v;
  drawn = [];
  let out = "", maxDepth = focus.depth, matched = 0;
  const query = state.search;
  // Ancestors of the zoomed frame span the full width; its subtree is laid out below.
  for (let n = focus.parent; n; n = n.parent) drawn.push({ node: n, x: 0, w: W, above: true });
  const pending = [focus];
  while (pending.length) {
    const node = pending.pop();
    const w = node.v * scale;
    if (w < 0.6) continue;
    drawn.push({ node, x: (node.x0 - focus.x0) * scale, w });
    if (node.depth > maxDepth) maxDepth = node.depth;
    for (const child of node.children) pending.push(child);
  }
  if (query) {
    const walk = [focus];
    while (walk.length) {
      const node = walk.pop();
      if (node !== tree && node.name.toLowerCase().includes(query)) { matched += node.v; continue; }
      for (const child of node.children) walk.push(child);
    }
  }
  drawn.forEach((d, i) => {
    const node = d.node, y = node.depth * ROW;
    const hit = query && node.name.toLowerCase().includes(query);
    const cls = "pf-f" + (d.above || (query && !hit) ? " pf-dim" : "") + (hit ? " pf-hit" : "");
    out += `<rect class="${cls}" data-i="${i}" x="${d.x.toFixed(1)}" y="${y}" width="${Math.max(1, d.w).toFixed(1)}" height="${ROW - 1}" rx="2" fill="${node === tree ? "var(--line-hi)" : color(node.cat)}"/>`;
    if (d.w > 34) {
      const label = shortName(node.name), room = Math.floor((d.w - 8) / 6.7);
      const text = label.length > room ? label.slice(0, Math.max(1, room - 1)) + "…" : label;
      out += `<text x="${(d.x + 4).toFixed(1)}" y="${y + 13}"${node === tree ? ' style="fill:var(--ink)"' : ""}${d.above ? ' opacity="0.5"' : ""}>${esc(text)}</text>`;
    }
  });
  const H = (maxDepth + 1) * ROW + 2;
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`); svg.setAttribute("width", W); svg.setAttribute("height", H);
  svg.innerHTML = out;
  el("crumb").textContent = focus === tree ? "Click a bar to zoom into it." : "Zoomed into: " + pathOf(focus).map(shortName).join(" › ");
  el("search-note").textContent = query ? `${pct(matched, focus.v)} of what is shown` : "";
  const lead = Math.abs(status.block_window_ms[0]), tail = status.block_window_ms[1];
  el("fl-note").textContent = (state.lens === "block"
    ? `Only samples taken from ${lead} ms before to ${tail} ms after a block announcement: ${cpu(flame.whole_us)} of CPU across ${flame.blocks.toLocaleString()} blocks. `
    : "")
    + (state.focus ? "Everything that ran under this function, merged across every place it was called from. "
      : state.view === "all" ? "Top row is the kind of thread; below it, each bar is a function and the bars under it are what it called. Width is CPU time. "
      : `Each tower starts at the outermost ${labels[state.view].toLowerCase()} function on the stack, merged across every task and thread it ran on. Width is CPU time. `);
  renderDetail(focus === tree ? null : focus);
}
function renderDetail(node) {
  selected = node;
  el("detail").hidden = !node;
  if (!node) return;
  el("detail-name").textContent = node.name;
  const seconds = flame.covered_seconds || 1;
  el("detail-facts").innerHTML = [
    ["CPU time", cpu(node.v)], ["of this view", pct(node.v, flame.total_us)], ["of all node CPU", pct(node.v, flame.whole_us)],
    ["in the function itself", cpu(node.s)], ["cores on average", cores(node.v / 1e6 / seconds)],
  ].map(([label, value]) => `<span>${label} <b>${value}</b></span>`).join("");
  el("focus-fn").hidden = node.name.startsWith("thread: ") || node.name.startsWith("(");
}
(function flameInteraction() {
  const svg = el("flame");
  const nodeAt = event => { const rect = event.target.closest("rect.pf-f"); return rect ? drawn[+rect.dataset.i].node : null; };
  svg.addEventListener("click", event => {
    const node = nodeAt(event); if (!node) return;
    state.zoom = pathOf(node === zoomNode() && node.parent ? node.parent : node);
    drawFlame();
  });
  svg.addEventListener("pointermove", event => {
    const node = nodeAt(event); if (!node) return hideTip();
    showTip(event, `<div class="pf-name">${esc(node.name)}</div>`
      + `<div class="pf-row"><i class="pf-swatch" style="background:${color(node.cat)}"></i><span>${esc(labels[node.cat] || "")}</span><b>${cpu(node.v)}</b></div>`
      + `<div class="pf-row"><span>of this view</span><b>${pct(node.v, flame.total_us)}</b></div>`
      + `<div class="pf-row"><span>of all node CPU</span><b>${pct(node.v, flame.whole_us)}</b></div>`
      + `<div class="pf-row"><span>in the function itself</span><b>${cpu(node.s)}</b></div>`);
  });
  svg.addEventListener("pointerleave", hideTip);
})();

// ---- a block's path: wall-clock time per stage, from the node's trace points ----
function drawStages() {
  const panel = el("stages-panel"), data = blockStages;
  panel.hidden = !data;
  if (!data) return;
  document.querySelectorAll("[data-pf-scale]").forEach(b => b.setAttribute("aria-pressed", b.dataset.pfScale === state.scale));
  const svg = el("stages"), table = el("block-table");
  const total = data.blocks.length;
  // Most common arrival path first; the typical block is shown for one path at a time.
  const paths = Object.keys(data.paths).sort((a, b) => data.paths[b].blocks - data.paths[a].blocks);
  if (!paths.includes(state.path)) state.path = paths[0];
  el("paths").innerHTML = paths.length < 2 ? "" : "<span>Arrived over</span>" + paths.map(path =>
    `<button data-pf-path="${esc(path)}" aria-pressed="${path === state.path}">${esc(PATH_LABEL[path] || path)} (${data.paths[path].blocks})</button>`).join("");
  if (!total) {
    svg.innerHTML = ""; svg.setAttribute("height", 0); table.innerHTML = ""; el("stage-median").hidden = true;
    el("st-note").textContent = "No block has been recorded yet. This needs the node's trace files switched on (trace_dir under [network.zakura] in its config) and fills in with the next block.";
    return;
  }
  const block = data.blocks.find(b => b.hash === state.block) || null;
  el("stage-median").hidden = !block;
  const typical = data.paths[block ? block.path : state.path];
  const value = stage => block ? block.ms[stage] : typical.median_ms[stage];
  const stages = data.stages.filter(s => value(s.id) != null && (state.scale === "all" || !DOWNLOAD_STAGES.includes(s.id)));
  const span = stages.reduce((sum, s) => sum + value(s.id), 0) || 1;
  const W = Math.max(560, svg.parentElement.clientWidth), ROWH = 26, left = 168, right = 236, top = 4, bottom = 22;
  const H = top + stages.length * ROWH + bottom, plot = W - left - right;
  let out = "", at = 0;
  for (const fraction of [0, 0.5, 1]) {
    const x = left + plot * fraction;
    out += `<line x1="${x}" x2="${x}" y1="${top}" y2="${H - bottom + 4}" stroke="var(--line)"/><text x="${x}" y="${H - 5}" text-anchor="${fraction ? fraction === 1 ? "end" : "middle" : "start"}">${fraction ? ms(span * fraction) : "0"}</text>`;
  }
  stages.forEach((stage, i) => {
    const v = value(stage.id), y = top + i * ROWH, w = Math.max(2, plot * v / span);
    const x = Math.min(left + plot * at / span, left + plot - w);
    const other = block ? `typical ${ms(typical.median_ms[stage.id])}` : `slowest tenth ${ms(typical.p90_ms[stage.id])}`;
    out += `<text class="pf-label" x="0" y="${y + 17}">${esc(stage.label)}</text>`
      + `<rect x="${x.toFixed(1)}" y="${y + 5}" width="${w.toFixed(1)}" height="${ROWH - 10}" rx="3" fill="${color(STAGE_CATEGORY[stage.id])}"/>`
      + `<text class="pf-value" x="${left + plot + 10}" y="${y + 17}">${ms(v)}</text><text x="${left + plot + 82}" y="${y + 17}">${other}</text>`
      + `<rect class="pf-hit" data-stage="${esc(stage.id)}" x="0" y="${y}" width="${W}" height="${ROWH}"/>`;
    at += v;
  });
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`); svg.setAttribute("width", W); svg.setAttribute("height", H);
  svg.innerHTML = out;
  const mix = paths.map(path => `${data.paths[path].blocks} over ${PATH_PHRASE[path] || path}`).join(", ");
  el("st-note").textContent = (block
    ? `Block ${block.height.toLocaleString()}, which arrived over ${PATH_PHRASE[block.path] || block.path}. `
    : `Of the last ${total} blocks, ${mix}. Bars are the typical (median) time in each stage for the ${typical.blocks} that arrived over ${PATH_PHRASE[state.path] || state.path}. `)
    + "This is time on the clock, waiting included, from the node's own trace points; each bar starts where the one above it ends. "
    + ((block ? block.path : state.path) === "legacy" ? "For a block from the legacy network the record starts when its download finishes, so it has no header stages. " : "")
    + (block ? "" : "Pick a block below to see its own path.");
  const rows = data.blocks.slice(-BLOCK_ROWS).reverse();
  const widest = Math.max(...rows.map(insideMs), 0.001);
  table.innerHTML = "<tr><th>Block</th><th>Announced</th><th>Came over</th><th>Download</th><th>Verify</th><th>Writer</th><th>Peers told</th><th>After the download</th><th></th></tr>"
    + rows.map(b => {
      const writer = b.ms.writer_wait == null ? null : b.ms.writer_wait + b.ms.writer_commit;
      const told = b.status_peers ? `${b.status_peers} · typical ${ms(b.ms.status_fanout)} · last ${ms(b.status_slowest_ms)}` : "–";
      const mini = data.stages.filter(s => !DOWNLOAD_STAGES.includes(s.id) && b.ms[s.id] != null)
        .map(s => `<i style="width:${(100 * b.ms[s.id] / widest).toFixed(2)}%;background:${color(STAGE_CATEGORY[s.id])}"></i>`).join("");
      return `<tr aria-selected="${b.hash === state.block}"><td><button data-pf-block="${esc(b.hash)}" title="${esc(b.hash)}">${b.height.toLocaleString()}${b.reorg ? " (reorg)" : ""}</button></td>`
        + `<td class="pf-num">${b.t ? clockSeconds(b.t) : "–"}</td><td>${esc(PATH_LABEL[b.path] || b.path)}</td>`
        + `<td class="pf-num">${ms(b.ms.body_download)}</td><td class="pf-num">${ms(b.ms.verify)}</td><td class="pf-num">${ms(writer)}</td><td class="pf-num">${told}</td>`
        + `<td style="width:18%"><div class="pf-mini" title="${ms(insideMs(b))} from download to peers told">${mini}</div></td>`
        + `<td>${b.t ? `<button data-pf-cpu="${b.t}">CPU around it</button>` : ""}</td></tr>`;
    }).join("");
}
// Time from the block being in hand to its peers being told.
function insideMs(block) {
  return Object.entries(block.ms).reduce((sum, [stage, value]) => sum + (DOWNLOAD_STAGES.includes(stage) || value == null ? 0 : value), 0);
}
(function stageInteraction() {
  const svg = el("stages");
  svg.addEventListener("pointermove", event => {
    const hit = event.target.closest("rect.pf-hit");
    const stage = hit && blockStages && blockStages.stages.find(s => s.id === hit.dataset.stage);
    if (!stage) return hideTip();
    const shown = blockStages.blocks.find(b => b.hash === state.block);
    const typical = blockStages.paths[shown ? shown.path : state.path];
    showTip(event, `<div class="pf-row"><i class="pf-swatch" style="background:${color(STAGE_CATEGORY[stage.id])}"></i><span>${esc(stage.label)}</span></div>`
      + `<div class="pf-row"><span>${esc(stage.where)}</span></div>`
      + `<div class="pf-row"><span>typical block</span><b>${ms(typical.median_ms[stage.id])}</b></div>`
      + `<div class="pf-row"><span>slowest tenth of blocks</span><b>${ms(typical.p90_ms[stage.id])}</b></div>`);
  });
  svg.addEventListener("pointerleave", hideTip);
})();

function renderTop() {
  const rows = flame ? flame.top.slice(0, 20) : [];
  const peak = rows.length ? rows[0][1] : 1;
  el("top").innerHTML = "<tr><th>Function</th><th>Own CPU time</th><th>Of this view</th><th></th></tr>" + rows.map(([name, us], i) => {
    const cat = flame.categories[flame.names.indexOf(name)] || "other";
    return `<tr><td class="pf-fn"><button data-pf-fn="${i}" title="${esc(name)}">${esc(shortName(name))}</button></td><td class="pf-num">${cpu(us)}</td><td class="pf-num">${pct(us, flame.total_us)}</td>`
      + `<td style="width:28%"><div class="pf-bar" style="width:${(100 * us / peak).toFixed(1)}%;background:${color(cat)}"></div></td></tr>`;
  }).join("");
}
function renderNotes() {
  const lead = Math.abs(status.block_window_ms[0]), tail = status.block_window_ms[1];
  el("notes").innerHTML = [
    `Samples are taken from outside the node with <span class="pf-mono">perf</span>, ${status.window_seconds} seconds at a time, only while a node thread is on a CPU. Time spent waiting (on the network, a lock, a disk read) is not here, so a slow block download does not show up.`,
    "The sampling rate drops automatically when the node is busy, so the profiler's cost stays bounded. All figures are CPU time, already corrected for the rate.",
    `"While a block lands" keeps the samples from ${lead} ms before to ${tail} ms after each block announcement in the node log: the end of the download, verification, the commit and the hand-off to gossip.`,
    "Functions the compiler inlined are counted in the function they were inlined into. A tower that starts at “[stack truncated]” is a stack the unwinder could not follow to its thread's start.",
    `Per-minute detail is kept for 48 hours and hourly detail for ${status.retention_days} days.`,
    ...(status.stage_timeline ? ["“A block's path through the node” is a different measurement: time on the clock between the node's own trace points for each block, so it includes waiting. It has no trace point between the new tip and the hand-off to legacy gossip, or per legacy peer; “Tell peers” covers Zakura peers only, counting status messages queued within a second of the new tip."] : []),
  ].map(text => `<li>${text}</li>`).join("");
}

async function reload() {
  writeHash(); renderControls();
  try {
    const params = rangeParams();
    const flameParams = { ...params, view: state.view, lens: state.lens };
    if (state.focus) flameParams.focus = state.focus;
    [status, timeline, flame, blockStages] = await Promise.all([
      api("status"), api("timeline", params), api("flame", flameParams),
      // The stage timeline is extra: the profile still shows when it cannot be read.
      status && status.stage_timeline ? api("blocks", { limit: 200 }).catch(() => blockStages) : null,
    ]);
  } catch (error) {
    el("freshness").textContent = "Could not load the profile: " + error.message;
    return;
  }
  renderStatus(); renderTiles(); drawTimeline(); buildTree(); drawFlame(); drawStages(); renderTop(); renderNotes();
}
async function start() {
  readHash();
  try { status = await api("status"); } catch (error) { el("freshness").textContent = "Could not reach the profiler: " + error.message; setTimeout(start, 10000); return; }
  meta = { ranges: status.ranges, views: status.views, categories: status.categories };
  labels = Object.fromEntries(meta.categories.map(c => [c.id, c.label]));
  if (!meta.views.includes(state.view)) state.view = "all";
  if (!meta.ranges.includes(state.range)) state.range = "1h";
  await reload();
  setInterval(() => { if (!state.from && !document.hidden) reload(); }, REFRESH_MS);
  let resize; addEventListener("resize", () => { clearTimeout(resize); resize = setTimeout(() => { drawTimeline(); drawFlame(); drawStages(); }, 150); });
}
start();
}

/* ---------- interaction ---------- */
document.addEventListener('click', (event) => {
  const copy = event.target.closest('[data-copy]');
  if (copy) {
    event.stopPropagation();
    copyValue(copy);
    return;
  }
  // Let the node link navigate instead of toggling the row's drawer.
  if (event.target.closest('a.node-link')) return;
  const header = event.target.closest('thead th[data-sort]');
  if (header) {
    const key = header.dataset.sort;
    if (state.sortKey === key) {
      state.sortDir = -state.sortDir;
    } else {
      state.sortKey = key;
      state.sortDir = 1;
    }
    if (state.data) renderTable(state.data);
    return;
  }
  const row = event.target.closest('tr.node-row');
  if (row) {
    const name = row.dataset.node;
    if (state.expandAll) {
      // Leaving expand-all: keep every row open except the one just clicked.
      state.expandAll = false;
      el('expand-toggle').classList.remove('is-on');
      el('expand-toggle').textContent = 'Expand all';
      state.expanded = new Set((state.data.rows || []).map((each) => each.name));
      state.expanded.delete(name);
    } else if (state.expanded.has(name)) {
      state.expanded.delete(name);
    } else {
      state.expanded.add(name);
    }
    if (state.data) renderTable(state.data);
  }
});
el('filter-input').addEventListener('input', (event) => {
  state.query = event.target.value.trim().toLowerCase();
  if (state.data) renderTable(state.data);
});
el('issues-toggle').addEventListener('click', (event) => {
  state.issuesOnly = !state.issuesOnly;
  event.currentTarget.classList.toggle('is-on', state.issuesOnly);
  if (state.data) renderTable(state.data);
});
el('expand-toggle').addEventListener('click', (event) => {
  state.expandAll = !state.expandAll;
  if (!state.expandAll) state.expanded.clear();
  event.currentTarget.classList.toggle('is-on', state.expandAll);
  event.currentTarget.textContent = state.expandAll ? 'Collapse all' : 'Expand all';
  if (state.data) renderTable(state.data);
});
if (NODE_NAME) setupNodeNav();
tick();
setInterval(tick, REFRESH_MS);
setInterval(renderFreshness, 1000);
</script>
</body>
</html>"""


# -- the shared dashboard password --------------------------------------------
# One password gates the fleet pages and their data. Only its salted PBKDF2
# hash reaches this process, through the environment. A login sets a cookie
# holding an expiry and an HMAC of it under a key derived from that hash, so a
# login lasts across dashboard restarts and a new password signs everyone out.
SESSION_COOKIE = "zakura_status_session"
SESSION_SECONDS = 7 * 86400
PASSWORD_HASH_ALGORITHM = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 200_000
MAX_LOGIN_BODY = 4096


def hash_password(password: str, salt: bytes | None = None,
                  iterations: int = PASSWORD_HASH_ITERATIONS) -> str:
    """`pbkdf2_sha256$<iterations>$<salt hex>$<hash hex>` for a password."""
    salt = secrets.token_bytes(16) if salt is None else salt
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    return f"{PASSWORD_HASH_ALGORITHM}${iterations}${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    """Whether a password matches a hash from `hash_password`; false for a malformed hash."""
    try:
        algorithm, iterations, salt_hex, digest_hex = encoded.split("$")
        if algorithm != PASSWORD_HASH_ALGORITHM:
            return False
        expected = bytes.fromhex(digest_hex)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt_hex), int(iterations))
    except ValueError:
        return False
    return hmac.compare_digest(actual, expected)


def parse_cookies(header: str | None) -> dict[str, str]:
    """The cookies in a `Cookie` request header."""
    cookies = {}
    for part in (header or "").split(";"):
        name, _, value = part.strip().partition("=")
        if name:
            cookies[name] = value
    return cookies


@dataclass(frozen=True)
class DashboardLogin:
    """The password hash and the in-memory key that signs login cookies."""

    password_hash: str
    session_secret: str

    def _signature(self, expiry: int) -> str:
        return hmac.new(self.session_secret.encode(), str(expiry).encode(), hashlib.sha256).hexdigest()

    def issue_token(self, now: float) -> str:
        """A cookie value `<expiry>.<signature>` that `token_valid` accepts for `SESSION_SECONDS`."""
        expiry = int(now) + SESSION_SECONDS
        return f"{expiry}.{self._signature(expiry)}"

    def token_valid(self, token: str | None, now: float) -> bool:
        """Whether a cookie value was signed under this secret and has not expired."""
        expiry_text, _, signature = (token or "").partition(".")
        if not (expiry_text.isascii() and expiry_text.isdigit()) or not re.fullmatch(r"[0-9a-f]{64}", signature):
            return False
        expiry = int(expiry_text)
        return hmac.compare_digest(signature, self._signature(expiry)) and expiry > now


def dashboard_login_from_env(environ) -> DashboardLogin | None:
    """The login gate configured by `ZAKURA_DASHBOARD_PASSWORD_HASH`, or None when no password is set.

    The cookie-signing key is derived from the hash, so cookies stay valid when
    the dashboard restarts (deploys restart it often) and stop being valid when
    the password changes.
    """
    password_hash = environ.get("ZAKURA_DASHBOARD_PASSWORD_HASH", "").strip()
    if not password_hash:
        return None
    if not password_hash.startswith(PASSWORD_HASH_ALGORITHM + "$"):
        raise ValueError("ZAKURA_DASHBOARD_PASSWORD_HASH is not a pbkdf2_sha256 hash")
    session_secret = hmac.new(password_hash.encode(), b"zakura dashboard session cookie",
                              hashlib.sha256).hexdigest()
    return DashboardLogin(password_hash, session_secret)


LOGIN_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zakura fleet status</title>
<style>
  :root { color-scheme: light dark; --bg: #f6f7f9; --card: #ffffff; --text: #1d2330; --muted: #5d6677; --line: #d8dde6; --accent: #2f6fde; --bad: #c0392b; }
  @media (prefers-color-scheme: dark) { :root { --bg: #11151c; --card: #1a2029; --text: #e6e9ef; --muted: #98a2b3; --line: #2c3442; --accent: #6b9bff; --bad: #ff7b6b; } }
  body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: var(--bg); color: var(--text); font: 15px/1.5 system-ui, -apple-system, sans-serif; }
  form { width: min(340px, calc(100vw - 32px)); background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 24px; display: grid; gap: 12px; }
  h1 { margin: 0; font-size: 18px; }
  p { margin: 0; color: var(--muted); }
  input, button { font: inherit; padding: 9px 11px; border-radius: 7px; border: 1px solid var(--line); background: var(--bg); color: var(--text); }
  button { background: var(--accent); border-color: var(--accent); color: #fff; cursor: pointer; }
  .error { color: var(--bad); }
</style>
</head>
<body>
<form method="post" action="/login">
  <h1>Zakura fleet status</h1>
  <p>Enter the dashboard password.</p>
  <p class="error">{{error}}</p>
  <input type="password" name="password" autocomplete="current-password" autofocus required aria-label="Password">
  <button type="submit">Sign in</button>
</form>
</body>
</html>"""


COLLECTOR: ClusterCollector | None = None
PROPAGATION: PropagationView | None = None
ACTIVATION_FEED = None
TRUSTED_PROXY_IPS = set()
RATE_LIMITER = RateLimiter()
DASHBOARD_LOGIN: DashboardLogin | None = None
# Password guesses per client per minute.
LOGIN_RATE_LIMITER = RateLimiter(limit=10, window=60)
# The page and its data both change every poll, and the HTML carries no
# fingerprint. Without this a browser heuristically caches the page and keeps
# serving a stale build after a dashboard deploy.
NO_STORE = {
    "Cache-Control": "no-store, must-revalidate",
    "X-Content-Type-Options": "nosniff",
}
# The fleet pages and their data need the dashboard password when one is set.
# The public APIs (`/ironwood-status.json`, `/v1/*`) and `/healthz` stay open.
PASSWORD_GATED_PAGES = {"/", "/data"}
PASSWORD_GATED_PREFIXES = ("/data/node/", "/node/", "/profile/", "/propagation")


class BoundedHTTPServer(ThreadingHTTPServer):
    """A threading HTTP server that handles at most `max_concurrent` requests at once.

    ThreadingHTTPServer starts one thread per connection without limit. Here the
    accept loop waits for a free slot instead, so excess clients queue in the listen
    backlog rather than exhausting threads.
    """

    daemon_threads = True

    def __init__(self, address, handler, max_concurrent: int = MAX_CONCURRENT_REQUESTS):
        super().__init__(address, handler)
        self.slots = threading.BoundedSemaphore(max_concurrent)

    def process_request(self, request, client_address):
        self.slots.acquire()
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


class Handler(BaseHTTPRequestHandler):
    # Socket timeout, so a client that stalls mid-request frees its slot.
    timeout = REQUEST_TIMEOUT_SECONDS

    def log_message(self, *args) -> None:
        pass

    def send_body(
        self,
        status: int,
        body: bytes,
        content_type: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        if body and content_type.startswith("application/json"):
            try:
                body = json.dumps(redact_private_addresses(json.loads(body), private_addresses()),
                                  separators=(",", ":")).encode()
            except (OSError, ValueError, TypeError):
                if urllib.parse.urlparse(self.path).path == "/data" and COLLECTOR is not None:
                    body = json.dumps(monitoring_snapshot(json.loads(body), COLLECTOR.nodes_by_name),
                                      separators=(",", ":")).encode()
                else:
                    status = 503
                    body = b'{"error":"privacy configuration unavailable"}'
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def public_headers(self) -> dict[str, str]:
        assert COLLECTOR is not None
        headers = {
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "Vary": "Origin",
        }
        origin = self.headers.get("Origin")
        if origin in PUBLIC_ORIGINS[COLLECTOR.network]:
            headers.update({
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Max-Age": "600",
            })
        return headers

    def send_json(
        self,
        status: int,
        payload: dict,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_body(
            status,
            body,
            "application/json; charset=utf-8",
            headers,
        )

    def rate_limit_client(self) -> str:
        peer = self.client_address[0]
        try:
            peer_address = ipaddress.ip_address(peer)
        except ValueError:
            return peer

        forwarded_for = self.headers.get("X-Forwarded-For")
        if (peer_address.is_loopback or peer_address in TRUSTED_PROXY_IPS) and forwarded_for:
            candidate = forwarded_for.rsplit(",", 1)[-1].strip()
            try:
                return str(ipaddress.ip_address(candidate))
            except ValueError:
                pass
        return str(peer_address)

    def serve_profile(self, parsed: urllib.parse.ParseResult) -> None:
        """Forward the node page's CPU profile requests to the node's own profiler.

        `/profile/<node>/api/<request>` is one of the requests in PROFILER_API, with
        only the parameters listed there. `/profile/<node>` itself, the address of the
        profile's former page, sends the browser to the node page's profile section.
        """
        assert COLLECTOR is not None
        name, _, rest = parsed.path[len("/profile/"):].partition("/")
        node = COLLECTOR.nodes_by_name.get(urllib.parse.unquote(name))
        request = rest[len("api/"):] if rest.startswith("api/") else None
        if node is None or not node.profiler_url or (rest and request not in PROFILER_API):
            return self.send_body(
                404, b"not found\n", "text/plain; charset=utf-8", {"X-Content-Type-Options": "nosniff"}
            )
        if not RATE_LIMITER.allow(self.rate_limit_client()):
            return self.send_json(
                429, {"error": "Request rate limit exceeded"},
                {**NO_STORE, "Retry-After": str(int(PUBLIC_RATE_WINDOW))},
            )
        if request is None:
            self.send_response(302)
            self.send_header("Location", "/node/" + urllib.parse.quote(node.name, safe="") + "#profile")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None
        params = [
            (key, value)
            for key, value in urllib.parse.parse_qsl(parsed.query)
            if key in PROFILER_API[request] and len(value) <= PROFILER_MAX_PARAM_LENGTH
        ]
        path = f"/api/{request}" + ("?" + urllib.parse.urlencode(params) if params else "")
        status, body, _ = fetch_profiler(node.profiler_url, path, PROFILER_REQUEST_TIMEOUT_SECONDS)
        return self.send_body(status, body, "application/json", NO_STORE)

    def is_local_client(self) -> bool:
        """A process on this host talking to the port directly, not through the reverse proxy.

        Caddy always adds `X-Forwarded-For`, so a loopback request without it
        comes from a local reader such as the fleet watchdog.
        """
        try:
            peer = ipaddress.ip_address(self.client_address[0])
        except ValueError:
            return False
        return peer.is_loopback and "X-Forwarded-For" not in self.headers

    def logged_in(self) -> bool:
        """No password is configured, the client is local, or it carries a valid login cookie."""
        if DASHBOARD_LOGIN is None or self.is_local_client():
            return True
        token = parse_cookies(self.headers.get("Cookie")).get(SESSION_COOKIE)
        return DASHBOARD_LOGIN.token_valid(token, time.time())

    def send_login_page(self, status: int, error: str = "") -> None:
        page = LOGIN_PAGE.replace("{{error}}", html.escape(error))
        self.send_body(status, page.encode(), "text/html; charset=utf-8", NO_STORE)

    def redirect_home(self, cookie: str) -> None:
        self.send_response(303)
        self.send_header("Location", "/")
        self.send_header("Set-Cookie", cookie)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def came_through_proxy(self) -> bool:
        """The request arrived through the TLS reverse proxy, which always adds `X-Forwarded-For`."""
        try:
            peer = ipaddress.ip_address(self.client_address[0])
        except ValueError:
            return False
        return (peer.is_loopback or peer in TRUSTED_PROXY_IPS) and "X-Forwarded-For" in self.headers

    def do_POST(self) -> None:
        if urllib.parse.urlparse(self.path).path != "/login" or DASHBOARD_LOGIN is None:
            return self.send_body(404, b"not found\n", "text/plain; charset=utf-8",
                                  {"X-Content-Type-Options": "nosniff"})
        # The password must only cross an encrypted connection, so refuse logins
        # that reached the plain-HTTP port directly instead of through the proxy.
        if not self.came_through_proxy():
            return self.send_body(403, b"log in through the HTTPS dashboard address\n",
                                  "text/plain; charset=utf-8", {"X-Content-Type-Options": "nosniff"})
        if not LOGIN_RATE_LIMITER.allow(self.rate_limit_client()):
            return self.send_login_page(429, "Too many attempts; wait a minute and try again")
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        if length < 0 or length > MAX_LOGIN_BODY:
            return self.send_login_page(413, "Request too large")
        form = urllib.parse.parse_qs(self.rfile.read(length).decode(errors="replace"))
        password = form.get("password", [""])[0]
        if not verify_password(password, DASHBOARD_LOGIN.password_hash):
            return self.send_login_page(401, "Wrong password")
        token = DASHBOARD_LOGIN.issue_token(time.time())
        self.redirect_home(f"{SESSION_COOKIE}={token}; Max-Age={SESSION_SECONDS}; Path=/; HttpOnly; Secure; SameSite=Lax")

    def serve_propagation(self, parsed: urllib.parse.ParseResult) -> None:
        """The cluster view: `/propagation` is the page, `/propagation/data` what it shows."""
        if PROPAGATION is None or parsed.path not in ("/propagation", "/propagation/data"):
            return self.send_body(
                404, b"not found\n", "text/plain; charset=utf-8", {"X-Content-Type-Options": "nosniff"}
            )
        if not RATE_LIMITER.allow(self.rate_limit_client()):
            return self.send_json(
                429, {"error": "Request rate limit exceeded"},
                {**NO_STORE, "Retry-After": str(int(PUBLIC_RATE_WINDOW))},
            )
        if parsed.path == "/propagation/data":
            try:
                joined = PROPAGATION.data()
            except (OSError, ValueError):
                return self.send_json(503, {"error": "privacy configuration unavailable"}, NO_STORE)
            return self.send_json(200, joined, NO_STORE)
        try:
            page = PROPAGATION_PAGE.read_bytes()
        except OSError:
            return self.send_body(
                503, b"The cluster view page is not installed.\n", "text/plain; charset=utf-8", NO_STORE
            )
        return self.send_body(200, page, "text/html; charset=utf-8", NO_STORE)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/logout":
            return self.redirect_home(f"{SESSION_COOKIE}=; Max-Age=0; Path=/; HttpOnly; Secure; SameSite=Lax")
        if parsed.path in PASSWORD_GATED_PAGES or parsed.path.startswith(PASSWORD_GATED_PREFIXES):
            if not self.logged_in():
                if not self.came_through_proxy():
                    return self.send_json(401, {"error": "use the HTTPS dashboard address"}, NO_STORE)
                is_page = parsed.path in ("/", "/propagation") or (
                    parsed.path.startswith(("/node/", "/profile/")) and "/api/" not in parsed.path
                )
                if is_page:
                    return self.send_login_page(200)
                return self.send_json(401, {"error": "login required"}, NO_STORE)
        return self.serve_get(parsed)

    def serve_get(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path.startswith("/profile/"):
            return self.serve_profile(parsed)
        if parsed.path.startswith("/propagation"):
            return self.serve_propagation(parsed)
        if parsed.path == "/data":
            assert COLLECTOR is not None
            body = json.dumps(COLLECTOR.snapshot()).encode()
            return self.send_body(200, body, "application/json", NO_STORE)
        if parsed.path.startswith("/data/node/"):
            assert COLLECTOR is not None
            name = urllib.parse.unquote(parsed.path[len("/data/node/"):])
            snapshot = COLLECTOR.node_snapshot(name)
            if snapshot is None:
                return self.send_json(
                    404, {"error": "unknown node", "node": name}, NO_STORE
                )
            return self.send_body(
                200, json.dumps(snapshot).encode(), "application/json", NO_STORE
            )
        if parsed.path.startswith("/node/"):
            assert COLLECTOR is not None
            name = urllib.parse.unquote(parsed.path[len("/node/"):])
            if name not in COLLECTOR.nodes_by_name and COLLECTOR.node_snapshot(name) is None:
                return self.send_body(
                    404,
                    b'not found\n\nUnknown node. Return to the fleet: <a href="/">/</a>\n',
                    "text/html; charset=utf-8",
                    {"X-Content-Type-Options": "nosniff"},
                )
            # Same bytes as the fleet page; the client branches on the path.
            return self.send_body(
                200, PAGE.encode(), "text/html; charset=utf-8", NO_STORE
            )
        if parsed.path == "/ironwood-status.json":
            assert COLLECTOR is not None
            headers = self.public_headers()
            if not RATE_LIMITER.allow(self.rate_limit_client()):
                headers["Retry-After"] = str(int(PUBLIC_RATE_WINDOW))
                return self.send_json(
                    429,
                    {
                        "schema_version": PUBLIC_SCHEMA_VERSION,
                        "network": COLLECTOR.network,
                        "error": {
                            "code": "rate_limited",
                            "message": "Request rate limit exceeded.",
                        },
                    },
                    headers,
                )
            status, payload = COLLECTOR.ironwood_status()
            return self.send_json(status, payload, headers)
        if parsed.path == "/v1/dashboard" and ACTIVATION_FEED is not None:
            headers = self.public_headers()
            if not RATE_LIMITER.allow(self.rate_limit_client()):
                headers["Retry-After"] = str(int(PUBLIC_RATE_WINDOW))
                return self.send_json(429, {"schemaVersion": 1, "error": "Request rate limit exceeded"}, headers)
            code, payload = ACTIVATION_FEED.response()
            headers["Cache-Control"] = "public, max-age=10" if code == 200 else "no-store"
            return self.send_json(code, payload, headers)
        if parsed.path == "/v1/status" and COLLECTOR.nu7 is not None:
            headers = self.public_headers()
            if not RATE_LIMITER.allow(self.rate_limit_client()):
                headers["Retry-After"] = str(int(PUBLIC_RATE_WINDOW))
                return self.send_json(429, {"schemaVersion": NU7_SCHEMA_VERSION,
                                            "status": "unavailable",
                                            "error": "Request rate limit exceeded"}, headers)
            code, payload = COLLECTOR.nu7.response()
            if code == 200:
                headers["Cache-Control"] = "public, max-age=10"
            return self.send_json(code, payload, headers)
        if parsed.path == "/healthz":
            return self.send_body(
                200,
                b"ok\n",
                "text/plain; charset=utf-8",
                {
                    "Cache-Control": "no-store",
                    "X-Content-Type-Options": "nosniff",
                },
            )
        if parsed.path == "/":
            return self.send_body(
                200,
                PAGE.encode(),
                "text/html; charset=utf-8",
                NO_STORE,
            )
        return self.send_body(
            404,
            b"not found\n",
            "text/plain; charset=utf-8",
            {"X-Content-Type-Options": "nosniff"},
        )

    def do_OPTIONS(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        public_paths = {"/ironwood-status.json"}
        if COLLECTOR is not None and COLLECTOR.nu7 is not None:
            public_paths.add("/v1/status")
        if ACTIVATION_FEED is not None:
            public_paths.add("/v1/dashboard")
        if parsed.path not in public_paths:
            return self.send_body(
                404,
                b"not found\n",
                "text/plain; charset=utf-8",
                {"X-Content-Type-Options": "nosniff"},
            )
        return self.send_body(
            204,
            b"",
            "application/json; charset=utf-8",
            self.public_headers(),
        )


def main() -> None:
    global COLLECTOR, PROPAGATION, ACTIVATION_FEED, TRUSTED_PROXY_IPS, DASHBOARD_LOGIN

    parser = argparse.ArgumentParser(description="Serve a Zakura fleet status dashboard.")
    if sys.argv[1:] == ["--print-password-hash"]:
        password = sys.stdin.readline().rstrip("\n")
        if not password:
            sys.exit("empty password on stdin")
        print(hash_password(password))
        return
    parser.add_argument("--config", required=True, help="path to deploy/deployer nodes TOML")
    parser.add_argument("--host", default="0.0.0.0", help="dashboard bind host")
    parser.add_argument("--port", type=int, default=8090, help="dashboard bind port")
    parser.add_argument("--interval", type=float, default=10.0, help="poll interval in seconds")
    parser.add_argument(
        "--network",
        choices=sorted(IRONWOOD_ACTIVATION_HEIGHTS),
        required=True,
        help="network served by this dashboard",
    )
    parser.add_argument(
        "--stale-after",
        type=float,
        default=300.0,
        help="mark a node stale if height has not advanced in this many seconds",
    )
    parser.add_argument(
        "--state-file",
        default="",
        help="optional JSON path for durable orphan-pair history",
    )
    parser.add_argument(
        "--history-window",
        type=float,
        default=DEFAULT_NODE_HISTORY_WINDOW,
        help="seconds of per-node sparkline history to retain in memory",
    )
    parser.add_argument(
        "--expose-logs",
        action="store_true",
        help="serve each node's redacted log error tail on the public node page",
    )
    parser.add_argument(
        "--nu7-config",
        default="",
        help="primary NU7 fork node config; serves the NU7 fork's /v1/status",
    )
    parser.add_argument(
        "--nu7-generation-start",
        type=float,
        default=0.0,
        help="Unix time the current NU7 network generation started; older observations are not counted",
    )
    parser.add_argument(
        "--metrics-min-interval",
        type=float,
        default=None,
        help="seconds between metric scrapes; omit to adapt to the last scrape's cost",
    )
    parser.add_argument("--nu7-activation-config", default="",
                        help="approved profile/selector JSON; serves coherent /v1/dashboard")
    parser.add_argument("--trusted-proxy", action="append", default=[], type=ipaddress.ip_address,
                        help="explicit reverse-proxy IP allowed to supply X-Forwarded-For; repeat per IP")
    args = parser.parse_args()
    TRUSTED_PROXY_IPS = set(args.trusted_proxy)
    DASHBOARD_LOGIN = dashboard_login_from_env(os.environ)

    nodes = load_nodes(Path(args.config))
    state_file = Path(args.state_file) if args.state_file else None
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    nu7 = (Nu7Status(nodes, Path(args.nu7_config), args.nu7_generation_start)
           if args.nu7_config else None)
    COLLECTOR = ClusterCollector(
        nodes,
        args.interval,
        args.stale_after,
        args.network,
        state_file=state_file,
        history_window=args.history_window,
        expose_logs=args.expose_logs,
        metrics_min_interval=args.metrics_min_interval,
        nu7=nu7,
    )
    threading.Thread(target=COLLECTOR.loop, daemon=True).start()
    if any(node.profiler_url for node in nodes):
        # Only a fleet with profilers has a cluster view, so only its dashboard host
        # needs the module that builds it.
        from propagation import join
        PROPAGATION = PropagationView(nodes, join)
    if args.nu7_activation_config:
        from nu7_activation import ActivationFeed
        ACTIVATION_FEED = ActivationFeed(args.nu7_activation_config, nu7)
        threading.Thread(target=ACTIVATION_FEED.loop, daemon=True).start()

    print(
        f"cluster status dashboard bound on {args.host}:{args.port}; "
        f"polling {len(nodes)} node(s) every {args.interval}s"
        + (f"; state {state_file}" if state_file else "")
        + ("; fleet pages need the dashboard password" if DASHBOARD_LOGIN else "; no dashboard password set")
    )
    BoundedHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
