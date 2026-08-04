#!/usr/bin/env python3
"""Zakura fleet dashboard and narrow public Ironwood status API.

Reads a deploy/deployer nodes TOML, polls each node over SSH, and serves a small
HTML dashboard showing the running commit, Zakura node ID, restart time, current
height, latest block hash, tip agreement across the fleet, per-node reorg
candidates, and whether the node has advanced recently. It also serves a
deliberately small public Ironwood status response for zakura.com.

Only the Python stdlib is used.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import ipaddress
import json
import re
import shlex
import subprocess
import threading
import time
import tomllib
import urllib.parse
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


IRONWOOD_ACTIVATION_HEIGHTS = {
    "mainnet": 3_428_143,
    "testnet": 4_134_000,
}
DEFAULT_UPGRADE_HEIGHT = IRONWOOD_ACTIVATION_HEIGHTS["testnet"]
DEFAULT_TARGET_SPACING = 7.5
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
PUBLIC_ORIGINS = {
    "mainnet": frozenset({"https://zakura.com"}),
    "testnet": frozenset({
        "https://zakura.com",
        "http://127.0.0.1:1111",
        "http://localhost:1111",
    }),
}
HEIGHT_HISTORY_WINDOW = 60 * 60
MIN_OBSERVED_BLOCKS = 3
MIN_OBSERVED_SECONDS = 120
MIN_SECONDS_PER_BLOCK = 1.0
MAX_SECONDS_PER_BLOCK = 10 * 60.0
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
}


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
    port: object = None

    def ssh_cmd(self, *remote: str) -> list[str]:
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
                port=merged["port"],
            )
        )

    if not nodes:
        raise SystemExit(f"no [[nodes]] defined in {config_path}")
    return nodes


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
import json
import re
import shlex
import subprocess
import sys
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
) = sys.argv[1:12]

out = {
    "service": service,
    "bin_path": bin_path,
    "log_file": log_file,
    "rpc_url": rpc_url,
    "probe_kind": probe_kind,
}

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
                    "-p", "ExecMainStartTimestamp"])
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

        best_hash = out.get("block_hash")
        if best_hash:
            try:
                header = rpc_call("getblockheader", [best_hash, True])
                if isinstance(header, dict):
                    out["previous_hash"] = header.get("previousblockhash") or ""
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
    except Exception as error:
        out["rpc_metadata_error"] = str(error)
else:
    out["rpc_error"] = "RPC disabled in deployer config"

print(json.dumps(out, separators=(",", ":")))
"""


def ssh_capture_script(node: Node, script: str) -> subprocess.CompletedProcess:
    return subprocess.run(node.ssh_cmd("bash", "-s"), input=script, text=True, capture_output=True)


def probe_node(node: Node) -> dict:
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
        f"{shlex.quote(node.container_name)} <<'PY'\n"
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


class ClusterCollector:
    def __init__(
        self,
        nodes: list[Node],
        interval: float,
        stale_after: float,
        upgrade_height: int,
        target_spacing: float,
        network: str,
        state_file: Path | None = None,
    ):
        self.nodes = nodes
        self.interval = interval
        self.stale_after = stale_after
        self.upgrade_height = upgrade_height
        self.target_spacing = target_spacing
        self.network = network
        self.state_file = state_file
        self.ironwood_activation_height = IRONWOOD_ACTIVATION_HEIGHTS[network]
        self.lock = threading.Lock()
        restored_progress = load_progress(state_file)
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
        self.height_history: list[tuple[float, int]] = []
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(self.nodes))) as pool:
            futures = {pool.submit(probe_node, node): node for node in self.nodes}
            for future in concurrent.futures.as_completed(futures):
                node = futures[future]
                try:
                    probe = future.result()
                except Exception as error:
                    probe = {"error": str(error)}
                rows.append(self.row_for(node, probe, time.time()))

        rows.sort(key=lambda row: row["name"])
        now = time.time()
        with self.lock:
            recent_reorgs = list(self.recent_reorgs)
            chain = compute_chain_summary(rows, recent_reorgs)
            enrich_chain_roles(rows, chain)
            self.rows = rows
            self.chain = chain
            self.last_poll = now
            self.record_height_sample(now, rows)
            snapshot = self.progress_snapshot()
        # Snapshot under the lock, write outside it: the stall timers must
        # survive a restart or every node reports as freshly advanced on the
        # next process start.
        self.persist_state(snapshot)

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

    def record_height_sample(self, now: float, rows: list[dict]) -> None:
        heights = [row["height"] for row in rows if row.get("height") is not None]
        if not heights:
            return

        best_height = max(heights)
        if self.height_history and best_height < self.height_history[-1][1]:
            best_height = self.height_history[-1][1]
        self.height_history.append((now, best_height))

        cutoff = now - HEIGHT_HISTORY_WINDOW
        self.height_history = [
            (sample_time, height)
            for sample_time, height in self.height_history
            if sample_time >= cutoff
        ]

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

        return {
            "name": node.name,
            "ssh": node.ssh_string,
            "healthy": healthy,
            "health": health,
            "detail": detail,
            "commit": probe.get("commit") or "",
            "block_hash": block_hash,
            "previous_hash": previous_block_hash,
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
        }

    def snapshot(self) -> dict:
        with self.lock:
            rows = [dict(row) for row in self.rows]
            last_poll = self.last_poll
            upgrade = self.upgrade_estimate(time.time())
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
            "upgrade": upgrade,
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

    def upgrade_estimate(self, now: float) -> dict:
        # A non-positive upgrade height means there is no pending activation to
        # count down to; the dashboard hides the upgrade cards.
        if self.upgrade_height <= 0:
            return {"enabled": False}

        current_height = self.height_history[-1][1] if self.height_history else None
        blocks_remaining = (
            max(self.upgrade_height - current_height, 0)
            if current_height is not None
            else None
        )
        activated = blocks_remaining == 0 if blocks_remaining is not None else False

        seconds_per_block = self.observed_seconds_per_block()
        source = "observed"
        if seconds_per_block is None:
            seconds_per_block = self.target_spacing
            source = "fallback"

        eta_seconds = None
        eta_at = None
        if blocks_remaining is not None:
            eta_seconds = blocks_remaining * seconds_per_block
            eta_at = now + eta_seconds

        return {
            "enabled": True,
            "height": self.upgrade_height,
            "current_height": current_height,
            "blocks_remaining": blocks_remaining,
            "seconds_per_block": seconds_per_block,
            "eta_seconds": eta_seconds,
            "eta_at": eta_at,
            "source": source,
            "activated": activated,
        }

    def observed_seconds_per_block(self) -> float | None:
        if len(self.height_history) < 2:
            return None

        newest_time, newest_height = self.height_history[-1]
        for oldest_time, oldest_height in self.height_history:
            blocks = newest_height - oldest_height
            seconds = newest_time - oldest_time
            if blocks < MIN_OBSERVED_BLOCKS or seconds < MIN_OBSERVED_SECONDS:
                continue

            seconds_per_block = seconds / blocks
            if MIN_SECONDS_PER_BLOCK <= seconds_per_block <= MAX_SECONDS_PER_BLOCK:
                return seconds_per_block

        return None


def coerce_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def empty_chain_summary() -> dict:
    return {
        "status": "unknown",
        "split": False,
        "compat_split": False,
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


def best_tip_row(rows: list[dict], client_name: str | None = None) -> dict | None:
    candidates = tipped_rows(rows)
    if client_name is not None:
        candidates = [
            row for row in candidates if row.get("client_name") == client_name
        ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (row["height"], row.get("block_hash") or ""),
    )


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
                behind = majority["height"] - group["height"]
                group["fork_depth"] = behind
                group["fork_depth_label"] = f"{behind} behind"

    zakurad_tip = best_tip_row(rows, "zakurad")
    zcashd_tip = best_tip_row(rows, "zcashd")
    compat_split = bool(
        zakurad_tip
        and zcashd_tip
        and (
            zakurad_tip["height"] != zcashd_tip["height"]
            or zakurad_tip["block_hash"] != zcashd_tip["block_hash"]
        )
    )

    return {
        "status": status,
        "split": split,
        "compat_split": compat_split,
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

/* ---------- hero ---------- */
.hero {
  display: grid;
  grid-template-columns: minmax(0, 1.55fr) minmax(0, 1fr);
  gap: 16px;
}
.hero.is-single { grid-template-columns: minmax(0, 1fr); }
.hero-head {
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
.progress {
  margin-top: 18px;
  height: 8px;
  border-radius: 999px;
  background: var(--base);
  border: 1px solid var(--line);
  overflow: hidden;
}
.progress-fill {
  height: 100%;
  width: 0;
  border-radius: 999px;
  background: linear-gradient(90deg, #8b3f8f, var(--pink-hi));
  transition: width 0.6s ease;
}
.progress-legend {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-top: 7px;
  font-size: 0.72rem;
  color: var(--dim);
}
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
.banner[hidden] { display: none; }
footer {
  color: var(--muted);
  font-size: 0.8rem;
  text-align: center;
  padding-top: 6px;
}

@media (max-width: 1100px) {
  .hero { grid-template-columns: 1fr; }
}
@media (max-width: 720px) {
  .shell { width: calc(100% - 22px); padding-top: 16px; }
  .pad { padding: 16px; }
  .topbar { align-items: flex-start; }
  .search input { width: 130px; }
}
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

  <section class="hero">
    <article class="panel pad" id="activation-panel">
      <div class="hero-head">
        <div>
          <p class="eyebrow">Network upgrade</p>
          <h2 id="activation-title">Ironwood activation</h2>
        </div>
        <span class="badge" id="activation-badge">pending</span>
      </div>
      <div class="big-value" id="activation-countdown">...</div>
      <p class="big-sub" id="activation-eta">waiting for the first poll</p>
      <div class="progress"><div class="progress-fill" id="activation-progress"></div></div>
      <div class="progress-legend">
        <span id="progress-from">...</span>
        <span id="progress-to">...</span>
      </div>
      <div class="facts">
        <div class="fact">
          <span class="label">Current height</span>
          <strong id="current-height">...</strong>
        </div>
        <div class="fact">
          <span class="label">Target height</span>
          <strong id="upgrade-height">...</strong>
        </div>
        <div class="fact">
          <span class="label">Blocks left</span>
          <strong id="blocks-remaining">...</strong>
        </div>
        <div class="fact">
          <span class="label">Block time</span>
          <strong id="block-time">...</strong>
          <small id="block-time-source">...</small>
        </div>
      </div>
    </article>

    <article class="panel pad">
      <div class="hero-head">
        <div>
          <p class="eyebrow">Fleet health</p>
          <h2>Nodes reporting</h2>
        </div>
        <span class="badge" id="client-mix">...</span>
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
    </article>
  </section>

  <section class="stats">
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
      <span class="label">zakurad vs zcashd</span>
      <div class="stat-value" id="compat-split">...</div>
      <small id="compat-detail">...</small>
    </div>
    <div class="stat">
      <span class="label">Last poll</span>
      <div class="stat-value" id="last-poll">...</div>
      <small id="stale-window">...</small>
    </div>
  </section>

  <section class="panel pad">
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

  <section class="panel pad">
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

  <footer>For snapshots and install instructions, see
    <a href="https://zakura.com/snapshots/" target="_blank" rel="noopener">https://zakura.com/snapshots/</a>
  </footer>
</main>
<script>
const REFRESH_MS = 10000;
// The progress bar shows the run-in to activation, not all of chain history.
const PROGRESS_WINDOW = 10000;

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
function countdown(seconds) {
  if (seconds == null) return 'waiting for height';
  if (seconds <= 0) return 'activated';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return days + 'd ' + hours + 'h';
  if (hours > 0) return hours + 'h ' + minutes + 'm';
  return Math.max(1, minutes) + 'm';
}
function blockTime(seconds) {
  if (seconds == null) return '—';
  return (seconds < 10 ? Number(seconds).toFixed(1) : Math.round(seconds)) + 's';
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
  healthy: 'ok', stale: 'warn', rpc_error: 'bad', down: 'bad', starting: 'neutral',
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
function renderActivation(data) {
  const upgrade = data.upgrade || {};
  const panel = el('activation-panel');
  const enabled = upgrade.enabled !== false;
  panel.hidden = !enabled;
  document.querySelector('.hero').classList.toggle('is-single', !enabled);
  if (!enabled) return;

  const badgeNode = el('activation-badge');
  if (upgrade.activated) {
    badgeNode.textContent = 'activated';
    badgeNode.className = 'badge tone-ok';
  } else {
    badgeNode.textContent = 'pending';
    badgeNode.className = 'badge tone-info';
  }
  el('activation-countdown').textContent = upgrade.activated
    ? 'Activated'
    : countdown(upgrade.eta_seconds);
  el('activation-eta').textContent = upgrade.activated
    ? 'The upgrade height has been reached.'
    : (upgrade.eta_at
      ? 'estimated ' + new Date(upgrade.eta_at * 1000).toLocaleString()
      : 'waiting for block movement');

  const remaining = upgrade.blocks_remaining;
  const done = remaining == null
    ? 0
    : Math.min(1, Math.max(0, (PROGRESS_WINDOW - remaining) / PROGRESS_WINDOW));
  el('activation-progress').style.width = (done * 100).toFixed(2) + '%';
  el('progress-from').textContent = 'final ' + num(PROGRESS_WINDOW) + ' blocks';
  el('progress-to').textContent = remaining == null
    ? '—'
    : (done * 100).toFixed(1) + '% complete';

  el('current-height').textContent = num(upgrade.current_height);
  el('upgrade-height').textContent = num(upgrade.height);
  el('blocks-remaining').textContent = upgrade.activated ? '0' : num(remaining);
  el('block-time').textContent = blockTime(upgrade.seconds_per_block);
  el('block-time-source').textContent = upgrade.source === 'observed'
    ? 'observed average'
    : 'target spacing fallback';
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

  const order = ['healthy', 'stale', 'rpc_error', 'down', 'starting'];
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
  el('client-mix').textContent = Object.keys(clients).sort()
    .map((client) => clients[client] + ' ' + client).join(' · ') || 'no clients';

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

  el('compat-split').innerHTML = chain.compat_split
    ? badge('diverged', 'bad')
    : badge('aligned', 'ok');
  el('compat-detail').textContent = chain.compat_split
    ? 'best zakurad and zcashd tips differ'
    : 'best zakurad and zcashd tips match';

  el('last-poll').textContent = data.last_poll
    ? new Date(data.last_poll * 1000).toLocaleTimeString()
    : 'not yet polled';
  el('stale-window').textContent =
    'stale after ' + Math.round(data.stale_after) + 's without a new block';
}
function renderChain(data) {
  const chain = data.chain || {};
  const status = chain.status || 'unknown';
  const tipGroups = chain.tip_groups || [];
  const reorgs = chain.recent_reorgs || [];

  el('chain-summary').textContent = {
    split: 'Nodes disagree on the tip hash at the leading height.',
    lagging: 'One tip hash leads; some nodes are behind.',
    agreed: 'All observed nodes share the same tip.',
  }[status] || 'Waiting for tip observations.';

  el('tip-groups').innerHTML = tipGroups.length
    ? tipGroups.map((group) => {
      const isMajority = group.height === chain.majority_height
        && group.block_hash === chain.majority_hash;
      const depth = group.fork_depth_label && !isMajority
        ? badge(group.fork_depth_label, 'warn')
        : '';
      return '<div class="tip-group ' + (isMajority ? 'is-majority' : 'is-fork') + '">'
        + '<div class="tip-group-top">'
        + badge(isMajority ? 'majority' : 'fork', isMajority ? 'ok' : 'bad')
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
      + '<div class="stack"><span class="node-name">' + esc(row.name) + '</span>'
      + '<span class="node-sub" title="' + esc(row.node_id || '') + '">'
      + esc(row.node_id ? tinyHash(row.node_id) : 'node id unknown') + '</span></div></div></td>'
      + '<td class="col-health">' + badge(HEALTH_LABEL[row.health] || row.health, healthTone) + '</td>'
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
function render() {
  if (!state.data) return;
  renderHeader(state.data);
  renderActivation(state.data);
  renderFleet(state.data);
  renderStats(state.data);
  renderChain(state.data);
  renderTable(state.data);
  renderFreshness();
}

/* ---------- data ---------- */
async function tick() {
  try {
    const response = await fetch('/data', { cache: 'no-store' });
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

/* ---------- interaction ---------- */
document.addEventListener('click', (event) => {
  const copy = event.target.closest('[data-copy]');
  if (copy) {
    event.stopPropagation();
    copyValue(copy);
    return;
  }
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
tick();
setInterval(tick, REFRESH_MS);
setInterval(renderFreshness, 1000);
</script>
</body>
</html>"""


COLLECTOR: ClusterCollector | None = None
RATE_LIMITER = RateLimiter()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass

    def send_body(
        self,
        status: int,
        body: bytes,
        content_type: str,
        headers: dict[str, str] | None = None,
    ) -> None:
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
        if peer_address.is_loopback and forwarded_for:
            candidate = forwarded_for.rsplit(",", 1)[-1].strip()
            try:
                return str(ipaddress.ip_address(candidate))
            except ValueError:
                pass
        return str(peer_address)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/data":
            assert COLLECTOR is not None
            body = json.dumps(COLLECTOR.snapshot()).encode()
            return self.send_body(200, body, "application/json")
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
            )
        return self.send_body(
            404,
            b"not found\n",
            "text/plain; charset=utf-8",
            {"X-Content-Type-Options": "nosniff"},
        )

    def do_OPTIONS(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/ironwood-status.json":
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
    global COLLECTOR

    parser = argparse.ArgumentParser(description="Serve a Zakura fleet status dashboard.")
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
        "--upgrade-height",
        type=int,
        default=DEFAULT_UPGRADE_HEIGHT,
        help="upgrade activation height to estimate; 0 hides the upgrade cards",
    )
    parser.add_argument(
        "--target-spacing",
        type=float,
        default=DEFAULT_TARGET_SPACING,
        help="fallback seconds per block before enough live samples are observed",
    )
    parser.add_argument(
        "--state-file",
        default="",
        help="optional JSON path for durable orphan-pair history",
    )
    args = parser.parse_args()

    nodes = load_nodes(Path(args.config))
    state_file = Path(args.state_file) if args.state_file else None
    COLLECTOR = ClusterCollector(
        nodes,
        args.interval,
        args.stale_after,
        args.upgrade_height,
        args.target_spacing,
        args.network,
        state_file=state_file,
    )
    threading.Thread(target=COLLECTOR.loop, daemon=True).start()

    print(
        f"cluster status dashboard bound on {args.host}:{args.port}; "
        f"polling {len(nodes)} node(s) every {args.interval}s"
        + (f"; state {state_file}" if state_file else "")
    )
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
