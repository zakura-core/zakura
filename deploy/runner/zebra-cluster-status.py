#!/usr/bin/env python3
"""Simple Zakura fleet status dashboard.

Reads a deploy/deployer nodes TOML, polls each node over SSH, and serves a small
HTML dashboard showing the running commit, Zakura node ID, restart time, current
height, latest block hash, and whether the node has advanced recently.

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
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DEFAULT_UPGRADE_HEIGHT = 4_134_000
DEFAULT_TARGET_SPACING = 7.5
HEIGHT_HISTORY_WINDOW = 60 * 60
MIN_OBSERVED_BLOCKS = 3
MIN_OBSERVED_SECONDS = 120
MIN_SECONDS_PER_BLOCK = 1.0
MAX_SECONDS_PER_BLOCK = 10 * 60.0

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
) = sys.argv[1:11]

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

try:
    running = process_is_running(process_pattern)
    if running is not None:
        out["process_running"] = running
except Exception as error:
    out["process_error"] = str(error)

try:
    if service:
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
    proc = run([bin_path, "--version"])
    out["version"] = (proc.stdout or proc.stderr).splitlines()[0].strip()
except Exception as error:
    out["version_error"] = str(error)

try:
    if log_file:
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
    if log_file:
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
    headers = rpc_headers()
    try:
        height_body = json.dumps({
            "jsonrpc": "2.0",
            "id": "zebra-cluster-status",
            "method": "getblockcount",
            "params": [],
        }).encode()
        height_req = urllib.request.Request(
            rpc_url,
            data=height_body,
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(height_req, timeout=6) as resp:
            payload = json.loads(resp.read().decode())
        if "error" in payload and payload["error"]:
            out["rpc_error"] = payload["error"]
        else:
            out["height"] = payload.get("result")
    except Exception as error:
        out["rpc_error"] = str(error)

    try:
        hash_body = json.dumps({
            "jsonrpc": "2.0",
            "id": "zebra-cluster-status",
            "method": "getbestblockhash",
            "params": [],
        }).encode()
        hash_req = urllib.request.Request(
            rpc_url,
            data=hash_body,
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(hash_req, timeout=6) as resp:
            payload = json.loads(resp.read().decode())
        if "error" in payload and payload["error"]:
            out["block_hash_error"] = payload["error"]
        else:
            out["block_hash"] = payload.get("result")
    except Exception as error:
        out["block_hash_error"] = str(error)
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
        f"{shlex.quote(node.rpc_config_path)} <<'PY'\n"
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


class ClusterCollector:
    def __init__(
        self,
        nodes: list[Node],
        interval: float,
        stale_after: float,
        upgrade_height: int,
        target_spacing: float,
    ):
        self.nodes = nodes
        self.interval = interval
        self.stale_after = stale_after
        self.upgrade_height = upgrade_height
        self.target_spacing = target_spacing
        self.lock = threading.Lock()
        self.last_height: dict[str, int | None] = {node.name: None for node in nodes}
        self.last_advanced_at: dict[str, float | None] = {node.name: None for node in nodes}
        self.height_history: list[tuple[float, int]] = []
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
        now = time.time()
        rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(self.nodes))) as pool:
            futures = {pool.submit(probe_node, node): node for node in self.nodes}
            for future in concurrent.futures.as_completed(futures):
                node = futures[future]
                try:
                    probe = future.result()
                except Exception as error:
                    probe = {"error": str(error)}
                rows.append(self.row_for(node, probe, now))

        rows.sort(key=lambda row: row["name"])
        with self.lock:
            self.rows = rows
            self.last_poll = now
            self.record_height_sample(now, rows)

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
        height = coerce_int(probe.get("height"))

        advanced = False
        if height is not None:
            if previous_height is None and self.last_advanced_at.get(node.name) is None:
                self.last_advanced_at[node.name] = now
            elif previous_height is not None and height > previous_height:
                self.last_advanced_at[node.name] = now
                advanced = True
            self.last_height[node.name] = height

        last_advanced_at = self.last_advanced_at.get(node.name)
        seconds_since_advanced = (
            now - last_advanced_at if last_advanced_at is not None else None
        )

        active_state = probe.get("active_state") or "unknown"
        process_running = probe.get("process_running")
        service_active = active_state == "active" and process_running is not False
        rpc_disabled = (
            not node.rpc_listen_addr
            or probe.get("rpc_error") == "RPC disabled in deployer config"
        )
        rpc_ok = height is not None and not probe.get("rpc_error")
        recent = (
            seconds_since_advanced is not None
            and seconds_since_advanced <= self.stale_after
        )
        # Nodes with RPC intentionally disabled (e.g. zakura-compat front) are
        # healthy when the process/service is up; height comes from the sidecar row.
        healthy = service_active and (rpc_disabled or (rpc_ok and recent))

        if probe.get("error"):
            health = "down"
            detail = probe["error"]
        elif process_running is False:
            health = "down"
            detail = f"process not found: {node.process_pattern}"
        elif not service_active:
            health = "down"
            detail = f"systemd state: {active_state}"
        elif rpc_disabled:
            health = "healthy"
            detail = "service active (RPC probe disabled)"
        elif not rpc_ok:
            health = "rpc_error"
            detail = str(probe.get("rpc_error") or "RPC height unavailable")
        elif not recent:
            health = "stale"
            detail = "height has not advanced within stale window"
        else:
            health = "healthy"
            detail = "advanced this poll" if advanced else "height recently advanced"

        return {
            "name": node.name,
            "ssh": node.ssh_string,
            "healthy": healthy,
            "health": health,
            "detail": detail,
            "commit": probe.get("commit") or "",
            "block_hash": probe.get("block_hash") or "",
            "node_id": node.node_id or probe.get("node_id") or "",
            "version": probe.get("version") or "",
            "last_restarted": probe.get("last_restarted") or "",
            "height": height,
            "active_state": active_state,
            "rpc_ok": rpc_ok,
            "last_seen_at": now,
            "last_advanced_at": last_advanced_at,
            "seconds_since_advanced": seconds_since_advanced,
        }

    def snapshot(self) -> dict:
        with self.lock:
            rows = [dict(row) for row in self.rows]
            last_poll = self.last_poll
            upgrade = self.upgrade_estimate(time.time())
        healthy = sum(1 for row in rows if row.get("healthy"))
        return {
            "generated_at": time.time(),
            "last_poll": last_poll,
            "stale_after": self.stale_after,
            "healthy": healthy,
            "total": len(rows),
            "upgrade": upgrade,
            "rows": rows,
        }

    def upgrade_estimate(self, now: float) -> dict:
        # A non-positive upgrade height means there is no pending activation to
        # count down to (e.g. mainnet); the dashboard hides the upgrade cards.
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


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Zakura Ironwood testnet cluster status">
<title>Zakura cluster status</title>
<link rel="icon" href="https://avatars.githubusercontent.com/u/272444516?s=200&v=4" type="image/png">
<style>
:root {
  color-scheme: dark;
  --void: #080610;
  --base: #0e0c18;
  --surface: #131120;
  --overlay: #1a1728;
  --line: #221f32;
  --line-hi: #2e2a42;
  --ink: #e2dff0;
  --muted: #7a7494;
  --dim: #3f3a56;
  --pink: #c2457a;
  --pink-hi: #e0609a;
  --pink-soft: rgba(194, 69, 122, 0.10);
  --green: #7ecfb0;
  --yellow: #e3b341;
  --red: #ff7b72;
  --r: 14px;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  color: var(--ink);
  background:
    radial-gradient(circle at top left, rgba(194, 69, 122, 0.12), transparent 32rem),
    var(--void);
  font: 15px/1.65 'Inter', ui-sans-serif, system-ui, -apple-system, sans-serif;
}
h1, h2, p { margin: 0; }
h1 {
  color: var(--ink);
  font-size: clamp(1.45rem, 3.5vw, 2.45rem);
  font-weight: 700;
  letter-spacing: -0.02em;
  line-height: 1.1;
}
h2 {
  color: var(--ink);
  font-size: 1.05rem;
  font-weight: 600;
  letter-spacing: -0.01em;
}
.shell {
  width: min(1120px, calc(100% - 32px));
  margin: 0 auto;
  padding: 36px 0 80px;
}
.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 24px;
  padding: 22px 26px;
  background: var(--surface);
  border: 1px solid var(--line-hi);
  border-radius: var(--r);
  box-shadow:
    0 0 0 1px rgba(194, 69, 122, 0.08),
    0 8px 48px rgba(0, 0, 0, 0.55),
    0 1px 0 rgba(255, 255, 255, 0.03) inset;
}
.brand {
  display: flex;
  align-items: center;
  gap: 16px;
  min-width: 0;
}
.brand-icon {
  width: 46px;
  height: 46px;
  border: 2px solid var(--pink);
  border-radius: 50%;
  box-shadow: 0 0 0 4px var(--pink-soft), 0 4px 20px rgba(0, 0, 0, 0.5);
  flex-shrink: 0;
  object-fit: cover;
}
.brand-wordmark {
  display: flex;
  flex-direction: column;
  gap: 3px;
  min-width: 0;
}
.eyebrow {
  color: var(--muted);
  font-size: 0.72rem;
  font-weight: 500;
  letter-spacing: 0.03em;
}
.status {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 32px;
  padding: 0 14px;
  border: 1px solid rgba(194, 69, 122, 0.35);
  border-radius: 999px;
  background: var(--pink-soft);
  color: var(--pink-hi);
  font-size: 0.73rem;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  white-space: nowrap;
}
.status::before {
  content: '';
  display: block;
  width: 6px;
  height: 6px;
  background: var(--pink-hi);
  border-radius: 50%;
  box-shadow: 0 0 8px var(--pink-hi);
  animation: bloom 2.8s ease-in-out infinite;
}
@keyframes bloom {
  0%, 100% { opacity: 1; transform: scale(1); }
  50% { opacity: 0.3; transform: scale(0.7); }
}
.grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 16px;
  margin-top: 16px;
}
.panel {
  min-width: 0;
  padding: 24px;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: var(--r);
  overflow: hidden;
  position: relative;
  transition: border-color 0.2s;
}
.panel:hover { border-color: var(--line-hi); }
.panel-full { grid-column: 1 / -1; }
.panel-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 14px;
  flex-wrap: wrap;
}
.body-copy {
  color: var(--muted);
  font-size: 0.9rem;
}
.summary-card {
  padding: 12px;
  background: var(--base);
  border: 1px solid var(--line);
  border-radius: 10px;
}
.summary-card span {
  display: block;
  color: var(--muted);
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.summary-card strong {
  display: block;
  margin-top: 3px;
  color: var(--ink);
  font-size: 0.95rem;
  overflow-wrap: anywhere;
}
.summary-card small {
  display: block;
  margin-top: 2px;
  color: var(--dim);
  font-size: 0.76rem;
}
.table-wrap {
  margin-top: 16px;
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--overlay);
}
table {
  width: 100%;
  min-width: 1040px;
  border-collapse: collapse;
}
th, td {
  padding: 12px 14px;
  border-bottom: 1px solid var(--line);
  text-align: left;
  font-size: 0.86rem;
  vertical-align: top;
}
th {
  background: var(--base);
  color: var(--muted);
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
tr:last-child td { border-bottom: 0; }
tbody tr:hover td { background: rgba(46, 42, 66, 0.35); }
.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.badge {
  display: inline-flex;
  align-items: center;
  min-height: 22px;
  padding: 0 9px;
  border-radius: 999px;
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  white-space: nowrap;
}
.healthy {
  border: 1px solid rgba(126, 207, 176, 0.35);
  background: rgba(126, 207, 176, 0.08);
  color: var(--green);
}
.stale {
  border: 1px solid rgba(227, 179, 65, 0.35);
  background: rgba(227, 179, 65, 0.08);
  color: var(--yellow);
}
.down, .rpc_error {
  border: 1px solid rgba(255, 123, 114, 0.35);
  background: rgba(255, 123, 114, 0.08);
  color: var(--red);
}
.starting {
  border: 1px solid var(--line-hi);
  background: var(--base);
  color: var(--muted);
}
.muted { color: var(--muted); }
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 0.82rem;
}
.details {
  max-width: 280px;
  color: var(--muted);
}
.wide-mono {
  max-width: 260px;
  overflow-wrap: anywhere;
}
.copyable-value {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}
.copy-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  padding: 0;
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: var(--dim);
  cursor: pointer;
}
.copy-button:hover {
  background: var(--pink-soft);
  color: var(--pink-hi);
}
.copy-button svg {
  width: 14px;
  height: 14px;
  fill: none;
  stroke: currentColor;
  stroke-width: 1.8;
  stroke-linecap: round;
  stroke-linejoin: round;
}
@media (max-width: 900px) {
  .grid { grid-template-columns: 1fr; }
}
@media (max-width: 760px) {
  .shell {
    width: min(100% - 20px, 1120px);
    padding-top: 16px;
  }
  .topbar {
    flex-direction: column;
    align-items: flex-start;
  }
  .panel { padding: 18px; }
}
</style>
</head>
<body>
<main class="shell">
  <section class="topbar" aria-label="Page header">
    <div class="brand">
      <img src="https://avatars.githubusercontent.com/u/272444516?s=200&v=4" alt="Valar Group" class="brand-icon" width="44" height="44">
      <div class="brand-wordmark">
        <p class="eyebrow">Zakura Ironwood testnet observability</p>
        <h1>Zakura Cluster Status</h1>
      </div>
    </div>
    <div class="status" id="status">Connecting</div>
  </section>

  <section class="grid" aria-label="Cluster summary">
    <article class="summary-card">
      <span>Healthy nodes</span>
      <strong id="healthy-count">...</strong>
    </article>
    <article class="summary-card">
      <span>Last poll</span>
      <strong id="last-poll">...</strong>
    </article>
    <article class="summary-card">
      <span>Stale window</span>
      <strong id="stale-window">...</strong>
    </article>
    <article class="summary-card upgrade-card">
      <span>Upgrade height</span>
      <strong id="upgrade-height">...</strong>
    </article>
    <article class="summary-card upgrade-card">
      <span>Blocks remaining</span>
      <strong id="blocks-remaining">...</strong>
    </article>
    <article class="summary-card upgrade-card">
      <span>Upgrade ETA</span>
      <strong id="upgrade-eta">...</strong>
    </article>
    <article class="summary-card upgrade-card">
      <span>Block time</span>
      <strong id="block-time">...</strong>
      <small id="block-time-source">...</small>
    </article>
  </section>

  <section class="panel panel-full" style="margin-top:16px;">
    <div class="panel-header">
      <div>
        <p class="eyebrow">Fleet health</p>
        <h2>Live node status</h2>
      </div>
      <p class="body-copy" id="summary">Waiting for first poll...</p>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Node</th>
            <th>Health</th>
            <th>Commit</th>
            <th>Latest block commit hash</th>
            <th>Last restarted</th>
            <th class="num">Height</th>
            <th>Last advanced</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody id="rows"></tbody>
      </table>
    </div>
  </section>
</main>
<script>
function age(seconds) {
  if (seconds == null) return 'never observed';
  if (seconds < 60) return Math.round(seconds) + 's ago';
  if (seconds < 3600) return Math.round(seconds / 60) + 'm ago';
  return Math.round(seconds / 3600) + 'h ago';
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
function formatNumber(value) {
  return value == null ? '...' : Number(value).toLocaleString();
}
function formatBlockTime(seconds) {
  if (seconds == null) return '...';
  if (seconds < 10) return Number(seconds).toFixed(1) + 's/block';
  return Math.round(seconds) + 's/block';
}
const copyIcon = '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="10" height="10" rx="1"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
const checkIcon = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 6 9 17l-5-5"></path></svg>';
function middleHash(value, left = 8, right = 8) {
  if (!value) return 'unknown';
  if (value.length <= left + right + 4) return value;
  return value.slice(0, left) + '....' + value.slice(-right);
}
function shortCommit(commit) { return middleHash(commit, 8, 8); }
function shortHash(hash) { return middleHash(hash, 8, 8); }
function esc(value) {
  return String(value == null ? '' : value).replace(/[&<>"']/g, (match) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  }[match]));
}
async function copyValue(button) {
  const value = button.dataset.copyValue || '';
  if (!value) return;

  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(value);
  } else {
    const textarea = document.createElement('textarea');
    textarea.value = value;
    textarea.setAttribute('readonly', '');
    textarea.style.cssText = 'position:fixed;top:-1000px;left:-1000px';
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    try { document.execCommand('copy'); }
    finally { textarea.remove(); }
  }

  button.innerHTML = checkIcon;
  setTimeout(() => { button.innerHTML = copyIcon; }, 1400);
}
function copyButton(value, label) {
  if (!value) return '';
  return `<button class="copy-button" type="button" data-copy-value="${esc(value)}" aria-label="${esc(label)}" title="${esc(label)}">${copyIcon}</button>`;
}
async function tick() {
  let data;
  try {
    const response = await fetch('/data', { cache: 'no-store' });
    data = await response.json();
  } catch (error) {
    document.getElementById('status').textContent = 'Unreachable';
    document.getElementById('summary').textContent = 'Dashboard data endpoint is unreachable.';
    return;
  }
  const poll = data.last_poll ? new Date(data.last_poll * 1000).toLocaleString() : 'not yet polled';
  document.getElementById('status').textContent = data.healthy === data.total ? 'Healthy' : 'Degraded';
  document.getElementById('healthy-count').textContent = data.healthy + ' / ' + data.total;
  document.getElementById('last-poll').textContent = poll;
  document.getElementById('stale-window').textContent = Math.round(data.stale_after) + 's';
  const upgrade = data.upgrade || {};
  const upgradeEnabled = upgrade.enabled !== false;
  document.querySelectorAll('.upgrade-card').forEach((card) => {
    card.style.display = upgradeEnabled ? '' : 'none';
  });
  if (upgradeEnabled) {
    const etaAt = upgrade.eta_at ? new Date(upgrade.eta_at * 1000).toLocaleString() : 'waiting for block movement';
    document.getElementById('upgrade-height').textContent = formatNumber(upgrade.height);
    document.getElementById('blocks-remaining').textContent = upgrade.activated ? 'activated' : formatNumber(upgrade.blocks_remaining);
    document.getElementById('upgrade-eta').textContent = upgrade.activated ? 'activated' : countdown(upgrade.eta_seconds) + ' / ' + etaAt;
    document.getElementById('block-time').textContent = formatBlockTime(upgrade.seconds_per_block);
    document.getElementById('block-time-source').textContent = upgrade.source === 'observed'
      ? 'recent average'
      : 'default estimate';
  }
  document.getElementById('summary').textContent = data.healthy + ' / ' + data.total + ' nodes healthy';
  const body = document.getElementById('rows');
  body.innerHTML = data.rows.map((row) => `
    <tr>
      <td><div>${esc(row.name)}</div><div class="muted mono wide-mono">${esc(row.node_id || 'node ID unknown')}</div></td>
      <td><span class="badge ${esc(row.health)}">${esc(row.health)}</span></td>
      <td class="mono" title="${esc(row.commit)}"><span class="copyable-value"><span>${esc(shortCommit(row.commit))}</span>${copyButton(row.commit, 'Copy full commit hash')}</span></td>
      <td class="mono wide-mono" title="${esc(row.block_hash)}"><span class="copyable-value"><span>${esc(shortHash(row.block_hash))}</span>${copyButton(row.block_hash, 'Copy full latest block commit hash')}</span></td>
      <td>${esc(row.last_restarted || 'unknown')}</td>
      <td class="num mono">${row.height == null ? '--' : esc(row.height)}</td>
      <td>${esc(age(row.seconds_since_advanced))}</td>
      <td class="details">${esc(row.detail || '')}</td>
    </tr>`).join('');
  for (const button of body.querySelectorAll('[data-copy-value]')) {
    button.addEventListener('click', () => copyValue(button).catch(() => {
      button.textContent = '!';
      setTimeout(() => { button.innerHTML = copyIcon; }, 1400);
    }));
  }
}
tick();
setInterval(tick, 10000);
</script>
</body>
</html>"""


COLLECTOR: ClusterCollector | None = None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass

    def send_body(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/data":
            assert COLLECTOR is not None
            body = json.dumps(COLLECTOR.snapshot()).encode()
            return self.send_body(body, "application/json")
        return self.send_body(PAGE.encode(), "text/html; charset=utf-8")


def main() -> None:
    global COLLECTOR

    parser = argparse.ArgumentParser(description="Serve a Zebra fleet status dashboard.")
    parser.add_argument("--config", required=True, help="path to deploy/deployer nodes TOML")
    parser.add_argument("--host", default="0.0.0.0", help="dashboard bind host")
    parser.add_argument("--port", type=int, default=8090, help="dashboard bind port")
    parser.add_argument("--interval", type=float, default=10.0, help="poll interval in seconds")
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
        help="upgrade activation height to estimate; 0 hides the upgrade cards (e.g. mainnet)",
    )
    parser.add_argument(
        "--target-spacing",
        type=float,
        default=DEFAULT_TARGET_SPACING,
        help="fallback seconds per block before enough live samples are observed",
    )
    args = parser.parse_args()

    nodes = load_nodes(Path(args.config))
    COLLECTOR = ClusterCollector(
        nodes,
        args.interval,
        args.stale_after,
        args.upgrade_height,
        args.target_spacing,
    )
    threading.Thread(target=COLLECTOR.loop, daemon=True).start()

    print(
        f"cluster status dashboard bound on {args.host}:{args.port}; "
        f"polling {len(nodes)} node(s) every {args.interval}s"
    )
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
