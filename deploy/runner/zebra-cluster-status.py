#!/usr/bin/env python3
"""Simple Zebra fleet status dashboard.

Reads a deploy/deployer nodes TOML, polls each node over SSH, and serves a small
HTML dashboard showing the running commit, restart time, current height, and
whether the node has advanced recently.

Only the Python stdlib is used.
"""

from __future__ import annotations

import argparse
import concurrent.futures
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


SSH_COMMON_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=15",
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ServerAliveInterval=30",
]

DEFAULTS = {
    "service_name": "zebrad",
    "bin_path": "/usr/local/bin/zebrad",
    "config_path": "/etc/zebrad/zebrad.toml",
    "log_file": "/var/log/zebrad/zebrad.log",
    "state_cache_dir": "/var/lib/zebrad",
    "network": "Mainnet",
    "listen_addr": "[::]:8233",
    "rpc_listen_addr": "",
    "port": None,
}


@dataclass
class Node:
    name: str
    ssh_string: str
    service_name: str
    bin_path: str
    log_file: str
    rpc_listen_addr: str
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
                service_name=merged["service_name"],
                bin_path=merged["bin_path"],
                log_file=merged["log_file"],
                rpc_listen_addr=merged["rpc_listen_addr"],
                port=merged["port"],
            )
        )

    if not nodes:
        raise SystemExit(f"no [[nodes]] defined in {config_path}")
    return nodes


def rpc_url_for(listen_addr: str) -> str:
    if not listen_addr:
        return ""
    match = re.search(r":(\d+)$", listen_addr)
    if not match:
        return ""
    return f"http://127.0.0.1:{match.group(1)}/"


REMOTE_PROBE = r"""
import json
import shlex
import subprocess
import sys
import urllib.request

service, bin_path, log_file, rpc_url = sys.argv[1:5]

out = {
    "service": service,
    "bin_path": bin_path,
    "log_file": log_file,
    "rpc_url": rpc_url,
}

def run(cmd, timeout=6):
    return subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)

try:
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
except Exception as error:
    out["active_state"] = "unknown"
    out["systemd_error"] = str(error)

try:
    proc = run([bin_path, "--version"])
    out["version"] = (proc.stdout or proc.stderr).splitlines()[0].strip()
except Exception as error:
    out["version_error"] = str(error)

try:
    grep = "grep -aoE 'git commit: [0-9a-f]+' {} 2>/dev/null | tail -1".format(
        shlex.quote(log_file)
    )
    proc = run(["bash", "-lc", grep])
    line = proc.stdout.strip()
    out["commit"] = line.rsplit(" ", 1)[-1] if line else ""
except Exception as error:
    out["commit_error"] = str(error)

if rpc_url:
    try:
        body = json.dumps({
            "jsonrpc": "2.0",
            "id": "zebra-cluster-status",
            "method": "getblockcount",
            "params": [],
        }).encode()
        req = urllib.request.Request(
            rpc_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=6) as resp:
            payload = json.loads(resp.read().decode())
        if "error" in payload and payload["error"]:
            out["rpc_error"] = payload["error"]
        else:
            out["height"] = payload.get("result")
    except Exception as error:
        out["rpc_error"] = str(error)
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
        f"{shlex.quote(rpc_url)} <<'PY'\n"
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
    def __init__(self, nodes: list[Node], interval: float, stale_after: float):
        self.nodes = nodes
        self.interval = interval
        self.stale_after = stale_after
        self.lock = threading.Lock()
        self.last_height: dict[str, int | None] = {node.name: None for node in nodes}
        self.last_advanced_at: dict[str, float | None] = {node.name: None for node in nodes}
        self.rows: list[dict] = [
            {"name": node.name, "ssh": node.ssh_string, "health": "starting", "healthy": False}
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

    def row_for(self, node: Node, probe: dict, now: float) -> dict:
        previous_height = self.last_height.get(node.name)
        height = coerce_int(probe.get("height"))

        advanced = False
        if height is not None:
            if previous_height is not None and height > previous_height:
                self.last_advanced_at[node.name] = now
                advanced = True
            self.last_height[node.name] = height

        last_advanced_at = self.last_advanced_at.get(node.name)
        seconds_since_advanced = (
            now - last_advanced_at if last_advanced_at is not None else None
        )

        active_state = probe.get("active_state") or "unknown"
        service_active = active_state == "active"
        rpc_ok = height is not None and not probe.get("rpc_error")
        recent = (
            seconds_since_advanced is not None
            and seconds_since_advanced <= self.stale_after
        )
        healthy = service_active and rpc_ok and recent

        if probe.get("error"):
            health = "down"
            detail = probe["error"]
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

        return {
            "name": node.name,
            "ssh": node.ssh_string,
            "healthy": healthy,
            "health": health,
            "detail": detail,
            "commit": probe.get("commit") or "",
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
        healthy = sum(1 for row in rows if row.get("healthy"))
        return {
            "generated_at": time.time(),
            "last_poll": last_poll,
            "stale_after": self.stale_after,
            "healthy": healthy,
            "total": len(rows),
            "rows": rows,
        }


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
  grid-template-columns: repeat(3, minmax(0, 1fr));
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
.table-wrap {
  margin-top: 16px;
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--overlay);
}
table {
  width: 100%;
  min-width: 860px;
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
function shortCommit(commit) { return commit ? commit.slice(0, 12) : 'unknown'; }
function esc(value) {
  return String(value == null ? '' : value).replace(/[&<>"']/g, (match) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  }[match]));
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
  document.getElementById('summary').textContent = data.healthy + ' / ' + data.total + ' nodes healthy';
  const body = document.getElementById('rows');
  body.innerHTML = data.rows.map((row) => `
    <tr>
      <td><div>${esc(row.name)}</div><div class="muted mono">${esc(row.ssh)}</div></td>
      <td><span class="badge ${esc(row.health)}">${esc(row.health)}</span></td>
      <td class="mono" title="${esc(row.commit)}">${esc(shortCommit(row.commit))}</td>
      <td>${esc(row.last_restarted || 'unknown')}</td>
      <td class="num mono">${row.height == null ? '--' : esc(row.height)}</td>
      <td>${esc(age(row.seconds_since_advanced))}</td>
      <td class="details">${esc(row.detail || '')}</td>
    </tr>`).join('');
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
    args = parser.parse_args()

    nodes = load_nodes(Path(args.config))
    COLLECTOR = ClusterCollector(nodes, args.interval, args.stale_after)
    threading.Thread(target=COLLECTOR.loop, daemon=True).start()

    print(
        f"cluster status dashboard bound on {args.host}:{args.port}; "
        f"polling {len(nodes)} node(s) every {args.interval}s"
    )
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
