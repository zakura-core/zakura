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


PAGE = r"""<!doctype html><html><head><meta charset=utf-8><title>Zebra cluster status</title>
<style>
body{font-family:system-ui,sans-serif;margin:0;background:#0e1116;color:#d7dde5}
header{padding:14px 18px;background:#161b22;border-bottom:1px solid #30363d}
h1{font-size:16px;margin:0 0 4px;font-weight:600}.sub{font-size:12px;color:#8b949e}
main{padding:14px 18px}.summary{margin-bottom:12px;font-size:13px;color:#8b949e}
table{width:100%;border-collapse:collapse;background:#161b22;border:1px solid #30363d;border-radius:8px;overflow:hidden}
th,td{padding:9px 10px;border-bottom:1px solid #30363d;text-align:left;font-size:13px;vertical-align:top}
th{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:#8b949e;background:#0f141b}
tr:last-child td{border-bottom:0}.num{text-align:right;font-variant-numeric:tabular-nums}
.badge{display:inline-block;border-radius:999px;padding:2px 8px;font-size:12px;font-weight:600}
.healthy{background:#12351f;color:#3fb950}.stale{background:#3a2d12;color:#e3b341}
.down,.rpc_error{background:#3d1417;color:#ff7b72}.starting{background:#21262d;color:#8b949e}
.muted{color:#8b949e}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
</style></head><body>
<header><h1>Zebra cluster status</h1><div class=sub id=status>connecting...</div></header>
<main><div class=summary id=summary></div><table>
<thead><tr><th>Node</th><th>Health</th><th>Commit</th><th>Last restarted</th><th class=num>Height</th><th>Last advanced</th><th>Details</th></tr></thead>
<tbody id=rows></tbody></table></main>
<script>
function age(seconds){
 if(seconds==null)return 'never observed';
 if(seconds<60)return Math.round(seconds)+'s ago';
 if(seconds<3600)return Math.round(seconds/60)+'m ago';
 return Math.round(seconds/3600)+'h ago';
}
function shortCommit(c){return c?c.slice(0,12):'unknown'}
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
async function tick(){
 let data;
 try{data=await (await fetch('/data')).json()}catch(e){
  document.getElementById('status').textContent='dashboard unreachable';return;
 }
 const poll=data.last_poll?new Date(data.last_poll*1000).toLocaleString():'not yet polled';
 document.getElementById('status').textContent='last poll: '+poll+' | stale after '+Math.round(data.stale_after)+'s';
 document.getElementById('summary').textContent=data.healthy+' / '+data.total+' nodes healthy';
 const body=document.getElementById('rows');
 body.innerHTML=data.rows.map(r=>`
  <tr>
   <td><div>${esc(r.name)}</div><div class="muted mono">${esc(r.ssh)}</div></td>
   <td><span class="badge ${esc(r.health)}">${esc(r.health)}</span></td>
   <td class=mono title="${esc(r.commit)}">${esc(shortCommit(r.commit))}</td>
   <td>${esc(r.last_restarted||'unknown')}</td>
   <td class="num mono">${r.height==null?'--':esc(r.height)}</td>
   <td>${esc(age(r.seconds_since_advanced))}</td>
   <td>${esc(r.detail||'')}</td>
  </tr>`).join('');
}
tick();setInterval(tick,10000);
</script></body></html>"""


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
