#!/usr/bin/env python3
"""Read-only health endpoint for a remote NU7 miner and its local node.

The node mines with zakurad's internal miner, so mining health comes from the
node's own config and log file rather than a separate miner service.
"""

import argparse
import json
import os
import subprocess
import threading
import time
import tomllib
import urllib.request
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Logged by zakurad's internal miner once the node accepts one of its own blocks.
MINED_BLOCK = b"successfully mined a new block"
ACCEPTED = b"success=Accepted"


def rpc(port, method, params=None):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/",
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}).encode(),
        {"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=4) as response:
        payload = json.load(response)
    if payload.get("error") is not None:
        raise RuntimeError(f"{method} failed")
    return payload["result"]


def service_active(name):
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", name], timeout=2, check=False
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def node_settings(config_path):
    """Whether the node runs its internal miner, and where it writes its log."""
    with Path(config_path).open("rb") as stream:
        config = tomllib.load(stream)
    mining = config.get("mining", {})
    enabled = mining.get("internal_miner") is True and bool(mining.get("miner_address"))
    return enabled, config.get("tracing", {}).get("log_file")


def log_line_time(line):
    """The leading RFC 3339 timestamp of a zakurad log line, or None."""
    try:
        return datetime.fromisoformat(line.split(maxsplit=1)[0].decode()).timestamp()
    except (IndexError, UnicodeDecodeError, ValueError):
        return None


class MinedBlocks:
    """Counts this node's accepted internal-miner blocks, reading only new log lines."""

    def __init__(self, path, since=0):
        self.path = path
        self.since = since
        # The file being read, as (st_dev, st_ino), and how far into it.
        self.identity = None
        self.offset = 0
        self.times = deque()
        self.lock = threading.Lock()

    def count_24h(self, now=None):
        """Blocks accepted in the last day, never counting a previous network generation."""
        now = time.time() if now is None else now
        with self.lock:
            try:
                with open(self.path, "rb") as stream:
                    status = os.fstat(stream.fileno())
                    identity = (status.st_dev, status.st_ino)
                    if identity != self.identity or status.st_size < self.offset:
                        # A rotated or replaced log is a new file even when it is as
                        # large as the old one, and a truncated one restarts: count it
                        # from the start rather than resume at a stale offset.
                        self.identity = identity
                        self.offset = 0
                        self.times.clear()
                    stream.seek(self.offset)
                    while (line := stream.readline()).endswith(b"\n"):
                        self.offset += len(line)
                        if MINED_BLOCK in line and ACCEPTED in line:
                            observed = log_line_time(line)
                            if observed is not None:
                                self.times.append(observed)
            except OSError:
                return None
            start = max(now - 86400, self.since)
            while self.times and self.times[0] < start:
                self.times.popleft()
            return len(self.times)


def sample(rpc_port, node_service, miner_enabled, mined_blocks):
    node_active = service_active(node_service)
    result = {
        "observedAt": time.time(),
        "minerActive": node_active and miner_enabled,
        "nodeActive": node_active,
        "acceptedBlocks24h": mined_blocks.count_24h() if miner_enabled else 0,
    }
    try:
        info = rpc(rpc_port, "getblockchaininfo")
        nu7 = next(
            ((branch, upgrade) for branch, upgrade in info["upgrades"].items()
             if upgrade.get("name") == "NU7"), None
        )
        if info.get("chain") != "test" or nu7 is None:
            raise ValueError("wrong network or no NU7 upgrade")
        height = info["blocks"]
        recent_hashes = {
            str(number): rpc(rpc_port, "getblockhash", [number])
            for number in range(max(0, height - 2), height + 1)
        }
        result.update({
            "nodeHealthy": True,
            "height": height,
            "hash": info["bestblockhash"],
            "recentHashes": recent_hashes,
            "branchId": nu7[0],
            "activationHeight": nu7[1]["activationheight"],
        })
    except (OSError, ValueError, KeyError, RuntimeError):
        result["nodeHealthy"] = False
    return result


class Handler(BaseHTTPRequestHandler):
    rpc_port = 18232
    node_service = "zakurad.service"
    miner_enabled = False
    mined_blocks = None

    def do_GET(self):
        if self.path != "/v1/miner":
            self.send_error(404)
            return
        body = json.dumps(sample(self.rpc_port, self.node_service, self.miner_enabled,
                                 self.mined_blocks)).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8094)
    parser.add_argument("--rpc-port", type=int, default=18232)
    parser.add_argument("--config", type=Path, default=Path("/etc/zakura/zakura.toml"),
                        help="the node config, read for [mining] and [tracing] log_file")
    parser.add_argument("--node-service", default="zakurad.service")
    parser.add_argument("--since", type=int, default=0,
                        help="Unix timestamp of the current network generation")
    args = parser.parse_args()
    if not 0 <= args.since <= time.time():
        parser.error("--since must be a non-negative timestamp in the past")
    miner_enabled, log_file = node_settings(args.config)
    if miner_enabled and not log_file:
        parser.error("the node config has no [tracing] log_file to count mined blocks from")
    Handler.rpc_port = args.rpc_port
    Handler.node_service = args.node_service
    Handler.miner_enabled = miner_enabled
    Handler.mined_blocks = MinedBlocks(log_file, args.since)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
