#!/usr/bin/env python3
"""Read-only health endpoint for a remote NU7 miner and its local node."""

import argparse
import json
import subprocess
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


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


def accepted_blocks_24h(name):
    try:
        result = subprocess.run(
            ["journalctl", "--unit", name, "--since", "24 hours ago",
             "--grep", "block accepted", "--output", "cat", "--no-pager", "--quiet"],
            capture_output=True, text=True, timeout=3, check=False,
        )
        if result.returncode == 0 or (result.returncode == 1 and not result.stderr):
            return len(result.stdout.splitlines())
        return None
    except (OSError, subprocess.TimeoutExpired):
        return None


def sample(rpc_port, miner_service, node_service):
    result = {
        "observedAt": time.time(),
        "minerActive": service_active(miner_service),
        "nodeActive": service_active(node_service),
        "acceptedBlocks24h": accepted_blocks_24h(miner_service),
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
    miner_service = "zakura-fork-miner.service"
    node_service = "zakurad.service"

    def do_GET(self):
        if self.path != "/v1/miner":
            self.send_error(404)
            return
        body = json.dumps(sample(self.rpc_port, self.miner_service, self.node_service)).encode()
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
    parser.add_argument("--miner-service", default="zakura-fork-miner.service")
    parser.add_argument("--node-service", default="zakurad.service")
    args = parser.parse_args()
    Handler.rpc_port = args.rpc_port
    Handler.miner_service = args.miner_service
    Handler.node_service = args.node_service
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
