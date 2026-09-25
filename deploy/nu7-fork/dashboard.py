#!/usr/bin/env python3
"""Small read-only status feed for the public NU7 fork dashboard."""

from __future__ import annotations

import argparse
import ipaddress
import json
import logging
import re
import statistics
import subprocess
import threading
import time
import tomllib
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


TARGET_SPACING_SECONDS = 25
DAA_WINDOW_BLOCKS = 102
RECENT_HEADER_COUNT = 31
MAX_OBSERVATION_AGE_SECONDS = 120
# Bounds for the public HTTP server: explorer requests each hold a thread and a
# node RPC call, on the host that also validates and mines the fork.
MAX_CONCURRENT_REQUESTS = 16
REQUEST_TIMEOUT_SECONDS = 10
BLOCK_ID = re.compile(r"(?:[0-9]{1,10}|[0-9a-fA-F]{64})\Z")
TX_ID = re.compile(r"[0-9a-fA-F]{64}\Z")


def rpc(port: int, method: str, params: list | None = None):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/",
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}).encode(),
        {"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        result = json.load(response)
    if result.get("error") is not None:
        raise RuntimeError(f"{method}: {result['error']}")
    return result["result"]


def network_parameters(path: Path) -> dict:
    with path.open("rb") as stream:
        config = tomllib.load(stream)
    network = config["network"]
    # A configured testnet is `network = { ... }` since #1147. The running fork's
    # config predates that and keeps its parameters in [network.testnet_parameters].
    parameters = (network["network"] if isinstance(network.get("network"), dict)
                  else network["testnet_parameters"])
    return {
        "name": parameters["network_name"],
        "magic": "".join(f"{byte:02x}" for byte in parameters["network_magic"]),
        "activationHeight": parameters["activation_heights"]["NU7"],
        "nsmSeedZat": parameters["initial_nsm_value_balance"],
        "targetSpacingSeconds": TARGET_SPACING_SECONDS,
        "daaWindowBlocks": DAA_WINDOW_BLOCKS,
    }


def external_peer_count(peers: list[dict]) -> int:
    count = 0
    for peer in peers:
        address = peer.get("addr", "")
        host = address.rsplit(":", 1)[0].strip("[]")
        try:
            if not ipaddress.ip_address(host).is_loopback:
                count += 1
        except ValueError:
            continue
    return count


def active_miners() -> int:
    result = subprocess.run(
        ["systemctl", "is-active", "zakura-fork-miner.service"],
        capture_output=True, text=True, timeout=3, check=False,
    )
    return sum(line == "active" for line in result.stdout.splitlines())


def transaction_summary(transaction: dict) -> dict:
    inputs = transaction.get("vin") or []
    outputs = transaction.get("vout") or []
    orchard = transaction.get("orchard") or {}
    ironwood = transaction.get("ironwood") or {}
    return {
        "txid": transaction["txid"],
        "coinbase": any("coinbase" in item for item in inputs),
        "inputCount": len(inputs),
        "outputCount": len(outputs),
        "transparentOutputZat": sum(item.get("valueZat", 0) for item in outputs),
        "saplingSpends": len(transaction.get("vShieldedSpend") or []),
        "saplingOutputs": len(transaction.get("vShieldedOutput") or []),
        "orchardActions": len(orchard.get("actions") or []),
        "ironwoodActions": len(ironwood.get("actions") or []),
    }


def block_detail(block: dict) -> dict:
    return {
        "hash": block["hash"], "height": block["height"],
        "confirmations": block.get("confirmations"), "time": block["time"],
        "size": block.get("size"), "version": block.get("version"),
        "difficulty": block.get("difficulty"), "bits": block.get("bits"),
        "nonce": block.get("nonce"), "merkleRoot": block.get("merkleroot"),
        "previousHash": block.get("previousblockhash"),
        "nextHash": block.get("nextblockhash"),
        "transactions": [transaction_summary(tx) for tx in block.get("tx", [])],
    }


def transaction_detail(transaction: dict) -> dict:
    summary = transaction_summary(transaction)
    return {
        **summary,
        "blockHash": transaction.get("blockhash"),
        "height": transaction.get("height"),
        "confirmations": transaction.get("confirmations"),
        "time": transaction.get("blocktime") or transaction.get("time"),
        "size": transaction.get("size"), "version": transaction.get("version"),
        "expiryHeight": transaction.get("expiryheight"),
        "inputs": [
            {key: item[key] for key in ("txid", "vout", "coinbase", "sequence") if key in item}
            for item in transaction.get("vin") or []
        ],
        "outputs": [
            {"index": item.get("n"), "valueZat": item.get("valueZat"),
             "addresses": item.get("scriptPubKey", {}).get("addresses") or [],
             "scriptType": item.get("scriptPubKey", {}).get("type")}
            for item in transaction.get("vout") or []
        ],
    }


class Collector:
    def __init__(self, config: Path, primary_port: int, observer_port: int,
                 remote_miners: list[dict] | None = None):
        self.network = network_parameters(config)
        self.ports = (("primary", primary_port), ("local observer", observer_port))
        self.remote_miners = remote_miners or []
        self.lock = threading.Lock()
        self.payload: dict | None = None
        self.headers: dict[int, dict] = {}
        self.last_tip: tuple[int, str] | None = None
        self.observed_blocks: deque[float] = deque()
        self.observed_reorgs: deque[float] = deque()
        self.started_at = time.time()

    def sample_headers(self, port: int, height: int) -> list[dict]:
        # Post-NU7 blocks only, but always at least the tip: a freshly seeded fork
        # sits below activation until its first blocks are mined.
        start = max(min(self.network["activationHeight"], height),
                    height - RECENT_HEADER_COUNT + 1)
        for number in range(start, height + 1):
            if number not in self.headers:
                block_hash = rpc(port, "getblockhash", [number])
                self.headers[number] = rpc(port, "getblockheader", [block_hash, True])
        self.headers = {number: header for number, header in self.headers.items() if number >= start}
        return [self.headers[number] for number in range(start, height + 1)]

    def sample_remote_miner(self, miner: dict, primary: dict) -> dict:
        result = {"id": miner["id"], "region": miner["region"], "healthy": False}
        try:
            with urllib.request.urlopen(miner["url"], timeout=2) as response:
                status = json.load(response)
            result.update({"minerActive": status["minerActive"],
                           "nodeHealthy": status["nodeHealthy"],
                           "acceptedBlocks24h": status.get("acceptedBlocks24h"),
                           "observedAt": status["observedAt"]})
            height = status["height"]
            # A remote report is untrusted: a malformed height would reach the primary's
            # RPC below and must not take down the whole status feed.
            if (type(height) is not int or height < 0
                    or not isinstance(status["recentHashes"], dict)):
                raise ValueError("malformed remote miner status")
            fresh = 0 <= time.time() - status["observedAt"] <= 90
            lag = primary["height"] - height
            common_height = min(height, primary["height"])
            primary_hash = (primary["hash"] if common_height == primary["height"]
                            else rpc(primary["port"], "getblockhash", [common_height]))
            same_chain = status["recentHashes"].get(str(common_height)) == primary_hash
            healthy = (fresh and status["minerActive"] and status["nodeActive"]
                       and status["nodeHealthy"] and same_chain and -2 <= lag <= 2
                       and status["branchId"] == primary["branch"]
                       and status["activationHeight"] == self.network["activationHeight"])
            result.update({"healthy": healthy, "height": height, "lagBlocks": lag})
        except (OSError, ValueError, KeyError, TypeError, RuntimeError, urllib.error.URLError):
            pass
        return result

    def collect(self) -> dict:
        observed_at = time.time()
        nodes = []
        for name, port in self.ports:
            try:
                info = rpc(port, "getblockchaininfo")
                nu7 = next(
                    ((branch, upgrade) for branch, upgrade in info["upgrades"].items()
                     if upgrade.get("name") == "NU7"), None,
                )
                if (info.get("chain") != "test" or nu7 is None
                        or nu7[1].get("activationheight") != self.network["activationHeight"]):
                    raise RuntimeError("RPC network or NU7 activation does not match config")
                peers = rpc(port, "getpeerinfo")
                nodes.append({"name": name, "height": info["blocks"],
                              "hash": info["bestblockhash"], "peers": len(peers),
                              "externalPeers": external_peer_count(peers),
                              "healthy": True, "info": info, "branch": nu7[0], "port": port})
            except (OSError, ValueError, KeyError, RuntimeError, urllib.error.URLError) as error:
                nodes.append({"name": name, "healthy": False, "error": str(error)})

        primary = nodes[0]
        if not primary["healthy"]:
            return {"schemaVersion": 1, "observedAt": observed_at, "status": "unavailable",
                    "network": self.network,
                    "nodes": [{"name": node["name"], "healthy": node["healthy"]} for node in nodes],
                    "error": "Primary node RPC unavailable"}

        height = primary["height"]
        block_hash = primary["hash"]
        headers = self.sample_headers(primary["port"], height)
        if self.last_tip and self.last_tip != (height, block_hash):
            old_height, old_hash = self.last_tip
            # A lower tip can only come from a reorganization, and its old height may
            # no longer exist to compare hashes at.
            if (old_height > height
                    or rpc(primary["port"], "getblockhash", [old_height]) != old_hash):
                self.observed_reorgs.append(observed_at)
                self.headers.clear()
                headers = self.sample_headers(primary["port"], height)
            if height > old_height:
                self.observed_blocks.extend([observed_at] * (height - old_height))
        self.last_tip = (height, block_hash)

        cutoff = observed_at - 86400
        while self.observed_blocks and self.observed_blocks[0] < cutoff:
            self.observed_blocks.popleft()
        while self.observed_reorgs and self.observed_reorgs[0] < cutoff:
            self.observed_reorgs.popleft()

        intervals = [max(0, right["time"] - left["time"])
                     for left, right in zip(headers, headers[1:])]
        info = primary["info"]
        agreement = nodes[1]["healthy"] and nodes[1]["hash"] == block_hash
        nsm_balance = info.get("nsmValueBalanceZat")
        remote_miners = [self.sample_remote_miner(miner, primary)
                         for miner in self.remote_miners]
        reissuance = info.get("nsmReissuanceHeight")
        network = {**self.network, "branchId": primary["branch"],
                   "reissuanceHeight": reissuance,
                   "reissuanceKnown": "nsmReissuanceHeight" in info}
        return {
            "schemaVersion": 1, "observedAt": observed_at,
            "status": "live" if agreement else "degraded", "network": network,
            "chain": {"height": height, "hash": block_hash, "blockTime": headers[-1]["time"],
                      "tipAgeSeconds": max(0, int(observed_at - headers[-1]["time"])),
                      "difficulty": info.get("difficulty"),
                      "meanIntervalSeconds": round(statistics.mean(intervals), 1) if intervals else None,
                      "medianIntervalSeconds": round(statistics.median(intervals), 1) if intervals else None,
                      "intervalSampleBlocks": len(intervals)},
            "nsm": {"balanceZat": nsm_balance, "available": nsm_balance is not None,
                    "seedZat": self.network["nsmSeedZat"]},
            "observation": {"localNodesAgree": agreement,
                            "blocks24h": len(self.observed_blocks),
                            "reorgs24h": len(self.observed_reorgs),
                            "reorgRate24h": (len(self.observed_reorgs) / len(self.observed_blocks)
                                             if self.observed_blocks else None),
                            "since": self.started_at,
                            "scope": "two nodes on one host; observed tip replacements only"},
            "mining": {"operatorMinersActive": active_miners() + sum(
                           miner["healthy"] for miner in remote_miners),
                       "operatorMinersConfigured": 1 + len(remote_miners),
                       "remoteMiners": remote_miners},
            "nodes": [{key: node[key] for key in ("name", "healthy", "height", "hash", "peers", "externalPeers")
                       if key in node} for node in nodes],
            "recentBlocks": [{"height": header["height"], "hash": header["hash"],
                              "time": header["time"], "difficulty": header.get("difficulty")}
                             for header in reversed(headers[-8:])],
        }

    def run(self, interval: float):
        while True:
            try:
                payload = self.collect()
            except Exception:
                # The public payload stays generic; the journal keeps the cause.
                logging.exception("status collection failed")
                payload = {"schemaVersion": 1, "observedAt": time.time(),
                           "status": "unavailable", "network": self.network,
                           "error": "Collector failed to sample node RPC"}
            with self.lock:
                self.payload = payload
            time.sleep(interval)


class BoundedHTTPServer(ThreadingHTTPServer):
    """A threading HTTP server that handles at most `max_concurrent` requests at once.

    ThreadingHTTPServer starts one thread per connection without limit. Here the
    accept loop waits for a free slot instead, so excess clients queue in the listen
    backlog rather than exhausting threads and node RPC capacity.
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
    collector: Collector
    # Socket timeout, so a client that stalls mid-request frees its slot.
    timeout = REQUEST_TIMEOUT_SECONDS

    def do_GET(self):
        if self.path == "/healthz":
            body = b"ok\n"
            status = 200
            content_type = "text/plain"
        elif self.path == "/v1/status":
            with self.collector.lock:
                payload = self.collector.payload
            if payload is None or time.time() - payload["observedAt"] > MAX_OBSERVATION_AGE_SECONDS:
                payload = {"schemaVersion": 1, "status": "unavailable",
                           "error": "No fresh observation", "network": self.collector.network}
            status = 200 if payload["status"] != "unavailable" else 503
            body = json.dumps(payload, separators=(",", ":")).encode()
            content_type = "application/json"
        elif self.path.startswith("/v1/block/") or self.path.startswith("/v1/tx/"):
            kind, identifier = self.path.removeprefix("/v1/").split("/", 1)
            valid = BLOCK_ID.fullmatch(identifier) if kind == "block" else TX_ID.fullmatch(identifier)
            if not valid:
                self.send_error(400, "Invalid explorer identifier")
                return
            try:
                if kind == "block":
                    result = rpc(self.collector.ports[0][1], "getblock", [identifier, 2])
                    payload = block_detail(result)
                else:
                    result = rpc(self.collector.ports[0][1], "getrawtransaction", [identifier, 1])
                    payload = transaction_detail(result)
                status = 200
            except (OSError, ValueError, KeyError, RuntimeError, urllib.error.URLError):
                payload = {"error": "Block or transaction unavailable on this node"}
                status = 404
            body = json.dumps(payload, separators=(",", ":")).encode()
            content_type = "application/json"
        else:
            self.send_error(404)
            return
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "public, max-age=10" if status == 200 else "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("/etc/zakura/zakura.toml"))
    parser.add_argument("--primary-port", type=int, default=18232)
    parser.add_argument("--observer-port", type=int, default=18242)
    parser.add_argument("--remote-miners", type=Path,
                        default=Path("/etc/zakura/nu7-remote-miners.json"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8093)
    parser.add_argument("--interval", type=float, default=15)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    remote_miners = json.loads(args.remote_miners.read_text())["miners"] if args.remote_miners.exists() else []
    collector = Collector(args.config, args.primary_port, args.observer_port, remote_miners)
    Handler.collector = collector
    threading.Thread(target=collector.run, args=(args.interval,), daemon=True).start()
    BoundedHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
