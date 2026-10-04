#!/usr/bin/env python3
"""Read-only health endpoint for a remote NU7 miner and its local node.

The node mines with zakurad's internal miner, so mining health comes from the
node's own config and log file rather than a separate miner service.
"""

import argparse
import json
import logging
import os
import subprocess
import threading
import time
import tomllib
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Only the primary's collector polls this endpoint, so a few slots are plenty; excess
# connections wait in the listen backlog instead of each holding a thread and RPC.
MAX_CONCURRENT_REQUESTS = 4
REQUEST_TIMEOUT_SECONDS = 5
# Requests within this window share one sample of the node.
SAMPLE_TTL_SECONDS = 5

# Whether each input was last readable. Failures are logged when they start and end,
# not on every sample, so a long outage stays visible without flooding the journal.
HEALTHY = {"node RPC": True, "node log": True}


def report_health(source, healthy, detail=""):
    if HEALTHY[source] != healthy:
        HEALTHY[source] = healthy
        if healthy:
            logging.info("%s is readable again", source)
        else:
            logging.warning("%s is unavailable: %s", source, detail)


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
        # Exact log lines identify submissions when rotation or copy-truncation
        # causes old lines to be read again. Keep observations for the full day.
        self.accepted_blocks = {}
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
                    stream.seek(self.offset)
                    while (line := stream.readline()).endswith(b"\n"):
                        self.offset += len(line)
                        if MINED_BLOCK in line and ACCEPTED in line:
                            observed = log_line_time(line)
                            if observed is not None:
                                self.accepted_blocks[line] = observed
            except OSError as error:
                report_health("node log", False, f"{self.path}: {error}")
                return None
            report_health("node log", True)
            start = max(now - 86400, self.since)
            self.accepted_blocks = {line: observed for line, observed in self.accepted_blocks.items()
                                    if observed >= start}
            return sum(observed <= now for observed in self.accepted_blocks.values())


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
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        report_health("node RPC", False, f"{type(error).__name__}: {error}")
        result["nodeHealthy"] = False
    else:
        report_health("node RPC", True)
    return result


class CachedSample:
    """Samples the node at most once per `ttl`, however often the endpoint is read."""

    def __init__(self, take, ttl=SAMPLE_TTL_SECONDS):
        self.take = take
        self.ttl = ttl
        self.lock = threading.Lock()
        self.value = None
        self.taken_at = None

    def get(self, now=None):
        now = time.monotonic() if now is None else now
        with self.lock:
            if self.taken_at is None or now - self.taken_at >= self.ttl:
                self.value = self.take()
                self.taken_at = now
            return self.value


class BoundedHTTPServer(ThreadingHTTPServer):
    """A threading HTTP server that handles at most `max_concurrent` requests at once."""

    daemon_threads = True

    def __init__(self, address, handler, max_concurrent=MAX_CONCURRENT_REQUESTS):
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
    samples = None

    def do_GET(self):
        if self.path != "/v1/miner":
            self.send_error(404)
            return
        body = json.dumps(self.samples.get()).encode()
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
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.info("serving /v1/miner on %s:%d; internal miner %s; node log %s; "
                 "counting blocks since %d", args.host, args.port,
                 "enabled" if miner_enabled else "disabled", log_file, args.since)
    mined_blocks = MinedBlocks(log_file, args.since)
    if miner_enabled:
        # Read the existing log once before serving, so the first report does not
        # outlast the collector's request timeout while a long log is scanned.
        mined_blocks.count_24h()
    Handler.samples = CachedSample(
        lambda: sample(args.rpc_port, args.node_service, miner_enabled, mined_blocks))
    BoundedHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
