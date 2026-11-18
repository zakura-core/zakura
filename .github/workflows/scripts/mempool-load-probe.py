#!/usr/bin/env python3
"""Time transaction and block propagation across a running mempool-load lab.

Each node gets its own poller thread. It reads `getbestblockhash` every
--tip-interval seconds and `getrawmempool` every --mempool-interval seconds, and
stamps what it sees with the time its own reply arrived, so no node is favoured
by poll order. The sequential monitor (mempool-load-monitor.py) takes seconds
per round once a lab has 16 nodes and block-sized pools, which hides the
differences these runs are after.

After --duration-secs it writes, under --out:
  firstseen.jsonl  every first sighting (node, kind, id, time)
  probe.json       transaction reach times, per-mode and per-hop delays, block
                   spreads and per-mode block delays, and block fullness

Reads `topology.json`, which `mempool-load-lab.py up` writes, for each node's
mode and its hop count from node 0, where the blaster submits.

Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import threading
import time
import urllib.request
from collections import deque
from pathlib import Path

RPC_PORT = 18232
NODE_IP_BASE = 101


def rpc_call(url: str, method: str, params: list | None = None, timeout: float = 10):
    """Calls one JSON-RPC method and returns its result, or raises."""
    body = json.dumps({"jsonrpc": "1.0", "id": "probe", "method": method, "params": params or []})
    req = urllib.request.Request(url, body.encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        reply = json.loads(resp.read())
    if reply.get("error"):
        raise RuntimeError(f"{method}: {reply['error']}")
    return reply["result"]


def node_url(index: int) -> str:
    """Returns node `index`'s RPC URL, matching mempool-load-lab.py's addressing."""
    return f"http://127.0.0.{NODE_IP_BASE + index}:{RPC_PORT}"


def hops_from_node_0(dials: list[list[int]]) -> list[int]:
    """Returns each node's shortest hop count to node 0 over the lab's links."""
    links: list[set[int]] = [set() for _ in dials]
    for node, targets in enumerate(dials):
        for target in targets:
            links[node].add(target)
            links[target].add(node)
    hops = [-1] * len(dials)
    hops[0] = 0
    queue = deque([0])
    while queue:
        node = queue.popleft()
        for peer in links[node]:
            if hops[peer] < 0:
                hops[peer] = hops[node] + 1
                queue.append(peer)
    return hops


def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile in seconds, rounded to the millisecond, or None if empty."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), math.ceil(pct / 100.0 * len(ordered))))
    return round(ordered[rank - 1], 3)


def spread_summary(values: list[float]) -> dict:
    """Returns the count and p50, p95 and max of `values`."""
    return {
        "n": len(values),
        "p50_s": percentile(values, 50),
        "p95_s": percentile(values, 95),
        "max_s": round(max(values), 3) if values else None,
    }


class Poller(threading.Thread):
    """Polls one node and records the first time it reports each transaction and tip."""

    def __init__(self, index: int, args, stop: threading.Event):
        super().__init__(daemon=True)
        self.index, self.url, self.args, self.stop = index, node_url(index), args, stop
        self.first: dict[str, dict[str, float]] = {"tx": {}, "tip": {}}
        self.errors = 0

    def run(self) -> None:
        """Polls until `stop` is set; tips every tick, the mempool every few ticks."""
        every = max(1, round(self.args.mempool_interval / self.args.tip_interval))
        tick = 0
        while not self.stop.is_set():
            started = time.time()
            try:
                self.see("tip", [rpc_call(self.url, "getbestblockhash", timeout=5)])
                if tick % every == 0:
                    self.see("tx", rpc_call(self.url, "getrawmempool", timeout=10))
            except Exception:  # noqa: BLE001 -- a missed poll, counted below
                self.errors += 1
            tick += 1
            self.stop.wait(max(0.0, self.args.tip_interval - (time.time() - started)))

    def see(self, kind: str, ids: list[str]) -> None:
        """Records the reply time for each id this node hasn't reported before."""
        now = time.time()
        first = self.first[kind]
        for item in ids:
            first.setdefault(item, now)


def reach_times(first_by_node: list[dict[str, float]], node_count: int) -> dict:
    """Returns how long each transaction took to reach 50%, 90% and 100% of nodes.

    Times run from the first node to report it. A transaction mined before some
    node saw it in its pool doesn't count toward that node, so `reached_all` is
    the share of transactions every node held before it was mined.
    """
    sightings: dict[str, list[float]] = {}
    for first in first_by_node:
        for txid, seen in first.items():
            sightings.setdefault(txid, []).append(seen)
    out: dict = {"txids": len(sightings)}
    reached_all = 0
    for share in (50, 90, 100):
        need = max(1, math.ceil(share / 100 * node_count))
        delays = []
        for times in sightings.values():
            if len(times) >= need:
                ordered = sorted(times)
                delays.append(ordered[need - 1] - ordered[0])
        if share == 100:
            reached_all = len(delays)
        out[f"to_{share}pct"] = spread_summary(delays)
    out["reached_all"] = reached_all
    out["reached_all_share"] = round(reached_all / len(sightings), 4) if sightings else None
    return out


def delays_by_group(first_by_node: list[dict[str, float]], groups: list) -> dict:
    """Returns each group's delay behind the first node to report each transaction."""
    earliest: dict[str, float] = {}
    for first in first_by_node:
        for txid, seen in first.items():
            earliest[txid] = min(seen, earliest.get(txid, seen))
    grouped: dict[str, list[float]] = {}
    for node, first in enumerate(first_by_node):
        bucket = grouped.setdefault(str(groups[node]), [])
        bucket.extend(seen - earliest[txid] for txid, seen in first.items())
    return {group: spread_summary(values) for group, values in sorted(grouped.items())}


def block_spreads(first_by_node: list[dict[str, float]], node_count: int) -> dict:
    """Returns how long each new tip took to go from the first node to the last."""
    sightings: dict[str, list[float]] = {}
    for first in first_by_node:
        for tip, seen in first.items():
            sightings.setdefault(tip, []).append(seen)
    spreads = [max(t) - min(t) for t in sightings.values() if len(t) == node_count]
    return {"tips": len(sightings), "reached_all": len(spreads), **spread_summary(spreads)}


def block_fullness(start_height: int, end_height: int) -> dict:
    """Returns the transaction count and size of each block node 0 mined in the window."""
    txs, sizes = [], []
    for height in range(start_height + 1, end_height + 1):
        block = rpc_call(node_url(0), "getblock", [str(height), 1], timeout=30)
        txs.append(len(block["tx"]) - 1)
        sizes.append(block["size"])
    return {
        "blocks": len(txs),
        "mean_pool_txs": round(sum(txs) / len(txs), 1) if txs else None,
        "max_pool_txs": max(txs) if txs else None,
        "mean_bytes": round(sum(sizes) / len(sizes)) if sizes else None,
        "max_bytes": max(sizes) if sizes else None,
    }


def main(argv: list[str] | None = None) -> int:
    """Polls every node for --duration-secs and writes probe.json and firstseen.jsonl."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab-dir", default="/root/mempool-lab")
    parser.add_argument("--duration-secs", type=int, default=600)
    parser.add_argument("--tip-interval", type=float, default=0.05)
    parser.add_argument("--mempool-interval", type=float, default=0.25)
    parser.add_argument("--out", default="/root/out")
    parser.add_argument("--meta", default="", help="comma-separated key=value pairs")
    args = parser.parse_args(argv)

    topology = json.loads((Path(args.lab_dir) / "topology.json").read_text())
    modes, dials = topology["modes"], topology["dials"]
    node_count = len(modes)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    start_height = rpc_call(node_url(0), "getblockcount")
    stop = threading.Event()
    pollers = [Poller(i, args, stop) for i in range(node_count)]
    for poller in pollers:
        poller.start()
    stop.wait(args.duration_secs)
    stop.set()
    for poller in pollers:
        poller.join(timeout=30)
    end_height = rpc_call(node_url(0), "getblockcount")

    with open(out / "firstseen.jsonl", "w") as log:
        for poller in pollers:
            for kind, first in poller.first.items():
                for item, seen in first.items():
                    log.write(json.dumps({"node": poller.index, "kind": kind, "id": item, "t": round(seen, 3)}) + "\n")

    txs = [p.first["tx"] for p in pollers]
    tips = [p.first["tip"] for p in pollers]
    hops = hops_from_node_0(dials)
    result = {
        "meta": dict(pair.split("=", 1) for pair in args.meta.split(",") if "=" in pair),
        "node_count": node_count,
        "modes": modes,
        "hops": hops,
        "resolution_s": {"tip": args.tip_interval, "mempool": args.mempool_interval},
        "poll_errors": [p.errors for p in pollers],
        "tx_reach": reach_times(txs, node_count),
        "tx_delay_by_mode": delays_by_group(txs, modes),
        "tx_delay_by_hop": delays_by_group(txs, hops),
        "block_spread": block_spreads(tips, node_count),
        "block_delay_by_mode": delays_by_group(tips, modes),
        "block_fullness": block_fullness(start_height, end_height),
    }
    (out / "probe.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
