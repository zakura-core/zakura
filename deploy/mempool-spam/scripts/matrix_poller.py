#!/usr/bin/env python3
"""Poll an environment's RPC nodes and write a transaction propagation matrix."""

from __future__ import annotations

import argparse
import json
import math
import time
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
HARNESS_DIR = SCRIPT_DIR.parent
FORBIDDEN_PATH_PARTS = (
    "mnemonic",
    "recovery.json",
    "identity.txt",
    "keys.toml",
    "wallet.dat",
    "seed",
)


def rpc_call(url: str, method: str, params=None, timeout=15):
    request = urllib.request.Request(
        url,
        data=json.dumps(
            {"jsonrpc": "1.0", "id": "matrix-poller", "method": method, "params": params or []}
        ).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = json.loads(response.read())
    if body.get("error"):
        raise RuntimeError(f"{url} {method}: {body['error']}")
    return body["result"]


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), math.ceil(len(ordered) * pct / 100)))
    return round(ordered[rank - 1], 3)


def load_config(args: argparse.Namespace) -> dict:
    path = args.config or HARNESS_DIR / "envs" / args.environment / "config.json"
    if not path.is_file():
        raise SystemExit(f"environment config does not exist: {path}")
    config = json.loads(path.read_text())
    if not config.get("nodes"):
        raise SystemExit(f"config has no nodes: {path}")
    return config


def load_watch(path: Path | None) -> set[str] | None:
    if path is None:
        return None
    return {
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }


def sample_node(node: dict, elapsed: float) -> dict:
    sample = {
        "node": node["name"],
        "impl": node.get("impl", "unknown"),
        "elapsed": round(elapsed, 3),
    }
    try:
        info = rpc_call(node["rpc_url"], "getblockchaininfo")
        sample["height"] = info.get("blocks")
        sample["bestblockhash"] = info.get("bestblockhash")
        sample["chaintip"] = (info.get("consensus") or {}).get("chaintip")
    except Exception as exc:  # noqa: BLE001
        sample["rpc_error"] = str(exc)
    try:
        sample["mempool_txids"] = rpc_call(node["rpc_url"], "getrawmempool")
    except Exception:  # noqa: BLE001
        sample["mempool_txids"] = None
    try:
        peers = rpc_call(node["rpc_url"], "getpeerinfo")
        sample["peers"] = len(peers)
        sample["peer_addrs"] = sorted(
            {peer.get("addr") for peer in peers if isinstance(peer, dict) and peer.get("addr")}
        )
    except Exception:  # noqa: BLE001
        pass
    return sample


def build_matrix(
    first_seen: dict[str, dict[str, float]], node_names: list[str], watch: set[str] | None
) -> dict:
    rows = []
    for txid in sorted(watch if watch is not None else first_seen):
        seen = first_seen.get(txid, {})
        spread = (
            round(max(seen.values()) - min(seen.values()), 3) if len(seen) >= 2 else None
        )
        rows.append(
            {
                "txid": txid,
                "seen_by": sorted(seen),
                "missing": sorted(name for name in node_names if name not in seen),
                "first_seen": dict(sorted(seen.items())),
                "spread_secs": spread,
                "on_all_nodes": bool(node_names) and len(seen) == len(node_names),
            }
        )
    spreads = [row["spread_secs"] for row in rows if row["spread_secs"] is not None]
    return {
        "txids": rows,
        "summary": {
            "txids_tracked": len(rows),
            "txids_seen_any": sum(bool(row["seen_by"]) for row in rows),
            "txids_on_all_nodes": sum(row["on_all_nodes"] for row in rows),
            "spread_p50_secs": percentile(spreads, 50),
            "spread_p95_secs": percentile(spreads, 95),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--environment", choices=("testnet", "mainnet"), default="testnet")
    group.add_argument("--config", type=Path)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--duration-secs", type=float, default=60)
    parser.add_argument("--interval-secs", type=float, default=2)
    parser.add_argument("--watch-txids", type=Path)
    parser.add_argument("--record-peer-graph", action="store_true")
    args = parser.parse_args()
    if any(part in str(args.out_dir).lower() for part in FORBIDDEN_PATH_PARTS):
        raise SystemExit(f"refusing output path that looks like key material: {args.out_dir}")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    nodes = load_config(args)["nodes"]
    watch = load_watch(args.watch_txids)
    node_names = [node["name"] for node in nodes]
    first_seen: dict[str, dict[str, float]] = {}
    samples = []
    peer_graph = {}
    peak_mempool = 0
    started = time.monotonic()
    while time.monotonic() - started < args.duration_secs:
        for node in nodes:
            sample = sample_node(node, time.monotonic() - started)
            txids = sample.pop("mempool_txids", None)
            peers = sample.pop("peer_addrs", None)
            if peers is not None:
                peer_graph[node["name"]] = peers
            if txids is not None:
                peak_mempool = max(peak_mempool, len(txids))
                for txid in txids:
                    first_seen.setdefault(txid, {}).setdefault(node["name"], sample["elapsed"])
                sample["mempool_size"] = len(txids)
            samples.append(sample)
        if watch and all(
            all(name in first_seen.get(txid, {}) for name in node_names) for txid in watch
        ):
            break
        time.sleep(args.interval_secs)

    result = {
        "duration_secs": round(time.monotonic() - started, 3),
        "nodes": nodes,
        "peak_mempool": peak_mempool,
        "matrix": build_matrix(first_seen, node_names, watch),
        "sample_count": len(samples),
    }
    (args.out_dir / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    (args.out_dir / "samples.jsonl").write_text(
        "".join(json.dumps(sample) + "\n" for sample in samples)
    )
    if args.record_peer_graph:
        (args.out_dir / "peer_graph.json").write_text(json.dumps(peer_graph, indent=2) + "\n")
    print(json.dumps(result["matrix"]["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
