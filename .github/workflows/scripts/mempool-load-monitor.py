#!/usr/bin/env python3
"""Sample a mempool-load testnet and emit a throughput/propagation verdict.

Samples every node's RPC and Prometheus endpoint on an interval for a fixed
duration, then derives the numbers the harness exists to produce:

  throughput    -- transactions submitted vs. accepted, from the txblast traces
  propagation   -- per-txid spread across nodes, from getrawmempool sampling
  backpressure  -- mempool queue/full-queue/failed-verify counters
  liveness      -- node crashes, panics, and post-run tip convergence

Writes summary.json (machine) + summary.md (human, posted as the PR comment).

Stdlib only, mirroring deploy/deployer/deploy.py and pr-node-monitor.py.

Exit status: 0 for an ok/degraded run, 1 for a failed run (see grade_run).
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# Mempool counters worth grading. Prometheus renders dots as underscores.
MEMPOOL_METRICS = (
    "zcash_mempool_size_transactions",
    "zcash_mempool_size_bytes",
    "zcash_mempool_cost_bytes",
    "mempool_currently_queued_transactions",
    "mempool_queued_transactions_total",
    "mempool_gossiped_transactions_total",
    "mempool_rejected_transaction_ids",
    "mempool_failed_verify_tasks_total",
    "mempool_full_queue_per_peer_total",
)

# Must match mempool-load-lab.py's NODE_IP_BASE.
NODE_IP_BASE = 101

PANIC_PATTERN = re.compile(r"panicked at|thread '.*' panicked", re.IGNORECASE)


def rpc_call(url: str, method: str, params: list | None = None, timeout: int = 15):
    payload = json.dumps(
        {
            "jsonrpc": "1.0",
            "id": "mempool-load-monitor",
            "method": method,
            "params": params or [],
        }
    ).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = json.loads(resp.read())
    if body.get("error"):
        raise RuntimeError(f"{method}: {body['error']}")
    return body["result"]


def scrape_metrics(url: str, wanted: tuple[str, ...], timeout: int = 10) -> dict[str, float]:
    """Parse the wanted series out of a Prometheus text exposition response."""
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        text = resp.read().decode("utf-8", errors="replace")
    return parse_prometheus(text, wanted)


def parse_prometheus(text: str, wanted: tuple[str, ...]) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, rest = line.partition(" ")
        # Strip any label set: `metric{label="v"} 1.0` -> `metric`.
        base = name.split("{", 1)[0]
        if base not in wanted:
            continue
        try:
            value = float(rest.strip())
        except ValueError:
            continue
        # Sum across label sets (e.g. per-peer counters).
        out[base] = out.get(base, 0.0) + value
    return out


# ---------------------------------------------------------------------------- #
# Sampling
# ---------------------------------------------------------------------------- #


def sample_node(node: dict, elapsed: float) -> dict:
    # Milliseconds, not tenths: nodes within a round are sampled milliseconds
    # apart, and rounding that away collapses their propagation ordering.
    sample: dict = {"node": node["name"], "elapsed": round(elapsed, 3)}
    try:
        info = rpc_call(node["rpc_url"], "getblockchaininfo")
        sample["height"] = info.get("blocks")
    except Exception as exc:  # noqa: BLE001 -- any RPC failure is a missed sample
        sample["rpc_error"] = str(exc)
    try:
        sample["mempool_txids"] = rpc_call(node["rpc_url"], "getrawmempool")
    except Exception:  # noqa: BLE001
        sample["mempool_txids"] = None
    try:
        sample["peers"] = len(rpc_call(node["rpc_url"], "getpeerinfo"))
    except Exception:  # noqa: BLE001
        pass
    try:
        sample["metrics"] = scrape_metrics(node["metrics_url"], MEMPOOL_METRICS)
    except Exception:  # noqa: BLE001 -- metrics are best-effort
        sample["metrics"] = {}
    return sample


def record_first_seen(
    first_seen: dict[str, dict[str, float]], sample: dict, elapsed: float
) -> None:
    """Record the earliest time each node reported each txid in its mempool."""
    txids = sample.get("mempool_txids")
    if not txids:
        return
    node = sample["node"]
    for txid in txids:
        first_seen.setdefault(txid, {}).setdefault(node, elapsed)


# ---------------------------------------------------------------------------- #
# Derived numbers
# ---------------------------------------------------------------------------- #


def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile. Returns None for an empty input."""
    if not values:
        return None
    ordered = sorted(values)
    # Nearest-rank: the smallest value at or above the requested percentile.
    rank = max(1, min(len(ordered), math.ceil(pct / 100.0 * len(ordered))))
    return round(ordered[rank - 1], 3)


def sampling_resolution(samples: list[dict]) -> float | None:
    """Median gap between consecutive samples of the same node.

    This is the floor on every propagation figure: a txid can only be observed
    once per node per round, so any spread below one round is unresolvable and
    reads as 0. Reported alongside the percentiles so they can be read honestly.
    """
    per_node: dict[str, list[float]] = {}
    for sample in samples:
        per_node.setdefault(sample["node"], []).append(sample["elapsed"])
    gaps = [
        round(times[i + 1] - times[i], 3)
        for times in per_node.values()
        for i in range(len(times) - 1)
    ]
    return percentile(gaps, 50)


def propagation_stats(first_seen: dict[str, dict[str, float]], node_count: int) -> dict:
    """Spread between the first and last node to see each txid.

    Only txids seen by more than one node contribute a spread; a txid seen by
    exactly one node either never propagated or was mined before the next
    sample, and those are counted separately rather than folded into the
    latency figures.
    """
    spreads: list[float] = []
    fully_propagated = 0
    single_node = 0
    for _txid, seen in first_seen.items():
        if len(seen) < 2:
            single_node += 1
            continue
        spreads.append(max(seen.values()) - min(seen.values()))
        if len(seen) >= node_count:
            fully_propagated += 1
    return {
        "txids_observed": len(first_seen),
        "txids_on_multiple_nodes": len(spreads),
        "txids_on_single_node": single_node,
        "txids_on_all_nodes": fully_propagated,
        "spread_p50_secs": percentile(spreads, 50),
        "spread_p95_secs": percentile(spreads, 95),
        "spread_max_secs": round(max(spreads), 3) if spreads else None,
    }


# Workload-side failures that say nothing about the node under test. On a
# fast local chain the blaster regularly builds against an anchor that a newly
# mined block supersedes; Kresko detects this and rebuilds. Counting these as
# node rejects makes a healthy run look like a mempool regression -- at low
# submission counts a single one is several percent.
WORKLOAD_ERROR_CLASSES = frozenset({"unknown_orchard_anchor", "anchor_rejection"})

# Event names from Kresko's src/txblast/shielded.rs.
EVENT_COUNTERS = {
    "tx_submitted": "submitted",
    "tx_submit_failed": "submit_failed",
    "tx_build_failed": "build_failed",
    "tx_post_submit_mempool_seen": "mempool_seen",
    "tx_confirmed": "confirmed",
    "pending_tx_evicted": "evicted",
    "chain_rebuild_started": "chain_rebuilds",
}


def read_txblast_traces(trace_dir: Path) -> dict:
    """Summarize submit outcomes from the Kresko txblast_event JSONL stream.

    Unknown events are ignored rather than treated as failures, so a Kresko
    version bump that adds events under-counts instead of producing a spurious
    red verdict.
    """
    counts = dict.fromkeys(EVENT_COUNTERS.values(), 0)
    workload_failures = 0
    confirm_delays: list[float] = []
    files = sorted(trace_dir.glob("*txblast_event*.jsonl")) if trace_dir.is_dir() else []
    for path in files:
        for line in path.read_text(errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = EVENT_COUNTERS.get(event.get("event"))
            if key:
                counts[key] += 1
                if key in ("submit_failed", "build_failed"):
                    if event.get("error_class") in WORKLOAD_ERROR_CLASSES:
                        workload_failures += 1
            delay = event.get("confirm_delay_ms")
            if isinstance(delay, (int, float)):
                confirm_delays.append(float(delay))

    failures = counts["submit_failed"] + counts["build_failed"]
    # Only failures the node is responsible for are graded.
    node_rejects = failures - workload_failures
    graded_total = counts["submitted"] + node_rejects
    return {
        "trace_files": len(files),
        **counts,
        "workload_failures": workload_failures,
        "node_rejects": node_rejects,
        "reject_rate": round(node_rejects / graded_total, 4) if graded_total else None,
        "confirm_delay_p50_ms": percentile(confirm_delays, 50),
        "confirm_delay_p95_ms": percentile(confirm_delays, 95),
    }


def scan_logs_for_panics(log_paths: list[Path]) -> list[str]:
    hits = []
    for path in log_paths:
        if not path.is_file():
            continue
        for line in path.read_text(errors="replace").splitlines():
            if PANIC_PATTERN.search(line):
                hits.append(f"{path.name}: {line.strip()[:300]}")
    return hits


def tip_convergence(samples: list[dict], nodes: list[dict]) -> dict:
    """Compare each node's height, and check the chain actually advanced.

    Convergence alone is not evidence of health: if block production dies
    mid-window every node freezes at the same height, which looks perfectly
    converged. `advanced` is what distinguishes a live chain from a dead one.
    """
    first: dict[str, int] = {}
    last: dict[str, int] = {}
    silent_tail: dict[str, int] = {}
    for sample in samples:
        node = sample["node"]
        if isinstance(sample.get("height"), int):
            first.setdefault(node, sample["height"])
            last[node] = sample["height"]
            silent_tail[node] = 0
        else:
            # Consecutive failed samples at the end mean the node went away.
            silent_tail[node] = silent_tail.get(node, 0) + 1
    heights = list(last.values())
    growth = {node: last[node] - first[node] for node in last}
    return {
        "heights": last,
        "height_growth": growth,
        "reporting_nodes": len(last),
        "expected_nodes": len(nodes),
        "unresponsive_at_end": sorted(n for n, c in silent_tail.items() if c > 0),
        "spread": (max(heights) - min(heights)) if heights else None,
        "advanced": bool(growth) and max(growth.values()) > 0,
        "converged": bool(heights)
        and len(last) == len(nodes)
        and (max(heights) - min(heights)) <= 1,
    }


def grade_run(result: dict, args) -> tuple[str, list[str]]:
    """Return (verdict, reasons). 'failed' is the only status that exits 1."""
    reasons = []
    throughput = result["throughput"]
    convergence = result["convergence"]

    if result["panics"]:
        reasons.append(f"{len(result['panics'])} panic(s) in node logs")
    if throughput["trace_files"] == 0:
        reasons.append("no txblast trace files were produced")
    elif throughput["submitted"] == 0:
        reasons.append("txblast submitted zero transactions")
    # Guarded on sample count: throughput here is bounded by Orchard proving, so
    # a short run submits tens of transactions, where one reject is already
    # several percent. Grading that rate would fail healthy runs on noise.
    if (
        throughput["reject_rate"] is not None
        and throughput["submitted"] >= args.min_graded_submissions
        and throughput["reject_rate"] > args.max_reject_rate
    ):
        reasons.append(
            f"reject rate {throughput['reject_rate']:.2%} over {throughput['submitted']} "
            f"submissions exceeds the {args.max_reject_rate:.2%} threshold"
        )
    if not convergence["converged"]:
        reasons.append(
            f"nodes did not converge on one tip (heights: {convergence['heights']})"
        )
    # A frozen chain converges trivially, so this must be checked separately or
    # a run where block production died grades green.
    if not convergence.get("advanced"):
        reasons.append(
            f"the chain never advanced during the run (heights: {convergence['heights']})"
        )
    if convergence.get("unresponsive_at_end"):
        reasons.append(
            "node(s) stopped answering RPC before the end: "
            + ", ".join(convergence["unresponsive_at_end"])
        )
    if result["propagation"]["txids_on_multiple_nodes"] == 0:
        reasons.append("no transaction was ever observed on more than one node")
    # Zero scraped series means the metrics endpoint never answered, so every
    # backpressure number in the report is silently absent rather than zero.
    if not result["backpressure"]:
        reasons.append("no Prometheus metrics were scraped from any node")

    if reasons:
        return "failed", reasons
    # A run can be clean but thin -- flag it rather than calling it a pass.
    if result["propagation"]["txids_observed"] < args.min_txids_observed:
        return "degraded", [
            f"only {result['propagation']['txids_observed']} txids observed "
            f"(expected at least {args.min_txids_observed})"
        ]
    return "ok", []


# ---------------------------------------------------------------------------- #
# Reporting
# ---------------------------------------------------------------------------- #


def render_markdown(result: dict) -> str:
    throughput = result["throughput"]
    propagation = result["propagation"]
    convergence = result["convergence"]
    meta = result["meta"]

    icon = {"ok": "PASS", "degraded": "WARN", "failed": "FAIL"}[result["verdict"]]
    lines = [
        "## Zakura mempool load run",
        "",
        f"**Verdict: {icon}**",
        "",
    ]
    if result["reasons"]:
        for reason in result["reasons"]:
            lines.append(f"- {reason}")
        lines.append("")
    lines += [
        f"`{meta.get('sha', 'unknown')}` | {meta.get('node_count')} nodes | "
        f"{meta.get('duration_secs')}s | target {meta.get('tx_rate')} tx/s",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Transactions submitted | {throughput['submitted']} |",
        f"| Confirmed | {throughput['confirmed']} |",
        f"| Seen in mempool | {throughput['mempool_seen']} |",
        f"| Node rejects | {throughput['node_rejects']} |",
        f"| Workload-side failures | {throughput['workload_failures']} "
        f"(anchor races, not graded) |",
        f"| Reject rate | {fmt_pct(throughput['reject_rate'])} |",
        f"| Effective throughput | {fmt_num(result['effective_tx_per_sec'])} tx/s |",
        f"| Confirm delay p50 / p95 | {fmt_num(throughput['confirm_delay_p50_ms'])} / "
        f"{fmt_num(throughput['confirm_delay_p95_ms'])} ms |",
        f"| Distinct txids observed | {propagation['txids_observed']} |",
        f"| Reached all nodes | {propagation['txids_on_all_nodes']} |",
        f"| Propagation spread p50 / p95 | {fmt_num(propagation['spread_p50_secs'])} / "
        f"{fmt_num(propagation['spread_p95_secs'])} s |",
        f"| Propagation resolution | {fmt_num(propagation.get('resolution_secs'))} s "
        f"(sampling floor) |",
        f"| Peak mempool depth | {result['peak_mempool_txs']} |",
        f"| Tip spread at end | {fmt_num(convergence['spread'])} |",
        "",
    ]
    if result["backpressure"]:
        lines += ["### Backpressure counters (final)", "", "| Counter | Value |", "| --- | --- |"]
        for key, value in sorted(result["backpressure"].items()):
            lines.append(f"| `{key}` | {fmt_num(value)} |")
        lines.append("")
    return "\n".join(lines)


def fmt_num(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3g}"
    return str(value)


def fmt_pct(value) -> str:
    return "n/a" if value is None else f"{value:.2%}"


# ---------------------------------------------------------------------------- #
# Main
# ---------------------------------------------------------------------------- #


def build_nodes(args) -> list[dict]:
    nodes = []
    for i in range(args.node_count):
        ip = f"127.0.0.{NODE_IP_BASE + i}"
        nodes.append(
            {
                "name": f"miner-{i}",
                "rpc_url": f"http://{ip}:{args.rpc_port}",
                "metrics_url": f"http://{ip}:{args.metrics_port}/metrics",
                "log": Path(args.lab_dir) / "nodes" / f"miner-{i}" / "run.log",
            }
        )
    return nodes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab-dir", default="/root/mempool-lab")
    parser.add_argument("--node-count", type=int, default=4)
    parser.add_argument("--rpc-port", type=int, default=18232)
    parser.add_argument("--metrics-port", type=int, default=19999)
    parser.add_argument("--duration-secs", type=int, default=300)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--out", default="/root/out")
    parser.add_argument("--meta", default="", help="comma-separated key=value pairs")
    parser.add_argument("--max-reject-rate", type=float, default=0.05)
    parser.add_argument("--min-txids-observed", type=int, default=10)
    parser.add_argument(
        "--min-graded-submissions",
        type=int,
        default=50,
        help="below this many submissions the reject rate is reported but not graded",
    )
    args = parser.parse_args()

    lab = Path(args.lab_dir)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    nodes = build_nodes(args)

    samples: list[dict] = []
    first_seen: dict[str, dict[str, float]] = {}
    peak_mempool = 0

    start = time.monotonic()
    deadline = start + args.duration_secs
    while time.monotonic() < deadline:
        for node in nodes:
            # Timestamped per node, not once per round: nodes are sampled
            # sequentially, so a shared round timestamp quantizes every
            # propagation spread to --interval and reports 0 for anything that
            # propagates faster than one full round.
            elapsed = time.monotonic() - start
            sample = sample_node(node, elapsed)
            record_first_seen(first_seen, sample, elapsed)
            txids = sample.get("mempool_txids") or []
            peak_mempool = max(peak_mempool, len(txids))
            # The txid list is the bulky part and is already folded into
            # first_seen; keep only its size in the retained sample.
            sample["mempool_size"] = len(txids)
            del sample["mempool_txids"]
            samples.append(sample)
        time.sleep(args.interval)

    meta = dict(
        pair.split("=", 1) for pair in args.meta.split(",") if "=" in pair
    )
    meta.update(
        {
            "node_count": args.node_count,
            "duration_secs": args.duration_secs,
        }
    )

    throughput = read_txblast_traces(lab / "traces")
    result = {
        "meta": meta,
        "throughput": throughput,
        "propagation": {
            **propagation_stats(first_seen, args.node_count),
            "resolution_secs": sampling_resolution(samples),
        },
        "convergence": tip_convergence(samples, nodes),
        "backpressure": final_backpressure(samples),
        "peak_mempool_txs": peak_mempool,
        "panics": scan_logs_for_panics([n["log"] for n in nodes]),
        "effective_tx_per_sec": round(throughput["submitted"] / args.duration_secs, 3)
        if args.duration_secs
        else None,
    }
    verdict, reasons = grade_run(result, args)
    result["verdict"] = verdict
    result["reasons"] = reasons

    (out_dir / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    (out_dir / "summary.md").write_text(render_markdown(result))
    (out_dir / "samples.jsonl").write_text(
        "".join(json.dumps(s) + "\n" for s in samples)
    )
    print(render_markdown(result))
    return 1 if verdict == "failed" else 0


def final_backpressure(samples: list[dict]) -> dict[str, float]:
    """Sum the last metrics reading from each node."""
    last_per_node: dict[str, dict[str, float]] = {}
    for sample in samples:
        if sample.get("metrics"):
            last_per_node[sample["node"]] = sample["metrics"]
    totals: dict[str, float] = {}
    for metrics in last_per_node.values():
        for key, value in metrics.items():
            totals[key] = totals.get(key, 0.0) + value
    return totals


if __name__ == "__main__":
    sys.exit(main())
