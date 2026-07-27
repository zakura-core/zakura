#!/usr/bin/env python3
"""Build JSON and Markdown mempool-spam reports from a run JSONL log."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), math.ceil(len(ordered) * pct / 100)))
    return round(ordered[rank - 1], 3)


def load_events(path: Path) -> list[dict]:
    events = []
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
    return events


def build_report(events: list[dict]) -> dict:
    start = next((event for event in events if event.get("event") == "start"), {})
    submitted = [event.copy() for event in events if event.get("event") == "submitted"]
    mined_by_txid = {
        event["txid"]: event
        for event in events
        if event.get("event") == "drain_result" and event.get("txid")
    }
    node_names = list(start.get("nodes", []))
    missed_by_node: Counter[str] = Counter()
    spreads = []
    by_submit: dict[str, Counter[str]] = defaultdict(Counter)
    by_impl: dict[str, Counter[str]] = defaultdict(Counter)

    for tx in submitted:
        drain = mined_by_txid.get(tx["txid"], {})
        tx["mined_height"] = drain.get("mined_height")
        tx["confirmations"] = drain.get("confirmations", 0)
        tx["mined"] = tx["confirmations"] > 0
        missing = tx.get("missing") or [
            name for name in node_names if name not in tx.get("first_seen", {})
        ]
        tx["missing"] = missing
        missed_by_node.update(missing)
        seen_values = list(tx.get("first_seen", {}).values())
        spread = round(max(seen_values) - min(seen_values), 3) if len(seen_values) > 1 else None
        tx["spread_secs"] = spread
        if spread is not None:
            spreads.append(spread)
        for key, value in (
            ("submitted", 1),
            ("seen_all", int(bool(tx.get("seen_all")))),
            ("mined", int(tx["mined"])),
        ):
            by_submit[tx.get("submit", "unknown")][key] += value
            by_impl[tx.get("impl", "unknown")][key] += value

    summary = {
        "submitted": len(submitted),
        "seen_all": sum(bool(tx.get("seen_all")) for tx in submitted),
        "missed": sum(not tx.get("seen_all") for tx in submitted),
        "mined": sum(tx["mined"] for tx in submitted),
        "unconfirmed": sum(not tx["mined"] for tx in submitted),
        "spread_p50_secs": percentile(spreads, 50),
        "spread_p95_secs": percentile(spreads, 95),
        "missed_by_node": dict(sorted(missed_by_node.items())),
    }
    return {
        "environment": start.get("environment"),
        "network": start.get("network"),
        "nodes": node_names,
        "summary": summary,
        "by_submit_node": {name: dict(counts) for name, counts in sorted(by_submit.items())},
        "by_implementation": {name: dict(counts) for name, counts in sorted(by_impl.items())},
        "transactions": submitted,
    }


def markdown(report: dict) -> str:
    summary = report["summary"]
    lines = [
        "# Mempool spam report",
        "",
        f"- Environment: `{report.get('environment') or 'custom'}`",
        f"- Network: `{report.get('network') or 'unknown'}`",
        f"- Submitted: **{summary['submitted']}**",
        f"- Seen on all nodes: **{summary['seen_all']}**",
        f"- Propagation misses: **{summary['missed']}**",
        f"- Mined after drain: **{summary['mined']}**",
        f"- Still unconfirmed: **{summary['unconfirmed']}**",
        f"- First-seen spread p50 / p95: `{summary['spread_p50_secs']}` / `{summary['spread_p95_secs']}` seconds",
        "",
        "## By submit node",
        "",
        "| Node | Submitted | Seen all | Mined |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name, counts in report["by_submit_node"].items():
        lines.append(
            f"| {name} | {counts.get('submitted', 0)} | "
            f"{counts.get('seen_all', 0)} | {counts.get('mined', 0)} |"
        )
    lines.extend(
        [
            "",
            "## By implementation",
            "",
            "| Implementation | Submitted | Seen all | Mined |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for name, counts in report["by_implementation"].items():
        lines.append(
            f"| {name} | {counts.get('submitted', 0)} | "
            f"{counts.get('seen_all', 0)} | {counts.get('mined', 0)} |"
        )
    lines.extend(["", "## Misses by observation node", ""])
    if summary["missed_by_node"]:
        lines.extend(
            f"- `{name}`: {count}" for name, count in summary["missed_by_node"].items()
        )
    else:
        lines.append("None.")
    lines.append("")
    return "\n".join(lines)


def write_reports(events: list[dict], out_dir: Path) -> dict:
    report = build_report(events)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (out_dir / "report.md").write_text(markdown(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jsonl", type=Path)
    parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args()
    out_dir = args.out_dir or args.jsonl.parent
    report = write_reports(load_events(args.jsonl), out_dir)
    print(markdown(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
