#!/usr/bin/env python3
"""Compare two mempool-load runs and render a regression table.

Takes the summary.json from a baseline leg and a target leg (same droplet, same
workload, run back to back) and reports the deltas that matter for a mempool
change: throughput, propagation latency, rejects, and backpressure.

Stdlib only, mirroring deploy/deployer/deploy.py.

Exit status is always 0: this script reports, it does not gate. The per-leg
verdict from mempool-load-monitor.py is what fails a run.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# (label, json path, lower_is_better). Percent deltas are only meaningful for
# ratio-scale quantities, which is every metric here.
# (label, json path, lower_is_better, how to read it)
COMPARISONS = (
    ("Transactions submitted", ("throughput", "submitted"), False,
     "Proving-bound, so treat small moves as noise."),
    ("Effective throughput (tx/s)", ("effective_tx_per_sec",), False,
     "Same figure per second. A real drop shows up here and above together."),
    ("Reject rate", ("throughput", "reject_rate"), True,
     "Node rejects only. 0 -> non-zero is the most actionable regression here."),
    # Confirm delay is bounded by when the next block lands, so its percentiles
    # swing hundreds of percent between identical runs. Reported for context,
    # never graded -- otherwise it flags a regression on unchanged code.
    ("Confirm delay p50 (ms)", ("throughput", "confirm_delay_p50_ms"), None,
     "Bounded by block timing; informational, not graded (swings with mining luck)."),
    ("Confirm delay p95 (ms)", ("throughput", "confirm_delay_p95_ms"), None,
     "Bounded by block timing; informational, not graded."),
    ("Propagation p50 (s)", ("propagation", "spread_p50_secs"), True,
     "Typical gossip spread across nodes."),
    ("Propagation p95 (s)", ("propagation", "spread_p95_secs"), True,
     "Tail gossip spread. **The number to watch for advertisement/retry changes.**"),
    ("Txids reaching all nodes", ("propagation", "txids_on_all_nodes"), False,
     "Full-network reach. A drop means transactions are not getting everywhere."),
    ("Peak mempool depth", ("peak_mempool_txs",), False,
     "Higher can mean slower draining, or simply more submitted; read with the rows above."),
)

# Below this, run-to-run noise dominates and a delta is not worth flagging.
NOISE_FLOOR_PCT = 10.0


def dig(data: dict, path: tuple[str, ...]):
    for key in path:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def pct_delta(baseline, target) -> float | None:
    """Percent change from baseline to target, or None if not computable.

    None also covers a zero baseline, where the change is unbounded; callers
    must not read None as "no change" -- see classify().
    """
    if not isinstance(baseline, (int, float)) or not isinstance(target, (int, float)):
        return None
    if baseline == 0:
        return None
    return (target - baseline) / abs(baseline) * 100.0


def classify(delta_pct: float | None, lower_is_better: bool, baseline=None, target=None) -> str:
    """Grade a delta, handling the unbounded zero-baseline case explicitly.

    Several graded metrics are zero on a healthy baseline (reject rate, submit
    failures). Treating their None delta as "=" would hide exactly the
    regression that matters most: 0 -> nonzero.
    """
    # lower_is_better is None for metrics that are reported but never graded.
    if lower_is_better is None:
        return "info"
    if delta_pct is None:
        if not isinstance(baseline, (int, float)) or not isinstance(target, (int, float)):
            return "="
        if baseline == 0 and target != 0:
            # Moving off a clean baseline in the bad direction.
            return "WORSE" if lower_is_better else "better"
        return "="
    if abs(delta_pct) < NOISE_FLOOR_PCT:
        return "="
    improved = (delta_pct < 0) if lower_is_better else (delta_pct > 0)
    return "better" if improved else "WORSE"


def fmt(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def fmt_delta(delta_pct: float | None) -> str:
    return "n/a" if delta_pct is None else f"{delta_pct:+.1f}%"


def build_rows(baseline: dict, target: dict) -> list[dict]:
    rows = []
    for label, path, lower_is_better, hint in COMPARISONS:
        base_value = dig(baseline, path)
        target_value = dig(target, path)
        delta = pct_delta(base_value, target_value)
        rows.append(
            {
                "metric": label,
                "baseline": base_value,
                "target": target_value,
                "delta_pct": delta,
                "verdict": classify(delta, lower_is_better, base_value, target_value),
                "hint": hint,
            }
        )
    return rows


def render(baseline: dict, target: dict, rows: list[dict]) -> str:
    regressions = [r for r in rows if r["verdict"] == "WORSE"]
    base_meta = baseline.get("meta", {})
    target_meta = target.get("meta", {})

    lines = [
        "## Zakura mempool load: baseline vs target",
        "",
        f"baseline `{base_meta.get('sha', 'unknown')}` "
        f"({baseline.get('verdict', 'unknown')}) "
        f"vs target `{target_meta.get('sha', 'unknown')}` "
        f"({target.get('verdict', 'unknown')})",
        "",
        f"{target_meta.get('node_count', '?')} nodes | "
        f"{target_meta.get('duration_secs', '?')}s per leg | "
        f"target {target_meta.get('tx_rate', '?')} tx/s | "
        f"deltas under {NOISE_FLOOR_PCT:.0f}% treated as noise",
        "",
        "| Metric | Baseline | Target | Delta | | How to read it |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['metric']} | {fmt(row['baseline'])} | {fmt(row['target'])} | "
            f"{fmt_delta(row['delta_pct'])} | {row['verdict']} | {row.get('hint', '')} |"
        )
    lines.append("")
    if regressions:
        lines.append(
            f"**{len(regressions)} metric(s) regressed beyond the noise floor:** "
            + ", ".join(r["metric"] for r in regressions)
        )
    else:
        lines.append("No metric regressed beyond the noise floor.")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    try:
        baseline = json.loads(Path(args.baseline).read_text())
        target = json.loads(Path(args.target).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        # A missing leg is not a comparison failure to hard-stop the run on;
        # the per-leg verdict already covers it.
        Path(args.out).write_text(
            f"## Zakura mempool load\n\nComparison unavailable: {exc}\n"
        )
        print(f"comparison unavailable: {exc}", file=sys.stderr)
        return 0

    rows = build_rows(baseline, target)
    report = render(baseline, target, rows)
    Path(args.out).write_text(report)
    Path(args.out).with_suffix(".json").write_text(json.dumps(rows, indent=2) + "\n")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
