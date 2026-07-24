#!/usr/bin/env python3
"""Digest CPU profiles and block-processing latency for checkpoint-sync bench runs.

Stdlib-only helper used by scripts/checkpoint-sync-bench.sh (see the profiling
section of that script's header and docs/cpu-profiling.md). Every subcommand is
best-effort: missing or partial inputs produce a markdown note instead of an
error, so a digest failure can never fail a bench run.

Subcommands:
  collapse   Fold `perf script` output (stdin) into flamegraph "folded stacks"
             (stdout): one `comm;frame;...;frame count` line per unique stack,
             root-first, with the thread name as the root frame. Equivalent to
             stackcollapse-perf.pl.
  top        Render a folded-stacks file as markdown: CPU share by thread group
             and the hottest functions by self/total sample share.
  diff       Compare two folded-stacks files (baseline vs primary) and render
             the largest per-function self-share changes as markdown.
  latency    Render block-processing latency as markdown (+ optional JSON) from
             a run's Zakura JSONL traces (per-block `commit_start`/`commit_finish`
             rows in commit_state.jsonl) and a final Prometheus /metrics snapshot
             (cumulative per-stage duration histograms).
"""

import argparse
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# collapse
# ---------------------------------------------------------------------------

# `perf script` sample header: "comm pid[/tid] [cpu] time: period event:".
# comm can contain spaces ("rayon 0"), so anchor on the "[cpu]" bracket first
# and fall back to a bracket-less form for stripped-down field sets.
HEADER_WITH_CPU = re.compile(r"^(\S.*?)\s+(\d+)(?:/(\d+))?\s+\[(\d+)\]")
HEADER_NO_CPU = re.compile(r"^(\S.*?)\s+(\d+)(?:/(\d+))?\s+")
# Stack frame: "<hex addr> <symbol> (<dso>)". The dso is stripped via rfind so
# parentheses inside demangled Rust symbols don't confuse the parse.
FRAME = re.compile(r"^\s+([0-9a-fx]+)\s+(.*)$")
RUST_HASH_SUFFIX = re.compile(r"::h[0-9a-f]{16}$")
LLVM_SUFFIX = re.compile(r"\.llvm\.\d+$")


def clean_symbol(raw):
    """Normalize one perf frame symbol for folded-stack aggregation."""
    idx = raw.rfind(" (")
    if idx != -1 and raw.endswith(")"):
        raw = raw[:idx]
    if not raw or raw == "[unknown]":
        return "[unknown]"
    # strip "+0x1a" offsets, LLVM internalization suffixes (".llvm.123..."),
    # and rustc's "::h0123456789abcdef" hash suffixes
    plus = raw.rfind("+0x")
    if plus > 0:
        raw = raw[:plus]
    raw = LLVM_SUFFIX.sub("", raw)
    raw = RUST_HASH_SUFFIX.sub("", raw)
    # ";" is the folded-format frame separator
    return raw.replace(";", ":").strip() or "[unknown]"


def cmd_collapse(args):
    """Fold `perf script` text from stdin into folded stacks on stdout."""
    counts = defaultdict(int)
    comm = None
    frames = []

    def flush():
        nonlocal frames
        if comm is not None and frames:
            # perf prints leaf-first; folded format is root-first
            counts[";".join([comm] + frames[::-1])] += 1
        frames = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line.strip():
            flush()
            continue
        if line.startswith("#"):
            continue
        if not line[0].isspace():
            flush()
            m = HEADER_WITH_CPU.match(line) or HEADER_NO_CPU.match(line)
            comm = m.group(1) if m else "[unknown-comm]"
            continue
        m = FRAME.match(line)
        if m and comm is not None:
            frames.append(clean_symbol(m.group(2)))
    flush()

    out = sys.stdout
    for stack in sorted(counts):
        out.write(f"{stack} {counts[stack]}\n")
    return 0


# ---------------------------------------------------------------------------
# top / diff
# ---------------------------------------------------------------------------


def read_folded(path):
    """Read a folded-stacks file into a list of `(frames, count)` tuples."""
    stacks = []
    for line in Path(path).read_text().splitlines():
        stack, _, count = line.rpartition(" ")
        if not stack or not count.isdigit():
            continue
        stacks.append((stack.split(";"), int(count)))
    return stacks


def thread_group(comm):
    """Collapse per-thread name suffixes ("rayon 7" -> "rayon") into a group."""
    return re.sub(r"[ \-_]?\d+$", "", comm) or comm


def aggregate(stacks):
    """Return `(total, self_by_symbol, total_by_symbol, by_thread_group)`.

    `self` counts samples where the symbol is the leaf; `total` counts samples
    where it appears anywhere in the stack (once per stack, so recursion does
    not double-count). The root frame is the thread comm and is excluded from
    the per-function tables.
    """
    total = 0
    self_by = defaultdict(int)
    total_by = defaultdict(int)
    threads = defaultdict(int)
    for frames, count in stacks:
        total += count
        threads[thread_group(frames[0])] += count
        funcs = frames[1:]
        if not funcs:
            continue
        self_by[funcs[-1]] += count
        for sym in set(funcs):
            total_by[sym] += count
    return total, self_by, total_by, threads


def md_escape(sym):
    """Escape a symbol for use inside a markdown table cell."""
    return "`" + sym.replace("|", "\\|").replace("`", "'") + "`"


def pct(part, whole):
    """Format `part/whole` as a percentage string."""
    return f"{100.0 * part / whole:.1f}%" if whole else "n/a"


def cmd_top(args):
    """Render one folded-stacks file as a markdown CPU digest."""
    out = sys.stdout
    out.write(f"### CPU profile — {args.title}\n\n")
    try:
        stacks = read_folded(args.folded)
    except OSError as error:
        out.write(f"_(no CPU profile: {error})_\n")
        return 0
    total, self_by, total_by, threads = aggregate(stacks)
    if not total:
        out.write("_(no CPU profile: folded stacks file is empty)_\n")
        return 0

    if args.note:
        out.write(f"{args.note}\n\n")
    out.write(f"{total} samples\n\n")

    out.write("| thread group | cpu share |\n|---|---:|\n")
    for name, count in sorted(threads.items(), key=lambda kv: -kv[1])[: args.limit]:
        out.write(f"| `{name}` | {pct(count, total)} |\n")

    out.write("\n| # | self | total | hottest functions (by self time) |\n")
    out.write("|--:|----:|-----:|---|\n")
    ranked = sorted(self_by.items(), key=lambda kv: -kv[1])[: args.limit]
    for rank, (sym, count) in enumerate(ranked, start=1):
        out.write(
            f"| {rank} | {pct(count, total)} | {pct(total_by[sym], total)} "
            f"| {md_escape(sym)} |\n"
        )
    return 0


def self_shares(path):
    """Return `(total_samples, {symbol: self_share})` for a folded file."""
    total, self_by, _, _ = aggregate(read_folded(path))
    if not total:
        return 0, {}
    return total, {sym: count / total for sym, count in self_by.items()}


def cmd_diff(args):
    """Render the largest self-share deltas between two folded files."""
    out = sys.stdout
    out.write(f"### CPU profile diff — {args.title}\n\n")
    try:
        base_total, base = self_shares(args.baseline)
        prim_total, prim = self_shares(args.primary)
    except OSError as error:
        out.write(f"_(no CPU diff: {error})_\n")
        return 0
    if not base_total or not prim_total:
        out.write("_(no CPU diff: one of the folded stacks files is empty)_\n")
        return 0

    out.write(
        f"self-time share per function, primary minus baseline "
        f"({prim_total} vs {base_total} samples); positive = hotter in primary\n\n"
    )
    deltas = [
        (prim.get(sym, 0.0) - base.get(sym, 0.0), sym)
        for sym in set(base) | set(prim)
    ]
    deltas.sort(key=lambda pair: -abs(pair[0]))
    out.write("| Δ self | baseline | primary | function |\n|---:|---:|---:|---|\n")
    for delta, sym in deltas[: args.limit]:
        out.write(
            f"| {100.0 * delta:+.2f}pp | {100.0 * base.get(sym, 0.0):.1f}% "
            f"| {100.0 * prim.get(sym, 0.0):.1f}% | {md_escape(sym)} |\n"
        )
    return 0


# ---------------------------------------------------------------------------
# latency
# ---------------------------------------------------------------------------

# Prometheus duration histograms worth surfacing per pipeline stage, in
# pipeline order. Names are the exporter's dot->underscore renderings of the
# `metrics::histogram!` names on the block path (see zakura-state
# finalized_state.rs, zakura-consensus primitives, sequencer_task.rs).
STAGE_METRICS = [
    ("sync_block_download_duration_seconds", "legacy sync: block download"),
    ("sync_block_verify_duration_seconds", "legacy sync: block verify+commit"),
    ("sync_block_submit_queue_wait_seconds", "sequencer: submit queue wait"),
    ("zakura_consensus_batch_duration_seconds", "verify: batch"),
    ("zakura_state_write_update_trees_duration_seconds", "commit: update note trees"),
    ("zakura_state_write_commitment_check_duration_seconds", "commit: commitment check"),
    ("zakura_state_write_checkpoint_compute_duration_seconds", "commit: checkpoint compute"),
    ("zakura_state_rocksdb_batch_commit_duration_seconds", "commit: rocksdb batch write"),
]

PROM_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)$")


def parse_prometheus(text):
    """Parse Prometheus exposition text into `{(name, labels): value}`.

    `labels` is a sorted tuple of `(key, value)` pairs. Unparsable lines and
    non-numeric values are skipped.
    """
    metrics = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = PROM_LINE.match(line.strip())
        if not m:
            continue
        name, labels_raw, value_raw = m.groups()
        try:
            value = float(value_raw)
        except ValueError:
            continue
        labels = []
        if labels_raw:
            for part in re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', labels_raw):
                labels.append(part)
        metrics[(name, tuple(sorted(labels)))] = value
    return metrics


def stage_rows(metrics):
    """Extract per-stage duration rows from parsed Prometheus metrics.

    Returns a list of dicts with keys: stage, labels, count, and mean_ms
    (from the cumulative `_sum`/`_count` series). The exporter's summary
    quantile lines are deliberately ignored: they are rolling-window values
    that decay toward zero on an idle stage, so they cannot describe the run
    as a whole — per-block percentiles come from the trace instead.
    """
    rows = []
    for base, stage in STAGE_METRICS:
        # group the _sum/_count series by their labels (e.g. verifier="halo2")
        by_labels = defaultdict(dict)
        for (name, labels), value in metrics.items():
            if name == base + "_sum":
                by_labels[labels]["sum"] = value
            elif name == base + "_count":
                by_labels[labels]["count"] = value
        for plain, data in sorted(by_labels.items()):
            count = data.get("count")
            if not count:
                continue
            rows.append(
                {
                    "stage": stage,
                    "labels": ",".join(f"{k}={v}" for k, v in plain),
                    "count": int(count),
                    "mean_ms": 1000.0 * data.get("sum", 0.0) / count,
                }
            )
    return rows


def nearest_rank(sorted_values, quantile):
    """Nearest-rank percentile of an ascending list (empty list -> None)."""
    if not sorted_values:
        return None
    rank = max(1, math.ceil(quantile * len(sorted_values)))
    return sorted_values[rank - 1]


def parse_commit_trace(trace_path):
    """Parse commit_state.jsonl into block, header-range, and stall records.

    The trace table is shared by two drivers: the block-sync driver (per-block
    `block_submit_queued`/`commit_start`/`commit_finish` rows keyed by height,
    with `apply_class` checkpoint|full) and the header-sync driver
    (`commit_header_range` rows with `range_count`, no height). Returns
    `(queued_ts, start_ts, finishes, headers, stalls, non_committed)` where
    `finishes` maps height to `(ts, elapsed_ms, apply_class)` for committed
    blocks and `headers` lists `(elapsed_ms, range_count)` per committed range.
    """
    queued_ts, start_ts, finishes = {}, {}, {}
    headers = []
    stalls = defaultdict(int)
    non_committed = defaultdict(int)
    with open(trace_path, encoding="utf-8", errors="replace") as trace:
        for line in trace:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            event = row.get("event")
            if row.get("source") == "header_sync_driver":
                if event == "commit_finish" and row.get("result") == "committed":
                    elapsed = row.get("elapsed_ms")
                    if isinstance(elapsed, (int, float)):
                        headers.append((float(elapsed), int(row.get("range_count") or 0)))
                continue
            height = row.get("height")
            if event == "block_submit_queued" and height is not None:
                queued_ts.setdefault(height, row.get("ts"))
            elif event == "commit_start" and height is not None:
                start_ts.setdefault(height, row.get("ts"))
            elif event == "commit_stalled":
                stalls[row.get("commit_stall_reason", "unknown")] += 1
            elif event == "commit_finish":
                if row.get("result") != "committed":
                    non_committed[row.get("result") or "unknown"] += 1
                    continue
                elapsed = row.get("elapsed_ms")
                if height is None or not isinstance(elapsed, (int, float)):
                    continue
                finishes[height] = (
                    row.get("ts"),
                    float(elapsed),
                    row.get("apply_class") or "unknown",
                )
    return queued_ts, start_ts, finishes, headers, dict(stalls), dict(non_committed)


def latency_stats(values):
    """Distribution stats dict for an ascending list of millisecond values."""
    return {
        "count": len(values),
        "mean_ms": sum(values) / len(values),
        "p50_ms": nearest_rank(values, 0.50),
        "p90_ms": nearest_rank(values, 0.90),
        "p99_ms": nearest_rank(values, 0.99),
        "max_ms": values[-1],
    }


def steady_bps(finish_ts):
    """Blocks/s over the span of finish timestamps (µs); None when unknowable."""
    if len(finish_ts) < 2:
        return None
    span = (max(finish_ts) - min(finish_ts)) / 1e6
    if span <= 0:
        return None
    return (len(finish_ts) - 1) / span


def fmt_ms(value):
    """Format a millisecond value for a markdown cell ("-" when absent)."""
    if value is None:
        return "-"
    return f"{value:,.0f}" if value >= 100 else f"{value:.1f}"


def cmd_latency(args):
    """Render the block-processing latency digest as markdown (+ JSON)."""
    out = sys.stdout
    out.write(f"### Block-processing latency — {args.title}\n\n")
    report = {}

    trace_path = Path(args.traces, "commit_state.jsonl") if args.traces else None
    if trace_path and trace_path.is_file():
        queued_ts, start_ts, finishes, headers, stalls, non_committed = (
            parse_commit_trace(trace_path)
        )
        by_class = defaultdict(list)  # class -> [(height, ts, elapsed_ms)]
        for height, (ts, elapsed, apply_class) in finishes.items():
            by_class[apply_class].append((height, ts, elapsed))
        report["stalls"] = stalls
        report["non_committed_results"] = non_committed

        checkpoint_rows = by_class.get("checkpoint")
        if checkpoint_rows:
            residence = sorted(ms for _, _, ms in checkpoint_rows)
            stats = latency_stats(residence)
            bps = steady_bps([ts for _, ts, _ in checkpoint_rows if ts is not None])
            takeaway = f"**Takeaway:** committed {stats['count']:,} blocks"
            if bps:
                takeaway += (
                    f" at **{bps:.0f} blocks/s** steady"
                    f" (**{1000.0 / bps:.1f} ms/block** marginal cost)"
                )
            takeaway += f". Median pipeline residence is **{fmt_ms(stats['p50_ms'])} ms**"
            if bps:
                takeaway += f" ≈ {stats['p50_ms'] / 1000.0 * bps:.0f} blocks of arrival time"
            takeaway += (
                " — checkpoint-range batching (400-block ranges commit together),"
                " not per-block processing cost.\n\n"
            )
            out.write(takeaway)

            out.write(
                "**Block pipeline residence** (submitted→committed; checkpoint"
                " mode batches ranges, so this is batching + head-of-line, not"
                " processing cost)\n\n"
            )
            out.write(
                "| segment | blocks | mean | p50 | p90 | p99 | max |\n"
                "|---|---:|---:|---:|---:|---:|---:|\n"
            )
            queue_waits = sorted(
                (start_ts[h] - queued_ts[h]) / 1000.0
                for h, _, _ in checkpoint_rows
                if h in queued_ts and h in start_ts
                and start_ts[h] is not None and queued_ts[h] is not None
            )
            # the driver queue is normally negligible; give it a row only when
            # it is a meaningful share of residence, else a one-line note
            queue_stats = latency_stats(queue_waits) if queue_waits else None
            if queue_stats and queue_stats["p50_ms"] >= max(1.0, 0.05 * stats["p50_ms"]):
                out.write(
                    f"| driver queue (queued→submitted) | {queue_stats['count']:,} "
                    f"| {fmt_ms(queue_stats['mean_ms'])} | {fmt_ms(queue_stats['p50_ms'])} "
                    f"| {fmt_ms(queue_stats['p90_ms'])} | {fmt_ms(queue_stats['p99_ms'])} "
                    f"| {fmt_ms(queue_stats['max_ms'])} |\n"
                )
                queue_note = None
            else:
                queue_note = (
                    f"driver queue wait: negligible (p50 {fmt_ms(queue_stats['p50_ms'])} ms)"
                    if queue_stats
                    else None
                )
            out.write(
                f"| residence (submitted→committed) | {stats['count']:,} "
                f"| {fmt_ms(stats['mean_ms'])} | {fmt_ms(stats['p50_ms'])} "
                f"| {fmt_ms(stats['p90_ms'])} | {fmt_ms(stats['p99_ms'])} "
                f"| {fmt_ms(stats['max_ms'])} |\n"
            )
            if queue_note:
                out.write(f"\n{queue_note}\n")
            slowest = sorted(checkpoint_rows, key=lambda r: -r[2])[:5]
            out.write(
                "\nslowest: "
                + ", ".join(f"{h} ({fmt_ms(ms)} ms)" for h, _, ms in slowest)
                + "\n"
            )
            report["checkpoint_residence"] = {
                "stats": stats,
                "steady_bps": bps,
                "queue_wait": queue_stats,
                "slowest": [
                    {"height": h, "elapsed_ms": ms} for h, _, ms in slowest
                ],
            }

        full_rows = by_class.get("full")
        if full_rows:
            values = sorted(ms for _, _, ms in full_rows)
            stats = latency_stats(values)
            bps = steady_bps([ts for _, ts, _ in full_rows if ts is not None])
            out.write("\n**Per-block verify+commit latency** (full class)\n\n")
            if bps:
                out.write(
                    f"**Takeaway:** verified+committed {stats['count']:,} blocks at"
                    f" **{bps:.0f} blocks/s** (**{1000.0 / bps:.1f} ms/block** marginal"
                    " cost); per-block latency below is true verification+commit"
                    " cost (no range batching).\n\n"
                )
            out.write(
                "| blocks | mean | p50 | p90 | p99 | max |\n"
                "|---:|---:|---:|---:|---:|---:|\n"
            )
            out.write(
                f"| {stats['count']:,} | {fmt_ms(stats['mean_ms'])} "
                f"| {fmt_ms(stats['p50_ms'])} | {fmt_ms(stats['p90_ms'])} "
                f"| {fmt_ms(stats['p99_ms'])} | {fmt_ms(stats['max_ms'])} |\n"
            )
            slowest = sorted(full_rows, key=lambda r: -r[2])[:5]
            out.write(
                "\nslowest: "
                + ", ".join(f"{h} ({fmt_ms(ms)} ms)" for h, _, ms in slowest)
                + "\n"
            )
            report["full_per_block"] = {
                "stats": stats,
                "steady_bps": bps,
                "slowest": [
                    {"height": h, "elapsed_ms": ms} for h, _, ms in slowest
                ],
            }
        elif checkpoint_rows:
            out.write(
                "\n**Per-block verify+commit latency** — _not applicable:"
                " checkpoint mode skips per-block verification; run with"
                " `verify_mode: semantic` for true per-block cost percentiles._\n"
            )

        other = {
            cls: rows
            for cls, rows in by_class.items()
            if cls not in ("checkpoint", "full")
        }
        if other:
            counts = ", ".join(f"{cls}: {len(rows)}" for cls, rows in sorted(other.items()))
            out.write(f"\n_(unclassified block commit rows: {counts})_\n")

        if not finishes:
            out.write("_(commit_state.jsonl has no successful commit_finish rows)_\n")

        if headers:
            header_ms = sorted(ms for ms, _ in headers)
            per_range = sum(count for _, count in headers) / len(headers)
            out.write(
                f"\n**Header-range commits** (parallel header pipeline):"
                f" {len(headers):,} ranges × ~{per_range:.0f} headers,"
                f" mean {fmt_ms(sum(header_ms) / len(header_ms))} ms,"
                f" p99 {fmt_ms(nearest_rank(header_ms, 0.99))} ms.\n"
            )
            report["header_ranges"] = {
                "ranges": len(headers),
                "mean_headers_per_range": per_range,
                "mean_ms": sum(header_ms) / len(header_ms),
                "p99_ms": nearest_rank(header_ms, 0.99),
            }

        if stalls:
            breakdown = ", ".join(f"{reason}: {n}" for reason, n in sorted(stalls.items()))
            out.write(f"\n⚠ commit stalls (>30s): {breakdown}\n")
        failures = {
            result: count
            for result, count in non_committed.items()
            if result != "duplicate"
        }
        if failures:
            breakdown = ", ".join(f"{r}: {n}" for r, n in sorted(failures.items()))
            out.write(f"\n⚠ non-committed results: {breakdown}\n")
        if non_committed.get("duplicate"):
            out.write(f"\nduplicate commits: {non_committed['duplicate']}\n")
    else:
        out.write(
            "_(no per-block trace: commit_state.jsonl absent — legacy-stack leg"
            " or tracing disabled)_\n"
        )

    if args.metrics and Path(args.metrics).is_file():
        rows = stage_rows(parse_prometheus(Path(args.metrics).read_text()))
        report["stages"] = rows
        if rows:
            out.write(
                "\n**Pipeline stage timings** (cumulative Prometheus histograms"
                " at run end; per recorded operation, not per block)\n\n"
            )
            out.write("| stage | ops | mean ms |\n|---|---:|---:|\n")
            for row in rows:
                label = row["stage"] + (f" ({row['labels']})" if row["labels"] else "")
                out.write(
                    f"| {label} | {row['count']:,} | {fmt_ms(row['mean_ms'])} |\n"
                )
        else:
            out.write(
                "\n_(no stage histograms in the metrics snapshot — build without"
                " the commit-metrics feature?)_\n"
            )
    else:
        out.write("\n_(no final /metrics snapshot captured)_\n")

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=2) + "\n")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    """Parse the subcommand CLI and dispatch."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("collapse", help="fold `perf script` stdin into folded stacks")

    top = sub.add_parser("top", help="markdown digest of a folded-stacks file")
    top.add_argument("--folded", required=True)
    top.add_argument("--title", default="CPU")
    top.add_argument("--note", default="")
    top.add_argument("--limit", type=int, default=15)

    diff = sub.add_parser("diff", help="markdown diff of two folded-stacks files")
    diff.add_argument("--baseline", required=True)
    diff.add_argument("--primary", required=True)
    diff.add_argument("--title", default="primary vs baseline")
    diff.add_argument("--limit", type=int, default=15)

    latency = sub.add_parser("latency", help="markdown block-latency digest")
    latency.add_argument("--traces", default="", help="dir with commit_state.jsonl")
    latency.add_argument("--metrics", default="", help="final /metrics text snapshot")
    latency.add_argument("--json-out", default="")
    latency.add_argument("--title", default="run")

    args = parser.parse_args()
    handler = {
        "collapse": cmd_collapse,
        "top": cmd_top,
        "diff": cmd_diff,
        "latency": cmd_latency,
    }[args.command]
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
