#!/usr/bin/env python3
"""Generate metrics-aware SVG plots from Zebra Zakura trace directories."""

from __future__ import annotations

import argparse
import bisect
import csv
import html
import json
import math
from collections import Counter
from pathlib import Path


def number(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def nice_max(values: list[float]) -> float:
    values = [value for value in values if math.isfinite(value)]
    maximum = max(values) if values else 1
    if maximum <= 0:
        return 1
    exponent = math.floor(math.log10(maximum))
    base = 10**exponent
    for multiplier in (1, 2, 5, 10):
        if maximum <= multiplier * base:
            return multiplier * base
    return 10 * base


def infer_label(trace_dir: Path) -> str:
    name = trace_dir.name
    if name.startswith("feedrun-") and name.endswith("-traces"):
        return name.removeprefix("feedrun-").removesuffix("-traces")
    return name.removesuffix("-traces")


def find_csv(trace_dir: Path, label: str, explicit: str | None) -> Path | None:
    if explicit:
        path = Path(explicit)
        return path if path.exists() else None

    candidates = [
        trace_dir.parent / f"feedrun-{label}.csv",
        Path("/root/wal-bench") / f"feedrun-{label}.csv",
        Path.cwd() / f"feedrun-{label}.csv",
    ]
    return next((candidate for candidate in candidates if candidate.exists()), None)


def load_csv_throughput(csv_path: Path | None) -> tuple[list[tuple[float, float, float]], list[dict[str, str]]]:
    if csv_path is None:
        return [], []

    rows = list(csv.DictReader(csv_path.open()))
    throughput: list[tuple[float, float, float]] = []
    previous: tuple[float, float] | None = None

    for row in rows:
        elapsed = number(row.get("elapsed"))
        height = number(row.get("height"))
        if height <= 0:
            previous = None
            continue

        if previous is not None:
            dt = elapsed - previous[0]
            dh = height - previous[1]
            if dt > 0 and dh >= 0:
                throughput.append((elapsed, height, dh / dt))

        previous = (elapsed, height)

    return throughput, rows


def load_trace_states(trace_dir: Path) -> list[tuple[float, float, float, float, float, float, float, str]]:
    path = trace_dir / "block_sync.jsonl"
    if not path.exists():
        raise SystemExit(f"missing block_sync.jsonl in {trace_dir}")

    states: list[tuple[float, float, float, float, float, float, float, str]] = []
    first_ts: float | None = None

    with path.open() as trace:
        for line in trace:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            if row.get("event") != "block_sync_state":
                continue

            ts = number(row.get("ts")) / 1_000_000.0
            if first_ts is None:
                first_ts = ts

            elapsed = ts - first_ts
            height = number(row.get("verified_block_tip"))
            if height <= 0:
                continue

            applying = number(row.get("applying"))
            reorder = number(row.get("reorder"))
            # Floor HoL: verifier/apply has almost nothing, but later bodies are buffered.
            hol_stall = 1.0 if applying <= 10 and reorder >= 100 else 0.0
            applying_bytes = number(row.get("applying_buffered_bytes"))
            retained_bytes = number(row.get("retained_pipeline_wire_bytes"))
            floor_state = str(row.get("floor_gap_state") or "")
            states.append((elapsed, height, applying, reorder, hol_stall, applying_bytes, retained_bytes, floor_state))

    return states


def downsample(points: list[tuple], max_points: int = 1400) -> list[tuple]:
    if len(points) <= max_points:
        return points

    bucket_size = max(1, len(points) // max_points)
    sampled = []
    for offset in range(0, len(points), bucket_size):
        bucket = points[offset : offset + bucket_size]
        sampled.append(bucket[0])
        peak_reorder = max(bucket, key=lambda point: point[3])
        if peak_reorder is not bucket[0]:
            sampled.append(peak_reorder)
    return sampled


def throughput_at_time(throughput: list[tuple[float, float, float]], elapsed: float) -> float:
    if not throughput:
        return 0.0

    times = [row[0] for row in throughput]
    index = bisect.bisect_right(times, elapsed) - 1
    index = max(0, index)
    return throughput[index][2]


def make_svg(points: list[tuple], x_get, xlabel: str, title: str, out_path: Path) -> None:
    if not points:
        raise SystemExit("no points to plot")

    xs = [x_get(point) for point in points]
    x_min = min(xs)
    x_max = max(xs)
    if x_max <= x_min:
        x_max = x_min + 1

    series = [
        ("Applying", [point[1] for point in points], "#1f77b4"),
        ("Reorder", [point[2] for point in points], "#ff7f0e"),
        ("Stalls", [point[3] for point in points], "#9467bd"),
        ("Blocks/s", [point[4] for point in points], "#2ca02c"),
    ]

    width, height = 1450, 980
    left, right, top, bottom = 105, 35, 65, 65
    gap = 30
    panel_height = (height - top - bottom - gap * (len(series) - 1)) / len(series)
    plot_width = width - left - right

    def sx(value: float) -> float:
        return left + (value - x_min) / (x_max - x_min) * plot_width

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        "<style>text{font-family:Arial,sans-serif;font-size:13px;fill:#222}.title{font-size:20px;font-weight:bold}.label{font-size:14px;font-weight:bold}.tick{fill:#555;font-size:11px}.grid{stroke:#ddd;stroke-width:1}.axis{stroke:#444;stroke-width:1}.line{fill:none;stroke-width:2}</style>",
        f'<text x="{left}" y="32" class="title">{html.escape(title)}</text>',
        f'<text x="{left}" y="52" class="tick">points={len(points)} x range {x_min:.0f}-{x_max:.0f}</text>',
    ]

    xticks = [x_min + (x_max - x_min) * index / 5 for index in range(6)]

    for index, (name, values, color) in enumerate(series):
        y0 = top + index * (panel_height + gap)
        ymax = nice_max(values)
        parts.append(f'<rect x="{left}" y="{y0:.1f}" width="{plot_width}" height="{panel_height:.1f}" fill="#fafafa" stroke="#ddd"/>')
        parts.append(f'<text x="12" y="{y0 + 20:.1f}" class="label">{html.escape(name)}</text>')

        for tick_index in range(5):
            value = ymax * tick_index / 4
            y = y0 + panel_height - (value / ymax) * panel_height
            label = f"{value:.0f}" if ymax >= 10 else f"{value:.2g}"
            parts.append(f'<line x1="{left}" x2="{left + plot_width}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>')
            parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end" class="tick">{label}</text>')

        for xtick in xticks:
            x = sx(xtick)
            parts.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{y0:.1f}" y2="{y0 + panel_height:.1f}" class="grid"/>')

        if name == "Stalls":
            for point, value in zip(points, values):
                if value >= 0.5:
                    x = sx(x_get(point))
                    parts.append(f'<rect x="{x - 1.3:.1f}" y="{y0:.1f}" width="2.6" height="{panel_height:.1f}" fill="{color}" opacity="0.24"/>')

        coords = []
        for point, value in zip(points, values):
            x = sx(x_get(point))
            y = y0 + panel_height - (value / ymax) * panel_height
            coords.append(f"{x:.1f},{y:.1f}")
        parts.append(f'<path d="M {" L ".join(coords)}" class="line" stroke="{color}"/>')
        parts.append(f'<line x1="{left}" x2="{left + plot_width}" y1="{y0 + panel_height:.1f}" y2="{y0 + panel_height:.1f}" class="axis"/>')
        parts.append(f'<line x1="{left}" x2="{left}" y1="{y0:.1f}" y2="{y0 + panel_height:.1f}" class="axis"/>')

    last_y = top + (len(series) - 1) * (panel_height + gap) + panel_height
    for xtick in xticks:
        parts.append(f'<text x="{sx(xtick):.1f}" y="{last_y + 24:.1f}" text-anchor="middle" class="tick">{xtick:.0f}</text>')
    parts.append(f'<text x="{left + plot_width / 2:.1f}" y="{height - 18}" text-anchor="middle" class="label">{html.escape(xlabel)}</text>')
    parts.append("</svg>")

    out_path.write_text("\n".join(parts))


def write_summary(
    out_path: Path,
    trace_dir: Path,
    csv_path: Path | None,
    states: list[tuple[float, float, float, float, float, float, float, str]],
    csv_rows: list[dict[str, str]],
) -> None:
    floor_states = Counter(state[7] for state in states if state[7])
    hol_samples = sum(1 for state in states if state[4] >= 0.5)
    max_reorder = max((state[3] for state in states), default=0)
    max_applying = max((state[2] for state in states), default=0)
    max_retained_gb = max((state[6] for state in states), default=0) / 1e9

    lines = [
        f"trace_dir: {trace_dir}",
        f"csv: {csv_path or '(not found)'}",
        f"trace_samples: {len(states)}",
        f"height_range: {min((s[1] for s in states), default=0):.0f}-{max((s[1] for s in states), default=0):.0f}",
        f"max_applying: {max_applying:.0f}",
        f"max_reorder: {max_reorder:.0f}",
        f"hol_stall_samples: {hol_samples}",
        f"max_retained_pipeline_wire_gb: {max_retained_gb:.2f}",
        f"floor_gap_states: {floor_states.most_common(8)}",
    ]

    if csv_rows:
        nonzero = [row for row in csv_rows if number(row.get("height")) > 0]
        if nonzero:
            lines.append(f"csv_last_height: {number(nonzero[-1].get('height')):.0f}")
            lines.append(f"csv_last_blk_s: {number(nonzero[-1].get('blk_s')):.1f}")

    out_path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace_dir", help="Directory containing block_sync.jsonl and related Zakura traces")
    parser.add_argument("--csv", help="Optional feed_run CSV. Auto-detected when omitted.")
    parser.add_argument("--out-dir", default="perf-artifacts", help="Directory for generated artifacts")
    parser.add_argument("--prefix", help="Output filename prefix. Defaults to the trace label.")
    args = parser.parse_args()

    trace_dir = Path(args.trace_dir).expanduser().resolve()
    label = infer_label(trace_dir)
    csv_path = find_csv(trace_dir, label, args.csv)
    output_dir = Path(args.out_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = args.prefix or label

    throughput, csv_rows = load_csv_throughput(csv_path)
    states = load_trace_states(trace_dir)
    sampled = downsample(states)
    points = [
        (elapsed, applying, reorder, stall, throughput_at_time(throughput, elapsed), height)
        for elapsed, height, applying, reorder, stall, _applying_bytes, _retained_bytes, _floor_state in sampled
    ]
    height_points = sorted(points, key=lambda point: (point[5], point[0]))

    height_svg = output_dir / f"{prefix}-height-apply-reorder-stalls-bps.svg"
    time_svg = output_dir / f"{prefix}-time-apply-reorder-stalls-bps.svg"
    summary = output_dir / f"{prefix}-summary.txt"

    make_svg(height_points, lambda point: point[5], "Verified/finalized height", "", height_svg)
    make_svg(points, lambda point: point[0], "Elapsed seconds", "", time_svg)
    write_summary(summary, trace_dir, csv_path, states, csv_rows)

    print(height_svg)
    print(time_svg)
    print(summary)


if __name__ == "__main__":
    main()
