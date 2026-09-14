"""Render retained sync reports as Slack-sized PNGs, without contacting services."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import median

from sync_report import boundaries, comparison_key, finite, rates, region_durations, unpack

COLORS = {
    "Sprout": "#c8d8e5", "Sapling": "#78bea1", "Sandblast": "#efbb67",
    "Post-Sandblast": "#a2b8ec", "Ironwood": "#bd9ad4",
    "Startup / unobserved": "#e2e5e9", "Unobserved": "#e2e5e9",
    "Readiness / stop": "#b8bfc9",
}
QUEUES = [("apply_ready", "Ready to submit", "#2367a0"),
          ("apply_submitted", "Submitted", "#bd5170"),
          ("reorder", "Behind a gap", "#d08d21")]
LEGACY_QUEUES = [("legacy_waiting_verifier", "Waiting for verifier", "#2367a0"),
                 ("legacy_verifying", "Verifying / committing", "#bd5170")]


def queue_series(mode: str) -> list:
    return LEGACY_QUEUES if mode == "legacy" else QUEUES


def scales(reports: list[dict]) -> dict:
    """Use shared limits for every mode and page in one daily snapshot."""
    limits = {"rate": 1, "queue": 1, "height": .000001, "time": 1 / 3600, "duration": 1 / 3600}
    for report in reports:
        for row in rates(report):
            for key in ("download", "commit"):
                if finite(row[key]):
                    limits["rate"] = max(limits["rate"], row[key])
        for row in unpack(report):
            for key, _, _ in QUEUES + LEGACY_QUEUES:
                if finite(row[key]):
                    limits["queue"] = max(limits["queue"], row[key])
            if finite(row["height"]):
                limits["height"] = max(limits["height"], row["height"] / 1e6)
            limits["time"] = max(limits["time"], row["t"] / 3600)
        duration = report.get("metadata", {}).get("duration")
        if finite(duration):
            limits["duration"] = max(limits["duration"], duration / 3600)
    return limits


def baseline_record(report: dict, settings: dict) -> dict:
    """Retain small comparison summaries, never a fleet of full traces in memory."""
    metadata = report.get("metadata", {})
    rows = unpack(report)
    regions = boundaries(report, settings)
    times = region_durations(report, settings)
    heights = [row["height"] for row in rows if finite(row["height"])]
    covered = {}
    if heights and metadata.get("phase") == "complete" and not metadata.get("collection_error"):
        maximum_gap = max(90, 3 * metadata.get("interval", 10))
        previous_heights, next_heights = [], []
        value = 0
        for row in rows:
            value = row["height"] if finite(row["height"]) else value
            previous_heights.append(value)
        value = math.inf
        for row in reversed(rows):
            value = row["height"] if finite(row["height"]) else value
            next_heights.append(value)
        next_heights.reverse()
        bad_ranges = []
        for index, (left, right) in enumerate(zip(rows, rows[1:])):
            if (not finite(left["height"]) or not finite(right["height"])
                    or right["height"] < left["height"] or right["t"] - left["t"] > maximum_gap):
                bad_ranges.append(sorted((previous_heights[index], next_heights[index + 1])))
        for (start, name), (end, _) in zip(regions, regions[1:]):
            if (heights[0] <= start and heights[-1] >= end and name in times
                    and not any(low < end and high >= start for low, high in bad_ranges)):
                covered[name] = times[name]
    return {"identity": comparison_key(report), "started_at": metadata.get("started_at", 0),
            "run_id": metadata.get("run_id"), "regions": [list(region) for region in regions], "times": covered}


def baseline(report: dict, history: list[dict], settings: dict) -> str:
    """Compare only fully observed, fixed height regions on the same host/config."""
    current = baseline_record(report, settings)
    if current["identity"] is None:
        return "No matched baseline yet"
    candidates = [other for other in history if other.get("identity") == current["identity"]
                  and other.get("started_at", 0) < current["started_at"]
                  and other.get("regions") == current["regions"]]
    candidates.sort(key=lambda other: other["started_at"], reverse=True)
    deltas = []
    for name, seconds in current["times"].items():
        values = [other["times"][name] for other in candidates[:20] if name in other["times"]]
        if len(values) >= 3 and median(values) > 0:
            delta = 100 * (seconds / median(values) - 1)
            deltas.append((abs(delta), f"{name} {delta:+.0f}% vs median ({len(values)} runs)"))
    return max(deltas)[1] if deltas else "No matched baseline yet (needs 3 runs)"


def _bands(axis, report, settings, view):
    regions = boundaries(report, settings)
    if view == "height":
        upper = axis.get_xlim()[1] * 1e6
        for index, (start, name) in enumerate(regions):
            end = regions[index + 1][0] if index + 1 < len(regions) else upper
            if start < upper:
                axis.axvspan(start / 1e6, min(end, upper) / 1e6, color=COLORS[name], alpha=.22, lw=0)
    else:
        # Shade contiguous observations together, leaving long scrape gaps blank.
        samples = unpack(report)
        spans = []
        maximum_gap = max(90, 3 * report.get("metadata", {}).get("interval", 10))
        for left, right in zip(samples, samples[1:]):
            if not regions or not finite(left["height"]) or right["t"] - left["t"] > maximum_gap:
                continue
            name = next(name for height, name in reversed(regions) if height <= left["height"])
            if spans and spans[-1][2] == name and spans[-1][1] == left["t"]:
                spans[-1] = (spans[-1][0], right["t"], name)
            else:
                spans.append((left["t"], right["t"], name))
        for start, end, name in spans:
            axis.axvspan(start / 3600, end / 3600, color=COLORS[name], alpha=.22, lw=0)


def render(reports: list[dict], title: str, output: Path, settings: dict,
           *, limits: dict | None = None, history: list[dict] = ()) -> None:
    """Render up to three runs. Missing data is visible and never filled with zero."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    if not 1 <= len(reports) <= 3:
        raise ValueError("render one to three runs per image")
    view = settings.get("chart_axis", "height")
    if view not in ("height", "time"):
        raise ValueError("chart_axis must be height or time")
    limits = limits or scales(reports)
    plt.rcParams.update({"font.size": 10, "font.family": "DejaVu Sans", "axes.spines.top": False,
                         "axes.spines.right": False, "axes.edgecolor": "#c0c7d1",
                         "text.color": "#26364b", "axes.labelcolor": "#26364b"})
    figure = plt.figure(figsize=(max(10, 5 * len(reports)), 9.5), facecolor="white")
    grid = figure.add_gridspec(4, len(reports), height_ratios=[1.15, 1.6, 1.6, .65],
                              left=.085, right=.975, top=.86, bottom=.16, hspace=.75, wspace=.28)
    figure.suptitle(title, x=.055, y=.975, ha="left", fontsize=19, weight="bold")
    figure.legend(handles=[Patch(color=color, label="Sprout (no VCT)" if name == "Sprout" else name) for name, color in COLORS.items()
                           if name != "Startup / unobserved"], loc="upper left",
                  bbox_to_anchor=(.05, .945), ncol=4, frameon=False, fontsize=9)
    duration_axis = figure.add_subplot(grid[0, :])
    run_labels = []
    for index, report in enumerate(reports):
        metadata = report.get("metadata", {})
        mode = metadata.get("mode", report.get("mode"))
        run_id = metadata.get("run_id", report.get("run_id", "unavailable"))
        run_labels.append(f"Run {index + 1}")
        duration = metadata.get("duration")
        values = region_durations(report, settings)
        offset = 0
        for name, seconds in values.items():
            hours = seconds / 3600
            duration_axis.barh(index, hours, left=offset, color=COLORS[name], height=.6)
            if hours > limits["duration"] * .065:
                duration_axis.text(offset + hours / 2, index, f"{seconds / 60:.0f}m",
                                   ha="center", va="center", fontsize=9)
            offset += hours
        detail = f"{duration / 3600:.2f}h" if finite(duration) else report.get("unavailable", "Telemetry unavailable")
        if metadata.get("phase") not in (None, "complete"):
            detail += " • " + metadata["phase"]
        duration_axis.text(offset + limits["duration"] * .015, index, detail, va="center", fontsize=9)

        series = rates(report)
        rows = unpack(report)
        xkey, factor = ("height", 1e6) if view == "height" else ("t", 3600)
        axes = [figure.add_subplot(grid[row, index]) for row in (1, 2, 3)]
        for axis in axes:
            axis.set_xlim(0, limits[view] * 1.02)
            axis.grid(axis="y", color="#d6dce3", alpha=.45, lw=.7)
            axis.set_axisbelow(True)
            _bands(axis, report, settings, view)
        rate_axis, queue_axis, vct_axis = axes
        rate_axis.set_ylim(0, limits["rate"] * 1.08)
        queue_axis.set_ylim(0, limits["queue"] * 1.08)
        if index == 0:
            rate_axis.set_ylabel("Payload MB/s")
            queue_axis.set_ylabel("Blocks")
            vct_axis.set_ylabel("VCT %")
        sha = str(metadata.get("sha") or "unknown")[:9]
        floor = next((row["request_floor_bytes"] for row in rows if finite(row["request_floor_bytes"])), None)
        tuning = f" • min window {floor / 1048576:.2f} MiB" if floor is not None else ""
        rate_axis.set_title(f"Run {index + 1} • {sha}{tuning}\n{run_id}", loc="left", fontsize=9, pad=9)

        def plot(axis, points, key, label, color):
            # Explicit NaNs break lines across missing values and counter resets.
            xs, ys, previous = [], [], None
            for row in points:
                if previous is not None and row["t"] - previous > max(90, 3 * metadata.get("interval", 10)):
                    xs.append(math.nan)
                    ys.append(math.nan)
                xs.append(row[xkey] / factor if finite(row[xkey]) else math.nan)
                ys.append(row[key] if finite(row[key]) else math.nan)
                previous = row["t"]
            axis.plot(xs, ys, label=label, color=color, lw=1.1)

        for key, label, color in (("download", "Download", "#2367a0"), ("commit", "Commit", "#bf5771")):
            plot(rate_axis, series, key, label, color)
        for key, label, color in queue_series(mode):
            plot(queue_axis, rows, key, label, color)
        for axis in (rate_axis, queue_axis):
            axis.legend(loc="upper right", frameon=False, fontsize=8)
            axis.tick_params(axis="x", labelbottom=False)
        if not any(finite(point["download"]) for point in series):
            rate_axis.text(.5, .5, "Download rate unavailable", transform=rate_axis.transAxes,
                           ha="center", fontsize=10)
        if not any(finite(point["commit"]) for point in series):
            rate_axis.text(.5, .32, "Commit rate unavailable", transform=rate_axis.transAxes,
                           ha="center", fontsize=9)
        if not any(finite(row[key]) for row in rows for key, _, _ in queue_series(mode)):
            queue_axis.text(.5, .5, "Queue telemetry unavailable", transform=queue_axis.transAxes, ha="center")
        vct_axis.set_ylim(-5, 105)
        vct_axis.set_yticks([0, 100])
        if mode == "legacy":
            vct_axis.text(.5, .5, "Legacy • VCT not used", transform=vct_axis.transAxes, ha="center", fontsize=9)
        elif any(finite(row["vct_share"]) for row in series):
            plot(vct_axis, [{**row, "vct_share": row["vct_share"] * 100 if finite(row["vct_share"]) else None}
                            for row in series], "vct_share", "Observed VCT share", "#487c5c")
        else:
            vct_axis.text(.5, .5, "VCT telemetry unavailable", transform=vct_axis.transAxes, ha="center", fontsize=9)
        checkpoint = next((row["checkpoint_height"] for row in rows if finite(row["checkpoint_height"])), None)
        if checkpoint is not None and view == "height":
            for axis in axes:
                if checkpoint / factor <= limits[view]:
                    axis.axvline(checkpoint / factor, ls=":", color="#475569", lw=.9)
        vct_axis.set_xlabel("Committed height (millions)" if view == "height" else "Elapsed hours")
        queue_axis.set_title(baseline(report, list(history), settings), loc="left", fontsize=9, pad=8)

    duration_axis.set_yticks(range(len(reports)), run_labels)
    duration_axis.invert_yaxis()
    duration_axis.set_xlim(0, limits["duration"] * 1.3)
    duration_axis.set_xlabel("Elapsed hours by committed-height region", fontsize=9)
    figure.text(.055, .025,
                "Download and commit count serialized block payloads, 1 MB = 1,000,000 bytes. Missing samples and resets are gaps.\n"
                "Region crossings are sampled estimates. Grey time includes startup, missing coverage and readiness checks.\n"
                f"Sandblast: {settings['sandblast_start']:,}–{settings['sandblast_end']:,} inclusive. "
                "Height view dotted line: checkpoint limit. VCT % measures tree updates using the fast path.", fontsize=9, linespacing=1.5)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        figure.savefig(output, dpi=130, facecolor="white")
    finally:
        plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path, help="JSON list of retained report responses")
    parser.add_argument("output", type=Path)
    parser.add_argument("--axis", choices=("height", "time"), default="height")
    parser.add_argument("--sandblast-start", type=int, default=1707211)
    parser.add_argument("--sandblast-end", type=int, default=2000000)
    args = parser.parse_args()
    render(json.loads(args.reports.read_text()), "Continuous sync • local preview", args.output,
           {"chart_axis": args.axis, "sandblast_start": args.sandblast_start, "sandblast_end": args.sandblast_end})


if __name__ == "__main__":
    main()
