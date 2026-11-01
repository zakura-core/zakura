#!/usr/bin/env python3
"""Monitor a running zakurad node and emit a run summary.

Runs ON the ephemeral PR-node droplet (invoked by pr-node-run.sh). Samples the
node's JSON-RPC endpoint, systemd unit state, and memory use on an interval for
a fixed duration, then scans the node log for errors/panics and writes
summary.json (machine) + summary.md (human, posted as the PR comment).

Stdlib only, mirroring deploy/deployer/deploy.py.

Exit status: 0 for an ok/degraded run, 1 for a failed run (service inactive at
the end, a panic, an unexpected restart, RPC never came up, or a required
height boundary was not crossed).
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

# The node gets this long to open the state DB and bind RPC before failing
# samples start counting against it.
RPC_GRACE_SECS = 300


def rpc_call(url: str, method: str):
    payload = json.dumps(
        {"jsonrpc": "1.0", "id": "pr-node-monitor", "method": method, "params": []}
    ).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())["result"]


def metric_value(text: str, name: str) -> float | None:
    values = [
        float(match.group(1))
        for match in re.finditer(
            rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([-+0-9.eE]+)$",
            text,
            re.MULTILINE,
        )
    ]
    return max(values) if values else None


CAPACITY_FIELDS = (
    "capacity", "reserved", "limit", "active", "headroom", "peak", "deferred",
    "limit_reached_count", "capacity_deferred_count", "capacity_reduction_overage_count",
    "near_80_count", "near_90_count", "available_count", "admitted_count",
    "read_failed_count", "inconclusive_count", "expired_count", "overflow_count",
    "retry_limit_count", "retried_count",
)


def capacity_metrics(text: str) -> dict:
    """Absent reservation metrics remain unknown, never zero."""
    return {path: {field: metric_value(text, f"{path}_missing_parent_{field}")
                   for field in CAPACITY_FIELDS} for path in ("sync", "gossip")}


def duration_metrics(text: str) -> dict:
    """Preserve exported duration summaries by bounded path and release reason."""
    result = {}
    for match in re.finditer(
        r'^(sync|gossip)_missing_parent_(charge|deferred)_duration_seconds'
        r'(_sum|_count)?(?:\{([^}]*)\})?\s+([-+0-9.eE]+)$', text, re.MULTILINE
    ):
        path, kind, suffix, labels, value = match.groups()
        labels = dict(re.findall(r'(\w+)="([^"]*)"', labels or ""))
        reason = labels.get("reason", "all")
        field = (suffix or "").lstrip("_") or labels.get("quantile", "value")
        result.setdefault(path, {}).setdefault(kind, {}).setdefault(reason, {})[field] = float(value)
    return result


def counter_window(samples: list[dict], field: str) -> dict:
    """Count work strictly between the first and last observed counter values."""
    values = [s[field] for s in samples if s.get(field) is not None]
    return {"baseline": values[0] if values else None,
            "final": values[-1] if values else None,
            "delta": values[-1] - values[0] if values else 0}


def summarize_capacity(samples: list[dict], interval: float, *, at_tip: bool = False) -> dict:
    """Combine sampled utilization with event counters that capture brief saturation."""
    result = {}
    for path in ("sync", "gossip"):
        starts = [s for s in samples
                  if s.get("missing_parent", {}).get(path, {}).get("capacity") is not None
                  and (s.get("peers") or 0) > 0 and not s.get("rpc_error")
                  and (not at_tip or (s.get("reference_height") is not None and s.get("height") is not None
                                      and abs(s["reference_height"] - s["height"]) <= 2))]
        start = starts[0]["elapsed"] if starts else None
        observations = [s for s in samples if start is not None and s["elapsed"] >= start
                        and s.get("missing_parent", {}).get(path, {}).get("capacity") is not None]
        if not observations:
            result[path] = {"verdict": "incomplete", "reason": "No reservation metrics."}
            continue
        values = [s["missing_parent"][path] for s in observations]
        failures = []
        window = [s for s in samples if s.get("elapsed", 0) >= observations[0]["elapsed"]]
        if len(window) != len(observations):
            failures.append("Metrics missing inside the observation window.")
        gaps = [b["elapsed"] - a["elapsed"] for a, b in zip(observations, observations[1:])]
        if any(gap > max(2.5 * interval, 2.5) for gap in gaps):
            failures.append("The sampling interval was interrupted.")
        if any(v.get(key) is None for v in values for key in CAPACITY_FIELDS):
            failures.append("Reservation metrics are incomplete.")
        if any(v.get("limit") != v["capacity"] - ((int(v["capacity"]) + 3) // 4)
               for v in values):
            failures.append("The measured allowance disagrees with the reservation.")
        if any((s.get("peers") or 0) < 1 or s.get("rpc_error") for s in observations):
            failures.append("RPC or peer connectivity was unavailable.")
        if len({s.get("pid") for s in observations if s.get("pid")}) > 1:
            failures.append("The node process changed.")
        for name in ("limit_reached_count", "capacity_deferred_count", "capacity_reduction_overage_count"):
            counters = [v.get(name) for v in values]
            if any(value is None for value in counters):
                continue
            if any(b < a for a, b in zip(counters, counters[1:])):
                failures.append(f"Counter reset: {name}.")
            if max(counters) > 0:
                failures.append(f"Nonzero {name}: {max(counters):g}.")
        utilization = sorted(v["active"] / v["limit"] for v in values
                             if v.get("active") is not None and (v.get("limit") or 0) > 0)

        def quantile(fraction):
            if not utilization:
                return None
            return utilization[min(len(utilization) - 1, int((len(utilization) - 1) * fraction))]

        result[path] = {
            "verdict": "failed" if failures else "ok", "failures": failures,
            "start_elapsed": observations[0]["elapsed"],
            "end_elapsed": observations[-1]["elapsed"],
            "observed_seconds": observations[-1]["elapsed"] - observations[0]["elapsed"],
            "samples": len(observations),
            "capacities": sorted({v["capacity"] for v in values}),
            "allowances": sorted({v["limit"] for v in values if v.get("limit") is not None}),
            "peak": max((v.get("peak") or 0) for v in values),
            "minimum_headroom": min((v["headroom"] for v in values if v.get("headroom") is not None), default=None),
            "utilization_p50": quantile(.50), "utilization_p95": quantile(.95), "utilization_p99": quantile(.99),
            "sampled_seconds_above_80_percent": sum(gap for gap, value in zip(gaps, values) if value.get("active") is not None and (value.get("limit") or 0) > 0 and value["active"] / value["limit"] >= .8),
            "sampled_seconds_above_90_percent": sum(gap for gap, value in zip(gaps, values) if value.get("active") is not None and (value.get("limit") or 0) > 0 and value["active"] / value["limit"] >= .9),
            "counter_baseline": {key: values[0].get(key) for key in CAPACITY_FIELDS if key.endswith("_count")},
            "counter_delta": {key: values[-1][key] - values[0][key] if values[-1].get(key) is not None and values[0].get(key) is not None else None
                              for key in CAPACITY_FIELDS if key.endswith("_count")},
            "capacity_deferral_rate_per_second": (
                (values[-1]["capacity_deferred_count"] - values[0]["capacity_deferred_count"])
                / (observations[-1]["elapsed"] - observations[0]["elapsed"])
                if len(observations) > 1 and observations[-1]["elapsed"] > observations[0]["elapsed"]
                and values[-1].get("capacity_deferred_count") is not None and values[0].get("capacity_deferred_count") is not None else None),
            "events_since_process_start": {key: values[-1].get(key) for key in CAPACITY_FIELDS if key.endswith("_count")},
        }
    return result


def systemd_props(service: str) -> dict[str, str]:
    out = subprocess.run(
        ["systemctl", "show", service, "--property=ActiveState,NRestarts,MainPID"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def process_props(pid_file: str) -> dict[str, str]:
    """Observe an isolated follower without replacing the host's service unit."""
    try:
        pid = Path(pid_file).read_text().strip()
        if not pid.isdecimal() or int(pid) < 1:
            raise ValueError("invalid process identity")
        status = Path(f"/proc/{pid}/status").read_text()
        active = not re.search(r"^State:\s+Z", status, re.MULTILINE)
        return {"MainPID": pid, "ActiveState": "active" if active else "inactive", "NRestarts": "0"}
    except (OSError, ValueError):
        return {"MainPID": "0", "ActiveState": "inactive", "NRestarts": "0"}


def rss_mib(pid: str) -> float | None:
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return None
    m = re.search(r"^VmRSS:\s+(\d+)\s+kB", status, re.MULTILINE)
    return round(int(m.group(1)) / 1024, 1) if m else None


def take_sample(args) -> dict:
    sample: dict = {
        "elapsed": None,
        "height": None,
        "estimated": None,
        "peers": None,
        "finalized_height": None,
        "vct_fast_blocks": None,
        "vct_committed_blocks": None,
    }
    try:
        info = rpc_call(args.rpc_url, "getblockchaininfo")
        sample["height"] = info.get("blocks")
        sample["estimated"] = info.get("estimatedheight")
    except Exception as exc:  # noqa: BLE001 — any RPC failure is just a missed sample
        sample["rpc_error"] = str(exc)
    try:
        sample["peers"] = len(rpc_call(args.rpc_url, "getpeerinfo"))
    except Exception:  # noqa: BLE001
        pass
    try:
        with urllib.request.urlopen(args.metrics_url, timeout=15) as resp:
            metrics = resp.read().decode("utf-8", "replace")
        # Retain one complete scrape and use it for all counters in this sample.
        (Path(args.out) / "metrics.prom").write_text(metrics)
        with gzip.open(Path(args.out) / "metrics-scrapes.jsonl.gz", "at") as archive:
            archive.write(json.dumps({"time": time.time(), "metrics": metrics}) + "\n")
        sample["missing_parent"] = capacity_metrics(metrics)
        sample["missing_parent_durations"] = duration_metrics(metrics)
        sample["semantic_commits"] = sum(metric_value(metrics, f"{path}_semantic_committed_count") or 0
                                          for path in ("sync", "gossip"))
        sample["body_downloads"] = sum(metric_value(metrics, f"{path}_downloaded_block_count") or 0
                                       for path in ("sync", "gossip"))
        for key, name in (
            ("finalized_height", "state_finalized_block_height"),
            ("vct_fast_blocks", "state_vct_fast_block_count"),
            ("vct_committed_blocks", "state_vct_fast_path_hit"),
        ):
            sample[key] = metric_value(metrics, name)
    except Exception as exc:  # noqa: BLE001 — metrics failure is recorded in the sample
        sample["metrics_error"] = str(exc)
    props = process_props(args.pid_file) if getattr(args, "pid_file", None) else systemd_props(args.service)
    sample["active_state"] = props.get("ActiveState")
    sample["restarts"] = int(props.get("NRestarts") or 0)
    pid = props.get("MainPID", "0")
    sample["pid"] = pid
    sample["rss_mib"] = rss_mib(pid) if pid != "0" else None
    return sample


def missed_vct_handoff(sample: dict, checkpoint: int | None) -> bool:
    """A fresh scrape past C cannot gain a VCT commit later in this process.

    Counters are registered lazily, so an absent hit counter in a successful
    scrape means zero. Callers require two consecutive observations because
    the finalized gauge and post-commit counter are not updated atomically.
    """
    return (
        checkpoint is not None
        and not sample.get("metrics_error")
        and sample.get("finalized_height") is not None
        and sample["finalized_height"] >= checkpoint
        and (sample.get("vct_committed_blocks") or 0) <= 0
    )


def read_startup_height(log_file: str) -> tuple[int | None, str | None]:
    """Read the restored finalized height recorded after startup format upgrades."""
    entry = None
    try:
        with open(log_file, errors="replace") as stream:
            for line in stream:
                if "loaded Zakura state cache" not in line:
                    continue
                if entry is not None:
                    return None, "Multiple startup state entries were recorded."
                entry = line
    except OSError:
        return None, "The startup state log could not be read."
    if entry is None:
        return None, "No startup state entry was recorded."
    height = re.search(r"(?:^|\s)finalized_tip_height=([0-9]+)(?=\s|$)", entry)
    if height is None or entry.count("finalized_tip_height=") != 1:
        return None, "The startup finalized height is missing or malformed."
    # A real block height is a u32; reject impossible or unbounded log values.
    digits = height.group(1)
    if len(digits) > 10 or int(digits) > 2**32 - 1:
        return None, "The startup finalized height is outside the block height range."
    return int(digits), None


def startup_height_failure(
    height: int | None,
    error: str | None,
    expected: int | None,
    checkpoint: int | None,
) -> str | None:
    if error is not None:
        return error
    if height is None:
        return "No startup finalized height was recorded."
    if expected is not None and height != expected:
        return f"Snapshot height {expected} does not match startup finalized height {height}."
    if checkpoint is not None and height >= checkpoint:
        return f"Startup finalized height {height} is not below checkpoint {checkpoint}."
    return None


def scan_logs(log_file: str, service: str | None) -> dict:
    errors = warns = panics = 0
    last_errors: list[str] = []
    try:
        with open(log_file, errors="replace") as fh:
            for line in fh:
                panics += len(re.findall(r"panicked at", line))
                if " ERROR " in line:
                    errors += 1
                    last_errors.append(line.rstrip()[:300])
                    last_errors = last_errors[-5:]
                elif " WARN " in line:
                    warns += 1
    except OSError:
        pass
    # Panics go to stderr (the journal), not the tracing log file.
    journal = subprocess.run(
        ["journalctl", "-u", service, "--no-pager", "-o", "cat"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout if service else ""
    panics += len(re.findall(r"panicked at", journal))
    return {"errors": errors, "warns": warns, "panics": panics, "last_errors": last_errors}


def fmt(value, suffix: str = "") -> str:
    return f"{value}{suffix}" if value is not None else "n/a"


def build_summary(
    meta: dict,
    samples: list[dict],
    logs: dict,
    duration_min: float,
    known_start_height: int | None = None,
    required_start_below: int | None = None,
    stop_after_height: int | None = None,
    required_finalized_at_least: int | None = None,
    require_vct_fast_blocks: bool = False,
    require_startup_height: bool = False,
    expected_snapshot_height: int | None = None,
    startup_height_error: str | None = None,
) -> dict:
    heighted = [s for s in samples if s["height"] is not None]
    first_observed_h = heighted[0]["height"] if heighted else None
    start_h = (
        known_start_height if require_startup_height or known_start_height is not None
        else first_observed_h
    )
    startup_failure = startup_height_failure(
        start_h, startup_height_error, expected_snapshot_height, required_start_below,
    ) if require_startup_height else None
    end_h = heighted[-1]["height"] if heighted else None
    progress = (end_h - start_h) if end_h is not None and start_h is not None else None
    last = samples[-1] if samples else {}
    peers = [s["peers"] for s in samples if s["peers"] is not None]
    rss = [s["rss_mib"] for s in samples if s["rss_mib"] is not None]
    finalized = [
        s["finalized_height"] for s in samples if s.get("finalized_height") is not None
    ]
    vct_fast = [s["vct_fast_blocks"] for s in samples if s.get("vct_fast_blocks") is not None]
    finalized_h = max(finalized) if finalized else None
    vct_fast_blocks = max(vct_fast) if vct_fast else None
    vct_committed = [
        s["vct_committed_blocks"]
        for s in samples if s.get("vct_committed_blocks") is not None
    ]
    vct_committed_blocks = max(vct_committed) if vct_committed else None
    restarts = max((s["restarts"] for s in samples), default=0)

    if (
        last.get("active_state") != "active"
        or logs["panics"] > 0
        or restarts > 0
        or not heighted
        or startup_failure is not None
        or (
            required_start_below is not None
            and (start_h is None or start_h >= required_start_below)
        )
        or (
            stop_after_height is not None
            and (end_h is None or end_h <= stop_after_height)
        )
        or (
            required_finalized_at_least is not None
            and (finalized_h is None or finalized_h < required_finalized_at_least)
        )
        or (
            require_vct_fast_blocks
            and (vct_committed_blocks is None or vct_committed_blocks <= 0)
        )
    ):
        verdict = "failed"
    elif logs["errors"] > 0 or (progress is not None and progress <= 0):
        verdict = "degraded"
    else:
        verdict = "ok"

    return {
        **meta,
        "verdict": verdict,
        "duration_minutes": duration_min,
        "start_height": start_h,
        "first_observed_height": first_observed_h,
        "end_height": end_h,
        "blocks_synced": progress,
        "blocks_per_hour": (
            round(progress / duration_min * 60) if progress is not None and duration_min else None
        ),
        "estimated_tip_at_end": last.get("estimated"),
        "peers_last": peers[-1] if peers else None,
        "peers_min": min(peers) if peers else None,
        "peak_rss_mib": max(rss) if rss else None,
        "service_active_at_end": last.get("active_state"),
        "service_restarts": restarts,
        "log_errors": logs["errors"],
        "log_warns": logs["warns"],
        "panics": logs["panics"],
        "last_errors": logs["last_errors"],
        "required_start_below": required_start_below,
        "required_end_above": stop_after_height,
        "finalized_height": finalized_h,
        "required_finalized_at_least": required_finalized_at_least,
        "vct_fast_blocks": vct_fast_blocks,
        "vct_committed_blocks": vct_committed_blocks,
        "vct_failure_reason": (
            "Checkpoint finalized without a successful VCT commit; handoff was not exercised."
            if require_vct_fast_blocks
            and finalized_h is not None
            and required_finalized_at_least is not None
            and finalized_h >= required_finalized_at_least
            and (vct_committed_blocks or 0) <= 0
            else None
        ),
        "require_vct_fast_blocks": require_vct_fast_blocks,
        "require_startup_height": require_startup_height,
        "expected_snapshot_height": expected_snapshot_height,
        "startup_height_failure_reason": startup_failure,
    }


def write_markdown(out: Path, summary: dict, samples: list[dict], notes_file: str | None):
    icon = {"ok": "✅", "degraded": "⚠️", "failed": "❌"}[summary["verdict"]]
    mode, network = summary.get("mode", "?"), summary.get("network", "?")
    lines = [
        f"## Zakura PR node run — {mode}/{network} {icon}",
        "",
        "| | |",
        "|---|---|",
        f"| Verdict | **{summary['verdict']}** |",
        f"| Commit | `{summary.get('sha', '?')}` |",
        f"| Duration | {summary['duration_minutes']:g} min |",
        f"| Height | {fmt(summary['start_height'])} → {fmt(summary['end_height'])} "
        f"(+{fmt(summary['blocks_synced'])}) |",
        f"| Blocks/hour | {fmt(summary['blocks_per_hour'])} |",
        f"| Estimated tip at end | {fmt(summary['estimated_tip_at_end'])} |",
        f"| Peers (min/last) | {fmt(summary['peers_min'])} / {fmt(summary['peers_last'])} |",
        f"| Peak RSS | {fmt(summary['peak_rss_mib'], ' MiB')} |",
        f"| Service at end | {fmt(summary['service_active_at_end'])}, "
        f"{summary['service_restarts']} restart(s) |",
        f"| Log errors/warns/panics | {summary['log_errors']} / {summary['log_warns']} / "
        f"{summary['panics']} |",
    ]
    if summary["required_start_below"] is not None:
        lines.append(
            f"| Required crossing | start < {summary['required_start_below']}, "
            f"end > {summary['required_end_above']} |"
        )
        lines.append(f"| First observed RPC height | {fmt(summary['first_observed_height'])} |")
        lines.append(
            f"| Finalized height | {fmt(summary['finalized_height'])} "
            f"(required ≥ {summary['required_finalized_at_least']}) |"
        )
        lines.append(
            f"| VCT fast-path blocks | {fmt(summary['vct_fast_blocks'])} "
            "(before commit) |"
        )
        lines.append(
            f"| Successful VCT commits | {fmt(summary['vct_committed_blocks'])} "
            f"(required > 0: {summary['require_vct_fast_blocks']}) |"
        )
    if summary["vct_failure_reason"]:
        lines += ["", summary["vct_failure_reason"]]
    if summary["startup_height_failure_reason"]:
        lines += ["", summary["startup_height_failure_reason"]]

    shown = [s for s in samples if s["height"] is not None]
    if shown:
        # Subsample to ~12 rows so an hour-long run stays readable.
        step = max(1, len(shown) // 12)
        picked = shown[::step]
        if picked[-1] is not shown[-1]:
            picked.append(shown[-1])
        lines += ["", "### Height over time", "", "| Elapsed | Height | Peers | RSS (MiB) |", "|---|---|---|---|"]
        for s in picked:
            lines.append(
                f"| {int(s['elapsed'] // 60)}m | {fmt(s['height'])} | {fmt(s['peers'])} "
                f"| {fmt(s['rss_mib'])} |"
            )

    if summary["last_errors"]:
        lines += ["", "### Last errors", "", "```"]
        lines += summary["last_errors"]
        lines += ["```"]

    if "missing_parent" in summary:
        lines += ["", "### Missing-parent reservation", "",
                  "| Path | Verdict | Peak | Minimum headroom | p99 utilization | Saturation events | Capacity deferrals |",
                  "|---|---|---:|---:|---:|---:|---:|"]
        for path, measured in summary["missing_parent"].items():
            events = measured.get("events_since_process_start", {})
            lines.append(f"| {path} | {measured['verdict']} | {fmt(measured.get('peak'))} | "
                         f"{fmt(measured.get('minimum_headroom'))} | {fmt(measured.get('utilization_p99'))} | "
                         f"{fmt(events.get('limit_reached_count'))} | {fmt(events.get('capacity_deferred_count'))} |")
        for reason in summary.get("qualification_failures", []):
            lines.append(f"- {reason}")

    if notes_file and Path(notes_file).is_file():
        lines += ["", "### Notes", "", Path(notes_file).read_text().rstrip()]

    (out / "summary.md").write_text("\n".join(lines) + "\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-minutes", type=float, required=True)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--require-capacity", action="store_true")
    parser.add_argument("--required-semantic-blocks", type=int, default=0)
    parser.add_argument("--required-observation-seconds", type=float, default=0)
    parser.add_argument("--reference-rpc-url", help="independent healthy node for tip qualification")
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8232")
    parser.add_argument("--metrics-url", default="http://127.0.0.1:9999/metrics")
    parser.add_argument("--service", default="zakurad")
    parser.add_argument("--pid-file", help="observe an isolated foreground follower; log-file must include its stderr")
    parser.add_argument("--log-file", default="/var/log/zakura/zakura.log")
    parser.add_argument("--notes", default=None, help="markdown notes file to append")
    parser.add_argument("--meta", default="", help="comma-separated key=value run metadata")
    starting_height = parser.add_mutually_exclusive_group()
    starting_height.add_argument(
        "--known-start-height",
        type=int,
        default=None,
        help="previously verified state height, used when sync crosses before RPC is available",
    )
    starting_height.add_argument(
        "--require-startup-height", action="store_true",
        help="require the restored finalized height from this run's startup log",
    )
    parser.add_argument("--expected-snapshot-height", type=int, default=None)
    parser.add_argument(
        "--required-start-below",
        type=int,
        default=None,
        help="fail unless the known/first observed start height is below this height",
    )
    parser.add_argument(
        "--stop-after-height",
        type=int,
        default=None,
        help="stop successfully once RPC block height is strictly above this height",
    )
    parser.add_argument("--required-finalized-at-least", type=int, default=None)
    parser.add_argument(
        "--require-vct-fast-blocks", action="store_true",
        help="require successful tree_aux commits, not just pre-commit fast-block attempts",
    )
    parser.add_argument("--out", required=True, help="output directory")
    args = parser.parse_args(argv)
    if args.interval <= 0 or args.duration_minutes <= 0:
        parser.error("duration and sampling interval must be positive")
    if args.required_semantic_blocks < 0 or args.required_observation_seconds < 0:
        parser.error("required coverage cannot be negative")
    if args.expected_snapshot_height is not None and (
        not args.require_startup_height or args.expected_snapshot_height < 0
    ):
        parser.error("--expected-snapshot-height requires --require-startup-height and a nonnegative height")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    meta = dict(kv.split("=", 1) for kv in args.meta.split(",") if "=" in kv)

    start = time.monotonic()
    deadline = start + args.duration_minutes * 60
    samples: list[dict] = []
    missed_handoff_samples = 0
    startup_height_checked = False
    while True:
        sample = take_sample(args)
        if args.reference_rpc_url:
            try:
                sample["reference_height"] = rpc_call(args.reference_rpc_url, "getblockcount")
            except Exception as exc:
                sample["reference_error"] = str(exc)
        sample["elapsed"] = time.monotonic() - start
        samples.append(sample)
        with (out / "samples.jsonl").open("a") as stream:
            stream.write(json.dumps(sample) + "\n")
        status = (
            f"[{int(sample['elapsed'])}s] height={fmt(sample['height'])} "
            f"finalized={fmt(sample['finalized_height'])} "
            f"vct_fast={fmt(sample['vct_fast_blocks'])} "
            f"vct_committed={fmt(sample['vct_committed_blocks'])} "
            f"peers={fmt(sample['peers'])} rss={fmt(sample['rss_mib'])}MiB "
            f"state={sample['active_state']}"
        )
        print(status, flush=True)
        if args.require_capacity and any(
            (path.get("limit_reached_count") or 0) > 0 or (path.get("capacity_deferred_count") or 0) > 0
            for path in sample.get("missing_parent", {}).values()
        ):
            print("normal-traffic capacity acceptance failed; preserving this observation", flush=True)
            break
        if args.require_startup_height and not startup_height_checked:
            startup_height, startup_error = read_startup_height(args.log_file)
            if startup_error is None or sample["elapsed"] > RPC_GRACE_SECS:
                failure = startup_height_failure(
                    startup_height, startup_error, args.expected_snapshot_height,
                    args.required_start_below,
                )
                if failure is not None:
                    print(f"startup height check failed: {failure}", flush=True)
                    break
                startup_height_checked = True
        # Bail out early once the node is clearly gone (past the startup grace).
        if sample["elapsed"] > RPC_GRACE_SECS and sample["active_state"] == "failed":
            print("service failed; stopping monitor early", flush=True)
            break
        if args.require_vct_fast_blocks and missed_vct_handoff(
            sample, args.required_finalized_at_least
        ):
            missed_handoff_samples += 1
        else:
            missed_handoff_samples = 0
        if missed_handoff_samples >= 2:
            print("checkpoint finalized without a VCT commit; stopping monitor", flush=True)
            break
        if (
            args.stop_after_height is not None
            and (not args.require_startup_height or startup_height_checked)
            and sample["height"] is not None
            and sample["height"] > args.stop_after_height
            and (
                args.required_finalized_at_least is None
                or (
                    sample["finalized_height"] is not None
                    and sample["finalized_height"] >= args.required_finalized_at_least
                )
            )
            and (
                not args.require_vct_fast_blocks
                or (
                    sample["vct_committed_blocks"] is not None
                    and sample["vct_committed_blocks"] > 0
                )
            )
        ):
            print(
                f"crossed required height {args.stop_after_height}; stopping monitor",
                flush=True,
            )
            break
        if time.monotonic() + args.interval > deadline:
            break
        time.sleep(args.interval)

    startup_height, startup_error = (
        read_startup_height(args.log_file) if args.require_startup_height
        else (args.known_start_height, None)
    )
    logs = scan_logs(args.log_file, None if args.pid_file else args.service)
    actual_duration_min = (time.monotonic() - start) / 60
    summary = build_summary(
        meta,
        samples,
        logs,
        actual_duration_min,
        known_start_height=startup_height,
        required_start_below=args.required_start_below,
        stop_after_height=args.stop_after_height,
        required_finalized_at_least=args.required_finalized_at_least,
        require_vct_fast_blocks=args.require_vct_fast_blocks,
        require_startup_height=args.require_startup_height,
        expected_snapshot_height=args.expected_snapshot_height,
        startup_height_error=startup_error,
    )
    summary["missing_parent"] = summarize_capacity(samples, args.interval, at_tip=bool(args.required_observation_seconds))
    summary["missing_parent_durations"] = next((s["missing_parent_durations"] for s in reversed(samples)
                                                if s.get("missing_parent_durations")), {})
    window_start = max((path.get("start_elapsed", 0) for path in summary["missing_parent"].values()), default=0)
    measured_samples = [s for s in samples if s["elapsed"] >= window_start]
    summary["work_counter_windows"] = {field: counter_window(measured_samples, field)
                                        for field in ("semantic_commits", "body_downloads")}
    summary["semantic_commits"] = summary["work_counter_windows"]["semantic_commits"]["delta"]
    summary["body_downloads"] = summary["work_counter_windows"]["body_downloads"]["delta"]
    if args.require_capacity:
        reasons = []
        if any(v["verdict"] != "ok" for v in summary["missing_parent"].values()):
            reasons.append("Reservation qualification failed or is incomplete.")
        if summary["semantic_commits"] < args.required_semantic_blocks:
            reasons.append("Insufficient fully semantically verified blocks.")
        if any(v.get("observed_seconds", 0) < args.required_observation_seconds
               for v in summary["missing_parent"].values()):
            reasons.append("Insufficient uninterrupted observation time.")
        if args.required_observation_seconds:
            if not args.reference_rpc_url:
                reasons.append("Tip qualification requires an independent reference.")
            elif any(s.get("reference_error") or s.get("reference_height") is None
                     or s.get("height") is None or abs(s["reference_height"] - s["height"]) > 2
                     for s in measured_samples if s.get("missing_parent")):
                reasons.append("Independent tip coverage is missing or lag exceeded two blocks.")
            if (summary.get("blocks_synced") or 0) < 1:
                reasons.append("The tip run observed no committed chain progress.")
        summary["qualification_failures"] = reasons
        if reasons:
            summary["verdict"] = "failed"
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_markdown(out, summary, samples, args.notes)
    print(f"verdict: {summary['verdict']}", flush=True)
    return 1 if summary["verdict"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
