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


def scrape_metric(url: str, name: str) -> float | None:
    with urllib.request.urlopen(url, timeout=15) as resp:
        text = resp.read().decode("utf-8", "replace")
    values = [
        float(match.group(1))
        for match in re.finditer(
            rf"^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([-+0-9.eE]+)$",
            text,
            re.MULTILINE,
        )
    ]
    return max(values) if values else None


def systemd_props(service: str) -> dict[str, str]:
    out = subprocess.run(
        ["systemctl", "show", service, "--property=ActiveState,NRestarts,MainPID"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


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
        sample["finalized_height"] = scrape_metric(
            args.metrics_url, "state_finalized_block_height"
        )
        sample["vct_fast_blocks"] = scrape_metric(
            args.metrics_url, "state_vct_fast_block_count"
        )
    except Exception as exc:  # noqa: BLE001 — metrics failure is recorded in the sample
        sample["metrics_error"] = str(exc)
    props = systemd_props(args.service)
    sample["active_state"] = props.get("ActiveState")
    sample["restarts"] = int(props.get("NRestarts") or 0)
    pid = props.get("MainPID", "0")
    sample["rss_mib"] = rss_mib(pid) if pid != "0" else None
    return sample


def scan_logs(log_file: str, service: str) -> dict:
    errors = warns = 0
    last_errors: list[str] = []
    try:
        with open(log_file, errors="replace") as fh:
            for line in fh:
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
    ).stdout
    panics = len(re.findall(r"panicked at", journal)) + len(
        re.findall(r"panicked at", "".join(last_errors))
    )
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
) -> dict:
    heighted = [s for s in samples if s["height"] is not None]
    first_observed_h = heighted[0]["height"] if heighted else None
    start_h = known_start_height if known_start_height is not None else first_observed_h
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
    restarts = max((s["restarts"] for s in samples), default=0)

    if (
        last.get("active_state") != "active"
        or logs["panics"] > 0
        or restarts > 0
        or not heighted
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
        or (require_vct_fast_blocks and (vct_fast_blocks is None or vct_fast_blocks <= 0))
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
        "require_vct_fast_blocks": require_vct_fast_blocks,
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
            f"(required > 0: {summary['require_vct_fast_blocks']}) |"
        )

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

    if notes_file and Path(notes_file).is_file():
        lines += ["", "### Notes", "", Path(notes_file).read_text().rstrip()]

    (out / "summary.md").write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-minutes", type=float, required=True)
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8232")
    parser.add_argument("--metrics-url", default="http://127.0.0.1:9999/metrics")
    parser.add_argument("--service", default="zakurad")
    parser.add_argument("--log-file", default="/var/log/zakura/zakura.log")
    parser.add_argument("--notes", default=None, help="markdown notes file to append")
    parser.add_argument("--meta", default="", help="comma-separated key=value run metadata")
    parser.add_argument(
        "--known-start-height",
        type=int,
        default=None,
        help="state snapshot height, used when sync crosses before RPC is available",
    )
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
    parser.add_argument("--require-vct-fast-blocks", action="store_true")
    parser.add_argument("--out", required=True, help="output directory")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    meta = dict(kv.split("=", 1) for kv in args.meta.split(",") if "=" in kv)

    start = time.monotonic()
    deadline = start + args.duration_minutes * 60
    samples: list[dict] = []
    while True:
        sample = take_sample(args)
        sample["elapsed"] = time.monotonic() - start
        samples.append(sample)
        status = (
            f"[{int(sample['elapsed'])}s] height={fmt(sample['height'])} "
            f"finalized={fmt(sample['finalized_height'])} "
            f"vct_fast={fmt(sample['vct_fast_blocks'])} "
            f"peers={fmt(sample['peers'])} rss={fmt(sample['rss_mib'])}MiB "
            f"state={sample['active_state']}"
        )
        print(status, flush=True)
        # Bail out early once the node is clearly gone (past the startup grace).
        if sample["elapsed"] > RPC_GRACE_SECS and sample["active_state"] == "failed":
            print("service failed; stopping monitor early", flush=True)
            break
        if (
            args.stop_after_height is not None
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
                    sample["vct_fast_blocks"] is not None
                    and sample["vct_fast_blocks"] > 0
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

    logs = scan_logs(args.log_file, args.service)
    actual_duration_min = (time.monotonic() - start) / 60
    summary = build_summary(
        meta,
        samples,
        logs,
        actual_duration_min,
        known_start_height=args.known_start_height,
        required_start_below=args.required_start_below,
        stop_after_height=args.stop_after_height,
        required_finalized_at_least=args.required_finalized_at_least,
        require_vct_fast_blocks=args.require_vct_fast_blocks,
    )
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_markdown(out, summary, samples, args.notes)
    print(f"verdict: {summary['verdict']}", flush=True)
    return 1 if summary["verdict"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
