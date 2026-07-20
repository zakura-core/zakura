#!/usr/bin/env python3
"""Monitor an ephemeral Zakura PR node and its optional managed zcashd child."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

RPC_GRACE_SECS = 300
PRUNED_SIDECAR_ERROR = (
    "cannot serve block because its body has been pruned; a zcashd sidecar "
    "requiring this block cannot continue syncing"
)
EXPECTED_ZCASHD_STARTUP_ERROR = re.compile(
    r'^\S+\s+INFO zcashd_compat\.zcashd: \S+ ERROR Init: main: Read: Failed to open file '
    r'/mnt/zcashd/(?:peers|banlist)\.dat stream="stdout"$'
)


def rpc_call(url: str, method: str, cookie_file: str | None = None):
    payload = json.dumps(
        {"jsonrpc": "1.0", "id": "pr-node-monitor", "method": method, "params": []}
    ).encode()
    headers = {"Content-Type": "application/json"}
    if cookie_file:
        cookie = Path(cookie_file).read_text().strip()
        headers["Authorization"] = "Basic " + base64.b64encode(cookie.encode()).decode()
    req = urllib.request.Request(url, data=payload, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        response = json.loads(resp.read())
    if response.get("error") is not None:
        raise RuntimeError(f"RPC error: {response['error']}")
    return response["result"]


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
    match = re.search(r"^VmRSS:\s+(\d+)\s+kB", status, re.MULTILINE)
    return round(int(match.group(1)) / 1024, 1) if match else None


def managed_zcashd_children(parent_pid: str) -> list[int]:
    if parent_pid == "0":
        return []
    proc = subprocess.run(
        ["pgrep", "--parent", parent_pid, "--exact", "zcashd"],
        capture_output=True,
        text=True,
        check=False,
    )
    return sorted(int(pid) for pid in proc.stdout.split() if pid.isdigit())


def take_sample(args) -> dict:
    sample: dict = {"elapsed": None, "height": None, "estimated": None, "peers": None}
    try:
        info = rpc_call(args.rpc_url, "getblockchaininfo")
        sample["height"] = info.get("blocks")
        sample["estimated"] = info.get("estimatedheight")
    except Exception as exc:  # noqa: BLE001 - an RPC failure is a missed sample
        sample["rpc_error"] = str(exc)
    try:
        sample["peers"] = len(rpc_call(args.rpc_url, "getpeerinfo"))
    except Exception:  # noqa: BLE001
        pass

    props = systemd_props(args.service)
    sample["active_state"] = props.get("ActiveState")
    sample["restarts"] = int(props.get("NRestarts") or 0)
    pid = props.get("MainPID", "0")
    sample["rss_mib"] = rss_mib(pid) if pid != "0" else None

    if args.test_profile == "zcashd-compat":
        children = managed_zcashd_children(pid)
        sample["zcashd_child_pids"] = children
        sample["zcashd_child_count"] = len(children)
        try:
            zcashd_info = rpc_call(
                args.zcashd_rpc_url, "getblockchaininfo", args.zcashd_cookie_file
            )
            sample["zcashd_height"] = zcashd_info.get("blocks")
        except Exception as exc:  # noqa: BLE001
            sample["zcashd_height"] = None
            sample["zcashd_rpc_error"] = str(exc)
        try:
            sample["zcashd_connections"] = rpc_call(
                args.zcashd_rpc_url, "getconnectioncount", args.zcashd_cookie_file
            )
        except Exception:  # noqa: BLE001
            sample["zcashd_connections"] = None
    return sample


def scan_logs(log_file: str, service: str, test_profile: str) -> dict:
    errors = warns = pruned_sidecar_errors = expected_zcashd_startup_errors = 0
    last_errors: list[str] = []
    try:
        with open(log_file, errors="replace") as fh:
            for line in fh:
                line = line.rstrip()
                if PRUNED_SIDECAR_ERROR in line:
                    pruned_sidecar_errors += 1
                if " ERROR " in line:
                    if (
                        test_profile == "zcashd-compat"
                        and EXPECTED_ZCASHD_STARTUP_ERROR.fullmatch(line)
                    ):
                        expected_zcashd_startup_errors += 1
                    else:
                        errors += 1
                        last_errors.append(line[:300])
                        last_errors = last_errors[-5:]
                elif " WARN " in line:
                    warns += 1
    except OSError:
        pass
    journal = subprocess.run(
        ["journalctl", "--boot", "-u", service, "--no-pager", "-o", "cat"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    panics = len(re.findall(r"panicked at", journal))
    return {
        "errors": errors,
        "warns": warns,
        "panics": panics,
        "pruned_sidecar_errors": pruned_sidecar_errors,
        "expected_zcashd_startup_errors": expected_zcashd_startup_errors,
        "last_errors": last_errors,
    }


def height_summary(samples: list[dict], field: str) -> tuple[int | None, int | None, int | None]:
    values = [sample[field] for sample in samples if sample.get(field) is not None]
    if not values:
        return None, None, None
    return values[0], values[-1], values[-1] - values[0]


def load_json(path: str | None):
    if not path or not Path(path).is_file():
        return None
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return {"manifest_error": str(exc)}


def asset_age(created: str | None) -> str:
    if not created:
        return "unknown"
    try:
        timestamp = dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
        age = dt.datetime.now(dt.timezone.utc) - timestamp.astimezone(dt.timezone.utc)
    except ValueError:
        return "unknown"
    hours = max(0, int(age.total_seconds() // 3600))
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d {hours % 24}h"


def build_summary(
    meta: dict,
    samples: list[dict],
    logs: dict,
    duration_min: float,
    lifecycle: dict | None,
    zakura_fixture,
    zcashd_fixture,
) -> dict:
    start_h, end_h, progress = height_summary(samples, "height")
    last = samples[-1] if samples else {}
    peers = [sample["peers"] for sample in samples if sample.get("peers") is not None]
    rss = [sample["rss_mib"] for sample in samples if sample.get("rss_mib") is not None]
    restarts = max((sample["restarts"] for sample in samples), default=0)
    compat = meta.get("test_profile") == "zcashd-compat"

    verdict = "ok"
    if (
        last.get("active_state") != "active"
        or logs["panics"] > 0
        or restarts > 0
        or end_h is None
        or logs["pruned_sidecar_errors"] > 0
        or (lifecycle is not None and not lifecycle.get("passed", False))
    ):
        verdict = "failed"
    elif logs["errors"] > 0 or (progress is not None and progress <= 0):
        verdict = "degraded"

    summary = {
        **meta,
        "verdict": verdict,
        "duration_minutes": duration_min,
        "start_height": start_h,
        "end_height": end_h,
        "blocks_synced": progress,
        "blocks_per_hour": (
            round(progress / duration_min * 60)
            if progress is not None and duration_min
            else None
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
        "pruned_sidecar_errors": logs["pruned_sidecar_errors"],
        "expected_zcashd_startup_errors": logs["expected_zcashd_startup_errors"],
        "last_errors": logs["last_errors"],
        "cold_warm_lifecycle": lifecycle,
        "zakura_fixture_manifest": zakura_fixture,
        "zcashd_fixture_manifest": zcashd_fixture,
    }

    for prefix in ("image", "zakura_snapshot", "zcashd_snapshot"):
        summary[f"{prefix}_age"] = asset_age(meta.get(f"{prefix}_created"))

    if compat:
        z_start, z_end, z_progress = height_summary(samples, "zcashd_height")
        child_pids = sorted(
            {
                pid
                for sample in samples
                for pid in sample.get("zcashd_child_pids", [])
            }
        )
        missing_children = sum(
            1
            for sample in samples
            if sample.get("elapsed", 0) > RPC_GRACE_SECS
            and sample.get("zcashd_child_count") != 1
        )
        summary.update(
            {
                "zcashd_start_height": z_start,
                "zcashd_end_height": z_end,
                "zcashd_blocks_synced": z_progress,
                "zcashd_child_pids": child_pids,
                "zcashd_child_restarts": max(0, len(child_pids) - 1),
                "zcashd_child_missing_samples": missing_children,
                "zcashd_connections_at_end": last.get("zcashd_connections"),
                "height_drift_at_end": (
                    abs(end_h - z_end) if end_h is not None and z_end is not None else None
                ),
            }
        )
        if (
            z_end is None
            or lifecycle is None
            or not lifecycle.get("passed", False)
            or last.get("zcashd_child_count") != 1
            or last.get("zcashd_connections") != 1
            or summary["zcashd_child_restarts"] > 0
            or missing_children > 0
        ):
            summary["verdict"] = "failed"
        elif z_progress is not None and z_progress <= 0 and summary["verdict"] == "ok":
            summary["verdict"] = "degraded"
    return summary


def fmt(value, suffix: str = "") -> str:
    return f"{value}{suffix}" if value is not None else "n/a"


def write_markdown(out: Path, summary: dict, samples: list[dict], notes_file: str | None):
    icon = {"ok": "✅", "degraded": "⚠️", "failed": "❌"}[summary["verdict"]]
    mode, network = summary.get("mode", "?"), summary.get("network", "?")
    profile = summary.get("test_profile", "zakura")
    if profile == "zakura":
        title = f"## Zakura PR node run — {mode}/{network} {icon}"
    else:
        title = f"## Zakura PR node run — {profile}/{mode}/{network} {icon}"
    lines = [
        title,
        "",
        "| | |",
        "|---|---|",
        f"| Verdict | **{summary['verdict']}** |",
        f"| Test profile | `{profile}` |",
        f"| Commit | `{summary.get('sha', '?')}` |",
        f"| Duration | {summary['duration_minutes']:g} min |",
        f"| Zakura height | {fmt(summary['start_height'])} → {fmt(summary['end_height'])} "
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
    if profile == "zcashd-compat":
        lines += [
            f"| zcashd height | {fmt(summary['zcashd_start_height'])} → "
            f"{fmt(summary['zcashd_end_height'])} (+{fmt(summary['zcashd_blocks_synced'])}) |",
            f"| Height drift at end | {fmt(summary['height_drift_at_end'])} |",
            f"| zcashd child | {len(summary['zcashd_child_pids'])} PID(s) observed, "
            f"{summary['zcashd_child_restarts']} unexpected restart(s) |",
            f"| zcashd connections at end | {fmt(summary['zcashd_connections_at_end'])} |",
            f"| Pruned sidecar diagnostic | {summary['pruned_sidecar_errors']} occurrence(s) |",
            f"| Expected zcashd startup errors | "
            f"{summary['expected_zcashd_startup_errors']} occurrence(s) |",
            f"| Cold stop/warm restart | "
            f"{'passed' if (summary.get('cold_warm_lifecycle') or {}).get('passed') else 'failed'} |",
        ]

    lines += [
        "",
        "### Golden assets",
        "",
        "| Asset | Name | ID | Created | Age |",
        "|---|---|---|---|---|",
        f"| Base image | `{summary.get('image_name', 'unknown')}` | "
        f"`{summary.get('image_id', 'unknown')}` | {summary.get('image_created', 'unknown')} | "
        f"{summary.get('image_age', 'unknown')} |",
        f"| Zakura state | `{summary.get('zakura_snapshot_name', 'none')}` | "
        f"`{summary.get('zakura_snapshot_id', 'none')}` | "
        f"{summary.get('zakura_snapshot_created', 'unknown')} | "
        f"{summary.get('zakura_snapshot_age', 'unknown')} |",
    ]
    if profile == "zcashd-compat":
        lines.append(
            f"| zcashd state | `{summary.get('zcashd_snapshot_name', 'none')}` | "
            f"`{summary.get('zcashd_snapshot_id', 'none')}` | "
            f"{summary.get('zcashd_snapshot_created', 'unknown')} | "
            f"{summary.get('zcashd_snapshot_age', 'unknown')} |"
        )

    shown = [sample for sample in samples if sample.get("height") is not None]
    if shown:
        step = max(1, len(shown) // 12)
        picked = shown[::step]
        if picked[-1] is not shown[-1]:
            picked.append(shown[-1])
        headers = "| Elapsed | Zakura | zcashd | Peers | RSS (MiB) |" if profile == "zcashd-compat" else "| Elapsed | Height | Peers | RSS (MiB) |"
        separator = "|---|---|---|---|---|" if profile == "zcashd-compat" else "|---|---|---|---|"
        lines += ["", "### Height over time", "", headers, separator]
        for sample in picked:
            if profile == "zcashd-compat":
                lines.append(
                    f"| {int(sample['elapsed'] // 60)}m | {fmt(sample['height'])} | "
                    f"{fmt(sample.get('zcashd_height'))} | {fmt(sample['peers'])} | "
                    f"{fmt(sample['rss_mib'])} |"
                )
            else:
                lines.append(
                    f"| {int(sample['elapsed'] // 60)}m | {fmt(sample['height'])} | "
                    f"{fmt(sample['peers'])} | {fmt(sample['rss_mib'])} |"
                )

    manifests = [
        ("Zakura", summary.get("zakura_fixture_manifest")),
        ("zcashd", summary.get("zcashd_fixture_manifest")),
    ]
    manifests = [(name, value) for name, value in manifests if value is not None]
    if manifests:
        lines += ["", "### Fixture manifests"]
        for name, value in manifests:
            lines += ["", f"**{name}**", "", "```json", json.dumps(value, indent=2), "```"]

    if summary["last_errors"]:
        lines += ["", "### Last errors", "", "```", *summary["last_errors"], "```"]
    if notes_file and Path(notes_file).is_file():
        lines += ["", "### Notes", "", Path(notes_file).read_text().rstrip()]
    (out / "summary.md").write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-minutes", type=float, required=True)
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--test-profile", choices=("zakura", "zcashd-compat"), default="zakura")
    parser.add_argument("--rpc-url", default="http://127.0.0.1:8232")
    parser.add_argument("--zcashd-rpc-url", default="http://127.0.0.1:8232")
    parser.add_argument("--zcashd-cookie-file")
    parser.add_argument("--service", default="zakurad")
    parser.add_argument("--log-file", default="/var/log/zakura/zakura.log")
    parser.add_argument("--notes")
    parser.add_argument("--meta", default="", help="comma-separated key=value metadata")
    parser.add_argument("--lifecycle-file")
    parser.add_argument("--zakura-fixture-manifest")
    parser.add_argument("--zcashd-fixture-manifest")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    meta = dict(kv.split("=", 1) for kv in args.meta.split(",") if "=" in kv)
    meta["test_profile"] = args.test_profile

    start = time.monotonic()
    deadline = start + args.duration_minutes * 60
    samples: list[dict] = []
    while True:
        sample = take_sample(args)
        sample["elapsed"] = time.monotonic() - start
        samples.append(sample)
        status = (
            f"[{int(sample['elapsed'])}s] zakura={fmt(sample['height'])} "
            f"peers={fmt(sample['peers'])} rss={fmt(sample['rss_mib'])}MiB "
            f"state={sample['active_state']}"
        )
        if args.test_profile == "zcashd-compat":
            status += (
                f" zcashd={fmt(sample.get('zcashd_height'))} "
                f"child={fmt(sample.get('zcashd_child_pids'))}"
            )
        print(status, flush=True)
        if sample["elapsed"] > RPC_GRACE_SECS and sample["active_state"] == "failed":
            print("service failed; stopping monitor early", flush=True)
            break
        if time.monotonic() + args.interval > deadline:
            break
        time.sleep(args.interval)

    summary = build_summary(
        meta,
        samples,
        scan_logs(args.log_file, args.service, args.test_profile),
        args.duration_minutes,
        load_json(args.lifecycle_file),
        load_json(args.zakura_fixture_manifest),
        load_json(args.zcashd_fixture_manifest),
    )
    (out / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_markdown(out, summary, samples, args.notes)
    print(f"verdict: {summary['verdict']}", flush=True)
    return 1 if summary["verdict"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
