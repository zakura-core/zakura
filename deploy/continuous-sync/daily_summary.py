#!/usr/bin/env python3
"""Deliver the fleet summary at a fixed local time with durable completion cursors."""

from __future__ import annotations

import argparse
import concurrent.futures
from datetime import datetime, time as wall_time, timedelta
import fcntl
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import socket
import sys
import time
import tomllib
from zoneinfo import ZoneInfo

from deploy import DeployError, Node, completion_updates, post_slack, slack_webhook_url, sync_label
import report_charts
from slack_report import Client, send_pending
from sync_report import REPORT_MODES, RETENTION_SECONDS, RUN_ID, validate_report


SCRIPT_DIR = Path(__file__).resolve().parent
STATE_VERSION = 1
spec = importlib.util.spec_from_file_location("summary_monitor", SCRIPT_DIR / "alert-monitor.py")
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


def latest_slot(timestamp: int, settings: dict) -> tuple[str, int]:
    """Return the latest daily deadline, including yesterday before today's cutoff."""
    zone = ZoneInfo(settings["timezone"])
    hour, minute = map(int, settings["time"].split(":"))
    local = datetime.fromtimestamp(timestamp, zone)
    deadline = datetime.combine(local.date(), wall_time(hour, minute), zone)
    if local < deadline:
        deadline -= timedelta(days=1)
    return deadline.date().isoformat(), int(deadline.timestamp())


def load_state(path: Path, timestamp: int, settings: dict, names: set[str]) -> dict:
    """Missing or invalid delivery history requires recovery, never a fresh timer."""
    try:
        state = json.loads(path.read_text())
        if (state["version"] != STATE_VERSION
                or type(state["last_posted_at"]) is not int
                or not 0 < state["last_posted_at"] <= timestamp
                or state["last_slot"] != latest_slot(state["last_posted_at"], settings)[0]
                or not isinstance(state["destination"], str)
                or len(state["destination"]) != 64):
            raise ValueError("invalid delivery metadata")
        cursors = state["cursors"]
        if not isinstance(cursors, dict) or not names <= cursors.keys():
            raise ValueError("missing completion cursors")
        unavailable = state.get("unavailable", [])
        if not isinstance(unavailable, list) or any(not isinstance(name, str) for name in unavailable):
            raise ValueError("invalid unavailable node list")
        for cursor in cursors.values():
            if (type(cursor["number"]) is not int or cursor["number"] < 0
                    or not isinstance(cursor["run_id"], str)
                    or bool(cursor["run_id"]) != (cursor["number"] > 0)):
                raise ValueError("invalid completion cursor")
        pending = state.get("pending")
        if pending is not None:
            if (not isinstance(pending, dict) or type(pending["timestamp"]) is not int
                    or not state["last_posted_at"] < pending["timestamp"] <= timestamp
                    or pending["slot"] != latest_slot(pending["timestamp"], settings)[0]
                    or pending["slot"] <= state["last_slot"]
                    or not isinstance(pending["text"], str) or len(pending["text"]) > 40000
                    or not isinstance(pending["files"], list) or not pending["files"]
                    or not isinstance(pending["cursors"], dict) or not names <= pending["cursors"].keys()
                    or not isinstance(pending["unavailable"], list)):
                raise ValueError("invalid pending delivery")
            if pending.get("parent_ts") and not re.fullmatch(r"\d+\.\d+", pending["parent_ts"]):
                raise ValueError("invalid pending parent")
            if pending.get("parent_ts") and pending.get("parent_posting"):
                raise ValueError("conflicting parent delivery state")
            for name, cursor in pending["cursors"].items():
                if (name not in cursors or type(cursor["number"]) is not int
                        or cursor["number"] < cursors[name]["number"] or not isinstance(cursor["run_id"], str)
                        or bool(cursor["run_id"]) != (cursor["number"] > 0)
                        or ((cursor["number"] == cursors[name]["number"]) != (cursor["run_id"] == cursors[name]["run_id"]))):
                    raise ValueError("invalid pending completion cursor")
            filenames = set()
            for file in pending["files"]:
                if (not RUN_ID.fullmatch(file["name"]) or not file["name"].endswith(".png")
                        or file["name"] in filenames or not isinstance(file["title"], str)
                        or any(type(file.get(flag, False)) is not bool for flag in ("done", "completing"))
                        or (file.get("done") or file.get("completing")) and not pending.get("parent_ts")):
                    raise ValueError("invalid pending image")
                filenames.add(file["name"])
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise DeployError("summary delivery state missing or invalid; restore it or initialize from the last confirmed post") from error
    return state


def save_state(path: Path, state: dict) -> None:
    """Atomically replace and fsync delivery state on the persistent host filesystem."""
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as file:
        json.dump(state, file, indent=2, sort_keys=True, allow_nan=False)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def collect_statuses(config: dict, cursors: dict) -> tuple[dict, dict]:
    """Use the existing read-only monitor access; retain cursors for unavailable nodes."""
    statuses = {}
    next_cursors = dict(cursors)
    nodes = config["nodes"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        results = pool.map(lambda node: monitor.query_node(config, node), nodes)
        for node, data in zip(nodes, results):
            name = node["name"]
            data = data if isinstance(data, dict) else {}
            controller = data.get("controller_state")
            controller = controller if isinstance(controller, dict) else {}
            total, run_id = controller.get("runs"), controller.get("last_success_run")
            cursor = cursors[name]
            if type(total) is int and total == 0 and cursor["number"] == 0 and not data.get("query_error"):
                statuses[name] = data
                continue
            valid = (
                not data.get("query_error")
                and controller.get("completion_digest") is True
                and type(total) is int and total >= cursor["number"]
                and isinstance(run_id, str) and bool(run_id)
                and ((total == cursor["number"]) == (run_id == cursor["run_id"]))
            )
            if not valid:
                print(f"{name}: completion status unavailable or counter reset; retaining delivered cursor", file=sys.stderr)
                continue
            statuses[name] = {**data, "sample": {"metrics_status": data.get("metrics_status", "unknown")}}
            next_cursors[name] = {"number": total, "run_id": run_id}
    return statuses, next_cursors


def prepare_charts(config: dict, statuses: dict, cursors: dict, directory: Path) -> list[dict]:
    """Freeze available run data and render before sending the parent message."""
    directory.mkdir(parents=True, exist_ok=True)
    # No pending delivery exists here. Prune abandoned pre-publication bundles.
    for previous in directory.parent.glob("charts-????-??-??"):
        if (previous != directory and previous.is_dir() and not previous.is_symlink()
                and time.time() - previous.stat().st_mtime > RETENTION_SECONDS):
            shutil.rmtree(previous)
    groups, history = [], []
    cache = directory.parent / "history"
    cache.mkdir(exist_ok=True)
    cached_paths = sorted(cache.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
    for cached in cached_paths[256:]:
        cached.unlink()
    for cached in cached_paths[:256]:
        if time.time() - cached.stat().st_mtime > RETENTION_SECONDS:
            cached.unlink()
            continue
        try:
            if cached.stat().st_size <= 65536:
                history.append(json.loads(cached.read_text()))
        except (OSError, ValueError, TypeError):
            continue
    for node in config["nodes"]:
        if node.get("p2p_stack") not in REPORT_MODES:
            continue
        name = node["name"]
        state = statuses.get(name, {}).get("controller_state", {})
        total = state.get("runs", cursors[name]["number"])
        raw = state.get("completion_history", [])
        raw = raw if isinstance(raw, list) else []
        runs = {item["number"]: item["run_id"] for item in raw
                if isinstance(item, dict) and type(item.get("number")) is int
                and cursors[name]["number"] < item["number"] <= total
                and isinstance(item.get("run_id"), str) and RUN_ID.fullmatch(item["run_id"])}
        if total > cursors[name]["number"] and RUN_ID.fullmatch(state.get("last_success_run", "")):
            runs[total] = state["last_success_run"]
        # Daily runs normally fit on one page. Keep outage catch-up work bounded.
        run_ids = [runs[number] for number in sorted(runs)[-12:]]
        omitted = max(0, total - cursors[name]["number"] - len(run_ids))
        failed = state.get("last_failed_run") if state.get("failed") else None
        if isinstance(failed, str) and RUN_ID.fullmatch(failed) and failed not in run_ids:
            run_ids.append(failed)
        reports = []
        for run_id in run_ids:
            report = monitor.query_node(config, node, report_id=run_id)
            try:
                validate_report(report)
                if report["metadata"]["run_id"] != run_id:
                    raise ValueError("wrong run returned")
                if report["metadata"]["mode"] != node["p2p_stack"]:
                    raise ValueError("wrong networking mode returned")
                report["metadata"]["host"]["node"] = name
            except (ValueError, TypeError, KeyError):
                report = {"run_id": run_id, "mode": node.get("p2p_stack"),
                          "unavailable": "telemetry not retained or node unavailable"}
                if run_id == failed:
                    report["unavailable"] = "failed run • telemetry unavailable"
            reports.append(report)
            if "metadata" in report:
                key = hashlib.sha256(f"{name}:{run_id}".encode()).hexdigest()
                save_state(cache / f"{key}.json", report_charts.baseline_record(report, config["summary"]))
        title = sync_label(Node(node))
        if omitted:
            title += f" • {omitted} earlier run(s) without charts"
        if not reports:
            reports = [{"mode": node.get("p2p_stack"),
                        "unavailable": "no new completed runs" if name in statuses else "status unavailable"}]
        groups.append((title, reports))
    with gzip.open(directory / "snapshot.json.gz", "wt") as file:
        json.dump({"settings": config["summary"], "groups": groups}, file, separators=(",", ":"), allow_nan=False)
    limits = report_charts.scales([report for _, reports in groups for report in reports])
    files = []
    for group, (title, reports) in enumerate(groups):
        for page, offset in enumerate(range(0, len(reports), 3)):
            name = f"section-{group + 1}-{page + 1}.png"
            page_title = title + (f" • page {page + 1}" if len(reports) > 3 else "")
            report_charts.render(reports[offset:offset + 3], page_title, directory / name,
                                 config["summary"], limits=limits, history=history)
            files.append({"name": name, "title": page_title})
    for file in directory.iterdir():
        with file.open("rb") as stream:
            os.fsync(stream.fileno())
    return files


def finish_pending(config: dict, path: Path, state: dict, client: Client) -> int:
    pending = state["pending"]
    directory = path.parent / f"charts-{pending['slot']}"
    send_pending(client, config["summary"]["channel_id"], pending, directory, lambda: save_state(path, state))
    state.update(last_slot=pending["slot"], last_posted_at=pending["timestamp"],
                 cursors=pending["cursors"], unavailable=pending["unavailable"])
    del state["pending"]
    save_state(path, state)
    # Only remove the bundle after both parent and every image are confirmed.
    shutil.rmtree(directory)
    print(f"summary and charts delivered for {state['last_slot']}")
    return 0


def deliver(config: dict, path: Path, timestamp: int, *, dry_run: bool = False) -> int:
    settings = config["summary"]
    names = {node["name"] for node in config["nodes"]}
    state = load_state(path, timestamp, settings, names)
    slot, _ = latest_slot(timestamp, settings)
    charts = settings.get("charts", False)
    if charts and not state.get("pending") and state["last_slot"] >= slot:
        print(f"summary already delivered for {slot}")
        return 0
    client = None
    if charts and not dry_run:
        client = Client()
        if client.destination(settings["channel_id"]) != state.get("chart_destination"):
            raise DeployError("chart destination is not bound to delivery history; run bind-bot first")
    elif not charts:
        destination = slack_webhook_url()
        if not destination or hashlib.sha256(destination.encode()).hexdigest() != state["destination"]:
            raise DeployError("summary Slack destination missing or changed; explicitly restore delivery history for this destination")
    if state.get("pending"):
        if dry_run:
            print(state["pending"]["text"])
            print("Pending chart delivery will resume without collecting a new snapshot.")
            return 0
        if not charts:
            raise DeployError("finish pending chart delivery before switching back to webhook delivery")
        return finish_pending(config, path, state, client)
    if state["last_slot"] >= slot:
        print(f"summary already delivered for {slot}")
        return 0
    statuses, cursors = collect_statuses(config, state["cursors"])
    previous = {"completions": {
        name: {"total": cursor["number"], "run_id": cursor["run_id"], "pending": 0,
               "details": [], "sha": "unknown", "duration": None}
        for name, cursor in state["cursors"].items()
    }}
    labels = {node["name"]: sync_label(Node(node)) for node in config["nodes"]}
    lines, _ = completion_updates(statuses, previous, True, labels)
    unavailable = names - statuses.keys()
    # The shared formatter returns one section per configured name, sorted by name.
    lines = [f"*{labels[name]}*\nstatus unavailable; unreported runs retained" if name in unavailable else line
             for name, line in zip(sorted(labels), lines)]
    text = ":memo: Mainnet sync summary — since previous digest\n\n" + "\n\n".join(lines)
    if unavailable:
        text += "\n\nUnavailable nodes retain their unreported runs for the next summary."
    caught_up = [name for name in state.get("unavailable", []) if name in statuses
                 and cursors[name]["number"] > state["cursors"][name]["number"]]
    if caught_up:
        text += "\n\nIncludes earlier unreported runs from previously unavailable nodes: " + ", ".join(sorted(caught_up)) + "."
    text += "\n\nRuns listed oldest first. Each run starts from genesis."
    if dry_run:
        print(text)
        return 0
    if charts:
        directory = path.parent / f"charts-{slot}"
        files = prepare_charts(config, statuses, state["cursors"], directory)
        state["pending"] = {"slot": slot, "timestamp": timestamp, "text": text,
                            "cursors": cursors, "unavailable": sorted(unavailable), "files": files}
        save_state(path, state)
        return finish_pending(config, path, state, client)
    if not post_slack(text):
        raise DeployError("summary Slack delivery failed; completion cursors retained for retry")
    # The slot belongs to this attempt's snapshot. A later retry never shifts the next deadline.
    save_state(path, {**state, "last_slot": slot, "last_posted_at": timestamp,
                      "cursors": cursors, "unavailable": sorted(unavailable)})
    print(f"summary delivered for {slot}; {len(unavailable)} node(s) unavailable")
    return 0


def initialize(config: dict, path: Path, seed_path: Path, timestamp: int) -> int:
    """Seed only explicitly supplied, already-reported cursors; never overwrite state."""
    if path.exists():
        raise DeployError("summary state already exists; initialization cannot overwrite delivery history")
    state = json.loads(seed_path.read_text())
    destination = slack_webhook_url()
    if not destination:
        raise DeployError("summary Slack destination is not configured")
    state = {**state, "version": STATE_VERSION,
             "destination": hashlib.sha256(destination.encode()).hexdigest(),
             "last_slot": latest_slot(state["last_posted_at"], config["summary"])[0]}
    candidate = path.with_suffix(".seed")
    try:
        save_state(candidate, state)
        load_state(candidate, timestamp, config["summary"], {node["name"] for node in config["nodes"]})
        save_state(path, state)
    finally:
        candidate.unlink(missing_ok=True)
    print("initialized from confirmed delivery history; no Slack post sent")
    return 0


def check_status(config: dict, path: Path, timestamp: int) -> int:
    settings = config["summary"]
    state = load_state(path, timestamp, settings, {node["name"] for node in config["nodes"]})
    slot, deadline = latest_slot(timestamp, settings)
    overdue = state["last_slot"] < slot and timestamp - deadline > 900
    print(json.dumps({"last_posted_at": state["last_posted_at"], "last_slot": state["last_slot"],
                      "due_slot": slot, "overdue": overdue, "pending": state.get("pending")}))
    return int(overdue)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=SCRIPT_DIR / "nodes.toml")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--dry-run", action="store_true")
    seed = sub.add_parser("initialize")
    seed.add_argument("--from-file", type=Path, required=True)
    sub.add_parser("status")
    sub.add_parser("bind-bot", help="bind the configured bot channel while retaining confirmed cursors")
    recover = sub.add_parser("recover-parent", help="record the parent verified by an operator in Slack")
    parent = recover.add_mutually_exclusive_group(required=True)
    parent.add_argument("--ts", help="timestamp of the confirmed parent in the configured channel")
    parent.add_argument("--confirmed-absent", action="store_true", help="operator confirmed no parent was posted")
    image = sub.add_parser("recover-image", help="retry a share the operator confirmed absent in Slack")
    image.add_argument("--name", required=True, help="pending PNG filename shown by status")
    image.add_argument("--confirmed-absent", action="store_true", required=True)
    args = parser.parse_args()
    try:
        with args.config.open("rb") as file:
            config = tomllib.load(file)
        settings = config["summary"]
        if socket.gethostname().split(".", 1)[0] != settings["hostname"]:
            raise DeployError("this host is not the configured summary sender")
        path = Path(settings["state_file"])
        timestamp = int(time.time())
        if args.command == "status":
            return check_status(config, path, timestamp)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with path.with_suffix(".lock").open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise DeployError("another summary invocation holds the delivery lock") from error
            if args.command == "initialize":
                return initialize(config, path, args.from_file, timestamp)
            if args.command in ("bind-bot", "recover-parent", "recover-image"):
                return recover_delivery(config, path, timestamp, args)
            return deliver(config, path, timestamp, dry_run=args.dry_run)
    except (DeployError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"summary failed: {error}", file=sys.stderr)
        return 1


def recover_delivery(config: dict, path: Path, timestamp: int, args) -> int:
    """Operator recovery is explicit and never resets completion history."""
    state = load_state(path, timestamp, config["summary"], {node["name"] for node in config["nodes"]})
    if args.command == "bind-bot":
        destination = slack_webhook_url()
        if state.get("pending") or not destination or hashlib.sha256(destination.encode()).hexdigest() != state["destination"]:
            raise DeployError("binding requires the original webhook destination and no pending delivery")
        state["chart_destination"] = Client().destination(config["summary"]["channel_id"])
    else:
        pending = state.get("pending", {})
        if args.command == "recover-parent":
            if not pending.get("parent_posting") or pending.get("parent_ts"):
                raise DeployError("there is no uncertain parent to recover")
            if args.ts:
                if not re.fullmatch(r"\d+\.\d+", args.ts):
                    raise DeployError("invalid Slack parent timestamp")
                pending["parent_ts"] = args.ts
            pending.pop("parent_posting")
        else:
            file = next((file for file in pending.get("files", []) if file["name"] == args.name), {})
            if not file.get("completing") or file.get("done"):
                raise DeployError("there is no uncertain chart share to recover")
            file.pop("completing")
    save_state(path, state)
    print("delivery history preserved; next timer tick can continue")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
