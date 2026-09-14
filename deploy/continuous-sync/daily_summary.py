#!/usr/bin/env python3
"""Deliver the fleet summary at a fixed local time with durable completion cursors."""

from __future__ import annotations

import argparse
import concurrent.futures
from datetime import datetime, time as wall_time, timedelta
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import time
import tomllib
from zoneinfo import ZoneInfo

from deploy import DeployError, Node, completion_updates, post_slack, slack_webhook_url, sync_label


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
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise DeployError("summary delivery state missing or invalid; restore it or initialize from the last confirmed post") from error
    return state


def save_state(path: Path, state: dict) -> None:
    """Atomically replace and fsync delivery state on the persistent host filesystem."""
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as file:
        json.dump(state, file, indent=2, sort_keys=True)
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


def deliver(config: dict, path: Path, timestamp: int, *, dry_run: bool = False) -> int:
    settings = config["summary"]
    names = {node["name"] for node in config["nodes"]}
    state = load_state(path, timestamp, settings, names)
    slot, _ = latest_slot(timestamp, settings)
    destination = slack_webhook_url()
    if not destination or hashlib.sha256(destination.encode()).hexdigest() != state["destination"]:
        raise DeployError("summary Slack destination missing or changed; explicitly restore delivery history for this destination")
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
                      "due_slot": slot, "overdue": overdue}))
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
            return deliver(config, path, timestamp, dry_run=args.dry_run)
    except (DeployError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"summary failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
