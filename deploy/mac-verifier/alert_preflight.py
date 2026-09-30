#!/usr/bin/env python3
"""Fail closed before quiet alert enablement; never contact Slack."""
import argparse
import math
import os
from pathlib import Path
import time
import uuid

from common import Transport, Unavailable, atomic_json, read_json
from monitor import Monitor, exclusive


def replace_owned_json(path, value):
    """Retain service ownership when a root operator replaces its state."""
    path = Path(path)
    stat = path.stat()
    atomic_json(path, value)
    os.chown(path, stat.st_uid, stat.st_gid)
    path.chmod(stat.st_mode & 0o777)


def inspect(directory, expected, now, fleet):
    directory = Path(directory)
    state, status = read_json(directory / "cursor.json"), read_json(directory / "status.json")
    if state.get("receipt_digest") != Monitor.receipt_digest(expected):
        raise Unavailable("receipt changed")
    stamp, since = status.get("sample_time"), status.get("enablement_ready_since")
    if (type(stamp) not in (int, float) or not 0 <= now - stamp <= 60
            or type(since) not in (int, float) or now - since < 120
            or type(status.get("enablement_good_samples")) is not int
            or status["enablement_good_samples"] < 5):
        raise Unavailable("five fresh healthy samples spanning two minutes required")
    actionable = set(state.get("incidents", {})) - {"alert delivery unavailable"}
    if actionable or not status.get("caught_up") or state.get("outbox_overflow"):
        raise Unavailable("active incident, incomplete coverage or queue overflow")
    poll = fleet.get("last_poll")
    rows = fleet.get("rows")
    if (fleet.get("network") != "mainnet" or type(poll) not in (int, float)
            or not 0 <= now - poll <= 90 or not isinstance(rows, list)
            or any(not isinstance(row, dict) for row in rows)):
        raise Unavailable("fresh mainnet dashboard rows required")
    mac = next((r for r in rows if r.get("name") == "zakura-mac-os"), None)
    if mac is None or mac.get("health") != "healthy":
        raise Unavailable("Mac fleet row is not healthy")
    anchor = mac.get("fork_anchor")
    if not isinstance(anchor, dict) or type(anchor.get("height")) is not int or not anchor.get("hash"):
        raise Unavailable("Mac quorum evidence absent")
    others = [r for r in rows if r.get("name") != "zakura-mac-os"]
    agreeing = sum(r.get("health") == "healthy" and r.get("fork_anchor") == anchor for r in others)
    if not others or agreeing < math.ceil(0.7 * len(others)):
        raise Unavailable("quiet enablement requires a matching fleet quorum")
    prefixes = ("Zakura Mac verifier: ", "Zakura Mac verifier recovered: ",
                "Zakura Mac verifier needs attention: ")
    queue = state.get("outbox", [])
    if any(not isinstance(item, dict) or not isinstance(item.get("text"), str)
           or not item["text"].startswith(prefixes) for item in queue):
        raise Unavailable("unrecognized queued notification requires inspection")
    return state, {"allowed_to_enable": True, "pending_messages_on_enable": 0,
                   "historical_messages_to_archive": len(queue), "healthy_samples": status["enablement_good_samples"],
                   "agreeing_peers": agreeing, "other_peers": len(others), "sample_time": stamp}


def prepare(directory, expected, now, fleet, apply=False, fleet_state_path=None):
    with exclusive(directory):
        state, report = inspect(directory, expected, now, fleet)
        fleet_state = read_json(fleet_state_path) if fleet_state_path else None
        if fleet_state and "mainnet" in fleet_state.get("pending_delivery", {}):
            raise Unavailable("pending fleet delivery must drain before quiet enablement")
        if apply:
            archive = {"event": "quiet alert enablement", "time": now, "report": report,
                       "archived_notifications": state["outbox"]}
            atomic_json(Path(directory) / "incidents" / ("enablement-" + uuid.uuid4().hex + ".json"), archive)
            state["outbox"] = []
            state["incidents"].pop("alert delivery unavailable", None)
            state.update(channel_episode_version=1, channel_episode_active=False, channel_episode_findings=[],
                         healthy_since=None, healthy_start_height=None, qualified=False)
            replace_owned_json(Path(directory) / "cursor.json", state)
            if fleet_state is not None:
                atomic_json(Path(directory) / "incidents" / ("fleet-before-enablement-" + uuid.uuid4().hex + ".json"), fleet_state)
                fleet_state.get("nodes", {}).pop("mainnet/zakura-mac-os", None)
                fleet_state.get("mac_forks", {}).pop("mainnet", None)
                replace_owned_json(fleet_state_path, fleet_state)
        return report


if __name__ == "__main__":
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", default="/var/lib/zakura-mac-verifier")
    parser.add_argument("--receipt", default="/etc/zakura-mac-verifier/receipt.json")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--fleet-state", help="watchdog state; stop its service before applying")
    args = parser.parse_args()
    try:
        fleet = Transport().json("https://status.mainnet.zakura.valargroup.dev/data")
        print(json.dumps(prepare(args.directory, read_json(args.receipt), time.time(), fleet, args.apply, args.fleet_state)))
    except Exception:
        raise SystemExit("Quiet enablement blocked; inspect privately, alerts remain muted") from None
