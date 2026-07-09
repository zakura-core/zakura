#!/usr/bin/env python3
"""Slack watchdog for Zakura fleet status dashboards.

Polls one or more `zebra-cluster-status.py` `/data` endpoints, tracks sustained
node failures in a small JSON state file, and posts transition alerts to Slack.

Only the Python stdlib is used.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import tomllib
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_SLACK_CHANNEL = "C0BCQ7PP32A"
DOWN_HEALTH = {"down", "rpc_error"}
STATE_VERSION = 1


@dataclass(frozen=True)
class Fleet:
    name: str
    url: str
    dashboard_url: str


def load_fleets(config_path: Path) -> list[Fleet]:
    with config_path.open("rb") as config_file:
        data = tomllib.load(config_file)

    fleets = []
    seen = set()
    for raw in data.get("fleets", []):
        for required in ("name", "url"):
            if required not in raw:
                raise SystemExit(f"fleet missing required field '{required}': {raw}")

        name = str(raw["name"])
        if name in seen:
            raise SystemExit(f"duplicate fleet name: {name}")
        seen.add(name)

        url = str(raw["url"])
        dashboard_url = str(raw.get("dashboard_url") or url.removesuffix("/data"))
        fleets.append(Fleet(name=name, url=url, dashboard_url=dashboard_url))

    if not fleets:
        raise SystemExit(f"no [[fleets]] defined in {config_path}")

    return fleets


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {"version": STATE_VERSION, "nodes": {}, "fleets": {}}

    with state_path.open(encoding="utf-8") as state_file:
        state = json.load(state_file)

    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        return {"version": STATE_VERSION, "nodes": {}, "fleets": {}}

    state.setdefault("nodes", {})
    state.setdefault("fleets", {})
    return state


def save_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    with tmp_path.open("w", encoding="utf-8") as state_file:
        json.dump(state, state_file, indent=2, sort_keys=True)
        state_file.write("\n")
    tmp_path.replace(state_path)


def fetch_json(url: str, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()

    decoded = json.loads(body.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError(f"expected JSON object from {url}")
    return decoded


def coerce_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"

    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m" if seconds == 0 else f"{minutes}m {seconds}s"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h" if minutes == 0 else f"{hours}h {minutes}m"


def suppression_until(path: Path) -> float | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as error:
        print(f"warning: could not read suppression file {path}: {error}", file=sys.stderr)
        return None

    try:
        return float(raw)
    except ValueError:
        print(f"warning: invalid suppression timestamp in {path}: {raw}", file=sys.stderr)
        return None


def post_slack(text: str, args: argparse.Namespace) -> bool:
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    webhook = (
        os.environ.get("SLACK_WEBHOOK_URL", "")
        or os.environ.get("SLACK_WEB_HOOK", "")
        or os.environ.get("SLACK_WEBHOOK", "")
    )
    if args.dry_run:
        print(f"dry-run Slack message:\n{text}\n")
        return True

    if webhook:
        return post_slack_webhook(webhook, text, args)

    if not token:
        print(f"Slack credential not set; would post:\n{text}\n")
        return True

    payload = json.dumps({"channel": args.slack_channel, "text": text}).encode("utf-8")
    request = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=args.slack_timeout) as response:
            body = response.read()
    except (OSError, urllib.error.URLError) as error:
        print(f"Slack post failed: {error}", file=sys.stderr)
        return False

    try:
        decoded = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as error:
        print(f"Slack returned invalid JSON: {error}", file=sys.stderr)
        return False

    if not decoded.get("ok"):
        print(f"Slack post failed: {decoded}", file=sys.stderr)
        return False

    return True


def post_slack_webhook(webhook: str, text: str, args: argparse.Namespace) -> bool:
    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=args.slack_timeout) as response:
            body = response.read().decode("utf-8", errors="replace").strip()
    except (OSError, urllib.error.URLError) as error:
        print(f"Slack webhook post failed: {error}", file=sys.stderr)
        return False

    if response.status < 200 or response.status >= 300 or body != "ok":
        print(
            f"Slack webhook post failed: status={response.status} body={body}",
            file=sys.stderr,
        )
        return False

    return True


def node_condition(
    row: dict[str, Any],
    now: float,
    grace_since: float,
    args: argparse.Namespace,
) -> tuple[str, float, float]:
    health = str(row.get("health") or "unknown")
    seconds_since_advanced = coerce_float(row.get("seconds_since_advanced"))

    if health == "starting" and now - grace_since < args.starting_grace:
        return ("ok", now, 0)

    if health in DOWN_HEALTH:
        return ("down", now, args.down_after)

    if (
        seconds_since_advanced is not None
        and seconds_since_advanced >= args.stalled_after
    ):
        return ("stalled", now - seconds_since_advanced, args.stalled_after)

    return ("ok", now, 0)


def update_alert_state(
    state_bucket: dict[str, Any],
    key: str,
    condition: str,
    bad_since: float,
    threshold: float,
    alert_text: str,
    recovery_text: str,
    now: float,
    suppressed: bool,
    args: argparse.Namespace,
) -> None:
    entry = state_bucket.get(key, {"condition": "ok", "alerting": False})
    was_alerting = bool(entry.get("alerting"))

    if condition == "ok":
        if was_alerting:
            if post_slack(recovery_text, args):
                state_bucket[key] = {"condition": "ok", "alerting": False}
            return

        state_bucket[key] = {"condition": "ok", "alerting": False}
        return

    if entry.get("condition") == condition:
        bad_since = min(float(entry.get("bad_since", bad_since)), bad_since)
        alerting = was_alerting
    else:
        alerting = False

    age = now - bad_since
    next_entry = {
        "condition": condition,
        "bad_since": bad_since,
        "alerting": alerting,
        "last_seen": now,
    }

    if not alerting and age >= threshold:
        if suppressed:
            print(f"suppressed alert for {key}: {condition} for {format_duration(age)}")
        elif post_slack(alert_text, args):
            next_entry["alerting"] = True
            next_entry["last_alert_at"] = now

    state_bucket[key] = next_entry


def node_alert_text(fleet: Fleet, row: dict[str, Any], condition: str, age: float) -> str:
    name = row.get("name") or "unknown"
    health = row.get("health") or "unknown"
    height = row.get("height")
    detail = row.get("detail") or "no detail"
    height_text = str(height) if height is not None else "-"

    return (
        f":rotating_light: *Zakura {fleet.name}* - `{name}` {condition} "
        f"for {format_duration(age)}\n"
        f"health: {health} - height: {height_text} - detail: {detail}\n"
        f"dashboard: {fleet.dashboard_url}"
    )


def node_recovery_text(fleet: Fleet, row: dict[str, Any], previous: dict[str, Any]) -> str:
    name = row.get("name") or "unknown"
    condition = previous.get("condition") or "unhealthy"
    height = row.get("height")
    height_text = str(height) if height is not None else "-"

    return (
        f":white_check_mark: *Zakura {fleet.name}* - `{name}` recovered "
        f"from {condition}\n"
        f"health: {row.get('health') or 'unknown'} - height: {height_text}\n"
        f"dashboard: {fleet.dashboard_url}"
    )


def fleet_alert_text(fleet: Fleet, error: Exception, age: float) -> str:
    return (
        f":rotating_light: *Zakura {fleet.name}* dashboard unreachable "
        f"for {format_duration(age)}\n"
        f"endpoint: {fleet.url}\n"
        f"error: {error}"
    )


def fleet_recovery_text(fleet: Fleet, previous: dict[str, Any]) -> str:
    condition = previous.get("condition") or "unreachable"
    return (
        f":white_check_mark: *Zakura {fleet.name}* dashboard recovered "
        f"from {condition}\n"
        f"endpoint: {fleet.url}"
    )


class Watchdog:
    def __init__(self, fleets: list[Fleet], args: argparse.Namespace):
        self.fleets = fleets
        self.args = args
        self.started_at = time.time()
        self.fetch_recovered_at: dict[str, float] = {}

    def run_once(self, state: dict[str, Any]) -> None:
        now = time.time()
        suppressed_until = suppression_until(self.args.suppression_file)
        suppressed = suppressed_until is not None and suppressed_until > now

        for fleet in self.fleets:
            try:
                snapshot = fetch_json(fleet.url, self.args.request_timeout)
            except Exception as error:
                self.handle_fleet_error(state, fleet, error, now, suppressed)
                continue

            self.handle_fleet_recovered(state, fleet, now)
            rows = snapshot.get("rows", [])
            if not isinstance(rows, list):
                rows = []

            for row in rows:
                if isinstance(row, dict):
                    self.handle_node(state, fleet, row, now, suppressed)

    def handle_fleet_error(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        error: Exception,
        now: float,
        suppressed: bool,
    ) -> None:
        key = fleet.name
        bucket = state.setdefault("fleets", {})
        entry = bucket.get(key, {})
        bad_since = (
            float(entry.get("bad_since", now))
            if entry.get("condition") == "unreachable"
            else now
        )
        age = now - bad_since
        update_alert_state(
            bucket,
            key,
            "unreachable",
            bad_since,
            self.args.dashboard_down_after,
            fleet_alert_text(fleet, error, age),
            fleet_recovery_text(fleet, entry),
            now,
            suppressed,
            self.args,
        )

    def handle_fleet_recovered(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        now: float,
    ) -> None:
        key = fleet.name
        bucket = state.setdefault("fleets", {})
        previous = dict(bucket.get(key, {}))
        if previous.get("condition") == "unreachable":
            self.fetch_recovered_at[fleet.name] = now

        update_alert_state(
            bucket,
            key,
            "ok",
            now,
            0,
            "",
            fleet_recovery_text(fleet, previous),
            now,
            False,
            self.args,
        )

    def handle_node(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        row: dict[str, Any],
        now: float,
        suppressed: bool,
    ) -> None:
        node_name = str(row.get("name") or "unknown")
        key = f"{fleet.name}/{node_name}"
        bucket = state.setdefault("nodes", {})
        previous = dict(bucket.get(key, {}))
        grace_since = max(self.started_at, self.fetch_recovered_at.get(fleet.name, 0))
        condition, bad_since, threshold = node_condition(row, now, grace_since, self.args)
        if condition != "ok" and previous.get("condition") == condition:
            bad_since = min(float(previous.get("bad_since", bad_since)), bad_since)
        age = now - bad_since

        update_alert_state(
            bucket,
            key,
            condition,
            bad_since,
            threshold,
            node_alert_text(fleet, row, condition, age),
            node_recovery_text(fleet, row, previous),
            now,
            suppressed,
            self.args,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Alert Slack when Zakura fleet dashboard nodes stay unhealthy."
    )
    parser.add_argument("--config", required=True, type=Path, help="fleet TOML config")
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("/var/lib/zakura-fleet-watchdog/state.json"),
        help="JSON file used to persist alert state",
    )
    parser.add_argument("--interval", type=float, default=60.0, help="poll interval seconds")
    parser.add_argument(
        "--down-after",
        type=float,
        default=600.0,
        help="alert after down/rpc_error has persisted this many seconds",
    )
    parser.add_argument(
        "--stalled-after",
        type=float,
        default=600.0,
        help="alert after no block progress for this many seconds",
    )
    parser.add_argument(
        "--dashboard-down-after",
        type=float,
        default=600.0,
        help="alert after a dashboard fetch failure persists this many seconds",
    )
    parser.add_argument(
        "--starting-grace",
        type=float,
        default=120.0,
        help="ignore starting nodes for this many seconds after startup or fetch recovery",
    )
    parser.add_argument(
        "--suppression-file",
        type=Path,
        default=Path("/run/zakura-fleet-watchdog/deploy-suppressed-until"),
        help="Unix timestamp file that suppresses failure alerts while in the future",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=20.0,
        help="dashboard request timeout seconds",
    )
    parser.add_argument(
        "--slack-timeout",
        type=float,
        default=20.0,
        help="Slack API request timeout seconds",
    )
    parser.add_argument(
        "--slack-channel",
        default=os.environ.get("SLACK_CHANNEL_ID", DEFAULT_SLACK_CHANNEL),
        help="Slack channel ID for alerts",
    )
    parser.add_argument("--once", action="store_true", help="poll once, update state, and exit")
    parser.add_argument("--dry-run", action="store_true", help="log Slack messages instead")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fleets = load_fleets(args.config)
    watchdog = Watchdog(fleets, args)

    while True:
        state = load_state(args.state_file)
        watchdog.run_once(state)
        save_state(args.state_file, state)

        if args.once:
            return 0

        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
