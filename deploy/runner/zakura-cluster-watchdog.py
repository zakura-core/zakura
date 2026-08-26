#!/usr/bin/env python3
"""Slack watchdog for Zakura fleet status dashboards.

Polls one or more `zakura-cluster-status.py` `/data` endpoints, tracks sustained
node failures in a small JSON state file, and posts transition alerts to Slack.

Only the Python stdlib is used.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import tomllib
import urllib.error
import urllib.request
import datetime

from dataclasses import dataclass
from pathlib import Path
from typing import Any


DOWN_HEALTH = {"down", "rpc_error"}
STATE_VERSION = 1
MAX_SHARED_DIAGNOSTIC_ROWS = 8
STALL_PIPELINE_METRICS = (
    ("network tip", "sync_estimated_network_tip_height"),
    ("distance", "sync_estimated_distance_to_tip"),
    ("checkpoint next", "checkpoint_processing_next_height"),
    ("checkpoint verified", "checkpoint_verified_height"),
    ("state finalized", "state_finalized_block_height"),
    ("header best", "sync_header_chain_frontier_header_best_height"),
    ("header verified", "sync_header_chain_frontier_verified_best_height"),
    ("header finalized", "sync_header_chain_frontier_finalized_height"),
    ("header progress age", "sync_header_work_last_progress_age_seconds"),
    ("oldest missing", "sync_header_work_oldest_missing_height"),
    ("header in flight", "sync_header_work_in_flight_count"),
    ("block applying", "sync_block_applying"),
    ("block header tip", "sync_block_best_header_tip_height"),
    ("block verified tip", "sync_block_verified_tip_height"),
    ("block fill stop", "sync_block_fill_stop"),
    ("block outstanding", "sync_block_outstanding"),
    ("missing bodies", "sync_block_missing_bodies"),
)
STALL_REPAIR_METRICS = (
    ("stalled height", "state_vct_root_stalled_height"),
    ("state requests", "state_vct_root_repair_requested"),
    ("state retries", "state_vct_root_retry_count"),
    ("sweep frontier", "state_vct_aux_sweep_frontier_height"),
    ("requested", "sync_header_vct_repair_requested_total"),
    ("scheduled", "sync_header_vct_repair_scheduled_total"),
    ("admitted", "sync_header_vct_repair_admitted_total"),
    ("context unavailable", "sync_header_vct_repair_context_unavailable_total"),
    ("timed out", "sync_header_vct_repair_timed_out_total"),
    ("resource stalled", "sync_header_vct_repair_resource_stalled_total"),
)


@dataclass(frozen=True)
class Fleet:
    name: str
    url: str
    dashboard_url: str


@dataclass(frozen=True)
class SharedTip:
    height: int
    block_hash: str
    bad_since: float
    node_names: tuple[str, ...]


@dataclass(frozen=True)
class NodeObservation:
    name: str
    row: dict[str, Any]
    condition: str
    bad_since: float
    threshold: float
    height: int | None
    block_hash: str


@dataclass(frozen=True)
class ReleaseState:
    """One published release-state pointer to watch.

    The generator is a single unmonitored host on a daily timer, and the artifact it
    publishes is what every consumer actually reads. Watching the artifact rather than
    the unit catches a timer that silently stopped firing, a host that vanished, and a
    bundle that published successfully while missing a file — none of which a
    systemd OnFailure would see.
    """

    name: str
    url: str
    stale_after: float
    required_files: tuple[str, ...]


def load_release_state(config_path: Path) -> list[ReleaseState]:
    with config_path.open("rb") as config_file:
        data = tomllib.load(config_file)

    targets = []
    seen = set()
    for raw in data.get("release_state", []):
        for required in ("name", "url"):
            if required not in raw:
                raise SystemExit(
                    f"release_state missing required field '{required}': {raw}"
                )

        name = str(raw["name"])
        if name in seen:
            raise SystemExit(f"duplicate release_state name: {name}")
        seen.add(name)

        # Defaults match the importer: fetch-release-state.py rejects a bundle older
        # than 48h, so alerting at the same threshold fires before the weekly import
        # would refuse it rather than after.
        targets.append(
            ReleaseState(
                name=name,
                url=str(raw["url"]),
                stale_after=float(raw.get("stale_after", 172800)),
                required_files=tuple(
                    raw.get(
                        "required_files",
                        [
                            "main-checkpoints.txt",
                            "mainnet-frontier.bin",
                            "mainnet-treestate-subtrees.bin",
                            "mainnet-frontier-grid.bin",
                        ],
                    )
                ),
            )
        )

    return targets


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
        return {
            "version": STATE_VERSION,
            "nodes": {},
            "fleets": {},
            "shared_stalls": {},
        }

    with state_path.open(encoding="utf-8") as state_file:
        state = json.load(state_file)

    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        return {
            "version": STATE_VERSION,
            "nodes": {},
            "fleets": {},
            "shared_stalls": {},
        }

    state.setdefault("nodes", {})
    state.setdefault("fleets", {})
    state.setdefault("shared_stalls", {})
    state.setdefault("release_state", {})
    return state


def save_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    with tmp_path.open("w", encoding="utf-8") as state_file:
        json.dump(state, state_file, indent=2, sort_keys=True)
        state_file.write("\n")
    tmp_path.replace(state_path)


def fetch_json(url: str, timeout: float) -> dict[str, Any]:
    # A User-Agent is required, not cosmetic: the dashboard on loopback does not care,
    # but the public release-state origin sits behind an edge that answers urllib's
    # default agent with 403. Without this the release-state check reports
    # "unreachable" forever instead of watching the artifact.
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "zakura-cluster-watchdog",
        },
    )
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


def coerce_height(value: object) -> int | None:
    height = coerce_float(value)
    if height is None or height < 0 or not height.is_integer():
        return None
    return int(height)


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"

    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m" if seconds == 0 else f"{minutes}m {seconds}s"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h" if minutes == 0 else f"{hours}h {minutes}m"


def format_metric(value: object) -> str | None:
    number = coerce_float(value)
    if number is None or not math.isfinite(number):
        return None
    if number.is_integer():
        return str(int(number))
    return f"{number:.2f}".rstrip("0").rstrip(".")


def named_metrics(
    source: dict[str, Any], fields: tuple[tuple[str, str], ...]
) -> str:
    values = []
    for label, key in fields:
        value = format_metric(source.get(key))
        if value is not None:
            values.append(f"{label} {value}")
    return " | ".join(values)


def bounded_text(value: object, limit: int) -> str:
    return " ".join(str(value or "").split())[:limit]


def alert_metrics(row: dict[str, Any]) -> tuple[dict[str, Any], bool | None]:
    diagnostics = row.get("alert_diagnostics")
    if not isinstance(diagnostics, dict):
        return {}, None
    metrics = diagnostics.get("metrics")
    if not isinstance(metrics, dict):
        metrics = {}
    available = diagnostics.get("metrics_available")
    return metrics, available if isinstance(available, bool) else None


def node_diagnostic_lines(row: dict[str, Any]) -> list[str]:
    identity = named_metrics(
        row,
        (
            ("headers", "headers"),
            ("header lag", "header_lag"),
            ("peers", "peer_count"),
        ),
    )
    commit = bounded_text(row.get("commit"), 12)
    version = bounded_text(row.get("version"), 64)
    build = " | ".join(
        value
        for value in (
            f"commit {commit}" if commit else "",
            f"version {version}" if version else "",
        )
        if value
    )
    metrics, available = alert_metrics(row)
    pipeline = named_metrics(metrics, STALL_PIPELINE_METRICS)
    repair = named_metrics(metrics, STALL_REPAIR_METRICS)

    lines = []
    if identity or build:
        lines.append("node: " + " | ".join(value for value in (identity, build) if value))
    if pipeline:
        lines.append(f"pipeline: {pipeline}")
    if repair:
        lines.append(f"repair: {repair}")
    if not pipeline and not repair:
        if available is False:
            lines.append("metrics: unavailable")
        elif available is None:
            lines.append("metrics: absent from fleet snapshot")
        else:
            lines.append("metrics: no alert diagnostics emitted")
    return lines


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


def slack_webhook_url() -> str:
    """Return the configured incoming webhook URL for #zakura-alerts.

    Bot tokens are intentionally unsupported: a token without channel
    membership fails with `not_in_channel` and previously masked webhook
    misconfiguration.
    """
    return (
        os.environ.get("SLACK_WEB_HOOK", "")
        or os.environ.get("SLACK_WEBHOOK_URL", "")
        or os.environ.get("SLACK_WEBHOOK", "")
    )


def post_slack(text: str, args: argparse.Namespace) -> bool:
    webhook = slack_webhook_url()
    if args.dry_run:
        print(f"dry-run Slack message:\n{text}\n")
        return True

    if not webhook:
        print(
            "SLACK_WEB_HOOK (or SLACK_WEBHOOK_URL / SLACK_WEBHOOK) is not set; "
            f"cannot post:\n{text}\n",
            file=sys.stderr,
        )
        return False

    return post_slack_webhook(webhook, text, args)


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


def tip_is_observable(row: dict[str, Any]) -> bool:
    health = str(row.get("health") or "unknown")
    return health not in DOWN_HEALTH and health != "starting"


def normalized_block_hash(value: object) -> str:
    return str(value or "").strip().casefold()


def shared_stall_candidate(
    rows: list[dict[str, Any]],
    now: float,
) -> SharedTip | None:
    """Return a common verifiable tip before individual stall alerts become due."""
    observed: list[tuple[int, str, float, str]] = []

    for row in rows:
        if not tip_is_observable(row):
            continue

        height = coerce_height(row.get("height"))
        block_hash = normalized_block_hash(row.get("block_hash"))
        seconds_since_advanced = coerce_float(row.get("seconds_since_advanced"))
        node_name = str(row.get("name") or "unknown")
        if height is None or not block_hash or seconds_since_advanced is None:
            return None
        observed.append((height, block_hash, now - seconds_since_advanced, node_name))

    if len(observed) < 2 or len({item[3] for item in observed}) != len(observed):
        return None

    identities = {(height, block_hash) for height, block_hash, *_rest in observed}
    if len(identities) != 1:
        return None

    height, block_hash = next(iter(identities))
    return SharedTip(
        height=height,
        block_hash=block_hash,
        # The shared interval starts when the last node reached this tip.
        bad_since=max(bad_since for _height, _hash, bad_since, _name in observed),
        node_names=tuple(name for _height, _hash, _bad_since, name in observed),
    )


def classify_node_observations(
    rows: list[dict[str, Any]],
    now: float,
    grace_since: float,
    args: argparse.Namespace,
) -> tuple[NodeObservation, ...]:
    observations = []
    for row in rows:
        condition, bad_since, threshold = node_condition(
            row, now, grace_since, args
        )
        observations.append(
            NodeObservation(
                name=str(row.get("name") or "unknown"),
                row=row,
                condition=condition,
                bad_since=bad_since,
                threshold=threshold,
                height=coerce_height(row.get("height")),
                block_hash=normalized_block_hash(row.get("block_hash")),
            )
        )
    return tuple(observations)


def stall_cleared(entry: dict[str, Any], height: float | None) -> bool:
    """True when a stalled alert may be retired.

    A stall is only over once the node is strictly higher than it was when the
    alert fired. The dashboard's stall timer lives in memory, so any restart
    reports every node as freshly advanced and would otherwise clear the alert
    at an unchanged height.
    """
    if entry.get("condition") != "stalled":
        return True
    alert_height = coerce_float(entry.get("alert_height"))
    if alert_height is None:
        return True
    return height is not None and height > alert_height


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
    height: float | None = None,
    log_suppressed: bool = True,
) -> None:
    entry = state_bucket.get(key, {"condition": "ok", "alerting": False})
    was_alerting = bool(entry.get("alerting"))

    if condition == "ok":
        if was_alerting:
            if not stall_cleared(entry, height):
                # Keep the alert latched; the timer reset but the node did not move.
                anchor = coerce_float(entry.get("alert_height"))
                if height is not None and anchor is not None and height < anchor:
                    # The node was wiped, rolled back, or restarted onto a shorter
                    # chain. Follow it down: anchored at the old tip the alert could
                    # only clear once it re-synced past it, so a node that was fixed
                    # by a resync would never post a recovery.
                    entry = {**entry, "alert_height": height}
                    if "event_height" in entry:
                        entry["event_height"] = height
                state_bucket[key] = entry
                return
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

    if alerting:
        anchor = entry.get("alert_height")
        if anchor is None and condition == "stalled":
            # The alert fired on a sample with no usable height, so it has nothing
            # to compare against and would clear on the first reset timer. Anchor
            # on the first sample that does report one.
            anchor = height
        if anchor is not None:
            next_entry["alert_height"] = anchor

    if not alerting and age >= threshold:
        if suppressed:
            if log_suppressed:
                print(f"suppressed alert for {key}: {condition} for {format_duration(age)}")
        elif post_slack(alert_text, args):
            next_entry["alerting"] = True
            next_entry["last_alert_at"] = now
            if condition == "stalled" and height is not None:
                # Anchor recovery to this height, not to the dashboard's timer.
                next_entry["alert_height"] = height

    state_bucket[key] = next_entry


def node_alert_text(fleet: Fleet, row: dict[str, Any], condition: str, age: float) -> str:
    name = row.get("name") or "unknown"
    health = row.get("health") or "unknown"
    height = row.get("height")
    detail = row.get("detail") or "no detail"
    height_text = str(height) if height is not None else "-"

    lines = [
        f":rotating_light: *Zakura {fleet.name}* - `{name}` {condition} "
        f"for {format_duration(age)}",
        f"health: {health} - height: {height_text} - detail: {detail}",
        *node_diagnostic_lines(row),
        f"dashboard: {fleet.dashboard_url}",
    ]
    return "\n".join(lines)


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


def shared_stall_alert_text(
    fleet: Fleet,
    height: float,
    block_hash: str,
    node_count: int,
    age: float,
    rows: list[dict[str, Any]],
) -> str:
    lines = [
        f":rotating_light: *Zakura {fleet.name}* network height has not advanced "
        f"for {format_duration(age)}",
        f"{node_count} nodes agree at height {int(height)}",
        f"tip hash: {block_hash}",
    ]
    for row in rows[:MAX_SHARED_DIAGNOSTIC_ROWS]:
        summary = named_metrics(
            row,
            (
                ("height", "height"),
                ("headers", "headers"),
                ("header lag", "header_lag"),
                ("peers", "peer_count"),
            ),
        )
        metrics, _available = alert_metrics(row)
        pipeline = named_metrics(
            metrics,
            (
                ("checkpoint", "checkpoint_verified_height"),
                ("network tip", "sync_estimated_network_tip_height"),
                ("distance", "sync_estimated_distance_to_tip"),
                ("applying", "sync_block_applying"),
                ("outstanding", "sync_block_outstanding"),
                ("missing", "sync_block_missing_bodies"),
            ),
        )
        commit = bounded_text(row.get("commit"), 12)
        summary = " | ".join(
            value
            for value in (
                summary,
                pipeline,
                f"commit {commit}" if commit else "",
            )
            if value
        )
        name = bounded_text(row.get("name") or "unknown", 64)
        lines.append(f"- {name}: {summary or 'no diagnostics'}")
    hidden = max(0, node_count - MAX_SHARED_DIAGNOSTIC_ROWS)
    if hidden:
        lines.append(f"- {hidden} more participating nodes not shown")
    lines.append(f"dashboard: {fleet.dashboard_url}")
    return "\n".join(lines)


def shared_stall_recovery_text(
    fleet: Fleet, height: float | None, detail: str = "network height advanced"
) -> str:
    height_text = str(int(height)) if height is not None else "-"
    return (
        f":white_check_mark: *Zakura {fleet.name}* shared stall cleared\n"
        f"detail: {detail}\n"
        f"height: {height_text}\n"
        f"dashboard: {fleet.dashboard_url}"
    )


def release_state_condition(
    target: ReleaseState, pointer: dict[str, Any], meta: dict[str, Any], now: float
) -> tuple[str, str]:
    """Classify a published release state, returning (condition, detail).

    Missing files are reported before staleness because a bundle can be perfectly
    fresh and still useless: that is exactly what a generator exporting from the wrong
    database produces, and no freshness check would notice.
    """

    # An unreadable file list is reported, never skipped. Treating it as a pass would
    # make a pointer with no usable meta_url, or a meta fetch that half-failed, look
    # exactly like a healthy bundle — the one outcome monitoring must never produce.
    files = meta.get("files")
    if not isinstance(files, dict):
        return "unreadable", "meta.files is missing or not an object"

    missing = [name for name in target.required_files if name not in files]
    if missing:
        return "incomplete", "missing " + ", ".join(sorted(missing))

    generated_at = pointer.get("generated_at")
    age = pointer_age_seconds(generated_at, now)
    if age is None:
        return "unreadable", f"unparsable generated_at: {generated_at!r}"
    if age > target.stale_after:
        return "stale", f"published {format_duration(age)} ago"

    return "ok", ""


def pointer_age_seconds(generated_at: Any, now: float) -> float | None:
    if not isinstance(generated_at, str):
        return None
    try:
        stamp = datetime.datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=datetime.timezone.utc)
    return now - stamp.timestamp()


def release_state_alert_text(
    target: ReleaseState, condition: str, detail: str, height: Any
) -> str:
    return (
        f":rotating_light: *Zakura {target.name} release state* {condition}\n"
        f"{detail}\n"
        f"pointer: {target.url}\n"
        f"height: {height}"
    )


def release_state_recovery_text(target: ReleaseState, previous: dict[str, Any]) -> str:
    condition = previous.get("condition") or "unhealthy"
    return (
        f":white_check_mark: *Zakura {target.name} release state* recovered "
        f"from {condition}\n"
        f"pointer: {target.url}"
    )


class Watchdog:
    def __init__(
        self,
        fleets: list[Fleet],
        args: argparse.Namespace,
        release_state: list[ReleaseState] | None = None,
    ):
        self.fleets = fleets
        self.release_state = release_state or []
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

            if not self.handle_fleet_recovered(state, fleet, now):
                continue
            rows = snapshot.get("rows", [])
            if not isinstance(rows, list):
                rows = []

            node_rows = [row for row in rows if isinstance(row, dict)]
            grace_since = max(
                self.started_at, self.fetch_recovered_at.get(fleet.name, 0)
            )
            observations = classify_node_observations(
                node_rows, now, grace_since, self.args
            )
            common_stall = shared_stall_candidate(node_rows, now)
            if not self.reconcile_obsolete_node_alerts(
                state, fleet, observations
            ):
                continue
            if not self.reconcile_duplicate_owners(
                state, fleet, observations, common_stall
            ):
                continue
            reconciled, shared_nodes = self.reconcile_shared_stall(
                state,
                fleet,
                node_rows,
                observations,
                common_stall,
                now,
                suppressed,
            )
            if not reconciled:
                continue

            for observation in observations:
                self.handle_node_observation(
                    state,
                    fleet,
                    observation,
                    now,
                    suppressed,
                    observation.condition == "stalled"
                    and observation.name in shared_nodes,
                )

        for target in self.release_state:
            self.handle_release_state(state, target, now, suppressed)

    def handle_release_state(
        self,
        state: dict[str, Any],
        target: ReleaseState,
        now: float,
        suppressed: bool,
    ) -> None:
        bucket = state.setdefault("release_state", {})
        key = target.name
        entry = bucket.get(key, {})
        height: Any = entry.get("height")

        try:
            pointer = fetch_json(target.url, self.args.request_timeout)
            meta_url = pointer.get("meta_url")
            if not isinstance(meta_url, str):
                raise ValueError(f"pointer has no meta_url: {target.url}")
            meta = fetch_json(meta_url, self.args.request_timeout)
            height = pointer.get("height", height)
            condition, detail = release_state_condition(target, pointer, meta, now)
        except Exception as error:
            condition, detail = "unreachable", str(error)

        bad_since = (
            float(entry.get("bad_since", now))
            if entry.get("condition") == condition and condition != "ok"
            else now
        )

        # Threshold zero: a bundle that is already past its staleness window, or is
        # missing a file, is not a transient blip to wait out. The window itself is
        # the grace period.
        update_alert_state(
            bucket,
            key,
            condition,
            bad_since,
            0.0,
            release_state_alert_text(target, condition, detail, height),
            release_state_recovery_text(target, entry),
            now,
            suppressed,
            self.args,
        )
        bucket.setdefault(key, {})["height"] = height

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
    ) -> bool:
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
        return not (
            previous.get("alerting")
            and bucket.get(key, {}).get("alerting")
        )

    @staticmethod
    def node_event_height(entry: dict[str, Any]) -> int | None:
        return coerce_height(entry.get("event_height", entry.get("alert_height")))

    @classmethod
    def node_alert_matches_observation(
        cls, entry: dict[str, Any], observation: NodeObservation
    ) -> bool:
        previous_condition = entry.get("condition")
        if previous_condition == observation.condition:
            if observation.condition != "stalled":
                return True
            previous_height = cls.node_event_height(entry)
            return (
                previous_height is None
                or observation.height is None
                or previous_height == observation.height
            )

        if previous_condition == "stalled" and observation.condition == "ok":
            return not stall_cleared(entry, observation.height)
        return False

    def reconcile_obsolete_node_alerts(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        observations: tuple[NodeObservation, ...],
    ) -> bool:
        bucket = state.setdefault("nodes", {})
        for observation in observations:
            key = f"{fleet.name}/{observation.name}"
            previous = dict(bucket.get(key, {}))
            if not previous.get("alerting"):
                continue
            if self.node_alert_matches_observation(previous, observation):
                continue
            if not post_slack(
                node_recovery_text(fleet, observation.row, previous), self.args
            ):
                return False
            bucket[key] = {"condition": "ok", "alerting": False}
        return True

    @staticmethod
    def shared_event_identity(entry: dict[str, Any]) -> tuple[int | None, str]:
        height = coerce_height(entry.get("event_height", entry.get("alert_height")))
        return height, normalized_block_hash(entry.get("event_hash"))

    @classmethod
    def shared_event_matches_tip(
        cls, entry: dict[str, Any], common_stall: SharedTip
    ) -> bool:
        height, block_hash = cls.shared_event_identity(entry)
        return (
            entry.get("condition") == "stalled"
            and height == common_stall.height
            and (not block_hash or block_hash == common_stall.block_hash)
        )

    @staticmethod
    def observation_matches_shared_event(
        observation: NodeObservation,
        height: int | None,
        block_hash: str,
    ) -> bool:
        return (
            observation.condition == "stalled"
            and height is not None
            and observation.height == height
            and (not block_hash or observation.block_hash == block_hash)
        )

    @classmethod
    def active_node_owners(
        cls,
        state: dict[str, Any],
        fleet: Fleet,
        observations: tuple[NodeObservation, ...],
        height: int | None,
        block_hash: str,
    ) -> list[NodeObservation]:
        bucket = state.setdefault("nodes", {})
        owners = []
        for observation in observations:
            entry = bucket.get(f"{fleet.name}/{observation.name}", {})
            alert_height = cls.node_event_height(entry)
            if (
                entry.get("condition") == "stalled"
                and entry.get("alerting")
                and (alert_height is None or alert_height == height)
                and observation.height == height
                and (not block_hash or observation.block_hash == block_hash)
                and cls.node_alert_matches_observation(
                    entry, observation
                )
            ):
                owners.append(observation)
        return sorted(owners, key=lambda observation: observation.name)

    def reconcile_duplicate_owners(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        observations: tuple[NodeObservation, ...],
        common_stall: SharedTip | None,
    ) -> bool:
        shared_bucket = state.setdefault("shared_stalls", {})
        shared_entry = dict(shared_bucket.get(fleet.name, {}))
        shared_height, shared_hash = self.shared_event_identity(shared_entry)

        if shared_entry.get("alerting"):
            node_owners = self.active_node_owners(
                state,
                fleet,
                observations,
                shared_height,
                shared_hash,
            )
            if node_owners:
                if not post_slack(
                    shared_stall_recovery_text(
                        fleet,
                        shared_height,
                        "constituent node alert continues to represent this incident",
                    ),
                    self.args,
                ):
                    return False
                shared_bucket[fleet.name] = {
                    "condition": "ok",
                    "alerting": False,
                }

        if common_stall is None:
            return True

        node_owners = self.active_node_owners(
            state,
            fleet,
            observations,
            common_stall.height,
            common_stall.block_hash,
        )
        if len(node_owners) < 2:
            return True

        owner = node_owners[0]
        bucket = state.setdefault("nodes", {})
        for duplicate in node_owners[1:]:
            text = (
                f":white_check_mark: *Zakura {fleet.name}* - `{duplicate.name}` "
                "duplicate stall alert cleared\n"
                f"detail: `{owner.name}` continues to represent the shared incident\n"
                f"height: {common_stall.height}\n"
                f"dashboard: {fleet.dashboard_url}"
            )
            if not post_slack(text, self.args):
                return False
            bucket[f"{fleet.name}/{duplicate.name}"] = {
                "condition": "ok",
                "alerting": False,
            }
        return True

    def reconcile_shared_stall(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        rows: list[dict[str, Any]],
        observations: tuple[NodeObservation, ...],
        common_stall: SharedTip | None,
        now: float,
        suppressed: bool,
    ) -> tuple[bool, set[str]]:
        bucket = state.setdefault("shared_stalls", {})
        previous = dict(bucket.get(fleet.name, {}))
        heights = [
            height
            for row in rows
            if tip_is_observable(row)
            if (height := coerce_height(row.get("height"))) is not None
        ]
        current_height = max(heights, default=None)

        if common_stall is None:
            return self.clear_or_hold_shared_stall(
                bucket,
                fleet,
                observations,
                previous,
                current_height,
            )

        previous_height, _previous_hash = self.shared_event_identity(previous)
        same_event = self.shared_event_matches_tip(previous, common_stall)

        if not same_event:
            recovery_detail = (
                "network height advanced"
                if previous_height is not None
                and common_stall.height > previous_height
                else "shared tip changed"
            )
            if previous.get("alerting") and not post_slack(
                shared_stall_recovery_text(
                    fleet, common_stall.height, recovery_detail
                ),
                self.args,
            ):
                return False, set()
            previous = {}

        bad_since = common_stall.bad_since
        timer_floor = bad_since
        if same_event:
            previous_bad_since = float(previous.get("bad_since", bad_since))
            previous_nodes = set(previous.get("node_names") or ())
            current_nodes = set(common_stall.node_names)
            if previous_nodes == current_nodes:
                timer_floor = float(
                    previous.get("timer_floor", previous_bad_since)
                )
                bad_since = max(
                    timer_floor,
                    min(previous_bad_since, bad_since),
                )
            else:
                # A changed constituent set starts a conservative timer boundary.
                # Persist the boundary so the next poll cannot backdate it.
                bad_since = max(previous_bad_since, bad_since)
                timer_floor = bad_since
        age = now - bad_since
        alerting = bool(previous.get("alerting"))
        node_owners = self.active_node_owners(
            state,
            fleet,
            observations,
            common_stall.height,
            common_stall.block_hash,
        )
        node_owner = node_owners[0].name if node_owners else None
        next_entry = {
            "condition": "stalled",
            "event_height": common_stall.height,
            "event_hash": common_stall.block_hash,
            "node_names": list(common_stall.node_names),
            "bad_since": bad_since,
            "timer_floor": timer_floor,
            "alerting": alerting,
            "owner": f"node:{node_owner}" if node_owner else "shared",
            "last_seen": now,
        }
        if alerting:
            next_entry["alert_height"] = common_stall.height
            if "last_alert_at" in previous:
                next_entry["last_alert_at"] = previous["last_alert_at"]

        if not alerting and node_owner is None and age >= self.args.shared_stalled_after:
            if suppressed:
                if not previous.get("suppression_logged"):
                    print(
                        f"suppressed alert for {fleet.name}: shared stall for "
                        f"{format_duration(age)}"
                    )
                next_entry["suppression_logged"] = True
            else:
                participant_names = set(common_stall.node_names)
                participant_rows = [
                    row
                    for row in rows
                    if str(row.get("name") or "unknown") in participant_names
                ]
                alert_text = shared_stall_alert_text(
                    fleet,
                    common_stall.height,
                    common_stall.block_hash,
                    len(common_stall.node_names),
                    age,
                    participant_rows,
                )
                if post_slack(alert_text, self.args):
                    next_entry["alerting"] = True
                    next_entry["last_alert_at"] = now
                    next_entry["alert_height"] = common_stall.height

        bucket[fleet.name] = next_entry
        return True, set(common_stall.node_names)

    def clear_or_hold_shared_stall(
        self,
        bucket: dict[str, Any],
        fleet: Fleet,
        observations: tuple[NodeObservation, ...],
        previous: dict[str, Any],
        current_height: int | None,
    ) -> tuple[bool, set[str]]:
        if not previous.get("alerting"):
            bucket[fleet.name] = {"condition": "ok", "alerting": False}
            return True, set()

        previous_height = coerce_height(
            previous.get("event_height", previous.get("alert_height"))
        )
        eligible_count = sum(
            tip_is_observable(observation.row) for observation in observations
        )
        if current_height is not None and (
            previous_height is None or current_height > previous_height
        ):
            detail = "network height advanced"
        elif eligible_count >= 2:
            detail = "nodes no longer share one verifiable tip"
        else:
            previous["owner"] = "shared"
            bucket[fleet.name] = previous
            previous_hash = normalized_block_hash(previous.get("event_hash"))
            owned_nodes = {
                observation.name
                for observation in observations
                if self.observation_matches_shared_event(
                    observation, previous_height, previous_hash
                )
            }
            return True, owned_nodes

        if not post_slack(
            shared_stall_recovery_text(fleet, current_height, detail), self.args
        ):
            return False, set()
        bucket[fleet.name] = {"condition": "ok", "alerting": False}
        return True, set()

    def handle_node_observation(
        self,
        state: dict[str, Any],
        fleet: Fleet,
        observation: NodeObservation,
        now: float,
        suppressed: bool,
        coalesced: bool = False,
    ) -> None:
        key = f"{fleet.name}/{observation.name}"
        bucket = state.setdefault("nodes", {})
        previous = dict(bucket.get(key, {}))
        previous_height = self.node_event_height(previous)
        same_stall_event = (
            observation.condition == "stalled"
            and previous.get("condition") == "stalled"
            and (
                previous_height is None
                or observation.height is None
                or previous_height == observation.height
            )
        )
        if (
            observation.condition == "stalled"
            and previous.get("condition") == "stalled"
            and not same_stall_event
        ):
            previous = {}
            bucket[key] = {"condition": "ok", "alerting": False}

        bad_since = observation.bad_since
        if (
            observation.condition != "ok"
            and previous.get("condition") == observation.condition
        ):
            bad_since = min(float(previous.get("bad_since", bad_since)), bad_since)
        age = now - bad_since

        update_alert_state(
            bucket,
            key,
            observation.condition,
            bad_since,
            observation.threshold,
            node_alert_text(fleet, observation.row, observation.condition, age),
            node_recovery_text(fleet, observation.row, previous),
            now,
            suppressed or coalesced,
            self.args,
            observation.height,
            log_suppressed=not coalesced,
        )
        if observation.condition == "stalled":
            entry = bucket.get(key, {})
            if entry.get("condition") == "stalled" and observation.height is not None:
                entry["event_height"] = observation.height


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
        "--shared-stalled-after",
        type=float,
        default=1800.0,
        help="alert after every observable node shares one stalled tip this long",
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
        help="Slack webhook request timeout seconds",
    )
    parser.add_argument("--once", action="store_true", help="poll once, update state, and exit")
    parser.add_argument("--dry-run", action="store_true", help="log Slack messages instead")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fleets = load_fleets(args.config)
    watchdog = Watchdog(fleets, args, load_release_state(args.config))

    while True:
        state = load_state(args.state_file)
        watchdog.run_once(state)
        save_state(args.state_file, state)

        if args.once:
            return 0

        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
