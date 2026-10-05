"""Fleet-side compatibility lane: bounded SSH probes of zakura-compat.

The fleet watchdog's main thread owns all state and delivery. This module only
schedules probes on one worker thread (at most one in flight), bounds each
probe with a hard timeout covering SSH and RPC time, and turns the remote,
untrusted output into a typed :class:`ProbeResult`.

Anything other than a complete, fresh, well-formed outcome for the probe that
was launched (an SSH timeout or failure, a missing checker, malformed, stale or
oversized output, an overrunning worker) is a monitoring failure. Such a result
can open or continue an incident but can never recover one.
"""

from __future__ import annotations

import datetime
import json
import math
import queue
import re
import secrets
import threading
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from . import compat, remote, slack, suppression


ENABLE_ENV = "ZAKURA_COMPAT_MONITORING"
COMPAT_STATE = "compatibility"
COMPAT_PROBES = "compatibility_probes"
COMPAT_QUEUE = "compatibility_pending_delivery"
COMPAT_BATCH_TITLE = "Zakura compatibility updates"

# The single supported target. The deploy workflow pins the same host key.
KNOWN_TARGETS = {"zakura-compat": "root@159.203.113.196"}
DEFAULT_CHECKER = "/opt/zakura-monitoring/current/zakura-compat-check"
DEFAULT_ENV_FILE = "/etc/zakura-monitoring/compat.env"
DEFAULT_KNOWN_HOSTS = "/etc/zakura-fleet-watchdog/known_hosts"
DEFAULT_INTERVAL = 60.0
MAX_PROBE_TIMEOUT = 120.0
DEFAULT_REMOTE_DEADLINE = 100
MAX_OUTCOME_BYTES = 16 * 1024
CLOCK_SKEW_SECONDS = 60.0
OVERRUN_GRACE_SECONDS = 15.0
UNAVAILABLE = "monitoring_unavailable"
SSH_TARGET = re.compile(r"[a-z_][a-z0-9_-]{0,31}@(?:[0-9]{1,3}\.){3}[0-9]{1,3}")

UNAVAILABLE_REASONS = {
    "ssh_timeout": "SSH probe exceeded its hard timeout",
    "ssh_failed": "SSH connection or authentication failed",
    "checker_missing": "checker is not installed on the host",
    "checker_config_invalid": "checker configuration is invalid",
    "malformed_outcome": "checker output was malformed",
    "stale_outcome": "checker outcome was stale",
    "oversized_outcome": "checker output exceeded its size limit",
    "probe_overrun": "probe worker overran its hard timeout",
    "probe_failed": "probe worker failed",
}
SUMMARIES = {
    "zakurad_process": "zakurad process is not running",
    "zcashd_process": "zcashd process is not running",
    "zcashd_getconnectioncount": "zcashd getconnectioncount RPC failed",
    "peer_pinning": "sidecar zcashd must peer with exactly one Zakura node",
    "zakura_getblockcount": "zakurad getblockcount RPC failed",
    "zcashd_getblockcount": "zcashd getblockcount RPC failed",
    "height_drift": "height drift exceeds the configured maximum",
    "deadline": "checker deadline expired before the cycle completed",
    "in_sync": "zakurad and zcashd are in sync",
    UNAVAILABLE: "compatibility probe produced no valid outcome",
}


@dataclass(frozen=True)
class CompatTarget:
    name: str
    ssh_target: str
    checker: str = DEFAULT_CHECKER
    env_file: str = DEFAULT_ENV_FILE
    known_hosts: Path | None = Path(DEFAULT_KNOWN_HOSTS)
    identity_file: Path | None = None
    interval: float = DEFAULT_INTERVAL
    timeout: float = MAX_PROBE_TIMEOUT
    remote_deadline: int = DEFAULT_REMOTE_DEADLINE


def load_compatibility_targets(config_path: Path) -> list[CompatTarget]:
    """Load the one explicit ``[[compatibility]]`` target from the fleet config."""
    with config_path.open("rb") as config_file:
        data = tomllib.load(config_file)
    raw_targets = data.get("compatibility", [])
    if not raw_targets:
        return []
    if not isinstance(raw_targets, list) or len(raw_targets) != 1:
        raise SystemExit("exactly one [[compatibility]] target is supported")
    raw = raw_targets[0]
    allowed = {
        "name", "ssh_target", "checker", "env_file", "known_hosts", "identity_file",
        "interval", "timeout", "remote_deadline",
    }
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise SystemExit(f"compatibility target has unknown fields: {', '.join(unknown)}")
    name = str(raw.get("name", ""))
    ssh_target = str(raw.get("ssh_target", ""))
    if KNOWN_TARGETS.get(name) != ssh_target or not SSH_TARGET.fullmatch(ssh_target):
        raise SystemExit(
            "compatibility target must be zakura-compat at its known host "
            f"{KNOWN_TARGETS['zakura-compat']}"
        )
    checker = str(raw.get("checker", DEFAULT_CHECKER))
    env_file = str(raw.get("env_file", DEFAULT_ENV_FILE))
    for label, value in (("checker", checker), ("env_file", env_file)):
        if not value.startswith("/") or any(char.isspace() for char in value):
            raise SystemExit(f"compatibility {label} must be an absolute path")
    interval = float(raw.get("interval", DEFAULT_INTERVAL))
    timeout = float(raw.get("timeout", MAX_PROBE_TIMEOUT))
    remote_deadline = int(raw.get("remote_deadline", DEFAULT_REMOTE_DEADLINE))
    if not (math.isfinite(interval) and interval >= DEFAULT_INTERVAL):
        raise SystemExit("compatibility interval must be at least 60 seconds")
    if not (0 < timeout <= MAX_PROBE_TIMEOUT):
        raise SystemExit("compatibility timeout must be in (0, 120] seconds")
    if not 0 < remote_deadline < timeout:
        raise SystemExit("compatibility remote_deadline must be shorter than timeout")

    def optional_path(key: str, default: str | None) -> Path | None:
        value = raw.get(key, default)
        return Path(str(value)) if value else None

    return [
        CompatTarget(
            name=name,
            ssh_target=ssh_target,
            checker=checker,
            env_file=env_file,
            known_hosts=optional_path("known_hosts", DEFAULT_KNOWN_HOSTS),
            identity_file=optional_path("identity_file", None),
            interval=interval,
            timeout=timeout,
            remote_deadline=remote_deadline,
        )
    ]


@dataclass(frozen=True)
class ProbeResult:
    """A validated probe result. Only ``valid`` results with status pass recover."""

    valid: bool
    status: str
    predicate: str
    error_kind: str | None
    details: dict[str, int] = field(default_factory=dict)
    observed_at: float = 0.0
    suppression_state: str | None = None
    # Local-clock end of an active compatibility suppression window.
    suppressed_until: float | None = None

    @property
    def passed(self) -> bool:
        return self.valid and self.status == compat.PASS


def unavailable(reason: str, at: float) -> ProbeResult:
    return ProbeResult(False, compat.FAIL, UNAVAILABLE, reason, {}, at)


def finite_number(value: object) -> float | None:
    if type(value) not in (int, float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def parse_probe_output(
    result: remote.BoundedResult,
    nonce: str,
    started_at: float,
    finished_at: float,
) -> ProbeResult:
    """Validate untrusted checker output for the probe identified by ``nonce``."""
    if result.returncode is None:
        return unavailable("ssh_failed", finished_at)
    if result.timed_out:
        return unavailable("ssh_timeout", finished_at)
    if result.oversized:
        return unavailable("oversized_outcome", finished_at)
    try:
        outcome = json.loads(result.stdout.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        outcome = None
    if not isinstance(outcome, dict):
        if result.returncode == 255:
            return unavailable("ssh_failed", finished_at)
        if result.returncode in (2, 126, 127) and not result.stdout.strip():
            return unavailable("checker_missing", finished_at)
        return unavailable("malformed_outcome", finished_at)

    def malformed() -> ProbeResult:
        return unavailable("malformed_outcome", finished_at)

    if (
        outcome.get("schema") != compat.SCHEMA
        or outcome.get("check") != compat.CHECK_NAME
        or outcome.get("nonce") != nonce
    ):
        return malformed()
    if outcome.get("predicate") == "invalid_config" and result.returncode == 2:
        return unavailable("checker_config_invalid", finished_at)
    if result.returncode != 0:
        return malformed()

    status = outcome.get("status")
    predicate = outcome.get("predicate")
    error_kind = outcome.get("error_kind")
    details = outcome.get("details")
    if (
        status not in (compat.PASS, compat.FAIL)
        or predicate not in compat.PREDICATES
        or predicate == "invalid_config"
        or (error_kind is not None and error_kind not in compat.ERROR_KINDS)
        or not isinstance(details, dict)
        or any(
            key not in compat.DETAIL_KEYS or type(value) is not int or value < 0
            for key, value in details.items()
        )
    ):
        return malformed()

    observed_at = finite_number(outcome.get("observed_at"))
    if observed_at is None:
        return malformed()
    if not (
        started_at - CLOCK_SKEW_SECONDS <= observed_at <= finished_at + CLOCK_SKEW_SECONDS
    ):
        return unavailable("stale_outcome", finished_at)

    if status == compat.PASS:
        required = set(compat.DETAIL_KEYS)
        if (
            predicate != "in_sync"
            or error_kind is not None
            or not required <= set(details)
            or details["zcashd_connections"] != 1
            or details["height_drift"]
            != abs(details["zakura_height"] - details["zcashd_height"])
            or details["height_drift"] > details["height_max_drift"]
        ):
            return malformed()
    elif predicate == "in_sync":
        return malformed()

    marker = outcome.get("suppression")
    if not isinstance(marker, dict):
        return malformed()
    state = marker.get("state")
    until = marker.get("until")
    max_seconds = marker.get("max_seconds")
    if (
        state not in suppression.SUPPRESSION_STATES
        or marker.get("active") is not (state == suppression.ACTIVE)
        or (until is not None and type(until) is not int)
        or type(max_seconds) is not int
    ):
        return malformed()
    suppressed_until = None
    if state == suppression.ACTIVE:
        window = (until or 0) - observed_at
        limit = min(max_seconds, suppression.COMPAT_MAX_SUPPRESSION_SECONDS)
        # Re-check the bound here: a marker the host accepted with a larger
        # configured maximum still cannot mute the lane past 20 minutes.
        if 0 < window <= limit:
            suppressed_until = finished_at + window

    return ProbeResult(
        True,
        status,
        predicate,
        error_kind,
        dict(details),
        observed_at,
        state,
        suppressed_until,
    )


class ProbeWorker:
    """Schedules one bounded probe at a time on a single worker thread."""

    def __init__(
        self,
        target: CompatTarget,
        runner: Callable[..., remote.BoundedResult] = remote.run_bounded,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        nonce_factory: Callable[[], str] = lambda: secrets.token_hex(8),
    ):
        self.target = target
        self.runner = runner
        self.clock = clock
        self.monotonic = monotonic
        self.nonce_factory = nonce_factory
        self.starts = 0
        self._results: queue.SimpleQueue[ProbeResult] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self._started_mono = 0.0
        self._next_due = float("-inf")
        self._overrun_reported = False

    @property
    def in_flight(self) -> bool:
        return self._thread is not None

    def command(self, nonce: str) -> list[str]:
        remote_argv = [
            "python3", "-I", self.target.checker, "probe",
            "--env-file", self.target.env_file,
            "--deadline", str(self.target.remote_deadline),
            "--nonce", nonce,
        ]
        return remote.ssh_command(
            self.target.ssh_target,
            remote_argv,
            known_hosts=self.target.known_hosts,
            identity_file=self.target.identity_file,
        )

    def _probe(self, nonce: str, started_at: float) -> None:
        try:
            result = self.runner(self.command(nonce), self.target.timeout, MAX_OUTCOME_BYTES)
            self._results.put(parse_probe_output(result, nonce, started_at, self.clock()))
        except Exception:  # A worker failure is a monitoring failure, never a crash.
            self._results.put(unavailable("probe_failed", self.clock()))

    def _start(self) -> None:
        nonce = self.nonce_factory()
        self._started_mono = self.monotonic()
        self._next_due = self._started_mono + self.target.interval
        self._overrun_reported = False
        self.starts += 1
        self._thread = threading.Thread(
            target=self._probe,
            args=(nonce, self.clock()),
            name=f"compat-probe-{self.target.name}",
            daemon=True,
        )
        self._thread.start()

    def poll(self) -> ProbeResult | None:
        """Collect a finished probe and start the next one when it is due.

        Never blocks. While a probe is in flight no other probe starts, even
        after the in-flight one overran; the overrun itself is reported once as
        a monitoring failure.
        """
        completed = None
        try:
            completed = self._results.get_nowait()
        except queue.Empty:
            pass
        if completed is not None:
            self._thread = None
        elif (
            self._thread is not None
            and not self._overrun_reported
            and self.monotonic() - self._started_mono
            > self.target.timeout + OVERRUN_GRACE_SECONDS
        ):
            self._overrun_reported = True
            completed = unavailable("probe_overrun", self.clock())
        if self._thread is None and self.monotonic() >= self._next_due:
            self._start()
        return completed

    def wait(self, timeout: float) -> ProbeResult | None:
        """Block for the in-flight probe; only for one-shot manual runs."""
        if self._thread is None:
            self.poll()
        try:
            completed = self._results.get(timeout=timeout)
        except queue.Empty:
            return None
        self._thread = None
        return completed


def iso_time(at: float) -> str:
    return datetime.datetime.fromtimestamp(at, datetime.timezone.utc).isoformat(
        timespec="seconds"
    )


def number(details: dict[str, Any], key: str) -> str:
    value = details.get(key)
    return str(value) if type(value) is int else "-"


def predicate_line(result: ProbeResult) -> str:
    if result.valid:
        summary = SUMMARIES.get(result.predicate, "check failed")
        kind = f" ({result.error_kind})" if result.error_kind else ""
    else:
        summary = UNAVAILABLE_REASONS.get(result.error_kind or "", SUMMARIES[UNAVAILABLE])
        kind = ""
    predicate = slack.slack_plain_text(result.predicate, 64)
    return f"predicate: {predicate} - {summary}{kind} - observed {iso_time(result.observed_at)}"


def numbers_line(result: ProbeResult) -> str:
    details = result.details
    return (
        f"zcashd peers: {number(details, 'zcashd_connections')} - "
        f"zakurad height: {number(details, 'zakura_height')} - "
        f"zcashd height: {number(details, 'zcashd_height')} - "
        f"drift: {number(details, 'height_drift')} "
        f"(max {number(details, 'height_max_drift')})"
    )


def heading(target: CompatTarget, icon: str, status: str) -> str:
    name = slack.slack_identity(target.name, 128, "unknown")
    host = slack.slack_plain_text(target.ssh_target, 128)
    return (
        f"{icon} *Zakura compatibility* - `{name}` ({host}) "
        f"check `{compat.CHECK_NAME}` {status}"
    )


def alert_text(target: CompatTarget, result: ProbeResult) -> str:
    return "\n".join(
        (
            heading(target, ":rotating_light:", "failing"),
            predicate_line(result),
            numbers_line(result),
        )
    )


def recovery_text(target: CompatTarget, result: ProbeResult, previous: dict[str, Any]) -> str:
    recovered_from = slack.slack_identity(previous.get("predicate"), 64, "failure")
    return "\n".join(
        (
            heading(target, ":white_check_mark:", "recovered"),
            f"recovered from: {recovered_from} - observed {iso_time(result.observed_at)}",
            numbers_line(result),
        )
    )


def probe_record(result: ProbeResult, completed_at: float) -> dict[str, Any]:
    """Credential-free telemetry for the latest completed probe."""
    return {
        "completed_at": completed_at,
        "observed_at": result.observed_at,
        "valid": result.valid,
        "status": result.status,
        "predicate": result.predicate,
        "error_kind": result.error_kind,
        "details": dict(result.details),
        "suppression_state": result.suppression_state,
    }
