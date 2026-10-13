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
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable

from . import compat, remote, slack, suppression


ENABLE_ENV = "ZAKURA_COMPAT_MONITORING"
COMPAT_STATE = "compatibility"
COMPAT_PROBES = "compatibility_probes"
COMPAT_QUEUE = "compatibility_pending_delivery"

# The single supported target. The deploy workflow pins the same host key.
KNOWN_TARGETS = {"zakura-compat": "root@159.203.113.196"}
DEFAULT_CHECKER = "/opt/zakura-monitoring/current/zakura-compat-check"
DEFAULT_ENV_FILE = "/etc/zakura-monitoring/compat.env"
DEFAULT_KNOWN_HOSTS = "/etc/zakura-fleet-watchdog/known_hosts"
DEFAULT_INTERVAL = 60.0
MAX_PROBE_TIMEOUT = 120.0
DEFAULT_REMOTE_DEADLINE = 100
# The checker's default, also the deployed Rust watchdog's. The lane passes it
# explicitly so a host env file cannot loosen monitoring unnoticed.
DEFAULT_HEIGHT_MAX_DRIFT = 10
MAX_OUTCOME_BYTES = 16 * 1024
CLOCK_SKEW_SECONDS = 60.0
OVERRUN_GRACE_SECONDS = 15.0
UNAVAILABLE = "monitoring_unavailable"
SSH_TARGET = re.compile(r"[a-z_][a-z0-9_-]{0,31}@(?:[0-9]{1,3}\.){3}[0-9]{1,3}")

UNAVAILABLE_REASONS = {
    "ssh_timeout": "The host did not respond before the monitoring timeout.",
    "ssh_failed": "Cannot connect to the host over SSH.",
    "checker_missing": "The compatibility checker is missing from the host.",
    "checker_config_invalid": "The compatibility checker configuration is invalid.",
    "malformed_outcome": "The compatibility checker returned an invalid response.",
    "stale_outcome": "The compatibility check returned an out-of-date result.",
    "oversized_outcome": "The compatibility checker returned too much data.",
    "probe_overrun": "The compatibility check did not finish before its timeout.",
    "probe_failed": "The compatibility check could not complete.",
}
SUMMARIES = {
    "zakurad_process": "Zakura is not running.",
    "zcashd_process": "zcashd is not running.",
    "zcashd_getconnectioncount": "Could not read zcashd's peer count.",
    "peer_pinning": "zcashd must have exactly one peer.",
    "zakura_getblockcount": "Could not read Zakura's block height.",
    "zcashd_getblockcount": "Could not read zcashd's block height.",
    "height_drift": "Zakura and zcashd are too far apart in block height.",
    "deadline": "The compatibility check did not finish before its timeout.",
    UNAVAILABLE: "The compatibility check could not complete.",
}
RPC_ERROR_SUMMARIES = {
    "auth_unavailable": "RPC credentials are unavailable.",
    "auth_malformed": "RPC credentials could not be read.",
    "connection": "The node did not accept the connection.",
    "timeout": "The request timed out.",
    "http_status": "The node rejected the request.",
    "oversized_response": "The node response was too large.",
    "malformed_json": "The node returned an invalid response.",
    "rpc_error": "The node returned an RPC error.",
    "missing_result": "The node returned an incomplete response.",
    "invalid_result": "The node returned an invalid result.",
    "invalid_config": "The compatibility checker configuration is invalid.",
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
    height_max_drift: int = DEFAULT_HEIGHT_MAX_DRIFT


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
        "interval", "timeout", "remote_deadline", "height_max_drift",
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
    height_max_drift = raw.get("height_max_drift", DEFAULT_HEIGHT_MAX_DRIFT)
    if type(height_max_drift) is not int or height_max_drift < 0:
        raise SystemExit("compatibility height_max_drift must be a non-negative integer")

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
            height_max_drift=height_max_drift,
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
    # Local wall clock when the worker actually finished, never queue-consumption time.
    completed_at: float = 0.0

    @property
    def passed(self) -> bool:
        return self.valid and self.status == compat.PASS


def unavailable(reason: str, at: float) -> ProbeResult:
    return ProbeResult(False, compat.FAIL, UNAVAILABLE, reason, {}, at, completed_at=at)


def finite_number(value: object) -> float | None:
    if type(value) not in (int, float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def fresh_result(result: ProbeResult, now: float, max_age: float) -> ProbeResult:
    """Recheck a valid result at consumption, preserving its real completion time.

    Missing/future completion times or stale local/remote observations fail
    closed. Remote observation time permits the existing host clock skew.
    """
    completed = finite_number(result.completed_at)
    observed = finite_number(result.observed_at)
    if result.valid and (completed is None or completed <= 0 or observed is None
            or not 0 <= now - completed <= max_age
            or not -CLOCK_SKEW_SECONDS <= now - observed <= max_age + CLOCK_SKEW_SECONDS):
        return replace(unavailable("stale_outcome", now), completed_at=result.completed_at)
    return result


def parse_probe_output(
    result: remote.BoundedResult,
    nonce: str,
    started_at: float,
    finished_at: float,
    expected_max_drift: int,
) -> ProbeResult:
    """Validate checker output against this probe's nonce and requested drift policy."""
    if result.timed_out:
        return unavailable("ssh_timeout", finished_at)
    if result.returncode is None:
        return unavailable("ssh_failed", finished_at)
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

    if details.get("height_max_drift") != expected_max_drift:
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
        completed_at=finished_at,
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
        self._results: queue.SimpleQueue[tuple[ProbeResult, float]] = queue.SimpleQueue()
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
            "--height-max-drift", str(self.target.height_max_drift),
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
            parsed = parse_probe_output(
                result, nonce, started_at, self.clock(), self.target.height_max_drift)
        except Exception:  # A worker failure is a monitoring failure, never a crash.
            parsed = unavailable("probe_failed", self.clock())
        finished_mono = self.monotonic()
        if parsed.valid and finished_mono - self._started_mono > self.target.timeout:
            parsed = unavailable("probe_overrun", self.clock())
        self._results.put((parsed, finished_mono))

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

    def _consume(self, queued: tuple[ProbeResult, float]) -> ProbeResult:
        result, finished_mono = queued
        age = self.monotonic() - finished_mono
        if result.valid and (self._overrun_reported or not 0 <= age <= self.target.timeout):
            result = replace(unavailable("stale_outcome", self.clock()),
                             completed_at=result.completed_at)
        return fresh_result(result, self.clock(), self.target.timeout)

    def poll(self) -> ProbeResult | None:
        """Collect a finished probe and start the next one when it is due.

        Never blocks. While a probe is in flight no other probe starts, even
        after the in-flight one overran; the overrun itself is reported once as
        a monitoring failure.
        """
        completed = None
        try:
            completed = self._consume(self._results.get_nowait())
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

    def wait(self, timeout: float, keep: bool = False) -> ProbeResult | None:
        """Block for the in-flight probe; only for one-shot manual runs.

        With ``keep`` the result stays queued for the next :meth:`poll`.
        """
        if self._thread is None:
            self.poll()
        try:
            queued = self._results.get(timeout=timeout)
        except queue.Empty:
            return None
        completed = self._consume(queued)
        if keep:
            self._results.put(queued)
        else:
            self._thread = None
        return completed


def iso_time(at: float) -> str:
    return datetime.datetime.fromtimestamp(at, datetime.timezone.utc).isoformat(
        timespec="seconds"
    )


def problem_lines(result: ProbeResult) -> list[str]:
    """Explain the failed check, including only measurements relevant to it."""
    if not result.valid:
        return [UNAVAILABLE_REASONS.get(result.error_kind or "", SUMMARIES[UNAVAILABLE])]
    details = result.details
    peers = details.get("zcashd_connections")
    if result.predicate == "peer_pinning" and type(peers) is int:
        noun = "peer" if peers == 1 else "peers"
        return [f"zcashd has *{peers} {noun}*; expected *1*."]
    zakura, zcashd = details.get("zakura_height"), details.get("zcashd_height")
    if result.predicate == "height_drift" and type(zakura) is int and type(zcashd) is int:
        direction = "ahead of" if zakura >= zcashd else "behind"
        maximum = details.get("height_max_drift")
        limit = f" (limit: {maximum})" if type(maximum) is int else ""
        return [
            f"Zakura is *{abs(zakura - zcashd):,} blocks {direction}* zcashd{limit}.",
            f"Heights: Zakura {zakura:,} · zcashd {zcashd:,}",
        ]
    summary = SUMMARIES.get(result.predicate, "The compatibility check failed.")
    reason = RPC_ERROR_SUMMARIES.get(result.error_kind or "", "")
    return [f"{summary} {reason}".strip()]


def observation_line(result: ProbeResult) -> str:
    observed = datetime.datetime.fromtimestamp(result.observed_at, datetime.timezone.utc)
    return f"_Observed {observed.day} {observed:%b %H:%M} UTC_"


def heading(target: CompatTarget, icon: str, status: str) -> str:
    name = slack.slack_identity(target.name, 128, "unknown")
    return f"{icon} *Zakura compatibility {status}* — `{name}`"


def alert_text(target: CompatTarget, result: ProbeResult) -> str:
    return "\n".join(
        (
            heading(target, ":rotating_light:", "problem"),
            *problem_lines(result),
            observation_line(result),
        )
    )


def recovery_text(target: CompatTarget, result: ProbeResult, _previous: dict[str, Any]) -> str:
    return "\n".join(
        (
            heading(target, ":white_check_mark:", "restored"),
            "Zakura and zcashd are back in sync.",
            observation_line(result),
        )
    )


def probe_record(result: ProbeResult) -> dict[str, Any]:
    """Credential-free telemetry for the latest completed probe."""
    return {
        "completed_at": result.completed_at,
        "observed_at": result.observed_at,
        "valid": result.valid,
        "status": result.status,
        "predicate": result.predicate,
        "error_kind": result.error_kind,
        "details": dict(result.details),
        "suppression_state": result.suppression_state,
    }
