#!/usr/bin/env python3
"""Acceptance checks for compatibility monitoring, runnable from an installed release.

Every subcommand prints one JSON report of statuses, counts, heights and
digests. Credentials, webhook URLs, cookies, env file contents and raw node or
checker logs are never printed.

- ``probe``: one service-context SSH probe of the configured target.
- ``parity``: on zakura-compat, the retired Rust ``zakura-watchdog check`` (with
  SENTRY_DSN unset) and the Python checker against the same configuration.
- ``synthetic``: the shipped lane, checker and installer tests, isolated in
  temporary directories with local fakes for nodes and Slack.
- ``soak``: a read-only observation of the running fleet watchdog.
- ``slack-test``: opt-in, clearly labeled failure and recovery messages through
  the real webhook, using temporary state only.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from zakura_monitoring import compat, monitor, remote, state as state_module  # noqa: E402

TEST_LABEL = "[ACCEPTANCE TEST - not an incident, no action needed]"


def emit(report: dict) -> None:
    print(json.dumps(report, sort_keys=True, indent=2))


def safe_result(result: monitor.ProbeResult | None) -> dict:
    if result is None:
        return {"completed": False}
    return {
        "completed": True,
        "valid": result.valid,
        "status": result.status,
        "predicate": result.predicate,
        "error_kind": result.error_kind,
        "details": result.details,
        "observed_at": result.observed_at,
        "suppression_state": result.suppression_state,
    }


def probe(args: argparse.Namespace) -> int:
    targets = monitor.load_compatibility_targets(args.config)
    if not targets:
        emit({"probe": "no [[compatibility]] target configured"})
        return 3
    worker = monitor.ProbeWorker(targets[0])
    result = worker.wait(targets[0].timeout + monitor.OVERRUN_GRACE_SECONDS)
    emit({"target": targets[0].name, "ssh_target": targets[0].ssh_target,
          "release": compat.release_info(), **safe_result(result)})
    if result is None or not result.valid:
        return 3
    return 0 if result.passed else 1


def parity(args: argparse.Namespace) -> int:
    """Compare the retired Rust check with the Python checker on the same host."""
    shared = {}
    for path in (args.rust_env, args.env_file):
        if path and path.exists():
            # Only checker settings: SENTRY_* and anything else are dropped.
            shared.update(compat.read_env_file(path))
    shared["SYNC_CHECK_TIMEOUT"] = str(args.timeout)
    # Both checks get the lane's limit explicitly, so binary defaults cannot differ.
    shared["HEIGHT_MAX_DRIFT"] = str(args.height_max_drift)
    environment = {"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                   "HOME": os.environ.get("HOME", "/root"), **shared}
    assert "SENTRY_DSN" not in environment
    bound = args.timeout + 60

    rust = None
    if args.rust_bin.exists():
        outcome = remote.run_bounded([str(args.rust_bin), "check"], bound, 0, env=environment)
        rust = {"exit": outcome.returncode, "timed_out": outcome.timed_out}
    checker = HERE / "zakura-compat-check"
    python = remote.run_bounded(
        [sys.executable, "-I", str(checker), "check"], bound, 0, env=environment
    )
    probed = remote.run_bounded(
        [sys.executable, "-I", str(checker), "probe", "--nonce", "parity"], bound,
        monitor.MAX_OUTCOME_BYTES, env=environment,
    )
    try:
        outcome = json.loads(probed.stdout)
        probe_summary = {key: outcome.get(key) for key in
                         ("status", "predicate", "error_kind", "details", "suppression")}
    except ValueError:
        probe_summary = {"status": "unparsable"}
    report = {
        "sentry_dsn_set": False,
        "settings": sorted(shared),
        "rust": rust,
        "python": {"exit": python.returncode, "timed_out": python.timed_out},
        "probe": probe_summary,
        "release": compat.release_info(),
    }
    report["agree"] = rust is not None and rust["exit"] == report["python"]["exit"]
    emit(report)
    return 0 if report["agree"] and python.returncode == 0 else 1


def synthetic(args: argparse.Namespace) -> int:
    """Run the shipped tests against this release from outside any checkout."""
    with tempfile.TemporaryDirectory() as directory:
        result = subprocess.run(
            [sys.executable, "-m", "unittest", "discover", "-s", str(HERE),
             "-p", "test_zakura_monitoring_*.py"],
            cwd=directory,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": directory,
                 "TMPDIR": directory},
            capture_output=True, text=True, timeout=args.timeout, check=False,
        )
    summary = [line for line in result.stderr.splitlines()
               if line.startswith(("Ran ", "OK", "FAILED"))]
    failures = [line for line in result.stderr.splitlines()
                if line.startswith(("FAIL:", "ERROR:"))]
    emit({"release": compat.release_info(), "exit": result.returncode,
          "summary": summary, "failures": failures})
    return 0 if result.returncode == 0 else 1


def service_state(unit: str) -> str:
    try:
        return subprocess.run(["systemctl", "is-active", unit], capture_output=True,
                              text=True, timeout=30, check=False).stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def soak(args: argparse.Namespace) -> int:
    """Observe the live lane read-only and record its probes and service health."""
    samples = []
    deadline = time.monotonic() + args.duration
    while True:
        state = state_module.load_state(args.state) if args.state.exists() else {}
        record = state.get(monitor.COMPAT_PROBES, {}).get(args.target, {})
        last = record.get("last", {})
        samples.append({
            "at": time.time(),
            "service": service_state(args.unit),
            "completed": record.get("completed", 0),
            "passed": record.get("passed", 0),
            "unavailable": record.get("unavailable", 0),
            "last_status": last.get("status"),
            "last_predicate": last.get("predicate"),
            "zakura_height": last.get("details", {}).get("zakura_height"),
            "zcashd_height": last.get("details", {}).get("zcashd_height"),
            "incident": state.get(monitor.COMPAT_STATE, {}).get(args.target, {}).get("alerting"),
            "fleets_alerting": sorted(
                name for name, entry in state.get("fleets", {}).items() if entry.get("alerting")
            ),
        })
        if time.monotonic() >= deadline:
            break
        time.sleep(min(args.interval, max(0.0, deadline - time.monotonic())))

    first, last = samples[0], samples[-1]
    heights = [s["zakura_height"] for s in samples if isinstance(s["zakura_height"], int)]
    expected = max(1, int(args.duration // 60 * 0.8))
    checks = {
        "service_always_active": all(s["service"] == "active" for s in samples),
        "probes_completed": last["completed"] - first["completed"] >= expected,
        "all_new_probes_passed": (
            last["passed"] - first["passed"] == last["completed"] - first["completed"]
        ),
        "no_unavailable_probes": last["unavailable"] == first["unavailable"],
        "height_advanced": len(heights) >= 2 and heights[-1] > heights[0],
        "no_open_incident": not last["incident"],
    }
    report = {"duration": args.duration, "samples": samples, "checks": checks,
              "expected_probes": expected, "passed": all(checks.values())}
    if args.report:
        args.report.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    emit({key: value for key, value in report.items() if key != "samples"}
         | {"sample_count": len(samples)})
    return 0 if report["passed"] else 1


def slack_test(args: argparse.Namespace) -> int:
    """Post one labeled failure and recovery through the real lane and webhook."""
    if not args.confirm_real_slack:
        emit({"slack_test": "refused: pass --confirm-real-slack to post to #zakura-alerts"})
        return 2
    for key, value in read_slack_env(args.env_file).items():
        os.environ[key] = value
    spec = importlib.util.spec_from_file_location(
        "zakura_cluster_watchdog_acceptance", HERE / "zakura-cluster-watchdog.py"
    )
    watchdog = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(watchdog)
    real_post = watchdog.post_slack
    sent = []

    def labeled(text, post_args):
        accepted = real_post(f"{TEST_LABEL}\n{text}", post_args)
        sent.append(accepted)
        return accepted

    watchdog.post_slack = labeled

    class Scripted:
        target = monitor.CompatTarget(name="ACCEPTANCE-TEST-zakura-compat",
                                      ssh_target="root@159.203.113.196", known_hosts=None)

        def __init__(self):
            now = time.time()
            numbers = {"height_max_drift": 30, "zcashd_connections": 1}
            self.results = [
                monitor.ProbeResult(True, "fail", "peer_pinning", None,
                                    {**numbers, "zcashd_connections": 2}, now, "missing"),
                monitor.ProbeResult(True, "pass", "in_sync", None,
                                    {**numbers, "zakura_height": 1, "zcashd_height": 1,
                                     "height_drift": 0}, now, "missing"),
            ]

        def poll(self):
            return self.results.pop(0) if self.results else None

    worker = Scripted()
    with tempfile.TemporaryDirectory() as directory:
        state_path = Path(directory) / "state.json"
        options = argparse.Namespace(
            dry_run=False, slack_timeout=20.0, mac_comparison=None,
            suppression_file=Path(directory) / "none",
        )
        agent = watchdog.Watchdog([], options, compatibility=[worker],
                                  checkpoint=lambda s: state_module.save_state(state_path, s))
        state = state_module.load_state(state_path)
        for _ in range(6):
            agent.handle_compatibility(state, worker, time.time())
            if not worker.results and not state.get(monitor.COMPAT_QUEUE):
                break
            time.sleep(2)
    emit({"slack_test": "posted", "label": TEST_LABEL, "accepted": sent,
          "temporary_state_only": True})
    return 0 if sent and all(sent) and len(sent) == 2 else 1


def read_slack_env(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.strip().partition("=")
        if separator and key in ("SLACK_WEB_HOOK", "SLACK_WEBHOOK_URL", "SLACK_WEBHOOK"):
            values[key] = value.strip()
    return values


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    probe_parser = sub.add_parser("probe")
    probe_parser.add_argument("--config", type=Path, default=HERE / "fleet-watchdog.toml")
    parity_parser = sub.add_parser("parity")
    parity_parser.add_argument("--rust-bin", type=Path,
                               default=Path("/usr/local/bin/zakura-watchdog"))
    parity_parser.add_argument("--rust-env", type=Path, default=Path("/etc/zakura-watchdog/env"))
    parity_parser.add_argument("--env-file", type=Path,
                               default=Path("/etc/zakura-monitoring/compat.env"))
    parity_parser.add_argument("--timeout", type=int, default=60)
    parity_parser.add_argument("--height-max-drift", type=int,
                               default=monitor.DEFAULT_HEIGHT_MAX_DRIFT)
    synthetic_parser = sub.add_parser("synthetic")
    synthetic_parser.add_argument("--timeout", type=int, default=900)
    soak_parser = sub.add_parser("soak")
    soak_parser.add_argument("--state", type=Path,
                             default=Path("/var/lib/zakura-fleet-watchdog/state.json"))
    soak_parser.add_argument("--target", default="zakura-compat")
    soak_parser.add_argument("--unit", default="zakura-fleet-watchdog.service")
    soak_parser.add_argument("--duration", type=float, default=1800)
    soak_parser.add_argument("--interval", type=float, default=60)
    soak_parser.add_argument("--report", type=Path)
    slack_parser = sub.add_parser("slack-test")
    slack_parser.add_argument("--env-file", type=Path, default=Path("/etc/zakura-fleet-watchdog/env"))
    slack_parser.add_argument("--confirm-real-slack", action="store_true")
    args = parser.parse_args()
    return {
        "probe": probe, "parity": parity, "synthetic": synthetic,
        "soak": soak, "slack-test": slack_test,
    }[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
