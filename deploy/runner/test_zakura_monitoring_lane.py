#!/usr/bin/env python3
"""Tests for the fleet watchdog's compatibility lane.

They run from the repository and from an installed release: every module is
loaded relative to this file, Slack is replaced by a local fake or a local HTTP
receiver, and all state lives in temporary directories.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shlex
import socket
import stat
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from zakura_monitoring import compat, monitor, remote, state as state_module  # noqa: E402

SPEC = importlib.util.spec_from_file_location(
    "zakura_cluster_watchdog_lane", HERE / "zakura-cluster-watchdog.py"
)
assert SPEC is not None and SPEC.loader is not None
watchdog = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = watchdog
SPEC.loader.exec_module(watchdog)

TARGET = monitor.CompatTarget(
    name="zakura-compat", ssh_target="root@159.203.113.196", known_hosts=None
)
NOW = 1_800_000_000.0


def make_args(**overrides):
    values = {
        "suppression_file": Path("/nonexistent/zakura-fleet-suppression"),
        "slack_timeout": 5.0,
        "dry_run": False,
        "mac_comparison": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def details(peers=1, zakura=3_000_000, zcashd=3_000_000, maximum=30):
    values = {"height_max_drift": maximum, "zcashd_connections": peers}
    if zakura is not None:
        values.update(zakura_height=zakura, zcashd_height=zcashd,
                      height_drift=abs(zakura - zcashd))
    return values


def passing(at=NOW, suppressed_until=None, **kwargs):
    return monitor.ProbeResult(
        True, "pass", "in_sync", None, details(**kwargs), at,
        "active" if suppressed_until else "missing", suppressed_until,
    )


def failing(predicate="height_drift", at=NOW, suppressed_until=None, **kwargs):
    kwargs.setdefault("zakura", 3_000_100)
    return monitor.ProbeResult(
        True, "fail", predicate, None, details(**kwargs), at,
        "active" if suppressed_until else "missing", suppressed_until,
    )


def missing_checker(at=NOW):
    return monitor.unavailable("checker_missing", at)


class FakeWorker:
    """Scripted stand-in for ProbeWorker with the same poll() contract."""

    def __init__(self, target=TARGET):
        self.target = target
        self.results: list = []

    def poll(self):
        return self.results.pop(0) if self.results else None


class LaneCase(unittest.TestCase):
    def setUp(self):
        self.posted: list[str] = []
        self.accept = True
        self.checkpoints = 0
        patcher = mock.patch.object(watchdog, "post_slack", side_effect=self.post)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.worker = FakeWorker()
        self.state = state_module.load_state(Path("/nonexistent/state.json"))
        self.agent = self.make_agent()

    def make_agent(self, fleets=None):
        def checkpoint(_state):
            self.checkpoints += 1
        return watchdog.Watchdog(
            fleets or [], make_args(), checkpoint=checkpoint, compatibility=[self.worker]
        )

    def post(self, text, _args):
        if self.accept:
            self.posted.append(text)
        return self.accept

    def step(self, result=None, now=NOW):
        if result is not None:
            self.worker.results.append(result)
        self.agent.handle_compatibility(self.state, self.worker, now)

    @property
    def entry(self):
        return self.state.get(monitor.COMPAT_STATE, {}).get(TARGET.name, {})

    @property
    def pending(self):
        return self.state.get(monitor.COMPAT_QUEUE, {}).get(TARGET.name)


class TransitionTests(LaneCase):
    def test_first_completed_failure_alerts_immediately(self):
        self.step(failing())
        self.assertEqual(len(self.posted), 1)
        self.assertTrue(self.entry["alerting"])
        self.assertEqual(self.entry["predicate"], "height_drift")
        self.assertIsNone(self.pending)

    def test_persistent_failure_does_not_repeat(self):
        self.step(failing())
        for offset, predicate in ((60, "height_drift"), (120, "peer_pinning"), (180, "zakurad_process")):
            self.step(failing(predicate, at=NOW + offset), now=NOW + offset)
        self.step(missing_checker(NOW + 240), now=NOW + 240)
        self.assertEqual(len(self.posted), 1)
        self.assertEqual(self.entry["predicate"], "height_drift")
        self.assertEqual(self.entry["last_predicate"], "monitoring_unavailable")

    def test_complete_pass_recovers_once(self):
        self.step(failing())
        self.step(passing(at=NOW + 60), now=NOW + 60)
        self.step(passing(at=NOW + 120), now=NOW + 120)
        self.assertEqual(len(self.posted), 2)
        self.assertIn("recovered", self.posted[1])
        self.assertIn("recovered from: height＿drift", self.posted[1])
        self.assertEqual(self.entry, {"condition": "ok", "alerting": False})

    def test_healthy_start_is_silent(self):
        self.step(passing())
        self.step(None, now=NOW + 30)
        self.assertEqual(self.posted, [])

    def test_unavailable_probe_alerts_but_never_recovers(self):
        self.step(missing_checker())
        self.assertEqual(len(self.posted), 1)
        self.assertIn("checker is not installed", self.posted[0])
        for reason in ("ssh_timeout", "stale_outcome", "malformed_outcome"):
            self.step(monitor.unavailable(reason, NOW + 60), now=NOW + 60)
        self.assertEqual(len(self.posted), 1)
        self.assertTrue(self.entry["alerting"])

    def test_alert_text_is_complete_and_credential_free(self):
        self.step(failing(peers=1, zakura=3_000_041, zcashd=3_000_000))
        text = self.posted[0]
        for expected in (
            "zakura-compat", "root＠159.203.113.196", "check `zcashd_compat_sync` failing",
            "height＿drift", "zcashd peers: 1", "zakurad height: 3000041",
            "zcashd height: 3000000", "drift: 41 (max 30)",
            "observed 2027-01-15T08:00:00+00:00",
        ):
            self.assertIn(expected, text)
        self.assertNotIn("nonce", text)

    def test_probe_telemetry_records_heights(self):
        self.step(passing(zakura=10, zcashd=9))
        record = self.state[monitor.COMPAT_PROBES][TARGET.name]
        self.assertEqual((record["completed"], record["passed"]), (1, 1))
        self.assertEqual(record["last"]["details"]["zakura_height"], 10)


class DeliveryTests(LaneCase):
    def test_failed_delivery_is_retried_without_losing_state(self):
        self.accept = False
        self.step(failing())
        self.assertEqual(self.posted, [])
        self.assertEqual(len(self.pending["messages"]), 1)
        self.assertNotIn(TARGET.name, self.state.get(monitor.COMPAT_STATE, {}))
        self.step(failing(at=NOW + 60), now=NOW + 60)
        self.assertEqual(len(self.pending["messages"]), 1, "persistent failure must not re-queue")
        self.accept = True
        self.step(None, now=NOW + 120)
        self.assertEqual(len(self.posted), 1)
        self.assertIsNone(self.pending)
        self.assertTrue(self.entry["alerting"])

    def test_pending_failure_followed_by_recovery_keeps_both_in_order(self):
        self.accept = False
        self.step(failing())
        self.step(passing(at=NOW + 60), now=NOW + 60)
        self.assertEqual(len(self.pending["messages"]), 2)
        self.accept = True
        self.step(None, now=NOW + 120)
        self.step(None, now=NOW + 180)
        self.assertEqual(len(self.posted), 2)
        self.assertIn("failing", self.posted[0])
        self.assertIn("recovered", self.posted[1])
        self.assertEqual(self.entry, {"condition": "ok", "alerting": False})

    def test_restart_keeps_pending_queue_and_incident_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            self.state["operator_note"] = {"keep": True}
            self.accept = False
            self.step(failing())
            state_module.save_state(path, self.state)

            self.worker = FakeWorker()
            self.agent = self.make_agent()
            self.state = state_module.load_state(path)
            self.assertEqual(self.state["operator_note"], {"keep": True})
            self.accept = True
            self.step(None, now=NOW + 60)
            self.assertEqual(len(self.posted), 1)
            state_module.save_state(path, self.state)

            self.worker = FakeWorker()
            self.agent = self.make_agent()
            self.state = state_module.load_state(path)
            self.step(failing(at=NOW + 120), now=NOW + 120)
            self.assertEqual(len(self.posted), 1, "a restart must not repeat the alert")
            self.step(passing(at=NOW + 180), now=NOW + 180)
            self.assertEqual(len(self.posted), 2)
            self.assertEqual(self.state["version"], state_module.STATE_VERSION)

    def test_local_receiver_failed_delivery_then_retry(self):
        receiver = Receiver(fail_first=1)
        self.addCleanup(receiver.close)
        mock.patch.object(watchdog, "post_slack", side_effect=watchdog.slack.post_slack).start()
        with mock.patch.dict(os.environ, {"SLACK_WEB_HOOK": receiver.url}):
            self.step(failing())
            self.assertIsNotNone(self.pending)
            self.step(None, now=NOW + 60)
        self.assertIsNone(self.pending)
        self.assertEqual(receiver.attempts, 2)
        self.assertIn("Zakura compatibility updates", receiver.accepted[0])
        mock.patch.stopall()

    def test_slack_absent_keeps_the_alert_pending(self):
        mock.patch.object(watchdog, "post_slack", side_effect=watchdog.slack.post_slack).start()
        environ = {key: value for key, value in os.environ.items() if "SLACK" not in key}
        with mock.patch.dict(os.environ, environ, clear=True):
            self.step(failing())
        self.assertEqual(len(self.pending["messages"]), 1)
        mock.patch.stopall()


class NamespaceTests(LaneCase):
    def test_fleet_and_compatibility_queues_cannot_overwrite_each_other(self):
        fleet = watchdog.Fleet("zakura-compat", "http://127.0.0.1:9/data", "http://127.0.0.1:9/")
        self.state["pending_delivery"] = {
            "zakura-compat": {"messages": ["fleet message"], "state": {"fleets": {}}}
        }
        self.state["fleets"] = {"zakura-compat": {"condition": "unreachable", "alerting": True}}
        self.accept = False
        self.step(failing())
        self.assertEqual(self.state["pending_delivery"]["zakura-compat"]["messages"],
                         ["fleet message"])
        self.accept = True
        self.step(None, now=NOW + 60)
        self.assertTrue(self.entry["alerting"])
        self.assertIn("zakura-compat", self.state["pending_delivery"])
        self.assertTrue(self.state["fleets"]["zakura-compat"]["alerting"])

        self.agent.deliver_batch(self.state, fleet)
        self.assertNotIn("zakura-compat", self.state["pending_delivery"])
        self.assertTrue(self.entry["alerting"], "fleet commit must not touch compatibility")

    def test_fleet_deploy_suppression_does_not_mute_compatibility(self):
        self.worker.results.append(failing())
        with mock.patch.object(watchdog, "suppression_until", return_value=time.time() + 600):
            self.agent.run_once(self.state)
        self.assertEqual(len(self.posted), 1)


class SuppressionTests(LaneCase):
    def test_active_marker_suppresses_transitions_but_not_post_expiry_failure(self):
        until = NOW + 600
        self.step(failing(suppressed_until=until))
        self.step(missing_checker(NOW + 60), now=NOW + 60)
        self.step(passing(at=NOW + 120, suppressed_until=until), now=NOW + 120)
        self.assertEqual(self.posted, [])
        self.assertEqual(self.entry, {})
        self.step(failing(at=NOW + 660), now=NOW + 660)
        self.assertEqual(len(self.posted), 1)

    def test_unavailable_probe_after_window_alerts(self):
        self.step(failing(suppressed_until=NOW + 120))
        self.step(missing_checker(NOW + 180), now=NOW + 180)
        self.assertEqual(len(self.posted), 1)

    def test_recovery_during_suppression_is_sent_after_expiry(self):
        self.step(failing())
        self.posted.clear()
        self.step(failing(at=NOW + 120, suppressed_until=NOW + 400), now=NOW + 120)
        self.step(passing(at=NOW + 180, suppressed_until=NOW + 400), now=NOW + 180)
        self.assertEqual(self.posted, [])
        self.assertTrue(self.entry["alerting"], "suppression must not commit the recovery")
        self.step(passing(at=NOW + 460), now=NOW + 460)
        self.assertEqual(len(self.posted), 1)
        self.assertIn("recovered", self.posted[0])
        self.assertFalse(self.entry["alerting"])


def bounded(outcome: dict | bytes | None, returncode=0, timed_out=False, oversized=False):
    stdout = outcome if isinstance(outcome, bytes) else json.dumps(outcome or {}).encode()
    if outcome is None:
        stdout = b""
    return remote.BoundedResult(returncode, stdout, timed_out, oversized, 1.0)


def outcome_dict(nonce="n", status="pass", predicate="in_sync", observed_at=NOW, **changes):
    value = {
        "schema": compat.SCHEMA, "check": compat.CHECK_NAME, "status": status,
        "predicate": predicate, "summary": "x", "details": details(),
        "error_kind": None, "observed_at": observed_at, "nonce": nonce,
        "suppression": {"state": "missing", "active": False, "until": None, "max_seconds": 1200},
    }
    value.update(changes)
    return value


class ParseTests(unittest.TestCase):
    def parse(self, result, nonce="n"):
        return monitor.parse_probe_output(result, nonce, NOW - 5, NOW + 5)

    def test_valid_pass(self):
        result = self.parse(bounded(outcome_dict()))
        self.assertTrue(result.passed)

    def test_transport_failures_are_unavailable(self):
        cases = {
            "ssh_timeout": bounded(None, returncode=-9, timed_out=True),
            "oversized_outcome": bounded(outcome_dict(), oversized=True),
            "ssh_failed": bounded(None, returncode=255),
            "checker_missing": bounded(None, returncode=2),
            "malformed_outcome": bounded(b"not json"),
        }
        for reason, result in cases.items():
            with self.subTest(reason=reason):
                parsed = self.parse(result)
                self.assertFalse(parsed.valid)
                self.assertFalse(parsed.passed)
                self.assertEqual(parsed.error_kind, reason)
        self.assertEqual(self.parse(remote.BoundedResult(None, b"", False, False, 0)).error_kind,
                         "ssh_failed")

    def test_wrong_nonce_schema_or_exit_code_is_malformed(self):
        for result in (
            bounded(outcome_dict(nonce="other")),
            bounded(outcome_dict(schema="v0")),
            bounded(outcome_dict(), returncode=1),
        ):
            self.assertEqual(self.parse(result).error_kind, "malformed_outcome")
        config = bounded(outcome_dict(status="fail", predicate="invalid_config"), returncode=2)
        self.assertEqual(self.parse(config).error_kind, "checker_config_invalid")

    def test_stale_outcome_cannot_recover(self):
        for observed_at in (NOW - 600, NOW + 600):
            parsed = self.parse(bounded(outcome_dict(observed_at=observed_at)))
            self.assertEqual(parsed.error_kind, "stale_outcome")
            self.assertFalse(parsed.passed)

    def test_incomplete_or_inconsistent_pass_is_malformed(self):
        broken = [
            {"details": {"height_max_drift": 30}},
            {"details": details(peers=2)},
            {"details": {**details(), "height_drift": 5}},
            {"details": details(zakura=100, zcashd=200)},
            {"details": {**details(), "zakura_height": True}},
            {"details": {**details(), "zakura_height": 1.5}},
            {"details": {**details(), "secret": 1}},
            {"error_kind": "timeout"},
            {"predicate": "height_drift"},
            {"suppression": None},
            {"suppression": {"state": "active", "active": False, "until": 1, "max_seconds": 1}},
        ]
        for change in broken:
            with self.subTest(change=change):
                parsed = self.parse(bounded(outcome_dict(**change)))
                self.assertFalse(parsed.valid)

    def test_suppression_window_is_bounded_and_converted_to_local_time(self):
        def marker(until, state="active"):
            return {"state": state, "active": state == "active", "until": until,
                    "max_seconds": 5000}
        parsed = self.parse(bounded(outcome_dict(suppression=marker(int(NOW) + 300))))
        self.assertEqual(parsed.suppressed_until, NOW + 5 + 300)
        parsed = self.parse(bounded(outcome_dict(suppression=marker(int(NOW) + 3000))))
        self.assertIsNone(parsed.suppressed_until)
        for state in ("excessive", "malformed", "expired", "missing"):
            parsed = self.parse(bounded(outcome_dict(suppression=marker(None, state))))
            self.assertTrue(parsed.valid)
            self.assertIsNone(parsed.suppressed_until)


class WorkerTests(unittest.TestCase):
    def test_hung_probe_never_blocks_and_never_starts_a_second_worker(self):
        release = threading.Event()
        calls = []

        def runner(command, timeout, max_output):
            calls.append(command)
            release.wait(10)
            return bounded(None, returncode=255)

        clock = [0.0]
        worker = monitor.ProbeWorker(
            monitor.CompatTarget(name="zakura-compat", ssh_target="root@159.203.113.196",
                                 known_hosts=None),
            runner=runner, monotonic=lambda: clock[0],
        )
        started = time.monotonic()
        overruns = []
        for second in range(0, 400, 10):
            clock[0] = float(second)
            result = worker.poll()
            if result is not None:
                overruns.append(result.error_kind)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(worker.starts, 1)
        self.assertEqual(overruns, ["probe_overrun"])
        release.set()
        deadline = time.monotonic() + 5
        result = None
        while result is None and time.monotonic() < deadline:
            result = worker.poll()
            time.sleep(0.01)
        self.assertEqual(result.error_kind, "ssh_failed")
        self.assertEqual(worker.starts, 2, "the next probe starts once the hung one finished")
        release.set()

    def test_probes_run_on_the_interval(self):
        clock = [0.0]
        worker = monitor.ProbeWorker(TARGET, runner=lambda *_: bounded(None, returncode=255),
                                     monotonic=lambda: clock[0])
        worker.poll()
        worker._thread.join(5)
        clock[0] = 30.0
        self.assertIsNotNone(worker.poll())
        self.assertEqual(worker.starts, 1)
        clock[0] = 60.0
        worker.poll()
        self.assertEqual(worker.starts, 2)

    def test_hung_probe_does_not_block_fleet_polls(self):
        release = threading.Event()
        worker = monitor.ProbeWorker(
            TARGET, runner=lambda *_: (release.wait(10), bounded(None, returncode=255))[1]
        )
        fleet = watchdog.Fleet("mainnet", "http://127.0.0.1:9/data", "http://127.0.0.1:9/")
        snapshot = {"rows": [{"name": "node-a", "health": "healthy", "height": 1,
                              "block_hash": "ab", "seconds_since_advanced": 1}]}
        agent = watchdog.Watchdog([fleet], argparse.Namespace(
            **{**vars(make_args()), "down_after": 600.0, "stalled_after": 600.0,
               "shared_stalled_after": 1800.0, "starting_grace": 120.0,
               "dashboard_down_after": 600.0, "request_timeout": 1.0}),
            compatibility=[worker])
        state = state_module.load_state(Path("/nonexistent/state.json"))
        started = time.monotonic()
        with mock.patch.object(watchdog, "fetch_json", return_value=snapshot) as fetch:
            for _ in range(5):
                agent.run_once(state)
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertEqual(fetch.call_count, 5)
        self.assertEqual(worker.starts, 1)
        release.set()

    def test_ssh_command_is_batch_mode_with_pinned_host_keys(self):
        worker = monitor.ProbeWorker(
            monitor.CompatTarget(name="zakura-compat", ssh_target="root@159.203.113.196")
        )
        command = worker.command("abc")
        self.assertEqual(command[0], "ssh")
        for option in ("BatchMode=yes", "StrictHostKeyChecking=yes",
                       "UserKnownHostsFile=/etc/zakura-fleet-watchdog/known_hosts"):
            self.assertIn(option, command)
        self.assertEqual(command[-2], "root@159.203.113.196")
        self.assertEqual(
            shlex.split(command[-1]),
            ["python3", "-I", "/opt/zakura-monitoring/current/zakura-compat-check", "probe",
             "--env-file", "/etc/zakura-monitoring/compat.env", "--deadline", "100",
             "--nonce", "abc"],
        )


class OneShotTests(unittest.TestCase):
    def test_once_mode_applies_the_probe_it_waited_for(self):
        worker = monitor.ProbeWorker(TARGET, runner=lambda *_: bounded(None, returncode=255))
        result = worker.wait(5, keep=True)
        self.assertEqual(result.error_kind, "ssh_failed")
        posted = []
        with mock.patch.object(watchdog, "post_slack",
                               side_effect=lambda text, _args: (posted.append(text), True)[1]):
            agent = watchdog.Watchdog([], make_args(), compatibility=[worker])
            agent.run_once(state_module.load_state(Path("/nonexistent/state.json")))
        self.assertEqual(len(posted), 1)
        self.assertEqual(worker.starts, 1)


class BoundedRunTests(unittest.TestCase):
    def test_timeout_kills_the_process_group(self):
        started = time.monotonic()
        result = remote.run_bounded(["sh", "-c", "sleep 30 & sleep 30"], 0.5, 100)
        self.assertTrue(result.timed_out)
        self.assertLess(time.monotonic() - started, 7)

    def test_output_is_capped(self):
        result = remote.run_bounded([sys.executable, "-c", "print('x' * 100000)"], 10, 1000)
        self.assertTrue(result.oversized)
        self.assertEqual(len(result.stdout), 1000)

    def test_missing_command(self):
        self.assertIsNone(remote.run_bounded(["/nonexistent/ssh"], 1, 10).returncode)


class EndToEndProbeTests(unittest.TestCase):
    """The real checker, run locally in place of SSH, through the real worker."""

    def test_installed_checker_round_trip(self):
        sys.path.insert(0, str(HERE))
        from test_zakura_monitoring_compat import RpcServer

        servers = [RpcServer(), RpcServer()]
        for server in servers:
            self.addCleanup(server.close)
        zakura, zcashd = servers
        zakura.replies = {"getblockcount": {"result": 50, "error": None}}
        zcashd.replies = {"getconnectioncount": {"result": 1, "error": None},
                          "getblockcount": {"result": 49, "error": None}}
        with tempfile.TemporaryDirectory() as directory:
            bin_dir = Path(directory)
            pgrep = bin_dir / "pgrep"
            pgrep.write_text("#!/bin/sh\necho 4242\n")
            pgrep.chmod(pgrep.stat().st_mode | stat.S_IXUSR)
            env_file = bin_dir / "compat.env"
            env_file.write_text(
                f"ZAKURA_RPC_URL={zakura.url}\nZCASHD_RPC_URL={zcashd.url}\n"
                "ZAKURA_COOKIE_FILE=\nZCASHD_COOKIE_FILE=\n"
                f"WATCHDOG_DEPLOYMENT_SUPPRESSION_FILE={bin_dir / 'marker'}\n"
            )
            environment = {"PATH": f"{bin_dir}:{os.environ.get('PATH', '/usr/bin:/bin')}"}

            def runner(command, timeout, max_output):
                argv = shlex.split(command[-1])
                return remote.run_bounded(
                    [sys.executable, "-I", str(HERE / "zakura-compat-check"), *argv[3:]],
                    timeout, max_output, env=environment,
                )

            target = monitor.CompatTarget(
                name="zakura-compat", ssh_target="root@159.203.113.196",
                env_file=str(env_file), known_hosts=None,
            )
            worker = monitor.ProbeWorker(target, runner=runner)
            result = worker.wait(30)
        self.assertTrue(result.passed, result)
        self.assertEqual(result.details["height_drift"], 1)


class ConfigTests(unittest.TestCase):
    def load(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fleets.toml"
            path.write_text(text)
            return monitor.load_compatibility_targets(path)

    def test_repository_config_has_the_one_known_target(self):
        targets = monitor.load_compatibility_targets(HERE / "fleet-watchdog.toml")
        self.assertEqual([(t.name, t.ssh_target) for t in targets],
                         [("zakura-compat", "root@159.203.113.196")])
        self.assertEqual((targets[0].interval, targets[0].timeout), (60.0, 120.0))

    def test_rejects_other_targets_and_unbounded_timeouts(self):
        base = '[[compatibility]]\nname = "zakura-compat"\nssh_target = "root@159.203.113.196"\n'
        self.assertEqual(len(self.load(base)), 1)
        self.assertEqual(self.load(""), [])
        for bad in (
            base.replace("159.203.113.196", "10.0.0.1"),
            base.replace('"zakura-compat"', '"other"'),
            base + "timeout = 300\n",
            base + "interval = 5\n",
            base + "remote_deadline = 150\n",
            base + 'checker = "relative"\n',
            base + "unknown = 1\n",
            base + base,
        ):
            with self.subTest(bad=bad), self.assertRaises(SystemExit):
                self.load(bad)


class Receiver:
    """Local stand-in for a Slack incoming webhook that can fail first."""

    def __init__(self, fail_first=0):
        self.attempts = 0
        self.accepted: list[str] = []
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                receiver.attempts += 1
                if receiver.attempts <= fail_first:
                    self.send_response(500)
                    self.end_headers()
                    return
                receiver.accepted.append(json.loads(body)["text"])
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/hook"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


if __name__ == "__main__":
    socket.setdefaulttimeout(30)
    unittest.main()
