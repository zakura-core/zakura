#!/usr/bin/env python3
"""Tests for the fleet watchdog's compatibility lane.

They run from the repository and from an installed release: every module is
loaded relative to this file, Slack is replaced by a local fake or a local HTTP
receiver, and all state lives in temporary directories.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import importlib.util
import json
import os
import signal
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

from zakura_monitoring import compat, delivery, monitor, remote, slack, state as state_module  # noqa: E402

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
        "active" if suppressed_until else "missing", suppressed_until, completed_at=at,
    )


def failing(predicate="height_drift", at=NOW, suppressed_until=None, **kwargs):
    kwargs.setdefault("zakura", 3_000_100)
    return monitor.ProbeResult(
        True, "fail", predicate, None, details(**kwargs), at,
        "active" if suppressed_until else "missing", suppressed_until, completed_at=at,
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
    def test_delayed_pass_cannot_recover_or_qualify_as_a_fresh_probe(self):
        self.step(failing())
        self.step(passing(at=NOW + 60), now=NOW + 660)
        self.assertTrue(self.entry["alerting"])
        self.assertEqual(len(self.posted), 1)
        record = self.state[monitor.COMPAT_PROBES][TARGET.name]
        self.assertEqual(record["last"]["error_kind"], "stale_outcome")
        self.assertFalse(record["last"]["valid"])
        self.assertEqual(record["last"]["completed_at"], NOW + 60)
        self.assertEqual(record.get("passed", 0), 0)
        self.assertNotIn("last_pass", record)
        self.step(passing(at=NOW + 720), now=NOW + 720)
        self.assertFalse(self.entry["alerting"])
        self.assertEqual(len(self.posted), 2)

    def test_delayed_pass_cannot_queue_recovery_behind_undelivered_failure(self):
        self.accept = False
        self.step(failing())
        self.step(passing(at=NOW + 60), now=NOW + 660)
        self.assertEqual(len(self.pending["messages"]), 1)
        self.assertTrue(self.pending["state"]["alerting"])
        self.accept = True
        self.step(None, now=NOW + 670)
        self.assertTrue(self.entry["alerting"])
        self.assertEqual(len(self.posted), 1)

    def test_consumption_freshness_boundary_and_invalid_timestamps(self):
        for age, accepted in ((120, True), (120.01, False), (-1, False)):
            result = monitor.fresh_result(passing(), NOW + age, TARGET.timeout)
            self.assertEqual(result.passed, accepted)
            self.assertEqual(result.completed_at, NOW)
        for stamp in (0, float("nan"), float("inf"), None):
            result = monitor.fresh_result(replace(passing(), completed_at=stamp), NOW, TARGET.timeout)
            self.assertFalse(result.passed)
        result = monitor.fresh_result(replace(passing(), observed_at=NOW - 600), NOW, TARGET.timeout)
        self.assertFalse(result.passed)

    def test_fresh_probe_keeps_actual_completion_time_in_telemetry(self):
        self.step(passing(), now=NOW + 60)
        self.assertEqual(self.state[monitor.COMPAT_PROBES][TARGET.name]["last"]["completed_at"], NOW)

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
        self.assertIn("restored", self.posted[1])
        self.assertIn("Zakura and zcashd are back in sync.", self.posted[1])
        self.assertEqual(self.entry, {"condition": "ok", "alerting": False})

    def test_healthy_start_is_silent(self):
        self.step(passing())
        self.step(None, now=NOW + 30)
        self.assertEqual(self.posted, [])

    def test_unavailable_probe_alerts_but_never_recovers(self):
        self.step(missing_checker())
        self.assertEqual(len(self.posted), 1)
        self.assertIn("checker is missing", self.posted[0])
        for reason in ("ssh_timeout", "stale_outcome", "malformed_outcome"):
            self.step(monitor.unavailable(reason, NOW + 60), now=NOW + 60)
        self.assertEqual(len(self.posted), 1)
        self.assertTrue(self.entry["alerting"])

    def test_alert_includes_relevant_measurements_and_keeps_full_telemetry(self):
        self.step(failing(peers=1, zakura=3_000_041, zcashd=3_000_000))
        text = self.posted[0]
        for expected in (
            "Zakura compatibility problem", "zakura-compat",
            "Zakura is *41 blocks ahead of* zcashd (limit: 30).",
            "Heights: Zakura 3,000,041 · zcashd 3,000,000",
            "_Observed 15 Jan 08:00 UTC_",
        ):
            self.assertIn(expected, text)
        self.assertEqual(len(text.splitlines()), 4)
        for omitted in ("root", "159.203.113.196", "zcashd_compat_sync",
                        "height＿drift", "peers", "2027-01-15T", "nonce"):
            self.assertNotIn(omitted, text)
        record = self.state[monitor.COMPAT_PROBES][TARGET.name]["last"]
        self.assertEqual(record["predicate"], "height_drift")
        self.assertEqual(record["details"], details(zakura=3_000_041))
        self.assertEqual(record["observed_at"], NOW)

    def test_probe_telemetry_records_heights(self):
        self.step(passing(zakura=10, zcashd=9))
        record = self.state[monitor.COMPAT_PROBES][TARGET.name]
        self.assertEqual((record["completed"], record["passed"]), (1, 1))
        self.assertEqual(record["last"]["details"]["zakura_height"], 10)


class FormattingTests(unittest.TestCase):
    def test_peer_alert_omits_unrelated_heights_and_missing_values(self):
        for peers in (0, 2):
            with self.subTest(peers=peers):
                text = monitor.alert_text(TARGET, failing("peer_pinning", peers=peers, zakura=None))
                self.assertIn(f"zcashd has *{peers} peers*; expected *1*.", text)
                self.assertEqual(len(text.splitlines()), 3)
                for omitted in ("height", "drift", "predicate", "check `", "root@", " - "):
                    self.assertNotIn(omitted, text)

    def test_drift_alert_describes_both_directions(self):
        for height, direction in ((3_000_011, "ahead of"), (2_999_989, "behind")):
            with self.subTest(direction=direction):
                text = monitor.alert_text(TARGET, failing(zakura=height, maximum=10))
                self.assertIn(f"Zakura is *11 blocks {direction}* zcashd (limit: 10).", text)
                self.assertIn(f"Heights: Zakura {height:,} · zcashd 3,000,000", text)

    def test_process_and_rpc_errors_are_explained_without_raw_metadata(self):
        for predicate, summary in (
            ("zakurad_process", "Zakura is not running."),
            ("zcashd_process", "zcashd is not running."),
        ):
            with self.subTest(predicate=predicate):
                text = monitor.alert_text(TARGET, failing(predicate))
                self.assertIn(summary, text)
                self.assertNotIn("Heights:", text)
        for kind in compat.ERROR_KINDS:
            with self.subTest(kind=kind):
                result = monitor.ProbeResult(True, "fail", "zcashd_getblockcount", kind,
                                             {}, NOW, "missing")
                text = monitor.alert_text(TARGET, result)
                self.assertIn("Could not read zcashd's block height.", text)
                self.assertIn(monitor.RPC_ERROR_SUMMARIES[kind], text)
                self.assertEqual(len(text.splitlines()), 3)

    def test_unavailable_and_incomplete_results_use_safe_plain_explanations(self):
        for kind, summary in monitor.UNAVAILABLE_REASONS.items():
            with self.subTest(kind=kind):
                text = monitor.alert_text(TARGET, monitor.unavailable(kind, NOW))
                self.assertIn(summary, text)
                self.assertEqual(len(text.splitlines()), 3)
        raw = "secret-credential://user:password@host"
        for valid, predicate in ((False, raw), (True, raw), (True, "height_drift")):
            with self.subTest(valid=valid, predicate=predicate):
                result = monitor.ProbeResult(valid, "fail", predicate, raw, {}, NOW, "missing")
                text = monitor.alert_text(TARGET, result)
                self.assertNotIn(raw, text)
                self.assertNotIn(" - ", text)

    def test_recovery_is_short_and_has_one_observation_time(self):
        text = monitor.recovery_text(TARGET, passing(), {"predicate": "peer_pinning"})
        self.assertIn("Zakura compatibility restored", text)
        self.assertIn("Zakura and zcashd are back in sync.", text)
        self.assertEqual(len(text.splitlines()), 3)
        self.assertEqual(text.count("Observed"), 1)
        for omitted in ("peer", "height", "drift", "root", "recovered from"):
            self.assertNotIn(omitted, text)

    def test_host_name_cannot_inject_slack_formatting_or_mentions(self):
        target = monitor.CompatTarget(name="<@U123> `injected`\n<!channel>",
                                      ssh_target=TARGET.ssh_target, known_hosts=None)
        text = monitor.alert_text(target, failing("zcashd_process"))
        self.assertNotIn("<@", text)
        self.assertNotIn("<!channel>", text)
        self.assertEqual(text.count("`"), 2)
        self.assertEqual(len(text.splitlines()), 3)

    def test_optional_batch_header_preserves_fleet_default_and_message_bounds(self):
        failure = monitor.alert_text(TARGET, failing())
        recovery = monitor.recovery_text(TARGET, passing(), {})
        fleet = delivery.batch_messages([failure], NOW)
        self.assertTrue(fleet[0].startswith("*Fleet status updates* — observed 2027-01-15T08:00:00+00:00"))
        standalone = delivery.batch_messages([failure, recovery], NOW, title=None)
        self.assertEqual(standalone, [failure + delivery.BATCH_SEPARATOR + recovery])
        chunks = delivery.batch_messages([failure] * 200, NOW, title=None)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(sum(chunk.count("Zakura compatibility problem") for chunk in chunks), 200)
        self.assertTrue(all(len(chunk) <= slack.MAX_SLACK_MESSAGE_CHARS for chunk in chunks))


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
        self.assertIn("problem", self.posted[0])
        self.assertIn("restored", self.posted[1])
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
        self.assertIn("Zakura compatibility problem", receiver.accepted[0])
        self.assertNotIn("compatibility updates", receiver.accepted[0])
        mock.patch.stopall()

    def test_slack_absent_keeps_the_alert_pending(self):
        mock.patch.object(watchdog, "post_slack", side_effect=watchdog.slack.post_slack).start()
        environ = {key: value for key, value in os.environ.items() if "SLACK" not in key}
        with mock.patch.dict(os.environ, environ, clear=True):
            self.step(failing())
        self.assertEqual(len(self.pending["messages"]), 1)
        mock.patch.stopall()


class NamespaceTests(LaneCase):
    def test_unknown_buckets_such_as_pagerduty_survive_every_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            live = {
                "version": 1, "nodes": {}, "fleets": {}, "shared_stalls": {},
                "release_state": {}, "decisions": {}, "propagation": {}, "mac_forks": {},
                "mac_comparison": {"mainnet": {"condition": "ok", "alerting": False}},
                "pending_delivery": {},
                "pagerduty": {"incidents": {"mainnet/node-a": {"dedup_key": "k1"}}},
                "future_bucket": [1, 2, 3],
            }
            path.write_text(json.dumps(live))
            fleet = watchdog.Fleet("mainnet", "http://127.0.0.1:9/data", "http://127.0.0.1:9/")
            self.agent = self.make_agent(fleets=[fleet])
            self.agent.args = argparse.Namespace(**{
                **vars(make_args()), "down_after": 600.0, "stalled_after": 600.0,
                "shared_stalled_after": 1800.0, "starting_grace": 120.0,
                "dashboard_down_after": 600.0, "request_timeout": 1.0})
            snapshot = {"rows": [{"name": "node-a", "health": "healthy", "height": 1,
                                  "block_hash": "ab", "seconds_since_advanced": 1}]}
            for result in (failing(), passing(at=NOW + 60)):
                state = state_module.load_state(path)
                self.worker.results.append(result)
                with mock.patch.object(watchdog, "fetch_json", return_value=snapshot), \
                        mock.patch.object(watchdog.time, "time", return_value=result.completed_at):
                    self.agent.run_once(state)
                state_module.save_state(path, state)
            restored = json.loads(path.read_text())
        self.assertEqual(restored["pagerduty"], live["pagerduty"])
        self.assertEqual(restored["future_bucket"], [1, 2, 3])
        self.assertEqual(restored["mac_comparison"], live["mac_comparison"])
        self.assertEqual(restored["version"], 1)
        self.assertEqual(len(self.posted), 2)

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
    def test_pending_failure_stays_suppressed_after_restart_until_expiry(self):
        self.accept = False
        self.step(failing())
        queued = json.loads(json.dumps(self.pending))
        self.accept = True
        until = NOW + 600
        self.step(failing(at=NOW + 60, suppressed_until=until), now=NOW + 60)
        self.assertEqual(self.posted, [])
        self.assertEqual(self.pending, queued)
        self.assertEqual(self.entry, {})

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            state_module.save_state(path, self.state)
            self.worker = FakeWorker()
            self.agent = self.make_agent()
            self.state = state_module.load_state(path)
        self.step(None, now=NOW + 120)
        self.step(missing_checker(NOW + 180), now=NOW + 180)
        self.assertEqual(self.posted, [])
        self.assertEqual(self.pending, queued)
        self.assertEqual(self.entry, {})
        self.step(None, now=until)
        self.assertEqual(len(self.posted), 1)
        self.assertIsNone(self.pending)
        self.assertTrue(self.entry["alerting"])
        self.step(failing(at=until + 60), now=until + 60)
        self.assertEqual(len(self.posted), 1, "the queued failure must not repeat")

    def test_pending_failure_and_recovery_resume_in_order_after_suppression(self):
        self.accept = False
        self.step(failing())
        self.step(passing(at=NOW + 60), now=NOW + 60)
        queued = json.loads(json.dumps(self.pending))
        self.accept = True
        until = NOW + 600
        self.step(passing(at=NOW + 120, suppressed_until=until), now=NOW + 120)
        self.step(None, now=until - 1)
        self.assertEqual(self.posted, [])
        self.assertEqual(self.pending, queued)
        self.assertEqual(self.entry, {})
        self.step(None, now=until)
        self.assertEqual(len(self.posted), 1)
        self.assertIn("problem", self.posted[0])
        self.assertEqual(len(self.pending["messages"]), 1)
        self.step(None, now=until + 1)
        self.assertEqual(len(self.posted), 2)
        self.assertIn("restored", self.posted[1])
        self.assertIsNone(self.pending)
        self.assertEqual(self.entry, {"condition": "ok", "alerting": False})

    def test_removed_marker_releases_pending_failure_before_previous_expiry(self):
        self.accept = False
        self.step(failing())
        self.accept = True
        self.step(failing(at=NOW + 60, suppressed_until=NOW + 600), now=NOW + 60)
        self.assertEqual(self.posted, [])
        self.step(failing(at=NOW + 120), now=NOW + 120)
        self.assertEqual(len(self.posted), 1)
        self.assertIsNone(self.pending)
        self.assertTrue(self.entry["alerting"])

    def test_pending_recovery_does_not_acknowledge_during_suppression(self):
        self.step(failing())
        self.posted.clear()
        self.accept = False
        self.step(passing(at=NOW + 60), now=NOW + 60)
        queued = json.loads(json.dumps(self.pending))
        self.accept = True
        self.step(passing(at=NOW + 120, suppressed_until=NOW + 600), now=NOW + 120)
        self.step(missing_checker(NOW + 180), now=NOW + 180)
        self.assertEqual(self.posted, [])
        self.assertEqual(self.pending, queued)
        self.assertTrue(self.entry["alerting"])
        self.step(None, now=NOW + 600)
        self.assertEqual(len(self.posted), 1)
        self.assertIn("restored", self.posted[0])
        self.assertIsNone(self.pending)
        self.assertFalse(self.entry["alerting"])

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
        self.assertIn("restored", self.posted[0])
        self.assertFalse(self.entry["alerting"])


def bounded(outcome: dict | bytes | None, returncode=0, timed_out=False, oversized=False):
    stdout = outcome if isinstance(outcome, bytes) else json.dumps(outcome or {}).encode()
    if outcome is None:
        stdout = b""
    return remote.BoundedResult(returncode, stdout, timed_out, oversized, 1.0)


def outcome_dict(nonce="n", status="pass", predicate="in_sync", observed_at=NOW, **changes):
    value = {
        "schema": compat.SCHEMA, "check": compat.CHECK_NAME, "status": status,
        "predicate": predicate, "summary": "x", "details": details(maximum=10),
        "error_kind": None, "observed_at": observed_at, "nonce": nonce,
        "suppression": {"state": "missing", "active": False, "until": None, "max_seconds": 1200},
    }
    value.update(changes)
    return value


class ParseTests(unittest.TestCase):
    def parse(self, result, nonce="n"):
        return monitor.parse_probe_output(result, nonce, NOW - 5, NOW + 5, 10)

    def test_valid_pass(self):
        result = self.parse(bounded(outcome_dict()))
        self.assertTrue(result.passed)

    def test_reported_limit_must_match_the_requested_policy(self):
        for maximum, drift in ((30, 11), (9, 0), (11, 0)):
            with self.subTest(maximum=maximum, drift=drift):
                result = monitor.parse_probe_output(
                    bounded(outcome_dict(details=details(zakura=100 + drift, zcashd=100,
                                                        maximum=maximum))),
                    "n", NOW - 5, NOW + 5, 10)
                self.assertFalse(result.valid)
                self.assertFalse(result.passed)
        for drift in (9, 10):
            result = monitor.parse_probe_output(
                bounded(outcome_dict(details=details(zakura=100 + drift, zcashd=100,
                                                    maximum=10))),
                "n", NOW - 5, NOW + 5, 10)
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
        unreaped = remote.BoundedResult(None, b"", True, False, 120)
        self.assertEqual(self.parse(unreaped).error_kind, "ssh_timeout")
        self.assertFalse(self.parse(unreaped).passed)

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
    def test_delayed_queue_is_stale_even_when_wall_clock_has_not_advanced(self):
        for consume in ("poll", "wait"):
            mono = [0.0]
            worker = monitor.ProbeWorker(TARGET, runner=lambda *_: bounded(outcome_dict()),
                                         clock=lambda: NOW, monotonic=lambda: mono[0],
                                         nonce_factory=lambda: "n")
            worker.poll()
            worker._thread.join(5)
            worker._next_due = float("inf")
            mono[0] = 600.0
            result = worker.poll() if consume == "poll" else worker.wait(1)
            self.assertFalse(result.passed)
            self.assertEqual(result.error_kind, "stale_outcome")
            self.assertEqual(result.completed_at, NOW)

    def test_wait_keep_does_not_refresh_age_before_later_poll(self):
        mono = [0.0]
        worker = monitor.ProbeWorker(TARGET, runner=lambda *_: bounded(outcome_dict()),
                                     clock=lambda: NOW, monotonic=lambda: mono[0],
                                     nonce_factory=lambda: "n")
        self.assertTrue(worker.wait(1, keep=True).passed)
        worker._next_due = float("inf")
        mono[0] = 600.0
        self.assertFalse(worker.poll().passed)

    def test_late_passing_completion_cannot_recover_after_overrun(self):
        mono = [0.0]
        wall = [NOW]
        def runner(*_args):
            mono[0] = 130.0
            wall[0] = NOW + 130
            return bounded(outcome_dict(observed_at=wall[0]))
        worker = monitor.ProbeWorker(TARGET, runner=runner, clock=lambda: wall[0],
                                     monotonic=lambda: mono[0], nonce_factory=lambda: "n")
        result = worker.wait(1)
        self.assertFalse(result.passed)
        self.assertEqual(result.error_kind, "probe_overrun")

    def test_worker_checks_the_configured_limit_in_remote_results(self):
        worker = monitor.ProbeWorker(
            monitor.CompatTarget(name="zakura-compat", ssh_target="root@159.203.113.196",
                                 known_hosts=None, height_max_drift=10),
            runner=lambda *_args: bounded(outcome_dict(details=details(maximum=30))),
            clock=lambda: NOW, nonce_factory=lambda: "n")
        result = worker.wait(1)
        self.assertIsNotNone(result)
        self.assertFalse(result.valid)
        self.assertFalse(result.passed)

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
             "--height-max-drift", "10", "--nonce", "abc"],
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
    def test_worker_can_probe_again_after_a_detached_stdout_holder_timeout(self):
        raw = []
        clock = [0.0]
        parent = (
            "import subprocess,sys,time; "
            "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(8)'], "
            "start_new_session=True); print(child.pid,flush=True); time.sleep(30)"
        )

        def runner(*_args):
            if not raw:
                raw.append(remote.run_bounded([sys.executable, "-c", parent], 0.5, 100))
                return raw[0]
            return bounded(outcome_dict())

        worker = monitor.ProbeWorker(TARGET, runner=runner, clock=lambda: NOW,
                                     monotonic=lambda: clock[0], nonce_factory=lambda: "n")
        try:
            failed = worker.wait(2)
            self.assertIsNotNone(failed)
            self.assertEqual(failed.error_kind, "ssh_timeout")
            self.assertFalse(failed.passed)
            clock[0] = 60.0
            worker.poll()
            self.assertTrue(worker.wait(2).passed)
            self.assertEqual(worker.starts, 2)
        finally:
            if raw and raw[0].stdout.strip().isdigit():
                try:
                    os.kill(int(raw[0].stdout), signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_detached_stdout_holder_cannot_extend_timeout(self):
        for parent_exits in (False, True):
            with self.subTest(parent_exits=parent_exits):
                child = "import time; time.sleep(8)"
                parent = (
                    "import subprocess,sys,time; "
                    f"child=subprocess.Popen([sys.executable,'-c',{child!r}], "
                    "start_new_session=True); print(child.pid,flush=True); "
                    + ("" if parent_exits else "time.sleep(30)")
                )
                result = remote.run_bounded([sys.executable, "-c", parent], 0.5, 100)
                try:
                    self.assertTrue(result.timed_out)
                    self.assertLess(result.elapsed, 2)
                    self.assertTrue(result.stdout.strip().isdigit())
                finally:
                    if result.stdout.strip().isdigit():
                        try:
                            os.kill(int(result.stdout), signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_stdout_eof_does_not_skip_process_timeout(self):
        result = remote.run_bounded(
            [sys.executable, "-c", "import os,time; os.close(1); time.sleep(30)"],
            0.2, 100,
        )
        self.assertTrue(result.timed_out)
        self.assertLess(result.elapsed, 2)

    def test_zero_timeout_is_bounded(self):
        result = remote.run_bounded([sys.executable, "-c", "import time; time.sleep(30)"],
                                    0, 100)
        self.assertTrue(result.timed_out)
        self.assertLess(result.elapsed, 1)

    def test_success_preserves_output_and_exit_code(self):
        result = remote.run_bounded([sys.executable, "-c", "print('outcome'); exit(7)"],
                                    5, 100)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(result.stdout, b"outcome\n")
        self.assertFalse(result.timed_out)
        self.assertFalse(result.oversized)

    def test_reaping_cannot_wait_past_the_same_deadline(self):
        for kill_error in (ProcessLookupError, PermissionError):
            with self.subTest(kill_error=kill_error):
                descriptor, writer = os.pipe()
                os.close(writer)
                with os.fdopen(descriptor, "rb") as pipe:
                    process = mock.Mock(pid=1234, stdout=pipe)
                    process.poll.return_value = None
                    process.wait.side_effect = remote.subprocess.TimeoutExpired("fixture", 0.2)
                    with mock.patch.object(remote.subprocess, "Popen", return_value=process), \
                            mock.patch.object(remote.os, "killpg", side_effect=kill_error):
                        result = remote.run_bounded(["fixture"], 0.2, 100)
                    self.assertTrue(pipe.closed)
                self.assertTrue(result.timed_out)
                self.assertIsNone(result.returncode)
                self.assertLess(result.elapsed, 1)
                self.assertEqual(process.wait.call_count, 2)
                for call in process.wait.call_args_list:
                    self.assertGreaterEqual(call.kwargs["timeout"], 0)
                    self.assertLessEqual(call.kwargs["timeout"], 0.2)

    def test_transient_nonblocking_read_is_retried(self):
        read = os.read
        calls = 0

        def transient_read(*args):
            nonlocal calls
            if not os.get_blocking(args[0]):
                calls += 1
                if calls == 1:
                    raise BlockingIOError
            return read(*args)

        with mock.patch.object(remote.os, "read", side_effect=transient_read):
            result = remote.run_bounded([sys.executable, "-c", "print('outcome')"], 5, 100)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"outcome\n")
        self.assertFalse(result.timed_out)
        self.assertGreater(calls, 1)

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
        # Production's effective Rust watchdog limit, pinned for the lane.
        self.assertEqual(targets[0].height_max_drift, 10)

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
            base + "height_max_drift = -1\n",
            base + 'height_max_drift = "10"\n',
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
