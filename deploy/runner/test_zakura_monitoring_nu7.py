#!/usr/bin/env python3
"""NU7 endpoint checks, also shipped for installed monitoring validation."""

from __future__ import annotations

import argparse
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from zakura_monitoring import nu7  # noqa: E402

SPEC = importlib.util.spec_from_file_location("nu7_watchdog_tests", HERE / "zakura-cluster-watchdog.py")
assert SPEC is not None and SPEC.loader is not None
watchdog = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = watchdog
SPEC.loader.exec_module(watchdog)


def snapshot(now=1000, status="live"):
    return {"schemaVersion": 1, "generatedAt": now,
            "status": {"schemaVersion": 1, "observedAt": now, "status": status}}


class Nu7MonitoringTests(unittest.TestCase):
    def setUp(self):
        self.target = nu7.Target("nu7", "https://api.example/dashboard", "https://example/nu7/")
        self.args = argparse.Namespace(dashboard_down_after=600, request_timeout=20, dry_run=True)
        self.sent = []
        self.checkpoints = []
        self.instance = watchdog.Watchdog([], self.args, nu7_dashboard=[self.target],
            checkpoint=lambda state: self.checkpoints.append(copy.deepcopy(state)))
        self.state = {"version": 1}

    def poll(self, payload, now=1000, suppressed=False, accepted=True):
        with patch.object(watchdog, "fetch_json", side_effect=payload if isinstance(payload, Exception) else None,
                          return_value=payload), patch.object(watchdog, "post_slack",
                          side_effect=lambda text, args: (self.sent.append(text), accepted)[1]):
            self.instance.handle_nu7_dashboard(self.state, self.target, now, suppressed)

    def test_fresh_live_and_degraded_do_not_alert(self):
        for status in ("live", "degraded"):
            self.poll(snapshot(status=status))
        self.assertEqual(self.sent, [])

    def test_each_timestamp_can_detect_frozen_data_at_threshold(self):
        for field in ("generatedAt", "observedAt"):
            with self.subTest(field=field):
                self.state, self.sent = {"version": 1}, []
                data = snapshot()
                container = data if field == "generatedAt" else data["status"]
                container[field] = 401
                self.poll(data)
                self.assertEqual(self.sent, [])
                self.poll(data, now=1001)
                self.assertEqual(len(self.sent), 1)
                self.assertIn("stale", self.sent[0])
                self.poll(data, now=1100)
                self.assertEqual(len(self.sent), 1)

    def test_invalid_responses_are_failures(self):
        invalid = []
        for value in (None, True, "1000", float("nan"), float("inf"), -1):
            for field in ("generatedAt", "observedAt"):
                data = snapshot()
                (data if field == "generatedAt" else data["status"])[field] = value
                invalid.append(data)
        for field in ("generatedAt", "status", "schemaVersion"):
            data = snapshot()
            del data[field]
            invalid.append(data)
        for field in ("observedAt", "schemaVersion", "status"):
            data = snapshot()
            del data["status"][field]
            invalid.append(data)
        invalid.extend([snapshot(status="unknown"), {"schemaVersion": True}])
        for data in invalid:
            with self.subTest(data=data):
                self.state, self.sent = {"version": 1}, []
                self.poll(data)
                self.assertEqual(self.sent, [])
                self.poll(data, now=1600)
                self.assertEqual(len(self.sent), 1)
                self.assertTrue(self.state["nu7_dashboard"]["nu7"]["alerting"])
        self.assertIsNotNone(nu7.failure(snapshot(1061), 1000, 600))
        self.assertIsNone(nu7.failure(snapshot(1060), 1000, 600))
        self.state, self.sent = {"version": 1}, []
        self.poll(snapshot(1061))
        self.poll(snapshot(1661), now=1600)
        self.assertEqual(len(self.sent), 1)

    def test_changing_failure_reasons_preserves_timer(self):
        self.poll(TimeoutError())
        self.poll(snapshot(1300, "unavailable"), now=1300)
        self.poll({"schemaVersion": 9}, now=1599)
        self.assertEqual(self.sent, [])
        self.poll(urllib.error.HTTPError(self.target.url, 503, "error", {}, io.BytesIO()), now=1600)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("10m", self.sent[0])
        self.poll(snapshot(1700, "unavailable"), now=1700)
        self.assertEqual(len(self.sent), 1)

    def test_fetch_errors_and_unavailable_alert_after_ten_minutes(self):
        for failure in (TimeoutError(), json.JSONDecodeError("bad", "", 0),
                        urllib.error.URLError("offline"),
                        urllib.error.HTTPError(self.target.url, 500, "bad", {}, io.BytesIO()),
                        snapshot(status="unavailable")):
            with self.subTest(failure=failure):
                self.state, self.sent = {"version": 1}, []
                self.poll(failure)
                self.poll(failure, now=1599)
                self.assertEqual(self.sent, [])
                self.poll(failure, now=1600)
                self.assertEqual(len(self.sent), 1)

    def test_short_failure_does_not_send_recovery(self):
        self.poll(TimeoutError())
        self.poll(snapshot(1200), now=1200)
        self.assertEqual(self.sent, [])

    def test_recovery_requires_fresh_valid_data(self):
        self.poll(snapshot(100), now=1000)
        self.poll(snapshot(1100, "unavailable"), now=1100)
        self.poll({"schemaVersion": 9}, now=1200)
        self.assertEqual(len(self.sent), 1)
        self.poll(snapshot(1300, "degraded"), now=1300)
        self.poll(snapshot(1400), now=1400)
        self.assertEqual(len(self.sent), 2)
        self.assertIn("recovered", self.sent[-1])

    def test_restart_retains_failure_timer_and_delivery_queue(self):
        self.poll(TimeoutError())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            watchdog.save_state(path, self.state)
            self.state = watchdog.load_state(path)
            self.poll(TimeoutError(), now=1600, accepted=False)
            watchdog.save_state(path, self.state)
            self.state = watchdog.load_state(path)
            self.instance = watchdog.Watchdog([], self.args, nu7_dashboard=[self.target],
                checkpoint=lambda state: self.checkpoints.append(copy.deepcopy(state)))
            # A recovery while Slack is down must queue after the owed failure.
            self.poll(snapshot(1700), now=1700, accepted=False)
            self.assertEqual(len(self.state["nu7_dashboard_pending_delivery"]["nu7"]["messages"]), 2)
            self.poll(snapshot(1800), now=1800)
            self.poll(snapshot(1900), now=1900)
            self.assertEqual(self.state["nu7_dashboard_pending_delivery"], {})
            self.assertEqual(self.state["nu7_dashboard"]["nu7"]["condition"], "ok")
            self.assertIn("unavailable", self.sent[-2])
            self.assertIn("recovered", self.sent[-1])
            self.assertTrue(self.checkpoints)

    def test_suppression_defers_failure_and_pending_delivery(self):
        self.poll(snapshot(100), suppressed=True)
        self.assertEqual(self.sent, [])
        self.poll(snapshot(100), now=1100, accepted=False)
        self.poll(snapshot(100), now=1200, suppressed=True)
        self.assertEqual(len(self.sent), 1)
        self.poll(snapshot(100), now=1300)
        self.assertEqual(len(self.sent), 2)
        self.assertEqual(self.sent[0], self.sent[1])

    def test_suppressed_recovery_is_delivered_after_the_window(self):
        self.poll(snapshot(100))
        self.poll(snapshot(1100), now=1100, suppressed=True)
        self.assertEqual(len(self.sent), 1)
        self.poll(snapshot(1200), now=1200)
        self.assertEqual(len(self.sent), 2)
        self.assertIn("recovered", self.sent[-1])
        self.assertEqual(self.state["nu7_dashboard_pending_delivery"], {})

    def test_endpoint_delivery_keeps_other_lanes_state(self):
        self.state["compatibility"] = {"zakura-compat": {"condition": "failing"}}
        self.state["pending_delivery"] = {"testnet": {"messages": ["queued"]}}
        other = copy.deepcopy(self.state)
        self.poll(snapshot(100))
        for key in ("compatibility", "pending_delivery"):
            self.assertEqual(self.state[key], other[key])

    def test_run_once_wires_configured_endpoint_without_paging(self):
        self.args.suppression_file = Path("/nonexistent/nu7-monitor-test")
        self.args.mac_comparison = None
        with patch.object(watchdog, "time") as clock, patch.object(watchdog, "fetch_json", return_value=snapshot(100)), \
                patch.object(watchdog, "post_slack", return_value=True) as slack, \
                patch.object(self.instance, "reconcile_pagerduty") as paging:
            clock.time.return_value = 1000
            self.instance.run_once(self.state)
        slack.assert_called_once()
        self.assertTrue(self.state["nu7_dashboard"]["nu7"]["alerting"])
        self.assertNotIn("nu7_dashboard", [incident.bucket for incident in watchdog.PAGED_INCIDENTS])
        paging.assert_called_once()

    def test_config_is_optional_and_rejects_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text("")
            self.assertEqual(nu7.load_targets(path), [])
            config = '[[nu7_dashboard]]\nname="nu7"\nurl="https://api.example/dashboard"\ndashboard_url="https://example/nu7/"\n'
            path.write_text(config)
            self.assertEqual(nu7.load_targets(path), [self.target])
            for bad in (config + config, config.replace("https://", "http://"), config + 'other="unknown"\n'):
                path.write_text(bad)
                with self.assertRaises(SystemExit):
                    nu7.load_targets(path)


if __name__ == "__main__":
    unittest.main()
