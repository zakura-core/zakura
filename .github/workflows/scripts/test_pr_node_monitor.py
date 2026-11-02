#!/usr/bin/env python3
"""Unit tests for the PR-node crossing verdict."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

SCRIPTS = Path(__file__).resolve().parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


monitor = load_module("pr_node_monitor", SCRIPTS / "pr-node-monitor.py")


def sample(height: int, finalized: int = 100, vct_fast_blocks: int = 1,
           vct_committed_blocks: int | None = 1) -> dict:
    return {
        "height": height,
        "estimated": height + 10,
        "peers": 1,
        "rss_mib": 100.0,
        "restarts": 0,
        "active_state": "active",
        "finalized_height": finalized,
        "vct_fast_blocks": vct_fast_blocks,
        "vct_committed_blocks": vct_committed_blocks,
    }


LOGS = {"errors": 0, "warns": 0, "panics": 0, "last_errors": []}


class CapacityQualification(unittest.TestCase):
    def observation(self, elapsed, *, active=0, saturation=0, deferred=0, peak=None):
        metrics = {field: 0 for field in monitor.CAPACITY_FIELDS}
        metrics.update(capacity=20, reserved=5, limit=15, active=active,
                       headroom=15-active, peak=active if peak is None else peak,
                       limit_reached_count=saturation, capacity_deferred_count=deferred)
        return {"elapsed": elapsed, "peers": 2, "pid": "123", "missing_parent": {
            "sync": metrics.copy(), "gossip": metrics.copy()}}

    def test_quiet_run_reports_margin_without_claiming_it_was_exercised(self):
        measured = monitor.summarize_capacity([self.observation(0), self.observation(1)], 1)
        self.assertEqual(measured["sync"]["verdict"], "ok")
        self.assertEqual(measured["sync"]["minimum_headroom"], 15)
        self.assertEqual(measured["sync"]["events_since_process_start"]["admitted_count"], 0)

    def test_startup_is_outside_the_window_but_later_connectivity_loss_fails(self):
        startup = dict(self.observation(0), peers=0)
        measured = monitor.summarize_capacity([startup, self.observation(1), self.observation(2)], 1)
        self.assertEqual(measured["sync"]["verdict"], "ok")
        self.assertEqual(measured["sync"]["start_elapsed"], 1)
        self.assertEqual(measured["sync"]["observed_seconds"], 1)
        lost = dict(self.observation(3), peers=0)
        self.assertEqual(monitor.summarize_capacity([self.observation(1), lost], 1)["sync"]["verdict"], "failed")

    def test_event_counter_catches_a_spike_between_scrapes(self):
        measured = monitor.summarize_capacity([
            self.observation(0), self.observation(1, saturation=1, peak=15)], 1)
        self.assertEqual(measured["sync"]["verdict"], "failed")
        self.assertEqual(measured["sync"]["peak"], 15)
        self.assertEqual(measured["sync"]["minimum_headroom"], 0)
        self.assertEqual(measured["sync"]["minimum_sampled_headroom"], 15)

    def test_near_limit_is_not_exhaustion(self):
        measured = monitor.summarize_capacity([
            self.observation(0, active=12), self.observation(1, active=14)], 1)
        self.assertEqual(measured["sync"]["verdict"], "ok")
        self.assertEqual(measured["sync"]["minimum_headroom"], 1)

    def test_grandfathered_reductions_are_reported_separately_from_admission_saturation(self):
        first = self.observation(0, active=8)
        reduced = self.observation(1, active=8)
        for values in reduced["missing_parent"].values():
            values.update(capacity=8, reserved=2, limit=6, headroom=0, capacity_reduction_overage_count=1)
        measured = monitor.summarize_capacity([first, reduced], 1)
        self.assertEqual(measured["sync"]["verdict"], "ok")
        self.assertEqual(measured["sync"]["events_since_process_start"]["capacity_reduction_overage_count"], 1)
        self.assertIsNone(measured["sync"]["minimum_headroom"])

    def test_missing_samples_counter_resets_and_process_changes_fail(self):
        for broken in (self.observation(5), {"elapsed": 1},
                       dict(self.observation(1), pid="456")):
            with self.subTest(broken=broken):
                measured = monitor.summarize_capacity([self.observation(0), broken], 1)
                self.assertEqual(measured["sync"]["verdict"], "failed")
        measured = monitor.summarize_capacity([
            self.observation(0, saturation=1), self.observation(1)], 1)
        self.assertTrue(any("Counter reset" in issue for issue in measured["sync"]["failures"]))

    def test_absent_metrics_do_not_mean_no_exhaustion(self):
        measured = monitor.summarize_capacity([{"elapsed": 0}], 1)
        self.assertEqual(measured["sync"]["verdict"], "incomplete")
        self.assertIsNone(monitor.capacity_metrics("")["sync"]["limit_reached_count"])

    def test_partial_gauges_report_incomplete_without_crashing(self):
        broken = self.observation(0)
        broken["missing_parent"]["sync"]["active"] = None
        measured = monitor.summarize_capacity([broken, self.observation(1)], 1)
        self.assertEqual(measured["sync"]["verdict"], "failed")

    def test_duration_metrics_keep_reasons_and_quantiles(self):
        measured = monitor.duration_metrics('''sync_missing_parent_charge_duration_seconds_sum{reason="parent_available"} 6
sync_missing_parent_charge_duration_seconds_count{reason="parent_available"} 3
sync_missing_parent_charge_duration_seconds{reason="parent_available",quantile="0.95"} 4
''')
        self.assertEqual(measured["sync"]["charge"]["parent_available"], {"sum": 6, "count": 3, "0.95": 4})

    def test_pressure_uses_event_timers_and_distinguishes_metadata_loss(self):
        first, last = self.observation(0), self.observation(1, saturation=2, peak=15)
        for values in last["missing_parent"].values():
            values.update(saturation_observed_seconds=1, saturation_total_seconds=.15,
                          saturation_longest_seconds=.1, expired_count=2, retry_limit_count=1,
                          capacity_evicted_count=3, overflow_rejected_count=4, reset_discarded_count=5,
                          deferred_created_count=10, retried_count=7)
        measured = monitor.summarize_capacity([first, last], 1)["sync"]["pressure"]
        self.assertEqual(measured["saturated_seconds"], .15)
        self.assertEqual(measured["saturated_fraction"], .15)
        self.assertEqual(measured["admission_saturation_episodes"], 2)
        self.assertEqual(measured["metadata_evictions"], {"expired": 2, "retry_limit": 1, "capacity_reduction": 3})
        self.assertEqual(measured["metadata_overflow_rejections"], 4)
        self.assertEqual(measured["metadata_discarded_on_reset"], 5)
        self.assertEqual(measured["deferred_retry_attempts"], 7)
        self.assertFalse(measured["open_at_end"])

    def test_pressure_rates_include_idle_time_after_the_last_timer_refresh(self):
        first, last = self.observation(0), self.observation(3600, saturation=1)
        last["missing_parent"]["sync"].update(saturation_observed_seconds=10, saturation_total_seconds=1)
        pressure = monitor.summarize_capacity([first, last], 3600)["sync"]["pressure"]
        self.assertEqual(pressure["admission_saturation_episodes_per_hour"], 1)
        self.assertEqual(pressure["saturated_fraction"], 1 / 3600)

    def test_open_episode_is_reported_without_claiming_its_completed_duration(self):
        first, last = self.observation(0), self.observation(1, saturation=1, active=15)
        last["missing_parent"]["sync"].update(saturation_active=1, saturation_observed_seconds=1,
                                               saturation_total_seconds=.3, saturation_current_duration_seconds=.3)
        pressure = monitor.summarize_capacity([first, last], 1)["sync"]["pressure"]
        self.assertTrue(pressure["open_at_end"])
        self.assertEqual(pressure["open_episode_elapsed_seconds"], .3)
        self.assertEqual(pressure["last_completed_episode_seconds"], 0)

    def test_diagnostic_continuation_preserves_the_failed_verdict(self):
        spike = self.observation(1, saturation=1)
        self.assertTrue(monitor.stop_for_capacity_failure(spike, required=True, continue_after_failure=False))
        self.assertFalse(monitor.stop_for_capacity_failure(spike, required=True, continue_after_failure=True))
        measured = monitor.summarize_capacity([self.observation(0), spike], 1)
        self.assertEqual(measured["sync"]["verdict"], "failed")

    def test_work_before_the_window_does_not_satisfy_semantic_coverage(self):
        self.assertEqual(monitor.counter_window([{"semantic_commits": 2000}, {"semantic_commits": 2100}],
                                                "semantic_commits"),
                         {"baseline": 2000, "final": 2100, "delta": 100})

    def test_isolated_process_observation_fails_when_identity_is_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            pid = Path(directory) / "node.pid"
            self.assertEqual(monitor.process_props(str(pid))["ActiveState"], "inactive")
            pid.write_text("invalid")
            self.assertEqual(monitor.process_props(str(pid))["ActiveState"], "inactive")
        with patch.object(monitor.Path, "read_text", side_effect=["123", "State:\tS (sleeping)\n"]):
            self.assertEqual(monitor.process_props("node.pid"), {"MainPID": "123", "ActiveState": "active", "NRestarts": "0"})
        with patch.object(monitor.Path, "read_text", side_effect=["123", "State:\tZ (zombie)\n"]):
            self.assertEqual(monitor.process_props("node.pid")["ActiveState"], "inactive")

    def test_isolated_follower_panics_are_detected_without_a_journal(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "node.log"
            log.write_text("thread 'worker' panicked at src/main.rs:1\n")
            with patch.object(monitor.subprocess, "run") as run:
                self.assertEqual(monitor.scan_logs(str(log), None)["panics"], 1)
                run.assert_not_called()

    def test_tip_progress_must_occur_in_the_measured_window(self):
        quiet = [{"height": 100, "reference_height": 100}] * 2
        self.assertEqual(len(monitor.tip_window_failures(quiet, "reference")), 2)
        active = [{"height": 100, "reference_height": 100}, {"height": 101, "reference_height": 101}]
        self.assertEqual(monitor.tip_window_failures(active, "reference"), [])
        self.assertTrue(monitor.tip_window_failures(active, None))


class StartupHeight(unittest.TestCase):
    def read(self, text: str):
        with tempfile.TemporaryDirectory() as out:
            path = Path(out) / "zakura.log"
            path.write_text(text)
            return monitor.read_startup_height(str(path))

    def test_reads_finalized_height_separately_from_best_chain_tip(self):
        height, error = self.read(
            "INFO loaded Zakura state cache chain_tip=Some((hash, Height(105))) "
            "finalized_tip_height=99\n"
        )
        self.assertEqual(height, 99)
        self.assertIsNone(error)

    def test_refuses_missing_empty_malformed_or_duplicate_height(self):
        for fields in ("", "finalized_tip_height=", "finalized_tip_height=None",
                       "finalized_tip_height=-1", "finalized_tip_height=99x",
                       "finalized_tip_height=99 finalized_tip_height=100"):
            with self.subTest(fields=fields):
                height, error = self.read(f"INFO loaded Zakura state cache {fields}\n")
                self.assertIsNone(height)
                self.assertIsNotNone(error)

    def test_refuses_missing_or_multiple_startups(self):
        for text in ("", "INFO finalized_tip_height=99\n",
                     "INFO loaded Zakura state cache finalized_tip_height=99\n" * 2):
            with self.subTest(text=text):
                height, error = self.read(text)
                self.assertIsNone(height)
                self.assertIsNotNone(error)

    def test_missing_file_returns_a_diagnostic(self):
        with tempfile.TemporaryDirectory() as out:
            height, error = monitor.read_startup_height(str(Path(out) / "absent.log"))
        self.assertIsNone(height)
        self.assertIsNotNone(error)

    def test_startup_height_proves_crossing_when_rpc_is_already_past_checkpoint(self):
        summary = monitor.build_summary(
            {}, [sample(101)], LOGS, 1.0, known_start_height=99,
            required_start_below=100, stop_after_height=100,
            required_finalized_at_least=100, require_vct_fast_blocks=True,
            require_startup_height=True, expected_snapshot_height=99,
        )
        self.assertEqual(summary["verdict"], "ok")
        self.assertEqual(summary["start_height"], 99)
        self.assertEqual(summary["first_observed_height"], 101)

    def test_snapshot_mismatch_or_start_at_checkpoint_fails(self):
        for start, expected in ((99, 98), (100, 100), (101, 101)):
            with self.subTest(start=start, expected=expected):
                summary = monitor.build_summary(
                    {}, [sample(102)], LOGS, 1.0, known_start_height=start,
                    required_start_below=100, require_startup_height=True,
                    expected_snapshot_height=expected,
                )
                self.assertEqual(summary["verdict"], "failed")
                self.assertIsNotNone(summary["startup_height_failure_reason"])

    def test_missing_startup_height_never_falls_back_to_rpc(self):
        summary = monitor.build_summary(
            {}, [sample(99), sample(101)], LOGS, 1.0,
            required_start_below=100, require_startup_height=True,
            startup_height_error="No startup state entry was recorded.",
        )
        self.assertEqual(summary["verdict"], "failed")
        self.assertIsNone(summary["start_height"])
        self.assertIn("No startup", summary["startup_height_failure_reason"])

    def test_monitor_checks_startup_log_and_writes_diagnostics(self):
        for text, expected, result in (
            ("INFO loaded Zakura state cache finalized_tip_height=99\n", 99, 0),
            ("INFO loaded Zakura state cache finalized_tip_height=99\n", 98, 1),
            ("INFO loaded Zakura state cache finalized_tip_height=100\n", 100, 1),
            ("", 99, 1),
            ("INFO loaded Zakura state cache finalized_tip_height=99\n" * 2, 99, 1),
        ):
            with self.subTest(text=text, expected=expected), tempfile.TemporaryDirectory() as out:
                log = Path(out) / "zakura.log"
                log.write_text(text)
                with patch.object(sys, "argv", [
                    "monitor", "--duration-minutes", "1", "--out", out,
                    "--log-file", str(log), "--require-startup-height",
                    "--expected-snapshot-height", str(expected),
                    "--required-start-below", "100", "--stop-after-height", "100",
                    "--required-finalized-at-least", "100", "--require-vct-fast-blocks",
                ]), patch.object(monitor, "take_sample", return_value=sample(101)), patch.object(
                    monitor, "scan_logs", return_value=LOGS
                ), patch.object(monitor.time, "monotonic", side_effect=[0, 301, 302]):
                    self.assertEqual(monitor.main(), result)
                summary = json.loads((Path(out) / "summary.json").read_text())
                if result:
                    reason = summary["startup_height_failure_reason"]
                    self.assertIsNotNone(reason)
                    self.assertIn(reason, (Path(out) / "summary.md").read_text())
                else:
                    self.assertEqual(summary["start_height"], 99)
                    self.assertEqual(summary["first_observed_height"], 101)

    def test_waits_for_startup_log_to_flush_after_rpc_is_available(self):
        with tempfile.TemporaryDirectory() as out, patch.object(sys, "argv", [
            "monitor", "--duration-minutes", "1", "--out", out,
            "--require-startup-height", "--required-start-below", "100",
            "--stop-after-height", "100",
        ]), patch.object(monitor, "take_sample", return_value=sample(101)) as take, patch.object(
            monitor, "read_startup_height", side_effect=[
                (None, "No startup state entry was recorded."), (99, None), (99, None),
            ]
        ), patch.object(monitor, "scan_logs", return_value=LOGS), patch.object(
            monitor.time, "monotonic", side_effect=[0, 1, 1, 2, 3]
        ), patch.object(monitor.time, "sleep"):
            self.assertEqual(monitor.main(), 0)
            self.assertEqual(take.call_count, 2)
            summary = json.loads((Path(out) / "summary.json").read_text())
            self.assertEqual(summary["start_height"], 99)


class HandoffCrossingVerdict(unittest.TestCase):
    def summary(self, heights: list[int], known_start: int | None):
        return monitor.build_summary(
            {"mode": "pre-checkpoint", "network": "mainnet"},
            [sample(height) for height in heights],
            LOGS,
            1.0,
            known_start_height=known_start,
            required_start_below=100,
            stop_after_height=100,
            required_finalized_at_least=100,
            require_vct_fast_blocks=True,
        )

    def test_height_stamped_snapshot_proves_start_if_rpc_appears_after_crossing(self):
        summary = self.summary([101], known_start=99)

        self.assertEqual(summary["verdict"], "ok")
        self.assertEqual(summary["start_height"], 99)
        self.assertEqual(summary["first_observed_height"], 101)

    def test_fails_if_end_does_not_cross_handoff(self):
        summary = self.summary([99, 100], known_start=99)

        self.assertEqual(summary["verdict"], "failed")

    def test_fails_if_snapshot_did_not_start_below_handoff(self):
        summary = self.summary([101], known_start=100)

        self.assertEqual(summary["verdict"], "failed")

    def test_first_rpc_sample_can_prove_legacy_snapshot_start(self):
        summary = self.summary([99, 101], known_start=None)

        self.assertEqual(summary["verdict"], "ok")
        self.assertEqual(summary["start_height"], 99)

    def test_fails_without_vct_fast_path_activity(self):
        summary = monitor.build_summary(
            {"mode": "pre-checkpoint", "network": "mainnet"},
            [sample(101, vct_fast_blocks=0, vct_committed_blocks=0)],
            LOGS,
            1.0,
            known_start_height=99,
            required_start_below=100,
            stop_after_height=100,
            required_finalized_at_least=100,
            require_vct_fast_blocks=True,
        )

        self.assertEqual(summary["verdict"], "failed")
        self.assertIn("without a successful VCT commit", summary["vct_failure_reason"])

    def test_precommit_counter_cannot_pass_without_a_successful_commit(self):
        summary = monitor.build_summary(
            {}, [sample(101, vct_fast_blocks=5, vct_committed_blocks=None)],
            LOGS, 1.0, require_vct_fast_blocks=True,
        )
        self.assertEqual(summary["verdict"], "failed")

    def test_missed_handoff_requires_fresh_finalized_metrics(self):
        self.assertTrue(monitor.missed_vct_handoff(sample(101, vct_committed_blocks=None), 100))
        self.assertTrue(monitor.missed_vct_handoff(sample(101, vct_committed_blocks=0), 100))
        self.assertFalse(monitor.missed_vct_handoff(sample(101), 100))
        self.assertFalse(monitor.missed_vct_handoff(sample(101, finalized=99, vct_committed_blocks=0), 100))
        self.assertFalse(monitor.missed_vct_handoff(
            {**sample(101, vct_committed_blocks=0), "metrics_error": "timeout"}, 100
        ))

    def test_metric_parser_handles_labels_and_missing_counters(self):
        metrics = '# HELP state_vct_fast_path_hit test\nstate_vct_fast_path_hit{network="Mainnet"} 49\n'
        self.assertEqual(monitor.metric_value(metrics, "state_vct_fast_path_hit"), 49)
        self.assertIsNone(monitor.metric_value(metrics, "state_vct_fast_block_count"))

    def test_sample_uses_one_scrape_and_saves_successful_commit_evidence(self):
        metrics = ("state_finalized_block_height 100\nstate_vct_fast_block_count 5\n"
                   "state_vct_fast_path_hit 4\n")
        response = MagicMock()
        response.__enter__.return_value.read.return_value = metrics.encode()
        with tempfile.TemporaryDirectory() as out, patch.object(
            monitor.urllib.request, "urlopen", return_value=response
        ) as scrape, patch.object(monitor, "rpc_call", side_effect=[
            {"blocks": 101, "estimatedheight": 101}, []
        ]), patch.object(monitor, "systemd_props", return_value={"ActiveState": "active"}):
            args = SimpleNamespace(out=out, rpc_url="rpc", metrics_url="metrics", service="zakurad")
            result = monitor.take_sample(args)
            self.assertEqual(scrape.call_count, 1)
            self.assertEqual(result["finalized_height"], 100)
            self.assertEqual(result["vct_committed_blocks"], 4)
            self.assertEqual((Path(out) / "metrics.prom").read_text(), metrics)

    def test_monitor_allows_counter_to_catch_up_after_finalized_gauge(self):
        with tempfile.TemporaryDirectory() as out, patch.object(
            sys, "argv", ["monitor", "--duration-minutes", "60", "--out", out,
                          "--require-vct-fast-blocks", "--required-finalized-at-least", "100",
                          "--stop-after-height", "100"]
        ), patch.object(monitor, "take_sample", side_effect=[
            sample(101, vct_committed_blocks=None), sample(101)
        ]) as take, patch.object(monitor, "scan_logs", return_value=LOGS), patch.object(
            monitor.time, "sleep"
        ):
            self.assertEqual(monitor.main(), 0)
            self.assertEqual(take.call_count, 2)

    def test_monitor_stops_after_two_missed_samples_and_preserves_evidence(self):
        with tempfile.TemporaryDirectory() as out, patch.object(
            sys, "argv", ["monitor", "--duration-minutes", "60", "--out", out,
                          "--require-vct-fast-blocks", "--required-finalized-at-least", "100"]
        ), patch.object(monitor, "take_sample", return_value=sample(
            101, vct_fast_blocks=0, vct_committed_blocks=None
        )) as take, patch.object(monitor, "scan_logs", return_value=LOGS), patch.object(
            monitor.time, "sleep"
        ):
            self.assertEqual(monitor.main(), 1)
            self.assertEqual(take.call_count, 2)
            rows = [json.loads(line) for line in (Path(out) / "samples.jsonl").read_text().splitlines()]
            self.assertEqual(len(rows), 2)
            summary = json.loads((Path(out) / "summary.json").read_text())
            self.assertIn("handoff was not exercised", summary["vct_failure_reason"])

    def test_fails_before_handoff_is_finalized(self):
        summary = monitor.build_summary(
            {"mode": "pre-checkpoint", "network": "mainnet"},
            [sample(101, finalized=99)],
            LOGS,
            1.0,
            known_start_height=99,
            required_start_below=100,
            stop_after_height=100,
            required_finalized_at_least=100,
            require_vct_fast_blocks=True,
        )

        self.assertEqual(summary["verdict"], "failed")


if __name__ == "__main__":
    unittest.main()
