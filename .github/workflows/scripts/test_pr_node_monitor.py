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
