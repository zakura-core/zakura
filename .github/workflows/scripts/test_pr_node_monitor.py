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
