#!/usr/bin/env python3
"""Unit tests for the PR-node crossing verdict."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


monitor = load_module("pr_node_monitor", SCRIPTS / "pr-node-monitor.py")


def sample(height: int, finalized: int = 100, vct_fast_blocks: int = 1) -> dict:
    return {
        "height": height,
        "estimated": height + 10,
        "peers": 1,
        "rss_mib": 100.0,
        "restarts": 0,
        "active_state": "active",
        "finalized_height": finalized,
        "vct_fast_blocks": vct_fast_blocks,
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
            [sample(101, vct_fast_blocks=0)],
            LOGS,
            1.0,
            known_start_height=99,
            required_start_below=100,
            stop_after_height=100,
            required_finalized_at_least=100,
            require_vct_fast_blocks=True,
        )

        self.assertEqual(summary["verdict"], "failed")

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
