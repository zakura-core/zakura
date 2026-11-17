#!/usr/bin/env python3
"""Fleet difficulty thresholds, freshness and durable Slack integration."""

from __future__ import annotations

import argparse
import copy
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from zakura_monitoring import difficulty  # noqa: E402

SPEC = importlib.util.spec_from_file_location("difficulty_watchdog_tests", HERE / "zakura-cluster-watchdog.py")
watchdog = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = watchdog
SPEC.loader.exec_module(watchdog)


def compact(target):
    size = (target.bit_length() + 7) // 8
    word = target << (8 * (3 - size)) if size <= 3 else target >> (8 * (size - 3))
    if word & 0x800000:
        word >>= 8
        size += 1
    return f"{size << 24 | word:08x}"


def snapshot(now=1000, multiplier=1, height=None):
    height = difficulty.FLOOR_HEIGHT + (int(now) if height is None else height)
    bits = compact((6990935 << 192) * multiplier)
    rows = [{"name": name, "height": height, "block_hash": f"{height:064x}",
             "healthy": True, "health": "healthy", "rpc_ok": True, "rpc_chain": "main",
             "last_seen_at": now, "last_advanced_at": now, "difficulty_bits": bits}
            for name in ("node-a", "node-b")]
    return {"network": "mainnet", "last_poll": now, "rows": rows}


class DifficultyTests(unittest.TestCase):
    def test_known_target_and_invalid_encodings(self):
        self.assertEqual(difficulty.expanded_target("1b6aac57"), 6990935 << 192)
        self.assertEqual(difficulty.MAX_TARGET, (6990935 << 192) * 10)
        self.assertEqual(difficulty.expanded_target("03012345"), 0x12345)
        self.assertEqual(difficulty.expanded_target("02012345"), 0x123)
        for bits in (None, True, "", "0x1b6aac57", "00000000", "1b800001", "ff7fffff", "1f7fffff"):
            with self.subTest(bits=bits), self.assertRaises(ValueError):
                difficulty.expanded_target(bits)

    def test_warning_escalation_and_recovery(self):
        state = {}
        for now in range(1000, 1900, 60):
            self.assertEqual(difficulty.update(state, snapshot(now, 6), now), [])
        self.assertIn("approaching", difficulty.update(state, snapshot(1900, 6), 1900)[0])
        self.assertEqual(difficulty.update(state, snapshot(1960, 6), 1960), [])
        self.assertIn("near admission", difficulty.update(state, snapshot(2020, 9), 2020)[0])
        self.assertIn("below admission", difficulty.update(state, snapshot(2080, 11), 2080)[0])
        for now in range(2140, 3040, 60):
            self.assertEqual(difficulty.update(state, snapshot(now), now), [])
        self.assertIn("difficulty recovered", difficulty.update(state, snapshot(3040), 3040)[0])
        self.assertEqual(state["severity"], 0)

    def test_nearest_admitted_target_does_not_report_a_breach(self):
        allowed = compact(difficulty.MAX_TARGET)
        easier = f"{int(allowed, 16) + 1:08x}"
        for bits, expected in ((allowed, 2), (easier, 3)):
            data = snapshot()
            for row in data["rows"]:
                row["difficulty_bits"] = bits
            state = {}
            difficulty.update(state, data, 1000)
            self.assertEqual(state["severity"], expected)

    def test_threshold_comparisons_are_exact(self):
        # Small integer targets make all ratio boundaries exact, independent of
        # the compact rounding in the real Mainnet reference.
        with patch.object(difficulty, "MAX_TARGET", 90):
            for target, expected in ((45, 0), (46, 1), (72, 1), (73, 2), (90, 2), (91, 3)):
                state = {}
                for now in range(1000, 1960, 60):
                    data = snapshot(now)
                    for row in data["rows"]:
                        row["difficulty_bits"] = compact(target)
                    difficulty.update(state, data, now)
                self.assertEqual(state.get("severity", 0), expected)
            state = {"severity": 1}
            for now in range(1000, 1960, 60):
                data = snapshot(now)
                for row in data["rows"]:
                    row["difficulty_bits"] = compact(40)
                difficulty.update(state, data, now)
            self.assertEqual(state["severity"], 1, "exactly 2.25× must not recover")

    def test_unavailable_data_preserves_incident_and_retries_as_one_incident(self):
        state = {}
        difficulty.update(state, snapshot(1000, 9), 1000)
        self.assertEqual(difficulty.update(state, None, 1060), [])
        self.assertEqual(difficulty.update(state, {"network": "testnet"}, 1600), [])
        self.assertIn("unavailable", difficulty.update(state, None, 1660)[0])
        self.assertEqual(difficulty.update(state, None, 1720), [])
        self.assertEqual(state["severity"], 2)
        messages = difficulty.update(state, snapshot(1780), 1780)
        self.assertEqual(len(messages), 1)
        self.assertIn("observations restored", messages[0])
        self.assertEqual(state["severity"], 2)

    def test_frozen_tip_cannot_be_refreshed_by_collector_restarts(self):
        state = {}
        difficulty.update(state, snapshot(1000, 9, height=100), 1000)
        for now in range(1060, 1600, 60):
            self.assertEqual(difficulty.update(state, snapshot(now, height=100), now), [])
        messages = difficulty.update(state, snapshot(1600, height=100), 1600)
        self.assertIn("unavailable", messages[0])
        self.assertEqual(state["severity"], 2)
        self.assertEqual(difficulty.update(state, snapshot(1660, height=99), 1660), [])
        self.assertIn("restored", difficulty.update(state, snapshot(1720, height=101), 1720)[0])

    def test_already_stale_data_alerts_without_an_additional_wait(self):
        for data in (snapshot(400), snapshot(1000)):
            if data["last_poll"] == 1000:
                for row in data["rows"]:
                    row.update(last_advanced_at=400, healthy=False, health="stale")
            messages = difficulty.update({}, data, 1000)
            self.assertIn("unavailable", messages[0])

    def test_gaps_interrupt_warning_and_recovery_holds(self):
        for initial_severity, multiplier, timer in ((0, 6, "warning_since"), (2, 1, "recovery_since")):
            state = {"severity": initial_severity}
            difficulty.update(state, snapshot(1000, multiplier), 1000)
            difficulty.update(state, snapshot(1900, multiplier), 1900)
            self.assertEqual(state[timer], 1900)
            self.assertEqual(state["severity"], initial_severity)
            difficulty.update(state, None, 1960)
            self.assertNotIn(timer, state)

    def test_choose_highest_tip_and_check_shared_ancestry(self):
        data = snapshot()
        data["rows"][1].update(height=data["rows"][0]["height"] - 1, block_hash="a" * 64)
        data["rows"][0]["ancestor_hashes"] = {"1": "a" * 64}
        self.assertEqual(difficulty.observation(data, 1000)["nodes"], ["node-a", "node-b"])
        data["rows"][1]["block_hash"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "ancestry disagrees"):
            difficulty.observation(data, 1000)
        data["rows"][0]["difficulty_bits"] = None
        with self.assertRaisesRegex(ValueError, "malformed target"):
            difficulty.observation(data, 1000)

    def test_invalid_and_conflicting_data_never_establish_health(self):
        invalid = []
        for field, value in (("rpc_chain", "test"), ("rpc_ok", False), ("difficulty_bits", "bad"),
                             ("last_seen_at", float("nan")), ("block_hash", "bad")):
            data = snapshot()
            data["rows"][0][field] = value
            invalid.append(data)
        data = snapshot()
        data["rows"][1]["block_hash"] = "a" * 64
        invalid.append(data)
        data = snapshot()
        data["rows"][1]["difficulty_bits"] = "1b6aac58"
        invalid.append(data)
        data = snapshot()
        data["rows"][1]["name"] = data["rows"][0]["name"]
        invalid.append(data)
        invalid.extend((snapshot(1061), {"network": "mainnet", "last_poll": 1000, "rows": []}))
        for stamp in (True, float("inf"), float("nan"), 10**1000):
            data = snapshot()
            data["last_poll"] = stamp
            invalid.append(data)
        for data in invalid:
            with self.subTest(data=data), self.assertRaises(ValueError):
                difficulty.observation(data, 1000)

    def test_tip_only_reporter_can_corroborate_the_same_highest_header(self):
        data = snapshot()
        del data["rows"][1]["difficulty_bits"]
        sample = difficulty.observation(data, 1000)
        self.assertEqual(sample["nodes"], ["node-a", "node-b"])
        self.assertEqual(sample["target"], 6990935 << 192)
        del data["rows"][0]["difficulty_bits"]
        with self.assertRaisesRegex(ValueError, "highest Mainnet tip"):
            difficulty.observation(data, 1000)


class WatchdogDifficultyTests(unittest.TestCase):
    def setUp(self):
        self.fleet = watchdog.Fleet("mainnet", "http://localhost/data", "https://example/dashboard", True)
        self.args = argparse.Namespace(request_timeout=1, dashboard_down_after=600, dry_run=True,
                                       suppression_file=Path("/nonexistent/difficulty-suppression"))
        self.state = {"version": 1}
        self.sent = []
        self.checkpoints = []
        self.instance = watchdog.Watchdog([self.fleet], self.args,
            checkpoint=lambda state: self.checkpoints.append(copy.deepcopy(state)))

    def poll(self, now, multiplier=9, accepted=True, suppressed=False):
        with patch.object(watchdog, "fetch_json", return_value=snapshot(now, multiplier)), \
             patch.object(watchdog.time, "time", return_value=now), \
             patch.object(watchdog, "suppression_until", return_value=now + 60 if suppressed else None), \
             patch.object(self.instance, "handle_fleet_recovered", return_value=False), \
             patch.object(self.instance, "reconcile_pagerduty"), \
             patch.object(watchdog, "post_slack", side_effect=lambda text, args: (self.sent.append(text), accepted)[1]):
            self.instance.run_once(self.state)

    def test_slack_failure_restart_and_suppression_preserve_delivery(self):
        self.poll(1000, accepted=False)
        self.assertTrue(self.checkpoints)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            watchdog.save_state(path, self.state)
            self.state = watchdog.load_state(path)
        self.poll(1060, suppressed=True)
        self.assertEqual(len(self.sent), 1)
        self.poll(1120)
        self.assertEqual(self.sent[0], self.sent[1])
        self.assertEqual(self.state["difficulty"]["mainnet"]["severity"], 2)
        self.assertFalse(self.state["pending_delivery"])
        self.poll(1180)
        self.assertEqual(len(self.sent), 2)
        self.assertIn("node-a", self.sent[0])
        self.assertNotIn("difficulty", [incident.bucket for incident in watchdog.PAGED_INCIDENTS])

    def test_recovery_is_queued_after_an_undelivered_alert(self):
        self.poll(1000, accepted=False)
        for now in range(1060, 2020, 60):
            self.poll(now, multiplier=1, accepted=False)
        pending = self.state["pending_delivery"]["mainnet"]
        self.assertEqual(len(pending["messages"]), 2)
        self.assertIn("difficulty recovered", pending["messages"][1])
        self.poll(2080, multiplier=1)
        self.poll(2140, multiplier=1)
        self.assertEqual(self.state["difficulty"]["mainnet"]["severity"], 0)

    def test_only_mainnet_config_can_enable_the_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fleets.toml"
            for name, enabled in (("mainnet", "true"), ("mainnet", "false"), ("testnet", "true"), ("mainnet", '"true"')):
                path.write_text(f'[[fleets]]\nname="{name}"\nurl="http://localhost/data"\ndifficulty_monitor={enabled}\n')
                if name == "mainnet" and enabled in ("true", "false"):
                    self.assertEqual(watchdog.load_fleets(path)[0].difficulty_monitor, enabled == "true")
                else:
                    with self.assertRaises(SystemExit):
                        watchdog.load_fleets(path)

    def test_monitoring_fetch_rejects_oversized_responses(self):
        with patch.object(watchdog.urllib.request, "urlopen") as open_url:
            open_url.return_value.__enter__.return_value.read.return_value = b" " * (4 * 1024 * 1024 + 1)
            with self.assertRaisesRegex(ValueError, "exceeds 4 MiB"):
                watchdog.fetch_json(self.fleet.url, 1)


if __name__ == "__main__":
    unittest.main()
