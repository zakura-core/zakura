#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("zakura-cluster-watchdog.py")
SPEC = importlib.util.spec_from_file_location("zakura_cluster_watchdog", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
watchdog = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = watchdog
SPEC.loader.exec_module(watchdog)


def make_args(**overrides):
    defaults = {
        "down_after": 600.0,
        "stalled_after": 600.0,
        "starting_grace": 120.0,
        "slack_webhook": None,
        "dry_run": True,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class StallRecoveryTests(unittest.TestCase):
    """A stalled node must not be declared recovered at an unchanged height."""

    def setUp(self):
        self.posted: list[str] = []
        self._real_post = watchdog.post_slack
        watchdog.post_slack = lambda text, args: (self.posted.append(text), True)[1]

    def tearDown(self):
        watchdog.post_slack = self._real_post

    def fire_stall(self, bucket, key="fleet/node-a", height=4129396, now=1000.0):
        """Drive the alert past its threshold so it latches."""
        watchdog.update_alert_state(
            bucket, key, "stalled", now - 700.0, 600.0,
            "STALLED", "RECOVERED", now, False, make_args(), height,
        )

    def test_stall_alert_records_the_height_it_fired_at(self):
        bucket = {}
        self.fire_stall(bucket)
        self.assertEqual(len(self.posted), 1)
        entry = bucket["fleet/node-a"]
        self.assertTrue(entry["alerting"])
        self.assertEqual(entry["alert_height"], 4129396)

    def test_unchanged_height_does_not_clear_the_stall(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()

        # The dashboard restarted: its in-memory timer reset, so the condition
        # reads "ok" again -- but the node has not moved a single block.
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 4129396,
        )
        self.assertEqual(self.posted, [], "posted a recovery at an unchanged height")
        self.assertTrue(bucket["fleet/node-a"]["alerting"], "alert should stay latched")
        self.assertEqual(bucket["fleet/node-a"]["alert_height"], 4129396)

    def test_repeated_timer_resets_never_clear_the_stall(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()
        # Five dashboard restarts, as happened on 2026-08-03.
        for cycle in range(5):
            watchdog.update_alert_state(
                bucket, "fleet/node-a", "ok", 2000.0 + cycle, 0.0,
                "STALLED", "RECOVERED", 2000.0 + cycle, False, make_args(), 4129396,
            )
        self.assertEqual(self.posted, [], "stall oscillated recovered/stalled")

    def test_forward_progress_clears_the_stall(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3000.0, 0.0,
            "STALLED", "RECOVERED", 3000.0, False, make_args(), 4129397,
        )
        self.assertEqual(self.posted, ["RECOVERED"])
        self.assertFalse(bucket["fleet/node-a"]["alerting"])

    def test_missing_height_does_not_clear_the_stall(self):
        # The "starting" grace window reports ok with no usable height.
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3000.0, 0.0,
            "STALLED", "RECOVERED", 3000.0, False, make_args(), None,
        )
        self.assertEqual(self.posted, [])
        self.assertTrue(bucket["fleet/node-a"]["alerting"])

    def test_height_going_backwards_does_not_clear_the_stall(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3000.0, 0.0,
            "STALLED", "RECOVERED", 3000.0, False, make_args(), 4000000,
        )
        self.assertEqual(self.posted, [])

    def test_a_node_resynced_from_a_lower_tip_can_still_recover(self):
        # Wiping and resyncing is a normal fix for a stuck node. Anchored at the
        # pre-stall tip the alert would stay latched for the whole resync and no
        # recovery would ever post, so the anchor follows the node down.
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3000.0, 0.0,
            "STALLED", "RECOVERED", 3000.0, False, make_args(), 5_000,
        )
        self.assertEqual(self.posted, [], "a lower tip is not forward progress")
        self.assertTrue(bucket["fleet/node-a"]["alerting"])
        self.assertEqual(bucket["fleet/node-a"]["alert_height"], 5_000)

        # It is now climbing again, so the recovery posts on the next sample.
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 3600.0, 0.0,
            "STALLED", "RECOVERED", 3600.0, False, make_args(), 5_100,
        )
        self.assertEqual(self.posted, ["RECOVERED"])
        self.assertFalse(bucket["fleet/node-a"]["alerting"])

    def test_alert_height_is_backfilled_when_it_was_unknown_at_fire_time(self):
        # A row can alert without a usable height. With no anchor the latch has
        # nothing to compare against and the first reset timer clears it.
        bucket = {}
        self.fire_stall(bucket, height=None)
        self.assertEqual(len(self.posted), 1)
        self.assertIsNone(bucket["fleet/node-a"].get("alert_height"))
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "stalled", 300.0, 600.0,
            "STALLED", "RECOVERED", 1100.0, False, make_args(), 4129396,
        )
        self.assertEqual(bucket["fleet/node-a"]["alert_height"], 4129396)

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 4129396,
        )
        self.assertEqual(self.posted, [], "cleared at an unchanged height")
        self.assertTrue(bucket["fleet/node-a"]["alerting"])

    def test_down_alerts_still_recover_without_height_progress(self):
        # Only stalls are height-gated; a restarted node legitimately recovers
        # at whatever height it comes back on.
        bucket = {}
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "down", 300.0, 600.0,
            "DOWN", "RECOVERED", 1000.0, False, make_args(), None,
        )
        self.assertEqual(self.posted, ["DOWN"])
        self.posted.clear()

        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "DOWN", "RECOVERED", 2000.0, False, make_args(), 4129396,
        )
        self.assertEqual(self.posted, ["RECOVERED"])

    def test_stall_alert_height_survives_intermediate_cycles(self):
        bucket = {}
        self.fire_stall(bucket)
        self.posted.clear()
        # Still stalled on the next poll: the anchor must not be lost.
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "stalled", 300.0, 600.0,
            "STALLED", "RECOVERED", 1100.0, False, make_args(), 4129396,
        )
        self.assertEqual(bucket["fleet/node-a"]["alert_height"], 4129396)
        watchdog.update_alert_state(
            bucket, "fleet/node-a", "ok", 2000.0, 0.0,
            "STALLED", "RECOVERED", 2000.0, False, make_args(), 4129396,
        )
        self.assertEqual(self.posted, [])


if __name__ == "__main__":
    unittest.main()


def make_release_state(**overrides):
    defaults = {
        "name": "mainnet",
        "url": "https://example.invalid/release-state/latest.json",
        "stale_after": 172800.0,
        "required_files": (
            "main-checkpoints.txt",
            "mainnet-frontier.bin",
            "mainnet-treestate-subtrees.bin",
            "mainnet-frontier-grid.bin",
        ),
    }
    defaults.update(overrides)
    return watchdog.ReleaseState(**defaults)


ALL_FILES = {
    "main-checkpoints.txt": {},
    "mainnet-frontier.bin": {},
    "mainnet-treestate-subtrees.bin": {},
    "mainnet-frontier-grid.bin": {},
}


class ReleaseStateConditionTests(unittest.TestCase):
    """The published artifact, not the unit, is what tells us the pipeline is alive."""

    NOW = 1_800_000_000.0

    def pointer(self, age_seconds=0.0, height=3457371):
        stamp = watchdog.datetime.datetime.fromtimestamp(
            self.NOW - age_seconds, watchdog.datetime.timezone.utc
        )
        return {
            "height": height,
            "generated_at": stamp.isoformat().replace("+00:00", "Z"),
        }

    def condition(self, pointer, files, target=None):
        return watchdog.release_state_condition(
            target or make_release_state(), pointer, {"files": files}, self.NOW
        )

    def test_fresh_complete_bundle_is_ok(self):
        cond, _ = self.condition(self.pointer(age_seconds=3600.0), ALL_FILES)
        self.assertEqual(cond, "ok")

    def test_a_bundle_past_its_window_is_stale(self):
        cond, detail = self.condition(self.pointer(age_seconds=200_000.0), ALL_FILES)
        self.assertEqual(cond, "stale")
        self.assertIn("ago", detail)

    def test_one_missed_daily_export_does_not_alert(self):
        # The generator runs daily; a 48h window has to tolerate a single miss.
        cond, _ = self.condition(self.pointer(age_seconds=90_000.0), ALL_FILES)
        self.assertEqual(cond, "ok")

    def test_a_fresh_bundle_missing_the_grid_is_incomplete(self):
        # The regression the retired snapshot-host publisher would have caused: a
        # perfectly fresh three-file bundle that no freshness check would flag.
        three_files = {k: v for k, v in ALL_FILES.items() if "grid" not in k}
        cond, detail = self.condition(self.pointer(age_seconds=60.0), three_files)
        self.assertEqual(cond, "incomplete")
        self.assertIn("mainnet-frontier-grid.bin", detail)

    def test_missing_file_outranks_staleness(self):
        three_files = {k: v for k, v in ALL_FILES.items() if "grid" not in k}
        cond, _ = self.condition(self.pointer(age_seconds=999_999.0), three_files)
        self.assertEqual(cond, "incomplete")

    def test_unparsable_timestamp_is_reported_not_silently_ok(self):
        cond, _ = self.condition({"height": 1, "generated_at": "nonsense"}, ALL_FILES)
        self.assertEqual(cond, "unreadable")

    def test_absent_timestamp_is_reported(self):
        cond, _ = self.condition({"height": 1}, ALL_FILES)
        self.assertEqual(cond, "unreadable")


    def test_absent_file_list_is_reported_not_treated_as_healthy(self):
        # A pointer with no usable meta_url, or a half-failed meta fetch, must not look
        # identical to a healthy bundle.
        cond, detail = watchdog.release_state_condition(
            make_release_state(), self.pointer(age_seconds=60.0), {}, self.NOW
        )
        self.assertEqual(cond, "unreadable")
        self.assertIn("meta.files", detail)

    def test_non_object_file_list_is_reported(self):
        cond, _ = watchdog.release_state_condition(
            make_release_state(), self.pointer(age_seconds=60.0),
            {"files": ["a", "b"]}, self.NOW,
        )
        self.assertEqual(cond, "unreadable")


class ReleaseStateConfigTests(unittest.TestCase):
    def load(self, body):
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as handle:
            handle.write(body)
            path = Path(handle.name)
        return watchdog.load_release_state(path)

    def test_absent_section_is_not_an_error(self):
        # The fleet watchdog must keep working on a config that predates this check.
        self.assertEqual(self.load('[[fleets]]\nname = "t"\nurl = "u"\n'), [])

    def test_defaults_match_the_importer_window(self):
        targets = self.load('[[release_state]]\nname = "mainnet"\nurl = "u"\n')
        self.assertEqual(targets[0].stale_after, 172800)
        self.assertIn("mainnet-frontier-grid.bin", targets[0].required_files)

    def test_duplicate_names_are_rejected(self):
        with self.assertRaises(SystemExit):
            self.load(
                '[[release_state]]\nname = "m"\nurl = "u"\n'
                '[[release_state]]\nname = "m"\nurl = "u"\n'
            )

    def test_missing_url_is_rejected(self):
        with self.assertRaises(SystemExit):
            self.load('[[release_state]]\nname = "m"\n')

