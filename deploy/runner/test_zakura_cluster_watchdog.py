#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


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
        "shared_stalled_after": 1800.0,
        "starting_grace": 120.0,
        "dashboard_down_after": 600.0,
        "request_timeout": 20.0,
        "slack_timeout": 20.0,
        "suppression_file": Path("/nonexistent/zakura-watchdog-suppression"),
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


class StallDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.fleet = watchdog.Fleet(
            name="mainnet",
            url="http://dashboard.invalid/data",
            dashboard_url="http://dashboard.invalid/",
        )

    @staticmethod
    def diagnostics(**metrics):
        return {
            "last_poll": 1_000.0,
            "metrics_at": 999.0,
            "metrics_available": True,
            "metrics": metrics,
        }

    def test_node_alert_formats_atomic_pipeline_and_repair_metrics(self):
        row = {
            "name": "canada-0",
            "health": "stale",
            "height": 3_461_397,
            "headers": 3_461_900,
            "header_lag": 503,
            "peer_count": 7,
            "commit": "5c33befdfae0a9e047079c96b9254d9d894c3885",
            "version": "1.3.0-rc2",
            "detail": "height has not advanced within stale window",
            "alert_diagnostics": self.diagnostics(
                checkpoint_verified_height=3_461_397.0,
                sync_estimated_network_tip_height=3_461_901.0,
                sync_estimated_distance_to_tip=504.0,
                sync_block_applying=0.0,
                sync_block_best_header_tip_height=3_461_900.0,
                sync_block_verified_tip_height=3_461_397.0,
                sync_block_fill_stop=1.0,
                sync_block_outstanding=0.0,
                sync_block_missing_bodies=4_000.0,
                state_vct_root_stalled_height=3_461_398.0,
                sync_header_vct_repair_context_unavailable_total=5.0,
                sync_header_vct_repair_timed_out_total=2.0,
            ),
        }
        row["alert_diagnostics"].update({"health": "healthy", "height": 9_999_999})

        text = watchdog.node_alert_text(self.fleet, row, "stalled", 617.0)

        self.assertIn("health: stale - height: 3461397", text)
        self.assertIn("headers 3461900 | header lag 503 | peers 7", text)
        self.assertIn("checkpoint verified 3461397", text)
        self.assertIn("block applying 0", text)
        self.assertIn("block header tip 3461900", text)
        self.assertIn("block verified tip 3461397", text)
        self.assertIn("block fill stop 1", text)
        self.assertIn("block outstanding 0 | missing bodies 4000", text)
        self.assertIn("context unavailable 5 | timed out 2", text)
        self.assertNotIn("no supplier", text)

    def test_missing_diagnostics_do_not_hide_the_stall(self):
        row = {
            "name": "canada-0",
            "health": "stale",
            "height": 3_461_397,
            "detail": "height has not advanced within stale window",
        }

        text = watchdog.node_alert_text(self.fleet, row, "stalled", 617.0)

        self.assertIn("`canada-0` stalled", text)
        self.assertIn("metrics: absent from fleet snapshot", text)

    def test_due_alert_uses_one_fleet_request(self):
        posted = []
        instance = watchdog.Watchdog([self.fleet], make_args())
        snapshot = {
            "last_poll": 1_000.0,
            "rows": [
                {
                    "name": "canada-0",
                    "health": "stale",
                    "height": 3_461_397,
                    "block_hash": "aaaa",
                    "seconds_since_advanced": 700.0,
                    "alert_diagnostics": self.diagnostics(
                        sync_block_missing_bodies=4_000.0
                    ),
                }
            ],
        }

        with (
            patch.object(watchdog, "fetch_json", return_value=snapshot) as fetch,
            patch.object(watchdog.time, "time", return_value=1_000.0),
            patch.object(
                watchdog,
                "post_slack",
                side_effect=lambda text, _args: (posted.append(text), True)[1],
            ),
        ):
            instance.run_once({"nodes": {}, "fleets": {}, "shared_stalls": {}})

        fetch.assert_called_once_with(self.fleet.url, instance.args.request_timeout)
        self.assertEqual(len(posted), 1)
        self.assertIn("missing bodies 4000", posted[0])

    def test_diagnostic_rendering_is_bounded(self):
        fields = watchdog.STALL_PIPELINE_METRICS + watchdog.STALL_REPAIR_METRICS
        metrics = {
            name: float(index) for index, (_label, name) in enumerate(fields)
        }
        row = {
            "name": "node-a",
            "health": "stale",
            "height": 1,
            "detail": "stalled",
            "version": "x" * 10_000,
            "alert_diagnostics": self.diagnostics(**metrics),
        }

        text = watchdog.node_alert_text(self.fleet, row, "stalled", 700.0)

        self.assertLess(len(text), 2_000)
        self.assertNotIn("x" * 65, text)

    def test_node_detail_is_plain_text_and_bounded(self):
        detail = (
            "connection failed\n<!channel> *second_line* `command` "
            + "x" * 10_000
        )
        row = {
            "name": "node-a",
            "health": "down",
            "detail": detail,
        }

        text = watchdog.node_alert_text(self.fleet, row, "down", 700.0)
        detail_line = next(
            line for line in text.splitlines() if line.startswith("health:")
        )

        self.assertIn("connection failed", detail_line)
        self.assertIn("second＿line", detail_line)
        self.assertNotIn("<!channel>", detail_line)
        self.assertNotIn("*second_line*", detail_line)
        self.assertNotIn("`command`", detail_line)
        self.assertLessEqual(
            len(detail_line), watchdog.MAX_NODE_DETAIL_CHARS + 64
        )


class SlackPayloadTests(unittest.TestCase):
    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def read():
            return b"ok"

    def posted_text(self, text, limit=watchdog.MAX_SLACK_MESSAGE_CHARS):
        with (
            patch.object(watchdog, "MAX_SLACK_MESSAGE_CHARS", limit),
            patch.object(
                watchdog.urllib.request,
                "urlopen",
                return_value=self.Response(),
            ) as urlopen,
        ):
            posted = watchdog.post_slack_webhook(
                "https://hooks.slack.invalid/test", text, make_args()
            )

        self.assertTrue(posted)
        request = urlopen.call_args.args[0]
        return json.loads(request.data.decode("utf-8"))["text"]

    def test_webhook_payload_caps_the_final_message(self):
        text = "useful prefix\n" + "x" * (watchdog.MAX_SLACK_MESSAGE_CHARS * 2)
        posted_text = self.posted_text(text)

        self.assertLessEqual(len(posted_text), watchdog.MAX_SLACK_MESSAGE_CHARS)
        self.assertTrue(posted_text.startswith("useful prefix\n"))
        self.assertIn(watchdog.SLACK_TRUNCATION_MARKER, posted_text)

    def test_truncated_node_alert_keeps_incident_and_dashboard(self):
        fields = watchdog.STALL_PIPELINE_METRICS + watchdog.STALL_REPAIR_METRICS
        metrics = {name: 1e308 for _label, name in fields}
        fleet = watchdog.Fleet(
            name="mainnet",
            url="http://source.invalid/data",
            dashboard_url="http://dashboard.invalid/",
        )
        row = {
            "name": "node-" + "n" * 10_000,
            "health": "stale",
            "height": "9" * 10_000,
            "detail": "probe failed " + "d" * 10_000,
            "alert_diagnostics": StallDiagnosticTests.diagnostics(**metrics),
        }

        alert = watchdog.node_alert_text(fleet, row, "stalled", 700.0)
        posted_text = self.posted_text(alert, limit=1_200)

        self.assertLessEqual(len(posted_text), 1_200)
        self.assertTrue(
            posted_text.startswith(":rotating_light: *Zakura mainnet* - `node-")
        )
        self.assertIn("stalled for 11m 40s", posted_text.splitlines()[0])
        self.assertIn(watchdog.SLACK_TRUNCATION_MARKER, posted_text)
        self.assertTrue(posted_text.endswith("dashboard: http://dashboard.invalid/"))
        self.assertNotIn("n" * (watchdog.MAX_ALERT_NAME_CHARS + 1), posted_text)

    def test_truncated_shared_alert_keeps_full_incident_and_dashboard(self):
        fields = watchdog.STALL_PIPELINE_METRICS + watchdog.STALL_REPAIR_METRICS
        metrics = {name: 1e308 for _label, name in fields}
        fleet = watchdog.Fleet(
            name="mainnet",
            url="http://source.invalid/data",
            dashboard_url="http://dashboard.invalid/",
        )
        rows = [
            {
                "name": f"node-{index}",
                "height": 100,
                "alert_diagnostics": StallDiagnosticTests.diagnostics(**metrics),
            }
            for index in range(watchdog.MAX_SHARED_DIAGNOSTIC_ROWS)
        ]

        alert = watchdog.shared_stall_alert_text(
            fleet,
            100,
            "g" * 10_000,
            len(rows),
            1_900.0,
            rows,
        )
        posted_text = self.posted_text(alert, limit=512)

        self.assertLessEqual(len(posted_text), 512)
        self.assertEqual(
            posted_text.splitlines()[:3],
            [
                ":rotating_light: *Zakura mainnet* network height has not advanced for 31m 40s",
                "8 nodes agree at height 100",
                "tip hash: invalid (" + "g" * watchdog.MAX_BLOCK_HASH_CHARS + ")",
            ],
        )
        self.assertIn(watchdog.SLACK_TRUNCATION_MARKER, posted_text)
        self.assertTrue(posted_text.endswith("dashboard: http://dashboard.invalid/"))

    def test_duplicate_owner_recovery_bounds_and_sanitizes_identities(self):
        fleet = watchdog.Fleet(
            name="<!channel>" + "f" * 10_000,
            url="http://source.invalid/data",
            dashboard_url="http://dashboard.invalid/",
        )

        text = watchdog.duplicate_stall_recovery_text(
            fleet,
            "<duplicate>" + "d" * 10_000,
            "`owner`" + "o" * 10_000,
            "9" * 10_000,
        )
        posted_text = self.posted_text(text)

        self.assertLessEqual(len(posted_text), watchdog.MAX_SLACK_MESSAGE_CHARS)
        self.assertNotIn("<!channel>", posted_text)
        self.assertNotIn("<duplicate>", posted_text)
        self.assertNotIn("`owner`", posted_text)
        self.assertNotIn("f" * (watchdog.MAX_ALERT_NAME_CHARS + 1), posted_text)
        self.assertNotIn("d" * (watchdog.MAX_ALERT_NAME_CHARS + 1), posted_text)
        self.assertNotIn("o" * (watchdog.MAX_ALERT_NAME_CHARS + 1), posted_text)
        self.assertIn("height: -", posted_text)
        self.assertTrue(posted_text.endswith("dashboard: http://dashboard.invalid/"))


class FleetSnapshotTests(unittest.TestCase):
    NOW = 2_000.0

    def setUp(self):
        self.posted: list[str] = []
        self.fleet = watchdog.Fleet(
            name="testnet",
            url="http://dashboard.invalid/data",
            dashboard_url="http://dashboard.invalid/",
        )
        self.instance = watchdog.Watchdog([self.fleet], make_args())

    @staticmethod
    def state():
        return {
            "version": watchdog.STATE_VERSION,
            "nodes": {},
            "fleets": {},
            "shared_stalls": {},
        }

    @staticmethod
    def healthy_row():
        return {
            "name": "node-a",
            "health": "healthy",
            "height": 100,
            "block_hash": "aaaa",
            "seconds_since_advanced": 1.0,
        }

    def run_snapshot(self, snapshot, state=None):
        if state is None:
            state = self.state()
        with (
            patch.object(watchdog, "fetch_json", return_value=snapshot),
            patch.object(watchdog.time, "time", return_value=self.NOW),
            patch.object(
                watchdog,
                "post_slack",
                side_effect=lambda text, _args: (self.posted.append(text), True)[1],
            ),
        ):
            self.instance.run_once(state)
        return state

    def test_stale_snapshot_alerts_from_the_last_successful_poll(self):
        state = self.run_snapshot(
            {
                "last_poll": self.NOW - 600.0,
                "rows": [self.healthy_row()],
            }
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("dashboard unavailable for 10m", self.posted[0])
        self.assertIn("snapshot is stale", self.posted[0])
        self.assertTrue(state["fleets"]["testnet"]["alerting"])
        self.assertEqual(state["nodes"], {})
        self.assertEqual(state["shared_stalls"], {})

    def test_stale_snapshot_keeps_the_earliest_fleet_failure_time(self):
        state = self.state()
        state["fleets"]["testnet"] = {
            "condition": "unreachable",
            "bad_since": self.NOW - 100.0,
            "alerting": False,
        }

        self.run_snapshot(
            {
                "last_poll": self.NOW - 600.0,
                "rows": [self.healthy_row()],
            },
            state,
        )

        self.assertEqual(len(self.posted), 1)
        self.assertEqual(state["fleets"]["testnet"]["bad_since"], self.NOW - 600.0)

    def test_malformed_or_empty_rows_start_a_fleet_failure(self):
        duplicate = self.healthy_row()
        for rows in (
            None,
            [],
            ["not an object"],
            [{"health": "healthy"}],
            [duplicate, dict(duplicate)],
        ):
            with self.subTest(rows=rows):
                state = self.run_snapshot({"last_poll": self.NOW, "rows": rows})
                self.assertEqual(
                    state["fleets"]["testnet"]["condition"], "unreachable"
                )
                self.assertFalse(state["fleets"]["testnet"]["alerting"])
                self.assertEqual(state["nodes"], {})

    def test_invalid_last_poll_starts_a_fleet_failure(self):
        for last_poll in (None, float("nan"), float("inf"), True):
            with self.subTest(last_poll=last_poll):
                state = self.run_snapshot(
                    {"last_poll": last_poll, "rows": [self.healthy_row()]}
                )
                self.assertEqual(
                    state["fleets"]["testnet"]["condition"], "unreachable"
                )
                self.assertEqual(state["nodes"], {})

    def test_older_snapshot_without_last_poll_remains_compatible(self):
        state = self.run_snapshot({"rows": [self.healthy_row()]})

        self.assertEqual(state["fleets"]["testnet"]["condition"], "ok")
        self.assertEqual(state["nodes"]["testnet/node-a"]["condition"], "ok")


class SharedStallTests(unittest.TestCase):
    NOW = 2_000.0

    def setUp(self):
        self.posted: list[str] = []
        self._real_post = watchdog.post_slack
        watchdog.post_slack = lambda text, args: (self.posted.append(text), True)[1]
        self.fleet = watchdog.Fleet(
            name="testnet",
            url="http://dashboard.invalid/data",
            dashboard_url="http://dashboard.invalid/",
        )
        self.args = make_args()
        self.instance = watchdog.Watchdog([self.fleet], self.args)
        self.state = {
            "version": watchdog.STATE_VERSION,
            "nodes": {},
            "fleets": {},
            "shared_stalls": {},
        }

    def tearDown(self):
        watchdog.post_slack = self._real_post

    @staticmethod
    def row(
        name,
        height,
        seconds_since_advanced,
        health="stale",
        block_hash="00aa",
    ):
        return {
            "name": name,
            "height": height,
            "block_hash": block_hash,
            "health": health,
            "detail": "height has not advanced within stale window",
            "seconds_since_advanced": seconds_since_advanced,
        }

    def run_snapshot(self, rows, now=None):
        snapshot = {"rows": rows}
        with (
            patch.object(watchdog, "fetch_json", return_value=snapshot),
            patch.object(watchdog.time, "time", return_value=now or self.NOW),
        ):
            self.instance.run_once(self.state)

    def test_matching_height_and_hash_posts_one_fleet_alert(self):
        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 1_817),
                self.row("node-b", 4_302_737, 1_816),
                self.row("node-c", 4_302_737, 1_803),
            ]
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("3 nodes agree at height 4302737", self.posted[0])
        self.assertTrue(self.state["shared_stalls"]["testnet"]["alerting"])
        self.assertTrue(
            all(not entry["alerting"] for entry in self.state["nodes"].values())
        )

    def test_shared_alert_lists_only_participating_nodes(self):
        node_a = self.row("node-a", 4_302_737, 1_817)
        node_b = self.row("node-b", 4_302_737, 1_816)
        for row in (node_a, node_b):
            row["alert_diagnostics"] = StallDiagnosticTests.diagnostics(
                checkpoint_verified_height=4_302_737.0
            )
        down = self.row("down-node", 7, 1_900, health="down", block_hash="bbbb")

        self.run_snapshot([down, node_a, node_b])

        self.assertEqual(len(self.posted), 1)
        self.assertIn("- node-a:", self.posted[0])
        self.assertIn("- node-b:", self.posted[0])
        self.assertNotIn("- down-node:", self.posted[0])

    def test_shared_alert_limits_rendered_participants(self):
        rows = [
            {
                **self.row(f"node-{index:02}", 100, 1_900, block_hash="aaaa"),
                "alert_diagnostics": StallDiagnosticTests.diagnostics(
                    sync_block_missing_bodies=4_000.0,
                    state_vct_root_stalled_height=101.0,
                    state_vct_root_retry_count=3.0,
                    state_vct_aux_sweep_frontier_height=99.0,
                    sync_header_vct_repair_context_unavailable_total=2.0,
                    sync_header_vct_repair_timed_out_total=1.0,
                    sync_header_vct_repair_resource_stalled_total=0.0,
                ),
            }
            for index in range(12)
        ]

        text = watchdog.shared_stall_alert_text(
            self.fleet, 100, "aaaa", len(rows), 1_900.0, rows
        )

        self.assertEqual(
            sum(line.startswith("- node-") for line in text.splitlines()),
            watchdog.MAX_SHARED_DIAGNOSTIC_ROWS,
        )
        self.assertIn("- 4 more participating nodes not shown", text)
        self.assertEqual(text.count("metrics ok"), watchdog.MAX_SHARED_DIAGNOSTIC_ROWS)
        self.assertIn("vct stalled 101 | vct retries 3 | sweep 99", text)
        self.assertIn("repair unavailable 2 | repair timed out 1", text)
        self.assertLess(len(text), 3_000)

    def test_shared_alert_marks_unavailable_and_absent_metrics(self):
        unavailable = self.row("node-a", 100, 1_900, block_hash="aaaa")
        unavailable["alert_diagnostics"] = {
            "last_poll": 1_000.0,
            "metrics_at": None,
            "metrics_available": False,
            "metrics": {},
        }
        absent = self.row("node-b", 100, 1_900, block_hash="aaaa")

        text = watchdog.shared_stall_alert_text(
            self.fleet, 100, "aaaa", 2, 1_900.0, [unavailable, absent]
        )

        self.assertIn("- node-a: height 100 | metrics unavailable", text)
        self.assertIn("- node-b: height 100 | metrics absent", text)

    def test_short_common_idle_does_not_alert(self):
        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 617),
                self.row("node-b", 4_302_737, 616),
                self.row("node-c", 4_302_737, 603),
            ]
        )

        self.assertEqual(self.posted, [])
        self.assertFalse(self.state["shared_stalls"]["testnet"]["alerting"])

    def test_common_height_recovery_posts_once_after_progress(self):
        stalled = [
            self.row("node-a", 4_302_737, 1_817),
            self.row("node-b", 4_302_737, 1_816),
        ]
        self.run_snapshot(stalled)
        self.posted.clear()

        advanced = [
            self.row("node-a", 4_302_790, 5, "healthy"),
            self.row("node-b", 4_302_790, 5, "healthy"),
        ]
        self.run_snapshot(advanced, now=self.NOW + 60)

        self.assertEqual(len(self.posted), 1)
        self.assertIn("network height advanced", self.posted[0])
        self.assertFalse(self.state["shared_stalls"]["testnet"]["alerting"])

    def test_divergence_recovers_fleet_and_alerts_lagging_node(self):
        stalled = [
            self.row("node-a", 4_302_737, 1_817),
            self.row("node-b", 4_302_737, 1_816),
        ]
        self.run_snapshot(stalled)
        self.posted.clear()

        diverged = [
            self.row("node-a", 4_302_800, 5, "healthy"),
            self.row("node-b", 4_302_737, 1_876),
        ]
        self.run_snapshot(diverged, now=self.NOW + 60)

        self.assertEqual(len(self.posted), 2)
        self.assertIn("network height advanced", self.posted[0])
        self.assertIn("`node-b` stalled", self.posted[1])

    def test_matching_height_with_different_hashes_keeps_node_alerts(self):
        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 601, block_hash="aaaa"),
                self.row("node-b", 4_302_737, 601, block_hash="bbbb"),
            ]
        )

        self.assertEqual(len(self.posted), 2)
        self.assertTrue(all("stalled" in post for post in self.posted))
        self.assertFalse(self.state["shared_stalls"]["testnet"]["alerting"])

    def test_missing_hash_keeps_node_alerts(self):
        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 601),
                self.row("node-b", 4_302_737, 601, block_hash=""),
            ]
        )

        self.assertEqual(len(self.posted), 2)
        self.assertFalse(self.state["shared_stalls"]["testnet"]["alerting"])

    def test_fork_closes_shared_alert_before_posting_node_alerts(self):
        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 1_801, block_hash="aaaa"),
                self.row("node-b", 4_302_737, 1_801, block_hash="aaaa"),
            ]
        )
        self.posted.clear()

        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 1_861, block_hash="aaaa"),
                self.row("node-b", 4_302_737, 1_861, block_hash="bbbb"),
            ],
            now=self.NOW + 60,
        )

        self.assertEqual(len(self.posted), 3)
        self.assertIn("shared stall cleared", self.posted[0])
        self.assertTrue(all("stalled" in post for post in self.posted[1:]))
        self.assertFalse(self.state["shared_stalls"]["testnet"]["alerting"])

    def test_threshold_skew_never_posts_a_constituent_node_alert(self):
        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 601),
                self.row("node-b", 4_302_737, 541),
            ]
        )
        self.assertEqual(self.posted, [])

        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 1_861),
                self.row("node-b", 4_302_737, 1_801),
            ],
            now=self.NOW + 1_260,
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("network height has not advanced", self.posted[0])
        self.assertTrue(
            all(not entry["alerting"] for entry in self.state["nodes"].values())
        )

    def test_new_tip_after_polling_gap_gets_a_new_timer(self):
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_200, block_hash="aaaa"),
                self.row("node-b", 100, 1_200, block_hash="aaaa"),
            ]
        )
        self.run_snapshot(
            [
                self.row("node-a", 101, 660, block_hash="bbbb"),
                self.row("node-b", 101, 660, block_hash="bbbb"),
            ],
            now=self.NOW + 660,
        )

        self.assertEqual(self.posted, [])
        entry = self.state["shared_stalls"]["testnet"]
        self.assertEqual(entry["event_height"], 101)
        self.assertEqual(entry["event_hash"], "bbbb")
        self.assertEqual(entry["bad_since"], self.NOW)

    def test_alerted_old_tip_recovers_before_new_tip_timer_starts(self):
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_801, block_hash="aaaa"),
                self.row("node-b", 100, 1_801, block_hash="aaaa"),
            ]
        )
        self.posted.clear()

        self.run_snapshot(
            [
                self.row("node-a", 101, 660, block_hash="bbbb"),
                self.row("node-b", 101, 660, block_hash="bbbb"),
            ],
            now=self.NOW + 660,
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("network height advanced", self.posted[0])
        self.assertFalse(self.state["shared_stalls"]["testnet"]["alerting"])
        self.assertEqual(
            self.state["shared_stalls"]["testnet"]["event_height"], 101
        )

    def test_preexisting_node_alert_represents_the_shared_incident(self):
        self.state["nodes"]["testnet/node-a"] = {
            "condition": "stalled",
            "bad_since": self.NOW - 2_000,
            "alerting": True,
            "alert_height": 4_302_737,
        }
        self.run_snapshot(
            [
                self.row("node-a", 4_302_737, 1_900),
                self.row("node-b", 4_302_737, 1_900),
            ]
        )

        self.assertEqual(self.posted, [])
        shared = self.state["shared_stalls"]["testnet"]
        self.assertFalse(shared["alerting"])
        self.assertEqual(shared["owner"], "node:node-a")
        self.assertTrue(self.state["nodes"]["testnet/node-a"]["alerting"])

    def test_timer_reset_does_not_replace_a_latched_node_owner(self):
        self.state["nodes"]["testnet/node-a"] = {
            "condition": "stalled",
            "bad_since": 0,
            "alerting": True,
            "alert_height": 100,
        }
        self.state["shared_stalls"]["testnet"] = {
            "condition": "stalled",
            "event_height": 100,
            "event_hash": "aaaa",
            "node_names": ["node-a", "node-b"],
            "bad_since": 0,
            "alerting": False,
        }

        self.run_snapshot(
            [
                self.row("node-a", 100, 5, "healthy", block_hash="aaaa"),
                self.row("node-b", 100, 5, "healthy", block_hash="aaaa"),
            ]
        )

        self.assertEqual(self.posted, [])
        self.assertTrue(self.state["nodes"]["testnet/node-a"]["alerting"])
        self.assertEqual(
            self.state["shared_stalls"]["testnet"]["owner"],
            "node:node-a",
        )

    def test_missed_progress_closes_old_node_event_before_new_shared_event(self):
        self.run_snapshot([self.row("node-a", 100, 601, block_hash="aaaa")])
        self.assertEqual(len(self.posted), 1)
        self.assertIn("`node-a` stalled", self.posted[0])

        self.run_snapshot(
            [
                self.row("node-a", 101, 660, block_hash="bbbb"),
                self.row("node-b", 101, 660, block_hash="bbbb"),
            ],
            now=self.NOW + 60,
        )

        self.assertEqual(len(self.posted), 2)
        self.assertIn("`node-a` recovered from stalled", self.posted[1])
        self.assertFalse(self.state["nodes"]["testnet/node-a"]["alerting"])
        self.assertEqual(
            self.state["nodes"]["testnet/node-a"]["event_height"], 101
        )

        self.posted.clear()
        self.run_snapshot(
            [
                self.row("node-a", 101, 1_801, block_hash="bbbb"),
                self.row("node-b", 101, 1_801, block_hash="bbbb"),
            ],
            now=self.NOW + 1_201,
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("network height has not advanced", self.posted[0])
        self.assertTrue(self.state["shared_stalls"]["testnet"]["alerting"])
        self.assertFalse(self.state["nodes"]["testnet/node-a"]["alerting"])

    def test_resync_to_lower_height_keeps_one_latched_node_event(self):
        self.run_snapshot([self.row("node-a", 100, 601, block_hash="aaaa")])
        self.posted.clear()

        self.run_snapshot(
            [self.row("node-a", 50, 5, "healthy", block_hash="bbbb")],
            now=self.NOW + 60,
        )
        entry = self.state["nodes"]["testnet/node-a"]
        self.assertEqual(self.posted, [])
        self.assertTrue(entry["alerting"])
        self.assertEqual(entry["alert_height"], 50)
        self.assertEqual(entry["event_height"], 50)

        self.run_snapshot(
            [self.row("node-a", 50, 601, block_hash="bbbb")],
            now=self.NOW + 660,
        )
        self.assertEqual(self.posted, [])
        self.assertTrue(self.state["nodes"]["testnet/node-a"]["alerting"])

    def test_retained_shared_owner_suppresses_the_remaining_node(self):
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_801, block_hash="aaaa"),
                self.row("node-b", 100, 1_801, block_hash="aaaa"),
            ]
        )
        self.posted.clear()

        self.run_snapshot(
            [
                self.row("node-a", 100, 1_861, block_hash="aaaa"),
                self.row("node-b", 100, 1_861, "down", block_hash="aaaa"),
            ],
            now=self.NOW + 60,
        )

        self.assertEqual(self.posted, [])
        self.assertTrue(self.state["shared_stalls"]["testnet"]["alerting"])
        self.assertFalse(self.state["nodes"]["testnet/node-a"]["alerting"])

    def test_malformed_rows_cannot_silently_clear_a_shared_owner(self):
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_801, block_hash="aaaa"),
                self.row("node-b", 100, 1_801, block_hash="aaaa"),
            ]
        )
        self.posted.clear()

        malformed = [
            self.row("node-a", 100, None, block_hash="aaaa"),
            self.row("node-b", 100, None, block_hash="aaaa"),
        ]
        self.run_snapshot(malformed, now=self.NOW + 60)

        self.assertEqual(self.posted, [])
        self.assertTrue(self.state["shared_stalls"]["testnet"]["alerting"])
        self.assertTrue(
            all(not entry["alerting"] for entry in self.state["nodes"].values())
        )

    def test_non_finite_timers_are_not_stall_evidence(self):
        for timer in (float("nan"), float("inf"), "-inf", True):
            with self.subTest(timer=timer):
                self.state = {
                    "version": watchdog.STATE_VERSION,
                    "nodes": {},
                    "fleets": {},
                    "shared_stalls": {},
                }
                self.posted.clear()
                self.run_snapshot([self.row("node-a", 100, timer)])
                self.assertEqual(self.posted, [])
                self.assertEqual(
                    self.state["nodes"]["testnet/node-a"]["condition"], "ok"
                )

    def test_non_finite_persisted_timer_does_not_crash_reconciliation(self):
        self.state["nodes"]["testnet/node-a"] = {
            "condition": "stalled",
            "bad_since": float("inf"),
            "alerting": False,
            "event_height": 100,
        }

        self.run_snapshot([self.row("node-a", 100, 700.0)])

        self.assertEqual(len(self.posted), 1)
        self.assertTrue(self.state["nodes"]["testnet/node-a"]["alerting"])

    def test_failed_shared_recovery_aborts_constituent_alerts(self):
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_801, block_hash="aaaa"),
                self.row("node-b", 100, 1_801, block_hash="aaaa"),
            ]
        )
        self.posted.clear()

        def fail_recovery(text, _args):
            self.posted.append(text)
            return "shared stall cleared" not in text

        watchdog.post_slack = fail_recovery
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_861, block_hash="aaaa"),
                self.row("node-b", 100, 1_861, block_hash="bbbb"),
            ],
            now=self.NOW + 60,
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("shared stall cleared", self.posted[0])
        self.assertTrue(self.state["shared_stalls"]["testnet"]["alerting"])
        self.assertTrue(
            all(not entry["alerting"] for entry in self.state["nodes"].values())
        )

    def test_failed_shared_alert_retries_without_constituent_alerts(self):
        watchdog.post_slack = lambda text, _args: (self.posted.append(text), False)[1]
        rows = [
            self.row("node-a", 100, 1_801, block_hash="aaaa"),
            self.row("node-b", 100, 1_801, block_hash="aaaa"),
        ]

        self.run_snapshot(rows)

        self.assertEqual(len(self.posted), 1)
        self.assertFalse(self.state["shared_stalls"]["testnet"]["alerting"])
        self.assertTrue(
            all(not entry["alerting"] for entry in self.state["nodes"].values())
        )

        watchdog.post_slack = lambda text, _args: (self.posted.append(text), True)[1]
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_861, block_hash="aaaa"),
                self.row("node-b", 100, 1_861, block_hash="aaaa"),
            ],
            now=self.NOW + 60,
        )

        self.assertEqual(len(self.posted), 2)
        self.assertTrue(self.state["shared_stalls"]["testnet"]["alerting"])
        self.assertTrue(
            all(not entry["alerting"] for entry in self.state["nodes"].values())
        )

    def test_failed_node_recovery_aborts_new_shared_event(self):
        self.run_snapshot([self.row("node-a", 100, 601, block_hash="aaaa")])
        old_node = dict(self.state["nodes"]["testnet/node-a"])
        self.posted.clear()
        watchdog.post_slack = lambda text, _args: (self.posted.append(text), False)[1]

        self.run_snapshot(
            [
                self.row("node-a", 101, 660, block_hash="bbbb"),
                self.row("node-b", 101, 660, block_hash="bbbb"),
            ],
            now=self.NOW + 60,
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("`node-a` recovered from stalled", self.posted[0])
        self.assertEqual(self.state["nodes"]["testnet/node-a"], old_node)
        self.assertEqual(self.state["shared_stalls"]["testnet"]["condition"], "ok")

    def test_failed_dashboard_recovery_aborts_stall_processing(self):
        self.state["fleets"]["testnet"] = {
            "condition": "unreachable",
            "bad_since": self.NOW - 1_000,
            "alerting": True,
        }
        watchdog.post_slack = lambda text, _args: (self.posted.append(text), False)[1]

        self.run_snapshot(
            [
                self.row("node-a", 100, 1_900, block_hash="aaaa"),
                self.row("node-b", 100, 1_900, block_hash="aaaa"),
            ]
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("dashboard recovered", self.posted[0])
        self.assertTrue(self.state["fleets"]["testnet"]["alerting"])
        self.assertEqual(self.state["nodes"], {})
        self.assertEqual(self.state["shared_stalls"], {})

    def test_pr802_duplicate_owners_migrate_to_one_node_owner(self):
        self.state["nodes"]["testnet/node-a"] = {
            "condition": "stalled",
            "bad_since": self.NOW - 2_000,
            "alerting": True,
            "alert_height": 100,
        }
        self.state["shared_stalls"]["testnet"] = {
            "condition": "stalled",
            "bad_since": self.NOW - 2_000,
            "alerting": True,
            "alert_height": 100,
        }

        self.run_snapshot(
            [
                self.row("node-a", 100, 1_900, block_hash="aaaa"),
                self.row("node-b", 100, 1_900, block_hash="aaaa"),
            ]
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("constituent node alert continues", self.posted[0])
        shared = self.state["shared_stalls"]["testnet"]
        self.assertFalse(shared["alerting"])
        self.assertEqual(shared["owner"], "node:node-a")
        self.assertTrue(self.state["nodes"]["testnet/node-a"]["alerting"])

    def test_multiple_constituent_owners_reconcile_to_one_node(self):
        for node_name in ("node-a", "node-b"):
            self.state["nodes"][f"testnet/{node_name}"] = {
                "condition": "stalled",
                "bad_since": self.NOW - 2_000,
                "alerting": True,
                "alert_height": 100,
            }

        self.run_snapshot(
            [
                self.row("node-a", 100, 1_900, block_hash="aaaa"),
                self.row("node-b", 100, 1_900, block_hash="aaaa"),
            ]
        )

        self.assertEqual(len(self.posted), 1)
        self.assertIn("`node-b` duplicate stall alert cleared", self.posted[0])
        self.assertTrue(self.state["nodes"]["testnet/node-a"]["alerting"])
        self.assertFalse(self.state["nodes"]["testnet/node-b"]["alerting"])
        self.assertEqual(
            self.state["shared_stalls"]["testnet"]["owner"],
            "node:node-a",
        )

    def test_failed_pr802_migration_recovery_preserves_both_states(self):
        old_node = {
            "condition": "stalled",
            "bad_since": self.NOW - 2_000,
            "alerting": True,
            "alert_height": 100,
        }
        old_shared = {
            "condition": "stalled",
            "bad_since": self.NOW - 2_000,
            "alerting": True,
            "alert_height": 100,
        }
        self.state["nodes"]["testnet/node-a"] = dict(old_node)
        self.state["shared_stalls"]["testnet"] = dict(old_shared)
        watchdog.post_slack = lambda text, _args: (self.posted.append(text), False)[1]

        self.run_snapshot(
            [
                self.row("node-a", 100, 1_900, block_hash="aaaa"),
                self.row("node-b", 100, 1_900, block_hash="aaaa"),
            ]
        )

        self.assertEqual(len(self.posted), 1)
        self.assertEqual(self.state["nodes"]["testnet/node-a"], old_node)
        self.assertEqual(self.state["shared_stalls"]["testnet"], old_shared)

    def test_membership_timer_boundary_persists_after_removal(self):
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_000, block_hash="aaaa"),
                self.row("node-b", 100, 1_000, block_hash="aaaa"),
                self.row("node-c", 100, 2_000, block_hash="aaaa"),
            ]
        )

        for now in (self.NOW + 60, self.NOW + 120):
            self.run_snapshot(
                [
                    self.row("node-a", 100, now, block_hash="aaaa"),
                    self.row("node-b", 100, now, block_hash="aaaa"),
                ],
                now=now,
            )

        shared = self.state["shared_stalls"]["testnet"]
        self.assertEqual(self.posted, [])
        self.assertEqual(shared["bad_since"], self.NOW - 1_000)
        self.assertEqual(shared["timer_floor"], self.NOW - 1_000)

    def test_membership_timer_boundary_persists_after_addition(self):
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_200, block_hash="aaaa"),
                self.row("node-b", 100, 1_200, block_hash="aaaa"),
            ]
        )
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_260, block_hash="aaaa"),
                self.row("node-b", 100, 1_260, block_hash="aaaa"),
                self.row("node-c", 100, 60, block_hash="aaaa"),
            ],
            now=self.NOW + 60,
        )
        self.run_snapshot(
            [
                self.row("node-a", 100, 1_320, block_hash="aaaa"),
                self.row("node-b", 100, 1_320, block_hash="aaaa"),
                self.row("node-c", 100, 120, block_hash="aaaa"),
            ],
            now=self.NOW + 120,
        )

        shared = self.state["shared_stalls"]["testnet"]
        self.assertEqual(self.posted, [])
        self.assertEqual(shared["bad_since"], self.NOW)
        self.assertEqual(shared["timer_floor"], self.NOW)


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


if __name__ == "__main__":
    unittest.main()
