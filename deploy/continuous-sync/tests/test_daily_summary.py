"""Exercise calendar delivery and durable cursors without contacting hosts or Slack."""

import argparse
import contextlib
from datetime import datetime
import fcntl
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import daily_summary as summary
import deploy


def stamp(value):
    return int(datetime.fromisoformat(value).timestamp())


class DailySummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "state.json"
        self.config = {
            "summary": {"hostname": "node", "time": "17:00", "timezone": "America/Denver",
                        "state_file": str(self.path)},
            "nodes": [{"name": "node", "hostname": "node", "ssh_string": "root@node", "p2p_stack": "dual"}],
        }
        self.webhook = "https://slack.invalid/summary-test"
        self.last = stamp("2026-09-07T23:46:00+00:00")
        self.due = stamp("2026-09-08T23:00:00+00:00")
        self.state = {
            "version": 1, "last_posted_at": self.last, "last_slot": "2026-09-07",
            "destination": hashlib.sha256(self.webhook.encode()).hexdigest(),
            "cursors": {"node": {"number": 2, "run_id": "run2"}},
        }
        summary.save_state(self.path, self.state)

    def data(self, total=3):
        return {"service_active": True, "metrics_status": "ok", "controller_state": {
            "phase": "syncing", "completion_digest": True, "runs": total,
            "last_success_run": f"run{total}", "completion_digest_start_runs": 0,
            "last_success_duration_seconds": 3600, "last_success_end_height": 999,
            "completion_history": [{"number": n, "run_id": f"run{n}", "duration": 3600,
                                    "end_height": 999} for n in range(1, total + 1)],
        }}

    def deliver(self, timestamp, *, posted=True, statuses=None, dry_run=False):
        with patch.object(summary, "slack_webhook_url", return_value=self.webhook), \
             patch.object(summary, "post_slack", return_value=posted) as post, \
             patch.object(summary.monitor, "query_node", side_effect=lambda _, node: (statuses or {"node": self.data()})[node["name"]]), \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = summary.deliver(self.config, self.path, timestamp, dry_run=dry_run)
        return result, post

    def test_fixed_deadline_does_not_wait_24_hours_after_late_post(self):
        _, post = self.deliver(self.due - 1)
        post.assert_not_called()
        _, post = self.deliver(self.due)
        self.assertIn("1 completed", post.call_args.args[0])
        self.assertNotIn("3 completed", post.call_args.args[0])
        _, post = self.deliver(self.due + 60)
        post.assert_not_called()

    def test_spring_and_fall_dst_keep_five_pm_local(self):
        for deadline in ["2026-03-08T00:00:00+00:00", "2026-03-08T23:00:00+00:00",
                         "2026-10-31T23:00:00+00:00", "2026-11-02T00:00:00+00:00"]:
            timestamp = stamp(deadline)
            self.assertEqual(summary.latest_slot(timestamp, self.config["summary"])[1], timestamp)
            self.assertLess(summary.latest_slot(timestamp - 1, self.config["summary"])[1], timestamp)

    def test_failed_delivery_retries_all_new_completions_without_moving_deadline(self):
        before = self.path.read_text()
        with self.assertRaisesRegex(deploy.DeployError, "delivery failed"):
            self.deliver(self.due, posted=False)
        self.assertEqual(self.path.read_text(), before)
        _, post = self.deliver(self.due + 600, statuses={"node": self.data(4)})
        self.assertIn("2 completed", post.call_args.args[0])
        _, post = self.deliver(self.due + 86400, statuses={"node": self.data(5)})
        self.assertIn("1 completed", post.call_args.args[0])

    def test_missing_corrupt_and_incomplete_state_do_not_reset_timer(self):
        for contents in [None, "{}", "invalid", json.dumps({**self.state, "cursors": {}})]:
            if contents is None:
                self.path.unlink(missing_ok=True)
            else:
                self.path.write_text(contents)
            with self.assertRaisesRegex(deploy.DeployError, "state missing or invalid"):
                self.deliver(self.due)
            self.assertEqual(self.path.read_text() if self.path.exists() else None, contents)

    def test_unreachable_node_is_reported_and_caught_up_next_day(self):
        self.config["nodes"].append({"name": "peer", "hostname": "peer", "p2p_stack": "legacy"})
        self.state["cursors"]["peer"] = {"number": 2, "run_id": "run2"}
        summary.save_state(self.path, self.state)
        _, post = self.deliver(self.due, statuses={"node": self.data(3), "peer": {"query_error": "unreachable"}})
        self.assertIn("status unavailable", post.call_args.args[0])
        self.assertNotIn("peer) · 0 completed", post.call_args.args[0])
        self.assertEqual(json.loads(self.path.read_text())["cursors"]["peer"]["number"], 2)
        _, post = self.deliver(self.due + 86400, statuses={"node": self.data(4), "peer": self.data(5)})
        self.assertIn("peer) · 3 completed", post.call_args.args[0])
        self.assertIn("earlier unreported runs", post.call_args.args[0])

    def test_zero_completions_and_counter_reset_do_not_replay_history(self):
        _, post = self.deliver(self.due, statuses={"node": self.data(2)})
        self.assertIn("0 completed", post.call_args.args[0])
        _, post = self.deliver(self.due + 86400, statuses={"node": self.data(1)})
        self.assertIn("status unavailable", post.call_args.args[0])
        self.assertEqual(json.loads(self.path.read_text())["cursors"]["node"]["number"], 2)

    def test_new_node_with_no_finished_runs_shows_its_phase(self):
        self.state["cursors"]["node"] = {"number": 0, "run_id": ""}
        summary.save_state(self.path, self.state)
        _, post = self.deliver(self.due, statuses={"node": {"controller_state": {"runs": 0, "phase": "building"}}})
        self.assertIn("0 completed", post.call_args.args[0])
        self.assertIn("building", post.call_args.args[0])

    def test_malformed_status_cannot_consume_a_cursor(self):
        for data in [[], {"controller_state": []}, self.data(True)]:
            summary.save_state(self.path, self.state)
            _, post = self.deliver(self.due, statuses={"node": data})
            self.assertIn("status unavailable", post.call_args.args[0])
            self.assertEqual(json.loads(self.path.read_text())["cursors"], self.state["cursors"])

    def test_restart_after_multiple_missed_days_posts_one_catchup(self):
        _, post = self.deliver(self.due + 3 * 86400, statuses={"node": self.data(10)})
        self.assertIn("8 completed", post.call_args.args[0])
        _, post = self.deliver(self.due + 3 * 86400 + 60)
        post.assert_not_called()

    def test_destination_change_fails_without_consuming_cursors(self):
        self.webhook += "-changed"
        before = self.path.read_text()
        with self.assertRaisesRegex(deploy.DeployError, "destination missing or changed"):
            self.deliver(self.due)
        self.assertEqual(self.path.read_text(), before)

    def test_dry_run_does_not_post_or_advance(self):
        before = self.path.read_text()
        _, post = self.deliver(self.due, dry_run=True)
        post.assert_not_called()
        self.assertEqual(self.path.read_text(), before)

    def test_initialize_requires_confirmed_cursors_and_cannot_overwrite(self):
        seed = self.path.with_name("seed.json")
        seed.write_text(json.dumps({"last_posted_at": self.last, "cursors": self.state["cursors"]}))
        self.path.unlink()
        with patch.object(summary, "slack_webhook_url", return_value=self.webhook):
            summary.initialize(self.config, self.path, seed, self.due)
            with self.assertRaisesRegex(deploy.DeployError, "already exists"):
                summary.initialize(self.config, self.path, seed, self.due)
        self.assertEqual(json.loads(self.path.read_text()), self.state)
        _, post = self.deliver(self.due)
        self.assertIn("1 completed", post.call_args.args[0])

    def test_status_reports_overdue_after_grace_without_contacting_slack(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(summary.check_status(self.config, self.path, self.due + 900), 0)
            self.assertEqual(summary.check_status(self.config, self.path, self.due + 901), 1)
        self.deliver(self.due + 902)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(summary.check_status(self.config, self.path, self.due + 903), 0)

    def invoke_main(self):
        args = argparse.Namespace(config=self.path, command="run", dry_run=False)
        with patch.object(summary.argparse.ArgumentParser, "parse_args", return_value=args), \
             patch.object(summary.tomllib, "load", return_value=self.config), \
             patch.object(summary.socket, "gethostname", return_value="node"), \
             patch.object(summary.time, "time", return_value=self.due), \
             contextlib.redirect_stderr(io.StringIO()):
            return summary.main()

    def test_delivery_lock_blocks_a_second_invocation(self):
        with self.path.with_suffix(".lock").open("w") as lock, patch.object(summary, "deliver") as deliver:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.invoke_main(), 1)
            deliver.assert_not_called()

    def test_failed_slack_post_fails_the_service(self):
        with patch.object(summary, "slack_webhook_url", return_value=self.webhook), \
             patch.object(summary, "post_slack", return_value=False), \
             patch.object(summary.monitor, "query_node", return_value=self.data()):
            self.assertEqual(self.invoke_main(), 1)
        self.assertEqual(json.loads(self.path.read_text()), self.state)

    def test_regular_audit_cannot_emit_a_second_summary(self):
        args = argparse.Namespace(config=Path("unused"), node=None, dry_run=False, max_completion_age=0,
                                  reminder_interval=86400, state_file=self.path, legacy_digest=False)
        data = {**self.data(), "sample": {"metrics_status": "ok"}, "disk_free_bytes": 20 * 1024**3}
        self.path.write_text(json.dumps({"version": 2, "problems": {}, "last_digest_at": 1}))
        with patch.object(deploy, "load_nodes", return_value=[deploy.Node(self.config["nodes"][0])]), \
             patch.object(deploy, "remote_json", return_value=(True, data)), \
             patch.object(deploy, "post_slack") as post, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(deploy.cmd_audit(args), 0)
            post.assert_not_called()

    def test_installer_does_not_restart_sync_controllers_or_start_when_staging(self):
        args = argparse.Namespace(config=ROOT / "nodes.toml", node=None, no_start=True)
        with patch.object(deploy, "run") as run:
            deploy.cmd_deploy_summary(args)
        commands = "\n".join(" ".join(call.args[0]) for call in run.call_args_list)
        self.assertNotIn("zakura-continuous-sync.service", commands)
        self.assertNotIn("systemctl stop zakura-sync-summary.service", commands)
        self.assertNotIn("enable --now", commands)
        self.assertIn("zakura-sync-summary.timer", commands)


if __name__ == "__main__":
    unittest.main()
