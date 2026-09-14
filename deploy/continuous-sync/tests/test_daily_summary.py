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
from unittest.mock import Mock, patch


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

    def test_daily_report_keeps_each_unreported_archive_link_on_retry(self):
        data = self.data(4)
        controller = data["controller_state"]
        for item in controller["completion_history"]:
            item["trace_archive_url"] = f"https://traces.invalid/run{item['number']}?signature=test&expires=604800"
        controller["last_success_trace_archive_url"] = controller["completion_history"][-1]["trace_archive_url"]
        before = self.path.read_text()
        with self.assertRaisesRegex(deploy.DeployError, "delivery failed"):
            self.deliver(self.due, posted=False, statuses={"node": data})
        self.assertEqual(self.path.read_text(), before)
        _, post = self.deliver(self.due + 60, statuses={"node": data})
        text = post.call_args.args[0]
        self.assertIn("2 completed", text)
        for number in (3, 4):
            self.assertIn(f"<https://traces.invalid/run{number}?signature=test&expires=604800|Download traces (7 days)>", text)
        self.assertNotIn("traces.invalid/run2", text)
        self.assertLess(text.index("traces.invalid/run3"), text.index("traces.invalid/run4"))
        _, post = self.deliver(self.due + 86400, statuses={"node": data})
        self.assertNotIn("Download traces", post.call_args.args[0])

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


class ChartSummaryTests(unittest.TestCase):
    setUp = DailySummaryTests.setUp
    data = DailySummaryTests.data

    def enable(self):
        self.config["summary"].update(charts=True, channel_id="C0123456789", sandblast_start=200,
                                      sandblast_end=300, chart_axis="height")
        self.state["chart_destination"] = "bound"
        summary.save_state(self.path, self.state)

    def test_partial_delivery_freezes_cursors_and_retries_without_a_new_snapshot(self):
        self.enable()
        client = Mock()
        client.destination.return_value = "bound"
        snapshot = {"node": self.data()}
        cursors = {"node": {"number": 3, "run_id": "run3"}}
        def render(*args):
            args[-1].mkdir()
            return [{"name": "chart.png", "title": "Dual"}]
        def send(_client, channel, pending, directory, persist):
            self.assertEqual(channel, "C0123456789")
            pending["parent_ts"] = "1.123"
            persist()
            raise deploy.DeployError("image transfer failed")
        with patch.object(summary, "Client", return_value=client), \
             patch.object(summary, "collect_statuses", return_value=(snapshot, cursors)) as collect, \
             patch.object(summary, "prepare_charts", side_effect=render) as prepare, \
             patch.object(summary, "send_pending", side_effect=send), \
             self.assertRaises(deploy.DeployError):
            summary.deliver(self.config, self.path, self.due)
        collect.assert_called_once()
        prepare.assert_called_once()
        saved = json.loads(self.path.read_text())
        self.assertEqual(saved["cursors"]["node"]["number"], 2)
        self.assertEqual(saved["pending"]["parent_ts"], "1.123")
        with patch.object(summary, "Client", return_value=client), \
             patch.object(summary, "collect_statuses") as collect, \
             patch.object(summary, "prepare_charts") as prepare, \
             patch.object(summary, "send_pending") as send:
            summary.deliver(self.config, self.path, self.due + 86400)
        collect.assert_not_called()
        prepare.assert_not_called()
        self.assertEqual(send.call_args.args[2]["parent_ts"], "1.123")
        saved = json.loads(self.path.read_text())
        self.assertNotIn("pending", saved)
        self.assertEqual(saved["cursors"]["node"]["number"], 3)
        self.assertEqual(saved["last_slot"], "2026-09-08")

    def test_unbound_channel_cannot_send(self):
        self.enable()
        client = Mock()
        client.destination.return_value = "other-channel"
        with patch.object(summary, "Client", return_value=client), \
             patch.object(summary, "collect_statuses") as collect, self.assertRaisesRegex(deploy.DeployError, "not bound"):
            summary.deliver(self.config, self.path, self.due)
        collect.assert_not_called()

    def test_binding_preserves_cursors_and_requires_original_destination(self):
        self.enable()
        args = argparse.Namespace(command="bind-bot")
        client = Mock()
        client.destination.return_value = "new-binding"
        with patch.object(summary, "Client", return_value=client), \
             patch.object(summary, "slack_webhook_url", return_value=self.webhook):
            summary.recover_delivery(self.config, self.path, self.due, args)
        saved = json.loads(self.path.read_text())
        self.assertEqual(saved["cursors"], self.state["cursors"])
        self.assertEqual(saved["last_posted_at"], self.last)
        self.assertEqual(saved["chart_destination"], "new-binding")
        with patch.object(summary, "slack_webhook_url", return_value="changed"), self.assertRaises(deploy.DeployError):
            summary.recover_delivery(self.config, self.path, self.due, args)

    def test_pending_delivery_cannot_be_bypassed_by_disabling_charts(self):
        self.state["pending"] = {"text": "frozen", "timestamp": self.due, "slot": "2026-09-08",
                                 "cursors": self.state["cursors"], "unavailable": [],
                                 "files": [{"name": "chart.png", "title": "Dual"}]}
        summary.save_state(self.path, self.state)
        with patch.object(summary, "slack_webhook_url", return_value=self.webhook), \
             patch.object(summary, "post_slack") as post, self.assertRaisesRegex(deploy.DeployError, "finish pending"):
            summary.deliver(self.config, self.path, self.due)
        post.assert_not_called()

    def test_prepare_keeps_failed_runs_missing_data_and_unavailable_nodes_visible(self):
        self.enable()
        from test_sync_report import fixture
        self.config["nodes"].append({"name": "absent", "hostname": "absent", "ssh_string": "root@absent", "p2p_stack": "zakura"})
        cursors = {**self.state["cursors"], "absent": {"number": 0, "run_id": ""}}
        snapshot = self.data(total=4)
        snapshot["controller_state"].update(failed=True, last_failed_run="failed-run")
        current = fixture()
        current["metadata"]["run_id"] = "run4"
        def read(_config, _node, *, report_id):
            return current if report_id == "run4" else {"unavailable": "no retained report"}
        rendered = []
        def render(reports, title, path, *_args, **_kwargs):
            rendered.append(reports)
            path.write_bytes(b"png")
        with patch.object(summary.monitor, "query_node", side_effect=read), \
             patch.object(summary.report_charts, "render", side_effect=render):
            files = summary.prepare_charts(self.config, {"node": snapshot}, cursors, self.path.parent / "charts-test")
        self.assertEqual(len(files), 2)
        self.assertEqual(rendered[0][0]["run_id"], "run3")
        self.assertEqual(rendered[0][1]["metadata"]["run_id"], "run4")
        self.assertEqual(rendered[0][2]["run_id"], "failed-run")
        self.assertEqual(rendered[1], [{"mode": "zakura", "unavailable": "status unavailable"}])

    def test_chart_delivery_keeps_legacy_text_and_cursor_without_collecting_its_report(self):
        self.enable()
        from test_sync_report import fixture
        self.config["nodes"].append({"name": "legacy", "hostname": "legacy", "p2p_stack": "legacy"})
        self.state["cursors"]["legacy"] = {"number": 2, "run_id": "run2"}
        summary.save_state(self.path, self.state)
        client = Mock()
        client.destination.return_value = "bound"
        current = fixture()
        current["metadata"]["run_id"] = "run3"
        report_nodes = []
        def read(_config, node, *, report_id=None):
            if report_id is not None:
                report_nodes.append(node["name"])
                return current
            return self.data()
        def render(_reports, _title, path, *_args, **_kwargs):
            path.write_bytes(b"png")
        with patch.object(summary, "Client", return_value=client), \
             patch.object(summary.monitor, "query_node", side_effect=read), \
             patch.object(summary.report_charts, "render", side_effect=render) as rendered, \
             patch.object(summary, "send_pending") as send:
            summary.deliver(self.config, self.path, self.due)
        self.assertEqual(report_nodes, ["node"])
        rendered.assert_called_once()
        pending = send.call_args.args[2]
        self.assertIn("Legacy networking only (legacy) · 1 completed", pending["text"])
        self.assertEqual(len(pending["files"]), 1)
        self.assertIn("Dual", pending["files"][0]["title"])
        saved = json.loads(self.path.read_text())
        self.assertEqual(saved["cursors"]["legacy"], {"number": 3, "run_id": "run3"})

    def test_legacy_has_no_placeholder_chart_when_unavailable(self):
        self.enable()
        self.config["nodes"] = [{"name": "node", "p2p_stack": "legacy"}]
        with patch.object(summary.monitor, "query_node") as query, \
             patch.object(summary.report_charts, "render") as render:
            files = summary.prepare_charts(self.config, {}, self.state["cursors"], self.path.parent / "charts-test")
        self.assertEqual(files, [])
        query.assert_not_called()
        render.assert_not_called()

    def test_chart_dry_run_does_not_authenticate_or_advance_state(self):
        self.enable()
        before = self.path.read_text()
        with patch.object(summary, "Client") as client, patch.object(summary, "prepare_charts") as prepare, \
             patch.object(summary.monitor, "query_node", return_value=self.data()), contextlib.redirect_stdout(io.StringIO()):
            summary.deliver(self.config, self.path, self.due, dry_run=True)
        client.assert_not_called()
        prepare.assert_not_called()
        self.assertEqual(self.path.read_text(), before)


if __name__ == "__main__":
    unittest.main()
