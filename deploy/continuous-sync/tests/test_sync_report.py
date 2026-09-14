"""Exercise timing, stage semantics, retention, and interrupted Slack delivery."""

import contextlib
import copy
import argparse
import gzip
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import report_charts as charts
import slack_report
import sync_report as report
from deploy import DeployError

SETTINGS = {"sandblast_start": 200, "sandblast_end": 300, "chart_axis": "height"}


def fixture(heights=(0, 100, 200, 301, 401), times=(0, 10, 20, 30, 40), mode="dual"):
    rows = []
    for height, timestamp in zip(heights, times):
        values = {"t": timestamp, "height": height, "download_zakura": timestamp * 10_000_000,
                  "download_legacy": 0, "commit_zakura": timestamp * 8_000_000, "commit_legacy": 0,
                  "sapling_height": 100, "ironwood_height": 400, "checkpoint_height": 450,
                  "request_floor_bytes": 2524288, "vct_fast": max(0, height - 100) if height is not None else None,
                  "vct_legacy": min(height, 100) if height is not None else None,
                  "apply_ready": 50, "apply_submitted": 5, "reorder": 200, "ready": False}
        rows.append([values.get(key) for key in report.COLUMNS])
    return {"metadata": {"version": 1, "columns": report.COLUMNS, "run_id": "fixture-run",
                         "mode": mode, "sha": "a" * 40, "started_at": 100, "phase": "complete",
                         "duration": times[-1], "interval": 10, "host": {"node": "node", "cpus": 8},
                         "settings": {"available": True}}, "samples": rows}


class ReportTests(unittest.TestCase):
    def test_forced_ssh_allows_only_status_and_one_named_report(self):
        source = Path(__file__).resolve().parents[1] / "alert-status.py"
        spec = importlib.util.spec_from_file_location("report_status_test", source)
        status = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(status)
        command = "/usr/local/sbin/zakura-monitor-status.py"
        args = argparse.Namespace(config=Path("unused"), report=None, ssh=True)
        with patch.object(status, "parse_args", return_value=args), patch.object(status, "load_config", return_value={}), \
             patch.object(status, "read_report", return_value={}) as read, contextlib.redirect_stdout(io.StringIO()):
            with patch.dict("os.environ", {"SSH_ORIGINAL_COMMAND": command + " --report run1"}):
                self.assertEqual(status.main(), 0)
            read.assert_called_once_with(Path("/var/lib/zakura-continuous-sync/reports"), "run1")
            for original in ("cat /etc/passwd", command + " --config /other", command + " --report run1 extra", command + "; id"):
                with patch.dict("os.environ", {"SSH_ORIGINAL_COMMAND": original}), self.assertRaises(ValueError):
                    status.main()

    def test_metrics_ignore_labels_secrets_nan_and_wrong_height(self):
        values = report.sample_metrics('''
sync_block_payload_received_bytes_total 1.25e8
sync.block.applying.unsubmitted 3
sync_block_applying_unsubmitted{peer="secret"} 99
sync_block_payload_committed_bytes_total NaN
sync_block_reorder_blocks +Inf
sync_estimated_network_tip_height 999999
zcash_chain_verified_block_height 123
unrelated_secret 3
''')
        self.assertEqual(values, {"download_zakura": 125000000, "apply_ready": 3, "height": 123})

    def test_bytes_per_second_resets_missing_lane_and_gaps(self):
        data = fixture()
        self.assertEqual(report.rates(data)[0]["download"], 10)
        self.assertEqual(report.rates(data)[0]["commit"], 8)
        data["samples"][2][report.INDEX["download_legacy"]] = None
        self.assertIsNone(report.rates(data)[1]["download"])
        self.assertIsNone(report.rates(data)[2]["download"])
        data["samples"][1][report.INDEX["download_zakura"]] = 0
        data["samples"][0][report.INDEX["download_zakura"]] = 100
        self.assertIsNone(report.rates(data)[0]["download"])
        data = fixture(times=(0, 10, 200, 210, 220))
        self.assertIsNone(report.rates(data)[1]["download"])
        self.assertEqual(report.rates(data)[2]["download"], 10)

    def test_sandblast_includes_end_height_and_stall_time(self):
        data = fixture(heights=(0, 100, 200, 300, 300, 301), times=(0, 10, 20, 30, 50, 60))
        times = report.region_durations(data, SETTINGS)
        self.assertAlmostEqual(times["Sandblast"], 40)
        self.assertAlmostEqual(sum(times.values()), 60)
        self.assertIn((301, "Post-Sandblast"), report.boundaries(data, SETTINGS))

    def test_missing_height_does_not_assign_work_to_a_region(self):
        data = fixture(heights=(0, 100, None, 301, 401))
        times = report.region_durations(data, SETTINGS)
        self.assertEqual(times["Unobserved"], 20)
        self.assertAlmostEqual(sum(times.values()), 40)
        data["metadata"]["ready_since"] = 35
        data["metadata"]["duration"] = 45
        times = report.region_durations(data, SETTINGS)
        self.assertAlmostEqual(times["Readiness / stop"], 10)
        self.assertAlmostEqual(sum(times.values()), 45)

    def test_old_binary_has_no_invented_vct_or_regions(self):
        data = fixture()
        for row in data["samples"]:
            for key in ("sapling_height", "vct_fast", "vct_legacy"):
                row[report.INDEX[key]] = None
        self.assertEqual(report.boundaries(data, SETTINGS), [])
        self.assertEqual(report.region_durations(data, SETTINGS), {"Unobserved": 40})
        self.assertTrue(all(row["vct_share"] is None for row in report.rates(data)))

    def test_recorder_retains_full_run_when_diagnostic_logs_are_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.toml"
            config.write_text('[network]\nnetwork = "Mainnet"\nsecret = "do not retain"\n'
                              '[network.zakura.block_sync]\nbbr_min_cwnd_bytes = 8388608\nsecret_number = 987\n')
            with patch.object(report.time, "monotonic", side_effect=[100, 100, 110, 120]):
                recorder = report.Recorder(root / "reports", {"run_id": "test-run", "p2p_stack": "legacy"}, config, 10)
                recorder.record({"report": {"height": 0}})
                recorder.record({"report": {"height": 100}})
                recorder.finish("complete")
            data = report.read_report(root / "reports", "test-run")
            self.assertEqual([row[0] for row in data["samples"]], [0, 10])
            self.assertEqual(data["metadata"]["duration"], 20)
            self.assertEqual(data["metadata"]["settings"]["block_sync"], {"bbr_min_cwnd_bytes": 8388608})
            self.assertNotIn("secret", json.dumps(data))
            self.assertFalse((root / "reports/test-run.jsonl").exists())
            self.assertTrue((root / "reports/test-run.jsonl.gz").exists())

    def test_read_only_report_rejects_paths_symlinks_oversize_and_bad_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for run_id in ("../state", "/etc/passwd", "-id", "abc;echo", "x" * 97):
                with self.assertRaises(ValueError):
                    report.read_report(root, run_id)
            self.assertIn("unavailable", report.read_report(root, "old-run"))
            data = fixture()
            report.atomic_json(root / "fixture-run.json", data["metadata"])
            outside = root / "outside"
            outside.write_text("{}")
            (root / "fixture-run.jsonl").symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "symlink"):
                report.read_report(root, "fixture-run")
            (root / "fixture-run.jsonl").unlink()
            with gzip.open(root / "fixture-run.jsonl.gz", "wb") as file:
                file.write(b"x" * 100)
            with patch.object(report, "MAX_REPORT_BYTES", 10), self.assertRaisesRegex(ValueError, "limit"):
                report.read_report(root, "fixture-run")
            data["samples"][1][0] = data["samples"][0][0]
            with self.assertRaisesRegex(ValueError, "sample"):
                report.validate_report(data)

    def test_record_failure_does_not_stop_sync(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            root = Path(tmp)
            recorder = report.Recorder(root, {"run_id": "test-run"}, root / "missing-config", 10)
            with patch.object(report, "MAX_REPORT_BYTES", 0):
                recorder.record({})
            recorder.finish("failed")
            metadata = json.loads((root / "test-run.json").read_text())
            self.assertEqual(metadata["collection_error"], "ValueError")

    def test_baselines_require_matching_host_tuning_range_and_coverage(self):
        data = fixture()
        history = []
        for started in (1, 2, 3):
            previous = copy.deepcopy(data)
            previous["metadata"]["started_at"] = started
            previous["metadata"]["sha"] = "b" * 40
            history.append(charts.baseline_record(previous, SETTINGS))
        self.assertIn("+0% vs median (3 runs)", charts.baseline(data, history, SETTINGS))
        data["metadata"]["host"]["node"] = "different-host"
        self.assertIn("No matched", charts.baseline(data, history, SETTINGS))
        data = fixture()
        data["samples"][0][report.INDEX["request_floor_bytes"]] = 8388608
        self.assertIsNone(report.comparison_key(data))
        self.assertIn("No matched", charts.baseline(fixture(), history, {**SETTINGS, "sandblast_end": 299}))
        partial = fixture(heights=(0, 100, None, 301, 401))
        self.assertNotIn("Sandblast", charts.baseline_record(partial, SETTINGS)["times"])

    def test_mode_scales_and_queue_stages_are_distinct(self):
        self.assertEqual(charts.scales([fixture()])["queue"], 200)
        self.assertEqual(charts.scales([fixture()])["rate"], 10)
        self.assertNotIn("reorder", [key for key, _, _ in charts.queue_series("legacy")])

    @unittest.skipUnless(importlib.util.find_spec("matplotlib"), "renderer dependencies not installed")
    def test_render_height_and_time_with_missing_and_legacy_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            for view in ("height", "time"):
                output = Path(tmp) / f"{view}.png"
                charts.render([fixture(), fixture(mode="legacy"), {"run_id": "old-run"}], "Test report", output,
                              {**SETTINGS, "chart_axis": view})
                self.assertEqual(output.read_bytes()[:8], b"\x89PNG\r\n\x1a\n")


class SlackDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        (self.directory / "chart.png").write_bytes(b"png")
        self.pending = {"text": "daily summary", "files": [{"name": "chart.png", "title": "Dual"}]}
        self.client = Mock()
        self.saved = []

    def send(self):
        slack_report.send_pending(self.client, "C0123456789", self.pending, self.directory,
                                  lambda: self.saved.append(copy.deepcopy(self.pending)))

    def test_failed_image_transfer_keeps_parent_and_resumes_in_thread(self):
        self.client.api.side_effect = [
            {"ts": "123.456", "channel": "C0123456789"}, {"file_id": "F1", "upload_url": "url"},
            {"file_id": "F2", "upload_url": "url"}, {"ok": True, "files": [{"id": "F2"}]},
        ]
        self.client.upload.side_effect = [DeployError("offline"), None]
        with self.assertRaises(DeployError):
            self.send()
        self.assertEqual(self.pending["parent_ts"], "123.456")
        self.send()
        self.assertTrue(self.pending["files"][0]["done"])
        self.assertEqual([call.args[0] for call in self.client.api.call_args_list].count("chat.postMessage"), 1)
        self.assertEqual(self.client.api.call_args.kwargs["thread_ts"], "123.456")
        self.assertEqual(self.client.api.call_args.kwargs["channel_id"], "C0123456789")
        self.send()
        self.assertEqual(self.client.api.call_count, 4)

    def test_unknown_parent_response_requires_recovery_and_retains_writeahead_state(self):
        self.client.api.side_effect = DeployError("timeout")
        with self.assertRaises(DeployError):
            self.send()
        self.assertTrue(self.saved[0]["parent_posting"])
        with self.assertRaisesRegex(DeployError, "recover-parent"):
            self.send()
        self.client.api.assert_called_once()

    def test_explicit_rejection_can_retry_parent(self):
        self.client.api.side_effect = slack_report.Rejected("rate limit")
        with self.assertRaises(slack_report.Rejected):
            self.send()
        self.assertNotIn("parent_posting", self.pending)

    def test_unknown_completion_reconciles_same_thread_without_reuploading(self):
        self.pending["parent_ts"] = "123.456"
        self.pending["files"][0].update(file_id="F1", completing=True)
        self.client.api.return_value = {"file": {"shares": {"private": {"C0123456789": [
            {"thread_ts": "123.456"}]}}}}
        self.send()
        self.assertTrue(self.pending["files"][0]["done"])
        self.client.upload.assert_not_called()
        self.client.api.assert_called_once_with("files.info", file="F1")

    def test_share_in_wrong_thread_does_not_advance(self):
        self.pending["parent_ts"] = "123.456"
        self.pending["files"][0].update(file_id="F1", completing=True)
        self.client.api.return_value = {"file": {"shares": {"public": {"C0123456789": [
            {"thread_ts": "123.777"}]}}}}
        with self.assertRaisesRegex(DeployError, "recover-image"):
            self.send()
        self.assertNotIn("done", self.pending["files"][0])

    def test_upload_never_sends_token_to_redirect_or_foreign_origin(self):
        with patch.dict("os.environ", {"SLACK_BOT_TOKEN": "secret"}):
            client = slack_report.Client()
        for url in ("http://files.slack.com/upload", "https://evil.test/upload", "https://slack.com.evil.test/upload"):
            with self.assertRaises(DeployError):
                client.upload(url, b"png")
        client.opener = MagicMock()
        client.opener.open.return_value.__enter__.return_value.status = 200
        client.upload("https://files.slack.com/upload/v1/test", b"png")
        self.assertNotIn("Authorization", client.opener.open.call_args.args[0].headers)

    def test_file_reconciliation_uses_get_and_server_errors_stay_uncertain(self):
        with patch.dict("os.environ", {"SLACK_BOT_TOKEN": "secret"}):
            client = slack_report.Client()
        client.opener = MagicMock()
        response = client.opener.open.return_value.__enter__.return_value
        response.read.return_value = b'{"ok":true,"file":{}}'
        client.api("files.info", file="F1")
        request = client.opener.open.call_args.args[0]
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.full_url, "https://slack.com/api/files.info?file=F1")
        response.read.return_value = b'{"ok":false,"error":"new_server_error"}'
        with self.assertRaises(DeployError) as caught:
            client.api("chat.postMessage")
        self.assertNotIsInstance(caught.exception, slack_report.Rejected)


if __name__ == "__main__":
    unittest.main()
