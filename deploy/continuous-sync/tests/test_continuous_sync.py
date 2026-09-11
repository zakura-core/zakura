import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
SYNC_PATH = ROOT / "deploy" / "continuous-sync" / "continuous-sync.py"
DEPLOY_PATH = ROOT / "deploy" / "continuous-sync" / "deploy.py"
ALERT_PATH = ROOT / "deploy" / "continuous-sync" / "alert-monitor.py"
ALERT_STATUS_PATH = ROOT / "deploy" / "continuous-sync" / "alert-status.py"
STATUS_WRAPPER_PATH = ROOT / "deploy" / "continuous-sync" / "monitor-status-wrapper.sh"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


sync = load_module("continuous_sync", SYNC_PATH)
deploy = load_module("continuous_sync_deploy", DEPLOY_PATH)
with patch.dict(sys.modules, {"deploy": deploy}):
    canary = load_module("canary_notify", DEPLOY_PATH.with_name("canary-notify.py"))
alert = load_module("continuous_sync_alert", ALERT_PATH)
alert_status = load_module("continuous_sync_alert_status", ALERT_STATUS_PATH)


class ContinuousSyncTests(unittest.TestCase):
    def test_retention_archive_failure_reports_halt_during_disk_recovery(self):
        for restarting in (False, True):
            with self.subTest(restarting=restarting), tempfile.TemporaryDirectory() as tmp:
                config = make_config(Path(tmp), policy=sync.Policy(archive_traces=True))
                state_path = config.paths.state_dir / "state.json"
                if restarting:
                    sync.save_state(state_path, {"failed": True, "failure": "DiskPressure: low"})
                with (
                    patch.object(sync, "one_cycle", side_effect=sync.DiskPressure("low")),
                    patch.object(sync, "stop_service"),
                    patch.object(sync, "safe_wipe_state"),
                    patch.object(sync, "cleanup_retention", side_effect=sync.ControllerError("upload failed")),
                    patch.object(sync, "post_slack", return_value=False) as post,
                ):
                    self.assertEqual(sync.run_loop(config, Path("unused")), 1)
                self.assertTrue(sync.load_state(state_path)["failed"])
                self.assertIn("upload failed", sync.load_state(state_path)["failure"])
                post.assert_called_once()

    def test_archive_rejects_symlink_before_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "run"
            run_dir.mkdir()
            (run_dir / "traces").symlink_to(root / "missing")
            config = make_config(root, policy=sync.Policy(archive_traces=True))
            with self.assertRaisesRegex(sync.ControllerError, "symlink"):
                sync.archive_traces(config, run_dir, {})

    def test_archive_requires_expiration_and_preserves_traces(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp), policy=sync.Policy(archive_traces=True))
            run_dir = Path(tmp) / "run"
            traces = run_dir / "traces"
            traces.mkdir(parents=True)
            (traces / "events.jsonl").write_text('{"event":"test"}\n')
            with patch.dict(os.environ, {"ZAKURA_TRACE_SPACE": "test",
                                        "ZAKURA_TRACE_ENDPOINT": "https://nyc3.digitaloceanspaces.com"}), patch.object(
                    sync, "run", return_value=subprocess.CompletedProcess([], 0, '{"Rules":[]}')):
                with self.assertRaisesRegex(sync.ControllerError, "seven-day"):
                    sync.archive_traces(config, run_dir, {})
            self.assertTrue((traces / "events.jsonl").exists())

    def test_archive_streams_compressed_traces_and_records_link(self):
        import gzip
        import tarfile
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp), policy=sync.Policy(archive_traces=True, hostname="host"))
            run_dir = Path(tmp) / "run"
            (run_dir / "traces").mkdir(parents=True)
            (run_dir / "traces" / "events.csv").write_text("event\ntest\n")
            uploaded = []
            def upload(cmd, **kwargs):
                uploaded.append(kwargs["stdin"].read())
                return subprocess.CompletedProcess(cmd, 0)
            state = {}
            lifecycle = '{"Rules":[{"Status":"Enabled","Prefix":"sync-traces/","Expiration":{"Days":7}}]}'
            with patch.dict(os.environ, {"ZAKURA_TRACE_SPACE": "test",
                                        "ZAKURA_TRACE_ENDPOINT": "https://nyc3.digitaloceanspaces.com"}), patch.object(
                    sync, "run", side_effect=[subprocess.CompletedProcess([], 0, lifecycle),
                                               subprocess.CompletedProcess([], 0, "https://download")]), patch.object(
                    sync.subprocess, "run", side_effect=upload):
                sync.archive_traces(config, run_dir, state)
            with tarfile.open(fileobj=io.BytesIO(gzip.decompress(uploaded[0]))) as archive:
                self.assertEqual(archive.extractfile("traces/events.csv").read(), b"event\ntest\n")
            self.assertEqual(state["trace_archive_url"], "https://download")
            self.assertIn("https://download", deploy.completion_run_text(state))
            self.assertFalse((run_dir / "traces").exists())

    def test_archive_failures_preserve_payload_and_allow_retry(self):
        lifecycle = '{"Rules":[{"Status":"Enabled","Prefix":"sync-traces/","Expiration":{"Days":7}}]}'
        for failure in ("upload", "metadata", "fsync", "delete"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                config = make_config(root, policy=sync.Policy(archive_traces=True))
                run_dir = root / "run"
                traces = run_dir / "traces"
                traces.mkdir(parents=True)
                (traces / "events.csv").write_text("event\ntest\n")
                state = {}
                def upload(cmd, **kwargs):
                    kwargs["stdin"].read()
                    return subprocess.CompletedProcess(cmd, 1 if failure == "upload" else 0)
                with contextlib.ExitStack() as stack:
                    stack.enter_context(patch.dict(os.environ, {
                        "ZAKURA_TRACE_SPACE": "test",
                        "ZAKURA_TRACE_ENDPOINT": "https://nyc3.digitaloceanspaces.com",
                    }))
                    stack.enter_context(patch.object(sync, "run", side_effect=[
                        subprocess.CompletedProcess([], 0, lifecycle),
                        subprocess.CompletedProcess([], 0, "https://download"),
                    ]))
                    stack.enter_context(patch.object(sync.subprocess, "run", side_effect=upload))
                    if failure == "metadata":
                        stack.enter_context(patch.object(sync, "write_run_json", side_effect=OSError("write failed")))
                    elif failure == "fsync":
                        stack.enter_context(patch.object(sync.os, "fsync", side_effect=OSError("sync failed")))
                    elif failure == "delete":
                        stack.enter_context(patch.object(sync.shutil, "rmtree", side_effect=OSError("delete failed")))
                    with self.assertRaises((OSError, sync.ControllerError)):
                        sync.archive_traces(config, run_dir, state)
                self.assertTrue((traces / "events.csv").exists())
                if failure in ("upload", "metadata"):
                    self.assertNotIn("trace_archive_url", state)
                else:
                    persisted = json.loads((run_dir / "run.json").read_text())
                    self.assertEqual(persisted["trace_archive_url"], "https://download")
                    with patch.object(sync, "run") as command:
                        sync.archive_traces(config, run_dir, persisted)
                        command.assert_not_called()
                    self.assertFalse(traces.exists())

    def test_archived_trace_cleanup_retries_without_uploading(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp), policy=sync.Policy(archive_traces=True))
            run_dir = Path(tmp) / "run"
            (run_dir / "traces").mkdir(parents=True)
            (run_dir / "traces" / "old.csv").write_text("old data")
            (run_dir / "run.json").write_text("{}")
            with patch.object(sync, "run") as command:
                sync.archive_traces(config, run_dir, {"trace_archive_url": "https://download"})
                command.assert_not_called()
            self.assertFalse((run_dir / "traces").exists())
            self.assertTrue((run_dir / "run.json").exists())

    def test_trace_cleanup_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "run"
            run_dir.mkdir()
            target = root / "unrelated"
            target.mkdir()
            (run_dir / "traces").symlink_to(target)
            with self.assertRaisesRegex(sync.ControllerError, "symlink"):
                sync.clear_archived_traces(run_dir)
            self.assertTrue(target.exists())

    def test_failure_audit_preserves_archive_link(self):
        problem = deploy.audit_problem({"controller_state": {
            "failed": True, "failure": "stalled", "last_failed_run": "run-1",
            "last_failed_trace_archive_url": "https://download",
        }}, 3600)
        self.assertIn("https://download", problem.detail)

    def test_metric_value_accepts_dotted_and_prometheus_names(self):
        metrics = "\n".join(
            [
                "state_memory_best_committed_block_height 42",
                "sync.estimated_distance_to_tip 1",
                "checkpoint_processing_next_height 99",
            ]
        )

        self.assertEqual(sync.metric_value(metrics, "state.memory.best.committed.block.height"), 42)
        self.assertEqual(sync.metric_value(metrics, "sync.estimated_distance_to_tip"), 1)
        self.assertEqual(sync.metric_value(metrics, "checkpoint_processing_next_height"), 99)

    def test_sample_status_falls_back_to_estimated_height(self):
        metrics = "\n".join(
            [
                "sync_estimated_network_tip_height 1000",
                "sync_estimated_distance_to_tip 100",
                "sync_downloads_in_flight 17",
                "sync_downloads_verifying 4",
            ]
        )
        config = make_config(Path("/tmp"))

        with (
            patch.object(sync, "service_active", return_value=True),
            patch.object(sync, "fetch_text", return_value=metrics),
            patch.object(sync, "fetch_ready", return_value=(False, "syncing")),
        ):
            status = sync.sample_status(config)

        self.assertEqual(status["height"], 900)
        self.assertEqual(status["height_source"], "estimated_tip_minus_distance")
        self.assertEqual(status["sync.downloads.in_flight"], 17)
        self.assertEqual(status["sync.downloads.verifying"], 4)

    def test_final_ready_sample_supplies_confirmed_height_without_stale_fallback(self):
        for final_height in (101, None, -1, True, 2**32):
            with self.subTest(final_height=final_height), tempfile.TemporaryDirectory() as tmp:
                config = make_config(Path(tmp))
                run_dir = Path(tmp) / "run"
                run_dir.mkdir()
                early = {"ready": True, "height": 100, "zcash_chain_verified_block_height": 100}
                final = {"ready": True, "height": 105, "height_source": "estimated_tip_minus_distance",
                         "zcash_chain_verified_block_height": final_height}
                run_state = {}
                with (
                    patch.object(sync, "service_active", return_value=True),
                    patch.object(sync, "check_free_space"),
                    patch.object(sync, "rotate_run_logs"),
                    patch.object(sync, "now", return_value=1000),
                    patch.object(sync.time, "sleep"),
                    patch.object(sync, "sample_status", side_effect=[early] * 5 + [final]),
                ):
                    sync.wait_for_completion(config, run_dir, run_state, {})
                self.assertEqual(run_state["end_height"], 101 if final_height == 101 else None)
                # Progress tracking can still use estimates; throughput cannot.
                self.assertEqual(run_state["height"], 105)

    def test_alert_status_falls_back_to_estimated_height(self):
        metrics = "\n".join(
            [
                "sync_estimated_network_tip_height 1000",
                "sync_estimated_distance_to_tip 100",
            ]
        )

        self.assertEqual(alert_status.metric_height(metrics), 900)

    def test_alert_status_distinguishes_active_and_inactive_service(self):
        for active_state, expected in (("active", True), ("inactive", False), ("failed", False)):
            with self.subTest(active_state=active_state), patch.object(
                alert_status.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout=f"{active_state}\n",
                    stderr="",
                ),
            ):
                self.assertIs(alert_status.service_active("zakura.service"), expected)

    def test_alert_status_service_query_failure_propagates(self):
        with patch.object(
            alert_status.subprocess,
            "run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="Failed to connect to bus",
            ),
        ), self.assertRaisesRegex(RuntimeError, "Failed to connect to bus"):
            alert_status.service_active("zakura.service")

    def test_preflight_checks_dependencies_before_a_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config = make_config(tmp_path)
            config.paths.repo_dir.mkdir()
            config.paths.config_template.write_text("", encoding="utf-8")
            config.paths.wipe_sentinel.write_text("", encoding="utf-8")

            with patch.object(sync.shutil, "which", return_value="/usr/bin/tool") as which:
                sync.preflight(config)

            self.assertEqual(
                [call.args[0] for call in which.call_args_list],
                ["cargo", "git", "systemctl", "logrotate"],
            )

    def test_safe_wipe_state_removes_only_allowlisted_entries(self):
        os.environ["ZAKURA_CONTINUOUS_SYNC_TESTING"] = "1"
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            root = tmp_path / "var" / "lib" / "zakura"
            state = root / "state"
            non_finalized = root / "non_finalized_state"
            network = root / "network"
            for path in (state, non_finalized, network):
                path.mkdir(parents=True)
                (path / "marker").write_text("kept?", encoding="utf-8")
            sentinel = root / ".continuous-sync-wipe-ok"
            sentinel.write_text("", encoding="utf-8")

            config = make_config(tmp_path, chain_state_dir=root, wipe_sentinel=sentinel)

            sync.safe_wipe_state(config)

            self.assertFalse(state.exists())
            self.assertFalse(non_finalized.exists())
            self.assertTrue((network / "marker").exists())
        os.environ.pop("ZAKURA_CONTINUOUS_SYNC_TESTING", None)

    def test_stop_service_requires_a_completed_stop(self):
        config = make_config(Path.cwd())

        with (
            patch.object(sync, "run") as run,
            patch.object(sync, "service_active", return_value=False),
        ):
            sync.stop_service(config)

        run.assert_called_once_with(["systemctl", "stop", config.policy.service_name])

    def test_stop_service_rejects_an_active_service(self):
        config = make_config(Path.cwd())

        with (
            patch.object(sync, "run"),
            patch.object(sync, "service_active", return_value=True),
            self.assertRaisesRegex(sync.ControllerError, "service remained active after stop"),
        ):
            sync.stop_service(config)

    def test_cleanup_retention_keeps_active_and_two_newest_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            runs_dir = tmp_path / "runs"
            active = runs_dir / "20260709T000000Z-aaaaaaaaaaaa"
            completed = [runs_dir / f"2026071{index}T000000Z-aaaaaaaaaaaa" for index in range(4)]
            for path in (*completed, active):
                path.mkdir(parents=True)
            for index, path in enumerate(completed):
                (path / "run.json").write_text(
                    json.dumps({"started_at": f"2026071{index}T000000Z", "phase": "complete"}),
                    encoding="utf-8",
                )
            (active / "run.json").write_text(
                json.dumps({"started_at": "20260709T000000Z", "phase": "syncing"}),
                encoding="utf-8",
            )

            config = make_config(tmp_path, runs_dir=runs_dir, policy=sync.Policy(retention_runs=3))

            sync.cleanup_retention(config, active_run=active)

            self.assertTrue(active.exists())
            self.assertFalse(completed[0].exists())
            self.assertFalse(completed[1].exists())
            self.assertTrue(completed[2].exists())
            self.assertTrue(completed[3].exists())

    def test_cleanup_preserves_failure_evidence_and_unmanaged_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = make_config(root, policy=sync.Policy(retention_bytes=1))
            failed = config.paths.runs_dir / "20260709T000000Z-aaaaaaaaaaaa"
            complete = config.paths.runs_dir / "20260710T000000Z-bbbbbbbbbbbb"
            unrelated = config.paths.runs_dir / "investigation"
            for path, phase in ((failed, "failed"), (complete, "complete"), (unrelated, "complete")):
                (path / "traces").mkdir(parents=True)
                (path / "traces" / "big.jsonl").write_text("trace")
                sync.write_run_json(path, {"started_at": path.name, "phase": phase})
                (path / "samples.jsonl").write_text("height evidence")
            outside = root / "outside"
            outside.mkdir()
            link = config.paths.runs_dir / "20260711T000000Z-cccccccccccc"
            link.symlink_to(outside)
            network = root / "network"
            network.mkdir()
            (network / "identity").write_text("identity")
            sync.cleanup_retention(config, recovery=True)
            self.assertTrue((failed / "run.json").exists())
            self.assertTrue((failed / "samples.jsonl").exists())
            self.assertEqual((failed / "traces" / "big.jsonl").read_text(), "trace")
            self.assertFalse(complete.exists())
            self.assertTrue((unrelated / "traces" / "big.jsonl").exists())
            self.assertTrue(link.is_symlink())
            self.assertTrue((network / "identity").exists())

    def test_retention_discards_successes_before_older_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp), policy=sync.Policy(retention_runs=2))
            paths = []
            for day, phase in ((1, "failed"), (2, "failed"), (3, "complete")):
                path = config.paths.runs_dir / f"2026070{day}T000000Z-aaaaaaaaaaaa"
                path.mkdir(parents=True)
                sync.write_run_json(path, {"phase": phase, "started_at": path.name})
                paths.append(path)
            sync.cleanup_retention(config)
            self.assertEqual([path.exists() for path in paths], [True, True, False])

    @unittest.skipUnless(sync.shutil.which("logrotate"), "logrotate is required on the canaries")
    def test_trace_rotation_preserves_recent_segments_and_open_writer(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp), policy=sync.Policy(trace_file_bytes=64))
            run_dir = config.paths.runs_dir / "current"
            traces = run_dir / "traces"
            traces.mkdir(parents=True)
            trace = traces / "block_sync.jsonl"
            with trace.open("ab", buffering=0) as writer:
                inode = trace.stat().st_ino
                for batch in range(3):
                    writer.write((json.dumps({"batch": batch, "detail": "x" * 100}) + "\n").encode())
                    sync.rotate_run_logs(config, run_dir)
                    self.assertEqual(trace.stat().st_ino, inode)
                    self.assertEqual(trace.stat().st_size, 0)
                writer.write(b'{"batch": 3}\n')
            history = [json.loads(path.read_text())["batch"] for path in (
                traces / "block_sync.jsonl.2", traces / "block_sync.jsonl.1", trace,
            )]
            self.assertEqual(history, [1, 2, 3])
            self.assertFalse((traces / "block_sync.jsonl.3").exists())

    def test_cleanup_bounds_binary_cache_and_removes_interrupted_builds(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            cache = config.paths.build_cache_dir
            cache.mkdir()
            binaries = [cache / ("zakurad-" + str(i) * 40) for i in range(4)]
            for index, binary in enumerate(binaries):
                binary.write_text("binary")
                binary.with_suffix(".json").write_text("{}")
                os.utime(binary, (index, index))
            partial = binaries[0].with_suffix(".tmp")
            partial.write_text("partial")
            worktree = cache / "worktree-aaaaaaaaaaaa"
            worktree.mkdir()
            (cache / "manual-build").mkdir()
            with patch.object(sync, "run"):
                sync.cleanup_retention(config)
            self.assertFalse(partial.exists())
            self.assertFalse(worktree.exists())
            self.assertTrue((cache / "manual-build").exists())
            for index, binary in enumerate(binaries):
                self.assertEqual(binary.exists(), index >= 2)
                self.assertEqual(binary.with_suffix(".json").exists(), index >= 2)

    def test_disk_check_covers_separate_run_volume_and_recovery_headroom(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            config.paths.runs_dir.mkdir()
            config.paths.build_cache_dir.mkdir()
            usage = sync.shutil.disk_usage(Path(tmp))
            def disk_usage(path):
                free = (12 if path == config.paths.runs_dir else 100) * 1024**3
                return usage._replace(free=free)
            with patch.object(sync.shutil, "disk_usage", side_effect=disk_usage):
                sync.check_free_space(config)
                with self.assertRaises(sync.DiskPressure):
                    sync.check_free_space(config, recovery=True)

    def test_cycle_persists_preflight_before_stopping_an_interrupted_sync(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            path = config.paths.state_dir / "state.json"
            state = {"phase": "syncing", "current_run": "previous"}
            sync.save_state(path, state)
            def check_stopped_phase(_):
                persisted = sync.load_state(path)
                self.assertEqual(persisted["phase"], "preflight")
                self.assertNotIn("current_run", persisted)
                self.assertIsNone(deploy.audit_problem({"controller_state": persisted,
                    "service_active": False, "disk_free_bytes": 20 * 1024**3}, 0))
            with (
                patch.object(sync, "stop_service", side_effect=check_stopped_phase),
                patch.object(sync, "cleanup_retention") as cleanup,
                patch.object(sync, "preflight", side_effect=sync.DiskPressure("low disk")),
                self.assertRaises(sync.DiskPressure),
            ):
                sync.one_cycle(config, path, state)
            cleanup.assert_called_once()

    def test_disk_failure_retries_but_sync_failure_stays_halted(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            state_path = config.paths.state_dir / "state.json"
            failed_run = config.paths.runs_dir / "20260701T000000Z-aaaaaaaaaaaa"
            def cycle_with_disk_failure(_, __, state):
                if failed_run.exists():
                    raise KeyboardInterrupt
                (failed_run / "traces").mkdir(parents=True)
                (failed_run / "traces" / "block_sync.jsonl").write_text("failure evidence")
                sync.write_run_json(failed_run, {"phase": "syncing", "run_id": failed_run.name,
                                               "run_dir": str(failed_run), "started_at": failed_run.name})
                state["current_run"] = failed_run.name
                raise sync.DiskPressure("low disk")
            with (
                patch.object(sync, "one_cycle", side_effect=cycle_with_disk_failure),
                patch.object(sync, "stop_service"),
                patch.object(sync, "safe_wipe_state"),
                patch.object(sync, "check_free_space"),
                patch.object(sync, "post_slack", return_value=False),
                patch.object(sync.time, "sleep"),
                self.assertRaises(KeyboardInterrupt),
            ):
                sync.run_loop(config, Path("/unused"))
            self.assertFalse(sync.load_state(state_path)["failed"])
            self.assertEqual((failed_run / "traces" / "block_sync.jsonl").read_text(), "failure evidence")
            self.assertEqual(json.loads((failed_run / "run.json").read_text())["phase"], "failed")
            with (
                patch.object(sync, "one_cycle", side_effect=sync.ControllerError("sync stalled")) as cycle,
                patch.object(sync, "stop_service"),
                patch.object(sync, "post_slack", return_value=False),
            ):
                self.assertEqual(sync.run_loop(config, Path("/unused")), 1)
                self.assertEqual(sync.run_loop(config, Path("/unused")), 2)
                self.assertEqual(cycle.call_count, 1)
            self.assertTrue(sync.load_state(state_path)["failed"])

    def test_old_disk_halt_waits_for_headroom_before_fresh_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            state_path = config.paths.state_dir / "state.json"
            sync.save_state(state_path, {"failed": True, "failure": "ControllerError: free disk 1 bytes below minimum 2"})
            with (
                patch.object(sync, "stop_service"),
                patch.object(sync, "safe_wipe_state"),
                patch.object(sync, "check_free_space", side_effect=[sync.DiskPressure("low"), None]),
                patch.object(sync.time, "sleep") as sleep,
                patch.object(sync, "one_cycle", side_effect=KeyboardInterrupt) as cycle,
                self.assertRaises(KeyboardInterrupt),
            ):
                sync.run_loop(config, Path("/unused"))
            sleep.assert_called_once_with(60)
            cycle.assert_called_once()
            self.assertFalse(sync.load_state(state_path)["failed"])

    @unittest.skipUnless(sync.shutil.which("logrotate"), "logrotate is required on the canaries")
    def test_rotated_node_log_stays_with_failure_after_next_run_starts(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            config.paths.config_template.write_text('[tracing]\nlog_file = "{{LOG_FILE}}"\n')
            failed = config.paths.runs_dir / "failed"
            sync.render_config(config, failed)
            with config.paths.log_file.open("wb") as writer:
                writer.seek(64 * 1024**2)
                writer.write(b"failure details")
            sync.rotate_run_logs(config, failed)
            self.assertEqual((failed / "zebrad.log").stat().st_size, 0)
            next_run = config.paths.runs_dir / "next"
            sync.render_config(config, next_run)
            config.paths.log_file.write_text("next run")
            self.assertEqual(config.paths.log_file.resolve(), (next_run / "zebrad.log").resolve())
            self.assertEqual(tomllib.loads(config.paths.zakurad_config.read_text())["tracing"]["log_file"],
                             str(next_run / "zebrad.log"))
            with (failed / "zebrad.log.1").open("rb") as previous:
                previous.seek(-15, 2)
                self.assertEqual(previous.read(), b"failure details")

    def test_deployment_retires_old_timer_without_starting_controller(self):
        node = deploy.load_nodes(DEPLOY_PATH.with_name("nodes.toml"), None)[0]
        with patch.object(deploy, "run"), patch.object(deploy, "ssh_with_script") as ssh:
            ssh.return_value.returncode = 0
            result = deploy.deploy_node(node, argparse.Namespace(dry_run=False, no_start=True))
        self.assertTrue(result[1])
        script = ssh.call_args.args[1]
        subprocess.run(["bash", "-n"], input=script, text=True, check=True)
        self.assertIn("start_controller=0", script)
        self.assertLess(script.index("systemctl disable --now zakura-storage.timer"),
                        script.index("install -m 755 /tmp/zakura-continuous-sync.py"))
        rendered = deploy.render_files(node)
        self.assertNotIn("zakura-storage.timer", rendered)
        self.assertIn("logrotate", rendered["zakura-monitor.service"])
        self.assertIn("maxsize 64M", rendered["logrotate"])

    def test_relink_backs_up_existing_trace_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            link = tmp_path / "traces"
            target = tmp_path / "runs" / "run" / "traces"
            target.mkdir(parents=True)
            link.mkdir()
            (link / "old.jsonl").write_text("old", encoding="utf-8")

            sync.relink(link, target)

            self.assertTrue(link.is_symlink())
            self.assertEqual(link.resolve(), target.resolve())
            backups = list(tmp_path.glob("traces.migrated-*"))
            self.assertEqual(len(backups), 1)
            self.assertTrue((backups[0] / "old.jsonl").exists())

    def test_relink_backs_up_stale_temporary_trace_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            link = tmp_path / "traces"
            target = tmp_path / "runs" / "run" / "traces"
            stale = tmp_path / ".traces.tmp"
            target.mkdir(parents=True)
            stale.mkdir()
            (stale / "old.jsonl").write_text("old", encoding="utf-8")

            sync.relink(link, target)

            self.assertTrue(link.is_symlink())
            backups = list(tmp_path.glob(".traces.tmp.migrated-*"))
            self.assertEqual(len(backups), 1)
            self.assertTrue((backups[0] / "old.jsonl").exists())

    def test_deploy_renders_per_node_p2p_config(self):
        nodes = deploy.load_nodes(
            ROOT / "deploy" / "continuous-sync" / "nodes.toml",
            ["temp-zakura-sync-test-2"],
        )
        rendered = deploy.render_files(nodes[0])

        self.assertIn('p2p_stack = "zakura"', rendered["zakurad.toml.template"])
        self.assertIn('mode_label = "Zakura/v2-only"', rendered["controller.toml"])
        self.assertIn("[[nodes]]", rendered["alert-monitor.toml"])
        self.assertIn('hostname = "temp-zakura-sync-test-1"', rendered["alert-monitor.toml"])
        self.assertIn("zakura-monitor.py", rendered["zakura-monitor.service"])
        self.assertIn("OnUnitActiveSec=1m", rendered["zakura-monitor.timer"])
        self.assertIn("down_confirmation_samples = 2", rendered["alert-monitor.toml"])
        self.assertIn("zakura.service", rendered)

    def test_deploy_renders_each_node_public_ip_as_external_address(self):
        nodes = deploy.load_nodes(
            ROOT / "deploy" / "continuous-sync" / "nodes.toml",
            None,
        )

        for node in nodes:
            with self.subTest(node=node.name):
                rendered = deploy.render_files(node)
                config = tomllib.loads(rendered["zakurad.toml.template"])
                self.assertEqual(config["network"]["zakura"]["trace_dir"], "{{TRACE_DIR}}")
                self.assertNotIn("debug", config["tracing"]["filter"])
                self.assertEqual(
                    config["network"]["external_addr"],
                    f"{node.raw['public_ip']}:8233",
                )

    def test_deploy_disables_legacy_peer_health_gate_only_for_v2_only_node(self):
        nodes = deploy.load_nodes(
            ROOT / "deploy" / "continuous-sync" / "nodes.toml",
            None,
        )
        expected = {
            "temp-zakura-sync-test-1": 1,
            "temp-zakura-sync-test-2": 0,
            "temp-zakura-sync-test-5": 1,
        }

        for node in nodes:
            with self.subTest(node=node.name):
                rendered = deploy.render_files(node)
                config = tomllib.loads(rendered["zakurad.toml.template"])
                self.assertEqual(
                    config["health"]["min_connected_peers"],
                    expected[node.name],
                )

    def test_deploy_rejects_node_without_public_ip(self):
        inventory = """
[[nodes]]
name = "missing-public-ip"
hostname = "missing-public-ip"
ssh_string = "root@example.test"
mode_label = "test"
p2p_stack = "zakura"
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.toml"
            path.write_text(inventory, encoding="utf-8")

            with self.assertRaisesRegex(
                deploy.DeployError,
                "node missing required field 'public_ip'",
            ):
                deploy.load_nodes(path, None)

    def test_deploy_renders_expanded_legacy_alert_inventory(self):
        nodes = deploy.load_nodes(
            ROOT / "deploy" / "continuous-sync" / "nodes.toml",
            ["temp-zakura-sync-test-5"],
        )
        rendered = deploy.render_files(nodes[0])

        self.assertIn('p2p_stack = "legacy"', rendered["zakurad.toml.template"])
        self.assertIn('mode_label = "Zebra/legacy-only"', rendered["controller.toml"])
        self.assertIn('branch = "main"', rendered["controller.toml"])
        self.assertEqual(rendered["alert-monitor.toml"].count("[[nodes]]"), 3)
        for index in [1, 2, 5]:
            self.assertIn(
                f'hostname = "temp-zakura-sync-test-{index}"',
                rendered["alert-monitor.toml"],
            )

    def test_deploy_does_not_stop_node_before_restarting_controller(self):
        self.assertNotIn('systemctl stop "$node_service"', deploy.INSTALL_SCRIPT)

    def test_deploy_creates_zakurad_config_parent_directory(self):
        self.assertIn('dirname "$config_path"', deploy.INSTALL_SCRIPT)

    def test_audit_alerts_once_then_throttles_until_reminder_interval(self):
        problems = {
            "temp-zakura-sync-test-6": deploy.Problem(
                "controller-halted:build failed", "controller halted: build failed"
            )
        }
        interval = 21600

        new, reminder, recovered, state = deploy.audit_transitions(problems, {}, interval, 1000)
        self.assertEqual(new, ["temp-zakura-sync-test-6: controller halted: build failed"])
        self.assertEqual((reminder, recovered), ([], []))

        # Same failure one cycle later: silent.
        new, reminder, recovered, state = deploy.audit_transitions(
            problems, state, interval, 1000 + 1800
        )
        self.assertEqual((new, reminder, recovered), ([], [], []))

        # Still silent just under the reminder interval.
        new, reminder, _, state = deploy.audit_transitions(
            problems, state, interval, 1000 + interval - 1
        )
        self.assertEqual((new, reminder), ([], []))

        # Reminds once the interval elapses, and reports how long it has been broken.
        new, reminder, _, state = deploy.audit_transitions(
            problems, state, interval, 1000 + interval
        )
        self.assertEqual(new, [])
        self.assertEqual(len(reminder), 1)
        self.assertIn("unresolved for 6h0m", reminder[0])

        # Then goes quiet again until the next interval.
        new, reminder, _, _ = deploy.audit_transitions(
            problems, state, interval, 1000 + interval + 60
        )
        self.assertEqual((new, reminder), ([], []))

    def test_audit_realerts_when_the_failure_changes(self):
        first = {"node": deploy.Problem("controller-halted:build", "controller halted: build failed")}
        second = {"node": deploy.Problem("controller-halted:stalled", "controller halted: stalled")}
        _, _, _, state = deploy.audit_transitions(first, {}, 21600, 1000)
        new, reminder, recovered, _ = deploy.audit_transitions(second, state, 21600, 1100)
        self.assertEqual(new, ["node: controller halted: stalled"])
        self.assertEqual((reminder, recovered), ([], []))

    def test_audit_throttles_a_problem_whose_detail_keeps_changing(self):
        # Free disk moves on every sample and ssh stderr differs between attempts
        # at one outage. Keying continuity on the rendered line would classify each
        # cycle as a brand-new problem and page every 30 minutes -- the exact
        # behaviour the reminder interval exists to stop.
        interval = 21600
        first = {"node": deploy.Problem("low-disk", "low disk: 9000000000 bytes free")}
        second = {"node": deploy.Problem("low-disk", "low disk: 8912345678 bytes free")}

        new, _, _, state = deploy.audit_transitions(first, {}, interval, 1000)
        self.assertEqual(new, ["node: low disk: 9000000000 bytes free"])

        new, reminder, recovered, state = deploy.audit_transitions(
            second, state, interval, 1000 + 1800
        )
        self.assertEqual((new, reminder, recovered), ([], [], []))

        # The reminder still reports the freshest detail, not the stale one.
        _, reminder, _, _ = deploy.audit_transitions(second, state, interval, 1000 + interval)
        self.assertEqual(len(reminder), 1)
        self.assertIn("8912345678", reminder[0])

    def test_audit_problem_kinds_are_stable_across_samples(self):
        def status(free_bytes):
            return {
                "controller_state": {"phase": "syncing"},
                "service_active": True,
                "sample": {"metrics_status": "ok"},
                "disk_free_bytes": free_bytes,
            }

        first = deploy.audit_problem(status(9_000_000_000), 0)
        second = deploy.audit_problem(status(8_912_345_678), 0)
        self.assertEqual(first.kind, second.kind)
        self.assertNotEqual(first.detail, second.detail)

    def test_audit_reports_recovery_once(self):
        boom = {"node": deploy.Problem("boom", "boom")}
        _, _, _, state = deploy.audit_transitions(boom, {}, 21600, 1000)
        new, reminder, recovered, state = deploy.audit_transitions({}, state, 21600, 1100)
        self.assertEqual(recovered, ["node: was boom"])
        self.assertEqual((new, reminder), ([], []))
        # The recovery is not repeated on the next cycle.
        new, reminder, recovered, _ = deploy.audit_transitions({}, state, 21600, 1200)
        self.assertEqual((new, reminder, recovered), ([], [], []))

    def test_audit_state_roundtrips_and_rejects_corrupt_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nested" / "state.json"
            boom = {"node": deploy.Problem("boom", "boom")}
            _, _, _, state = deploy.audit_transitions(boom, {}, 21600, 1000)
            deploy.save_audit_state(path, state)
            self.assertEqual(deploy.load_audit_state(path), state)

            fresh = {"version": deploy.AUDIT_STATE_VERSION, "problems": {}}

            path.write_text("{not json")
            self.assertEqual(deploy.load_audit_state(path), fresh)

            path.write_text(json.dumps({"version": 999, "problems": {"node": {}}}))
            self.assertEqual(deploy.load_audit_state(path), fresh)

            # A record written before `kind`/`detail` replaced `problem` cannot be
            # compared against a current problem, so the whole file is discarded.
            path.write_text(json.dumps({"version": 1, "problems": {"node": {"problem": "boom"}}}))
            self.assertEqual(deploy.load_audit_state(path), fresh)

    def test_audit_stamp_is_parsed_as_utc(self):
        # `time.mktime` would shift this by the runner's UTC offset.
        self.assertEqual(deploy.time_from_stamp("19700102T000000Z"), 86400)

    def test_audit_does_not_record_an_undelivered_alert_as_sent(self):
        # Recording `last_sent` for a page Slack never accepted would silence the
        # audit until the 6h reminder elapsed, so the state must not advance.
        node = deploy.Node({"name": "node", "ssh_string": "root@host"})
        args = argparse.Namespace(
            config=Path("unused.toml"),
            node=None,
            dry_run=False,
            max_completion_age=0,
            reminder_interval=21600,
        )
        with tempfile.TemporaryDirectory() as tmp:
            args.state_file = Path(tmp) / "state.json"
            with patch.object(deploy, "load_nodes", return_value=[node]), patch.object(
                deploy, "remote_json", return_value=(False, "connection timed out")
            ), patch.object(deploy, "post_slack", return_value=False) as post:
                self.assertEqual(deploy.cmd_audit(args), 1)
            self.assertEqual(post.call_count, 1)
            self.assertFalse(args.state_file.exists(), "recorded an undelivered page")

            # The next audit retries the same page, and a successful post commits.
            with patch.object(deploy, "load_nodes", return_value=[node]), patch.object(
                deploy, "remote_json", return_value=(False, "connection timed out")
            ), patch.object(deploy, "post_slack", return_value=True) as post:
                self.assertEqual(deploy.cmd_audit(args), 1)
            self.assertIn("unreachable", post.call_args[0][0])
            self.assertEqual(
                deploy.load_audit_state(args.state_file)["problems"]["node"]["kind"],
                "unreachable",
            )

    def test_forced_ssh_wrapper_uses_current_status_script(self):
        self.assertIn(
            "exec /usr/local/sbin/zakura-monitor-status.py",
            STATUS_WRAPPER_PATH.read_text(encoding="utf-8"),
        )

    def test_alert_requires_two_consecutive_down_samples(self):
        hostname = "temp-zakura-sync-test-1"
        status = {
            "hostname": hostname,
            "public_ip": "138.68.43.212",
            "mode": "dual-stack",
            "service": "zakura.service",
            "service_active": False,
            "metrics_status": "unavailable",
            "height": None,
            "connection": "root@138.68.43.212",
            "alias_connection": f"ssh {hostname}",
            "log_path": "/tmp/zebrad.log",
            "trace_path": "/tmp/traces",
            "monitor_log_path": "/tmp/monitor.log",
            "controller_state": {"phase": "syncing", "failed": False},
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = {
                "defaults": {
                    "alert_state_file": str(Path(tmp) / "state.json"),
                    "monitor_log": str(Path(tmp) / "monitor.log"),
                    "down_confirmation_samples": 2,
                },
                "nodes": [{"hostname": hostname}],
            }
            with (
                patch.object(alert, "query_node", return_value=status),
                patch.object(alert.socket, "gethostname", return_value=hostname),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                post_alert.assert_not_called()

                alert.run_once(config)
                post_alert.assert_called_once()

                alert.run_once(config)
                post_alert.assert_called_once()

                status["service_active"] = True
                status["metrics_status"] = "ok"
                status["height"] = 42
                alert.run_once(config)

            self.assertEqual(post_alert.call_count, 2)
            self.assertEqual(post_alert.call_args_list[0].args[1], "NODE DOWN")
            self.assertEqual(post_alert.call_args_list[1].args[1], "NODE RECOVERED")

    def test_controller_lifecycle_retires_node_down_before_a_fresh_outage(self):
        hostname = "temp-zakura-sync-test-1"
        status = alert_status_fixture(hostname, service_active=False, phase="syncing")
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [hostname])
            with (
                patch.object(alert, "query_node", return_value=status),
                patch.object(alert.socket, "gethostname", return_value=hostname),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                alert.run_once(config)

                status["controller_state"] = {"phase": "failed", "failed": True}
                alert.run_once(config)

                status["controller_state"] = {"phase": "syncing", "failed": False}
                alert.run_once(config)
                self.assertEqual(post_alert.call_count, 1)
                alert.run_once(config)

            self.assertEqual(
                [call.args[1] for call in post_alert.call_args_list],
                ["NODE DOWN", "NODE DOWN"],
            )

    def test_metrics_degraded_while_service_active_does_not_page_down(self):
        hostname = "temp-zakura-sync-test-5"
        status = {
            "hostname": hostname,
            "public_ip": "142.93.27.189",
            "mode": "Zebra/legacy-only",
            "service": "zakura.service",
            "service_active": True,
            "metrics_status": "unavailable: TimeoutError",
            "height": None,
            "connection": "root@142.93.27.189",
            "alias_connection": f"ssh {hostname}",
            "log_path": "/tmp/zebrad.log",
            "trace_path": "/tmp/traces",
            "monitor_log_path": "/tmp/monitor.log",
            "controller_state": {"phase": "syncing", "failed": False},
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = {
                "defaults": {
                    "alert_state_file": str(Path(tmp) / "state.json"),
                    "monitor_log": str(Path(tmp) / "monitor.log"),
                    "down_confirmation_samples": 2,
                },
                "nodes": [{"hostname": hostname}],
            }
            with (
                patch.object(alert, "query_node", return_value=status),
                patch.object(alert.socket, "gethostname", return_value=hostname),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                alert.run_once(config)
                post_alert.assert_not_called()

            log_text = (Path(tmp) / "monitor.log").read_text(encoding="utf-8")
            self.assertIn("metrics-degraded", log_text)
            self.assertTrue(alert.metrics_degraded(status))
            self.assertFalse(alert.node_healthy(status))

    def test_intentionally_inactive_service_and_controller_failure_do_not_page(self):
        hostname = "temp-zakura-sync-test-1"
        for phase in ("building", "installing", "preparing-empty-state", "cleanup", "cooldown", "complete", "failed"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as tmp:
                status = alert_status_fixture(hostname, service_active=False, phase=phase)
                status["controller_state"]["failed"] = phase == "failed"
                config = alert_config(Path(tmp), [hostname])
                with (
                    patch.object(alert, "query_node", return_value=status),
                    patch.object(alert.socket, "gethostname", return_value=hostname),
                    patch.object(alert, "post_alert", return_value=True) as post_alert,
                ):
                    alert.run_once(config)
                    alert.run_once(config)

                post_alert.assert_not_called()

    def test_controller_failure_with_active_service_does_not_page(self):
        hostname = "temp-zakura-sync-test-1"
        status = alert_status_fixture(hostname, service_active=True, phase="failed")
        status["controller_state"].update({"failed": True, "failure": "build failed"})
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [hostname])
            with (
                patch.object(alert, "query_node", return_value=status),
                patch.object(alert.socket, "gethostname", return_value=hostname),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)

            post_alert.assert_not_called()

    def test_controller_failure_does_not_page_even_with_syncing_phase(self):
        hostname = "temp-zakura-sync-test-1"
        status = alert_status_fixture(hostname, service_active=False, phase="syncing")
        status["controller_state"].update({"failed": True, "failure": "sync failed"})
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [hostname])
            with (
                patch.object(alert, "query_node", return_value=status),
                patch.object(alert.socket, "gethostname", return_value=hostname),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                alert.run_once(config)

            post_alert.assert_not_called()

    def test_local_query_failure_is_logged_without_changing_alert_state(self):
        hostname = "temp-zakura-sync-test-1"
        status = alert_status_fixture(hostname, service_active=None, phase="unknown")
        status["query_error"] = "status command timed out"
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config = alert_config(tmp_path, [hostname])
            with (
                patch.object(alert, "query_node", return_value=status),
                patch.object(alert.socket, "gethostname", return_value=hostname),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)

            post_alert.assert_not_called()
            self.assertIn(
                "local-query-failed",
                (tmp_path / "monitor.log").read_text(encoding="utf-8"),
            )

    def test_unknown_local_sample_restarts_down_confirmation(self):
        hostname = "temp-zakura-sync-test-1"
        inactive = alert_status_fixture(hostname, service_active=False)
        unknown = alert_status_fixture(hostname, service_active=None)
        unknown["query_error"] = "status command timed out"
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [hostname])
            with (
                patch.object(
                    alert,
                    "query_node",
                    side_effect=[inactive, unknown, inactive, inactive],
                ),
                patch.object(alert.socket, "gethostname", return_value=hostname),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                # Service state unknown: the streak restarts rather than being
                # carried across a gap that may have lasted hours.
                alert.run_once(config)
                alert.run_once(config)
                post_alert.assert_not_called()

                # Two genuinely consecutive inactive samples still page.
                alert.run_once(config)
                post_alert.assert_called_once()
                self.assertEqual(post_alert.call_args.args[1], "NODE DOWN")

    def test_remote_node_down_does_not_page(self):
        local = "temp-zakura-sync-test-1"
        remote = "temp-zakura-sync-test-2"
        statuses = {
            local: alert_status_fixture(local, service_active=True, height=20),
            remote: alert_status_fixture(remote, service_active=False, height=10),
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [local, remote])
            with (
                patch.object(alert, "query_node", side_effect=lambda _, node: statuses[node["hostname"]]),
                patch.object(alert.socket, "gethostname", return_value=local),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                alert.run_once(config)

            post_alert.assert_not_called()

    def test_local_stall_ignores_height_regression_then_recovers_on_progress(self):
        local = "temp-zakura-sync-test-1"
        peer = "temp-zakura-sync-test-2"
        statuses = {
            local: alert_status_fixture(local, service_active=True, height=10),
            peer: alert_status_fixture(peer, service_active=True, height=11),
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [local, peer], cluster_stall_seconds=10)
            with (
                patch.object(alert, "query_node", side_effect=lambda _, node: statuses[node["hostname"]]),
                patch.object(alert.socket, "gethostname", return_value=local),
                patch.object(alert, "now", side_effect=[100, 111, 112, 113]),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                statuses[peer]["height"] = 12
                alert.run_once(config)
                statuses[local]["height"] = 0
                statuses[peer]["height"] = 13
                alert.run_once(config)
                statuses[local]["height"] = 1
                statuses[peer]["height"] = 14
                alert.run_once(config)

            self.assertEqual(
                [call.args[1] for call in post_alert.call_args_list],
                ["SYNC STALLED", "SYNC RECOVERED"],
            )

    def test_stationary_higher_peer_does_not_prove_local_stall(self):
        local = "temp-zakura-sync-test-1"
        peer = "temp-zakura-sync-test-2"
        statuses = {
            local: alert_status_fixture(local, service_active=True, height=10),
            peer: alert_status_fixture(peer, service_active=True, height=11),
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [local, peer], cluster_stall_seconds=10)
            with (
                patch.object(alert, "query_node", side_effect=lambda _, node: statuses[node["hostname"]]),
                patch.object(alert.socket, "gethostname", return_value=local),
                patch.object(alert, "now", side_effect=[100, 111]),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                alert.run_once(config)

            post_alert.assert_not_called()

    def test_regressing_higher_peer_does_not_prove_local_stall(self):
        local = "temp-zakura-sync-test-1"
        peer = "temp-zakura-sync-test-2"
        statuses = {
            local: alert_status_fixture(local, service_active=True, height=10),
            peer: alert_status_fixture(peer, service_active=True, height=20),
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [local, peer], cluster_stall_seconds=10)
            with (
                patch.object(alert, "query_node", side_effect=lambda _, node: statuses[node["hostname"]]),
                patch.object(alert.socket, "gethostname", return_value=local),
                patch.object(alert, "now", side_effect=[100, 111]),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                statuses[peer]["height"] = 15
                alert.run_once(config)

            post_alert.assert_not_called()

    def test_height_regression_does_not_reset_progress_time(self):
        hostname = "temp-zakura-sync-test-1"
        status = alert_status_fixture(hostname, service_active=True, height=10)
        state = {"nodes": {}, "alerts": {}}

        alert.update_progress_state(state, [status], 100)
        status["height"] = 0
        alert.update_progress_state(state, [status], 111)

        self.assertEqual(state["nodes"][hostname]["height"], 0)
        self.assertEqual(state["nodes"][hostname]["last_progress"], 100)

        status["height"] = 1
        alert.update_progress_state(state, [status], 112)
        self.assertEqual(state["nodes"][hostname]["last_progress"], 112)

    def test_new_controller_run_retires_stall_and_starts_a_fresh_progress_window(self):
        local = "temp-zakura-sync-test-1"
        peer = "temp-zakura-sync-test-2"
        statuses = {
            local: alert_status_fixture(local, service_active=True, height=10),
            peer: alert_status_fixture(peer, service_active=True, height=20),
        }
        statuses[local]["controller_state"]["current_run"] = "run-1"
        statuses[peer]["controller_state"]["current_run"] = "peer-run"
        with tempfile.TemporaryDirectory() as tmp:
            config = alert_config(Path(tmp), [local, peer], cluster_stall_seconds=10)
            with (
                patch.object(alert, "query_node", side_effect=lambda _, node: statuses[node["hostname"]]),
                patch.object(alert.socket, "gethostname", return_value=local),
                patch.object(alert, "now", side_effect=[100, 111, 112, 113, 124]),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                statuses[peer]["height"] = 21
                alert.run_once(config)

                statuses[local]["controller_state"].update({"phase": "failed", "failed": True})
                alert.run_once(config)

                statuses[local] = alert_status_fixture(local, service_active=True, height=0)
                statuses[local]["controller_state"]["current_run"] = "run-2"
                alert.run_once(config)
                self.assertEqual(post_alert.call_count, 1)

                statuses[peer]["height"] = 22
                alert.run_once(config)

            self.assertEqual(
                [call.args[1] for call in post_alert.call_args_list],
                ["SYNC STALLED", "SYNC STALLED"],
            )

    def test_failed_stall_recovery_is_retried(self):
        local = "temp-zakura-sync-test-1"
        peer = "temp-zakura-sync-test-2"
        statuses = {
            local: alert_status_fixture(local, service_active=True, height=10),
            peer: alert_status_fixture(peer, service_active=True, height=11),
        }
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config = alert_config(tmp_path, [local, peer], cluster_stall_seconds=10)
            with (
                patch.object(alert, "query_node", side_effect=lambda _, node: statuses[node["hostname"]]),
                patch.object(alert.socket, "gethostname", return_value=local),
                patch.object(alert, "now", side_effect=[100, 111, 112, 113]),
                patch.object(alert, "post_alert", side_effect=[True, False, True]) as post_alert,
            ):
                alert.run_once(config)
                statuses[peer]["height"] = 12
                alert.run_once(config)
                statuses[local]["height"] = 11
                statuses[peer]["height"] = 13
                alert.run_once(config)
                alert.run_once(config)

            self.assertEqual(
                [call.args[1] for call in post_alert.call_args_list],
                ["SYNC STALLED", "SYNC RECOVERED", "SYNC RECOVERED"],
            )
            state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
            self.assertFalse(state["alerts"][f"local-sync-stall:{local}"]["active"])
            self.assertNotIn(
                "recovery_pending",
                state["alerts"][f"local-sync-stall:{local}"],
            )

    def test_legacy_alert_state_migrates_without_recovery(self):
        hostname = "temp-zakura-sync-test-1"
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            state_path = tmp_path / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "nodes": {
                            hostname: {
                                "height": 7,
                                "last_progress": 50,
                                "consecutive_down_samples": 1,
                            }
                        },
                        "alerts": {
                            f"node-down:{hostname}": {"active": True, "last_sent": 50},
                            f"cluster-stall:{hostname}": {"active": True, "last_sent": 50},
                        },
                    }
                ),
                encoding="utf-8",
            )
            status = alert_status_fixture(hostname, service_active=False, height=7)
            config = alert_config(tmp_path, [hostname])
            with (
                patch.object(alert, "query_node", return_value=status),
                patch.object(alert.socket, "gethostname", return_value=hostname),
                patch.object(alert, "post_alert", return_value=True) as post_alert,
            ):
                alert.run_once(config)
                post_alert.assert_not_called()
                migrated = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    migrated["nodes"][hostname]["consecutive_down_samples"],
                    1,
                )
                alert.run_once(config)

            post_alert.assert_called_once()
            migrated = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(migrated["version"], alert.STATE_VERSION)
            self.assertEqual(migrated["nodes"][hostname]["height"], 7)
            self.assertEqual(migrated["nodes"][hostname]["last_progress"], 50)
            self.assertNotIn(f"node-down:{hostname}", migrated["alerts"])
            self.assertNotIn(f"cluster-stall:{hostname}", migrated["alerts"])

    def test_alert_text_names_condition_and_includes_diagnostics(self):
        text = alert.main_alert_text(
            "NODE DOWN",
            {
                "hostname": "temp-zakura-sync-test-2",
                "mode": "Zakura/v2-only",
                "public_ip": "138.197.218.91",
                "height": 123,
            },
            "zakura.service is inactive while controller phase is syncing",
        )

        self.assertEqual(
            text,
            ":rotating_light: Zakura node down: temp-zakura-sync-test-2 | height: 123 | "
            "reason: zakura.service is inactive while controller phase is syncing | "
            "ssh: root@138.197.218.91",
        )
        self.assertNotIn("\n", text)

    def test_controller_slack_text_is_concise(self):
        config = make_config(
            Path("/tmp"),
            policy=sync.Policy(
                hostname="temp-zakura-sync-test-5",
                p2p_stack="zebra",
                public_ip="142.93.27.189",
            ),
        )

        text = sync.failure_text(
            config,
            {"sha": "abcdef", "time_to_failure_seconds": 3723, "height": 2584406},
            "boom",
        )

        self.assertEqual(
            text,
            ":rotating_light: Zakura failed: temp-zakura-sync-test-5 | legacy | "
            "root@142.93.27.189 | time to failure: 1h 2m 3s | height: 2584406 | "
            "reason: boom",
        )
        self.assertNotIn("\n", text)

    def test_controller_failure_slack_text_handles_unknown_height(self):
        config = make_config(Path("/tmp"))

        text = sync.failure_text(config, {"time_to_failure_seconds": 5}, "boom")

        self.assertIn("time to failure: 5s | height: unknown", text)

    def test_controller_failure_reason_is_normalized_and_bounded(self):
        config = make_config(Path("/tmp"))

        text = sync.failure_text(
            config,
            {"time_to_failure_seconds": 5},
            "first line\n" + "x" * 200,
        )

        reason = text.split(" | reason: ", 1)[1]
        self.assertNotIn("\n", reason)
        self.assertLessEqual(len(reason), 96)
        self.assertTrue(reason.endswith("..."))

    def test_halt_records_time_to_failure_at_failure_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config = make_config(tmp_path)
            state_path = config.paths.state_dir / "state.json"
            run_dir = config.paths.runs_dir / "current"
            run_dir.mkdir(parents=True)
            run_state = {
                "run_dir": str(run_dir),
                "sync_started_at_epoch": 1000,
            }

            with (
                patch.object(sync, "now", return_value=1305),
                patch.object(sync, "post_slack") as post_slack,
            ):
                sync.halt(config, state_path, {}, run_state, "boom")

            self.assertEqual(run_state["failed_at_epoch"], 1305)
            self.assertEqual(run_state["time_to_failure_seconds"], 305)
            posted_state = post_slack.call_args.args[1]
            self.assertIn("time to failure: 5m 5s", posted_state)

    def test_resume_posts_recovery_only_after_successful_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            state_path = config.paths.state_dir / "state.json"
            sync.save_state(state_path, {"failed": True, "failure": "boom", "phase": "failed"})

            with (
                patch.object(sync, "run") as run,
                patch.object(sync, "post_slack") as post_slack,
            ):
                sync.resume(config)

            self.assertEqual(run.call_count, 2)
            self.assertNotIn("failed", sync.load_state(state_path))
            post_slack.assert_called_once_with(config, sync.resumed_text(config))

    def test_resume_restores_failure_and_does_not_post_when_start_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            state_path = config.paths.state_dir / "state.json"
            original = {"failed": True, "failure": "boom", "phase": "failed"}
            sync.save_state(state_path, original)

            with (
                patch.object(sync, "run", side_effect=[None, RuntimeError("start failed")]),
                patch.object(sync, "post_slack") as post_slack,
                self.assertRaisesRegex(RuntimeError, "start failed"),
            ):
                sync.resume(config)

            self.assertEqual(sync.load_state(state_path), original)
            post_slack.assert_not_called()

    def test_resume_reports_a_dropped_slack_notification(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            state_path = config.paths.state_dir / "state.json"
            sync.save_state(state_path, {"failed": True, "failure": "boom", "phase": "failed"})

            stdout = io.StringIO()
            with (
                patch.object(sync, "run"),
                patch.object(sync, "post_slack", return_value=False),
                contextlib.redirect_stdout(stdout),
            ):
                self.assertEqual(sync.resume(config), 0)

            # The resume itself worked, so the latch stays cleared and the exit
            # code stays 0; only the notification was lost, and `deploy.py
            # resume` reads stdout to tell the operator about it.
            self.assertNotIn("failed", sync.load_state(state_path))
            self.assertIn("slack notification failed", stdout.getvalue())


class NotificationTests(unittest.TestCase):
    WEBHOOK = "https://slack.invalid/test"

    def halted_status(self):
        destination = hashlib.sha256(self.WEBHOOK.encode()).hexdigest()
        return {
            "controller_state": {
                "failed": True, "failure": "boom", "last_failed_run": "run-1",
                "failed_at": "19700101T001500Z",
                "failure_notification": {
                    "run_id": "run-1", "failed_at": "19700101T001500Z",
                    "reason": "boom", "sent_at": 900, "destination": destination,
                },
            },
            "disk_free_bytes": 20 * 1024**3,
        }

    def audit(self, path, data, timestamp, *, posted=True, selected=None, webhook=WEBHOOK):
        args = argparse.Namespace(
            config=Path("unused"), node=selected, dry_run=False,
            max_completion_age=0, reminder_interval=86400, state_file=path,
            legacy_digest=True,
        )
        node = deploy.Node({"name": "node", "ssh_string": "root@host"})
        with (
            patch.object(deploy, "load_nodes", return_value=[node]),
            patch.object(deploy, "remote_json", return_value=(True, data)),
            patch.object(deploy, "now", return_value=timestamp),
            patch.object(deploy, "post_slack", return_value=posted) as post,
            patch.dict(os.environ, {"SLACK_WEB_HOOK": webhook}),
        ):
            result = deploy.cmd_audit(args)
        return result, post

    def test_halt_receipt_requires_successful_delivery(self):
        for delivered in (False, True):
            with self.subTest(delivered=delivered), tempfile.TemporaryDirectory() as tmp:
                config = make_config(Path(tmp))
                path = config.paths.state_dir / "state.json"
                with (
                    patch.object(sync, "post_slack", return_value=delivered),
                    patch.object(sync, "now", return_value=900),
                    patch.dict(os.environ, {"SLACK_WEB_HOOK": self.WEBHOOK}),
                ):
                    sync.halt(config, path, {}, {"run_id": "run-1"}, "boom")
                state = sync.load_state(path)
                self.assertTrue(state["failed"])
                self.assertEqual("failure_notification" in state, delivered)
                with patch.object(deploy, "now", return_value=1000):
                    problem = deploy.audit_problem(
                        {"controller_state": state}, 0,
                        hashlib.sha256(self.WEBHOOK.encode()).hexdigest(),
                    )
                self.assertEqual(problem.delivered_at, 900 if delivered else None)
                self.assertIn("run-1", problem.detail)

    def test_receipt_must_match_incident_destination_and_valid_delivery_time(self):
        destination = hashlib.sha256(self.WEBHOOK.encode()).hexdigest()
        for key, value in (
            ("run_id", "different"), ("failed_at", "different"),
            ("reason", "different"), ("destination", "other-channel"),
            ("sent_at", 2000), ("sent_at", -1), ("sent_at", True), ("sent_at", "900"),
        ):
            with self.subTest(key=key, value=value):
                data = self.halted_status()
                data["controller_state"]["failure_notification"][key] = value
                with patch.object(deploy, "now", return_value=1000):
                    self.assertIsNone(deploy.audit_problem(data, 0, destination).delivered_at)

    def test_audit_adopts_confirmed_failure_then_reports_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            result, post = self.audit(path, self.halted_status(), 1000)
            self.assertEqual(result, 1)
            post.assert_not_called()
            self.assertIn("node", deploy.load_audit_state(path)["problems"])
            healthy = {"controller_state": {"phase": "complete"}, "disk_free_bytes": 20 * 1024**3}
            result, post = self.audit(path, healthy, 1100)
            self.assertEqual(result, 0)
            self.assertIn("recovered", post.call_args.args[0])
            _, post = self.audit(path, healthy, 1200)
            post.assert_not_called()

    def test_changed_problem_and_new_run_are_not_hidden_by_old_delivery(self):
        for old in (
            deploy.Problem("unreachable", "unreachable"),
            deploy.Problem("controller-halted:boom", "boom", "old-run"),
        ):
            with self.subTest(old=old):
                _, _, _, previous = deploy.audit_transitions({"node": old}, {}, 86400, 1000)
                with patch.object(deploy, "now", return_value=1100):
                    problem = deploy.audit_problem(
                        self.halted_status(), 0,
                        hashlib.sha256(self.WEBHOOK.encode()).hexdigest(),
                    )
                new, _, _, _ = deploy.audit_transitions({"node": problem}, previous, 86400, 1100)
                self.assertEqual(len(new), 1)

    def test_destination_change_realerts_cached_failure_and_retries_delivery(self):
        other_webhook = "https://slack.invalid/other"
        for receipt in (True, False):
            with self.subTest(receipt=receipt), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "state.json"
                data = self.halted_status()
                if not receipt:
                    del data["controller_state"]["failure_notification"]
                self.audit(path, data, 1000)
                before = path.read_text()
                result, post = self.audit(path, data, 1100, webhook=other_webhook, posted=False)
                self.assertEqual(result, 1)
                post.assert_called_once()
                self.assertIn("controller halted", post.call_args.args[0])
                self.assertEqual(path.read_text(), before)

                _, post = self.audit(path, data, 1200, webhook=other_webhook)
                post.assert_called_once()
                state = deploy.load_audit_state(path)
                self.assertEqual(state["problems"]["node"]["destination"], hashlib.sha256(other_webhook.encode()).hexdigest())
                _, post = self.audit(path, data, 1300, webhook=other_webhook)
                post.assert_not_called()

    def test_destination_change_accepts_receipt_for_new_destination(self):
        other_webhook = "https://slack.invalid/other"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            data = self.halted_status()
            self.audit(path, data, 1000)
            data["controller_state"]["failure_notification"].update({
                "destination": hashlib.sha256(other_webhook.encode()).hexdigest(),
                "sent_at": 1050,
            })
            result, post = self.audit(path, data, 1100, webhook=other_webhook)
            self.assertEqual(result, 1)
            post.assert_not_called()
            self.assertEqual(deploy.load_audit_state(path)["problems"]["node"]["last_sent"], 1050)

    def test_recovery_requires_cached_delivery_to_current_destination(self):
        for webhook in (self.WEBHOOK, "https://slack.invalid/other"):
            with self.subTest(webhook=webhook), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "state.json"
                self.audit(path, self.halted_status(), 1000)
                healthy = {"controller_state": {}, "disk_free_bytes": 20 * 1024**3}
                result, post = self.audit(path, healthy, 1100, webhook=webhook)
                self.assertEqual(result, 0)
                if webhook == self.WEBHOOK:
                    post.assert_called_once()
                    self.assertIn("recovered", post.call_args.args[0])
                else:
                    post.assert_not_called()

    def test_cache_without_destination_cannot_suppress_undelivered_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            data = self.halted_status()
            del data["controller_state"]["failure_notification"]
            self.audit(path, data, 1000)
            state = deploy.load_audit_state(path)
            state["problems"]["node"].pop("destination", None)
            deploy.save_audit_state(path, state)
            _, post = self.audit(path, data, 1100)
            post.assert_called_once()
            self.assertIn("controller halted", post.call_args.args[0])

    def test_missing_receipt_preserves_audit_fallback_and_failed_delivery_retry(self):
        data = self.halted_status()
        del data["controller_state"]["failure_notification"]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            _, post = self.audit(path, data, 1000, posted=False)
            post.assert_called_once()
            self.assertFalse(path.exists())
            _, post = self.audit(path, data, 1100)
            post.assert_called_once()

    def test_daily_digest_combines_pending_successes_and_unresolved_failure(self):
        data = self.halted_status()
        data["controller_state"].update({
            "completion_digest": True, "completion_digest_start_runs": 0,
            "last_success_run": "success-3", "last_success_sha": "abc",
            "last_success_duration_seconds": 3600, "runs": 3,
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            _, post = self.audit(path, data, 1000)
            post.assert_not_called()
            _, post = self.audit(path, data, 22600)
            post.assert_not_called()  # The former six-hour reminder is now in the digest.
            old = path.read_text()
            _, post = self.audit(path, data, 87400, posted=False)
            text = post.call_args.args[0]
            self.assertIn("unresolved", text)
            self.assertIn("3 completed", text)
            self.assertEqual(path.read_text(), old)
            _, post = self.audit(path, data, 87460)
            self.assertIn("3 completed", post.call_args.args[0])
            _, post = self.audit(path, data, 88000)
            post.assert_not_called()

    def test_digest_respects_failure_delivery_age(self):
        boundary = 200000
        for previously_observed in (False, True):
            for age in (5, 86399, 86400, 86401):
                with self.subTest(previously_observed=previously_observed, age=age), tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "state.json"
                    data = self.halted_status()
                    sent_at = boundary - age
                    data["controller_state"]["failure_notification"]["sent_at"] = sent_at
                    deploy.save_audit_state(path, {
                        "version": deploy.AUDIT_STATE_VERSION, "problems": {},
                        "last_digest_at": boundary - 86400,
                    })
                    if previously_observed:
                        _, post = self.audit(path, data, boundary - 1)
                        post.assert_not_called()

                    result, post = self.audit(path, data, boundary)
                    self.assertEqual(result, 1)
                    if age < 86400:
                        post.assert_called_once()
                        self.assertIn("0 completed", post.call_args.args[0])
                        self.assertNotIn("unresolved", post.call_args.args[0])
                        self.assertNotIn("controller halted: boom", post.call_args.args[0])
                        self.assertEqual(deploy.load_audit_state(path)["problems"]["node"]["last_sent"], sent_at)
                    else:
                        post.assert_called_once()
                        self.assertIn("unresolved", post.call_args.args[0])

                    _, post = self.audit(path, data, boundary + 86400)
                    post.assert_called_once()
                    self.assertIn("unresolved", post.call_args.args[0])
                    _, post = self.audit(path, data, boundary + 86401)
                    post.assert_not_called()

    def test_recent_failure_does_not_block_completion_digest(self):
        data = self.halted_status()
        data["controller_state"]["failure_notification"]["sent_at"] = 87395
        data["controller_state"].update({
            "completion_digest": True, "completion_digest_start_runs": 0,
            "last_success_run": "success-3", "runs": 3,
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            deploy.save_audit_state(path, {
                "version": deploy.AUDIT_STATE_VERSION, "problems": {},
                "last_digest_at": 1000,
            })
            result, post = self.audit(path, data, 87400)
            self.assertEqual(result, 1)
            post.assert_called_once()
            self.assertIn("3 completed", post.call_args.args[0])
            self.assertNotIn("unresolved", post.call_args.args[0])
            self.assertEqual(deploy.load_audit_state(path)["problems"]["node"]["last_sent"], 87395)

    def test_digest_boundary_preserves_undelivered_failure_alert(self):
        data = self.halted_status()
        del data["controller_state"]["failure_notification"]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            deploy.save_audit_state(path, {
                "version": deploy.AUDIT_STATE_VERSION, "problems": {},
                "last_digest_at": 1000,
            })
            before = path.read_text()
            result, post = self.audit(path, data, 87400, posted=False)
            self.assertEqual(result, 1)
            post.assert_called_once()
            self.assertIn("controller halted", post.call_args.args[0])
            self.assertEqual(path.read_text(), before)
            _, post = self.audit(path, data, 87460)
            post.assert_called_once()
            self.assertIn("controller halted", post.call_args.args[0])

    def test_completion_counter_reset_and_missing_host_preserve_pending_digest(self):
        previous = {"completions": {"node": {
            "run_id": "old", "total": 10, "pending": 2, "sha": "old", "duration": 30,
        }}}
        data = {"node": {"controller_state": {
            "completion_digest": True, "last_success_run": "new", "runs": 1,
            "last_success_sha": "new", "last_success_duration_seconds": 40,
        }}}
        lines, records = deploy.completion_updates(data, previous, False)
        self.assertEqual(lines, [])
        self.assertEqual(records["node"]["pending"], 3)
        lines, records = deploy.completion_updates({}, {"completions": records}, True)
        self.assertIn("3 completed", lines[0])
        self.assertNotIn("node", records)
        self.assertEqual(previous["completions"]["node"]["pending"], 2)

    def test_three_mode_summary_shows_one_three_and_one_runs(self):
        statuses, labels = {}, {}
        cases = (
            ("dual", [24000], "Dual networking", [145]),
            ("zakura", [25800, 25200, 26400], "Zakura networking only", [134, 138, 131]),
            ("legacy", [28800], "Legacy networking only", [120]),
        )
        for mode, durations, label, rates in cases:
            node = deploy.Node({"name": mode, "p2p_stack": mode})
            labels[mode] = deploy.sync_label(node)
            statuses[mode] = {"controller_state": {
                "phase": "syncing", "completion_digest": True, "completion_digest_start_runs": 0,
                "runs": len(durations), "last_success_run": f"{mode}-{len(durations)}",
                "last_success_duration_seconds": durations[-1], "last_success_end_height": 3469999,
                "completion_history": [
                    {"number": n, "run_id": f"{mode}-{n}", "duration": duration, "end_height": 3469999}
                    for n, duration in enumerate(durations, 1)
                ],
            }}
        lines, _ = deploy.completion_updates(statuses, {}, True, labels)
        self.assertEqual(len(lines), 3)
        for mode, durations, label, rates in cases:
            section = next(line for line in lines if f"({mode})" in line)
            expected = [f"*{label} ({mode}) · {len(durations)} completed*"]
            expected.extend(
                f"• {duration // 3600}h {duration % 3600 // 60:02d}m · {rate} blocks/sec"
                for duration, rate in zip(durations, rates)
            )
            expected.append("currently syncing")
            self.assertEqual(section, "\n".join(expected))

    def test_completion_throughput_requires_valid_height_and_nonzero_duration(self):
        self.assertEqual(deploy.completion_run_text({"duration": 24000, "end_height": 3469999}),
                         "6h 40m · 145 blocks/sec")
        self.assertEqual(deploy.completion_run_text({"duration": 2, "end_height": 3}),
                         "0h 00m · 2 blocks/sec")
        self.assertEqual(deploy.completion_run_text({"duration": 1, "end_height": 0}),
                         "0h 00m · 1 blocks/sec")
        for height in (None, -1, True, "3469999", 2**32):
            with self.subTest(height=height):
                self.assertIn("BPS unavailable",
                              deploy.completion_run_text({"duration": 3600, "end_height": height}))
        for duration in (0, None, -1, True, "3600"):
            with self.subTest(duration=duration):
                self.assertIn("BPS unavailable",
                              deploy.completion_run_text({"duration": duration, "end_height": 3469999}))

    def test_retired_hosts_deliver_pending_once_then_leave_the_summary(self):
        previous = {"completions": {name: {
            "run_id": name, "total": 2, "pending": pending,
            "sha": "abc", "duration": 3600,
        } for name, pending in (("retired", 2), ("already-delivered", 0), ("active", 0))}}
        labels = {"active": "Active host"}
        before = json.dumps(previous, sort_keys=True)
        _, waiting = deploy.completion_updates({}, previous, False, labels)
        self.assertEqual(waiting, previous["completions"])
        lines, records = deploy.completion_updates({}, previous, True, labels)
        self.assertEqual(len(lines), 2)
        self.assertIn("retired · 2 completed", lines[1])
        self.assertIn("Active host · 0 completed", lines[0])
        self.assertEqual(set(records), {"active"})
        # Failed delivery leaves the input cache intact, so a retry is identical.
        self.assertEqual(json.dumps(previous, sort_keys=True), before)
        retry, _ = deploy.completion_updates({}, previous, True, labels)
        self.assertEqual(retry, lines)
        later, _ = deploy.completion_updates({}, {"completions": records}, True, labels)
        self.assertEqual(len(later), 1)
        self.assertIn("Active host · 0 completed", later[0])

    def test_digest_preserves_all_timings_across_missed_audits(self):
        controller = {
            "completion_digest": True, "completion_digest_start_runs": 0,
            "last_success_run": "run-3", "runs": 3,
            "last_success_duration_seconds": 25989, "phase": "syncing",
            "completion_history": [
                {"number": 1, "run_id": "run-1", "duration": 26979},
                {"number": 2, "run_id": "run-2", "duration": 25696},
                {"number": 3, "run_id": "run-3", "duration": 25989},
            ],
        }
        data = {"node": {"controller_state": controller}}
        label = deploy.sync_label(deploy.Node({"name": "node", "p2p_stack": "zakura"}))
        _, records = deploy.completion_updates(data, {}, False, {"node": label})
        before = json.dumps(records, sort_keys=True)
        lines, delivered = deploy.completion_updates(data, {"completions": records}, True)
        self.assertIn("Zakura networking only (node)", lines[0])
        self.assertIn("3 completed", lines[0])
        self.assertEqual([line.split(" · ")[0] for line in lines[0].splitlines()[1:4]],
                         ["• 7h 29m", "• 7h 08m", "• 7h 13m"])
        self.assertIn("currently syncing", lines[0])
        self.assertEqual(json.dumps(records, sort_keys=True), before)
        lines, _ = deploy.completion_updates(data, {"completions": delivered}, True)
        self.assertIn("0 completed", lines[0])
        self.assertNotIn("7h", lines[0])

    def test_multiple_audits_and_failed_delivery_preserve_each_duration(self):
        data = {"controller_state": {
            "completion_digest": True, "completion_digest_start_runs": 0,
            "last_success_run": "run-1", "last_success_duration_seconds": 3600,
            "runs": 1, "phase": "syncing",
            "last_success_end_height": 3599999,
            "completion_history": [{"number": 1, "run_id": "run-1", "duration": 3600, "end_height": 3599999}],
        }, "disk_free_bytes": 20 * 1024**3,
            "service_active": True, "sample": {"metrics_status": "ok"}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            _, post = self.audit(path, data, 1000)
            post.assert_not_called()
            data["controller_state"].update({
                "last_success_run": "run-3", "last_success_duration_seconds": 10800,
                "last_success_end_height": 3779999, "runs": 3,
                "completion_history": [
                    {"number": n, "run_id": f"run-{n}", "duration": n * 3600, "end_height": blocks - 1}
                    for n, blocks in enumerate((3600000, 3240000, 3780000), 1)
                ],
            })
            _, post = self.audit(path, data, 2000)
            post.assert_not_called()
            saved = path.read_text()
            _, post = self.audit(path, data, 87400, posted=False)
            expected = ("• 1h 00m · 1000 blocks/sec\n"
                        "• 2h 00m · 450 blocks/sec\n"
                        "• 3h 00m · 350 blocks/sec")
            self.assertIn(expected, post.call_args.args[0])
            self.assertEqual(path.read_text(), saved)
            _, post = self.audit(path, data, 87460)
            self.assertIn(expected, post.call_args.args[0])
            _, post = self.audit(path, data, 88000)
            post.assert_not_called()
            self.assertEqual(deploy.load_audit_state(path)["completions"]["node"]["pending"], 0)

    def test_invalid_cached_details_cannot_break_failure_notifications(self):
        for details in (None, "bad", [None], [{}] * 257):
            with self.subTest(details_type=type(details)), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "state.json"
                deploy.save_audit_state(path, {
                    "version": deploy.AUDIT_STATE_VERSION, "problems": {},
                    "completions": {"node": {
                        "run_id": "old", "total": 1, "pending": 1,
                        "sha": "abc", "duration": 60, "details": details,
                    }},
                })
                data = self.halted_status()
                del data["controller_state"]["failure_notification"]
                result, post = self.audit(path, data, 1000)
                self.assertEqual(result, 1)
                self.assertIn("controller halted", post.call_args.args[0])

    def test_zero_completion_summary_does_not_reset_first_completion_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            data = {"controller_state": {"runs": 260, "phase": "complete"},
                    "disk_free_bytes": 20 * 1024**3}
            self.audit(path, data, 1000)
            _, post = self.audit(path, data, 87400)
            self.assertIn("0 completed", post.call_args.args[0])
            data["controller_state"].update({
                "runs": 261, "completion_digest": True, "completion_digest_start_runs": 260,
                "last_success_run": "new", "last_success_duration_seconds": 3600,
            })
            self.audit(path, data, 88000)
            records = deploy.load_audit_state(path)["completions"]
            self.assertEqual(records["node"]["pending"], 1)
            _, post = self.audit(path, data, 173800)
            self.assertIn("1 completed*\n• 1h 00m", post.call_args.args[0])

    def test_sync_status_does_not_hide_inactive_service_or_missing_metrics(self):
        for extra, expected in (
            ({"service_active": False}, "node service inactive"),
            ({"sample": {"metrics_status": "connection refused"}},
             "sync status unavailable (metrics unavailable)"),
        ):
            with self.subTest(expected=expected):
                data = {"controller_state": {"phase": "syncing"}, **extra}
                lines, _ = deploy.completion_updates({"node": data}, {}, True)
                self.assertIn(expected, lines[0])
                self.assertNotIn("currently syncing", lines[0])

    def test_malformed_controller_history_preserves_counts_and_failure_alerts(self):
        for history in (None, {}, "bad", 7, [None, {"number": "bad"}]):
            with self.subTest(history_type=type(history)), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "state.json"
                data = self.halted_status()
                del data["controller_state"]["failure_notification"]
                data["controller_state"].update({
                    "completion_digest": True, "completion_digest_start_runs": 0,
                    "runs": 3, "last_success_run": "run-3",
                    "last_success_duration_seconds": 3600, "completion_history": history,
                })
                result, post = self.audit(path, data, 1000)
                self.assertEqual(result, 1)
                self.assertIn("controller halted", post.call_args.args[0])
                records = deploy.load_audit_state(path)["completions"]
                self.assertEqual(records["node"]["pending"], 3)
                _, post = self.audit(path, data, 87400)
                self.assertIn("3 completed*\n• 1h 00m", post.call_args.args[0])
                self.assertIn("2 earlier run(s): details unavailable", post.call_args.args[0])

    def test_digest_upgrade_and_retention_report_missing_timings(self):
        previous = {"completions": {"node": {
            "run_id": "old", "total": 4, "pending": 3, "duration": 3600,
        }}}
        data = {"node": {"controller_state": {
            "completion_digest": True, "last_success_run": "new", "runs": 6,
            "last_success_duration_seconds": 7200,
        }}}
        lines, _ = deploy.completion_updates(data, previous, True)
        self.assertIn("5 completed*\n• 1h 00m", lines[0])
        self.assertIn("• 2h 00m", lines[0])
        self.assertIn("3 earlier run(s): details unavailable", lines[0])
        controller = data["node"]["controller_state"]
        controller.update({
            "completion_digest_start_runs": 0, "runs": 300,
            "completion_history": [
                {"number": n, "run_id": f"run-{n}", "duration": 3600}
                for n in range(1, 300)
            ],
        })
        _, records = deploy.completion_updates(data, {}, False)
        self.assertEqual(len(records["node"]["details"]), 256)
        lines, _ = deploy.completion_updates({}, {"completions": records}, True)
        self.assertIn("300 completed", lines[0])
        self.assertIn("44 earlier run(s): details unavailable", lines[0])
        self.assertIn("status unavailable", lines[0])

    def test_digest_lists_zero_completions_with_observed_status(self):
        data = {
            "dual": {"controller_state": {"phase": "syncing"}},
            "legacy": {"controller_state": {"phase": "syncing", "failed": True}},
        }
        labels = {name: deploy.sync_label(deploy.Node({"name": name, "p2p_stack": mode}))
                  for name, mode in (("dual", "dual"), ("legacy", "legacy"), ("new", "zakura"))}
        lines, _ = deploy.completion_updates(data, {}, True, labels)
        self.assertEqual(len(lines), 3)
        self.assertIn("Dual networking (dual) · 0 completed*\ncurrently syncing", lines[0])
        self.assertIn("Legacy networking only (legacy) · 0 completed*\nhalted after failure", lines[1])
        self.assertIn("Zakura networking only (new) · 0 completed*\nstatus unavailable", lines[2])

    def test_targeted_audit_cannot_recover_unobserved_nodes_or_consume_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            _, _, _, state = deploy.audit_transitions(
                {"other": deploy.Problem("unreachable", "unreachable")}, {}, 86400, 1
            )
            state["last_digest_at"] = 1
            deploy.save_audit_state(path, state)
            healthy = {"controller_state": {}, "disk_free_bytes": 20 * 1024**3}
            _, post = self.audit(path, healthy, 100000, selected="node")
            post.assert_not_called()
            loaded = deploy.load_audit_state(path)
            self.assertIn("other", loaded["problems"])
            self.assertEqual(loaded["last_digest_at"], 1)

    def test_disk_recovery_wipes_before_headroom_and_announces_after_start(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.ExitStack() as stack:
            root = Path(tmp)
            config = make_config(root)
            path = config.paths.state_dir / "state.json"
            for directory in (root / "state", root / "network"):
                directory.mkdir()
                (directory / "marker").write_text("keep network only")
            config.paths.wipe_sentinel.touch()
            config.paths.config_template.write_text('[tracing]\nlog_file = "{{LOG_FILE}}"\n')
            sync.save_state(path, {"failed": True, "failure": "DiskPressure: low", "last_failed_run": "old-run"})
            usage = sync.shutil.disk_usage(root)
            def disk_usage(_):
                return usage._replace(free=(12 if (root / "state").exists() else 30) * 1024**3)
            stack.enter_context(patch.dict(os.environ, {"ZAKURA_CONTINUOUS_SYNC_TESTING": "1"}))
            stack.enter_context(patch.object(sync.shutil, "disk_usage", side_effect=disk_usage))
            for name in ("preflight", "build_binary", "sha256_file", "install_binary", "stop_service", "rotate_run_logs"):
                stack.enter_context(patch.object(sync, name, return_value="test"))
            stack.enter_context(patch.object(sync, "resolve_sha", return_value="a" * 40))
            started = stack.enter_context(patch.object(sync, "start_service"))
            stack.enter_context(patch.object(sync, "service_active", return_value=True))
            def observe_message(_, text):
                started.assert_called_once()
                self.assertIn("old-run", text)
                self.assertIn("resumed", text)
                return True
            post = stack.enter_context(patch.object(sync, "post_slack", side_effect=observe_message))
            stack.enter_context(patch.object(sync, "sample_status", side_effect=KeyboardInterrupt))
            with self.assertRaises(KeyboardInterrupt):
                sync.run_loop(config, Path("/unused"))
            post.assert_called_once()
            self.assertFalse((root / "state").exists())
            self.assertTrue((root / "network" / "marker").exists())
            self.assertNotIn("disk_recovery_run", sync.load_state(path))
            self.assertEqual(sync.load_state(path)["phase"], "stopping")

    def test_recovery_delivery_retries_from_persisted_state_until_confirmed(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            run_dir = config.paths.runs_dir / "current"
            run_dir.mkdir(parents=True)
            path = config.paths.state_dir / "state.json"
            sync.save_state(path, {"disk_recovery_run": "failed-run"})
            with (
                patch.object(sync, "check_free_space"),
                patch.object(sync, "service_active", return_value=True),
                patch.object(sync, "rotate_run_logs"),
                patch.object(sync, "post_slack", side_effect=[False, True]) as post,
                patch.object(sync, "sample_status", return_value={"height": 1}),
                patch.object(sync.time, "sleep", side_effect=KeyboardInterrupt),
            ):
                for attempt in range(3):
                    with self.assertRaises(KeyboardInterrupt):
                        sync.wait_for_completion(config, run_dir, {}, sync.load_state(path))
                    self.assertEqual("disk_recovery_run" in sync.load_state(path), attempt == 0)
            self.assertEqual(post.call_count, 2)
            self.assertEqual(post.call_args_list[0], post.call_args_list[1])

    def test_preflight_failure_receipt_is_accepted_by_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(Path(tmp))
            path = config.paths.state_dir / "state.json"
            with patch.object(sync, "post_slack", return_value=True):
                sync.halt(config, path, {}, {}, "DiskPressure: preflight low")
            state = sync.load_state(path)
            receipt = state["failure_notification"]
            self.assertTrue(state["last_failed_run"].startswith("preflight-"))
            problem = deploy.audit_problem({"controller_state": state}, 0, receipt["destination"])
            self.assertEqual(problem.delivered_at, receipt["sent_at"])

    def test_successful_cycle_records_digest_without_sending_routine_message(self):
        valid_history = [
            {"number": n, "run_id": f"old-{n}", "duration": 100} for n in range(5, 261)
        ]
        for persisted_history in (valid_history, None, {}, "bad", 7, [None]):
            with self.subTest(history_type=type(persisted_history)):
                with tempfile.TemporaryDirectory() as tmp, contextlib.ExitStack() as stack:
                    config = make_config(Path(tmp))
                    for name in (
                        "preflight", "build_binary", "sha256_file", "install_binary", "stop_service",
                        "safe_wipe_state", "render_config", "start_service",
                        "rotate_run_logs", "cleanup_retention",
                    ):
                        stack.enter_context(patch.object(sync, name, return_value="test"))
                    stack.enter_context(patch.object(sync, "wait_for_completion",
                        side_effect=lambda _config, _run_dir, run_state, _state: run_state.update(end_height=3469999)))
                    stack.enter_context(patch.object(sync, "resolve_sha", return_value="a" * 40))
                    stack.enter_context(patch.object(sync, "now", return_value=1000))
                    post = stack.enter_context(patch.object(sync, "post_slack"))
                    path = config.paths.state_dir / "state.json"
                    state = sync.one_cycle(config, path, {
                        "runs": 260, "completion_history": persisted_history,
                    })
                    post.assert_not_called()
                    self.assertFalse(state["failed"])
                    self.assertEqual(state["phase"], "complete")
                    self.assertEqual(state["runs"], 261)
                    history = sync.load_state(path)["completion_history"]
                    self.assertEqual(len(history), min(
                        len(persisted_history) + 1 if isinstance(persisted_history, list) else 1,
                        sync.COMPLETION_HISTORY_LIMIT,
                    ))
                    self.assertEqual(history[-1], {
                        "number": 261, "run_id": state["current_run"], "duration": 0,
                        "end_height": 3469999,
                        "trace_archive_url": None,
                    })
                    self.assertEqual(state["last_success_end_height"], 3469999)
                    self.assertEqual(state["completion_digest_start_runs"], 260)
                    self.assertEqual(sync.load_state(path)["last_success_run"], state["current_run"])


class CanaryNotificationTests(unittest.TestCase):
    def event(self, **updates):
        return {
            "RUN_ID": "123", "RUN_ATTEMPT": "1", "EXECUTION_ATTEMPT": "1",
            "RESULT": "failure", "TARGET_SHA": "a" * 40, "FAILURE_PHASE": "node-execution",
            "MAX_CHECKPOINT": "100", "START_HEIGHT": "90", "END_HEIGHT": "99",
            "RUN_URL": "https://github.invalid/actions/runs/123", **updates,
        }

    def test_only_duplicate_notification_of_same_execution_is_suppressed(self):
        event = self.event()
        text, state = canary.transition(event, {}, 1000)
        self.assertIn("canary failed", text)
        text, _ = canary.transition(self.event(RUN_ATTEMPT="2"), state, 1100)
        self.assertEqual(text, "")
        for key, value in (
            ("RUN_ID", "124"), ("EXECUTION_ATTEMPT", "2"), ("TARGET_SHA", "b" * 40),
            ("FAILURE_PHASE", "node-startup"), ("END_HEIGHT", "98"),
        ):
            with self.subTest(key=key):
                text, _ = canary.transition(self.event(**{key: value}), state, 1100)
                self.assertIn("canary failed", text)

    def test_incomplete_diagnostics_always_alert(self):
        for key in ("TARGET_SHA", "FAILURE_PHASE", "MAX_CHECKPOINT", "START_HEIGHT", "END_HEIGHT", "EXECUTION_ATTEMPT"):
            with self.subTest(key=key):
                event = self.event(**{key: ""})
                _, state = canary.transition(event, {}, 1000)
                text, _ = canary.transition(event, state, 1100)
                self.assertIn("canary failed", text)

    def test_recovery_is_once_and_cancelled_run_does_not_clear_failure(self):
        _, state = canary.transition(self.event(), {}, 1000)
        for result in ("cancelled", "skipped"):
            text, unchanged = canary.transition(self.event(RESULT=result), state, 1100)
            self.assertEqual((text, unchanged), ("", state))
        text, cleared = canary.transition(self.event(RESULT="success"), state, 1200)
        self.assertIn("recovered", text)
        text, _ = canary.transition(self.event(RESULT="success"), cleared, 1300)
        self.assertEqual(text, "")

    def test_failed_delivery_does_not_commit_failure_or_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            for result, delivered in (("failure", False), ("failure", True), ("success", False), ("success", True)):
                with self.subTest(result=result, delivered=delivered):
                    before = path.read_text() if path.exists() else None
                    with (
                        patch.object(sys, "argv", ["canary-notify.py", "--state-file", str(path)]),
                        patch.dict(os.environ, self.event(RESULT=result)),
                        patch.object(canary, "slack_webhook_url", return_value="https://slack.invalid/test"),
                        patch.object(canary, "post_slack", return_value=delivered),
                        patch.object(canary.time, "time", return_value=1000),
                    ):
                        self.assertEqual(canary.main(), 0 if delivered else 1)
                    if not delivered:
                        self.assertEqual(path.read_text() if path.exists() else None, before)


def alert_status_fixture(
    hostname: str,
    *,
    service_active: bool | None,
    phase: str = "syncing",
    height: int | None = None,
):
    return {
        "hostname": hostname,
        "public_ip": "138.68.43.212",
        "mode": "dual-stack",
        "service": "zakura.service",
        "service_active": service_active,
        "metrics_status": "ok" if service_active else "unavailable",
        "height": height,
        "connection": "root@138.68.43.212",
        "alias_connection": f"ssh {hostname}",
        "log_path": "/tmp/zebrad.log",
        "trace_path": "/tmp/traces",
        "monitor_log_path": "/tmp/monitor.log",
        "controller_state": {"phase": phase, "failed": False},
    }


def alert_config(tmp_path: Path, hostnames: list[str], **default_overrides):
    defaults = {
        "alert_state_file": str(tmp_path / "state.json"),
        "monitor_log": str(tmp_path / "monitor.log"),
        "down_confirmation_samples": 2,
    }
    defaults.update(default_overrides)
    return {
        "defaults": defaults,
        "nodes": [{"hostname": hostname} for hostname in hostnames],
    }


def make_config(tmp_path: Path, **overrides):
    paths = {
        "repo_dir": tmp_path / "repo",
        "state_dir": tmp_path / "controller",
        "runs_dir": tmp_path / "runs",
        "chain_state_dir": tmp_path,
        "wipe_sentinel": tmp_path / ".sentinel",
        "build_cache_dir": tmp_path / "build-cache",
        "config_template": tmp_path / "template.toml",
        "zakurad_config": tmp_path / "zebrad.toml",
        "bin_path": tmp_path / "zakurad",
        "log_file": tmp_path / "zebrad.log",
        "monitor_log": tmp_path / "monitor.log",
        "trace_link": tmp_path / "traces",
    }
    policy = overrides.pop("policy", sync.Policy())
    paths.update(overrides)
    return sync.Config(paths=sync.Paths(**paths), policy=policy)


if __name__ == "__main__":
    unittest.main()
