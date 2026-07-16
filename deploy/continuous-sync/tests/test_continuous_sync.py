import importlib.util
import json
import os
import sys
import tempfile
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
alert = load_module("continuous_sync_alert", ALERT_PATH)
alert_status = load_module("continuous_sync_alert_status", ALERT_STATUS_PATH)


class ContinuousSyncTests(unittest.TestCase):
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

    def test_alert_status_falls_back_to_estimated_height(self):
        metrics = "\n".join(
            [
                "sync_estimated_network_tip_height 1000",
                "sync_estimated_distance_to_tip 100",
            ]
        )

        self.assertEqual(alert_status.metric_height(metrics), 900)

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
                ["cargo", "git", "systemctl"],
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

    def test_cleanup_retention_keeps_active_and_two_newest_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            runs_dir = tmp_path / "runs"
            active = runs_dir / "active"
            completed = [runs_dir / f"completed-{index}" for index in range(4)]
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

    def test_archive_run_log_copies_current_log_and_truncates_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            run_dir = tmp_path / "runs" / "current"
            run_dir.mkdir(parents=True)
            config = make_config(tmp_path)
            config.paths.log_file.write_text("current run log\n", encoding="utf-8")

            sync.archive_run_log(config, run_dir)

            self.assertEqual(
                (run_dir / "zebrad.log").read_text(encoding="utf-8"),
                "current run log\n",
            )
            self.assertEqual(config.paths.log_file.read_text(encoding="utf-8"), "")

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

    def test_deploy_renders_expanded_legacy_alert_inventory(self):
        nodes = deploy.load_nodes(
            ROOT / "deploy" / "continuous-sync" / "nodes.toml",
            ["temp-zakura-sync-test-4"],
        )
        rendered = deploy.render_files(nodes[0])

        self.assertIn('p2p_stack = "legacy"', rendered["zakurad.toml.template"])
        self.assertIn('mode_label = "Zebra/legacy-only"', rendered["controller.toml"])
        self.assertIn('branch = "main"', rendered["controller.toml"])
        self.assertEqual(rendered["alert-monitor.toml"].count("[[nodes]]"), 7)
        for index in range(1, 8):
            self.assertIn(
                f'hostname = "temp-zakura-sync-test-{index}"',
                rendered["alert-monitor.toml"],
            )

    def test_deploy_does_not_stop_node_before_restarting_controller(self):
        self.assertNotIn('systemctl stop "$node_service"', deploy.INSTALL_SCRIPT)

    def test_deploy_creates_zakurad_config_parent_directory(self):
        self.assertIn('dirname "$config_path"', deploy.INSTALL_SCRIPT)

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

    def test_controller_failure_alerts_immediately(self):
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
            "controller_state": {
                "phase": "failed",
                "failed": True,
                "failure": "build failed",
            },
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

            post_alert.assert_called_once()


    def test_alert_text_is_concise_and_normalizes_mode(self):
        text = alert.main_alert_text(
            "TEST ALERT",
            {
                "hostname": "temp-zakura-sync-test-2",
                "mode": "Zakura/v2-only",
                "public_ip": "138.197.218.91",
            },
            "controller halted: a very noisy reason",
        )

        self.assertEqual(
            text,
            ":rotating_light: Zakura continuous sync alert: temp-zakura-sync-test-2 | v2p2p | root@138.197.218.91",
        )
        self.assertNotIn("\n", text)

    def test_controller_slack_text_is_concise(self):
        config = make_config(
            Path("/tmp"),
            policy=sync.Policy(
                hostname="temp-zakura-sync-test-3",
                p2p_stack="zebra",
                public_ip="134.209.49.92",
            ),
        )

        text = sync.failure_text(
            config,
            {"sha": "abcdef", "time_to_failure_seconds": 3723},
            "boom",
        )

        self.assertEqual(
            text,
            ":rotating_light: Zakura failed: temp-zakura-sync-test-3 | legacy | "
            "root@134.209.49.92 | time to failure: 1h 2m 3s",
        )
        self.assertNotIn("\n", text)

    def test_completion_slack_text_includes_sync_duration(self):
        config = make_config(
            Path("/tmp"),
            policy=sync.Policy(
                hostname="temp-zakura-sync-test-2",
                p2p_stack="zakura",
                public_ip="138.197.218.91",
            ),
        )

        text = sync.completion_text(config, {"sync_duration_seconds": 90061})

        self.assertEqual(
            text,
            ":white_check_mark: Zakura sync complete: temp-zakura-sync-test-2 | v2p2p | "
            "root@138.197.218.91 | sync time: 1d 1h 1m 1s",
        )

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
