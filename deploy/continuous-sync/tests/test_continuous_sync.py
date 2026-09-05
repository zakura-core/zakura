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

    def test_deploy_renders_each_node_public_ip_as_external_address(self):
        nodes = deploy.load_nodes(
            ROOT / "deploy" / "continuous-sync" / "nodes.toml",
            None,
        )

        for node in nodes:
            with self.subTest(node=node.name):
                rendered = deploy.render_files(node)
                config = tomllib.loads(rendered["zakurad.toml.template"])
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
            self.assertIn("3 completed run(s)", text)
            self.assertEqual(path.read_text(), old)
            _, post = self.audit(path, data, 87460)
            self.assertIn("3 completed run(s)", post.call_args.args[0])
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
                        post.assert_not_called()
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
            self.assertIn("3 completed run(s)", post.call_args.args[0])
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
        self.assertIn("3 completed run(s)", lines[0])
        self.assertEqual(records["node"]["pending"], 0)
        self.assertEqual(previous["completions"]["node"]["pending"], 2)

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

    def test_successful_cycle_records_digest_without_sending_routine_message(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.ExitStack() as stack:
            config = make_config(Path(tmp))
            for name in (
                "preflight", "build_binary", "sha256_file", "install_binary", "stop_service",
                "safe_wipe_state", "render_config", "start_service", "wait_for_completion",
                "archive_run_log", "cleanup_retention",
            ):
                stack.enter_context(patch.object(sync, name, return_value="test"))
            stack.enter_context(patch.object(sync, "resolve_sha", return_value="a" * 40))
            stack.enter_context(patch.object(sync, "now", return_value=1000))
            post = stack.enter_context(patch.object(sync, "post_slack"))
            path = config.paths.state_dir / "state.json"
            state = sync.one_cycle(config, path, {"runs": 4})
            post.assert_not_called()
            self.assertEqual(state["runs"], 5)
            self.assertEqual(state["completion_digest_start_runs"], 4)
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
