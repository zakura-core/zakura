import importlib.util
import os
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("deploy", SCRIPT_DIR / "deploy.py")
deploy = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
# Register before exec: dataclasses resolves a class's module through
# sys.modules, and raises on Python 3.12+ when a decorated class is defined in
# a module that was never registered.
sys.modules["deploy"] = deploy
SPEC.loader.exec_module(deploy)


class BuildCacheTests(unittest.TestCase):
    def test_build_cache_dir_uses_env_override(self):
        with mock.patch.dict(os.environ, {
            deploy.BUILD_CACHE_DIR_ENV: "/tmp/zakura-build-cache",
        }):
            self.assertEqual(deploy.build_cache_dir(), Path("/tmp/zakura-build-cache"))
            self.assertEqual(
                deploy.cached_binary("abc123"),
                Path("/tmp/zakura-build-cache/zakurad-abc123"),
            )

    def test_cached_binary_reused_without_build(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            sha = "a" * 40
            target = cache_dir / f"zakurad-{sha}"
            target.write_text("cached")

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: str(cache_dir)}), \
                    mock.patch.object(deploy, "binary_is_runnable", return_value=True), \
                    mock.patch.object(deploy, "run") as run:
                self.assertEqual(deploy.build_commit(Path(tmp), sha), target)
                run.assert_not_called()

    def test_corrupt_cached_binary_is_rebuilt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_dir = root / "cache"
            cache_dir.mkdir()
            sha = "b" * 40
            target = cache_dir / f"zakurad-{sha}"
            target.write_text("corrupt")
            calls = []

            def fake_run(cmd, *, cwd=None, capture=False, check=True):
                calls.append(cmd)
                if cmd[:3] == ["git", "worktree", "add"]:
                    Path(cmd[-2]).mkdir(parents=True)
                if cmd[:2] == ["cargo", "build"]:
                    built = Path(cwd) / "target" / "release" / "zakurad"
                    built.parent.mkdir(parents=True)
                    built.write_text("rebuilt")
                return mock.Mock(returncode=0, stdout="")

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: str(cache_dir)}), \
                    mock.patch.object(deploy, "binary_is_runnable", return_value=False), \
                    mock.patch.object(deploy, "run", side_effect=fake_run):
                self.assertEqual(deploy.build_commit(root, sha), target)

            self.assertEqual(target.read_text(), "rebuilt")
            self.assertIn(["cargo", "build", "--release", "--locked", "-p", "zakura"], calls)

    def test_force_rebuild_skips_cached_binary_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_dir = root / "cache"
            cache_dir.mkdir()
            sha = "c" * 40
            (cache_dir / f"zakurad-{sha}").write_text("cached")

            def fake_run(cmd, *, cwd=None, capture=False, check=True):
                if cmd[:3] == ["git", "worktree", "add"]:
                    Path(cmd[-2]).mkdir(parents=True)
                if cmd[:2] == ["cargo", "build"]:
                    built = Path(cwd) / "target" / "release" / "zakurad"
                    built.parent.mkdir(parents=True)
                    built.write_text("forced")
                return mock.Mock(returncode=0, stdout="")

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: str(cache_dir)}), \
                    mock.patch.object(deploy, "binary_is_runnable") as binary_is_runnable, \
                    mock.patch.object(deploy, "run", side_effect=fake_run):
                deploy.build_commit(root, sha, force=True)

            binary_is_runnable.assert_not_called()

    def test_prune_cached_binaries_keeps_current_and_recent(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            current_sha = "d" * 40
            (cache_dir / f"zakurad-{current_sha}").write_text("current")
            old = []
            for idx in range(5):
                path = cache_dir / f"zakurad-{idx:040x}"
                path.write_text(str(idx))
                os.utime(path, (100 + idx, 100 + idx))
                old.append(path)

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_RETAIN_ENV: "3"}):
                deploy.prune_cached_binaries(cache_dir, current_sha)

            remaining = {path.name for path in cache_dir.iterdir()}
            self.assertEqual(remaining, {
                f"zakurad-{current_sha}",
                old[4].name,
                old[3].name,
            })


class DeploymentSuppressionTests(unittest.TestCase):
    def test_suppression_script_uses_node_watchdog_configuration(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "watchdog" / "deployment-suppressed-until"
            duration = 30
            node = mock.Mock(
                watchdog_deployment_suppression_file=str(marker),
                watchdog_max_deployment_suppression=duration,
            )
            before = int(time.time())

            subprocess.run(
                ["bash", "-c", deploy.watchdog_deployment_suppression_script(node)],
                check=True,
            )

            after = int(time.time())
            suppression_until = int(marker.read_text().strip())
            self.assertGreaterEqual(
                suppression_until,
                before + duration,
            )
            self.assertLessEqual(
                suppression_until,
                after + duration,
            )
            self.assertEqual(marker.stat().st_mode & 0o777, 0o644)

    def test_suppression_script_preserves_existing_directory_permissions(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker_dir = Path(tmp) / "private-watchdog"
            marker_dir.mkdir(mode=0o700)
            marker = marker_dir / "deployment-suppressed-until"
            node = mock.Mock(
                watchdog_deployment_suppression_file=str(marker),
                watchdog_max_deployment_suppression=30,
            )

            subprocess.run(
                ["bash", "-c", deploy.watchdog_deployment_suppression_script(node)],
                check=True,
            )

            self.assertEqual(marker_dir.stat().st_mode & 0o777, 0o700)

    def test_suppression_script_rejects_timestamp_overflow(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "watchdog" / "deployment-suppressed-until"
            node = mock.Mock(
                watchdog_deployment_suppression_file=str(marker),
                watchdog_max_deployment_suppression=deploy.SHELL_SIGNED_INTEGER_MAX,
            )

            result = subprocess.run(
                ["bash", "-c", deploy.watchdog_deployment_suppression_script(node)],
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("exceeds shell arithmetic range", result.stderr)
            self.assertFalse(marker.exists())

    def test_suppression_script_publishes_marker_atomically(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "watchdog" / "deployment-suppressed-until"
            marker.parent.mkdir()
            marker.write_text("old\n")
            node = mock.Mock(
                watchdog_deployment_suppression_file=str(marker),
                watchdog_max_deployment_suppression=30,
            )
            assert_old_marker_during_write = """
set -e
printf() {
    [ "$(<\"$WATCHDOG_TEST_MARKER\")" = "old" ] || return 90
    command printf "$@"
}
"""

            subprocess.run(
                [
                    "bash",
                    "-c",
                    assert_old_marker_during_write
                    + deploy.watchdog_deployment_suppression_script(node),
                ],
                check=True,
                env={**os.environ, "WATCHDOG_TEST_MARKER": str(marker)},
            )

            self.assertGreater(int(marker.read_text().strip()), int(time.time()))
            self.assertEqual(list(marker.parent.glob(f"{marker.name}.tmp.*")), [])

    def test_systemd_restarts_activate_suppression_before_replacing_artifacts(self):
        marker_command = "ACTIVATE_WATCHDOG_SUPPRESSION"
        scripts = {
            "managed": (
                deploy.INSTALL_SCRIPT.format(
                    bin_path="/usr/local/bin/zakurad",
                    config_path="/etc/zakura/zakura.toml",
                    service="zakurad",
                    log_file="/var/log/zakura/zakura.log",
                    state_dir="/var/lib/zakura",
                    no_restart="0",
                    watchdog_suppression=marker_command,
                ),
                "install -m 644 /tmp/zakurad-deploy.service",
            ),
            "binary-only": (
                deploy.BINARY_ONLY_INSTALL_SCRIPT.format(
                    bin_path="/usr/local/bin/zakurad",
                    service="zakurad",
                    no_restart="0",
                    watchdog_suppression=marker_command,
                ),
                "install -m 755 /tmp/zakurad-deploy.new",
            ),
        }

        for name, (script, first_artifact_install) in scripts.items():
            with self.subTest(name=name):
                subprocess.run(["bash", "-n"], input=script, text=True, check=True)
                restart_guard = script.index('if [ "$NO_RESTART" != "1" ]')
                suppression = script.index(marker_command)
                restart_guard_end = script.index("\nfi", suppression)
                artifact_install = script.index(first_artifact_install)
                self.assertLess(restart_guard, suppression)
                self.assertLess(suppression, restart_guard_end)
                self.assertLess(restart_guard_end, artifact_install)

    def test_mainnet_workflow_bounds_and_tolerates_suppression_ssh_failures(self):
        workflow = (
            SCRIPT_DIR.parent.parent
            / ".github/workflows/zakura-mainnet-deploy.yml"
        ).read_text()
        suppression_step = workflow.split(
            "      - name: Suppress node watchdog alerts during deploy\n", 1
        )[1].split("      - name: Deploy fleet\n", 1)[0]

        self.assertIn(
            "if ! timeout --signal=TERM --kill-after=5s 60s \\", suppression_step
        )
        self.assertIn("-o ServerAliveInterval=10 \\", suppression_step)
        self.assertIn("-o ServerAliveCountMax=3 \\", suppression_step)
        self.assertIn("continuing deploy", suppression_step)


class NodeBuilder:
    """Shared minimal node fixture for the rendering tests."""

    def node(self, **overrides):
        data = {
            "name": "node-a",
            "ssh_string": "root@example",
            "commit": "main",
            "deploy_kind": "systemd",
            "manage_config": True,
            "service_name": "zakurad",
            "bin_path": "/usr/local/bin/zakurad",
            "config_path": "/etc/zakura/zakura.toml",
            "log_file": "/var/log/zakura/zakura.log",
            "state_cache_dir": "/var/lib/zakura",
            "network": "Testnet",
            "listen_addr": "0.0.0.0:18233",
            "identity_dir": "",
            "network_cache_dir": "",
            "rpc_listen_addr": "",
            "rpc_enable_cookie_auth": None,
            "storage_mode": "archive",
            "p2p_stack": "dual",
            "metrics_endpoint": "",
            "health_listen_addr": "",
            "tracing_filter": "",
            "checkpoint_sync": True,
            "vct_fast_sync": True,
            "zakura": None,
            "working_dir": "",
            "start_command": "",
            "process_pattern": "",
            "container_name": "",
            "watchdog_deployment_suppression_file": str(
                deploy.WATCHDOG_DEPLOYMENT_SUPPRESSION_FILE
            ),
            "watchdog_max_deployment_suppression": (
                deploy.WATCHDOG_MAX_DEPLOYMENT_SUPPRESSION
            ),
        }
        data.update(overrides)
        return deploy.Node(**data)


class MountRenderingTests(NodeBuilder, unittest.TestCase):
    def test_render_service_requires_data_mount_for_data_paths(self):
        service = deploy.render_service(self.node(
            state_cache_dir="/mnt/data/zakura-cache",
            log_file="/mnt/data/logs/zakura.log",
        ))

        self.assertIn("RequiresMountsFor=/mnt/data", service)
        self.assertIn("AssertPathIsMountPoint=/mnt/data", service)

    def test_render_service_omits_mount_for_non_data_paths(self):
        service = deploy.render_service(self.node())

        self.assertNotIn("RequiresMountsFor=/mnt/data", service)
        self.assertNotIn("AssertPathIsMountPoint=/mnt/data", service)


class ObservabilityRenderingTests(NodeBuilder, unittest.TestCase):
    """The [metrics] and [health] sections are opt-in and must round-trip as TOML."""

    def test_endpoints_render_when_configured(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            metrics_endpoint="127.0.0.1:9999",
            health_listen_addr="127.0.0.1:8080",
        )))

        self.assertEqual(config["metrics"], {"endpoint_addr": "127.0.0.1:9999"})
        self.assertEqual(config["health"], {"listen_addr": "127.0.0.1:8080"})

    def test_endpoints_omitted_when_unset(self):
        config = tomllib.loads(deploy.render_node_config(self.node()))

        self.assertNotIn("metrics", config)
        self.assertNotIn("health", config)

    def test_health_renders_independently_of_metrics(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            health_listen_addr="127.0.0.1:8080",
        )))

        self.assertNotIn("metrics", config)
        self.assertEqual(config["health"], {"listen_addr": "127.0.0.1:8080"})


class ConfigKeyTests(unittest.TestCase):
    def write_config(self, tmp: str, body: str) -> Path:
        path = Path(tmp) / "nodes.toml"
        path.write_text(body)
        return path

    def test_health_listen_addr_is_a_known_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
                [defaults]
                health_listen_addr = "127.0.0.1:8080"

                [[nodes]]
                name = "node-a"
                ssh_string = "root@example"
                commit = "main"
            """.replace("                ", ""))

            nodes = deploy.load_nodes(path, None)

            self.assertEqual(nodes[0].health_listen_addr, "127.0.0.1:8080")

    def test_watchdog_suppression_settings_are_per_node(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
                [defaults]
                watchdog_deployment_suppression_file = "/run/custom-watchdog/default"
                watchdog_max_deployment_suppression = 45

                [[nodes]]
                name = "node-a"
                ssh_string = "root@example"
                commit = "main"
                watchdog_deployment_suppression_file = "/run/custom-watchdog/node-a"
                watchdog_max_deployment_suppression = 30
            """.replace("                ", ""))

            node = deploy.load_nodes(path, None)[0]

            self.assertEqual(
                node.watchdog_deployment_suppression_file,
                "/run/custom-watchdog/node-a",
            )
            self.assertEqual(node.watchdog_max_deployment_suppression, 30)

    def test_watchdog_suppression_file_must_be_absolute(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
                [defaults]
                watchdog_deployment_suppression_file = "relative/marker"

                [[nodes]]
                name = "node-a"
                ssh_string = "root@example"
                commit = "main"
            """.replace("                ", ""))

            with self.assertRaisesRegex(deploy.DeployError, "must be an absolute path"):
                deploy.load_nodes(path, None)

    def test_unknown_key_still_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
                [defaults]
                health_listen_address = "127.0.0.1:8080"

                [[nodes]]
                name = "node-a"
                ssh_string = "root@example"
                commit = "main"
            """.replace("                ", ""))

            with self.assertRaises(deploy.DeployError):
                deploy.load_nodes(path, None)


if __name__ == "__main__":
    unittest.main()
