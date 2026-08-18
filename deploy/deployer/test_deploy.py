import importlib.util
import os
import sys
import tempfile
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
            "block_propagation_trace_dir": "",
            "zakura": None,
            "working_dir": "",
            "start_command": "",
            "process_pattern": "",
            "container_name": "",
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

    def test_render_service_sets_stable_trace_node_id(self):
        service = deploy.render_service(self.node(name="testnet-observer-eu"))

        self.assertIn("Environment=ZAKURA_NODE_ID=testnet-observer-eu", service)


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

    def test_dedicated_propagation_directory_renders_in_zakura_section(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            zakura={
                "block_propagation_trace_dir": "/mnt/data/traces/block-propagation",
            },
        )))

        self.assertEqual(
            config["network"]["zakura"]["block_propagation_trace_dir"],
            "/mnt/data/traces/block-propagation",
        )


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

    def test_binary_only_block_propagation_trace_dir_is_loaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
                [defaults]
                manage_config = false
                block_propagation_trace_dir = "/mnt/data/traces/block-propagation"

                [[nodes]]
                name = "node-a"
                ssh_string = "root@example"
                commit = "main"
            """.replace("                ", ""))

            nodes = deploy.load_nodes(path, None)

            self.assertEqual(
                nodes[0].block_propagation_trace_dir,
                "/mnt/data/traces/block-propagation",
            )

    def test_block_propagation_trace_dir_rejects_managed_or_external_paths(self):
        for defaults in (
            'block_propagation_trace_dir = "/mnt/data/traces/block-propagation"',
            'manage_config = false\nblock_propagation_trace_dir = "/tmp/traces"',
        ):
            with self.subTest(defaults=defaults), tempfile.TemporaryDirectory() as tmp:
                path = self.write_config(tmp, f"""
                    [defaults]
                    {defaults}

                    [[nodes]]
                    name = "node-a"
                    ssh_string = "root@example"
                    commit = "main"
                """.replace("                    ", ""))

                with self.assertRaises(deploy.DeployError):
                    deploy.load_nodes(path, None)


class BinaryOnlyTraceOverrideTests(unittest.TestCase):
    def render_script(self, trace_dir, no_restart="0"):
        return deploy.BINARY_ONLY_INSTALL_SCRIPT.format(
            bin_path="'/usr/local/bin/zakurad'",
            service="'zakurad'",
            no_restart=no_restart,
            block_propagation_trace_dir=f"'{trace_dir}'",
            node_id="'us-east-0'",
        )

    def test_enable_stages_narrow_environment_override(self):
        script = self.render_script("/mnt/data/traces/block-propagation")

        self.assertIn(
            "ZAKURA_NETWORK__ZAKURA__BLOCK_PROPAGATION_TRACE_DIR=%s",
            script,
        )
        self.assertIn('Environment="ZAKURA_NODE_ID=%s"', script)
        self.assertIn('install -d -m 755 "$TRACE_DIR" "$DROP_IN_DIR"', script)
        self.assertIn("systemctl daemon-reload", script)

    def test_disable_removes_only_owned_drop_in(self):
        script = self.render_script("")

        self.assertIn('DROP_IN="${DROP_IN_DIR}/50-zakura-block-propagation-trace.conf"', script)
        self.assertIn('rm -f "$DROP_IN"', script)

    def test_failure_restores_binary_and_previous_drop_in(self):
        script = self.render_script("/mnt/data/traces/block-propagation")

        self.assertIn('install -m 755 "${BIN_PATH}.bak" "$BIN_PATH"', script)
        self.assertIn('install -m 644 "$DROP_IN_BACKUP" "$DROP_IN"', script)
        self.assertIn("rolling back binary and trace override", script)

    def test_no_restart_stages_drop_in_before_exiting(self):
        script = self.render_script(
            "/mnt/data/traces/block-propagation",
            no_restart="1",
        )

        self.assertLess(
            script.index('install -m 644 "${DROP_IN}.new" "$DROP_IN"'),
            script.index('if [ "$NO_RESTART" = "1" ]'),
        )
        self.assertIn(
            "installed binary and propagation trace override (restart skipped)",
            script,
        )


if __name__ == "__main__":
    unittest.main()
