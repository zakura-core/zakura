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
                deploy.prune_cached_binaries(cache_dir, f"zakurad-{current_sha}")

            remaining = {path.name for path in cache_dir.iterdir()}
            self.assertEqual(remaining, {
                f"zakurad-{current_sha}",
                old[4].name,
                old[3].name,
            })


class MainnetWorkflowSuppressionTests(unittest.TestCase):
    def test_suppression_is_scoped_to_compatibility_restarts(self):
        workflow = (
            SCRIPT_DIR.parent.parent
            / ".github/workflows/zakura-mainnet-deploy.yml"
        ).read_text()
        suppression_step = workflow.split(
            "      - name: Suppress compatibility watchdog alerts during deploy\n",
            1,
        )[1].split("      - name: Deploy fleet\n", 1)[0]

        self.assertIn(
            "inputs.node == '' || inputs.node == 'zakura-compat'",
            suppression_step,
        )
        self.assertIn("!inputs.no_restart", suppression_step)
        self.assertIn("root@159.203.113.196", suppression_step)
        self.assertIn(
            "/run/zakura-watchdog/deployment-suppressed-until",
            suppression_step,
        )
        self.assertIn(
            "if ! timeout --signal=TERM --kill-after=5s 60s \\",
            suppression_step,
        )
        self.assertIn("systemctl restart zakura-watchdog", suppression_step)
        self.assertIn("continuing deploy", suppression_step)
        self.assertNotIn("tomllib", suppression_step)


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
            "initial_testnet_peers": None,
            "rpc_listen_addr": "",
            "rpc_enable_cookie_auth": None,
            "storage_mode": "archive",
            "p2p_stack": "dual",
            "metrics_endpoint": "",
            "health_listen_addr": "",
            "miner_address": "",
            "tracing_filter": "",
            "checkpoint_sync": True,
            "vct_fast_sync": True,
            "zakura": None,
            "testnet_parameters": None,
            "cargo_features": "",
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

    def test_mining_section_renders_when_a_miner_address_is_set(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            miner_address="tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
        )))

        # getblocktemplate is refused without it, so an external miner on the
        # fork could never produce a block.
        self.assertEqual(
            config["mining"], {"miner_address": "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"}
        )

    def test_mining_section_omitted_when_unset(self):
        config = tomllib.loads(deploy.render_node_config(self.node()))

        self.assertNotIn("mining", config)

    def test_health_renders_independently_of_metrics(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            health_listen_addr="127.0.0.1:8080",
        )))

        self.assertNotIn("metrics", config)
        self.assertEqual(config["health"], {"listen_addr": "127.0.0.1:8080"})


class TestnetParametersRenderingTests(NodeBuilder, unittest.TestCase):
    """A configured testnet such as the NU7 fork renders [network.testnet_parameters]."""

    FORK_PARAMS = {
        "network_name": "Nu7Fork",
        "network_magic": [0xF0, 0x0D, 0xCA, 0xFE],
        "checkpoints": True,
        "initial_nsm_value_balance": 55_768_414_957,
        "activation_heights": {
            "BeforeOverwinter": 1,
            "Overwinter": 207_500,
            "Sapling": 280_000,
            "Blossom": 584_000,
            "Heartwood": 903_800,
            "Canopy": 1_028_500,
            "NU5": 1_842_420,
            "NU6": 2_976_000,
            "NU6.1": 3_536_500,
            "NU6.2": 4_052_000,
            "NU6.3": 4_134_000,
            "NU7": 4_376_000,
        },
    }

    def test_omitted_when_unset(self):
        config = tomllib.loads(deploy.render_node_config(self.node()))

        self.assertNotIn("testnet_parameters", config["network"])

    def test_fork_parameters_round_trip(self):
        config = tomllib.loads(deploy.render_node_config(
            self.node(testnet_parameters=self.FORK_PARAMS)
        ))

        params = config["network"]["testnet_parameters"]
        self.assertEqual(params["network_name"], "Nu7Fork")
        self.assertEqual(params["network_magic"], [240, 13, 202, 254])
        self.assertEqual(params["initial_nsm_value_balance"], 55_768_414_957)
        # Genesis-only checkpoints would make the node verify 4M blocks from scratch.
        self.assertTrue(params["checkpoints"])

    def test_dotted_upgrade_names_are_quoted(self):
        config = tomllib.loads(deploy.render_node_config(
            self.node(testnet_parameters=self.FORK_PARAMS)
        ))

        heights = config["network"]["testnet_parameters"]["activation_heights"]
        # An unquoted "NU6.1" would parse as a nested table, silently dropping the
        # height, and a partial list wipes every upgrade above it in the builder.
        self.assertEqual(heights["NU6.1"], 3_536_500)
        self.assertEqual(heights["NU7"], 4_376_000)
        self.assertEqual(len(heights), 12)

    def test_lockbox_disbursements_render_as_an_array_of_tables(self):
        config = tomllib.loads(deploy.render_node_config(self.node(testnet_parameters={
            "network_name": "Nu7Fork",
            "lockbox_disbursements": [{"address": "t2Lockbox", "amount": 0}],
        })))

        disbursements = config["network"]["testnet_parameters"]["lockbox_disbursements"]
        self.assertEqual(disbursements, [{"address": "t2Lockbox", "amount": 0}])

    def test_empty_peer_list_renders_for_an_incompatible_testnet(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            testnet_parameters=self.FORK_PARAMS,
            initial_testnet_peers=[],
        )))

        # zakurad refuses to load a config that pairs the default public DNS
        # seeds with parameters incompatible with the public Testnet.
        self.assertEqual(config["network"]["initial_testnet_peers"], [])

    def test_peer_list_omitted_when_unset(self):
        config = tomllib.loads(deploy.render_node_config(self.node()))

        self.assertNotIn("initial_testnet_peers", config["network"])

    def test_zakura_block_still_renders_alongside(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            testnet_parameters=self.FORK_PARAMS,
            zakura={"listen_addr": "0.0.0.0:8234", "bootstrap_peers": ["abc@1.2.3.4:8234"]},
        )))

        self.assertEqual(config["network"]["zakura"]["listen_addr"], "0.0.0.0:8234")
        self.assertEqual(config["network"]["testnet_parameters"]["network_name"], "Nu7Fork")


class CargoFeatureTests(unittest.TestCase):
    """Featured builds must never collide with the stock fleet binary in the cache."""

    def test_stock_build_keeps_its_historic_cache_name(self):
        with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: "/tmp/cache"}):
            self.assertEqual(
                deploy.cached_binary("abc123", ""),
                Path("/tmp/cache/zakurad-abc123"),
            )

    def test_featured_build_gets_a_distinct_cache_name(self):
        with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: "/tmp/cache"}):
            stock = deploy.cached_binary("abc123", "")
            featured = deploy.cached_binary("abc123", "internal-miner")

            self.assertNotEqual(stock, featured)
            self.assertTrue(featured.name.startswith("zakurad-abc123-"))

    def test_feature_slug_ignores_order_and_spacing(self):
        self.assertEqual(
            deploy.feature_slug("internal-miner, indexer"),
            deploy.feature_slug("indexer,internal-miner"),
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

    def test_testnet_parameters_and_cargo_features_are_known_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
                [defaults]
                cargo_features = "internal-miner"

                [defaults.testnet_parameters]
                network_name = "Nu7Fork"

                [defaults.testnet_parameters.activation_heights]
                NU7 = 4376000

                [[nodes]]
                name = "node-a"
                ssh_string = "root@example"
                commit = "main"
            """.replace("                ", ""))

            nodes = deploy.load_nodes(path, None)

            self.assertEqual(nodes[0].cargo_features, "internal-miner")
            self.assertEqual(
                nodes[0].testnet_parameters["activation_heights"]["NU7"], 4_376_000
            )

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
