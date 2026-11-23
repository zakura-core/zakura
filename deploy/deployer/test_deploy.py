import importlib.util
import os
import re
import subprocess
import sys
import threading
import tempfile
import tomllib
import types
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

    def test_feature_builds_are_cached_separately_from_the_stock_build(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_dir = root / "cache"
            cache_dir.mkdir()
            sha = "e" * 40
            (cache_dir / f"zakurad-{sha}").write_text("stock")
            calls = []

            def fake_run(cmd, *, cwd=None, capture=False, check=True):
                calls.append(cmd)
                if cmd[:3] == ["git", "worktree", "add"]:
                    Path(cmd[-2]).mkdir(parents=True)
                if cmd[:2] == ["cargo", "build"]:
                    built = Path(cwd) / "target" / "release" / "zakurad"
                    built.parent.mkdir(parents=True)
                    built.write_text("miner")
                return mock.Mock(returncode=0, stdout="")

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: str(cache_dir)}), \
                    mock.patch.object(deploy, "binary_is_runnable", return_value=True), \
                    mock.patch.object(deploy, "run", side_effect=fake_run):
                built = deploy.build_commit(root, sha, features=["internal-miner"])

            # The stock binary for the same commit must not satisfy a miner build.
            self.assertEqual(built, cache_dir / f"zakurad-{sha}-internal-miner")
            self.assertEqual(built.read_text(), "miner")
            self.assertEqual((cache_dir / f"zakurad-{sha}").read_text(), "stock")
            self.assertIn(["cargo", "build", "--release", "--locked", "-p", "zakura",
                           "--features", "internal-miner"], calls)

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


class MainnetWorkflowSuppressionTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (
            SCRIPT_DIR.parent.parent
            / ".github/workflows/zakura-mainnet-deploy.yml"
        ).read_text()

    def step(self, name):
        return self.workflow.split(f"      - name: {name}\n", 1)[1].split("      - name:", 1)[0]

    def test_compatibility_mute_and_readiness_only_run_for_compatibility_restarts(self):
        for name in ("Suppress compatibility watchdog alerts during deploy",
                     "Wait for the compatibility sidecar to sync"):
            with self.subTest(step=name):
                self.assertIn(
                    "if: ${{ inputs.operation == 'deploy' && !inputs.no_restart "
                    "&& (inputs.node == '' || inputs.node == 'zakura-compat') }}",
                    self.step(name),
                )
        self.assertIn("zakura-monitoring-deploy.py check-compat-readiness",
                      self.step("Wait for the compatibility sidecar to sync"))
        self.assertNotIn("continue-on-error", self.step("Wait for the compatibility sidecar to sync"))

    def test_both_mutes_are_cleared_even_if_deploy_or_readiness_fails(self):
        steps = [
            "Install profiler and prepare trace files",
            "Suppress fleet watchdog alerts during deploy",
            "Suppress compatibility watchdog alerts during deploy",
            "Deploy fleet",
            "Install fleet watchdog",
            "Wait for the compatibility sidecar to sync",
            "Restore compatibility watchdog alerts",
            "Restore fleet watchdog alerts",
        ]
        positions = [self.workflow.index(f"      - name: {name}\n") for name in steps]
        self.assertEqual(positions, sorted(positions))
        for label, target in (("compatibility", "compat"), ("fleet", "fleet")):
            with self.subTest(marker=target):
                restore = self.step(f"Restore {label} watchdog alerts")
                self.assertIn(f"always() && steps.mute_{target}.outputs.expires_at != ''", restore)
                self.assertIn(f'watchdog-mute.sh clear-{target} "$EXPIRES_AT"', restore)

    def test_cleanup_only_removes_the_timestamp_owned_by_the_run(self):
        script = SCRIPT_DIR / "mainnet" / "watchdog-mute.sh"
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "deploy-suppressed-until"
            expires_at = subprocess.check_output(
                ["bash", str(script), "arm-marker", str(marker)], text=True, timeout=5,
            ).strip()
            clear = ["bash", str(script), "clear-marker", str(marker), expires_at]
            self.assertEqual(marker.read_text(), expires_at + "\n")

            newer = str(int(expires_at) + 1) + "\n"
            marker.write_text(newer)
            subprocess.run(clear, check=True, timeout=5)
            self.assertEqual(marker.read_text(), newer)

            marker.write_text(expires_at + "\n")
            subprocess.run(clear, check=True, timeout=5)
            self.assertFalse(marker.exists())
            subprocess.run(clear, check=True, timeout=5)

    def test_fleet_mute_requires_effective_runtime_directory_preservation(self):
        script = SCRIPT_DIR / "mainnet" / "watchdog-mute.sh"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            marker = root / "deploy-suppressed-until"
            drop_in = root / "60-deploy-mute.conf"
            sudo = root / "sudo"
            sudo.write_text(
                '#!/bin/bash\n'
                'case "$2" in\n'
                '    install) cat > "$MOCK_DROP_IN" ;;\n'
                '    systemctl) grep -qx RuntimeDirectoryPreserve=restart "$MOCK_DROP_IN" ;;\n'
                '    bash) exec bash "$3" "$4" "$MOCK_MARKER" ;;\n'
                '    *) exit 2 ;;\n'
                'esac\n'
            )
            systemctl = root / "systemctl"
            systemctl.write_text(
                '#!/bin/bash\n'
                'case "$2" in\n'
                '    --property=LoadState) echo loaded ;;\n'
                '    --property=RuntimeDirectoryPreserve) echo "$MOCK_PRESERVE" ;;\n'
                '    *) exit 2 ;;\n'
                'esac\n'
            )
            sudo.chmod(0o755)
            systemctl.chmod(0o755)
            env = {**os.environ, "PATH": f"{root}{os.pathsep}{os.environ['PATH']}",
                   "MOCK_DROP_IN": str(drop_in), "MOCK_MARKER": str(marker)}
            arm = ["bash", str(script), "arm-fleet"]

            blocked = subprocess.run(arm, env={**env, "MOCK_PRESERVE": "no"},
                                     capture_output=True, text=True, timeout=5)
            self.assertNotEqual(blocked.returncode, 0)
            self.assertIn("would remove its mute file on restart", blocked.stderr)
            self.assertFalse(marker.exists())

            armed = subprocess.check_output(arm, env={**env, "MOCK_PRESERVE": "restart"},
                                            text=True, timeout=5)
            self.assertEqual(drop_in.read_text(), "[Service]\nRuntimeDirectoryPreserve=restart\n")
            self.assertEqual(marker.read_text(), armed)


class MainnetWorkflowMonitoringTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (
            SCRIPT_DIR.parent.parent / ".github/workflows/zakura-mainnet-deploy.yml"
        ).read_text()
        self.job = self.workflow.split("\n  monitoring:\n", 1)[1].split("\n  deploy:\n", 1)[0]

    def test_monitoring_is_an_explicit_operation_for_the_compat_node(self):
        self.assertIn("if: inputs.operation == 'monitoring'", self.job)
        self.assertIn('if [ "${NODE}" != "zakura-compat" ]; then', self.job)
        self.assertIn("^[0-9a-f]{40}$", self.job)
        for stage in ("install", "validate", "cutover", "soak", "finalize", "rollback"):
            self.assertIn(stage, self.workflow.split("monitoring_stage:", 1)[1].split("\n\n", 1)[0])

    def test_fleet_job_runs_for_dashboard_or_non_mac_deploys(self):
        fleet_job = self.workflow.split("\n  deploy:\n", 1)[1]
        self.assertEqual(
            fleet_job.splitlines()[0].strip(),
            "if: inputs.operation == 'dashboard' || (inputs.operation == 'deploy' && inputs.node != 'zakura-mac-os')",
        )

    def test_only_mac_deploy_status_bypasses_fleet_serialization(self):
        concurrency = self.workflow.split("\nconcurrency:\n", 1)[1].split("\njobs:\n", 1)[0]
        self.assertIn(
            "inputs.operation == 'deploy' && inputs.node == 'zakura-mac-os' && inputs.mac_operation == 'status'",
            concurrency,
        )
        self.assertIn("|| 'zakura-mainnet-deploy'", concurrency)

    def test_monitoring_never_touches_nodes_dashboard_or_gateway(self):
        for forbidden in ("python3 deploy.py", "deploy/deployer", "cargo", "zakurad", "dashboard", "broadcast",
                          "caddy", "systemctl", "SLACK_WEB_HOOK"):
            self.assertNotIn(forbidden, self.job)
        self.assertIn("deploy/runner/zakura-monitoring-deploy.py", self.job)
        self.assertIn("ref: ${{ inputs.ref }}", self.job)

    def test_regular_deploys_share_the_installer(self):
        step = self.workflow.split("      - name: Install fleet watchdog\n", 1)[1].split(
            "      - name: Fetch recent logs on failure\n", 1)[0]
        self.assertIn("zakura-monitoring-deploy.py fleet-deploy", step)
        self.assertNotIn("zakura-fleet-watchdog.service /etc/systemd/system", step)
        self.assertNotIn("install -m 600", step)


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
            "mempool_implementation": "",
            "tx_relay": False,
            "metrics_endpoint": "",
            "health_listen_addr": "",
            "miner_address": "",
            "internal_miner": False,
            "extra_coinbase_data": "",
            "build_features": [],
            "tracing_filter": "",
            "checkpoint_sync": True,
            "vct_fast_sync": True,
            "zakura": None,
            "testnet_parameters": None,
            "working_dir": "",
            "start_command": "",
            "process_pattern": "",
            "container_name": "",
        }
        data.update(overrides)
        return deploy.Node(**data)


class NetworkSysctlTests(NodeBuilder, unittest.TestCase):
    def test_sysctl_file_raises_udp_socket_buffers(self):
        settings = {}
        for line in deploy.NETWORK_SYSCTL_CONF.read_text().splitlines():
            if line.strip() and not line.startswith("#"):
                key, value = (part.strip() for part in line.split("=", 1))
                settings[key] = int(value)
        # netwatch requests 7 MiB buffers for the QUIC socket; the caps must admit them.
        for key in ("rmem_max", "wmem_max"):
            self.assertGreaterEqual(settings[f"net.core.{key}"], 7 << 20)
        self.assertNotIn("net.core.rmem_default", settings)
        self.assertNotIn("net.core.wmem_default", settings)
        self.assertEqual(settings["net.ipv4.tcp_slow_start_after_idle"], 0)

    def test_deploy_installs_sysctls_before_the_node(self):
        node = self.node(manage_config=False)
        events = []

        def fake_ssh(_node, script):
            events.append("sysctl" if "sysctl --load" in script else "install")
            return mock.Mock(returncode=0)

        with mock.patch.object(deploy, "load_nodes", return_value=[node]), \
                mock.patch.object(deploy, "build_nodes", return_value={"": Path("/bin/true")}), \
                mock.patch.object(deploy, "build_publishers", return_value={}), \
                mock.patch.object(deploy, "run", return_value=mock.Mock(stdout="/tmp/zakurad-deploy.test")), \
                mock.patch.object(deploy, "ssh_with_stdin", side_effect=fake_ssh):
            args = mock.Mock(config="nodes.toml", node=None, force=False, no_restart=False)
            self.assertEqual(deploy.cmd_deploy(args), 0)
        self.assertEqual(events, ["sysctl", "install"])

    def test_sysctl_failure_fails_the_node(self):
        node = self.node(manage_config=False)
        with mock.patch.object(deploy, "load_nodes", return_value=[node]), \
                mock.patch.object(deploy, "build_nodes", return_value={"": Path("/bin/true")}), \
                mock.patch.object(deploy, "build_publishers", return_value={}), \
                mock.patch.object(deploy, "run", return_value=mock.Mock(stdout="/tmp/zakurad-deploy.test")), \
                mock.patch.object(deploy, "ssh_with_stdin", return_value=mock.Mock(returncode=1)) as ssh:
            args = mock.Mock(config="nodes.toml", node=None, force=False, no_restart=False)
            self.assertEqual(deploy.cmd_deploy(args), 1)
        ssh.assert_called_once()


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


class BlockDownloadConfigTests(NodeBuilder, unittest.TestCase):
    def test_mainnet_configs_enable_block_download_scoring_and_hedging(self):
        config = tomllib.loads(deploy.render_node_config(self.node(network="Mainnet")))
        self.assertIs(config["network"]["block_download_score_and_hedge"]["enabled"], True)

    def test_other_networks_keep_their_binary_default(self):
        for network in ("Testnet", "Regtest"):
            with self.subTest(network=network):
                config = tomllib.loads(deploy.render_node_config(self.node(network=network)))
                self.assertNotIn("block_download_score_and_hedge", config["network"])


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

    def test_internal_miner_renders_with_a_distinct_coinbase_tag(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            miner_address="tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
            internal_miner=True,
            extra_coinbase_data="nu7-us",
            build_features=["internal-miner"],
        )))

        self.assertEqual(config["mining"], {
            "miner_address": "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
            "extra_coinbase_data": "nu7-us",
            "internal_miner": True,
        })

    def test_mining_section_omitted_when_unset(self):
        config = tomllib.loads(deploy.render_node_config(self.node()))

        self.assertNotIn("mining", config)

    def test_mempool_section_renders_the_pool_choice(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            mempool_implementation="core",
        )))

        self.assertEqual(config["mempool"], {"implementation": "core"})

    def test_tx_relay_renders_under_network_zakura(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            mempool_implementation="core",
            tx_relay=True,
            zakura={"listen_addr": "0.0.0.0:8234"},
        )))

        self.assertEqual(config["network"]["zakura"]["listen_addr"], "0.0.0.0:8234")
        self.assertEqual(config["network"]["zakura"]["tx_relay"], {"enabled": True})

    def test_tx_relay_omitted_when_off(self):
        config = tomllib.loads(deploy.render_node_config(self.node()))

        self.assertNotIn("zakura", config["network"])

    def test_mempool_section_omitted_when_unset(self):
        # Unset leaves zakurad's own default in charge, so a later release
        # that changes the default reaches the node.
        config = tomllib.loads(deploy.render_node_config(self.node()))

        self.assertNotIn("mempool", config)

    def test_health_renders_independently_of_metrics(self):
        config = tomllib.loads(deploy.render_node_config(self.node(
            health_listen_addr="127.0.0.1:8080",
        )))

        self.assertNotIn("metrics", config)
        self.assertEqual(config["health"], {"listen_addr": "127.0.0.1:8080"})


class TestnetParametersRenderingTests(NodeBuilder, unittest.TestCase):
    """A configured testnet such as the NU7 fork renders as one `network = { ... }` value."""

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

        self.assertEqual(config["network"]["network"], "Testnet")
        self.assertNotIn("testnet_parameters", config["network"])

    def test_configured_testnet_replaces_the_public_network_name(self):
        rendered = deploy.render_node_config(self.node(testnet_parameters=self.FORK_PARAMS))
        config = tomllib.loads(rendered)

        # zakurad rejects [network.testnet_parameters] beside `network = "Testnet"`,
        # because the public Testnet's parameters are fixed. The parameters must be
        # the `network` value itself.
        self.assertIsInstance(config["network"]["network"], dict)
        self.assertNotIn("testnet_parameters", config["network"])
        self.assertNotIn('network = "Testnet"', rendered)

    def test_parameters_on_a_non_testnet_node_are_refused(self):
        for network in ("Mainnet", "Regtest"):
            with self.assertRaises(deploy.DeployError, msg=network):
                deploy.render_node_config(
                    self.node(network=network, testnet_parameters=self.FORK_PARAMS)
                )

    def test_fork_parameters_round_trip(self):
        config = tomllib.loads(deploy.render_node_config(
            self.node(testnet_parameters=self.FORK_PARAMS)
        ))

        params = config["network"]["network"]
        self.assertEqual(params["network_name"], "Nu7Fork")
        self.assertEqual(params["network_magic"], [240, 13, 202, 254])
        self.assertEqual(params["initial_nsm_value_balance"], 55_768_414_957)
        # Genesis-only checkpoints would make the node verify 4M blocks from scratch.
        self.assertTrue(params["checkpoints"])

    def test_dotted_upgrade_names_are_quoted(self):
        config = tomllib.loads(deploy.render_node_config(
            self.node(testnet_parameters=self.FORK_PARAMS)
        ))

        heights = config["network"]["network"]["activation_heights"]
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

        disbursements = config["network"]["network"]["lockbox_disbursements"]
        self.assertEqual(disbursements, [{"address": "t2Lockbox", "amount": 0}])

    def test_nested_parameters_round_trip_exactly(self):
        params = {
            **self.FORK_PARAMS,
            "lockbox_disbursements": [
                {"address": "t2Lockbox1", "amount": 78_750_000_000_000},
                {"address": "t2Lockbox2", "amount": 0},
            ],
        }
        rendered = deploy.render_node_config(self.node(testnet_parameters=params))

        self.assertEqual(tomllib.loads(rendered)["network"]["network"], params)
        # One inline line: no [network.network] tables that could capture later keys.
        self.assertNotIn("[network.network", rendered)
        self.assertIn('"NU6.1" = 3536500', rendered)

    def test_the_fleet_config_round_trips_through_load_nodes(self):
        params = {**self.FORK_PARAMS,
                  "lockbox_disbursements": [{"address": "t2Lockbox", "amount": 1}]}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.toml"
            path.write_text(
                "[defaults]\n"
                'network = "Testnet"\n'
                f"testnet_parameters = {deploy.toml_scalar(params)}\n"
                '[[nodes]]\nname = "node-a"\nssh_string = "root@example"\ncommit = "main"\n'
            )
            node = deploy.load_nodes(path, None)[0]

        self.assertEqual(node.testnet_parameters, params)
        self.assertEqual(
            tomllib.loads(deploy.render_node_config(node))["network"]["network"], params
        )

    def test_tables_nested_in_an_array_of_tables_render_inline(self):
        streams = [{
            "height_range": {"start": 1, "end": 2},
            "recipients": [{"receiver": "Deferred", "numerator": 12, "addresses": []}],
        }]
        config = tomllib.loads(deploy.render_node_config(self.node(testnet_parameters={
            "network_name": "Nu7Fork",
            "funding_streams": streams,
        })))

        self.assertEqual(config["network"]["network"]["funding_streams"], streams)

    def test_strings_are_escaped(self):
        name = 'quote " backslash \\ newline \n'
        config = tomllib.loads(deploy.render_node_config(self.node(testnet_parameters={
            "network_name": name,
        })))

        self.assertEqual(config["network"]["network"]["network_name"], name)

    def test_a_null_value_is_refused(self):
        with self.assertRaises(deploy.DeployError):
            deploy.render_node_config(self.node(testnet_parameters={"network_name": None}))

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
        self.assertEqual(config["network"]["network"]["network_name"], "Nu7Fork")


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

    def test_mempool_implementation_is_a_known_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
                [defaults]
                mempool_implementation = "Core"

                [[nodes]]
                name = "node-a"
                ssh_string = "root@example"
                commit = "main"
            """.replace("                ", ""))

            nodes = deploy.load_nodes(path, None)

            self.assertEqual(nodes[0].mempool_implementation, "core")

    def test_unknown_mempool_implementation_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
                [[nodes]]
                name = "node-a"
                ssh_string = "root@example"
                commit = "main"
                mempool_implementation = "tx-relay"
            """.replace("                ", ""))

            with self.assertRaises(deploy.DeployError):
                deploy.load_nodes(path, None)

    def test_tx_relay_needs_the_core_pool_and_a_v2_stack(self):
        for extra in ('mempool_implementation = "classic"\np2p_stack = "dual"',
                      'mempool_implementation = "core"\np2p_stack = "legacy"',
                      'p2p_stack = "dual"'):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as tmp:
                path = self.write_config(tmp, (
                    '[[nodes]]\nname = "node-a"\nssh_string = "root@example"\n'
                    'commit = "main"\ntx_relay = true\n' + extra + "\n"
                ))

                with self.assertRaises(deploy.DeployError):
                    deploy.load_nodes(path, None)

    def test_tx_relay_on_a_core_dual_node_loads(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, (
                '[[nodes]]\nname = "node-a"\nssh_string = "root@example"\n'
                'commit = "main"\ntx_relay = true\nmempool_implementation = "core"\n'
                'p2p_stack = "dual"\n'
            ))

            self.assertTrue(deploy.load_nodes(path, None)[0].tx_relay)

    def test_testnet_parameters_is_a_known_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, """
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

            self.assertEqual(
                nodes[0].testnet_parameters["activation_heights"]["NU7"], 4_376_000
            )

    def test_setups_render_their_p2p_stack_pool_and_tx_relay(self):
        # The testnet deploy workflow's `setup` input writes this key next to
        # the fleet's `p2p_stack = "dual"`, which the setup overrides.
        for setup, stack, pool, tx_relay in (
            ("legacy", "legacy", "classic", False),
            ("v2", "dual", "core", True),
        ):
            with self.subTest(setup), tempfile.TemporaryDirectory() as tmp:
                path = self.write_config(tmp, f"""
                    [defaults]
                    network = "Testnet"
                    p2p_stack = "dual"
                    setup = "{setup}"

                    [defaults.zakura]
                    listen_addr = "0.0.0.0:8234"

                    [[nodes]]
                    name = "node-a"
                    ssh_string = "root@example"
                    commit = "main"
                """.replace("                    ", ""))

                node = deploy.load_nodes(path, None)[0]
                config = tomllib.loads(deploy.render_node_config(node))

                self.assertEqual(config["network"]["p2p_stack"], stack)
                self.assertEqual(config["mempool"], {"implementation": pool})
                self.assertEqual(config["network"]["zakura"]["tx_relay"], {"enabled": tx_relay})
                self.assertEqual(config["network"]["zakura"]["listen_addr"], "0.0.0.0:8234")

    def test_ci_overrides_render_a_canary_the_loader_accepts(self):
        # The testnet deploy workflow puts these lines in `[defaults]`.
        def overrides(**inputs):
            fields = dict(node="", p2p_stack="auto", fleet_p2p_stack="dual",
                          mempool_implementation="auto", tx_relay=False, setup="auto")
            return deploy.ci_overrides(**{**fields, **inputs})

        self.assertEqual(overrides(), ['p2p_stack = "dual"'])
        lines = overrides(node="node-a", mempool_implementation="core", tx_relay=True)
        with tempfile.TemporaryDirectory() as tmp:
            path = self.write_config(tmp, "\n".join([
                "[defaults]", 'network = "Testnet"', *lines, "",
                "[[nodes]]", 'name = "node-a"', 'ssh_string = "root@example"',
                'commit = "main"', "",
            ]))
            config = tomllib.loads(deploy.render_node_config(deploy.load_nodes(path, None)[0]))
        self.assertEqual(config["mempool"], {"implementation": "core"})
        self.assertEqual(config["network"]["zakura"]["tx_relay"], {"enabled": True})

        for inputs, error in (
            (dict(mempool_implementation="core"), "mempool_implementation overrides require one"),
            (dict(tx_relay=True), "tx_relay requires one"),
            (dict(p2p_stack="legacy"), "p2p_stack overrides require one"),
            (dict(setup="v2"), "setup overrides require one"),
            (dict(node="node-a", p2p_stack="legacy", setup="v2"), "not both"),
        ):
            with self.subTest(inputs), self.assertRaisesRegex(deploy.DeployError, error):
                overrides(**inputs)

    def test_internal_miner_requires_an_address_and_the_miner_build(self):
        for extra in ('build_features = ["internal-miner"]',
                      'miner_address = "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"'):
            with tempfile.TemporaryDirectory() as tmp:
                path = self.write_config(tmp, f"""
                    [[nodes]]
                    name = "node-a"
                    ssh_string = "root@example"
                    commit = "main"
                    internal_miner = true
                    {extra}
                """.replace("                    ", ""))

                with self.assertRaises(deploy.DeployError, msg=extra):
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




class SharedHostStagingTests(unittest.TestCase):
    """Two nodes on one host, deployed in parallel, must each install their own files."""

    NODES = """
        [defaults]
        network = "Testnet"
        initial_testnet_peers = []

        [[nodes]]
        name = "fork-1"
        ssh_string = "root@shared-host"
        commit = "main"
        listen_addr = "0.0.0.0:18233"

        [[nodes]]
        name = "fork-2"
        ssh_string = "root@shared-host"
        commit = "main"
        service_name = "zakurad-fork2"
        config_path = "/etc/zakura/zakura-fork2.toml"
        bin_path = "/usr/local/bin/zakurad-fork2"
        listen_addr = "0.0.0.0:18333"
    """.replace("        ", "")

    def deploy_in_parallel(self, fail_install_for=None):
        host_files: dict[str, bytes] = {}
        installed: dict[str, dict[str, str]] = {}
        stages: list[str] = []
        removed: list[str] = []
        lock = threading.Lock()
        # Both uploads finish before either install starts, as in the original race.
        uploaded = threading.Barrier(2, timeout=5)

        def fake_run(cmd, *, cwd=None, capture=False, check=True):
            if cmd[0] == "ssh" and cmd[-1].startswith("mktemp -d /tmp/zakurad-deploy."):
                with lock:
                    stage = f"/tmp/zakurad-deploy.{len(stages):08d}"
                    stages.append(stage)
                return mock.Mock(returncode=0, stdout=stage + "\n")
            if cmd[0] == "ssh" and cmd[-1].startswith("rm -rf -- "):
                with lock:
                    removed.append(cmd[-1].removeprefix("rm -rf -- "))
                    for path in [path for path in host_files
                                 if path.startswith(removed[-1] + "/")]:
                        del host_files[path]
                return mock.Mock(returncode=0, stdout="")
            if cmd[0] == "scp":
                local, remote = cmd[-2], cmd[-1].split(":", 1)[1]
                with lock:
                    host_files[remote] = Path(local).read_bytes()
                return mock.Mock(returncode=0, stdout="")
            raise AssertionError(f"unexpected command: {cmd}")

        def fake_install(node, script):
            if "sysctl --load" in script:
                stage = re.search(r"^STAGE=(\S+)$", script, re.MULTILINE)[1]
                with lock:
                    self.assertEqual(host_files[f"{stage}/zakura-network-sysctl.conf"],
                                     deploy.NETWORK_SYSCTL_CONF.read_bytes())
                return mock.Mock(returncode=0)
            uploaded.wait()
            stage = re.search(r"^STAGE=(\S+)$", script, re.MULTILINE)[1]
            config_path = re.search(r"^CONFIG_PATH=(\S+)$", script, re.MULTILINE)[1]
            with lock:
                installed[node.name] = {
                    "config_path": config_path,
                    "config": host_files[f"{stage}/zakura.toml"].decode(),
                    "unit": host_files[f"{stage}/zakurad.service"].decode(),
                    "binary": host_files[f"{stage}/zakurad.new"].decode(),
                }
            return mock.Mock(returncode=1 if node.name == fail_install_for else 0)

        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "nodes.toml"
            config.write_text(self.NODES)
            binary = Path(tmp) / "zakurad"
            binary.write_text("binary for main")
            args = types.SimpleNamespace(config=str(config), node=None, force=False,
                                         no_restart=False)
            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: tmp}), \
                    mock.patch.object(deploy, "build_nodes", side_effect=lambda nodes, force:
                                      [setattr(n, "sha", "a" * 40) for n in nodes]
                                      and {"a" * 40: binary}), \
                    mock.patch.object(deploy, "build_publishers", return_value={}), \
                    mock.patch.object(deploy, "run", side_effect=fake_run), \
                    mock.patch.object(deploy, "ssh_with_stdin", side_effect=fake_install):
                status = deploy.cmd_deploy(args)
            leftovers = [path.name for path in Path(tmp).iterdir()
                         if path.name.startswith((".cfg-", ".unit-"))]
        return status, installed, stages, removed, host_files, leftovers

    def test_each_node_installs_its_own_config_and_unit(self):
        status, installed, stages, _, _, _ = self.deploy_in_parallel()

        self.assertEqual(status, 0)
        self.assertEqual(len(set(stages)), 2)
        self.assertEqual(installed["fork-1"]["config_path"], "/etc/zakura/zakura.toml")
        self.assertIn('listen_addr = "0.0.0.0:18233"', installed["fork-1"]["config"])
        self.assertIn("Description=Zakura full node (zakurad)", installed["fork-1"]["unit"])
        self.assertEqual(installed["fork-2"]["config_path"], "/etc/zakura/zakura-fork2.toml")
        self.assertIn('listen_addr = "0.0.0.0:18333"', installed["fork-2"]["config"])
        self.assertIn("Description=Zakura full node (zakurad-fork2)", installed["fork-2"]["unit"])

    def test_staging_is_removed_even_when_an_install_fails(self):
        status, _, stages, removed, host_files, leftovers = self.deploy_in_parallel(
            fail_install_for="fork-2")

        self.assertEqual(status, 1)
        self.assertEqual(sorted(removed), sorted(stages))
        self.assertEqual(host_files, {})
        self.assertEqual(leftovers, [])

    def test_install_scripts_read_only_their_staging_directory(self):
        for script in (deploy.NETWORK_SYSCTL_SCRIPT, deploy.INSTALL_SCRIPT, deploy.PROCESS_INSTALL_SCRIPT,
                       deploy.BINARY_ONLY_INSTALL_SCRIPT,
                       deploy.DOCKER_BINARY_ONLY_INSTALL_SCRIPT):
            self.assertIn("STAGE={stage}", script)
            host_side = script.replace('"$CONTAINER:/tmp/zakurad-deploy.new"', "")
            host_side = host_side.split("docker exec --user 0", 1)[0]
            self.assertNotIn("/tmp/zakurad-deploy.", host_side)



if __name__ == "__main__":
    unittest.main()
