"""Tests for the fork fleet-config renderer.

Run with `python3 -m unittest test_fork` from `deploy/nu7-fork`.
"""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import fork

# The rendered primary node config, loaded by zakura-network's own config deserializer
# in its `rendered_nu7_fork_config_loads` test. Regenerate it with
# `ZAKURA_REGENERATE_FIXTURES=1 python3 -m unittest test_fork` after an intended change.
RENDERED_NODE_FIXTURE = (Path(__file__).resolve().parents[2] / "crates" / "zakura-network"
                         / "src" / "config" / "tests" / "data" / "nu7-fork-node.toml")


def base_config(peer=None):
    """A config with the fields `render_nodes_toml` reads, and nothing else."""
    config = {
        "fork": {
            "network_name": "Nu7Fork",
            "network_magic": [0x7A, 0x6B, 0x75, 0x37],
            "activation_offset": 10,
        },
        "host": {
            "ssh_string": "root@localhost",
            "commit": "main",
            "fork_cache_dir": "/var/lib/zakura-fork",
            "listen_addr": "0.0.0.0:18233",
            "rpc_listen_addr": "127.0.0.1:18232",
            "metrics_endpoint": "127.0.0.1:9999",
            "storage_mode": "pruned",
        },
        "droplet": {"name": "zakura-nu7-fork-1"},
        "miner": {"address": "tmGkvoQGmvJu6H5Wp22wUFAsBuX6SPGHnMq"},
    }
    if peer is not None:
        config["peer"] = peer
    return config


PEER = {
    "enabled": True,
    "name": "zakura-nu7-fork-2",
    "service_name": "zakurad-fork2",
    "bin_path": "/usr/local/bin/zakurad-fork2",
    "config_path": "/etc/zakura/zakura-fork2.toml",
    "cache_dir": "/var/lib/zakura-fork2",
    "log_file": "/var/log/zakura/zakura-fork2.log",
    "identity_dir": "/root/.zakura-fork2",
    "listen_addr": "0.0.0.0:18333",
    "rpc_listen_addr": "127.0.0.1:18242",
    "metrics_endpoint": "127.0.0.1:9998",
    "miner_address": "",
}


def render(config):
    plan = fork.fork_plan(config, 4_400_000)
    return fork.render_nodes_toml(config, plan)


class PeerDialAddr(unittest.TestCase):
    """A wildcard listen address is not dialable; the loopback form is."""

    def test_wildcard_forms_become_loopback(self):
        for listen in ("0.0.0.0:18333", "[::]:18333"):
            self.assertEqual(fork.peer_dial_addr(listen), "127.0.0.1:18333")

    def test_a_concrete_address_is_left_alone(self):
        self.assertEqual(fork.peer_dial_addr("10.0.0.4:18333"), "10.0.0.4:18333")


class RenderedNodes(unittest.TestCase):
    def test_one_node_when_the_peer_is_absent(self):
        rendered = render(base_config())
        self.assertEqual(rendered.count("[[nodes]]"), 1)

    def test_one_node_when_the_peer_is_disabled(self):
        rendered = render(base_config({**PEER, "enabled": False}))
        self.assertEqual(rendered.count("[[nodes]]"), 1)

    def test_the_peer_adds_a_second_node_that_shares_nothing(self):
        rendered = render(base_config(PEER))
        self.assertEqual(rendered.count("[[nodes]]"), 2)

        # Everything the two nodes would otherwise collide on must be overridden:
        # sharing a unit, binary, config, state directory, port or node identity
        # makes the second node fail to start or corrupt the first one's state.
        for key in (
            'service_name = "zakurad-fork2"',
            'bin_path = "/usr/local/bin/zakurad-fork2"',
            'config_path = "/etc/zakura/zakura-fork2.toml"',
            'state_cache_dir = "/var/lib/zakura-fork2"',
            'identity_dir = "/root/.zakura-fork2"',
            'listen_addr = "0.0.0.0:18333"',
            'rpc_listen_addr = "127.0.0.1:18242"',
            'metrics_endpoint = "127.0.0.1:9998"',
        ):
            self.assertIn(key, rendered, key)

    def test_the_nodes_dial_each_other(self):
        rendered = render(base_config(PEER))
        self.assertIn('initial_testnet_peers = ["127.0.0.1:18333"]', rendered)
        self.assertIn('initial_testnet_peers = ["127.0.0.1:18233"]', rendered)

    def test_an_empty_peer_address_leaves_it_a_pure_validator(self):
        rendered = render(base_config(PEER))
        self.assertIn('miner_address = ""', rendered)

    def test_a_peer_address_makes_both_nodes_mine(self):
        rendered = render(base_config({**PEER, "miner_address": "tmA1eXkKigWig8xmeDBJdLGPiyhrmKcTxnq"}))
        self.assertIn('miner_address = "tmA1eXkKigWig8xmeDBJdLGPiyhrmKcTxnq"', rendered)
        self.assertIn('miner_address = "tmGkvoQGmvJu6H5Wp22wUFAsBuX6SPGHnMq"', rendered)

    def test_the_primary_runs_the_internal_miner_and_the_observer_does_not(self):
        nodes = __import__("tomllib").loads(render(base_config(PEER)))
        self.assertEqual(nodes["defaults"]["build_features"], ["internal-miner"])
        primary, observer = nodes["nodes"]
        self.assertIs(primary["internal_miner"], True)
        self.assertEqual(primary["extra_coinbase_data"], "zakura-nu7-fork-1")
        self.assertNotIn("internal_miner", observer)

    def test_a_mining_peer_gets_its_own_coinbase_data(self):
        peer = {**PEER, "miner_address": "tmA1eXkKigWig8xmeDBJdLGPiyhrmKcTxnq"}
        _, observer = __import__("tomllib").loads(render(base_config(peer)))["nodes"]
        self.assertIs(observer["internal_miner"], True)
        self.assertEqual(observer["extra_coinbase_data"], "zakura-nu7-fork-2")

    def test_the_primary_funds_a_separate_faucet_address(self):
        config = base_config(PEER)
        config["faucet"] = {"address": "tmFaucetAddressXXXXXXXXXXXXXXXXXXXX"}
        config["remote"] = [{"name": "eu", "ssh_string": "root@203.0.113.7",
                             "initial_testnet_peers": ["seed.example:18233"]}]
        nodes = __import__("tomllib").loads(render(config))
        primary, observer, remote = nodes["nodes"]
        self.assertEqual(nodes["defaults"]["miner_address"], "tmGkvoQGmvJu6H5Wp22wUFAsBuX6SPGHnMq")
        self.assertEqual(primary["miner_address"], "tmFaucetAddressXXXXXXXXXXXXXXXXXXXX")
        self.assertNotIn("miner_address", remote)
        self.assertEqual(observer["miner_address"], "")

    def test_the_faucet_may_not_share_the_miner_address(self):
        config = base_config(PEER)
        config["faucet"] = {"address": config["miner"]["address"]}
        with self.assertRaises(fork.ForkError):
            render(config)

    def test_a_missing_miner_address_is_refused(self):
        config = base_config(PEER)
        config["miner"]["address"] = ""
        with self.assertRaises(fork.ForkError):
            render(config)


class ActivationHeights(unittest.TestCase):
    def test_only_nu7_is_configured_and_the_public_heights_are_inherited(self):
        nodes = __import__("tomllib").loads(render(base_config()))
        params = nodes["defaults"]["testnet_parameters"]
        self.assertIs(params["inherit_activation_heights"], True)
        self.assertEqual(params["activation_heights"], {"NU7": 4_400_010})

    def test_the_heights_are_not_parsed_from_rust_sources(self):
        self.assertFalse(hasattr(fork, "testnet_activation_heights"))
        self.assertNotIn("parameters/constants.rs", Path(fork.__file__).read_text())

    def test_activation_must_follow_the_seed_tip(self):
        config = base_config()
        config["fork"]["activation_offset"] = 0
        with self.assertRaises(fork.ForkError):
            render(config)


class Provision(unittest.TestCase):
    DROPLET = {
        "name": "zakura-nu7-fork-1",
        "size": "c-8",
        "regions": "nyc1",
        "tag": "zakura-nu7-fork",
        "volume_name": "zakura-pr-nu7-fork-state",
    }

    def provision(self, fingerprint, plan):
        config = {"droplet": {**self.DROPLET, "ssh_fingerprint": fingerprint}}
        done = subprocess.CompletedProcess([], 0, "", "")
        with mock.patch.object(fork, "run", return_value=done) as run:
            fork.cmd_provision(config, types.SimpleNamespace(plan=plan))
        return run.call_args.args[0]

    def test_a_missing_fingerprint_is_refused_before_provisioning(self):
        # do_provision.py refuses to create a host without a key, and would only
        # say so after the DigitalOcean catalog lookups.
        with mock.patch.object(fork, "run") as run:
            with self.assertRaises(fork.ForkError):
                fork.cmd_provision({"droplet": {**self.DROPLET, "ssh_fingerprint": ""}},
                                   types.SimpleNamespace(plan=False))
        run.assert_not_called()

    def test_plan_runs_without_a_fingerprint(self):
        self.assertIn("--plan", self.provision("", plan=True))

    def test_the_fingerprint_is_passed_through(self):
        cmd = self.provision("aa:bb", plan=False)
        self.assertEqual(cmd[cmd.index("--ssh-fingerprint") + 1], "aa:bb")


class Deploy(unittest.TestCase):
    def test_the_fleet_deploys_in_one_parallel_deployer_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            nodes = Path(tmp) / "nodes.toml"
            nodes.write_text(render(base_config(PEER)))
            done = subprocess.CompletedProcess([], 0, "", "")
            with mock.patch.object(fork, "run", return_value=done) as run:
                fork.cmd_deploy({}, types.SimpleNamespace(nodes=nodes))
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0][2:], ["deploy", "--config", str(nodes)])
        self.assertNotIn("--node", commands[0])


class SshOptions(unittest.TestCase):
    def test_an_unresponsive_connected_host_is_dropped(self):
        # ConnectTimeout only bounds the handshake; keepalives bound every later wait.
        self.assertIn("ServerAliveInterval=30", fork.SSH_OPTS)
        self.assertIn("ServerAliveCountMax=4", fork.SSH_OPTS)


REMOTE = {
    "name": "zakura-nu7-miner-eu",
    "ssh_string": "root@203.0.113.7",
    "initial_testnet_peers": ["seed.example:18233", "203.0.113.8:18233"],
}


class RemoteMiners(unittest.TestCase):
    def nodes(self, *remotes):
        config = base_config(PEER)
        config["remote"] = list(remotes)
        return __import__("tomllib").loads(render(config))["nodes"]

    def test_a_remote_miner_is_a_deployer_node_with_its_own_peers_and_tag(self):
        remote = self.nodes(REMOTE)[-1]
        self.assertEqual(remote, {
            "name": "zakura-nu7-miner-eu",
            "ssh_string": "root@203.0.113.7",
            "commit": "main",
            "initial_testnet_peers": ["seed.example:18233", "203.0.113.8:18233"],
            "internal_miner": True,
            "extra_coinbase_data": "zakura-nu7-miner-eu",
            "monitor": {"status_url": "http://203.0.113.7:8094/v1/miner",
                        "id": "zakura-nu7-miner-eu", "region": ""},
        })

    def test_a_remote_miner_installs_the_deployers_standard_unit(self):
        fork.sys.path.insert(0, str(fork.DEPLOYER.parent))
        import deploy  # noqa: E402  (path is set immediately above)

        config = base_config(PEER)
        config["remote"] = [REMOTE]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.toml"
            path.write_text(render(config))
            node = deploy.load_nodes(path, ["zakura-nu7-miner-eu"])[0]
        unit = deploy.render_service(node)
        self.assertIn("ExecStart=/usr/local/bin/zakurad -c /etc/zakura/zakura.toml start", unit)
        rendered = __import__("tomllib").loads(deploy.render_node_config(node))
        self.assertEqual(rendered["mining"]["miner_address"], "tmGkvoQGmvJu6H5Wp22wUFAsBuX6SPGHnMq")
        self.assertEqual(rendered["network"]["network"]["activation_heights"], {"NU7": 4_400_010})
        self.assertFalse((Path(fork.__file__).parent / "miner" / "zakurad.service").exists())

    def test_the_status_collector_reads_the_same_fleet_config(self):
        spec = importlib.util.spec_from_file_location(
            "cluster_status", fork.REPO_ROOT / "deploy" / "runner" / "zakura-cluster-status.py")
        status = importlib.util.module_from_spec(spec)
        # The collector's dataclasses resolve annotations through sys.modules.
        sys.modules[spec.name] = status
        self.addCleanup(sys.modules.pop, spec.name)
        spec.loader.exec_module(status)
        config = base_config(PEER)
        config["remote"] = [{**REMOTE, "id": "eu", "region": "Amsterdam, NL"}]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.toml"
            path.write_text(render(config))
            primary, observer, remote = status.load_nodes(path)
        self.assertTrue(primary.local and primary.internal_miner)
        self.assertEqual(primary.label, "primary")
        self.assertTrue(observer.local and not observer.internal_miner)
        self.assertEqual(remote.status_url, "http://203.0.113.7:8094/v1/miner")
        self.assertEqual((remote.miner_id, remote.region), ("eu", "Amsterdam, NL"))
        self.assertTrue(remote.internal_miner)

    def test_a_remote_miner_without_peers_is_refused(self):
        for broken in ({**REMOTE, "initial_testnet_peers": []},
                       {**REMOTE, "initial_testnet_peers": ["no port"]},
                       {**REMOTE, "ssh_string": ""}):
            with self.assertRaises(fork.ForkError):
                self.nodes(broken)


class FakeHost:
    """Records remote commands and answers the few whose output fork.py reads."""

    def __init__(self, seed_tip=None):
        self.commands = []
        self.seed_tip = seed_tip

    def ssh(self, host, *remote, capture=False, check=True):
        command = " ".join(remote)
        self.commands.append(command)
        stdout, code = "", 0
        if command.startswith("ls -d"):
            stdout = "/var/lib/zakura-pristine/state/v29/testnet\n"
        elif command.startswith("cat ") and command.endswith(fork.SEED_TIP_FILE):
            if self.seed_tip is None:
                code = 1
            else:
                stdout = json.dumps(self.seed_tip)
        return subprocess.CompletedProcess([], code, stdout, "")


class CaughtUpSeed(unittest.TestCase):
    TIP = {"height": 4_390_000, "hash": "ab" * 32, "time": 1_790_000_000}

    def config(self):
        config = base_config(PEER)
        config["host"]["pristine_cache_dir"] = "/var/lib/zakura-pristine"
        config["host"]["snapshot_mount"] = "/mnt/snapshots"
        config["droplet"]["volume_name"] = "zakura-pr-nu7-fork-state"
        return config

    def seed(self, host):
        with mock.patch.object(fork, "ssh", side_effect=host.ssh), \
                mock.patch.object(fork, "db_format_version", return_value=29):
            fork.cmd_seed(self.config(), types.SimpleNamespace(force=False))
        return host.commands

    def test_the_recorded_tip_is_the_seed_tip(self):
        host = FakeHost(self.TIP)
        with mock.patch.object(fork, "ssh", side_effect=host.ssh):
            self.assertEqual(fork.seeded_tip_height(self.config()), 4_390_000)
        # The finalized-only `tip-height` is not consulted.
        self.assertFalse(any("tip-height" in command for command in host.commands))

    def test_a_caught_up_seed_copies_the_non_finalized_backup(self):
        commands = self.seed(FakeHost(self.TIP))
        copies = [c for c in commands if "cp -a" in c and "non_finalized_state" in c]
        self.assertEqual(len(copies), 2)
        self.assertIn("/var/lib/zakura-fork/non_finalized_state/nu7fork", copies[0])
        self.assertIn("/var/lib/zakura-fork2/non_finalized_state/nu7fork", copies[1])

    def test_without_catch_up_only_the_finalized_state_is_seeded(self):
        # The finalized tip is what the activation height is computed from, so
        # non-finalized blocks above it must not be seeded.
        commands = self.seed(FakeHost(None))
        self.assertFalse(any("cp -a" in c and "non_finalized_state" in c for c in commands))


class CatchUp(unittest.TestCase):
    def run_catch_up(self, tip_times):
        host = FakeHost()
        now = 1_790_000_000
        answers = iter(tip_times)

        def fake_rpc(host_name, addr, method, params=None):
            self.assertEqual(addr, "127.0.0.1:18252")
            if method == "getblockchaininfo":
                self.block_time = next(answers)
                return {"blocks": 4_390_000, "bestblockhash": "cd" * 32}
            return {"time": self.block_time}

        config = CaughtUpSeed().config()
        with mock.patch.object(fork, "ssh", side_effect=host.ssh), \
                mock.patch.object(fork, "rpc", side_effect=fake_rpc), \
                mock.patch.object(fork.time, "time", return_value=now), \
                mock.patch.object(fork.time, "sleep"):
            fork.cmd_catch_up(config, types.SimpleNamespace())
        return host.commands

    def test_waits_for_a_fresh_tip_then_stops_before_recording_it(self):
        now = 1_790_000_000
        commands = self.run_catch_up([now - 6 * 3600, now - 300])

        self.assertTrue(commands[0].startswith("rm -f /var/lib/zakura-pristine/seed-tip.json"))
        started = next(i for i, c in enumerate(commands) if c.startswith("systemctl reset-failed"))
        stopped = next(i for i, c in enumerate(commands) if c.startswith("systemctl stop"))
        recorded = next(i for i, c in enumerate(commands) if "seed-tip.json <<" in c)
        self.assertLess(started, stopped)
        # The tip is only written once the node has stopped and flushed its state.
        self.assertLess(stopped, recorded)
        self.assertIn('"height": 4390000', commands[recorded])
        self.assertIn('"time": 1789999700', commands[recorded])

    def test_a_node_that_never_catches_up_is_stopped_and_nothing_is_recorded(self):
        host = FakeHost()
        config = CaughtUpSeed().config()
        config["catch_up"] = {"timeout_minutes": 0}
        with mock.patch.object(fork, "ssh", side_effect=host.ssh), \
                mock.patch.object(fork, "rpc", return_value={"blocks": 1, "bestblockhash": "x",
                                                             "time": 0}), \
                mock.patch.object(fork.time, "sleep"):
            with self.assertRaises(fork.ForkError):
                fork.cmd_catch_up(config, types.SimpleNamespace())
        self.assertTrue(any(c.startswith("systemctl stop") for c in host.commands))
        self.assertFalse(any("seed-tip.json <<" in c for c in host.commands))

    def test_the_temporary_node_runs_the_public_testnet_over_the_pristine_cache(self):
        text = fork.catch_up_config(CaughtUpSeed().config(), fork.CATCH_UP_DEFAULTS)
        config = __import__("tomllib").loads(text)
        self.assertEqual(config["network"]["network"], "Testnet")
        self.assertNotIn("initial_testnet_peers", config["network"])
        self.assertEqual(config["state"]["cache_dir"], "/var/lib/zakura-pristine")
        self.assertEqual(config["state"]["storage_mode"], "pruned")
        self.assertEqual(config["rpc"]["listen_addr"], "127.0.0.1:18252")


class RenderedNodeConfig(unittest.TestCase):
    """The deployer's output for a fork node must be a config zakurad accepts.

    tomllib only proves the output is TOML. The fixture is what the Rust side loads,
    so a shape zakurad rejects fails `cargo test -p zakura-network rendered_nu7_fork`.
    """

    def render_primary(self) -> str:
        fork.sys.path.insert(0, str(fork.DEPLOYER.parent))
        import deploy  # noqa: E402  (path is set immediately above)

        with tempfile.TemporaryDirectory() as tmp:
            nodes_path = Path(tmp) / "nodes.toml"
            nodes_path.write_text(render(base_config(PEER)))
            nodes = deploy.load_nodes(nodes_path, None)
        return deploy.render_node_config(nodes[0])

    def test_matches_the_fixture_the_rust_test_loads(self):
        rendered = self.render_primary()
        if os.environ.get("ZAKURA_REGENERATE_FIXTURES"):
            RENDERED_NODE_FIXTURE.parent.mkdir(parents=True, exist_ok=True)
            RENDERED_NODE_FIXTURE.write_text(rendered)
        self.assertEqual(
            rendered, RENDERED_NODE_FIXTURE.read_text(),
            "the rendered fork config changed; regenerate the fixture with "
            "ZAKURA_REGENERATE_FIXTURES=1 and run cargo test -p zakura-network",
        )


if __name__ == "__main__":
    unittest.main()
