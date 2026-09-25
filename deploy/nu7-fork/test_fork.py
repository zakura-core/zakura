"""Tests for the fork fleet-config renderer.

Run with `python3 -m unittest test_fork` from `deploy/nu7-fork`.
"""

import importlib.util
import os
import subprocess
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import fork

# The rendered primary node config, loaded by zakurad's own config deserializer in
# the fork miner's `rendered_fork_config_loads_in_zakurad` test. Regenerate it with
# `ZAKURA_REGENERATE_FIXTURES=1 python3 -m unittest test_fork` after an intended change.
RENDERED_NODE_FIXTURE = Path(__file__).parent / "miner" / "testdata" / "fork-node.toml"


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

    def test_a_missing_miner_address_is_refused(self):
        config = base_config(PEER)
        config["miner"]["address"] = ""
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


class SshOptions(unittest.TestCase):
    def test_an_unresponsive_connected_host_is_dropped(self):
        # ConnectTimeout only bounds the handshake; keepalives bound every later wait.
        self.assertIn("ServerAliveInterval=30", fork.SSH_OPTS)
        self.assertIn("ServerAliveCountMax=4", fork.SSH_OPTS)


def load_remote_config_renderer():
    path = Path(__file__).parent / "miner" / "render-remote-config.py"
    spec = importlib.util.spec_from_file_location("render_remote_config", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RemoteMinerConfig(unittest.TestCase):
    PARAMETERS = (
        'network_name = "Nu7Fork"\n'
        'network_magic = [122, 107, 117, 55]\n'
        '[{table}.activation_heights]\n'
        'NU7 = 4382859\n'
    )

    def render(self, table: str) -> str:
        base = (
            '[network]\n'
            'initial_testnet_peers = ["127.0.0.1:18333"]\n'
            f'[{table}]\n' + self.PARAMETERS.format(table=table)
            + '[mining]\nminer_address = "tmOld"\n'
        )
        renderer = load_remote_config_renderer()
        return renderer.render(base, ["seed.example:18233"], "t" + "A" * 34)

    def test_reads_both_config_forms(self):
        # The running fork predates #1147's `network = { ... }` form; a new fork uses it.
        for table in ("network.testnet_parameters", "network.network"):
            rendered = self.render(table)
            self.assertIn('"seed.example:18233"', rendered, table)
            self.assertIn('miner_address = "t' + "A" * 34 + '"', rendered, table)


class RenderedNodeConfig(unittest.TestCase):
    """The deployer's output for a fork node must be a config zakurad accepts.

    tomllib only proves the output is TOML. The fixture is what the Rust side loads,
    so a shape zakurad rejects fails `cargo test -p zakura-fork-miner`.
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
            "ZAKURA_REGENERATE_FIXTURES=1 and run cargo test -p zakura-fork-miner",
        )


if __name__ == "__main__":
    unittest.main()
