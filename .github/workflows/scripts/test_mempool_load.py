#!/usr/bin/env python3
"""Unit tests for the mempool-load harness.

Run directly: python3 .github/workflows/scripts/test_mempool_load.py

Loads the scripts by path because they are hyphenated and therefore not
importable as modules, matching deploy/deployer/test_deploy.py and
deploy/continuous-sync/tests/test_continuous_sync.py.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    # Register before exec so anything resolving a class's module through
    # sys.modules (dataclasses, pickle) works on Python 3.12+.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


lab = load_module("mempool_load_lab", SCRIPTS / "mempool-load-lab.py")
monitor = load_module("mempool_load_monitor", SCRIPTS / "mempool-load-monitor.py")
compare = load_module("mempool_load_compare", SCRIPTS / "mempool-load-compare.py")


# A trimmed copy of what `kresko genesis` actually renders, keeping the
# structural features that broke a naive rewriter in the first real run:
#   - `cache_dir` in BOTH [network] (peer cache) and [state] (RocksDB)
#   - `listen_addr` in [network], [rpc], AND [network.zakura]
#   - no internal_miner and no [metrics] section at all
#   - multi-line public seed-peer arrays
GENERATED_CONFIG = """\
[mempool]
debug_enable_at_height = 0

[mining]
miner_address = "tmExampleAddress"

[network]
cache_dir = "/root/.cache/zebra-peers"
identity_dir = "/root/.zakura"
initial_mainnet_peers = [
    "dnsseed.z.cash:8233",
    "mainnet.seeder.zfnd.org:8233",
]
initial_testnet_peers = [
    "127.0.0.1:18233",
    "127.0.0.3:18233",
]
listen_addr = "0.0.0.0:18233"
network = "Testnet"
p2p_stack = "default"

[network.testnet_parameters]
checkpoints = "/root/payload/local_genesis/checkpoints.txt"
disable_pow = true

[network.zakura]
bootstrap_peers = [
    "abc@165.22.54.66:8234",
    "def@104.131.184.123:8234",
]
listen_addr = "0.0.0.0:8234"

[rpc]
cookie_dir = "/root/.cache/zakura"
enable_cookie_auth = false
listen_addr = "0.0.0.0:18232"

[state]
cache_dir = "/root/.cache/zebra"
ephemeral = false
"""


def rewrite_for_node(index: int = 1, *, miner: bool = False) -> str:
    """Apply the same rewrites prepare_node_dirs does, for one node."""
    ip = lab.node_ip(index)
    text = lab.set_toml_values(
        GENERATED_CONFIG,
        {
            "network.cache_dir": f'"/lab/miner-{index}/peer-cache"',
            "network.identity_dir": f'"/lab/miner-{index}/identity"',
            "network.listen_addr": f'"{ip}:{lab.P2P_PORT}"',
            "state.cache_dir": f'"/lab/miner-{index}/state"',
            "rpc.cookie_dir": f'"/lab/miner-{index}/cookie"',
            "rpc.listen_addr": f'"{ip}:{lab.RPC_PORT}"',
            "network.testnet_parameters.checkpoints": '"/lab/checkpoints.txt"',
            "network.zakura.listen_addr": f'"{ip}:{lab.ZAKURA_P2P_PORT}"',
        },
    )
    text = lab.set_toml_values(
        text,
        {
            "metrics.endpoint_addr": f'"{ip}:{lab.METRICS_PORT}"',
            "mining.internal_miner": str(miner).lower(),
        },
        insert_missing=True,
    )
    return lab.clear_public_peers(text)


class NodeAddressing(unittest.TestCase):
    def test_node_ips_are_distinct_loopback_addresses(self):
        ips = [lab.node_ip(i) for i in range(4)]
        self.assertEqual(ips, ["127.0.0.101", "127.0.0.102", "127.0.0.103", "127.0.0.104"])
        self.assertEqual(len(set(ips)), 4)

    def test_nodes_avoid_the_default_loopback_address(self):
        # A real run found a pre-existing node's docker-proxy on 127.0.0.1;
        # the lab must coexist with it rather than collide.
        self.assertNotIn("127.0.0.1", [lab.node_ip(i) for i in range(8)])

    def test_node_ip_rejects_out_of_range_index(self):
        with self.assertRaises(ValueError):
            lab.node_ip(254)

    def test_monitor_and_lab_agree_on_addressing(self):
        # The monitor derives node addresses independently; a drift between
        # the two would silently sample the wrong (or no) nodes.
        self.assertEqual(monitor.NODE_IP_BASE, lab.NODE_IP_BASE)
        args = type(
            "Args",
            (),
            {
                "node_count": 3,
                "rpc_port": lab.RPC_PORT,
                "metrics_port": lab.METRICS_PORT,
                "lab_dir": "/lab",
            },
        )()
        for i, node in enumerate(monitor.build_nodes(args)):
            self.assertIn(lab.node_ip(i), node["rpc_url"])
            self.assertIn(lab.node_ip(i), node["metrics_url"])
            self.assertEqual(node["name"], lab.node_name(i))

    def test_node_name_survives_kresko_parsed_hostname(self):
        # Kresko's Instance::parsed_hostname() keeps the first two dash-parts,
        # so our names must be stable under that transform or the payload dir
        # it writes will not match the one we read.
        for i in range(4):
            name = lab.node_name(i)
            self.assertEqual("-".join(name.split("-")[:2]), name)


class PortSafety(unittest.TestCase):
    """The lab must never bind on top of, or talk to, someone else's node."""

    # These stub port_owner rather than binding real sockets: a running lab (or
    # anything else on the box) would otherwise make the result depend on
    # ambient state.
    def with_busy_ports(self, busy: set[tuple[str, int]]):
        original = lab.port_owner
        lab.port_owner = lambda host, port: (host, port) in busy
        self.addCleanup(lambda: setattr(lab, "port_owner", original))

    def test_occupied_port_is_reported_with_its_node_and_role(self):
        self.with_busy_ports({(lab.node_ip(0), lab.RPC_PORT)})
        with self.assertRaises(RuntimeError) as ctx:
            lab.assert_ports_free(2)
        message = str(ctx.exception)
        self.assertIn(f"{lab.node_ip(0)}:{lab.RPC_PORT}", message)
        self.assertIn("rpc", message)
        self.assertIn("miner-0", message)

    def test_every_conflicting_port_is_listed_at_once(self):
        self.with_busy_ports(
            {
                (lab.node_ip(0), lab.RPC_PORT),
                (lab.node_ip(1), lab.METRICS_PORT),
            }
        )
        with self.assertRaises(RuntimeError) as ctx:
            lab.assert_ports_free(2)
        message = str(ctx.exception)
        self.assertIn("miner-0", message)
        self.assertIn("miner-1", message)
        self.assertIn("metrics", message)

    def test_free_ports_pass_preflight(self):
        self.with_busy_ports(set())
        lab.assert_ports_free(4)

    def test_preflight_checks_every_port_of_every_node(self):
        checked: list[tuple[str, int]] = []
        original = lab.port_owner
        lab.port_owner = lambda host, port: (checked.append((host, port)), False)[1]
        self.addCleanup(lambda: setattr(lab, "port_owner", original))
        lab.assert_ports_free(3)
        for i in range(3):
            for port in (lab.P2P_PORT, lab.RPC_PORT, lab.METRICS_PORT, lab.ZAKURA_P2P_PORT):
                self.assertIn((lab.node_ip(i), port), checked)

    def test_port_owner_detects_a_real_listener(self):
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            sock.listen(1)
            _, port = sock.getsockname()
            self.assertTrue(lab.port_owner("127.0.0.1", port))
        # Once closed, the same port is free again.
        self.assertFalse(lab.port_owner("127.0.0.1", port))

    def test_wait_for_rpc_gives_up_when_our_node_died(self):
        # Without this, a dead node plus a stranger holding the port means we
        # submit blocks to the wrong chain -- which is exactly what happened.
        class DeadProc:
            returncode = 1

            def poll(self):
                return 1

        started = __import__("time").monotonic()
        self.assertFalse(
            lab.wait_for_rpc("http://127.0.0.1:1", timeout_secs=30, proc=DeadProc())
        )
        # It must fail fast on the process check, not burn the full timeout.
        self.assertLess(__import__("time").monotonic() - started, 5)


class Teardown(unittest.TestCase):
    """Nothing this harness starts may outlive a `down`."""

    def write_pidfile(self, lab_dir: Path, pids: dict) -> Path:
        lab_dir.mkdir(parents=True, exist_ok=True)
        path = lab_dir / "pids.json"
        path.write_text(json.dumps(pids))
        return path

    def test_a_surviving_node_is_kept_in_the_pidfile(self):
        # If a node ignores every signal, `down` must leave a record of it --
        # otherwise the next `up` fails the port preflight with nothing to
        # clean up from.
        import tempfile

        original = lab.stop_pid
        lab.stop_pid = lambda pid, name: pid != 999001
        self.addCleanup(lambda: setattr(lab, "stop_pid", original))
        with tempfile.TemporaryDirectory() as tmpdir:
            lab_dir = Path(tmpdir) / "lab"
            pidfile = self.write_pidfile(lab_dir, {"miner-0": 999001, "miner-1": 999002})
            args = type("Args", (), {"lab_dir": str(lab_dir)})()
            self.assertEqual(lab.cmd_down(args), 1)
            self.assertTrue(pidfile.is_file())
            self.assertEqual(json.loads(pidfile.read_text()), {"miner-0": 999001})

    def test_pidfile_is_removed_only_when_everything_stopped(self):
        import tempfile

        original = lab.stop_pid
        lab.stop_pid = lambda pid, name: True
        self.addCleanup(lambda: setattr(lab, "stop_pid", original))
        with tempfile.TemporaryDirectory() as tmpdir:
            lab_dir = Path(tmpdir) / "lab"
            pidfile = self.write_pidfile(lab_dir, {"miner-0": 999001})
            args = type("Args", (), {"lab_dir": str(lab_dir)})()
            self.assertEqual(lab.cmd_down(args), 0)
            self.assertFalse(pidfile.exists())

    def test_stop_pid_reports_an_already_dead_process_as_stopped(self):
        # PID 0 is never a real target; ProcessLookupError is the "already
        # gone" path and must count as success, not a leak.
        original_kill = os.kill

        def fake_kill(pid, sig):
            raise ProcessLookupError()

        os.kill = fake_kill
        self.addCleanup(lambda: setattr(os, "kill", original_kill))
        self.assertTrue(lab.stop_pid(999003, "miner-0"))

    def test_blaster_is_stopped_via_its_process_group(self):
        # The orphan bug: signalling only the Python wrapper leaves kresko
        # running and submitting into the next A/B leg.
        killed = []

        class FakeProc:
            pid = 4242

            def __init__(self):
                self.calls = 0

            def poll(self):
                # Alive for the first check, gone after the first signal.
                return None if self.calls == 0 else 0

            def wait(self, timeout=None):
                self.calls += 1
                return 0

        original_killpg, original_getpgid = os.killpg, os.getpgid
        os.killpg = lambda pgid, sig: killed.append((pgid, sig))
        os.getpgid = lambda pid: pid
        self.addCleanup(lambda: (setattr(os, "killpg", original_killpg),
                                 setattr(os, "getpgid", original_getpgid)))
        lab.stop_blast(FakeProc())
        self.assertEqual(len(killed), 1)
        self.assertEqual(killed[0], (4242, __import__("signal").SIGINT))

    def test_stop_blast_is_a_noop_for_an_exited_blaster(self):
        killed = []
        original_killpg = os.killpg
        os.killpg = lambda pgid, sig: killed.append(sig)
        self.addCleanup(lambda: setattr(os, "killpg", original_killpg))

        class Dead:
            pid = 1

            def poll(self):
                return 0

        lab.stop_blast(Dead())
        self.assertEqual(killed, [])


class PeeringCheck(unittest.TestCase):
    """A network that never peers measures nothing, so `up` must catch it."""

    def with_peer_counts(self, sequence):
        """Stub rpc_call to return successive peer lists per node."""
        calls = {"n": 0}

        def fake(url, method, params=None, timeout=15):
            self.assertEqual(method, "getpeerinfo")
            result = sequence[min(calls["n"], len(sequence) - 1)]
            calls["n"] += 1
            return result

        original = lab.rpc_call
        lab.rpc_call = fake
        self.addCleanup(lambda: setattr(lab, "rpc_call", original))

    def test_peered_nodes_pass_immediately(self):
        self.with_peer_counts([[{"addr": "x"}]])
        self.assertTrue(lab.wait_for_peers(3, timeout_secs=5, poll_secs=0.01))

    def test_asymmetric_peer_counts_are_accepted(self):
        # A healthy 3-node lab really does report 2/0/1: getpeerinfo lists a
        # connection from the dialling side only. Grading per node would
        # reject a fully connected network.
        self.with_peer_counts([[{"a": 1}, {"a": 2}], [], [{"a": 3}]])
        self.assertTrue(lab.wait_for_peers(3, timeout_secs=5, poll_secs=0.01))

    def test_isolated_nodes_are_reported(self):
        # The exact bug a real run hit: a stale peer list produced three live,
        # mining, transaction-accepting nodes that never found each other.
        self.with_peer_counts([[]])
        self.assertFalse(lab.wait_for_peers(3, timeout_secs=0.5, poll_secs=0.01))

    def test_rpc_failure_counts_as_unpeered(self):
        def boom(url, method, params=None, timeout=15):
            raise RuntimeError("connection refused")

        original = lab.rpc_call
        lab.rpc_call = boom
        self.addCleanup(lambda: setattr(lab, "rpc_call", original))
        self.assertFalse(lab.wait_for_peers(2, timeout_secs=0.5, poll_secs=0.01))


class NodeEnvironment(unittest.TestCase):
    """zakurad reads ZAKURA_* env vars as config keys."""

    def test_zakura_prefixed_vars_are_dropped(self):
        # A real failure: exporting ZAKURA_DIR made the node reject its config
        # with "unknown field `dir`" -- the env var, not the file, was the
        # problem, which is a genuinely confusing thing to debug.
        os.environ["ZAKURA_DIR"] = "/tmp/should-not-reach-the-node"
        self.addCleanup(lambda: os.environ.pop("ZAKURA_DIR", None))
        env = lab.node_env()
        self.assertNotIn("ZAKURA_DIR", env)

    def test_unrelated_vars_are_preserved(self):
        os.environ["MEMPOOL_LOAD_PROBE"] = "keep-me"
        self.addCleanup(lambda: os.environ.pop("MEMPOOL_LOAD_PROBE", None))
        env = lab.node_env()
        self.assertEqual(env.get("MEMPOOL_LOAD_PROBE"), "keep-me")
        self.assertIn("PATH", env)


class StartupConvergence(unittest.TestCase):
    """Peer counts are too weak a gate; a mined block must actually arrive."""

    def with_heights(self, rounds):
        """Stub rpc_call to walk through successive per-round height maps."""
        state = {"round": 0, "i": 0}

        def fake(url, method, params=None, timeout=15):
            r = rounds[min(state["round"], len(rounds) - 1)]
            value = r[state["i"] % len(r)]
            state["i"] += 1
            if state["i"] % len(r) == 0:
                state["round"] += 1
            return {"blocks": value}

        original = lab.rpc_call
        lab.rpc_call = fake
        self.addCleanup(lambda: setattr(lab, "rpc_call", original))

    def test_agreement_without_progress_is_not_convergence(self):
        # All nodes parked at the seed height agree trivially. That says
        # nothing about whether blocks propagate.
        self.with_heights([[128, 128, 128]])
        self.assertFalse(
            lab.wait_for_chain_convergence(3, timeout_secs=0.5, poll_secs=0.01)
        )

    def test_a_block_reaching_every_node_converges(self):
        self.with_heights([[128, 128, 128], [130, 130, 130]])
        self.assertTrue(
            lab.wait_for_chain_convergence(3, timeout_secs=5, poll_secs=0.01)
        )

    def test_a_node_left_behind_is_rejected(self):
        # The observed failure: miner-1 stuck while the others advanced.
        self.with_heights([[128, 128, 128], [140, 135, 140]])
        self.assertFalse(
            lab.wait_for_chain_convergence(3, timeout_secs=0.5, poll_secs=0.01)
        )

    def test_one_block_of_spread_is_tolerated(self):
        self.with_heights([[128, 128, 128], [131, 130, 131]])
        self.assertTrue(
            lab.wait_for_chain_convergence(3, timeout_secs=5, poll_secs=0.01)
        )


class StartupDiagnostics(unittest.TestCase):
    """An opaque `unknown field` error must name its actual cause."""

    def write_log(self, tmpdir, body):
        path = Path(tmpdir) / "bootstrap.log"
        path.write_text(body)
        return path

    def test_unknown_field_is_explained_as_version_skew(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            log = self.write_log(
                tmpdir,
                "error: Configuration error: unknown field `expose_peer_addresses`, "
                "expected one of `listen_addr`, `network`\n",
            )
            hint = lab.explain_startup_failure(log)
        self.assertIn("expose_peer_addresses", hint)
        self.assertIn("older than Kresko's pin", hint)

    def test_panic_is_reported(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            log = self.write_log(tmpdir, "thread 'main' panicked at src/foo.rs:1:1\n")
            self.assertIn("panicked", lab.explain_startup_failure(log))

    def test_unrecognised_failure_adds_nothing(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            log = self.write_log(tmpdir, "starting up\nall good\n")
            self.assertEqual(lab.explain_startup_failure(log), "")

    def test_missing_log_is_not_an_error(self):
        self.assertEqual(lab.explain_startup_failure(Path("/nonexistent/x.log")), "")


class ConfigRewriting(unittest.TestCase):
    def section_of(self, text: str, name: str) -> str:
        """The body of [name], up to the next section header."""
        body = text.split(f"[{name}]\n", 1)[1]
        lines = []
        for line in body.splitlines():
            if line.strip().startswith("[") and line.strip().endswith("]"):
                break
            lines.append(line)
        return "\n".join(lines)

    def test_same_key_in_two_sections_is_addressed_independently(self):
        # The bug a real run caught: `cache_dir` exists in both [network] and
        # [state], and a bare-key rewrite hit only the first, leaving every
        # node sharing one RocksDB.
        out = lab.set_toml_values(
            GENERATED_CONFIG,
            {
                "network.cache_dir": '"/lab/peers"',
                "state.cache_dir": '"/lab/state"',
            },
        )
        self.assertIn('cache_dir = "/lab/peers"', self.section_of(out, "network"))
        self.assertIn('cache_dir = "/lab/state"', self.section_of(out, "state"))
        self.assertNotIn("/root/.cache/zebra", out)

    def test_listen_addr_is_set_per_section(self):
        out = rewrite_for_node(1)
        self.assertIn(f'listen_addr = "{lab.node_ip(1)}:{lab.P2P_PORT}"', self.section_of(out, "network"))
        self.assertIn(f'listen_addr = "{lab.node_ip(1)}:{lab.RPC_PORT}"', self.section_of(out, "rpc"))

    def test_missing_key_is_a_loud_failure(self):
        # Template drift must fail rather than silently leave a default binding.
        with self.assertRaises(KeyError):
            lab.set_toml_values(GENERATED_CONFIG, {"state.nonexistent": "1"})

    def test_wrong_section_is_also_a_loud_failure(self):
        # cache_dir exists, but not in [rpc]; this must not silently no-op.
        with self.assertRaises(KeyError):
            lab.set_toml_values(GENERATED_CONFIG, {"rpc.cache_dir": '"/x"'})

    def test_dotted_subsection_keys_are_addressable(self):
        out = lab.set_toml_values(
            GENERATED_CONFIG,
            {"network.testnet_parameters.checkpoints": '"/lab/checkpoints.txt"'},
        )
        self.assertIn('checkpoints = "/lab/checkpoints.txt"', out)
        self.assertNotIn("/root/payload", out)

    def test_internal_miner_is_inserted_into_the_mining_section(self):
        out = rewrite_for_node(miner=True)
        self.assertIn("internal_miner = true", self.section_of(out, "mining"))

    def test_relay_nodes_do_not_mine(self):
        self.assertIn("internal_miner = false", self.section_of(rewrite_for_node(), "mining"))

    def test_metrics_section_is_created_when_absent(self):
        # Kresko emits no [metrics] section at all, but the backpressure
        # counters we grade are Prometheus-only.
        self.assertNotIn("[metrics]", GENERATED_CONFIG)
        out = rewrite_for_node(1)
        self.assertIn("[metrics]", out)
        self.assertIn(f'endpoint_addr = "{lab.node_ip(1)}:{lab.METRICS_PORT}"', self.section_of(out, "metrics"))

    def test_insertion_is_idempotent(self):
        once = rewrite_for_node(1)
        twice = lab.set_toml_values(
            once, {"mining.internal_miner": "false"}, insert_missing=True
        )
        self.assertEqual(twice.count("internal_miner"), 1)
        self.assertEqual(twice.count("[metrics]"), 1)

    def test_public_seed_peers_are_emptied(self):
        out = rewrite_for_node(1)
        self.assertIn("initial_mainnet_peers = []", out)
        self.assertIn("bootstrap_peers = []", out)
        # No public host may survive anywhere in the config.
        for host in ("dnsseed.z.cash", "zfnd.org", "165.22.54.66", "104.131.184.123"):
            self.assertNotIn(host, out)

    def test_loopback_peer_list_is_preserved(self):
        # Clearing public peers must not disconnect the testnet from itself.
        out = rewrite_for_node(1)
        self.assertIn("127.0.0.1:18233", out)
        self.assertIn("127.0.0.3:18233", out)

    def test_peer_list_is_regenerated_from_live_addressing(self):
        # The bug a real run caught: genesis baked 127.0.0.2/.3 into the peer
        # list, the nodes moved to 127.0.0.101+, and the network came up with
        # zero peers -- which would have silently measured nothing.
        out = lab.set_peer_list(GENERATED_CONFIG, index=0, node_count=3)
        self.assertIn(f'"{lab.node_ip(1)}:{lab.P2P_PORT}"', out)
        self.assertIn(f'"{lab.node_ip(2)}:{lab.P2P_PORT}"', out)
        # The stale entries from genesis must be gone.
        self.assertNotIn("127.0.0.3:18233", out)

    def test_a_node_is_not_its_own_peer(self):
        out = lab.set_peer_list(GENERATED_CONFIG, index=1, node_count=3)
        self.assertNotIn(f'"{lab.node_ip(1)}:{lab.P2P_PORT}"', out)
        self.assertIn(f'"{lab.node_ip(0)}:{lab.P2P_PORT}"', out)

    def test_every_node_reaches_every_other(self):
        count = 4
        for i in range(count):
            out = lab.set_peer_list(GENERATED_CONFIG, index=i, node_count=count)
            for other in range(count):
                expected = f'"{lab.node_ip(other)}:{lab.P2P_PORT}"'
                if other == i:
                    self.assertNotIn(expected, out)
                else:
                    self.assertIn(expected, out)

    def test_missing_peer_key_is_a_loud_failure(self):
        with self.assertRaises(KeyError):
            lab.set_peer_list("[network]\nlisten_addr = \"x\"\n", 0, 2)

    def test_fully_rewritten_config_has_no_wildcard_binds_or_shared_paths(self):
        out = rewrite_for_node(1)
        # A leftover 0.0.0.0 bind means two nodes collide on one port.
        self.assertNotIn("0.0.0.0", out)
        # A leftover /root path means nodes share state or read a path that
        # only exists on a deployed droplet.
        self.assertNotIn("/root/", out)


class KreskoConfigGeneration(unittest.TestCase):
    def make_args(self, **overrides):
        defaults = dict(
            node_count=3,
            chain_id="mempool-load",
            experiment="mempool-load",
            block_time_secs=5,
            orchard_lanes_per_miner=384,
            orchard_lane_value_zats=100_000,
        )
        defaults.update(overrides)
        return type("Args", (), defaults)()

    def test_config_is_local_genesis_only(self):
        # This is the safety property: Kresko's require_local_genesis() gate
        # refuses every public-network command when this field is local-genesis.
        config = lab.build_kresko_config(self.make_args())
        self.assertEqual(config["network_kind"], "local-genesis")

    def test_one_miner_entry_per_node_with_distinct_addresses(self):
        config = lab.build_kresko_config(self.make_args(node_count=4))
        self.assertEqual(len(config["miners"]), 4)
        ips = [m["public_ip"] for m in config["miners"]]
        self.assertEqual(len(set(ips)), 4)
        for miner in config["miners"]:
            self.assertTrue(miner["public_ip"].startswith("127.0.0."))
            self.assertEqual(miner["public_ip"], miner["private_ip"])

    def test_config_is_deterministic(self):
        first = lab.build_kresko_config(self.make_args())
        second = lab.build_kresko_config(self.make_args())
        self.assertEqual(json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True))

    def test_no_ssh_material_is_embedded(self):
        config = lab.build_kresko_config(self.make_args())
        self.assertEqual(config["ssh_key_path"], "")
        self.assertEqual(config["ssh_pub_key_path"], "")

    def test_orchard_knobs_are_threaded_through(self):
        config = lab.build_kresko_config(
            self.make_args(orchard_lanes_per_miner=16, orchard_lane_value_zats=1234)
        )
        self.assertEqual(config["orchard_txblast"]["lanes_per_miner"], 16)
        self.assertEqual(config["orchard_txblast"]["lane_value_zats"], 1234)


class PrometheusParsing(unittest.TestCase):
    EXPOSITION = """\
# HELP mempool_queued_transactions_total total
# TYPE mempool_queued_transactions_total counter
mempool_queued_transactions_total 42
zcash_mempool_size_transactions 7
mempool_full_queue_per_peer_total{peer="1.2.3.4:8233"} 3
mempool_full_queue_per_peer_total{peer="5.6.7.8:8233"} 4
some_unrelated_metric 999
malformed_metric not_a_number
"""

    def test_extracts_only_wanted_series(self):
        out = monitor.parse_prometheus(self.EXPOSITION, monitor.MEMPOOL_METRICS)
        self.assertEqual(out["mempool_queued_transactions_total"], 42.0)
        self.assertEqual(out["zcash_mempool_size_transactions"], 7.0)
        self.assertNotIn("some_unrelated_metric", out)

    def test_sums_across_label_sets(self):
        # Per-peer counters arrive as one series per peer; the backpressure
        # figure we grade is the total across peers.
        out = monitor.parse_prometheus(self.EXPOSITION, monitor.MEMPOOL_METRICS)
        self.assertEqual(out["mempool_full_queue_per_peer_total"], 7.0)

    def test_ignores_comments_and_malformed_values(self):
        out = monitor.parse_prometheus(self.EXPOSITION, ("malformed_metric",))
        self.assertEqual(out, {})


class Percentiles(unittest.TestCase):
    def test_empty_input_is_none(self):
        self.assertIsNone(monitor.percentile([], 50))

    def test_p50_and_p95_of_a_known_series(self):
        values = [float(i) for i in range(1, 101)]
        self.assertEqual(monitor.percentile(values, 50), 50.0)
        self.assertEqual(monitor.percentile(values, 95), 95.0)

    def test_single_value(self):
        self.assertEqual(monitor.percentile([4.2], 95), 4.2)


class Propagation(unittest.TestCase):
    def test_spread_is_last_seen_minus_first_seen(self):
        first_seen = {"tx1": {"miner-0": 1.0, "miner-1": 3.0, "miner-2": 2.0}}
        stats = monitor.propagation_stats(first_seen, node_count=3)
        self.assertEqual(stats["spread_max_secs"], 2.0)
        self.assertEqual(stats["txids_on_all_nodes"], 1)

    def test_single_node_txids_are_excluded_from_latency(self):
        # A txid only ever seen on one node never propagated (or was mined
        # first); folding a 0s spread in would flatter the latency figures.
        first_seen = {
            "tx1": {"miner-0": 1.0},
            "tx2": {"miner-0": 1.0, "miner-1": 5.0},
        }
        stats = monitor.propagation_stats(first_seen, node_count=2)
        self.assertEqual(stats["txids_on_single_node"], 1)
        self.assertEqual(stats["txids_on_multiple_nodes"], 1)
        self.assertEqual(stats["spread_p50_secs"], 4.0)

    def test_first_seen_keeps_the_earliest_observation(self):
        first_seen: dict = {}
        monitor.record_first_seen(
            first_seen, {"node": "miner-0", "mempool_txids": ["tx1"]}, 1.0
        )
        monitor.record_first_seen(
            first_seen, {"node": "miner-0", "mempool_txids": ["tx1"]}, 9.0
        )
        self.assertEqual(first_seen["tx1"]["miner-0"], 1.0)

    def test_sampling_resolution_is_the_median_round_gap(self):
        # Every propagation figure is floored by this, so it is reported
        # alongside them rather than left for the reader to infer.
        samples = [
            {"node": "miner-0", "elapsed": 0.0},
            {"node": "miner-1", "elapsed": 0.05},
            {"node": "miner-0", "elapsed": 1.0},
            {"node": "miner-1", "elapsed": 1.05},
            {"node": "miner-0", "elapsed": 2.0},
            {"node": "miner-1", "elapsed": 2.05},
        ]
        self.assertEqual(monitor.sampling_resolution(samples), 1.0)

    def test_sampling_resolution_of_a_single_round_is_none(self):
        self.assertIsNone(
            monitor.sampling_resolution([{"node": "miner-0", "elapsed": 0.0}])
        )

    def test_missing_mempool_list_is_ignored(self):
        first_seen: dict = {}
        monitor.record_first_seen(first_seen, {"node": "m", "mempool_txids": None}, 1.0)
        self.assertEqual(first_seen, {})


class TxblastTraces(unittest.TestCase):
    def write_trace(self, tmp: Path, events: list[dict]) -> Path:
        trace_dir = tmp / "traces"
        trace_dir.mkdir(parents=True, exist_ok=True)
        (trace_dir / "txblast_event.jsonl").write_text(
            "".join(json.dumps(e) + "\n" for e in events)
        )
        return trace_dir

    def test_counts_submit_outcomes(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            trace_dir = self.write_trace(
                Path(tmpdir),
                [
                    {"event": "tx_submitted", "confirm_delay_ms": 100},
                    {"event": "tx_submitted", "confirm_delay_ms": 300},
                    {"event": "tx_submit_failed", "error_class": "mempool_full"},
                    {"event": "tx_build_failed", "error_class": "builder_error"},
                    # Real event name, verified against a live txblast run.
                    {"event": "tx_post_submit_mempool_seen"},
                    {"event": "tx_confirmed"},
                ],
            )
            out = monitor.read_txblast_traces(trace_dir)
        self.assertEqual(out["submitted"], 2)
        self.assertEqual(out["submit_failed"], 1)
        self.assertEqual(out["build_failed"], 1)
        self.assertEqual(out["mempool_seen"], 1)
        self.assertEqual(out["confirmed"], 1)
        self.assertEqual(out["node_rejects"], 2)
        self.assertEqual(out["reject_rate"], 0.5)
        self.assertEqual(out["confirm_delay_p50_ms"], 100)

    def test_anchor_races_are_not_counted_as_node_rejects(self):
        # A live run produced exactly this: one unknown_orchard_anchor among 22
        # submissions is 4.35%, close enough to the 5% threshold to fail a
        # healthy PR. It is the blaster racing a new block, not a node reject.
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            trace_dir = self.write_trace(
                Path(tmpdir),
                [{"event": "tx_submitted"} for _ in range(21)]
                + [
                    {"event": "tx_submit_failed", "error_class": "unknown_orchard_anchor"},
                    {"event": "chain_rebuild_started"},
                ],
            )
            out = monitor.read_txblast_traces(trace_dir)
        self.assertEqual(out["workload_failures"], 1)
        self.assertEqual(out["node_rejects"], 0)
        self.assertEqual(out["reject_rate"], 0.0)
        self.assertEqual(out["chain_rebuilds"], 1)

    def test_genuine_node_rejects_still_count(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            trace_dir = self.write_trace(
                Path(tmpdir),
                [{"event": "tx_submitted"} for _ in range(9)]
                + [{"event": "tx_submit_failed", "error_class": "mempool_full"}],
            )
            out = monitor.read_txblast_traces(trace_dir)
        self.assertEqual(out["node_rejects"], 1)
        self.assertEqual(out["reject_rate"], 0.1)

    def test_unknown_events_and_bad_lines_are_ignored(self):
        # A Kresko version that adds events should under-count, never fail.
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            trace_dir = Path(tmpdir) / "traces"
            trace_dir.mkdir(parents=True)
            (trace_dir / "txblast_event.jsonl").write_text(
                '{"event": "tx_submitted"}\nnot json at all\n{"event": "brand_new"}\n\n'
            )
            out = monitor.read_txblast_traces(trace_dir)
        self.assertEqual(out["submitted"], 1)
        self.assertEqual(out["reject_rate"], 0.0)

    def test_missing_trace_dir_reports_zero_files(self):
        out = monitor.read_txblast_traces(Path("/nonexistent/traces"))
        self.assertEqual(out["trace_files"], 0)
        self.assertIsNone(out["reject_rate"])


class Convergence(unittest.TestCase):
    NODES = [{"name": "miner-0"}, {"name": "miner-1"}]

    def test_converged_when_heights_match(self):
        samples = [
            {"node": "miner-0", "height": 100},
            {"node": "miner-1", "height": 100},
        ]
        out = monitor.tip_convergence(samples, self.NODES)
        self.assertTrue(out["converged"])
        self.assertEqual(out["spread"], 0)

    def test_one_block_of_spread_is_tolerated(self):
        # Nodes are sampled sequentially, so a one-block difference is a
        # sampling artifact rather than a fork.
        samples = [
            {"node": "miner-0", "height": 101},
            {"node": "miner-1", "height": 100},
        ]
        self.assertTrue(monitor.tip_convergence(samples, self.NODES)["converged"])

    def test_divergence_is_detected(self):
        samples = [
            {"node": "miner-0", "height": 120},
            {"node": "miner-1", "height": 100},
        ]
        self.assertFalse(monitor.tip_convergence(samples, self.NODES)["converged"])

    def test_a_silent_node_is_not_converged(self):
        samples = [{"node": "miner-0", "height": 100}]
        out = monitor.tip_convergence(samples, self.NODES)
        self.assertFalse(out["converged"])
        self.assertEqual(out["reporting_nodes"], 1)

    def test_uses_the_last_height_per_node(self):
        samples = [
            {"node": "miner-0", "height": 10},
            {"node": "miner-0", "height": 50},
            {"node": "miner-1", "height": 50},
        ]
        self.assertTrue(monitor.tip_convergence(samples, self.NODES)["converged"])

    def test_a_frozen_chain_converges_but_did_not_advance(self):
        # If block production dies, every node freezes at the same height and
        # looks perfectly converged. `advanced` is what catches that.
        samples = [
            {"node": "miner-0", "height": 100},
            {"node": "miner-1", "height": 100},
            {"node": "miner-0", "height": 100},
            {"node": "miner-1", "height": 100},
        ]
        out = monitor.tip_convergence(samples, self.NODES)
        self.assertTrue(out["converged"])
        self.assertFalse(out["advanced"])

    def test_a_live_chain_reports_advanced(self):
        samples = [
            {"node": "miner-0", "height": 100},
            {"node": "miner-1", "height": 100},
            {"node": "miner-0", "height": 140},
            {"node": "miner-1", "height": 140},
        ]
        out = monitor.tip_convergence(samples, self.NODES)
        self.assertTrue(out["advanced"])
        self.assertEqual(out["height_growth"]["miner-0"], 40)

    def test_a_node_that_stops_answering_is_reported(self):
        samples = [
            {"node": "miner-0", "height": 100},
            {"node": "miner-1", "height": 100},
            {"node": "miner-0", "height": 140},
            {"node": "miner-1", "rpc_error": "connection refused"},
        ]
        out = monitor.tip_convergence(samples, self.NODES)
        self.assertEqual(out["unresponsive_at_end"], ["miner-1"])


class Verdict(unittest.TestCase):
    def make_args(self, **overrides):
        defaults = dict(max_reject_rate=0.05, min_txids_observed=10,
                        min_graded_submissions=50)
        defaults.update(overrides)
        return type("Args", (), defaults)()

    def healthy_result(self, **overrides):
        result = {
            "throughput": {
                "trace_files": 1,
                "submitted": 500,
                "submit_failed": 0,
                "build_failed": 0,
                "confirmed": 480,
                "mempool_seen": 500,
                "node_rejects": 0,
                "workload_failures": 0,
                "reject_rate": 0.0,
            },
            "propagation": {"txids_observed": 400, "txids_on_multiple_nodes": 400},
            "convergence": {
                "converged": True,
                "advanced": True,
                "heights": {},
                "unresponsive_at_end": [],
            },
            "backpressure": {"zcash_mempool_size_transactions": 12.0},
            "panics": [],
        }
        result.update(overrides)
        return result

    def test_a_frozen_chain_fails_the_run(self):
        # The headline "green run that measured nothing" case.
        result = self.healthy_result()
        result["convergence"]["advanced"] = False
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")
        self.assertTrue(any("never advanced" in r for r in reasons))

    def test_a_node_going_silent_fails_the_run(self):
        result = self.healthy_result()
        result["convergence"]["unresponsive_at_end"] = ["miner-1"]
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")
        self.assertTrue(any("stopped answering" in r for r in reasons))

    def test_no_scraped_metrics_fails_the_run(self):
        # Absent backpressure data must not read as "zero backpressure".
        result = self.healthy_result(backpressure={})
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")
        self.assertTrue(any("Prometheus" in r for r in reasons))

    def test_healthy_run_passes(self):
        verdict, reasons = monitor.grade_run(self.healthy_result(), self.make_args())
        self.assertEqual(verdict, "ok")
        self.assertEqual(reasons, [])

    def test_panic_fails_the_run(self):
        result = self.healthy_result(panics=["miner-0: panicked at foo"])
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")
        self.assertTrue(any("panic" in r for r in reasons))

    def test_zero_submissions_fails_the_run(self):
        result = self.healthy_result()
        result["throughput"]["submitted"] = 0
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")
        self.assertTrue(any("zero transactions" in r for r in reasons))

    def test_missing_traces_fails_the_run(self):
        # Silence from the load generator must not read as a clean run.
        result = self.healthy_result()
        result["throughput"]["trace_files"] = 0
        verdict, _ = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")

    def test_excessive_rejects_fail_the_run(self):
        result = self.healthy_result()
        result["throughput"]["reject_rate"] = 0.2
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")
        self.assertTrue(any("reject rate" in r for r in reasons))

    def test_reject_rate_is_not_graded_below_the_sample_floor(self):
        # Throughput is proving-bound, so a short run submits tens of
        # transactions where one reject is already several percent. Grading
        # that would fail healthy runs on noise.
        result = self.healthy_result()
        result["throughput"]["submitted"] = 22
        result["throughput"]["reject_rate"] = 0.2
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertFalse(any("reject rate" in r for r in reasons))
        self.assertNotEqual(verdict, "failed")

    def test_reject_rate_is_graded_once_there_are_enough_samples(self):
        result = self.healthy_result()
        result["throughput"]["submitted"] = 500
        result["throughput"]["reject_rate"] = 0.2
        verdict, _ = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")

    def test_tip_divergence_fails_the_run(self):
        result = self.healthy_result(convergence={"converged": False, "heights": {}})
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")
        self.assertTrue(any("converge" in r for r in reasons))

    def test_no_propagation_fails_the_run(self):
        # This is the signal PRs #341 and #64 are about; it must be fatal.
        result = self.healthy_result()
        result["propagation"]["txids_on_multiple_nodes"] = 0
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "failed")

    def test_thin_but_clean_run_is_degraded_not_failed(self):
        result = self.healthy_result()
        result["propagation"]["txids_observed"] = 3
        verdict, reasons = monitor.grade_run(result, self.make_args())
        self.assertEqual(verdict, "degraded")
        self.assertTrue(reasons)

    def test_all_failure_reasons_are_reported_together(self):
        result = self.healthy_result(
            panics=["boom"], convergence={"converged": False, "heights": {}}
        )
        result["throughput"]["submitted"] = 0
        _, reasons = monitor.grade_run(result, self.make_args())
        self.assertGreaterEqual(len(reasons), 3)


class Reporting(unittest.TestCase):
    def full_result(self, verdict="ok"):
        return {
            "meta": {"sha": "abc123", "node_count": 4, "duration_secs": 300, "tx_rate": 10},
            "throughput": {
                "trace_files": 1,
                "submitted": 500,
                "submit_failed": 2,
                "build_failed": 0,
                "confirmed": 470,
                "mempool_seen": 498,
                "node_rejects": 2,
                "workload_failures": 0,
                "reject_rate": 0.004,
                "confirm_delay_p50_ms": 1200.0,
                "confirm_delay_p95_ms": 4000.0,
            },
            "propagation": {
                "txids_observed": 480,
                "txids_on_multiple_nodes": 470,
                "txids_on_all_nodes": 450,
                "spread_p50_secs": 0.4,
                "spread_p95_secs": 1.2,
            },
            "convergence": {"spread": 0, "heights": {}},
            "backpressure": {"mempool_full_queue_per_peer_total": 7.0},
            "peak_mempool_txs": 220,
            "panics": [],
            "effective_tx_per_sec": 1.667,
            "verdict": verdict,
            "reasons": [] if verdict == "ok" else ["something went wrong"],
        }

    def test_every_metric_row_carries_an_interpretation_hint(self):
        # The hints exist because these numbers are genuinely easy to misread:
        # "Confirmed 10 / Submitted 27" looked like a 63% failure rate when
        # acceptance was actually 100%.
        out = monitor.render_markdown(self.full_result())
        header = "| Metric | Value | How to read it |"
        self.assertIn(header, out)
        body = [
            line for line in out.splitlines()
            if line.startswith("| ") and line != header and not line.startswith("| ---")
            and not line.startswith("| Counter")
        ]
        self.assertTrue(body)
        for line in body:
            cells = [c.strip() for c in line.strip("|").split("|")]
            self.assertEqual(len(cells), 3, f"row is not 3 columns: {line}")
            self.assertTrue(cells[2], f"row has no hint: {line}")

    def test_confirmed_is_not_presented_as_a_success_rate(self):
        out = monitor.render_markdown(self.full_result())
        confirmed = next(l for l in out.splitlines() if l.startswith("| Confirmed"))
        self.assertIn("not** a success rate", confirmed)
        self.assertIn("lags", confirmed)

    def test_backpressure_counters_are_explained(self):
        out = monitor.render_markdown(self.full_result())
        row = next(l for l in out.splitlines() if "mempool_full_queue_per_peer_total" in l)
        self.assertIn("backpressure", row.lower())
        # The rejected-ID cache is routinely misread as a reject count.
        self.assertIn(
            "NOT a count",
            monitor.BACKPRESSURE_HINTS["mempool_rejected_transaction_ids"],
        )

    def test_markdown_reports_the_headline_numbers(self):
        out = monitor.render_markdown(self.full_result())
        self.assertIn("PASS", out)
        self.assertIn("500", out)
        self.assertIn("0.40%", out)
        self.assertIn("mempool_full_queue_per_peer_total", out)

    def test_failed_verdict_lists_reasons(self):
        out = monitor.render_markdown(self.full_result(verdict="failed"))
        self.assertIn("FAIL", out)
        self.assertIn("something went wrong", out)

    def test_missing_values_render_as_na_not_none(self):
        result = self.full_result()
        result["propagation"]["spread_p95_secs"] = None
        result["throughput"]["reject_rate"] = None
        out = monitor.render_markdown(result)
        self.assertIn("n/a", out)
        self.assertNotIn("None", out)


class SecretSafety(unittest.TestCase):
    def test_collected_paths_exclude_key_material(self):
        # The artifact allowlist is the control that keeps funded keys out of
        # uploaded artifacts, so assert on it directly.
        for path in lab.COLLECTED_PATHS:
            self.assertNotIn("funded_key", path)
            self.assertNotIn("funded_keys", path)
            self.assertNotIn("treasury", path)

    def test_secret_filenames_are_recognised(self):
        for name in (
            "funded_key.json",
            "funded_keys.json",
            "treasury_key.json",
            "nodes/miner-0/funded_key.json",
        ):
            self.assertTrue(lab.is_secret_path(name), name)

    def test_ordinary_artifacts_are_not_treated_as_secret(self):
        for name in ("run.log", "summary.json", "traces/txblast_event.jsonl", "zakura.toml"):
            self.assertFalse(lab.is_secret_path(name), name)

    def test_config_json_secrets_are_detected_by_content(self):
        # The leak a real run found: `kresko genesis` writes every funded key's
        # secret_key_hex, and the bootstrap treasury key, into config.json --
        # a filename the name-based check waves straight through.
        self.assertFalse(lab.is_secret_path("config.json"))
        self.assertTrue(lab.contains_secret('{"secret_key_hex": "DEADBEEF"}'))
        self.assertTrue(lab.contains_secret('{"bootstrap_treasury_key": {}}'))
        self.assertFalse(lab.contains_secret('{"height": 128, "address": "tmAbc"}'))

    def test_sanitized_config_keeps_parameters_and_drops_keys(self):
        raw = json.dumps(
            {
                "chain_id": "mempool-load",
                "orchard_txblast": {"lanes_per_miner": 24},
                "local_genesis": {
                    "network_name": "Kresko_local",
                    "genesis_hash": "abc123",
                    "funded_keys": [
                        {"name": "miner-0", "secret_key_hex": "DEADBEEF", "address": "tmAbc"}
                    ],
                    "bootstrap_treasury_key": {"secret_key_hex": "C0FFEE"},
                },
            }
        )
        out = lab.sanitize_config(raw)
        self.assertNotIn("DEADBEEF", out)
        self.assertNotIn("C0FFEE", out)
        self.assertFalse(lab.contains_secret(out))
        # The run parameters must survive, or the artifact loses its value.
        self.assertIn("mempool-load", out)
        self.assertIn("Kresko_local", out)
        self.assertIn("abc123", out)
        self.assertIn("24", out)

    def test_collect_sanitizes_config_json(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            lab_dir = root / "lab"
            lab_dir.mkdir()
            (lab_dir / "config.json").write_text(
                json.dumps(
                    {
                        "chain_id": "keepme",
                        "local_genesis": {
                            "funded_keys": [{"secret_key_hex": "DEADBEEF"}],
                            "bootstrap_treasury_key": {"secret_key_hex": "C0FFEE"},
                        },
                    }
                )
            )
            out = root / "out"
            args = type("Args", (), {"lab_dir": str(lab_dir), "out": str(out)})()
            self.assertEqual(lab.cmd_collect(args), 0)

            collected = (out / "config.json").read_text()
            self.assertIn("keepme", collected)
            self.assertNotIn("DEADBEEF", collected)
            self.assertNotIn("C0FFEE", collected)

    def test_collect_refuses_an_allowlisted_file_carrying_secrets(self):
        # Defence in depth: if a future Kresko writes key material into some
        # other allowlisted file, collection must drop it rather than copy it.
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            lab_dir = root / "lab"
            (lab_dir / "nodes" / "miner-0").mkdir(parents=True)
            (lab_dir / "nodes" / "miner-0" / "run.log").write_text(
                'startup ok\nsecret_key_hex = "DEADBEEF"\n'
            )
            out = root / "out"
            args = type("Args", (), {"lab_dir": str(lab_dir), "out": str(out)})()
            self.assertEqual(lab.cmd_collect(args), 0)
            self.assertFalse((out / "nodes" / "miner-0" / "run.log").exists())

    def test_collect_copies_artifacts_but_never_keys(self):
        """End-to-end check on a lab dir seeded with real key filenames."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            lab_dir = root / "lab"
            node = lab_dir / "nodes" / "miner-0"
            node.mkdir(parents=True)
            (node / "run.log").write_text("node log\n")
            (node / "zakura.toml").write_text("[network]\n")
            (node / "funded_key.json").write_text('{"secret_key_hex": "DEADBEEF"}')
            (lab_dir / "traces").mkdir()
            (lab_dir / "traces" / "txblast_event.jsonl").write_text("{}\n")
            payload = lab_dir / "payload" / "local_genesis"
            payload.mkdir(parents=True)
            (payload / "funded_keys.json").write_text('{"secret_key_hex": "C0FFEE"}')

            out = root / "out"
            args = type(
                "Args", (), {"lab_dir": str(lab_dir), "out": str(out)}
            )()
            self.assertEqual(lab.cmd_collect(args), 0)

            collected = {p.name for p in out.rglob("*") if p.is_file()}
            self.assertIn("run.log", collected)
            self.assertIn("txblast_event.jsonl", collected)
            self.assertNotIn("funded_key.json", collected)
            self.assertNotIn("funded_keys.json", collected)

            # And no collected byte contains a secret, whatever the filename.
            for path in out.rglob("*"):
                if path.is_file():
                    body = path.read_text(errors="replace")
                    self.assertNotIn("DEADBEEF", body)
                    self.assertNotIn("C0FFEE", body)


class Comparison(unittest.TestCase):
    def leg(self, **overrides):
        leg = {
            "meta": {"sha": "abc", "node_count": 4, "duration_secs": 300, "tx_rate": 10},
            "verdict": "ok",
            "throughput": {
                "submitted": 1000,
                "reject_rate": 0.01,
                "confirm_delay_p50_ms": 1000.0,
                "confirm_delay_p95_ms": 2000.0,
            },
            "propagation": {
                "spread_p50_secs": 0.5,
                "spread_p95_secs": 1.0,
                "txids_on_all_nodes": 900,
            },
            "effective_tx_per_sec": 3.33,
            "peak_mempool_txs": 200,
        }
        for section, values in overrides.items():
            if isinstance(values, dict):
                leg[section].update(values)
            else:
                leg[section] = values
        return leg

    def row_for(self, rows, metric):
        return next(r for r in rows if r["metric"] == metric)

    def test_identical_runs_show_no_change(self):
        rows = compare.build_rows(self.leg(), self.leg())
        self.assertTrue(all(r["verdict"] == "=" for r in rows))

    def test_throughput_drop_is_flagged_as_worse(self):
        target = self.leg(throughput={"submitted": 500})
        rows = compare.build_rows(self.leg(), target)
        row = self.row_for(rows, "Transactions submitted")
        self.assertEqual(row["verdict"], "WORSE")
        self.assertAlmostEqual(row["delta_pct"], -50.0)

    def test_throughput_gain_is_flagged_as_better(self):
        target = self.leg(throughput={"submitted": 2000})
        rows = compare.build_rows(self.leg(), target)
        self.assertEqual(self.row_for(rows, "Transactions submitted")["verdict"], "better")

    def test_latency_direction_is_inverted(self):
        # Higher propagation latency is a regression, unlike higher throughput.
        target = self.leg(propagation={"spread_p95_secs": 5.0})
        rows = compare.build_rows(self.leg(), target)
        self.assertEqual(self.row_for(rows, "Propagation p95 (s)")["verdict"], "WORSE")

        faster = self.leg(propagation={"spread_p95_secs": 0.1})
        rows = compare.build_rows(self.leg(), faster)
        self.assertEqual(self.row_for(rows, "Propagation p95 (s)")["verdict"], "better")

    def test_small_deltas_are_treated_as_noise(self):
        target = self.leg(throughput={"submitted": 1050})  # +5%
        rows = compare.build_rows(self.leg(), target)
        self.assertEqual(self.row_for(rows, "Transactions submitted")["verdict"], "=")

    def test_zero_baseline_does_not_produce_infinity(self):
        baseline = self.leg(throughput={"submitted": 0})
        rows = compare.build_rows(baseline, self.leg())
        row = self.row_for(rows, "Transactions submitted")
        self.assertIsNone(row["delta_pct"])
        # Unbounded, but unambiguously an improvement in direction.
        self.assertEqual(row["verdict"], "better")

    def test_rejects_appearing_from_a_clean_baseline_are_flagged(self):
        # Reject rate is 0 on every healthy baseline, so a None delta must not
        # be read as "no change" -- 0 -> nonzero is the regression that matters.
        target = self.leg(throughput={"reject_rate": 0.4})
        baseline = self.leg(throughput={"reject_rate": 0.0})
        rows = compare.build_rows(baseline, target)
        row = self.row_for(rows, "Reject rate")
        self.assertEqual(row["verdict"], "WORSE")
        report = compare.render(baseline, target, rows)
        self.assertIn("Reject rate", report.split("regressed beyond")[-1])

    def test_zero_to_zero_is_not_a_regression(self):
        rows = compare.build_rows(
            self.leg(throughput={"reject_rate": 0.0}),
            self.leg(throughput={"reject_rate": 0.0}),
        )
        self.assertEqual(self.row_for(rows, "Reject rate")["verdict"], "=")

    def test_missing_metrics_do_not_crash(self):
        target = self.leg()
        del target["propagation"]["spread_p95_secs"]
        rows = compare.build_rows(self.leg(), target)
        self.assertIsNone(self.row_for(rows, "Propagation p95 (s)")["delta_pct"])

    def test_report_names_the_regressed_metrics(self):
        target = self.leg(throughput={"submitted": 400})
        rows = compare.build_rows(self.leg(), target)
        report = compare.render(self.leg(), target, rows)
        self.assertIn("regressed beyond the noise floor", report)
        self.assertIn("Transactions submitted", report)
        self.assertNotIn("inf", report)

    def test_clean_report_says_so(self):
        rows = compare.build_rows(self.leg(), self.leg())
        report = compare.render(self.leg(), self.leg(), rows)
        self.assertIn("No metric regressed", report)


if __name__ == "__main__":
    unittest.main(verbosity=2)
