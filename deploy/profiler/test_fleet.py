#!/usr/bin/env python3
"""Tests for the fleet installer: the node list, the config edit and the files it writes."""

import contextlib
import hashlib
import io
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest
from pathlib import Path
from dataclasses import replace

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fleet  # noqa: E402

NODE_LIST = """\
[defaults]
service_name = "zakurad"
log_file = "/root/logs/zakura-tracing.log"

[[nodes]]
name = "us-east-0"
ssh_string = "root@192.0.2.1"
monitor = { profiler_url = "http://127.0.0.1:8097" }

[[nodes]]
name = "archive-vct-off"
ssh_string = "root@192.0.2.2"
log_file = "/var/log/zakura/zakura.log"
monitor = { profiler_url = "http://127.0.0.1:8098" }

[[nodes]]
name = "zakura-compat"
ssh_string = "root@192.0.2.3"
service_name = "zakurad-compat"
"""

# A dual-stack node's config, with a comment and a later section to keep in place.
DUAL_CONFIG = """\
[network]
listen_addr = "0.0.0.0:8233"
p2p_stack = "dual"   # both stacks

[network.zakura]
listen_addr = "0.0.0.0:8234"

[state]
cache_dir = "/mnt/data/zakura-cache"
"""

LEGACY_CONFIG = """\
[network]
listen_addr = "0.0.0.0:8233"
p2p_stack = "legacy"

[state]
cache_dir = "/mnt/data/zakura-cache\""""


def load(node_list):
    with tempfile.NamedTemporaryFile("w", suffix=".toml") as handle:
        handle.write(node_list)
        handle.flush()
        return fleet.load_nodes(handle.name)


class NodeListTests(unittest.TestCase):
    def test_only_nodes_with_a_profiler_address_take_part(self):
        nodes = load(NODE_LIST)
        self.assertEqual([node.name for node in nodes], ["us-east-0", "archive-vct-off"])

    def test_a_node_takes_its_own_values_over_the_defaults(self):
        local, remote = load(NODE_LIST)
        self.assertEqual((local.log_file, local.service_name), ("/root/logs/zakura-tracing.log", "zakurad"))
        self.assertEqual(remote.log_file, "/var/log/zakura/zakura.log")
        self.assertEqual((remote.ssh_string, remote.ssh_port, remote.dashboard_port), ("root@192.0.2.2", 22, 8098))

    def test_only_a_profiler_on_another_host_needs_a_tunnel(self):
        local, remote = load(NODE_LIST)
        self.assertFalse(local.needs_tunnel)
        self.assertTrue(remote.needs_tunnel)

    def test_two_nodes_on_one_tunnel_port_are_refused(self):
        clash = NODE_LIST.replace(
            'name = "zakura-compat"', 'name = "zakura-compat"\nmonitor = { profiler_url = "http://127.0.0.1:8098" }'
        )
        with self.assertRaises(fleet.FleetError):
            load(clash)

    def test_a_profiler_address_without_a_port_is_refused(self):
        with self.assertRaises(fleet.FleetError):
            load(NODE_LIST.replace("http://127.0.0.1:8098", "http://127.0.0.1"))


class TraceConfigTests(unittest.TestCase):
    def test_the_setting_goes_under_the_existing_section_and_nothing_else_moves(self):
        edited = fleet.with_trace_dir(DUAL_CONFIG)
        added = 'trace_dir = "/mnt/data/zakura-traces"\n'
        self.assertEqual(edited.replace(added, "", 1), DUAL_CONFIG)
        self.assertIn("[network.zakura]\n" + added + 'listen_addr = "0.0.0.0:8234"', edited)
        self.assertEqual(fleet.trace_dir_of(edited), "/mnt/data/zakura-traces")

    def test_peer_names_use_typed_addresses_and_preserve_other_settings(self):
        nodes = load(NODE_LIST)
        edited = fleet.with_trace_peer_names(DUAL_CONFIG, nodes[1], nodes)
        expected = tomllib.loads(DUAL_CONFIG)
        expected["network"]["zakura"]["trace_peer_names"] = {"192.0.2.1": "us-east-0"}
        self.assertEqual(tomllib.loads(edited), expected)
        self.assertEqual(fleet.with_trace_peer_names(edited, nodes[1], nodes), edited)

    def test_existing_peer_names_including_an_empty_mapping_are_preserved(self):
        nodes = load(NODE_LIST)
        for mapping in ('{}', '{ "2001:db8::9" = "my-node" }'):
            own = DUAL_CONFIG.replace("[network.zakura]", f"[network.zakura]\ntrace_peer_names = {mapping}")
            self.assertEqual(fleet.with_trace_peer_names(own, nodes[1], nodes), own)

    def test_ipv6_addresses_and_quoted_names_are_written_as_valid_toml(self):
        nodes = load(NODE_LIST.replace("192.0.2.1", "2001:db8::9"))
        nodes[0] = replace(nodes[0], name='a"quoted\\node')
        edited = fleet.with_trace_peer_names(LEGACY_CONFIG, nodes[1], nodes)
        self.assertEqual(tomllib.loads(edited)["network"]["zakura"]["trace_peer_names"], {"2001:db8::9": nodes[0].name})

    def test_a_hostname_cannot_be_silently_used_as_an_ip_address(self):
        nodes = load(NODE_LIST.replace("192.0.2.1", "example.com"))
        with self.assertRaisesRegex(fleet.FleetError, "must name an IP"):
            fleet.with_trace_peer_names(DUAL_CONFIG, nodes[1], nodes)

    def test_a_config_without_the_section_gets_it_at_the_end(self):
        edited = fleet.with_trace_dir(LEGACY_CONFIG)
        self.assertTrue(edited.startswith(LEGACY_CONFIG + "\n"))
        self.assertTrue(edited.endswith('\n[network.zakura]\ntrace_dir = "/mnt/data/zakura-traces"\n'))
        parsed = tomllib.loads(edited)
        self.assertEqual(parsed["network"]["zakura"], {"trace_dir": "/mnt/data/zakura-traces"})
        self.assertEqual(parsed["state"], {"cache_dir": "/mnt/data/zakura-cache"})

    def test_a_config_that_already_names_a_trace_directory_is_left_alone(self):
        own = DUAL_CONFIG.replace("[network.zakura]\n", '[network.zakura]\ntrace_dir = "/mnt/data/perftraces/raw"\n')
        self.assertEqual(fleet.with_trace_dir(own), own)
        self.assertEqual(fleet.trace_dir_of(own), "/mnt/data/perftraces/raw")

    def test_editing_twice_changes_nothing_more(self):
        once = fleet.with_trace_dir(LEGACY_CONFIG)
        self.assertEqual(fleet.with_trace_dir(once), once)

    def test_a_section_written_inline_is_refused_instead_of_broken(self):
        inline = '[network]\nzakura = { listen_addr = "0.0.0.0:8234" }\n'
        with self.assertRaises(fleet.FleetError):
            fleet.with_trace_dir(inline)

    def test_a_config_that_does_not_parse_is_refused(self):
        with self.assertRaises(fleet.FleetError):
            fleet.with_trace_dir("[network\n")

    def test_peer_addresses_are_switched_on_where_the_config_is_silent(self):
        edited = fleet.with_peer_addresses(DUAL_CONFIG)
        added = "expose_peer_addresses = true\n"
        self.assertEqual(edited.replace(added, "", 1), DUAL_CONFIG)
        self.assertIn("[network]\n" + added + 'listen_addr = "0.0.0.0:8233"', edited)
        self.assertTrue(fleet.exposes_peer_addresses(edited))
        self.assertEqual(fleet.with_peer_addresses(edited), edited)

    def test_a_config_that_hides_peer_addresses_on_purpose_is_left_alone(self):
        hidden = DUAL_CONFIG.replace("[network]\n", "[network]\nexpose_peer_addresses = false\n")
        self.assertEqual(fleet.with_peer_addresses(hidden), hidden)
        self.assertFalse(fleet.exposes_peer_addresses(hidden))

    def test_both_settings_can_be_added_to_one_config(self):
        edited = fleet.with_peer_addresses(fleet.with_trace_dir(LEGACY_CONFIG))
        parsed = tomllib.loads(edited)
        self.assertEqual(parsed["network"]["expose_peer_addresses"], True)
        self.assertEqual(parsed["network"]["zakura"], {"trace_dir": "/mnt/data/zakura-traces"})
        self.assertEqual(parsed["network"]["listen_addr"], "0.0.0.0:8233")

    def test_the_config_path_is_read_from_the_service_command_line(self):
        shown = (
            "{ path=/usr/local/bin/zakurad ; argv[]=/usr/local/bin/zakurad -c /root/.config/zakura.toml start ;"
            " ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }"
        )
        self.assertEqual(fleet.config_path_from_unit(shown), "/root/.config/zakura.toml")
        self.assertEqual(
            fleet.config_path_from_unit("{ argv[]=/usr/local/bin/zakurad --config=/etc/zakura/zakura.toml start ; }"),
            "/etc/zakura/zakura.toml",
        )
        with self.assertRaises(fleet.FleetError):
            fleet.config_path_from_unit("{ path=/usr/local/bin/zakurad ; argv[]=/usr/local/bin/zakurad start ; }")


class InstalledFilesTests(unittest.TestCase):
    def setUp(self):
        self.local, self.remote = load(NODE_LIST)

    def test_the_profiler_reads_the_node_s_own_log_and_trace_files(self):
        command = fleet.profiler_command(self.remote, fleet.TRACE_DIR, manage_traces=True)
        self.assertEqual(command[command.index("--node-name") + 1], "archive-vct-off")
        self.assertEqual(command[command.index("--log-file") + 1], "/var/log/zakura/zakura.log")
        self.assertEqual(command[command.index("--trace-dir") + 1], "/mnt/data/zakura-traces")
        self.assertEqual(command[command.index("--trace-table-cap-mb") + 1], "200")

    def test_a_trace_directory_someone_else_chose_is_never_emptied(self):
        command = fleet.profiler_command(self.local, "/mnt/data/perftraces/raw")
        self.assertEqual(command[command.index("--trace-dir") + 1], "/mnt/data/perftraces/raw")
        self.assertEqual(command[command.index("--trace-table-cap-mb") + 1], "0")

    def test_an_existing_directory_matching_the_default_is_also_preserved(self):
        command = fleet.profiler_command(self.local, fleet.TRACE_DIR)
        self.assertEqual(command[command.index("--trace-table-cap-mb") + 1], "0")

    def test_only_a_new_directory_is_claimed_and_a_second_claim_is_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "traces"
            command = ["bash", "-c", fleet.CLAIM_TRACE_SCRIPT, "fleet", str(path)]
            subprocess.run(command, check=True, timeout=5)
            capture = path / "capture.jsonl"
            capture.write_text("keep me\n")
            subprocess.run(command, check=True, timeout=5)
            self.assertEqual(capture.read_text(), "keep me\n")
            self.assertTrue((path / fleet.TRACE_MARKER).is_file())

    def test_an_existing_unclaimed_directory_cannot_be_adopted(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Path(directory) / "capture.jsonl"
            capture.write_text("keep me\n")
            done = subprocess.run(
                ["bash", "-c", fleet.CLAIM_TRACE_SCRIPT, "fleet", directory],
                capture_output=True, timeout=5,
            )
            self.assertNotEqual(done.returncode, 0)
            self.assertFalse((Path(directory) / fleet.TRACE_MARKER).exists())
            self.assertEqual(capture.read_text(), "keep me\n")

    def test_a_node_without_trace_files_gets_a_profiler_without_them(self):
        command = fleet.profiler_command(self.local, None)
        self.assertNotIn("--trace-dir", command)
        self.assertNotIn("--trace-table-cap-mb", command)

    def test_the_drop_in_replaces_the_unit_s_command(self):
        drop_in = fleet.unit_drop_in(self.remote, fleet.TRACE_DIR)
        lines = drop_in.splitlines()
        self.assertEqual(lines[1:3], ["[Service]", "ExecStart="])
        self.assertTrue(lines[3].startswith("ExecStart=/usr/bin/python3 /opt/zakura-profiler/zakura_profiler.py "))
        self.assertIn("--node-name archive-vct-off", lines[3])

    def test_the_archive_holds_the_profiler_its_unit_and_the_drop_in(self):
        archive = tarfile.open(fileobj=io.BytesIO(fleet.profiler_archive("[Service]\n")))
        self.assertEqual(
            sorted(archive.getnames()),
            sorted([*fleet.PROFILER_FILES, fleet.PROFILER_UNIT, "node.conf"]),
        )
        self.assertEqual(archive.extractfile("node.conf").read(), b"[Service]\n")

    def test_the_installed_profiler_normalizes_addresses_without_the_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            with tarfile.open(fileobj=io.BytesIO(fleet.profiler_archive("[Service]\n"))) as archive:
                archive.extractall(directory, filter="data")
            done = subprocess.run(
                [sys.executable, "-I", "-c",
                 "import sys; sys.path.insert(0, sys.argv[1]); import blocks; "
                 "print(blocks.canonical_ip('[::ffff:192.0.2.7]:8233'))", directory],
                capture_output=True, text=True, timeout=5, check=True,
            )
        self.assertEqual(done.stdout.strip(), "192.0.2.7")

    def test_a_node_is_told_every_other_node_s_name_and_address(self):
        drop_in = fleet.peer_names_drop_in(fleet.named_peers(self.remote, [self.local, self.remote], frozenset()))
        self.assertEqual(drop_in.splitlines()[1:], ["[Service]", "Environment=ZAKURA_TRACE_PEER_NAMES=us-east-0=192.0.2.1"])

    def test_a_node_with_a_private_address_is_named_to_no_node(self):
        private = fleet.parse_private_addresses('["::ffff:192.0.2.1"]')
        self.assertEqual(fleet.named_peers(self.remote, [self.local, self.remote], private), [])
        self.assertNotIn("192.0.2.1", fleet.peer_names_drop_in([]))

    def test_a_private_list_that_cannot_be_read_is_refused(self):
        for listed in ("", "{}", '["not an address"]', "[1]"):
            with self.assertRaises(fleet.FleetError):
                fleet.parse_private_addresses(listed)

    def test_a_node_reached_by_host_name_cannot_be_named(self):
        named = load(NODE_LIST.replace("root@192.0.2.2", "root@archive.example"))[1]
        with self.assertRaises(fleet.FleetError):
            fleet.peer_address(named)

    def test_the_tunnel_settings_name_the_dashboard_port_and_the_node(self):
        self.assertEqual(
            fleet.tunnel_env(self.remote), "LOCAL_PORT=8098\nSSH_TARGET=root@192.0.2.2\nSSH_PORT=22\n"
        )

    def test_the_tunnel_unit_reads_those_settings(self):
        unit = (Path(fleet.HERE) / fleet.TUNNEL_UNIT).read_text()
        self.assertIn(f"EnvironmentFile={fleet.TUNNEL_ENV_DIR}/%i.env", unit)
        self.assertIn(f"-L 127.0.0.1:${{LOCAL_PORT}}:127.0.0.1:{fleet.PROFILER_PORT} ${{SSH_TARGET}}", unit)


@unittest.skipUnless(shutil.which("sha256sum"), "the write script needs sha256sum, as the nodes have")
class ConfigWriteTests(unittest.TestCase):
    """The script that replaces a node's config, run as the node runs it."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.config = self.directory / "zakura.toml"
        self.config.write_text(DUAL_CONFIG)
        self.config.chmod(0o640)
        self.original = self.directory / "zakura.toml.before-trace-dir"
        self.edited = fleet.with_trace_dir(DUAL_CONFIG).encode()

    def write(self, arrived, expected=None):
        """Run the script with `arrived` on its input, expecting the checksum of `expected`."""
        checksum = hashlib.sha256(self.edited if expected is None else expected).hexdigest()
        return subprocess.run(
            ["bash", "-c", fleet.WRITE_CONFIG_SCRIPT, "fleet", str(self.config), checksum],
            input=arrived, capture_output=True, timeout=5,
        )

    def assert_config_untouched(self, done):
        self.assertNotEqual(done.returncode, 0)
        self.assertIn(b"did not arrive whole", done.stderr)
        self.assertEqual(self.config.read_text(), DUAL_CONFIG)
        self.assertEqual(sorted(path.name for path in self.directory.iterdir()), ["zakura.toml"])

    def test_the_config_is_replaced_and_the_first_original_is_kept(self):
        self.assertEqual(self.write(self.edited).returncode, 0)
        self.assertEqual(self.config.read_bytes(), self.edited)
        self.assertEqual(self.config.stat().st_mode & 0o777, 0o640)
        self.assertEqual(self.original.read_text(), DUAL_CONFIG)

        again = self.edited + b"# a later edit\n"
        self.assertEqual(self.write(again, expected=again).returncode, 0)
        self.assertEqual(self.config.read_bytes(), again)
        self.assertEqual(self.original.read_text(), DUAL_CONFIG)
        self.assertEqual(
            sorted(path.name for path in self.directory.iterdir()), ["zakura.toml", "zakura.toml.before-trace-dir"]
        )

    def test_a_connection_that_ends_before_the_config_arrives_changes_nothing(self):
        self.assert_config_untouched(self.write(b""))

    def test_a_config_that_arrives_cut_short_changes_nothing(self):
        self.assert_config_untouched(self.write(self.edited[: len(self.edited) // 2]))


class StepTests(unittest.TestCase):
    """The steps against a stand-in for the node, so no SSH is needed."""

    def setUp(self):
        self.node = load(NODE_LIST)[1]
        self.config = DUAL_CONFIG
        self.archives = []
        self.managed = False
        self.install_error = None
        self.write_error = None
        self.steps = []
        self.drop_in = b""
        self.real_remote = fleet.remote
        fleet.remote = self.fake_remote
        self.addCleanup(lambda: setattr(fleet, "remote", self.real_remote))

    def fake_remote(self, node, script, *arguments, stdin=b""):
        self.steps.append(script)
        if script.startswith("systemctl show"):
            return "{ argv[]=/usr/local/bin/zakurad -c /etc/zakura/zakura.toml start ; }"
        if script.startswith("cat "):
            return self.config
        if script == fleet.WRITE_CONFIG_SCRIPT:
            if self.write_error:
                raise fleet.FleetError(self.write_error)
            self.assertEqual(arguments, ("/etc/zakura/zakura.toml", hashlib.sha256(stdin).hexdigest()))
            self.config = stdin.decode()
            return ""
        if script == fleet.REMOVE_PEER_NAMES_SCRIPT:
            self.assertEqual(arguments, (self.node.service_name,))
            self.assertIn("trace_peer_names", tomllib.loads(self.config)["network"]["zakura"])
            return ""
        if script == fleet.CLAIM_TRACE_SCRIPT:
            self.assertEqual(arguments, (fleet.TRACE_DIR,))
            self.managed = True
            return ""
        if script == fleet.READ_TRACE_MARKER_SCRIPT:
            return "managed" if self.managed else ""
        if script == fleet.WRITE_DROP_IN_SCRIPT:
            self.assertEqual(arguments, ("zakurad.service", "trace-peer-names.conf"))
            changed = stdin != self.drop_in
            self.drop_in = stdin
            return "written\n" if changed else "unchanged\n"
        if script == fleet.INSTALL_SCRIPT:
            self.assertEqual(arguments, (fleet.TRACE_DIR, "200" if self.managed else "0"))
            if self.install_error:
                raise fleet.FleetError(self.install_error)
            self.archives.append(stdin)
            return ""
        raise AssertionError(f"unexpected remote script: {script[:40]}")

    def test_the_trace_setting_is_written_once(self):
        first = fleet.ensure_trace_config(self.node, load(NODE_LIST))
        self.assertIn("updated trace settings in /etc/zakura/zakura.toml", first)
        self.assertEqual(fleet.trace_dir_of(self.config), fleet.TRACE_DIR)
        self.assertTrue(fleet.exposes_peer_addresses(self.config))
        written = self.config
        self.assertIn("already writes trace files to /mnt/data/zakura-traces", fleet.ensure_trace_config(self.node, load(NODE_LIST)))
        self.assertEqual(self.config, written)
        self.assertLess(self.steps.index(fleet.WRITE_CONFIG_SCRIPT), self.steps.index(fleet.REMOVE_PEER_NAMES_SCRIPT))
        self.assertLess(self.steps.index(fleet.INSTALL_SCRIPT), self.steps.index(fleet.WRITE_CONFIG_SCRIPT))

    def test_a_failed_install_leaves_new_trace_writes_disabled_and_can_be_retried(self):
        self.install_error = "perf is not installed on this node"
        with self.assertRaisesRegex(fleet.FleetError, "perf is not installed"):
            fleet.ensure_trace_config(self.node, load(NODE_LIST))
        self.assertEqual(self.config, DUAL_CONFIG)
        self.assertNotIn(fleet.WRITE_CONFIG_SCRIPT, self.steps)
        self.assertNotIn(fleet.REMOVE_PEER_NAMES_SCRIPT, self.steps)
        self.install_error = None
        fleet.ensure_trace_config(self.node, load(NODE_LIST))
        self.assertEqual(fleet.trace_dir_of(self.config), fleet.TRACE_DIR)

    def test_a_failed_config_write_keeps_the_old_environment_override(self):
        self.write_error = "config did not arrive whole"
        with self.assertRaisesRegex(fleet.FleetError, "did not arrive whole"):
            fleet.ensure_trace_config(self.node, load(NODE_LIST))
        self.assertEqual(self.config, DUAL_CONFIG)
        self.assertNotIn(fleet.REMOVE_PEER_NAMES_SCRIPT, self.steps)

    def test_a_single_node_deploy_still_names_the_other_fleet_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "nodes.toml"
            config.write_text(NODE_LIST)
            self.assertEqual(fleet.main(["ensure-trace-config", "--config", str(config), "--node", self.node.name]), 0)
        self.assertEqual(tomllib.loads(self.config)["network"]["zakura"]["trace_peer_names"], {"192.0.2.1": "us-east-0"})

    def test_a_retention_readiness_failure_also_prevents_enabling_traces(self):
        self.install_error = "the profiler did not confirm trace retention"
        with self.assertRaisesRegex(fleet.FleetError, "did not confirm trace retention"):
            fleet.ensure_trace_config(self.node, load(NODE_LIST))
        self.assertEqual(self.config, DUAL_CONFIG)

    def test_a_node_that_hides_peer_addresses_is_reported_and_keeps_its_choice(self):
        self.config = DUAL_CONFIG.replace("[network]\n", "[network]\nexpose_peer_addresses = false\n")
        self.assertTrue(fleet.ensure_trace_config(self.node, load(NODE_LIST)).endswith("its config keeps peer addresses hidden"))
        self.assertFalse(fleet.exposes_peer_addresses(self.config))

    def test_a_node_that_already_traces_gets_peer_addresses_after_its_profiler_is_running(self):
        self.config = fleet.with_trace_dir(DUAL_CONFIG)
        self.assertIn("updated trace settings in", fleet.ensure_trace_config(self.node, load(NODE_LIST)))
        self.assertTrue(fleet.exposes_peer_addresses(self.config))
        self.assertFalse(self.managed)
        self.assertLess(self.steps.index(fleet.INSTALL_SCRIPT), self.steps.index(fleet.WRITE_CONFIG_SCRIPT))

    def test_the_profiler_is_pointed_at_the_trace_directory_in_the_node_s_config(self):
        fleet.ensure_trace_config(self.node, load(NODE_LIST))
        self.assertEqual(fleet.install_profiler(self.node), "profiler running, reading /mnt/data/zakura-traces")
        sent = self.archives[-1]
        drop_in = tarfile.open(fileobj=io.BytesIO(sent)).extractfile("node.conf").read().decode()
        self.assertIn("--trace-dir /mnt/data/zakura-traces --trace-table-cap-mb 200", drop_in)

    def test_a_preexisting_default_trace_setting_does_not_grant_rotation_permission(self):
        self.config = fleet.with_trace_dir(DUAL_CONFIG)
        fleet.ensure_trace_config(self.node, load(NODE_LIST))
        self.assertFalse(self.managed)
        fleet.install_profiler(self.node)
        sent = self.archives[-1]
        drop_in = tarfile.open(fileobj=io.BytesIO(sent)).extractfile("node.conf").read().decode()
        self.assertIn("--trace-dir /mnt/data/zakura-traces --trace-table-cap-mb 0", drop_in)

    def test_peer_names_are_written_once_as_a_drop_in_of_the_node_service(self):
        nodes = load(NODE_LIST)
        self.assertEqual(
            fleet.set_peer_names(self.node, nodes, frozenset()), "names its 1 fleet peers once the node next starts"
        )
        self.assertIn(b"ZAKURA_TRACE_PEER_NAMES=us-east-0=192.0.2.1\n", self.drop_in)
        self.assertEqual(fleet.set_peer_names(self.node, nodes, frozenset()), "already names its 1 fleet peers")

    def test_a_private_peer_is_left_out_and_only_counted(self):
        line = fleet.set_peer_names(self.node, load(NODE_LIST), frozenset({"192.0.2.1"}))
        self.assertEqual(line, "names its 0 fleet peers once the node next starts, leaving out 1 with a private address")
        self.assertNotIn(b"192.0.2.1", self.drop_in)

    def test_no_peer_names_are_written_without_the_private_list(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml") as node_list, \
                contextlib.redirect_stderr(io.StringIO()):
            node_list.write(NODE_LIST)
            node_list.flush()
            missing = node_list.name + ".missing"
            self.assertEqual(
                fleet.main(["peer-names", "--config", node_list.name, "--private-addresses", missing]), 2
            )
        self.assertNotIn(fleet.WRITE_DROP_IN_SCRIPT, self.steps)

    def test_one_failing_node_does_not_stop_the_others(self):
        def step(node):
            if node.name == "us-east-0":
                raise fleet.FleetError("no answer")
            return "done"

        self.assertEqual(fleet.for_each_node(load(NODE_LIST), step), 1)


if __name__ == "__main__":
    unittest.main()
