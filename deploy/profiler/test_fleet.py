#!/usr/bin/env python3
"""Tests for the fleet installer: the node list, the config edit and the files it writes."""

import io
import sys
import tarfile
import tempfile
import tomllib
import unittest
from pathlib import Path

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
        command = fleet.profiler_command(self.remote, fleet.TRACE_DIR)
        self.assertEqual(command[command.index("--node-name") + 1], "archive-vct-off")
        self.assertEqual(command[command.index("--log-file") + 1], "/var/log/zakura/zakura.log")
        self.assertEqual(command[command.index("--trace-dir") + 1], "/mnt/data/zakura-traces")
        self.assertEqual(command[command.index("--trace-table-cap-mb") + 1], "200")

    def test_a_trace_directory_someone_else_chose_is_never_emptied(self):
        command = fleet.profiler_command(self.local, "/mnt/data/perftraces/raw")
        self.assertEqual(command[command.index("--trace-dir") + 1], "/mnt/data/perftraces/raw")
        self.assertEqual(command[command.index("--trace-table-cap-mb") + 1], "0")

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

    def test_a_node_is_told_every_other_node_s_name_and_address(self):
        drop_in = fleet.peer_names_drop_in(self.remote, [self.local, self.remote])
        self.assertEqual(drop_in.splitlines()[1:], ["[Service]", "Environment=ZAKURA_TRACE_PEER_NAMES=us-east-0=192.0.2.1"])

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


class StepTests(unittest.TestCase):
    """The steps against a stand-in for the node, so no SSH is needed."""

    def setUp(self):
        self.node = load(NODE_LIST)[1]
        self.config = DUAL_CONFIG
        self.archives = []
        self.drop_in = b""
        self.real_remote = fleet.remote
        fleet.remote = self.fake_remote
        self.addCleanup(lambda: setattr(fleet, "remote", self.real_remote))

    def fake_remote(self, node, script, *arguments, stdin=b""):
        if script.startswith("systemctl show"):
            return "{ argv[]=/usr/local/bin/zakurad -c /etc/zakura/zakura.toml start ; }"
        if script.startswith("cat "):
            return self.config
        if script == fleet.WRITE_CONFIG_SCRIPT:
            self.assertEqual(arguments, ("/etc/zakura/zakura.toml",))
            self.config = stdin.decode()
            return ""
        if script == fleet.WRITE_DROP_IN_SCRIPT:
            self.assertEqual(arguments, ("zakurad.service", "trace-peer-names.conf"))
            changed = stdin != self.drop_in
            self.drop_in = stdin
            return "written\n" if changed else "unchanged\n"
        if script == fleet.INSTALL_SCRIPT:
            self.archives.append(stdin)
            return ""
        raise AssertionError(f"unexpected remote script: {script[:40]}")

    def test_the_trace_setting_is_written_once(self):
        first = fleet.ensure_trace_config(self.node)
        self.assertIn("added trace_dir to /etc/zakura/zakura.toml", first)
        self.assertEqual(fleet.trace_dir_of(self.config), fleet.TRACE_DIR)
        written = self.config
        self.assertIn("already writes trace files to /mnt/data/zakura-traces", fleet.ensure_trace_config(self.node))
        self.assertEqual(self.config, written)

    def test_the_profiler_is_pointed_at_the_trace_directory_in_the_node_s_config(self):
        self.config = fleet.with_trace_dir(DUAL_CONFIG)
        self.assertEqual(fleet.install_profiler(self.node), "profiler running, reading /mnt/data/zakura-traces")
        (sent,) = self.archives
        drop_in = tarfile.open(fileobj=io.BytesIO(sent)).extractfile("node.conf").read().decode()
        self.assertIn("--trace-dir /mnt/data/zakura-traces --trace-table-cap-mb 200", drop_in)

    def test_peer_names_are_written_once_as_a_drop_in_of_the_node_service(self):
        nodes = load(NODE_LIST)
        self.assertEqual(fleet.set_peer_names(self.node, nodes), "names its 1 fleet peers once the node next starts")
        self.assertIn(b"ZAKURA_TRACE_PEER_NAMES=us-east-0=192.0.2.1\n", self.drop_in)
        self.assertEqual(fleet.set_peer_names(self.node, nodes), "already names its 1 fleet peers")

    def test_one_failing_node_does_not_stop_the_others(self):
        def step(node):
            if node.name == "us-east-0":
                raise fleet.FleetError("no answer")
            return "done"

        self.assertEqual(fleet.for_each_node(load(NODE_LIST), step), 1)


if __name__ == "__main__":
    unittest.main()
