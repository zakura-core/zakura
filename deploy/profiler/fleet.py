#!/usr/bin/env python3
"""Put the continuous profiler on every node of a fleet.

Reads the deployer's node list (`deploy/deployer/nodes*.toml`). A node takes part when
its entry says where the dashboard reaches its profiler:

    monitor = { profiler_url = "http://127.0.0.1:8098" }

Three steps, each safe to repeat:

    fleet.py ensure-trace-config --config nodes.toml [--node NAME]
        Add `trace_dir` under `[network.zakura]` in the node's own config when it has
        none, so the node writes the trace files the profiler reads for per-block
        timings. Nothing else in the file changes, and the node is not restarted: the
        setting takes effect the next time it starts.

    fleet.py install --config nodes.toml [--node NAME]
        Copy the profiler to the node, point it at that node's log and trace files,
        and start it.

    fleet.py tunnels --config nodes.toml
        Run on the dashboard host. A profiler listens only on its own host's loopback
        address; this keeps one SSH tunnel per remote node, from the port in the
        node's `profiler_url` to that profiler.

Stdlib only (Python 3.11+ for tomllib).
"""

import argparse
import copy
import io
import os
import re
import shlex
import subprocess
import sys
import tarfile
import tomllib
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROFILER_FILES = ("zakura_profiler.py", "stacks.py", "blocks.py", "page.html")
PROFILER_UNIT = "zakura-profiler.service"
TUNNEL_UNIT = "zakura-profiler-tunnel@.service"
# The archive-vct-off tunnel from before every node had one; it holds a tunnel port.
OLD_TUNNEL_UNIT = "zakura-profiler-tunnel-archive-vct-off.service"
TUNNEL_ENV_DIR = "/etc/zakura-profiler-tunnels"
# Where every profiler listens on its own host.
PROFILER_PORT = 8097
# The trace directory this tool sets. The profiler keeps the files in it small.
TRACE_DIR = "/mnt/data/zakura-traces"
TRACE_MARKER = ".zakura-profiler-managed"
TRACE_TABLE_CAP_MB = 200
ZAKURA_SECTION = re.compile(r"^\s*\[network\.zakura\]\s*(#.*)?$")
# `systemctl show -p ExecStart` prints the command line as `argv[]=... ;`.
CONFIG_ARGUMENT = re.compile(r"argv\[\]=[^;]*?\s(?:-c|--config)[ =](\S+)")
SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=20", "-o", "StrictHostKeyChecking=yes")
REMOTE_TIMEOUT_SECONDS = 180


class FleetError(Exception):
    """A step that could not be done on one node; the message says why."""


@dataclass(frozen=True)
class Node:
    name: str
    ssh_string: str
    ssh_port: int
    service_name: str
    log_file: str
    # The port in the node's `profiler_url`, on the dashboard host.
    dashboard_port: int

    @property
    def needs_tunnel(self):
        """Whether the dashboard reaches this profiler through a tunnel.

        The node on the dashboard host itself is listed with the profiler's own port.
        """
        return self.dashboard_port != PROFILER_PORT


def load_nodes(config_path):
    """The nodes of `config_path` that have a profiler, in file order."""
    with open(config_path, "rb") as handle:
        document = tomllib.load(handle)
    defaults = document.get("defaults", {})
    nodes = []
    for entry in document.get("nodes", []):
        merged = {**defaults, **entry}
        profiler_url = (merged.get("monitor") or {}).get("profiler_url", "")
        if not profiler_url:
            continue
        port = urllib.parse.urlparse(profiler_url).port
        if port is None:
            raise FleetError(f"{merged['name']}: profiler_url {profiler_url!r} names no port")
        nodes.append(
            Node(
                name=merged["name"],
                ssh_string=merged["ssh_string"],
                ssh_port=int(merged.get("port", 22)),
                service_name=merged.get("service_name") or "zakurad",
                log_file=merged.get("log_file", ""),
                dashboard_port=port,
            )
        )
    ports = [node.dashboard_port for node in nodes if node.needs_tunnel]
    if len(ports) != len(set(ports)):
        raise FleetError("two nodes share a profiler_url port")
    return nodes


# ---- the node's config ----


def parse_config(config_text):
    """A node config as a dictionary; `FleetError` when it is not valid TOML."""
    try:
        return tomllib.loads(config_text)
    except tomllib.TOMLDecodeError as error:
        raise FleetError(f"the node's config does not parse: {error}") from error


def trace_dir_of(config_text):
    """The `[network.zakura] trace_dir` a node config sets, or `None`."""
    zakura = parse_config(config_text).get("network", {}).get("zakura", {})
    return zakura.get("trace_dir") if isinstance(zakura, dict) else None


def with_trace_dir(config_text, trace_dir=TRACE_DIR):
    """`config_text` with `trace_dir` added under `[network.zakura]`.

    A config that already sets `trace_dir` is returned as it is, whatever its value.
    Otherwise one line is added below the section's heading, or the section is added
    at the end of the file. Every other byte stays, and the result is parsed to
    confirm that the new setting is the only difference; a layout this cannot edit
    safely (the section written as an inline table, say) raises `FleetError`.
    """
    if trace_dir_of(config_text) is not None:
        return config_text
    setting = f'trace_dir = "{trace_dir}"\n'
    lines = config_text.splitlines(keepends=True)
    heading = next((i for i, line in enumerate(lines) if ZAKURA_SECTION.match(line)), None)
    if heading is None:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines += ["\n", "[network.zakura]\n", setting]
    else:
        if not lines[heading].endswith("\n"):
            lines[heading] += "\n"
        lines.insert(heading + 1, setting)
    edited = "".join(lines)

    expected = copy.deepcopy(parse_config(config_text))
    expected.setdefault("network", {}).setdefault("zakura", {})["trace_dir"] = trace_dir
    try:
        after = tomllib.loads(edited)
    except tomllib.TOMLDecodeError as error:
        raise FleetError(f"adding trace_dir would break the config ({error}); add it by hand") from error
    if after != expected:
        raise FleetError("adding trace_dir would change another setting; add it by hand")
    return edited


def config_path_from_unit(exec_start):
    """The config file named on a node's `ExecStart` line, as systemd shows it."""
    found = CONFIG_ARGUMENT.search(exec_start)
    if found is None:
        raise FleetError("the node's service names no config file (-c PATH)")
    return found.group(1)


# ---- the profiler's files ----


def profiler_command(node, trace_dir, *, manage_traces=False):
    """The profiler's command line for `node`, whose config sets `trace_dir` (or none).

    Only a directory explicitly claimed by this installer may be emptied. An existing
    setting, including one equal to TRACE_DIR, does not grant that permission.
    """
    command = [
        "/usr/bin/python3", "/opt/zakura-profiler/zakura_profiler.py",
        "--node-name", node.name,
        "--service", node.service_name,
        "--data-dir", "/mnt/data/zakura-profiler",
        "--scratch-dir", "/run/zakura-profiler",
    ]
    if node.log_file:
        command += ["--log-file", node.log_file]
    if trace_dir:
        cap = TRACE_TABLE_CAP_MB if manage_traces else 0
        command += ["--trace-dir", trace_dir, "--trace-table-cap-mb", str(cap)]
    command += ["--host", "127.0.0.1", "--port", str(PROFILER_PORT)]
    return command


def unit_drop_in(node, trace_dir, *, manage_traces=False):
    """A systemd drop-in that starts the profiler with this node's own settings."""
    return (
        "# Written by deploy/profiler/fleet.py: this node's name, log and trace files.\n"
        "[Service]\n"
        "ExecStart=\n"
        f"ExecStart={shlex.join(profiler_command(node, trace_dir, manage_traces=manage_traces))}\n"
    )


def tunnel_env(node):
    """The settings one instance of the tunnel unit reads for `node`."""
    return f"LOCAL_PORT={node.dashboard_port}\nSSH_TARGET={node.ssh_string}\nSSH_PORT={node.ssh_port}\n"


def profiler_archive(drop_in):
    """A tar of the files a node needs: the profiler, its unit and this node's drop-in."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name in (*PROFILER_FILES, PROFILER_UNIT):
            archive.add(HERE / name, arcname=name)
        member = tarfile.TarInfo("node.conf")
        data = drop_in.encode()
        member.size = len(data)
        archive.addfile(member, io.BytesIO(data))
    return buffer.getvalue()


INSTALL_SCRIPT = f"""
set -euo pipefail
stage="$(mktemp -d /tmp/zakura-profiler.XXXXXXXX)"
trap 'rm -rf "$stage"' EXIT
tar -xf - -C "$stage"
command -v perf >/dev/null || {{ echo "perf is not installed on this node" >&2; exit 3; }}
if ! command -v c++filt >/dev/null; then
    # The profiler turns symbol names readable with c++filt, from binutils.
    DEBIAN_FRONTEND=noninteractive apt-get install -y -q binutils </dev/null >/dev/null
fi
install -d -m 755 /opt/zakura-profiler /etc/systemd/system/{PROFILER_UNIT}.d
(cd "$stage" && install -m 644 {" ".join(PROFILER_FILES)} /opt/zakura-profiler/)
install -m 644 "$stage/{PROFILER_UNIT}" /etc/systemd/system/{PROFILER_UNIT}
install -m 644 "$stage/node.conf" /etc/systemd/system/{PROFILER_UNIT}.d/node.conf
systemctl daemon-reload
systemctl enable --quiet {PROFILER_UNIT}
systemctl restart {PROFILER_UNIT}
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if curl -fsS --max-time 3 http://127.0.0.1:{PROFILER_PORT}/api/status >/dev/null 2>&1; then
        exit 0
    fi
    sleep 1
done
echo "the profiler did not answer on 127.0.0.1:{PROFILER_PORT}" >&2
exit 4
"""

# Replaces the config in place, keeping its owner and mode, and keeps the first
# original beside it.
WRITE_CONFIG_SCRIPT = """
set -euo pipefail
config="$1"
cp -p "$config" "$config.tmp-trace-dir"
cat > "$config.tmp-trace-dir"
[ -e "$config.before-trace-dir" ] || cp -p "$config" "$config.before-trace-dir"
mv "$config.tmp-trace-dir" "$config"
"""

# Claim only a new directory; never grant rotation permission over existing files.
# The marker survives failed installs and later runs without relying on the path name.
CLAIM_TRACE_SCRIPT = f"""
set -euo pipefail
marker="$1/{TRACE_MARKER}"
if [ -f "$marker" ]; then
    exit 0
fi
mkdir -- "$1"
: > "$marker"
"""

READ_TRACE_MARKER_SCRIPT = f'if [ -f "$1/{TRACE_MARKER}" ]; then printf managed; fi'


# ---- running things on a node ----


def remote(node, script, *arguments, stdin=b""):
    """Run a bash `script` on `node` as its SSH user; return what it printed.

    Raises `FleetError` with the script's error output when it fails.
    """
    command = shlex.join(["bash", "-c", script, "fleet", *arguments])
    try:
        done = subprocess.run(
            ["ssh", *SSH_OPTIONS, "-p", str(node.ssh_port), node.ssh_string, command],
            input=stdin, capture_output=True, timeout=REMOTE_TIMEOUT_SECONDS, check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise FleetError(f"no answer within {REMOTE_TIMEOUT_SECONDS} s") from error
    if done.returncode != 0:
        detail = done.stderr.decode(errors="replace").strip().splitlines()
        raise FleetError(detail[-1] if detail else f"exit status {done.returncode}")
    return done.stdout.decode(errors="replace")


def read_node_config(node):
    """The path and text of the config the node's service starts with."""
    exec_start = remote(node, 'systemctl show -p ExecStart --value "$1"', node.service_name)
    path = config_path_from_unit(exec_start)
    return path, remote(node, 'cat "$1"', path)


def ensure_trace_config(node):
    """Make the node write trace files; returns a line saying what was done."""
    path, text = read_node_config(node)
    edited = with_trace_dir(text)
    if edited == text:
        return f"already writes trace files to {trace_dir_of(text)}"
    remote(node, CLAIM_TRACE_SCRIPT, TRACE_DIR)
    remote(node, WRITE_CONFIG_SCRIPT, path, stdin=edited.encode())
    if remote(node, 'cat "$1"', path) != edited:
        raise FleetError(f"{path} does not hold what was written")
    return f"added trace_dir to {path}; it takes effect when the node next starts"


def install_profiler(node):
    """Install and start the profiler on the node; returns a line saying what was done."""
    _, text = read_node_config(node)
    trace_dir = trace_dir_of(text)
    managed = bool(trace_dir and remote(node, READ_TRACE_MARKER_SCRIPT, trace_dir) == "managed")
    remote(node, INSTALL_SCRIPT, stdin=profiler_archive(unit_drop_in(node, trace_dir, manage_traces=managed)))
    return "profiler running" + (f", reading {trace_dir}" if trace_dir else ", no trace files")


def run_local(command, stdin=None):
    """Run a command on this host as root."""
    if os.geteuid() != 0:
        command = ["sudo", *command]
    subprocess.run(command, input=stdin, check=True)


def install_tunnels(nodes):
    """Keep one tunnel on this host for every node whose profiler is on another host."""
    run_local(["install", "-d", "-m", "755", TUNNEL_ENV_DIR])
    run_local(["install", "-m", "644", str(HERE / TUNNEL_UNIT), f"/etc/systemd/system/{TUNNEL_UNIT}"])
    if os.path.exists(f"/etc/systemd/system/{OLD_TUNNEL_UNIT}"):
        run_local(["systemctl", "disable", "--now", "--quiet", OLD_TUNNEL_UNIT])
        run_local(["rm", "-f", f"/etc/systemd/system/{OLD_TUNNEL_UNIT}"])
    run_local(["systemctl", "daemon-reload"])
    for node in nodes:
        if not node.needs_tunnel:
            continue
        run_local(["dd", "status=none", f"of={TUNNEL_ENV_DIR}/{node.name}.env"], stdin=tunnel_env(node).encode())
        instance = TUNNEL_UNIT.replace("@", f"@{node.name}")
        run_local(["systemctl", "enable", "--quiet", instance])
        run_local(["systemctl", "restart", instance])
        print(f"{node.name}: tunnel on 127.0.0.1:{node.dashboard_port}")


def for_each_node(nodes, step):
    """Run `step` on every node, report each result, and return how many failed."""
    failed = 0
    for node in nodes:
        try:
            print(f"{node.name}: {step(node)}", flush=True)
        except FleetError as error:
            failed += 1
            print(f"{node.name}: FAILED: {error}", file=sys.stderr, flush=True)
    return failed


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    steps = parser.add_subparsers(dest="step", required=True)
    for name in ("ensure-trace-config", "install"):
        step = steps.add_parser(name)
        step.add_argument("--config", required=True, help="the deployer's node list")
        step.add_argument("--node", default="", help="only this node; all profiled nodes when empty")
    tunnels = steps.add_parser("tunnels")
    tunnels.add_argument("--config", required=True, help="the deployer's node list")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        nodes = load_nodes(args.config)
    except FleetError as error:
        print(f"{args.config}: {error}", file=sys.stderr)
        return 2
    if args.step == "tunnels":
        install_tunnels(nodes)
        return 0
    if args.node:
        nodes = [node for node in nodes if node.name == args.node]
        if not nodes:
            print(f"{args.node} has no profiler in {args.config}; nothing to do")
            return 0
    step = ensure_trace_config if args.step == "ensure-trace-config" else install_profiler
    return 1 if for_each_node(nodes, step) else 0


if __name__ == "__main__":
    sys.exit(main())
