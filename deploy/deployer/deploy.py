#!/usr/bin/env python3
"""Deploy zakurad to a fleet of nodes and collect their logs.

Stdlib only (Python 3.11+ for tomllib). No third-party dependencies.

The tool reads a node config (name / ssh_string / commit per node), builds the
zakurad binary from each node's commit (reusing a cache keyed on the resolved
commit SHA), distributes the binary, installs+restarts a systemd service that
logs to a deterministic file, and pulls those logs back on demand.

See deploy/deployer/README.md for usage and the example config.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import shlex
import shutil
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = SCRIPT_DIR / "templates"
BUILD_CACHE_DIR = SCRIPT_DIR / ".build-cache"

# ssh/scp options shared by every remote call. BatchMode avoids interactive
# password prompts hanging a parallel deploy; accept-new pins unknown host keys
# on first contact without failing (operator convenience for fresh droplets).
SSH_COMMON_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=15",
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ServerAliveInterval=30",
]

DEFAULTS = {
    "deploy_kind": "systemd",
    "service_name": "zakurad",
    "legacy_service_name": "",
    "bin_path": "/usr/local/bin/zakurad",
    "legacy_bin_path": "",
    "config_path": "/etc/zakura/zakura.toml",
    "log_file": "/var/log/zakura/zakura.log",
    "state_cache_dir": "/var/lib/zakura",
    "network": "Mainnet",
    "listen_addr": "[::]:8233",
    "network_cache_dir": "",
    "cache_dir_migrate_from": "",
    "rpc_listen_addr": "",  # empty -> RPC stays disabled
    "rpc_enable_cookie_auth": None,
    "port": None,           # ssh port; None -> ssh default
    # Match zakurad's own defaults so existing fleets render unchanged.
    "storage_mode": "archive",
    "v2_p2p": True,
    "legacy_p2p": True,
    "metrics_endpoint": "",  # e.g. "127.0.0.1:9100" -> renders [metrics]; "" omits it
    "tracing_filter": "",    # e.g. "info,zebra_network::zakura=debug"; "" uses zakurad default
    "checkpoint_sync": True,
    # Setting this false keeps checkpoint sync on while selecting the legacy non-VCT path.
    "vct_fast_sync": True,
    # Optional fleet-wide [defaults.zakura] table -> rendered [network.zakura].
    # Keys: dev_network, listen_addr, bootstrap_peers. Absent -> no section.
    "zakura": None,
    # Process deploys are for manually supervised nodes, like the testnet
    # zcashd-compat Zakura sidecar, where systemd would fight the local runbook.
    "working_dir": "",
    "start_command": "",
    "process_pattern": "",
    "legacy_process_pattern": "",
}


class DeployError(Exception):
    """Operator-facing failure; printed without a traceback."""


@dataclass
class Node:
    name: str
    ssh_string: str
    commit: str
    deploy_kind: str
    service_name: str
    legacy_service_name: str
    bin_path: str
    legacy_bin_path: str
    config_path: str
    log_file: str
    state_cache_dir: str
    network: str
    listen_addr: str
    network_cache_dir: str
    cache_dir_migrate_from: str
    rpc_listen_addr: str
    rpc_enable_cookie_auth: object
    storage_mode: str
    v2_p2p: bool
    legacy_p2p: bool
    metrics_endpoint: str
    tracing_filter: str
    checkpoint_sync: bool
    vct_fast_sync: bool
    zakura: object  # dict | None: fleet-wide [network.zakura] settings
    working_dir: str
    start_command: str
    process_pattern: str
    legacy_process_pattern: str
    port: object = None
    # resolved at runtime
    sha: str = ""

    def ssh_cmd(self, *remote: str) -> list[str]:
        cmd = ["ssh", *SSH_COMMON_OPTS]
        if self.port:
            cmd += ["-p", str(self.port)]
        cmd += [self.ssh_string, *remote]
        return cmd

    def scp_to(self, local: str, remote_path: str) -> list[str]:
        cmd = ["scp", *SSH_COMMON_OPTS]
        if self.port:
            cmd += ["-P", str(self.port)]
        cmd += [local, f"{self.ssh_string}:{remote_path}"]
        return cmd

    def scp_from(self, remote_path: str, local: str) -> list[str]:
        cmd = ["scp", *SSH_COMMON_OPTS]
        if self.port:
            cmd += ["-P", str(self.port)]
        cmd += [f"{self.ssh_string}:{remote_path}", local]
        return cmd


# --------------------------------------------------------------------------- #
# Config loading
# --------------------------------------------------------------------------- #

def load_nodes(config_path: Path, only: list[str] | None) -> list[Node]:
    if not config_path.is_file():
        raise DeployError(f"config not found: {config_path}")
    with config_path.open("rb") as fh:
        data = tomllib.load(fh)

    defaults = dict(DEFAULTS)
    defaults.update(data.get("defaults", {}))

    raw_nodes = data.get("nodes", [])
    if not raw_nodes:
        raise DeployError(f"no [[nodes]] defined in {config_path}")

    nodes: list[Node] = []
    seen: set[str] = set()
    for raw in raw_nodes:
        for required in ("name", "ssh_string", "commit"):
            if required not in raw:
                raise DeployError(f"node missing required field '{required}': {raw}")
        name = raw["name"]
        if name in seen:
            raise DeployError(f"duplicate node name: {name}")
        seen.add(name)
        merged = dict(defaults)
        merged.update(raw)
        nodes.append(Node(
            name=name,
            ssh_string=merged["ssh_string"],
            commit=merged["commit"],
            deploy_kind=merged["deploy_kind"],
            service_name=merged["service_name"],
            legacy_service_name=merged["legacy_service_name"],
            bin_path=merged["bin_path"],
            legacy_bin_path=merged["legacy_bin_path"],
            config_path=merged["config_path"],
            log_file=merged["log_file"],
            state_cache_dir=merged["state_cache_dir"],
            network=merged["network"],
            listen_addr=merged["listen_addr"],
            network_cache_dir=merged["network_cache_dir"],
            cache_dir_migrate_from=merged["cache_dir_migrate_from"],
            rpc_listen_addr=merged["rpc_listen_addr"],
            rpc_enable_cookie_auth=merged["rpc_enable_cookie_auth"],
            storage_mode=merged["storage_mode"],
            v2_p2p=merged["v2_p2p"],
            legacy_p2p=merged["legacy_p2p"],
            metrics_endpoint=merged["metrics_endpoint"],
            tracing_filter=merged["tracing_filter"],
            checkpoint_sync=merged["checkpoint_sync"],
            vct_fast_sync=merged["vct_fast_sync"],
            zakura=merged.get("zakura"),
            working_dir=merged["working_dir"],
            start_command=merged["start_command"],
            process_pattern=merged["process_pattern"],
            legacy_process_pattern=merged["legacy_process_pattern"],
            port=merged["port"],
        ))

    if only:
        wanted = set(only)
        unknown = wanted - {n.name for n in nodes}
        if unknown:
            raise DeployError(f"unknown --node name(s): {', '.join(sorted(unknown))}")
        nodes = [n for n in nodes if n.name in wanted]
    return nodes


# --------------------------------------------------------------------------- #
# Shell helpers
# --------------------------------------------------------------------------- #

def run(cmd: list[str], *, cwd: Path | None = None, capture: bool = False,
        check: bool = True) -> subprocess.CompletedProcess:
    """Run a command, streaming or capturing output. Raises DeployError on failure."""
    printable = " ".join(shlex.quote(c) for c in cmd)
    try:
        result = subprocess.run(
            cmd, cwd=cwd, check=check, text=True,
            capture_output=capture,
        )
    except subprocess.CalledProcessError as exc:
        detail = ""
        if capture:
            detail = (exc.stderr or exc.stdout or "").strip()
        raise DeployError(f"command failed ({exc.returncode}): {printable}\n{detail}") from exc
    return result


def repo_root() -> Path:
    result = run(["git", "rev-parse", "--show-toplevel"], cwd=SCRIPT_DIR, capture=True)
    return Path(result.stdout.strip())


# --------------------------------------------------------------------------- #
# Build (cache keyed on resolved commit SHA)
# --------------------------------------------------------------------------- #

def resolve_sha(root: Path, commit: str) -> str:
    """Resolve a branch/tag/SHA to a full commit SHA in the repo.

    Tries the ref as written first, then `origin/<ref>` so a config can name a
    branch that only exists as a remote-tracking ref (the common operator case).
    """
    for candidate in (commit, f"origin/{commit}"):
        result = run(["git", "rev-parse", "--verify", "--quiet", f"{candidate}^{{commit}}"],
                     cwd=root, capture=True, check=False)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    raise DeployError(
        f"cannot resolve commit '{commit}' (also tried origin/{commit}). "
        f"Fetch it first: git fetch origin {commit}"
    )


def cached_binary(sha: str) -> Path:
    return BUILD_CACHE_DIR / f"zakurad-{sha}"


def binary_is_runnable(binary: Path) -> bool:
    """Sanity-check that a cached binary is a valid, runnable zakurad.

    We can't verify the commit from `--version` (it prints clean semver without
    the git SHA), so the cache key (the SHA-named filename) is what ties a cached
    binary to its commit. This only guards against a truncated/corrupt cache file.
    """
    try:
        run([str(binary), "--version"], capture=True, check=True)
        return True
    except DeployError:
        return False


def build_commit(root: Path, sha: str, *, force: bool = False) -> Path:
    """Build zakurad at `sha` into the cache, or reuse an existing cached build."""
    BUILD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    target = cached_binary(sha)
    if target.exists() and not force:
        if binary_is_runnable(target):
            print(f"[build] reusing cached binary for {sha[:9]} -> {target.name}")
            return target
        print(f"[build] cached binary for {sha[:9]} is corrupt, rebuilding")

    # Build at the exact commit in a throwaway detached worktree so the caller's
    # working tree (which may be dirty) is never disturbed.
    work = BUILD_CACHE_DIR / f"wt-{sha[:12]}"
    if work.exists():
        run(["git", "worktree", "remove", "--force", str(work)], cwd=root, check=False)
        shutil.rmtree(work, ignore_errors=True)
    print(f"[build] checking out {sha[:9]} into {work.name}")
    run(["git", "worktree", "add", "--detach", str(work), sha], cwd=root)
    try:
        print(f"[build] cargo build --release -p zebrad ({sha[:9]}) ...")
        run(["cargo", "build", "--release", "--locked", "-p", "zebrad"], cwd=work)
        # Respect CARGO_TARGET_DIR (set per-worktree or shared) when locating the
        # output, falling back to the in-worktree target dir.
        target_dir = os.environ.get("CARGO_TARGET_DIR")
        built = (Path(target_dir) if target_dir else work / "target") / "release" / "zakurad"
        if not built.is_file():
            raise DeployError(f"expected binary not found after build: {built}")
        tmp = target.with_suffix(".tmp")
        shutil.copy2(built, tmp)
        os.chmod(tmp, 0o755)
        tmp.replace(target)
        print(f"[build] cached -> {target}")
    finally:
        run(["git", "worktree", "remove", "--force", str(work)], cwd=root, check=False)
        shutil.rmtree(work, ignore_errors=True)
    return target


def build_nodes(nodes: list[Node], *, force: bool = False) -> dict[str, Path]:
    """Resolve + build every distinct commit once. Returns sha -> binary path."""
    root = repo_root()
    by_sha: dict[str, Path] = {}
    for node in nodes:
        node.sha = resolve_sha(root, node.commit)
    for sha in dict.fromkeys(n.sha for n in nodes):  # unique, order-preserving
        by_sha[sha] = build_commit(root, sha, force=force)
    return by_sha


# --------------------------------------------------------------------------- #
# Template rendering
# --------------------------------------------------------------------------- #

def render_template(name: str, subst: dict[str, str]) -> str:
    text = (TEMPLATES_DIR / name).read_text()
    for key, value in subst.items():
        text = text.replace("{{" + key + "}}", value)
    return text


def render_zakura_block(zakura: object) -> str:
    """Render a fleet-wide [network.zakura] section from a dict, or "" if unset.

    Recognises `dev_network` (str), `listen_addr` (str), and `bootstrap_peers`
    (list of `node_id@addr` strings). Unknown keys are passed through verbatim so
    the deployer does not need to learn every Zakura field.
    """
    if not zakura:
        return ""
    lines = ["[network.zakura]"]
    for key, value in zakura.items():
        if isinstance(value, bool):
            lines.append(f"{key} = {'true' if value else 'false'}")
        elif isinstance(value, (int, float)):
            lines.append(f"{key} = {value}")
        elif isinstance(value, list):
            if value:
                items = "".join(f'    "{v}",\n' for v in value)
                lines.append(f"{key} = [\n{items}]")
            else:
                lines.append(f"{key} = []")
        else:
            lines.append(f'{key} = "{value}"')
    # Leading/trailing blank lines so the section reads cleanly between [network] and [state].
    return "\n" + "\n".join(lines) + "\n"


def render_node_config(node: Node) -> str:
    rpc_block = ""
    if node.rpc_listen_addr:
        rpc_lines = [f'listen_addr = "{node.rpc_listen_addr}"']
        if node.rpc_enable_cookie_auth is not None:
            rpc_lines.append(
                f"enable_cookie_auth = {'true' if node.rpc_enable_cookie_auth else 'false'}"
            )
        rpc_block = "\n".join(rpc_lines)
    else:
        rpc_block = "# listen_addr disabled"
    metrics_block = f'[metrics]\nendpoint_addr = "{node.metrics_endpoint}"\n' if node.metrics_endpoint else ""
    filter_line = f'filter = "{node.tracing_filter}"' if node.tracing_filter else "# filter unset (zakurad default)"
    network_cache_line = (
        f'cache_dir = "{node.network_cache_dir}"' if node.network_cache_dir else "# cache_dir unset (zakurad default)"
    )
    return render_template("zakura.toml", {
        "NETWORK": node.network,
        "LISTEN_ADDR": node.listen_addr,
        "NETWORK_CACHE_DIR": network_cache_line,
        "STATE_CACHE_DIR": node.state_cache_dir,
        "STORAGE_MODE": node.storage_mode,
        "V2_P2P": "true" if node.v2_p2p else "false",
        "LEGACY_P2P": "true" if node.legacy_p2p else "false",
        "ZAKURA_BLOCK": render_zakura_block(node.zakura),
        "METRICS_BLOCK": metrics_block,
        "TRACING_FILTER": filter_line,
        "LOG_FILE": node.log_file,
        "RPC_BLOCK": rpc_block,
        "CHECKPOINT_SYNC": "true" if node.checkpoint_sync else "false",
        "VCT_FAST_SYNC": "true" if node.vct_fast_sync else "false",
    })


def render_service(node: Node) -> str:
    return render_template("zakurad.service", {
        "SERVICE_NAME": node.service_name,
        "BIN_PATH": node.bin_path,
        "CONFIG_PATH": node.config_path,
        "LOG_FILE": node.log_file,
    })


# --------------------------------------------------------------------------- #
# Remote install script
# --------------------------------------------------------------------------- #

INSTALL_SCRIPT = r"""
set -euo pipefail

BIN_PATH={bin_path}
CONFIG_PATH={config_path}
SERVICE={service}
LEGACY_SERVICE={legacy_service}
LEGACY_BIN_PATH={legacy_bin_path}
LOG_FILE={log_file}
STATE_DIR={state_dir}
CACHE_DIR_MIGRATE_FROM={cache_dir_migrate_from}
NO_RESTART={no_restart}

mkdir -p "$(dirname "$BIN_PATH")" "$(dirname "$CONFIG_PATH")" "$(dirname "$LOG_FILE")"

# Stage uploaded artifacts (uploaded to /tmp by the deploy step).
install -m 644 /tmp/zakurad-deploy.service "/etc/systemd/system/${{SERVICE}}.service"
install -m 644 /tmp/zakurad-deploy.toml "$CONFIG_PATH"

# Back up the currently installed binary before replacing it.
if [ -x "$BIN_PATH" ]; then
    cp -a "$BIN_PATH" "${{BIN_PATH}}.bak"
fi
install -m 755 /tmp/zakurad-deploy.new "$BIN_PATH"
rm -f /tmp/zakurad-deploy.new /tmp/zakurad-deploy.service /tmp/zakurad-deploy.toml

systemctl daemon-reload
systemctl enable "$SERVICE" >/dev/null 2>&1 || true

stop_legacy_service() {{
    if [ -n "$LEGACY_SERVICE" ] && [ "$LEGACY_SERVICE" != "$SERVICE" ]; then
        systemctl stop "$LEGACY_SERVICE" || true
        systemctl disable "$LEGACY_SERVICE" >/dev/null 2>&1 || true
    fi
}}

if [ "$NO_RESTART" = "1" ]; then
    if [ -n "$CACHE_DIR_MIGRATE_FROM" ] && [ ! -e "$STATE_DIR" ] && [ -d "$CACHE_DIR_MIGRATE_FROM" ]; then
        echo "cache migration from $CACHE_DIR_MIGRATE_FROM to $STATE_DIR requires restart" >&2
        exit 1
    fi
    mkdir -p "$STATE_DIR"
    echo "installed (restart skipped)"
    exit 0
fi

if [ -n "$CACHE_DIR_MIGRATE_FROM" ] && [ "$CACHE_DIR_MIGRATE_FROM" != "$STATE_DIR" ]; then
    if [ ! -e "$STATE_DIR" ] && [ -d "$CACHE_DIR_MIGRATE_FROM" ]; then
        stop_legacy_service
        systemctl stop "$SERVICE" || true
        mkdir -p "$(dirname "$STATE_DIR")"
        mv "$CACHE_DIR_MIGRATE_FROM" "$STATE_DIR"
        echo "migrated cache dir: $CACHE_DIR_MIGRATE_FROM -> $STATE_DIR"
    elif [ -e "$STATE_DIR" ]; then
        echo "cache dir already present: $STATE_DIR"
    fi
fi
mkdir -p "$STATE_DIR"

start_service() {{
    stop_legacy_service
    systemctl stop "$SERVICE" || true

    # Some long-running testnet nodes can survive a plain systemctl restart long
    # enough for the deploy to report success while the old process keeps the
    # state DB open. Bound that window, then kill only processes running this
    # deployed binary before starting the updated unit.
    for _ in 1 2 3 4 5; do
        if ! pgrep -f "^${{BIN_PATH}}( |$)" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    pkill -TERM -f "^${{BIN_PATH}}( |$)" >/dev/null 2>&1 || true
    if [ -n "$LEGACY_BIN_PATH" ] && [ "$LEGACY_BIN_PATH" != "$BIN_PATH" ]; then
        pkill -TERM -f "^${{LEGACY_BIN_PATH}}( |$)" >/dev/null 2>&1 || true
    fi
    sleep 1
    pkill -KILL -f "^${{BIN_PATH}}( |$)" >/dev/null 2>&1 || true
    if [ -n "$LEGACY_BIN_PATH" ] && [ "$LEGACY_BIN_PATH" != "$BIN_PATH" ]; then
        pkill -KILL -f "^${{LEGACY_BIN_PATH}}( |$)" >/dev/null 2>&1 || true
    fi

    systemctl start "$SERVICE"
}}

if ! start_service; then
    echo "start failed; rolling back to ${{BIN_PATH}}.bak" >&2
    if [ -x "${{BIN_PATH}}.bak" ]; then
        install -m 755 "${{BIN_PATH}}.bak" "$BIN_PATH"
        start_service || true
    fi
    exit 1
fi

sleep 2
systemctl is-active "$SERVICE"
"$BIN_PATH" --version || true
"""


PROCESS_INSTALL_SCRIPT = r"""
set -euo pipefail

BIN_PATH={bin_path}
CONFIG_PATH={config_path}
LOG_FILE={log_file}
STATE_DIR={state_dir}
CACHE_DIR_MIGRATE_FROM={cache_dir_migrate_from}
WORKING_DIR={working_dir}
START_COMMAND={start_command}
PROCESS_PATTERN={process_pattern}
LEGACY_PROCESS_PATTERN={legacy_process_pattern}
NO_RESTART={no_restart}

mkdir -p "$(dirname "$BIN_PATH")" "$(dirname "$CONFIG_PATH")"
if [ -n "$LOG_FILE" ]; then
    mkdir -p "$(dirname "$LOG_FILE")"
fi
if [ -n "$WORKING_DIR" ]; then
    mkdir -p "$WORKING_DIR"
fi

install -m 644 /tmp/zakurad-deploy.toml "$CONFIG_PATH"

if [ -x "$BIN_PATH" ]; then
    cp -a "$BIN_PATH" "${{BIN_PATH}}.bak"
fi
install -m 755 /tmp/zakurad-deploy.new "$BIN_PATH"
rm -f /tmp/zakurad-deploy.new /tmp/zakurad-deploy.toml

if [ "$NO_RESTART" = "1" ]; then
    if [ -n "$CACHE_DIR_MIGRATE_FROM" ] && [ ! -e "$STATE_DIR" ] && [ -d "$CACHE_DIR_MIGRATE_FROM" ]; then
        echo "cache migration from $CACHE_DIR_MIGRATE_FROM to $STATE_DIR requires restart" >&2
        exit 1
    fi
    mkdir -p "$STATE_DIR"
    echo "installed process binary/config (restart skipped)"
    exit 0
fi

if [ -z "$START_COMMAND" ] || [ -z "$PROCESS_PATTERN" ]; then
    echo "process deploy requires start_command and process_pattern" >&2
    exit 1
fi

if pgrep -f "$PROCESS_PATTERN" >/dev/null 2>&1; then
    pkill -TERM -f "$PROCESS_PATTERN" >/dev/null 2>&1 || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        if ! pgrep -f "$PROCESS_PATTERN" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    pkill -KILL -f "$PROCESS_PATTERN" >/dev/null 2>&1 || true
fi
if [ -n "$LEGACY_PROCESS_PATTERN" ] && [ "$LEGACY_PROCESS_PATTERN" != "$PROCESS_PATTERN" ]; then
    pkill -TERM -f "$LEGACY_PROCESS_PATTERN" >/dev/null 2>&1 || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        if ! pgrep -f "$LEGACY_PROCESS_PATTERN" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    pkill -KILL -f "$LEGACY_PROCESS_PATTERN" >/dev/null 2>&1 || true
fi

if [ -n "$CACHE_DIR_MIGRATE_FROM" ] && [ "$CACHE_DIR_MIGRATE_FROM" != "$STATE_DIR" ]; then
    if [ ! -e "$STATE_DIR" ] && [ -d "$CACHE_DIR_MIGRATE_FROM" ]; then
        mkdir -p "$(dirname "$STATE_DIR")"
        mv "$CACHE_DIR_MIGRATE_FROM" "$STATE_DIR"
        echo "migrated cache dir: $CACHE_DIR_MIGRATE_FROM -> $STATE_DIR"
    elif [ -e "$STATE_DIR" ]; then
        echo "cache dir already present: $STATE_DIR"
    fi
fi
mkdir -p "$STATE_DIR"

if [ -n "$WORKING_DIR" ]; then
    cd "$WORKING_DIR"
fi

launcher_log="${{LOG_FILE:-/tmp/zakurad-process-deploy}}.launcher"
nohup bash -lc "$START_COMMAND" >> "$launcher_log" 2>&1 &
sleep 3

if ! pgrep -f "$PROCESS_PATTERN" >/dev/null 2>&1; then
    echo "process failed to start; rolling back to ${{BIN_PATH}}.bak" >&2
    if [ -x "${{BIN_PATH}}.bak" ]; then
        install -m 755 "${{BIN_PATH}}.bak" "$BIN_PATH"
        nohup bash -lc "$START_COMMAND" >> "$launcher_log" 2>&1 &
        sleep 3
    fi
    pgrep -f "$PROCESS_PATTERN" >/dev/null 2>&1
fi

"$BIN_PATH" --version || true
"""


def ssh_with_stdin(node: Node, script: str) -> subprocess.CompletedProcess:
    """Run an install script on the node via `ssh ... bash -s`, feeding it on stdin."""
    return subprocess.run(node.ssh_cmd("bash", "-s"), input=script, text=True)


def ssh_capture_script(node: Node, script: str) -> subprocess.CompletedProcess:
    """Run a script on the node via `ssh ... bash -s` and capture its output.

    Feeding the script on stdin avoids ssh's argv flattening, which otherwise
    collapses `bash -c '<multi-word>'` into `bash -c <firstword>` on the remote.
    """
    return subprocess.run(node.ssh_cmd("bash", "-s"), input=script,
                          text=True, capture_output=True)


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #

def cmd_build(args) -> int:
    nodes = load_nodes(Path(args.config), args.node)
    build_nodes(nodes, force=args.force)
    return 0


def cmd_deploy(args) -> int:
    nodes = load_nodes(Path(args.config), args.node)
    by_sha = build_nodes(nodes, force=args.force)

    results: list[tuple[str, bool, str]] = []

    def work(node: Node) -> tuple[str, bool, str]:
        binary = by_sha[node.sha]
        try:
            if node.deploy_kind not in ("systemd", "process"):
                return (node.name, False, f"unknown deploy_kind: {node.deploy_kind}")
            cfg = render_node_config(node)
            cfg_tmp = BUILD_CACHE_DIR / f".cfg-{node.name}.toml"
            cfg_tmp.write_text(cfg)
            try:
                run(node.scp_to(str(binary), "/tmp/zakurad-deploy.new"), capture=True)
                run(node.scp_to(str(cfg_tmp), "/tmp/zakurad-deploy.toml"), capture=True)
                if node.deploy_kind == "systemd":
                    unit = render_service(node)
                    unit_tmp = BUILD_CACHE_DIR / f".unit-{node.name}.service"
                    unit_tmp.write_text(unit)
                    try:
                        run(node.scp_to(str(unit_tmp), "/tmp/zakurad-deploy.service"), capture=True)
                    finally:
                        unit_tmp.unlink(missing_ok=True)
            finally:
                cfg_tmp.unlink(missing_ok=True)

            if node.deploy_kind == "systemd":
                script = INSTALL_SCRIPT.format(
                    bin_path=shlex.quote(node.bin_path),
                    config_path=shlex.quote(node.config_path),
                    service=shlex.quote(node.service_name),
                    legacy_service=shlex.quote(node.legacy_service_name),
                    legacy_bin_path=shlex.quote(node.legacy_bin_path),
                    log_file=shlex.quote(node.log_file),
                    state_dir=shlex.quote(node.state_cache_dir),
                    cache_dir_migrate_from=shlex.quote(node.cache_dir_migrate_from),
                    no_restart="1" if args.no_restart else "0",
                )
            else:
                script = PROCESS_INSTALL_SCRIPT.format(
                    bin_path=shlex.quote(node.bin_path),
                    config_path=shlex.quote(node.config_path),
                    log_file=shlex.quote(node.log_file),
                    state_dir=shlex.quote(node.state_cache_dir),
                    cache_dir_migrate_from=shlex.quote(node.cache_dir_migrate_from),
                    working_dir=shlex.quote(node.working_dir),
                    start_command=shlex.quote(node.start_command),
                    process_pattern=shlex.quote(node.process_pattern),
                    legacy_process_pattern=shlex.quote(node.legacy_process_pattern),
                    no_restart="1" if args.no_restart else "0",
                )
            proc = ssh_with_stdin(node, script)
            if proc.returncode != 0:
                return (node.name, False, f"install/restart failed (rc={proc.returncode})")
            return (node.name, True, f"deployed {node.sha[:9]}")
        except DeployError as exc:
            return (node.name, False, str(exc))

    print(f"[deploy] distributing to {len(nodes)} node(s)...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as pool:
        for res in pool.map(work, nodes):
            results.append(res)

    print("\n=== deploy summary ===")
    failed = 0
    for name, ok, msg in results:
        status = "OK  " if ok else "FAIL"
        if not ok:
            failed += 1
        print(f"  [{status}] {name}: {msg}")
    return 1 if failed else 0


def cmd_logs_fetch(args) -> int:
    nodes = load_nodes(Path(args.config), args.node)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    def work(node: Node) -> tuple[str, bool, str]:
        dest = out_dir / f"{node.name}.log"
        try:
            if args.lines:
                # Tail N lines remotely to avoid copying huge files.
                cmd = node.ssh_cmd("tail", "-n", str(args.lines), node.log_file)
                proc = subprocess.run(cmd, text=True, capture_output=True)
                if proc.returncode != 0:
                    return (node.name, False, proc.stderr.strip() or "tail failed")
                dest.write_text(proc.stdout)
            else:
                run(node.scp_from(node.log_file, str(dest)), capture=True)
            return (node.name, True, str(dest))
        except DeployError as exc:
            return (node.name, False, str(exc))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as pool:
        for res in pool.map(work, nodes):
            results.append(res)

    failed = 0
    for name, ok, msg in results:
        if ok:
            print(f"  [OK  ] {name}: -> {msg}")
        else:
            failed += 1
            print(f"  [FAIL] {name}: {msg}")
    return 1 if failed else 0


def cmd_logs_follow(args) -> int:
    nodes = load_nodes(Path(args.config), args.node)
    if len(nodes) != 1:
        raise DeployError("logs follow requires exactly one --node")
    node = nodes[0]
    lines = str(args.lines) if args.lines else "50"
    cmd = node.ssh_cmd("tail", "-n", lines, "-F", node.log_file)
    print(f"[follow] {node.name}: tail -F {node.log_file} (Ctrl-C to stop)")
    try:
        return subprocess.run(cmd).returncode
    except KeyboardInterrupt:
        return 0


def cmd_status(args) -> int:
    nodes = load_nodes(Path(args.config), args.node)

    def work(node: Node) -> tuple[str, str]:
        # `zakurad --version` prints clean semver (e.g. "zakurad 5.0.0-rc.3") with no
        # commit, so also read the running build's git commit from the startup
        # diagnostic line in the node's log (`git commit: <sha>`). The configured
        # ref is appended so requested-vs-running is visible at a glance.
        if node.service_name:
            service_probe = f"systemctl is-active {shlex.quote(node.service_name)} 2>/dev/null"
        elif node.process_pattern:
            service_probe = (
                f"pgrep -f {shlex.quote(node.process_pattern)} >/dev/null 2>&1 "
                "&& printf 'active\\n' || printf 'inactive\\n'"
            )
        else:
            service_probe = "printf 'unknown\\n'"
        log_probe = (
            f"grep -aoE 'git commit: [0-9a-f]+' {shlex.quote(node.log_file)} 2>/dev/null | tail -1"
            if node.log_file else "true"
        )
        probe = (
            f"{service_probe}; "
            f"{shlex.quote(node.bin_path)} --version 2>/dev/null | head -1; "
            f"{log_probe}"
        )
        proc = ssh_capture_script(node, probe)
        lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
        out = " | ".join(lines) if lines else (proc.stderr.strip() or "unreachable")
        return (node.name, f"{out} | cfg {node.commit}")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as pool:
        for res in pool.map(work, nodes):
            results.append(res)
    for name, out in results:
        print(f"  {name}: {out}")
    return 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="deploy.py",
        description="Build, deploy, and collect logs for a Zakura node fleet.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    def add_common(sp):
        sp.add_argument("--config", "-c", required=True, help="path to nodes config (TOML)")
        sp.add_argument("--node", "-n", action="append",
                        help="limit to node name (repeatable); default = all")

    b = sub.add_parser("build", help="build each unique commit into the cache")
    add_common(b)
    b.add_argument("--force", action="store_true", help="rebuild even if cached")
    b.set_defaults(func=cmd_build)

    d = sub.add_parser("deploy", help="build (if needed), distribute, restart service")
    add_common(d)
    d.add_argument("--force", action="store_true", help="rebuild even if cached")
    d.add_argument("--no-restart", action="store_true",
                   help="install binary/config/unit but don't restart the service")
    d.set_defaults(func=cmd_deploy)

    s = sub.add_parser("status", help="show service state + version per node")
    add_common(s)
    s.set_defaults(func=cmd_status)

    logs = sub.add_parser("logs", help="pull logs from nodes")
    logs_sub = logs.add_subparsers(dest="logs_command", required=True)

    lf = logs_sub.add_parser("fetch", help="copy each node's log file locally")
    add_common(lf)
    lf.add_argument("--out-dir", default="logs", help="local dir for <name>.log (default: logs/)")
    lf.add_argument("--lines", type=int, default=0,
                    help="only fetch the last N lines (0 = whole file)")
    lf.set_defaults(func=cmd_logs_fetch)

    lo = logs_sub.add_parser("follow", help="stream-follow one node's log file")
    add_common(lo)
    lo.add_argument("--lines", type=int, default=50, help="initial lines to show (default: 50)")
    lo.set_defaults(func=cmd_logs_follow)

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except DeployError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
