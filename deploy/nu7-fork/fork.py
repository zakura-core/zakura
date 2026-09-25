#!/usr/bin/env python3
"""Spin up and reconfigure a NU7 fork of the public Testnet.

Stdlib only (Python 3.11+ for tomllib), matching deploy/deployer/deploy.py.

The fork is an ordinary configured testnet: it inherits every public Testnet
parameter from `testnet::Parameters::build()` and overrides only its name, its
network magic, and the NU7 activation height. Its chain state is seeded from a
real Testnet cache, so it carries genuine pre-NU7 history and the measured NSM
value balance rather than starting from nothing.

Typical use:

    ./fork.py provision          # create the droplet + clone a Testnet snapshot
    ./fork.py catch-up           # sync the pristine copy to the public Testnet tip
    ./fork.py up                 # seed, plan, render, deploy
    ./fork.py status             # height and NU7 status
    ./fork.py reconfigure        # re-seed and redeploy at a new activation height

See README.md for the full runbook.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
import tomllib
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEPLOYER = REPO_ROOT / "deploy" / "deployer" / "deploy.py"
DO_PROVISION = REPO_ROOT / ".github" / "workflows" / "scripts" / "do_provision.py"
STATE_CONSTANTS = REPO_ROOT / "crates" / "zakura-state" / "src" / "constants.rs"
ACTIVATION_CONSTANTS = (
    REPO_ROOT / "crates" / "zakura-chain" / "src" / "parameters" / "constants.rs"
)

# The measured Testnet NSM value balance immediately before NU7, from
# crates/zakura-chain/src/parameters/network/subsidy/constants/testnet.rs.
# The ParametersBuilder default is zero, which is only correct for a chain with
# no pre-NU7 history. A fork of Testnet has 4M blocks of it.
TESTNET_INITIAL_NSM_VALUE_BALANCE = 55_768_414_957

SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=15",
    # Bound waits after the connection is up: a host that stops answering is
    # dropped after about two minutes of missed keepalives, while a long silent
    # remote command such as the state copy keeps running.
    "-o", "ServerAliveInterval=30",
    "-o", "ServerAliveCountMax=4",
    "-o", "StrictHostKeyChecking=accept-new",
]

# Upgrade names as ConfiguredActivationHeights serialises them, paired with the
# Rust constant that carries each height.
UPGRADE_CONSTANTS = [
    ("BeforeOverwinter", "BEFORE_OVERWINTER"),
    ("Overwinter", "OVERWINTER"),
    ("Sapling", "SAPLING"),
    ("Blossom", "BLOSSOM"),
    ("Heartwood", "HEARTWOOD"),
    ("Canopy", "CANOPY"),
    ("NU5", "NU5"),
    ("NU6", "NU6"),
    ("NU6.1", "NU6_1"),
    ("NU6.2", "NU6_2"),
    ("NU6.3", "NU6_3"),
]


class ForkError(Exception):
    """Operator-facing failure; printed without a traceback."""


# --------------------------------------------------------------------------- #
# Parameters read out of the source tree
# --------------------------------------------------------------------------- #

def db_format_version() -> int:
    """Read DATABASE_FORMAT_VERSION, which names the state directory (state/vNN)."""
    text = STATE_CONSTANTS.read_text()
    match = re.search(r"const DATABASE_FORMAT_VERSION: u64 = (\d+)", text)
    if not match:
        raise ForkError(f"could not read DATABASE_FORMAT_VERSION from {STATE_CONSTANTS}")
    return int(match[1])


def testnet_activation_heights() -> dict[str, int]:
    """Parse the public Testnet activation heights out of the source tree.

    Read rather than duplicated: `with_activation_heights` discards every height
    at or above Height(1) before applying the configured set, so an incomplete
    or stale list silently disables Sapling through NU6.3 on the fork.
    """
    text = ACTIVATION_CONSTANTS.read_text()
    match = re.search(r"pub mod testnet \{(.*?)\n    \}", text, re.DOTALL)
    if not match:
        raise ForkError(f"could not find the testnet module in {ACTIVATION_CONSTANTS}")
    body = match[1]

    heights: dict[str, int] = {}
    for name, constant in UPGRADE_CONSTANTS:
        found = re.search(
            rf"pub const {constant}: Height = Height\(([\d_]+)\)", body
        )
        if not found:
            raise ForkError(
                f"{constant} missing from {ACTIVATION_CONSTANTS}; "
                f"the fork config would silently drop upgrades above it"
            )
        heights[name] = int(found[1].replace("_", ""))
    return heights


def load_fork_config(path: Path) -> dict:
    if not path.is_file():
        raise ForkError(f"fork config not found: {path}")
    with path.open("rb") as fh:
        return tomllib.load(fh)


# --------------------------------------------------------------------------- #
# Shell helpers
# --------------------------------------------------------------------------- #

def run(cmd: list[str], *, capture: bool = False, check: bool = True,
        cwd: Path | None = None) -> subprocess.CompletedProcess:
    printable = " ".join(shlex.quote(part) for part in cmd)
    try:
        result = subprocess.run(
            cmd, cwd=cwd, text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
    except FileNotFoundError as error:
        raise ForkError(f"command not found: {cmd[0]} ({error})") from error
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise ForkError(f"command failed ({result.returncode}): {printable}\n{detail}")
    return result


def ssh(host: str, *remote: str, capture: bool = False,
        check: bool = True) -> subprocess.CompletedProcess:
    if not host:
        raise ForkError("host.ssh_string is empty; run `fork.py provision` or set it")
    return run(["ssh", *SSH_OPTS, host, *remote], capture=capture, check=check)


# --------------------------------------------------------------------------- #
# Fork parameters
# --------------------------------------------------------------------------- #

def state_dir_name(network_name: str) -> str:
    """zakurad derives the state directory from the lowercased network name."""
    return network_name.lower()


# Written into the pristine cache by `catch-up`, once the temporary node has stopped.
SEED_TIP_FILE = "seed-tip.json"

# The temporary public-Testnet node `catch-up` runs over the pristine cache.
CATCH_UP_UNIT = "zakura-fork-seed-catch-up"
CATCH_UP_CONFIG = "/etc/zakura/zakura-seed-catch-up.toml"
CATCH_UP_DEFAULTS = {
    # Off the fork nodes' ports, so it can run beside them.
    "listen_addr": "0.0.0.0:18433",
    "rpc_listen_addr": "127.0.0.1:18252",
    "log_file": "/var/log/zakura/zakura-seed-catch-up.log",
    # The seed counts as caught up once its tip block is at most this old. The
    # fork's first block may be at most 90 minutes past the median time of the
    # tip, so a tip older than that cannot be mined on.
    "max_tip_age_minutes": 20,
    "timeout_minutes": 240,
}


def rpc(host: str, rpc_addr: str, method: str, params: list | None = None) -> object:
    """Call a node's JSON-RPC on `host`, returning the result."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []})
    result = ssh(
        host,
        f"curl -s --max-time 10 -X POST http://{rpc_addr}/ "
        f"-H 'content-type: application/json' -d {shlex.quote(body)}",
        capture=True, check=False,
    )
    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ForkError(f"{method}: no JSON-RPC response from {rpc_addr}: "
                        f"{result.stdout or result.stderr}") from error
    if response.get("error"):
        raise ForkError(f"{method}: {response['error']}")
    return response["result"]


def recorded_seed_tip(config: dict) -> dict | None:
    """The tip `catch-up` recorded for the pristine cache, or None if it never ran."""
    host = config["host"]["ssh_string"]
    path = f"{config['host']['pristine_cache_dir']}/{SEED_TIP_FILE}"
    result = ssh(host, f"cat {shlex.quote(path)}", capture=True, check=False)
    if result.returncode != 0:
        return None
    try:
        tip = json.loads(result.stdout)
        return {"height": int(tip["height"]), "hash": str(tip["hash"]), "time": int(tip["time"])}
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        raise ForkError(f"{path} is malformed; re-run `fork.py catch-up`") from error


def seeded_tip_height(config: dict) -> int:
    """Read the tip of the pristine Testnet seed.

    After `catch-up`, this is the tip it recorded over RPC, which includes the
    non-finalized blocks `seed` copies. Otherwise it is the finalized tip, read with
    the node's own binary before the fork node starts, so nothing else holds the
    database.
    """
    recorded = recorded_seed_tip(config)
    if recorded is not None:
        age = (time.time() - recorded["time"]) / 60
        print(f"[plan] caught-up seed tip {recorded['height']}, block time {age:.0f} min ago")
        return recorded["height"]

    host = config["host"]["ssh_string"]
    pristine = config["host"]["pristine_cache_dir"]
    binary = config["host"].get("bin_path", "/usr/local/bin/zakurad")
    node_config = config["host"].get("config_path", "/etc/zakura/zakura.toml")

    # `tip-height` opens the database in the storage mode its config names, and defaults
    # to archive. The snapshots this seeds from are pruned, so without the node's own
    # config it panics with "this database has been pruned and cannot be opened in
    # archive storage mode".
    result = ssh(
        host,
        f"{shlex.quote(binary)} -c {shlex.quote(node_config)} "
        f"tip-height --cache-dir {shlex.quote(pristine)} --network Testnet",
        capture=True,
    )
    for line in reversed(result.stdout.strip().splitlines()):
        if line.strip().isdigit():
            return int(line.strip())
    raise ForkError(
        f"could not read a tip height from the seed at {pristine}:\n{result.stdout}"
    )


def fork_plan(config: dict, tip: int) -> dict:
    """Resolve the concrete heights this fork will run with."""
    fork = config["fork"]
    activation = tip + int(fork["activation_offset"])

    heights = testnet_activation_heights()
    highest_public = max(heights.values())
    if activation <= highest_public:
        raise ForkError(
            f"NU7 activation {activation} is not above NU6.3 ({highest_public}); "
            f"activation heights must be strictly increasing"
        )
    heights["NU7"] = activation

    return {
        "seeded_tip": tip,
        "activation": activation,
        "activation_heights": heights,
    }


def testnet_parameters(config: dict, plan: dict) -> dict:
    fork = config["fork"]
    params = {
        "network_name": fork["network_name"],
        "network_magic": list(fork["network_magic"]),
        # Without this the serde default is genesis-only checkpoints, and the
        # node would fully verify millions of blocks it already trusts.
        "checkpoints": True,
        "initial_nsm_value_balance": TESTNET_INITIAL_NSM_VALUE_BALANCE,
        "activation_heights": plan["activation_heights"],
    }
    # NSM reissuance has no configurable start height on this base, so it stays
    # unscheduled. Fee recycling still begins at NU7 activation regardless.
    return params


def render_nodes_toml(config: dict, plan: dict) -> str:
    """Emit a deploy.py fleet config describing the fork node(s)."""
    sys.path.insert(0, str(DEPLOYER.parent))
    import deploy  # noqa: E402  (path is set immediately above)

    host = config["host"]
    fork = config["fork"]
    params = testnet_parameters(config, plan)

    miner_address = config.get("miner", {}).get("address", "")
    if not miner_address:
        raise ForkError(
            "miner.address is empty in fork.toml; the node would reject "
            "getblocktemplate and the fork could never produce a block"
        )

    lines = [
        "# Generated by deploy/nu7-fork/fork.py — do not edit by hand.",
        f"# NU7 activates at {plan['activation']} "
        f"({fork['activation_offset']} blocks above the seed tip {plan['seeded_tip']}).",
        "",
        "[defaults]",
        'network = "Testnet"',
        f'listen_addr = "{host["listen_addr"]}"',
        f'rpc_listen_addr = "{host["rpc_listen_addr"]}"',
        "rpc_enable_cookie_auth = false",
        f'state_cache_dir = "{host["fork_cache_dir"]}"',
        f'network_cache_dir = "{host["fork_cache_dir"]}"',
        # Mandatory: zakurad refuses to load a config that pairs the default
        # public DNS seeds with testnet parameters incompatible with Testnet.
        "initial_testnet_peers = []",
        f'log_file = "{host.get("log_file", "/var/log/zakura/zakura-fork.log")}"',
        f'metrics_endpoint = "{host["metrics_endpoint"]}"',
        # Without a miner address the node refuses getblocktemplate, so the
        # external miner cannot produce a single block.
        f'miner_address = "{miner_address}"',
        f'storage_mode = "{host.get("storage_mode", "pruned")}"',
        # The fork has no Zakura v2 peers and must not dial the public network.
        'p2p_stack = "legacy"',
        "checkpoint_sync = true",
        "vct_fast_sync = false",
        f'tracing_filter = "{host.get("tracing_filter", "info")}"',
        "",
    ]

    lines.extend(deploy.render_toml_table("defaults.testnet_parameters", params))

    peer = config.get("peer", {})
    peer_enabled = bool(peer.get("enabled", False))

    lines.extend([
        "",
        "[[nodes]]",
        f'name = "{config["droplet"]["name"]}"',
        f'ssh_string = "{host["ssh_string"]}"',
        f'commit = "{host["commit"]}"',
    ])
    if peer_enabled:
        # Dial the validator so the miner does not depend on inbound-only
        # connectivity. These are explicit local addresses, not the public DNS
        # seeds, so build_configured_testnet's incompatible-seeds guard does
        # not fire.
        lines.append(
            f'initial_testnet_peers = ["{peer_dial_addr(peer["listen_addr"])}"]'
        )
    lines.append("")

    if peer_enabled:
        lines.extend([
            "[[nodes]]",
            f'name = "{peer["name"]}"',
            f'ssh_string = "{host["ssh_string"]}"',
            f'commit = "{host["commit"]}"',
            f'service_name = "{peer["service_name"]}"',
            f'bin_path = "{peer["bin_path"]}"',
            f'config_path = "{peer["config_path"]}"',
            f'state_cache_dir = "{peer["cache_dir"]}"',
            f'network_cache_dir = "{peer["cache_dir"]}"',
            f'log_file = "{peer["log_file"]}"',
            f'identity_dir = "{peer["identity_dir"]}"',
            f'listen_addr = "{peer["listen_addr"]}"',
            f'rpc_listen_addr = "{peer["rpc_listen_addr"]}"',
            f'metrics_endpoint = "{peer["metrics_endpoint"]}"',
            f'initial_testnet_peers = ["{peer_dial_addr(host["listen_addr"])}"]',
            # An empty address leaves this node a pure validator: without one it refuses
            # getblocktemplate, so it can only ever accept blocks the other node produced.
            # Setting one makes the two nodes compete, which is what exercises losing a
            # race and re-templating on a tip someone else mined.
            f'miner_address = "{peer.get("miner_address", "")}"',
            "",
        ])

    return "\n".join(lines)


def peer_dial_addr(listen_addr: str) -> str:
    """Turn a wildcard listen address into one the other local node can dial."""
    host_part, _, port = listen_addr.rpartition(":")
    if host_part in ("0.0.0.0", "[::]", "::", ""):
        return f"127.0.0.1:{port}"
    return listen_addr


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #

def cmd_provision(config: dict, args) -> int:
    """Create the droplet and attach a clone of the newest Testnet state snapshot."""
    droplet = config["droplet"]
    cmd = [
        sys.executable, str(DO_PROVISION),
        "--name", droplet["name"],
        "--size", droplet["size"],
        "--regions", droplet["regions"],
        "--tag", droplet["tag"],
        "--network", "testnet",
        "--mode", "tip",
        "--volume-name", droplet["volume_name"],
    ]
    fingerprint = droplet.get("ssh_fingerprint", "")
    if fingerprint:
        cmd += ["--ssh-fingerprint", fingerprint]
    elif not args.plan:
        # do_provision.py refuses to create a host without one, and a droplet no
        # key can reach would be useless anyway.
        raise ForkError(
            "droplet.ssh_fingerprint is empty in fork.toml; set it to the fingerprint "
            "of a DigitalOcean SSH key (`doctl compute ssh-key list`)"
        )
    if args.plan:
        cmd.append("--plan")

    print(f"[provision] {' '.join(shlex.quote(part) for part in cmd)}")
    result = run(cmd, capture=True)
    print(result.stdout)
    print(
        "[provision] record the droplet's address in fork.toml as host.ssh_string, "
        "then run `fork.py up`"
    )
    return 0


def mount_state_volume(config: dict) -> None:
    """Mount the cloned state volume if it is attached but not yet mounted.

    A droplet from `do_provision.py` has the volume attached, but nothing has
    mounted it: in CI that is pr-node-run.sh's job. The device path follows
    DigitalOcean's by-id convention, matching that script.

    The device is matched by prefix rather than by exact name, because
    `do_provision.py` appends the region to the volume name it allocates: asking for
    `zakura-pr-nu7-fork-state` yields `zakura-pr-nu7-fork-state-nyc1` on disk.
    """
    host = config["host"]["ssh_string"]
    mount_point = config["host"]["snapshot_mount"]
    volume = config["droplet"]["volume_name"]
    pattern = f"/dev/disk/by-id/scsi-0DO_Volume_{volume}*"

    result = ssh(
        host,
        f"if mountpoint -q {shlex.quote(mount_point)}; then echo mounted; else "
        f"for _ in $(seq 1 30); do "
        f"  set -- {pattern}; [ -e \"$1\" ] && break; sleep 2; "
        f"done; "
        f"set -- {pattern}; "
        f"test -e \"$1\" || {{ echo 'no state volume device matching {pattern}' >&2; exit 1; }}; "
        f"mkdir -p {shlex.quote(mount_point)} && mount \"$1\" {shlex.quote(mount_point)} "
        f"&& echo \"newly-mounted $1\"; fi",
        capture=True,
    )
    print(f"[seed] state volume: {result.stdout.strip()}")


def fork_nodes(config: dict) -> list[dict]:
    """The fork's zakurad nodes on the host: their deploy name, unit, and cache root."""
    nodes = [{
        "name": config["droplet"]["name"],
        # render_nodes_toml leaves the primary on deploy.py's default unit name.
        "service": config["host"].get("service_name", "zakurad"),
        "cache_dir": config["host"]["fork_cache_dir"],
    }]
    peer = config.get("peer", {})
    if peer.get("enabled", False):
        nodes.append({
            "name": peer["name"],
            "service": peer["service_name"],
            "cache_dir": peer["cache_dir"],
        })
    return nodes


def check_fork_paths(config: dict, nodes: list[dict]) -> str:
    """Refuse fork state paths that a forced re-seed must never delete.

    Returns the fork's state directory name.
    """
    name = state_dir_name(config["fork"]["network_name"])
    if not re.fullmatch(r"[a-z0-9_-]+", name) or name in ("mainnet", "testnet", "regtest"):
        raise ForkError(
            f"network_name {config['fork']['network_name']!r} must be a distinct fork name; "
            f"it names the state directories that a re-seed deletes"
        )
    pristine = Path(config["host"]["pristine_cache_dir"])
    for node in nodes:
        root = Path(node["cache_dir"])
        if not root.is_absolute() or root == Path("/"):
            raise ForkError(f"{node['name']}: cache_dir {root} must be an absolute directory")
        if root == pristine or pristine in root.parents or root in pristine.parents:
            raise ForkError(
                f"{node['name']}: cache_dir {root} overlaps the pristine seed at {pristine}"
            )
    return name


def cmd_seed(config: dict, args) -> int:
    """Copy the pristine Testnet state into every fork node's own state directory.

    The pristine copy is never modified, so a reconfigure can re-seed from it
    without touching DigitalOcean again. Each node gets its own copy: the fork
    nodes are pruned, so a node seeded from nothing could not sync the inherited
    history from its peer.
    """
    host = config["host"]["ssh_string"]
    code_version = db_format_version()
    pristine_root = config["host"]["pristine_cache_dir"]
    nodes = fork_nodes(config)
    name = check_fork_paths(config, nodes)

    mount_state_volume(config)

    # A snapshot is written at whatever database format was current when it was baked,
    # which is not necessarily the version this tree runs. One major version behind is
    # restored in place by a reusable major upgrade, so take the newest seed at or below
    # the code's version rather than demanding an exact match.
    found = ssh(
        host,
        f"ls -d {shlex.quote(pristine_root)}/state/v*/testnet 2>/dev/null | sort -V | tail -n 1",
        capture=True,
    ).stdout.strip()
    if not found:
        raise ForkError(
            f"no seed under {pristine_root}/state/v*/testnet; the state volume is "
            f"empty or was never mounted"
        )

    seed_version = int(found.rsplit("/state/v", 1)[1].split("/", 1)[0])
    if seed_version > code_version:
        raise ForkError(
            f"the seed at {found} is database format v{seed_version}, newer than this "
            f"tree's v{code_version}; zakurad cannot open a future format"
        )
    if seed_version < code_version:
        print(
            f"[seed] seed is v{seed_version} and this tree is v{code_version}: zakurad "
            f"restores the previous major format in place on first start"
        )

    # After `catch-up`, the pristine non-finalized backup holds the ~1000 most recent
    # blocks, and the recorded tip includes them. Without `catch-up` the tip is read
    # from the finalized database alone, so the backup must not be seeded: it would
    # put blocks above the tip the activation height is computed from.
    caught_up = recorded_seed_tip(config) is not None
    non_finalized = f"{pristine_root}/non_finalized_state/testnet"
    if caught_up:
        print(f"[seed] caught-up seed: also copying {non_finalized}")

    if args.force:
        # A running node keeps writing database files into the directory being
        # replaced, which would silently corrupt the fresh copy.
        for node in nodes:
            unit = shlex.quote(node["service"])
            print(f"[seed] stopping {node['service']}")
            ssh(host, f"if systemctl cat {unit} >/dev/null 2>&1; then systemctl stop {unit}; fi")

    for node in nodes:
        root = node["cache_dir"]
        state_target = f"{root}/state/v{seed_version}/{name}"
        # zakurad moves a previous-major seed to the code's version on first start,
        # so an earlier run's state can live under either version, and the moved
        # copy would win over a fresh seed.
        targets = [f"{root}/state/v{version}/{name}"
                   for version in sorted({seed_version, code_version})]
        # A previous run's non-finalized backup would reload blocks from the old
        # fork, so it goes too.
        non_finalized_target = f"{root}/non_finalized_state/{name}"
        targets.append(non_finalized_target)

        print(f"[seed] {found} -> {state_target}")
        quoted = " ".join(shlex.quote(target) for target in targets)
        if args.force:
            ssh(host, f"rm -rf {quoted}")
        else:
            ssh(host, f"for target in {quoted}; do test ! -e \"$target\" || "
                      f"{{ echo \"fork state already exists at $target; pass --force\" >&2; "
                      f"exit 1; }}; done")
        ssh(host, f"mkdir -p {shlex.quote(str(Path(state_target).parent))}")
        # -a preserves the RocksDB file set exactly; --link-dest would share inodes
        # with the pristine copy and let the fork corrupt its own seed.
        ssh(host, f"cp -a {shlex.quote(found)} {shlex.quote(state_target)}")
        if caught_up:
            ssh(host, f"mkdir -p {shlex.quote(str(Path(non_finalized_target).parent))} && "
                      f"cp -a {shlex.quote(non_finalized)} {shlex.quote(non_finalized_target)}")
    print("[seed] done")
    return 0


def catch_up_config(config: dict, settings: dict) -> str:
    """The temporary node's config: the public Testnet, over the pristine cache."""
    pristine = config["host"]["pristine_cache_dir"]
    storage_mode = config["host"].get("storage_mode", "pruned")
    return "\n".join([
        "# Written by deploy/nu7-fork/fork.py catch-up; removed when it finishes.",
        "[network]",
        'network = "Testnet"',
        f'listen_addr = "{settings["listen_addr"]}"',
        f'cache_dir = "{pristine}"',
        "",
        "[state]",
        f'cache_dir = "{pristine}"',
        f'storage_mode = "{storage_mode}"',
        "",
        "[rpc]",
        f'listen_addr = "{settings["rpc_listen_addr"]}"',
        "enable_cookie_auth = false",
        "",
        "[tracing]",
        f'log_file = "{settings["log_file"]}"',
        "use_color = false",
        "",
    ])


def cmd_catch_up(config: dict, args) -> int:
    """Sync the pristine Testnet cache to the public tip, and record that tip.

    A seed whose tip is hours old cannot be mined on: the fork's first block time is
    capped at the tip's median time plus 90 minutes. The temporary node syncs the
    pristine cache from public peers, then stops, flushing the finalized database and
    its non-finalized backup. The tip it reached is recorded over RPC, because
    `tip-height` sees only the finalized part.
    """
    host = config["host"]["ssh_string"]
    pristine = config["host"]["pristine_cache_dir"]
    binary = config["host"].get("bin_path", "/usr/local/bin/zakurad")
    settings = {**CATCH_UP_DEFAULTS, **config.get("catch_up", {})}
    rpc_addr = settings["rpc_listen_addr"]
    unit = shlex.quote(CATCH_UP_UNIT)
    tip_file = shlex.quote(f"{pristine}/{SEED_TIP_FILE}")

    # The recorded tip describes the cache only until the node writes to it again.
    ssh(host, f"rm -f {tip_file}")
    ssh(host, f"mkdir -p /etc/zakura $(dirname {shlex.quote(settings['log_file'])}) && "
              f"cat > {CATCH_UP_CONFIG} <<'EOF'\n{catch_up_config(config, settings)}EOF")
    ssh(host, f"systemctl reset-failed {unit} 2>/dev/null; "
              f"systemd-run --unit={unit} --collect --property=TimeoutStopSec=600 "
              f"{shlex.quote(binary)} -c {CATCH_UP_CONFIG} start")
    print(f"[catch-up] started {CATCH_UP_UNIT}; syncing {pristine} to the public Testnet tip")

    deadline = time.monotonic() + settings["timeout_minutes"] * 60
    max_age = settings["max_tip_age_minutes"] * 60
    try:
        while True:
            try:
                info = rpc(host, rpc_addr, "getblockchaininfo")
                header = rpc(host, rpc_addr, "getblockheader", [info["bestblockhash"], True])
                age = time.time() - header["time"]
                print(f"[catch-up] height {info['blocks']}, tip block {age / 60:.0f} min old")
                if age <= max_age:
                    tip = {"height": info["blocks"], "hash": info["bestblockhash"],
                           "time": header["time"]}
                    break
            except ForkError as error:
                print(f"[catch-up] waiting for RPC: {error}")
            if time.monotonic() > deadline:
                raise ForkError(f"the seed did not reach a tip younger than "
                                f"{settings['max_tip_age_minutes']} min in time")
            time.sleep(30)
    finally:
        # Stopping flushes the database and the non-finalized backup the fork seeds from.
        ssh(host, f"systemctl stop {unit}; rm -f {CATCH_UP_CONFIG}", check=False)

    ssh(host, f"! systemctl is-active --quiet {unit}")
    ssh(host, f"cat > {tip_file} <<'EOF'\n{json.dumps(tip)}\nEOF")
    print(f"[catch-up] recorded seed tip {tip['height']} ({tip['hash']})")
    return 0


def cmd_plan(config: dict, args) -> int:
    tip = seeded_tip_height(config)
    plan = fork_plan(config, tip)
    print(json.dumps(plan, indent=2))

    activation_blocks = plan["activation"] - tip
    # 450s is the Testnet minimum-difficulty gap before NU7: target spacing x 6.
    print(
        f"\n[plan] {activation_blocks} blocks to mine before NU7 activates, "
        f"about {activation_blocks * 450 / 3600:.1f}h at the pre-NU7 "
        f"minimum-difficulty pace"
    )
    return 0


def cmd_render(config: dict, args) -> int:
    tip = args.tip if args.tip else seeded_tip_height(config)
    plan = fork_plan(config, tip)
    rendered = render_nodes_toml(config, plan)
    args.out.write_text(rendered)
    print(f"[render] wrote {args.out} (NU7 at {plan['activation']})")
    return 0


def cmd_deploy(config: dict, args) -> int:
    if not args.nodes.is_file():
        raise ForkError(f"{args.nodes} not found; run `fork.py render` first")
    names = [node["name"] for node in tomllib.loads(args.nodes.read_text())["nodes"]]
    # deploy.py takes --config after the subcommand, not before it.
    deployer = [sys.executable, str(DEPLOYER)]
    run([*deployer, "build", "--config", str(args.nodes)], cwd=DEPLOYER.parent)
    # One node at a time: deploy.py deploys in parallel and stages every node at the
    # same /tmp/zakurad-deploy.* paths, so co-located fork nodes would install each
    # other's binary, config, or unit.
    for name in names:
        run([*deployer, "deploy", "--config", str(args.nodes), "--node", name],
            cwd=DEPLOYER.parent)
    return 0


def cmd_up(config: dict, args) -> int:
    cmd_seed(config, args)
    tip = seeded_tip_height(config)
    plan = fork_plan(config, tip)
    args.out.write_text(render_nodes_toml(config, plan))
    print(f"[up] NU7 activates at {plan['activation']} (seed tip {tip})")
    args.nodes = args.out
    cmd_deploy(config, args)
    print("[up] deployed; `fork.py status` to watch. To produce blocks, run "
          "zakura-fork-miner (see README.md, \"Continuous mining on the primary node\")")
    return 0


def cmd_status(config: dict, args) -> int:
    info = rpc(config["host"]["ssh_string"], config["host"]["rpc_listen_addr"],
               "getblockchaininfo")

    print(f"chain    {info['chain']}")
    print(f"height   {info['blocks']}")
    for branch, upgrade in info.get("upgrades", {}).items():
        if upgrade["name"] == "NU7":
            print(
                f"NU7      {upgrade['status']} at {upgrade['activationheight']} "
                f"(branch {branch})"
            )
            break
    else:
        print("NU7      NOT PRESENT — the binary or the config is missing NU7")
    return 0


def cmd_reconfigure(config: dict, args) -> int:
    """Re-seed from the pristine copy and redeploy at a fresh activation height."""
    args.force = True
    return cmd_up(config, args)


def main() -> int:
    cli = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    cli.add_argument("--config", type=Path, default=SCRIPT_DIR / "fork.toml")
    cli.add_argument("--out", type=Path, default=SCRIPT_DIR / "nodes.generated.toml",
                     help="where the generated deploy.py fleet config is written")
    sub = cli.add_subparsers(dest="command", required=True)

    provision = sub.add_parser("provision", help="create the droplet and state volume")
    provision.add_argument("--plan", action="store_true",
                           help="validate the DigitalOcean catalog without creating anything")
    provision.set_defaults(func=cmd_provision)

    catch_up = sub.add_parser(
        "catch-up", help="sync the pristine Testnet cache to the public tip and record it"
    )
    catch_up.set_defaults(func=cmd_catch_up)

    seed = sub.add_parser("seed", help="copy the pristine Testnet state into the fork")
    seed.add_argument("--force", action="store_true", help="replace existing fork state")
    seed.set_defaults(func=cmd_seed)

    plan_cmd = sub.add_parser("plan", help="show the heights this fork would use")
    plan_cmd.set_defaults(func=cmd_plan)

    render = sub.add_parser("render", help="write the deploy.py fleet config")
    render.add_argument("--tip", type=int, help="seed tip height, instead of reading the host")
    render.set_defaults(func=cmd_render)

    deploy_cmd = sub.add_parser("deploy", help="build and deploy the rendered config")
    deploy_cmd.set_defaults(func=cmd_deploy)

    up = sub.add_parser("up", help="seed, plan, render and deploy")
    up.add_argument("--force", action="store_true", help="replace existing fork state")
    up.set_defaults(func=cmd_up)

    sub.add_parser("status", help="report height and NU7 status").set_defaults(
        func=cmd_status
    )

    reconfigure = sub.add_parser(
        "reconfigure", help="re-seed and redeploy at a fresh activation height"
    )
    reconfigure.set_defaults(func=cmd_reconfigure)

    args = cli.parse_args()
    if not hasattr(args, "nodes"):
        args.nodes = args.out
    if not hasattr(args, "force"):
        args.force = False

    try:
        config = load_fork_config(args.config)
        return args.func(config, args)
    except ForkError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
