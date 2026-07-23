#!/usr/bin/env python3
"""Drive an isolated local-genesis mempool-load testnet.

Generates a throwaway chain with the Kresko Rust binary, runs N zakurad nodes
peered to each other, mines, and blasts funded Orchard transactions at one
node's RPC. Used identically by the local rehearsal and by the CI droplet, so a
green local run means the CI path is exercising the same code.

Stdlib only, mirroring deploy/deployer/deploy.py.

Each node binds a distinct loopback address (127.0.0.1, 127.0.0.2, ...) rather
than a distinct port: Kresko hardcodes one p2p/RPC port per host (18233/18232),
assuming one node per machine. `kresko localize-fleet` re-points each generated
config at that node's own address and directories, and `kresko seed-local`
loads the generated chain into it -- work this harness used to do in Python.

The chain is synthetic. Funded keys spend premine on a network whose magic is
generated per run, so they are worthless off this chain -- but they are still
never printed or copied into collected output.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# Kresko's fixed per-host ports (src/zebra_config.rs). We vary the bind address
# instead of these so its generated configs need no rewriting beyond listen_addr.
P2P_PORT = 18233
RPC_PORT = 18232
# Prometheus scrape endpoint. Needs zakurad built with the `prometheus` feature;
# the mempool counters we grade backpressure on are only exposed here, not over
# JSON-RPC.
METRICS_PORT = 19999
# The Zakura p2p stack's own listener, present in the config even when
# p2p_stack = "default" leaves it unused.
ZAKURA_P2P_PORT = 18234

# Node RPC has to open the state DB and bind before we start counting failures.
RPC_READY_TIMEOUT_SECS = 180


# Artifact allowlist. Collection copies exactly these globs -- never the lab dir
# wholesale -- so a new secret file appearing in the payload cannot silently
# start being uploaded. Kept here rather than in the workflow so the unit tests
# can assert on it.
COLLECTED_PATHS = (
    "nodes/*/run.log",
    "nodes/*/bootstrap.log",
    "nodes/*/zakura.toml",
    "traces/*.jsonl",
    "txblast.log",
    "config.json",
)

# Filename fragments that mark premine spending keys. These are worthless off
# the throwaway chain, but they are still key material and never leave the box.
SECRET_NAME_FRAGMENTS = ("funded_key", "funded_keys", "treasury_key", "recovery")

# Content markers for the same material. A filename check alone is not enough:
# `kresko genesis` writes every funded key's secret_key_hex, plus the bootstrap
# treasury key, into config.json -- a file whose name looks entirely innocuous.
SECRET_CONTENT_MARKERS = ("secret_key_hex", "bootstrap_treasury_key")

# JSON keys stripped when sanitizing a collected config. The rest of the config
# is the most useful record of how a run was parameterized, so it is redacted
# rather than dropped.
SECRET_JSON_KEYS = ("funded_keys", "bootstrap_treasury_key")


def is_secret_path(path: str) -> bool:
    """True if `path` names premine key material that must never be collected."""
    name = path.rsplit("/", 1)[-1].lower()
    return any(fragment in name for fragment in SECRET_NAME_FRAGMENTS)


def contains_secret(text: str) -> bool:
    """True if `text` carries premine key material, whatever the filename."""
    return any(marker in text for marker in SECRET_CONTENT_MARKERS)


def sanitize_config(raw: str) -> str:
    """Strip key material from a Kresko config.json, keeping the parameters."""
    config = json.loads(raw)
    local_genesis = config.get("local_genesis")
    if isinstance(local_genesis, dict):
        # Dropped outright rather than replaced with a placeholder: the key
        # names are themselves the markers contains_secret() looks for, so a
        # redacted-in-place config would still trip the content guard.
        removed = [key for key in SECRET_JSON_KEYS if local_genesis.pop(key, None) is not None]
        if removed:
            # Deliberately does not echo the removed key names: they are the
            # very strings contains_secret() matches on.
            local_genesis["_redacted"] = f"{len(removed)} key field(s) removed before collection"
    return json.dumps(config, indent=2) + "\n"


# Nodes start at 127.0.0.101, not .1: a developer box or droplet is very likely
# to already have a node (or a docker-proxy) on 127.0.0.1, and the whole lab
# should coexist with it rather than demand the port back.
NODE_IP_BASE = 101


def node_ip(index: int) -> str:
    """Loopback address for node `index`. 127.0.0.0/8 is all bound to lo."""
    if not 0 <= index < 254 - NODE_IP_BASE:
        raise ValueError(f"node index {index} out of range for a 127.0.0.x address")
    return f"127.0.0.{NODE_IP_BASE + index}"


def node_name(index: int) -> str:
    # Kresko derives the payload dir from Instance::parsed_hostname(), which
    # keeps the first two dash-separated parts -- so "miner-0" maps to itself.
    return f"miner-{index}"


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"+ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, check=True, **kwargs)


def rpc_call(url: str, method: str, params: list | None = None, timeout: int = 15):
    payload = json.dumps(
        {
            "jsonrpc": "1.0",
            "id": "mempool-load-lab",
            "method": method,
            "params": params or [],
        }
    ).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = json.loads(resp.read())
    if body.get("error"):
        raise RuntimeError(f"{method} failed: {body['error']}")
    return body["result"]


# ---------------------------------------------------------------------------- #
# config.json generation (schema: Kresko src/config.rs)
# ---------------------------------------------------------------------------- #


def build_kresko_config(args) -> dict:
    """The Kresko experiment config for a local-genesis run.

    network_kind is pinned to local-genesis: Kresko's require_local_genesis()
    gate then refuses every public-network command, so a misconfigured run
    cannot reach a real network.
    """
    miners = [
        {
            "node_type": "miner",
            "public_ip": node_ip(i),
            "private_ip": node_ip(i),
            "provider": "digitalocean",
            "slug": "local",
            "region": "local",
            "name": node_name(i),
            "tags": ["mempool-load"],
            "tier": "full",
        }
        for i in range(args.node_count)
    ]
    return {
        "miners": miners,
        "chain_id": args.chain_id,
        "experiment": args.experiment,
        "ssh_pub_key_path": "",
        "ssh_key_name": "",
        "ssh_key_path": "",
        "provider": "digitalocean",
        "network_kind": "local-genesis",
        "mining_mode": "generate",
        "block_time_secs": args.block_time_secs,
        "equihash_params": "regtest",
        "daa": {},
        "orchard_txblast": {
            "lanes_per_miner": args.orchard_lanes_per_miner,
            "lane_value_zats": args.orchard_lane_value_zats,
            "fanout_source_value_zats": 500_000,
            "fanout_outputs": 1,
        },
        "local_genesis": None,
    }


def cmd_genesis(args) -> int:
    lab = Path(args.lab_dir).resolve()
    lab.mkdir(parents=True, exist_ok=True)
    config = build_kresko_config(args)
    (lab / "config.json").write_text(json.dumps(config, indent=2) + "\n")

    # NU6.3 gates the Ironwood shielded pool, so a chain that stops below it
    # cannot exercise Ironwood at all. Nu7 is deliberately not requested: a
    # release zakurad compiles its consensus branch id out, and no block at an
    # Nu7 activation height can be mined.
    env = {**os.environ, "KRESKO_LATEST_NETWORK_UPGRADE": args.latest_upgrade}
    # Recorded so the blast uses the same upgrade as the chain. Kresko's
    # transaction builder derives its consensus branch id from this, and a
    # builder that disagrees with the chain has every transaction rejected.
    (lab / "latest-upgrade").write_text(args.latest_upgrade + "\n")
    run(
        [
            args.kresko_binary,
            "genesis",
            "--zebrad-binary",
            str(Path(args.zakurad_binary).resolve()),
            "--kresko-binary",
            str(Path(args.kresko_binary).resolve()),
            "--maturity-padding-blocks",
            str(args.maturity_padding_blocks),
            "--orchard-lanes-per-miner",
            str(args.orchard_lanes_per_miner),
            "--orchard-lane-value-zats",
            str(args.orchard_lane_value_zats),
            "-d",
            str(lab),
        ],
        env=env,
    )

    payload = lab / "payload"
    if not (payload / "local_genesis").is_dir():
        print("kresko genesis did not produce payload/local_genesis", file=sys.stderr)
        return 1
    for i in range(args.node_count):
        node_toml = payload / node_name(i) / "zebrad.toml"
        if not node_toml.is_file():
            print(f"missing generated config: {node_toml}", file=sys.stderr)
            return 1
    print(f"Generated local-genesis payload under {payload}")
    return 0


# ---------------------------------------------------------------------------- #
# Per-node config localization (delegated to Kresko)
# ---------------------------------------------------------------------------- #


def localize_fleet(lab: Path, args) -> None:
    """Render each node's localized config via `kresko localize-fleet`.

    Kresko generates one-node-per-host configs (0.0.0.0 binds, a shared /root
    state dir). `kresko localize-fleet` re-points each node's config at its own
    127.0.0.x loopback and per-node directories, inserts the metrics endpoint
    and internal-miner flag, empties the public seed-peer lists, and regenerates
    the loopback peer list -- the work the harness used to do in Python. The
    resolved lab dir is passed so the absolute node-relative paths Kresko bakes
    in match this run's layout.
    """
    run(
        [
            args.kresko_binary,
            "localize-fleet",
            "-d",
            str(lab),
            "--miner-nodes",
            str(args.miner_nodes),
        ]
    )


# ---------------------------------------------------------------------------- #
# Node lifecycle
# ---------------------------------------------------------------------------- #


# zakurad maps ZAKURA_<FIELD> environment variables onto config fields, so any
# stray one in the environment is read as a config key and the node refuses to
# start ("unknown field `dir`"). The lab always passes an explicit config file,
# so nothing of ours belongs in that namespace.
def node_env() -> dict:
    hijacked = sorted(k for k in os.environ if k.startswith("ZAKURA_"))
    if hijacked:
        print(
            "dropping ZAKURA_* vars that zakurad would read as config: "
            + ", ".join(hijacked),
            file=sys.stderr,
        )
    return {k: v for k, v in os.environ.items() if not k.startswith("ZAKURA_")}


def spawn_node(lab: Path, args, index: int, *, bootstrap: bool) -> subprocess.Popen:
    name = node_name(index)
    node_dir = lab / "nodes" / name
    config = node_dir / ("zakura.bootstrap.toml" if bootstrap else "zakura.toml")
    suffix = "bootstrap" if bootstrap else "run"
    log_path = node_dir / f"{suffix}.log"
    log = open(log_path, "ab")
    proc = subprocess.Popen(
        [str(Path(args.zakurad_binary).resolve()), "-c", str(config), "start"],
        stdout=log,
        stderr=subprocess.STDOUT,
        # Own process group, so stopping a node never signals the whole lab.
        start_new_session=True,
        env=node_env(),
    )
    print(f"started {name} ({suffix}) pid={proc.pid} log={log_path}", flush=True)
    return proc


def wait_for_rpc(
    url: str, timeout_secs: int = RPC_READY_TIMEOUT_SECS, proc: subprocess.Popen | None = None
) -> bool:
    """Wait for `url` to answer, failing fast if our own node died.

    The liveness check matters: if our node exits and some unrelated process
    owns the port, a bare RPC probe would succeed and we would go on to submit
    blocks to a stranger's node. Checking `proc` first makes that impossible.
    """
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            print(
                f"node exited with code {proc.returncode} before RPC came up",
                file=sys.stderr,
            )
            return False
        try:
            rpc_call(url, "getblockchaininfo", timeout=3)
            return True
        except (urllib.error.URLError, OSError, RuntimeError, json.JSONDecodeError):
            time.sleep(2)
    return False


def port_owner(host: str, port: int) -> bool:
    """True if something is already listening on host:port."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        return sock.connect_ex((host, port)) == 0


def assert_ports_free(node_count: int) -> None:
    """Refuse to start if any port the lab needs is already taken.

    Developer boxes and CI droplets can already be running a node; binding on
    top of one silently produces a lab that talks to the wrong chain.
    """
    conflicts = []
    for i in range(node_count):
        ip = node_ip(i)
        for port, label in (
            (P2P_PORT, "p2p"),
            (RPC_PORT, "rpc"),
            (METRICS_PORT, "metrics"),
            (ZAKURA_P2P_PORT, "zakura-p2p"),
        ):
            if port_owner(ip, port):
                conflicts.append(f"{ip}:{port} ({label}, node {node_name(i)})")
    if conflicts:
        raise RuntimeError(
            "ports already in use -- stop the process holding them, or the lab "
            "would talk to the wrong node:\n  " + "\n  ".join(conflicts)
        )


def stop_proc(proc: subprocess.Popen, name: str, timeout_secs: int = 60) -> None:
    if proc.poll() is not None:
        return
    # SIGINT is zakurad's clean-shutdown path; it flushes and closes the state DB.
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=timeout_secs)
        return
    except subprocess.TimeoutExpired:
        print(f"{name} ignored SIGINT, escalating to SIGTERM", file=sys.stderr)
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        print(f"{name} ignored SIGTERM, killing", file=sys.stderr)
        proc.kill()
        proc.wait(timeout=30)


def seed_node(lab: Path, args, index: int) -> None:
    """Load the generated chain into one node via the P2P-disabled bootstrap config.

    Brings the node up with P2P off so it cannot gossip a partial chain, then
    hands the genesis and premine blocks to `kresko seed-local`, which submits
    them over RPC (with the duplicate/rejected retry and height-idempotence
    semantics scripts/node_init.sh uses), then shuts the node down. The node is
    restarted on the real config by `up`.
    """
    name = node_name(index)
    url = f"http://{node_ip(index)}:{RPC_PORT}"
    genesis_path = lab / "payload" / "local_genesis" / "genesis.hex"
    premine_path = lab / "payload" / "local_genesis" / "premine_blocks.hex"

    proc = spawn_node(lab, args, index, bootstrap=True)
    try:
        if not wait_for_rpc(url, proc=proc):
            log_path = lab / "nodes" / name / "bootstrap.log"
            raise RuntimeError(
                f"{name}: bootstrap RPC never came up (see {log_path})"
                + explain_startup_failure(log_path)
            )
        run(
            [
                args.kresko_binary,
                "seed-local",
                "--rpc-endpoint",
                url,
                "--genesis",
                str(genesis_path),
                "--premine",
                str(premine_path),
            ]
        )
    finally:
        stop_proc(proc, name)


def explain_startup_failure(log_path: Path) -> str:
    """Turn known startup failures into an actionable hint.

    The config Kresko renders comes from the zakura version *Kresko* links, and
    zakura's config is #[serde(deny_unknown_fields)]. So a node older than
    Kresko's pin rejects fields it has never heard of. The raw error names only
    the field, which is a long way from the actual cause.
    """
    try:
        body = log_path.read_text(errors="replace")
    except OSError:
        return ""
    match = re.search(r"unknown field `([^`]+)`", body)
    if match:
        return (
            f"\n  The node rejected config field `{match.group(1)}`. That field comes "
            "from the zakura version Kresko was built against, so the node under "
            "test is older than Kresko's pin. Test a newer ref, or repin Kresko."
        )
    if "panicked at" in body:
        return "\n  The node panicked during startup; see the log."
    return ""


def cmd_up(args) -> int:
    """Seed every node, start them all, and leave them running."""
    lab = Path(args.lab_dir).resolve()
    assert_ports_free(args.node_count)
    localize_fleet(lab, args)
    for i in range(args.node_count):
        seed_node(lab, args, i)

    procs: dict[str, subprocess.Popen] = {}
    for i in range(args.node_count):
        procs[node_name(i)] = spawn_node(lab, args, i, bootstrap=False)
        # Recorded after each spawn, not once at the end: if a later spawn
        # raises, the nodes already started must still be visible to `down`.
        write_pidfile(lab, procs)

    failed = []
    for i in range(args.node_count):
        url = f"http://{node_ip(i)}:{RPC_PORT}"
        if not wait_for_rpc(url, proc=procs[node_name(i)]):
            failed.append(node_name(i))
    if failed:
        print(f"nodes never came up: {', '.join(failed)}", file=sys.stderr)
        return 1

    if args.node_count > 1 and not wait_for_peers(args.node_count):
        # A network whose nodes never find each other still runs, still mines,
        # and still accepts transactions -- it just measures nothing, because
        # nothing propagates. Failing here beats discovering it in the verdict.
        print("nodes did not peer with each other", file=sys.stderr)
        return 1

    # Peer counts alone are too weak a signal. A run that came up with only
    # n-1 connections satisfied the peer gate but left one node unable to
    # follow the chain -- it sat 8 blocks behind for the whole run and the
    # verdict failed on tip divergence at the very end. Waiting for a mined
    # block to reach every node proves the topology actually carries data,
    # and turns that late, confusing failure into an immediate one.
    if args.node_count > 1 and not wait_for_chain_convergence(args.node_count):
        print("nodes did not converge on a common tip at startup", file=sys.stderr)
        return 1

    print(f"{args.node_count} nodes up, peered, and following one chain", flush=True)
    return 0


def wait_for_chain_convergence(
    node_count: int, timeout_secs: int = 180, poll_secs: float = 5.0
) -> bool:
    """Wait until every node reports the same tip, above the seeded height.

    Requires progress as well as agreement: nodes all sitting at the seed
    height agree trivially, which proves nothing about propagation.
    """
    deadline = time.monotonic() + timeout_secs
    heights: dict[str, int] = {}
    baseline: int | None = None
    while True:
        heights = {}
        for i in range(node_count):
            url = f"http://{node_ip(i)}:{RPC_PORT}"
            try:
                heights[node_name(i)] = rpc_call(url, "getblockchaininfo", timeout=5)["blocks"]
            except (urllib.error.URLError, OSError, RuntimeError, json.JSONDecodeError, KeyError):
                heights[node_name(i)] = -1
        if baseline is None:
            baseline = min(heights.values())
        values = list(heights.values())
        if min(values) > baseline and max(values) - min(values) <= 1:
            print(f"all nodes following one chain: {heights}", flush=True)
            return True
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_secs)
    print(
        f"nodes still diverged after {timeout_secs}s (baseline {baseline}): {heights}",
        file=sys.stderr,
    )
    return False


def wait_for_peers(node_count: int, timeout_secs: int = 120, poll_secs: float = 5.0) -> bool:
    """Wait until the fleet has enough connections to form a connected graph.

    Graded on the total across nodes, not per node: `getpeerinfo` reports a
    connection from the dialling side, so in a healthy 3-node lab the counts
    are routinely 2/0/1. Requiring every node to report a peer would reject a
    perfectly connected network. n-1 connections is the minimum that can span
    n nodes, so anything less is definitely partitioned.
    """
    needed = node_count - 1
    deadline = time.monotonic() + timeout_secs
    counts: dict[str, int] = {}
    while True:
        counts = {}
        for i in range(node_count):
            url = f"http://{node_ip(i)}:{RPC_PORT}"
            try:
                counts[node_name(i)] = len(rpc_call(url, "getpeerinfo", timeout=5))
            except (urllib.error.URLError, OSError, RuntimeError, json.JSONDecodeError):
                counts[node_name(i)] = 0
        if sum(counts.values()) >= needed:
            print(f"peer connections: {counts}", flush=True)
            return True
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_secs)
    print(
        f"only {sum(counts.values())} peer connection(s) after {timeout_secs}s, "
        f"need {needed}: {counts}",
        file=sys.stderr,
    )
    return False


def write_pidfile(lab: Path, procs: dict[str, subprocess.Popen]) -> None:
    (lab / "pids.json").write_text(
        json.dumps({name: p.pid for name, p in procs.items()}, indent=2) + "\n"
    )


def cmd_down(args) -> int:
    """Stop every recorded node, escalating until each is actually gone.

    Keeps any PID it could not kill in the pidfile: a node that ignores SIGINT
    (zakurad flushes and closes RocksDB on shutdown, which is slow under a full
    mempool) must stay recoverable, or the next `up` fails the port preflight
    with no record of what to clean up.
    """
    lab = Path(args.lab_dir).resolve()
    pidfile = lab / "pids.json"
    if not pidfile.is_file():
        print("no pids.json; nothing to stop")
        return 0

    survivors = {}
    for name, pid in json.loads(pidfile.read_text()).items():
        if stop_pid(pid, name):
            print(f"stopped {name} (pid {pid})")
        else:
            print(f"{name} (pid {pid}) is still running", file=sys.stderr)
            survivors[name] = pid

    if survivors:
        pidfile.write_text(json.dumps(survivors, indent=2) + "\n")
        return 1
    pidfile.unlink()
    return 0


def stop_pid(pid: int, name: str) -> bool:
    """SIGINT -> SIGTERM -> SIGKILL a PID we do not own as a child. True if gone."""
    for sig, grace in ((signal.SIGINT, 60), (signal.SIGTERM, 30), (signal.SIGKILL, 30)):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return True
        except PermissionError:
            print(f"{name} (pid {pid}) is not ours to signal", file=sys.stderr)
            return False
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return True
            time.sleep(1)
        print(f"{name} (pid {pid}) ignored {sig.name}, escalating", file=sys.stderr)
    return False


def cmd_blast(args) -> int:
    """Run the funded Orchard transaction blast against node 0's RPC."""
    lab = Path(args.lab_dir).resolve()
    trace_dir = lab / "traces"
    trace_dir.mkdir(parents=True, exist_ok=True)
    node_dir = lab / "nodes" / node_name(0)
    # txblast-local defaults its Orchard tuning independently of the chain, so
    # the lane inventory it tries to build can exceed what genesis funded.
    # Replaying the generated values keeps the two in step.
    orchard = json.loads((lab / "config.json").read_text())["orchard_txblast"]
    cmd = [
        args.kresko_binary,
        "txblast-local",
        "--rpc-endpoint",
        f"http://{node_ip(0)}:{RPC_PORT}",
        "--rate",
        str(args.tx_rate),
        "--trace-dir",
        str(trace_dir),
        "--funded-key-path",
        str(node_dir / "funded_key.json"),
        "--orchard-lanes-per-miner",
        str(orchard["lanes_per_miner"]),
        "--orchard-lane-value-zats",
        str(orchard["lane_value_zats"]),
        "--orchard-fanout-source-value-zats",
        str(orchard["fanout_source_value_zats"]),
        "--orchard-fanout-outputs",
        str(orchard["fanout_outputs"]),
    ]
    print(f"+ {' '.join(cmd)}", flush=True)
    # Same upgrade the chain was generated with -- see cmd_genesis.
    upgrade_file = lab / "latest-upgrade"
    blast_env = {**os.environ}
    if upgrade_file.is_file():
        blast_env["KRESKO_LATEST_NETWORK_UPGRADE"] = upgrade_file.read_text().strip()
    log_path = lab / "txblast.log"
    with open(log_path, "ab") as log:
        # Own process group so the whole blaster can be signalled as a unit.
        proc = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=blast_env,
        )
        try:
            proc.wait(timeout=args.duration_secs)
        except subprocess.TimeoutExpired:
            # Expected: the blast runs until we stop it at the duration bound.
            print(f"blast duration reached ({args.duration_secs}s), stopping", flush=True)
        finally:
            # Runs on every exit path, including the SIGINT the runner sends at
            # the end of a leg. Without it the wrapper dies and leaves kresko
            # submitting transactions into the next A/B leg's nodes.
            stop_blast(proc)
    print(f"blast log: {log_path}, traces: {trace_dir}")
    return 0


def stop_blast(proc: subprocess.Popen) -> None:
    """Signal the blaster's whole process group, escalating until it is gone."""
    if proc.poll() is not None:
        return
    for sig, grace in ((signal.SIGINT, 60), (signal.SIGTERM, 30), (signal.SIGKILL, 30)):
        try:
            os.killpg(os.getpgid(proc.pid), sig)
        except (ProcessLookupError, PermissionError):
            return
        try:
            proc.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            print(f"blaster ignored {sig.name}, escalating", file=sys.stderr)


def cmd_collect(args) -> int:
    """Copy allowlisted artifacts out of the lab dir.

    Copies only COLLECTED_PATHS, and refuses any file that looks like key
    material even if a glob would otherwise match it -- so the allowlist and the
    secret check both have to fail before a key could be uploaded.
    """
    lab = Path(args.lab_dir).resolve()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)

    copied = skipped = 0
    for pattern in COLLECTED_PATHS:
        for src in sorted(lab.glob(pattern)):
            if not src.is_file():
                continue
            rel = src.relative_to(lab)
            if is_secret_path(str(rel)):
                print(f"refusing to collect key material: {rel}", file=sys.stderr)
                skipped += 1
                continue
            dst = out / rel
            dst.parent.mkdir(parents=True, exist_ok=True)

            if src.name == "config.json":
                body = sanitize_config(src.read_text())
            else:
                body = src.read_text(errors="replace")
            # Content check after any sanitizing: an allowlisted, innocuously
            # named file must still never carry key material out.
            if contains_secret(body):
                print(f"refusing to collect (secret content): {rel}", file=sys.stderr)
                skipped += 1
                continue
            dst.write_text(body)
            copied += 1
    print(f"collected {copied} file(s) into {out} ({skipped} refused)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab-dir", default="/root/mempool-lab")
    parser.add_argument("--zakurad-binary", default="zakurad")
    parser.add_argument("--kresko-binary", default="kresko")
    parser.add_argument("--node-count", type=int, default=4)
    parser.add_argument(
        "--miner-nodes",
        type=int,
        default=1,
        help="how many nodes run zakurad's internal miner",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("genesis", help="generate the local-genesis chain payload")
    gen.add_argument("--chain-id", default="mempool-load")
    gen.add_argument(
        "--latest-upgrade",
        # nu6_1, not nu6_3, until Kresko's transaction builder can follow. Its
        # zcash_protocol 0.7 has no Nu6_2/Nu6_3 variants, so on an NU6.3 chain
        # it signs at NU6.1's consensus branch id and every transaction is
        # rejected -- the blast dies during bootstrap and the run measures
        # nothing. Kresko can now *express* NU6.2/NU6.3 (valargroup/kresko),
        # so this becomes nu6_3 once its zcash stack is bumped to the 0.10 /
        # 0.29 / orchard 0.15 line that Zakura already uses.
        #
        # Ironwood is gated on NU6.3, so Ironwood coverage waits on that too.
        default="nu6_1",
        help="newest network upgrade the generated chain activates "
        "(nu6_3 needs a Kresko built against zcash_protocol 0.10+)",
    )
    gen.add_argument("--experiment", default="mempool-load")
    gen.add_argument("--block-time-secs", type=int, default=5)
    gen.add_argument("--maturity-padding-blocks", type=int, default=125)
    # 32, not Kresko's 384: every lane is proved during bootstrap, so a large
    # inventory means a bounded run never reaches steady state.
    gen.add_argument("--orchard-lanes-per-miner", type=int, default=32)
    gen.add_argument("--orchard-lane-value-zats", type=int, default=100_000)
    gen.set_defaults(func=cmd_genesis)

    up = sub.add_parser("up", help="seed and start every node")
    up.set_defaults(func=cmd_up)

    down = sub.add_parser("down", help="stop every node")
    down.set_defaults(func=cmd_down)

    blast = sub.add_parser("blast", help="run txblast against node 0")
    blast.add_argument("--tx-rate", type=int, default=10)
    blast.add_argument("--duration-secs", type=int, default=300)
    blast.set_defaults(func=cmd_blast)

    collect = sub.add_parser("collect", help="copy allowlisted artifacts to --out")
    collect.add_argument("--out", default="/root/out")
    collect.set_defaults(func=cmd_collect)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.miner_nodes < 1 or args.miner_nodes > args.node_count:
        print("--miner-nodes must be between 1 and --node-count", file=sys.stderr)
        return 2
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
