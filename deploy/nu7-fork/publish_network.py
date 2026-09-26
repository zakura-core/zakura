#!/usr/bin/env python3
"""Publish a participant config and manifest derived from the running fork.

Only consensus parameters and explicit public peers are exported. The node's
mining address, identity, credentials, and operator paths are never published.
"""

import argparse
import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path

import dashboard

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deployer"))
from deploy import render_toml_table  # noqa: E402


def manifest(config, info, revision, peers, seed, snapshot=None):
    """Refuse mismatched live consensus before exporting a join configuration."""
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("revision must be a full source commit SHA")
    if not peers or any(not re.fullmatch(r"[A-Za-z0-9.:-]+", peer) for peer in peers):
        raise ValueError("explicit public peers are required")
    parameters = config["network"]["network"]
    if not isinstance(parameters, dict):
        raise ValueError("publish a current, explicitly configured testnet")
    activation = parameters["activation_heights"]["NU7"]
    branch, upgrade = next((branch, upgrade) for branch, upgrade in info["upgrades"].items()
                           if upgrade["name"] == "NU7")
    if info.get("chain") != "test" or upgrade["activationheight"] != activation:
        raise ValueError("live RPC and configured activation disagree")
    if not re.fullmatch(r"[0-9a-f]{8}", branch):
        raise ValueError("invalid consensus branch ID")
    if not 0 <= seed["height"] < activation:
        raise ValueError("seed must precede NU7 activation")
    participant = {
        "network": {
            "listen_addr": "0.0.0.0:18233", "initial_testnet_peers": peers,
            "p2p_stack": "legacy", "network": parameters,
        },
        "rpc": {"listen_addr": "127.0.0.1:18232", "enable_cookie_auth": False},
        "state": {"cache_dir": "./nu7-state", "storage_mode": "pruned"},
    }
    rendered = "\n\n".join("\n".join(render_toml_table(key, value))
                            for key, value in participant.items()) + "\n"
    result = {
        "schemaVersion": 1, "nodeRevision": revision,
        "network": {
            "name": parameters["network_name"],
            "magic": "".join(f"{byte:02x}" for byte in parameters["network_magic"]),
            "activationHeight": activation, "branchId": branch,
            "targetSpacingSeconds": dashboard.TARGET_SPACING_SECONDS,
            "daaWindowBlocks": dashboard.DAA_WINDOW_BLOCKS,
        },
        "peers": peers, "seed": seed, "config": rendered,
        "configSha256": hashlib.sha256(rendered.encode()).hexdigest(),
    }
    if snapshot is not None:
        if (not snapshot["url"].startswith("https://api.nu7.valargroup.dev/snapshots/")
                or not re.fullmatch(r"[0-9a-f]{64}", snapshot["sha256"])):
            raise ValueError("snapshot must have a public HTTPS URL and SHA256")
        result["snapshot"] = snapshot
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--peer", action="append", required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--rpc-port", type=int, default=18232)
    parser.add_argument("--snapshot", type=Path, help="snapshot metadata JSON")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = manifest(
        tomllib.loads(args.config.read_text()), dashboard.rpc(args.rpc_port, "getblockchaininfo"),
        args.revision, args.peer, json.loads(args.seed.read_text()),
        json.loads(args.snapshot.read_text()) if args.snapshot else None,
    )
    args.out.mkdir(parents=True, exist_ok=True)
    # The manifest embeds the config so consumers fetch one coherent generation.
    for filename, content in (("nu7-zakura.toml", result["config"]),
                              ("network.json", json.dumps(result, indent=2) + "\n")):
        temporary = args.out / (filename + ".tmp")
        temporary.write_text(content)
        temporary.replace(args.out / filename)


if __name__ == "__main__":
    main()
