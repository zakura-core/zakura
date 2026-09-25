#!/usr/bin/env python3
"""Derive a remote miner node config from a fork node's own config."""

import argparse
import re
import tomllib
from pathlib import Path


def render(source: str, peers: list[str], miner_address: str) -> str:
    config = tomllib.loads(source)
    network = config["network"]
    # A configured testnet is `network = { ... }` since #1147. The running fork's
    # config predates that and keeps its parameters in [network.testnet_parameters].
    network = (network["network"] if isinstance(network.get("network"), dict)
               else network.get("testnet_parameters"))
    # The base must be a fork node's own config, so the remote node inherits exactly
    # its name, magic and activation heights. Nothing is restated here, so a
    # reconfigured fork needs no edit to this script.
    if not network or "NU7" not in network.get("activation_heights", {}):
        raise ValueError("base config is not a NU7 fork node config")
    if not peers or any(not re.fullmatch(r"[A-Za-z0-9.:-]+", peer) for peer in peers):
        raise ValueError("provide at least one valid P2P peer")
    if not re.fullmatch(r"t[0-9A-Za-z]{34}", miner_address):
        raise ValueError("miner address is not a Testnet transparent address")
    source, peer_count = re.subn(
        r'initial_testnet_peers = \[[^\]]*\]',
        'initial_testnet_peers = [\n' + ''.join(f'    "{peer}",\n' for peer in peers) + ']',
        source,
        count=1,
    )
    source, miner_count = re.subn(
        r'miner_address = "[^"]*"', f'miner_address = "{miner_address}"', source, count=1
    )
    if peer_count != 1 or miner_count != 1:
        raise ValueError("base config has no unique peer list or miner address")
    return source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--peer", action="append", required=True)
    parser.add_argument("--miner-address", required=True)
    args = parser.parse_args()
    args.output.write_text(render(args.base.read_text(), args.peer, args.miner_address))


if __name__ == "__main__":
    main()
