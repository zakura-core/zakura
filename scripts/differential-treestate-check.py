#!/usr/bin/env python3
"""Compare a fast-synced node's derived treestates against a legacy-synced node's.

The validation described by `docs/design/verified-commitment-trees.md` uses a differential test:
the same heights, served from a verified-commitment-trees fast-synced database and from a legacy
archive node, must agree. This is the strongest available check on the design, because the two
sides build their trees by entirely different routes — the legacy node recomputes every per-height
tree as it syncs, while the fast-synced side replays block bodies and checks the result against
authenticated roots. Agreement is independent evidence that replay reconstructs real history
rather than merely being self-consistent.

The comparison covers whatever range both sides can answer, so it strengthens on its own as a
legacy node syncs further: rerun it as the node advances and the covered range grows.

Usage:

    scripts/differential-treestate-check.py \\
        --cache-dir /path/to/fast-synced-cache \\
        --network Mainnet \\
        --rpc-url http://127.0.0.1:8232/ \\
        --from-height 419200 --to-height 435000 --step 50

The legacy node's RPC is usually loopback-only; forward it first, for example
`ssh -N -L 8232:127.0.0.1:8232 root@<host>`.
"""

import argparse
import json
import subprocess
import sys
import urllib.request


def derive_locally(args):
    """Runs zakurad and returns {height: (sapling, orchard, ironwood)} hex roots."""
    command = [
        args.zakurad,
        "audit-historical-treestates",
        "--cache-dir", args.cache_dir,
        "--network", args.network,
        "--no-scan",
        "--walk",
        "--print-roots",
        "--from", str(args.from_height),
        "--to", str(args.to_height),
        "--step", str(args.step),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"local derivation failed:\n{result.stderr[-4000:]}")

    roots = {}
    for line in result.stdout.splitlines():
        if line.startswith("ROOT "):
            _, height, sapling, orchard, ironwood = line.split()
            roots[int(height)] = (sapling, orchard, ironwood)

    if not roots:
        sys.exit("local derivation produced no roots; check the height range")

    return roots


def fetch_remote(rpc_url, height):
    """Returns the legacy node's z_gettreestate result for `height`."""
    payload = json.dumps(
        {"jsonrpc": "1.0", "id": 1, "method": "z_gettreestate", "params": [str(height)]}
    ).encode()
    request = urllib.request.Request(
        rpc_url, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response).get("result")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", required=True, help="fast-synced state cache directory")
    parser.add_argument("--network", default="Mainnet")
    parser.add_argument("--rpc-url", required=True, help="legacy node's JSON-RPC endpoint")
    parser.add_argument("--from-height", type=int, required=True)
    parser.add_argument("--to-height", type=int, required=True)
    parser.add_argument("--step", type=int, default=50)
    parser.add_argument("--zakurad", default="target/release/zakurad")
    args = parser.parse_args()

    derived = derive_locally(args)
    print(f"derived {len(derived)} heights locally")

    compared = {"sapling": [0, 0], "orchard": [0, 0], "ironwood": [0, 0]}
    unavailable = 0
    mismatches = []

    for height in sorted(derived):
        result = fetch_remote(args.rpc_url, height)
        if result is None:
            unavailable += 1
            continue

        for index, pool in enumerate(("sapling", "orchard", "ironwood")):
            # A pool with no root at this height is either pre-activation or beyond what the
            # legacy node has synced. Neither is a disagreement, so it is not counted as one.
            legacy = result.get(pool, {}).get("commitments", {}).get("finalRoot")
            if legacy is None:
                continue

            if legacy == derived[height][index]:
                compared[pool][0] += 1
            else:
                compared[pool][1] += 1
                mismatches.append((height, pool, legacy, derived[height][index]))

    print()
    for pool, (matched, mismatched) in compared.items():
        if matched or mismatched:
            print(f"  {pool:>8}: {matched} matched, {mismatched} mismatched")
    if unavailable:
        print(f"  {unavailable} heights the legacy node could not answer (not yet synced)")

    if mismatches:
        print("\nMISMATCHES:")
        for height, pool, legacy, ours in mismatches[:20]:
            print(f"  height {height} {pool}: legacy {legacy} derived {ours}")
        sys.exit(1)

    total = sum(matched for matched, _ in compared.values())
    if total == 0:
        sys.exit("no roots could be compared; the legacy node may not have synced this range yet")

    print(f"\nOK: {total} roots agree across both backends")


if __name__ == "__main__":
    main()
