#!/usr/bin/env python3
"""Create a volume snapshot and resolve its ID from the snapshot catalog."""

import argparse
import subprocess
import sys
import time

from do_provision import doctl


def matching_snapshots(volume_id, name, region):
    return [
        snapshot
        for snapshot in doctl("snapshot", "list", "--resource", "volume")
        if snapshot["name"] == name
        and snapshot["resource_id"] == volume_id
        and region in snapshot["regions"]
    ]


def create_snapshot(volume_id, name, region, timeout=300):
    """Create once, then wait for the exact volume/name/region to be listed."""
    if matching_snapshots(volume_id, name, region):
        raise RuntimeError(f"snapshot already exists for volume {volume_id}: {name}")
    # doctl 1.120.0 discards the create response, even with --output json.
    doctl("volume", "snapshot", volume_id, "--snapshot-name", name)
    deadline = time.monotonic() + timeout
    while True:
        matches = matching_snapshots(volume_id, name, region)
        if len(matches) > 1:
            raise RuntimeError(f"multiple snapshots match volume {volume_id}: {name}")
        if matches:
            return str(matches[0]["id"])
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"created snapshot not listed for volume {volume_id}: {name}"
            )
        time.sleep(min(5, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume-id", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--region", required=True)
    args = parser.parse_args()
    print(create_snapshot(args.volume_id, args.name, args.region))


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.stderr, file=sys.stderr)
        sys.exit(1)
