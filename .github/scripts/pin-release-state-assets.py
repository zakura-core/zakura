#!/usr/bin/env python3
"""Pin the verified asset from the preserved ready release-state set."""

import argparse
import json
from pathlib import Path

from ready_release_state import RESOLUTION_KEYS, verify_ready_snapshot
from release_state_assets import AssetError, pin_assets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--import-result", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    manifest = verify_ready_snapshot(args.state)
    imported = json.loads(args.import_result.read_text())
    if any(imported[key] != manifest[key] for key in RESOLUTION_KEYS):
        raise AssetError("imported data does not match the preserved ready set")
    if imported["has_changes"] and imported["frontier_grid_sha256"] != manifest["asset"]["grid_sha256"]:
        raise AssetError("imported grid does not match the ready asset")
    pin_assets(Path.cwd(), imported, expected_checksum=manifest["asset"]["checksum"])


if __name__ == "__main__":
    main()
