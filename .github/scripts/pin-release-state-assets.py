#!/usr/bin/env python3
"""Pin the verified, published grid matching an imported release-state bundle."""

import argparse
import json
from pathlib import Path

from release_state_assets import pin_assets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--import-result", type=Path, required=True)
    parser.add_argument("--selection", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--request-id")
    args = parser.parse_args()
    if len({bool(args.selection), bool(args.receipt), bool(args.request_id)}) != 1:
        parser.error("--selection, --receipt, and --request-id must be supplied together")
    pin_assets(Path.cwd(), json.loads(args.import_result.read_text()),
               receipt=json.loads(args.receipt.read_text()) if args.receipt else None, request_id=args.request_id,
               selected=json.loads(args.selection.read_text()) if args.selection else None)


if __name__ == "__main__":
    main()
