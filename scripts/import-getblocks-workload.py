#!/usr/bin/env python3
"""Import one closed, fully reconciled GetBlocks application-lifetime capture.

Unsupported outcomes reject the entire capture. Returned writes can include
errors; this profile does not establish delivery to the downloading peer.
"""
import argparse
import json
from pathlib import Path

from getblocks_capture import import_arrivals
from getblocks_lifetimes import import_completed_lifetimes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path, help="closed run directory containing traces and final boundary evidence")
    parser.add_argument("output", type=Path, help="new workload artifact; existing files are preserved")
    args = parser.parse_args()
    try:
        metrics = (args.run / "final-metrics.prom").read_bytes()
        boundary = (args.run / "capture-boundary.json").read_bytes()
        clients = (args.run / "clients-stopped.json").read_bytes()
        block_path = args.run / "traces" / "block_sync.jsonl"
        query_path = args.run / "traces" / "commit_state.jsonl"
        with block_path.open("rb") as source:
            arrivals = import_arrivals(source, metrics)
        with block_path.open("rb") as blocks, query_path.open("rb") as queries:
            result = import_completed_lifetimes(blocks, queries, arrivals, metrics, boundary, clients)
        with args.output.open("x") as output:
            json.dump(result, output, indent=2)
            output.write("\n")
    except (OSError, ValueError) as error:
        parser.exit(1, f"capture import failed: {error}\n")


if __name__ == "__main__":
    main()
