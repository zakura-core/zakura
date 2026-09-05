#!/usr/bin/env python3
"""Import complete decode sessions, without claiming service-lifecycle coverage."""

import argparse
import json
from pathlib import Path

from getblocks_capture import IncompleteCapture, import_arrivals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path, help="one process's block_sync.jsonl")
    parser.add_argument("output", type=Path, help="new arrival artifact; existing files are preserved")
    parser.add_argument("--final-metrics", type=Path, help="saved local Prometheus scrape after all observed peers disconnect")
    args = parser.parse_args()
    try:
        with args.trace.open("rb") as source:
            metrics = args.final_metrics.read_bytes() if args.final_metrics else None
            result = import_arrivals(source, metrics)
        with args.output.open("x") as output:
            json.dump(result, output, indent=2)
            output.write("\n")
    except (OSError, IncompleteCapture) as error:
        parser.exit(1, f"capture import failed: {error}\n")


if __name__ == "__main__":
    main()
