#!/usr/bin/env python3
"""Report all native sync pairs in a frozen experiment plan."""
import argparse
import json
from pathlib import Path

from native_sync_series import report_series


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = report_series(args.plan)
    with args.output.open("x") as output:
        json.dump(report, output, indent=2)
        output.write("\n")


if __name__ == "__main__":
    main()
