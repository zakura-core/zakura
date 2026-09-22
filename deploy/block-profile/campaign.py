#!/usr/bin/env python3
"""Bounded daily semantic benchmark dispatch. Reuses the existing weekly benchmark workflow."""
import argparse
import datetime
import fcntl
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    start = datetime.date.fromisoformat(config["start"])
    end = datetime.date.fromisoformat(config["end"])
    today = datetime.datetime.now(datetime.timezone.utc).date()
    if not 0 <= (end - start).days <= 6:
        raise ValueError("Campaigns are limited to seven dates, inclusive")
    if not start <= today <= end:
        return
    args.state.parent.mkdir(parents=True, exist_ok=True)
    with args.state.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = json.loads(args.state.read_text()) if args.state.exists() else {}
        if previous.get("day") == str(today):
            return
        # Reserve the date before dispatch. An uncertain dispatch must be inspected, never retried blindly.
        with args.state.open("w") as file:
            json.dump({"day": str(today), "status": "dispatching"}, file)
            file.flush()
            os.fsync(file.fileno())
        command = ["gh", "workflow", "run", "zakura-perf-bench.yml", "--repo", "zakura-core/zakura", "-f", f"ref={config['ref']}", "-f", "workload=historical_semantic", "-f", "profile=cpu", "-f", "teardown_after_run=true", "-f", "wall_cap_seconds=2000"]
        if config.get("baseline_ref"):
            command += ["-f", f"baseline_ref={config['baseline_ref']}"]
        subprocess.run(command, check=True, timeout=60)
        args.state.write_text(json.dumps({"day": str(today), "status": "dispatched"}))


if __name__ == "__main__":
    main()
