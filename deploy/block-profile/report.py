#!/usr/bin/env python3
"""Save a bounded daily report beside the retained profiles. Never sends messages."""
import argparse
import datetime
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", required=True, type=Path)
    parser.add_argument("--binary", default="/usr/local/bin/zakura-profile-explorer")
    args = parser.parse_args()
    os.umask(0o077)
    output = args.store / "reports"
    output.mkdir(exist_ok=True)
    lock = (output / "report.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    # A prior interrupted atomic write is not a retained report.
    for file in output.glob("*.tmp"):
        file.unlink()
    today = datetime.datetime.now(datetime.timezone.utc).date()
    since = int(datetime.datetime.combine(today - datetime.timedelta(days=1), datetime.time(), datetime.timezone.utc).timestamp() * 1000)
    with sqlite3.connect(f"file:{args.store / 'index.sqlite'}?mode=ro", uri=True, timeout=0.1) as db:
        runs = db.execute("SELECT id FROM runs WHERE seen_ms>=? ORDER BY utc_ms DESC LIMIT 100", (since,)).fetchall()
    for (run,) in runs:
        if len(run) != 32 or any(c not in "0123456789abcdef" for c in run):
            raise ValueError("invalid run identity")
        result = subprocess.run([args.binary, "report", "--store", str(args.store), "--run", run], capture_output=True, timeout=8, check=True)
        if len(result.stdout) > 2 * 1024 * 1024:
            raise ValueError("daily report exceeds bound")
        data = json.loads(result.stdout)
        temp = output / f"{today}-{run}.tmp"
        with temp.open("w") as file:
            json.dump(data, file, separators=(",", ":"))
            file.flush()
            os.fsync(file.fileno())
        temp.rename(temp.with_suffix(".json"))
    # At most 90 reports, across all nodes/runs. The quota also covers temporary files.
    files = sorted(output.glob("*.json"), key=lambda p: p.name, reverse=True)
    for file in files[90:]:
        file.unlink()


if __name__ == "__main__":
    main()
