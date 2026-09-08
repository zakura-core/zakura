#!/usr/bin/env python3
"""Capture aligned Linux thread, host, and Prometheus counters on a test node."""

import argparse
import gzip
import json
import os
from pathlib import Path
import signal
import time
import urllib.error
import urllib.request


def read_text(path):
    try:
        return path.read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None


def thread_stat(text):
    """Parse proc stat without splitting a comm containing spaces or parentheses."""
    end = text.rindex(")")
    fields = text[end + 2:].split()
    return {
        "comm": text[text.index("(") + 1:end],
        "state": fields[0],
        "utime_ticks": int(fields[11]),
        "stime_ticks": int(fields[12]),
        "start_ticks": int(fields[19]),
        "rss_pages": int(fields[21]),
        "processor": int(fields[36]),
        "blkio_delay_ticks": int(fields[39]),
    }


def capture(pid, metrics_url):
    proc = Path("/proc")
    task = proc / str(pid)
    stat = read_text(task / "stat")
    if stat is None:
        return None
    row = {
        "epoch_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "process": thread_stat(stat),
        "threads": {},
        "io": read_text(task / "io"),
        "host_stat": read_text(proc / "stat"),
        "diskstats": read_text(proc / "diskstats"),
        "netdev": read_text(proc / "net/dev"),
        "meminfo": read_text(proc / "meminfo"),
        "pressure": {k: read_text(proc / "pressure" / k) for k in ("cpu", "io", "memory")},
    }
    for thread in sorted((task / "task").glob("[0-9]*")):
        stat = read_text(thread / "stat")
        if stat is None:
            continue
        row["threads"][thread.name] = {
            **thread_stat(stat),
            "schedstat": read_text(thread / "schedstat"),
            "wchan": read_text(thread / "wchan"),
            "status": read_text(thread / "status"),
        }
    row["metrics_started_monotonic_ns"] = time.monotonic_ns()
    try:
        with urllib.request.urlopen(metrics_url, timeout=0.75) as response:
            # Bound output even if the endpoint behaves unexpectedly.
            body = response.read(8 * 1024 * 1024 + 1)
        if len(body) > 8 * 1024 * 1024:
            raise ValueError("metrics response exceeds 8 MiB")
        row["metrics"] = body.decode("utf-8")
    except (OSError, urllib.error.URLError, ValueError) as error:
        row["metrics_error"] = str(error)
    row["finished_monotonic_ns"] = time.monotonic_ns()
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--metrics-url", default="http://127.0.0.1:9999/metrics")
    args = parser.parse_args()
    if args.pid <= 0 or not 1 <= args.seconds <= 7200:
        parser.error("positive PID and a duration of 1–7200 seconds required")
    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    args.out.mkdir(parents=True, exist_ok=True)
    metadata = {
        "pid": args.pid,
        "epoch_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
        "page_size": os.sysconf("SC_PAGE_SIZE"),
        "sched_schedstats": read_text(Path("/proc/sys/kernel/sched_schedstats")),
        "interval_seconds": 1,
        "maximum_seconds": args.seconds,
    }
    (args.out / "sampling.json").write_text(json.dumps(metadata, indent=2) + "\n")
    start = time.monotonic()
    start_ticks = None
    with gzip.open(args.out / "system-timeline.jsonl.gz", "wt") as output:
        while running and time.monotonic() - start < args.seconds:
            tick = time.monotonic()
            row = capture(args.pid, args.metrics_url)
            if row is None:
                break
            if start_ticks is None:
                start_ticks = row["process"]["start_ticks"]
            if row["process"]["start_ticks"] != start_ticks:
                break  # Never follow a reused PID after the node exits.
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
            output.flush()
            time.sleep(max(0, 1 - (time.monotonic() - tick)))


if __name__ == "__main__":
    main()
