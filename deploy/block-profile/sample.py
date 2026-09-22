#!/usr/bin/env python3
"""Bounded Linux perf capture for one supervised node. No shell or host sysctl changes."""
import argparse
import decimal
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import sqlite3
import subprocess
import time
import uuid

MAX_FILE = 256 * 1024 * 1024
MAX_JSON = 12 * 1024 * 1024
RAW_BUDGET = 8_000_000_000
HEADER = re.compile(r"^\s*(\d+)(?:/|\s+)(\d+)\s+(\d+\.\d+):\s+(\S+:)\s*(.*)$")
FRAME = re.compile(r"^\s*[0-9a-fA-F]+\s+(.+?)\s+\((.+)\)\s*$")


def parse_perf(lines, pid, start, end):
    """Parse explicit perf script fields, retaining timestamps and marking malformed records."""
    samples, current, errors, size, truncated = [], None, 0, 0, False

    def append_sample(sample):
        nonlocal size, truncated
        if sample is None:
            return
        size += len(json.dumps(sample))
        if size <= MAX_JSON and len(samples) < 50_000:
            samples.append(sample)
        else:
            truncated = True

    for line in lines:
        if len(line) > 4096:
            errors += 1
            truncated = True
            break
        match = HEADER.match(line)
        if match:
            append_sample(current)
            sample_pid, tid, stamp, _event, tail = match.groups()
            mono_us = int(decimal.Decimal(stamp) * 1_000_000)
            current = {"mono_us": mono_us, "tid": int(tid), "frames": []} if int(sample_pid) == pid and start <= mono_us <= end else None
            if current is None:
                errors += 1
            line = tail
        elif not line.strip():
            append_sample(current)
            current = None
            continue
        frame = FRAME.match(line)
        if frame and current is not None:
            symbol, dso = frame.groups()
            if len(current["frames"]) < 128:
                current["frames"].append(f"{symbol} ({Path(dso).name})"[:1024])
            else:
                truncated = True
        elif line.strip() and not match:
            errors += 1
        if truncated and size > MAX_JSON:
            break
    append_sample(current)
    return samples, errors, truncated


def limits():
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_FILE, MAX_FILE))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def identity(pid, executable):
    proc = Path(f"/proc/{pid}")
    if not os.path.samefile(proc / "exe", executable):
        raise RuntimeError("PID does not match the configured node executable")
    # The command name in stat may contain spaces or parentheses.
    fields = (proc / "stat").read_text().rsplit(") ", 1)[1].split()
    return int(fields[19])


def current_run(store, pid, start_ticks):
    with sqlite3.connect(f"file:{store / 'index.sqlite'}?mode=ro", uri=True, timeout=0.1) as db:
        rows = db.execute("SELECT metadata,seen_ms FROM runs ORDER BY utc_ms DESC LIMIT 100").fetchall()
    process_start_us = start_ticks * 1_000_000 // os.sysconf("SC_CLK_TCK")
    for metadata, seen in rows:
        run = json.loads(metadata)
        if run["pid"] == pid and run["monotonic_start_us"] is not None and run["monotonic_start_us"] >= process_start_us and time.time() * 1000 - seen < 10_000:
            return run
    raise RuntimeError("No fresh profiling run matches this node process")


def prune_raw(root):
    files = sorted((p for p in root.iterdir() if p.is_file() and p.name != "sampler.lock"), key=lambda p: p.stat().st_mtime)
    size = sum(p.stat().st_size for p in files)
    for path in files:
        if size <= RAW_BUDGET - 3 * MAX_FILE:
            break
        size -= path.stat().st_size
        path.unlink()


def capture(args, stopping):
    store, executable = args.store.resolve(), args.executable.resolve(strict=True)
    os.environ["PERF_BUILDID_DIR"] = str(store / "symbols")
    raw = store / "raw"
    raw.mkdir(exist_ok=True)
    inbox = store / "inbox"
    inbox.mkdir(exist_ok=True)
    lock = (raw / "sampler.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    start_ticks = identity(args.pid, executable)
    digest = hashlib.file_digest(executable.open("rb"), "sha256").hexdigest()
    deadline = time.monotonic() + args.duration_seconds
    while time.monotonic() < deadline and not stopping[0]:
        if identity(args.pid, executable) != start_ticks:
            raise RuntimeError("Node process identity changed")
        run = current_run(store, args.pid, start_ticks)
        if sum(p.stat().st_size for p in inbox.glob("*.json")) > 128 * 1024 * 1024:
            raise RuntimeError("Collector import backlog reached its limit")
        prune_raw(raw)
        key = uuid.uuid4().hex
        output, stderr = raw / f"{key}.data", raw / f"{key}.stderr"
        start = time.clock_gettime_ns(time.CLOCK_MONOTONIC) // 1000
        command = ["perf", "record", "--no-buildid-cache", "--clockid", "mono", "-e", "cpu-clock:u", "-F", str(args.frequency), "--call-graph", "dwarf,8192", "--mmap-pages", "128", "-p", str(args.pid), "-o", str(output)]
        with stderr.open("wb") as errors:
            process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=errors, preexec_fn=limits, start_new_session=True)
            until = min(deadline, time.monotonic() + 60)
            while process.poll() is None and time.monotonic() < until and not stopping[0]:
                time.sleep(0.2)
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                code = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError("perf did not seal its capture")
        end = time.clock_gettime_ns(time.CLOCK_MONOTONIC) // 1000
        if code not in (0, 130, -signal.SIGINT):
            raise RuntimeError(f"perf capture failed ({code}). See private sampler stderr; no kernel settings were changed")
        decoded = raw / f"{key}.txt"
        with decoded.open("wb") as out, stderr.open("ab") as errors:
            # Expanding inlined frames starts addr2line, exceeding the sampler memory cap.
            subprocess.run(["perf", "script", "--no-inline", "--ns", "--show-lost-events", "-F", "pid,tid,time,event,ip,sym,dso", "-i", str(output)], stdout=out, stderr=errors, timeout=45, preexec_fn=limits, check=True)
        with decoded.open(errors="replace") as lines:
            samples, errors, truncated = parse_perf(lines, args.pid, start, end)
        # Stack order is leaf to root. Keep this explicit for flamegraph construction.
        build_ids = subprocess.run(["perf", "buildid-list", "-i", str(output)], capture_output=True, text=True, timeout=10, check=True).stdout.splitlines()
        metadata = {"run": run["id"], "pid": args.pid, "frequency": args.frequency, "clock": "monotonic", "start_mono_us": start, "end_mono_us": end, "process_start_ticks": start_ticks, "executable_sha256": digest, "build_ids": [x[:512] for x in build_ids[:512]], "decode_errors": errors, "truncated": truncated or len(build_ids) > 512, "samples": samples}
        temp = inbox / f"{key}.tmp"
        with temp.open("x") as out:
            json.dump(metadata, out, separators=(",", ":"))
            out.flush()
            os.fsync(out.fileno())
        temp.rename(temp.with_suffix(".json"))
        decoded.unlink()
        if args.once:
            break


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--frequency", type=int, choices=(19, 49), default=19)
    parser.add_argument("--duration-seconds", type=int, default=86400)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.duration_seconds <= 7 * 86400:
        parser.error("capture duration must be 1 second to 7 days")
    os.umask(0o077)
    stopping = [False]
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda _s, _f: stopping.__setitem__(0, True))
    capture(args, stopping)


if __name__ == "__main__":
    main()
