#!/usr/bin/env python3
"""Continuous CPU profiler for one Zakura node.

Runs beside the node and never inside it. In a loop it asks `perf` to sample where the
node's threads are on the CPU for one window (a minute by default), turns the samples
into a tree of functions and CPU time, and stores one small file per window and one per
hour. A loopback HTTP server answers with flame graphs for a time range, a CPU timeline
and the profiler's own status; the cluster status dashboard forwards those to the public
page.

It is built to stay out of the node's way: the systemd unit runs it at idle priority
under a hard CPU and memory ceiling, only the `perf record` child runs at normal
priority, and the sampling rate drops when the node is busy so a window never holds more
than a fixed number of samples.
"""

import argparse
import bisect
import collections
import gzip
import json
import operator
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import blocks  # noqa: E402
import stacks  # noqa: E402

WINDOW_SECONDS = 60
# Sampling rates the profiler moves between, highest first.
RATE_LADDER = (199, 149, 99, 49, 29, 19)
# The most samples one window may hold; the rate drops to stay under it.
SAMPLE_BUDGET = 3000
# Bytes of user stack perf copies per sample for unwinding: the most perf allows.
# The node's async call chains are deep; measured on mainnet, two thirds of stacks
# were cut short at 8 KiB and one in fourteen at this size.
STACK_BYTES = 65528
# perf stops a window early at this size, so a sudden burst of node activity cannot
# fill the scratch directory before the rate has adapted.
MAX_WINDOW_BYTES = "256M"
# A sample counts as "while a block was processed" from this long before the block's
# announcement until this long after it.
BLOCK_LEAD_SECONDS = 0.25
BLOCK_TAIL_SECONDS = 0.10
MINUTE_RETENTION_SECONDS = 48 * 3600
HOUR_RETENTION_SECONDS = 14 * 86400
STORE_CAP_BYTES = 2 * 1024**3
RANGES = {"15m": 900, "1h": 3600, "6h": 6 * 3600, "24h": 86400, "7d": 7 * 86400}
LENSES = ("all", "block")
TIMELINE_BUCKETS = 240
MAX_TIMELINE_BLOCKS = 2000
MAX_FOCUS_LENGTH = 300
# Per-block stage records kept, about a week of blocks.
MAX_STAGE_RECORDS = 10_000
DEFAULT_TRACE_TABLE_CAP_MB = 200
# What the page gets of each block's stage record.
BLOCK_FIELDS = ("height", "hash", "t", "path", "size", "status_peers", "status_slowest_ms", "reorg", "ms")
# What the cluster view needs of a block to join it with the same block on other nodes.
PROPAGATION_FIELDS = ("height", "hash", "t", "path", "reorg", "ms", "wall", "heard_from", "peers", "unnamed")
# How far this host's clock is from true time, as `chronyc tracking` prints it.
CLOCK_OFFSET = re.compile(r"System time\s*:\s*([0-9.]+) seconds (slow|fast)")
CLOCK_RMS = re.compile(r"RMS offset\s*:\s*([0-9.]+) seconds")
# `timedatectl timesync-status` prints each as one number and a unit.
TIMESYNC_OFFSET = re.compile(r"^\s*Offset:\s*([+-]?[0-9.]+)(us|ms|s)\s*$", re.MULTILINE)
TIMESYNC_JITTER = re.compile(r"^\s*Jitter:\s*([0-9.]+)(us|ms|s)\s*$", re.MULTILINE)
TIMESYNC_UNIT_MS = {"us": 0.001, "ms": 1.0, "s": 1000.0}
CLOCK_CHECK_SECONDS = 60
COMPUTE_WAIT_SECONDS = 20
# Flame graphs that are not cached may use this much CPU time between them in any
# minute. Past it the API answers "busy", so requests for many different ranges cannot
# take the CPU that processing new windows needs.
QUERY_CPU_BUDGET_SECONDS = 10
QUERY_BUDGET_PERIOD_SECONDS = 60
# A `perf script` run normally takes a few seconds; one still running after this is
# stopped so the next windows are processed.
PERF_SCRIPT_TIMEOUT_SECONDS = 120
# A window is late when no new one has been stored for this many window lengths.
STALE_WINDOWS = 4
PERF_SCRIPT_FIELDS = "comm,tid,time,ip,sym,dso"


def next_rate(rate, samples, budget=SAMPLE_BUDGET):
    """The sampling rate for the next window, given how many samples the last one took.

    Picks the highest rate on the ladder that would have stayed within `budget` at the
    load just seen, so an idle node is sampled finely and a busy one cheaply.
    """
    for candidate in RATE_LADDER:
        if samples * candidate / rate <= budget:
            return candidate
    return RATE_LADDER[-1]


def in_block_window(when, block_times):
    """Whether `when` falls inside the processing window of a block in `block_times`."""
    index = bisect.bisect_left(block_times, when - BLOCK_TAIL_SECONDS)
    return index < len(block_times) and block_times[index] - BLOCK_LEAD_SECONDS <= when


def build_window(samples, symbols, categorizer, rate, block_times):
    """Turn one window's samples into a tree and its per-category CPU totals.

    `samples` are `(comm, time, raw_frames)` as `stacks.parse_perf_script` yields them.
    `block_times` is the sorted list of block announcement times. Returns
    `(tree, totals)` where `totals` has the sample count, CPU microseconds overall, in
    block windows, and by category.
    """
    samples = list(samples)
    symbols.resolve(symbol for _, _, frames in samples for _, symbol in frames)
    weight = round(1_000_000 / rate)
    tree = stacks.new_tree()
    by_category = collections.Counter()
    block_us = 0
    for comm, when, raw_frames in samples:
        frames = [symbols.frame_name(address, symbol) for address, symbol in reversed(raw_frames)]
        in_block = in_block_window(when, block_times)
        stacks.add_stack(
            tree,
            [stacks.THREAD_PREFIX + stacks.thread_group(comm), *frames],
            weight,
            weight if in_block else 0,
        )
        by_category[categorizer.stack(frames)] += weight
        if in_block:
            block_us += weight
    totals = {
        "samples": len(samples),
        "cpu_us": weight * len(samples),
        "block_us": block_us,
        "by_category": dict(by_category),
    }
    return tree, totals


def sampled_seconds(window):
    """How long a stored window actually sampled for.

    A window's start and end bracket the whole `perf record` run, which spends a
    few seconds attaching to the node's threads before it samples and writing its
    file after. Rates such as cores busy divide by the sampling time alone.
    """
    return window.get("sampled_seconds", window["end"] - window["start"])


def write_json_gz(path, document):
    """Write `document` to `path` so a reader never sees a partial file."""
    temporary = path.with_name(path.name + ".tmp")
    with gzip.open(temporary, "wt", compresslevel=5) as handle:
        json.dump(document, handle, separators=(",", ":"))
    os.replace(temporary, path)


def read_json_gz(path):
    with gzip.open(path, "rt") as handle:
        return json.load(handle)


def json_line(row):
    return json.dumps(row, separators=(",", ":")) + "\n"


class JsonlLog:
    """A file of one JSON row per line: appended to, and rewritten whole to drop old rows."""

    def __init__(self, path):
        self.path = path

    def read(self):
        rows = []
        try:
            with open(self.path) as handle:
                for line in handle:
                    try:
                        rows.append(json.loads(line))
                    except ValueError:
                        continue  # a line cut short by a crash
        except FileNotFoundError:
            pass
        return rows

    def append(self, rows):
        with open(self.path, "a") as handle:
            handle.writelines(json_line(row) for row in rows)

    def rewrite(self, rows):
        """Replace the file's rows so a reader never sees a partial file."""
        temporary = self.path.with_name(self.path.name + ".tmp")
        with open(temporary, "w") as handle:
            handle.writelines(json_line(row) for row in rows)
        os.replace(temporary, self.path)


class BoundedCache:
    """Remembers the `size` most recently stored values; the oldest goes first."""

    def __init__(self, size):
        self.size = size
        self._values = collections.OrderedDict()

    def get(self, key):
        return self._values.get(key)

    def put(self, key, value):
        """Store `value` under `key` and return it."""
        self._values[key] = value
        while len(self._values) > self.size:
            self._values.popitem(last=False)
        return value


_window_start = operator.itemgetter("start")
_block_time = operator.attrgetter("time")


class Store:
    """The profile on disk: one file per window, one merged file per hour, an index.

    The index holds each window's summary (times, rate, CPU by category), which is all
    the timeline needs, so it outlives the per-window files.
    """

    def __init__(self, root):
        self.root = Path(root)
        self.minutes = self.root / "min"
        self.hours = self.root / "hour"
        for directory in (self.minutes, self.hours):
            directory.mkdir(parents=True, exist_ok=True)
        self.index_log = JsonlLog(self.root / "index.jsonl")
        self.blocks_log = JsonlLog(self.root / "blocks.jsonl")
        self.stages_log = JsonlLog(self.root / "block_stages.jsonl")
        self._lock = threading.Lock()
        # Window summaries sorted by start, and `blocks.Block`s sorted by time.
        self.windows = sorted(self.index_log.read(), key=_window_start)
        self.blocks = sorted(blocks.Block(*row) for row in self.blocks_log.read())
        self.block_stages = self.stages_log.read()[-MAX_STAGE_RECORDS:]
        self._drop_older_than(time.time() - HOUR_RETENTION_SECONDS)
        # Rewrite the files without the rows that aged out.
        self.index_log.rewrite(self.windows)
        self.blocks_log.rewrite(self.blocks)
        self.stages_log.rewrite(self.block_stages)

    def _drop_older_than(self, horizon):
        """Forget window summaries and block announcements from before `horizon`."""
        with self._lock:
            self.windows = [w for w in self.windows if w["end"] >= horizon]
            self.blocks = [b for b in self.blocks if b.time >= horizon]

    def add_window(self, summary, tree):
        """Store one finished window."""
        write_json_gz(
            self.minutes / f"{int(summary['start'])}.json.gz",
            {"summary": summary, "tree": stacks.encode_tree(tree)},
        )
        hour_path = self.hours / f"{int(summary['start']) // 3600 * 3600}.json.gz"
        if hour_path.exists():
            # Processing fell behind and the window's hour was merged before it was
            # stored. Ranges read whole hours from the hourly file, so add it there too.
            document = read_json_gz(hour_path)
            hour_tree = stacks.decode_tree(document["tree"])
            stacks.merge_into(hour_tree, tree)
            write_json_gz(hour_path, {**document, "tree": stacks.encode_tree(hour_tree)})
        self.index_log.append([summary])
        with self._lock:
            self.windows.append(summary)

    def add_blocks(self, announcements):
        """Remember block announcements (`blocks.Block`s) not seen before."""
        with self._lock:
            known = {b.hash for b in self.blocks[-500:]}
            new = [blocks.Block(*b) for b in announcements if b[2] not in known]
            self.blocks.extend(new)
            self.blocks.sort()
        if new:
            self.blocks_log.append(new)

    def add_block_stages(self, records):
        """Remember the per-stage timings of blocks that just finished."""
        if not records:
            return
        self.stages_log.append(records)
        with self._lock:
            self.block_stages = (self.block_stages + records)[-MAX_STAGE_RECORDS:]

    def recent_block_stages(self, limit):
        """The newest stage records, oldest first, each with its announcement time."""
        with self._lock:
            records = self.block_stages[-limit:]
            announced = {block.hash: block.time for block in self.blocks[-(limit + 500) :]}
        return [{**record, "t": announced.get(record["hash"])} for record in records]

    def block_times(self, start, end):
        """Announcement times in `[start, end]`, oldest first."""
        with self._lock:
            first = bisect.bisect_left(self.blocks, start, key=_block_time)
            last = bisect.bisect_right(self.blocks, end, key=_block_time)
            return [b.time for b in self.blocks[first:last]]

    def windows_between(self, start, end):
        """Summaries of the windows that started in `[start, end)`, oldest first."""
        with self._lock:
            first = bisect.bisect_left(self.windows, start, key=_window_start)
            last = bisect.bisect_left(self.windows, end, key=_window_start)
            return self.windows[first:last]

    def latest_end(self):
        with self._lock:
            return self.windows[-1]["end"] if self.windows else None

    def merged_tree(self, start, end):
        """One tree of every window that started in `[start, end)`.

        Whole hours inside the range come from the hourly files; the edges come from
        the per-window files while those still exist.
        """
        tree = stacks.new_tree()
        merged_hours = set()
        hour = int(start) // 3600 * 3600
        if hour < start:
            hour += 3600
        while hour + 3600 <= end:
            path = self.hours / f"{hour}.json.gz"
            if path.exists():
                stacks.merge_into(tree, stacks.decode_tree(read_json_gz(path)["tree"]))
                merged_hours.add(hour)
            hour += 3600
        for summary in self.windows_between(start, end):
            window_start = int(summary["start"])
            if window_start // 3600 * 3600 in merged_hours:
                continue
            path = self.minutes / f"{window_start}.json.gz"
            try:
                document = read_json_gz(path)
            except FileNotFoundError:
                continue
            stacks.merge_into(tree, stacks.decode_tree(document["tree"]))
        return tree

    def roll_up_hours(self, now):
        """Write the hourly file for every finished hour that lacks one."""
        finished_before = (int(now) - 120) // 3600 * 3600
        pending = collections.defaultdict(list)
        for path in self.minutes.glob("*.json.gz"):
            window_start = int(path.name.split(".")[0])
            hour = window_start // 3600 * 3600
            if hour < finished_before and not (self.hours / f"{hour}.json.gz").exists():
                pending[hour].append(path)
        for hour, paths in sorted(pending.items()):
            tree = stacks.new_tree()
            for path in sorted(paths):
                stacks.merge_into(tree, stacks.decode_tree(read_json_gz(path)["tree"]))
            write_json_gz(
                self.hours / f"{hour}.json.gz",
                {"start": hour, "end": hour + 3600, "tree": stacks.encode_tree(tree)},
            )

    def prune(self, now):
        """Delete files past their retention, then the oldest until under the size cap."""
        aged = [
            (self.minutes, now - MINUTE_RETENTION_SECONDS),
            (self.hours, now - HOUR_RETENTION_SECONDS),
        ]
        remaining = []
        for directory, horizon in aged:
            for path in directory.glob("*.json.gz"):
                started = int(path.name.split(".")[0])
                # A per-window file is only dropped once its hour has been merged.
                merged = directory is self.hours or (
                    self.hours / f"{started // 3600 * 3600}.json.gz"
                ).exists()
                if started < horizon and merged:
                    path.unlink(missing_ok=True)
                else:
                    remaining.append((started, path, path.stat().st_size))
        total = sum(size for _, _, size in remaining)
        for _, path, size in sorted(remaining):
            if total <= STORE_CAP_BYTES:
                break
            path.unlink(missing_ok=True)
            total -= size
        self._drop_older_than(now - HOUR_RETENTION_SECONDS)

    def size_bytes(self):
        return sum(
            path.stat().st_size
            for directory in (self.minutes, self.hours)
            for path in directory.glob("*.json.gz")
        )


def parse_clock_offset(tracking):
    """`{"offset_ms", "rms_ms"}` from the output of `chronyc tracking`, else `None`.

    `offset_ms` is how far the system clock is from true time right now, positive when
    it is ahead; `rms_ms` is its typical distance over the recent past.
    """
    offset, rms = CLOCK_OFFSET.search(tracking), CLOCK_RMS.search(tracking)
    if offset is None or rms is None:
        return None
    sign = 1 if offset.group(2) == "fast" else -1
    return {
        "offset_ms": round(sign * float(offset.group(1)) * 1000, 3),
        "rms_ms": round(float(rms.group(1)) * 1000, 3),
    }


def parse_timesync_offset(status):
    """The same from the output of `timedatectl timesync-status`, else `None`.

    systemd-timesyncd reports the last measured offset, positive when the clock is
    behind, and the jitter between measurements, which stands in for `rms_ms`.
    """
    offset, jitter = TIMESYNC_OFFSET.search(status), TIMESYNC_JITTER.search(status)
    if offset is None or jitter is None:
        return None
    return {
        "offset_ms": round(-float(offset.group(1)) * TIMESYNC_UNIT_MS[offset.group(2)], 3),
        "rms_ms": round(float(jitter.group(1)) * TIMESYNC_UNIT_MS[jitter.group(2)], 3),
    }


# The time services a node may run, with how to read each one's answer.
CLOCK_SOURCES = (
    (("chronyc", "tracking"), parse_clock_offset),
    (("timedatectl", "timesync-status"), parse_timesync_offset),
)
_clock = {"checked": 0.0, "offset": None}


def clock_offset():
    """This host's clock offset, asked of its time service at most once a minute.

    `None` when the host runs neither chrony nor systemd-timesyncd.
    """
    now = time.monotonic()
    if now - _clock["checked"] >= CLOCK_CHECK_SECONDS or not _clock["checked"]:
        _clock["checked"] = now
        _clock["offset"] = None
        for command, parse in CLOCK_SOURCES:
            try:
                done = subprocess.run(command, capture_output=True, text=True, timeout=2, check=False)
            except (OSError, subprocess.SubprocessError):
                continue
            _clock["offset"] = parse(done.stdout)
            if _clock["offset"] is not None:
                break
    return _clock["offset"]


def use_normal_scheduling():
    """Run the calling process at normal priority, for the child that takes samples.

    The service as a whole runs at idle priority. The sampler must not: it only copies
    ring buffers, and starving it loses samples exactly when the node is busiest.
    """
    try:
        os.sched_setscheduler(0, os.SCHED_OTHER, os.sched_param(0))
        os.setpriority(os.PRIO_PROCESS, 0, 0)
    except (AttributeError, OSError):
        pass


class Profiler:
    """Samples the node window after window and stores what it finds."""

    def __init__(self, args, store):
        self.args = args
        self.store = store
        self.symbols = stacks.SymbolNames()
        self.categorizer = stacks.Categorizer()
        self.log = blocks.FileFollower(args.log_file) if args.log_file else None
        self.traces = (
            blocks.TraceFollower(args.trace_dir, args.trace_table_cap_mb * 1024 * 1024)
            if args.trace_dir
            else None
        )
        self.rate = RATE_LADDER[0]
        self.state = "starting"
        self.build = ""
        self.started_at = time.time()
        self.dropped_windows = 0
        self.last_error = ""
        self._build_pid = None
        self._stop = threading.Event()
        self._perf = None
        self._jobs = collections.deque()
        self._job_ready = threading.Condition()
        self._cpu_mark = sum(os.times()[:4])
        self._trace_checked_at = None

    def fail(self, summary, detail=""):
        """Record a failure: `summary` is shown on the public page, `detail` goes only to the journal."""
        self.state = "error"
        self.last_error = summary
        print(f"{summary}: {detail}" if detail else summary, file=sys.stderr, flush=True)

    def stop(self):
        self._stop.set()
        perf = self._perf
        if perf is not None and perf.poll() is None:
            perf.terminate()
        with self._job_ready:
            self._job_ready.notify_all()

    def main_pid(self):
        """The node's process id, or `None` while the service is not running."""
        try:
            output = subprocess.run(
                ["systemctl", "show", self.args.service, "-p", "MainPID", "--value"],
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None
        return int(output) if output.isdigit() and int(output) > 0 else None

    def node_build(self, pid):
        """The version of the binary the node is running, read once per process."""
        if pid != self._build_pid:
            try:
                self.build = subprocess.run(
                    [f"/proc/{pid}/exe", "--version"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout.strip().removeprefix("zakurad ")
            except (OSError, subprocess.SubprocessError):
                self.build = ""
            self._build_pid = pid
        return self.build

    def record_loop(self):
        """Take one window after another until stopped."""
        scratch = Path(self.args.scratch_dir)
        scratch.mkdir(parents=True, exist_ok=True)
        for leftover in scratch.glob("*.data"):
            leftover.unlink(missing_ok=True)
        while not self._stop.is_set():
            pid = self.main_pid()
            if pid is None:
                self.state = "waiting for node"
                self._stop.wait(10)
                continue
            build = self.node_build(pid)
            rate = self.rate
            data_path = scratch / f"{int(time.time())}.data"
            started = time.time()
            command = [
                self.args.perf, "record", "-e", "cpu-clock", "-k", "CLOCK_REALTIME",
                "-F", str(rate), "-g", "--call-graph", f"dwarf,{STACK_BYTES}",
                "-p", str(pid), "-o", str(data_path), f"--max-size={MAX_WINDOW_BYTES}",
                "--", "sleep", str(self.args.window),
            ]  # fmt: skip
            try:
                self._perf = subprocess.Popen(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    preexec_fn=use_normal_scheduling,
                )
                _, errors = self._perf.communicate(timeout=self.args.window + 30)
            except subprocess.TimeoutExpired:
                self._perf.kill()
                _, errors = self._perf.communicate()
            except OSError as error:
                self.fail("perf record could not start", str(error))
                self._stop.wait(30)
                continue
            ended = time.time()
            if self._stop.is_set():
                data_path.unlink(missing_ok=True)
                break
            if not data_path.exists() or data_path.stat().st_size == 0:
                self.fail("perf record wrote no data", errors.decode(errors="replace")[-300:])
                data_path.unlink(missing_ok=True)
                self._stop.wait(10)
                continue
            job = {
                "path": data_path, "start": started, "end": ended,
                "rate": rate, "pid": pid, "build": build,
            }  # fmt: skip
            with self._job_ready:
                if len(self._jobs) >= 2:
                    # Processing has fallen behind: give up the oldest window rather
                    # than let scratch files pile up.
                    self._jobs.popleft()["path"].unlink(missing_ok=True)
                    self.dropped_windows += 1
                self._jobs.append(job)
                self._job_ready.notify()

    def process_loop(self):
        """Turn recorded windows into stored trees, one at a time."""
        while not self._stop.is_set():
            with self._job_ready:
                while not self._jobs and not self._stop.is_set():
                    self._job_ready.wait(5)
                if self._stop.is_set():
                    return
                job = self._jobs.popleft()
            try:
                self.process_window(job)
                self.state = "ok"
                self.last_error = ""
            except Exception as error:  # noqa: BLE001 - one bad window must not end profiling
                self.fail(f"processing a window failed ({type(error).__name__})", str(error))
            finally:
                job["path"].unlink(missing_ok=True)
            try:
                self.store.roll_up_hours(time.time())
                self.store.prune(time.time())
            except OSError as error:
                self.fail(f"storing the profile failed ({type(error).__name__})", str(error))

    def trace_loop(self):
        """Read and cap trace files even when CPU sampling cannot produce a window."""
        if self.traces is None:
            return
        while not self._stop.is_set():
            try:
                self.store.add_block_stages(self.traces.poll())
                untrimmed = self.traces.trim_failure
                if untrimmed is None:
                    self._trace_checked_at = time.monotonic()
                else:
                    # The poll read the files but left one over the cap: not ready.
                    self._trace_checked_at = None
                    self.fail(f"trimming trace files failed ({type(untrimmed).__name__})", str(untrimmed))
            except Exception as error:  # noqa: BLE001 - keep retention alive after a bad row or failed write
                self._trace_checked_at = None
                self.fail(f"following trace files failed ({type(error).__name__})", str(error))
            self._stop.wait(5)

    def process_window(self, job):
        # The block announced at the very end of the window reaches the log a moment
        # later; wait for it so its samples are tagged.
        self._stop.wait(max(0.0, job["end"] + BLOCK_TAIL_SECONDS + 0.5 - time.time()))
        if self.log is not None:
            self.store.add_blocks(blocks.announcements(self.log.read_new_lines()))
        block_times = self.store.block_times(
            job["start"] - BLOCK_TAIL_SECONDS, job["end"] + BLOCK_LEAD_SECONDS
        )
        tree, totals, errors = self.run_perf_script(job, block_times)
        cpu_now = sum(os.times()[:4])
        summary = {
            "start": round(job["start"], 3),
            "end": round(job["end"], 3),
            "sampled_seconds": min(self.args.window, round(job["end"] - job["start"], 3)),
            "rate": job["rate"],
            "pid": job["pid"],
            "build": job["build"],
            "lost": "lost" in errors and "chunks" in errors,
            "overhead_cpu_s": round(cpu_now - self._cpu_mark, 3),
            **totals,
        }
        self._cpu_mark = cpu_now
        self.store.add_window(summary, tree)
        self.rate = next_rate(job["rate"], totals["samples"])

    def run_perf_script(self, job, block_times):
        """Read one recorded window through `perf script`; returns `(tree, totals, stderr text)`.

        The error output goes to a file, so a run that writes many warnings cannot stall
        on a full pipe, and a run that hangs is stopped after PERF_SCRIPT_TIMEOUT_SECONDS.
        """
        timed_out = threading.Event()
        with tempfile.TemporaryFile(dir=self.args.scratch_dir) as errors_file:
            script = subprocess.Popen(
                [self.args.perf, "script", "-i", str(job["path"]), "--no-inline",
                 "-F", PERF_SCRIPT_FIELDS],  # fmt: skip
                stdout=subprocess.PIPE,
                stderr=errors_file,
                text=True,
                errors="replace",
            )

            def stop_script():
                timed_out.set()
                script.kill()

            watchdog = threading.Timer(PERF_SCRIPT_TIMEOUT_SECONDS, stop_script)
            watchdog.start()
            try:
                tree, totals = build_window(
                    stacks.parse_perf_script(script.stdout),
                    self.symbols,
                    self.categorizer,
                    job["rate"],
                    block_times,
                )
            finally:
                watchdog.cancel()
                script.stdout.close()
                script.wait()
            if timed_out.is_set():
                raise TimeoutError(f"perf script ran longer than {PERF_SCRIPT_TIMEOUT_SECONDS} s")
            errors_file.seek(0)
            return tree, totals, errors_file.read().decode(errors="replace")

    def status(self):
        now = time.time()
        latest = self.store.latest_end()
        hour = self.store.windows_between(now - 3600, now)
        covered = sum(sampled_seconds(w) for w in hour)
        state = self.state
        fresh = latest is not None and now - latest <= STALE_WINDOWS * self.args.window
        if state == "starting" and fresh:
            # Restarted with recent data on disk: nothing is missing yet.
            state = "ok"
        elif state == "ok" and not fresh:
            state = "stale"
        return {
            "node": self.args.node_name,
            "state": state,
            "error": self.last_error,
            "build": self.build,
            "rate_hz": self.rate,
            "window_seconds": self.args.window,
            "last_window_end": latest,
            "started_at": self.started_at,
            "samples_last_hour": sum(w["samples"] for w in hour),
            "node_cpu_cores_last_hour": (
                round(sum(w["cpu_us"] for w in hour) / 1e6 / covered, 4) if covered else None
            ),
            "overhead_pct_last_hour": (
                round(100 * sum(w["overhead_cpu_s"] for w in hour) / covered, 2) if covered else None
            ),
            "lost_windows_last_hour": sum(1 for w in hour if w.get("lost")),
            "dropped_windows": self.dropped_windows,
            "retention_days": HOUR_RETENTION_SECONDS // 86400,
            "block_window_ms": [round(BLOCK_LEAD_SECONDS * 1000), round(BLOCK_TAIL_SECONDS * 1000)],
            "stage_timeline": self.traces is not None,
            "trace_retention": (
                {
                    "directory": self.args.trace_dir,
                    "cap_mb": self.args.trace_table_cap_mb,
                    "ready": self._trace_checked_at is not None and time.monotonic() - self._trace_checked_at < 30,
                }
                if self.traces is not None else None
            ),
        }


class Queries:
    """Answers the page's questions from the store, caching what is expensive."""

    def __init__(self, store, profiler):
        self.store = store
        self.profiler = profiler
        self._compute = threading.Lock()
        self._trees = BoundedCache(3)
        self._flames = BoundedCache(48)
        # `(finished_at, cpu_seconds)` of recent uncached flame graphs, oldest first.
        self._spent = collections.deque()

    def _over_budget(self, now):
        """Whether uncached flame graphs used up QUERY_CPU_BUDGET_SECONDS in the last period."""
        while self._spent and self._spent[0][0] < now - QUERY_BUDGET_PERIOD_SECONDS:
            self._spent.popleft()
        return sum(seconds for _, seconds in self._spent) >= QUERY_CPU_BUDGET_SECONDS

    def resolve_range(self, params):
        """`(start, end)` for a request's `range` or `from`/`to` parameters."""
        if "from" in params or "to" in params:
            start, end = float(params["from"]), float(params["to"])
            if not (0 < start < end and end - start <= HOUR_RETENTION_SECONDS + 3600):
                raise ValueError("bad from/to")
            return start, end
        span = RANGES[params.get("range", "1h")]
        end = self.store.latest_end() or time.time()
        return end - span, end

    def flame(self, start, end, view, lens, focus):
        """The flame graph document for a range, view and lens, as JSON bytes."""
        key = (round(start), round(end), view, lens, focus)
        cached = self._flames.get(key)
        if cached is not None:
            return cached
        if not self._compute.acquire(timeout=COMPUTE_WAIT_SECONDS):
            raise TimeoutError("profile is busy")
        cpu_started = time.thread_time()
        try:
            if self._over_budget(time.monotonic()):
                raise TimeoutError("flame graph CPU budget used up")
            tree = self._trees.get(key[:2])
            if tree is None:
                tree = self._trees.put(key[:2], self.store.merged_tree(start, end))
            block_only = lens == "block"
            whole_us = stacks.total_us(tree, block_only)
            if focus:
                tree = stacks.function_tree(tree, focus)
            else:
                tree = stacks.view_tree(tree, view, self.profiler.categorizer)
            document = stacks.flame_document(tree, block_only)
            windows = self.store.windows_between(start, end)
            document.update(
                {
                    "from": start,
                    "to": end,
                    "view": view,
                    "lens": lens,
                    "focus": focus,
                    "whole_us": whole_us,
                    "covered_seconds": round(sum(sampled_seconds(w) for w in windows), 1),
                    "samples": sum(w["samples"] for w in windows),
                    "blocks": len(self.store.block_times(start, end)),
                    "categories": [self.profiler.categorizer.frame(n) for n in document["names"]],
                }
            )
            return self._flames.put(key, json.dumps(document, separators=(",", ":")).encode())
        finally:
            self._spent.append((time.monotonic(), time.thread_time() - cpu_started))
            self._compute.release()

    def timeline(self, start, end):
        """CPU by category over time, in at most `TIMELINE_BUCKETS` steps."""
        windows = self.store.windows_between(start, end)
        step = max(self.profiler.args.window, (end - start) / TIMELINE_BUCKETS)
        buckets = {}
        builds = []
        for window in windows:
            slot = int((window["start"] - start) // step)
            bucket = buckets.setdefault(
                slot, {"t": start + slot * step, "seconds": 0.0, "us": collections.Counter()}
            )
            bucket["seconds"] += sampled_seconds(window)
            bucket["us"].update(window["by_category"])
            if not builds or builds[-1][1] != window["build"]:
                builds.append([window["start"], window["build"]])
        block_times = self.store.block_times(start, end)
        return {
            "from": start,
            "to": end,
            "step": step,
            "buckets": [
                {"t": round(b["t"], 1), "seconds": round(b["seconds"], 1), "us": dict(b["us"])}
                for _, b in sorted(buckets.items())
            ],
            "blocks": [round(t, 2) for t in block_times] if len(block_times) <= MAX_TIMELINE_BLOCKS else None,
            "block_count": len(block_times),
            "builds": builds,
        }

    def block_stages(self, limit):
        """How long recent blocks spent in each stage, and the median per stage.

        Medians are kept apart by how the block arrived: the two networks record
        different stages, so one median across both would describe neither.
        """
        records = self.store.recent_block_stages(limit)
        paths = {}
        for record in records:
            paths.setdefault(record["path"], []).append(record["ms"])
        for path, timings in paths.items():
            medians, slowest = {}, {}
            for stage, _, _ in blocks.STAGES:
                values = sorted(ms[stage] for ms in timings if ms.get(stage) is not None)
                if values:
                    medians[stage] = values[len(values) // 2]
                    slowest[stage] = values[min(len(values) - 1, len(values) * 9 // 10)]
            paths[path] = {"blocks": len(timings), "median_ms": medians, "p90_ms": slowest}
        return {
            "stages": [{"id": s, "label": label, "where": where} for s, label, where in blocks.STAGES],
            "paths": paths,
            "fanout_window_ms": blocks.FANOUT_MICROSECONDS // 1000,
            "blocks": [{key: record[key] for key in BLOCK_FIELDS} for record in records],
        }

    def propagation(self, limit):
        """Recent blocks with the wall-clock times the cluster view joins across nodes.

        `t` is when the node log announced the block, on the system clock: a second
        reading of the tip time, which shows whether the trace clock can be trusted.
        """
        records = self.store.recent_block_stages(limit)
        return {
            "node": self.profiler.args.node_name,
            "clock": clock_offset(),
            "blocks": [
                {key: record[key] for key in PROPAGATION_FIELDS if key in record}
                for record in records
                if record.get("wall")
            ],
        }

    def meta(self):
        """What the page needs before its first query: views, colours, ranges."""
        labels = {c["id"]: c["label"] for c in stacks.CATEGORIES}
        labels[stacks.KERNEL_CATEGORY["id"]] = stacks.KERNEL_CATEGORY["label"]
        labels[stacks.OTHER_CATEGORY["id"]] = stacks.OTHER_CATEGORY["label"]
        return {
            "categories": [{"id": c, "label": labels[c]} for c in stacks.category_ids()],
            "views": stacks.view_ids(),
            "ranges": list(RANGES),
        }


def make_handler(queries, profiler, page_path):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        timeout = 15

        def log_message(self, *args):
            pass

        def reply(self, status, body, content_type):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def reply_json(self, status, document):
            body = document if isinstance(document, bytes) else json.dumps(document).encode()
            self.reply(status, body, "application/json")

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            params = dict(urllib.parse.parse_qsl(parsed.query))
            try:
                if parsed.path == "/":
                    return self.reply(200, page_path.read_bytes(), "text/html; charset=utf-8")
                if parsed.path == "/healthz":
                    return self.reply(200, b"ok\n", "text/plain; charset=utf-8")
                if parsed.path == "/api/status":
                    return self.reply_json(200, {**profiler.status(), **queries.meta()})
                if parsed.path == "/api/flame":
                    view = params.get("view", stacks.ALL_VIEW)
                    lens = params.get("lens", "all")
                    focus = params.get("focus", "")
                    if view not in stacks.view_ids() or lens not in LENSES or len(focus) > MAX_FOCUS_LENGTH:
                        raise ValueError("bad view, lens or focus")
                    start, end = queries.resolve_range(params)
                    return self.reply_json(200, queries.flame(start, end, view, lens, focus))
                if parsed.path == "/api/timeline":
                    return self.reply_json(200, queries.timeline(*queries.resolve_range(params)))
                if parsed.path == "/api/blocks":
                    limit = max(1, min(int(params.get("limit", "200")), 1000))
                    return self.reply_json(200, queries.block_stages(limit))
                if parsed.path == "/api/propagation":
                    limit = max(1, min(int(params.get("limit", "200")), 1000))
                    return self.reply_json(200, queries.propagation(limit))
            except (KeyError, ValueError):
                return self.reply_json(400, {"error": "bad request"})
            except TimeoutError:
                return self.reply_json(503, {"error": "busy, try again"})
            return self.reply_json(404, {"error": "not found"})

    return Handler


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--node-name", required=True, help="name shown on the page, e.g. us-east-0")
    parser.add_argument("--service", default="zakurad", help="systemd unit of the node to profile")
    parser.add_argument("--data-dir", required=True, help="where profiles are stored")
    parser.add_argument("--scratch-dir", required=True, help="where perf writes each window; use a tmpfs")
    parser.add_argument("--log-file", default="", help="node log, read for block announcement times")
    parser.add_argument(
        "--trace-dir", default="",
        help="the node's [network.zakura] trace_dir, read for per-block stage timings",
    )
    parser.add_argument(
        "--trace-table-cap-mb", type=int, default=DEFAULT_TRACE_TABLE_CAP_MB,
        help="rename any trace table in --trace-dir that grows past this size to <table>.jsonl.1; 0 disables rotation",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8097)
    parser.add_argument("--window", type=int, default=WINDOW_SECONDS, help="seconds per sampling window")
    parser.add_argument("--perf", default=shutil.which("perf") or "perf", help="path to the perf binary")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    store = Store(args.data_dir)
    profiler = Profiler(args, store)
    queries = Queries(store, profiler)
    page_path = Path(__file__).resolve().with_name("page.html")
    server = ThreadingHTTPServer((args.host, args.port), make_handler(queries, profiler, page_path))
    server.daemon_threads = True

    def shut_down(*_):
        profiler.stop()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shut_down)
    signal.signal(signal.SIGINT, shut_down)
    workers = [
        threading.Thread(target=profiler.record_loop, name="record", daemon=True),
        threading.Thread(target=profiler.process_loop, name="process", daemon=True),
        threading.Thread(target=profiler.trace_loop, name="traces", daemon=True),
    ]
    for worker in workers:
        worker.start()
    server.serve_forever()
    for worker in workers:
        worker.join(timeout=10)
    return 0


if __name__ == "__main__":
    sys.exit(main())
