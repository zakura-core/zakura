"""When blocks landed on the node, read from the files the node already writes.

`FileFollower` reads only the bytes appended to a file since the last call, so following
a multi-gigabyte node log costs the size of what was written, not the size of the file.
`announcements` picks the block announcement lines out of the node log; each one marks
the moment a newly committed block was handed to gossip. `TraceFollower` reads the
node's trace tables, when it writes them, into how long each block spent in each stage
from its header arriving to its peers being told.
"""

import calendar
import collections
import json
import os
import re

ANNOUNCEMENT_MARKER = b"sending block announcement"
ANNOUNCEMENT = re.compile(
    rb"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z\s.*"
    rb"sending block announcement.*?height=Height\((\d+)\).*?hash=block::Hash\(\"([0-9a-f]{64})\"\)"
)
# On the first read, look this far back so a restart of the profiler still knows the
# blocks of the last few minutes.
INITIAL_LOOKBACK_BYTES = 8 * 1024 * 1024
MAX_READ_BYTES = 256 * 1024 * 1024

# A block announcement from the node log: unix time, height and hash.
Block = collections.namedtuple("Block", "time height hash")


class FileFollower:
    """Reads the complete lines appended to a file since the previous read.

    Given `moved_path`, it also follows the file through a rotation by rename: when
    the file it was reading has been renamed to `moved_path`, it reads the rest of
    that file before it starts on the new file at `path`.
    """

    def __init__(self, path, lookback_bytes=INITIAL_LOOKBACK_BYTES, moved_path=None):
        self.path = path
        self.moved_path = moved_path
        self._lookback_bytes = lookback_bytes
        self._offset = None
        self._inode = None
        self._partial = b""

    def read_new_lines(self):
        """The lines completed since the last call, without their newlines.

        A file that was replaced or truncated is read again from its start. With
        nothing at `path`, the file at `moved_path` is followed if it is the one being
        followed, or if nothing has been followed yet; otherwise nothing is returned
        and `path` is retried on the next call.
        """
        try:
            with open(self.path, "rb") as handle:
                return self._read_current(handle)
        except FileNotFoundError:
            return self._read_moved()
        except OSError:
            return []

    def _read_current(self, handle):
        """The new lines of the file open at `path`, after any left in the file it replaced."""
        stat = os.fstat(handle.fileno())
        if self._offset is None:
            return self._start_following(handle, stat)
        earlier = []
        if stat.st_ino != self._inode:
            # A new file has the name. Its writer stopped writing the old file before
            # creating this one, so what the old file holds now is all it will hold.
            earlier = self._read_moved()
            self._offset, self._partial = 0, b""
        elif stat.st_size < self._offset:
            self._offset, self._partial = 0, b""
        self._inode = stat.st_ino
        return earlier + self._read_appended(handle, stat.st_size)

    def _read_moved(self):
        """The new lines of the file at `moved_path`, if that is the file to follow."""
        if self.moved_path is None:
            return []
        try:
            with open(self.moved_path, "rb") as handle:
                stat = os.fstat(handle.fileno())
                if self._offset is None:
                    # Nothing is at `path` and nothing has been followed yet, so this
                    # is the file the writer still has open.
                    return self._start_following(handle, stat)
                if stat.st_ino != self._inode:
                    return []
                if stat.st_size < self._offset:
                    self._offset, self._partial = 0, b""
                return self._read_appended(handle, stat.st_size)
        except OSError:
            return []

    def _start_following(self, handle, stat):
        """Follow the open file from `lookback_bytes` before its end; return the lines there."""
        self._inode = stat.st_ino
        self._offset = max(0, stat.st_size - self._lookback_bytes)
        starts_mid_file = self._offset > 0
        lines = self._read_appended(handle, stat.st_size)
        if starts_mid_file and lines:
            # The first line read began before the starting point: drop it.
            lines.pop(0)
        return lines

    def _read_appended(self, handle, size):
        handle.seek(self._offset)
        data = handle.read(min(size - self._offset, MAX_READ_BYTES))
        self._offset += len(data)
        lines = (self._partial + data).split(b"\n")
        self._partial = lines.pop()
        return lines


def parse_announcement(line):
    """The `Block` a block announcement log line reports, else `None`."""
    if ANNOUNCEMENT_MARKER not in line:
        return None
    match = ANNOUNCEMENT.match(line)
    if match is None:
        return None
    year, month, day, hour, minute, second = (int(match.group(i)) for i in range(1, 7))
    fraction = match.group(7) or b"0"
    when = calendar.timegm((year, month, day, hour, minute, second)) + int(fraction) / (
        10 ** len(fraction)
    )
    return Block(when, int(match.group(8)), match.group(9).decode())


def announcements(lines):
    """The block announcements among `lines`, oldest first."""
    return [block for line in lines if (block := parse_announcement(line)) is not None]


# ---- per-block stage timings, from the node's trace tables ----
#
# With `[network.zakura] trace_dir` set, the node appends one JSON row per event to a
# file per table. Rows carry `ts`, microseconds since the node process started, so the
# time between two rows of the same process is exact. The rows below are the ones that
# mark a block's way through the node; each names its block by hash, by height, or both.

# A trace table is `<table>.jsonl`; the one rotated out before it is `<table>.jsonl.1`.
TABLE_SUFFIX = ".jsonl"
MOVED_SUFFIX = ".1"
TRACE_TABLES = {
    "header_sync": ("header_request_sent", "header_response_received", "header_target_admitted"),
    "block_sync": ("block_get_blocks_sent", "block_body_received", "block_body_submitted", "block_status_sent"),
    "legacy_peer_request": ("block_request_finish",),
    "commit_state": ("commit_start", "commit_finish"),
    "chain_tip": ("tip_published",),
}
# A block's record is closed this long (in the node's clock) after its tip was
# published, which leaves time for the status messages to its peers to be recorded.
SETTLE_MICROSECONDS = 3_000_000
# Status messages queued within this long of the tip are the block's fan-out. Later
# ones go to peers that connected afterwards, or were held back behind other traffic.
FANOUT_MICROSECONDS = 1_000_000
MAX_STATUS_TIMES = 512
# How a block reached the node: Zakura's own block sync, the legacy Zcash network, or
# neither on record (mined here, submitted over RPC, or downloaded before a restart).
PATH_ZAKURA, PATH_LEGACY, PATH_UNKNOWN = "zakura", "legacy", "unknown"
# Blocks this far below the newest tip are dropped unfinished: their tip never arrived.
PENDING_DEPTH = 20
HASH_IN_TEXT = re.compile(r"[0-9a-f]{64}")

# The stages shown on the page, in order: id, label, and where in the code it happens.
STAGES = (
    ("header_fetch", "Fetch the header", "request to a peer that announced a new tip, until its headers arrive"),
    ("header_check", "Check the header", "header chain validation, until the new tip is accepted as a target"),
    ("body_request", "Ask for the block", "block sync picks a peer and sends the request"),
    ("body_download", "Download the block", "request sent to a peer until the block's bytes have arrived"),
    ("submit", "Hand to the verifier", "decode, reorder buffer and the queue in front of the verifier"),
    ("verify", "Verify", "consensus checks, from the block being in hand until it joins the state writer's queue"),
    ("writer_wait", "Wait for the writer", "time queued for the single block-writer thread"),
    ("writer_commit", "Commit to state", "the writer's own work until the new tip is published"),
    ("status_fanout", "Tell peers", "new tip published until the typical Zakura peer's status message is queued"),
)


# The trace events that start and end each stage, by how the block arrived. A block
# from the legacy network has no header or hand-off stages: its trace starts when its
# download finishes, and that row says how long the download took. `writer_queued` is
# when the block joined the state writer's queue, worked back from the tip row.
STAGE_BOUNDARIES = {
    PATH_ZAKURA: (
        ("header_fetch", "header_request_sent", "header_response_received"),
        ("header_check", "header_response_received", "header_target_admitted"),
        ("body_request", "header_target_admitted", "block_get_blocks_sent"),
        ("body_download", "block_get_blocks_sent", "block_body_received"),
        ("submit", "block_body_received", "commit_start"),
        ("verify", "commit_start", "writer_queued"),
    ),
    PATH_LEGACY: (("verify", "block_request_finish", "writer_queued"),),
}


def block_path(events):
    """Which network the block's download was recorded on."""
    if "commit_start" in events:
        return PATH_ZAKURA
    if "block_request_finish" in events:
        return PATH_LEGACY
    return PATH_UNKNOWN


def stage_durations(events, queue_us, commit_us, typical_status):
    """Milliseconds per stage for one block, `None` where a boundary was not recorded.

    `events` maps an event name to the node-clock time it first happened.
    """

    def between(start, end):
        if start is None or end is None or end < start:
            return None
        return round((end - start) / 1000, 3)

    tip = events.get("tip_published")
    at = dict(events)
    if tip is not None:
        at["writer_queued"] = tip - queue_us - commit_us
    path = block_path(events)
    stages = dict.fromkeys(stage for stage, _, _ in STAGES)
    for stage, start, end in STAGE_BOUNDARIES.get(path, ()):
        stages[stage] = between(at.get(start), at.get(end))
    if path == PATH_LEGACY:
        stages["body_download"] = events.get("download_ms")
    if tip is not None:
        stages["writer_wait"] = round(queue_us / 1000, 3)
        stages["writer_commit"] = round(commit_us / 1000, 3)
    stages["status_fanout"] = between(tip, typical_status)
    return stages


class BlockStages:
    """Joins trace rows into one record per block that became the node's tip.

    Events are kept per node run, under `(process_trace_id, hash)` and
    `(process_trace_id, height)`: a restarted node counts time from zero again, so rows
    from different runs are never compared.
    """

    def __init__(self):
        self._by_hash = {}
        self._by_height = {}
        self._open = []
        self._latest = 0
        self._process = None

    def feed(self, row):
        """Take one parsed trace row. Rows may arrive in any order across tables."""
        event, when = row.get("event"), row.get("ts")
        if not isinstance(event, str) or type(when) is not int:
            return
        if row.get("process_trace_id") != self._process:
            # A restarted node counts time from zero again.
            self._process, self._latest = row.get("process_trace_id"), when
        self._latest = max(self._latest, when)
        if event == "block_request_finish" and row.get("result") != "available":
            return
        height = row.get("tip_height", row.get("height", row.get("returned_height")))
        block_hash = (
            row.get("tip_hash") or row.get("hash") or row.get("branch_target") or row.get("returned_hash")
        )
        if block_hash is None and event == "block_get_blocks_sent":
            # The request names the tip it is fetching toward at the end of its owner.
            found = HASH_IN_TEXT.findall(str(row.get("request_owner", "")))
            block_hash = found[-1] if found else None
        process = row.get("process_trace_id")
        for key, table in ((block_hash, self._by_hash), (height, self._by_height)):
            if key is None:
                continue
            seen = table.setdefault((process, key), {})
            if event == "block_status_sent":
                times = seen.setdefault("status_times", [])
                if len(times) < MAX_STATUS_TIMES:
                    times.append(when)
                continue
            if event in seen:
                continue
            seen[event] = when
            if event == "block_body_received" and type(row.get("serialized_bytes")) is int:
                seen["size"] = row["serialized_bytes"]
            if event == "block_request_finish" and isinstance(row.get("elapsed_ms"), (int, float)):
                seen["download_ms"] = row["elapsed_ms"]
        if event == "tip_published" and isinstance(block_hash, str) and type(height) is int:
            self._open.append(
                {
                    "height": height,
                    "hash": block_hash,
                    "tip": when,
                    "process": process,
                    "queue_us": row.get("queue_us") or 0,
                    "commit_us": row.get("commit_us") or 0,
                    "reorg": row.get("kind") == "reorg",
                }
            )

    def finished(self):
        """The records of blocks whose tip was published long enough ago to be complete."""
        done, still_open = [], []
        for block in self._open:
            same_run = block["process"] == self._process
            if same_run and self._latest - block["tip"] < SETTLE_MICROSECONDS:
                still_open.append(block)
                continue
            events = {
                **self._by_height.get((block["process"], block["height"]), {}),
                **self._by_hash.get((block["process"], block["hash"]), {}),
            }
            # The status messages of this block's fan-out, soonest first.
            told = sorted(
                when
                for when in events.get("status_times", ())
                if 0 <= when - block["tip"] <= FANOUT_MICROSECONDS
            )
            done.append(
                {
                    "height": block["height"],
                    "hash": block["hash"],
                    "process": block["process"],
                    "tip_us": block["tip"],
                    "reorg": block["reorg"],
                    "path": block_path(events),
                    "size": events.get("size"),
                    "status_peers": len(told),
                    "status_slowest_ms": round((told[-1] - block["tip"]) / 1000, 3) if told else None,
                    "ms": stage_durations(
                        events, block["queue_us"], block["commit_us"], told[len(told) // 2] if told else None
                    ),
                }
            )
        self._open = still_open
        if done:
            floor = max(block["height"] for block in done) - PENDING_DEPTH
            self._by_height = {key: seen for key, seen in self._by_height.items() if key[1] > floor}
            keep = {(block["process"], block["hash"]) for block in self._open}
            newest = set(list(self._by_hash)[-4 * PENDING_DEPTH :])
            self._by_hash = {key: seen for key, seen in self._by_hash.items() if key in keep or key in newest}
        return done


class TraceFollower:
    """Follows the node's trace tables and returns finished per-block records.

    The node never rotates its trace files, so this also renames any table that has
    grown past `table_cap_bytes` to `<table>.jsonl.1`, replacing the one before it.
    Nothing is lost while the node runs: it keeps appending to the renamed file until
    its next sync, about a second later, then starts a new file under the old name,
    and the rows left in the renamed file are read before the new file is.
    """

    def __init__(self, trace_dir, table_cap_bytes):
        self.trim_failure = None
        self.trace_dir = trace_dir
        self.table_cap_bytes = table_cap_bytes
        self._followers = {}
        for table in TRACE_TABLES:
            path = os.path.join(trace_dir, table + TABLE_SUFFIX)
            self._followers[table] = FileFollower(
                path, lookback_bytes=2 * 1024 * 1024, moved_path=path + MOVED_SUFFIX
            )
        self._markers = {
            table: tuple(('"event":"%s"' % event).encode() for event in events)
            for table, events in TRACE_TABLES.items()
        }
        self._stages = BlockStages()

    def poll(self):
        """Read what the node appended since the last call; return finished blocks."""
        for table, follower in self._followers.items():
            markers = self._markers[table]
            for line in follower.read_new_lines():
                # Most rows are peer chatter: skip them before paying for a JSON parse.
                if not any(marker in line for marker in markers):
                    continue
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if isinstance(row, dict):
                    self._stages.feed(row)
        self._rotate_full_tables()
        return self._stages.finished()

    def _rotate_full_tables(self):
        """Rename full tables and record the first failure in `trim_failure`."""
        self.trim_failure = None
        if not self.table_cap_bytes:
            return
        try:
            names = os.listdir(self.trace_dir)
        except FileNotFoundError:
            # The node has not written a trace file yet: there is nothing to empty.
            return
        except OSError as error:
            self.trim_failure = error
            return
        moved_suffix = TABLE_SUFFIX + MOVED_SUFFIX
        # Renamed files first, so one renamed in this pass is not also checked as one.
        for name in sorted(names, key=lambda name: not name.endswith(moved_suffix)):
            path = os.path.join(self.trace_dir, name)
            try:
                size = os.path.getsize(path)
                if name.endswith(moved_suffix):
                    # A node build that does not start a new file keeps growing the
                    # renamed one. Emptying it in place bounds the disk it uses, at the
                    # cost of the rows not read yet.
                    if size > 2 * self.table_cap_bytes:
                        os.truncate(path, 0)
                elif name.endswith(TABLE_SUFFIX) and size > self.table_cap_bytes:
                    os.replace(path, path + MOVED_SUFFIX)
            except FileNotFoundError:
                continue
            except OSError as error:
                # Keep going: one table that cannot be emptied must not stop the others.
                self.trim_failure = self.trim_failure or error
