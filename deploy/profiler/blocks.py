"""When blocks landed on the node, read from the files the node already writes.

`FileFollower` reads only the bytes appended to a file since the last call, so following
a multi-gigabyte node log costs the size of what was written, not the size of the file.
`announcements` picks the block announcement lines out of the node log; each one marks
the moment a newly committed block was handed to gossip. `TraceFollower` reads the
node's trace tables, when it writes them, into how long each block spent in each stage
from its header arriving to its peers being told.
"""

import calendar
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


class FileFollower:
    """Reads the complete lines appended to a file since the previous read."""

    def __init__(self, path, lookback_bytes=INITIAL_LOOKBACK_BYTES):
        self.path = path
        self._lookback_bytes = lookback_bytes
        self._offset = None
        self._inode = None
        self._partial = b""

    def read_new_lines(self):
        """The lines completed since the last call, without their newlines.

        A file that was replaced or truncated is read again from its start. A missing
        file yields nothing and is retried on the next call.
        """
        try:
            with open(self.path, "rb") as handle:
                stat = os.fstat(handle.fileno())
                if self._offset is None:
                    self._offset = max(0, stat.st_size - self._lookback_bytes)
                    # Start mid-file on a line boundary: drop the first, partial line.
                    skip_first = self._offset > 0
                elif stat.st_ino != self._inode or stat.st_size < self._offset:
                    self._offset, self._partial, skip_first = 0, b"", False
                else:
                    skip_first = False
                self._inode = stat.st_ino
                handle.seek(self._offset)
                data = handle.read(min(stat.st_size - self._offset, MAX_READ_BYTES))
        except OSError:
            return []
        self._offset += len(data)
        lines = (self._partial + data).split(b"\n")
        self._partial = lines.pop()
        if skip_first and lines:
            lines.pop(0)
        return lines


def parse_announcement(line):
    """`(unix_time, height, hash)` for a block announcement log line, else `None`."""
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
    return when, int(match.group(8)), match.group(9).decode()


def announcements(lines):
    """The block announcements among `lines`, oldest first."""
    found = []
    for line in lines:
        parsed = parse_announcement(line)
        if parsed is not None:
            found.append(parsed)
    return found


# ---- per-block stage timings, from the node's trace tables ----
#
# With `[network.zakura] trace_dir` set, the node appends one JSON row per event to a
# file per table. Rows carry `ts`, microseconds since the node process started, so the
# time between two rows of the same process is exact. The rows below are the ones that
# mark a block's way through the node; each names its block by hash, by height, or both.

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


def block_path(events):
    """Which network the block's download was recorded on."""
    if "commit_start" in events:
        return PATH_ZAKURA
    if "block_request_finish" in events:
        return PATH_LEGACY
    return PATH_UNKNOWN


def stage_durations(events, queue_us, commit_us, typical_status):
    """Milliseconds per stage for one block, `None` where a boundary was not recorded.

    `events` maps an event name to the node-clock time it first happened. A block from
    the legacy network has no header or hand-off stages: its trace starts when its
    download finishes, and that row says how long the download took.
    """

    def between(start, end):
        if start is None or end is None or end < start:
            return None
        return round((end - start) / 1000, 3)

    at = events.get
    tip = at("tip_published")
    queued = tip - queue_us - commit_us if tip is not None else None
    stages = dict.fromkeys(stage for stage, _, _ in STAGES)
    path = block_path(events)
    if path == PATH_ZAKURA:
        stages["header_fetch"] = between(at("header_request_sent"), at("header_response_received"))
        stages["header_check"] = between(at("header_response_received"), at("header_target_admitted"))
        stages["body_request"] = between(at("header_target_admitted"), at("block_get_blocks_sent"))
        stages["body_download"] = between(at("block_get_blocks_sent"), at("block_body_received"))
        stages["submit"] = between(at("block_body_received"), at("commit_start"))
        stages["verify"] = between(at("commit_start"), queued)
    elif path == PATH_LEGACY:
        stages["body_download"] = events.get("download_ms")
        stages["verify"] = between(at("block_request_finish"), queued)
    if tip is not None:
        stages["writer_wait"] = round(queue_us / 1000, 3)
        stages["writer_commit"] = round(commit_us / 1000, 3)
    stages["status_fanout"] = between(tip, typical_status)
    return stages


class BlockStages:
    """Joins trace rows into one record per block that became the node's tip."""

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
        for key, table in ((block_hash, self._by_hash), (height, self._by_height)):
            if key is None:
                continue
            seen = table.setdefault(key, {"process": row.get("process_trace_id")})
            if seen["process"] != row.get("process_trace_id"):
                # The node restarted: its clock began again, so earlier rows cannot be compared.
                seen.clear()
                seen["process"] = row.get("process_trace_id")
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
                    "process": row.get("process_trace_id"),
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
            events = {}
            for seen in (self._by_height.get(block["height"], {}), self._by_hash.get(block["hash"], {})):
                if seen.get("process") == block["process"]:
                    events.update(seen)
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
            self._by_height = {h: seen for h, seen in self._by_height.items() if h > floor}
            keep = {block["hash"] for block in self._open}
            newest = dict(list(self._by_hash.items())[-4 * PENDING_DEPTH :])
            self._by_hash = {h: seen for h, seen in self._by_hash.items() if h in keep or h in newest}
        return done


class TraceFollower:
    """Follows the node's trace tables and returns finished per-block records.

    The node never rotates its trace files, so this also empties any table that has
    grown past `table_cap_bytes`. That is safe while the node runs: it opens the files
    for append, so its next row lands at the start of the emptied file.
    """

    def __init__(self, trace_dir, table_cap_bytes):
        self.trace_dir = trace_dir
        self.table_cap_bytes = table_cap_bytes
        self._followers = {
            table: FileFollower(os.path.join(trace_dir, table + ".jsonl"), lookback_bytes=2 * 1024 * 1024)
            for table in TRACE_TABLES
        }
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
        self._cap_tables()
        return self._stages.finished()

    def _cap_tables(self):
        try:
            names = os.listdir(self.trace_dir)
        except OSError:
            return
        for name in names:
            path = os.path.join(self.trace_dir, name)
            try:
                if name.endswith(".jsonl") and os.path.getsize(path) > self.table_cap_bytes:
                    os.truncate(path, 0)
            except OSError:
                continue
