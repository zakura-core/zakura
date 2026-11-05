#!/usr/bin/env python3
"""Follow Zakura raw trace tables and query them with SQL.

The node side (`[tracing].raw_endpoint_addr`, docs/raw-trace-endpoint.md) only
streams byte ranges of its JSONL trace files. This is the collecting side:

- `pull` follows tables on one or more nodes and mirrors their rows into SQLite.
- `sql` runs an arbitrary read-only query over that mirror.
- `dump` prints a fresh prefix of one table as JSONL, for `jq` and other pipes.

Following a growing file. A resumed range is capped at the length its
`X-Trace-File-Id` captured, so each poll first probes `bytes=0-0` for a fresh
token. The token is `device:inode:length`. When the device and inode match the
stored cursor, the poll resumes at the cursor with the fresh token, so it reads
only appended bytes. A different device or inode, a file shorter than the cursor,
or a 409 means the file was replaced; the cursor restarts at byte 0 of the new
file. The cursor only advances past complete lines, so a line the writer has not
finished is fetched again by the next poll.

Rows keep the node's process-relative `ts` (microseconds since that process
started). The mirror adds `fetched_unix`, the collector's wall clock when it
stored the row.

Stdlib only, so it runs on any host with Python 3.9+.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Iterable, Iterator, Optional

LOG = logging.getLogger("trace_query")

# The node's fixed export allowlist (raw_endpoint.rs `TABLES`).
TABLES = (
    "handshake",
    "conn",
    "stream",
    "discovery",
    "ratelimit",
    "header_sync",
    "legacy_request",
    "block_sync",
    "commit_state",
    "quic_conn",
    "queue_send",
    "legacy_peer_request",
    "legacy_sync",
)

# The node rejects ranges above 1 MiB.
MAX_RANGE_BYTES = 1024 * 1024
# The node closes every connection after 30 s.
REQUEST_TIMEOUT_SECS = 35.0
# One poll reads at most this much, so a first catch-up of a large file
# commits in bounded steps.
DEFAULT_POLL_BUDGET_BYTES = 64 * MAX_RANGE_BYTES


class EndpointError(Exception):
    """The endpoint answered with a status this client does not handle."""


class FileReplaced(Exception):
    """The file behind a token was replaced or truncated (HTTP 409)."""


@dataclass
class Cursor:
    """How far one table has been read: the file identity and a byte offset.

    `offset` is always the start of a line, so it never splits a JSONL record.
    """

    file_id: Optional[str] = None
    offset: int = 0


@dataclass
class Poll:
    """One poll's result: complete rows with their byte offsets and the new cursor."""

    cursor: Cursor
    rows: list = field(default_factory=list)
    reset: bool = False
    malformed: int = 0
    # True when the poll stopped at its byte budget before the captured end.
    more: bool = False


@dataclass(frozen=True)
class Capture:
    """A fixed prefix the node captured: its `device:inode:length` token, split.

    `file_id` is `device:inode`. An empty file has no token and no identity.
    """

    token: Optional[str]
    file_id: Optional[str]
    length: int

    @classmethod
    def parse(cls, token: str) -> "Capture":
        try:
            device, inode, length = (int(part) for part in token.split(":"))
        except ValueError:
            raise EndpointError(f"malformed X-Trace-File-Id {token!r}") from None
        return cls(token=token, file_id=f"{device}:{inode}", length=length)


class Endpoint:
    """One node's raw trace endpoint."""

    def __init__(self, base_url: str, host_header: Optional[str] = None,
                 timeout: float = REQUEST_TIMEOUT_SECS, range_bytes: int = MAX_RANGE_BYTES):
        self.base_url = base_url.rstrip("/")
        # A tunnel or proxy changes the URL authority. The node only accepts its
        # literal bound address as Host, so callers pass that through here.
        self.host_header = host_header
        self.timeout = timeout
        self.range_bytes = range_bytes

    def _request(self, table: str, headers: dict) -> tuple[int, dict, bytes]:
        if table not in TABLES:
            raise ValueError(f"unknown trace table {table!r}")
        request = urllib.request.Request(f"{self.base_url}/traces/{table}", headers=headers)
        if self.host_header:
            request.add_header("Host", self.host_header)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
                expected = response.headers.get("Content-Length")
                if expected is not None and len(body) != int(expected):
                    raise EndpointError(f"{table}: short body {len(body)} of {expected} bytes")
                return response.status, dict(response.headers), body
        except urllib.error.HTTPError as error:
            return error.code, dict(error.headers or {}), b""

    def probe(self, table: str) -> Optional["Capture"]:
        """Capture a fresh prefix. Returns None when the table file is missing."""
        status, headers, _ = self._request(table, {"Range": "bytes=0-0"})
        if status == 404:
            return None
        if status == 416:
            # Only an empty file rejects byte 0, and the 416 carries no identity.
            return Capture(token=None, file_id=None, length=0)
        if status != 206:
            raise EndpointError(f"{table}: probe returned HTTP {status}")
        token = headers.get("X-Trace-File-Id") or headers.get("x-trace-file-id")
        if not token:
            raise EndpointError(f"{table}: probe response has no X-Trace-File-Id")
        return Capture.parse(token)

    def read_range(self, table: str, token: str, start: int, end: int) -> bytes:
        """Read bytes `start..end` (exclusive) of the prefix that `token` captured."""
        status, _, body = self._request(
            table, {"Range": f"bytes={start}-{end - 1}", "X-Trace-File-Id": token})
        if status == 409:
            raise FileReplaced(table)
        if status != 206:
            raise EndpointError(f"{table}: range {start}-{end - 1} returned HTTP {status}")
        return body

    def dump(self, table: str) -> bytes:
        """Return a fresh, complete prefix of one table."""
        capture = self.probe(table)
        if capture is None:
            raise EndpointError(f"{table}: no such trace file")
        token, length = capture.token, capture.length
        chunks = []
        offset = 0
        while offset < length:
            end = min(offset + self.range_bytes, length)
            chunk = self.read_range(table, token, offset, end)
            if not chunk:
                raise EndpointError(f"{table}: empty range at {offset}")
            chunks.append(chunk)
            offset += len(chunk)
        return b"".join(chunks)

    def poll(self, table: str, cursor: Cursor,
             budget_bytes: int = DEFAULT_POLL_BUDGET_BYTES) -> Poll:
        """Read the rows appended after `cursor`, up to `budget_bytes`."""
        capture = self.probe(table)
        if capture is None:
            return Poll(cursor=cursor)
        if capture.length == 0:
            # The writer only appends, so an empty file under a cursor is a new file.
            return Poll(cursor=Cursor(), reset=cursor.offset > 0)
        token, file_id, length = capture.token, capture.file_id, capture.length
        reset = False
        if cursor.file_id != file_id or cursor.offset > length:
            reset = cursor.file_id is not None
            cursor = Cursor(file_id, 0)

        result = Poll(cursor=Cursor(file_id, cursor.offset), reset=reset)
        # `pending` starts at `line_start`, the first byte not yet returned as a row.
        line_start = cursor.offset
        read_to = cursor.offset
        pending = bytearray()
        stop = min(length, cursor.offset + budget_bytes)
        try:
            # Past the budget, keep reading until one line completes, so a line
            # longer than the budget still makes progress.
            while read_to < stop or (line_start == cursor.offset and read_to < length):
                end = min(read_to + self.range_bytes, length)
                chunk = self.read_range(table, token, read_to, end)
                if not chunk:
                    raise EndpointError(f"{table}: empty range at {read_to}")
                read_to += len(chunk)
                pending += chunk
                line_start = self._take_lines(pending, line_start, result)
                del pending[: line_start - (read_to - len(pending))]
        except FileReplaced:
            # Keep the complete lines already parsed; the next poll restarts.
            LOG.warning("%s: file replaced during a poll; restarting next poll", table)
            result.cursor = Cursor(None, 0)
            return result
        result.cursor.offset = line_start
        result.more = read_to < length
        return result

    @staticmethod
    def _take_lines(pending: bytearray, line_start: int, result: Poll) -> int:
        """Move complete lines from `pending` into `result.rows`; return the new line start."""
        position = 0
        while True:
            newline = pending.find(b"\n", position)
            if newline < 0:
                return line_start + position
            line = bytes(pending[position:newline])
            offset = line_start + position
            position = newline + 1
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                result.malformed += 1
                continue
            if isinstance(row, dict):
                result.rows.append((offset, row))
            else:
                result.malformed += 1


SCHEMA = """
CREATE TABLE IF NOT EXISTS trace_cursors (
    node TEXT NOT NULL,
    tbl TEXT NOT NULL,
    file_id TEXT,
    offset INTEGER NOT NULL,
    updated_unix REAL NOT NULL,
    PRIMARY KEY (node, tbl)
);
CREATE TABLE IF NOT EXISTS trace_rows (
    node TEXT NOT NULL,
    tbl TEXT NOT NULL,
    file_id TEXT NOT NULL,
    offset INTEGER NOT NULL,
    event TEXT,
    ts INTEGER,
    process_trace_id TEXT,
    fetched_unix REAL NOT NULL,
    row TEXT NOT NULL,
    PRIMARY KEY (node, tbl, file_id, offset)
);
CREATE INDEX IF NOT EXISTS trace_rows_event ON trace_rows (tbl, event);
CREATE INDEX IF NOT EXISTS trace_rows_fetched ON trace_rows (fetched_unix);
"""


class Mirror:
    """A SQLite copy of trace rows from any number of nodes.

    `trace_rows` holds every row with its raw JSON in `row`; each trace table also
    gets a view of the same name, so `SELECT json_extract(row, '$.hash') FROM
    commit_state` works. A row and its table's cursor commit in one transaction,
    so a crash never skips or repeats rows.
    """

    def __init__(self, path: str):
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)
        for table in TABLES:
            self.db.execute(
                f'CREATE VIEW IF NOT EXISTS "{table}" AS SELECT node, file_id, offset, event, ts, '
                f"process_trace_id, fetched_unix, row FROM trace_rows WHERE tbl = '{table}'")
        self.db.commit()

    def cursor(self, node: str, table: str) -> Cursor:
        found = self.db.execute(
            "SELECT file_id, offset FROM trace_cursors WHERE node = ? AND tbl = ?",
            (node, table)).fetchone()
        return Cursor(*found) if found else Cursor()

    def store(self, node: str, table: str, poll: Poll,
              events: Optional[set] = None, now: Optional[float] = None) -> int:
        """Store a poll's rows (optionally only `events`) and its cursor; return rows stored."""
        now = time.time() if now is None else now
        stored = 0
        with self.db:
            for offset, row in poll.rows:
                event = row.get("event")
                if events and event not in events:
                    continue
                self.db.execute(
                    "INSERT OR IGNORE INTO trace_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (node, table, poll.cursor.file_id or "", offset, event, row.get("ts"),
                     row.get("process_trace_id"), now, json.dumps(row, separators=(",", ":"))))
                stored += 1
            self.db.execute(
                "INSERT INTO trace_cursors VALUES (?, ?, ?, ?, ?) ON CONFLICT (node, tbl) "
                "DO UPDATE SET file_id = excluded.file_id, offset = excluded.offset, "
                "updated_unix = excluded.updated_unix",
                (node, table, poll.cursor.file_id, poll.cursor.offset, now))
        return stored

    def prune(self, older_than_unix: float) -> int:
        """Delete rows fetched before `older_than_unix`; return how many."""
        with self.db:
            return self.db.execute(
                "DELETE FROM trace_rows WHERE fetched_unix < ?", (older_than_unix,)).rowcount


def follow(endpoints: dict, tables: Iterable[str], mirror: Mirror,
           events: Optional[set] = None, budget_bytes: int = DEFAULT_POLL_BUDGET_BYTES) -> int:
    """Poll every (node, table) once into `mirror`; return rows stored.

    A table that stops at its byte budget is polled again until it catches up.
    One failing node or table is logged and skipped.
    """
    stored = 0
    for node, endpoint in endpoints.items():
        for table in tables:
            while True:
                try:
                    poll = endpoint.poll(table, mirror.cursor(node, table), budget_bytes)
                except (EndpointError, OSError) as error:
                    LOG.warning("%s/%s: %s", node, table, error)
                    break
                if poll.reset:
                    LOG.info("%s/%s: trace file replaced; reading the new file", node, table)
                if poll.malformed:
                    LOG.warning("%s/%s: skipped %d malformed lines", node, table, poll.malformed)
                stored += mirror.store(node, table, poll, events)
                if not poll.more:
                    break
    return stored


def run_query(path: str, query: str, params: Iterable = ()) -> tuple[list, list]:
    """Run one read-only query; return the column names and rows."""
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        cursor = db.execute(query, tuple(params))
        columns = [column[0] for column in cursor.description or ()]
        return columns, cursor.fetchall()
    finally:
        db.close()


def parse_pairs(values: Iterable[str], flag: str) -> dict:
    pairs = {}
    for value in values:
        name, sep, rest = value.partition("=")
        if not sep or not name or not rest:
            raise SystemExit(f"{flag} expects NAME=VALUE, got {value!r}")
        pairs[name] = rest
    return pairs


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("-v", "--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)

    pull = commands.add_parser("pull", help="mirror trace rows from nodes into SQLite")
    pull.add_argument("--db", required=True, help="SQLite mirror path (created if missing)")
    pull.add_argument("--node", action="append", required=True, metavar="NAME=URL",
                      help="a node's endpoint, e.g. local=http://127.0.0.1:9009 (repeatable)")
    pull.add_argument("--host-header", action="append", default=[], metavar="NAME=HOST",
                      help="Host header for a tunnelled node: its literal bound address")
    pull.add_argument("--table", action="append", required=True, choices=TABLES)
    pull.add_argument("--event", action="append", default=[],
                      help="store only rows with this event (repeatable; default all)")
    pull.add_argument("--follow", type=float, metavar="SECS",
                      help="keep polling every SECS instead of once")
    pull.add_argument("--retain-hours", type=float,
                      help="delete rows fetched more than this many hours ago")

    sql = commands.add_parser("sql", help="run a read-only SQL query over a mirror")
    sql.add_argument("--db", required=True)
    sql.add_argument("--format", choices=("tsv", "json"), default="tsv")
    sql.add_argument("query")

    dump = commands.add_parser("dump", help="print a fresh prefix of one table as JSONL")
    dump.add_argument("--url", required=True, help="endpoint URL, e.g. http://127.0.0.1:9009")
    dump.add_argument("--host-header")
    dump.add_argument("--table", required=True, choices=TABLES)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "dump":
        sys.stdout.buffer.write(Endpoint(args.url, args.host_header).dump(args.table))
        return 0

    if args.command == "sql":
        columns, rows = run_query(args.db, args.query)
        if args.format == "json":
            for row in rows:
                print(json.dumps(dict(zip(columns, row))))
        else:
            print("\t".join(columns))
            for row in rows:
                print("\t".join("" if value is None else str(value) for value in row))
        return 0

    nodes = parse_pairs(args.node, "--node")
    hosts = parse_pairs(args.host_header, "--host-header")
    endpoints = {name: Endpoint(url, hosts.get(name)) for name, url in nodes.items()}
    mirror = Mirror(args.db)
    events = set(args.event) or None
    while True:
        stored = follow(endpoints, args.table, mirror, events)
        if args.retain_hours is not None:
            mirror.prune(time.time() - args.retain_hours * 3600)
        LOG.debug("stored %d rows", stored)
        if args.follow is None:
            return 0
        time.sleep(args.follow)


if __name__ == "__main__":
    sys.exit(main())
