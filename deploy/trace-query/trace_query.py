#!/usr/bin/env python3
"""Follow Zakura raw trace tables and query them with SQL.

The node side (`[tracing].raw_endpoint_addr`, docs/raw-trace-endpoint.md) only
serves its JSONL trace files over HTTP with standard byte ranges. This is the collecting side:

- `pull` follows tables on one or more nodes and mirrors their rows into SQLite.
- `sql` runs an arbitrary read-only query over that mirror.
- `tables` lists a node's tables with their sizes and ETags.
- `dump` prints one table as JSONL, for `jq` and other pipes.

Following a growing file. Each poll sends one request, `Range: bytes=<cursor>-`
with `If-Range: <etag>`. The ETag names the file (device, inode, birth time), not
its contents, and the writer only appends. A 206 carries the appended bytes. A
416 whose length equals the cursor means nothing new; a shorter length means
the file was truncated, and the cursor restarts at 0. A 200 means the ETag no
longer matches: the file was replaced, and the body is the new file from byte 0.
The cursor only advances past complete lines, so a line the writer has not
finished is fetched again by the next poll.

Rows keep the node's process-relative `ts` (microseconds since that process
started). The mirror adds `fetched_unix`, the collector's wall clock when it
stored the row.

Stdlib only, so it runs on any host with Python 3.9+.
"""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Optional

LOG = logging.getLogger("trace_query")

# The node serves `<trace_dir>/<name>.jsonl` for names matching this.
TABLE_NAME = re.compile(r"[a-z0-9_]{1,64}")
# Names the mirror uses for itself, so no trace table gets a view with them.
RESERVED_NAMES = {"trace_rows", "trace_cursors"}
# The node drops a connection that stalls for 30 s.
REQUEST_TIMEOUT_SECS = 35.0
READ_CHUNK_BYTES = 256 * 1024
# One poll reads about this much, so a first catch-up of a large file
# commits in bounded steps.
DEFAULT_POLL_BUDGET_BYTES = 64 * 1024 * 1024
MAX_LINE_BYTES = 8 * 1024 * 1024
# A writer waits this long for another writer's lock before the pass skips the table.
BUSY_TIMEOUT_SECS = 30.0
CONTENT_RANGE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")
UNSATISFIED_RANGE = re.compile(r"bytes \*/(\d+)")


class EndpointError(Exception):
    """The endpoint answered with a status or headers this client does not accept."""


def check_table(table: str) -> str:
    """Return `table` if the node could serve it; raise ValueError otherwise."""
    if not isinstance(table, str) or not TABLE_NAME.fullmatch(table):
        raise ValueError(f"invalid trace table name {table!r}")
    return table


@dataclass
class Cursor:
    """How far one table has been read: the file's ETag and a byte offset.

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
    # True when the poll stopped at its byte budget before the end of the file.
    more: bool = False


class Endpoint:
    """One node's raw trace endpoint."""

    def __init__(self, base_url: str, host_header: Optional[str] = None,
                 timeout: float = REQUEST_TIMEOUT_SECS, chunk_bytes: int = READ_CHUNK_BYTES):
        self.base_url = base_url.rstrip("/")
        # A tunnel or proxy changes the URL authority. The node only accepts its
        # literal bound address as Host, so callers pass that through here.
        self.host_header = host_header
        self.timeout = timeout
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be positive")
        self.chunk_bytes = chunk_bytes

    def _open(self, path: str, headers: dict):
        """Send one GET; return (status, lower-cased headers, open response or None)."""
        request = urllib.request.Request(f"{self.base_url}{path}", headers=headers)
        if self.host_header:
            request.add_header("Host", self.host_header)
        try:
            response = urllib.request.urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as error:
            with error:
                return error.code, {k.lower(): v for k, v in (error.headers or {}).items()}, None
        except http.client.HTTPException as error:
            raise EndpointError(f"{path}: malformed HTTP response: {error}") from error
        return response.status, {k.lower(): v for k, v in response.headers.items()}, response

    def _chunks(self, table: str, response, length: int) -> Iterator[bytes]:
        """Yield the body in chunks; raise if it ends before `length` bytes."""
        remaining = length
        try:
            while remaining > 0:
                chunk = response.read(min(self.chunk_bytes, remaining))
                if not chunk:
                    raise EndpointError(f"{table}: body ended {remaining} bytes early")
                remaining -= len(chunk)
                yield chunk
        except http.client.HTTPException as error:
            raise EndpointError(f"{table}: interrupted HTTP response: {error}") from error

    def tables(self) -> list:
        """The node's index: one dict per table with `name`, `bytes` and `etag`."""
        status, _, response = self._open("/traces", {})
        if status != 200:
            raise EndpointError(f"index returned HTTP {status}")
        with response:
            try:
                index = json.loads(response.read(16 * 1024 * 1024))
            except ValueError as error:
                raise EndpointError(f"index is not JSON: {error}") from None
        tables = index.get("tables") if isinstance(index, dict) else None
        if not isinstance(tables, list) or not all(
                isinstance(entry, dict) and isinstance(entry.get("name"), str)
                and TABLE_NAME.fullmatch(entry["name"]) for entry in tables):
            raise EndpointError("index has an unexpected shape")
        return tables

    def dump(self, table: str) -> Iterator[bytes]:
        """Stream one table up to its length when the node opened it; the last line may be incomplete."""
        status, headers, response = self._open(f"/traces/{check_table(table)}", {})
        if status != 200:
            raise EndpointError(f"{table}: returned HTTP {status}")
        with response:
            yield from self._chunks(table, response, content_length(table, headers))

    def poll(self, table: str, cursor: Cursor,
             budget_bytes: int = DEFAULT_POLL_BUDGET_BYTES) -> Poll:
        """Read the rows appended after `cursor`, up to about `budget_bytes`."""
        if budget_bytes <= 0:
            raise ValueError("budget_bytes must be positive")
        headers = {"Range": f"bytes={cursor.offset}-"}
        if cursor.file_id:
            headers["If-Range"] = cursor.file_id
        status, response_headers, response = self._open(f"/traces/{check_table(table)}", headers)
        if status == 404:
            return Poll(cursor=cursor)
        if status == 416:
            found = UNSATISFIED_RANGE.fullmatch(response_headers.get("content-range", ""))
            if not found:
                raise EndpointError(f"{table}: 416 without a valid Content-Range")
            length = int(found.group(1))
            if length < cursor.offset:
                # The writer only appends, so a shorter file was truncated.
                return Poll(cursor=Cursor(), reset=True)
            if length > cursor.offset:
                raise EndpointError(f"{table}: 416 for an offset inside the file")
            return Poll(cursor=cursor)
        if status not in (200, 206):
            if response is not None:
                response.close()
            raise EndpointError(f"{table}: returned HTTP {status}")

        with response:
            etag = response_headers.get("etag")
            if not etag:
                raise EndpointError(f"{table}: HTTP {status} without an ETag")
            length = content_length(table, response_headers)
            if status == 206:
                found = CONTENT_RANGE.fullmatch(response_headers.get("content-range", ""))
                if (not found or int(found.group(1)) != cursor.offset
                        or int(found.group(2)) + 1 - cursor.offset != length
                        or (cursor.file_id and etag != cursor.file_id)):
                    raise EndpointError(f"{table}: 206 does not match the requested range")
                result = Poll(cursor=Cursor(etag, cursor.offset))
            elif status == 200:
                # If-Range failed, or there was no ETag yet: the body is the file from byte 0.
                result = Poll(cursor=Cursor(etag, 0), reset=cursor.file_id is not None)
            self._read_lines(table, response, length, budget_bytes, result)
        return result

    def _read_lines(self, table: str, response, length: int, budget_bytes: int, result: Poll):
        """Parse complete lines from the body into `result`, stopping after the budget.

        Past the budget it keeps reading until one line completes, so a line longer
        than the budget still makes progress.
        """
        start = result.cursor.offset
        line_start = start
        read = 0
        pending = bytearray()
        for chunk in self._chunks(table, response, length):
            read += len(chunk)
            pending += chunk
            consumed = self._take_lines(pending, line_start, result)
            del pending[: consumed - line_start]
            line_start = consumed
            if read >= budget_bytes and line_start > start:
                break
        result.cursor.offset = line_start
        result.more = read < length

    @staticmethod
    def _take_lines(pending: bytearray, line_start: int, result: Poll) -> int:
        """Move complete lines from `pending` into `result.rows`; return the new line start."""
        position = 0
        while True:
            newline = pending.find(b"\n", position)
            end = len(pending) if newline < 0 else newline
            if end - position > MAX_LINE_BYTES:
                raise EndpointError(f"line exceeds {MAX_LINE_BYTES} bytes at {line_start + position}")
            if newline < 0:
                return line_start + position
            line = bytes(pending[position:newline])
            offset = line_start + position
            position = newline + 1
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except (ValueError, RecursionError):
                result.malformed += 1
                continue
            if (isinstance(row, dict)
                    and (row.get("event") is None or isinstance(row["event"], str))
                    and (row.get("process_trace_id") is None or isinstance(row["process_trace_id"], str))
                    and (row.get("ts") is None or
                         (type(row["ts"]) is int and -(2**63) <= row["ts"] < 2**63))):
                result.rows.append((offset, row))
            else:
                result.malformed += 1


def content_length(table: str, headers: dict) -> int:
    value = headers.get("content-length", "")
    if not value.isdecimal():
        raise EndpointError(f"{table}: missing Content-Length")
    return int(value)


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

    The mirror uses WAL mode, so a long `sql` query never blocks `pull`.
    """

    def __init__(self, path: str):
        self.db = sqlite3.connect(path, timeout=BUSY_TIMEOUT_SECS)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        self.db.commit()

    def ensure_view(self, table: str):
        """Give `table` a view of the same name over `trace_rows`, unless the mirror uses that name."""
        if check_table(table) in RESERVED_NAMES:
            return
        # `check_table` limits the name to [a-z0-9_], so it is safe inside the SQL text.
        self.db.execute(
            f'CREATE VIEW IF NOT EXISTS "{table}" AS SELECT node, file_id, offset, event, ts, '
            f"process_trace_id, fetched_unix, row FROM trace_rows WHERE tbl = '{table}'")

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
            self.ensure_view(table)
            for offset, row in poll.rows:
                event = row.get("event")
                if events and event not in events:
                    continue
                inserted = self.db.execute(
                    "INSERT OR IGNORE INTO trace_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (node, table, poll.cursor.file_id or "", offset, event, row.get("ts"),
                     row.get("process_trace_id"), now, json.dumps(row, separators=(",", ":"))))
                stored += inserted.rowcount
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


def follow(endpoints: dict, tables: Optional[Iterable[str]], mirror: Mirror,
           events: Optional[set] = None, budget_bytes: int = DEFAULT_POLL_BUDGET_BYTES) -> int:
    """Poll every (node, table) once into `mirror`; return rows stored.

    `tables=None` follows every table in each node's index. Each table gets one
    bounded poll so a busy table cannot starve another. One failing node or table
    is logged and skipped. A table whose store finds the mirror locked keeps its
    cursor, so the next pass reads the same rows again.
    """
    stored = 0
    for node, endpoint in endpoints.items():
        try:
            names = list(tables) if tables is not None else [entry["name"] for entry in endpoint.tables()]
        except (EndpointError, OSError) as error:
            LOG.warning("%s: %s", node, error)
            continue
        for table in names:
            try:
                poll = endpoint.poll(table, mirror.cursor(node, table), budget_bytes)
            except (EndpointError, OSError) as error:
                LOG.warning("%s/%s: %s", node, table, error)
                continue
            if poll.reset:
                LOG.info("%s/%s: trace file replaced; reading the new file", node, table)
            if poll.malformed:
                LOG.warning("%s/%s: skipped %d malformed lines", node, table, poll.malformed)
            try:
                stored += mirror.store(node, table, poll, events)
            except sqlite3.OperationalError as error:
                if not is_locked(error):
                    raise
                LOG.warning("%s/%s: %s; retrying next pass", node, table, error)
    return stored


def is_locked(error: sqlite3.OperationalError) -> bool:
    """Whether another connection held the lock past the busy timeout."""
    # SQLITE_BUSY and SQLITE_LOCKED read "database is locked" and "database table is locked".
    return "is locked" in str(error)


def run_query(path: str, query: str, params: Iterable = ()) -> tuple[list, list]:
    """Run one read-only query; return the column names and rows."""
    db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        db.execute("PRAGMA query_only = ON")
        allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION,
                   sqlite3.SQLITE_RECURSIVE}
        db.set_authorizer(lambda action, *_: sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY)
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
    pull.add_argument("--table", action="append", type=check_table,
                      help="a table to follow (repeatable; default every table in each node's index)")
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

    tables = commands.add_parser("tables", help="list a node's tables with their sizes and ETags")
    tables.add_argument("--url", required=True, help="endpoint URL, e.g. http://127.0.0.1:9009")
    tables.add_argument("--host-header")

    dump = commands.add_parser("dump", help="print one table as JSONL")
    dump.add_argument("--url", required=True, help="endpoint URL, e.g. http://127.0.0.1:9009")
    dump.add_argument("--host-header")
    dump.add_argument("--table", required=True, type=check_table)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "tables":
        for entry in Endpoint(args.url, args.host_header).tables():
            print(json.dumps(entry))
        return 0

    if args.command == "dump":
        for chunk in Endpoint(args.url, args.host_header).dump(args.table):
            sys.stdout.buffer.write(chunk)
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
            try:
                mirror.prune(time.time() - args.retain_hours * 3600)
            except sqlite3.OperationalError as error:
                if not is_locked(error):
                    raise
                LOG.warning("prune: %s; retrying next pass", error)
        LOG.debug("stored %d rows", stored)
        if args.follow is None:
            return 0
        time.sleep(args.follow)


if __name__ == "__main__":
    sys.exit(main())
