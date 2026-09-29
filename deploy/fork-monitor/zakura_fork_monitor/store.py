"""SQLite persistence for the fork monitor: schema plus a single-writer DAO.

One `Store` owns the only writable connection and must be used from a single
thread (the asyncio loop). Writes are batched into one transaction that is
committed once it is `commit_interval` seconds old or MAX_PENDING_WRITES long;
the owner must also call `commit_if_due()` periodically so a quiet batch is
flushed. Other threads (the web server) read through `reader()`, a thread-local
`query_only` connection; WAL mode lets them read while a write batch is open.

Blocks and headers are duck-typed (the attributes of `consensus.Block` /
`consensus.BlockHeader`), so this module has no import-time dependency on
consensus.py; block work is computed by a local mirror of
`consensus.work_from_bits`, clamped into SQLite's int64.

Data from peers and remote APIs is bounded before it is stored: free text is
capped at MAX_TEXT characters and hashes must be alphanumeric tokens of at most
64 characters (display hex in practice). `blocks.payout` holds the coinbase's
transparent outputs as compact JSON `[[address, zatoshis], ...]`; telling the
miner's payout from funding streams needs network params, so readers do that.
"""

from __future__ import annotations

import itertools
import json
import math
import operator
import re
import sqlite3
import threading
import time
import weakref
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any, Callable

SCHEMA_VERSION = 2
# Batch commits: bounded staleness for web readers without one fsync per sighting.
DEFAULT_COMMIT_INTERVAL = 1.0
# Bounds the WAL growth and lost work of a single open batch during backfill.
MAX_PENDING_WRITES = 5_000
# How long a connection waits on a lock (e.g. a checkpoint) before failing.
BUSY_TIMEOUT = 5.0
# Rows deleted per prune transaction, so a large backlog never holds the write lock long.
PRUNE_BATCH = 5_000
MAX_TEXT = 256
MAX_TEMPLATE = 32
MAX_NONCE = 64
# Consensus caps scriptSig at 100 bytes; the extra room only tolerates odd parsers.
MAX_SCRIPT_SIG = 256
MAX_PAYOUTS = 16
MAX_ADDRESS = 128
MAX_JSON = 256 * 1024
INT64_MIN, INT64_MAX = -(1 << 63), (1 << 63) - 1
# Sighting kinds timed at fetch, long after the block arrived: they never set `blocks.first_seen_*`.
UNTIMED_KINDS = frozenset({"backfill"})

_HASH_RE = re.compile(r"[0-9A-Za-z]{1,64}")
_UNTIMED_MARKS = ", ".join("?" * len(UNTIMED_KINDS))

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS blocks (
  hash TEXT PRIMARY KEY, prev_hash TEXT NOT NULL, height INTEGER,
  version INTEGER, time INTEGER NOT NULL, bits INTEGER NOT NULL, nonce TEXT, work INTEGER NOT NULL,
  is_min_diff INTEGER NOT NULL DEFAULT 0,
  size INTEGER, tx_count INTEGER,
  miner TEXT, miner_tag TEXT, template TEXT, payout TEXT, extranonce TEXT, coinbase_hex TEXT,
  body INTEGER NOT NULL DEFAULT 0,
  first_seen_at REAL, first_seen_source TEXT,
  created_at REAL NOT NULL,
  body_trusted INTEGER NOT NULL DEFAULT 1);
CREATE INDEX IF NOT EXISTS blocks_height ON blocks(height);
CREATE INDEX IF NOT EXISTS blocks_prev ON blocks(prev_hash);
CREATE TABLE IF NOT EXISTS sightings (
  hash TEXT NOT NULL, source TEXT NOT NULL, kind TEXT NOT NULL, at REAL NOT NULL,
  PRIMARY KEY (hash, source)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS sightings_at ON sightings(at);
CREATE TABLE IF NOT EXISTS sources (
  source TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  impl TEXT, impl_version TEXT, user_agent TEXT, protocol_version INTEGER, services INTEGER,
  discovered_via TEXT, first_seen_at REAL, last_ok_at REAL, last_error TEXT, last_error_at REAL,
  start_height INTEGER, tip_hash TEXT, tip_height INTEGER, tip_at REAL, tip_via TEXT,
  status TEXT);
CREATE TABLE IF NOT EXISTS tip_changes (
  id INTEGER PRIMARY KEY, source TEXT NOT NULL, at REAL NOT NULL,
  old_hash TEXT, old_height INTEGER, new_hash TEXT NOT NULL, new_height INTEGER,
  fork_hash TEXT, fork_height INTEGER, disconnected INTEGER NOT NULL DEFAULT 0, connected INTEGER NOT NULL DEFAULT 0,
  is_reorg INTEGER NOT NULL DEFAULT 0, disconnected_work INTEGER, connected_work INTEGER);
CREATE INDEX IF NOT EXISTS tip_changes_at ON tip_changes(at);
CREATE INDEX IF NOT EXISTS tip_changes_reorg ON tip_changes(is_reorg, at);
CREATE INDEX IF NOT EXISTS tip_changes_new ON tip_changes(new_hash);
CREATE INDEX IF NOT EXISTS tip_changes_fork ON tip_changes(is_reorg, fork_hash);
CREATE TABLE IF NOT EXISTS chaintips (
  source TEXT NOT NULL, hash TEXT NOT NULL, height INTEGER, branchlen INTEGER, status TEXT,
  first_at REAL NOT NULL, last_at REAL NOT NULL, PRIMARY KEY (source, hash)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS chaintips_last_at ON chaintips(last_at);
CREATE TABLE IF NOT EXISTS probes (
  id INTEGER PRIMARY KEY, at REAL NOT NULL, source TEXT NOT NULL, impl TEXT, hash TEXT NOT NULL,
  reason TEXT NOT NULL,
  result TEXT NOT NULL,
  latency_ms INTEGER, peer_tip_hash TEXT, announced_by_same_peer INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS probes_at ON probes(at);
CREATE INDEX IF NOT EXISTS probes_hash ON probes(hash);
CREATE TABLE IF NOT EXISTS split_events (
  id INTEGER PRIMARY KEY, started_at REAL NOT NULL, ended_at REAL, fork_hash TEXT, fork_height INTEGER,
  summary TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS external_orphans (
  source TEXT NOT NULL, hash TEXT NOT NULL, height INTEGER, prev_hash TEXT, canonical_hash TEXT,
  time INTEGER, difficulty REAL, miner_address TEXT, size INTEGER, first_seen_at REAL, detected_at REAL,
  raw TEXT, PRIMARY KEY (source, hash)) WITHOUT ROWID;
"""

# Columns a body fills in (see `Store._body_fields`).
_BODY_COLUMNS = ("size", "tx_count", "miner_tag", "template", "payout", "extranonce", "coinbase_hex")

# Schema changes from each older version, applied in order before SCHEMA (which only adds tables and
# indexes). Version 2: `blocks.body_trusted` (bodies stored before it existed count as trusted).
_MIGRATIONS = {
    1: ("ALTER TABLE blocks ADD COLUMN body_trusted INTEGER NOT NULL DEFAULT 1",),
}

# Row-value IN keeps each prune DELETE bounded, including on the WITHOUT ROWID table.
_PRUNE_DELETES = (
    ("sightings", "(hash, source) IN (SELECT hash, source FROM sightings WHERE at < ? LIMIT ?)"),
    ("probes", "id IN (SELECT id FROM probes WHERE at < ? LIMIT ?)"),
    ("tip_changes", "id IN (SELECT id FROM tip_changes WHERE at < ? LIMIT ?)"),
    # The RPC collector rewrites every tip it still lists at least every `rpc.CHAINTIP_WRITE_INTERVAL`.
    ("chaintips", "(source, hash) IN (SELECT source, hash FROM chaintips WHERE last_at < ? LIMIT ?)"),
)


def _text(value: Any, limit: int = MAX_TEXT) -> str | None:
    """Stringify and length-cap a possibly remote value; None passes through."""
    if value is None:
        return None
    return (value if isinstance(value, str) else str(value))[:limit]


def _int(value: Any) -> int | None:
    """Coerce to an SQLite INTEGER, clamped into int64 (e.g. u64 service bits); None passes through."""
    if value is None:
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"expected a finite number, got {value!r}")
        value = int(value)
    return max(INT64_MIN, min(INT64_MAX, operator.index(value)))


def _real(value: Any) -> float | None:
    """Coerce a timestamp or measurement to a finite float; None passes through."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"expected a finite number, got {value!r}")
    return float(value)


def _hash(value: Any) -> str | None:
    """Normalize a block hash to lowercase, rejecting anything but a short alphanumeric token."""
    if value is None:
        return None
    if not isinstance(value, str) or not _HASH_RE.fullmatch(value):
        raise ValueError(f"invalid block hash: {_text(value, 80)!r}")
    return value.lower()


def _json(value: Any) -> str | None:
    """Serialize a JSON-able value (strings are stored as given), bounded by MAX_JSON."""
    if value is None:
        return None
    text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), sort_keys=True)
    if len(text) > MAX_JSON:
        raise ValueError(f"JSON value of {len(text)} characters exceeds {MAX_JSON}")
    return text


def _required(value: Any, name: str) -> Any:
    """Reject a missing value for a NOT NULL column."""
    if value is None:
        raise ValueError(f"{name} is required")
    return value


Coerce = Callable[[Any], Any]

_SOURCE_COLUMNS: dict[str, Coerce] = {
    "kind": lambda v: _text(v, 16),
    "impl": _text,
    "impl_version": _text,
    "user_agent": _text,
    "protocol_version": _int,
    "services": _int,
    "discovered_via": _text,
    "first_seen_at": _real,
    "last_ok_at": _real,
    "last_error": _text,
    "last_error_at": _real,
    "start_height": _int,
    "tip_hash": _hash,
    "tip_height": _int,
    "tip_at": _real,
    "tip_via": _text,
    "status": _text,
}
_TIP_CHANGE_COLUMNS: dict[str, Coerce] = {
    "source": _text,
    "at": _real,
    "old_hash": _hash,
    "old_height": _int,
    "new_hash": _hash,
    "new_height": _int,
    "fork_hash": _hash,
    "fork_height": _int,
    "disconnected": _int,
    "connected": _int,
    "is_reorg": _int,
    "disconnected_work": _int,
    "connected_work": _int,
}
_PROBE_COLUMNS: dict[str, Coerce] = {
    "at": _real,
    "source": _text,
    "impl": _text,
    "hash": _hash,
    "reason": _text,
    "result": _text,
    "latency_ms": _int,
    "peer_tip_hash": _hash,
    "announced_by_same_peer": _int,
}
_SPLIT_COLUMNS: dict[str, Coerce] = {
    "started_at": _real,
    "ended_at": _real,
    "fork_hash": _hash,
    "fork_height": _int,
    "summary": _json,
}
_EXTERNAL_ORPHAN_COLUMNS: dict[str, Coerce] = {
    "source": _text,
    "hash": _hash,
    "height": _int,
    "prev_hash": _hash,
    "canonical_hash": _hash,
    "time": _int,
    "difficulty": _real,
    "miner_address": _text,
    "size": _int,
    "first_seen_at": _real,
    "detected_at": _real,
    "raw": _json,
}


def _coerce_row(
    columns: Mapping[str, Coerce], fields: Mapping[str, Any], required: Iterable[str], what: str
) -> dict[str, Any]:
    """Validate keyword fields against a table's column whitelist and coerce each value."""
    unknown = sorted(set(fields) - set(columns))
    if unknown:
        raise ValueError(f"{what}: unknown field(s) {', '.join(unknown)}")
    row = {name: columns[name](value) for name, value in fields.items()}
    missing = [name for name in required if row.get(name) is None]
    if missing:
        raise ValueError(f"{what}: missing required field(s) {', '.join(missing)}")
    return row


def _work_from_bits(bits: int) -> int:
    """Block work floor(2**256 / (target + 1)) clamped to int64; 0 for invalid compact bits.

    Mirrors `consensus.work_from_bits`, kept local so the store imports nothing from consensus.
    """
    mantissa = bits & 0x007FFFFF
    exponent = (bits >> 24) & 0xFF
    if bits & 0x00800000 or mantissa == 0:
        return 0
    target = mantissa >> (8 * (3 - exponent)) if exponent <= 3 else mantissa << (8 * (exponent - 3))
    if target == 0 or target >= 1 << 256:
        return 0
    return min((1 << 256) // (target + 1), INT64_MAX)


def _payouts_json(payouts: Any) -> str:
    """Encode coinbase payouts as compact `[[address, zatoshis], ...]` JSON, bounded."""
    entries = [
        [_text(address, MAX_ADDRESS), _int(value)] for address, value in itertools.islice(payouts or (), MAX_PAYOUTS)
    ]
    return json.dumps(entries, separators=(",", ":"))


def _script_hex(script_sig: Any) -> str | None:
    """Hex-encode (a bounded prefix of) the coinbase scriptSig."""
    if script_sig is None:
        return None
    if isinstance(script_sig, (bytes, bytearray, memoryview)):
        return bytes(script_sig[:MAX_SCRIPT_SIG]).hex()
    return _text(script_sig, 2 * MAX_SCRIPT_SIG)


class _ReaderHolder:
    """Owns one thread's read-only connection; closes it when the thread's locals are released."""

    __slots__ = ("conn", "__weakref__")

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Wrap `conn`."""
        self.conn = conn

    def __del__(self) -> None:
        """Close the connection when the owning thread exits (or the holder is dropped)."""
        try:
            self.conn.close()
        except Exception:  # sqlite3 globals may already be gone at interpreter shutdown
            pass


class Store:
    """SQLite store with one batched writer connection and thread-local read-only readers."""

    def __init__(self, path: str | Path, *, commit_interval: float = DEFAULT_COMMIT_INTERVAL) -> None:
        """Open (creating if needed) the database at `path` and apply the schema.

        Raises SystemExit if the file carries a schema version newer than this code.
        """
        self.path = str(path)
        self.commit_interval = commit_interval
        self._local = threading.local()
        self._readers: weakref.WeakSet[_ReaderHolder] = weakref.WeakSet()
        self._readers_lock = threading.Lock()
        self._pending = 0
        self._closed = False
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT, isolation_level="DEFERRED")
        try:
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = NORMAL")
            self._init_schema()
        except BaseException:
            self._conn.close()
            raise
        self._last_commit = time.monotonic()

    def _init_schema(self) -> None:
        """Check `meta.schema_version`, then create or migrate the schema."""
        conn = self._conn
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
        version = None
        if row is not None:
            try:
                version = int(row[0])
            except ValueError:
                raise SystemExit(f"{self.path}: unreadable schema_version {row[0]!r}") from None
            if version > SCHEMA_VERSION:
                raise SystemExit(
                    f"{self.path}: database schema version {version} is newer than this monitor "
                    f"supports ({SCHEMA_VERSION}); upgrade zakura-fork-monitor or use another --db"
                )
            if version < 1:
                raise SystemExit(f"{self.path}: unreadable schema_version {row[0]!r}")
            if version < SCHEMA_VERSION:
                # One transaction, so a crash mid-migration leaves the old version to migrate again.
                conn.execute("BEGIN")
                for step in range(version, SCHEMA_VERSION):
                    for statement in _MIGRATIONS[step]:
                        conn.execute(statement)
                conn.execute("UPDATE meta SET value = ? WHERE key = 'schema_version'", (str(SCHEMA_VERSION),))
                conn.commit()
                version = SCHEMA_VERSION
        conn.executescript(SCHEMA)
        if version != SCHEMA_VERSION:
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
        conn.commit()

    def close(self) -> None:
        """Commit pending writes and close the writer and every open reader."""
        if self._closed:
            return
        self._closed = True
        try:
            if self._conn.in_transaction:
                self._conn.commit()
        finally:
            self._conn.close()
            with self._readers_lock:
                holders = list(self._readers)
            for holder in holders:
                holder.conn.close()

    def __enter__(self) -> Store:
        """Use the store as a context manager that closes on exit."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Close the store."""
        self.close()

    def reader(self) -> sqlite3.Connection:
        """Return this thread's read-only connection, opening it on first use.

        It runs in autocommit mode (each statement sees the latest committed data;
        issue BEGIN/COMMIT for a multi-query snapshot) and is closed when the thread
        exits or the store closes.
        """
        if self._closed:
            raise sqlite3.ProgrammingError("store is closed")
        holder = getattr(self._local, "holder", None)
        if holder is None:
            # check_same_thread=False only so close() can close it from the owner thread.
            conn = sqlite3.connect(
                self.path, timeout=BUSY_TIMEOUT, isolation_level=None, check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only = 1")
            holder = _ReaderHolder(conn)
            self._local.holder = holder
            with self._readers_lock:
                self._readers.add(holder)
        return holder.conn

    def commit_if_due(self, force: bool = False) -> bool:
        """Commit the open write batch if forced, old enough or large enough; True if committed."""
        if not self._conn.in_transaction:
            self._pending = 0
            return False
        now = time.monotonic()
        if force or self._pending >= MAX_PENDING_WRITES or now - self._last_commit >= self.commit_interval:
            self._conn.commit()
            self._last_commit = now
            self._pending = 0
            return True
        return False

    def _wrote(self) -> None:
        """Count one write into the current batch and commit it when due."""
        self._pending += 1
        self.commit_if_due()

    def _insert(self, table: str, row: Mapping[str, Any]) -> int:
        """INSERT a whitelisted row (NULLs omitted so column defaults apply); return its rowid."""
        values = {name: value for name, value in row.items() if value is not None}
        columns = ", ".join(values)
        marks = ", ".join("?" * len(values))
        cursor = self._conn.execute(f"INSERT INTO {table} ({columns}) VALUES ({marks})", tuple(values.values()))
        self._wrote()
        return cursor.lastrowid

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        """Return a meta value, or `default` when unset."""
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return default if row is None else row[0]

    def set_meta(self, key: str, value: Any) -> None:
        """Set a meta value (stored as text)."""
        self._conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (_required(_text(key, 64), "key"), str(value)),
        )
        self._wrote()

    def upsert_block(
        self,
        block: Any | None,
        header: Any,
        height: int | None,
        *,
        miner: str | None,
        is_min_diff: bool,
        seen_at: float | None,
        seen_source: str,
        trusted: bool = True,
    ) -> bool:
        """Insert or enrich a block row; True if the block was new.

        A header-only row is upgraded in place when a body arrives (a body marks
        `body = 1` even if its coinbase could not be parsed, so it is not
        re-fetched), and so is an untrusted body's row when a `trusted` one
        arrives (see `chain.Node.body_trusted`). `height` is stored as given,
        never taken from the coinbase (the chain decides heights; an unplaced
        body's BIP34 height is unverified), and a stored height is never
        changed. `is_min_diff` only ever turns on. `first_seen_*` keeps the
        earliest timed observation, including sightings recorded before the
        block itself; `seen_at` None (an untimed fetch, see UNTIMED_KINDS)
        leaves it unset.
        """
        block_hash = _required(_hash(header.hash), "header.hash")
        seen_at = _real(seen_at)
        seen_source = _required(_text(seen_source), "seen_source")
        bits = operator.index(header.bits)
        if not 0 <= bits <= 0xFFFFFFFF:
            raise ValueError(f"bits out of range: {bits}")
        body = None
        if block is not None:
            body = {**self._body_fields(block), "body_trusted": int(bool(trusted))}
        height = _int(height)
        if height is not None and height < 0:
            raise ValueError(f"negative height {height}")

        row = self._conn.execute(
            "SELECT height, body, body_trusted, miner, is_min_diff, first_seen_at FROM blocks WHERE hash = ?",
            (block_hash,),
        ).fetchone()
        if row is None:
            first_at, first_source = (seen_at, seen_source) if seen_at is not None else (None, None)
            earliest = self._conn.execute(
                f"SELECT at, source FROM sightings WHERE hash = ? AND kind NOT IN ({_UNTIMED_MARKS}) "
                "ORDER BY at LIMIT 1",
                (block_hash, *UNTIMED_KINDS),
            ).fetchone()
            if earliest is not None and (first_at is None or earliest["at"] < first_at):
                first_at, first_source = earliest["at"], earliest["source"]
            values = {
                "hash": block_hash,
                "prev_hash": _required(_hash(header.prev_hash), "header.prev_hash"),
                "height": height,
                "version": _int(header.version),
                "time": _required(_int(header.time), "header.time"),
                "bits": bits,
                "nonce": _text(header.nonce, MAX_NONCE),
                "work": _work_from_bits(bits),
                "is_min_diff": int(bool(is_min_diff)),
                "miner": _text(miner),
                "first_seen_at": first_at,
                "first_seen_source": first_source,
                "created_at": time.time(),
                **(body or {"body_trusted": 0}),
            }
            self._insert("blocks", values)
            return True

        updates: dict[str, Any] = {}
        replacing = body is not None and row["body"] and trusted and not row["body_trusted"]
        upgrading = body is not None and (not row["body"] or replacing)
        if upgrading:
            # Every body column, so an untrusted body's values are not left behind.
            updates.update(dict.fromkeys(_BODY_COLUMNS), **body)
        # An untrusted caller never sets the miner of a row that holds a trusted body.
        locked = row["body"] and row["body_trusted"] and not trusted
        if replacing or (miner is not None and (upgrading or row["miner"] is None) and not locked):
            updates["miner"] = _text(miner)
        if height is not None and row["height"] is None:
            updates["height"] = height
        if is_min_diff and not row["is_min_diff"]:
            updates["is_min_diff"] = 1
        if seen_at is not None and (row["first_seen_at"] is None or seen_at < row["first_seen_at"]):
            updates["first_seen_at"] = seen_at
            updates["first_seen_source"] = seen_source
        if updates:
            assignments = ", ".join(f"{name} = ?" for name in updates)
            self._conn.execute(f"UPDATE blocks SET {assignments} WHERE hash = ?", (*updates.values(), block_hash))
            self._wrote()
        return False

    @staticmethod
    def _body_fields(block: Any) -> dict[str, Any]:
        """Columns filled in from a full block (size, tx count, coinbase attribution)."""
        fields: dict[str, Any] = {"size": _int(block.size), "tx_count": _int(block.tx_count), "body": 1}
        coinbase = block.coinbase
        if coinbase is not None:
            fields.update(
                miner_tag=_text(coinbase.tag),
                template=_text(coinbase.template, MAX_TEMPLATE),
                payout=_payouts_json(coinbase.payouts),
                extranonce=_text(coinbase.extranonce),
                coinbase_hex=_script_hex(coinbase.script_sig),
            )
        return fields

    def get_block(self, block_hash: str) -> sqlite3.Row | None:
        """Return one block row by hash, or None."""
        return self._conn.execute("SELECT * FROM blocks WHERE hash = ?", (_hash(block_hash),)).fetchone()

    def record_sighting(self, hash: str, source: str, kind: str, at: float) -> bool:
        """Record the first time `source` saw `hash`; True if this is its first sighting.

        A repeat keeps the earlier time, and `blocks.first_seen_*` is lowered when
        this sighting predates it (never by an UNTIMED_KINDS sighting).
        """
        block_hash = _required(_hash(hash), "hash")
        source = _required(_text(source), "source")
        kind = _required(_text(kind, 32), "kind")
        at = _required(_real(at), "at")
        inserted = (
            self._conn.execute(
                "INSERT OR IGNORE INTO sightings (hash, source, kind, at) VALUES (?, ?, ?, ?)",
                (block_hash, source, kind, at),
            ).rowcount
            == 1
        )
        if not inserted:
            self._conn.execute(
                "UPDATE sightings SET kind = ?, at = ? WHERE hash = ? AND source = ? AND at > ?",
                (kind, at, block_hash, source, at),
            )
        if kind not in UNTIMED_KINDS:
            self._conn.execute(
                "UPDATE blocks SET first_seen_at = ?, first_seen_source = ? "
                "WHERE hash = ? AND (first_seen_at IS NULL OR first_seen_at > ?)",
                (at, source, block_hash, at),
            )
        self._wrote()
        return inserted

    def upsert_source(self, source: str, **fields: Any) -> None:
        """Create or update a vantage point; only the given fields change.

        On insert `kind` defaults to the source prefix ("rpc"/"p2p") and
        `first_seen_at` to now; `first_seen_at` is never overwritten once set.
        """
        source = _required(_text(source), "source")
        row = _coerce_row(_SOURCE_COLUMNS, fields, (), "source")
        insert_only = set()
        if row.get("kind") is None:
            row["kind"] = _text(source.partition(":")[0], 16)
            insert_only.add("kind")
        if row.get("first_seen_at") is None:
            row["first_seen_at"] = time.time()
        columns = ["source", *row]
        assignments = ", ".join(
            "first_seen_at = COALESCE(sources.first_seen_at, excluded.first_seen_at)"
            if name == "first_seen_at"
            else f"{name} = excluded.{name}"
            for name in row
            if name not in insert_only
        )
        self._conn.execute(
            f"INSERT INTO sources ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))}) "
            f"ON CONFLICT(source) DO UPDATE SET {assignments}",
            (source, *row.values()),
        )
        self._wrote()

    def get_sources(self) -> list[dict[str, Any]]:
        """Return every source row as a dict, ordered by source."""
        return [dict(row) for row in self._conn.execute("SELECT * FROM sources ORDER BY source")]

    def record_tip_change(self, **fields: Any) -> int:
        """Append a tip transition (see the tip_changes columns); return its id."""
        row = _coerce_row(_TIP_CHANGE_COLUMNS, fields, ("source", "at", "new_hash"), "tip change")
        return self._insert("tip_changes", row)

    def upsert_chaintip(
        self, source: str, hash: str, height: int | None, branchlen: int | None, status: str | None, at: float
    ) -> None:
        """Record a getchaintips entry, widening its first/last observation window."""
        at = _required(_real(at), "at")
        self._conn.execute(
            "INSERT INTO chaintips (source, hash, height, branchlen, status, first_at, last_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(source, hash) DO UPDATE SET "
            "height = COALESCE(excluded.height, chaintips.height), branchlen = excluded.branchlen, "
            "status = excluded.status, first_at = MIN(chaintips.first_at, excluded.first_at), "
            "last_at = MAX(chaintips.last_at, excluded.last_at)",
            (
                _required(_text(source), "source"),
                _required(_hash(hash), "hash"),
                _int(height),
                _int(branchlen),
                _text(status, 32),
                at,
                at,
            ),
        )
        self._wrote()

    def record_probe(self, **fields: Any) -> int:
        """Append a getdata availability probe (see the probes columns); return its id."""
        row = _coerce_row(_PROBE_COLUMNS, fields, ("at", "source", "hash", "reason", "result"), "probe")
        return self._insert("probes", row)

    def open_split(self, **fields: Any) -> int:
        """Start a network split event (`summary` may be a JSON-able value); return its id."""
        row = _coerce_row(_SPLIT_COLUMNS, fields, ("started_at", "summary"), "split")
        return self._insert("split_events", row)

    def close_split(self, id: int, ended_at: float, summary: Any) -> None:
        """End a split event, replacing its summary unless `summary` is None."""
        self._conn.execute(
            "UPDATE split_events SET ended_at = ?, summary = COALESCE(?, summary) WHERE id = ?",
            (_required(_real(ended_at), "ended_at"), _json(summary), operator.index(id)),
        )
        self._wrote()

    def get_open_splits(self) -> list[dict[str, Any]]:
        """Return split events that have not ended (e.g. to resume or close them after a restart)."""
        return [
            dict(row) for row in self._conn.execute("SELECT * FROM split_events WHERE ended_at IS NULL ORDER BY id")
        ]

    def upsert_external_orphan(self, **fields: Any) -> bool:
        """Insert or refresh an external orphan record; True if it was new.

        Given non-null fields overwrite; `detected_at` keeps its first value.
        """
        row = _coerce_row(_EXTERNAL_ORPHAN_COLUMNS, fields, ("source", "hash"), "external orphan")
        if row.get("detected_at") is None:
            row["detected_at"] = time.time()
        values = {name: value for name, value in row.items() if value is not None}
        columns = ", ".join(values)
        cursor = self._conn.execute(
            f"INSERT OR IGNORE INTO external_orphans ({columns}) VALUES ({', '.join('?' * len(values))})",
            tuple(values.values()),
        )
        inserted = cursor.rowcount == 1
        if not inserted:
            updates = {k: v for k, v in values.items() if k not in ("source", "hash", "detected_at")}
            if updates:
                assignments = ", ".join(f"{name} = ?" for name in updates)
                self._conn.execute(
                    f"UPDATE external_orphans SET {assignments} WHERE source = ? AND hash = ?",
                    (*updates.values(), values["source"], values["hash"]),
                )
        self._wrote()
        return inserted

    def load_blocks(self, min_height: int | None = None) -> Iterable[sqlite3.Row]:
        """Stream block rows for Chain bootstrap, ordered by height (unknown heights last).

        Rows with an unknown height are always included since they may attach later.
        """
        low = INT64_MIN if min_height is None else operator.index(min_height)
        # Two index-ordered queries stream without sorting the whole table.
        known = self._conn.execute("SELECT * FROM blocks WHERE height >= ? ORDER BY height", (low,))
        unknown = self._conn.execute("SELECT * FROM blocks WHERE height IS NULL")
        return itertools.chain(known, unknown)

    def known_hashes(self) -> set[str]:
        """Return the hash of every stored block."""
        return {row[0] for row in self._conn.execute("SELECT hash FROM blocks")}

    def prune(self, before: float, *, batch: int = PRUNE_BATCH) -> dict[str, int]:
        """Delete sightings, probes, tip changes and chaintips older than `before`; return rows deleted per table.

        Blocks are kept. Runs `prune_batches` to completion.
        """
        deleted = {table: 0 for table, _ in _PRUNE_DELETES}
        for table, count in self.prune_batches(before, batch=batch):
            deleted[table] += count
        return deleted

    def prune_batches(self, before: float, *, batch: int = PRUNE_BATCH) -> Iterator[tuple[str, int]]:
        """Prune in `batch`-row DELETE transactions, yielding (table, rows deleted) after each commit.

        Short transactions never hold up web readers or WAL checkpoints, and an
        event-loop caller can yield between batches so a large backlog never stalls it.
        """
        before = _required(_real(before), "before")
        if batch < 1:
            raise ValueError("batch must be positive")
        for table, where in _PRUNE_DELETES:
            while True:
                count = self._conn.execute(f"DELETE FROM {table} WHERE {where}", (before, batch)).rowcount
                self._conn.commit()  # also commits any write batch opened since the last step
                self._last_commit = time.monotonic()
                self._pending = 0
                yield table, count
                if count < batch:
                    break
