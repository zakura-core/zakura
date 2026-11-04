"""SQLite storage for the NU7 mainnet dashboard.

Three tables, all written by the collector and read by the JSON API:

- `blocks`: one row per best-chain block the collector has seen, keyed by
  height. Header fields come from `getblock`; the fee, subsidy and operation
  columns come from the node's per-block counters and stay NULL on a build
  without them. A reorg deletes the rows above the fork point and they are
  refilled from the new chain.
- `samples`: one row per sample tick (mempool, peers, latency summaries and
  node health). Pruned after `SAMPLE_RETENTION_SECONDS`.
- `events`: reorgs, counter resets and other things worth a marker on a chart.
- `meta`: schema version and the last counter snapshot, so a collector restart
  does not lose a block's attribution.
"""

from __future__ import annotations

import json
import sqlite3
import time

SCHEMA_VERSION = 1
SAMPLE_RETENTION_SECONDS = 7 * 24 * 3600

BLOCK_COLUMNS = [
    "height", "hash", "time", "observed_at", "interval_s", "bits", "difficulty",
    "difficulty_change_pct", "size", "n_tx", "solps", "subsidy_total_zat",
    "subsidy_miner_zat", "subsidy_fs_zat", "subsidy_lockbox_zat", "fees_zat",
    "nsm_fee_zat", "miner_fee_zat", "reissuance_zat", "fee_split_ok",
    "nsm_balance_zat", "orchard_actions", "ironwood_actions", "sapling_ios",
    "sprout_joinsplits", "transparent_inputs", "transparent_outputs",
    "pool_transparent_zat", "pool_sprout_zat", "pool_sapling_zat",
    "pool_orchard_zat", "pool_ironwood_zat", "pool_lockbox_zat",
    "chain_supply_zat", "merged_from", "nu7_active", "source",
]

SAMPLE_COLUMNS = [
    "t", "height", "mempool_tx", "mempool_bytes", "mempool_actions_paid",
    "mempool_actions_unpaid", "mempool_evicted_total", "mempool_fee_p10_zat",
    "mempool_fee_p50_zat", "mempool_fee_p90_zat", "mempool_feerate_p50",
    "mempool_count", "node_fee_p50_zat", "node_fee_p90_zat", "node_fee_p99_zat",
    "peers", "peers_obsolete", "chain_tips", "sync_distance", "verify_p50_ms",
    "verify_p99_ms", "commit_p50_ms", "commit_p99_ms", "rocksdb_p99_ms",
    "rss_bytes", "restarts", "active_state", "scrape_ms",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS blocks (
  height INTEGER PRIMARY KEY, hash TEXT NOT NULL, time INTEGER NOT NULL,
  observed_at REAL, interval_s INTEGER, bits TEXT, difficulty REAL,
  difficulty_change_pct REAL, size INTEGER, n_tx INTEGER, solps INTEGER,
  subsidy_total_zat INTEGER, subsidy_miner_zat INTEGER, subsidy_fs_zat INTEGER,
  subsidy_lockbox_zat INTEGER, fees_zat INTEGER, nsm_fee_zat INTEGER,
  miner_fee_zat INTEGER, reissuance_zat INTEGER, fee_split_ok INTEGER,
  nsm_balance_zat INTEGER, orchard_actions INTEGER, ironwood_actions INTEGER,
  sapling_ios INTEGER, sprout_joinsplits INTEGER, transparent_inputs INTEGER,
  transparent_outputs INTEGER, pool_transparent_zat INTEGER, pool_sprout_zat INTEGER,
  pool_sapling_zat INTEGER, pool_orchard_zat INTEGER, pool_ironwood_zat INTEGER,
  pool_lockbox_zat INTEGER, chain_supply_zat INTEGER, merged_from INTEGER,
  nu7_active INTEGER NOT NULL DEFAULT 0, source TEXT NOT NULL DEFAULT 'live'
);
CREATE TABLE IF NOT EXISTS samples (
  t REAL PRIMARY KEY, height INTEGER, mempool_tx INTEGER, mempool_bytes INTEGER,
  mempool_actions_paid INTEGER, mempool_actions_unpaid INTEGER,
  mempool_evicted_total INTEGER, mempool_fee_p10_zat INTEGER,
  mempool_fee_p50_zat INTEGER, mempool_fee_p90_zat INTEGER, mempool_feerate_p50 REAL,
  mempool_count INTEGER, node_fee_p50_zat REAL, node_fee_p90_zat REAL,
  node_fee_p99_zat REAL, peers INTEGER, peers_obsolete INTEGER, chain_tips INTEGER,
  sync_distance INTEGER, verify_p50_ms REAL, verify_p99_ms REAL, commit_p50_ms REAL,
  commit_p99_ms REAL, rocksdb_p99_ms REAL, rss_bytes INTEGER, restarts INTEGER,
  active_state TEXT, scrape_ms REAL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY, t REAL NOT NULL, kind TEXT NOT NULL, height INTEGER,
  depth INTEGER, detail TEXT
);
CREATE INDEX IF NOT EXISTS events_t ON events(t);
"""


class Store:
    """One SQLite connection; the collector gives each thread its own."""

    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript(SCHEMA)
        self.set_meta("schema_version", SCHEMA_VERSION)

    def close(self):
        self.connection.close()

    # -- meta ---------------------------------------------------------------

    def set_meta(self, key, value):
        self.connection.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, json.dumps(value)),
        )

    def meta(self, key, default=None):
        row = self.connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return json.loads(row["value"]) if row else default

    # -- blocks -------------------------------------------------------------

    def upsert_block(self, values):
        """Insert or merge one block row; only the given columns are written."""
        columns = [column for column in BLOCK_COLUMNS if column in values]
        if "height" not in columns:
            raise ValueError("a block row needs a height")
        placeholders = ", ".join("?" for _ in columns)
        updates = ", ".join(f"{column} = excluded.{column}" for column in columns if column != "height")
        self.connection.execute(
            f"INSERT INTO blocks({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT(height) DO UPDATE SET {updates}",
            [values[column] for column in columns],
        )

    def update_block(self, height, values):
        """Set the given columns on an existing block row, if it exists."""
        columns = [column for column in BLOCK_COLUMNS if column in values and column != "height"]
        if not columns:
            return
        assignments = ", ".join(f"{column} = ?" for column in columns)
        self.connection.execute(
            f"UPDATE blocks SET {assignments} WHERE height = ?",
            [values[column] for column in columns] + [height],
        )

    def block(self, height):
        row = self.connection.execute("SELECT * FROM blocks WHERE height = ?", (height,)).fetchone()
        return dict(row) if row else None

    def tip(self):
        row = self.connection.execute("SELECT * FROM blocks ORDER BY height DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def blocks_since(self, height, limit=5000):
        rows = self.connection.execute(
            "SELECT * FROM blocks WHERE height >= ? ORDER BY height ASC LIMIT ?",
            (height, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def latest_blocks(self, count):
        rows = self.connection.execute(
            "SELECT * FROM blocks ORDER BY height DESC LIMIT ?", (count,)
        ).fetchall()
        return [dict(row) for row in rows]

    def rollback_above(self, fork_height):
        """Drop the rows above the fork point after a reorg."""
        self.connection.execute("DELETE FROM blocks WHERE height > ?", (fork_height,))

    def blocks_observed_since(self, since):
        row = self.connection.execute(
            "SELECT COUNT(*) AS n FROM blocks WHERE observed_at IS NOT NULL AND observed_at >= ?",
            (since,),
        ).fetchone()
        return row["n"]

    # -- samples ------------------------------------------------------------

    def insert_sample(self, values):
        columns = [column for column in SAMPLE_COLUMNS if column in values]
        placeholders = ", ".join("?" for _ in columns)
        self.connection.execute(
            f"INSERT OR REPLACE INTO samples({', '.join(columns)}) VALUES ({placeholders})",
            [values[column] for column in columns],
        )

    def samples_since(self, since, max_points=720):
        rows = self.connection.execute(
            "SELECT * FROM samples WHERE t >= ? ORDER BY t ASC", (since,)
        ).fetchall()
        rows = [dict(row) for row in rows]
        if len(rows) <= max_points:
            return rows
        step = len(rows) / max_points
        return [rows[int(index * step)] for index in range(max_points)]

    def latest_sample(self):
        row = self.connection.execute("SELECT * FROM samples ORDER BY t DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def prune_samples(self, now=None):
        cutoff = (time.time() if now is None else now) - SAMPLE_RETENTION_SECONDS
        self.connection.execute("DELETE FROM samples WHERE t < ?", (cutoff,))

    # -- events -------------------------------------------------------------

    def add_event(self, kind, t=None, height=None, depth=None, detail=None):
        self.connection.execute(
            "INSERT INTO events(t, kind, height, depth, detail) VALUES (?, ?, ?, ?, ?)",
            (time.time() if t is None else t, kind, height, depth, detail),
        )

    def events_since(self, since, limit=500):
        rows = self.connection.execute(
            "SELECT t, kind, height, depth, detail FROM events WHERE t >= ? ORDER BY t ASC LIMIT ?",
            (since, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def count_events(self, kind, since):
        row = self.connection.execute(
            "SELECT COUNT(*) AS n FROM events WHERE kind = ? AND t >= ?", (kind, since)
        ).fetchone()
        return row["n"]

    def blocks_in_window(self, since_time, limit=5000):
        """Blocks whose header time is at or after `since_time`, oldest first."""
        rows = self.connection.execute(
            "SELECT * FROM blocks WHERE time >= ? ORDER BY height ASC LIMIT ?",
            (since_time, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def intervals(self, count):
        """The last `count` stored block intervals, oldest first, as (height, interval)."""
        rows = self.connection.execute(
            "SELECT height, interval_s FROM blocks WHERE interval_s IS NOT NULL "
            "ORDER BY height DESC LIMIT ?",
            (count,),
        ).fetchall()
        return [(row["height"], row["interval_s"]) for row in reversed(rows)]

    def fee_totals_since(self, height):
        """Summed fees over the stored blocks at or above `height` that carry fee data."""
        row = self.connection.execute(
            "SELECT COUNT(*) AS blocks, SUM(fees_zat) AS fees, SUM(nsm_fee_zat) AS nsm, "
            "SUM(miner_fee_zat) AS miner, SUM(fee_split_ok) AS split_ok, "
            "SUM(reissuance_zat) AS reissuance FROM blocks "
            "WHERE height >= ? AND fees_zat IS NOT NULL",
            (height,),
        ).fetchone()
        return dict(row)
