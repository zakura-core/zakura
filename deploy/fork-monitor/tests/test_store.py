"""Tests for the SQLite store: schema, block/sighting upserts, DAO tables, WAL readers, prune."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

from zakura_fork_monitor import store as store_mod
from zakura_fork_monitor.store import Store

FIXTURES = Path(__file__).resolve().parent / "fixtures"
MIN_DIFF_BITS = 0x2007FFFF


def fake_header(n: int, prev: int | None = None, *, bits: int = 0x1F0AB3C0, time: int = 1_700_000_000):
    """Build a duck-typed header whose hash/prev_hash are derived from small integers."""
    return SimpleNamespace(
        hash=f"{n:064x}",
        prev_hash=f"{(n - 1 if prev is None else prev):064x}",
        version=4,
        time=time + n,
        bits=bits,
        nonce="ab" * 32,
    )


def fake_block(header, *, height: int | None = 100, tag: str = "zkcodexcoder", template: str | None = "zakura"):
    """Build a duck-typed full block with a parsed coinbase."""
    coinbase = SimpleNamespace(
        height=height,
        script_sig=bytes.fromhex("03aabbcc04f09f8cb8"),
        template=template,
        tag=tag,
        extranonce="deadbeef",
        payouts=(("tmMinerAddr", 250_000_000), ("t2FundingAddr", 25_000_000)),
        tx_version=5,
    )
    return SimpleNamespace(header=header, size=1_900, tx_count=2, coinbase=coinbase)


class StoreTestCase(unittest.TestCase):
    """Creates a fresh on-disk store per test."""

    commit_interval = 3_600.0

    def setUp(self) -> None:
        """Open a store in a temporary directory."""
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "monitor.sqlite3"
        self.store = Store(self.path, commit_interval=self.commit_interval)

    def tearDown(self) -> None:
        """Close the store and remove the directory."""
        self.store.close()
        self._tmp.cleanup()

    def block_row(self, n: int) -> sqlite3.Row:
        """Fetch the stored row for fake block `n`."""
        row = self.store.get_block(f"{n:064x}")
        self.assertIsNotNone(row)
        return row


class SchemaTests(StoreTestCase):
    """Schema creation, versioning and connection setup."""

    def test_schema_creation_is_idempotent(self) -> None:
        """Reopening an existing file keeps data and the schema version."""
        self.store.upsert_block(None, fake_header(1), 1, miner=None, is_min_diff=False, seen_at=1.0, seen_source="x")
        self.store.close()
        with Store(self.path) as reopened:
            self.assertEqual(reopened.get_meta("schema_version"), str(store_mod.SCHEMA_VERSION))
            self.assertEqual(reopened.known_hashes(), {f"{1:064x}"})
            tables = {
                row[0] for row in reopened.reader().execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
        self.assertTrue(
            {"meta", "blocks", "sightings", "sources", "tip_changes", "chaintips", "probes", "split_events",
             "external_orphans"} <= tables
        )

    def test_wal_mode(self) -> None:
        """The database runs in WAL mode."""
        mode = self.store.reader().execute("PRAGMA journal_mode").fetchone()[0]
        self.assertEqual(mode, "wal")

    def test_newer_schema_version_is_refused(self) -> None:
        """A database written by newer code refuses to open."""
        self.store.set_meta("schema_version", store_mod.SCHEMA_VERSION + 1)
        self.store.close()
        with self.assertRaises(SystemExit) as caught:
            Store(self.path)
        self.assertIn("newer", str(caught.exception))

    def test_version_1_databases_are_migrated(self) -> None:
        """A version 1 file gains `blocks.body_trusted`, with its stored bodies counted as trusted."""
        self.store.close()
        self.path.unlink()
        for suffix in ("-wal", "-shm"):
            Path(str(self.path) + suffix).unlink(missing_ok=True)
        # Every table a version 1 file had, minus the column version 2 added.
        v1_schema = store_mod.SCHEMA.replace(",\n  body_trusted INTEGER NOT NULL DEFAULT 1", "")
        self.assertNotIn("body_trusted", v1_schema)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(v1_schema)
            conn.execute("INSERT INTO meta VALUES ('schema_version', '1')")
            conn.execute("INSERT INTO blocks (hash, prev_hash, time, bits, work, body, created_at) "
                         "VALUES (?, ?, 1, 1, 1, 1, 1)", (f"{1:064x}", f"{0:064x}"))
        conn.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.get_meta("schema_version"), str(store_mod.SCHEMA_VERSION))
        self.assertEqual(self.block_row(1)["body_trusted"], 1)
        self.store.close()
        self.store = Store(self.path)  # reopening does not migrate again
        self.assertEqual(self.block_row(1)["body_trusted"], 1)

    def test_version_2_rewinds_are_relabelled(self) -> None:
        """Migrating from version 2 clears `is_reorg` on rows that connected no blocks."""
        self.store.record_tip_change(source="p2p:a", at=1.0, new_hash="aa" * 32, disconnected=997, connected=0,
                                     is_reorg=True)
        self.store.record_tip_change(source="p2p:a", at=2.0, new_hash="bb" * 32, disconnected=1, connected=2,
                                     is_reorg=True)
        self.store.set_meta("schema_version", 2)
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.get_meta("schema_version"), str(store_mod.SCHEMA_VERSION))
        rows = self.store.reader().execute("SELECT is_reorg FROM tip_changes ORDER BY at").fetchall()
        self.assertEqual([row[0] for row in rows], [0, 1])

    def test_unreadable_schema_version_is_refused(self) -> None:
        """A garbled schema_version refuses to open."""
        self.store.set_meta("schema_version", "banana")
        self.store.close()
        with self.assertRaises(SystemExit):
            Store(self.path)

    def test_meta_roundtrip(self) -> None:
        """get_meta/set_meta store text and honour defaults."""
        self.assertIsNone(self.store.get_meta("missing"))
        self.assertEqual(self.store.get_meta("missing", "d"), "d")
        self.store.set_meta("k", 5)
        self.store.set_meta("k", 6)
        self.assertEqual(self.store.get_meta("k"), "6")

    def test_close_is_idempotent_and_blocks_readers(self) -> None:
        """close() can repeat and later reader() calls fail."""
        self.store.reader()
        self.store.close()
        self.store.close()
        with self.assertRaises(sqlite3.ProgrammingError):
            self.store.reader()


class BlockTests(StoreTestCase):
    """upsert_block, work, first_seen and body upgrades."""

    def test_upsert_returns_new_only_once_and_keeps_earliest_first_seen(self) -> None:
        """Only the first upsert is new; first_seen only moves earlier."""
        header = fake_header(10)
        self.assertTrue(
            self.store.upsert_block(None, header, 100, miner=None, is_min_diff=False, seen_at=50.0, seen_source="a")
        )
        self.assertFalse(
            self.store.upsert_block(None, header, 100, miner=None, is_min_diff=False, seen_at=60.0, seen_source="b")
        )
        row = self.block_row(10)
        self.assertEqual((row["first_seen_at"], row["first_seen_source"]), (50.0, "a"))

        self.store.upsert_block(None, header, 100, miner=None, is_min_diff=False, seen_at=40.0, seen_source="c")
        row = self.block_row(10)
        self.assertEqual((row["first_seen_at"], row["first_seen_source"]), (40.0, "c"))
        self.assertEqual(row["body"], 0)
        self.assertEqual(row["prev_hash"], f"{9:064x}")
        self.assertEqual(row["height"], 100)

    def test_body_upgrades_header_only_row(self) -> None:
        """A body fills a header-only row and is never downgraded."""
        header = fake_header(11)
        self.store.upsert_block(None, header, None, miner=None, is_min_diff=False, seen_at=10.0, seen_source="p2p:x")
        row = self.block_row(11)
        self.assertEqual(row["body"], 0)
        self.assertIsNone(row["height"])
        self.assertIsNone(row["miner"])

        block = fake_block(header, height=4_410_736)
        is_new = self.store.upsert_block(
            block, header, 4_410_736, miner="zkcodexcoder", is_min_diff=False, seen_at=20.0, seen_source="rpc:a"
        )
        self.assertFalse(is_new)
        row = self.block_row(11)
        self.assertEqual(row["body"], 1)
        self.assertEqual(row["height"], 4_410_736)
        self.assertEqual(row["miner"], "zkcodexcoder")
        self.assertEqual(row["miner_tag"], "zkcodexcoder")
        self.assertEqual(row["template"], "zakura")
        self.assertEqual(row["extranonce"], "deadbeef")
        self.assertEqual(row["coinbase_hex"], "03aabbcc04f09f8cb8")
        self.assertEqual(json.loads(row["payout"]), [["tmMinerAddr", 250_000_000], ["t2FundingAddr", 25_000_000]])
        self.assertEqual((row["size"], row["tx_count"]), (1_900, 2))
        self.assertEqual(row["first_seen_at"], 10.0)

        # A later header-only observation never downgrades the body.
        self.store.upsert_block(None, header, 5, miner=None, is_min_diff=False, seen_at=30.0, seen_source="p2p:y")
        row = self.block_row(11)
        self.assertEqual((row["body"], row["height"], row["miner"]), (1, 4_410_736, "zkcodexcoder"))

    def test_heights_come_from_the_caller_only(self) -> None:
        """A body's BIP34 height neither fills a missing height nor replaces a stored one."""
        header = fake_header(24)
        self.store.upsert_block(fake_block(header, height=2_000_000_000), header, None, miner=None,
                                is_min_diff=False, seen_at=1.0, seen_source="p2p:x")
        self.assertIsNone(self.block_row(24)["height"])
        other = fake_header(25)
        self.store.upsert_block(None, other, 7, miner=None, is_min_diff=False, seen_at=1.0, seen_source="p2p:x")
        self.store.upsert_block(fake_block(other, height=5_000_000), other, None, miner=None, is_min_diff=False,
                                seen_at=2.0, seen_source="p2p:y")
        row = self.block_row(25)
        self.assertEqual((row["height"], row["body"]), (7, 1))

    def test_trusted_body_replaces_an_untrusted_one(self) -> None:
        """An untrusted body is replaced by a trusted one, every body column included; never the reverse."""
        header = fake_header(26)
        forged = fake_block(header, tag="forged")
        forged.coinbase.payouts = (("tmForged", 1),)
        self.store.upsert_block(forged, header, 5, miner="forged", is_min_diff=False, seen_at=1.0,
                                seen_source="p2p:x", trusted=False)
        self.assertEqual((self.block_row(26)["body_trusted"], self.block_row(26)["miner"]), (0, "forged"))
        real = SimpleNamespace(header=header, size=1_000, tx_count=1, coinbase=None)
        self.store.upsert_block(real, header, 5, miner=None, is_min_diff=False, seen_at=2.0, seen_source="rpc:a")
        row = self.block_row(26)
        self.assertEqual((row["body_trusted"], row["miner"], row["miner_tag"], row["payout"], row["size"]),
                         (1, None, None, None, 1_000))
        self.store.upsert_block(forged, header, 5, miner="forged", is_min_diff=False, seen_at=3.0,
                                seen_source="p2p:x", trusted=False)
        self.assertEqual((self.block_row(26)["body_trusted"], self.block_row(26)["miner"]), (1, None))

    def test_block_without_parsed_coinbase_still_counts_as_body(self) -> None:
        """A body whose coinbase did not parse still sets body=1."""
        header = fake_header(12)
        block = SimpleNamespace(header=header, size=1_600, tx_count=1, coinbase=None)
        self.store.upsert_block(block, header, 7, miner=None, is_min_diff=False, seen_at=1.0, seen_source="rpc:a")
        row = self.block_row(12)
        self.assertEqual((row["body"], row["height"], row["template"], row["payout"]), (1, 7, None, None))

    def test_min_diff_flag_only_turns_on(self) -> None:
        """is_min_diff is sticky once set."""
        header = fake_header(13, bits=MIN_DIFF_BITS)
        self.store.upsert_block(None, header, 1, miner=None, is_min_diff=False, seen_at=1.0, seen_source="a")
        self.store.upsert_block(None, header, 1, miner=None, is_min_diff=True, seen_at=2.0, seen_source="a")
        self.store.upsert_block(None, header, 1, miner=None, is_min_diff=False, seen_at=3.0, seen_source="a")
        self.assertEqual(self.block_row(13)["is_min_diff"], 1)

    def test_work_column(self) -> None:
        """work follows floor(2^256/(target+1)), clamped and 0 for invalid bits."""
        self.store.upsert_block(
            None, fake_header(14, bits=MIN_DIFF_BITS), 1, miner=None, is_min_diff=True, seen_at=1.0, seen_source="a"
        )
        self.assertEqual(self.block_row(14)["work"], 32)
        # Bitcoin's genesis bits have a well-known work of 0x100010001.
        self.assertEqual(store_mod._work_from_bits(0x1D00FFFF), 0x100010001)
        self.assertEqual(store_mod._work_from_bits(0x03000001), store_mod.INT64_MAX)
        self.assertEqual(store_mod._work_from_bits(0x04923456), 0)  # negative compact

    def test_work_matches_consensus_when_available(self) -> None:
        """The local work helper agrees with consensus.work_from_bits."""
        try:
            from zakura_fork_monitor import consensus
        except ImportError:
            self.skipTest("consensus.py not available")
        for bits in (MIN_DIFF_BITS, 0x1F0AB3C0, 0x1D00FFFF, 0x1C0FFFFF):
            self.assertEqual(store_mod._work_from_bits(bits), consensus.work_from_bits(bits))

    def test_earlier_sighting_seeds_first_seen_of_new_block(self) -> None:
        """A sighting recorded before the block sets its first_seen."""
        header = fake_header(15)
        self.store.record_sighting(header.hash, "p2p:1.2.3.4:18233", "inv", 5.0)
        self.store.upsert_block(None, header, 1, miner=None, is_min_diff=False, seen_at=9.0, seen_source="rpc:a")
        row = self.block_row(15)
        self.assertEqual((row["first_seen_at"], row["first_seen_source"]), (5.0, "p2p:1.2.3.4:18233"))

    def test_untimed_upserts_and_sightings_never_set_first_seen(self) -> None:
        """A backfill (seen_at None, kind "backfill") leaves first_seen unset; a timed sighting still fills it."""
        header = fake_header(18)
        self.store.record_sighting(header.hash, "rpc:a", "backfill", 3.0)
        self.store.upsert_block(None, header, 1, miner=None, is_min_diff=False, seen_at=None, seen_source="rpc:a")
        self.store.record_sighting(header.hash, "rpc:b", "backfill", 4.0)
        row = self.block_row(18)
        self.assertEqual((row["first_seen_at"], row["first_seen_source"]), (None, None))
        self.store.record_sighting(header.hash, "p2p:1.2.3.4:18233", "headers", 9.0)
        self.store.upsert_block(None, header, 1, miner=None, is_min_diff=False, seen_at=None, seen_source="rpc:a")
        self.assertEqual(self.block_row(18)["first_seen_at"], 9.0)
        # A new untimed row still takes an earlier timed sighting.
        other = fake_header(19)
        self.store.record_sighting(other.hash, "p2p:1.2.3.4:18233", "inv", 5.0)
        self.store.upsert_block(None, other, 1, miner=None, is_min_diff=False, seen_at=None, seen_source="rpc:a")
        self.assertEqual(self.block_row(19)["first_seen_at"], 5.0)

    def test_invalid_hash_is_rejected(self) -> None:
        """Malformed or oversized hashes never reach the table."""
        for bad in ("zz\n", "a" * 65, "", "<script>"):
            header = fake_header(16)
            header.hash = bad
            with self.assertRaises(ValueError):
                self.store.upsert_block(None, header, 1, miner=None, is_min_diff=False, seen_at=1.0, seen_source="a")

    def test_untrusted_text_is_capped(self) -> None:
        """Remote text and payout lists are bounded."""
        header = fake_header(17)
        block = fake_block(header, tag="x" * 5_000)
        block.coinbase.payouts = [(f"addr{i}", i) for i in range(1_000)]
        self.store.upsert_block(block, header, None, miner="m" * 999, is_min_diff=False, seen_at=1.0, seen_source="a")
        row = self.block_row(17)
        self.assertEqual(len(row["miner_tag"]), store_mod.MAX_TEXT)
        self.assertEqual(len(row["miner"]), store_mod.MAX_TEXT)
        self.assertEqual(len(json.loads(row["payout"])), store_mod.MAX_PAYOUTS)

    def test_load_blocks_orders_by_height_with_unknown_heights_last(self) -> None:
        """load_blocks streams by height with NULL heights last."""
        for n, height in ((20, 3), (21, 1), (22, None), (23, 2)):
            self.store.upsert_block(
                None, fake_header(n), height, miner=None, is_min_diff=False, seen_at=1.0, seen_source="a"
            )
        self.assertEqual([row["height"] for row in self.store.load_blocks()], [1, 2, 3, None])
        self.assertEqual([row["height"] for row in self.store.load_blocks(min_height=2)], [2, 3, None])
        self.assertEqual(self.store.known_hashes(), {f"{n:064x}" for n in (20, 21, 22, 23)})

    @unittest.skipUnless((FIXTURES / "block-test-4410736.hex").exists(), "fixture missing")
    def test_real_fixture_block_when_consensus_available(self) -> None:
        """A parsed fixture block stores its height and work."""
        try:
            from zakura_fork_monitor import consensus
        except ImportError:
            self.skipTest("consensus.py not available")
        raw = bytes.fromhex((FIXTURES / "block-test-4410736.hex").read_text().strip())
        block = consensus.parse_block(raw, consensus.TESTNET)
        self.assertTrue(
            self.store.upsert_block(
                block, block.header, block.coinbase.height, miner="x", is_min_diff=False, seen_at=1.0,
                seen_source="rpc:a",
            )
        )
        row = self.store.get_block(block.header.hash)
        self.assertEqual(row["height"], 4_410_736)
        self.assertEqual(row["body"], 1)
        self.assertEqual(row["work"], consensus.work_from_bits(block.header.bits))


class SightingTests(StoreTestCase):
    """record_sighting semantics."""

    def test_insert_or_ignore_and_lowering(self) -> None:
        """Repeats are ignored unless earlier; block first_seen follows."""
        header = fake_header(30)
        self.store.upsert_block(None, header, 1, miner=None, is_min_diff=False, seen_at=100.0, seen_source="rpc:a")
        self.assertTrue(self.store.record_sighting(header.hash, "p2p:1.1.1.1:18233", "inv", 90.0))
        self.assertFalse(self.store.record_sighting(header.hash, "p2p:1.1.1.1:18233", "headers", 95.0))
        rows = self.store.reader().execute("SELECT kind, at FROM sightings").fetchall()
        self.assertEqual([tuple(r) for r in rows], [])  # not committed yet: batch still open
        self.store.commit_if_due(force=True)
        rows = self.store.reader().execute("SELECT kind, at FROM sightings").fetchall()
        self.assertEqual([tuple(r) for r in rows], [("inv", 90.0)])
        row = self.block_row(30)
        self.assertEqual((row["first_seen_at"], row["first_seen_source"]), (90.0, "p2p:1.1.1.1:18233"))

        # An earlier repeat from the same source lowers both the sighting and the block.
        self.assertFalse(self.store.record_sighting(header.hash, "p2p:1.1.1.1:18233", "headers", 80.0))
        # A later sighting from another source changes neither.
        self.assertTrue(self.store.record_sighting(header.hash, "rpc:b", "rpc_tip", 200.0))
        self.store.commit_if_due(force=True)
        rows = self.store.reader().execute("SELECT source, kind, at FROM sightings ORDER BY at").fetchall()
        self.assertEqual(
            [tuple(r) for r in rows], [("p2p:1.1.1.1:18233", "headers", 80.0), ("rpc:b", "rpc_tip", 200.0)]
        )
        self.assertEqual(self.block_row(30)["first_seen_at"], 80.0)

    def test_sighting_before_block_is_kept(self) -> None:
        """Sightings may precede the block row."""
        self.assertTrue(self.store.record_sighting(f"{31:064x}", "p2p:1.1.1.1:18233", "inv", 1.0))
        self.assertIsNone(self.store.get_block(f"{31:064x}"))


class TableTests(StoreTestCase):
    """Sources, tip changes, chaintips, probes, splits and external orphans."""

    def test_sources(self) -> None:
        """Sources upsert partially, derive kind and keep first_seen_at."""
        self.store.upsert_source("p2p:1.2.3.4:18233", impl="zebra", user_agent="/Zebra:6.4.2/" + "x" * 1_000,
                                 services=(1 << 64) - 1, first_seen_at=10.0)
        self.store.upsert_source("p2p:1.2.3.4:18233", status="connected", first_seen_at=20.0, tip_hash="AB" * 32)
        self.store.upsert_source("rpc:zakura-testnet-1", status="ok")
        sources = {s["source"]: s for s in self.store.get_sources()}
        peer = sources["p2p:1.2.3.4:18233"]
        self.assertEqual(peer["kind"], "p2p")
        self.assertEqual(peer["impl"], "zebra")
        self.assertEqual(peer["status"], "connected")
        self.assertEqual(peer["first_seen_at"], 10.0)
        self.assertEqual(peer["tip_hash"], "ab" * 32)
        self.assertEqual(peer["services"], store_mod.INT64_MAX)
        self.assertEqual(len(peer["user_agent"]), store_mod.MAX_TEXT)
        self.assertEqual(sources["rpc:zakura-testnet-1"]["kind"], "rpc")
        self.store.upsert_source("cipherscan", kind="external")
        self.store.upsert_source("cipherscan", status="ok")
        self.assertEqual({s["source"]: s["kind"] for s in self.store.get_sources()}["cipherscan"], "external")
        self.assertIsNotNone(sources["rpc:zakura-testnet-1"]["first_seen_at"])
        with self.assertRaises(ValueError):
            self.store.upsert_source("p2p:1.2.3.4:18233", bogus=1)

    def test_tip_changes(self) -> None:
        """Tip changes append with defaults, clamping and field checks."""
        first = self.store.record_tip_change(
            source="rpc:a", at=1.0, old_hash="aa" * 32, old_height=10, new_hash="bb" * 32, new_height=11
        )
        second = self.store.record_tip_change(
            source="rpc:a", at=2.0, old_hash="bb" * 32, new_hash="cc" * 32, fork_hash="aa" * 32, fork_height=10,
            disconnected=1, connected=2, is_reorg=True, disconnected_work=1 << 70, connected_work=64,
        )
        self.assertGreater(second, first)
        self.store.commit_if_due(force=True)
        rows = self.store.reader().execute("SELECT * FROM tip_changes ORDER BY id").fetchall()
        self.assertEqual((rows[0]["is_reorg"], rows[0]["disconnected"]), (0, 0))
        self.assertEqual((rows[1]["is_reorg"], rows[1]["connected"]), (1, 2))
        self.assertEqual(rows[1]["disconnected_work"], store_mod.INT64_MAX)
        with self.assertRaises(ValueError):
            self.store.record_tip_change(source="rpc:a", at=3.0)
        with self.assertRaises(ValueError):
            self.store.record_tip_change(source="rpc:a", at=3.0, new_hash="dd" * 32, extra=1)

    def test_chaintips_window(self) -> None:
        """Chaintips widen their first/last window and keep the latest status."""
        self.store.upsert_chaintip("rpc:a", "aa" * 32, 100, 1, "valid-fork", 50.0)
        self.store.upsert_chaintip("rpc:a", "aa" * 32, 100, 2, "valid-headers", 70.0)
        self.store.upsert_chaintip("rpc:a", "aa" * 32, None, 2, "valid-headers", 40.0)
        row = self.store._conn.execute("SELECT * FROM chaintips").fetchone()
        self.assertEqual(
            (row["height"], row["branchlen"], row["status"], row["first_at"], row["last_at"]),
            (100, 2, "valid-headers", 40.0, 70.0),
        )

    def test_probes(self) -> None:
        """Probes append with defaults and require their key fields."""
        probe_id = self.store.record_probe(
            at=1.0, source="p2p:1.2.3.4:18233", impl="zebra", hash="aa" * 32, reason="announce",
            result="notfound", latency_ms=120, announced_by_same_peer=True,
        )
        self.store.record_probe(at=2.0, source="p2p:1.2.3.4:18233", hash="bb" * 32, reason="fetch", result="block")
        rows = self.store._conn.execute("SELECT * FROM probes ORDER BY id").fetchall()
        self.assertEqual(rows[0]["id"], probe_id)
        self.assertEqual((rows[0]["result"], rows[0]["announced_by_same_peer"]), ("notfound", 1))
        self.assertEqual(rows[1]["announced_by_same_peer"], 0)
        with self.assertRaises(ValueError):
            self.store.record_probe(at=3.0, source="p2p:x", hash="cc" * 32, reason="fetch")

    def test_splits_open_and_close(self) -> None:
        """Splits open, list while open and close with an optional summary."""
        split_id = self.store.open_split(
            started_at=10.0, fork_hash="aa" * 32, fork_height=100, summary={"groups": ["zebra-6.4", "zakura"]}
        )
        self.assertEqual([s["id"] for s in self.store.get_open_splits()], [split_id])
        self.store.close_split(split_id, 70.0, {"groups": [], "resolved": True})
        self.assertEqual(self.store.get_open_splits(), [])
        row = self.store._conn.execute("SELECT * FROM split_events WHERE id = ?", (split_id,)).fetchone()
        self.assertEqual(row["ended_at"], 70.0)
        self.assertEqual(json.loads(row["summary"]), {"groups": [], "resolved": True})

        other = self.store.open_split(started_at=80.0, summary="{}")
        self.store.close_split(other, 90.0, None)
        row = self.store._conn.execute("SELECT summary FROM split_events WHERE id = ?", (other,)).fetchone()
        self.assertEqual(row["summary"], "{}")

    def test_external_orphans(self) -> None:
        """External orphans upsert and keep their first detected_at."""
        self.assertTrue(
            self.store.upsert_external_orphan(
                source="cipherscan", hash="aa" * 32, height=100, prev_hash="bb" * 32, detected_at=5.0,
                raw={"hash": "aa" * 32, "miner": "<b>x</b>"},
            )
        )
        self.assertFalse(
            self.store.upsert_external_orphan(
                source="cipherscan", hash="aa" * 32, canonical_hash="cc" * 32, detected_at=9.0, difficulty=1.5
            )
        )
        row = self.store._conn.execute("SELECT * FROM external_orphans").fetchone()
        self.assertEqual(row["detected_at"], 5.0)
        self.assertEqual(row["canonical_hash"], "cc" * 32)
        self.assertEqual(row["height"], 100)
        self.assertEqual(row["difficulty"], 1.5)
        self.assertEqual(json.loads(row["raw"])["miner"], "<b>x</b>")
        with self.assertRaises(ValueError):
            self.store.upsert_external_orphan(source="cipherscan")


class ConcurrencyTests(StoreTestCase):
    """WAL reader isolation and batching."""

    def test_reader_isolated_from_open_batch_and_not_blocked(self) -> None:
        """WAL readers see only committed data and never block the writer."""
        self.store.upsert_block(None, fake_header(40), 1, miner=None, is_min_diff=False, seen_at=1.0, seen_source="a")
        self.store.commit_if_due(force=True)
        self.store.upsert_block(None, fake_header(41), 2, miner=None, is_min_diff=False, seen_at=1.0, seen_source="a")
        self.assertTrue(self.store._conn.in_transaction)

        snapshot_open = threading.Event()
        writer_done = threading.Event()
        results: dict[str, object] = {}

        def read() -> None:
            """Read mid-batch, hold a snapshot across a commit, then re-read."""
            try:
                conn = self.store.reader()
                results["mid_batch"] = conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
                try:
                    conn.execute("DELETE FROM blocks")
                except sqlite3.OperationalError as err:
                    results["write_error"] = str(err)
                conn.execute("BEGIN")
                results["snapshot_before"] = conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
                snapshot_open.set()
                writer_done.wait(10)
                results["snapshot_after"] = conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
                conn.execute("COMMIT")
                results["fresh"] = conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
            except Exception as err:  # surfaced through results for the main thread's assertions
                results["error"] = repr(err)
                snapshot_open.set()

        thread = threading.Thread(target=read)
        thread.start()
        self.assertTrue(snapshot_open.wait(10))
        # The writer commits while the reader holds an open snapshot (WAL: no lock wait).
        self.store.commit_if_due(force=True)
        self.store.upsert_block(None, fake_header(42), 3, miner=None, is_min_diff=False, seen_at=1.0, seen_source="a")
        self.store.commit_if_due(force=True)
        writer_done.set()
        thread.join(10)

        self.assertNotIn("error", results)
        self.assertEqual(results["mid_batch"], 1)
        self.assertIn("readonly", results["write_error"])
        self.assertEqual(results["snapshot_before"], 1)
        self.assertEqual(results["snapshot_after"], 1)
        self.assertEqual(results["fresh"], 3)

    def test_readers_are_per_thread(self) -> None:
        """Each thread gets its own reader connection."""
        main_reader = self.store.reader()
        self.assertIs(self.store.reader(), main_reader)
        other: list[sqlite3.Connection] = []
        thread = threading.Thread(target=lambda: other.append(self.store.reader()))
        thread.start()
        thread.join(10)
        self.assertIsNot(other[0], main_reader)

    def test_commit_after_interval(self) -> None:
        """A write commits once the batch interval has passed."""
        self.store.commit_interval = 0.0
        self.store.set_meta("k", "v")
        self.assertFalse(self.store._conn.in_transaction)
        self.assertEqual(self.store.reader().execute("SELECT value FROM meta WHERE key = 'k'").fetchone()[0], "v")

    def test_commit_after_max_pending_writes(self) -> None:
        """A batch commits once it holds MAX_PENDING_WRITES writes."""
        for i in range(store_mod.MAX_PENDING_WRITES):
            self.store.record_sighting(f"{i:064x}", "p2p:1.1.1.1:18233", "inv", float(i))
        self.assertFalse(self.store._conn.in_transaction)
        self.assertFalse(self.store.commit_if_due())


class PruneTests(StoreTestCase):
    """prune() removes only old observation rows."""

    def test_prune(self) -> None:
        """prune deletes old observation rows and chaintips in batches and keeps blocks."""
        for i in range(12):
            at = float(i * 10)
            self.store.record_sighting(f"{i:064x}", "p2p:1.1.1.1:18233", "inv", at)
            self.store.record_probe(at=at, source="p2p:1.1.1.1:18233", hash=f"{i:064x}", reason="fetch",
                                    result="timeout")
            self.store.record_tip_change(source="rpc:a", at=at, new_hash=f"{i:064x}")
            self.store.upsert_chaintip("rpc:a", f"{i:064x}", i, 0, "active", at)
            self.store.upsert_block(None, fake_header(i + 1), i, miner=None, is_min_diff=False, seen_at=at,
                                    seen_source="a")
        deleted = self.store.prune(55.0, batch=2)
        self.assertEqual(deleted, {"sightings": 6, "probes": 6, "tip_changes": 6, "chaintips": 6})
        conn = self.store.reader()
        for table in ("sightings", "probes", "tip_changes"):
            self.assertEqual(tuple(conn.execute(f"SELECT MIN(at), COUNT(*) FROM {table}").fetchone()), (60.0, 6))
        self.assertEqual(tuple(conn.execute("SELECT MIN(last_at), COUNT(*) FROM chaintips").fetchone()), (60.0, 6))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0], 12)
        self.assertEqual(self.store.prune(55.0), {"sightings": 0, "probes": 0, "tip_changes": 0, "chaintips": 0})

    def test_prune_batches_commit_between_steps(self) -> None:
        """prune_batches yields after every committed batch, so readers see progress and the caller can pause."""
        for i in range(5):
            self.store.record_sighting(f"{i:064x}", "p2p:1.1.1.1:18233", "inv", float(i))
        self.store.commit_if_due(force=True)
        steps = self.store.prune_batches(10.0, batch=2)
        self.assertEqual(next(steps), ("sightings", 2))
        self.assertFalse(self.store._conn.in_transaction)
        self.assertEqual(self.store.reader().execute("SELECT COUNT(*) FROM sightings").fetchone()[0], 3)
        self.assertEqual(
            list(steps), [("sightings", 2), ("sightings", 1), ("probes", 0), ("tip_changes", 0), ("chaintips", 0)]
        )


if __name__ == "__main__":
    unittest.main()
