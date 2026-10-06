"""Tests for the in-memory block tree: canonical chain, forks, reorgs, sawtooth phases and bounds."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from zakura_fork_monitor import chain as chain_mod
from zakura_fork_monitor.chain import Chain
from zakura_fork_monitor.consensus import (
    MAINNET,
    TESTNET,
    Block,
    BlockHeader,
    Coinbase,
    difficulty_from_bits,
    work_from_bits,
)
from zakura_fork_monitor.store import Store

FIXTURES = Path(__file__).resolve().parent / "fixtures"
BASE = 1_000_000
T0 = 1_790_000_000
NORMAL = 0x1E0D94E6  # difficulty ~38.6k, a late-cycle testnet block
EASY = 0x1F76710D  # difficulty ~17, the first block after a reset
MIN = TESTNET.pow_limit_bits
AUTO = object()


def bhash(name: str) -> str:
    """Derive a 64-hex block hash from a test block name."""
    return hashlib.sha256(name.encode()).hexdigest()


def raw_hash(last_byte: int) -> str:
    """Build a display hash whose raw (reversed) key starts with `last_byte`."""
    return "11" * 31 + f"{last_byte:02x}"


def header(block_hash: str, prev_hash: str, time: int, bits: int = NORMAL) -> BlockHeader:
    """Build a synthetic header (the raw bytes are not needed by the tree)."""
    return BlockHeader(
        hash=block_hash,
        prev_hash=prev_hash,
        version=4,
        merkle_root="00" * 32,
        time=time,
        bits=bits,
        nonce="00" * 32,
        raw=b"",
    )


def coinbase_block(hdr: BlockHeader, height: int, *, tag: str = "zkcodexcoder", template: str = "zakura") -> Block:
    """Wrap a header in a parsed block whose coinbase carries a miner tag and template marker."""
    coinbase = Coinbase(
        height=height, script_sig=b"\x03abc", template=template, tag=tag, extranonce="ab01", payouts=(), tx_version=5
    )
    return Block(header=hdr, size=1_900, tx_count=1, coinbase=coinbase)


def row(block_hash: str, prev_hash: str, height: int | None, **fields) -> dict:
    """Build a `blocks` table row as Store.load_blocks returns it."""
    base = {
        "hash": block_hash,
        "prev_hash": prev_hash,
        "height": height,
        "time": T0 + (height or 0),
        "bits": NORMAL,
        "miner": None,
        "template": None,
        "miner_tag": None,
        "extranonce": None,
        "first_seen_at": None,
        "body": 0,
    }
    base.update(fields)
    return base


class Builder:
    """Adds named synthetic blocks to a Chain; times follow the parent and first-seen follows arrival."""

    def __init__(self, chain: Chain | None = None, *, settle_depth: int = 3) -> None:
        """Wrap `chain` (default: a fresh testnet chain)."""
        self.chain = chain if chain is not None else Chain(TESTNET, settle_depth)
        self.hashes: dict[str, str] = {}
        self.times: dict[str, int] = {}
        self.clock = 0.0

    def hash(self, name: str) -> str:
        """Return the hash registered for `name`, or one derived from it."""
        return self.hashes.get(name) or bhash(name)

    def add(
        self,
        name: str,
        parent: str,
        *,
        dt: int = 20,
        bits: int = NORMAL,
        miner: str | None = "A",
        seen: object = AUTO,
        height: int | None = None,
        at: int | None = None,
        block_hash: str | None = None,
        body: bool = False,
    ):
        """Add block `name` on top of `parent`; return the chain's result."""
        if block_hash is not None:
            self.hashes[name] = block_hash
        block_time = at if at is not None else self.times.get(parent, T0) + dt
        self.times[name] = block_time
        if seen is AUTO:
            self.clock += 1.0
            seen = self.clock
        hdr = header(self.hash(name), self.hash(parent), block_time, bits)
        block = coinbase_block(hdr, height or 0, tag=miner or "") if body else None
        return self.chain.add(hdr, height, block=block, miner=None if body else miner, first_seen_at=seen)

    def genesis(self, name: str = "g", **kwargs):
        """Add the first block at height BASE."""
        return self.add(name, f"before-{name}", height=BASE, at=T0, **kwargs)

    def line(self, prefix: str, parent: str, first: int, last: int, **kwargs) -> list[str]:
        """Add blocks `prefix<first>`..`prefix<last>` in a line on `parent`; return their names."""
        names = []
        for index in range(first, last + 1):
            name = f"{prefix}{index}"
            self.add(name, parent, **kwargs)
            names.append(name)
            parent = name
        return names

    def best(self) -> str:
        """Return the best tip's hash."""
        return self.chain.best_tip().hash


class LinearChainTests(unittest.TestCase):
    """A single chain: heights, canonical index, ancestry and idempotent adds."""

    def setUp(self) -> None:
        """Build g, a1..a9."""
        self.b = Builder()
        self.b.genesis()
        self.b.line("a", "g", 1, 9)
        self.chain = self.b.chain

    def test_heights_best_and_canonical_index(self) -> None:
        """Heights come from the parent, and the canonical index covers the whole line."""
        best = self.chain.best_tip()
        self.assertEqual(best.hash, self.b.hash("a9"))
        self.assertEqual(best.height, BASE + 9)
        self.assertEqual(len(self.chain), 10)
        self.assertEqual(self.chain.canonical_hash_at(BASE), self.b.hash("g"))
        self.assertEqual(self.chain.canonical_hash_at(BASE + 4), self.b.hash("a4"))
        self.assertIsNone(self.chain.canonical_hash_at(BASE + 10))
        self.assertIsNone(self.chain.canonical_hash_at(BASE - 1))
        self.assertEqual([n.height for n in self.chain.canonical_nodes(BASE + 8)], [BASE + 8, BASE + 9])
        genesis_work = self.chain.get(self.b.hash("g")).cumwork
        self.assertEqual(self.chain.best_tip().cumwork - genesis_work, 9 * work_from_bits(NORMAL))
        self.assertEqual(self.chain.stale_blocks(), [])
        self.assertEqual(self.chain.fork_events(), [])
        self.assertIn(self.b.hash("a3"), self.chain)
        self.assertNotIn("ff" * 32, self.chain)

    def test_ancestry_and_path(self) -> None:
        """is_ancestor, fork_point and path follow the line in both directions."""
        a3, a7 = self.b.hash("a3"), self.b.hash("a7")
        self.assertTrue(self.chain.is_ancestor(a3, a7))
        self.assertTrue(self.chain.is_ancestor(a3, a3))
        self.assertFalse(self.chain.is_ancestor(a7, a3))
        self.assertFalse(self.chain.is_ancestor("ff" * 32, a3))
        self.assertEqual(self.chain.fork_point(a3, a7).hash, a3)
        self.assertEqual([n.hash for n in self.chain.path(a3, a7)], [self.b.hash(f"a{i}") for i in range(4, 8)])
        self.assertEqual(self.chain.path(a3, a3), [])
        with self.assertRaises(ValueError):
            self.chain.path(a7, a3)
        with self.assertRaises(KeyError):
            self.chain.path("ff" * 32, a3)

    def test_add_is_idempotent_and_upgrades_body(self) -> None:
        """Re-adding with a body fills the coinbase fields and keeps the earliest first-seen time."""
        node = self.chain.get(self.b.hash("a5"))
        self.assertFalse(node.body)
        hdr = header(node.hash, node.prev_hash, node.time)
        again = self.chain.add(hdr, None, block=coinbase_block(hdr, BASE + 5, tag="Foundry USA"), first_seen_at=0.5)
        self.assertIs(again, node)
        self.assertEqual(len(self.chain), 10)
        self.assertTrue(node.body)
        self.assertEqual(
            (node.miner, node.template, node.tag, node.extranonce), ("Foundry", "zakura", "Foundry USA", "ab01")
        )
        self.assertEqual(node.first_seen_at, 0.5)
        self.chain.add(hdr, None, first_seen_at=99.0)
        self.assertEqual(node.first_seen_at, 0.5)

    def test_trusted_bodies_replace_untrusted_attribution(self) -> None:
        """An untrusted body gives way to a trusted one, never the other way round."""
        node = self.chain.get(self.b.hash("a5"))
        hdr = header(node.hash, node.prev_hash, node.time)
        self.chain.add(hdr, None, block=coinbase_block(hdr, BASE + 5, tag="forged Foundry"), trusted=False)
        self.assertEqual((node.body, node.body_trusted, node.miner), (True, False, "Foundry"))
        self.chain.add(hdr, None, block=coinbase_block(hdr, BASE + 5, tag="toalt23 again"), trusted=False)
        self.assertEqual(node.tag, "forged Foundry")
        self.chain.add(hdr, None, block=coinbase_block(hdr, BASE + 5, tag="zkcodexcoder"))
        self.assertEqual((node.body_trusted, node.miner, node.tag), (True, "zkcodexcoder", "zkcodexcoder"))
        self.chain.add(hdr, None, block=coinbase_block(hdr, BASE + 5, tag="Foundry USA"), trusted=False)
        self.assertEqual((node.body_trusted, node.miner), (True, "zkcodexcoder"))

    def test_malformed_input(self) -> None:
        """Bad hashes, self-parenting and mismatched blocks raise; invalid bits and wild heights are dropped."""
        tip = self.b.hash("a9")
        with self.assertRaises(ValueError):
            self.chain.add(header("", tip, T0), None)
        with self.assertRaises(ValueError):
            self.chain.add(header("ab" * 40, tip, T0), None)
        with self.assertRaises(ValueError):
            self.chain.add(header(tip, tip, T0), None)
        with self.assertRaises(ValueError):
            hdr = header(bhash("x"), tip, T0)
            self.chain.add(hdr, None, block=coinbase_block(header(bhash("y"), tip, T0), 1))
        self.assertIsNone(self.chain.add(header(bhash("bad-bits"), tip, T0, bits=0x04923456), None))
        # A garbage height hint is ignored, so an unanchored header just waits.
        self.assertIsNone(self.chain.add(header(bhash("wild"), bhash("nowhere"), T0), 1 << 40))
        self.assertEqual(self.chain.best_tip().hash, tip)


class ForkTests(unittest.TestCase):
    """Sibling races, reorgs, tie-breaks and detached ancestry."""

    def test_one_block_sibling_race(self) -> None:
        """A losing sibling becomes stale once settled and forms a race fork event."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 4)
        b.add("x4", "a3", miner="B", dt=21)
        self.assertEqual(b.best(), b.hash("a4"))
        self.assertEqual(b.chain.stale_blocks(), [])
        b.line("a", "a4", 5, 7)
        self.assertEqual([n.hash for n in b.chain.stale_blocks()], [b.hash("x4")])
        self.assertEqual(b.chain.stale_blocks(since_height=BASE + 5), [])
        [event] = b.chain.fork_events()
        self.assertEqual((event.fork_hash, event.fork_height), (b.hash("a3"), BASE + 3))
        self.assertEqual(event.winner.hash, b.hash("a4"))
        [loser] = event.losers
        self.assertEqual((loser.block.hash, loser.tip_hash, loser.length, loser.blocks), (b.hash("x4"),) * 2 + (1, 1))
        self.assertEqual(loser.work, work_from_bits(NORMAL))
        self.assertEqual(loser.miners, ("B",))
        self.assertEqual((loser.classification, event.classification), ("race", "race"))
        self.assertFalse(event.same_job)
        self.assertTrue(event.winner_first_seen)
        self.assertFalse(loser.seen_first)
        self.assertTrue(event.equal_work)
        self.assertEqual(loser.winner_len, 1)
        self.assertTrue(event.settled)
        self.assertEqual((event.depth, event.depth_work), (1, work_from_bits(NORMAL)))
        self.assertEqual(b.chain.fork_events(since_height=BASE + 4), [])

    def test_self_orphan_and_same_job(self) -> None:
        """Same-miner siblings are self-orphans; with the same header time they are the same job."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 2)
        b.add("w3", "a2", miner="zkcodexcoder", dt=5)
        b.add("s3", "a2", miner="zkcodexcoder", dt=5)
        b.add("w4", "w3")
        b.add("t4", "w3", miner="zkcodexcoder", dt=9)
        [first, second] = b.chain.fork_events()
        self.assertEqual((first.classification, first.same_job), ("self", True))
        self.assertEqual((second.classification, second.same_job), ("race", False))
        b.add("v4", "w3", miner="A", dt=30)
        b.add("u4", "w3", miner=None)
        losers = {loser.block.hash: loser for loser in b.chain.fork_events()[1].losers}
        self.assertEqual(losers[b.hash("v4")].classification, "self")
        self.assertFalse(losers[b.hash("v4")].same_job)
        self.assertEqual(losers[b.hash("u4")].classification, "unknown")
        self.assertEqual(losers[b.hash("u4")].miners, ("unknown",))
        self.assertEqual(b.chain.fork_events()[1].classification, "race")

    def test_three_deep_reorg(self) -> None:
        """A heavier side branch replaces three canonical blocks."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 5)
        b.line("b", "a2", 3, 5, miner="B")
        self.assertEqual(b.best(), b.hash("a5"))  # equal work: the earlier-seen tip stays
        b.add("b6", "b5", miner="B")
        self.assertEqual(b.best(), b.hash("b6"))
        for i in (3, 4, 5):
            self.assertEqual(b.chain.canonical_hash_at(BASE + i), b.hash(f"b{i}"))
        change = b.chain.classify_tip_change(b.hash("a5"), b.hash("b6"))
        self.assertEqual((change.fork_hash, change.fork_height), (b.hash("a2"), BASE + 2))
        self.assertEqual((change.disconnected, change.connected, change.is_reorg), (3, 4, True))
        self.assertEqual(change.disconnected_work, 3 * work_from_bits(NORMAL))
        self.assertEqual(change.connected_work, 4 * work_from_bits(NORMAL))
        b.line("b", "b6", 7, 8, miner="B")
        self.assertEqual([n.hash for n in b.chain.stale_blocks()], [b.hash(f"a{i}") for i in (3, 4, 5)])
        [event] = b.chain.fork_events()
        [loser] = event.losers
        self.assertEqual(
            (event.winner.hash, loser.block.hash, loser.tip_hash), (b.hash("b3"), b.hash("a3"), b.hash("a5"))
        )
        self.assertEqual((loser.length, loser.blocks, loser.miners, loser.winner_len), (3, 3, ("A",) * 3, 3))
        self.assertTrue(event.settled)

    def test_short_heavy_branch_beats_long_cheap_branch(self) -> None:
        """Depth in work: one normal block outweighs six post-reset blocks."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 3)
        b.line("l", "a3", 1, 6, bits=EASY, dt=3, miner="B")
        self.assertEqual(b.best(), b.hash("l6"))
        b.add("w1", "a3")
        self.assertEqual(b.best(), b.hash("w1"))
        change = b.chain.classify_tip_change(b.hash("l6"), b.hash("w1"))
        self.assertEqual((change.disconnected, change.connected), (6, 1))
        [event] = b.chain.fork_events()
        self.assertEqual((event.depth, event.depth_work), (6, 6 * work_from_bits(EASY)))
        self.assertEqual(event.losers[0].winner_len, 1)
        self.assertFalse(event.equal_work)
        self.assertFalse(event.settled)
        self.assertEqual(b.chain.stale_blocks(), [])  # none is settle_depth below the best tip yet

    def test_equal_work_tie_breaks_on_raw_hash(self) -> None:
        """Without first-seen times the greater raw hash wins, whatever the arrival order."""
        for order in ((0x10, 0xF0), (0xF0, 0x10)):
            b = Builder()
            b.genesis()
            b.add("p", "g")
            for index, last in enumerate(order):
                b.add(f"s{index}", "p", seen=None, block_hash=raw_hash(last))
            self.assertEqual(b.best(), raw_hash(0xF0))
            [event] = b.chain.fork_events()
            self.assertEqual(event.winner.hash, raw_hash(0xF0))
            self.assertTrue(event.winner_greater_raw_hash)
            self.assertIsNone(event.winner_first_seen)
            self.assertTrue(event.equal_work)
            self.assertFalse(event.settled)

    def test_first_seen_beats_raw_hash(self) -> None:
        """An earlier first-seen time wins a tie, including one learned after both arrived."""
        b = Builder()
        b.genesis()
        b.add("p", "g")
        b.add("lo", "p", seen=10.0, block_hash=raw_hash(0x10))
        b.add("hi", "p", seen=20.0, block_hash=raw_hash(0xF0))
        self.assertEqual(b.best(), raw_hash(0x10))
        [event] = b.chain.fork_events()
        self.assertEqual((event.winner_first_seen, event.winner_greater_raw_hash), (True, False))
        hi = b.chain.get(raw_hash(0xF0))
        b.chain.add(header(hi.hash, hi.prev_hash, hi.time), None, first_seen_at=5.0)
        self.assertEqual(b.best(), raw_hash(0xF0))
        self.assertEqual(b.chain.canonical_hash_at(BASE + 2), raw_hash(0xF0))

    def test_min_difficulty_sibling_loses_on_work(self) -> None:
        """A min-difficulty sibling loses to a normal one even when seen first with a greater hash."""
        b = Builder()
        b.genesis()
        b.add("p", "g")
        b.add("m", "p", dt=451, bits=MIN, seen=1.0, block_hash=raw_hash(0xF0), miner="B")
        b.add("n", "p", dt=20, seen=2.0, block_hash=raw_hash(0x10))
        self.assertEqual(b.best(), raw_hash(0x10))
        self.assertTrue(b.chain.get(raw_hash(0xF0)).is_min_diff)
        self.assertFalse(b.chain.get(raw_hash(0x10)).is_min_diff)
        [event] = b.chain.fork_events()
        [loser] = event.losers
        self.assertTrue(loser.block.is_min_diff)
        self.assertFalse(event.equal_work)
        self.assertEqual((event.winner_first_seen, event.winner_greater_raw_hash), (False, False))

    def test_detached_parent_arrives_later(self) -> None:
        """A block with a missing parent waits detached and attaches (becoming best) when it arrives."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 5)
        c7 = b.add("c7", "c6", height=BASE + 7, at=T0 + 500)
        self.assertIsNone(c7.cumwork)
        self.assertEqual(b.best(), b.hash("a5"))
        self.assertEqual(b.chain.missing_parents(), [(b.hash("c6"), BASE + 6)])
        self.assertEqual(b.chain.relation(c7.hash).kind, "unknown")
        b.add("c6", "a5")
        self.assertEqual(b.best(), c7.hash)
        self.assertIsNotNone(c7.cumwork)
        self.assertEqual(b.chain.canonical_hash_at(BASE + 7), c7.hash)
        self.assertEqual(b.chain.missing_parents(), [])

    def test_detached_height_follows_parent(self) -> None:
        """A wrong height hint on a detached block is corrected when its parent attaches it."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 2)
        c = b.add("c4", "c3", height=BASE + 40)
        b.add("c3", "a2")
        self.assertEqual(c.height, BASE + 4)
        self.assertEqual(b.chain.nodes_at(BASE + 40), [])
        self.assertEqual(b.chain.canonical_hash_at(BASE + 4), c.hash)

    def test_heightless_headers_wait_for_parent(self) -> None:
        """Headers with no height and no parent are held, then placed when the ancestry arrives."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 3)
        self.assertIsNone(b.add("h6", "h5"))
        self.assertIsNone(b.add("h5", "h4"))
        self.assertNotIn(b.hash("h5"), b.chain)
        placed = b.add("h4", "a3")
        self.assertEqual(placed.height, BASE + 4)
        self.assertEqual(b.best(), b.hash("h6"))
        self.assertEqual(b.chain.best_tip().height, BASE + 6)

    def test_walk_back_extends_down(self) -> None:
        """Adding the tip first and then its ancestors grows the canonical chain downward."""
        b = Builder()
        names = [f"t{i}" for i in range(11)]
        for i in range(10, 0, -1):
            b.add(names[i], names[i - 1], height=BASE + i, at=T0 + 20 * i)
        self.assertEqual(b.best(), b.hash("t10"))
        self.assertEqual(b.chain.canonical_hash_at(BASE + 1), b.hash("t1"))
        self.assertEqual(b.chain.canonical_hash_at(BASE + 10), b.hash("t10"))
        self.assertEqual(len(b.chain.canonical_nodes()), 10)
        b.add("s5", "t4", at=T0 + 81)
        self.assertEqual(b.best(), b.hash("t10"))
        self.assertEqual([n.hash for n in b.chain.stale_blocks()], [b.hash("s5")])
        self.assertTrue(b.chain.is_ancestor(b.hash("t1"), b.hash("t10")))

    def test_out_of_order_fragments_attach(self) -> None:
        """Backfill-style fragments below the window join once the missing link arrives."""
        b = Builder()
        names = [f"f{i}" for i in range(21)]
        for i in range(10, 21):
            b.add(names[i], names[i - 1], height=BASE + i, at=T0 + 20 * i)
        for i in (5, 6, 7, 8):
            node = b.add(names[i], names[i - 1], height=BASE + i, at=T0 + 20 * i)
            self.assertIsNone(node.cumwork)
        self.assertEqual(b.chain.missing_parents(), [(b.hash("f4"), BASE + 4)])
        b.add("f9", "f8", height=BASE + 9, at=T0 + 180)
        self.assertEqual(b.best(), b.hash("f20"))
        self.assertEqual(b.chain.canonical_hash_at(BASE + 5), b.hash("f5"))
        self.assertEqual(len(b.chain.canonical_nodes()), 16)
        self.assertTrue(all(n.cumwork is not None for n in b.chain.canonical_nodes()))
        self.assertEqual(b.chain.missing_parents(), [])


class TipChangeAndRelationTests(unittest.TestCase):
    """classify_tip_change and relation over a small forked tree."""

    def setUp(self) -> None:
        """Build g, a1..a6 with a side branch x4, x5 on a3."""
        self.b = Builder()
        self.b.genesis()
        self.b.line("a", "g", 1, 6)
        self.b.line("x", "a3", 4, 5, miner="B")
        self.chain = self.b.chain
        self.h = self.b.hash

    def test_tip_change_kinds(self) -> None:
        """Extension, rewind, no change, first observation and unknown tips."""
        extend = self.chain.classify_tip_change(self.h("a3"), self.h("a5"))
        self.assertEqual(
            (extend.fork_hash, extend.connected, extend.disconnected, extend.is_reorg), (self.h("a3"), 2, 0, False)
        )
        self.assertEqual((extend.connected_work, extend.disconnected_work), (2 * work_from_bits(NORMAL), 0))
        # A move to an ancestor (a peer restarting at its finalized height) is a rewind, not a reorg.
        rewind = self.chain.classify_tip_change(self.h("a5"), self.h("a3"))
        self.assertEqual((rewind.fork_hash, rewind.disconnected, rewind.connected), (self.h("a3"), 2, 0))
        self.assertFalse(rewind.is_reorg)
        same = self.chain.classify_tip_change(self.h("a5"), self.h("a5"))
        self.assertEqual((same.disconnected, same.connected, same.is_reorg), (0, 0, False))
        first = self.chain.classify_tip_change(None, self.h("a5"))
        self.assertEqual(
            (first.old_hash, first.fork_hash, first.new_height, first.is_reorg), (None, None, BASE + 5, False)
        )
        unknown = self.chain.classify_tip_change(self.h("a5"), "ee" * 32)
        self.assertEqual((unknown.old_height, unknown.new_height, unknown.fork_hash), (BASE + 5, None, None))
        fork = self.chain.classify_tip_change(self.h("a6"), self.h("x5"))
        self.assertEqual((fork.fork_height, fork.disconnected, fork.connected, fork.is_reorg), (BASE + 3, 3, 2, True))

    def test_tip_change_row_fits_store(self) -> None:
        """as_row() round-trips through Store.record_tip_change."""
        change = self.chain.classify_tip_change(self.h("a6"), self.h("x5"))
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "m.sqlite3") as store:
            row_id = store.record_tip_change(source="rpc:a", at=1.5, **change.as_row())
            store.commit_if_due(force=True)
            stored = store.reader().execute("SELECT * FROM tip_changes WHERE id = ?", (row_id,)).fetchone()
            self.assertEqual(
                (stored["fork_hash"], stored["disconnected"], stored["connected"], stored["is_reorg"]),
                (self.h("a3"), 3, 2, 1),
            )
            self.assertEqual(stored["connected_work"], 2 * work_from_bits(NORMAL))

    def test_relation_kinds(self) -> None:
        """same / behind / fork / ahead (against a non-default reference) / unknown."""
        same = self.chain.relation(self.h("a6"))
        self.assertEqual((same.kind, same.n, same.depth_ours, same.depth_theirs), ("same", 0, 0, 0))
        behind = self.chain.relation(self.h("a2"))
        self.assertEqual(
            (behind.kind, behind.n, behind.fork_height, behind.tip_height), ("behind", 4, BASE + 2, BASE + 2)
        )
        fork = self.chain.relation(self.h("x5"))
        self.assertEqual(
            (fork.kind, fork.n, fork.fork_hash, fork.depth_ours, fork.depth_theirs), ("fork", 2, self.h("a3"), 3, 2)
        )
        ahead = self.chain.relation(self.h("a6"), ref_hash=self.h("a2"))
        self.assertEqual((ahead.kind, ahead.n), ("ahead", 4))
        unknown = self.chain.relation("ee" * 32)
        self.assertEqual((unknown.kind, unknown.n, unknown.tip_height), ("unknown", None, None))
        self.assertEqual(Chain(TESTNET).relation(self.h("a6")).kind, "unknown")
        self.assertIsNone(Chain(TESTNET).best_tip())


def build_sawtooth(b: Builder) -> int:
    """Build 40 slow blocks, a 451 s reset, 30 blocks 5 s apart, then 20 blocks 100 s apart; return the reset height."""
    b.genesis()
    b.line("p", "g", 1, 40, dt=75)
    b.add("r", "p40", dt=451, bits=MIN)
    b.line("f", "r", 1, 30, dt=5, bits=EASY)
    b.line("s", "f30", 1, 20, dt=100, bits=EASY)
    return BASE + 41


class PhaseTests(unittest.TestCase):
    """phase_at and resets around synthetic resets, including reorg invalidation."""

    def test_phase_around_reset(self) -> None:
        """k, d_ratio and the fast flag follow the reset; fast ends when the 17-block mean reaches 37.5 s."""
        b = Builder()
        r = build_sawtooth(b)
        chain = b.chain
        before = chain.phase_at(r - 1)
        self.assertEqual((before.k, before.reset_height, before.fast, before.d_ratio), (None, None, None, None))
        self.assertAlmostEqual(before.difficulty, difficulty_from_bits(NORMAL, TESTNET))
        reset = chain.phase_at(r)
        self.assertEqual((reset.k, reset.reset_height, reset.min_diff, reset.fast), (0, r, True, True))
        self.assertEqual(reset.difficulty, 1.0)
        self.assertAlmostEqual(reset.d_pre, difficulty_from_bits(NORMAL, TESTNET))
        after = chain.phase_at(r + 1)
        expected_ratio = difficulty_from_bits(EASY, TESTNET) / difficulty_from_bits(NORMAL, TESTNET)
        self.assertEqual((after.k, after.min_diff), (1, False))
        self.assertAlmostEqual(after.d_ratio, expected_ratio)
        # 6 blocks at 100 s plus 11 at 5 s = 655 s >= 17 * 37.5 s, first reached at r + 36.
        self.assertTrue(chain.phase_at(r + 35).fast)
        self.assertFalse(chain.phase_at(r + 36).fast)
        self.assertFalse(chain.phase_at(r + 50).fast)
        self.assertIs(chain.phase_at(r + 36), chain.phase_at(r + 36))
        outside = chain.phase_at(r + 51)
        self.assertEqual((outside.k, outside.difficulty, outside.min_diff), (None, None, False))

    def test_reorg_removing_reset_invalidates_phases(self) -> None:
        """A heavier branch without the reset replaces cached phases and the reset list."""
        b = Builder()
        r = build_sawtooth(b)
        self.assertEqual(b.chain.phase_at(r + 5).k, 5)
        self.assertEqual(len(b.chain.resets()), 1)
        b.add("q1", "p40", dt=75)
        self.assertEqual(b.best(), b.hash("q1"))
        self.assertEqual(b.chain.resets(), [])
        self.assertIsNone(b.chain.phase_at(r).k)
        self.assertFalse(b.chain.phase_at(r).min_diff)
        self.assertIsNone(b.chain.phase_at(r + 5).difficulty)

    def test_reorg_rolls_back_fast_end(self) -> None:
        """If the blocks that ended the fast phase are reorged out, the phase is fast again."""
        b = Builder()
        r = build_sawtooth(b)
        self.assertFalse(b.chain.phase_at(r + 40).fast)
        self.assertEqual(b.chain.resets()[0].fast_blocks, 36)
        b.line("z", "s2", 1, 19, dt=5, bits=EASY)
        self.assertEqual(b.best(), b.hash("z19"))
        self.assertTrue(b.chain.phase_at(r + 36).fast)
        self.assertTrue(b.chain.phase_at(r + 45).fast)
        self.assertIsNone(b.chain.resets()[0].fast_blocks)

    def test_resets_report_gap_next_dt_and_forward_dating(self) -> None:
        """Each reset lists its gap, the negative next interval, forward-dating and cycle lengths."""
        b = Builder()
        b.genesis()
        b.line("p", "g", 1, 30, dt=75)
        reset_time = b.times["p30"] + 451
        b.times["r1"] = reset_time
        hdr = header(b.hash("r1"), b.hash("p30"), reset_time, MIN)
        b.chain.add(hdr, None, block=coinbase_block(hdr, BASE + 31), first_seen_at=reset_time - 140.0)
        b.add("n1", "r1", dt=-120, bits=EASY)
        b.line("n", "n1", 2, 25, dt=3, bits=EASY)
        b.add("r2", "n25", dt=451, bits=MIN)
        first, second = b.chain.resets()
        self.assertEqual((first.height, first.hash), (BASE + 31, b.hash("r1")))
        self.assertEqual((first.gap, first.next_dt, first.forward_dating), (451, -120, 140.0))
        self.assertEqual((first.miner, first.template), ("zkcodexcoder", "zakura"))
        self.assertAlmostEqual(first.d_pre, difficulty_from_bits(NORMAL, TESTNET))
        self.assertEqual((first.cycle_blocks, first.fast_blocks), (26, None))
        self.assertEqual((second.gap, second.next_dt, second.cycle_blocks), (451, None, None))
        self.assertAlmostEqual(second.d_pre, difficulty_from_bits(EASY, TESTNET))
        self.assertEqual([r.height for r in b.chain.resets(since_height=BASE + 32)], [second.height])

    def test_nu7_fast_phase_uses_the_nu7_window_and_spacing(self) -> None:
        """From NU7 the fast phase ends when the 102-block mean reaches 12.5 s, not the 17-block mean 37.5 s."""
        fast_blocks = []
        for nu7_height in (None, BASE):
            b = Builder(Chain(dataclasses.replace(TESTNET, nu7_height=nu7_height)))
            b.genesis()
            b.line("p", "g", 1, 110, dt=25)
            b.add("r", "p110", dt=451, bits=MIN)
            b.line("f", "r", 1, 120, dt=5, bits=EASY)
            b.line("s", "f120", 1, 20, dt=100, bits=EASY)
            fast_blocks.append(b.chain.resets()[0].fast_blocks)
        # m blocks at 100 s after the burst: 85 + 95m >= 17 * 37.5 at m = 6; 510 + 95m >= 102 * 12.5 at m = 9.
        self.assertEqual(fast_blocks, [126, 129])

    def test_fast_phase_open_at_nu7_waits_for_the_nu7_window(self) -> None:
        """A cycle still fast at activation stays fast until the 102-block window has left the reset."""
        b = Builder(Chain(dataclasses.replace(TESTNET, nu7_height=BASE + 51)))
        b.genesis()
        b.line("p", "g", 1, 40, dt=75)
        b.add("r", "p40", dt=451, bits=MIN)
        b.line("f", "r", 1, 9, dt=5, bits=EASY)
        b.line("s", "f9", 1, 100, dt=100, bits=EASY)
        r = BASE + 41
        self.assertTrue(b.chain.phase_at(r + 17).fast)  # the pre-NU7 rules would have ended it here
        self.assertTrue(b.chain.phase_at(r + 101).fast)
        self.assertFalse(b.chain.phase_at(r + 102).fast)
        self.assertEqual(b.chain.resets()[0].fast_blocks, 102)

    def test_mainnet_has_no_resets(self) -> None:
        """Pow-limit bits are not a reset on a network without the minimum-difficulty rule."""
        b = Builder(Chain(MAINNET))
        b.genesis(bits=MAINNET.pow_limit_bits)
        b.line("a", "g", 1, 3, bits=MAINNET.pow_limit_bits, dt=500)
        self.assertFalse(b.chain.best_tip().is_min_diff)
        self.assertEqual(b.chain.resets(), [])
        self.assertIsNone(b.chain.phase_at(BASE + 3).k)


class FixtureRegressionTests(unittest.TestCase):
    """Load the trimmed live Testnet headers and compare with the historical analysis (analyze.py)."""

    @classmethod
    def setUpClass(cls) -> None:
        """Load 800 headers as a linear chain with synthetic hashes."""
        data = json.loads((FIXTURES / "testnet-headers-4413473-4414272.json").read_text())
        cls.chain = Chain(TESTNET)
        for height in sorted(map(int, data)):
            bits, block_time = data[str(height)]
            cls.chain.add(header(f"{height:064x}", f"{height - 1:064x}", block_time, bits), height)

    def test_resets_match_analysis(self) -> None:
        """Both resets in range, with the 451 s gap and the negative next interval."""
        resets = self.chain.resets()
        self.assertEqual([r.height for r in resets], [4413534, 4414001])
        self.assertEqual([(r.gap, r.next_dt) for r in resets], [(451, -145), (451, -142)])
        # cycles.csv: the 4413534 cycle is 467 blocks with a 421-block fast phase; 4414001 is still open.
        self.assertEqual((resets[0].cycle_blocks, resets[0].fast_blocks), (467, 421))
        self.assertIsNone(resets[1].cycle_blocks)

    def test_phases_match_analysis(self) -> None:
        """k, D_pre and D/D_pre equal blocks_enriched.csv at sampled heights."""
        expected = {
            4413535: (1, 38602.6438254281, 0.00044792756753281847),
            4413600: (66, 38602.6438254281, 0.0007083599587277102),
            4414000: (466, 38602.6438254281, 0.9506286893378604),
            4414002: (1, 36696.78070474297, 0.0005304233464372688),
            4414100: (99, 36696.78070474297, 0.001438530589996641),
        }
        for height, (k, d_pre, ratio) in expected.items():
            phase = self.chain.phase_at(height)
            self.assertEqual(phase.k, k, height)
            self.assertAlmostEqual(phase.d_pre, d_pre, places=6)
            self.assertAlmostEqual(phase.d_ratio / ratio, 1.0, places=12)
        self.assertTrue(self.chain.phase_at(4413954).fast)
        self.assertFalse(self.chain.phase_at(4413955).fast)
        self.assertIsNone(self.chain.phase_at(4413533).k)
        self.assertTrue(self.chain.phase_at(4414001).min_diff)


class PruneTests(unittest.TestCase):
    """prune_below keeps the window consistent."""

    def test_prune_keeps_phase_and_anchors(self) -> None:
        """Phases above the floor survive pruning the reset; old blocks are ignored; floor siblings attach."""
        pruned, reference = Builder(), Builder()
        r = build_sawtooth(pruned)
        build_sawtooth(reference)
        floor = r + 10
        self.assertEqual(pruned.chain.prune_below(floor), floor - BASE)
        self.assertEqual(len(pruned.chain), 92 - (floor - BASE))
        self.assertIsNone(pruned.chain.canonical_hash_at(floor - 1))
        self.assertEqual(pruned.chain.canonical_hash_at(floor), pruned.hash("f10"))
        for height in (floor, r + 30, r + 36, r + 50):
            self.assertEqual(pruned.chain.phase_at(height), reference.chain.phase_at(height))
        self.assertEqual(pruned.chain.resets(), [])
        self.assertEqual(pruned.chain.prune_below(floor), 0)
        self.assertIsNone(pruned.add("old", "f3", height=r + 4))
        sibling = pruned.add("f10b", "f9", dt=6, bits=EASY)
        self.assertIsNotNone(sibling.cumwork)
        pruned.line("s", "s20", 21, 24, dt=100, bits=EASY)
        self.assertIn(sibling.hash, [n.hash for n in pruned.chain.stale_blocks()])
        pruned.chain.prune_below(10**9)  # clamped: the best tip always survives
        self.assertEqual(len(pruned.chain), 1)
        self.assertEqual(pruned.chain.best_tip().hash, pruned.hash("s24"))
        self.assertEqual(pruned.add("s25", "s24").cumwork, pruned.chain.best_tip().cumwork)


class BootstrapTests(unittest.TestCase):
    """Loading from Store rows."""

    def test_from_store_round_trip(self) -> None:
        """Rows written by Store.upsert_block rebuild the same tree with attribution."""
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "m.sqlite3") as store:
            prev = bhash("before")
            for i in range(8):
                hdr = header(bhash(f"c{i}"), prev, T0 + 20 * i)
                block = coinbase_block(hdr, BASE + i) if i == 3 else None
                miner = "zkcodexcoder" if block is not None else None
                store.upsert_block(
                    block, hdr, BASE + i, miner=miner, is_min_diff=False, seen_at=100.0 + i, seen_source="rpc:a"
                )
                prev = hdr.hash
            stale = header(bhash("stale"), bhash("c2"), T0 + 61)
            store.upsert_block(None, stale, BASE + 3, miner="B", is_min_diff=False, seen_at=104.0, seen_source="p2p:x")
            chain = Chain.from_store(TESTNET, store)
            self.assertEqual(len(chain), 9)
            self.assertEqual(chain.best_tip().hash, bhash("c7"))
            self.assertEqual([n.hash for n in chain.stale_blocks()], [bhash("stale")])
            node = chain.get(bhash("c3"))
            self.assertEqual(
                (node.body, node.miner, node.template, node.tag, node.first_seen_at),
                (True, "zkcodexcoder", "zakura", "zkcodexcoder", 103.0),
            )
            self.assertTrue(node.body_trusted)
            untrusted = header(bhash("u"), bhash("c7"), T0 + 200)
            store.upsert_block(coinbase_block(untrusted, BASE + 8), untrusted, BASE + 8, miner="x", is_min_diff=False,
                               seen_at=110.0, seen_source="p2p:x", trusted=False)
            self.assertFalse(Chain.from_store(TESTNET, store).get(bhash("u")).body_trusted)
            self.assertEqual(Chain.from_store(TESTNET, store, min_height=BASE + 5).best_tip().hash, bhash("u"))
            self.assertEqual(len(Chain.from_store(TESTNET, store, min_height=BASE + 5)), 4)

    def test_load_anchors_on_the_group_that_reaches_highest(self) -> None:
        """A stale block crossing the window bottom cannot capture the anchor, even if loaded first."""
        rows = [row(bhash("stale"), bhash("stale-parent"), BASE, first_seen_at=1.0)]
        prev = bhash("canon-parent")
        for i in range(6):
            rows.append(row(bhash(f"c{i}"), prev, BASE + i, first_seen_at=2.0 + i))
            prev = bhash(f"c{i}")
        chain = Chain(TESTNET)
        self.assertEqual(chain.load(rows), 7)
        self.assertEqual(chain.best_tip().hash, bhash("c5"))
        self.assertEqual(chain.canonical_hash_at(BASE), bhash("c0"))
        self.assertIsNone(chain.get(bhash("stale")).cumwork)
        self.assertEqual(chain.stale_blocks(), [])

    def test_load_anchors_on_the_persisted_best_tip(self) -> None:
        """A detached block above the tip reaches highest, but the persisted best tip keeps the anchor."""
        rows = []
        prev = bhash("canon-parent")
        for i in range(6):
            rows.append(row(bhash(f"c{i}"), prev, BASE + i, first_seen_at=2.0 + i))
            prev = bhash(f"c{i}")
        rows.append(row(bhash("forged"), bhash("random"), BASE + 500, first_seen_at=9.0))
        hijacked = Chain(TESTNET)
        hijacked.load(rows)
        self.assertEqual(hijacked.best_tip().hash, bhash("forged"))  # reach alone is fooled
        chain = Chain(TESTNET)
        chain.load(rows, prefer=bhash("c3"))
        self.assertEqual(chain.best_tip().hash, bhash("c5"))
        self.assertIsNone(chain.get(bhash("forged")).cumwork)
        cycle = Chain(TESTNET)  # a preferred hash inside a crafted cycle falls back to reach
        cycle.load([row(bhash("a"), bhash("b"), BASE), row(bhash("b"), bhash("a"), BASE + 1), *rows[:6]],
                   prefer=bhash("a"))
        self.assertEqual(cycle.best_tip().hash, bhash("c5"))
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / "m.sqlite3") as store:
            for entry in rows:
                hdr = header(entry["hash"], entry["prev_hash"], entry["time"])
                store.upsert_block(None, hdr, entry["height"], miner=None, is_min_diff=False,
                                   seen_at=entry["first_seen_at"], seen_source="test")
            self.assertEqual(Chain.from_store(TESTNET, store).best_tip().hash, bhash("forged"))
            store.set_meta("best_hash", bhash("c5"))
            self.assertEqual(Chain.from_store(TESTNET, store).best_tip().hash, bhash("c5"))

    def test_load_skips_bad_rows_and_survives_cycles(self) -> None:
        """Malformed rows are skipped and a crafted hash cycle does not hang the walks."""
        a, b_, c = bhash("a"), bhash("b"), bhash("c")
        rows = [
            row(c, bhash("parent"), BASE),
            row(bhash("self"), bhash("self"), BASE + 1),
            row(bhash("bad"), c, BASE + 1, bits="nope"),
            row(a, b_, BASE + 1),
            row(b_, a, BASE + 2),
            {"hash": bhash("short")},
        ]
        chain = Chain(TESTNET)
        chain.load(rows)
        self.assertEqual(chain.best_tip().hash, c)
        self.assertFalse(chain.is_ancestor(a, b_) and chain.is_ancestor(b_, a))
        self.assertIsNone(chain.fork_point(a, c))
        chain.relation(a)
        chain.fork_events()

    def test_thirty_thousand_blocks_load_quickly(self) -> None:
        """A 30k-block window with forks loads from rows (and via add) in well under 3 s."""
        rows, headers = [], []
        prev = bhash("root-parent")
        for i in range(30_000):
            block_hash = f"{i + 1:064x}"
            bits = MIN if i % 470 == 0 else EASY if i % 470 < 200 else NORMAL
            rows.append(row(block_hash, prev, BASE + i, time=T0 + 19 * i, bits=bits, first_seen_at=float(i)))
            headers.append((header(block_hash, prev, T0 + 19 * i, bits), BASE + i))
            if i % 100 == 50:
                side = row(bhash(f"side{i}"), prev, BASE + i, time=T0 + 19 * i + 1, bits=bits, first_seen_at=i + 0.5)
                rows.append(side)
            prev = block_hash
        started = time.perf_counter()
        chain = Chain(TESTNET)
        chain.load(rows)
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 3.0)
        self.assertEqual(chain.best_tip().height, BASE + 29_999)
        self.assertEqual(len(chain.fork_events()), 300)
        self.assertEqual(len(chain.resets()), 64)
        started = time.perf_counter()
        for height in range(BASE, BASE + 30_000):
            chain.phase_at(height)
        self.assertLess(time.perf_counter() - started, 3.0)
        started = time.perf_counter()
        incremental = Chain(TESTNET)
        for hdr, height in headers:
            incremental.add(hdr, height)
        self.assertLess(time.perf_counter() - started, 3.0)
        self.assertEqual(incremental.best_tip().hash, chain.best_tip().hash)


class BoundsTests(unittest.TestCase):
    """Caps on data that cannot be attached yet."""

    def test_detached_groups_are_evicted_oldest_first(self) -> None:
        """Past MAX_DETACHED the oldest detached groups go; the main chain is untouched."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 3)
        with mock.patch.object(chain_mod, "MAX_DETACHED", 3):
            for i in range(5):
                b.add(f"d{i}", f"missing{i}", height=BASE + 50 + i)
            self.assertNotIn(b.hash("d0"), b.chain)
            self.assertNotIn(b.hash("d1"), b.chain)
            self.assertIn(b.hash("d4"), b.chain)
            self.assertEqual(len(b.chain.missing_parents()), 3)
            self.assertIsNotNone(b.add("d4b", "d4"))
            self.assertNotIn(b.hash("d2"), b.chain)
            self.assertEqual(b.best(), b.hash("a3"))
        solo = Builder()
        solo.genesis()
        with mock.patch.object(chain_mod, "MAX_DETACHED", 1):
            solo.add("e1", "missing", height=BASE + 9)
            # The only detached group is the one growing past the cap, so it evicts itself.
            self.assertIsNone(solo.add("e2", "e1"))
            self.assertNotIn(solo.hash("e1"), solo.chain)
            self.assertEqual(solo.chain.missing_parents(), [])

    def test_detached_blocks_far_above_the_tip_are_refused(self) -> None:
        """A detached block, or a child of one, more than MAX_DETACHED_AHEAD above the best tip is refused."""
        b = Builder()
        b.genesis()
        b.line("a", "g", 1, 5)
        limit = BASE + 5 + chain_mod.MAX_DETACHED_AHEAD
        forged = header(bhash("forged"), bhash("random"), T0)
        self.assertIsNone(b.chain.add(forged, None, block=coinbase_block(forged, 2_000_000_000)))
        self.assertIsNone(b.add("far", "nowhere", height=limit + 1))
        self.assertNotIn(b.hash("far"), b.chain)
        self.assertIsNotNone(b.add("near", "missing", height=limit))
        self.assertIsNone(b.add("near2", "near"))
        self.assertEqual(b.chain.missing_parents(), [(b.hash("missing"), limit - 1)])
        self.assertEqual(b.best(), b.hash("a5"))
        # An RPC walk-back after downtime (tip first, then down) still catches up in steps.
        walk = Builder()
        walk.genesis()
        top = 1_500
        hashes = {i: bhash(f"w{i}") for i in range(1, top + 1)}
        hashes[0] = walk.hash("g")

        def walk_back() -> None:
            """Ingest w<top>..w1 top first, as `_walk_back` does."""
            for i in range(top, 0, -1):
                walk.chain.add(header(hashes[i], hashes[i - 1], T0 + i), BASE + i)

        walk_back()
        self.assertEqual(walk.chain.best_tip().height, BASE + chain_mod.MAX_DETACHED_AHEAD)
        walk_back()
        self.assertEqual(walk.chain.best_tip().height, BASE + top)

    def test_pending_headers_are_capped(self) -> None:
        """Past MAX_PENDING the oldest heightless headers are dropped."""
        b = Builder()
        b.genesis()
        with mock.patch.object(chain_mod, "MAX_PENDING", 2):
            for name, parent in (("h2", "h1"), ("j2", "j1"), ("k2", "k1")):
                self.assertIsNone(b.add(name, parent))
            b.add("h1", "g")
            b.add("k1", "g")
        self.assertNotIn(b.hash("h2"), b.chain)
        self.assertIn(b.hash("k2"), b.chain)


if __name__ == "__main__":
    unittest.main()
