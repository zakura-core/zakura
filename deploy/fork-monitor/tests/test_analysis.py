"""Tests for the dashboard/API summaries: synthetic chains and stores with hand-computed answers."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from zakura_fork_monitor import analysis
from zakura_fork_monitor.chain import Chain
from zakura_fork_monitor.config import parse_config
from zakura_fork_monitor.consensus import MAINNET, TESTNET, Block, BlockHeader, Coinbase, difficulty_from_bits
from zakura_fork_monitor.store import Store

BASE = 1_000_000
T0 = 1_790_000_000  # 2026-09-21 14:13:20 UTC
NORMAL = 0x1E0D94E6  # difficulty ~38.6k, a late-cycle testnet block
EASY = 0x1F76710D  # difficulty ~17, the first blocks after a reset
MIN = TESTNET.pow_limit_bits


def bhash(name: str, last_byte: int | None = None) -> str:
    """Derive a display hash from a name; `last_byte` fixes the most significant raw-hash byte."""
    digest = hashlib.sha256(name.encode()).hexdigest()
    return digest if last_byte is None else digest[:62] + f"{last_byte:02x}"


def header(block_hash: str, prev_hash: str, time: int, bits: int) -> BlockHeader:
    """Build a synthetic header (the raw bytes are not needed)."""
    return BlockHeader(hash=block_hash, prev_hash=prev_hash, version=4, merkle_root="00" * 32, time=time, bits=bits,
                       nonce="00" * 32, raw=b"")


class Tree:
    """Adds named synthetic blocks to a Chain; each block is first seen 1 s after its header time."""

    def __init__(self, params=TESTNET, settle_depth: int = 3) -> None:
        """Start an empty chain."""
        self.chain = Chain(params, settle_depth)
        self.hashes: dict[str, str] = {}
        self.times: dict[str, int] = {}

    def hash(self, name: str) -> str:
        """Return the hash of block `name`."""
        return self.hashes.setdefault(name, bhash(name))

    def node(self, name: str):
        """Return the chain node of block `name`."""
        return self.chain.get(self.hash(name))

    def add(self, name, parent, *, dt=20, bits=NORMAL, miner="A", seen=None, height=None, last_byte=None,
            template=None):
        """Add block `name` on `parent`; `template` attaches a parsed coinbase with that marker."""
        if last_byte is not None:
            self.hashes[name] = bhash(name, last_byte)
        block_time = self.times.get(parent, T0) + dt
        self.times[name] = block_time
        hdr = header(self.hash(name), self.hash(parent), block_time, bits)
        block = None
        if template is not None:
            coinbase = Coinbase(height=height, script_sig=b"\x03abc", template=template, tag=miner, extranonce="",
                                payouts=(), tx_version=5)
            block = Block(header=hdr, size=2_000, tx_count=1, coinbase=coinbase)
        node = self.chain.add(hdr, height, block=block, miner=miner,
                              first_seen_at=block_time + 1.0 if seen is None else seen)
        assert node is not None, name
        return node

    def line(self, prefix, parent, first, last, **kwargs) -> None:
        """Add blocks prefix<first>..prefix<last> in a line on `parent`."""
        for index in range(first, last + 1):
            self.add(f"{prefix}{index}", parent, **kwargs)
            parent = f"{prefix}{index}"


def sawtooth_tree(params=TESTNET) -> Tree:
    """A chain with one reset, a fast and a slow phase, and five settled orphans.

    g..p5 at B..B+5 (before any reset), reset r at B+6 (451 s gap, miner B),
    f1..f30 at B+7..B+36 (2 s apart), s1..s30 at B+37..B+66 (80 s apart).
    The fast phase ends at s8 (B+44). Stale: x1 (B+7, self), x20 (B+26, race),
    l1-l2 (B+31..32, race), y (B+46, slow race) and the unsettled z (B+65).
    """
    tree = Tree(params)
    tree.add("g", "pre", dt=0, height=BASE)
    tree.line("p", "g", 1, 5)
    tree.add("r", "p5", dt=451, bits=MIN, miner="B", template="zakura", height=BASE + 6, seen=T0 + 551 - 140.0)
    tree.line("f", "r", 1, 30, dt=2, bits=EASY)
    tree.line("s", "f30", 1, 30, dt=80)
    tree.add("x1", "r", dt=2, bits=EASY)
    tree.add("x20", "f19", dt=2, bits=EASY, miner="B")
    tree.add("l1", "f24", dt=2, bits=EASY, miner="C")
    tree.add("l2", "l1", dt=2, bits=EASY, miner="C")
    tree.add("y", "s9", dt=80, miner="B")
    tree.add("z", "s28", dt=80, miner="B")
    return tree


def rows_by_key(rows):
    """Index bucket rows by key."""
    return {row["key"]: row for row in rows}


def assert_json(test: unittest.TestCase, value) -> None:
    """Assert that `value` serializes as strict JSON (no NaN/inf, only JSON types)."""
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as err:  # pragma: no cover - only on failure
        test.fail(f"not JSON-able: {err}")


class StoreCase(unittest.TestCase):
    """Provides a temporary Store and a helper to read it through a reader connection."""

    def setUp(self) -> None:
        """Open a fresh store."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = Store(Path(tmp.name) / "monitor.sqlite3")
        self.addCleanup(self.store.close)

    def conn(self):
        """Commit pending writes and return this thread's reader connection."""
        self.store.commit_if_due(force=True)
        return self.store.reader()

    def add_block(self, block_hash: str, height: int, seen: float, prev: str = "00" * 32) -> None:
        """Store a header-only block row."""
        self.store.upsert_block(None, header(block_hash, prev, T0 + height, NORMAL), height, miner=None,
                                is_min_diff=False, seen_at=seen, seen_source="test")


class BucketTests(unittest.TestCase):
    """Bucket boundaries and labels."""

    def test_k_and_ratio_buckets(self) -> None:
        """Boundaries match the historical analysis: k inclusive ranges, ratio lower bound inclusive."""
        self.assertEqual([analysis._k_bucket(k) for k in (0, 1, 17, 18, 400, 401, None)],
                         ["0", "1-17", "1-17", "18-50", "301-400", ">=401", "unknown"])
        self.assertEqual([analysis._k_fine(k) for k in (0, 1, 2, 5, 6, 17, 30, 31)],
                         ["0", "1", "2", "3-5", "6-10", "11-17", "18-30", None])
        self.assertEqual([analysis._ratio_bucket(r) for r in (0.0, 0.000999, 0.001, 0.5, 0.99, 1.0, 7.0, None)],
                         ["<0.001", "<0.001", "0.001-0.01", "0.5-1", "0.5-1", ">=1", ">=1", "unknown"])

    def test_group_keys(self) -> None:
        """Groups are impl plus major.minor; unknown versions leave just the impl."""
        self.assertEqual(analysis._group_key("zebra", "6.4.2"), "zebra 6.4")
        self.assertEqual(analysis._group_key("Zakura", "1.5.0-rc0"), "zakura 1.5")
        self.assertEqual(analysis._group_key("zcashd", None), "zcashd")
        self.assertEqual(analysis._group_key(None, "junk"), "unknown")


class OrphanStatsTests(unittest.TestCase):
    """orphan_stats on the sawtooth tree."""

    def setUp(self) -> None:
        """Build the tree and compute the stats."""
        self.tree = sawtooth_tree()
        self.now = T0 + 3_100
        self.stats = analysis.orphan_stats(self.tree.chain, now=self.now)

    def test_window_and_totals(self) -> None:
        """Only settled heights count: 64 canonical blocks, 5 orphans, 4 forks (z is unsettled)."""
        stats = self.stats
        self.assertEqual((stats["from_height"], stats["to_height"]), (BASE, BASE + 63))
        self.assertEqual(stats["totals"], {"blocks": 64, "orphans": 5, "rate": round(5 / 64, 6), "forks": 4,
                                           "self": 1, "race": 4, "unknown": 0, "no_body": 0, "siblings": 4,
                                           "unobserved": 0})
        assert_json(self, stats)

    def test_by_phase(self) -> None:
        """The fast phase ends when the trailing 17-block mean interval reaches 37.5 s (at s8)."""
        rows = rows_by_key(self.stats["by_phase"])
        self.assertEqual([r["key"] for r in self.stats["by_phase"]], ["fast", "slow", "steady", "unknown"])
        self.assertEqual((rows["fast"]["blocks"], rows["fast"]["orphans"], rows["fast"]["forks"]), (38, 4, 3))
        self.assertEqual((rows["slow"]["blocks"], rows["slow"]["orphans"], rows["slow"]["forks"]), (20, 1, 1))
        self.assertEqual((rows["unknown"]["blocks"], rows["unknown"]["orphans"]), (6, 0))
        self.assertAlmostEqual(rows["fast"]["rate"], 4 / 38, places=6)
        self.assertAlmostEqual(rows["fast"]["share"], 0.8)
        self.assertAlmostEqual(rows["slow"]["forks_per_1000"], 50.0)

    def test_by_k_and_ratio(self) -> None:
        """Blocks and orphans land in the historical analysis's buckets."""
        k = rows_by_key(self.stats["by_k"])
        self.assertEqual({key: (r["blocks"], r["orphans"], r["forks"]) for key, r in k.items()}, {
            "0": (1, 0, 0), "1-17": (17, 1, 1), "18-50": (33, 4, 3), "51-100": (7, 0, 0), "101-200": (0, 0, 0),
            "201-300": (0, 0, 0), "301-400": (0, 0, 0), ">=401": (0, 0, 0), "no reset since NU7": (0, 0, 0),
            "unknown": (6, 0, 0)})
        fine = rows_by_key(self.stats["by_k_fine"])
        self.assertEqual({key: (r["blocks"], r["orphans"]) for key, r in fine.items()}, {
            "0": (1, 0), "1": (1, 1), "2": (1, 0), "3-5": (3, 0), "6-10": (5, 0), "11-17": (7, 0), "18-30": (13, 3)})
        self.assertAlmostEqual(fine["18-30"]["share"], 3 / 5)
        ratio = rows_by_key(self.stats["by_ratio"])
        self.assertEqual({key: (r["blocks"], r["orphans"]) for key, r in ratio.items() if r["blocks"]},
                         {"<0.001": (31, 4), ">=1": (27, 1), "unknown": (6, 0)})

    def test_time_bins(self) -> None:
        """Hourly bins cover the last 72 h; bins sum to the totals."""
        hours = self.stats["by_hour"]
        self.assertEqual(len(hours), 73)
        self.assertEqual(hours[-1]["start"], self.now // 3600 * 3600)
        self.assertEqual(sum(h["blocks"] for h in hours), 64)
        self.assertEqual(sum(h["orphans"] for h in hours), 5)
        self.assertEqual(sum(h["resets"] for h in hours), 1)
        days = self.stats["by_day"]
        self.assertEqual([d["day"] for d in days], ["2026-09-21"])
        self.assertEqual((days[0]["blocks"], days[0]["orphans"], days[0]["forks"], days[0]["resets"]), (64, 5, 4, 1))

    def test_exclude_heights(self) -> None:
        """Excluded ranges drop their canonical blocks, orphans and forks."""
        stats = analysis.orphan_stats(self.tree.chain, now=self.now, exclude_heights=[(BASE + 24, BASE + 26)])
        self.assertEqual((stats["totals"]["blocks"], stats["totals"]["orphans"], stats["totals"]["forks"]), (61, 4, 3))
        self.assertEqual(stats["excluded"], [[BASE + 24, BASE + 26]])
        with self.assertRaises(ValueError):
            analysis.orphan_stats(self.tree.chain, exclude_heights=[(5, 1)])
        with self.assertRaises(ValueError):
            analysis.orphan_stats(self.tree.chain, exclude_heights=[(1, 2)] * 100)
        with self.assertRaises(ValueError):
            analysis.orphan_stats(self.tree.chain, now=float("nan"))

    def test_mainnet_has_no_phases(self) -> None:
        """Without the min-difficulty rule every block is in the unknown phase."""
        tree = Tree(MAINNET)
        tree.add("g", "pre", dt=0, height=BASE)
        tree.line("a", "g", 1, 10)
        tree.add("b5", "a4")
        stats = analysis.orphan_stats(tree.chain, now=T0)
        phases = rows_by_key(stats["by_phase"])
        self.assertEqual((phases["unknown"]["blocks"], phases["unknown"]["orphans"]), (8, 1))
        self.assertEqual(phases["fast"]["blocks"], 0)


class MinerStatsTests(unittest.TestCase):
    """miner_stats on the sawtooth tree."""

    def setUp(self) -> None:
        """Build the tree."""
        self.tree = sawtooth_tree()

    def test_per_miner(self) -> None:
        """Canonical, stale, self-orphans, races, resets, templates and phases per miner."""
        stats = analysis.miner_stats(self.tree.chain)
        assert_json(self, stats)
        self.assertEqual((stats["blocks"], stats["stale"]), (64, 5))
        miners = {row["miner"]: row for row in stats["miners"]}
        self.assertEqual([row["miner"] for row in stats["miners"]], ["A", "B", "C"])
        a, b, c = miners["A"], miners["B"], miners["C"]
        self.assertEqual((a["canonical"], a["stale"], a["self_orphans"], a["races_lost"], a["races_won"]),
                         (63, 1, 1, 0, 4))
        self.assertAlmostEqual(a["share"], 63 / 64, places=6)
        self.assertAlmostEqual(a["stale_rate"], 1 / 64, places=6)
        self.assertEqual((b["canonical"], b["stale"], b["races_lost"], b["resets"]), (1, 2, 2, 1))
        self.assertEqual(b["templates"], {"zakura": 1, "no body": 2})
        self.assertEqual((c["stale"], c["races_lost"], c["stale_rate"]), (2, 2, 1.0))
        self.assertEqual(a["by_phase"], {"fast": {"canonical": 37, "stale": 1}, "slow": {"canonical": 20, "stale": 0},
                                         "steady": {"canonical": 0, "stale": 0},
                                         "unknown": {"canonical": 6, "stale": 0}})
        self.assertEqual(stats["pairs"], [
            {"loser": "B", "winner": "A", "kind": "race", "n": 2},
            {"loser": "C", "winner": "A", "kind": "race", "n": 2},
            {"loser": "A", "winner": "A", "kind": "self", "n": 1},
        ])

    def test_since_limit_and_exclusions(self) -> None:
        """`since` filters by canonical header time, `limit` merges the tail into "(other)"."""
        since = self.tree.node("s10").time
        stats = analysis.miner_stats(self.tree.chain, since=since)
        self.assertEqual((stats["blocks"], stats["stale"], stats["from_height"]), (18, 1, BASE + 46))
        merged = analysis.miner_stats(self.tree.chain, limit=2)
        self.assertEqual([row["miner"] for row in merged["miners"]], ["A", "(other)"])
        other = merged["miners"][1]
        self.assertEqual((other["canonical"], other["stale"], other["races_lost"]), (1, 4, 4))
        excluded = analysis.miner_stats(self.tree.chain, exclude_heights=[(BASE, BASE + 6)])
        self.assertEqual({row["miner"]: row["canonical"] for row in excluded["miners"]}, {"A": 57, "B": 0, "C": 0})
        with self.assertRaises(ValueError):
            analysis.miner_stats(self.tree.chain, since=float("inf"))


class SawtoothAndResetTests(unittest.TestCase):
    """sawtooth and resets."""

    def setUp(self) -> None:
        """Build the tree."""
        self.tree = sawtooth_tree()

    def test_sawtooth_rows(self) -> None:
        """Rows carry dt, k, phase, min-diff and per-height orphans (unsettled ones included)."""
        saw = analysis.sawtooth(self.tree.chain, n=10)
        assert_json(self, saw)
        self.assertEqual((saw["from_height"], saw["to_height"], len(saw["blocks"])), (BASE + 57, BASE + 66, 10))
        self.assertEqual((saw["target_spacing"], saw["averaging_window"]), (75, 17))
        self.assertEqual(saw["blocks"][0]["dt"], 80)
        by_height = {row["height"]: row for row in saw["blocks"]}
        self.assertEqual(by_height[BASE + 65]["orphans"], 1)
        self.assertEqual((by_height[BASE + 66]["k"], by_height[BASE + 66]["label"]), (60, "slow"))
        self.assertEqual(saw["resets"], [])
        full = analysis.sawtooth(self.tree.chain, n=10**9)
        self.assertEqual(len(full["blocks"]), 67)
        self.assertIsNone(full["blocks"][0]["dt"])
        reset_row = next(row for row in full["blocks"] if row["min_diff"])
        self.assertEqual((reset_row["height"], reset_row["dt"], reset_row["difficulty"], reset_row["k"]),
                         (BASE + 6, 451, 1.0, 0))
        self.assertEqual([r["height"] for r in full["resets"]], [BASE + 6])
        self.assertEqual(full["tip_phase"]["label"], "slow")
        with self.assertRaises(ValueError):
            analysis.sawtooth(self.tree.chain, n="10")

    def test_sawtooth_reports_the_rules_at_the_tip(self) -> None:
        """target_spacing and averaging_window switch when the tip reaches NU7; an empty chain has neither."""
        tree = Tree(dataclasses.replace(TESTNET, nu7_height=BASE + 2))
        tree.add("g", "pre", dt=0, height=BASE)
        tree.add("a1", "g")
        saw = analysis.sawtooth(tree.chain)
        self.assertEqual((saw["target_spacing"], saw["averaging_window"]), (75, 17))
        tree.add("a2", "a1")
        saw = analysis.sawtooth(tree.chain)
        self.assertEqual((saw["target_spacing"], saw["averaging_window"]), (25, 102))
        empty = analysis.sawtooth(Chain(TESTNET))
        self.assertEqual((empty["target_spacing"], empty["averaging_window"], empty["blocks"]), (None, None, []))

    def test_resets(self) -> None:
        """Reset rows: gap, forward-dated next block, first-seen forward dating, phase lengths, orphans."""
        (reset,) = analysis.resets(self.tree.chain)
        assert_json(self, reset)
        self.assertEqual((reset["height"], reset["miner"], reset["template"], reset["gap"], reset["next_dt"]),
                         (BASE + 6, "B", "zakura", 451, 2))
        self.assertEqual(reset["forward_dating"], 140.0)
        self.assertAlmostEqual(reset["d_pre"], difficulty_from_bits(NORMAL, TESTNET), places=0)
        self.assertEqual((reset["fast_blocks"], reset["cycle_blocks"], reset["orphans"]), (38, None, 5))
        self.assertEqual((reset["never_slowed"], reset["era"]), (False, "pre-nu7"))
        self.assertEqual(analysis.resets(self.tree.chain, since=reset["time"] + 1), [])
        self.assertEqual(len(analysis.resets(self.tree.chain, limit=0)), 1)


class ForkEventTests(StoreCase):
    """fork_events with and without the store."""

    def tiebreak_tree(self) -> Tree:
        """Five forks whose winners are predicted by hash, first seen, both, work and neither."""
        tree = Tree()
        tree.add("g", "pre", dt=0, height=BASE)
        tree.add("a1", "g")
        tree.add("a2", "a1")
        cases = [("1", 0xF0, 10.0, 0x10, 5.0, NORMAL), ("2", 0x10, 20.0, 0xF0, 25.0, NORMAL),
                 ("3", 0xF0, 30.0, 0x10, 35.0, NORMAL), ("4", 0x10, 40.0, 0xF0, 35.0, EASY),
                 ("5", 0x10, 50.0, 0xF0, 45.0, NORMAL)]
        parent = "a2"
        for name, win_byte, win_seen, lose_byte, lose_seen, lose_bits in cases:
            tree.add(f"W{name}", parent, last_byte=win_byte, seen=win_seen)
            tree.add(f"L{name}", parent, last_byte=lose_byte, seen=lose_seen, bits=lose_bits, miner="B")
            tree.add(f"W{name}c", f"W{name}")
            parent = f"W{name}c"
        tree.line("t", parent, 1, 3)
        return tree

    def test_tiebreak_and_shape_without_store(self) -> None:
        """Events come newest first; without sightings arrival order is unknown, so only work and hash decide."""
        tree = self.tiebreak_tree()
        events = analysis.fork_events(tree.chain)
        assert_json(self, events)
        self.assertEqual([e["tiebreak"] for e in events], ["unresolved", "work", "hash", "unresolved", "hash"])
        self.assertEqual({e["winner_first_seen"] for e in events}, {None})
        first = events[-1]
        self.assertEqual((first["fork_hash"], first["height"], first["depth"]), (tree.hash("a2"), BASE + 3, 1))
        self.assertEqual(first["winner"]["hash"], tree.hash("W1"))
        self.assertEqual(first["losers"][0]["block"]["hash"], tree.hash("L1"))
        self.assertEqual((first["classification"], first["loser_count"]), ("race", 1))
        self.assertIsNone(first["winner"]["probes"])
        self.assertIsNone(first["losers"][0]["adopted_by"])
        self.assertIsNone(first["reorgs"])
        self.assertEqual(first["phase"]["label"], "unknown")
        self.assertEqual(len(analysis.fork_events(tree.chain, limit=2)), 2)
        since = tree.node("W4").time
        self.assertEqual([e["winner"]["hash"] for e in analysis.fork_events(tree.chain, since=since)],
                         [tree.hash("W5"), tree.hash("W4")])
        with self.assertRaises(ValueError):
            analysis.fork_events(tree.chain, limit="all")

    def test_seen_order_from_sightings(self) -> None:
        """Arrival order needs the later block's first sighting to be timely and over SEEN_MARGIN later."""
        tree = self.tiebreak_tree()
        for name in "12345":
            for side in "WL":
                block = tree.node(f"{side}{name}")
                self.store.record_sighting(block.hash, "p2p:1.1.1.1:18233", "inv", block.first_seen_at)
        events = analysis.fork_events(tree.chain, self.conn())
        self.assertEqual([e["tiebreak"] for e in events], ["neither", "work", "both", "first_seen", "hash"])
        first = events[-1]
        self.assertEqual((first["winner_first_seen"], first["losers"][0]["seen_first"]), (False, True))
        # A getchaintips poll half a second before the winner's inv says nothing about when L3 arrived.
        self.store.record_sighting(tree.hash("L3"), "rpc:node1", "chaintip", 29.5)
        # L2 announced 0.5 s after W2 is within the margin.
        self.store.record_sighting(tree.hash("L2"), "p2p:2.2.2.2:18233", "inv", 20.5)
        # A backfill fetch of W1 is no arrival time, but it still shows W1 existed before L1's inv.
        self.store.record_sighting(tree.hash("W1"), "rpc:node1", "backfill", 1.0)
        events = analysis.fork_events(tree.chain, self.conn())
        self.assertEqual([e["tiebreak"] for e in events], ["neither", "work", "hash", "unresolved", "both"])
        self.assertEqual([e["winner_first_seen"] for e in events], [False, False, None, None, True])
        """Self races, multi-block losers and settledness come through from the chain."""
        tree = sawtooth_tree()
        events = {e["height"]: e for e in analysis.fork_events(tree.chain)}
        self.assertEqual(sorted(events), [BASE + 7, BASE + 26, BASE + 31, BASE + 46, BASE + 65])
        self.assertEqual((events[BASE + 7]["classification"], events[BASE + 7]["same_job"]), ("self", True))
        deep = events[BASE + 31]
        self.assertEqual((deep["depth"], deep["losers"][0]["miners"], deep["losers"][0]["winner_len"]),
                         (2, ["C", "C"], 2))
        self.assertEqual(deep["phase"]["k"], 25)
        self.assertFalse(events[BASE + 65]["settled"])
        self.assertTrue(events[BASE + 46]["settled"])

    def test_store_enrichment(self) -> None:
        """Probes, adopting sources (tip changes and best-chain sightings) and per-source reorgs."""
        tree = self.tiebreak_tree()
        store = self.store
        store.upsert_source("p2p:1.1.1.1:18233", impl="zebra", impl_version="6.4.2")
        store.upsert_source("p2p:2.2.2.2:18233", impl="zakura", impl_version="1.5.0")
        store.upsert_source("rpc:node1", kind="rpc")
        w1, l1, w1c = tree.hash("W1"), tree.hash("L1"), tree.hash("W1c")
        store.record_tip_change(source="rpc:node1", at=100.0, new_hash=l1)
        store.record_tip_change(source="rpc:node1", at=105.0, old_hash=l1, new_hash=w1c, fork_hash=tree.hash("a2"),
                                is_reorg=1, disconnected=1, connected=2)
        store.record_sighting(l1, "p2p:1.1.1.1:18233", "inv", 101.0)
        store.record_sighting(w1, "p2p:2.2.2.2:18233", "inv", 102.0)
        store.record_sighting(w1, "p2p:1.1.1.1:18233", "inv", 104.0)
        store.record_sighting(l1, "p2p:3.3.3.3:18233", "chaintip", 99.0)
        store.record_probe(at=101.5, source="p2p:1.1.1.1:18233", impl="zebra", hash=l1, reason="announce",
                           result="notfound", announced_by_same_peer=1)
        store.record_probe(at=102.5, source="p2p:2.2.2.2:18233", impl="zakura", hash=w1, reason="announce",
                           result="block", latency_ms=40)
        # A body fetch asks a peer that never announced the block: no availability evidence.
        store.record_probe(at=103.0, source="p2p:2.2.2.2:18233", impl="zakura", hash=l1, reason="fetch",
                           result="notfound")
        # A source that flip-flops is one vantage point that reorged twice.
        store.record_tip_change(source="p2p:1.1.1.1:18233", at=106.0, old_hash=l1, new_hash=w1c,
                                fork_hash=tree.hash("a2"), is_reorg=1, disconnected=1, connected=2)
        store.record_tip_change(source="rpc:node1", at=107.0, old_hash=l1, new_hash=w1c, fork_hash=tree.hash("a2"),
                                is_reorg=1, disconnected=1, connected=2)
        (event,) = [e for e in analysis.fork_events(tree.chain, self.conn()) if e["winner"]["hash"] == w1]
        assert_json(self, event)
        winner, loser = event["winner"], event["losers"][0]
        self.assertEqual(winner["probes"]["block"], 1)
        self.assertEqual((loser["probes"]["notfound"], loser["probes"]["notfound_same_announcer"]), (1, 1))
        self.assertEqual(winner["adopted_by"]["count"], 2)
        self.assertEqual(winner["adopted_by"]["by_group"], {"zakura 1.5": 1, "zebra 6.4": 1})
        self.assertEqual([s["source"] for s in winner["adopted_by"]["sources"]],
                         ["p2p:2.2.2.2:18233", "p2p:1.1.1.1:18233"])
        self.assertEqual(loser["adopted_by"]["by_group"], {"rpc:node1": 1, "zebra 6.4": 1})
        self.assertEqual(loser["adopted_by"]["sources"][0], {"source": "rpc:node1", "group": "rpc:node1", "at": 100.0,
                                                             "via": "tip"})
        self.assertEqual((event["reorgs"]["count"], event["reorgs"]["total"]), (2, 3))
        self.assertEqual([(s["source"], s["at"], s["reorgs"]) for s in event["reorgs"]["sources"]],
                         [("rpc:node1", 105.0, 2), ("p2p:1.1.1.1:18233", 106.0, 1)])
        self.assertEqual(event["reorgs"]["sources"][0]["disconnected"], 1)
        self.assertFalse(winner["body_trusted"])

    def test_late_losers_and_seen_gap(self) -> None:
        """A loser first seen after the winner's child lost on work at arrival; gaps compare timely sightings."""
        tree = Tree()
        tree.add("g", "pre", dt=0, height=BASE)
        tree.add("a1", "g")
        tree.add("W", "a1", last_byte=0x10, seen=100.0)
        tree.add("L", "a1", last_byte=0xF0, seen=130.0, miner="B")
        tree.add("Wc", "W", seen=120.0)
        tree.line("t", "Wc", 1, 3)
        for name, at in (("W", 100.0), ("Wc", 120.0), ("L", 130.0)):
            self.store.record_sighting(tree.hash(name), "p2p:1.1.1.1:18233", "inv", at)
        (event,) = analysis.fork_events(tree.chain, self.conn())
        self.assertEqual((event["tiebreak"], event["losers"][0]["seen_gap_s"]), ("late", 30.0))
        # Announced before the winner's child, so the tie-break rules still apply (first seen beats hash here).
        self.store.record_sighting(tree.hash("L"), "p2p:2.2.2.2:18233", "inv", 110.0)
        (event,) = analysis.fork_events(tree.chain, self.conn())
        self.assertEqual((event["tiebreak"], event["losers"][0]["seen_gap_s"]), ("first_seen", 10.0))
        self.assertIsNone(analysis.fork_events(tree.chain)[0]["losers"][0]["seen_gap_s"])


class SummaryTests(StoreCase):
    """summary and text_report."""

    def test_periods_reorgs_and_sources(self) -> None:
        """Periods count by header time; reorgs and source health come from the store."""
        tree = sawtooth_tree()
        now = T0 + 3_600 + 300
        store = self.store
        store.upsert_source("rpc:node1", kind="rpc", status="ok", tip_hash=tree.hash("s27"), tip_height=BASE + 63,
                            last_ok_at=now)
        store.upsert_source("p2p:1.1.1.1:18233", impl="zebra", impl_version="6.4.2", status="connected")
        store.upsert_source("p2p:2.2.2.2:18233", status="backoff")
        store.record_tip_change(source="rpc:node1", at=now - 100, new_hash=tree.hash("f26"), is_reorg=1,
                                disconnected=2, connected=2, fork_hash=tree.hash("f24"))
        store.record_tip_change(source="p2p:1.1.1.1:18233", at=now - 7_200, new_hash=tree.hash("f8"), is_reorg=1,
                                disconnected=1, connected=1)
        store.record_tip_change(source="p2p:1.1.1.1:18233", at=now - 50, new_hash=tree.hash("s1"))
        summary = analysis.summary(tree.chain, self.conn(), now)
        assert_json(self, summary)
        hour, day = summary["periods"]["1h"], summary["periods"]["24h"]
        self.assertEqual((hour["blocks"], hour["orphans"], hour["resets"], hour["forks"], hour["deepest_fork"]),
                         (58, 5, 1, 5, 2))
        self.assertEqual((hour["reorgs"], hour["reorged_sources"], hour["deepest_reorg"]), (1, 1, 2))
        # The window starts at T0, so the 24 h reorg counts leave out the reorg before it (now - 7,200).
        self.assertEqual((day["blocks"], day["reorgs"], day["reorged_sources"]), (64, 1, 1))
        self.assertTrue(day["partial"])
        self.assertAlmostEqual(day["orphan_rate"], 5 / 64, places=6)
        self.assertEqual(summary["deepest_reorg_24h"]["source"], "rpc:node1")
        self.assertEqual(summary["tip"]["height"], BASE + 66)
        self.assertEqual(summary["last_reset"]["height"], BASE + 6)
        self.assertEqual(summary["sources"]["rpc"][0]["behind"], 3)
        self.assertEqual(summary["sources"]["p2p"], {"total": 2, "by_status": {"connected": 1, "backoff": 1},
                                                     "connected_by_group": {"zebra 6.4": 1}})
        without_db = analysis.summary(tree.chain, None, now)
        self.assertIsNone(without_db["periods"]["1h"]["reorgs"])

    def test_text_report(self) -> None:
        """The report renders every section, with and without a store, and on an empty chain."""
        tree = sawtooth_tree()
        text = analysis.text_report(tree.chain, self.conn(), now=T0 + 3_100)
        for heading in ("Activity", "Orphan rate by phase", "Orphan rate by blocks since reset", "Miners",
                        "Recent fork events", "Recent resets"):
            self.assertIn(heading, text)
        self.assertIn(f"Tip {BASE + 66}", text)
        self.assertIn("fast", text)
        self.assertIn("No blocks loaded.", analysis.text_report(Chain(TESTNET), None, now=T0))


class EmptyChainTests(StoreCase):
    """Every function tolerates an empty chain and an empty store."""

    def test_empty(self) -> None:
        """Shapes stay stable with nothing loaded."""
        chain, conn = Chain(TESTNET), self.conn()
        results = [
            analysis.summary(chain, conn, T0),
            analysis.fork_events(chain, conn),
            analysis.orphan_stats(chain, conn, now=T0),
            analysis.miner_stats(chain, conn),
            analysis.sawtooth(chain),
            analysis.resets(chain),
            analysis.external_crosscheck(chain, conn),
            analysis.propagation(conn, now=T0),
            analysis.probe_stats(conn, now=T0),
            analysis.group_sources(chain, [], T0),
        ]
        for result in results:
            assert_json(self, result)
        self.assertEqual(results[2]["totals"]["blocks"], 0)
        self.assertIsNone(results[0]["tip"])
        self.assertEqual(results[7]["blocks"], 0)


def split_tree(now: float) -> Tree:
    """Canonical g..c10, a live side branch d6-d7 off c5, a side branch h8 off c7, a dead fork e3 off c2."""
    tree = Tree()
    tree.add("g", "pre", dt=0, height=BASE)
    tree.line("c", "g", 1, 10)
    tree.add("d6", "c5", miner="B")
    tree.add("d7", "d6", miner="B")
    tree.add("h8", "c7", miner="B")
    tree.add("e3", "c2", miner="B", seen=now - 7_200)
    return tree


def source_rows(tree: Tree, now: float) -> list[dict]:
    """Sources rows covering every state: synced, lagging, fork, stuck, inactive and non-node."""
    connected = {"status": "connected", "last_ok_at": now}
    return [
        {"source": "rpc:z1", "kind": "rpc", "impl": "zakura", "impl_version": "1.5.0-rc0", "tip_hash": tree.hash("c10"),
         "status": "ok", "last_ok_at": now},
        {"source": "p2p:10.0.0.2:18233", "kind": "p2p", "impl": "zakura", "impl_version": "1.5.0",
         "tip_hash": tree.hash("c10"), **connected},
        {"source": "p2p:10.0.0.3:18233", "kind": "p2p", "impl": "zebra", "impl_version": "6.4.2",
         "tip_hash": tree.hash("d7"), **connected},
        {"source": "p2p:10.0.0.4:18233", "kind": "p2p", "impl": "zebra", "impl_version": "6.4.1",
         "tip_hash": tree.hash("d6"), **connected},
        {"source": "p2p:10.0.0.5:18233", "kind": "p2p", "impl": "zebra", "impl_version": "6.4.2",
         "tip_hash": tree.hash("c10"), **connected},
        {"source": "p2p:10.0.0.6:18233", "kind": "p2p", "impl": "zebra", "impl_version": "6.3.0",
         "tip_hash": tree.hash("c4"), **connected},
        {"source": "p2p:10.0.0.7:18233", "kind": "p2p", "impl": "zebra", "impl_version": "6.0.0",
         "tip_hash": tree.hash("e3"), **connected},
        {"source": "p2p:10.0.0.8:18233", "kind": "p2p", "impl": "zcashd", "impl_version": "6.2.0",
         "tip_hash": "ab" * 32, "tip_height": BASE - 5_000, **connected},
        {"source": "p2p:10.0.0.9:18233", "kind": "p2p", "impl": "zebra", "impl_version": "6.4.2",
         "tip_hash": tree.hash("d7"), "status": "backoff", "last_ok_at": now - 86_400},
        {"source": "p2p:10.0.0.10:18233", "kind": "p2p", "impl": "zeeder", "impl_version": "0.3.0",
         "tip_hash": tree.hash("c10"), **connected},
    ]


class GroupAndSplitTests(unittest.TestCase):
    """group_sources and detect_split."""

    def setUp(self) -> None:
        """Build the split tree and its sources."""
        self.now = T0 + 1_000.0
        self.tree = split_tree(self.now)
        self.rows = source_rows(self.tree, self.now)

    def test_groups(self) -> None:
        """Members are classified and clustered by branch; the majority branch represents the group."""
        groups = {g["key"]: g for g in analysis.group_sources(self.tree.chain, self.rows, self.now)}
        assert_json(self, list(groups.values()))
        self.assertEqual(set(groups), {"zakura 1.5", "zebra 6.4", "zebra 6.3", "zebra 6.0", "zcashd 6.2",
                                       "zeeder 0.3"})
        zebra = groups["zebra 6.4"]
        self.assertEqual((zebra["members"], zebra["active"], zebra["stuck"]), (4, 3, 0))
        self.assertEqual({k: v for k, v in zebra["states"].items() if v}, {"fork": 2, "synced": 1, "inactive": 1})
        branch = zebra["branch"]
        self.assertEqual((branch["key"], branch["tip_hash"], branch["members"]),
                         (self.tree.hash("d6"), self.tree.hash("d7"), 2))
        self.assertEqual((branch["fork_hash"], branch["fork_height"]), (self.tree.hash("c5"), BASE + 5))
        self.assertEqual(branch["relation"]["kind"], "fork")
        self.assertEqual([b["key"] for b in zebra["branches"]], [self.tree.hash("d6"), "canonical"])
        zakura = groups["zakura 1.5"]
        self.assertEqual((zakura["branch"]["key"], zakura["branch"]["members"], zakura["branch"]["tip_height"]),
                         ("canonical", 2, BASE + 10))
        self.assertEqual(groups["zebra 6.3"]["states"]["lagging"], 1)
        self.assertEqual(groups["zebra 6.3"]["branch"]["tip_height"], BASE + 4)
        for key in ("zebra 6.0", "zcashd 6.2"):
            self.assertIsNone(groups[key]["branch"])
            self.assertEqual(groups[key]["stuck"], 1)
        self.assertEqual(groups["zebra 6.0"]["stuck_sources"], ["p2p:10.0.0.7:18233"])
        self.assertIsNone(groups["zeeder 0.3"]["branch"])

    def test_split_detected(self) -> None:
        """Zebra 6.4 on the side branch conflicts with the canonical groups past the fork."""
        groups = analysis.group_sources(self.tree.chain, self.rows, self.now)
        split = analysis.detect_split(groups)
        assert_json(self, split)
        side = self.tree.hash("d6")
        self.assertEqual((split["key"], split["fork_height"], split["depth"]), (self.tree.hash("c5"), BASE + 5, 5))
        self.assertEqual(split["groups"], {"zakura 1.5": "canonical", "zebra 6.3": "canonical", "zebra 6.4": side})
        self.assertEqual([(s["branch"], s["members"], s["blocks_past_fork"]) for s in split["sides"]],
                         [("canonical", 3, 5), (side, 2, 2)])
        self.assertEqual(split["neutral_groups"], [])
        self.assertIsNone(analysis.detect_split(groups, min_members=3))

    def test_no_split_when_canonical_groups_lag_behind_the_fork(self) -> None:
        """A canonical group still below the fork point is lagging, not split."""
        rows = [self.rows[2], self.rows[5]]  # zebra 6.4 on d7 (fork at c5), zebra 6.3 at c4
        self.assertIsNone(analysis.detect_split(analysis.group_sources(self.tree.chain, rows, self.now)))

    def test_two_side_branches_conflict(self) -> None:
        """Groups on two different side branches split even when nobody is on the best chain."""
        rows = [self.rows[2], {"source": "p2p:10.0.0.20:18233", "kind": "p2p", "impl": "zakura",
                               "impl_version": "1.4.0", "tip_hash": self.tree.hash("h8"), "status": "connected"}]
        split = analysis.detect_split(analysis.group_sources(self.tree.chain, rows, self.now))
        self.assertEqual(split["fork_height"], BASE + 5)
        self.assertEqual(sorted(split["groups"]), ["zakura 1.4", "zebra 6.4"])
        self.assertIsNone(analysis.detect_split([]))

    def test_stuck_rules(self) -> None:
        """Stuck: an old fork tip, or more than 1000 blocks behind; young forks are only forks."""
        self.assertEqual(analysis._state("fork", 2, 3, 10.0), "fork")
        self.assertEqual(analysis._state("fork", 2, 3, analysis.STUCK_AGE + 1), "stuck")
        self.assertEqual(analysis._state("behind", 2, 2, None), "synced")
        self.assertEqual(analysis._state("behind", 50, 50, None), "lagging")
        self.assertEqual(analysis._state("behind", 1_001, 1_001, None), "stuck")
        self.assertEqual(analysis._state("unknown", None, 1_001, None), "stuck")
        self.assertEqual(analysis._state("unknown", None, 5, None), "unknown")

    def test_dead_fork_met_on_first_contact_is_stuck(self) -> None:
        """A fork tip whose header is hours old is stuck, not a split, even when it was only just fetched."""
        now = T0 + 20_000.0
        tree = Tree()
        tree.add("g", "pre", dt=0, height=BASE)
        tree.line("c", "g", 1, 10)
        tree.add("d4", "c3", miner="B", seen=now - 5)
        tree.add("d5", "d4", miner="B", seen=now - 5)
        rows = [
            {"source": "rpc:z1", "kind": "rpc", "impl": "zakura", "impl_version": "1.5.0",
             "tip_hash": tree.hash("c10"), "status": "ok", "last_ok_at": now},
            {"source": "p2p:10.0.0.7:18233", "kind": "p2p", "impl": "zebra", "impl_version": "6.1.0",
             "tip_hash": tree.hash("d5"), "status": "connected", "last_ok_at": now},
        ]
        groups = {g["key"]: g for g in analysis.group_sources(tree.chain, rows, now)}
        self.assertEqual(groups["zebra 6.1"]["stuck"], 1)
        self.assertIsNone(analysis.detect_split(list(groups.values())))


class FakeP2P:
    """Stands in for the P2P observer's snapshot()."""

    def __init__(self, result=None, error: Exception | None = None) -> None:
        """Return `result` or raise `error`."""
        self.result, self.error = result, error

    def snapshot(self):
        """Return the canned snapshot."""
        if self.error is not None:
            raise self.error
        return self.result


class LiveSnapshotTests(StoreCase):
    """live_snapshot and peers against a fake monitor."""

    def setUp(self) -> None:
        """Build a monitor over the split tree with stored sources."""
        super().setUp()
        self.now = T0 + 1_000.0
        self.tree = split_tree(self.now)
        for row in source_rows(self.tree, self.now):
            fields = {k: v for k, v in row.items() if k != "source"}
            self.store.upsert_source(row["source"], **fields)
        self.store.record_tip_change(source="rpc:z1", at=self.now - 5, old_hash=self.tree.hash("d7"),
                                     new_hash=self.tree.hash("c10"), fork_hash=self.tree.hash("c5"), is_reorg=1,
                                     disconnected=2, connected=5)
        self.store.commit_if_due(force=True)
        config = parse_config({"rpc": [
            {"name": "z1", "url": "http://10.0.0.2:18232/", "fleet": True},
            {"name": "z2", "url": "http://10.0.0.30:18232/", "fleet": True},
        ]})
        live = [
            {"source": "p2p:10.0.0.3:18233", "connected": True, "rtt": 0.05, "ua": "x" * 1_000},
            {"ip": "10.0.0.11", "port": 18233, "connected": True, "impl": "zebra", "version": "6.2.3",
             "tip_hash": self.tree.hash("c10"), "rtt": float("nan")},
        ]
        self.monitor = types.SimpleNamespace(
            chain=self.tree.chain, store=self.store, config=config, params=TESTNET, p2p=FakeP2P(live),
            rpc_status=lambda: {"rpc:z1": {"status": "ok", "polls": 3}},
        )

    def test_snapshot(self) -> None:
        """The snapshot has the documented keys, is strict JSON and flags the split."""
        snap = analysis.live_snapshot(self.monitor, self.now)
        assert_json(self, snap)
        self.assertEqual(set(snap), {"generated_at", "network", "tip", "phase", "chain", "peers", "groups",
                                     "split_candidate", "stuck", "collectors", "recent_reorgs"})
        self.assertEqual(snap["tip"]["hash"], self.tree.hash("c10"))
        self.assertEqual(snap["chain"]["from_height"], BASE)
        self.assertEqual(snap["split_candidate"]["fork_hash"], self.tree.hash("c5"))
        self.assertEqual({v["source"] for v in snap["stuck"]}, {"p2p:10.0.0.7:18233", "p2p:10.0.0.8:18233"})
        self.assertEqual(snap["collectors"]["rpc"], [{"source": "rpc:z1", "status": "ok", "polls": 3}])
        p2p = snap["collectors"]["p2p"]
        self.assertEqual((p2p["enabled"], p2p["error"], p2p["peers"]), (True, None, 2))
        self.assertEqual(p2p["connected_by_group"], {"zakura 1.5": 1, "zebra 6.4": 3, "zebra 6.3": 1, "zebra 6.2": 1,
                                                     "zebra 6.0": 1, "zcashd 6.2": 1, "zeeder 0.3": 1})
        self.assertEqual(snap["recent_reorgs"][0]["disconnected"], 2)
        # rpc:z1 and p2p:10.0.0.2 are one fleet host.
        self.assertEqual({g["key"]: g for g in snap["groups"]}["zakura 1.5"]["fleet"], 1)

    def test_peer_views(self) -> None:
        """Configured endpoints and live-only peers appear; fleet hosts and live data are attached."""
        views = {v["source"]: v for v in analysis.peers(self.monitor, self.now)}
        self.assertEqual(next(iter(views)), "rpc:z1")
        self.assertEqual(views["rpc:z2"]["status"], "pending")
        self.assertEqual(views["rpc:z2"]["group"], "zakura")
        self.assertTrue(views["p2p:10.0.0.2:18233"]["fleet"])
        self.assertFalse(views["p2p:10.0.0.3:18233"]["fleet"])
        self.assertEqual(len(views["p2p:10.0.0.3:18233"]["live"]["ua"]), analysis.MAX_TEXT)
        added = views["p2p:10.0.0.11:18233"]
        self.assertEqual((added["group"], added["state"], added["live"]["rtt"]), ("zebra 6.2", "synced", None))
        self.assertEqual(views["p2p:10.0.0.9:18233"]["state"], "inactive")
        fork = views["p2p:10.0.0.3:18233"]
        self.assertEqual((fork["state"], fork["relation"]["kind"], fork["relation"]["depth_theirs"], fork["behind"]),
                         ("fork", "fork", 2, 3))

    def test_optional_collectors(self) -> None:
        """A failing or absent observer and a missing rpc_status never break the snapshot."""
        self.monitor.p2p = FakeP2P(error=RuntimeError("boom"))
        snap = analysis.live_snapshot(self.monitor, self.now)
        self.assertEqual(snap["collectors"]["p2p"]["error"], "RuntimeError: boom")
        bare = types.SimpleNamespace(chain=self.tree.chain, store=self.store, config=None, params=TESTNET)
        snap = analysis.live_snapshot(bare, self.now)
        assert_json(self, snap)
        self.assertEqual([r["source"] for r in snap["collectors"]["rpc"]], ["rpc:z1"])
        self.assertFalse(snap["collectors"]["p2p"]["enabled"])
        self.monitor.p2p = FakeP2P({"peers": {"10.0.0.40:18233": {"connected": True}}})
        self.assertIn("p2p:10.0.0.40:18233", {v["source"] for v in analysis.peers(self.monitor, self.now)})
        self.monitor.p2p = FakeP2P(42)
        self.assertEqual(analysis.live_snapshot(self.monitor, self.now)["collectors"]["p2p"]["error"],
                         "p2p snapshot has an unexpected shape")


class PropagationTests(StoreCase):
    """propagation from sightings."""

    def test_groups_and_recent(self) -> None:
        """Delays are relative to each block's first sighting; coverage follows each source's reporting span."""
        store = self.store
        store.upsert_source("rpc:node1", kind="rpc")
        store.upsert_source("p2p:1.1.1.1:18233", impl="zebra", impl_version="6.4.2")
        store.upsert_source("p2p:2.2.2.2:18233", impl="zakura", impl_version="1.5.0")
        store.upsert_source("p2p:4.4.4.4:18233", impl="zebra", impl_version="6.4.1")
        x, y, z = bhash("X"), bhash("Y"), bhash("Z")
        for block_hash, height, seen in ((x, 10, 100.0), (y, 11, 200.0), (z, 12, 300.0)):
            self.add_block(block_hash, height, seen)
        for block_hash, source, kind, at in (
            (x, "rpc:node1", "rpc_tip", 100.0), (x, "p2p:1.1.1.1:18233", "inv", 101.0),
            (x, "p2p:2.2.2.2:18233", "inv", 102.0), (x, "p2p:4.4.4.4:18233", "inv", 103.0),
            (y, "rpc:node1", "rpc_tip", 200.0), (y, "p2p:2.2.2.2:18233", "inv", 200.5),
            (y, "p2p:4.4.4.4:18233", "inv", 210.0), (y, "p2p:9.9.9.9:18233", "chaintip", 250.0),
            (z, "rpc:node1", "rpc_tip", 300.0), (z, "p2p:1.1.1.1:18233", "inv", 301.0),
        ):
            store.record_sighting(block_hash, source, kind, at)
        result = analysis.propagation(self.conn(), since=0.0, now=400.0)
        assert_json(self, result)
        self.assertEqual((result["blocks"], result["sightings"]), (3, 9))
        groups = {g["group"]: g for g in result["by_group"]}
        zebra = groups["zebra 6.4"]
        self.assertEqual((zebra["sources"], zebra["sightings"], zebra["p50_s"], zebra["p90_s"], zebra["max_s"]),
                         (2, 4, 3.0, 10.0, 10.0))
        self.assertAlmostEqual(zebra["coverage"], (2 / 3 + 1) / 2, places=5)
        self.assertEqual((groups["zakura 1.5"]["p50_s"], groups["zakura 1.5"]["p90_s"]), (0.5, 2.0))
        self.assertEqual((groups["rpc:node1"]["first"], groups["rpc:node1"]["kind"]), (3, "rpc"))
        self.assertEqual([r["hash"] for r in result["recent"]], [z, y, x])
        self.assertEqual((result["recent"][0]["sources"], result["recent"][0]["max_s"]), (2, 1.0))
        only_z = analysis.propagation(self.conn(), since=250.0, now=400.0)
        self.assertEqual(only_z["blocks"], 1)
        store.mark_rules_invalid([x])  # e.g. a pre-NU7 block: not part of the network's chain
        self.assertEqual([r["hash"] for r in analysis.propagation(self.conn(), since=0.0, now=400.0)["recent"]],
                         [z, y])

    def test_backfill_sightings_are_no_reference_time(self) -> None:
        """A block backfilled before it was seen live is timed from its first live sighting."""
        store = self.store
        store.upsert_source("rpc:node1", kind="rpc")
        store.upsert_source("p2p:1.1.1.1:18233", impl="zebra", impl_version="6.4.2")
        block_hash = bhash("T")
        store.record_sighting(block_hash, "rpc:node1", "backfill", 50.0)
        self.add_block(block_hash, 10, 100.0)
        store.record_sighting(block_hash, "rpc:node1", "rpc_tip", 100.0)
        store.record_sighting(block_hash, "p2p:1.1.1.1:18233", "inv", 104.0)
        result = analysis.propagation(self.conn(), since=0.0, now=400.0)
        self.assertEqual(result["recent"][0]["first_seen_at"], 100.0)
        self.assertEqual(result["recent"][0]["max_s"], 4.0)

    def test_window_follows_the_persisted_best_height(self) -> None:
        """A stored row with a forged height far above the tip does not move the window past real blocks."""
        store = self.store
        store.upsert_source("rpc:node1", kind="rpc")
        for index in range(3):
            block_hash = bhash(f"B{index}")
            self.add_block(block_hash, 30_000 + index, 100.0 + index)
            store.record_sighting(block_hash, "rpc:node1", "rpc_tip", 100.0 + index)
        self.add_block(bhash("forged"), 2_000_000_000, 150.0)
        self.assertEqual(analysis.propagation(self.conn(), since=0.0, now=400.0)["blocks"], 0)
        store.set_meta("best_height", 30_002)
        self.assertEqual(analysis.propagation(self.conn(), since=0.0, now=400.0)["blocks"], 3)


class ProbeStatsTests(StoreCase):
    """probe_stats from the probes table."""

    def test_rates_and_incidents(self) -> None:
        """Results by implementation and reason; announced-then-notfound incidents with canonicity."""
        tree = Tree()
        tree.add("g", "pre", dt=0, height=BASE)
        tree.line("a", "g", 1, 5)
        tree.add("L", "a2", miner="B")
        w, lost = tree.hash("a3"), tree.hash("L")
        store = self.store
        store.upsert_source("p2p:1.1.1.1:18233", impl="zebra", impl_version="6.4.2")
        store.upsert_source("p2p:2.2.2.2:18233", impl="zakura", impl_version="1.5.0")
        probes = [
            (100.0, "p2p:1.1.1.1:18233", "zebra", lost, "announce", "notfound", None, 1),
            (101.0, "p2p:1.1.1.1:18233", "zebra", w, "announce", "block", 50, 0),
            (102.0, "p2p:2.2.2.2:18233", "zakura", lost, "fetch", "block", 80, 0),
            (103.0, "p2p:6.6.6.6:18233", "zcashd", w, "reprobe", "timeout", None, 0),
            (104.0, "p2p:1.1.1.1:18233", "zebra", w, "reprobe", "block", 30, 1),
            (105.0, "p2p:1.1.1.1:18233", "zebra", w, "reprobe", "block", 30, 1),
            (10.0, "p2p:1.1.1.1:18233", "zebra", w, "announce", "notfound", None, 1),
        ]
        for at, source, impl, block_hash, reason, result, latency, same in probes:
            store.record_probe(at=at, source=source, impl=impl, hash=block_hash, reason=reason, result=result,
                               latency_ms=latency, announced_by_same_peer=same)
        stats = analysis.probe_stats(self.conn(), since=50.0, now=200.0, chain=tree.chain)
        assert_json(self, stats)
        self.assertEqual(stats["probes"], 6)
        groups = {g["group"]: g for g in stats["by_group"]}
        zebra = groups["zebra 6.4"]
        # Reprobes of blocks already held would dilute the announce notfound rate, so they are kept apart.
        self.assertEqual((zebra["probes"], zebra["block"], zebra["notfound"], zebra["latency_p50_ms"]), (2, 1, 1, 50))
        self.assertEqual(zebra["notfound_rate"], 0.5)
        self.assertEqual(set(groups), {"zebra 6.4"})  # a fetch is not an availability probe
        reprobes = {g["group"]: g for g in stats["reprobe_by_group"]}
        self.assertEqual((reprobes["zebra 6.4"]["probes"], reprobes["zebra 6.4"]["notfound_rate"]), (2, 0.0))
        self.assertEqual(reprobes["zcashd"]["timeout_rate"], 1.0)
        self.assertEqual({r["reason"]: r["probes"] for r in stats["by_reason"]},
                         {"announce": 2, "fetch": 1, "reprobe": 3})
        self.assertEqual(stats["incident_count"], 1)
        incident = stats["incidents"][0]
        self.assertEqual((incident["hash"], incident["height"], incident["canonical"], incident["group"]),
                         (lost, BASE + 3, False, "zebra 6.4"))
        everything = analysis.probe_stats(self.conn(), since=0.0, now=200.0)
        self.assertEqual((everything["probes"], everything["incident_count"]), (7, 2))
        self.assertIsNone(everything["incidents"][0]["canonical"])

    def test_groups_use_the_version_at_probe_time(self) -> None:
        """A probe stored with the peer's version then is grouped by it, not by the peer's current version."""
        store = self.store
        store.upsert_source("p2p:1.1.1.1:18233", impl="zebra", impl_version="6.4.2")
        for at, name, version in ((100.0, "a", "6.3.0"), (101.0, "b", None)):
            store.record_probe(at=at, source="p2p:1.1.1.1:18233", impl="zebra", hash=bhash(name), reason="announce",
                               result="block", impl_version=version)
        stats = analysis.probe_stats(self.conn(), since=0.0, now=200.0)
        self.assertEqual({group["group"]: group["probes"] for group in stats["by_group"]},
                         {"zebra 6.3": 1, "zebra 6.4": 1})


class CrosscheckTests(StoreCase):
    """external_crosscheck against the sawtooth tree."""

    def test_totals(self) -> None:
        """Both / only theirs / only ours / flip-flops / out of window, per source and per day."""
        tree = sawtooth_tree()
        store = self.store
        store.upsert_external_orphan(source="cipherscan", hash=tree.hash("x1"), height=BASE + 7)
        store.upsert_external_orphan(source="cipherscan", hash=bhash("theirs"), height=BASE + 10, time=T0,
                                     miner_address="t2abc")
        store.upsert_external_orphan(source="cipherscan", hash=tree.hash("f5"), height=BASE + 11)
        store.upsert_external_orphan(source="cipherscan", hash=bhash("old"), height=BASE - 100)
        store.upsert_external_orphan(source="cipherscan", hash=bhash("new"), height=BASE + 65)
        store.upsert_external_orphan(source="other", hash=tree.hash("x20"), height=BASE + 26)
        store.upsert_external_orphan(source="other", hash=tree.hash("x1"), height=BASE + 7)
        check = analysis.external_crosscheck(tree.chain, self.conn())
        assert_json(self, check)
        self.assertEqual(check["totals"], {"both": 2, "only_theirs": 1, "seen_unfetched": 0, "only_ours": 3,
                                           "theirs_canonical": 1, "out_of_window": 2, "unwatched": 0})
        self.assertEqual(check["sources"], ["cipherscan", "other"])
        self.assertEqual(check["by_source"]["other"], {"both": 2, "only_theirs": 0, "seen_unfetched": 0,
                                                       "theirs_canonical": 0, "unwatched": 0})
        self.assertEqual([(d["day"], d["both"], d["only_theirs"], d["only_ours"], d["theirs_canonical"])
                          for d in check["by_day"]], [("2026-09-21", 2, 1, 3, 1)])
        self.assertEqual(check["only_theirs"][0]["miner_address"], "t2abc")
        self.assertEqual([b["hash"] for b in check["only_ours"]], [tree.hash("y"), tree.hash("l2"), tree.hash("l1")])
        self.assertEqual(analysis.external_crosscheck(tree.chain, None)["totals"]["both"], 0)

    def test_seen_but_never_fetched(self) -> None:
        """External orphans seen here only as a sighting or a getchaintips tip are not "only theirs"."""
        tree = sawtooth_tree()
        store = self.store
        for name in ("tipped", "sighted", "unseen"):
            store.upsert_external_orphan(source="cipherscan", hash=bhash(name), height=BASE + 10, time=T0)
        store.upsert_chaintip("rpc:node1", bhash("tipped"), BASE + 10, 1, "valid-fork", 50.0)
        store.record_sighting(bhash("sighted"), "rpc:node1", "chaintip", 50.0)
        check = analysis.external_crosscheck(tree.chain, self.conn())
        self.assertEqual((check["totals"]["seen_unfetched"], check["totals"]["only_theirs"]), (2, 1))
        self.assertEqual(check["by_source"]["cipherscan"]["seen_unfetched"], 2)
        self.assertEqual(check["by_day"][0]["seen_unfetched"], 2)
        self.assertEqual([row["hash"] for row in check["only_theirs"]], [bhash("unseen")])


STARTUP = T0 + 5_000.0


def coverage_tree() -> Tree:
    """A start at STARTUP: g, b1..b10 backfilled then (resets b3 and b7), c1..c10 watched live.

    Stale: x5 (a getchaintips side tip at a backfilled height) and y3 (live).
    Settled canonical heights are BASE..BASE+17: 11 backfilled, 7 live.
    """
    tree = Tree()
    tree.add("g", "pre", dt=0, height=BASE, seen=STARTUP)
    for index in range(1, 11):
        tree.add(f"b{index}", f"b{index - 1}" if index > 1 else "g", seen=STARTUP,
                 bits=MIN if index in (3, 7) else NORMAL)
    tree.add("x5", "b4", miner="B", seen=STARTUP + 1)
    tree.add("c1", "b10", dt=4_900)
    tree.line("c", "c1", 2, 10)
    tree.add("y3", "c2", miner="B")
    return tree


class CoverageTests(StoreCase):
    """Heights not watched live (backfilled) stay out of orphan rates."""

    def setUp(self) -> None:
        """Build the tree."""
        super().setUp()
        self.tree = coverage_tree()

    def test_rates_skip_backfilled_heights(self) -> None:
        """Orphan stats, miner stats and summary periods count only the live heights and say so."""
        chain = self.tree.chain
        stats = analysis.orphan_stats(chain, now=STARTUP + 600)
        self.assertEqual({k: stats["totals"][k] for k in ("blocks", "orphans", "forks", "unobserved")},
                         {"blocks": 7, "orphans": 1, "forks": 1, "unobserved": 11})
        self.assertEqual(stats["from_height"], BASE + 11)
        miners = analysis.miner_stats(chain)
        self.assertEqual((miners["blocks"], miners["stale"], miners["unobserved"]), (7, 1, 11))
        periods = analysis.summary(chain, None, STARTUP + 600)["periods"]
        self.assertEqual({k: periods["24h"][k] for k in ("blocks", "unobserved", "orphans", "forks", "partial")},
                         {"blocks": 7, "unobserved": 11, "orphans": 1, "forks": 1, "partial": True})
        self.assertEqual((periods["1h"]["blocks"], periods["1h"]["unobserved"], periods["1h"]["partial"]),
                         (7, 0, False))
        # A late sighting (e.g. a lagging peer's headers) still does not make a height watched live.
        late = self.tree.node("b9")
        chain.add(header(late.hash, late.prev_hash, late.time, NORMAL), None, first_seen_at=late.time + 3_000.0)
        self.assertEqual(analysis.orphan_stats(chain, now=STARTUP + 600)["totals"]["unobserved"], 11)
        untimed = Tree()  # a backfill may leave no first-seen time at all
        untimed.add("g", "pre", dt=0, height=BASE)
        untimed.chain.add(header(untimed.hash("u"), untimed.hash("g"), T0 + 20, NORMAL), None)
        untimed.line("v", "u", 1, 4)
        self.assertEqual(analysis.orphan_stats(untimed.chain, now=T0)["totals"]["unobserved"], 1)

    def test_resets_and_crosscheck(self) -> None:
        """Reset cycles report orphans only where watched; external orphans at blind heights are unwatched."""
        newest, oldest = analysis.resets(self.tree.chain)
        self.assertEqual((newest["height"], newest["orphans"], newest["unobserved"]), (BASE + 7, 1, 4))
        self.assertEqual((oldest["height"], oldest["orphans"], oldest["unobserved"]), (BASE + 3, None, 4))
        store = self.store
        store.upsert_external_orphan(source="cipherscan", hash=bhash("blind"), height=BASE + 5)
        store.upsert_external_orphan(source="cipherscan", hash=self.tree.hash("y3"), height=BASE + 13)
        store.upsert_external_orphan(source="cipherscan", hash=bhash("missed"), height=BASE + 14)
        check = analysis.external_crosscheck(self.tree.chain, self.conn())
        self.assertEqual(check["totals"], {"both": 1, "only_theirs": 1, "seen_unfetched": 0, "only_ours": 1,
                                           "theirs_canonical": 0, "out_of_window": 0, "unwatched": 1})
        self.assertEqual([row["hash"] for row in check["only_theirs"]], [bhash("missed")])
        self.assertEqual(check["by_source"]["cipherscan"]["unwatched"], 1)


class NoResetSinceNu7Tests(unittest.TestCase):
    """Heights from NU7 on that no NU7 reset governs are their own "steady" phase."""

    def test_steady_buckets(self) -> None:
        """Blocks past NU7 leave the slow phase for "steady" and its own k and ratio buckets."""
        tree = sawtooth_tree(dataclasses.replace(TESTNET, nu7_height=BASE + 50))
        stats = analysis.orphan_stats(tree.chain, now=T0 + 3_100)
        phases = rows_by_key(stats["by_phase"])
        self.assertEqual({key: row["blocks"] for key, row in phases.items()},
                         {"fast": 38, "slow": 6, "steady": 14, "unknown": 6})
        self.assertEqual(rows_by_key(stats["by_k"])["no reset since NU7"]["blocks"], 14)
        self.assertEqual(rows_by_key(stats["by_ratio"])["no reset since NU7"]["blocks"], 14)
        head = analysis.summary(tree.chain, None, T0 + 3_100)
        phase = head["phase"]
        self.assertEqual(
            (phase["label"], phase["era"], phase["k"], phase["k_bucket"], phase["ratio_bucket"], phase["d_pre"],
             phase["fast"]),
            ("steady", "nu7", 60, "no reset since NU7", "no reset since NU7", None, False),
        )
        self.assertEqual((head["last_reset"]["height"], head["last_reset"]["era"]), (BASE + 6, "pre-nu7"))
        miners = {row["miner"]: row for row in analysis.miner_stats(tree.chain)["miners"]}
        self.assertEqual(miners["A"]["by_phase"]["steady"], {"canonical": 14, "stale": 0})
        rows = analysis.sawtooth(tree.chain, n=20)["blocks"]
        self.assertEqual({row["label"] for row in rows if row["height"] >= BASE + 50}, {"steady"})

    def test_last_reset_beyond_5000_blocks(self) -> None:
        """The newest reset is reported however far below the tip it is."""
        tree = Tree()
        tree.add("g", "pre", dt=0, height=BASE)
        tree.add("r", "g", dt=451, bits=MIN)
        tree.line("a", "r", 1, 5_010)
        self.assertEqual(analysis.summary(tree.chain, None, T0)["last_reset"]["height"], BASE + 1)


class AttributionTests(unittest.TestCase):
    """Losers without a body and untagged shielded coinbases."""

    def test_no_body_and_untagged_shielded(self) -> None:
        """A body-less loser is "no body", not an unattributed miner; "shielded:notag" attributes nothing."""
        tree = Tree()
        tree.add("g", "pre", dt=0, height=BASE)
        tree.line("a", "g", 1, 2)
        tree.add("w3", "a2", miner="shielded:notag", template="")
        tree.add("n3", "a2", miner="shielded:notag", dt=21)
        tree.add("w4", "w3")
        tree.add("h4", "w3", miner=None, dt=21)
        tree.line("t", "w4", 1, 4)
        stats = analysis.miner_stats(tree.chain)
        miners = {row["miner"]: row for row in stats["miners"]}
        body_less = miners["no body"]
        self.assertEqual(
            (body_less["stale"], body_less["unattributed_losses"], body_less["stale_rate"], body_less["share"],
             body_less["templates"]),
            (1, 0, None, None, {"no body": 1}),
        )
        self.assertEqual((miners["A"]["tag"], body_less["tag"]), (None, None))  # header-only blocks have no tag
        notag = miners["shielded:notag"]
        self.assertEqual((notag["canonical"], notag["stale"], notag["self_orphans"], notag["unattributed_losses"]),
                         (1, 1, 0, 1))
        self.assertEqual((notag["templates"], notag["tag"]), ({"no marker": 1, "no body": 1}, "shielded:notag"))
        self.assertEqual({(pair["loser"], pair["kind"]) for pair in stats["pairs"]},
                         {("no body", "no_body"), ("shielded:notag", "unknown")})
        totals = analysis.orphan_stats(tree.chain, now=T0)["totals"]
        self.assertEqual((totals["no_body"], totals["unknown"], totals["self"]), (1, 1, 0))
        events = {event["height"]: event for event in analysis.fork_events(tree.chain)}
        self.assertEqual((events[BASE + 3]["classification"], events[BASE + 4]["classification"]),
                         ("unknown", "no_body"))
        self.assertEqual(events[BASE + 4]["losers"][0]["miners"], ["no body"])
        # Block dicts carry the full coinbase tag of a parsed body, else None.
        self.assertEqual((events[BASE + 3]["winner"]["miner_tag"], events[BASE + 4]["losers"][0]["block"]["miner_tag"]),
                         ("shielded:notag", None))


class PeerViewTests(StoreCase):
    """Peer views and groups from stored sources: old rules, errors, staleness, retired and stored tips."""

    def monitor(self, chain: Chain, config=None, live=()):
        """Commit the store and return a fake monitor over `chain`."""
        self.store.commit_if_due(force=True)
        return types.SimpleNamespace(chain=chain, store=self.store, config=config, params=TESTNET,
                                     p2p=FakeP2P(list(live)))

    def test_old_rules_peers(self) -> None:
        """Peers rejected under our rules fork at their rules fork height and never stand for a split side."""
        now = T0 + 1_000.0
        tree = split_tree(now)
        store = self.store
        old = {"impl": "zebra", "impl_version": "6.4.2", "status": "old-rules", "start_height": BASE + 30}
        store.upsert_source("p2p:10.0.0.2:18233", impl="zakura", impl_version="1.6.0", tip_hash=tree.hash("c10"),
                            status="connected", last_ok_at=now, first_seen_at=now - 100)
        store.upsert_source("p2p:10.0.0.40:18233", **old, rules_fork_height=BASE + 5, tip_hash=tree.hash("c4"),
                            last_ok_at=now - 3_600,
                            first_seen_at=now - 7_200, last_error="header has the wrong difficulty",
                            last_error_at=now - 3_590)
        store.upsert_source("p2p:10.0.0.41:18233", **old, rules_fork_height=BASE + 5, last_ok_at=now - 80_000,
                            first_seen_at=now - 80_000)
        store.upsert_source("p2p:10.0.0.42:18233", **old, tip_hash=tree.hash("d7"), last_ok_at=now - 90_000,
                            first_seen_at=now - 90_000)
        snap = analysis.live_snapshot(self.monitor(tree.chain), now)
        assert_json(self, snap)
        views = {view["source"]: view for view in snap["peers"]}
        rejected = views["p2p:10.0.0.40:18233"]
        self.assertEqual((rejected["state"], rejected["active"], rejected["stuck"], rejected["last_error"]),
                         ("old-rules", True, False, "header has the wrong difficulty"))
        self.assertEqual(rejected["relation"], {"kind": "fork", "n": 25, "fork_hash": tree.hash("c5"),
                                                "fork_height": BASE + 5, "depth_ours": 5, "depth_theirs": 25,
                                                "tip_height": BASE + 30})
        # Its stored tip predates the rejection: the version height stands for its chain instead.
        self.assertEqual((rejected["tip_hash"], rejected["tip_height"], rejected["tip_age_s"], rejected["behind"]),
                         (None, None, None, -20))
        self.assertEqual(views["p2p:10.0.0.41:18233"]["state"], "old-rules")  # handshake within OLD_RULES_WINDOW
        # A reconnect writes "connected" before its walk meets the rejected header; the stored height decides.
        store.upsert_source("p2p:10.0.0.40:18233", status="connected")
        again = {v["source"]: v for v in analysis.peers(self.monitor(tree.chain), now)}["p2p:10.0.0.40:18233"]
        self.assertEqual((again["state"], again["stuck"], again["relation"]),
                         ("old-rules", False, rejected["relation"]))
        gone = views["p2p:10.0.0.42:18233"]  # no rules fork height: where its tip forks off
        self.assertEqual((gone["state"], gone["stale"], gone["relation"]["fork_height"]), ("inactive", True, BASE + 5))
        zebra = {group["key"]: group for group in snap["groups"]}["zebra 6.4"]
        self.assertEqual((zebra["members"], zebra["old_rules"], zebra["stale"], zebra["states"]["old-rules"]),
                         (2, 2, 1, 2))
        self.assertIsNone(zebra["branch"])
        self.assertEqual(zebra["old_rules_relation"], rejected["relation"])  # the highest of its old-rules members
        self.assertIsNone(snap["split_candidate"])

    def test_errors_ages_and_notes(self) -> None:
        """Errors older than a success or the live connection are previous; ages and tip notes come through."""
        now = T0 + 1_000.0
        tree = split_tree(now)
        store = self.store
        store.upsert_source("rpc:z1", kind="rpc", impl="zakura", impl_version="1.6.0", tip_hash=tree.hash("c10"),
                            status="ok", last_ok_at=now, last_error="tip: Connection refused", last_error_at=now - 500)
        store.upsert_source("p2p:10.0.0.3:18233", impl="zebra", impl_version="7.0.0", tip_hash=tree.hash("c10"),
                            status="connected", last_ok_at=now - 2_000, last_error="too many block announcements",
                            last_error_at=now - 1_000)
        config = parse_config({"rpc": [{"name": "z1", "url": "http://10.0.0.2:18232/", "fleet": True}]})
        live = [{"source": "p2p:10.0.0.3:18233", "connected": True, "connected_at": now - 900,
                 "tip_note": "no common block in window"}]
        views = {v["source"]: v for v in analysis.peers(self.monitor(tree.chain, config, live), now)}
        rpc, peer = views["rpc:z1"], views["p2p:10.0.0.3:18233"]
        self.assertEqual((rpc["last_error"], rpc["previous_error"], rpc["previous_error_at"]),
                         (None, "tip: Connection refused", now - 500))
        self.assertEqual((peer["last_error"], peer["previous_error"]), (None, "too many block announcements"))
        self.assertEqual(peer["tip_note"], "no common block in window")
        # A tip watched live ages from its first sighting, as the header's tip does.
        self.assertEqual(peer["tip_age_s"], now - (tree.node("c10").time + 1))
        (row,) = analysis.summary(tree.chain, self.conn(), now, config=config)["sources"]["rpc"]
        self.assertEqual((row["last_error"], row["previous_error"]), (None, "tip: Connection refused"))

    def test_stale_and_retired_sources(self) -> None:
        """Retired endpoints and fleet peers disappear; silent sources only count as stale; fleet counts hosts."""
        now = T0 + 300_000.0
        tree = split_tree(now)
        store = self.store
        config = parse_config({"rpc": [{"name": "z1", "url": "http://10.0.0.2:18232/", "fleet": True}]})
        zakura = {"impl": "zakura", "impl_version": "1.6.0"}
        old = {"last_ok_at": now - 200_000, "first_seen_at": now - 250_000}
        store.upsert_source("rpc:z1", kind="rpc", **zakura, tip_hash=tree.hash("c10"), status="ok", last_ok_at=now)
        store.upsert_source("rpc:gone", kind="rpc", **zakura, status="error", **old)
        store.upsert_source("p2p:10.0.0.2:18233", **zakura, discovered_via="fleet", tip_hash=tree.hash("c10"),
                            status="connected", last_ok_at=now)
        store.upsert_source("p2p:10.0.0.9:18233", **zakura, discovered_via="fleet", status="backoff", **old)
        store.upsert_source("p2p:10.0.0.5:18233", **zakura, tip_hash=tree.hash("c4"), status="unreachable",
                            last_ok_at=now - 100_000, first_seen_at=now - 200_000)
        monitor = self.monitor(tree.chain, config)
        views = {v["source"]: v for v in analysis.peers(monitor, now)}
        self.assertEqual(set(views), {"rpc:z1", "p2p:10.0.0.2:18233", "p2p:10.0.0.5:18233"})
        silent = views["p2p:10.0.0.5:18233"]
        self.assertEqual((silent["stale"], silent["state"], silent["last_seen_age_s"]), (True, "inactive", 100_000.0))
        self.assertEqual((views["rpc:z1"]["host"], views["p2p:10.0.0.2:18233"]["host"]), ("10.0.0.2", "10.0.0.2"))
        group = {g["key"]: g for g in analysis.live_snapshot(monitor, now)["groups"]}["zakura 1.6"]
        self.assertEqual((group["members"], group["seen_24h"], group["stale"], group["active"], group["fleet"]),
                         (2, 2, 1, 2, 1))
        self.assertEqual(sum(group["states"].values()), 2)
        sources = analysis.summary(tree.chain, self.conn(), now, config=config)["sources"]
        self.assertEqual(([row["source"] for row in sources["rpc"]], sources["p2p"]["total"]), (["rpc:z1"], 2))
        self.assertEqual(len(analysis.summary(tree.chain, self.conn(), now)["sources"]["rpc"]), 2)
        # A fleet configured by hostname cannot be matched to IP-keyed P2P rows, so none are hidden.
        named = parse_config({"rpc": [{"name": "z1", "url": "http://node1.example.org:18232/", "fleet": True}]})
        views = {v["source"] for v in analysis.peers(self.monitor(tree.chain, named), now)}
        self.assertIn("p2p:10.0.0.9:18233", views)

    def test_missing_parents_near_the_tip_only(self) -> None:
        """Only missing parents the body sweep still requests are reported."""
        tree = Tree()
        tree.add("g", "pre", dt=0, height=BASE)
        tree.line("c", "g", 1, 20)
        tree.chain.add(header(bhash("near"), bhash("near-parent"), T0 + 500, NORMAL), BASE + 19)
        tree.chain.add(header(bhash("deep"), bhash("deep-parent"), T0 + 100, NORMAL), BASE + 5)
        with mock.patch.object(analysis, "MISSING_PARENT_DEPTH", 10):
            snap = analysis.live_snapshot(self.monitor(tree.chain), T0 + 1_000)
        self.assertEqual((len(tree.chain.missing_parents()), snap["chain"]["missing_parents"]), (2, 1))

    def test_stored_tips_outside_the_chain_are_placed(self) -> None:
        """Tips below the window or left out of the chain are related through their stored ancestors."""
        now = T0 + 10_000.0
        store = self.store

        def put(name: str, parent: str, height: int, at: int) -> None:
            """Store a header-only block first seen 1 s after its header time."""
            store.upsert_block(None, header(bhash(name), bhash(parent), at, NORMAL), height, miner=None,
                               is_min_diff=False, seen_at=at + 1.0, seen_source="test")

        for index in range(31):
            put(f"c{index}", f"c{index - 1}" if index else "pre", BASE + index, T0 + 75 * index)
        for index in (6, 7, 8):
            put(f"d{index}", f"d{index - 1}" if index > 6 else "c5", BASE + index, T0 + 75 * index + 10)
        put("x3", "xp", BASE + 3, T0 + 240)  # its parent is not stored
        store.commit_if_due(force=True)
        chain = Chain.from_store(TESTNET, store, min_height=BASE + 20)
        put("z23", "c22", BASE + 23, T0 + 75 * 23 + 10)  # stored, but not in the chain
        put("z24", "z23", BASE + 24, T0 + 75 * 24 + 10)
        tips = {"10.0.0.50": "d8", "10.0.0.51": "c10", "10.0.0.52": "x3", "10.0.0.53": "z24"}
        for ip, name in tips.items():
            store.upsert_source(f"p2p:{ip}:18233", impl="zebra", impl_version="6.4.2", tip_hash=bhash(name),
                                tip_height=1, status="connected", last_ok_at=now)
        for ip, start in (("10.0.0.54", BASE + 4), ("10.0.0.55", BASE + 25)):  # no tip learned
            store.upsert_source(f"p2p:{ip}:18233", impl="zebra", impl_version="6.4.2", start_height=start,
                                status="connected", last_ok_at=now)

        def relations(at: float) -> dict:
            """Return {tip name: peer view} at time `at`."""
            views = {v["source"]: v for v in analysis.peers(self.monitor(chain), at)}
            return {name: views[f"p2p:{ip}:18233"] for ip, name in tips.items()}

        views = relations(now)
        dead = views["d8"]
        self.assertEqual(dead["relation"], {"kind": "fork", "n": 3, "fork_hash": bhash("c5"), "fork_height": BASE + 5,
                                            "depth_ours": 25, "depth_theirs": 3, "tip_height": BASE + 8})
        self.assertEqual((dead["tip_height"], dead["state"], dead["tip_age_s"]),
                         (BASE + 8, "stuck", now - (T0 + 75 * 8 + 11)))
        behind = views["c10"]["relation"]
        self.assertEqual((behind["kind"], behind["n"], behind["fork_height"]), ("behind", 20, BASE + 10))
        self.assertEqual(views["x3"]["relation"]["kind"], "below_window")
        tipless = {v["source"]: v["relation"] for v in analysis.peers(self.monitor(chain), now)}
        # Without a tip, a version height below the window places the peer there; one inside it says nothing.
        self.assertEqual((tipless["p2p:10.0.0.54:18233"]["kind"], tipless["p2p:10.0.0.54:18233"]["tip_height"]),
                         ("below_window", BASE + 4))
        self.assertEqual(tipless["p2p:10.0.0.55:18233"]["kind"], "unknown")
        side = views["z24"]["relation"]
        self.assertEqual((side["kind"], side["fork_height"], side["n"], side["depth_ours"]), ("fork", BASE + 22, 2, 8))
        put("xp", "c1", BASE + 2, T0 + 160)
        self.assertEqual(relations(now + 1)["x3"]["relation"]["kind"], "below_window")  # cached
        placed = relations(now + analysis.PLACE_RETRY + 1)["x3"]["relation"]
        self.assertEqual((placed["kind"], placed["fork_height"], placed["n"]), ("fork", BASE + 1, 2))


if __name__ == "__main__":
    unittest.main()
