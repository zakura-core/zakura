"""Tests for the Monitor (collector hooks, split tracking, snapshots, orchestration) and the CLI."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import io
import json
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from tests.test_rpc import BASE, StubNode, StubServer, block_hash, build_chain, make_block
from zakura_fork_monitor import __main__ as cli
from zakura_fork_monitor import analysis, consensus, service
from zakura_fork_monitor.config import parse_config
from zakura_fork_monitor.consensus import TESTNET, Block, BlockHeader, Coinbase, parse_block
from zakura_fork_monitor.service import Monitor, stored_top
from zakura_fork_monitor.store import Store

T0 = 1_790_000_000
NORMAL = 0x1E0D94E6
PEER = "p2p:203.0.113.7:18233"
RPC = "rpc:node"


def setUpModule() -> None:
    """Synthetic blocks carry no real Equihash solution, so only their targets are checked."""
    patcher = mock.patch.object(consensus, "check_equihash", return_value=True)
    patcher.start()
    unittest.addModuleCleanup(patcher.stop)


def bhash(name: str) -> str:
    """Derive a display hash from a name."""
    return hashlib.sha256(name.encode()).hexdigest()


def header(name: str, parent: str, time: int, bits: int = NORMAL) -> BlockHeader:
    """Build a synthetic header named `name` on the block named `parent`."""
    return BlockHeader(hash=bhash(name), prev_hash=bhash(parent), version=4, merkle_root="00" * 32, time=time,
                       bits=bits, nonce="00" * 32, raw=b"")


def with_body(hdr: BlockHeader, tag: str) -> Block:
    """Wrap a synthetic header in a block whose coinbase carries `tag`."""
    coinbase = Coinbase(height=None, script_sig=b"", template=None, tag=tag, extranonce="", payouts=(), tx_version=6)
    return Block(header=hdr, size=1_000, tx_count=1, coinbase=coinbase)


def make_config(**overrides: Any) -> Any:
    """A validated config with no RPC endpoints, P2P on and CipherScan off, plus TOML-shaped overrides."""
    data: dict[str, Any] = {"cipherscan": {"enabled": False}, "http": {"host": "127.0.0.1", "port": 0}}
    data.update(overrides)
    return parse_config(data)


class FakeP2P:
    """Records the calls the Monitor forwards to the P2P observer."""

    def __init__(self) -> None:
        """Start with no calls."""
        self.requested: list[tuple[str, int | None, str | None]] = []
        self.candidates: list[Any] = []
        self.accept = True

    def request_block(self, hash: str, height_hint: int | None = None, prefer_host: str | None = None) -> bool:
        """Record a body request; accepted unless `accept` is False."""
        self.requested.append((hash, height_hint, prefer_host))
        return self.accept

    def add_candidates(self, entries) -> int:
        """Record peer candidates."""
        entries = list(entries)
        self.candidates.extend(entries)
        return len(entries)


class MonitorCase(unittest.TestCase):
    """A Monitor over a temporary store."""

    def setUp(self) -> None:
        """Open a store and a monitor."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "monitor.sqlite3"
        self.store = Store(self.path, commit_interval=0)
        self.addCleanup(self.store.close)
        self.monitor = Monitor(make_config(), self.store)

    def line(self, names: list[str], parent: str = "genesis", height: int = BASE, source: str = RPC) -> None:
        """Ingest header-only blocks `names` in a line on `parent`, the first at `height`."""
        for index, name in enumerate(names):
            hint = height if index == 0 else None
            self.monitor.ingest_block(None, header(name, parent, T0 + 60 * index), hint, source=source, kind="rpc_tip",
                                      at=T0 + 60 * index + 1.0)
            parent = name

    def rows(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        """Run a query on committed data."""
        self.store.commit_if_due(force=True)
        return [dict(row) for row in self.store.reader().execute(sql, params)]


class HookTests(MonitorCase):
    """ingest_*, record_sighting, observe_tip and the P2P pass-throughs."""

    def test_ingest_block_writes_chain_store_and_sighting(self) -> None:
        """A block lands in the chain and the blocks table with its height, plus a sighting of its source."""
        self.line(["a", "b"])
        self.assertEqual(self.monitor.chain.best_tip().hash, bhash("b"))
        blocks = self.rows("SELECT hash, height, body FROM blocks ORDER BY height")
        self.assertEqual([(r["hash"], r["height"], r["body"]) for r in blocks], [(bhash("a"), BASE, 0),
                                                                                  (bhash("b"), BASE + 1, 0)])
        sightings = self.rows("SELECT source, kind FROM sightings WHERE hash = ?", bhash("b"))
        self.assertEqual(sightings, [{"source": RPC, "kind": "rpc_tip"}])

    def test_announcement_before_ingest_is_first_seen(self) -> None:
        """An inv recorded before the headers arrive sets first_seen in both the chain and the store."""
        self.line(["a"])
        self.assertTrue(self.monitor.record_sighting(bhash("b"), PEER, "inv", T0 + 100.0))
        self.monitor.ingest_headers([header("b", "a", T0 + 95)], PEER, T0 + 102.5)
        self.assertEqual(self.monitor.chain.get(bhash("b")).first_seen_at, T0 + 100.0)
        row = self.rows("SELECT first_seen_at, first_seen_source FROM blocks WHERE hash = ?", bhash("b"))[0]
        self.assertEqual((row["first_seen_at"], row["first_seen_source"]), (T0 + 100.0, PEER))
        kinds = {r["kind"] for r in self.rows("SELECT kind FROM sightings WHERE hash = ?", bhash("b"))}
        self.assertEqual(kinds, {"inv"})  # the earlier inv sighting is kept

    def test_sightings_of_unknown_blocks_wait_in_memory(self) -> None:
        """Announcements of a hash that never becomes a block never reach the store; the sources kept are bounded."""
        self.line(["a"])
        self.assertTrue(self.monitor.record_sighting(bhash("junk"), PEER, "inv", T0 + 10.0))
        self.assertFalse(self.monitor.record_sighting(bhash("junk"), PEER, "inv", T0 + 5.0))
        self.assertEqual(self.rows("SELECT hash FROM sightings WHERE hash = ?", bhash("junk")), [])
        with mock.patch.object(service, "MAX_EARLY_SOURCES", 2):
            self.assertTrue(self.monitor.record_sighting(bhash("b"), "p2p:192.0.2.1:18233", "inv", T0 + 101.0))
            self.assertTrue(self.monitor.record_sighting(bhash("b"), "p2p:192.0.2.2:18233", "inv", T0 + 100.0))
            self.assertFalse(self.monitor.record_sighting(bhash("b"), "p2p:192.0.2.3:18233", "inv", T0 + 99.0))
        self.monitor.ingest_headers([header("b", "a", T0 + 95)], PEER, T0 + 102.5)
        self.assertEqual(self.monitor.chain.get(bhash("b")).first_seen_at, T0 + 100.0)
        rows = self.rows("SELECT source, at FROM sightings WHERE hash = ? ORDER BY at", bhash("b"))
        self.assertEqual([(r["source"], r["at"]) for r in rows],
                         [("p2p:192.0.2.2:18233", T0 + 100.0), ("p2p:192.0.2.1:18233", T0 + 101.0),
                          (PEER, T0 + 102.5)])
        self.assertTrue(self.monitor.record_sighting(bhash("b"), "p2p:192.0.2.3:18233", "inv", T0 + 103.0))
        self.assertEqual(len(self.rows("SELECT source FROM sightings WHERE hash = ?", bhash("b"))), 4)

    def test_unplaced_bodies_keep_only_the_height_hint(self) -> None:
        """A body the chain refuses (detached far above the tip) is stored without its unverified BIP34 height."""
        self.line(["a"])
        hdr = header("forged", "nowhere", T0 + 60)
        block = dataclasses.replace(with_body(hdr, "x"), coinbase=dataclasses.replace(
            with_body(hdr, "x").coinbase, height=2_000_000_000))
        self.assertIsNone(self.monitor.ingest_block(block, hdr, None, source=PEER, kind="getdata", at=T0 + 61.0,
                                                    trusted=False))
        row = self.rows("SELECT height, body FROM blocks WHERE hash = ?", bhash("forged"))
        self.assertEqual(row, [{"height": None, "body": 1}])

    def test_backfilled_blocks_have_no_first_seen_time(self) -> None:
        """A backfill's fetch time is no arrival time: the tie-break stays unknown, also after a restart."""
        monitor = self.monitor
        self.line(["a"])
        monitor.ingest_block(None, header("b", "a", T0 + 60), None, source=RPC, kind="backfill", at=T0 + 5_000.0)
        monitor.ingest_block(None, header("b2", "a", T0 + 61), None, source=PEER, kind="getdata", at=T0 + 5_001.0)
        monitor.ingest_block(None, header("c", "b", T0 + 120), None, source=RPC, kind="backfill", at=T0 + 5_000.0)
        self.assertIsNone(monitor.chain.get(bhash("b")).first_seen_at)
        row = self.rows("SELECT first_seen_at, first_seen_source FROM blocks WHERE hash = ?", bhash("b"))[0]
        self.assertEqual(row, {"first_seen_at": None, "first_seen_source": None})
        for chain in (monitor.chain, Monitor(make_config(), self.store).chain):
            [event] = analysis.fork_events(chain)
            self.assertEqual(event["winner"]["hash"], bhash("b"))
            self.assertIsNone(event["winner_first_seen"])
            self.assertIn(event["tiebreak"], ("hash", "unknown"))
        # A live sighting is a real (if late) observation and fills it in.
        monitor.ingest_block(None, header("b", "a", T0 + 60), None, source=PEER, kind="headers", at=T0 + 6_000.0)
        self.assertEqual(monitor.chain.get(bhash("b")).first_seen_at, T0 + 6_000.0)

    def test_backfilled_resets_have_no_forward_dating(self) -> None:
        """A reset block from the backfill reports unknown forward-dating rather than minus the fetch delay."""
        height = TESTNET.min_diff_after_height + 10
        monitor = self.monitor
        monitor.ingest_block(None, header("p", "g", T0), height, source=RPC, kind="backfill", at=T0 + 9_000.0)
        monitor.ingest_block(None, header("r", "p", T0 + 900, bits=TESTNET.pow_limit_bits), None, source=RPC,
                             kind="backfill", at=T0 + 9_000.0)
        monitor.ingest_block(None, header("n", "r", T0 + 960), None, source=RPC, kind="rpc_tip", at=T0 + 9_001.0)
        [reset] = analysis.resets(monitor.chain)
        self.assertEqual(reset["hash"], bhash("r"))
        self.assertIsNone(reset["forward_dating"])

    def test_observe_tip_records_changes_and_reorgs(self) -> None:
        """Tip moves become tip_changes rows (with reorg depth) and update the source's tip fields."""
        self.line(["a", "b", "c"])
        self.line(["x", "y"], parent="b", height=BASE + 2)  # heavier branch after b
        monitor = self.monitor
        self.assertIsNotNone(monitor.observe_tip(RPC, bhash("c"), T0 + 200.0))
        self.assertIsNone(monitor.observe_tip(RPC, bhash("c"), T0 + 201.0))
        change = monitor.observe_tip(RPC, bhash("y"), T0 + 202.0)
        self.assertTrue(change.is_reorg)
        rows = self.rows("SELECT new_hash, is_reorg, disconnected, connected, fork_height FROM tip_changes ORDER BY id")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1], {"new_hash": bhash("y"), "is_reorg": 1, "disconnected": 1, "connected": 2,
                                   "fork_height": BASE + 1})
        source = self.rows("SELECT tip_hash, tip_height, tip_via FROM sources WHERE source = ?", RPC)[0]
        self.assertEqual(source, {"tip_hash": bhash("y"), "tip_height": BASE + 3, "tip_via": "rpc:getbestblockhash"})

    def test_unknown_tip_uses_the_height_hint(self) -> None:
        """A tip the chain does not hold is stored with the caller's height hint."""
        self.monitor.observe_tip(PEER, bhash("elsewhere"), T0, height_hint=BASE + 50)
        row = self.rows("SELECT new_height, fork_hash, is_reorg FROM tip_changes")[0]
        self.assertEqual(row, {"new_height": BASE + 50, "fork_hash": None, "is_reorg": 0})

    def test_restart_reloads_chain_and_tips(self) -> None:
        """A new Monitor on the same store rebuilds the chain and records no spurious tip change."""
        self.line(["a", "b", "c"])
        self.monitor.observe_tip(RPC, bhash("c"), T0 + 200.0)
        self.monitor.persist_best()
        self.store.commit_if_due(force=True)
        self.assertEqual(stored_top(self.store), BASE + 2)
        restarted = Monitor(make_config(), self.store)
        self.assertEqual(restarted.chain.best_tip().hash, bhash("c"))
        self.assertIsNone(restarted.observe_tip(RPC, bhash("c"), T0 + 300.0))
        self.assertEqual(len(self.rows("SELECT id FROM tip_changes")), 1)

    def test_side_branch_headers_request_bodies_once(self) -> None:
        """A header-only block off the canonical chain is fetched over P2P, and only once per retry period."""
        self.monitor.p2p = fake = FakeP2P()
        self.line(["a", "b", "c"])
        self.monitor.ingest_headers([header("s", "a", T0 + 61)], PEER, T0 + 70.0)
        self.monitor.ingest_headers([header("s", "a", T0 + 61)], "p2p:198.51.100.1:18233", T0 + 71.0)
        self.assertEqual(fake.requested, [(bhash("s"), BASE + 1, "203.0.113.7")])  # the peer that sent it

    def test_p2p_pass_throughs(self) -> None:
        """Candidates and block requests reach the observer, and are no-ops without one."""
        self.assertEqual(self.monitor.add_peer_candidates([("203.0.113.9", 18233, "/Zebra:6.4.2/", "x")]), 0)
        self.assertFalse(self.monitor.request_block(bhash("a"), 5))
        self.monitor.p2p = fake = FakeP2P()
        self.assertEqual(self.monitor.add_peer_candidates([("203.0.113.9", 18233, "/Zebra:6.4.2/", "x")]), 1)
        self.assertTrue(self.monitor.request_block(bhash("a"), 5, prefer_host="192.0.2.1"))
        self.assertEqual(fake.requested, [(bhash("a"), 5, "192.0.2.1")])


class BodySweepTests(MonitorCase):
    """fetch_missing_bodies routes header-only blocks near the tip."""

    def test_canonical_without_rpc_and_side_blocks_go_to_p2p(self) -> None:
        """With no healthy RPC endpoint every missing body is requested over P2P, each at most once per retry period."""
        self.monitor.p2p = fake = FakeP2P()
        self.line(["a", "b", "c"])
        self.monitor.ingest_block(None, header("s", "a", T0 + 61), None, source=PEER, kind="headers", at=T0 + 130.0)
        self.assertEqual(fake.requested, [(bhash("s"), BASE + 1, "203.0.113.7")])
        asyncio.run(self.monitor.fetch_missing_bodies(T0 + 140.0))
        self.assertEqual(len(fake.requested), 4)  # a, b and c; s waits for its retry period
        fake.requested.clear()
        now = T0 + 140.0 + service.BODY_RETRY
        asyncio.run(self.monitor.fetch_missing_bodies(now))
        self.assertEqual({hash for hash, _, _ in fake.requested}, {bhash(name) for name in ("a", "b", "c", "s")})
        fake.requested.clear()
        asyncio.run(self.monitor.fetch_missing_bodies(now + 1.0))
        self.assertEqual(fake.requested, [])

    def test_refused_requests_cost_no_attempt(self) -> None:
        """A request the observer refuses (e.g. its fetch table is full) is retried on the next sweep."""
        self.monitor.p2p = fake = FakeP2P()
        fake.accept = False
        self.line(["a", "b"])
        self.monitor.ingest_block(None, header("s", "a", T0 + 61), None, source=PEER, kind="headers", at=T0 + 130.0)
        for step in range(service.BODY_ATTEMPTS + 1):
            asyncio.run(self.monitor.fetch_missing_bodies(T0 + 140.0 + step))
        self.assertEqual([hash for hash, _, _ in fake.requested].count(bhash("s")), service.BODY_ATTEMPTS + 2)
        fake.accept = True
        fake.requested.clear()
        asyncio.run(self.monitor.fetch_missing_bodies(T0 + 150.0))
        self.assertIn(bhash("s"), [hash for hash, _, _ in fake.requested])

    def test_missing_parents_of_side_branches_are_requested(self) -> None:
        """The missing parent of a detached side block (a multi-block fork tip) is fetched over P2P."""
        self.monitor.p2p = fake = FakeP2P()
        self.line(["a", "b", "c"])
        self.monitor.ingest_block(None, header("t", "gone", T0 + 70), BASE + 2, source=PEER, kind="getdata",
                                  at=T0 + 71.0)
        asyncio.run(self.monitor.fetch_missing_bodies(T0 + 1_000.0))
        self.assertIn((bhash("gone"), BASE + 1, None), fake.requested)

    def test_untrusted_bodies_are_fetched_again(self) -> None:
        """Bodies from peers outside the fleet are fetched again, but not from the peer that showed them."""
        self.monitor.p2p = fake = FakeP2P()
        self.line(["a", "b"])
        for name, parent, trusted in (("c", "b", False), ("s", "a", False), ("d", "c", True)):
            hdr = header(name, parent, T0 + 200)
            self.monitor.record_sighting(hdr.hash, PEER, "inv", T0 + 201.0)
            self.monitor.ingest_block(with_body(hdr, "forged"), hdr, None, source=PEER, kind="getdata",
                                      at=T0 + 202.0, trusted=trusted)
        self.assertEqual(self.rows("SELECT body_trusted FROM blocks WHERE hash = ?", bhash("c")),
                         [{"body_trusted": 0}])
        fake.requested.clear()
        asyncio.run(self.monitor.fetch_missing_bodies(T0 + 1_000.0))
        requested = {block_hash: prefer for block_hash, _, prefer in fake.requested}
        self.assertEqual(set(requested), {bhash(name) for name in ("a", "b", "c", "s")})
        self.assertEqual((requested[bhash("c")], requested[bhash("s")]), (None, None))

    def test_only_the_fetched_batch_spends_attempts(self) -> None:
        """Canonical blocks past BODY_BATCH keep their attempts for the next sweep."""
        self.monitor.p2p = fake = FakeP2P()
        self.line([f"b{i}" for i in range(service.BODY_BATCH + 5)])
        asyncio.run(self.monitor.fetch_missing_bodies(T0 + 10_000.0))
        self.assertEqual(len(fake.requested), service.BODY_BATCH)
        fake.requested.clear()
        asyncio.run(self.monitor.fetch_missing_bodies(T0 + 10_001.0))
        self.assertEqual(len(fake.requested), 5)

    def test_fresh_blocks_wait_for_the_rpc_tip_poll(self) -> None:
        """Blocks first seen within BODY_GRACE are left to the RPC tip poll."""
        self.monitor.p2p = fake = FakeP2P()
        self.line(["a"])
        asyncio.run(self.monitor.fetch_missing_bodies(T0 + 1.0 + service.BODY_GRACE / 2))
        self.assertEqual(fake.requested, [])


def candidate(*sides: tuple[str, int]) -> dict[str, Any]:
    """A detect_split-shaped candidate whose sides are (branch key, tip height) pairs."""
    groups = [f"group {index}" for index in range(len(sides))]
    return {
        "key": "root", "fork_hash": None, "fork_height": None, "depth": None,
        "sides": [{"branch": branch, "tip_hash": None, "tip_height": tip, "blocks_past_fork": None,
                   "groups": [group], "members": 2} for (branch, tip), group in zip(sides, groups, strict=True)],
        "groups": {group: branch for (branch, _), group in zip(sides, groups, strict=True)}, "neutral_groups": [],
    }


class SplitTests(MonitorCase):
    """The split state machine over a best chain a..e (BASE..BASE+4) with side branches."""

    def setUp(self) -> None:
        """Build the best chain, a two-block branch x on b, another z on c and a one-block orphan y on d."""
        super().setUp()
        self.line(["a", "b", "c", "d", "e"])
        self.line(["x1", "x2"], parent="b", height=BASE + 2)
        self.line(["z1", "z2"], parent="c", height=BASE + 3)
        self.line(["y1"], parent="d", height=BASE + 4)
        self.x = candidate(("canonical", BASE + 4), (bhash("x1"), BASE + 3))
        self.z = candidate(("canonical", BASE + 4), (bhash("z1"), BASE + 4))

    def splits(self) -> list[dict[str, Any]]:
        """Return stored split events with decoded summaries."""
        rows = self.rows("SELECT * FROM split_events ORDER BY id")
        for row in rows:
            row["summary"] = json.loads(row["summary"])
        return rows

    def test_split_fork_needs_two_blocks_on_both_sides(self) -> None:
        """A two-block side branch against a best chain two past its fork qualifies; orphans and lag do not."""
        split_fork = self.monitor.split_fork
        self.assertEqual(split_fork(self.x), (bhash("b"), BASE + 1, 3))
        self.assertIsNone(split_fork(candidate(("canonical", BASE + 4), (bhash("y1"), BASE + 4))))
        self.assertIsNone(split_fork(candidate(("canonical", BASE + 2), (bhash("x1"), BASE + 3))))
        self.assertIsNone(split_fork(candidate(("canonical", BASE + 4), (bhash("unknown"), BASE + 9))))
        both = candidate((bhash("x1"), BASE + 3), (bhash("z1"), BASE + 4))
        self.assertEqual(split_fork(both), (bhash("b"), BASE + 1, 2))

    def test_opens_after_persisting_and_closes_after_clearing(self) -> None:
        """An event opens once the split lasted 30 s and ends when it has been gone for 30 s."""
        track = self.monitor.track_split
        track(self.x, 1_000.0)
        self.assertIsNone(track(self.x, 1_029.0).event_id)
        self.assertEqual(self.splits(), [])
        state = track(self.x, 1_030.0)
        self.assertIsNotNone(state.event_id)
        track(None, 1_040.0)
        track(self.x, 1_050.0)  # back before the clear period ended
        track(None, 1_060.0)
        self.assertIsNotNone(track(None, 1_089.0))
        self.assertIsNone(track(None, 1_090.0))
        [event] = self.splits()
        self.assertEqual((event["started_at"], event["ended_at"], event["fork_height"], event["fork_hash"]),
                         (1_000.0, 1_060.0, BASE + 1, bhash("b")))
        summary = event["summary"]
        self.assertEqual((summary["closed_by"], summary["max_depth"], summary["candidate"]["sides"][1]["branch"]),
                         ("resolved", 3, bhash("x1")))

    def test_brief_or_orphan_candidates_never_open(self) -> None:
        """A split gone before 30 s, or a peer lagging on a one-block orphan, records nothing."""
        track = self.monitor.track_split
        track(self.x, 0.0)
        self.assertIsNone(track(None, 10.0))
        orphan = candidate(("canonical", BASE + 4), (bhash("y1"), BASE + 4))
        self.assertIsNone(track(orphan, 20.0))
        self.assertIsNone(track(orphan, 60.0))
        self.assertEqual(self.splits(), [])

    def test_new_fork_point_supersedes_the_open_event(self) -> None:
        """A different fork point closes the open event and starts tracking the new one."""
        track = self.monitor.track_split
        track(self.x, 0.0)
        track(self.x, 40.0)
        state = track(self.z, 50.0)
        self.assertEqual((state.fork_hash, state.event_id), (bhash("c"), None))
        [event] = self.splits()
        self.assertEqual((event["ended_at"], event["summary"]["closed_by"]), (50.0, "superseded"))

    def test_leftover_open_events_are_closed(self) -> None:
        """Events left open by a crash are closed with closed_by=restart."""
        self.store.open_split(started_at=5.0, summary={"key": "old"})
        self.assertEqual(self.monitor.close_stale_splits(99.0, "restart"), 1)
        [event] = self.splits()
        self.assertEqual((event["ended_at"], event["summary"]), (99.0, {"key": "old", "closed_by": "restart"}))


class SnapshotTests(MonitorCase):
    """refresh_snapshot."""

    def test_snapshot_is_published_json_and_annotated(self) -> None:
        """The published snapshot is strict JSON with the service's extra keys, and the best tip is persisted."""
        self.line(["a", "b"])
        self.monitor.observe_tip(RPC, bhash("b"), T0 + 100.0)
        snap = self.monitor.refresh_snapshot(T0 + 120.0)
        self.assertIs(self.monitor.snapshot, snap)
        json.dumps(snap, allow_nan=False)
        self.assertEqual(snap["tip"]["hash"], bhash("b"))
        self.assertIsNone(snap["split_event"])
        self.assertIsNone(snap["collectors"]["cipherscan"])
        self.assertEqual(snap["service"]["version"], "0.1.0")
        self.assertEqual(self.store.get_meta("best_hash"), bhash("b"))
        self.assertEqual(stored_top(self.store), BASE + 1)


    def test_peer_without_a_common_block_is_stuck(self) -> None:
        """A peer whose tip is unknown but whose version height is far behind counts as stuck."""
        self.line(["a", "b"])
        self.store.upsert_source(PEER, kind="p2p", impl="zebra", impl_version="6.0.0", status="connected",
                                 start_height=BASE - 5_000, last_ok_at=T0 + 100.0)
        snap = self.monitor.refresh_snapshot(T0 + 120.0)
        [view] = [v for v in snap["stuck"] if v["source"] == PEER]
        self.assertEqual((view["state"], view["behind"], view["tip_height"]), ("stuck", 5_001, None))


    def test_previous_run_statuses_become_idle(self) -> None:
        """Rows left "connected"/"ok" by an earlier run are reset, so a vanished peer eventually goes inactive."""
        self.line(["a"])
        self.store.upsert_source(PEER, kind="p2p", impl="zebra", impl_version="6.4.2", status="connected",
                                 last_ok_at=T0, tip_hash=bhash("a"), tip_height=BASE, tip_at=T0)
        self.store.upsert_source(RPC, kind="rpc", status="error")
        self.assertEqual(self.monitor.reset_stale_statuses(), 1)
        snap = self.monitor.refresh_snapshot(T0 + analysis.ACTIVE_WINDOW + 1.0)
        [view] = [v for v in snap["peers"] if v["source"] == PEER]
        self.assertEqual((view["status"], view["active"], view["state"]), ("idle", False, "inactive"))


class LoopTests(unittest.IsolatedAsyncioTestCase):
    """The snapshot and prune loops."""

    async def asyncSetUp(self) -> None:
        """Open a store and a monitor."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = Store(Path(tmp.name) / "monitor.sqlite3", commit_interval=0)
        self.addCleanup(self.store.close)
        self.monitor = Monitor(make_config(), self.store)

    async def test_snapshot_survives_a_backward_clock_step(self) -> None:
        """Refreshes are throttled on the monotonic clock, so stepping the wall clock back does not pause them."""
        offset = 0.0
        clock = SimpleNamespace(time=lambda: time.time() + offset, monotonic=time.monotonic)
        stamps: list[float] = []
        patches = (mock.patch.object(service, "time", clock), mock.patch.object(service, "TICK", 0.01),
                   mock.patch.object(service, "SNAPSHOT_INTERVAL", 0.02),
                   mock.patch.object(self.monitor, "refresh_snapshot",
                                     side_effect=lambda now: stamps.append(now) or {}))
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            task = asyncio.create_task(self.monitor._snapshot_loop())
            await asyncio.sleep(0.1)
            offset, before = -600.0, len(stamps)
            await asyncio.sleep(0.2)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.assertGreater(len(stamps) - before, 3)
        self.assertLess(stamps[-1], time.time() - 500)  # refreshed with the stepped wall clock

    async def test_prune_runs_soon_after_start_and_yields_between_batches(self) -> None:
        """The first prune comes PRUNE_START_DELAY after start, and other tasks run between its store batches."""
        for i in range(10):
            self.store.record_sighting(f"{i:064x}", PEER, "inv", float(i))
        ticks = 0

        async def heartbeat() -> None:
            """Count event-loop turns."""
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0)

        at_batch: list[int] = []
        real = self.store.prune_batches

        def small_batches(before: float):
            """Prune two rows at a time, noting the heartbeat count at each batch."""
            for step in real(before, batch=2):
                at_batch.append(ticks)
                yield step

        beat = asyncio.create_task(heartbeat())
        with mock.patch.object(service, "PRUNE_START_DELAY", 0.0), mock.patch.object(service, "PRUNE_PAUSE", 0.0), \
                mock.patch.object(self.store, "prune_batches", small_batches):
            task = asyncio.create_task(self.monitor._prune_loop())
            for _ in range(200):
                if len(at_batch) == 9:  # 5 full and 1 short sightings batch, then probes, tip_changes, chaintips
                    break
                await asyncio.sleep(0.01)
            task.cancel()
            beat.cancel()
            await asyncio.gather(task, beat, return_exceptions=True)
        self.assertEqual(len(at_batch), 9)
        self.assertEqual(len(set(at_batch)), len(at_batch))  # the loop turned between every batch
        self.assertEqual(self.store.reader().execute("SELECT COUNT(*) FROM sightings").fetchone()[0], 0)


class RunTests(unittest.IsolatedAsyncioTestCase):
    """run() against a stub RPC node: backfill, collectors, snapshot, dashboard and shutdown."""

    async def asyncSetUp(self) -> None:
        """Start a stub node with a 5-block chain and a monitor configured for it."""
        self.raws = build_chain(5)
        self.hashes = [block_hash(raw) for raw in self.raws]
        self.node = StubNode(self.raws)
        self.server = StubServer(self.node)
        self.addCleanup(self.server.close)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = Store(Path(tmp.name) / "monitor.sqlite3", commit_interval=0.05)
        self.addCleanup(self.store.close)
        self.config = make_config(
            rpc=[{"name": "node", "url": self.server.url, "interval": 0.1, "chaintips_interval": 0.2,
                  "timeout": 5.0}],
            p2p={"enabled": False},
            chain={"backfill_blocks": 3, "memory_window": 1_000},
        )
        patches = [mock.patch.object(service, "TICK", 0.05), mock.patch.object(service, "SNAPSHOT_INTERVAL", 0.1)]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    async def wait_for(self, predicate, timeout: float = 10.0) -> None:
        """Poll `predicate` until it holds."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not predicate():
            if loop.time() > deadline:
                self.fail("condition not reached in time")
            await asyncio.sleep(0.05)

    async def test_run_serves_tracks_reorgs_and_stops(self) -> None:
        """The service backfills, follows the tip and a reorg, serves the API and shuts down on `stop`."""
        monitor = Monitor(self.config, self.store)
        stop = asyncio.Event()
        task = asyncio.create_task(monitor.run(stop=stop))
        await self.wait_for(lambda: monitor.snapshot is not None and monitor.snapshot["tip"] is not None
                            and monitor.snapshot["tip"]["hash"] == self.hashes[-1])
        self.assertEqual(monitor.backfill_result["fetched"], 3)
        host, port = monitor.http_address
        health = await asyncio.to_thread(_get_json, f"http://{host}:{port}/healthz")
        self.assertEqual((health["status"], health["tip_height"]), ("ok", BASE + 4))
        summary = await asyncio.to_thread(_get_json, f"http://{host}:{port}/api/summary")
        self.assertEqual(summary["tip"]["height"], BASE + 4)

        # Reorg: the node replaces its tip with a two-block branch from height BASE+3.
        branch = [make_block(self.hashes[3], BASE + 4, 1_790_001_000, tag=b"Foundry")]
        branch.append(make_block(block_hash(branch[0]), BASE + 5, 1_790_001_060, tag=b"Foundry"))
        self.node.set_best(self.raws[:4] + branch)
        await self.wait_for(lambda: monitor.chain.best_tip().hash == block_hash(branch[1]))
        stop.set()
        await asyncio.wait_for(task, 10.0)
        rows = self.store.reader().execute(
            "SELECT is_reorg, disconnected, connected FROM tip_changes WHERE source = 'rpc:node' ORDER BY id"
        ).fetchall()
        self.assertEqual([tuple(row) for row in rows][-1], (1, 1, 2))
        self.assertEqual(monitor.tasks["rpc:node"], "stopped")
        with self.assertRaises(OSError):
            await asyncio.to_thread(_get_json, f"http://{host}:{port}/healthz")

    async def test_backfill_bridges_downtime_and_links_the_chain(self) -> None:
        """After downtime longer than backfill_blocks, the backfill reaches the stored tip and the chain links up."""
        first = Monitor(self.config, self.store)
        block = parse_block(bytes.fromhex(self.raws[0]), TESTNET)
        first.ingest_block(block, block.header, BASE, source="rpc:node", kind="rpc_tip", at=1.0)
        first.persist_best()
        self.store.commit_if_due(force=True)
        config = dataclasses.replace(self.config, chain=dataclasses.replace(self.config.chain, backfill_blocks=1))
        monitor = Monitor(config, self.store)
        result = await monitor.backfill(1)
        self.assertEqual(result["fetched"], 4)
        best = monitor.chain.best_tip()
        self.assertEqual((best.hash, best.height), (self.hashes[-1], BASE + 4))
        self.assertEqual([n.hash for n in monitor.chain.canonical_nodes()], self.hashes)

    async def test_stop_during_backfill(self) -> None:
        """Setting `stop` while the backfill runs ends run() without starting collectors."""
        self.node.delay = 0.5
        monitor = Monitor(self.config, self.store)
        stop = asyncio.Event()
        task = asyncio.create_task(monitor.run(stop=stop, serve_http=False))
        await self.wait_for(lambda: monitor.tasks.get("backfill") == "running")
        stop.set()
        await asyncio.wait_for(task, 5.0)
        self.assertNotIn("snapshot", monitor.tasks)
        self.assertIsNone(monitor.snapshot)


def _get_json(url: str) -> Any:
    """GET a JSON document."""
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


class CliTests(unittest.TestCase):
    """Argument parsing, overrides and the report command."""

    def setUp(self) -> None:
        """Write a config file into a temporary directory."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.config_path = self.dir / "config.toml"
        self.config_path.write_text(
            'network = "testnet"\n'
            f'db = "{self.dir / "file.sqlite3"}"\n'
            '[[rpc]]\nname = "node"\nurl = "http://127.0.0.1:1/"\n'
        )

    def config(self, *argv: str) -> Any:
        """Parse `argv` and build the effective config."""
        return cli.build_config(cli.build_parser().parse_args(list(argv)))

    def test_overrides(self) -> None:
        """--db/--host/--port/--no-p2p/--no-cipherscan/--backfill-blocks override the file."""
        config = self.config("run", "--config", str(self.config_path), "--db", "/tmp/x.sqlite3", "--host", "0.0.0.0",
                             "--port", "9000", "--no-p2p", "--no-cipherscan", "--backfill-blocks", "0")
        self.assertEqual((config.db, config.http.host, config.http.port), ("/tmp/x.sqlite3", "0.0.0.0", 9000))
        self.assertFalse(config.p2p.enabled)
        self.assertFalse(config.cipherscan.enabled)
        self.assertEqual(config.chain.backfill_blocks, 0)
        plain = self.config("report", "--config", str(self.config_path))
        self.assertTrue(plain.p2p.enabled)
        self.assertEqual(plain.chain.backfill_blocks, 20_000)

    def test_bad_arguments_exit(self) -> None:
        """Out-of-range numbers and a missing --config are usage errors."""
        with contextlib.redirect_stderr(io.StringIO()):
            for argv in (["run", "--config", "x", "--port", "70000"], ["run"],
                         ["backfill", "--config", "x", "--blocks", "0"]):
                with self.subTest(argv=argv), self.assertRaises(SystemExit):
                    cli.build_parser().parse_args(argv)

    def test_nothing_to_monitor(self) -> None:
        """Disabling P2P with no RPC endpoints is refused."""
        self.config_path.write_text('network = "testnet"\n')
        with self.assertRaises(SystemExit):
            self.config("run", "--config", str(self.config_path), "--no-p2p")

    def test_probe_text_lists_groups_and_peers(self) -> None:
        """The probe-peers table shows each group's states and branch, then each peer (errors last)."""
        states = dict.fromkeys(("synced", "lagging", "fork", "stuck", "unknown", "inactive"), 0)
        snap = {"tip": {"height": BASE, "hash": bhash("a")},
                "groups": [{"key": "zebra 6.4", "active": 2, "states": {**states, "synced": 1, "stuck": 1},
                            "branch": {"key": "canonical"}}]}
        results = [{"source": "p2p:198.51.100.2:18233", "impl": None, "error": "connect timeout"},
                   {"source": PEER, "impl": "zebra", "version": "6.4.2", "tip_height": BASE, "error": None,
                    "relation": {"kind": "same"}, "tip_note": "bad\x1b[31m"}]
        text = cli._probe_text(results, snap)
        self.assertIn("1 of 2 peers answered", text)
        self.assertRegex(text, r"zebra 6.4 +2 +1 +0 +0 +1 +0  canonical")
        lines = text.splitlines()
        self.assertLess(lines.index(next(line for line in lines if PEER in line)),
                        lines.index(next(line for line in lines if "connect timeout" in line)))
        self.assertNotIn("\x1b", text)

    def test_report_prints_the_text_summary(self) -> None:
        """`report` loads the chain from the database and prints analysis.text_report."""
        db = self.dir / "report.sqlite3"
        with Store(db) as store:
            monitor = Monitor(self.config("report", "--config", str(self.config_path)), store)
            for index, name in enumerate(("a", "b", "c")):
                monitor.ingest_block(None, header(name, "g" if index == 0 else "abc"[index - 1], T0 + index),
                                     BASE + index, source=RPC, kind="rpc_tip", at=T0 + index)
            monitor.persist_best()
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(cli.main(["report", "--config", str(self.config_path), "--db", str(db)]), 0)
        self.assertIn(f"Tip {BASE + 2} {bhash('c')}", out.getvalue())


if __name__ == "__main__":
    unittest.main()
