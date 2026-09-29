"""The Monitor: owns the block tree, the store writer and every collector on one asyncio loop.

Collectors report to the Monitor (`ingest_block`, `ingest_headers`,
`record_sighting`, `observe_tip`, `add_peer_candidates`, `request_block`),
and the web server and `analysis.live_snapshot` read it (`chain`, `store`,
`config`, `p2p`, `rpc_status()`, `snapshot`, `loop`). Everything except the
web handler threads runs on the event-loop thread, so the Monitor is the only
writer and the `Chain` is never touched concurrently.

`run()` starts the dashboard, backfills recent canonical blocks, starts the
RPC collectors, the P2P observer and the CipherScan importer, then publishes a
live snapshot every SNAPSHOT_INTERVAL, fetches missing block bodies and prunes
(a minute after start, then hourly) until SIGINT/SIGTERM (or the `stop` event).

Notes:
- `Monitor(config, store, params=None)`: `params` defaults to the config's
  network. The constructor rebuilds the chain from the store (the newest
  `memory_window` heights) and each source's last tip, so a restart records no
  spurious tip changes.
- Extra hooks and helpers: `record_sighting` (used by the P2P observer so a
  block announced before it is ingested keeps its announcement time as first
  seen; such sightings wait in memory until the block is ingested, so
  announcements of made-up hashes never reach the store), `rpc_status()`,
  `track_split()`, `fetch_missing_bodies()`, `backfill()`, `probe_peers()`
  and `prune()`.
- Header-only blocks within BODY_SWEEP_DEPTH of the tip get their bodies:
  canonical ones over RPC (`getblock <hash> 0`); side-branch ones, RPC
  failures and the missing parents of detached side branches over P2P,
  preferring the peer that last showed the block on its chain. The RPC
  walk-back stops at the first block the chain holds, even a header-only one
  from P2P, so without this sweep blocks inside a burst would never be
  attributed to a miner.
- Backfill reaches down to the stored tip when the monitor was down for longer
  than `backfill_blocks` (up to `memory_window`), and the chain is rebuilt from
  the store after a backfill into a non-empty chain, so old and new blocks
  link up instead of piling up as detached blocks.
- The snapshot gains `split_event` (the open or pending split), `service`
  (start time, backfill result, task states) and `collectors.cipherscan`.
- Split events need a side branch at least SPLIT_MIN_DEPTH (2) blocks past
  its fork point with the best chain, with the other side as far past it
  (`split_fork`), not just one block: in the fast phase peers
  often sit 30-70 s on a one-block orphan before they reorg, which is lag.
  Such a split opens an event once it persists for SPLIT_MIN_DURATION and
  closes after SPLIT_CLEAR_AFTER without it; a new fork point closes the old
  event first. Events left open by a crash are closed at startup and the
  open event is closed on shutdown. The stored summary holds the fork, depth,
  `max_depth`, first/last seen, `closed_by` and the last `detect_split`
  result under "candidate".
- At startup, sources rows a previous run (or a crash) left "connected" or
  "ok" become "idle", so only recency keeps them active until they report.
- Backfilled blocks get no first-seen time (the fetch is not their arrival),
  so forward-dating and "seen first" stay unknown until a live sighting.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import math
import signal
import time
from collections import Counter, OrderedDict
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from . import __version__, analysis, web
from .chain import Chain, Node, TipChange
from .cipherscan import CipherscanImporter
from .config import Config
from .consensus import NETWORKS, Block, BlockHeader, NetworkParams, ParseError, identify_miner, parse_block
from .p2p import P2PObserver
from .rpc import RpcClient, RpcCollector, RpcError
from .rpc import backfill as rpc_backfill
from .store import UNTIMED_KINDS, Store

log = logging.getLogger(__name__)

# Refresh period of the live snapshot; commits run on every tick.
SNAPSHOT_INTERVAL = 2.0
TICK = 1.0
PRUNE_INTERVAL = 3_600.0
# The first prune runs this soon after start, so frequent restarts cannot postpone it indefinitely.
PRUNE_START_DELAY = 60.0
# Pause between prune batches so P2P I/O, timeouts and the snapshot keep running through a backlog.
PRUNE_PAUSE = 0.05
STATUS_LOG_INTERVAL = 300.0
# A split must persist this long before it becomes an event ...
SPLIT_MIN_DURATION = 30.0
# ... and an open event ends after this long without a split, so one lost poll does not end it.
SPLIT_CLEAR_AFTER = 30.0
# Blocks each side must be past the fork. Peers often sit 30-70 s on a one-block orphan
# during the fast phase before they reorg; that is lag, not a split.
SPLIT_MIN_DEPTH = 2
# Header-only blocks this close to the tip get their bodies fetched (miner attribution).
BODY_SWEEP_DEPTH = 500
BODY_SWEEP_INTERVAL = 5.0
# The RPC tip poll fetches a new tip's body within ~1 s; give it that chance first.
BODY_GRACE = 5.0
BODY_BATCH = 20
MAX_SIDE_REQUESTS = 32
BODY_ATTEMPTS = 3
BODY_RETRY = 60.0
MAX_BODY_MEMO = 4_096
# Blocks not ingested yet whose sightings are held in memory (the P2P observer records invs first),
# and the sources kept per block (well above the number of peers).
MAX_EARLY_SEEN = 20_000
MAX_EARLY_SOURCES = 512
MAX_TRACKED_SOURCES = 20_000
# Log a reorg at WARNING from this many disconnected blocks.
DEEP_REORG = 3
RESTART_DELAY = 30.0
SHUTDOWN_TIMEOUT = 10.0
# Blocks walked back per RPC endpoint by the one-shot `probe-peers`.
PROBE_WALK_LIMIT = 500
MAX_HEIGHT = (1 << 31) - 1
# Margin below the stored tip when backfill has to bridge downtime (reorged tips).
BRIDGE_MARGIN = 10


def load_chain(store: Store, params: NetworkParams, *, window: int, settle_depth: int, top: int | None = None) -> Chain:
    """Rebuild the in-memory chain from the newest `window` stored heights (below `top`, default: the stored best)."""
    if top is None:
        top = stored_top(store)
    min_height = top - window if top is not None else None
    return Chain.from_store(params, store, min_height=min_height, settle_depth=settle_depth)


def stored_top(store: Store) -> int | None:
    """Return the last persisted best height, else the highest height of a stored block body."""
    value = store.get_meta("best_height")
    if value is not None and value.isdigit():
        return int(value)
    row = store.reader().execute("SELECT MAX(height) FROM blocks WHERE body = 1").fetchone()
    return row[0] if row is not None and isinstance(row[0], int) else None


def _valid_height(value: Any) -> int | None:
    """Return `value` if it is a plausible block height, else None."""
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_HEIGHT:
        return None
    return value


@dataclass(slots=True)
class _Split:
    """A split being tracked, keyed by its fork hash; `event_id` is set once it persisted into an event."""

    fork_hash: str
    fork_height: int
    since: float
    last_seen: float
    depth: int
    max_depth: int
    candidate: dict[str, Any]
    event_id: int | None = None
    cleared_at: float | None = None

    def summary(self, closed_by: str | None = None) -> dict[str, Any]:
        """Return the JSON summary stored in `split_events` (the last `detect_split` result under "candidate")."""
        out = {"fork_hash": self.fork_hash, "fork_height": self.fork_height, "depth": self.depth,
               "max_depth": self.max_depth, "first_seen_at": self.since, "last_seen_at": self.last_seen,
               "candidate": self.candidate}
        if closed_by is not None:
            out["closed_by"] = closed_by
        return out

    def view(self) -> dict[str, Any]:
        """Return the snapshot's `split_event` entry."""
        return {"id": self.event_id, "open": self.event_id is not None, "since": self.since,
                "last_seen_at": self.last_seen, "fork_hash": self.fork_hash, "fork_height": self.fork_height,
                "depth": self.depth, "max_depth": self.max_depth,
                "groups": self.candidate.get("groups", {})}


class Monitor:
    """Wires the chain, the store and the collectors together; the single writer (see the module doc)."""

    def __init__(self, config: Config, store: Store, params: NetworkParams | None = None) -> None:
        """Bind to `store` and rebuild the chain and per-source tips from it; nothing starts yet."""
        self.config = config
        self.store = store
        self.params = params or NETWORKS[config.network]
        self.chain = load_chain(
            store, self.params, window=config.chain.memory_window, settle_depth=config.chain.settle_depth
        )
        self.snapshot: dict[str, Any] | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.p2p: P2PObserver | None = None
        self.collectors: list[RpcCollector] = []
        self.cipherscan: CipherscanImporter | None = None
        self.http_address: tuple[str, int] | None = None
        self.started_at: float | None = None
        self.backfill_result: dict[str, Any] | None = None
        self.tasks: dict[str, str] = {}
        self._tips: dict[str, str] = {
            row["source"]: row["tip_hash"] for row in store.get_sources() if row.get("tip_hash")
        }
        # Block hash -> source -> earliest (kind, at) of sightings made before the block was ingested.
        self._early_seen: OrderedDict[str, dict[str, tuple[str, float]]] = OrderedDict()
        self._body_memo: OrderedDict[str, tuple[int, float]] = OrderedDict()
        # Block hash -> IP of the last P2P peer that showed it on its chain: the likeliest server of a
        # side-branch body (Zebra serves only its best chain, and Zakura may hold only the header).
        self._body_hosts: OrderedDict[str, str] = OrderedDict()
        self._split: _Split | None = None
        self._persisted_best: str | None = None
        self._last_status_log = -math.inf  # monotonic
        self._snapshot_errors = 0
        best = self.chain.best_tip()
        log.info("loaded %d blocks from %s (tip %s)", len(self.chain), store.path, best.height if best else "none")

    # -- collector hooks -------------------------------------------------------------

    def ingest_block(
        self,
        block: Block | None,
        header: BlockHeader,
        height: int | None,
        *,
        source: str,
        kind: str,
        at: float,
        trusted: bool = True,
    ) -> Node | None:
        """Add a block (or a bare header) to the chain and the store and record `source`'s sighting.

        An UNTIMED_KINDS ingest (the backfill) does not make `at` the block's first-seen time.
        `trusted` is False for a body from a P2P peer outside the fleet (see `Node.body_trusted`).
        A header-only block off the canonical chain near the tip gets its body requested.
        """
        seen_at = at if kind not in UNTIMED_KINDS else None
        early = self._early_seen.pop(header.hash, {})
        first_seen = min((when for k, when in early.values() if k not in UNTIMED_KINDS), default=None)
        if seen_at is not None:
            first_seen = seen_at if first_seen is None else min(first_seen, seen_at)
        node = self.chain.add(header, height, block=block, first_seen_at=first_seen, trusted=trusted)
        miner = node.miner if node is not None else None
        if miner is None and block is not None:
            miner = identify_miner(block.coinbase, self.params)
        for early_source, (early_kind, early_at) in early.items():
            self.store.record_sighting(header.hash, early_source, early_kind, early_at)
        self._store_block(block, header, node, height, miner, seen_at, source, trusted)
        self.store.record_sighting(header.hash, source, kind, at)
        if block is None:
            self._note_host(header.hash, source)
            self._want_side_body(node, at)
        return node

    def ingest_headers(self, headers: Sequence[BlockHeader], source: str, at: float) -> int:
        """Ingest a `headers` reply from a P2P peer; return how many headers were new to the chain."""
        new = 0
        for header in headers:
            new += header.hash not in self.chain
            self.ingest_block(None, header, None, source=source, kind="headers", at=at)
        return new

    def record_sighting(self, hash: str, source: str, kind: str, at: float) -> bool:
        """Record that `source` saw `hash`; True if it is `source`'s first sighting of it.

        Sightings of a block not ingested yet are held in memory (the newest MAX_EARLY_SEEN
        blocks) and written when it is, so hashes that never become blocks never reach the store.
        """
        self._note_host(hash, source)
        # A stored block the chain does not hold (e.g. a header waiting for its parent) was ingested already.
        if hash in self.chain or self.store.get_block(hash) is not None:
            return self.store.record_sighting(hash, source, kind, at)
        seen = self._early_seen.get(hash)
        if seen is None:
            seen = self._early_seen[hash] = {}
            while len(self._early_seen) > MAX_EARLY_SEEN:
                self._early_seen.popitem(last=False)
        previous = seen.get(source)
        if previous is None:
            if len(seen) >= MAX_EARLY_SOURCES:
                return False
            self._early_seen.move_to_end(hash)
        if previous is None or at < previous[1]:
            seen[source] = (kind, at)
        return previous is None

    def observe_tip(self, source: str, tip_hash: str, at: float, height_hint: int | None = None) -> TipChange | None:
        """Record `source`'s new tip as a tip change (reorg depth from the chain) and on its sources row.

        Returns the change, or None when the tip did not move.
        """
        old = self._tips.get(source)
        if old == tip_hash:
            return None
        change = self.chain.classify_tip_change(old, tip_hash)
        row = change.as_row()
        if row["new_height"] is None:
            row["new_height"] = _valid_height(height_hint)
        self.store.record_tip_change(source=source, at=at, **row)
        fields: dict[str, Any] = {"tip_hash": tip_hash, "tip_height": row["new_height"], "tip_at": at}
        if source.startswith("rpc:"):
            fields["tip_via"] = "rpc:getbestblockhash"
        self.store.upsert_source(source, **fields)
        self._tips.pop(source, None)
        self._tips[source] = tip_hash
        if len(self._tips) > MAX_TRACKED_SOURCES:
            self._tips.pop(next(iter(self._tips)))
        if change.is_reorg:
            level = logging.WARNING if change.disconnected >= DEEP_REORG else logging.INFO
            if source.startswith("p2p:") and level == logging.INFO:
                level = logging.DEBUG
            log.log(level, "%s reorged at %s: -%d +%d blocks (%s -> %s)", source, change.fork_height,
                    change.disconnected, change.connected, _short(old), _short(tip_hash))
        return change

    def add_peer_candidates(self, entries: Iterable[Sequence[Any]]) -> int:
        """Pass (ip, port, user_agent, via) peers to the P2P observer; return how many were new."""
        return self.p2p.add_candidates(entries) if self.p2p is not None else 0

    def request_block(self, hash: str, height_hint: int | None = None, prefer_host: str | None = None) -> bool:
        """Ask the P2P observer to fetch a block body; False when P2P is disabled or the request was refused."""
        if self.p2p is None:
            return False
        return self.p2p.request_block(hash, height_hint, prefer_host=prefer_host)

    def rpc_status(self) -> list[dict[str, Any]]:
        """Return each RPC collector's health plus how far its tip is behind the best tip."""
        best = self.chain.best_tip()
        out = []
        for collector in self.collectors:
            health = collector.health()
            height = health.get("tip_height")
            health["behind"] = best.height - height if best is not None and isinstance(height, int) else None
            out.append(health)
        return out

    # -- snapshot and split tracking -------------------------------------------------------

    def refresh_snapshot(self, now: float | None = None) -> dict[str, Any]:
        """Build, annotate and publish the live snapshot (loop thread); also track splits and the best tip."""
        now = time.time() if now is None else now
        snap = analysis.live_snapshot(self, now)
        self.track_split(snap.get("split_candidate"), now)
        snap["split_event"] = self._split.view() if self._split is not None else None
        collectors = snap.setdefault("collectors", {})
        collectors["cipherscan"] = self.cipherscan.health() if self.cipherscan is not None else None
        snap["service"] = {
            "version": __version__,
            "started_at": self.started_at,
            "backfill": self.backfill_result,
            "tasks": dict(self.tasks),
        }
        self.persist_best()
        self.snapshot = snap
        return snap

    def track_split(self, candidate: dict[str, Any] | None, now: float) -> _Split | None:
        """Advance the split state machine with this refresh's `detect_split` result; return the tracked split."""
        fork = self.split_fork(candidate) if candidate is not None else None
        state = self._split
        if fork is not None:
            fork_hash, fork_height, depth = fork
            if state is not None and state.fork_hash != fork_hash:
                self._end_split(state, now, "superseded")
                state = None
            if state is None:
                state = self._split = _Split(fork_hash, fork_height, now, now, depth, depth, candidate)
            else:
                state.last_seen, state.depth, state.candidate, state.cleared_at = now, depth, candidate, None
                state.max_depth = max(state.max_depth, depth)
            if state.event_id is None and now - state.since >= SPLIT_MIN_DURATION:
                state.event_id = self.store.open_split(
                    started_at=state.since, fork_hash=fork_hash, fork_height=fork_height, summary=state.summary()
                )
                log.warning("network split at %d (%s): %s", fork_height, _short(fork_hash),
                            "; ".join(",".join(side["groups"]) for side in candidate.get("sides", ())))
        elif state is not None:
            if state.event_id is None:
                self._split = None
            else:
                state.cleared_at = state.cleared_at or now
                if now - state.cleared_at >= SPLIT_CLEAR_AFTER:
                    self._end_split(state, state.cleared_at, "resolved")
                    self._split = None
        return self._split

    def split_fork(self, candidate: dict[str, Any]) -> tuple[str, int, int] | None:
        """Return (fork hash, fork height, depth) if `candidate` is a split that can become an event, else None.

        Only side branches at least SPLIT_MIN_DEPTH blocks past their own fork point with the best
        chain count, and the other side (the best chain or another such branch) must also be that
        far past it. The fork is the lowest such fork point; depth is the longest side past its fork.
        """
        branches: list[tuple[int, str, int]] = []
        canonical_tip = None
        for side in candidate.get("sides") or ():
            tip = side.get("tip_height")
            if not isinstance(tip, int):
                continue
            if side.get("branch") == analysis.CANONICAL:
                canonical_tip = tip
                continue
            first = self.chain.get(side.get("branch"))
            if first is not None and tip - (first.height - 1) >= SPLIT_MIN_DEPTH:
                branches.append((first.height - 1, first.prev_hash, tip - (first.height - 1)))
        if not branches:
            return None
        fork_height, fork_hash, _ = min(branches)
        ours = canonical_tip - fork_height if canonical_tip is not None else None
        if len(branches) < 2 and (ours is None or ours < SPLIT_MIN_DEPTH):
            return None
        depths = [depth for _, _, depth in branches] + ([ours] if ours is not None else [])
        return fork_hash, fork_height, max(depths)

    def _end_split(self, state: _Split, ended_at: float, reason: str) -> None:
        """Close `state`'s split event, if it was opened."""
        if state.event_id is not None:
            self.store.close_split(state.event_id, ended_at, state.summary(reason))
            log.warning("network split at %d ended (%s) after %.0fs, max depth %d", state.fork_height, reason,
                        ended_at - state.since, state.max_depth)

    def close_stale_splits(self, now: float, reason: str) -> int:
        """Close split events left open (by a crash or an earlier run); return how many."""
        closed = 0
        for row in self.store.get_open_splits():
            try:
                summary = json.loads(row["summary"])
            except (TypeError, ValueError):
                summary = {}
            if not isinstance(summary, dict):
                summary = {"summary": summary}
            summary["closed_by"] = reason
            self.store.close_split(row["id"], now, summary)
            closed += 1
        return closed

    def reset_stale_statuses(self) -> int:
        """Mark sources a previous run left "connected" or "ok" as "idle" until a collector reports again."""
        stale = [row["source"] for row in self.store.get_sources() if row.get("status") in ("connected", "ok")]
        for source in stale:
            self.store.upsert_source(source, status="idle")
        return len(stale)

    def persist_best(self) -> None:
        """Save the best tip in `meta` so the next start loads the right height window."""
        best = self.chain.best_tip()
        if best is not None and best.hash != self._persisted_best:
            self.store.set_meta("best_hash", best.hash)
            self.store.set_meta("best_height", best.height)
            self._persisted_best = best.hash

    # -- bodies, backfill, pruning ---------------------------------------------------------

    async def fetch_missing_bodies(self, now: float | None = None) -> int:
        """Fetch bodies of header-only blocks near the tip: canonical over RPC, side branches over P2P.

        Blocks whose body came from an untrusted peer are fetched again the same way, so a
        trusted body replaces their attribution. Missing parents of detached blocks are
        requested over P2P too. Returns how many bodies were ingested over RPC.
        """
        best = self.chain.best_tip()
        if best is None:
            return 0
        now = time.time() if now is None else now
        canonical: list[Node] = []
        side: list[Node] = []
        for height in range(max(0, best.height - BODY_SWEEP_DEPTH), best.height + 1):
            canon = self.chain.canonical_hash_at(height)
            for node in self.chain.nodes_at(height):
                if (node.body and node.body_trusted) or node.cumwork is None:
                    continue
                if node.first_seen_at is not None and now - node.first_seen_at < BODY_GRACE:
                    continue
                (canonical if node.hash == canon else side).append(node)
        # Missing parents of detached blocks (e.g. a multi-block fork tip from getchaintips) are
        # walked back one block per sweep; Zakura peers serve any retained chain.
        wanted_side = [(node.hash, node.height) for node in side]
        wanted_side += [
            (missing, height)
            for missing, height in self.chain.missing_parents()
            if best.height - height <= BODY_SWEEP_DEPTH
        ]
        for block_hash, height in wanted_side[:MAX_SIDE_REQUESTS]:
            self._request_body(block_hash, height, now)
        wanted: list[Node] = []
        for node in canonical:
            if len(wanted) == BODY_BATCH:
                break  # the rest keep their attempts for the next sweep
            if self._take_attempt(node.hash, now):
                wanted.append(node)
        if not wanted:
            return 0
        fetched, failed = await self._fetch_bodies_rpc(wanted)
        for node in failed:
            prefer = None if node.body else self._body_hosts.get(node.hash)  # see `_request_body`
            self.request_block(node.hash, node.height, prefer_host=prefer)
        return fetched

    async def _fetch_bodies_rpc(self, nodes: list[Node]) -> tuple[int, list[Node]]:
        """Fetch `nodes` by hash from a healthy RPC endpoint; return (ingested, nodes that failed)."""
        collector = self._healthy_collector()
        if collector is None:
            return 0, nodes
        try:
            replies = await asyncio.to_thread(collector.client.batch, [("getblock", [n.hash, 0]) for n in nodes])
        except RpcError as err:
            log.debug("body fetch from %s failed: %s", collector.source, err)
            return 0, nodes
        if len(replies) != len(nodes):
            return 0, nodes
        fetched, failed = 0, []
        for node, reply in zip(nodes, replies, strict=True):
            try:
                if isinstance(reply, Exception) or not isinstance(reply, str):
                    raise ValueError(f"no block: {reply!r:.80}")
                block = parse_block(bytes.fromhex(reply), self.params)
                if block.header.hash != node.hash:
                    raise ValueError("wrong block")
            except (ParseError, ValueError):
                failed.append(node)
                continue
            self.ingest_block(block, block.header, node.height, source=collector.source, kind="rpc_body",
                              at=time.time())
            fetched += 1
        return fetched, failed

    def _healthy_collector(self) -> RpcCollector | None:
        """Return an RPC collector whose endpoint answered recently, fleet endpoints first."""
        fleet = {endpoint.source for endpoint in self.config.rpc if endpoint.fleet}
        healthy = [c for c in self.collectors if c.health().get("status") == "ok"]
        healthy.sort(key=lambda c: c.source not in fleet)
        return healthy[0] if healthy else None

    def _request_body(self, block_hash: str, height: int | None, now: float) -> None:
        """Request a body over P2P within its attempt budget; a request the observer refuses costs no attempt.

        A block held with an untrusted body is not steered to the peer that showed it, which may have sent that body.
        """
        previous = self._body_memo.get(block_hash)
        if not self._take_attempt(block_hash, now):
            return
        node = self.chain.get(block_hash)
        prefer = None if node is not None and node.body else self._body_hosts.get(block_hash)
        if not self.request_block(block_hash, height, prefer_host=prefer):
            if previous is None:
                self._body_memo.pop(block_hash, None)
            else:
                self._body_memo[block_hash] = previous

    def _take_attempt(self, block_hash: str, now: float) -> bool:
        """Rate-limit body fetches per block (BODY_ATTEMPTS, BODY_RETRY apart)."""
        attempts, last = self._body_memo.get(block_hash, (0, -math.inf))
        if attempts >= BODY_ATTEMPTS or now - last < BODY_RETRY:
            return False
        self._body_memo[block_hash] = (attempts + 1, now)
        self._body_memo.move_to_end(block_hash)
        while len(self._body_memo) > MAX_BODY_MEMO:
            self._body_memo.popitem(last=False)
        return True

    async def backfill(self, blocks: int) -> dict[str, Any] | None:
        """Backfill the last `blocks` canonical blocks from the first `backfill` RPC endpoint that works.

        Returns the result as a dict, or None when no endpoint could be used.
        """
        endpoints = sorted((e for e in self.config.rpc if e.backfill), key=lambda e: not e.fleet)
        for endpoint in endpoints:
            client = RpcClient(endpoint.url, timeout=endpoint.timeout)
            try:
                node_tip = _valid_height(await asyncio.to_thread(client.call, "getblockcount"))
                if node_tip is None:
                    raise ValueError("getblockcount returned no height")
                count = self._bridge(blocks, node_tip)
                had_blocks = len(self.chain) > 0
                result = await rpc_backfill(client, self, count, name=endpoint.name)
            except (RpcError, ValueError) as err:
                log.warning("backfill from %s failed: %s", endpoint.name, err)
                continue
            if had_blocks and result.fetched:
                self.chain = load_chain(
                    self.store, self.params, window=self.config.chain.memory_window,
                    settle_depth=self.config.chain.settle_depth, top=result.tip_height,
                )
                log.info("rebuilt the chain from the store: %d blocks", len(self.chain))
            self.backfill_result = {"source": endpoint.source, **dataclasses.asdict(result)}
            return self.backfill_result
        return None

    def _bridge(self, blocks: int, node_tip: int) -> int:
        """Widen a backfill of `blocks` so it reaches the stored tip after downtime (up to the memory window)."""
        best = self.chain.best_tip()
        if best is None or blocks <= 0:
            return blocks
        gap = node_tip - best.height + BRIDGE_MARGIN
        if blocks < gap <= self.config.chain.memory_window:
            log.info("backfill widened from %d to %d blocks to reach the stored tip %d", blocks, gap, best.height)
            return gap
        return blocks

    async def prune(self, now: float | None = None) -> dict[str, int]:
        """Drop chain blocks below the memory window and store rows older than the retention period.

        Store rows go one batch at a time with PRUNE_PAUSE between batches, so a large
        backlog (e.g. after lowering `retention.days`) never blocks the event loop for long.
        """
        now = time.time() if now is None else now
        removed = {"chain_blocks": 0}
        best = self.chain.best_tip()
        if best is not None:
            removed["chain_blocks"] = self.chain.prune_below(best.height - self.config.chain.memory_window)
        for table, count in self.store.prune_batches(now - self.config.retention.seconds):
            removed[table] = removed.get(table, 0) + count
            await asyncio.sleep(PRUNE_PAUSE)
        if any(removed.values()):
            log.info("pruned %s", ", ".join(f"{k} {v}" for k, v in removed.items() if v))
        return removed

    async def probe_peers(self, limit: int = 200) -> list[dict[str, Any]]:
        """One-shot survey: refresh the tip and peer list over RPC, then sweep up to `limit` P2P peers."""
        self.loop = asyncio.get_running_loop()
        if self.p2p is None:
            self.p2p = P2PObserver.from_config(self.params, self, self.config)
        for endpoint in self.config.rpc:
            collector = RpcCollector.from_endpoint(endpoint, self, walk_limit=PROBE_WALK_LIMIT)
            try:
                await collector.poll_tip()
                await collector.poll_peers()
            except (RpcError, ValueError) as err:
                log.warning("%s: %s", endpoint.source, err)
        results = await self.p2p.sweep(limit)
        self.store.commit_if_due(force=True)
        return results

    # -- orchestration ---------------------------------------------------------------------

    async def run(self, *, stop: asyncio.Event | None = None, serve_http: bool = True) -> None:
        """Run the service until `stop` is set (default: until SIGINT/SIGTERM), then shut down cleanly."""
        self.loop = asyncio.get_running_loop()
        self.started_at = time.time()
        signals: list[signal.Signals] = []
        if stop is None:
            stop = asyncio.Event()
            signals = self._install_signal_handlers(stop)
        closed = self.close_stale_splits(self.started_at, "restart")
        if closed:
            log.info("closed %d split event(s) left open by the previous run", closed)
        self.reset_stale_statuses()
        server = None
        tasks: list[asyncio.Task[None]] = []
        try:
            if serve_http:
                server = web.start_server(self, self.config.http.host, self.config.http.port)
                self.http_address = tuple(server.server_address[:2])
                log.info("dashboard on http://%s:%d/", self.http_address[0], self.http_address[1])
            if self.config.chain.backfill_blocks > 0 and self.config.rpc:
                self.tasks["backfill"] = "running"
                await _unless_stopped(self.backfill(self.config.chain.backfill_blocks), stop)
                self.tasks["backfill"] = "done"
            if stop.is_set():
                return
            tasks = self._start_tasks()
            self.refresh_snapshot()
            await stop.wait()
        finally:
            log.info("shutting down")
            if server is not None:
                await asyncio.to_thread(server.shutdown)
                server.server_close()
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.wait(tasks, timeout=SHUTDOWN_TIMEOUT)
            if self._split is not None:
                self._end_split(self._split, time.time(), "shutdown")
                self._split = None
            self.store.commit_if_due(force=True)
            for sig in signals:
                self.loop.remove_signal_handler(sig)

    def _start_tasks(self) -> list[asyncio.Task[None]]:
        """Create the collectors and start every long-running task under supervision."""
        config = self.config
        if config.p2p.enabled:
            self.p2p = P2PObserver.from_config(self.params, self, config)
        if config.cipherscan.enabled:
            self.cipherscan = CipherscanImporter.from_config(config.cipherscan, self)
        self.collectors = [RpcCollector.from_endpoint(endpoint, self) for endpoint in config.rpc]
        jobs: list[tuple[str, Callable[[], Awaitable[None]]]] = [
            (collector.source, collector.run) for collector in self.collectors
        ]
        if self.p2p is not None:
            jobs.append(("p2p", self.p2p.run))
        if self.cipherscan is not None:
            jobs.append(("cipherscan", self.cipherscan.run))
        jobs += [("snapshot", self._snapshot_loop), ("bodies", self._body_loop), ("prune", self._prune_loop)]
        log.info("starting %s", ", ".join(name for name, _ in jobs))
        return [asyncio.create_task(self._supervise(name, job), name=name) for name, job in jobs]

    async def _supervise(self, name: str, job: Callable[[], Awaitable[None]]) -> None:
        """Run `job`, restarting it after RESTART_DELAY if it crashes; a clean return ends it."""
        while True:
            self.tasks[name] = "running"
            try:
                await job()
            except asyncio.CancelledError:
                self.tasks[name] = "stopped"
                raise
            except Exception:
                self.tasks[name] = "restarting"
                log.exception("%s crashed; restarting in %.0fs", name, RESTART_DELAY)
                await asyncio.sleep(RESTART_DELAY)
                continue
            self.tasks[name] = "finished"
            log.info("%s finished", name)
            return

    async def _snapshot_loop(self) -> None:
        """Commit the write batch every TICK and publish the snapshot every SNAPSHOT_INTERVAL.

        The interval runs on the monotonic clock: a backward wall-clock step must not freeze the snapshot.
        """
        last = -math.inf
        while True:
            self.store.commit_if_due()
            tick = time.monotonic()
            if tick - last >= SNAPSHOT_INTERVAL:
                last = tick
                try:
                    snap = self.refresh_snapshot(time.time())
                except Exception:
                    self._snapshot_errors += 1
                    # Keep serving the previous snapshot; log the first failure and then every 30th.
                    if self._snapshot_errors % 30 == 1:
                        log.exception("snapshot refresh failed (%d so far)", self._snapshot_errors)
                else:
                    if tick - self._last_status_log >= STATUS_LOG_INTERVAL:
                        self._last_status_log = tick
                        log.info("%s", _status_line(snap))
            await asyncio.sleep(TICK)

    async def _body_loop(self) -> None:
        """Fetch missing bodies every BODY_SWEEP_INTERVAL."""
        while True:
            await asyncio.sleep(BODY_SWEEP_INTERVAL)
            await self.fetch_missing_bodies()

    async def _prune_loop(self) -> None:
        """Prune PRUNE_START_DELAY after start, then every PRUNE_INTERVAL."""
        await asyncio.sleep(PRUNE_START_DELAY)
        while True:
            await self.prune()
            await asyncio.sleep(PRUNE_INTERVAL)

    def _install_signal_handlers(self, stop: asyncio.Event) -> list[signal.Signals]:
        """Set `stop` on SIGINT/SIGTERM; return the signals handled (none off the main thread)."""
        installed = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self.loop.add_signal_handler(sig, _on_signal, sig, stop)
            except (NotImplementedError, RuntimeError, ValueError):
                continue
            installed.append(sig)
        return installed

    # -- helpers ---------------------------------------------------------------------------

    def _store_block(
        self,
        block: Block | None,
        header: BlockHeader,
        node: Node | None,
        height: int | None,
        miner: str | None,
        at: float | None,
        source: str,
        trusted: bool,
    ) -> None:
        """Upsert the block row (`at` None: untimed) with the chain's height and min-difficulty flag if placed.

        A block the chain did not place keeps only the caller's height hint: its coinbase height is unverified.
        """
        if node is not None:
            height, is_min_diff = node.height, node.is_min_diff
        else:
            height = _valid_height(height)
            start = self.params.min_diff_after_height
            is_min_diff = (
                height is not None and start is not None and height >= start
                and header.bits == self.params.pow_limit_bits
            )
        self.store.upsert_block(
            block, header, height, miner=miner, is_min_diff=is_min_diff, seen_at=at, seen_source=source,
            trusted=trusted,
        )

    def _want_side_body(self, node: Node | None, now: float) -> None:
        """Request the body of a header-only block that is off the canonical chain near the tip."""
        best = self.chain.best_tip()
        if node is None or node.body or best is None or best.height - node.height > BODY_SWEEP_DEPTH:
            return
        if self.chain.canonical_hash_at(node.height) != node.hash:
            self._request_body(node.hash, node.height, now)

    def _note_host(self, block_hash: str, source: str) -> None:
        """Remember the P2P peer that last showed `block_hash` on its chain (see `_body_hosts`)."""
        if not source.startswith("p2p:"):
            return
        self._body_hosts[block_hash] = source[4:].rpartition(":")[0].strip("[]")
        self._body_hosts.move_to_end(block_hash)
        while len(self._body_hosts) > MAX_BODY_MEMO:
            self._body_hosts.popitem(last=False)


async def _unless_stopped(job: Awaitable[Any], stop: asyncio.Event) -> Any:
    """Await `job`, cancelling it if `stop` is set first; return its result (None when stopped)."""
    task = asyncio.ensure_future(job)
    waiter = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        waiter.cancel()
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    return None if task.cancelled() else task.result()


def _on_signal(sig: signal.Signals, stop: asyncio.Event) -> None:
    """Signal handler: start a graceful shutdown."""
    log.info("received %s", sig.name)
    stop.set()


def _short(block_hash: str | None) -> str:
    """Abbreviate a block hash for log lines."""
    return f"{block_hash[:8]}..{block_hash[-6:]}" if block_hash else "none"


def _status_line(snap: dict[str, Any]) -> str:
    """One-line periodic status for the log."""
    tip = snap.get("tip") or {}
    p2p = (snap.get("collectors") or {}).get("p2p") or {}
    by_group = Counter(p2p.get("connected_by_group") or {})
    groups = ", ".join(f"{k} {v}" for k, v in by_group.most_common(6)) or "none"
    split = snap.get("split_event")
    return (
        f"tip {tip.get('height')} {_short(tip.get('hash'))}; chain {(snap.get('chain') or {}).get('blocks')} blocks; "
        f"p2p {p2p.get('connected', 0)} connected ({groups}); stuck {len(snap.get('stuck') or ())}; "
        f"split {'open' if split and split.get('open') else 'pending' if split else 'none'}"
    )
