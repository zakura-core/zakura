"""In-memory block tree: canonical chain, tip changes, forks, stale blocks and sawtooth phases.

`Chain` links every block the monitor has seen (inside a height window) by
`prev_hash` and tracks the canonical chain: the path to the tip with the most
cumulative work. Equal work goes to the earliest `first_seen_at`, then to the
greater raw (internal byte order) hash; an unknown first-seen time counts as
latest, so the order is total and does not depend on insertion order.

Cumulative work is relative. The missing parents of the lowest attached blocks
are "anchors" with a fixed cumwork; every attached block has
`cumwork = parent.cumwork + work`. When an anchor block itself arrives (an RPC
walk-back or a backfill below the window), the tree grows downward from the
anchor's value, so nothing already attached is recomputed. A block whose parent
is missing and is not an anchor is detached (`cumwork is None`) until its
ancestry arrives; a header with neither a known parent nor any height waits in
a bounded pending pool.

Heights come from the parent (parent + 1) when it is known, else from the
anchor, else from the coinbase BIP34 height, else from the caller's hint.

Not thread-safe: the monitor mutates and queries it on the event-loop thread.
Headers are trusted to be PoW-checked by the caller; the tree only bounds how
much unattached data it keeps and how far above the best tip it may sit.

Notes:
- `best_tip()` returns None while the chain is empty.
- `add()` gives `height`, `block`, `miner` and `first_seen_at` defaults; the
  miner defaults to `identify_miner(block.coinbase)` when a block is given, and
  parent + 1 wins over BIP34 (they only differ for consensus-invalid blocks).
- `relation()` takes an optional `ref_hash` (default: the best tip); only a
  non-default reference can make "ahead" happen.
- `is_min_diff` is "pow-limit bits at a height where the Testnet rule is
  active", as in the historical analysis (`bits == 2007ffff`); the 450 s gap is
  not re-checked.
- `Phase.fast` is causal: a cycle that never slows down stays fast (the
  historical analysis labels such cycles "unknown").
- Fork and loser classification is "unknown" when either miner is not attributed.
- `prune_below()` clamps to the best tip height and returns the number of
  blocks removed; blocks below the floor are ignored afterwards.
- Extra API: `from_store()`, `load()`, `canonical_nodes()`, `nodes_at()`,
  `missing_parents()`, `__len__`, `TipChange.as_row()`, and extra result fields
  (`Phase.d_pre/difficulty`, `Reset.d_pre/fast_blocks/cycle_blocks`,
  `ForkEvent.depth/depth_work/settled`, `LoserBranch.winner_len`, ...).
"""

from __future__ import annotations

import dataclasses
import math
from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .consensus import (
    Block,
    BlockHeader,
    NetworkParams,
    ParseError,
    bits_to_target,
    identify_miner,
    raw_hash_key,
    work_from_bits,
)

DEFAULT_SETTLE_DEPTH = 3
# Detached blocks (height known, parent missing) kept before the oldest groups are evicted.
MAX_DETACHED = 20_000
# Heightless headers with a missing parent kept before the oldest are evicted.
MAX_PENDING = 10_000
# Detached blocks may sit at most this far above the best tip. Real ones arrive just ahead of their
# parents (an RPC walk-back fills up to 2000 blocks, the top first); a forged body's BIP34 height
# can be anything.
MAX_DETACHED_AHEAD = 1_000
# Larger heights are garbage hints; Zcash is at ~4.4M after nine years.
MAX_HEIGHT = (1 << 31) - 1
MAX_HASH_LEN = 64
MAX_LABEL_LEN = 256

SELF, RACE, UNKNOWN = "self", "race", "unknown"


@dataclass(slots=True, eq=False)
class Node:
    """One block in the tree; `cumwork` is None while its ancestry is incomplete.

    `body_trusted` says the body came from RPC or a fleet peer. Nothing binds another peer's
    coinbase to the header (a merkle check would not: v5+ txids leave out scriptSigs, where
    the tag and template live), so a trusted body replaces an untrusted one's attribution.
    """

    hash: str
    prev_hash: str
    height: int
    time: int
    bits: int
    work: int
    is_min_diff: bool
    miner: str | None
    template: str | None
    tag: str
    extranonce: str
    first_seen_at: float | None
    body: bool
    body_trusted: bool = False
    cumwork: int | None = None
    children: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class TipChange:
    """How a tip moved; the fields are the `tip_changes` columns other than id, source and at."""

    old_hash: str | None
    old_height: int | None
    new_hash: str
    new_height: int | None
    fork_hash: str | None
    fork_height: int | None
    disconnected: int
    connected: int
    is_reorg: bool
    disconnected_work: int | None
    connected_work: int | None

    def as_row(self) -> dict[str, Any]:
        """Return the fields as keyword arguments for `Store.record_tip_change`."""
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class BlockRef:
    """Attribution and timing of one block, copied out of the mutable tree."""

    hash: str
    height: int
    time: int
    miner: str | None
    template: str | None
    first_seen_at: float | None
    is_min_diff: bool
    body: bool


@dataclass(frozen=True, slots=True)
class LoserBranch:
    """One losing branch at a fork point, compared with the winning (canonical) child."""

    block: BlockRef  # first block after the fork point
    tip_hash: str  # heaviest block of the branch
    length: int  # blocks from the fork point to tip_hash
    work: int  # work of those blocks
    blocks: int  # every block in the branch subtree
    miners: tuple[str, ...]  # per block from `block` to tip_hash; "unknown" when unattributed
    classification: str  # "self" (same miner as the winner) | "race" | "unknown"
    same_job: bool  # same miner and header time as the winner (siblings share the parent)
    seen_first: bool | None  # seen strictly before the winner; None if unknown or equal
    greater_raw_hash: bool | None  # raw hash above the winner's
    equal_work: bool  # first block carries the winner's work
    winner_len: int | None  # canonical blocks after the fork point whose work reaches `work`


@dataclass(frozen=True, slots=True)
class ForkEvent:
    """A canonical block with more than one attached child."""

    fork_hash: str
    fork_height: int
    fork_time: int
    winner: BlockRef
    losers: tuple[LoserBranch, ...]  # longest first
    depth: int  # longest loser branch, in blocks
    depth_work: int  # heaviest loser branch work
    classification: str  # "race" if any loser raced, else "self" if all did, else "unknown"
    same_job: bool  # any loser shares the winner's job
    winner_first_seen: bool | None  # winner seen strictly before every loser
    winner_greater_raw_hash: bool | None  # winner raw hash above every loser's
    equal_work: bool  # every loser's first block has the winner's work
    settled: bool  # best tip is settle_depth past every loser tip


@dataclass(frozen=True, slots=True)
class Phase:
    """Position of a canonical height in the min-difficulty sawtooth."""

    height: int
    k: int | None  # blocks since the last canonical min-difficulty block (0 at the reset)
    reset_height: int | None
    d_pre: float | None  # difficulty of the block just before that reset
    difficulty: float | None
    d_ratio: float | None  # difficulty / d_pre
    fast: bool | None  # before the trailing-window mean interval first reached target/2
    min_diff: bool


@dataclass(frozen=True, slots=True)
class Reset:
    """A canonical min-difficulty block and the timing around it."""

    height: int
    hash: str
    time: int
    miner: str | None
    template: str | None
    gap: int | None  # time - parent time (451 s for Zakura-template resets)
    next_dt: int | None  # next canonical block time - time; negative when forward-dated
    forward_dating: float | None  # time - first_seen_at
    d_pre: float | None
    fast_blocks: int | None  # blocks until the fast phase ended, None while still fast
    cycle_blocks: int | None  # blocks until the next reset, None while open


@dataclass(frozen=True, slots=True)
class Relation:
    """Where a tip sits relative to a reference tip (the best tip by default)."""

    kind: str  # "same" | "behind" | "ahead" | "fork" | "unknown"
    n: int | None  # blocks behind or ahead; for a fork, the tip's blocks past the fork point
    fork_hash: str | None
    fork_height: int | None
    depth_ours: int | None  # reference blocks after the fork point
    depth_theirs: int | None  # tip blocks after the fork point
    tip_height: int | None


@dataclass(slots=True)
class _Entry:
    """A validated block awaiting placement; `height` is the BIP34 height or the caller's hint."""

    hash: str
    prev_hash: str
    height: int | None
    time: int
    bits: int
    miner: str | None
    template: str | None
    tag: str
    extranonce: str
    first_seen_at: float | None
    body: bool
    trusted: bool


@dataclass(slots=True)
class _Cycle:
    """Per-reset sawtooth state; `scanned` is the highest height checked for the end of the fast phase."""

    d_pre: float | None
    scanned: int
    fast_end: int | None = None


class Chain:
    """Block tree with an incrementally maintained canonical chain."""

    def __init__(self, params: NetworkParams, settle_depth: int = DEFAULT_SETTLE_DEPTH) -> None:
        """Create an empty tree for `params`; blocks count as settled `settle_depth` below the best tip."""
        if settle_depth < 0:
            raise ValueError("settle_depth must not be negative")
        self.params = params
        self.settle_depth = settle_depth
        self._limit_target = bits_to_target(params.pow_limit_bits)
        self._nodes: dict[str, Node] = {}
        self._by_height: dict[int, list[Node]] = {}
        self._fork_heights: set[int] = set()  # heights holding more than one block
        self._waiting: dict[str, list[Node]] = {}  # missing parent hash -> its children in the tree
        self._anchors: dict[str, tuple[int, int]] = {}  # missing parent hash -> (height, cumwork)
        self._pending: dict[str, list[_Entry]] = {}  # missing parent hash -> heightless children
        self._pending_hashes: set[str] = set()
        self._detached = 0
        self._floor: int | None = None
        self._best: Node | None = None
        self._canon: list[Node] = []
        self._canon_base = 0
        self._phases: list[Phase | None] = []
        self._reset_heights: list[int] = []
        self._cycles: dict[int, _Cycle] = {}

    @classmethod
    def from_store(
        cls,
        params: NetworkParams,
        store: Any,
        *,
        min_height: int | None = None,
        settle_depth: int = DEFAULT_SETTLE_DEPTH,
    ) -> Chain:
        """Build a chain from `store.load_blocks(min_height)`, anchored on the persisted best tip (meta `best_hash`)."""
        chain = cls(params, settle_depth)
        get_meta = getattr(store, "get_meta", None)
        chain.load(store.load_blocks(min_height), prefer=get_meta("best_hash") if callable(get_meta) else None)
        return chain

    def load(self, rows: Iterable[Mapping[str, Any]], *, prefer: str | None = None) -> int:
        """Add `blocks` table rows (height order, unknown heights last); return how many were placed.

        Malformed rows are skipped. On an empty chain the main chain is anchored
        at the root group holding block `prefer` (the last best tip; reach alone
        could pick a detached forged block above it), else at the one whose
        subtree reaches highest, so a stale branch that crosses the bottom of the
        window cannot capture it.
        """
        entries = [entry for entry in map(_row_entry, rows) if entry is not None]
        if not self._anchors:
            anchor = _pick_anchor(entries, prefer)
            if anchor is not None:
                key, height = anchor
                self._anchors[key] = (height, 0)
        return sum(self._add_entry(entry) is not None for entry in entries)

    def add(
        self,
        header: BlockHeader,
        height: int | None = None,
        *,
        block: Block | None = None,
        miner: str | None = None,
        first_seen_at: float | None = None,
        trusted: bool = True,
    ) -> Node | None:
        """Insert a header, optionally with its parsed block; return its node, or None if it cannot be placed.

        Idempotent: adding a known block upgrades a header-only node with the
        body's coinbase data, or an untrusted body's with a `trusted` one's (see
        `Node.body_trusted`), and keeps the earliest `first_seen_at`. None means
        invalid bits, a height below the pruned floor, a detached block more than
        MAX_DETACHED_AHEAD above the best tip, or a header with neither a known
        parent nor a height (held until its parent arrives).
        Raises ValueError for malformed hashes, time or bits.
        """
        template, tag, extranonce = None, "", ""
        if block is not None:
            if block.header.hash != header.hash:
                raise ValueError("block does not belong to header")
            coinbase = block.coinbase
            if miner is None:
                miner = identify_miner(coinbase, self.params)
            if coinbase is not None:
                template, tag, extranonce = coinbase.template, coinbase.tag, coinbase.extranonce
                if coinbase.height is not None:
                    height = coinbase.height
        entry = _make_entry(
            hash=header.hash,
            prev_hash=header.prev_hash,
            height=height,
            time=header.time,
            bits=header.bits,
            miner=miner,
            template=template,
            tag=tag,
            extranonce=extranonce,
            first_seen_at=first_seen_at,
            body=block is not None,
            trusted=trusted,
        )
        return self._add_entry(entry)

    def get(self, hash: str) -> Node | None:
        """Return the node for `hash`, or None."""
        return self._nodes.get(hash)

    def __contains__(self, hash: object) -> bool:
        """Return True if `hash` is a node in the tree (pending headers are not)."""
        return hash in self._nodes

    def __len__(self) -> int:
        """Return the number of nodes in the tree."""
        return len(self._nodes)

    def best_tip(self) -> Node | None:
        """Return the attached block with the most cumulative work (see the module doc for ties)."""
        return self._best

    def canonical_hash_at(self, height: int) -> str | None:
        """Return the canonical block hash at `height`, or None outside the canonical window."""
        node = self._canon_at(height)
        return node.hash if node is not None else None

    def canonical_nodes(self, since_height: int | None = None) -> list[Node]:
        """Return canonical nodes from `since_height` (default: the window bottom) to the best tip."""
        start = 0 if since_height is None else max(0, since_height - self._canon_base)
        return self._canon[start:]

    def nodes_at(self, height: int) -> list[Node]:
        """Return every node at `height`, canonical or not."""
        return list(self._by_height.get(height, ()))

    def missing_parents(self) -> list[tuple[str, int]]:
        """Return (hash, height) of each missing block that detached nodes are waiting for."""
        return [
            (key, children[0].height - 1)
            for key, children in self._waiting.items()
            if key not in self._anchors
        ]

    def is_ancestor(self, a: str, b: str) -> bool:
        """Return True if `a` is `b` or one of its ancestors."""
        node_a, node_b = self._nodes.get(a), self._nodes.get(b)
        if node_a is None or node_b is None:
            return False
        return self._ancestor_at(node_b, node_a.height) is node_a

    def fork_point(self, a: str, b: str) -> Node | None:
        """Return the latest common ancestor of `a` and `b` (either one itself if on the other's path)."""
        x, y = self._nodes.get(a), self._nodes.get(b)
        if x is None or y is None:
            return None
        low = min(x.height, y.height)
        x, y = self._ancestor_at(x, low), self._ancestor_at(y, low)
        while x is not None and y is not None and x is not y:
            x, y = self._parent(x), self._parent(y)
        return x if x is y else None

    def path(self, from_hash: str, to_hash: str) -> list[Node]:
        """Return the nodes after `from_hash` up to and including `to_hash`, in height order.

        Raises KeyError for an unknown hash and ValueError if `from_hash` is not
        an ancestor of `to_hash`.
        """
        start, end = self._nodes[from_hash], self._nodes[to_hash]
        if self._ancestor_at(end, start.height) is not start:
            raise ValueError(f"{from_hash} is not an ancestor of {to_hash}")
        if self._is_canonical(end):
            base = self._canon_base
            return self._canon[start.height - base + 1 : end.height - base + 1]
        nodes = []
        node = end
        while node is not start:
            nodes.append(node)
            node = self._parent(node)
        nodes.reverse()
        return nodes

    def classify_tip_change(self, old: str | None, new: str) -> TipChange:
        """Describe a tip move from `old` to `new`; fork fields stay None when they cannot be derived."""
        old_node = self._nodes.get(old) if old is not None else None
        new_node = self._nodes.get(new)
        if old_node is None or new_node is None or (fork := self.fork_point(old_node.hash, new)) is None:
            return TipChange(
                old_hash=old,
                old_height=old_node.height if old_node is not None else None,
                new_hash=new,
                new_height=new_node.height if new_node is not None else None,
                fork_hash=None,
                fork_height=None,
                disconnected=0,
                connected=0,
                is_reorg=False,
                disconnected_work=None,
                connected_work=None,
            )
        return TipChange(
            old_hash=old,
            old_height=old_node.height,
            new_hash=new,
            new_height=new_node.height,
            fork_hash=fork.hash,
            fork_height=fork.height,
            disconnected=old_node.height - fork.height,
            connected=new_node.height - fork.height,
            # A move to an ancestor connects nothing: it is a rewind (a node restarting at its
            # finalized height, or a restored database), not a switch between branches.
            is_reorg=old_node is not fork and new_node is not fork,
            disconnected_work=self._work_between(fork, old_node),
            connected_work=self._work_between(fork, new_node),
        )

    def relation(self, tip_hash: str, ref_hash: str | None = None) -> Relation:
        """Relate `tip_hash` to `ref_hash` (default: the best tip)."""
        tip = self._nodes.get(tip_hash)
        ref = self._best if ref_hash is None else self._nodes.get(ref_hash)
        if tip is None or ref is None or (fork := self.fork_point(tip.hash, ref.hash)) is None:
            return Relation(UNKNOWN, None, None, None, None, None, tip.height if tip is not None else None)
        ours, theirs = ref.height - fork.height, tip.height - fork.height
        if tip is ref:
            kind, n = "same", 0
        elif fork is tip:
            kind, n = "behind", ours
        elif fork is ref:
            kind, n = "ahead", theirs
        else:
            kind, n = "fork", theirs
        return Relation(kind, n, fork.hash, fork.height, ours, theirs, tip.height)

    def stale_blocks(self, since_height: int | None = None) -> list[Node]:
        """Return attached non-canonical blocks at least `settle_depth` below the best tip, by height."""
        best = self._best
        if best is None:
            return []
        limit = best.height - self.settle_depth
        low = since_height if since_height is not None else -1
        stale = []
        for height in sorted(h for h in self._fork_heights if low <= h <= limit):
            canonical = self._canon_at(height)
            nodes = [n for n in self._by_height[height] if n is not canonical and n.cumwork is not None]
            stale.extend(sorted(nodes, key=lambda n: n.hash))
        return stale

    def fork_events(self, since_height: int | None = None) -> list[ForkEvent]:
        """Return one event per canonical block with several attached children, by fork height."""
        best = self._best
        if best is None:
            return []
        low = since_height if since_height is not None else -1
        events = []
        for height in sorted(h for h in self._fork_heights if h - 1 >= low):
            fork = self._canon_at(height - 1)
            winner = self._canon_at(height)
            if fork is None or winner is None:
                continue
            children = [self._nodes.get(child) for child in fork.children]
            rivals = [c for c in children if c is not None and c is not winner and c.cumwork is not None]
            if rivals:
                events.append(self._fork_event(fork, winner, rivals, best))
        return events

    def phase_at(self, height: int) -> Phase:
        """Return the sawtooth phase of the canonical block at `height` (all None outside the window)."""
        index = height - self._canon_base
        if not 0 <= index < len(self._canon):
            return Phase(height, None, None, None, None, None, None, False)
        cached = self._phases[index]
        if cached is not None:
            return cached
        node = self._canon[index]
        difficulty = self._difficulty(node.bits)
        slot = bisect_right(self._reset_heights, height) - 1
        if slot < 0:
            phase = Phase(height, None, None, None, difficulty, None, None, node.is_min_diff)
        else:
            reset = self._reset_heights[slot]
            cycle = self._cycles[reset]
            self._scan_fast(reset, height)
            phase = Phase(
                height=height,
                k=height - reset,
                reset_height=reset,
                d_pre=cycle.d_pre,
                difficulty=difficulty,
                d_ratio=difficulty / cycle.d_pre if cycle.d_pre else None,
                fast=cycle.fast_end is None or height < cycle.fast_end,
                min_diff=node.is_min_diff,
            )
        self._phases[index] = phase
        return phase

    def resets(self, since_height: int | None = None) -> list[Reset]:
        """Return the canonical min-difficulty blocks from `since_height` (default: window bottom)."""
        heights = self._reset_heights
        top = self._canon_base + len(self._canon) - 1
        low = self._canon_base if since_height is None else max(self._canon_base, since_height)
        out = []
        for slot in range(bisect_left(heights, low), len(heights)):
            reset = heights[slot]
            node = self._canon[reset - self._canon_base]
            parent, following = self._canon_at(reset - 1), self._canon_at(reset + 1)
            next_reset = heights[slot + 1] if slot + 1 < len(heights) else None
            cycle = self._cycles[reset]
            self._scan_fast(reset, next_reset - 1 if next_reset is not None else top)
            out.append(
                Reset(
                    height=reset,
                    hash=node.hash,
                    time=node.time,
                    miner=node.miner,
                    template=node.template,
                    gap=node.time - parent.time if parent is not None else None,
                    next_dt=following.time - node.time if following is not None else None,
                    forward_dating=node.time - node.first_seen_at if node.first_seen_at is not None else None,
                    d_pre=cycle.d_pre,
                    fast_blocks=cycle.fast_end - reset if cycle.fast_end is not None else None,
                    cycle_blocks=next_reset - reset if next_reset is not None else None,
                )
            )
        return out

    def prune_below(self, height: int) -> int:
        """Drop every block below `height` (clamped to the best tip height); return how many were removed."""
        best = self._best
        if best is None:
            return 0
        height = min(height, best.height)
        if self._floor is not None and height <= self._floor:
            return 0
        removed = 0
        for doomed in [h for h in self._by_height if h < height]:
            for node in self._by_height.pop(doomed):
                del self._nodes[node.hash]
                removed += 1
                if node.cumwork is None:
                    self._detached -= 1
                for child_hash in node.children:
                    child = self._nodes.get(child_hash)
                    if child is not None and child.height >= height:
                        self._waiting.setdefault(node.hash, []).append(child)
                        if node.cumwork is not None:
                            self._anchors[node.hash] = (node.height, node.cumwork)
            self._fork_heights.discard(doomed)
        for key in list(self._waiting):
            alive = [n for n in self._waiting[key] if self._nodes.get(n.hash) is n]
            if alive:
                self._waiting[key] = alive
            else:
                del self._waiting[key]
        for key in [key for key in self._anchors if key not in self._waiting]:
            del self._anchors[key]
        self._floor = height
        self._canon_prune(height)
        return removed

    # -- insertion -------------------------------------------------------------

    def _add_entry(self, entry: _Entry) -> Node | None:
        """Insert `entry`, then any pending headers that were waiting on it or its descendants."""
        node = self._insert(entry)
        stack = [node] if node is not None else []
        while stack:
            parent = stack.pop()
            for child in self._pending.pop(parent.hash, ()):
                self._pending_hashes.discard(child.hash)
                placed = self._insert(child)
                if placed is not None:
                    stack.append(placed)
        return node

    def _insert(self, entry: _Entry) -> Node | None:
        """Place one entry in the tree, attaching whatever its arrival completes."""
        node = self._nodes.get(entry.hash)
        if node is not None:
            self._merge(node, entry)
            return node
        try:
            work = work_from_bits(entry.bits)
        except ParseError:
            return None
        parent = self._nodes.get(entry.prev_hash)
        own_anchor = self._anchors.get(entry.hash)
        parent_anchor = self._anchors.get(entry.prev_hash) if parent is None else None
        if parent is not None:
            height = parent.height + 1
        elif own_anchor is not None:
            height = own_anchor[0]
        elif parent_anchor is not None:
            height = parent_anchor[0] + 1
        else:
            height = entry.height
        if height is None:
            self._hold(entry)
            return None
        if height > MAX_HEIGHT or (self._floor is not None and height < self._floor):
            return None
        attaches = (
            (parent is not None and parent.cumwork is not None)
            or own_anchor is not None
            or not self._anchors
            or parent_anchor is not None
        )
        if not attaches and self._best is not None and height > self._best.height + MAX_DETACHED_AHEAD:
            return None

        node = Node(
            hash=entry.hash,
            prev_hash=parent.hash if parent is not None else entry.prev_hash,
            height=height,
            time=entry.time,
            bits=entry.bits,
            work=work,
            is_min_diff=self._is_min_diff(height, entry.bits),
            miner=entry.miner,
            template=entry.template,
            tag=entry.tag,
            extranonce=entry.extranonce,
            first_seen_at=entry.first_seen_at,
            body=entry.body,
            body_trusted=entry.body and entry.trusted,
        )
        self._nodes[node.hash] = node
        self._index(node)
        waiting = self._waiting.pop(node.hash, None)
        if waiting:
            node.children.extend(child.hash for child in waiting)
        if parent is not None:
            parent.children.append(node.hash)
        else:
            self._waiting.setdefault(node.prev_hash, []).append(node)

        grew_down = False
        if parent is not None and parent.cumwork is not None:
            self._anchors.pop(node.hash, None)
            node.cumwork = parent.cumwork + work
            attached = self._attach_below(node)
        elif own_anchor is not None or not self._anchors:
            # The missing parent of the lowest attached blocks arrived (or this is
            # the first block): it takes the anchor's cumwork and brings its
            # detached ancestors into the main chain.
            self._anchors.pop(node.hash, None)
            node.cumwork = own_anchor[1] if own_anchor is not None else work
            attached = self._attach_ancestors(node)
            grew_down = True
        elif parent_anchor is not None:
            node.cumwork = parent_anchor[1] + work
            attached = self._attach_below(node)
        else:
            self._detached += 1
            if self._detached > MAX_DETACHED and not self._evict_detached(node):
                return None
            return node
        self._after_attach(attached, grew_down)
        return node

    def _merge(self, node: Node, entry: _Entry) -> None:
        """Fold a repeated observation into an existing node."""
        if entry.body and (not node.body or (entry.trusted and not node.body_trusted)):
            replacing = node.body
            node.body, node.body_trusted = True, entry.trusted
            node.template, node.tag, node.extranonce = entry.template, entry.tag, entry.extranonce
            if entry.miner is not None or replacing:
                node.miner = entry.miner
        elif node.miner is None and entry.miner is not None:
            node.miner = entry.miner
        seen = entry.first_seen_at
        if seen is not None and (node.first_seen_at is None or seen < node.first_seen_at):
            node.first_seen_at = seen
            best = self._best
            # An earlier sighting can win an equal-work tie against the current tip.
            if node.cumwork is not None and best is not None and node is not best and self._better(node, best):
                self._switch_best(node)

    def _hold(self, entry: _Entry) -> None:
        """Keep a heightless header until its parent arrives, evicting the oldest past MAX_PENDING."""
        if entry.hash in self._pending_hashes:
            return
        self._pending.setdefault(entry.prev_hash, []).append(entry)
        self._pending_hashes.add(entry.hash)
        while len(self._pending_hashes) > MAX_PENDING:
            for dropped in self._pending.pop(next(iter(self._pending))):
                self._pending_hashes.discard(dropped.hash)

    def _attach_below(self, root: Node) -> list[Node]:
        """Give cumwork to the detached subtree under attached `root`; return root plus newly attached nodes."""
        attached = [root]
        stack = [root]
        while stack:
            parent = stack.pop()
            for child_hash in parent.children:
                child = self._nodes[child_hash]
                if child.cumwork is None:
                    child.cumwork = parent.cumwork + child.work
                    self._detached -= 1
                    if child.height != parent.height + 1:
                        self._move(child, parent.height + 1)
                    attached.append(child)
                    stack.append(child)
        return attached

    def _attach_ancestors(self, node: Node) -> list[Node]:
        """Attach `node`'s detached ancestors (and their subtrees) below it; return newly attached nodes."""
        path = [node]
        child = node
        parent = self._nodes.get(node.prev_hash)
        while parent is not None and parent.cumwork is None:
            parent.cumwork = child.cumwork - child.work
            self._detached -= 1
            if parent.height != child.height - 1:
                self._move(parent, child.height - 1)
            path.append(parent)
            child = parent
            parent = self._nodes.get(child.prev_hash)
        attached: list[Node] = []
        for member in path:
            attached.extend(self._attach_below(member))
        lowest = path[-1]
        if parent is None:
            anchor = (lowest.height - 1, lowest.cumwork - lowest.work)
            self._anchors[lowest.prev_hash] = anchor
            for sibling in self._waiting.get(lowest.prev_hash, ()):
                if sibling.cumwork is None:
                    sibling.cumwork = anchor[1] + sibling.work
                    self._detached -= 1
                    if sibling.height != anchor[0] + 1:
                        self._move(sibling, anchor[0] + 1)
                    attached.extend(self._attach_below(sibling))
        return attached

    def _after_attach(self, attached: list[Node], grew_down: bool) -> None:
        """Extend the canonical chain downward if needed and adopt a better tip among `attached`."""
        if grew_down and self._canon:
            self._extend_canon_down()
        candidate = attached[0]
        for node in attached[1:]:
            if self._better(node, candidate):
                candidate = node
        if self._best is None or self._better(candidate, self._best):
            self._switch_best(candidate)

    def _evict_detached(self, keep: Node) -> bool:
        """Drop the oldest detached groups until at most MAX_DETACHED remain; return whether `keep` survived."""
        for key in list(self._waiting):
            if self._detached <= MAX_DETACHED:
                break
            if key not in self._anchors:
                for root in self._waiting.pop(key):
                    self._remove_subtree(root)
        return self._nodes.get(keep.hash) is keep

    def _remove_subtree(self, root: Node) -> None:
        """Remove a detached group root (its parent is missing) and all of its descendants."""
        stack = [root]
        while stack:
            node = stack.pop()
            if self._nodes.get(node.hash) is not node:
                continue
            del self._nodes[node.hash]
            self._unindex(node)
            if node.cumwork is None:
                self._detached -= 1
            stack.extend(self._nodes[child] for child in node.children if child in self._nodes)

    # -- tree helpers ------------------------------------------------------------

    def _parent(self, node: Node) -> Node | None:
        """Return the parent of `node`, or None if missing.

        Requiring parent height == height - 1 makes every downward walk terminate
        even on crafted data with hash cycles.
        """
        parent = self._nodes.get(node.prev_hash)
        return parent if parent is not None and parent.height == node.height - 1 else None

    def _ancestor_at(self, node: Node, height: int) -> Node | None:
        """Return the ancestor of `node` at `height`, jumping via the canonical index once on it."""
        if height > node.height:
            return None
        current: Node | None = node
        while current is not None and current.height > height:
            if self._is_canonical(current):
                return self._canon_at(height)
            current = self._parent(current)
        return current

    def _work_between(self, ancestor: Node, node: Node) -> int:
        """Return the work of the blocks after `ancestor` up to and including `node`."""
        if ancestor.cumwork is not None and node.cumwork is not None:
            return node.cumwork - ancestor.cumwork
        total = 0
        current: Node | None = node
        while current is not None and current is not ancestor:
            total += current.work
            current = self._parent(current)
        return total

    def _better(self, a: Node, b: Node) -> bool:
        """Return True if tip `a` beats tip `b`: more work, then earlier first seen, then greater raw hash."""
        if a.cumwork != b.cumwork:
            return a.cumwork > b.cumwork
        seen_a = a.first_seen_at if a.first_seen_at is not None else math.inf
        seen_b = b.first_seen_at if b.first_seen_at is not None else math.inf
        if seen_a != seen_b:
            return seen_a < seen_b
        return _raw_key(a.hash) > _raw_key(b.hash)

    def _is_min_diff(self, height: int, bits: int) -> bool:
        """Return True for pow-limit bits where the Testnet minimum-difficulty rule is active."""
        start = self.params.min_diff_after_height
        return start is not None and height >= start and bits == self.params.pow_limit_bits

    def _difficulty(self, bits: int) -> float:
        """Return the RPC-style difficulty of valid compact `bits`."""
        return self._limit_target / bits_to_target(bits)

    def _index(self, node: Node) -> None:
        """Add `node` to the height index."""
        bucket = self._by_height.setdefault(node.height, [])
        bucket.append(node)
        if len(bucket) == 2:
            self._fork_heights.add(node.height)

    def _unindex(self, node: Node) -> None:
        """Remove `node` from the height index."""
        bucket = self._by_height[node.height]
        bucket.remove(node)
        if len(bucket) < 2:
            self._fork_heights.discard(node.height)
        if not bucket:
            del self._by_height[node.height]

    def _move(self, node: Node, height: int) -> None:
        """Correct the height of a not-yet-canonical node to match its neighbour."""
        self._unindex(node)
        node.height = height
        node.is_min_diff = self._is_min_diff(height, node.bits)
        self._index(node)

    # -- canonical chain -----------------------------------------------------------

    def _canon_at(self, height: int) -> Node | None:
        """Return the canonical node at `height`, or None."""
        index = height - self._canon_base
        return self._canon[index] if 0 <= index < len(self._canon) else None

    def _is_canonical(self, node: Node) -> bool:
        """Return True if `node` is on the canonical chain."""
        index = node.height - self._canon_base
        return 0 <= index < len(self._canon) and self._canon[index] is node

    def _switch_best(self, new: Node) -> None:
        """Make `new` the best tip and update the canonical chain from the fork point."""
        old = self._best
        self._best = new
        if old is not None and new.prev_hash == old.hash:
            self._canon_append((new,))
            return
        branch = []
        node: Node | None = new
        while node is not None and not self._is_canonical(node):
            branch.append(node)
            node = self._parent(node)
        branch.reverse()
        if node is None:
            self._canon, self._phases, self._reset_heights, self._cycles = [], [], [], {}
            self._canon_base = branch[0].height
        else:
            self._canon_truncate(node.height)
        self._canon_append(branch)

    def _canon_append(self, nodes: Iterable[Node]) -> None:
        """Append blocks to the canonical chain, recording resets."""
        canon = self._canon
        for node in nodes:
            if node.is_min_diff:
                d_pre = self._difficulty(canon[-1].bits) if canon else None
                self._cycles[node.height] = _Cycle(d_pre=d_pre, scanned=node.height)
                self._reset_heights.append(node.height)
            canon.append(node)
            self._phases.append(None)

    def _canon_truncate(self, height: int) -> None:
        """Keep canonical heights up to `height`, rolling back reset state above it."""
        keep = height - self._canon_base + 1
        del self._canon[keep:]
        del self._phases[keep:]
        while self._reset_heights and self._reset_heights[-1] > height:
            del self._cycles[self._reset_heights.pop()]
        if self._reset_heights:
            reset = self._reset_heights[-1]
            cycle = self._cycles[reset]
            cycle.scanned = min(cycle.scanned, height)
            if cycle.fast_end is not None and cycle.fast_end > height:
                cycle.fast_end = None

    def _extend_canon_down(self) -> None:
        """Prepend newly attached ancestors of the canonical root."""
        below = []
        node = self._parent(self._canon[0])
        while node is not None:
            below.append(node)
            node = self._parent(node)
        if not below:
            return
        below.reverse()
        self._canon[:0] = below
        self._canon_base = below[0].height
        new_resets = [n.height for n in below if n.is_min_diff]
        for reset in new_resets:
            self._cycles[reset] = _Cycle(d_pre=None, scanned=reset)
        self._reset_heights[:0] = new_resets
        for reset, cycle in self._cycles.items():
            if cycle.d_pre is None and (before := self._canon_at(reset - 1)) is not None:
                cycle.d_pre = self._difficulty(before.bits)
        # Phases before the old first reset (and its d_pre) may have changed.
        self._phases = [None] * len(self._canon)

    def _canon_prune(self, height: int) -> None:
        """Drop canonical heights below `height`, keeping the reset that governs the new bottom."""
        cut = height - self._canon_base
        if cut <= 0:
            return
        governing = bisect_left(self._reset_heights, height) - 1
        if governing >= 0:
            reset = self._reset_heights[governing]
            following = governing + 1
            end = (
                self._reset_heights[following] - 1
                if following < len(self._reset_heights)
                else self._canon_base + len(self._canon) - 1
            )
            # Finish the fast-phase scan while the blocks it needs are still here.
            self._scan_fast(reset, end)
            for old in self._reset_heights[:governing]:
                del self._cycles[old]
            del self._reset_heights[:governing]
        del self._canon[:cut]
        del self._phases[:cut]
        self._canon_base = height

    def _scan_fast(self, reset: int, upto: int) -> None:
        """Advance the search for the end of `reset`'s fast phase, up to height `upto`.

        It ends at the first height whose trailing averaging window holds only blocks from the
        reset on and averages at least half the target spacing, both under the rules at that
        height. So a cycle still fast at NU7 activation cannot end before NU7's wider window has
        passed its reset.
        """
        cycle = self._cycles[reset]
        if cycle.fast_end is not None:
            return
        canon, base = self._canon, self._canon_base
        upto = min(upto, base + len(canon) - 1)
        height = cycle.scanned + 1
        while height <= upto:
            rules = self.params.difficulty_rules(height)
            window = rules.averaging_window
            if height - window >= reset:
                start = height - window - base
                if start < 0:
                    break
                # 2 * window * (target / 2), in integers
                if 2 * (canon[start + window].time - canon[start].time) >= window * rules.target_spacing:
                    cycle.fast_end = cycle.scanned = height
                    return
            height += 1
        cycle.scanned = height - 1

    # -- fork events -----------------------------------------------------------------

    def _fork_event(self, fork: Node, winner: Node, rivals: list[Node], best: Node) -> ForkEvent:
        """Build the ForkEvent for canonical `fork` whose canonical child is `winner`."""
        losers = []
        top = 0
        for rival in rivals:
            branch, tip = self._loser_branch(fork, winner, rival)
            losers.append(branch)
            top = max(top, tip.height)
        losers.sort(key=lambda b: (-b.length, b.block.hash))
        kinds = {b.classification for b in losers}
        seen = [b.seen_first for b in losers]
        greater = [b.greater_raw_hash for b in losers]
        return ForkEvent(
            fork_hash=fork.hash,
            fork_height=fork.height,
            fork_time=fork.time,
            winner=_ref(winner),
            losers=tuple(losers),
            depth=max(b.length for b in losers),
            depth_work=max(b.work for b in losers),
            classification=RACE if RACE in kinds else SELF if kinds == {SELF} else UNKNOWN,
            same_job=any(b.same_job for b in losers),
            winner_first_seen=None if None in seen else not any(seen),
            winner_greater_raw_hash=None if None in greater else not any(greater),
            equal_work=all(b.equal_work for b in losers),
            settled=best.height >= top + self.settle_depth,
        )

    def _loser_branch(self, fork: Node, winner: Node, first: Node) -> tuple[LoserBranch, Node]:
        """Summarize the subtree under `first` (a non-canonical child of `fork`); also return its tip."""
        tip, count = first, 0
        stack = [first]
        while stack:
            node = stack.pop()
            count += 1
            if self._better(node, tip):
                tip = node
            for child_hash in node.children:
                child = self._nodes.get(child_hash)
                if child is not None and child.cumwork is not None:
                    stack.append(child)
        miners = []
        node: Node | None = tip
        while node is not None and node is not fork:
            miners.append(node.miner or UNKNOWN)
            node = self._parent(node)
        miners.reverse()
        work = tip.cumwork - fork.cumwork
        kind = _classify(first.miner, winner.miner)
        branch = LoserBranch(
            block=_ref(first),
            tip_hash=tip.hash,
            length=tip.height - fork.height,
            work=work,
            blocks=count,
            miners=tuple(miners),
            classification=kind,
            same_job=kind == SELF and first.time == winner.time,
            seen_first=_strictly_before(first.first_seen_at, winner.first_seen_at),
            greater_raw_hash=_raw_greater(first.hash, winner.hash),
            equal_work=first.work == winner.work,
            winner_len=self._blocks_to_match(fork, work),
        )
        return branch, tip

    def _blocks_to_match(self, fork: Node, work: int) -> int | None:
        """Return how many canonical blocks after canonical `fork` it takes to accumulate `work`."""
        base = self._canon_base
        low = fork.height - base + 1
        index = bisect_left(self._canon, fork.cumwork + work, low, len(self._canon), key=_cumwork)
        return index - low + 1 if index < len(self._canon) else None


def _cumwork(node: Node) -> int:
    """Sort key for bisecting the canonical chain (always attached)."""
    return node.cumwork


def _ref(node: Node) -> BlockRef:
    """Copy the reportable fields of `node`."""
    return BlockRef(
        hash=node.hash,
        height=node.height,
        time=node.time,
        miner=node.miner,
        template=node.template,
        first_seen_at=node.first_seen_at,
        is_min_diff=node.is_min_diff,
        body=node.body,
    )


def _classify(loser: str | None, winner: str | None) -> str:
    """Return "self" for the same attributed miner on both sides, "race" for different ones."""
    if loser in (None, UNKNOWN) or winner in (None, UNKNOWN):
        return UNKNOWN
    return SELF if loser == winner else RACE


def _strictly_before(a: float | None, b: float | None) -> bool | None:
    """Return a < b, or None when either is unknown or they are equal."""
    if a is None or b is None or a == b:
        return None
    return a < b


def _raw_key(block_hash: str) -> bytes:
    """Return the raw-hash tie-break key; malformed hashes sort lowest."""
    try:
        return raw_hash_key(block_hash)
    except ValueError:
        return b""


def _raw_greater(a: str, b: str) -> bool | None:
    """Return whether `a`'s raw hash is above `b`'s, or None if either is malformed."""
    key_a, key_b = _raw_key(a), _raw_key(b)
    return key_a > key_b if key_a and key_b else None


def _make_entry(
    *,
    hash: Any,
    prev_hash: Any,
    height: Any,
    time: Any,
    bits: Any,
    miner: Any,
    template: Any,
    tag: Any,
    extranonce: Any,
    first_seen_at: Any,
    body: bool,
    trusted: bool = True,
) -> _Entry:
    """Validate and bound one block's fields; ValueError for malformed hashes, time or bits."""
    block_hash, prev = _hash_arg(hash, "hash"), _hash_arg(prev_hash, "prev_hash")
    if block_hash == prev:
        raise ValueError("block is its own parent")
    time, bits = _int_arg(time, "time"), _int_arg(bits, "bits")
    if not 0 <= time <= 0xFFFF_FFFF or not 0 <= bits <= 0xFFFF_FFFF:
        raise ValueError("time or bits out of range")
    height = _int_arg(height, "height") if height is not None else None
    if height is not None and not 0 <= height <= MAX_HEIGHT:
        height = None  # an untrusted hint or BIP34 value that cannot be real
    seen = float(first_seen_at) if first_seen_at is not None else None
    return _Entry(
        hash=block_hash,
        prev_hash=prev,
        height=height,
        time=time,
        bits=bits,
        miner=_label(miner),
        template=_label(template),
        tag=_label(tag) or "",
        extranonce=_label(extranonce) or "",
        first_seen_at=seen if seen is not None and math.isfinite(seen) else None,
        body=bool(body),
        trusted=bool(trusted),
    )


def _row_entry(row: Mapping[str, Any]) -> _Entry | None:
    """Convert a `blocks` table row to an entry, or None if it is malformed."""
    try:
        trusted = row["body_trusted"]
    except (IndexError, KeyError):  # rows written before the column existed
        trusted = 1
    try:
        return _make_entry(
            hash=row["hash"],
            prev_hash=row["prev_hash"],
            height=row["height"],
            time=row["time"],
            bits=row["bits"],
            miner=row["miner"],
            template=row["template"],
            tag=row["miner_tag"],
            extranonce=row["extranonce"],
            first_seen_at=row["first_seen_at"],
            body=bool(row["body"]),
            trusted=trusted != 0,
        )
    except (IndexError, KeyError, TypeError, ValueError):
        return None


def _pick_anchor(entries: list[_Entry], prefer: str | None = None) -> tuple[str, int] | None:
    """Return (missing parent hash, its height) for the root group holding `prefer`, else the one reaching highest.

    Ties go to the larger subtree. Groups without any known root height are skipped.
    """
    known = {entry.hash: entry for entry in entries}
    children: dict[str, list[_Entry]] = {}
    for entry in entries:
        children.setdefault(entry.prev_hash, []).append(entry)
    walk = known.get(prefer) if prefer is not None else None
    for _ in range(len(known)):  # bounded: crafted rows may hold hash cycles
        if walk is None or walk.prev_hash not in known:
            break
        walk = known[walk.prev_hash]
    preferred = walk.prev_hash if walk is not None else None
    best: tuple[tuple[bool, int, int], str, int] | None = None
    for key, roots in children.items():
        heights = [root.height for root in roots if root.height is not None]
        if key in known or not heights:
            continue
        base = min(heights)
        reach, count = base, 0
        stack = [(root, base) for root in roots]
        while stack:
            entry, height = stack.pop()
            count += 1
            reach = max(reach, height)
            stack.extend((child, height + 1) for child in children.get(entry.hash, ()))
        score = (key == preferred, reach, count)
        if best is None or score > best[0]:
            best = (score, key, base - 1)
    return (best[1], best[2]) if best is not None else None


def _hash_arg(value: Any, what: str) -> str:
    """Return a lowercased block hash string, or raise ValueError."""
    if not isinstance(value, str) or not 0 < len(value) <= MAX_HASH_LEN:
        raise ValueError(f"invalid {what}: {value!r:.80}")
    return value.lower()


def _int_arg(value: Any, what: str) -> int:
    """Return `value` if it is a plain integer, or raise ValueError."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"invalid {what}: {value!r:.80}")
    return value


def _label(value: Any) -> str | None:
    """Return `value` as a length-capped string, or None."""
    return None if value is None else str(value)[:MAX_LABEL_LEN]
