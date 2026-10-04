"""Summaries of the block tree and the store for the dashboard, the JSON API and the `report` CLI.

Every public function returns plain JSON-able data (dict, list, str, int,
finite float, bool, None) whose keys are documented in its docstring; the page
is built against those shapes, so keys are only ever added. Conventions:
- rates and shares are fractions (0.0116, not 1.16 %), rounded to 6
  significant digits; times are unix seconds; durations are seconds;
  difficulty is RPC-style (pow-limit target / target);
- "block" dicts are `{"hash", "height", "time", "miner", "template",
  "first_seen_at", "min_diff", "body"}`;
- "phase" dicts are `{"height", "k", "reset_height", "d_pre", "difficulty",
  "d_ratio", "fast", "min_diff", "label", "k_bucket", "ratio_bucket"}` with
  `label` "fast" | "slow" | "unknown" (see `chain.Phase`);
- "relation" dicts are `{"kind", "n", "fork_hash", "fork_height",
  "depth_ours", "depth_theirs", "tip_height"}` (see `chain.Relation`).

Block statistics are bucketed by header time, not first-seen time, because
backfilled blocks have no first-seen time; header times are
miner-written and run up to ~150 s ahead at resets. An orphan is a settled
stale block (`chain.settle_depth` below the best tip) and takes the phase and
time of the canonical block at its height, as in the historical analysis; rates divide by
settled canonical blocks. Statistics cover the chain's in-memory window, minus
heights whose canonical block was not watched live (`OBSERVED_LAG`): backfill
fetches only canonical blocks, so their competitors were never observable.

Threading: functions that take `chain` or `monitor` read the mutable `Chain`
(whose queries also fill caches), so they must run on the event-loop thread
that owns it; web handler threads have to hop onto the loop (for example
`asyncio.run_coroutine_threadsafe`) or serve cached results. `propagation` and
`probe_stats` take only a connection and are safe on any thread with that
thread's `Store.reader()`. Wherever `conn` is optional, None leaves the
database-derived fields None or empty.

Notes:
- `conn` is optional in the chain-based functions, which also take a
  keyword-only `now` (default: wall clock) instead of reading the clock.
- `orphan_stats` and `miner_stats` accept `exclude_heights` (inclusive height
  ranges, e.g. a known partition incident) to reproduce baseline tables.
- `probe_stats` takes an optional `chain` to mark incident blocks canonical.
- Extra public functions: `resets` (for `/api/resets`), `group_sources` and
  `detect_split` (the pure half of the service's split state machine).
- `orphan_stats` reports cycles that never slowed down as "fast" (the chain's
  causal phase); the historical analysis labels those "unknown".
"""

from __future__ import annotations

import dataclasses
import itertools
import math
import re
import time
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .chain import RACE, SELF, UNKNOWN, Chain, ForkEvent, LoserBranch, Phase, Relation, Reset
from .store import UNTIMED_KINDS

# Stuck rule: a non-canonical tip whose header time or first sighting is over an hour old is a dead fork ...
STUCK_AGE = 3600.0
# ... and a tip this many blocks behind the best tip is stuck even on the canonical chain.
STUCK_BEHIND = 1000
# Fast-phase blocks arrive every ~3 s, so a few blocks behind is ordinary propagation lag.
SYNCED_LAG = 3
# Sources with no successful contact or tip update for this long are left out of grouping.
ACTIVE_WINDOW = 900.0
# "idle" is a stored row nobody is connected to (e.g. from before a restart); recency decides for it.
ACTIVE_STATUSES = frozenset({"connected", "ok"})
# These hold no chain of their own, so their tips never define a branch.
NON_NODE_IMPLS = frozenset({"zeeder", "monitor"})
# Sightings that mean the source put the block on its best chain (chaintip/getdata do not).
ADOPTION_KINDS = ("inv", "headers", "rpc_tip")
# Sightings pushed (inv) or polled at ~1 s (rpc_tip), so their times measure propagation.
TIMELY_KINDS = ("inv", "rpc_tip")
# A canonical block first seen this long after its header time was fetched after the fact (backfill,
# a restart gap), when blocks competing at its height could not be observed. Live blocks arrive within
# seconds, or up to ~150 s before their forward-dated header time at resets.
OBSERVED_LAG = 600.0
# Arrival order needs this gap over the later block's first timely sighting (the 1 s RPC tip poll).
SEEN_MARGIN = 1.0
PERIODS = (("1h", 3_600), ("24h", 86_400), ("7d", 7 * 86_400))
HOURLY_WINDOW = 72 * 3_600
DEFAULT_PROPAGATION_WINDOW = 6 * 3_600.0
DEFAULT_PROBE_WINDOW = 86_400.0
# Blocks-since-reset buckets of the historical (pre-NU7) analysis, kept comparable across NU7; 17 was its
# averaging window.
K_BUCKETS = ((0, 0, "0"), (1, 17, "1-17"), (18, 50, "18-50"), (51, 100, "51-100"), (101, 200, "101-200"),
             (201, 300, "201-300"), (301, 400, "301-400"), (401, None, ">=401"))
K_FINE_BUCKETS = ((0, 0, "0"), (1, 1, "1"), (2, 2, "2"), (3, 5, "3-5"), (6, 10, "6-10"), (11, 17, "11-17"),
                  (18, 30, "18-30"))
# Difficulty relative to the pre-reset level, lower bound inclusive (as in the historical analysis).
RATIO_BUCKETS = ((None, 0.001, "<0.001"), (0.001, 0.01, "0.001-0.01"), (0.01, 0.1, "0.01-0.1"),
                 (0.1, 0.5, "0.1-0.5"), (0.5, 1.0, "0.5-1"), (1.0, None, ">=1"))
PHASE_LABELS = ("fast", "slow", UNKNOWN)
STATES = ("synced", "lagging", "fork", "stuck", UNKNOWN, "inactive")
CANONICAL = "canonical"
MAX_FORK_LIMIT = 200
MAX_RESET_LIMIT = 1_000
MAX_SAWTOOTH = 10_000
MAX_MINERS = 100
MAX_PAIRS = 20
MAX_LOSERS = 10
MAX_BRANCH_MINERS = 50
# Blocks per branch whose sightings and tip changes are searched for adoption.
MAX_BRANCH_HASHES = 200
MAX_EXCLUDE_RANGES = 64
MAX_SAMPLE = 20  # sources or blocks listed by name inside one aggregate
MAX_INCIDENTS = 50
MAX_RECENT = 50
MAX_SOURCES = 5_000
MAX_PROPAGATION_BLOCKS = 1_000
# Height span searched for recent blocks, so the query stays an index range scan.
PROPAGATION_HEIGHT_SPAN = 20_000
MAX_PROBE_ROWS = 200_000
MAX_TEXT = 256
MAX_PLAIN_ITEMS = 1_000
MAX_PLAIN_DEPTH = 4
# Below SQLITE_MAX_VARIABLE_NUMBER (999) on every SQLite build.
SQL_CHUNK = 500
PROBE_RESULTS = ("block", "notfound", "timeout", "error")
# Announce probes reach every implementation but only for blocks unknown at inv time; reprobes are
# sampled, go to Zebra only and target blocks already held. Pooling them would compare different
# populations, so each gets its own per-group table. Body fetches ("fetch") ask peers that never
# announced the block, so they only count in `by_reason`.
ANNOUNCE_REASON, REPROBE_REASON = "announce", "reprobe"
GROUP_REASONS = (ANNOUNCE_REASON, REPROBE_REASON)

_VERSION_RE = re.compile(r"v?(\d{1,4})\.(\d{1,4})")
_HASH_RE = re.compile(r"[0-9a-fA-F]{64}")

_BLOCKS, _ORPHANS, _FORKS, _RESETS = range(4)


# -- live view: sources, groups and splits ---------------------------------------------


def live_snapshot(monitor: Any, now: float | None = None) -> dict[str, Any]:
    """Build the live dashboard snapshot, including the split-detection inputs.

    Reads `monitor.chain`, `monitor.store.reader()`, `monitor.config` and, when
    present, `monitor.p2p.snapshot()` and `monitor.rpc_status()`; failures of
    the optional collectors are reported, never raised. Runs on the loop thread.

    Returns:
        {"generated_at": float, "network": str,
         "tip": block + {"age_s"} | None, "phase": phase | None,
         "chain": {"blocks", "from_height", "to_height", "settle_depth", "missing_parents"},
         "peers": [peer view, see `peers`],
         "groups": [group, see `group_sources`],
         "split_candidate": `detect_split(groups)` result | None,
         "stuck": [peer views with stuck true],
         "collectors": {"rpc": [rpc status dict], "rpc_error": str | None,
                        "p2p": {"enabled", "error", "peers", "connected", "connected_by_group": {group: n}}},
         "recent_reorgs": [tip change, see `summary`] (newest first, <= 20),
         "recent_forks": [fork event without DB fields, see `fork_events`] (newest first, <= 10)}
    """
    now = _now(now)
    chain: Chain = monitor.chain
    best = chain.best_tip()
    conn = _monitor_conn(monitor)
    p2p = getattr(monitor, "p2p", None)
    live, p2p_error = _p2p_live(p2p)
    views = _views(monitor, conn, now, live)
    groups = _group_views(chain, views)
    rpc, rpc_error = _rpc_status(monitor, views)
    connected = [v for v in views if v["kind"] == "p2p" and (v["status"] == "connected" or _live_connected(v["live"]))]
    config = getattr(monitor, "config", None)
    p2p_config = getattr(config, "p2p", None)
    canonical = chain.canonical_nodes() if best is not None else []
    recent_forks = []
    if best is not None:
        events = chain.fork_events(since_height=best.height - 500)
        recent_forks = [_fork_dict(chain, event) for event in reversed(events[-10:])]
    return {
        "generated_at": now,
        "network": chain.params.name,
        "tip": _tip(best, now),
        "phase": _phase(chain.phase_at(best.height)) if best is not None else None,
        "chain": {
            "blocks": len(chain),
            "from_height": canonical[0].height if canonical else None,
            "to_height": best.height if best is not None else None,
            "settle_depth": chain.settle_depth,
            "missing_parents": len(chain.missing_parents()),
        },
        "peers": views,
        "groups": groups,
        "split_candidate": detect_split(groups),
        "stuck": [view for view in views if view["stuck"]],
        "collectors": {
            "rpc": rpc,
            "rpc_error": rpc_error,
            "p2p": {
                "enabled": p2p is not None or bool(getattr(p2p_config, "enabled", False)),
                "error": p2p_error,
                "peers": len(live),
                "connected": len(connected),
                "connected_by_group": dict(Counter(v["group"] for v in connected)),
            },
        },
        "recent_reorgs": _recent_reorgs(conn, 20) if conn is not None else [],
        "recent_forks": recent_forks,
    }


def peers(monitor: Any, now: float | None = None) -> list[dict[str, Any]]:
    """Return every vantage point with its tip's relation to the best tip (loop thread only).

    Rows come from the `sources` table, plus configured RPC endpoints and live
    P2P peers not yet stored. RPC sources come first (by name), then the rest
    by group and source.

    Returns a list of peer views:
        {"source", "kind": "rpc" | "p2p", "fleet": bool, "impl", "version", "group",
         "user_agent", "status", "active": bool,
         "state": "synced" | "lagging" | "fork" | "stuck" | "unknown" | "inactive", "stuck": bool,
         "tip_hash", "tip_height", "tip_at", "tip_age_s", "behind", "relation": relation,
         "start_height", "first_seen_at", "last_ok_at", "last_error", "last_error_at",
         "live": dict from `p2p.snapshot()` | None}
    `tip_age_s` counts from the earlier of the tip block's first sighting and its
    header time (else `tip_at`); `behind` is best height minus tip height (minus the version
    message start height when no tip is known, e.g. a peer on a fork below our window).
    """
    now = _now(now)
    live, _ = _p2p_live(getattr(monitor, "p2p", None))
    return _views(monitor, _monitor_conn(monitor), now, live)


def group_sources(chain: Chain, sources: Iterable[Mapping[str, Any]], now: float) -> list[dict[str, Any]]:
    """Group vantage points (`sources` table rows) by implementation and version.

    Each active member with a known tip that is not stuck joins a branch
    cluster: "canonical" when its tip is on the best chain, otherwise the hash of
    the first block after the canonical fork point. The group's `branch` is the
    cluster with the most members (the canonical one on ties).

    Returns groups sorted by active members, most first:
        {"key": "zebra 6.4", "impl", "version", "members", "active", "stuck", "fleet",
         "states": {state: n for every state in STATES},
         "branch": branch | None, "branches": [branch, ...], "stuck_sources": [source, ...]}
    where branch = {"key": "canonical" | fork-child hash, "tip_hash", "tip_height",
                    "fork_hash", "fork_height", "relation": relation of tip_hash,
                    "members", "sources": [source, ...] (<= 20)}.
    """
    now = _finite_arg(now, "now")
    best = chain.best_tip()
    context = _Context()
    rows = itertools.islice(sources, MAX_SOURCES)
    return _group_views(chain, [_view(chain, dict(row), now, best, context) for row in rows])


def detect_split(groups: Sequence[Mapping[str, Any]], *, min_members: int = 1) -> dict[str, Any] | None:
    """Return a split description when non-stuck groups sit on conflicting branches, else None.

    Two group branches conflict when they are different side branches, or when
    one is canonical and reaches past the other's fork point (a canonical group
    still below the fork is only lagging). Groups whose branch has fewer than
    `min_members` members are ignored. Persistence (> 30 s) is the caller's job.

    Returns:
        {"key": fork hash identifying the split (stable while tips advance),
         "fork_hash", "fork_height", "depth": deepest side past the fork,
         "sides": [{"branch", "tip_hash", "tip_height", "blocks_past_fork", "groups": [key], "members"}],
         "groups": {group key: branch key}, "neutral_groups": [group keys not on any conflicting side]}
    """
    min_members = max(1, _int_arg(min_members, "min_members"))
    sides: dict[str, dict[str, Any]] = {}
    for group in groups:
        branch = group.get("branch")
        if not branch or branch.get("members", 0) < min_members:
            continue
        side = sides.setdefault(
            branch["key"],
            {"branch": branch["key"], "tip_hash": None, "tip_height": None, "fork_hash": branch.get("fork_hash"),
             "fork_height": branch.get("fork_height"), "groups": [], "members": 0},
        )
        side["groups"].append(group["key"])
        side["members"] += branch.get("members", 0)
        height = branch.get("tip_height")
        if height is not None and (side["tip_height"] is None or height > side["tip_height"]):
            side["tip_height"], side["tip_hash"] = height, branch.get("tip_hash")
    keys = list(sides)
    conflicting: set[str] = set()
    for index, a in enumerate(keys):
        for b in keys[index + 1 :]:
            if _conflict(sides[a], sides[b]):
                conflicting.update((a, b))
    if not conflicting:
        return None
    forks = [sides[key] for key in conflicting if key != CANONICAL and sides[key]["fork_height"] is not None]
    root = min(forks, key=lambda side: side["fork_height"]) if forks else None
    fork_height = root["fork_height"] if root is not None else None
    out_sides = []
    for key in sorted(conflicting, key=lambda k: (-sides[k]["members"], k != CANONICAL, k)):
        side = sides[key]
        past = (
            side["tip_height"] - fork_height if side["tip_height"] is not None and fork_height is not None else None
        )
        out_sides.append(
            {"branch": key, "tip_hash": side["tip_hash"], "tip_height": side["tip_height"],
             "blocks_past_fork": past, "groups": sorted(side["groups"]), "members": side["members"]}
        )
    depths = [side["blocks_past_fork"] for side in out_sides if side["blocks_past_fork"] is not None]
    fork_hash = root["fork_hash"] if root is not None else None
    return {
        "key": fork_hash or "|".join(sorted(conflicting)),
        "fork_hash": fork_hash,
        "fork_height": fork_height,
        "depth": max(depths) if depths else None,
        "sides": out_sides,
        "groups": {group: side["branch"] for side in out_sides for group in side["groups"]},
        "neutral_groups": sorted(g for key, side in sides.items() if key not in conflicting for g in side["groups"]),
    }


# -- history: summaries, forks, orphans, miners, sawtooth -------------------------------------


def summary(chain: Chain, conn: Any = None, now: float | None = None) -> dict[str, Any]:
    """Return the headline numbers for the stat tiles.

    Returns:
        {"generated_at", "network", "tip": block + {"age_s"} | None, "phase": phase | None,
         "window": {"from_height", "to_height", "from_time", "blocks_in_memory"},
         "periods": {"1h" | "24h" | "7d": {
             "seconds", "partial": bool (the window starts after the period does, or `unobserved` > 0),
             "blocks": settled canonical blocks watched live, "unobserved": settled canonical blocks left
             out (not watched live), "orphans", "orphan_rate", "self_orphans",
             "forks": fork events (settled or not), "deepest_fork": blocks, "resets",
             "reorgs": tip changes with is_reorg, "reorged_sources", "deepest_reorg": blocks | None}},
         "deepest_reorg_24h": tip change | None,
         "last_reset": reset (see `resets`) | None,
         "sources": {"rpc": [{"source", "status", "last_ok_at", "last_error", "last_error_at",
                              "tip_hash", "tip_height", "behind"}],
                     "p2p": {"total", "by_status": {status: n}, "connected_by_group": {group: n}}}}
    Tip changes use `at` (wall clock), everything else header time; `tip change`
    dicts are {"source", "at", "old_hash", "old_height", "new_hash", "new_height",
    "fork_hash", "fork_height", "disconnected", "connected"}.
    """
    now = _now(now)
    best = chain.best_tip()
    canonical = chain.canonical_nodes() if best is not None else []
    top = best.height - chain.settle_depth if best is not None else None
    cutoffs = {name: now - seconds for name, seconds in PERIODS}
    periods = {
        name: {"seconds": seconds, "partial": not canonical or canonical[0].time > cutoffs[name], "blocks": 0,
               "unobserved": 0, "orphans": 0, "orphan_rate": None, "self_orphans": 0, "forks": 0,
               "deepest_fork": 0, "resets": 0, "reorgs": None, "reorged_sources": None, "deepest_reorg": None}
        for name, seconds in PERIODS
    }

    def each(when: int) -> Iterator[dict[str, Any]]:
        """Yield the period dicts whose window contains header time `when`."""
        return (periods[name] for name, cutoff in cutoffs.items() if when >= cutoff)

    for node in canonical:
        settled, observed = node.height <= top, _observed(node)
        for period in each(node.time):
            period["blocks"] += settled and observed
            period["unobserved"] += settled and not observed
            period["resets"] += node.is_min_diff
    for loser, winner in _orphans(chain):
        if not _observed(winner):
            continue
        for period in each(winner.time):
            period["orphans"] += 1
            period["self_orphans"] += _contest(loser.miner, winner.miner) == SELF
    for event in (chain.fork_events() if best is not None else ()):
        if not _observed(event.winner):
            continue
        for period in each(event.winner.time):
            period["forks"] += 1
            period["deepest_fork"] = max(period["deepest_fork"], event.depth)
    for period in periods.values():
        period["orphan_rate"] = _rate(period["orphans"], period["blocks"])
        period["partial"] = period["partial"] or period["unobserved"] > 0
    deepest = None
    if conn is not None:
        for name, cutoff in cutoffs.items():
            count, sources, depth = conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT source), MAX(disconnected) FROM tip_changes "
                "WHERE is_reorg = 1 AND at >= ?",
                (cutoff,),
            ).fetchone()
            periods[name].update(reorgs=count, reorged_sources=sources, deepest_reorg=depth)
        rows = _query(
            conn,
            f"SELECT {_TIP_CHANGE_COLUMNS} FROM tip_changes WHERE is_reorg = 1 AND at >= ? "
            "ORDER BY disconnected DESC, at DESC LIMIT 1",
            (cutoffs["24h"],),
        )
        deepest = _tip_change(rows[0]) if rows else None
    reset_list = chain.resets(since_height=best.height - 5_000) if best is not None else []
    return {
        "generated_at": now,
        "network": chain.params.name,
        "tip": _tip(best, now),
        "phase": _phase(chain.phase_at(best.height)) if best is not None else None,
        "window": {
            "from_height": canonical[0].height if canonical else None,
            "to_height": best.height if best is not None else None,
            "from_time": canonical[0].time if canonical else None,
            "blocks_in_memory": len(chain),
        },
        "periods": periods,
        "deepest_reorg_24h": deepest,
        "last_reset": _reset(reset_list[-1]) if reset_list else None,
        "sources": _source_health(conn, best) if conn is not None else {"rpc": [], "p2p": None},
    }


def fork_events(
    chain: Chain, conn: Any = None, limit: int = 50, since: float | None = None
) -> list[dict[str, Any]]:
    """Return fork events (canonical blocks with more than one child), newest first.

    `since` filters on the winner's header time; `limit` is clamped to 1..200.

    Each event:
        {"fork_hash", "fork_height", "fork_time", "height": first contested height,
         "winner": block + {"probes", "adopted_by"},
         "losers": [{"block": block, "tip_hash", "length", "work", "blocks", "miners": [label] (<= 50),
                     "classification": "self" | "race" | "unknown", "same_job", "seen_first",
                     "greater_raw_hash", "equal_work", "winner_len", "probes", "adopted_by"}] (<= 10),
         "loser_count", "depth": blocks, "depth_work", "classification", "same_job",
         "winner_first_seen", "winner_greater_raw_hash", "equal_work",
         "tiebreak": "work" | "both" | "hash" | "first_seen" | "neither" | "unknown",
         "settled", "phase": phase at the first contested height, "reorgs"}
    `tiebreak` names the rule that predicts the winner: "work" when the first
    blocks differ in work, else "hash" (greater raw hash: Zakura, Zebra 6.3),
    "first_seen" (Zebra 6.4), "both", or "neither" (decided by later blocks).
    DB fields (None without `conn`, so `tiebreak` is then "work", "hash" or "unknown"):
        seen_first, winner_first_seen = arrival order from `sightings`: a block was seen first only when
          the other block's first sighting is an `inv` or `rpc_tip` more than SEEN_MARGIN later (fetch and
          poll times such as backfill or getchaintips do not time arrival);
        probes ={"block", "notfound", "timeout", "error", "other", "notfound_same_announcer"} for that block;
        adopted_by = {"count", "by_group": {group: n}, "sources": [{"source", "group", "at", "via"}] (<= 20)},
          sources whose tip or best-chain sighting (via "tip" | "inv" | "headers" | "rpc_tip") was on the
          branch (the winner side covers the contested heights only);
        reorgs = {"count", "sources": [{"source", "group", "at", "disconnected", "connected",
                                        "old_hash", "new_hash"}] (<= 20)} of tip changes reorging at this fork.
    """
    limit = _clamp_int(limit, 1, MAX_FORK_LIMIT, "limit")
    since = _since(since)
    if chain.best_tip() is None:
        return []
    events = [e for e in chain.fork_events() if since is None or e.winner.time >= since]
    events = events[-limit:][::-1]
    pairs = [(event, _fork_dict(chain, event)) for event in events]
    if conn is not None:
        _enrich_forks(chain, conn, pairs)
    else:
        for _, out in pairs:
            out["winner"].update(probes=None, adopted_by=None)
            for loser in out["losers"]:
                loser.update(probes=None, adopted_by=None)
            out["reorgs"] = None
    return [out for _, out in pairs]


def orphan_stats(
    chain: Chain,
    conn: Any = None,
    *,
    now: float | None = None,
    exclude_heights: Iterable[tuple[int, int]] = (),
) -> dict[str, Any]:
    """Return orphan rates by sawtooth phase, blocks since reset, difficulty ratio, hour and day.

    Only settled heights watched live count (see the module doc); `exclude_heights`
    drops inclusive height ranges. `conn` is unused (every input is in the chain window).

    Returns:
        {"generated_at", "from_height", "to_height", "excluded": [[low, high], ...],
         "totals": {"blocks", "orphans", "rate", "forks", "self", "race", "unknown", "siblings",
                    "unobserved": settled canonical blocks left out (not watched live)},
         "by_phase": rows keyed "fast" | "slow" | "unknown",
         "by_k": rows keyed "0", "1-17", ..., ">=401", "unknown",
         "by_k_fine": rows keyed "0", "1", "2", "3-5", "6-10", "11-17", "18-30",
         "by_ratio": rows keyed "<0.001", ..., ">=1", "unknown",
         "by_hour": [{"start", "blocks", "orphans", "rate", "forks", "resets"}] (last 72 h, oldest first),
         "by_day": [{"day": "YYYY-MM-DD", "start", "blocks", "orphans", "rate", "forks", "resets"}]}
    where rows = [{"key", "blocks", "orphans", "rate", "share": of all orphans, "forks", "forks_per_1000"}].
    `siblings` counts orphans whose parent is canonical; `self`/`race`/`unknown`
    compare the orphan's miner with the canonical block's at the same height.
    A fork is counted at its first contested height.
    """
    now = _now(now)
    ranges = _height_ranges(exclude_heights)
    by_phase = _Tally(PHASE_LABELS)
    by_k = _Tally([label for *_, label in K_BUCKETS] + [UNKNOWN])
    by_k_fine = _Tally([label for *_, label in K_FINE_BUCKETS], fixed=True)
    by_ratio = _Tally([label for *_, label in RATIO_BUCKETS] + [UNKNOWN])
    last_hour = int(now // 3_600 * 3_600)
    by_hour = _Tally(range(last_hour - HOURLY_WINDOW, last_hour + 1, 3_600), fixed=True)
    by_day = _Tally()
    totals: Counter[str] = Counter()
    first_height: int | None = None

    def count(phase: Phase, when: int, slot: int) -> None:
        """Add one item to every tally."""
        by_phase.add(_phase_label(phase), slot)
        by_k.add(_k_bucket(phase.k), slot)
        by_k_fine.add(_k_fine(phase.k), slot)
        by_ratio.add(_ratio_bucket(phase.d_ratio), slot)
        by_hour.add(min(when // 3_600 * 3_600, last_hour), slot)
        by_day.add(when // 86_400 * 86_400, slot)

    best = chain.best_tip()
    top = best.height - chain.settle_depth if best is not None else -1
    if best is not None:
        for node in chain.canonical_nodes():
            if node.height > top:
                break
            if _excluded(node.height, ranges):
                continue
            if not _observed(node):
                totals["unobserved"] += 1
                continue
            if first_height is None:
                first_height = node.height
            count(chain.phase_at(node.height), node.time, _BLOCKS)
            if node.is_min_diff:
                by_hour.add(min(node.time // 3_600 * 3_600, last_hour), _RESETS)
                by_day.add(node.time // 86_400 * 86_400, _RESETS)
        for loser, winner in _orphans(chain):
            if _excluded(loser.height, ranges) or not _observed(winner):
                continue
            count(chain.phase_at(loser.height), winner.time, _ORPHANS)
            totals[_contest(loser.miner, winner.miner)] += 1
            totals["siblings"] += loser.prev_hash == chain.canonical_hash_at(loser.height - 1)
        for event in chain.fork_events():
            height = event.fork_height + 1
            if height <= top and not _excluded(height, ranges) and _observed(event.winner):
                count(chain.phase_at(height), event.winner.time, _FORKS)
    blocks, orphans = by_phase.total(_BLOCKS), by_phase.total(_ORPHANS)
    return {
        "generated_at": now,
        "from_height": first_height,
        "to_height": top if best is not None else None,
        "excluded": [list(pair) for pair in ranges],
        "totals": {
            "blocks": blocks,
            "orphans": orphans,
            "rate": _rate(orphans, blocks),
            "forks": by_phase.total(_FORKS),
            "self": totals[SELF],
            "race": totals[RACE],
            "unknown": totals[UNKNOWN],
            "siblings": totals["siblings"],
            "unobserved": totals["unobserved"],
        },
        "by_phase": by_phase.bucket_rows(orphans),
        "by_k": by_k.bucket_rows(orphans),
        "by_k_fine": by_k_fine.bucket_rows(orphans),
        "by_ratio": by_ratio.bucket_rows(orphans),
        "by_hour": by_hour.time_rows(),
        "by_day": by_day.time_rows(day=True),
    }


def miner_stats(
    chain: Chain,
    conn: Any = None,
    since: float | None = None,
    *,
    exclude_heights: Iterable[tuple[int, int]] = (),
    limit: int = MAX_MINERS,
) -> dict[str, Any]:
    """Return per-miner canonical and stale counts, self-orphans, races and resets.

    Only settled heights watched live count (see the module doc); `since` filters
    on the canonical header time at the height; `exclude_heights` drops inclusive
    height ranges. Miners beyond `limit` (by canonical + stale blocks) are merged
    into one "(other)" row. `conn` is unused.

    Returns:
        {"since", "from_height", "to_height", "blocks": canonical, "stale",
         "unobserved": settled canonical blocks left out (not watched live),
         "miners": [{"miner", "canonical", "share": of canonical blocks, "stale",
                     "stale_rate": stale / (canonical + stale), "self_orphans",
                     "races_lost", "races_won": stale blocks of other miners it beat,
                     "unattributed_losses": lost where either miner is unknown, "resets",
                     "templates": {"zakura" | "zebra" | "none" | "unknown": n},
                     "by_phase": {"fast" | "slow" | "unknown": {"canonical", "stale"}}}],
         "pairs": [{"loser", "winner", "kind": "self" | "race" | "unknown", "n"}] (top 20)}
    Template "none" is a parsed coinbase without a marker; "unknown" has no body.
    """
    since = _since(since)
    limit = _clamp_int(limit, 1, 10 * MAX_MINERS, "limit")
    ranges = _height_ranges(exclude_heights)
    stats: dict[str, dict[str, Any]] = defaultdict(_miner_row)
    pairs: Counter[tuple[str, str, str]] = Counter()
    best = chain.best_tip()
    top = best.height - chain.settle_depth if best is not None else -1
    first_height: int | None = None
    blocks = stale = unobserved = 0
    if best is not None:
        for node in chain.canonical_nodes():
            if node.height > top:
                break
            if _excluded(node.height, ranges) or (since is not None and node.time < since):
                continue
            if not _observed(node):
                unobserved += 1
                continue
            if first_height is None:
                first_height = node.height
            blocks += 1
            row = stats[_miner(node.miner)]
            row["canonical"] += 1
            row["resets"] += node.is_min_diff
            row["templates"][_template(node)] += 1
            row["by_phase"][_phase_label(chain.phase_at(node.height))]["canonical"] += 1
        for loser, winner in _orphans(chain):
            if _excluded(loser.height, ranges) or (since is not None and winner.time < since) or not _observed(winner):
                continue
            stale += 1
            loser_label, winner_label = _miner(loser.miner), _miner(winner.miner)
            row = stats[loser_label]
            row["stale"] += 1
            row["templates"][_template(loser)] += 1
            row["by_phase"][_phase_label(chain.phase_at(loser.height))]["stale"] += 1
            kind = _contest(loser.miner, winner.miner)
            pairs[(loser_label, winner_label, kind)] += 1
            if kind == SELF:
                row["self_orphans"] += 1
            elif kind == RACE:
                row["races_lost"] += 1
                stats[winner_label]["races_won"] += 1
            else:
                row["unattributed_losses"] += 1
    ranked = sorted(stats.items(), key=lambda item: (-(item[1]["canonical"] + item[1]["stale"]), item[0]))
    if len(ranked) > limit:
        other = _miner_row()
        for _, row in ranked[limit - 1 :]:
            _merge_miner_rows(other, row)
        ranked = ranked[: limit - 1] + [("(other)", other)]
    miners = []
    for label, row in ranked:
        total = row["canonical"] + row["stale"]
        miners.append(
            {"miner": label, **row, "share": _rate(row["canonical"], blocks), "stale_rate": _rate(row["stale"], total),
             "templates": dict(row["templates"]),
             "by_phase": {phase: dict(counts) for phase, counts in row["by_phase"].items()}}
        )
    return {
        "since": since,
        "from_height": first_height,
        "to_height": top if best is not None else None,
        "blocks": blocks,
        "stale": stale,
        "unobserved": unobserved,
        "miners": miners,
        "pairs": [
            {"loser": loser, "winner": winner, "kind": kind, "n": n}
            for (loser, winner, kind), n in sorted(pairs.items(), key=lambda item: (-item[1], item[0]))[:MAX_PAIRS]
        ],
    }


def sawtooth(chain: Chain, n: int = 1_500) -> dict[str, Any]:
    """Return the last `n` canonical blocks (clamped to 1..10000) for the sawtooth chart.

    Returns:
        {"from_height", "to_height", "target_spacing", "averaging_window": both in force at to_height,
         "blocks": [{"height", "time", "dt": time - previous canonical time | None, "difficulty",
                     "k", "fast", "min_diff", "orphans": attached non-canonical blocks at the height}],
         "resets": [reset, see `resets`] (oldest first, within the range),
         "tip_phase": phase | None}
    """
    n = _clamp_int(n, 1, MAX_SAWTOOTH, "n")
    best = chain.best_tip()
    out: dict[str, Any] = {"from_height": None, "to_height": None, "target_spacing": None, "averaging_window": None,
                           "blocks": [], "resets": [], "tip_phase": None}
    if best is None:
        return out
    rules = chain.params.difficulty_rules(best.height)
    nodes = chain.canonical_nodes(best.height - n + 1)
    previous = chain.get(chain.canonical_hash_at(nodes[0].height - 1) or "")
    previous_time = previous.time if previous is not None else None
    rows = []
    for node in nodes:
        phase = chain.phase_at(node.height)
        rivals = sum(1 for other in chain.nodes_at(node.height) if other is not node and other.cumwork is not None)
        rows.append(
            {"height": node.height, "time": node.time,
             "dt": node.time - previous_time if previous_time is not None else None,
             "difficulty": _sig(phase.difficulty), "k": phase.k, "fast": phase.fast, "min_diff": node.is_min_diff,
             "orphans": rivals}
        )
        previous_time = node.time
    out.update(
        from_height=nodes[0].height,
        to_height=best.height,
        target_spacing=rules.target_spacing,
        averaging_window=rules.averaging_window,
        blocks=rows,
        resets=[_reset(reset) for reset in chain.resets(since_height=nodes[0].height)],
        tip_phase=_phase(chain.phase_at(best.height)),
    )
    return out


def resets(chain: Chain, limit: int = 100, since: float | None = None) -> list[dict[str, Any]]:
    """Return canonical min-difficulty resets, newest first (`limit` clamped to 1..1000).

    `since` filters on the reset block's header time. Each reset:
        {"height", "hash", "time", "miner", "template", "gap": time - parent time,
         "next_dt": next canonical time - time, "forward_dating": time - first_seen_at,
         "d_pre", "fast_blocks": blocks until the fast phase ended (None while fast),
         "cycle_blocks": blocks until the next reset (None while open),
         "orphans": settled stale blocks at heights of the cycle watched live (None if none was),
         "unobserved": canonical blocks of the cycle not watched live}
    """
    limit = _clamp_int(limit, 1, MAX_RESET_LIMIT, "limit")
    since = _since(since)
    items = chain.resets()
    heights = sorted(loser.height for loser, winner in _orphans(chain) if _observed(winner))
    canonical = chain.canonical_nodes()
    blind = [node.height for node in canonical if not _observed(node)]
    top = canonical[-1].height if canonical else -1
    out = []
    for index in range(len(items) - 1, -1, -1):
        reset = items[index]
        if since is not None and reset.time < since:
            continue
        end = items[index + 1].height if index + 1 < len(items) else top + 1
        row = _reset(reset)
        unobserved = bisect_left(blind, end) - bisect_left(blind, reset.height)
        row["orphans"] = (
            bisect_left(heights, end) - bisect_left(heights, reset.height) if unobserved < end - reset.height else None
        )
        row["unobserved"] = unobserved
        out.append(row)
        if len(out) >= limit:
            break
    return out


def external_crosscheck(chain: Chain, conn: Any) -> dict[str, Any]:
    """Compare external (CipherScan) orphans with this monitor's stale blocks in the settled window.

    External records whose hash is canonical here are tip flip-flops, not orphans;
    unmatched ones at heights not watched live (see the module doc) are "unwatched",
    not "only_theirs", because this monitor could not have seen them.

    Returns:
        {"from_height", "to_height", "sources": [external source names],
         "totals": {"both", "only_theirs", "only_ours", "theirs_canonical", "out_of_window", "unwatched"},
         "by_source": {source: {"both", "only_theirs", "theirs_canonical", "unwatched"}},
         "by_day": [{"day", "start", "both", "only_theirs", "only_ours", "theirs_canonical", "unwatched"}],
         "only_theirs": [{"source", "hash", "height", "time", "miner_address"}] (newest, <= 20),
         "only_ours": [block] (newest, <= 20)}
    Days use the canonical header time at the orphan's height.
    """
    best = chain.best_tip()
    empty = {"both": 0, "only_theirs": 0, "only_ours": 0, "theirs_canonical": 0, "out_of_window": 0, "unwatched": 0}
    out: dict[str, Any] = {"from_height": None, "to_height": None, "sources": [], "totals": empty, "by_source": {},
                           "by_day": [], "only_theirs": [], "only_ours": []}
    if best is None or conn is None:
        return out
    canonical = chain.canonical_nodes()
    low, top = canonical[0].height, best.height - chain.settle_depth
    ours = {loser.hash: (loser, winner) for loser, winner in _orphans(chain)}
    totals: Counter[str] = Counter()
    by_source: dict[str, Counter[str]] = defaultdict(Counter)
    days: dict[int, Counter[str]] = defaultdict(Counter)
    theirs: set[str] = set()
    only_theirs = []
    rows = _query(
        conn,
        "SELECT source, hash, height, time, miner_address FROM external_orphans "
        "WHERE height IS NOT NULL ORDER BY height DESC LIMIT ?",
        (MAX_PROBE_ROWS,),
    )
    for row in rows:
        height, block_hash = row["height"], row["hash"]
        source = _text(row["source"])
        if not low <= height <= top:
            totals["out_of_window"] += 1
            continue
        winner_hash = chain.canonical_hash_at(height)
        winner = chain.get(winner_hash) if winner_hash else None
        day = winner.time // 86_400 * 86_400 if winner is not None else None
        if block_hash == winner_hash:
            kind = "theirs_canonical"
        elif block_hash in ours:
            kind = "both"
        elif winner is not None and not _observed(winner):
            kind = "unwatched"
        else:
            kind = "only_theirs"
            if len(only_theirs) < MAX_SAMPLE:
                only_theirs.append(
                    {"source": source, "hash": block_hash, "height": height, "time": row["time"],
                     "miner_address": _text(row["miner_address"])}
                )
        by_source[source][kind] += 1
        if block_hash in theirs:
            continue  # counted once across sources
        theirs.add(block_hash)
        totals[kind] += 1
        if day is not None:
            days[day][kind] += 1
    only_ours = sorted((pair for h, pair in ours.items() if h not in theirs), key=lambda p: -p[0].height)
    for _, winner in only_ours:
        totals["only_ours"] += 1
        days[winner.time // 86_400 * 86_400]["only_ours"] += 1
    keys = ("both", "only_theirs", "only_ours", "theirs_canonical", "unwatched")
    out.update(
        from_height=low,
        to_height=top,
        sources=sorted(by_source),
        totals={**empty, **totals},
        by_source={
            source: {key: counts[key] for key in ("both", "only_theirs", "theirs_canonical", "unwatched")}
            for source, counts in sorted(by_source.items())
        },
        by_day=[
            {"day": _day(start), "start": start, **{key: days[start][key] for key in keys}} for start in sorted(days)
        ],
        only_theirs=only_theirs,
        only_ours=[_block(loser) for loser, _ in only_ours[:MAX_SAMPLE]],
    )
    return out


# -- store-only: propagation and availability -----------------------------------------------


def propagation(conn: Any, since: float | None = None, *, now: float | None = None) -> dict[str, Any]:
    """Return how quickly blocks reached each implementation group (store only; any thread).

    Uses up to 1000 of the highest blocks near the persisted best height first
    seen at or after `since` (default: the last 6 h). A block's reference time is its earliest timed
    sighting (any kind but `store.UNTIMED_KINDS`); delays use `inv` and `rpc_tip` sightings. Groups are
    "impl major.minor" for P2P sources and the source name for RPC endpoints.

    Returns:
        {"since", "blocks": blocks with a timely sighting, "sightings",
         "by_group": [{"group", "kind", "sources", "sightings", "first": blocks it saw first,
                       "p50_s", "p90_s", "max_s",
                       "coverage": mean share of the blocks each member announced while it was reporting}],
         "recent": [{"hash", "height", "first_seen_at", "first_source", "sources", "p50_s", "p90_s", "max_s"}]
                   (newest first, <= 50)}
    """
    now = _now(now)
    since = _since(since)
    if since is None:
        since = now - DEFAULT_PROPAGATION_WINDOW
    empty: dict[str, Any] = {"since": since, "blocks": 0, "sightings": 0, "by_group": [], "recent": []}
    # The persisted best height, not MAX(height): one detached row with a forged BIP34 height would
    # otherwise move the window past every real block.
    row = conn.execute("SELECT value FROM meta WHERE key = 'best_height'").fetchone()
    if row is not None and str(row[0]).isdigit():
        top = int(row[0])
    else:
        top = conn.execute("SELECT MAX(height) FROM blocks").fetchone()[0]
    if top is None:
        return empty
    blocks = _query(
        conn,
        "SELECT hash, height, first_seen_at, first_seen_source FROM blocks "
        "WHERE height BETWEEN ? AND ? AND first_seen_at >= ? ORDER BY height DESC LIMIT ?",
        (top - PROPAGATION_HEIGHT_SPAN, top + PROPAGATION_HEIGHT_SPAN, since, MAX_PROPAGATION_BLOCKS),
    )
    groups = _source_groups(conn)
    first: dict[str, float] = {row["hash"]: row["first_seen_at"] for row in blocks}
    timely: dict[str, list[tuple[str, float]]] = defaultdict(list)
    sql = "SELECT hash, source, kind, at FROM sightings WHERE hash IN ({marks})"
    for block_hash, source, kind, at in _in_query(conn, sql, list(first)):
        if at < first[block_hash] and kind not in UNTIMED_KINDS:
            first[block_hash] = at
        if kind in TIMELY_KINDS:
            timely[block_hash].append((source, at))
    delays: dict[str, list[float]] = defaultdict(list)
    members: dict[str, set[str]] = defaultdict(set)
    firsts: Counter[str] = Counter()
    per_source: dict[str, list[float]] = defaultdict(list)  # first-seen reference times of blocks it announced
    recent = []
    for row in blocks:
        block_hash = row["hash"]
        seen = timely.get(block_hash)
        if not seen:
            continue
        reference = first[block_hash]
        seen.sort(key=lambda item: item[1])
        firsts[_sighting_group(seen[0][0], groups)] += 1
        spread = []
        for source, at in seen:
            group = _sighting_group(source, groups)
            delay = at - reference
            delays[group].append(delay)
            members[group].add(source)
            per_source[source].append(reference)
            spread.append(delay)
        if len(recent) < MAX_RECENT:
            recent.append(
                {"hash": block_hash, "height": row["height"], "first_seen_at": reference,
                 "first_source": _text(seen[0][0]), "sources": len({s for s, _ in seen}),
                 "p50_s": _sig(_percentile(spread, 50)), "p90_s": _sig(_percentile(spread, 90)),
                 "max_s": _sig(spread[-1])}
            )
    references = sorted(first[h] for h in timely)
    coverage: dict[str, list[float]] = defaultdict(list)
    for source, times in per_source.items():
        eligible = bisect_right(references, max(times)) - bisect_left(references, min(times))
        coverage[_sighting_group(source, groups)].append(len(times) / eligible if eligible else 1.0)
    by_group = []
    for group, values in delays.items():
        values.sort()
        by_group.append(
            {"group": group, "kind": "rpc" if group.startswith("rpc:") else "p2p", "sources": len(members[group]),
             "sightings": len(values), "first": firsts[group], "p50_s": _sig(_percentile(values, 50)),
             "p90_s": _sig(_percentile(values, 90)), "max_s": _sig(values[-1]),
             "coverage": _sig(sum(coverage[group]) / len(coverage[group]))}
        )
    by_group.sort(key=lambda row: (-row["sightings"], row["group"]))
    return {"since": since, "blocks": len(timely), "sightings": sum(len(v) for v in timely.values()),
            "by_group": by_group, "recent": recent}


def probe_stats(
    conn: Any, since: float | None = None, *, now: float | None = None, chain: Chain | None = None
) -> dict[str, Any]:
    """Return getdata probe outcomes by implementation and the "announced then notfound" incidents.

    `since` defaults to the last 24 h. With `chain` (loop thread only), each
    incident says whether its block is canonical. `by_group` covers the
    "announce" probes (the first announcer of a block unknown here, every
    implementation), so groups compare like with like; `reprobe_by_group`
    covers the sampled "reprobe" probes (Zebra announcers of blocks already
    held, mostly canonical). Other reasons, such as "fetch", only count in
    `by_reason` and `probes`.

    Returns:
        {"since", "probes",
         "by_group": [{"group", "impl", "version", "probes", "block", "notfound", "timeout", "error",
                       "other", "block_rate", "notfound_rate", "timeout_rate", "latency_p50_ms",
                       "latency_p90_ms"}] (most probes first),
         "reprobe_by_group": [same rows for "reprobe" probes],
         "by_reason": [{"reason", "probes", "block", "notfound", "timeout", "error", "other"}],
         "incident_count",
         "incidents": [{"at", "source", "group", "hash", "height", "canonical": bool | None, "reason",
                        "peer_tip_hash"}] (newest first, <= 50)}
    """
    now = _now(now)
    since = _since(since)
    if since is None:
        since = now - DEFAULT_PROBE_WINDOW
    rows = _query(
        conn,
        "SELECT p.at, p.source, p.impl, p.hash, p.reason, p.result, p.latency_ms, p.peer_tip_hash, "
        "p.announced_by_same_peer, s.impl_version FROM probes p LEFT JOIN sources s ON s.source = p.source "
        "WHERE p.at >= ? ORDER BY p.at DESC LIMIT ?",
        (since, MAX_PROBE_ROWS),
    )
    by_group: dict[tuple[str, str], dict[str, Any]] = {}
    by_reason: dict[str, Counter[str]] = defaultdict(Counter)
    latencies: dict[tuple[str, str], list[int]] = defaultdict(list)
    incidents = []
    incident_count = 0
    for row in rows:
        impl = _text(row["impl"], 32) or UNKNOWN
        version = _version(row["impl_version"])
        group = _group_key(impl, row["impl_version"])
        result = row["result"] if row["result"] in PROBE_RESULTS else "other"
        reason = _text(row["reason"], 32) or UNKNOWN
        by_reason[reason]["probes"] += 1
        by_reason[reason][result] += 1
        if reason in GROUP_REASONS:
            entry = by_group.setdefault(
                (reason, group), {"group": group, "impl": impl, "version": version, "probes": 0,
                                  **{key: 0 for key in (*PROBE_RESULTS, "other")}}
            )
            entry["probes"] += 1
            entry[result] += 1
            if result == "block" and isinstance(row["latency_ms"], int):
                latencies[(reason, group)].append(row["latency_ms"])
        if result == "notfound" and row["announced_by_same_peer"]:
            incident_count += 1
            if len(incidents) < MAX_INCIDENTS:
                incidents.append(
                    {"at": row["at"], "source": _text(row["source"]), "group": group, "hash": row["hash"],
                     "height": None, "canonical": None, "reason": reason, "peer_tip_hash": row["peer_tip_hash"]}
                )
    if incidents:
        heights = dict(_in_query(conn, "SELECT hash, height FROM blocks WHERE hash IN ({marks})",
                                 list({i["hash"] for i in incidents})))
        for incident in incidents:
            node = chain.get(incident["hash"]) if chain is not None else None
            incident["height"] = node.height if node is not None else heights.get(incident["hash"])
            if node is not None:
                incident["canonical"] = chain.canonical_hash_at(node.height) == node.hash
    groups_out: dict[str, list[dict[str, Any]]] = {reason: [] for reason in GROUP_REASONS}
    for key, entry in by_group.items():
        values = sorted(latencies[key])
        groups_out[key[0]].append(
            {**entry, "block_rate": _rate(entry["block"], entry["probes"]),
             "notfound_rate": _rate(entry["notfound"], entry["probes"]),
             "timeout_rate": _rate(entry["timeout"], entry["probes"]),
             "latency_p50_ms": _percentile(values, 50), "latency_p90_ms": _percentile(values, 90)}
        )
    for rows_out in groups_out.values():
        rows_out.sort(key=lambda row: (-row["probes"], row["group"]))
    return {
        "since": since,
        "probes": len(rows),
        "by_group": groups_out[ANNOUNCE_REASON],
        "reprobe_by_group": groups_out[REPROBE_REASON],
        "by_reason": [
            {"reason": reason, "probes": counts["probes"], **{key: counts[key] for key in (*PROBE_RESULTS, "other")}}
            for reason, counts in sorted(by_reason.items())
        ],
        "incident_count": incident_count,
        "incidents": incidents,
    }


# -- text report --------------------------------------------------------------------------------


def text_report(chain: Chain, conn: Any = None, *, now: float | None = None, forks: int = 10) -> str:
    """Render a plain-text summary (tip, periods, orphan phases, miners, forks, resets, sources)."""
    now = _now(now)
    head = summary(chain, conn, now)
    lines = [f"Zakura fork monitor report ({head['network']}) at {_iso(now)} UTC"]
    tip = head["tip"]
    if tip is None:
        lines.append("No blocks loaded.")
        return "\n".join(lines) + "\n"
    phase = head["phase"]
    lines.append(
        f"Tip {tip['height']} {tip['hash']} miner {_clean(tip['miner'])} "
        f"phase {phase['label']} k={_fmt(phase['k'])} D/Dpre={_fmt(phase['d_ratio'])}"
    )
    window = head["window"]
    lines.append(
        f"Window {window['from_height']}..{window['to_height']} ({window['blocks_in_memory']} blocks in memory)"
    )
    lines += _table(
        ("period", "blocks", "orphans", "rate", "self", "forks", "deepest", "resets", "reorgs", "deepest reorg"),
        [
            (name + ("*" if p["partial"] else ""), p["blocks"], p["orphans"], _pct(p["orphan_rate"]), p["self_orphans"],
             p["forks"], p["deepest_fork"], p["resets"], _fmt(p["reorgs"]), _fmt(p["deepest_reorg"]))
            for name, p in head["periods"].items()
        ],
        "Activity (* = partial: window shorter than the period, or blocks not watched live left out)",
    )
    stats = orphan_stats(chain, conn, now=now)
    for title, key in (("Orphan rate by phase", "by_phase"), ("Orphan rate by blocks since reset", "by_k"),
                       ("Orphan rate by D/D_pre", "by_ratio")):
        lines += _table(
            ("bucket", "blocks", "orphans", "rate", "share", "forks/1000"),
            [(r["key"], r["blocks"], r["orphans"], _pct(r["rate"]), _pct(r["share"]), _fmt(r["forks_per_1000"]))
             for r in stats[key]],
            title,
        )
    miners = miner_stats(chain, conn, limit=10)
    lines += _table(
        ("miner", "canonical", "share", "stale", "stale rate", "self", "lost", "won", "resets"),
        [(_clean(m["miner"])[:40], m["canonical"], _pct(m["share"]), m["stale"], _pct(m["stale_rate"]),
          m["self_orphans"], m["races_lost"], m["races_won"], m["resets"]) for m in miners["miners"]],
        "Miners",
    )
    lines += _table(
        ("height", "depth", "class", "tiebreak", "winner", "loser", "phase", "k"),
        [(e["height"], e["depth"], e["classification"], e["tiebreak"], _clean(e["winner"]["miner"])[:24],
          _clean(e["losers"][0]["block"]["miner"])[:24] if e["losers"] else "-", e["phase"]["label"],
          _fmt(e["phase"]["k"])) for e in fork_events(chain, conn, limit=forks)],
        "Recent fork events",
    )
    lines += _table(
        ("height", "time UTC", "miner", "template", "gap", "next dt", "fast blocks", "cycle", "orphans"),
        [(r["height"], _iso(r["time"]), _clean(r["miner"])[:24], _fmt(r["template"]), _fmt(r["gap"]),
          _fmt(r["next_dt"]), _fmt(r["fast_blocks"]), _fmt(r["cycle_blocks"]), _fmt(r["orphans"]))
         for r in resets(chain, limit=10)],
        "Recent resets",
    )
    sources = head["sources"]
    if sources["rpc"]:
        lines += _table(
            ("source", "status", "tip height", "behind", "last error"),
            [(_clean(s["source"]), _fmt(s["status"]), _fmt(s["tip_height"]), _fmt(s["behind"]),
              _clean(s["last_error"])[:60]) for s in sources["rpc"]],
            "RPC sources",
        )
    if sources["p2p"]:
        p2p = sources["p2p"]
        by_group = ", ".join(f"{_clean(k)} {v}" for k, v in sorted(p2p["connected_by_group"].items())) or "-"
        lines += ["", f"P2P sources: {p2p['total']} known; connected by group: {by_group}"]
    return "\n".join(lines) + "\n"


# -- views and grouping helpers -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Context:
    """Configuration facts that label sources: RPC kinds and fleet membership."""

    rpc_kind: Mapping[str, str] = dataclasses.field(default_factory=dict)
    fleet_sources: frozenset[str] = frozenset()
    fleet_hosts: frozenset[str] = frozenset()


def _views(monitor: Any, conn: Any, now: float, live: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build peer views from the sources table, configured RPC endpoints and live P2P peers."""
    chain: Chain = monitor.chain
    rows = _query(conn, "SELECT * FROM sources ORDER BY source LIMIT ?", (MAX_SOURCES,)) if conn is not None else []
    config = getattr(monitor, "config", None)
    endpoints = tuple(getattr(config, "rpc", ()) or ())
    context = _Context(
        rpc_kind={e.source: e.kind for e in endpoints},
        fleet_sources=frozenset(e.source for e in endpoints if e.fleet),
        fleet_hosts=frozenset(getattr(config, "fleet_hosts", ()) or ()),
    )
    known = {row["source"] for row in rows}
    rows += [{"source": e.source, "kind": "rpc", "status": "pending"} for e in endpoints if e.source not in known]
    for source, entry in itertools.islice(live.items(), MAX_SOURCES):
        if source not in known:
            rows.append(_live_row(source, entry))
    best = chain.best_tip()
    views = [_view(chain, row, now, best, context, live.get(row["source"])) for row in rows[: 2 * MAX_SOURCES]]
    views.sort(key=lambda v: (v["kind"] != "rpc", v["group"] if v["kind"] != "rpc" else "", v["source"]))
    return views


def _view(
    chain: Chain,
    row: Mapping[str, Any],
    now: float,
    best: Any,
    context: _Context,
    live: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Describe one source: identity, tip, relation to the best tip, activity and state."""
    source = _text(row.get("source")) or "?"
    kind = _text(row.get("kind"), 16) or source.partition(":")[0]
    impl = (_text(row.get("impl"), 32) or context.rpc_kind.get(source) or UNKNOWN).lower()
    version_text = _text(row.get("impl_version"), 32)
    tip_hash = _hash_or_none(row.get("tip_hash"))
    tip_height = _int_or_none(row.get("tip_height"))
    if live and tip_hash is None:
        tip_hash = _hash_or_none(live.get("tip_hash") or live.get("tip"))
        tip_height = _int_or_none(live.get("tip_height"))
    node = chain.get(tip_hash) if tip_hash is not None else None
    if node is not None:
        relation = _relation(chain.relation(node.hash))
        tip_height = node.height
        # A dead-fork tip met on first contact is first seen now; its header time still shows its age,
        # and the first sighting still bounds a forward-dated header.
        seen: float | None = min(float(node.time), node.first_seen_at if node.first_seen_at is not None else math.inf)
    else:
        relation = {"kind": UNKNOWN, "n": None, "fork_hash": None, "fork_height": None, "depth_ours": None,
                    "depth_theirs": None, "tip_height": tip_height}
        seen = _float_or_none(row.get("tip_at"))
    age = now - seen if seen is not None else None
    behind = best.height - tip_height if best is not None and tip_height is not None else None
    start_height = _int_or_none(row.get("start_height"))
    if behind is None and tip_hash is None and best is not None and start_height is not None:
        # No tip learned (e.g. no common block in our window): its version height is the best estimate.
        behind = best.height - start_height
    status = _text(row.get("status"), 32)
    last_ok, tip_at = _float_or_none(row.get("last_ok_at")), _float_or_none(row.get("tip_at"))
    active = (
        status in ACTIVE_STATUSES
        or _live_connected(live)
        or any(t is not None and now - t <= ACTIVE_WINDOW for t in (last_ok, tip_at))
    )
    state = _state(relation["kind"], relation["n"], behind, age) if tip_hash or behind is not None else UNKNOWN
    stuck = active and state == "stuck"
    if not active:
        state = "inactive"
    host = source[4:].rpartition(":")[0].strip("[]").lower() if kind == "p2p" else ""
    return {
        "source": source,
        "kind": kind,
        "fleet": source in context.fleet_sources or (bool(host) and host in context.fleet_hosts),
        "impl": impl,
        "version": version_text,
        "group": _group_key(impl, version_text),
        "user_agent": _text(row.get("user_agent")),
        "status": status,
        "active": active,
        "state": state,
        "stuck": stuck,
        "tip_hash": tip_hash,
        "tip_height": tip_height,
        "tip_at": tip_at,
        "tip_age_s": _sig(age),
        "behind": behind,
        "relation": relation,
        "start_height": _int_or_none(row.get("start_height")),
        "first_seen_at": _float_or_none(row.get("first_seen_at")),
        "last_ok_at": last_ok,
        "last_error": _text(row.get("last_error")),
        "last_error_at": _float_or_none(row.get("last_error_at")),
        "live": _plain(live) if live is not None else None,
    }


def _state(kind: str, n: int | None, behind: int | None, age: float | None) -> str:
    """Classify a tip relation as synced, lagging, fork, stuck or unknown (see STUCK_AGE and STUCK_BEHIND)."""
    if kind in ("same", "ahead"):
        return "synced"
    if kind == "behind" and n is not None:
        return "stuck" if n > STUCK_BEHIND else "lagging" if n > SYNCED_LAG else "synced"
    far_behind = behind is not None and behind > STUCK_BEHIND
    if kind == "fork":
        return "stuck" if far_behind or (age is not None and age > STUCK_AGE) else "fork"
    return "stuck" if far_behind else UNKNOWN


def _group_views(chain: Chain, views: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate peer views into implementation groups with branch clusters (see `group_sources`)."""
    groups: dict[str, dict[str, Any]] = {}
    clusters: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    side_keys: dict[str, str | None] = {}
    for view in views:
        key = view["group"]
        group = groups.get(key)
        if group is None:
            match = _VERSION_RE.match(view["version"] or "")
            group = groups[key] = {
                "key": key, "impl": view["impl"], "version": f"{match[1]}.{match[2]}" if match else None,
                "members": 0, "active": 0, "stuck": 0, "fleet": 0, "states": dict.fromkeys(STATES, 0),
                "branch": None, "branches": [], "stuck_sources": [],
            }
        group["members"] += 1
        group["active"] += view["active"]
        group["fleet"] += view["fleet"]
        group["states"][view["state"]] = group["states"].get(view["state"], 0) + 1
        if view["stuck"]:
            group["stuck"] += 1
            if len(group["stuck_sources"]) < MAX_SAMPLE:
                group["stuck_sources"].append(view["source"])
        relation = view["relation"]
        if view["state"] not in ("synced", "lagging", "fork") or view["impl"] in NON_NODE_IMPLS:
            continue
        if relation["kind"] == "fork":
            tip = view["tip_hash"]
            if tip not in side_keys:
                side_keys[tip] = _side_key(chain, tip, relation["fork_height"])
            branch_key = side_keys[tip]
            if branch_key is None:
                continue
        else:
            branch_key = CANONICAL
        cluster = clusters[key].setdefault(
            branch_key,
            {"key": branch_key, "tip_hash": None, "tip_height": None,
             "fork_hash": relation["fork_hash"] if branch_key != CANONICAL else None,
             "fork_height": relation["fork_height"] if branch_key != CANONICAL else None,
             "relation": None, "members": 0, "sources": []},
        )
        cluster["members"] += 1
        if len(cluster["sources"]) < MAX_SAMPLE:
            cluster["sources"].append(view["source"])
        height = view["tip_height"]
        if cluster["tip_height"] is None or (height is not None and height > cluster["tip_height"]):
            cluster.update(tip_hash=view["tip_hash"], tip_height=height, relation=relation)
    for key, group in groups.items():
        branches = sorted(
            clusters[key].values(),
            key=lambda c: (-c["members"], c["key"] != CANONICAL, -(c["tip_height"] or 0), c["key"]),
        )
        group["branches"] = branches
        group["branch"] = branches[0] if branches else None
    return sorted(groups.values(), key=lambda g: (-g["active"], g["key"]))


def _side_key(chain: Chain, tip_hash: str, fork_height: int | None) -> str | None:
    """Return the hash of the first block after the canonical fork point on `tip_hash`'s branch."""
    node = chain.get(tip_hash)
    if node is None or fork_height is None:
        return None
    for _ in range(node.height - fork_height - 1):
        parent = chain.get(node.prev_hash)
        if parent is None or parent.height != node.height - 1:
            return None
        node = parent
    return node.hash


def _conflict(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    """Return whether two branch sides are on different branches, both past their fork point."""
    if a["branch"] == b["branch"]:
        return False
    if CANONICAL in (a["branch"], b["branch"]):
        canonical, side = (a, b) if a["branch"] == CANONICAL else (b, a)
        return (
            canonical["tip_height"] is not None
            and side["fork_height"] is not None
            and canonical["tip_height"] > side["fork_height"]
        )
    return True


def _p2p_live(p2p: Any) -> tuple[dict[str, Mapping[str, Any]], str | None]:
    """Return `p2p.snapshot()` as {source: peer dict}, tolerating a missing observer or odd shapes."""
    snapshot = getattr(p2p, "snapshot", None)
    if not callable(snapshot):
        return {}, None
    try:
        raw = snapshot()
    except Exception as err:  # a collector bug must not take the dashboard down
        return {}, _text(f"{type(err).__name__}: {err}")
    if isinstance(raw, Mapping) and isinstance(raw.get("peers"), (Mapping, list, tuple)):
        raw = raw["peers"]
    if isinstance(raw, Mapping):
        items: Iterable[tuple[Any, Any]] = raw.items()
    elif isinstance(raw, (list, tuple)):
        items = ((_live_key(entry), entry) for entry in raw if isinstance(entry, Mapping))
    else:
        return {}, "p2p snapshot has an unexpected shape"
    out: dict[str, Mapping[str, Any]] = {}
    for key, entry in itertools.islice(items, MAX_SOURCES):
        if key is None or not isinstance(entry, Mapping):
            continue
        key = str(key)[:MAX_TEXT]
        out[key if key.startswith("p2p:") else f"p2p:{key}"] = entry
    return out, None


def _live_key(entry: Mapping[str, Any]) -> str | None:
    """Return the source key of one live peer entry (source, addr or ip + port)."""
    for name in ("source", "addr", "peer"):
        if isinstance(entry.get(name), str):
            return entry[name]
    ip, port = entry.get("ip"), entry.get("port")
    return f"{ip}:{port}" if ip is not None and port is not None else None


def _live_row(source: str, entry: Mapping[str, Any]) -> dict[str, Any]:
    """Synthesize a sources row for a live peer that is not stored yet."""
    return {
        "source": source,
        "kind": "p2p",
        "impl": entry.get("impl"),
        "impl_version": entry.get("impl_version") or entry.get("version"),
        "user_agent": entry.get("user_agent"),
        "tip_hash": entry.get("tip_hash") or entry.get("tip"),
        "tip_height": entry.get("tip_height"),
        "status": "connected" if entry.get("connected") else entry.get("status"),
    }


def _live_connected(live: Mapping[str, Any] | None) -> bool:
    """Return whether a live peer entry says it is connected."""
    return bool(live) and live.get("connected") is True


def _rpc_status(monitor: Any, views: Sequence[Mapping[str, Any]]) -> tuple[list[Any], str | None]:
    """Return `monitor.rpc_status()` (sanitized) or, without it, the RPC rows of `views`."""
    status = getattr(monitor, "rpc_status", None)
    if callable(status):
        try:
            result = _plain(status())
        except Exception as err:  # a collector bug must not take the dashboard down
            return [], _text(f"{type(err).__name__}: {err}")
        if isinstance(result, dict):
            result = [{"source": key, **value} if isinstance(value, dict) else {"source": key, "status": value}
                      for key, value in result.items()]
        return result if isinstance(result, list) else [result], None
    keys = ("source", "status", "state", "tip_height", "behind", "last_ok_at", "last_error", "last_error_at")
    return [{key: view[key] for key in keys} for view in views if view["kind"] == "rpc"], None


def _source_health(conn: Any, best: Any) -> dict[str, Any]:
    """Summarize the sources table: RPC endpoints individually, P2P peers by status and group."""
    rows = _query(conn, "SELECT * FROM sources ORDER BY source LIMIT ?", (MAX_SOURCES,))
    rpc = []
    p2p_status: Counter[str] = Counter()
    connected: Counter[str] = Counter()
    p2p_total = 0
    for row in rows:
        tip_height = _int_or_none(row["tip_height"])
        if row["kind"] == "rpc":
            rpc.append(
                {"source": _text(row["source"]), "status": _text(row["status"], 32),
                 "last_ok_at": row["last_ok_at"], "last_error": _text(row["last_error"]),
                 "last_error_at": row["last_error_at"], "tip_hash": row["tip_hash"], "tip_height": tip_height,
                 "behind": best.height - tip_height if best is not None and tip_height is not None else None}
            )
        else:
            p2p_total += 1
            status = _text(row["status"], 32) or UNKNOWN
            p2p_status[status] += 1
            if status == "connected":
                connected[_group_key(row["impl"], row["impl_version"])] += 1
    return {"rpc": rpc, "p2p": {"total": p2p_total, "by_status": dict(p2p_status),
                                "connected_by_group": dict(connected)}}


def _monitor_conn(monitor: Any) -> Any:
    """Return this thread's read connection of `monitor.store`, or None without a store."""
    store = getattr(monitor, "store", None)
    reader = getattr(store, "reader", None)
    return reader() if callable(reader) else None


def _recent_reorgs(conn: Any, limit: int) -> list[dict[str, Any]]:
    """Return the newest reorging tip changes."""
    rows = _query(
        conn,
        f"SELECT {_TIP_CHANGE_COLUMNS} FROM tip_changes WHERE is_reorg = 1 ORDER BY at DESC LIMIT ?",
        (limit,),
    )
    return [_tip_change(row) for row in rows]


_TIP_CHANGE_COLUMNS = (
    "source, at, old_hash, old_height, new_hash, new_height, fork_hash, fork_height, disconnected, connected"
)


def _tip_change(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return a tip_changes row as a tip change dict."""
    return {key: (_text(row[key]) if key == "source" else row[key]) for key in _TIP_CHANGE_COLUMNS.split(", ")}


# -- fork helpers -----------------------------------------------------------------------------


def _fork_dict(chain: Chain, event: ForkEvent) -> dict[str, Any]:
    """Return the chain-only fields of a fork event dict."""
    return {
        "fork_hash": event.fork_hash,
        "fork_height": event.fork_height,
        "fork_time": event.fork_time,
        "height": event.fork_height + 1,
        "winner": _block(event.winner),
        "losers": [_loser(branch) for branch in event.losers[:MAX_LOSERS]],
        "loser_count": len(event.losers),
        "depth": event.depth,
        "depth_work": event.depth_work,
        "classification": event.classification,
        "same_job": event.same_job,
        "winner_first_seen": None,  # a DB field, see `_enrich_forks`
        "winner_greater_raw_hash": event.winner_greater_raw_hash,
        "equal_work": event.equal_work,
        "tiebreak": _tiebreak(event.equal_work, event.winner_greater_raw_hash, None),
        "settled": event.settled,
        "phase": _phase(chain.phase_at(event.fork_height + 1)),
    }


def _loser(branch: LoserBranch) -> dict[str, Any]:
    """Return a losing branch as a dict."""
    return {
        "block": _block(branch.block),
        "tip_hash": branch.tip_hash,
        "length": branch.length,
        "work": branch.work,
        "blocks": branch.blocks,
        "miners": [_text(miner) for miner in branch.miners[:MAX_BRANCH_MINERS]],
        "classification": branch.classification,
        "same_job": branch.same_job,
        "seen_first": None,  # a DB field, see `_enrich_forks`
        "greater_raw_hash": branch.greater_raw_hash,
        "equal_work": branch.equal_work,
        "winner_len": branch.winner_len,
    }


def _tiebreak(equal_work: bool, greater_hash: bool | None, first_seen: bool | None) -> str:
    """Name the tie-break rule that predicts the winner of an equal-work race."""
    if not equal_work:
        return "work"
    by_hash, by_seen = greater_hash is True, first_seen is True
    if by_hash and by_seen:
        return "both"
    if by_hash:
        return "hash"
    if by_seen:
        return "first_seen"
    if greater_hash is False and first_seen is False:
        return "neither"
    return UNKNOWN


def _seen_bounds(conn: Any, refs: Mapping[str, Any]) -> dict[str, tuple[float | None, float | None]]:
    """Return {hash: (first seen, earliest arrival or None)} for BlockRefs keyed by hash.

    The earliest arrival is known only when the first sighting is timely (`inv`
    or `rpc_tip`); a fetch or poll can come any time after the block arrived.
    """
    earliest: dict[str, tuple[float, str]] = {}
    sql = "SELECT hash, kind, at FROM sightings WHERE hash IN ({marks})"
    for block_hash, kind, at in _in_query(conn, sql, list(refs)):
        known = earliest.get(block_hash)
        if known is None or at < known[0] or (at == known[0] and kind in TIMELY_KINDS):
            earliest[block_hash] = (at, kind)
    bounds = {}
    for block_hash, ref in refs.items():
        at, kind = earliest.get(block_hash, (None, None))
        first = ref.first_seen_at
        if at is not None and (first is None or at <= first):
            first = at
            lower = at - SEEN_MARGIN if kind in TIMELY_KINDS else None
        else:
            lower = None  # the chain's first sighting is not in the table (pruned): its kind is unknown
        bounds[block_hash] = (first, lower)
    return bounds


def _seen_before(a: tuple[float | None, float | None], b: tuple[float | None, float | None]) -> bool | None:
    """Return True if block `a` was seen before `b` could have arrived, False for the reverse, else None."""
    if a[0] is not None and b[1] is not None and a[0] < b[1]:
        return True
    if b[0] is not None and a[1] is not None and b[0] < a[1]:
        return False
    return None


class _Adoption:
    """Earliest adoption of one branch per source."""

    def __init__(self) -> None:
        """Start empty."""
        self.first: dict[str, tuple[float, str]] = {}

    def add(self, source: str, at: float, via: str) -> None:
        """Record that `source` had the branch at `at` (keeps the earliest)."""
        if source not in self.first or at < self.first[source][0]:
            self.first[source] = (at, via)

    def result(self, groups: Mapping[str, str]) -> dict[str, Any]:
        """Return the adoption dict documented in `fork_events`."""
        ordered = sorted(self.first.items(), key=lambda item: (item[1][0], item[0]))
        return {
            "count": len(ordered),
            "by_group": dict(Counter(_sighting_group(source, groups) for source, _ in ordered)),
            "sources": [
                {"source": _text(source), "group": _sighting_group(source, groups), "at": at, "via": via}
                for source, (at, via) in ordered[:MAX_SAMPLE]
            ],
        }


def _enrich_forks(chain: Chain, conn: Any, pairs: list[tuple[ForkEvent, dict[str, Any]]]) -> None:
    """Add arrival order, probe results, adopting sources and per-source reorgs to fork event dicts."""
    refs = {ref.hash: ref for event, _ in pairs for ref in (event.winner, *(b.block for b in event.losers))}
    bounds = _seen_bounds(conn, refs)
    for event, out in pairs:
        winner_bounds = bounds[event.winner.hash]
        seen = [_seen_before(bounds[branch.block.hash], winner_bounds) for branch in event.losers]
        for loser_out, value in zip(out["losers"], seen, strict=False):  # out lists at most MAX_LOSERS
            loser_out["seen_first"] = value
        out["winner_first_seen"] = None if None in seen else not any(seen)
        out["tiebreak"] = _tiebreak(event.equal_work, event.winner_greater_raw_hash, out["winner_first_seen"])

    groups = _source_groups(conn)
    members: dict[str, list[_Adoption]] = defaultdict(list)  # block hash -> branches it belongs to
    shown: dict[str, list[dict[str, Any]]] = defaultdict(list)  # block hash -> dicts that show its probes
    finish: list[tuple[dict[str, Any], _Adoption]] = []
    reorgs: dict[str, dict[str, Any]] = {}  # fork hash -> that event's "reorgs" dict
    for event, out in pairs:
        winner = _Adoption()
        finish.append((out["winner"], winner))
        shown[event.winner.hash].append(out["winner"])
        for height in range(event.fork_height + 1, event.fork_height + 1 + min(event.depth, MAX_BRANCH_HASHES)):
            block_hash = chain.canonical_hash_at(height)
            if block_hash is not None:
                members[block_hash].append(winner)
        for branch, loser_out in zip(event.losers, out["losers"], strict=False):  # out lists at most MAX_LOSERS
            adoption = _Adoption()
            finish.append((loser_out, adoption))
            shown[branch.block.hash].append(loser_out)
            try:
                nodes = chain.path(event.fork_hash, branch.tip_hash)[:MAX_BRANCH_HASHES]
            except (KeyError, ValueError):
                nodes = []
            for node in nodes:
                members[node.hash].append(adoption)
        out["reorgs"] = reorgs[event.fork_hash] = {"count": 0, "sources": []}

    hashes = list(members)
    sql = "SELECT source, new_hash, at FROM tip_changes WHERE new_hash IN ({marks})"
    for source, block_hash, at in _in_query(conn, sql, hashes):
        for adoption in members[block_hash]:
            adoption.add(source, at, "tip")
    marks = ",".join("?" * len(ADOPTION_KINDS))
    sql = f"SELECT source, hash, kind, at FROM sightings WHERE kind IN ({marks}) AND hash IN ({{marks}})"
    for source, block_hash, kind, at in _in_query(conn, sql, hashes, before=ADOPTION_KINDS):
        for adoption in members[block_hash]:
            adoption.add(source, at, kind)
    for target, adoption in finish:
        target["adopted_by"] = adoption.result(groups)

    probes: dict[str, Counter[str]] = defaultdict(Counter)
    sql = (
        "SELECT hash, result, announced_by_same_peer, COUNT(*) FROM probes WHERE hash IN ({marks}) "
        "GROUP BY hash, result, announced_by_same_peer"
    )
    for block_hash, result, same_peer, count in _in_query(conn, sql, list(shown)):
        result = result if result in PROBE_RESULTS else "other"
        probes[block_hash][result] += count
        if result == "notfound" and same_peer:
            probes[block_hash]["notfound_same_announcer"] += count
    for block_hash, targets in shown.items():
        counts = probes.get(block_hash, Counter())
        for target in targets:
            target["probes"] = {key: counts[key] for key in (*PROBE_RESULTS, "other", "notfound_same_announcer")}

    # "+at" stops SQLite from walking every reorg row in the (is_reorg, at) index just to skip a sort,
    # so it can use an (is_reorg, fork_hash) index.
    sql = (
        "SELECT source, at, disconnected, connected, old_hash, new_hash, fork_hash FROM tip_changes "
        "WHERE is_reorg = 1 AND fork_hash IN ({marks}) ORDER BY +at"
    )
    for source, at, disconnected, connected, old_hash, new_hash, fork_hash in _in_query(conn, sql, list(reorgs)):
        entry = reorgs[fork_hash]
        entry["count"] += 1
        if len(entry["sources"]) < MAX_SAMPLE:
            entry["sources"].append(
                {"source": _text(source), "group": _sighting_group(source, groups), "at": at,
                 "disconnected": disconnected, "connected": connected, "old_hash": old_hash, "new_hash": new_hash}
            )


# -- small shared helpers ---------------------------------------------------------------------


class _Tally:
    """Per-key counts of canonical blocks, orphans, fork events and resets."""

    def __init__(self, keys: Iterable[Any] = (), *, fixed: bool = False) -> None:
        """Start `keys` at zero; a `fixed` tally ignores any other key."""
        self.counts: dict[Any, list[int]] = {key: [0, 0, 0, 0] for key in keys}
        self.fixed = fixed

    def add(self, key: Any, slot: int) -> None:
        """Count one item of kind `slot` under `key`."""
        entry = self.counts.get(key)
        if entry is None:
            if self.fixed or key is None:
                return
            entry = self.counts[key] = [0, 0, 0, 0]
        entry[slot] += 1

    def total(self, slot: int) -> int:
        """Return the count of kind `slot` over every key."""
        return sum(entry[slot] for entry in self.counts.values())

    def bucket_rows(self, orphan_total: int) -> list[dict[str, Any]]:
        """Return phase-bucket rows in key order; `share` divides by `orphan_total`."""
        orphans = orphan_total
        return [
            {"key": key, "blocks": blocks, "orphans": count, "rate": _rate(count, blocks),
             "share": _rate(count, orphans), "forks": forks, "forks_per_1000": _rate(1_000 * forks, blocks)}
            for key, (blocks, count, forks, _) in self.counts.items()
        ]

    def time_rows(self, *, day: bool = False) -> list[dict[str, Any]]:
        """Return time-bin rows oldest first; `day` adds a "day" label."""
        rows = []
        for start in sorted(self.counts):
            blocks, count, forks, reset_count = self.counts[start]
            row = {"start": start, "blocks": blocks, "orphans": count, "rate": _rate(count, blocks), "forks": forks,
                   "resets": reset_count}
            rows.append({"day": _day(start), **row} if day else row)
        return rows


def _miner_row() -> dict[str, Any]:
    """Return an empty per-miner accumulator."""
    return {
        "canonical": 0, "stale": 0, "self_orphans": 0, "races_lost": 0, "races_won": 0, "unattributed_losses": 0,
        "resets": 0, "templates": Counter(),
        "by_phase": {phase: {"canonical": 0, "stale": 0} for phase in PHASE_LABELS},
    }


def _merge_miner_rows(into: dict[str, Any], row: Mapping[str, Any]) -> None:
    """Add one per-miner accumulator into another."""
    for key, value in row.items():
        if key == "templates":
            into[key].update(value)
        elif key == "by_phase":
            for phase, counts in value.items():
                for name, count in counts.items():
                    into[key][phase][name] += count
        else:
            into[key] += value


def _orphans(chain: Chain) -> list[tuple[Any, Any]]:
    """Return (stale block, canonical block at its height) for every settled stale block."""
    out = []
    for node in chain.stale_blocks():
        winner_hash = chain.canonical_hash_at(node.height)
        winner = chain.get(winner_hash) if winner_hash is not None else None
        if winner is not None:
            out.append((node, winner))
    return out


def _observed(node: Any) -> bool:
    """Return whether a canonical block (Node or BlockRef) was watched live (see OBSERVED_LAG).

    A block with no first-seen time was only ever backfilled.
    """
    return node.first_seen_at is not None and node.first_seen_at - node.time <= OBSERVED_LAG


def _contest(loser: str | None, winner: str | None) -> str:
    """Return "self" for the same attributed miner, "race" for different ones, else "unknown"."""
    if loser in (None, UNKNOWN) or winner in (None, UNKNOWN):
        return UNKNOWN
    return SELF if loser == winner else RACE


def _miner(label: str | None) -> str:
    """Return a bounded miner label, "unknown" when unattributed."""
    return _text(label) or UNKNOWN


def _template(node: Any) -> str:
    """Return the template marker of a block, "none" without one, "unknown" without a body."""
    if not node.body:
        return UNKNOWN
    return _text(node.template, 32) or "none"


def _block(node: Any) -> dict[str, Any]:
    """Return the block dict of a chain Node or BlockRef."""
    return {
        "hash": node.hash,
        "height": node.height,
        "time": node.time,
        "miner": _text(node.miner),
        "template": _text(node.template, 32),
        "first_seen_at": node.first_seen_at,
        "min_diff": node.is_min_diff,
        "body": node.body,
    }


def _tip(node: Any, now: float) -> dict[str, Any] | None:
    """Return the best tip's block dict plus its age, or None."""
    if node is None:
        return None
    seen = node.first_seen_at if node.first_seen_at is not None else node.time
    return {**_block(node), "age_s": _sig(now - seen)}


def _phase(phase: Phase) -> dict[str, Any]:
    """Return a Phase as a phase dict."""
    return {
        "height": phase.height,
        "k": phase.k,
        "reset_height": phase.reset_height,
        "d_pre": _sig(phase.d_pre),
        "difficulty": _sig(phase.difficulty),
        "d_ratio": _sig(phase.d_ratio),
        "fast": phase.fast,
        "min_diff": phase.min_diff,
        "label": _phase_label(phase),
        "k_bucket": _k_bucket(phase.k),
        "ratio_bucket": _ratio_bucket(phase.d_ratio),
    }


def _reset(reset: Reset) -> dict[str, Any]:
    """Return a Reset as a reset dict (without the orphan count)."""
    return {
        "height": reset.height,
        "hash": reset.hash,
        "time": reset.time,
        "miner": _text(reset.miner),
        "template": _text(reset.template, 32),
        "gap": reset.gap,
        "next_dt": reset.next_dt,
        "forward_dating": _sig(reset.forward_dating),
        "d_pre": _sig(reset.d_pre),
        "fast_blocks": reset.fast_blocks,
        "cycle_blocks": reset.cycle_blocks,
    }


def _relation(relation: Relation) -> dict[str, Any]:
    """Return a Relation as a relation dict."""
    return dataclasses.asdict(relation)


def _phase_label(phase: Phase) -> str:
    """Return "fast", "slow" or "unknown" for a phase."""
    return UNKNOWN if phase.fast is None else "fast" if phase.fast else "slow"


def _k_bucket(k: int | None) -> str:
    """Return the blocks-since-reset bucket of `k`."""
    if k is None:
        return UNKNOWN
    for low, high, label in K_BUCKETS:
        if k >= low and (high is None or k <= high):
            return label
    return UNKNOWN


def _k_fine(k: int | None) -> str | None:
    """Return the fine near-reset bucket of `k`, or None past 30."""
    if k is None:
        return None
    for low, high, label in K_FINE_BUCKETS:
        if low <= k <= high:
            return label
    return None


def _ratio_bucket(ratio: float | None) -> str:
    """Return the D/D_pre bucket of `ratio`."""
    if ratio is None or not math.isfinite(ratio):
        return UNKNOWN
    for low, high, label in RATIO_BUCKETS:
        if (low is None or ratio >= low) and (high is None or ratio < high):
            return label
    return UNKNOWN


def _group_key(impl: Any, version: Any) -> str:
    """Return the group key "impl major.minor" (or just the impl when the version is unknown)."""
    name = (_text(impl, 32) or UNKNOWN).lower()
    minor = _version(version)
    return f"{name} {minor}" if minor else name


def _version(version: Any) -> str | None:
    """Return "major.minor" of a version string, or None."""
    match = _VERSION_RE.match(version) if isinstance(version, str) else None
    return f"{match[1]}.{match[2]}" if match else None


def _source_groups(conn: Any) -> dict[str, str]:
    """Map each stored source to its group key (RPC endpoints without an impl are "rpc")."""
    rows = conn.execute("SELECT source, kind, impl, impl_version FROM sources LIMIT ?", (MAX_SOURCES,))
    return {source: _group_key(impl or ("rpc" if kind == "rpc" else None), version)
            for source, kind, impl, version in rows}


def _sighting_group(source: str, groups: Mapping[str, str]) -> str:
    """Return the propagation group of a source: the RPC source name, else its impl group."""
    if source.startswith("rpc:"):
        return _text(source) or "rpc"
    return groups.get(source, UNKNOWN)


def _query(conn: Any, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    """Run a query and return its rows as dicts, whatever the connection's row factory."""
    cursor = conn.execute(sql, params)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor]


def _in_query(conn: Any, sql: str, keys: Sequence[Any], *, before: Sequence[Any] = ()) -> Iterator[Any]:
    """Run `sql` (with a `{marks}` IN-list) over `keys` in chunks and yield its rows."""
    for start in range(0, len(keys), SQL_CHUNK):
        chunk = keys[start : start + SQL_CHUNK]
        yield from conn.execute(sql.format(marks=",".join("?" * len(chunk))), (*before, *chunk))


def _height_ranges(ranges: Iterable[tuple[int, int]]) -> tuple[tuple[int, int], ...]:
    """Validate inclusive (low, high) height ranges."""
    out = []
    for item in itertools.islice(ranges, MAX_EXCLUDE_RANGES + 1):
        low, high = item
        if _int_arg(low, "exclude_heights") > _int_arg(high, "exclude_heights"):
            raise ValueError(f"exclude_heights range {low}..{high} is empty")
        out.append((low, high))
    if len(out) > MAX_EXCLUDE_RANGES:
        raise ValueError(f"at most {MAX_EXCLUDE_RANGES} exclude_heights ranges are supported")
    return tuple(out)


def _excluded(height: int, ranges: Sequence[tuple[int, int]]) -> bool:
    """Return whether `height` falls in any excluded range."""
    return any(low <= height <= high for low, high in ranges)


def _now(now: float | None) -> float:
    """Return `now` validated, or the wall clock."""
    return time.time() if now is None else _finite_arg(now, "now")


def _since(value: float | None) -> float | None:
    """Validate an optional `since` timestamp."""
    return None if value is None else _finite_arg(value, "since")


def _finite_arg(value: Any, what: str) -> float:
    """Return `value` as a float, or raise ValueError unless it is a finite number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{what} must be a finite number, got {value!r:.40}")
    return float(value)


def _int_arg(value: Any, what: str) -> int:
    """Return `value` if it is a plain integer, else raise ValueError."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{what} must be an integer, got {value!r:.40}")
    return value


def _clamp_int(value: Any, low: int, high: int, what: str) -> int:
    """Return an integer parameter clamped into [low, high]."""
    return max(low, min(high, _int_arg(value, what)))


def _int_or_none(value: Any) -> int | None:
    """Return `value` if it is a plain integer, else None."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _float_or_none(value: Any) -> float | None:
    """Return `value` as a finite float, else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def _hash_or_none(value: Any) -> str | None:
    """Return a lowercased 64-hex block hash, else None."""
    return value.lower() if isinstance(value, str) and _HASH_RE.fullmatch(value) else None


def _text(value: Any, limit: int = MAX_TEXT) -> str | None:
    """Return `value` as a length-capped string; None passes through."""
    if value is None:
        return None
    return (value if isinstance(value, str) else str(value))[:limit]


def _sig(value: float | None) -> float | None:
    """Round to 6 significant digits for JSON; None for missing or non-finite values."""
    if value is None or not math.isfinite(value):
        return None
    return float(f"{value:.6g}")


def _rate(count: float, total: float) -> float | None:
    """Return count / total rounded, or None when total is zero."""
    return _sig(count / total) if total else None


def _percentile(values: Sequence[float], p: float) -> float | None:
    """Return the nearest-rank percentile of sorted `values` (the historical analysis's convention)."""
    if not values:
        return None
    return values[min(len(values) - 1, max(0, int(round(p / 100 * (len(values) - 1)))))]


def _plain(value: Any, depth: int = 0) -> Any:
    """Return a bounded JSON-able copy of collector data (strings capped, floats finite)."""
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value[:MAX_TEXT]
    if isinstance(value, (bytes, bytearray)):
        return bytes(value[:64]).hex()
    if depth >= MAX_PLAIN_DEPTH:
        return _text(repr(value))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        value = {f.name: getattr(value, f.name) for f in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(k)[:64]: _plain(v, depth + 1) for k, v in itertools.islice(value.items(), MAX_PLAIN_ITEMS)}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_plain(v, depth + 1) for v in itertools.islice(value, MAX_PLAIN_ITEMS)]
    return _text(value)


def _day(start: float) -> str:
    """Return the UTC date of a unix time."""
    return datetime.fromtimestamp(start, timezone.utc).strftime("%Y-%m-%d")


def _iso(when: float) -> str:
    """Return a unix time as "YYYY-MM-DD HH:MM:SS" UTC."""
    return datetime.fromtimestamp(when, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _clean(value: Any) -> str:
    """Return printable text for the terminal ("-" for None)."""
    if value is None:
        return "-"
    return "".join(ch if ch.isprintable() else "?" for ch in str(value)[:MAX_TEXT])


def _fmt(value: Any) -> str:
    """Format a table cell: "-" for None, 4 significant digits for floats."""
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def _pct(value: float | None) -> str:
    """Format a fraction as a percentage."""
    return "-" if value is None else f"{100 * value:.2f}%"


def _table(header: Sequence[str], rows: Iterable[Sequence[Any]], title: str) -> list[str]:
    """Render a titled, column-aligned text table."""
    cells = [[str(cell) for cell in header]] + [[str(cell) for cell in row] for row in rows]
    widths = [max(len(row[i]) for row in cells) for i in range(len(header))]
    lines = ["", title]
    for row in cells:
        lines.append("  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip())
    if len(cells) == 1:
        lines.append("(none)")
    return lines
