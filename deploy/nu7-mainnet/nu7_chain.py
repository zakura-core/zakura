"""Pure NU7 chain arithmetic for the mainnet developer dashboard.

Everything here is a function of its arguments: metric expositions, block
header times, counter snapshots and RPC results go in, numbers and small dicts
come out. The collector does the I/O. Mainnet constants live here so the
collector, the page and the tests agree on one source.
"""

from __future__ import annotations

import math
import re
import statistics

NU7_ACTIVATION_HEIGHT = 3_543_405
NU7_BRANCH_ID = "77190ad9"
MAINNET_MAGIC = "24e92764"
PRE_NU7_TARGET_SPACING = 75
POST_NU7_TARGET_SPACING = 25
PRE_NU7_DAA_WINDOW = 17
POST_NU7_DAA_WINDOW = 102
DAA_DAMPING_FACTOR = 4
DAA_MAX_ADJUST_UP_PERCENT = 16
DAA_MAX_ADJUST_DOWN_PERCENT = 32
DAA_MEDIAN_TIME_SPAN_BLOCKS = 11
NSM_FEE_PERCENT = 60
MINER_FEE_PERCENT = 40
ORCHARD_BLOCK_ACTION_LIMIT = 330
SAPLING_BLOCK_IO_LIMIT = 300
SPROUT_BLOCK_JOINSPLIT_LIMIT = 0
COINBASE_MATURITY_BLOCKS = 100
MIN_PROTOCOL_VERSION_NU7 = 170190
ZAT_PER_ZEC = 100_000_000

# Prometheus exposition line: name, optional labels, value.
EXPOSITION_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+-]+|NaN|[+-]?Inf)\s*$")

# The zakurad metric names the collector reads, by the role they play. The
# per-block `zcash_chain_block_*` family is emitted by the instrumented build
# only; the collector treats every entry as optional.
METRIC_NAMES = {
    "tip_height": "zcash_chain_tip_height",
    "verified_height": "zcash_chain_verified_block_height",
    "tip_time": "zcash_chain_tip_time_seconds",
    "tip_interval": "zcash_chain_tip_block_interval_seconds",
    "tip_difficulty": "zcash_chain_tip_difficulty",
    "tip_nu7_active": "zcash_chain_tip_nu7_active",
    "nsm_balance": "zcash_chain_nsm_value_balance_zat",
    "nsm_change": "zcash_chain_nsm_value_balance_change_zat",
    "pool_lockbox": "zcash_chain_value_pool_lockbox_zat",
    "reorgs_total": "zcash_chain_reorg_total",
    "block_height": "zcash_chain_block_height",
    "peers": "zcash_net_peers",
    "peers_obsolete": "zcash_net_peers_obsolete",
    "mempool_tx": "zcash_mempool_size_transactions",
    "mempool_bytes": "zcash_mempool_size_bytes",
    "mempool_actions_paid": "zcash_mempool_actions_paid",
    "mempool_actions_unpaid": "zcash_mempool_actions_unpaid",
    "mempool_evicted": "zcash_mempool_evicted",
    "sync_distance": "sync_estimated_distance_to_tip",
    "build_info": "zakura_build_info",
    "verify_duration": "sync_block_verify_duration_seconds",
    "commit_duration": "state_semantic_commit_queue_and_commit_duration_seconds",
    "rocksdb_duration": "zakura_state_rocksdb_batch_commit_duration_seconds",
    "tx_fee": "zcash_chain_block_tx_fee_zat",
}

# Per-block quantities whose `_total` counters attribute values to blocks: the
# stored column name maps to the metric stem (gauge = stem, counter = stem_total).
BLOCK_COUNTERS = {
    "fees_zat": "zcash_chain_block_fees_zat",
    "nsm_fee_zat": "zcash_chain_block_fees_nsm_zat",
    "miner_fee_zat": "zcash_chain_block_fees_miner_zat",
    "subsidy_zat": "zcash_chain_block_subsidy_zat",
    "reissuance_zat": "zcash_chain_block_subsidy_reissuance_zat",
    "orchard_actions": "zcash_chain_block_orchard_actions",
    "ironwood_actions": "zcash_chain_block_ironwood_actions",
    "sapling_ios": "zcash_chain_block_sapling_ios",
    "sprout_joinsplits": "zcash_chain_block_sprout_joinsplits",
    "transparent_inputs": "zcash_chain_block_transparent_inputs",
    "transparent_outputs": "zcash_chain_block_transparent_outputs",
}

WANTED_PREFIXES = ("zcash_chain_", "zcash_net_peers", "zcash_mempool_", "sync_estimated_",
                   "zakura_build_info", "sync_block_verify_duration",
                   "state_semantic_commit_queue_and_commit_duration",
                   "zakura_state_rocksdb_batch_commit_duration")


def parse_exposition(text, prefixes=WANTED_PREFIXES):
    """Parse the Prometheus text format into {name: [(labels, value)]}.

    Only lines whose name starts with one of `prefixes` are kept, which keeps
    a multi-megabyte mainnet exposition cheap to scan.
    """
    out = {}
    for line in text.splitlines():
        if not line or line[0] == "#" or not line.startswith(prefixes):
            continue
        match = EXPOSITION_LINE.match(line)
        if not match:
            continue
        name, labels, raw = match.group(1), match.group(2) or "", match.group(3)
        try:
            value = float(raw)
        except ValueError:
            continue
        if math.isnan(value) or math.isinf(value):
            continue
        out.setdefault(name, []).append((labels, value))
    return out


def bare(metrics, name):
    """The unlabelled sample of `name`, or the first sample when every one is labelled."""
    samples = metrics.get(name, [])
    for labels, value in samples:
        if labels == "":
            return value
    return samples[0][1] if samples else None


def quantile(metrics, name, q):
    """The `quantile="q"` sample of a summary, or None."""
    wanted = f'quantile="{q}"'
    for labels, value in metrics.get(name, []):
        if wanted in labels:
            return value
    return None


def label_value(metrics, name, label):
    """The first value of `label` among `name`'s samples, or None."""
    pattern = re.compile(label + r'="([^"]*)"')
    for labels, _ in metrics.get(name, []):
        match = pattern.search(labels)
        if match:
            return match.group(1)
    return None


def whole(value):
    """A metric sample as an int, or None when absent."""
    return None if value is None else int(round(value))


def target_spacing(height, activation_height=NU7_ACTIVATION_HEIGHT):
    return POST_NU7_TARGET_SPACING if height >= activation_height else PRE_NU7_TARGET_SPACING


def daa_window(height, activation_height=NU7_ACTIVATION_HEIGHT):
    return POST_NU7_DAA_WINDOW if height >= activation_height else PRE_NU7_DAA_WINDOW


def expected_median_interval(spacing):
    """Block intervals are exponential, so the median is ln2 times the mean target."""
    return spacing * math.log(2)


def interval_stats(intervals, limit):
    """Median, mean and count over the last `limit` intervals (None when empty)."""
    sample = [i for i in intervals[-limit:] if i is not None] if limit else []
    if not sample:
        return {"median": None, "mean": None, "count": 0}
    return {
        "median": statistics.median(sample),
        "mean": statistics.fmean(sample),
        "count": len(sample),
    }


def difficulty_change_pct(previous, current):
    """Per-block difficulty change in percent, or None without a previous value."""
    if previous is None or current is None or previous <= 0:
        return None
    return (current / previous - 1.0) * 100.0


def activation_eta(height, tip_time, recent_intervals, activation_height=NU7_ACTIVATION_HEIGHT):
    """When activation is expected, extrapolating the mean of recent intervals.

    Returns (blocks_remaining, eta_unix, basis_interval). Past activation the
    remaining count is 0 and there is no ETA.
    """
    remaining = activation_height - height
    if remaining <= 0:
        return 0, None, None
    sample = [i for i in recent_intervals if i is not None and i > 0]
    basis = statistics.fmean(sample) if sample else float(PRE_NU7_TARGET_SPACING)
    return remaining, tip_time + remaining * basis, basis


def nsm_fee_share(fees_zat):
    """The NSM's share of a block's aggregate fees: floor(60%), per ZIP 233 as deployed."""
    return fees_zat * NSM_FEE_PERCENT // 100


def fee_split_ok(fees_zat, nsm_fee_zat, nu7_active=True):
    """Whether a block's reported NSM share matches consensus: floor(60%) of the
    fees once NU7 is active, nothing before. None when a value is missing or the
    activation state is unknown."""
    if fees_zat is None or nsm_fee_zat is None or nu7_active is None:
        return None
    expected = nsm_fee_share(fees_zat) if nu7_active else 0
    return nsm_fee_zat == expected


def attribute_counters(previous, current):
    """Attribute per-block counter totals to the blocks seen between two scrapes.

    `previous` and `current` are {"height": int, "totals": {column: int},
    "latest": {column: int}} snapshots from consecutive scrapes. Returns a list
    of (height, values, merged_from) rows to store, oldest first, or an
    "reset" marker when the counters went backwards (the node restarted).

    With one new block every delta is that block's value. With k > 1 new
    blocks the tip row takes the latest gauges and the row before it takes the
    rest of the delta, marked as merged from the oldest unseen height, so the
    totals stay exact even though the split between those blocks is unknown.
    """
    if previous is None or current is None:
        return []
    new_blocks = current["height"] - previous["height"]
    if new_blocks <= 0:
        return []
    deltas = {}
    for column, total in current["totals"].items():
        before = previous["totals"].get(column)
        if before is None or total is None:
            continue
        if total < before:
            return "reset"
        deltas[column] = total - before
    if not deltas:
        return []
    tip = current["height"]
    if new_blocks == 1:
        return [(tip, deltas, None)]
    latest = {column: current["latest"].get(column) for column in deltas}
    remainder = {
        column: deltas[column] - (latest[column] or 0)
        for column in deltas
    }
    rows = [(tip - 1, remainder, previous["height"] + 1)]
    if all(value is not None for value in latest.values()):
        rows.append((tip, latest, None))
    return rows


def mempool_fee_quantiles(raw_mempool):
    """Fee quantiles (zat) and the median fee rate (zat/byte) of a verbose getrawmempool."""
    fees = []
    rates = []
    for entry in (raw_mempool or {}).values():
        fee = entry.get("fee")
        size = entry.get("size")
        if not isinstance(fee, (int, float)):
            continue
        fee_zat = int(round(fee * ZAT_PER_ZEC))
        fees.append(fee_zat)
        if isinstance(size, int) and size > 0:
            rates.append(fee_zat / size)
    if not fees:
        return {"p10": None, "p50": None, "p90": None, "feeRateP50": None, "count": 0}
    fees.sort()

    def pick(fraction):
        # Nearest-rank quantile: the smallest fee at or above the fraction.
        index = min(len(fees) - 1, max(0, math.ceil(fraction * len(fees)) - 1))
        return fees[index]

    return {
        "p10": pick(0.10),
        "p50": pick(0.50),
        "p90": pick(0.90),
        "feeRateP50": statistics.median(rates) if rates else None,
        "count": len(fees),
    }


def rules_at(height, branch_id, base_subsidy_zat, activation_height=NU7_ACTIVATION_HEIGHT):
    """The consensus parameters in force at `height`, keyed like the public NU7 page."""
    active = height >= activation_height
    spacing = target_spacing(height, activation_height)
    window = daa_window(height, activation_height)
    return {
        "effectiveHeight": height,
        "nu7Active": active,
        "branchId": branch_id,
        "targetSpacingSeconds": spacing,
        "averagingWindowBlocks": window,
        "averagingTimespanSeconds": spacing * window,
        "dampingFactor": DAA_DAMPING_FACTOR,
        "maxAdjustUpPercent": DAA_MAX_ADJUST_UP_PERCENT,
        "maxAdjustDownPercent": DAA_MAX_ADJUST_DOWN_PERCENT,
        "medianTimeSpanBlocks": DAA_MEDIAN_TIME_SPAN_BLOCKS,
        "minerPercent": MINER_FEE_PERCENT if active else 100,
        "nsmPercent": NSM_FEE_PERCENT if active else 0,
        "nsmRounding": "floor-on-block-aggregate" if active else None,
        "coinbaseMaturityBlocks": COINBASE_MATURITY_BLOCKS,
        "baseSubsidyZat": base_subsidy_zat,
        "minimumProtocolVersion": MIN_PROTOCOL_VERSION_NU7 if active else None,
        "actionLimits": {
            "orchardActionsPerPool": ORCHARD_BLOCK_ACTION_LIMIT if active else None,
            "saplingSpendsAndOutputs": SAPLING_BLOCK_IO_LIMIT if active else None,
            "sproutJoinSplits": SPROUT_BLOCK_JOINSPLIT_LIMIT if active else None,
        },
        "minimumDifficulty": {"comparison": "disabled"},
    }


def zec_to_zat(value):
    """An RPC ZEC float as integer zatoshis, or None."""
    if not isinstance(value, (int, float)):
        return None
    return int(round(value * ZAT_PER_ZEC))
