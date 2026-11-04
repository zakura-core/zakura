"""The Zcash difficulty adjustment algorithm, bit for bit, plus projections.

`next_threshold` replicates the node's rule (zakura-header-chain
`AdjustedDifficulty::threshold_bits`) with Python integers so the dashboard
can check every block's `bits` against what consensus required, predict the
next block's difficulty, and simulate how difficulty and block time move after
NU7 lowers the target spacing from 75 s to 25 s.

Targets are the 256-bit numbers a block hash must stay below; difficulty is
the pow limit divided by the target, the float `getblock` reports; work is the
expected number of hash attempts, 2^256 / (target + 1). A block at difficulty
D needs about D × 2^13 attempts on mainnet, so with a network solving H
attempts per second the expected block interval is work / H.

Mainnet only: the testnet minimum-difficulty rule is not modelled.
"""

from __future__ import annotations

import math
import random
import statistics

POW_LIMIT_BITS = 0x1F07FFFF
MEDIAN_TIME_SPAN = 11
DAMPING_FACTOR = 4
MAX_ADJUST_UP_PERCENT = 16
MAX_ADJUST_DOWN_PERCENT = 32
PRE_NU7_WINDOW = 17
POST_NU7_WINDOW = 102
PRE_NU7_SPACING = 75
POST_NU7_SPACING = 25
CONTEXT_BLOCKS = POST_NU7_WINDOW + MEDIAN_TIME_SPAN
COMPACT_MANTISSA_MASK = 0x7FFFFF


def expand_compact(bits):
    """The target a compact `bits` value encodes (nBits decoding), or None for a negative value."""
    if bits & 0x800000:
        return None
    mantissa = bits & COMPACT_MANTISSA_MASK
    exponent = bits >> 24
    if exponent <= 3:
        return mantissa >> (8 * (3 - exponent))
    return mantissa << (8 * (exponent - 3))


def compact_from_target(target):
    """The compact encoding of a positive target, truncating like the node's `to_compact`."""
    if target <= 0:
        raise ValueError("a target must be positive")
    size = target.bit_length() // 8 + 1
    if target <= COMPACT_MANTISSA_MASK:
        mantissa = target << (8 * (3 - size))
    else:
        mantissa = target >> (8 * (size - 3))
    return mantissa | (size << 24)


MAINNET_POW_LIMIT = expand_compact(POW_LIMIT_BITS)


def work(target):
    """Expected hash attempts to find a block under `target`."""
    return (1 << 256) // (target + 1)


WORK_PER_DIFFICULTY = work(MAINNET_POW_LIMIT)


def difficulty(bits):
    """The difficulty `getblock` reports for `bits`: pow-limit mantissa over this mantissa, scaled by the exponent gap."""
    # Same arithmetic as zcashd GetDifficulty / zakura relative_to_network: both mantissas
    # shifted up one byte in u32 space (which drops the exponent byte), then 256 per exponent step.
    numerator = float((POW_LIMIT_BITS << 8) & 0xFFFFFFFF)
    denominator = float((bits << 8) & 0xFFFFFFFF)
    exponent_gap = (POW_LIMIT_BITS >> 24) - (bits >> 24)
    return numerator / denominator * (256.0 ** exponent_gap)


def parse_bits(value):
    """A `bits` field as an int, from the hex string RPC returns or an int."""
    return int(value, 16) if isinstance(value, str) else int(value)


def rule_at(height, activation_height):
    """(window, spacing, averaging timespan) for the block at `height`."""
    if height >= activation_height:
        return POST_NU7_WINDOW, POST_NU7_SPACING, POST_NU7_WINDOW * POST_NU7_SPACING
    return PRE_NU7_WINDOW, PRE_NU7_SPACING, PRE_NU7_WINDOW * PRE_NU7_SPACING


def median(values):
    """The consensus median: the middle element of the sorted list (upper middle for even counts)."""
    ordered = sorted(values)
    return ordered[len(ordered) // 2]


def truncating_div(value, divisor):
    """Integer division rounding toward zero, like the node's Duration arithmetic."""
    quotient = abs(value) // divisor
    return quotient if value >= 0 else -quotient


def next_threshold(targets, times, candidate_height, activation_height):
    """The compact bits consensus requires for the block at `candidate_height`.

    `targets` and `times` list the previous blocks newest first, at least
    window + 11 of them (fewer only near genesis). Mirrors `threshold_bits`:
    mean of the window's targets, a damped and clamped median timespan, then
    the pow-limit cap and compact truncation.
    """
    window, _, awt = rule_at(candidate_height, activation_height)
    if candidate_height <= window:
        return POW_LIMIT_BITS
    recent = targets[:window]
    mean_target = sum(recent) // len(recent)
    timespan = median_timespan(times, window)
    damped = awt + truncating_div(timespan - awt, DAMPING_FACTOR)
    lower = awt * (100 - MAX_ADJUST_UP_PERCENT) // 100
    upper = awt * (100 + MAX_ADJUST_DOWN_PERCENT) // 100
    bounded = max(lower, min(upper, damped))
    threshold = min(MAINNET_POW_LIMIT, (mean_target // awt) * bounded)
    return compact_from_target(threshold)


def median_timespan(times, window):
    """Median time of the newest 11 blocks minus the median time of the 11 blocks before the window."""
    newest = median(times[:MEDIAN_TIME_SPAN])
    older = times[window:window + MEDIAN_TIME_SPAN]
    if not older:
        older = times[-1:]
    return newest - median(older)


def hashrate(bits_and_times, count):
    """Network solutions per second over the newest `count` blocks, as `getnetworksolps` computes it.

    `bits_and_times` lists (bits, time) newest first with at least count + 1
    entries; the work of the newest `count` blocks is divided by the time span
    of count + 1 headers. None when the span is not positive.
    """
    headers = bits_and_times[:count + 1]
    if len(headers) < 2:
        return None
    span = max(t for _, t in headers) - min(t for _, t in headers)
    if span <= 0:
        return None
    total = sum(work(expand_compact(parse_bits(bits))) for bits, _ in headers[:count])
    return total // span


def expected_interval(bits, solutions_per_second):
    """Seconds a block at `bits` takes on average at that solution rate."""
    if not solutions_per_second:
        return None
    return work(expand_compact(parse_bits(bits))) / solutions_per_second


def equilibrium_bits(solutions_per_second, spacing):
    """The compact target at which blocks arrive every `spacing` seconds at that solution rate."""
    target = (1 << 256) // (solutions_per_second * spacing) - 1
    return compact_from_target(min(max(target, 1), MAINNET_POW_LIMIT))


class DaaSim:
    """Steps the difficulty rule forward over simulated blocks.

    The context is the real or synthetic chain the simulation continues from:
    (bits, time) pairs newest first. Each step picks the next block's bits
    from the rule, places it `interval` seconds after the previous block
    (never at or before the median time past, which consensus rejects), and
    makes it the newest context entry.
    """

    def __init__(self, context, start_height, activation_height):
        self.targets = [expand_compact(parse_bits(bits)) for bits, _ in context[:CONTEXT_BLOCKS]]
        self.times = [int(t) for _, t in context[:CONTEXT_BLOCKS]]
        self.height = start_height
        self.activation_height = activation_height
        self.clock = float(self.times[0])

    @property
    def next_bits(self):
        return next_threshold(self.targets, self.times, self.height, self.activation_height)

    def step(self, interval):
        bits = self.next_bits
        self.clock += interval
        time = max(int(round(self.clock)), median(self.times[:MEDIAN_TIME_SPAN]) + 1)
        self.targets.insert(0, expand_compact(bits))
        self.times.insert(0, time)
        del self.targets[CONTEXT_BLOCKS:]
        del self.times[CONTEXT_BLOCKS:]
        height = self.height
        self.height += 1
        return height, bits, time


def project(context, start_height, blocks, solutions_per_second, activation_height):
    """The noise-free difficulty path: every block takes exactly its expected time.

    Returns one dict per simulated height with the bits, difficulty and the
    interval that block took.
    """
    sim = DaaSim(context, start_height, activation_height)
    path = []
    for _ in range(blocks):
        bits = sim.next_bits
        interval = expected_interval(bits, solutions_per_second)
        height, _, _ = sim.step(interval)
        path.append({"height": height, "bits": bits, "difficulty": difficulty(bits),
                     "expectedIntervalSeconds": interval})
    return path


def monte_carlo(context, start_height, blocks, solutions_per_second, activation_height, runs=300, seed=7):
    """Difficulty quantiles per height when block intervals are random (exponential).

    Returns one dict per height with p10, p50 and p90 difficulty over `runs`
    simulations, each drawing every interval from an exponential distribution
    whose mean is that block's expected time.
    """
    rng = random.Random(seed)
    samples = [[] for _ in range(blocks)]
    for _ in range(runs):
        sim = DaaSim(context, start_height, activation_height)
        for index in range(blocks):
            bits = sim.next_bits
            mean = expected_interval(bits, solutions_per_second)
            sim.step(rng.expovariate(1.0 / mean))
            samples[index].append(difficulty(bits))
    band = []
    for index, values in enumerate(samples):
        values.sort()
        band.append({
            "height": start_height + index,
            "p10": values[int(len(values) * 0.10)],
            "p50": statistics.median(values),
            "p90": values[min(len(values) - 1, int(len(values) * 0.90))],
        })
    return band


def steady_context(bits, solutions_per_second, end_time, blocks=CONTEXT_BLOCKS):
    """A synthetic chain of `blocks` blocks all at `bits`, spaced by their expected time, ending at `end_time`."""
    interval = expected_interval(bits, solutions_per_second)
    return [(bits, int(round(end_time - index * interval))) for index in range(blocks)]


def convergence_summary(path, target_spacing):
    """When a projected path first comes within 5% of the target spacing, and its extreme interval."""
    for entry in path:
        if abs(entry["expectedIntervalSeconds"] - target_spacing) <= 0.05 * target_spacing:
            return {"withinFivePercentAt": entry["height"],
                    "minIntervalSeconds": min(e["expectedIntervalSeconds"] for e in path),
                    "maxIntervalSeconds": max(e["expectedIntervalSeconds"] for e in path)}
    return {"withinFivePercentAt": None,
            "minIntervalSeconds": min(e["expectedIntervalSeconds"] for e in path) if path else None,
            "maxIntervalSeconds": max(e["expectedIntervalSeconds"] for e in path) if path else None}


def geometric_mean_interval(intervals):
    """Helper for tests and summaries: the geometric mean of positive intervals."""
    values = [i for i in intervals if i and i > 0]
    if not values:
        return None
    return math.exp(sum(math.log(v) for v in values) / len(values))
