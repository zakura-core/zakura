#!/usr/bin/env python3
"""Tests for the difficulty model and miner attribution (stdlib unittest)."""

from __future__ import annotations

import json
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import nu7_daa as daa  # noqa: E402
import nu7_miners as miners  # noqa: E402

TESTDATA = Path(__file__).resolve().parent / "testdata"
ACTIVATION = 3_543_405
HEADERS = json.loads((TESTDATA / "mainnet_headers.json").read_text())
COINBASES = json.loads((TESTDATA / "mainnet_coinbases.json").read_text())["coinbases"]


def context_before(index):
    """(targets, times) newest first for the fixture block at `index`."""
    previous = HEADERS["headers"][max(0, index - daa.CONTEXT_BLOCKS):index][::-1]
    return ([daa.expand_compact(int(h["bits"], 16)) for h in previous], [h["time"] for h in previous])


class CompactEncodingTests(unittest.TestCase):
    def test_pow_limit_round_trips(self):
        self.assertEqual(daa.MAINNET_POW_LIMIT, 0x7FFFF << 224)
        self.assertEqual(daa.compact_from_target(daa.MAINNET_POW_LIMIT), 0x1F07FFFF)
        self.assertEqual(daa.WORK_PER_DIFFICULTY, 8192)
        self.assertAlmostEqual(daa.difficulty(0x1F07FFFF), 1.0)

    def test_compact_truncates_and_keeps_the_sign_bit_clear(self):
        for bits in (0x1C123456, 0x1B782EE4, 0x1F07FFFF, 0x0300FFFF, 0x04123456):
            target = daa.expand_compact(bits)
            self.assertEqual(daa.compact_from_target(target), bits)
            if target > daa.COMPACT_MANTISSA_MASK:
                self.assertEqual(daa.compact_from_target(target + 1), bits, "a lower byte is truncated away")
        self.assertEqual(daa.compact_from_target(0x800000), 0x04008000, "a mantissa that would set the sign bit shifts up a byte")
        self.assertIsNone(daa.expand_compact(0x1C800000))

    def test_damping_truncates_toward_zero(self):
        self.assertEqual(daa.truncating_div(-5, 4), -1)
        self.assertEqual(daa.truncating_div(5, 4), 1)
        self.assertEqual(daa.truncating_div(-8, 4), -2)


class RuleTests(unittest.TestCase):
    def test_reproduces_every_mainnet_block_in_the_fixture(self):
        headers = HEADERS["headers"]
        checked = 0
        for index in range(daa.PRE_NU7_WINDOW + daa.MEDIAN_TIME_SPAN, len(headers)):
            targets, times = context_before(index)
            block = headers[index]
            self.assertEqual(daa.next_threshold(targets, times, block["height"], ACTIVATION), int(block["bits"], 16),
                             f"block {block['height']}")
            self.assertEqual(daa.difficulty(int(block["bits"], 16)), block["difficulty"])
            checked += 1
        self.assertGreaterEqual(checked, 130)

    def test_hashrate_matches_getnetworksolps(self):
        newest_first = [(h["bits"], h["time"]) for h in reversed(HEADERS["headers"])]
        self.assertEqual(daa.hashrate(newest_first, 120), HEADERS["solps120AtTip"])
        self.assertIsNone(daa.hashrate([("1c123456", 5), ("1c123456", 5)], 1), "no time span, no rate")

    def test_rule_switches_at_the_activation_height(self):
        self.assertEqual(daa.rule_at(ACTIVATION - 1, ACTIVATION), (17, 75, 1275))
        self.assertEqual(daa.rule_at(ACTIVATION, ACTIVATION), (102, 25, 2550))

    def test_clamps_bound_the_timespan(self):
        targets = [daa.expand_compact(0x1C123456)] * daa.CONTEXT_BLOCKS
        mean = sum(targets[:17]) // 17
        slow = [1_000_000 - 10_000 * i for i in range(daa.CONTEXT_BLOCKS)]
        fast = [1_000_000 - i for i in range(daa.CONTEXT_BLOCKS)]
        self.assertEqual(daa.next_threshold(targets, slow, 1000, ACTIVATION), daa.compact_from_target((mean // 1275) * 1683))
        self.assertEqual(daa.next_threshold(targets, fast, 1000, ACTIVATION), daa.compact_from_target((mean // 1275) * 1071))

    def test_constant_hashrate_projection_settles_at_a_third(self):
        tip = HEADERS["headers"][-1]
        bits0 = int(tip["bits"], 16)
        rate = daa.hashrate([(h["bits"], h["time"]) for h in reversed(HEADERS["headers"])], 120)
        seed = daa.steady_context(bits0, rate, tip["time"])
        self.assertAlmostEqual(seed[0][1] - seed[1][1], daa.expected_interval(bits0, rate), delta=1)
        path = daa.project(seed, ACTIVATION, 1500, rate, ACTIVATION)
        self.assertEqual(path[0]["height"], ACTIVATION)
        d0 = daa.difficulty(bits0)
        self.assertAlmostEqual(path[0]["difficulty"] / d0, 1 / 1.32, places=2, msg="the first NU7 block already takes the full clamp")
        self.assertAlmostEqual(path[-1]["difficulty"] / d0, 1 / 3, delta=0.02)
        self.assertAlmostEqual(path[-1]["expectedIntervalSeconds"], 25, delta=0.5)
        for earlier, later in zip(path, path[1:]):
            self.assertLessEqual(later["difficulty"], earlier["difficulty"] * 1.001, "difficulty only eases toward the new spacing")
        summary = daa.convergence_summary(path, 25)
        self.assertLess(summary["withinFivePercentAt"] - ACTIVATION, 600)

    def test_simulation_steps_match_the_rule_across_activation(self):
        rng = random.Random(1)
        bits = [f"{daa.compact_from_target(daa.expand_compact(0x1C123456) + rng.randrange(1 << 190)):08x}" for _ in range(daa.CONTEXT_BLOCKS)]
        times = [2_000_000 - 75 * i + rng.randrange(-30, 30) for i in range(daa.CONTEXT_BLOCKS)]
        sim = daa.DaaSim(list(zip(bits, times)), ACTIVATION - 5, ACTIVATION)
        for _ in range(12):
            expected = daa.next_threshold(list(sim.targets), list(sim.times), sim.height, ACTIVATION)
            height, stepped, time = sim.step(rng.uniform(5, 120))
            self.assertEqual(stepped, expected)
            self.assertGreater(time, daa.median(sim.times[1:12]), "a simulated block never sits at or before its median time past")
        self.assertEqual(sim.height, ACTIVATION + 7)

    def test_monte_carlo_band_brackets_the_noise_free_path(self):
        tip = HEADERS["headers"][-1]
        rate = daa.hashrate([(h["bits"], h["time"]) for h in reversed(HEADERS["headers"])], 120)
        seed = daa.steady_context(int(tip["bits"], 16), rate, tip["time"])
        band = daa.monte_carlo(seed, ACTIVATION, 120, rate, ACTIVATION, runs=30, seed=3)
        path = daa.project(seed, ACTIVATION, 120, rate, ACTIVATION)
        self.assertEqual(len(band), 120)
        self.assertLessEqual(band[-1]["p10"], path[-1]["difficulty"] * 1.05)
        self.assertGreaterEqual(band[-1]["p90"], path[-1]["difficulty"] * 0.95)


class MinerTests(unittest.TestCase):
    def test_tags_and_payouts_from_real_coinbases(self):
        by_height = {c["height"]: c for c in COINBASES}
        luxor_like = [c for c in COINBASES if "Mined by" in (miners.coinbase_tag(c["coinbase"]) or "")]
        self.assertTrue(luxor_like)
        for c in COINBASES:
            tag = miners.coinbase_tag(c["coinbase"])
            address = miners.miner_payout(c["vout"], c["fundingstreams"])
            self.assertNotEqual(address, "t3cFfPt1Bcvgez9ZbMBFWeZsskxTkPzGCow", "a funding stream is never the miner")
            self.assertTrue(miners.miner_id(tag, address))
        shielded = by_height[3541777]
        self.assertIsNone(miners.miner_payout(shielded["vout"], shielded["fundingstreams"]))
        self.assertEqual(miners.miner_id(miners.coinbase_tag(shielded["coinbase"]), None), "Sluicey")
        untagged = by_height[3541757]
        self.assertIsNone(miners.coinbase_tag(untagged["coinbase"]))
        self.assertEqual(miners.miner_id(None, miners.miner_payout(untagged["vout"], untagged["fundingstreams"])), "addr:t1SEgZ…8NJv")

    def test_known_pools_and_solo_tags(self):
        self.assertEqual(miners.miner_id("/Mined by Luxor/", "t1abc"), "Luxor")
        self.assertEqual(miners.miner_id("2Miners https://2miners.com", None), "2Miners")
        self.assertEqual(miners.miner_id("Mined by milledgeville", "t1abcdefghij"), "solo:milledgeville")
        self.assertEqual(miners.miner_id(None, None), "shielded:unknown")
        self.assertEqual(miners.coinbase_tag("03fd0a3604f09fa693"), None)
        self.assertEqual(miners.coinbase_tag("bad"), None)

    def test_status_flags_only_statistically_missing_miners(self):
        self.assertEqual(miners.miner_status(0.30, 0, 20), "absent", "(0.7)^20 is well under 1%")
        self.assertEqual(miners.miner_status(0.05, 0, 20), "ok", "a 5% pool can easily miss 20 blocks")
        self.assertEqual(miners.miner_status(0.30, 20, 300), "low", "90 expected, 20 seen")
        self.assertEqual(miners.miner_status(0.30, 80, 300), "ok")
        self.assertEqual(miners.miner_status(None, 0, 300), "ok")

    def test_shares_table_and_buckets(self):
        rows = [{"height": h, "time": 1000 + 25 * h, "miner_id": "Luxor" if h % 3 else "2Miners"} for h in range(1, 31)]
        baseline = miners.miner_shares(rows[:20])
        recent = miners.miner_shares(rows[20:] + [{"height": 31, "time": 1800, "miner_id": "Newcomer"}])
        self.assertEqual(baseline["blocks"], 20)
        self.assertAlmostEqual(baseline["miners"]["Luxor"]["share"], 14 / 20)
        table = miners.miner_table(baseline, recent, now=2000)
        self.assertEqual([row["miner"] for row in table], ["Luxor", "2Miners", "Newcomer"])
        self.assertEqual(table[2]["status"], "new")
        self.assertEqual(table[0]["lastHeight"], 29)
        buckets = miners.miner_buckets(rows, 250, top=1)
        self.assertEqual(buckets["miners"], ["Luxor", "other"])
        self.assertEqual(sum(buckets["counts"]["Luxor"]) + sum(buckets["counts"]["other"]), 30)


if __name__ == "__main__":
    unittest.main()
