"""Controlled counterexamples and seeded comparisons; no network simulator."""

import math
import random
import statistics


def comparisons(seed, blocks, mode):
    rng = random.Random(seed)
    passive_a, passive_b, paired = [], [], []
    censored_a = censored_b = 0
    for _ in range(blocks):
        common = rng.uniform(0, 40)
        for i in range(16):
            release = (80 if i < 8 else 0) if mode != "iid" else rng.expovariate(1 / 40)
            a = common + release + 20 + rng.gauss(0, 12)
            b = common + release + 30 + rng.gauss(0, 12)
            to_a = i < 8 if mode == "fixed_skew" else rng.random() < .5
            if mode == "unequal_opportunity":
                to_a = rng.random() < .125
            cutoff = common + 65 if mode == "censored" else math.inf
            if to_a:
                if a <= cutoff:
                    passive_a.append(a - common)
                else:
                    censored_a += 1
            else:
                if b <= cutoff:
                    passive_b.append(b - common)
                else:
                    censored_b += 1
            # One randomly selected paired part per block; never wait past completion.
            if i == 0:
                release_pair = rng.choice((0, 80)) if mode != "iid" else rng.expovariate(1 / 40)
                ta = common + release_pair + 20 + rng.gauss(0, 12)
                tb = common + release_pair + 30 + rng.gauss(0, 12)
                if ta + 1 <= min(tb, cutoff):
                    paired.append(1)
                elif tb + 1 <= min(ta, cutoff):
                    paired.append(-1)
    # A has lower expected delay for a uniformly selected part in every mode.
    passive_wrong = (not passive_a or not passive_b or
                     statistics.mean(passive_a) >= statistics.mean(passive_b))
    count_wrong = len(passive_a) <= len(passive_b)
    paired_wrong = bool(paired) and sum(paired) <= 0
    return dict(passive_wrong=passive_wrong, count_wrong=count_wrong, paired_wrong=paired_wrong,
                paired_inconclusive=not paired,
                censored=censored_a + censored_b, observations=len(passive_a) + len(passive_b))


def experiment(seeds=200):
    rows = []
    for mode in ("iid", "fixed_skew", "randomized_skew", "unequal_opportunity", "censored"):
        for blocks in (4, 16, 64):
            samples = [comparisons(seed, blocks, mode) for seed in range(seeds)]
            rows.append(dict(mode=mode, blocks=blocks, seeds=seeds,
                             **{key: statistics.mean(x[key] for x in samples) for key in samples[0]}))
    return rows


def parity_bounds():
    rows = []
    for k in (8, 32, 128):
        for ratio in (.125, .25, .5, 1.0):
            n = k + math.ceil(k * ratio)
            for peers in (2, 3, 4, 5, 8):
                for extra in (0, .125, .25, .5, 1.0):
                    m = min(n, k + math.ceil(k * extra))
                    # With one copy/index, balanced allocation minimizes maximum peer share.
                    exclusive_max = math.ceil(m / peers)
                    rows.append(dict(k=k, n=n, subscribed=m, peers=peers,
                                     tolerate_one_without_duplicates=m - exclusive_max >= k,
                                     lost_parts=exclusive_max,
                                     minimum_surviving=m - exclusive_max,
                                     # Necessary aggregate lower bound with arbitrary duplication.
                                     copies_lower_bound=max(m, math.ceil(peers * k / (peers - 1)))))
    return rows
