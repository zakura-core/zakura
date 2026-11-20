#!/usr/bin/env python3
"""LAB ONLY: summarize lab CSVs.

    analyze.py exp1 results/grid.csv       claim age against the deadline, per cell
    analyze.py margin results/grid.csv     p90 age minus deadline, rate x RTT, one table per size
    analyze.py race results/exp2.csv       win rate with a Wilson 95% interval, valid trials only
    analyze.py invalid results/exp2.csv    why trials were invalid, per cell
    analyze.py multihop results/hop.csv    how often relays split, per cell
    analyze.py hops results/hop.csv        per relay: A's claim age and outcome, and its final tip

Rows written before the `valid` column existed get their validity inferred: a race is valid when
the observer resolved B and ended on A or B, a delivery when the observer resolved A.
"""
import csv
import math
import sys
from collections import Counter, defaultdict


def quantile(values, q):
    values = sorted(values)
    if not values:
        return float("nan")
    return values[min(len(values) - 1, int(q * len(values)))]


def wilson(wins, n, z=1.96):
    """95% Wilson score interval for wins out of n."""
    if not n:
        return float("nan"), float("nan")
    p = wins / n
    denominator = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denominator
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominator
    return max(0.0, centre - half), min(1.0, centre + half)


def rows_of(path):
    """Rows of a CSV that parallel writers may have given several header lines."""
    lines = [line for line in csv.reader(open(path)) if line]
    header = next(line for line in lines if line[0] == "exp")
    return [dict(zip(header, line)) for line in lines if line != header]


def race_valid(row):
    if row.get("valid", "") != "":
        return row["valid"] == "1", row.get("invalid_reason", "")
    if row.get("b_outcome") in ("timeout", "", None):
        return False, "b_not_seen"
    if row.get("winner") not in ("a", "b"):
        return False, "tip_other"
    return True, ""


def delivery_valid(row):
    if row.get("valid", "") != "":
        return row["valid"] == "1", row.get("invalid_reason", "")
    return row.get("outcome") not in ("timeout", "", None), "a_not_seen"


def ages_of(rows, column):
    return [float(r[column]) for r in rows if r.get(column) not in ("", "-1", None)]


def slow_share(ages):
    """Ages at least 1.5 times the cell's fastest: a second mode, such as a body fetched twice."""
    if not ages:
        return "-"
    fastest = min(ages)
    return f"{sum(age >= 1.5 * fastest for age in ages)}/{len(ages)}"


def delivery_cells(path):
    cells = defaultdict(list)
    for row in rows_of(path):
        key = (row["stack"], float(row["rtt_ms"]), float(row["rate_mbit"]),
               row.get("cold", "0") or "0", int(row["size"]))
        cells[key].append(row)
    return cells


def exp1(path):
    print(f"{'stack':14}{'rtt':>5}{'rate':>6}{'cold':>5}{'size':>9}{'valid':>8}{'p50':>7}"
          f"{'p90':>7}{'max':>7}{'deadline':>9}{'margin':>8}{'expired':>9}{'slow':>7}"
          f"{'hdr lag':>8}")
    cells = delivery_cells(path)
    for key in sorted(cells):
        rows = [r for r in cells[key] if delivery_valid(r)[0]]
        stack, rtt, rate, cold, size = key
        ages = ages_of(rows, "age_ms")
        expired = sum(r["outcome"] == "expired" for r in rows)
        deadline = max((int(r["window_ms"]) for r in rows if r.get("window_ms")), default=-1)
        lags = [float(r["submit_to_claim_ms"]) for r in rows if r.get("submit_to_claim_ms")]
        margin = quantile(ages, .9) - deadline if ages and deadline >= 0 else float("nan")
        print(f"{stack:14}{rtt:5.0f}{rate:6.0f}{cold:>5}{size:9d}"
              f"{len(rows):4d}/{len(cells[key]):<3d}"
              f"{quantile(ages, .5):7.0f}{quantile(ages, .9):7.0f}{max(ages, default=0):7.0f}"
              f"{deadline:9d}{margin:8.0f}{expired:6d}/{len(rows):<3d}{slow_share(ages):>6}"
              f"{quantile(lags, .5):8.0f}")


def margin(path):
    """p90 claim age minus the deadline, in ms. Positive means honest blocks of that size lose
    their claim at least one time in ten."""
    cells = delivery_cells(path)
    for stack, cold, size in sorted({(k[0], k[3], k[4]) for k in cells}):
        rtts = sorted({k[1] for k in cells if (k[0], k[3], k[4]) == (stack, cold, size)})
        rates = sorted({k[2] for k in cells if (k[0], k[3], k[4]) == (stack, cold, size)})
        print(f"\n{stack} cold={cold} size={size}: p90 age - deadline (ms), n valid")
        print(f"{'rtt \\ rate':>11}" + "".join(f"{rate:>14.0f}" for rate in rates))
        for rtt in rtts:
            line = f"{rtt:11.0f}"
            for rate in rates:
                rows = [r for r in cells.get((stack, rtt, rate, cold, size), [])
                        if delivery_valid(r)[0]]
                ages = ages_of(rows, "age_ms")
                deadline = max((int(r["window_ms"]) for r in rows if r.get("window_ms")),
                               default=-1)
                if not ages or deadline < 0:
                    line += f"{'-':>14}"
                else:
                    line += f"{quantile(ages, .9) - deadline:+9.0f} n={len(ages):<2d}"
            print(line)


def race_cells(path):
    cells = defaultdict(list)
    for row in rows_of(path):
        key = (row["stack"], float(row["rtt_ms"]), float(row["rate_mbit"]), int(row["window_ms"]),
               int(row.get("cap_ms") or -1), int(row["body_delay_ms"]), int(row["size"]),
               int(row["offset_ms"]))
        cells[key].append(row)
    return cells


def race(path):
    print(f"{'stack':14}{'rtt':>5}{'rate':>6}{'window':>7}{'cap':>6}{'delay':>6}{'size':>9}"
          f"{'offset':>7}"
          f"{'valid':>8}{'A wins':>8}{'95% CI':>13}{'A age p50':>10}{'p90':>6}{'slow':>7}"
          f"{'O moved p50':>12}")
    cells = race_cells(path)
    for key in sorted(cells):
        rows = [r for r in cells[key] if race_valid(r)[0]]
        wins = sum(r["winner"] == "a" for r in rows)
        low, high = wilson(wins, len(rows))
        ages = ages_of(rows, "a_age_ms")
        moved = [float(r["o_moved_ms"]) for r in rows if r.get("o_moved_ms")]
        stack, rtt, rate, window, cap, delay, size, offset = key
        print(f"{stack:14}{rtt:5.0f}{rate:6.0f}{window:7d}{cap:6d}{delay:6d}{size:9d}{offset:7d}"
              f"{len(rows):4d}/{len(cells[key]):<3d}{wins:5d}/{len(rows):<3d}"
              f"  {low:4.2f}-{high:4.2f}{quantile(ages, .5):10.0f}{quantile(ages, .9):6.0f}"
              f"{slow_share(ages):>7}{quantile(moved, .5):12.0f}")


def invalid(path):
    cells = race_cells(path) if "offset_ms" in rows_of(path)[0] else delivery_cells(path)
    check = race_valid if "offset_ms" in rows_of(path)[0] else delivery_valid
    total = Counter()
    for key in sorted(cells):
        reasons = Counter(check(r)[1] or "valid" for r in cells[key])
        total.update(reasons)
        print(key, dict(reasons))
    print("all", dict(total))


def multihop(path):
    cells = defaultdict(list)
    for row in rows_of(path):
        key = (row["topology"], row["rule_off"], int(row.get("cap_ms") or -1),
               float(row["rtt_ms"]), float(row["rate_mbit"]), int(row["size"]),
               int(row["offset_ms"]))
        cells[key].append(row)
    print(f"{'topology':10}{'rule off':>12}{'cap':>6}{'rtt':>5}{'rate':>6}{'size':>9}{'offset':>7}"
          f"{'valid':>8}{'split':>8}{'95% CI':>13}{'A majority':>11}")
    for key in sorted(cells):
        rows = [r for r in cells[key] if r.get("valid") == "1"]
        splits = sum(int(r["split"]) > 1 for r in rows)
        low, high = wilson(splits, len(rows))
        majority = sum(r["majority"] == "a" for r in rows)
        topology, rule_off, cap, rtt, rate, size, offset = key
        print(f"{topology:10}{rule_off or '-':>12}{cap:6d}{rtt:5.0f}{rate:6.0f}{size:9d}{offset:7d}"
              f"{len(rows):4d}/{len(cells[key]):<3d}{splits:4d}/{len(rows):<3d}"
              f"  {low:4.2f}-{high:4.2f}{majority:7d}/{len(rows):<3d}")


def hops(path):
    """Per cell and relay: how A's claim resolved, its age, and how often the relay ended on A."""
    cells = defaultdict(list)
    for row in rows_of(path):
        key = (row["topology"], row["rule_off"] or "-", int(row.get("cap_ms") or -1),
               float(row["rtt_ms"]), float(row["rate_mbit"]), int(row["size"]),
               int(row["offset_ms"]))
        cells[key].append(row)
    for key in sorted(cells):
        rows = [r for r in cells[key] if r.get("valid") == "1"]
        relays = sorted({c[:-2] for c in rows[0] if c.endswith("_a") and c[:-2] not in "ab"}
                        ) if rows else []
        splits = sum(int(r["split"]) > 1 for r in rows)
        low, high = wilson(splits, len(rows))
        print(f"\n{key}  valid {len(rows)}/{len(cells[key])}  split {splits}/{len(rows)}"
              f" ({low:.2f}-{high:.2f})")
        for relay in relays:
            outcomes = Counter(r[f"{relay}_a"].split(":")[0] or "-" for r in rows)
            ages = [float(r[f"{relay}_a"].split(":")[1]) for r in rows
                    if ":" in r[f"{relay}_a"]]
            on_a = sum(r[f"{relay}_tip"] == "a" for r in rows)
            print(f"  {relay:4} A age p50 {quantile(ages, .5):6.0f} p90 {quantile(ages, .9):6.0f}"
                  f"  outcomes {dict(outcomes)}  ends on A {on_a}/{len(rows)}")


if __name__ == "__main__":
    for path in sys.argv[2:]:
        {"exp1": exp1, "margin": margin, "race": race, "invalid": invalid,
         "multihop": multihop, "hops": hops}[sys.argv[1]](path)
