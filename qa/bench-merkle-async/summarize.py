"""Summarize run.sh output: summarize.py <merkle.csv> <phase.csv>."""
import sys
from collections import defaultdict
from statistics import median


def rows(path, kind):
    with open(path) as f:
        for line in f:
            parts = line.strip().split(",")
            if parts[1] == kind:
                yield parts[0], parts[2:]


def fmt(vals, scale=1.0):
    vals = sorted(v / scale for v in vals)
    return f"{median(vals):.1f} [{vals[0]:.1f}-{vals[-1]:.1f}] n={len(vals)}"


def main(argv=None):
    merkle_path, phase_path = (argv or sys.argv[1:])[:2]
    # MERKLE,arm,count,median_ns,min,max,inner,root
    times, roots = defaultdict(list), defaultdict(set)
    for _, (arm, count, med, *_rest, root) in rows(merkle_path, "MERKLE"):
        times[(int(count), arm)].append(float(med))
        roots[int(count)].add(root)
    assert all(len(r) == 1 for r in roots.values()), "Merkle roots differ between arms"
    print("## Merkle root from precomputed hashes, us per call: median of per-round medians [range] n=rounds")
    print("| leaves | A | B | C | B/A | C/B |")
    print("|---|---|---|---|---|---|")
    for count in sorted(roots):
        m = {a: median(times[(count, a)]) for a in "ABC"}
        print(f"| {count} | " + " | ".join(fmt(times[(count, a)], 1000) for a in "ABC")
              + f" | {m['B']/m['A']:.2f} | {m['C']/m['B']:.2f} |")

    verify = defaultdict(set)
    for _, (arm, case, *rest) in rows(phase_path, "VERIFY"):
        verify[case].add(",".join(rest))
    print("\n## Output and verdict equivalence across all arms and rounds")
    for case, recs in verify.items():
        status = "IDENTICAL" if len(recs) == 1 else f"DIFFERENT ({len(recs)})"
        print(f"- {case}: {status}: {next(iter(recs)) if len(recs) == 1 else recs}")

    bd = defaultdict(lambda: defaultdict(list))
    for _, (arm, case, *kv) in rows(phase_path, "BREAKDOWN"):
        for item in kv:
            k, v = item.split("=")
            bd[(case, arm)][k].append(float(v))
    print("\n## CPU breakdown on a plain thread, us (median of round medians)")
    print("| case | arm | txids | merkle | auth digests |")
    print("|---|---|---|---|---|")
    for (case, arm), d in bd.items():
        print(f"| {case} | {arm} | " + " | ".join(f"{median(d[k]):.0f}" for k in ("txids_us", "merkle_us", "auth_digests_us")) + " |")

    # PHASE,arm,mode,case,med,lo,hi,gap_med,gap_max,ticks_med
    ph = defaultdict(lambda: defaultdict(list))
    for _, (arm, mode, case, med, lo, hi, gmed, gmax, ticks) in rows(phase_path, "PHASE"):
        d = ph[(mode, case, arm)]
        d["lat"].append(float(med)); d["gap"].append(float(gmed)); d["gapmax"].append(float(gmax)); d["ticks"].append(float(ticks))
    for mode in ("quiet", "busy_rayon_synthetic"):
        print(f"\n## Hashing phase, {mode}: latency ms, median [range of round medians]; ticker max gap ms (median, worst); ticker wakeups during phase")
        print("| case | arm | latency ms | ticker gap ms | worst gap ms | wakeups |")
        print("|---|---|---|---|---|---|")
        for (m, case, arm), d in ph.items():
            if m != mode:
                continue
            print(f"| {case} | {arm} | {fmt(d['lat'], 1000)} | {median(d['gap'])/1000:.1f} | {max(d['gapmax'])/1000:.1f} | {median(d['ticks']):.0f} |")


if __name__ == "__main__":
    main()
