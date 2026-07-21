#!/usr/bin/env python3
"""Turn a checkpoint-sync-bench summary.md into a machine verdict.

Gating metric (design §4): post-first-commit blocks/s. Verdicts:
WIN_CANDIDATE (delta >= effective threshold; needs a confirmation run to
become WIN), NEUTRAL (within +/- threshold), LOSS (below -threshold).
--aa prints the observed |delta| as the noise sample instead.

Usage:
  verdict.py SUMMARY_MD [--threshold-pct F] [--noise-band-pct F] [--aa]
Exit 0 with JSON on stdout; exit 2 on parse failure (missing rows/columns).
"""
import argparse, json, re, sys


def parse_table(text):
    """Return {role: {col_name: cell}} for the (baseline)/(primary) rows."""
    lines = [l.strip() for l in text.splitlines() if l.strip().startswith("|")]
    if len(lines) < 3:
        sys.exit(2)
    header = [c.strip().lower() for c in lines[0].strip("|").split("|")]
    rows = {}
    for line in lines[2:]:  # skip header + separator
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) != len(header):
            continue
        binary = cells[0]
        m = re.search(r"\((baseline|primary)\)", binary)
        if m:
            rows[m.group(1)] = dict(zip(header, cells))
    return rows


def fnum(row, col):
    try:
        return float(row[col])
    except (KeyError, ValueError):
        print(f"verdict.py: cannot read column '{col}' from row {row}", file=sys.stderr)
        sys.exit(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("summary")
    ap.add_argument("--threshold-pct", type=float, default=3.0)
    ap.add_argument("--noise-band-pct", type=float, default=0.0)
    ap.add_argument("--aa", action="store_true")
    args = ap.parse_args()

    rows = parse_table(open(args.summary).read())
    if "baseline" not in rows or "primary" not in rows:
        print("verdict.py: need both (baseline) and (primary) rows", file=sys.stderr)
        sys.exit(2)

    col = "post-commit blk/s"
    b, p = fnum(rows["baseline"], col), fnum(rows["primary"], col)
    if b <= 0:
        print("verdict.py: baseline gating metric is zero", file=sys.stderr)
        sys.exit(2)
    delta = (p - b) / b * 100.0

    if args.aa:
        print(json.dumps({"mode": "aa", "observed_noise_pct": round(abs(delta), 3),
                          "baseline_pc_bps": b, "primary_pc_bps": p}))
        return

    eff = max(args.threshold_pct, 2.0 * args.noise_band_pct)
    verdict = ("WIN_CANDIDATE" if delta >= eff
               else "LOSS" if delta <= -eff else "NEUTRAL")
    print(json.dumps({
        "mode": "ab", "baseline_pc_bps": b, "primary_pc_bps": p,
        "delta_pct": round(delta, 3), "threshold_pct": args.threshold_pct,
        "noise_band_pct": args.noise_band_pct,
        "effective_threshold_pct": eff, "verdict": verdict,
        "baseline_bps": fnum(rows["baseline"], "blocks/s"),
        "primary_bps": fnum(rows["primary"], "blocks/s"),
    }))


if __name__ == "__main__":
    main()
