# perf-lab report

Last regenerated: 2026-07-22, SESSION 1 close (halted per rule).

- Regime: single-peer feed, 120k window, band 8.7%, single-run threshold
  17.4%, sweep bar 26.1%. Campaign baseline 85.37 pc blk/s (SHA 847f6085;
  re-pin on drift).
- Confirmed wins: none. Promising: none.
- Evidence produced: DL_LIMIT flat across 50/150/400 (concurrency is not the
  lever; floor-body HOL gates the window); seeder mode narrows worst-case
  noise to 5.8% but fails the ≤3% adoption bar; absolute throughput swings
  ~10% with network phase and ~15 commits of main drift — within-run
  A/B is the only trustworthy comparison, which the tooling enforces.
- DECISION WAITING (Adam): B-15 frozen-cohort port — needs MAX_DROPLETS 2→3
  (2 seeded-then-frozen serving droplets + 1 bench) and ~$0.5/h per frozen
  server while active. This is the unlock for tight thresholds (expected
  ≤1% band per the cohort design's own rationale).
- PROPOSAL queue: blocks-per-response / request-shape experiments (blocked on
  PRs 166/217); B-13 per-side knobs; B-14 upstream PR (local patch active).
- Incidents this session: provision heredoc escape bug (fixed, orphan
  self-clean validated live); none after.
- Spend: SESSION 0+1 total ≈ US$8.
- Resume: say "run the perf lab" after any unlock (B-15 approval, PRs 166/217
  landing, or choosing a heavier-blocks window campaign).
