# perf-lab report

Last regenerated: 2026-07-21, after SESSION 0 (calibration only — no real
experiments yet).

- Baseline: ~90-100 post-commit blk/s at the standard 120k window
  (1707210→1827210), single pinned feed peer, c-16 droplet.
- Noise band: 8.7% (clean pinned A/A samples 0.401/8.383/8.653%) → effective
  single-run threshold 17.4%. Sensitivity is limited by live-peer delivery
  variance; B-15 is the path back to tight thresholds.
- Confirmed wins: none yet. Promising: none yet.
- Parked proposals: B-13 (per-side knob overrides), B-14 upstreaming (fork
  cleanup — local patch active), B-15 (peer-variance reduction; campaign-1
  critical).
- Incidents: aa2 disk-fill + unpinned-ref confound (fixed in tooling); aa5
  excluded (disk-squeeze wall cap). Details in LEDGER SESSION 0.
- Spend: ~US$4-5.
- Next: campaign-target memo, then the first experiment batch (download-path
  focus per attribution).
