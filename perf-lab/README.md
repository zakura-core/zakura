# perf-lab

Tooling for the agentic sync-perf loop. Design + decisions:
`docs/superpowers/2026-07-20-agentic-perf-workflow-design.md`.
Operating playbook: `.agents/skills/perf-lab/SKILL.md` (invoke the `perf-lab`
skill to start/resume a session).

Start a session: invoke the `perf-lab` skill ("run the perf lab").

- `config.env` — every knob. `NOISE_BAND_PCT` (8.7% as of 2026-07-21) comes
  from A/A calibration; `BENCH_STOP_HEIGHT` fixes the standard 120k window.
- `droplet.sh` — `provision|ip|ssh|destroy|reap|list`. Only touches DO
  resources named `perf-lab-*` AND tagged `zakura-perf-lab`.
- `bench.sh` — `start|status|collect` one A/B bench on the droplet; refs are
  SHA-pinned at start.
- `gates.sh` — local L0 gates and the mock-blocksync L1 pre-filter.
- `verdict.py` — bench artifacts → verdict JSON (refuses unequal coverage).
- `BACKLOG.md` — ranked experiment queue with exclusions; `LEDGER.md` — the
  append-only record and sole reporting channel; `REPORT.md` — regenerated
  morning summary; `state.json` — crash-recovery state (see SKILL protocol).
- Artifacts land in `~/zakura-perf-lab/runs/<label>/`.

Cost: one c-16 droplet ≈ $0.5/h; a 12 h session ≈ $6. Every create/destroy is
recorded in LEDGER.md.

## Measured timings

- Droplet smoke (2026-07-21): provision→ready **77 s** from golden image
  `zakura-pr-node-20260720-2311` (c-16, nyc3) — warm-cache symlinks verified,
  prebuilt release `zakurad` present, 186 GiB free (bench floor is 45),
  destroy+list-empty clean.
- A/A noise band: clean pinned samples 0.401 / 8.383 / 8.653 % at the 120k
  window → NOISE_BAND_PCT 8.7 (live-peer delivery variance; see B-15).
- Bench leg: ~20-22 min at ~90-100 blk/s; snapshot download ~10 min once per
  droplet; collect pulls multi-GB traces (use 600 s timeouts).
- Mac mock-blocksync baseline (M4 Max, 3 runs, 2026-07-21):
  - run 1: throughput: 17517.87 blocks/sec, 43.58 MiB/sec, elapsed=5.708s
  - run 2: throughput: 24828.31 blocks/sec, 61.76 MiB/sec, elapsed=4.028s
  - run 3: throughput: 25199.50 blocks/sec, 62.69 MiB/sec, elapsed=3.968s
