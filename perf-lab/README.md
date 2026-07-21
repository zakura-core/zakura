# perf-lab

Tooling for the agentic sync-perf loop. Design + decisions:
`docs/superpowers/2026-07-20-agentic-perf-workflow-design.md`.
Operating playbook: `.agents/skills/perf-lab/SKILL.md` (invoke the `perf-lab`
skill to start/resume a session).

- `config.env` — every knob. `NOISE_BAND_PCT` is written by A/A calibration.
- `droplet.sh` — `provision|ip|ssh|destroy|reap|list`. Only touches DO
  resources named `perf-lab-*` AND tagged `zakura-perf-lab`.
- `bench.sh` — `start|status|collect` one A/B bench on the droplet.
- `gates.sh` — local L0 gates and the mock-blocksync L1 pre-filter.
- `verdict.py` — bench artifacts → verdict JSON.
- Artifacts land in `~/zakura-perf-lab/runs/<label>/`.

Cost: one c-16 droplet ≈ $0.5/h; a 12 h session ≈ $6. Every create/destroy is
recorded in LEDGER.md.

## Measured timings

- Droplet smoke (2026-07-21): provision→ready **77 s** from golden image
  `zakura-pr-node-20260720-2311` (c-16, nyc3) — warm-cache symlinks verified,
  prebuilt release `zakurad` present, 186 GiB free (bench floor is 45),
  destroy+list-empty clean.
- Mac mock-blocksync baseline (M4 Max, 3 runs, 2026-07-21):
  - run 1: throughput: 17517.87 blocks/sec, 43.58 MiB/sec, elapsed=5.708s
  - run 2: throughput: 24828.31 blocks/sec, 61.76 MiB/sec, elapsed=4.028s
  - run 3: throughput: 25199.50 blocks/sec, 62.69 MiB/sec, elapsed=3.968s
