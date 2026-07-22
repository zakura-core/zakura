# perf-lab ledger

Append-only. One `## EXP-NNN` entry per experiment; one `## SESSION` header
per orchestrator session; one `## BATCH` summary every BATCH_SIZE bench runs.
This file is the sole reporting channel (design D5). Entry template:

    ## EXP-NNN <slug>
    - date / session: ...
    - backlog id / hypothesis: ...
    - risk class: green|yellow|red-proposal
    - branch: adam/perf-exp/NNN-<slug>   patch: ~/zakura-perf-lab/runs/<label>/exp.patch
    - diff summary: 2–4 lines
    - gates: L0 pass|fail, L1 <numbers or n/a>
    - bench: label(s), droplet, baseline vs candidate post-commit blk/s,
      delta %, noise band %, threshold %
    - verdict: WIN | PROMISING | NEUTRAL | LOSS | BROKEN | PROPOSAL
    - attribution: dominant bottleneck class from verdict-*.json
    - simplicity: 1–5 (1 = config constant, 5 = pipeline restructure)
    - follow-ups: ...

## SESSION 0 — calibration (2026-07-21)

- origin/main during calibration: `ea979e11a` → `4784aca68` (pinned SHA for all clean runs)
- droplet: perf-lab-cal (c-16, nyc3, golden image zakura-pr-node-20260720-2311, ip 134.209.44.208); provision→ready 77 s
- runs (bench collects: 7 incl. exp000, recorded below):
  - aa1 SHORT window (30k blocks, ~5 min/leg): |delta| 13.0% — window too short; download variance dominates. Superseded.
  - aa2 LONG window, UNPINNED refs: origin/main moved mid-run so the legs built different commits; leftover ~116 GB/leg forks filled the 200 GB disk to 0, stalling RocksDB into the 2000 s wall cap and crashing the harness's trace zip. Post-mortem produced: SHA pinning, per-start fork cleanup, post-collect remote purge, 600 s collect timeouts.
  - aa3 clean pinned: |delta| 0.401% (legs 1295/1291 s)
  - aa4 clean pinned: |delta| 8.383% (legs 1321/1221 s; both download-HOL, reorder buffer 540 vs 365 MB — feed-peer delivery variance)
  - aa5 EXCLUDED: primary leg ran through a ≤1 GB disk squeeze and wall-capped at 83k/120k blocks (bogus 56% delta). Led to the verdict.py coverage guard (unequal block ranges now exit 2) and the B-14 harness patch (baseline fork auto-freed after its summary row; validated live on aa6 with 63 GB free mid-run, no manual rescue).
  - aa6 clean pinned, B-14-patched: |delta| 8.653% (legs 1200/1311 s)
- **NOISE_BAND_PCT = 8.7** (max of clean samples, rounded up). Effective single-run threshold = max(3%, 2×8.7) = 17.4%. Single-run verdicts below that are noise-indistinguishable; confirmation runs and multi-run medians are mandatory, and B-15 (multi-peer pinning or frozen-cohort port) is campaign-1-critical to restore sensitivity.
- Attribution at the standard window (1707210→1827210): download head-of-line dominant; commit single-writer 22-26% busy. State/commit-path experiments will NOT register here — campaign 1 must target the download path, raise the window into heavier blocks, or land B-15 first.
- Timings: snapshot download ~10 min (once per droplet; one transient HTTP/2 mid-stream failure observed — retry succeeded); featured build ~3-20 min (golden cargo cache, features differ from bake); leg ~20-22 min at ~90-100 blk/s.
- Droplets: perf-lab-smoke and perf-lab-cal both created and destroyed this session (list-empty verified).
- Cost so far: ~US$4-5 droplet time. perf-lab skill registration verified (appears in session skill lists).

## EXP-000 noop-dry-run
- date / session: 2026-07-21 / SESSION 0
- backlog id / hypothesis: n/a — a comment-only change must traverse the whole
  state machine and come out NEUTRAL
- risk class: green
- branch: adam/perf-exp/000-noop-dry-run (deleted after verdict; patch:
  ~/zakura-perf-lab/runs/exp000/exp.patch)
- diff summary: 2-line comment marker in zakura-utils/src/lib.rs
- gates: L0 PASS (zakura-utils; fmt+clippy+nextest with --no-tests=pass)
- bench: exp000 on perf-lab-cal; baseline 96.15 vs candidate 90.36 post-commit
  blk/s; delta -6.0%; band 8.7%; effective threshold 17.4%
- verdict: NEUTRAL — correct. The -6% is another live-peer noise sample inside
  the band; the naive 3% floor would have false-flagged a LOSS, so the
  calibration already paid for itself on the first verdict.
- attribution: download-HOL both legs (consistent with SESSION 0)
- simplicity: 1
- follow-ups: none — pipeline (worktree → branch → gates → push → pinned bench
  → verdict → ledger → cleanup) validated end to end

## SESSION 1 (2026-07-21, started ~23:55Z)

- origin/main: b3a2ad506 (moved from 4784aca68 since calibration — ~10 commits)
- band on entry: 8.7% (measured today, golden image 20260720-2311); gate
  re-checks if provision picks a newer image
- exclusions refreshed: PRs 336/330/322/323/341 added (network/sync areas)
- plan: (1) B-15 measurement first — two seeder-mode A/A runs (FEED_PEER
  empty, PEERSET_SIZE=75) to test whether multi-peer delivery averaging
  shrinks the ~9% single-peer variance; if yes, adopt seeder mode as the
  standard window config and re-derive the band + campaign baseline from
  those runs; (2) meanwhile implement EXP-001 (download-path code experiment
  scoped away from PR-active files) behind the L1 mock-blocksync pre-filter;
  (3) B-02 DL_LIMIT sweep once the band question settles. Batch note: 1
  collect remains in batch 1; the first seeder run closes it.

### SESSION 1 scouting notes (pre-EXP-001)

- `max_blocks_per_response` default 1 is DELIBERATE (doc comment: narrow
  ranges keep a missing response from stalling multiple floor heights) — and
  `block_sync/config.rs` is touched by open PRs 166/217, so code-default
  experiments there are excluded for now. Filed as a PROPOSAL candidate
  pending those PRs.
- L1 mock-blocksync knob probe (base vs ZAKURA_MOCK_BS_MAX_BLOCKS_PER_RESPONSE=32,
  medians of runs 2-4): 25196 vs 25455 blk/s = +1.0%, within warm noise.
  Domain-validity lesson: loopback can't model the WAN per-request latency
  this knob trades against — the mockbs pre-filter is valid for
  codec/scheduling costs only; batching questions need L2.
