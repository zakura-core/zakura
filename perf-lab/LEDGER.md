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

## BATCH 1 summary (2026-07-22)

- collects (8): aa1, aa2, aa3, aa4, aa5, aa6, exp000, aa-seed1 — all
  calibration/validation; wins 0 by design.
- key products: band 8.7% (single-peer), harness fixes (SHA pinning, fork
  hygiene incl. B-14 auto-patch, coverage guard, honest exit codes),
  EXP-000 pipeline validation, first seeder-mode sample 0.435%.
- spend: ~US$5-6 total across two droplets.

## B-15 MEASUREMENT — seeder mode NOT adopted (2026-07-22)

- seeder-mode A/A samples: 0.435% (aa-seed1, SHA b3a2ad506), 5.828%
  (aa-seed2, SHA 847f6085) vs single-peer history 0.401/8.383/8.653%.
- read: multi-peer averaging lowers the observed worst case (5.8 vs 8.7) but
  both live-peer modes drift ≥5% on hour timescales; adoption bar (both ≤3%)
  not met. Seeder absolute throughput also runs ~8% below single-peer
  (~85-88 vs ~93-100 pc blk/s) — numbers across modes are not comparable.
- conclusion: the real B-15 is the frozen-cohort port (two seeded-then-frozen
  serving droplets; byte-identical range every run — the deploy/runner
  design's own rationale). **DECISION NEEDED (Adam):** that needs 3
  concurrent droplets (2 serving + 1 bench), above the MAX_DROPLETS=2 hard
  rule, and frozen servers cost ~$0.5/h each while kept. Until approved, the
  band stays 8.7% (single-peer) and the effective single-run threshold 17.4%.

## CAMPAIGN (2026-07-22, SESSION 1)

- regime: single-peer (167.99.162.47), 120k window, band 8.7%, single-run
  threshold 17.4%, sweep PROMISING bar max(5%, 3×band) = 26.1%.
- attribution: download head-of-line dominant; commit writer 22-26% busy.
  Download-path experiments only; state/commit invisible at this window.
- campaign baseline: STALE — prior single-peer legs are on SHA 4784aca6
  (median 94.90 pc blk/s) but main has drifted twice since. Drift rule: when
  a run's pinned SHA differs from the baseline's, re-pin with one fresh
  default-knob single-peer run before comparing. base1 (next bench) re-pins.
- top-5: (1) B-02 DL_LIMIT sweep 50/400 vs re-pinned baseline (env-only, no
  collisions; even sub-bar deltas are recorded as evidence for when the band
  tightens); (2) B-04 body-commit batch knee (state-file collision check
  first); (3) B-06 verifier batch sizes; (4) B-07 rayon pool sizing;
  (5) B-05 RocksDB read-side. B-01/B-03 knob-default work PROPOSAL-blocked
  on PRs 166/217. B-15-full awaits the droplet-cap decision above.

### base1 re-pin (2026-07-22)

- campaign baseline: **85.37 pc blk/s** = median of base1's legs (86.77 /
  83.97) on SHA 847f6085, single-peer regime. Internal |delta| 3.23% — a
  fourth single-peer noise sample, within band.
- note: the prior-era baseline (94.90 on 4784aca6) is ~10% higher; the gap is
  confounded (≈15 commits of code drift × time-of-day network phase — seeder
  runs in this window also sat ~85-88). All sweep comparisons use base1 only.

### exp001-p50 (DL_LIMIT=50, 2026-07-22)

- legs 85.23 / 83.97 pc blk/s (within-run 1.48% — valid); run level ~84.6 vs
  campaign baseline 85.37 = **-0.9%, flat**. 3× less download concurrency
  changes nothing → concurrency is not the constraint at this window; the
  floor-body HOL (banner: writer 23-24%, reorder ~430-460 MB) gates progress
  regardless of parallelism.

### exp001-p400 (DL_LIMIT=400, 2026-07-22)

- legs 93.60 / 94.27 pc blk/s (within-run 0.72% — valid), SHA 8862004e (main
  drifted from base1's 847f6085 → absolute comparison to 85.37 is confounded;
  the ~94 level matches the faster network phases seen all day). Directional
  evidence only.

## EXP-001 — DL_LIMIT sweep: NEUTRAL with evidence (2026-07-22)

- points: 50 (flat, -0.9% within-SHA vs base1), 150 (the default, every
  baseline run), 400 (drift-confounded, same envelope). Verdict: **download
  concurrency is not the lever** in the checkpoint-zone window — throughput
  is gated by per-peer delivery of the floor body (HOL), unchanged from 50 to
  400 concurrent downloads.
- consequence: the download-path levers that could matter are (a) B-15
  frozen-cohort (also unlocks tight thresholds), (b) request-shape /
  blocks-per-response work — PROPOSAL-blocked on PRs 166/217, (c) a
  heavier-blocks window campaign where commit-side work becomes visible.

## SESSION 1 close — HALTED per rule (2026-07-22, ~05:40Z)

- collects this session: aa-seed1 (closed batch 1), aa-seed2, base1,
  exp001-p50, exp001-p400 (batch 2: 4 of 8 used).
- findings: (1) seeder mode narrows worst-case A/A noise to 5.8% but misses
  the ≤3% adoption bar — B-15-full (frozen cohort) is the real fix and needs
  Adam's droplet-cap decision (3 concurrent, ~$0.5/h per frozen server);
  (2) DL_LIMIT flat 50→400 (EXP-001); (3) main drifted twice mid-session —
  SHA pinning caught both; cross-SHA/phase comparisons confounded by design
  awareness, within-run measurements stayed clean throughout.
- halt rationale: zero runs at/above the 17.4% threshold and no READY item
  clears it under current rails (sweep space measured flat; knob code work
  blocked on PRs 166/217; commit-side invisible at this window; B-15-full
  awaiting decision). Unlocks: B-15 approval OR PRs 166/217 landing OR a
  heavier-window campaign.
- spend: ~US$2.75 this session (perf-lab-s1, ~5.5 h); droplet destroyed at
  close.

## SESSION 2 (2026-07-22) — B-15 cohort port APPROVED by Adam

- decision: MAX_DROPLETS 2→3; frozen-cohort port green-lit (~$0.5/h per
  frozen server accepted). Design doc hard rule amended.
- plan: (1) investigate harness bootstrap-peer/dev_network overridability;
  (2) cohort tooling (seed two serving droplets past 1,835,000, capture
  zakura node ids, freeze onto a private dev_network tag); (3) bench cohort
  mode; (4) cohort A/A pair → new band (target ≤1%) → resume campaign with
  tight thresholds.

### cohort gate + re-seed (2026-07-22)

- freeze-gate review caught two criticals before any state was served: the
  harness EXIT trap deletes the seed's fork on completion (first seed pair
  written off, ~$1; KEEP_CUR_FORK guard patch added, chain-validated), and
  the 24h reaper would have destroyed the frozen servers at the next session
  start (perf-lab-serve-* now code-exempt). Also: freeze height assertion,
  newest-binary pick, serve-crash status, identity_dir wording. Both serve
  droplets re-patched via prepare_remote (self-healing checkout+repatch) and
  re-seeding with KEEP_CUR_FORK=1.

## COHORT STANDING (2026-07-22, ~09:05Z)

- perf-lab-serve-a (138.197.91.62) and perf-lab-serve-b (159.203.134.228)
  frozen and SERVING cohort 'perf-lab-cohort-1' from seeded state (both
  reached 1,836,000; keepfork patch preserved the forks; freeze height
  assertion passed via log fallback — wrapper .exit death struck again,
  fallback now built into freeze). COHORT_PEERS captured and committed.
- reaper-exempt by code; ~$1/h combined while standing (Adam-approved).

### aa-cohort1 (2026-07-22) — first cohort determinism sample

- within-run |delta| **0.00%** on post-commit blk/s (191.39 / 191.39; legs
  645s / 650s; whole-run 186.05 / 184.62 = 0.77%). SHA 2d3d7e51.
- absolute throughput **~2× live-peer** (191 vs 85-100 pc blk/s): frozen
  uncontended servers on the private overlay feed flat-out — the bench now
  stresses the node itself much harder.

### aa-cohort2 + diagnosis (2026-07-22)

- within-run 7.60% (179.64 / 165.98 pc; legs 686/746s), SHA 63b8d4dc. Both
  legs SUPPLY-BOUND with idle servers (loads ~0) and idle writer (33%/27%):
  the slow leg's apply queue starved at 39 vs 401. Constraint = per-connection
  overlay delivery, varying leg-to-leg. Cohort samples now {0.00, 7.60} —
  bimodal; third sample running before the band is set.
- run-to-run 191→~172 across SHAs 2d3d7e51→63b8d4dc: possible mainline
  regression candidate — cross-SHA within-run A/B queued after band settles.
- unlock: `max_blocks_per_response` is a config field — testable via a
  harness write_config patch in our bench configs (no code-default change, no
  PR-166/217 collision). Prime suspect for both the supply ceiling and the
  leg variance (per-request RTT integrates into throughput at 1 blk/response).

## BATCH 2 summary (2026-07-22)

- collects (8): aa-seed2, base1, exp001-p50, exp001-p400, seed-a, seed-b,
  aa-cohort1, aa-cohort2. Wins 0; products: B-15 cohort STANDING with 2×
  throughput, EXP-001 DL_LIMIT-flat evidence, supply-bound attribution.
- spend: ~US$5 (SESSION 1 tail + cohort build incl. one written-off seed
  pair).

## B-15 DELIVERED (partial) — 2026-07-22

- cohort determinism samples: {0.00, 7.60, 0.00} — a bimodal picture: the
  normal mode is PERFECT (two samples with byte-identical leg numbers), with
  occasional per-connection delivery excursions (cohort2's starved-queue leg).
- throughput 2× live peers (~165-191 pc blk/s); attribution now SUPPLY-BOUND
  (writer 27-34%) — per-connection overlay delivery is both ceiling and
  excursion source; request shape (1 blk/response) is the prime suspect and
  EXP-002 tests it via the bsknob config patch.
- thresholds: NOISE_BAND_PCT 8.7→7.6 (max rule); single-run effective
  threshold 15.2%; NEW two-run protocol: confirmed same-direction results
  ≥3% on the cohort qualify as WINs (excursions do not repeat consistently;
  2 of 3 samples show exact-zero noise).
- standing cost: ~$1/h serve pair (reaper-exempt) + bench droplet while
  active.

### exp002-base anomaly (2026-07-22)

- legs 105.26 pc (full window, 1163s) / 18.70 pc (wall-capped at 37k of
  120k) on SHA 30b0c63d — coverage guard refused the comparison. All droplet
  health checks clean (disk/mem/serve nodes). Both legs sit far below the
  63b8d4dc-era ~180-190 norm → suspicion shifts to the code: regcheck-1
  (30b0c63d vs 63b8d4dc, within-run on the cohort) separates code from
  environment. EXP-002 paused pending its verdict.

## FINDING: no mainline regression — the cohort has a cache regime (2026-07-22)

- regcheck-1 (within-run, same cohort): 63b8d4dc = 108.60 pc, 30b0c63d =
  118.34 pc (+9%, inside single-run band). The new main is fine; exp002-base's
  collapse was NOT code.
- BUT both legs ran ~110 vs the ~185-190 both SHAs achieved hours earlier,
  banners back to floor-body HOL with idle writer (20-22%). Health checks
  clean. Hypothesis that fits every run today: SERVE-SIDE PAGE-CACHE REGIME —
  freshly-seeded state serves RAM-warm (~190); idle gaps cool it toward
  disk-read speed (~110); each bench's reads partially re-warm. aa-cohort1/3
  (back-to-back-ish, warm, 0.0%), aa-cohort2 (transition, 7.6%), exp002-base
  (after idle gap, cold/mixed, one leg collapsed), regcheck (cold-steady
  ~110 both legs).
- discriminator running: aa-cohort4 started immediately after regcheck —
  warm-regime prediction ~150+; cold-steady prediction ~110. Either outcome
  fixes the protocol (warmup pass before measuring, or re-baseline at the
  cold-steady level).

## COHORT CACHE REGIME — resolved (2026-07-22)

- aa-cohort4 (back-to-back after regcheck): legs 105.26 / 105.17 pc, |delta|
  0.086%, durations 1163/1164 s. COLD-STEADY CONFIRMED at ~105 pc blk/s.
- full picture: ~185-190 was the post-seed RAM-warm transient (state >> RAM,
  reads evict as they load, so ordinary benching cannot re-warm it); the
  steady disk-read regime is ~105 and deterministic to ~0.1% in cadence.
  Every anomaly today maps onto the warm→cold transition.
- PROTOCOL: measure in cadence (back-to-back runs; after >30 min cohort idle,
  run one settling run before measuring). Two-run confirmation stays. Steady
  regime still ~15% above live-peer throughput with vastly better precision.
- EXP-002 restart: aa-cohort4 doubles as the default-knob baseline on SHA
  41368ad4 — only k8/k32 needed, back-to-back, now.

### exp002-k8 (2026-07-22) — WIN-candidate

- k=8 legs: 114.83 / 110.09 pc (1078/1113 s) vs k=1 baseline legs 105.26 /
  105.17 (aa-cohort4, same SHA 41368ad4, same cadence). Worst-vs-best margin
  +4.7%; means +6.9%. Both k8 legs ≥3% above both baseline legs → the
  two-leg WIN test passes. k8's own leg spread (4.1%) is wider than the
  steady 0.1% — batching may chunk delivery; noted, does not overturn the
  worst-leg margin.

## EXP-002 — WIN (pending one confirmation) : block-request batching (2026-07-22)

- dose-response on SHA 41368ad4, frozen cohort, steady regime, back-to-back:
  | k (max_blocks_per_response) | legs (pc blk/s) | mean | spread |
  | 1 (default)                 | 105.26 / 105.17 | 105.2 | 0.1% |
  | 8                           | 114.83 / 110.09 | 112.5 | 4.1% |
  | 32                          |  99.59 / 104.71 | 102.2 | 5.0% |
- k=8 passes the two-leg test against BOTH neighbors (its worst leg beats
  k=1's best by +4.7% and k=32's best by +5.1%). Inverted-U shape matches the
  HOL trade documented at zakura-network/src/zakura/block_sync/config.rs:3-7
  — the caution is right, the calibration is not: the optimum is near 8, not 1.
- verdict: WIN pending one confirmation run (exp002-k8-c, queued as the next
  session's first work per the two-run protocol).
- PROPOSAL (for Adam): raise DEFAULT_BS_BLOCKS_PER_RESPONSE 1→8 once PRs
  166/217 release block_sync/config.rs. Caveats to weigh: evidence is
  cohort-mode/steady-regime (clean links, 2 peers); public-network loss/HOL
  behavior at k=8 deserves a live-peer comparison run and/or e2e long-mode
  before the default changes. perf-lab adopts k=8 as a standing comparison
  point in its own bench configs meanwhile.
- also settled: EXP-002's earlier anomaly was the cache transition, not code;
  B-03's request-shape question is now answered with data (backlog updated).

## SESSION 2 close (2026-07-22, ~23:50Z)

- collects: 14 across the session (cohort build 2 seeds + 1 written-off pair,
  4 cohort A/As, exp002-base + regcheck + k8 + k32, plus batch-2 carryover).
- findings: B-15 cohort delivered; cache regime decoded into protocol; no
  mainline regression; EXP-002 batching WIN pending confirmation.
- spend ~US$9 today. perf-lab-s2 destroyed; frozen serve pair standing.
- next session queue: exp002-k8-c confirmation → formal WIN; then B-04 (state
  collision check first) / B-06 / B-07 under the steady protocol.

## SESSION 3 (2026-07-22, ~23:58Z)

- origin/main: c3b26d24e (drifted again); serves standing ~8h; band 7.6
  (steady protocol); batch 3 at 5 of 8.
- plan: settling run (doubles as k=1 cadence baseline, pinned to current
  main) → exp002-k8-c confirmation back-to-back → formal WIN if the two-leg
  test passes again → then B-04 (collision-check PR 390 first) or B-06/B-07.
- PRs 166/217 still open: batching default stays PROPOSAL.

### settle1 incident + B-16 applied (2026-07-23)

- first settle1 attempt died at ~50 min on the third HTTP/2 mid-stream
  download failure in two days; two-strikes escalation: B-16 patch built
  (curl --http1.1 + primary-source double-try, baked into prepare_remote as
  the fourth marker-guarded patch) and applied live to s3; settle1 restarted
  under it.

### settle1 second failure — root cause found, resumable-fetch rescue (2026-07-23)

- settle1 retry FAILED the download again even under B-16 (both primary
  attempts died mid-stream; second at 11.5G with curl 18 "transfer closed").
  So the protocol version was never the root cause.
- Diagnosis: origin file is INTACT (HEAD Content-Length 32104948084, range
  probe at the 32.1G tail returns 206 with data; Cloudflare-fronted). The
  failure class is long-single-stream deaths through the CDN path — any
  one-shot streaming download loses everything on a mid-stream cut.
- Rescue (one bounded attempt before the halt rule fires): resumable fetch
  to a file on the droplet (`curl -C -` retry loop, 90 tries, progress is
  monotonic), sha256-verify against the harness's pin, manual extract into
  /opt/zakura-bench/master-1707210 — the harness then skips its own download
  forever on this droplet. Disk fits (182G free; ~30G tarball + ~40G master).
- If this also fails: halt the droplet lane for the night per the
  twice-in-a-row rule. NOTE FOR ADAM either way: the snapshot origin
  (zakura.valargroup.dev via Cloudflare) cannot reliably serve a ~32G single
  stream tonight — 4 consecutive mid-stream deaths across two protocols.
  Worth either a chunk-friendly mirror or accepting resume-style fetch as
  the default (harness patch B-16 v2, planned).

### Rescue succeeded — settle1 relaunched (2026-07-23 ~03:10Z)

- resumable fetch completed (single unbroken stream this time — the CDN
  cutting is intermittent), sha256 matched the harness pin, manual extract
  verified against the harness's own presence check
  (state/v28/mainnet/version). Master cached on-droplet; no more downloads
  needed for perf-lab-s3's lifetime.
- settle1 relaunched (build + two k=1 legs only, ~45 min). Chain unchanged:
  settle1 → exp002-k8-c confirmation → two-leg WIN test.

### B-16 v2 baked (2026-07-23 ~03:25Z)

- prepare_remote's B-16 patch upgraded from protocol-tweak to resume-to-file
  fetch (the mechanism that actually rescued tonight): curl -C - retry loop
  (30 tries, size-verified against Content-Length), tarball streams into the
  unchanged sha256/zstd/tar tail and is deleted as it streams; free-space
  gate 45→75GiB for the transient. Verified offline against pristine
  origin/main (anchors, syntax, idempotency, gate message intact). NOT
  applied to s3 mid-run (rewriting a running script corrupts it; s3's master
  is cached so it never downloads again). Future droplets get it at provision.

### settle1 landed cold — second settling pass (2026-07-23 ~04:15Z)

- settle1 (k=1, both legs c3b26d24): 84.45 -> 94.86 post-commit blk/s,
  ascending and 12% apart — the cohort warm-up curve after the serves idled
  through the download outage, not a regression (same SHA measured 105.26/
  105.17 yesterday once settled). Per the steady protocol the k=8
  confirmation must wait for two agreeing k=1 legs.
- settle2 started back-to-back, same pinned SHA (binary cached, no build).
  Batch 3: settle1 = 6 of 8, settle2 will be 7, exp002-k8-c 8 — boundary
  lands exactly on the confirmation.

### settle2 — plateau at a lower level; proceeding to confirmation (2026-07-23 ~05:20Z)

- settle2 (k=1, c3b26d24): 91.53 / 94.79. Four k=1 legs tonight: 84.45,
  94.86, 91.53, 94.79 — plateaued ~93-95 with ~±2% wobble, i.e. tonight's
  steady level sits ~10% below yesterday's ~105 on the fresh s3 droplet
  (droplet-level shift; same SHA both days, so not a code change). The
  two-leg WIN test is relative within droplet+cadence, so this only moves
  the bar, not the validity.
- exp002-k8-c started in cadence (same SHA, k=8). Confirmation bar: BOTH
  k8 legs >= 3% above the highest plateau k=1 leg (94.86) -> both >= 97.7.
  Expected if yesterday's +9% mean effect is real: ~101-103. Batch 3:
  settle2 = 7 of 8; k8-c collect will be 8 (boundary).
