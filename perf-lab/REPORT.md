# perf-lab report — regenerated 2026-07-23 (SESSION 3 close)

One-page digest of everything the agentic perf loop has established.
Authoritative detail: `LEDGER.md`. Queue: `BACKLOG.md`.

## Confirmed win — request batching (EXP-002)

**Raise `DEFAULT_BS_BLOCKS_PER_RESPONSE` from 1 to 8.** Mean effect ~+9-12%
post-commit blk/s on the standard 120k checkpoint window; worst-leg margin
+3.6-4.6%. Reproduced across two days and two bench droplets at the same
SHA (c3b26d24); every k=8 leg beat every same-night k=1 leg (10/10
pairwise). k=32 regresses — the sweet spot is at or near 8. All evidence is
live-public-network substrate (see incident below); a cohort-grade
deterministic effect-size run is an optional strengthener.

- Change: zakura-network/src/zakura/block_sync/config.rs:7, one line.
- Risk: yellow (protocol behavior; both sides already negotiate k).
- Suggested gating: after PRs 166 and 217 land; optional on-cohort A/B.

## Other findings

- **No mainline regression** 63b8d4dc → 30b0c63d: head-to-head showed the
  newer commit +9% (i.e. mainline got faster, not slower).
- **Download concurrency is not the lever**: DL_LIMIT flat across 50-400.
- **Measurement protocol**: single-run A/B noise band 7.6% (max of genuine
  A/A samples); effective single-run threshold 15.2%; two-leg worst-vs-best
  rule with settling passes is the workable alternative for smaller
  effects. Public-peer levels shift ~10% between droplets/nights — only
  same-night same-droplet comparisons are valid.
- **Cache regime (corrected 07-23)**: the RAM-warm ~190 transient after
  seeding was a genuine cohort observation (aa-cohort1-3); later "steady
  ~105" levels were public-peer numbers, so serve-side regime claims
  beyond the seed transient are unproven pending an on-cohort rerun.

## Incidents (both fixed)

- **Cohort injection silently regressed** (07-22 13:31, plan-regeneration
  clobber): every run from aa-cohort4 onward — including all of EXP-002 —
  measured against public bootstrap peers, not the frozen cohort. Each
  night remained an internally consistent A/B; conclusions above are
  stated on the corrected substrate. Fixed at the canonical plan + a
  substrate banner tripwire on every bench start. The frozen serves have
  carried only 3 runs ever; decide: validate the fixed lane next session
  or tear them down (~$1/h standing).
- **Snapshot download outage** (4 consecutive mid-stream deaths on a ~32G
  single stream; origin file verified intact — the CDN path kills long
  streams intermittently): fixed permanently with a resume-to-file fetch
  (B-16 v2) baked into droplet provisioning; free-space gate raised 45→75G
  for the transient tarball.

## Spend

~$26 program-to-date (three sessions). Standing: two frozen serve droplets
~$1/h combined (Adam-approved; revisit above). Bench droplets are always
destroyed at session close.

## Next session queue

1. Validate the fixed cohort lane (one A/A; the start banner must say
   `substrate: COHORT`).
2. Optional: cohort-grade EXP-002 effect size.
3. B-04 (state-crate item; `gh pr diff 390 --name-only | grep state`
   collision-check first), B-06 (verifier batch sizes, criterion), B-07
   (rayon pool sizing).
