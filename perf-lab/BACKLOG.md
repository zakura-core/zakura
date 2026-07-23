# perf-lab backlog

Ranked queue. The campaign-target memo (SKILL.md, campaign-target-memo section) re-ranks this list
against measured attribution before experiment 001. Statuses:
READY | BLOCKED(<why>) | DONE(EXP-NNN) | DROPPED(<why>).

## Tuning-class (green)

- B-01 READY — Sweep `CKPT_LIMIT` (checkpoint_verify_concurrency_limit).
  Node default is 1000 (`DEFAULT_CHECKPOINT_CONCURRENCY_LIMIT` = 500*2 in
  zakurad/src/components/sync.rs); the bench harness pins CKPT_LIMIT=1500 on
  every run, so 1500-point data accrues free from baselines. Hypothesis: the
  knee sits elsewhere on dedicated CPU. Lane: absolute cross-invocation sweep
  per SKILL step 6 (the harness applies the knob to BOTH rows, so the
  within-run delta is only a noise check). Cost: 1 bench run per point +
  confirmations, 3 points (500/1000/3000).
- B-02 READY — Sweep `DL_LIMIT` (download_concurrency_limit) 50/150/400.
  Same shape as B-01.
- B-03 DONE(EXP-002: k=8 wins +5-9%, curve in ledger) — Block-sync knob sweep: `max_blocks_per_response`, request
  timeout, in-flight cap. Hypothesis: one manual pass tuned these; a
  mock-blocksync-pre-filtered sweep finds a better operating point.
  Lane: code-default change per point; mock-blocksync L1 pre-filter.
- B-04 READY — Body-commit batch size knee (DiskWriteBatch batching).
  Hypothesis: batch-size metrics show batching active; the knee is unmeasured.
  Lane: code-default change + L2.
- B-05 READY — RocksDB bulk-load read-side options during checkpoint sync
  (block cache size, memtable count/size, compaction style). Guarded by the
  rocksdb batch-commit histogram. Lane: code-default change + L2.
- B-06 READY — Verifier batch sizes/windows for redpallas/halo2/groth16
  batched verification. Criterion L1 pre-filter; consensus subsystem has never
  had a perf pass (risk: green while only limits/windows change, red if
  verification logic changes).
- B-07 READY — Rayon pool sizing: global verifier pool vs dedicated commit
  pool vs core count. Lane: code-default change + L2.
- B-08 READY — Tokio worker-thread count + channel capacities on the split
  sequencer channels / writer input queue. Lane: code-default change + L2.

- B-13 READY — Per-side knob overrides (`TARGET_CKPT_LIMIT`/
  `BASELINE_CKPT_LIMIT`, likewise for DL_LIMIT) in
  scripts/checkpoint-sync-bench.sh plus bench.sh plumbing, mirroring the
  existing `TARGET_/BASELINE_P2P_STACK` split, so knob sweeps become true
  within-invocation A/Bs. Harness tooling, PR-able upstream; tightens B-01/
  B-02 thresholds.

- B-14 READY — Harness: free the baseline fork before the primary leg runs
  (one-liner after the baseline `summary_row` in
  scripts/checkpoint-sync-bench.sh). Each leg's fork grows to ~116 GB
  hardlink-unique on the 200 GB c-16 disk; aa2 filled the disk to 0 and
  stalled RocksDB into the wall cap. Upstreamable.

- B-15 READY — Reduce live-peer download variance: pin a small multi-peer
  set (peerset_size 2-3 with pinned peers) or port the frozen-cohort
  determinism to perf-lab droplets. Clean pinned A/A deltas measured 0.4%,
  8.4%, 8.7% — leg throughput drifts ~9% on 30-60 min timescales (aa4
  evidence: reorder buffer 540 vs 365 MB, download-HOL both legs), so the
  single-run sensitivity floor is peer delivery, not the harness. Until this
  lands, only large effects or multi-run medians clear the band.

- B-16 READY — Harness snapshot fetch resilience: curl HTTP/2 mid-stream
  INTERNAL_ERROR (err 92) killed first-download attempts on two different
  fresh droplets (aa1 2026-07-21, aa-cohort1 2026-07-22); force --http1.1
  and add -C - resumable retries to the snapshot curl. Upstreamable.

## Structural-class (yellow)

- B-09 READY — `FromDisk` TODO at
  zakura-state/src/service/finalized_state/disk_format/block.rs:296 —
  skip redundant crypto checks when deserializing transactions from trusted
  storage, or parallelize across transactions. Extra gates per spec §5.
- B-10 BLOCKED(profile-first) — Allocation/clone hotspots in
  download→verify→commit. Needs samply/perf evidence naming the site.
- B-11 BLOCKED(coordinate PR 228) — Tracing/metrics overhead in hot loops.
- B-12 BLOCKED(attribution) — Writer idle / commit pacing; only if verdicts
  show commit-bound.

## Exclusions (refresh from `gh pr list` at session start; as of 2026-07-20)

sighash/ZIP-244 (merged caching + PR 288), block-template isolation (PR 292),
VCT artifact generation (PR 249), retained-memory accounting (PR 217/225),
lazy trace events (PR 228), block-sync peer accountability/reconnect
(PR 209/166), header-sync alignment (PR 313 + active main-tip work),
consensus/state-integrity fixes branch (PR 165). Refreshed 2026-07-22 SESSION
3: sync successor/reanchor + registry gating (PRs 386/393), best-tip context
check (394), header-root auth frontier CF in state (390) — B-04's
state-write-path work must collision-check against 390 at pick time.
Earlier refresh 2026-07-21 SESSION
1: also native peer discovery (PR 336), peer-ban on invalid blocks (PR 330),
near-tip legacy-sync stall fallback (PR 322), VCT root authentication
(PR 323), mempool advert retry (PR 341) — B-03-style reactor-knob work must
avoid discovery/ban/legacy-sync files.
