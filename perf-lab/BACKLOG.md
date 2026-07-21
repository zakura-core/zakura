# perf-lab backlog

Ranked queue. The campaign-target memo (SKILL.md step 0) re-ranks this list
against measured attribution before experiment 001. Statuses:
READY | BLOCKED(<why>) | DONE(EXP-NNN) | DROPPED(<why>).

## Tuning-class (green)

- B-01 READY — Sweep `CKPT_LIMIT` (checkpoint_verify_concurrency_limit).
  Node default is 1000 (`DEFAULT_CHECKPOINT_CONCURRENCY_LIMIT` = 500*2 in
  zakurad/src/components/sync.rs); the bench harness pins CKPT_LIMIT=1500 on
  every run, so 1500-point data accrues free from baselines. Hypothesis: the
  knee sits elsewhere on dedicated CPU. Lane: bench env var only (no code
  change). Cost: 1 bench run per point, 3 points (500/1000/3000).
- B-02 READY — Sweep `DL_LIMIT` (download_concurrency_limit) 50/150/400.
  Same shape as B-01.
- B-03 READY — Block-sync knob sweep: `max_blocks_per_response`, request
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
consensus/state-integrity fixes branch (PR 165).
