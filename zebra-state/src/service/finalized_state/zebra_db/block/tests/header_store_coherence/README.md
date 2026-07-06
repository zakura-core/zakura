# `header_store_coherence` — zakura header-store coherence suite

A deterministic, model-based test harness for the zakura header store: it
drives the store's real write paths through sequences of production-shaped
operations and checks the store's structural invariants after every single
mutation. No network, no tokio, no mining.

## What it tests

The zakura header store is five height-indexed RocksDB column families
(`zakura_header_by_height`, `zakura_header_hash_by_height`,
`zakura_header_height_by_hash`, `zakura_header_body_size_by_height`,
`commitment_roots_by_height`) mutated by several independent writers in
`zebra_db/block.rs`. Every reader assumes, but nothing enforces, that:

- **I1 (bijection):** `hash_by_height` ↔ `height_by_hash` are mutually inverse.
- **I2 (linkage):** rows chain by `previous_block_hash` from the finalized tip
  up to the last height-index row.
- **I3 (tip):** `best_header_tip()` is the tip of that linked chain — no
  orphan rows above it, no gaps below it, and no zakura rows at heights that
  already have a committed body (the frontier-overlay rule).

When these are violated, readers silently feed stale rows into difficulty
validation or anchor resolution, which surfaces later as
`InvalidDifficultyThreshold` or `UnknownAnchor` failures against honest peers.
This suite makes such violations visible at the moment of the corrupting
write instead.

## How it works

| Module | Role |
| --- | --- |
| `fabricate.rs` | Pure header/branch fabrication. Every threshold is computed with the store's own `AdjustedDifficulty` logic, so all fabricated chains pass real contextual validation. Work divergence between branches comes from block spacing (`Fast` = target/16, `Slow` = 4×target); branches are tens of headers long because the difficulty adjustment drifts only ~2%/block. Builds a fixed `Universe`: a 60-header trunk, a fork at height 50, and branches **A** (26 fast headers, high work), **B** (30 slow headers — longer than A but lower total work, so height order and work order disagree), **B_ext** (B's first 4 headers plus a fast continuation that out-works A), and **C** (5 headers off A's second header). |
| `audit.rs` | The invariant audit: A1 (bijection, both directions), A2 (linkage walk upward from the finalized tip over the merged header view), A3 (tip integrity, gaps, frontier overlay, aux-row backing), and A4 (the on-disk chain equals the model's expected canonical chain). Also `dump_store`, a comparable snapshot of all five column families used to assert that rejected commits are side-effect free and that reopens preserve the store byte-for-byte. |
| `oracle.rs` | An in-memory model of the store's _specified_ behavior: a single linked canonical chain, best-cumulative-work selection with strict improvement, total suffix replacement above the first conflicting height, and a sequential body tip. It predicts whether each op must be accepted or rejected. |
| `ops.rs` | The op alphabet, each op mapped to one real production write-batch shape: `CommitHeaderRange` → `prepare_header_range_batch_with_roots`; `CommitBody` / `Finalize` → `prepare_block_header_and_transaction_data_batch` plus the finalization roots delete (which runs the release path internally); `Seed` → `seed_zakura_header_from_committed_block` (the non-finalized best-chain commit hook); `Reopen` → shutdown and reopen of the persistent store. The `Harness` executes ops, cross-checks the oracle's prediction against the store's response, and audits after every mutation; failures come back as a transcribable `FailureReport` (the executed op prefix plus every violation found). |
| `scenarios.rs` | Scripted production event shapes (s01–s10): simple reorgs, lower-work rejections and their later reversal, split-range and walk-back deliveries, body commits racing header reorgs, reorgs to a lower height, double reorgs at one fork point, activity across the difficulty-adjustment window edge, restarts at every boundary, and seed/range interplay. Also holds the `corruption_repro_*` tests below. |
| `prop.rs` | Discovery proptests: random op sequences over the fixed universe, shrunk to minimal counterexamples on any audit failure. Both are `#[ignore]`d because the store has known bugs today; see "Running". |

## Known corruption bugs pinned by this suite

Each bug is pinned by a pair of tests: `corruption_repro_<name>` **passes
today** and deterministically demonstrates the violation, and
`<name>_upholds_invariants` is `#[ignore]`d and asserts the correct behavior.
When the write path is fixed, the repro fails loudly (forcing re-triage) and
the twin gets un-ignored as the permanent regression test.

1. **Unlinked-anchor commit** (`corruption_repro_unlinked_anchor_commit`).
   `prepare_header_range_batch_with_roots` never checks that
   `headers[0].previous_block_hash == anchor`, nor any intra-range linkage. A
   range anchored at a same-height hash of a different branch passes
   difficulty validation and commits a suffix that does not link to the row
   below it — an on-disk I2 violation reachable from a single untrusted peer
   response.
2. **Re-delivery over committed bodies**
   (`corruption_repro_redelivery_over_bodies`). The range insert loop gates
   only its _roots_ write on `contains_body_at_height`; the
   header/hash/height/body-size writes are unconditional. A header range
   re-delivered over heights whose bodies were committed in the meantime
   re-inserts zakura rows below the body tip, and nothing ever trims them
   again (the release trim already ran at body-commit time) — a permanent I3
   frontier-overlay violation.
3. **Unlinked seed** (`corruption_repro_seed_above_gap`,
   `corruption_repro_seed_fork_switch`; found by the proptest and shrunk to a
   single op). `prepare_zakura_header_from_committed_block` writes its row
   with no linkage or anchor precondition. Seeds fire only at non-finalized
   best-_tip_ commits, so any best-tip jump (a fork switch between
   non-finalized chains, or a restart that restores the non-finalized backup)
   seeds a height whose parent row is missing or belongs to another branch:
   a gap or broken link on disk, and a generator of poisoned
   difficulty-adjustment windows.

A discovery sweep of 2048 random sequences with these three shapes excluded
found no further violation class at that depth.

## Running

```bash
# The whole suite (scripted scenarios, audits, corruption repros):
cargo test -p zebra-state --lib header_store_coherence

# Discovery sweep beyond the known bugs (any failure = a NEW violation class):
PROPTEST_CASES=4096 cargo test -p zebra-state --lib \
    header_store_coherence::prop::prop_discovery -- --ignored

# The full random sweep (fails today on the known shapes; becomes a permanent
# regression gate once the write paths are fixed):
cargo test -p zebra-state --lib \
    header_store_coherence::prop::prop_random_sequences -- --ignored
```

Shrunk proptest counterexamples should be transcribed into `scenarios.rs` as
hardcoded `corruption_repro_*`/`#[ignore]` pairs (seed-independent pinning);
the file under `zebra-state/proptest-regressions/` pins the seeds as a
backstop and is checked in.

## Scope

Finalized-store writers only. Out of scope: `Request::InvalidateBlock` /
`ReconsiderBlock` (they operate on the non-finalized state), commitment-roots
staging (`insert_zakura_header_commitment_roots`), `rollback_finalized_state`,
and pruning. The body-commit op omits the verified-roots write from the trees
batch (it needs treestates the harness does not model); the audit treats a
missing verified-roots row as acceptable.
