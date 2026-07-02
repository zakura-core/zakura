# PR #282 review — `feat!: enforce ranged header requests have roots`

Branch `review/headersync-roots` @ `8c2f7d379` onto `perf-note-commit-tree` (`e73e09d71`, includes #254).

Scope: ranged Zakura header-sync responses/commits must now carry exactly one
`tree_aux_root` per header (previously optional / all-or-nothing). Threaded through
wire decode → reactor serving/inbound → state commit → root-covered best-header-tip
capping (state service + header-sync driver, startup + steady-state).

## Verdict

Design and implementation are consistent end-to-end. One real compile bug found and
fixed; remaining red tests are all pre-existing on the base branch (documented as flups
below). The roots invariant holds transitively: a header only enters a peer's store via
`CommitHeaderRange` (now mandates roots → persists provisional roots) or a full-block
commit (roots derivable from state), so any header a peer can serve, it can also serve a
root for. Tip propagation still flows over full-block `NewBlock` gossip, so the
mandatory-roots rule on _ranged_ requests does not starve the tip.

## Bug fixed in this review

- **zebrad lib tests did not compile.** `start.rs`'s `zakura_header_sync_driver_tests`
  imports `block_roots_cover_range` and `root_covered_query_best_header_tip` via
  `super::zakura::`, but `zebrad/src/commands/start/zakura/mod.rs` never re-exported them
  (both are `pub(crate)` in `header_sync_driver.rs` and used in-module by production code).
  The PR author missed this because their local `librocksdb-sys` build failed before
  reaching zebrad, so the zebrad tests never compiled. Fix: added both to the
  `#[cfg(test)]` re-export block in `mod.rs`. The reported `E0282` was a cascade from the
  unresolved import.

## Flups — pre-existing test failures (NOT caused by #282; reproduce on base `e73e09d71`)

1. **`zebra-state` proptest `service::finalized_state::tests::prop::vct_frozen_frontier_survives_reopen`.**
   Panics at `finalized_state.rs:551`: "database was previously synced in verified
   commitment tree mode ... fast path ... is disabled. Set `consensus.checkpoint_sync = true`
   and `consensus.disable_vct_fast_sync = false`...". This is #254's VCT fast-sync resume
   gate; the proptest reopen config doesn't satisfy the resume preconditions. Verified to
   fail identically on the base branch. Relies on later VCT-resume wiring → flup.

2. **`zebrad` legacy block-sync vectors (run via nextest):**
   `components::sync::tests::vectors::request_genesis_accepts_duplicate_finalized_genesis`,
   `...::sync_block_too_high_obtain_tips`, `...::sync_block_too_high_extend_tips`.
   Legacy (non-Zakura) sync component, untouched by this PR. Verified to fail identically
   on the base branch → flup.

3. **`zebra-network` testkit network tests (env-flaky):**
   `zakura::testkit::cluster::tests::connected_peers_import_each_others_signed_records` and
   `...::native_stream5_status_exchange_uses_handler_wire_path`. Real iroh peer
   registration with 5s timeouts; fail only under parallel-build CPU load, **pass in
   isolation**. Harness flakiness, not a sync defect → flup.

## Harness notes (not failures)

- `cargo test -p zebrad --lib` (single process) cascades ~76 failures from one root panic:
  `zebra_test::init()` → color-eyre `install().unwrap()` → "a hook has already been
  installed", poisoning the init `Once`. CI uses **nextest** (process-per-test), which
  sidesteps this. Always validate zebrad with `cargo nextest run`, not `cargo test --lib`.
- `cargo clippy --workspace -- -D warnings` fails on **pre-existing** zebra-chain lints
  (`unexpected_cfgs: tx_v6` at `transaction.rs:1099`; 4× `ValueCommitment` Copy-clone),
  not on anything in #282. PR-touched files are clippy-clean.
- Build requires `CXXFLAGS="-include cstdint"` on GCC 15 (the `librocksdb-sys` C++ /
  `<cstdint>` failure the PR author hit). Not a code issue.

## Non-blocking review observations (candidate follow-ups for the author)

- **Redundant double root-cover.** `ReadRequest::BestHeaderTip` already returns the
  root-covered tip (`root_covered_best_header_tip` in the state service), yet
  `drive_zakura_header_sync_actions` re-applies `root_covered_query_best_header_tip` to
  that result on every `query_best_header_tip` tick — two extra state reads
  (`Tip` + `BlockRoots`) plus a duplicated root scan. Correct (idempotent/monotonic) but
  wasteful; consider keeping the cap in one layer.
- **Per-height serving cost.** `block_roots_by_height_range` does point lookups per height
  (`finalized_tip_height()` + `serve_block_roots(h..=h)` + provisional read each iteration),
  up to `MAX_HEADER_SYNC_HEIGHT_RANGE` = 4000, on a hot serving path that previously used a
  single range scan. Consider batching the finalized/provisional reads.
- **Stream version.** `ZAKURA_HEADER_SYNC_STREAM_VERSION` stays `4` while v4 semantics flip
  from "optional all-or-nothing roots" to "mandatory one-per-header". An old-v4 peer
  answering a non-finalized range would now be rejected (`TreeAuxRootCountMismatch` →
  `MalformedMessage`). Fine for a pre-GA fleet upgraded together, but a deliberate
  bump-to-5 would make the incompatibility explicit.
- **No backfill migration for pre-existing rootless header rows.** A DB written under the
  old optional-roots regime has header rows without provisional roots; after upgrade those
  ranges serve empty and the advertised tip is capped to the verified tip until re-synced
  with roots. Self-heals (no wedge), but there is no explicit migration. Confirm this is the
  intended degradation path (cross-ref the earlier "header-carried roots" plan that leaned
  toward keeping roots optional).
- **CHANGELOG.** `feat!` with no CHANGELOG entry; intentional for experimental Zakura
  internals, but worth a deliberate note.
