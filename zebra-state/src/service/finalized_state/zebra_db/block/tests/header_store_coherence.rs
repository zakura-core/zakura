//! Zakura header-store coherence harness.
//!
//! A deterministic, model-based test harness for the zakura header store: an
//! in-memory oracle models the *specified* behavior (best-cumulative-work
//! canonical chain with total suffix replacement above the fork point), an op
//! alphabet drives the real `DiskWriteBatch` write paths, and an audit checks
//! the store invariants after every mutation:
//!
//! - **I1 (bijection):** `hash_by_height` ↔ `height_by_hash` are mutually inverse.
//! - **I2 (linkage):** rows chain by `previous_block_hash` from the finalized tip
//!   up to the last row in the height index.
//! - **I3 (tip):** `best_header_tip()` is the tip of that linked chain — no orphan
//!   rows above it, no gaps below it.
//! - **A4 (oracle):** the linked chain equals the model's expected canonical chain.
//!
//! # Scope
//!
//! This harness exercises the finalized-store writers only:
//! `prepare_header_range_batch_with_roots`, the body-commit batch
//! (`prepare_block_header_and_transaction_data_batch` + the finalization roots
//! delete), the release path (`prepare_zakura_header_release_from_committed_block`),
//! and the seed path (`seed_zakura_header_from_committed_block`).
//!
//! Deliberately out of scope (covered by reactor-level tests or excluded from
//! the op alphabet): `Request::InvalidateBlock` / `ReconsiderBlock`
//! (non-finalized-state level), commitment-roots staging
//! (`insert_zakura_header_commitment_roots`), `rollback_finalized_state`, and
//! pruning.

mod audit;
mod fabricate;
mod ops;
mod oracle;
mod prop;
mod scenarios;
mod startup_audit;
