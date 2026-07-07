//! Read-path coherence tests for finalized header storage.
//!
//! The write path rejects incoherent stores, so these tests corrupt column
//! families directly and assert that:
//!
//! - `recent_header_context` returns `BrokenLinkage`/`Gap` instead of a
//!   poisoned or silently shortened difficulty window, and
//! - the range writer rejects with `StoreIncoherent` (a local storage fault,
//!   never a peer-attributed validation failure) and leaves the store
//!   untouched, and
//! - the anchor round-trip distinguishes a bijection violation in our own
//!   indexes (`BijectionMismatch`) from a genuinely unknown anchor.

use std::sync::Arc;

use zebra_chain::block::{self, Height};

use super::super::super::{ZAKURA_HEADER_BY_HEIGHT, ZAKURA_HEADER_HASH_BY_HEIGHT};
use super::super::common::{commit_header_range, state_with_genesis_config};
use super::{
    audit::dump_store,
    fabricate::{Universe, BRANCH_A, FORK_HEIGHT},
};
use crate::{
    error::{CommitHeaderRangeError, StoreIncoherentError},
    service::finalized_state::{
        disk_db::{DiskWriteBatch, WriteDisk},
        ZebraDb,
    },
    Config,
};

/// A store holding genesis plus the trunk up to the fork height, built through
/// the production write path.
fn trunk_state(universe: &Universe) -> ZebraDb {
    let state = state_with_genesis_config(
        &universe.network,
        universe.genesis.clone(),
        Config::ephemeral(),
    );
    let trunk_headers: Vec<_> = universe.trunk[..FORK_HEIGHT as usize]
        .iter()
        .map(|fab| fab.header.clone())
        .collect();
    commit_header_range(&state, universe.genesis.hash(), &trunk_headers);
    state
}

/// Re-delivers a slice of trunk headers through the production range writer.
fn redeliver_trunk(
    state: &ZebraDb,
    universe: &Universe,
    anchor_height: u32,
    len: usize,
) -> Result<block::Hash, CommitHeaderRangeError> {
    let anchor = universe.trunk_at(anchor_height).hash;
    let headers: Vec<Arc<block::Header>> = universe.trunk
        [anchor_height as usize..anchor_height as usize + len]
        .iter()
        .map(|fab| fab.header.clone())
        .collect();
    let body_sizes = vec![0; headers.len()];
    let mut batch = DiskWriteBatch::new();
    let result = batch.prepare_header_range_batch(state, anchor, &headers, &body_sizes);
    if result.is_ok() {
        state
            .write_batch(batch)
            .expect("header range batch writes successfully");
    }
    result
}

/// A coherent store yields a full-span window mid-chain and a legitimately
/// short window near genesis.
#[test]
fn coherent_walks_return_ok_contexts() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    let full = state
        .recent_header_context(Height(40))
        .expect("coherent store walks cleanly");
    assert_eq!(
        full.len(),
        crate::service::check::difficulty::POW_ADJUSTMENT_BLOCK_SPAN
    );

    // Heights 5..=0 inclusive: six rows, then the walk stops at genesis.
    let short = state
        .recent_header_context(Height(5))
        .expect("a short walk ending at genesis is legitimate");
    assert_eq!(short.len(), 6);

    // A missing anchor row is not incoherence; the caller decides what an
    // unknown anchor means.
    let missing = state
        .recent_header_context(Height(FORK_HEIGHT + 10))
        .expect("a missing starting row is not a violation");
    assert!(missing.is_empty());
}

/// A header row that is not the block its hash row names (the incident-shaped
/// poison: a stale row's (threshold, time) feeding the DAA window): the walk
/// reports the divergence instead of consuming the row, and the range writer
/// maps it to a side-effect-free `StoreIncoherent` rejection.
#[test]
fn foreign_header_row_is_reported_not_validated() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    // Overwrite the header row at height 20 with a branch-A header while the
    // hash rows keep claiming the trunk.
    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let foreign_header = universe.branches[BRANCH_A].headers[3].header.clone();
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&header_cf, Height(20), foreign_header);
    state.db.write(batch).expect("raw insert writes");

    let error = state
        .recent_header_context(Height(30))
        .expect_err("the walk crosses the corrupted row");
    assert!(
        matches!(
            error,
            StoreIncoherentError::HeaderHashMismatch { height, .. } if height == Height(20)
        ),
        "expected HeaderHashMismatch at the corrupted row, got {error:?}"
    );

    // The writer surfaces the same fault as a local rejection: the range is
    // not blamed (no contextual-validation error) and nothing is written.
    let dump_before = dump_store(&state);
    let error = redeliver_trunk(&state, &universe, 30, 5)
        .expect_err("validation context crosses the corrupted row");
    assert!(
        matches!(
            error,
            CommitHeaderRangeError::StoreIncoherent(
                StoreIncoherentError::HeaderHashMismatch { .. }
            )
        ),
        "expected StoreIncoherent(HeaderHashMismatch), got {error:?}"
    );
    assert_eq!(
        dump_store(&state),
        dump_before,
        "a StoreIncoherent rejection must be side-effect free"
    );
}

/// A self-consistent foreign row (its header *is* the block its hash row
/// names, but it belongs to another branch): the row above it no longer links
/// down, and the walk reports the broken link from above.
#[test]
fn broken_linkage_is_reported_not_validated() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    // Replace both the header and hash rows at height 20 with branch-A's
    // fourth row: internally consistent, but trunk@21 does not link to it.
    let foreign = &universe.branches[BRANCH_A].headers[3];
    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&header_cf, Height(20), foreign.header.clone());
    batch.zs_insert(&hash_cf, Height(20), foreign.hash);
    state.db.write(batch).expect("raw insert writes");

    let error = state
        .recent_header_context(Height(30))
        .expect_err("the walk crosses the corrupted row");
    assert!(
        matches!(
            error,
            StoreIncoherentError::BrokenLinkage { height, actual_below, .. }
                if height == Height(21) && actual_below == foreign.hash
        ),
        "expected BrokenLinkage above the corrupted row, got {error:?}"
    );

    let error = redeliver_trunk(&state, &universe, 30, 5)
        .expect_err("validation context crosses the corrupted row");
    assert!(
        matches!(
            error,
            CommitHeaderRangeError::StoreIncoherent(StoreIncoherentError::BrokenLinkage { .. })
        ),
        "expected StoreIncoherent(BrokenLinkage), got {error:?}"
    );
}

/// A gap mid-window: a missing row below a stored one is incoherence, not the
/// end of history — a silently shortened window would shift the difficulty
/// adjustment instead of failing.
#[test]
fn gap_is_reported_not_shortened() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let mut batch = DiskWriteBatch::new();
    batch.zs_delete(&header_cf, Height(15));
    batch.zs_delete(&hash_cf, Height(15));
    state.db.write(batch).expect("raw delete writes");

    let error = state
        .recent_header_context(Height(25))
        .expect_err("the walk crosses the gap");
    assert!(
        matches!(
            error,
            StoreIncoherentError::Gap { height, missing }
                if height == Height(16) && missing == Height(15)
        ),
        "expected Gap below height 16, got {error:?}"
    );

    let error =
        redeliver_trunk(&state, &universe, 25, 5).expect_err("validation context crosses the gap");
    assert!(
        matches!(
            error,
            CommitHeaderRangeError::StoreIncoherent(StoreIncoherentError::Gap { .. })
        ),
        "expected StoreIncoherent(Gap), got {error:?}"
    );
}

/// A hash→height entry whose height→hash row disagrees is a bijection
/// violation in our own indexes: the anchor round-trip reports it as
/// `StoreIncoherent`, distinct from a genuinely unknown anchor.
#[test]
fn anchor_bijection_violation_is_store_incoherent_not_unknown() {
    let _init_guard = zebra_test::init();
    let universe = Universe::new();
    let state = trunk_state(&universe);

    // The hash row at height 30 now names a different block, while
    // height_by_hash still maps the trunk hash to 30.
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let stray_hash = universe.branches[BRANCH_A].headers[0].hash;
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&hash_cf, Height(30), stray_hash);
    state.db.write(batch).expect("raw insert writes");

    let error = redeliver_trunk(&state, &universe, 30, 5)
        .expect_err("the anchor round-trip fails on the corrupted index");
    assert!(
        matches!(
            error,
            CommitHeaderRangeError::StoreIncoherent(StoreIncoherentError::BijectionMismatch {
                hash,
                height,
                stored: Some(stored),
            }) if hash == universe.trunk_at(30).hash
                && height == Height(30)
                && stored == stray_hash
        ),
        "expected StoreIncoherent(BijectionMismatch), got {error:?}"
    );

    // An anchor the store has never heard of stays UnknownAnchor.
    let unknown = universe.branches[BRANCH_A].headers[10].hash;
    let headers = vec![universe.trunk[30].header.clone()];
    let mut batch = DiskWriteBatch::new();
    let error = batch
        .prepare_header_range_batch(&state, unknown, &headers, &[0])
        .expect_err("an unindexed anchor is unknown");
    assert!(
        matches!(error, CommitHeaderRangeError::UnknownAnchor { .. }),
        "expected UnknownAnchor, got {error:?}"
    );
}
