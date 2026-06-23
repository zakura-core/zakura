//! Tests for the history-tree rebuild format upgrade.
//!
//! The Ironwood `zcash_history` bump grew `MAX_ENTRY_SIZE`, which made history-tree column-family
//! entries written by older Zebra versions unreadable (a bincode `UnexpectedEof` panic). The
//! [`rebuild_history_tree`](crate::service::finalized_state::disk_format::upgrade::rebuild_history_tree)
//! upgrade repairs this by rebuilding the tip tree from blocks and the per-height note commitment
//! tree roots and rewriting it in the current format.
//!
//! This test can't open a real pre-Ironwood on-disk database here (that runs on a droplet with a
//! snapshot), so it reproduces the failure mode locally and verifies the load-bearing properties:
//!
//! 1. The rebuild reproduces the *exact same* chain-history MMR root that was stored — this is the
//!    consensus-correctness guarantee. This is checked both by calling the rebuild directly
//!    (bypassing the up-front "needs rebuild?" check), and by corrupting the stored entry into an
//!    unreadable old-format blob and driving the real synchronous repair path end to end.
//! 2. The rebuild detects that a database already written in the current format does not need
//!    repair, so it never touches a healthy database (including an empty one).
//! 3. A database that needs a rebuild but is missing the historical data the rebuild reads (a
//!    database pruned before the Ironwood bump) fails with a clear, explained error rather than an
//!    opaque panic.

use std::env;

use zebra_chain::{
    block::Height,
    parameters::{
        testnet::{ConfiguredActivationHeights, Parameters as TestnetParameters},
        Network, NetworkUpgrade,
    },
    LedgerState,
};
use zebra_test::prelude::*;

use crate::{
    config::Config,
    service::{
        arbitrary::PreparedChain,
        finalized_state::{
            disk_format::{upgrade::rebuild_history_tree, RawBytes},
            CheckpointVerifiedBlock, DiskWriteBatch, FinalizedState,
        },
    },
    SemanticallyVerifiedBlock,
};

/// Number of proptest cases. Each case syncs an on-disk database, so the default is low.
const DEFAULT_REBUILD_PROPTEST_CASES: u32 = 1;

fn proptest_cases() -> u32 {
    env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_REBUILD_PROPTEST_CASES)
}

/// A configured testnet with low activation heights, so a short generated chain reaches Heartwood
/// (height 5) and stores a non-empty history tree.
fn rebuild_test_network() -> Network {
    TestnetParameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(2),
            sapling: Some(3),
            blossom: Some(4),
            heartwood: Some(5),
            canopy: Some(6),
            nu5: Some(7),
            nu6: Some(8),
            nu6_1: Some(9),
            nu6_2: Some(10),
            nu6_3: Some(11),
            nu7: Some(12),
        })
        .expect("configured activation heights are valid")
        .extend_funding_streams()
        .to_network()
        .expect("configured network is valid")
}

/// Syncs a fresh ephemeral finalized state by committing `blocks` in order, returning the live
/// state so its database can be queried.
///
/// Format upgrades are skipped so the background format-change thread (which runs concurrently with
/// commits and would race the genesis-roots format check on a partially-synced database) is never
/// spawned. This isolates the test to the history-tree rebuild logic, which it drives directly.
fn sync_to(network: &Network, blocks: &[SemanticallyVerifiedBlock]) -> FinalizedState {
    let mut state = FinalizedState::new_with_debug(
        &Config::ephemeral(),
        network,
        true,
        #[cfg(feature = "elasticsearch")]
        false,
        false,
    );

    for block in blocks {
        let checkpoint_verified = CheckpointVerifiedBlock::from(block.block.clone());
        state
            .commit_finalized_direct(
                checkpoint_verified.into(),
                None,
                "rebuild history tree test",
            )
            .expect("committing a generated block to a fresh state succeeds");
    }

    state
}

/// Overwrites the stored tip history-tree entry with an unreadable, old-format-style blob.
///
/// Reproduces the on-disk state of a pre-Ironwood database: the entry exists but can no longer be
/// deserialized in the current `Entry` format. We do this by truncating the real current-format
/// bytes, which makes the bincode reader hit end-of-input partway through an `Entry` — exactly the
/// `UnexpectedEof` failure the larger Ironwood `MAX_ENTRY_SIZE` produces when it reads a smaller
/// stored entry.
fn corrupt_tip_history_tree_to_old_format(db: &crate::service::finalized_state::ZebraDb) {
    let raw_entry = db
        .raw_history_tree_value_cf()
        .zs_get(&())
        .expect("a synced post-Heartwood database has a stored tip history tree entry");

    let mut truncated = raw_entry.raw_bytes().clone();
    assert!(
        !truncated.is_empty(),
        "the stored history tree entry should have a non-empty serialization to truncate",
    );
    // Drop the final byte so the reader runs out of input mid-entry.
    truncated.pop();

    let mut batch = DiskWriteBatch::new();
    let _ = db
        .raw_history_tree_value_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&(), &RawBytes::new_raw_bytes(truncated));
    db.write_batch(batch)
        .expect("writing a synthetic old-format history tree entry succeeds");

    assert!(
        rebuild_history_tree::needs_rebuild(db),
        "the corrupted entry must be detected as needing a rebuild",
    );
}

#[test]
fn rebuild_reproduces_stored_history_root() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = rebuild_test_network();
    // Generate a chain from genesis with valid commitments, so the post-Heartwood blocks carry
    // valid chain-history-root commitments and can be committed to a finalized state.
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, Some(2), true);

    proptest!(
        ProptestConfig::with_cases(proptest_cases()),
        |((chain, _count, network, _history_tree) in PreparedChain::default()
            .with_ledger_strategy(ledger_strategy)
            .with_valid_commitments()
            .no_shrink())| {
            let synced: Vec<SemanticallyVerifiedBlock> = chain.iter().cloned().collect();
            // Require enough blocks to be a few past the Heartwood activation height (5), so a
            // non-empty history tree with more than one entry is stored.
            prop_assume!(synced.len() > 8);

            let state = sync_to(&network, &synced);
            let db = &state.db;

            let tip_height = db
                .finalized_tip_height()
                .expect("synced database has a finalized tip");

            // The stored tip history root is the ground truth a fresh sync produced.
            let stored_root = db.history_tree().hash();
            prop_assert!(
                stored_root.is_some(),
                "a Heartwood-onward chain should store a non-empty history tree",
            );

            // A freshly synced database is already in the current format, so the upgrade must detect
            // that no rebuild is needed.
            prop_assert!(
                !rebuild_history_tree::needs_rebuild(db),
                "a freshly synced database should already be in the current history tree format",
            );

            // Property 1, directly: rebuilding from blocks and note commitment roots (exercising the
            // activation-height selection and the push loop) reproduces the identical MMR root. This
            // bypasses the up-front "needs rebuild?" check so the rebuild always runs.
            let rebuilt = rebuild_history_tree::rebuild_tip_history_tree(db, &network, tip_height)
                .expect("rebuild from a fully synced database should not be missing any data")
                .expect("a Heartwood-onward tip should rebuild a non-empty history tree");
            prop_assert_eq!(
                rebuilt.hash(),
                stored_root,
                "the directly rebuilt history tree root must match the originally stored root",
            );

            // Property 1, end to end: corrupt the stored entry into an unreadable old-format blob,
            // then run the real synchronous repair path. It must rewrite the entry so it reads back
            // in the current format, with the same root, and pass the upgrade's validity check.
            corrupt_tip_history_tree_to_old_format(db);

            rebuild_history_tree::rebuild_tip_history_tree_if_needed(db, tip_height)
                .expect("repairing a fully synced database should not be missing any data");

            prop_assert!(
                !rebuild_history_tree::needs_rebuild(db),
                "the entry must be readable in the current format after the repair",
            );
            prop_assert!(
                rebuild_history_tree::quick_check(db).is_ok(),
                "history tree should pass its validity check after the repair",
            );
            prop_assert_eq!(
                db.history_tree().hash(),
                stored_root,
                "the repaired history tree root must match the originally stored root",
            );
        }
    );

    Ok(())
}

/// An empty database has no history tree entry, so the upgrade detects nothing to rebuild and the
/// repair is a no-op.
#[test]
fn rebuild_is_noop_on_empty_database() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = rebuild_test_network();

    let state = sync_to(&network, &[]);
    let db = &state.db;

    assert!(
        !rebuild_history_tree::needs_rebuild(db),
        "an empty database has no history tree entry to rebuild",
    );

    // An empty database has no finalized tip; the open path skips the synchronous repair when there
    // is no tip, but the repair is exercised directly here using the genesis height as a stand-in to
    // confirm it does nothing when there is no entry.
    rebuild_history_tree::rebuild_tip_history_tree_if_needed(db, Height(0))
        .expect("an empty database needs no rebuild");

    assert!(
        rebuild_history_tree::quick_check(db).is_ok(),
        "an empty database passes the history tree validity check",
    );

    Ok(())
}

/// A database that needs a rebuild but is missing the historical blocks the rebuild reads (a
/// database pruned before the Ironwood bump) fails with a clear, explained error rather than an
/// opaque `.expect()` panic.
#[test]
fn rebuild_fails_clearly_on_pruned_old_format_database() -> Result<()> {
    let _init_guard = zebra_test::init();

    let network = rebuild_test_network();
    let ledger_strategy =
        LedgerState::genesis_strategy(Some(network), NetworkUpgrade::Nu5, Some(2), true);

    proptest!(
        ProptestConfig::with_cases(proptest_cases()),
        |((chain, _count, network, _history_tree) in PreparedChain::default()
            .with_ledger_strategy(ledger_strategy)
            .with_valid_commitments()
            .no_shrink())| {
            let synced: Vec<SemanticallyVerifiedBlock> = chain.iter().cloned().collect();
            prop_assume!(synced.len() > 8);

            let state = sync_to(&network, &synced);
            let db = &state.db;

            let tip_height = db
                .finalized_tip_height()
                .expect("synced database has a finalized tip");

            // Make the tip entry need a rebuild, then delete a block the rebuild requires. The
            // history tree resets at the current network upgrade's activation height, so deleting a
            // block at or after that height removes data the rebuild reads. Heartwood activates at
            // height 5 and the post-NU5 tip's history window starts at the most recent upgrade
            // activation, so the block just below the tip is always within the rebuild range.
            corrupt_tip_history_tree_to_old_format(db);

            let missing_height = Height(tip_height.0 - 1);
            let mut batch = DiskWriteBatch::new();
            batch.delete_block_header(db, missing_height);
            db.write_batch(batch)
                .expect("deleting a block header to simulate a pruned database succeeds");

            let result = rebuild_history_tree::rebuild_tip_history_tree_if_needed(db, tip_height);

            prop_assert!(
                matches!(
                    result,
                    Err(rebuild_history_tree::RebuildError::MissingData { .. })
                ),
                "a pruned old-format database must fail the rebuild with a clear MissingData error, \
                 got: {result:?}",
            );
        }
    );

    Ok(())
}
