//! Tests for the zips#1354 rule that a block must not make the ZIP 234 issuance deficit
//! negative.
//!
//! `issuance_accounting` runs the shared state histories with reissuance on and off.

use zakura_chain::{block::Block, block::Height, parameters::subsidy::is_zip234_active};

use crate::{
    service::non_finalized_state::NonFinalizedState, CommitBlockError, SemanticallyVerifiedBlock,
    ValidateContextError,
};

use super::issuance_accounting::{
    accounting_network, commit, permitted_start_block, start_block, state_below_start, START,
};

/// Returns whether `error` is the negative issuance deficit error for [`START`].
fn is_negative_deficit_at_start(error: &ValidateContextError) -> bool {
    matches!(
        error,
        ValidateContextError::NegativeNsmValueBalance { height, .. } if *height == START
    )
}

/// The non-finalized state rejects the block that makes the deficit negative.
#[test]
fn non_finalized_state_rejects_a_block_that_makes_the_deficit_negative() {
    let _init_guard = zakura_test::init();

    let network = accounting_network(true);
    assert!(is_zip234_active(&network, START));
    let (state, parent) = state_below_start(&network);

    let over = start_block(&state, &network, &parent, 1);
    let error = NonFinalizedState::new(&network)
        .commit_new_chain(SemanticallyVerifiedBlock::from(over), &state.db)
        .expect_err("a block that issues above the schedule is invalid");
    assert!(is_negative_deficit_at_start(&error), "{error:?}");

    // A block that brings the issued supply exactly to the schedule leaves a zero deficit.
    let on_schedule = start_block(&state, &network, &parent, 0);
    NonFinalizedState::new(&network)
        .commit_new_chain(SemanticallyVerifiedBlock::from(on_schedule), &state.db)
        .expect("a zero deficit is valid");
}

/// The finalized state rejects a checkpoint-verified block that makes the deficit negative.
#[test]
fn finalized_state_rejects_a_block_that_makes_the_deficit_negative() {
    let _init_guard = zakura_test::init();

    let network = accounting_network(true);
    let (mut state, parent) = state_below_start(&network);
    let pools_before = state.db.finalized_value_pool();

    let over = start_block(&state, &network, &parent, 1);
    let error =
        commit(&mut state, &over).expect_err("a block that issues above the schedule is invalid");
    let CommitBlockError::ValidateContextError(error) = error else {
        panic!("unexpected commit error: {error:?}");
    };
    assert!(is_negative_deficit_at_start(&error), "{error:?}");
    assert_eq!(
        state.db.finalized_tip_height(),
        START.previous().ok(),
        "the rejected block is not committed",
    );

    assert_eq!(state.db.finalized_value_pool(), pools_before);
    assert!(state.db.block_info(START.into()).is_none());

    let on_schedule = start_block(&state, &network, &parent, 0);
    commit(&mut state, &on_schedule).expect("a zero deficit is valid");
    assert_eq!(state.db.finalized_tip_height(), Some(START));
}

/// Older versions committed blocks at or above the ZIP 234 start without reissuance,
/// so the migration requires a resync instead of keeping their Deferred balances.
#[test]
fn migration_requires_resync_after_the_reissuance_start() {
    use crate::service::finalized_state::disk_format::upgrade::{
        nsm_value_balance_pool::Upgrade, DiskFormatUpgrade, FormatChangeError,
    };
    let _guard = zakura_test::init();
    let network = accounting_network(true);
    let (mut state, parent) = state_below_start(&network);
    let (_cancel, cancel_receiver) = crossbeam_channel::bounded(1);

    // A tip below the start migrates.
    Upgrade
        .run(Some(parent_height(&parent)), &state.db, &cancel_receiver)
        .unwrap();

    let block = permitted_start_block(&state, &network, &parent);
    commit(&mut state, &block).unwrap();
    let pools = state.db.finalized_value_pool();
    let result = Upgrade.run(Some(START), &state.db, &cancel_receiver);
    assert!(matches!(result, Err(FormatChangeError::ResyncRequired(_))));
    assert_eq!(state.db.finalized_value_pool(), pools);
}

fn parent_height(parent: &Block) -> Height {
    parent
        .coinbase_height()
        .expect("the parent has a coinbase height")
}
