//! Tests for the zips#1354 rule that a block must not make the ZIP 234 issuance deficit
//! negative.

use std::sync::Arc;

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    parameters::{
        subsidy::{expected_issued_supply, is_zip234_active},
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkKind, NetworkUpgrade,
    },
    transaction::{LockTime, Transaction},
    transparent::{Address, Input, Output},
};

use crate::{
    service::{
        finalized_state::{CheckpointVerifiedBlock, FinalizedState},
        non_finalized_state::NonFinalizedState,
    },
    CommitBlockError, Config, SemanticallyVerifiedBlock, ValidateContextError,
};

use super::rollback::{child_block, child_block_with_history_commitment, coinbase_tx};

/// The ZIP 234 start height on the test network.
const START: Height = Height(3);

/// Returns a Regtest network with NU7 at height 2 and ZIP 234 reissuance from [`START`].
fn zip234_network() -> Network {
    Network::new_regtest(RegtestParameters {
        // Regtest activates Heartwood at height 1, where the block commitment is reserved.
        // NU7 activates after it.
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(2),
            ..Default::default()
        },
        zip234_start_height: Some(START),
        ..Default::default()
    })
}

/// Returns a finalized state with Regtest blocks up to the parent of [`START`], and that
/// parent block.
fn state_below_start(network: &Network) -> (FinalizedState, Arc<Block>) {
    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    let dust = Amount::<NonNegative>::try_from(1).expect("1 fits in Amount<NonNegative>");

    let mut state = FinalizedState::new(&Config::ephemeral(), network)
        .expect("opening an ephemeral database should succeed");
    let mut parent = zakura_chain::block::genesis::regtest_genesis_block();
    commit(&mut state, &parent).expect("the genesis block commits");

    for height in 1..START.0 {
        let transactions = vec![coinbase_tx(Height(height), dust, &address)];
        let block = if height == 1 {
            // The Heartwood activation block has the reserved all-zero commitment.
            child_block(&parent, transactions)
        } else {
            child_block_with_history_commitment(
                &parent,
                transactions,
                network,
                &state.db.history_tree(),
            )
        };
        commit(&mut state, &block).expect("a block below the start height commits");
        parent = block;
    }

    (state, parent)
}

/// Returns a child of `parent` at [`START`] whose coinbase makes the issued supply exceed
/// the expected issued supply by `excess` zatoshi.
fn start_block(
    state: &FinalizedState,
    network: &Network,
    parent: &Block,
    excess: i64,
) -> Arc<Block> {
    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    let expected = expected_issued_supply(START, network).expect("valid expected issued supply");
    let issued = state.db.finalized_value_pool().issued_supply();
    let coinbase_value =
        Amount::<NonNegative>::try_from(i64::from(expected) - i64::from(issued) + excess)
            .expect("valid coinbase value");

    // The non-finalized state only accepts transaction versions that are valid after
    // Canopy.
    let coinbase = Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu7,
        lock_time: LockTime::unlocked(),
        expiry_height: START,
        inputs: vec![Input::Coinbase {
            height: START,
            data: vec![0x00; 8],
            sequence: u32::MAX,
        }],
        outputs: vec![Output::new(coinbase_value, address.script())],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    };

    child_block_with_history_commitment(
        parent,
        vec![Arc::new(coinbase)],
        network,
        &state.db.history_tree(),
    )
}

/// Commits `block` to the finalized state as a checkpoint-verified block.
fn commit(state: &mut FinalizedState, block: &Arc<Block>) -> Result<(), CommitBlockError> {
    state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(block.clone()).into(),
            None,
            None,
            "issuance deficit test",
        )
        .map(|_| ())
        .map_err(|error| error.inner().clone())
}

/// Returns whether `error` is the negative issuance deficit error for [`START`].
fn is_negative_deficit_at_start(error: &ValidateContextError) -> bool {
    matches!(
        error,
        ValidateContextError::NegativeIssuanceDeficit { height, .. } if *height == START
    )
}

/// The non-finalized state rejects the block that makes the deficit negative.
#[test]
fn non_finalized_state_rejects_a_block_that_makes_the_deficit_negative() {
    let _init_guard = zakura_test::init();

    let network = zip234_network();
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

    let network = zip234_network();
    let (mut state, parent) = state_below_start(&network);

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

    let on_schedule = start_block(&state, &network, &parent, 0);
    commit(&mut state, &on_schedule).expect("a zero deficit is valid");
    assert_eq!(state.db.finalized_tip_height(), Some(START));
}
