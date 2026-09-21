//! Tests for the zips#1354 rule that a block must not make the ZIP 234 NSM value balance
//! negative.
//!
//! `issuance_accounting` runs the shared state histories with reissuance on and off.

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    parameters::{
        subsidy::is_zip234_active,
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkKind, NetworkUpgrade,
    },
    transparent::Address,
};

use crate::{
    service::{finalized_state::FinalizedState, non_finalized_state::NonFinalizedState},
    CommitBlockError, Config, SemanticallyVerifiedBlock, ValidateContextError,
};

use super::{
    issuance_accounting::{
        accounting_network, child_block_with_history_commitment, commit, permitted_start_block,
        start_block, state_below_start, state_below_start_with_config, START,
    },
    rollback::{child_block, coinbase_tx},
};

/// Returns whether `error` is the negative NSM value balance error for [`START`].
fn is_negative_balance_at_start(error: &ValidateContextError) -> bool {
    matches!(
        error,
        ValidateContextError::NegativeNsmValueBalance { height, .. } if *height == START
    )
}

/// The non-finalized state rejects the block that makes the balance negative.
#[test]
fn non_finalized_state_rejects_a_block_that_makes_the_balance_negative() {
    let _init_guard = zakura_test::init();

    let network = accounting_network(true);
    assert!(is_zip234_active(&network, START));
    let (state, parent) = state_below_start(&network);

    let over = start_block(&state, &network, &parent, 1);
    let error = NonFinalizedState::new(&network)
        .commit_new_chain(SemanticallyVerifiedBlock::from(over), &state.db)
        .expect_err("a block that issues above the schedule is invalid");
    assert!(is_negative_balance_at_start(&error), "{error:?}");

    // A block that brings the issued supply exactly to the schedule leaves a zero balance.
    let on_schedule = start_block(&state, &network, &parent, 0);
    NonFinalizedState::new(&network)
        .commit_new_chain(SemanticallyVerifiedBlock::from(on_schedule), &state.db)
        .expect("a zero balance is valid");
}

/// The rule applies from NU7, not from the later reissuance start height.
#[test]
fn nu7_rejects_a_negative_balance_below_the_reissuance_start() {
    use zakura_chain::parameters::subsidy::halving_block_subsidy;

    let _init_guard = zakura_test::init();

    let network = accounting_network(true);
    let nu7 = NetworkUpgrade::Nu7
        .activation_height(&network)
        .expect("the test network activates NU7");

    assert!(
        nu7 < START,
        "the fixture must leave a gap between NU7 and the reissuance start",
    );
    assert!(
        !is_zip234_active(&network, nu7),
        "no block at NU7 claims a bonus on this network",
    );

    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    let mut state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("opening an ephemeral database should succeed");
    let mut parent = zakura_chain::block::genesis::regtest_genesis_block();
    commit(&mut state, &parent).expect("the genesis block commits");

    for height in 1..nu7.0 {
        // The Heartwood activation block has the reserved all-zero commitment.
        let dust = Amount::<NonNegative>::try_from(1).expect("1 fits in Amount<NonNegative>");
        let block = child_block(&parent, vec![coinbase_tx(Height(height), dust, &address)]);
        commit(&mut state, &block).expect("a block below NU7 commits");
        parent = block;
    }

    // One zatoshi above the schedule drives the balance below zero.
    let scheduled = i64::from(halving_block_subsidy(nu7, &network).expect("valid subsidy"));
    let over = child_block_with_history_commitment(
        &parent,
        vec![coinbase_tx(
            nu7,
            Amount::try_from(scheduled + 1).expect("valid coinbase value"),
            &address,
        )],
        &network,
        &state.db.history_tree(),
    );

    let error = commit(&mut state, &over)
        .expect_err("a block that issues above the schedule is invalid from NU7");
    let CommitBlockError::ValidateContextError(error) = error else {
        panic!("unexpected commit error: {error:?}");
    };

    assert!(
        matches!(
            *error,
            ValidateContextError::NegativeNsmValueBalance { height, .. } if height == nu7
        ),
        "{error:?}",
    );

    // The same block claiming exactly the schedule commits.
    let on_schedule = child_block_with_history_commitment(
        &parent,
        vec![coinbase_tx(
            nu7,
            Amount::try_from(scheduled).expect("valid coinbase value"),
            &address,
        )],
        &network,
        &state.db.history_tree(),
    );
    commit(&mut state, &on_schedule).expect("a zero balance is valid");
}

/// The finalized state rejects a checkpoint-verified block that makes the balance negative.
#[test]
fn finalized_state_rejects_a_block_that_makes_the_balance_negative() {
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
    assert!(is_negative_balance_at_start(&error), "{error:?}");
    assert_eq!(
        state.db.finalized_tip_height(),
        START.previous().ok(),
        "the rejected block is not committed",
    );

    assert_eq!(state.db.finalized_value_pool(), pools_before);
    assert!(state.db.block_info(START.into()).is_none());

    let on_schedule = start_block(&state, &network, &parent, 0);
    commit(&mut state, &on_schedule).expect("a zero balance is valid");
    assert_eq!(state.db.finalized_tip_height(), Some(START));
}

/// A seed large enough to stand out against the test network's dust coinbases.
const SEED: i64 = 1_234_567;

/// Returns [`accounting_network`] with a nonzero `INITIAL_NSM_VALUE_BALANCE`.
fn seeded_network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(2),
            ..Default::default()
        },
        test_nsm_reissuance_height: Some(START),
        initial_nsm_value_balance: Some(
            Amount::try_from(SEED).expect("the seed is a valid amount"),
        ),
        ..Default::default()
    })
}

/// Returns NU7 activation and the block below it on `network`.
fn seed_height(network: &Network) -> (Height, Height) {
    let nu7 = NetworkUpgrade::Nu7
        .activation_height(network)
        .expect("the test network activates NU7");

    (nu7, nu7.previous().expect("NU7 activates above genesis"))
}

/// The seed lands on the last block below NU7, and no earlier block carries it.
#[test]
fn the_seed_lands_on_the_last_block_below_nu7() {
    let _init_guard = zakura_test::init();

    let network = seeded_network();
    let (nu7, seed_height) = seed_height(&network);
    let (state, _parent) = state_below_start(&network);

    for height in 0..seed_height.0 {
        let info = state
            .db
            .block_info(Height(height).into())
            .expect("every committed block has block info");

        assert_eq!(
            i64::from(info.value_pools().nsm_value_balance_amount()),
            0,
            "the balance must stay empty below the seed height, at {height}",
        );
    }

    let seeded = state
        .db
        .block_info(seed_height.into())
        .expect("the seeded block has block info");

    assert_eq!(
        i64::from(seeded.value_pools().nsm_value_balance_amount()),
        SEED,
        "the last block below NU7 must carry the whole seed",
    );

    // The dust coinbase at NU7 leaves its own subsidy unclaimed, which the balance keeps
    // on top of the seed.
    let activation = state
        .db
        .block_info(nu7.into())
        .expect("the NU7 activation block has block info");

    assert!(
        i64::from(activation.value_pools().nsm_value_balance_amount()) > SEED,
        "an under-claiming NU7 block must add to the seed, got {:?}",
        activation.value_pools().nsm_value_balance_amount(),
    );
}

/// Rolling back below the seed height clears the seed, and replay restores it.
#[test]
fn rollback_across_the_seed_height_restores_it_on_replay() {
    rollback_seed_history(seeded_network());
    rollback_seed_history(derived_seed_network());
}

fn derived_seed_network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(2),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn rollback_seed_history(network: Network) {
    use crate::{rollback_finalized_state, RollbackFinalizedStateOptions};

    let _init_guard = zakura_test::init();

    let (_nu7, seed_height) = seed_height(&network);
    let dir = tempfile::tempdir().expect("a temporary directory is available");
    let config = Config {
        cache_dir: dir.path().to_owned(),
        ephemeral: false,
        ..Config::default()
    };

    let (state, _parent) = state_below_start_with_config(&network, &config);
    let tip = state
        .db
        .finalized_tip_height()
        .expect("the fixture commits blocks");
    let committed: Vec<_> = (0..=tip.0)
        .map(|height| {
            state
                .db
                .block(Height(height).into())
                .expect("every committed block is readable")
        })
        .collect();
    let before = state.db.finalized_value_pool();
    drop(state);

    let target = seed_height
        .previous()
        .expect("the seed height is above genesis");

    rollback_finalized_state(
        config.clone(),
        &network,
        RollbackFinalizedStateOptions {
            target_height: target,
            keep_rolled_back_blocks: false,
            max_checkpoint_height: Some(Height(0)),
        },
    )
    .expect("rolling back below the seed height succeeds");

    let mut state =
        FinalizedState::new(&config, &network).expect("reopening the database succeeds");

    assert_eq!(
        i64::from(state.db.finalized_value_pool().nsm_value_balance_amount()),
        0,
        "rolling back below the seed height must undo the seed",
    );

    for block in committed
        .iter()
        .skip(usize::try_from(target.0).expect("the target height fits in a usize") + 1)
    {
        commit(&mut state, block).expect("replaying a rolled back block commits");
    }

    assert_eq!(
        state.db.finalized_value_pool(),
        before,
        "replay must restore the seed and every later change",
    );
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

#[test]
fn derived_seed_survives_non_finalized_forks_and_finalization() {
    fn coinbase_tx(
        height: Height,
        value: Amount<NonNegative>,
        address: &Address,
    ) -> std::sync::Arc<zakura_chain::transaction::Transaction> {
        use zakura_chain::{
            transaction::{LockTime, Transaction},
            transparent::{Input, Output},
        };
        std::sync::Arc::new(Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            lock_time: LockTime::unlocked(),
            expiry_height: height,
            inputs: vec![Input::Coinbase {
                height,
                data: vec![0; 8],
                sequence: u32::MAX,
            }],
            outputs: vec![Output::new(value, address.script())],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        })
    }

    use zakura_chain::parameters::subsidy::scheduled_issuance_zatoshis;
    let _guard = zakura_test::init();
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(3),
            ..Default::default()
        },
        ..Default::default()
    });
    let mut state = FinalizedState::new(&Config::ephemeral(), &network).unwrap();
    let genesis = zakura_chain::block::genesis::regtest_genesis_block();
    commit(&mut state, &genesis).unwrap();
    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    let root = child_block(
        &genesis,
        vec![coinbase_tx(
            Height(1),
            Amount::try_from(1).unwrap(),
            &address,
        )],
    );
    let mut forks = NonFinalizedState::new(&network);
    forks
        .commit_new_chain(SemanticallyVerifiedBlock::from(root.clone()), &state.db)
        .unwrap();
    let history = forks
        .best_chain()
        .unwrap()
        .history_tree(root.hash().into())
        .unwrap();
    let seed_block = child_block_with_history_commitment(
        &root,
        vec![coinbase_tx(
            Height(2),
            Amount::try_from(2).unwrap(),
            &address,
        )],
        &network,
        &history,
    );
    forks
        .commit_block(
            SemanticallyVerifiedBlock::from(seed_block.clone()),
            &state.db,
        )
        .unwrap();
    let expected =
        i64::try_from(scheduled_issuance_zatoshis(Height(2), &network).unwrap()).unwrap() - 3;
    let seeded = forks
        .best_chain()
        .unwrap()
        .non_finalized_tip_with_value_balance()
        .2;
    assert_eq!(i64::from(seeded.nsm_value_balance_amount()), expected);

    // Reverting the seed block must clear its state-derived balance.
    let fork = forks.best_chain().unwrap().fork(root.hash()).unwrap();
    assert_eq!(
        i64::from(
            fork.non_finalized_tip_with_value_balance()
                .2
                .nsm_value_balance_amount()
        ),
        0
    );
    let alternative = child_block_with_history_commitment(
        &root,
        vec![coinbase_tx(
            Height(2),
            Amount::try_from(3).unwrap(),
            &address,
        )],
        &network,
        &history,
    );
    forks
        .commit_block(
            SemanticallyVerifiedBlock::from(alternative.clone()),
            &state.db,
        )
        .unwrap();
    let alternative_pools = forks
        .chain_iter()
        .find_map(|chain| chain.block_info(alternative.hash().into()))
        .unwrap();
    assert_eq!(
        i64::from(alternative_pools.value_pools().nsm_value_balance_amount()),
        expected - 1
    );
    // Finalization recalculates the seed from finalized monetary pools.
    let selected = forks
        .best_chain()
        .unwrap()
        .non_finalized_tip_with_value_balance()
        .2;
    state
        .commit_finalized_direct(forks.finalize(), None, None, "derived seed test")
        .unwrap();
    state
        .commit_finalized_direct(forks.finalize(), None, None, "derived seed test")
        .unwrap();
    assert_eq!(state.db.finalized_value_pool(), selected);
}
