//! Tests for the zips#1354 rule that a block must not make the ZIP 234 NSM value balance
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

use super::rollback::{child_block, coinbase_tx};

/// Include transaction IDs so competing fixtures have distinct block hashes.
fn child_block_with_history_commitment(
    parent: &Block,
    transactions: Vec<Arc<Transaction>>,
    network: &Network,
    history: &zakura_chain::history_tree::HistoryTree,
) -> Arc<Block> {
    let mut block = super::rollback::child_block_with_history_commitment(
        parent,
        transactions,
        network,
        history,
    );
    let block_mut = Arc::make_mut(&mut block);
    Arc::make_mut(&mut block_mut.header).merkle_root = block_mut.transactions.iter().collect();
    block
}

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
    state_below_start_with_config(network, &Config::ephemeral())
}

fn state_below_start_with_config(
    network: &Network,
    config: &Config,
) -> (FinalizedState, Arc<Block>) {
    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    let dust = Amount::<NonNegative>::try_from(1).expect("1 fits in Amount<NonNegative>");

    let mut state =
        FinalizedState::new(config, network).expect("opening an ephemeral database should succeed");
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

/// Returns an accounting fixture that overdraws the eligible balance by `excess` zatoshi.
fn start_block(
    state: &FinalizedState,
    network: &Network,
    parent: &Block,
    excess: i64,
) -> Arc<Block> {
    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    let scheduled =
        zakura_chain::parameters::subsidy::halving_block_subsidy(START, network).unwrap();
    let balance = state.db.finalized_value_pool().nsm_value_balance_amount();
    let coinbase_value =
        Amount::<NonNegative>::try_from(i64::from(scheduled) + i64::from(balance) + excess)
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
            "NSM value balance test",
        )
        .map(|_| ())
        .map_err(|error| error.inner().clone())
}

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

    let network = zip234_network();
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

/// The finalized state rejects a checkpoint-verified block that makes the balance negative.
#[test]
fn finalized_state_rejects_a_block_that_makes_the_balance_negative() {
    let _init_guard = zakura_test::init();

    let network = zip234_network();
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

/// Check the unseeded balance before NU7 and after a permitted bonus claim.
#[test]
fn nsm_value_balance_matches_the_schedule() {
    let _init_guard = zakura_test::init();

    let network = zip234_network();
    let (mut state, parent) = state_below_start(&network);

    let baseline_info = state.db.block_info(Height(1).into()).unwrap();
    let baseline = i64::from(expected_issued_supply(Height(1), &network).unwrap())
        - i64::from(baseline_info.value_pools().issued_supply());

    // `state_below_start` mines dust coinbases, so every block so far under-claimed its
    // subsidy and the balance has been accumulating.
    for height in 0..START.0 {
        let height = Height(height);
        let block_info = state
            .db
            .block_info(height.into())
            .expect("every committed block has block info");

        let expected = expected_issued_supply(height, &network)
            .expect("the halving schedule is a valid amount at every height");
        let issued = block_info.value_pools().issued_supply();
        let derived = if height < Height(2) {
            0
        } else {
            i64::from(expected) - i64::from(issued) - baseline
        };

        assert_eq!(
            i64::from(block_info.value_pools().nsm_value_balance_amount()),
            derived,
            "the stored balance must exclude the pre-NU7 baseline at {height:?}",
        );
    }

    let balance_below_start = state.db.finalized_value_pool().nsm_value_balance_amount();
    assert!(
        balance_below_start > Amount::<NonNegative>::zero(),
        "under-claiming coinbases must leave a balance to reissue, got {balance_below_start:?}",
    );

    // The block at START claims its reissuance bonus, which draws the balance back down by
    // exactly the bonus.
    let block = permitted_start_block(&state, &network, &parent);
    commit(&mut state, &block).expect("the permitted claim commits");

    let expected = expected_issued_supply(START, &network).expect("valid expected issued supply");
    let pools = state.db.finalized_value_pool();
    assert_eq!(
        i64::from(pools.nsm_value_balance_amount()),
        i64::from(expected) - i64::from(pools.issued_supply()) - baseline,
        "the identity must still hold after a block that claims the reissuance bonus",
    );
}

#[test]
fn claim_fixture_obeys_subsidy_limit() {
    use zakura_chain::parameters::subsidy::block_subsidy;

    let _init_guard = zakura_test::init();
    let network = zip234_network();
    let (state, parent) = state_below_start(&network);
    let balance = state.db.finalized_value_pool().nsm_value_balance_amount();
    let allowed = block_subsidy(START, &network, Some(balance.constrain().unwrap())).unwrap();
    let block = permitted_start_block(&state, &network, &parent);
    let change = block
        .chain_value_pool_change(&network, &Default::default(), None)
        .unwrap();
    assert_eq!(
        change.total().unwrap(),
        allowed,
        "the acceptance fixture must claim the exact subsidy"
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]

    #[test]
    fn checkpoint_claim_sequences_preserve_balance(
        claims in proptest::collection::vec(0u64..=1_000_000, 1..32),
    ) {
        use zakura_chain::parameters::subsidy::{block_subsidy, halving_block_subsidy};

        let _init_guard = zakura_test::init();
        let network = zip234_network();
        let (mut state, mut parent) = state_below_start(&network);
        let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
        let mut issued = i64::from(state.db.finalized_value_pool().issued_supply());
        let mut expected = issued + i64::from(state.db.finalized_value_pool().nsm_value_balance_amount());

        for (index, claim_fraction) in claims.into_iter().enumerate() {
            let height = Height(START.0 + u32::try_from(index).unwrap());
            let before = expected - issued;
            let scheduled = i64::from(halving_block_subsidy(height, &network).unwrap());
            let bonus = (i128::from(before) * 4126 + 9_999_999_999) / 10_000_000_000;
            let allowed = block_subsidy(height, &network, Some(Amount::try_from(before).unwrap())).unwrap();
            proptest::prop_assert_eq!(i128::from(i64::from(allowed)), i128::from(scheduled) + bonus);
            let claimed = i64::try_from(i128::from(i64::from(allowed)) * i128::from(claim_fraction) / 1_000_000).unwrap();
            let block = child_block_with_history_commitment(
                &parent,
                vec![coinbase_tx(height, Amount::try_from(claimed).unwrap(), &address)],
                &network,
                &state.db.history_tree(),
            );
            commit(&mut state, &block).unwrap();
            expected += scheduled;
            issued += claimed;
            let pools = state.db.finalized_value_pool();
            proptest::prop_assert_eq!(i64::from(pools.issued_supply()), issued);
            proptest::prop_assert_eq!(i64::from(pools.nsm_value_balance_amount()), expected - issued);
            proptest::prop_assert!(expected >= issued);
            proptest::prop_assert_eq!(*state.db.block_info(height.into()).unwrap().value_pools(), pools);
            parent = block;
        }
    }
}

/// Construct an accounting fixture with the exact permitted subsidy.
/// Semantic subsidy validation lives in zakura-consensus block tests.
fn permitted_start_block(state: &FinalizedState, network: &Network, parent: &Block) -> Arc<Block> {
    use zakura_chain::parameters::subsidy::halving_block_subsidy;
    let balance = i64::from(state.db.finalized_value_pool().nsm_value_balance_amount());
    let bonus =
        i64::try_from((i128::from(balance) * 4126 + 9_999_999_999) / 10_000_000_000).unwrap();
    let allowed = i64::from(halving_block_subsidy(START, network).unwrap()) + bonus;
    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    child_block_with_history_commitment(
        parent,
        vec![coinbase_tx(
            START,
            Amount::try_from(allowed).unwrap(),
            &address,
        )],
        network,
        &state.db.history_tree(),
    )
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(std::env::var("NSM_HISTORY_CASES").ok().and_then(|value| value.parse().ok()).unwrap_or(64)))]

    /// Exercise accounting transitions through real databases. Partial claims intentionally
    /// bypass semantic subsidy validation; this property tests the state accounting layer.
    #[test]
    fn balance_rollback_replay_and_fork_equivalence(
        claims in proptest::collection::vec(0u32..=1_000_000, 2..12),
        target_offset in 0usize..12,
    ) {
        use zakura_chain::parameters::subsidy::halving_block_subsidy;
        use crate::{rollback_finalized_state, RollbackFinalizedStateOptions};
        let _guard = zakura_test::init();
        let network = zip234_network();
        let dir = tempfile::tempdir().unwrap();
        let config = Config { cache_dir: dir.path().to_owned(), ephemeral: false, ..Config::default() };
        let (mut state, mut parent) = state_below_start_with_config(&network, &config);
        let initial = state.db.finalized_value_pool();
        let mut snapshots = vec![initial];
        let mut blocks = vec![parent.clone()];
        let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
        let mut balance = i128::from(i64::from(initial.nsm_value_balance_amount()));
        let mut issued = i128::from(i64::from(initial.issued_supply()));
        for (index, fraction) in claims.iter().enumerate() {
            let height = Height(START.0 + u32::try_from(index).unwrap());
            let scheduled = i128::from(i64::from(halving_block_subsidy(height, &network).unwrap()));
            let bonus = (balance * 4126 + 9_999_999_999) / 10_000_000_000;
            let claim = (scheduled + bonus) * i128::from(*fraction) / 1_000_000;
            let block = child_block_with_history_commitment(&parent,
                vec![coinbase_tx(height, Amount::try_from(i64::try_from(claim).unwrap()).unwrap(), &address)],
                &network, &state.db.history_tree());
            commit(&mut state, &block).unwrap();
            balance += scheduled - claim;
            issued += claim;
            let pools = state.db.finalized_value_pool();
            proptest::prop_assert_eq!(i128::from(i64::from(pools.nsm_value_balance_amount())), balance);
            proptest::prop_assert_eq!(i128::from(i64::from(pools.issued_supply())), issued);
            snapshots.push(pools);
            blocks.push(block.clone());
            parent = block;
        }
        let target = target_offset % claims.len();
        let target_height = Height(START.0 - 1 + u32::try_from(target).unwrap());
        drop(state);
        rollback_finalized_state(config.clone(), &network, RollbackFinalizedStateOptions {
            target_height, keep_rolled_back_blocks: true, max_checkpoint_height: Some(Height(0)),
        }).unwrap();
        let mut state = FinalizedState::new(&config, &network).unwrap();
        proptest::prop_assert_eq!(state.db.finalized_value_pool(), snapshots[target]);
        for (index, block) in blocks.iter().enumerate().skip(target + 1) {
            commit(&mut state, block).unwrap();
            proptest::prop_assert_eq!(state.db.finalized_value_pool(), snapshots[index]);
            proptest::prop_assert_eq!(*state.db.block_info(block.coinbase_height().unwrap().into()).unwrap().value_pools(), snapshots[index]);
        }
        drop(state);
        rollback_finalized_state(config.clone(), &network, RollbackFinalizedStateOptions {
            target_height, keep_rolled_back_blocks: false, max_checkpoint_height: Some(Height(0)),
        }).unwrap();
        let mut state = FinalizedState::new(&config, &network).unwrap();
        let height = target_height.next().unwrap();
        let balance = i128::from(i64::from(snapshots[target].nsm_value_balance_amount()));
        let bonus = (balance * 4126 + 9_999_999_999) / 10_000_000_000;
        let scheduled = i128::from(i64::from(halving_block_subsidy(height, &network).unwrap()));
        let fork = child_block_with_history_commitment(&blocks[target],
            vec![coinbase_tx(height, Amount::try_from(i64::try_from(scheduled + bonus).unwrap()).unwrap(), &Address::from_script_hash(NetworkKind::Regtest, [0x43; 20]))],
            &network, &state.db.history_tree());
        proptest::prop_assert_ne!(fork.hash(), blocks[target + 1].hash());
        commit(&mut state, &fork).unwrap();
        proptest::prop_assert_eq!(i128::from(i64::from(state.db.finalized_value_pool().nsm_value_balance_amount())), balance - bonus);
        let (mut fresh, _) = state_below_start(&network);
        for block in blocks.iter().take(target + 1).skip(1) {
            commit(&mut fresh, block).unwrap();
        }
        commit(&mut fresh, &fork).unwrap();
        proptest::prop_assert_eq!(state.db.finalized_value_pool(), fresh.db.finalized_value_pool());
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]

    #[test]
    fn non_finalized_forks_keep_independent_balances(shortfall in 1i64..1_000_000) {
        let _guard = zakura_test::init();
        let network = zip234_network();
        let (mut state, parent) = state_below_start(&network);
        let before = state.db.finalized_value_pool();
        let balance = i64::from(before.nsm_value_balance_amount());
        let bonus = i64::try_from((i128::from(balance) * 4126 + 9_999_999_999) / 10_000_000_000).unwrap();
        // These fixtures isolate contextual accounting from semantic subsidy validation.
        let full = start_block(&state, &network, &parent, bonus - balance);
        let partial = start_block(&state, &network, &parent, bonus - balance - shortfall);
        let mut forks = NonFinalizedState::new(&network);
        forks.commit_new_chain(SemanticallyVerifiedBlock::from(full.clone()), &state.db).unwrap();
        forks.commit_new_chain(SemanticallyVerifiedBlock::from(partial.clone()), &state.db).unwrap();
        proptest::prop_assert_ne!(full.hash(), partial.hash());
        proptest::prop_assert_eq!(forks.chain_count(), 2);
        for (block, expected) in [(&full, balance - bonus), (&partial, balance - bonus + shortfall)] {
            let info = forks.chain_iter().find_map(|chain| chain.block_info(block.hash().into())).unwrap();
            proptest::prop_assert_eq!(i64::from(info.value_pools().nsm_value_balance_amount()), expected);
        }
        let over = start_block(&state, &network, &parent, 1);
        proptest::prop_assert!(forks.commit_new_chain(SemanticallyVerifiedBlock::from(over), &state.db).is_err());
        proptest::prop_assert_eq!(forks.chain_count(), 2);
        proptest::prop_assert_eq!(state.db.finalized_value_pool(), before);
        for (index, (parent, parent_balance)) in [(&full, balance - bonus), (&partial, balance - bonus + shortfall)].into_iter().enumerate() {
            let height = START.next().unwrap();
            let bonus = i64::try_from((i128::from(parent_balance) * 4126 + 9_999_999_999) / 10_000_000_000).unwrap();
            let scheduled = i64::from(zakura_chain::parameters::subsidy::halving_block_subsidy(height, &network).unwrap());
            let mut coinbase = (*parent.transactions[0]).clone();
            let Transaction::V5 { inputs, outputs, expiry_height, .. } = &mut coinbase else { unreachable!() };
            *expiry_height = height;
            let Input::Coinbase { height: input_height, .. } = &mut inputs[0] else { unreachable!() };
            *input_height = height;
            outputs[0].value = Amount::try_from(scheduled + bonus).unwrap();
            let history = forks.chain_iter().find_map(|chain| chain.history_tree(parent.hash().into())).unwrap();
            let child = child_block_with_history_commitment(parent, vec![Arc::new(coinbase)], &network, &history);
            forks.commit_block(SemanticallyVerifiedBlock::from(child.clone()), &state.db).unwrap();
            let info = forks.chain_iter().find_map(|chain| chain.block_info(child.hash().into())).unwrap();
            proptest::prop_assert_eq!(i64::from(info.value_pools().nsm_value_balance_amount()), parent_balance - bonus);
            if index == 0 {
                proptest::prop_assert_eq!(forks.best_chain().unwrap().non_finalized_tip_hash(), child.hash());
            }
        }
        let best = forks.best_chain().unwrap();
        let root_pools = *best.block_info(START.into()).unwrap().value_pools();
        let tip_before = best.non_finalized_tip_with_value_balance();
        state.commit_finalized_direct(forks.finalize(), None, None, "balance property").unwrap();
        proptest::prop_assert_eq!(state.db.finalized_value_pool(), root_pools);
        proptest::prop_assert_eq!(forks.chain_count(), 1);
        proptest::prop_assert_eq!(forks.best_chain().unwrap().non_finalized_tip_with_value_balance(), tip_before);
        state.commit_finalized_direct(forks.finalize(), None, None, "balance property").unwrap();
        proptest::prop_assert_eq!(state.db.finalized_value_pool(), tip_before.2);

    }
}

#[test]
fn startup_migration_failure_preserves_version_and_retry_matches_fresh_sync() {
    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{
            disk_format::{FromDisk, IntoDisk, RawBytes},
            DiskWriteBatch, ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE,
        },
    };
    let _guard = zakura_test::init();
    let network = zip234_network();
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        cache_dir: dir.path().to_owned(),
        ephemeral: false,
        ..Config::default()
    };
    let (mut state, parent) = state_below_start_with_config(&network, &config);
    let block = permitted_start_block(&state, &network, &parent);
    commit(&mut state, &block).unwrap();
    let expected = state.db.finalized_value_pool();
    let baseline_bytes = state.db.raw_block_info_cf().zs_get(&Height(1)).unwrap();
    let mut batch = DiskWriteBatch::new();
    for height in 0..=START.0 {
        let mut bytes = state
            .db
            .raw_block_info_cf()
            .zs_get(&Height(height))
            .unwrap()
            .as_bytes();
        bytes.drain(48..56);
        let _ = state
            .db
            .raw_block_info_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&Height(height), &RawBytes::from_bytes(bytes));
    }
    let _ = state
        .db
        .raw_block_info_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&Height(1), &RawBytes::from_bytes(vec![0; 51]));
    let tip_bytes = state
        .db
        .raw_chain_value_pools_cf()
        .zs_get(&())
        .unwrap()
        .as_bytes();
    let _ = state
        .db
        .raw_chain_value_pools_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&(), &RawBytes::from_bytes(&tip_bytes[..48]));
    state.db.write_batch(batch).unwrap();
    let old_version = semver::Version::new(28, 2, 0);
    state
        .db
        .update_format_version_on_disk(&old_version)
        .unwrap();
    drop(state);
    let old_path = config.db_path(STATE_DATABASE_KIND, 28, &network);
    let new_path = config.db_path(STATE_DATABASE_KIND, 29, &network);
    std::fs::create_dir_all(old_path.parent().unwrap()).unwrap();
    std::fs::rename(&new_path, &old_path).unwrap();
    assert!(FinalizedState::new(&config, &network).is_err());
    assert!(
        !old_path.exists(),
        "the upgrader moves the legacy database out of the old binary's path"
    );
    assert!(new_path.exists());
    let db = ZakuraDb::new(
        &config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        &network,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .unwrap();
    assert_eq!(db.format_version_on_disk().unwrap(), Some(old_version));
    let mut batch = DiskWriteBatch::new();
    let _ = db
        .raw_block_info_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&Height(1), &baseline_bytes);
    db.write_batch(batch).unwrap();
    drop(db);
    let upgraded = FinalizedState::new(&config, &network).unwrap();
    assert_eq!(upgraded.db.finalized_value_pool(), expected);
    assert_eq!(
        upgraded.db.format_version_on_disk().unwrap(),
        Some(state_database_format_version_in_code())
    );
    assert_eq!(
        *upgraded.db.block_info(START.into()).unwrap().value_pools(),
        expected
    );
}
