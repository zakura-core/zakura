//! State histories for the signed issuance counter, with and without ZIP 234 reissuance.

use std::sync::Arc;

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    parameters::{
        subsidy::{
            expected_issued_supply, is_zip234_active, BLOCK_SUBSIDY_FRACTION_DENOMINATOR,
            BLOCK_SUBSIDY_FRACTION_NUMERATOR,
        },
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
    CommitBlockError, Config, SemanticallyVerifiedBlock,
};

use super::rollback::{child_block, coinbase_tx};

/// Include transaction IDs so competing fixtures have distinct block hashes.
pub(super) fn child_block_with_history_commitment(
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

/// The accounting fixture height on the test network, and its ZIP 234 start height when
/// reissuance is on.
pub(super) const START: Height = Height(3);

/// Returns a Regtest network with NU7 at height 2 and accounting fixtures from [`START`].
///
/// If `reissuance` is true, ZIP 234 reissuance starts at [`START`].
pub(super) fn accounting_network(reissuance: bool) -> Network {
    Network::new_regtest(RegtestParameters {
        // Regtest activates Heartwood at height 1, where the block commitment is reserved.
        // NU7 activates after it.
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(2),
            ..Default::default()
        },
        nsm_reissuance_height: reissuance.then_some(START),
        ..Default::default()
    })
}

/// Returns the ZIP 234 reissuance bonus for a block at `height` whose parent left
/// `deficit`, or zero where reissuance is inactive.
///
/// This oracle restates `ceil(deficit * BLOCK_SUBSIDY_FRACTION)` independently of
/// `block_subsidy`.
fn reissuance_bonus(network: &Network, height: Height, deficit: i128) -> i128 {
    if !is_zip234_active(network, height) {
        return 0;
    }
    let numerator = i128::try_from(BLOCK_SUBSIDY_FRACTION_NUMERATOR).unwrap();
    let denominator = i128::try_from(BLOCK_SUBSIDY_FRACTION_DENOMINATOR).unwrap();
    (deficit * numerator + denominator - 1) / denominator
}

/// Returns a finalized state with Regtest blocks up to the parent of [`START`], and that
/// parent block.
pub(super) fn state_below_start(network: &Network) -> (FinalizedState, Arc<Block>) {
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

/// Returns an accounting fixture that overdraws the eligible deficit by `excess` zatoshi.
pub(super) fn start_block(
    state: &FinalizedState,
    network: &Network,
    parent: &Block,
    excess: i64,
) -> Arc<Block> {
    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    let scheduled =
        zakura_chain::parameters::subsidy::halving_block_subsidy(START, network).unwrap();
    let deficit = state.db.finalized_value_pool().nsm_value_balance_amount();
    let coinbase_value =
        Amount::<NonNegative>::try_from(i64::from(scheduled) + i64::from(deficit) + excess)
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
pub(super) fn commit(
    state: &mut FinalizedState,
    block: &Arc<Block>,
) -> Result<(), CommitBlockError> {
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

/// Check historical exclusion and the first permitted claim after NU7.
#[test]
fn nsm_value_balance_matches_the_schedule() {
    let _init_guard = zakura_test::init();

    for reissuance in [false, true] {
        nsm_value_balance_matches_the_schedule_on(&accounting_network(reissuance));
    }
}

fn nsm_value_balance_matches_the_schedule_on(network: &Network) {
    let network = network.clone();
    let (mut state, parent) = state_below_start(&network);

    let baseline_info = state.db.block_info(Height(1).into()).unwrap();
    let baseline = i64::from(expected_issued_supply(Height(1), &network).unwrap())
        - i64::from(baseline_info.value_pools().issued_supply());

    // `state_below_start` mines dust coinbases, so every block so far under-claimed its
    // subsidy and the deficit has been accumulating.
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
            "the stored deficit must exclude the pre-NU7 baseline at {height:?}",
        );
    }

    let deficit_below_start = state.db.finalized_value_pool().nsm_value_balance_amount();
    assert!(
        deficit_below_start > Amount::<NonNegative>::zero(),
        "under-claiming coinbases must leave a deficit to reissue, got {deficit_below_start:?}",
    );

    // A full permitted claim leaves the deficit lower by exactly the reissuance bonus.
    let block = permitted_start_block(&state, &network, &parent);
    commit(&mut state, &block).expect("the permitted claim commits");

    let expected = expected_issued_supply(START, &network).expect("valid expected issued supply");
    let pools = state.db.finalized_value_pool();
    assert_eq!(
        i64::from(pools.nsm_value_balance_amount()),
        i64::from(expected) - i64::from(pools.issued_supply()) - baseline,
        "the identity must still hold after a block that claims the permitted subsidy",
    );
}

#[test]
fn claim_fixture_obeys_subsidy_limit() {
    use zakura_chain::parameters::subsidy::block_subsidy;

    let _init_guard = zakura_test::init();
    for reissuance in [false, true] {
        let network = accounting_network(reissuance);
        let (state, parent) = state_below_start(&network);
        let deficit = state.db.finalized_value_pool().nsm_value_balance_amount();
        let allowed = block_subsidy(START, &network, Some(deficit.constrain().unwrap())).unwrap();
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
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]

    #[test]
    fn checkpoint_claim_sequences_preserve_deficit(
        claims in proptest::collection::vec(0u64..=1_000_000, 1..32),
        reissuance in proptest::bool::ANY,
    ) {
        use zakura_chain::parameters::subsidy::{block_subsidy, halving_block_subsidy};

        let _init_guard = zakura_test::init();
        let network = accounting_network(reissuance);
        let (mut state, mut parent) = state_below_start(&network);
        let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
        let mut issued = i64::from(state.db.finalized_value_pool().issued_supply());
        let mut expected = issued + i64::from(state.db.finalized_value_pool().nsm_value_balance_amount());

        for (index, claim_fraction) in claims.into_iter().enumerate() {
            let height = Height(START.0 + u32::try_from(index).unwrap());
            let before = expected - issued;
            let scheduled = i64::from(halving_block_subsidy(height, &network).unwrap());
            let bonus = reissuance_bonus(&network, height, i128::from(before));
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
pub(super) fn permitted_start_block(
    state: &FinalizedState,
    network: &Network,
    parent: &Block,
) -> Arc<Block> {
    use zakura_chain::parameters::subsidy::halving_block_subsidy;

    let deficit = i64::from(state.db.finalized_value_pool().nsm_value_balance_amount());
    let bonus = i64::try_from(reissuance_bonus(network, START, i128::from(deficit))).unwrap();
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
    fn deficit_rollback_replay_and_fork_equivalence(
        claims in proptest::collection::vec(0u32..=1_000_000, 2..12),
        target_offset in 0usize..12,
        reissuance in proptest::bool::ANY,
    ) {
        use zakura_chain::parameters::subsidy::halving_block_subsidy;
        use crate::{rollback_finalized_state, RollbackFinalizedStateOptions};
        let _guard = zakura_test::init();
        let network = accounting_network(reissuance);
        let dir = tempfile::tempdir().unwrap();
        let config = Config { cache_dir: dir.path().to_owned(), ephemeral: false, ..Config::default() };
        let (mut state, mut parent) = state_below_start_with_config(&network, &config);
        let initial = state.db.finalized_value_pool();
        let mut snapshots = vec![initial];
        let mut blocks = vec![parent.clone()];
        let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
        let mut deficit = i128::from(i64::from(initial.nsm_value_balance_amount()));
        let mut issued = i128::from(i64::from(initial.issued_supply()));
        for (index, fraction) in claims.iter().enumerate() {
            let height = Height(START.0 + u32::try_from(index).unwrap());
            let scheduled = i128::from(i64::from(halving_block_subsidy(height, &network).unwrap()));
            let bonus = reissuance_bonus(&network, height, deficit);
            let claim = (scheduled + bonus) * i128::from(*fraction) / 1_000_000;
            let block = child_block_with_history_commitment(&parent,
                vec![coinbase_tx(height, Amount::try_from(i64::try_from(claim).unwrap()).unwrap(), &address)],
                &network, &state.db.history_tree());
            commit(&mut state, &block).unwrap();
            deficit += scheduled - claim;
            issued += claim;
            let pools = state.db.finalized_value_pool();
            proptest::prop_assert_eq!(i128::from(i64::from(pools.nsm_value_balance_amount())), deficit);
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
        let deficit = i128::from(i64::from(snapshots[target].nsm_value_balance_amount()));
        let bonus = reissuance_bonus(&network, height, deficit);
        let scheduled = i128::from(i64::from(halving_block_subsidy(height, &network).unwrap()));
        let fork = child_block_with_history_commitment(&blocks[target],
            vec![coinbase_tx(height, Amount::try_from(i64::try_from(scheduled + bonus).unwrap()).unwrap(), &Address::from_script_hash(NetworkKind::Regtest, [0x43; 20]))],
            &network, &state.db.history_tree());
        proptest::prop_assert_ne!(fork.hash(), blocks[target + 1].hash());
        commit(&mut state, &fork).unwrap();
        proptest::prop_assert_eq!(i128::from(i64::from(state.db.finalized_value_pool().nsm_value_balance_amount())), deficit - bonus);
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
    fn non_finalized_forks_keep_independent_deficits(
        shortfall in 1i64..1_000_000,
        reissuance in proptest::bool::ANY,
    ) {
        let _guard = zakura_test::init();
        let network = accounting_network(reissuance);
        let (mut state, parent) = state_below_start(&network);
        let before = state.db.finalized_value_pool();
        let deficit = i64::from(before.nsm_value_balance_amount());
        let bonus = i64::try_from(reissuance_bonus(&network, START, i128::from(deficit))).unwrap();
        // These fixtures isolate contextual accounting from semantic subsidy validation.
        let full = start_block(&state, &network, &parent, bonus - deficit);
        let partial = start_block(&state, &network, &parent, bonus - deficit - shortfall);
        let mut forks = NonFinalizedState::new(&network);
        forks.commit_new_chain(SemanticallyVerifiedBlock::from(full.clone()), &state.db).unwrap();
        forks.commit_new_chain(SemanticallyVerifiedBlock::from(partial.clone()), &state.db).unwrap();
        proptest::prop_assert_ne!(full.hash(), partial.hash());
        proptest::prop_assert_eq!(forks.chain_count(), 2);
        for (block, expected) in [(&full, deficit - bonus), (&partial, deficit - bonus + shortfall)] {
            let info = forks.chain_iter().find_map(|chain| chain.block_info(block.hash().into())).unwrap();
            proptest::prop_assert_eq!(i64::from(info.value_pools().nsm_value_balance_amount()), expected);
        }
        // Reissuance rejects a block that leaves the deficit negative.
        let over = start_block(&state, &network, &parent, 1);
        proptest::prop_assert_eq!(
            forks.commit_new_chain(SemanticallyVerifiedBlock::from(over), &state.db).is_err(),
            reissuance,
        );
        if reissuance {
            proptest::prop_assert_eq!(forks.chain_count(), 2);
        } else {
            proptest::prop_assert_eq!(forks.chain_count(), 3);
            forks = NonFinalizedState::new(&network);
            forks.commit_new_chain(SemanticallyVerifiedBlock::from(full.clone()), &state.db).unwrap();
            forks.commit_new_chain(SemanticallyVerifiedBlock::from(partial.clone()), &state.db).unwrap();
        }
        proptest::prop_assert_eq!(state.db.finalized_value_pool(), before);
        for (index, (parent, parent_deficit)) in [(&full, deficit - bonus), (&partial, deficit - bonus + shortfall)].into_iter().enumerate() {
            let height = START.next().unwrap();
            let bonus = i64::try_from(reissuance_bonus(&network, height, i128::from(parent_deficit))).unwrap();
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
            proptest::prop_assert_eq!(i64::from(info.value_pools().nsm_value_balance_amount()), parent_deficit - bonus);
            if index == 0 {
                proptest::prop_assert_eq!(forks.best_chain().unwrap().non_finalized_tip_hash(), child.hash());
            }
        }
        let best = forks.best_chain().unwrap();
        let root_pools = *best.block_info(START.into()).unwrap().value_pools();
        let tip_before = best.non_finalized_tip_with_value_balance();
        state.commit_finalized_direct(forks.finalize(), None, None, "deficit property").unwrap();
        proptest::prop_assert_eq!(state.db.finalized_value_pool(), root_pools);
        proptest::prop_assert_eq!(forks.chain_count(), 1);
        proptest::prop_assert_eq!(forks.best_chain().unwrap().non_finalized_tip_with_value_balance(), tip_before);
        state.commit_finalized_direct(forks.finalize(), None, None, "deficit property").unwrap();
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
    let network = accounting_network(false);
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

#[test]
fn signed_deficit_survives_both_commit_paths_before_reissuance() {
    let _guard = zakura_test::init();
    let network = accounting_network(false);
    for excess in [-1, 0, 1, 1_000_000] {
        let (mut state, parent) = state_below_start(&network);
        let block = start_block(&state, &network, &parent, excess);
        let mut forks = NonFinalizedState::new(&network);
        forks
            .commit_new_chain(SemanticallyVerifiedBlock::from(block.clone()), &state.db)
            .unwrap();
        let fork_pools = *forks
            .best_chain()
            .unwrap()
            .block_info(block.hash().into())
            .unwrap()
            .value_pools();
        commit(&mut state, &block).unwrap();
        assert_eq!(state.db.finalized_value_pool(), fork_pools);
        assert_eq!(i64::from(fork_pools.nsm_value_balance_amount()), -excess);
    }
}

#[test]
fn deficit_validation_accepts_block_info_rows_with_appended_fields() {
    use crate::service::finalized_state::{
        disk_format::{
            upgrade::{nsm_value_balance_pool, DiskFormatUpgrade},
            FromDisk, IntoDisk, RawBytes,
        },
        DiskWriteBatch,
    };

    let _guard = zakura_test::init();
    let network = accounting_network(false);
    let (mut state, parent) = state_below_start(&network);
    let block = permitted_start_block(&state, &network, &parent);
    commit(&mut state, &block).unwrap();

    // A later format can append fields to BlockInfo, which a downgrade to this format
    // must still validate.
    let mut bytes = state
        .db
        .raw_block_info_cf()
        .zs_get(&START)
        .unwrap()
        .as_bytes();
    bytes.extend_from_slice(&[0xa5; 4]);
    let mut batch = DiskWriteBatch::new();
    let _ = state
        .db
        .raw_block_info_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&START, &RawBytes::from_bytes(bytes));
    state.db.write_batch(batch).unwrap();

    let (_cancel_sender, cancel_receiver) = crossbeam_channel::bounded(1);
    assert_eq!(
        nsm_value_balance_pool::Upgrade
            .validate(&state.db, &cancel_receiver)
            .unwrap(),
        Ok(())
    );
}
