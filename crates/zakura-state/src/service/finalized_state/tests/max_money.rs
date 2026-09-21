//! Monetary supply boundaries across commit paths and recovery.

use std::sync::Arc;

use zakura_chain::{
    amount::{Amount, NonNegative, MAX_MONEY},
    block::{Block, Height},
    block_info::BlockInfo,
    parameters::{
        subsidy::block_subsidy,
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkKind, NetworkUpgrade,
    },
    transaction::{LockTime, Transaction},
    transparent::{Address, Input, Output},
    value_balance::{ValueBalance, ValueBalanceError},
};

use crate::{
    rollback_finalized_state,
    service::{
        finalized_state::{DiskWriteBatch, FinalizedState},
        non_finalized_state::NonFinalizedState,
    },
    CommitBlockError, Config, RollbackFinalizedStateOptions, SemanticallyVerifiedBlock,
    ValidateContextError,
};

use super::issuance_accounting::{child_block_with_history_commitment, commit};

fn network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(2),
            nu7: Some(3),
            ..Default::default()
        },
        test_nsm_reissuance_height: Some(Height(5)),
        initial_nsm_value_balance: Some(Amount::try_from(1_000_000_000_000i64).unwrap()),
        ..Default::default()
    })
}

fn child(state: &FinalizedState, network: &Network, parent: &Block, value: i64) -> Arc<Block> {
    let height = parent.coinbase_height().unwrap().next().unwrap();
    let address = Address::from_script_hash(NetworkKind::Regtest, [0x42; 20]);
    if height == Height(1) {
        return super::rollback::child_block(
            parent,
            vec![super::rollback::coinbase_tx(
                height,
                Amount::try_from(value).unwrap(),
                &address,
            )],
        );
    }
    let tx = Arc::new(Transaction::V5 {
        network_upgrade: NetworkUpgrade::current(network, height),
        lock_time: LockTime::unlocked(),
        expiry_height: height,
        inputs: vec![Input::Coinbase {
            height,
            data: vec![0; 8],
            sequence: u32::MAX,
        }],
        outputs: vec![Output::new(
            Amount::try_from(value).unwrap(),
            address.script(),
        )],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    });
    child_block_with_history_commitment(parent, vec![tx], network, &state.db.history_tree())
}

fn total_overflow(error: &ValidateContextError) -> bool {
    matches!(
        error,
        ValidateContextError::AddValuePool {
            value_balance_error: ValueBalanceError::Total(_),
            ..
        }
    )
}

/// Seed a contextual accounting fixture without generating 21 million ZEC in coinbases.
/// Preserve the real transparent UTXOs and NSM history; put the remaining supply in Sapling.
pub(super) fn set_supply(state: &FinalizedState, total: i64) -> ValueBalance<NonNegative> {
    let height = state.db.finalized_tip_height().unwrap();
    let mut pools = state.db.finalized_value_pool();
    pools.set_sapling_value_balance(ValueBalance::from_sapling_amount(
        Amount::try_from(total - i64::from(pools.transparent_amount())).unwrap(),
    ));
    let mut batch = DiskWriteBatch::new();
    let _ = state
        .db
        .chain_value_pools_cf()
        .with_batch_for_writing(&mut batch)
        .zs_insert(&(), &pools);
    // Give each historical snapshot the same reserve so restart validation sees
    // the same issuance since the pre-NU7 baseline.
    for h in 0..=height.0 {
        let info = state.db.block_info(Height(h).into()).unwrap();
        let mut historical = *info.value_pools();
        historical
            .set_sapling_value_balance(ValueBalance::from_sapling_amount(pools.sapling_amount()));
        let _ = state
            .db
            .block_info_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&Height(h), &BlockInfo::new(historical, info.size()));
    }
    state.db.write_batch(batch).unwrap();
    pools
}

#[test]
fn max_money_commit_rejection_is_atomic_across_activation_and_recovery() {
    let _guard = zakura_test::init();
    let network = network();
    for (height, headroom) in [Height(2), Height(3), Height(5)]
        .into_iter()
        .flat_map(|height| [-1i64, 0, 1].map(|headroom| (height, headroom)))
    {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            cache_dir: dir.path().to_owned(),
            ephemeral: false,
            ..Config::default()
        };
        let mut state = FinalizedState::new(&config, &network).unwrap();
        let mut parent = zakura_chain::block::genesis::regtest_genesis_block();
        commit(&mut state, &parent).unwrap();
        for _ in 1..height.0 {
            let block = child(&state, &network, &parent, 1);
            commit(&mut state, &block).unwrap();
            parent = block;
        }
        let nsm = state
            .db
            .finalized_value_pool()
            .nsm_value_balance_amount()
            .constrain()
            .unwrap();
        let payout = i64::from(block_subsidy(height, &network, Some(nsm)).unwrap());
        let before = set_supply(&state, MAX_MONEY - payout - headroom);
        // A negative headroom makes even the scheduled payout exceed MAX_MONEY.
        // These fixtures isolate contextual accounting from exact coinbase validation.
        let accepted = child(&state, &network, &parent, payout + headroom.min(0));
        let rejected = child(&state, &network, &parent, payout + headroom + 1);
        let mut forks = NonFinalizedState::new(&network);

        let error = forks
            .commit_new_chain(SemanticallyVerifiedBlock::from(rejected.clone()), &state.db)
            .unwrap_err();
        assert!(total_overflow(&error), "{error:?}");
        assert_eq!(forks.chain_count(), 0);
        let error = commit(&mut state, &rejected).unwrap_err();
        assert!(
            matches!(error, CommitBlockError::ValidateContextError(ref error) if total_overflow(error)),
            "{error:?}"
        );
        assert_eq!(state.db.finalized_tip_hash(), parent.hash());
        assert_eq!(state.db.finalized_value_pool(), before);
        assert!(state.db.block(rejected.hash().into()).is_none());
        assert!(state.db.block_info(height.into()).is_none());
        assert!(state
            .db
            .utxo(&zakura_chain::transparent::OutPoint {
                hash: rejected.transactions[0].hash(),
                index: 0,
            })
            .is_none());

        // Reopen immediately: a later successful write must not hide rejected data.
        drop(state);
        state = FinalizedState::new(&config, &network).unwrap();
        assert_eq!(state.db.finalized_tip_hash(), parent.hash());
        assert_eq!(state.db.finalized_value_pool(), before);
        assert!(state.db.block(rejected.hash().into()).is_none());
        assert!(state.db.block_info(height.into()).is_none());

        // The same state accepts a sibling that stays within the cap.
        forks
            .commit_new_chain(SemanticallyVerifiedBlock::from(accepted.clone()), &state.db)
            .unwrap();
        commit(&mut state, &accepted).unwrap();
        let after = state.db.finalized_value_pool();
        assert_eq!(
            i64::from(after.total().unwrap()),
            MAX_MONEY - headroom.max(0)
        );
        assert_eq!(
            *forks
                .best_chain()
                .unwrap()
                .block_info(height.into())
                .unwrap()
                .value_pools(),
            after
        );
        // Reject an extension of a populated chain, where undoing tentative changes
        // must preserve the existing chain and every cached accounting snapshot.
        let history = forks
            .best_chain()
            .unwrap()
            .history_tree(accepted.hash().into())
            .unwrap();
        let extension = child(&state, &network, &accepted, headroom.max(0) + 1);
        let extension = child_block_with_history_commitment(
            &accepted,
            extension.transactions.clone(),
            &network,
            &history,
        );
        let saved = forks.clone();
        let error = forks
            .commit_block(
                SemanticallyVerifiedBlock::from(extension.clone()),
                &state.db,
            )
            .unwrap_err();
        assert!(total_overflow(&error), "{error:?}");
        assert!(forks.eq_internal_state(&saved));
        assert!(!forks.any_chain_contains(&extension.hash()));

        drop(state);
        state = FinalizedState::new(&config, &network).unwrap();
        assert_eq!(state.db.finalized_value_pool(), after);
        assert_eq!(state.db.finalized_tip_hash(), accepted.hash());
        drop(state);

        rollback_finalized_state(
            config.clone(),
            &network,
            RollbackFinalizedStateOptions {
                target_height: height.previous().unwrap(),
                keep_rolled_back_blocks: false,
                max_checkpoint_height: Some(Height(0)),
            },
        )
        .unwrap();
        state = FinalizedState::new(&config, &network).unwrap();
        assert_eq!(state.db.finalized_value_pool(), before);
        let error = commit(&mut state, &rejected).unwrap_err();
        assert!(
            matches!(error, CommitBlockError::ValidateContextError(ref error) if total_overflow(error)),
            "{error:?}"
        );
        commit(&mut state, &accepted).unwrap();
        assert_eq!(state.db.finalized_value_pool(), after);
    }
}
