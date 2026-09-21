//! Contextual NSM histories with real UTXO spends. These fixtures bypass proof and
//! signature verification. Separate production acceptance tests cover transparent
//! signatures and shielded coinbase proofs, not valid shielded spends.

use std::sync::Arc;

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    parameters::{
        subsidy::halving_block_subsidy,
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkUpgrade,
    },
    transaction::{LockTime, Transaction},
    transparent::{Input, OutPoint, Output, Script},
    value_balance::ValueBalance,
};

use crate::{
    rollback_finalized_state,
    service::{finalized_state::FinalizedState, non_finalized_state::NonFinalizedState},
    Config, RollbackFinalizedStateOptions, SemanticallyVerifiedBlock,
};

use super::issuance_accounting::{child_block_with_history_commitment, commit};

const ACTIVATION: u32 = 104;
const FIRST_SPEND: u32 = ACTIVATION - 1;
const INPUT_VALUE: i64 = 50_000_000;

fn network(start: u32, seed: Option<i64>) -> Network {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(2),
            nu7: Some(ACTIVATION),
            ..Default::default()
        },
        nsm_reissuance_height: Some(Height(start)),
        initial_nsm_value_balance: seed.map(|value| Amount::try_from(value).unwrap()),
        ..Default::default()
    });
    assert!(network.should_allow_unshielded_coinbase_spends());
    network
}

fn transaction(
    network: &Network,
    height: Height,
    inputs: Vec<Input>,
    values: &[i64],
) -> Arc<Transaction> {
    if height == Height(1) {
        return Arc::new(Transaction::V4 {
            inputs,
            outputs: values
                .iter()
                .map(|value| Output::new(Amount::try_from(*value).unwrap(), Script::new(&[0x51])))
                .collect(),
            lock_time: LockTime::unlocked(),
            expiry_height: height,
            joinsplit_data: None,
            sapling_shielded_data: None,
        });
    }
    Arc::new(Transaction::V5 {
        network_upgrade: NetworkUpgrade::current(network, height),
        lock_time: LockTime::unlocked(),
        expiry_height: height,
        inputs,
        outputs: values
            .iter()
            .map(|value| Output::new(Amount::try_from(*value).unwrap(), Script::new(&[0x51])))
            .collect(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    })
}

fn coinbase(network: &Network, height: Height, values: &[i64]) -> Arc<Transaction> {
    transaction(
        network,
        height,
        vec![Input::Coinbase {
            height,
            data: vec![0; 8],
            sequence: u32::MAX,
        }],
        values,
    )
}

fn spend(network: &Network, height: Height, input: OutPoint, value: i64) -> Arc<Transaction> {
    transaction(
        network,
        height,
        vec![Input::PrevOut {
            outpoint: input,
            unlock_script: Script::new(&[]),
            sequence: u32::MAX,
        }],
        &[value],
    )
}

fn output(tx: &Transaction, index: u32) -> OutPoint {
    OutPoint {
        hash: tx.hash(),
        index,
    }
}

fn persistent(dir: &tempfile::TempDir) -> Config {
    Config {
        cache_dir: dir.path().to_owned(),
        ephemeral: false,
        ..Default::default()
    }
}

/// Build a mature funding prefix through checkpoint commits, retaining it for replay.
fn prefix(state: &mut FinalizedState, network: &Network) -> (Vec<Arc<Block>>, [OutPoint; 2], i128) {
    let genesis = zakura_chain::block::genesis::regtest_genesis_block();
    commit(state, &genesis).unwrap();
    let mut blocks = vec![genesis];
    let mut funding = None;
    let mut unclaimed = 0i128;
    for h in 1..FIRST_SPEND {
        let height = Height(h);
        let values = if h == 1 {
            vec![INPUT_VALUE, INPUT_VALUE]
        } else {
            vec![1]
        };
        let tx = coinbase(network, height, &values);
        if h == 1 {
            funding = Some([output(&tx, 0), output(&tx, 1)]);
        }
        let parent = blocks.last().unwrap();
        let block = if h == 1 {
            super::rollback::child_block(parent, vec![tx])
        } else {
            child_block_with_history_commitment(parent, vec![tx], network, &state.db.history_tree())
        };
        unclaimed += i128::from(i64::from(halving_block_subsidy(height, network).unwrap()))
            - i128::from(values.iter().sum::<i64>());
        commit(state, &block).unwrap();
        blocks.push(block);
    }
    (blocks, funding.unwrap(), unclaimed)
}

fn bonus(balance: i128) -> i128 {
    (balance * 1_375 + 9_999_999_999) / 10_000_000_000
}

fn run_history(start: u32, seed: Option<i64>, fee_pairs: &[[u32; 2]]) {
    let network = network(start, seed);
    let dir = tempfile::tempdir().unwrap();
    let config = persistent(&dir);
    let mut state = FinalizedState::new(&config, &network).unwrap();
    let (mut blocks, mut inputs, unclaimed) = prefix(&mut state, &network);
    let prefix_tip = blocks.last().unwrap().clone();
    let mut values = [INPUT_VALUE; 2];
    let mut forks = NonFinalizedState::new(&network);
    let mut expected_nsm = 0i128;
    let mut expected_total =
        i128::from(i64::from(state.db.finalized_value_pool().total().unwrap()));
    let mut snapshots = Vec::<ValueBalance<NonNegative>>::new();

    for (offset, fees) in fee_pairs.iter().enumerate() {
        let height = Height(FIRST_SPEND + u32::try_from(offset).unwrap());
        let scheduled = i128::from(i64::from(halving_block_subsidy(height, &network).unwrap()));
        let reissued = if height.0 >= start {
            bonus(expected_nsm)
        } else {
            0
        };
        let total_fees = i128::from(fees[0]) + i128::from(fees[1]);
        let recycled = if height.0 >= ACTIVATION {
            total_fees * 3 / 5
        } else {
            0
        };
        let claim = i64::try_from(scheduled + reissued + total_fees - recycled).unwrap();
        let mut txs = vec![coinbase(&network, height, &[claim])];
        for index in 0..2 {
            values[index] -= i64::from(fees[index]);
            txs.push(spend(&network, height, inputs[index], values[index]));
        }
        let parent = blocks.last().unwrap();
        let history = if offset == 0 {
            state.db.history_tree()
        } else {
            forks
                .best_chain()
                .unwrap()
                .history_tree(parent.hash().into())
                .unwrap()
        };
        let block = child_block_with_history_commitment(parent, txs, &network, &history);

        // Duplicate and value-creating spends must not consume the original inputs,
        // even after this chain has accumulated several non-finalized blocks.
        if offset > 0 {
            for duplicate in [false, true] {
                let mut bad_txs = block.transactions.clone();
                if duplicate {
                    bad_txs.push(bad_txs[1].clone());
                } else {
                    bad_txs[1] = spend(
                        &network,
                        height,
                        inputs[0],
                        values[0] + i64::from(fees[0]) + 1,
                    );
                }
                let invalid =
                    child_block_with_history_commitment(parent, bad_txs, &network, &history);
                let saved = forks.clone();
                assert!(forks
                    .commit_block(SemanticallyVerifiedBlock::from(invalid.clone()), &state.db)
                    .is_err());
                assert!(forks.eq_internal_state(&saved));
                assert!(!forks.any_chain_contains(&invalid.hash()));
            }
        }
        if offset == 0 {
            forks
                .commit_new_chain(SemanticallyVerifiedBlock::from(block.clone()), &state.db)
                .unwrap();
        } else {
            forks
                .commit_block(SemanticallyVerifiedBlock::from(block.clone()), &state.db)
                .unwrap();
        }
        if height.0 == ACTIVATION - 1 {
            expected_nsm = seed.map(i128::from).unwrap_or(unclaimed);
        } else if height.0 >= ACTIVATION {
            expected_nsm += recycled - reissued;
        }
        expected_total += scheduled + reissued - recycled;
        let pools = forks
            .best_chain()
            .unwrap()
            .non_finalized_tip_with_value_balance()
            .2;
        assert_eq!(
            i128::from(i64::from(pools.nsm_value_balance_amount())),
            expected_nsm,
            "NSM at {height:?}"
        );
        assert_eq!(
            i128::from(i64::from(pools.total().unwrap())),
            expected_total,
            "supply at {height:?}"
        );
        inputs = [
            output(&block.transactions[1], 0),
            output(&block.transactions[2], 0),
        ];
        for (input, value) in inputs.iter().zip(values) {
            assert_eq!(
                i64::from(forks.any_utxo(input).unwrap().output.value),
                value
            );
        }
        snapshots.push(pools);
        blocks.push(block);
    }

    // Finalize the real non-finalized chain, comparing each checkpoint-equivalent snapshot.
    for expected in &snapshots {
        state
            .commit_finalized_direct(forks.finalize(), None, None, "NSM spend history")
            .unwrap();
        assert_eq!(&state.db.finalized_value_pool(), expected);
    }
    drop(state);
    let state = FinalizedState::new(&config, &network).unwrap();
    assert_eq!(state.db.finalized_value_pool(), *snapshots.last().unwrap());
    for (input, value) in inputs.iter().zip(values) {
        assert_eq!(
            i64::from(state.db.utxo(input).unwrap().utxo.output.value),
            value
        );
    }
    drop(state);

    rollback_finalized_state(
        config.clone(),
        &network,
        RollbackFinalizedStateOptions {
            target_height: prefix_tip.coinbase_height().unwrap(),
            keep_rolled_back_blocks: false,
            max_checkpoint_height: Some(Height(0)),
        },
    )
    .unwrap();
    let mut replay = FinalizedState::new(&config, &network).unwrap();
    for (block, expected) in blocks
        .iter()
        .skip(usize::try_from(FIRST_SPEND).unwrap())
        .zip(&snapshots)
    {
        commit(&mut replay, block).unwrap();
        assert_eq!(&replay.db.finalized_value_pool(), expected);
    }
    assert_eq!(
        replay.db.finalized_tip_hash(),
        blocks.last().unwrap().hash()
    );
}

#[test]
fn nsm_release_spend_histories_cross_both_boundaries() {
    let _guard = zakura_test::init();
    // All residues mod five, aggregate-vs-per-transaction rounding, and a credit
    // large enough to change the next block's rounded bonus.
    let fees = [
        [0, 0],
        [1, 1],
        [2, 3],
        [10_001, 10_001],
        [7_000_000, 7_000_000],
        [0, 0],
        [4, 5],
        [1, 0],
    ];
    for start in [ACTIVATION, ACTIVATION + 2] {
        for seed in [
            None,
            Some(0),
            Some(1),
            Some(7_272_727),
            Some(7_272_728),
            Some(1_000_000_000_000),
        ] {
            run_history(start, seed, &fees);
        }
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(std::env::var("NSM_RELEASE_CASES").ok().map(|value| value.parse::<std::num::NonZeroU32>().expect("NSM_RELEASE_CASES must be a positive integer").get()).unwrap_or(8)))]
    #[test]
    fn nsm_release_generated_spend_histories(
        fees in proptest::collection::vec(proptest::array::uniform2(0u32..100_000), 5..12),
        seed in 0i64..1_000_000_000_000,
        delay in 0u32..4,
    ) {
        let _guard = zakura_test::init();
        run_history(ACTIVATION + delay, Some(seed), &fees);
    }
}

#[test]
fn nsm_release_competing_fee_histories_reorganize_and_reconsider() {
    let _guard = zakura_test::init();
    let network = network(ACTIVATION, Some(7_272_727));
    let mut state = FinalizedState::new(&Config::ephemeral(), &network).unwrap();
    let (prefix, funding, _) = prefix(&mut state, &network);
    let ancestor = prefix.last().unwrap();
    let mut forks = NonFinalizedState::new(&network);
    let mut branches = Vec::new();
    for fee in [1i64, 7_000_000] {
        let mut parent = ancestor.clone();
        let mut inputs = funding;
        let mut values = [INPUT_VALUE; 2];
        let mut balance = 0i128;
        let mut branch = Vec::new();
        for h in FIRST_SPEND..=ACTIVATION + 2 {
            let height = Height(h);
            let scheduled = i128::from(i64::from(halving_block_subsidy(height, &network).unwrap()));
            let reissued = if h >= ACTIVATION { bonus(balance) } else { 0 };
            let recycled = if h >= ACTIVATION {
                i128::from(fee * 2) * 3 / 5
            } else {
                0
            };
            let mut txs = vec![coinbase(
                &network,
                height,
                &[i64::try_from(scheduled + reissued + i128::from(fee * 2) - recycled).unwrap()],
            )];
            for index in 0..2 {
                values[index] -= fee;
                txs.push(spend(&network, height, inputs[index], values[index]));
            }
            let history = if h == FIRST_SPEND {
                state.db.history_tree()
            } else {
                forks
                    .chain_iter()
                    .find_map(|chain| chain.history_tree(parent.hash().into()))
                    .unwrap()
            };
            let block = child_block_with_history_commitment(&parent, txs, &network, &history);
            if h == FIRST_SPEND {
                forks
                    .commit_new_chain(SemanticallyVerifiedBlock::from(block.clone()), &state.db)
                    .unwrap();
            } else {
                forks
                    .commit_block(SemanticallyVerifiedBlock::from(block.clone()), &state.db)
                    .unwrap();
            }
            balance = if h == FIRST_SPEND {
                7_272_727
            } else {
                balance + recycled - reissued
            };
            let pools = *forks
                .chain_iter()
                .find_map(|chain| chain.block_info(block.hash().into()))
                .unwrap()
                .value_pools();
            assert_eq!(
                i128::from(i64::from(pools.nsm_value_balance_amount())),
                balance
            );
            inputs = [
                output(&block.transactions[1], 0),
                output(&block.transactions[2], 0),
            ];
            branch.push((block.clone(), pools));
            parent = block;
        }
        branches.push(branch);
    }
    assert_ne!(
        bonus(i128::from(i64::from(
            branches[0].last().unwrap().1.nsm_value_balance_amount()
        ))),
        bonus(i128::from(i64::from(
            branches[1].last().unwrap().1.nsm_value_balance_amount()
        )))
    );
    assert_eq!(forks.chain_count(), 2);
    // Removing either root crosses NU7 and reissuance and must select the other history.
    for branch_index in [0usize, 1] {
        let root = branches[branch_index][0].0.hash();
        forks.invalidate_block(root).unwrap();
        let alternative = branches[1 - branch_index].last().unwrap();
        assert_eq!(forks.best_tip().unwrap().1, alternative.0.hash());
        assert_eq!(
            forks
                .best_chain()
                .unwrap()
                .non_finalized_tip_with_value_balance()
                .2,
            alternative.1
        );
        forks.reconsider_block(root, &state.db).unwrap();
        assert_eq!(forks.chain_count(), 2);
    }
    // Pin the high-fee branch by removing its competitor, then compare finalization
    // against an independent checkpoint replay of the complete winning history.
    forks.invalidate_block(branches[0][0].0.hash()).unwrap();
    let mut fresh = FinalizedState::new(&Config::ephemeral(), &network).unwrap();
    for block in &prefix {
        commit(&mut fresh, block).unwrap();
    }
    for (block, expected) in &branches[1] {
        state
            .commit_finalized_direct(forks.finalize(), None, None, "NSM fork release test")
            .unwrap();
        commit(&mut fresh, block).unwrap();
        assert_eq!(state.db.finalized_value_pool(), *expected);
        assert_eq!(
            state.db.finalized_value_pool(),
            fresh.db.finalized_value_pool()
        );
        assert_eq!(state.db.finalized_tip_hash(), fresh.db.finalized_tip_hash());
    }
}

/// Fake shielded proofs isolate contextual atomicity; a successful sibling proves
/// rejection reached the monetary check, rather than failing on its anchor.
#[test]
fn nsm_release_cap_rejection_preserves_spends_nullifiers_and_trees() {
    use zakura_chain::{
        amount::MAX_MONEY, transaction::arbitrary::fake_v6_with_orchard_and_ironwood_actions,
        value_balance::ValueBalanceError,
    };
    let _guard = zakura_test::init();
    let network = network(ACTIVATION, Some(1_000_000_000_000));
    let dir = tempfile::tempdir().unwrap();
    let config = persistent(&dir);
    let mut state = FinalizedState::new(&config, &network).unwrap();
    let (blocks, funding, _) = prefix(&mut state, &network);
    super::max_money::set_supply(&state, MAX_MONEY);
    let parent = blocks.last().unwrap();
    let root = child_block_with_history_commitment(
        parent,
        vec![coinbase(&network, Height(FIRST_SPEND), &[0])],
        &network,
        &state.db.history_tree(),
    );
    let mut forks = NonFinalizedState::new(&network);
    forks
        .commit_new_chain(SemanticallyVerifiedBlock::from(root.clone()), &state.db)
        .unwrap();
    commit(&mut state, &root).unwrap();
    let before = state.db.finalized_value_pool();
    let tree = state.db.ironwood_tree_for_tip().root();
    let history = forks
        .best_chain()
        .unwrap()
        .history_tree(root.hash().into())
        .unwrap();
    let mut transfer = fake_v6_with_orchard_and_ironwood_actions(NetworkUpgrade::Nu7, 0, 1);
    let Transaction::V6 {
        expiry_height,
        inputs,
        outputs,
        ironwood_shielded_data,
        ..
    } = Arc::make_mut(&mut transfer)
    else {
        unreachable!()
    };
    *expiry_height = Height(ACTIVATION);
    *inputs = vec![Input::PrevOut {
        outpoint: funding[0],
        unlock_script: Script::new(&[]),
        sequence: u32::MAX,
    }];
    *outputs = vec![Output::new(
        Amount::try_from(INPUT_VALUE - 10).unwrap(),
        Script::new(&[0x51]),
    )];
    let shielded = ironwood_shielded_data.as_mut().unwrap();
    shielded.shared_anchor = tree;
    shielded.proof =
        zakura_chain::primitives::Halo2Proof(vec![0; orchard::Proof::expected_proof_size(1)]);
    shielded.value_balance = Amount::try_from(-10).unwrap();
    let nullifier = *transfer.ironwood_nullifiers().next().unwrap();
    let outpoint = output(&transfer, 0);
    let invalid = child_block_with_history_commitment(
        &root,
        vec![
            coinbase(&network, Height(ACTIVATION), &[1]),
            transfer.clone(),
        ],
        &network,
        &history,
    );
    let saved = forks.clone();
    let error = forks
        .commit_block(SemanticallyVerifiedBlock::from(invalid.clone()), &state.db)
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ValidateContextError::AddValuePool {
                value_balance_error: ValueBalanceError::Total(_),
                ..
            }
        ),
        "{error:?}"
    );
    assert!(forks.eq_internal_state(&saved));
    assert!(commit(&mut state, &invalid).is_err());
    drop(state);
    state = FinalizedState::new(&config, &network).unwrap();
    assert_eq!(state.db.finalized_value_pool(), before);
    assert_eq!(state.db.ironwood_tree_for_tip().root(), tree);
    assert!(!state.db.contains_ironwood_nullifier(&nullifier));
    assert!(state.db.utxo(&funding[0]).is_some());
    assert!(state.db.utxo(&outpoint).is_none());
    assert!(state.db.block(invalid.hash().into()).is_none());
    let valid = child_block_with_history_commitment(
        &root,
        vec![coinbase(&network, Height(ACTIVATION), &[0]), transfer],
        &network,
        &history,
    );
    forks
        .commit_block(SemanticallyVerifiedBlock::from(valid.clone()), &state.db)
        .unwrap();
    commit(&mut state, &valid).unwrap();
    assert!(state.db.utxo(&funding[0]).is_none());
    assert!(state.db.utxo(&outpoint).is_some());
    assert!(state.db.contains_ironwood_nullifier(&nullifier));
    assert_ne!(state.db.ironwood_tree_for_tip().root(), tree);
    assert_eq!(
        forks
            .best_chain()
            .unwrap()
            .ironwood_note_commitment_tree_for_tip()
            .root(),
        state.db.ironwood_tree_for_tip().root()
    );
    assert_eq!(
        forks
            .best_chain()
            .unwrap()
            .non_finalized_tip_with_value_balance()
            .2,
        state.db.finalized_value_pool()
    );
}
