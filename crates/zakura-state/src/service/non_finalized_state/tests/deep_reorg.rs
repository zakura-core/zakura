//! Deep rollback of mixed transparent, Sapling, and Orchard transactions.
//!
//! These state-level fixtures reuse historical shielded descriptions with new
//! nullifiers. Their proofs and signatures are not re-created: semantic proof
//! verification is outside this test. Blocks enter through [`Chain::push`],
//! with real transparent values and pool deltas, and forks through [`Chain::fork`].

use std::{collections::HashMap, sync::Arc};

use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    history_tree::HistoryTree,
    orchard,
    parameters::Network,
    sapling::TransferData,
    serialization::ZcashDeserializeInto,
    transaction::{self, LockTime, Transaction},
    transparent,
    value_balance::ValueBalance,
};

use crate::{
    arbitrary::Prepare,
    constants::MAX_BLOCK_REORG_HEIGHT,
    service::{
        check,
        finalized_state::FinalizedState,
        non_finalized_state::{Chain, NonFinalizedState},
    },
    tests::{copy_block_fixture, FakeChainHelper},
    Config, ContextuallyVerifiedBlock, ValidateContextError,
};

const REORG_DEPTH: u32 = 100;
const FINALIZED_BEFORE_FORK: u32 = 64;
const TRANSFER_VALUE: i64 = 1_000;
const TRANSPARENT_TX_INDEX: usize = 1;
const SHIELDED_TX_INDEX: usize = 2;

#[test]
fn deep_reorg_with_transparent_and_shielded_transactions() {
    let _guard = zakura_test::init();

    for skip_address_index in [false, true] {
        let finalized = FinalizedState::new(&Config::ephemeral(), &Network::Mainnet).unwrap();
        let (initial, template, shielded) = fixtures(skip_address_index);
        let common_blocks = MAX_BLOCK_REORG_HEIGHT - REORG_DEPTH;
        let mut source = initial.clone();
        let mut rebuilt = initial;
        let mut parent = template;
        let mut abandoned = Vec::new();
        let mut ancestor = None;

        for seed in 0..MAX_BLOCK_REORG_HEIGHT {
            let (block, contextual) = mixed_child(&parent, &shielded, seed, 0);
            if seed < common_blocks {
                rebuilt = rebuilt.push(contextual.clone()).unwrap();
                ancestor = Some(block.clone());
            } else {
                abandoned.push(contextual.clone());
            }
            source = source.push(contextual).unwrap();
            parent = block;
        }

        // Advance both roots through a full sweep cycle before the deep fork.
        // The old published state must retain the finalized blocks as well.
        let before_finalization = source.clone();
        for _ in 0..FINALIZED_BEFORE_FORK {
            source.pop_root();
            rebuilt.pop_root();
        }
        let reader = source.clone();
        let mut parent = ancestor.unwrap();
        let mut fork = source.fork(parent.hash()).unwrap();
        assert_eq!(
            source.len() - fork.len(),
            usize::try_from(REORG_DEPTH).unwrap()
        );
        assert!(fork.eq_internal_state(&rebuilt));
        let common_tip = rebuilt.tip_block().unwrap().clone();
        assert_live_double_spends_rejected(&fork, &rebuilt, &shielded, &common_tip, &finalized);

        for record in &abandoned {
            assert_membership(&reader, record, true);
            assert_membership(&fork, record, false);
        }

        // Reuse each abandoned transparent input and shielded nullifier in a
        // different transaction on the replacement branch. An extra block gives
        // the replacement strictly more work than the abandoned branch.
        for seed in common_blocks..=MAX_BLOCK_REORG_HEIGHT {
            let (block, contextual) = mixed_child(&parent, &shielded, seed, 1);
            fork = fork.push(contextual.clone()).unwrap();
            rebuilt = rebuilt.push(contextual.clone()).unwrap();
            assert_membership(&fork, &contextual, true);
            parent = block;
        }
        assert!(fork.eq_internal_state(&rebuilt));
        assert_live_double_spends_rejected(&fork, &rebuilt, &shielded, &common_tip, &finalized);
        assert_live_double_spends_rejected(
            &fork,
            &rebuilt,
            &shielded,
            fork.tip_block().unwrap(),
            &finalized,
        );
        assert!(source.eq_internal_state(&reader));
        for record in &abandoned {
            assert_membership(&reader, record, true);
            assert!(!fork.contains_transaction(&record.transaction_hashes[TRANSPARENT_TX_INDEX]));
            assert!(!fork.contains_transaction(&record.transaction_hashes[SHIELDED_TX_INDEX]));
        }

        let mut state = NonFinalizedState::new(&Network::Mainnet);
        state.insert_test_chain(Arc::new(source));
        state.insert_test_chain(Arc::new(fork));
        assert_eq!(state.best_tip().unwrap().1, parent.hash());
        let published = state.clone();

        // Finalize the shared prefix and the first replacement block. The old
        // branch is now orphaned, while retained snapshots still own both tips.
        for _ in FINALIZED_BEFORE_FORK..=common_blocks {
            state.finalize();
            rebuilt.pop_root();
        }
        assert_eq!(state.chain_count(), 1);
        assert!(state.best_chain().unwrap().eq_internal_state(&rebuilt));
        assert_eq!(published.chain_count(), 2);
        assert_eq!(published.best_tip().unwrap().1, parent.hash());
        assert_eq!(
            before_finalization.len(),
            usize::try_from(MAX_BLOCK_REORG_HEIGHT).unwrap()
        );
        for record in &abandoned {
            assert_membership(&reader, record, true);
            assert_membership(&before_finalization, record, true);
        }
    }
}

/// Rejects conflicts with a live block while preserving independently built state.
fn assert_live_double_spends_rejected(
    chain: &Chain,
    expected: &Chain,
    shielded: &Transaction,
    live: &ContextuallyVerifiedBlock,
    finalized: &FinalizedState,
) {
    let parent = copy_block_fixture(&chain.tip_block().unwrap().block);
    let (block, _) = mixed_child(&parent, shielded, MAX_BLOCK_REORG_HEIGHT + 1, 2);
    let mut valid = Block::clone(&block);
    let unspent = transparent::OutPoint {
        hash: live.transaction_hashes[TRANSPARENT_TX_INDEX],
        index: 0,
    };
    let Transaction::V4 { inputs, .. } =
        Arc::make_mut(&mut valid.transactions[TRANSPARENT_TX_INDEX])
    else {
        panic!("the fixture has a V4 transparent transfer");
    };
    inputs[0] = transparent::Input::PrevOut {
        outpoint: unspent,
        unlock_script: transparent::Script::new(&[]),
        sequence: u32::MAX,
    };
    set_merkle_root(&mut valid);
    let prepared = Arc::new(valid.clone()).prepare();
    let spent = check::utxo::transparent_spend(&prepared, Some(chain), &finalized.db).unwrap();
    let contextual = ContextuallyVerifiedBlock::with_block_and_spent_utxos(
        &Network::Mainnet,
        prepared,
        spent.clone(),
    )
    .unwrap();
    assert!(chain.clone().push(contextual).is_ok());
    assert!(chain.eq_internal_state(expected));

    // Use the actual commit boundary for transparent rejection, before a
    // supplied UTXO map could bypass the spent-output check in the service.
    let mut duplicate = valid.clone();
    let Transaction::V4 { inputs, .. } =
        Arc::make_mut(&mut duplicate.transactions[TRANSPARENT_TX_INDEX])
    else {
        unreachable!("the valid candidate has a V4 transparent transfer");
    };
    let outpoint = *live.spent_outputs.keys().next().unwrap();
    inputs[0] = transparent::Input::PrevOut {
        outpoint,
        unlock_script: transparent::Script::new(&[]),
        sequence: u32::MAX,
    };
    set_merkle_root(&mut duplicate);
    let mut state = NonFinalizedState::new(&Network::Mainnet);
    state.insert_test_chain(Arc::new(chain.clone()));
    let reader = state.clone();
    assert_eq!(
        state.commit_block(Arc::new(duplicate).prepare(), &finalized.db),
        Err(ValidateContextError::DuplicateTransparentSpend {
            outpoint,
            location: "the non-finalized chain",
        }),
    );
    assert_eq!(state.chain_count(), 1);
    assert!(state.best_chain().unwrap().eq_internal_state(expected));
    assert!(reader.best_chain().unwrap().eq_internal_state(expected));

    let live_transaction = live
        .block
        .view()
        .transactions()
        .nth(SHIELDED_TX_INDEX)
        .unwrap();
    let Transaction::V5 {
        sapling_shielded_data: Some(sapling),
        orchard_shielded_data: Some(orchard),
        ..
    } = live_transaction
    else {
        panic!("the live fixture includes both shielded pools");
    };
    for (sapling, orchard, error) in [
        (
            Some(sapling.clone()),
            None,
            ValidateContextError::DuplicateSaplingNullifier {
                nullifier: *live_transaction.sapling_nullifiers().next().unwrap(),
                in_finalized_state: false,
            },
        ),
        (
            None,
            Some(orchard.clone()),
            ValidateContextError::DuplicateOrchardNullifier {
                nullifier: *live_transaction.orchard_nullifiers().next().unwrap(),
                in_finalized_state: false,
            },
        ),
    ] {
        let mut duplicate = valid.clone();
        let Transaction::V5 {
            sapling_shielded_data,
            orchard_shielded_data,
            ..
        } = Arc::make_mut(&mut duplicate.transactions[SHIELDED_TX_INDEX])
        else {
            unreachable!("the valid candidate has a V5 shielded transaction");
        };
        if let Some(sapling) = sapling {
            *sapling_shielded_data = Some(sapling);
        }
        if let Some(orchard) = orchard {
            *orchard_shielded_data = Some(orchard);
        }
        set_merkle_root(&mut duplicate);
        let prepared = Arc::new(duplicate).prepare();
        let contextual = ContextuallyVerifiedBlock::with_block_and_spent_utxos(
            &Network::Mainnet,
            prepared,
            spent.clone(),
        )
        .unwrap();
        assert_eq!(chain.clone().push(contextual).unwrap_err(), error);
        assert!(chain.eq_internal_state(expected));
        assert!(reader.best_chain().unwrap().eq_internal_state(expected));
        assert_membership(chain, live, true);
    }
}

/// Checks membership in every index affected by the transactions in `record`.
fn assert_membership(chain: &Chain, record: &ContextuallyVerifiedBlock, present: bool) {
    assert_eq!(chain.block(record.hash.into()).is_some(), present);
    for hash in record.transaction_hashes.iter() {
        assert_eq!(chain.contains_transaction(hash), present);
    }
    for transaction in record.block.view().transactions().skip(1) {
        for outpoint in transaction
            .inputs()
            .iter()
            .filter_map(transparent::Input::outpoint)
        {
            assert_eq!(chain.spent_utxo(&outpoint).is_some(), present);
            #[cfg(feature = "indexer")]
            assert_eq!(
                chain.spending_transaction_hash(&crate::Spend::OutPoint(outpoint)),
                present.then(|| transaction.hash()),
            );
        }
        for nullifier in transaction.sapling_nullifiers() {
            assert_eq!(chain.sapling_nullifiers.contains_key(nullifier), present);
            #[cfg(feature = "indexer")]
            assert_eq!(
                chain.spending_transaction_hash(&crate::Spend::Sapling(*nullifier)),
                present.then(|| transaction.hash()),
            );
        }
        for nullifier in transaction.orchard_nullifiers() {
            assert_eq!(chain.orchard_nullifiers.contains_key(nullifier), present);
            #[cfg(feature = "indexer")]
            assert_eq!(
                chain.spending_transaction_hash(&crate::Spend::Orchard(*nullifier)),
                present.then(|| transaction.hash()),
            );
        }
    }
}

/// Returns an empty NU5 chain, its finalized tip, and a mixed shielded template.
fn fixtures(skip_address_index: bool) -> (Chain, Arc<Block>, Transaction) {
    let mut template: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1687118_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let shielded = template
        .transactions
        .iter()
        .find(|transaction| {
            transaction.sapling_nullifiers().next().is_some()
                && transaction.orchard_nullifiers().next().is_some()
        })
        .expect("the fixture contains a Sapling-to-Sapling-and-Orchard transaction")
        .as_ref()
        .clone();
    Arc::make_mut(&mut template).transactions.truncate(1);
    let history = HistoryTree::from_block(
        &Network::Mainnet,
        template.clone(),
        &Default::default(),
        &Default::default(),
        &Default::default(),
    )
    .unwrap();
    let chain = Chain::new(
        &Network::Mainnet,
        template.coinbase_height().unwrap(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Arc::new(history),
        ValueBalance::fake_populated_pool(),
    )
    .skipping_transparent_address_index(skip_address_index);
    (chain, template, shielded)
}

/// Builds one transparent spend and one Sapling/Orchard transaction per block.
fn mixed_child(
    parent: &Arc<Block>,
    shielded: &Transaction,
    seed: u32,
    branch: u32,
) -> (Arc<Block>, ContextuallyVerifiedBlock) {
    let mut block = parent.make_fake_child();
    let height = block.coinbase_height().unwrap();
    let mut shielded = shielded.clone();
    let Transaction::V5 {
        inputs,
        outputs,
        expiry_height,
        sapling_shielded_data: Some(sapling),
        orchard_shielded_data: Some(orchard),
        ..
    } = &mut shielded
    else {
        panic!("the mixed fixture is a V5 transaction with both shielded pools");
    };
    inputs.clear();
    outputs.clear();
    *expiry_height = Height(height.0 + branch);
    sapling.value_balance = Amount::zero();
    orchard.value_balance = Amount::zero();
    let TransferData::SpendsAndMaybeOutputs { spends, .. } = &mut sapling.transfers else {
        panic!("the mixed fixture includes Sapling spends");
    };
    for (index, spend) in spends.iter_mut().enumerate() {
        spend.nullifier = nullifier_bytes(seed, index).into();
    }
    for (index, action) in orchard.actions.iter_mut().enumerate() {
        action.action.nullifier =
            orchard::Nullifier::try_from(nullifier_bytes(seed, index)).unwrap();
    }

    let mut hash = [0xee; 32];
    hash[..4].copy_from_slice(&seed.to_le_bytes());
    let outpoint = transparent::OutPoint {
        hash: transaction::Hash(hash),
        index: 0,
    };
    let value = Amount::try_from(TRANSFER_VALUE).unwrap();
    let spent = transparent::OrderedUtxo::new(
        transparent::Output::new(value, address_script(seed, 0)),
        Height(1),
        0,
    );
    let transparent = Transaction::V4 {
        inputs: vec![transparent::Input::PrevOut {
            outpoint,
            unlock_script: transparent::Script::new(&[]),
            sequence: u32::MAX,
        }],
        outputs: vec![transparent::Output::new(
            value,
            address_script(seed, branch + 1),
        )],
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        joinsplit_data: None,
        sapling_shielded_data: None,
    };
    let body = Arc::make_mut(&mut block);
    body.transactions.truncate(1);
    // NU5 coinbase IDs commit to the expiry height rather than the coinbase
    // script. Keep it at the block height so every coinbase has a distinct ID.
    let Transaction::V5 { expiry_height, .. } = Arc::make_mut(&mut body.transactions[0]) else {
        panic!("the NU5 fixture has a V5 coinbase");
    };
    *expiry_height = height;
    body.transactions
        .extend([Arc::new(transparent), Arc::new(shielded)]);
    set_merkle_root(body);
    let contextual = ContextuallyVerifiedBlock::with_block_and_spent_utxos(
        &Network::Mainnet,
        block.clone().prepare(),
        HashMap::from([(outpoint, spent)]),
    )
    .unwrap();
    (block, contextual)
}

fn set_merkle_root(block: &mut Block) {
    let merkle_root = block.transactions.iter().collect();
    Arc::make_mut(&mut block.header).merkle_root = merkle_root;
}

/// Different seeds identify different blocks; indexes distinguish their spends.
fn nullifier_bytes(seed: u32, index: usize) -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&seed.to_le_bytes());
    bytes[4..12].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
    bytes
}

fn address_script(seed: u32, branch: u32) -> transparent::Script {
    let mut script = vec![0x76, 0xa9, 0x14];
    let mut hash = [0; 20];
    hash[..4].copy_from_slice(&seed.to_le_bytes());
    hash[4..8].copy_from_slice(&branch.to_le_bytes());
    script.extend_from_slice(&hash);
    script.extend_from_slice(&[0x88, 0xac]);
    transparent::Script::new(&script)
}
