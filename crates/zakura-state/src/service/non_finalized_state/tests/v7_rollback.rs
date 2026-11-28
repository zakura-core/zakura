//! Rollback of a non-finalized block that contains ZIP 248 v7 transactions.
//!
//! The v7 transactions reuse the mixed fixture's shielded descriptions, so their
//! proofs and signatures do not verify. The block is added through [`Chain::push`]
//! and removed through [`Chain::fork`], which pops it like a reorg does.

use std::sync::Arc;

use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parameters::{Network, NetworkUpgrade},
    transaction::{
        v7::{CoinbaseBundle, V7Fields, ValueDeltas},
        LockTime, Transaction,
    },
    transparent,
};

use crate::{
    arbitrary::Prepare,
    service::{check, finalized_state::FinalizedState},
    Config, ContextuallyVerifiedBlock,
};

use super::deep_reorg::{
    address_script, assert_membership, fixtures, mixed_child, set_merkle_root, SHIELDED_TX_INDEX,
    TRANSFER_VALUE, TRANSPARENT_TX_INDEX,
};

const COINBASE_VALUE: i64 = 5_000;

#[test]
fn v7_block_rollback_restores_the_prior_chain_state() {
    let _guard = zakura_test::init();

    let finalized = FinalizedState::new(&Config::ephemeral(), &Network::Mainnet).unwrap();
    let (chain, template, shielded) = fixtures(false);
    let (parent, parent_contextual) = mixed_child(&template, &shielded, 0, 0);
    let before = chain.push(parent_contextual.clone()).unwrap();

    // The v7 block spends the parent's transparent output, so rollback must restore it,
    // and reuses a fresh mixed child's Sapling and Orchard bundles in a v7 transaction.
    let (child, _) = mixed_child(&parent, &shielded, 1, 0);
    let height = child.coinbase_height().unwrap();
    let spent_outpoint = transparent::OutPoint {
        hash: parent_contextual.transaction_hashes[TRANSPARENT_TX_INDEX],
        index: 0,
    };
    let mut block = Block::clone(&child);
    block.transactions = vec![
        Arc::new(v7_coinbase(height)),
        Arc::new(v7_transparent_spend(spent_outpoint, height)),
        Arc::new(v7_shielded(&child.transactions[SHIELDED_TX_INDEX], height)),
    ];
    set_merkle_root(&mut block);
    let block = Arc::new(block);
    assert_eq!(block.coinbase_height(), Some(height));
    assert!(block.sapling_nullifiers().next().is_some());
    assert!(block.orchard_nullifiers().next().is_some());

    let prepared = block.clone().prepare();
    let spent = check::utxo::transparent_spend(&prepared, Some(&before), &finalized.db).unwrap();
    let contextual =
        ContextuallyVerifiedBlock::with_block_and_spent_utxos(&Network::Mainnet, prepared, spent)
            .unwrap();
    let coinbase_outpoint = transparent::OutPoint {
        hash: contextual.transaction_hashes[0],
        index: 0,
    };

    let after = before.clone().push(contextual.clone()).unwrap();
    assert_membership(&after, &contextual, true);
    assert!(after.created_utxo(&coinbase_outpoint).is_some());
    assert_ne!(after.chain_value_pools, before.chain_value_pools);

    let rolled_back = after.fork(parent.hash()).unwrap();
    assert!(rolled_back.eq_internal_state(&before));
    assert_membership(&rolled_back, &contextual, false);
    assert!(rolled_back.created_utxo(&coinbase_outpoint).is_none());
    assert_eq!(rolled_back.chain_value_pools, before.chain_value_pools);
}

fn v7_transaction(
    height: Height,
    inputs: Vec<transparent::Input>,
    outputs: Vec<transparent::Output>,
    v7_fields: V7Fields,
) -> Transaction {
    Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: height,
        inputs,
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        v7_fields: Box::new(v7_fields),
    }
}

/// A v7 coinbase that issues `COINBASE_VALUE` to one transparent output.
fn v7_coinbase(height: Height) -> Transaction {
    let value = Amount::try_from(COINBASE_VALUE).unwrap();
    let coinbase = CoinbaseBundle::new(height, value, Amount::zero(), b"v7".to_vec(), None)
        .expect("the coinbase bundle fields are in range");
    v7_transaction(
        height,
        Vec::new(),
        vec![transparent::Output::new(value, address_script(1, 9))],
        V7Fields {
            value_deltas: ValueDeltas {
                transparent: -value,
                coinbase: value.constrain().unwrap(),
                ..Default::default()
            },
            coinbase: Some(coinbase),
        },
    )
}

/// A fee-free v7 transfer of the `TRANSFER_VALUE` output at `outpoint`.
fn v7_transparent_spend(outpoint: transparent::OutPoint, height: Height) -> Transaction {
    v7_transaction(
        height,
        vec![transparent::Input::PrevOut {
            outpoint,
            unlock_script: transparent::Script::new(&[]),
            sequence: u32::MAX,
        }],
        vec![transparent::Output::new(
            Amount::try_from(TRANSFER_VALUE).unwrap(),
            address_script(1, 10),
        )],
        V7Fields::default(),
    )
}

/// A v7 transaction with the Sapling and Orchard bundles of the V5 `template`.
fn v7_shielded(template: &Transaction, height: Height) -> Transaction {
    let Transaction::V5 {
        sapling_shielded_data,
        orchard_shielded_data,
        ..
    } = template
    else {
        panic!("the mixed fixture's shielded transaction is V5");
    };
    let mut transaction = v7_transaction(height, Vec::new(), Vec::new(), V7Fields::default());
    let Transaction::V7 {
        sapling_shielded_data: sapling,
        orchard_shielded_data: orchard,
        ..
    } = &mut transaction
    else {
        unreachable!("v7_transaction builds a V7 transaction");
    };
    sapling.clone_from(sapling_shielded_data);
    orchard.clone_from(orchard_shielded_data);
    transaction
}
