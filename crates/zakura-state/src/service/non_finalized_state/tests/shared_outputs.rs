//! Large output ownership across chain snapshots, forks, rollback and finalization.

use std::{collections::HashMap, sync::Arc};

use zakura_chain::{
    block::{Block, Height},
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction::Transaction,
    transparent,
    value_balance::ValueBalance,
};

use crate::{
    arbitrary::Prepare, service::non_finalized_state::Chain, tests::FakeChainHelper,
    ContextuallyVerifiedBlock,
};

fn funded_chain() -> (Chain, Arc<Block>, transparent::OutPoint) {
    let mut block = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("the historical block vector deserializes");
    let Transaction::V4 { outputs, .. } =
        Arc::make_mut(&mut Arc::make_mut(&mut block).transactions[0])
    else {
        panic!("the fixture has a v4 coinbase");
    };
    outputs[0].lock_script = transparent::Script::new(&[0x51; 10_000]);
    let outpoint = transparent::OutPoint {
        hash: block.transactions[0].hash(),
        index: 0,
    };
    let chain = Chain::new(
        &Network::Mainnet,
        Height(0),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::fake_populated_pool(),
    )
    .push(block.clone().prepare().test_with_zero_spent_utxos())
    .expect("the funding fixture passes chain update checks");
    (chain, block, outpoint)
}

#[test]
fn snapshots_share_outputs_without_sharing_spend_or_branch_membership() {
    let (parent, block, outpoint) = funded_chain();
    let mut child = block.make_fake_child();
    let mut spend = block.transactions[0].as_ref().clone();
    let Transaction::V4 {
        inputs, outputs, ..
    } = &mut spend
    else {
        panic!("the fixture has a v4 transaction");
    };
    *inputs = vec![transparent::Input::PrevOut {
        outpoint,
        unlock_script: transparent::Script::new(&[]),
        sequence: u32::MAX,
    }];
    outputs.truncate(1);
    let spend_outpoint = transparent::OutPoint {
        hash: spend.hash(),
        index: 0,
    };
    Arc::make_mut(&mut child).transactions.push(Arc::new(spend));

    // Exercise state updates directly. Semantic checks and coinbase maturity
    // are covered by the existing service tests.
    let contextual = ContextuallyVerifiedBlock::with_block_and_spent_utxos(
        &Network::Mainnet,
        child.clone().prepare(),
        HashMap::from([(outpoint, parent.created_utxos[&outpoint].as_ref().clone())]),
    )
    .expect("the supplied spent output gives the fixture's value balance");
    let spent_chain = parent
        .clone()
        .push(contextual)
        .expect("the child updates the chain");
    assert!(Arc::ptr_eq(
        &parent.created_utxos[&outpoint],
        &spent_chain.created_utxos[&outpoint]
    ));
    assert!(parent.unspent_utxos().contains_key(&outpoint));
    assert!(!spent_chain.unspent_utxos().contains_key(&outpoint));
    assert!(spent_chain.created_utxo(&outpoint).is_some());
    assert!(parent.created_utxo(&spend_outpoint).is_none());
    assert!(spent_chain.created_utxo(&spend_outpoint).is_some());

    #[cfg(feature = "indexer")]
    {
        let spend = crate::Spend::OutPoint(outpoint);
        assert_eq!(parent.spending_transaction_hash(&spend), None);
        assert_eq!(
            spent_chain.spending_transaction_hash(&spend),
            Some(spend_outpoint.hash)
        );
    }

    let fork = spent_chain
        .fork(block.hash())
        .expect("the parent is in the chain");
    assert!(fork.eq_internal_state(&parent));
    assert!(Arc::ptr_eq(
        &fork.created_utxos[&outpoint],
        &parent.created_utxos[&outpoint]
    ));

    let (mut rolled_back, invalidated) = spent_chain
        .invalidate_block(child.hash())
        .expect("the child is in the chain");
    assert!(rolled_back.eq_internal_state(&parent));
    assert!(rolled_back.unspent_utxos().contains_key(&outpoint));
    assert!(rolled_back.created_utxo(&spend_outpoint).is_none());
    assert!(!spent_chain.unspent_utxos().contains_key(&outpoint));
    for block in invalidated {
        rolled_back = rolled_back
            .push(block)
            .expect("the invalidated child can be replayed");
    }
    assert!(rolled_back.eq_internal_state(&spent_chain));
}

#[test]
fn finalization_and_owned_queries_preserve_other_snapshot_payloads() {
    let (mut chain, block, outpoint) = funded_chain();
    let snapshot = chain.clone();
    let payload = Arc::downgrade(&chain.created_utxos[&outpoint]);
    let expected = chain.created_utxo(&outpoint).expect("the output exists");
    let mut returned_output = snapshot.created_utxo(&outpoint).expect("the output exists");
    returned_output.output.lock_script = transparent::Script::new(&[0x52]);
    assert_eq!(snapshot.created_utxo(&outpoint), Some(expected.clone()));

    let mut returned = snapshot.unspent_utxos();
    returned
        .get_mut(&outpoint)
        .expect("the output is unspent")
        .utxo
        .output
        .lock_script = transparent::Script::new(&[0x52]);
    assert_eq!(snapshot.created_utxo(&outpoint), Some(expected.clone()));

    chain = chain
        .push(
            block
                .make_fake_child()
                .prepare()
                .test_with_zero_spent_utxos(),
        )
        .expect("the child updates the chain");
    let (finalized, _) = chain.pop_root();
    assert_eq!(finalized.hash, block.hash());
    assert!(chain.created_utxo(&outpoint).is_none());
    assert_eq!(snapshot.created_utxo(&outpoint), Some(expected));
    assert!(
        payload.upgrade().is_some(),
        "the old snapshot still owns the payload"
    );
    drop(snapshot);
    assert!(
        payload.upgrade().is_none(),
        "the shared payload is released with its last snapshot"
    );
}
