//! Per-block transparent address indexes, and finalized entries left in shared chain indexes.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use zakura_chain::{
    amount::{Amount, NegativeAllowed},
    block::{Block, Height},
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction::{self, LockTime, Transaction},
    transparent,
    value_balance::ValueBalance,
};

use crate::{
    arbitrary::Prepare, service::non_finalized_state::shared_map::PARTITIONS,
    tests::FakeChainHelper, ContextuallyVerifiedBlock, OutputLocation, TransactionLocation,
};

use super::{
    super::{Chain, FINALIZED_PARTITIONS_PER_ROOT},
    AddressKey,
};

const VALUE: i64 = 1_000;

/// Returns a pay-to-public-key-hash address and its lock script.
fn p2pkh(seed: u32) -> (transparent::Address, transparent::Script) {
    let mut hash = [0; 20];
    hash[..4].copy_from_slice(&seed.to_le_bytes());
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend(hash);
    script.extend([0x88, 0xac]);
    let script = transparent::Script::new(&script);
    let address = transparent::Output::new(Amount::zero(), script.clone())
        .address(&Network::Mainnet)
        .expect("a P2PKH script has an address");
    (address, script)
}

/// A finalized output the tests treat as sent to `p2pkh(seed)`.
fn finalized_output(seed: u32) -> (transparent::OutPoint, transparent::OrderedUtxo) {
    let mut hash = [0xee; 32];
    hash[..4].copy_from_slice(&seed.to_le_bytes());
    let output = transparent::Output::new(
        Amount::try_from(VALUE).expect("the value is in range"),
        p2pkh(seed).1,
    );
    (
        transparent::OutPoint {
            hash: transaction::Hash(hash),
            index: 0,
        },
        transparent::OrderedUtxo::new(output, Height(1), 0),
    )
}

/// Returns a child of `parent` with `spends` transactions. Transaction `i` spends the
/// finalized output of `p2pkh(first_seed + i)` and pays `p2pkh(first_seed + i + 1_000_000)`.
fn child_with_spends(
    parent: &Arc<Block>,
    first_seed: u32,
    spends: u32,
) -> (Arc<Block>, ContextuallyVerifiedBlock) {
    let mut block = parent.make_fake_child();
    let mut spent = HashMap::new();
    let transactions = &mut Arc::make_mut(&mut block).transactions;
    transactions.truncate(1);
    for seed in first_seed..first_seed + spends {
        let (outpoint, utxo) = finalized_output(seed);
        spent.insert(outpoint, utxo);
        transactions.push(Arc::new(Transaction::V4 {
            inputs: vec![transparent::Input::PrevOut {
                outpoint,
                unlock_script: transparent::Script::new(&[]),
                sequence: u32::MAX,
            }],
            outputs: vec![transparent::Output::new(
                Amount::try_from(VALUE).expect("the value is in range"),
                p2pkh(seed + 1_000_000).1,
            )],
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            joinsplit_data: None,
            sapling_shielded_data: None,
        }));
    }
    let contextual = ContextuallyVerifiedBlock::with_block_and_spent_utxos(
        &Network::Mainnet,
        block.clone().prepare(),
        spent,
    )
    .expect("the spends and outputs balance");
    (block, contextual)
}

/// Returns an empty chain, and a coinbase-only copy of a historical block at its first height.
fn chain_and_template(skip_address_index: bool) -> (Chain, Arc<Block>) {
    let mut template = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("the historical block vector deserializes");
    Arc::make_mut(&mut template).transactions.truncate(1);
    let finalized_tip =
        (template.coinbase_height().expect("the block has a height") - 1).expect("above genesis");
    let chain = Chain::new(
        &Network::Mainnet,
        finalized_tip,
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::fake_populated_pool(),
    )
    .skipping_transparent_address_index(skip_address_index);
    let chain = push(
        chain,
        template.clone().prepare().test_with_zero_spent_utxos(),
    );
    (chain, template)
}

fn push(chain: Chain, block: ContextuallyVerifiedBlock) -> Chain {
    chain
        .push(block)
        .expect("test blocks are valid chain updates")
}

#[test]
fn block_index_records_each_address_transfers() {
    let (chain, template) = chain_and_template(false);
    let (block, contextual) = child_with_spends(&template, 7, 1);
    let height = contextual.height;
    let spending_hash = block.transactions[1].hash();
    let chain = push(chain, contextual);

    let (spender, _) = p2pkh(7);
    let (receiver, _) = p2pkh(1_000_007);
    let transfers = chain
        .transparent_transfers_by_height
        .get(&height)
        .expect("the block has indexed transfers");

    let spent = transfers
        .get(&AddressKey::new(spender))
        .expect("the spender is indexed");
    assert_eq!(
        spent.balance(),
        Amount::<NegativeAllowed>::try_from(-VALUE).unwrap()
    );
    assert_eq!(spent.received(), 0);
    assert!(spent.created_locations().is_empty());
    let spent_location = OutputLocation::from_usize(Height(1), 0, 0);
    assert_eq!(spent.spent().collect::<Vec<_>>(), [&spent_location]);

    let received = transfers
        .get(&AddressKey::new(receiver))
        .expect("the receiver is indexed");
    assert_eq!(
        received.balance(),
        Amount::<NegativeAllowed>::try_from(VALUE).unwrap()
    );
    assert_eq!(received.received(), 1_000);
    let created_location = OutputLocation::from_usize(height, 1, 0);
    assert_eq!(received.created_locations(), [created_location]);
    assert_eq!(received.spent().count(), 0);

    let both = HashSet::from([spender, receiver]);
    assert_eq!(
        chain.partial_transparent_balance_change(&both, height),
        (Amount::zero(), 1_000)
    );
    let (created, spent) = chain.partial_transparent_utxo_changes(&both);
    assert_eq!(
        created.into_iter().collect::<Vec<_>>(),
        [(created_location, block.transactions[1].outputs()[0].clone())]
    );
    assert_eq!(spent.into_iter().collect::<Vec<_>>(), [spent_location]);
    assert_eq!(
        chain.partial_transparent_tx_ids(&HashSet::from([spender]), Height(0)..=Height::MAX),
        [(TransactionLocation::from_usize(height, 1), spending_hash)].into()
    );
}

#[test]
fn chains_share_block_indexes_and_finalizing_drops_the_root_index() {
    let (chain, template) = chain_and_template(false);
    let (first, contextual) = child_with_spends(&template, 0, 2);
    let first_height = contextual.height;
    let chain = push(chain, contextual);
    let (second, contextual) = child_with_spends(&first, 10, 2);
    let mut best = push(chain, contextual);
    let mut side = best.clone();
    best = push(best, child_with_spends(&second, 20, 2).1);

    assert!(Arc::ptr_eq(
        best.transparent_transfers_by_height
            .get(&first_height)
            .unwrap(),
        side.transparent_transfers_by_height
            .get(&first_height)
            .unwrap(),
    ));

    let first_receiver = HashSet::from([p2pkh(1_000_000).0]);
    assert_eq!(
        best.partial_transparent_balance_change(&first_receiver, first_height),
        (Amount::<NegativeAllowed>::try_from(VALUE).unwrap(), 1_000)
    );
    for chain in [&mut best, &mut side] {
        // The template, then the first spending block.
        chain.pop_root();
        chain.pop_root();
        assert!(chain
            .transparent_transfers_by_height
            .get(&first_height)
            .is_none());
        assert_eq!(
            chain.partial_transparent_balance_change(&first_receiver, first_height),
            (Amount::zero(), 0)
        );
        assert!(chain
            .partial_transparent_utxo_changes(&first_receiver)
            .0
            .is_empty());
    }
}

#[test]
fn finalized_entries_are_hidden_then_swept() {
    let (chain, template) = chain_and_template(false);
    let (root, contextual) = child_with_spends(&template, 0, 300);
    let mut chain = push(chain, contextual);
    chain.pop_root();
    let mut parent = root.clone();
    // Enough blocks to finalize the spending block, then sweep every partition.
    let sweeps = PARTITIONS.div_ceil(FINALIZED_PARTITIONS_PER_ROOT);
    for _ in 0..=sweeps {
        let child = parent.make_fake_child();
        chain = push(chain, child.clone().prepare().test_with_zero_spent_utxos());
        parent = child;
    }

    let spent = finalized_output(0).0;
    let spending = root.transactions[1].hash();
    assert!(chain.spent_utxo(&spent).is_some());
    assert!(chain.contains_transaction(&spending));

    chain.pop_root();
    assert!(chain.spent_utxo(&spent).is_none());
    assert!(chain.transaction_location(&spending).is_none());
    assert!(!chain.contains_transaction(&spending));
    assert!(chain.transaction(spending).is_none());
    // Most of the finalized spends wait for a later sweep.
    assert!(chain.spent_utxos.len() > 200);

    for _ in 1..sweeps {
        chain.pop_root();
    }
    assert_eq!(chain.spent_utxos.len(), 0);
    assert!(root
        .transactions
        .iter()
        .all(|transaction| chain.tx_loc_by_hash.get(&transaction.hash()).is_none()));
}

#[test]
fn finalizing_copies_only_the_swept_partitions_of_a_shared_chain() {
    let (chain, template) = chain_and_template(false);
    // Enough spends to put the root's entries in every partition.
    let (root, contextual) = child_with_spends(&template, 0, 4_000);
    let mut chain = push(chain, contextual);
    chain.pop_root();
    let (_, contextual) = child_with_spends(&root, 10_000, 1);
    let mut chain = push(chain, contextual);

    let snapshot = chain.clone();
    assert_eq!(
        chain.spent_utxos.shared_partitions(&snapshot.spent_utxos),
        PARTITIONS
    );
    chain.pop_root();

    let copied = FINALIZED_PARTITIONS_PER_ROOT;
    assert_eq!(
        chain.spent_utxos.shared_partitions(&snapshot.spent_utxos),
        PARTITIONS - copied
    );
    assert_eq!(
        chain
            .tx_loc_by_hash
            .shared_partitions(&snapshot.tx_loc_by_hash),
        PARTITIONS - copied
    );
}

#[test]
fn skipped_transparent_address_index_stays_empty_through_commit_and_revert() {
    for skip in [false, true] {
        let (chain, template) = chain_and_template(skip);
        let (_, contextual) = child_with_spends(&template, 0, 1);
        let height = contextual.height;
        let mut chain = push(chain, contextual);

        // Spending checks remain active even when address indexing is skipped.
        let spent = finalized_output(0).0;
        assert!(chain.spent_utxo(&spent).is_some());
        assert_eq!(
            chain.transparent_transfers_by_height.get(&height).is_some(),
            !skip
        );

        chain.pop_tip();
        assert!(chain.spent_utxo(&spent).is_none());
        assert!(chain.spent_utxos.get(&spent).is_none());
        assert!(chain.transparent_transfers_by_height.get(&height).is_none());
    }
}
