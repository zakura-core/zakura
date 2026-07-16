//! Tests for ZIP-317 transaction selection for block template production

#![allow(clippy::unwrap_in_result)]

use zcash_keys::address::Address;
use zcash_transparent::address::TransparentAddress;

use zakura_chain::{
    amount::Amount,
    block::{Height, MAX_BLOCK_BYTES},
    parameters::Network,
    transaction,
    transparent::OutPoint,
};
use zakura_node_services::mempool::TransactionDependencies;

use crate::methods::types::{get_block_template::MinerParams, transaction::TransactionTemplate};

use super::{block_template_overhead_bytes, select_mempool_transactions};

#[test]
fn reserves_network_specific_header_and_transaction_count_sizes() {
    let regtest = Network::new_regtest(Default::default());

    assert_eq!(block_template_overhead_bytes(&Network::Mainnet), 1_490);
    assert_eq!(block_template_overhead_bytes(&regtest), 180);
}

#[test]
fn reserves_serialized_block_overhead() {
    let network = Network::Mainnet;
    let height = Height(1_000_000);
    let miner_params =
        MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20])));
    let fake_coinbase =
        TransactionTemplate::new_coinbase(&network, height, &miner_params, Amount::zero())
            .expect("test coinbase template is valid");
    let max_block_bytes = usize::try_from(MAX_BLOCK_BYTES).expect("fits in memory");
    let max_mempool_transaction_bytes = max_block_bytes
        - block_template_overhead_bytes(&network)
        - fake_coinbase.data.as_ref().len();

    let template_transactions = |transaction_size| {
        let mut transaction = network
            .unmined_transactions_in_blocks(..)
            .next()
            .expect("test network has an unmined transaction");
        transaction.transaction.size = transaction_size;

        select_mempool_transactions(
            &network,
            height,
            &miner_params,
            vec![transaction],
            TransactionDependencies::default(),
        )
    };

    assert_eq!(
        template_transactions(max_mempool_transaction_bytes).len(),
        1,
        "a transaction that exactly fills the safe block budget is selected",
    );
    assert!(
        template_transactions(max_mempool_transaction_bytes + 1).is_empty(),
        "a transaction that only fits when block overhead is omitted is rejected",
    );
}

#[test]
fn excludes_tx_with_unselected_dependencies() {
    let network = Network::Mainnet;
    let mut mempool_tx_deps = TransactionDependencies::default();

    let unmined_tx = network
        .unmined_transactions_in_blocks(..)
        .next()
        .expect("should not be empty");

    mempool_tx_deps.add(
        unmined_tx.transaction.id.mined_id(),
        vec![OutPoint::from_usize(transaction::Hash([0; 32]), 0)],
    );

    assert_eq!(
        select_mempool_transactions(
            &network,
            Height(1_000_000),
            &MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20]))),
            vec![unmined_tx],
            mempool_tx_deps,
        ),
        vec![],
        "should not select any transactions when dependencies are unavailable"
    );
}

#[test]
fn includes_tx_with_selected_dependencies() {
    let network = Network::Mainnet;
    let unmined_txs: Vec<_> = network.unmined_transactions_in_blocks(..).take(3).collect();

    let dependent_tx1 = unmined_txs.first().expect("should have 3 txns");
    let dependent_tx2 = unmined_txs.get(1).expect("should have 3 txns");
    let independent_tx_id = unmined_txs
        .get(2)
        .expect("should have 3 txns")
        .transaction
        .id
        .mined_id();

    let mut mempool_tx_deps = TransactionDependencies::default();
    mempool_tx_deps.add(
        dependent_tx1.transaction.id.mined_id(),
        vec![OutPoint::from_usize(independent_tx_id, 0)],
    );
    mempool_tx_deps.add(
        dependent_tx2.transaction.id.mined_id(),
        vec![
            OutPoint::from_usize(independent_tx_id, 0),
            OutPoint::from_usize(transaction::Hash([0; 32]), 0),
        ],
    );

    let selected_txs = select_mempool_transactions(
        &network,
        Height(1_000_000),
        &MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20]))),
        unmined_txs.clone(),
        mempool_tx_deps.clone(),
    );

    assert_eq!(
        selected_txs.len(),
        2,
        "should select the independent transaction and 1 of the dependent txs, selected: {selected_txs:?}"
    );

    let selected_tx_by_id = |id| {
        selected_txs
            .iter()
            .find(|(_, tx)| tx.transaction.id.mined_id() == id)
    };

    let (dependency_depth, _) =
        selected_tx_by_id(independent_tx_id).expect("should select the independent tx");

    assert_eq!(
        *dependency_depth, 0,
        "should return a dependency depth of 0 for the independent tx"
    );

    let (dependency_depth, _) = selected_tx_by_id(dependent_tx1.transaction.id.mined_id())
        .expect("should select dependent_tx1");

    assert_eq!(
        *dependency_depth, 1,
        "should return a dependency depth of 1 for the dependent tx"
    );
}
