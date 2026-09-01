//! Tests for ZIP-317 transaction selection for block template production

#![allow(clippy::unwrap_in_result)]

use zcash_keys::address::Address;
use zcash_transparent::address::TransparentAddress;

use zakura_chain::{
    amount::Amount,
    block::{Height, MAX_BLOCK_BYTES},
    parameters::Network,
    serialization::ZcashSerialize,
    transaction,
    transparent::{OutPoint, Output, Script},
};
use zakura_node_services::mempool::TransactionDependencies;

use crate::methods::types::{get_block_template::MinerParams, transaction::TransactionTemplate};

use super::{block_template_overhead_bytes, max_coinbase_bytes, select_mempool_transactions};

/// Replaces `transaction`'s inner transaction with one that has exactly
/// `target_size` serialized bytes.
fn with_serialized_size(
    mut transaction: transaction::VerifiedUnminedTx,
    target_size: usize,
) -> transaction::VerifiedUnminedTx {
    let mut inner = transaction.transaction.transaction().as_ref().clone();
    inner
        .outputs_mut()
        .push(Output::new(Amount::zero(), Script::new(&[])));

    let base_size = inner.zcash_serialized_size();
    let mut script_size = target_size
        .checked_sub(base_size)
        .expect("the target size is larger than the test transaction");

    loop {
        inner
            .outputs_mut()
            .last_mut()
            .expect("the test added an output")
            .lock_script = Script::new(&vec![0; script_size]);

        let actual_size = inner.zcash_serialized_size();
        match actual_size.cmp(&target_size) {
            std::cmp::Ordering::Less => script_size += target_size - actual_size,
            std::cmp::Ordering::Greater => script_size -= actual_size - target_size,
            std::cmp::Ordering::Equal => break,
        }
    }

    transaction.transaction = inner.into();
    assert_eq!(transaction.transaction.size(), target_size);
    transaction
}

#[test]
fn reserves_network_specific_header_and_transaction_count_sizes() {
    let regtest = Network::new_regtest(Default::default());

    assert_eq!(block_template_overhead_bytes(&Network::Mainnet), 1_490);
    assert_eq!(block_template_overhead_bytes(&regtest), 180);
}

#[test]
fn reserves_serialized_block_and_pool_tag_overhead() {
    let network = Network::Mainnet;
    let height = Height(1_000_000);
    let miner_params =
        MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20])));
    let fake_coinbase =
        TransactionTemplate::new_coinbase(&network, height, &miner_params, Amount::zero())
            .expect("test coinbase template is valid");
    assert!(
        max_coinbase_bytes(&fake_coinbase) > fake_coinbase.data.as_ref().len(),
        "the test coinbase leaves room for a pool tag",
    );
    let max_block_bytes = usize::try_from(MAX_BLOCK_BYTES).expect("fits in memory");
    let max_mempool_transaction_bytes = max_block_bytes
        - block_template_overhead_bytes(&network)
        - max_coinbase_bytes(&fake_coinbase);

    let template_transactions = |transaction_size| {
        let transaction = network
            .unmined_transactions_in_blocks(..)
            .next()
            .expect("test network has an unmined transaction");
        let transaction = with_serialized_size(transaction, transaction_size);

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
        unmined_tx.transaction.id().mined_id(),
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
        .id()
        .mined_id();

    let mut mempool_tx_deps = TransactionDependencies::default();
    mempool_tx_deps.add(
        dependent_tx1.transaction.id().mined_id(),
        vec![OutPoint::from_usize(independent_tx_id, 0)],
    );
    mempool_tx_deps.add(
        dependent_tx2.transaction.id().mined_id(),
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
            .find(|(_, tx)| tx.transaction.id().mined_id() == id)
    };

    let (dependency_depth, _) =
        selected_tx_by_id(independent_tx_id).expect("should select the independent tx");

    assert_eq!(
        *dependency_depth, 0,
        "should return a dependency depth of 0 for the independent tx"
    );

    let (dependency_depth, _) = selected_tx_by_id(dependent_tx1.transaction.id().mined_id())
        .expect("should select dependent_tx1");

    assert_eq!(
        *dependency_depth, 1,
        "should return a dependency depth of 1 for the dependent tx"
    );
}

/// Tests that block template selection respects the ZIP 218 shielded limits, so
/// a template cannot exceed a limit the block verifier enforces.
mod zip218_template_limits {
    use std::sync::Arc;

    use zakura_chain::{
        parameters::{
            testnet::{ConfiguredActivationHeights, Parameters},
            Network, GLOBAL_SHIELDED_BUDGET, ORCHARD_BLOCK_ACTION_LIMIT, SAPLING_BLOCK_IO_LIMIT,
            SPROUT_BLOCK_JOINSPLIT_LIMIT,
        },
        transaction::{
            arbitrary::{fake_v5_with_orchard_actions, fake_v5_with_sapling_outputs},
            Transaction, UnminedTx, VerifiedUnminedTx,
        },
    };

    use zcash_keys::address::Address;
    use zcash_transparent::address::TransparentAddress;

    use super::{
        super::{BlockTemplateLimits, MinerParams},
        Amount, Height, TransactionTemplate,
    };

    /// A transaction that fills the Orchard limit leaves no room under the
    /// global budget for a single Sapling output, even though the Sapling
    /// per-pool limit is untouched.
    #[test]
    fn the_global_budget_bounds_selection_across_pools() {
        let mut limits = nu7_template_limits();

        let orchard_tx = verified_unmined_tx(fake_v5_with_orchard_actions(
            usize::try_from(ORCHARD_BLOCK_ACTION_LIMIT).expect("the limit fits in usize"),
        ));
        let sapling_tx = verified_unmined_tx(fake_v5_with_sapling_outputs(1));

        assert!(
            limits.try_add(&orchard_tx),
            "Orchard actions exactly at the per-pool limit fit in an empty template"
        );
        assert!(
            !limits.try_add(&sapling_tx),
            "a Sapling output past the global budget must not be selected"
        );
    }

    /// The shielded limits only bind once ZIP 218 is active, so a pre-NU7
    /// template accepts a transaction that a post-NU7 template rejects.
    #[test]
    fn the_shielded_limits_only_bind_after_activation() {
        let network = nu7_activation_testnet(2);
        let over_limit_tx = verified_unmined_tx(fake_v5_with_orchard_actions(
            usize::try_from(ORCHARD_BLOCK_ACTION_LIMIT + 1).expect("the limit fits in usize"),
        ));

        let mut pre_activation = template_limits(&network, Height(1));
        assert!(
            pre_activation.try_add(&over_limit_tx),
            "the shielded limits are inactive below the NU7 activation height"
        );

        let mut post_activation = template_limits(&network, Height(2));
        assert_eq!(
            post_activation.try_add(&over_limit_tx),
            !cfg!(feature = "zip218"),
            "a zip218 build rejects Orchard actions above the per-block limit at NU7"
        );
    }

    fn template_limits(network: &Network, height: Height) -> BlockTemplateLimits {
        let miner_params =
            MinerParams::from(Address::from(TransparentAddress::PublicKeyHash([0x7e; 20])));
        let fake_coinbase_tx =
            TransactionTemplate::new_coinbase(network, height, &miner_params, Amount::zero())
                .expect("valid coinbase transaction template");

        BlockTemplateLimits::initial(network, height, &fake_coinbase_tx)
    }

    fn nu7_template_limits() -> BlockTemplateLimits {
        BlockTemplateLimits {
            remaining_bytes: usize::MAX,
            remaining_sigops: u32::MAX,
            remaining_unpaid_actions: u32::MAX,
            remaining_orchard_actions: ORCHARD_BLOCK_ACTION_LIMIT,
            remaining_sapling_ios: SAPLING_BLOCK_IO_LIMIT,
            remaining_sprout_joinsplits: SPROUT_BLOCK_JOINSPLIT_LIMIT,
            remaining_shielded_cost: GLOBAL_SHIELDED_BUDGET,
        }
    }

    fn nu7_activation_testnet(nu7_activation_height: u32) -> Network {
        Parameters::build()
            .with_slow_start_interval(Height(0))
            .with_activation_heights(ConfiguredActivationHeights {
                // The coinbase template hashes the transaction, which the
                // pre-Overwinter format does not support, so activate the
                // earlier upgrades from height 1.
                nu5: Some(1),
                nu7: Some(nu7_activation_height),
                ..Default::default()
            })
            .expect("activation heights are valid")
            .clear_funding_streams()
            .to_network()
            .expect("configured testnet is valid")
    }

    fn verified_unmined_tx(transaction: Arc<Transaction>) -> VerifiedUnminedTx {
        let unmined_tx = UnminedTx::from(transaction);
        let miner_fee = unmined_tx.conventional_fee();

        VerifiedUnminedTx::new(unmined_tx, miner_fee, 0, 0, Arc::new(Vec::new()))
            .expect("the fake transaction pays the conventional fee")
    }
}
