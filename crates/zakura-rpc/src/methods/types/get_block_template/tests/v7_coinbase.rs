//! From NU7.1, block templates have a [ZIP 248] v7 coinbase that the block rules accept.
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use std::sync::Arc;

use zakura_chain::{
    amount::{Amount, NegativeAllowed, NonNegative},
    block::{
        merkle::{AuthDataRoot, Root},
        Hash, Height,
    },
    parameters::{
        subsidy::{block_subsidy, miner_fee_share, FundingStreamReceiver, V7CoinbaseIssuance},
        testnet::{
            ConfiguredActivationHeights, ConfiguredFundingStreamRecipient,
            ConfiguredFundingStreams, RegtestParameters,
        },
        Network, NetworkUpgrade,
    },
    serialization::{DateTime32, ZcashDeserializeInto, ZcashSerialize},
    transaction::{
        self,
        v7::{V7Fields, ValueDeltas},
        LockTime, Transaction, VerifiedUnminedTx,
    },
    transparent::{Input, OutPoint, Output, Script},
    value_balance::ValueBalance,
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
};
use zakura_script::Sigops;
use zakura_state::GetBlockTemplateChainInfo;
use zcash_address::{ToAddress, ZcashAddress};
use zcash_protocol::consensus::NetworkType;

use super::super::{proposal::proposal_block_from_template, BlockTemplateResponse, MinerParams};
use crate::config::mining;

/// The first height after NU7.1 activation.
const NU7_1_HEIGHT: Height = Height(6);

/// A Regtest network with NU7 at height 5, NU7.1 at [`NU7_1_HEIGHT`], and a lockbox
/// funding stream, so that a v7 coinbase states a nonzero lockbox value.
fn network() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(5),
            nu7_1: Some(NU7_1_HEIGHT.0),
            ..Default::default()
        },
        funding_streams: Some(vec![ConfiguredFundingStreams {
            height_range: Some(Height(1)..Height(100)),
            recipients: Some(vec![ConfiguredFundingStreamRecipient {
                receiver: FundingStreamReceiver::Deferred,
                numerator: 12,
                addresses: None,
            }]),
        }]),
        ..Default::default()
    })
}

fn miner(network: &Network) -> MinerParams {
    MinerParams::new(
        network,
        mining::Config {
            miner_address: Some(ZcashAddress::from_transparent_p2pkh(
                NetworkType::Regtest,
                [0x7e; 20],
            )),
            ..Default::default()
        },
    )
    .expect("the miner address is valid")
}

fn zatoshis(value: i64) -> Amount<NonNegative> {
    value.try_into().expect("valid amount")
}

fn delta(value: i64) -> Amount<NegativeAllowed> {
    value.try_into().expect("valid amount")
}

/// A verified v7 transfer that spends `fee + 10_000` zatoshis, pays 10_000 of them, and
/// states the rest as its fee.
fn v7_transfer(fee: i64) -> VerifiedUnminedTx {
    let spent = Output::new(zatoshis(fee + 10_000), Script::new(&[]));
    let transaction = Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(100),
        inputs: vec![Input::PrevOut {
            outpoint: OutPoint {
                hash: transaction::Hash([1; 32]),
                index: 0,
            },
            unlock_script: Script::new(&[]),
            sequence: u32::MAX,
        }],
        outputs: vec![Output::new(zatoshis(10_000), Script::new(&[]))],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        v7_fields: Box::new(V7Fields {
            value_deltas: ValueDeltas {
                transparent: delta(fee),
                fee: delta(-fee),
                ..ValueDeltas::default()
            },
            coinbase: None,
        }),
    };
    VerifiedUnminedTx::new(
        Arc::new(transaction).into(),
        zatoshis(fee),
        0,
        0,
        Arc::new(vec![spent]),
    )
    .expect("the fee pays for the transaction's actions")
}

fn template(
    network: &Network,
    height: Height,
    mempool: Vec<VerifiedUnminedTx>,
) -> BlockTemplateResponse {
    let chain_info = GetBlockTemplateChainInfo {
        value_pools: ValueBalance::zero(),
        expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
        tip_height: height.previous().expect("the height is above genesis"),
        tip_hash: Hash([1; 32]),
        cur_time: DateTime32::from(1_654_008_617),
        min_time: DateTime32::from(1_654_008_606),
        max_time: DateTime32::from(1_654_008_728),
        chain_history_root: Some(zakura_chain::block::CHAIN_HISTORY_ACTIVATION_RESERVED.into()),
    };
    BlockTemplateResponse::new_internal(
        network,
        None,
        &miner(network),
        &chain_info,
        "0".repeat(46).parse().expect("valid long poll ID"),
        mempool.into_iter().map(|tx| (0, tx)).collect(),
        None,
        None,
    )
    .expect("the template builds")
}

/// The template's v7 coinbase states the amounts that the block rules expect, and its
/// block's roots commit to the v7 coinbase, both when its coinbase delta is positive
/// and when the ZIP 235 fee contribution makes it negative.
#[test]
fn template_coinbase_from_nu7_1_is_a_valid_v7_coinbase() {
    let _init_guard = zakura_test::init();
    let network = network();
    let height = (NU7_1_HEIGHT + 1).expect("valid height");

    for (fee, negative_coinbase_delta) in [(10_000, false), (5_000_000_000, true)] {
        let template = template(&network, height, vec![v7_transfer(fee)]);
        let block = proposal_block_from_template(&template, None, &network)
            .expect("the template's transactions decode");
        let coinbase = &block.transactions[0];

        let issuance = V7CoinbaseIssuance::new(
            height,
            &network,
            block_subsidy(height, &network, None).expect("the block has a subsidy"),
            zatoshis(fee),
        )
        .expect("the amounts are valid");
        let fields = coinbase
            .v7_fields()
            .expect("the coinbase is a v7 transaction");
        let bundle = fields
            .coinbase
            .as_ref()
            .expect("a v7 coinbase has a coinbase bundle");
        assert_eq!(coinbase.coinbase_height(), Some(height));
        assert!(coinbase.inputs().is_empty());
        assert!(bundle
            .coinbase_data()
            .starts_with(mining::ZAKURA_COINBASE_MARKER.as_bytes()));
        assert_eq!(bundle.block_subsidy(), issuance.block_subsidy);
        assert!(!bundle.lockbox_value().is_zero());
        assert_eq!(bundle.lockbox_value(), issuance.lockbox_value);
        assert_eq!(
            fields.value_deltas.coinbase,
            issuance.coinbase_delta().unwrap()
        );
        assert_eq!(
            fields.value_deltas.coinbase < delta(0),
            negative_coinbase_delta
        );
        assert_eq!(fields.value_deltas.fee, delta(fee));
        let total = coinbase
            .value_pool_deltas()
            .expect("v7")
            .into_iter()
            .map(|(_, value)| i64::from(value))
            .sum::<i64>();
        assert_eq!(total, 0, "the deltas balance");

        // The space the template reserves for the coinbase covers it.
        let reserved = crate::client::TransactionTemplate::coinbase_resource_usage(
            &network,
            height,
            &miner(&network),
            None,
            None,
        )
        .expect("the coinbase's resource usage is known");
        assert!(coinbase.zcash_serialized_size() <= reserved.max_serialized_size);
        assert_eq!(reserved.sigops, coinbase.sigops());
        assert_eq!(
            reserved.shielded_action_counts,
            coinbase.shielded_action_counts()
        );

        // The template reports the miner's share of the fees as the coinbase's fee.
        assert_eq!(
            i64::from(template.coinbase_txn.fee),
            -i64::from(miner_fee_share(height, &network, zatoshis(fee)))
        );
        assert_eq!(template.coinbase_txn.hash, coinbase.hash());
        assert_eq!(
            template.coinbase_txn.auth_digest,
            coinbase.auth_digest().unwrap()
        );
        assert_eq!(
            block.header.merkle_root,
            block.transactions.iter().collect::<Root>()
        );
        assert_eq!(
            template.default_roots.auth_data_root,
            block.transactions.iter().collect::<AuthDataRoot>()
        );

        let subsidy = issuance.block_subsidy;
        let deferred = zakura_consensus::subsidy_is_valid(&block, &network, subsidy)
            .expect("the coinbase pays the funding streams");
        assert_eq!(
            zakura_consensus::miner_fees_are_valid(
                coinbase,
                height,
                zatoshis(fee),
                subsidy,
                deferred,
                &network,
            ),
            Ok(())
        );
    }
}

#[test]
fn template_coinbase_before_nu7_1_is_v6() {
    let _init_guard = zakura_test::init();
    let network = network();
    let height = (NU7_1_HEIGHT - 1).expect("valid height");

    let template = template(&network, height, Vec::new());
    let coinbase: Transaction = template
        .coinbase_txn
        .data
        .as_ref()
        .zcash_deserialize_into()
        .expect("the coinbase decodes");

    assert!(matches!(coinbase, Transaction::V6 { .. }));
    assert_eq!(coinbase.coinbase_height(), Some(height));
}
