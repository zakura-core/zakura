//! Mining templates pay the miner share and account for the NSM contribution once.

use std::{collections::HashMap, sync::Arc};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{genesis, Block, Hash, Height},
    parameters::{
        subsidy::{block_subsidy, halving_block_subsidy},
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkUpgrade,
    },
    serialization::{DateTime32, ZcashDeserializeInto},
    transaction::{self, LockTime, Transaction, VerifiedUnminedTx},
    transparent::{Input, OrderedUtxo, OutPoint, Output, Script, Utxo},
    value_balance::ValueBalance,
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
};
use zakura_state::GetBlockTemplateChainInfo;
use zcash_address::{ToAddress, ZcashAddress};
use zcash_protocol::consensus::NetworkType;

use super::super::{BlockTemplateResponse, MinerParams};
use crate::config::mining;

#[test]
fn nsm_fee_templates_pay_miner_and_credit_balance_once() {
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(5),
            ..Default::default()
        },
        nsm_reissuance_height: Some(Height(10)),
        ..Default::default()
    });
    let miner = MinerParams::new(
        &network,
        mining::Config {
            miner_address: Some(ZcashAddress::from_transparent_p2pkh(
                NetworkType::Regtest,
                [0x7e; 20],
            )),
            ..Default::default()
        },
    )
    .unwrap();
    let balance = Amount::try_from(400_000_000).unwrap();

    for height in [Height(4), Height(5), Height(9), Height(10), Height(11)] {
        for transaction_count in [0u8, 2] {
            let mut mempool = Vec::new();
            let mut utxos = HashMap::new();
            for index in 1..=transaction_count {
                let outpoint = OutPoint {
                    hash: transaction::Hash([index; 32]),
                    index: 0,
                };
                let spent_output = Output::new(Amount::try_from(20_000).unwrap(), Script::new(&[]));
                utxos.insert(outpoint, Utxo::new(spent_output.clone(), Height(1), false));
                let transaction = Transaction::V5 {
                    network_upgrade: NetworkUpgrade::current(&network, height),
                    lock_time: LockTime::unlocked(),
                    expiry_height: height,
                    inputs: vec![Input::PrevOut {
                        outpoint,
                        unlock_script: Script::new(&[]),
                        sequence: u32::MAX,
                    }],
                    outputs: vec![Output::new(
                        Amount::try_from(9_999).unwrap(),
                        Script::new(&[]),
                    )],
                    sapling_shielded_data: None,
                    orchard_shielded_data: None,
                };
                let verified = VerifiedUnminedTx::new(
                    Arc::new(transaction).into(),
                    Amount::try_from(10_001).unwrap(),
                    0,
                    0,
                    Arc::new(vec![spent_output]),
                )
                .unwrap();
                mempool.push((0, verified));
            }
            let mut pools = ValueBalance::zero();
            pools.set_nsm_value_balance_amount(balance.constrain().unwrap());
            let chain_info = GetBlockTemplateChainInfo {
                value_pools: pools,
                expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
                tip_height: height.previous().unwrap(),
                tip_hash: Hash([1; 32]),
                cur_time: DateTime32::from(1_654_008_617),
                min_time: DateTime32::from(1_654_008_606),
                max_time: DateTime32::from(1_654_008_728),
                chain_history_root: Some(
                    zakura_chain::block::CHAIN_HISTORY_ACTIVATION_RESERVED.into(),
                ),
            };
            let template = BlockTemplateResponse::new_internal(
                &network,
                None,
                &miner,
                &chain_info,
                "0".repeat(46).parse().unwrap(),
                mempool,
                None,
            )
            .unwrap();
            let coinbase: Transaction = template
                .coinbase_txn
                .data
                .as_ref()
                .zcash_deserialize_into()
                .unwrap();
            let fees = if transaction_count == 0 { 0 } else { 20_002 };
            // Rounding two fees of 10,001 separately would contribute 12,000, not 12,001.
            let contribution = if cfg!(feature = "nu7") && height >= Height(5) && fees > 0 {
                12_001
            } else {
                0
            };
            let miner_fees = fees - contribution;
            assert_eq!(i64::from(template.coinbase_txn.fee), -miner_fees);
            assert!(template
                .transactions
                .iter()
                .all(|tx| i64::from(tx.fee) == 10_001));
            let subsidy = block_subsidy(height, &network, Some(balance)).unwrap();
            let coinbase_value = coinbase
                .outputs()
                .iter()
                .map(Output::value)
                .sum::<Result<Amount<NonNegative>, _>>()
                .unwrap();
            assert_eq!(i64::from(coinbase_value), i64::from(subsidy) + miner_fees);

            let mut block: Block = (*genesis::regtest_genesis_block()).clone();
            block.transactions = vec![Arc::new(coinbase)];
            block.transactions.extend(
                template
                    .transactions
                    .iter()
                    .map(|tx| Arc::new(tx.data.as_ref().zcash_deserialize_into().unwrap())),
            );
            let change = block
                .chain_value_pool_change(&network, &utxos, None)
                .unwrap();
            let bonus =
                i64::from(subsidy) - i64::from(halving_block_subsidy(height, &network).unwrap());
            assert_eq!(
                i64::from(change.nsm_value_balance_amount()),
                contribution - bonus
            );
            assert_eq!(
                i64::from(change.total().unwrap()),
                i64::from(subsidy) - contribution
            );
            let ordered = utxos
                .into_iter()
                .map(|(outpoint, utxo)| {
                    (
                        outpoint,
                        OrderedUtxo {
                            utxo,
                            tx_index_in_block: 1,
                        },
                    )
                })
                .collect();
            assert_eq!(
                block
                    .chain_value_pool_change_from_ordered_utxos(&network, &ordered, None)
                    .unwrap(),
                change,
            );
        }
    }
}
