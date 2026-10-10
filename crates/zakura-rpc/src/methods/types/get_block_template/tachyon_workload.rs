//! Internally generated Tachyon transactions for proof-of-work-disabled test networks.

use std::{collections::HashSet, sync::Arc, time::Duration};

use rand_10::{rngs::StdRng, SeedableRng};
use tokio::time::timeout;
use tower::{BoxError, Service, ServiceExt};
use zcash_tachyon::{
    action, bundle,
    entropy::ActionEntropy,
    keys::private,
    note::{CommitmentTrapdoor, Note},
    nullifier, value, TachyonBundle,
};

use zakura_chain::{
    amount::Amount,
    block::{self, Height},
    parameters::{Network, NetworkUpgrade},
    transaction::{HashType, LockTime, Transaction, UnminedTx, VerifiedUnminedTx},
    transparent::{self, MIN_TRANSPARENT_COINBASE_MATURITY},
};
use zakura_state::{ReadRequest, ReadResponse};

/// The number of independently proof-stamped transactions generated in each eligible block.
pub const TRANSACTIONS_PER_BLOCK: usize = 3;

/// HASH160 of the one-byte `OP_TRUE` redeem script used by the workload's coinbase outputs.
pub const REDEEM_SCRIPT_HASH: [u8; 20] = [
    0xda, 0x17, 0x45, 0xe9, 0xb5, 0x49, 0xbd, 0x0b, 0xfa, 0x1a, 0x56, 0x99, 0x71, 0xc7, 0x7e, 0xba,
    0x30, 0xcd, 0x5a, 0x4b,
];

const OP_TRUE: u8 = 0x51;
const WORKLOAD_FEE: u64 = 50_000;
const STATE_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const GENERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Generate transactions that shield the first newly mature internal-miner reward into Tachyon.
///
/// The miner reward at each height is split into [`TRANSACTIONS_PER_BLOCK`] outputs. Exactly when
/// that reward matures, this function spends every matching output into a separately generated
/// Tachyon recipient. Recipient randomness and mock-Ragu proof randomness are seeded by the source
/// outpoint, so repeated templates at the same height contain identical transactions.
pub async fn generate_transactions<S>(
    network: &Network,
    candidate_height: Height,
    tip_hash: block::Hash,
    read_state: S,
) -> Vec<VerifiedUnminedTx>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Send + Clone + 'static,
    S::Future: Send + 'static,
{
    if NetworkUpgrade::current(network, candidate_height) != NetworkUpgrade::NuTachyon {
        return Vec::new();
    }

    let Some(source_height) = candidate_height
        .0
        .checked_sub(MIN_TRANSPARENT_COINBASE_MATURITY)
        .map(Height)
        .filter(|height| *height > Height::MIN)
    else {
        return Vec::new();
    };

    let block_request = read_state
        .clone()
        .oneshot(ReadRequest::Block(source_height.into()));
    let mining_request = read_state.oneshot(ReadRequest::TachyonMiningData {
        anchors: HashSet::new(),
        tachygrams: HashSet::new(),
        tip_hash,
        candidate_height,
    });

    let responses = timeout(STATE_QUERY_TIMEOUT, async {
        tokio::try_join!(block_request, mining_request)
    })
    .await;

    let (source_block, tip_anchor) = match responses {
        Ok(Ok((
            ReadResponse::Block(Some(source_block)),
            ReadResponse::TachyonMiningData(Some(mining_data)),
        ))) => (source_block, mining_data.tip_anchor),
        Ok(Ok((_block, _mining_data))) => {
            tracing::warn!(?source_height, "could not load Tachyon workload state");
            return Vec::new();
        }
        Ok(Err(error)) => {
            tracing::warn!(?error, "could not query Tachyon workload state");
            return Vec::new();
        }
        Err(_) => {
            tracing::warn!("timed out querying Tachyon workload state");
            return Vec::new();
        }
    };

    let workload_script = workload_address(network).script();
    let generation = tokio::task::spawn_blocking(move || {
        build_transactions(
            candidate_height,
            source_height,
            source_block,
            workload_script,
            tip_anchor,
        )
    });
    match timeout(GENERATION_TIMEOUT, generation).await {
        Ok(Ok(transactions)) => transactions,
        Ok(Err(error)) => {
            tracing::warn!(?error, "Tachyon workload generation task failed");
            Vec::new()
        }
        Err(_) => {
            tracing::warn!("timed out generating Tachyon workload transactions");
            Vec::new()
        }
    }
}

fn build_transactions(
    candidate_height: Height,
    source_height: Height,
    source_block: Arc<block::Block>,
    workload_script: transparent::Script,
    tip_anchor: zakura_chain::tachyon::Anchor,
) -> Vec<VerifiedUnminedTx> {
    let Some(coinbase) = source_block.transactions.first() else {
        tracing::warn!(
            ?source_height,
            "Tachyon workload source block has no coinbase"
        );
        return Vec::new();
    };

    let source_hash = coinbase.hash();
    let mut transactions = Vec::with_capacity(TRANSACTIONS_PER_BLOCK);

    for (index, output) in coinbase.outputs().iter().enumerate() {
        if output.lock_script != workload_script {
            continue;
        }

        let outpoint = transparent::OutPoint::from_usize(source_hash, index);
        match build_transaction(candidate_height, outpoint, output.clone(), tip_anchor) {
            Ok(transaction) => transactions.push(transaction),
            Err(error) => {
                tracing::warn!(
                    ?source_height,
                    index,
                    %error,
                    "could not build Tachyon workload transaction"
                );
                return Vec::new();
            }
        }
    }

    if transactions.len() != TRANSACTIONS_PER_BLOCK {
        tracing::warn!(
            ?source_height,
            expected = TRANSACTIONS_PER_BLOCK,
            actual = transactions.len(),
            "mature coinbase does not have the expected Tachyon workload outputs",
        );
        return Vec::new();
    }

    transactions
}

fn workload_address(network: &Network) -> transparent::Address {
    transparent::Address::from_script_hash(network.t_addr_kind(), REDEEM_SCRIPT_HASH)
}

fn build_transaction(
    candidate_height: Height,
    outpoint: transparent::OutPoint,
    source_output: transparent::Output,
    anchor: zakura_chain::tachyon::Anchor,
) -> Result<VerifiedUnminedTx, String> {
    let input_value = u64::try_from(source_output.value.zatoshis())
        .map_err(|error| format!("invalid source value: {error}"))?;
    let output_value = input_value
        .checked_sub(WORKLOAD_FEE)
        .ok_or_else(|| "miner reward part is smaller than the workload fee".to_string())?;

    let mut seed: [u8; 32] = outpoint.hash.into();
    for (seed_byte, index_byte) in seed
        .iter_mut()
        .zip(outpoint.index.to_le_bytes().iter().cycle())
    {
        *seed_byte ^= index_byte;
    }
    let mut rng = StdRng::from_seed(seed);

    let spending_key = private::SpendingKey::random(&mut rng);
    let note = Note {
        pk: spending_key.derive_payment_key(),
        value: value::Positive::try_from(output_value)
            .map_err(|error| format!("invalid Tachyon note value: {error}"))?,
        psi: nullifier::Trapdoor::random(&mut rng),
        rcm: CommitmentTrapdoor::random(&mut rng),
    };
    let output_plan = action::Plan::output(
        note,
        ActionEntropy::random(&mut rng),
        value::Trapdoor::random(&mut rng),
    );
    let plan = bundle::Plan::new(Vec::new(), vec![output_plan]);
    let proof_key = spending_key.derive_proof_private();
    let auth_key = spending_key.derive_auth_private();
    let anchor_bytes: [u8; 32] = anchor.into();
    let anchor = zcash_tachyon::Anchor::read(&anchor_bytes[..])
        .map_err(|error| format!("invalid stored Tachyon anchor: {error}"))?;
    let stamp = plan
        .clone()
        .stamp_plan(anchor)
        .prove(&mut rng, &proof_key, Vec::new())
        .map_err(|error| format!("could not create mock Ragu proof: {error}"))?;

    let placeholder_bundle = plan
        .sign(&mut rng, &[0; 32], &auth_key)
        .map_err(|error| format!("could not create placeholder signatures: {error}"))?
        .stamp(stamp.clone());
    let placeholder = transaction(
        candidate_height,
        outpoint,
        TachyonBundle::Proven(placeholder_bundle),
    );
    let spent_outputs = Arc::new(vec![source_output]);
    let sighash = placeholder
        .sighash(
            NetworkUpgrade::NuTachyon,
            HashType::ALL,
            spent_outputs.clone(),
            None,
        )
        .map_err(|error| format!("could not compute V7 sighash: {error}"))?;

    let bundle = plan
        .sign(&mut rng, sighash.as_ref(), &auth_key)
        .map_err(|error| format!("could not sign Tachyon bundle: {error}"))?
        .stamp(stamp);
    let transaction = transaction(candidate_height, outpoint, TachyonBundle::Proven(bundle));
    let fee = Amount::try_from(WORKLOAD_FEE)
        .map_err(|error| format!("invalid Tachyon workload fee: {error}"))?;

    VerifiedUnminedTx::new(UnminedTx::from(transaction), fee, 0, 0, spent_outputs)
        .map_err(|error| format!("invalid generated transaction: {error}"))
}

fn transaction(
    candidate_height: Height,
    outpoint: transparent::OutPoint,
    bundle: TachyonBundle,
) -> Transaction {
    Transaction::V7 {
        network_upgrade: NetworkUpgrade::NuTachyon,
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: candidate_height,
        zip233_amount: Amount::zero(),
        inputs: vec![transparent::Input::PrevOut {
            outpoint,
            // Push the one-byte OP_TRUE redeem script onto the P2SH script stack.
            unlock_script: transparent::Script::new(&[1, OP_TRUE]),
            sequence: u32::MAX,
        }],
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        tachyon_shielded_data: Some(bundle.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use zakura_chain::{
        amount::NonNegative,
        parameters::testnet::ConfiguredActivationHeights,
        serialization::{ZcashDeserialize, ZcashDeserializeInto},
        transaction::{Hash, WtxId},
    };
    use zakura_node_services::BoxError;
    use zakura_state::TachyonMiningData;
    use zakura_test::mock_service::MockService;

    use super::*;

    #[test]
    fn generated_transaction_has_a_valid_tachyon_bundle() {
        let network = Network::new_regtest(Default::default());
        let source_output = transparent::Output::new(
            Amount::<NonNegative>::try_from(100_000_000u64).expect("test value is valid"),
            workload_address(&network).script(),
        );
        let outpoint = transparent::OutPoint {
            hash: Hash([7; 32]),
            index: 2,
        };

        let generated = build_transaction(
            Height(200),
            outpoint,
            source_output,
            zcash_tachyon::Anchor::default().into(),
        )
        .expect("output-only mock proof can be generated");
        let transaction = generated.transaction.transaction();
        let TachyonBundle::Proven(bundle) = &transaction
            .tachyon_shielded_data()
            .expect("generated transaction has Tachyon data")
            .0
        else {
            panic!("generated transaction must carry its own proof");
        };

        assert!(bundle.is_autonome());
        assert_eq!(transaction.outputs().len(), 0);
        assert_eq!(transaction.inputs().len(), 1);
        assert_eq!(
            generated.miner_fee,
            Amount::<NonNegative>::try_from(WORKLOAD_FEE).unwrap()
        );
        let sighash = transaction
            .sighash(
                NetworkUpgrade::NuTachyon,
                HashType::ALL,
                generated.spent_outputs.clone(),
                None,
            )
            .expect("generated V7 transaction has a sighash");
        let wtxid: [u8; 64] = WtxId::from(transaction.as_ref()).into();
        bundle
            .verify(
                &mut StdRng::from_seed([8; 32]),
                sighash.as_ref(),
                &wtxid,
                &[],
            )
            .expect("generated signatures, tachygrams, coverage, and mock proof are valid");
    }

    #[tokio::test]
    async fn mature_reward_generates_and_aggregates_three_transactions() {
        let network = Network::new_regtest(
            ConfiguredActivationHeights {
                canopy: Some(1),
                nu5: Some(2),
                nu6: Some(3),
                nu6_1: Some(4),
                nu6_2: Some(5),
                nu6_3: Some(6),
                nu7: Some(8),
                nu_tachyon: Some(10),
                ..Default::default()
            }
            .into(),
        );
        let candidate_height = Height(200);
        let source_height = Height(candidate_height.0 - MIN_TRANSPARENT_COINBASE_MATURITY);
        let miner_params = super::super::MinerParams::new(
            &network,
            crate::config::mining::Config {
                internal_miner: true,
                tachyon_workload: true,
                ..Default::default()
            },
        )
        .expect("test network supports the workload");
        let coinbase = crate::methods::types::transaction::TransactionTemplate::new_coinbase(
            &network,
            source_height,
            &miner_params,
            Amount::zero(),
            Some(Amount::zero()),
        )
        .expect("workload coinbase can be built");
        let coinbase = coinbase
            .data
            .as_ref()
            .zcash_deserialize_into()
            .expect("workload coinbase deserializes");
        let mut source_block =
            block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
                .expect("hardcoded genesis block deserializes");
        source_block.transactions = vec![Arc::new(coinbase)];
        let tip_hash = source_block.hash();
        let tip_anchor = zcash_tachyon::Anchor::default().into();

        let mut read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
        let generation =
            generate_transactions(&network, candidate_height, tip_hash, read_state.clone());
        let responses = async {
            read_state
                .expect_request(ReadRequest::Block(source_height.into()))
                .await
                .respond(ReadResponse::Block(Some(Arc::new(source_block))));
            read_state
                .expect_request(ReadRequest::TachyonMiningData {
                    anchors: HashSet::new(),
                    tachygrams: HashSet::new(),
                    tip_hash,
                    candidate_height,
                })
                .await
                .respond(ReadResponse::TachyonMiningData(Some(TachyonMiningData {
                    tip_anchor,
                    anchor_heights: HashMap::new(),
                    blocks: Default::default(),
                    revealed_tachygrams: HashSet::new(),
                })));
        };
        let (generated, ()) = timeout(Duration::from_secs(10), async {
            tokio::join!(generation, responses)
        })
        .await
        .expect("workload generation and state queries complete promptly");

        assert_eq!(generated.len(), TRANSACTIONS_PER_BLOCK);
        assert!(generated.iter().all(|transaction| matches!(
            &transaction
                .transaction
                .transaction()
                .tachyon_shielded_data()
                .expect("generated transaction has Tachyon data")
                .0,
            TachyonBundle::Proven(bundle) if bundle.is_autonome()
        )));

        let anchor_heights = HashMap::from([(tip_anchor, Height(candidate_height.0 - 1))]);
        let aggregation = super::super::tachyon::aggregate_transactions(
            network,
            candidate_height,
            tip_hash,
            read_state.clone(),
            generated,
        );
        let aggregation_response = async {
            read_state
                .expect_request_that(|request| {
                    matches!(request, ReadRequest::TachyonMiningData { .. })
                })
                .await
                .respond(ReadResponse::TachyonMiningData(Some(TachyonMiningData {
                    tip_anchor,
                    anchor_heights,
                    blocks: Default::default(),
                    revealed_tachygrams: HashSet::new(),
                })));
        };
        let (aggregated, ()) = timeout(Duration::from_secs(10), async {
            tokio::join!(aggregation, aggregation_response)
        })
        .await
        .expect("workload aggregation and state queries complete promptly");

        assert_eq!(aggregated.len(), TRANSACTIONS_PER_BLOCK);
        assert!(matches!(
            &aggregated[0]
                .transaction
                .transaction()
                .tachyon_shielded_data()
                .expect("aggregate has Tachyon data")
                .0,
            TachyonBundle::Proven(bundle) if bundle.is_aggregate()
        ));
        assert!(aggregated[1..].iter().all(|transaction| matches!(
            transaction
                .transaction
                .transaction()
                .tachyon_shielded_data()
                .expect("adjunct has Tachyon data")
                .0,
            TachyonBundle::Adjunct(_)
        )));
    }
}
