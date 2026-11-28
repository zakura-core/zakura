//! Block production and verbose RPC output for [ZIP 248] v7 transactions.
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use proptest::{
    prelude::any,
    strategy::{Strategy, ValueTree},
    test_runner::TestRunner,
};
use zakura_chain::{
    at_least_one,
    block::Height,
    ironwood::{self, tree},
    parameters::NetworkUpgrade,
    primitives::Halo2Proof,
    transaction::{
        v7::{CoinbaseBundle, MinerPoll, MinerSignal, V7Fields, ValueDeltas},
        LockTime,
    },
    transparent::{self, Input, OutPoint, Output},
};

use super::*;

#[test]
fn coinbase_is_v7_from_nu7_1_unless_it_pays_a_lockbox_disbursement() {
    assert_eq!(coinbase_version(BranchId::Nu7, false), TxVersion::V6);
    assert_eq!(coinbase_version(BranchId::Nu7_1, false), TxVersion::V7);
    assert_eq!(coinbase_version(BranchId::Nu7_1, true), TxVersion::V6);
}

fn amount(zatoshis: i64) -> Amount<NegativeAllowed> {
    zatoshis.try_into().expect("valid amount")
}

fn v7(
    inputs: Vec<Input>,
    outputs: Vec<Output>,
    ironwood_shielded_data: Option<ironwood::ShieldedData>,
    v7_fields: V7Fields,
) -> Arc<Transaction> {
    Arc::new(Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1_000),
        inputs,
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: ironwood_shielded_data.map(Box::new),
        v7_fields: Box::new(v7_fields),
    })
}

fn output(zatoshis: i64) -> Output {
    Output {
        value: zatoshis.try_into().expect("valid amount"),
        lock_script: transparent::Script::new(&[0x51]),
    }
}

/// The verbose output of `tx` as a mined transaction.
fn transaction_json(tx: Arc<Transaction>) -> serde_json::Value {
    let object = TransactionObject::from_transaction(
        tx.clone(),
        Some(Height(1_000)),
        Some(1),
        &Network::Mainnet,
        None,
        None,
        Some(true),
        tx.hash(),
    );
    serde_json::to_value(object).expect("the transaction object serializes")
}

/// A v7 transfer lists its value pool deltas, including its stated fee, and has no
/// coinbase bundle.
#[test]
fn v7_transparent_transaction_object() {
    let _init_guard = zakura_test::init();
    let tx = v7(
        vec![Input::PrevOut {
            outpoint: OutPoint {
                hash: transaction::Hash([1; 32]),
                index: 0,
            },
            unlock_script: transparent::Script::new(&[0x51]),
            sequence: u32::MAX,
        }],
        vec![output(90_000)],
        None,
        V7Fields {
            value_deltas: ValueDeltas {
                transparent: amount(10_000),
                fee: amount(-10_000),
                ..ValueDeltas::default()
            },
            coinbase: None,
        },
    );

    let json = transaction_json(tx);
    assert_eq!(json["version"], 7);
    assert_eq!(json["versiongroupid"], "fffffff7");
    assert!(json.get("coinbase").is_none());
    insta::assert_json_snapshot!("v7_transparent_transaction", json);
}

/// A v7 coinbase has no coinbase input: its height, issuance, and miner data appear in
/// its coinbase bundle instead, and its deltas show what it releases.
#[test]
fn v7_coinbase_transaction_object() {
    let _init_guard = zakura_test::init();
    let poll = MinerPoll::new(
        vec![
            MinerSignal {
                signal_code: 1,
                level_of_support: 2,
            },
            MinerSignal {
                signal_code: 3,
                level_of_support: 0,
            },
        ],
        [0xab; 16],
    )
    .expect("the signal codes ascend");
    let coinbase = CoinbaseBundle::new(
        Height(1_000),
        625_000_000.try_into().expect("valid"),
        75_000_000.try_into().expect("valid"),
        "🌸: pool".as_bytes().to_vec(),
        Some(poll),
    )
    .expect("the coinbase fields are in range");
    let tx = v7(
        Vec::new(),
        vec![output(550_004_000)],
        None,
        V7Fields {
            value_deltas: ValueDeltas {
                transparent: amount(-550_004_000),
                coinbase: amount(549_994_000),
                fee: amount(10_000),
                ..ValueDeltas::default()
            },
            coinbase: Some(coinbase),
        },
    );

    let json = transaction_json(tx);
    assert_eq!(json["vin"], serde_json::json!([]));
    assert_eq!(json["coinbase"]["blockHeight"], 1_000);
    insta::assert_json_snapshot!("v7_coinbase_transaction", json);
}

/// A v7 transaction's shielded bundles appear as in earlier versions, and their
/// balancing values also appear among its value pool deltas.
#[test]
fn v7_shielded_transaction_object() {
    let _init_guard = zakura_test::init();
    let action = any::<ironwood::Action>()
        .new_tree(&mut TestRunner::deterministic())
        .expect("the action strategy creates a value")
        .current();
    let ironwood = ironwood::ShieldedData {
        flags: ironwood::Flags::ENABLE_SPENDS | ironwood::Flags::ENABLE_OUTPUTS,
        value_balance: amount(15_000),
        shared_anchor: tree::Root::default(),
        proof: Halo2Proof(vec![1; ::orchard::Proof::expected_proof_size(1)]),
        actions: at_least_one![ironwood::AuthorizedAction {
            action,
            spend_auth_sig: [7u8; 64].into(),
        }],
        binding_sig: [9u8; 64].into(),
    };
    let tx = v7(
        Vec::new(),
        Vec::new(),
        Some(ironwood),
        V7Fields {
            value_deltas: ValueDeltas {
                fee: amount(-15_000),
                ..ValueDeltas::default()
            },
            coinbase: None,
        },
    );

    let mut json = transaction_json(tx);
    assert_eq!(json["ironwood"]["valueBalanceZat"], 15_000);
    // The txid and auth digest commit to these bulk bytes.
    json["hex"] = "[elided]".into();
    json["ironwood"]["proof"] = "[elided]".into();
    insta::assert_json_snapshot!("v7_shielded_transaction", json);
}
