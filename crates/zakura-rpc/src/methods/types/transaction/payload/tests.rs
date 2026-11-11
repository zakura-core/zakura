use std::sync::{Arc, Barrier};

use zakura_chain::{
    amount::NonNegative,
    block::Height,
    parameters::NetworkUpgrade,
    transaction::{self, LockTime, Transaction, VerifiedUnminedTx},
    transparent::{Input, OutPoint, Output, Script},
};

use super::*;
use crate::{
    methods::types::transaction::TransactionTemplate,
    server::jsonrpc::{Id, MethodResponse},
};

fn transaction(index: u8, version: u32, signature: u8) -> UnminedTx {
    let inputs = vec![Input::PrevOut {
        outpoint: OutPoint {
            hash: transaction::Hash([index; 32]),
            index: 0,
        },
        unlock_script: Script::new(&[signature]),
        sequence: 0,
    }];
    let outputs = vec![Output::new(1u64.try_into().unwrap(), Script::new(&[]))];
    match version {
        5 => Transaction::V5 {
            network_upgrade: NetworkUpgrade::Nu5,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(100),
            inputs,
            outputs,
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        },
        6 => Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu7,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(100),
            inputs,
            outputs,
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        },
        _ => Transaction::V1 {
            inputs,
            outputs,
            lock_time: LockTime::unlocked(),
        },
    }
    .into()
}

fn verified(tx: UnminedTx) -> VerifiedUnminedTx {
    VerifiedUnminedTx::new(tx, 10_000u64.try_into().unwrap(), 0, 0, Arc::new(vec![])).unwrap()
}

#[test]
fn cold_and_reused_payloads_preserve_json_and_fresh_metadata() {
    let tx = verified(transaction(1, 6, 0));
    let cache = TransactionDataCache::default();
    let cold = TransactionTemplate::from_cached(&tx, &cache);
    let mut warm = TransactionTemplate::from_cached(&tx, &cache);
    assert!(Arc::ptr_eq(&cold.data.0, &warm.data.0));
    assert_eq!(
        serde_json::to_vec(&cold).unwrap(),
        serde_json::to_vec(&warm).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&warm).unwrap()["data"],
        hex::encode(cold.data.as_ref())
    );
    warm.depends = vec![1, 2];
    let mut updated = tx.clone();
    updated.miner_fee = 20_000u64.try_into().unwrap();
    updated.p2sh_sigop_count = 3;
    let fresh = TransactionTemplate::from_cached(&updated, &cache);
    assert!(Arc::ptr_eq(&cold.data.0, &fresh.data.0));
    assert_eq!(fresh.fee, updated.miner_fee);
    assert_eq!(fresh.sigops, 3);
    assert!(cold.depends.is_empty());
    assert_ne!(cold.fee, fresh.fee);
    assert_eq!(
        serde_json::to_value(&warm).unwrap()["depends"],
        serde_json::json!([1, 2])
    );
    let decoded: TransactionTemplate<NonNegative> =
        serde_json::from_slice(&serde_json::to_vec(&cold).unwrap()).unwrap();
    assert_eq!(cold, decoded);
}

#[test]
fn authorizing_bytes_do_not_alias_under_the_same_mined_id() {
    for version in [5, 6] {
        let first = transaction(1, version, 0);
        let second = transaction(1, version, 1);
        assert_eq!(first.id().mined_id(), second.id().mined_id());
        assert_ne!(first.id(), second.id());
        let cache = TransactionDataCache::default();
        let first = cache.get(&first);
        let second = cache.get(&second);
        assert_ne!(first.as_ref(), second.as_ref());
        assert!(!Arc::ptr_eq(&first.0, &second.0));
    }
}

#[test]
fn legacy_transactions_keep_their_full_byte_identity() {
    let cache = TransactionDataCache::default();
    let first = transaction(1, 1, 0);
    let second = transaction(1, 1, 1);
    assert_ne!(first.id(), second.id());
    assert_ne!(cache.get(&first).as_ref(), cache.get(&second).as_ref());
}

#[test]
fn independently_constructed_coinbases_keep_their_own_data() {
    use crate::{config::mining, methods::types::get_block_template::MinerParams};
    use zakura_chain::parameters::{testnet::RegtestParameters, Network};
    use zcash_address::{ToAddress, ZcashAddress};
    use zcash_protocol::consensus::NetworkType;

    let network = Network::new_regtest(RegtestParameters::default());
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
    let first = TransactionTemplate::new_coinbase(
        &network,
        Height(1),
        &miner,
        0u64.try_into().unwrap(),
        None,
    )
    .unwrap();
    let second = TransactionTemplate::new_coinbase(
        &network,
        Height(2),
        &miner,
        0u64.try_into().unwrap(),
        None,
    )
    .unwrap();
    assert_ne!(first.data.as_ref(), second.data.as_ref());
    assert!(Arc::ptr_eq(&first.data.0, &first.clone().data.0));
    assert!(!Arc::ptr_eq(&first.data.0, &second.data.0));
}

#[test]
fn concurrent_misses_converge_on_one_retained_payload() {
    let cache = TransactionDataCache::default();
    let tx = transaction(1, 6, 0);
    let barrier = Barrier::new(8);
    let payloads = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    cache.get(&tx)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(payloads.iter().all(|p| Arc::ptr_eq(&p.0, &payloads[0].0)));
    assert_eq!(cache.0.lock().unwrap().entries.len(), 1);
}

#[test]
fn fifo_eviction_keeps_readers_alive_until_they_drop() {
    let cache = TransactionDataCache::new(MAX_BYTES, 2);
    let first_tx = transaction(1, 6, 0);
    let first = cache.get(&first_tx);
    let weak = Arc::downgrade(&first.0);
    let second = cache.get(&transaction(2, 6, 0));
    let _hit = cache.get(&first_tx);
    drop(_hit);
    let third = cache.get(&transaction(3, 6, 0));
    assert!(!cache.0.lock().unwrap().entries.contains_key(&first_tx.id()));
    assert!(weak.upgrade().is_some());
    drop(first);
    assert!(weak.upgrade().is_none());
    drop((second, third));
    assert!(cache.0.lock().unwrap().bytes <= MAX_BYTES);
}

#[test]
fn byte_limit_evicts_and_oversized_payloads_bypass_retention() {
    let cache = TransactionDataCache::new(MAX_BYTES, 2);
    let overhead = cache.0.lock().unwrap().bytes;
    let tx = transaction(1, 6, 0);
    let data = cache.get(&tx);
    cache.0.lock().unwrap().max_bytes = overhead + data.retained_bytes();
    cache.get(&transaction(2, 6, 0));
    assert_eq!(cache.0.lock().unwrap().entries.len(), 1);
    assert!(!cache.0.lock().unwrap().entries.contains_key(&tx.id()));
    let cache = TransactionDataCache::new(overhead, 2);
    assert_eq!(cache.get(&tx).as_ref(), data.as_ref());
    assert!(cache.0.lock().unwrap().entries.is_empty());
}

#[test]
fn payloads_keep_hex_input_rules_and_normal_string_escaping() {
    let data: TransactionData = serde_json::from_str("\"aAbB\"").unwrap();
    assert_eq!(serde_json::to_string(&data).unwrap(), "\"aabb\"");
    for input in ["\"0x12\"", "\"0X12\"", "\"abc\"", "\"zz\""] {
        assert!(serde_json::from_str::<TransactionData>(input).is_err());
    }
    let value = serde_json::json!({"payload": data, "text": "\"\\\n"});
    let encoded = serde_json::to_string(&value).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
        value
    );
}

#[test]
fn cached_payloads_obey_the_complete_response_limit() {
    let tx = verified(transaction(1, 6, 0));
    let cache = TransactionDataCache::default();
    for _ in 0..2 {
        let template = TransactionTemplate::from_cached(&tx, &cache);
        let result = Ok(template);
        let expected = MethodResponse::response(Id::Null, &result, usize::MAX);
        let size = expected.as_json().get().len();
        assert_eq!(
            MethodResponse::response(Id::Null, &result, size)
                .as_json()
                .get(),
            expected.as_json().get()
        );
        let oversized = MethodResponse::response(Id::Null, &result, size - 1);
        let json: serde_json::Value = serde_json::from_str(oversized.as_json().get()).unwrap();
        assert!(json.get("error").is_some());
        assert!(json.get("result").is_none());
    }
}
