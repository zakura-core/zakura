#![no_main]
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;
use zakura_chain::{
    block::Height,
    parameters::NetworkUpgrade,
    serialization::ZcashDeserialize,
    transaction::{Hash, LockTime, Transaction},
    transparent::{Input, OutPoint, Output, Script},
};
use zakura_script_oracle::CachedFfiTransaction;

fuzz_target!(|data: (u8, u32, u32, Vec<u8>, Vec<u8>)| {
    let (version, lock_time, sequence, sig, pub_key) = data;
    if sig.len() > 10_001 || pub_key.len() > 10_001 {
        return;
    }
    let inputs = vec![Input::PrevOut {
        outpoint: OutPoint {
            hash: Hash([0; 32]),
            index: 0,
        },
        unlock_script: Script::new(&sig),
        sequence,
    }];
    let output = Output {
        value: 1u64.try_into().unwrap(),
        lock_script: Script::new(&pub_key),
    };
    let outputs = vec![output.clone()];
    let lock_time = LockTime::zcash_deserialize(&lock_time.to_le_bytes()[..]).unwrap();
    let (tx, nu) = match version % 3 {
        0 => (
            Transaction::V4 {
                inputs,
                outputs,
                lock_time,
                expiry_height: Height(0),
                joinsplit_data: None,
                sapling_shielded_data: None,
            },
            NetworkUpgrade::Canopy,
        ),
        1 => (
            Transaction::V5 {
                inputs,
                outputs,
                lock_time,
                expiry_height: Height(0),
                network_upgrade: NetworkUpgrade::Nu5,
                sapling_shielded_data: None,
                orchard_shielded_data: None,
            },
            NetworkUpgrade::Nu5,
        ),
        _ => (
            Transaction::V6 {
                inputs,
                outputs,
                lock_time,
                expiry_height: Height(0),
                network_upgrade: NetworkUpgrade::Nu6_3,
                sapling_shielded_data: None,
                orchard_shielded_data: None,
                ironwood_shielded_data: None,
            },
            NetworkUpgrade::Nu6_3,
        ),
    };
    if let Ok(verifier) = CachedFfiTransaction::new(Arc::new(tx), Arc::new(vec![output]), nu) {
        let _ = verifier.is_valid(0);
    }
});
