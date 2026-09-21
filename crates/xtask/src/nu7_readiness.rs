//! Runs outside cfg(test) and without proptest-impl, so test-only consensus IDs
//! cannot accidentally satisfy the production release prerequisite.

use std::sync::Arc;
use zakura_chain::{
    amount::Amount,
    block::Height,
    parameters::NetworkUpgrade,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{HashType, LockTime, SigHasher, Transaction},
    transparent::{Input, OutPoint, Output, Script},
};

pub(crate) fn run() -> Result<(), super::BoxError> {
    let upgrade = NetworkUpgrade::Nu7;
    let branch = upgrade.branch_id().ok_or(
        "NU7 release blocked: no production consensus branch ID; unit-test IDs are not deployment support",
    )?;
    let raw = u32::from(branch);
    if matches!(raw, 0xfffffffd..=0xffffffff) {
        return Err(
            "NU7 release blocked: a test/unstable placeholder branch ID is configured".into(),
        );
    }
    // NU7 deployment draft, as implemented by valargroup/librustzcash#76.
    if raw != 0x7719_0ad8 {
        return Err("NU7 release blocked: branch ID differs from the deployment draft".into());
    }
    let dependency_branch = zcash_protocol::consensus::BranchId::try_from(raw).map_err(|_| {
        "NU7 release blocked: zcash_protocol does not recognize the configured branch ID"
    })?;
    if u32::from(dependency_branch) != raw || NetworkUpgrade::try_from(raw)? != upgrade {
        return Err("NU7 release blocked: branch ID round trip disagrees".into());
    }
    let previous = Output::new(Amount::try_from(20_001i64)?, Script::new(&[0x51]));
    let tx = Transaction::V5 {
        network_upgrade: upgrade,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(104),
        inputs: vec![Input::PrevOut {
            outpoint: OutPoint {
                hash: zakura_chain::transaction::Hash([1; 32]),
                index: 0,
            },
            unlock_script: Script::new(&[]),
            sequence: u32::MAX,
        }],
        outputs: vec![Output::new(
            Amount::try_from(10_000i64)?,
            Script::new(&[0x51]),
        )],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    };
    let v6 = Transaction::V6 {
        network_upgrade: upgrade,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(104),
        inputs: tx.inputs().to_vec(),
        outputs: tx.outputs().to_vec(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    };
    for tx in [tx, v6] {
        let bytes = tx.zcash_serialize_to_vec()?;
        let decoded: Transaction = bytes.as_slice().zcash_deserialize_into()?;
        if tx != decoded || tx.hash() != decoded.hash() {
            return Err(
                "NU7 release blocked: transaction serialization/hash round trip disagrees".into(),
            );
        }
        let previous = Arc::new(vec![previous.clone()]);
        let hasher = SigHasher::new(&tx, upgrade, previous.clone())?;
        let decoded_hasher = SigHasher::new(&decoded, upgrade, previous.clone())?;
        if hasher.sighash(HashType::ALL, Some((0, vec![])))
            != decoded_hasher.sighash(HashType::ALL, Some((0, vec![])))
        {
            return Err("NU7 release blocked: signature hash round trip disagrees".into());
        }
        if SigHasher::new(&tx, NetworkUpgrade::Nu6_3, previous).is_ok() {
            return Err("NU7 release blocked: previous-upgrade signature context accepted".into());
        }
        let mut invalid = bytes;
        invalid[8..12].copy_from_slice(&0u32.to_le_bytes());
        if invalid
            .as_slice()
            .zcash_deserialize_into::<Transaction>()
            .is_ok()
        {
            return Err("NU7 release blocked: unknown branch ID decoded successfully".into());
        }
    }
    println!("NU7 production branch, wire, digest, and dependency compatibility passed");
    Ok(())
}
