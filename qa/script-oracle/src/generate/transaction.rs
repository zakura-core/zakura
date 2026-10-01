//! Transactions whose transparent inputs are signed with Zakura's sighasher.

use std::sync::Arc;

use arbitrary::{Result, Unstructured};
use zakura_chain::{
    amount::{Amount, NonNegative, MAX_MONEY},
    block::Height,
    parameters::NetworkUpgrade,
    serialization::ZcashDeserialize as _,
    transaction::{self, HashType, LockTime, SigHasher, Transaction},
    transparent::{Input, OutPoint, Output, Script},
};

use super::script::{self, Builder};
use crate::{check_transaction, Verdict};

/// Transaction versions with the network upgrades they are valid in.
const VERSIONS: &[(u8, NetworkUpgrade)] = &[
    (4, NetworkUpgrade::Sapling),
    (4, NetworkUpgrade::Blossom),
    (4, NetworkUpgrade::Heartwood),
    (4, NetworkUpgrade::Canopy),
    (4, NetworkUpgrade::Nu5),
    (4, NetworkUpgrade::Nu6),
    (5, NetworkUpgrade::Nu5),
    (5, NetworkUpgrade::Nu6),
    (5, NetworkUpgrade::Nu6_1),
    (6, NetworkUpgrade::Nu6_3),
];

/// A transaction, the outputs its inputs spend, and its network upgrade.
#[derive(Debug)]
pub struct TransactionCase {
    transaction: Arc<Transaction>,
    previous_outputs: Arc<Vec<Output>>,
    nu: NetworkUpgrade,
    needs_signature: Vec<bool>,
}

impl TransactionCase {
    /// Generates a case: usually a spend of one to four generated outputs, sometimes a coinbase.
    pub fn arbitrary(u: &mut Unstructured) -> Result<Self> {
        let (version, nu) = *u.choose(VERSIONS)?;
        let lock_time = LockTime::zcash_deserialize(&script::lock_time(u)?.to_le_bytes()[..])
            .expect("every u32 is a lock time");
        let outputs = (0..u.int_in_range(0..=3)?)
            .map(|_| output(u))
            .collect::<Result<Vec<_>>>()?;
        let build = |inputs| build(version, nu, inputs, outputs.clone(), lock_time);

        if u.ratio(1, 10)? {
            let input = Input::Coinbase {
                height: Height(u.int_in_range(1..=3_000_000)?),
                data: coinbase_data(u)?,
                sequence: sequence(u)?,
            };
            return Ok(Self {
                transaction: Arc::new(build(vec![input])),
                previous_outputs: Arc::new(Vec::new()),
                nu,
                needs_signature: vec![false],
            });
        }

        let mut locks = Vec::new();
        let mut previous_outputs = Vec::new();
        let mut prevouts = Vec::new();
        for _ in 0..u.int_in_range(1..=4)? {
            let locked = Builder::new(u, &|_, _| [0; 32]).locked()?;
            previous_outputs.push(Output {
                value: amount(u)?,
                lock_script: Script::new(locked.script_pubkey()),
            });
            let outpoint = OutPoint {
                hash: transaction::Hash(u.arbitrary()?),
                index: u.arbitrary()?,
            };
            prevouts.push((outpoint, sequence(u)?));
            locks.push(locked);
        }
        let previous_outputs = Arc::new(previous_outputs);
        let inputs = |script_sigs: &[Vec<u8>]| {
            prevouts
                .iter()
                .zip(script_sigs)
                .map(|(&(outpoint, sequence), script_sig)| Input::PrevOut {
                    outpoint,
                    unlock_script: Script::new(script_sig),
                    sequence,
                })
                .collect()
        };

        // Neither sighash commits to scriptSigs, so sign a copy with empty ones.
        let unsigned = build(inputs(&vec![Vec::new(); prevouts.len()]));
        let sighasher = unsigned.sighasher(nu, Arc::clone(&previous_outputs)).ok();
        let mut script_sigs = Vec::new();
        let mut needs_signature = Vec::new();
        for (index, locked) in locks.into_iter().enumerate() {
            let digest = |code: &[u8], hash_type: u8| {
                sighasher.as_ref().map_or([0; 32], |sighasher| {
                    digest(sighasher, version, outputs.len(), index, code, hash_type)
                })
            };
            let spend = Builder::new(u, &digest).unlock(locked)?;
            script_sigs.push(spend.script_sig);
            needs_signature.push(spend.needs_signature);
        }
        Ok(Self {
            transaction: Arc::new(build(inputs(&script_sigs))),
            previous_outputs,
            nu,
            needs_signature,
        })
    }

    /// Runs [`check_transaction`].
    pub fn check(&self) -> Option<Vec<Verdict>> {
        check_transaction(
            Arc::clone(&self.transaction),
            Arc::clone(&self.previous_outputs),
            self.nu,
        )
    }

    /// Whether accepting each input requires a successful signature check.
    pub fn needs_signature(&self) -> &[bool] {
        &self.needs_signature
    }
}

/// The sighash both adapters compute for `hash_type`.
///
/// Both adapters reject a V5+ hash type that is not canonical, or `SIGHASH_SINGLE` without a
/// corresponding output. For those, this returns the sighash of the nearest accepted hash type,
/// which a verifier that wrongly accepted the signature would use.
fn digest(
    sighasher: &SigHasher,
    version: u8,
    output_count: usize,
    index: usize,
    code: &[u8],
    hash_type: u8,
) -> [u8; 32] {
    let input = Some((index, code.to_vec()));
    if version < 5 {
        return sighasher.sighash_v4_raw(hash_type, input).0;
    }
    let mut nearest = hash_type & 0x83;
    if nearest & 0x03 == 0 || (nearest & 0x03 == 0x03 && index >= output_count) {
        nearest = (nearest & 0x80) | 0x01;
    }
    let nearest = HashType::from_bits(u32::from(nearest)).expect("nearest is canonical");
    sighasher.sighash(nearest, input).0
}

fn build(
    version: u8,
    network_upgrade: NetworkUpgrade,
    inputs: Vec<Input>,
    outputs: Vec<Output>,
    lock_time: LockTime,
) -> Transaction {
    let expiry_height = Height(0);
    match version {
        4 => Transaction::V4 {
            inputs,
            outputs,
            lock_time,
            expiry_height,
            joinsplit_data: None,
            sapling_shielded_data: None,
        },
        5 => Transaction::V5 {
            network_upgrade,
            lock_time,
            expiry_height,
            inputs,
            outputs,
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        },
        _ => Transaction::V6 {
            network_upgrade,
            lock_time,
            expiry_height,
            inputs,
            outputs,
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        },
    }
}

fn output(u: &mut Unstructured) -> Result<Output> {
    Ok(Output {
        value: amount(u)?,
        lock_script: Script::new(&super::bytes(u, 40)?),
    })
}

fn amount(u: &mut Unstructured) -> Result<Amount<NonNegative>> {
    let zatoshis = if u.ratio(1, 8)? {
        *u.choose(&[0, 1, MAX_MONEY])?
    } else {
        u.int_in_range(0..=100_000_000)?
    };
    Ok(Amount::try_from(zatoshis).expect("generated amounts are in range"))
}

fn sequence(u: &mut Unstructured) -> Result<u32> {
    Ok(if u.ratio(1, 2)? {
        u32::MAX
    } else {
        *u.choose(&[0, 1, u32::MAX - 1, 0x8000_0000])?
    })
}

/// Coinbase data that is mostly sigop opcodes and push prefixes, to stress sigop counting.
fn coinbase_data(u: &mut Unstructured) -> Result<Vec<u8>> {
    (0..u.int_in_range(0..=90)?)
        .map(|_| {
            Ok(if u.ratio(1, 2)? {
                *u.choose(&[0xac, 0xad, 0xae, 0xaf, 0x4c, 0x4d, 0x4e, 0x52])?
            } else {
                u.arbitrary()?
            })
        })
        .collect()
}
