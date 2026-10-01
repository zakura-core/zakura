//! Structure-aware inputs for the differential checks, generated from fuzzer bytes.
//!
//! Signatures are real: each one signs the digest the verifier will compute for its script code
//! and hash type, so generated spends reach successful signature checks as well as every
//! failure path.

mod script;
mod sign;
mod transaction;

pub use script::Spend;
pub use transaction::TransactionCase;

use arbitrary::{Result, Unstructured};
use sha2::{Digest as _, Sha256};
use zcash_script::script::Raw;

use crate::{check_script, Verdict};

/// The hash types a script-level verifier rejects.
#[derive(Clone, Copy, Debug)]
enum Rejected {
    /// Accept every hash type, like V4 transactions.
    None,
    /// Accept only the six canonical hash types, like ZIP-244 transactions.
    NonCanonical,
    /// Reject every hash type.
    All,
}

/// A spend checked against a synthetic sighash.
#[derive(Clone, Debug)]
pub struct ScriptCase {
    spend: Spend,
    lock_time: u32,
    is_final: bool,
    rejected: Rejected,
    /// Whether the sighash commits to the script code, as V4 sighashes do.
    binds_code: bool,
}

impl ScriptCase {
    /// Generates a case.
    pub fn arbitrary(u: &mut Unstructured) -> Result<Self> {
        let rejected = *u.choose(&[
            Rejected::None,
            Rejected::NonCanonical,
            Rejected::NonCanonical,
            Rejected::All,
        ])?;
        let binds_code = u.ratio(3, 4)?;
        let digest = |code: &[u8], hash_type: u8| digest(binds_code, code, hash_type);
        let spend = script::Builder::new(u, &digest).spend()?;
        Ok(Self {
            spend,
            lock_time: script::lock_time(u)?,
            is_final: u.arbitrary()?,
            rejected,
            binds_code,
        })
    }

    /// Runs [`check_script`].
    pub fn check(&self) -> Verdict {
        let raw = Raw::from_raw_parts(
            self.spend.script_sig.clone(),
            self.spend.script_pubkey.clone(),
        );
        check_script(&raw, self.lock_time, self.is_final, &|code, hash_type| {
            let hash_type = u8::try_from(hash_type.raw_bits()).expect("hash types are one byte");
            sighash(self.rejected, self.binds_code, &code.0, hash_type)
        })
    }

    /// Whether accepting this case requires a successful signature check.
    pub fn needs_signature(&self) -> bool {
        self.spend.needs_signature
    }
}

/// The synthetic sighash, or `None` if the verifier rejects the hash type.
fn sighash(rejected: Rejected, binds_code: bool, code: &[u8], hash_type: u8) -> Option<[u8; 32]> {
    let canonical = matches!(hash_type, 0x01..=0x03 | 0x81..=0x83);
    match rejected {
        Rejected::All => None,
        Rejected::NonCanonical if !canonical => None,
        _ => Some(digest(binds_code, code, hash_type)),
    }
}

/// A hash of the script code (if bound) and the hash type.
fn digest(binds_code: bool, code: &[u8], hash_type: u8) -> [u8; 32] {
    let mut hasher = Sha256::new();
    if binds_code {
        hasher.update(code);
    }
    hasher.update([hash_type]);
    hasher.finalize().into()
}

/// Takes up to `max_len` bytes, fewer if the input runs out.
fn bytes(u: &mut Unstructured, max_len: usize) -> Result<Vec<u8>> {
    let len = u.int_in_range(0..=max_len)?.min(u.len());
    Ok(u.bytes(len)?.to_vec())
}
