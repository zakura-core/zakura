//! Compare the candidate against the pinned production C++ adapter.

#[allow(dead_code)]
mod baseline;
#[cfg(test)]
mod differential;

#[cfg(test)]
use baseline::parse_zip244_hash_type;
use std::sync::Arc;
use zakura_chain::{parameters::NetworkUpgrade, transaction::Transaction, transparent};
pub use zakura_script::{p2sh_sigop_count, Error, Sigops};
use zcash_script::script::Evaluable as _;

/// Run both adapters for every transaction regression from zakura-script.
pub struct CachedFfiTransaction {
    candidate: zakura_script::CachedFfiTransaction,
    baseline: baseline::CachedFfiTransaction,
}

impl CachedFfiTransaction {
    pub fn new(
        tx: Arc<Transaction>,
        outputs: Arc<Vec<transparent::Output>>,
        nu: NetworkUpgrade,
    ) -> Result<Self, Error> {
        let baseline = baseline::CachedFfiTransaction::new(tx.clone(), outputs.clone(), nu);
        let candidate = zakura_script::CachedFfiTransaction::new(tx, outputs, nu);
        assert_eq!(
            candidate.is_ok(),
            baseline.is_ok(),
            "constructor acceptance differs"
        );
        candidate.map(|candidate| Self {
            candidate,
            baseline: baseline.unwrap(),
        })
    }

    pub fn is_valid(&self, index: usize) -> Result<(), Error> {
        let candidate = self.candidate.is_valid(index);
        let baseline = self.baseline.is_valid(index);
        assert_eq!(
            candidate.is_ok(),
            baseline.is_ok(),
            "input {index}: Rust={candidate:?}, C++={baseline:?}"
        );
        candidate
    }

    pub fn p2sh_sigops(&self) -> u32 {
        let candidate = self.candidate.p2sh_sigops();
        let cpp: u32 = self
            .baseline
            .inputs()
            .iter()
            .zip(self.baseline.all_previous_outputs())
            .filter_map(|(input, output)| {
                let transparent::Input::PrevOut { unlock_script, .. } = input else {
                    return None;
                };
                let key = output.lock_script.as_raw_bytes();
                zcash_script::script::Code(key.to_vec())
                    .is_pay_to_script_hash()
                    .then(|| cxx_p2sh_sigops(key, unlock_script.as_raw_bytes()))
            })
            .sum();
        assert_eq!(candidate, cpp, "P2SH accounting differs");
        candidate
    }
}

impl Sigops for CachedFfiTransaction {
    fn scripts(&self) -> impl Iterator<Item = Vec<u8>> {
        self.candidate.scripts()
    }
}

#[cfg(test)]
#[path = "../../../crates/zakura-script/src/tests.rs"]
mod transaction_tests;

unsafe extern "C" {
    fn oracle_sigops(bytes: *const u8, size: usize, accurate: bool) -> u32;
    fn oracle_p2sh_sigops(key: *const u8, key_size: usize, sig: *const u8, sig_size: usize) -> u32;
}

/// Count sigops with the pinned C++ CScript implementation.
pub fn cxx_sigops(bytes: &[u8], accurate: bool) -> u32 {
    // SAFETY: C++ copies the bounded slice and retains no pointer.
    unsafe { oracle_sigops(bytes.as_ptr(), bytes.len(), accurate) }
}

/// Extract and count a P2SH redeem script with the pinned C++ implementation.
pub fn cxx_p2sh_sigops(key: &[u8], sig: &[u8]) -> u32 {
    // SAFETY: C++ copies both bounded slices and retains no pointer.
    unsafe { oracle_p2sh_sigops(key.as_ptr(), key.len(), sig.as_ptr(), sig.len()) }
}

/// Count a transaction's legacy sigops with the frozen production C++ adapter.
pub fn cxx_transaction_sigops(tx: &Transaction) -> Result<u32, libzcash_script::Error> {
    baseline::Sigops::sigops(tx)
}
