//! Differential checks between Zakura's Rust script verification (the candidate) and the C++
//! adapter it replaced (the baseline).
//!
//! Each `check_*` function panics when the candidate and the baseline disagree. Fuzz targets,
//! seeded tests, and the historical replay share these functions, so they share one definition of
//! agreement.

use std::sync::Arc;

use libzcash_script::{testing::normalize_err, CxxInterpreter, ZcashScript as _};
use zakura_chain::{parameters::NetworkUpgrade, transaction::Transaction, transparent};
use zcash_script::{
    interpreter::{CallbackTransactionSignatureChecker, Flags},
    script,
    signature::HashType,
};

pub mod baseline;
pub mod cxx;
pub mod generate;

/// The flags Zakura passes to the interpreter.
pub const FLAGS: Flags = Flags::P2SH.union(Flags::CHECKLOCKTIMEVERIFY);

/// A sighash callback. `None` rejects the hash type.
pub type Sighash<'a> = &'a dyn Fn(&script::Code, &HashType) -> Option<[u8; 32]>;

/// Whether both implementations accepted or both rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Both implementations accepted.
    Accepted,
    /// Both implementations rejected.
    Rejected,
}

impl Verdict {
    fn from_ok(ok: bool) -> Self {
        if ok {
            Verdict::Accepted
        } else {
            Verdict::Rejected
        }
    }
}

/// Evaluates `script` with the Rust interpreter and the C++ interpreter.
///
/// Each interpreter receives the callback its production adapter built from `sighash`. The
/// candidate passes `None` through. The baseline replaces `None` with a random hash, because the
/// C++ interpreter cannot observe a failed callback.
///
/// # Panics
///
/// Panics if the results differ, after normalizing errors to the cases the C++ code can report.
pub fn check_script(
    script: &script::Raw,
    lock_time: u32,
    is_final: bool,
    sighash: Sighash<'_>,
) -> Verdict {
    let rust = script
        .eval(
            FLAGS,
            &CallbackTransactionSignatureChecker {
                sighash,
                lock_time: lock_time.into(),
                is_final,
            },
        )
        .map_err(|(_, error)| libzcash_script::Error::Script(error).normalize());
    let random_on_none = |code: &script::Code, hash_type: &HashType| {
        Some(sighash(code, hash_type).unwrap_or_else(rand::random))
    };
    let cxx = CxxInterpreter {
        sighash: &random_on_none,
        lock_time,
        is_final,
    }
    .verify_callback(script, FLAGS)
    .map_err(normalize_err);
    assert_eq!(
        rust, cxx,
        "interpreters disagree: script={script} lock_time={lock_time} is_final={is_final}"
    );
    Verdict::from_ok(rust == Ok(true))
}

/// Compares sigop counts of one script.
///
/// - The candidate's legacy count must equal the baseline's C++ legacy count.
/// - Policy counts a P2SH redeem script of at most 520 bytes with `zcash_script`'s accurate
///   counter, which must equal zcashd's accurate count.
///
/// # Panics
///
/// Panics if a count differs.
pub fn check_script_sigops(bytes: &[u8]) {
    let code = script::Code(bytes.to_vec());
    let baseline = CxxInterpreter {
        sighash: &|_, _| None,
        lock_time: 0,
        is_final: true,
    }
    .legacy_sigop_count_script(&code)
    .expect("the C++ adapter counts scripts shorter than 4 GiB");
    assert_eq!(
        zakura_script::legacy_sigop_count(bytes),
        baseline,
        "legacy sigops differ: {}",
        hex::encode(bytes)
    );
    if bytes.len() <= 520 {
        assert_eq!(
            code.sig_op_count(true),
            cxx::sigops(bytes, true),
            "accurate sigops differ: {}",
            hex::encode(bytes)
        );
    }
}

/// Compares the P2SH sigop count of a scriptSig that spends a P2SH output.
///
/// The candidate and the baseline share the Rust P2SH counter, so they must match exactly. That
/// counter skips scriptSigs that `zcash_script` cannot parse, while zcashd's counter reads pushes
/// larger than 520 bytes. A scriptSig where the two differ must fail evaluation in both
/// interpreters, so no valid transaction can contain it.
///
/// # Panics
///
/// Panics if the candidate differs from the baseline, or if zcashd's count differs on a scriptSig
/// that either interpreter evaluates successfully.
pub fn check_p2sh_sigops(script_sig: &[u8]) {
    let p2sh = p2sh_script_pubkey(script_sig);
    let input = transparent::Input::PrevOut {
        outpoint: transparent::OutPoint {
            hash: zakura_chain::transaction::Hash([0; 32]),
            index: 0,
        },
        unlock_script: transparent::Script::new(script_sig),
        sequence: u32::MAX,
    };
    let output = transparent::Output {
        value: zakura_chain::amount::Amount::zero(),
        lock_script: transparent::Script::new(&p2sh),
    };
    let candidate = zakura_script::p2sh_input_sigop_count(&input, &output);
    assert_eq!(
        candidate,
        baseline::p2sh_input_sigop_count(&input, &output),
        "P2SH sigops differ from the baseline: {}",
        hex::encode(script_sig)
    );
    if candidate != cxx::p2sh_sigops(&p2sh, script_sig) {
        // A true scriptPubKey isolates the scriptSig, which every spend evaluates first.
        let raw = script::Raw::from_raw_parts(script_sig.to_vec(), vec![0x51]);
        assert_eq!(
            check_script(&raw, 0, true, &|_, _| None),
            Verdict::Rejected,
            "zcashd counts P2SH sigops differently in an executable scriptSig: {}",
            hex::encode(script_sig)
        );
    }
}

/// Returns the P2SH scriptPubKey `OP_HASH160 <hash160(redeem)> OP_EQUAL`.
pub fn p2sh_script_pubkey(redeem: &[u8]) -> Vec<u8> {
    [&[0xa9, 0x14][..], &hash160(redeem), &[0x87]].concat()
}

/// Returns `RIPEMD160(SHA256(bytes))`, as computed by `OP_HASH160`.
pub fn hash160(bytes: &[u8]) -> [u8; 20] {
    use ripemd::Digest as _;
    ripemd::Ripemd160::digest(sha2::Sha256::digest(bytes)).into()
}

/// Verifies every transparent input of `transaction` with the candidate and the baseline adapter.
///
/// Returns one verdict per input, or `None` when both adapters refuse to prepare the transaction.
///
/// # Panics
///
/// Panics if preparation, any input's result, the legacy sigop count, or the P2SH sigop count
/// differs.
pub fn check_transaction(
    transaction: Arc<Transaction>,
    previous_outputs: Arc<Vec<transparent::Output>>,
    nu: NetworkUpgrade,
) -> Option<Vec<Verdict>> {
    assert_eq!(
        zakura_script::Sigops::sigops(transaction.as_ref()),
        baseline::Sigops::sigops(transaction.as_ref()).expect("the C++ adapter counts sigops"),
        "legacy sigops differ"
    );
    let candidate =
        zakura_script::CachedFfiTransaction::new(transaction.clone(), previous_outputs.clone(), nu);
    let baseline = baseline::CachedFfiTransaction::new(transaction.clone(), previous_outputs, nu);
    let (candidate, baseline) = match (candidate, baseline) {
        (Ok(candidate), Ok(baseline)) => (candidate, baseline),
        (Err(_), Err(_)) => return None,
        (candidate, baseline) => {
            panic!("preparation differs: candidate={candidate:?} baseline={baseline:?}")
        }
    };
    if !transaction.is_coinbase() {
        assert_eq!(
            candidate.p2sh_sigops(),
            baseline.p2sh_sigops(),
            "P2SH sigops differ"
        );
    }
    let verdicts = (0..transaction.inputs().len())
        .map(|index| {
            let (candidate, baseline) = (candidate.is_valid(index), baseline.is_valid(index));
            assert_eq!(
                candidate.is_ok(),
                baseline.is_ok(),
                "input {index}: candidate={candidate:?} baseline={baseline:?}"
            );
            Verdict::from_ok(candidate.is_ok())
        })
        .collect();
    Some(verdicts)
}
