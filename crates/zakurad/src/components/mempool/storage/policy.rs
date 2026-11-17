//! Mempool transaction standardness policy constants and helpers.
//!
//! These mirror zcashd's mempool policy for rejecting non-standard transactions
//! (`IsStandardTx()` and `AreInputsStandard()`). The transparent-input checks now live in
//! `zakura-consensus`, where the transaction verifier applies them before script verification;
//! they are re-exported here and used by the storage-time policy in the parent module.

#[cfg(any(test, feature = "mempool-bench"))]
use zakura_chain::transparent;

// The transparent-input standardness checks (`AreInputsStandard()` and the spent-output
// classifier) live in `zakura-consensus`, where the transaction verifier also applies them to
// mempool transactions *before* script verification (`check::mempool_standard_input_scripts`).
// They are re-exported here for the storage-time policy checks, so the two paths can't drift apart.
pub(super) use zakura_consensus::transaction::check::{
    are_inputs_standard, standard_script_kind, MAX_STANDARD_SCRIPTSIG_SIZE,
};

/// Maximum number of signature operations allowed per standard transaction (zcashd `MAX_STANDARD_TX_SIGOPS`).
/// <https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.h#L22>
pub(super) const MAX_STANDARD_TX_SIGOPS: u32 = 4000;

/// Maximum number of public keys allowed in a standard multisig script.
/// <https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.cpp#L46-L48>
pub(super) const MAX_STANDARD_MULTISIG_PUBKEYS: usize = 3;

// -- Test helper functions shared across test modules --

/// Build a P2PKH lock script: OP_DUP OP_HASH160 <20-byte hash> OP_EQUALVERIFY OP_CHECKSIG
#[cfg(any(test, feature = "mempool-bench"))]
pub(super) fn p2pkh_lock_script(hash: &[u8; 20]) -> transparent::Script {
    let mut s = vec![0x76, 0xa9, 0x14];
    s.extend_from_slice(hash);
    s.push(0x88);
    s.push(0xac);
    transparent::Script::new(&s)
}
