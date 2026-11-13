//! Standardness (policy) helpers for transparent scripts.
//!
//! These mirror zcashd's policy checks. They are not consensus rules. The
//! transaction verifier applies them to mempool transactions before script
//! verification, and the mempool applies them again when it stores a
//! transaction, so both use this one copy.

use zakura_chain::{transaction::Transaction, transparent};
use zcash_script::{opcode::PossiblyBad, script, solver, Opcode};

/// The maximum number of signature operations in the redeem script of a standard P2SH input.
///
/// This is zcashd's `MAX_P2SH_SIGOPS` standardness (policy) constant:
/// <https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.h#L20>
pub const MAX_P2SH_SIGOPS: u32 = 15;

/// The maximum size in bytes of the scriptSig of a standard transaction input.
///
/// This is zcashd's `MAX_STANDARD_SCRIPTSIG_SIZE` standardness (policy) constant:
/// <https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.cpp#L92-L99>
pub const MAX_STANDARD_SCRIPTSIG_SIZE: usize = 1650;

/// Classify a script using the `zcash_script` solver.
///
/// Returns `Some(kind)` for standard script types, `None` for non-standard.
///
/// Mirrors the classification done by zcashd's `Solver()`.
pub fn standard_script_kind(lock_script: &transparent::Script) -> Option<solver::ScriptKind> {
    let code = script::Code(lock_script.as_raw_bytes().to_vec());
    let component = code.to_component().ok()?.refine().ok()?;
    solver::standard(&component)
}

/// Returns the expected number of scriptSig arguments for a given script kind.
///
/// Mirrors zcashd's `ScriptSigArgsExpected()`:
/// <https://github.com/zcash/zcash/blob/v6.11.0/src/script/standard.cpp#L135>
///
/// Returns `None` for non-standard types (TX_NONSTANDARD, TX_NULL_DATA).
pub fn script_sig_args_expected(kind: &solver::ScriptKind) -> Option<usize> {
    match kind {
        solver::ScriptKind::PubKey { .. } => Some(1),
        solver::ScriptKind::PubKeyHash { .. } => Some(2),
        solver::ScriptKind::ScriptHash { .. } => Some(1),
        solver::ScriptKind::MultiSig { required, .. } => Some(*required as usize + 1),
        solver::ScriptKind::NullData { .. } => None,
    }
}

/// Extract the redeemed script bytes from a P2SH scriptSig.
///
/// The redeemed script is the last data push in the scriptSig.
/// Returns `None` if the scriptSig has no push operations.
///
/// # Precondition
///
/// The scriptSig should be push-only (the mempool's standardness checks enforce this before this
/// function is reached). Non-push opcodes are silently ignored.
pub fn extract_p2sh_redeemed_script(unlock_script: &transparent::Script) -> Option<Vec<u8>> {
    let code = script::Code(unlock_script.as_raw_bytes().to_vec());
    let mut last_push_data: Option<Vec<u8>> = None;
    for opcode in code.parse().flatten() {
        if let PossiblyBad::Good(Opcode::PushValue(pv)) = opcode {
            last_push_data = Some(pv.value());
        }
    }
    last_push_data
}

/// Count the number of push operations in a script.
///
/// For a push-only script (already enforced for mempool scriptSigs),
/// this equals the stack depth after evaluation.
pub fn count_script_push_ops(script_bytes: &[u8]) -> usize {
    let code = script::Code(script_bytes.to_vec());
    code.parse()
        .filter(|op| matches!(op, Ok(PossiblyBad::Good(Opcode::PushValue(_)))))
        .count()
}

/// Returns `true` if all of a transaction's transparent inputs are standard.
///
/// Mirrors zcashd's `AreInputsStandard()`:
/// <https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.cpp#L136>
///
/// For each input:
/// 1. The spent output's scriptPubKey must be a known standard type (via the `zcash_script`
///    solver). Non-standard scripts and OP_RETURN outputs are rejected.
/// 2. The scriptSig stack depth must match `ScriptSigArgsExpected()`.
/// 3. For P2SH inputs:
///    - If the redeemed script is standard, its expected args are added to the total.
///    - If the redeemed script is non-standard, it must have at most [`MAX_P2SH_SIGOPS`] sigops.
///
/// # Correctness
///
/// Callers must ensure `spent_outputs.len()` matches the number of transparent inputs.
/// If the lengths differ, `false` is returned.
pub fn are_inputs_standard(tx: &Transaction, spent_outputs: &[transparent::Output]) -> bool {
    if tx.inputs().len() != spent_outputs.len() {
        return false;
    }
    for (input, spent_output) in tx.inputs().iter().zip(spent_outputs.iter()) {
        let unlock_script = match input {
            transparent::Input::PrevOut { unlock_script, .. } => unlock_script,
            transparent::Input::Coinbase { .. } => continue,
        };

        // Step 1: Classify the spent output's scriptPubKey via the zcash_script solver.
        let script_kind = match standard_script_kind(&spent_output.lock_script) {
            Some(kind) => kind,
            None => return false,
        };

        // Step 2: Get expected number of scriptSig arguments.
        // Returns None for TX_NONSTANDARD and TX_NULL_DATA.
        let mut n_args_expected = match script_sig_args_expected(&script_kind) {
            Some(n) => n,
            None => return false,
        };

        // Step 3: Count actual push operations in scriptSig.
        // For push-only scripts (enforced before this function), this equals the stack depth.
        let stack_size = count_script_push_ops(unlock_script.as_raw_bytes());

        // Step 4: P2SH-specific checks.
        if matches!(script_kind, solver::ScriptKind::ScriptHash { .. }) {
            let Some(redeemed_bytes) = extract_p2sh_redeemed_script(unlock_script) else {
                return false;
            };

            let redeemed_code = script::Code(redeemed_bytes);

            // Classify the redeemed script using the zcash_script solver.
            let redeemed_kind = {
                let component = redeemed_code
                    .to_component()
                    .ok()
                    .and_then(|c| c.refine().ok());
                component.and_then(|c| solver::standard(&c))
            };

            match redeemed_kind {
                Some(ref inner_kind) => {
                    // Standard redeemed script: add its expected args.
                    match script_sig_args_expected(inner_kind) {
                        Some(inner) => n_args_expected += inner,
                        None => return false,
                    }
                }
                None => {
                    // Non-standard redeemed script: accept if sigops <= limit.
                    // Matches zcashd: "Any other Script with less than 15 sigops OK:
                    // ... extra data left on the stack after execution is OK, too"
                    let sigops = redeemed_code.sig_op_count(true);
                    if sigops > MAX_P2SH_SIGOPS {
                        return false;
                    }

                    // This input is acceptable; move on to the next input.
                    continue;
                }
            }
        }

        // Step 5: Reject if scriptSig has wrong number of stack items.
        if stack_size != n_args_expected {
            return false;
        }
    }
    true
}
