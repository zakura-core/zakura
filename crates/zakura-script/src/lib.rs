//! Zakura transparent script verification with the Rust `zcash_script` interpreter.
#![doc(html_favicon_url = "https://zakura.com/assets/rustdoc/zakura-favicon-128.png")]
#![doc(html_logo_url = "https://zakura.com/assets/rustdoc/zakura-icon.png")]
#![doc(html_root_url = "https://docs.rs/zakura_script")]
#![forbid(unsafe_code)]

#[cfg(test)]
mod encoding_tests;
#[cfg(test)]
mod tests;

use core::fmt;
use std::{ops::Deref, sync::Arc};

use thiserror::Error;

use zakura_chain::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    transaction::{HashType, SigHasher},
    transparent,
};
use zcash_script::{
    interpreter::{CallbackTransactionSignatureChecker, Flags},
    opcode::{Operation, PossiblyBad},
    script::{self, Evaluable as _},
    Opcode,
};

/// Whether the chain applies the post-quantum transparent output rules at this height.
///
/// This reads the network's configuration and the block height being validated, never the
/// transaction's own bytes, and it answers for every transaction in the block alike: once
/// the rules apply, every input is verified under them. A binary built without the `pq`
/// feature always says no, so those outputs keep whatever the legacy rules make of them.
pub fn pq_outputs_active(network: &Network, height: Height) -> bool {
    cfg!(feature = "pq")
        && network
            .pq_activation_height()
            .is_some_and(|activation| height >= activation)
}

/// Errors from transaction preparation and script verification.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// script verification failed
    ScriptInvalid,
    /// input index out of bounds
    TxIndex,
    /// tx is a coinbase transaction and should not be verified
    TxCoinbase,
    /// The interpreter rejected a script component.
    Interpreter {
        /// The component that failed.
        component: script::ComponentType,
        /// The interpreter error.
        #[source]
        error: script::Error,
    },
    /// transaction is invalid according to zakura_chain (not a zcash_script error)
    TxInvalid(#[from] zakura_chain::Error),
    /// The post-quantum rules rejected this input, or the output it spends is one they
    /// define no way to spend.
    #[cfg(feature = "pq")]
    Pq(#[from] zcash_transparent::pq::output::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&match self {
            Error::ScriptInvalid => "script verification failed".to_owned(),
            Error::TxIndex => "input index out of bounds".to_owned(),
            Error::TxCoinbase => {
                "tx is a coinbase transaction and should not be verified".to_owned()
            }
            Error::Interpreter { component, error } => {
                format!("{component:?} script failed: {error}")
            }
            Error::TxInvalid(e) => format!("tx is invalid: {e}"),
            #[cfg(feature = "pq")]
            Error::Pq(e) => format!("post-quantum input failed: {e}"),
        })
    }
}

impl Error {
    /// Returns true when the script failed verification.
    ///
    /// Returns false for transaction preparation failures that prevent verification.
    pub fn is_script_failure(&self) -> bool {
        match self {
            Error::ScriptInvalid | Error::Interpreter { .. } => true,
            #[cfg(feature = "pq")]
            Error::Pq(_) => true,
            _ => false,
        }
    }
}

fn parse_zip244_hash_type(raw_hash_type: i32) -> Option<HashType> {
    match raw_hash_type {
        0x01 => Some(HashType::ALL),
        0x02 => Some(HashType::NONE),
        0x03 => Some(HashType::SINGLE),
        0x81 => Some(HashType::ALL_ANYONECANPAY),
        0x82 => Some(HashType::NONE_ANYONECANPAY),
        0x83 => Some(HashType::SINGLE_ANYONECANPAY),
        _ => None,
    }
}

/// A preprocessed Transaction which can be used to verify scripts within said
/// Transaction. The cache retains its input handle so runtime ownership can
/// survive a caller's cancellation while a script worker still uses the cache.
#[derive(Debug)]
pub struct CachedFfiTransaction<T = Arc<zakura_chain::transaction::Transaction>> {
    /// The deserialized Zebra transaction.
    ///
    /// This field is private so that `transaction`, and `all_previous_outputs` always match.
    transaction: T,

    /// The outputs from previous transactions that match each input in the transaction
    /// being verified.
    all_previous_outputs: Arc<Vec<transparent::Output>>,

    /// The sighasher context to use to compute sighashes.
    sighasher: SigHasher,

    /// The network whose post-quantum signing context a spend must have been signed under,
    /// or `None` when those rules do not apply to this transaction.
    ///
    /// This is decided from the network's configuration and the validation height, never
    /// from the transaction's own bytes, and the network matters: a post-quantum signature
    /// is made for one network and will not verify on another.
    #[cfg(feature = "pq")]
    pq_outputs: Option<zcash_protocol::consensus::NetworkType>,
}

impl<T: Deref<Target = zakura_chain::transaction::Transaction>> CachedFfiTransaction<T> {
    /// Construct a `CachedFfiTransaction` from a `Transaction` and the outputs
    /// from previous transactions that match each input in the transaction
    /// being verified.
    pub fn new(
        transaction: T,
        all_previous_outputs: Arc<Vec<transparent::Output>>,
        nu: NetworkUpgrade,
    ) -> Result<Self, Error> {
        let sighasher = transaction.sighasher(nu, all_previous_outputs.clone())?;
        Ok(Self {
            transaction,
            all_previous_outputs,
            sighasher,
            #[cfg(feature = "pq")]
            pq_outputs: None,
        })
    }

    /// Prepares input verification under the rules in force at this network and height.
    ///
    /// The caller must resolve every input's previous output from trusted state, in input
    /// order: those outputs decide which rules each input is verified under, and they feed
    /// the transaction digest as well.
    ///
    /// Use this wherever the validation height is known. [`Self::new`] keeps the legacy
    /// rules whatever the chain is configured to do, which is right only for callers that
    /// have no height to judge by.
    pub fn new_for_network(
        transaction: T,
        all_previous_outputs: Arc<Vec<transparent::Output>>,
        network: &Network,
        height: Height,
    ) -> Result<Self, Error> {
        // The previous outputs decide which rules each input is verified under, so a list
        // that does not line up with the inputs is refused here rather than per input.
        if !transaction.is_coinbase() && all_previous_outputs.len() != transaction.inputs().len() {
            return Err(Error::TxIndex);
        }
        let cached = Self::new(
            transaction,
            all_previous_outputs,
            NetworkUpgrade::current(network, height),
        )?;
        #[cfg(feature = "pq")]
        let cached = Self {
            pq_outputs: pq_outputs_active(network, height)
                .then(|| zcash_protocol::consensus::Parameters::network_type(network)),
            ..cached
        };
        Ok(cached)
    }

    /// The script rule that refuses to run a post-quantum output under the legacy
    /// interpreter, set for every input of every transaction once those rules apply.
    ///
    /// Setting it for some inputs and not others would make the same output spendable or
    /// not depending on which verifier looked at it, so it is derived here from the
    /// transaction's rules rather than from anything per-input.
    fn pq_flags(&self) -> Flags {
        #[cfg(feature = "pq")]
        if self.pq_outputs.is_some() {
            return Flags::PqOutputs;
        }
        Flags::empty()
    }

    /// Returns the transparent inputs for this transaction.
    pub fn inputs(&self) -> &[transparent::Input] {
        self.transaction.inputs()
    }

    /// Returns the outputs from previous transactions that match each input in the transaction
    /// being verified.
    pub fn all_previous_outputs(&self) -> &Vec<transparent::Output> {
        &self.all_previous_outputs
    }

    /// Return the sighasher being used for this transaction.
    pub fn sighasher(&self) -> &SigHasher {
        &self.sighasher
    }

    /// Returns the total number of P2SH sigops across all inputs of this transaction.
    ///
    /// Mirrors zcashd's [`GetP2SHSigOpCount()`].
    ///
    /// For each P2SH input (where the spent `scriptPubKey` is P2SH), the redeem script (the last
    /// data push in the `scriptSig`) is parsed in "accurate" mode and its sigops are counted.
    /// Coinbase inputs contribute zero.
    ///
    /// This must be included in the block-wide `MAX_BLOCK_SIGOPS` total to match zcashd's consensus
    /// behavior.
    ///
    /// [`GetP2SHSigOpCount()`]: https://github.com/zcash/zcash/blob/v6.11.0/src/main.cpp#L840-L852
    pub fn p2sh_sigops(&self) -> u32 {
        #[cfg(feature = "pq")]
        if self.pq_outputs.is_some() && !self.transaction.is_coinbase() {
            return self
                .transaction
                .inputs()
                .iter()
                .zip(self.all_previous_outputs.iter())
                .map(|(input, output)| pq_input_sigop_count(input, output))
                .sum();
        }
        p2sh_sigop_count(&self.transaction, &self.all_previous_outputs)
    }

    /// Verify if the script in the input at `input_index` of a transaction correctly spends the
    /// matching [`transparent::Output`] it refers to.
    pub fn is_valid(&self, input_index: usize) -> Result<(), Error> {
        let previous_output = self
            .all_previous_outputs
            .get(input_index)
            .filter(|_| self.all_previous_outputs.len() == self.transaction.inputs().len())
            .ok_or(Error::TxIndex)?
            .clone();

        let transparent::Output {
            value: _,
            lock_script,
        } = previous_output;
        let script_pub_key: &[u8] = lock_script.as_raw_bytes();

        let flags = Flags::P2SH | Flags::CHECKLOCKTIMEVERIFY | self.pq_flags();

        let lock_time = self.transaction.raw_lock_time();
        let is_final = self.transaction.inputs()[input_index].sequence() == u32::MAX;
        let signature_script = match &self.transaction.inputs()[input_index] {
            transparent::Input::PrevOut {
                outpoint: _,
                unlock_script,
                sequence: _,
            } => unlock_script.as_raw_bytes(),
            transparent::Input::Coinbase { .. } => Err(Error::TxCoinbase)?,
        };

        let script =
            script::Raw::from_raw_parts(signature_script.to_vec(), script_pub_key.to_vec());

        // Returning `None` fails only this signature check, like zcashd's `CheckSig`: the
        // interpreter pushes false, which later opcodes such as `OP_NOT` may consume.
        let calculate_sighash =
            |script_code: &script::Code, hash_type: &zcash_script::signature::HashType| {
                let script_code_vec = script_code.0.clone();

                // For pre-v5 (v4) transactions: zcashd serializes the raw
                // hash_type byte into the sighash preimage (only masking with
                // 0x1f for selection logic). Use the raw byte to match.
                if self.transaction.version() < 5 {
                    let raw_byte = hash_type
                        .raw_bits()
                        .try_into()
                        .expect("script signature hash types are one byte");
                    return Some(
                        self.sighasher()
                            .sighash_v4_raw(raw_byte, Some((input_index, script_code_vec)))
                            .0,
                    );
                }

                let our_hash_type = parse_zip244_hash_type(hash_type.raw_bits())?;

                // ZIP-244 §S.2a requires a corresponding output for
                // SIGHASH_SINGLE.
                if (our_hash_type == HashType::SINGLE
                    || our_hash_type == HashType::SINGLE_ANYONECANPAY)
                    && input_index >= self.transaction.outputs().len()
                {
                    return None;
                }

                Some(
                    self.sighasher()
                        .sighash(our_hash_type, Some((input_index, script_code_vec)))
                        .0,
                )
            };
        let checker = CallbackTransactionSignatureChecker {
            sighash: &calculate_sighash,
            lock_time: i64::from(lock_time),
            is_final,
        };
        // Which rules an input is verified under is decided by the output it spends, so a
        // transaction can spend a legacy output and a post-quantum one side by side and
        // each is judged by its own rules.
        //
        // Only a v5 or later transaction can spend a post-quantum output: a post-quantum
        // signature is made over the v5 digest, which commits to the output being spent and
        // so to the commitment in it. The v4 digest does not, and an earlier transaction
        // spending such an output is instead refused by the namespace rule in `flags`.
        #[cfg(feature = "pq")]
        if let Some(network) = self.pq_outputs.filter(|_| self.transaction.version() >= 5) {
            let lock_code = script::Code(script_pub_key.to_vec());
            match zcash_transparent::pq::output::parse_output(&lock_code) {
                Ok(Some(output)) => {
                    return match output.verify_spend(network, signature_script, &checker) {
                        Ok(true) => Ok(()),
                        Ok(false) => Err(Error::ScriptInvalid),
                        Err(error) => Err(Error::Pq(error)),
                    };
                }
                // Not a post-quantum output, so the legacy rules below apply to it as before.
                Ok(None) => {}
                // An output inside the namespace that nothing defines a way to spend. The
                // namespace rule in `flags` would refuse it too; failing here says why.
                Err(error) => return Err(Error::Pq(error)),
            }
        }

        match script.eval(flags, &checker) {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::ScriptInvalid),
            Err((component, error)) => Err(Error::Interpreter { component, error }),
        }
    }
}

/// Counts the signature operations in the script a post-quantum output commits to, rather
/// than in the input that reveals it.
///
/// The input's signature pushes are far larger than the legacy counter's push bound, where
/// that counter stops and would report almost none of the checks the script performs. This
/// is accounting only: the input is still authenticated and executed independently.
#[cfg(feature = "pq")]
fn pq_input_sigop_count(input: &transparent::Input, output: &transparent::Output) -> u32 {
    let legacy_count = p2sh_input_sigop_count(input, output);
    let transparent::Input::PrevOut { unlock_script, .. } = input else {
        return legacy_count;
    };
    let lock_code = script::Code(output.lock_script.as_raw_bytes().to_vec());
    match zcash_transparent::pq::output::parse_output(&lock_code) {
        Ok(Some(pq_output)) => pq_output
            .authenticate_spend(unlock_script.as_raw_bytes())
            .map_or(legacy_count, |spend| spend.sig_op_count(true)),
        // Either a legacy output, or one no spend can satisfy; verification rejects the
        // latter, and counting it as the legacy rules do charges nothing extra for it.
        _ => legacy_count,
    }
}

/// Trait for counting the number of transparent signature operations in the transparent inputs and
/// outputs of a transaction.
///
/// Mirrors zcashd's [`GetLegacySigOpCount()`].
///
/// All transparent inputs are included, including the coinbase input script. zcashd charges
/// coinbase `scriptSig` sigops against the block `MAX_BLOCK_SIGOPS` limit, so Zebra must do the
/// same to avoid a consensus split.
///
/// [`GetLegacySigOpCount()`]: https://github.com/zcash/zcash/blob/v6.11.0/src/main.cpp#L826-L836
pub trait Sigops {
    /// Returns the number of transparent signature operations in the
    /// transparent inputs and outputs of the given transaction.
    fn sigops(&self) -> u32 {
        self.scripts()
            .map(|script| legacy_sigop_count(&script))
            .sum()
    }

    /// Returns an iterator over the input and output scripts in the transaction.
    ///
    /// For consensus sigop accounting, this must include the coinbase input
    /// script (height prefix followed by extra data), matching zcashd's
    /// `GetLegacySigOpCount()`.
    fn scripts(&self) -> impl Iterator<Item = Vec<u8>>;
}

impl Sigops for zakura_chain::transaction::Transaction {
    fn scripts(&self) -> impl Iterator<Item = Vec<u8>> {
        self.inputs()
            .iter()
            .map(|input| match input {
                transparent::Input::PrevOut { unlock_script, .. } => {
                    unlock_script.as_raw_bytes().to_vec()
                }
                // Coinbase scriptSig = encoded height || extra data, which must be reconstructed
                // for sigop counting. `coinbase_script()` round-trips through
                // `write_coinbase_height`, which only fails when called on a malformed in-memory
                // genesis coinbase. Any coinbase that was successfully deserialized round-trips
                // cleanly, so this `expect` cannot fire on validation paths.
                transparent::Input::Coinbase { .. } => input
                    .coinbase_script()
                    .expect("coinbase_script reconstructs from a deserialized coinbase input"),
            })
            .chain(
                self.outputs()
                    .iter()
                    .map(|o| o.lock_script.as_raw_bytes().to_vec()),
            )
    }
}

impl Sigops for zakura_chain::transaction::UnminedTx {
    fn scripts(&self) -> impl Iterator<Item = Vec<u8>> {
        self.transaction().scripts()
    }
}

impl<T: Deref<Target = zakura_chain::transaction::Transaction>> Sigops for CachedFfiTransaction<T> {
    fn scripts(&self) -> impl Iterator<Item = Vec<u8>> {
        self.transaction.scripts()
    }
}

impl Sigops for zcash_primitives::transaction::Transaction {
    fn scripts(&self) -> impl Iterator<Item = Vec<u8>> {
        self.transparent_bundle().into_iter().flat_map(|bundle| {
            // `zcash_primitives` stores the coinbase input's full serialized scriptSig (height
            // prefix + extra data) in the synthesized input's script_sig, so it is included as-is
            // for sigop counting.
            bundle
                .vin
                .iter()
                .map(|i| i.script_sig().0 .0.clone())
                .chain(bundle.vout.iter().map(|o| o.script_pubkey().0 .0.clone()))
        })
    }
}

/// Counts the signature operations in `script` like zcashd's `CScript::GetSigOpCount(false)`.
///
/// zcashd reads opcodes with `GetOp` and applies no execution limits. This function therefore
/// counts sigops after pushes larger than the 520-byte execution limit, which output scripts can
/// contain because creating an output never executes its script. Every `CHECKMULTISIG` counts as
/// the maximum key count. A truncated push ends the count, like a `GetOp` failure.
pub fn legacy_sigop_count(mut script: &[u8]) -> u32 {
    // `Operation` is `repr(u8)`, so each cast yields the opcode byte.
    const CHECKSIG: u8 = Operation::OP_CHECKSIG as u8;
    const CHECKSIGVERIFY: u8 = Operation::OP_CHECKSIGVERIFY as u8;
    const CHECKMULTISIG: u8 = Operation::OP_CHECKMULTISIG as u8;
    const CHECKMULTISIGVERIFY: u8 = Operation::OP_CHECKMULTISIGVERIFY as u8;
    /// zcashd's `MAX_PUBKEYS_PER_MULTISIG`.
    const MAX_PUBKEYS_PER_MULTISIG: u32 = 20;

    let mut count = 0;
    while let Some((&opcode, rest)) = script.split_first() {
        let Some(rest) = skip_push_data(opcode, rest) else {
            break;
        };
        script = rest;
        count += match opcode {
            CHECKSIG | CHECKSIGVERIFY => 1,
            CHECKMULTISIG | CHECKMULTISIGVERIFY => MAX_PUBKEYS_PER_MULTISIG,
            _ => 0,
        };
    }
    count
}

/// Returns `script` after the push data of `opcode`, or `None` if the push data is truncated.
///
/// `script` starts after `opcode`. Opcodes that push no data return `script` unchanged.
fn skip_push_data(opcode: u8, script: &[u8]) -> Option<&[u8]> {
    /// Splits a little-endian length of `N` bytes off the front of `script`.
    fn length<const N: usize>(script: &[u8]) -> Option<(usize, &[u8])> {
        let (length, rest) = script.split_first_chunk::<N>()?;
        let mut bytes = [0; 4];
        bytes[..N].copy_from_slice(length);
        Some((usize::try_from(u32::from_le_bytes(bytes)).ok()?, rest))
    }

    let (length, script) = match opcode {
        // Direct pushes of 0 to 75 bytes, then OP_PUSHDATA1, OP_PUSHDATA2, and OP_PUSHDATA4.
        0x00..=0x4b => (usize::from(opcode), script),
        0x4c => length::<1>(script)?,
        0x4d => length::<2>(script)?,
        0x4e => length::<4>(script)?,
        _ => (0, script),
    };
    script.get(length..)
}

/// Extract the redeem script bytes from a P2SH scriptSig.
///
/// Mirrors zcashd's P2SH redeem-script extraction in
/// [`CScript::GetSigOpCount(const CScript& scriptSig)`].
///
/// Iterates the scriptSig opcodes and returns the last successfully pushed data value. Returns
/// `None` if any opcode fails to parse, OR if any opcode is not a push value (zcashd: `opcode >
/// OP_16`). This matches zcashd's behavior of returning 0 P2SH sigops for malformed or
/// non-push-only scriptSigs.
///
/// [`CScript::GetSigOpCount(const CScript& scriptSig)`]: https://github.com/zcash/zcash/blob/v6.11.0/src/script/script.cpp#L176-L199
fn extract_p2sh_redeem_script(unlock_script: &transparent::Script) -> Option<Vec<u8>> {
    let code = script::Code(unlock_script.as_raw_bytes().to_vec());
    let mut last_push_data: Option<Vec<u8>> = None;
    for opcode in code.parse() {
        match opcode {
            Ok(PossiblyBad::Good(Opcode::PushValue(pv))) => {
                last_push_data = Some(pv.value());
            }
            // Non-push opcode (operation, control, or bad) or parse error: zcashd returns 0 sigops
            // in this case. Match that behavior by discarding any data collected so far.
            _ => return None,
        }
    }
    last_push_data
}

/// Returns the P2SH sigop count for a single input.
///
/// `spent_output` must be the output spent by `input`.
///
/// Returns 0 for non-P2SH inputs, coinbase inputs, and P2SH inputs where no redeem script can be
/// extracted from the scriptSig (mirroring zcashd's `CScript::GetSigOpCount(scriptSig)`, which
/// returns 0 when the scriptSig is not push-only).
///
/// This is the per-input counting used by [`p2sh_sigop_count`] for the block-wide consensus sigop
/// total, and by the mempool standardness gate that rejects high-sigop P2SH inputs before script
/// verification.
pub fn p2sh_input_sigop_count(
    input: &transparent::Input,
    spent_output: &transparent::Output,
) -> u32 {
    let unlock_script = match input {
        transparent::Input::PrevOut { unlock_script, .. } => unlock_script,
        transparent::Input::Coinbase { .. } => return 0,
    };

    let lock_code = script::Code(spent_output.lock_script.as_raw_bytes().to_vec());

    if !lock_code.is_pay_to_script_hash() {
        return 0;
    }

    let Some(redeemed_bytes) = extract_p2sh_redeem_script(unlock_script) else {
        return 0;
    };

    // Count the redeem script's sigops in zcashd's "accurate" mode, matching
    // `GetP2SHSigOpCount` -> `CScript::GetSigOpCount(scriptSig)` -> `subscript.GetSigOpCount(true)`.
    // The redeem script is at most 520 bytes, so `zcash_script`'s execution push limit cannot stop
    // this count early. Disabled opcodes, including OP_CODESEPARATOR, do not stop it either.
    script::Code(redeemed_bytes).sig_op_count(true)
}

/// Returns the total number of P2SH sigops across all inputs of `tx`.
///
/// Mirrors zcashd's [`GetP2SHSigOpCount()`].
///
/// Coinbase transactions always return zero, matching zcashd's early-return for `tx.IsCoinBase()`.
/// Callers are therefore permitted to pass an empty `spent_outputs` slice for coinbase transactions
/// (which is what the block-verifier does, since coinbase inputs have no previous output).
///
/// # Correctness
///
/// For non-coinbase transactions, `spent_outputs.len()` must equal the number of transparent inputs
/// in `tx`. If the lengths differ, `zip()` silently truncates the longer iterator, causing an
/// incorrect (undercount) result.
///
/// # Panics
///
/// Panics if a non-coinbase transaction is passed a misaligned `spent_outputs` slice.
///
/// [`GetP2SHSigOpCount()`]: https://github.com/zcash/zcash/blob/v6.11.0/src/main.cpp#L840-L852
pub fn p2sh_sigop_count(
    tx: &zakura_chain::transaction::Transaction,
    spent_outputs: &[transparent::Output],
) -> u32 {
    if tx.is_coinbase() {
        return 0;
    }

    assert_eq!(
        tx.inputs().len(),
        spent_outputs.len(),
        "spent_outputs must align with transaction inputs for non-coinbase txs"
    );

    tx.inputs()
        .iter()
        .zip(spent_outputs.iter())
        .map(|(input, spent_output)| p2sh_input_sigop_count(input, spent_output))
        .sum()
}
