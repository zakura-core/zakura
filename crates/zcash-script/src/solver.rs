//! Solvers for detecting known script kinds.

use alloc::vec::Vec;

use bounded_vec::BoundedVec;

use crate::{
    external::pubkey::PubKey,
    op,
    opcode::{push_value::LargeValue::PushdataBytelength, Evaluable, PushValue},
    script, Opcode,
};

/// Parses a [`script::PubKey`] or [`script::Redeem`] and detects standard scripts.
///
/// If successful, returns the script kind, and any relevant properties parsed from the
/// script. For example, for a P2SH script, the result will contain the script hash; for
/// P2PKH it will contain the key hash, etc.
///
/// Returns `None` if the script is non-standard.
pub fn standard(script_code: &script::Component<Opcode>) -> Option<ScriptKind> {
    match &script_code.0[..] {
        // Pay-to-Script-Hash (P2SH)
        [op::HASH160, Opcode::PushValue(PushValue::LargeValue(PushdataBytelength(v))), op::EQUAL] => {
            v.as_slice()
                .try_into()
                .ok()
                .map(|hash| ScriptKind::ScriptHash { hash })
        }

        // Pay-to-Public-Key-Hash (P2PKH)
        [op::DUP, op::HASH160, Opcode::PushValue(PushValue::LargeValue(PushdataBytelength(v))), op::EQUALVERIFY, op::CHECKSIG] => {
            v.as_slice()
                .try_into()
                .ok()
                .map(|hash| ScriptKind::PubKeyHash { hash })
        }

        // Provably prunable, data-carrying output.
        //
        // So long as the script passes the `is_unspendable()` test, and all but the first
        // byte passes the `is_push_only()` test, we don't care what exactly is in the
        // script.
        [op::RETURN, rest @ ..] => rest
            .iter()
            .map(|op| op.extract_push_value().ok().cloned())
            // This `.collect()` is equivalent to `script::Opcode::is_push_only` for the
            // subset of inputs consisting of entirely valid `PushValue`s, which is all we
            // care about here.
            .collect::<Option<_>>()
            .map(|data| ScriptKind::NullData { data }),

        // Legacy Pay-to-Public-Key (P2PK), which only occurred in early Zcash coinbase outputs.
        [Opcode::PushValue(PushValue::LargeValue(PushdataBytelength(v))), op::CHECKSIG] => {
            (v.len() == PubKey::SIZE || v.len() == PubKey::COMPRESSED_SIZE).then(|| {
                ScriptKind::PubKey {
                    data: v.clone().to_vec().try_into().expect("bounds checked"),
                }
            })
        }

        // Pay-to-Multi-Signature (P2MS)
        [Opcode::PushValue(PushValue::SmallValue(required)), pubkeys @ .., Opcode::PushValue(PushValue::SmallValue(keys)), op::CHECKMULTISIG] =>
        {
            match (u8::try_from(required.to_num()), u8::try_from(keys.to_num())) {
                (Ok(required @ 1..=16), Ok(keys @ 1..=16)) => {
                    // The remaining opcodes must be `PushData`s that are valid lengths for pubkeys.
                    let pubkeys = pubkeys
                        .iter()
                        .map(|op| match op {
                            Opcode::PushValue(data) => {
                                let pubkey_bytes = data.value();
                                // Equivalent to `CPubKey::GetLen`
                                let expected_len = match pubkey_bytes.first() {
                                    Some(2 | 3) => Some(PubKey::COMPRESSED_SIZE),
                                    Some(4 | 6 | 7) => Some(PubKey::SIZE),
                                    _ => None,
                                };
                                // Equivalent to `CPubKey::ValidSize`
                                (expected_len == Some(pubkey_bytes.len()))
                                    .then(|| pubkey_bytes.try_into().expect("bounds checked"))
                            }
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>();

                    pubkeys.and_then(|pubkeys| {
                        (pubkeys.len() == usize::from(keys) && required <= keys)
                            .then_some(ScriptKind::MultiSig { required, pubkeys })
                    })
                }

                // Non-standard
                _ => None,
            }
        }

        // Non-standard
        _ => None,
    }
}

/// Known kinds of standard scripts.
pub enum ScriptKind {
    /// A P2PKH script.
    PubKeyHash {
        /// The Hash160 of the public key.
        hash: [u8; 20],
    },

    /// A P2SH script, used in transaction outputs to efficiently commit to other scripts.
    ScriptHash {
        /// The Hash160 of the script.
        hash: [u8; 20],
    },

    /// A transparent threshold multisig script.
    MultiSig {
        /// The number of signatures required to spend.
        required: u8,
        /// The pubkeys that can be used to sign spends.
        pubkeys: Vec<BoundedVec<u8, { PubKey::COMPRESSED_SIZE }, { PubKey::SIZE }>>,
    },

    /// An unspendable `OP_RETURN` script that carries data.
    NullData {
        /// The carried data.
        data: Vec<PushValue>,
    },

    /// A legacy P2PK script.
    PubKey {
        /// The encoded public key.
        data: BoundedVec<u8, { PubKey::COMPRESSED_SIZE }, { PubKey::SIZE }>,
    },
}

impl ScriptKind {
    /// Returns a string identifier for this script kind.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScriptKind::PubKeyHash { .. } => "pubkeyhash",
            ScriptKind::ScriptHash { .. } => "scripthash",
            ScriptKind::MultiSig { .. } => "multisig",
            ScriptKind::NullData { .. } => "nulldata",
            ScriptKind::PubKey { .. } => "pubkey",
        }
    }

    /// Returns the number of signatures required to spend an output of this script kind.
    pub fn req_sigs(&self) -> u8 {
        match self {
            ScriptKind::PubKeyHash { .. } => 1,
            ScriptKind::ScriptHash { .. } => 1,
            ScriptKind::MultiSig { required, .. } => *required,
            // TODO: null-data value might be incorrect,
            //       fix after https://github.com/zcash/wallet/issues/236
            ScriptKind::NullData { .. } => 0,
            ScriptKind::PubKey { .. } => 1,
        }
    }
}
