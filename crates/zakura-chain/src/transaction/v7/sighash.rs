//! The sighash variant 0 signature digest of [ZIP 248].
//!
//! Every v7 signature commits to the transaction identifier, to the amounts and script
//! public keys of all spent transparent outputs, to the bundle that contains it, and to
//! its position in that bundle. The shared prefix is hashed once per transaction, and
//! each signature continues a copy of that hash state.
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use blake2b_simd::State;

use crate::{
    serialization::ZcashSerialize,
    transaction::{
        zip244::{self, finalize_node_hash, hasher},
        HashType, SigHash, Transaction,
    },
    transparent,
};

use super::{
    codec::{prevout_input, SIGHASH_VARIANT_0},
    digest::write_bundle_id,
    registry::{self, BundleId},
    V7Ref,
};

/// `signature_digest` personalization.
const SIGHASH_V7_PERSONALIZATION: &[u8; 16] = b"ZcashSigHash_v7_";

/// A v7 bundle whose signatures are Sapling or Orchard-protocol signatures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShieldedBundle {
    /// The Sapling bundle.
    Sapling,
    /// The Orchard bundle.
    Orchard,
    /// The Ironwood bundle.
    Ironwood,
}

impl ShieldedBundle {
    fn id(self) -> BundleId {
        match self {
            Self::Sapling => registry::SAPLING,
            Self::Orchard => registry::ORCHARD,
            Self::Ironwood => registry::IRONWOOD,
        }
    }
}

/// The digests that the signatures of one Sapling, Orchard, or Ironwood bundle of a v7
/// transaction sign, for that bundle's batch validator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShieldedSighashes {
    spend_auth: Vec<[u8; 32]>,
    binding: [u8; 32],
}

impl ShieldedSighashes {
    /// Returns the digest that the spend authorization signature of the spend or action
    /// at `index` signs, or `None` if the bundle has no such spend or action.
    pub fn spend_auth(&self, index: usize) -> Option<[u8; 32]> {
        self.spend_auth.get(index).copied()
    }

    /// Returns the digest that the bundle's binding signature signs.
    pub fn binding(&self) -> [u8; 32] {
        self.binding
    }
}

impl ::orchard::bundle::SignatureDigests for ShieldedSighashes {
    fn spend_auth(&self, index: usize) -> Option<[u8; 32]> {
        ShieldedSighashes::spend_auth(self, index)
    }

    fn binding(&self) -> [u8; 32] {
        ShieldedSighashes::binding(self)
    }
}

impl sapling_crypto::bundle::SignatureDigests for ShieldedSighashes {
    fn spend_auth(&self, index: usize) -> Option<[u8; 32]> {
        ShieldedSighashes::spend_auth(self, index)
    }

    fn binding(&self) -> [u8; 32] {
        ShieldedSighashes::binding(self)
    }
}

/// The data of a v7 transaction that its signature digests reuse.
#[derive(Clone, Debug)]
pub(crate) struct V7SighashCache {
    /// S.1 through S.4, or `None` if the spent outputs do not match the inputs, so that
    /// no signature can be checked.
    prefix: Option<State>,
    /// Each transparent input's signature context (S.7), or `None` if its position does
    /// not fit in two bytes.
    transparent_contexts: Vec<Option<Vec<u8>>>,
    sapling_spends: Option<usize>,
    orchard_actions: Option<usize>,
    ironwood_actions: Option<usize>,
}

impl V7SighashCache {
    /// Precomputes the digests shared by every signature of a v7 transaction, or returns
    /// `None` for other versions.
    ///
    /// `previous_outputs` must contain the output each transparent input spends, in input
    /// order.
    pub(crate) fn new(tx: &Transaction, previous_outputs: &[transparent::Output]) -> Option<Self> {
        let v7 = V7Ref::of(tx)?;
        let spent_outputs_match = previous_outputs.len() == v7.inputs.len();
        let prefix = spent_outputs_match.then(|| {
            let mut h = hasher(SIGHASH_V7_PERSONALIZATION);
            h.update(&[SIGHASH_VARIANT_0]);
            h.update(&v7.txid_digest());
            h.update(zip244::hash_amounts(previous_outputs).as_bytes());
            h.update(zip244::hash_scriptpubkeys(previous_outputs).as_bytes());
            h
        });
        let transparent_contexts = v7
            .inputs
            .iter()
            .zip(previous_outputs)
            .enumerate()
            .map(|(index, (input, previous_output))| {
                transparent_context(input, previous_output, index)
            })
            .collect();
        Some(Self {
            prefix,
            transparent_contexts,
            sapling_spends: v7.sapling.map(|b| b.spends().count()),
            orchard_actions: v7.orchard.map(|b| b.actions.len()),
            ironwood_actions: v7.ironwood.map(|b| b.actions.len()),
        })
    }

    fn digest(&self, bundle: BundleId, context: &[u8]) -> Option<[u8; 32]> {
        let mut h = self.prefix.clone()?;
        write_bundle_id(&mut h, bundle);
        h.update(context);
        Some(finalize_node_hash(h))
    }

    /// Returns the digest of the signature on the transparent input at `input_index`, or
    /// `None` if variant 0 cannot sign it: variant 0 signatures use `SIGHASH_ALL` only.
    pub(crate) fn transparent(&self, hash_type: HashType, input_index: usize) -> Option<SigHash> {
        if hash_type != HashType::ALL {
            return None;
        }
        let context = self.transparent_contexts.get(input_index)?.as_ref()?;
        self.digest(registry::TRANSPARENT, context).map(SigHash)
    }

    /// Returns the digests of every signature of `bundle`, or `None` if the transaction
    /// has no such bundle or its spent outputs do not match its inputs.
    pub(crate) fn shielded(&self, bundle: ShieldedBundle) -> Option<ShieldedSighashes> {
        let count = match bundle {
            ShieldedBundle::Sapling => self.sapling_spends,
            ShieldedBundle::Orchard => self.orchard_actions,
            ShieldedBundle::Ironwood => self.ironwood_actions,
        }?;
        let spend_auth = (0..count)
            .map(|index| self.digest(bundle.id(), &u16::try_from(index).ok()?.to_le_bytes()))
            .collect::<Option<_>>()?;
        Some(ShieldedSighashes {
            spend_auth,
            binding: self.digest(bundle.id(), &[])?,
        })
    }
}

/// S.7 for a transparent input: its previous output, value, script public key, sequence
/// number, and two-byte position.
fn transparent_context(
    input: &transparent::Input,
    previous_output: &transparent::Output,
    index: usize,
) -> Option<Vec<u8>> {
    let (outpoint, sequence) = prevout_input(input).ok()?;
    let index = u16::try_from(index).ok()?;
    let mut context = Vec::new();
    outpoint
        .zcash_serialize(&mut context)
        .expect("writing to a Vec cannot fail");
    context.extend_from_slice(&previous_output.value.zatoshis().to_le_bytes());
    previous_output
        .lock_script
        .zcash_serialize(&mut context)
        .expect("writing to a Vec cannot fail");
    context.extend_from_slice(&sequence.to_le_bytes());
    context.extend_from_slice(&index.to_le_bytes());
    Some(context)
}
