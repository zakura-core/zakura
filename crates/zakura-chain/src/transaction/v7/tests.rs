//! Tests for v7 transactions: known answers, differential tests against
//! `zcash_primitives`, decoding rejections, and coinbase accessors.

mod agreement;
mod coinbase;
mod differential;
mod known_answers;
mod preallocate;
mod registry;
mod rejection;
mod vectors;

use std::sync::Arc;

use crate::{
    amount::Amount,
    parameters::NetworkUpgrade,
    serialization::{SerializationError, ZcashDeserializeInto, ZcashSerialize},
    transaction::{HashType, SigHasher, Transaction},
    transparent,
};

use super::ShieldedBundle;

/// The consensus branch ID that v7 test transactions use.
fn branch_id() -> zcash_protocol::consensus::BranchId {
    zcash_protocol::consensus::BranchId::Nu7_1
}

fn decode(bytes: &[u8]) -> Result<Transaction, SerializationError> {
    bytes.zcash_deserialize_into::<Transaction>()
}

/// Returns `true` if `result` is the early rejection of a coinbase transaction's Sapling
/// spends.
fn is_coinbase_sapling_spends<T>(result: &Result<T, SerializationError>) -> bool {
    matches!(
        result,
        Err(SerializationError::Parse(message))
            if *message == "coinbase transaction must not have Sapling spends"
    )
}

/// Returns the transaction that a known-answer vector encodes.
fn vector_transaction(bytes: &[u8]) -> Transaction {
    decode(bytes).expect("vectors decode")
}

/// Returns `true` if `tx` breaks a ZIP 248 bundle-local rule: a transparent or coinbase
/// delta without its bundle, or a coinbase transaction that spends from a pool.
///
/// The decoder accepts such a transaction and consensus rejects it, while
/// `zcash_primitives` rejects it when decoding. A coinbase transaction's Sapling spends
/// are the exception: both decoders reject them.
fn breaks_bundle_local_rule(tx: &Transaction) -> bool {
    let fields = tx.v7_fields().expect("the transaction is v7");
    let has_transparent = !tx.inputs().is_empty() || !tx.outputs().is_empty();
    let spends = !tx.inputs().is_empty()
        || tx.sapling_nullifiers().next().is_some()
        || tx
            .orchard_flags()
            .is_some_and(|flags| flags.contains(crate::orchard::Flags::ENABLE_SPENDS))
        || tx
            .ironwood_flags()
            .is_some_and(|flags| flags.contains(crate::orchard::Flags::ENABLE_SPENDS));
    (!fields.value_deltas.transparent.is_zero() && !has_transparent)
        || (!fields.value_deltas.coinbase.is_zero() && fields.coinbase.is_none())
        || (fields.coinbase.is_some() && spends)
}

/// Returns `true` if `error` is `zcash_primitives` rejecting a transaction that breaks a
/// ZIP 248 bundle-local rule.
fn is_bundle_rule_error(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .and_then(|inner| {
            inner.downcast_ref::<zcash_primitives::transaction::v7::BundleRuleError>()
        })
        .is_some()
}

fn encode(tx: &Transaction) -> Vec<u8> {
    tx.zcash_serialize_to_vec()
        .expect("a decoded v7 transaction re-encodes")
}

/// Parses `bytes` with `zcash_primitives`.
fn common_read(bytes: &[u8]) -> std::io::Result<zcash_primitives::transaction::Transaction> {
    zcash_primitives::transaction::Transaction::read(bytes, branch_id())
}

fn sighasher(tx: &Transaction, previous_outputs: Vec<transparent::Output>) -> SigHasher {
    tx.sighasher(NetworkUpgrade::Nu7_1, Arc::new(previous_outputs))
        .expect("zcash_primitives accepts the v7 transaction")
}

fn previous_output(value: u64, lock_script: &[u8]) -> transparent::Output {
    transparent::Output {
        value: Amount::try_from(value).expect("test amounts are valid"),
        lock_script: transparent::Script::new(lock_script),
    }
}

/// Returns each signature digest of `tx` as `(label, digest)`, labelled as in
/// [`vectors::TestVector::sighashes`].
fn native_sighashes(
    tx: &Transaction,
    previous_outputs: Vec<transparent::Output>,
) -> Vec<(String, [u8; 32])> {
    let hasher = sighasher(tx, previous_outputs);
    let mut digests: Vec<_> = (0..tx.inputs().len())
        .map(|index| {
            let digest = hasher
                .v7_transparent_sighash(HashType::ALL, index)
                .expect("SIGHASH_ALL signs every input");
            (format!("t {index}"), digest.0)
        })
        .collect();
    for (bundle, bundle_type, count) in [
        (
            ShieldedBundle::Sapling,
            2,
            tx.sapling_spends_per_anchor().count(),
        ),
        (
            ShieldedBundle::Orchard,
            3,
            tx.orchard_shielded_data().map_or(0, |b| b.actions.len()),
        ),
        (
            ShieldedBundle::Ironwood,
            4,
            tx.ironwood_shielded_data().map_or(0, |b| b.actions.len()),
        ),
    ] {
        let Some(sighashes) = hasher.v7_shielded_sighashes(bundle) else {
            continue;
        };
        for index in 0..count {
            let digest = sighashes
                .spend_auth(index)
                .expect("each spend or action has a digest");
            digests.push((format!("s {bundle_type} {index}"), digest));
        }
        assert_eq!(sighashes.spend_auth(count), None);
        digests.push((format!("s {bundle_type} b"), sighashes.binding()));
    }
    digests
}

fn usize_to_u64(n: usize) -> u64 {
    u64::try_from(n).expect("test lengths fit in a u64")
}

/// Encodes `n` as a compactSize.
fn compact_size(n: u64) -> Vec<u8> {
    match n {
        0..=252 => vec![u8::try_from(n).expect("checked by the match")],
        253..=0xffff => [
            &[0xfd][..],
            &u16::try_from(n).expect("in range").to_le_bytes(),
        ]
        .concat(),
        0x1_0000..=0xffff_ffff => [
            &[0xfe][..],
            &u32::try_from(n).expect("in range").to_le_bytes(),
        ]
        .concat(),
        _ => [&[0xff][..], &n.to_le_bytes()].concat(),
    }
}

/// A v7 encoding split into its maps, so that tests can change single entries.
#[derive(Clone, Debug)]
struct Envelope {
    header: Vec<u8>,
    deltas: Vec<(u64, u64, i64)>,
    effects: Vec<(u64, u64, Vec<u8>)>,
    auths: Vec<(u64, u64, Vec<u8>)>,
}

impl Envelope {
    /// The common fields of an empty NU7.1 transaction.
    fn empty() -> Self {
        let mut header = Vec::new();
        let header_field = crate::transaction::serialize::header_field(
            true,
            zcash_protocol::constants::V7_TX_VERSION,
        );
        header.extend_from_slice(&header_field.to_le_bytes());
        header.extend_from_slice(&super::V7_VERSION_GROUP_ID.to_le_bytes());
        header.extend_from_slice(&u32::from(branch_id()).to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        Self {
            header,
            deltas: Vec::new(),
            effects: Vec::new(),
            auths: Vec::new(),
        }
    }

    /// Splits a well-formed v7 encoding.
    fn parse(bytes: &[u8]) -> Self {
        struct Cursor<'a>(&'a [u8]);
        impl Cursor<'_> {
            fn take(&mut self, n: usize) -> Vec<u8> {
                let (head, rest) = self.0.split_at(n);
                self.0 = rest;
                head.to_vec()
            }
            fn compact_size(&mut self) -> u64 {
                match self.take(1)[0] {
                    0xfd => u64::from(u16::from_le_bytes(self.take(2).try_into().unwrap())),
                    0xfe => u64::from(u32::from_le_bytes(self.take(4).try_into().unwrap())),
                    0xff => u64::from_le_bytes(self.take(8).try_into().unwrap()),
                    n => u64::from(n),
                }
            }
            fn bundles(&mut self) -> Vec<(u64, u64, Vec<u8>)> {
                (0..self.compact_size())
                    .map(|_| {
                        let (t, v, n) = (
                            self.compact_size(),
                            self.compact_size(),
                            self.compact_size(),
                        );
                        (t, v, self.take(usize::try_from(n).unwrap()))
                    })
                    .collect()
            }
        }
        let mut cursor = Cursor(bytes);
        let header = cursor.take(20);
        let deltas = (0..cursor.compact_size())
            .map(|_| {
                let (t, v) = (cursor.compact_size(), cursor.compact_size());
                (t, v, i64::from_le_bytes(cursor.take(8).try_into().unwrap()))
            })
            .collect();
        let effects = cursor.bundles();
        let auths = cursor.bundles();
        assert!(cursor.0.is_empty(), "trailing bytes after the auth bundles");
        Self {
            header,
            deltas,
            effects,
            auths,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = self.header.clone();
        bytes.extend(compact_size(usize_to_u64(self.deltas.len())));
        for (t, v, value) in &self.deltas {
            bytes.extend(compact_size(*t));
            bytes.extend(compact_size(*v));
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        for map in [&self.effects, &self.auths] {
            bytes.extend(compact_size(usize_to_u64(map.len())));
            for (t, v, payload) in map {
                bytes.extend(compact_size(*t));
                bytes.extend(compact_size(*v));
                bytes.extend(compact_size(usize_to_u64(payload.len())));
                bytes.extend_from_slice(payload);
            }
        }
        bytes
    }

    /// Removes every entry of `bundle_type` from the three maps.
    fn without_bundle(mut self, bundle_type: u64) -> Self {
        self.deltas.retain(|(t, _, _)| *t != bundle_type);
        self.effects.retain(|(t, _, _)| *t != bundle_type);
        self.auths.retain(|(t, _, _)| *t != bundle_type);
        self
    }

    fn effect_mut(&mut self, bundle_type: u64) -> &mut Vec<u8> {
        &mut self
            .effects
            .iter_mut()
            .find(|(t, _, _)| *t == bundle_type)
            .expect("the bundle has effecting data")
            .2
    }

    fn auth_mut(&mut self, bundle_type: u64) -> &mut Vec<u8> {
        &mut self
            .auths
            .iter_mut()
            .find(|(t, _, _)| *t == bundle_type)
            .expect("the bundle has authorizing data")
            .2
    }
}

/// The transparent effecting payload of one input spending `outpoint` with
/// `sequence`, and one output.
fn transparent_effects(outpoint: [u8; 36], sequence: u32) -> Vec<u8> {
    let mut payload = compact_size(1);
    payload.extend_from_slice(&outpoint);
    payload.extend_from_slice(&sequence.to_le_bytes());
    payload.extend(compact_size(1));
    payload.extend_from_slice(&5000u64.to_le_bytes());
    payload.extend(compact_size(2));
    payload.extend_from_slice(&[0x51, 0x87]);
    payload
}

/// The transparent authorizing payload of one input with a two-byte scriptSig.
fn transparent_auth() -> Vec<u8> {
    let mut payload = super::codec::SIGHASH_INFO_V0.to_vec();
    payload.extend(compact_size(2));
    payload.extend_from_slice(&[0x01, 0x02]);
    payload
}

/// A transaction with one transparent input spending `outpoint`.
fn transparent_envelope(outpoint: [u8; 36]) -> Envelope {
    let mut envelope = Envelope::empty();
    envelope
        .effects
        .push((0, 0, transparent_effects(outpoint, 0xffff_fffe)));
    envelope.auths.push((0, 0, transparent_auth()));
    envelope
}

/// A non-null previous output.
const OUTPOINT: [u8; 36] = [7; 36];
