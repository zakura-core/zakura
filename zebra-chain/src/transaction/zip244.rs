//! Native ZIP-244 transaction identifier (txid) and authorizing-data commitment.
//!
//! Computes the v5 txid digest tree and the ZIP-244 authorizing-data digest
//! directly from Zebra's parsed [`Transaction`], without converting to the
//! `librustzcash` transaction type via [`Transaction::to_librustzcash`].
//!
//! That conversion re-serializes the whole transaction and re-parses it,
//! decompressing every Jubjub/Pallas curve point (`cv`, `rk`, ephemeral keys,
//! …) into typed group elements — purely so `librustzcash` can re-serialize
//! those same bytes back into the BLAKE2b digest tree. In the checkpoint range
//! the points are never otherwise needed (no proof/signature verification), so
//! the decompression is pure overhead; profiling the heavy shielded region
//! attributes ~44% of all CPU to these reparses. This module feeds Zebra's
//! canonical field bytes straight into the same BLAKE2b tree.
//!
//! The output is **byte-for-byte identical** to the `librustzcash` computation;
//! this is consensus-critical and is proven by the differential property test
//! `native_zip244_matches_librustzcash` (and
//! `txid_and_auth_digest_matches_separate`) in `transaction/tests/prop.rs`, plus
//! the existing ZIP-244 known-answer vectors and a clean differential mainnet
//! sync.
//!
//! Specified in [ZIP-244] and [ZIP-225]. The personalizations and field
//! orderings mirror `zcash_primitives::transaction::txid` and
//! `orchard::bundle::commitments`.
//!
//! Only v5 transactions are handled here; v6 (the unstable `tx_v6` feature,
//! which can carry a ZIP-233 header field) still routes through `librustzcash`.
//!
//! [ZIP-244]: https://zips.z.cash/zip-0244
//! [ZIP-225]: https://zips.z.cash/zip-0225

use std::io;

use blake2b_simd::{Hash as Blake2bHash, Params, State};

use crate::{
    orchard,
    parameters::TX_V5_VERSION_GROUP_ID,
    sapling,
    serialization::ZcashSerialize,
    transaction::{AuthDigest, Hash, Transaction},
    transparent,
};

// Reference implementation for the ZIP-244 txid/auth personalizations:
// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L33-L68
//
// txid tree root personalization (`ZcashTxHash_` ‖ consensus_branch_id LE32)
const ZCASH_TX_PERSONALIZATION_PREFIX: &[u8; 12] = b"ZcashTxHash_";

// txid level-1 node personalizations
const ZCASH_HEADERS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdHeadersHash";
const ZCASH_TRANSPARENT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdTranspaHash";
const ZCASH_SAPLING_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSaplingHash";
const ZCASH_ORCHARD_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrchardHash";

// txid transparent level-2 node personalizations
const ZCASH_PREVOUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdPrevoutHash";
const ZCASH_SEQUENCE_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSequencHash";
const ZCASH_OUTPUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOutputsHash";

// txid sapling level-2 node personalizations
const ZCASH_SAPLING_SPENDS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendsHash";
const ZCASH_SAPLING_SPENDS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendCHash";
const ZCASH_SAPLING_SPENDS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendNHash";
const ZCASH_SAPLING_OUTPUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutputHash";
const ZCASH_SAPLING_OUTPUTS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutC__Hash";
const ZCASH_SAPLING_OUTPUTS_MEMOS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutM__Hash";
const ZCASH_SAPLING_OUTPUTS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutN__Hash";

// txid orchard level-2 node personalizations
const ZCASH_ORCHARD_ACTIONS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActCHash";
const ZCASH_ORCHARD_ACTIONS_MEMOS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActMHash";
const ZCASH_ORCHARD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActNHash";

// auth-digest tree root personalization (`ZTxAuthHash_` ‖ consensus_branch_id LE32)
const ZCASH_AUTH_PERSONALIZATION_PREFIX: &[u8; 12] = b"ZTxAuthHash_";
const ZCASH_TRANSPARENT_SCRIPTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthTransHash";
const ZCASH_SAPLING_SIGS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthSapliHash";
const ZCASH_ORCHARD_SIGS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthOrchaHash";

/// A new BLAKE2b-256 state with the given 16-byte personalization.
fn hasher(personal: &[u8; 16]) -> State {
    Params::new().hash_length(32).personal(personal).to_state()
}

/// `io::Write` adapter that feeds bytes into a BLAKE2b [`State`], so Zebra's
/// existing [`ZcashSerialize`] implementations can write a field's canonical
/// bytes straight into a hash with no intermediate allocation.
struct HashWriter<'a>(&'a mut State);

impl io::Write for HashWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Write a value's canonical [`ZcashSerialize`] bytes into a BLAKE2b state.
fn update_serialized<T: ZcashSerialize>(state: &mut State, value: &T) {
    value
        .zcash_serialize(HashWriter(state))
        .expect("writing to a BLAKE2b state is infallible");
}

/// The fields of a v5 transaction needed to compute its digests.
///
/// Returns `None` for non-v5 transactions (the caller falls back to
/// `librustzcash`).
struct V5Parts<'a> {
    network_upgrade: crate::parameters::NetworkUpgrade,
    lock_time: &'a crate::transaction::LockTime,
    expiry_height: crate::block::Height,
    inputs: &'a [transparent::Input],
    outputs: &'a [transparent::Output],
    sapling: Option<&'a sapling::ShieldedData<sapling::SharedAnchor>>,
    orchard: Option<&'a orchard::ShieldedData>,
}

fn v5_parts(tx: &Transaction) -> Option<V5Parts<'_>> {
    match tx {
        Transaction::V5 {
            network_upgrade,
            lock_time,
            expiry_height,
            inputs,
            outputs,
            sapling_shielded_data,
            orchard_shielded_data,
        } => Some(V5Parts {
            network_upgrade: *network_upgrade,
            lock_time,
            expiry_height: *expiry_height,
            inputs,
            outputs,
            sapling: sapling_shielded_data.as_ref(),
            orchard: orchard_shielded_data.as_ref(),
        }),
        _ => None,
    }
}

/// The consensus branch id of a v5 transaction, as the LE `u32` committed to by
/// the header digest and both tree-root personalizations.
fn consensus_branch_id(parts: &V5Parts) -> u32 {
    u32::from(
        parts
            .network_upgrade
            .branch_id()
            .expect("v5 network upgrade has a consensus branch id"),
    )
}

// --- txid digest (ZIP-244 §T) -------------------------------------------------

/// ZIP-244 §T.1 header digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L170
fn hash_header(parts: &V5Parts) -> Blake2bHash {
    let mut h = hasher(ZCASH_HEADERS_HASH_PERSONALIZATION);
    // header: fOverwintered (set for v5) in the high bit, version 5 in the low bits.
    h.update(&(0x8000_0005_u32).to_le_bytes());
    h.update(&TX_V5_VERSION_GROUP_ID.to_le_bytes());
    h.update(&consensus_branch_id(parts).to_le_bytes());
    // lock_time and expiry_height are each a single LE u32; `LockTime` serializes
    // as exactly that u32.
    update_serialized(&mut h, parts.lock_time);
    h.update(&parts.expiry_height.0.to_le_bytes());
    h.finalize()
}

/// ZIP-244 §T.2a prevouts digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L85
fn hash_prevouts(inputs: &[transparent::Input]) -> Blake2bHash {
    let mut h = hasher(ZCASH_PREVOUTS_HASH_PERSONALIZATION);
    for input in inputs {
        match input {
            transparent::Input::PrevOut { outpoint, .. } => update_serialized(&mut h, outpoint),
            // A coinbase input commits to the null prevout, exactly as Zebra's
            // `Input` serialization writes it.
            transparent::Input::Coinbase { .. } => {
                h.update(&[0u8; 32]);
                h.update(&0xffff_ffff_u32.to_le_bytes());
            }
        }
    }
    h.finalize()
}

/// ZIP-244 §T.2b sequence digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L71
fn hash_sequence(inputs: &[transparent::Input]) -> Blake2bHash {
    let mut h = hasher(ZCASH_SEQUENCE_HASH_PERSONALIZATION);
    for input in inputs {
        h.update(&input.sequence().to_le_bytes());
    }
    h.finalize()
}

/// ZIP-244 §T.2c outputs digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L85
fn hash_outputs(outputs: &[transparent::Output]) -> Blake2bHash {
    let mut h = hasher(ZCASH_OUTPUTS_HASH_PERSONALIZATION);
    for output in outputs {
        update_serialized(&mut h, output);
    }
    h.finalize()
}

/// ZIP-244 §T.2 transparent digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L196
fn hash_transparent_txid(
    inputs: &[transparent::Input],
    outputs: &[transparent::Output],
) -> Blake2bHash {
    let mut h = hasher(ZCASH_TRANSPARENT_HASH_PERSONALIZATION);
    // The transparent bundle is absent (and the digest is the bare
    // personalization hash) only when there are no inputs and no outputs.
    if !inputs.is_empty() || !outputs.is_empty() {
        h.update(hash_prevouts(inputs).as_bytes());
        h.update(hash_sequence(inputs).as_bytes());
        h.update(hash_outputs(outputs).as_bytes());
    }
    h.finalize()
}

/// ZIP-244 §T.3a sapling spends digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L100
fn hash_sapling_spends(sapling: &sapling::ShieldedData<sapling::SharedAnchor>) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_SPENDS_HASH_PERSONALIZATION);
    if sapling.spends().next().is_some() {
        let mut ch = hasher(ZCASH_SAPLING_SPENDS_COMPACT_HASH_PERSONALIZATION);
        let mut nh = hasher(ZCASH_SAPLING_SPENDS_NONCOMPACT_HASH_PERSONALIZATION);
        // In a v5 transaction every spend shares the one anchor.
        let anchor = <[u8; 32]>::from(
            sapling
                .shared_anchor()
                .expect("v5 sapling spends share an anchor when present"),
        );
        for spend in sapling.spends() {
            ch.update(&<[u8; 32]>::from(spend.nullifier));

            update_serialized(&mut nh, &spend.cv);
            nh.update(&anchor);
            nh.update(&<[u8; 32]>::from(spend.rk.clone()));
        }
        h.update(ch.finalize().as_bytes());
        h.update(nh.finalize().as_bytes());
    }
    h.finalize()
}

/// ZIP-244 §T.3b sapling outputs digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L132
fn hash_sapling_outputs(sapling: &sapling::ShieldedData<sapling::SharedAnchor>) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_OUTPUTS_HASH_PERSONALIZATION);
    if sapling.outputs().next().is_some() {
        let mut ch = hasher(ZCASH_SAPLING_OUTPUTS_COMPACT_HASH_PERSONALIZATION);
        let mut mh = hasher(ZCASH_SAPLING_OUTPUTS_MEMOS_HASH_PERSONALIZATION);
        let mut nh = hasher(ZCASH_SAPLING_OUTPUTS_NONCOMPACT_HASH_PERSONALIZATION);
        for output in sapling.outputs() {
            ch.update(&output.cm_u.to_bytes());
            ch.update(&<[u8; 32]>::from(&output.ephemeral_key));
            ch.update(&output.enc_ciphertext.0[..52]);

            mh.update(&output.enc_ciphertext.0[52..564]);

            update_serialized(&mut nh, &output.cv);
            nh.update(&output.enc_ciphertext.0[564..]);
            nh.update(&output.out_ciphertext.0[..]);
        }
        h.update(ch.finalize().as_bytes());
        h.update(mh.finalize().as_bytes());
        h.update(nh.finalize().as_bytes());
    }
    h.finalize()
}

/// ZIP-244 §T.3 sapling digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L209
fn hash_sapling_txid(
    sapling: Option<&sapling::ShieldedData<sapling::SharedAnchor>>,
) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_HASH_PERSONALIZATION);
    if let Some(sapling) = sapling {
        // `ShieldedData` only exists with at least one spend or output, so this
        // matches librustzcash's "non-empty bundle" branch.
        if sapling.spends().next().is_some() || sapling.outputs().next().is_some() {
            h.update(hash_sapling_spends(sapling).as_bytes());
            h.update(hash_sapling_outputs(sapling).as_bytes());
            h.update(&sapling.value_balance.zatoshis().to_le_bytes());
        }
    }
    h.finalize()
}

/// ZIP-244 §T.4 orchard digest (mirrors `orchard::bundle::commitments::hash_bundle_txid_data`).
///
/// Reference implementation:
/// https://github.com/zcash/orchard/blob/82e0739ced29e1c113804e1abba48976bbfc665e/src/bundle/commitments.rs#L30
fn hash_orchard_txid(orchard: Option<&orchard::ShieldedData>) -> Blake2bHash {
    let mut h = hasher(ZCASH_ORCHARD_HASH_PERSONALIZATION);
    if let Some(orchard) = orchard {
        let mut ch = hasher(ZCASH_ORCHARD_ACTIONS_COMPACT_HASH_PERSONALIZATION);
        let mut mh = hasher(ZCASH_ORCHARD_ACTIONS_MEMOS_HASH_PERSONALIZATION);
        let mut nh = hasher(ZCASH_ORCHARD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION);
        for action in orchard.actions() {
            ch.update(&<[u8; 32]>::from(action.nullifier));
            ch.update(&<[u8; 32]>::from(action.cm_x));
            update_serialized(&mut ch, &action.ephemeral_key);
            ch.update(&action.enc_ciphertext.0[..52]);

            mh.update(&action.enc_ciphertext.0[52..564]);

            update_serialized(&mut nh, &action.cv);
            nh.update(&<[u8; 32]>::from(action.rk));
            nh.update(&action.enc_ciphertext.0[564..]);
            nh.update(&action.out_ciphertext.0[..]);
        }
        h.update(ch.finalize().as_bytes());
        h.update(mh.finalize().as_bytes());
        h.update(nh.finalize().as_bytes());
        h.update(&[orchard.flags.bits()]);
        h.update(&orchard.value_balance.zatoshis().to_le_bytes());
        h.update(&<[u8; 32]>::from(orchard.shared_anchor));
    }
    h.finalize()
}

/// Combine the four level-1 digests into the txid (ZIP-244 txid digest).
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L426
fn txid_inner(parts: &V5Parts) -> Hash {
    let header = hash_header(parts);
    let transparent = hash_transparent_txid(parts.inputs, parts.outputs);
    let sapling = hash_sapling_txid(parts.sapling);
    let orchard = hash_orchard_txid(parts.orchard);

    let mut personal = [0u8; 16];
    personal[..12].copy_from_slice(ZCASH_TX_PERSONALIZATION_PREFIX);
    personal[12..].copy_from_slice(&consensus_branch_id(parts).to_le_bytes());

    let mut h = hasher(&personal);
    h.update(header.as_bytes());
    h.update(transparent.as_bytes());
    h.update(sapling.as_bytes());
    h.update(orchard.as_bytes());

    Hash(
        h.finalize()
            .as_bytes()
            .try_into()
            .expect("BLAKE2b-256 digest is 32 bytes"),
    )
}

// --- auth digest (ZIP-244 authorizing-data commitment) ------------------------

/// ZIP-244 transparent script-sig digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L379-L390
fn hash_transparent_auth(
    inputs: &[transparent::Input],
    outputs: &[transparent::Output],
) -> Blake2bHash {
    let mut h = hasher(ZCASH_TRANSPARENT_SCRIPTS_HASH_PERSONALIZATION);
    // Present only when the transparent bundle is present (any input or output).
    if !inputs.is_empty() || !outputs.is_empty() {
        for input in inputs {
            match input {
                transparent::Input::PrevOut { unlock_script, .. } => {
                    update_serialized(&mut h, unlock_script)
                }
                transparent::Input::Coinbase { .. } => {
                    let script = input
                        .coinbase_script()
                        .expect("v5 coinbase input has a valid script sig");
                    update_serialized(&mut h, &script);
                }
            }
        }
    }
    h.finalize()
}

/// ZIP-244 sapling auth digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L392
fn hash_sapling_auth(
    sapling: Option<&sapling::ShieldedData<sapling::SharedAnchor>>,
) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_SIGS_HASH_PERSONALIZATION);
    if let Some(sapling) = sapling {
        for spend in sapling.spends() {
            h.update(&spend.zkproof.0[..]);
        }
        for spend in sapling.spends() {
            h.update(&<[u8; 64]>::from(spend.spend_auth_sig)[..]);
        }
        for output in sapling.outputs() {
            h.update(&output.zkproof.0[..]);
        }
        h.update(&<[u8; 64]>::from(sapling.binding_sig)[..]);
    }
    h.finalize()
}

/// ZIP-244 orchard auth digest (mirrors `orchard::bundle::commitments::hash_bundle_auth_data`).
///
/// Reference implementation:
/// https://github.com/zcash/orchard/blob/82e0739ced29e1c113804e1abba48976bbfc665e/src/bundle/commitments.rs#L69-L92
fn hash_orchard_auth(orchard: Option<&orchard::ShieldedData>) -> Blake2bHash {
    let mut h = hasher(ZCASH_ORCHARD_SIGS_HASH_PERSONALIZATION);
    if let Some(orchard) = orchard {
        h.update(&orchard.proof.0[..]);
        for action in orchard.actions.iter() {
            update_serialized(&mut h, &action.spend_auth_sig);
        }
        update_serialized(&mut h, &orchard.binding_sig);
    }
    h.finalize()
}

/// Combine the three authorizing-data digests into the ZIP-244 auth commitment.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L426-L448
fn auth_digest_inner(parts: &V5Parts) -> AuthDigest {
    let transparent = hash_transparent_auth(parts.inputs, parts.outputs);
    let sapling = hash_sapling_auth(parts.sapling);
    let orchard = hash_orchard_auth(parts.orchard);

    let mut personal = [0u8; 16];
    personal[..12].copy_from_slice(ZCASH_AUTH_PERSONALIZATION_PREFIX);
    personal[12..].copy_from_slice(&consensus_branch_id(parts).to_le_bytes());

    let mut h = hasher(&personal);
    h.update(transparent.as_bytes());
    h.update(sapling.as_bytes());
    h.update(orchard.as_bytes());

    AuthDigest(
        h.finalize()
            .as_bytes()
            .try_into()
            .expect("BLAKE2b-256 digest is 32 bytes"),
    )
}

// --- public entry points ------------------------------------------------------

/// Computes the txid of a v5 transaction natively, or returns `None` for other
/// versions (the caller falls back to the `librustzcash` path).
pub(crate) fn txid(tx: &Transaction) -> Option<Hash> {
    Some(txid_inner(&v5_parts(tx)?))
}

/// Computes the ZIP-244 authorizing-data digest of a v5 transaction natively, or
/// returns `None` for other versions.
pub(crate) fn auth_digest(tx: &Transaction) -> Option<AuthDigest> {
    Some(auth_digest_inner(&v5_parts(tx)?))
}

/// Computes both the txid and the ZIP-244 authorizing-data digest of a v5
/// transaction natively, or returns `None` for other versions.
pub(crate) fn txid_and_auth_digest(tx: &Transaction) -> Option<(Hash, AuthDigest)> {
    let parts = v5_parts(tx)?;
    Some((txid_inner(&parts), auth_digest_inner(&parts)))
}
