//! Native ZIP-244 transaction identifier (txid) and authorizing-data commitment.
//!
//! Computes the v5/v6 txid digest tree and the ZIP-244 authorizing-data digest
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
//! [ZIP-244]: https://zips.z.cash/zip-0244
//! [ZIP-225]: https://zips.z.cash/zip-0225

use std::io;

use blake2b_simd::{Hash as Blake2bHash, Params, State};

use crate::{
    orchard,
    parameters::{NetworkUpgrade, TX_V5_VERSION_GROUP_ID, TX_V6_VERSION_GROUP_ID},
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
const TX_OVERWINTERED_FLAG: u32 = 1 << 31;
const TX_V5_VERSION: u32 = 5;
const TX_V6_VERSION: u32 = 6;

// txid level-1 node personalizations
const ZCASH_HEADERS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdHeadersHash";
const ZCASH_TRANSPARENT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdTranspaHash";
const ZCASH_SAPLING_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSaplingHash";
const ZCASH_ORCHARD_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrchardHash";
const ZCASH_ORCHARD_V6_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrchardH_v6";
const ZCASH_IRONWOOD_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdIronwd_H_v6";

// txid transparent level-2 node personalizations
const ZCASH_PREVOUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdPrevoutHash";
const ZCASH_SEQUENCE_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSequencHash";
const ZCASH_OUTPUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOutputsHash";

// txid sapling level-2 node personalizations
const ZCASH_SAPLING_SPENDS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendsHash";
const ZCASH_SAPLING_SPENDS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendCHash";
const ZCASH_SAPLING_SPENDS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendNHash";
const ZCASH_SAPLING_SPENDS_V6_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSSpendNH_v6";
const ZCASH_SAPLING_OUTPUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutputHash";
const ZCASH_SAPLING_OUTPUTS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutC__Hash";
const ZCASH_SAPLING_OUTPUTS_MEMOS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutM__Hash";
const ZCASH_SAPLING_OUTPUTS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdSOutN__Hash";

// txid orchard level-2 node personalizations
const ZCASH_ORCHARD_ACTIONS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActCHash";
const ZCASH_ORCHARD_ACTIONS_MEMOS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActMHash";
const ZCASH_ORCHARD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrcActNHash";
const ZCASH_IRONWOOD_ACTIONS_COMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdIrnActCH_v6";
const ZCASH_IRONWOOD_ACTIONS_MEMOS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdIrnActMH_v6";
const ZCASH_IRONWOOD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxIdIrnActNH_v6";

// auth-digest tree root personalization (`ZTxAuthHash_` ‖ consensus_branch_id LE32)
const ZCASH_AUTH_PERSONALIZATION_PREFIX: &[u8; 12] = b"ZTxAuthHash_";
const ZCASH_TRANSPARENT_SCRIPTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthTransHash";
const ZCASH_SAPLING_SIGS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthSapliHash";
const ZCASH_SAPLING_V6_SIGS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthSapliH_v6";
const ZCASH_ORCHARD_SIGS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthOrchaHash";
const ZCASH_ORCHARD_V6_SIGS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthOrchaH_v6";
const ZCASH_IRONWOOD_SIGS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthIrnwdH_v6";

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Zip244Version {
    V5,
    V6,
}

impl Zip244Version {
    fn header(self) -> u32 {
        TX_OVERWINTERED_FLAG
            | match self {
                Self::V5 => TX_V5_VERSION,
                Self::V6 => TX_V6_VERSION,
            }
    }

    fn version_group_id(self) -> u32 {
        match self {
            Self::V5 => TX_V5_VERSION_GROUP_ID,
            Self::V6 => TX_V6_VERSION_GROUP_ID,
        }
    }

    fn sapling_spends_noncompact_personalization(self) -> &'static [u8; 16] {
        match self {
            Self::V5 => ZCASH_SAPLING_SPENDS_NONCOMPACT_HASH_PERSONALIZATION,
            Self::V6 => ZCASH_SAPLING_SPENDS_V6_NONCOMPACT_HASH_PERSONALIZATION,
        }
    }

    fn sapling_spends_txid_includes_anchor(self) -> bool {
        match self {
            Self::V5 => true,
            Self::V6 => false,
        }
    }

    fn sapling_auth_personalization(self) -> &'static [u8; 16] {
        match self {
            Self::V5 => ZCASH_SAPLING_SIGS_HASH_PERSONALIZATION,
            Self::V6 => ZCASH_SAPLING_V6_SIGS_HASH_PERSONALIZATION,
        }
    }

    fn sapling_auth_includes_anchor(self) -> bool {
        match self {
            Self::V5 => false,
            Self::V6 => true,
        }
    }

    fn orchard_format(self) -> BundleCommitmentFormat {
        match self {
            Self::V5 => BundleCommitmentFormat::OrchardV5,
            Self::V6 => BundleCommitmentFormat::OrchardV6,
        }
    }

    fn has_ironwood(self) -> bool {
        matches!(self, Self::V6)
    }
}

#[derive(Clone, Copy, Debug)]
struct BundleCommitmentPersonalizations {
    bundle: &'static [u8; 16],
    actions_compact: &'static [u8; 16],
    actions_memos: &'static [u8; 16],
    actions_noncompact: &'static [u8; 16],
    auth: &'static [u8; 16],
}

const ORCHARD_V5_PERSONALIZATIONS: BundleCommitmentPersonalizations =
    BundleCommitmentPersonalizations {
        bundle: ZCASH_ORCHARD_HASH_PERSONALIZATION,
        actions_compact: ZCASH_ORCHARD_ACTIONS_COMPACT_HASH_PERSONALIZATION,
        actions_memos: ZCASH_ORCHARD_ACTIONS_MEMOS_HASH_PERSONALIZATION,
        actions_noncompact: ZCASH_ORCHARD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION,
        auth: ZCASH_ORCHARD_SIGS_HASH_PERSONALIZATION,
    };

const ORCHARD_V6_PERSONALIZATIONS: BundleCommitmentPersonalizations =
    BundleCommitmentPersonalizations {
        bundle: ZCASH_ORCHARD_V6_HASH_PERSONALIZATION,
        actions_compact: ZCASH_ORCHARD_ACTIONS_COMPACT_HASH_PERSONALIZATION,
        actions_memos: ZCASH_ORCHARD_ACTIONS_MEMOS_HASH_PERSONALIZATION,
        actions_noncompact: ZCASH_ORCHARD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION,
        auth: ZCASH_ORCHARD_V6_SIGS_HASH_PERSONALIZATION,
    };

const IRONWOOD_V6_PERSONALIZATIONS: BundleCommitmentPersonalizations =
    BundleCommitmentPersonalizations {
        bundle: ZCASH_IRONWOOD_HASH_PERSONALIZATION,
        actions_compact: ZCASH_IRONWOOD_ACTIONS_COMPACT_HASH_PERSONALIZATION,
        actions_memos: ZCASH_IRONWOOD_ACTIONS_MEMOS_HASH_PERSONALIZATION,
        actions_noncompact: ZCASH_IRONWOOD_ACTIONS_NONCOMPACT_HASH_PERSONALIZATION,
        auth: ZCASH_IRONWOOD_SIGS_HASH_PERSONALIZATION,
    };

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BundleCommitmentFormat {
    OrchardV5,
    OrchardV6,
    IronwoodV6,
}

impl BundleCommitmentFormat {
    fn personalizations(self) -> BundleCommitmentPersonalizations {
        match self {
            Self::OrchardV5 => ORCHARD_V5_PERSONALIZATIONS,
            Self::OrchardV6 => ORCHARD_V6_PERSONALIZATIONS,
            Self::IronwoodV6 => IRONWOOD_V6_PERSONALIZATIONS,
        }
    }

    fn includes_anchor_in_txid_digest(self) -> bool {
        match self {
            Self::OrchardV5 => true,
            Self::OrchardV6 | Self::IronwoodV6 => false,
        }
    }

    fn includes_anchor_in_authorizing_digest(self) -> bool {
        match self {
            Self::OrchardV5 => false,
            Self::OrchardV6 | Self::IronwoodV6 => true,
        }
    }
}

/// The fields of a v5/v6 transaction needed to compute its digests.
///
/// Returns `None` for unsupported transaction versions (the caller falls back
/// to `librustzcash`).
struct Zip244Parts<'a> {
    version: Zip244Version,
    network_upgrade: NetworkUpgrade,
    lock_time: u32,
    expiry_height: crate::block::Height,
    inputs: &'a [transparent::Input],
    outputs: &'a [transparent::Output],
    sapling: Option<&'a sapling::ShieldedData<sapling::SharedAnchor>>,
    orchard: Option<&'a orchard::ShieldedData>,
    ironwood: Option<&'a orchard::ShieldedData>,
}

fn zip244_parts(tx: &Transaction) -> Option<Zip244Parts<'_>> {
    let version = match tx.version() {
        TX_V5_VERSION => Zip244Version::V5,
        TX_V6_VERSION => Zip244Version::V6,
        _ => return None,
    };

    Some(Zip244Parts {
        version,
        network_upgrade: tx.network_upgrade()?,
        lock_time: tx.raw_lock_time(),
        expiry_height: tx.expiry_height().unwrap_or(crate::block::Height(0)),
        inputs: tx.inputs(),
        outputs: tx.outputs(),
        sapling: tx.sapling_shielded_data(),
        orchard: tx.orchard_shielded_data(),
        ironwood: tx.ironwood_shielded_data(),
    })
}

/// The consensus branch ID committed to by the header digest and both tree-root
/// personalizations.
fn consensus_branch_id(parts: &Zip244Parts) -> u32 {
    u32::from(
        parts
            .network_upgrade
            .branch_id()
            .expect("v5/v6 network upgrade has a consensus branch ID"),
    )
}

// --- txid digest (ZIP-244 §T) -------------------------------------------------

/// ZIP-244 §T.1 header digest.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L170
fn hash_header(parts: &Zip244Parts) -> Blake2bHash {
    let mut h = hasher(ZCASH_HEADERS_HASH_PERSONALIZATION);
    h.update(&parts.version.header().to_le_bytes());
    h.update(&parts.version.version_group_id().to_le_bytes());
    h.update(&consensus_branch_id(parts).to_le_bytes());
    // lock_time and expiry_height are each a single LE u32.
    h.update(&parts.lock_time.to_le_bytes());
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
fn hash_sapling_spends(
    sapling: &sapling::ShieldedData<sapling::SharedAnchor>,
    version: Zip244Version,
) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_SPENDS_HASH_PERSONALIZATION);
    if sapling.spends().next().is_some() {
        let mut ch = hasher(ZCASH_SAPLING_SPENDS_COMPACT_HASH_PERSONALIZATION);
        let mut nh = hasher(version.sapling_spends_noncompact_personalization());
        let anchor = version.sapling_spends_txid_includes_anchor().then(|| {
            <[u8; 32]>::from(
                sapling
                    .shared_anchor()
                    .expect("sapling spends share an anchor when present"),
            )
        });
        for spend in sapling.spends() {
            ch.update(&<[u8; 32]>::from(spend.nullifier));

            update_serialized(&mut nh, &spend.cv);
            if let Some(anchor) = &anchor {
                nh.update(anchor);
            }
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
    version: Zip244Version,
) -> Blake2bHash {
    let mut h = hasher(ZCASH_SAPLING_HASH_PERSONALIZATION);
    if let Some(sapling) = sapling {
        // `ShieldedData` only exists with at least one spend or output, so this
        // matches librustzcash's "non-empty bundle" branch.
        if sapling.spends().next().is_some() || sapling.outputs().next().is_some() {
            h.update(hash_sapling_spends(sapling, version).as_bytes());
            h.update(hash_sapling_outputs(sapling).as_bytes());
            h.update(&sapling.value_balance.zatoshis().to_le_bytes());
        }
    }
    h.finalize()
}

/// ZIP-244 §T.4 Orchard-style bundle digest.
///
/// Mirrors `orchard::bundle::commitments::hash_bundle_txid_data`.
fn hash_bundle_txid(
    bundle: Option<&orchard::ShieldedData>,
    format: BundleCommitmentFormat,
) -> Blake2bHash {
    let personalizations = format.personalizations();
    let mut h = hasher(personalizations.bundle);
    if let Some(bundle) = bundle {
        let mut ch = hasher(personalizations.actions_compact);
        let mut mh = hasher(personalizations.actions_memos);
        let mut nh = hasher(personalizations.actions_noncompact);
        for action in bundle.actions() {
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
        h.update(&[bundle.flags.bits()]);
        h.update(&bundle.value_balance.zatoshis().to_le_bytes());
        if format.includes_anchor_in_txid_digest() {
            h.update(&<[u8; 32]>::from(bundle.shared_anchor));
        }
    }
    h.finalize()
}

/// Combine the level-1 digests into the txid (ZIP-244 txid digest).
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L426
fn txid_inner(parts: &Zip244Parts) -> Hash {
    let header = hash_header(parts);
    let transparent = hash_transparent_txid(parts.inputs, parts.outputs);
    let sapling = hash_sapling_txid(parts.sapling, parts.version);
    let orchard = hash_bundle_txid(parts.orchard, parts.version.orchard_format());
    let ironwood = parts
        .version
        .has_ironwood()
        .then(|| hash_bundle_txid(parts.ironwood, BundleCommitmentFormat::IronwoodV6));

    let mut personal = [0u8; 16];
    personal[..12].copy_from_slice(ZCASH_TX_PERSONALIZATION_PREFIX);
    personal[12..].copy_from_slice(&consensus_branch_id(parts).to_le_bytes());

    let mut h = hasher(&personal);
    h.update(header.as_bytes());
    h.update(transparent.as_bytes());
    h.update(sapling.as_bytes());
    h.update(orchard.as_bytes());
    if let Some(ironwood) = ironwood {
        h.update(ironwood.as_bytes());
    }

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
    version: Zip244Version,
) -> Blake2bHash {
    let mut h = hasher(version.sapling_auth_personalization());
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
        if version.sapling_auth_includes_anchor() && sapling.spends().next().is_some() {
            let anchor = <[u8; 32]>::from(
                sapling
                    .shared_anchor()
                    .expect("sapling spends share an anchor when present"),
            );
            h.update(&anchor);
        }
    }
    h.finalize()
}

/// ZIP-244 Orchard-style auth digest.
///
/// Mirrors `orchard::bundle::commitments::hash_bundle_auth_data`.
fn hash_bundle_auth(
    bundle: Option<&orchard::ShieldedData>,
    format: BundleCommitmentFormat,
) -> Blake2bHash {
    let mut h = hasher(format.personalizations().auth);
    if let Some(bundle) = bundle {
        h.update(&bundle.proof.0[..]);
        for action in bundle.actions.iter() {
            update_serialized(&mut h, &action.spend_auth_sig);
        }
        update_serialized(&mut h, &bundle.binding_sig);
        if format.includes_anchor_in_authorizing_digest() {
            h.update(&<[u8; 32]>::from(bundle.shared_anchor));
        }
    }
    h.finalize()
}

/// Combine the authorizing-data digests into the ZIP-244 auth commitment.
///
/// Reference implementation:
/// https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L426-L448
fn auth_digest_inner(parts: &Zip244Parts) -> AuthDigest {
    let transparent = hash_transparent_auth(parts.inputs, parts.outputs);
    let sapling = hash_sapling_auth(parts.sapling, parts.version);
    let orchard = hash_bundle_auth(parts.orchard, parts.version.orchard_format());
    let ironwood = parts
        .version
        .has_ironwood()
        .then(|| hash_bundle_auth(parts.ironwood, BundleCommitmentFormat::IronwoodV6));

    let mut personal = [0u8; 16];
    personal[..12].copy_from_slice(ZCASH_AUTH_PERSONALIZATION_PREFIX);
    personal[12..].copy_from_slice(&consensus_branch_id(parts).to_le_bytes());

    let mut h = hasher(&personal);
    h.update(transparent.as_bytes());
    h.update(sapling.as_bytes());
    h.update(orchard.as_bytes());
    if let Some(ironwood) = ironwood {
        h.update(ironwood.as_bytes());
    }

    AuthDigest(
        h.finalize()
            .as_bytes()
            .try_into()
            .expect("BLAKE2b-256 digest is 32 bytes"),
    )
}

// --- public entry points ------------------------------------------------------

/// Computes the txid of a v5/v6 transaction natively, or returns `None` for
/// other versions (the caller falls back to the `librustzcash` path).
pub(crate) fn txid(tx: &Transaction) -> Option<Hash> {
    Some(txid_inner(&zip244_parts(tx)?))
}

/// Computes the ZIP-244 authorizing-data digest of a v5/v6 transaction
/// natively, or returns `None` for other versions.
pub(crate) fn auth_digest(tx: &Transaction) -> Option<AuthDigest> {
    Some(auth_digest_inner(&zip244_parts(tx)?))
}

/// Computes both the txid and the ZIP-244 authorizing-data digest of a v5/v6
/// transaction natively, or returns `None` for other versions.
pub(crate) fn txid_and_auth_digest(tx: &Transaction) -> Option<(Hash, AuthDigest)> {
    let parts = zip244_parts(tx)?;
    Some((txid_inner(&parts), auth_digest_inner(&parts)))
}
