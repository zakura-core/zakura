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
    transaction::{sighash::CanonicalHashType, AuthDigest, Hash, SigHash, Transaction},
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

// signature transparent level-2 node personalizations
const ZCASH_TRANSPARENT_INPUT_HASH_PERSONALIZATION: &[u8; 16] = b"Zcash___TxInHash";
const ZCASH_TRANSPARENT_AMOUNTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxTrAmountsHash";
const ZCASH_TRANSPARENT_SCRIPTPUBKEYS_HASH_PERSONALIZATION: &[u8; 16] = b"ZTxTrScriptsHash";

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

const EMPTY_TRANSPARENT_TXID_HASH: &[u8; 32] = &[
    0xc3, 0x3f, 0x2e, 0x95, 0x70, 0x5f, 0xaa, 0xb3, 0x5f, 0x8d, 0x53, 0x3f, 0xa6, 0x1e, 0x95, 0xc3,
    0xb7, 0xaa, 0xba, 0x07, 0x76, 0xb8, 0x74, 0xa9, 0xf7, 0x4f, 0xc1, 0x27, 0x84, 0x37, 0x6a, 0x59,
];
const EMPTY_SAPLING_TXID_HASH: &[u8; 32] = &[
    0x6f, 0x2f, 0xc8, 0xf9, 0x8f, 0xea, 0xfd, 0x94, 0xe7, 0x4a, 0x0d, 0xf4, 0xbe, 0xd7, 0x43, 0x91,
    0xee, 0x0b, 0x5a, 0x69, 0x94, 0x5e, 0x4c, 0xed, 0x8c, 0xa8, 0xa0, 0x95, 0x20, 0x6f, 0x00, 0xae,
];
const EMPTY_SAPLING_SPENDS_HASH: &[u8; 32] = &[
    0xd7, 0x9c, 0x8d, 0xcb, 0x8a, 0xf4, 0x6b, 0xd8, 0x0f, 0xe3, 0xbc, 0xdc, 0x0e, 0x5d, 0x0e, 0xd7,
    0x34, 0x4d, 0x5f, 0x48, 0xcb, 0xef, 0xf3, 0xc9, 0x4b, 0x13, 0xd4, 0x0a, 0x23, 0x05, 0xf8, 0x4d,
];
const EMPTY_SAPLING_OUTPUTS_HASH: &[u8; 32] = &[
    0x9e, 0x28, 0xee, 0xf8, 0xdf, 0x6f, 0xcc, 0x96, 0x68, 0xef, 0x92, 0xfc, 0xdc, 0x41, 0x2f, 0xc5,
    0xb6, 0xd7, 0x03, 0x7e, 0xf1, 0xf9, 0x63, 0x76, 0x7b, 0xd7, 0xb8, 0x01, 0x12, 0x67, 0xbf, 0x95,
];
const EMPTY_ORCHARD_V5_TXID_HASH: &[u8; 32] = &[
    0x9f, 0xbe, 0x4e, 0xd1, 0x3b, 0x0c, 0x08, 0xe6, 0x71, 0xc1, 0x1a, 0x34, 0x07, 0xd8, 0x4e, 0x11,
    0x17, 0xcd, 0x45, 0x02, 0x8a, 0x2e, 0xee, 0x1b, 0x9f, 0xea, 0xe7, 0x8b, 0x48, 0xa6, 0xe2, 0xc1,
];
const EMPTY_ORCHARD_V6_TXID_HASH: &[u8; 32] = &[
    0xa3, 0x36, 0x7d, 0x2f, 0xde, 0xa2, 0x91, 0x01, 0x59, 0xfc, 0x50, 0x26, 0xe9, 0xbf, 0x1f, 0xcc,
    0xd3, 0xe2, 0x8c, 0xe5, 0xe6, 0xde, 0x46, 0xbf, 0xb7, 0x15, 0x87, 0x23, 0x0e, 0xea, 0x95, 0x15,
];
const EMPTY_IRONWOOD_TXID_HASH: &[u8; 32] = &[
    0xb9, 0xcf, 0xe6, 0x43, 0xce, 0x45, 0xb2, 0x8c, 0x33, 0x19, 0x0f, 0x0d, 0x52, 0x23, 0xe4, 0x75,
    0x97, 0x2f, 0x2a, 0x14, 0x9d, 0xc5, 0x44, 0x04, 0xfd, 0x83, 0x65, 0x52, 0x1f, 0x84, 0x16, 0xc5,
];
const EMPTY_TRANSPARENT_AUTH_HASH: &[u8; 32] = &[
    0xe9, 0x88, 0x2b, 0xce, 0x1c, 0xf1, 0x35, 0x69, 0x02, 0xc6, 0xe2, 0x58, 0xc5, 0x67, 0xeb, 0xc0,
    0xd9, 0x92, 0x88, 0x67, 0xde, 0x9a, 0x35, 0x89, 0xbb, 0xbd, 0x31, 0x0e, 0xb6, 0x89, 0x04, 0xe5,
];
const EMPTY_SAPLING_V5_AUTH_HASH: &[u8; 32] = &[
    0xd2, 0x25, 0x67, 0x30, 0x66, 0xb0, 0xcd, 0x76, 0xa7, 0x71, 0x51, 0xbf, 0x05, 0x6d, 0x57, 0x77,
    0x92, 0xf3, 0x57, 0x73, 0x91, 0x20, 0x8d, 0x4c, 0xec, 0x25, 0x31, 0x8a, 0x8d, 0x5c, 0xd9, 0x6f,
];
const EMPTY_SAPLING_V6_AUTH_HASH: &[u8; 32] = &[
    0x0e, 0xe4, 0xb9, 0x56, 0x0b, 0xae, 0x42, 0x30, 0xa9, 0x9b, 0xfa, 0x52, 0x8e, 0x0a, 0x6a, 0xab,
    0xc7, 0xe2, 0x53, 0x86, 0xf3, 0x66, 0x59, 0x97, 0x67, 0xe7, 0xca, 0x10, 0x7b, 0x8c, 0x17, 0x46,
];
const EMPTY_ORCHARD_V5_AUTH_HASH: &[u8; 32] = &[
    0x14, 0xed, 0xaa, 0x1e, 0x66, 0x9a, 0x63, 0xa8, 0x00, 0xbf, 0xe0, 0xb8, 0xfc, 0xd3, 0xd1, 0x0e,
    0x36, 0x81, 0x11, 0x5b, 0xee, 0x03, 0x25, 0x3d, 0xa0, 0x2e, 0x09, 0x80, 0x42, 0xd9, 0xff, 0x90,
];
const EMPTY_ORCHARD_V6_AUTH_HASH: &[u8; 32] = &[
    0x79, 0x8e, 0x7f, 0xcd, 0x05, 0xbb, 0x7b, 0x7e, 0x9e, 0x49, 0x42, 0x4b, 0xdd, 0xe7, 0x68, 0xa5,
    0x00, 0xb6, 0x6f, 0xf2, 0x3d, 0x75, 0x9b, 0x87, 0x7d, 0x07, 0x54, 0xe3, 0xff, 0x79, 0x7d, 0x91,
];
const EMPTY_IRONWOOD_AUTH_HASH: &[u8; 32] = &[
    0xec, 0x97, 0x68, 0xfd, 0xaa, 0x11, 0xf1, 0x2c, 0xdb, 0x13, 0xf5, 0x66, 0xb5, 0x95, 0x84, 0x3f,
    0x3d, 0x0e, 0x92, 0xb6, 0x70, 0x3e, 0xaf, 0xff, 0x17, 0x2e, 0x21, 0x34, 0x5b, 0x58, 0x61, 0x33,
];

/// A new BLAKE2b-256 state with the given 16-byte personalization.
fn hasher(personal: &[u8; 16]) -> State {
    Params::new().hash_length(32).personal(personal).to_state()
}

/// Finalizes a ZIP-244 node digest as its fixed-width byte representation.
fn finalize_node_hash(state: State) -> [u8; 32] {
    state
        .finalize()
        .as_bytes()
        .try_into()
        .expect("ZIP-244 node hashers have 32-byte outputs")
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

    fn empty_sapling_auth_hash(self) -> &'static [u8; 32] {
        match self {
            Self::V5 => EMPTY_SAPLING_V5_AUTH_HASH,
            Self::V6 => EMPTY_SAPLING_V6_AUTH_HASH,
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

    fn empty_txid_hash(self) -> &'static [u8; 32] {
        match self {
            Self::OrchardV5 => EMPTY_ORCHARD_V5_TXID_HASH,
            Self::OrchardV6 => EMPTY_ORCHARD_V6_TXID_HASH,
            Self::IronwoodV6 => EMPTY_IRONWOOD_TXID_HASH,
        }
    }

    fn empty_auth_hash(self) -> &'static [u8; 32] {
        match self {
            Self::OrchardV5 => EMPTY_ORCHARD_V5_AUTH_HASH,
            Self::OrchardV6 => EMPTY_ORCHARD_V6_AUTH_HASH,
            Self::IronwoodV6 => EMPTY_IRONWOOD_AUTH_HASH,
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
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L170>
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
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L85>
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
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L71>
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
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L85>
fn hash_outputs(outputs: &[transparent::Output]) -> Blake2bHash {
    let mut h = hasher(ZCASH_OUTPUTS_HASH_PERSONALIZATION);
    for output in outputs {
        update_serialized(&mut h, output);
    }
    h.finalize()
}

/// ZIP-244 §S.2b amounts digest.
fn hash_amounts(previous_outputs: &[transparent::Output]) -> Blake2bHash {
    let mut h = hasher(ZCASH_TRANSPARENT_AMOUNTS_HASH_PERSONALIZATION);
    for output in previous_outputs {
        h.update(&output.value.zatoshis().to_le_bytes());
    }
    h.finalize()
}

/// ZIP-244 §S.2c scriptPubKeys digest.
fn hash_scriptpubkeys(previous_outputs: &[transparent::Output]) -> Blake2bHash {
    let mut h = hasher(ZCASH_TRANSPARENT_SCRIPTPUBKEYS_HASH_PERSONALIZATION);
    for output in previous_outputs {
        update_serialized(&mut h, &output.lock_script);
    }
    h.finalize()
}

/// ZIP-244 §S.2g digest for one transparent input.
fn hash_txin(
    input: &transparent::Input,
    previous_output: &transparent::Output,
) -> Option<Blake2bHash> {
    let transparent::Input::PrevOut {
        outpoint, sequence, ..
    } = input
    else {
        return None;
    };

    let mut h = hasher(ZCASH_TRANSPARENT_INPUT_HASH_PERSONALIZATION);
    update_serialized(&mut h, outpoint);
    h.update(&previous_output.value.zatoshis().to_le_bytes());
    update_serialized(&mut h, &previous_output.lock_script);
    h.update(&sequence.to_le_bytes());
    Some(h.finalize())
}

fn hash_transparent_txid_from_digests(
    bundle_present: bool,
    prevouts: &Blake2bHash,
    sequence: &Blake2bHash,
    outputs: &Blake2bHash,
) -> [u8; 32] {
    if !bundle_present {
        return *EMPTY_TRANSPARENT_TXID_HASH;
    }

    let mut h = hasher(ZCASH_TRANSPARENT_HASH_PERSONALIZATION);
    h.update(prevouts.as_bytes());
    h.update(sequence.as_bytes());
    h.update(outputs.as_bytes());
    finalize_node_hash(h)
}

/// ZIP-244 §T.2 transparent digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L196>
fn hash_transparent_txid(
    inputs: &[transparent::Input],
    outputs: &[transparent::Output],
) -> [u8; 32] {
    if inputs.is_empty() && outputs.is_empty() {
        return *EMPTY_TRANSPARENT_TXID_HASH;
    }

    hash_transparent_txid_from_digests(
        true,
        &hash_prevouts(inputs),
        &hash_sequence(inputs),
        &hash_outputs(outputs),
    )
}

/// ZIP-244 §T.3a sapling spends digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L100>
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
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L132>
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
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L209>
fn hash_sapling_txid(
    sapling: Option<&sapling::ShieldedData<sapling::SharedAnchor>>,
    version: Zip244Version,
) -> [u8; 32] {
    let Some(sapling) = sapling else {
        return *EMPTY_SAPLING_TXID_HASH;
    };

    let mut h = hasher(ZCASH_SAPLING_HASH_PERSONALIZATION);
    // `ShieldedData` only exists with at least one spend or output, so this
    // matches librustzcash's "non-empty bundle" branch.
    if sapling.spends().next().is_some() {
        h.update(hash_sapling_spends(sapling, version).as_bytes());
    } else {
        h.update(EMPTY_SAPLING_SPENDS_HASH);
    }
    if sapling.outputs().next().is_some() {
        h.update(hash_sapling_outputs(sapling).as_bytes());
    } else {
        h.update(EMPTY_SAPLING_OUTPUTS_HASH);
    }
    h.update(&sapling.value_balance.zatoshis().to_le_bytes());
    finalize_node_hash(h)
}

/// ZIP-244 §T.4 Orchard-style bundle digest.
///
/// Mirrors `orchard::bundle::commitments::hash_bundle_txid_data`.
fn hash_bundle_txid(
    bundle: Option<&orchard::ShieldedData>,
    format: BundleCommitmentFormat,
) -> [u8; 32] {
    let Some(bundle) = bundle else {
        return *format.empty_txid_hash();
    };

    let personalizations = format.personalizations();
    let mut h = hasher(personalizations.bundle);
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
    finalize_node_hash(h)
}

/// Combine the level-1 digests into the txid (ZIP-244 txid digest).
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L426>
fn combine_txid_digests(
    consensus_branch_id: u32,
    header: &Blake2bHash,
    transparent: &[u8; 32],
    sapling: &[u8; 32],
    orchard: &[u8; 32],
    ironwood: Option<&[u8; 32]>,
) -> Blake2bHash {
    let mut personal = [0u8; 16];
    personal[..12].copy_from_slice(ZCASH_TX_PERSONALIZATION_PREFIX);
    personal[12..].copy_from_slice(&consensus_branch_id.to_le_bytes());

    // Commit the level-1 nodes in their ZIP-244 order.
    let mut h = hasher(&personal);
    h.update(header.as_bytes());
    h.update(transparent);
    h.update(sapling);
    h.update(orchard);
    if let Some(ironwood) = ironwood {
        h.update(ironwood);
    }

    h.finalize()
}

fn txid_inner(parts: &Zip244Parts) -> Hash {
    // Compute the level-1 nodes, substituting personalized empty hashes
    // for absent bundles.
    let header = hash_header(parts);
    let transparent = hash_transparent_txid(parts.inputs, parts.outputs);
    let sapling = hash_sapling_txid(parts.sapling, parts.version);
    let orchard = hash_bundle_txid(parts.orchard, parts.version.orchard_format());
    let ironwood = parts
        .version
        .has_ironwood()
        .then(|| hash_bundle_txid(parts.ironwood, BundleCommitmentFormat::IronwoodV6));

    Hash(
        combine_txid_digests(
            consensus_branch_id(parts),
            &header,
            &transparent,
            &sapling,
            &orchard,
            ironwood.as_ref(),
        )
        .as_bytes()
        .try_into()
        .expect("BLAKE2b-256 digest is 32 bytes"),
    )
}

// --- auth digest (ZIP-244 authorizing-data commitment) ------------------------

/// ZIP-244 transparent script-sig digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L379-L390>
fn hash_transparent_auth(
    inputs: &[transparent::Input],
    outputs: &[transparent::Output],
) -> [u8; 32] {
    if inputs.is_empty() && outputs.is_empty() {
        return *EMPTY_TRANSPARENT_AUTH_HASH;
    }

    let mut h = hasher(ZCASH_TRANSPARENT_SCRIPTS_HASH_PERSONALIZATION);
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
    finalize_node_hash(h)
}

/// ZIP-244 sapling auth digest.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L392>
fn hash_sapling_auth(
    sapling: Option<&sapling::ShieldedData<sapling::SharedAnchor>>,
    version: Zip244Version,
) -> [u8; 32] {
    let Some(sapling) = sapling else {
        return *version.empty_sapling_auth_hash();
    };

    let mut h = hasher(version.sapling_auth_personalization());
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
    finalize_node_hash(h)
}

/// ZIP-244 Orchard-style auth digest.
///
/// Mirrors `orchard::bundle::commitments::hash_bundle_auth_data`.
fn hash_bundle_auth(
    bundle: Option<&orchard::ShieldedData>,
    format: BundleCommitmentFormat,
) -> [u8; 32] {
    let Some(bundle) = bundle else {
        return *format.empty_auth_hash();
    };

    let mut h = hasher(format.personalizations().auth);
    h.update(&bundle.proof.0[..]);
    for action in bundle.actions.iter() {
        update_serialized(&mut h, &action.spend_auth_sig);
    }
    update_serialized(&mut h, &bundle.binding_sig);
    if format.includes_anchor_in_authorizing_digest() {
        h.update(&<[u8; 32]>::from(bundle.shared_anchor));
    }
    finalize_node_hash(h)
}

/// Combine the authorizing-data digests into the ZIP-244 auth commitment.
///
/// Reference implementation:
/// <https://github.com/zcash/librustzcash/blob/4367ba26ed57624544e2350f055a5df89079474a/zcash_primitives/src/transaction/txid.rs#L426-L448>
fn auth_digest_inner(parts: &Zip244Parts) -> AuthDigest {
    // Compute the level-1 nodes, substituting personalized empty hashes
    // for absent bundles.
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

    // Commit the level-1 nodes in their ZIP-244 order.
    let mut h = hasher(&personal);
    h.update(&transparent);
    h.update(&sapling);
    h.update(&orchard);
    if let Some(ironwood) = ironwood {
        h.update(&ironwood);
    }

    AuthDigest(
        h.finalize()
            .as_bytes()
            .try_into()
            .expect("BLAKE2b-256 digest is 32 bytes"),
    )
}

// --- signature digest (ZIP-244 §S) -----------------------------------------

/// Owned v5/v6 ZIP-244 data reused by signature hash calculations.
#[derive(Clone, Debug)]
pub(super) struct Zip244SighashCache {
    consensus_branch_id: u32,
    header: Blake2bHash,
    transparent_txid: [u8; 32],
    prevouts: Blake2bHash,
    amounts: Blake2bHash,
    scriptpubkeys: Blake2bHash,
    sequence: Blake2bHash,
    outputs: Blake2bHash,
    single_outputs: Vec<Blake2bHash>,
    txins: Vec<Option<Blake2bHash>>,
    transparent_bundle_present: bool,
    transparent_is_coinbase_or_has_no_inputs: bool,
    sapling: [u8; 32],
    orchard: [u8; 32],
    ironwood: Option<[u8; 32]>,
}

impl Zip244SighashCache {
    /// Precomputes v5/v6 transaction and transparent input digests.
    ///
    /// `previous_outputs` must contain the spent output corresponding to each
    /// transparent input. Coinbase transactions may pass an empty slice.
    pub(super) fn new(tx: &Transaction, previous_outputs: &[transparent::Output]) -> Option<Self> {
        let parts = zip244_parts(tx)?;
        let prevouts = hash_prevouts(parts.inputs);
        let sequence = hash_sequence(parts.inputs);
        let outputs = hash_outputs(parts.outputs);
        let transparent_bundle_present = !parts.inputs.is_empty() || !parts.outputs.is_empty();

        Some(Self {
            consensus_branch_id: consensus_branch_id(&parts),
            header: hash_header(&parts),
            transparent_txid: hash_transparent_txid_from_digests(
                transparent_bundle_present,
                &prevouts,
                &sequence,
                &outputs,
            ),
            prevouts,
            amounts: hash_amounts(previous_outputs),
            scriptpubkeys: hash_scriptpubkeys(previous_outputs),
            sequence,
            outputs,
            single_outputs: parts
                .outputs
                .iter()
                .take(parts.inputs.len())
                .map(|output| hash_outputs(std::slice::from_ref(output)))
                .collect(),
            txins: parts
                .inputs
                .iter()
                .enumerate()
                .map(|(index, input)| {
                    previous_outputs
                        .get(index)
                        .and_then(|previous_output| hash_txin(input, previous_output))
                })
                .collect(),
            transparent_bundle_present,
            transparent_is_coinbase_or_has_no_inputs: tx.is_coinbase() || parts.inputs.is_empty(),
            sapling: hash_sapling_txid(parts.sapling, parts.version),
            orchard: hash_bundle_txid(parts.orchard, parts.version.orchard_format()),
            ironwood: parts
                .version
                .has_ironwood()
                .then(|| hash_bundle_txid(parts.ironwood, BundleCommitmentFormat::IronwoodV6)),
        })
    }

    /// Computes a ZIP-244 signature hash from the cached transaction data.
    ///
    /// `input_index` is `Some` for a transparent signature and `None` for a
    /// shielded signature. ZIP-244 commits to the spent output's scriptPubKey,
    /// so the script code supplied to the interpreter is not an input here.
    pub(super) fn sighash(
        &self,
        hash_type: CanonicalHashType,
        input_index: Option<usize>,
    ) -> SigHash {
        let transparent = self.transparent_sig_digest(hash_type, input_index);
        SigHash(
            combine_txid_digests(
                self.consensus_branch_id,
                &self.header,
                &transparent,
                &self.sapling,
                &self.orchard,
                self.ironwood.as_ref(),
            )
            .as_bytes()
            .try_into()
            .expect("BLAKE2b-256 digest is 32 bytes"),
        )
    }

    fn transparent_sig_digest(
        &self,
        hash_type: CanonicalHashType,
        input_index: Option<usize>,
    ) -> [u8; 32] {
        if !self.transparent_bundle_present || self.transparent_is_coinbase_or_has_no_inputs {
            return self.transparent_txid;
        }

        // Shielded signatures always use SIGHASH_ALL in ZIP-244, regardless of
        // the caller's otherwise-unused hash type argument.
        let hash_type = input_index.map_or(CanonicalHashType::All, |_| hash_type);
        let anyone_can_pay = hash_type.anyone_can_pay();
        let single = hash_type.is_single();
        let none = hash_type.is_none();

        let prevouts = if anyone_can_pay {
            hash_prevouts(&[])
        } else {
            self.prevouts
        };
        let amounts = if anyone_can_pay {
            hash_amounts(&[])
        } else {
            self.amounts
        };
        let scriptpubkeys = if anyone_can_pay {
            hash_scriptpubkeys(&[])
        } else {
            self.scriptpubkeys
        };
        let sequence = if anyone_can_pay {
            hash_sequence(&[])
        } else {
            self.sequence
        };
        let outputs = match input_index {
            Some(index) if single => self
                .single_outputs
                .get(index)
                .copied()
                .unwrap_or_else(|| hash_outputs(&[])),
            Some(_) if none => hash_outputs(&[]),
            _ => self.outputs,
        };
        let txin = match input_index {
            Some(index) => self
                .txins
                .get(index)
                .and_then(Option::as_ref)
                .copied()
                .expect("transparent sighash input has a matching previous output"),
            None => hasher(ZCASH_TRANSPARENT_INPUT_HASH_PERSONALIZATION).finalize(),
        };

        let hash_type = hash_type.encode();
        let mut h = hasher(ZCASH_TRANSPARENT_HASH_PERSONALIZATION);
        h.update(&[hash_type]);
        h.update(prevouts.as_bytes());
        h.update(amounts.as_bytes());
        h.update(scriptpubkeys.as_bytes());
        h.update(sequence.as_bytes());
        h.update(outputs.as_bytes());
        h.update(txin.as_bytes());
        finalize_node_hash(h)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_hash_constants_match_personalizations() {
        let empty_hashes = [
            (
                ZCASH_TRANSPARENT_HASH_PERSONALIZATION,
                EMPTY_TRANSPARENT_TXID_HASH,
            ),
            (ZCASH_SAPLING_HASH_PERSONALIZATION, EMPTY_SAPLING_TXID_HASH),
            (
                ZCASH_SAPLING_SPENDS_HASH_PERSONALIZATION,
                EMPTY_SAPLING_SPENDS_HASH,
            ),
            (
                ZCASH_SAPLING_OUTPUTS_HASH_PERSONALIZATION,
                EMPTY_SAPLING_OUTPUTS_HASH,
            ),
            (
                ZCASH_ORCHARD_HASH_PERSONALIZATION,
                EMPTY_ORCHARD_V5_TXID_HASH,
            ),
            (
                ZCASH_ORCHARD_V6_HASH_PERSONALIZATION,
                EMPTY_ORCHARD_V6_TXID_HASH,
            ),
            (
                ZCASH_IRONWOOD_HASH_PERSONALIZATION,
                EMPTY_IRONWOOD_TXID_HASH,
            ),
            (
                ZCASH_TRANSPARENT_SCRIPTS_HASH_PERSONALIZATION,
                EMPTY_TRANSPARENT_AUTH_HASH,
            ),
            (
                ZCASH_SAPLING_SIGS_HASH_PERSONALIZATION,
                EMPTY_SAPLING_V5_AUTH_HASH,
            ),
            (
                ZCASH_SAPLING_V6_SIGS_HASH_PERSONALIZATION,
                EMPTY_SAPLING_V6_AUTH_HASH,
            ),
            (
                ZCASH_ORCHARD_SIGS_HASH_PERSONALIZATION,
                EMPTY_ORCHARD_V5_AUTH_HASH,
            ),
            (
                ZCASH_ORCHARD_V6_SIGS_HASH_PERSONALIZATION,
                EMPTY_ORCHARD_V6_AUTH_HASH,
            ),
            (
                ZCASH_IRONWOOD_SIGS_HASH_PERSONALIZATION,
                EMPTY_IRONWOOD_AUTH_HASH,
            ),
        ];

        for (personalization, expected_hash) in empty_hashes {
            assert_eq!(
                hasher(personalization).finalize().as_bytes(),
                expected_hash,
                "empty hash must match {personalization:?}",
            );
        }
    }
}
