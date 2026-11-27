//! The v7 transaction identifier and authorizing data commitment of [ZIP 248].
//!
//! Each bundle in `mEffectBundles` and `mAuthBundles` contributes a digest tagged with
//! its type and variant; absent bundles contribute nothing. The bundle digests reuse the
//! ZIP 244 and ZIP 229 hashing in [`super::super::zip244`] wherever ZIP 248 commits to
//! the same data, and the v7 roots are new.
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use std::io;

use blake2b_simd::State;

use crate::{
    orchard,
    serialization::ZcashSerialize,
    transaction::serialize::header_field,
    transaction::zip244::{
        self, finalize_node_hash, hasher, BundleCommitmentFormat, HashWriter, Zip244Version,
        ZCASH_AUTH_PERSONALIZATION_PREFIX, ZCASH_HEADERS_HASH_PERSONALIZATION,
        ZCASH_TRANSPARENT_HASH_PERSONALIZATION, ZCASH_TRANSPARENT_SCRIPTS_HASH_PERSONALIZATION,
        ZCASH_TX_PERSONALIZATION_PREFIX,
    },
};

use super::{
    codec::{self, Pool, V7_VERSION_GROUP_ID},
    registry::{self, BundleId},
    V7Ref,
};

/// T.2 `value_pool_deltas_digest`.
const VALUE_POOL_DELTAS_PERSONALIZATION: &[u8; 16] = b"ZTxIdVPDeltaHash";
/// T.3 `effects_bundles_digest`.
const EFFECTS_BUNDLES_PERSONALIZATION: &[u8; 16] = b"ZTxIdEffBnd_Hash";
/// T.3.1 `coinbase_effects_digest`.
const COINBASE_PERSONALIZATION: &[u8; 16] = b"ZTxIdCoinbasHash";
/// T.3.2 `sapling_effects_digest`.
const SAPLING_EFFECTS_PERSONALIZATION: &[u8; 16] = b"ZTxIdSaplingH_v7";
/// T.3.3 `orchard_effects_digest`.
const ORCHARD_EFFECTS_PERSONALIZATION: &[u8; 16] = b"ZTxIdOrchardH_v7";
/// T.3.4 `ironwood_effects_digest`.
const IRONWOOD_EFFECTS_PERSONALIZATION: &[u8; 16] = b"ZTxIdIronwd_Hash";
/// A.1 `auth_bundles_digest`.
const AUTH_BUNDLES_PERSONALIZATION: &[u8; 16] = b"ZTxAuthBnd__Hash";
/// A.1.2 `sapling_auth_digest`.
const SAPLING_AUTH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthSapliH_v7";
/// A.1.3 `orchard_auth_digest`.
const ORCHARD_AUTH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthOrchaH_v7";
/// A.1.4 `ironwood_auth_digest`.
const IRONWOOD_AUTH_PERSONALIZATION: &[u8; 16] = b"ZTxAuthIrnwdHash";

/// The `header` field of every v7 transaction.
const V7_HEADER: u32 = header_field(true, zcash_protocol::constants::V7_TX_VERSION);

/// Hashes the encoding that `write` produces.
fn hash_encoding(
    personalization: &[u8; 16],
    write: impl FnOnce(&mut HashWriter<'_>) -> io::Result<()>,
) -> [u8; 32] {
    let mut h = hasher(personalization);
    write(&mut HashWriter(&mut h)).expect(
        "Transaction::V7 holds only PrevOut inputs, as the decoder produces, \
         and writing to a hash cannot fail",
    );
    finalize_node_hash(h)
}

/// Hashes a bundle's type and variant in their encoding.
pub(super) fn write_bundle_id(state: &mut State, bundle: BundleId) {
    codec::write_bundle_id(&mut HashWriter(state), bundle)
        .expect("registered bundle identifiers fit in a compactSize, and hashing cannot fail");
}

fn branch_personalization(prefix: &[u8; 12], consensus_branch_id: u32) -> [u8; 16] {
    let mut personalization = [0; 16];
    personalization[..12].copy_from_slice(prefix);
    personalization[12..].copy_from_slice(&consensus_branch_id.to_le_bytes());
    personalization
}

impl V7Ref<'_> {
    fn consensus_branch_id(&self) -> u32 {
        u32::from(
            self.network_upgrade
                .branch_id()
                .expect("Transaction::V7 requires NU7.1 or later, which has a consensus branch ID"),
        )
    }

    /// T.1 `header_digest`.
    fn header_digest(&self) -> [u8; 32] {
        hash_encoding(ZCASH_HEADERS_HASH_PERSONALIZATION, |w| {
            use byteorder::{LittleEndian, WriteBytesExt};
            w.write_u32::<LittleEndian>(V7_HEADER)?;
            w.write_u32::<LittleEndian>(V7_VERSION_GROUP_ID)?;
            w.write_u32::<LittleEndian>(self.consensus_branch_id())?;
            self.lock_time.zcash_serialize(&mut *w)?;
            w.write_u32::<LittleEndian>(self.expiry_height.0)
        })
    }

    /// T.2 `value_pool_deltas_digest`.
    fn value_pool_deltas_digest(&self) -> [u8; 32] {
        let mut h = hasher(VALUE_POOL_DELTAS_PERSONALIZATION);
        for (bundle, value) in self.pool_deltas().nonzero() {
            write_bundle_id(&mut h, bundle);
            h.update(&value.zatoshis().to_le_bytes());
        }
        finalize_node_hash(h)
    }

    /// The `bundle_effects_digest` of each bundle in `mEffectBundles`, in encoding order.
    fn effects_digests(&self) -> Vec<(BundleId, [u8; 32])> {
        let mut digests = Vec::new();
        if self.has_transparent_effects() {
            // T.3.0, which is ZIP 244's transparent digest of a present bundle.
            let mut h = hasher(ZCASH_TRANSPARENT_HASH_PERSONALIZATION);
            h.update(zip244::hash_prevouts(self.inputs).as_bytes());
            h.update(zip244::hash_sequence(self.inputs).as_bytes());
            h.update(zip244::hash_outputs(self.outputs).as_bytes());
            digests.push((registry::TRANSPARENT, finalize_node_hash(h)));
        }
        if let Some(coinbase) = &self.fields.coinbase {
            // T.3.1 hashes the fields in their encoding.
            digests.push((
                registry::COINBASE,
                hash_encoding(COINBASE_PERSONALIZATION, |w| {
                    codec::write_coinbase(coinbase, w)
                }),
            ));
        }
        if let Some(sapling) = self.sapling {
            // T.3.2: the ZIP 229 spends and outputs digests, without the value balance.
            let mut h = hasher(SAPLING_EFFECTS_PERSONALIZATION);
            h.update(zip244::hash_sapling_spends(sapling, Zip244Version::V6).as_bytes());
            h.update(zip244::hash_sapling_outputs(sapling).as_bytes());
            digests.push((registry::SAPLING, finalize_node_hash(h)));
        }
        for (bundle, data, pool) in [
            (registry::ORCHARD, self.orchard, Pool::Orchard),
            (registry::IRONWOOD, self.ironwood, Pool::Ironwood),
        ] {
            if let Some(data) = data {
                digests.push((bundle, orchard_effects_digest(data, pool)));
            }
        }
        digests
    }

    /// The `bundle_auth_digest` of each bundle in `mAuthBundles`, in encoding order.
    fn auth_digests(&self) -> Vec<(BundleId, [u8; 32])> {
        let mut digests = Vec::new();
        if !self.inputs.is_empty() {
            // A.1.0 hashes each input's sighashInfo and scriptSig, as they are encoded.
            digests.push((
                registry::TRANSPARENT,
                hash_encoding(ZCASH_TRANSPARENT_SCRIPTS_HASH_PERSONALIZATION, |w| {
                    self.write_transparent_auth(w)
                }),
            ));
        }
        if let Some(sapling) = self.sapling {
            // A.1.2 hashes the authorizing data as it is encoded.
            digests.push((
                registry::SAPLING,
                hash_encoding(SAPLING_AUTH_PERSONALIZATION, |w| {
                    codec::write_sapling_auth(sapling, w)
                }),
            ));
        }
        for (bundle, data, personalization) in [
            (
                registry::ORCHARD,
                self.orchard,
                ORCHARD_AUTH_PERSONALIZATION,
            ),
            (
                registry::IRONWOOD,
                self.ironwood,
                IRONWOOD_AUTH_PERSONALIZATION,
            ),
        ] {
            if let Some(data) = data {
                digests.push((bundle, orchard_auth_digest(data, personalization)));
            }
        }
        digests
    }

    /// `txid_digest`: the transaction identifier.
    pub(super) fn txid_digest(&self) -> [u8; 32] {
        let mut effects = hasher(EFFECTS_BUNDLES_PERSONALIZATION);
        for (bundle, digest) in self.effects_digests() {
            write_bundle_id(&mut effects, bundle);
            effects.update(&digest);
        }

        let mut h = hasher(&branch_personalization(
            ZCASH_TX_PERSONALIZATION_PREFIX,
            self.consensus_branch_id(),
        ));
        h.update(&self.header_digest());
        h.update(&self.value_pool_deltas_digest());
        h.update(&finalize_node_hash(effects));
        finalize_node_hash(h)
    }

    /// `auth_digest`: the authorizing data commitment.
    pub(super) fn auth_digest(&self) -> [u8; 32] {
        let mut auths = hasher(AUTH_BUNDLES_PERSONALIZATION);
        for (bundle, digest) in self.auth_digests() {
            write_bundle_id(&mut auths, bundle);
            auths.update(&digest);
        }

        let mut h = hasher(&branch_personalization(
            ZCASH_AUTH_PERSONALIZATION_PREFIX,
            self.consensus_branch_id(),
        ));
        h.update(&finalize_node_hash(auths));
        finalize_node_hash(h)
    }
}

/// T.3.3 and T.3.4: the ZIP 229 action digests of the bundle's pool, and its flags.
fn orchard_effects_digest(bundle: &orchard::ShieldedData, pool: Pool) -> [u8; 32] {
    // ZIP 248 keeps each pool's v6 action-level personalizations.
    let (personalization, format) = match pool {
        Pool::Orchard => (
            ORCHARD_EFFECTS_PERSONALIZATION,
            BundleCommitmentFormat::OrchardV6,
        ),
        Pool::Ironwood => (
            IRONWOOD_EFFECTS_PERSONALIZATION,
            BundleCommitmentFormat::IronwoodV6,
        ),
    };
    let mut h = hasher(personalization);
    for digest in zip244::hash_bundle_actions(bundle, format) {
        h.update(digest.as_bytes());
    }
    h.update(&[bundle.flags.bits()]);
    finalize_node_hash(h)
}

/// A.1.3 and A.1.4: the anchor, the proof bytes without their length prefix (as in
/// ZIP 244), and each signature in its field encoding, which includes `sighashInfo`.
fn orchard_auth_digest(bundle: &orchard::ShieldedData, personalization: &[u8; 16]) -> [u8; 32] {
    let mut h = hasher(personalization);
    h.update(&<[u8; 32]>::from(bundle.shared_anchor));
    h.update(&bundle.proof.0);
    for action in bundle.actions.iter() {
        h.update(&codec::SIGHASH_INFO_V0);
        h.update(&<[u8; 64]>::from(action.spend_auth_sig));
    }
    h.update(&codec::SIGHASH_INFO_V0);
    h.update(&<[u8; 64]>::from(bundle.binding_sig));
    finalize_node_hash(h)
}
