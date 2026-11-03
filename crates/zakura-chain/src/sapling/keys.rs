//! Sapling key types.
//!
//! Unused key types are not implemented, see PR #5476.
//!
//! "The spend authorizing key ask, proof authorizing key (ak, nsk),
//! full viewing key (ak, nk, ovk), incoming viewing key ivk, and each
//! diversified payment address addr_d = (d, pk_d ) are derived from sk,
//! as described in [Sapling Key Components][ps]." - [§3.1][3.1]
//!
//! [ps]: https://zips.z.cash/protocol/protocol.pdf#saplingkeycomponents
//! [3.1]: https://zips.z.cash/protocol/protocol.pdf#addressesandkeys

use crate::serialization::ZcashReader;
use std::{fmt, io};

use crate::{
    primitives::redjubjub::SpendAuth,
    serialization::{ReadZcashExt, SerializationError, ZcashDeserialize, ZcashSerialize},
};

/// An [ephemeral public key][1] (`epk`) for Sapling key agreement, stored as its
/// 32-byte encoding.
///
/// The key is a Jubjub point, but nodes only need its bytes for the txid digest
/// and serialization. The point itself is only used for wallet trial decryption,
/// so Zebra keeps the raw bytes and skips decompression during deserialization.
///
/// # Consensus
///
/// Deserialization only checks the byte length; the semantic verifier and
/// mempool must check that it is a canonical, non-small-order point. They do so
/// by calling
/// [`EphemeralPublicKey::is_valid_not_small_order`] (via
/// [`Transaction::sapling_point_encodings_are_valid`]). They also verify the
/// Sapling bundle through librustzcash, whose `check_output` rejects a
/// small-order `epk`.
///
/// Its serialized form is [KA^{Sapling}.Public][2].
///
/// [1]: https://zips.z.cash/protocol/protocol.pdf#outputdesc
/// [2]: https://zips.z.cash/protocol/protocol.pdf#concretesaplingkeyagreement
/// [`Transaction::sapling_point_encodings_are_valid`]: crate::transaction::Transaction::sapling_point_encodings_are_valid
#[derive(Copy, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub struct EphemeralPublicKey(pub(crate) [u8; 32]);

impl EphemeralPublicKey {
    /// Returns true if the stored encoding is a canonical, non-small-order
    /// Jubjub point, i.e. a valid ephemeral public key per the consensus rules.
    ///
    /// This is the not-small-order check deferred from deserialization, run by
    /// the semantic verifier (not the checkpoint verifier) on untrusted
    /// transactions.
    ///
    /// To stay in consensus with the rest of the network, this must accept
    /// exactly the `epk` encodings librustzcash accepts. librustzcash decodes as
    /// an `ExtendedPoint` and rejects small-order points in `check_output`;
    /// decoding as an `AffinePoint` here is equivalent — both reject the same
    /// off-curve/non-canonical encodings and agree on `is_small_order`.
    /// Equivalence is pinned by
    /// `sapling_point_checks_match_librustzcash_predicates`.
    pub fn is_valid_not_small_order(&self) -> bool {
        match jubjub::AffinePoint::from_bytes(self.0).into_option() {
            Some(point) => !bool::from(point.is_small_order()),
            None => false,
        }
    }
}

impl fmt::Debug for EphemeralPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EphemeralPublicKey")
            .field("epk", &hex::encode(self.0))
            .finish()
    }
}

impl From<EphemeralPublicKey> for [u8; 32] {
    fn from(nk: EphemeralPublicKey) -> [u8; 32] {
        nk.0
    }
}

impl From<&EphemeralPublicKey> for [u8; 32] {
    fn from(nk: &EphemeralPublicKey) -> [u8; 32] {
        nk.0
    }
}

impl PartialEq<[u8; 32]> for EphemeralPublicKey {
    fn eq(&self, other: &[u8; 32]) -> bool {
        &self.0 == other
    }
}

impl TryFrom<[u8; 32]> for EphemeralPublicKey {
    type Error = &'static str;

    /// Store an `EphemeralPublicKey` from a byte array, deferring point
    /// decompression and the not-small-order check (see the type docs).
    ///
    /// This constructor is fallible only to match older call sites and trait
    /// bounds; any 32-byte array is stored successfully.
    fn try_from(bytes: [u8; 32]) -> Result<Self, Self::Error> {
        Ok(Self(bytes))
    }
}

impl ZcashSerialize for EphemeralPublicKey {
    fn zcash_serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_all(&<[u8; 32]>::from(self)[..])?;
        Ok(())
    }
}

impl ZcashDeserialize for EphemeralPublicKey {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        Self::try_from(reader.read_32_bytes()?).map_err(SerializationError::Parse)
    }
}

/// A randomized [validating key][1] that should be used to validate `spendAuthSig`.
///
/// It is denoted by `rk` in the specification. (This type does _not_
/// represent [SpendAuthSig^{Sapling}.Public][2], which allows any points, including
/// of small order).
///
/// [1]: https://zips.z.cash/protocol/protocol.pdf#spenddesc
/// [2]: https://zips.z.cash/protocol/protocol.pdf#concretereddsa
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ValidatingKey(redjubjub::VerificationKey<SpendAuth>);

impl From<ValidatingKey> for redjubjub::VerificationKey<SpendAuth> {
    fn from(rk: ValidatingKey) -> Self {
        rk.0
    }
}

impl TryFrom<redjubjub::VerificationKey<SpendAuth>> for ValidatingKey {
    type Error = &'static str;

    /// Convert an array into a ValidatingKey.
    ///
    /// Returns an error if the key is malformed or [is of small order][1].
    ///
    /// # Consensus
    ///
    /// > Check that a Spend description's cv and rk are not of small order,
    /// > i.e. \[h_J\]cv MUST NOT be 𝒪_J and \[h_J\]rk MUST NOT be 𝒪_J.
    ///
    /// [1]: https://zips.z.cash/protocol/protocol.pdf#spenddesc
    fn try_from(key: redjubjub::VerificationKey<SpendAuth>) -> Result<Self, Self::Error> {
        if bool::from(
            jubjub::AffinePoint::from_bytes(key.into())
                .unwrap()
                .is_small_order(),
        ) {
            Err("jubjub::AffinePoint value for Sapling ValidatingKey is of small order")
        } else {
            Ok(Self(key))
        }
    }
}

impl TryFrom<[u8; 32]> for ValidatingKey {
    type Error = &'static str;

    fn try_from(value: [u8; 32]) -> Result<Self, Self::Error> {
        let vk = redjubjub::VerificationKey::<SpendAuth>::try_from(value)
            .map_err(|_| "Invalid redjubjub::ValidatingKey for Sapling ValidatingKey")?;
        vk.try_into()
    }
}

impl From<ValidatingKey> for [u8; 32] {
    fn from(key: ValidatingKey) -> Self {
        key.0.into()
    }
}

impl From<ValidatingKey> for redjubjub::VerificationKeyBytes<SpendAuth> {
    fn from(key: ValidatingKey) -> Self {
        key.0.into()
    }
}
