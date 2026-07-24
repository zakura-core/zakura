//! Orchard key types.
//!
//! Unused key types are not implemented, see PR #5476.
//!
//! <https://zips.z.cash/protocol/nu5.pdf#orchardkeycomponents>

use std::{fmt, io};

use group::{ff::PrimeField, prime::PrimeCurveAffine, Group, GroupEncoding};
use halo2::{
    arithmetic::{Coordinates, CurveAffine},
    pasta::pallas,
};
use rand_core::{CryptoRng, RngCore};

use crate::{
    error::RandError,
    serialization::{ReadZcashExt, SerializationError, ZcashDeserialize, ZcashSerialize},
};

use super::sinsemilla::*;

/// Used to derive a diversified base point from a diversifier value.
///
/// DiversifyHash^Orchard(d) := {︃ GroupHash^P("z.cash:Orchard-gd",""), if P = 0_P
///                               P,                                   otherwise
///
/// where P = GroupHash^P(("z.cash:Orchard-gd", LEBS2OSP_l_d(d)))
///
/// <https://zips.z.cash/protocol/nu5.pdf#concretediversifyhash>
fn diversify_hash(d: &[u8]) -> pallas::Point {
    let p = pallas_group_hash(b"z.cash:Orchard-gd", d);

    if <bool>::from(p.is_identity()) {
        pallas_group_hash(b"z.cash:Orchard-gd", b"")
    } else {
        p
    }
}

/// A _diversifier_, as described in [protocol specification §4.2.3][ps].
///
/// [ps]: https://zips.z.cash/protocol/nu5.pdf#orchardkeycomponents
#[derive(Copy, Clone, Eq, PartialEq)]
#[cfg_attr(
    any(test, feature = "proptest-impl"),
    derive(proptest_derive::Arbitrary)
)]
pub struct Diversifier(pub(crate) [u8; 11]);

impl fmt::Debug for Diversifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Diversifier")
            .field(&hex::encode(self.0))
            .finish()
    }
}

impl From<[u8; 11]> for Diversifier {
    fn from(bytes: [u8; 11]) -> Self {
        Self(bytes)
    }
}

impl From<Diversifier> for [u8; 11] {
    fn from(d: Diversifier) -> [u8; 11] {
        d.0
    }
}

impl From<Diversifier> for pallas::Point {
    /// Derive a _diversified base_ point.
    ///
    /// g_d := DiversifyHash^Orchard(d)
    ///
    /// [orchardkeycomponents]: https://zips.z.cash/protocol/nu5.pdf#orchardkeycomponents
    fn from(d: Diversifier) -> Self {
        diversify_hash(&d.0)
    }
}

impl PartialEq<[u8; 11]> for Diversifier {
    fn eq(&self, other: &[u8; 11]) -> bool {
        self.0 == *other
    }
}

impl From<Diversifier> for pallas::Affine {
    /// Get a diversified base point from a diversifier value in affine
    /// representation.
    fn from(d: Diversifier) -> Self {
        let projective_point = pallas::Point::from(d);
        projective_point.into()
    }
}

impl Diversifier {
    /// Generate a new `Diversifier`.
    ///
    /// <https://zips.z.cash/protocol/nu5.pdf#orchardkeycomponents>
    pub fn new<T>(csprng: &mut T) -> Result<Self, RandError>
    where
        T: RngCore + CryptoRng,
    {
        let mut bytes = [0u8; 11];
        csprng
            .try_fill_bytes(&mut bytes)
            .map_err(|_| RandError::FillBytes)?;

        Ok(Self::from(bytes))
    }
}

/// A (diversified) transmission Key
///
/// In Orchard, secrets need to be transmitted to a recipient of funds in order
/// for them to be later spent. To transmit these secrets securely to a
/// recipient without requiring an out-of-band communication channel, the
/// transmission key is used to encrypt them.
///
/// Derived by multiplying a Pallas point [derived][concretediversifyhash] from
/// a `Diversifier` by the `IncomingViewingKey` scalar.
///
/// [concretediversifyhash]: https://zips.z.cash/protocol/nu5.pdf#concretediversifyhash
/// <https://zips.z.cash/protocol/nu5.pdf#orchardkeycomponents>
#[derive(Copy, Clone, PartialEq)]
pub struct TransmissionKey(pub(crate) pallas::Affine);

impl fmt::Debug for TransmissionKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut d = f.debug_struct("TransmissionKey");

        let option: Option<Coordinates<pallas::Affine>> = self.0.coordinates().into();

        match option {
            Some(coordinates) => d
                .field("x", &hex::encode(coordinates.x().to_repr()))
                .field("y", &hex::encode(coordinates.y().to_repr()))
                .finish(),
            None => d
                .field("x", &hex::encode(pallas::Base::zero().to_repr()))
                .field("y", &hex::encode(pallas::Base::zero().to_repr()))
                .finish(),
        }
    }
}

impl Eq for TransmissionKey {}

impl From<TransmissionKey> for [u8; 32] {
    fn from(pk_d: TransmissionKey) -> [u8; 32] {
        pk_d.0.to_bytes()
    }
}

impl PartialEq<[u8; 32]> for TransmissionKey {
    fn eq(&self, other: &[u8; 32]) -> bool {
        &self.0.to_bytes() == other
    }
}

/// An ephemeral public key for Orchard key agreement.
///
/// Stored as the raw 32-byte encoding: deserialization defers the canonical,
/// non-identity Pallas point check (a modular square root, the dominant CPU
/// cost of parsing Orchard actions) to [`EphemeralPublicKey::decompress`]. The
/// semantic verifier enforces the deferred check on untrusted transactions
/// (see [`Transaction::orchard_point_encodings_are_valid`]); the checkpoint
/// verifier trusts block hashes and skips it.
///
/// <https://zips.z.cash/protocol/nu5.pdf#concreteorchardkeyagreement>
/// <https://zips.z.cash/protocol/nu5.pdf#saplingandorchardencrypt>
///
/// [`Transaction::orchard_point_encodings_are_valid`]: crate::transaction::Transaction::orchard_point_encodings_are_valid
#[derive(Copy, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub struct EphemeralPublicKey(pub(crate) [u8; 32]);

impl fmt::Debug for EphemeralPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("EphemeralPublicKey")
            .field(&hex::encode(self.0))
            .finish()
    }
}

impl From<EphemeralPublicKey> for [u8; 32] {
    fn from(epk: EphemeralPublicKey) -> [u8; 32] {
        epk.0
    }
}

impl From<&EphemeralPublicKey> for [u8; 32] {
    fn from(epk: &EphemeralPublicKey) -> [u8; 32] {
        epk.0
    }
}

impl PartialEq<[u8; 32]> for EphemeralPublicKey {
    fn eq(&self, other: &[u8; 32]) -> bool {
        &self.0 == other
    }
}

impl EphemeralPublicKey {
    /// Decompresses and returns the underlying Pallas point, or `None` if the
    /// stored bytes are not a canonical encoding of a non-identity point.
    ///
    /// This is the point decompression that deserialization defers, so it is
    /// fallible: callers must handle an invalid key rather than assume the
    /// stored bytes are valid.
    ///
    /// To stay in consensus with the rest of the network, this must accept
    /// exactly the `epk` encodings the pre-lazy deserializer accepted:
    /// canonical Pallas points excluding the identity (`epk` cannot be 𝒪_P,
    /// which is intrinsic to the `KA^{Orchard}.Public` type). Do not swap in a
    /// different decoder.
    ///
    /// <https://zips.z.cash/protocol/protocol.pdf#concreteorchardkeyagreement>
    pub fn decompress(&self) -> Option<pallas::Affine> {
        let point: pallas::Affine = pallas::Affine::from_bytes(&self.0).into_option()?;

        if point.to_curve().is_identity().into() {
            None
        } else {
            Some(point)
        }
    }

    /// Return the stored 32-byte (little-endian) compressed encoding.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }
}

impl TryFrom<[u8; 32]> for EphemeralPublicKey {
    type Error = &'static str;

    /// Convert an array into a [`EphemeralPublicKey`].
    ///
    /// Returns an error if the encoding is malformed or if [it encodes the
    /// identity point][1].
    ///
    /// > epk cannot be 𝒪_P
    ///
    /// Note that this is [intrinsic to the EphemeralPublicKey][2] type and it is not
    /// a separate consensus rule:
    ///
    /// > Define KA^{Orchard}.Public := P^*.
    ///
    /// [1]: https://zips.z.cash/protocol/protocol.pdf#actiondesc
    /// [2]: https://zips.z.cash/protocol/protocol.pdf#concreteorchardkeyagreement
    fn try_from(bytes: [u8; 32]) -> Result<Self, Self::Error> {
        let possible_point = pallas::Affine::from_bytes(&bytes);

        if possible_point.is_some().into() {
            let point = possible_point.unwrap();
            if point.to_curve().is_identity().into() {
                Err("pallas::Affine value for Orchard EphemeralPublicKey is the identity")
            } else {
                Ok(Self(bytes))
            }
        } else {
            Err("Invalid pallas::Affine value for Orchard EphemeralPublicKey")
        }
    }
}

impl ZcashSerialize for EphemeralPublicKey {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_all(&<[u8; 32]>::from(self)[..])?;
        Ok(())
    }
}

impl ZcashDeserialize for EphemeralPublicKey {
    /// Reads the raw encoding without decompressing the point, deferring the
    /// canonicity and non-identity checks to the semantic verifier; see
    /// [`EphemeralPublicKey`].
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        Ok(Self(reader.read_32_bytes()?))
    }
}

#[cfg(test)]
mod tests {
    use group::{prime::PrimeCurveAffine, GroupEncoding};

    use super::*;
    use crate::serialization::ZcashSerialize;

    #[test]
    fn ephemeral_key_deserialization_defers_the_point_checks() {
        let _init_guard = zakura_test::init();

        // the smallest x-coordinate encoding with no Pallas point
        let off_curve = (0u8..=255)
            .map(|x| {
                let mut bytes = [0u8; 32];
                bytes[0] = x;
                bytes
            })
            .find(|bytes| bool::from(pallas::Affine::from_bytes(bytes).is_none()))
            .expect("roughly half of all x-coordinates are off-curve");
        // the identity encodes as all zeroes and is a canonical point, but is
        // rejected for epk ("epk cannot be 𝒪_P")
        let identity = pallas::Affine::identity().to_bytes();

        for bytes in [off_curve, identity] {
            // the validating constructor still rejects, like the pre-lazy parser
            assert!(EphemeralPublicKey::try_from(bytes).is_err());

            // wire deserialization accepts the raw bytes and round-trips them
            // unchanged, so txids and merkle roots are unaffected
            let epk = EphemeralPublicKey::zcash_deserialize(&bytes[..])
                .expect("lazy deserialization accepts any 32 bytes");
            assert_eq!(epk.to_bytes(), bytes, "epk bytes preserved exactly");
            assert_eq!(
                epk.zcash_serialize_to_vec().expect("serializes"),
                bytes.to_vec(),
            );

            // the deferred check catches the invalid encoding
            assert!(epk.decompress().is_none());
        }

        // a valid non-identity encoding decompresses to the same point
        let g = pallas::Affine::generator();
        let epk = EphemeralPublicKey::zcash_deserialize(&g.to_bytes()[..])
            .expect("lazy deserialization accepts any 32 bytes");
        assert_eq!(epk.decompress().expect("generator is valid"), g);
    }
}
