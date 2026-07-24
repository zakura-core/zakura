//! Note and value commitments.

use std::{fmt, io};

use group::{
    ff::{FromUniformBytes, PrimeField},
    GroupEncoding,
};
use halo2::{
    arithmetic::{Coordinates, CurveAffine},
    pasta::pallas,
};
use lazy_static::lazy_static;
use rand_core::{CryptoRng, RngCore};

use crate::{
    amount::Amount,
    error::RandError,
    serialization::{
        serde_helpers, ReadZcashExt, SerializationError, ZcashDeserialize, ZcashSerialize,
    },
};

use super::sinsemilla::*;

/// Generates a random scalar from the scalar field 𝔽_{q_P}.
///
/// <https://zips.z.cash/protocol/nu5.pdf#pallasandvesta>
pub fn generate_trapdoor<T>(csprng: &mut T) -> Result<pallas::Scalar, RandError>
where
    T: RngCore + CryptoRng,
{
    let mut bytes = [0u8; 64];
    csprng
        .try_fill_bytes(&mut bytes)
        .map_err(|_| RandError::FillBytes)?;
    // pallas::Scalar::from_uniform_bytes() reduces the input modulo q_P under the hood.
    Ok(pallas::Scalar::from_uniform_bytes(&bytes))
}

/// The randomness used in the Simsemilla hash for note commitment.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CommitmentRandomness(pallas::Scalar);

/// Note commitments for the output notes.
#[derive(Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
pub struct NoteCommitment(#[serde(with = "serde_helpers::Affine")] pub pallas::Affine);

impl fmt::Debug for NoteCommitment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut d = f.debug_struct("NoteCommitment");

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

impl From<pallas::Point> for NoteCommitment {
    fn from(projective_point: pallas::Point) -> Self {
        Self(pallas::Affine::from(projective_point))
    }
}

impl From<NoteCommitment> for [u8; 32] {
    fn from(cm: NoteCommitment) -> [u8; 32] {
        cm.0.to_bytes()
    }
}

impl TryFrom<[u8; 32]> for NoteCommitment {
    type Error = &'static str;

    fn try_from(bytes: [u8; 32]) -> Result<Self, Self::Error> {
        let possible_point = pallas::Affine::from_bytes(&bytes);

        if possible_point.is_some().into() {
            Ok(Self(possible_point.unwrap()))
        } else {
            Err("Invalid pallas::Affine value")
        }
    }
}

impl NoteCommitment {
    /// Extract the x coordinate of the note commitment.
    pub fn extract_x(&self) -> pallas::Base {
        extract_p(self.0.into())
    }
}

/// A homomorphic Pedersen commitment to the net value of a _note_, used in
/// Action descriptions.
///
/// Stored as the raw 32-byte encoding: deserialization defers the canonical
/// Pallas point check (a modular square root, the dominant CPU cost of parsing
/// Orchard actions) to [`ValueCommitment::commitment`]. The semantic verifier
/// enforces the deferred check on untrusted transactions (see
/// [`Transaction::orchard_point_encodings_are_valid`]); the checkpoint verifier
/// trusts block hashes and skips it.
///
/// <https://zips.z.cash/protocol/nu5.pdf#concretehomomorphiccommit>
///
/// [`Transaction::orchard_point_encodings_are_valid`]: crate::transaction::Transaction::orchard_point_encodings_are_valid
#[derive(Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
pub struct ValueCommitment(pub(crate) [u8; 32]);

impl fmt::Debug for ValueCommitment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("ValueCommitment")
            .field(&hex::encode(self.0))
            .finish()
    }
}

impl From<pallas::Point> for ValueCommitment {
    fn from(projective_point: pallas::Point) -> Self {
        Self(pallas::Affine::from(projective_point).to_bytes())
    }
}

/// LEBS2OSP256(repr_P(cv))
///
/// <https://zips.z.cash/protocol/nu5.pdf#pallasandvesta>
impl From<ValueCommitment> for [u8; 32] {
    fn from(cm: ValueCommitment) -> [u8; 32] {
        cm.0
    }
}

/// LEBS2OSP256(repr_P(cv))
///
/// Validating constructor: rejects encodings that are not canonical Pallas
/// points, exactly like the pre-lazy deserializer did. Wire deserialization
/// intentionally does NOT go through this check — see [`ValueCommitment`].
///
/// <https://zips.z.cash/protocol/nu5.pdf#pallasandvesta>
impl TryFrom<[u8; 32]> for ValueCommitment {
    type Error = &'static str;

    fn try_from(bytes: [u8; 32]) -> Result<Self, Self::Error> {
        let possible_point = pallas::Affine::from_bytes(&bytes);

        if possible_point.is_some().into() {
            Ok(Self(bytes))
        } else {
            Err("Invalid pallas::Affine value")
        }
    }
}

impl ZcashSerialize for ValueCommitment {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_all(&self.0[..])?;
        Ok(())
    }
}

impl ZcashDeserialize for ValueCommitment {
    /// Reads the raw encoding without decompressing the point, deferring the
    /// canonicity check to the semantic verifier; see [`ValueCommitment`].
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        Ok(Self(reader.read_32_bytes()?))
    }
}

impl ValueCommitment {
    /// Generate a new _ValueCommitment_.
    ///
    /// <https://zips.z.cash/protocol/nu5.pdf#concretehomomorphiccommit>
    pub fn randomized<T>(csprng: &mut T, value: Amount) -> Result<Self, RandError>
    where
        T: RngCore + CryptoRng,
    {
        let rcv = generate_trapdoor(csprng)?;

        Ok(Self::new(rcv, value))
    }

    /// Generate a new `ValueCommitment` from an existing `rcv on a `value`.
    ///
    /// ValueCommit^Orchard(v) :=
    ///
    /// <https://zips.z.cash/protocol/nu5.pdf#concretehomomorphiccommit>
    #[allow(non_snake_case)]
    pub fn new(rcv: pallas::Scalar, value: Amount) -> Self {
        Self::from(Self::commit_point(rcv, value))
    }

    /// Compute ValueCommit^Orchard(rcv, value) as a Pallas point, without
    /// compressing it. Used by the binding-key computation, which sums
    /// commitment points directly.
    ///
    /// <https://zips.z.cash/protocol/nu5.pdf#concretehomomorphiccommit>
    pub fn commit_point(rcv: pallas::Scalar, value: Amount) -> pallas::Point {
        let v = pallas::Scalar::from(value);
        *V * v + *R * rcv
    }

    /// Decompresses and returns the underlying Pallas point, or `None` if the
    /// stored bytes are not a canonical point encoding.
    ///
    /// This is the point decompression that deserialization defers, so it is
    /// fallible: callers must handle an invalid commitment rather than assume
    /// the stored bytes are valid.
    ///
    /// To stay in consensus with the rest of the network, this must accept
    /// exactly the `cv` encodings the pre-lazy deserializer (and librustzcash's
    /// Orchard action parser) accepted: canonical Pallas points, including the
    /// identity. Do not swap in a different decoder.
    pub fn commitment(&self) -> Option<pallas::Affine> {
        pallas::Affine::from_bytes(&self.0).into_option()
    }

    /// Return the stored 32-byte (little-endian) compressed encoding.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }
}

lazy_static! {
    static ref V: pallas::Point = pallas_group_hash(b"z.cash:Orchard-cv", b"v");
    static ref R: pallas::Point = pallas_group_hash(b"z.cash:Orchard-cv", b"r");
}

#[cfg(test)]
mod tests {

    use group::{prime::PrimeCurveAffine, Group};

    use super::*;

    /// The smallest x-coordinate encoding with no Pallas point: not a valid
    /// point encoding, so decompression must fail.
    fn off_curve_bytes() -> [u8; 32] {
        for x in 0u8..=255 {
            let mut bytes = [0u8; 32];
            bytes[0] = x;
            if bool::from(pallas::Affine::from_bytes(&bytes).is_none()) {
                return bytes;
            }
        }
        unreachable!("roughly half of all x-coordinates are off-curve");
    }

    #[test]
    fn commitment_round_trips_valid_points() {
        let _init_guard = zakura_test::init();

        let g = pallas::Affine::generator();
        let cv = ValueCommitment::from(g.to_curve());

        assert_eq!(cv.to_bytes(), g.to_bytes());
        assert_eq!(cv.commitment().expect("generator decompresses"), g);
        // the identity is a canonical encoding and a valid cv
        let identity_cv = ValueCommitment::from(pallas::Point::identity());
        assert!(identity_cv.commitment().is_some());
    }

    #[test]
    fn deserialization_defers_the_point_check() {
        let _init_guard = zakura_test::init();

        let bytes = off_curve_bytes();

        // the validating constructor still rejects, like the pre-lazy parser
        assert!(ValueCommitment::try_from(bytes).is_err());

        // wire deserialization accepts the raw bytes and round-trips them
        // unchanged, so txids and merkle roots are unaffected
        let cv = ValueCommitment::zcash_deserialize(&bytes[..])
            .expect("lazy deserialization accepts any 32 bytes");
        assert_eq!(cv.to_bytes(), bytes, "cv bytes preserved exactly");
        assert_eq!(
            cv.zcash_serialize_to_vec().expect("serializes"),
            bytes.to_vec(),
        );

        // the deferred check catches the invalid encoding
        assert!(cv.commitment().is_none());
    }

    #[test]
    fn binding_key_math_matches_commit_point() {
        let _init_guard = zakura_test::init();

        let value = Amount::try_from(1).expect("valid amount");
        let rcv = pallas::Scalar::from(7u64);

        // new() is commit_point() compressed
        assert_eq!(
            ValueCommitment::new(rcv, value).to_bytes(),
            pallas::Affine::from(ValueCommitment::commit_point(rcv, value)).to_bytes(),
        );
    }
}
