//! Orchard key types.
//!
//! Unused key types are not implemented, see PR #5476.
//!
//! <https://zips.z.cash/protocol/nu5.pdf#orchardkeycomponents>

use crate::serialization::ZcashReader;
use std::{fmt, io};

use group::{ff::PrimeField, CurveAffine as _, Group, GroupEncoding};
use halo2::{
    arithmetic::{Coordinates, CurveAffine},
    pasta::pallas,
};

use crate::serialization::{
    serde_helpers, ReadZcashExt, SerializationError, ZcashDeserialize, ZcashSerialize,
};

/// An ephemeral public key for Orchard key agreement.
///
/// <https://zips.z.cash/protocol/nu5.pdf#concreteorchardkeyagreement>
/// <https://zips.z.cash/protocol/nu5.pdf#saplingandorchardencrypt>
#[derive(Copy, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub struct EphemeralPublicKey(#[serde(with = "serde_helpers::Affine")] pub(crate) pallas::Affine);

impl fmt::Debug for EphemeralPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut d = f.debug_struct("EphemeralPublicKey");

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

impl From<EphemeralPublicKey> for [u8; 32] {
    fn from(epk: EphemeralPublicKey) -> [u8; 32] {
        epk.0.to_bytes()
    }
}

impl From<&EphemeralPublicKey> for [u8; 32] {
    fn from(epk: &EphemeralPublicKey) -> [u8; 32] {
        epk.0.to_bytes()
    }
}

impl PartialEq<[u8; 32]> for EphemeralPublicKey {
    fn eq(&self, other: &[u8; 32]) -> bool {
        &self.0.to_bytes() == other
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
                Ok(Self(possible_point.unwrap()))
            }
        } else {
            Err("Invalid pallas::Affine value for Orchard EphemeralPublicKey")
        }
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
