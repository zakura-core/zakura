use group::{ff::PrimeField, GroupEncoding};
use halo2::pasta::pallas;
use serde::{de::Error, Deserializer};

#[derive(Deserialize, Serialize)]
#[serde(remote = "jubjub::AffinePoint")]
pub struct AffinePoint {
    #[serde(getter = "jubjub::AffinePoint::to_bytes")]
    bytes: [u8; 32],
}

impl From<AffinePoint> for jubjub::AffinePoint {
    fn from(local: AffinePoint) -> Self {
        jubjub::AffinePoint::from_bytes(local.bytes).unwrap()
    }
}

#[derive(Serialize)]
#[serde(remote = "jubjub::Fq")]
pub struct Fq {
    #[serde(getter = "jubjub::Fq::to_bytes")]
    bytes: [u8; 32],
}

impl Fq {
    /// Deserializes a canonical field element, rejecting any other encoding.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<jubjub::Fq, D::Error> {
        let bytes = <FqBytes as serde::Deserialize>::deserialize(deserializer)?.bytes;
        Option::from(jubjub::Fq::from_bytes(&bytes))
            .ok_or_else(|| D::Error::custom("non-canonical Jubjub base field element"))
    }
}

/// The serialized shape of [`Fq`].
#[derive(Deserialize)]
#[serde(rename = "Fq")]
struct FqBytes {
    bytes: [u8; 32],
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "pallas::Affine")]
pub struct Affine {
    #[serde(getter = "pallas::Affine::to_bytes")]
    bytes: [u8; 32],
}

impl From<Affine> for pallas::Affine {
    fn from(local: Affine) -> Self {
        pallas::Affine::from_bytes(&local.bytes).unwrap()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "pallas::Scalar")]
pub struct Scalar {
    #[serde(getter = "pallas::Scalar::to_repr")]
    bytes: [u8; 32],
}

impl From<Scalar> for pallas::Scalar {
    fn from(local: Scalar) -> Self {
        pallas::Scalar::from_repr(local.bytes).unwrap()
    }
}

#[derive(Serialize)]
#[serde(remote = "pallas::Base")]
pub struct Base {
    #[serde(getter = "pallas::Base::to_repr")]
    bytes: [u8; 32],
}

impl Base {
    /// Deserializes a canonical field element, rejecting any other encoding.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<pallas::Base, D::Error> {
        let bytes = <BaseBytes as serde::Deserialize>::deserialize(deserializer)?.bytes;
        Option::from(pallas::Base::from_repr(bytes))
            .ok_or_else(|| D::Error::custom("non-canonical Pallas base field element"))
    }
}

/// The serialized shape of [`Base`].
#[derive(Deserialize)]
#[serde(rename = "Base")]
struct BaseBytes {
    bytes: [u8; 32],
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "sapling_crypto::value::ValueCommitment")]
pub struct ValueCommitment {
    #[serde(getter = "sapling_crypto::value::ValueCommitment::to_bytes")]
    bytes: [u8; 32],
}

impl From<ValueCommitment> for sapling_crypto::value::ValueCommitment {
    fn from(local: ValueCommitment) -> Self {
        sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&local.bytes).unwrap()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "sapling_crypto::note::ExtractedNoteCommitment")]
pub struct SaplingExtractedNoteCommitment {
    #[serde(getter = "SaplingExtractedNoteCommitment::as_serializable_bytes")]
    bytes: [u8; 32],
}

impl From<SaplingExtractedNoteCommitment> for sapling_crypto::note::ExtractedNoteCommitment {
    fn from(local: SaplingExtractedNoteCommitment) -> Self {
        sapling_crypto::note::ExtractedNoteCommitment::from_bytes(&local.bytes).unwrap()
    }
}

impl SaplingExtractedNoteCommitment {
    fn as_serializable_bytes(remote: &sapling_crypto::note::ExtractedNoteCommitment) -> [u8; 32] {
        remote.to_bytes()
    }
}

#[derive(Serialize)]
#[serde(remote = "sapling_crypto::Node")]
pub struct Node {
    #[serde(getter = "Node::as_serializable_bytes")]
    bytes: [u8; 32],
}

impl Node {
    fn as_serializable_bytes(remote: &sapling_crypto::Node) -> [u8; 32] {
        remote.to_bytes()
    }

    /// Deserializes a canonical Sapling tree node, rejecting any other encoding.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<sapling_crypto::Node, D::Error> {
        let bytes = <NodeBytes as serde::Deserialize>::deserialize(deserializer)?.bytes;
        Option::from(sapling_crypto::Node::from_bytes(bytes))
            .ok_or_else(|| D::Error::custom("non-canonical Sapling tree node"))
    }
}

/// The serialized shape of [`Node`].
#[derive(Deserialize)]
#[serde(rename = "Node")]
struct NodeBytes {
    bytes: [u8; 32],
}
