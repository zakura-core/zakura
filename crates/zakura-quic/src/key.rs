//! Node identity: Ed25519 public and secret keys (SPEC §3).
//!
//! Adapted from `iroh-base/src/key.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.
//!
//! The text forms match Iroh's, so existing key files and `id@addr` bootstrap
//! entries keep parsing and every key yields the same node ID.
//!
//! Signing and the verification equation use `ed25519-zebra`, which Zakura's
//! discovery records already use for the same key. `curve25519-dalek` checks
//! the public key and each signature's `R` point before the equation runs
//! (ID-6).

use std::{
    fmt,
    hash::{Hash, Hasher},
    net::SocketAddr,
    str::FromStr,
};

use curve25519_dalek::edwards::{CompressedEdwardsY, EdwardsPoint};
use ed25519_zebra::{Signature, SigningKey, VerificationKey};
use zeroize::Zeroize as _;

/// Length of an Ed25519 public key or secret seed, in bytes.
pub const KEY_LENGTH: usize = 32;

/// A node identity: a 32-byte compressed Ed25519 public key (ID-1).
///
/// Construction checks that the bytes are a canonical encoding of a point in
/// the prime-order subgroup (ID-6). Text form is 64 lowercase hex characters
/// (ID-2).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeId([u8; KEY_LENGTH]);

/// Error returned when parsing a [`NodeId`] or a [`NodeSecretKey`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum KeyParsingError {
    /// The input is neither 64 hex characters nor 52 base32 characters.
    #[error("invalid key length")]
    InvalidLength,
    /// The input is not valid hex.
    #[error("failed to decode hex key")]
    InvalidHex,
    /// The input is not valid base32.
    #[error("failed to decode base32 key")]
    InvalidBase32,
    /// The bytes aren't a canonical encoding of an Ed25519 point outside the
    /// small-order subgroup.
    #[error("data is not a valid Ed25519 public key")]
    InvalidKeyData,
}

impl NodeId {
    /// Builds a node ID from its 32 bytes (ID-6).
    ///
    /// Rejects bytes that aren't a curve point, a non-canonical encoding and a
    /// small-order point. Every honestly generated key passes, because its
    /// point is a multiple of the base point.
    ///
    /// A key with a torsion component parses, because the torsion check costs
    /// a full scalar multiplication and Zakura converts peer IDs to node IDs
    /// on hot paths. [`verify_strict`](Self::verify_strict) refuses such a
    /// key, so it can never sign a handshake or a record.
    pub fn from_bytes(bytes: &[u8; KEY_LENGTH]) -> Result<Self, KeyParsingError> {
        if curve_point(bytes).is_none() {
            return Err(KeyParsingError::InvalidKeyData);
        }
        Ok(Self(*bytes))
    }

    /// Returns the 32 raw bytes, the form Zakura's wire formats use (ID-7).
    pub fn as_bytes(&self) -> &[u8; KEY_LENGTH] {
        &self.0
    }

    /// Verifies `signature` over `message` (ID-6).
    ///
    /// The key `A` and the signature's `R` must be canonical, not of small
    /// order and torsion-free, and `S` must be canonical. With `A` and `R` in
    /// the prime-order subgroup,
    /// `ed25519-zebra`'s cofactored equation accepts exactly the signatures the
    /// cofactorless equation accepts, so this accepts a subset of what
    /// `ed25519-dalek`'s `verify_strict` accepts.
    pub fn verify_strict(&self, message: &[u8], signature: &[u8]) -> Result<(), SignatureError> {
        let signature = Signature::from_slice(signature).map_err(|_| SignatureError)?;
        prime_order_point(&self.0).ok_or(SignatureError)?;
        prime_order_point(signature.r_bytes()).ok_or(SignatureError)?;
        self.verifying_key()
            .verify(&signature, message)
            .map_err(|_| SignatureError)
    }

    /// Returns the first 5 bytes as hex, for logs.
    pub fn fmt_short(&self) -> String {
        hex::encode(&self.0[..5])
    }

    fn verifying_key(&self) -> VerificationKey {
        VerificationKey::try_from(self.0)
            .expect("from_bytes checked that the bytes decompress to a curve point")
    }
}

/// Decompresses `bytes` and returns the point if it's canonical and not of
/// small order (ID-6).
fn curve_point(bytes: &[u8; KEY_LENGTH]) -> Option<EdwardsPoint> {
    let point = CompressedEdwardsY(*bytes).decompress()?;
    (point.compress().as_bytes() == bytes && !point.is_small_order()).then_some(point)
}

/// Like [`curve_point`], and also requires the point to be torsion-free, so it
/// lies in the prime-order subgroup (ID-6).
fn prime_order_point(bytes: &[u8; KEY_LENGTH]) -> Option<EdwardsPoint> {
    curve_point(bytes).filter(EdwardsPoint::is_torsion_free)
}

impl Hash for NodeId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl TryFrom<&[u8]> for NodeId {
    type Error = KeyParsingError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: &[u8; KEY_LENGTH] = bytes
            .try_into()
            .map_err(|_| KeyParsingError::InvalidLength)?;
        Self::from_bytes(bytes)
    }
}

impl AsRef<[u8]> for NodeId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({self})")
    }
}

/// Parses hex (either case) or Iroh's base32 form.
impl FromStr for NodeId {
    type Err = KeyParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_bytes(&decode_key_text(s)?)
    }
}

impl serde::Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> serde::Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let text = String::deserialize(deserializer)?;
            text.parse().map_err(serde::de::Error::custom)
        } else {
            let bytes = <[u8; KEY_LENGTH]>::deserialize(deserializer)?;
            Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
        }
    }
}

/// A signature failed [`NodeId::verify_strict`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid Ed25519 signature")]
pub struct SignatureError;

/// A node's Ed25519 secret seed (ID-3).
///
/// It zeroizes on drop. `Debug` and `Display` never show the seed, and it has
/// no `Serialize` impl.
#[derive(Clone)]
pub struct NodeSecretKey(SigningKey);

impl Drop for NodeSecretKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl NodeSecretKey {
    /// Builds a key from its 32-byte seed. The public key derives per RFC 8032 (ID-4).
    pub fn from_bytes(seed: &[u8; KEY_LENGTH]) -> Self {
        Self(SigningKey::from(*seed))
    }

    /// Generates a key from the operating system's random source.
    pub fn generate() -> Self {
        let mut seed = [0u8; KEY_LENGTH];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut seed)
            .expect("the operating system random source is available");
        let key = Self::from_bytes(&seed);
        seed.zeroize();
        key
    }

    /// Returns the 32-byte seed. Callers must not log it.
    pub fn to_bytes(&self) -> [u8; KEY_LENGTH] {
        self.0.to_bytes()
    }

    /// Returns the seed as 64 lowercase hex characters, the key-file form (ID-5).
    pub fn to_hex(&self) -> String {
        let mut seed = self.to_bytes();
        let text = hex::encode(seed);
        seed.zeroize();
        text
    }

    /// Returns this key's node ID.
    pub fn public(&self) -> NodeId {
        NodeId(self.0.verification_key().into())
    }

    /// Signs `message` with Ed25519.
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.0.sign(message).to_bytes()
    }
}

impl fmt::Debug for NodeSecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NodeSecretKey([redacted])")
    }
}

/// Parses hex (either case) or Iroh's base32 form, like Iroh's `SecretKey::from_str`.
impl FromStr for NodeSecretKey {
    type Err = KeyParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut seed = decode_key_text(s)?;
        let key = Self::from_bytes(&seed);
        seed.zeroize();
        Ok(key)
    }
}

/// A node's identity plus the direct addresses to dial it on (API-1).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeAddr {
    /// The node's identity.
    pub id: NodeId,
    /// Direct UDP addresses, in the order to try them.
    pub direct: Vec<SocketAddr>,
}

impl NodeAddr {
    /// Builds an address with no direct addresses.
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            direct: Vec::new(),
        }
    }

    /// Builds an address with the given direct addresses.
    pub fn with_addrs(id: NodeId, direct: impl IntoIterator<Item = SocketAddr>) -> Self {
        Self {
            id,
            direct: direct.into_iter().collect(),
        }
    }
}

/// Decodes 64 hex characters (either case) or 52 RFC 4648 base32 characters.
///
/// Iroh accepted both forms in `SecretKey::from_str`, so deployed key files and
/// configs written in either form keep loading.
fn decode_key_text(s: &str) -> Result<[u8; KEY_LENGTH], KeyParsingError> {
    let mut bytes = [0u8; KEY_LENGTH];
    if s.len() == KEY_LENGTH * 2 {
        hex::decode_to_slice(s, &mut bytes).map_err(|_| KeyParsingError::InvalidHex)?;
        return Ok(bytes);
    }
    // 32 bytes is 256 bits, which is 52 base32 characters without padding.
    if s.len() != 52 {
        return Err(KeyParsingError::InvalidLength);
    }
    let mut acc: u64 = 0;
    let mut bits = 0u32;
    let mut out = 0usize;
    for c in s.bytes() {
        let value = match c.to_ascii_uppercase() {
            c @ b'A'..=b'Z' => c - b'A',
            c @ b'2'..=b'7' => c - b'2' + 26,
            _ => return Err(KeyParsingError::InvalidBase32),
        };
        acc = (acc << 5) | u64::from(value);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            bytes[out] = (acc >> bits) as u8;
            out += 1;
        }
    }
    // The last character carries 4 padding bits, which must be zero.
    if out != KEY_LENGTH || acc & ((1 << bits) - 1) != 0 {
        return Err(KeyParsingError::InvalidBase32);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 8032 §7.1, test 1.
    const RFC8032_SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
    const RFC8032_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    const RFC8032_SIG: &str = "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b";

    #[test]
    fn rfc8032_vector_derives_and_verifies() {
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        assert_eq!(secret.public().to_string(), RFC8032_PUBLIC);
        let sig = secret.sign(b"");
        assert_eq!(hex::encode(sig), RFC8032_SIG);
        secret.public().verify_strict(b"", &sig).unwrap();
    }

    #[test]
    fn hex_and_base32_parse_to_the_same_key() {
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        let upper: NodeSecretKey = RFC8032_SEED.to_uppercase().parse().unwrap();
        assert_eq!(secret.to_bytes(), upper.to_bytes());

        // RFC 4648 base32 of the seed, as Iroh's `BASE32_NOPAD` writes it.
        let base32 = "TVQ3DHPP7VNGBOUEJL2JF3BMYRCETRLJPMZGSGLQHOWAGHFOP5QA";
        let from_base32: NodeSecretKey = base32.parse().unwrap();
        assert_eq!(from_base32.to_bytes(), secret.to_bytes());
        let lower: NodeSecretKey = base32.to_lowercase().parse().unwrap();
        assert_eq!(lower.to_bytes(), secret.to_bytes());
    }

    #[test]
    fn malformed_key_text_is_refused() {
        assert_eq!(
            "foobarbaz".parse::<NodeId>(),
            Err(KeyParsingError::InvalidLength)
        );
        assert_eq!(
            "zz".repeat(32).parse::<NodeId>(),
            Err(KeyParsingError::InvalidHex)
        );
        // A trailing character with non-zero padding bits.
        assert!("TVQ3DHPP7VNGBOUEJL2JF3BMYRCETRLJPMZGSGLQHOWAGHFOP5QB"
            .parse::<NodeSecretKey>()
            .is_err());
    }

    #[test]
    fn node_id_round_trips_through_text_and_serde() {
        let id: NodeId = RFC8032_PUBLIC.parse().unwrap();
        assert_eq!(id.to_string().parse::<NodeId>().unwrap(), id);
        assert_eq!(NodeId::try_from(&id.as_bytes()[..]).unwrap(), id);
    }

    #[test]
    fn non_point_bytes_are_refused() {
        // y = 2 has no matching x on the curve.
        let mut bytes = [0u8; 32];
        bytes[0] = 2;
        assert_eq!(
            NodeId::from_bytes(&bytes),
            Err(KeyParsingError::InvalidKeyData)
        );
    }

    /// The identity point: small order.
    const IDENTITY: [u8; 32] = {
        let mut bytes = [0u8; 32];
        bytes[0] = 1;
        bytes
    };

    /// A point of order 8.
    const ORDER_8: [u8; 32] = [
        0xc7, 0x17, 0x6a, 0x70, 0x3d, 0x4d, 0xd8, 0x4f, 0xba, 0x3c, 0x0b, 0x76, 0x0d, 0x10, 0x67,
        0x0f, 0x2a, 0x20, 0x53, 0xfa, 0x2c, 0x39, 0xcc, 0xc6, 0x4e, 0xc7, 0xfd, 0x77, 0x92, 0xac,
        0x03, 0x7a,
    ];

    /// The order of the prime-order subgroup, little-endian.
    const L: [u8; 32] = [
        0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde,
        0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x10,
    ];

    /// Adds `point` to the point encoded by `bytes`.
    fn add_point(bytes: &[u8; 32], point: &[u8; 32]) -> [u8; 32] {
        let a = CompressedEdwardsY(*bytes).decompress().unwrap();
        let b = CompressedEdwardsY(*point).decompress().unwrap();
        (a + b).compress().to_bytes()
    }

    #[test]
    fn small_order_keys_are_refused() {
        assert_eq!(
            NodeId::from_bytes(&IDENTITY),
            Err(KeyParsingError::InvalidKeyData)
        );
        assert_eq!(
            NodeId::from_bytes(&ORDER_8),
            Err(KeyParsingError::InvalidKeyData)
        );
    }

    #[test]
    fn mixed_order_keys_never_verify() {
        // An honest key plus a point of order 8 decompresses, isn't small
        // order, and has a torsion component. It parses, but no signature
        // verifies under it, including one the honest key made.
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        let honest = secret.public();
        let mixed = NodeId::from_bytes(&add_point(honest.as_bytes(), &ORDER_8))
            .expect("a mixed-order key parses");
        let signature = secret.sign(b"message");
        assert!(honest.verify_strict(b"message", &signature).is_ok());
        assert_eq!(
            mixed.verify_strict(b"message", &signature),
            Err(SignatureError)
        );
    }

    #[test]
    fn non_canonical_key_encodings_are_refused() {
        // y = p + 1 encodes the same point as y = 1 (the identity), but
        // non-canonically. 2^255 - 19 + 1 = 0x7f..ffee, little-endian.
        let mut bytes = [0xff; 32];
        bytes[0] = 0xee;
        bytes[31] = 0x7f;
        assert_eq!(
            NodeId::from_bytes(&bytes),
            Err(KeyParsingError::InvalidKeyData)
        );
        // x = 0 with the sign bit set decodes to the identity, but its
        // canonical encoding has the bit clear.
        let mut negative_zero = IDENTITY;
        negative_zero[31] |= 0x80;
        assert_eq!(
            NodeId::from_bytes(&negative_zero),
            Err(KeyParsingError::InvalidKeyData)
        );
    }

    #[test]
    fn honest_keys_always_pass() {
        for seed in 0..64u8 {
            let secret = NodeSecretKey::from_bytes(&[seed; 32]);
            let id = secret.public();
            assert_eq!(NodeId::from_bytes(id.as_bytes()), Ok(id));
            id.verify_strict(b"msg", &secret.sign(b"msg")).unwrap();
        }
    }

    #[test]
    fn non_canonical_s_is_refused() {
        // A valid signature with S + L.
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        let mut sig = secret.sign(b"");
        let mut carry = 0u16;
        for (s, l) in sig[32..].iter_mut().zip(L) {
            let sum = u16::from(*s) + u16::from(l) + carry;
            // Keeps the low byte; `carry` holds the rest.
            *s = sum as u8;
            carry = sum >> 8;
        }
        assert_eq!(
            secret.public().verify_strict(b"", &sig),
            Err(SignatureError)
        );
    }

    #[test]
    fn small_order_r_is_refused() {
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        let mut sig = secret.sign(b"");
        sig[..32].copy_from_slice(&IDENTITY);
        assert_eq!(
            secret.public().verify_strict(b"", &sig),
            Err(SignatureError)
        );
    }

    #[test]
    fn r_with_a_torsion_component_is_refused() {
        use curve25519_dalek::{constants::ED25519_BASEPOINT_POINT, Scalar};
        use sha2::{Digest as _, Sha512};

        // Sign with R' = rB + T for a point T of order 8, and compute S for
        // the challenge over R'. ZIP 215's cofactored equation ignores T, so
        // `ed25519-zebra` alone accepts this signature; the torsion check on
        // R refuses it.
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        let public = secret.public();
        let expanded = Sha512::digest(secret.to_bytes());
        let mut a_bytes = [0u8; 32];
        a_bytes.copy_from_slice(&expanded[..32]);
        a_bytes[0] &= 248;
        a_bytes[31] &= 127;
        a_bytes[31] |= 64;
        let a = Scalar::from_bytes_mod_order(a_bytes);
        let r = Scalar::from_bytes_mod_order([3; 32]);
        let torsion = CompressedEdwardsY(ORDER_8).decompress().unwrap();
        let r_point = (r * ED25519_BASEPOINT_POINT + torsion)
            .compress()
            .to_bytes();
        let challenge: [u8; 64] = Sha512::new()
            .chain_update(r_point)
            .chain_update(public.as_bytes())
            .chain_update(b"msg")
            .finalize()
            .into();
        let s = r + Scalar::from_bytes_mod_order_wide(&challenge) * a;
        let mut sig = [0u8; 64];
        sig[..32].copy_from_slice(&r_point);
        sig[32..].copy_from_slice(s.as_bytes());

        let zebra = VerificationKey::try_from(*public.as_bytes()).unwrap();
        zebra.verify(&Signature::from_bytes(&sig), b"msg").unwrap();
        assert_eq!(public.verify_strict(b"msg", &sig), Err(SignatureError));
    }

    #[test]
    fn secret_debug_is_redacted() {
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        let debug = format!("{secret:?}");
        assert!(!debug.contains(&RFC8032_SEED[..8]));
        assert_eq!(secret.to_hex(), RFC8032_SEED);
    }
}
