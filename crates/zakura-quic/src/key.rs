//! Node identity: Ed25519 public and secret keys (SPEC §3).
//!
//! Adapted from `iroh-base/src/key.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.
//!
//! The text forms match Iroh's, so existing key files and `id@addr` bootstrap
//! entries keep parsing and every key yields the same node ID.

use std::{
    fmt,
    hash::{Hash, Hasher},
    net::SocketAddr,
    str::FromStr,
};

use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use zeroize::Zeroize as _;

/// Length of an Ed25519 public key or secret seed, in bytes.
pub const KEY_LENGTH: usize = 32;

/// A node identity: a 32-byte compressed Ed25519 public key (ID-1).
///
/// Construction checks that the bytes decompress to a curve point. Text form is
/// 64 lowercase hex characters (ID-2).
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
    /// The bytes don't decompress to an Ed25519 curve point.
    #[error("data is not a valid Ed25519 public key")]
    InvalidKeyData,
}

impl NodeId {
    /// Builds a node ID from its 32 bytes, rejecting bytes that aren't a curve point.
    pub fn from_bytes(bytes: &[u8; KEY_LENGTH]) -> Result<Self, KeyParsingError> {
        VerifyingKey::from_bytes(bytes).map_err(|_| KeyParsingError::InvalidKeyData)?;
        Ok(Self(*bytes))
    }

    /// Returns the 32 raw bytes, the form Zakura's wire formats use (ID-7).
    pub fn as_bytes(&self) -> &[u8; KEY_LENGTH] {
        &self.0
    }

    /// Verifies `signature` over `message` with `verify_strict` (ID-6).
    pub fn verify_strict(&self, message: &[u8], signature: &[u8]) -> Result<(), SignatureError> {
        let signature = Signature::from_slice(signature).map_err(|_| SignatureError)?;
        self.verifying_key()
            .verify_strict(message, &signature)
            .map_err(|_| SignatureError)
    }

    /// Returns the first 5 bytes as hex, for logs.
    pub fn fmt_short(&self) -> String {
        hex::encode(&self.0[..5])
    }

    fn verifying_key(&self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.0).expect("NodeId bytes were validated at construction")
    }
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

/// A signature failed `verify_strict`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid Ed25519 signature")]
pub struct SignatureError;

/// A node's Ed25519 secret seed (ID-3).
///
/// It zeroizes on drop. `Debug` and `Display` never show the seed, and it has
/// no `Serialize` impl.
#[derive(Clone)]
pub struct NodeSecretKey(SigningKey);

impl NodeSecretKey {
    /// Builds a key from its 32-byte seed. The public key derives per RFC 8032 (ID-4).
    pub fn from_bytes(seed: &[u8; KEY_LENGTH]) -> Self {
        Self(SigningKey::from_bytes(seed))
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
        NodeId(self.0.verifying_key().to_bytes())
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

    #[test]
    fn verify_strict_refuses_small_order_and_non_canonical_inputs() {
        // The identity point (small order) as a public key.
        let mut identity = [0u8; 32];
        identity[0] = 1;
        let small_order = NodeId::from_bytes(&identity).unwrap();
        let mut sig = [0u8; 64];
        sig[0] = 1;
        assert_eq!(
            small_order.verify_strict(b"msg", &sig),
            Err(SignatureError)
        );

        // A valid signature with S + L (non-canonical scalar).
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        let mut sig = secret.sign(b"");
        const L: [u8; 32] = [
            0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9,
            0xde, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x10,
        ];
        let mut carry = 0u16;
        for (s, l) in sig[32..].iter_mut().zip(L) {
            let sum = u16::from(*s) + u16::from(l) + carry;
            *s = sum as u8;
            carry = sum >> 8;
        }
        assert_eq!(
            secret.public().verify_strict(b"", &sig),
            Err(SignatureError)
        );

        // A small-order R point with an otherwise well-formed signature.
        let mut sig = secret.sign(b"");
        sig[..32].copy_from_slice(&identity);
        assert_eq!(
            secret.public().verify_strict(b"", &sig),
            Err(SignatureError)
        );
    }

    #[test]
    fn secret_debug_is_redacted() {
        let secret: NodeSecretKey = RFC8032_SEED.parse().unwrap();
        let debug = format!("{secret:?}");
        assert!(!debug.contains(&RFC8032_SEED[..8]));
        assert_eq!(secret.to_hex(), RFC8032_SEED);
    }
}
