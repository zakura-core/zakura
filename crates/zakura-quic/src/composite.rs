//! The composite Ed25519 + ML-DSA-44 signature a node signs its TLS
//! handshakes with (SPEC TLS-4, ID-13).
//!
//! A composite signature is an Ed25519 signature followed by an ML-DSA-44
//! signature (FIPS 204) over the same message, and it verifies only if both
//! halves do. Forging one therefore needs a break of both schemes: ML-DSA
//! guards against a quantum computer, and Ed25519 against a flaw in the
//! young ML-DSA code.

use blake2b_simd::Params;
use libcrux_ml_dsa::{
    ml_dsa_44::{self, MLDSA44Signature, MLDSA44VerificationKey},
    KEY_GENERATION_RANDOMNESS_SIZE, SIGNING_RANDOMNESS_SIZE,
};
use zeroize::Zeroize as _;

use crate::key::{NodeKey, SignatureError, KEY_LENGTH};

/// The credential tag of an ML-DSA-44 public key (ID-13).
pub const ML_DSA_44_KEY_TAG: u16 = 2;

/// Length of an ML-DSA-44 public key, in bytes.
pub const ML_DSA_44_KEY_BYTES: usize = 1_312;

/// Length of an ML-DSA-44 signature, in bytes.
pub const ML_DSA_44_SIGNATURE_BYTES: usize = 2_420;

/// Length of an Ed25519 signature, in bytes.
const ED25519_SIGNATURE_BYTES: usize = 64;

/// Length of a composite signature: the Ed25519 half, then the ML-DSA-44
/// half.
pub const COMPOSITE_SIGNATURE_BYTES: usize = ED25519_SIGNATURE_BYTES + ML_DSA_44_SIGNATURE_BYTES;

/// The TLS signature scheme of the composite (TLS-4), from the private-use
/// range of RFC 8446 §4.2.3. The IETF composite ML-DSA draft hasn't fixed its
/// code points, and its message encoding differs from this one anyway.
/// rustls reads a private-use scheme's low byte as a signature algorithm and
/// refuses 0x00 to 0x02 (anonymous, RSA and DSA), so it must be above them.
pub(crate) const COMPOSITE_SCHEME: rustls::SignatureScheme =
    rustls::SignatureScheme::Unknown(0xFE44);

/// The FIPS 204 context string of every ML-DSA signature a node makes, so a
/// handshake signature can't be passed off as one made for anything else.
const ML_DSA_CONTEXT: &[u8] = b"Zakura TLS 1.3 Ed25519+ML-DSA-44 v1";

/// The BLAKE2b personalisation that derives a node's ML-DSA-44 key
/// generation seed from its Ed25519 seed (ID-13).
const ML_DSA_SEED_PERSONALISATION: &[u8; 16] = b"Zakura_MLDSA44v1";

/// The ML-DSA-44 key generation seed of the node whose Ed25519 seed is
/// `seed`: BLAKE2b-256 keyed by the seed, under its own personalisation, so
/// one key file holds both keys and neither reveals the other.
fn ml_dsa_seed(seed: &[u8; KEY_LENGTH]) -> [u8; KEY_GENERATION_RANDOMNESS_SIZE] {
    let hash = Params::new()
        .hash_length(KEY_GENERATION_RANDOMNESS_SIZE)
        .key(seed)
        .personal(ML_DSA_SEED_PERSONALISATION)
        .hash(&[]);
    let mut derived = [0; KEY_GENERATION_RANDOMNESS_SIZE];
    derived.copy_from_slice(hash.as_bytes());
    derived
}

/// The ML-DSA-44 public key of the node whose Ed25519 seed is `seed`.
pub(crate) fn ml_dsa_public_key(seed: &[u8; KEY_LENGTH]) -> [u8; ML_DSA_44_KEY_BYTES] {
    let mut derived = ml_dsa_seed(seed);
    let mut pair = ml_dsa_44::generate_key_pair(derived);
    derived.zeroize();
    pair.signing_key.as_mut_slice().zeroize();
    *pair.verification_key.as_ref()
}

/// Signs `message` with the ML-DSA-44 key of the node whose Ed25519 seed is
/// `seed`, hedged with `randomness`.
///
/// The signing key is regenerated from the seed for each signature and wiped
/// after, as the Ed25519 key is, so the node holds one secret in memory.
pub(crate) fn ml_dsa_sign(
    seed: &[u8; KEY_LENGTH],
    message: &[u8],
    mut randomness: [u8; SIGNING_RANDOMNESS_SIZE],
) -> [u8; ML_DSA_44_SIGNATURE_BYTES] {
    let mut derived = ml_dsa_seed(seed);
    let mut pair = ml_dsa_44::generate_key_pair(derived);
    derived.zeroize();
    let signature = ml_dsa_44::sign(&pair.signing_key, message, ML_DSA_CONTEXT, randomness);
    pair.signing_key.as_mut_slice().zeroize();
    randomness.zeroize();
    *signature
        .expect("ML-DSA signing fails only for a context longer than 255 bytes")
        .as_ref()
}

/// Verifies a composite `signature` over `message`: the Ed25519 half under
/// `ed25519`'s strict rules (ID-6) and the ML-DSA-44 half under `ml_dsa`.
/// Both must verify.
pub(crate) fn verify(
    ed25519: &NodeKey,
    ml_dsa: &[u8],
    message: &[u8],
    signature: &[u8],
) -> Result<(), SignatureError> {
    if signature.len() != COMPOSITE_SIGNATURE_BYTES {
        return Err(SignatureError);
    }
    let (classical, post_quantum) = signature.split_at(ED25519_SIGNATURE_BYTES);
    let ml_dsa: [u8; ML_DSA_44_KEY_BYTES] = ml_dsa.try_into().map_err(|_| SignatureError)?;
    let post_quantum: [u8; ML_DSA_44_SIGNATURE_BYTES] =
        post_quantum.try_into().map_err(|_| SignatureError)?;
    ed25519.verify_strict(message, classical)?;
    ml_dsa_44::verify(
        &MLDSA44VerificationKey::new(ml_dsa),
        message,
        ML_DSA_CONTEXT,
        &MLDSA44Signature::new(post_quantum),
    )
    .map_err(|_| SignatureError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeSecretKey;

    const SEED: [u8; KEY_LENGTH] = [7; KEY_LENGTH];

    fn sign(secret: &NodeSecretKey, message: &[u8]) -> Vec<u8> {
        let mut signature = secret.sign(message).to_vec();
        signature.extend_from_slice(&ml_dsa_sign(&secret.to_bytes(), message, [9; 32]));
        signature
    }

    #[test]
    fn a_composite_signature_verifies_only_whole() {
        let secret = NodeSecretKey::from_bytes(&SEED);
        let ml_dsa = ml_dsa_public_key(&SEED);
        let signature = sign(&secret, b"transcript");
        assert_eq!(signature.len(), COMPOSITE_SIGNATURE_BYTES);
        assert_eq!(
            verify(&secret.public_key(), &ml_dsa, b"transcript", &signature),
            Ok(())
        );

        assert!(verify(&secret.public_key(), &ml_dsa, b"another", &signature).is_err());
        for flipped in [0, ED25519_SIGNATURE_BYTES, COMPOSITE_SIGNATURE_BYTES - 1] {
            let mut tampered = signature.clone();
            tampered[flipped] ^= 1;
            assert!(
                verify(&secret.public_key(), &ml_dsa, b"transcript", &tampered).is_err(),
                "byte {flipped} tampered"
            );
        }
        assert!(verify(
            &secret.public_key(),
            &ml_dsa,
            b"transcript",
            &signature[..ED25519_SIGNATURE_BYTES]
        )
        .is_err());
    }

    #[test]
    fn each_half_is_checked_against_its_own_key() {
        let secret = NodeSecretKey::from_bytes(&SEED);
        let other = NodeSecretKey::from_bytes(&[8; KEY_LENGTH]);
        let signature = sign(&secret, b"transcript");
        assert!(verify(
            &other.public_key(),
            &ml_dsa_public_key(&SEED),
            b"transcript",
            &signature
        )
        .is_err());
        assert!(verify(
            &secret.public_key(),
            &ml_dsa_public_key(&[8; KEY_LENGTH]),
            b"transcript",
            &signature
        )
        .is_err());
    }

    #[test]
    fn the_ml_dsa_key_derives_from_the_seed_alone() {
        assert_eq!(ml_dsa_public_key(&SEED), ml_dsa_public_key(&SEED));
        assert_ne!(
            ml_dsa_public_key(&SEED),
            ml_dsa_public_key(&[8; KEY_LENGTH])
        );
        // Computed with Python's `hashlib.blake2b(b"", digest_size=32,
        // key=bytes([7] * 32), person=b"Zakura_MLDSA44v1")`.
        assert_eq!(
            hex::encode(ml_dsa_seed(&SEED)),
            "b6bb692614d0c256236e87538aae1899a93b28431c89e63252e471c005d29aeb"
        );
    }

    #[test]
    fn hedged_signatures_differ_and_both_verify() {
        let ml_dsa = ml_dsa_public_key(&SEED);
        let secret = NodeSecretKey::from_bytes(&SEED);
        let ed = secret.sign(b"m");
        let first = ml_dsa_sign(&SEED, b"m", [1; 32]);
        let second = ml_dsa_sign(&SEED, b"m", [2; 32]);
        assert_ne!(first, second);
        for half in [first, second] {
            let mut signature = ed.to_vec();
            signature.extend_from_slice(&half);
            assert_eq!(
                verify(&secret.public_key(), &ml_dsa, b"m", &signature),
                Ok(())
            );
        }
    }
}
