//! The composite Ed25519 + ML-DSA-65 signature a node signs its TLS
//! handshakes with (SPEC TLS-4, ID-13).
//!
//! A composite signature is an Ed25519 signature followed by an ML-DSA-65
//! signature (FIPS 204) over the same message, and it verifies only if both
//! halves do. Forging one therefore needs a break of both schemes: ML-DSA
//! guards against a quantum computer, and Ed25519 against a flaw in the
//! young ML-DSA code.

use blake2b_simd::Params;
use libcrux_ml_dsa::{
    ml_dsa_65::{self, MLDSA65Signature, MLDSA65SigningKey, MLDSA65VerificationKey},
    KEY_GENERATION_RANDOMNESS_SIZE, SIGNING_RANDOMNESS_SIZE,
};
use zeroize::{Zeroize as _, Zeroizing};

use crate::{
    key::{NodeKey, SignatureError, KEY_LENGTH},
    key_list::{ED25519_KEY_TAG, MAX_NODE_KEYS},
};

/// The credential tag of an ML-DSA-65 public key (ID-13). Tag 2 was the
/// ML-DSA-44 key of `p2p-v2/4`; this release reads it as an unknown tag
/// (ID-10).
pub const ML_DSA_65_KEY_TAG: u16 = 3;

/// The credential tags of the two keys a composite signature is made with
/// (TLS-4). A handshake signed with the composite proves exactly these keys.
pub(crate) const COMPOSITE_KEY_TAGS: [u16; 2] = [ED25519_KEY_TAG, ML_DSA_65_KEY_TAG];

/// The most earlier node IDs a handshake signed with the composite proves
/// (TLS-7): one for each proper prefix of the credential's key list that
/// keeps both signing keys. Such a prefix holds at least those two entries
/// and a credential at most [`MAX_NODE_KEYS`], which makes 30.
pub const MAX_EARLIER_NODE_IDS: usize = MAX_NODE_KEYS - COMPOSITE_KEY_TAGS.len();

/// Length of an ML-DSA-65 public key, in bytes.
pub const ML_DSA_65_KEY_BYTES: usize = 1_952;

/// Length of an ML-DSA-65 signature, in bytes.
pub const ML_DSA_65_SIGNATURE_BYTES: usize = 3_309;

/// Length of an Ed25519 signature, in bytes.
const ED25519_SIGNATURE_BYTES: usize = 64;

/// Length of a composite signature: the Ed25519 half, then the ML-DSA-65
/// half.
pub const COMPOSITE_SIGNATURE_BYTES: usize = ED25519_SIGNATURE_BYTES + ML_DSA_65_SIGNATURE_BYTES;

/// The TLS signature scheme of the composite (TLS-4), from the private-use
/// range of RFC 8446 §4.2.3. The IETF composite ML-DSA draft hasn't fixed its
/// code points, and its message encoding differs from this one anyway.
/// rustls reads a private-use scheme's low byte as a signature algorithm and
/// refuses 0x00 to 0x02 (anonymous, RSA and DSA), so it must be above them.
pub(crate) const COMPOSITE_SCHEME: rustls::SignatureScheme =
    rustls::SignatureScheme::Unknown(0xFE65);

/// The composite's `scheme` label in metrics (OBS-1). rustls has no name for
/// a private-use scheme.
pub(crate) const COMPOSITE_SCHEME_LABEL: &str = "ed25519_mldsa65";

/// The FIPS 204 context string of the ML-DSA signature in a handshake's
/// composite, so a handshake signature can't be passed off as one made for
/// anything else.
const ML_DSA_CONTEXT: &[u8] = b"Zakura TLS 1.3 Ed25519+ML-DSA-65 v1";

/// The FIPS 204 context string of the ML-DSA signature in a discovery record
/// (ID-7). A node signs handshakes and records with one ML-DSA key, and
/// FIPS 204 binds the context into what is signed, so neither signature
/// verifies as the other.
const ML_DSA_RECORD_CONTEXT: &[u8] = b"Zakura node record ML-DSA-65 v1";

/// The BLAKE2b personalisation that derives a node's ML-DSA-65 key
/// generation seed from its Ed25519 seed (ID-13).
const ML_DSA_SEED_PERSONALISATION: &[u8; 16] = b"Zakura_MLDSA65v1";

/// The ML-DSA-65 key generation seed of the node whose Ed25519 seed is
/// `seed`: BLAKE2b-256 keyed by the seed, under its own personalisation, so
/// one key file holds both keys and neither reveals the other (ID-13).
///
/// `blake2b_simd` keeps copies of its key, and of the derived seed in its
/// output, in values it doesn't wipe, so this runs once, when a node loads
/// its key, not per handshake.
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

/// An ML-DSA-65 signing key whose stored bytes are wiped on drop. Signers
/// share it so key generation runs only when loading a node's identity.
pub(crate) struct MlDsaSigningKey(MLDSA65SigningKey);

impl Drop for MlDsaSigningKey {
    fn drop(&mut self) {
        self.0.as_mut_slice().zeroize();
    }
}

impl MlDsaSigningKey {
    /// Derives the signing key and its matching public key from the node's
    /// Ed25519 seed (ID-13). The derived ML-DSA seed is wiped after key generation.
    pub(crate) fn from_identity_seed(seed: &[u8; KEY_LENGTH]) -> (Self, [u8; ML_DSA_65_KEY_BYTES]) {
        let seed = Zeroizing::new(ml_dsa_seed(seed));
        Self::from_seed(&seed)
    }

    /// Generates both keys from the ML-DSA-65 seed.
    fn from_seed(seed: &[u8; KEY_GENERATION_RANDOMNESS_SIZE]) -> (Self, [u8; ML_DSA_65_KEY_BYTES]) {
        let pair = ml_dsa_65::generate_key_pair(*seed);
        (Self(pair.signing_key), *pair.verification_key.as_ref())
    }

    /// Signs a TLS handshake message, hedged with fresh `randomness`.
    pub(crate) fn sign(
        &self,
        message: &[u8],
        randomness: [u8; SIGNING_RANDOMNESS_SIZE],
    ) -> Result<[u8; ML_DSA_65_SIGNATURE_BYTES], SignatureError> {
        self.sign_in_context(message, ML_DSA_CONTEXT, randomness)
    }

    /// Signs the bytes a discovery record's ML-DSA signature covers (ID-7),
    /// hedged with fresh `randomness`.
    pub(crate) fn sign_record(
        &self,
        message: &[u8],
        randomness: [u8; SIGNING_RANDOMNESS_SIZE],
    ) -> Result<[u8; ML_DSA_65_SIGNATURE_BYTES], SignatureError> {
        self.sign_in_context(message, ML_DSA_RECORD_CONTEXT, randomness)
    }

    /// Signs under `context`, with all-zero `randomness` selecting the
    /// deterministic variant. Fails for contexts over 255 bytes or if
    /// libcrux exhausts its rejection-sampling attempts.
    fn sign_in_context(
        &self,
        message: &[u8],
        context: &[u8],
        mut randomness: [u8; SIGNING_RANDOMNESS_SIZE],
    ) -> Result<[u8; ML_DSA_65_SIGNATURE_BYTES], SignatureError> {
        let signature = ml_dsa_65::sign(&self.0, message, context, randomness);
        randomness.zeroize();
        Ok(*signature.map_err(|_| SignatureError)?.as_ref())
    }
}

/// Verifies a composite `signature` over `message`: the Ed25519 half under
/// `ed25519`'s strict rules (ID-6) and the ML-DSA-65 half under `ml_dsa`.
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
    ed25519.verify_strict(message, classical)?;
    ml_dsa_verify_in_context(ml_dsa, message, ML_DSA_CONTEXT, post_quantum)
}

/// Verifies a discovery record's ML-DSA-65 `signature` over `message`, under
/// the record context, with the encoded public key `ml_dsa` (ID-7). The
/// caller has checked that the key is the one the record's key list names.
pub(crate) fn verify_record(
    ml_dsa: &[u8; ML_DSA_65_KEY_BYTES],
    message: &[u8],
    signature: &[u8],
) -> Result<(), SignatureError> {
    ml_dsa_verify_in_context(ml_dsa, message, ML_DSA_RECORD_CONTEXT, signature)
}

/// FIPS 204 ML-DSA.Verify of `signature` over `message` under `context`,
/// with the encoded ML-DSA-65 public key `ml_dsa`. A key or signature of
/// another length, or a context over 255 bytes, fails.
fn ml_dsa_verify_in_context(
    ml_dsa: &[u8],
    message: &[u8],
    context: &[u8],
    signature: &[u8],
) -> Result<(), SignatureError> {
    let ml_dsa: [u8; ML_DSA_65_KEY_BYTES] = ml_dsa.try_into().map_err(|_| SignatureError)?;
    let signature: [u8; ML_DSA_65_SIGNATURE_BYTES] =
        signature.try_into().map_err(|_| SignatureError)?;
    ml_dsa_65::verify(
        &MLDSA65VerificationKey::new(ml_dsa),
        message,
        context,
        &MLDSA65Signature::new(signature),
    )
    .map_err(|_| SignatureError)
}

#[cfg(test)]
mod interop;
#[cfg(test)]
mod vectors;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeSecretKey;

    const SEED: [u8; KEY_LENGTH] = [7; KEY_LENGTH];

    #[test]
    fn a_composite_signature_verifies_only_whole() {
        let secret = NodeSecretKey::from_bytes(&SEED);
        let ml_dsa = secret.credential().key(ML_DSA_65_KEY_TAG).unwrap();
        let signature = secret.sign_composite(b"transcript", [9; 32]).unwrap();
        assert_eq!(signature.len(), COMPOSITE_SIGNATURE_BYTES);
        assert_eq!(
            verify(&secret.public_key(), ml_dsa, b"transcript", &signature),
            Ok(())
        );

        assert!(verify(&secret.public_key(), ml_dsa, b"another", &signature).is_err());
        for flipped in [0, ED25519_SIGNATURE_BYTES, COMPOSITE_SIGNATURE_BYTES - 1] {
            let mut tampered = signature.clone();
            tampered[flipped] ^= 1;
            assert!(
                verify(&secret.public_key(), ml_dsa, b"transcript", &tampered).is_err(),
                "byte {flipped} tampered"
            );
        }
        assert!(verify(
            &secret.public_key(),
            ml_dsa,
            b"transcript",
            &signature[..ED25519_SIGNATURE_BYTES]
        )
        .is_err());
    }

    #[test]
    fn each_half_is_checked_against_its_own_key() {
        let secret = NodeSecretKey::from_bytes(&SEED);
        let other = NodeSecretKey::from_bytes(&[8; KEY_LENGTH]);
        let signature = secret.sign_composite(b"transcript", [9; 32]).unwrap();
        assert!(verify(
            &other.public_key(),
            secret.credential().key(ML_DSA_65_KEY_TAG).unwrap(),
            b"transcript",
            &signature
        )
        .is_err());
        assert!(verify(
            &secret.public_key(),
            other.credential().key(ML_DSA_65_KEY_TAG).unwrap(),
            b"transcript",
            &signature
        )
        .is_err());
    }

    #[test]
    fn the_ml_dsa_key_derives_from_the_seed_alone() {
        assert_eq!(
            MlDsaSigningKey::from_identity_seed(&SEED).1,
            MlDsaSigningKey::from_identity_seed(&SEED).1
        );
        assert_ne!(
            MlDsaSigningKey::from_identity_seed(&SEED).1,
            MlDsaSigningKey::from_identity_seed(&[8; KEY_LENGTH]).1
        );
        // Computed with Python's `hashlib.blake2b(b"", digest_size=32,
        // key=bytes([7] * 32), person=b"Zakura_MLDSA65v1")`.
        assert_eq!(
            hex::encode(ml_dsa_seed(&SEED)),
            "bff8a2ef4140474cd7814ae25071788c5a2edb3e1dbc0c1769a8a38c19b3c77c"
        );
    }

    /// A node signs handshakes and records with one ML-DSA-65 key. The two
    /// contexts keep each signature out of the other's place.
    #[test]
    fn a_handshake_signature_and_a_record_signature_do_not_swap() {
        assert_ne!(ML_DSA_CONTEXT, ML_DSA_RECORD_CONTEXT);
        // FIPS 204 allows a context of at most 255 bytes.
        assert!(ML_DSA_CONTEXT.len() <= 255 && ML_DSA_RECORD_CONTEXT.len() <= 255);

        let secret = NodeSecretKey::from_bytes(&SEED);
        let ml_dsa = secret.credential().key(ML_DSA_65_KEY_TAG).unwrap();
        let message = b"the same bytes under both contexts";

        let handshake = secret.sign_composite(message, [9; 32]).unwrap();
        assert_eq!(
            verify(&secret.public_key(), ml_dsa, message, &handshake),
            Ok(())
        );
        let record_key: &[u8; ML_DSA_65_KEY_BYTES] = ml_dsa.try_into().unwrap();
        assert!(verify_record(record_key, message, &handshake[ED25519_SIGNATURE_BYTES..]).is_err());

        let record = secret.sign_record_ml_dsa_65(message, [9; 32]).unwrap();
        assert_eq!(verify_record(record_key, message, &record), Ok(()));
        let mut swapped = handshake[..ED25519_SIGNATURE_BYTES].to_vec();
        swapped.extend_from_slice(&record);
        assert!(verify(&secret.public_key(), ml_dsa, message, &swapped).is_err());
    }

    #[test]
    fn hedged_signatures_differ_and_both_verify() {
        let secret = NodeSecretKey::from_bytes(&SEED);
        let ml_dsa = secret.credential().key(ML_DSA_65_KEY_TAG).unwrap();
        let first = secret.sign_composite(b"m", [1; 32]).unwrap();
        let second = secret.sign_composite(b"m", [2; 32]).unwrap();
        assert_ne!(
            first[ED25519_SIGNATURE_BYTES..],
            second[ED25519_SIGNATURE_BYTES..]
        );
        for signature in [first, second] {
            assert_eq!(
                verify(&secret.public_key(), ml_dsa, b"m", &signature),
                Ok(())
            );
        }
    }
}
