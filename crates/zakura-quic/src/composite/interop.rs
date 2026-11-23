//! The composite's ML-DSA-44 half against an independent implementation,
//! RustCrypto's `ml-dsa`, through bytes it produced once, offline.
//!
//! `rustcrypto_ml_dsa_44.json` was recorded on 2026-11-22 by a throwaway
//! program outside this repository, built on `ml-dsa` 0.1.2 from crates.io
//! with default features off. It isn't a dependency here. From the file's
//! `mlDsaSeed` (the ID-13 seed of the node seed `seed`) the program took
//! `SigningKey::<MlDsa44>::from_seed`, and recorded:
//!
//! - `publicKey`: `verifying_key().encode()`;
//! - the hedged signature: `expanded_key().sign_internal(&[&[0, 35], CONTEXT,
//!   message], &rnd)`, FIPS 204 Algorithm 2 with the given randomness, where
//!   35 is the context's length;
//! - the deterministic signature: `expanded_key().sign_deterministic(message,
//!   CONTEXT)`.
//!
//! `CONTEXT` is the TLS-4 context. The same program also checked our side:
//! `verify_with_context` accepted the ML-DSA half of
//! `ed25519_mldsa44_kat.json` under `CONTEXT` and refused it under the empty
//! context, and `sign_internal` with that file's randomness reproduced the
//! half byte for byte.

use serde::Deserialize;

use super::*;
use crate::NodeSecretKey;

/// The recorded file.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Recorded {
    seed: String,
    ml_dsa_seed: String,
    public_key: String,
    context: String,
    signatures: Vec<RecordedSignature>,
}

/// One recorded signature over `message`, with the randomness it was hedged
/// with (all zeros for the deterministic one).
#[derive(Deserialize)]
struct RecordedSignature {
    comment: String,
    message: String,
    rnd: String,
    signature: String,
}

fn recorded() -> Recorded {
    serde_json::from_str(include_str!("rustcrypto_ml_dsa_44.json")).unwrap()
}

/// Decodes a hex field of a fixed length.
fn array<const N: usize>(text: &str) -> [u8; N] {
    hex::decode(text).unwrap().try_into().unwrap()
}

/// Both implementations derive the same ML-DSA-44 public key from the same
/// seed, and the recorded context is the one the composite signs under.
#[test]
fn rustcrypto_derives_the_same_public_key() {
    let recorded = recorded();
    let seed: [u8; KEY_LENGTH] = array(&recorded.seed);
    let ml_seed = ml_dsa_seed(&seed);
    assert_eq!(hex::encode(ml_seed), recorded.ml_dsa_seed);
    assert_eq!(
        hex::encode(ml_dsa_public_key(&ml_seed)),
        recorded.public_key
    );
    assert_eq!(recorded.context.as_bytes(), ML_DSA_CONTEXT);
}

/// A RustCrypto signature under the TLS-4 context verifies as the ML-DSA
/// half of a composite signature, and the composite's own signer, given the
/// same randomness, makes the same bytes.
#[test]
fn rustcrypto_signatures_verify_and_match_ours() {
    let recorded = recorded();
    let secret = NodeSecretKey::from_bytes(&array(&recorded.seed));
    let ml_dsa = hex::decode(&recorded.public_key).unwrap();
    assert_eq!(recorded.signatures.len(), 2);
    for entry in recorded.signatures {
        let name = entry.comment;
        let message = hex::decode(&entry.message).unwrap();
        let theirs = hex::decode(&entry.signature).unwrap();

        let mut composite = secret.sign(&message).to_vec();
        composite.extend_from_slice(&theirs);
        assert_eq!(
            verify(&secret.public_key(), &ml_dsa, &message, &composite),
            Ok(()),
            "{name}"
        );

        let ours = ml_dsa_sign(
            &ml_dsa_seed(&secret.to_bytes()),
            &message,
            array(&entry.rnd),
        )
        .unwrap();
        assert_eq!(ours[..], theirs, "{name}");
    }
}
