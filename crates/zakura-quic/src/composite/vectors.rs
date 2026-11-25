//! ML-DSA-65 against published known answers, through the composite's own
//! key generation, signing and verification.
//!
//! The vectors are subsets of two sources. The files keep the upstream
//! layout with fewer tests:
//!
//! - `wycheproof/`: Project Wycheproof's `testvectors_v1/mldsa_65_sign_seed_test.json`
//!   and `mldsa_65_verify_test.json` at <https://github.com/C2SP/wycheproof>,
//!   commit 65ca4947, Apache-2.0. The unused `privateKeyPkcs8` and
//!   `publicKeyDer` group fields are dropped.
//! - `acvp/`: the ML-DSA-65 groups of NIST's ACVP-Server
//!   `gen-val/json-files/ML-DSA-keyGen-FIPS204/internalProjection.json` and
//!   `ML-DSA-sigGen-FIPS204/internalProjection.json` at
//!   <https://github.com/usnistgov/ACVP-Server>, commit 975de31e. NIST
//!   software, provided by NIST as a public service; cut down to the tests
//!   kept here on 2026-11-25.
//!
//! A node signs with a key generated from a 32-byte seed, so the Wycheproof
//! "seed" vectors run through [`ml_dsa_sign_in_context`] and
//! [`ml_dsa_public_key`]. ACVP's signing vectors give an expanded private key
//! instead, which the composite never imports, so those sign with
//! `libcrux-ml-dsa` directly. Vectors flagged `Internal`, which give only
//! μ, and HashML-DSA ("preHash") vectors don't map onto the composite: it
//! signs pure ML-DSA over the message and a context.

use serde::Deserialize;

use super::*;
use crate::NodeSecretKey;

/// Decodes a hex string field.
fn from_hex<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    hex::decode(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

/// Decodes a hex string field of a fixed length.
fn from_hex_array<'de, D: serde::Deserializer<'de>, const N: usize>(
    deserializer: D,
) -> Result<[u8; N], D::Error> {
    from_hex(deserializer)?
        .try_into()
        .map_err(|bytes: Vec<u8>| serde::de::Error::invalid_length(bytes.len(), &"a fixed length"))
}

/// A Wycheproof or ACVP file: its test groups.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorFile<G> {
    test_groups: Vec<G>,
}

/// A Wycheproof `MlDsaSign` group: one key, generated from its seed.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignGroup {
    #[serde(deserialize_with = "from_hex_array")]
    private_seed: [u8; KEY_GENERATION_RANDOMNESS_SIZE],
    #[serde(deserialize_with = "from_hex")]
    public_key: Vec<u8>,
    tests: Vec<SignTest>,
}

/// A Wycheproof signing test. A missing `ctx` is the empty context, and a
/// missing `rnd` the deterministic variant's 32 zero bytes.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignTest {
    tc_id: u32,
    #[serde(deserialize_with = "from_hex")]
    msg: Vec<u8>,
    #[serde(default, deserialize_with = "from_hex")]
    ctx: Vec<u8>,
    #[serde(default, deserialize_with = "from_hex")]
    rnd: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    sig: Vec<u8>,
    result: String,
}

/// A Wycheproof `MlDsaVerify` group: one encoded public key, which may have
/// the wrong length.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VerifyGroup {
    #[serde(deserialize_with = "from_hex")]
    public_key: Vec<u8>,
    tests: Vec<VerifyTest>,
}

/// A Wycheproof verification test. A missing `ctx` is the empty context.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VerifyTest {
    tc_id: u32,
    #[serde(deserialize_with = "from_hex")]
    msg: Vec<u8>,
    #[serde(default, deserialize_with = "from_hex")]
    ctx: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    sig: Vec<u8>,
    result: String,
}

/// An ACVP group, with the tests of its mode.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcvpGroup<T> {
    parameter_set: String,
    tests: Vec<T>,
}

/// An ACVP ML-DSA key generation test.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcvpKeyGen {
    tc_id: u32,
    #[serde(deserialize_with = "from_hex_array")]
    seed: [u8; KEY_GENERATION_RANDOMNESS_SIZE],
    #[serde(deserialize_with = "from_hex")]
    pk: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    sk: Vec<u8>,
}

/// An ACVP ML-DSA signing test of the external, pure interface.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcvpSigGen {
    tc_id: u32,
    #[serde(deserialize_with = "from_hex")]
    message: Vec<u8>,
    #[serde(deserialize_with = "from_hex_array")]
    rnd: [u8; SIGNING_RANDOMNESS_SIZE],
    #[serde(deserialize_with = "from_hex")]
    pk: Vec<u8>,
    #[serde(deserialize_with = "from_hex_array")]
    sk: [u8; ml_dsa_65::MLDSA65SigningKey::len()],
    #[serde(deserialize_with = "from_hex")]
    context: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    signature: Vec<u8>,
}

/// The groups of one vector file.
fn groups<G: serde::de::DeserializeOwned>(json: &str) -> Vec<G> {
    serde_json::from_str::<VectorFile<G>>(json)
        .unwrap()
        .test_groups
}

/// FIPS 204 key generation from a seed gives Wycheproof's public key, and
/// signing with that key gives its signature, for a context of 0, 7 and 255
/// bytes and for hedged randomness. Each signature then verifies under its
/// own context. A 256-byte context is refused.
#[test]
fn seeded_keys_and_signatures_match_wycheproof() {
    let groups: Vec<SignGroup> = groups(include_str!("wycheproof/mldsa_65_sign_seed_test.json"));
    let mut count = 0;
    for group in groups {
        assert_eq!(ml_dsa_public_key(&group.private_seed), group.public_key[..]);
        for test in group.tests {
            count += 1;
            let id = test.tc_id;
            let mut rnd = [0; SIGNING_RANDOMNESS_SIZE];
            if !test.rnd.is_empty() {
                rnd.copy_from_slice(&test.rnd);
            }
            let signed = ml_dsa_sign_in_context(&group.private_seed, &test.msg, &test.ctx, rnd);
            match test.result.as_str() {
                "valid" => {
                    assert_eq!(signed.unwrap()[..], test.sig, "test {id}");
                    assert_eq!(
                        ml_dsa_verify_in_context(
                            &group.public_key,
                            &test.msg,
                            &test.ctx,
                            &test.sig
                        ),
                        Ok(()),
                        "test {id}"
                    );
                }
                "invalid" => {
                    assert!(test.ctx.len() > 255, "test {id}");
                    assert_eq!(signed, Err(SignatureError), "test {id}");
                }
                other => panic!("test {id} has result {other}"),
            }
        }
    }
    assert_eq!(count, 5);
}

/// FIPS 204 verification accepts Wycheproof's valid signature under its
/// context and refuses the invalid ones: a 256-byte context, a short
/// signature, a short public key, a flipped bit in the commitment hash, an
/// overflowing hint encoding and a response above the norm bound.
#[test]
fn verification_matches_wycheproof() {
    let groups: Vec<VerifyGroup> = groups(include_str!("wycheproof/mldsa_65_verify_test.json"));
    let mut count = 0;
    for group in groups {
        for test in group.tests {
            count += 1;
            let id = test.tc_id;
            let verified =
                ml_dsa_verify_in_context(&group.public_key, &test.msg, &test.ctx, &test.sig);
            match test.result.as_str() {
                "valid" => assert_eq!(verified, Ok(()), "test {id}"),
                "invalid" => assert_eq!(verified, Err(SignatureError), "test {id}"),
                other => panic!("test {id} has result {other}"),
            }
        }
    }
    assert_eq!(count, 7);
}

/// NIST's key generation answer: the seed gives the published public key
/// through [`ml_dsa_public_key`], and `libcrux-ml-dsa` the published private
/// key.
#[test]
fn key_generation_matches_acvp() {
    let groups: Vec<AcvpGroup<AcvpKeyGen>> =
        groups(include_str!("acvp/ML-DSA-keyGen-FIPS204.json"));
    let mut count = 0;
    for group in groups {
        assert_eq!(group.parameter_set, "ML-DSA-65");
        for test in group.tests {
            count += 1;
            let id = test.tc_id;
            assert_eq!(ml_dsa_public_key(&test.seed)[..], test.pk, "test {id}");
            let pair = ml_dsa_65::generate_key_pair(test.seed);
            assert_eq!(pair.signing_key.as_slice(), test.sk, "test {id}");
        }
    }
    assert_eq!(count, 1);
}

/// NIST's answer for hedged, pure ML-DSA with a context, the variant the
/// composite signs with: `libcrux-ml-dsa`, given the published private key
/// and randomness, gives the published signature, which verifies through
/// [`ml_dsa_verify_in_context`] under the published public key and context
/// and under no other context.
#[test]
fn hedged_signing_matches_acvp() {
    let groups: Vec<AcvpGroup<AcvpSigGen>> =
        groups(include_str!("acvp/ML-DSA-sigGen-FIPS204.json"));
    let mut count = 0;
    for group in groups {
        assert_eq!(group.parameter_set, "ML-DSA-65");
        for test in group.tests {
            count += 1;
            let id = test.tc_id;
            let key = ml_dsa_65::MLDSA65SigningKey::new(test.sk);
            let signature = ml_dsa_65::sign(&key, &test.message, &test.context, test.rnd).unwrap();
            assert_eq!(signature.as_slice(), test.signature, "test {id}");
            assert_eq!(
                ml_dsa_verify_in_context(&test.pk, &test.message, &test.context, &test.signature),
                Ok(()),
                "test {id}"
            );
            assert_eq!(
                ml_dsa_verify_in_context(&test.pk, &test.message, &[], &test.signature),
                Err(SignatureError),
                "test {id}"
            );
        }
    }
    assert_eq!(count, 1);
}

/// The published composite known answer (`ed25519_mldsa65_kat.json`, TLS-4).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompositeFile {
    scheme: String,
    ml_dsa_context: String,
    tests: Vec<CompositeTest>,
}

/// One composite known answer, made without this crate: its Ed25519 key and
/// signature, ML-DSA seed and node ID with Python's `cryptography` and
/// `hashlib`, and its ML-DSA key and signature with RustCrypto's `ml-dsa`
/// (see `interop.rs`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CompositeTest {
    #[serde(deserialize_with = "from_hex_array")]
    seed: [u8; KEY_LENGTH],
    #[serde(deserialize_with = "from_hex")]
    message: Vec<u8>,
    #[serde(deserialize_with = "from_hex_array")]
    ml_dsa_randomness: [u8; SIGNING_RANDOMNESS_SIZE],
    ed25519_public_key: String,
    ml_dsa_seed: String,
    #[serde(deserialize_with = "from_hex")]
    ml_dsa_public_key: Vec<u8>,
    node_id: String,
    #[serde(deserialize_with = "from_hex")]
    signature: Vec<u8>,
}

/// A fixed seed, message and hedging randomness give the published keys,
/// node ID and composite signature, which verifies.
#[test]
fn the_composite_has_its_published_known_answer() {
    let file: CompositeFile =
        serde_json::from_str(include_str!("ed25519_mldsa65_kat.json")).unwrap();
    assert_eq!(
        file.scheme,
        format!("0x{:04X}", u16::from(COMPOSITE_SCHEME))
    );
    assert_eq!(file.ml_dsa_context.as_bytes(), ML_DSA_CONTEXT);
    assert_eq!(file.tests.len(), 1);
    for test in file.tests {
        let secret = NodeSecretKey::from_bytes(&test.seed);
        assert_eq!(
            hex::encode(secret.public_key().as_bytes()),
            test.ed25519_public_key
        );
        assert_eq!(hex::encode(ml_dsa_seed(&test.seed)), test.ml_dsa_seed);
        assert_eq!(
            secret.credential().key(ML_DSA_65_KEY_TAG),
            Some(test.ml_dsa_public_key.as_slice())
        );
        assert_eq!(secret.public().to_string(), test.node_id);

        let signature = secret
            .sign_composite(&test.message, test.ml_dsa_randomness)
            .unwrap();
        assert_eq!(signature.len(), COMPOSITE_SIGNATURE_BYTES);
        assert_eq!(signature, test.signature);
        assert_eq!(
            verify(
                &secret.public_key(),
                &test.ml_dsa_public_key,
                &test.message,
                &test.signature
            ),
            Ok(())
        );
    }
}
