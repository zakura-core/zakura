//! ML-KEM-768: known answers, refused inputs and the random draws.
//!
//! The known answers are a subset of Project Wycheproof's ML-KEM-768 files
//! (`testvectors_v1/mlkem_768_test.json` and `mlkem_768_encaps_test.json` at
//! <https://github.com/C2SP/wycheproof>, commit 65ca4947, Apache-2.0). The
//! files under `wycheproof/` keep the upstream layout with fewer tests.

use std::{collections::VecDeque, sync::Mutex};

use rustls::crypto::GetRandomFailed;
use serde::Deserialize;

use super::*;

/// A Wycheproof file: its test groups.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorFile {
    test_groups: Vec<VectorGroup>,
}

/// One group of a Wycheproof file.
#[derive(Deserialize)]
struct VectorGroup {
    tests: Vec<Vector>,
}

/// One Wycheproof test. A file sets the fields its test type has.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vector {
    tc_id: u32,
    /// The key generation seed, `d` then `z`.
    #[serde(default, deserialize_with = "from_hex")]
    seed: Vec<u8>,
    /// The encapsulation randomness.
    #[serde(default, deserialize_with = "from_hex")]
    m: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    ek: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    c: Vec<u8>,
    #[serde(rename = "K", deserialize_with = "from_hex")]
    k: Vec<u8>,
    result: String,
}

/// Decodes a hex string field.
fn from_hex<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    hex::decode(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

/// The tests of one Wycheproof file.
fn vectors(json: &str) -> Vec<Vector> {
    let file: VectorFile = serde_json::from_str(json).unwrap();
    file.test_groups
        .into_iter()
        .flat_map(|group| group.tests)
        .collect()
}

/// A random source that hands out the queued draws in order. A draw of
/// another length than the next queued one fails, and so does a draw from
/// an empty queue.
#[derive(Debug, Default)]
struct Script(Mutex<VecDeque<Vec<u8>>>);

impl Script {
    /// A leaked script holding `draws`, since [`MlKem768`] wants `'static`.
    fn leaked<const N: usize>(draws: [&[u8]; N]) -> &'static Self {
        let draws = draws.iter().map(|draw| draw.to_vec()).collect();
        Box::leak(Box::new(Self(Mutex::new(draws))))
    }

    /// How many queued draws nobody took.
    fn left(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

impl SecureRandom for Script {
    fn fill(&self, buf: &mut [u8]) -> Result<(), GetRandomFailed> {
        let mut draws = self.0.lock().unwrap();
        match draws.front() {
            Some(draw) if draw.len() == buf.len() => {
                buf.copy_from_slice(draw);
                draws.pop_front();
                Ok(())
            }
            _ => Err(GetRandomFailed),
        }
    }
}

/// ML-KEM-768 on the operating system's random source.
fn system() -> MlKem768 {
    MlKem768::new(rustls::crypto::ring::default_provider().secure_random)
}

const INVALID_KEY_SHARE: rustls::Error =
    rustls::Error::PeerMisbehaved(PeerMisbehaved::InvalidKeyShare);

/// FIPS 203 key generation and decapsulation: the seed gives the published
/// encapsulation key, and the ciphertext gives the published secret. That
/// covers ciphertexts altered after encapsulation, where the secret is the
/// implicit-rejection value. A ciphertext of the wrong length is refused.
#[test]
fn key_generation_and_decapsulation_match_wycheproof() {
    let vectors = vectors(include_str!("wycheproof/mlkem_768_test.json"));
    assert_eq!(vectors.len(), 8);
    for vector in vectors {
        let id = vector.tc_id;
        let random = Script::leaked([&vector.seed]);
        let key = MlKem768::new(random).generate().unwrap();
        assert_eq!(random.left(), 0, "test {id}");
        assert_eq!(key.encapsulation_key().as_slice(), vector.ek, "test {id}");
        match vector.result.as_str() {
            "valid" => assert_eq!(
                key.decapsulate(&vector.c).unwrap().as_slice(),
                vector.k,
                "test {id}"
            ),
            "invalid" => {
                assert_ne!(vector.c.len(), CIPHERTEXT_LEN, "test {id}");
                assert_eq!(
                    key.decapsulate(&vector.c).unwrap_err(),
                    INVALID_KEY_SHARE,
                    "test {id}"
                );
            }
            other => panic!("test {id} has result {other}"),
        }
    }
}

/// FIPS 203 encapsulation: the key and the randomness give the published
/// ciphertext and secret. A key of the wrong length, or one with a
/// coefficient at or above the modulus, is refused before any randomness is
/// drawn.
#[test]
fn encapsulation_matches_wycheproof() {
    let vectors = vectors(include_str!("wycheproof/mlkem_768_encaps_test.json"));
    assert_eq!(vectors.len(), 11);
    let mut refused = 0;
    for vector in vectors {
        let id = vector.tc_id;
        let random = Script::leaked([&vector.m]);
        let result = MlKem768::new(random).encapsulate(&vector.ek);
        match vector.result.as_str() {
            "valid" => {
                let (ciphertext, secret) = result.unwrap();
                assert_eq!(ciphertext.as_slice(), vector.c, "test {id}");
                assert_eq!(secret.as_slice(), vector.k, "test {id}");
                assert_eq!(random.left(), 0, "test {id}");
            }
            "invalid" => {
                assert_eq!(result.unwrap_err(), INVALID_KEY_SHARE, "test {id}");
                assert_eq!(random.left(), 1, "test {id} drew randomness");
                refused += 1;
            }
            other => panic!("test {id} has result {other}"),
        }
    }
    assert_eq!(refused, 8);
}

/// Both sides derive the same secret, and the shares have the lengths the
/// hybrid layout splits on.
#[test]
fn both_sides_agree_on_the_secret() {
    let mlkem = system();
    let key = mlkem.generate().unwrap();
    assert_eq!(key.encapsulation_key().len(), ENCAPSULATION_KEY_LEN);
    let (ciphertext, listener_secret) = mlkem.encapsulate(key.encapsulation_key()).unwrap();
    assert_eq!(ciphertext.len(), CIPHERTEXT_LEN);
    let dialer_secret = key.decapsulate(&ciphertext).unwrap();
    assert_eq!(*dialer_secret, *listener_secret);

    // Fresh randomness each time.
    let other = mlkem.generate().unwrap();
    assert_ne!(other.encapsulation_key(), key.encapsulation_key());
    let (again, _) = mlkem.encapsulate(key.encapsulation_key()).unwrap();
    assert_ne!(again, ciphertext);
}

/// A share that is empty, one byte short or one byte long is refused in
/// both directions.
#[test]
fn shares_of_the_wrong_length_are_refused() {
    let mlkem = system();
    let key = mlkem.generate().unwrap();
    let (ciphertext, _) = mlkem.encapsulate(key.encapsulation_key()).unwrap();
    for len in [0, ENCAPSULATION_KEY_LEN - 1, ENCAPSULATION_KEY_LEN + 1] {
        let mut share = key.encapsulation_key().to_vec();
        share.resize(len, 0);
        assert_eq!(
            mlkem.encapsulate(&share).unwrap_err(),
            INVALID_KEY_SHARE,
            "encapsulation key of {len} bytes"
        );
    }
    for len in [0, CIPHERTEXT_LEN - 1, CIPHERTEXT_LEN + 1] {
        let mut share = ciphertext.to_vec();
        share.resize(len, 0);
        assert_eq!(
            key.decapsulate(&share).unwrap_err(),
            INVALID_KEY_SHARE,
            "ciphertext of {len} bytes"
        );
    }
}

/// A ciphertext altered anywhere still decapsulates, to a secret unrelated
/// to the listener's. Nothing tells the two apart but the failed handshake.
#[test]
fn an_altered_ciphertext_gives_another_secret() {
    let mlkem = system();
    let key = mlkem.generate().unwrap();
    let (ciphertext, listener_secret) = mlkem.encapsulate(key.encapsulation_key()).unwrap();
    for position in [0, CIPHERTEXT_LEN / 2, CIPHERTEXT_LEN - 1] {
        let mut altered = ciphertext;
        altered[position] ^= 1;
        let secret = key.decapsulate(&altered).unwrap();
        assert_ne!(*secret, *listener_secret, "byte {position}");
        // The same wrong ciphertext always gives the same secret.
        assert_eq!(*key.decapsulate(&altered).unwrap(), *secret);
    }
}

/// When the random source fails, neither side produces a share.
#[test]
fn a_failed_random_source_fails_the_exchange() {
    let failing = MlKem768::new(Script::leaked([]));
    assert_eq!(
        failing.generate().err(),
        Some(rustls::Error::FailedToGetRandomBytes)
    );
    let key = system().generate().unwrap();
    assert_eq!(
        failing.encapsulate(key.encapsulation_key()).unwrap_err(),
        rustls::Error::FailedToGetRandomBytes
    );
}

/// Each operation takes one draw: 64 bytes for a key pair and 32 bytes for
/// an encapsulation.
#[test]
fn each_operation_takes_one_draw() {
    let random = Script::leaked([&[1; 64], &[2; 32]]);
    let mlkem = MlKem768::new(random);
    let key = mlkem.generate().unwrap();
    assert_eq!(random.left(), 1);
    mlkem.encapsulate(key.encapsulation_key()).unwrap();
    assert_eq!(random.left(), 0);
}
