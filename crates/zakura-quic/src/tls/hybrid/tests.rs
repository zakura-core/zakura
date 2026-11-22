//! X25519MLKEM768: the layout of its shares and secret, refused inputs, the
//! plain X25519 fallback and agreement with another implementation's
//! recorded answers.

use curve25519_dalek::montgomery::MontgomeryPoint;
use libcrux_ml_kem::mlkem768::{self, MlKem768PublicKey};
use rustls::crypto::GetRandomFailed;
use serde::Deserialize;

use super::*;

/// The dialer's share: an ML-KEM encapsulation key, then an X25519 key.
const DIALER_SHARE_LEN: usize = 1216;

/// The listener's share: an ML-KEM ciphertext, then an X25519 key.
const LISTENER_SHARE_LEN: usize = 1120;

/// The hybrid secret: 32 bytes from each half.
const SECRET_LEN: usize = 64;

const INVALID_KEY_SHARE: rustls::Error =
    rustls::Error::PeerMisbehaved(PeerMisbehaved::InvalidKeyShare);

/// A random source that fills every draw with one byte.
#[derive(Debug)]
struct Fixed(u8);

impl SecureRandom for Fixed {
    fn fill(&self, buf: &mut [u8]) -> Result<(), GetRandomFailed> {
        buf.fill(self.0);
        Ok(())
    }
}

/// A random source that always fails.
#[derive(Debug)]
struct Failing;

impl SecureRandom for Failing {
    fn fill(&self, _: &mut [u8]) -> Result<(), GetRandomFailed> {
        Err(GetRandomFailed)
    }
}

/// The byte [`FIXED`] fills ML-KEM's draws with.
const FIXED_BYTE: u8 = 0x42;

static FIXED: Fixed = Fixed(FIXED_BYTE);
static FAILING: Failing = Failing;

/// X25519 whose every key pair has one fixed scalar, computed with
/// curve25519-dalek, for known answers.
#[derive(Debug)]
struct FixedX25519([u8; 32]);

impl SupportedKxGroup for FixedX25519 {
    fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, rustls::Error> {
        let (scalar, public) = x25519_pair(self.0);
        Ok(Box::new(FixedX25519Exchange { scalar, public }))
    }

    fn name(&self) -> NamedGroup {
        NamedGroup::X25519
    }
}

/// One [`FixedX25519`] exchange.
struct FixedX25519Exchange {
    scalar: [u8; 32],
    public: [u8; 32],
}

impl ActiveKeyExchange for FixedX25519Exchange {
    fn complete(self: Box<Self>, peer: &[u8]) -> Result<SharedSecret, rustls::Error> {
        Ok(SharedSecret::from(&x25519_secret(self.scalar, peer)[..]))
    }

    fn pub_key(&self) -> &[u8] {
        &self.public
    }

    fn group(&self) -> NamedGroup {
        NamedGroup::X25519
    }
}

/// The scalar [`FIXED_X25519`] uses.
const X25519_SCALAR: [u8; 32] = [0x5a; 32];

static FIXED_X25519: FixedX25519 = FixedX25519(X25519_SCALAR);

/// ring's X25519, the production classical half.
fn ring_x25519() -> &'static dyn SupportedKxGroup {
    rustls::crypto::ring::kx_group::X25519
}

/// The hybrid whose ML-KEM half draws [`FIXED_BYTE`]s. Its X25519 half is
/// ring's, which draws from the operating system.
fn fixed_mlkem() -> X25519MlKem768 {
    X25519MlKem768::new(ring_x25519(), &FIXED)
}

/// An X25519 key pair computed with curve25519-dalek, independently of ring.
fn x25519_pair(scalar: [u8; 32]) -> ([u8; 32], [u8; 32]) {
    (scalar, MontgomeryPoint::mul_base_clamped(scalar).to_bytes())
}

/// The X25519 secret of `scalar` and a peer's public key.
fn x25519_secret(scalar: [u8; 32], peer: &[u8]) -> [u8; 32] {
    MontgomeryPoint(peer.try_into().unwrap())
        .mul_clamped(scalar)
        .to_bytes()
}

/// One full exchange between `dialer` and `listener`. Returns both sides'
/// secrets, which the caller compares.
fn exchange(dialer: &dyn SupportedKxGroup, listener: &dyn SupportedKxGroup) -> (Vec<u8>, Vec<u8>) {
    let started = dialer.start().unwrap();
    assert_eq!(started.group(), NamedGroup::X25519MLKEM768);
    assert_eq!(started.pub_key().len(), DIALER_SHARE_LEN);
    let answered = listener.start_and_complete(started.pub_key()).unwrap();
    assert_eq!(answered.group, NamedGroup::X25519MLKEM768);
    assert_eq!(answered.pub_key.len(), LISTENER_SHARE_LEN);
    let dialer_secret = started.complete(&answered.pub_key).unwrap();
    (
        dialer_secret.secret_bytes().to_vec(),
        answered.secret.secret_bytes().to_vec(),
    )
}

/// Two production endpoints derive the same 64-byte secret, fresh for each
/// exchange.
#[test]
fn both_sides_agree_on_the_secret() {
    let group = x25519_mlkem768();
    let (dialer, listener) = exchange(group, group);
    assert_eq!(dialer, listener);
    assert_eq!(dialer.len(), SECRET_LEN);
    let (again, _) = exchange(group, group);
    assert_ne!(again, dialer);
}

/// The dialer's share is the ML-KEM encapsulation key followed by the
/// X25519 key, and its secret is the ML-KEM secret followed by the X25519
/// secret. The listener here is libcrux and curve25519-dalek used directly.
#[test]
fn the_dialers_share_and_secret_are_the_ml_kem_part_then_the_x25519_part() {
    let dialer = fixed_mlkem().start().unwrap();
    let share = dialer.pub_key().to_vec();
    let (encapsulation_key, dialer_x25519) = share.split_at(mlkem::ENCAPSULATION_KEY_LEN);
    assert_eq!(
        encapsulation_key,
        mlkem768::generate_key_pair([FIXED_BYTE; 64]).pk()
    );

    let (ciphertext, mlkem768_secret) = mlkem768::encapsulate(
        &MlKem768PublicKey::try_from(encapsulation_key).unwrap(),
        [7; 32],
    );
    let (scalar, listener_x25519) = x25519_pair([9; 32]);
    let listener_share = [ciphertext.as_slice().as_slice(), &listener_x25519].concat();
    assert_eq!(listener_share.len(), LISTENER_SHARE_LEN);

    let secret = dialer.complete(&listener_share).unwrap();
    assert_eq!(
        secret.secret_bytes(),
        [mlkem768_secret, x25519_secret(scalar, dialer_x25519)].concat()
    );
}

/// The listener's share is the ML-KEM ciphertext followed by the X25519
/// key, and its secret has the same order. The dialer here is libcrux and
/// curve25519-dalek used directly.
#[test]
fn the_listeners_share_and_secret_are_the_ml_kem_part_then_the_x25519_part() {
    let key_pair = mlkem768::generate_key_pair([5; 64]);
    let (scalar, dialer_x25519) = x25519_pair([3; 32]);
    let dialer_share = [key_pair.pk().as_slice(), &dialer_x25519].concat();
    assert_eq!(dialer_share.len(), DIALER_SHARE_LEN);

    let answered = fixed_mlkem().start_and_complete(&dialer_share).unwrap();
    let (ciphertext, listener_x25519) = answered.pub_key.split_at(mlkem::CIPHERTEXT_LEN);
    let (expected_ciphertext, _) = mlkem768::encapsulate(key_pair.public_key(), [FIXED_BYTE; 32]);
    assert_eq!(ciphertext, expected_ciphertext.as_slice());

    let mlkem768_secret = mlkem768::decapsulate(
        key_pair.private_key(),
        &mlkem768::MlKem768Ciphertext::try_from(ciphertext).unwrap(),
    );
    assert_eq!(
        answered.secret.secret_bytes(),
        [mlkem768_secret, x25519_secret(scalar, listener_x25519)].concat()
    );
}

/// A share whose total length is off, in either direction, is refused. That
/// includes a share holding only one of the two parts.
#[test]
fn shares_of_the_wrong_length_are_refused() {
    let group = x25519_mlkem768();
    for len in [
        0,
        X25519_LEN,
        mlkem::ENCAPSULATION_KEY_LEN,
        DIALER_SHARE_LEN - 1,
        DIALER_SHARE_LEN + 1,
    ] {
        let mut share = group.start().unwrap().pub_key().to_vec();
        share.resize(len, 0);
        assert_eq!(
            group.start_and_complete(&share).err(),
            Some(INVALID_KEY_SHARE),
            "dialer share of {len} bytes"
        );
    }
    for len in [
        0,
        X25519_LEN,
        mlkem::CIPHERTEXT_LEN,
        LISTENER_SHARE_LEN - 1,
        LISTENER_SHARE_LEN + 1,
    ] {
        let dialer = group.start().unwrap();
        let mut share = group.start_and_complete(dialer.pub_key()).unwrap().pub_key;
        share.resize(len, 0);
        assert_eq!(
            dialer.complete(&share).err(),
            Some(INVALID_KEY_SHARE),
            "listener share of {len} bytes"
        );
    }
}

/// One bad half fails the exchange even when the other half is fine: an
/// ML-KEM key with a coefficient above the modulus, and an X25519 share
/// that gives the all-zero secret.
#[test]
fn a_bad_half_fails_the_exchange() {
    let group = x25519_mlkem768();
    let good = group.start().unwrap().pub_key().to_vec();

    // The first 12-bit coefficient becomes 4095, above the modulus 3329.
    let mut bad_mlkem768 = good.clone();
    bad_mlkem768[..2].fill(0xff);
    assert_eq!(
        group.start_and_complete(&bad_mlkem768).err(),
        Some(INVALID_KEY_SHARE)
    );

    let mut zero_x25519 = good.clone();
    zero_x25519[mlkem::ENCAPSULATION_KEY_LEN..].fill(0);
    assert_eq!(
        group.start_and_complete(&zero_x25519).err(),
        Some(INVALID_KEY_SHARE)
    );

    let dialer = group.start().unwrap();
    let mut zero_x25519 = group.start_and_complete(dialer.pub_key()).unwrap().pub_key;
    zero_x25519[mlkem::CIPHERTEXT_LEN..].fill(0);
    assert_eq!(dialer.complete(&zero_x25519).err(), Some(INVALID_KEY_SHARE));
}

/// An altered ML-KEM ciphertext changes the ML-KEM half of the dialer's
/// secret and nothing else, so the two sides end with different secrets and
/// the handshake fails at its first authenticated message.
#[test]
fn an_altered_ciphertext_gives_the_dialer_another_secret() {
    let group = x25519_mlkem768();
    let dialer = group.start().unwrap();
    let answered = group.start_and_complete(dialer.pub_key()).unwrap();
    let mut altered = answered.pub_key.clone();
    altered[0] ^= 1;
    let secret = dialer.complete(&altered).unwrap();
    let (dialer_mlkem768, dialer_x25519) = secret.secret_bytes().split_at(32);
    let (listener_mlkem768, listener_x25519) = answered.secret.secret_bytes().split_at(32);
    assert_ne!(dialer_mlkem768, listener_mlkem768);
    assert_eq!(dialer_x25519, listener_x25519);
}

/// A provider that also enables standalone X25519 can reuse the hybrid's
/// X25519 half for that exchange.
#[test]
fn the_x25519_half_completes_against_a_plain_x25519_listener() {
    let dialer = x25519_mlkem768().start().unwrap();
    let (group, share) = dialer.hybrid_component().unwrap();
    assert_eq!(group, NamedGroup::X25519);
    assert_eq!(share, &dialer.pub_key()[mlkem::ENCAPSULATION_KEY_LEN..]);

    let answered = ring_x25519().start_and_complete(share).unwrap();
    let secret = dialer.complete_hybrid_component(&answered.pub_key).unwrap();
    assert_eq!(secret.secret_bytes(), answered.secret.secret_bytes());
    assert_eq!(secret.secret_bytes().len(), X25519_LEN);
}

/// The group names itself X25519MLKEM768 and is offered with TLS 1.3 only.
#[test]
fn the_group_is_x25519mlkem768_for_tls13_only() {
    let group = x25519_mlkem768();
    assert_eq!(group.name(), NamedGroup::X25519MLKEM768);
    assert_eq!(u16::from(group.name()), 0x11ec);
    assert!(group.usable_for_version(ProtocolVersion::TLSv1_3));
    assert!(!group.usable_for_version(ProtocolVersion::TLSv1_2));
    assert!(group.ffdhe_group().is_none());
    assert!(group.start().unwrap().ffdhe_group().is_none());
}

/// When ML-KEM's random source fails, neither side produces a share.
#[test]
fn a_failed_random_source_fails_the_exchange() {
    let failing = X25519MlKem768::new(ring_x25519(), &FAILING);
    assert_eq!(
        failing.start().err(),
        Some(rustls::Error::FailedToGetRandomBytes)
    );
    let share = x25519_mlkem768().start().unwrap().pub_key().to_vec();
    assert_eq!(
        failing.start_and_complete(&share).err(),
        Some(rustls::Error::FailedToGetRandomBytes)
    );
}

/// Exchanges an independent X25519MLKEM768 implementation recorded with
/// our group, one in each role. Shares and secrets are hex, and the file's
/// `comment` says how they were recorded.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Recorded {
    /// The group's TLS code point.
    group: u16,
    /// The independent implementation dialed, and `secret` is its dialer's.
    they_dial: RecordedExchange,
    /// The independent implementation listened, and `secret` is its
    /// listener's.
    they_listen: RecordedExchange,
}

/// One recorded exchange.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecordedExchange {
    #[serde(deserialize_with = "from_hex")]
    dialer_share: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    listener_share: Vec<u8>,
    #[serde(deserialize_with = "from_hex")]
    secret: Vec<u8>,
}

/// Decodes a hex string field.
fn from_hex<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    hex::decode(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

/// Our group, fed the shares an independent implementation recorded, gives
/// back the shares it answered with and the secrets it derived, in both
/// roles. So the shares and the secret follow the standard layout and not
/// only our own reading of it.
///
/// Our side is the production combiner and ML-KEM with every draw fixed, as
/// when the answers were recorded. Its X25519 half is [`FixedX25519`],
/// because ring's draws from the operating system.
#[test]
fn the_group_agrees_with_an_independent_implementation() {
    let recorded: Recorded =
        serde_json::from_str(include_str!("independent_answers.json")).unwrap();
    let ours = X25519MlKem768::new(&FIXED_X25519, &FIXED);
    assert_eq!(u16::from(ours.name()), recorded.group);

    let they_dial = recorded.they_dial;
    let answered = ours.start_and_complete(&they_dial.dialer_share).unwrap();
    assert_eq!(answered.pub_key, they_dial.listener_share);
    assert_eq!(answered.secret.secret_bytes(), they_dial.secret);

    let they_listen = recorded.they_listen;
    let dialer = ours.start().unwrap();
    assert_eq!(dialer.pub_key(), they_listen.dialer_share);
    let secret = dialer.complete(&they_listen.listener_share).unwrap();
    assert_eq!(secret.secret_bytes(), they_listen.secret);
}
