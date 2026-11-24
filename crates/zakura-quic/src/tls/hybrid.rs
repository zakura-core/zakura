//! X25519MLKEM1024, the hybrid key-exchange group (TLS-9).
//!
//! Both sides run X25519 and ML-KEM-1024, and TLS derives the session keys
//! from both secrets. The keys stay secret as long as either algorithm
//! holds, so a break of ML-KEM, or a wrong answer from its newer code, can't
//! make a connection's keys weaker than X25519 alone. That covers the keys,
//! not the code: a memory-safety fault in the ML-KEM library, which reads a
//! peer's share before the peer is authenticated, is outside it.
//!
//! No standard names this pairing: `draft-ietf-tls-ecdhe-mlkem` pairs
//! ML-KEM-1024 only with P-384. The group therefore takes a private-use code
//! point, and only Zakura nodes speak it. Its layout is the one the draft
//! fixes for X25519MLKEM768: each share and the secret are the ML-KEM part
//! followed by the X25519 part. The combiner follows rustls' own, which
//! ships only with its aws-lc-rs provider.

use std::sync::LazyLock;

use rustls::{
    crypto::{
        ActiveKeyExchange, CompletedKeyExchange, SecureRandom, SharedSecret, SupportedKxGroup,
    },
    ffdhe_groups::FfdheGroup,
    NamedGroup, PeerMisbehaved, ProtocolVersion,
};

use super::mlkem::{self, DecapsulationKey, MlKem1024};

/// The length of an X25519 share.
const X25519_LEN: usize = 32;

/// The group's code point, from the private-use range of RFC 8446 §4.2.7
/// (0xFE00 to 0xFEFF).
pub(crate) const X25519_MLKEM1024: NamedGroup = NamedGroup::Unknown(0xFE24);

/// The `group` label of an exchange completed in the hybrid. rustls has no
/// name for a private-use group.
pub(crate) const HYBRID_LABEL: &str = "X25519MLKEM1024";

/// The `group` label of an exchange completed in the hybrid's X25519 half
/// alone.
const X25519_LABEL: &str = "X25519";

/// The hybrid group production endpoints offer: ring's X25519, and
/// ML-KEM-1024 drawing from the same random source ring's provider uses.
pub(crate) fn x25519_mlkem1024() -> &'static dyn SupportedKxGroup {
    static GROUP: LazyLock<X25519MlKem1024> = LazyLock::new(|| {
        X25519MlKem1024::new(
            rustls::crypto::ring::kx_group::X25519,
            rustls::crypto::ring::default_provider().secure_random,
        )
    });
    &*GROUP
}

/// X25519MLKEM1024 over a given X25519 group and ML-KEM random source.
#[derive(Debug)]
pub(crate) struct X25519MlKem1024 {
    x25519: &'static dyn SupportedKxGroup,
    mlkem1024: MlKem1024,
}

impl X25519MlKem1024 {
    /// The hybrid of `x25519`, which must be an X25519 group, and ML-KEM-1024
    /// drawing from `mlkem_random`. Only debug builds check the group; with
    /// another one, every share fails the fixed 32-byte split.
    pub(crate) fn new(
        x25519: &'static dyn SupportedKxGroup,
        mlkem_random: &'static dyn SecureRandom,
    ) -> Self {
        // The share layout fixes a 32-byte classical half.
        debug_assert_eq!(x25519.name(), NamedGroup::X25519);
        Self {
            x25519,
            mlkem1024: MlKem1024::new(mlkem_random),
        }
    }
}

impl SupportedKxGroup for X25519MlKem1024 {
    /// Starts the dialer's side: an ML-KEM key pair and an X25519 key.
    fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, rustls::Error> {
        let mlkem1024 = self.mlkem1024.generate()?;
        let x25519 = self.x25519.start()?;
        let share = [mlkem1024.encapsulation_key(), x25519.pub_key()].concat();
        Ok(Box::new(Dialer {
            mlkem1024,
            x25519,
            share,
        }))
    }

    /// Runs the listener's side against the dialer's share. Both halves
    /// must succeed.
    fn start_and_complete(
        &self,
        dialer_share: &[u8],
    ) -> Result<CompletedKeyExchange, rustls::Error> {
        let (encapsulation_key, x25519_share) = split(dialer_share, mlkem::ENCAPSULATION_KEY_LEN)?;
        let (ciphertext, mlkem1024_secret) = self.mlkem1024.encapsulate(encapsulation_key)?;
        let x25519 = self.x25519.start_and_complete(x25519_share)?;
        count_completed(HYBRID_LABEL);
        Ok(CompletedKeyExchange {
            group: X25519_MLKEM1024,
            pub_key: [ciphertext.as_slice(), &x25519.pub_key].concat(),
            secret: combine(&mlkem1024_secret, &x25519.secret),
        })
    }

    /// Not a finite-field group.
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        None
    }

    /// X25519MLKEM1024.
    fn name(&self) -> NamedGroup {
        X25519_MLKEM1024
    }

    /// The group is defined for TLS 1.3 only.
    fn usable_for_version(&self, version: ProtocolVersion) -> bool {
        version == ProtocolVersion::TLSv1_3
    }
}

/// The dialer's side of one exchange, waiting for the listener's share.
struct Dialer {
    mlkem1024: DecapsulationKey,
    x25519: Box<dyn ActiveKeyExchange>,
    /// The share to send: the encapsulation key, then the X25519 key.
    share: Vec<u8>,
}

impl ActiveKeyExchange for Dialer {
    /// The combined secret with the listener's share. Both halves must
    /// succeed.
    fn complete(self: Box<Self>, listener_share: &[u8]) -> Result<SharedSecret, rustls::Error> {
        let (ciphertext, x25519_share) = split(listener_share, mlkem::CIPHERTEXT_LEN)?;
        let mlkem1024_secret = self.mlkem1024.decapsulate(ciphertext)?;
        let x25519_secret = self.x25519.complete(x25519_share)?;
        count_completed(HYBRID_LABEL);
        Ok(combine(&mlkem1024_secret, &x25519_secret))
    }

    /// The X25519 half. rustls sends a separate share for it only when the
    /// provider also enables standalone X25519.
    fn hybrid_component(&self) -> Option<(NamedGroup, &[u8])> {
        Some((self.x25519.group(), self.x25519.pub_key()))
    }

    /// Completes plain X25519 when the listener chose it over the hybrid.
    fn complete_hybrid_component(
        self: Box<Self>,
        listener_share: &[u8],
    ) -> Result<SharedSecret, rustls::Error> {
        let secret = self.x25519.complete(listener_share)?;
        count_completed(X25519_LABEL);
        Ok(secret)
    }

    /// The dialer's share.
    fn pub_key(&self) -> &[u8] {
        &self.share
    }

    /// Not a finite-field group.
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        None
    }

    /// X25519MLKEM1024.
    fn group(&self) -> NamedGroup {
        X25519_MLKEM1024
    }
}

/// Counts one key exchange completed in the group `label` (OBS-1).
fn count_completed(label: &'static str) {
    metrics::counter!("zakura.quic.tls.key_exchange", "group" => label).increment(1);
}

/// Splits a peer's share into its ML-KEM part, of `mlkem1024_len` bytes, and
/// its X25519 part. A share of any other total length is refused.
fn split(share: &[u8], mlkem1024_len: usize) -> Result<(&[u8], &[u8]), rustls::Error> {
    if share.len() != mlkem1024_len + X25519_LEN {
        return Err(PeerMisbehaved::InvalidKeyShare.into());
    }
    Ok(share.split_at(mlkem1024_len))
}

/// The hybrid secret: the ML-KEM secret, then the X25519 secret. rustls
/// wipes it when it's dropped.
fn combine(mlkem1024: &mlkem::SharedSecret, x25519: &SharedSecret) -> SharedSecret {
    SharedSecret::from([mlkem1024.as_slice(), x25519.secret_bytes()].concat())
}

#[cfg(test)]
mod tests;
