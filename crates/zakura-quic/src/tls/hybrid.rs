//! X25519MLKEM768, the hybrid key-exchange group (TLS-9).
//!
//! Both sides run X25519 and ML-KEM-768, and TLS derives the session keys
//! from both secrets. The keys stay secret as long as either algorithm
//! holds, so a break of ML-KEM, or a wrong answer from its newer code, can't
//! make a connection weaker than X25519 alone.
//!
//! The layout is the one `draft-ietf-tls-ecdhe-mlkem` fixes for this group:
//! each share and the secret are the ML-KEM part followed by the X25519
//! part. The combiner follows rustls' own, which ships only with its
//! aws-lc-rs provider.

use std::sync::LazyLock;

use rustls::{
    crypto::{
        ActiveKeyExchange, CompletedKeyExchange, SecureRandom, SharedSecret, SupportedKxGroup,
    },
    ffdhe_groups::FfdheGroup,
    NamedGroup, PeerMisbehaved, ProtocolVersion,
};

use super::mlkem::{self, DecapsulationKey, MlKem768};

/// The length of an X25519 share.
const X25519_LEN: usize = 32;

/// The hybrid group production endpoints offer first: ring's X25519, and
/// ML-KEM-768 drawing from the same random source ring's provider uses.
pub(crate) fn x25519_mlkem768() -> &'static dyn SupportedKxGroup {
    static GROUP: LazyLock<X25519MlKem768> = LazyLock::new(|| {
        X25519MlKem768::new(
            rustls::crypto::ring::kx_group::X25519,
            rustls::crypto::ring::default_provider().secure_random,
        )
    });
    &*GROUP
}

/// X25519MLKEM768 over a given X25519 group and ML-KEM random source.
#[derive(Debug)]
pub(crate) struct X25519MlKem768 {
    x25519: &'static dyn SupportedKxGroup,
    mlkem768: MlKem768,
}

impl X25519MlKem768 {
    /// The hybrid of `x25519`, which must be an X25519 group, and ML-KEM-768
    /// drawing from `mlkem_random`.
    pub(crate) const fn new(
        x25519: &'static dyn SupportedKxGroup,
        mlkem_random: &'static dyn SecureRandom,
    ) -> Self {
        Self {
            x25519,
            mlkem768: MlKem768::new(mlkem_random),
        }
    }
}

impl SupportedKxGroup for X25519MlKem768 {
    /// Starts the dialer's side: an ML-KEM key pair and an X25519 key.
    fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, rustls::Error> {
        let mlkem768 = self.mlkem768.generate()?;
        let x25519 = self.x25519.start()?;
        let share = [mlkem768.encapsulation_key(), x25519.pub_key()].concat();
        Ok(Box::new(Dialer {
            mlkem768,
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
        let (ciphertext, mlkem768_secret) = self.mlkem768.encapsulate(encapsulation_key)?;
        let x25519 = self.x25519.start_and_complete(x25519_share)?;
        Ok(CompletedKeyExchange {
            group: NamedGroup::X25519MLKEM768,
            pub_key: [ciphertext.as_slice(), &x25519.pub_key].concat(),
            secret: combine(&mlkem768_secret, &x25519.secret),
        })
    }

    /// Not a finite-field group.
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        None
    }

    /// X25519MLKEM768.
    fn name(&self) -> NamedGroup {
        NamedGroup::X25519MLKEM768
    }

    /// The group is defined for TLS 1.3 only.
    fn usable_for_version(&self, version: ProtocolVersion) -> bool {
        version == ProtocolVersion::TLSv1_3
    }
}

/// The dialer's side of one exchange, waiting for the listener's share.
struct Dialer {
    mlkem768: DecapsulationKey,
    x25519: Box<dyn ActiveKeyExchange>,
    /// The share to send: the encapsulation key, then the X25519 key.
    share: Vec<u8>,
}

impl ActiveKeyExchange for Dialer {
    /// The combined secret with the listener's share. Both halves must
    /// succeed.
    fn complete(self: Box<Self>, listener_share: &[u8]) -> Result<SharedSecret, rustls::Error> {
        let (ciphertext, x25519_share) = split(listener_share, mlkem::CIPHERTEXT_LEN)?;
        let mlkem768_secret = self.mlkem768.decapsulate(ciphertext)?;
        let x25519_secret = self.x25519.complete(x25519_share)?;
        Ok(combine(&mlkem768_secret, &x25519_secret))
    }

    /// The X25519 half, so rustls also sends it as a plain X25519 share. A
    /// listener without the hybrid then answers that share in the same
    /// round trip.
    fn hybrid_component(&self) -> Option<(NamedGroup, &[u8])> {
        Some((self.x25519.group(), self.x25519.pub_key()))
    }

    /// Completes plain X25519 when the listener chose it over the hybrid.
    fn complete_hybrid_component(
        self: Box<Self>,
        listener_share: &[u8],
    ) -> Result<SharedSecret, rustls::Error> {
        self.x25519.complete(listener_share)
    }

    /// The dialer's share.
    fn pub_key(&self) -> &[u8] {
        &self.share
    }

    /// Not a finite-field group.
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        None
    }

    /// X25519MLKEM768.
    fn group(&self) -> NamedGroup {
        NamedGroup::X25519MLKEM768
    }
}

/// Splits a peer's share into its ML-KEM part, of `mlkem768_len` bytes, and
/// its X25519 part. A share of any other total length is refused.
fn split(share: &[u8], mlkem768_len: usize) -> Result<(&[u8], &[u8]), rustls::Error> {
    if share.len() != mlkem768_len + X25519_LEN {
        return Err(PeerMisbehaved::InvalidKeyShare.into());
    }
    Ok(share.split_at(mlkem768_len))
}

/// The hybrid secret: the ML-KEM secret, then the X25519 secret. rustls
/// wipes it when it's dropped.
fn combine(mlkem768: &mlkem::SharedSecret, x25519: &SharedSecret) -> SharedSecret {
    SharedSecret::from([mlkem768.as_slice(), x25519.secret_bytes()].concat())
}

#[cfg(test)]
mod tests;
