//! Presents the local credential as an RFC 7250 raw public key and signs
//! with its composite Ed25519 + ML-DSA-44 key (TLS-2 to TLS-4).
//!
//! Adapted from `iroh/src/tls/resolver.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use rustls_pki_types::{CertificateDer, SubjectPublicKeyInfoDer};

use crate::{composite::COMPOSITE_SCHEME, key::NodeSecretKey};

#[derive(Debug)]
pub(crate) struct ResolveRawPublicKeyCert {
    key: Arc<rustls::sign::CertifiedKey>,
}

impl ResolveRawPublicKeyCert {
    /// Presents `secret_key`'s credential and signs with its composite key,
    /// hedging ML-DSA with `random`.
    pub(crate) fn new(
        secret_key: &NodeSecretKey,
        random: &'static dyn rustls::crypto::SecureRandom,
    ) -> Self {
        let signing_key = Arc::new(CompositeSigningKey {
            secret: secret_key.clone(),
            random,
        });
        let public_key_as_cert = CertificateDer::from(signing_key.spki().to_vec());
        let key = Arc::new(rustls::sign::CertifiedKey::new(
            vec![public_key_as_cert],
            signing_key,
        ));
        Self { key }
    }
}

impl rustls::client::ResolvesClientCert for ResolveRawPublicKeyCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(Arc::clone(&self.key))
    }

    fn only_raw_public_keys(&self) -> bool {
        true
    }

    fn has_certs(&self) -> bool {
        true
    }
}

impl rustls::server::ResolvesServerCert for ResolveRawPublicKeyCert {
    fn resolve(
        &self,
        _client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(Arc::clone(&self.key))
    }

    fn only_raw_public_keys(&self) -> bool {
        true
    }
}

/// Signs handshakes with the node's composite Ed25519 + ML-DSA-44 key
/// (TLS-4), drawing ML-DSA's hedging randomness from the endpoint's TLS
/// random source, which a simulation seeds.
#[derive(Clone)]
struct CompositeSigningKey {
    secret: NodeSecretKey,
    random: &'static dyn rustls::crypto::SecureRandom,
}

impl std::fmt::Debug for CompositeSigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompositeSigningKey([redacted])")
    }
}

impl CompositeSigningKey {
    fn spki(&self) -> SubjectPublicKeyInfoDer<'static> {
        self.secret.credential().to_spki()
    }
}

impl rustls::sign::SigningKey for CompositeSigningKey {
    fn choose_scheme(
        &self,
        offered: &[rustls::SignatureScheme],
    ) -> Option<Box<dyn rustls::sign::Signer>> {
        offered
            .contains(&COMPOSITE_SCHEME)
            .then(|| Box::new(self.clone()) as Box<dyn rustls::sign::Signer>)
    }

    fn algorithm(&self) -> rustls::SignatureAlgorithm {
        // The low byte of the private-use scheme, as rustls reads one.
        rustls::SignatureAlgorithm::Unknown(0x44)
    }

    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(self.spki())
    }
}

impl rustls::sign::Signer for CompositeSigningKey {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        let mut randomness = [0; 32];
        self.random
            .fill(&mut randomness)
            .map_err(|_| rustls::Error::FailedToGetRandomBytes)?;
        self.secret
            .sign_composite(message, randomness)
            .map_err(|_| rustls::Error::General("ML-DSA-44 signing failed".into()))
    }

    fn scheme(&self) -> rustls::SignatureScheme {
        COMPOSITE_SCHEME
    }
}
