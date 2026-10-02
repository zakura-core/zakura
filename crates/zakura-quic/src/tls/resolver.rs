//! Presents the local Ed25519 key as an RFC 7250 raw public key (TLS-2, TLS-3).
//!
//! Adapted from `iroh/src/tls/resolver.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use rustls_pki_types::{CertificateDer, SubjectPublicKeyInfoDer};

use crate::key::NodeSecretKey;

#[derive(Debug)]
pub(super) struct ResolveRawPublicKeyCert {
    key: Arc<rustls::sign::CertifiedKey>,
}

impl ResolveRawPublicKeyCert {
    pub(super) fn new(secret_key: &NodeSecretKey) -> Self {
        let signing_key = Arc::new(Ed25519SigningKey(secret_key.clone()));
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

#[derive(Clone)]
struct Ed25519SigningKey(NodeSecretKey);

impl std::fmt::Debug for Ed25519SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Ed25519SigningKey([redacted])")
    }
}

impl Ed25519SigningKey {
    fn spki(&self) -> SubjectPublicKeyInfoDer<'static> {
        super::ed25519_spki(&self.0.public())
    }
}

impl rustls::sign::SigningKey for Ed25519SigningKey {
    fn choose_scheme(
        &self,
        offered: &[rustls::SignatureScheme],
    ) -> Option<Box<dyn rustls::sign::Signer>> {
        offered
            .contains(&rustls::SignatureScheme::ED25519)
            .then(|| Box::new(self.clone()) as Box<dyn rustls::sign::Signer>)
    }

    fn algorithm(&self) -> rustls::SignatureAlgorithm {
        rustls::SignatureAlgorithm::ED25519
    }

    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(self.spki())
    }
}

impl rustls::sign::Signer for Ed25519SigningKey {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        Ok(self.0.sign(message).to_vec())
    }

    fn scheme(&self) -> rustls::SignatureScheme {
        rustls::SignatureScheme::ED25519
    }
}
