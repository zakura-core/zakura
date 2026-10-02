//! The TLS 1.3 raw-public-key profile (SPEC §4).
//!
//! Adapted from `iroh/src/tls.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.
//!
//! Every rustls config in this crate comes from this module (TLS-11). The
//! profile matches Iroh 1.1 on the wire: TLS 1.3 only, RFC 7250 raw public keys
//! in both directions, Ed25519 only, mandatory client authentication and no
//! SNI. It differs from Iroh in two local choices that don't change what a peer
//! sees on a full handshake: the server refuses 0-RTT and issues no session
//! tickets, and the client never resumes (TLS-10).

use std::sync::Arc;

use noq::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls_pki_types::{alg_id, SubjectPublicKeyInfoDer};

use crate::key::{NodeId, NodeSecretKey, KEY_LENGTH};

mod resolver;
mod verifier;

/// The server name passed to noq for every dial.
///
/// SNI is off (TLS-6), so this name never leaves the process. The verifier
/// checks the dialed node ID instead.
pub(crate) const UNSENT_SERVER_NAME: &str = "zakura.invalid";

/// An error building a rustls or QUIC crypto config.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TlsConfigError {
    /// The crypto provider lacks the TLS 1.3 suite QUIC Initial packets need.
    #[error("crypto provider lacks TLS13_AES_128_GCM_SHA256, which QUIC requires")]
    NoInitialCipherSuite(#[from] noq::crypto::rustls::NoInitialCipherSuite),
    /// rustls refused the config.
    #[error("rustls refused the TLS config: {0}")]
    Rustls(#[from] rustls::Error),
}

/// Long-lived TLS state for one endpoint.
#[derive(Debug)]
pub(crate) struct TlsConfig {
    cert_resolver: Arc<resolver::ResolveRawPublicKeyCert>,
    client_verifier: Arc<verifier::ClientCertificateVerifier>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl TlsConfig {
    pub(crate) fn new(secret_key: &NodeSecretKey) -> Self {
        Self {
            cert_resolver: Arc::new(resolver::ResolveRawPublicKeyCert::new(secret_key)),
            client_verifier: Arc::new(verifier::ClientCertificateVerifier),
            // TLS-9: rustls' ring provider with its default TLS 1.3 suites.
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
    }

    /// Builds the server config, offering `alpns` in the given order (WIRE-2).
    pub(crate) fn server_config(
        &self,
        alpns: Vec<Vec<u8>>,
    ) -> Result<QuicServerConfig, TlsConfigError> {
        let mut crypto = rustls::ServerConfig::builder_with_provider(self.provider.clone())
            .with_protocol_versions(verifier::PROTOCOL_VERSIONS)?
            .with_client_cert_verifier(self.client_verifier.clone())
            .with_cert_resolver(self.cert_resolver.clone());
        crypto.alpn_protocols = alpns;
        // TLS-10: refuse 0-RTT. Without tickets, no client can resume either.
        crypto.max_early_data_size = 0;
        crypto.send_tls13_tickets = 0;
        Ok(QuicServerConfig::try_from(crypto)?)
    }

    /// Builds a client config that accepts only `expected` as the server (TLS-7).
    pub(crate) fn client_config(
        &self,
        expected: NodeId,
        alpn: &[u8],
    ) -> Result<QuicClientConfig, TlsConfigError> {
        let mut crypto = rustls::ClientConfig::builder_with_provider(self.provider.clone())
            .with_protocol_versions(verifier::PROTOCOL_VERSIONS)?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier::ServerCertificateVerifier::new(
                expected,
            )))
            .with_client_cert_resolver(self.cert_resolver.clone());
        crypto.alpn_protocols = vec![alpn.to_vec()];
        crypto.enable_sni = false;
        crypto.enable_early_data = false;
        crypto.resumption = rustls::client::Resumption::disabled();
        Ok(QuicClientConfig::try_from(crypto)?)
    }
}

/// The DER SubjectPublicKeyInfo for an Ed25519 key, as Iroh encodes it (TLS-3).
pub(crate) fn ed25519_spki(key: &NodeId) -> SubjectPublicKeyInfoDer<'static> {
    rustls::sign::public_key_to_spki(&alg_id::ED25519, key.as_bytes())
}

/// Extracts the node ID from an Ed25519 SubjectPublicKeyInfo.
///
/// Returns `None` unless `spki` is exactly the constant Ed25519 prefix
/// followed by a valid 32-byte key.
pub(crate) fn node_id_from_spki(spki: &[u8]) -> Option<NodeId> {
    let key = spki.get(spki.len().checked_sub(KEY_LENGTH)?..)?;
    let id = NodeId::try_from(key).ok()?;
    (ed25519_spki(&id).as_ref() == spki).then_some(id)
}

/// Returns the TLS-proven node ID of a connection's peer (TLS-8).
pub(crate) fn remote_node_id(conn: &noq::Connection) -> Option<NodeId> {
    let certs = conn
        .peer_identity()?
        .downcast::<Vec<rustls_pki_types::CertificateDer<'static>>>()
        .ok()?;
    match certs.as_slice() {
        [cert] => node_id_from_spki(cert.as_ref()),
        _ => None,
    }
}

/// Returns the ALPN a completed handshake negotiated.
pub(crate) fn negotiated_alpn(conn: &noq::Connection) -> Option<Vec<u8>> {
    conn.handshake_data()?
        .downcast::<noq::crypto::rustls::HandshakeData>()
        .ok()?
        .protocol
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spki_matches_iroh_encoding() {
        let id = NodeSecretKey::from_bytes(&[7; 32]).public();
        let spki = ed25519_spki(&id);
        // 30 2a 30 05 06 03 2b 65 70 03 21 00 || key (TLS-3).
        assert_eq!(
            &spki.as_ref()[..12],
            &[0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00]
        );
        assert_eq!(&spki.as_ref()[12..], id.as_bytes());
        assert_eq!(node_id_from_spki(spki.as_ref()), Some(id));
    }

    #[test]
    fn spki_with_another_prefix_is_refused() {
        let id = NodeSecretKey::from_bytes(&[7; 32]).public();
        let mut spki = ed25519_spki(&id).as_ref().to_vec();
        spki[8] = 0x71; // Ed448's OID byte.
        assert_eq!(node_id_from_spki(&spki), None);
        assert_eq!(node_id_from_spki(id.as_bytes()), None);
    }
}
