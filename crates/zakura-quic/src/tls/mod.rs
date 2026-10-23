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

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use quinn_proto::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls_pki_types::{alg_id, SubjectPublicKeyInfoDer, UnixTime};

use crate::{
    inputs::EndpointInputs,
    key::{NodeId, NodeSecretKey, KEY_LENGTH},
};

pub(crate) mod resolver;
pub(crate) mod verifier;

/// The server name passed to quinn-proto for every dial.
///
/// SNI is off (TLS-6), so this name never leaves the process. The verifier
/// checks the dialed node ID instead.
pub(crate) const UNSENT_SERVER_NAME: &str = "zakura.invalid";

/// The runtime's UTC clock, `zakura_rt::time::utc_now`, for the Retry token
/// time and the TLS certificate-check time. It's the host clock in
/// production and the scenario clock in a simulated run.
#[derive(Debug)]
pub(crate) struct RuntimeUtc;

impl quinn_proto::TimeSource for RuntimeUtc {
    /// The runtime's UTC time.
    fn now(&self) -> SystemTime {
        zakura_rt::time::utc_now()
    }
}

impl rustls::time_provider::TimeProvider for RuntimeUtc {
    /// The runtime's UTC time, or `None` before the Unix epoch, which rustls
    /// treats as a failed clock read.
    fn current_time(&self) -> Option<UnixTime> {
        let since_epoch = zakura_rt::time::utc_now().duration_since(UNIX_EPOCH).ok()?;
        Some(UnixTime::since_unix_epoch(since_epoch))
    }
}

/// An error building a rustls or QUIC crypto config.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TlsConfigError {
    /// The crypto provider lacks the TLS 1.3 suite QUIC Initial packets need.
    #[error("crypto provider lacks TLS13_AES_128_GCM_SHA256, which QUIC requires")]
    NoInitialCipherSuite(#[from] quinn_proto::crypto::rustls::NoInitialCipherSuite),
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
        Self::with_provider(secret_key, rustls::crypto::ring::default_provider())
    }

    /// TLS state whose provider takes its random source and key-exchange
    /// groups from `inputs`, when they're set (TLS-9).
    ///
    /// Injected groups that TLS 1.3 can't use are dropped, and at least one
    /// must remain (TLS-1). With either input injected this also builds a
    /// client config once, so unusable groups fail at bind rather than at
    /// the first handshake.
    pub(crate) fn with_inputs(
        secret_key: &NodeSecretKey,
        inputs: &EndpointInputs,
    ) -> Result<Self, TlsConfigError> {
        let random = inputs.injected_tls_random();
        let groups = inputs.injected_kx_groups();
        if random.is_none() && groups.is_none() {
            return Ok(Self::new(secret_key));
        }
        // TLS-9: everything but these two fields stays ring's default.
        let mut provider = rustls::crypto::ring::default_provider();
        if let Some(random) = random {
            provider.secure_random = random;
        }
        if let Some(groups) = groups {
            // TLS-1: drop groups TLS 1.3 can't use. rustls' builder doesn't
            // check `usable_for_version`, and its client starts the first
            // configured group for the key share while advertising only the
            // usable ones.
            provider.kx_groups = groups
                .iter()
                .copied()
                .filter(|group| group.usable_for_version(rustls::ProtocolVersion::TLSv1_3))
                .collect();
            if provider.kx_groups.is_empty() {
                return Err(TlsConfigError::Rustls(rustls::Error::General(
                    "no key-exchange group is usable with TLS 1.3".into(),
                )));
            }
        }
        let tls = Self::with_provider(secret_key, provider);
        #[cfg(any(test, zakura_sim))]
        inputs.provider_built(&tls.provider);
        tls.client_config(secret_key.public(), &[b"zakura-quic/check"])?;
        Ok(tls)
    }

    fn with_provider(secret_key: &NodeSecretKey, provider: rustls::crypto::CryptoProvider) -> Self {
        Self {
            cert_resolver: Arc::new(resolver::ResolveRawPublicKeyCert::new(secret_key)),
            client_verifier: Arc::new(verifier::ClientCertificateVerifier),
            // TLS-9: rustls' ring provider with its default TLS 1.3 suites.
            provider: Arc::new(provider),
        }
    }

    /// Whether the provider is rustls' ring default, field for field.
    #[cfg(test)]
    pub(crate) fn uses_default_provider(&self) -> bool {
        format!("{:?}", self.provider) == format!("{:?}", rustls::crypto::ring::default_provider())
    }

    /// Builds the server config, offering `alpns` in the given order (WIRE-2).
    pub(crate) fn server_config(
        &self,
        alpns: Vec<Vec<u8>>,
    ) -> Result<QuicServerConfig, TlsConfigError> {
        Ok(QuicServerConfig::try_from(self.rustls_server(alpns)?)?)
    }

    fn rustls_server(&self, alpns: Vec<Vec<u8>>) -> Result<rustls::ServerConfig, TlsConfigError> {
        let mut crypto =
            rustls::ServerConfig::builder_with_details(self.provider.clone(), Arc::new(RuntimeUtc))
                .with_protocol_versions(verifier::PROTOCOL_VERSIONS)?
                .with_client_cert_verifier(self.client_verifier.clone())
                .with_cert_resolver(self.cert_resolver.clone());
        crypto.alpn_protocols = alpns;
        // TLS-10: refuse 0-RTT. Without tickets, no client can resume either.
        crypto.max_early_data_size = 0;
        crypto.send_tls13_tickets = 0;
        Ok(crypto)
    }

    /// Builds a client config that accepts only `expected` as the server
    /// (TLS-7) and offers `alpns` in preference order.
    pub(crate) fn client_config(
        &self,
        expected: NodeId,
        alpns: &[&[u8]],
    ) -> Result<QuicClientConfig, TlsConfigError> {
        Ok(QuicClientConfig::try_from(
            self.rustls_client(expected, alpns)?,
        )?)
    }

    fn rustls_client(
        &self,
        expected: NodeId,
        alpns: &[&[u8]],
    ) -> Result<rustls::ClientConfig, TlsConfigError> {
        let mut crypto =
            rustls::ClientConfig::builder_with_details(self.provider.clone(), Arc::new(RuntimeUtc))
                .with_protocol_versions(verifier::PROTOCOL_VERSIONS)?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(
                    verifier::ServerCertificateVerifier::new(expected),
                ))
                .with_client_cert_resolver(self.cert_resolver.clone());
        crypto.alpn_protocols = alpns.iter().map(|alpn| alpn.to_vec()).collect();
        crypto.enable_sni = false;
        crypto.enable_early_data = false;
        crypto.resumption = rustls::client::Resumption::disabled();
        Ok(crypto)
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
pub(crate) fn remote_node_id(session: &dyn quinn_proto::crypto::Session) -> Option<NodeId> {
    let certs = session
        .peer_identity()?
        .downcast::<Vec<rustls_pki_types::CertificateDer<'static>>>()
        .ok()?;
    match certs.as_slice() {
        [cert] => node_id_from_spki(cert.as_ref()),
        _ => None,
    }
}

/// Returns the ALPN a completed handshake negotiated.
pub(crate) fn negotiated_alpn(session: &dyn quinn_proto::crypto::Session) -> Option<Vec<u8>> {
    session
        .handshake_data()?
        .downcast::<quinn_proto::crypto::rustls::HandshakeData>()
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

    /// A random source that always fills with 7s.
    #[derive(Debug)]
    struct Sevens;

    impl rustls::crypto::SecureRandom for Sevens {
        fn fill(&self, buf: &mut [u8]) -> Result<(), rustls::crypto::GetRandomFailed> {
            buf.fill(7);
            Ok(())
        }
    }

    static SEVENS: Sevens = Sevens;

    /// TLS-9: injected inputs replace only the random source and the
    /// key-exchange groups. The cipher suites, signature algorithms and key
    /// provider stay ring's.
    #[test]
    fn injected_inputs_keep_rings_suites() {
        let ring = rustls::crypto::ring::default_provider();
        let x25519 = rustls::crypto::ring::kx_group::X25519;
        let inputs = EndpointInputs::default()
            .tls_random(&SEVENS)
            .kx_groups(vec![x25519]);
        let tls = TlsConfig::with_inputs(&NodeSecretKey::generate(), &inputs).unwrap();
        let provider = &tls.provider;
        assert_eq!(
            format!("{:?}", provider.cipher_suites),
            format!("{:?}", ring.cipher_suites)
        );
        assert_eq!(
            format!("{:?}", provider.signature_verification_algorithms),
            format!("{:?}", ring.signature_verification_algorithms)
        );
        assert_eq!(
            format!("{:?}", provider.key_provider),
            format!("{:?}", ring.key_provider)
        );
        assert_eq!(format!("{:?}", provider.secure_random), "Sevens");
        assert_eq!(
            provider
                .kx_groups
                .iter()
                .map(|group| group.name())
                .collect::<Vec<_>>(),
            [x25519.name()]
        );
        assert!(!tls.uses_default_provider());

        let tls =
            TlsConfig::with_inputs(&NodeSecretKey::generate(), &EndpointInputs::default()).unwrap();
        assert!(tls.uses_default_provider());
    }

    /// ring's P-256, limited to TLS 1.2, counting the key exchanges it starts.
    #[derive(Debug)]
    struct Tls12OnlyP256(std::sync::atomic::AtomicUsize);

    impl rustls::crypto::SupportedKxGroup for Tls12OnlyP256 {
        fn start(&self) -> Result<Box<dyn rustls::crypto::ActiveKeyExchange>, rustls::Error> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            rustls::crypto::ring::kx_group::SECP256R1.start()
        }

        fn name(&self) -> rustls::NamedGroup {
            rustls::crypto::ring::kx_group::SECP256R1.name()
        }

        fn usable_for_version(&self, version: rustls::ProtocolVersion) -> bool {
            version == rustls::ProtocolVersion::TLSv1_2
        }
    }

    static TLS12_ONLY_P256: Tls12OnlyP256 = Tls12OnlyP256(std::sync::atomic::AtomicUsize::new(0));

    /// TLS-1, TLS-9: rustls' builder accepts groups that only TLS 1.2 can
    /// use. A list with none for TLS 1.3 fails here. In a mixed list the
    /// TLS 1.2-only group is dropped, because rustls would otherwise start
    /// the first configured group for its key share while advertising only
    /// the usable ones.
    #[test]
    fn kx_groups_unusable_with_tls13_are_refused_or_dropped() {
        let secret = NodeSecretKey::generate();
        let inputs = EndpointInputs::default().kx_groups(vec![&TLS12_ONLY_P256]);
        assert!(TlsConfig::with_inputs(&secret, &inputs).is_err());

        let x25519 = rustls::crypto::ring::kx_group::X25519;
        let inputs = EndpointInputs::default().kx_groups(vec![&TLS12_ONLY_P256, x25519]);
        let tls = TlsConfig::with_inputs(&secret, &inputs).unwrap();
        assert_eq!(
            tls.provider
                .kx_groups
                .iter()
                .map(|group| group.name())
                .collect::<Vec<_>>(),
            [x25519.name()]
        );

        // Writing a ClientHello starts the key share.
        let starts = TLS12_ONLY_P256.0.load(std::sync::atomic::Ordering::Relaxed);
        let client = tls
            .rustls_client(NodeSecretKey::generate().public(), &[b"a"])
            .unwrap();
        let name = rustls_pki_types::ServerName::try_from(UNSENT_SERVER_NAME).unwrap();
        rustls::ClientConnection::new(Arc::new(client), name).unwrap();
        assert_eq!(
            TLS12_ONLY_P256.0.load(std::sync::atomic::Ordering::Relaxed),
            starts
        );
    }

    /// M3.5: both TLS configs check certificates against the runtime's UTC
    /// clock, which is the host clock outside a simulated run.
    #[test]
    fn tls_configs_read_the_runtime_utc_clock() {
        let tls = TlsConfig::new(&NodeSecretKey::generate());
        let server = tls.rustls_server(vec![b"a".to_vec()]).unwrap();
        let client = tls
            .rustls_client(NodeSecretKey::generate().public(), &[b"a"])
            .unwrap();
        assert_eq!(format!("{:?}", server.time_provider), "RuntimeUtc");
        assert_eq!(format!("{:?}", client.time_provider), "RuntimeUtc");

        let secs = |time: SystemTime| time.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let before = secs(SystemTime::now());
        let tls_now = rustls::time_provider::TimeProvider::current_time(&RuntimeUtc)
            .unwrap()
            .as_secs();
        let retry_now = secs(quinn_proto::TimeSource::now(&RuntimeUtc));
        let after = secs(SystemTime::now());
        assert!((before..=after).contains(&tls_now));
        assert!((before..=after).contains(&retry_now));
    }

    /// TLS-10, TLS-11, WIRE-2: no 0-RTT, no tickets, no SNI, and the ALPNs in
    /// the caller's order.
    #[test]
    fn tls_profile_refuses_early_data_tickets_and_sni() {
        let tls = TlsConfig::new(&NodeSecretKey::generate());
        let server = tls
            .rustls_server(vec![b"b".to_vec(), b"a".to_vec()])
            .unwrap();
        assert_eq!(server.max_early_data_size, 0);
        assert_eq!(server.send_tls13_tickets, 0);
        assert_eq!(server.alpn_protocols, [b"b".to_vec(), b"a".to_vec()]);

        let peer = NodeSecretKey::generate().public();
        let client = tls.rustls_client(peer, &[b"b", b"a"]).unwrap();
        assert!(!client.enable_sni);
        assert!(!client.enable_early_data);
        assert_eq!(client.alpn_protocols, [b"b".to_vec(), b"a".to_vec()]);
    }
}
