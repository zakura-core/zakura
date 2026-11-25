//! The TLS 1.3 raw-public-key profile (SPEC §4).
//!
//! Adapted from `iroh/src/tls.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.
//!
//! Every rustls config in this crate comes from this module (TLS-11). The
//! profile started as Iroh 1.1's: TLS 1.3 only, RFC 7250 raw public keys in
//! both directions, mandatory client authentication and no SNI. It differs
//! from Iroh on the wire in two ways: the raw public key is the node's
//! credential, its full key list (TLS-3), and the only key-exchange group is
//! the hybrid X25519MLKEM1024 (TLS-9). It also differs in two local choices
//! that don't change what a peer sees on a full handshake: the server refuses
//! 0-RTT and issues no session tickets, and the client never resumes
//! (TLS-10).

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use quinn_proto::crypto::rustls::{QuicClientConfig, QuicServerConfig};
#[cfg(test)]
use rustls_pki_types::SubjectPublicKeyInfoDer;
use rustls_pki_types::UnixTime;

#[cfg(test)]
use crate::key::NodeKey;
use crate::{
    credential::NodeCredential,
    inputs::EndpointInputs,
    key::{NodeId, NodeSecretKey},
};

pub(crate) mod address_validation;
pub(crate) mod hybrid;
mod mlkem;
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
        Self::with_provider(secret_key, production_provider())
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
        // TLS-9: everything but these two fields stays the production
        // provider's.
        let mut provider = production_provider();
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
            cert_resolver: Arc::new(resolver::ResolveRawPublicKeyCert::new(
                secret_key,
                provider.secure_random,
            )),
            client_verifier: Arc::new(verifier::ClientCertificateVerifier),
            provider: Arc::new(provider),
        }
    }

    /// Whether the provider is the production one, field for field.
    #[cfg(test)]
    pub(crate) fn uses_production_provider(&self) -> bool {
        format!("{:?}", self.provider) == format!("{:?}", production_provider())
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

    /// Builds a client config that accepts only a server whose credential
    /// derives the node ID `expected` (TLS-7), and offers `alpns` in
    /// preference order.
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

/// The crypto provider of an endpoint with no injected inputs (TLS-9):
/// rustls' ring provider whose only key-exchange group is the hybrid
/// X25519MLKEM1024.
///
/// Every node since `p2p-v2/4` offers a hybrid, so a classical fallback would
/// protect no connection. A peer that offers only classical groups fails the
/// handshake.
pub(crate) fn production_provider() -> rustls::crypto::CryptoProvider {
    let mut provider = rustls::crypto::ring::default_provider();
    provider.kx_groups = vec![hybrid::x25519_mlkem1024()];
    provider
}

/// The DER SubjectPublicKeyInfo for a bare Ed25519 key, as Iroh encoded its
/// raw public keys. Zakura no longer presents one; tests use it as a
/// certificate that is not a credential.
#[cfg(test)]
pub(crate) fn ed25519_spki(key: &NodeKey) -> SubjectPublicKeyInfoDer<'static> {
    rustls::sign::public_key_to_spki(&rustls_pki_types::alg_id::ED25519, key.as_bytes())
}

/// Reads the credential a TLS certificate carries (TLS-3, TLS-8).
///
/// This is the one place a certificate becomes a credential, and so a node
/// ID: the client verifier compares that ID with the dialed one, and both
/// endpoints report it as the peer's ID.
pub(crate) fn credential_from_cert(cert: &[u8]) -> Option<NodeCredential> {
    NodeCredential::from_spki(cert).ok()
}

/// Returns the credential a connection's peer proved in the TLS handshake
/// (TLS-8).
pub(crate) fn remote_credential(
    session: &dyn quinn_proto::crypto::Session,
) -> Option<NodeCredential> {
    let certs = session
        .peer_identity()?
        .downcast::<Vec<rustls_pki_types::CertificateDer<'static>>>()
        .ok()?;
    match certs.as_slice() {
        [cert] => credential_from_cert(cert.as_ref()),
        _ => None,
    }
}

/// The length of [`shared_id`].
#[cfg(any(test, zakura_sim, feature = "test-support"))]
pub(crate) const SHARED_ID_LEN: usize = 16;

/// The exporter label of [`shared_id`].
#[cfg(any(test, zakura_sim, feature = "test-support"))]
const SHARED_ID_LABEL: &[u8] = b"EXPERIMENTAL-zakura-quic-shared-id";

/// The ID both ends of a completed handshake derive from it: the keying
/// material `session` exports under [`SHARED_ID_LABEL`] (RFC 8446 §7.5).
#[cfg(any(test, zakura_sim, feature = "test-support"))]
pub(crate) fn shared_id(session: &dyn quinn_proto::crypto::Session) -> [u8; SHARED_ID_LEN] {
    let mut id = [0; SHARED_ID_LEN];
    session
        .export_keying_material(&mut id, SHARED_ID_LABEL, &[])
        .expect("TLS 1.3 exports far more than 16 bytes once the handshake completes");
    id
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
        assert!(!tls.uses_production_provider());

        let tls =
            TlsConfig::with_inputs(&NodeSecretKey::generate(), &EndpointInputs::default()).unwrap();
        assert!(tls.uses_production_provider());
    }

    /// The names of a provider's key-exchange groups, in offer order.
    fn group_names(provider: &rustls::crypto::CryptoProvider) -> Vec<rustls::NamedGroup> {
        provider
            .kx_groups
            .iter()
            .map(|group| group.name())
            .collect()
    }

    /// TLS-9: the production provider is ring's with one change: its only
    /// key-exchange group is the hybrid.
    #[test]
    fn the_production_provider_is_rings_with_only_the_hybrid_group() {
        let ring = rustls::crypto::ring::default_provider();
        let production = production_provider();
        assert_eq!(group_names(&production), [hybrid::X25519_MLKEM1024]);
        assert_eq!(
            format!("{:?}", production.cipher_suites),
            format!("{:?}", ring.cipher_suites)
        );
        assert_eq!(
            format!("{:?}", production.signature_verification_algorithms),
            format!("{:?}", ring.signature_verification_algorithms)
        );
        assert_eq!(
            format!("{:?}", production.key_provider),
            format!("{:?}", ring.key_provider)
        );
        assert_eq!(
            format!("{:?}", production.secure_random),
            format!("{:?}", ring.secure_random)
        );
        assert!(TlsConfig::new(&NodeSecretKey::generate()).uses_production_provider());
    }

    /// TLS-9: an endpoint that injects only a random source still offers the
    /// production group alone.
    #[test]
    fn an_injected_random_source_keeps_the_production_groups() {
        let inputs = EndpointInputs::default().tls_random(&SEVENS);
        let tls = TlsConfig::with_inputs(&NodeSecretKey::generate(), &inputs).unwrap();
        assert_eq!(format!("{:?}", tls.provider.secure_random), "Sevens");
        assert_eq!(
            group_names(&tls.provider),
            group_names(&production_provider())
        );
    }

    /// A dialer's group that sends the production hybrid share altered by
    /// `alter`, for handshakes that must fail.
    #[derive(Debug)]
    struct Altered {
        alter: fn(Vec<u8>) -> Vec<u8>,
    }

    impl rustls::crypto::SupportedKxGroup for Altered {
        fn start(&self) -> Result<Box<dyn rustls::crypto::ActiveKeyExchange>, rustls::Error> {
            let exchange = hybrid::x25519_mlkem1024().start()?;
            let share = (self.alter)(exchange.pub_key().to_vec());
            Ok(Box::new(AlteredExchange { exchange, share }))
        }

        fn name(&self) -> rustls::NamedGroup {
            hybrid::X25519_MLKEM1024
        }
    }

    /// An exchange of [`Altered`]: the production one, with another share.
    struct AlteredExchange {
        exchange: Box<dyn rustls::crypto::ActiveKeyExchange>,
        share: Vec<u8>,
    }

    impl rustls::crypto::ActiveKeyExchange for AlteredExchange {
        fn complete(
            self: Box<Self>,
            peer: &[u8],
        ) -> Result<rustls::crypto::SharedSecret, rustls::Error> {
            self.exchange.complete(peer)
        }

        fn hybrid_component(&self) -> Option<(rustls::NamedGroup, &[u8])> {
            self.exchange.hybrid_component()
        }

        fn complete_hybrid_component(
            self: Box<Self>,
            peer: &[u8],
        ) -> Result<rustls::crypto::SharedSecret, rustls::Error> {
            self.exchange.complete_hybrid_component(peer)
        }

        fn pub_key(&self) -> &[u8] {
            &self.share
        }

        fn group(&self) -> rustls::NamedGroup {
            self.exchange.group()
        }
    }

    /// SEC-2: a production listener refuses the handshake when the dialer's
    /// hybrid share has the wrong length or carries an ML-KEM key that
    /// fails the modulus check. The hello also carries a valid plain X25519
    /// share, as a production hello does, and the listener doesn't fall back
    /// to it.
    #[test]
    fn a_malformed_hybrid_share_fails_the_handshake() {
        fn one_byte_short(mut share: Vec<u8>) -> Vec<u8> {
            share.pop();
            share
        }
        fn coefficient_above_the_modulus(mut share: Vec<u8>) -> Vec<u8> {
            // The first 12-bit coefficient becomes 4,095, above q = 3,329.
            share[0] = 0xff;
            share[1] |= 0x0f;
            share
        }
        let alterations: [fn(Vec<u8>) -> Vec<u8>; 2] =
            [one_byte_short, coefficient_above_the_modulus];
        for alter in alterations {
            let dialer_key = NodeSecretKey::generate();
            let listener_key = NodeSecretKey::generate();
            let group: &'static Altered = Box::leak(Box::new(Altered { alter }));
            let client = TlsConfig::with_inputs(
                &dialer_key,
                &EndpointInputs::default()
                    .kx_groups(vec![group, rustls::crypto::ring::kx_group::X25519]),
            )
            .unwrap()
            .rustls_client(listener_key.public(), &[b"a"])
            .unwrap();
            let client = Arc::new(client);
            // The hello carries a valid plain X25519 share: a listener with
            // X25519 alone completes on it in one round trip.
            assert_eq!(
                handshake_to(&client, &ring_only(&listener_key)),
                (rustls::NamedGroup::X25519, rustls::HandshakeKind::Full)
            );
            let server = TlsConfig::new(&listener_key)
                .rustls_server(vec![b"a".to_vec()])
                .unwrap();
            let name = rustls_pki_types::ServerName::try_from(UNSENT_SERVER_NAME).unwrap();
            let mut client = rustls::ClientConnection::new(client, name).unwrap();
            let mut server = rustls::ServerConnection::new(Arc::new(server)).unwrap();
            let mut hello = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut hello).unwrap();
            }
            server.read_tls(&mut hello.as_slice()).unwrap();
            assert_eq!(
                server.process_new_packets().unwrap_err(),
                rustls::Error::PeerMisbehaved(rustls::PeerMisbehaved::InvalidKeyShare)
            );
        }
    }

    /// TLS state like a release before the hybrid group: ring's own groups.
    fn ring_only(secret_key: &NodeSecretKey) -> TlsConfig {
        let groups = rustls::crypto::ring::default_provider().kx_groups;
        TlsConfig::with_inputs(secret_key, &EndpointInputs::default().kx_groups(groups)).unwrap()
    }

    /// Runs one TLS handshake in memory, the `dialer` side dialing the
    /// `listener` side. Returns the group both sides used and whether the
    /// dialer needed a second round trip (a HelloRetryRequest).
    fn handshake(
        dialer: impl Fn(&NodeSecretKey) -> TlsConfig,
        listener: impl Fn(&NodeSecretKey) -> TlsConfig,
    ) -> (rustls::NamedGroup, rustls::HandshakeKind) {
        let dialer_key = NodeSecretKey::generate();
        let listener_key = NodeSecretKey::generate();
        let client = dialer(&dialer_key)
            .rustls_client(listener_key.public(), &[b"a"])
            .unwrap();
        handshake_to(&Arc::new(client), &listener(&listener_key))
    }

    /// Runs one TLS handshake in memory from `client` to a listener with
    /// `listener`'s TLS state. Returns what [`handshake`] returns.
    fn handshake_to(
        client: &Arc<rustls::ClientConfig>,
        listener: &TlsConfig,
    ) -> (rustls::NamedGroup, rustls::HandshakeKind) {
        let server = listener.rustls_server(vec![b"a".to_vec()]).unwrap();
        let name = rustls_pki_types::ServerName::try_from(UNSENT_SERVER_NAME).unwrap();
        let mut client = rustls::ClientConnection::new(client.clone(), name).unwrap();
        let mut server = rustls::ServerConnection::new(Arc::new(server)).unwrap();
        // A full handshake takes two exchanges and one with a
        // HelloRetryRequest three, so one that needs more has stalled.
        for _ in 0..4 {
            if !client.is_handshaking() && !server.is_handshaking() {
                break;
            }
            let mut flight = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut flight).unwrap();
            }
            let mut unread = flight.as_slice();
            while !unread.is_empty() {
                server.read_tls(&mut unread).unwrap();
                server.process_new_packets().unwrap();
            }
            let mut flight = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut flight).unwrap();
            }
            let mut unread = flight.as_slice();
            while !unread.is_empty() {
                client.read_tls(&mut unread).unwrap();
                client.process_new_packets().unwrap();
            }
        }
        assert!(
            !client.is_handshaking() && !server.is_handshaking(),
            "the handshake stalled"
        );
        let group = |group: Option<&'static dyn rustls::crypto::SupportedKxGroup>| {
            group
                .expect("a finished TLS 1.3 handshake used a key-exchange group")
                .name()
        };
        let used = group(client.negotiated_key_exchange_group());
        assert_eq!(group(server.negotiated_key_exchange_group()), used);
        let kind = client
            .handshake_kind()
            .expect("a finished handshake has a kind");
        (used, kind)
    }

    /// TLS-9: two endpoints on the production provider use the hybrid group,
    /// in one round trip.
    #[test]
    fn two_production_endpoints_use_the_hybrid_group() {
        assert_eq!(
            handshake(TlsConfig::new, TlsConfig::new),
            (hybrid::X25519_MLKEM1024, rustls::HandshakeKind::Full)
        );
    }

    /// TLS-9: a peer that offers only classical groups shares no group with
    /// a production endpoint, so the handshake fails in either direction.
    #[test]
    fn a_peer_without_the_hybrid_group_is_refused() {
        let no_shared_group =
            rustls::Error::PeerIncompatible(rustls::PeerIncompatible::NoKxGroupsInCommon);
        assert_eq!(handshake_error(TlsConfig::new, ring_only), no_shared_group);
        assert_eq!(handshake_error(ring_only, TlsConfig::new), no_shared_group);
    }

    /// Runs one TLS handshake in memory like [`handshake`] and returns the
    /// error the listener fails it with.
    fn handshake_error(
        dialer: impl Fn(&NodeSecretKey) -> TlsConfig,
        listener: impl Fn(&NodeSecretKey) -> TlsConfig,
    ) -> rustls::Error {
        let dialer_key = NodeSecretKey::generate();
        let listener_key = NodeSecretKey::generate();
        let client = dialer(&dialer_key)
            .rustls_client(listener_key.public(), &[b"a"])
            .unwrap();
        let server = listener(&listener_key)
            .rustls_server(vec![b"a".to_vec()])
            .unwrap();
        let name = rustls_pki_types::ServerName::try_from(UNSENT_SERVER_NAME).unwrap();
        let mut client = rustls::ClientConnection::new(Arc::new(client), name).unwrap();
        let mut server = rustls::ServerConnection::new(Arc::new(server)).unwrap();
        let mut hello = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut hello).unwrap();
        }
        server.read_tls(&mut hello.as_slice()).unwrap();
        server
            .process_new_packets()
            .expect_err("the listener shares no group with the dialer")
    }

    /// A listener's key that signs the composite with `self.0`'s two keys,
    /// whatever credential the listener presents.
    #[derive(Clone, Debug)]
    struct SignsAs(NodeSecretKey);

    impl rustls::sign::SigningKey for SignsAs {
        fn choose_scheme(
            &self,
            offered: &[rustls::SignatureScheme],
        ) -> Option<Box<dyn rustls::sign::Signer>> {
            offered
                .contains(&crate::composite::COMPOSITE_SCHEME)
                .then(|| Box::new(self.clone()) as Box<dyn rustls::sign::Signer>)
        }

        fn algorithm(&self) -> rustls::SignatureAlgorithm {
            rustls::SignatureAlgorithm::Unknown(0x65)
        }
    }

    impl rustls::sign::Signer for SignsAs {
        fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
            self.0
                .sign_composite(message, [9; 32])
                .map_err(|_| rustls::Error::General("the composite signature failed".into()))
        }

        fn scheme(&self) -> rustls::SignatureScheme {
            crate::composite::COMPOSITE_SCHEME
        }
    }

    /// Presents one fixed raw public key to every dialer.
    #[derive(Debug)]
    struct Presents(Arc<rustls::sign::CertifiedKey>);

    impl rustls::server::ResolvesServerCert for Presents {
        fn resolve(
            &self,
            _hello: rustls::server::ClientHello<'_>,
        ) -> Option<Arc<rustls::sign::CertifiedKey>> {
            Some(self.0.clone())
        }

        fn only_raw_public_keys(&self) -> bool {
            true
        }
    }

    /// Runs one TLS handshake in memory. A production dialer dials the node
    /// `dialed` and reaches a listener that presents `credential` and signs
    /// with `signer`'s keys. Returns the error the dialer fails the handshake
    /// with, or `None` when the handshake completes.
    fn dial_presenting(
        dialed: NodeId,
        credential: &NodeCredential,
        signer: &NodeSecretKey,
    ) -> Option<rustls::Error> {
        dial_presenting_cert(dialed, credential.to_spki().to_vec(), signer)
    }

    /// [`dial_presenting`] with the listener's raw public key given as
    /// bytes, which need not be a credential.
    fn dial_presenting_cert(
        dialed: NodeId,
        cert: Vec<u8>,
        signer: &NodeSecretKey,
    ) -> Option<rustls::Error> {
        let client = TlsConfig::new(&NodeSecretKey::generate())
            .rustls_client(dialed, &[b"a"])
            .unwrap();
        let listener = TlsConfig::new(signer);
        let cert = rustls_pki_types::CertificateDer::from(cert);
        let key = rustls::sign::CertifiedKey::new(vec![cert], Arc::new(SignsAs(signer.clone())));
        let mut server =
            rustls::ServerConfig::builder_with_details(listener.provider, Arc::new(RuntimeUtc))
                .with_protocol_versions(verifier::PROTOCOL_VERSIONS)
                .unwrap()
                .with_client_cert_verifier(listener.client_verifier)
                .with_cert_resolver(Arc::new(Presents(Arc::new(key))));
        server.alpn_protocols = vec![b"a".to_vec()];

        let name = rustls_pki_types::ServerName::try_from(UNSENT_SERVER_NAME).unwrap();
        let mut client = rustls::ClientConnection::new(Arc::new(client), name).unwrap();
        let mut server = rustls::ServerConnection::new(Arc::new(server)).unwrap();
        for _ in 0..4 {
            if !client.is_handshaking() && !server.is_handshaking() {
                return None;
            }
            let mut flight = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut flight).unwrap();
            }
            let mut unread = flight.as_slice();
            while !unread.is_empty() {
                server.read_tls(&mut unread).unwrap();
                server
                    .process_new_packets()
                    .expect("the listener accepts the dialer's own credential");
            }
            let mut flight = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut flight).unwrap();
            }
            let mut unread = flight.as_slice();
            while !unread.is_empty() {
                client.read_tls(&mut unread).unwrap();
                if let Err(error) = client.process_new_packets() {
                    return Some(error);
                }
            }
        }
        panic!("the handshake stalled");
    }

    /// TLS-7 and TLS-4 through a whole handshake, as rustls runs the two
    /// checks: a dial to a node's earlier ID completes only against a
    /// listener that signs with the keys that ID names.
    #[test]
    fn a_dial_completes_only_with_a_listener_that_holds_the_dialed_ids_keys() {
        use rustls::CertificateError;

        use crate::{CredentialKey, ED25519_KEY_TAG, ML_DSA_65_KEY_TAG};

        let node = NodeSecretKey::generate();
        let attacker = NodeSecretKey::generate();
        let earlier = node.public();
        let ml_dsa = |secret: &NodeSecretKey| {
            let key = secret.credential().key(ML_DSA_65_KEY_TAG).unwrap();
            CredentialKey::Full(key.to_vec())
        };
        // `secret`'s two keys, then `added` under a tag of its own.
        let with = |secret: &NodeSecretKey, added: CredentialKey| {
            let ed25519 = CredentialKey::Full(secret.public_key().as_bytes().to_vec());
            NodeCredential::from_entries([
                (ED25519_KEY_TAG, ed25519),
                (ML_DSA_65_KEY_TAG, ml_dsa(secret)),
                (4, added),
            ])
            .unwrap()
        };
        let refused = |error| Some(rustls::Error::InvalidCertificate(error));

        // The node itself, before and after it added a key.
        assert_eq!(dial_presenting(earlier, node.credential(), &node), None);
        let extended = with(&node, CredentialKey::Full(vec![0x5a; 1_312]));
        assert_ne!(extended.node_id(), earlier);
        assert_eq!(dial_presenting(earlier, &extended, &node), None);

        // The node's public credential, signed by someone else.
        assert_eq!(
            dial_presenting(earlier, node.credential(), &attacker),
            refused(CertificateError::BadSignature)
        );
        // The node's public keys beside the attacker's key: the ID check
        // passes, and the signature is checked with the node's keys alone.
        let beside = with(&node, ml_dsa(&attacker));
        assert_eq!(
            dial_presenting(earlier, &beside, &attacker),
            refused(CertificateError::BadSignature)
        );
        // The attacker's keys beside the node's key: a valid signature for
        // a credential that never names the node.
        let claiming = with(&attacker, ml_dsa(&node));
        assert_eq!(
            dial_presenting(claiming.node_id(), &claiming, &attacker),
            None
        );
        assert_eq!(
            dial_presenting(earlier, &claiming, &attacker),
            refused(CertificateError::UnknownIssuer)
        );
        // A node this release can't authenticate: its ID names only a key
        // under a later scheme's tag. Its record is public, so the attacker
        // copies that entry beside its own keys and signs validly. The
        // copied entry follows the attacker's keys, so no prefix of the
        // list is the node's.
        let later_key = (5, CredentialKey::Full(vec![0x77; 1_952]));
        let later_node = NodeCredential::from_entries([later_key.clone()]).unwrap();
        let copying = NodeCredential::from_entries([
            (
                ED25519_KEY_TAG,
                CredentialKey::Full(attacker.public_key().as_bytes().to_vec()),
            ),
            (ML_DSA_65_KEY_TAG, ml_dsa(&attacker)),
            later_key,
        ])
        .unwrap();
        assert!(!copying
            .key_list()
            .contains_list_named(later_node.node_id(), &[]));
        assert_eq!(
            dial_presenting(copying.node_id(), &copying, &attacker),
            None
        );
        assert_eq!(
            dial_presenting(later_node.node_id(), &copying, &attacker),
            refused(CertificateError::UnknownIssuer)
        );
        // An ID that names only the node's Ed25519 key is the hash of a
        // prefix of its credential's list. The prefix leaves out the
        // ML-DSA-65 key the handshake is also signed with, so the node's own
        // valid signature doesn't prove that ID.
        let ed25519_only = NodeCredential::single(node.public_key());
        assert!(node
            .credential()
            .key_list()
            .contains_list_named(ed25519_only.node_id(), &[ED25519_KEY_TAG]));
        assert_eq!(
            dial_presenting(ed25519_only.node_id(), node.credential(), &node),
            refused(CertificateError::UnknownIssuer)
        );
        // The node's Ed25519 key and the fingerprint of its ML-DSA-65 key,
        // which hash to the node's ID, beside the attacker's key in full.
        // The ID is committed to, but no key under the signing tag can
        // check a signature, so there is nothing for the attacker's to pass.
        let committed = node
            .credential()
            .clone()
            .with_fingerprint_only(ML_DSA_65_KEY_TAG)
            .unwrap();
        assert_eq!(committed.node_id(), earlier);
        let attacker_key = attacker.credential().key(ML_DSA_65_KEY_TAG).unwrap();
        let mut bytes = committed.encode();
        bytes[0] += 1;
        bytes.extend_from_slice(&4u16.to_le_bytes());
        bytes.extend_from_slice(&(attacker_key.len() as u16).to_le_bytes());
        bytes.extend_from_slice(attacker_key);
        let shaped = NodeCredential::decode(&bytes).unwrap();
        assert!(shaped
            .key_list()
            .contains_list_named(earlier, &[ED25519_KEY_TAG, ML_DSA_65_KEY_TAG]));
        assert_eq!(
            dial_presenting(earlier, &shaped, &attacker),
            refused(CertificateError::ApplicationVerificationFailure)
        );
    }

    /// TLS-7 through whole handshakes: a node that added two keys answers a
    /// dial to each ID it has had, which are the prefixes of its key list
    /// that keep both signing keys. The other lists of its entries that keep
    /// both signing keys are refused, though the node signs validly. Spec
    /// versions before 0.34 accepted them.
    #[test]
    fn a_dial_to_a_list_that_skips_an_entry_is_refused() {
        use rustls::CertificateError;

        use crate::{CredentialKey, ED25519_KEY_TAG, ML_DSA_65_KEY_TAG};

        let node = NodeSecretKey::generate();
        let own = [ED25519_KEY_TAG, ML_DSA_65_KEY_TAG].map(|tag| {
            let key = node.credential().key(tag).unwrap().to_vec();
            (tag, CredentialKey::Full(key))
        });
        let first = (4, CredentialKey::Full(vec![0x5a; 1_312]));
        let second = (5, CredentialKey::Fingerprint([0x6b; 32]));
        let with = |added: &[(u16, CredentialKey)]| {
            NodeCredential::from_entries(own.iter().chain(added).cloned()).unwrap()
        };
        let current = with(&[first.clone(), second.clone()]);

        for earlier in [with(&[]), with(&[first]), current.clone()] {
            assert_eq!(dial_presenting(earlier.node_id(), &current, &node), None);
        }
        let skipping = with(&[second]);
        assert_eq!(
            dial_presenting(skipping.node_id(), &current, &node),
            Some(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer
            ))
        );
    }

    /// TLS-7, ID-8 through a whole handshake: a credential with 32 entries
    /// authenticates on a dial to its own ID and to each earlier one, and
    /// the same entries presented by someone without the node's keys fail
    /// the signature.
    #[test]
    fn a_32_entry_credential_authenticates_only_its_holder() {
        use rustls::CertificateError;

        let node = crate::tests::largest_credential_key();
        let attacker = NodeSecretKey::generate();
        let credential = node.credential();
        assert_eq!(
            credential.key_list().encoded_len(),
            crate::MAX_NODE_KEY_LIST_BYTES
        );
        let current = credential.node_id();
        // The ID of the node's two signing keys alone.
        let original = NodeSecretKey::from_bytes(&node.to_bytes()).public();
        assert_ne!(original, current);

        for dialed in [original, current] {
            assert_eq!(dial_presenting(dialed, credential, &node), None);
            assert_eq!(
                dial_presenting(dialed, credential, &attacker),
                Some(rustls::Error::InvalidCertificate(
                    CertificateError::BadSignature
                ))
            );
        }
    }

    /// ID-12: a credential one byte above the limit is refused as a bad
    /// encoding when the dialer reads it, before any signature check, and
    /// the handshake fails with that error.
    #[test]
    fn a_credential_above_the_byte_limit_fails_the_handshake() {
        use rustls::CertificateError;

        let node = crate::tests::largest_credential_key();
        let credential = node.credential();
        // The last entry is a key of 2,224 bytes. One byte more keeps every
        // rule but the byte limit.
        let mut encoded = credential.encode();
        let length_at = encoded.len() - 2_224 - 2;
        assert_eq!(encoded[length_at..length_at + 2], 2_224u16.to_le_bytes());
        encoded[length_at..length_at + 2].copy_from_slice(&2_225u16.to_le_bytes());
        encoded.push(0);
        assert_eq!(encoded.len(), crate::MAX_CREDENTIAL_BYTES + 1);
        let spki = crate::credential::spki_of(&encoded).to_vec();
        assert_eq!(
            NodeCredential::from_spki(&spki),
            Err(crate::CredentialError::TooLong)
        );
        assert_eq!(
            dial_presenting_cert(credential.node_id(), spki, &node),
            Some(rustls::Error::InvalidCertificate(
                CertificateError::BadEncoding
            ))
        );
    }

    /// Runs one handshake between two production endpoints through rustls'
    /// QUIC interface, which hands over the handshake messages as QUIC
    /// carries them, without TLS records. Returns the bytes each side sends
    /// under the handshake keys: the listener's flight from its extensions
    /// to its Finished, and the dialer's credential, signature and Finished.
    fn handshake_flights(dialer: &NodeSecretKey, listener: &NodeSecretKey) -> (usize, usize) {
        use rustls::quic::{ClientConnection, KeyChange, ServerConnection, Version};

        // Stands in for the QUIC transport parameters. An endpoint's real
        // ones, and its ALPN, move the listener's flight by a few bytes.
        let parameters = vec![0; 128];
        let client = TlsConfig::new(dialer)
            .rustls_client(listener.public(), &[b"a"])
            .unwrap();
        let server = TlsConfig::new(listener)
            .rustls_server(vec![b"a".to_vec()])
            .unwrap();
        let name = rustls_pki_types::ServerName::try_from(UNSENT_SERVER_NAME).unwrap();
        let mut client =
            ClientConnection::new(Arc::new(client), Version::V1, name, parameters.clone()).unwrap();
        let mut server = ServerConnection::new(Arc::new(server), Version::V1, parameters).unwrap();

        let mut hello = Vec::new();
        assert!(client.write_hs(&mut hello).is_none());
        server.read_hs(&hello).unwrap();
        let mut server_hello = Vec::new();
        let keys = server.write_hs(&mut server_hello);
        assert!(matches!(keys, Some(KeyChange::Handshake { .. })));
        let mut listener_flight = Vec::new();
        let keys = server.write_hs(&mut listener_flight);
        assert!(matches!(keys, Some(KeyChange::OneRtt { .. })));

        client.read_hs(&server_hello).unwrap();
        let mut none = Vec::new();
        let keys = client.write_hs(&mut none);
        assert!(matches!(keys, Some(KeyChange::Handshake { .. })) && none.is_empty());
        client.read_hs(&listener_flight).unwrap();
        let mut dialer_flight = Vec::new();
        let keys = client.write_hs(&mut dialer_flight);
        assert!(matches!(keys, Some(KeyChange::OneRtt { .. })));
        server.read_hs(&dialer_flight).unwrap();
        assert!(!client.is_handshaking() && !server.is_handshaking());
        (listener_flight.len(), dialer_flight.len())
    }

    /// ID-12: the flight that carries the largest credential fits the
    /// handshake data `quinn-proto` buffers, in either direction, with more
    /// than 2 KiB to spare. A peer refuses a handshake frame that ends more
    /// than that buffer past the data it has read in order, so a larger
    /// flight completes only if the network delivers it nearly in order.
    /// This one completes whatever is lost or reordered.
    #[test]
    fn the_largest_credentials_flight_fits_the_handshake_buffer() {
        /// `quinn-proto`'s default `crypto_buffer_size`, which this crate
        /// keeps (`transport_profile_snapshot` pins it).
        const HANDSHAKE_BUFFER_BYTES: usize = 16 * 1024;

        let plain = NodeSecretKey::generate();
        let largest = crate::tests::largest_credential_key();
        assert_eq!(
            largest.credential().encode().len(),
            crate::MAX_CREDENTIAL_BYTES
        );
        // What a flight holds beside the credential: the composite
        // signature, the Finished and the message headers, and for the
        // listener its extensions and its request for the dialer's key.
        let (listener_flight, dialer_flight) = handshake_flights(&plain, &plain);
        let credential = plain.credential().encode().len();
        let beside = (listener_flight - credential, dialer_flight - credential);
        assert_eq!(beside, (3_650, 3_479));

        let (listener_flight, dialer_flight) = handshake_flights(&largest, &largest);
        assert_eq!(
            (listener_flight, dialer_flight),
            (
                crate::MAX_CREDENTIAL_BYTES + beside.0,
                crate::MAX_CREDENTIAL_BYTES + beside.1
            )
        );
        assert_eq!((listener_flight, dialer_flight), (14_035, 13_864));
        assert!(listener_flight + 2 * 1024 < HANDSHAKE_BUFFER_BYTES);
    }

    /// TLS-3, TLS-8: the ID a peer's verifier derives from a node's
    /// credential is the ID the node gives itself, including for a credential
    /// with an entry this release doesn't know.
    #[test]
    fn a_node_is_known_by_the_id_its_credential_derives() {
        let plain = NodeSecretKey::generate();
        let extended = NodeSecretKey::generate();
        let own = |tag| {
            let key = extended.credential().key(tag).unwrap().to_vec();
            (tag, key)
        };
        let credential = NodeCredential::from_entries([
            own(crate::ED25519_KEY_TAG),
            own(crate::ML_DSA_65_KEY_TAG),
            (0x0102, vec![0x5A; 1_312]),
        ])
        .unwrap();
        let extended = extended.with_credential(credential).unwrap();
        for secret in [plain, extended] {
            let cert = secret.credential().to_spki();
            let derived = credential_from_cert(cert.as_ref()).map(|remote| remote.node_id());
            assert_eq!(derived, Some(secret.public()));
        }
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

    /// M3.5: inside a simulated run, the Retry token time and the
    /// certificate-check time are the scenario's UTC: its origin plus
    /// virtual time, however much host time passes.
    #[cfg(zakura_sim)]
    #[test]
    fn runtime_utc_reads_scenario_time_in_a_simulated_run() {
        use zakura_scheduler::{Scheduler, SchedulerConfig, SimInstant};

        let origin = UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let config = SchedulerConfig {
            utc_origin: Some(origin),
            ..SchedulerConfig::default()
        };
        let mut run = Scheduler::new(config, SimInstant::ZERO);
        run.advance_to(SimInstant::from_start(std::time::Duration::from_secs(5)));
        std::thread::sleep(std::time::Duration::from_millis(20));
        let scenario = origin + std::time::Duration::from_secs(5);
        assert_eq!(quinn_proto::TimeSource::now(&RuntimeUtc), scenario);
        let tls_now = rustls::time_provider::TimeProvider::current_time(&RuntimeUtc).unwrap();
        assert_eq!(tls_now.as_secs(), 1_700_000_005);
        drop(run);
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
