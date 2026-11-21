//! Raw-public-key certificate verifiers (TLS-1 to TLS-8).
//!
//! Adapted from `iroh/src/tls/verifier.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.
//!
//! One change: Iroh decoded the expected server identity from a synthetic
//! server name. Zakura builds one verifier per dial that holds the expected
//! [`NodeKey`] in memory (TLS-6), so no name encoding is needed.

use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{verify_tls13_signature_with_raw_key, WebPkiSupportedAlgorithms},
    server::danger::{ClientCertVerified, ClientCertVerifier},
    CertificateError, DigitallySignedStruct, DistinguishedName, SignatureScheme,
    SupportedProtocolVersion,
};
use rustls_pki_types::{
    alg_id, AlgorithmIdentifier, CertificateDer, InvalidSignature, ServerName,
    SignatureVerificationAlgorithm, SubjectPublicKeyInfoDer, UnixTime,
};

use crate::key::NodeKey;

/// TLS 1.3 is the only version (TLS-1).
pub(crate) const PROTOCOL_VERSIONS: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];

const ED25519_STRICT: Ed25519Strict = Ed25519Strict;
const SUPPORTED_SIG_ALGS: WebPkiSupportedAlgorithms = WebPkiSupportedAlgorithms {
    all: &[&ED25519_STRICT],
    mapping: &[(SignatureScheme::ED25519, &[&ED25519_STRICT])],
};

/// Accepts the server only if its raw public key is the key the dialer
/// expects (TLS-7). A dialer that expects no particular key accepts any valid
/// Ed25519 key, and learns which one from the handshake.
#[derive(Debug)]
pub(crate) struct ServerCertificateVerifier {
    expected: Option<NodeKey>,
}

impl ServerCertificateVerifier {
    pub(crate) fn new(expected: Option<NodeKey>) -> Self {
        Self { expected }
    }
}

impl ServerCertVerifier for ServerCertificateVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if !intermediates.is_empty() {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer,
            ));
        }

        let acceptable = match &self.expected {
            // Comparing whole SPKI encodings checks both the constant Ed25519
            // prefix and the key bytes.
            Some(expected) => super::ed25519_spki(expected).as_ref() == end_entity.as_ref(),
            None => super::node_key_from_spki(end_entity.as_ref()).is_some(),
        };
        if !acceptable {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer,
            ));
        }

        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &SUPPORTED_SIG_ALGS,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        SUPPORTED_SIG_ALGS.supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// Requires a client raw public key (TLS-5).
///
/// rustls verifies the handshake signature against the presented key, which
/// proves possession of the key that determines the client's node ID (TLS-8).
#[derive(Debug, Default)]
pub(super) struct ClientCertificateVerifier;

impl ClientCertVerifier for ClientCertificateVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        if !intermediates.is_empty() {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer,
            ));
        }
        if super::node_key_from_spki(end_entity.as_ref()).is_none() {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::BadEncoding,
            ));
        }
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &SUPPORTED_SIG_ALGS,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        SUPPORTED_SIG_ALGS.supported_schemes()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// Ed25519 with `verify_strict` (ID-6).
#[derive(Debug)]
struct Ed25519Strict;

impl SignatureVerificationAlgorithm for Ed25519Strict {
    fn verify_signature(
        &self,
        public_key: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), InvalidSignature> {
        let key = NodeKey::try_from(public_key).map_err(|_| InvalidSignature)?;
        key.verify_strict(message, signature)
            .map_err(|_| InvalidSignature)
    }

    fn public_key_alg_id(&self) -> AlgorithmIdentifier {
        alg_id::ED25519
    }

    fn signature_alg_id(&self) -> AlgorithmIdentifier {
        alg_id::ED25519
    }

    fn fips(&self) -> bool {
        false
    }
}
