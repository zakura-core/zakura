//! Raw-public-key certificate verifiers (TLS-1 to TLS-8).
//!
//! Adapted from `iroh/src/tls/verifier.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.
//!
//! Two changes from Iroh. Iroh decoded the expected server identity from a
//! synthetic server name; Zakura builds one verifier per dial that holds the
//! expected [`NodeId`] in memory (TLS-6). And the raw public key is the
//! node's credential, not a bare Ed25519 key (TLS-3), so both verifiers read
//! it through [`super::credential_from_cert`] and check the signature with
//! the credential's Ed25519 key themselves.

use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    server::danger::{ClientCertVerified, ClientCertVerifier},
    CertificateError, DigitallySignedStruct, DistinguishedName, SignatureScheme,
    SupportedProtocolVersion,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

use crate::{credential::NodeCredential, key::NodeId};

/// TLS 1.3 is the only version (TLS-1).
pub(crate) const PROTOCOL_VERSIONS: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];

/// The signature schemes a peer may sign its handshake with (TLS-4).
const SIGNATURE_SCHEMES: &[SignatureScheme] = &[SignatureScheme::ED25519];

/// Accepts the server only if its credential derives the dialed node ID (TLS-7).
#[derive(Debug)]
pub(crate) struct ServerCertificateVerifier {
    expected: NodeId,
}

impl ServerCertificateVerifier {
    pub(crate) fn new(expected: NodeId) -> Self {
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
        let credential = verifiable_credential(end_entity, intermediates)?;
        if credential.node_id() != self.expected {
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
        verify_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        SIGNATURE_SCHEMES.to_vec()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// Requires a client credential (TLS-5).
///
/// The handshake signature, checked with the credential's Ed25519 key, proves
/// the client holds the credential its node ID derives from (TLS-8).
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
        verifiable_credential(end_entity, intermediates)?;
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
        verify_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        SIGNATURE_SCHEMES.to_vec()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// Reads a peer's credential and checks this release can authenticate it: it
/// stands alone and holds an Ed25519 key (TLS-3, ID-11).
fn verifiable_credential(
    end_entity: &CertificateDer<'_>,
    intermediates: &[CertificateDer<'_>],
) -> Result<NodeCredential, rustls::Error> {
    if !intermediates.is_empty() {
        return Err(rustls::Error::InvalidCertificate(
            CertificateError::UnknownIssuer,
        ));
    }
    let credential = super::credential_from_cert(end_entity.as_ref()).ok_or(
        rustls::Error::InvalidCertificate(CertificateError::BadEncoding),
    )?;
    // A well-formed credential with no key this release can check is
    // unverifiable, not malformed (ID-11): the handshake fails either way.
    if credential.ed25519().is_none() {
        return Err(rustls::Error::InvalidCertificate(
            CertificateError::ApplicationVerificationFailure,
        ));
    }
    Ok(credential)
}

/// Checks the peer's handshake signature with its credential's Ed25519 key
/// under ID-6's strict rules (TLS-4).
fn verify_signature(
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
) -> Result<HandshakeSignatureValid, rustls::Error> {
    if dss.scheme != SignatureScheme::ED25519 {
        return Err(rustls::Error::PeerMisbehaved(
            rustls::PeerMisbehaved::SignedHandshakeWithUnadvertisedSigScheme,
        ));
    }
    // rustls calls this only after a verifier accepted `cert`.
    let key = super::credential_from_cert(cert.as_ref())
        .and_then(|credential| credential.ed25519())
        .ok_or(rustls::Error::InvalidCertificate(
            CertificateError::BadEncoding,
        ))?;
    key.verify_strict(message, dss.signature())
        .map_err(|_| rustls::Error::InvalidCertificate(CertificateError::BadSignature))?;
    Ok(HandshakeSignatureValid::assertion())
}
