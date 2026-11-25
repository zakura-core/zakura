//! Raw-public-key certificate verifiers (TLS-1 to TLS-8).
//!
//! Adapted from `iroh/src/tls/verifier.rs` at fork tag `zakura-iroh-v1.1.0-rc.1`.
//! Copyright 2025 N0, INC. Licensed under MIT OR Apache-2.0.
//!
//! Two changes from Iroh. Iroh decoded the expected server identity from a
//! synthetic server name; Zakura builds one verifier per dial that holds the
//! expected [`NodeId`] in memory (TLS-6). And the raw public key is the
//! node's credential, not a bare Ed25519 key (TLS-3), and the signature is a
//! composite of Ed25519 and ML-DSA-65 (TLS-4), so both verifiers read the
//! credential through [`super::credential_from_cert`] and check the
//! signature with its two keys themselves.

use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    server::danger::{ClientCertVerified, ClientCertVerifier},
    CertificateError, DigitallySignedStruct, DistinguishedName, SignatureScheme,
    SupportedProtocolVersion,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

use crate::{
    composite::{self, COMPOSITE_SCHEME, COMPOSITE_SCHEME_LABEL, ML_DSA_65_KEY_TAG},
    credential::NodeCredential,
    key::NodeId,
};

/// TLS 1.3 is the only version (TLS-1).
pub(crate) const PROTOCOL_VERSIONS: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];

/// The signature schemes a peer may sign its handshake with (TLS-4).
const SIGNATURE_SCHEMES: &[SignatureScheme] = &[COMPOSITE_SCHEME];

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
/// The handshake signature, checked with both of the credential's keys,
/// proves the client holds the credential its node ID derives from (TLS-8).
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
/// stands alone and holds both an Ed25519 and an ML-DSA-65 key (TLS-3,
/// TLS-4, ID-11).
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
    // A well-formed credential without both keys this release signs with
    // is unverifiable, not malformed (ID-11): the handshake fails either way.
    if credential.ed25519().is_none() || credential.key(ML_DSA_65_KEY_TAG).is_none() {
        return Err(rustls::Error::InvalidCertificate(
            CertificateError::ApplicationVerificationFailure,
        ));
    }
    Ok(credential)
}

/// Checks the peer's composite handshake signature with its credential's
/// Ed25519 key, under ID-6's strict rules, and its ML-DSA-65 key (TLS-4).
fn verify_signature(
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
) -> Result<HandshakeSignatureValid, rustls::Error> {
    if dss.scheme != COMPOSITE_SCHEME {
        return Err(rustls::Error::PeerMisbehaved(
            rustls::PeerMisbehaved::SignedHandshakeWithUnadvertisedSigScheme,
        ));
    }
    // rustls calls this only after a verifier accepted `cert`, so the
    // credential parses and holds both keys.
    let bad_encoding = rustls::Error::InvalidCertificate(CertificateError::BadEncoding);
    let credential = super::credential_from_cert(cert.as_ref()).ok_or(bad_encoding.clone())?;
    let (Some(ed25519), Some(ml_dsa)) = (credential.ed25519(), credential.key(ML_DSA_65_KEY_TAG))
    else {
        return Err(bad_encoding);
    };
    composite::verify(&ed25519, ml_dsa, message, dss.signature())
        .map_err(|_| rustls::Error::InvalidCertificate(CertificateError::BadSignature))?;
    metrics::counter!("zakura.quic.tls.signature_verified", "scheme" => COMPOSITE_SCHEME_LABEL)
        .increment(1);
    Ok(HandshakeSignatureValid::assertion())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeSecretKey, ED25519_KEY_TAG};

    fn cert(credential: &NodeCredential) -> CertificateDer<'static> {
        CertificateDer::from(credential.to_spki().to_vec())
    }

    /// TLS-4, ID-11: a credential with only an Ed25519 key can't sign the
    /// composite, so both verifiers refuse it, even when its ID is the
    /// dialed one.
    #[test]
    fn a_credential_without_an_ml_dsa_key_is_refused() {
        let secret = NodeSecretKey::generate();
        let ed25519_only = NodeCredential::single(secret.public_key());
        let refused = verifiable_credential(&cert(&ed25519_only), &[]);
        assert!(refused.is_err());

        let verifier = ServerCertificateVerifier::new(ed25519_only.node_id());
        let name = ServerName::try_from(super::super::UNSENT_SERVER_NAME).unwrap();
        assert!(verifier
            .verify_server_cert(&cert(&ed25519_only), &[], &name, &[], UnixTime::now())
            .is_err());
        assert!(verifiable_credential(&cert(secret.credential()), &[]).is_ok());
    }

    /// ID-10, ID-11, ID-13: a `p2p-v2/4` credential, whose ML-DSA-44 key sits
    /// under tag 2, still decodes and names its node, but tag 2 is an
    /// unknown entry now, so it holds no key this release authenticates with.
    #[test]
    fn a_p2p_v2_4_credential_decodes_but_is_refused() {
        let secret = NodeSecretKey::generate();
        let previous = NodeCredential::from_entries([
            (ED25519_KEY_TAG, secret.public_key().as_bytes().to_vec()),
            (2, vec![0x44; 1_312]),
        ])
        .expect("an Ed25519 key and an unknown entry form a credential");
        assert_eq!(
            NodeCredential::decode(&previous.encode()),
            Ok(previous.clone())
        );
        assert_eq!(previous.key(ML_DSA_65_KEY_TAG), None);
        assert!(verifiable_credential(&cert(&previous), &[]).is_err());
    }

    /// ID-12, TLS-3: a credential from a draft of `p2p-v2/5` that signed with
    /// ML-DSA-87 holds a 2,592-byte key under tag 3. Tag 3 is ML-DSA-65 now,
    /// fixed at 1,952 bytes, so the credential is malformed, not merely
    /// unverifiable, and both verifiers refuse it as a bad encoding.
    #[test]
    fn an_ml_dsa_87_credential_under_tag_3_is_malformed() {
        const ML_DSA_87_KEY_BYTES: usize = 2_592;
        let secret = NodeSecretKey::generate();
        // Build the entry under an unknown tag, which takes any length up
        // to the bound, then move it to tag 3 in the encoding.
        let unknown_tag: u16 = 4;
        let draft = NodeCredential::from_entries([
            (ED25519_KEY_TAG, secret.public_key().as_bytes().to_vec()),
            (unknown_tag, vec![0x87; ML_DSA_87_KEY_BYTES]),
        ])
        .expect("an Ed25519 key and an unknown entry form a credential");
        let mut spki = draft.to_spki().as_ref().to_vec();
        // The encoded credential ends the SubjectPublicKeyInfo, and its
        // second entry's tag follows the count byte and the Ed25519 entry.
        let tag_at = spki.len() - draft.encode().len() + 1 + 4 + 32;
        assert_eq!(spki[tag_at..tag_at + 2], unknown_tag.to_le_bytes());
        spki[tag_at..tag_at + 2].copy_from_slice(&ML_DSA_65_KEY_TAG.to_le_bytes());

        assert_eq!(
            NodeCredential::from_spki(&spki),
            Err(crate::CredentialError::KeyLength)
        );
        assert_eq!(
            verifiable_credential(&CertificateDer::from(spki), &[]),
            Err(rustls::Error::InvalidCertificate(
                CertificateError::BadEncoding
            ))
        );
    }

    /// TLS-4: a credential that pairs this node's Ed25519 key with another
    /// node's ML-DSA key fails the ML-DSA half of the composite.
    #[test]
    fn a_credential_with_a_swapped_ml_dsa_key_fails_the_signature() {
        let secret = NodeSecretKey::generate();
        let other = NodeSecretKey::generate();
        let message = b"transcript";
        let signature = secret.sign_composite(message, [3; 32]).unwrap();
        let ml_dsa =
            |node: &NodeSecretKey| node.credential().key(ML_DSA_65_KEY_TAG).unwrap().to_vec();
        assert!(
            composite::verify(&secret.public_key(), &ml_dsa(&secret), message, &signature).is_ok()
        );

        let swapped = NodeCredential::from_entries([
            (ED25519_KEY_TAG, secret.public_key().as_bytes().to_vec()),
            (ML_DSA_65_KEY_TAG, ml_dsa(&other)),
        ])
        .unwrap();
        assert!(verifiable_credential(&cert(&swapped), &[]).is_ok());
        assert!(composite::verify(
            &secret.public_key(),
            swapped.key(ML_DSA_65_KEY_TAG).unwrap(),
            message,
            &signature
        )
        .is_err());
    }
}
