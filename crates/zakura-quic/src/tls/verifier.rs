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
    composite::{
        self, COMPOSITE_KEY_TAGS, COMPOSITE_SCHEME, COMPOSITE_SCHEME_LABEL, ML_DSA_65_KEY_TAG,
    },
    credential::NodeCredential,
    key::NodeId,
};

/// TLS 1.3 is the only version (TLS-1).
pub(crate) const PROTOCOL_VERSIONS: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];

/// The signature schemes a peer may sign its handshake with (TLS-4).
const SIGNATURE_SCHEMES: &[SignatureScheme] = &[COMPOSITE_SCHEME];

/// Accepts the server only if the keys it signs with prove it is the dialed
/// node (TLS-7): its credential derives the dialed node ID, or extends the
/// credential that did.
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
        // The composite is the only scheme offered, so its keys are the
        // ones the signature check below will use.
        if !credential.proves_dialed_id(self.expected, &COMPOSITE_KEY_TAGS) {
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
    let credential = super::credential_from_cert(cert.as_ref()).ok_or(bad_encoding)?;
    verify_composite(&credential, message, dss.signature())?;
    metrics::counter!("zakura.quic.tls.signature_verified", "scheme" => COMPOSITE_SCHEME_LABEL)
        .increment(1);
    Ok(HandshakeSignatureValid::assertion())
}

/// Checks a composite `signature` over `message` with the two keys
/// `credential` holds under [`COMPOSITE_KEY_TAGS`], and with no other entry.
fn verify_composite(
    credential: &NodeCredential,
    message: &[u8],
    signature: &[u8],
) -> Result<(), rustls::Error> {
    let (Some(ed25519), Some(ml_dsa)) = (credential.ed25519(), credential.key(ML_DSA_65_KEY_TAG))
    else {
        return Err(rustls::Error::InvalidCertificate(
            CertificateError::BadEncoding,
        ));
    };
    composite::verify(&ed25519, ml_dsa, message, signature)
        .map_err(|_| rustls::Error::InvalidCertificate(CertificateError::BadSignature))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CredentialKey, NodeSecretKey, ED25519_KEY_TAG};

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

    fn accepts(dialed: NodeId, presented: &NodeCredential) -> bool {
        let name = ServerName::try_from(super::super::UNSENT_SERVER_NAME).unwrap();
        ServerCertificateVerifier::new(dialed)
            .verify_server_cert(&cert(presented), &[], &name, &[], UnixTime::now())
            .is_ok()
    }

    /// `secret`'s credential with `extra` entries after its own two.
    fn extended(
        secret: &NodeSecretKey,
        extra: impl IntoIterator<Item = (u16, CredentialKey)>,
    ) -> NodeCredential {
        let own = [ED25519_KEY_TAG, ML_DSA_65_KEY_TAG].map(|tag| {
            let key = secret.credential().key(tag).unwrap().to_vec();
            (tag, CredentialKey::Full(key))
        });
        NodeCredential::from_entries(own.into_iter().chain(extra)).unwrap()
    }

    /// TLS-7: a node that added keys still answers a dial to the ID its two
    /// signing keys had on their own, and to its current ID.
    #[test]
    fn a_dial_to_an_earlier_id_accepts_the_extended_credential() {
        let secret = NodeSecretKey::generate();
        let earlier = secret.public();
        let with_key = extended(&secret, [(4, CredentialKey::Full(vec![0x5a; 1_312]))]);
        let with_fingerprint = extended(&secret, [(4, CredentialKey::Fingerprint([0x5a; 32]))]);
        let with_two = extended(
            &secret,
            [
                (4, CredentialKey::Full(vec![0x5a; 1_312])),
                (5, CredentialKey::Fingerprint([0x6b; 32])),
            ],
        );

        assert!(accepts(earlier, secret.credential()));
        for credential in [&with_key, &with_fingerprint, &with_two] {
            assert_ne!(credential.node_id(), earlier);
            assert!(accepts(earlier, credential));
            assert!(accepts(credential.node_id(), credential));
        }
        // The ID the node had between the two additions.
        assert!(accepts(with_key.node_id(), &with_two));
        assert!(!accepts(with_two.node_id(), &with_key));
        assert!(!accepts(NodeSecretKey::generate().public(), &with_two));
    }

    /// TLS-7: the dialed ID must commit to both signing keys. An ID whose
    /// list leaves one out is refused, though the credential holds every
    /// entry of that list.
    #[test]
    fn a_dialed_id_that_leaves_a_signing_key_out_is_refused() {
        let secret = NodeSecretKey::generate();
        let ed25519 = secret.public_key().as_bytes().to_vec();
        let ml_dsa = secret.credential().key(ML_DSA_65_KEY_TAG).unwrap().to_vec();
        let other = vec![0x5a; 1_312];
        let presented = extended(&secret, [(4, CredentialKey::Full(other.clone()))]);
        let id_of =
            |entries: Vec<(u16, Vec<u8>)>| NodeCredential::from_entries(entries).unwrap().node_id();

        assert!(!accepts(
            id_of(vec![(ED25519_KEY_TAG, ed25519.clone())]),
            &presented
        ));
        assert!(!accepts(
            id_of(vec![(ED25519_KEY_TAG, ed25519), (4, other.clone())]),
            &presented
        ));
        assert!(!accepts(
            id_of(vec![(ML_DSA_65_KEY_TAG, ml_dsa), (4, other)]),
            &presented
        ));
    }

    /// TLS-4, ID-12: a fingerprint-only ML-DSA-65 entry keeps the node ID
    /// but can't check a signature, so both verifiers refuse the credential
    /// even on a dial to exactly its ID.
    #[test]
    fn a_signing_key_given_as_a_fingerprint_is_refused() {
        let secret = NodeSecretKey::generate();
        let retired = secret
            .credential()
            .clone()
            .with_fingerprint_only(ML_DSA_65_KEY_TAG)
            .unwrap();
        assert_eq!(retired.node_id(), secret.public());

        assert!(!retired.proves_dialed_id(secret.public(), &COMPOSITE_KEY_TAGS));
        assert!(!accepts(secret.public(), &retired));
        assert_eq!(
            verifiable_credential(&cert(&retired), &[]),
            Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure
            ))
        );
        assert!(verify_composite(&retired, b"transcript", &[0; 3_373]).is_err());
        // Nor can the node itself be set to present it.
        assert!(secret.with_credential(retired).is_err());
    }

    /// TLS-7: anyone can present a node's public keys beside a key of their
    /// own. The credential passes for a dial to that node, and the handshake
    /// then fails, because the signature is checked with the node's two keys
    /// and never with the added one.
    #[test]
    fn a_nodes_keys_beside_an_attackers_key_fail_the_signature() {
        let node = NodeSecretKey::generate();
        let attacker = NodeSecretKey::generate();
        let attacker_key = attacker.credential().key(ML_DSA_65_KEY_TAG).unwrap();
        let forged = extended(&node, [(4, CredentialKey::Full(attacker_key.to_vec()))]);
        let message = b"transcript";

        assert!(accepts(node.public(), &forged));
        let attackers = attacker.sign_composite(message, [3; 32]).unwrap();
        assert_eq!(
            verify_composite(&forged, message, &attackers),
            Err(rustls::Error::InvalidCertificate(
                CertificateError::BadSignature
            ))
        );
        let nodes = node.sign_composite(message, [3; 32]).unwrap();
        assert_eq!(verify_composite(&forged, message, &nodes), Ok(()));

        // The attacker's own credential, extended with the node's keys as
        // extra entries, signs validly but never names the node.
        let node_key = node.credential().key(ML_DSA_65_KEY_TAG).unwrap();
        let claiming = extended(&attacker, [(4, CredentialKey::Full(node_key.to_vec()))]);
        assert_eq!(verify_composite(&claiming, message, &attackers), Ok(()));
        assert!(!accepts(node.public(), &claiming));
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
