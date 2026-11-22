//! A node's TLS credential: its full public keys, which TLS proves and from
//! which the node ID derives (SPEC ID-12, TLS-3).
//!
//! A [`NodeKeyList`] names a node in 32-byte entries, short enough for every
//! discovery record. A key longer than 32 bytes appears there as its
//! fingerprint. The credential carries each key in full, so a verifier can
//! check signatures with it, and maps back to the key list by fingerprinting:
//! the node ID a handshake proves is always the ID of
//! [`NodeCredential::key_list`].

use blake2b_simd::Params;
use rustls_pki_types::{AlgorithmIdentifier, SubjectPublicKeyInfoDer};

use crate::{
    key::{NodeId, NodeKey, KEY_LENGTH},
    key_list::{KeyListError, NodeKeyList, ED25519_KEY_TAG, MAX_NODE_KEYS},
};

/// The longest key one credential entry may hold, in bytes: an ML-DSA-87
/// public key, the largest of the NIST signature schemes.
pub const MAX_CREDENTIAL_KEY_BYTES: usize = 2_592;

/// The longest encoded credential: the count byte, then [`MAX_NODE_KEYS`]
/// entries of a tag, a length and [`MAX_CREDENTIAL_KEY_BYTES`].
pub const MAX_CREDENTIAL_BYTES: usize = 1 + MAX_NODE_KEYS * (4 + MAX_CREDENTIAL_KEY_BYTES);

/// The BLAKE2b personalisation of a key's fingerprint (ID-12).
const FINGERPRINT_PERSONALISATION: &[u8; 16] = b"Zakura_KeyFpr_v1";

/// The algorithm of a credential's SubjectPublicKeyInfo (TLS-3): the DER
/// object identifier `2.25.141156145174207769675414719167485054362`, the
/// X.667 arc for UUID `6a31af47-5aa9-48c7-8ab9-d117edca0d9a`, with no
/// parameters. A UUID arc needs no registration and can't collide.
const CREDENTIAL_ALGORITHM: &[u8] = &[
    0x06, 0x14, 0x69, 0x81, 0xd4, 0xb1, 0xd7, 0xd1, 0xeb, 0xaa, 0xca, 0xa3, 0x8f, 0x8a, 0xdc, 0xf4,
    0xa2, 0xfe, 0xee, 0xa8, 0x9b, 0x1a,
];

/// Why bytes are not a credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CredentialError {
    /// The bytes are not a SubjectPublicKeyInfo with the credential algorithm.
    #[error("the certificate is not a Zakura key credential")]
    Encoding,
    /// The entries break a key-list rule (ID-8).
    #[error(transparent)]
    KeyList(#[from] KeyListError),
    /// A key is empty, longer than [`MAX_CREDENTIAL_KEY_BYTES`], or not the
    /// length its tag fixes.
    #[error("a credential key has the wrong length")]
    KeyLength,
}

/// A node's public keys in full, in tag order (ID-12).
///
/// Encoded as a count byte, then each entry as its tag (little-endian `u16`),
/// its length (little-endian `u16`) and its bytes. The rules on count and tags
/// are the key list's (ID-8). An Ed25519 entry is a checked 32-byte
/// [`NodeKey`]. An entry with an unknown tag is kept byte for byte, so a
/// release reaches the same node ID for a node holding a key it doesn't know.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeCredential {
    entries: Vec<(u16, Vec<u8>)>,
    /// The fingerprinted list, built once: every handshake reads its ID.
    key_list: NodeKeyList,
}

impl NodeCredential {
    /// The credential of a node whose only key is the Ed25519 key `key`.
    pub fn single(key: NodeKey) -> Self {
        Self::from_entries([(ED25519_KEY_TAG, key.as_bytes().to_vec())])
            .expect("one valid Ed25519 entry is a credential")
    }

    /// Builds a credential from `(tag, key)` entries in increasing tag order.
    pub fn from_entries(
        entries: impl IntoIterator<Item = (u16, Vec<u8>)>,
    ) -> Result<Self, CredentialError> {
        let entries: Vec<_> = entries.into_iter().take(MAX_NODE_KEYS + 1).collect();
        for (tag, key) in &entries {
            let fixed = (*tag == ED25519_KEY_TAG).then_some(KEY_LENGTH);
            let fits = match fixed {
                Some(length) => key.len() == length,
                None => !key.is_empty() && key.len() <= MAX_CREDENTIAL_KEY_BYTES,
            };
            if !fits {
                return Err(CredentialError::KeyLength);
            }
        }
        // The key list checks the count, the tag order and the Ed25519 point.
        let key_list = NodeKeyList::from_entries(
            entries
                .iter()
                .map(|(tag, key)| (*tag, fingerprint(*tag, key))),
        )?;
        Ok(Self { entries, key_list })
    }

    /// Decodes a credential from exactly `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<Self, CredentialError> {
        let (&count, mut rest) = bytes.split_first().ok_or(CredentialError::Encoding)?;
        let count = usize::from(count);
        if count == 0 || count > MAX_NODE_KEYS {
            return Err(KeyListError::EntryCount.into());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let (header, body) = rest
                .split_first_chunk::<4>()
                .ok_or(CredentialError::Encoding)?;
            let tag = u16::from_le_bytes([header[0], header[1]]);
            let length = usize::from(u16::from_le_bytes([header[2], header[3]]));
            if length > MAX_CREDENTIAL_KEY_BYTES {
                return Err(CredentialError::KeyLength);
            }
            let (key, tail) = body
                .split_at_checked(length)
                .ok_or(CredentialError::Encoding)?;
            entries.push((tag, key.to_vec()));
            rest = tail;
        }
        if !rest.is_empty() {
            return Err(CredentialError::Encoding);
        }
        Self::from_entries(entries)
    }

    /// The credential's canonical bytes, which [`Self::decode`] reads back.
    pub fn encode(&self) -> Vec<u8> {
        let length = 1 + self
            .entries
            .iter()
            .map(|(_, key)| 4 + key.len())
            .sum::<usize>();
        let mut bytes = Vec::with_capacity(length);
        // A credential holds at most `MAX_NODE_KEYS` entries.
        bytes.push(self.entries.len() as u8);
        for (tag, key) in &self.entries {
            bytes.extend_from_slice(&tag.to_le_bytes());
            // Construction bounds each key by `MAX_CREDENTIAL_KEY_BYTES`.
            bytes.extend_from_slice(&(key.len() as u16).to_le_bytes());
            bytes.extend_from_slice(key);
        }
        bytes
    }

    /// The key list naming this node: each key as its fingerprint.
    pub fn key_list(&self) -> &NodeKeyList {
        &self.key_list
    }

    /// The node ID this credential proves: the ID of [`Self::key_list`].
    pub fn node_id(&self) -> NodeId {
        self.key_list.node_id()
    }

    /// The node's Ed25519 key, if the credential has one.
    pub fn ed25519(&self) -> Option<NodeKey> {
        self.key_list.ed25519()
    }

    /// The full key with `tag`, if the credential has one.
    pub fn key(&self, tag: u16) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|(entry_tag, _)| *entry_tag == tag)
            .map(|(_, key)| key.as_slice())
    }

    /// The SubjectPublicKeyInfo a node presents as its TLS raw public key
    /// (TLS-3).
    pub(crate) fn to_spki(&self) -> SubjectPublicKeyInfoDer<'static> {
        rustls::sign::public_key_to_spki(
            &AlgorithmIdentifier::from_slice(CREDENTIAL_ALGORITHM),
            self.encode(),
        )
    }

    /// Reads a credential back from exactly the SubjectPublicKeyInfo
    /// [`Self::to_spki`] produces.
    pub(crate) fn from_spki(spki: &[u8]) -> Result<Self, CredentialError> {
        let encoded = spki_key(spki).ok_or(CredentialError::Encoding)?;
        let credential = Self::decode(encoded)?;
        // Re-encoding rules out every other DER form of the same contents.
        if credential.to_spki().as_ref() != spki {
            return Err(CredentialError::Encoding);
        }
        Ok(credential)
    }
}

/// A key's 32-byte entry in the key list (ID-12): an Ed25519 key is its own
/// entry, and any other key is its BLAKE2b-256 hash, personalised and keyed
/// by nothing but the tag.
fn fingerprint(tag: u16, key: &[u8]) -> [u8; KEY_LENGTH] {
    let mut value = [0; KEY_LENGTH];
    if tag == ED25519_KEY_TAG {
        value.copy_from_slice(key);
        return value;
    }
    let hash = Params::new()
        .hash_length(KEY_LENGTH)
        .personal(FINGERPRINT_PERSONALISATION)
        .to_state()
        .update(&tag.to_le_bytes())
        .update(key)
        .finalize();
    value.copy_from_slice(hash.as_bytes());
    value
}

/// Returns the BIT STRING contents of a SubjectPublicKeyInfo whose algorithm
/// is the credential's, or `None` for anything else. The caller checks the
/// encoding is canonical.
fn spki_key(spki: &[u8]) -> Option<&[u8]> {
    let (outer, rest) = der_element(spki, 0x30)?;
    if !rest.is_empty() {
        return None;
    }
    let (algorithm, rest) = der_element(outer, 0x30)?;
    if algorithm != CREDENTIAL_ALGORITHM {
        return None;
    }
    let (bits, rest) = der_element(rest, 0x03)?;
    if !rest.is_empty() {
        return None;
    }
    // A key is a whole number of bytes: no unused bits.
    bits.strip_prefix(&[0])
}

/// Splits one DER element with `tag` off the front of `bytes`, returning its
/// contents and the bytes after it. Lengths up to two bytes long are read,
/// which covers [`MAX_CREDENTIAL_BYTES`].
fn der_element(bytes: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    let (&found, rest) = bytes.split_first()?;
    if found != tag {
        return None;
    }
    let (&first, rest) = rest.split_first()?;
    let (length, rest) = match first {
        0..=0x7f => (usize::from(first), rest),
        0x81 => {
            let (&length, rest) = rest.split_first()?;
            (usize::from(length), rest)
        }
        0x82 => {
            let (length, rest) = rest.split_first_chunk::<2>()?;
            (usize::from(u16::from_be_bytes(*length)), rest)
        }
        _ => return None,
    };
    rest.split_at_checked(length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeSecretKey;

    /// A tag this release doesn't define.
    const UNKNOWN_TAG: u16 = 0x0102;

    fn ed25519() -> NodeKey {
        NodeSecretKey::from_bytes(&[7; KEY_LENGTH]).public_key()
    }

    #[test]
    fn an_ed25519_only_credential_names_the_one_entry_list() {
        let credential = NodeCredential::single(ed25519());
        assert_eq!(credential.key_list(), &NodeKeyList::single(ed25519()));
        assert_eq!(credential.node_id(), ed25519().node_id());
        assert_eq!(credential.ed25519(), Some(ed25519()));
    }

    #[test]
    fn a_long_key_enters_the_list_as_its_fingerprint() {
        let long = vec![0xAB; 1_312];
        let credential = NodeCredential::from_entries([
            (ED25519_KEY_TAG, ed25519().as_bytes().to_vec()),
            (UNKNOWN_TAG, long.clone()),
        ])
        .expect("two increasing entries form a credential");
        let expected = NodeKeyList::from_entries([
            (ED25519_KEY_TAG, *ed25519().as_bytes()),
            (UNKNOWN_TAG, fingerprint(UNKNOWN_TAG, &long)),
        ])
        .expect("two increasing entries form a list");

        assert_eq!(credential.key_list(), &expected);
        assert_eq!(credential.key(UNKNOWN_TAG), Some(long.as_slice()));
        // The fingerprint covers the tag, so the same bytes under another
        // tag name another key.
        assert_ne!(fingerprint(UNKNOWN_TAG, &long), fingerprint(3, &long));
    }

    /// The Regtest e2e harness names its seed node by this ID
    /// (`docker/zakura-regtest-e2e/node2.toml`). Computed with Python's
    /// `hashlib.blake2b(digest_size=32, person=b"Zakura_NodeId_v1")` over
    /// the one-entry list of the seed's public key.
    #[test]
    fn the_e2e_seed_nodes_id_has_its_known_value() {
        let secret = NodeSecretKey::from_bytes(&[1; KEY_LENGTH]);
        assert_eq!(
            secret.public().to_string(),
            "5cb9925880507c23dcf120a5a87e0c7577e1c16cb0ea62488d01174175550a91"
        );
    }

    #[test]
    fn a_fingerprint_has_its_known_value() {
        // Computed with Python's `hashlib.blake2b(digest_size=32,
        // person=b"Zakura_KeyFpr_v1")` over the tag's two little-endian bytes
        // and the key.
        assert_eq!(
            hex::encode(fingerprint(2, &[0xAB; 4])),
            "7ace21a146bcda05d7b4257192a0f3bcf0eec65249ea953d3aa629ea9cd7685e"
        );
    }

    #[test]
    fn a_credential_round_trips_through_its_spki() {
        let credential = NodeCredential::from_entries([
            (ED25519_KEY_TAG, ed25519().as_bytes().to_vec()),
            (UNKNOWN_TAG, vec![0xCD; 1_312]),
        ])
        .expect("two increasing entries form a credential");
        let spki = credential.to_spki();

        assert_eq!(
            NodeCredential::from_spki(spki.as_ref()),
            Ok(credential.clone())
        );
        assert_eq!(NodeCredential::decode(&credential.encode()), Ok(credential));
    }

    #[test]
    fn other_certificates_are_not_credentials() {
        let ed25519_spki = crate::tls::ed25519_spki(&ed25519());
        assert_eq!(
            NodeCredential::from_spki(ed25519_spki.as_ref()),
            Err(CredentialError::Encoding)
        );

        let spki = NodeCredential::single(ed25519())
            .to_spki()
            .as_ref()
            .to_vec();
        let mut trailing = spki.clone();
        trailing.push(0);
        assert_eq!(
            NodeCredential::from_spki(&trailing),
            Err(CredentialError::Encoding)
        );
        // A long-form length where the short form fits is not canonical DER.
        let mut long_form = vec![0x30, 0x81, spki[1]];
        long_form.extend_from_slice(&spki[2..]);
        assert_eq!(
            NodeCredential::from_spki(&long_form),
            Err(CredentialError::Encoding)
        );
    }

    #[test]
    fn malformed_credentials_are_refused() {
        let ed = ed25519().as_bytes().to_vec();
        assert_eq!(
            NodeCredential::from_entries([(ED25519_KEY_TAG, ed[..31].to_vec())]),
            Err(CredentialError::KeyLength),
            "an Ed25519 key is 32 bytes"
        );
        assert_eq!(
            NodeCredential::from_entries([(UNKNOWN_TAG, Vec::new())]),
            Err(CredentialError::KeyLength)
        );
        assert_eq!(
            NodeCredential::from_entries([(UNKNOWN_TAG, vec![1; MAX_CREDENTIAL_KEY_BYTES + 1])]),
            Err(CredentialError::KeyLength)
        );
        assert_eq!(
            NodeCredential::from_entries([(UNKNOWN_TAG, vec![1]), (ED25519_KEY_TAG, ed.clone())]),
            Err(KeyListError::Tags.into())
        );
        assert_eq!(
            NodeCredential::from_entries([(ED25519_KEY_TAG, vec![0; KEY_LENGTH])]),
            Err(KeyListError::InvalidEd25519Key.into())
        );

        let encoded = NodeCredential::single(ed25519()).encode();
        assert_eq!(
            NodeCredential::decode(&encoded[..encoded.len() - 1]),
            Err(CredentialError::Encoding)
        );
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            NodeCredential::decode(&trailing),
            Err(CredentialError::Encoding)
        );
        assert_eq!(
            NodeCredential::decode(&[0]),
            Err(KeyListError::EntryCount.into())
        );
    }

    #[test]
    fn the_longest_credential_fits_its_bound() {
        let entries = [ED25519_KEY_TAG, 2, 3, 4].map(|tag| {
            let key = if tag == ED25519_KEY_TAG {
                ed25519().as_bytes().to_vec()
            } else {
                vec![tag as u8; MAX_CREDENTIAL_KEY_BYTES]
            };
            (tag, key)
        });
        let credential = NodeCredential::from_entries(entries).expect("four entries fit");
        assert_eq!(
            credential.encode().len(),
            MAX_CREDENTIAL_BYTES - (MAX_CREDENTIAL_KEY_BYTES - KEY_LENGTH)
        );
        assert!(NodeCredential::from_spki(credential.to_spki().as_ref()).is_ok());
    }
}
