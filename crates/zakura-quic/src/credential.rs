//! A node's TLS credential: its full public keys, which TLS proves and from
//! which the node ID derives (SPEC ID-12, TLS-3).
//!
//! A [`NodeKeyList`] names a node in 32-byte entries, short enough for every
//! discovery record. A key longer than 32 bytes appears there as its
//! fingerprint. The credential carries each key in full, so a verifier can
//! check signatures with it, and maps back to the key list by fingerprinting:
//! the node ID a handshake proves is always the ID of
//! [`NodeCredential::key_list`].
//!
//! An entry may instead carry only its key's fingerprint. The node is then
//! still named by that key, so its ID is unchanged, but nobody can verify a
//! signature with it. A node that stops signing with a key keeps its ID this
//! way without sending the key in every handshake.

use blake2b_simd::Params;
use rustls_pki_types::{AlgorithmIdentifier, SubjectPublicKeyInfoDer};

use crate::{
    composite::{ML_DSA_65_KEY_BYTES, ML_DSA_65_KEY_TAG},
    key::{NodeId, NodeKey, KEY_LENGTH},
    key_list::{KeyListError, NodeKeyList, ED25519_KEY_TAG, MAX_NODE_KEYS},
};

/// The longest key one credential entry may hold, in bytes: an ML-DSA-87
/// public key, the largest of the NIST signature schemes. It is larger than
/// the ML-DSA-65 key this release signs with, so this release still reads a
/// credential that a later release extends with a larger key, keeping that
/// key as an unknown entry (ID-10, ID-12).
pub const MAX_CREDENTIAL_KEY_BYTES: usize = 2_592;

/// The bit of an entry's encoded length that marks the entry as carrying a
/// fingerprint in place of the key (ID-12).
const FINGERPRINT_ONLY_FLAG: u16 = 0x8000;

// No key length may reach the flag bit. The cast widens a `u16`.
const _: () = assert!(MAX_CREDENTIAL_KEY_BYTES < FINGERPRINT_ONLY_FLAG as usize);

/// Length of an entry's header: its tag and its length, a `u16` each.
const ENTRY_HEADER_BYTES: usize = 2 + 2;

/// The longest encoded credential, in bytes (ID-12).
///
/// The limit is on the whole encoding, not on each entry: a credential may
/// hold [`MAX_NODE_KEYS`] entries, and far fewer of them at
/// [`MAX_CREDENTIAL_KEY_BYTES`]. The value is the count byte and four
/// entries of the longest key, what a credential could reach while it held
/// no more than four entries. The entry count rose and this did not,
/// because a credential travels whole in one handshake flight.
///
/// A flight is the credential and about 3,650 bytes more, so about 14 KB at
/// this limit. `quinn-proto` refuses handshake data that ends more than
/// 16 KiB past what it has read in order, so a flight inside 16 KiB
/// completes whatever the network reorders, and this limit leaves over
/// 2 KiB of that for later handshake messages to grow into.
pub const MAX_CREDENTIAL_BYTES: usize = 10_385;

// The limit leaves room for the two keys this release signs with and a key
// of the longest length beside them.
const _: () = assert!(
    1 + (ENTRY_HEADER_BYTES + KEY_LENGTH)
        + (ENTRY_HEADER_BYTES + ML_DSA_65_KEY_BYTES)
        + (ENTRY_HEADER_BYTES + MAX_CREDENTIAL_KEY_BYTES)
        <= MAX_CREDENTIAL_BYTES
);

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
    /// length its tag fixes (32 bytes for Ed25519, 1,952 for ML-DSA-65).
    #[error("a credential key has the wrong length")]
    KeyLength,
    /// A fingerprint-only entry is not exactly 32 bytes, or stands for an
    /// Ed25519 key, which is its own fingerprint and is always sent in full.
    #[error("a fingerprint-only credential entry is 32 bytes and not Ed25519")]
    FingerprintOnly,
    /// The encoded credential is longer than [`MAX_CREDENTIAL_BYTES`].
    #[error("a credential is at most 10,385 bytes")]
    TooLong,
}

/// What a credential holds for one of the node's keys (ID-12).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialKey {
    /// The public key, which can verify the node's signatures.
    Full(Vec<u8>),
    /// Only the key's fingerprint: the 32 bytes the key has in the node's
    /// key list. It names the node like the full key does and verifies
    /// nothing.
    Fingerprint([u8; KEY_LENGTH]),
}

impl From<Vec<u8>> for CredentialKey {
    fn from(key: Vec<u8>) -> Self {
        Self::Full(key)
    }
}

/// A node's public keys, in tag order (ID-12).
///
/// Encoded as a count byte, then each entry as its tag (little-endian `u16`),
/// its length (little-endian `u16`) and its bytes, in at most
/// [`MAX_CREDENTIAL_BYTES`]. The rules on count and tags
/// are the key list's (ID-8). An Ed25519 entry is a checked 32-byte
/// [`NodeKey`]. An entry with an unknown tag is kept byte for byte, so a
/// release reaches the same node ID for a node holding a key it doesn't know.
///
/// A length with its top bit set marks a fingerprint-only entry
/// ([`CredentialKey::Fingerprint`]): the other 15 bits are 32 and the bytes
/// are the key's fingerprint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeCredential {
    entries: Vec<(u16, CredentialKey)>,
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
    /// A `Vec<u8>` is a key in full.
    pub fn from_entries<K: Into<CredentialKey>>(
        entries: impl IntoIterator<Item = (u16, K)>,
    ) -> Result<Self, CredentialError> {
        let entries: Vec<(u16, CredentialKey)> = entries
            .into_iter()
            .take(MAX_NODE_KEYS + 1)
            .map(|(tag, key)| (tag, key.into()))
            .collect();
        for (tag, key) in &entries {
            match key {
                CredentialKey::Full(key) => {
                    let fixed = match *tag {
                        ED25519_KEY_TAG => Some(KEY_LENGTH),
                        ML_DSA_65_KEY_TAG => Some(ML_DSA_65_KEY_BYTES),
                        _ => None,
                    };
                    let fits = match fixed {
                        Some(length) => key.len() == length,
                        None => !key.is_empty() && key.len() <= MAX_CREDENTIAL_KEY_BYTES,
                    };
                    if !fits {
                        return Err(CredentialError::KeyLength);
                    }
                }
                CredentialKey::Fingerprint(_) if *tag == ED25519_KEY_TAG => {
                    return Err(CredentialError::FingerprintOnly);
                }
                CredentialKey::Fingerprint(_) => {}
            }
        }
        // The key list checks the count, the tag order and the Ed25519 point.
        let key_list = NodeKeyList::from_entries(entries.iter().map(|(tag, key)| {
            let value = match key {
                CredentialKey::Full(key) => fingerprint(*tag, key),
                CredentialKey::Fingerprint(value) => *value,
            };
            (*tag, value)
        }))?;
        if encoded_len(&entries) > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::TooLong);
        }
        Ok(Self { entries, key_list })
    }

    /// This credential with the key under `tag` given as its fingerprint.
    /// The node ID is unchanged, and [`Self::key`] no longer returns the key.
    ///
    /// Fails when the credential has no key in full under `tag`, or `tag` is
    /// Ed25519.
    pub fn with_fingerprint_only(mut self, tag: u16) -> Result<Self, CredentialError> {
        let value = match self.key(tag) {
            Some(full) if tag != ED25519_KEY_TAG => fingerprint(tag, full),
            _ => return Err(CredentialError::FingerprintOnly),
        };
        for (entry_tag, key) in &mut self.entries {
            if *entry_tag == tag {
                *key = CredentialKey::Fingerprint(value);
            }
        }
        // The key list already holds this fingerprint, so it is unchanged.
        Ok(self)
    }

    /// Decodes a credential from exactly `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<Self, CredentialError> {
        // Refused before any key is copied. A decoded credential re-encodes
        // to exactly `bytes`, so this is the bound `from_entries` checks.
        if bytes.len() > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::TooLong);
        }
        let (&count, mut rest) = bytes.split_first().ok_or(CredentialError::Encoding)?;
        let count = usize::from(count);
        if count == 0 || count > MAX_NODE_KEYS {
            return Err(KeyListError::EntryCount.into());
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let (header, body) = rest
                .split_first_chunk::<ENTRY_HEADER_BYTES>()
                .ok_or(CredentialError::Encoding)?;
            let tag = u16::from_le_bytes([header[0], header[1]]);
            let length = u16::from_le_bytes([header[2], header[3]]);
            let fingerprint_only = length & FINGERPRINT_ONLY_FLAG != 0;
            let length = usize::from(length & !FINGERPRINT_ONLY_FLAG);
            if fingerprint_only && length != KEY_LENGTH {
                return Err(CredentialError::FingerprintOnly);
            }
            if length > MAX_CREDENTIAL_KEY_BYTES {
                return Err(CredentialError::KeyLength);
            }
            let (key, tail) = body
                .split_at_checked(length)
                .ok_or(CredentialError::Encoding)?;
            let key = if fingerprint_only {
                let mut value = [0; KEY_LENGTH];
                value.copy_from_slice(key);
                CredentialKey::Fingerprint(value)
            } else {
                CredentialKey::Full(key.to_vec())
            };
            entries.push((tag, key));
            rest = tail;
        }
        if !rest.is_empty() {
            return Err(CredentialError::Encoding);
        }
        Self::from_entries(entries)
    }

    /// The credential's canonical bytes, which [`Self::decode`] reads back.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(encoded_len(&self.entries));
        // A credential holds at most `MAX_NODE_KEYS` entries.
        bytes.push(self.entries.len() as u8);
        for (tag, key) in &self.entries {
            bytes.extend_from_slice(&tag.to_le_bytes());
            let (flag, value) = match key {
                CredentialKey::Full(key) => (0, key.as_slice()),
                CredentialKey::Fingerprint(value) => (FINGERPRINT_ONLY_FLAG, value.as_slice()),
            };
            // Construction bounds each key by `MAX_CREDENTIAL_KEY_BYTES`,
            // which is below the flag bit.
            bytes.extend_from_slice(&(value.len() as u16 | flag).to_le_bytes());
            bytes.extend_from_slice(value);
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

    /// The node IDs that a handshake signed with the keys under
    /// `signing_tags` proves this credential's holder answers to (TLS-7):
    /// the credential's own ID first, then the ID of each shorter prefix of
    /// its key list that includes every signing key, shortest first. Those
    /// are the IDs the node had before it added its later entries.
    ///
    /// An ID whose list leaves a signing key out is not among
    /// them: whoever added that key to another node's entries could
    /// otherwise sign as that node. Empty unless the credential holds each
    /// signing key in full, since a fingerprint verifies no signature.
    pub(crate) fn proven_ids<'a>(
        &'a self,
        signing_tags: &'a [u16],
    ) -> impl Iterator<Item = NodeId> + 'a {
        let holds_keys = signing_tags.iter().all(|tag| self.key(*tag).is_some());
        holds_keys
            .then(|| self.key_list.ids_of_lists_keeping(signing_tags))
            .into_iter()
            .flatten()
    }

    /// Whether a handshake signed with the keys under `signing_tags` proves
    /// this credential's holder is the node a dialer knows as `dialed`
    /// (TLS-7): `dialed` is one of [`Self::proven_ids`].
    pub(crate) fn proves_dialed_id(&self, dialed: NodeId, signing_tags: &[u16]) -> bool {
        self.proven_ids(signing_tags).any(|id| id == dialed)
    }

    /// The node's Ed25519 key, if the credential has one.
    pub fn ed25519(&self) -> Option<NodeKey> {
        self.key_list.ed25519()
    }

    /// The full key with `tag`, if the credential has one. A fingerprint-only
    /// entry has none, so nothing verifies a signature with it.
    pub fn key(&self, tag: u16) -> Option<&[u8]> {
        self.entries.iter().find_map(|(entry_tag, key)| match key {
            CredentialKey::Full(key) if *entry_tag == tag => Some(key.as_slice()),
            _ => None,
        })
    }

    /// The SubjectPublicKeyInfo a node presents as its TLS raw public key
    /// (TLS-3).
    pub(crate) fn to_spki(&self) -> SubjectPublicKeyInfoDer<'static> {
        spki_of(&self.encode())
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

/// The SubjectPublicKeyInfo that carries the credential encoding `encoded`
/// (TLS-3).
pub(crate) fn spki_of(encoded: &[u8]) -> SubjectPublicKeyInfoDer<'static> {
    rustls::sign::public_key_to_spki(
        &AlgorithmIdentifier::from_slice(CREDENTIAL_ALGORITHM),
        encoded,
    )
}

/// The length of the encoding of `entries`: the count byte, then each
/// entry's header and bytes.
fn encoded_len(entries: &[(u16, CredentialKey)]) -> usize {
    let key_bytes = |key: &CredentialKey| match key {
        CredentialKey::Full(key) => key.len(),
        CredentialKey::Fingerprint(_) => KEY_LENGTH,
    };
    1 + entries
        .iter()
        .map(|(_, key)| ENTRY_HEADER_BYTES + key_bytes(key))
        .sum::<usize>()
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
        assert_eq!(
            credential.node_id(),
            NodeKeyList::single(ed25519()).node_id()
        );
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
    /// `hashlib.blake2b`: the ML-DSA-65 key's fingerprint under
    /// `Zakura_KeyFpr_v1`, then the two-entry list under `Zakura_NodeId_v1`.
    #[test]
    fn the_e2e_seed_nodes_id_has_its_known_value() {
        let secret = NodeSecretKey::from_bytes(&[1; KEY_LENGTH]);
        assert_eq!(
            secret.public().to_string(),
            "bee4ed7d2e07dd73d681bdc52e8efb8aa4860cfbe832ef5b1479d7b313a9b72f"
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
        for length in [
            ML_DSA_65_KEY_BYTES - 1,
            ML_DSA_65_KEY_BYTES + 1,
            MAX_CREDENTIAL_KEY_BYTES,
        ] {
            assert_eq!(
                NodeCredential::from_entries([
                    (ED25519_KEY_TAG, ed.clone()),
                    (ML_DSA_65_KEY_TAG, vec![1; length]),
                ]),
                Err(CredentialError::KeyLength),
                "an ML-DSA-65 key is {ML_DSA_65_KEY_BYTES} bytes, not {length}"
            );
        }
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

    /// A credential entry built by hand, with `flag` set in its length.
    fn entry(tag: u16, flag: u16, bytes: &[u8]) -> Vec<u8> {
        let mut entry = tag.to_le_bytes().to_vec();
        entry.extend_from_slice(&(bytes.len() as u16 | flag).to_le_bytes());
        entry.extend_from_slice(bytes);
        entry
    }

    /// ID-12: a key given as its fingerprint names the same node, and the
    /// credential no longer holds a key to verify with.
    #[test]
    fn a_fingerprint_only_entry_keeps_the_node_id_and_drops_the_key() {
        let secret = NodeSecretKey::from_bytes(&[7; KEY_LENGTH]);
        let full = secret.credential().clone();
        let retired = full
            .clone()
            .with_fingerprint_only(ML_DSA_65_KEY_TAG)
            .expect("an ML-DSA-65 key can be given as its fingerprint");

        assert_eq!(retired.key_list(), full.key_list());
        assert_eq!(retired.node_id(), full.node_id());
        assert!(full.key(ML_DSA_65_KEY_TAG).is_some());
        assert_eq!(retired.key(ML_DSA_65_KEY_TAG), None);
        assert_eq!(retired.ed25519(), full.ed25519());
        assert_eq!(
            retired.encode().len(),
            full.encode().len() - (ML_DSA_65_KEY_BYTES - KEY_LENGTH)
        );

        assert_eq!(
            NodeCredential::decode(&retired.encode()),
            Ok(retired.clone())
        );
        assert_eq!(
            NodeCredential::from_spki(retired.to_spki().as_ref()),
            Ok(retired.clone())
        );
        assert_ne!(retired, full, "the two forms are different credentials");
    }

    /// ID-10, ID-12: a fingerprint under a tag this release doesn't know is
    /// kept byte for byte, and enters the key list unchanged.
    #[test]
    fn a_fingerprint_only_entry_of_an_unknown_tag_is_kept_byte_for_byte() {
        let value = [0xEF; KEY_LENGTH];
        let credential = NodeCredential::from_entries([
            (
                ED25519_KEY_TAG,
                CredentialKey::Full(ed25519().as_bytes().to_vec()),
            ),
            (UNKNOWN_TAG, CredentialKey::Fingerprint(value)),
        ])
        .expect("a key and a fingerprint form a credential");
        let expected = NodeKeyList::from_entries([
            (ED25519_KEY_TAG, *ed25519().as_bytes()),
            (UNKNOWN_TAG, value),
        ])
        .expect("two increasing entries form a list");

        assert_eq!(credential.key_list(), &expected);
        assert_eq!(credential.key(UNKNOWN_TAG), None);
        let mut encoded = vec![2];
        encoded.extend(entry(ED25519_KEY_TAG, 0, ed25519().as_bytes()));
        encoded.extend(entry(UNKNOWN_TAG, FINGERPRINT_ONLY_FLAG, &value));
        assert_eq!(credential.encode(), encoded);
        assert_eq!(NodeCredential::decode(&encoded), Ok(credential));
    }

    #[test]
    fn malformed_fingerprint_only_entries_are_refused() {
        let ed = ed25519().as_bytes().to_vec();
        // An Ed25519 key is its own fingerprint and is always sent in full.
        assert_eq!(
            NodeCredential::from_entries([(
                ED25519_KEY_TAG,
                CredentialKey::Fingerprint(*ed25519().as_bytes())
            )]),
            Err(CredentialError::FingerprintOnly)
        );
        let mut flagged_ed25519 = vec![1];
        flagged_ed25519.extend(entry(ED25519_KEY_TAG, FINGERPRINT_ONLY_FLAG, &ed));
        assert_eq!(
            NodeCredential::decode(&flagged_ed25519),
            Err(CredentialError::FingerprintOnly)
        );

        for length in [0, KEY_LENGTH - 1, KEY_LENGTH + 1, ML_DSA_65_KEY_BYTES] {
            let mut encoded = vec![2];
            encoded.extend(entry(ED25519_KEY_TAG, 0, &ed));
            encoded.extend(entry(
                ML_DSA_65_KEY_TAG,
                FINGERPRINT_ONLY_FLAG,
                &vec![1; length],
            ));
            assert_eq!(
                NodeCredential::decode(&encoded),
                Err(CredentialError::FingerprintOnly),
                "a fingerprint is 32 bytes, not {length}"
            );
        }

        // The flag doesn't excuse bytes after the fingerprint.
        let mut trailing = vec![2];
        trailing.extend(entry(ED25519_KEY_TAG, 0, &ed));
        trailing.extend(entry(UNKNOWN_TAG, FINGERPRINT_ONLY_FLAG, &[1; KEY_LENGTH]));
        assert!(NodeCredential::decode(&trailing).is_ok());
        trailing.push(0);
        assert_eq!(
            NodeCredential::decode(&trailing),
            Err(CredentialError::Encoding)
        );

        // Only a key held in full can be turned into its fingerprint.
        let single = NodeCredential::single(ed25519());
        for tag in [ED25519_KEY_TAG, ML_DSA_65_KEY_TAG] {
            assert_eq!(
                single.clone().with_fingerprint_only(tag),
                Err(CredentialError::FingerprintOnly)
            );
        }
        let secret = NodeSecretKey::from_bytes(&[7; KEY_LENGTH]);
        let retired = secret
            .credential()
            .clone()
            .with_fingerprint_only(ML_DSA_65_KEY_TAG)
            .expect("an ML-DSA-65 key can be given as its fingerprint");
        assert_eq!(
            retired.with_fingerprint_only(ML_DSA_65_KEY_TAG),
            Err(CredentialError::FingerprintOnly)
        );
    }

    /// The Ed25519 key, then `more` further entries with the tags from 4 up.
    fn with_more(more: impl IntoIterator<Item = CredentialKey>) -> Vec<(u16, CredentialKey)> {
        let ed25519 = CredentialKey::Full(ed25519().as_bytes().to_vec());
        // Tag 3 is ML-DSA-65, whose length is fixed.
        [(ED25519_KEY_TAG, ed25519)]
            .into_iter()
            .chain((4..).zip(more))
            .collect()
    }

    /// TLS-7: a credential proves the ID of each prefix of its key list that
    /// keeps every signing key, when it holds those keys in full, and what
    /// it proves to a dialer is exactly what it reports. An entry outside
    /// the signing keys may be a fingerprint. A signing key given as its
    /// fingerprint proves nothing, not even the credential's own ID.
    #[test]
    fn a_credential_proves_the_ids_of_the_prefixes_that_keep_its_signing_keys() {
        const SIGNING: [u16; 2] = [ED25519_KEY_TAG, ML_DSA_65_KEY_TAG];
        let secret = NodeSecretKey::from_bytes(&[7; KEY_LENGTH]);
        let own = |tag: u16| {
            let key = secret.credential().key(tag).expect("a node holds its keys");
            (tag, CredentialKey::Full(key.to_vec()))
        };
        // Tag 2 sorts between the signing keys and tag 4 after them.
        let entries = [
            own(ED25519_KEY_TAG),
            (2, CredentialKey::Full(vec![2; 40])),
            own(ML_DSA_65_KEY_TAG),
            (4, CredentialKey::Fingerprint([4; KEY_LENGTH])),
        ];
        let credential = NodeCredential::from_entries(entries.clone()).expect("four entries fit");
        let id_of = |mask: u32| {
            let kept = (0..entries.len())
                .filter(|index| mask & (1 << index) != 0)
                .map(|index| entries[index].clone());
            NodeCredential::from_entries(kept).map(|kept| kept.node_id())
        };

        // The whole list, then the one shorter prefix that keeps both
        // signing keys: the first three entries.
        let proven: Vec<NodeId> = credential.proven_ids(&SIGNING).collect();
        assert_eq!(
            proven,
            [
                credential.node_id(),
                id_of(0b0111).expect("a prefix is a list")
            ]
        );
        for mask in 1u32..1 << entries.len() {
            // A prefix's mask is one below a power of two. Bits 0 and 2 are
            // the signing keys.
            let proves = (mask + 1).is_power_of_two() && mask & 0b0101 == 0b0101;
            let id = id_of(mask).expect("entries in tag order form a credential");
            assert_eq!(proven.contains(&id), proves, "{mask:#06b}");
            assert_eq!(
                credential.proves_dialed_id(id, &SIGNING),
                proves,
                "{mask:#06b}"
            );
        }

        let retired = credential
            .clone()
            .with_fingerprint_only(ML_DSA_65_KEY_TAG)
            .expect("an ML-DSA-65 key can be given as its fingerprint");
        assert_eq!(retired.node_id(), credential.node_id());
        assert_eq!(retired.proven_ids(&SIGNING).next(), None);
        assert!(!retired.proves_dialed_id(retired.node_id(), &SIGNING));
        // The same credential still proves its four prefixes to a scheme
        // that signs with the Ed25519 key alone.
        assert_eq!(retired.proven_ids(&[ED25519_KEY_TAG]).count(), 4);

        // A credential without a signing key proves nothing either.
        let single = NodeCredential::single(ed25519());
        assert_eq!(single.proven_ids(&SIGNING).next(), None);
        assert_eq!(
            single.proven_ids(&[ED25519_KEY_TAG]).collect::<Vec<_>>(),
            [single.node_id()]
        );
    }

    /// ID-12: the byte limit bounds a credential, whatever its entry count.
    /// The Ed25519 key, three keys of the longest length and one of 2,556
    /// bytes fill it exactly, and one byte more is refused however it
    /// arrives.
    #[test]
    fn the_longest_credential_fills_its_bound() {
        assert_eq!(MAX_CREDENTIAL_BYTES, 10_385);
        let keys = |last: usize| {
            let longest = CredentialKey::Full(vec![0xEE; MAX_CREDENTIAL_KEY_BYTES]);
            with_more([
                longest.clone(),
                longest.clone(),
                longest,
                CredentialKey::Full(vec![0xDD; last]),
            ])
        };
        let credential = NodeCredential::from_entries(keys(2_556)).expect("the entries fit");
        let encoded = credential.encode();
        assert_eq!(encoded.len(), MAX_CREDENTIAL_BYTES);
        assert_eq!(NodeCredential::decode(&encoded), Ok(credential.clone()));
        assert_eq!(
            NodeCredential::from_spki(credential.to_spki().as_ref()),
            Ok(credential)
        );

        assert_eq!(
            NodeCredential::from_entries(keys(2_557)),
            Err(CredentialError::TooLong)
        );
        // The same credential on the wire: its last entry is one byte
        // longer, and every other rule holds.
        let mut too_long = encoded;
        let length_at = too_long.len() - 2_556 - 2;
        assert_eq!(too_long[length_at..length_at + 2], 2_556u16.to_le_bytes());
        too_long[length_at..length_at + 2].copy_from_slice(&2_557u16.to_le_bytes());
        too_long.push(0xDD);
        assert_eq!(
            NodeCredential::decode(&too_long),
            Err(CredentialError::TooLong)
        );
        // The length is checked before an entry is read or a key copied, so
        // an input above the limit is refused for its length whatever else
        // is wrong with it: here a count of 255 and no well-formed entry.
        assert_eq!(
            NodeCredential::decode(&[0xFF; MAX_CREDENTIAL_BYTES + 1]),
            Err(CredentialError::TooLong)
        );
        assert_eq!(
            NodeCredential::decode(&[0xFF; MAX_CREDENTIAL_BYTES]),
            Err(KeyListError::EntryCount.into())
        );
        assert_eq!(
            NodeCredential::from_spki(spki_of(&too_long).as_ref()),
            Err(CredentialError::TooLong)
        );
    }

    /// ID-8, ID-12: a credential holds up to 32 entries. The entry count
    /// alone never refuses 32, and 33 are refused at any size.
    #[test]
    fn a_credential_holds_32_entries_within_the_byte_limit() {
        let fingerprints =
            |count: u8| (0..count).map(|index| CredentialKey::Fingerprint([index; 32]));
        let most = NodeCredential::from_entries(with_more(fingerprints(31)))
            .expect("32 entries form a credential");
        assert_eq!(most.key_list().encoded_len(), 1 + MAX_NODE_KEYS * 34);
        assert_eq!(most.encode().len(), 1 + MAX_NODE_KEYS * (4 + 32));
        assert_eq!(NodeCredential::decode(&most.encode()), Ok(most.clone()));
        assert_eq!(
            NodeCredential::from_spki(most.to_spki().as_ref()),
            Ok(most.clone())
        );

        assert_eq!(
            NodeCredential::from_entries(with_more(fingerprints(32))),
            Err(KeyListError::EntryCount.into())
        );
        let mut one_more = most.encode();
        one_more[0] += 1;
        one_more.extend(entry(0x0102, FINGERPRINT_ONLY_FLAG, &[0xAB; 32]));
        assert_eq!(
            NodeCredential::decode(&one_more),
            Err(KeyListError::EntryCount.into())
        );

        // 32 entries of keys in full: each of the 31 after the Ed25519 key
        // may average 329 bytes, and one byte more passes the limit.
        let keys = |last: usize| {
            let mut keys = vec![CredentialKey::Full(vec![0x11; 329]); 30];
            keys.push(CredentialKey::Full(vec![0x22; last]));
            with_more(keys)
        };
        let full = NodeCredential::from_entries(keys(354)).expect("the keys fit");
        assert_eq!(full.encode().len(), MAX_CREDENTIAL_BYTES);
        assert_eq!(NodeCredential::decode(&full.encode()), Ok(full));
        assert_eq!(
            NodeCredential::from_entries(keys(355)),
            Err(CredentialError::TooLong)
        );
        // Four entries of the longest key were the limit's worth before:
        // with the Ed25519 key beside them they no longer fit.
        let longest = CredentialKey::Full(vec![0xEE; MAX_CREDENTIAL_KEY_BYTES]);
        assert_eq!(
            NodeCredential::from_entries(with_more(vec![longest; 4])),
            Err(CredentialError::TooLong)
        );
    }
}
