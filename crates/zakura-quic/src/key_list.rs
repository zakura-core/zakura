//! A node's key list, and the node ID derived from it (SPEC §3).
//!
//! A node ID names a list of keys, not one key, so a node can later hold a
//! key of another signature scheme without every format that carries an ID
//! changing. A reader checks the entries it understands, keeps the others as
//! opaque bytes, and hashes the whole list.

use blake2b_simd::Params;

use crate::key::{NodeId, NodeKey, KEY_LENGTH};

/// The most entries a key list holds (ID-8).
pub const MAX_NODE_KEYS: usize = 4;

/// The tag of an Ed25519 entry. Its value is the [`NodeKey`] (ID-8).
pub const ED25519_KEY_TAG: u16 = 1;

/// Length of one encoded entry: a 2-byte tag and a 32-byte value.
pub const NODE_KEY_ENTRY_BYTES: usize = 2 + KEY_LENGTH;
const ENTRY_BYTES: usize = NODE_KEY_ENTRY_BYTES;

/// Length of the shortest encoded key list: the count byte and one entry.
pub const MIN_NODE_KEY_LIST_BYTES: usize = 1 + ENTRY_BYTES;

/// Length of the longest encoded key list: the count byte and
/// [`MAX_NODE_KEYS`] entries.
pub const MAX_NODE_KEY_LIST_BYTES: usize = 1 + MAX_NODE_KEYS * ENTRY_BYTES;

/// The BLAKE2b personalisation of a node ID (ID-9).
const NODE_ID_PERSONALISATION: &[u8; 16] = b"Zakura_NodeId_v1";

/// Why bytes are not a key list.
///
/// These are the only ways a list is malformed. A list with no entry this
/// release understands is well formed: [`NodeKeyList::ed25519`] returns
/// `None` for it, and what to do then is for the caller to decide (ID-11).
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum KeyListError {
    /// The list holds no entry or more than [`MAX_NODE_KEYS`].
    #[error("a key list holds 1 to 4 entries")]
    EntryCount,
    /// The bytes are not the count byte followed by exactly that many
    /// entries.
    #[error("the key list's length does not match its entry count")]
    Length,
    /// A tag is zero, or the tags are not strictly increasing.
    #[error("key list tags must be nonzero and strictly increasing")]
    Tags,
    /// The Ed25519 entry is not a valid public key.
    #[error("the key list's Ed25519 entry is not a valid public key")]
    InvalidEd25519Key,
}

/// The keys a node is known by, in their one canonical order (ID-8).
///
/// Encoded as a count byte, then each entry as its tag (little-endian `u16`)
/// and 32 bytes. Tags are nonzero and strictly increasing, so a set of keys
/// has exactly one encoding and one [`node_id`](Self::node_id).
///
/// An entry with [`ED25519_KEY_TAG`] holds a checked [`NodeKey`]. An entry
/// with any other tag is kept and re-encoded byte for byte (ID-10).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeKeyList {
    /// `(tag, value)` in increasing tag order, 1 to [`MAX_NODE_KEYS`] of them.
    entries: Vec<(u16, [u8; KEY_LENGTH])>,
    /// The hash of the encoded entries, computed once at construction. Code
    /// that handles many records reads a node's ID far more often than it
    /// builds a list.
    id: NodeId,
}

impl NodeKeyList {
    /// The list of a node whose only key is the Ed25519 key `key`.
    pub fn single(key: NodeKey) -> Self {
        Self::from_checked(vec![(ED25519_KEY_TAG, *key.as_bytes())])
    }

    /// Wraps entries that already satisfy every rule of
    /// [`Self::from_entries`], and derives their node ID.
    fn from_checked(entries: Vec<(u16, [u8; KEY_LENGTH])>) -> Self {
        let hash = Params::new()
            .hash_length(KEY_LENGTH)
            .personal(NODE_ID_PERSONALISATION)
            .hash(&encode(&entries));
        let mut digest = [0; KEY_LENGTH];
        digest.copy_from_slice(hash.as_bytes());
        Self {
            entries,
            id: NodeId::from_bytes(&digest),
        }
    }

    /// Builds a list from `(tag, value)` entries, which must already be in
    /// increasing tag order.
    pub fn from_entries(
        entries: impl IntoIterator<Item = (u16, [u8; KEY_LENGTH])>,
    ) -> Result<Self, KeyListError> {
        let entries: Vec<_> = entries.into_iter().take(MAX_NODE_KEYS + 1).collect();
        if entries.is_empty() || entries.len() > MAX_NODE_KEYS {
            return Err(KeyListError::EntryCount);
        }
        let mut previous = 0;
        for (tag, value) in &entries {
            // The first tag must exceed zero and each later one its
            // predecessor, which rules out a zero tag and a repeated one.
            if *tag <= previous {
                return Err(KeyListError::Tags);
            }
            previous = *tag;
            if *tag == ED25519_KEY_TAG && NodeKey::from_bytes(value).is_err() {
                return Err(KeyListError::InvalidEd25519Key);
            }
        }
        Ok(Self::from_checked(entries))
    }

    /// Decodes a list from exactly `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<Self, KeyListError> {
        let (&count, body) = bytes.split_first().ok_or(KeyListError::Length)?;
        let count = usize::from(count);
        if count == 0 || count > MAX_NODE_KEYS {
            return Err(KeyListError::EntryCount);
        }
        if body.len() != count * ENTRY_BYTES {
            return Err(KeyListError::Length);
        }
        // The length check above leaves no remainder.
        let (entries, _) = body.as_chunks::<ENTRY_BYTES>();
        Self::from_entries(entries.iter().map(|entry| {
            let (tag, value) = entry.split_at(2);
            let mut key = [0; KEY_LENGTH];
            key.copy_from_slice(value);
            (u16::from_le_bytes([tag[0], tag[1]]), key)
        }))
    }

    /// The list's canonical bytes, which [`Self::decode`] reads back.
    pub fn encode(&self) -> Vec<u8> {
        encode(&self.entries)
    }

    /// The length of [`Self::encode`]'s bytes.
    pub fn encoded_len(&self) -> usize {
        1 + self.entries.len() * ENTRY_BYTES
    }

    /// The node's Ed25519 key, if the list has one.
    pub fn ed25519(&self) -> Option<NodeKey> {
        self.entries
            .iter()
            .find(|(tag, _)| *tag == ED25519_KEY_TAG)
            .map(|(_, value)| {
                NodeKey::from_bytes(value)
                    .expect("construction checked the Ed25519 entry is a valid key")
            })
    }

    /// The node ID of the node these keys belong to: BLAKE2b-256 of the
    /// encoded list (ID-9). Entries this release doesn't understand are
    /// hashed too, so every release derives the same ID from the same list.
    pub fn node_id(&self) -> NodeId {
        self.id
    }
}

/// Encodes entries as a count byte, then each tag and value.
fn encode(entries: &[(u16, [u8; KEY_LENGTH])]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + entries.len() * ENTRY_BYTES);
    // A list holds at most `MAX_NODE_KEYS` entries, so the count fits a byte.
    bytes.push(entries.len() as u8);
    for (tag, value) in entries {
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(value);
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeSecretKey;

    /// The public key of RFC 8032's first Ed25519 test vector.
    const KEY: [u8; KEY_LENGTH] = [
        0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07,
        0x3a, 0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07,
        0x51, 0x1a,
    ];

    /// A tag this release doesn't define, and a value for it.
    const UNKNOWN: (u16, [u8; KEY_LENGTH]) = (0x0102, [0xAB; KEY_LENGTH]);

    fn key() -> NodeKey {
        NodeKey::from_bytes(&KEY).expect("the RFC 8032 key is valid")
    }

    // The digests were computed with Python's `hashlib.blake2b`, so they
    // check the encoding and the personalisation against a second
    // implementation.
    #[test]
    fn a_list_hashes_to_its_known_node_id() {
        let one = NodeKeyList::single(key());
        assert_eq!(one.encode().len(), 35);
        assert_eq!(
            one.node_id().to_string(),
            "b596e1f72ff13a26526121761aaa59cdc0b0414ec5673ffcf40e45613c624ce5"
        );

        let two = NodeKeyList::from_entries([(ED25519_KEY_TAG, KEY), UNKNOWN])
            .expect("increasing tags form a list");
        assert_eq!(two.encode().len(), 69);
        assert_eq!(
            two.node_id().to_string(),
            "a849105942d50fa486dfb52ce19533507e265f703349e9f24d413247a20bef99"
        );
    }

    #[test]
    fn an_entry_of_an_unknown_tag_is_kept_byte_for_byte() {
        let list = NodeKeyList::from_entries([(ED25519_KEY_TAG, KEY), UNKNOWN])
            .expect("increasing tags form a list");
        let bytes = list.encode();
        let decoded = NodeKeyList::decode(&bytes).expect("an encoded list decodes");

        assert_eq!(decoded.encode(), bytes);
        assert_eq!(decoded.node_id(), list.node_id());
        assert_eq!(decoded.ed25519(), Some(key()));
        assert_ne!(
            decoded.node_id(),
            NodeKeyList::single(key()).node_id(),
            "the unknown entry is part of the node ID"
        );
    }

    #[test]
    fn a_list_without_an_ed25519_entry_is_well_formed() {
        let list = NodeKeyList::from_entries([UNKNOWN]).expect("one unknown entry is a list");
        assert_eq!(list.ed25519(), None);
        assert_eq!(NodeKeyList::decode(&list.encode()), Ok(list));
    }

    #[test]
    fn malformed_lists_are_refused() {
        let entry = |tag: u16, value: [u8; KEY_LENGTH]| {
            let mut bytes = tag.to_le_bytes().to_vec();
            bytes.extend_from_slice(&value);
            bytes
        };
        let list = |count: u8, entries: &[Vec<u8>]| {
            let mut bytes = vec![count];
            bytes.extend(entries.iter().flatten());
            bytes
        };
        let value = [0xCD; KEY_LENGTH];

        assert_eq!(NodeKeyList::decode(&[]), Err(KeyListError::Length));
        assert_eq!(NodeKeyList::decode(&[0]), Err(KeyListError::EntryCount));
        let five: Vec<_> = (2..7).map(|tag| entry(tag, value)).collect();
        assert_eq!(
            NodeKeyList::decode(&list(5, &five)),
            Err(KeyListError::EntryCount)
        );
        // A count that disagrees with the bytes, either way.
        assert_eq!(
            NodeKeyList::decode(&list(2, &[entry(2, value)])),
            Err(KeyListError::Length)
        );
        let mut trailing = list(1, &[entry(2, value)]);
        trailing.push(0);
        assert_eq!(NodeKeyList::decode(&trailing), Err(KeyListError::Length));

        assert_eq!(
            NodeKeyList::decode(&list(1, &[entry(0, value)])),
            Err(KeyListError::Tags),
            "tag zero is not a key"
        );
        assert_eq!(
            NodeKeyList::decode(&list(2, &[entry(3, value), entry(2, value)])),
            Err(KeyListError::Tags),
            "tags out of order"
        );
        assert_eq!(
            NodeKeyList::decode(&list(2, &[entry(2, value), entry(2, value)])),
            Err(KeyListError::Tags),
            "a repeated tag"
        );
        // All zeroes is a point of small order.
        assert_eq!(
            NodeKeyList::decode(&list(1, &[entry(ED25519_KEY_TAG, [0; KEY_LENGTH])])),
            Err(KeyListError::InvalidEd25519Key)
        );
    }

    #[test]
    fn the_longest_list_fills_the_length_bound() {
        let list = NodeKeyList::from_entries([
            (
                ED25519_KEY_TAG,
                *NodeSecretKey::generate().public_key().as_bytes(),
            ),
            (2, [2; KEY_LENGTH]),
            (3, [3; KEY_LENGTH]),
            (4, [4; KEY_LENGTH]),
        ])
        .expect("four increasing tags form a list");
        assert_eq!(list.encode().len(), MAX_NODE_KEY_LIST_BYTES);
    }
}
