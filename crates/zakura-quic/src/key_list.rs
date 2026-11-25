//! A node's key list, and the node ID derived from it (SPEC §3).
//!
//! A node ID names a list of keys, not one key, so a node can later hold a
//! key of another signature scheme without every format that carries an ID
//! changing. A reader checks the entries it understands, keeps the others as
//! opaque bytes, and hashes the whole list.

use blake2b_simd::Params;

use crate::key::{NodeId, NodeKey, KEY_LENGTH};

/// The most entries a key list holds (ID-8).
///
/// A node keeps the entry of every key it has ever added, a retired key as
/// its fingerprint, so this is the number of keys one node ID's line can
/// hold over its lifetime. The count travels as one byte.
pub const MAX_NODE_KEYS: usize = 32;

// The count byte holds the largest count. The cast widens a `u8`.
const _: () = assert!(MAX_NODE_KEYS <= u8::MAX as usize);

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
    #[error("a key list holds 1 to 32 entries")]
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
        let id = node_id(&entries);
        Self { entries, id }
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

    /// The node IDs of this list and of each prefix of it that keeps every
    /// entry tagged in `required` (TLS-7), the list's own ID first and then
    /// its prefixes, shortest first. Empty when this list lacks a required
    /// tag.
    ///
    /// A node only ever adds a key under a tag above those it holds, so each
    /// list it has had is a prefix of the one it has now, and it is still
    /// the node its earlier, shorter list named. `required` holds the tags
    /// of the keys that prove it: an ID whose list leaves one of them out is
    /// not proven by them. This is the one place that says which shorter
    /// lists a list stands for, so a dial's check and a connection's report
    /// can't disagree.
    ///
    /// Costs at most one hash per entry.
    pub(crate) fn ids_of_lists_keeping<'a>(
        &'a self,
        required: &'a [u16],
    ) -> impl Iterator<Item = NodeId> + 'a {
        self.shortest_prefix_keeping(required)
            .map(|shortest| std::iter::once(self.id).chain(self.earlier_ids(shortest)))
            .into_iter()
            .flatten()
    }

    /// The length of the shortest prefix that keeps every entry tagged in
    /// `required`, at least one entry. `None` when the list lacks one of the
    /// tags.
    fn shortest_prefix_keeping(&self, required: &[u16]) -> Option<usize> {
        required.iter().try_fold(1, |shortest: usize, tag| {
            let index = self.entries.iter().position(|(held, _)| held == tag)?;
            Some(shortest.max(index + 1))
        })
    }

    /// The node IDs of this list's proper prefixes of `shortest` entries or
    /// more, shortest first: one hash each, and fewer of them than the list
    /// has entries. The whole list's ID is [`Self::node_id`].
    fn earlier_ids(&self, shortest: usize) -> impl Iterator<Item = NodeId> + '_ {
        (shortest..self.entries.len()).map(|length| node_id(&self.entries[..length]))
    }
}

/// The node ID of `entries`: the BLAKE2b-256 hash of their encoding (ID-9).
fn node_id(entries: &[(u16, [u8; KEY_LENGTH])]) -> NodeId {
    let hash = Params::new()
        .hash_length(KEY_LENGTH)
        .personal(NODE_ID_PERSONALISATION)
        .hash(&encode(entries));
    let mut digest = [0; KEY_LENGTH];
    digest.copy_from_slice(hash.as_bytes());
    NodeId::from_bytes(&digest)
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

    /// Whether `id` names `list`, or a list of some of its entries that
    /// keeps every entry tagged in `required`.
    fn contains_list_named(list: &NodeKeyList, id: NodeId, required: &[u16]) -> bool {
        list.ids_of_lists_keeping(required).any(|named| named == id)
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
        let too_many: Vec<_> = (2..35).map(|tag| entry(tag, value)).collect();
        assert_eq!(too_many.len(), MAX_NODE_KEYS + 1);
        assert_eq!(
            NodeKeyList::decode(&list(33, &too_many)),
            Err(KeyListError::EntryCount)
        );
        assert!(NodeKeyList::decode(&list(32, &too_many[..32])).is_ok());
        assert_eq!(
            NodeKeyList::from_entries((2..35).map(|tag| (tag, value))),
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

    /// `count` entries with tags 1 to `count`: the Ed25519 key, then values
    /// that differ by tag.
    fn tagged_entries(count: u16) -> Vec<(u16, [u8; KEY_LENGTH])> {
        (1..=count)
            .map(|tag| match tag {
                ED25519_KEY_TAG => (tag, KEY),
                // A tag is at most 32 here, so it fits a byte.
                _ => (tag, [tag as u8; KEY_LENGTH]),
            })
            .collect()
    }

    /// TLS-7: a list answers to the ID of each prefix that keeps the
    /// required entries, and to no other.
    #[test]
    fn a_list_contains_the_prefixes_that_keep_the_required_entries() {
        let entries = tagged_entries(4);
        let list = NodeKeyList::from_entries(entries.iter().copied())
            .expect("four increasing tags form a list");
        let required = [ED25519_KEY_TAG, 3];
        let named = |kept: &[usize]| {
            let kept: Vec<_> = kept.iter().map(|index| entries[*index]).collect();
            node_id(&kept)
        };

        assert!(contains_list_named(&list, list.node_id(), &required));
        for kept in [&[0, 1, 2][..], &[0, 1, 2, 3]] {
            assert!(
                contains_list_named(&list, named(kept), &required),
                "the prefix {kept:?} keeps both required tags"
            );
        }
        for kept in [&[0][..], &[0, 1]] {
            assert!(
                !contains_list_named(&list, named(kept), &required),
                "the prefix {kept:?} leaves a required tag out"
            );
        }
        // Lists of the entries, in their order, that are not prefixes.
        for kept in [&[2][..], &[0, 3], &[1, 2, 3], &[0, 1, 3], &[1, 3]] {
            assert!(
                !contains_list_named(&list, named(kept), &required),
                "{kept:?} is not a prefix"
            );
            assert!(!contains_list_named(&list, named(kept), &[]));
        }
        // Not lists at all: no entries, entries out of order, one repeated.
        for kept in [&[][..], &[1, 0], &[0, 0, 1], &[0, 1, 1], &[3, 2, 1, 0]] {
            assert!(
                !contains_list_named(&list, named(kept), &required),
                "{kept:?} is not a list of the entries in their order"
            );
            assert!(!contains_list_named(&list, named(kept), &[]));
        }
        // With nothing required, every prefix is contained.
        assert!(contains_list_named(&list, named(&[0]), &[]));
        assert!(contains_list_named(&list, named(&[0, 1]), &[]));
        assert!(!contains_list_named(&list, named(&[]), &[]));
        assert!(!contains_list_named(
            &list,
            NodeKeyList::from_entries([UNKNOWN])
                .expect("one entry is a list")
                .node_id(),
            &[]
        ));
    }

    /// TLS-7 named a list of any of the credential's entries before spec
    /// version 0.34. The lists that rule accepted and a prefix rule doesn't
    /// are refused: each keeps both required entries, in order, and skips
    /// an entry before the last one it keeps.
    #[test]
    fn a_sublist_that_skips_an_entry_is_refused() {
        let entries = tagged_entries(4);
        let list = NodeKeyList::from_entries(entries.iter().copied())
            .expect("four increasing tags form a list");
        let required = [ED25519_KEY_TAG, 3];
        for kept in [&[0, 2][..], &[0, 2, 3]] {
            let kept: Vec<_> = kept.iter().map(|index| entries[*index]).collect();
            let sublist = NodeKeyList::from_entries(kept).expect("a sublist is a list");
            assert!(required
                .iter()
                .all(|tag| sublist.entries.iter().any(|(held, _)| held == tag)));
            assert!(!contains_list_named(&list, sublist.node_id(), &required));
        }
    }

    /// TLS-7, exhaustively: for every list of 1 to 6 entries, every set of
    /// required tags and every sublist, the list contains the sublist's ID
    /// exactly when the sublist is the first entries of the list, with none
    /// skipped, and keeps every required entry. No sublist passes when a
    /// required tag is missing from the list, and no sublist of a list that
    /// differs in one value passes.
    #[test]
    fn a_list_contains_exactly_the_prefixes_that_keep_every_required_entry() {
        /// A tag none of the lists holds.
        const ABSENT_TAG: u16 = 99;
        let all = tagged_entries(6);
        let picked = |entries: &[(u16, [u8; KEY_LENGTH])], mask: u32| -> Vec<_> {
            let kept = |index: &usize| mask & (1 << index) != 0;
            (0..entries.len())
                .filter(kept)
                .map(|index| entries[index])
                .collect()
        };
        let mut checked = 0;
        let mut contained = 0;
        for size in 1..=all.len() {
            let entries = &all[..size];
            let list = NodeKeyList::from_entries(entries.iter().copied())
                .expect("increasing tags form a list");
            let mut other = entries.to_vec();
            other[0].1 = [0xEE; KEY_LENGTH];
            for required_mask in 0u32..1 << size {
                let required: Vec<u16> = picked(entries, required_mask)
                    .iter()
                    .map(|(tag, _)| *tag)
                    .collect();
                let mut with_absent = required.clone();
                with_absent.push(ABSENT_TAG);
                for kept_mask in 1u32..1 << size {
                    let id = node_id(&picked(entries, kept_mask));
                    // A prefix keeps entries 0 to k - 1 and no other, so its
                    // mask is 2^k - 1: one below a power of two.
                    let is_prefix = (kept_mask + 1).is_power_of_two();
                    let keeps_required = required_mask & !kept_mask == 0;
                    assert_eq!(
                        contains_list_named(&list, id, &required),
                        is_prefix && keeps_required,
                        "{size} entries, required {required_mask:#08b}, kept {kept_mask:#08b}"
                    );
                    assert!(!contains_list_named(&list, id, &with_absent));
                    // Every prefix holds the first entry, whose value
                    // differs in `other`, so this list contains none of
                    // that list's sublists.
                    if kept_mask & 1 != 0 {
                        let foreign = node_id(&picked(&other, kept_mask));
                        assert!(!contains_list_named(&list, foreign, &required));
                    }
                    checked += 1;
                    contained += usize::from(is_prefix && keeps_required);
                }
            }
        }
        // 2^n sets of required tags times 2^n - 1 sublists, for n of 1 to 6.
        assert_eq!(checked, 2 + 12 + 56 + 240 + 992 + 4_032);
        // A prefix of k entries keeps the required tags of 2^k of the sets:
        // 2^(n+1) - 2 pairs for each n.
        assert_eq!(contained, 2 + 6 + 14 + 30 + 62 + 126);
    }

    /// TLS-7 on the longest list: it contains each of its 32 prefixes that
    /// keeps the required entries, wherever those sit, and no list that
    /// drops, swaps or changes one entry of a prefix.
    #[test]
    fn the_longest_list_contains_its_prefixes_and_nothing_near_them() {
        let entries = tagged_entries(32);
        assert_eq!(entries.len(), MAX_NODE_KEYS);
        let list = NodeKeyList::from_entries(entries.iter().copied())
            .expect("32 increasing tags form a list");
        for length in 1..=entries.len() {
            let prefix = &entries[..length];
            let id = node_id(prefix);
            assert!(contains_list_named(&list, id, &[]));
            for (index, (tag, _)) in entries.iter().enumerate() {
                assert_eq!(
                    contains_list_named(&list, id, &[ED25519_KEY_TAG, *tag]),
                    index < length,
                    "a prefix of {length} entries and the required entry {index}"
                );
            }
            for index in 0..length {
                let mut dropped = prefix.to_vec();
                dropped.remove(index);
                // Dropping the last entry leaves a shorter prefix.
                let is_prefix = index + 1 == length && length > 1;
                assert_eq!(
                    contains_list_named(&list, node_id(&dropped), &[]),
                    is_prefix
                );
                let mut changed = prefix.to_vec();
                changed[index].1[0] ^= 1;
                assert!(!contains_list_named(&list, node_id(&changed), &[]));
                if index + 1 < length {
                    let mut swapped = prefix.to_vec();
                    swapped.swap(index, index + 1);
                    assert!(!contains_list_named(&list, node_id(&swapped), &[]));
                }
            }
        }
    }

    /// The check hashes fewer lists than the list has entries, on the
    /// longest list too: one per proper prefix that could match.
    #[test]
    fn the_check_hashes_at_most_one_list_per_entry() {
        for count in [1, 2, 4, 32] {
            let entries = tagged_entries(count);
            let list = NodeKeyList::from_entries(entries.iter().copied())
                .expect("increasing tags form a list");
            let entries = usize::from(count);
            assert_eq!(list.shortest_prefix_keeping(&[]), Some(1));
            assert_eq!(list.earlier_ids(1).count(), entries - 1);
            // Requiring the last entry leaves only the whole list, whose ID
            // is already known.
            assert_eq!(list.shortest_prefix_keeping(&[count]), Some(entries));
            assert_eq!(list.earlier_ids(entries).count(), 0);
            assert_eq!(list.shortest_prefix_keeping(&[count + 1]), None);
        }
    }

    /// A list that lacks a required tag contains nothing, not even itself.
    #[test]
    fn a_list_without_a_required_entry_contains_no_list() {
        let list = NodeKeyList::single(key());
        assert!(contains_list_named(
            &list,
            list.node_id(),
            &[ED25519_KEY_TAG]
        ));
        assert!(!contains_list_named(
            &list,
            list.node_id(),
            &[ED25519_KEY_TAG, 3]
        ));
    }

    #[test]
    fn the_longest_list_fills_the_length_bound() {
        let mut entries = tagged_entries(32);
        entries[0].1 = *NodeSecretKey::generate().public_key().as_bytes();
        let list = NodeKeyList::from_entries(entries).expect("32 increasing tags form a list");
        assert_eq!(list.encode().len(), MAX_NODE_KEY_LIST_BYTES);
        assert_eq!(MAX_NODE_KEY_LIST_BYTES, 1_089);
        assert_eq!(NodeKeyList::decode(&list.encode()), Ok(list));
    }
}
