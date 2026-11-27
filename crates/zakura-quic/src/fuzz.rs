//! Fuzz drivers for the bytes a peer chooses before its handshake
//! authenticates it: its credential (TLS-3, ID-12), a key list (ID-8), and its
//! composite handshake signature (TLS-4).
//!
//! `qa/fuzz/p2p` calls each driver with libFuzzer's bytes. A driver panics
//! with the property ID that a parser or the verifier broke. The unit tests
//! run the same drivers on [`seed_inputs`], the corpus a campaign starts from.

use std::sync::OnceLock;

use blake2b_simd::Params;

use crate::{
    composite::{self, COMPOSITE_SIGNATURE_BYTES, ML_DSA_65_KEY_TAG, ML_DSA_65_SIGNATURE_BYTES},
    credential::{CredentialKey, NodeCredential, MAX_CREDENTIAL_KEY_BYTES},
    key::{NodeId, NodeKey, NodeSecretKey, KEY_LENGTH},
    key_list::{NodeKeyList, ED25519_KEY_TAG},
};

const CREDENTIAL_CANONICAL: &str = "credential/canonical-encoding";
const CREDENTIAL_NODE_ID: &str = "credential/node-id";
const CREDENTIAL_BOUNDED: &str = "credential/bounded-parse";
const KEY_LIST_CANONICAL: &str = "key-list/canonical-encoding";
const KEY_LIST_ID: &str = "key-list/stable-id";
const COMPOSITE_TAMPER: &str = "composite/tamper-refused";

/// How far one allocation of a credential parse may exceed its input's
/// length, in bytes. The allocations the input's length doesn't bound follow
/// its entry count, at most 32 (ID-8): the parsers' entry tables, under 50
/// bytes an entry, and the key list a credential of short keys maps to, 34
/// bytes an entry.
pub const PARSE_ALLOCATION_SLACK_BYTES: usize = 2_048;

/// The BLAKE2b personalisation of a node ID (ID-9), restated so the drivers
/// check IDs against the spec rather than the code under test.
const NODE_ID_PERSONALISATION: &[u8; 16] = b"Zakura_NodeId_v1";

/// The BLAKE2b personalisation of a key's fingerprint (ID-12), restated for
/// the same reason.
const KEY_FINGERPRINT_PERSONALISATION: &[u8; 16] = b"Zakura_KeyFpr_v1";

/// The longest credential encoding (ID-12), restated for the same reason.
const LONGEST_CREDENTIAL_BYTES: usize = 10_385;

/// The most entries a key list or a credential holds (ID-8), restated for
/// the same reason.
const MOST_ENTRIES: usize = 32;

/// The DER `AlgorithmIdentifier` of a credential's SPKI (TLS-3): a SEQUENCE
/// holding only the object identifier `2.25.<UUID 6a31af47-5aa9-48c7-8ab9-d117edca0d9a>`.
/// A unit test derives the identifier from the UUID.
const CREDENTIAL_ALGORITHM_DER: &[u8] = &[
    0x30, 0x16, 0x06, 0x14, 0x69, 0x81, 0xd4, 0xb1, 0xd7, 0xd1, 0xeb, 0xaa, 0xca, 0xa3, 0x8f, 0x8a,
    0xdc, 0xf4, 0xa2, 0xfe, 0xee, 0xa8, 0x9b, 0x1a,
];

/// Length of the Ed25519 half of a composite signature.
const ED25519_SIGNATURE_BYTES: usize = COMPOSITE_SIGNATURE_BYTES - ML_DSA_65_SIGNATURE_BYTES;

/// The most edits one composite input applies to the honest witness.
const MAX_WITNESS_EDITS: usize = 64;

/// The longest field a witness edit can resize to, in bytes. It exceeds every
/// length the verifier accepts.
const MAX_WITNESS_FIELD_BYTES: usize = 8_192;

/// The seed of the node whose signature is the honest witness.
const WITNESS_SEED: [u8; KEY_LENGTH] = [7; KEY_LENGTH];

/// The message the honest witness signs.
const WITNESS_MESSAGE: &[u8] = b"Zakura composite fuzz witness";

/// Which parsers accepted one credential input.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CredentialReplay {
    /// `NodeCredential::from_spki` accepted the input as a TLS raw public key.
    pub spki: bool,
    /// [`NodeCredential::decode`] accepted the input as a credential encoding.
    pub encoding: bool,
}

/// Parses `data` as a peer's TLS raw public key and as a bare credential
/// encoding, and checks every credential either parser accepts.
///
/// `largest_allocation` runs the two parses once and returns the largest
/// single allocation they made, in bytes. No allocation may exceed the
/// input's length by more than [`PARSE_ALLOCATION_SLACK_BYTES`].
///
/// An accepted credential re-encodes to exactly `data` and decodes back to
/// itself. An accepted SPKI is the canonical DER that TLS-3 describes, and
/// the credential's node ID is the ID-9 hash of its keys fingerprinted per
/// ID-12. Both are rebuilt here from the spec, not by the code under test.
pub fn replay_credential_bytes(
    data: &[u8],
    largest_allocation: &mut dyn FnMut(&mut dyn FnMut()) -> usize,
) -> CredentialReplay {
    let mut spki = None;
    let mut encoding = None;
    let largest = largest_allocation(&mut || {
        spki = Some(NodeCredential::from_spki(data));
        encoding = Some(NodeCredential::decode(data));
    });
    assert!(
        largest <= data.len() + PARSE_ALLOCATION_SLACK_BYTES,
        "{CREDENTIAL_BOUNDED}: parsing {} bytes allocated {largest} bytes at once",
        data.len()
    );
    let spki = spki.expect("the meter runs the parses it measures");
    let encoding = encoding.expect("the meter runs the parses it measures");

    if let Ok(credential) = &spki {
        assert_eq!(
            spki_der(&credential.encode()),
            data,
            "{CREDENTIAL_CANONICAL}: an accepted SPKI is not the canonical DER of its credential"
        );
        check_credential(credential);
    }
    if let Ok(credential) = &encoding {
        assert_eq!(
            credential.encode(),
            data,
            "{CREDENTIAL_CANONICAL}: an accepted encoding is not its credential's encoding"
        );
        assert_eq!(
            NodeCredential::from_spki(&spki_der(data)).as_ref(),
            Ok(credential),
            "{CREDENTIAL_CANONICAL}: the canonical DER of an accepted credential is refused"
        );
        check_credential(credential);
    }
    CredentialReplay {
        spki: spki.is_ok(),
        encoding: encoding.is_ok(),
    }
}

/// Checks the properties every accepted credential holds, however it
/// arrived. The caller has checked that its encoding is the input's.
fn check_credential(credential: &NodeCredential) {
    let encoding = credential.encode();
    assert!(
        encoding.len() <= LONGEST_CREDENTIAL_BYTES && usize::from(encoding[0]) <= MOST_ENTRIES,
        "{CREDENTIAL_BOUNDED}: an accepted credential has {} entries in {} bytes",
        encoding[0],
        encoding.len()
    );
    assert_eq!(
        NodeCredential::decode(&encoding).as_ref(),
        Ok(credential),
        "{CREDENTIAL_CANONICAL}: a credential doesn't decode from its encoding"
    );
    assert_eq!(
        node_id_of(&fingerprinted_key_list(&encoding)),
        credential.node_id(),
        "{CREDENTIAL_NODE_ID}: the ID is not the hash of the fingerprinted keys"
    );
    assert_eq!(
        credential.ed25519().map(|key| key.as_bytes().to_vec()),
        credential.key(ED25519_KEY_TAG).map(<[u8]>::to_vec),
        "{CREDENTIAL_NODE_ID}: the key list's Ed25519 key isn't the credential's"
    );
}

/// The canonical DER SubjectPublicKeyInfo carrying credential `encoding`
/// (TLS-3): the algorithm, then the encoding as a BIT STRING with no unused
/// bits.
fn spki_der(encoding: &[u8]) -> Vec<u8> {
    let mut bits = vec![0];
    bits.extend_from_slice(encoding);
    let mut contents = CREDENTIAL_ALGORITHM_DER.to_vec();
    contents.extend(der(0x03, &bits));
    der(0x30, &contents)
}

/// One DER element: `tag`, the shortest length form, then `contents`.
fn der(tag: u8, contents: &[u8]) -> Vec<u8> {
    let mut bytes = vec![tag];
    match u8::try_from(contents.len()) {
        Ok(short @ 0..=0x7f) => bytes.push(short),
        Ok(one_byte) => bytes.extend([0x81, one_byte]),
        Err(_) => {
            let two_bytes =
                u16::try_from(contents.len()).expect("a credential's SPKI is under 64 KiB");
            bytes.push(0x82);
            bytes.extend(two_bytes.to_be_bytes());
        }
    }
    bytes.extend_from_slice(contents);
    bytes
}

/// The bit of a credential entry's length that marks it fingerprint-only
/// (ID-12), restated like the personalisations above.
const FINGERPRINT_ONLY_FLAG: u16 = 0x8000;

/// The encoded key list naming an accepted credential `encoding` (ID-12): each
/// Ed25519 key as itself, any other key as BLAKE2b-256 under
/// [`KEY_FINGERPRINT_PERSONALISATION`] of its little-endian tag and its bytes,
/// and a fingerprint-only entry as the 32 bytes it carries.
fn fingerprinted_key_list(encoding: &[u8]) -> Vec<u8> {
    let (&count, mut rest) = encoding
        .split_first()
        .expect("an accepted credential has a count byte");
    let mut list = vec![count];
    for _ in 0..count {
        let (&[tag_low, tag_high, length_low, length_high], body) = rest
            .split_first_chunk::<4>()
            .expect("an accepted credential holds its counted entries");
        let length = u16::from_le_bytes([length_low, length_high]);
        let fingerprint_only = length & FINGERPRINT_ONLY_FLAG != 0;
        let (key, tail) = body.split_at(usize::from(length & !FINGERPRINT_ONLY_FLAG));
        let tag = [tag_low, tag_high];
        let is_ed25519 = u16::from_le_bytes(tag) == ED25519_KEY_TAG;
        assert!(
            !fingerprint_only || (key.len() == KEY_LENGTH && !is_ed25519),
            "{CREDENTIAL_CANONICAL}: an accepted fingerprint-only entry is not 32 bytes of a \
             key other than Ed25519"
        );
        list.extend_from_slice(&tag);
        if is_ed25519 || fingerprint_only {
            list.extend_from_slice(key);
        } else {
            let fingerprint = Params::new()
                .hash_length(KEY_LENGTH)
                .personal(KEY_FINGERPRINT_PERSONALISATION)
                .to_state()
                .update(&tag)
                .update(key)
                .finalize();
            list.extend_from_slice(fingerprint.as_bytes());
        }
        rest = tail;
    }
    list
}

/// Whether [`NodeKeyList::decode`] accepted one input.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KeyListReplay {
    /// The input decoded as a key list.
    pub accepted: bool,
}

/// Decodes `data` as a key list, as discovery does with a peer's record.
///
/// An accepted list re-encodes to exactly `data` and decodes back to itself,
/// and its node ID is the ID-9 hash of `data`.
pub fn replay_key_list_bytes(data: &[u8]) -> KeyListReplay {
    let Ok(list) = NodeKeyList::decode(data) else {
        return KeyListReplay::default();
    };
    assert!(
        data.len() <= 1 + MOST_ENTRIES * (2 + KEY_LENGTH),
        "{KEY_LIST_CANONICAL}: an accepted list is {} bytes, more than {MOST_ENTRIES} entries",
        data.len()
    );
    assert_eq!(
        list.encode(),
        data,
        "{KEY_LIST_CANONICAL}: an accepted list is not its own encoding"
    );
    assert_eq!(
        list.encoded_len(),
        data.len(),
        "{KEY_LIST_CANONICAL}: the encoded length disagrees with the encoding"
    );
    let decoded = NodeKeyList::decode(&list.encode());
    assert_eq!(
        decoded.as_ref(),
        Ok(&list),
        "{KEY_LIST_CANONICAL}: a list doesn't decode from its encoding"
    );
    assert_eq!(
        decoded.map(|decoded| decoded.node_id()),
        Ok(list.node_id()),
        "{KEY_LIST_ID}: the re-decoded list names another ID"
    );
    assert_eq!(
        list.node_id(),
        node_id_of(data),
        "{KEY_LIST_ID}: the ID is not the hash of the bytes received"
    );
    // Reads the Ed25519 entry, whose accessor expects construction checked it.
    let _ = list.ed25519();
    KeyListReplay { accepted: true }
}

/// The ID-9 node ID of an encoded key list.
fn node_id_of(encoded: &[u8]) -> NodeId {
    let hash = Params::new()
        .hash_length(KEY_LENGTH)
        .personal(NODE_ID_PERSONALISATION)
        .hash(encoded);
    let mut digest = [0; KEY_LENGTH];
    digest.copy_from_slice(hash.as_bytes());
    NodeId::from_bytes(&digest)
}

/// What one composite input reached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompositeReplay {
    /// The input's Ed25519 key is not one a credential admits, so the
    /// verifier never sees it.
    KeyRefused,
    /// The verifier ran on the input's own key, message and signature.
    Raw {
        /// The signature verified.
        verified: bool,
    },
    /// The verifier ran on the honest witness after the input's edits.
    Witness {
        /// The edits changed the witness.
        edited: bool,
        /// The signature verified.
        verified: bool,
    },
}

/// Verifies one composite signature that `data` describes.
///
/// The first byte's low bit picks the layout. With it clear, the rest is an
/// Ed25519 key, a `u16`-prefixed ML-DSA-65 key, a `u16`-prefixed message and
/// the signature, each length cut to the bytes left. With it set, the rest is
/// up to [`MAX_WITNESS_EDITS`] four-byte edits of an honest signature, so a
/// campaign explores near-valid signatures without a corpus. Each input
/// verifies at most once.
///
/// The honest witness verifies, and every edit of it is refused. A raw input
/// checks only that the verifier doesn't panic.
pub fn replay_composite_bytes(data: &[u8]) -> CompositeReplay {
    match data.split_first() {
        Some((&layout, rest)) if layout & 1 == 1 => replay_witness(rest),
        Some((_, rest)) => replay_raw(rest),
        None => replay_raw(&[]),
    }
}

fn replay_raw(data: &[u8]) -> CompositeReplay {
    let Some((key, mut rest)) = data.split_first_chunk::<KEY_LENGTH>() else {
        return CompositeReplay::KeyRefused;
    };
    let Ok(ed25519) = NodeKey::from_bytes(key) else {
        return CompositeReplay::KeyRefused;
    };
    let ml_dsa = take_prefixed(&mut rest);
    let message = take_prefixed(&mut rest);
    let verified = verify(&ed25519, ml_dsa, message, rest);
    CompositeReplay::Raw { verified }
}

/// Splits a little-endian `u16` length and that many bytes, or as many as
/// remain, off the front of `rest`.
fn take_prefixed<'a>(rest: &mut &'a [u8]) -> &'a [u8] {
    let (length, after) = match rest.split_first_chunk::<2>() {
        Some((length, after)) => (usize::from(u16::from_le_bytes(*length)), after),
        None => (0, &[][..]),
    };
    let (field, tail) = after.split_at(length.min(after.len()));
    *rest = tail;
    field
}

/// An honest composite signature and what it verifies under.
struct Witness {
    ed25519: [u8; KEY_LENGTH],
    ml_dsa: Vec<u8>,
    message: Vec<u8>,
    signature: Vec<u8>,
}

/// The honest witness, signed once per process: key generation and signing
/// cost more than the verification each input pays for.
fn witness() -> &'static Witness {
    static WITNESS: OnceLock<Witness> = OnceLock::new();
    WITNESS.get_or_init(|| {
        let secret = NodeSecretKey::from_bytes(&WITNESS_SEED);
        Witness {
            ed25519: *secret.public_key().as_bytes(),
            ml_dsa: secret
                .credential()
                .key(ML_DSA_65_KEY_TAG)
                .expect("a seed-derived credential holds its ML-DSA-65 key")
                .to_vec(),
            message: WITNESS_MESSAGE.to_vec(),
            signature: secret
                .sign_composite(WITNESS_MESSAGE, [9; 32])
                .expect("signing fails only past FIPS 204's 2^-256 rejection bound"),
        }
    })
}

/// Applies `data`'s edits to the witness and verifies the result.
///
/// Each edit is four bytes: a field selector, a little-endian `u16` and a
/// value. Selectors 0 to 3 XOR the value into the Ed25519 key, the ML-DSA-65
/// key, the message or the signature at the `u16`'s offset; 4 to 6 resize the
/// ML-DSA-65 key, the message or the signature to the `u16`'s length, up to
/// [`MAX_WITNESS_FIELD_BYTES`], filling with the value.
fn replay_witness(data: &[u8]) -> CompositeReplay {
    let honest = witness();
    let mut ed25519 = honest.ed25519;
    let mut fields = [
        honest.ml_dsa.clone(),
        honest.message.clone(),
        honest.signature.clone(),
    ];
    let (edits, _) = data.as_chunks::<4>();
    for &[selector, low, high, value] in edits.iter().take(MAX_WITNESS_EDITS) {
        let at = usize::from(u16::from_le_bytes([low, high]));
        match selector % 7 {
            0 => ed25519[at % KEY_LENGTH] ^= value,
            selector @ 1..=3 => {
                let field = &mut fields[usize::from(selector - 1)];
                if !field.is_empty() {
                    let at = at % field.len();
                    field[at] ^= value;
                }
            }
            selector => {
                fields[usize::from(selector - 4)].resize(at % (MAX_WITNESS_FIELD_BYTES + 1), value)
            }
        }
    }
    let [ml_dsa, message, signature] = &fields;
    let edited = ed25519 != honest.ed25519
        || *ml_dsa != honest.ml_dsa
        || *message != honest.message
        || *signature != honest.signature;
    let Ok(key) = NodeKey::from_bytes(&ed25519) else {
        return CompositeReplay::KeyRefused;
    };
    let verified = verify(&key, ml_dsa, message, signature);
    assert_eq!(
        verified, !edited,
        "{COMPOSITE_TAMPER}: the witness verified {verified} after edits that changed it: {edited}"
    );
    CompositeReplay::Witness { edited, verified }
}

fn verify(ed25519: &NodeKey, ml_dsa: &[u8], message: &[u8], signature: &[u8]) -> bool {
    composite::verify(ed25519, ml_dsa, message, signature).is_ok()
}

/// One input of a campaign's starting corpus.
#[derive(Clone, Debug)]
pub struct Seed {
    /// The fuzz target whose corpus it seeds.
    pub target: &'static str,
    /// Its file name. A name starting `valid-` is an input the target's
    /// parser or verifier accepts; `invalid-` is one it refuses.
    pub name: &'static str,
    /// The input.
    pub bytes: Vec<u8>,
}

/// The starting corpus of the `credential_parse`, `key_list_decode` and
/// `composite_verify` targets: the well-formed and malformed values of the
/// parsers' unit tests, and the composite witness in both layouts.
pub fn seed_inputs() -> Vec<Seed> {
    let mut seeds = credential_seeds();
    seeds.extend(key_list_seeds());
    seeds.extend(composite_seeds());
    seeds
}

/// A credential encoding built by hand, so it can break the rules that
/// [`NodeCredential::from_entries`] enforces.
fn credential_encoding(count: u8, entries: &[(u16, &[u8])]) -> Vec<u8> {
    let mut bytes = vec![count];
    for (tag, key) in entries {
        bytes.extend_from_slice(&tag.to_le_bytes());
        let length = u16::try_from(key.len()).expect("seed keys are short");
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(key);
    }
    bytes
}

fn credential_seeds() -> Vec<Seed> {
    const TARGET: &str = "credential_parse";
    const UNKNOWN_TAG: u16 = 0x0102;
    let node = NodeSecretKey::from_bytes(&WITNESS_SEED);
    let ed = *node.public_key().as_bytes();
    let single = NodeCredential::single(node.public_key());
    let unknown = NodeCredential::from_entries([
        (ED25519_KEY_TAG, ed.to_vec()),
        (UNKNOWN_TAG, vec![0xCD; 1_312]),
    ])
    .expect("two increasing entries form a credential");
    // The Ed25519 key, then keys of the given lengths under the tags from 4
    // up. Tag 3 is ML-DSA-65, whose length is fixed.
    let keys_of = |lengths: &[usize]| {
        let more: Vec<_> = lengths.iter().map(|length| vec![0xEE; *length]).collect();
        let mut entries = vec![(ED25519_KEY_TAG, &ed[..])];
        entries.extend((4u16..).zip(more.iter().map(Vec::as_slice)));
        let count = u8::try_from(entries.len()).expect("seeds hold few entries");
        credential_encoding(count, &entries)
    };
    // The byte limit (ID-12): three keys of the longest length and one of
    // 2,556 bytes fill it, and one byte more passes it.
    let longest = keys_of(&[
        MAX_CREDENTIAL_KEY_BYTES,
        MAX_CREDENTIAL_KEY_BYTES,
        MAX_CREDENTIAL_KEY_BYTES,
        2_556,
    ]);
    assert_eq!(longest.len(), LONGEST_CREDENTIAL_BYTES);
    let too_long = keys_of(&[
        MAX_CREDENTIAL_KEY_BYTES,
        MAX_CREDENTIAL_KEY_BYTES,
        MAX_CREDENTIAL_KEY_BYTES,
        2_557,
    ]);
    // The entry count (ID-8): 32 entries are a credential and 33 are not.
    let retired = |count: u8| {
        let fingerprints = (0..count).map(|index| {
            (
                u16::from(index) + 4,
                CredentialKey::Fingerprint([index; 32]),
            )
        });
        [(ED25519_KEY_TAG, CredentialKey::Full(ed.to_vec()))]
            .into_iter()
            .chain(fingerprints)
    };
    let most_entries =
        NodeCredential::from_entries(retired(31)).expect("32 increasing entries form a credential");
    let mut too_many_entries = most_entries.encode();
    too_many_entries[0] += 1;
    too_many_entries.extend_from_slice(&UNKNOWN_TAG.to_le_bytes());
    too_many_entries.extend_from_slice(&(32 | FINGERPRINT_ONLY_FLAG).to_le_bytes());
    too_many_entries.extend_from_slice(&[0xCD; KEY_LENGTH]);

    let fingerprint_only = node
        .credential()
        .clone()
        .with_fingerprint_only(ML_DSA_65_KEY_TAG)
        .expect("the node's ML-DSA-65 key can be given as its fingerprint");
    // One entry whose length carries the fingerprint-only flag.
    let flagged = |count: u8, leading: &[(u16, &[u8])], tag: u16, value: &[u8]| {
        let mut bytes = credential_encoding(count, leading);
        bytes.extend_from_slice(&tag.to_le_bytes());
        let length = u16::try_from(value.len()).expect("seed keys are short");
        bytes.extend_from_slice(&(length | FINGERPRINT_ONLY_FLAG).to_le_bytes());
        bytes.extend_from_slice(value);
        bytes
    };

    let spki = single.to_spki().as_ref().to_vec();
    let mut trailing = spki.clone();
    trailing.push(0);
    let mut long_form = vec![0x30, 0x81, spki[1]];
    long_form.extend_from_slice(&spki[2..]);
    let encoding = single.encode();

    let seeds = [
        ("valid-ed25519-spki", spki.clone()),
        ("valid-ed25519-encoding", encoding.clone()),
        (
            "valid-node-spki",
            node.credential().to_spki().as_ref().to_vec(),
        ),
        ("valid-node-encoding", node.credential().encode()),
        (
            "valid-unknown-tag-spki",
            unknown.to_spki().as_ref().to_vec(),
        ),
        ("valid-longest-spki", spki_der(&longest)),
        ("valid-longest-encoding", longest),
        ("invalid-too-long-spki", spki_der(&too_long)),
        ("invalid-too-long-encoding", too_long),
        (
            "valid-32-entries-spki",
            most_entries.to_spki().as_ref().to_vec(),
        ),
        ("valid-32-entries-encoding", most_entries.encode()),
        ("invalid-33-entries", too_many_entries),
        (
            "valid-fingerprint-only-spki",
            fingerprint_only.to_spki().as_ref().to_vec(),
        ),
        ("valid-fingerprint-only-encoding", fingerprint_only.encode()),
        (
            "valid-fingerprint-only-unknown-tag",
            flagged(
                2,
                &[(ED25519_KEY_TAG, &ed)],
                UNKNOWN_TAG,
                &[0xCD; KEY_LENGTH],
            ),
        ),
        (
            "invalid-fingerprint-only-ed25519",
            flagged(1, &[], ED25519_KEY_TAG, &ed),
        ),
        (
            "invalid-fingerprint-only-short",
            flagged(
                2,
                &[(ED25519_KEY_TAG, &ed)],
                UNKNOWN_TAG,
                &[0xCD; KEY_LENGTH - 1],
            ),
        ),
        (
            "invalid-fingerprint-only-long",
            flagged(
                2,
                &[(ED25519_KEY_TAG, &ed)],
                UNKNOWN_TAG,
                &[0xCD; KEY_LENGTH + 1],
            ),
        ),
        ("invalid-trailing-spki", trailing),
        ("invalid-long-form-spki", long_form),
        (
            "invalid-truncated-encoding",
            encoding[..encoding.len() - 1].to_vec(),
        ),
        ("invalid-no-entries", vec![0]),
        (
            "invalid-empty-key",
            credential_encoding(1, &[(UNKNOWN_TAG, &[])]),
        ),
        (
            "invalid-short-ed25519",
            credential_encoding(1, &[(ED25519_KEY_TAG, &ed[..31])]),
        ),
        (
            "invalid-zero-ed25519",
            credential_encoding(1, &[(ED25519_KEY_TAG, &[0; KEY_LENGTH])]),
        ),
        (
            "invalid-tag-order",
            credential_encoding(2, &[(UNKNOWN_TAG, &[1]), (ED25519_KEY_TAG, &ed)]),
        ),
        (
            "invalid-short-ml-dsa",
            credential_encoding(
                2,
                &[
                    (ED25519_KEY_TAG, &ed),
                    (ML_DSA_65_KEY_TAG, &[2; KEY_LENGTH]),
                ],
            ),
        ),
    ];
    seeds
        .into_iter()
        .map(|(name, bytes)| Seed {
            target: TARGET,
            name,
            bytes,
        })
        .collect()
}

fn key_list_seeds() -> Vec<Seed> {
    const TARGET: &str = "key_list_decode";
    let ed = *NodeSecretKey::from_bytes(&WITNESS_SEED)
        .public_key()
        .as_bytes();
    let list = |count: u8, entries: &[(u16, [u8; KEY_LENGTH])]| {
        let mut bytes = vec![count];
        for (tag, value) in entries {
            bytes.extend_from_slice(&tag.to_le_bytes());
            bytes.extend_from_slice(value);
        }
        bytes
    };
    let value = [0xCD; KEY_LENGTH];
    // A list holds 32 entries (ID-8): the Ed25519 key and 31 more, or 33.
    let after_ed25519 = |more: u16| -> Vec<_> {
        [(ED25519_KEY_TAG, ed)]
            .into_iter()
            .chain((2..2 + more).map(|tag| (tag, value)))
            .collect()
    };
    let mut trailing = list(1, &[(2, value)]);
    trailing.push(0);

    let seeds = [
        ("valid-ed25519", list(1, &[(ED25519_KEY_TAG, ed)])),
        (
            "valid-unknown-tag",
            list(2, &[(ED25519_KEY_TAG, ed), (0x0102, [0xAB; KEY_LENGTH])]),
        ),
        ("valid-no-ed25519", list(1, &[(0x0102, [0xAB; KEY_LENGTH])])),
        ("valid-five-entries", list(5, &after_ed25519(4))),
        ("valid-longest", list(32, &after_ed25519(31))),
        ("invalid-empty", Vec::new()),
        ("invalid-no-entries", vec![0]),
        ("invalid-33-entries", list(33, &after_ed25519(32))),
        ("invalid-count-mismatch", list(2, &[(2, value)])),
        ("invalid-trailing", trailing),
        ("invalid-zero-tag", list(1, &[(0, value)])),
        ("invalid-tag-order", list(2, &[(3, value), (2, value)])),
        ("invalid-repeated-tag", list(2, &[(2, value), (2, value)])),
        (
            "invalid-zero-ed25519",
            list(1, &[(ED25519_KEY_TAG, [0; KEY_LENGTH])]),
        ),
    ];
    seeds
        .into_iter()
        .map(|(name, bytes)| Seed {
            target: TARGET,
            name,
            bytes,
        })
        .collect()
}

fn composite_seeds() -> Vec<Seed> {
    const TARGET: &str = "composite_verify";
    let honest = witness();
    let raw = |message: &[u8]| {
        let mut bytes = vec![0];
        bytes.extend_from_slice(&honest.ed25519);
        for field in [&honest.ml_dsa[..], message] {
            let length = u16::try_from(field.len()).expect("the witness's fields are short");
            bytes.extend_from_slice(&length.to_le_bytes());
            bytes.extend_from_slice(field);
        }
        bytes.extend_from_slice(&honest.signature);
        bytes
    };
    // Selector 3 flips a bit of the signature's ML-DSA-65 half; 6 truncates it.
    let ml_dsa_half = u16::try_from(ED25519_SIGNATURE_BYTES).expect("64 fits a u16");
    let [low, high] = ml_dsa_half.to_le_bytes();

    let seeds = [
        ("valid-raw", raw(&honest.message)),
        ("invalid-raw-message", raw(b"another message")),
        ("valid-witness", vec![1]),
        ("invalid-witness-ml-dsa-bit", vec![1, 3, low, high, 1]),
        ("invalid-witness-truncated", vec![1, 6, low, high, 0]),
    ];
    seeds
        .into_iter()
        .map(|(name, bytes)| Seed {
            target: TARGET,
            name,
            bytes,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs a parse without measuring it.
    fn unmetered(parse: &mut dyn FnMut()) -> usize {
        parse();
        0
    }

    fn seeds(target: &str) -> impl Iterator<Item = Seed> + '_ {
        seed_inputs()
            .into_iter()
            .filter(move |seed| seed.target == target)
    }

    #[test]
    fn credential_seeds_hold_their_named_outcome() {
        for seed in seeds("credential_parse") {
            let replay = replay_credential_bytes(&seed.bytes, &mut unmetered);
            assert_eq!(
                replay.spki || replay.encoding,
                seed.name.starts_with("valid-"),
                "{}",
                seed.name
            );
        }
    }

    #[test]
    fn key_list_seeds_hold_their_named_outcome() {
        for seed in seeds("key_list_decode") {
            assert_eq!(
                replay_key_list_bytes(&seed.bytes).accepted,
                seed.name.starts_with("valid-"),
                "{}",
                seed.name
            );
        }
    }

    #[test]
    fn composite_seeds_hold_their_named_outcome() {
        for seed in seeds("composite_verify") {
            let verified = match replay_composite_bytes(&seed.bytes) {
                CompositeReplay::KeyRefused => false,
                CompositeReplay::Raw { verified } | CompositeReplay::Witness { verified, .. } => {
                    verified
                }
            };
            assert_eq!(verified, seed.name.starts_with("valid-"), "{}", seed.name);
        }
    }

    #[test]
    fn a_witness_edit_that_restores_the_byte_is_no_edit() {
        assert_eq!(
            replay_composite_bytes(&[1, 3, 70, 0, 0xFF, 3, 70, 0, 0xFF]),
            CompositeReplay::Witness {
                edited: false,
                verified: true
            }
        );
    }

    #[test]
    fn an_invalid_ed25519_key_never_reaches_the_verifier() {
        assert_eq!(replay_composite_bytes(&[]), CompositeReplay::KeyRefused);
        assert_eq!(
            replay_composite_bytes(&[0; 1 + KEY_LENGTH]),
            CompositeReplay::KeyRefused
        );
    }

    /// TLS-3 names the credential algorithm by the X.667 UUID arc: `2.25`
    /// packs into one byte, then the UUID as an integer in base 128.
    #[test]
    fn the_restated_algorithm_is_the_uuid_arc() {
        let mut uuid = u128::from_str_radix("6a31af475aa948c78ab9d117edca0d9a", 16).unwrap();
        let mut arc = Vec::new();
        while uuid > 0 {
            let digit = u8::try_from(uuid & 0x7f).unwrap();
            arc.insert(0, if arc.is_empty() { digit } else { digit | 0x80 });
            uuid >>= 7;
        }
        let oid = der(0x06, &[&[2 * 40 + 25][..], &arc].concat());
        assert_eq!(der(0x30, &oid), CREDENTIAL_ALGORITHM_DER);
    }

    #[test]
    #[should_panic(expected = "credential/bounded-parse")]
    fn an_allocation_beyond_the_slack_fails_the_bound() {
        let input =
            NodeCredential::single(NodeSecretKey::from_bytes(&WITNESS_SEED).public_key()).encode();
        replay_credential_bytes(&input, &mut |parse| {
            parse();
            input.len() + PARSE_ALLOCATION_SLACK_BYTES + 1
        });
    }
}
