//! A record is authentic only with both of its signatures.
//!
//! Most tests here play the attacker the ML-DSA-65 signature exists for: one
//! who can make an Ed25519 signature for any body. [`resigned`] is that
//! attacker, signing a changed body with the author's own Ed25519 key, so
//! only the ML-DSA-65 rules stand between the change and the book.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use zakura_quic::{NodeKeyList, NodeSecretKey, ML_DSA_65_KEY_BYTES, ML_DSA_65_SIGNATURE_BYTES};
use zakura_rt::sync::watch;

use super::{
    super::{
        book_file::DiscoveryBookFile,
        protocol::{
            current_unix_secs, hello_verdict, DiscoveryBookError, DiscoveryMessage,
            DiscoveryRecordError, DiscoveryRecordValidationContext, DiscoveryWireError,
            RecordVerdict, ZakuraDiscoveryConfig, ZakuraDiscoveryHandle,
            ZakuraDiscoveryLocalConfig, ZakuraDiscoveryPersistedEntry, ZakuraNodeRecord,
            ZakuraNodeRecordBody, ZakuraServiceId, MAX_NODE_RECORD_BODY_BYTES,
            MAX_NODE_RECORD_BYTES, MAX_NODE_RECORD_FIELDS_BYTES, RECORD_SIGNATURE_EXTENSION_BYTES,
        },
        wire::record_len,
    },
    *,
};
use crate::zakura::{
    wire_codec::{decode_frame, encode_frame},
    Frame, ZakuraNetworkId, ZAKURA_PROTOCOL_VERSION_CURRENT,
};

const CHAIN_ID: [u8; 32] = [7; 32];

/// A fixed time for the tests that verify records directly.
const NOW: u64 = 1_800_000_000;

/// The author most tests sign as.
fn author() -> NodeSecretKey {
    NodeSecretKey::from_bytes(&[0x21; 32])
}

/// Another node, whose key and signatures are valid but not the author's.
fn other_node() -> NodeSecretKey {
    NodeSecretKey::from_bytes(&[0x22; 32])
}

fn context_at(now: u64) -> DiscoveryRecordValidationContext {
    DiscoveryRecordValidationContext {
        expected_network_id: ZakuraNetworkId::Regtest,
        expected_chain_id: CHAIN_ID,
        current_unix_secs: now,
        supported_protocol_min: ZAKURA_PROTOCOL_VERSION_CURRENT,
        supported_protocol_max: ZAKURA_PROTOCOL_VERSION_CURRENT,
        max_record_ttl: std::time::Duration::from_secs(24 * 60 * 60),
        clock_skew_tolerance: std::time::Duration::from_secs(300),
    }
}

fn context() -> DiscoveryRecordValidationContext {
    context_at(NOW)
}

fn addr(host: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(45, 33, 30, host)), 8233)
}

/// A body by `secret` that expires a minute after `now`.
fn body_at(secret: &NodeSecretKey, now: u64) -> ZakuraNodeRecordBody {
    ZakuraNodeRecordBody {
        keys: secret.key_list().clone(),
        direct_addrs: vec![addr(10)],
        services: vec![ZakuraServiceId::discovery()],
        zakura_protocol_min: ZAKURA_PROTOCOL_VERSION_CURRENT,
        zakura_protocol_max: ZAKURA_PROTOCOL_VERSION_CURRENT,
        network_id: ZakuraNetworkId::Regtest,
        chain_id: CHAIN_ID,
        sequence: 42,
        expires_at_unix_secs: now + 60,
        extensions: Vec::new(),
    }
}

fn body(secret: &NodeSecretKey) -> ZakuraNodeRecordBody {
    body_at(secret, NOW)
}

fn tlv(kind: u16, value: &[u8]) -> Vec<u8> {
    let len = u16::try_from(value.len()).expect("a test TLV value is short");
    let mut bytes = kind.to_le_bytes().to_vec();
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value);
    bytes
}

/// Two TLVs this release does not know, one of them empty.
fn unknown_tlvs() -> Vec<u8> {
    [tlv(0x7001, &[0xaa, 0xbb, 0xcc]), tlv(0x7002, &[])].concat()
}

/// The author's record with [`unknown_tlvs`] before its key and signature.
fn signed() -> ZakuraNodeRecord {
    let mut body = body(&author());
    body.extensions = unknown_tlvs();
    ZakuraNodeRecord::sign(body, &author()).expect("the record signs")
}

/// The key TLV and the signature TLV of `record`, each whole.
fn key_and_signature_tlvs(record: &ZakuraNodeRecord) -> (Vec<u8>, Vec<u8>) {
    let extensions = parse(&record.body.extensions).expect("a signed record's area parses");
    (
        tlv(RECORD_EXTENSION_ML_DSA_65_KEY, extensions.key),
        tlv(RECORD_EXTENSION_ML_DSA_65_SIGNATURE, extensions.signature),
    )
}

/// `record` with `edit` applied to its body and a fresh, valid Ed25519
/// signature by `secret` over the result: what an Ed25519 forger can make.
fn resigned(
    mut record: ZakuraNodeRecord,
    secret: &NodeSecretKey,
    edit: impl FnOnce(&mut ZakuraNodeRecordBody),
) -> ZakuraNodeRecord {
    edit(&mut record.body);
    let pre_image = record
        .body
        .encode_for_signature()
        .expect("the edited body is within its bounds");
    record.signature = secret.sign(&pre_image);
    record
}

/// The author's record with its extension area replaced by `area`, under a
/// valid Ed25519 signature.
fn with_area(area: Vec<u8>) -> ZakuraNodeRecord {
    resigned(signed(), &author(), |body| body.extensions = area)
}

/// The author's record whose area is `before_signature` and then the
/// author's own valid ML-DSA-65 signature over everything before it, under
/// a valid Ed25519 signature. Only the author can make it, so a refusal of
/// it rests on the encoding rules alone, not on a signature.
fn author_signed(before_signature: Vec<u8>) -> ZakuraNodeRecord {
    let placeholder = tlv(
        RECORD_EXTENSION_ML_DSA_65_SIGNATURE,
        &[0; ML_DSA_65_SIGNATURE_BYTES],
    );
    let unsigned = with_area([before_signature.clone(), placeholder.clone()].concat());
    let pre_image = unsigned.body.encode_for_signature().unwrap();
    let covered = &pre_image[..pre_image.len() - placeholder.len()];
    let signature = author().sign_record_ml_dsa_65(covered, [6; 32]).unwrap();
    with_area(
        [
            before_signature,
            tlv(RECORD_EXTENSION_ML_DSA_65_SIGNATURE, &signature),
        ]
        .concat(),
    )
}

/// The ML-DSA-65 rule that refuses `record`. Panics when the record
/// verifies or fails any other check, so each test proves its own rule.
fn refusal(record: &ZakuraNodeRecord) -> RecordSignatureError {
    match record.verify(&context()) {
        Err(DiscoveryRecordError::InvalidMlDsaSignature(error)) => error,
        other => panic!("expected an ML-DSA-65 refusal, got {other:?}"),
    }
}

fn round_trip(message: &DiscoveryMessage) -> (Vec<u8>, DiscoveryMessage) {
    let frame = encode_frame(message).expect("the message encodes");
    let decoded = decode_frame(&frame).expect("the message decodes");
    (frame.payload, decoded)
}

#[test]
fn a_signed_record_ends_in_its_key_and_signature() {
    let record = signed();
    record.verify(&context()).expect("the record verifies");

    let area = &record.body.extensions;
    let extensions = parse(area).expect("the area parses");
    assert_eq!(extensions.other, unknown_tlvs());
    assert_eq!(
        extensions.key,
        author()
            .credential()
            .key(zakura_quic::ML_DSA_65_KEY_TAG)
            .unwrap()
    );
    assert_eq!(extensions.key.len(), ML_DSA_65_KEY_BYTES);
    assert_eq!(extensions.signature.len(), ML_DSA_65_SIGNATURE_BYTES);
    // FIPS 204 gives the lengths, restated so the test doesn't take them
    // from the constants it checks.
    assert_eq!(RECORD_ML_DSA_65_EXTENSION_BYTES, 4 + 1_952 + 4 + 3_309);
    assert_eq!(
        area.len(),
        unknown_tlvs().len() + RECORD_ML_DSA_65_EXTENSION_BYTES
    );
    // The headers, byte for byte: type 1 and length 1,952, then type 2 and
    // length 3,309, little-endian.
    let key_at = unknown_tlvs().len();
    assert_eq!(area[key_at..key_at + 4], [0x01, 0x00, 0xa0, 0x07]);
    let signature_at = key_at + 4 + 1_952;
    assert_eq!(
        area[signature_at..signature_at + 4],
        [0x02, 0x00, 0xed, 0x0c]
    );
    assert_eq!(signature_at + 4 + 3_309, area.len());
}

/// The ML-DSA-65 signature covers the domain, the format version and the
/// body up to the signature TLV, under the record context. This rebuilds
/// that message without the module's own slicing and checks the signature
/// against it with the public verifier.
#[test]
fn the_signature_covers_the_body_before_the_signature_tlv() {
    let record = signed();
    let (_, signature_tlv) = key_and_signature_tlvs(&record);
    let pre_image = record.body.encode_for_signature().unwrap();
    assert!(pre_image.starts_with(b"zakura-node-record-v1\x03\x00"));
    assert!(pre_image.ends_with(&signature_tlv));
    let covered = &pre_image[..pre_image.len() - signature_tlv.len()];

    let extensions = parse(&record.body.extensions).unwrap();
    assert_eq!(
        verify_record_ml_dsa_65(
            &record.body.keys,
            extensions.key,
            covered,
            extensions.signature
        ),
        Ok(())
    );
    // One byte more or less is another message.
    for message in [&covered[1..], &covered[..covered.len() - 1], &pre_image[..]] {
        assert!(verify_record_ml_dsa_65(
            &record.body.keys,
            extensions.key,
            message,
            extensions.signature
        )
        .is_err());
    }
}

#[test]
fn a_record_without_its_key_tlv_is_refused() {
    let (_, signature) = key_and_signature_tlvs(&signed());
    let record = with_area([unknown_tlvs(), signature].concat());
    assert_eq!(refusal(&record), RecordSignatureError::MissingKey);
}

#[test]
fn a_record_without_its_signature_tlv_is_refused() {
    let (key, _) = key_and_signature_tlvs(&signed());
    let record = with_area([unknown_tlvs(), key].concat());
    assert_eq!(refusal(&record), RecordSignatureError::MissingSignature);
}

/// The record this release's predecessor design would have sent: Ed25519
/// alone, with nothing or only unknown TLVs in the area.
#[test]
fn a_record_with_only_an_ed25519_signature_is_refused() {
    assert_eq!(
        refusal(&with_area(Vec::new())),
        RecordSignatureError::MissingKey
    );
    assert_eq!(
        refusal(&with_area(unknown_tlvs())),
        RecordSignatureError::MissingKey
    );
}

/// A key the list does not name fails on the fingerprint, though it is a
/// real ML-DSA-65 key.
#[test]
fn a_key_that_is_not_the_lists_is_refused() {
    let (_, signature) = key_and_signature_tlvs(&signed());
    let (others_key, _) = key_and_signature_tlvs(
        &ZakuraNodeRecord::sign(body(&other_node()), &other_node()).unwrap(),
    );
    let record = with_area([unknown_tlvs(), others_key, signature].concat());
    assert_eq!(refusal(&record), RecordSignatureError::Invalid);
}

/// Another node's key with that node's valid signature over this very
/// record. Only the fingerprint in the author's key list refuses it: the
/// signature verifies under the key beside it.
#[test]
fn another_nodes_valid_key_and_signature_are_refused() {
    let other = other_node();
    let others_key = other
        .credential()
        .key(zakura_quic::ML_DSA_65_KEY_TAG)
        .unwrap();
    let key_tlv = tlv(RECORD_EXTENSION_ML_DSA_65_KEY, others_key);
    let unsigned = with_area(
        [
            unknown_tlvs(),
            key_tlv.clone(),
            tlv(
                RECORD_EXTENSION_ML_DSA_65_SIGNATURE,
                &[0; ML_DSA_65_SIGNATURE_BYTES],
            ),
        ]
        .concat(),
    );
    let pre_image = unsigned.body.encode_for_signature().unwrap();
    let covered = &pre_image[..pre_image.len() - 4 - ML_DSA_65_SIGNATURE_BYTES];
    let signature = other.sign_record_ml_dsa_65(covered, [5; 32]).unwrap();
    let record = with_area(
        [
            unknown_tlvs(),
            key_tlv,
            tlv(RECORD_EXTENSION_ML_DSA_65_SIGNATURE, &signature),
        ]
        .concat(),
    );

    // The forgery is sound in every way but the key's owner.
    let extensions = parse(&record.body.extensions).unwrap();
    assert_eq!(
        verify_record_ml_dsa_65(
            other.key_list(),
            extensions.key,
            covered,
            extensions.signature
        ),
        Ok(())
    );
    assert_eq!(refusal(&record), RecordSignatureError::Invalid);
}

/// A node's signature over one of its records does not carry to another:
/// the newer record with the older record's signature TLV is refused.
#[test]
fn a_signature_from_another_record_of_the_node_is_refused() {
    let older = signed();
    let mut newer = older.body.clone();
    newer.sequence += 1;
    newer.direct_addrs = vec![addr(99)];
    let newer = ZakuraNodeRecord::sign(newer, &author()).unwrap();
    newer.verify(&context()).expect("the newer record verifies");

    let (key, _) = key_and_signature_tlvs(&newer);
    let (_, old_signature) = key_and_signature_tlvs(&older);
    let replayed = resigned(newer, &author(), |body| {
        body.extensions = [unknown_tlvs(), key, old_signature].concat();
    });
    assert_eq!(refusal(&replayed), RecordSignatureError::Invalid);
}

#[test]
fn a_flipped_byte_in_the_key_is_refused() {
    let key_at = unknown_tlvs().len() + 4;
    for offset in [0, ML_DSA_65_KEY_BYTES / 2, ML_DSA_65_KEY_BYTES - 1] {
        let record = resigned(signed(), &author(), |body| {
            body.extensions[key_at + offset] ^= 1
        });
        assert_eq!(
            refusal(&record),
            RecordSignatureError::Invalid,
            "key byte {offset}"
        );
    }
}

#[test]
fn a_flipped_byte_in_the_signature_is_refused() {
    let area_len = signed().body.extensions.len();
    let signature_at = area_len - ML_DSA_65_SIGNATURE_BYTES;
    for offset in [
        0,
        ML_DSA_65_SIGNATURE_BYTES / 2,
        ML_DSA_65_SIGNATURE_BYTES - 1,
    ] {
        let record = resigned(signed(), &author(), |body| {
            body.extensions[signature_at + offset] ^= 1
        });
        assert_eq!(
            refusal(&record),
            RecordSignatureError::Invalid,
            "signature byte {offset}"
        );
    }
}

/// Every part of the body before the signature TLV is signed: the key
/// list, each field and a byte of an unknown TLV.
#[test]
fn a_flipped_byte_in_the_signed_bytes_is_refused() {
    let edits: [(&str, fn(&mut ZakuraNodeRecordBody)); 10] = [
        // A third entry beside the author's two. The list still names the
        // ML-DSA-65 key, so only the signature over the list refuses it.
        ("key list", |body| {
            let mut entries: Vec<(u16, [u8; 32])> = body.keys.encode()[1..]
                .chunks(34)
                .map(|entry| {
                    let tag = u16::from_le_bytes([entry[0], entry[1]]);
                    (tag, entry[2..].try_into().unwrap())
                })
                .collect();
            entries.push((0x7f00, [0x44; 32]));
            body.keys = NodeKeyList::from_entries(entries).unwrap();
        }),
        ("sequence", |body| body.sequence ^= 1),
        ("expiry", |body| body.expires_at_unix_secs += 1),
        ("address", |body| body.direct_addrs[0] = addr(11)),
        ("service", |body| {
            body.services = vec![ZakuraServiceId::block_sync()]
        }),
        ("protocol minimum", |body| body.zakura_protocol_min -= 1),
        ("protocol maximum", |body| body.zakura_protocol_max += 1),
        ("network", |body| body.network_id = ZakuraNetworkId::Testnet),
        ("chain", |body| body.chain_id[31] ^= 1),
        ("unknown TLV value", |body| body.extensions[4] ^= 1),
    ];
    for (name, edit) in edits {
        let record = resigned(signed(), &author(), edit);
        assert_eq!(refusal(&record), RecordSignatureError::Invalid, "{name}");
    }
}

/// Without an Ed25519 forgery, the same edits fail the Ed25519 check, which
/// covers the whole body and the ML-DSA-65 signature in it.
#[test]
fn the_ed25519_signature_covers_the_whole_body() {
    let area_len = signed().body.extensions.len();
    for offset in [4, unknown_tlvs().len() + 4, area_len - 1] {
        let mut record = signed();
        record.body.extensions[offset] ^= 1;
        assert!(
            matches!(
                record.verify(&context()),
                Err(DiscoveryRecordError::InvalidSignature)
            ),
            "extension byte {offset}"
        );
    }
}

/// The helper that plays the author makes records that verify, so the
/// refusals built on it are the encoding rules' and not a bad signature's.
#[test]
fn a_hand_built_record_by_its_author_verifies() {
    let (key, _) = key_and_signature_tlvs(&signed());
    author_signed([unknown_tlvs(), key.clone()].concat())
        .verify(&context())
        .expect("the canonical area verifies");
    author_signed(key).verify(&context()).unwrap();
}

/// The author itself can't publish a record outside the canonical form:
/// with its own valid signature over the area, a repeated key, a stray
/// signature-typed TLV, and a TLV between the key and the signature are
/// each refused. A reader therefore finds one key and one signature, in one
/// place, in any record it accepts.
#[test]
fn the_authors_valid_signature_does_not_excuse_a_non_canonical_area() {
    let (key, _) = key_and_signature_tlvs(&signed());
    let stray_signature = tlv(RECORD_EXTENSION_ML_DSA_65_SIGNATURE, &[0; 8]);
    let cases = [
        (
            [key.clone(), key.clone()].concat(),
            RecordSignatureError::Duplicate,
        ),
        (
            [stray_signature, key.clone()].concat(),
            RecordSignatureError::Duplicate,
        ),
        (
            [key.clone(), unknown_tlvs()].concat(),
            RecordSignatureError::Misplaced,
        ),
        // No key at all: the signature has nothing to verify under.
        (unknown_tlvs(), RecordSignatureError::MissingKey),
    ];
    for (before_signature, expected) in cases {
        assert_eq!(refusal(&author_signed(before_signature)), expected);
    }
}

#[test]
fn a_duplicated_key_or_signature_tlv_is_refused() {
    let (key, signature) = key_and_signature_tlvs(&signed());
    let twice_key = [unknown_tlvs(), key.clone(), key.clone(), signature.clone()].concat();
    // The room has no space for two signatures, so the body's cap refuses
    // this one first. A short TLV of the signature's type shows the rule.
    let short_signature = tlv(RECORD_EXTENSION_ML_DSA_65_SIGNATURE, &[0; 8]);
    let twice_signature = [unknown_tlvs(), short_signature, key, signature].concat();
    for area in [twice_key, twice_signature] {
        assert_eq!(refusal(&with_area(area)), RecordSignatureError::Duplicate);
    }
}

#[test]
fn key_and_signature_tlvs_out_of_order_are_refused() {
    let (key, signature) = key_and_signature_tlvs(&signed());
    let swapped = [unknown_tlvs(), signature.clone(), key.clone()].concat();
    let unknown_between = [key, unknown_tlvs(), signature].concat();
    for area in [swapped, unknown_between] {
        assert_eq!(refusal(&with_area(area)), RecordSignatureError::Misplaced);
    }
}

#[test]
fn bytes_after_the_signature_tlv_are_refused() {
    let signed_area = signed().body.extensions;
    let unknown_after = [signed_area.clone(), tlv(0x7003, &[1])].concat();
    assert_eq!(
        refusal(&with_area(unknown_after)),
        RecordSignatureError::Misplaced
    );
    // Fewer bytes than a header are no TLV at all.
    for stray in 1..=3 {
        let area = [signed_area.clone(), vec![0; stray]].concat();
        assert_eq!(
            refusal(&with_area(area)),
            RecordSignatureError::Malformed,
            "{stray} stray bytes"
        );
    }
}

#[test]
fn a_key_or_signature_of_the_wrong_length_is_refused() {
    let record = signed();
    let extensions = parse(&record.body.extensions).unwrap();
    let key = |len: usize| tlv(RECORD_EXTENSION_ML_DSA_65_KEY, &extensions.key[..len]);
    let signature = |len: usize| {
        tlv(
            RECORD_EXTENSION_ML_DSA_65_SIGNATURE,
            &extensions.signature[..len],
        )
    };
    let areas = [
        [
            key(ML_DSA_65_KEY_BYTES - 1),
            signature(ML_DSA_65_SIGNATURE_BYTES),
        ],
        [
            key(ML_DSA_65_KEY_BYTES),
            signature(ML_DSA_65_SIGNATURE_BYTES - 1),
        ],
        [key(0), signature(ML_DSA_65_SIGNATURE_BYTES)],
        [key(ML_DSA_65_KEY_BYTES), signature(0)],
    ];
    for area in areas {
        assert_eq!(
            refusal(&with_area(area.concat())),
            RecordSignatureError::Length
        );
    }
    // A longer key is a real ML-DSA-87 key's length.
    let long_key = tlv(RECORD_EXTENSION_ML_DSA_65_KEY, &[0x5a; 2_592]);
    assert_eq!(
        refusal(&with_area(
            [long_key, signature(ML_DSA_65_SIGNATURE_BYTES)].concat()
        )),
        RecordSignatureError::Length
    );
}

/// A length that runs past the area, in a known or an unknown TLV, leaves
/// no whole TLV sequence to read.
#[test]
fn a_tlv_that_runs_past_the_area_is_refused() {
    let signed_area = signed().body.extensions;
    let mut overlong_signature = signed_area.clone();
    let length_at = signed_area.len() - ML_DSA_65_SIGNATURE_BYTES - 2;
    overlong_signature[length_at..length_at + 2].copy_from_slice(&3_310u16.to_le_bytes());
    let mut overlong_unknown = signed_area;
    overlong_unknown[2..4].copy_from_slice(&u16::MAX.to_le_bytes());
    for area in [overlong_signature, overlong_unknown] {
        assert_eq!(refusal(&with_area(area)), RecordSignatureError::Malformed);
    }
}

/// Unknown TLVs are the author's: any type but the two, in any order, more
/// than once, and empty. They are kept and relayed byte for byte, and both
/// signatures cover them.
#[test]
fn unknown_tlvs_are_kept_relayed_and_signed() {
    let unknown = [
        tlv(0xffff, &[9; 40]),
        tlv(0x0000, &[]),
        tlv(0x7001, &[1, 2, 3]),
        tlv(0x7001, &[1, 2, 3]),
        tlv(0x0003, &[7]),
    ]
    .concat();
    let mut body = body(&author());
    body.extensions = unknown.clone();
    let record = ZakuraNodeRecord::sign(body, &author()).unwrap();
    record.verify(&context()).expect("the record verifies");
    assert!(record.body.extensions.starts_with(&unknown));

    let hello = DiscoveryMessage::Hello {
        record: record.clone(),
    };
    let (payload, decoded) = round_trip(&hello);
    assert_eq!(decoded, hello, "the decoded record keeps every byte");
    assert_eq!(
        encode_frame(&decoded).unwrap().payload,
        payload,
        "a relayed record is byte-identical"
    );
    let area_at = payload.len() - 64 - record.body.extensions.len();
    assert_eq!(
        payload[area_at..payload.len() - 64],
        record.body.extensions,
        "the area is on the wire as the record holds it"
    );

    // ML-DSA-65 covers each unknown TLV: with Ed25519 forged, a change to
    // one still fails.
    for offset in [0, 4, unknown.len() - 1] {
        let forged = resigned(record.clone(), &author(), |body| {
            body.extensions[offset] ^= 1
        });
        assert_eq!(
            refusal(&forged),
            RecordSignatureError::Invalid,
            "byte {offset}"
        );
    }
    // And dropping them all, which keeps the area well formed, fails too.
    let (key, signature) = key_and_signature_tlvs(&record);
    let stripped = resigned(record, &author(), |body| {
        body.extensions = [key, signature].concat();
    });
    assert_eq!(refusal(&stripped), RecordSignatureError::Invalid);
}

/// Signing writes the two TLVs itself, so a body may not bring its own, and
/// its other extensions must be whole TLVs: no reader would accept the
/// record otherwise.
#[test]
fn signing_refuses_an_area_no_reader_accepts() {
    let sign = |extensions: Vec<u8>| {
        let mut body = body(&author());
        body.extensions = extensions;
        ZakuraNodeRecord::sign(body, &author())
    };
    let refused = |extensions: Vec<u8>| match sign(extensions) {
        Err(DiscoveryWireError::RecordSignature(error)) => error,
        other => panic!("expected a signing refusal, got {other:?}"),
    };
    assert_eq!(
        refused(tlv(RECORD_EXTENSION_ML_DSA_65_KEY, &[1, 2])),
        RecordSignatureError::Duplicate
    );
    assert_eq!(
        refused(tlv(RECORD_EXTENSION_ML_DSA_65_SIGNATURE, &[])),
        RecordSignatureError::Duplicate
    );
    assert_eq!(
        refused(vec![0x01, 0x70, 0x03]),
        RecordSignatureError::Malformed
    );
    assert_eq!(
        refused(vec![0x01, 0x70, 0x03, 0x00, 0xaa]),
        RecordSignatureError::Malformed
    );
    // A signed area with something after it is neither an earlier record's
    // area nor free of the two types.
    let trailing = [signed().body.extensions, tlv(0x7003, &[1])].concat();
    assert_eq!(refused(trailing), RecordSignatureError::Duplicate);
}

/// A body taken from a signed record signs again: the earlier key and
/// signature are replaced, not repeated.
#[test]
fn signing_a_signed_body_replaces_its_key_and_signature() {
    let first = signed();
    let mut renewed = first.body.clone();
    renewed.sequence += 1;
    let second = ZakuraNodeRecord::sign(renewed, &author()).unwrap();
    second
        .verify(&context())
        .expect("the renewed record verifies");
    assert_eq!(second.body.extensions.len(), first.body.extensions.len());
    assert_eq!(
        parse(&second.body.extensions).unwrap().other,
        unknown_tlvs()
    );
}

/// The signature is hedged: one body signs to different records, each
/// valid. With the randomness fixed, signing is a function of its inputs.
#[test]
fn hedged_signatures_differ_and_fixed_randomness_repeats() {
    let first = ZakuraNodeRecord::sign(body(&author()), &author()).unwrap();
    let second = ZakuraNodeRecord::sign(body(&author()), &author()).unwrap();
    assert_ne!(first, second);
    assert_eq!(first.body.sequence, second.body.sequence);
    for record in [&first, &second] {
        record.verify(&context()).expect("each record verifies");
    }

    let fixed = |randomness| {
        ZakuraNodeRecord::sign_with_randomness(body(&author()), &author(), randomness).unwrap()
    };
    assert_eq!(fixed([3; 32]), fixed([3; 32]));
    assert_ne!(fixed([3; 32]), fixed([4; 32]));
    fixed([3; 32]).verify(&context()).unwrap();
}

/// The ML-DSA-65 key and signature leave 2,627 bytes of the room beside the
/// largest fields, and the caps do not move: the largest record is as long
/// as it was, and one byte more is refused.
#[test]
fn the_largest_record_holds_the_signature_and_fills_the_cap() {
    const { assert!(RECORD_SIGNATURE_EXTENSION_BYTES - RECORD_ML_DSA_65_EXTENSION_BYTES == 2_627) };
    let secret = author();
    let fields_len = {
        let pre_image = body(&secret).encode_for_signature().unwrap();
        pre_image.len() - b"zakura-node-record-v1".len() - 2
    };
    assert!(fields_len < MAX_NODE_RECORD_FIELDS_BYTES);
    let room = MAX_NODE_RECORD_BODY_BYTES - fields_len - RECORD_ML_DSA_65_EXTENSION_BYTES;

    let mut largest = body(&secret);
    largest.extensions = tlv(0x7001, &vec![0x5a; room - 4]);
    let record = ZakuraNodeRecord::sign(largest.clone(), &secret).unwrap();
    assert_eq!(record_len(&record), MAX_NODE_RECORD_BYTES);
    assert_eq!(MAX_NODE_RECORD_BYTES, 9_601);
    record
        .verify(&context())
        .expect("the largest record verifies");

    largest.extensions = tlv(0x7001, &vec![0x5a; room - 3]);
    assert!(matches!(
        ZakuraNodeRecord::sign(largest, &secret),
        Err(DiscoveryWireError::OversizedPayload { actual, max })
            if actual == MAX_NODE_RECORD_BODY_BYTES + 1 && max == MAX_NODE_RECORD_BODY_BYTES
    ));
}

/// The published known answer: fixed inputs and the record they give.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct KnownAnswer {
    seed: String,
    randomness: String,
    context: String,
    ed25519_public_key: String,
    node_id: String,
    sequence: u64,
    expires_at_unix_secs: u64,
    unknown_extension: String,
    record: String,
}

/// `record_kat.json` was made on 2026-11-25 by a throwaway script outside
/// this repository, with none of the Zakura crates. It derived the Ed25519
/// key and signed with Python's `cryptography`, hashed the ML-DSA seed, the
/// key fingerprint and the node ID with `hashlib.blake2b`, made the
/// ML-DSA-65 key and signature with RustCrypto's `ml-dsa` 0.1.2
/// (`sign_internal` over the context's encoding and the message, with the
/// file's randomness), and put the record's bytes together by hand.
///
/// Signing the same fields here gives the same bytes, so the encoding, what
/// each signature covers, the context string and the hedging all match a
/// second implementation. The recorded bytes also decode and verify.
#[test]
fn the_record_known_answer_is_reproduced() {
    let known: KnownAnswer = serde_json::from_str(include_str!("record_kat.json")).unwrap();
    let array = |text: &str| -> [u8; 32] { hex::decode(text).unwrap().try_into().unwrap() };
    let secret = NodeSecretKey::from_bytes(&array(&known.seed));
    assert_eq!(
        hex::encode(secret.public_key().as_bytes()),
        known.ed25519_public_key
    );
    assert_eq!(secret.public().to_string(), known.node_id);
    assert_eq!(known.context, "Zakura node record ML-DSA-65 v1");

    let body = ZakuraNodeRecordBody {
        keys: secret.key_list().clone(),
        direct_addrs: vec![addr(10), "[2606:4700:4700::10]:18233".parse().unwrap()],
        services: vec![ZakuraServiceId::discovery(), ZakuraServiceId::block_sync()],
        // The record names `p2p-v2/5`, whatever this build's version is.
        zakura_protocol_min: 5,
        zakura_protocol_max: 5,
        network_id: ZakuraNetworkId::Regtest,
        chain_id: CHAIN_ID,
        sequence: known.sequence,
        expires_at_unix_secs: known.expires_at_unix_secs,
        extensions: hex::decode(&known.unknown_extension).unwrap(),
    };
    let record =
        ZakuraNodeRecord::sign_with_randomness(body, &secret, array(&known.randomness)).unwrap();
    let hello = encode_frame(&DiscoveryMessage::Hello {
        record: record.clone(),
    })
    .unwrap();
    assert_eq!(hex::encode(&hello.payload), known.record);

    let recorded: DiscoveryMessage = decode_frame(&Frame {
        message_type: hello.message_type,
        flags: 0,
        payload: hex::decode(&known.record).unwrap(),
    })
    .expect("the recorded bytes decode");
    assert_eq!(recorded, DiscoveryMessage::Hello { record });
    let DiscoveryMessage::Hello { record } = recorded else {
        unreachable!("the frame is a hello");
    };
    let context = DiscoveryRecordValidationContext {
        supported_protocol_min: 5,
        supported_protocol_max: 5,
        ..context_at(known.expires_at_unix_secs - 60)
    };
    record
        .verify(&context)
        .expect("the recorded record verifies");
}

/// The sizes the pull request reports, measured on the encoders: a default
/// node's record with and without the ML-DSA-65 key and signature, a full
/// `Peers` answer of such records, and a full book file of them.
///
/// A default node lists its two keys, one IPv4 address and seven services:
/// the six every node advertises and `tx-relay`.
#[test]
fn a_default_nodes_record_grows_by_the_key_and_signature() {
    use super::super::{
        runtime::default_advertised_services,
        wire::{
            encode_persisted_entries, records_within, served_peers_cap, MAX_BOOK_FILE_BYTES,
            MAX_PERSISTED_DISCOVERY_ENTRIES,
        },
    };

    let typical = |index: u8| {
        let secret = NodeSecretKey::from_bytes(&[index; 32]);
        let mut body = body(&secret);
        body.network_id = ZakuraNetworkId::Mainnet;
        body.services = default_advertised_services();
        body.services
            .push(crate::zakura::tx_relay::tx_relay_service_id());
        let before = resigned(
            ZakuraNodeRecord {
                body: body.clone(),
                signature: [0; 64],
            },
            &secret,
            |_| {},
        );
        let after = ZakuraNodeRecord::sign(body, &secret).unwrap();
        (before, after)
    };
    let (before, after) = typical(1);
    assert_eq!(record_len(&before), 361);
    assert_eq!(record_len(&after), 5_630);
    assert_eq!(record_len(&after) - record_len(&before), 5_269);

    // A `Hello` payload is the record. A frame adds an 8-byte header.
    let hello = |record: &ZakuraNodeRecord| {
        encode_frame(&DiscoveryMessage::Hello {
            record: record.clone(),
        })
        .unwrap()
        .payload
        .len()
    };
    assert_eq!((hello(&before), hello(&after)), (361, 5_630));

    // A full `Peers` answer: a count and 32 records.
    let records = |after: bool| -> Vec<ZakuraNodeRecord> {
        (1..=32)
            .map(|index| {
                let (old, new) = typical(index);
                if after {
                    new
                } else {
                    old
                }
            })
            .collect()
    };
    let peers = |records: Vec<ZakuraNodeRecord>| {
        encode_frame(&DiscoveryMessage::Peers { records })
            .unwrap()
            .payload
            .len()
    };
    assert_eq!(peers(records(false)), 2 + 32 * 361);
    assert_eq!(peers(records(true)), 2 + 32 * 5_630);
    assert_eq!(2 + 32 * 5_630, 180_162);

    // A node accepts that answer and does not send it: what it serves stops
    // at 24,098 bytes, which is 4 of these records where it was all 32.
    let served = |records: Vec<ZakuraNodeRecord>| {
        let served = records_within(records, served_peers_cap(32).bytes);
        (served.len(), peers(served))
    };
    assert_eq!(served(records(false)), (32, 11_554));
    assert_eq!(served(records(true)), (4, 22_522));

    // A full book file: 256 entries, each a record and the dial history of
    // a peer this node dialed once at the record's one address.
    let file = |record: &ZakuraNodeRecord| {
        let entry = ZakuraDiscoveryPersistedEntry {
            record: record.clone(),
            source: Some(record.body.node_id()),
            is_static: false,
            last_seen: NOW,
            last_dial_attempt: Some(NOW),
            last_success: Some(NOW),
            confirmed_addrs: record.body.direct_addrs.clone(),
            dialed_addrs: record.body.direct_addrs.clone(),
            failure_count: 0,
        };
        let entries = vec![entry; MAX_PERSISTED_DISCOVERY_ENTRIES];
        encode_persisted_entries(&entries, MAX_PERSISTED_DISCOVERY_ENTRIES)
            .unwrap()
            .len()
    };
    assert_eq!(file(&before), 10 + 256 * (361 + 81));
    assert_eq!(file(&after), 10 + 256 * (5_630 + 81));
    assert_eq!(10 + 256 * (5_630 + 81), 1_462_026);
    // The largest file a node reads is unchanged.
    assert_eq!(MAX_BOOK_FILE_BYTES, 2_585_610);
}

fn handle(secret: NodeSecretKey, host: u8) -> ZakuraDiscoveryHandle {
    let (_connected_tx, connected) = watch::channel(Vec::new());
    ZakuraDiscoveryHandle::new(
        ZakuraDiscoveryLocalConfig {
            secret_key: secret,
            direct_addrs: vec![addr(host)],
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: ZAKURA_PROTOCOL_VERSION_CURRENT,
            zakura_protocol_max: ZAKURA_PROTOCOL_VERSION_CURRENT,
            network_id: ZakuraNetworkId::Regtest,
            chain_id: CHAIN_ID,
            last_authored_sequence: None,
        },
        ZakuraDiscoveryConfig::default(),
        connected,
    )
    .expect("the discovery handle builds")
}

/// A record the author signs at the wall clock, which the handle's import
/// paths validate against, with unknown TLVs.
fn live_record(secret: &NodeSecretKey) -> ZakuraNodeRecord {
    let mut body = body_at(secret, current_unix_secs());
    body.extensions = unknown_tlvs();
    ZakuraNodeRecord::sign(body, secret).expect("the record signs")
}

/// The author's live record without its ML-DSA-65 key and signature, under
/// a valid Ed25519 signature.
fn live_record_without_ml_dsa(secret: &NodeSecretKey) -> ZakuraNodeRecord {
    resigned(live_record(secret), secret, |body| {
        body.extensions = unknown_tlvs()
    })
}

/// One record travels every path a node moves records on, and arrives byte
/// for byte and verified each time: its author's `Hello`, the receiver's
/// book, a `Peers` answer to a third node, that node's book file, and a
/// reload into a fourth.
#[tokio::test]
async fn a_record_passes_hello_the_book_peers_and_the_book_file() {
    let author = author();
    let author_id = author.public();
    let record = live_record(&author);

    // The author's `Hello` to the first node.
    let (_, hello) = round_trip(&DiscoveryMessage::Hello {
        record: record.clone(),
    });
    let DiscoveryMessage::Hello { record: received } = hello else {
        panic!("expected a hello");
    };
    let first = handle(NodeSecretKey::from_bytes(&[0x31; 32]), 31);
    first
        .import_connected_peer_record(received, author_id, Some(addr(10).ip()))
        .await
        .expect("the hello imports");
    assert_eq!(first.record_for(author_id).await, Some(record.clone()));

    // The first node's `Peers` answer to the second.
    let second_secret = NodeSecretKey::from_bytes(&[0x32; 32]);
    let sampled = first
        .sample_peers(second_secret.public(), 32, &[], &[])
        .await;
    assert_eq!(sampled, std::slice::from_ref(&record));
    let (_, peers) = round_trip(&DiscoveryMessage::Peers { records: sampled });
    let DiscoveryMessage::Peers { records: relayed } = peers else {
        panic!("expected a peers message");
    };
    let second = handle(second_secret, 32);
    let outcome = second
        .import_peer_records(relayed, Some(first.local_node_id()))
        .await
        .expect("the relayed record is authentic");
    assert_eq!((outcome.added, outcome.rejected), (1, 0));
    assert_eq!(second.record_for(author_id).await, Some(record.clone()));

    // The second node's book file, reloaded by a third.
    let dir = tempfile::tempdir().unwrap();
    let file = DiscoveryBookFile::new(dir.path().join("book"));
    file.store(second.persisted_entries().await, current_unix_secs())
        .unwrap();
    let loaded = file.load();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].record, record);
    let third = handle(NodeSecretKey::from_bytes(&[0x33; 32]), 33);
    let outcome = third.import_persisted_entries(loaded).await;
    assert_eq!((outcome.added, outcome.rejected), (1, 0));
    assert_eq!(third.record_for(author_id).await, Some(record));
}

/// No import path takes a record on its Ed25519 signature alone, and each
/// treats the missing ML-DSA-65 signature as it treats a bad Ed25519 one:
/// a `Hello` and a `Peers` disconnect their sender, and the book file
/// rejects the entry.
#[tokio::test]
async fn every_import_path_requires_the_ml_dsa_signature() {
    let author = author();
    let author_id = author.public();
    let unsigned = live_record_without_ml_dsa(&author);
    let node = handle(NodeSecretKey::from_bytes(&[0x31; 32]), 31);

    let hello = node
        .import_connected_peer_record(unsigned.clone(), author_id, Some(addr(10).ip()))
        .await
        .expect_err("a hello without the ML-DSA-65 signature fails");
    assert!(matches!(
        hello,
        DiscoveryBookError::Record(DiscoveryRecordError::InvalidMlDsaSignature(
            RecordSignatureError::MissingKey
        ))
    ));
    assert_eq!(hello_verdict(&hello), RecordVerdict::Disconnect);

    // A valid record beside it does not save the batch.
    let valid = live_record(&other_node());
    let peers = node
        .import_peer_records([valid.clone(), unsigned.clone()], None)
        .await;
    assert!(matches!(
        peers,
        Err(DiscoveryRecordError::InvalidMlDsaSignature(
            RecordSignatureError::MissingKey
        ))
    ));
    assert_eq!(node.record_for(valid.body.node_id()).await, None);

    let entry = |record: ZakuraNodeRecord| ZakuraDiscoveryPersistedEntry {
        record,
        source: None,
        is_static: false,
        last_seen: current_unix_secs(),
        last_dial_attempt: None,
        last_success: None,
        confirmed_addrs: Vec::new(),
        dialed_addrs: Vec::new(),
        failure_count: 0,
    };
    let outcome = node
        .import_persisted_entries([entry(unsigned.clone()), entry(valid.clone())])
        .await;
    assert_eq!((outcome.added, outcome.rejected), (1, 1));
    assert_eq!(node.record_for(author_id).await, None);

    assert!(matches!(
        node.import_static_record(unsigned).await,
        Err(DiscoveryBookError::Record(
            DiscoveryRecordError::InvalidMlDsaSignature(RecordSignatureError::MissingKey)
        ))
    ));
}

/// A key list without an ML-DSA-65 entry names a node this release can't
/// verify a record of (zakura-quic ID-11). Its record is dropped, whatever
/// its signature, and its relay is not disconnected.
#[tokio::test]
async fn a_record_without_an_ml_dsa_entry_is_dropped_without_penalty() {
    let author = author();
    let ed25519_only = |signature_ok: bool| {
        let mut body = body_at(&author, current_unix_secs());
        body.keys = NodeKeyList::single(author.public_key());
        let mut signature = author.sign(&body.encode_for_signature().unwrap());
        if !signature_ok {
            signature[0] ^= 1;
        }
        ZakuraNodeRecord { body, signature }
    };
    for signature_ok in [true, false] {
        let record = ed25519_only(signature_ok);
        assert!(matches!(
            record.verify(&context_at(current_unix_secs())),
            Err(DiscoveryRecordError::NoSupportedKey)
        ));
        let node = handle(NodeSecretKey::from_bytes(&[0x31; 32]), 31);
        let valid = live_record(&other_node());
        let outcome = node
            .import_peer_records([record.clone(), valid.clone()], None)
            .await
            .expect("an unverifiable record does not fail its batch");
        assert_eq!((outcome.added, outcome.rejected), (1, 1));
        assert_eq!(node.record_for(record.body.node_id()).await, None);
        assert!(node.record_for(valid.body.node_id()).await.is_some());
    }
}
