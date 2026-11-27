//! What signing discovery records with ML-DSA-65 costs a node (zakura-quic
//! ID-7).
//!
//! Each `ed25519_only` row is the work a record took before it carried an
//! ML-DSA-65 key and signature: an Ed25519 signature over a body with no
//! extension. Each `both` row is the work it takes now.
//!
//! - `record/sign/*`: authoring one record.
//! - `record/verify/*`: the author checks of one record, as a node runs
//!   them on a `Hello`, a relayed record and a book file entry.
//! - `record/verify/both_largest`: the same for a record that fills the
//!   body cap, the most a peer can make one verification cost.
//! - `record/verify/ed25519_then_refused`: a record whose Ed25519 signature
//!   is valid and whose ML-DSA-65 signature has a wrong commitment hash, so
//!   it is refused only after the whole ML-DSA-65 check: the costliest
//!   record to refuse.
//! - `peers/decode_and_verify_32/*`: decoding a full `Peers` answer of 32
//!   typical records and verifying each.
//!
//! A typical record is the one a default node authors: its two keys, one
//! IPv4 address and the seven services a default node advertises.
//!
//! The bench uses only the crate's public API.
#![allow(missing_docs)]

use std::{
    hint::black_box,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use zakura_network::zakura::{
    wire_codec::{decode_frame, encode_frame},
    DiscoveryMessage, DiscoveryRecordError, DiscoveryRecordValidationContext, Frame,
    RecordSignatureError, ZakuraNetworkId, ZakuraNodeRecord, ZakuraNodeRecordBody, ZakuraServiceId,
    MAX_DISCOVERY_RECORDS_PER_RESPONSE, MAX_NODE_RECORD_BODY_BYTES,
    RECORD_ML_DSA_65_EXTENSION_BYTES,
};
use zakura_quic::{NodeSecretKey, ML_DSA_65_SIGNATURE_BYTES};

const NOW: u64 = 1_800_000_000;
const CHAIN_ID: [u8; 32] = [7; 32];
const PROTOCOL: u16 = 5;

fn secret(index: usize) -> NodeSecretKey {
    let mut seed = [0x5a; 32];
    seed[..8].copy_from_slice(
        &u64::try_from(index)
            .expect("an index fits u64")
            .to_le_bytes(),
    );
    NodeSecretKey::from_bytes(&seed)
}

fn context() -> DiscoveryRecordValidationContext {
    DiscoveryRecordValidationContext {
        expected_network_id: ZakuraNetworkId::Mainnet,
        expected_chain_id: CHAIN_ID,
        current_unix_secs: NOW,
        supported_protocol_min: PROTOCOL,
        supported_protocol_max: PROTOCOL,
        max_record_ttl: Duration::from_secs(24 * 60 * 60),
        clock_skew_tolerance: Duration::from_secs(300),
    }
}

/// The body a default node authors.
fn typical_body(secret: &NodeSecretKey) -> ZakuraNodeRecordBody {
    let service = |id: &str| ZakuraServiceId::new(id).expect("a built-in service id is valid");
    ZakuraNodeRecordBody {
        keys: secret.key_list().clone(),
        direct_addrs: vec![SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(45, 33, 30, 10)),
            8233,
        )],
        services: vec![
            ZakuraServiceId::discovery(),
            ZakuraServiceId::block_sync(),
            ZakuraServiceId::header_sync(),
            ZakuraServiceId::legacy_gossip(),
            ZakuraServiceId::legacy_requests(),
            ZakuraServiceId::service_discovery(),
            service("tx-relay"),
        ],
        zakura_protocol_min: PROTOCOL,
        zakura_protocol_max: PROTOCOL,
        network_id: ZakuraNetworkId::Mainnet,
        chain_id: CHAIN_ID,
        sequence: 1_800_000_000_000_000_000,
        expires_at_unix_secs: NOW + 24 * 60 * 60,
        extensions: Vec::new(),
    }
}

/// `body` under an Ed25519 signature alone, as a record was before it
/// carried an ML-DSA-65 signature.
fn sign_ed25519_only(body: ZakuraNodeRecordBody, secret: &NodeSecretKey) -> ZakuraNodeRecord {
    let pre_image = body.encode_for_signature().expect("the body is in bounds");
    let signature = secret.sign(&pre_image);
    ZakuraNodeRecord { body, signature }
}

/// The author checks a record took before: the Ed25519 signature over the
/// body's pre-image.
fn verify_ed25519_only(record: &ZakuraNodeRecord) {
    let pre_image = record
        .body
        .encode_for_signature()
        .expect("the body is in bounds");
    record
        .body
        .keys
        .ed25519()
        .expect("the list holds an Ed25519 key")
        .verify_strict(&pre_image, &record.signature)
        .expect("the Ed25519 signature verifies");
}

/// The length of `record` on the wire: its `Hello` payload.
fn record_bytes(record: &ZakuraNodeRecord) -> u64 {
    let frame = encode_frame(&DiscoveryMessage::Hello {
        record: record.clone(),
    })
    .expect("the record encodes");
    u64::try_from(frame.payload.len()).expect("a record is a few kilobytes")
}

fn bench_sign(c: &mut Criterion) {
    let secret = secret(0);
    let mut group = c.benchmark_group("record/sign");
    group.bench_function("ed25519_only", |b| {
        b.iter_batched(
            || typical_body(&secret),
            |body| sign_ed25519_only(body, &secret),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("both", |b| {
        b.iter_batched(
            || typical_body(&secret),
            |body| ZakuraNodeRecord::sign(body, &secret).expect("the record signs"),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_verify(c: &mut Criterion) {
    let secret = secret(0);
    let context = context();
    let before = sign_ed25519_only(typical_body(&secret), &secret);
    let typical = ZakuraNodeRecord::sign(typical_body(&secret), &secret).expect("the record signs");

    // One unknown TLV fills what the fields, key and signature leave.
    let mut largest = typical_body(&secret);
    let fields = largest
        .encode_for_signature()
        .expect("the body is in bounds")
        .len()
        - b"zakura-node-record-v1".len()
        - 2;
    let room = MAX_NODE_RECORD_BODY_BYTES - fields - RECORD_ML_DSA_65_EXTENSION_BYTES - 4;
    largest.extensions = 0x7001u16.to_le_bytes().to_vec();
    largest.extensions.extend_from_slice(
        &u16::try_from(room)
            .expect("the room fits u16")
            .to_le_bytes(),
    );
    largest.extensions.resize(4 + room, 0x5a);
    let largest = ZakuraNodeRecord::sign(largest, &secret).expect("the record signs");

    // A valid Ed25519 signature over a body whose ML-DSA-65 signature has
    // one flipped byte in its commitment hash, the signature's first field.
    // The signature still decodes, so the verifier does all its work before
    // it refuses. A flip in the hint, at the end, is refused while decoding.
    let mut refused = typical.body.clone();
    let signature_at = refused.extensions.len() - ML_DSA_65_SIGNATURE_BYTES;
    refused.extensions[signature_at] ^= 1;
    let refused = sign_ed25519_only(refused, &secret);
    // The refusal must be the signature check's, not an earlier and cheaper
    // one.
    assert!(matches!(
        refused.verify(&context),
        Err(DiscoveryRecordError::InvalidMlDsaSignature(
            RecordSignatureError::Invalid
        ))
    ));

    let mut group = c.benchmark_group("record/verify");
    group.throughput(Throughput::Bytes(record_bytes(&before)));
    group.bench_function("ed25519_only", |b| {
        b.iter(|| verify_ed25519_only(black_box(&before)));
    });
    group.throughput(Throughput::Bytes(record_bytes(&typical)));
    group.bench_function("both", |b| {
        b.iter(|| {
            black_box(&typical)
                .verify(&context)
                .expect("the record verifies")
        });
    });
    group.throughput(Throughput::Bytes(record_bytes(&largest)));
    group.bench_function("both_largest", |b| {
        b.iter(|| {
            black_box(&largest)
                .verify(&context)
                .expect("the record verifies")
        });
    });
    group.throughput(Throughput::Bytes(record_bytes(&refused)));
    group.bench_function("ed25519_then_refused", |b| {
        b.iter(|| {
            black_box(&refused)
                .verify(&context)
                .expect_err("the record is refused")
        });
    });
    group.finish();
}

fn bench_peers(c: &mut Criterion) {
    let context = context();
    let secrets: Vec<_> = (1..=MAX_DISCOVERY_RECORDS_PER_RESPONSE)
        .map(secret)
        .collect();
    let peers = |sign: fn(ZakuraNodeRecordBody, &NodeSecretKey) -> ZakuraNodeRecord| {
        let records = secrets
            .iter()
            .map(|secret| sign(typical_body(secret), secret))
            .collect();
        encode_frame(&DiscoveryMessage::Peers { records }).expect("the answer encodes")
    };
    let decode = |frame: &Frame| -> Vec<ZakuraNodeRecord> {
        match decode_frame(frame).expect("the answer decodes") {
            DiscoveryMessage::Peers { records } => records,
            other => panic!("expected a peers message, got {other:?}"),
        }
    };
    let before = peers(sign_ed25519_only);
    let after =
        peers(|body, secret| ZakuraNodeRecord::sign(body, secret).expect("the record signs"));

    let mut group = c.benchmark_group("peers/decode_and_verify_32");
    let bytes = |frame: &Frame| {
        Throughput::Bytes(u64::try_from(frame.payload.len()).expect("an answer is under 1 MiB"))
    };
    group.throughput(bytes(&before));
    group.bench_function("ed25519_only", |b| {
        b.iter(|| {
            for record in decode(black_box(&before)) {
                verify_ed25519_only(&record);
            }
        });
    });
    group.throughput(bytes(&after));
    group.bench_function("both", |b| {
        b.iter(|| {
            for record in decode(black_box(&after)) {
                record.verify(&context).expect("the record verifies");
            }
        });
    });
    group.bench_function("decode_only", |b| {
        b.iter(|| decode(black_box(&after)));
    });
    group.finish();
}

criterion_group!(benches, bench_sign, bench_verify, bench_peers);
criterion_main!(benches);
