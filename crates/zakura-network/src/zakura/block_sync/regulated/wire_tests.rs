//! Exercise both the bounded envelope and the full production block decoder.
//!
//! The generated message and frame suites run on the stream-6 envelope.

use proptest::prelude::*;
use zakura_chain::block;

use super::wire::{decode_control, Message, Range, RangeEnding};
use crate::zakura::{
    block_sync::{
        config::MAX_BS_INFLIGHT_REQUESTS, service::block_sync_streams, BlockSyncMessage,
        BlockSyncStatus, MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_RESPONSE_BYTES, MSG_BS_BLOCK,
        MSG_BS_BLOCKS_DONE, MSG_BS_GET_BLOCKS, MSG_BS_RANGE_UNAVAILABLE, MSG_BS_STATUS,
    },
    check_frame_filter,
    wire_codec::{
        decode_frame, encode_frame,
        message_suite::{
            check_layout_carries_family, decode_checked, message_suite, MessageSample, Violation,
        },
        sample::WireSample,
        HeightLe, WireError,
    },
    Frame,
};

/// A status with every field at its lowest or highest valid value.
fn status(height: block::Height, hash_byte: u8, limit: impl Fn(u32) -> u32) -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: height,
        servable_high: height,
        tip_hash: block::Hash([hash_byte; 32]),
        max_blocks_per_response: limit(MAX_BS_BLOCKS_PER_REQUEST),
        max_inflight_requests: limit(MAX_BS_INFLIGHT_REQUESTS),
        max_response_bytes: limit(MAX_BS_RESPONSE_BYTES),
    }
}

/// A frame of `message_type` whose payload may break a domain rule.
fn frame(message_type: u8, payload: Vec<u8>) -> Frame {
    Frame {
        message_type: message_type.into(),
        flags: 0,
        payload,
    }
}

impl MessageSample for Message {
    fn samples() -> Vec<Self> {
        let max = block::Height::MAX;
        let last_start = block::Height(max.0 - (MAX_BS_BLOCKS_PER_REQUEST - 1));
        let largest_block = usize::try_from(block::MAX_BLOCK_BYTES).unwrap();
        vec![
            Self::Status(status(block::Height(0), 0, |_| 1)),
            Self::Status(status(max, u8::MAX, |max| max)),
            Self::GetBlocks(Range::new(block::Height(0), 1).unwrap()),
            Self::GetBlocks(Range::new(last_start, MAX_BS_BLOCKS_PER_REQUEST).unwrap()),
            Self::Block(vec![0]),
            Self::Block(vec![u8::MAX; largest_block]),
            Self::End(RangeEnding::done(block::Height(0), 1)),
            Self::End(RangeEnding::done(max, MAX_BS_BLOCKS_PER_REQUEST)),
            Self::End(RangeEnding::unavailable(block::Height(0), 1)),
            Self::End(RangeEnding::unavailable(max, MAX_BS_BLOCKS_PER_REQUEST)),
        ]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        let height = HeightLe::arbitrary;
        let count = 1..=MAX_BS_BLOCKS_PER_REQUEST;
        prop_oneof![
            (
                height(),
                height(),
                any::<[u8; 32]>(),
                count.clone(),
                1..=MAX_BS_INFLIGHT_REQUESTS,
                1..=MAX_BS_RESPONSE_BYTES,
            )
                .prop_map(|(a, b, hash, blocks, inflight, bytes)| {
                    Self::Status(BlockSyncStatus {
                        servable_low: a.min(b),
                        servable_high: a.max(b),
                        tip_hash: block::Hash(hash),
                        max_blocks_per_response: blocks,
                        max_inflight_requests: inflight,
                        max_response_bytes: bytes,
                    })
                }),
            (height(), count.clone()).prop_filter_map(
                "the range ends above the maximum height",
                |(start, count)| Range::new(start, count).ok().map(Self::GetBlocks)
            ),
            proptest::collection::vec(any::<u8>(), 1..256).prop_map(Self::Block),
            (height(), count.clone())
                .prop_map(|(start, count)| Self::End(RangeEnding::done(start, count))),
            (height(), count)
                .prop_map(|(start, count)| Self::End(RangeEnding::unavailable(start, count))),
        ]
        .boxed()
    }

    fn violations() -> Vec<Violation<Self>> {
        let max_height = block::Height::MAX.0;
        let too_many = MAX_BS_BLOCKS_PER_REQUEST + 1;
        vec![
            Violation {
                name: "status range runs backwards",
                frame: frame(MSG_BS_STATUS, status_payload(2, 1, 1, 1, 1)),
                rejected_by: |error| *error == WireError::OutOfRange("block-sync status"),
            },
            Violation {
                name: "status serves no blocks per range",
                frame: frame(MSG_BS_STATUS, status_payload(0, 0, 0, 1, 1)),
                rejected_by: |error| *error == WireError::OutOfRange("block count"),
            },
            Violation {
                name: "status allows no request in flight",
                frame: frame(MSG_BS_STATUS, status_payload(0, 0, 1, 0, 1)),
                rejected_by: |error| *error == WireError::OutOfRange("block-sync status"),
            },
            Violation {
                name: "status targets no response bytes",
                frame: frame(MSG_BS_STATUS, status_payload(0, 0, 1, 1, 0)),
                rejected_by: |error| *error == WireError::OutOfRange("block-sync status"),
            },
            Violation {
                name: "request for no blocks",
                frame: frame(MSG_BS_GET_BLOCKS, range_payload(MSG_BS_GET_BLOCKS, 0, 0)),
                rejected_by: |error| *error == WireError::OutOfRange("GetBlocks range"),
            },
            Violation {
                name: "request for too many blocks",
                frame: frame(
                    MSG_BS_GET_BLOCKS,
                    range_payload(MSG_BS_GET_BLOCKS, 0, too_many),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("GetBlocks range"),
            },
            Violation {
                name: "request ends above the maximum height",
                frame: frame(
                    MSG_BS_GET_BLOCKS,
                    range_payload(MSG_BS_GET_BLOCKS, max_height, 2),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("GetBlocks range"),
            },
            Violation {
                name: "done after no blocks",
                frame: frame(MSG_BS_BLOCKS_DONE, range_payload(MSG_BS_BLOCKS_DONE, 0, 0)),
                rejected_by: |error| *error == WireError::OutOfRange("block count"),
            },
            Violation {
                name: "unavailable range of too many blocks",
                frame: frame(
                    MSG_BS_RANGE_UNAVAILABLE,
                    range_payload(MSG_BS_RANGE_UNAVAILABLE, 0, too_many),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("block count"),
            },
            Violation {
                name: "payload discriminator names another ending",
                frame: frame(
                    MSG_BS_BLOCKS_DONE,
                    range_payload(MSG_BS_RANGE_UNAVAILABLE, 0, 1),
                ),
                rejected_by: |error| {
                    *error == WireError::OutOfRange("frame and payload discriminator agreement")
                },
            },
            Violation {
                name: "block payload names a request",
                frame: frame(MSG_BS_BLOCK, vec![MSG_BS_GET_BLOCKS, 0]),
                rejected_by: |error| {
                    *error == WireError::OutOfRange("frame and payload discriminator agreement")
                },
            },
        ]
    }

    fn opaque_types() -> &'static [u16] {
        // Widening a u8 discriminator to u16 is lossless.
        &[MSG_BS_BLOCK as u16]
    }
}

message_suite!(stream_six_family, Message);

#[test]
fn the_stream_carries_exactly_the_family_rows() {
    check_layout_carries_family::<Message>(block_sync_streams());
}

#[test]
fn readers_follow_the_stream() {
    check_frame_filter(block_sync_streams());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn stream_six_messages_round_trip_and_reject_trailing_bytes(
        kind in 1u16..=5,
        start in 0u32..1_000_000,
        count in 1u32..=128,
        extra in proptest::collection::vec(any::<u8>(), 1..32),
    ) {
        let range = Range::new(block::Height(start), count).unwrap();
        let message = match kind {
            1 => Message::Status(BlockSyncStatus {
                servable_low: range.start(),
                servable_high: block::Height(start + count),
                ..Default::default()
            }),
            2 => Message::GetBlocks(range),
            3 => Message::Block(zakura_test::vectors::BLOCK_MAINNET_1_BYTES.to_vec()),
            4 => Message::End(RangeEnding::done(range.start(), count)),
            _ => Message::End(RangeEnding::unavailable(range.start(), range.count())),
        };
        let frame = encode_frame(&message).unwrap();
        prop_assert_eq!(decode_frame::<Message>(&frame).unwrap(), message);
        let decoded = BlockSyncMessage::decode_frame(frame.clone()).unwrap();
        prop_assert_eq!(decoded.encode_frame().unwrap(), frame.clone());
        let mut trailing = frame;
        trailing.payload.extend(extra);
        // The envelope keeps `Block` bytes opaque, so only the full decoder rejects that tail.
        prop_assert_eq!(decode_frame::<Message>(&trailing).is_ok(), kind == 3);
        prop_assert!(BlockSyncMessage::decode_frame(trailing).is_err());
    }

    #[test]
    fn hostile_stream_six_payloads_do_not_panic_or_exceed_envelope_allocations(
        kind in 1u16..=5,
        body in proptest::collection::vec(any::<u8>(), 0..4_096),
    ) {
        let mut payload = vec![u8::try_from(kind).unwrap()];
        payload.extend(body);
        let frame = crate::zakura::Frame { message_type: kind, flags: 0, payload: payload.clone() };
        if let Ok(message) = decode_checked::<Message>(&frame) {
            prop_assert_eq!(encode_frame(&message).unwrap().payload, payload.clone());
        }
        // This also exercises nested block decoding when the discriminator is 3.
        let _ = BlockSyncMessage::decode(&payload);
    }
}

/// A Status payload with the given limits, laid out by hand.
fn status_payload(low: u32, high: u32, blocks: u32, inflight: u32, bytes: u32) -> Vec<u8> {
    let mut payload = vec![1];
    payload.extend_from_slice(&low.to_le_bytes());
    payload.extend_from_slice(&high.to_le_bytes());
    payload.extend_from_slice(&[7; 32]);
    for limit in [blocks, inflight, bytes] {
        payload.extend_from_slice(&limit.to_le_bytes());
    }
    payload
}

/// A GetBlocks, BlocksDone, or RangeUnavailable payload, laid out by hand.
fn range_payload(tag: u8, start: u32, count: u32) -> Vec<u8> {
    let mut payload = vec![tag];
    payload.extend_from_slice(&start.to_le_bytes());
    payload.extend_from_slice(&count.to_le_bytes());
    payload
}

/// A Block payload of `len` bytes. The envelope keeps the body opaque.
fn block_payload(len: usize) -> Vec<u8> {
    let mut payload = vec![0; len];
    payload[0] = 3;
    payload
}

/// Hand-written byte fixtures pin every stream-6 kind and rule independently of
/// the encoder. Legal boundaries decode and re-encode to the same bytes. Each
/// malformed fixture breaks exactly one rule and is rejected. `decode_checked`
/// also bounds every decode's heap by `Message::max_heap_bytes`.
#[test]
fn stream_six_fixed_fixtures_cover_every_kind_and_rule() {
    const MAX_BLOCKS: u32 = 128;
    const MAX_INFLIGHT: u32 = 32_768;
    const MAX_BYTES: u32 = 33_554_432;
    const MAX_BLOCK_PAYLOAD: usize = 2_000_001;
    let max_height = block::Height::MAX.0;
    // The fixtures hardcode the protocol limits, so a constant change fails here.
    assert_eq!(
        crate::zakura::block_sync::MAX_BS_BLOCKS_PER_REQUEST,
        MAX_BLOCKS
    );
    assert_eq!(
        crate::zakura::block_sync::config::MAX_BS_INFLIGHT_REQUESTS,
        MAX_INFLIGHT
    );
    assert_eq!(crate::zakura::block_sync::MAX_BS_RESPONSE_BYTES, MAX_BYTES);

    let legal: Vec<(&str, u16, Vec<u8>)> = vec![
        ("status limits at 1", 1, status_payload(0, 0, 1, 1, 1)),
        (
            "status blocks at max",
            1,
            status_payload(1, max_height, MAX_BLOCKS, 1, 1),
        ),
        (
            "status inflight at max",
            1,
            status_payload(1, 1, 1, MAX_INFLIGHT, 1),
        ),
        (
            "status bytes at max",
            1,
            status_payload(1, 1, 1, 1, MAX_BYTES),
        ),
        ("get blocks count 1", 2, range_payload(2, 0, 1)),
        ("get blocks count 128", 2, range_payload(2, 0, MAX_BLOCKS)),
        (
            "get blocks at the last height",
            2,
            range_payload(2, max_height, 1),
        ),
        ("smallest block", 3, block_payload(2)),
        ("largest block", 3, block_payload(MAX_BLOCK_PAYLOAD)),
        ("blocks done count 1", 4, range_payload(4, 0, 1)),
        ("blocks done count 128", 4, range_payload(4, 0, MAX_BLOCKS)),
        ("unavailable count 1", 5, range_payload(5, 0, 1)),
        ("unavailable count 128", 5, range_payload(5, 0, MAX_BLOCKS)),
    ];
    let mut malformed: Vec<(String, u16, Vec<u8>)> = vec![
        ("status blocks 0".into(), 1, status_payload(0, 0, 0, 1, 1)),
        (
            "status blocks max+1".into(),
            1,
            status_payload(0, 0, MAX_BLOCKS + 1, 1, 1),
        ),
        ("status inflight 0".into(), 1, status_payload(0, 0, 1, 0, 1)),
        (
            "status inflight max+1".into(),
            1,
            status_payload(0, 0, 1, MAX_INFLIGHT + 1, 1),
        ),
        ("status bytes 0".into(), 1, status_payload(0, 0, 1, 1, 0)),
        (
            "status bytes max+1".into(),
            1,
            status_payload(0, 0, 1, 1, MAX_BYTES + 1),
        ),
        (
            "status low above high".into(),
            1,
            status_payload(2, 1, 1, 1, 1),
        ),
        (
            "get blocks past the last height".into(),
            2,
            range_payload(2, max_height, 2),
        ),
        ("block below its minimum".into(), 3, block_payload(1)),
        (
            "block above its maximum".into(),
            3,
            block_payload(MAX_BLOCK_PAYLOAD + 1),
        ),
    ];
    for tag in [2_u8, 4, 5] {
        for count in [0, MAX_BLOCKS + 1] {
            malformed.push((
                format!("tag {tag} count {count}"),
                tag.into(),
                range_payload(tag, 1, count),
            ));
        }
    }
    for (name, tag, payload) in &legal {
        let wrong_kind = tag % 5 + 1;
        let mut mismatched = payload.clone();
        mismatched[0] = u8::try_from(wrong_kind).unwrap();
        malformed.push((
            format!("{name}: discriminator {wrong_kind}"),
            *tag,
            mismatched,
        ));
        // Block bodies are opaque, so only the length bounds above apply to them.
        if *tag != 3 {
            let mut trailing = payload.clone();
            trailing.push(0);
            malformed.push((format!("{name}: one trailing byte"), *tag, trailing));
            let mut truncated = payload.clone();
            truncated.pop();
            malformed.push((format!("{name}: one byte short"), *tag, truncated));
        }
    }

    let frame = |message_type, payload: &Vec<u8>| crate::zakura::Frame {
        message_type,
        flags: 0,
        payload: payload.clone(),
    };
    for (name, tag, payload) in &legal {
        let message = decode_checked::<Message>(&frame(*tag, payload))
            .unwrap_or_else(|error| panic!("{name} is legal: {error}"));
        assert_eq!(
            &encode_frame(&message).unwrap().payload,
            payload,
            "{name} re-encodes to its fixture"
        );
    }
    for (name, tag, payload) in &malformed {
        assert!(
            decode_checked::<Message>(&frame(*tag, payload)).is_err(),
            "{name} is malformed"
        );
    }

    // Production decodes controls through `decode_control`, and the public
    // `BlockSyncMessage` decoder shares it. Both agree with the fixtures.
    // A discriminator mismatch is an envelope fault, so those cases stay above.
    let controls = |cases: &[(String, u16, Vec<u8>)]| {
        cases
            .iter()
            .filter(|(_, tag, payload)| {
                *tag != 3 && payload.first() == u8::try_from(*tag).ok().as_ref()
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    let legal: Vec<_> = legal
        .into_iter()
        .map(|(name, tag, payload)| (name.to_string(), tag, payload))
        .collect();
    assert_eq!(controls(&legal).len(), 11);
    assert!(controls(&malformed).len() > 20);
    for (name, _, payload) in controls(&legal) {
        assert!(
            decode_control(&payload).is_ok(),
            "{name} is a legal control"
        );
        assert!(
            BlockSyncMessage::decode(&payload).is_ok(),
            "{name} is legal through the public decoder"
        );
    }
    for (name, _, payload) in controls(&malformed) {
        assert!(
            decode_control(&payload).is_err(),
            "{name} is a malformed control"
        );
        assert!(
            BlockSyncMessage::decode(&payload).is_err(),
            "{name} is malformed through the public decoder"
        );
    }
}
