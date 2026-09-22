//! Tests for the codec pieces, and a small family that exercises the suite.

use proptest::prelude::*;
use zakura_chain::block;

use super::{
    bounded_vec::compact_size_len,
    conformance::{check_conformance, wire_conformance_tests, WireSample, WireViolation},
    *,
};
use crate::zakura::{Frame, MessageRule, PayloadLen};

const PING: u16 = 1;
const HASHES: u16 = 2;
const MAX_HASHES: usize = 3;
const HASH_LIST: BoundedVec<HashItem> = BoundedVec::new("hashes", 1, MAX_HASHES);

/// A two-message family: a fixed-size ping and a short hash list.
#[derive(Clone, Debug, PartialEq)]
enum ToyMessage {
    Ping(block::Height),
    Hashes(Vec<block::Hash>),
}

impl WireMessage for ToyMessage {
    type Error = WireError;
    const RULES: &'static [MessageRule] = &[
        MessageRule::announcement(PING, PayloadLen::exact(HeightLe::MAX_LEN)),
        MessageRule::request(
            HASHES,
            PayloadLen::between(HASH_LIST.min_len(), HASH_LIST.max_len()),
        ),
    ];

    fn message_type(&self) -> u16 {
        match self {
            Self::Ping(_) => PING,
            Self::Hashes(_) => HASHES,
        }
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        match self {
            Self::Ping(height) => HeightLe::encode(height, out),
            Self::Hashes(hashes) => HASH_LIST.encode(hashes, out),
        }
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        match message_type {
            PING => Ok(Self::Ping(reader.read::<HeightLe>()?)),
            HASHES => Ok(Self::Hashes(HASH_LIST.decode(reader)?)),
            _ => Err(WireError::UnknownMessageType(message_type)),
        }
    }
}

fn hashes(count: usize) -> ToyMessage {
    ToyMessage::Hashes(
        (0..count)
            .map(|index| block::Hash([u8::try_from(index).expect("toy lists are short"); 32]))
            .collect(),
    )
}

impl WireSample for ToyMessage {
    const OPEN_ENDED_ROWS: &'static [u16] = &[];

    fn samples() -> Vec<Self> {
        vec![Self::Ping(block::Height(7)), hashes(1), hashes(MAX_HASHES)]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        prop_oneof![
            (0..=block::Height::MAX.0).prop_map(|height| Self::Ping(block::Height(height))),
            (1..=MAX_HASHES).prop_map(hashes),
        ]
        .boxed()
    }

    fn decode_allocation_bound(message_type: u16, payload_len: usize) -> Option<usize> {
        match message_type {
            PING => Some(0),
            HASHES => Some(HASH_LIST.allocation_bound(payload_len)),
            _ => None,
        }
    }

    fn violations() -> Vec<WireViolation<Self>> {
        vec![
            WireViolation {
                name: "height above the maximum",
                frame: Frame {
                    message_type: PING,
                    flags: 0,
                    payload: u32::MAX.to_le_bytes().to_vec(),
                },
                rejected_by: |error| matches!(error, WireError::OutOfRange("block height")),
            },
            WireViolation {
                name: "empty list",
                frame: Frame {
                    message_type: HASHES,
                    flags: 0,
                    payload: vec![0; HASH_LIST.min_len()],
                },
                rejected_by: |error| matches!(error, WireError::CountOutOfRange { count: 0, .. }),
            },
            WireViolation {
                name: "count needs more bytes than remain",
                frame: Frame {
                    message_type: HASHES,
                    flags: 0,
                    payload: [&[2][..], &[0; 40]].concat(),
                },
                rejected_by: |error| {
                    matches!(error, WireError::CountExceedsPayload { count: 2, .. })
                },
            },
        ]
    }
}

wire_conformance_tests!(toy_message_conformance, ToyMessage);

/// A family whose rule claims one more byte than any real encoding.
#[derive(Clone, Debug, PartialEq)]
struct LooseBoundMessage(ToyMessage);

impl WireMessage for LooseBoundMessage {
    type Error = WireError;
    const RULES: &'static [MessageRule] = &[
        MessageRule::announcement(PING, PayloadLen::between(4, 5)),
        MessageRule::request(
            HASHES,
            PayloadLen::between(HASH_LIST.min_len(), HASH_LIST.max_len()),
        ),
    ];

    fn message_type(&self) -> u16 {
        self.0.message_type()
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        self.0.encode_payload(out)
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        ToyMessage::decode_payload(message_type, reader).map(Self)
    }
}

impl WireSample for LooseBoundMessage {
    const OPEN_ENDED_ROWS: &'static [u16] = &[];

    fn samples() -> Vec<Self> {
        ToyMessage::samples().into_iter().map(Self).collect()
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        ToyMessage::arbitrary_valid().prop_map(Self).boxed()
    }

    fn violations() -> Vec<WireViolation<Self>> {
        Vec::new()
    }

    fn decode_allocation_bound(message_type: u16, payload_len: usize) -> Option<usize> {
        ToyMessage::decode_allocation_bound(message_type, payload_len)
    }
}

#[test]
#[should_panic(expected = "samples must reach its declared bounds")]
fn conformance_rejects_a_rule_that_no_encoding_reaches() {
    check_conformance::<LooseBoundMessage>();
}

#[test]
fn check_count_rejects_counts_before_allocation() {
    let payload = [0u8; 10];
    let reader = BoundedReader::new(&payload);
    assert_eq!(reader.check_count("list", 2, 0, 5, 5).ok(), Some(2));
    assert!(matches!(
        reader.check_count("list", 3, 0, 5, 5),
        Err(WireError::CountExceedsPayload {
            count: 3,
            remaining: 10,
            ..
        })
    ));
    assert!(matches!(
        reader.check_count("list", 6, 0, 5, 0),
        Err(WireError::CountOutOfRange { count: 6, .. })
    ));
    assert!(matches!(
        reader.check_count("list", u64::MAX, 0, usize::MAX, 2),
        Err(WireError::CountExceedsPayload { .. })
    ));
}

#[test]
fn compact_size_lengths_match_encodings() {
    use zakura_chain::serialization::{CompactSize64, ZcashSerialize};

    for count in [0, 0xfc, 0xfd, 0xffff, 0x1_0000, 0xffff_ffff, 0x1_0000_0000] {
        let encoded = CompactSize64::from(u64::try_from(count).expect("usize fits u64"))
            .zcash_serialize_to_vec()
            .expect("CompactSize encodes");
        assert_eq!(compact_size_len(count), encoded.len(), "count {count}");
    }
}

#[test]
fn a_hostile_count_prefix_allocates_nothing() {
    // A 3-byte CompactSize claims 25,000 items with no item bytes behind it.
    let frame = Frame {
        message_type: HASHES,
        flags: 0,
        payload: vec![0xfd, 0xa8, 0x61],
    };
    let (result, allocated) =
        allocation_meter::measure_allocated_bytes(|| decode_frame::<ToyMessage>(&frame));
    assert!(result.is_err());
    assert!(allocated <= conformance::ALLOCATION_SLACK_BYTES);
}
