//! The example reactor's tests: its test data, then the generated suites.
//!
//! The only hand-written parts are the samples, the value strategy, the
//! domain violations, and the conformance adapter. Every other check comes
//! from a suite.

use proptest::prelude::*;

use super::*;
use crate::zakura::{
    check_frame_filter,
    testkit::stream_conformance::{stream_conformance_suite, StreamConformance},
    wire_codec::{
        message_suite::{check_layout_carries_family, message_suite, MessageSample, Violation},
        sample::WireSample,
    },
    Frame,
};

fn range(start: Height, count: u32) -> ItemRange {
    ItemRange { start, count }
}

fn frame(message_type: u16, payload: Vec<u8>) -> Frame {
    Frame {
        message_type,
        flags: 0,
        payload,
    }
}

/// The payload of a range row, which may break the range's domain rules.
fn range_payload(start: u32, count: u32) -> Vec<u8> {
    [start.to_le_bytes(), count.to_le_bytes()].concat()
}

impl MessageSample for ExampleMessage {
    fn samples() -> Vec<Self> {
        let last_start = Height(Height::MAX.0 - (MAX_ITEMS_PER_REQUEST - 1));
        vec![
            Self::Status {
                low: Height(0),
                high: Height(0),
            },
            Self::Status {
                low: Height(7),
                high: Height::MAX,
            },
            Self::GetItems(range(Height(0), 1)),
            Self::GetItems(range(last_start, MAX_ITEMS_PER_REQUEST)),
            Self::Item {
                height: Height(0),
                bytes: vec![0],
            },
            Self::Item {
                height: Height::MAX,
                bytes: vec![u8::MAX; MAX_ITEM_BYTES],
            },
            Self::ItemsDone {
                start: Height(0),
                returned: 1,
            },
            Self::ItemsDone {
                start: Height::MAX,
                returned: MAX_ITEMS_PER_REQUEST,
            },
            Self::RangeUnavailable(range(Height::MAX, 1)),
        ]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        let height = HeightLe::arbitrary;
        let ranges = (height(), 1..=MAX_ITEMS_PER_REQUEST).prop_filter_map(
            "the range ends above the maximum height",
            |(start, count)| {
                let range = range(start, count);
                ExampleMessage::GetItems(range).check().ok().map(|()| range)
            },
        );
        prop_oneof![
            (height(), height()).prop_map(|(a, b)| Self::Status {
                low: a.min(b),
                high: a.max(b),
            }),
            ranges.clone().prop_map(Self::GetItems),
            (height(), ItemBytes::arbitrary())
                .prop_map(|(height, bytes)| Self::Item { height, bytes }),
            (height(), 1..=MAX_ITEMS_PER_REQUEST)
                .prop_map(|(start, returned)| Self::ItemsDone { start, returned }),
            ranges.prop_map(Self::RangeUnavailable),
        ]
        .boxed()
    }

    fn violations() -> Vec<Violation<Self>> {
        let above_last_height = Height::MAX.0 - (MAX_ITEMS_PER_REQUEST - 1) + 1;
        vec![
            Violation {
                name: "status range runs backwards",
                frame: frame(message_type::STATUS, range_payload(8, 7)),
                rejected_by: |error| *error == WireError::OutOfRange("servable range"),
            },
            Violation {
                name: "request for no items",
                frame: frame(message_type::GET_ITEMS, range_payload(0, 0)),
                rejected_by: |error| *error == WireError::OutOfRange("item range"),
            },
            Violation {
                name: "request for too many items",
                frame: frame(
                    message_type::GET_ITEMS,
                    range_payload(0, MAX_ITEMS_PER_REQUEST + 1),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("item range"),
            },
            Violation {
                name: "request ends above the maximum height",
                frame: frame(
                    message_type::GET_ITEMS,
                    range_payload(above_last_height, MAX_ITEMS_PER_REQUEST),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("item range"),
            },
            Violation {
                name: "unavailable range for no items",
                frame: frame(message_type::RANGE_UNAVAILABLE, range_payload(0, 0)),
                rejected_by: |error| *error == WireError::OutOfRange("item range"),
            },
            Violation {
                name: "done after no items",
                frame: frame(message_type::ITEMS_DONE, range_payload(0, 0)),
                rejected_by: |error| *error == WireError::OutOfRange("returned count"),
            },
            Violation {
                name: "done after too many items",
                frame: frame(
                    message_type::ITEMS_DONE,
                    range_payload(0, MAX_ITEMS_PER_REQUEST + 1),
                ),
                rejected_by: |error| *error == WireError::OutOfRange("returned count"),
            },
            Violation {
                // A zero count, padded to the row's minimum length.
                name: "item with no bytes",
                frame: frame(message_type::ITEM, [&[0; 4][..], &[0, 0xaa]].concat()),
                rejected_by: |error| matches!(error, WireError::CountOutOfRange { count: 0, .. }),
            },
        ]
    }
}

message_suite!(message_family, ExampleMessage);

#[test]
fn both_layouts_carry_exactly_the_family_rows() {
    check_layout_carries_family::<ExampleMessage>(&SINGLE);
    check_layout_carries_family::<ExampleMessage>(&PAIRED);
}

#[test]
fn readers_follow_both_layouts() {
    check_frame_filter(&SINGLE);
    check_frame_filter(&PAIRED);
}

/// The conformance suite's view of the example: exchange `e` asks for the
/// one item at height `e`.
#[derive(Debug)]
struct ExampleConformance;

impl StreamConformance for ExampleConformance {
    type Message = ExampleMessage;

    fn message(row: &MessageRule, exchange: u32) -> ExampleMessage {
        let height = Height(exchange);
        match row.message_type {
            message_type::STATUS => ExampleMessage::Status {
                low: height,
                high: height,
            },
            message_type::GET_ITEMS => ExampleMessage::GetItems(range(height, 1)),
            message_type::ITEM => ExampleMessage::Item {
                height,
                bytes: super::exchange::item_bytes(height),
            },
            message_type::ITEMS_DONE => ExampleMessage::ItemsDone {
                start: height,
                returned: 1,
            },
            message_type::RANGE_UNAVAILABLE => ExampleMessage::RangeUnavailable(range(height, 1)),
            other => unreachable!("the example family has no row {other}"),
        }
    }

    fn exchange(message: &ExampleMessage) -> u32 {
        match message {
            ExampleMessage::Status { low, .. } => low.0,
            ExampleMessage::GetItems(range) | ExampleMessage::RangeUnavailable(range) => {
                range.start.0
            }
            ExampleMessage::Item { height, .. } => height.0,
            ExampleMessage::ItemsDone { start, .. } => start.0,
        }
    }
}

stream_conformance_suite!(single_stream_conformance, SINGLE, ExampleConformance);
stream_conformance_suite!(paired_stream_conformance, PAIRED, ExampleConformance);
