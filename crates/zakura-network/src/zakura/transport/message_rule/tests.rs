//! Layout rules, one test per rule, and the frame filter's legacy behavior.
//!
//! Each test builds the smallest layout that breaks one rule and checks the
//! exact error, so a rule cannot pass by failing another one first.

use std::time::Duration;

use super::{frame_filter::*, layout::LayoutError, *};
use crate::zakura::{check_frame_filter, Stream, StreamMode, FRAME_HEADER_BYTES};

const EVERY_15_SECONDS: Cadence = Cadence {
    capacity: 4,
    refill_interval: Duration::from_secs(15),
};

const fn announcement(message_type: u16) -> MessageRule {
    MessageRule {
        message_type,
        payload: PayloadLen::exact(8),
        role: MessageRole::Announcement {
            cadence: EVERY_15_SECONDS,
        },
    }
}

const fn request(message_type: u16) -> MessageRule {
    MessageRule {
        message_type,
        payload: PayloadLen::exact(8),
        role: MessageRole::Request {
            max_in_flight: 2,
            cadence: None,
        },
    }
}

const fn response(message_type: u16, request: u16, ends_exchange: bool) -> MessageRule {
    MessageRule {
        message_type,
        payload: PayloadLen::between(1, 100),
        role: MessageRole::Response {
            request,
            ends_exchange,
        },
    }
}

const STATUS: MessageRule = announcement(1);
const GET: MessageRule = request(2);
const PART: MessageRule = response(3, 2, false);
const DONE: MessageRule = response(4, 2, true);

const fn persistent(kind: u16, messages: &'static [MessageRule]) -> Stream {
    Stream {
        kind,
        version: 1,
        frame_cap: 1024,
        capability: 1 << 16,
        messages: Some(messages),
        ..Stream::PERSISTENT
    }
}

const fn request_response(kind: u16, messages: &'static [MessageRule]) -> Stream {
    Stream {
        mode: StreamMode::RequestResponse,
        ..persistent(kind, messages)
    }
}

const SINGLE: [Stream; 1] = [persistent(64, &[STATUS, GET, PART, DONE])];
const PAIRED: [Stream; 2] = [
    persistent(64, &[STATUS, PART, DONE]),
    persistent(65, &[GET]),
];
const LOOKUP: [Stream; 1] = [request_response(66, &[GET, PART, DONE])];

const _: () = Stream::validate_layout(&SINGLE);
const _: () = Stream::validate_layout(&PAIRED);
const _: () = Stream::validate_layout(&LOOKUP);

/// A table built at runtime. Streams hold `'static` tables.
fn leak(rows: &[MessageRule]) -> &'static [MessageRule] {
    Box::leak(rows.to_vec().into_boxed_slice())
}

fn check(layout: &[Stream]) -> Result<(), LayoutError> {
    Stream::check_layout(layout)
}

#[test]
fn valid_layouts_pass_and_their_readers_follow_their_rows() {
    for layout in [&SINGLE[..], &PAIRED, &LOOKUP] {
        assert_eq!(check(layout), Ok(()));
        check_frame_filter(layout);
    }
}

#[test]
fn layouts_without_tables_pass() {
    let legacy = Stream {
        messages: None,
        ..persistent(64, &[])
    };
    assert_eq!(check(&[legacy, Stream { kind: 65, ..legacy }]), Ok(()));
}

#[test]
fn a_layout_needs_a_stream() {
    assert_eq!(check(&[]), Err(LayoutError::Empty));
}

#[test]
fn a_request_response_stream_stands_alone() {
    assert_eq!(
        check(&[PAIRED[0], request_response(65, &[GET])]),
        Err(LayoutError::RequestResponseNotAlone { kind: 65 })
    );
}

#[test]
fn a_session_shares_one_capability() {
    let requests = Stream {
        capability: 1 << 17,
        ..PAIRED[1]
    };
    assert_eq!(
        check(&[PAIRED[0], requests]),
        Err(LayoutError::MixedCapabilities { kind: 65 })
    );
}

#[test]
fn every_stream_has_its_own_kind() {
    let requests = Stream {
        kind: 64,
        ..PAIRED[1]
    };
    assert_eq!(
        check(&[PAIRED[0], requests]),
        Err(LayoutError::DuplicateKind { kind: 64 })
    );
}

#[test]
fn every_stream_or_none_declares_a_table() {
    let untabled = Stream {
        messages: None,
        ..PAIRED[1]
    };
    assert_eq!(
        check(&[PAIRED[0], untabled]),
        Err(LayoutError::PartialTables { kind: 65 })
    );
}

#[test]
fn a_table_needs_a_row() {
    assert_eq!(
        check(&[persistent(64, &[])]),
        Err(LayoutError::EmptyTable { kind: 64 })
    );
}

#[test]
fn a_message_type_appears_once_per_layout() {
    let within_a_stream = [persistent(64, leak(&[STATUS, GET, PART, DONE, request(1)]))];
    let across_streams = [
        persistent(64, &[STATUS, PART, DONE]),
        persistent(65, leak(&[GET, request(1)])),
    ];
    for layout in [&within_a_stream[..], &across_streams] {
        assert_eq!(
            check(layout),
            Err(LayoutError::DuplicateMessageType { message_type: 1 })
        );
    }
}

#[test]
fn a_row_fits_its_stream_frame_cap() {
    let largest = 1024 - FRAME_HEADER_BYTES;
    let fitting = MessageRule {
        payload: PayloadLen::exact(largest),
        ..STATUS
    };
    assert_eq!(check(&[persistent(64, leak(&[fitting]))]), Ok(()));

    let one_byte_more = MessageRule {
        payload: PayloadLen::between(0, largest + 1),
        ..STATUS
    };
    assert_eq!(
        check(&[persistent(64, leak(&[one_byte_more]))]),
        Err(LayoutError::PayloadAboveFrameCap {
            kind: 64,
            message_type: 1,
        })
    );
}

#[test]
fn a_request_response_stream_carries_no_announcement() {
    assert_eq!(
        check(&[request_response(66, &[GET, DONE, STATUS])]),
        Err(LayoutError::AnnouncementOnRequestResponse {
            kind: 66,
            message_type: 1,
        })
    );
}

#[test]
fn a_request_allows_an_exchange_in_flight() {
    let closed = MessageRule {
        role: MessageRole::Request {
            max_in_flight: 0,
            cadence: None,
        },
        ..GET
    };
    assert_eq!(
        check(&[persistent(64, leak(&[closed, DONE]))]),
        Err(LayoutError::NoRequestsInFlight { message_type: 2 })
    );
}

#[test]
fn a_cadence_admits_a_message_and_refills() {
    let empty = Cadence {
        capacity: 0,
        ..EVERY_15_SECONDS
    };
    let frozen = Cadence {
        refill_interval: Duration::ZERO,
        ..EVERY_15_SECONDS
    };
    for cadence in [empty, frozen] {
        let announcement = MessageRule {
            role: MessageRole::Announcement { cadence },
            ..STATUS
        };
        let request = MessageRule {
            role: MessageRole::Request {
                max_in_flight: 1,
                cadence: Some(cadence),
            },
            ..GET
        };
        assert_eq!(
            check(&[persistent(64, leak(&[announcement]))]),
            Err(LayoutError::EmptyCadence { message_type: 1 })
        );
        assert_eq!(
            check(&[persistent(64, leak(&[request, DONE]))]),
            Err(LayoutError::EmptyCadence { message_type: 2 })
        );
    }
}

#[test]
fn a_response_answers_a_request_row_of_its_layout() {
    // The request sits on the other stream of the pair, outside this layout.
    let missing = [PAIRED[0]];
    // The named type exists, but it is an announcement.
    let answers_an_announcement = [persistent(
        64,
        leak(&[STATUS, GET, DONE, response(3, 1, true)]),
    )];
    for layout in [&missing[..], &answers_an_announcement] {
        assert_eq!(
            check(layout),
            Err(LayoutError::ResponseWithoutRequest { message_type: 3 })
        );
    }
}

#[test]
fn a_request_has_a_response_that_ends_it() {
    assert_eq!(
        check(&[persistent(64, &[GET, PART])]),
        Err(LayoutError::RequestWithoutEnding { message_type: 2 })
    );
}

#[test]
fn errors_name_the_rule_and_the_row() {
    let error = LayoutError::PayloadAboveFrameCap {
        kind: 64,
        message_type: 1,
    };
    assert_eq!(
        error.to_string(),
        "each row's largest frame fits its stream's cap (stream kind 64, message type 1)"
    );
}

#[test]
fn a_stream_without_a_table_accepts_any_header_up_to_its_cap() {
    for reader in [
        InboundReader::Persistent,
        InboundReader::Responder,
        InboundReader::Requester,
    ] {
        let filter = FrameFilter::new(None, reader);
        for (message_type, flags, payload_len) in [(0, 0, 0), (u16::MAX, u16::MAX, 1 << 20)] {
            assert_eq!(
                filter.check_header(message_type, flags, payload_len, 4096),
                Ok(4096)
            );
        }
    }
}

#[test]
fn a_tighter_negotiated_cap_still_applies() {
    let filter = FrameFilter::new(SINGLE[0].messages, InboundReader::Persistent);
    assert_eq!(filter.check_header(PART.message_type, 0, 1, 50), Ok(50));
}
