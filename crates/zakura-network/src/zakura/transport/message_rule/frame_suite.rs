//! The frame suite: one layout's header checks, derived from its tables.
//!
//! [`check_frame_filter`] runs every reader of every stream in a layout
//! against every row of the layout. For each reader it checks that:
//!
//! - a row the reader admits passes at its minimum and maximum payload length,
//!   and the returned frame cap stops one byte past the row's maximum;
//! - such a row fails one byte below its minimum and with any flag bit set;
//! - a row the reader does not admit fails from its type, including a request
//!   on a request/response stream's requester and a response on its responder;
//! - a sibling stream's rows fail from their type, so each message travels only
//!   on its declared stream;
//! - every undeclared message type fails from its type.
//!
//! A new row or a new layout needs no new test: a reactor calls
//! [`check_frame_filter`] once for each layout it declares.

use std::collections::BTreeSet;

use super::{
    frame_filter::{FrameFilter, FrameRejection, InboundReader},
    MessageRole,
};
use crate::zakura::{Stream, StreamMode, FRAME_HEADER_BYTES};

/// Check every reader of `layout` against every row of `layout`.
///
/// # Panics
///
/// If the layout declares no tables, or a reader accepts or rejects a header
/// that its table says otherwise.
pub(crate) fn check_frame_filter(layout: &[Stream]) {
    Stream::check_layout(layout).expect("the frame suite checks valid layouts");
    for stream in layout {
        let rows = stream
            .messages
            .expect("the frame suite checks declared tables");
        let frame_cap = usize::try_from(stream.frame_cap).expect("u32 fits usize");
        for &reader in readers(stream.mode) {
            let filter = FrameFilter::new(stream.messages, reader);
            let check = |message_type, flags, payload_len| {
                filter.check_header(message_type, flags, payload_len, frame_cap)
            };

            for rule in rows {
                let min = rule.payload.min();
                let max = rule.payload.max();
                if !reads(reader, rule.role) {
                    assert_eq!(
                        check(rule.message_type, 0, min),
                        Err(FrameRejection::UnknownMessageType),
                        "{reader:?} of stream {} admitted type {}",
                        stream.kind,
                        rule.message_type
                    );
                    continue;
                }

                let row_cap = FRAME_HEADER_BYTES + max;
                assert_eq!(check(rule.message_type, 0, min), Ok(row_cap));
                assert_eq!(check(rule.message_type, 0, max), Ok(row_cap));
                // The caller's oversize check rejects one byte more.
                assert!(FRAME_HEADER_BYTES + max + 1 > row_cap);
                if min > 0 {
                    assert_eq!(
                        check(rule.message_type, 0, min - 1),
                        Err(FrameRejection::PayloadTooShort { min })
                    );
                }
                for bit in 0..u16::BITS {
                    assert_eq!(
                        check(rule.message_type, 1 << bit, min),
                        Err(FrameRejection::ReservedFlags),
                        "flag bit {bit} passed on type {}",
                        rule.message_type
                    );
                }
            }

            let own: BTreeSet<u16> = rows.iter().map(|rule| rule.message_type).collect();
            for message_type in (0..=u16::MAX).filter(|message_type| !own.contains(message_type)) {
                assert_eq!(
                    check(message_type, 0, 0),
                    Err(FrameRejection::UnknownMessageType),
                    "stream {} admitted type {message_type}, which another stream carries \
                     or no row declares",
                    stream.kind
                );
            }
        }
    }
}

/// Whether `reader` reads messages of `role`.
///
/// The suite states this independently of the filter, so a filter that admits
/// the wrong role cannot also change the suite's expectation.
fn reads(reader: InboundReader, role: MessageRole) -> bool {
    matches!(
        (reader, role),
        (InboundReader::Persistent, _)
            | (InboundReader::Responder, MessageRole::Request { .. })
            | (InboundReader::Requester, MessageRole::Response { .. })
    )
}

/// The readers of a stream in each mode.
fn readers(mode: StreamMode) -> &'static [InboundReader] {
    match mode {
        StreamMode::Persistent => &[InboundReader::Persistent],
        StreamMode::RequestResponse => &[InboundReader::Responder, InboundReader::Requester],
    }
}
