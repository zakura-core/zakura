//! Frame suite: every native rule table, checked from the header alone.
//!
//! Adding a rule row needs no new test. Adding a native stream with a table
//! needs one entry in [`native_rule_tables`].

use std::collections::BTreeSet;

use super::*;
use crate::zakura::{
    block_sync_message_rules, block_sync_streams, legacy_gossip_streams, legacy_message_rules,
    Stream,
};

const READERS: [InboundReader; 3] = [
    InboundReader::Persistent,
    InboundReader::RequestStream,
    InboundReader::ResponseStream,
];

/// Rows whose declared maximum exceeds the stream's frame cap, as
/// `(stream kind, message type)`. The stream cap wins, so an honest sender
/// cannot send these messages at their protocol maximum. This is a known
/// finding; the list keeps it visible.
const ROWS_ABOVE_STREAM_CAP: [(u16, u16); 2] = [(2, 2), (3, 4)];

/// Every native stream whose service declares message rules, with its table.
fn native_rule_tables() -> Vec<(Stream, &'static [MessageRule])> {
    let block_sync = block_sync_streams()
        .iter()
        .map(|stream| (*stream, block_sync_message_rules(*stream)));
    let legacy = legacy_gossip_streams()
        .iter()
        .map(|stream| (*stream, legacy_message_rules(*stream)));
    block_sync
        .chain(legacy)
        .map(|(stream, rules)| {
            let rules = rules
                .unwrap_or_else(|| panic!("stream kind {} declares no message rules", stream.kind));
            (stream, rules)
        })
        .collect()
}

fn stream_cap(stream: Stream) -> usize {
    usize::try_from(stream.frame_cap).expect("u32 frame caps fit usize on supported targets")
}

#[test]
fn tables_have_unique_types_and_ordered_bounds() {
    for (stream, rules) in native_rule_tables() {
        let mut seen = BTreeSet::new();
        for rule in rules {
            assert!(
                seen.insert(rule.message_type),
                "stream {} declares type {} twice",
                stream.kind,
                rule.message_type
            );
            assert!(rule.payload.min <= rule.payload.max);
            assert!(
                rule.payload.min.saturating_add(FRAME_HEADER_BYTES) <= stream_cap(stream),
                "stream {} type {} can never fit its minimum",
                stream.kind,
                rule.message_type
            );
        }
    }
}

#[test]
fn rows_above_the_stream_cap_are_pinned() {
    let above: Vec<_> = native_rule_tables()
        .into_iter()
        .flat_map(|(stream, rules)| {
            rules
                .iter()
                .filter(move |rule| {
                    !rule.payload.is_open_ended()
                        && rule.payload.max.saturating_add(FRAME_HEADER_BYTES) > stream_cap(stream)
                })
                .map(move |rule| (stream.kind, rule.message_type))
        })
        .collect();
    assert_eq!(above, ROWS_ABOVE_STREAM_CAP);
}

#[test]
fn readers_accept_rows_within_bounds_and_reject_the_rest_from_the_header() {
    for (stream, rules) in native_rule_tables() {
        let cap = stream_cap(stream);
        for reader in READERS {
            let filter = FrameFilter::new(Some(rules), reader);
            for rule in rules {
                let check = |flags, len| filter.check_header(rule.message_type, flags, len, cap);
                if !reader.admits(rule.role) {
                    assert_eq!(
                        check(0, rule.payload.min),
                        Err(FrameRejection::UnknownMessageType),
                        "{reader:?} admitted a {:?} row",
                        rule.role
                    );
                    continue;
                }

                let row_cap = cap.min(rule.payload.max.saturating_add(FRAME_HEADER_BYTES));
                assert_eq!(check(0, rule.payload.min), Ok(row_cap));
                let largest = row_cap - FRAME_HEADER_BYTES;
                assert_eq!(check(0, largest), Ok(row_cap));
                // One byte past the row's cap reaches the caller's oversize check.
                assert!(FRAME_HEADER_BYTES + largest + 1 > row_cap);

                if rule.payload.min > 0 {
                    let short = Err(FrameRejection::PayloadTooShort {
                        min: rule.payload.min,
                    });
                    assert_eq!(check(0, rule.payload.min - 1), short);
                    assert_eq!(check(0, 0), short, "a header-only frame is too short");
                }
                for bit in 0..u16::BITS {
                    assert_eq!(
                        check(1 << bit, rule.payload.min),
                        Err(FrameRejection::ReservedFlags)
                    );
                }
            }
        }
    }
}

#[test]
fn undeclared_types_are_rejected_on_every_reader() {
    for (stream, rules) in native_rule_tables() {
        let declared: BTreeSet<u16> = rules.iter().map(|rule| rule.message_type).collect();
        for reader in READERS {
            let filter = FrameFilter::new(Some(rules), reader);
            for message_type in (0..=u16::MAX).filter(|kind| !declared.contains(kind)) {
                assert_eq!(
                    filter.check_header(message_type, 0, 0, stream_cap(stream)),
                    Err(FrameRejection::UnknownMessageType)
                );
            }
        }
    }
}

#[test]
fn services_without_rules_keep_the_stream_cap_for_any_header() {
    for reader in READERS {
        let filter = FrameFilter::new(None, reader);
        for (message_type, flags, len) in [(0, 0, 0), (u16::MAX, u16::MAX, 1 << 20), (3, 5, 7)] {
            assert_eq!(
                filter.check_header(message_type, flags, len, 4096),
                Ok(4096)
            );
        }
    }
}
