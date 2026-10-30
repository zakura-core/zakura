//! What each version 9 frame says, as [`FrameSummary`] values (M6a-6).
//!
//! A producer builds its frame's summary from its own state where it builds
//! the frame, and reports it as the output's attempt. The stream writer
//! decodes the frame it writes with [`header_frame_summary`], and a
//! simulated peer decodes what it read the same way, so the output ledger
//! can check the actual frame against the attempt without a receipt.
//! This module exists only in test and simulator builds.

use zakura_chain::block;
use zakura_header_chain::Frontier;

use super::{Cursor, Message, Operation, SubscribeHeaders};
use crate::zakura::{wire_codec::decode_frame, Frame, FrameSummary, Producer, SummaryCursor};

/// `frame` decoded as a version 9 frame of a network whose headers take
/// `HEADER_BYTES`, or `None` if it doesn't decode.
///
/// A page's summary counts its own entries and payload bytes, and names its
/// last header, at the ancestor's height plus its entry count.
pub fn header_frame_summary<const HEADER_BYTES: usize>(frame: &Frame) -> Option<FrameSummary> {
    let message = decode_frame::<Message<HEADER_BYTES>>(frame).ok()?;
    let mut summary = match message {
        Message::Status(_) => status(),
        Message::SubscribeHeaders(update) => self::update(&update),
        Message::Headers(page) => {
            let last = page.entries.last()?;
            let count = u32::try_from(page.entries.len()).ok()?;
            let height = page.common_ancestor_height.0.checked_add(count)?;
            self::page(
                page.subscription_id,
                count,
                // Widening usize to u64 is lossless on supported targets.
                frame.payload.len() as u64,
                &Frontier {
                    height: block::Height(height),
                    hash: last.header.hash(),
                },
            )
        }
        Message::HeadersOutcome(outcome) => terminal(outcome.subscription_id),
    };
    summary.flags = frame.flags;
    Some(summary)
}

/// [`header_frame_summary`] on Regtest, whose headers take 177 bytes.
pub fn regtest_frame_summary(frame: &Frame) -> Option<FrameSummary> {
    header_frame_summary::<{ crate::zakura::header_sync::config::REGTEST_HEADER_BYTES }>(frame)
}

/// The attempt of a cadence frame carrying `message`, from the value the
/// cadence coalesced into it. Header sync's cadence row is `Status`.
pub(crate) fn of_cadence(message: &super::CommonMessage) -> FrameSummary {
    match message {
        Message::SubscribeHeaders(update) => self::update(update),
        Message::HeadersOutcome(outcome) => terminal(outcome.subscription_id),
        // `Status` is the only cadence row. A page names its subscription.
        Message::Status(_) => status(),
        Message::Headers(page) => FrameSummary {
            subscription: Some(page.subscription_id),
            ..status()
        },
    }
}

/// A `Status`, which names no subscription.
pub(crate) fn status() -> FrameSummary {
    FrameSummary {
        producer: Producer::Status,
        subscription: None,
        flags: 0,
        objects: 0,
        bytes: 0,
        cursor: None,
    }
}

/// A subscription update: its operation, subscription, the credit it adds
/// and the cursor it acknowledges.
pub(crate) fn update(update: &SubscribeHeaders) -> FrameSummary {
    FrameSummary {
        producer: match update.operation {
            Operation::Open => Producer::Open,
            Operation::Grant => Producer::Grant,
            Operation::Close => Producer::Close,
        },
        subscription: Some(update.subscription_id),
        flags: 0,
        objects: update.added_credit.objects.into(),
        bytes: update.added_credit.bytes.into(),
        cursor: Some(cursor(&update.acknowledged)),
    }
}

/// A page of `count` headers in `bytes` payload bytes for `subscription`,
/// ending at `last`.
pub(crate) fn page(subscription: u64, count: u32, bytes: u64, last: &Frontier) -> FrameSummary {
    FrameSummary {
        producer: Producer::Page,
        subscription: Some(subscription),
        flags: 0,
        objects: count.into(),
        bytes,
        cursor: Some(SummaryCursor {
            height: last.height.0,
            hash: last.hash.0,
            headers: None,
            bytes: None,
        }),
    }
}

/// A terminal outcome of `subscription`.
pub(crate) fn terminal(subscription: u64) -> FrameSummary {
    FrameSummary {
        producer: Producer::Terminal,
        subscription: Some(subscription),
        flags: 0,
        objects: 0,
        bytes: 0,
        cursor: None,
    }
}

/// `cursor` in summary fields.
fn cursor(cursor: &Cursor) -> SummaryCursor {
    SummaryCursor {
        height: cursor.height.0,
        hash: cursor.hash.0,
        headers: Some(cursor.headers),
        bytes: Some(cursor.bytes),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::block;

    use super::*;
    use crate::zakura::{
        header_sync::{
            config::REGTEST_HEADER_BYTES,
            v9::{
                test_support::{smallest_status, HeaderTree},
                CommonMessage, Headers, RegtestMessage, Status,
            },
            wire::{AuxSchema, HeaderEntry},
        },
        regulation::CadenceSender,
        transport::output_tests::{tapped, Life},
        wire_codec::encode_frame,
        Credit, TransportEvent,
    };

    /// A Regtest page for subscription 9 of `count` new headers above
    /// `parent`, and its last header.
    fn regtest_page(tree: &mut HeaderTree, parent: Frontier, count: u32) -> (Frame, Frontier) {
        let added = tree.extend(parent, count);
        let entries = added
            .iter()
            .map(|frontier| HeaderEntry {
                header: Arc::clone(&tree.nodes[&frontier.hash].1),
                body_size: 0,
                tree_aux: None,
            })
            .collect();
        let frame = encode_frame(&RegtestMessage::Headers(Headers {
            subscription_id: 9,
            initial_target_tip_hash: block::Hash([7; 32]),
            common_ancestor_height: parent.height,
            common_ancestor_hash: parent.hash,
            reaches_initial_target: false,
            tree_aux_schema: AuxSchema::None,
            entries,
        }))
        .expect("the page encodes");
        (frame, *added.last().expect("count is nonzero"))
    }

    /// Each page's summary is its own: two pages of two headers each name
    /// two objects, their own payload bytes and their own last frontier, and
    /// match what the publisher reports from its page.
    ///
    /// This fails if a page's summary counts from the subscription's start.
    #[test]
    fn page_summaries_count_their_own_headers() {
        let mut tree = HeaderTree::new();
        let genesis = tree.genesis;
        let (first, first_last) = regtest_page(&mut tree, genesis, 2);
        let (second, second_last) = regtest_page(&mut tree, first_last, 2);
        for (frame, last) in [(&first, first_last), (&second, second_last)] {
            let summary =
                header_frame_summary::<REGTEST_HEADER_BYTES>(frame).expect("the page decodes");
            assert_eq!(
                summary,
                // A test page's length fits u64.
                super::page(9, 2, frame.payload.len() as u64, &last),
                "{last:?}"
            );
        }
    }

    /// An update's summary carries its operation, the credit it adds and
    /// the full cursor it acknowledges, and a frame with a flag set has
    /// none.
    #[test]
    fn an_update_summary_names_its_cursor_and_credit() {
        let update = SubscribeHeaders {
            operation: Operation::Grant,
            subscription_id: 3,
            update_sequence: 2,
            initial_target_tip_hash: block::Hash([7; 32]),
            acknowledged: Cursor {
                height: block::Height(12),
                hash: block::Hash([5; 32]),
                headers: 12,
                bytes: 3_000,
            },
            locator_hashes: Vec::new(),
            added_credit: Credit {
                objects: 4,
                bytes: 1_000,
            },
            tree_aux_schema: AuxSchema::None,
        };
        let mut frame = encode_frame(&RegtestMessage::SubscribeHeaders(update.clone()))
            .expect("the update encodes");
        let expected = self::update(&update);
        assert_eq!(expected.producer, Producer::Grant);
        assert_eq!(
            expected.cursor,
            Some(SummaryCursor {
                height: 12,
                hash: [5; 32],
                headers: Some(12),
                bytes: Some(3_000),
            })
        );
        assert_eq!(
            header_frame_summary::<REGTEST_HEADER_BYTES>(&frame),
            Some(expected)
        );
        // Version 9 reserves every flag bit, so a frame with one set doesn't
        // decode, and its summary can't match any attempt.
        frame.flags = 1;
        assert_eq!(header_frame_summary::<REGTEST_HEADER_BYTES>(&frame), None);
    }

    /// Two `Status` updates the cadence coalesces into one frame give one
    /// output with one attempt, reported where the cadence builds the
    /// frame, and the output's fingerprint is that frame's (M6a-6).
    ///
    /// This fails if the attempt is reported per update.
    #[test]
    fn a_coalesced_status_keeps_its_attempt() {
        let (send, _recv, recorder) = tapped(2);
        let mut cadence = CadenceSender::<CommonMessage>::new().attempting(super::of_cadence);
        let first =
            smallest_status::<{ crate::zakura::header_sync::config::COMMON_HEADER_BYTES }>();
        let second = Status {
            max_headers_per_response: 2,
            ..first.clone()
        };
        cadence
            .update(CommonMessage::Status(first))
            .expect("status has a cadence");
        cadence
            .update(CommonMessage::Status(second.clone()))
            .expect("status has a cadence");
        assert_eq!(cadence.send_due(&send), Ok(1));
        let frame = encode_frame(&CommonMessage::Status(second)).expect("the status encodes");
        let events = recorder.events();
        let attempts: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                TransportEvent::OutputAttempt {
                    output, attempt, ..
                } => Some((*output, *attempt)),
                _ => None,
            })
            .collect();
        assert_eq!(attempts, [(0, status())]);
        assert!(events.iter().any(|event| matches!(
            event,
            TransportEvent::OutputAllocated { output: 0, expected, .. }
                if *expected == crate::zakura::frame_fingerprint(&frame)
        )));
        assert_eq!(
            recorder.lives(),
            [(0, Life::Allocated), (0, Life::Enqueued(0))]
        );
    }
}
