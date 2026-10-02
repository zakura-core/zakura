//! The message suite: one message family, checked against its rows.
//!
//! A family implements [`MessageSample`] with valid samples and its domain
//! violations. [`message_suite!`] then adds these tests:
//!
//! - **Closed coverage.** Every row has a sample, and every sample has a row.
//! - **Tight bounds.** Each row's shortest and longest sample encode to exactly
//!   the row's minimum and maximum. This checks that the family's `match` arms
//!   agree with its rows.
//! - **Round trip.** Every sample and every generated message decodes to
//!   itself and re-encodes to the same bytes.
//! - **Header mutations.** Each flag bit, each undeclared message type, and a
//!   payload one byte past the row's maximum fail to decode.
//! - **Payload mutations.** Every short prefix and one trailing byte fail to
//!   decode.
//! - **Domain violations.** Each violation fails with the error it names.
//! - **Hostile payloads.** Arbitrary payloads within each row's bounds, and
//!   valid payloads with one byte changed, decode without panicking. Whatever
//!   decodes re-encodes to the same bytes.
//! - **Allocation.** Every decode above requests no more heap than the family's
//!   `max_heap_bytes` allows.
//!
//! [`check_layout_carries_family`] checks that one layout's rows are exactly the
//! family's rows, so the frame filter and the codec read the same table.

use std::{collections::BTreeSet, fmt::Debug};

use proptest::prelude::*;

use super::{decode_frame, encode_frame, item_suite::prefixes, WireMessage};
use crate::zakura::{Frame, MessageRule, Stream};

/// A hostile frame and the error it must produce.
pub(crate) struct Violation<M: WireMessage> {
    /// Short description for failure messages.
    pub(crate) name: &'static str,
    /// The frame to decode.
    pub(crate) frame: Frame,
    /// Whether an error is the one this violation must produce.
    pub(crate) rejected_by: fn(&M::Error) -> bool,
}

/// Test data for one message family.
pub(crate) trait MessageSample: WireMessage + Clone + Debug + PartialEq + 'static
where
    Self::Error: Debug,
{
    /// Valid messages that include the shortest and the longest encoding of
    /// every row.
    fn samples() -> Vec<Self>;

    /// A strategy for valid messages.
    fn arbitrary_valid() -> BoxedStrategy<Self>;

    /// Frames that pass their row's header checks but break a domain rule.
    fn violations() -> Vec<Violation<Self>>;
}

/// Run every deterministic check for `M`.
pub(crate) fn check_family<M: MessageSample>()
where
    M::Error: Debug,
{
    let samples = M::samples();
    check_closed_coverage::<M>(&samples);
    check_tight_bounds::<M>(&samples);
    for sample in &samples {
        let frame = check_round_trip(sample);
        check_mutations::<M>(&frame);
    }
    check_undeclared_types::<M>(&samples[0]);
    for violation in M::violations() {
        match decode_checked::<M>(&violation.frame) {
            Err(error) => assert!(
                (violation.rejected_by)(&error),
                "violation {:?} failed with the wrong error: {error:?}",
                violation.name
            ),
            Ok(message) => panic!("violation {:?} decoded as {message:?}", violation.name),
        }
    }
}

/// Check that `layout`'s rows are exactly `M`'s rows.
pub(crate) fn check_layout_carries_family<M: WireMessage>(layout: &[Stream]) {
    let mut carried: Vec<MessageRule> = layout
        .iter()
        .flat_map(|stream| stream.messages.expect("a checked layout declares tables"))
        .copied()
        .collect();
    let mut family = M::RULES.to_vec();
    carried.sort_by_key(|rule| rule.message_type);
    family.sort_by_key(|rule| rule.message_type);
    assert_eq!(
        carried, family,
        "the layout's rows must be the family's rows"
    );
}

fn check_closed_coverage<M: MessageSample>(samples: &[M])
where
    M::Error: Debug,
{
    let declared: BTreeSet<u16> = M::RULES.iter().map(|rule| rule.message_type).collect();
    let sampled: BTreeSet<u16> = samples.iter().map(WireMessage::message_type).collect();
    assert_eq!(
        sampled, declared,
        "every row needs a sample and every sample a row"
    );
}

fn check_tight_bounds<M: MessageSample>(samples: &[M])
where
    M::Error: Debug,
{
    for rule in M::RULES {
        let lengths: Vec<usize> = samples
            .iter()
            .filter(|sample| sample.message_type() == rule.message_type)
            .map(|sample| encode_frame(sample).expect("samples encode").payload.len())
            .collect();
        let shortest = lengths.iter().min().copied();
        let longest = lengths.iter().max().copied();
        assert_eq!(
            (shortest, longest),
            (Some(rule.payload.min()), Some(rule.payload.max())),
            "the samples of type {} must reach its row's bounds",
            rule.message_type
        );
    }
}

fn check_round_trip<M: MessageSample>(sample: &M) -> Frame
where
    M::Error: Debug,
{
    let frame = encode_frame(sample).expect("samples encode");
    let decoded = decode_checked::<M>(&frame).expect("samples decode");
    assert_eq!(&decoded, sample);
    let reencoded = encode_frame(&decoded).expect("decoded samples encode");
    assert_eq!(reencoded.payload, frame.payload, "encodings are canonical");
    frame
}

fn check_mutations<M: MessageSample>(frame: &Frame)
where
    M::Error: Debug,
{
    let len = frame.payload.len();
    for prefix in prefixes(len) {
        let truncated = Frame {
            payload: frame.payload[..prefix].to_vec(),
            ..frame.clone()
        };
        assert!(
            decode_checked::<M>(&truncated).is_err(),
            "a {prefix}-byte prefix of a {len}-byte type {} payload was accepted",
            frame.message_type
        );
    }

    let mut trailing = frame.clone();
    trailing.payload.push(0);
    assert!(
        decode_checked::<M>(&trailing).is_err(),
        "a trailing byte was accepted"
    );

    for bit in 0..u16::BITS {
        let flagged = Frame {
            flags: 1 << bit,
            ..frame.clone()
        };
        assert!(
            decode_checked::<M>(&flagged).is_err(),
            "flag bit {bit} was accepted"
        );
    }

    let rule = MessageRule::find(M::RULES, frame.message_type).expect("samples use rows");
    let mut oversized = frame.clone();
    oversized.payload.resize(rule.payload.max() + 1, 0);
    assert!(
        decode_checked::<M>(&oversized).is_err(),
        "a payload one byte past the row's maximum was accepted"
    );
}

fn check_undeclared_types<M: MessageSample>(sample: &M)
where
    M::Error: Debug,
{
    let frame = encode_frame(sample).expect("samples encode");
    for message_type in 0..=u16::MAX {
        if MessageRule::find(M::RULES, message_type).is_none() {
            let undeclared = Frame {
                message_type,
                ..frame.clone()
            };
            assert!(
                decode_checked::<M>(&undeclared).is_err(),
                "undeclared type {message_type} was accepted"
            );
        }
    }
}

/// Check that decoding `payload` as `message_type` is total, bounded, and
/// canonical.
pub(crate) fn check_payload<M: MessageSample>(message_type: u16, payload: Vec<u8>)
where
    M::Error: Debug,
{
    let frame = Frame {
        message_type,
        flags: 0,
        payload,
    };
    if let Ok(message) = decode_checked::<M>(&frame) {
        let reencoded = encode_frame(&message).expect("a decoded message encodes");
        assert_eq!(
            reencoded.payload, frame.payload,
            "a decoded message re-encodes to the same bytes"
        );
    }
}

/// Decode `frame`, and check the heap it requested against the family's bound.
pub(crate) fn decode_checked<M: WireMessage>(frame: &Frame) -> Result<M, M::Error> {
    let (decoded, allocated) = zakura_test::allocations::measure(|| decode_frame::<M>(frame));
    let bound = M::max_heap_bytes(frame.message_type, frame.payload.len());
    assert!(
        allocated.peak_live_bytes <= bound,
        "decoding a {}-byte type {} payload requested {} heap bytes; the bound is {bound}",
        frame.payload.len(),
        frame.message_type,
        allocated.peak_live_bytes
    );
    decoded
}

/// A row's type and a payload within its bounds, capped for test speed.
pub(crate) fn bounded_payload<M: WireMessage>() -> BoxedStrategy<(u16, Vec<u8>)> {
    proptest::sample::select(M::RULES)
        .prop_flat_map(|rule| {
            let max = rule
                .payload
                .max()
                .min(rule.payload.min().saturating_add(4096));
            (
                Just(rule.message_type),
                proptest::collection::vec(any::<u8>(), rule.payload.min()..=max),
            )
        })
        .boxed()
}

/// Add the message suite for `$family` in a module named `$name`.
macro_rules! message_suite {
    ($name:ident, $family:ty) => {
        mod $name {
            use proptest::prelude::*;

            #[allow(unused_imports)]
            use super::*;
            use $crate::zakura::wire_codec::{
                decode_frame, encode_frame,
                message_suite::{bounded_payload, check_family, check_payload, MessageSample},
            };

            #[test]
            fn conforms_to_its_rows() {
                check_family::<$family>();
            }

            proptest! {
                #[test]
                fn valid_messages_round_trip(message in <$family as MessageSample>::arbitrary_valid()) {
                    let frame = encode_frame(&message).expect("valid messages encode");
                    let decoded: $family = decode_frame(&frame).expect("valid messages decode");
                    prop_assert_eq!(decoded, message);
                }

                #[test]
                fn bounded_payloads_decode_totally_within_bounds(
                    (message_type, payload) in bounded_payload::<$family>(),
                ) {
                    check_payload::<$family>(message_type, payload);
                }

                #[test]
                fn changed_payloads_decode_totally_within_bounds(
                    message in <$family as MessageSample>::arbitrary_valid(),
                    position in any::<prop::sample::Index>(),
                    byte in any::<u8>(),
                ) {
                    let mut frame = encode_frame(&message).expect("valid messages encode");
                    if !frame.payload.is_empty() {
                        let position = position.index(frame.payload.len());
                        frame.payload[position] = byte;
                    }
                    check_payload::<$family>(frame.message_type, frame.payload);
                }
            }
        }
    };
}

pub(crate) use message_suite;
