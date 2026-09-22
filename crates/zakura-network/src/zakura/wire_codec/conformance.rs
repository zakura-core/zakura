//! Conformance suite for any [`WireMessage`] family.
//!
//! A family implements [`WireSample`] with hand-picked samples and specific
//! violations. The suite then checks the family against its own rule table:
//!
//! - coverage is closed: every rule has a sample and every sample has a rule;
//! - bounds are tight: the shortest and longest sample of each bounded rule
//!   encode to exactly the rule's minimum and maximum, so a derived constant is
//!   checked against real encodings instead of against itself;
//! - every sample round-trips to the same value and the same bytes;
//! - generic mutations fail: truncations, a trailing byte, each flag bit, one
//!   byte past the maximum, and undeclared types;
//! - each specific violation fails with the error its predicate names;
//! - decoding a hostile payload allocates no more than the family's bound.
//!
//! [`wire_conformance_tests!`] adds these checks and two property tests to a
//! family's test module.

use std::{collections::BTreeSet, fmt::Debug};

use proptest::prelude::*;

use crate::zakura::Frame;

use super::{
    allocation_meter::measure_allocated_bytes, decode_frame, decode_payload_exact, encode_frame,
    WireMessage,
};

/// Heap bytes a decode may use beyond its declared bound, for error values and
/// small fixed buffers.
pub(crate) const ALLOCATION_SLACK_BYTES: usize = 256;

/// Truncation lengths checked for each sample: every short prefix, then a
/// sparse set, so large samples stay cheap.
const DENSE_PREFIX_BYTES: usize = 64;

/// A hostile frame and the error it must produce.
pub(crate) struct WireViolation<M: WireMessage> {
    /// Short description used in failure messages.
    pub name: &'static str,
    /// The frame to decode.
    pub frame: Frame,
    /// Whether an error is the one this violation must produce.
    pub rejected_by: fn(&M::Error) -> bool,
}

/// Test data for one message family.
pub(crate) trait WireSample: WireMessage + Clone + Debug + PartialEq
where
    Self::Error: Debug,
{
    /// Rule types whose bounds the samples do not reach exactly, such as a
    /// block row whose minimum depends on the network. The suite still checks
    /// that their samples fit the rule.
    const OPEN_ENDED_ROWS: &'static [u16];

    /// Valid values that include the shortest and longest encoding of every
    /// bounded rule.
    fn samples() -> Vec<Self>;

    /// A strategy for valid values.
    fn arbitrary_valid() -> BoxedStrategy<Self>;

    /// Hostile frames that pass the header check but must fail to decode.
    fn violations() -> Vec<WireViolation<Self>>;

    /// Most heap bytes that decoding a `payload_len`-byte payload of
    /// `message_type` may allocate, or `None` if the family does not bound it.
    fn decode_allocation_bound(message_type: u16, payload_len: usize) -> Option<usize>;
}

/// Run every deterministic check for `M`.
pub(crate) fn check_conformance<M: WireSample>()
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
    check_undeclared_types::<M>(&samples);
    for violation in M::violations() {
        let result = check_allocation::<M>(&violation.frame);
        match result {
            Err(error) => assert!(
                (violation.rejected_by)(&error),
                "violation {} failed with the wrong error: {error:?}",
                violation.name
            ),
            Ok(message) => panic!("violation {} decoded as {message:?}", violation.name),
        }
    }
}

fn check_closed_coverage<M: WireSample>(samples: &[M])
where
    M::Error: Debug,
{
    let declared: BTreeSet<u16> = M::RULES.iter().map(|rule| rule.message_type).collect();
    let sampled: BTreeSet<u16> = samples.iter().map(WireMessage::message_type).collect();
    assert_eq!(
        sampled, declared,
        "every rule needs a sample and every sample a rule"
    );
}

fn check_tight_bounds<M: WireSample>(samples: &[M])
where
    M::Error: Debug,
{
    for rule in M::RULES {
        let lengths: Vec<usize> = samples
            .iter()
            .filter(|sample| sample.message_type() == rule.message_type)
            .map(|sample| encode_frame(sample).expect("samples encode").payload.len())
            .collect();
        let shortest = *lengths.iter().min().expect("coverage is closed");
        let longest = *lengths.iter().max().expect("coverage is closed");
        if M::OPEN_ENDED_ROWS.contains(&rule.message_type) {
            assert!(shortest >= rule.payload.min && longest <= rule.payload.max);
        } else {
            assert_eq!(
                (shortest, longest),
                (rule.payload.min, rule.payload.max),
                "type {} samples must reach its declared bounds",
                rule.message_type
            );
        }
    }
}

fn check_round_trip<M: WireSample>(sample: &M) -> Frame
where
    M::Error: Debug,
{
    let frame = encode_frame(sample).expect("samples encode");
    let decoded: M = decode_frame(&frame).expect("samples decode");
    assert_eq!(&decoded, sample);
    let reencoded = encode_frame(&decoded).expect("decoded samples encode");
    assert_eq!(reencoded.payload, frame.payload, "encodings are canonical");
    frame
}

fn check_mutations<M: WireSample>(frame: &Frame)
where
    M::Error: Debug,
{
    let len = frame.payload.len();
    let rule = MessageRuleRef::of::<M>(frame.message_type);
    let prefixes = (0..len.min(DENSE_PREFIX_BYTES))
        .chain([len / 2, len.saturating_sub(1)])
        .filter(|prefix| *prefix < len);
    for prefix in prefixes {
        let truncated = Frame {
            payload: frame.payload[..prefix].to_vec(),
            ..frame.clone()
        };
        assert!(
            check_allocation::<M>(&truncated).is_err(),
            "a {prefix}-byte prefix of a {len}-byte payload was accepted"
        );
    }

    let mut trailing = frame.clone();
    trailing.payload.push(0);
    assert!(
        check_allocation::<M>(&trailing).is_err(),
        "a trailing byte was accepted"
    );

    for bit in 0..u16::BITS {
        let flagged = Frame {
            flags: 1 << bit,
            ..frame.clone()
        };
        assert!(
            decode_frame::<M>(&flagged).is_err(),
            "flag bit {bit} was accepted"
        );
    }

    if !rule.open_ended {
        let mut oversized = frame.clone();
        oversized.payload.resize(rule.max + 1, 0);
        assert!(
            decode_frame::<M>(&oversized).is_err(),
            "max + 1 was accepted"
        );
    }
}

fn check_undeclared_types<M: WireSample>(samples: &[M])
where
    M::Error: Debug,
{
    let declared: BTreeSet<u16> = M::RULES.iter().map(|rule| rule.message_type).collect();
    let undeclared = [0, 1, 2, 6, 19, 255, 256, u16::MAX]
        .into_iter()
        .filter(|message_type| !declared.contains(message_type));
    for message_type in undeclared {
        for sample in samples.iter().take(1) {
            let frame = Frame {
                message_type,
                ..encode_frame(sample).expect("samples encode")
            };
            assert!(
                decode_frame::<M>(&frame).is_err(),
                "type {message_type} was accepted"
            );
        }
    }
}

/// Decode `frame` under the allocation meter and check the family's bound.
pub(crate) fn check_allocation<M: WireSample>(frame: &Frame) -> Result<M, M::Error>
where
    M::Error: Debug,
{
    let (result, allocated) = measure_allocated_bytes(|| decode_frame::<M>(frame));
    if let Some(bound) = M::decode_allocation_bound(frame.message_type, frame.payload.len()) {
        assert!(
            allocated <= bound + ALLOCATION_SLACK_BYTES,
            "type {} allocated {allocated} bytes from a {}-byte payload; bound {bound}",
            frame.message_type,
            frame.payload.len()
        );
    }
    result
}

/// Check that decoding `payload` as `message_type` is total and idempotent.
pub(crate) fn check_bounded_payload<M: WireSample>(message_type: u16, payload: &[u8])
where
    M::Error: Debug,
{
    let frame = Frame {
        message_type,
        flags: 0,
        payload: payload.to_vec(),
    };
    if let Ok(message) = check_allocation::<M>(&frame) {
        let reencoded = encode_frame(&message).expect("a decoded message encodes");
        let again: M = decode_payload_exact(message_type, &reencoded.payload)
            .expect("a re-encoded message decodes");
        assert_eq!(again, message);
    }
}

/// Bounds of one rule, looked up by type.
struct MessageRuleRef {
    max: usize,
    open_ended: bool,
}

impl MessageRuleRef {
    fn of<M: WireMessage>(message_type: u16) -> Self {
        let rule =
            super::wire_message::rule_for::<M>(message_type).expect("samples use declared types");
        Self {
            max: rule.payload.max,
            open_ended: rule.payload.is_open_ended(),
        }
    }
}

/// Add the conformance suite for `$family` to a module named `$name`.
macro_rules! wire_conformance_tests {
    ($name:ident, $family:ty) => {
        mod $name {
            use proptest::prelude::*;

            use super::*;
            use $crate::zakura::wire_codec::{
                conformance::{check_bounded_payload, check_conformance, WireSample},
                decode_frame, encode_frame, WireMessage,
            };

            #[test]
            fn conforms_to_its_rule_table() {
                let _init_guard = zakura_test::init();
                check_conformance::<$family>();
            }

            proptest! {
                #![proptest_config(ProptestConfig::with_cases(256))]

                #[test]
                fn valid_values_round_trip(message in <$family as WireSample>::arbitrary_valid()) {
                    let frame = encode_frame(&message).expect("valid values encode");
                    let decoded: $family = decode_frame(&frame).expect("valid values decode");
                    prop_assert_eq!(decoded, message);
                }

                #[test]
                fn bounded_payload_decode_is_total_and_idempotent(
                    (message_type, payload) in proptest::sample::select(<$family as WireMessage>::RULES)
                        .prop_flat_map(|rule| {
                            let max = rule.payload.max.min(rule.payload.min.saturating_add(4096));
                            (
                                Just(rule.message_type),
                                proptest::collection::vec(any::<u8>(), rule.payload.min..=max),
                            )
                        }),
                ) {
                    check_bounded_payload::<$family>(message_type, &payload);
                }
            }
        }
    };
}

pub(crate) use wire_conformance_tests;
