//! The item suite: one wire item, checked against its real encodings.
//!
//! [`item_suite!`] adds these tests for one item type:
//!
//! - **Tight bounds.** The item's shortest and longest boundary values encode
//!   to exactly `MIN_LEN` and `MAX_LEN`, so a derived bound is checked against
//!   encodings, never against itself.
//! - **Round trip.** Every boundary value and every generated value decodes to
//!   itself and reads exactly its own encoding, even with bytes after it.
//! - **Truncation.** Every short prefix of a valid encoding fails to decode.
//! - **Hostile input.** Decoding arbitrary bytes, and valid encodings with one
//!   byte changed, returns without panicking. Whatever decodes re-encodes to the
//!   bytes it read, so the codec accepts only canonical encodings.
//! - **Allocation.** Every decode above requests no more heap than
//!   `max_heap_bytes` allows, measured with [`TrackingAllocator`].
//!
//! Tuples and lists are items too, so the suite checks composition once. A
//! message whose payload composes checked items needs no bound test of its own.
//!
//! [`TrackingAllocator`]: zakura_test::allocations::TrackingAllocator

use super::{sample::encoding, sample::WireSample, BoundedReader, Wire, WireError};

/// Short prefixes are all checked. Beyond this length the suite checks a
/// sparse set, so large encodings stay cheap.
const DENSE_PREFIX_BYTES: usize = 64;

/// The longest arbitrary input the hostile-input test generates.
pub(crate) const MAX_ARBITRARY_INPUT_BYTES: usize = 4096;

/// Check `T`'s bounds and every boundary value.
pub(crate) fn check_item<T: WireSample>() {
    let values = T::boundary_values();
    let lengths: Vec<usize> = values
        .iter()
        .map(|value| encoding::<T>(value).len())
        .collect();
    assert_eq!(
        lengths.iter().min(),
        Some(&T::MIN_LEN),
        "the shortest boundary value must encode to MIN_LEN"
    );
    assert_eq!(
        lengths.iter().max(),
        Some(&T::MAX_LEN),
        "the longest boundary value must encode to MAX_LEN"
    );
    for value in &values {
        check_value::<T>(value);
    }
}

/// Check one valid value: round trip, exact reads, and truncation.
pub(crate) fn check_value<T: WireSample>(value: &T::Value) {
    let bytes = encoding::<T>(value);
    assert!(
        (T::MIN_LEN..=T::MAX_LEN).contains(&bytes.len()),
        "a {}-byte encoding is outside {}..={}",
        bytes.len(),
        T::MIN_LEN,
        T::MAX_LEN
    );

    let mut followed = bytes.clone();
    followed.push(0xa5);
    let (decoded, unread) = decode_checked::<T>(&followed);
    assert_eq!(decoded.as_ref(), Ok(value), "a valid encoding round-trips");
    assert_eq!(unread, 1, "decoding reads exactly the item's encoding");

    for prefix in prefixes(bytes.len()) {
        let (decoded, _) = decode_checked::<T>(&bytes[..prefix]);
        assert!(
            decoded.is_err(),
            "a {prefix}-byte prefix of a {}-byte encoding decoded as {decoded:?}",
            bytes.len()
        );
    }
}

/// Check that decoding `input` is total, bounded, and canonical.
pub(crate) fn check_input<T: Wire>(input: &[u8]) {
    let (decoded, unread) = decode_checked::<T>(input);
    if let Ok(value) = decoded {
        let read = &input[..input.len() - unread];
        let mut reencoded = Vec::new();
        T::encode(&value, &mut reencoded).expect("a decoded value encodes");
        assert_eq!(
            reencoded, read,
            "a decoded value re-encodes to the bytes it read"
        );
    }
}

/// Decode `input` as `T`, and check the heap it requested.
///
/// Returns the result and the number of unread bytes.
pub(crate) fn decode_checked<T: Wire>(input: &[u8]) -> (Result<T::Value, WireError>, usize) {
    let (decoded, allocated) = zakura_test::allocations::measure(|| {
        let mut reader = BoundedReader::new(input);
        let decoded = T::decode(&mut reader);
        (decoded, reader.remaining())
    });
    let bound = T::max_heap_bytes(input.len());
    assert!(
        allocated.peak_live_bytes <= bound,
        "decoding {} bytes requested {} heap bytes; the item's bound is {bound}",
        input.len(),
        allocated.peak_live_bytes
    );
    decoded
}

/// Prefix lengths to check for an encoding of `len` bytes: every short prefix,
/// then the middle and the last.
pub(crate) fn prefixes(len: usize) -> impl Iterator<Item = usize> {
    (0..len.min(DENSE_PREFIX_BYTES))
        .chain([len / 2, len.saturating_sub(1)])
        .filter(move |prefix| *prefix < len)
}

/// Add the item suite for `$item` in a module named `$name`.
macro_rules! item_suite {
    ($name:ident, $item:ty) => {
        mod $name {
            use proptest::prelude::*;

            #[allow(unused_imports)]
            use super::*;
            use $crate::zakura::wire_codec::{
                item_suite::{check_input, check_item, check_value, MAX_ARBITRARY_INPUT_BYTES},
                sample::{encoding, WireSample},
                Wire,
            };

            fn input_len() -> std::ops::RangeInclusive<usize> {
                0..=<$item as Wire>::MAX_LEN
                    .saturating_add(8)
                    .min(MAX_ARBITRARY_INPUT_BYTES)
            }

            #[test]
            fn bounds_are_tight_and_boundary_values_round_trip() {
                check_item::<$item>();
            }

            proptest! {
                #[test]
                fn valid_values_round_trip(value in <$item as WireSample>::arbitrary()) {
                    check_value::<$item>(&value);
                }

                #[test]
                fn arbitrary_input_decodes_totally_within_bounds(
                    input in proptest::collection::vec(any::<u8>(), input_len()),
                ) {
                    check_input::<$item>(&input);
                }

                #[test]
                fn changed_encodings_decode_totally_within_bounds(
                    value in <$item as WireSample>::arbitrary(),
                    position in any::<prop::sample::Index>(),
                    byte in any::<u8>(),
                ) {
                    let mut input = encoding::<$item>(&value);
                    if !input.is_empty() {
                        let position = position.index(input.len());
                        input[position] = byte;
                    }
                    check_input::<$item>(&input);
                }
            }
        }
    };
}

pub(crate) use item_suite;
