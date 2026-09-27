//! The item suite over every item and composition, and controls that show the
//! suites catch broken codecs.

use proptest::prelude::*;
use zakura_chain::serialization::{CompactSize64, ZcashDeserialize, ZcashSerialize};

use super::{
    item_suite::{check_input, check_item, decode_checked, item_suite},
    message_suite::{check_family, MessageSample, Violation},
    reader::{compact_size_len, write_compact_size},
    sample::WireSample,
    *,
};
use crate::zakura::{MessageRole, MessageRule, PayloadLen};

item_suite!(one_byte, U8);
item_suite!(le_u32, LeU32);
item_suite!(height, HeightLe);
item_suite!(pair, (HeightLe, LeU32));
item_suite!(nested_tuple, (U8, (LeU32, U8), HeightLe));
item_suite!(short_list, List<U8, 0, 3>);
item_suite!(empty_only_list, List<LeU32, 0, 0>);
// A count of 253 or more takes a three-byte CompactSize.
item_suite!(list_across_count_widths, List<U8, 1, 300>);
item_suite!(
    list_of_tuples_of_lists,
    List<(HeightLe, List<U8, 1, 2>), 1, 4>
);

#[test]
fn composed_bounds_are_sums_of_their_parts() {
    assert_eq!(PayloadLen::of::<(HeightLe, LeU32)>(), PayloadLen::exact(8));
    assert_eq!(
        PayloadLen::of::<(HeightLe, List<U8, 1, 300>)>(),
        PayloadLen::between(4 + 1 + 1, 4 + 3 + 300)
    );
}

#[test]
fn a_hostile_count_allocates_nothing() {
    // A three-byte CompactSize claims 25,000 items with no item bytes behind it.
    let (decoded, _) = decode_checked::<List<LeU32, 0, 100_000>>(&[0xfd, 0xa8, 0x61]);
    assert_eq!(
        decoded,
        Err(WireError::CountExceedsPayload {
            count: 25_000,
            remaining: 0,
        })
    );
    assert_eq!(<List<LeU32, 0, 100_000>>::max_heap_bytes(3), 0);
}

#[test]
fn a_count_above_the_list_maximum_fails_before_the_payload_check() {
    let (decoded, _) = decode_checked::<List<U8, 0, 3>>(&[4, 1, 2, 3, 4]);
    assert_eq!(
        decoded,
        Err(WireError::CountOutOfRange {
            count: 4,
            min: 0,
            max: 3,
        })
    );
}

#[test]
fn heights_above_the_maximum_fail_both_ways() {
    let above = zakura_chain::block::Height(zakura_chain::block::Height::MAX.0 + 1);
    assert_eq!(
        HeightLe::encode(&above, &mut Vec::new()),
        Err(WireError::OutOfRange("block height"))
    );
    let (decoded, _) = decode_checked::<HeightLe>(&above.0.to_le_bytes());
    assert_eq!(decoded, Err(WireError::OutOfRange("block height")));
}

proptest! {
    /// The reader's CompactSize matches Zcash's encoder for every count.
    #[test]
    fn compact_size_encoding_matches_zcash(count in any::<u64>()) {
        let mut ours = Vec::new();
        write_compact_size(count, &mut ours);
        let zcash = CompactSize64::from(count)
            .zcash_serialize_to_vec()
            .expect("writing to a Vec cannot fail");
        prop_assert_eq!(&ours, &zcash);
        prop_assert_eq!(compact_size_len(usize::try_from(count).unwrap_or(usize::MAX)), ours.len());
    }

    /// The reader's CompactSize accepts exactly what Zcash's decoder accepts,
    /// including its rejection of non-canonical encodings.
    ///
    /// Most inputs start with a width marker followed by a small value, so
    /// non-canonical and truncated encodings of every width occur often.
    #[test]
    fn compact_size_decoding_matches_zcash(
        marker in prop_oneof![Just(0xfd_u8), Just(0xfe), Just(0xff), any::<u8>()],
        value in prop_oneof![0..=0x1_0000_u64, any::<u64>()],
        len in 0..=8_usize,
    ) {
        let input = [&[marker][..], &value.to_le_bytes()[..len]].concat();
        let mut reader = BoundedReader::new(&input);
        let ours = reader.compact_size();
        let zcash = CompactSize64::zcash_deserialize(&input[..]).map(u64::from);
        prop_assert_eq!(ours.ok(), zcash.ok());
    }
}

#[test]
fn non_canonical_counts_fail() {
    for input in [
        &[0xfd, 0xfc, 0x00][..],
        &[0xfe, 0xff, 0xff, 0x00, 0x00],
        &[0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00],
    ] {
        assert_eq!(
            BoundedReader::new(input).compact_size(),
            Err(WireError::NonCanonicalCount)
        );
    }
}

/// An item that claims one more byte than any encoding uses.
#[derive(Debug)]
enum LooseMaximum {}

impl Wire for LooseMaximum {
    type Value = u32;
    const MIN_LEN: usize = 4;
    const MAX_LEN: usize = 5;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &u32, out: &mut Vec<u8>) -> Result<(), WireError> {
        LeU32::encode(value, out)
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<u32, WireError> {
        LeU32::decode(reader)
    }
}

impl WireSample for LooseMaximum {
    fn boundary_values() -> Vec<u32> {
        LeU32::boundary_values()
    }

    fn arbitrary() -> BoxedStrategy<u32> {
        LeU32::arbitrary()
    }
}

#[test]
#[should_panic(expected = "the longest boundary value must encode to MAX_LEN")]
fn the_item_suite_rejects_a_bound_that_no_encoding_reaches() {
    check_item::<LooseMaximum>();
}

/// An item that copies its input into a buffer its bound does not declare.
#[derive(Debug)]
enum UndeclaredBuffer {}

impl Wire for UndeclaredBuffer {
    type Value = u8;
    const MIN_LEN: usize = 1;
    const MAX_LEN: usize = 1;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &u8, out: &mut Vec<u8>) -> Result<(), WireError> {
        U8::encode(value, out)
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<u8, WireError> {
        let copy = std::hint::black_box(vec![0u8; 64]);
        drop(copy);
        U8::decode(reader)
    }
}

#[test]
#[should_panic(expected = "requested 64 heap bytes; the item's bound is 0")]
fn the_item_suite_rejects_an_undeclared_allocation() {
    let _ = decode_checked::<UndeclaredBuffer>(&[1]);
}

/// A boolean that decodes any nonzero byte as `true`.
#[derive(Debug)]
enum LenientBool {}

impl Wire for LenientBool {
    type Value = bool;
    const MIN_LEN: usize = 1;
    const MAX_LEN: usize = 1;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &bool, out: &mut Vec<u8>) -> Result<(), WireError> {
        U8::encode(&u8::from(*value), out)
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<bool, WireError> {
        Ok(U8::decode(reader)? != 0)
    }
}

#[test]
#[should_panic(expected = "a decoded value re-encodes to the bytes it read")]
fn the_item_suite_rejects_a_non_canonical_decoder() {
    check_input::<LenientBool>(&[2]);
}

/// A one-message family whose row claims one more byte than its encoding.
#[derive(Clone, Debug, PartialEq)]
struct LooseRow(u32);

impl WireMessage for LooseRow {
    type Error = WireError;
    const RULES: &'static [MessageRule] = &[MessageRule {
        message_type: 1,
        payload: PayloadLen::between(4, 5),
        role: MessageRole::Announcement {
            cadence: crate::zakura::Cadence {
                capacity: 1,
                refill_interval: std::time::Duration::from_secs(1),
                send_interval: std::time::Duration::from_secs(2),
            },
        },
    }];

    fn message_type(&self) -> u16 {
        1
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        LeU32::encode(&self.0, out)
    }

    fn decode_payload(
        _message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        reader.read::<LeU32>().map(Self)
    }

    fn max_heap_bytes(_message_type: u16, _payload_len: usize) -> usize {
        0
    }
}

impl MessageSample for LooseRow {
    fn samples() -> Vec<Self> {
        vec![Self(0), Self(u32::MAX)]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        any::<u32>().prop_map(Self).boxed()
    }

    fn violations() -> Vec<Violation<Self>> {
        Vec::new()
    }
}

#[test]
#[should_panic(expected = "the samples of type 1 must reach its row's bounds")]
fn the_message_suite_rejects_a_row_that_no_encoding_reaches() {
    check_family::<LooseRow>();
}
