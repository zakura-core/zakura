//! The item suite over every item and composition.

use proptest::prelude::*;
use zakura_chain::serialization::{CompactSize64, ZcashDeserialize, ZcashSerialize};

use super::{
    item_suite::{decode_checked, item_suite},
    reader::{compact_size_len, write_compact_size},
    *,
};
use crate::zakura::PayloadLen;

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
