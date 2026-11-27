//! Tests for trusted preallocation of the v7 envelope and transparent effecting data.

use proptest::prelude::*;

use crate::{
    block::MAX_BLOCK_BYTES,
    serialization::{SerializationError, TrustedPreallocate, ZcashDeserializeInto, ZcashSerialize},
    transparent,
};

use super::{
    super::{
        codec::{InputEffects, RawBundle, RawDelta},
        registry::{MAX_MAP_ENTRIES, REGISTRY},
    },
    compact_size, decode, usize_to_u64, Envelope,
};

fn delta_entry(bundle_type: u64, variant: u64, value: i64) -> Vec<u8> {
    [
        compact_size(bundle_type),
        compact_size(variant),
        value.to_le_bytes().to_vec(),
    ]
    .concat()
}

fn bundle_entry(bundle_type: u64, variant: u64, payload: &[u8]) -> Vec<u8> {
    [
        compact_size(bundle_type),
        compact_size(variant),
        compact_size(usize_to_u64(payload.len())),
        payload.to_vec(),
    ]
    .concat()
}

/// Encodes a map of `count` entries, each produced by `entry` from its position.
fn map(count: u64, entry: impl Fn(u64) -> Vec<u8>) -> Vec<u8> {
    let mut bytes = compact_size(count);
    for index in 0..count {
        bytes.extend(entry(index));
    }
    bytes
}

fn is_too_long<T>(result: &Result<T, SerializationError>) -> bool {
    matches!(
        result,
        Err(SerializationError::Parse(
            "Vector longer than max_allocation"
        ))
    )
}

#[test]
fn map_entries_are_capped_at_the_registered_types() {
    let _init_guard = zakura_test::init();

    // The cap counts bundle types, so it relies on one pair per type.
    assert!(
        REGISTRY.windows(2).all(|pair| pair[0].0 .0 < pair[1].0 .0),
        "the registry has one pair per type, in type order"
    );
    assert_eq!(RawDelta::max_allocation(), MAX_MAP_ENTRIES);
    assert_eq!(RawBundle::max_allocation(), MAX_MAP_ENTRIES);

    // A map with one entry per registered type has the most entries that can decode.
    let deltas: Vec<RawDelta> = map(MAX_MAP_ENTRIES, |t| delta_entry(t, 0, 1))
        .zcash_deserialize_into()
        .expect("a full map is within the cap");
    assert_eq!(usize_to_u64(deltas.len()), MAX_MAP_ENTRIES);
    let bundles: Vec<RawBundle> = map(MAX_MAP_ENTRIES, |t| bundle_entry(t, 0, &[]))
        .zcash_deserialize_into()
        .expect("a full map is within the cap");
    assert_eq!(usize_to_u64(bundles.len()), MAX_MAP_ENTRIES);

    // One more entry is rejected by its count, before any entry is read.
    let too_many = MAX_MAP_ENTRIES + 1;
    assert!(is_too_long(
        &map(too_many, |t| delta_entry(t, 0, 1)).zcash_deserialize_into::<Vec<RawDelta>>()
    ));
    assert!(is_too_long(
        &map(too_many, |t| bundle_entry(t, 0, &[])).zcash_deserialize_into::<Vec<RawBundle>>()
    ));
}

/// A block-sized transaction of empty bundle entries is rejected by its entry count,
/// instead of allocating every entry before the parsing rules reject it.
#[test]
fn block_sized_maps_are_rejected_by_their_count() {
    let _init_guard = zakura_test::init();

    let count = MAX_BLOCK_BYTES / RawBundle::min_serialized_size();
    let mut bytes = Envelope::empty().header;
    bytes.extend(compact_size(0));
    bytes.extend(map(count, |_| bundle_entry(0, 0, &[])));
    bytes.extend(compact_size(0));
    assert!(is_too_long(&decode(&bytes)));
}

proptest! {
    /// Every delta takes at least `RawDelta::min_serialized_size` bytes.
    #[test]
    fn delta_size_is_small_enough(t in any::<u32>(), v in any::<u32>(), value in any::<i64>()) {
        let bytes = delta_entry(u64::from(t), u64::from(v), value);
        prop_assert!(usize_to_u64(bytes.len()) >= RawDelta::min_serialized_size());
    }

    /// Every bundle entry takes at least `RawBundle::min_serialized_size` bytes.
    #[test]
    fn bundle_size_is_small_enough(
        t in any::<u32>(),
        v in any::<u32>(),
        payload in proptest::collection::vec(any::<u8>(), 0..300),
    ) {
        let bytes = bundle_entry(u64::from(t), u64::from(v), &payload);
        prop_assert!(usize_to_u64(bytes.len()) >= RawBundle::min_serialized_size());
    }

    /// Every input's effecting data takes `InputEffects::min_serialized_size` bytes, and
    /// one more input than `max_allocation` cannot fit in a block.
    #[test]
    fn input_effects_size_and_allocation(
        outpoint in any::<transparent::OutPoint>(),
        sequence in any::<u32>(),
    ) {
        let mut bytes = outpoint.zcash_serialize_to_vec().expect("writing to a Vec cannot fail");
        bytes.extend_from_slice(&sequence.to_le_bytes());
        prop_assert_eq!(usize_to_u64(bytes.len()), InputEffects::min_serialized_size());
        prop_assert!(bytes.zcash_deserialize_into::<InputEffects>().is_ok());

        let smallest_disallowed =
            (InputEffects::max_allocation() + 1) * InputEffects::min_serialized_size();
        prop_assert!(smallest_disallowed > MAX_BLOCK_BYTES);
    }
}
