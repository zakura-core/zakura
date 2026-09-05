//! The storage half of the serving byte contract, independent of network policy.

use proptest::prelude::*;
use zakura_chain::block::{Height, MAX_BLOCK_BYTES};

proptest! {
    #[test]
    fn response_cap_that_fits_any_block_makes_progress(
        sizes in prop::collection::vec(1u32..=u32::try_from(MAX_BLOCK_BYTES).unwrap(), 1..16),
        byte_cap in u32::try_from(MAX_BLOCK_BYTES).unwrap()..=33_554_432u32,
    ) {
        let response = super::super::collect_bounded_height_range(
            Height(0), u32::try_from(sizes.len()).unwrap(), byte_cap, |height| {
                let size = usize::try_from(sizes[usize::try_from(height.0).unwrap()]).unwrap();
                Some((height, size))
            });
        prop_assert!(!response.is_empty(), "an available first block must fit");
        prop_assert_eq!(response[0].0, Height(0));
        prop_assert!(response.iter().map(|(_, _, size)| u64::try_from(*size).unwrap()).sum::<u64>()
            <= u64::from(byte_cap));
    }

    #[test]
    fn bounded_range_matches_a_contiguous_prefix(
        sizes in prop::collection::vec(prop::option::of(1u32..4_000_001), 0..16),
        count in 0u32..20,
        byte_cap in 0u32..16_000_001,
        start in prop_oneof![0u32..100, (u32::MAX - 16)..=u32::MAX],
    ) {
        let mut expected = Vec::new();
        let mut expected_lookups = Vec::new();
        let mut total = 0u64;
        for offset in 0..count {
            let height = u64::from(start) + u64::from(offset);
            if height > u64::from(u32::MAX) { break; }
            let height = u32::try_from(height).unwrap();
            expected_lookups.push(Height(height));
            let Some(Some(size)) = sizes.get(usize::try_from(offset).unwrap()) else { break };
            total += u64::from(*size);
            if total > u64::from(byte_cap) { break; }
            expected.push((Height(height), offset, usize::try_from(*size).unwrap()));
        }
        let mut actual_lookups = Vec::new();
        let actual = super::super::collect_bounded_height_range(Height(start), count, byte_cap, |height| {
            actual_lookups.push(height);
            let offset = height.0 - start;
            sizes.get(usize::try_from(offset).unwrap()).copied().flatten()
                .map(|size| (offset, usize::try_from(size).unwrap()))
        });
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(actual_lookups, expected_lookups);
    }
}
