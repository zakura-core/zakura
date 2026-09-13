//! Independent capacity accounting, including the old buffer during replacement.

use super::*;
use zakura_test::allocations::measure;

proptest! {
    #[test]
    fn retained_collection_histories_fund_capacity_and_growth_peaks(
        budget_units in 1u64..128,
        actions in prop::collection::vec((0u8..3, any::<u8>(), any::<bool>()), 1..256),
    ) {
        let setup = ResponseMemory::node_setup_bytes_for_test() + ResponseMemory::setup_bytes_for_test();
        let budget = budget_units * 32;
        let node = ResponseMemory::new(setup + budget, setup + budget);
        let memory = node.connection();
        let mut actual = ResponseVec::<[u64; 4]>::new();
        let mut expected = Vec::new();
        let mut capacity = 0usize;

        for (action, value, geometric) in actions {
            if action == 0 {
                let required = expected.len() + 1;
                let target = if required <= capacity { capacity }
                    else if geometric { required.max(capacity * 2).max(4) }
                    else { required };
                let growth_bytes = if target == capacity { 0 } else { u64::try_from(target).unwrap() * 32 };
                let old_bytes = u64::try_from(capacity).unwrap() * 32;
                let allowed = old_bytes + growth_bytes <= budget;
                let (plan, allocations) = measure(|| actual.plan_capacity(1, geometric).unwrap());
                prop_assert_eq!(allocations.requested_bytes, 0);
                prop_assert_eq!(plan.as_ref().map_or(0, |plan| plan.bytes()), growth_bytes);
                let (funding, allocations) = measure(|| {
                    if growth_bytes == 0 { None } else { memory.try_reserve(growth_bytes) }
                });
                prop_assert_eq!(allocations.requested_bytes, 0);
                prop_assert_eq!(growth_bytes == 0 || funding.is_some(), allowed);
                if allowed {
                    prop_assert_eq!(node.reserved_for_test(), setup + old_bytes + growth_bytes);
                    let (result, allocations) = measure(|| actual.apply_capacity(plan, funding));
                    prop_assert!(result.is_ok());
                    prop_assert_eq!(u64::try_from(allocations.peak_live_bytes).unwrap(), growth_bytes);
                    let (_, allocations) = measure(|| actual.push([u64::from(value); 4]));
                    prop_assert_eq!(allocations.requested_bytes, 0);
                    expected.push([u64::from(value); 4]);
                    capacity = target;
                }
            } else if action == 1 && !expected.is_empty() {
                let index = usize::from(value) % expected.len();
                let (removed, allocations) = measure(|| actual.remove(index));
                prop_assert_eq!(removed, expected.remove(index));
                prop_assert_eq!(allocations.requested_bytes, 0);
            } else if action == 2 {
                let (_, allocations) = measure(|| actual.clear());
                prop_assert_eq!(allocations.requested_bytes, 0);
                expected.clear();
            }
            prop_assert_eq!(&*actual, expected.as_slice());
            prop_assert_eq!(actual.capacity(), capacity);
            prop_assert_eq!(node.reserved_for_test(), setup + u64::try_from(capacity).unwrap() * 32);
        }
        drop(actual);
        prop_assert_eq!(node.reserved_for_test(), setup);
        drop(memory);
        prop_assert_eq!(node.reserved_for_test(), ResponseMemory::node_setup_bytes_for_test());
    }
}
