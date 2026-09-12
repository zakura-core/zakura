//! Generated ownership histories for reusable concurrency slots.

use super::*;
use proptest::prelude::*;

mod response_lifecycle;

proptest! {
    #[test]
    fn slot_histories_bound_owned_work(
        capacity in 1usize..8,
        actions in prop::collection::vec((any::<bool>(), 0usize..12), 1..96),
    ) {
        let budget = SlotBudget::new(capacity).unwrap();
        let mut permits: [Option<SlotPermit>; 12] = std::array::from_fn(|_| None);
        let mut occupied = [false; 12];
        for (acquire, index) in actions {
            if acquire && !occupied[index] {
                let expected = occupied.iter().filter(|owned| **owned).count() < capacity;
                permits[index] = budget.try_reserve();
                prop_assert_eq!(permits[index].is_some(), expected);
                occupied[index] = expected;
            } else if !acquire {
                drop(permits[index].take());
                occupied[index] = false;
            }
            prop_assert_eq!(budget.reserved(), occupied.iter().filter(|owned| **owned).count());
        }
        drop(permits);
        prop_assert_eq!(budget.reserved(), 0);
    }
}
