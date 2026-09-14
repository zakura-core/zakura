//! Observe real tree allocations, including splits, replacement and empty roots.

use super::*;
use crate::zakura::regulation::{ConnectionResponseMemory, ResponseMemory};
use zakura_test::allocations::measure;

fn reserve<K: Copy + Ord>(
    index: &mut ResponseIndex<K>,
    count: usize,
    memory: &ConnectionResponseMemory,
) {
    let (plan, allocations) = measure(|| index.plan_capacity(count, false).unwrap());
    assert_eq!(allocations.requests, 0);
    let mut funding = plan
        .as_ref()
        .map(|plan| memory.try_reserve(plan.bytes()).unwrap());
    index.apply_capacity_from(plan, &mut funding);
    assert!(funding.is_none_or(|funding| funding.bytes() == 0));
}

#[test]
fn cold_tree_growth_replacement_and_removal_stay_inside_the_funded_allowance() {
    for count in [1usize, 2, 4, 5, 11, 12, 64, 128, 1024, 32_768] {
        let node = ResponseMemory::new(128 * 1024 * 1024, 128 * 1024 * 1024);
        let memory = node.connection();
        let baseline = node.reserved_for_test();
        let mut index = ResponseIndex::new();
        reserve(&mut index, count, &memory);
        let funded = node.reserved_for_test() - baseline;
        let (_, allocations) = measure(|| {
            for owner in 0..count {
                // Descending keys exercise splits at the other end of the tree.
                let mut key = [0u8; 32];
                key[..8].copy_from_slice(&u64::try_from(count - owner).unwrap().to_be_bytes());
                index.insert(key, owner);
            }
            for owner in 0..count {
                let mut key = [0u8; 32];
                key[..8].copy_from_slice(&u64::try_from(count - owner).unwrap().to_be_bytes());
                index.remove(key, owner);
                index.insert([255; 32], owner);
            }
            for owner in 0..count {
                index.remove([255; 32], owner);
            }
        });
        assert!(allocations.requests > 0);
        assert!(u64::try_from(allocations.peak_live_bytes).unwrap() <= funded);
        assert_eq!(node.reserved_for_test(), baseline + funded);
        assert_eq!(index.find([255; 32]), ResponseMatch::Missing);
        // Clearing or reusing an empty index must not refund its allowance.
        index.clear();
        assert_eq!(node.reserved_for_test(), baseline + funded);
        index.insert([1; 32], 0);
        assert_eq!(node.reserved_for_test(), baseline + funded);
        drop(index);
        assert_eq!(node.reserved_for_test(), baseline);
    }
}

#[test]
fn denied_tree_growth_preserves_keys_and_funding_without_allocating() {
    let node = ResponseMemory::default();
    let memory = node.connection();
    let baseline = memory.reserved_for_test();
    let mut index = ResponseIndex::new();
    reserve(&mut index, 1, &memory);
    index.insert(7, 0);
    let funded = memory.reserved_for_test();
    let held = memory.try_reserve(16 * 1024 * 1024 - funded).unwrap();
    let (denied, allocations) = measure(|| {
        let plan = index.plan_capacity(2, false).unwrap().unwrap();
        memory.try_reserve(plan.bytes())
    });
    assert!(denied.is_none());
    assert_eq!(allocations.requests, 0);
    assert_eq!(index.find(7), ResponseMatch::Unique(0));
    assert!(index.plan_capacity(usize::MAX, false).is_err());
    drop(held);
    assert_eq!(memory.reserved_for_test(), funded);
    reserve(&mut index, 2, &memory);
    index.insert(8, 1);
    assert_eq!(index.find(7), ResponseMatch::Unique(0));
    drop(index);
    assert_eq!(memory.reserved_for_test(), baseline);
}

proptest! {
    #[test]
    fn generated_tree_churn_keeps_one_allowance_until_the_index_drops(
        actions in prop::collection::vec((0usize..64, any::<u8>(), any::<bool>()), 1..256),
    ) {
        let node = ResponseMemory::default();
        let memory = node.connection();
        let baseline = node.reserved_for_test();
        let mut index = ResponseIndex::new();
        reserve(&mut index, 64, &memory);
        let funded = node.reserved_for_test();
        // A fixed array keeps the independent oracle out of the allocation measurement.
        let mut owners = [None; 64];
        let (_, allocations) = measure(|| {
            for (owner, key, insert) in actions {
                if let Some(old) = owners[owner].take() { index.remove(old, owner); }
                if insert { index.insert(key, owner); owners[owner] = Some(key); }
                assert_eq!(node.reserved_for_test(), funded);
            }
        });
        prop_assert!(u64::try_from(allocations.peak_live_bytes).unwrap() <= funded - baseline);
        for (owner, key) in owners.into_iter().enumerate() {
            if let Some(key) = key { index.remove(key, owner); }
        }
        prop_assert_eq!(node.reserved_for_test(), funded);
        drop(index);
        prop_assert_eq!(node.reserved_for_test(), baseline);
    }
}
