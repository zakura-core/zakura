use super::*;
use proptest::prelude::*;
use std::{cell::Cell, cmp::Ordering, collections::HashMap};

#[test]
fn duplicate_keys_remain_ambiguous_until_one_owner_remains() {
    let mut index = ResponseIndex::new();
    index.insert(7, 0);
    index.insert(7, 4);
    index.insert(7, 9);
    assert_eq!(index.find(7), ResponseMatch::Ambiguous);
    index.remove(7, 4);
    assert_eq!(index.find(7), ResponseMatch::Ambiguous);
    index.remove(7, 0);
    assert_eq!(index.find(7), ResponseMatch::Unique(9));
    index.remove(7, 9);
    assert_eq!(index.find(7), ResponseMatch::Missing);
    assert!(index.entries.is_empty());
}

proptest! {
    #[test]
    fn generated_updates_and_removals_match_live_owners(
        history in prop::collection::vec((0usize..64, 0u8..16, any::<bool>()), 0..256),
    ) {
        let mut index = ResponseIndex::new();
        let mut owners = HashMap::new();
        for (owner, key, insert) in history {
            if let Some(old) = owners.remove(&owner) {
                index.remove(old, owner);
            }
            if insert {
                owners.insert(owner, key);
                index.insert(key, owner);
            }
            prop_assert_eq!(index.entries.len(), owners.len());
            for query in 0..=16 {
                let matches: Vec<_> = owners.iter().filter_map(|(owner, key)| (*key == query).then_some(*owner)).collect();
                let expected = match matches.as_slice() {
                    [] => ResponseMatch::Missing,
                    [owner] => ResponseMatch::Unique(*owner),
                    _ => ResponseMatch::Ambiguous,
                };
                prop_assert_eq!(index.find(query), expected);
            }
        }
    }
}

thread_local! {
    static COMPARISONS: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CountedKey(u32);

impl Ord for CountedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        COMPARISONS.with(|count| count.set(count.get() + 1));
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for CountedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[test]
fn full_window_lookups_and_removals_do_not_scan_every_owner() {
    // Count key comparisons instead of relying on machine speed or wall time.
    const REQUESTS: u32 = 32_768;
    let mut index = ResponseIndex::new();
    for owner in 0..REQUESTS {
        index.insert(CountedKey(owner), usize::try_from(owner).unwrap());
    }
    COMPARISONS.with(|count| count.set(0));
    for owner in 0..REQUESTS {
        let position = usize::try_from(owner).unwrap();
        assert_eq!(
            index.find(CountedKey(owner)),
            ResponseMatch::Unique(position)
        );
        index.remove(CountedKey(owner), position);
    }
    let comparisons = COMPARISONS.with(Cell::get);
    // A linear search over the shrinking window would exceed 500 million.
    // This permits 256 comparisons per lookup/removal pair, well above the
    // balanced tree's actual work without depending on its exact node layout.
    assert!(
        comparisons < usize::try_from(REQUESTS).unwrap() * 256,
        "full-window lookup/removal used {comparisons} key comparisons"
    );
    assert!(index.entries.is_empty());
}

#[test]
fn response_lookup_does_not_allocate() {
    let mut index = ResponseIndex::new();
    index.insert(7, 0);
    index.insert(8, 1);
    index.insert(8, 2);
    let (matches, allocations) =
        zakura_test::allocations::measure(|| [index.find(7), index.find(8), index.find(9)]);
    assert_eq!(
        matches,
        [
            ResponseMatch::Unique(0),
            ResponseMatch::Ambiguous,
            ResponseMatch::Missing
        ]
    );
    assert_eq!(allocations.requests, 0);
}
