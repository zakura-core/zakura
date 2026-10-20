//! Ordered height indexes shared between independent chain snapshots.

use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU64,
    ops::{Bound, RangeBounds},
    sync::Arc,
};
use zakura_chain::block::Height;

const HEIGHTS_PER_CHUNK: u32 = 64;

/// The number of slots in a [`Chunk`]. The cast is lossless: 64 fits in `usize`.
const CHUNK_SLOTS: usize = HEIGHTS_PER_CHUNK as usize;

/// A private ordered index with copy-on-write 64-height chunks.
/// Snapshots copy the outer index; writes copy only touched chunks.
#[derive(Debug)]
pub(crate) struct HeightMap<V> {
    chunks: BTreeMap<u32, Arc<Chunk<V>>>,
}

/// The entries for 64 consecutive heights, in a slot per height.
///
/// A fixed array keeps lookups to an index and compiles far less code per value
/// type than a nested `BTreeMap`.
#[derive(Clone, PartialEq, Eq)]
struct Chunk<V> {
    /// The entry for height `chunk_id * 64 + i`, if any, in slot `i`.
    slots: [Option<(Height, V)>; CHUNK_SLOTS],
    /// Bit `i` is set when slot `i` holds an entry.
    occupied: u64,
}

impl<V> Default for Chunk<V> {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| None),
            occupied: 0,
        }
    }
}

impl<V: fmt::Debug> fmt::Debug for Chunk<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

fn chunk_id(height: Height) -> u32 {
    height.0 / HEIGHTS_PER_CHUNK
}

fn slot(height: Height) -> usize {
    // The cast is lossless: the remainder is less than 64.
    (height.0 % HEIGHTS_PER_CHUNK) as usize
}

impl<V> Chunk<V> {
    fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        // The cast is lossless: a `u64` has at most 64 set bits.
        self.occupied.count_ones() as usize
    }

    fn entry_at(&self, slot: u32) -> Option<(&Height, &V)> {
        // The cast is lossless: slots are less than 64.
        let (height, value) = self.slots[slot as usize].as_ref()?;
        Some((height, value))
    }

    fn first(&self) -> Option<(&Height, &V)> {
        self.entry_at(NonZeroU64::new(self.occupied)?.trailing_zeros())
    }

    fn last(&self) -> Option<(&Height, &V)> {
        self.entry_at(63 - NonZeroU64::new(self.occupied)?.leading_zeros())
    }

    fn get(&self, height: &Height) -> Option<&V> {
        self.slots[slot(*height)].as_ref().map(|(_, value)| value)
    }

    fn get_mut(&mut self, height: &Height) -> Option<&mut V> {
        self.slots[slot(*height)].as_mut().map(|(_, value)| value)
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&Height, &V)> {
        self.slots
            .iter()
            .filter_map(|entry| entry.as_ref().map(|(height, value)| (height, value)))
    }

    fn insert(&mut self, height: Height, value: V) -> Option<V> {
        let slot = slot(height);
        self.occupied |= 1 << slot;
        self.slots[slot]
            .replace((height, value))
            .map(|(_, value)| value)
    }

    fn remove(&mut self, height: &Height) -> Option<V> {
        let slot = slot(*height);
        let (_, value) = self.slots[slot].take()?;
        self.occupied &= !(1 << slot);
        Some(value)
    }

    /// Removes the entries at or below `height`, which must be in this chunk.
    fn remove_through(&mut self, height: Height) {
        let last = slot(height);
        for entry in &mut self.slots[..=last] {
            *entry = None;
        }
        // Keep only the bits above `last`; shifting by 64 would overflow.
        self.occupied &= if last == CHUNK_SLOTS - 1 {
            0
        } else {
            u64::MAX << (last + 1)
        };
    }

    #[cfg(test)]
    fn retain(&mut self, keep: &mut impl FnMut(&Height, &mut V) -> bool) {
        for entry in &mut self.slots {
            if let Some((height, value)) = entry {
                if !keep(height, value) {
                    self.occupied &= !(1 << slot(*height));
                    *entry = None;
                }
            }
        }
    }
}

impl<V> Default for HeightMap<V> {
    fn default() -> Self {
        Self {
            chunks: BTreeMap::new(),
        }
    }
}

impl<V> Clone for HeightMap<V> {
    fn clone(&self) -> Self {
        Self {
            chunks: self.chunks.clone(),
        }
    }
}

impl<V: PartialEq> PartialEq for HeightMap<V> {
    fn eq(&self, other: &Self) -> bool {
        self.chunks == other.chunks
    }
}
impl<V: Eq> Eq for HeightMap<V> {}

impl<V> HeightMap<V> {
    pub(super) fn get(&self, height: &Height) -> Option<&V> {
        self.chunks.get(&chunk_id(*height))?.get(height)
    }
    #[cfg(test)]
    pub(super) fn first_key_value(&self) -> Option<(&Height, &V)> {
        self.chunks.first_key_value()?.1.first()
    }
    pub(super) fn last_key_value(&self) -> Option<(&Height, &V)> {
        self.chunks.last_key_value()?.1.last()
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.chunks.values().map(|chunk| chunk.len()).sum()
    }
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
    #[cfg(test)]
    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = (&Height, &V)> {
        self.chunks.values().flat_map(|chunk| chunk.iter())
    }
    /// Returns entries in height order within `bounds`.
    ///
    /// # Panics
    ///
    /// Panics if the start exceeds the end, or if equal bounds are both excluded,
    /// even when the map is empty.
    pub(super) fn range<R: RangeBounds<Height>>(
        &self,
        bounds: R,
    ) -> impl DoubleEndedIterator<Item = (&Height, &V)> {
        let start = bounds.start_bound().cloned();
        let end = bounds.end_bound().cloned();
        if let (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) =
            (&start, &end)
        {
            assert!(a <= b, "range start exceeds range end");
            assert!(
                a != b || !matches!((&start, &end), (Bound::Excluded(_), Bound::Excluded(_))),
                "equal range bounds cannot both be excluded"
            );
        }
        let chunk_bound = |bound: &Bound<Height>| match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(height) | Bound::Excluded(height) => Bound::Included(chunk_id(*height)),
        };
        self.chunks
            .range((chunk_bound(&start), chunk_bound(&end)))
            .flat_map(|(_, chunk)| chunk.iter())
            .filter(move |(height, _)| (start, end).contains(*height))
    }
}

impl<V: Clone> HeightMap<V> {
    pub(super) fn insert(&mut self, height: Height, value: V) -> Option<V> {
        let chunk = self.chunks.entry(chunk_id(height)).or_default();
        Arc::make_mut(chunk).insert(height, value)
    }
    pub(super) fn remove(&mut self, height: &Height) -> Option<V> {
        let id = chunk_id(*height);
        let chunk = self.chunks.get_mut(&id)?;
        chunk.get(height)?;
        let removed = Arc::make_mut(chunk).remove(height);
        if chunk.is_empty() {
            self.chunks.remove(&id);
        }
        removed
    }
    pub(super) fn entry(&mut self, height: Height) -> Entry<'_, V> {
        Entry { map: self, height }
    }
    /// Removes every entry at or below `height`, copying at most the boundary chunk.
    pub(super) fn remove_through(&mut self, height: Height) {
        let boundary_chunk_id = chunk_id(height);
        // Keep the boundary chunk and every chunk above it, and drop the chunks below it.
        self.chunks = self.chunks.split_off(&boundary_chunk_id);
        if let Some(chunk) = self.chunks.get_mut(&boundary_chunk_id) {
            if chunk.first().is_some_and(|(key, _)| *key <= height) {
                Arc::make_mut(chunk).remove_through(height);
            }
            if chunk.is_empty() {
                self.chunks.remove(&boundary_chunk_id);
            }
        }
    }
    #[cfg(test)]
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&Height, &mut V) -> bool) {
        for chunk in self.chunks.values_mut() {
            Arc::make_mut(chunk).retain(&mut keep);
        }
        self.chunks.retain(|_, chunk| !chunk.is_empty());
    }
}

pub(super) struct Entry<'a, V> {
    map: &'a mut HeightMap<V>,
    height: Height,
}
impl<'a, V: Clone> Entry<'a, V> {
    pub(super) fn or_insert_with(self, make: impl FnOnce() -> V) -> &'a mut V {
        if self.map.get(&self.height).is_none() {
            // Compute before creating a chunk, so a panicking constructor leaves membership unchanged.
            let value = make();
            self.map.insert(self.height, value);
        }
        let chunk = self
            .map
            .chunks
            .get_mut(&chunk_id(self.height))
            .expect("the entry exists after lookup or insertion");
        Arc::make_mut(chunk)
            .get_mut(&self.height)
            .expect("the entry exists after lookup or insertion")
    }
}

/// Release-mode timings of [`HeightMap`] operations at non-finalized chain sizes.
#[cfg(feature = "internal-bench")]
pub mod benchmark {
    use std::{hint::black_box, sync::Arc, time::Instant};

    use zakura_chain::block::Height;

    use super::HeightMap;

    /// The median time of one benchmarked operation.
    #[derive(Clone, Debug)]
    pub struct HeightMapBenchmarkSample {
        /// The operation and value type measured.
        pub name: String,
        /// The median nanoseconds per operation over all samples.
        pub nanos_per_op: f64,
    }

    /// Times the operations a non-finalized chain performs on its height indexes,
    /// for a chain of `heights` blocks, with tree-like (`Arc`) and root-like
    /// (32-byte) values.
    pub fn benchmark_height_map(heights: u32, samples: usize) -> Vec<HeightMapBenchmarkSample> {
        let mut results = run::<Arc<u64>>("arc", heights, samples, |height| {
            Arc::new(u64::from(height))
        });
        results.extend(run::<[u8; 32]>("root", heights, samples, |height| {
            let mut root = [0; 32];
            root[..4].copy_from_slice(&height.to_le_bytes());
            root
        }));
        results
    }

    fn run<V: Clone>(
        value_type: &str,
        heights: u32,
        samples: usize,
        value: impl Fn(u32) -> V,
    ) -> Vec<HeightMapBenchmarkSample> {
        let full: HeightMap<V> = {
            let mut map = HeightMap::default();
            for height in 0..heights {
                map.insert(Height(height), value(height));
            }
            map
        };
        let with_tip = |tip: u32| {
            let mut map = HeightMap::default();
            for height in 0..=tip {
                map.insert(Height(height), value(height));
            }
            map
        };
        let tip_at_chunk_start = with_tip(heights / 64 * 64);
        let tip_at_chunk_end = with_tip(heights / 64 * 64 + 63);
        let per_op = |ops: u32, mut body: Box<dyn FnMut() + '_>| {
            let mut timings: Vec<f64> = (0..samples)
                .map(|_| {
                    let start = Instant::now();
                    body();
                    // Rounding nanosecond counts to f64 is harmless for timings.
                    start.elapsed().as_nanos() as f64 / f64::from(ops)
                })
                .collect();
            timings.sort_by(f64::total_cmp);
            timings[timings.len() / 2]
        };
        let sample = |op: &str, nanos_per_op| HeightMapBenchmarkSample {
            name: format!("{op}/{value_type}"),
            nanos_per_op,
        };

        vec![
            // Commit a block onto a snapshot of the chain.
            sample(
                "snapshot_and_push",
                per_op(
                    heights,
                    Box::new(|| {
                        let mut map = HeightMap::default();
                        for height in 0..heights {
                            let mut next = map.clone();
                            next.insert(Height(height), value(height));
                            map = black_box(next);
                        }
                    }),
                ),
            ),
            sample(
                "get",
                per_op(
                    heights,
                    Box::new(|| {
                        for height in 0..heights {
                            black_box(full.get(&Height(height)));
                        }
                    }),
                ),
            ),
            sample(
                "range_all",
                per_op(
                    heights,
                    Box::new(|| {
                        for entry in full.range(..) {
                            black_box(entry);
                        }
                    }),
                ),
            ),
            // Read the tip, at the first and the last slot of its chunk.
            sample(
                "last_key_value_chunk_start",
                per_op(
                    heights,
                    Box::new(|| {
                        let map = black_box(&tip_at_chunk_start);
                        for _ in 0..heights {
                            black_box(map.last_key_value());
                        }
                    }),
                ),
            ),
            sample(
                "last_key_value_chunk_end",
                per_op(
                    heights,
                    Box::new(|| {
                        let map = black_box(&tip_at_chunk_end);
                        for _ in 0..heights {
                            black_box(map.last_key_value());
                        }
                    }),
                ),
            ),
            // Finalize the root block of a snapshot, one block at a time.
            sample(
                "finalize_root",
                per_op(
                    heights,
                    Box::new(|| {
                        let mut map = full.clone();
                        for height in 0..heights {
                            map.remove_through(Height(height));
                        }
                        black_box(map);
                    }),
                ),
            ),
            // Pop the tip of a snapshot, as a fork or rollback does.
            sample(
                "pop_tip",
                per_op(
                    heights,
                    Box::new(|| {
                        let mut map = full.clone();
                        for height in (0..heights).rev() {
                            black_box(map.remove(&Height(height)));
                        }
                    }),
                ),
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;
    use zakura_test::prelude::*;
    fn assert_equal(actual: &HeightMap<u64>, expected: &BTreeMap<Height, u64>) {
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            actual.iter().rev().collect::<Vec<_>>(),
            expected.iter().rev().collect::<Vec<_>>()
        );
        assert_eq!(actual.last_key_value(), expected.last_key_value());
        assert!(actual.chunks.values().all(|chunk| !chunk.is_empty()));
        for chunk in actual.chunks.values() {
            for (index, entry) in chunk.slots.iter().enumerate() {
                assert_eq!(entry.is_some(), chunk.occupied & (1 << index) != 0);
            }
        }
        let mut actual_entries = actual.iter();
        let mut expected_entries = expected.iter();
        for index in 0..expected.len() {
            if index % 2 == 0 {
                assert_eq!(actual_entries.next(), expected_entries.next());
            } else {
                assert_eq!(actual_entries.next_back(), expected_entries.next_back());
            }
        }
        assert_eq!(actual_entries.next(), None);
        assert_eq!(actual_entries.next_back(), None);
    }

    proptest! {
        #[test]
        fn operations_and_snapshot_siblings_match_ordered_maps(
            operations in prop::collection::vec((any::<u8>(), any::<u32>(), any::<u64>()), 0..128)
        ) {
            let mut actual = HeightMap::default();
            let mut expected = BTreeMap::new();
            for (operation, raw_height, value) in operations {
                let height = Height(raw_height);
                let sibling = actual.clone();
                let expected_sibling = expected.clone();
                match operation % 4 {
                    0 | 1 => assert_eq!(actual.insert(height, value), expected.insert(height, value)),
                    2 => assert_eq!(actual.remove(&height), expected.remove(&height)),
                    _ => {
                        actual.remove_through(height);
                        expected.retain(|key, _| *key > height);
                    }
                }
                assert_equal(&actual, &expected);
                assert_equal(&sibling, &expected_sibling);
                assert_eq!(actual.get(&height), expected.get(&height));
                let lower = Height(raw_height / 2);
                for bounds in [(Bound::Included(lower), Bound::Included(height)),
                    (Bound::Excluded(lower), Bound::Included(height)),
                    (Bound::Unbounded, Bound::Excluded(height))] {
                    assert_eq!(actual.range(bounds).collect::<Vec<_>>(), expected.range(bounds).collect::<Vec<_>>());
                    assert_eq!(actual.range(bounds).rev().collect::<Vec<_>>(), expected.range(bounds).rev().collect::<Vec<_>>());
                }
            }
        }
    }

    proptest! {
        #[test]
        fn randomized_root_tip_and_fork_operations_preserve_duplicate_counts(
            operations in prop::collection::vec((any::<u8>(), any::<u32>(), 0u64..4), 1..256)
        ) {
            use super::super::counted_set::CountedSet;
            let mut actual = HeightMap::default();
            let mut expected = BTreeMap::new();
            let mut counts = CountedSet::new();
            for (operation, raw_height, root) in operations {
                let height = Height(if operation % 2 == 0 { raw_height % 2048 } else { raw_height });
                let sibling = actual.clone();
                let sibling_counts = counts.clone();
                let sibling_model = expected.clone();
                match operation % 4 {
                    0 | 1 => {
                        if let Some(previous) = actual.insert(height, root) { assert!(counts.remove(&previous)); }
                        counts.insert(root);
                        expected.insert(height, root);
                    }
                    2 => {
                        let removed: Vec<_> = actual.range(..=height).map(|(key, root)| (*key, *root)).collect();
                        for (key, root) in removed { assert_eq!(actual.remove(&key), Some(root)); assert!(counts.remove(&root)); }
                        expected.retain(|key, _| *key > height);
                    }
                    _ => {
                        if let Some((key, root)) = actual.last_key_value().map(|(key, root)| (*key, *root)) {
                            assert_eq!(actual.remove(&key), Some(root)); assert!(counts.remove(&root));
                            expected.remove(&key);
                        }
                    }
                }
                assert_equal(&actual, &expected);
                assert_equal(&sibling, &sibling_model);
                let mut expected_counts = CountedSet::new();
                for root in expected.values() { expected_counts.insert(*root); }
                assert_eq!(counts, expected_counts);
                let mut expected_sibling_counts = CountedSet::new();
                for root in sibling_model.values() { expected_sibling_counts.insert(*root); }
                assert_eq!(sibling_counts, expected_sibling_counts);
            }
        }
    }

    #[test]
    fn chunk_boundaries_ranges_and_fork_reversal_preserve_order() {
        let mut actual = HeightMap::default();
        let mut expected = BTreeMap::new();
        for raw_height in [0, 1, 62, 63, 64, 65, 127, 128, u32::MAX - 1, u32::MAX] {
            assert_eq!(
                actual.insert(Height(raw_height), u64::from(raw_height)),
                expected.insert(Height(raw_height), u64::from(raw_height))
            );
        }
        let sibling = actual.clone();
        assert_eq!(sibling, actual);
        assert_eq!(
            actual.insert(Height(64), 999),
            expected.insert(Height(64), 999)
        );
        assert_eq!(sibling.get(&Height(64)), Some(&64));
        for lower in [0, 63, 64, 127, u32::MAX] {
            for upper in [lower, u32::MAX] {
                for bounds in [
                    (
                        Bound::Included(Height(lower)),
                        Bound::Included(Height(upper)),
                    ),
                    (
                        Bound::Included(Height(lower)),
                        Bound::Excluded(Height(upper)),
                    ),
                    (
                        Bound::Excluded(Height(lower)),
                        Bound::Included(Height(upper)),
                    ),
                    (Bound::Unbounded, Bound::Included(Height(upper))),
                    (Bound::Included(Height(lower)), Bound::Unbounded),
                ] {
                    assert_eq!(
                        actual.range(bounds).collect::<Vec<_>>(),
                        expected.range(bounds).collect::<Vec<_>>()
                    );
                    assert_eq!(
                        actual.range(bounds).rev().collect::<Vec<_>>(),
                        expected.range(bounds).rev().collect::<Vec<_>>()
                    );
                }
            }
        }
        let mut fork = actual.clone();
        let mut expected_fork = expected.clone();
        for height in [u32::MAX, u32::MAX - 1, 128] {
            assert_eq!(
                fork.remove(&Height(height)),
                expected_fork.remove(&Height(height))
            );
        }
        fork.insert(Height(128), 500);
        expected_fork.insert(Height(128), 500);
        assert_equal(&fork, &expected_fork);
        assert_equal(&actual, &expected);
        for height in [0, 63, 64, 127, u32::MAX] {
            actual.remove_through(Height(height));
            expected.retain(|key, _| *key > Height(height));
            assert_equal(&actual, &expected);
        }
    }

    #[test]
    fn cloned_history_payloads_share_untouched_chunks() {
        let mut index = HeightMap::default();
        for height in 0..1024 {
            index.insert(Height(height), Arc::new(height));
        }
        let mut sibling = index.clone();
        assert_eq!(index.chunks.len(), 16);
        assert_eq!(Arc::strong_count(index.get(&Height(64)).unwrap()), 1);
        sibling.insert(Height(1024), Arc::new(1024));
        sibling.remove_through(Height(31));
        assert!(Arc::ptr_eq(
            index.chunks.get(&1).unwrap(),
            sibling.chunks.get(&1).unwrap()
        ));
        assert!(!Arc::ptr_eq(
            index.chunks.get(&0).unwrap(),
            sibling.chunks.get(&0).unwrap()
        ));
        assert!(index.get(&Height(0)).is_some());
        assert!(sibling.get(&Height(0)).is_none());
        assert_eq!(index.get(&Height(1024)), None);
        assert_eq!(sibling.get(&Height(1024)).map(|value| **value), Some(1024));
    }

    #[test]
    fn snapshot_and_boundary_updates_clone_at_most_two_chunks() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct Counted(Arc<AtomicUsize>);
        impl Clone for Counted {
            fn clone(&self) -> Self {
                self.0.fetch_add(1, Ordering::Relaxed);
                Self(self.0.clone())
            }
        }
        let copies = Arc::new(AtomicUsize::new(0));
        let mut flat = BTreeMap::new();
        let mut chunked = HeightMap::default();
        for raw in 0..1000 {
            flat.insert(Height(raw), Counted(copies.clone()));
            chunked.insert(Height(raw), Counted(copies.clone()));
        }
        // The old flat representation demonstrably exceeds the proposed budget.
        let mut flat_sibling = flat.clone();
        assert_eq!(copies.swap(0, Ordering::Relaxed), 1000);
        flat_sibling.insert(Height(1000), Counted(copies.clone()));
        flat_sibling.remove(&Height(0));
        let mut sibling = chunked.clone();
        assert_eq!(copies.load(Ordering::Relaxed), 0);
        sibling.insert(Height(1000), Counted(copies.clone()));
        sibling.remove(&Height(0));
        assert!(copies.load(Ordering::Relaxed) <= 2 * 64);
        assert!(chunked.get(&Height(0)).is_some());
        assert!(chunked.get(&Height(1000)).is_none());
        assert_eq!(
            sibling
                .iter()
                .map(|(height, _)| *height)
                .collect::<Vec<_>>(),
            flat_sibling.keys().copied().collect::<Vec<_>>()
        );
    }

    #[test]
    fn invalid_ranges_and_panicking_entry_preserve_snapshot_membership() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let mut actual = HeightMap::default();
        actual.insert(Height(63), 7);
        let sibling = actual.clone();
        for bounds in [
            (Bound::Included(Height(64)), Bound::Included(Height(63))),
            (Bound::Excluded(Height(63)), Bound::Excluded(Height(63))),
        ] {
            assert!(catch_unwind(|| actual.range(bounds).count()).is_err());
        }
        assert!(catch_unwind(AssertUnwindSafe(|| {
            actual
                .entry(Height(64))
                .or_insert_with(|| panic!("entry value construction rejected"));
        }))
        .is_err());
        assert_eq!(actual, sibling);
        assert!(actual.chunks.values().all(|chunk| !chunk.is_empty()));
        let mut evaluated = false;
        assert_eq!(
            *actual.entry(Height(63)).or_insert_with(|| {
                evaluated = true;
                8
            }),
            7
        );
        assert!(!evaluated);
        *actual.entry(Height(63)).or_insert_with(|| unreachable!()) = 9;
        assert_eq!(sibling.get(&Height(63)), Some(&7));
    }

    #[test]
    fn range_contract_is_independent_of_map_history() {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        fn check(actual: &HeightMap<u64>) {
            for (lower, upper) in [(64, 63), (65, 64), (u32::MAX, 0)] {
                for start in [
                    Bound::Included(Height(lower)),
                    Bound::Excluded(Height(lower)),
                ] {
                    for end in [
                        Bound::Included(Height(upper)),
                        Bound::Excluded(Height(upper)),
                    ] {
                        assert!(catch_unwind(|| actual.range((start, end))).is_err());
                    }
                }
            }
            for height in [0, 63, 64, u32::MAX] {
                let excluded = Bound::Excluded(Height(height));
                assert!(catch_unwind(|| actual.range((excluded, excluded))).is_err());
            }

            let expected: BTreeMap<_, _> =
                actual.iter().map(|(key, value)| (*key, *value)).collect();
            for bounds in [
                (Bound::Included(Height(63)), Bound::Included(Height(63))),
                (Bound::Included(Height(63)), Bound::Excluded(Height(63))),
                (Bound::Excluded(Height(63)), Bound::Included(Height(63))),
                (Bound::Unbounded, Bound::Unbounded),
            ] {
                assert_eq!(
                    actual.range(bounds).collect::<Vec<_>>(),
                    expected.range(bounds).collect::<Vec<_>>()
                );
                assert_eq!(
                    actual.range(bounds).rev().collect::<Vec<_>>(),
                    expected.range(bounds).rev().collect::<Vec<_>>()
                );
            }
        }

        let mut actual = HeightMap::default();
        check(&actual);
        check(&actual.clone());
        assert_eq!(actual.remove(&Height(63)), None);
        check(&actual);
        let _ = actual.entry(Height(63));
        check(&actual);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            actual
                .entry(Height(63))
                .or_insert_with(|| panic!("entry construction rejected"));
        }))
        .is_err());
        check(&actual);
        for raw in [63, 64, u32::MAX] {
            actual.insert(Height(raw), 7);
        }
        check(&actual);
        let sibling = actual.clone();
        check(&sibling);
        for raw in [63, 64, u32::MAX] {
            actual.remove(&Height(raw));
        }
        assert!(actual.is_empty());
        check(&actual);
        check(&actual.clone());
        check(&sibling);
        actual.insert(Height(63), 7);
        actual.retain(|_, _| false);
        assert!(actual.is_empty());
        check(&actual);
        check(&actual.clone());
    }
}
