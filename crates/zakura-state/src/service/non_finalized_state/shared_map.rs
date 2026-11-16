//! Hash indexes shared between independent chain snapshots.

use std::{
    collections::{hash_map::RandomState, HashMap},
    hash::{BuildHasher, Hash},
    sync::Arc,
};

#[cfg(test)]
mod tests;

/// The number of hash partitions in a [`SharedMap`]. Writes copy only touched partitions.
pub(super) const PARTITIONS: usize = 256;

/// The number of partition groups. A snapshot shares all of them through one pointer, and a
/// write copies the group list and the touched group: two lists of 16 pointers, instead of
/// one list of 256 for every snapshot.
const GROUPS: usize = 16;

/// The number of partitions in each group.
const PARTITIONS_PER_GROUP: usize = PARTITIONS / GROUPS;

type Partition<K, V> = Arc<HashMap<K, V>>;
type Group<K, V> = Arc<[Partition<K, V>; PARTITIONS_PER_GROUP]>;

/// A partitioned hash map with independent snapshots.
///
/// Cloning shares every partition through one pointer. The first write to a shared
/// partition clones its entries and the two pointer lists above it, leaving other snapshots
/// unchanged.
///
/// Insertions replace existing values and return the previous value.
/// Equality compares contents; iteration order is unspecified.
#[derive(Clone, Debug)]
pub(crate) struct SharedMap<K, V> {
    // Clones must keep this seed so each key stays in the same partition.
    partition_hasher: RandomState,
    /// The partitions, in groups of consecutive partition indexes.
    groups: Arc<[Group<K, V>; GROUPS]>,
    len: usize,
}

impl<K, V> Default for SharedMap<K, V> {
    fn default() -> Self {
        // Empty slots share one allocation until their first write.
        let empty_partition: Partition<K, V> = Arc::new(HashMap::new());
        let empty_group: Group<K, V> = Arc::new(std::array::from_fn(|_| empty_partition.clone()));
        Self {
            partition_hasher: RandomState::new(),
            groups: Arc::new(std::array::from_fn(|_| empty_group.clone())),
            len: 0,
        }
    }
}

impl<K, V> SharedMap<K, V> {
    /// Returns the entries of partition `partition`.
    fn partition(&self, partition: usize) -> &HashMap<K, V> {
        &self.groups[partition / PARTITIONS_PER_GROUP][partition % PARTITIONS_PER_GROUP]
    }

    /// Returns every partition, in partition index order.
    fn partitions(&self) -> impl Iterator<Item = &Partition<K, V>> {
        self.groups.iter().flat_map(|group| group.iter())
    }
}

impl<K: Clone, V: Clone> SharedMap<K, V> {
    /// Returns the entries of partition `partition` for a write, first copying what other
    /// snapshots share: the group list, the partition's group, and the partition's entries.
    fn partition_mut(&mut self, partition: usize) -> &mut HashMap<K, V> {
        let group = &mut Arc::make_mut(&mut self.groups)[partition / PARTITIONS_PER_GROUP];
        Arc::make_mut(&mut Arc::make_mut(group)[partition % PARTITIONS_PER_GROUP])
    }
}

impl<K: Eq + Hash, V> SharedMap<K, V> {
    fn partition_index(&self, key: &K) -> usize {
        // Use a randomized hash rather than attacker-controlled key bytes.
        let mask = u64::try_from(PARTITIONS - 1).expect("the partition count fits in u64");
        usize::try_from(self.partition_hasher.hash_one(key) & mask)
            .expect("a partition index fits in usize")
    }

    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        self.partition(self.partition_index(key)).get(key)
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns how many partitions `self` and `other` share without a copy.
    #[cfg(test)]
    pub(crate) fn shared_partitions(&self, other: &Self) -> usize {
        self.partitions()
            .zip(other.partitions())
            .filter(|(mine, theirs)| Arc::ptr_eq(mine, theirs))
            .count()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.partitions().flat_map(|partition| partition.iter())
    }
}

impl<K: Eq + Hash + Clone, V: Clone> SharedMap<K, V> {
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let partition = self.partition_index(&key);
        let previous = self.partition_mut(partition).insert(key, value);
        if previous.is_none() {
            self.len += 1;
        }
        previous
    }

    /// Removes the entries in partition `partition` that `keep` rejects.
    ///
    /// Copies a shared partition only if it has an entry to remove.
    pub(crate) fn retain_partition(
        &mut self,
        partition: usize,
        mut keep: impl FnMut(&K, &V) -> bool,
    ) {
        if self
            .partition(partition)
            .iter()
            .all(|(key, value)| keep(key, value))
        {
            return;
        }
        let entries = self.partition_mut(partition);
        let before = entries.len();
        entries.retain(|key, value| keep(key, value));
        let removed = before - entries.len();
        self.len -= removed;
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let partition = self.partition_index(key);
        // Avoid copying a shared partition when the key is absent.
        if !self.partition(partition).contains_key(key) {
            return None;
        }
        let removed = self.partition_mut(partition).remove(key);
        if removed.is_some() {
            self.len -= 1;
        }
        removed
    }
}

impl<K: Eq + Hash, V: PartialEq> PartialEq for SharedMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        // Independently built indexes can partition the same entries differently.
        self.len == other.len
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

impl<K: Eq + Hash, V: Eq> Eq for SharedMap<K, V> {}

impl<K: Eq + Hash + Clone, V: Clone> FromIterator<(K, V)> for SharedMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(entries: I) -> Self {
        let mut map = Self::default();
        for (key, value) in entries {
            map.insert(key, value);
        }
        map
    }
}

#[cfg(test)]
impl<K: Eq + Hash, V> std::ops::Index<&K> for SharedMap<K, V> {
    type Output = V;

    fn index(&self, key: &K) -> &Self::Output {
        self.get(key)
            .expect("indexed keys were inserted by the test")
    }
}
