//! Hash indexes shared between independent chain snapshots.

use std::{
    borrow::Borrow,
    collections::{hash_map::RandomState, HashMap},
    hash::{BuildHasher, Hash},
    sync::Arc,
};

#[cfg(test)]
mod tests;

// Bound snapshot cloning to 256 Arc increments. Writes copy only touched partitions.
const PARTITIONS: usize = 256;

/// A private copy-on-write hash index. Clones share partitions but keep independent membership.
#[derive(Clone, Debug)]
pub(crate) struct SharedMap<K, V> {
    // Clones must keep this seed so each key stays in the same partition.
    hash: RandomState,
    partitions: Vec<Arc<HashMap<K, V>>>,
    len: usize,
}

impl<K, V> Default for SharedMap<K, V> {
    fn default() -> Self {
        Self {
            hash: RandomState::new(),
            partitions: (0..PARTITIONS).map(|_| Arc::new(HashMap::new())).collect(),
            len: 0,
        }
    }
}

impl<K: Eq + Hash, V> SharedMap<K, V> {
    fn partition<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // Use a randomized hash rather than attacker-controlled key bytes.
        let mask = u64::try_from(PARTITIONS - 1).expect("the partition count fits in u64");
        usize::try_from(self.hash.hash_one(key) & mask).expect("a partition index fits in usize")
    }

    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.partitions[self.partition(key)].get(key)
    }

    pub(crate) fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.partitions
            .iter()
            .flat_map(|partition| partition.iter())
    }
}

impl<K: Eq + Hash + Clone, V: Clone> SharedMap<K, V> {
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let partition = self.partition(&key);
        let previous = Arc::make_mut(&mut self.partitions[partition]).insert(key, value);
        if previous.is_none() {
            self.len += 1;
        }
        previous
    }

    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let partition = self.partition(key);
        // Avoid copying a shared partition when the key is absent.
        if !self.partitions[partition].contains_key(key) {
            return None;
        }
        let removed = Arc::make_mut(&mut self.partitions[partition]).remove(key);
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
