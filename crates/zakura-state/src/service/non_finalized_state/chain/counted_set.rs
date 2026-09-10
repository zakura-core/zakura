//! A set that tracks the multiplicity of each distinct value.

use std::{collections::HashMap, hash::Hash};

/// Stores one count for each distinct value.
///
/// Removing a value decrements its count and only removes the distinct value
/// after its final occurrence is removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CountedSet<T: Eq + Hash> {
    counts: HashMap<T, usize>,
}

impl<T> CountedSet<T>
where
    T: Eq + Hash,
{
    /// Creates an empty counted set.
    pub(crate) fn new() -> Self {
        Self {
            counts: HashMap::new(),
        }
    }

    /// Adds one occurrence of `value`.
    pub(crate) fn insert(&mut self, value: T) {
        let count = self.counts.entry(value).or_default();
        *count = count
            .checked_add(1)
            .expect("a counted set cannot contain more values than usize::MAX");
    }

    /// Removes one occurrence of `value`, returning whether it was present.
    pub(crate) fn remove(&mut self, value: &T) -> bool {
        let Some(count) = self.counts.get_mut(value) else {
            return false;
        };

        if *count > 1 {
            *count -= 1;
            return true;
        }

        self.counts.remove(value);
        true
    }

    /// Returns whether at least one occurrence of `value` is present.
    pub(crate) fn contains(&self, value: &T) -> bool {
        self.counts.contains_key(value)
    }

    /// Returns whether the set contains no values.
    pub(crate) fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    /// Iterates over each distinct value once.
    pub(crate) fn distinct_values(&self) -> impl Iterator<Item = &T> {
        self.counts.keys()
    }
}

impl<T> Default for CountedSet<T>
where
    T: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::CountedSet;

    #[test]
    fn removal_preserves_multiplicity() {
        let mut values = CountedSet::new();

        values.insert("repeated");
        values.insert("repeated");
        values.insert("distinct");

        assert!(values.remove(&"repeated"));
        assert!(values.contains(&"repeated"));
        assert!(values.remove(&"repeated"));
        assert!(!values.contains(&"repeated"));
        assert!(!values.remove(&"repeated"));

        assert_eq!(
            values.distinct_values().copied().collect::<HashSet<_>>(),
            HashSet::from(["distinct"])
        );
        assert!(!values.is_empty());
        assert!(values.remove(&"distinct"));
        assert!(values.is_empty());
    }
}
