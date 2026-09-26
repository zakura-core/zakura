//! Find a response's owner without scanning every live request.
//!
//! Each entry pairs a message's matching key with its owner's position. Keeping
//! duplicate keys lets callers reject ambiguous responses instead of choosing
//! whichever request happened to be inserted first.
//! The message adapter supplies the keys and decides how to handle each match.

use std::collections::BTreeSet;

mod funding;
use super::ResponseMemoryPermit;
pub(crate) use funding::ResponseIndexPlan;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ResponseMatch {
    Missing,
    Unique(usize),
    Ambiguous,
}

#[cfg(test)]
mod tests;

/// One entry per live matching key. Lookups and updates take logarithmic work.
/// The owner must remove or move entries when its request storage changes.
#[derive(Debug)]
pub(crate) struct ResponseIndex<K> {
    entries: BTreeSet<(K, usize)>,
    funded_entries: usize,
    // Drop the tree before releasing the allowance for its nodes.
    funding: Option<ResponseMemoryPermit>,
}

impl<K: Copy + Ord> ResponseIndex<K> {
    pub(crate) fn new() -> Self {
        Self {
            entries: BTreeSet::new(),
            funded_entries: 0,
            funding: None,
        }
    }

    pub(crate) fn insert(&mut self, key: K, position: usize) {
        assert!(
            self.entries.len() < self.funded_entries,
            "index growth is funded before publication"
        );
        assert!(
            self.entries.insert((key, position)),
            "each response key is indexed once per owner"
        );
    }

    pub(crate) fn remove(&mut self, key: K, position: usize) {
        assert!(
            self.entries.remove(&(key, position)),
            "a live response key has an index entry"
        );
    }

    pub(crate) fn find(&self, key: K) -> ResponseMatch {
        let mut matches = self.entries.range((key, 0)..=(key, usize::MAX));
        match (matches.next(), matches.next()) {
            (None, _) => ResponseMatch::Missing,
            (Some((_, position)), None) => ResponseMatch::Unique(*position),
            (Some(_), Some(_)) => ResponseMatch::Ambiguous,
        }
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}
