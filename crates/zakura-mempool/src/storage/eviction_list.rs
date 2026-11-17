//! [`EvictionList`] represents the transaction eviction list with
//! efficient operations.
use std::{collections::VecDeque, time::Duration};

use zakura_chain::transaction;
use zakura_rt::collections::HashMap;

use crate::Instant;

/// An eviction list that allows Zebra to efficiently add entries, get entries,
/// and remove older entries in the order they were inserted.
pub struct EvictionList {
    // Maps each TXID in the list to the most recent instant they were added.
    unique_entries: HashMap<transaction::Hash, Instant>,
    // The same as `unique_entries` but in the order they were inserted.
    ordered_entries: VecDeque<transaction::Hash>,
    // The maximum size of `unique_entries`.
    max_size: usize,
    /// The mempool transaction eviction age limit.
    /// Same as [`Config::eviction_memory_time`][1].
    ///
    /// [1]: super::super::Config::eviction_memory_time
    eviction_memory_time: Duration,
}

impl EvictionList {
    /// Create a new [`EvictionList`] with the given maximum size and
    /// eviction time.
    pub fn new(max_size: usize, eviction_memory_time: Duration) -> Self {
        Self {
            unique_entries: Default::default(),
            ordered_entries: Default::default(),
            max_size,
            eviction_memory_time,
        }
    }

    /// Inserts a TXID in the list, keeping track of the time it was inserted.
    ///
    /// All entries older than [`EvictionList::eviction_memory_time`] will be removed.
    ///
    /// A TXID already in the list keeps its first entry, so its time and its
    /// place in the order stay in step.
    pub fn insert(&mut self, key: transaction::Hash, now: Instant) {
        // From https://zips.z.cash/zip-0401#specification:
        // > Nodes SHOULD remove transactions from RecentlyEvicted that were evicted more than
        // > mempoolevictionmemoryminutes minutes ago. This MAY be done periodically,
        // > and/or just before RecentlyEvicted is accessed when receiving a transaction.
        self.prune_old(now);
        // Today's pool never gives the list a TXID twice. The pool core's
        // chain events could, so a repeat is not a reason to panic.
        if self.unique_entries.contains_key(&key) {
            return;
        }
        // > Add the txid and the current time to RecentlyEvicted, dropping the oldest entry
        // > in RecentlyEvicted if necessary to keep it to at most eviction_memory_entries entries.
        if self.len() >= self.max_size {
            self.pop_front();
        }
        self.unique_entries.insert(key, now);
        self.ordered_entries.push_back(key)
    }

    /// Checks if the given TXID is in the list.
    pub fn contains_key(&self, txid: &transaction::Hash, now: Instant) -> bool {
        if let Some(evicted_at) = self.unique_entries.get(txid) {
            // Since the list is pruned only in mutable functions, make sure
            // we take expired items into account.
            if !self.has_expired(evicted_at, now) {
                return true;
            }
        }
        false
    }

    /// The number of entries, counting expired ones that
    /// [`Self::prune_old`] has not dropped yet.
    pub fn len(&self) -> usize {
        self.unique_entries.len()
    }

    /// Drops the TXIDs that expired at `now`.
    ///
    /// ZIP-401 allows this to happen periodically, and the pool core does it
    /// on every input.
    pub fn prune_old(&mut self, now: Instant) {
        while let Some(txid) = self.front() {
            let evicted_at = self
                .unique_entries
                .get(txid)
                .unwrap_or_else(|| panic!("all entries should exist in both ordered_entries and unique_entries, missing {txid:?} in unique_entries"));
            if self.has_expired(evicted_at, now) {
                self.pop_front();
            } else {
                break;
            }
        }
    }

    /// Get the oldest TXID in the list.
    fn front(&self) -> Option<&transaction::Hash> {
        self.ordered_entries.front()
    }

    /// Removes the first element and returns it, or `None` if the `EvictionList`
    /// is empty.
    fn pop_front(&mut self) -> Option<transaction::Hash> {
        if let Some(key) = self.ordered_entries.pop_front() {
            let removed = self.unique_entries.remove(&key);
            assert!(
                removed.is_some(),
                "all entries should exist in both ordered_entries and unique_entries, missing {key:?} in unique_entries"
            );
            Some(key)
        } else {
            None
        }
    }

    /// Returns if `evicted_at` is considered expired considering the current
    /// time and the configured eviction time.
    fn has_expired(&self, evicted_at: &Instant, now: Instant) -> bool {
        now.saturating_duration_since(*evicted_at) > self.eviction_memory_time
    }
}
