//! The discovery book's eviction order, kept as ordered keys (#1990).
//!
//! The book updates this index on every change to an entry, under the same
//! lock. So the record count and the next eviction victim are answers from
//! the index alone, without a scan of the book.

use std::{cmp::Reverse, collections::BTreeSet};

use zakura_quic::NodeId;

use super::{ZakuraDiscoveryEntry, HIGH_DIAL_FAILURE_COUNT};

/// The book's non-static entries, in each eviction group's order.
///
/// A `NodeId` orders as its bytes, so the node ID in each key breaks ties
/// as `node_id_sort_key` does.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct EvictionIndex {
    /// Every non-static entry, earliest expiry first.
    by_expiry: BTreeSet<(u64, NodeId)>,
    /// The never-successful entries with at least `HIGH_DIAL_FAILURE_COUNT`
    /// failures. The last key goes first: most failures, then least recently
    /// seen, then lowest node ID.
    failing: BTreeSet<(u32, Reverse<u64>, Reverse<NodeId>)>,
    /// Every non-static entry: unconfirmed before confirmed, then least
    /// recently successful, then least recently seen, then lowest node ID.
    /// Unconfirmed entries go first, so relays cannot evict the confirmed
    /// entries that peer samples disclose.
    by_rank: BTreeSet<(bool, u64, u64, NodeId)>,
}

impl EvictionIndex {
    /// Adds `entry`'s keys. A static entry has none.
    pub(super) fn insert(&mut self, node_id: NodeId, entry: &ZakuraDiscoveryEntry) {
        if entry.is_static {
            return;
        }
        self.by_expiry.insert(expiry_key(node_id, entry));
        if let Some(key) = failing_key(node_id, entry) {
            self.failing.insert(key);
        }
        self.by_rank.insert(rank_key(node_id, entry));
    }

    /// Removes the keys [`Self::insert`] added for `entry`. The caller passes
    /// the entry as it was when it was inserted.
    pub(super) fn remove(&mut self, node_id: NodeId, entry: &ZakuraDiscoveryEntry) {
        if entry.is_static {
            return;
        }
        self.by_expiry.remove(&expiry_key(node_id, entry));
        if let Some(key) = failing_key(node_id, entry) {
            self.failing.remove(&key);
        }
        self.by_rank.remove(&rank_key(node_id, entry));
    }

    /// The number of non-static entries.
    pub(super) fn len(&self) -> usize {
        self.by_expiry.len()
    }

    /// Returns the next entry to evict at `now`, the first answer of
    /// [`Self::group_heads`].
    pub(super) fn candidate(&self, now: u64) -> Option<NodeId> {
        self.group_heads(now).flatten().next()
    }

    /// Each eviction group's answer at `now`, in eviction order. Each group
    /// reads one key: the earliest-expiring entry if it has expired, then the
    /// first failing entry, then the first entry by rank.
    pub(super) fn group_heads(&self, now: u64) -> impl Iterator<Item = Option<NodeId>> + '_ {
        (0..3).map(move |group| match group {
            0 => self
                .by_expiry
                .first()
                .filter(|(expires_at, _)| *expires_at < now)
                .map(|(_, node_id)| *node_id),
            1 => self.failing.last().map(|(_, _, node_id)| node_id.0),
            _ => self.by_rank.first().map(|(_, _, _, node_id)| *node_id),
        })
    }
}

fn expiry_key(node_id: NodeId, entry: &ZakuraDiscoveryEntry) -> (u64, NodeId) {
    (entry.record.body.expires_at_unix_secs, node_id)
}

fn failing_key(
    node_id: NodeId,
    entry: &ZakuraDiscoveryEntry,
) -> Option<(u32, Reverse<u64>, Reverse<NodeId>)> {
    (entry.last_success.is_none() && entry.failure_count >= HIGH_DIAL_FAILURE_COUNT).then_some((
        entry.failure_count,
        Reverse(entry.last_seen),
        Reverse(node_id),
    ))
}

fn rank_key(node_id: NodeId, entry: &ZakuraDiscoveryEntry) -> (bool, u64, u64, NodeId) {
    (
        entry.is_confirmed(),
        entry.last_success.unwrap_or(0),
        entry.last_seen,
        node_id,
    )
}
