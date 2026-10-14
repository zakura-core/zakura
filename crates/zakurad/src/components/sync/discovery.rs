//! Bounded provenance for hashes admitted from block discovery.

use std::collections::{HashMap, HashSet};
use tokio::time::Instant;
use zakura_chain::block::Hash;
use zakura_network::DiscoveryFeedback;

pub(super) type Evidence = (Vec<Hash>, DiscoveryFeedback);
const MAX_RESPONSES: usize = 64;
const MAX_HASHES: usize = 500;

#[derive(Default)]
pub(super) struct Discovery {
    next_id: u64,
    reconcile_at: Option<Instant>,
    reconcile_cursor: usize,
    responses: HashMap<u64, (Instant, Evidence)>,
    by_hash: HashMap<Hash, HashSet<u64>>,
}

impl Discovery {
    pub(super) fn admit(&mut self, records: Vec<Evidence>, admitted: &indexmap::IndexSet<Hash>) {
        self.expire();
        for (mut hashes, feedback) in records {
            if self.responses.len() >= MAX_RESPONSES {
                break;
            }
            hashes.retain(|hash| admitted.contains(hash));
            hashes.truncate(MAX_HASHES);
            if hashes.is_empty() {
                continue;
            }
            let id = self.next_id;
            self.next_id = self
                .next_id
                .checked_add(1)
                .expect("discovery IDs cannot exhaust in a process lifetime");
            for hash in &hashes {
                self.by_hash.entry(*hash).or_default().insert(id);
            }
            self.responses
                .insert(id, (Instant::now(), (hashes, feedback)));
        }
    }

    pub(super) fn reconciliation_batch(&mut self) -> Vec<Hash> {
        if self.reconcile_at.is_some_and(|at| Instant::now() < at) || self.by_hash.is_empty() {
            return Vec::new();
        }
        self.reconcile_at = Some(Instant::now() + std::time::Duration::from_secs(1));
        let count = self.by_hash.len().min(64);
        self.reconcile_cursor %= self.by_hash.len();
        let hashes = self
            .by_hash
            .keys()
            .cycle()
            .skip(self.reconcile_cursor)
            .take(count)
            .copied()
            .collect();
        self.reconcile_cursor += count;
        hashes
    }

    pub(super) fn committed(&mut self, hash: Hash) {
        if let Some(ids) = self.by_hash.get(&hash).cloned() {
            for id in ids {
                if let Some((_, (_, feedback))) = self.remove(id) {
                    feedback.verified();
                }
            }
        }
    }

    fn remove(&mut self, id: u64) -> Option<(Instant, Evidence)> {
        let record = self.responses.remove(&id)?;
        for hash in &record.1 .0 {
            if let Some(ids) = self.by_hash.get_mut(hash) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.by_hash.remove(hash);
                }
            }
        }
        Some(record)
    }

    /// Resolves every retained response when the round discards its evidence.
    ///
    /// Dropping the records completes them as neutral, which neither scores the peer nor
    /// rotates it out, so an aborted round would let the same peers refill the next one
    /// with hashes that never commit.
    pub(super) fn abandon(&mut self) {
        for (_, (_, feedback)) in std::mem::take(&mut self.responses).into_values() {
            feedback.expired();
        }
        self.by_hash.clear();
        self.reconcile_at = None;
        self.reconcile_cursor = 0;
    }

    pub(super) fn expire(&mut self) {
        let expired: Vec<_> = self
            .responses
            .iter()
            .filter(|(_, (at, _))| at.elapsed() >= super::BLOCK_VERIFY_TIMEOUT * 2)
            .map(|(&id, _)| id)
            .collect();
        for id in expired {
            if let Some((_, (_, feedback))) = self.remove(id) {
                feedback.expired();
            }
        }
    }
}
