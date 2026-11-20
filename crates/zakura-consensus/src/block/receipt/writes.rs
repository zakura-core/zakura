//! Matches local submissions to overlapping peer writes of the same complete block.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use zakura_chain::block;
use zakura_node_services::block_memory::BlockHandle;

/// Tracks only local submissions and peer bodies that have reached the state writer. A peer
/// can stop waiting after dispatch even though state still commits its block.
#[derive(Clone, Debug, Default)]
pub(in crate::block) struct ConcurrentWrites(Arc<Mutex<HashMap<block::Hash, Candidates>>>);

#[derive(Debug, Default)]
struct Candidates {
    peer_writes: HashMap<u64, BlockHandle>,
    submissions: HashMap<u64, Submission>,
}

#[derive(Debug)]
struct Submission {
    block: BlockHandle,
    saw_write: bool,
}

/// Removes this local submission or peer write when its caller finishes or is cancelled.
pub(in crate::block) struct WriteGuard {
    writes: ConcurrentWrites,
    hash: block::Hash,
    id: u64,
}

impl ConcurrentWrites {
    /// Starts observing peer writes before the local submission takes its receipt.
    pub(in crate::block) fn submit(&self, hash: block::Hash, block: &BlockHandle) -> WriteGuard {
        let guard = self.guard(hash);
        let mut active = self.active();
        let candidates = active.entry(hash).or_default();
        let saw_write = candidates.peer_writes.values().any(|peer| peer == block);
        candidates.submissions.insert(
            guard.id,
            Submission {
                block: block.clone(),
                saw_write,
            },
        );
        guard
    }

    /// Called immediately before dispatch to state. Only an identical local body can use this
    /// write as evidence of overlap, including when the receipt registry's body slots are full.
    pub(in crate::block) fn write(&self, hash: block::Hash, block: &BlockHandle) -> WriteGuard {
        let guard = self.guard(hash);
        let mut active = self.active();
        let candidates = active.entry(hash).or_default();
        for local in candidates.submissions.values_mut() {
            if !local.saw_write && local.block == *block {
                local.saw_write = true;
            }
        }
        candidates.peer_writes.insert(guard.id, block.clone());
        guard
    }

    fn active(&self) -> MutexGuard<'_, HashMap<block::Hash, Candidates>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn guard(&self, hash: block::Hash) -> WriteGuard {
        WriteGuard {
            writes: self.clone(),
            hash,
            id: super::order::next(),
        }
    }
}

impl WriteGuard {
    /// Whether a peer dispatched this local submission's complete body during their overlap.
    pub(in crate::block) fn saw_peer_write(&self) -> bool {
        let active = self.writes.active();
        active[&self.hash].submissions[&self.id].saw_write
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        let mut active = self.writes.active();
        let candidates = active
            .get_mut(&self.hash)
            .expect("a live guard owns its registration");
        candidates.submissions.remove(&self.id);
        candidates.peer_writes.remove(&self.id);
        if candidates.submissions.is_empty() && candidates.peer_writes.is_empty() {
            active.remove(&self.hash);
        }
    }
}
