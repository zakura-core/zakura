//! Defines the [`MempoolChange`] and [`MempoolChangeKind`] types used by the mempool change broadcast channel.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use tokio::sync::broadcast;
use zakura_chain::transaction::UnminedTxId;

/// A newtype around [`broadcast::Sender<MempoolChange>`] used to
/// subscribe to the channel without an active receiver.
#[derive(Clone, Debug)]
pub struct MempoolTxSubscriber {
    sender: broadcast::Sender<MempoolChange>,
    freshness: Arc<MempoolFreshness>,
}

#[derive(Debug, Default)]
struct MempoolFreshness {
    updating: AtomicBool,
    validity: AtomicU64,
}

/// Prevents cached template use while the mempool mutates its verified set.
pub struct MempoolUpdate(Arc<MempoolFreshness>);

impl Drop for MempoolUpdate {
    fn drop(&mut self) {
        self.0.updating.store(false, Ordering::Release);
    }
}

impl MempoolTxSubscriber {
    /// Creates a new [`MempoolTxSubscriber`].
    pub fn new(sender: broadcast::Sender<MempoolChange>) -> Self {
        Self {
            sender,
            freshness: Arc::default(),
        }
    }

    /// Begins a serialized service update. The guard must cover all verified-set mutations.
    pub fn begin_update(&self) -> MempoolUpdate {
        self.freshness.updating.store(true, Ordering::Release);
        MempoolUpdate(self.freshness.clone())
    }

    /// The current validity revision, or no revision while a service update is in progress.
    pub fn validity_revision(&self) -> Option<u64> {
        let revision = self.freshness.validity.load(Ordering::Acquire);
        (!self.freshness.updating.load(Ordering::Acquire)
            && self.freshness.validity.load(Ordering::Acquire) == revision)
            .then_some(revision)
    }

    /// Invalidates snapshots, including when a reset removes an already empty set.
    pub fn invalidate(&self) {
        self.freshness
            .validity
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                revision.checked_add(1)
            })
            .expect("mempool revision cannot exhaust u64 during a node lifetime");
    }

    /// Publishes a change and retains invalidation even when no receiver is listening.
    pub fn send(
        &self,
        change: MempoolChange,
    ) -> Result<usize, broadcast::error::SendError<MempoolChange>> {
        if !change.is_added() {
            self.invalidate();
        }
        self.sender.send(change)
    }

    /// Subscribes to the channel, returning a [`broadcast::Receiver`].
    pub fn subscribe(&self) -> broadcast::Receiver<MempoolChange> {
        self.sender.subscribe()
    }
}

/// Represents a kind of change in the mempool's verified set of transactions
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MempoolChangeKind {
    /// Transactions were added to the mempool.
    Added,
    /// Transactions were invalidated or could not be verified and were rejected from the mempool.
    Invalidated,
    /// Transactions were mined onto the best chain and removed from the mempool.
    Mined,
}

/// Represents a change in the mempool's verified set of transactions
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MempoolChange {
    /// The kind of change that occurred in the mempool.
    pub kind: MempoolChangeKind,
    /// The set of [`UnminedTxId`]s of transactions that were affected by the change.
    pub tx_ids: HashSet<UnminedTxId>,
}

impl MempoolChange {
    /// Creates a new [`MempoolChange`] with the specified kind and transaction IDs.
    pub fn new(kind: MempoolChangeKind, tx_ids: HashSet<UnminedTxId>) -> Self {
        Self { kind, tx_ids }
    }

    /// Returns the kind of change that occurred in the mempool.
    pub fn kind(&self) -> MempoolChangeKind {
        self.kind
    }

    /// Returns true if the kind of change that occurred in the mempool is [`MempoolChangeKind::Added`].
    pub fn is_added(&self) -> bool {
        self.kind == MempoolChangeKind::Added
    }

    /// Consumes self and returns the set of [`UnminedTxId`]s of transactions that were affected by the change.
    pub fn into_tx_ids(self) -> HashSet<UnminedTxId> {
        self.tx_ids
    }

    /// Returns a reference to the set of [`UnminedTxId`]s of transactions that were affected by the change.
    pub fn tx_ids(&self) -> &HashSet<UnminedTxId> {
        &self.tx_ids
    }

    /// Creates a new [`MempoolChange`] indicating that transactions were added to the mempool.
    pub fn added(tx_ids: HashSet<UnminedTxId>) -> Self {
        Self::new(MempoolChangeKind::Added, tx_ids)
    }

    /// Creates a new [`MempoolChange`] indicating that transactions were invalidated or rejected from the mempool.
    pub fn invalidated(tx_ids: HashSet<UnminedTxId>) -> Self {
        Self::new(MempoolChangeKind::Invalidated, tx_ids)
    }

    /// Creates a new [`MempoolChange`] indicating that transactions were mined onto the best chain and
    /// removed from the mempool.
    pub fn mined(tx_ids: HashSet<UnminedTxId>) -> Self {
        Self::new(MempoolChangeKind::Mined, tx_ids)
    }
}

#[cfg(test)]
mod freshness_tests {
    use super::*;

    #[test]
    fn additions_preserve_validity_and_invalidations_survive_without_receivers() {
        let (sender, _) = broadcast::channel(1);
        let changes = MempoolTxSubscriber::new(sender);
        let original = changes.validity_revision().unwrap();
        {
            let _update = changes.begin_update();
            assert_eq!(changes.validity_revision(), None);
            let _ = changes.send(MempoolChange::added(HashSet::new()));
        }
        assert_eq!(changes.validity_revision(), Some(original));
        {
            let _update = changes.begin_update();
            let _ = changes.send(MempoolChange::invalidated(HashSet::new()));
        }
        assert_ne!(changes.validity_revision(), Some(original));
    }
}
