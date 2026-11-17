//! Submission-scoped permission for queued peer announcements and block bodies.

use std::{
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};
use zakura_chain::block::Block;
use zakura_rt::sync::watch;

use crate::block_memory::BlockHandle;

/// Whether temporary peer data still belongs to a live or successfully committed submission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingBlockStatus {
    /// Verification has not finished.
    Waiting,
    /// Verification and local commitment succeeded; active delivery may continue.
    Committed,
    /// Verification failed, timed out, or its owner was dropped.
    Failed,
}

/// Read-only permission to start writes from one particular submission attempt.
///
/// Clones share identity, but a retry with the same header hash gets a new permission.
#[derive(Clone, Debug)]
pub struct PendingBlockSendPermit(Arc<watch::Sender<PendingBlockStatus>>);

impl PendingBlockSendPermit {
    /// Creates a pending permission and its resolution sender.
    ///
    /// The submission owner must publish failure on cancellation or drop.
    pub fn channel() -> (watch::Sender<PendingBlockStatus>, Self) {
        let (status, _) = watch::channel(PendingBlockStatus::Waiting);
        (status.clone(), Self(Arc::new(status)))
    }

    /// Subscribes to the same decision used at the writer boundary.
    pub fn subscribe(&self) -> watch::Receiver<PendingBlockStatus> {
        self.0.subscribe()
    }

    /// Runs a synchronous write start unless this submission has been withdrawn.
    ///
    /// The watch read lock orders the callback against resolution's write lock, so resolution
    /// waits for the callback to return. Call only after writer readiness, keep the callback
    /// short and synchronous, and never resolve this permit's submission inside it.
    /// Writes that started before withdrawal may finish flushing afterward.
    pub fn try_start_write<T>(&self, start: impl FnOnce() -> T) -> Option<T> {
        let status = self.0.borrow();
        if *status == PendingBlockStatus::Failed {
            return None;
        }
        let result = start();
        drop(status);
        Some(result)
    }
}

impl PartialEq for PendingBlockSendPermit {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for PendingBlockSendPermit {}

impl Hash for PendingBlockSendPermit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

/// A peer response body, retaining withdrawal permission when read from temporary storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServedBlock {
    /// The retained body and its memory admission.
    pub block: BlockHandle,
    /// Permission for a temporary body; committed state reads need no extra permission.
    pub pending_send: Option<PendingBlockSendPermit>,
}

impl Deref for ServedBlock {
    type Target = BlockHandle;
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl From<BlockHandle> for ServedBlock {
    fn from(block: BlockHandle) -> Self {
        Self {
            block,
            pending_send: None,
        }
    }
}

impl From<Arc<Block>> for ServedBlock {
    fn from(block: Arc<Block>) -> Self {
        BlockHandle::from(block).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_withdraws_old_handles_without_poisoning_retry() {
        let (status, permit) = PendingBlockSendPermit::channel();
        let retained = permit.clone();
        assert_eq!(retained.try_start_write(|| 1), Some(1));
        status.send_replace(PendingBlockStatus::Failed);
        assert!(retained
            .try_start_write(|| panic!("withdrawn data cannot start a write"))
            .is_none());
        let (_, retry) = PendingBlockSendPermit::channel();
        assert_ne!(retained, retry);
        assert_eq!(retry.try_start_write(|| 2), Some(2));
        assert!(retained.try_start_write(|| ()).is_none());
    }

    #[test]
    fn commitment_preserves_permission_for_escaped_handles() {
        let (status, permit) = PendingBlockSendPermit::channel();
        status.send_replace(PendingBlockStatus::Committed);
        assert_eq!(permit.try_start_write(|| 3), Some(3));
    }

    #[test]
    fn resolution_is_ordered_after_a_write_that_already_started() {
        use std::{sync::mpsc, time::Duration};
        let (status, permit) = PendingBlockSendPermit::channel();
        let (attempted, attempt) = mpsc::channel();
        let (finished, finish) = mpsc::channel();
        let thread = permit
            .try_start_write(|| {
                let thread = std::thread::spawn(move || {
                    attempted.send(()).unwrap();
                    status.send_replace(PendingBlockStatus::Failed);
                    finished.send(()).unwrap();
                });
                attempt.recv_timeout(Duration::from_secs(5)).unwrap();
                assert!(
                    finish.try_recv().is_err(),
                    "resolution cannot overtake the write start"
                );
                thread
            })
            .unwrap();
        finish.recv_timeout(Duration::from_secs(5)).unwrap();
        thread.join().unwrap();
        assert!(permit.try_start_write(|| ()).is_none());
    }
}
