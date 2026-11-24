//! Submission-scoped permission for queued peer announcements and block bodies.

use std::{
    hash::{Hash, Hasher},
    ops::Deref,
    sync::{Arc, OnceLock},
};
use zakura_chain::serialization::sha256d;
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
pub struct PendingBlockSendPermit {
    status: Arc<watch::Sender<PendingBlockStatus>>,
    checksum: Option<AttemptChecksum>,
}

impl PendingBlockSendPermit {
    /// Creates a pending permission and its resolution sender.
    ///
    /// The submission owner must publish failure on cancellation or drop.
    pub fn channel() -> (watch::Sender<PendingBlockStatus>, Self) {
        Self::with_checksum(Some(AttemptChecksum::default()))
    }

    /// Creates a revocable permission carrying the exact bytes authorized for direct peer reads.
    ///
    /// `bytes` must be the serialization of the block this permission is registered with:
    /// transports write them without consulting the block.
    pub fn prepared_channel(bytes: Arc<[u8]>) -> (watch::Sender<PendingBlockStatus>, Self) {
        Self::with_checksum(Some(AttemptChecksum {
            prepared: Some(bytes),
            ..Default::default()
        }))
    }

    /// Creates a revocable permission for a body whose commitments were checked before
    /// state admission. Legacy sends can safely share its checksum by header hash.
    pub fn state_admitted_channel() -> (watch::Sender<PendingBlockStatus>, Self) {
        Self::with_checksum(None)
    }

    fn with_checksum(
        checksum: Option<AttemptChecksum>,
    ) -> (watch::Sender<PendingBlockStatus>, Self) {
        let (status, _) = watch::channel(PendingBlockStatus::Waiting);
        (
            status.clone(),
            Self {
                status: Arc::new(status),
                checksum,
            },
        )
    }

    /// Subscribes to the same decision used at the writer boundary.
    pub fn subscribe(&self) -> watch::Receiver<PendingBlockStatus> {
        self.status.subscribe()
    }

    /// The attempt-local checksum, if the body has not been authorized for shared caching.
    ///
    /// One permit belongs to one submitted body, so its sends can share a checksum without the
    /// cache keyed by header hash, where a different body under the same hash could poison it.
    pub fn attempt_checksum(&self) -> Option<&AttemptChecksum> {
        self.checksum.as_ref()
    }

    /// The exact serialized body, when the submission supplied it for direct peer writes.
    ///
    /// A transport writes these bytes in place of serializing the block. Whoever creates the
    /// permit guarantees that they are the block's serialization.
    pub fn prepared_bytes(&self) -> Option<&[u8]> {
        self.checksum.as_ref()?.prepared_bytes()
    }

    /// Runs a synchronous write start unless this submission has been withdrawn.
    ///
    /// The watch read lock orders the callback against resolution's write lock, so resolution
    /// waits for the callback to return. Call only after writer readiness, keep the callback
    /// short and synchronous, and never resolve this permit's submission inside it.
    /// Writes that started before withdrawal may finish flushing afterward.
    pub fn try_start_write<T>(&self, start: impl FnOnce() -> T) -> Option<T> {
        let status = self.status.borrow();
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
        Arc::ptr_eq(&self.status, &other.status)
    }
}

impl Eq for PendingBlockSendPermit {}

impl Hash for PendingBlockSendPermit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.status).hash(state);
    }
}

/// What the sends of one submission attempt share: the checksum of its exact serialized body,
/// computed by its first send, and that body itself when the submission supplied it.
///
/// Identity is the attempt, so clones are equal and a retry of the same block is not.
#[derive(Clone, Default)]
pub struct AttemptChecksum {
    checksum: Arc<OnceLock<(usize, sha256d::Checksum)>>,
    /// The attempt's serialized body, written to peers in place of serializing the block.
    prepared: Option<Arc<[u8]>>,
}

impl std::fmt::Debug for AttemptChecksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttemptChecksum")
            .field("prepared_len", &self.prepared_bytes().map(<[u8]>::len))
            .finish_non_exhaustive()
    }
}

impl AttemptChecksum {
    /// The attempt's exact serialized body, when the submission supplied it.
    pub fn prepared_bytes(&self) -> Option<&[u8]> {
        self.prepared.as_deref()
    }

    /// Returns the checksum of `body`, computing it on the attempt's first send.
    ///
    /// The length guards the invariant that an attempt serializes to one body: a mismatch is
    /// computed afresh rather than trusted.
    pub fn get_or_compute(
        &self,
        body: &[u8],
        compute: impl Fn(&[u8]) -> sha256d::Checksum,
    ) -> sha256d::Checksum {
        let (len, checksum) = *self.checksum.get_or_init(|| (body.len(), compute(body)));
        if len == body.len() {
            checksum
        } else {
            compute(body)
        }
    }
}

impl PartialEq for AttemptChecksum {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.checksum, &other.checksum)
    }
}

impl Eq for AttemptChecksum {}

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

impl<T: Into<BlockHandle>> From<T> for ServedBlock {
    fn from(block: T) -> Self {
        Self {
            block: block.into(),
            pending_send: None,
        }
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
    fn prepared_bytes_follow_the_permit_and_its_withdrawal() {
        let bytes: Arc<[u8]> = Arc::from(&b"exact body"[..]);
        let (status, permit) = PendingBlockSendPermit::prepared_channel(bytes.clone());
        assert_eq!(permit.prepared_bytes(), Some(&bytes[..]));
        assert_eq!(permit.clone().prepared_bytes(), Some(&bytes[..]));
        assert!(format!("{permit:?}").contains("prepared_len: Some(10)"));
        status.send_replace(PendingBlockStatus::Failed);
        assert!(permit
            .try_start_write(|| panic!("withdrawn prepared bytes cannot start a write"))
            .is_none());

        let (_, serialized) = PendingBlockSendPermit::channel();
        assert_eq!(serialized.prepared_bytes(), None);
        let (_, admitted) = PendingBlockSendPermit::state_admitted_channel();
        assert_eq!(admitted.prepared_bytes(), None);
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
