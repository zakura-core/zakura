//! Cancellation before a commit owner admits work to its write pipeline.

use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    Verifying,
    WaitingForParent,
    WaitingForCheckpointRange,
    Writing,
    Cancelled,
}

#[derive(Debug)]
struct Inner {
    stage: Mutex<Stage>,
    cancelled: Notify,
}

/// Fences a pending commit against cancellation.
/// Owners must claim this fence before admitting any state mutation.
/// Create a fresh fence for each request and share its clones with the request owner.
#[derive(Clone, Debug)]
pub struct CommitCancellation(Arc<Inner>);

impl PartialEq for CommitCancellation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CommitCancellation {}

impl Default for CommitCancellation {
    fn default() -> Self {
        Self(Arc::new(Inner {
            stage: Mutex::new(Stage::Verifying),
            cancelled: Notify::new(),
        }))
    }
}

impl CommitCancellation {
    /// Records the state queue's dependency wait without changing write authority.
    pub fn waiting_for_parent(&self) {
        self.set_pending_stage(Stage::WaitingForParent);
    }

    /// Records the checkpoint verifier's range wait without changing write authority.
    pub fn waiting_for_checkpoint_range(&self) {
        self.set_pending_stage(Stage::WaitingForCheckpointRange);
    }

    fn set_pending_stage(&self, next: Stage) {
        let mut stage = self
            .0
            .stage
            .lock()
            .expect("commit fence lock is not poisoned");
        if !matches!(*stage, Stage::Writing | Stage::Cancelled) {
            *stage = next;
        }
    }

    /// Returns the commit owner's current processing stage for diagnostics.
    pub fn stage(&self) -> &'static str {
        match *self
            .0
            .stage
            .lock()
            .expect("commit fence lock is not poisoned")
        {
            Stage::Verifying => "verifying",
            Stage::WaitingForParent => "waiting_for_parent",
            Stage::WaitingForCheckpointRange => "waiting_for_checkpoint_range",
            Stage::Writing => "writing",
            Stage::Cancelled => "cancelled",
        }
    }

    /// Acknowledges cancellation unless the owner has already admitted the commit.
    pub fn cancel(&self) -> bool {
        let mut stage = self
            .0
            .stage
            .lock()
            .expect("commit fence lock is not poisoned");
        if *stage == Stage::Writing {
            return false;
        }
        *stage = Stage::Cancelled;
        self.0.cancelled.notify_waiters();
        true
    }

    /// Claims a complete checkpoint range, or one full-verification commit, atomically.
    /// A cancelled member prevents the owner from admitting any member of the range.
    pub fn try_start_batch(tokens: &[Self]) -> bool {
        let mut tokens: Vec<_> = tokens.iter().collect();
        tokens.sort_unstable_by_key(|token| Arc::as_ptr(&token.0));
        tokens.dedup_by_key(|token| Arc::as_ptr(&token.0));
        let mut stages: Vec<_> = tokens
            .iter()
            .map(|token| {
                token
                    .0
                    .stage
                    .lock()
                    .expect("commit fence lock is not poisoned")
            })
            .collect();
        if stages.iter().any(|stage| **stage == Stage::Cancelled) {
            return false;
        }
        for stage in &mut stages {
            **stage = Stage::Writing;
        }
        true
    }

    /// Returns whether cancellation has excluded all future writes for this request.
    pub fn is_cancelled(&self) -> bool {
        *self
            .0
            .stage
            .lock()
            .expect("commit fence lock is not poisoned")
            == Stage::Cancelled
    }

    /// Waits for acknowledged cancellation. Admitted writes must finish normally.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.0.cancelled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_range_does_not_admit_its_other_members() {
        let first = CommitCancellation::default();
        let second = CommitCancellation::default();
        assert!(second.cancel());
        assert!(!CommitCancellation::try_start_batch(&[
            first.clone(),
            second
        ]));
        assert!(first.cancel());
    }

    #[test]
    fn admitted_range_rejects_cancellation_for_every_member() {
        let first = CommitCancellation::default();
        let second = CommitCancellation::default();
        assert!(CommitCancellation::try_start_batch(&[
            first.clone(),
            second.clone(),
            first.clone()
        ]));
        assert!(!first.cancel());
        assert!(!second.cancel());
    }

    #[test]
    fn cancellation_racing_with_admission_has_one_winner() {
        for _ in 0..100 {
            let cancellation = CommitCancellation::default();
            let writer = cancellation.clone();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let writer_barrier = barrier.clone();
            let write = std::thread::spawn(move || {
                writer_barrier.wait();
                CommitCancellation::try_start_batch(&[writer])
            });
            barrier.wait();
            let cancelled = cancellation.cancel();
            assert_ne!(cancelled, write.join().unwrap());
            assert_eq!(cancellation.is_cancelled(), cancelled);
        }
    }
}
