//! Which blocks this round's downloads may use as a parent before verification.
//!
//! A block that is still downloading counts as a parent only once it has passed the parent check
//! itself: its hash came from a peer, so being requested proves nothing about where it connects.

use std::{collections::HashSet, sync::Mutex, time::Duration};

use tokio::{
    sync::watch,
    time::{timeout_at, Instant},
};

use zakura_chain::block;

use super::ParentExtendable;
use crate::components::sync::{
    BLOCK_DOWNLOAD_TIMEOUT, FINAL_CHECKPOINT_BLOCK_VERIFY_TIMEOUT,
    MISSING_BLOCK_REGISTRY_RETRY_LIMIT, REGISTRY_MISS_RETRY_BACKOFF,
};

/// Bounds a download task's wait for a parent that is still pending.
///
/// The parent's own task already bounds the wait, so this is only a safety net. Its download
/// times out after [`BLOCK_DOWNLOAD_TIMEOUT`]. A block missing from every peer is retried for up
/// to [`MISSING_BLOCK_REGISTRY_RETRY_LIMIT`] × [`REGISTRY_MISS_RETRY_BACKOFF`] (120 s). Any other
/// failure, or an exhausted retry budget, restarts the round, which cancels the waiting children.
/// The first block after the last checkpoint waits for the final checkpoint range to be written,
/// which takes up to [`FINAL_CHECKPOINT_BLOCK_VERIFY_TIMEOUT`] (120 s). This cap is above both, so
/// an honest child is never dropped while its parent is still legitimately on its way.
pub(in crate::components::sync) const PARENT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(150);

const _: () = {
    // usize to u64: the retry limit is a small constant, so the cast is lossless.
    let registry_retry_window =
        REGISTRY_MISS_RETRY_BACKOFF.as_secs() * MISSING_BLOCK_REGISTRY_RETRY_LIMIT as u64;
    assert!(
        PARENT_ADMISSION_TIMEOUT.as_secs()
            > registry_retry_window + BLOCK_DOWNLOAD_TIMEOUT.as_secs(),
        "the parent wait must outlast the parent's own registry retries and one download",
    );
    assert!(
        PARENT_ADMISSION_TIMEOUT.as_secs() > FINAL_CHECKPOINT_BLOCK_VERIFY_TIMEOUT.as_secs(),
        "the parent wait must outlast the final checkpoint range's write",
    );
};

/// The blocks requested this round, and those of them that passed the parent check.
#[derive(Debug, Default)]
struct Sets {
    /// Blocks requested this round that have neither verified nor been rejected. A block whose
    /// download failed stays here while the syncer requests it again.
    pending: HashSet<block::Hash>,

    /// Pending blocks that passed the parent check, so their children may go to the verifier.
    passed: HashSet<block::Hash>,
}

/// Where a parent stands in this round's downloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParentStatus {
    /// The parent passed its own parent check.
    Passed,
    /// The parent was requested and has not passed, verified or been rejected yet.
    Pending,
    /// The parent is not among this round's unfinished downloads.
    Neither,
}

/// The shared record of which in-flight blocks may serve as a parent.
///
/// Both sets live behind one lock. Every change bumps a generation counter under that lock and
/// publishes it on a [`watch`] channel, so a child that marked the counter seen before reading
/// the sets is woken by any later change.
#[derive(Debug)]
pub(super) struct ParentGate {
    sets: Mutex<Sets>,
    generation: watch::Sender<u64>,
}

impl Default for ParentGate {
    fn default() -> Self {
        Self {
            sets: Mutex::default(),
            generation: watch::channel(0).0,
        }
    }
}

impl ParentGate {
    /// Records that the syncer requested `hash` this round.
    pub(super) fn request(&self, hash: block::Hash) {
        self.update(|sets| sets.pending.insert(hash));
    }

    /// Records that pending block `hash` passed its parent check.
    pub(super) fn pass(&self, hash: block::Hash) {
        self.update(|sets| sets.pending.contains(&hash) && sets.passed.insert(hash));
    }

    /// Records that `hash` verified or was rejected, so it no longer counts as a parent.
    pub(super) fn finish(&self, hash: block::Hash) {
        self.update(|sets| {
            let was_passed = sets.passed.remove(&hash);
            sets.pending.remove(&hash) || was_passed
        });
    }

    /// Forgets every block, for a new round.
    pub(super) fn clear(&self) {
        self.update(|sets| {
            let changed = !sets.pending.is_empty() || !sets.passed.is_empty();
            sets.pending.clear();
            sets.passed.clear();
            changed
        });
    }

    /// Returns whether a block whose parent is `parent` may go to the verifier.
    ///
    /// A passed parent qualifies at once. A pending parent is waited on until it passes, or until
    /// it leaves the pending set and the state is asked. A parent in neither set qualifies if
    /// `parent_extendable` says the state can build on it. The state is asked at most once, and
    /// never while the parent is pending. Returns `false` if the parent is still pending after
    /// [`PARENT_ADMISSION_TIMEOUT`].
    ///
    /// The sets are read before the state. A verified parent leaves the sets only after the state
    /// has made it visible, so a parent that verifies between the two reads is found by one of
    /// them.
    pub(super) async fn parent_ready(
        &self,
        parent: block::Hash,
        parent_extendable: &ParentExtendable,
    ) -> bool {
        let deadline = Instant::now() + PARENT_ADMISSION_TIMEOUT;
        let mut generation = self.generation.subscribe();

        loop {
            generation.borrow_and_update();
            match self.status(parent) {
                ParentStatus::Passed => return true,
                ParentStatus::Neither => return (parent_extendable.0)(parent).await,
                ParentStatus::Pending => {
                    // The sender lives as long as `self`, so `changed` only fails if the gate
                    // is gone, which can't happen while this borrow is alive.
                    if !matches!(timeout_at(deadline, generation.changed()).await, Ok(Ok(()))) {
                        return false;
                    }
                }
            }
        }
    }

    /// Returns whether `hash` is still pending.
    #[cfg(test)]
    fn is_pending(&self, hash: &block::Hash) -> bool {
        self.lock().pending.contains(hash)
    }

    /// Returns whether `hash` passed its parent check and is still pending.
    #[cfg(test)]
    fn is_passed(&self, hash: &block::Hash) -> bool {
        self.lock().passed.contains(hash)
    }

    /// Returns whether no block is recorded.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        let sets = self.lock();
        sets.pending.is_empty() && sets.passed.is_empty()
    }

    fn status(&self, parent: block::Hash) -> ParentStatus {
        let sets = self.lock();
        if sets.passed.contains(&parent) {
            ParentStatus::Passed
        } else if sets.pending.contains(&parent) {
            ParentStatus::Pending
        } else {
            ParentStatus::Neither
        }
    }

    /// Applies `change` and, if it changed either set, wakes the waiting children. The counter is
    /// bumped while the lock is held, so a child that reads the sets after marking the counter
    /// seen always observes a later bump.
    fn update(&self, change: impl FnOnce(&mut Sets) -> bool) {
        let mut sets = self.lock();
        if change(&mut sets) {
            self.generation
                .send_modify(|generation| *generation = generation.wrapping_add(1));
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Sets> {
        self.sets
            .lock()
            .expect("parent gate lock is only held for synchronous updates")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::FutureExt;

    use super::*;

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    /// A [`ParentExtendable`] that answers `answer` and records every parent it is asked about.
    fn recording_state(answer: bool) -> (ParentExtendable, Arc<Mutex<Vec<block::Hash>>>) {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let recorded = asked.clone();
        let state = ParentExtendable(Arc::new(move |parent| {
            recorded.lock().expect("test lock").push(parent);
            async move { answer }.boxed()
        }));
        (state, asked)
    }

    fn asked(asked: &Mutex<Vec<block::Hash>>) -> Vec<block::Hash> {
        asked.lock().expect("test lock").clone()
    }

    /// Spawns a child's parent check on `parent`.
    fn spawn_child(
        gate: &Arc<ParentGate>,
        parent: block::Hash,
        state: &ParentExtendable,
    ) -> tokio::task::JoinHandle<bool> {
        let gate = gate.clone();
        let state = state.clone();
        tokio::spawn(async move { gate.parent_ready(parent, &state).await })
    }

    /// Lets spawned children run until they wait.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn passed_parent_is_ready_without_asking_the_state() {
        let gate = ParentGate::default();
        let (state, questions) = recording_state(false);
        gate.request(hash(1));
        gate.pass(hash(1));

        assert!(gate.parent_ready(hash(1), &state).await);
        assert!(asked(&questions).is_empty());
    }

    #[tokio::test]
    async fn parent_in_neither_set_is_decided_by_one_state_question() {
        for answer in [true, false] {
            let gate = ParentGate::default();
            let (state, questions) = recording_state(answer);

            assert_eq!(gate.parent_ready(hash(1), &state).await, answer);
            assert_eq!(asked(&questions), vec![hash(1)]);
        }
    }

    #[tokio::test]
    async fn pending_parent_that_passes_is_ready_without_asking_the_state() {
        let gate = Arc::new(ParentGate::default());
        let (state, questions) = recording_state(false);
        gate.request(hash(1));

        let child = spawn_child(&gate, hash(1), &state);
        settle().await;
        assert!(
            !child.is_finished(),
            "the child waits while its parent is pending"
        );

        gate.pass(hash(1));
        assert!(child.await.expect("child task doesn't panic"));
        assert!(asked(&questions).is_empty());
    }

    /// Changes to other blocks wake a waiting child, which keeps waiting without asking the
    /// state. Once its parent leaves the sets, the state decides with a single question.
    #[tokio::test]
    async fn unrelated_changes_keep_a_pending_parent_waiting() {
        for answer in [true, false] {
            let gate = Arc::new(ParentGate::default());
            let (state, questions) = recording_state(answer);
            gate.request(hash(1));

            let child = spawn_child(&gate, hash(1), &state);
            settle().await;
            gate.request(hash(2));
            gate.pass(hash(2));
            gate.finish(hash(2));
            settle().await;
            assert!(!child.is_finished(), "the parent is still pending");
            assert!(asked(&questions).is_empty());

            // A parent leaves the sets when it verifies (the state has it) or is rejected.
            gate.finish(hash(1));
            assert_eq!(child.await.expect("child task doesn't panic"), answer);
            assert_eq!(asked(&questions), vec![hash(1)]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn parent_still_pending_at_the_cap_is_not_ready() {
        let gate = Arc::new(ParentGate::default());
        let (state, questions) = recording_state(true);
        gate.request(hash(1));
        let start = Instant::now();

        assert!(!spawn_child(&gate, hash(1), &state)
            .await
            .expect("child task doesn't panic"));
        assert!(start.elapsed() >= PARENT_ADMISSION_TIMEOUT);
        assert!(
            asked(&questions).is_empty(),
            "the state is never asked about a pending parent"
        );
    }

    #[test]
    fn only_pending_blocks_can_pass() {
        let gate = ParentGate::default();

        gate.pass(hash(1));
        assert!(gate.is_empty(), "a block that wasn't requested can't pass");

        gate.request(hash(1));
        gate.pass(hash(1));
        assert!(gate.is_pending(&hash(1)) && gate.is_passed(&hash(1)));
        gate.finish(hash(1));
        assert!(gate.is_empty());

        gate.request(hash(1));
        gate.request(hash(2));
        gate.pass(hash(2));
        gate.clear();
        assert!(gate.is_empty());
    }
}
