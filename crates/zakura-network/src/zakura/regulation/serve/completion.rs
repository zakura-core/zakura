//! Session-local completion records without a channel or future for each request.

use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex, PoisonError, Weak},
};

/// A caller key and a unique admission on this completion queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompletionId {
    key: u64,
    sequence: NonZeroU64,
}

impl CompletionId {
    /// The caller's lookup key. Equality also checks the admission sequence.
    pub(crate) fn key(self) -> u64 {
        self.key
    }
}

/// Records kept after a drain regardless of load, so steady traffic does not reallocate.
const MIN_RETAINED_RECORDS: usize = 64;

#[derive(Debug, Default)]
struct State {
    sequence: u64,
    pending: usize,
    completed: Vec<CompletionId>,
}

/// One queue per session. Drain it before each admission to bound retained records.
///
/// Draining synchronizes with ending publication. If a peer has received an ending,
/// its completion is visible here even when the producer has not returned yet.
#[derive(Debug, Default)]
pub(crate) struct Completions(Arc<Mutex<State>>);

impl Completions {
    /// Create a completion lease, reserving its record in the shared buffer.
    /// Drain before each admission so completed records cannot accumulate indefinitely.
    pub(crate) fn track(&self, key: u64) -> Completion {
        let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        state.sequence = state
            .sequence
            .checked_add(1)
            .expect("a session cannot admit u64::MAX requests in its lifetime");
        state.pending += 1;
        // Reserve on admission so completion never allocates on a producer thread.
        let pending = state.pending;
        state.completed.reserve(pending);
        Completion {
            state: Arc::downgrade(&self.0),
            id: Some(CompletionId {
                key,
                sequence: NonZeroU64::new(state.sequence)
                    .expect("admission sequences start at one"),
            }),
        }
    }

    /// Consume completed admissions under the publication lock.
    /// The callback must not admit, drop, or complete a request on this queue.
    pub(crate) fn drain(&self, mut consume: impl FnMut(CompletionId)) {
        let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for id in state.completed.drain(..) {
            consume(id);
        }
        // Return a burst's reservation. The retained capacity still covers every
        // pending record, so completion never allocates on a producer thread.
        let target = state.pending.max(MIN_RETAINED_RECORDS);
        if state.completed.capacity() > 4 * target {
            state.completed.shrink_to(2 * target);
        }
    }
}

/// One admission. Dropping it reports cancellation unless its ending already completed it.
/// A retired session need not retain a queue for jobs that are still cleaning up.
#[derive(Debug)]
pub(crate) struct Completion {
    state: Weak<Mutex<State>>,
    id: Option<CompletionId>,
}

impl Completion {
    /// Identity retained by the caller to distinguish reuse of the same request key.
    pub(crate) fn id(&self) -> CompletionId {
        self.id
            .expect("an admission is identified before completion")
    }

    /// Publish and record completion under one lock, including a failed publication.
    pub(super) fn queue_ending<T, E>(
        &mut self,
        publish: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let Some(state) = self.state.upgrade() else {
            return publish();
        };
        let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
        let result = publish();
        if let Some(id) = self.id.take() {
            state.pending -= 1;
            state.completed.push(id);
        }
        result
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if let (Some(id), Some(state)) = (self.id.take(), self.state.upgrade()) {
            let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
            state.pending -= 1;
            state.completed.push(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cancellation, failed output and late drops each report a lease at most once.
    #[test]
    fn completion_records_are_exact_across_reuse_and_failure() {
        let completed = Completions::default();
        let mut first = completed.track(7);
        let first_id = first.id();
        assert_eq!(first.queue_ending(|| Ok::<_, ()>(())), Ok(()));
        let mut records = Vec::new();
        completed.drain(|id| records.push(id));
        assert_eq!(records, [first_id]);
        let replacement = completed.track(7);
        let replacement_id = replacement.id();
        assert_ne!(first_id, replacement_id);
        drop(first);
        completed.drain(|_| panic!("the old lease must not complete its replacement"));
        drop(replacement);
        let mut failed = completed.track(8);
        let failed_id = failed.id();
        assert_eq!(
            failed.queue_ending(|| Err::<(), _>("closed")),
            Err("closed")
        );
        drop(failed);
        records.clear();
        completed.drain(|id| records.push(id));
        assert_eq!(records, [replacement_id, failed_id]);
    }

    /// Seeing a published ending cannot race ahead of its completion record.
    #[test]
    fn draining_waits_for_ending_publication() {
        let completed = Arc::new(Completions::default());
        let mut lease = completed.track(1);
        let id = lease.id();
        let (published, observed) = std::sync::mpsc::channel();
        let (checked, proceed) = std::sync::mpsc::channel();
        let reader = {
            let completed = completed.clone();
            std::thread::spawn(move || {
                observed
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                assert!(completed.0.try_lock().is_err());
                checked.send(()).unwrap();
                let mut records = Vec::new();
                completed.drain(|id| records.push(id));
                assert_eq!(records, [id]);
            })
        };
        lease
            .queue_ending(|| {
                published.send(()).unwrap();
                proceed
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();
        reader.join().unwrap();
    }

    /// Out-of-order producers record distinct admissions without allocating at completion.
    #[test]
    fn completion_order_is_independent_of_admission_order() {
        let completed = Completions::default();
        let first = completed.track(1);
        let second = completed.track(2);
        let third = completed.track(3);
        let expected = [second.id(), third.id(), first.id()];
        let (_, allocations) = zakura_test::allocations::measure(|| {
            drop(second);
            drop(third);
            drop(first);
        });
        assert_eq!(allocations.requests, 0);
        let mut records = Vec::new();
        completed.drain(|id| records.push(id));
        assert_eq!(records, expected);
    }

    /// A burst's reservation is released once its records drain.
    #[test]
    fn draining_after_a_burst_releases_its_reservation() {
        let completed = Completions::default();
        let burst: Vec<_> = (0..10_000).map(|key| completed.track(key)).collect();
        let still_pending = completed.track(u64::MAX);
        drop(burst);
        completed.drain(|_| {});
        let capacity = completed.0.lock().unwrap().completed.capacity();
        assert!(capacity >= 1, "the pending record keeps its reservation");
        assert!(
            capacity <= 4 * MIN_RETAINED_RECORDS,
            "retained {capacity} records"
        );
        let (_, allocations) = zakura_test::allocations::measure(|| drop(still_pending));
        assert_eq!(allocations.requests, 0);
    }

    /// Old jobs neither retain retired state nor notify a replacement session.
    #[test]
    fn retired_completion_queues_are_not_kept_alive_by_jobs() {
        let completed = Completions::default();
        let mut old = completed.track(1);
        drop(completed);
        assert!(old.state.upgrade().is_none());
        let replacement = Completions::default();
        let current = replacement.track(1);
        let current_id = current.id();
        old.queue_ending(|| Ok::<_, ()>(())).unwrap();
        drop(old);
        replacement.drain(|_| panic!("old cleanup cannot complete an active replacement"));
        drop(current);
        let mut records = Vec::new();
        replacement.drain(|id| records.push(id));
        assert_eq!(records, [current_id]);
    }
}
