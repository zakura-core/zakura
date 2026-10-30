//! Download work source for Zakura block sync.
//!
//! The [`WorkQueue`] is the sole shared download-scheduling primitive: a sorted
//! set of needed block heights the per-peer issuance path pulls from. It replaces
//! the central `BlockRangeScheduler`'s eligibility/dedup/retry roles with a small
//! API the caller drives from its own per-peer state (see the ):
//!
//! - a height is in **exactly one** of `{below-floor (gone), pending, in_flight}`;
//! - [`take_in_range`](WorkQueue::take_in_range) moves a contiguous-ascending run
//!   `pending → in_flight` (so one taken chunk maps to one `BlockRangeRequest`),
//!   bounded only by the caller's servable range and a count cap — never by how
//!   far above the download floor the heights already are;
//! - only `return_items` (timeout/disconnect retry) and
//!   [`reset_above`](WorkQueue::reset_above) move `in_flight → pending`;
//! - [`advance_floor`](WorkQueue::advance_floor) is garbage collection only — the
//!   download floor never throttles the fetch decision.
//!
//! Internals are a brief `std::sync::Mutex` whose critical sections are tiny map
//! splices held **never across `.await`** (the anti-block rule). The pure
//! [`WorkQueueState`] owns the logical work and charges; this wrapper owns the lock,
//! the request-write registrations, and the notifications. `estimated_bytes`
//! on a [`WorkItem`] is the block's size *estimate* (not its worst-case
//! reservation); it feeds the request budget reservation and the advisory
//! receive-path size-hint mismatch metric, which never discards a hash-matched body.

use std::{collections::BTreeSet, num::NonZeroU64, sync::Arc};

use zakura_chain::block;
use zakura_rt::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex as StdMutex, MutexGuard, Notify,
    },
};

use super::{request::BlockSizeEstimate, state::BlockBudgetLedger};

mod request_write;
pub(super) mod state;

#[cfg(any(test, zakura_sim))]
pub use request_write::RequestWritePhase;
use request_write::RequestWriteRegistration;
pub(super) use request_write::{RequestWrite, RequestWriteStatus};
use state::{WorkQueueState, WorkWake};

/// Lower clamp on a body-size estimate.
pub(super) const DEFAULT_BS_SIZE_FLOOR_BYTES: u64 = 1024;

/// Per-height download metadata held in the [`WorkQueue`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct WorkItem {
    /// Exact durable coordinates that authorized this body download.
    pub(super) scope: zakura_header_chain::BodyWorkAuthority,
    /// Exact attempt, retained from provisional take through response receipt.
    pub(super) owner: Option<zakura_header_chain::BodyWorkOwner>,
    /// Taken by an attempt that has not yet published its reservation and frame.
    provisional: bool,
    /// Expected hash of the block at this height (drives the response match).
    pub(super) hash: block::Hash,
    /// The block's size estimate. Used for request budget reservation and the receive-path size-hint mismatch metric.
    pub(super) estimated_bytes: u64,
    /// Largest payload seen locally. Peer hints cannot shrink a retry below it.
    observed_bytes: u64,
    /// Refreshed hint for a future retry while an issued reservation stays fixed.
    retry_estimated_bytes: Option<u64>,
    /// A pressure refusal waits for verified progress before it can be retried.
    retry_after_tip: Option<block::Height>,
    /// Request reservation; received bodies use `Released`.
    pub(super) budget: BlockBudgetLedger,
    /// Another attempt's body ended this owner's reservation. The owner's request
    /// still claims the height, so dropping or moving the item is a revocation.
    answered_elsewhere: bool,
}

/// Where a queued item sits in its download lifecycle.
#[cfg(any(test, zakura_sim))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WorkItemPlace {
    /// Waiting for a request.
    Pending,
    /// Taken by a request that has not yet published its reservation here.
    Provisional,
    /// Taken by a published request.
    InFlight,
}

/// One queued item's owner and byte quantities.
#[cfg(any(test, zakura_sim))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WorkItemOwnership {
    /// The item's height.
    pub height: block::Height,
    /// The request that took the item, if one did.
    pub owner: Option<zakura_header_chain::BodyWorkOwner>,
    /// Where the item sits.
    pub place: WorkItemPlace,
    /// The size estimate a request for this item reserves.
    pub estimated_bytes: u64,
    /// The estimate bytes this item currently charges to the queue's tally.
    pub reserved_bytes: u64,
}

/// A request reservation that a received body ended.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct EndedReservation {
    /// The height's own charge: the estimate its request reserved for it.
    pub(super) charge: u64,
    /// Every byte released: the charge, plus the unsent heights of a queued
    /// request the release retired.
    pub(super) released: u64,
}

/// Diagnostics for an attempted `in_flight -> pending` retry transition.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct WorkReturnOutcome {
    /// This owner's frame was skipped before writing, atomically with settlement.
    pub(super) request_was_unwritten: bool,
    /// Reserved bytes released while returning or discarding items.
    pub(super) released_bytes: u64,
    /// Reserved items successfully moved back to `pending`.
    pub(super) returned_count: u64,
    /// Owned heights discarded because they are already committed.
    pub(super) committed_count: u64,
    /// Requested heights that were already back in `pending`.
    pub(super) already_pending_count: u64,
    /// Received items still present in `in_flight` with a `Released` ledger.
    pub(super) released_count: u64,
    /// Requested heights absent from both `pending` and `in_flight`.
    pub(super) missing_count: u64,
    /// Lowest height considered by the cleanup.
    pub(super) min_height: Option<block::Height>,
    /// Highest height considered by the cleanup.
    pub(super) max_height: Option<block::Height>,
}

impl WorkReturnOutcome {
    /// Whether the cleanup took unreceived work away from its owner: it returned
    /// a reserved height or discarded a committed one.
    pub(super) fn revoked_work(&self) -> bool {
        self.returned_count > 0 || self.committed_count > 0
    }
}

/// The queue lock's contents: the logical owner plus the runtime registrations
/// that must change atomically with it.
#[derive(Debug)]
struct WorkQueueInner {
    state: WorkQueueState,
    /// Weak owners allow reset notification without retaining session resources.
    /// Their status survives until writing or terminal cleanup finishes.
    request_writes: HashMap<zakura_header_chain::BodyWorkOwner, RequestWriteRegistration>,
}

/// Publishes a changed reservation gauge before releasing the queue lock.
struct WorkQueueGuard<'a> {
    inner: MutexGuard<'a, WorkQueueInner>,
    reserved_bytes: u64,
}

impl std::ops::Deref for WorkQueueGuard<'_> {
    type Target = WorkQueueInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for WorkQueueGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for WorkQueueGuard<'_> {
    fn drop(&mut self) {
        let bytes = self.inner.state.reserved_bytes();
        if bytes != self.reserved_bytes {
            // This lossy cast affects diagnostics only; the state keeps an integer.
            metrics::gauge!("sync.block.work.reserved_bytes").set(bytes as f64);
        }
    }
}

/// The shared download work source. See the module docs for the invariants.
#[derive(Debug)]
pub(super) struct WorkQueue {
    inner: StdMutex<WorkQueueInner>,
    available: Notify,
    /// Counts events that take an unreceived height from its owner without that
    /// owner's routine retiring the request. Routines rescan their requests for
    /// lost ownership only after this changes.
    revocations: AtomicU64,
    /// Receives request write steps and queue snapshots, in test and simulator
    /// builds.
    #[cfg(any(test, zakura_sim))]
    pub(super) observer: super::block_sync_observe::ObserverSlot,
}

impl WorkQueue {
    pub(super) fn new(floor: block::Height) -> Self {
        Self {
            inner: StdMutex::new(WorkQueueInner {
                state: WorkQueueState::new(floor),
                request_writes: HashMap::new(),
            }),
            available: Notify::new(),
            revocations: AtomicU64::new(0),
            #[cfg(any(test, zakura_sim))]
            observer: Default::default(),
        }
    }

    /// This queue, reporting to `observer`.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn with_observer(
        mut self,
        observer: super::block_sync_observe::ObserverSlot,
    ) -> Self {
        self.observer = observer;
        self
    }

    /// Reports the queue's items to the observer, if one is installed. A task
    /// that changes the queue calls this after it handled an input.
    ///
    /// It reports under the queue's lock. Releasing the lock is a preemption
    /// point under the simulator, so a snapshot read before it could land
    /// after a newer one.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn observe_items(&self) {
        let inner = self.lock();
        observe!(
            self.observer,
            super::block_sync_observe::BlockEvent::WorkQueue(Self::ownership(&inner))
        );
    }

    /// The number of ownership revocations so far. See [`Self::revoke_for_owner`].
    pub(super) fn revocations(&self) -> u64 {
        self.revocations.load(Ordering::Acquire)
    }

    fn note_revocation(&self) {
        self.revocations.fetch_add(1, Ordering::AcqRel);
    }

    /// Deliver the wakeups a logical transition requires. Callers choose whether
    /// this runs inside or after the queue lock, matching each transition's
    /// linearization point.
    fn wake(&self, wake: WorkWake) {
        if wake.available {
            self.available.notify_waiters();
        }
        if wake.revocation {
            self.note_revocation();
        }
    }

    /// Return `owner`'s unreceived heights on behalf of another actor, such as
    /// the floor watchdog. The owner's routine learns of the change from
    /// [`Self::revocations`].
    pub(super) fn revoke_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> WorkReturnOutcome {
        let outcome = self.release_reserved_and_return_items_detailed_for_owner(owner, heights);
        if outcome.revoked_work() {
            self.note_revocation();
        }
        outcome
    }

    #[cfg(test)]
    pub(super) fn set_estimate_floor_for_tests(&self, floor: u64) {
        self.lock().state.set_estimate_floor(floor);
    }

    fn lock(&self) -> WorkQueueGuard<'_> {
        let inner = self
            .inner
            .lock()
            .expect("work queue mutex is never poisoned");
        let reserved_bytes = inner.state.reserved_bytes();
        WorkQueueGuard {
            inner,
            reserved_bytes,
        }
    }

    /// Add scoped `(height, hash, size)` items to `pending`.
    /// Insert a height above `floor` only when no pending or in-flight item owns it.
    /// Return the number of inserted heights.
    /// Wake waiters after inserting any height.
    pub(super) fn extend(
        &self,
        scope: zakura_header_chain::BodyWorkAuthority,
        items: impl IntoIterator<Item = (block::Height, block::Hash, BlockSizeEstimate)>,
    ) -> usize {
        let (inserted, wake) = self.lock().state.extend(scope, items);
        self.wake(wake);
        inserted
    }

    /// Refresh scheduling metadata without rewriting an issued reservation.
    pub(super) fn refresh_size_estimates(
        &self,
        scope: zakura_header_chain::BodyWorkAuthority,
        items: impl IntoIterator<Item = (block::Height, block::Hash, BlockSizeEstimate)>,
    ) {
        let wake = self.lock().state.refresh_size_estimates(scope, items);
        self.wake(wake);
    }

    /// Bound a correction refresh to work already queued locally.
    pub(super) fn queued_bounds(&self) -> Option<(block::Height, block::Height)> {
        self.lock().state.queued_bounds()
    }

    /// Reauthorize queued work while in-flight requests retain their registered owners.
    pub(super) fn refresh_authority(&self, authority: zakura_header_chain::BodyWorkAuthority) {
        let mut inner = self.lock();
        let wake = inner.state.refresh_authority(authority);
        self.wake(wake);
    }

    /// Move up to `max` contiguous-ascending `pending` heights within
    /// `low..=high` from `pending` to `in_flight`, returned in ascending order.
    ///
    /// "Contiguous-ascending" stops at the first gap, so the returned chunk maps
    /// to a single `BlockRangeRequest`. `high` is the caller's `servable_high`
    /// and is **NOT** clamped to the floor (the download floor is never an upper
    /// bound on the fetch). Returns empty if nothing is eligible.
    pub(super) fn take_in_range(
        &self,
        low: block::Height,
        high: block::Height,
        max: usize,
    ) -> Vec<(block::Height, WorkItem)> {
        if max == 0 || low > high {
            return Vec::new();
        }
        self.lock().state.take_in_range(low, high, max)
    }

    /// Move up to `max_count` contiguous-ascending `pending` heights within
    /// `low..=high` from `pending` to `in_flight`, also stopping before the
    /// sum of stored size estimates would exceed `max_estimated_bytes`.
    ///
    /// The estimate cap bounds the request's summed byte reservation. To
    /// guarantee progress, the first eligible item is always taken when
    /// `max_count > 0`, even if its estimate alone exceeds the cap.
    #[cfg(test)]
    pub(super) fn take_in_range_budgeted(
        &self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
    ) -> Vec<(block::Height, WorkItem)> {
        self.lock()
            .state
            .take_budgeted(low, high, max_count, max_estimated_bytes, None)
    }

    /// Give the provisional take an exact owner before releasing the queue lock.
    /// A reset followed by a new take cannot be undone by this attempt's cleanup.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn take_for_request(
        &self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
        session_id: u64,
        request_id: NonZeroU64,
    ) -> Vec<(block::Height, WorkItem)> {
        self.lock().state.take_budgeted(
            low,
            high,
            max_count,
            max_estimated_bytes,
            Some((session_id, request_id)),
        )
    }

    /// Move each given height `in_flight → pending`, preserving its stored
    /// [`WorkItem`]. Heights not currently `in_flight` are skipped (idempotent).
    /// Wakes waiters if anything moved.
    #[cfg(test)]
    pub(super) fn return_items(&self, heights: impl IntoIterator<Item = block::Height>) {
        let wake = self.lock().state.return_items(heights);
        self.wake(wake);
    }

    /// Like `return_items` but **does not** notify waiters.
    ///
    /// Used by a peer routine to put back a chunk it took but chose not to issue
    /// (e.g. the heights are in its own short retry-avoid window after it just
    /// failed them). Notifying here would re-wake the returning routine's own
    /// freshly-registered `available` future and busy-loop the want-work arm
    /// (a self-wake spin); other peers were already woken by the original failure
    /// `return_items`, so suppressing the notify only affects the caller.
    pub(super) fn return_unpublished(&self, items: &[(block::Height, WorkItem)]) {
        self.lock().state.return_unpublished(items);
    }

    /// Mark already-taken heights as owning an estimated byte reservation.
    ///
    /// Returns the sum marked. The caller must have already admitted the same
    /// byte total through [`ByteBudget`](crate::zakura::transport::ByteBudget).
    #[cfg(test)]
    pub(super) fn mark_reserved(&self, heights: impl IntoIterator<Item = block::Height>) -> u64 {
        self.lock().state.mark_reserved(None, heights)
    }

    #[cfg(test)]
    pub(super) fn mark_reserved_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        self.lock().state.mark_reserved(Some(owner), heights)
    }

    /// End an active request reservation at receipt.
    #[cfg(test)]
    pub(super) fn release_active_reserved_height(&self, height: block::Height) -> Option<u64> {
        self.release_active_reserved_height_matching(None, height, false)
            .map(|ended| ended.released)
    }

    /// End the receipt reservation and retire any queued request it invalidates.
    /// Returns all released bytes, including the request's other unsent heights.
    pub(super) fn release_active_reserved_height_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) -> Option<u64> {
        self.release_active_reserved_height_matching(Some(owner), height, false)
            .map(|ended| ended.released)
    }

    /// Like [`Self::release_active_reserved_height_for_owner`], for a body that
    /// another attempt received. Returns the height's own charge with the
    /// released total, both read under the lock that checks `owner`.
    pub(super) fn release_answered_elsewhere(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) -> Option<EndedReservation> {
        self.release_active_reserved_height_matching(Some(owner), height, true)
    }

    fn release_active_reserved_height_matching(
        &self,
        owner: Option<zakura_header_chain::BodyWorkOwner>,
        height: block::Height,
        answered_elsewhere: bool,
    ) -> Option<EndedReservation> {
        let (ended, claim) = {
            let mut inner = self.lock();
            let ended =
                inner
                    .state
                    .end_reservation_at_receipt(owner, height, answered_elsewhere)?;
            let (outcome, claim) = self.return_items_locked(&mut inner, ended.owner, []);
            (
                EndedReservation {
                    charge: ended.released_bytes,
                    released: ended.released_bytes.saturating_add(outcome.released_bytes),
                },
                claim,
            )
        };
        drop(claim);
        Some(ended)
    }

    /// Claim a received height and end any request reservation it owned.
    #[cfg(test)]
    pub(super) fn claim_received(&self, height: block::Height) -> u64 {
        self.claim_received_matching(None, height)
    }

    /// Claim a received height and return its reservation plus any unsent bytes
    /// released by retiring the queued request.
    pub(super) fn claim_received_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) -> u64 {
        self.claim_received_matching(Some(owner), height)
    }

    fn claim_received_matching(
        &self,
        owner: Option<zakura_header_chain::BodyWorkOwner>,
        height: block::Height,
    ) -> u64 {
        let (released, claim) = {
            let mut inner = self.lock();
            let Some(ended) = inner.state.claim_received(owner, height) else {
                return 0;
            };
            let (outcome, claim) = self.return_items_locked(&mut inner, ended.owner, []);
            (
                ended.released_bytes.saturating_add(outcome.released_bytes),
                claim,
            )
        };
        drop(claim);
        released
    }

    /// Release active request reservations, leaving received heights in place.
    #[cfg(test)]
    pub(super) fn release_reserved_heights(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        self.lock().state.release_reserved_heights(None, heights)
    }

    pub(super) fn release_reserved_heights_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        self.lock()
            .state
            .release_reserved_heights(Some(owner), heights)
    }

    /// Release and return heights, including received ones, in tests.
    #[cfg(test)]
    pub(super) fn release_and_return_items(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        let delta = self.lock().state.release_and_return_items(heights);
        self.wake(delta.wake);
        delta.released_bytes
    }

    /// Release and return only unreceived heights.
    #[cfg(test)]
    pub(super) fn release_reserved_and_return_items(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        self.release_reserved_and_return_items_detailed(heights)
            .released_bytes
    }

    /// Release and return still-reserved items, preserving the outcome of every
    /// requested height for low-volume lifecycle tracing.
    #[cfg(test)]
    pub(super) fn release_reserved_and_return_items_detailed(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> WorkReturnOutcome {
        self.release_reserved_and_return_items_detailed_matching(None, heights)
    }

    /// Return this owner's unreceived heights. Expiring a queued frame also
    /// returns every other unsent height owned by that request under the same lock.
    pub(super) fn release_reserved_and_return_items_detailed_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> WorkReturnOutcome {
        self.release_reserved_and_return_items_detailed_matching(Some(owner), heights)
    }

    fn release_reserved_and_return_items_detailed_matching(
        &self,
        owner: Option<zakura_header_chain::BodyWorkOwner>,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> WorkReturnOutcome {
        let (outcome, claim) = {
            let mut inner = self.lock();
            self.return_items_locked(&mut inner, owner, heights)
        };
        drop(claim);
        outcome
    }

    /// Return an exact received attempt without releasing its request credit twice.
    /// Waiting for verified progress prevents repeated downloads under memory pressure.
    pub(super) fn defer_received_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        hash: block::Hash,
        bytes: u64,
        verified_tip: block::Height,
    ) -> bool {
        let deferred = self
            .lock()
            .state
            .defer_received(owner, height, hash, bytes, verified_tip);
        let Some(wake) = deferred else {
            return false;
        };
        self.wake(wake);
        true
    }

    /// Settle requested heights and any queued request they retire. The caller
    /// must retain the returned claim until after releasing the queue lock.
    fn return_items_locked(
        &self,
        inner: &mut WorkQueueInner,
        owner: Option<zakura_header_chain::BodyWorkOwner>,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> (WorkReturnOutcome, Option<Arc<RequestWrite>>) {
        let mut heights: BTreeSet<_> = heights.into_iter().collect();
        let registration = owner.and_then(|owner| inner.request_writes.get(&owner));
        let claim = registration.and_then(|registration| registration.claim.upgrade());
        let mut request_was_unwritten = false;
        if let Some(registration) = registration {
            // The writer claims under this same lock. Expiry skips an
            // unwritten frame even while its last owner is being dropped.
            if registration.status.expire_unwritten() {
                self.available.notify_waiters();
            }
            request_was_unwritten = registration.status.was_skipped();
            if request_was_unwritten {
                if let Some(claim) = &claim {
                    // Skipping an unsent frame retires its whole request.
                    heights.extend(claim.heights());
                }
            }
        }
        let (mut outcome, wake) = inner.state.return_owned(owner, heights);
        outcome.request_was_unwritten = request_was_unwritten;
        self.wake(wake);

        (outcome, claim)
    }

    /// Garbage-collect committed heights: raise the floor to `max(self.floor,
    /// floor)` and drop every `pending`/`in_flight` entry `<= floor`.
    ///
    /// Returns request-estimate bytes that were still reserved for unreceived
    /// heights, so the caller returns them to the `ByteBudget`. Received bodies
    /// carry no charge here; the Sequencer's buffers own them.
    pub(super) fn advance_floor(&self, floor: block::Height) -> u64 {
        let delta = self.lock().state.advance_floor(floor);
        self.wake(delta.wake);
        delta.released_bytes
    }

    /// Frontier reset: pin the floor and drop every `pending`/`in_flight` entry
    /// `> floor` (their buffers were dropped; the producer re-fills via the next
    /// query).
    ///
    /// Returns request-estimate bytes still reserved for unreceived heights, as
    /// in [`advance_floor`](Self::advance_floor).
    pub(super) fn reset_above(&self, floor: block::Height) -> u64 {
        let mut inner = self.lock();
        // Retain upgraded owners until after unlocking: a concurrently dropped
        // queue entry can make this the last reference, whose Drop settles work.
        let claims: Vec<_> = inner
            .request_writes
            .values()
            .filter_map(|registration| registration.claim.upgrade())
            .collect();
        for claim in claims.iter().filter(|claim| claim.has_height_above(floor)) {
            claim.reset();
            inner.request_writes.remove(&claim.owner());
        }
        let delta = inner.state.reset_above(floor);

        drop(inner);
        drop(claims);
        self.wake(delta.wake);
        delta.released_bytes
    }

    /// The "work added" notifier (per-peer routines wake source).
    pub(super) fn subscribe_available(&self) -> &Notify {
        &self.available
    }

    // ---- diagnostics (trace + late-response classification) ----

    pub(super) fn pending_len(&self) -> usize {
        self.lock().state.pending_len()
    }

    pub(super) fn in_flight_len(&self) -> usize {
        self.lock().state.in_flight_len()
    }

    /// Reserved bytes and reserved items in `in_flight` above `floor`.
    ///
    /// Admission reads this once per issued request, so it must not scan the
    /// map. `in_flight` holds heights above the queue floor, which normally
    /// matches `floor`, so only the heights between the two floors are scanned.
    pub(super) fn reserved_above(&self, floor: block::Height) -> (u64, u64) {
        self.lock().state.reserved_above(floor)
    }

    /// Sum of reserved request-estimate bytes across `pending` + `in_flight`.
    ///
    /// O(1): returns the incrementally-maintained counter. This is on the
    /// sequencer's hot path via `publish_view`, so it must not scan the maps.
    /// `reserved_bytes_scanned` is the O(n) ground-truth recomputation used by
    /// the audit / tests to catch drift.
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.lock().state.reserved_bytes()
    }

    /// Ground-truth O(pending + in_flight) recomputation of [`reserved_bytes`],
    /// used by tests to assert the maintained counter never drifts.
    #[cfg(test)]
    pub(super) fn reserved_bytes_scanned(&self) -> u64 {
        self.lock().state.reserved_bytes_scanned()
    }

    /// Every queued item with its owner and byte quantities, read from the
    /// queue maps rather than from the maintained tallies.
    #[cfg(test)]
    pub(super) fn ownership_snapshot(&self) -> Vec<WorkItemOwnership> {
        Self::ownership(&self.lock())
    }

    /// The items of `inner`, as [`Self::ownership_snapshot`] reads them.
    #[cfg(any(test, zakura_sim))]
    fn ownership(inner: &WorkQueueInner) -> Vec<WorkItemOwnership> {
        inner.state.ownership()
    }

    /// Number of contiguous runs across `pending` (one queued range per maximal
    /// contiguous run of heights).
    pub(super) fn pending_run_count(&self) -> usize {
        self.lock().state.pending_run_count()
    }

    pub(super) fn min_pending(&self) -> Option<block::Height> {
        self.lock().state.min_pending()
    }

    pub(super) fn first_pending_in_range(
        &self,
        low: block::Height,
        high: block::Height,
    ) -> Option<block::Height> {
        if low > high {
            return None;
        }
        self.lock().state.first_pending_in_range(low, high)
    }

    pub(super) fn max_in_flight(&self) -> Option<block::Height> {
        self.lock().state.max_in_flight()
    }

    /// The lowest height above the floor that is in neither `pending` nor
    /// `in_flight` — the producer's query cursor.
    ///
    /// Anchored to the floor rather than to the highest claimed height, so a
    /// height that leaves the claimed set while higher heights stay claimed is
    /// offered to the next query instead of being stranded below a forward-only
    /// cursor. That happens whenever a destructive `reset_above` races an
    /// unreceived request, and whenever a refill batch is filtered down but its
    /// surviving heights still push `max_claimed` past the filtered one.
    ///
    /// `floor` is the caller's mirror of the download floor; the queue's own floor
    /// wins when the mirror lags, since heights at or below it are already GC'd.
    ///
    /// O(1) in the gap-free case: `pending`/`in_flight` are disjoint, so once the
    /// lowest claimed height is known to be `anchor + 1`, the claimed set spans
    /// `(anchor, max_claimed]` contiguously exactly when its size equals that span.
    /// Only a real gap pays for the merge walk, which stops at the first hole.
    pub(super) fn first_unclaimed_above(&self, floor: block::Height) -> Option<block::Height> {
        self.lock().state.first_unclaimed_above(floor)
    }

    /// Expected hash for a height in `pending` or `in_flight` (late-response
    /// recovery).
    pub(super) fn hash_for_height(&self, height: block::Height) -> Option<block::Hash> {
        self.item_for_height(height).map(|item| item.hash)
    }

    /// Current authority and identity, including work returned by a timed-out request.
    pub(super) fn item_for_height(&self, height: block::Height) -> Option<WorkItem> {
        self.lock().state.item_for_height(height)
    }

    /// Active request owner for a height, if it is currently reserved.
    pub(super) fn owner_for_height(
        &self,
        height: block::Height,
    ) -> Option<zakura_header_chain::BodyWorkOwner> {
        self.lock().state.owner_for_height(height)
    }

    /// Return the entries whose height no longer belongs to their owner, checked
    /// under one queue lock. `Copy` keeps request resources out of the lock.
    pub(super) fn disowned<T: Copy>(
        &self,
        entries: &[(block::Height, T)],
        owner_of: impl Fn(&T) -> zakura_header_chain::BodyWorkOwner,
    ) -> Vec<(block::Height, T)> {
        let inner = self.lock();
        entries
            .iter()
            .filter(|(height, entry)| {
                inner.state.owner_for_height(*height) != Some(owner_of(entry))
            })
            .copied()
            .collect()
    }

    pub(super) fn pending_contains(&self, height: block::Height) -> bool {
        self.lock().state.pending_contains(height)
    }

    pub(super) fn reserved_in_flight_charge(&self, height: block::Height) -> Option<u64> {
        self.lock().state.reserved_in_flight_charge(height)
    }

    pub(super) fn in_flight_contains(&self, height: block::Height) -> bool {
        self.lock().state.in_flight_contains(height)
    }
}
