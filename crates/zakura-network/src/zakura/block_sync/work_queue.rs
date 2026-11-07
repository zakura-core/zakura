//! Download work source for Zakura block sync.
//!
//! The [`WorkQueue`] is the sole shared download-scheduling primitive: a sorted
//! set of needed block heights the per-peer issuance path pulls from. It replaces
//! the central `BlockRangeScheduler`'s eligibility/dedup/retry roles with a small
//! API the caller drives from its own per-peer state (see the ):
//!
//! - a height is in **exactly one** of `{below-floor (gone), pending, in_flight}`;
//! - [`take_request`](WorkQueue::take_request) moves a contiguous-ascending run
//!   `pending → in_flight` (so one taken chunk maps to one `BlockRangeRequest`),
//!   bounded only by the caller's servable range and a count cap — never by how
//!   far above the download floor the heights already are;
//! - only [`release_reserved_and_return_items_detailed_for_owner`](WorkQueue::release_reserved_and_return_items_detailed_for_owner)
//!   (timeout/disconnect retry) and [`reset_above`](WorkQueue::reset_above) move
//!   `in_flight → pending`;
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

use super::request::BlockSizeEstimate;
use crate::zakura::transport::{
    BudgetClosure, BudgetReader, ByteBudget, ByteRelease, ByteReservation, ReservationError,
};

mod request_write;
pub(super) mod state;

#[cfg(any(test, zakura_sim))]
pub use request_write::RequestWritePhase;
use request_write::RequestWriteRegistration;
pub(super) use request_write::{
    AdmitRefusal, RequestWrite, RequestWriteStatus, TakenRequest, UnpublishedRequest,
};
pub(super) use state::{OwnerReservationCap, WorkItem, WorkReturnOutcome};
#[cfg(any(test, zakura_sim))]
pub use state::{WorkItemOwnership, WorkItemPlace};
use state::{WorkQueueState, WorkWake};

/// A request reservation that a received body ended.
#[derive(Debug)]
pub(super) struct EndedReservation {
    /// The height's own charge: the estimate its request reserved for it.
    pub(super) charge: u64,
    /// Owns the charge and the unsent heights of a queued request that receipt retired.
    pub(super) funding: ByteReservation,
}

/// The queue lock's contents: the logical owner plus the runtime registrations
/// that must change atomically with it.
#[derive(Debug)]
struct WorkQueueInner {
    state: WorkQueueState,
    funding: ByteReservation,
    /// Weak owners allow reset notification without retaining session resources.
    /// Their status survives until writing or terminal cleanup finishes.
    request_writes: HashMap<zakura_header_chain::BodyWorkOwner, RequestWriteRegistration>,
}

impl WorkQueueInner {
    fn settle(&mut self, release: &mut ByteRelease<'_>, bytes: u64) {
        // Rejection closes the budget and preserves both owners.
        let _ = self.funding.settle_into(release, bytes);
    }
}

/// Publishes a changed reservation gauge and closes the budget on a ledger fault
/// before releasing the queue lock.
struct WorkQueueGuard<'a> {
    inner: MutexGuard<'a, WorkQueueInner>,
    reserved_bytes: u64,
    budget: &'a ByteBudget,
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
        if let Some(fault) = self.inner.state.ledger_fault() {
            self.budget.close(BudgetClosure::Arithmetic {
                operation: fault.operation,
                owned: fault.total,
                amount: fault.amount,
            });
        }
    }
}

/// The shared download work source. See the module docs for the invariants.
#[derive(Debug)]
pub(super) struct WorkQueue {
    inner: StdMutex<WorkQueueInner>,
    budget: ByteBudget,
    /// Caps the reservations one session may publish. Admission checks it.
    owner_cap: OwnerReservationCap,
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
    pub(super) fn new(floor: block::Height, budget: ByteBudget) -> Self {
        Self {
            inner: StdMutex::new(WorkQueueInner {
                state: WorkQueueState::new(floor),
                funding: budget.empty_reservation(),
                request_writes: HashMap::new(),
            }),
            budget,
            owner_cap: OwnerReservationCap::UNLIMITED,
            available: Notify::new(),
            revocations: AtomicU64::new(0),
            #[cfg(any(test, zakura_sim))]
            observer: Default::default(),
        }
    }

    /// This queue, capping each session's published reservations at `cap`.
    pub(super) fn with_owner_cap(mut self, cap: OwnerReservationCap) -> Self {
        self.owner_cap = cap;
        self
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
            super::block_sync_observe::BlockEvent::WorkQueue(inner.state.ownership())
        );
    }

    pub(super) fn budget(&self) -> BudgetReader {
        self.budget.reader()
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

    /// The budget this queue owns, so a test can hold an unrelated charge.
    #[cfg(test)]
    pub(super) fn budget_for_tests(&self) -> ByteBudget {
        self.budget.clone()
    }

    #[cfg(test)]
    pub(super) fn set_estimate_floor_for_tests(&self, floor: u64) {
        self.lock().state.set_estimate_floor(floor);
    }

    fn lock(&self) -> WorkQueueGuard<'_> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            // Cleanup can acquire this lock during unwind. Keep the interrupted
            // ledger and its funding, but prevent further admission/publication.
            self.budget.close(BudgetClosure::QueuePoisoned);
            poisoned.into_inner()
        });
        let reserved_bytes = inner.state.reserved_bytes();
        WorkQueueGuard {
            inner,
            reserved_bytes,
            budget: &self.budget,
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
    #[cfg(test)]
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

    /// Give the provisional take an exact owner before releasing the queue lock.
    /// A reset followed by a new take cannot be undone by this attempt's cleanup.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn take_request(
        self: &Arc<Self>,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
        session_id: u64,
        request_id: NonZeroU64,
    ) -> TakenRequest {
        let items = self.lock().state.take_budgeted(
            low,
            high,
            max_count,
            max_estimated_bytes,
            (session_id, request_id),
        );
        TakenRequest::new(self.clone(), items)
    }

    /// Put back provisional heights **without** notifying waiters.
    ///
    /// Used by a peer routine to put back a chunk it took but chose not to issue
    /// (e.g. the heights are in its own short retry-avoid window after it just
    /// failed them). Notifying here would re-wake the returning routine's own
    /// freshly-registered `available` future and busy-loop the want-work arm
    /// (a self-wake spin); other peers were already woken by the original failure
    /// return, so suppressing the notify only affects the caller.
    fn return_unpublished(&self, items: &[(block::Height, WorkItem)]) {
        self.lock().state.return_unpublished(items);
    }

    /// Transfer this owner's receipt charge into the body handoff.
    /// The returned reservation retains its admission budget until it drops.
    pub(super) fn begin_receipt_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) -> Result<Option<EndedReservation>, ReservationError> {
        self.begin_receipt(|state| {
            state
                .end_reservation_at_receipt(owner, height, false)
                .map(|charge| (owner, charge))
        })
        .map(|ended| ended.map(|(_, ended)| ended))
    }

    /// Transfer a competing request's charge when another attempt receives its body.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn begin_competing_receipt(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) -> Result<Option<EndedReservation>, ReservationError> {
        self.begin_receipt(|state| {
            state
                .end_reservation_at_receipt(owner, height, true)
                .map(|charge| (owner, charge))
        })
        .map(|ended| ended.map(|(_, ended)| ended))
    }

    /// Transfer the charge of whichever published request reserves `height`
    /// for `hash` when an attempt that holds no request for it receives its
    /// body. The owner and hash checks and the transfer share one queue lock,
    /// so neither a request that takes the height concurrently nor one reissued
    /// for another hash loses its charge. Returns `None` for pending work,
    /// which has no owner.
    pub(super) fn begin_unmatched_receipt(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<(zakura_header_chain::BodyWorkOwner, EndedReservation)>, ReservationError>
    {
        self.begin_receipt(|state| state.end_competing_reservation_at_receipt(height, hash))
    }

    fn begin_receipt(
        &self,
        end: impl FnOnce(&mut WorkQueueState) -> Option<(zakura_header_chain::BodyWorkOwner, u64)>,
    ) -> Result<Option<(zakura_header_chain::BodyWorkOwner, EndedReservation)>, ReservationError>
    {
        let (ended, claim) = {
            let mut inner = self.lock();
            let Some((owner, charge)) = end(&mut inner.state) else {
                return Ok(None);
            };
            let (outcome, claim) = self.return_items_locked(&mut inner, owner, []);
            let funding = self.split_receipt_funding(&mut inner, charge, outcome.released_bytes);
            (
                funding.map(|funding| (owner, EndedReservation { charge, funding })),
                claim,
            )
        };
        drop(claim);
        ended.map(Some)
    }

    fn split_receipt_funding(
        &self,
        inner: &mut WorkQueueInner,
        receipt: u64,
        retired: u64,
    ) -> Result<ByteReservation, ReservationError> {
        let funding = receipt
            .checked_add(retired)
            .ok_or(ReservationError::Amount)
            .and_then(|bytes| inner.funding.split(bytes));
        if funding.is_err() {
            self.budget.close(BudgetClosure::Arithmetic {
                operation: "receipt settlement",
                owned: receipt,
                amount: retired,
            });
        }
        funding
    }

    /// Claim a received height and transfer its funding into the handoff.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn claim_receipt_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) -> Result<ByteReservation, ReservationError> {
        let (released, claim) = {
            let mut inner = self.lock();
            let Some(charge) = inner.state.claim_received(owner, height) else {
                return inner.funding.split(0);
            };
            let (outcome, claim) = self.return_items_locked(&mut inner, owner, []);
            (
                self.split_receipt_funding(&mut inner, charge, outcome.released_bytes),
                claim,
            )
        };
        drop(claim);
        released
    }

    pub(super) fn release_reserved_heights_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) {
        let mut release = self.budget.deferred_release();
        let mut inner = self.lock();
        let bytes = inner.state.release_reserved_heights(owner, heights);
        inner.settle(&mut release, bytes);
        drop(inner);
        drop(release);
    }

    /// Return this owner's unreceived heights. Expiring a queued frame also
    /// returns every other unsent height owned by that request under the same lock.
    pub(super) fn release_reserved_and_return_items_detailed_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> WorkReturnOutcome {
        let mut release = self.budget.deferred_release();
        let (outcome, claim) = {
            let mut inner = self.lock();
            let result = self.return_items_locked(&mut inner, owner, heights);
            inner.settle(&mut release, result.0.released_bytes);
            result
        };
        drop(release);
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
        owner: zakura_header_chain::BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> (WorkReturnOutcome, Option<Arc<RequestWrite>>) {
        let mut heights: BTreeSet<_> = heights.into_iter().collect();
        let registration = inner.request_writes.get(&owner);
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
    /// Settle request estimates for unreceived heights after unlocking.
    /// Received bodies retain their separate payload ownership.
    pub(super) fn advance_floor(&self, floor: block::Height) {
        let mut release = self.budget.deferred_release();
        let mut inner = self.lock();
        let delta = inner.state.advance_floor(floor);
        inner.settle(&mut release, delta.released_bytes);
        drop(inner);
        drop(release);
        self.wake(delta.wake);
    }

    /// Frontier reset: pin the floor and drop every `pending`/`in_flight` entry
    /// `> floor` (their buffers were dropped; the producer re-fills via the next
    /// query).
    ///
    /// Settle request estimates for removed heights after unlocking.
    pub(super) fn reset_above(&self, floor: block::Height) {
        let mut release = self.budget.deferred_release();
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
        inner.settle(&mut release, delta.released_bytes);
        drop(inner);
        drop(release);
        drop(claims);
        self.wake(delta.wake);
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

    /// Close admission and publication when queue funding disagrees with its ledger.
    /// Reset can drain owners but cannot reopen this queue.
    pub(super) fn audit_funding(&self) -> bool {
        let inner = self.lock();
        let logical = inner.state.reserved_bytes();
        let funded = inner.funding.bytes();
        // Unpublished requests and receipt handoffs can retain additional charge.
        if logical != funded || self.budget.reserved() < funded {
            metrics::counter!("sync.block.budget.audit_drift").increment(1);
            self.budget
                .close(BudgetClosure::Funding { logical, funded });
        }
        !self.budget.is_closed()
    }

    /// Sum of reserved request-estimate bytes across `pending` + `in_flight`.
    ///
    /// Returns the maintained counter. Tests compare it with
    /// `reserved_bytes_scanned` to detect drift.
    #[cfg(test)]
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.lock().state.reserved_bytes()
    }

    /// Ground-truth O(pending + in_flight) recomputation of [`reserved_bytes`],
    /// used by tests to assert the maintained counter never drifts.
    #[cfg(test)]
    pub(super) fn reserved_bytes_scanned(&self) -> u64 {
        self.lock().state.reserved_bytes_scanned()
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

    #[cfg(test)]
    pub(super) fn reserved_in_flight_charge(&self, height: block::Height) -> Option<u64> {
        self.lock().state.reserved_in_flight_charge(height)
    }

    pub(super) fn in_flight_contains(&self, height: block::Height) -> bool {
        self.lock().state.in_flight_contains(height)
    }
}

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use state::{
    trace_work_funding_bytes, trace_work_ownership_bytes, FundingReceiptView, FundingRequestPhase,
    FundingRequestView, WorkFundingHistory, WorkFundingStep, WorkFundingView, WorkOwnershipEntry,
    WorkOwnershipFailure, WorkOwnershipHistory, WorkOwnershipPhase, WorkOwnershipReplaySummary,
    WorkOwnershipStep, WorkOwnershipView,
};
