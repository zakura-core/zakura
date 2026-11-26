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
use zakura_node_services::body_claims::SharedBodyClaims;
use zakura_rt::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex as StdMutex, MutexGuard, Notify,
    },
    time::Instant,
};

use super::request::BlockSizeEstimate;
use crate::zakura::{
    memory::{ConnectionCredit, Consumer, MemoryPool, PoolLease},
    transport::{
        BudgetClosure, BudgetReader, ByteBudget, ByteRelease, ByteReservation, ReservationError,
    },
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
    /// Pool lease transferred from the work queue for this received height
    /// (`decision-4`: follows the body from request credit into sequencer retention).
    pub(super) pool_lease: Option<PoolLease>,
    /// Connection credit controller that holds the matching QUIC receive window
    /// increase for this request, if any.
    pub(super) conn_credit: Option<ConnectionCredit>,
}

/// Pool lease and connection credit controller owned by one published request
/// while its heights remain unreceived in [`WorkQueueInner`].
#[derive(Debug)]
pub(super) struct OwnerLease {
    pub(super) lease: PoolLease,
    pub(super) conn_credit: Option<ConnectionCredit>,
}

/// Deferred release/drain batch for pool leases extracted under the queue lock
/// and settled only after unlocking (never calling `set_limits` or dropping
/// leases inside the work-queue mutex).
#[derive(Debug, Default)]
struct LeaseDrainBatch {
    entries: Vec<(Option<ConnectionCredit>, PoolLease)>,
}

impl LeaseDrainBatch {
    fn push(&mut self, conn_credit: Option<ConnectionCredit>, lease: PoolLease) {
        if lease.bytes() > 0 {
            self.entries.push((conn_credit, lease));
        }
    }
}

impl Drop for LeaseDrainBatch {
    fn drop(&mut self) {
        for (conn_credit, lease) in self.entries.drain(..) {
            if let Some(credit) = conn_credit {
                finish_receive_credit(&credit, lease.bytes(), Some(lease));
            } else {
                drop(lease);
            }
        }
    }
}

/// Finish `total` bytes of `credit`'s receive credit, and queue `unspent`, the
/// lease of the credit the peer didn't deliver, on its drain queue (`02 §6`).
/// A Consensus drain moves to `Consumer::BlockSync` first.
pub(super) fn finish_receive_credit(
    credit: &ConnectionCredit,
    total: u64,
    unspent: Option<PoolLease>,
) {
    let Some(unspent) = unspent else {
        credit.finish_receive_credit(total, None);
        return;
    };
    let (drain, left) = drain_on_block_sync(unspent);
    credit.finish_receive_credit(total, Some(drain));
    if let Some(left) = left {
        credit.finish_receive_credit(0, Some(left));
    }
}

/// Move a Consensus drain, or a detached submission's lease, to
/// `Consumer::BlockSync`, and return the moved lease and the bytes left on
/// Consensus.
///
/// The height then holds no Consensus lease, so its re-issued request takes
/// the only one (mem-15). Only a full pool leaves bytes on Consensus; block
/// sync counts them.
pub(super) fn drain_on_block_sync(mut lease: PoolLease) -> (PoolLease, Option<PoolLease>) {
    if lease.consumer() != Consumer::Consensus {
        return (lease, None);
    }
    let drain = lease.move_to(Consumer::BlockSync);
    if lease.bytes() == 0 {
        return (drain, None);
    }
    metrics::counter!("sync.block.consensus.drain_unmoved_bytes").increment(lease.bytes());
    (drain, Some(lease))
}

/// The queue lock's contents: the logical owner plus the runtime registrations
/// that must change atomically with it.
#[derive(Debug)]
struct WorkQueueInner {
    state: WorkQueueState,
    funding: ByteReservation,
    /// Per-owner pool leases backing published requests.
    owner_leases: HashMap<zakura_header_chain::BodyWorkOwner, OwnerLease>,
    /// Weak owners allow reset notification without retaining session resources.
    /// Their status survives until writing or terminal cleanup finishes.
    request_writes: HashMap<zakura_header_chain::BodyWorkOwner, RequestWriteRegistration>,
}

impl WorkQueueInner {
    fn settle(&mut self, release: &mut ByteRelease<'_>, bytes: u64) {
        // Rejection closes the budget and preserves both owners.
        let _ = self.funding.settle_into(release, bytes);
    }

    fn take_owner_lease(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        bytes: u64,
    ) -> (Option<ConnectionCredit>, Option<PoolLease>) {
        if bytes == 0 {
            return (None, None);
        }
        let Some(entry) = self.owner_leases.get_mut(&owner) else {
            return (None, None);
        };
        let conn_credit = entry.conn_credit.clone();
        let take = bytes.min(entry.lease.bytes());
        let lease = if take == entry.lease.bytes() {
            self.owner_leases
                .remove(&owner)
                .map(|removed| removed.lease)
        } else {
            entry.lease.split(take)
        };
        (conn_credit, lease)
    }

    fn settle_owner_lease(
        &mut self,
        drain: &mut LeaseDrainBatch,
        owner: zakura_header_chain::BodyWorkOwner,
        bytes: u64,
    ) {
        let (conn_credit, lease) = self.take_owner_lease(owner, bytes);
        if let Some(lease) = lease {
            drain.push(conn_credit, lease);
        }
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

/// A pool whose reserved Consensus and bulk BlockSync consumers each lease
/// up to `bytes`, the shape block sync leases from, for tests and fuzzers.
#[cfg(any(test, feature = "p2p-fuzz", feature = "internal-bench"))]
pub(super) fn test_memory_pool(bytes: u64) -> MemoryPool {
    use crate::zakura::memory::{Consumer, ConsumerSpec, PoolConfig};
    // Keep the pool's sums far from overflow for "unlimited" test budgets.
    let bytes = bytes.min(1 << 50);
    MemoryPool::new(
        PoolConfig::new(bytes.saturating_mul(2), 0)
            .with_consumer(Consumer::Consensus, ConsumerSpec::reserved(bytes))
            .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(0, bytes, 1)),
    )
}

/// The shared download work source. See the module docs for the invariants.
#[derive(Debug)]
pub(super) struct WorkQueue {
    inner: StdMutex<WorkQueueInner>,
    budget: ByteBudget,
    /// The node-wide memory pool funding published requests (`02 §4`).
    memory_pool: MemoryPool,
    /// Caps the reservations one session may publish. Admission checks it.
    owner_cap: OwnerReservationCap,
    /// Bodies another v2 front is fetching. Selection skips them, and a
    /// request's writer refuses to start while any of its bodies is claimed.
    body_claims: Option<SharedBodyClaims>,
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
    /// A queue whose requests lease from `memory_pool`. `budget` is the
    /// settlement ledger of request owners; the pool bounds the bytes.
    pub(super) fn new(floor: block::Height, budget: ByteBudget, memory_pool: MemoryPool) -> Self {
        Self {
            inner: StdMutex::new(WorkQueueInner {
                state: WorkQueueState::new(floor),
                funding: budget.empty_reservation(),
                owner_leases: HashMap::new(),
                request_writes: HashMap::new(),
            }),
            budget,
            memory_pool,
            owner_cap: OwnerReservationCap::UNLIMITED,
            body_claims: None,
            available: Notify::new(),
            revocations: AtomicU64::new(0),
            #[cfg(any(test, zakura_sim))]
            observer: Default::default(),
        }
    }

    /// A queue whose pool gives Consensus and BlockSync each the byte limit
    /// of `budget`, for tests and fuzzers that size requests by that limit.
    #[cfg(any(test, feature = "p2p-fuzz", feature = "internal-bench"))]
    pub(super) fn with_test_pool(floor: block::Height, budget: ByteBudget) -> Self {
        let pool = test_memory_pool(budget.max_bytes_for_test());
        Self::new(floor, budget, pool)
    }

    /// The memory pool that funds this queue's requests.
    pub(super) fn memory_pool(&self) -> &MemoryPool {
        &self.memory_pool
    }

    /// This queue, capping each session's published reservations at `cap`.
    pub(super) fn with_owner_cap(mut self, cap: OwnerReservationCap) -> Self {
        self.owner_cap = cap;
        self
    }

    /// This queue, skipping bodies claimed in `claims`.
    pub(super) fn with_body_claims(mut self, claims: Option<SharedBodyClaims>) -> Self {
        self.body_claims = claims;
        self
    }

    fn is_body_claimed(&self, hash: block::Hash, now: Instant) -> bool {
        self.body_claims
            .as_ref()
            .is_some_and(|claims| claims.is_claimed(hash, now))
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
        let now = Instant::now();
        let items = self.lock().state.take_budgeted(
            low,
            high,
            max_count,
            max_estimated_bytes,
            (session_id, request_id),
            |hash| self.is_body_claimed(hash, now),
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
        let mut lease_drain = LeaseDrainBatch::default();
        let (ended, claim) = {
            let mut inner = self.lock();
            let Some((owner, charge)) = end(&mut inner.state) else {
                return Ok(None);
            };
            let (outcome, claim) = self.return_items_locked(&mut inner, owner, []);
            let funding = self.split_receipt_funding(&mut inner, charge, outcome.released_bytes);
            let (conn_credit, pool_lease) = if funding.is_ok() {
                inner.settle_owner_lease(&mut lease_drain, owner, outcome.released_bytes);
                inner.take_owner_lease(owner, charge)
            } else {
                (None, None)
            };
            (
                funding.map(|funding| {
                    (
                        owner,
                        EndedReservation {
                            charge,
                            funding,
                            pool_lease,
                            conn_credit,
                        },
                    )
                }),
                claim,
            )
        };
        drop(lease_drain);
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
        let mut lease_drain = LeaseDrainBatch::default();
        let (released, claim) = {
            let mut inner = self.lock();
            let Some(charge) = inner.state.claim_received(owner, height) else {
                return inner.funding.split(0);
            };
            let (outcome, claim) = self.return_items_locked(&mut inner, owner, []);
            let funding = self.split_receipt_funding(&mut inner, charge, outcome.released_bytes);
            if funding.is_ok() {
                inner.settle_owner_lease(
                    &mut lease_drain,
                    owner,
                    charge.saturating_add(outcome.released_bytes),
                );
            }
            (funding, claim)
        };
        drop(lease_drain);
        drop(claim);
        released
    }

    pub(super) fn release_reserved_heights_for_owner(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) {
        let mut release = self.budget.deferred_release();
        let mut lease_drain = LeaseDrainBatch::default();
        let mut inner = self.lock();
        let bytes = inner.state.release_reserved_heights(owner, heights);
        inner.settle(&mut release, bytes);
        inner.settle_owner_lease(&mut lease_drain, owner, bytes);
        drop(inner);
        drop(lease_drain);
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
        let mut lease_drain = LeaseDrainBatch::default();
        let (outcome, claim) = {
            let mut inner = self.lock();
            let result = self.return_items_locked(&mut inner, owner, heights);
            inner.settle(&mut release, result.0.released_bytes);
            inner.settle_owner_lease(&mut lease_drain, owner, result.0.released_bytes);
            result
        };
        drop(lease_drain);
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
        self.return_received(owner, height, hash, bytes, Some(verified_tip))
    }

    /// Return a received attempt whose body the pool could not lease, at its
    /// measured size. Its next request leases the whole body, so it waits for
    /// the pool instead of retaining unleased bytes.
    pub(super) fn return_unleased_body(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        hash: block::Hash,
        bytes: u64,
    ) -> bool {
        self.return_received(owner, height, hash, bytes, None)
    }

    fn return_received(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        hash: block::Hash,
        bytes: u64,
        retry_after_tip: Option<block::Height>,
    ) -> bool {
        let deferred =
            self.lock()
                .state
                .defer_received(owner, height, hash, bytes, retry_after_tip);
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
        let mut lease_drain = LeaseDrainBatch::default();
        let mut inner = self.lock();
        let delta = inner.state.advance_floor(floor);
        inner.settle(&mut release, delta.released_bytes);
        for (owner, bytes) in &delta.released_by_owner {
            inner.settle_owner_lease(&mut lease_drain, *owner, *bytes);
        }
        drop(inner);
        drop(lease_drain);
        drop(release);
        self.wake(delta.wake);
    }

    /// Frontier reset: pin the floor and drop every `pending`/`in_flight` entry
    /// `> floor` (their buffers were dropped; the producer re-fills via the next
    /// query), plus the entries at or below a raised floor.
    ///
    /// Settle request estimates for removed heights after unlocking.
    pub(super) fn reset_above(&self, floor: block::Height) {
        let mut release = self.budget.deferred_release();
        let mut lease_drain = LeaseDrainBatch::default();
        let mut inner = self.lock();
        // Retain upgraded owners until after unlocking: a concurrently dropped
        // queue entry can make this the last reference, whose Drop settles work.
        let claims: Vec<_> = inner
            .request_writes
            .values()
            .filter_map(|registration| registration.claim.upgrade())
            .collect();
        // A claim wholly at or below the floor is left as `advance_floor` leaves
        // it. Its heights leave the queue, so `try_start` skips an unwritten frame.
        for claim in claims.iter().filter(|claim| claim.has_height_above(floor)) {
            claim.reset();
            inner.request_writes.remove(&claim.owner());
        }
        let delta = inner.state.reset_above(floor);
        inner.settle(&mut release, delta.released_bytes);
        for (owner, bytes) in &delta.released_by_owner {
            inner.settle_owner_lease(&mut lease_drain, *owner, *bytes);
        }
        drop(inner);
        drop(lease_drain);
        drop(release);
        drop(claims);
        self.wake(delta.wake);
    }

    /// The "work added" notifier (per-peer routines wake source).
    pub(super) fn subscribe_available(&self) -> &Notify {
        &self.available
    }

    /// Wakes routines after a body claim releases or expires, because a
    /// skipped pending height may now be takeable.
    pub(super) fn wake_after_body_claim_change(&self) {
        self.available.notify_waiters();
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
        let now = Instant::now();
        self.lock()
            .state
            .first_pending_in_range(low, high, |hash| self.is_body_claimed(hash, now))
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

    /// Return the entries whose height the queue no longer charges to their
    /// owner, checked under one queue lock: another owner holds the height,
    /// or the item is `answered_elsewhere`, because another attempt's body
    /// ended the owner's reservation. The owner's request still claims an
    /// answered height, because its peer still owes the body, but the budget
    /// no longer counts it.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn uncharged<T: Copy>(
        &self,
        entries: &[(block::Height, T)],
        owner_of: impl Fn(&T) -> zakura_header_chain::BodyWorkOwner,
    ) -> Vec<(block::Height, T)> {
        let inner = self.lock();
        entries
            .iter()
            .filter(|(height, entry)| {
                inner.state.owner_for_height(*height) != Some(owner_of(entry))
                    || inner.state.answered_elsewhere(*height)
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

#[cfg(test)]
mod memory_pool_tests {
    use std::time::Duration;

    use zakura_rt::sync::CancellationToken;

    use super::*;
    use crate::zakura::{
        block_sync::state::LookAheadCap,
        memory::{Consumer, ConsumerSpec, MemoryPool, PoolConfig},
    };

    fn test_scope() -> zakura_header_chain::BodyWorkAuthority {
        crate::zakura::block_sync::test_work_scope()
    }

    const MB: u64 = 1_000_000;

    fn hash(height: u32) -> block::Hash {
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&height.to_le_bytes());
        block::Hash(bytes)
    }

    /// A queue over `pool` with a confirmed 2 MB block at each of `heights`.
    fn queue_with(pool: &MemoryPool, heights: &[u32]) -> Arc<WorkQueue> {
        let queue = Arc::new(WorkQueue::new(
            block::Height(0),
            ByteBudget::new(u64::MAX),
            pool.clone(),
        ));
        queue.extend(
            test_scope(),
            heights.iter().map(|&height| {
                (
                    block::Height(height),
                    hash(height),
                    BlockSizeEstimate::Confirmed(2_000_000),
                )
            }),
        );
        queue
    }

    /// Takes `height` as request `id` and admits it for connection `conn_id`,
    /// with the floor and the verified tip at genesis.
    fn admit(
        queue: &Arc<WorkQueue>,
        height: u32,
        id: u64,
        conn_id: u64,
        cancel: &CancellationToken,
    ) -> Result<UnpublishedRequest, AdmitRefusal> {
        queue
            .take_request(
                block::Height(height),
                block::Height(height),
                1,
                2 * MB,
                1,
                NonZeroU64::new(id).unwrap(),
            )
            .admit_for_connection(
                block::Height(0),
                block::Height(0),
                conn_id,
                None,
                cancel.clone(),
            )
    }

    /// mem-15: a withheld commit-window drain moves to block sync, so the
    /// height's re-issued request takes its only Consensus lease, even with
    /// block sync at its cap. A full pool leaves the drain on Consensus, and
    /// the re-issued request waits.
    #[test]
    fn withheld_consensus_drain_moves_to_block_sync_before_reissue() {
        for pool_full in [false, true] {
            // Consensus reserves exactly one 2 MB height. S = 4 MB.
            let config = PoolConfig::new(7 * MB, 0)
                .with_consumer(Consumer::Consensus, ConsumerSpec::reserved(2 * MB))
                .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MB, MB, 1))
                .with_consumer(Consumer::Serving, ConsumerSpec::bulk(0, 4 * MB, 1));
            let pool = MemoryPool::new(config);
            let _bulk = pool
                .try_acquire(Consumer::BlockSync, Some(1), MB)
                .expect("block sync fills its cap");
            let _serving = pool_full.then(|| {
                pool.try_acquire(Consumer::Serving, Some(2), 4 * MB)
                    .expect("serving fills S")
            });
            let queue = queue_with(&pool, &[100]);
            // The withheld request's unspent credit lease.
            let withheld = pool
                .try_acquire(Consumer::Consensus, Some(42), 2 * MB)
                .expect("the first request leases the height from Consensus");

            let (drain, left) = drain_on_block_sync(withheld);
            let reissue = admit(&queue, 100, 1, 43, &CancellationToken::new());
            if pool_full {
                assert_eq!(drain.bytes(), 0);
                assert_eq!(left.as_ref().map(PoolLease::bytes), Some(2 * MB));
                assert_eq!(
                    reissue.map(|_| ()).unwrap_err(),
                    AdmitRefusal::Capacity(Consumer::Consensus)
                );
            } else {
                assert!(left.is_none());
                assert_eq!(drain.consumer(), Consumer::BlockSync);
                assert_eq!(drain.bytes(), 2 * MB);
                assert_eq!(pool.consumer_leased(Consumer::BlockSync), 3 * MB);
                let reissue =
                    reissue.expect("the re-issued request takes the only Consensus lease");
                let _claim = reissue.publish(|_| {}).expect("publish the re-issue");
                assert_eq!(pool.consumer_leased(Consumer::Consensus), 2 * MB);
            }
            assert!(pool.snapshot().total_leased <= pool.snapshot().lease_limit);
        }
    }

    #[test]
    fn commit_window_uses_consensus_reserve_even_when_bulk_pool_is_saturated() {
        // Shared space S = 8 MB; Consensus reserved = 8 MB; BlockSync floor = 2 MB, cap = 4 MB.
        let config = PoolConfig::new(20 * MB, 0)
            .with_consumer(Consumer::Consensus, ConsumerSpec::reserved(8 * MB))
            .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(2 * MB, 4 * MB, 1))
            .with_consumer(Consumer::Serving, ConsumerSpec::bulk(2 * MB, 10 * MB, 1));
        let pool = MemoryPool::new(config);

        // Saturate bulk completely (BlockSync floor + cap and Serving taking all of S).
        let bulk_block_sync = pool
            .try_acquire(Consumer::BlockSync, Some(1), 4 * MB)
            .expect("fill block sync to cap");
        let bulk_serving = pool
            .try_acquire(Consumer::Serving, Some(2), 8 * MB)
            .expect("drain remaining shared space S");
        assert_eq!(pool.available_for(Consumer::BlockSync), 0);

        // Height 100 is inside commit window above verified_tip=0 (0..=401);
        // Height 500 is above the commit window.
        let queue = queue_with(&pool, &[100, 500]);
        let cancel = CancellationToken::new();
        // 1. Commit-window request succeeds on Consumer::Consensus without waiting on bulk!
        let unpublished_in = admit(&queue, 100, 1, 42, &cancel)
            .expect("commit-window height must lease from Consensus reserve");
        let owner_in = unpublished_in.owner();
        let _claim_in = unpublished_in.publish(|_| {}).expect("publish in-window");
        assert_eq!(pool.consumer_leased(Consumer::Consensus), 2 * MB);

        // 2. Above-window request is refused cleanly while bulk is full.
        let err = admit(&queue, 500, 2, 42, &cancel)
            .expect_err("above-window request must defer when bulk is saturated");
        assert_eq!(err, AdmitRefusal::Capacity(Consumer::BlockSync));

        // 3. Releasing bulk leases unblocks above-window requests on Consumer::BlockSync.
        drop(bulk_block_sync);
        drop(bulk_serving);
        let unpublished_above = admit(&queue, 500, 3, 42, &cancel)
            .expect("above-window request leases from BlockSync once room opens");
        let _claim_above = unpublished_above
            .publish(|_| {})
            .expect("publish above-window");
        assert_eq!(pool.consumer_leased(Consumer::BlockSync), 2 * MB);

        // 4. Receipt transfers the Consensus lease out of WorkQueue; dropping it returns to 0.
        let ended = queue
            .begin_receipt_for_owner(owner_in, block::Height(100))
            .expect("receipt succeeds")
            .expect("owner held height 100");
        assert_eq!(
            ended.pool_lease.as_ref().map(PoolLease::bytes),
            Some(2 * MB)
        );
        assert_eq!(
            ended.pool_lease.as_ref().map(PoolLease::consumer),
            Some(Consumer::Consensus)
        );
        assert_eq!(pool.consumer_leased(Consumer::Consensus), 2 * MB);
        drop(ended);
        assert_eq!(pool.consumer_leased(Consumer::Consensus), 0);

        // 5. Returning unreceived work releases the BlockSync lease back to 0.
        queue.reset_above(block::Height(0));
        assert_eq!(pool.consumer_leased(Consumer::BlockSync), 0);
        assert!(queue.audit_funding());
    }

    #[test]
    fn range_fetch_p99_and_derived_look_ahead_cap() {
        let mut cap = LookAheadCap::new(None);
        assert_eq!(cap.p99_fetch_time(), None);

        // 99 fast samples at 100 ms + 1 slow tail sample at 2000 ms -> p99 = 100 ms,
        // then 2 slow samples at 2000 ms -> p99 = 2000 ms.
        for _ in 0..99 {
            cap.record_fetch(Duration::from_millis(100));
        }
        cap.record_fetch(Duration::from_millis(2_000));
        assert_eq!(cap.p99_fetch_time(), Some(Duration::from_millis(100)));

        cap.record_fetch(Duration::from_millis(2_000));
        assert_eq!(cap.p99_fetch_time(), Some(Duration::from_millis(2_000)));

        // Idle at tip (commit_rate == 0) yields 0 look-ahead.
        assert_eq!(cap.look_ahead_bytes(), 0);

        // 2 × 100 MB/s × 2.0 s = 400 MB.
        let rate = 100 * 1024 * 1024;
        cap.tick(rate, 0);
        assert_eq!(cap.look_ahead_bytes(), 400 * 1024 * 1024);

        // A ceiling lowers the derived cap.
        let mut capped = LookAheadCap::new(Some(256 * 1024 * 1024));
        capped.record_fetch(Duration::from_secs(2));
        capped.tick(rate, 0);
        assert_eq!(capped.look_ahead_bytes(), 256 * 1024 * 1024);
    }

    /// A floor stall that stops commits for a few ticks keeps most of the
    /// look-ahead; the smoothed rate halves in about 30 seconds.
    #[test]
    fn look_ahead_cap_decays_through_a_commit_stall() {
        let mut cap = LookAheadCap::new(None);
        for _ in 0..100 {
            cap.record_fetch(Duration::from_secs(1));
        }
        let rate = 100 * 1024 * 1024;
        let full = cap.tick(rate, 0);
        assert_eq!(full, 2 * rate, "2 × rate × the 1 s p99");

        let mut stalled = full;
        for _ in 0..5 {
            stalled = cap.tick(0, 0);
        }
        assert!(stalled > full * 85 / 100, "{stalled} of {full} after 5 s");
        for _ in 5..30 {
            stalled = cap.tick(0, 0);
        }
        assert!(
            (full * 45 / 100..=full * 55 / 100).contains(&stalled),
            "{stalled} of {full} after 30 s"
        );
        // A recovered rate restores the cap at once, and the cwnd sum adds.
        assert_eq!(cap.tick(rate, 7), 2 * rate + 7);
    }
}
