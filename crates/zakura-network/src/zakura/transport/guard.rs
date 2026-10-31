//! Shared per-session protections for Zakura services.
//!
//! [`SessionGuard`] is the single home for the protections that wrap a peer
//! session: the allowed inbound message-type filter, the optional byte budget,
//! and the per-peer semantic meters. It reuses the existing limiter primitives
//! rather than inventing new limiter math.
//!
//! Boundary note (do not double-count). The transport's per-connection,
//! per-stream-kind message-rate `TokenBucket` and the oversize check already
//! run in the transport stream worker *before* a frame reaches the service.
//! `SessionGuard` therefore owns only the **service-specific** protections
//! (allowed-types, byte budget, per-peer semantic meters); the transport keeps
//! its connection-global count bucket exactly as-is. Document this split at the
//! [`SessionGuard::new`] call site.

use std::sync::Arc;

use zakura_rt::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Notify,
};

use super::Frame;

/// Reservation budget for inflight stream payloads.
///
/// Promoted here from `block_sync/state.rs` so byte-rate protection is reusable
/// across services; only block_sync currently passes `Some(..)` to a guard.
///
/// Clones share one atomic counter. Reservations carry release authority.
/// Raw counter operations stay inside this module for `SessionGuard`.
/// An underflow closes admission and preserves the last known charge.
#[derive(Clone, Debug)]
pub(crate) struct ByteBudget {
    inner: Arc<ByteBudgetInner>,
}

#[derive(Debug)]
struct ByteBudgetInner {
    max_bytes: u64,
    reserved_bytes: AtomicU64,
    closed: AtomicBool,
    /// Notified whenever bytes are released/shrunk, so a consumer blocked on a
    /// full budget can re-check capacity. Uses `notify_waiters` (no stored
    /// permit), so a waiter must register `subscribe_capacity().notified()`
    /// before re-reading the budget.
    capacity: Notify,
    /// Receives each counter change, in test and simulator builds.
    #[cfg(any(test, zakura_sim))]
    observer: Option<BudgetObserver>,
}

/// One change of a [`ByteBudget`]'s counter, as a test or simulator observer
/// sees it.
#[cfg(any(test, zakura_sim))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BudgetChange {
    /// Which operation changed the counter.
    pub kind: BudgetChangeKind,
    /// The counter before the change.
    pub before: u64,
    /// The counter after the change.
    pub after: u64,
    /// The budget's maximum, which a reservation may not pass.
    pub limit: u64,
}

/// Which [`ByteBudget`] operation changed its counter.
#[cfg(any(test, zakura_sim))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BudgetChangeKind {
    /// A reservation within the budget's maximum.
    Reserved,
    /// A charge that skipped the admission gate: a floor overdraft.
    Charged,
    /// A release.
    Released,
}

/// The callback a [`ByteBudget`] reports each counter change to.
#[cfg(any(test, zakura_sim))]
#[derive(Clone)]
pub(crate) struct BudgetObserver(pub(crate) Arc<dyn Fn(BudgetChange) + Send + Sync>);

#[cfg(any(test, zakura_sim))]
impl std::fmt::Debug for BudgetObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BudgetObserver")
    }
}

/// Owns a charge against the budget that admitted it.
/// Transfers preserve the budget association and never reacquire capacity.
#[must_use = "retain the reservation while its resource is owned"]
#[derive(Debug)]
pub(crate) struct ByteReservation {
    budget: ByteBudget,
    bytes: u64,
}

/// Owns an aggregate release until its caller leaves the protected transition.
/// Borrowing the budget avoids an Arc operation for immediate settlement.
#[must_use = "retain the release until the protected transition has unlocked"]
#[derive(Debug)]
pub(crate) struct ByteRelease<'a> {
    budget: &'a ByteBudget,
    bytes: u64,
}

impl Drop for ByteRelease<'_> {
    fn drop(&mut self) {
        self.budget.release_charge(self.bytes);
    }
}

/// A rejected ownership transfer leaves both owners unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ReservationError {
    #[error("reservation belongs to another budget")]
    Budget,
    #[error("transfer amount exceeds owned funding")]
    Amount,
}

impl ByteReservation {
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn release_then(mut self, released: impl FnOnce()) {
        let bytes = std::mem::take(&mut self.bytes);
        self.budget.release_then(bytes, released);
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn same_budget(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.budget.inner, &other.budget.inner)
    }

    /// Transfer an admitted charge. Rejection preserves both owners and closes admission.
    pub(crate) fn merge(&mut self, other: &mut Self) -> Result<(), ReservationError> {
        if !self.same_budget(other) {
            self.budget.close();
            other.budget.close();
            return Err(ReservationError::Budget);
        }
        let Some(total) = self.bytes.checked_add(other.bytes) else {
            self.budget.close();
            return Err(ReservationError::Amount);
        };
        self.bytes = total;
        other.bytes = 0;
        Ok(())
    }

    /// Transfer funding to a deferred release. Rejection preserves both owners.
    pub(crate) fn settle_into(
        &mut self,
        release: &mut ByteRelease<'_>,
        bytes: u64,
    ) -> Result<(), ReservationError> {
        if !Arc::ptr_eq(&self.budget.inner, &release.budget.inner) {
            self.budget.close();
            release.budget.close();
            return Err(ReservationError::Budget);
        }
        let amounts = self
            .bytes
            .checked_sub(bytes)
            .zip(release.bytes.checked_add(bytes));
        let Some((remaining, released)) = amounts else {
            self.budget.close();
            return Err(ReservationError::Amount);
        };
        self.bytes = remaining;
        release.bytes = released;
        Ok(())
    }

    /// Transfer part of this charge. Rejection preserves funding and closes admission.
    pub(crate) fn split(&mut self, bytes: u64) -> Result<Self, ReservationError> {
        let Some(remaining) = self.bytes.checked_sub(bytes) else {
            self.budget.close();
            return Err(ReservationError::Amount);
        };
        self.bytes = remaining;
        Ok(Self {
            budget: self.budget.clone(),
            bytes,
        })
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        self.budget.release_charge(self.bytes);
    }
}

impl ByteBudget {
    /// Start an empty release batch before acquiring the resource owner's lock.
    pub(crate) fn deferred_release(&self) -> ByteRelease<'_> {
        ByteRelease {
            budget: self,
            bytes: 0,
        }
    }

    /// Create an empty owner bound to this budget.
    pub(crate) fn empty_reservation(&self) -> ByteReservation {
        ByteReservation {
            budget: self.clone(),
            bytes: 0,
        }
    }

    /// Admit an owned charge. An admission concurrent with closure can finish;
    /// the publisher must check closure before exposing its resource.
    pub(crate) fn reserve_owned(&self, bytes: u64) -> Option<ByteReservation> {
        self.try_reserve(bytes).then(|| ByteReservation {
            budget: self.clone(),
            bytes,
        })
    }

    /// Admit the existing floor-progress exception and own its charge.
    pub(crate) fn charge_owned(&self, bytes: u64) -> Option<ByteReservation> {
        if self.is_closed() {
            return None;
        }
        let mut reserved = self.inner.reserved_bytes.load(Ordering::Acquire);
        loop {
            let next = reserved.checked_add(bytes)?;
            match self.inner.reserved_bytes.compare_exchange_weak(
                reserved,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    publish_reserved_change(reserved, next);
                    #[cfg(any(test, zakura_sim))]
                    self.observe(BudgetChangeKind::Charged, reserved, next);
                    return Some(ByteReservation {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(observed) => reserved = observed,
            }
        }
    }

    pub(crate) fn new(max_bytes: u64) -> Self {
        Self {
            inner: Arc::new(ByteBudgetInner {
                max_bytes,
                reserved_bytes: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                capacity: Notify::new(),
                #[cfg(any(test, zakura_sim))]
                observer: None,
            }),
        }
    }

    /// This new budget, reporting each counter change to `observer`.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn with_observer(mut self, observer: Option<BudgetObserver>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("a budget gets its observer before anything clones it")
            .observer = observer;
        self
    }

    /// Reports one counter change to the observer, if one is installed.
    #[cfg(any(test, zakura_sim))]
    fn observe(&self, kind: BudgetChangeKind, before: u64, after: u64) {
        if let Some(observer) = &self.inner.observer {
            (observer.0)(BudgetChange {
                kind,
                before,
                after,
                limit: self.inner.max_bytes,
            });
        }
    }

    /// A failed accounting operation closes this budget until its owner is replaced.
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    pub(crate) fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            metrics::counter!("sync.block.budget.closed").increment(1);
        }
    }

    pub(crate) fn available(&self) -> u64 {
        if self.is_closed() {
            return 0;
        }
        self.inner
            .max_bytes
            .saturating_sub(self.inner.reserved_bytes.load(Ordering::Acquire))
    }

    #[cfg(test)]
    pub(crate) fn max_bytes_for_test(&self) -> u64 {
        self.inner.max_bytes
    }

    pub(crate) fn reserved(&self) -> u64 {
        self.inner.reserved_bytes.load(Ordering::Acquire)
    }

    /// Reserve `bytes` against the shared counter, or fail if the budget cannot
    /// cover it. A CAS loop so concurrent reservers never over-commit the max.
    fn try_reserve(&self, bytes: u64) -> bool {
        if bytes == 0 || self.is_closed() {
            return false;
        }
        let mut reserved = self.inner.reserved_bytes.load(Ordering::Acquire);
        loop {
            let available = self.inner.max_bytes.saturating_sub(reserved);
            if bytes > available {
                return false;
            }
            let next = reserved.saturating_add(bytes);
            match self.inner.reserved_bytes.compare_exchange_weak(
                reserved,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    publish_reserved_change(reserved, next);
                    #[cfg(any(test, zakura_sim))]
                    self.observe(BudgetChangeKind::Reserved, reserved, next);
                    return true;
                }
                Err(observed) => reserved = observed,
            }
        }
    }

    /// Release `bytes` back to the shared counter and wake
    /// any consumer blocked on capacity.
    fn release(&mut self, bytes: u64) {
        self.release_charge(bytes);
    }

    fn release_charge(&self, bytes: u64) {
        self.release_then(bytes, || {});
    }

    /// [`Self::release`] that calls `released` after the counter changes and
    /// before waking waiters, so an observer sees the release before any task
    /// it wakes runs. Does nothing for 0 bytes.
    fn release_then(&self, bytes: u64, released: impl FnOnce()) {
        if bytes == 0 {
            return;
        }
        let mut reserved = self.inner.reserved_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = reserved.checked_sub(bytes) else {
                self.close();
                break;
            };
            match self.inner.reserved_bytes.compare_exchange_weak(
                reserved,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    publish_reserved_change(reserved, next);
                    #[cfg(any(test, zakura_sim))]
                    self.observe(BudgetChangeKind::Released, reserved, next);
                    break;
                }
                Err(observed) => reserved = observed,
            }
        }
        released();
        self.inner.capacity.notify_waiters();
    }

    /// Audit the shared counter against an externally-derived expected value.
    ///
    /// Handoff ordering can transiently leave the budget above the source
    /// ledger; only a shortfall proves a double release or lost charge.
    pub(crate) fn audit(&self, expected: u64, _context: &'static str) -> bool {
        let actual = self.reserved();
        let ok = !self.is_closed() && actual >= expected;
        if !ok {
            metrics::counter!("sync.block.budget.audit_drift").increment(1);
        }
        ok
    }

    /// Subscribe to capacity-freed notifications. A consumer blocked on a full
    /// budget registers `subscribe_capacity().notified()` *before* re-reading
    /// `available()`/`try_reserve`, so a concurrent `release`/`shrink` can never
    /// be missed between the check and the wait.
    pub(crate) fn subscribe_capacity(&self) -> &Notify {
        &self.inner.capacity
    }
}

/// Apply one change of the shared reservation counter, from `before` to
/// `after`, to its gauge.
///
/// Gauge increments commute, so concurrent reservers and releasers leave the
/// gauge equal to the counter. A rejected release leaves both unchanged.
fn publish_reserved_change(before: u64, after: u64) {
    let gauge = metrics::gauge!("sync.block.budget.reserved_bytes");
    // These lossy casts are metrics-only; the counter keeps the integer value.
    if after >= before {
        gauge.increment((after - before) as f64);
    } else {
        gauge.decrement((before - after) as f64);
    }
}

/// Outcome of admitting one inbound frame through a [`SessionGuard`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Admit {
    /// The frame is admitted for processing.
    Pass,
    /// The frame is dropped but the peer is kept (rate/budget back-pressure).
    Throttle,
    /// The peer violated a protocol-level protection and should be disconnected.
    Reject(&'static str),
}

/// Per-peer semantic meters (status spam, new-block spam,...).
///
/// Minimal in Phase 0: a pass-through that admits everything. The real
/// per-service semantic meters (the `RateMeter`s currently in the per-peer
/// state) are moved here in a later migration phase.
#[derive(Debug)]
pub(crate) struct PeerMeters;

impl PeerMeters {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn try_take(&mut self, _message_type: u8) -> bool {
        true
    }
}

/// The single home for the protections that wrap one peer session.
#[derive(Debug)]
pub(crate) struct SessionGuard {
    /// Allowed inbound message types for this stream kind.
    allowed: &'static [u8],
    /// Maximum reassembled message size in bytes.
    max_bytes: u32,
    /// Optional byte budget; `None` for services without byte-rate protection.
    byte_budget: Option<ByteBudget>,
    /// Per-peer semantic meters.
    meters: PeerMeters,
}

impl SessionGuard {
    /// Build a session guard for one peer stream.
    ///
    /// Boundary note (do not double-count): the transport already applies the
    /// per-connection, per-stream-kind message-rate bucket and the oversize cap
    /// before frames reach this guard. Pass `Some(..)` for `byte_budget` only
    /// when this service owns byte-rate protection (block_sync); header_sync and
    /// others pass `None`.
    ///
    /// Phase 1 header_sync uses [`SessionGuard::oversize_only`] (the allowed-type
    /// filter stays off so the decode stage remains the sole validity arbiter);
    /// the explicit-`allowed` constructor is consumed when block_sync/discovery
    /// move their allowed-type lists onto the guard in later phases.
    #[allow(dead_code)] // consumed when block_sync/discovery move their type filters onto the guard
    pub(crate) fn new(
        allowed: &'static [u8],
        max_bytes: u32,
        byte_budget: Option<ByteBudget>,
    ) -> Self {
        Self {
            allowed,
            max_bytes,
            byte_budget,
            meters: PeerMeters::new(),
        }
    }

    /// Build a guard that applies only the oversize cap and admits every type.
    ///
    /// This is the behavior-preserving configuration for a service that has not
    /// yet moved its allowed-type filter and per-peer semantic meters behind the
    /// guard: the decode stage remains the sole arbiter of message validity, so
    /// the exact same wire events fire as before the lift. The allowed-type
    /// filter (`ALL_TYPES`) and the meters are wired per-service in later phases;
    /// `byte_budget` stays `None` because only block_sync owns byte-rate
    /// protection.
    pub(crate) fn oversize_only(max_bytes: u32) -> Self {
        // An empty `allowed` slot would reject every type; `ALL_TYPES` admits
        // every `u8` discriminator so type validity is left to the decode stage.
        const ALL_TYPES: &[u8] = &{
            let mut all = [0u8; 256];
            let mut ty = 0usize;
            while ty < all.len() {
                // `ty` ranges 0..=255 so the `as u8` truncation is exact.
                all[ty] = ty as u8;
                ty += 1;
            }
            all
        };
        Self {
            allowed: ALL_TYPES,
            max_bytes,
            byte_budget: None,
            meters: PeerMeters::new(),
        }
    }

    /// Admit one inbound frame through the service-specific protections.
    pub(crate) fn admit(&mut self, frame: &Frame) -> Admit {
        // `message_type` is a wire `u16`; allowed message types are single
        // bytes, so a value that does not fit in a `u8` is by definition not an
        // allowed type and is treated as a protocol violation.
        let Ok(ty) = u8::try_from(frame.message_type) else {
            return Admit::Reject("bad type");
        };
        if !self.allowed.contains(&ty) {
            return Admit::Reject("disallowed type");
        }
        // `max_bytes` is a `u32`; widen to `usize` for the payload-length
        // comparison (`usize` is at least 32 bits on supported targets).
        if frame.payload.len() > self.max_bytes as usize {
            return Admit::Reject("oversize");
        }
        if let Some(budget) = &mut self.byte_budget {
            // Payload length fits in `u64` on all supported targets.
            if !budget.try_reserve(frame.payload.len() as u64) {
                return Admit::Throttle;
            }
        }
        if !self.meters.try_take(ty) {
            return Admit::Throttle;
        }
        Admit::Pass
    }

    /// Return reserved bytes to the byte budget once a message is processed.
    ///
    /// Unused until a service with a byte budget (block_sync) moves onto the
    /// guard; header_sync passes `byte_budget: None`, so it never calls this.
    #[allow(dead_code)] // consumed when block_sync moves its byte budget onto the guard
    pub(crate) fn release(&mut self, bytes: u64) {
        if let Some(budget) = &mut self.byte_budget {
            budget.release(bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALLOWED: &[u8] = &[1, 2];

    fn frame(message_type: u16, payload_len: usize) -> Frame {
        Frame {
            message_type,
            flags: 0,
            payload: vec![0u8; payload_len],
        }
    }

    #[test]
    fn deferred_settlement_retains_charge_until_drop() {
        let budget = ByteBudget::new(1000);
        let unrelated = budget.reserve_owned(17).unwrap();
        let mut funding = budget.reserve_owned(400).unwrap();
        let mut release = budget.deferred_release();
        funding.settle_into(&mut release, 100).unwrap();
        funding.settle_into(&mut release, 50).unwrap();
        assert_eq!(funding.bytes(), 250);
        assert_eq!(budget.reserved(), 417);
        drop(funding);
        assert_eq!(budget.reserved(), 167);
        drop(release);
        assert_eq!(budget.reserved(), 17);
        drop(unrelated);
    }

    #[test]
    fn deferred_settlement_rejects_wrong_budget_without_transferring() {
        let budget = ByteBudget::new(1000);
        let other = ByteBudget::new(1000);
        let mut funding = budget.reserve_owned(200).unwrap();
        let mut release = other.deferred_release();
        assert_eq!(
            funding.settle_into(&mut release, 200),
            Err(ReservationError::Budget)
        );
        assert!(budget.is_closed());
        assert!(other.is_closed());
        assert_eq!(funding.bytes(), 200);
        drop(release);
        assert_eq!(other.reserved(), 0);
        assert_eq!(budget.reserved(), 200);
        drop(funding);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn deferred_settlement_survives_unwind() {
        let budget = ByteBudget::new(1000);
        let unrelated = budget.reserve_owned(17).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut funding = budget.reserve_owned(200).unwrap();
            let mut release = budget.deferred_release();
            funding.settle_into(&mut release, 100).unwrap();
            panic!("transition interrupted after funding transfer");
        }));
        assert!(result.is_err());
        assert_eq!(budget.reserved(), 17);
        drop(unrelated);
    }

    #[test]
    fn rejected_merge_preserves_equal_charges_in_both_budgets() {
        let first = ByteBudget::new(400);
        let second = ByteBudget::new(400);
        let mut destination = first.reserve_owned(200).unwrap();
        let mut source = second.reserve_owned(200).unwrap();
        assert_eq!(
            destination.merge(&mut source),
            Err(ReservationError::Budget),
            "equal charges do not authorize cross-budget transfer"
        );
        assert_eq!((destination.bytes(), source.bytes()), (200, 200));
        assert_eq!((first.reserved(), second.reserved()), (200, 200));
        assert!(first.is_closed() && second.is_closed());
        drop(source);
        assert_eq!((first.reserved(), second.reserved()), (200, 0));
        drop(destination);
        assert_eq!(first.reserved(), 0);
    }

    #[test]
    fn rejected_transfer_preserves_pool_and_deferred_release() {
        let budget = ByteBudget::new(400);
        let mut funding = budget.reserve_owned(200).unwrap();
        let mut release = budget.deferred_release();
        funding.settle_into(&mut release, 50).unwrap();
        assert_eq!(
            funding.settle_into(&mut release, 151),
            Err(ReservationError::Amount)
        );
        assert_eq!(funding.bytes(), 150);
        assert_eq!(release.bytes, 50);
        assert!(matches!(funding.split(151), Err(ReservationError::Amount)));
        assert_eq!(funding.bytes(), 150);
        assert_eq!(budget.reserved(), 200);
        assert!(budget.is_closed());
        drop(funding);
        assert_eq!(budget.reserved(), 50);
        drop(release);
        assert_eq!(budget.reserved(), 0);
        assert!(budget.is_closed());
    }

    #[test]
    fn underflow_keeps_charge_and_closes_all_admission() {
        let mut budget = ByteBudget::new(1_000);
        let funding = budget.reserve_owned(400).unwrap();
        let clone = budget.clone();
        budget.release(401);
        assert!(clone.is_closed());
        assert_eq!(clone.reserved(), 400);
        assert_eq!(clone.available(), 0);
        assert!(clone.reserve_owned(1).is_none());
        assert!(
            clone.charge_owned(1).is_none(),
            "floor cannot bypass closure"
        );
        assert!(!clone.audit(400, "closed budget"));
        drop(funding);
        assert_eq!(clone.reserved(), 0, "existing owners can still settle");
        assert!(clone.is_closed(), "drain must not reopen admission");
        assert_eq!(clone.available(), 0);
    }

    #[test]
    fn byte_budget_reserves_and_releases() {
        let mut budget = ByteBudget::new(1_000);
        assert_eq!(budget.available(), 1_000);
        assert!(budget.try_reserve(400));
        assert_eq!(budget.reserved(), 400);
        assert_eq!(budget.available(), 600);
        // Zero-byte and over-budget reservations are rejected without mutation.
        assert!(!budget.try_reserve(0));
        assert!(!budget.try_reserve(601));
        assert_eq!(budget.reserved(), 400);
        budget.release(400);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn byte_budget_charge_overdrafts_past_the_max() {
        let budget = ByteBudget::new(1_000);
        let ordinary = budget.reserve_owned(900).unwrap();
        let overdraft = budget.charge_owned(300).unwrap();
        assert_eq!(budget.reserved(), 1_200);
        assert_eq!(budget.available(), 0);
        assert!(!budget.try_reserve(1));
        drop(overdraft);
        assert_eq!(budget.reserved(), 900);
        drop(ordinary);
        assert_eq!(budget.reserved(), 0);
    }

    // A `ByteBudget` is cloned and shared across the block-sync Sequencer and every
    // per-peer routine, all reserving concurrently against one counter. The reservation
    // path must never over-commit the max — the property the CAS loop in `try_reserve`
    // exists for. This drives many reservers at a tight budget and asserts the shared
    // counter never exceeds the max; a regression to a non-atomic load-modify-store
    // would over-commit under this contention.
    #[test]
    fn byte_budget_concurrent_reservations_never_over_commit() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        use std::sync::{Arc, Barrier};
        use std::thread;

        const CHUNK: u64 = 4_096;
        const CAP: u64 = 8;
        const THREADS: usize = 16; // deliberately oversubscribed: 16 reservers, 8 slots
        let max = CHUNK * CAP;
        let budget = ByteBudget::new(max);

        // Phase 1 — deterministic oversubscription. Every thread tries to reserve one
        // CHUNK simultaneously and holds it across a barrier, so the counter sits at its
        // peak with all reservations decided. Exactly CAP may be admitted; never more.
        let start = Arc::new(Barrier::new(THREADS));
        let all_reserved = Arc::new(Barrier::new(THREADS));
        let successes = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let mut budget = budget.clone();
            let start = start.clone();
            let all_reserved = all_reserved.clone();
            let successes = successes.clone();
            handles.push(thread::spawn(move || {
                start.wait();
                let ok = budget.try_reserve(CHUNK);
                if ok {
                    successes.fetch_add(1, AtomicOrdering::AcqRel);
                }
                // All reservations are decided and still held: the counter is at its peak.
                all_reserved.wait();
                assert!(
                    budget.reserved() <= max,
                    "reserved {} exceeded the budget max {max} (over-commit)",
                    budget.reserved(),
                );
                if ok {
                    budget.release(CHUNK);
                }
            }));
        }
        for handle in handles {
            handle.join().expect("reserver thread panicked");
        }
        assert_eq!(
            u64::try_from(successes.load(AtomicOrdering::Acquire)).unwrap(),
            CAP,
            "exactly CAP reservations may be admitted; the rest must be rejected",
        );
        assert_eq!(
            budget.reserved(),
            0,
            "every admitted reservation was released"
        );

        // Phase 2 — sustained churn. Many reserve/release rounds under contention, each
        // asserting the counter is never over budget.
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let mut budget = budget.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..5_000 {
                    if budget.try_reserve(CHUNK) {
                        assert!(budget.reserved() <= max, "over-commit during churn");
                        budget.release(CHUNK);
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().expect("churn thread panicked");
        }
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn admit_rejects_disallowed_type() {
        let mut guard = SessionGuard::new(ALLOWED, 1_024, None);
        assert_eq!(guard.admit(&frame(99, 0)), Admit::Reject("disallowed type"));
    }

    #[test]
    fn admit_rejects_non_u8_type() {
        let mut guard = SessionGuard::new(ALLOWED, 1_024, None);
        assert_eq!(guard.admit(&frame(0x0100, 0)), Admit::Reject("bad type"));
    }

    #[test]
    fn admit_rejects_oversize() {
        let mut guard = SessionGuard::new(ALLOWED, 4, None);
        assert_eq!(guard.admit(&frame(1, 5)), Admit::Reject("oversize"));
    }

    #[test]
    fn admit_throttles_when_budget_exhausted() {
        let mut guard = SessionGuard::new(ALLOWED, 1_024, Some(ByteBudget::new(8)));
        // First frame reserves 8 bytes and passes.
        assert_eq!(guard.admit(&frame(1, 8)), Admit::Pass);
        // Second frame cannot reserve and is throttled (peer kept).
        assert_eq!(guard.admit(&frame(1, 8)), Admit::Throttle);
        // Releasing the budget admits the next frame again.
        guard.release(8);
        assert_eq!(guard.admit(&frame(1, 8)), Admit::Pass);
    }

    #[test]
    fn admit_passes_allowed_under_caps() {
        let mut guard = SessionGuard::new(ALLOWED, 1_024, None);
        assert_eq!(guard.admit(&frame(1, 16)), Admit::Pass);
        assert_eq!(guard.admit(&frame(2, 16)), Admit::Pass);
    }

    #[test]
    fn oversize_only_admits_all_u8_types_under_cap() {
        let mut guard = SessionGuard::oversize_only(1_024);
        // Every single-byte discriminator is admitted, including ones no
        // service knows about, so decode stays the arbiter of type validity.
        assert_eq!(guard.admit(&frame(0, 0)), Admit::Pass);
        assert_eq!(guard.admit(&frame(99, 16)), Admit::Pass);
        assert_eq!(guard.admit(&frame(255, 16)), Admit::Pass);
    }

    #[test]
    fn oversize_only_rejects_non_u8_type() {
        let mut guard = SessionGuard::oversize_only(1_024);
        assert_eq!(guard.admit(&frame(0x0100, 0)), Admit::Reject("bad type"));
    }

    #[test]
    fn oversize_only_rejects_oversize() {
        let mut guard = SessionGuard::oversize_only(4);
        assert_eq!(guard.admit(&frame(1, 5)), Admit::Reject("oversize"));
    }
}
