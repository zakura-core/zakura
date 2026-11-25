//! Session admission checks and owned byte reservations for Zakura services.
//!
//! [`SessionGuard`] checks allowed inbound types, message size and per-peer
//! semantic meters. The transport worker applies its message-rate bucket and
//! size cap before forwarding frames to the service.
//!
//! Runtime resource owners use [`ByteBudget`] reservations for byte accounting.
//! A frame's admission through `SessionGuard` does not acquire a reservation.

use std::sync::Arc;

use zakura_rt::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Notify,
};

use super::{CloseRow, Frame};

/// Shared budget for owned byte charges.
///
/// Clones share one atomic counter. Reservations carry release authority.
/// Runtime owners choose the charged lifetime. An underflow closes admission
/// and preserves the last known charge.
#[derive(Clone, Debug)]
pub(crate) struct ByteBudget {
    inner: Arc<ByteBudgetInner>,
}

/// Capacity and closure observations without admission or settlement authority.
#[derive(Clone, Debug)]
pub(crate) struct BudgetReader(ByteBudget);

impl BudgetReader {
    pub(crate) fn available(&self) -> u64 {
        self.0.available()
    }

    pub(crate) fn reserved(&self) -> u64 {
        self.0.reserved()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    pub(crate) fn subscribe_capacity(&self) -> &Notify {
        self.0.subscribe_capacity()
    }

    /// Wait for permanent closure, including closure before registration.
    pub(crate) async fn closed(&self) {
        loop {
            let notified = self.0.inner.closure.notified();
            zakura_rt::pin!(notified);
            notified.as_mut().enable();
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug)]
struct ByteBudgetInner {
    max_bytes: u64,
    reserved_bytes: AtomicU64,
    closed: AtomicBool,
    closure: Notify,
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
    /// An owned charge on the settlement ledger.
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
    #[error("transfer amount exceeds owned funding")]
    Amount,
}

/// The local accounting failure that permanently closes a budget generation.
#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(crate) enum BudgetClosure {
    #[error("queue transition poisoned the accounting lock")]
    QueuePoisoned,
    #[error("{operation} cannot transfer {amount} bytes from {owned} owned bytes")]
    Arithmetic {
        operation: &'static str,
        owned: u64,
        amount: u64,
    },
    #[error("queue funding disagrees: logical={logical}, funded={funded}")]
    Funding { logical: u64, funded: u64 },
    #[error("counter cannot release {release} bytes")]
    CounterUnderflow { release: u64 },
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

    /// Transfer an admitted charge. Rejection preserves both owners and closes admission.
    pub(crate) fn merge(&mut self, other: &mut Self) -> Result<(), ReservationError> {
        debug_assert!(
            Arc::ptr_eq(&self.budget.inner, &other.budget.inner),
            "the owning queue admits all its funding from one budget"
        );
        let Some(total) = self.bytes.checked_add(other.bytes) else {
            self.budget.close(BudgetClosure::Arithmetic {
                operation: "merge",
                owned: self.bytes,
                amount: other.bytes,
            });
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
        debug_assert!(
            Arc::ptr_eq(&self.budget.inner, &release.budget.inner),
            "the owning queue starts its release from its own budget"
        );
        let amounts = self
            .bytes
            .checked_sub(bytes)
            .zip(release.bytes.checked_add(bytes));
        let Some((remaining, released)) = amounts else {
            self.budget.close(BudgetClosure::Arithmetic {
                operation: "deferred settlement",
                owned: self.bytes,
                amount: bytes,
            });
            return Err(ReservationError::Amount);
        };
        self.bytes = remaining;
        release.bytes = released;
        Ok(())
    }

    /// Transfer part of this charge. Rejection preserves funding and closes admission.
    pub(crate) fn split(&mut self, bytes: u64) -> Result<Self, ReservationError> {
        let Some(remaining) = self.bytes.checked_sub(bytes) else {
            self.budget.close(BudgetClosure::Arithmetic {
                operation: "split",
                owned: self.bytes,
                amount: bytes,
            });
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

    /// Own a charge of `bytes`. The budget is a settlement ledger: it refuses
    /// only after closing, or on overflow. An admission concurrent with closure
    /// can finish; the publisher must check closure before exposing its resource.
    // Rust 1.99 deprecates std's `fetch_update` for `try_update`, which the
    // 1.97 MSRV lacks. The simulator's atomic keeps its own `fetch_update`.
    #[allow(deprecated)]
    pub(crate) fn charge_owned(&self, bytes: u64) -> Option<ByteReservation> {
        if self.is_closed() {
            return None;
        }
        let reserved = self
            .inner
            .reserved_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                reserved.checked_add(bytes)
            })
            .ok()?;
        let next = reserved + bytes;
        publish_reserved_change(reserved, next);
        #[cfg(any(test, zakura_sim))]
        self.observe(BudgetChangeKind::Charged, reserved, next);
        Some(ByteReservation {
            budget: self.clone(),
            bytes,
        })
    }

    pub(crate) fn new(max_bytes: u64) -> Self {
        Self {
            inner: Arc::new(ByteBudgetInner {
                max_bytes,
                reserved_bytes: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                closure: Notify::new(),
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

    /// A failed accounting operation permanently closes this budget generation.
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Close admission immediately and wake closure waiters. A waiter registers
    /// before it reads the latch, so it cannot miss this wake.
    pub(crate) fn close(&self, cause: BudgetClosure) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            metrics::counter!("sync.block.budget.closed").increment(1);
            tracing::error!(
                %cause,
                reserved_bytes = self.reserved(),
                limit_bytes = self.inner.max_bytes,
                "block request accounting closed; retained owners remain charged"
            );
            self.inner.closure.notify_waiters();
        }
    }

    pub(crate) fn reader(&self) -> BudgetReader {
        BudgetReader(self.clone())
    }

    pub(crate) fn available(&self) -> u64 {
        if self.is_closed() {
            return 0;
        }
        self.inner
            .max_bytes
            .saturating_sub(self.inner.reserved_bytes.load(Ordering::Acquire))
    }

    #[cfg(any(test, feature = "p2p-fuzz", feature = "internal-bench"))]
    pub(crate) fn max_bytes_for_test(&self) -> u64 {
        self.inner.max_bytes
    }

    pub(crate) fn reserved(&self) -> u64 {
        self.inner.reserved_bytes.load(Ordering::Acquire)
    }

    fn release_charge(&self, bytes: u64) {
        self.release_then(bytes, || {});
    }

    /// Invoke `released` after the settlement attempt and before waking capacity
    /// waiters. A zero-byte release does nothing.
    // Rust 1.99 deprecates std's `fetch_update` for `try_update`, which the
    // 1.97 MSRV lacks. The simulator's atomic keeps its own `fetch_update`.
    #[allow(deprecated)]
    fn release_then(&self, bytes: u64, released: impl FnOnce()) {
        if bytes == 0 {
            return;
        }
        match self.inner.reserved_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |reserved| reserved.checked_sub(bytes),
        ) {
            Ok(reserved) => {
                let next = reserved - bytes;
                publish_reserved_change(reserved, next);
                #[cfg(any(test, zakura_sim))]
                self.observe(BudgetChangeKind::Released, reserved, next);
            }
            Err(_) => self.close(BudgetClosure::CounterUnderflow { release: bytes }),
        }
        released();
        self.inner.capacity.notify_waiters();
    }

    /// Subscribe to capacity-freed notifications. A consumer blocked on a full
    /// budget registers `subscribe_capacity().notified()` *before* re-reading
    /// `available()`/`reserve_owned`, so a concurrent `release`/`shrink` can never
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
    /// The peer broke the close-spec row and should be disconnected.
    Reject(CloseRow),
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
    /// Per-peer semantic meters.
    meters: PeerMeters,
}

impl SessionGuard {
    /// Build the service's type, size and semantic-meter guard.
    pub(crate) fn new(allowed: &'static [u8], max_bytes: u32) -> Self {
        Self {
            allowed,
            max_bytes,
            meters: PeerMeters::new(),
        }
    }

    /// Admit every single-byte discriminator under the size cap.
    /// The service decoder validates which message types its protocol accepts.
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
            meters: PeerMeters::new(),
        }
    }

    /// Admit one inbound frame through the service-specific protections.
    pub(crate) fn admit(&mut self, frame: &Frame) -> Admit {
        // `message_type` is a wire `u16`; allowed message types are single
        // bytes, so a value that does not fit in a `u8` is by definition not an
        // allowed type and is treated as a protocol violation.
        let Ok(ty) = u8::try_from(frame.message_type) else {
            return Admit::Reject(CloseRow::REG_CLOSE_2);
        };
        if !self.allowed.contains(&ty) {
            return Admit::Reject(CloseRow::REG_CLOSE_2);
        }
        // `max_bytes` is a `u32`; widen to `usize` for the payload-length
        // comparison (`usize` is at least 32 bits on supported targets).
        if frame.payload.len() > self.max_bytes as usize {
            return Admit::Reject(CloseRow::REG_CLOSE_1);
        }
        if !self.meters.try_take(ty) {
            return Admit::Throttle;
        }
        Admit::Pass
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const ALLOWED: &[u8] = &[1, 2];

    /// A waker that runs `on_wake` each time something wakes it.
    pub(crate) fn callback_waker(on_wake: impl Fn() + Send + Sync + 'static) -> std::task::Waker {
        struct Callback<F>(F);
        impl<F: Fn() + Send + Sync + 'static> std::task::Wake for Callback<F> {
            fn wake(self: Arc<Self>) {
                (self.0)();
            }
        }
        std::task::Waker::from(Arc::new(Callback(on_wake)))
    }

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
        let unrelated = budget.charge_owned(17).unwrap();
        let mut funding = budget.charge_owned(400).unwrap();
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
    fn deferred_settlement_survives_unwind() {
        let budget = ByteBudget::new(1000);
        let unrelated = budget.charge_owned(17).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut funding = budget.charge_owned(200).unwrap();
            let mut release = budget.deferred_release();
            funding.settle_into(&mut release, 100).unwrap();
            panic!("transition interrupted after funding transfer");
        }));
        assert!(result.is_err());
        assert_eq!(budget.reserved(), 17);
        drop(unrelated);
    }

    #[test]
    fn rejected_transfer_preserves_pool_and_deferred_release() {
        let budget = ByteBudget::new(400);
        let mut funding = budget.charge_owned(200).unwrap();
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
        use std::{future::Future, task::Context};
        let budget = ByteBudget::new(1_000);
        let funding = budget.charge_owned(400).unwrap();
        let clone = budget.clone();
        let reader = budget.reader();
        let mut closed = Box::pin(reader.closed());
        let wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = wakes.clone();
        let waker = callback_waker(move || {
            counted.fetch_add(1, Ordering::SeqCst);
        });
        let mut cx = Context::from_waker(&waker);
        assert!(closed.as_mut().poll(&mut cx).is_pending());
        // A corrupt owner must not release the valid owner's charge.
        drop(ByteReservation {
            budget: budget.clone(),
            bytes: 401,
        });
        assert!(
            wakes.load(Ordering::SeqCst) > 0,
            "counter failure must notify closure"
        );
        assert!(closed.as_mut().poll(&mut cx).is_ready());
        assert!(clone.is_closed());
        assert_eq!(clone.reserved(), 400);
        assert_eq!(clone.available(), 0);
        assert!(
            clone.charge_owned(1).is_none(),
            "floor cannot bypass closure"
        );
        drop(funding);
        assert_eq!(clone.reserved(), 0, "existing owners can still settle");
        assert!(clone.is_closed(), "drain must not reopen admission");
        assert_eq!(clone.available(), 0);
    }

    #[test]
    fn byte_budget_charge_overdrafts_past_the_max() {
        let budget = ByteBudget::new(1_000);
        let ordinary = budget.charge_owned(900).unwrap();
        let overdraft = budget.charge_owned(300).unwrap();
        assert_eq!(budget.reserved(), 1_200);
        assert_eq!(budget.available(), 0);
        drop(overdraft);
        assert_eq!(budget.reserved(), 900);
        drop(ordinary);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn admit_rejects_disallowed_type() {
        let mut guard = SessionGuard::new(ALLOWED, 1_024);
        assert_eq!(
            guard.admit(&frame(99, 0)),
            Admit::Reject(CloseRow::REG_CLOSE_2)
        );
    }

    #[test]
    fn admit_rejects_non_u8_type() {
        let mut guard = SessionGuard::new(ALLOWED, 1_024);
        assert_eq!(
            guard.admit(&frame(0x0100, 0)),
            Admit::Reject(CloseRow::REG_CLOSE_2)
        );
    }

    #[test]
    fn admit_rejects_oversize() {
        let mut guard = SessionGuard::new(ALLOWED, 4);
        assert_eq!(
            guard.admit(&frame(1, 5)),
            Admit::Reject(CloseRow::REG_CLOSE_1)
        );
    }

    #[test]
    fn admit_passes_allowed_under_caps() {
        let mut guard = SessionGuard::new(ALLOWED, 1_024);
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
        assert_eq!(
            guard.admit(&frame(0x0100, 0)),
            Admit::Reject(CloseRow::REG_CLOSE_2)
        );
    }

    #[test]
    fn oversize_only_rejects_oversize() {
        let mut guard = SessionGuard::oversize_only(4);
        assert_eq!(
            guard.admit(&frame(1, 5)),
            Admit::Reject(CloseRow::REG_CLOSE_1)
        );
    }
}
