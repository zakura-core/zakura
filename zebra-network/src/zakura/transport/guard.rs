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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::sync::Notify;

use super::Frame;

/// Byte-rate reservation budget for inflight stream payloads.
///
/// Promoted here from `block_sync/state.rs` so byte-rate protection is reusable
/// across services; only block_sync currently passes `Some(..)` to a guard.
///
/// A cheap `Clone` handle over a shared atomic reservation counter. Every clone
/// reserves and releases against the *same* counter, so one budget can be shared
/// across tasks (the block-sync Sequencer and, later, the per-peer routines)
/// without a lock. The reserve/release methods keep their `&mut self` receiver:
/// each owner holds its own clone, so `&mut` to that clone never aliases another
/// owner's, and the shared counter is mutated through the atomic regardless.
#[derive(Clone, Debug)]
pub(crate) struct ByteBudget {
    inner: Arc<ByteBudgetInner>,
}

#[derive(Debug)]
struct ByteBudgetInner {
    max_bytes: u64,
    reserved_bytes: AtomicU64,
    /// Notified whenever bytes are released/shrunk, so a consumer blocked on a
    /// full budget can re-check capacity. Uses `notify_waiters` (no stored
    /// permit), so a waiter must register `subscribe_capacity().notified()`
    /// before re-reading the budget.
    capacity: Notify,
}

impl ByteBudget {
    pub(crate) fn new(max_bytes: u64) -> Self {
        Self {
            inner: Arc::new(ByteBudgetInner {
                max_bytes,
                reserved_bytes: AtomicU64::new(0),
                capacity: Notify::new(),
            }),
        }
    }

    pub(crate) fn available(&self) -> u64 {
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
    pub(crate) fn try_reserve(&mut self, bytes: u64) -> bool {
        if bytes == 0 {
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
                Ok(_) => return true,
                Err(observed) => reserved = observed,
            }
        }
    }

    /// Release `bytes` back to the shared counter (saturating at zero) and wake
    /// any consumer blocked on capacity.
    pub(crate) fn release(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self.inner.reserved_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |reserved| Some(reserved.saturating_sub(bytes)),
        );
        self.inner.capacity.notify_waiters();
    }

    /// Add `bytes` to the shared counter without applying the admission gate.
    ///
    /// Used when a body was already admitted based on an estimate and its actual
    /// serialized size is larger than that estimate. The request cannot be
    /// rejected at this point, so the budget must record the overshoot and let
    /// later releases drain it.
    pub(crate) fn charge(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.inner
            .reserved_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                Some(reserved.saturating_add(bytes))
            })
            .ok();
    }

    /// Settle an estimated reservation to the actual bytes now held.
    ///
    /// If `actual` is smaller, this releases slack. If it is larger, this charges
    /// the overshoot so held bodies are never under-counted.
    #[cfg(test)]
    pub(crate) fn settle(&mut self, reserved: u64, actual: u64) {
        if actual > reserved {
            self.charge(actual - reserved);
        } else {
            self.release(reserved - actual);
        }
    }

    /// Audit the shared counter against an externally-derived expected value.
    ///
    /// The expected value can be a cross-task snapshot, so transient handoff
    /// skew is recorded as a metric rather than emitted as a warning.
    ///
    /// Returns `true` when the budget matches.
    pub(crate) fn audit(&self, expected: u64, _context: &'static str) -> bool {
        let actual = self.reserved();
        let ok = actual == expected;
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
    fn byte_budget_settles_estimates_to_actuals() {
        let mut budget = ByteBudget::new(1_000);
        assert!(budget.try_reserve(300));
        budget.settle(300, 200);
        assert_eq!(budget.reserved(), 200);

        assert!(budget.try_reserve(300));
        budget.settle(300, 300);
        assert_eq!(budget.reserved(), 500);

        assert!(budget.try_reserve(300));
        budget.settle(300, 450);
        assert_eq!(budget.reserved(), 950);
        assert_eq!(budget.available(), 50);
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
