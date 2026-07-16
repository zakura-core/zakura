use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::sync::Notify;

/// Authoritative total for retained block bodies and outstanding headroom reservations.
#[derive(Clone, Debug)]
pub(super) struct RetainedBodyMemoryTracker {
    inner: Arc<RetainedMemoryInner>,
}

#[derive(Debug)]
struct RetainedMemoryInner {
    used: AtomicU64,
    limit: u64,
    capacity: Notify,
}

impl RetainedBodyMemoryTracker {
    pub(super) fn new(limit: u64) -> Self {
        Self {
            inner: Arc::new(RetainedMemoryInner {
                used: AtomicU64::new(0),
                limit,
                capacity: Notify::new(),
            }),
        }
    }

    pub(super) fn used(&self) -> u64 {
        self.inner.used.load(Ordering::Relaxed)
    }

    pub(super) fn limit(&self) -> u64 {
        self.inner.limit
    }

    pub(super) fn overshoot(&self) -> u64 {
        self.used().saturating_sub(self.limit())
    }

    /// Charge bytes without consulting the limit.
    ///
    /// Matched responses and commit-window bodies use this path because their
    /// bounded work was admitted before the exact retained size was known.
    pub(super) fn charge(&self, bytes: u64) -> RetainedCharge {
        add_bytes(&self.inner.used, bytes);
        RetainedCharge::new(self.inner.clone(), bytes)
    }

    /// Charge bytes only when the resulting total fits within the limit.
    pub(super) fn try_charge(&self, bytes: u64) -> Option<RetainedCharge> {
        self.inner
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.inner.limit)
            })
            .ok()?;

        Some(RetainedCharge::new(self.inner.clone(), bytes))
    }

    /// Atomically reserve one request's per-height estimates, all or none.
    pub(super) fn try_reserve_many(&self, bytes: &[u64]) -> Option<Vec<InFlightMemoryReservation>> {
        if bytes.is_empty() {
            return Some(Vec::new());
        }

        let total = bytes
            .iter()
            .try_fold(0u64, |total, bytes| total.checked_add(*bytes))?;
        self.inner
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(total)
                    .filter(|next| *next <= self.inner.limit)
            })
            .ok()?;

        Some(
            bytes
                .iter()
                .map(|bytes| {
                    InFlightMemoryReservation(RetainedCharge::new(self.inner.clone(), *bytes))
                })
                .collect(),
        )
    }

    pub(super) fn subscribe_capacity(&self) -> &Notify {
        &self.inner.capacity
    }
}

/// Retained-memory headroom owned by one outstanding block-height request.
///
/// Dropping an unconsumed reservation releases it. On receipt,
/// [`reconcile_exact`](Self::reconcile_exact) transfers the same accounting into
/// the buffered body's exact RAII charge.
#[derive(Debug)]
pub(super) struct InFlightMemoryReservation(RetainedCharge);

impl InFlightMemoryReservation {
    // Resize the reservation to the exact bytes after receiving the body.
    pub(super) fn reconcile_exact(self, exact_bytes: u64) -> RetainedCharge {
        self.0.resize(exact_bytes);
        self.0
    }
}

/// A shareable Resource Acquisition Is Initialization (RAII) charge for one retained body representation.
///
/// Clones share one charge. The bytes are released when the last clone drops.
#[derive(Clone, Debug)]
pub(super) struct RetainedCharge {
    inner: Arc<RetainedChargeInner>,
}

#[derive(Debug)]
struct RetainedChargeInner {
    memory: Arc<RetainedMemoryInner>,
    bytes: AtomicU64,
}

impl RetainedCharge {
    fn new(memory: Arc<RetainedMemoryInner>, bytes: u64) -> Self {
        Self {
            inner: Arc::new(RetainedChargeInner {
                memory,
                bytes: AtomicU64::new(bytes),
            }),
        }
    }

    /// Replace this representation's charge with its current exact size.
    pub(super) fn resize(&self, bytes: u64) {
        let mut previous = self.inner.bytes.load(Ordering::Relaxed);
        loop {
            if bytes == previous {
                return;
            }
            if bytes > previous {
                let growth = bytes - previous;
                // Account growth globally before publishing it locally, so
                // concurrent admission can only observe a conservative total.
                add_bytes(&self.inner.memory.used, growth);
                match self.inner.bytes.compare_exchange_weak(
                    previous,
                    bytes,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(observed) => {
                        release_memory(&self.inner.memory, growth);
                        previous = observed;
                    }
                }
            } else {
                match self.inner.bytes.compare_exchange_weak(
                    previous,
                    bytes,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        release_memory(&self.inner.memory, previous - bytes);
                        return;
                    }
                    Err(observed) => previous = observed,
                }
            }
        }
    }
}

impl Drop for RetainedChargeInner {
    fn drop(&mut self) {
        release_memory(&self.memory, self.bytes.load(Ordering::Relaxed));
    }
}

fn add_bytes(counter: &AtomicU64, bytes: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(bytes))
    });
}

fn release_bytes(counter: &AtomicU64, bytes: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_sub(bytes);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn release_memory(memory: &RetainedMemoryInner, bytes: u64) {
    release_bytes(&memory.used, bytes);
    if bytes > 0 {
        memory.capacity.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_reserve(
        memory: &RetainedBodyMemoryTracker,
        bytes: u64,
    ) -> Option<InFlightMemoryReservation> {
        memory.try_reserve_many(&[bytes])?.pop()
    }

    #[test]
    fn charge_clones_release_once_and_resize_exactly() {
        let memory = RetainedBodyMemoryTracker::new(100);
        let charge = memory.charge(40);
        let clone = charge.clone();
        assert_eq!(memory.used(), 40);

        charge.resize(70);
        assert_eq!(memory.used(), 70);
        drop(charge);
        assert_eq!(memory.used(), 70);
        drop(clone);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn try_charge_is_atomic_at_the_limit() {
        let memory = RetainedBodyMemoryTracker::new(100);
        let first = memory.try_charge(80).expect("80 bytes fit");
        assert!(memory.try_charge(21).is_none());
        let second = memory.try_charge(20).expect("remaining 20 bytes fit");
        assert_eq!(memory.used(), 100);
        drop((first, second));
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn reservation_reconciles_without_double_charging() {
        let memory = RetainedBodyMemoryTracker::new(100);
        let reservation = try_reserve(&memory, 60).expect("60 bytes fit");
        assert_eq!(memory.used(), 60);

        let charge = reservation.reconcile_exact(80);
        assert_eq!(memory.used(), 80);
        drop(charge);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn outstanding_reservations_cannot_reuse_headroom() {
        let memory = RetainedBodyMemoryTracker::new(100);
        assert!(memory.try_reserve_many(&[60, 41]).is_none());
        assert_eq!(memory.used(), 0, "failed request reserves nothing");
        let reservations = memory
            .try_reserve_many(&[60, 40])
            .expect("whole request fits");
        assert_eq!(memory.used(), 100);
        drop(reservations);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn empty_reservation_succeeds_while_over_limit() {
        let memory = RetainedBodyMemoryTracker::new(1);
        let charge = memory.charge(2);
        assert_eq!(memory.overshoot(), 1);

        let reservations = memory
            .try_reserve_many(&[])
            .expect("an empty reservation requires no headroom");
        assert!(reservations.is_empty());
        assert_eq!(memory.used(), 2);

        drop(charge);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn releasing_memory_wakes_capacity_waiters() {
        let memory = RetainedBodyMemoryTracker::new(100);
        let reservation = try_reserve(&memory, 100).expect("reservation fits");
        let notified = memory.subscribe_capacity().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        drop(reservation);
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut notified)
            .await
            .expect("release wakes retained-capacity waiter");
    }
}
