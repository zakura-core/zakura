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

    /// Bytes this reservation currently holds, for the accounting probe.
    #[cfg(test)]
    pub(super) fn charged_bytes(&self) -> u64 {
        self.0.inner.bytes.load(Ordering::Relaxed)
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

    /// What a contender thread observed while racing a transition at the limit.
    struct ContenderResult {
        /// Bytes the contender managed to charge — headroom the tracker really published.
        stolen: u64,
        /// `try_charge` calls made, so a passing "stole nothing" assertion can be shown
        /// to have actually raced the transition rather than finished before it started.
        attempts: u64,
    }

    /// Spin `try_charge(1)` on a background thread until told to stop.
    ///
    /// A contender at a saturated limit is a *witness*, not a sampler: every byte it
    /// acquires is headroom the tracker actually published, so a bound on its take is a
    /// bound on the headroom the transition exposed. Sampling `used()` in a loop could
    /// miss a transient dip between reads; a contender that wins cannot miss one.
    ///
    /// Returns once `stop` is set. The caller must wait for `started` before running the
    /// transition under test, otherwise the race never happens and the assertion is vacuous.
    fn spawn_contender(
        memory: &RetainedBodyMemoryTracker,
        started: &Arc<std::sync::atomic::AtomicBool>,
        stop: &Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<ContenderResult> {
        let memory = memory.clone();
        let started = started.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut stolen = Vec::new();
            let mut attempts = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if let Some(charge) = memory.try_charge(1) {
                    stolen.push(charge);
                }
                attempts += 1;
                started.store(true, Ordering::Release);
                std::hint::spin_loop();
            }
            ContenderResult {
                // Each stolen charge is one byte, so the count is the byte total.
                stolen: stolen.len() as u64,
                attempts,
            }
        })
    }

    fn wait_for_contender(started: &Arc<std::sync::atomic::AtomicBool>) {
        while !started.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
    }

    #[test]
    fn growth_at_the_limit_never_exposes_headroom_to_a_contender() {
        const LIMIT: u64 = 100;

        // Saturate the limit, so *any* headroom the tracker publishes is winnable and a
        // contender that stays empty-handed proves none was ever published.
        let memory = RetainedBodyMemoryTracker::new(LIMIT);
        let _fixed = memory.charge(40);
        let reservation = try_reserve(&memory, 60).expect("60 bytes fit exactly");
        assert_eq!(memory.used(), LIMIT);

        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let contender = spawn_contender(&memory, &started, &stop);
        wait_for_contender(&started);

        // Reconcile, then keep growing. Growth is monotone, so the total never returns
        // below the limit and a correct tracker leaves the contender nothing at any
        // instant. The step count gives the contender thousands of chances to interleave
        // with the grow path's add-then-CAS (and with its refund on a lost CAS), which a
        // single transition would not.
        let charge = reservation.reconcile_exact(61);
        for bytes in 62..=4096 {
            charge.resize(bytes);
        }

        stop.store(true, Ordering::Relaxed);
        let observed = contender.join().expect("contender thread does not panic");

        assert!(
            observed.attempts > 0,
            "the contender never ran, so it never raced the growth",
        );
        assert_eq!(
            observed.stolen, 0,
            "growth published headroom to a contender across {} attempts; a \
             release-then-recharge would briefly free the whole reservation",
            observed.attempts,
        );
        assert_eq!(memory.used(), 4136, "40 fixed + 4096 grown");
        drop(charge);
        assert_eq!(memory.used(), 40);
    }

    #[test]
    fn racing_resizes_to_one_size_apply_growth_exactly_once() {
        // The growth path charges globally *before* publishing locally and refunds on a
        // lost CAS. If a refund were ever skipped, or a retry double-charged, the total
        // would settle above the agreed size. Racing threads onto one value keeps the
        // expected result exact while maximising CAS contention.
        let memory = RetainedBodyMemoryTracker::new(u64::MAX);
        let charge = memory.charge(1);

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let charge = charge.clone();
                std::thread::spawn(move || {
                    for _ in 0..2_000 {
                        charge.resize(4096);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("resize thread does not panic");
        }

        assert_eq!(
            memory.used(),
            4096,
            "concurrent resizes to one size must not accumulate",
        );
        drop(charge);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn clones_share_one_charge_and_the_last_drop_releases_it() {
        let memory = RetainedBodyMemoryTracker::new(1000);
        let charge = memory.charge(64);
        let clones: Vec<_> = (0..16).map(|_| charge.clone()).collect();

        // Cloning an owner never charges again, however many owners exist.
        assert_eq!(memory.used(), 64, "clones must not duplicate the charge");
        // Resizing through any clone resizes the one shared charge.
        clones[3].resize(128);
        assert_eq!(memory.used(), 128);

        drop(clones);
        assert_eq!(
            memory.used(),
            128,
            "the original owner still holds the body"
        );
        drop(charge);
        assert_eq!(memory.used(), 0, "the final owner's drop releases it once");
    }

    #[test]
    fn concurrent_clone_and_drop_releases_each_charge_exactly_once() {
        let memory = RetainedBodyMemoryTracker::new(u64::MAX);
        let bodies: Vec<_> = (0..64).map(|index| memory.charge(index + 1)).collect();
        let expected: u64 = (1..=64).sum();
        assert_eq!(memory.used(), expected);

        // Hand every body to several threads that clone and drop it concurrently. Only
        // the original owners below keep it alive, so the total must not move.
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let bodies = bodies.clone();
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let clones = bodies.to_vec();
                        drop(clones);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("clone thread does not panic");
        }

        assert_eq!(
            memory.used(),
            expected,
            "transient clones must neither charge nor release",
        );
        drop(bodies);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn concurrent_reservations_never_oversubscribe_the_limit() {
        const LIMIT: u64 = 10_000;
        const RESERVATION: u64 = 300;

        let memory = RetainedBodyMemoryTracker::new(LIMIT);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let memory = memory.clone();
                std::thread::spawn(move || {
                    let mut held = Vec::new();
                    for _ in 0..200 {
                        // Two-height requests exercise the all-or-none path.
                        if let Some(reservations) =
                            memory.try_reserve_many(&[RESERVATION, RESERVATION])
                        {
                            assert!(
                                memory.used() <= LIMIT,
                                "a granted reservation pushed the total past the limit",
                            );
                            held.push(reservations);
                        }
                    }
                    held
                })
            })
            .collect();

        let held: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().expect("reserving thread does not panic"))
            .collect();

        let granted: u64 = held.iter().flatten().flatten().count() as u64;
        assert!(
            granted > 0,
            "no reservation was granted; the test is vacuous"
        );
        assert_eq!(
            memory.used(),
            granted * RESERVATION,
            "the total must equal exactly the granted reservations",
        );
        assert!(memory.used() <= LIMIT);

        drop(held);
        assert_eq!(memory.used(), 0, "every reservation released exactly once");
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
