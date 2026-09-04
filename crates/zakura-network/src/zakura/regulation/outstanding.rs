//! Linear accounting for bytes that remain outstanding until an owner drops them.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use thiserror::Error;
use tokio::sync::{futures::Notified, Notify};

/// A request exceeded the total capacity of an outstanding byte budget.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("requested {requested} outstanding bytes exceeds budget capacity {capacity}")]
pub(crate) struct OutstandingCapacityError {
    /// Requested byte count.
    pub(crate) requested: u64,
    /// Configured budget capacity.
    pub(crate) capacity: u64,
}

/// Shared capacity that is returned only when outstanding work releases it.
///
/// Clones reserve and release against the same atomic counter. The legacy
/// `try_reserve` and `release` methods remain available while existing download
/// accounting migrates to [`OutstandingByteReservation`].
#[derive(Clone, Debug)]
pub(crate) struct OutstandingByteBudget {
    inner: Arc<OutstandingByteBudgetInner>,
}

#[derive(Debug)]
struct OutstandingByteBudgetInner {
    capacity: u64,
    reserved: AtomicU64,
    capacity_released: Notify,
}

impl OutstandingByteBudget {
    /// Create an empty outstanding byte budget with `capacity` bytes.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            inner: Arc::new(OutstandingByteBudgetInner {
                capacity,
                reserved: AtomicU64::new(0),
                capacity_released: Notify::new(),
            }),
        }
    }

    /// Return the configured capacity.
    pub(crate) fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    #[cfg(test)]
    pub(crate) fn max_bytes_for_test(&self) -> u64 {
        self.capacity()
    }

    /// Return the bytes currently available for reservation.
    pub(crate) fn available(&self) -> u64 {
        self.capacity().saturating_sub(self.reserved())
    }

    /// Return the bytes currently reserved.
    pub(crate) fn reserved(&self) -> u64 {
        self.inner.reserved.load(Ordering::Acquire)
    }

    /// Legacy reservation API for accounting whose lifetime is tracked elsewhere.
    pub(crate) fn try_reserve(&mut self, bytes: u64) -> bool {
        bytes > 0 && self.reserve_bytes(bytes)
    }

    /// Reserve bytes and return a linear owner that releases them on drop.
    pub(crate) fn try_reserve_owned(
        &self,
        bytes: u64,
    ) -> Result<Option<OutstandingByteReservation>, OutstandingCapacityError> {
        self.ensure_request_fits(bytes)?;

        if !self.reserve_bytes(bytes) {
            return Ok(None);
        }

        Ok(Some(OutstandingByteReservation {
            budget: self.clone(),
            remaining: bytes,
        }))
    }

    /// Wait until `bytes` could fit, without reserving them.
    ///
    /// The notification is registered before capacity is rechecked, so a
    /// concurrent release cannot be missed between the check and the wait.
    pub(crate) async fn wait_for(&self, bytes: u64) -> Result<(), OutstandingCapacityError> {
        self.ensure_request_fits(bytes)?;

        loop {
            let released = self.inner.capacity_released.notified();
            tokio::pin!(released);
            Notified::enable(released.as_mut());

            if self.available() >= bytes {
                return Ok(());
            }

            released.await;
        }
    }

    /// Legacy release API for accounting whose lifetime is tracked elsewhere.
    pub(crate) fn release(&mut self, bytes: u64) {
        self.release_bytes(bytes);
    }

    /// Add bytes without applying the capacity gate.
    ///
    /// Existing block-download floor progress uses this controlled overdraft.
    pub(crate) fn charge(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        let mut reserved = self.reserved();
        loop {
            let next = reserved.saturating_add(bytes);
            match self.inner.reserved.compare_exchange_weak(
                reserved,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => reserved = observed,
            }
        }
    }

    /// Audit the shared counter against an externally derived expected value.
    ///
    /// Handoff ordering can transiently leave the budget above the source
    /// ledger; only a shortfall proves a double release or lost charge.
    pub(crate) fn audit(&self, expected: u64, _context: &'static str) -> bool {
        let actual = self.reserved();
        let ok = actual >= expected;
        if !ok {
            metrics::counter!("sync.block.budget.audit_drift").increment(1);
        }
        ok
    }

    /// Subscribe to legacy capacity-release notifications.
    ///
    /// New regulation callers use [`Self::wait_for`] so they cannot miss a
    /// release. Existing download callers retain this API until they migrate.
    pub(crate) fn subscribe_capacity(&self) -> &Notify {
        &self.inner.capacity_released
    }

    fn ensure_request_fits(&self, bytes: u64) -> Result<(), OutstandingCapacityError> {
        if bytes > self.capacity() {
            return Err(OutstandingCapacityError {
                requested: bytes,
                capacity: self.capacity(),
            });
        }

        Ok(())
    }

    fn reserve_bytes(&self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }

        let mut reserved = self.reserved();
        loop {
            if bytes > self.capacity().saturating_sub(reserved) {
                return false;
            }

            let next = reserved.saturating_add(bytes);
            match self.inner.reserved.compare_exchange_weak(
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

    fn release_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        let mut reserved = self.reserved();
        loop {
            let next = reserved.saturating_sub(bytes);
            match self.inner.reserved.compare_exchange_weak(
                reserved,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => reserved = observed,
            }
        }
        self.inner.capacity_released.notify_waiters();
    }
}

/// Linear ownership of bytes reserved but not yet handed to a frame.
#[derive(Debug)]
#[must_use = "dropping a reservation releases its remaining bytes"]
pub(crate) struct OutstandingByteReservation {
    budget: OutstandingByteBudget,
    remaining: u64,
}

impl OutstandingByteReservation {
    /// Return the untransferred bytes still owned by this reservation.
    pub(crate) fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Transfer equal logical bytes from every reservation into one frame lease.
    ///
    /// All reservations are validated before any is changed, so failure leaves
    /// every owner untouched. A frame that is charged to both a peer and a node
    /// passes both reservations here and receives one lease.
    pub(crate) fn transfer_all<const N: usize>(
        reservations: [&mut OutstandingByteReservation; N],
        bytes: u64,
    ) -> Option<FrameLease> {
        if reservations
            .iter()
            .any(|reservation| reservation.remaining < bytes)
        {
            return None;
        }

        let mut releases = Vec::with_capacity(N);
        for reservation in reservations {
            reservation.remaining -= bytes;
            releases.push(FrameLeaseRelease {
                budget: reservation.budget.clone(),
                bytes,
            });
        }

        Some(FrameLease {
            accounted_bytes: bytes,
            releases,
        })
    }

    /// Release all untransferred bytes now rather than on scope exit.
    pub(crate) fn release_remaining(self) {
        drop(self);
    }
}

impl Drop for OutstandingByteReservation {
    fn drop(&mut self) {
        self.budget.release_bytes(self.remaining);
        self.remaining = 0;
    }
}

/// Linear ownership of one queued frame's outstanding-byte charges.
#[derive(Debug)]
#[must_use = "the transport must hold a frame lease until write completion or drop"]
pub(crate) struct FrameLease {
    accounted_bytes: u64,
    releases: Vec<FrameLeaseRelease>,
}

#[derive(Debug)]
struct FrameLeaseRelease {
    budget: OutstandingByteBudget,
    bytes: u64,
}

impl FrameLease {
    /// Return the logical response bytes represented by this frame.
    pub(crate) fn accounted_bytes(&self) -> u64 {
        self.accounted_bytes
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Self {
        Self {
            accounted_bytes: 0,
            releases: Vec::new(),
        }
    }
}

impl Drop for FrameLease {
    fn drop(&mut self) {
        for release in self.releases.drain(..) {
            release.budget.release_bytes(release.bytes);
        }
    }
}
