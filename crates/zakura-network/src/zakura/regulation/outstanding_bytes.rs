//! Linear accounting for response bytes until their transport owner drops them.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use thiserror::Error;
use tokio::sync::{futures::Notified, Notify};

/// A reservation exceeded an outstanding-byte budget's total capacity.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("requested {requested} outstanding bytes exceeds capacity {capacity}")]
pub(crate) struct OutstandingCapacityError {
    /// Requested byte count.
    pub(crate) requested: u64,
    /// Configured byte capacity.
    pub(crate) capacity: u64,
}

/// Shared capacity returned only when outstanding byte ownership ends.
///
/// Unlike [`RateBudget`](super::RateBudget), this balance does not refill with
/// time. Clones reserve and release against the same atomic counter, allowing a
/// node budget and a peer budget to be held by different tasks.
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
    /// Create an empty budget with `capacity` bytes.
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            inner: Arc::new(OutstandingByteBudgetInner {
                capacity,
                reserved: AtomicU64::new(0),
                capacity_released: Notify::new(),
            }),
        }
    }

    /// Return the configured byte capacity.
    pub(crate) fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    /// Return the bytes currently available for reservation.
    pub(crate) fn available(&self) -> u64 {
        self.capacity().saturating_sub(self.reserved())
    }

    /// Return the bytes currently owned by reservations and frame leases.
    pub(crate) fn reserved(&self) -> u64 {
        self.inner.reserved.load(Ordering::Acquire)
    }

    /// Reserve bytes and return their linear owner.
    ///
    /// `Err` means the request can never fit. `Ok(None)` means it can fit after
    /// another owner releases capacity.
    pub(crate) fn try_reserve(
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
    /// concurrent release cannot be missed.
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
#[must_use = "dropping an outstanding-byte reservation releases it"]
pub(crate) struct OutstandingByteReservation {
    budget: OutstandingByteBudget,
    remaining: u64,
}

impl OutstandingByteReservation {
    /// Return the bytes this reservation still owns directly.
    pub(crate) fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Transfer equal bytes from every reservation into one frame lease.
    ///
    /// This operation validates every reservation before changing any of them.
    /// It is intended for the peer and node response-byte reservations attached
    /// to the same outbound frame.
    pub(crate) fn transfer_to_frame<const N: usize>(
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

    /// Release all remaining bytes now.
    pub(crate) fn release(self) {
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
#[must_use = "the transport must retain a frame lease until write completion or drop"]
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
    /// Return the response bytes represented by this lease.
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
