//! Bounded ownership for decoded inputs waiting outside a service's work ledger.

use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A pending-input capacity cannot be represented by Tokio's semaphore.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("pending-input capacity {requested} exceeds maximum {maximum}")]
pub(crate) struct PendingInputCapacityError {
    /// Requested number of pending inputs.
    pub(crate) requested: usize,
    /// Largest supported number of pending inputs.
    pub(crate) maximum: usize,
}

/// Shared bound on decoded inputs retained before work admission.
///
/// A successful reservation returns one linear permit. Moving that permit with
/// the retained input and dropping both together makes cancellation, queue
/// removal, and task failure release capacity without separate cleanup logic.
#[derive(Clone, Debug)]
pub(crate) struct PendingInputBudget {
    capacity: usize,
    permits: Arc<Semaphore>,
}

impl PendingInputBudget {
    /// Create an empty budget that can retain at most `capacity` inputs.
    pub(crate) fn new(capacity: usize) -> Result<Self, PendingInputCapacityError> {
        if capacity > Semaphore::MAX_PERMITS {
            return Err(PendingInputCapacityError {
                requested: capacity,
                maximum: Semaphore::MAX_PERMITS,
            });
        }

        Ok(Self {
            capacity,
            permits: Arc::new(Semaphore::new(capacity)),
        })
    }

    /// Return the maximum number of retained inputs.
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Return the number of permits currently owned by pending inputs.
    pub(crate) fn reserved(&self) -> usize {
        self.capacity
            .saturating_sub(self.permits.available_permits())
    }

    /// Reserve one pending-input slot without waiting.
    pub(crate) fn try_reserve(&self) -> Option<PendingInputPermit> {
        self.permits
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| PendingInputPermit { _permit: permit })
    }
}

/// Linear ownership of one pending-input slot.
#[derive(Debug)]
#[must_use = "dropping a pending-input permit releases its slot"]
pub(crate) struct PendingInputPermit {
    _permit: OwnedSemaphorePermit,
}
