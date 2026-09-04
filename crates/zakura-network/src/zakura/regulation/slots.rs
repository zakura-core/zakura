//! Owned permits for bounded collections of retained or active work.

use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A slot capacity cannot be represented by Tokio's semaphore.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("slot capacity {requested} exceeds maximum {maximum}")]
pub(crate) struct SlotBudgetCapacityError {
    /// Requested slot count.
    pub(crate) requested: usize,
    /// Largest supported slot count.
    pub(crate) maximum: usize,
}

/// Shared ownership bound for retained or active work items.
///
/// Each successful admission returns one linear permit. Moving it with the
/// admitted item makes ordinary drop and cancellation release capacity.
#[derive(Clone, Debug)]
pub(crate) struct SlotBudget {
    capacity: usize,
    permits: Arc<Semaphore>,
}

impl SlotBudget {
    /// Create a budget with `capacity` independently owned slots.
    pub(crate) fn new(capacity: usize) -> Result<Self, SlotBudgetCapacityError> {
        if capacity > Semaphore::MAX_PERMITS {
            return Err(SlotBudgetCapacityError {
                requested: capacity,
                maximum: Semaphore::MAX_PERMITS,
            });
        }

        Ok(Self {
            capacity,
            permits: Arc::new(Semaphore::new(capacity)),
        })
    }

    /// Return the maximum number of owned slots.
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Return the number of currently owned slots.
    pub(crate) fn reserved(&self) -> usize {
        self.capacity
            .saturating_sub(self.permits.available_permits())
    }

    /// Reserve one slot without waiting.
    pub(crate) fn try_reserve(&self) -> Option<SlotPermit> {
        self.permits
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| SlotPermit { _permit: permit })
    }

    /// Wait until one slot could be reserved, without retaining it.
    ///
    /// Acquiring and immediately releasing the semaphore permit makes the wait
    /// race-free. A caller retries its complete multi-resource admission after
    /// this method returns.
    pub(crate) async fn wait_for(&self) {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("slot budget semaphore stays open because this type never closes it");
        drop(permit);
    }
}

/// Linear ownership of one slot.
#[derive(Debug)]
#[must_use = "dropping a slot permit releases its capacity"]
pub(crate) struct SlotPermit {
    _permit: OwnedSemaphorePermit,
}
