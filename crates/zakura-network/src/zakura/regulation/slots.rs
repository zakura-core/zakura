//! Owned permits for bounded collections of retained or active work.

use std::sync::{Arc, Weak};

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A slot capacity cannot provide usable, representable permits.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("slot capacity {requested} must be in 1..={maximum}")]
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
    #[cfg(test)]
    capacity: usize,
    permits: Arc<Semaphore>,
}

impl SlotBudget {
    /// Create a budget with `capacity` independently owned slots.
    pub(crate) fn new(capacity: usize) -> Result<Self, SlotBudgetCapacityError> {
        if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
            return Err(SlotBudgetCapacityError {
                requested: capacity,
                maximum: Semaphore::MAX_PERMITS,
            });
        }

        Ok(Self {
            #[cfg(test)]
            capacity,
            permits: Arc::new(Semaphore::new(capacity)),
        })
    }

    /// Return the maximum number of owned slots.
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Return the number of currently owned slots.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        self.capacity
            .saturating_sub(self.permits.available_permits())
    }

    /// Create a non-owning handle to this same pool for registry bookkeeping.
    /// This does not change capacity or release any reserved slots.
    pub(super) fn downgrade(&self) -> WeakSlotBudget {
        WeakSlotBudget {
            #[cfg(test)]
            capacity: self.capacity,
            permits: Arc::downgrade(&self.permits),
        }
    }

    /// Reserve one slot without waiting.
    #[cfg(test)]
    pub(crate) fn try_reserve(&self) -> Option<SlotPermit> {
        self.permits
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| SlotPermit { _permit: permit })
    }

    /// Wait for a slot and return its ownership in semaphore queue order.
    ///
    /// Keep the returned permit while owning the resource. Cancelling this
    /// future removes its waiter without consuming a slot.
    pub(crate) async fn reserve(&self) -> SlotPermit {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("slot budget semaphore stays open because this type never closes it");
        SlotPermit { _permit: permit }
    }
}

/// A byte budget for response output that is queued but not yet written.
///
/// A grant is taken before a response is produced and released when its frame
/// finishes its transport write. A peer that stops reading therefore stops at
/// this bound instead of growing the queue.
#[derive(Clone, Debug)]
pub(crate) struct OutputByteBudget {
    capacity: u32,
    bytes: Arc<Semaphore>,
}

impl OutputByteBudget {
    /// A budget of `capacity` bytes.
    pub(crate) fn new(capacity: u32) -> Self {
        Self {
            capacity,
            // The cast widens u32 to usize on supported targets. Output budgets
            // are a few MiB, far below `Semaphore::MAX_PERMITS`.
            bytes: Arc::new(Semaphore::new(capacity as usize)),
        }
    }

    /// Total bytes this budget can grant at once.
    pub(crate) fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Bytes currently granted.
    #[cfg(test)]
    pub(crate) fn granted(&self) -> usize {
        // A u32 capacity fits usize on supported targets.
        (self.capacity as usize).saturating_sub(self.bytes.available_permits())
    }

    /// Wait for `bytes` in FIFO order. Cancelling the wait takes nothing.
    ///
    /// The caller keeps `bytes <= capacity()`; a larger request never completes.
    pub(crate) async fn grant(&self, bytes: u32) -> OutputGrant {
        let permit = self
            .bytes
            .clone()
            .acquire_many_owned(bytes)
            .await
            .expect("output budget semaphore stays open because this type never closes it");
        OutputGrant { _permit: permit }
    }

    pub(super) fn downgrade(&self) -> WeakOutputByteBudget {
        WeakOutputByteBudget {
            capacity: self.capacity,
            bytes: Arc::downgrade(&self.bytes),
        }
    }
}

/// Granted output bytes. Dropping the grant returns them.
#[derive(Debug)]
#[must_use = "dropping an output grant releases its bytes"]
pub(crate) struct OutputGrant {
    _permit: OwnedSemaphorePermit,
}

/// A registry handle to an output budget that does not keep it alive.
#[derive(Debug)]
pub(super) struct WeakOutputByteBudget {
    capacity: u32,
    bytes: Weak<Semaphore>,
}

impl WeakOutputByteBudget {
    pub(super) fn upgrade(&self) -> Option<OutputByteBudget> {
        Some(OutputByteBudget {
            capacity: self.capacity,
            bytes: self.bytes.upgrade()?,
        })
    }
}

/// One reserved slot. The permit keeps its pool alive even if the session ends.
/// Dropping the permit returns the slot to that same pool.
#[derive(Debug)]
#[must_use = "dropping a slot permit releases its capacity"]
pub(crate) struct SlotPermit {
    _permit: OwnedSemaphorePermit,
}

/// A registry handle that can find a slot pool without keeping it alive.
///
/// Sessions and outstanding permits keep the pool alive. If a peer reconnects
/// while its old work remains, upgrading this handle reuses the same pool,
/// so reconnecting cannot bypass the peer's serving limit.
///
/// Once all strong owners are gone, upgrading returns `None`. The registry
/// can then remove the stale entry.
#[derive(Debug)]
pub(super) struct WeakSlotBudget {
    #[cfg(test)]
    capacity: usize,
    permits: Weak<Semaphore>,
}

impl WeakSlotBudget {
    /// Check whether the pool currently has any strong owners, for registry cleanup.
    /// This is a snapshot; use `upgrade` to obtain a handle that keeps it alive.
    pub(super) fn is_alive(&self) -> bool {
        self.permits.strong_count() > 0
    }

    /// Obtain shared ownership of the existing pool, or `None` if it is gone.
    /// This never creates a new pool or resets its available capacity.
    pub(super) fn upgrade(&self) -> Option<SlotBudget> {
        Some(SlotBudget {
            #[cfg(test)]
            capacity: self.capacity,
            permits: self.permits.upgrade()?,
        })
    }
}
