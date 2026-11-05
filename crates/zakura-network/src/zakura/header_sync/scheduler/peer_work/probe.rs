//! A sim-only read of the reactor's header budget and locator permits.
//!
//! It holds std's `Weak` rather than `zakura_rt`'s, whose `upgrade` is a
//! preemption point, since a boundary read may take none.

use std::sync::{Arc, Weak};

use zakura_rt::{
    sim::{in_boundary_read, Peek},
    sync::Semaphore,
};

use super::{HeaderChunkBudgetInner, HeaderChunkUsage, PeerWorkQueue};

/// A read of a reactor's header budget and locator permits for the
/// simulator's harness, which keeps it past the reactor. It holds both
/// weakly: every reservation, lease and permit holds its budget or semaphore
/// strongly, so once the reactor is gone a read still sees a stray one that
/// no owner reports, and the probe never delays either object's drop, whose
/// last release is a scheduling point.
#[derive(Clone, Debug)]
pub struct HeaderResourceProbe {
    budget: Weak<HeaderChunkBudgetInner>,
    capacity: usize,
    locators: Weak<Semaphore>,
    locator_capacity: usize,
}

/// One [`HeaderResourceProbe`] read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderResources {
    /// Headers reserved for pages not yet staged.
    pub reserved: usize,
    /// Headers staged and still leased.
    pub owned: usize,
    /// Locator permits free now.
    pub locator_permits: usize,
}

impl HeaderResourceProbe {
    /// A probe of `queue`'s budget and of `locators`, the reactor's locator
    /// semaphore, which starts with `locator_capacity` permits.
    pub(in crate::zakura::header_sync) fn new(
        queue: &PeerWorkQueue,
        locators: &Arc<Semaphore>,
        locator_capacity: usize,
    ) -> Self {
        HeaderResourceProbe {
            budget: Arc::downgrade(&queue.budget.0),
            capacity: queue.budget.0.capacity,
            locators: Arc::downgrade(locators),
            locator_capacity,
        }
    }

    /// The budget's counts and the free locator permits, read through peeks,
    /// so the read adds no scheduling point. A budget or semaphore that's
    /// gone has nothing out. `None` while a task holds the budget's lock, or
    /// after a holder panicked, since the counts may be mid-change.
    ///
    /// # Panics
    ///
    /// Outside a [`boundary_read`](zakura_rt::sim::boundary_read).
    pub fn read(&self) -> Option<HeaderResources> {
        assert!(
            in_boundary_read(),
            "a header resource probe reads only inside a boundary read"
        );
        // While a read runs no task does, so a handle upgraded here is never
        // the last one, and dropping it releases nothing.
        let usage = match self.budget.upgrade() {
            None => HeaderChunkUsage::default(),
            Some(budget) => match budget.usage.peek(|usage| *usage) {
                Peek::Current(usage) => usage,
                Peek::Held { .. } | Peek::Poisoned(_) => return None,
            },
        };
        Some(HeaderResources {
            reserved: usage.reserved,
            owned: usage.owned,
            locator_permits: self
                .locators
                .upgrade()
                .map_or(self.locator_capacity, |locators| locators.peek_available()),
        })
    }

    /// The most headers the budget reserves and owns together.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The locator permits the reactor started with.
    pub fn locator_capacity(&self) -> usize {
        self.locator_capacity
    }
}
