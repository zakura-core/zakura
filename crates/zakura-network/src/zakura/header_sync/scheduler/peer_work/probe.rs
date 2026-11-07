//! A sim-only read of the reactor's header budget and locator permits.
//!
//! It holds the budget through std's `Weak` rather than `zakura_rt`'s,
//! whose `upgrade` is a preemption point, since a boundary read may take
//! none.

use std::sync::{Arc, Weak};

use zakura_rt::{
    sim::{in_boundary_read, Peek, TaskId},
    sync::Semaphore,
};

use super::{HeaderChunkBudgetInner, HeaderChunkUsage, PeerWorkQueue};

/// A read of a reactor's header budget and locator permits for the
/// simulator's harness, which keeps it past the reactor.
///
/// It holds the budget weakly, so it never delays the budget's last drop,
/// whose release is a scheduling point. Every reservation and lease holds
/// the budget strongly, so a read once the reactor is gone still sees a
/// stray one. It holds the locator semaphore strongly, since dropping one
/// is no scheduling point, so a permit consumed with
/// [`forget`](zakura_rt::sync::OwnedSemaphorePermit::forget), which lets go
/// of its semaphore, still reads as missing.
#[derive(Clone, Debug)]
pub struct HeaderResourceProbe {
    targets: ProbeTargets,
    task: TaskId,
}

/// What a [`HeaderResourceProbe`] reads, before its reactor's task exists.
#[derive(Clone, Debug)]
pub(in crate::zakura::header_sync) struct ProbeTargets {
    budget: Weak<HeaderChunkBudgetInner>,
    capacity: usize,
    locators: Arc<Semaphore>,
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

impl ProbeTargets {
    /// `queue`'s budget and `locators`, the reactor's locator semaphore,
    /// which starts with `locator_capacity` permits.
    pub(in crate::zakura::header_sync) fn of(
        queue: &PeerWorkQueue,
        locators: &Arc<Semaphore>,
        locator_capacity: usize,
    ) -> Self {
        ProbeTargets {
            budget: Arc::downgrade(&queue.budget.0),
            capacity: queue.budget.0.capacity,
            locators: locators.clone(),
            locator_capacity,
        }
    }

    /// A probe of these, whose reactor runs as `task`.
    pub(in crate::zakura::header_sync) fn on(self, task: TaskId) -> HeaderResourceProbe {
        HeaderResourceProbe {
            targets: self,
            task,
        }
    }
}

impl HeaderResourceProbe {
    /// The budget's counts and the free locator permits, read through peeks,
    /// so the read adds no scheduling point. A budget that's gone has
    /// nothing out. `None` while a task holds the budget's lock, or after a
    /// holder panicked, since the counts may be mid-change.
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
        let usage = match self.targets.budget.upgrade() {
            None => HeaderChunkUsage::default(),
            Some(budget) => match budget.usage.peek(|usage| *usage) {
                Peek::Current(usage) => usage,
                Peek::Held { .. } | Peek::Poisoned(_) => return None,
            },
        };
        Some(HeaderResources {
            reserved: usage.reserved,
            owned: usage.owned,
            locator_permits: self.targets.locators.peek_available(),
        })
    }

    /// The reactor's task: the only task that changes the budget, which
    /// reports each change to the reactor's observer later in the same poll.
    pub fn task(&self) -> TaskId {
        self.task
    }

    /// The most headers the budget reserves and owns together.
    pub fn capacity(&self) -> usize {
        self.targets.capacity
    }

    /// The locator permits the reactor started with.
    pub fn locator_capacity(&self) -> usize {
        self.targets.locator_capacity
    }
}
