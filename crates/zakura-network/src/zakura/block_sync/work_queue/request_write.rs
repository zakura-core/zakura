//! Ownership from provisional download reservation through the request write.

use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

use tokio_util::sync::CancellationToken;
use zakura_header_chain::BodyWorkOwner;

use super::{block, BlockBudgetLedger, WorkItem, WorkQueue};
use crate::zakura::transport::{ByteBudget, FrameWriteClaim};

const UNPUBLISHED: u8 = 0;
const QUEUED: u8 = 1;
const STARTED: u8 = 2;
const WRITTEN: u8 = 3;
const EXPIRED: u8 = 4;

#[cfg(test)]
mod tests;

/// One exact attempt owns its provisional budget until publication. Afterwards
/// the work ledger arbitrates every release, including reset and response arrival.
#[derive(Debug)]
pub(crate) struct RequestWrite {
    owner: BodyWorkOwner,
    items: Vec<(block::Height, WorkItem)>,
    estimated_bytes: u64,
    work: Arc<WorkQueue>,
    budget: ByteBudget,
    cancel: CancellationToken,
    state: AtomicU8,
}

impl RequestWrite {
    pub(in crate::zakura::block_sync) fn new(
        owner: BodyWorkOwner,
        items: Vec<(block::Height, WorkItem)>,
        work: Arc<WorkQueue>,
        budget: ByteBudget,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let estimated_bytes = items.iter().map(|(_, item)| item.estimated_bytes).sum();
        Arc::new(Self {
            owner,
            items,
            estimated_bytes,
            work,
            budget,
            cancel,
            state: AtomicU8::new(UNPUBLISHED),
        })
    }

    /// Record outstanding state and enqueue into already-reserved capacity under
    /// the same lock used by reset and writer claim. The caller retains this Arc
    /// until return, even if a closed queue immediately discards its copy.
    pub(in crate::zakura::block_sync) fn publish(self: &Arc<Self>, publish: impl FnOnce()) -> bool {
        let mut inner = self.work.lock();
        if self.cancel.is_cancelled()
            || self.items.is_empty()
            || !self.items.iter().all(|(height, taken)| {
                inner.in_flight.get(height).is_some_and(|item| {
                    item.owner == Some(self.owner) && item.hash == taken.hash && item.provisional
                })
            })
        {
            return false;
        }
        assert_eq!(
            self.state.load(Ordering::Acquire),
            UNPUBLISHED,
            "a request is published once"
        );
        for (height, _) in &self.items {
            let item = inner
                .in_flight
                .get_mut(height)
                .expect("every provisional item was checked under this lock");
            item.budget = BlockBudgetLedger::reserved(item.estimated_bytes);
            item.provisional = false;
        }
        inner.reserved_bytes = inner.reserved_bytes.saturating_add(self.estimated_bytes);
        inner
            .request_writes
            .retain(|_, claim| claim.strong_count() > 0);
        inner
            .request_writes
            .insert(self.owner, Arc::downgrade(self));
        self.state.store(QUEUED, Ordering::Release);
        publish();
        true
    }

    pub(super) fn owner(&self) -> BodyWorkOwner {
        self.owner
    }

    /// A reserved queue slot lost its receiver during publication. Settle the
    /// ledger now even if that closed channel retains a copy of this claim.
    pub(in crate::zakura::block_sync) fn delivery_failed(&self) {
        self.cancel.cancel();
        self.expire_unwritten();
        let released = self
            .work
            .release_reserved_and_return_items_detailed_for_owner(
                self.owner,
                self.items.iter().map(|(height, _)| *height),
            );
        self.budget.clone().release(released.released_bytes);
    }

    pub(super) fn has_height_above(&self, floor: block::Height) -> bool {
        self.items.last().is_some_and(|(height, _)| *height > floor)
    }

    pub(super) fn expire_unwritten(&self) {
        let _ = self
            .state
            .compare_exchange(QUEUED, EXPIRED, Ordering::AcqRel, Ordering::Acquire);
    }

    pub(super) fn reset(&self) {
        self.expire_unwritten();
        if self.state.load(Ordering::Acquire) == STARTED {
            // The prefix already belongs to this ordered stream. Cancelling the
            // session resets the pair; no later request may follow that prefix.
            self.cancel.cancel();
        }
    }
}

impl FrameWriteClaim for RequestWrite {
    fn try_start(&self) -> bool {
        let inner = self.work.lock();
        let current = !self.cancel.is_cancelled()
            && self.items.iter().all(|(height, _)| {
                inner
                    .in_flight
                    .get(height)
                    .is_some_and(|item| item.owner == Some(self.owner) && item.budget.is_reserved())
            });
        if !current {
            self.expire_unwritten();
            return false;
        }
        self.state
            .compare_exchange(QUEUED, STARTED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn written(&self) {
        self.state.store(WRITTEN, Ordering::Release);
        self.work.lock().request_writes.remove(&self.owner);
    }
}

impl Drop for RequestWrite {
    fn drop(&mut self) {
        match self.state.load(Ordering::Acquire) {
            UNPUBLISHED => {
                self.work.return_unpublished(&self.items);
                self.budget.release(self.estimated_bytes);
            }
            WRITTEN => {}
            state => {
                if state == STARTED {
                    self.cancel.cancel();
                }
                let released = self
                    .work
                    .release_reserved_and_return_items_detailed_for_owner(
                        self.owner,
                        self.items.iter().map(|(height, _)| *height),
                    );
                self.budget.release(released.released_bytes);
            }
        }
    }
}
