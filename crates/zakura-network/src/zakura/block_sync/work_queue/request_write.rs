//! Ownership from provisional download reservation through the request write.

use std::sync::Arc;

use zakura_header_chain::BodyWorkOwner;
use zakura_rt::sync::{
    atomic::{AtomicU8, Ordering},
    downgrade, CancellationToken, Weak,
};

use super::{block, BlockBudgetLedger, WorkItem, WorkQueue};
#[cfg(any(test, zakura_sim))]
use crate::zakura::block_sync::block_sync_observe::{BlockEvent, RequestStep};
use crate::zakura::transport::{ByteBudget, FrameWriteClaim};

const UNPUBLISHED: u8 = 0;
const QUEUED: u8 = 1;
const STARTED: u8 = 2;
const WRITTEN: u8 = 3;
const EXPIRED: u8 = 4;

#[cfg(test)]
mod tests;

/// Observes whether transport skipped an attempt without retaining its work or
/// byte reservation. The routine can keep this after the writer drops its claim.
///
/// In test and simulator builds it also carries its request's reporter, so
/// every expiry reports its step, whichever owner expires the request.
#[derive(Clone, Debug)]
pub(in crate::zakura::block_sync) struct RequestWriteStatus(
    Arc<AtomicU8>,
    #[cfg(any(test, zakura_sim))] Option<Arc<RequestReporter>>,
);

impl RequestWriteStatus {
    pub(in crate::zakura::block_sync) fn was_skipped(&self) -> bool {
        self.0.load(Ordering::Acquire) == EXPIRED
    }

    /// Whether transport started writing the frame, after which it cannot skip it.
    pub(in crate::zakura::block_sync) fn has_started(&self) -> bool {
        matches!(self.0.load(Ordering::Acquire), STARTED | WRITTEN)
    }

    /// Where the frame write stands now.
    #[cfg(any(test, zakura_sim))]
    pub(in crate::zakura::block_sync) fn phase(&self) -> RequestWritePhase {
        match self.0.load(Ordering::Acquire) {
            UNPUBLISHED => RequestWritePhase::Unpublished,
            QUEUED => RequestWritePhase::Queued,
            STARTED => RequestWritePhase::Started,
            WRITTEN => RequestWritePhase::Written,
            EXPIRED => RequestWritePhase::Skipped,
            state => unreachable!("request write state {state} is one of the five constants"),
        }
    }

    /// Retire a queued attempt atomically against writer startup. Returns true
    /// only for the transition, which test and simulator builds report as
    /// `RequestStep::Expired`; a started frame remains the transport's owner.
    ///
    /// The report follows the compare-exchange, so on a multi-threaded
    /// runtime the handle's drop on another thread can report
    /// `Dropped(Skipped)` before this reports `Expired`.
    pub(in crate::zakura::block_sync) fn expire_unwritten(&self) -> bool {
        let expired = self
            .0
            .compare_exchange(QUEUED, EXPIRED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        #[cfg(any(test, zakura_sim))]
        if let (true, Some(reporter)) = (expired, &self.1) {
            reporter.report(RequestStep::Expired);
        }
        expired
    }
}

/// Reports one request's steps to the queue's observer.
#[cfg(any(test, zakura_sim))]
#[derive(Debug)]
pub(in crate::zakura::block_sync) struct RequestReporter {
    observer: crate::zakura::block_sync::block_sync_observe::ObserverSlot,
    owner: BodyWorkOwner,
    heights: Vec<block::Height>,
    estimated_bytes: u64,
}

#[cfg(any(test, zakura_sim))]
impl RequestReporter {
    fn report(&self, step: RequestStep) {
        observe!(
            self.observer,
            BlockEvent::Request {
                owner: self.owner,
                heights: self.heights.clone(),
                estimated_bytes: self.estimated_bytes,
                step,
            }
        );
    }
}

/// Where a request's frame write stands. A queued request is published but
/// not yet sent: transport can still skip it.
#[cfg(any(test, zakura_sim))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RequestWritePhase {
    /// The attempt holds its items provisionally and hasn't published them.
    Unpublished,
    /// Published and queued for transport.
    Queued,
    /// Transport started writing the frame.
    Started,
    /// Transport wrote the frame.
    Written,
    /// Transport skipped the frame before writing it.
    Skipped,
}

/// Keep the write disposition readable while the last owner is being dropped.
#[derive(Debug)]
pub(super) struct RequestWriteRegistration {
    pub(super) claim: Weak<RequestWrite>,
    pub(super) status: RequestWriteStatus,
}

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
    state: Arc<AtomicU8>,
    /// Reports this request's steps, in test and simulator builds.
    #[cfg(any(test, zakura_sim))]
    reporter: Arc<RequestReporter>,
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
            #[cfg(any(test, zakura_sim))]
            reporter: Arc::new(RequestReporter {
                observer: work.observer.clone(),
                owner,
                heights: items.iter().map(|(height, _)| *height).collect(),
                estimated_bytes,
            }),
            owner,
            items,
            estimated_bytes,
            work,
            budget,
            cancel,
            state: Arc::new(AtomicU8::new(UNPUBLISHED)),
        })
    }

    #[cfg(not(any(test, zakura_sim)))]
    pub(in crate::zakura::block_sync) fn status(&self) -> RequestWriteStatus {
        RequestWriteStatus(self.state.clone())
    }

    #[cfg(any(test, zakura_sim))]
    pub(in crate::zakura::block_sync) fn status(&self) -> RequestWriteStatus {
        RequestWriteStatus(self.state.clone(), Some(self.reporter.clone()))
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
            let charge = item.estimated_bytes;
            inner.in_flight_reserved.add(charge);
        }
        inner.add_reserved_bytes(self.estimated_bytes);
        inner.request_writes.insert(
            self.owner,
            RequestWriteRegistration {
                claim: downgrade(self),
                status: self.status(),
            },
        );
        self.state.store(QUEUED, Ordering::Release);
        #[cfg(any(test, zakura_sim))]
        self.observe(RequestStep::Published);
        publish();
        true
    }

    /// Reports `step` to the queue's observer, if one is installed.
    #[cfg(any(test, zakura_sim))]
    fn observe(&self, step: RequestStep) {
        self.reporter.report(step);
    }

    pub(super) fn owner(&self) -> BodyWorkOwner {
        self.owner
    }

    pub(super) fn heights(&self) -> impl Iterator<Item = block::Height> + '_ {
        self.items.iter().map(|(height, _)| *height)
    }

    /// A reserved queue slot lost its receiver during publication. Settle the
    /// ledger now even if that closed channel retains a copy of this claim.
    pub(in crate::zakura::block_sync) fn delivery_failed(&self) {
        #[cfg(any(test, zakura_sim))]
        self.observe(RequestStep::DeliveryFailed);
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
        #[cfg(test)]
        tests::before_reset_height_check();
        self.items.last().is_some_and(|(height, _)| *height > floor)
    }

    pub(super) fn expire_unwritten(&self) {
        if self.status().expire_unwritten() {
            // A competing body can settle every byte without returning work.
            // Wake the routine even when claim cleanup has no budget to release.
            self.work.available.notify_waiters();
        }
    }

    pub(super) fn reset(&self) {
        #[cfg(any(test, zakura_sim))]
        self.observe(RequestStep::Reset);
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
        let started = self
            .state
            .compare_exchange(QUEUED, STARTED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        #[cfg(any(test, zakura_sim))]
        if started {
            self.observe(RequestStep::Started);
        }
        started
    }

    fn written(&self) {
        self.state.store(WRITTEN, Ordering::Release);
        #[cfg(any(test, zakura_sim))]
        self.observe(RequestStep::Written);
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
                #[cfg(test)]
                tests::before_drop_cleanup();
                self.expire_unwritten();
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
                self.work.lock().request_writes.remove(&self.owner);
            }
        }
        #[cfg(any(test, zakura_sim))]
        self.observe(RequestStep::Dropped(self.status().phase()));
    }
}
