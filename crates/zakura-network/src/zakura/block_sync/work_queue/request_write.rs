//! Ownership from provisional download reservation through the request write.

use std::{ops::Range, sync::Arc};

use zakura_header_chain::BodyWorkOwner;
use zakura_rt::sync::{
    atomic::{AtomicU8, Ordering},
    downgrade, CancellationToken, Weak,
};

use super::super::admission::{request_priority, RequestPriority};
use super::{block, WorkItem, WorkQueue};
#[cfg(any(test, zakura_sim))]
use crate::zakura::block_sync::block_sync_observe::{BlockEvent, RequestStep};
use crate::zakura::transport::{BudgetClosure, ByteReservation, FrameWriteClaim};

const UNPUBLISHED: u8 = 0;
const QUEUED: u8 = 1;
const STARTED: u8 = 2;
const WRITTEN: u8 = 3;
const EXPIRED: u8 = 4;

/// Report one request step, in test and simulator builds.
macro_rules! report {
    ($reporter:expr, $step:expr) => {
        #[cfg(any(test, zakura_sim))]
        $reporter.report($step);
    };
}

#[cfg(test)]
mod tests;

/// Observes whether transport skipped an attempt without retaining its work or
/// byte reservation. The routine can keep this after the writer drops its claim.
///
/// It carries its request's reporter, so every expiry reports its step, whichever
/// owner expires the request.
#[derive(Clone, Debug)]
pub(in crate::zakura::block_sync) struct RequestWriteStatus(Arc<AtomicU8>, RequestReporter);

impl RequestWriteStatus {
    pub(in crate::zakura::block_sync) fn was_skipped(&self) -> bool {
        self.0.load(Ordering::Acquire) == EXPIRED
    }

    /// Whether transport started writing the frame, after which it cannot skip it.
    pub(in crate::zakura::block_sync) fn has_started(&self) -> bool {
        matches!(self.0.load(Ordering::Acquire), STARTED | WRITTEN)
    }

    /// Where the frame write stands now.
    #[cfg(any(test, zakura_sim, feature = "p2p-fuzz"))]
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
        if expired {
            report!(self.1, RequestStep::Expired);
        }
        expired
    }
}

/// Reports one request's steps to the queue's observer. Outside test and simulator
/// builds it is empty, and `report!` compiles to nothing.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::block_sync) struct RequestReporter(
    #[cfg(any(test, zakura_sim))] Option<Arc<RequestReport>>,
);

/// What a [`RequestReporter`] reports with each step.
#[cfg(any(test, zakura_sim))]
#[derive(Debug)]
pub(in crate::zakura::block_sync) struct RequestReport {
    observer: crate::zakura::block_sync::block_sync_observe::ObserverSlot,
    owner: BodyWorkOwner,
    heights: Vec<block::Height>,
    estimated_bytes: u64,
}

impl RequestReporter {
    fn new(
        work: &WorkQueue,
        owner: BodyWorkOwner,
        items: &[(block::Height, WorkItem)],
        estimated_bytes: u64,
    ) -> Self {
        #[cfg(not(any(test, zakura_sim)))]
        let _ = (work, owner, items, estimated_bytes);
        Self(
            #[cfg(any(test, zakura_sim))]
            Some(Arc::new(RequestReport {
                observer: work.observer.clone(),
                owner,
                heights: items.iter().map(|(height, _)| *height).collect(),
                estimated_bytes,
            })),
        )
    }

    #[cfg(any(test, zakura_sim))]
    fn report(&self, step: RequestStep) {
        if let Some(report) = &self.0 {
            observe!(
                report.observer,
                BlockEvent::Request {
                    owner: report.owner,
                    heights: report.heights.clone(),
                    estimated_bytes: report.estimated_bytes,
                    step,
                }
            );
        }
    }
}

/// Where a request's frame write stands. A queued request is published but
/// not yet sent: transport can still skip it.
#[cfg(any(test, zakura_sim, feature = "p2p-fuzz"))]
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

/// Tracks one published request while the queue owns its funding.
/// The work ledger arbitrates settlement by exact attempt identity.
#[derive(Debug)]
pub(crate) struct RequestWrite {
    owner: BodyWorkOwner,
    items: Vec<(block::Height, WorkItem)>,
    work: Arc<WorkQueue>,
    cancel: CancellationToken,
    state: Arc<AtomicU8>,
    reporter: RequestReporter,
}

/// Owns an exact provisional take until admission or rollback.
#[derive(Debug)]
pub(in crate::zakura::block_sync) struct TakenRequest {
    work: Arc<WorkQueue>,
    items: Vec<(block::Height, WorkItem)>,
    reporter: RequestReporter,
}

/// Why a provisional take could not acquire funding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) enum AdmitRefusal {
    Empty,
    MissingOwner,
    AmountOverflow,
    AccountingClosed,
    Cancelled,
    StaleClaim,
    /// The session's published reservations would pass its owner cap.
    OwnerCap,
    Capacity,
}

impl TakenRequest {
    pub(super) fn new(work: Arc<WorkQueue>, items: Vec<(block::Height, WorkItem)>) -> Self {
        Self {
            work,
            items,
            reporter: RequestReporter::default(),
        }
    }

    pub(in crate::zakura::block_sync) fn items(&self) -> &[(block::Height, WorkItem)] {
        &self.items
    }

    /// Keep one contiguous retry-eligible run and quietly return the rest.
    pub(in crate::zakura::block_sync) fn retain_range(&mut self, keep: Range<usize>) {
        assert!(keep.start <= keep.end && keep.end <= self.items.len());
        {
            let mut inner = self.work.lock();
            inner.state.return_unpublished(&self.items[..keep.start]);
            inner.state.return_unpublished(&self.items[keep.end..]);
        }
        self.items.truncate(keep.end);
        self.items.drain(..keep.start);
    }

    /// Admit the retained take against its queue's budget. Derive the charge and
    /// floor exception from its actual items and the supplied download-floor view.
    pub(in crate::zakura::block_sync) fn admit(
        mut self,
        download_floor: block::Height,
        cancel: CancellationToken,
    ) -> Result<UnpublishedRequest, AdmitRefusal> {
        let (first_height, first_item) = self.items.first().ok_or(AdmitRefusal::Empty)?;
        let owner = first_item.owner.ok_or(AdmitRefusal::MissingOwner)?;
        let priority = request_priority(download_floor, *first_height);
        let bytes = self
            .items
            .iter()
            .try_fold(0u64, |total, (_, item)| {
                total.checked_add(item.estimated_bytes)
            })
            .ok_or(AdmitRefusal::AmountOverflow)?;
        let funding = {
            let inner = self.work.lock();
            if self.work.budget.is_closed() {
                return Err(AdmitRefusal::AccountingClosed);
            }
            if cancel.is_cancelled() {
                return Err(AdmitRefusal::Cancelled);
            }
            if !inner.state.is_unpublished_claim(owner, &self.items) {
                return Err(AdmitRefusal::StaleClaim);
            }
            let held = inner.state.session_reserved(owner.session_id);
            if !self.work.owner_cap.admits(held, bytes, priority) {
                return Err(AdmitRefusal::OwnerCap);
            }
            match self.work.budget.reserve_owned(bytes) {
                Some(funding) => funding,
                None if priority == RequestPriority::Floor => {
                    let funding = self
                        .work
                        .budget
                        .charge_owned(bytes)
                        .ok_or_else(|| self.funding_refusal())?;
                    metrics::counter!("sync.block.budget.floor_overdraft").increment(1);
                    funding
                }
                None => return Err(self.funding_refusal()),
            }
        };
        self.reporter = RequestReporter::new(&self.work, owner, &self.items, bytes);
        report!(self.reporter, RequestStep::Funded);
        Ok(UnpublishedRequest {
            taken: self,
            owner,
            priority,
            funding,
            cancel,
        })
    }

    fn funding_refusal(&self) -> AdmitRefusal {
        if self.work.budget.is_closed() {
            AdmitRefusal::AccountingClosed
        } else {
            AdmitRefusal::Capacity
        }
    }
}

impl Drop for TakenRequest {
    fn drop(&mut self) {
        report!(
            self.reporter,
            RequestStep::Dropped(RequestWritePhase::Unpublished)
        );
        if !self.items.is_empty() {
            self.work.return_unpublished(&self.items);
        }
    }
}

/// Owns provisional funding until publication consumes the request.
#[derive(Debug)]
pub(in crate::zakura::block_sync) struct UnpublishedRequest {
    taken: TakenRequest,
    owner: BodyWorkOwner,
    priority: RequestPriority,
    funding: ByteReservation,
    cancel: CancellationToken,
}

impl UnpublishedRequest {
    pub(in crate::zakura::block_sync) fn owner(&self) -> BodyWorkOwner {
        self.owner
    }

    pub(in crate::zakura::block_sync) fn priority(&self) -> RequestPriority {
        self.priority
    }

    pub(in crate::zakura::block_sync) fn bytes(&self) -> u64 {
        self.funding.bytes()
    }

    pub(in crate::zakura::block_sync) fn items(&self) -> &[(block::Height, WorkItem)] {
        self.taken.items()
    }

    /// Publish outstanding state and enqueue under the work lock. The local
    /// claim survives the callback so a closed channel cannot drop it under the lock.
    pub(in crate::zakura::block_sync) fn publish(
        self,
        publish: impl FnOnce(&Arc<RequestWrite>),
    ) -> Option<Arc<RequestWrite>> {
        let Self {
            mut taken,
            owner,
            mut funding,
            cancel,
            ..
        } = self;
        let claim = Arc::new(RequestWrite {
            reporter: std::mem::take(&mut taken.reporter),
            owner,
            items: std::mem::take(&mut taken.items),
            work: taken.work.clone(),
            cancel,
            state: Arc::new(AtomicU8::new(UNPUBLISHED)),
        });
        {
            let mut inner = claim.work.lock();
            if claim.work.budget.is_closed()
                || claim.cancel.is_cancelled()
                || !inner.state.is_unpublished_claim(claim.owner, &claim.items)
            {
                return None;
            }
            let funded_bytes = funding.bytes();
            if inner.funding.merge(&mut funding).is_err() {
                return None;
            }
            let transferred = inner.state.publish_claim(&claim.items);
            if transferred != Some(funded_bytes) {
                claim.work.budget.close(BudgetClosure::Funding {
                    logical: inner.state.reserved_bytes(),
                    funded: inner.funding.bytes(),
                });
                return None;
            }
            inner.request_writes.insert(
                claim.owner,
                RequestWriteRegistration {
                    claim: downgrade(&claim),
                    status: claim.status(),
                },
            );
            claim.state.store(QUEUED, Ordering::Release);
            report!(claim.reporter, RequestStep::Published);
            // The queue transition is complete. Release its lock before unwinding
            // a callback so claim cleanup can settle the published reservation.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| publish(&claim)));
            drop(inner);
            if let Err(panic) = result {
                std::panic::resume_unwind(panic);
            }
        }
        Some(claim)
    }
}

impl RequestWrite {
    pub(in crate::zakura::block_sync) fn status(&self) -> RequestWriteStatus {
        RequestWriteStatus(self.state.clone(), self.reporter.clone())
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
        report!(self.reporter, RequestStep::DeliveryFailed);
        self.cancel.cancel();
        self.expire_unwritten();
        self.work
            .release_reserved_and_return_items_detailed_for_owner(
                self.owner,
                self.items.iter().map(|(height, _)| *height),
            );
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
        report!(self.reporter, RequestStep::Reset);
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
        let current = !self.work.budget.is_closed()
            && !self.cancel.is_cancelled()
            && inner.state.is_published_claim(self.owner, self.heights());
        if !current {
            self.expire_unwritten();
            return false;
        }
        let started = self
            .state
            .compare_exchange(QUEUED, STARTED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if started {
            report!(self.reporter, RequestStep::Started);
        }
        started
    }

    fn written(&self) {
        self.state.store(WRITTEN, Ordering::Release);
        report!(self.reporter, RequestStep::Written);
        self.work.lock().request_writes.remove(&self.owner);
    }
}

impl Drop for RequestWrite {
    fn drop(&mut self) {
        match self.state.load(Ordering::Acquire) {
            UNPUBLISHED => {
                self.work.return_unpublished(&self.items);
                // The enclosing unpublished request releases its funding.
            }
            WRITTEN => {}
            state => {
                #[cfg(test)]
                tests::before_drop_cleanup();
                self.expire_unwritten();
                if state == STARTED {
                    self.cancel.cancel();
                }
                self.work
                    .release_reserved_and_return_items_detailed_for_owner(
                        self.owner,
                        self.items.iter().map(|(height, _)| *height),
                    );
                self.work.lock().request_writes.remove(&self.owner);
            }
        }
        report!(self.reporter, RequestStep::Dropped(self.status().phase()));
    }
}
