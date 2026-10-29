//! Execution ownership for one admitted request.

use std::sync::Arc;

use zakura_rt::sync::{CancellationToken, Mutex};

use super::capacity::PeerBudgets;
use crate::zakura::regulation::SlotPermit;

/// The node and peer execution slots of one admitted request.
///
/// Admission takes the peer slot before the node slot, so the fields release
/// in the opposite order. Rust drops fields in declaration order, so `_node`
/// comes first: a waiter that the freed peer slot admits then finds the node
/// slot already returned. #976 found this order bug in the older copy.
#[derive(Debug)]
pub(crate) struct ExecutionSlots {
    _node: SlotPermit,
    _peer: SlotPermit,
    // Keep both budgets discoverable across reconnects while work remains.
    _peer_budgets: PeerBudgets,
    /// Reports the release after both slots return, in test and simulator
    /// builds. It drops last, on every path that drops the slots.
    #[cfg(any(test, zakura_sim))]
    released: Option<ReleaseReport>,
}

impl ExecutionSlots {
    pub(super) fn new(peer: SlotPermit, node: SlotPermit, peer_budgets: PeerBudgets) -> Self {
        Self {
            _node: node,
            _peer: peer,
            _peer_budgets: peer_budgets,
            #[cfg(any(test, zakura_sim))]
            released: None,
        }
    }

    /// These slots, running `released` once they return.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn reporting_release(mut self, released: Option<ReleaseReport>) -> Self {
        self.released = released;
        self
    }
}

/// A report that runs when it drops, in test and simulator builds.
#[cfg(any(test, zakura_sim))]
pub(crate) struct ReleaseReport(Option<Box<dyn FnOnce() + Send + Sync>>);

#[cfg(any(test, zakura_sim))]
impl ReleaseReport {
    pub(crate) fn new(report: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self(Some(Box::new(report)))
    }
}

#[cfg(any(test, zakura_sim))]
impl Drop for ReleaseReport {
    fn drop(&mut self) {
        if let Some(report) = self.0.take() {
            report();
        }
    }
}

#[cfg(any(test, zakura_sim))]
impl std::fmt::Debug for ReleaseReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReleaseReport")
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
enum ExecutionState {
    #[default]
    Queued,
    Started,
    Cancelled,
}

/// Shared ownership of one request's execution slots.
///
/// Every clone keeps the slots held. Move a clone into any blocking task that
/// `produce` starts, so a cancelled request keeps its capacity until that task
/// ends. Cancellation is advisory: work checks [`Self::is_cancelled`] between
/// steps.
#[derive(Clone, Debug)]
pub(crate) struct WorkLease {
    _slots: Arc<ExecutionSlots>,
    state: Arc<Mutex<ExecutionState>>,
    cancelled: CancellationToken,
}

impl WorkLease {
    pub(super) fn new(slots: ExecutionSlots) -> Self {
        Self {
            _slots: Arc::new(slots),
            state: Arc::default(),
            cancelled: CancellationToken::new(),
        }
    }

    /// Whether the requester is gone and further work is wasted.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.is_cancelled()
    }

    /// Claim the lease's one execution of a queued blocking job.
    ///
    /// Returns false after cancellation or an earlier claim, so a blocking job
    /// queued behind others never starts once its requester is gone.
    pub(crate) fn try_start(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("execution state is not poisoned because no holder panics");
        if *state != ExecutionState::Queued {
            return false;
        }
        *state = ExecutionState::Started;
        true
    }

    pub(super) fn cancel(&self) {
        let mut state = self
            .state
            .lock()
            .expect("execution state is not poisoned because no holder panics");
        if *state == ExecutionState::Queued {
            *state = ExecutionState::Cancelled;
        }
        self.cancelled.cancel();
    }
}

#[cfg(any(test, zakura_sim, feature = "zakura-testkit"))]
impl WorkLease {
    /// A lease outside any session, holding slots of its own one-slot
    /// budgets, for a storage range source's tests.
    pub(crate) fn detached() -> Self {
        use crate::zakura::regulation::{OutputByteBudget, SlotBudget};

        let budget = || SlotBudget::new(1).expect("one slot is a valid capacity");
        let reserve = |budget: &SlotBudget| {
            budget
                .try_reserve()
                .expect("a fresh one-slot budget has a free slot")
        };
        let (peer, node) = (budget(), budget());
        Self::new(ExecutionSlots::new(
            reserve(&peer),
            reserve(&node),
            PeerBudgets {
                output_responses: budget(),
                execution: peer,
                output: OutputByteBudget::new(1).expect("one byte is a valid capacity"),
            },
        ))
    }

    /// Cancels a [`Self::detached`] lease, as a closed session cancels its
    /// requests.
    pub(crate) fn cancel_detached(&self) {
        self.cancel();
    }
}
