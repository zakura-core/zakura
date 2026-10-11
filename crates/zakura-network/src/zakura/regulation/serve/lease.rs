//! Execution ownership for one admitted request.

use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

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
}

impl ExecutionSlots {
    pub(super) fn new(peer: SlotPermit, node: SlotPermit, peer_budgets: PeerBudgets) -> Self {
        Self {
            _node: node,
            _peer: peer,
            _peer_budgets: peer_budgets,
        }
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
