//! Execution ownership for one admitted request.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::zakura::regulation::SlotPermit;

/// The peer and node execution slots of one admitted request.
#[derive(Debug)]
pub(crate) struct ExecutionSlots {
    _peer: SlotPermit,
    _node: SlotPermit,
}

impl ExecutionSlots {
    pub(super) fn new(peer: SlotPermit, node: SlotPermit) -> Self {
        Self {
            _peer: peer,
            _node: node,
        }
    }
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
    cancelled: CancellationToken,
}

impl WorkLease {
    pub(super) fn new(slots: ExecutionSlots) -> Self {
        Self {
            _slots: Arc::new(slots),
            cancelled: CancellationToken::new(),
        }
    }

    /// Whether the requester is gone and further work is wasted.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.is_cancelled()
    }

    pub(super) fn cancel(&self) {
        self.cancelled.cancel();
    }
}
