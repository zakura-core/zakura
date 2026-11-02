//! The facts the reactor reports at a completed transition, for traces and observers.

use std::num::NonZeroU64;

use zakura_chain::block;
use zakura_header_chain::BodyWorkAuthority;

use super::{ActionRefusal, PendingNeededQuery, ReactorState};

/// How [`ReactorState::complete_needed_query`] classified a reply.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum QueryReplyOutcome {
    /// The reply matched no pending query.
    Stale,
    /// The reply matched under a retired authority.
    StaleAuthority,
    /// State's anchor moved ahead of the verified mirror.
    AnchorAdvanced,
    /// State's anchor sits behind or beside the verified mirror.
    AnchorReset,
    /// The reply's blocks became current work.
    Accepted,
}

/// One completed view, reset, or query transition of the reactor's state.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[must_use = "a completed transition must reach the trace and the observer"]
pub enum ReactorTransition {
    /// The action channel accepted a query, which became the pending query.
    QueryAccepted(PendingNeededQuery),
    /// The action channel refused a query. The pending query and next ID stayed.
    QueryRefused {
        /// The refused proposal.
        query: PendingNeededQuery,
        /// Why the channel refused it.
        refusal: ActionRefusal,
    },
    /// A needed-block reply reached the state.
    QueryReply {
        /// The reply's query ID.
        query_id: NonZeroU64,
        /// The reply's authority.
        scope: BodyWorkAuthority,
        /// The classification.
        outcome: QueryReplyOutcome,
    },
    /// A query failure reached the state.
    QueryFailed {
        /// The failure's query ID.
        query_id: NonZeroU64,
        /// The failure's authority.
        scope: BodyWorkAuthority,
        /// Whether it retired the pending query.
        matched: bool,
    },
    /// A forced opening lifted the retry gate, or a covered range or a
    /// body-size hint change retired the query state.
    QueryGateChanged,
    /// The retry gate's deadline passed.
    RetryElapsed,
    /// A Sequencer view asked for a reaction.
    View {
        /// The reaction is a destructive reset.
        reset: bool,
        /// The view advanced the verified tip.
        tip_advanced: bool,
    },
    /// A committed view changed body-work authority.
    AuthorityChanged {
        /// The new authority.
        authority: BodyWorkAuthority,
        /// Whether the change started a new body-work epoch.
        epoch_changed: bool,
    },
    /// The reactor asked the Sequencer to reset to a verified tip.
    TipReset {
        /// The reset target.
        tip: block::Height,
    },
}

/// The reactor's logical state at a completed transition.
///
/// It holds no deadline, because an instant does not replay across processes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ReactorSnapshot {
    /// The accepted query awaiting a reply.
    pub pending: Option<PendingNeededQuery>,
    /// The next accepted query's ID. `None` means the IDs ran out.
    pub next_query_id: Option<NonZeroU64>,
    /// Whether a retry deadline is armed.
    pub retry_armed: bool,
    /// Whether the next proposal owes a refused or failed query's force.
    pub retry_forced: bool,
    /// Whether the action channel reported closed.
    pub dispatch_closed: bool,
    /// The committed body-work authority, if the reactor has one.
    pub scope: Option<BodyWorkAuthority>,
    /// Verified-tip mirror.
    pub verified_block_tip: block::Height,
    /// Query and scheduling floor.
    pub request_floor: block::Height,
    /// Header-tip mirror.
    pub best_header_tip: block::Height,
    /// Last `reset_epoch` the reactor reacted to.
    pub last_reset_epoch: u64,
    /// Last `reaction_epoch` the reactor reacted to.
    pub last_reaction_epoch: u64,
}

impl ReactorTransition {
    /// Whether this is a failure that retired the pending query.
    pub(in super::super) fn retired_failed_query(self) -> bool {
        matches!(self, Self::QueryFailed { matched: true, .. })
    }
}

impl ReactorState {
    /// Read the logical state. The caller reports it at a completed transition.
    pub(in super::super) fn snapshot(&self) -> ReactorSnapshot {
        ReactorSnapshot {
            pending: self.queries.pending,
            next_query_id: self.queries.next_id,
            retry_armed: self.queries.retry_at.is_some(),
            retry_forced: self.queries.retry_forced,
            dispatch_closed: self.queries.dispatch_closed,
            scope: self.body_work_scope(),
            verified_block_tip: self.progress.verified_block_tip,
            request_floor: self.progress.request_floor,
            best_header_tip: self.progress.best_header_tip,
            last_reset_epoch: self.progress.last_reset_epoch,
            last_reaction_epoch: self.progress.last_reaction_epoch,
        }
    }
}
