//! Logical block-sync reactor state.
//!
//! These types hold plain values. They own no channel, session, watch, timer,
//! or shared-state handle, and they read no clock. [`super::ReactorIO`] owns
//! those runtime resources. It reads the shared owners, passes the facts and
//! the time in here, and runs the operations this state chooses.
//!
//! A needed-block query moves through three steps:
//!
//! 1. [`QueryState::open`] applies the retry, quiet-period, and floor gates.
//! 2. [`QueryState::propose`] applies the low-water, authority, identifier, and
//!    deduplication rules, and returns a [`QueryProposal`].
//! 3. The reactor offers the proposal to the action channel and reports the
//!    synchronous result through [`QueryState::accepted`] or
//!    [`QueryState::refused`] before it reads another input.
//!
//! Only an accepted proposal replaces the pending query and consumes a query ID.
//! A full channel keeps the pending query and retries with the refused query's
//! force. A closed channel ends dispatch; a reply to an accepted query still
//! completes it.
//! Body-availability requests follow the same rule: only an accepted request
//! records its deduplication key.

use std::{collections::BTreeSet, num::NonZeroU64};

use zakura_chain::block;
use zakura_header_chain::{
    BodyCompletionDecision, BodySupplierDiscovered, BodyUnavailableSummary, BodyWorkAuthority,
    BodyWorkOwner, CommittedHeaderChainView, EngineSnapshot, EvidenceId, Frontier, Gate,
    OperatorBodyRetry, PendingOwners, SourceId, StaleReason, StateVersion,
};
use zakura_rt::{
    collections::HashMap,
    time::{self, Instant},
};

use super::{
    BlockSyncAction, BlockSyncFrontiers, BlockSyncStatus, OutstandingClaim, SequencerView,
    ServiceAdmissionDecision, ServicePeerDirection, ServicePeerLimits, WorkReturnOutcome,
    ZakuraBlockSyncConfig, ZakuraPeerId, EMPTY_STATE_HEADER_QUIET_MIN_LAG,
    EMPTY_STATE_HEADER_QUIET_PERIOD, NEEDED_BLOCK_QUERY_RETRY_DELAY, NEEDED_BLOCK_REFILL_LIMIT,
};
use crate::zakura::block_sync::state::next_height;
use crate::zakura::block_sync::status::StatusDelivery;

#[cfg(any(test, feature = "p2p-fuzz"))]
mod fuzz;
#[cfg(any(test, feature = "p2p-fuzz"))]
mod tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use fuzz::{replay_block_query_bytes, BlockQueryReplaySummary};
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use tests::{
    replay_block_reactor_bytes, trace_block_reactor_bytes, BlockReactorHistory,
    BlockReactorReplaySummary, BlockReactorStep, ReactorMachineAction, ReactorMachineQuery,
    ReactorMachineView,
};

/// The block-sync reactor's logical state.
#[derive(Debug)]
pub(super) struct ReactorState {
    /// Whether this node schedules body work from state queries.
    state_queries_enabled: bool,
    pub(super) queries: QueryState,
    pub(super) progress: ReactorProgress,
    pub(super) bootstrap: BootstrapGate,
    /// Sorted needed download heights, published as the candidate gap.
    pub(super) needed_heights: Vec<block::Height>,
    /// Latest committed header-engine view. It stamps body-work authority.
    committed_view: Option<CommittedHeaderChainView>,
    /// Body-availability requests accepted against the current committed view.
    alarm_requests: AlarmRequests,
    pub(super) status: StatusSchedule,
}

impl ReactorState {
    pub(super) fn new(
        state_queries_enabled: bool,
        progress: ReactorProgress,
        bootstrap: BootstrapGate,
        committed_view: Option<CommittedHeaderChainView>,
    ) -> Self {
        Self {
            state_queries_enabled,
            queries: QueryState::new(),
            progress,
            bootstrap,
            needed_heights: Vec::new(),
            committed_view,
            alarm_requests: AlarmRequests::default(),
            status: StatusSchedule::default(),
        }
    }

    pub(super) fn committed_view(&self) -> Option<&CommittedHeaderChainView> {
        self.committed_view.as_ref()
    }

    /// The body-work authority of the latest committed view.
    /// `None` until the reactor has a committed view.
    pub(super) fn body_work_scope(&self) -> Option<BodyWorkAuthority> {
        self.committed_view
            .as_ref()
            .map(BodyWorkAuthority::for_view)
    }

    /// Apply the query gates that need no shared-state reads.
    pub(super) fn open_query(&mut self, force: bool) -> QueryOpening {
        if !self.state_queries_enabled {
            return QueryOpening::Closed;
        }
        self.queries.open(QueryGate {
            force,
            header_quiet: self.bootstrap.is_header_quiet(),
            diverged: self.verified_tip_diverged(),
            request_floor: self.progress.request_floor,
            best_header_tip: self.progress.best_header_tip,
        })
    }

    /// Plan the query that `opening` allows.
    ///
    /// `work` carries the shared-owner facts that [`QueryOpening::NeedsWork`]
    /// asks for. A hint refresh needs none.
    pub(super) fn propose_query(&self, force: bool, start: QueryStart) -> QueryPlan {
        self.queries.propose(QueryRequest {
            force,
            best_header_tip: self.progress.best_header_tip,
            best_header_hash: self.progress.best_header_hash,
            scope: self.body_work_scope(),
            diverged: self.verified_tip_diverged(),
            start,
        })
    }

    /// Whether the committed verified tip lies off the selected chain at or
    /// above its tip.
    ///
    /// Such a tip leaves no selected height above the floor, yet only state's
    /// answer names the common ancestor block sync must reset to.
    fn verified_tip_diverged(&self) -> bool {
        self.committed_view.as_ref().is_some_and(|view| {
            let frontiers = view.frontiers;
            frontiers.verified_best.height > frontiers.header_best.height
                || (frontiers.verified_best.height == frontiers.header_best.height
                    && frontiers.verified_best.hash != frontiers.header_best.hash)
        })
    }

    /// Classify a needed-block reply against the pending query and the current
    /// body-work authority.
    ///
    /// An unrelated reply leaves the pending query in place. A matching reply
    /// always retires the pending query, even when its authority or anchor is
    /// stale.
    pub(super) fn complete_needed_query(
        &mut self,
        query_id: NonZeroU64,
        scope: BodyWorkAuthority,
        body_anchor: Frontier,
    ) -> NeededReply {
        let Some(query) = self.queries.complete(query_id, scope) else {
            return NeededReply::Stale;
        };
        if self.body_work_scope() != Some(scope) {
            return NeededReply::StaleAuthority;
        }
        let progress = &self.progress;
        if body_anchor.height != progress.verified_block_tip
            || body_anchor.hash != progress.verified_block_hash
        {
            let frontiers = BlockSyncFrontiers {
                finalized_height: progress.finalized_height,
                verified_block_tip: body_anchor.height,
                verified_block_hash: body_anchor.hash,
            };
            return if body_anchor.height > progress.verified_block_tip {
                NeededReply::AnchorAdvanced(frontiers)
            } else {
                NeededReply::AnchorReset(frontiers)
            };
        }
        NeededReply::Accept(query)
    }

    /// Update the progress mirrors from a Sequencer view.
    ///
    /// The mirrors change on every view. The expensive reaction runs only when
    /// `reaction_epoch` advances. A destructive reset also clears the pending
    /// query, whose bounds predate the reset, so the reaction must force a refill.
    pub(super) fn observe_view(&mut self, view: SequencerView) -> ViewReaction {
        let reset = view.reset_epoch != self.progress.last_reset_epoch;
        let react = view.reaction_epoch != self.progress.last_reaction_epoch;
        let tip_advanced = view.verified_tip > self.progress.verified_block_tip;

        if view.verified_tip > block::Height(0) {
            self.bootstrap.start_body_sync();
        }
        self.progress.observe_view(view);

        if !react {
            return ViewReaction::ObserveOnly;
        }
        self.progress.last_reaction_epoch = view.reaction_epoch;
        if reset {
            self.progress.last_reset_epoch = view.reset_epoch;
            self.queries.clear();
        }
        ViewReaction::React {
            reset,
            tip_advanced,
        }
    }

    /// Replace the needed heights with the sorted, deduplicated `heights`.
    pub(super) fn set_needed_heights(&mut self, heights: impl IntoIterator<Item = block::Height>) {
        self.needed_heights = heights.into_iter().collect();
        self.needed_heights.sort_unstable();
        self.needed_heights.dedup();
    }

    /// Drop needed heights at or below the request floor. Returns whether the
    /// candidate gap changed.
    #[must_use = "a changed candidate gap must be republished"]
    pub(super) fn prune_needed_below_floor(&mut self) -> bool {
        let floor = self.progress.request_floor;
        let before = self.needed_heights.len();
        self.needed_heights.retain(|height| *height > floor);
        self.needed_heights.len() != before
    }
}

// ---- transition evidence ----

/// A query's exact identity, authority, and bounds, for traces and observers.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct QueryIdentity {
    /// The query's exact ID.
    pub query_id: NonZeroU64,
    /// The body-work authority that dispatched it.
    pub scope: BodyWorkAuthority,
    /// First requested height.
    pub from: block::Height,
    /// Requested height count.
    pub limit: u32,
    /// Header tip the query asked about.
    pub best_header_tip: block::Height,
    /// Whether the query bypassed the low-water gate.
    pub forced: bool,
}

impl From<PendingNeededQuery> for QueryIdentity {
    fn from(query: PendingNeededQuery) -> Self {
        Self {
            query_id: query.query_id,
            scope: query.scope,
            from: query.from,
            limit: query.limit,
            best_header_tip: query.best_header_tip,
            forced: query.forced,
        }
    }
}

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
pub enum ReactorTransition {
    /// The action channel accepted a query, which became the pending query.
    QueryAccepted(QueryIdentity),
    /// The action channel refused a query. The pending query and next ID stayed.
    QueryRefused {
        /// The refused proposal.
        query: QueryIdentity,
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
    pub pending: Option<QueryIdentity>,
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

impl ReactorState {
    /// Read the logical state. The caller reports it at a completed transition.
    pub(super) fn snapshot(&self) -> ReactorSnapshot {
        ReactorSnapshot {
            pending: self.queries.pending.map(Into::into),
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

// ---- queries ----

/// Identity, authority, and bounds of one body-missing metadata query.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingNeededQuery {
    pub(super) query_id: NonZeroU64,
    pub(super) scope: BodyWorkAuthority,
    pub(super) from: block::Height,
    pub(super) limit: u32,
    pub(super) best_header_tip: block::Height,
    pub(super) best_header_hash: block::Hash,
    pub(super) hint_revision: Option<u64>,
    /// The query bypassed the low-water gate. Its refusal or failure carries
    /// that bypass into the retry.
    pub(super) forced: bool,
}

impl PendingNeededQuery {
    /// Whether `self` asks for the same metadata as `other`. Query IDs may differ.
    fn covers(&self, other: &Self) -> bool {
        self.hint_revision == other.hint_revision
            && self.scope == other.scope
            && self.from == other.from
            && self.limit == other.limit
            && self.best_header_tip == other.best_header_tip
            && self.best_header_hash == other.best_header_hash
    }
}

/// Revision and bounded height interval of queued hints being refreshed.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct HintRefresh {
    pub(super) revision: u64,
    pub(super) from: block::Height,
    pub(super) through: block::Height,
}

/// Query facts the reactor holds without reading a shared owner.
#[derive(Copy, Clone, Debug)]
pub(super) struct QueryGate {
    /// Skip the retry gate and the low-water gate.
    pub(super) force: bool,
    /// The empty-state header quiet period is holding body queries.
    pub(super) header_quiet: bool,
    /// The committed verified tip left the selected chain, so a floor at the
    /// header tip does not end the query.
    pub(super) diverged: bool,
    pub(super) request_floor: block::Height,
    pub(super) best_header_tip: block::Height,
}

/// What a query may do after [`QueryState::open`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum QueryOpening {
    /// A gate holds the query.
    Closed,
    /// Continue the hint refresh. It needs no work facts.
    Refresh(HintRefresh),
    /// Read [`WorkFacts`] from the shared owners and propose.
    NeedsWork,
}

/// Shared-owner facts for an ordinary refill query.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct WorkFacts {
    /// The lowest height above the request floor that no work item covers.
    pub(super) first_unclaimed: Option<block::Height>,
    /// Pending work heights plus unreceived requested heights.
    pub(super) backlog: usize,
    /// The refill low-water target in heights.
    pub(super) low_water: usize,
}

/// Where a proposed query starts.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum QueryStart {
    Refresh(HintRefresh),
    Work(WorkFacts),
}

/// Inputs to [`QueryState::propose`].
#[derive(Copy, Clone, Debug)]
pub(super) struct QueryRequest {
    pub(super) force: bool,
    pub(super) best_header_tip: block::Height,
    pub(super) best_header_hash: block::Hash,
    /// Current body-work authority, if the reactor has one.
    pub(super) scope: Option<BodyWorkAuthority>,
    /// The committed verified tip left the selected chain: without other work,
    /// the query asks state about the header tip.
    pub(super) diverged: bool,
    pub(super) start: QueryStart,
}

/// A query that the reactor must offer to the action channel.
///
/// The proposal changes nothing until the reactor reports the channel's
/// answer to [`QueryState::accepted`] or [`QueryState::refused`].
#[derive(Debug, Eq, PartialEq)]
#[must_use = "the action channel's answer must reach QueryState"]
pub(super) struct QueryProposal {
    query: PendingNeededQuery,
}

impl QueryProposal {
    pub(super) fn query(&self) -> PendingNeededQuery {
        self.query
    }

    /// The action that asks state for this query's metadata.
    pub(super) fn action(&self) -> BlockSyncAction {
        BlockSyncAction::QueryNeededBlocks {
            query_id: self.query.query_id,
            from: self.query.from,
            limit: self.query.limit,
            best_header_tip: self.query.best_header_tip,
            scope: self.query.scope,
        }
    }
}

/// The outcome of [`QueryState::propose`].
#[derive(Debug, Eq, PartialEq)]
pub(super) enum QueryPlan {
    /// No height needs a query, or the backlog is above the low-water target.
    Idle,
    /// The pending query already asks for the same metadata.
    Covered,
    /// The reactor has no committed body-work authority.
    NoAuthority,
    /// The reactor exhausted the query identifier space.
    IdsExhausted,
    Dispatch(QueryProposal),
}

/// The pending needed-block query, its retry gate, and the hint refresh.
#[derive(Debug)]
pub(super) struct QueryState {
    /// The accepted query awaiting a reply. Only an accepted dispatch replaces it.
    pending: Option<PendingNeededQuery>,
    /// The identifier for the next accepted query.
    /// `None` means the reactor exhausted the identifier space and stops scheduling.
    next_id: Option<NonZeroU64>,
    /// Earliest time to retry the last refused or failed query.
    retry_at: Option<Instant>,
    /// The next proposal bypasses the low-water gate, because a forced query
    /// was refused or failed before state answered it.
    retry_forced: bool,
    /// The action channel reported closed. The reactor proposes no more queries.
    dispatch_closed: bool,
    hint_refresh: Option<HintRefresh>,
}

impl QueryState {
    pub(super) fn new() -> Self {
        Self {
            pending: None,
            next_id: NonZeroU64::new(1),
            retry_at: None,
            retry_forced: false,
            dispatch_closed: false,
            hint_refresh: None,
        }
    }

    pub(super) fn pending(&self) -> Option<PendingNeededQuery> {
        self.pending
    }

    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn next_id(&self) -> Option<NonZeroU64> {
        self.next_id
    }

    pub(super) fn retry_at(&self) -> Option<Instant> {
        self.retry_at
    }

    pub(super) fn hint_refresh(&self) -> Option<HintRefresh> {
        self.hint_refresh
    }

    /// Whether a refused or failed forced query still owes a forced retry.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn retry_forced(&self) -> bool {
        self.retry_forced
    }

    /// Whether the action channel reported closed.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn dispatch_closed(&self) -> bool {
        self.dispatch_closed
    }

    /// Apply the closed-channel, retry, quiet-period, and floor gates.
    ///
    /// A forced query lifts the retry gate. A floor at the header tip retires
    /// the pending query. A hint refresh that the floor passed ends.
    pub(super) fn open(&mut self, gate: QueryGate) -> QueryOpening {
        if self.dispatch_closed {
            return QueryOpening::Closed;
        }
        if gate.force {
            self.retry_at = None;
        } else if self.retry_at.is_some() {
            return QueryOpening::Closed;
        }
        if gate.header_quiet {
            return QueryOpening::Closed;
        }
        if gate.request_floor >= gate.best_header_tip && !gate.diverged {
            self.clear();
            return QueryOpening::Closed;
        }
        if self
            .hint_refresh
            .is_some_and(|refresh| refresh.through <= gate.request_floor)
        {
            self.hint_refresh = None;
        }
        match self.hint_refresh {
            Some(refresh) => QueryOpening::Refresh(refresh),
            None => QueryOpening::NeedsWork,
        }
    }

    /// Choose the next query without changing state.
    ///
    /// An owed forced retry bypasses the low-water gate like `request.force`.
    pub(super) fn propose(&self, request: QueryRequest) -> QueryPlan {
        let force = request.force || self.retry_forced;
        let (from, limit, hint_revision) = match request.start {
            QueryStart::Refresh(refresh) => (
                refresh.from,
                refresh
                    .through
                    .0
                    .saturating_sub(refresh.from.0)
                    .saturating_add(1)
                    .min(NEEDED_BLOCK_REFILL_LIMIT),
                Some(refresh.revision),
            ),
            QueryStart::Work(work) => {
                let Some(from) = work
                    .first_unclaimed
                    .filter(|from| *from <= request.best_header_tip)
                    .or(request.diverged.then_some(request.best_header_tip))
                else {
                    return QueryPlan::Idle;
                };
                if !force && work.backlog >= work.low_water {
                    return QueryPlan::Idle;
                }
                (
                    from,
                    refill_query_limit(from, request.best_header_tip, work.low_water),
                    None,
                )
            }
        };
        let Some(scope) = request.scope else {
            return QueryPlan::NoAuthority;
        };
        let Some(query_id) = self.next_id else {
            return QueryPlan::IdsExhausted;
        };
        let query = PendingNeededQuery {
            query_id,
            scope,
            from,
            limit,
            best_header_tip: request.best_header_tip,
            best_header_hash: request.best_header_hash,
            hint_revision,
            forced: force,
        };
        if self.pending.is_some_and(|pending| pending.covers(&query)) {
            return QueryPlan::Covered;
        }
        QueryPlan::Dispatch(QueryProposal { query })
    }

    /// The action channel accepted `proposal`: it becomes the pending query and
    /// consumes its query ID.
    pub(super) fn accepted(&mut self, proposal: QueryProposal) {
        let query_id = proposal.query.query_id;
        self.pending = Some(proposal.query);
        self.retry_at = None;
        self.retry_forced = false;
        self.next_id = query_id.get().checked_add(1).and_then(NonZeroU64::new);
    }

    /// The action channel refused `proposal`. The pending query and the next
    /// query ID stay.
    ///
    /// A full channel retries after [`NEEDED_BLOCK_QUERY_RETRY_DELAY`], with the
    /// refused query's force. A closed channel can never accept a retry, so it
    /// ends dispatch and arms no wake.
    pub(super) fn refused(
        &mut self,
        proposal: QueryProposal,
        refusal: ActionRefusal,
        now: Instant,
    ) {
        match refusal {
            ActionRefusal::Closed => {
                self.dispatch_closed = true;
                self.retry_at = None;
                self.retry_forced = false;
            }
            ActionRefusal::ControlReserve | ActionRefusal::Full => {
                self.retry_forced |= proposal.query.forced;
                self.schedule_retry(now);
            }
        }
    }

    /// Take the pending query when `(query_id, scope)` identifies it, and lift
    /// the retry gate. An owed forced retry survives for the next proposal.
    pub(super) fn complete(
        &mut self,
        query_id: NonZeroU64,
        scope: BodyWorkAuthority,
    ) -> Option<PendingNeededQuery> {
        let query = self
            .pending
            .filter(|pending| pending.query_id == query_id && pending.scope == scope)?;
        self.pending = None;
        self.retry_at = None;
        Some(query)
    }

    /// Retire the pending query when `(query_id, scope)` identifies it, and
    /// schedule a retry. Returns whether the failure matched.
    #[must_use = "an unmatched failure is a stale completion"]
    pub(super) fn fail(
        &mut self,
        query_id: NonZeroU64,
        scope: BodyWorkAuthority,
        now: Instant,
    ) -> bool {
        let Some(query) = self
            .pending
            .filter(|pending| pending.query_id == query_id && pending.scope == scope)
        else {
            return false;
        };
        self.pending = None;
        self.retry_forced |= query.forced;
        self.schedule_retry(now);
        true
    }

    /// Lift the retry gate when its deadline passes.
    pub(super) fn retry_elapsed(&mut self) {
        self.retry_at = None;
    }

    /// Retire the pending query and lift the retry gate and any owed forced retry.
    ///
    /// The caller's reset, authority change, or empty range supersedes both.
    pub(super) fn clear(&mut self) {
        self.pending = None;
        self.retry_at = None;
        self.retry_forced = false;
    }

    /// End the hint refresh, for example after a body-work epoch change.
    pub(super) fn end_hint_refresh(&mut self) {
        self.hint_refresh = None;
    }

    /// Refresh hints for `revision` across the queued work `bounds`, if any.
    pub(super) fn start_hint_refresh(
        &mut self,
        revision: u64,
        bounds: Option<(block::Height, block::Height)>,
    ) {
        self.hint_refresh = bounds.map(|(from, through)| HintRefresh {
            revision,
            from,
            through,
        });
    }

    /// Advance the hint refresh past `completed` when it covered the refresh's
    /// next batch. Returns whether the refresh continues.
    #[must_use = "a continuing hint refresh needs its next query"]
    pub(super) fn advance_hint_refresh(&mut self, completed: &PendingNeededQuery) -> bool {
        let Some(refresh) = self.hint_refresh else {
            return false;
        };
        if completed.hint_revision == Some(refresh.revision) && completed.from == refresh.from {
            let next = block::Height(refresh.from.0.saturating_add(completed.limit));
            self.hint_refresh = (next <= refresh.through).then_some(HintRefresh {
                from: next,
                ..refresh
            });
        }
        self.hint_refresh.is_some()
    }

    fn schedule_retry(&mut self, now: Instant) {
        self.retry_at = Some(now + NEEDED_BLOCK_QUERY_RETRY_DELAY);
    }
}

/// Heights an ordinary refill query asks for: twice the low-water target,
/// capped by the remaining header range and by state's read limit.
fn refill_query_limit(
    from: block::Height,
    best_header_tip: block::Height,
    low_water: usize,
) -> u32 {
    let remaining = best_header_tip.0.saturating_sub(from.0).saturating_add(1);
    let fanout_window = low_water.saturating_mul(2).max(1);
    let fanout_window = u32::try_from(fanout_window).unwrap_or(u32::MAX);

    remaining.min(fanout_window).min(NEEDED_BLOCK_REFILL_LIMIT)
}

/// The outcome of [`ReactorState::complete_needed_query`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum NeededReply {
    /// The reply does not match the pending query.
    Stale,
    /// The reply matched, but body-work authority changed since dispatch.
    /// The reactor queries again under the current authority.
    StaleAuthority,
    /// State's body anchor is ahead of the verified mirror.
    AnchorAdvanced(BlockSyncFrontiers),
    /// State's body anchor is behind or beside the verified mirror.
    AnchorReset(BlockSyncFrontiers),
    /// The reply's blocks are current work for this completed query.
    Accept(PendingNeededQuery),
}

impl NeededReply {
    pub(super) fn outcome(&self) -> QueryReplyOutcome {
        match self {
            Self::Stale => QueryReplyOutcome::Stale,
            Self::StaleAuthority => QueryReplyOutcome::StaleAuthority,
            Self::AnchorAdvanced(_) => QueryReplyOutcome::AnchorAdvanced,
            Self::AnchorReset(_) => QueryReplyOutcome::AnchorReset,
            Self::Accept(_) => QueryReplyOutcome::Accepted,
        }
    }
}

// ---- progress mirrors ----

/// The reactor's mirrors of header, verified, and Sequencer progress.
#[derive(Debug)]
pub(super) struct ReactorProgress {
    pub(super) finalized_height: block::Height,
    /// Mirror of the Sequencer's verified tip.
    pub(super) verified_block_tip: block::Height,
    pub(super) verified_block_hash: block::Hash,
    /// Highest height this node advertises as servable.
    pub(super) servable_high: block::Height,
    pub(super) servable_hash: block::Hash,
    /// Scheduler and query lower bound. It follows the Sequencer's download
    /// floor, but it is not verified state and never reaches status.
    pub(super) request_floor: block::Height,
    pub(super) best_header_tip: block::Height,
    pub(super) best_header_hash: block::Hash,
    /// Last `reset_epoch` the reactor reacted to, so it can tell an advance
    /// from a destructive reset.
    pub(super) last_reset_epoch: u64,
    /// Last `reaction_epoch` the reactor reacted to.
    pub(super) last_reaction_epoch: u64,
    /// Latest view, kept for the Sequencer's reorder, applying, and
    /// throughput counters.
    pub(super) last_view: SequencerView,
}

impl ReactorProgress {
    pub(super) fn new(
        frontiers: BlockSyncFrontiers,
        best_header_tip: (block::Height, block::Hash),
        initial_view: SequencerView,
    ) -> Self {
        Self {
            finalized_height: frontiers.finalized_height,
            verified_block_tip: frontiers.verified_block_tip,
            verified_block_hash: frontiers.verified_block_hash,
            servable_high: frontiers.verified_block_tip,
            servable_hash: frontiers.verified_block_hash,
            request_floor: frontiers.verified_block_tip,
            best_header_tip: best_header_tip.0,
            best_header_hash: best_header_tip.1,
            last_reset_epoch: 0,
            last_reaction_epoch: 0,
            last_view: initial_view,
        }
    }

    pub(super) fn observe_header_tip(&mut self, height: block::Height, hash: block::Hash) {
        self.best_header_tip = height;
        self.best_header_hash = hash;
    }

    /// Fold `frontiers.finalized_height` forward. Returns `false` when the
    /// update is behind the verified mirror and must not reach the Sequencer.
    #[must_use = "a stale frontier update must be dropped"]
    pub(super) fn observe_frontiers(&mut self, frontiers: BlockSyncFrontiers) -> bool {
        self.finalized_height = self.finalized_height.max(frontiers.finalized_height);
        frontiers.verified_block_tip >= self.verified_block_tip
    }

    fn observe_view(&mut self, view: SequencerView) {
        self.last_view = view;
        self.finalized_height = self.finalized_height.max(view.finalized);
        self.verified_block_tip = view.verified_tip;
        self.request_floor = view.download_floor;
        self.verified_block_hash = view.verified_hash;
        self.servable_high = view.verified_tip;
        self.servable_hash = view.verified_hash;
    }

    /// The height whose expired claims the floor watchdog cancels.
    pub(super) fn floor_watchdog_height(&self) -> Option<block::Height> {
        next_height(self.request_floor)
    }

    /// The status this node advertises before body retention applies.
    pub(super) fn local_status(&self, config: &ZakuraBlockSyncConfig) -> BlockSyncStatus {
        BlockSyncStatus {
            servable_low: block::Height::MIN,
            servable_high: self.servable_high,
            tip_hash: self.servable_hash,
            max_blocks_per_response: config.advertised_max_blocks_per_response(),
            max_inflight_requests: config.advertised_max_inflight_requests(),
            max_response_bytes: config.advertised_max_response_bytes(),
        }
    }

    /// Header tip minus verified body tip.
    pub(super) fn body_lag(&self) -> u32 {
        self.best_header_tip
            .0
            .saturating_sub(self.verified_block_tip.0)
    }
}

/// The reaction a Sequencer view asks for.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum ViewReaction {
    /// Only the mirrors changed.
    ObserveOnly,
    /// Run pruning, status refresh, and a refill query, forced after a reset.
    React { reset: bool, tip_advanced: bool },
}

// ---- empty-state bootstrap ----

/// Holds the first body query while an empty node pages in far-ahead headers.
#[derive(Debug)]
pub(super) struct BootstrapGate {
    /// True once this process releases the startup wait or observes body progress.
    body_sync_started: bool,
    /// Body-query deadline set by the first far-ahead header bootstrap update.
    header_quiet_until: Option<time::Instant>,
}

impl BootstrapGate {
    pub(super) fn new(body_sync_started: bool, header_quiet_until: Option<time::Instant>) -> Self {
        Self {
            body_sync_started,
            header_quiet_until,
        }
    }

    pub(super) fn header_quiet_until(&self) -> Option<time::Instant> {
        self.header_quiet_until
    }

    pub(super) fn is_header_quiet(&self) -> bool {
        self.header_quiet_until.is_some()
    }

    /// Hold body queries until `until` unless body sync already started or a
    /// quiet period is already running.
    pub(super) fn hold_for_headers(&mut self, until: time::Instant) {
        if !self.body_sync_started {
            self.header_quiet_until.get_or_insert(until);
        }
    }

    /// Release the quiet period for good.
    pub(super) fn start_body_sync(&mut self) {
        self.body_sync_started = true;
        self.header_quiet_until = None;
    }
}

// ---- committed authority ----

/// The verified and finalized frontiers a committed snapshot carries.
pub(super) fn block_sync_frontiers(snapshot: &EngineSnapshot) -> BlockSyncFrontiers {
    BlockSyncFrontiers {
        finalized_height: snapshot.frontiers.finalized.height,
        verified_block_tip: snapshot.frontiers.verified_best.height,
        verified_block_hash: snapshot.frontiers.verified_best.hash,
    }
}

/// Whether an empty node must wait for far-ahead headers before its first body query.
pub(super) fn empty_state_header_quiet_required(snapshot: &EngineSnapshot) -> bool {
    snapshot.frontiers.verified_best.height == block::Height(0)
        && snapshot
            .frontiers
            .header_best
            .height
            .0
            .saturating_sub(snapshot.frontiers.verified_best.height.0)
            >= EMPTY_STATE_HEADER_QUIET_MIN_LAG
}

/// The authority and header hash of `view`'s body-unavailable alarm, if alarmed.
fn alarmed_header_authority(
    view: &CommittedHeaderChainView,
) -> Option<(BodyWorkAuthority, block::Hash)> {
    view.alarms
        .header_best_body_unavailable
        .filter(|summary| summary.alarmed)
        .map(|_| {
            (
                BodyWorkAuthority::for_view(view),
                view.frontiers.header_best.hash,
            )
        })
}

/// How a committed view changes body-work authority.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum AuthorityUpdate {
    /// A new body-work epoch: the Sequencer resets work to `frontiers`.
    EpochChanged {
        authority: BodyWorkAuthority,
        frontiers: BlockSyncFrontiers,
    },
    /// The same epoch under a refreshed authority: queued work moves to it.
    Refreshed(BodyWorkAuthority),
}

/// How a committed view moves the verified frontiers.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum FrontierUpdate {
    Unchanged,
    /// The first committed view resets the Sequencer to its frontiers.
    Initial(BlockSyncFrontiers),
    /// Same-epoch verified or finalized progress.
    Advanced(BlockSyncFrontiers),
}

/// How a committed view changes queued body-size hints.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum HintUpdate {
    Unchanged,
    /// Apply the view's hint batches newer than `after_revision` to queued work.
    Apply {
        after_revision: u64,
    },
    /// The view's batches miss a revision: refresh the queued window from state
    /// at hint `revision`.
    Restart {
        revision: u64,
    },
}

/// What [`ReactorState::observe_committed_view`] asks IO to do, in field order.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[must_use = "a committed view's operations must run"]
pub(super) struct CommittedViewUpdate {
    /// The previous view's alarm that this view clears.
    pub(super) alarm_cleared: Option<(BodyWorkAuthority, block::Hash)>,
    /// The new durable state version, when it changed.
    pub(super) state_version: Option<StateVersion>,
    pub(super) authority: Option<AuthorityUpdate>,
    pub(super) frontiers: FrontierUpdate,
    pub(super) hints: HintUpdate,
    header_changed: bool,
    /// The hint change retired a pending query whose sizes it corrects.
    refreshed_pending_query: bool,
}

impl CommittedViewUpdate {
    /// Whether the view needs a forced refill, once IO applied [`Self::hints`].
    pub(super) fn forces_refill(&self, hint_refresh_active: bool) -> bool {
        self.header_changed
            || self.authority.is_some()
            || self.refreshed_pending_query
            || (self.hints != HintUpdate::Unchanged && hint_refresh_active)
    }
}

impl ReactorState {
    /// Adopt `view` as the committed view.
    ///
    /// The state retires the pending query when authority or hints change,
    /// mirrors a changed header tip, holds an empty node for far-ahead headers
    /// until `now` plus the quiet period, and drops the accepted
    /// body-availability requests of the previous view.
    pub(super) fn observe_committed_view(
        &mut self,
        view: CommittedHeaderChainView,
        now: time::Instant,
    ) -> CommittedViewUpdate {
        let previous = self.committed_view.take();
        let previous_scope = previous.as_ref().map(BodyWorkAuthority::for_view);
        let body_work_epoch_changed = previous
            .as_ref()
            .is_some_and(|old| old.body_work_epoch != view.body_work_epoch);
        let frontiers = block_sync_frontiers(&view);
        let header_best = view.frontiers.header_best;
        self.alarm_requests = AlarmRequests::default();

        let current_alarm_hash = alarmed_header_authority(&view).map(|(_, hash)| hash);
        let alarm_cleared = previous
            .as_ref()
            .and_then(alarmed_header_authority)
            .filter(|(_, hash)| Some(*hash) != current_alarm_hash);
        let state_version = (previous.as_ref().map(|old| old.state_version)
            != Some(view.state_version))
        .then_some(view.state_version);

        let current_scope = BodyWorkAuthority::for_view(&view);
        let authority = (previous_scope != Some(current_scope)).then(|| {
            self.queries.clear();
            if body_work_epoch_changed {
                AuthorityUpdate::EpochChanged {
                    authority: current_scope,
                    frontiers,
                }
            } else {
                AuthorityUpdate::Refreshed(current_scope)
            }
        });

        let header_changed = previous
            .as_ref()
            .is_none_or(|old| old.frontiers.header_best != header_best);
        if header_changed {
            self.progress
                .observe_header_tip(header_best.height, header_best.hash);
            if empty_state_header_quiet_required(&view) {
                self.bootstrap
                    .hold_for_headers(now + EMPTY_STATE_HEADER_QUIET_PERIOD);
            }
        }

        let frontier_update = match previous.as_ref() {
            None => FrontierUpdate::Initial(frontiers),
            Some(_) if body_work_epoch_changed => FrontierUpdate::Unchanged,
            Some(old)
                if old.frontiers.verified_best != view.frontiers.verified_best
                    || old.frontiers.finalized != view.frontiers.finalized =>
            {
                debug_assert!(
                    view.frontiers.verified_best.height >= old.frontiers.verified_best.height,
                    "same-epoch verified authority cannot retreat"
                );
                debug_assert!(
                    view.frontiers.verified_best.height != old.frontiers.verified_best.height
                        || view.frontiers.verified_best.hash == old.frontiers.verified_best.hash,
                    "same-epoch verified authority cannot replace a hash at the same height"
                );
                FrontierUpdate::Advanced(frontiers)
            }
            Some(_) => FrontierUpdate::Unchanged,
        };

        if body_work_epoch_changed {
            self.queries.end_hint_refresh();
        }
        let old_hint_revision = previous
            .as_ref()
            .map(|old| old.body_size_hint_revision)
            .filter(|old| *old != view.body_size_hint_revision);
        let refreshed_pending_query =
            old_hint_revision.is_some() && self.queries.pending().is_some();
        let hints = match old_hint_revision {
            None => HintUpdate::Unchanged,
            Some(old_revision) => {
                // An older query can carry sizes from before this correction.
                self.queries.clear();
                let first_revision = view
                    .body_size_hint_batches
                    .as_deref()
                    .unwrap_or_default()
                    .first()
                    .map(|batch| batch.revision);
                if first_revision.is_some_and(|first| first <= old_revision.saturating_add(1)) {
                    HintUpdate::Apply {
                        after_revision: old_revision,
                    }
                } else {
                    HintUpdate::Restart {
                        revision: view.body_size_hint_revision,
                    }
                }
            }
        };

        self.committed_view = Some(view);
        CommittedViewUpdate {
            alarm_cleared,
            state_version,
            authority,
            frontiers: frontier_update,
            hints,
            header_changed,
            refreshed_pending_query,
        }
    }
}

/// Decide whether a body completion from `source` for `owner` is current
/// under the committed view `current`. Returns the owner to complete and the
/// view's state version, or the stale reason.
pub(super) fn body_completion_authority(
    current: &CommittedHeaderChainView,
    source: SourceId,
    owner: &BodyWorkOwner,
) -> Result<(BodyWorkOwner, StateVersion), StaleReason> {
    let mut pending = PendingOwners::default();
    pending.insert(source, *owner);
    match Gate::check_body(current, &pending, source, owner) {
        BodyCompletionDecision::Current => Ok((*owner, current.state_version)),
        BodyCompletionDecision::Rebased(owner) => Ok((owner, current.state_version)),
        BodyCompletionDecision::Stale(reason) => Err(reason),
    }
}

// ---- body-availability requests ----

/// Deduplication key of one body-availability request: the committed state
/// version, the alarmed header hash, and the eligible supplier-set digest.
type AlarmKey = (StateVersion, block::Hash, [u8; 32]);

/// Body-availability requests the action channel accepted against the
/// current committed view. A new committed view drops both.
#[derive(Debug, Default)]
struct AlarmRequests {
    supplier_restart: Option<AlarmKey>,
    operator_retry: Option<AlarmKey>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum AlarmRequestKind {
    SupplierRestart,
    OperatorRetry,
}

/// A body-availability action that the reactor must offer to the action channel.
///
/// The request changes nothing until the reactor reports an accepted dispatch
/// to [`ReactorState::alarm_request_accepted`]. A refused request records
/// nothing, so the next trigger proposes it again.
#[derive(Debug)]
#[must_use = "an accepted request must reach ReactorState"]
pub(super) struct AlarmRequest {
    kind: AlarmRequestKind,
    key: AlarmKey,
    action: BlockSyncAction,
}

impl AlarmRequest {
    pub(super) fn action(&self) -> BlockSyncAction {
        self.action.clone()
    }
}

/// The 32-byte evidence digest of a body-availability request.
fn alarm_evidence(personal: &[u8; 16], header: Frontier, key: AlarmKey) -> EvidenceId {
    let mut hasher = blake2b_simd::Params::new()
        .hash_length(32)
        .personal(personal)
        .to_state();
    hasher.update(&key.0.get().to_le_bytes());
    hasher.update(&header.height.0.to_le_bytes());
    hasher.update(&header.hash.0);
    hasher.update(&key.2);
    EvidenceId::from_digest(
        hasher
            .finalize()
            .as_bytes()
            .try_into()
            .expect("the configured evidence digest is exactly 32 bytes"),
    )
}

impl ReactorState {
    /// The header frontier of the committed view's body-unavailable alarm.
    pub(super) fn alarmed_header(&self) -> Option<Frontier> {
        self.committed_view
            .as_ref()
            .filter(|view| {
                view.alarms
                    .header_best_body_unavailable
                    .is_some_and(|summary| summary.alarmed)
            })
            .map(|view| view.frontiers.header_best)
    }

    /// Propose restarting the alarmed body search because `eligible_sources`
    /// for the alarmed header holds a supplier the alarm has not tried.
    ///
    /// Returns `None` without an alarm, without a new supplier, or when the
    /// channel already accepted the same restart.
    pub(super) fn propose_supplier_restart(
        &self,
        eligible_sources: &BTreeSet<SourceId>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<AlarmRequest> {
        let view = self.committed_view.as_ref()?;
        let previous = view
            .alarms
            .header_best_body_unavailable
            .filter(|summary| summary.alarmed)?;
        let header = view.frontiers.header_best;
        let suppliers = u32::try_from(eligible_sources.len()).unwrap_or(u32::MAX);
        let supplier_set_digest = BodyUnavailableSummary::supplier_set_digest(eligible_sources);
        let has_new_supplier = suppliers > previous.suppliers
            || (suppliers == previous.suppliers
                && supplier_set_digest != previous.supplier_set_digest);
        let key = (view.state_version, header.hash, supplier_set_digest);
        if !has_new_supplier || self.alarm_requests.supplier_restart == Some(key) {
            return None;
        }

        let discovery = BodySupplierDiscovered {
            hash: header.hash,
            evidence: alarm_evidence(b"ZkBodyDiscover1_", header, key),
            availability: BodyUnavailableSummary {
                started_at: previous.started_at,
                attempts: previous.attempts,
                suppliers,
                supplier_set_digest,
                alarmed: true,
                next_probe_at: now,
            },
        };
        Some(AlarmRequest {
            kind: AlarmRequestKind::SupplierRestart,
            key,
            action: BlockSyncAction::RestartBodyAvailability {
                expected_version: view.state_version,
                discovery,
            },
        })
    }

    /// Propose an operator retry of the alarmed body `hash` across
    /// `eligible_sources`.
    ///
    /// Returns `None` unless `hash` is the alarmed header, a supplier is
    /// eligible, and the channel has not accepted the same retry.
    pub(super) fn propose_operator_retry(
        &self,
        hash: block::Hash,
        eligible_sources: &BTreeSet<SourceId>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<AlarmRequest> {
        let view = self.committed_view.as_ref()?;
        let header = self.alarmed_header().filter(|header| header.hash == hash)?;
        let suppliers = u32::try_from(eligible_sources.len()).unwrap_or(u32::MAX);
        if suppliers == 0 {
            return None;
        }
        let supplier_set_digest = BodyUnavailableSummary::supplier_set_digest(eligible_sources);
        let key = (view.state_version, header.hash, supplier_set_digest);
        if self.alarm_requests.operator_retry == Some(key) {
            return None;
        }

        let retry = OperatorBodyRetry {
            hash: header.hash,
            evidence: alarm_evidence(b"ZkBodyOperator1_", header, key),
            availability: BodyUnavailableSummary {
                started_at: now,
                attempts: 0,
                suppliers,
                supplier_set_digest,
                alarmed: false,
                next_probe_at: now,
            },
        };
        Some(AlarmRequest {
            kind: AlarmRequestKind::OperatorRetry,
            key,
            action: BlockSyncAction::RetryBodyAvailability {
                expected_version: view.state_version,
                retry,
            },
        })
    }

    /// The action channel accepted `request`: suppress the same request until
    /// the committed view changes.
    pub(super) fn alarm_request_accepted(&mut self, request: AlarmRequest) {
        match request.kind {
            AlarmRequestKind::SupplierRestart => {
                self.alarm_requests.supplier_restart = Some(request.key)
            }
            AlarmRequestKind::OperatorRetry => {
                self.alarm_requests.operator_retry = Some(request.key)
            }
        }
    }
}

// ---- status delivery ----

/// Status-delivery records of the admitted peers and the next refresh wake.
///
/// Its keys equal the reactor's admitted peer sessions: the reactor admits and
/// removes both together.
#[derive(Debug, Default)]
pub(super) struct StatusSchedule {
    deliveries: HashMap<ZakuraPeerId, StatusDelivery>,
    /// Earliest pending status delivery, rebuilt after delivery state changes.
    refresh_at: Option<Instant>,
}

impl StatusSchedule {
    pub(super) fn refresh_at(&self) -> Option<Instant> {
        self.refresh_at
    }

    /// Start a fresh delivery record for `peer`'s new session at `now`.
    pub(super) fn admit(&mut self, peer: &ZakuraPeerId, now: Instant) {
        self.deliveries
            .insert(peer.clone(), StatusDelivery::new(now));
    }

    pub(super) fn remove(&mut self, peer: &ZakuraPeerId) {
        self.deliveries.remove(peer);
    }

    /// `peer`'s delivery record, to record a send outcome.
    pub(super) fn delivery_mut(&mut self, peer: &ZakuraPeerId) -> Option<&mut StatusDelivery> {
        self.deliveries.get_mut(peer)
    }

    /// The peers in `peers` whose send of `status` is due at `now`.
    ///
    /// `peers` lists the live sessions in send order, each with whether the
    /// peer has supplied its own status.
    pub(super) fn due<'a>(
        &self,
        status: BlockSyncStatus,
        peers: impl IntoIterator<Item = (&'a ZakuraPeerId, bool)>,
        now: Instant,
    ) -> Vec<ZakuraPeerId> {
        peers
            .into_iter()
            .filter(|(peer, received_status)| {
                self.next_deadline(peer, status, *received_status)
                    .is_some_and(|deadline| deadline <= now)
            })
            .map(|(peer, _)| peer.clone())
            .collect()
    }

    /// Rebuild the refresh wake from the live sessions in `peers`.
    ///
    /// The same delivery rules choose both sends and wakes, so a full queue
    /// cannot lose its retry when another peer accepts the new range.
    pub(super) fn rearm<'a>(
        &mut self,
        status: BlockSyncStatus,
        peers: impl IntoIterator<Item = (&'a ZakuraPeerId, bool)>,
    ) {
        self.refresh_at = peers
            .into_iter()
            .filter_map(|(peer, received_status)| self.next_deadline(peer, status, received_status))
            .min();
    }

    fn next_deadline(
        &self,
        peer: &ZakuraPeerId,
        status: BlockSyncStatus,
        received_status: bool,
    ) -> Option<Instant> {
        self.deliveries
            .get(peer)?
            .next_deadline(status, received_status)
    }
}

// ---- action classes ----

/// How much action-channel capacity an action may use.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum ActionClass {
    /// Reactor control: needed-block queries and body-availability requests.
    /// It may use the protected control reserve.
    Required,
    /// Expendable peer reports. It must leave the control reserve free.
    Optional,
}

impl ActionClass {
    pub(super) fn of(action: &BlockSyncAction) -> Self {
        if matches!(action, BlockSyncAction::Misbehavior { .. }) {
            Self::Optional
        } else {
            Self::Required
        }
    }
}

/// Why the action channel refused an action.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ActionRefusal {
    /// An optional action found only the control reserve free.
    ControlReserve,
    /// The channel is full.
    Full,
    /// The action driver closed the channel.
    Closed,
}

// ---- peers and the floor watchdog ----

/// Admit a peer that already holds a slot. Otherwise admit it while its
/// direction is under the configured cap.
pub(super) fn admission_decision(
    limits: ServicePeerLimits,
    direction: ServicePeerDirection,
    already_admitted: bool,
    admitted_in_direction: usize,
) -> ServiceAdmissionDecision {
    if already_admitted {
        return ServiceAdmissionDecision::Admit;
    }
    let cap = match direction {
        ServicePeerDirection::Inbound => limits.max_inbound_peers,
        ServicePeerDirection::Outbound => limits.max_outbound_peers,
    };
    if admitted_in_direction >= cap {
        ServiceAdmissionDecision::RejectFull
    } else {
        ServiceAdmissionDecision::Admit
    }
}

/// Whether the floor watchdog may cancel `claim` at `now`.
pub(super) fn floor_claim_expired(claim: &OutstandingClaim, now: Instant) -> bool {
    claim.meta.deadline <= now
}

/// Whether a cancelled floor claim's peer should avoid the floor height.
///
/// Avoidance needs more than two servable peers and returned work that the
/// owner had started writing. Skipped frames and already-settled work do not
/// count against the peer.
pub(super) fn floor_cancel_avoids_peer(servable_peers: usize, outcome: &WorkReturnOutcome) -> bool {
    servable_peers > 2 && outcome.returned_count > 0 && !outcome.request_was_unwritten
}
