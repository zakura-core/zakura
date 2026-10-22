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

use std::{num::NonZeroU64, time::Instant};

use zakura_chain::block;
use zakura_header_chain::{BodyWorkAuthority, Frontier};
use zakura_rt::time;

use super::{
    BlockSyncAction, BlockSyncFrontiers, BlockSyncStatus, OutstandingClaim, SequencerView,
    ServiceAdmissionDecision, ServicePeerDirection, ServicePeerLimits, WorkReturnOutcome,
    ZakuraBlockSyncConfig, NEEDED_BLOCK_QUERY_RETRY_DELAY, NEEDED_BLOCK_REFILL_LIMIT,
};
use crate::zakura::block_sync::state::next_height;

#[cfg(test)]
mod tests;

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
}

impl ReactorState {
    pub(super) fn new(
        state_queries_enabled: bool,
        progress: ReactorProgress,
        bootstrap: BootstrapGate,
    ) -> Self {
        Self {
            state_queries_enabled,
            queries: QueryState::new(),
            progress,
            bootstrap,
            needed_heights: Vec::new(),
        }
    }

    /// Apply the query gates that need no shared-state reads.
    pub(super) fn open_query(&mut self, force: bool) -> QueryOpening {
        if !self.state_queries_enabled {
            return QueryOpening::Closed;
        }
        self.queries.open(QueryGate {
            force,
            header_quiet: self.bootstrap.is_header_quiet(),
            request_floor: self.progress.request_floor,
            best_header_tip: self.progress.best_header_tip,
        })
    }

    /// Plan the query that `opening` allows.
    ///
    /// `work` carries the shared-owner facts that [`QueryOpening::NeedsWork`]
    /// asks for. A hint refresh needs none.
    pub(super) fn propose_query(
        &self,
        force: bool,
        start: QueryStart,
        scope: Option<BodyWorkAuthority>,
    ) -> QueryPlan {
        self.queries.propose(QueryRequest {
            force,
            best_header_tip: self.progress.best_header_tip,
            best_header_hash: self.progress.best_header_hash,
            scope,
            start,
        })
    }

    /// Classify a needed-block reply against the pending query and the current
    /// body-work authority `current_scope`.
    ///
    /// An unrelated reply leaves the pending query in place. A matching reply
    /// always retires the pending query, even when its authority or anchor is
    /// stale.
    pub(super) fn complete_needed_query(
        &mut self,
        query_id: NonZeroU64,
        scope: BodyWorkAuthority,
        body_anchor: Frontier,
        current_scope: Option<BodyWorkAuthority>,
    ) -> NeededReply {
        let Some(query) = self.queries.complete(query_id, scope) else {
            return NeededReply::Stale;
        };
        if current_scope != Some(scope) {
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
    #[cfg(test)]
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
    hint_refresh: Option<HintRefresh>,
}

impl QueryState {
    pub(super) fn new() -> Self {
        Self {
            pending: None,
            next_id: NonZeroU64::new(1),
            retry_at: None,
            hint_refresh: None,
        }
    }

    pub(super) fn pending(&self) -> Option<PendingNeededQuery> {
        self.pending
    }

    #[cfg(test)]
    pub(super) fn next_id(&self) -> Option<NonZeroU64> {
        self.next_id
    }

    pub(super) fn retry_at(&self) -> Option<Instant> {
        self.retry_at
    }

    pub(super) fn hint_refresh(&self) -> Option<HintRefresh> {
        self.hint_refresh
    }

    /// Apply the retry, quiet-period, and floor gates.
    ///
    /// A forced query lifts the retry gate. A floor at the header tip retires
    /// the pending query. A hint refresh that the floor passed ends.
    pub(super) fn open(&mut self, gate: QueryGate) -> QueryOpening {
        if gate.force {
            self.retry_at = None;
        } else if self.retry_at.is_some() {
            return QueryOpening::Closed;
        }
        if gate.header_quiet {
            return QueryOpening::Closed;
        }
        if gate.request_floor >= gate.best_header_tip {
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
    pub(super) fn propose(&self, request: QueryRequest) -> QueryPlan {
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
                else {
                    return QueryPlan::Idle;
                };
                if !request.force && work.backlog >= work.low_water {
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
        self.next_id = query_id.get().checked_add(1).and_then(NonZeroU64::new);
    }

    /// The action channel refused `proposal`. The pending query and the next
    /// query ID stay; the query retries after [`NEEDED_BLOCK_QUERY_RETRY_DELAY`].
    pub(super) fn refused(&mut self, _proposal: QueryProposal, now: Instant) {
        self.schedule_retry(now);
    }

    /// Take the pending query when `(query_id, scope)` identifies it.
    pub(super) fn complete(
        &mut self,
        query_id: NonZeroU64,
        scope: BodyWorkAuthority,
    ) -> Option<PendingNeededQuery> {
        let query = self
            .pending
            .filter(|pending| pending.query_id == query_id && pending.scope == scope)?;
        self.clear();
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
        if self
            .pending
            .is_none_or(|pending| pending.query_id != query_id || pending.scope != scope)
        {
            return false;
        }
        self.pending = None;
        self.schedule_retry(now);
        true
    }

    /// Lift the retry gate when its deadline passes.
    pub(super) fn retry_elapsed(&mut self) {
        self.retry_at = None;
    }

    /// Retire the pending query and lift the retry gate.
    pub(super) fn clear(&mut self) {
        self.pending = None;
        self.retry_at = None;
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
