//! Pure sequencing policy for the block-sync commit pipeline.
//!
//! [`SequencerState`] owns the [`Sequencer`] (reorder buffer, applying queue, and
//! submission identities), the committed-frontier mirrors, the body-work
//! authority, the global state version, the reset and reaction epochs, body retry
//! episodes, submission-retry timing, and required evidence that the driver has
//! not yet accepted. It reads no clock, lock, atomic, or channel. Time enters as
//! explicit arguments, and retry jitter comes from the seed given at construction.
//!
//! Each transition returns [`SequencerOperation`]s in the order the task runs
//! them. [`SequencerIO`](super::SequencerIO) runs each operation against the
//! shared work queue, byte budget, peer registry, and action channel.
//!
//! Retained bodies are immutable payloads (`Arc` bytes or an `Arc` block), so the
//! `Sequencer` keeps them. IO keeps the input-queue accounting guard and releases
//! it before it hands a body to [`SequencerState::accept_body`]. An apply attempt
//! is identified by its [`BlockApplyToken`]. The `Sequencer` never reuses a token,
//! and a completion must match the token's owner, source, height, and hash before
//! it changes current work.

use std::collections::VecDeque;

use chrono::{DateTime, Utc};

use super::*;
use crate::zakura::header_sync::{
    BodyRetryEpisode, BodyRetryQueue, RetryUpdate, SeededRetryJitter,
};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use tests::{
    replay_sequencer_evidence_bytes, replay_sequencer_order_bytes, replay_sequencer_reset_bytes,
    SequencerReplaySummary,
};

/// Delay before retrying a verifier submission that could not enter the shared
/// action channel.
const SUBMISSION_RETRY_DELAY: Duration = Duration::from_millis(100);

/// One shared-resource effect of a sequencer transition.
///
/// IO runs the operations of one transition in order. Required evidence stays in
/// [`SequencerState`] until IO delivers it at its [`Self::DeliverEvidence`] point.
#[derive(Debug)]
pub(super) enum SequencerOperation {
    /// Reauthorize queued download work under compatible authority.
    RefreshWorkAuthority(zakura_header_chain::BodyWorkAuthority),
    /// Drop queued download work above the height and release its reservation bytes.
    ResetWorkAbove(block::Height),
    /// Advance the download-work floor and release its reservation bytes.
    AdvanceWorkFloor(block::Height),
    /// Reauthorize registry body-retry deferrals under compatible authority.
    RefreshBodyRetryScope(zakura_header_chain::BodyWorkAuthority),
    /// Retire registry body-retry deferrals outside the authority.
    RetainBodyRetryScope(zakura_header_chain::BodyWorkAuthority),
    /// Clear the registry body-retry deferral for one exact body.
    ClearBodyRetry {
        scope: zakura_header_chain::BodyWorkAuthority,
        hash: block::Hash,
    },
    /// Defer the listed suppliers for one exact body until the wall-clock deadline.
    DeferBodyRetry {
        sources: BTreeSet<zakura_header_chain::SourceId>,
        scope: zakura_header_chain::BodyWorkAuthority,
        hash: block::Hash,
        retry_at: DateTime<Utc>,
    },
    /// Wait for action capacity, then deliver the oldest pending required evidence.
    DeliverEvidence,
    /// Send a replaceable action under the bounded action-send timeout.
    SendAction(BlockSyncAction),
    /// Run one verifier submission pass.
    SubmitPending,
}

/// What [`SequencerState::accept_body`] did with a received body.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum BodyAcceptance {
    /// The body belongs to a retired body-work epoch.
    StaleScope,
    /// Retention is full, so the exact claim returns to pending download work.
    RetentionDeferred {
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
        hash: block::Hash,
        bytes: u64,
        verified_tip: block::Height,
    },
    /// The body entered the reorder buffer.
    Buffered,
    /// The pipeline already retains this height.
    Redundant,
}

impl BodyAcceptance {
    /// Stable trace label for the acceptance result.
    pub(super) fn label(&self) -> &'static str {
        match self {
            Self::StaleScope => "stale_scope",
            Self::RetentionDeferred { .. } => "retention_deferred",
            Self::Buffered => "buffered",
            Self::Redundant => "redundant",
        }
    }
}

/// A received body after IO released its input-queue accounting.
#[derive(Debug)]
pub(super) struct ReceivedBody {
    pub(super) owner: zakura_header_chain::BodyWorkOwner,
    pub(super) source: zakura_header_chain::SourceId,
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) previous_block_hash: block::Hash,
    pub(super) body: BufferedBlockBody,
    pub(super) bytes: u64,
    pub(super) peer: ZakuraPeerId,
}

/// How a chain-tip reset was classified.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum FrontierResetKind {
    /// The reset is verified growth under active successor work.
    Growth,
    /// The reset is a stale or coalesced update inside the contiguous floor.
    PreservedStale,
    /// The reset clears successor work and starts a new reset epoch.
    Destructive,
}

impl FrontierResetKind {
    /// Stable trace label for the classification.
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Growth => "growth",
            Self::PreservedStale => "preserved_stale",
            Self::Destructive => "destructive",
        }
    }
}

/// The facts behind one frontier-reset classification, captured before the reset.
#[derive(Copy, Clone, Debug)]
pub(super) struct FrontierResetClassified {
    pub(super) kind: FrontierResetKind,
    pub(super) frontiers: BlockSyncFrontiers,
    pub(super) preserve_active_successors: bool,
    pub(super) peer_has_successor_after: bool,
    pub(super) peer_outstanding_conflicts_at_tip: bool,
    pub(super) reset_tip_matches_local_work: bool,
    pub(super) previous_verified_tip: block::Height,
    pub(super) previous_download_floor: block::Height,
    pub(super) has_local_successor_after: bool,
}

/// The result of one control input.
#[derive(Debug, Default)]
pub(super) struct ControlOutputs {
    /// The reactor must rerun its query and scheduling reaction.
    pub(super) needs_reaction: bool,
    /// Shared-resource effects, in execution order.
    pub(super) operations: Vec<SequencerOperation>,
    /// The classification of a chain-tip reset input.
    pub(super) frontier_reset: Option<FrontierResetClassified>,
    /// Wire bytes of a newly committed body, for commit throughput.
    pub(super) committed_bytes: Option<u64>,
}

/// A submission pass over the heights that were submittable when it began.
#[derive(Debug)]
pub(super) struct SubmissionPass {
    heights: VecDeque<block::Height>,
}

/// Retry timing for verifier submissions that the action channel refused.
#[derive(Copy, Clone, Debug, Default)]
struct SubmissionRetry {
    /// When the next submission pass is due.
    at: Option<time::Instant>,
    /// When the first refusal of the current retry episode happened.
    started_at: Option<time::Instant>,
    /// Refusals in the current retry episode.
    attempt: u64,
}

/// A fixed wall-clock reading for retry-episode transitions.
struct WallClock(DateTime<Utc>);

impl zakura_header_chain::Clock for WallClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

/// Pure sequencing state of the commit pipeline; see the module docs.
#[derive(Debug)]
pub(super) struct SequencerState {
    sequencer: Sequencer,
    retained_body_limit: u64,
    /// Tracks the finalized height so the published view carries it forward; the
    /// reactor folds it into its `finalized_height` mirror with a `max`.
    finalized_height: block::Height,
    verified_block_hash: block::Hash,
    reset_epoch: u64,
    reaction_epoch: u64,
    current_scope: Option<zakura_header_chain::BodyWorkAuthority>,
    /// Global CAS coordinate for synchronous alarm writes, never body-work authority.
    current_state_version: Option<zakura_header_chain::StateVersion>,
    body_retries: BodyRetryQueue,
    retry_jitter: SeededRetryJitter,
    submission_retry: SubmissionRetry,
    /// Required actions the driver has not accepted yet, oldest first.
    pending_evidence: VecDeque<BlockSyncAction>,
}

impl SequencerState {
    pub(super) fn new(
        sequencer: Sequencer,
        frontiers: BlockSyncFrontiers,
        current_scope: Option<zakura_header_chain::BodyWorkAuthority>,
        retry_jitter: SeededRetryJitter,
    ) -> Self {
        Self {
            sequencer,
            retained_body_limit: ZakuraBlockSyncConfig::default()
                .effective_max_reorder_lookahead_bytes(),
            finalized_height: frontiers.finalized_height,
            verified_block_hash: frontiers.verified_block_hash,
            reset_epoch: 0,
            reaction_epoch: 0,
            current_scope,
            current_state_version: current_scope
                .map(|_| zakura_header_chain::StateVersion::default()),
            body_retries: BodyRetryQueue::default(),
            retry_jitter,
            submission_retry: SubmissionRetry::default(),
            pending_evidence: VecDeque::new(),
        }
    }

    pub(super) fn set_retained_body_limit(&mut self, limit: u64) {
        self.retained_body_limit = limit;
    }

    /// Seed the synchronous state-write coordinate from the startup snapshot.
    pub(super) fn seed_state_version(
        &mut self,
        state_version: Option<zakura_header_chain::StateVersion>,
    ) {
        self.current_state_version = state_version.or(self.current_state_version);
    }

    // ---- observations ----

    pub(super) fn sequencer(&self) -> &Sequencer {
        &self.sequencer
    }

    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn reset_epoch(&self) -> u64 {
        self.reset_epoch
    }

    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn reaction_epoch(&self) -> u64 {
        self.reaction_epoch
    }

    #[cfg(test)]
    pub(super) fn current_scope(&self) -> Option<zakura_header_chain::BodyWorkAuthority> {
        self.current_scope
    }

    #[cfg(test)]
    pub(super) fn current_state_version(&self) -> Option<zakura_header_chain::StateVersion> {
        self.current_state_version
    }

    #[cfg(test)]
    pub(super) fn body_retries(&self) -> &BodyRetryQueue {
        &self.body_retries
    }

    /// When the next submission pass is due after an action-channel refusal.
    pub(super) fn submission_retry_at(&self) -> Option<time::Instant> {
        self.submission_retry.at
    }

    pub(super) fn submission_retry_attempt(&self) -> u64 {
        self.submission_retry.attempt
    }

    /// The oldest required action the driver has not accepted.
    pub(super) fn pending_evidence(&self) -> Option<&BlockSyncAction> {
        self.pending_evidence.front()
    }

    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(super) fn pending_evidence_len(&self) -> usize {
        self.pending_evidence.len()
    }

    /// Build the progress view from this state and IO's observations.
    pub(super) fn view(
        &self,
        sequencer_input_decoded_attributed_memory_bytes: u64,
        committed_bytes_per_sec: u64,
        committed_blocks_per_sec: u64,
    ) -> SequencerView {
        let reorder_decoded_attributed_memory_bytes =
            self.sequencer.reorder_decoded_attributed_memory_bytes();
        let applying_decoded_attributed_memory_bytes =
            self.sequencer.applying_decoded_attributed_memory_bytes();
        SequencerView {
            verified_tip: self.sequencer.verified_tip(),
            verified_hash: self.verified_block_hash,
            download_floor: self.sequencer.floor(),
            finalized: self.finalized_height,
            reset_epoch: self.reset_epoch,
            reaction_epoch: self.reaction_epoch,
            reorder_len: self.sequencer.reorder_len() as u64,
            applying_len: self.sequencer.applying_len() as u64,
            reorder_buffered_bytes: self.sequencer.reorder_buffered_bytes(),
            applying_buffered_bytes: self.sequencer.applying_buffered_bytes(),
            sequencer_input_decoded_attributed_memory_bytes,
            reorder_decoded_attributed_memory_bytes,
            applying_decoded_attributed_memory_bytes,
            active_pipeline_decoded_attributed_memory_bytes:
                sequencer_input_decoded_attributed_memory_bytes
                    .saturating_add(reorder_decoded_attributed_memory_bytes)
                    .saturating_add(applying_decoded_attributed_memory_bytes),
            unsubmitted_applying_count: self.sequencer.unsubmitted_applying_count() as u64,
            in_flight_submission_count: self.sequencer.in_flight_submission_count() as u64,
            in_flight_submission_bytes: self.sequencer.in_flight_submission_bytes(),
            committed_bytes_per_sec,
            committed_blocks_per_sec,
        }
    }

    // ---- body acceptance ----

    /// Offer a received body to the reorder buffer, then drain the ready
    /// contiguous prefix into applying.
    pub(super) fn accept_body(&mut self, mut body: ReceivedBody) -> BodyAcceptance {
        let scope_is_current = self.current_scope.is_some_and(|current| {
            current.body_work_epoch == body.owner.authority().body_work_epoch
        });
        if !scope_is_current {
            return BodyAcceptance::StaleScope;
        }
        if !super::super::admission::retain_received_body(
            self.retained_body_limit,
            self.sequencer.verified_tip(),
            self.sequencer
                .reorder_buffered_bytes()
                .saturating_add(self.sequencer.applying_buffered_bytes()),
            u64::try_from(
                self.sequencer
                    .reorder_len()
                    .saturating_add(self.sequencer.applying_len()),
            )
            .unwrap_or(u64::MAX),
            body.height,
            body.bytes,
        ) && !self.sequencer.already_retains(body.height, body.hash)
        {
            return BodyAcceptance::RetentionDeferred {
                owner: body.owner,
                height: body.height,
                hash: body.hash,
                bytes: body.bytes,
                verified_tip: self.sequencer.verified_tip(),
            };
        }
        if let Some(current) = self.current_scope {
            body.owner.authority = current;
        }
        let acceptance = match self.sequencer.accept_buffered_body(
            body.owner,
            body.source,
            body.height,
            body.hash,
            body.previous_block_hash,
            body.body,
            body.bytes,
            body.peer,
        ) {
            AcceptOutcome::Buffered { .. } => BodyAcceptance::Buffered,
            AcceptOutcome::Redundant { .. } => BodyAcceptance::Redundant,
        };
        let _ = self.sequencer.drain_ready_into_applying();
        acceptance
    }

    // ---- control inputs ----

    /// Apply one control input at wall-clock time `wall_now`.
    ///
    /// Increments `reaction_epoch` exactly when the input needs the reactor's
    /// query and scheduling reaction. A pure body buffer, a state-version refresh,
    /// a same-epoch authority refresh, and a stale or duplicate apply completion
    /// need none.
    pub(super) fn on_control_input(
        &mut self,
        input: SequencerControlInput,
        wall_now: DateTime<Utc>,
    ) -> ControlOutputs {
        let mut outputs = ControlOutputs::default();
        outputs.needs_reaction = match input {
            SequencerControlInput::WorkAuthorityRefreshed { authority } => {
                self.refresh_work_authority(authority, &mut outputs.operations);
                false
            }
            SequencerControlInput::BodyWorkEpochChanged {
                authority,
                frontiers,
            } => {
                self.change_body_work_epoch(authority, frontiers, &mut outputs.operations);
                true
            }
            SequencerControlInput::StateVersionChanged(state_version) => {
                self.current_state_version = Some(state_version);
                false
            }
            SequencerControlInput::BodyAlarmCleared { scope, hash } => {
                self.body_retries
                    .remove(scope.header_generation, scope.branch, hash);
                outputs
                    .operations
                    .push(SequencerOperation::ClearBodyRetry { scope, hash });
                true
            }
            SequencerControlInput::FrontierAdvance {
                frontiers,
                release_applied,
            } => {
                self.advance_frontier(frontiers, release_applied, &mut outputs.operations);
                true
            }
            SequencerControlInput::FrontierReset {
                frontiers,
                preserve_active_successors,
                peer_has_successor_after,
                peer_outstanding_conflicts_at_tip,
            } => {
                outputs.frontier_reset = Some(self.reset_frontier(
                    frontiers,
                    preserve_active_successors,
                    peer_has_successor_after,
                    peer_outstanding_conflicts_at_tip,
                    &mut outputs.operations,
                ));
                true
            }
            SequencerControlInput::ApplyFinished {
                owner,
                source,
                token,
                height,
                hash,
                outcome,
                eligible_sources,
                persisted_availability,
                semantic_completion,
            } => {
                let completion = ApplyCompletion {
                    owner: *owner,
                    source,
                    token,
                    height,
                    hash,
                    outcome,
                    eligible_sources,
                    persisted_availability,
                    semantic_completion,
                };
                let (needs_reaction, allow_submit) =
                    self.finish_apply(completion, wall_now, &mut outputs);
                if allow_submit {
                    outputs.operations.push(SequencerOperation::SubmitPending);
                }
                needs_reaction
            }
        };
        if outputs.needs_reaction {
            self.reaction_epoch = self.reaction_epoch.saturating_add(1);
        }
        outputs
    }

    /// Reauthorize compatible body work under a same-epoch authority. Retained
    /// bodies stay, and the reset epoch does not change.
    fn refresh_work_authority(
        &mut self,
        authority: zakura_header_chain::BodyWorkAuthority,
        operations: &mut Vec<SequencerOperation>,
    ) {
        debug_assert!(self
            .current_scope
            .is_none_or(|old| { old.body_work_epoch == authority.body_work_epoch }));
        self.current_scope = Some(authority);
        operations.push(SequencerOperation::RefreshBodyRetryScope(authority));
        self.body_retries
            .refresh_scope(authority.header_generation, authority.branch);
    }

    /// Retire body work after the header-chain engine invalidates its lineage.
    fn change_body_work_epoch(
        &mut self,
        authority: zakura_header_chain::BodyWorkAuthority,
        frontiers: BlockSyncFrontiers,
        operations: &mut Vec<SequencerOperation>,
    ) {
        debug_assert!(self
            .current_scope
            .is_none_or(|old| { old.body_work_epoch != authority.body_work_epoch }));
        self.current_scope = Some(authority);
        operations.push(SequencerOperation::RetainBodyRetryScope(authority));
        self.body_retries = BodyRetryQueue::default();
        self.finalized_height = frontiers.finalized_height;
        self.verified_block_hash = frontiers.verified_block_hash;
        self.destructive_reset_to(frontiers.verified_block_tip, false, operations);
        operations.push(SequencerOperation::RefreshWorkAuthority(authority));
    }

    /// Reset the body pipeline and queued work above `tip` under one reset epoch.
    fn destructive_reset_to(
        &mut self,
        tip: block::Height,
        keep_submitted_applies: bool,
        operations: &mut Vec<SequencerOperation>,
    ) {
        let _ = self.sequencer.reset_to(tip, keep_submitted_applies);
        operations.push(SequencerOperation::ResetWorkAbove(tip));
        self.reset_epoch = self.reset_epoch.saturating_add(1);
    }

    /// Apply a verified-tip frontier advance: fold finalized height forward, drop
    /// stale updates, then advance the verified tip and floor and drain the newly
    /// contiguous prefix.
    fn advance_frontier(
        &mut self,
        frontiers: BlockSyncFrontiers,
        release_applied: bool,
        operations: &mut Vec<SequencerOperation>,
    ) {
        // Fold the finalized height forward unconditionally, then drop a stale
        // update. The verified tip is monotonic: an advance whose target is below
        // our verified tip must be a no-op, never a regression. Without this guard
        // the second growth-reset path (`< floor`, which permits `< verified_tip`)
        // would call `advance_verified_tip` with a lower tip and regress it.
        self.finalized_height = self.finalized_height.max(frontiers.finalized_height);
        if frontiers.verified_block_tip < self.sequencer.verified_tip() {
            return;
        }
        self.verified_block_hash = frontiers.verified_block_hash;
        let advance = self
            .sequencer
            .advance_verified_tip(frontiers.verified_block_tip, release_applied);
        if advance.changed {
            operations.push(SequencerOperation::AdvanceWorkFloor(
                frontiers.verified_block_tip,
            ));
            self.release_contiguous_blocks(operations);
        }
    }

    /// Classify a chain-tip reset as growth (treat as an advance) or a
    /// destructive reorg (pin tip/floor to the target, clear successor buffers, and
    /// bump the reset epoch). The peer-outstanding clauses of the decision arrive as
    /// the precomputed `peer_*` bools, since the reactor owns peer state.
    fn reset_frontier(
        &mut self,
        frontiers: BlockSyncFrontiers,
        preserve_active_successors: bool,
        peer_has_successor_after: bool,
        peer_outstanding_conflicts_at_tip: bool,
        operations: &mut Vec<SequencerOperation>,
    ) -> FrontierResetClassified {
        let reset_tip_matches_local_work = !self.reset_tip_conflicts_with_local_work(
            &frontiers,
            frontiers.verified_block_tip <= self.sequencer.floor(),
            peer_outstanding_conflicts_at_tip,
        );
        let classified = |kind, sequencer: &Sequencer| FrontierResetClassified {
            kind,
            frontiers,
            preserve_active_successors,
            peer_has_successor_after,
            peer_outstanding_conflicts_at_tip,
            reset_tip_matches_local_work,
            previous_verified_tip: sequencer.verified_tip(),
            previous_download_floor: sequencer.floor(),
            has_local_successor_after: next_height(frontiers.verified_block_tip)
                .is_some_and(|next| sequencer.has_buffered_at_or_above(next)),
        };

        // State can report a forward `Reset` while checkpoint commits advance
        // under already-submitted or still-downloading successor bodies. Treat
        // that as verified growth once it is inside our submitted/downloaded
        // floor, or when we already have successor work in flight. Keep fork
        // resets destructive when they are not anchored by active successor
        // work.
        if frontiers.verified_block_tip > self.sequencer.verified_tip()
            && (frontiers.verified_block_tip <= self.sequencer.floor()
                || self.has_active_successor_after(
                    frontiers.verified_block_tip,
                    peer_has_successor_after,
                ))
            && reset_tip_matches_local_work
        {
            let classification = classified(FrontierResetKind::Growth, &self.sequencer);
            // Growth-classified reset: treat it as a frontier advance, releasing
            // applied bodies.
            self.advance_frontier(frontiers, true, operations);
            return classification;
        }

        // A `Reset` can also be a stale or coalesced state update for a tip
        // already inside our contiguous submitted/downloaded body floor. Do not
        // destructively clear successor bodies in that case: a stale reset
        // snapshot can otherwise erase `applying`/covered state and re-request
        // the same bodies while their first apply is still in flight.
        if preserve_active_successors
            && frontiers.verified_block_tip < self.sequencer.floor()
            && reset_tip_matches_local_work
            && self
                .has_active_successor_after(frontiers.verified_block_tip, peer_has_successor_after)
            && self.active_successor_links_to_anchor(
                frontiers.verified_block_tip,
                frontiers.verified_block_hash,
            )
        {
            let classification = classified(FrontierResetKind::PreservedStale, &self.sequencer);
            self.advance_frontier(frontiers, true, operations);
            return classification;
        }

        let classification = classified(FrontierResetKind::Destructive, &self.sequencer);
        let remember_released_applies = frontiers.verified_block_tip > frontiers.finalized_height
            && frontiers.verified_block_tip <= self.sequencer.floor();

        self.finalized_height = frontiers.finalized_height;
        self.verified_block_hash = frontiers.verified_block_hash;

        self.destructive_reset_to(
            frontiers.verified_block_tip,
            remember_released_applies,
            operations,
        );
        classification
    }

    /// `reset_tip_conflicts_with_local_work`'s Sequencer-internal predicates,
    /// with the peer-outstanding clause supplied by the reactor.
    fn reset_tip_conflicts_with_local_work(
        &self,
        frontiers: &BlockSyncFrontiers,
        ignore_non_material_conflicts: bool,
        peer_outstanding_conflicts_at_tip: bool,
    ) -> bool {
        let height = frontiers.verified_block_tip;
        let hash = frontiers.verified_block_hash;

        if self
            .sequencer
            .reorder_hash(height)
            .is_some_and(|buffered_hash| buffered_hash != hash)
        {
            return true;
        }
        if self
            .sequencer
            .applying_hash(height)
            .is_some_and(|applying_hash| applying_hash != hash)
        {
            return true;
        }
        if !ignore_non_material_conflicts
            && self.sequencer.submitted_has_only_other_hashes(height, hash)
        {
            return true;
        }
        if !ignore_non_material_conflicts && peer_outstanding_conflicts_at_tip {
            return true;
        }
        false
    }

    fn has_active_successor_after(
        &self,
        height: block::Height,
        peer_has_successor_after: bool,
    ) -> bool {
        let Some(next) = next_height(height) else {
            return false;
        };

        self.sequencer.has_buffered_at_or_above(next) || peer_has_successor_after
    }

    fn active_successor_links_to_anchor(
        &self,
        height: block::Height,
        anchor_hash: block::Hash,
    ) -> bool {
        let Some(next) = next_height(height) else {
            return true;
        };

        direct_successor_links_to_anchor(
            self.sequencer.applying_previous_block_hash(next),
            anchor_hash,
        )
    }

    // ---- apply completion ----

    /// Handle a verifier apply completion.
    /// Release the verifier slot.
    /// After a rejection, move the floor below the bad block so peers can request its range again.
    /// The authoritative snapshot watch supplies committed frontier changes.
    ///
    /// Returns whether the completion needs a reactor reaction and whether it
    /// may refill free submission slots.
    fn finish_apply(
        &mut self,
        completion: ApplyCompletion,
        wall_now: DateTime<Utc>,
        outputs: &mut ControlOutputs,
    ) -> (bool, bool) {
        let ApplyCompletion {
            owner,
            source,
            token,
            height,
            hash,
            mut outcome,
            mut eligible_sources,
            persisted_availability,
            semantic_completion,
        } = completion;
        let operations = &mut outputs.operations;
        let result = outcome.result();
        // Frontier updates can remove applying entries before their completions.
        // Every committed exit below retires the exact submission, so a repeated
        // completion cannot count its cached wire size again.
        #[cfg(feature = "sync-metrics")]
        if matches!(result, BlockApplyResult::Committed) {
            outputs.committed_bytes = self
                .sequencer
                .submission_bytes(owner, source, token, height, hash);
        }
        // A stale completion (no live applying entry, or token/hash mismatch)
        // releases only its exact token-aware in-flight-submission charge and
        // returns; there is no query/schedule tail here, so it needs no reaction.
        let Some((applying_owner, applying_source, applying_token, applying_hash)) =
            self.sequencer.applying_identity(height)
        else {
            let released = self
                .sequencer
                .finish_submission(owner, source, token, height, hash);
            let verified = matches!(
                outcome.verification(),
                zakura_header_chain::BodyVerificationOutcome::Verified(_)
            );
            return (false, released && verified && semantic_completion.is_some());
        };
        if applying_owner != owner
            || applying_source != source
            || applying_hash != hash
            || applying_token != token
        {
            let released = self
                .sequencer
                .finish_submission(owner, source, token, height, hash);
            // A scope transition can detach an older checkpoint submission and
            // install a newer submission for the same height. When the newer
            // request makes the old verifier call finish as a duplicate, the
            // exact old token frees a submission slot without removing the
            // current applying body. Refill that slot or the checkpoint window
            // can shrink below the range needed to resolve the next checkpoint.
            return (false, released);
        }
        let Some((semantic_owner, semantic_state_version)) = semantic_completion else {
            let _ = self.sequencer.remove_applying(height);
            self.sequencer
                .finish_submission(owner, source, token, height, hash);
            // Removing this submitted body can leave the decoded-cap window with
            // nothing submitted, so wake its pending work (see
            // `Sequencer::submission_admitted`). With nothing pending, a stale
            // completion still schedules nothing.
            return (
                false,
                self.sequencer.submission_window_idle_with_pending_work(),
            );
        };
        let attribution_matches = outcome
            .attributed_source()
            .is_none_or(|attributed| attributed == source);
        if attribution_matches {
            if let zakura_header_chain::BodyVerificationOutcome::ConsensusInvalid(invalid) =
                outcome.verification()
            {
                self.require_evidence(
                    BlockSyncAction::RecordBodyInvalid {
                        expected_version: semantic_state_version,
                        invalid: invalid.clone(),
                    },
                    operations,
                );
            }
        }
        self.record_body_retry(
            semantic_owner,
            source,
            zakura_header_chain::Frontier::new(height, hash),
            &mut outcome,
            &mut eligible_sources,
            persisted_availability,
            wall_now,
            operations,
        );
        if let zakura_header_chain::BodyVerificationOutcome::Retryable(failure) =
            outcome.verification()
        {
            self.require_evidence(
                BlockSyncAction::RecordBodyUnavailable {
                    expected_version: semantic_state_version,
                    failure: *failure,
                },
                operations,
            );
        }
        if matches!(result, BlockApplyResult::Duplicate) && self.sequencer.verified_tip() < height {
            // Keep a duplicate attached until the committed snapshot includes its height.
            // The driver already released its decoded copy.
            // Release the token-aware decode-window charge now.
            self.sequencer
                .finish_attached_submission(owner, source, token, height, hash);
            return (false, true);
        }
        let applying = self
            .sequencer
            .remove_applying(height)
            .expect("applying entry exists because it was just checked");

        // A `Committed` result is a body that newly extended the chain; count it
        // toward commit throughput (the apply rate the download path is racing).
        if !cfg!(feature = "sync-metrics") && matches!(result, BlockApplyResult::Committed) {
            outputs.committed_bytes = Some(applying.bytes);
        }
        self.sequencer
            .finish_submission(owner, source, token, height, hash);
        match result {
            BlockApplyResult::Committed | BlockApplyResult::Duplicate => {}
            BlockApplyResult::Rejected
            | BlockApplyResult::Unavailable
            | BlockApplyResult::TimedOut
                if height > self.sequencer.verified_tip() =>
            {
                // Drop the failed body and every successor (in applying and
                // reorder), roll the floor back below it, and drop the WorkQueue
                // entries above the rolled-back floor so the heights are
                // re-requestable (the reactor's `query_needed_blocks` re-fills).
                let _ = self.sequencer.release_applying_blocks_from(height);
                self.sequencer.reset_floor_below(height);
                operations.push(SequencerOperation::ResetWorkAbove(self.sequencer.floor()));
                let _ = self.sequencer.drop_reorder_from(height);
                // A `Rejected` result identifies a bad body from a peer.
                // Attribute it to the delivering peer.
                // Do not score local `Unavailable` or `TimedOut` failures.
                if matches!(result, BlockApplyResult::Rejected) && attribution_matches {
                    let reason = match outcome.verification() {
                        zakura_header_chain::BodyVerificationOutcome::PayloadMismatch(mismatch) => {
                            BlockSyncMisbehavior::BodyPayloadMismatch(*mismatch)
                        }
                        zakura_header_chain::BodyVerificationOutcome::ConsensusInvalid(invalid) => {
                            BlockSyncMisbehavior::ConsensusBodyInvalid(invalid.clone())
                        }
                        zakura_header_chain::BodyVerificationOutcome::Verified(_)
                        | zakura_header_chain::BodyVerificationOutcome::Retryable(_) => {
                            unreachable!("only rejected outcomes reach peer scoring")
                        }
                    };
                    let disconnects = reason.proves_tampered_body();
                    let action = BlockSyncAction::Misbehavior {
                        peer: applying.source_peer.clone(),
                        reason,
                    };
                    // The driver disconnects a proven tampering supplier, so that
                    // record must not be dropped when the action channel is busy.
                    if disconnects {
                        self.require_evidence(action, operations);
                    } else {
                        operations.push(SequencerOperation::SendAction(action));
                    }
                }
            }
            BlockApplyResult::Rejected
            | BlockApplyResult::Unavailable
            | BlockApplyResult::TimedOut => {}
        }
        self.release_contiguous_blocks(operations);
        (true, true)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_body_retry(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        source: zakura_header_chain::SourceId,
        header: zakura_header_chain::Frontier,
        outcome: &mut BlockApplyOutcome,
        eligible_sources: &mut BTreeSet<zakura_header_chain::SourceId>,
        persisted_availability: Option<zakura_header_chain::BodyUnavailableSummary>,
        wall_now: DateTime<Utc>,
        operations: &mut Vec<SequencerOperation>,
    ) {
        let hash = header.hash;
        let Some(failure) = outcome.retryable_mut() else {
            self.body_retries
                .remove(owner.header_generation, owner.branch, hash);
            operations.push(SequencerOperation::ClearBodyRetry {
                scope: owner.authority(),
                hash,
            });
            return;
        };
        let clock = WallClock(wall_now);
        eligible_sources.insert(source);
        if self
            .body_retries
            .get_mut(owner.header_generation, owner.branch, hash)
            .is_none()
        {
            let episode = persisted_availability
                .filter(|summary| summary.alarmed)
                .map(|summary| {
                    BodyRetryEpisode::restore(
                        owner.branch,
                        owner.header_generation,
                        header,
                        eligible_sources.clone(),
                        summary,
                    )
                })
                .unwrap_or_else(|| {
                    BodyRetryEpisode::new(
                        owner.branch,
                        owner.header_generation,
                        header,
                        eligible_sources.clone(),
                        &clock,
                    )
                });
            self.body_retries.insert(episode);
        }
        let episode = self
            .body_retries
            .get_mut(owner.header_generation, owner.branch, hash)
            .expect("the exact retry episode exists because it was inserted above");
        episode.refresh_suppliers(eligible_sources.clone());
        let update = episode.record_failure(source, &clock, &self.retry_jitter);
        let deferred_sources = if episode.alarmed {
            eligible_sources.clone()
        } else {
            episode.tried_suppliers.clone()
        };
        let retry_at = match update {
            RetryUpdate::TooEarly => episode.next_probe_at,
            RetryUpdate::RetryAt(retry_at) | RetryUpdate::ProbeAt(retry_at) => retry_at,
            RetryUpdate::Alarmed { probe_at } => probe_at,
        };
        failure.availability = episode.summary();
        if let Some(persisted) = persisted_availability.filter(|summary| summary.alarmed) {
            if persisted.suppliers > failure.availability.suppliers {
                failure.availability.suppliers = persisted.suppliers;
                failure.availability.supplier_set_digest = persisted.supplier_set_digest;
            }
        }
        operations.push(SequencerOperation::DeferBodyRetry {
            sources: deferred_sources,
            scope: owner.authority(),
            hash,
            retry_at,
        });
    }

    /// Drain the contiguous reorder prefix into applying, then submit it.
    fn release_contiguous_blocks(&mut self, operations: &mut Vec<SequencerOperation>) {
        let _ = self.sequencer.drain_ready_into_applying();
        operations.push(SequencerOperation::SubmitPending);
    }

    // ---- required evidence ----

    /// Keep `action` pending until IO delivers it at the returned operation.
    fn require_evidence(
        &mut self,
        action: BlockSyncAction,
        operations: &mut Vec<SequencerOperation>,
    ) {
        self.pending_evidence.push_back(action);
        operations.push(SequencerOperation::DeliverEvidence);
    }

    /// Remove the oldest pending required action for delivery.
    ///
    /// IO calls this only while it holds a reserved action-channel permit, so the
    /// driver accepts the returned action.
    pub(super) fn take_pending_evidence(&mut self) -> Option<BlockSyncAction> {
        self.pending_evidence.pop_front()
    }

    // ---- verifier submission ----

    /// Start a submission pass at time `now`.
    ///
    /// Returns `None` when no applying body is submittable. With unsubmitted work
    /// still waiting in a retry episode, this schedules the next pass unless the
    /// action channel is closed.
    pub(super) fn begin_submission_pass(
        &mut self,
        now: time::Instant,
        actions_closed: bool,
    ) -> Option<SubmissionPass> {
        let submittable_heights = self.sequencer.submittable_heights();
        if submittable_heights.is_empty() {
            if self.sequencer.unsubmitted_applying_count() == 0 {
                self.submission_retry = SubmissionRetry::default();
            } else if self.submission_retry.started_at.is_some()
                && self.submission_retry.at.is_none()
                && !actions_closed
            {
                self.submission_retry.at = Some(now + SUBMISSION_RETRY_DELAY);
            }
            return None;
        }

        self.submission_retry.at = None;
        Some(SubmissionPass {
            heights: submittable_heights.into(),
        })
    }

    /// Assign a token to the next eligible height of `pass` and return its
    /// dispatch item. Returns `None` when the pass ends.
    pub(super) fn next_submission(&mut self, pass: &mut SubmissionPass) -> Option<SubmitItem> {
        while let Some(height) = pass.heights.pop_front() {
            // Each decode replaces an estimate with the measured size, so recheck
            // the cap before every submission (see `submittable_heights`).
            if !self.sequencer.submission_fits_decoded_window(height) {
                pass.heights.clear();
                return None;
            }
            if let Some(item) = self.sequencer.prepare_submit(height) {
                return Some(item);
            }
        }
        None
    }

    /// The driver accepted the submission of `height`.
    ///
    /// Returns the start of the retry episode this acceptance ends, if any.
    pub(super) fn submission_accepted(
        &mut self,
        height: block::Height,
        hash: block::Hash,
    ) -> Option<time::Instant> {
        let retry_started_at = self.submission_retry.started_at.take();
        if retry_started_at.is_some() {
            self.submission_retry.attempt = 0;
        }
        self.sequencer.record_submitted_apply(height, hash);
        retry_started_at
    }

    /// The action channel refused the submission of `height` at time `now`, which
    /// ends the pass.
    ///
    /// Rolls back the exact `token` and returns whether a retry was scheduled. A
    /// closed action channel schedules no retry.
    pub(super) fn submission_refused(
        &mut self,
        height: block::Height,
        token: BlockApplyToken,
        now: time::Instant,
        actions_closed: bool,
    ) -> bool {
        self.sequencer.unsubmit(height, token);
        if actions_closed {
            return false;
        }
        self.submission_retry.started_at.get_or_insert(now);
        self.submission_retry.attempt = self.submission_retry.attempt.saturating_add(1);
        self.submission_retry.at = Some(now + SUBMISSION_RETRY_DELAY);
        true
    }

    /// The submission retry deadline passed; the caller runs a submission pass.
    pub(super) fn submission_retry_due(&mut self) {
        self.submission_retry.at = None;
    }
}

/// A verifier apply completion with its exact submission identity.
#[derive(Debug)]
struct ApplyCompletion {
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    token: BlockApplyToken,
    height: block::Height,
    hash: block::Hash,
    outcome: BlockApplyOutcome,
    eligible_sources: BTreeSet<zakura_header_chain::SourceId>,
    persisted_availability: Option<zakura_header_chain::BodyUnavailableSummary>,
    semantic_completion: Option<(
        zakura_header_chain::BodyWorkOwner,
        zakura_header_chain::StateVersion,
    )>,
}

fn direct_successor_links_to_anchor(
    previous_block_hash: Option<block::Hash>,
    anchor_hash: block::Hash,
) -> bool {
    previous_block_hash.is_some_and(|hash| hash == anchor_hash)
}
