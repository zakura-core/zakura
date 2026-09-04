//! Stateful reference model for the GetBlocks serving contract.
//!
//! Generated steps use logical peers and queries. A step issues one to three
//! operations before the runtime settles, which makes lifecycle ordering races
//! reachable without giving up deterministic observations between steps. The
//! harness binds logical queries to request identities observed from the real
//! reactor, so the model does not duplicate the implementation's allocator.

mod harness;
mod model;
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::AddAssign,
};

use super::super::super::{BlockRangeRequestId, BlockSyncMisbehavior};
use crate::zakura::{ServicePeerDirection, ServicePeerSnapshot, ZakuraConnId};
use zakura_chain::block;

const LOGICAL_PEER_COUNT: u8 = 4;

/// One reproducible configuration and operation history for the serving model.
#[derive(Clone, Debug)]
pub(super) struct ServingCase {
    /// Seed used to build deterministic synthetic block bodies.
    pub(super) corpus_seed: u64,
    /// Highest block height available from the synthetic state driver.
    pub(super) tip: u32,
    /// Per-peer serving ledger limit.
    pub(super) max_inflight: u32,
    /// Local count limit applied to each request.
    pub(super) max_blocks: u32,
    /// Direction shared by the generated peer sessions in this case.
    pub(super) direction: ServicePeerDirection,
    /// Number of distinct peers the service may admit in that direction.
    pub(super) max_peers: usize,
    /// Response byte boundary selected relative to generated block sizes.
    pub(super) byte_cap: ByteCap,
    /// Focused and generated operations appended after the coverage prelude.
    pub(super) steps: Vec<ServingStep>,
}

impl ServingCase {
    /// Prepend a small successful exchange so every replay proves the full
    /// query-to-wire path before exploring adversarial history.
    fn steps_with_prelude(&self) -> Vec<ServingStep> {
        let mut steps = vec![
            ServingStep::single(ServingOp::Connect { peer: 0 }),
            ServingStep::single(ServingOp::Status {
                peer: 0,
                validity: StatusValidity::Valid,
            }),
            ServingStep::single(ServingOp::GetBlocks {
                peer: 0,
                start: 1,
                count: 1,
            }),
            ServingStep::single(ServingOp::Complete {
                query: QuerySelector::Live(0),
                kind: CompletionKind::Ready,
            }),
        ];
        steps.extend(self.steps.iter().cloned());
        steps
    }
}

/// Operations issued back-to-back before observing the settled serving state.
#[derive(Clone, Debug)]
pub(super) struct ServingStep {
    /// Operations submitted before the harness next settles and observes.
    pub(super) operations: Vec<ServingOp>,
}

impl ServingStep {
    /// Build a settled step containing one operation.
    fn single(operation: ServingOp) -> Self {
        Self {
            operations: vec![operation],
        }
    }

    /// Build a step whose operations are issued back-to-back before observation.
    fn unsettled(operations: impl IntoIterator<Item = ServingOp>) -> Self {
        Self {
            operations: operations.into_iter().collect(),
        }
    }
}

/// Response byte caps expressed relative to the synthetic corpus.
#[derive(Copy, Clone, Debug)]
pub(super) enum ByteCap {
    /// Use the production maximum.
    All,
    /// Fit one block but stop before the second.
    ExactlyFirst,
    /// End exactly after the first two blocks.
    ExactlyFirstTwo,
}

/// Total operation alphabet accepted by the reference model and real harness.
///
/// Operations against missing peers or queries are intentional no-ops, which
/// keeps shrinking and arbitrary history generation well defined.
#[derive(Clone, Debug)]
pub(super) enum ServingOp {
    /// Attach a new connection generation for one logical peer.
    Connect { peer: u8 },
    /// Remove either the current connection or a stale predecessor.
    Disconnect { peer: u8, which: DisconnectWhich },
    /// Cancel the current connection token to exercise routine teardown.
    Cancel { peer: u8 },
    /// Send a Status frame through the synthetic peer's real framed stream.
    Status { peer: u8, validity: StatusValidity },
    /// Send a GetBlocks frame through the synthetic peer's real framed stream.
    GetBlocks { peer: u8, start: u32, count: u32 },
    /// Return a selected state-driver completion to the real reactor.
    Complete {
        query: QuerySelector,
        kind: CompletionKind,
    },
}

/// Whether a disconnect names the live connection or an older generation.
#[derive(Copy, Clone, Debug)]
pub(super) enum DisconnectWhich {
    /// Remove the generation that currently owns the logical peer.
    Current,
    /// Re-deliver removal for an older connection generation.
    Stale,
}

/// Status classes that cross the real peer wire boundary.
#[derive(Copy, Clone, Debug)]
pub(super) enum StatusValidity {
    /// Advertise an internally consistent block range.
    Valid,
    /// Advertise a range whose high point precedes its low point.
    InvalidRange,
}

/// Select a completion by lifecycle class and ordinal within that class.
#[derive(Copy, Clone, Debug)]
pub(super) enum QuerySelector {
    /// Select an outstanding request owned by a live session.
    Live(u8),
    /// Select a request that already received a terminal completion.
    Retired(u8),
    /// Select a request whose owning session was replaced or disconnected.
    Orphaned(u8),
    /// Derive a request identity that no accepted request owns.
    Unknown(u8),
    /// Reuse a live request identity with a mismatched start height.
    MismatchedStart(u8),
    /// Reuse a live request identity and start height for another connected peer.
    MismatchedPeer(u8),
}

/// Driver completion shapes supplied to a selected query.
#[derive(Copy, Clone, Debug)]
pub(super) enum CompletionKind {
    /// Return every synthetic block available for the accepted range.
    Ready,
    /// Return one more contiguous block than the request permits.
    ReadyOverlong,
    /// Return a selected prefix, including the empty prefix.
    ReadyPrefix(u8),
    /// Return the first block followed by a later block, leaving a height gap.
    ReadyWithGap,
    /// Report that the state driver could not serve the range.
    FinishedUnavailable,
}

/// Stable model identity for one logical peer connection.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SessionKey {
    peer: u8,
    conn_id: ZakuraConnId,
}

/// Relevant node-to-peer frames after status advertisements are counted.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ServingFrame {
    Block(block::Hash),
    BlocksDone { start: block::Height, returned: u32 },
    RangeUnavailable { start: block::Height, count: u32 },
    Unexpected(u8),
}

/// Relevant reactor-to-driver actions observed for one settled step.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ServingAction {
    Query {
        request_id: BlockRangeRequestId,
        peer: u8,
        start: block::Height,
        count: u32,
    },
    Misbehavior {
        peer: u8,
        reason: BlockSyncMisbehavior,
    },
    Unexpected(String),
}

/// Complete externally visible state drained from the real implementation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ServingObservation {
    frames: BTreeMap<SessionKey, Vec<ServingFrame>>,
    actions: Vec<ServingAction>,
    snapshot: ServicePeerSnapshot,
    cancelled: BTreeMap<SessionKey, bool>,
    status_frames_by_session: BTreeMap<SessionKey, u64>,
}

/// Independent model prediction for one unsettled step.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExpectedObservation {
    frames: BTreeMap<SessionKey, Vec<ServingFrame>>,
    actions: Vec<ExpectedAction>,
    required_status_sessions: BTreeSet<SessionKey>,
}

impl ExpectedObservation {
    /// Preserve operation order while combining predictions for one step.
    fn append(&mut self, other: Self) {
        for (session, frames) in other.frames {
            self.frames.entry(session).or_default().extend(frames);
        }
        self.actions.extend(other.actions);
        self.required_status_sessions
            .extend(other.required_status_sessions);
    }
}

/// Model action without a request ID, which must come from production.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ExpectedAction {
    Query {
        peer: u8,
        start: block::Height,
        count: u32,
    },
    Misbehavior {
        peer: u8,
        reason: BlockSyncMisbehavior,
    },
}

/// Accepted model query waiting to be bound to a production request ID.
#[derive(Clone, Debug)]
struct PendingQuery {
    query_index: usize,
    session: SessionKey,
    peer: u8,
    start: block::Height,
    requested: u32,
}

/// Query metadata selected for a simulated driver completion.
#[derive(Clone, Debug)]
struct CompletionTarget {
    class: QueryClass,
    request_id: BlockRangeRequestId,
    session: SessionKey,
    peer: u8,
    start: block::Height,
    original_count: u32,
    requested: u32,
    query_index: Option<usize>,
}

/// Lifecycle classification for a completion target.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum QueryClass {
    Live,
    Retired,
    Orphaned,
    Unknown,
    MismatchedStart,
    MismatchedPeer,
}

/// Stable identifiers for the documented GetBlocks serving requirements.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum ServingRequirement {
    ReplacementCancelsPreviousSession,
    StaleDisconnectPreservesCurrentSession,
    MissingStatusIsRejectedAsSpam,
    PeerLedgersAreIndependentAndBounded,
    SaturatedLedgerRejectsWithoutStateQuery,
    AboveTipRequestIsUnavailableWithoutStateQuery,
    AcceptedQueryCountRespectsAllBounds,
    RequestIdsAreNonzeroAndUnique,
    ReadyResponseSendsLargestValidPrefixAndOneTerminal,
    InvalidCompletionHasNoServingEffect,
    RepeatedCompletionDoesNotReleaseLiveSlot,
    EndedSessionResponsesDoNotReachReplacement,
    SaturatedPeerDoesNotBlockOtherPeers,
    FramesAreAttributableToLiveRequestOwner,
    DelayedOlderConnectCannotReplaceNewerSession,
    PeerFramesWaitForReactorAdmission,
    SupersededRoutineRequestCannotReachReplacementSession,
    LiveUnavailableCompletionSendsTerminalAndReleasesSlot,
    InboundSessionsServeAndUseInboundCap,
}

impl ServingRequirement {
    /// Requirements reached by concrete operations in the serving model.
    pub(super) const OCCURRENCE: [Self; 14] = [
        Self::ReplacementCancelsPreviousSession,
        Self::StaleDisconnectPreservesCurrentSession,
        Self::MissingStatusIsRejectedAsSpam,
        Self::SaturatedLedgerRejectsWithoutStateQuery,
        Self::AboveTipRequestIsUnavailableWithoutStateQuery,
        Self::AcceptedQueryCountRespectsAllBounds,
        Self::RequestIdsAreNonzeroAndUnique,
        Self::ReadyResponseSendsLargestValidPrefixAndOneTerminal,
        Self::InvalidCompletionHasNoServingEffect,
        Self::RepeatedCompletionDoesNotReleaseLiveSlot,
        Self::EndedSessionResponsesDoNotReachReplacement,
        Self::SaturatedPeerDoesNotBlockOtherPeers,
        Self::LiveUnavailableCompletionSendsTerminalAndReleasesSlot,
        Self::InboundSessionsServeAndUseInboundCap,
    ];

    /// Requirements checked after every successfully compared model step.
    pub(super) const INVARIANTS: [Self; 2] = [
        Self::PeerLedgersAreIndependentAndBounded,
        Self::FramesAreAttributableToLiveRequestOwner,
    ];

    /// Requirements that need separately forced task orderings.
    pub(super) const REGRESSION_ONLY: [Self; 3] = [
        Self::DelayedOlderConnectCannotReplaceNewerSession,
        Self::PeerFramesWaitForReactorAdmission,
        Self::SupersededRoutineRequestCannotReachReplacementSession,
    ];

    /// Return the contract identifier printed in failures and coverage reports.
    pub(super) const fn id(self) -> &'static str {
        match self {
            Self::ReplacementCancelsPreviousSession => "GB-SM-01",
            Self::StaleDisconnectPreservesCurrentSession => "GB-SM-02",
            Self::MissingStatusIsRejectedAsSpam => "GB-SM-03",
            Self::PeerLedgersAreIndependentAndBounded => "GB-SM-04",
            Self::SaturatedLedgerRejectsWithoutStateQuery => "GB-SM-05",
            Self::AboveTipRequestIsUnavailableWithoutStateQuery => "GB-SM-06",
            Self::AcceptedQueryCountRespectsAllBounds => "GB-SM-07",
            Self::RequestIdsAreNonzeroAndUnique => "GB-SM-08",
            Self::ReadyResponseSendsLargestValidPrefixAndOneTerminal => "GB-SM-09",
            Self::InvalidCompletionHasNoServingEffect => "GB-SM-10",
            Self::RepeatedCompletionDoesNotReleaseLiveSlot => "GB-SM-11",
            Self::EndedSessionResponsesDoNotReachReplacement => "GB-SM-12",
            Self::SaturatedPeerDoesNotBlockOtherPeers => "GB-SM-13",
            Self::FramesAreAttributableToLiveRequestOwner => "GB-SM-14",
            Self::DelayedOlderConnectCannotReplaceNewerSession => "GB-SM-15",
            Self::PeerFramesWaitForReactorAdmission => "GB-SM-16",
            Self::SupersededRoutineRequestCannotReachReplacementSession => "GB-SM-17",
            Self::LiveUnavailableCompletionSendsTerminalAndReleasesSlot => "GB-SM-18",
            Self::InboundSessionsServeAndUseInboundCap => "GB-SM-19",
        }
    }
}

/// Exact preconditions observed by a successfully compared model step.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum ServingEvidence {
    ReplacementCancelled,
    ServiceStaleDisconnectIgnored,
    ReactorStaleDisconnectIgnored,
    MissingStatusRejected,
    SaturatedLedgerRejected,
    AboveTipRejected,
    WireCountBound,
    LocalCountBound,
    AvailableRangeBound,
    MultipleRequestIdsValidated,
    ReadyResponseCompleted,
    OverlongResponseStoppedAtRequestedCount,
    GenesisReadyResponseCompleted,
    ByteCapStoppedPrefix,
    NonContiguousResponseStopped,
    EmptyReadyResponseTerminated,
    NonEmptyReadyResponseTerminated,
    UnknownCompletionIgnored,
    RetiredCompletionIgnored,
    MismatchedStartCompletionIgnored,
    MismatchedPeerCompletionIgnored,
    OrphanedCompletionIgnored,
    OrphanedCompletionWithoutReplacementIgnored,
    RetiredCompletionWithLiveSiblingIgnored,
    OrphanedCompletionWithLiveReplacementIgnored,
    SaturatedPeerAllowedOtherPeerProgress,
    LiveUnavailableResponseTerminated,
    RequestAcceptedAfterLiveUnavailableCompletion,
    InboundServingCompleted,
    InboundCapRejected,
}

/// Evidence that generated replays reached the important contract classes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ServingCoverage {
    /// Preconditions whose complete model step matched production.
    verified_evidence: BTreeSet<ServingEvidence>,
    /// Successful steps on which the per-peer ledger invariant was checked.
    ledger_invariant_checks: u64,
    /// Successful steps on which response ownership was checked.
    frame_ownership_checks: u64,
    pub(super) cases: u64,
    pub(super) steps: u64,
    pub(super) operations: u64,
    pub(super) multi_operation_steps: u64,
    pub(super) connected_sessions: u64,
    pub(super) replacement_sessions: u64,
    pub(super) rejected_sessions: u64,
    pub(super) current_disconnects: u64,
    pub(super) stale_disconnects: u64,
    pub(super) token_cancellations: u64,
    pub(super) valid_statuses: u64,
    pub(super) invalid_statuses: u64,
    pub(super) accepted_requests: u64,
    pub(super) no_status_requests: u64,
    pub(super) cap_rejections: u64,
    pub(super) above_tip_rejections: u64,
    pub(super) live_completions: u64,
    pub(super) empty_ready_completions: u64,
    pub(super) short_ready_completions: u64,
    pub(super) retired_completions: u64,
    pub(super) orphaned_completions: u64,
    pub(super) unknown_completions: u64,
    pub(super) mismatched_completions: u64,
    pub(super) captured_request_ids: u64,
    pub(super) cross_peer_progress: u64,
    pub(super) byte_cap_stops: u64,
    pub(super) exact_cap_endings: u64,
    pub(super) blocks_observed: u64,
    pub(super) terminal_frames: u64,
    pub(super) status_frames: u64,
    pub(super) max_peer_ledger: usize,
}

impl AddAssign for ServingCoverage {
    fn add_assign(&mut self, other: Self) {
        self.verified_evidence.extend(other.verified_evidence);
        self.ledger_invariant_checks = self
            .ledger_invariant_checks
            .saturating_add(other.ledger_invariant_checks);
        self.frame_ownership_checks = self
            .frame_ownership_checks
            .saturating_add(other.frame_ownership_checks);
        self.cases = self.cases.saturating_add(other.cases);
        self.steps = self.steps.saturating_add(other.steps);
        self.operations = self.operations.saturating_add(other.operations);
        self.multi_operation_steps = self
            .multi_operation_steps
            .saturating_add(other.multi_operation_steps);
        self.connected_sessions = self
            .connected_sessions
            .saturating_add(other.connected_sessions);
        self.replacement_sessions = self
            .replacement_sessions
            .saturating_add(other.replacement_sessions);
        self.rejected_sessions = self
            .rejected_sessions
            .saturating_add(other.rejected_sessions);
        self.current_disconnects = self
            .current_disconnects
            .saturating_add(other.current_disconnects);
        self.stale_disconnects = self
            .stale_disconnects
            .saturating_add(other.stale_disconnects);
        self.token_cancellations = self
            .token_cancellations
            .saturating_add(other.token_cancellations);
        self.valid_statuses = self.valid_statuses.saturating_add(other.valid_statuses);
        self.invalid_statuses = self.invalid_statuses.saturating_add(other.invalid_statuses);
        self.accepted_requests = self
            .accepted_requests
            .saturating_add(other.accepted_requests);
        self.no_status_requests = self
            .no_status_requests
            .saturating_add(other.no_status_requests);
        self.cap_rejections = self.cap_rejections.saturating_add(other.cap_rejections);
        self.above_tip_rejections = self
            .above_tip_rejections
            .saturating_add(other.above_tip_rejections);
        self.live_completions = self.live_completions.saturating_add(other.live_completions);
        self.empty_ready_completions = self
            .empty_ready_completions
            .saturating_add(other.empty_ready_completions);
        self.short_ready_completions = self
            .short_ready_completions
            .saturating_add(other.short_ready_completions);
        self.retired_completions = self
            .retired_completions
            .saturating_add(other.retired_completions);
        self.orphaned_completions = self
            .orphaned_completions
            .saturating_add(other.orphaned_completions);
        self.unknown_completions = self
            .unknown_completions
            .saturating_add(other.unknown_completions);
        self.mismatched_completions = self
            .mismatched_completions
            .saturating_add(other.mismatched_completions);
        self.captured_request_ids = self
            .captured_request_ids
            .saturating_add(other.captured_request_ids);
        self.cross_peer_progress = self
            .cross_peer_progress
            .saturating_add(other.cross_peer_progress);
        self.byte_cap_stops = self.byte_cap_stops.saturating_add(other.byte_cap_stops);
        self.exact_cap_endings = self
            .exact_cap_endings
            .saturating_add(other.exact_cap_endings);
        self.blocks_observed = self.blocks_observed.saturating_add(other.blocks_observed);
        self.terminal_frames = self.terminal_frames.saturating_add(other.terminal_frames);
        self.status_frames = self.status_frames.saturating_add(other.status_frames);
        self.max_peer_ledger = self.max_peer_ledger.max(other.max_peer_ledger);
    }
}

impl ServingCoverage {
    /// Record evidence only after the corresponding model step matched production.
    fn commit_verified_step(&mut self, evidence: BTreeSet<ServingEvidence>) {
        self.verified_evidence.extend(evidence);
        self.ledger_invariant_checks = self.ledger_invariant_checks.saturating_add(1);
        self.frame_ownership_checks = self.frame_ownership_checks.saturating_add(1);
    }

    /// Return whether this replay exercised and checked one documented requirement.
    pub(super) fn requirement_is_covered(&self, requirement: ServingRequirement) -> bool {
        use ServingEvidence as Evidence;
        use ServingRequirement as Requirement;

        let has = |evidence| self.verified_evidence.contains(&evidence);
        match requirement {
            Requirement::ReplacementCancelsPreviousSession => has(Evidence::ReplacementCancelled),
            Requirement::StaleDisconnectPreservesCurrentSession => {
                has(Evidence::ServiceStaleDisconnectIgnored)
                    && has(Evidence::ReactorStaleDisconnectIgnored)
            }
            Requirement::MissingStatusIsRejectedAsSpam => has(Evidence::MissingStatusRejected),
            Requirement::PeerLedgersAreIndependentAndBounded => {
                self.steps > 0
                    && self.ledger_invariant_checks == self.steps
                    && has(Evidence::SaturatedLedgerRejected)
                    && has(Evidence::SaturatedPeerAllowedOtherPeerProgress)
            }
            Requirement::SaturatedLedgerRejectsWithoutStateQuery => {
                has(Evidence::SaturatedLedgerRejected)
            }
            Requirement::AboveTipRequestIsUnavailableWithoutStateQuery => {
                has(Evidence::AboveTipRejected)
            }
            Requirement::AcceptedQueryCountRespectsAllBounds => {
                has(Evidence::WireCountBound)
                    && has(Evidence::LocalCountBound)
                    && has(Evidence::AvailableRangeBound)
            }
            Requirement::RequestIdsAreNonzeroAndUnique => {
                has(Evidence::MultipleRequestIdsValidated)
            }
            Requirement::ReadyResponseSendsLargestValidPrefixAndOneTerminal => {
                has(Evidence::ReadyResponseCompleted)
                    && has(Evidence::OverlongResponseStoppedAtRequestedCount)
                    && has(Evidence::GenesisReadyResponseCompleted)
                    && has(Evidence::ByteCapStoppedPrefix)
                    && has(Evidence::NonContiguousResponseStopped)
                    && has(Evidence::EmptyReadyResponseTerminated)
                    && has(Evidence::NonEmptyReadyResponseTerminated)
            }
            Requirement::InvalidCompletionHasNoServingEffect => {
                has(Evidence::UnknownCompletionIgnored)
                    && has(Evidence::RetiredCompletionIgnored)
                    && has(Evidence::MismatchedStartCompletionIgnored)
                    && has(Evidence::MismatchedPeerCompletionIgnored)
                    && has(Evidence::OrphanedCompletionIgnored)
            }
            Requirement::RepeatedCompletionDoesNotReleaseLiveSlot => {
                has(Evidence::RetiredCompletionWithLiveSiblingIgnored)
            }
            Requirement::EndedSessionResponsesDoNotReachReplacement => {
                has(Evidence::OrphanedCompletionWithoutReplacementIgnored)
                    && has(Evidence::OrphanedCompletionWithLiveReplacementIgnored)
            }
            Requirement::SaturatedPeerDoesNotBlockOtherPeers => {
                has(Evidence::SaturatedPeerAllowedOtherPeerProgress)
            }
            Requirement::LiveUnavailableCompletionSendsTerminalAndReleasesSlot => {
                has(Evidence::LiveUnavailableResponseTerminated)
                    && has(Evidence::RequestAcceptedAfterLiveUnavailableCompletion)
            }
            Requirement::InboundSessionsServeAndUseInboundCap => {
                has(Evidence::InboundServingCompleted) && has(Evidence::InboundCapRejected)
            }
            Requirement::FramesAreAttributableToLiveRequestOwner => {
                self.steps > 0
                    && self.frame_ownership_checks == self.steps
                    && self.blocks_observed > 0
                    && self.terminal_frames > 0
            }
            Requirement::DelayedOlderConnectCannotReplaceNewerSession
            | Requirement::PeerFramesWaitForReactorAdmission
            | Requirement::SupersededRoutineRequestCannotReachReplacementSession => false,
        }
    }

    /// Return the named model requirements missing from this coverage summary.
    pub(super) fn missing_model_requirements(&self) -> Vec<ServingRequirement> {
        ServingRequirement::OCCURRENCE
            .into_iter()
            .chain(ServingRequirement::INVARIANTS)
            .filter(|requirement| !self.requirement_is_covered(*requirement))
            .collect()
    }

    /// Return successful invariant-check counts for the human-readable report.
    pub(super) fn invariant_check_counts(&self) -> (u64, u64) {
        (self.ledger_invariant_checks, self.frame_ownership_checks)
    }
}

impl fmt::Display for ServingCoverage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} cases, {} steps ({} multi-operation), {} operations, sessions admitted/replaced/rejected {}/{}/{}, disconnects current/stale/cancel {}/{}/{}, status valid/invalid {}/{}, {} accepted requests, {} no-status, {} cap-rejected, {} above-tip, completions live/retired/orphaned/unknown/mismatched {}/{}/{}/{}/{}, ready empty/short {}/{}, {} request IDs, {} cross-peer progress, byte-cap stops/exact endings {}/{}, {} blocks, {} terminals, max ledger {}",
            self.cases,
            self.steps,
            self.multi_operation_steps,
            self.operations,
            self.connected_sessions,
            self.replacement_sessions,
            self.rejected_sessions,
            self.current_disconnects,
            self.stale_disconnects,
            self.token_cancellations,
            self.valid_statuses,
            self.invalid_statuses,
            self.accepted_requests,
            self.no_status_requests,
            self.cap_rejections,
            self.above_tip_rejections,
            self.live_completions,
            self.retired_completions,
            self.orphaned_completions,
            self.unknown_completions,
            self.mismatched_completions,
            self.empty_ready_completions,
            self.short_ready_completions,
            self.captured_request_ids,
            self.cross_peer_progress,
            self.byte_cap_stops,
            self.exact_cap_endings,
            self.blocks_observed,
            self.terminal_frames,
            self.max_peer_ledger,
        )
    }
}

/// Replay one case against the real node path and its independent model.
pub(super) fn replay_serving_case(case: &ServingCase) -> Result<ServingCoverage, String> {
    harness::replay_serving_case(case)
}
