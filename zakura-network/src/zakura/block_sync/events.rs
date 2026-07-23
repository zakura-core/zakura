#[cfg(any(test, feature = "proptest-impl"))]
use super::state::BlockSyncFrontiers;
use super::{request::*, *};
use std::num::NonZeroU64;

/// Committed header metadata used by block sync to schedule and validate a body.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockSyncBlockMeta {
    /// Header-known block height whose body is missing.
    pub height: block::Height,
    /// Committed header hash expected from the downloaded body.
    pub hash: block::Hash,
    /// Advisory or confirmed body-size estimate for scheduling.
    pub size: BlockSizeEstimate,
}

/// Facts accepted by the block-sync scaffold and later reactor.
///
/// The inbound data flow is inverted: a peer's stream-6 frames are decoded and
/// the download logic runs in the per-peer pipe-routine
/// ([`PeerRoutine`](super::peer_routine)). Inbound messages no longer flow
/// through the reactor as a `WireMessage`; the routine forwards only shared
/// concerns to the reactor over [`RoutineToReactor`].
#[derive(Clone, Debug)]
pub enum BlockSyncEvent {
    /// A peer became available for stream-6 block sync.
    PeerConnected(BlockSyncPeerSession),
    /// A peer disconnected; all of its outstanding work is dropped.
    PeerDisconnected(ZakuraPeerId),
    /// An authenticated local operator requested a fresh retry of one persistent alarm.
    RetryBodyAvailability {
        /// Exact alarmed selected header; stale requests fail closed.
        hash: block::Hash,
    },
    /// Test-only direct header-target injection.
    #[cfg(any(test, feature = "proptest-impl"))]
    HeaderTipChanged {
        /// Current best header height.
        height: block::Height,
        /// Current best header hash.
        hash: block::Hash,
    },
    /// Test-only direct frontier injection.
    #[cfg(any(test, feature = "proptest-impl"))]
    StateFrontiersChanged(BlockSyncFrontiers),
    /// Test-only direct growth injection.
    #[cfg(any(test, feature = "proptest-impl"))]
    ChainTipGrow(BlockSyncFrontiers),
    /// Test-only direct reset injection.
    #[cfg(any(test, feature = "proptest-impl"))]
    ChainTipReset(BlockSyncFrontiers),
    /// Driver returned body-missing metadata bound to the exact queried snapshot.
    ScopedNeededBlocks {
        /// Reactor-local query identifier echoed by the driver.
        query_id: NonZeroU64,
        /// Durable generation and branch coordinates echoed from the query.
        scope: zakura_header_chain::WorkScope,
        /// Header-known bodies missing under `scope`.
        blocks: Vec<BlockSyncBlockMeta>,
    },
    /// Ownerless unit-test fixture for the pre-ownership scheduling surface.
    #[cfg(test)]
    NeededBlocks(Vec<BlockSyncBlockMeta>),
    /// Node wiring finished applying a submitted block body.
    BlockApplyFinished {
        /// Exact network request that owned the submission.
        owner: zakura_header_chain::WorkOwner,
        /// Authenticated body supplier.
        source: zakura_header_chain::SourceId,
        /// Submission token from the matching [`BlockSyncAction::SubmitBlock`].
        token: BlockApplyToken,
        /// Submitted block height.
        height: block::Height,
        /// Submitted block hash.
        hash: block::Hash,
        /// Typed, evidence-bearing verifier outcome.
        outcome: BlockApplyOutcome,
    },
    /// Node wiring finished or abandoned a `Block` response to an inbound `GetBlocks`.
    BlockRangeResponseFinished {
        /// Peer whose served-response slot can be released.
        peer: ZakuraPeerId,
        /// First requested height.
        start_height: block::Height,
        /// Requested block count.
        requested_count: u32,
        /// Number of blocks read from state and sent in the response.
        returned_count: u32,
    },
    /// State returned committed bodies requested by a peer and the reactor should send them.
    BlockRangeResponseReady {
        /// Peer whose inbound request is being served.
        peer: ZakuraPeerId,
        /// First requested height.
        start_height: block::Height,
        /// Requested block count.
        requested_count: u32,
        /// Bounded committed blocks returned by state.
        blocks: Vec<(block::Height, Arc<block::Block>, usize)>,
    },
}

/// Result of applying a block-sync body through the verifier driver.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockApplyResult {
    /// The block was verified and committed.
    Committed,
    /// The verifier reported the block was already committed.
    Duplicate,
    /// The verifier produced a deterministic peer-attributable rejection.
    Rejected,
    /// Verification failed without a durable peer-attributable conclusion.
    Unavailable,
    /// The verifier did not answer before the driver timeout.
    TimedOut,
}

/// Typed body-verification outcome retained across the driver boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockApplyOutcome {
    verification: Box<zakura_header_chain::BodyVerificationOutcome>,
    duplicate: bool,
}

impl BlockApplyOutcome {
    /// A body newly accepted by full state.
    pub fn committed(evidence: zakura_header_chain::VerifiedBodyEvidence) -> Self {
        Self {
            verification: Box::new(zakura_header_chain::BodyVerificationOutcome::Verified(
                evidence,
            )),
            duplicate: false,
        }
    }

    /// A body already accepted by full state.
    pub fn duplicate(evidence: zakura_header_chain::VerifiedBodyEvidence) -> Self {
        Self {
            verification: Box::new(zakura_header_chain::BodyVerificationOutcome::Verified(
                evidence,
            )),
            duplicate: true,
        }
    }

    /// A supplier-attributed body/header commitment mismatch.
    pub fn payload_mismatch(evidence: zakura_header_chain::BodyPayloadMismatch) -> Self {
        Self {
            verification: Box::new(
                zakura_header_chain::BodyVerificationOutcome::PayloadMismatch(evidence),
            ),
            duplicate: false,
        }
    }

    /// A commitment-matching deterministic consensus failure.
    pub fn consensus_invalid(evidence: zakura_header_chain::ConsensusBodyInvalid) -> Self {
        Self {
            verification: Box::new(
                zakura_header_chain::BodyVerificationOutcome::ConsensusInvalid(evidence),
            ),
            duplicate: false,
        }
    }

    /// A verification attempt that did not reach a durable conclusion.
    pub fn retryable(evidence: zakura_header_chain::TransientBodyFailure) -> Self {
        Self {
            verification: Box::new(zakura_header_chain::BodyVerificationOutcome::Retryable(
                evidence,
            )),
            duplicate: false,
        }
    }

    /// Canonical typed verification evidence.
    pub fn verification(&self) -> &zakura_header_chain::BodyVerificationOutcome {
        self.verification.as_ref()
    }

    /// Consume this wrapper and return canonical typed verification evidence.
    pub fn into_verification(self) -> zakura_header_chain::BodyVerificationOutcome {
        *self.verification
    }

    pub(crate) fn attributed_source(&self) -> Option<zakura_header_chain::SourceId> {
        match self.verification.as_ref() {
            zakura_header_chain::BodyVerificationOutcome::PayloadMismatch(evidence) => {
                Some(evidence.source)
            }
            zakura_header_chain::BodyVerificationOutcome::ConsensusInvalid(evidence) => {
                Some(evidence.source)
            }
            zakura_header_chain::BodyVerificationOutcome::Verified(_)
            | zakura_header_chain::BodyVerificationOutcome::Retryable(_) => None,
        }
    }

    pub(crate) fn retryable_mut(
        &mut self,
    ) -> Option<&mut zakura_header_chain::TransientBodyFailure> {
        match self.verification.as_mut() {
            zakura_header_chain::BodyVerificationOutcome::Retryable(failure) => Some(failure),
            _ => None,
        }
    }

    /// Stable evidence identity for this exact outcome.
    pub fn evidence(&self) -> zakura_header_chain::EvidenceId {
        match self.verification.as_ref() {
            zakura_header_chain::BodyVerificationOutcome::Verified(evidence) => evidence.evidence,
            zakura_header_chain::BodyVerificationOutcome::PayloadMismatch(evidence) => {
                evidence.evidence
            }
            zakura_header_chain::BodyVerificationOutcome::ConsensusInvalid(evidence) => {
                evidence.evidence
            }
            zakura_header_chain::BodyVerificationOutcome::Retryable(evidence) => evidence.evidence,
        }
    }

    /// Coarse scheduling disposition derived without losing the typed outcome.
    pub fn result(&self) -> BlockApplyResult {
        match self.verification.as_ref() {
            zakura_header_chain::BodyVerificationOutcome::Verified(_) if self.duplicate => {
                BlockApplyResult::Duplicate
            }
            zakura_header_chain::BodyVerificationOutcome::Verified(_) => {
                BlockApplyResult::Committed
            }
            zakura_header_chain::BodyVerificationOutcome::PayloadMismatch(_)
            | zakura_header_chain::BodyVerificationOutcome::ConsensusInvalid(_) => {
                BlockApplyResult::Rejected
            }
            zakura_header_chain::BodyVerificationOutcome::Retryable(evidence)
                if evidence.kind == zakura_header_chain::TransientBodyFailureKind::Timeout =>
            {
                BlockApplyResult::TimedOut
            }
            zakura_header_chain::BodyVerificationOutcome::Retryable(_) => {
                BlockApplyResult::Unavailable
            }
        }
    }
}

/// Monotonic token assigned by the reactor to each verifier submission.
///
/// The verifier can return stale duplicate completions after a reset and
/// resubmission of the same height/hash. Echoing this token lets the reactor
/// ignore those stale completions instead of releasing a newer in-flight body.
pub type BlockApplyToken = u64;

/// Actions emitted by the future block-sync reactor for the service seam.
#[derive(Clone, Debug)]
pub enum BlockSyncAction {
    /// Ask node wiring to read `missing_block_bodies`, header hashes, and size hints.
    QueryNeededBlocks {
        /// Reactor-local query identifier the driver must echo with the result.
        query_id: NonZeroU64,
        /// First height to consider for the next local work-buffer refill.
        from: block::Height,
        /// Maximum number of heights to scan for this refill.
        limit: u32,
        /// Current best header target, used for diagnostics and coalescing.
        best_header_tip: block::Height,
        /// Atomic durable coordinates that own this state query and its result.
        scope: zakura_header_chain::WorkScope,
    },
    /// Ask node wiring to read committed bodies for an inbound `GetBlocks`.
    QueryBlocksByHeightRange {
        /// Peer that requested the range.
        peer: ZakuraPeerId,
        /// First height.
        start: block::Height,
        /// Maximum count.
        count: u32,
    },
    /// Parent-first body ready for B3's verifier/commit driver.
    SubmitBlock {
        /// Exact network request that owns this verifier submission.
        owner: zakura_header_chain::WorkOwner,
        /// Authenticated peer source that supplied the body.
        source: zakura_header_chain::SourceId,
        /// Submission token to echo in [`BlockSyncEvent::BlockApplyFinished`].
        token: BlockApplyToken,
        /// Block body that is contiguous above `verified_block_tip`.
        block: Arc<block::Block>,
    },
    /// Persist one completion-gated transient body result.
    RecordBodyUnavailable {
        /// Durable version that owned the attempt.
        expected_version: zakura_header_chain::StateVersion,
        /// Typed retry result with its bounded episode summary.
        failure: zakura_header_chain::TransientBodyFailure,
    },
    /// Persist one exact commitment-matching deterministic body rejection.
    RecordBodyInvalid {
        /// Durable version that owned the verification attempt.
        expected_version: zakura_header_chain::StateVersion,
        /// Exact invalid body conclusion and its authenticated supplier.
        invalid: zakura_header_chain::ConsensusBodyInvalid,
    },
    /// Persist a fresh episode after discovering a changed eligible supplier set.
    RestartBodyAvailability {
        /// Durable version whose selected alarm is being restarted.
        expected_version: zakura_header_chain::StateVersion,
        /// Authenticated supplier-set evidence and fresh summary.
        discovery: zakura_header_chain::BodySupplierDiscovered,
    },
    /// Persist a fresh episode after an authenticated operator request.
    RetryBodyAvailability {
        /// Durable version whose selected alarm is being retried.
        expected_version: zakura_header_chain::StateVersion,
        /// Authenticated operator evidence and fresh summary.
        retry: zakura_header_chain::OperatorBodyRetry,
    },
    /// Report peer misbehavior to the supervisor.
    Misbehavior {
        /// Misbehaving peer.
        peer: ZakuraPeerId,
        /// Reason for reporting.
        reason: BlockSyncMisbehavior,
    },
}

impl BlockSyncAction {
    /// Stable low-cardinality label for action-channel metrics.
    pub(super) fn metric_label(&self) -> &'static str {
        match self {
            Self::QueryNeededBlocks { .. } => "query_needed_blocks",
            Self::QueryBlocksByHeightRange { .. } => "query_blocks_by_height_range",
            Self::SubmitBlock { .. } => "submit_block",
            Self::RecordBodyUnavailable { .. } => "record_body_unavailable",
            Self::RecordBodyInvalid { .. } => "record_body_invalid",
            Self::RestartBodyAvailability { .. } => "restart_body_availability",
            Self::RetryBodyAvailability { .. } => "retry_body_availability",
            Self::Misbehavior { .. } => "misbehavior",
        }
    }
}

/// Block-sync peer-accounting violations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockSyncMisbehavior {
    /// A stream-6 payload was malformed before semantic handling.
    MalformedMessage,
    /// A peer sent blocks that were not requested.
    UnsolicitedBlock,
    /// A peer requested more blocks than this node advertised it can serve.
    GetBlocksTooLong,
    /// A peer exceeded this node's inbound `GetBlocks` serving budget.
    GetBlocksSpam,
    /// A peer supplied a body whose payload does not match its requested header.
    BodyPayloadMismatch(zakura_header_chain::BodyPayloadMismatch),
    /// A peer supplied another invalid block payload.
    InvalidBlock,
    /// A peer supplied a body outside the tolerated scheduling-size deviation.
    SizeMismatch,
    /// Peer status is internally impossible.
    InvalidStatus,
    /// A response terminator arrived without an outstanding range.
    UnsolicitedDone,
    /// A peer reported a requested range unavailable.
    RangeUnavailable,
    /// A peer sent too many status frames.
    StatusSpam,
}

/// The shared routine→reactor channel (per-peer routines inverted data flow).
///
/// Each per-peer pipe-routine ([`PeerRoutine`](super::peer_routine)) decodes its
/// own frames and runs the download logic locally; it forwards only the concerns
/// that need reactor-global state (serving, status advertisement, the producer,
/// misbehavior aggregation) over this channel. The sender is `try_send`/bounded
/// so a busy reactor never backpressures a routine's decode loop into stalling
/// its transport (the only blocking routine send is the Sequencer `AcceptBody`).
#[derive(Clone, Debug)]
pub(super) enum RoutineToReactor {
    /// A routine received a `Status` and updated its own servable/caps + the
    /// registry. The reactor advertises our `Status` reply and republishes the
    /// candidate set. `send_reply` is the routine's rate-meter decision for whether
    /// a reply is due this time.
    StatusReceived {
        /// Peer whose status was applied.
        peer: ZakuraPeerId,
        /// Whether the rate meter allows sending a `Status` reply now.
        send_reply: bool,
    },
    /// A peer requested OUR committed blocks (serving). The reactor runs the
    /// state query + driver path and sends via the peer's session clone.
    ServeGetBlocks {
        /// Peer that requested the range.
        peer: ZakuraPeerId,
        /// First requested height.
        start_height: block::Height,
        /// Requested block count.
        count: u32,
    },
    /// A routine drained its pending work; the producer should re-query (it
    /// self-gates on low-water, so the ping is idempotent/cheap).
    RequeryNeeded,
    /// A routine scored a peer offense that needs the reactor-side
    /// disconnect/scoring action (serving-side malformed frames report via this
    /// path; download-side offenses score directly through the `actions`
    /// channel + the shared registry count).
    Misbehavior {
        /// Misbehaving peer.
        peer: ZakuraPeerId,
        /// Reason for reporting.
        reason: BlockSyncMisbehavior,
    },
}
