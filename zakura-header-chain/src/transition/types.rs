//! Complete typed input and output surface for serialized header-chain transitions.

use std::{num::NonZeroU32, sync::Arc};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{
    block::{self, merkle::AuthDataRoot},
    ironwood, orchard,
    parameters::NetworkKind,
    sapling,
    work::difficulty::Work,
};

use crate::{
    BodyRuleId, BodyUnavailableSummary, BranchId, ChainScore, EligibilityState, EngineMode,
    EvidenceId, FinalityEpoch, Frontier, FrontierSet, HeaderGeneration, HeaderNode,
    HeaderValidationState, OperatorInvalidationId, SourceId, StateVersion, VerifiedGeneration,
    WorkOwner,
};

/// Opaque version of the durable header-chain disk schema.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderChainDiskVersion(pub u32);

/// Persistent externally visible engine alarms.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AlarmSet {
    /// Protected paths prevented resource-bound enforcement.
    pub resource_stalled: bool,
    /// The selected branch has exhausted its current body suppliers/retry episode.
    pub header_best_body_unavailable: Option<BodyUnavailableSummary>,
    /// An imported headers-only trust pin was refuted by deterministic body validation.
    pub migrated_pin_refuted: Option<Frontier>,
}

/// Atomic read snapshot published only after durable commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineSnapshot {
    /// Finality authority mode.
    pub mode: EngineMode,
    /// Complete durable state version.
    pub state_version: StateVersion,
    /// Selected-header work generation.
    pub header_generation: HeaderGeneration,
    /// Full-state verified-path generation.
    pub verified_generation: VerifiedGeneration,
    /// Exact finalized, selected-header, and verified frontiers.
    pub frontiers: FrontierSet,
    /// Exact score of `frontiers.header_best` after the work anchor.
    pub header_best_score: ChainScore,
    /// Lowest retained height available for serving/context.
    pub oldest_retained_height: block::Height,
    /// Durable operator-visible alarms.
    pub alarms: AlarmSet,
}

/// Singleton durable metadata row that is the logical root of one committed state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineMetadata {
    /// Durable schema version.
    pub disk_format: HeaderChainDiskVersion,
    /// Persisted finality mode.
    pub mode: EngineMode,
    /// Persisted authenticated network identity.
    pub network_id: NetworkKind,
    /// Digest of the release-authenticated settled manifest.
    pub anchor_manifest_digest: [u8; 32],
    /// Immutable work-coordinate origin.
    pub work_origin: Frontier,
    /// Complete durable state version.
    pub state_version: StateVersion,
    /// Selected-header work generation.
    pub header_generation: HeaderGeneration,
    /// Full-state verified-path generation.
    pub verified_generation: VerifiedGeneration,
    /// Finality advancement epoch.
    pub finality_epoch: FinalityEpoch,
    /// Exact durable frontiers.
    pub frontiers: FrontierSet,
    /// Exact selected-header score.
    pub header_best_score: ChainScore,
    /// Lowest retained height.
    pub oldest_retained_height: block::Height,
    /// Durable alarms.
    pub alarms: AlarmSet,
    /// Idempotency identity of the most recent committed transition.
    pub last_transition_id: EvidenceId,
}

impl EngineMetadata {
    /// Project the authoritative metadata row into its externally visible snapshot.
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            mode: self.mode,
            state_version: self.state_version,
            header_generation: self.header_generation,
            verified_generation: self.verified_generation,
            frontiers: self.frontiers,
            header_best_score: self.header_best_score,
            oldest_retained_height: self.oldest_retained_height,
            alarms: self.alarms.clone(),
        }
    }
}

/// One immutable predecessor fact sealed into a validation lease.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderContextFact {
    /// Exact predecessor frontier.
    pub frontier: Frontier,
    /// Compact target and time are authenticated by `frontier.hash`.
    pub difficulty_threshold: zakura_chain::work::difficulty::CompactDifficulty,
    /// Canonical predecessor time.
    pub time: DateTime<Utc>,
}

/// Exact branch-local context used to prepare a header batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationLease {
    /// Exact known parent.
    pub parent: Frontier,
    /// Up to 28 facts in reverse height order, beginning with `parent`.
    pub predecessors: Vec<HeaderContextFact>,
    /// Digest of current trust anchors.
    pub trust_anchor_digest: [u8; 32],
    /// Digest binding the complete lease contents.
    pub context_digest: [u8; 32],
}

impl ValidationLease {
    /// Construct a lease digest bound to its exact ordered durable context.
    pub fn new(
        parent: Frontier,
        predecessors: Vec<HeaderContextFact>,
        trust_anchor_digest: [u8; 32],
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-validation-lease-v1");
        hasher.update(parent.height.0.to_le_bytes());
        hasher.update(parent.hash.0);
        hasher.update(trust_anchor_digest);
        for fact in &predecessors {
            hasher.update(fact.frontier.height.0.to_le_bytes());
            hasher.update(fact.frontier.hash.0);
            hasher.update(fact.difficulty_threshold.to_le_bytes());
            hasher.update(fact.time.timestamp().to_le_bytes());
            hasher.update(fact.time.timestamp_subsec_nanos().to_le_bytes());
        }
        Self {
            parent,
            predecessors,
            trust_anchor_digest,
            context_digest: hasher.finalize().into(),
        }
    }
}

/// One fully prepared observable-header result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedHeader {
    /// Canonical header.
    pub header: Arc<block::Header>,
    /// Locally computed hash.
    pub hash: block::Hash,
    /// Locally inferred height.
    pub height: block::Height,
    /// Exact per-block work.
    pub block_work: Work,
    /// Valid or locally future-deferred state.
    pub validation: HeaderValidationState,
}

/// Sealed nonempty batch whose validation receipts are bound to one lease digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedHeaderBatch {
    headers: Vec<PreparedHeader>,
    lease_digest: [u8; 32],
    evidence: EvidenceId,
}

impl PreparedHeaderBatch {
    #[allow(dead_code)] // Called by the public preparation pipeline introduced in PR-11.
    pub(crate) fn new(
        headers: Vec<PreparedHeader>,
        lease_digest: [u8; 32],
        evidence: EvidenceId,
    ) -> Result<Self, TransitionTypeError> {
        if headers.is_empty() {
            return Err(TransitionTypeError::EmptyHeaderBatch);
        }
        Ok(Self {
            headers,
            lease_digest,
            evidence,
        })
    }

    /// Return the prepared headers in exact parent-first order.
    pub fn headers(&self) -> &[PreparedHeader] {
        &self.headers
    }

    /// Return the durable-context digest this work was prepared against.
    pub const fn lease_digest(&self) -> [u8; 32] {
        self.lease_digest
    }

    /// Return the batch's stable validation-evidence identity.
    pub const fn evidence(&self) -> EvidenceId {
        self.evidence
    }
}

/// Bounded advisory body-size metadata; it cannot allocate or grant admission credit.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BodySizeHint {
    /// Wire value zero: no size is known.
    Unknown,
    /// Canonical block size in `1..=MAX_BLOCK_BYTES`.
    Known(NonZeroU32),
}

impl BodySizeHint {
    /// Validate an advisory wire value.
    pub fn new(value: u32) -> Result<Self, TransitionTypeError> {
        if value == 0 {
            return Ok(Self::Unknown);
        }
        if u64::from(value) > block::MAX_BLOCK_BYTES {
            return Err(TransitionTypeError::InvalidBodySize(value));
        }
        Ok(Self::Known(
            NonZeroU32::new(value).expect("the zero body-size sentinel returned above"),
        ))
    }
}

/// Authentication state of one hash-keyed auxiliary delivery.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AuxAuthentication {
    /// Peer metadata has no selection or validity authority.
    Unauthenticated,
    /// Integrated verification authenticated this exact delivery.
    Authenticated {
        /// Stable authentication evidence.
        evidence: EvidenceId,
        /// One-header-later authentication boundary.
        boundary_hash: block::Hash,
    },
    /// This delivery was rejected without invalidating its header.
    Rejected {
        /// Stable rejection evidence.
        evidence: EvidenceId,
    },
}

/// Hash-keyed auxiliary delivery with complete provenance.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AuxDelivery {
    /// Stable delivery identity.
    pub delivery_id: EvidenceId,
    /// Exact retained header.
    pub header_hash: block::Hash,
    /// Supplying peer/session identity.
    pub source: SourceId,
    /// Complete work ownership at receipt.
    pub owner: WorkOwner,
    /// Advisory bounded body size.
    pub body_size: BodySizeHint,
    /// Complete schema-1 record retained for later one-header-later authentication.
    pub tree_aux: Option<TreeAuxRecordV1>,
    /// Current authentication state.
    pub authentication: AuxAuthentication,
}

/// Immutable schema-1 commitment inputs for one inferred block height.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TreeAuxRecordV1 {
    /// Exact inferred height of this record.
    pub height: block::Height,
    /// End-of-block Sapling note-commitment root.
    pub sapling_root: sapling::tree::Root,
    /// End-of-block Orchard root, empty below NU5.
    pub orchard_root: orchard::tree::Root,
    /// End-of-block Ironwood root, empty before configured NU7.
    pub ironwood_root: ironwood::tree::Root,
    /// Per-block Sapling shielded transaction count.
    pub sapling_tx_count: u64,
    /// Per-block Orchard shielded transaction count, zero below NU5.
    pub orchard_tx_count: u64,
    /// Per-block Ironwood shielded transaction count, zero before configured NU7.
    pub ironwood_tx_count: u64,
    /// ZIP-244 authorizing-data root, all zero below NU5.
    pub auth_data_root: AuthDataRoot,
}

/// Prepared auxiliary input admitted alongside a header batch.
pub type PreparedAuxDelivery = AuxDelivery;

/// Completion contract attached to one atomic header insertion.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TargetCompletion {
    /// Peer-advertised target was completed from this exact common ancestor.
    TargetComplete {
        /// Exact locator intersection.
        common_ancestor: Frontier,
    },
    /// Headers came from authenticated internal full-state evidence.
    InternalFullState,
}

/// Atomically insert one complete prepared header range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertHeaders {
    /// Current asynchronous work owner.
    pub owner: WorkOwner,
    /// Header supplier.
    pub source: SourceId,
    /// Exact retained parent.
    pub parent_hash: block::Hash,
    /// Exact pursued target.
    pub target_tip_hash: block::Hash,
    /// Target completion proof kind.
    pub completion: TargetCompletion,
    /// Sealed header validation evidence.
    pub batch: PreparedHeaderBatch,
    /// Exact parallel hash-keyed auxiliary deliveries.
    pub aux: Vec<PreparedAuxDelivery>,
}

/// One exact header reference accepted by full state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedHeaderRef {
    /// Exact height.
    pub height: block::Height,
    /// Exact locally computed hash.
    pub hash: block::Hash,
    /// Canonical header.
    pub header: Arc<block::Header>,
}

/// Explicit full-state selected-path change kind; height never infers it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VerifiedChangeCause {
    /// Direct or forward growth.
    Grow,
    /// Same-height, lower-height, or forward-height branch reset.
    Reset,
}

/// Authenticated full-state selected-path transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedChainChanged {
    /// Internal full-state transition identity and authority proof.
    pub full_state_transition_id: EvidenceId,
    /// Exact previously selected verified tip.
    pub old_tip: Frontier,
    /// Continuous new verified suffix, possibly empty back to finalized.
    pub new_path: Vec<VerifiedHeaderRef>,
    /// Explicit branch-aware grow/reset cause.
    pub cause: VerifiedChangeCause,
}

/// Exact body/header commitment mismatch kind.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BodyCommitmentKind {
    /// Delivered block header hash differs from the requested hash.
    HeaderHash,
    /// Transaction Merkle root mismatch.
    TransactionMerkleRoot,
    /// ZIP-244 authorization-data commitment mismatch.
    AuthDataRoot,
    /// Another height-applicable body-derived header commitment.
    Other(&'static str),
}

/// Supplier-attributed mismatched body payload; it cannot affect eligibility.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BodyPayloadMismatch {
    /// Stable delivery evidence.
    pub evidence: EvidenceId,
    /// Requested header hash.
    pub requested: block::Hash,
    /// Delivered header hash.
    pub delivered: block::Hash,
    /// Exact mismatched commitment.
    pub kind: BodyCommitmentKind,
    /// Body supplier, never a header-only supplier.
    pub source: SourceId,
}

/// Commitment-matching deterministic body consensus failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusBodyInvalid {
    /// Exact affected header.
    pub hash: block::Hash,
    /// Stable verifier evidence proving commitment matching and failure.
    pub evidence: EvidenceId,
    /// Exact full-state rule.
    pub rule: BodyRuleId,
    /// Proving body supplier, never inherited header suppliers.
    pub source: SourceId,
}

/// Retryable body failure category with no eligibility effect.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransientBodyFailureKind {
    /// Required state context is not available yet.
    MissingContext,
    /// Work was canceled or superseded.
    Canceled,
    /// Local storage failed transiently.
    Storage,
    /// Verifier service was unavailable.
    VerifierUnavailable,
    /// External wait timed out.
    Timeout,
    /// Local resources are temporarily exhausted.
    ResourceExhausted,
}

/// Retryable body failure evidence.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransientBodyFailure {
    /// Exact affected header.
    pub hash: block::Hash,
    /// Stable retry evidence.
    pub evidence: EvidenceId,
    /// Exact retry category.
    pub kind: TransientBodyFailureKind,
    /// Bounded persistent state of the owning retry episode.
    pub availability: BodyUnavailableSummary,
}

/// Authenticated discovery of a changed eligible body-supplier set.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BodySupplierDiscovered {
    /// Exact selected header whose retry episode restarts.
    pub hash: block::Hash,
    /// Stable identity of the authenticated supplier-set observation.
    pub evidence: EvidenceId,
    /// Fresh zero-attempt episode summary.
    pub availability: BodyUnavailableSummary,
}

/// Full-state acceptance of one exact body/header pair.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct VerifiedBodyEvidence {
    /// Exact accepted header.
    pub hash: block::Hash,
    /// Stable verification evidence.
    pub evidence: EvidenceId,
}

/// Exhaustive body-result categories with intentionally distinct effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BodyVerificationOutcome {
    /// Full-state accepted the exact body/header pair.
    Verified(VerifiedBodyEvidence),
    /// The supplier delivered a payload that did not match the requested header.
    PayloadMismatch(BodyPayloadMismatch),
    /// Commitment-matching body data deterministically failed consensus.
    ConsensusInvalid(ConsensusBodyInvalid),
    /// Verification could not reach a durable consensus conclusion.
    Retryable(TransientBodyFailure),
}

/// Evidence-free verifier classification used before supplier and stable evidence are attached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BodyVerificationClass {
    /// The exact body was already accepted by full state.
    Duplicate,
    /// Delivered body data disagrees with a commitment in its admitted header.
    PayloadMismatch(BodyCommitmentKind),
    /// All applicable commitments matched before one deterministic consensus rule failed.
    ConsensusInvalid(BodyRuleId),
    /// Verification could not reach a durable consensus conclusion.
    Retryable(TransientBodyFailureKind),
}

impl From<BodyVerificationOutcome> for BodyEvidence {
    fn from(outcome: BodyVerificationOutcome) -> Self {
        match outcome {
            BodyVerificationOutcome::Verified(evidence) => Self::Verified(evidence),
            BodyVerificationOutcome::PayloadMismatch(evidence) => Self::PayloadMismatch(evidence),
            BodyVerificationOutcome::ConsensusInvalid(evidence) => Self::ConsensusInvalid(evidence),
            BodyVerificationOutcome::Retryable(evidence) => Self::Transient(evidence),
        }
    }
}

/// Durable transition evidence derived from one body-verification outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BodyEvidence {
    /// Bad delivery only.
    PayloadMismatch(BodyPayloadMismatch),
    /// Intrinsic deterministic body invalidity.
    ConsensusInvalid(ConsensusBodyInvalid),
    /// Retryable local/delivery failure.
    Transient(TransientBodyFailure),
    /// Full-state verified body.
    Verified(VerifiedBodyEvidence),
}

/// Add one reversible operator reason.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperatorInvalidate {
    /// Exact retained target.
    pub target: block::Hash,
    /// Independently removable invalidation identity.
    pub id: OperatorInvalidationId,
    /// Stable authenticated operator-reason digest.
    pub operator_reason_digest: [u8; 32],
    /// Stable idempotency evidence for this authenticated operator action.
    pub evidence: EvidenceId,
}

/// Remove exactly one reversible operator reason.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperatorReconsider {
    /// Exact retained target.
    pub target: block::Hash,
    /// Exact invalidation identity to remove.
    pub id: OperatorInvalidationId,
    /// Stable idempotency evidence for this authenticated operator action.
    pub evidence: EvidenceId,
}

/// Authenticated integrated-mode finality evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullStateFinalized {
    /// Internal full-state transition identity.
    pub full_state_transition_id: EvidenceId,
    /// Exact nonretreating finalized frontier.
    pub new_finalized: Frontier,
    /// Exact verified ancestry proof ending at `new_finalized`.
    pub verified_path_proof: Vec<block::Hash>,
}

/// Deterministic full-state evidence that refutes an imported headers-only trust pin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigratedPinRefutation {
    /// Stable internal full-state transition identity.
    pub full_state_transition_id: EvidenceId,
    /// Exact preserved headers-only pin whose ancestry was refuted.
    pub pin: Frontier,
    /// Exact body-invalid header on the imported path at or below `pin`.
    pub invalid_header: Frontier,
    /// Exact deterministic full-state rule.
    pub rule: BodyRuleId,
}

/// Authenticated local checkpoint advancement.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AdvanceLocalCheckpoint {
    /// Exact configured checkpoint.
    pub checkpoint: Frontier,
    /// Digest of authenticated local configuration.
    pub authenticated_config_digest: [u8; 32],
    /// Stable transition identity.
    pub evidence: EvidenceId,
}

/// Auxiliary metadata authentication update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuxEvidence {
    /// Current work owner.
    pub owner: WorkOwner,
    /// One or two exact deliveries and their immutable provenance.
    pub deliveries: Vec<PreparedAuxDelivery>,
    /// New authentication state applied atomically to every named delivery.
    pub authentication: AuxAuthentication,
}

/// Startup-only recovery evidence.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RecoveryEvidence {
    /// Stable recovery/audit identity.
    pub evidence: EvidenceId,
    /// Digest of the audited source rows.
    pub source_digest: [u8; 32],
}

/// Every chain-changing input accepted by the sole transition planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionEvent {
    /// Prepared header admission.
    InsertHeaders(Box<InsertHeaders>),
    /// Full-state selected path changed.
    VerifiedChainChanged(VerifiedChainChanged),
    /// Body delivery/verification evidence.
    BodyEvidence(BodyEvidence),
    /// A newly eligible supplier restarted body acquisition.
    BodySupplierDiscovered(BodySupplierDiscovered),
    /// Reversible operator invalidation.
    OperatorInvalidate(OperatorInvalidate),
    /// Reason-scoped operator reconsideration.
    OperatorReconsider(OperatorReconsider),
    /// Integrated full-state finality advancement.
    FullStateFinalized(FullStateFinalized),
    /// Integrated full state refuted an imported headers-only pin.
    MigratedPinRefutation(MigratedPinRefutation),
    /// Authenticated local checkpoint advancement.
    AdvanceLocalCheckpoint(AdvanceLocalCheckpoint),
    /// Hash-scoped auxiliary evidence.
    AuxEvidence(Box<AuxEvidence>),
    /// Reevaluate all locally due future-time deferrals.
    ReevaluateDeferred,
    /// Startup-only reconstruction or audit repair.
    Recover(RecoveryEvidence),
}

/// Authority/mode gate checked before any transition effect.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EventAdmission {
    /// Valid in integrated and headers-only modes.
    AnyMode,
    /// Requires authenticated integrated full-state authority.
    IntegratedFullState,
    /// Requires startup authority while normal publication is disabled.
    StartupOnly,
}

impl TransitionEvent {
    /// Return the authority gate fixed for this event category.
    pub fn admission(&self) -> EventAdmission {
        match self {
            Self::InsertHeaders(event)
                if matches!(event.completion, TargetCompletion::InternalFullState) =>
            {
                EventAdmission::IntegratedFullState
            }
            Self::VerifiedChainChanged(_)
            | Self::BodyEvidence(_)
            | Self::BodySupplierDiscovered(_)
            | Self::FullStateFinalized(_)
            | Self::MigratedPinRefutation(_)
            | Self::AuxEvidence(_) => EventAdmission::IntegratedFullState,
            Self::Recover(_) => EventAdmission::StartupOnly,
            Self::InsertHeaders(_)
            | Self::OperatorInvalidate(_)
            | Self::OperatorReconsider(_)
            | Self::AdvanceLocalCheckpoint(_)
            | Self::ReevaluateDeferred => EventAdmission::AnyMode,
        }
    }

    /// Return this event's stable idempotency identity when it carries durable evidence.
    pub fn idempotency_key(&self) -> Option<EvidenceId> {
        match self {
            Self::InsertHeaders(event) => Some(event.batch.evidence()),
            Self::VerifiedChainChanged(event) => Some(event.full_state_transition_id),
            Self::BodyEvidence(BodyEvidence::PayloadMismatch(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::Transient(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::Verified(event)) => Some(event.evidence),
            Self::BodySupplierDiscovered(event) => Some(event.evidence),
            Self::OperatorInvalidate(event) => Some(event.evidence),
            Self::OperatorReconsider(event) => Some(event.evidence),
            Self::FullStateFinalized(event) => Some(event.full_state_transition_id),
            Self::MigratedPinRefutation(event) => Some(event.full_state_transition_id),
            Self::AdvanceLocalCheckpoint(event) => Some(event.evidence),
            Self::AuxEvidence(event) => match event.authentication {
                AuxAuthentication::Unauthenticated => None,
                AuxAuthentication::Authenticated { evidence, .. }
                | AuxAuthentication::Rejected { evidence } => Some(evidence),
            },
            Self::Recover(event) => Some(event.evidence),
            Self::ReevaluateDeferred => None,
        }
    }

    /// Return explicit branch ownership for asynchronous network-originated events.
    pub fn work_owner(&self) -> Option<WorkOwner> {
        match self {
            Self::InsertHeaders(event) => Some(event.owner),
            Self::AuxEvidence(event) => Some(event.owner),
            _ => None,
        }
    }
}

/// Version-qualified request to the sole serialized transition planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionRequest {
    /// Exact durable version observed by the caller.
    pub expected_version: StateVersion,
    /// Typed evidence; callers never submit desired consequences.
    pub event: TransitionEvent,
}

/// Selected or verified height projection replacement.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProjectionDelta {
    /// First height whose old suffix is removed.
    pub remove_from: Option<block::Height>,
    /// Exact replacement suffix in ascending height order.
    pub put: Vec<Frontier>,
}

/// One eligibility cache/reason-set change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EligibilityDelta {
    /// Exact affected header.
    pub hash: block::Hash,
    /// Previous state.
    pub before: EligibilityState,
    /// Projected state.
    pub after: EligibilityState,
}

/// Reconstructible hash/parent/height index changes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexChanges {
    /// Newly indexed frontiers.
    pub inserted: Vec<Frontier>,
    /// Hashes removed from every reconstructible index.
    pub deleted: Vec<block::Hash>,
}

/// One auxiliary-delivery mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuxDelta {
    /// Insert or idempotently retain a delivery.
    Put(Box<AuxDelivery>),
    /// Delete one bounded delivery record.
    Delete(EvidenceId),
}

/// Provenance of one irreversible finality advancement.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FinalitySource {
    /// Durable fully verified full-state decision.
    FullState {
        /// Internal full-state finalization evidence.
        evidence: EvidenceId,
    },
    /// Disclosed 1,000-deep headers-only local trust rule.
    HeadersOnlyDepth {
        /// Selected tip whose depth proved the new pin.
        selected_tip: Frontier,
    },
    /// Preserved local trust pin imported during an explicit mode migration.
    MigratedHeadersOnly,
}

/// Append-only finality audit record.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FinalityRecord {
    /// Previous immutable anchor.
    pub previous: Frontier,
    /// New immutable anchor.
    pub current: Frontier,
    /// Exact authority/proof kind.
    pub source: FinalitySource,
    /// Resulting finality epoch.
    pub epoch: FinalityEpoch,
}

/// Complete pure write plan applied atomically by the state adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeSet {
    /// New or replaced nodes.
    pub put_nodes: Vec<HeaderNode>,
    /// Evicted or finalized-away nodes.
    pub delete_nodes: Vec<block::Hash>,
    /// Reconstructible indexes changed with the nodes.
    pub index_changes: IndexChanges,
    /// Complete deterministic candidate-tip index after this transition.
    pub candidate_tips: Vec<(ChainScore, block::Hash)>,
    /// Selected-header height projection change.
    pub selected_projection: ProjectionDelta,
    /// Full-state verified height projection change.
    pub verified_projection: ProjectionDelta,
    /// Direct or inherited eligibility changes.
    pub eligibility_changes: Vec<EligibilityDelta>,
    /// Hash-keyed auxiliary changes.
    pub aux_changes: Vec<AuxDelta>,
    /// Optional append-only finality record.
    pub finality_append: Option<FinalityRecord>,
    /// New singleton metadata written last in the atomic batch.
    pub metadata: EngineMetadata,
}

/// High-level cause preserved in a committed receipt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransitionCause {
    /// One of the externally typed evidence categories.
    Event,
    /// Headers-only depth finality occurred in the same insertion/reselection.
    HeadersOnlyFinality,
    /// Startup recovery reconstructed state.
    Recovery,
}

/// Work that must be retired before new forward scheduling.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetiredWork {
    /// Header generation changed; all old forward owners are stale.
    pub header_generation_changed: bool,
    /// Verified generation changed; all old body-forward owners are stale.
    pub verified_generation_changed: bool,
    /// Exact owners retired for narrower causes.
    pub owners: Vec<WorkOwner>,
}

/// Ordered receipt created only after a state adapter durably commits a plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedTransition {
    /// Previous atomic snapshot.
    pub previous: EngineSnapshot,
    /// Current durable snapshot.
    pub current: EngineSnapshot,
    /// Stable transition cause.
    pub cause: TransitionCause,
    /// Newly admitted hashes.
    pub inserted: Vec<block::Hash>,
    /// Hashes whose eligibility changed.
    pub eligibility_changed: Vec<block::Hash>,
    /// Resource/finality-evicted hashes.
    pub evicted: Vec<block::Hash>,
    /// Stale work retired before rescheduling.
    pub retired_work: RetiredWork,
    /// State-adapter durable transaction identity.
    pub durable_tx_id: u64,
}

/// Successful idempotent replay with no durable effects.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NoChangeReceipt {
    /// Unchanged durable version.
    pub state_version: StateVersion,
    /// Previously committed event identity, if this event carries one.
    pub event: Option<EvidenceId>,
}

/// Stale version/branch/owner result with guaranteed zero effects.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct StaleReceipt {
    /// Current durable version the caller must reload.
    pub current_version: StateVersion,
    /// Exact stale branch when the event is branch-sensitive.
    pub branch: Option<BranchId>,
}

/// Serialized transition outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    /// State adapter durably committed and assigned a transaction ID.
    Committed(Box<CommittedTransition>),
    /// Idempotent evidence made no change.
    NoChange(NoChangeReceipt),
    /// Ownership/version was stale before effects.
    Stale(StaleReceipt),
}

/// Invalid construction at the transition type boundary.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum TransitionTypeError {
    /// Header insertion batches must be nonempty.
    #[error("prepared header batch must be nonempty")]
    EmptyHeaderBatch,
    /// Advisory body size exceeded the canonical block limit.
    #[error("invalid advisory body size {0}")]
    InvalidBodySize(u32),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_size_hints_enforce_zero_sentinel_and_canonical_limit() {
        assert_eq!(BodySizeHint::new(0), Ok(BodySizeHint::Unknown));
        assert!(matches!(BodySizeHint::new(1), Ok(BodySizeHint::Known(_))));
        let maximum =
            u32::try_from(block::MAX_BLOCK_BYTES).expect("the canonical block limit fits in u32");
        assert!(matches!(
            BodySizeHint::new(maximum),
            Ok(BodySizeHint::Known(_))
        ));
        assert_eq!(
            BodySizeHint::new(maximum + 1),
            Err(TransitionTypeError::InvalidBodySize(maximum + 1))
        );
    }

    #[test]
    fn event_authority_and_evidence_policies_are_typed() {
        let evidence = EvidenceId::from_digest([7; 32]);
        let reconsider = TransitionEvent::OperatorReconsider(OperatorReconsider {
            target: block::Hash([1; 32]),
            id: OperatorInvalidationId::new([2; 16]),
            evidence,
        });
        assert_eq!(reconsider.admission(), EventAdmission::AnyMode);
        assert_eq!(reconsider.idempotency_key(), Some(evidence));
        assert_eq!(reconsider.work_owner(), None);

        let recovery = RecoveryEvidence {
            evidence,
            source_digest: [3; 32],
        };
        let recovery = TransitionEvent::Recover(recovery);
        assert_eq!(recovery.admission(), EventAdmission::StartupOnly);
        assert_eq!(recovery.idempotency_key(), Some(evidence));

        let refutation = TransitionEvent::MigratedPinRefutation(MigratedPinRefutation {
            full_state_transition_id: evidence,
            pin: Frontier::new(block::Height(2), block::Hash([4; 32])),
            invalid_header: Frontier::new(block::Height(1), block::Hash([5; 32])),
            rule: BodyRuleId::new("body.rule"),
        });
        assert_eq!(refutation.admission(), EventAdmission::IntegratedFullState);
        assert_eq!(refutation.idempotency_key(), Some(evidence));
        assert_eq!(refutation.work_owner(), None);

        assert_eq!(
            TransitionEvent::ReevaluateDeferred.admission(),
            EventAdmission::AnyMode
        );
        assert_eq!(TransitionEvent::ReevaluateDeferred.idempotency_key(), None);
    }

    #[test]
    fn body_verification_outcomes_preserve_distinct_transition_effects() {
        let evidence = EvidenceId::from_digest([9; 32]);
        let hash = block::Hash([8; 32]);
        assert!(matches!(
            BodyEvidence::from(BodyVerificationOutcome::Verified(VerifiedBodyEvidence {
                hash,
                evidence,
            })),
            BodyEvidence::Verified(VerifiedBodyEvidence { hash: actual, .. }) if actual == hash
        ));
        assert!(matches!(
            BodyEvidence::from(BodyVerificationOutcome::PayloadMismatch(
                BodyPayloadMismatch {
                    evidence,
                    requested: hash,
                    delivered: block::Hash([7; 32]),
                    kind: BodyCommitmentKind::HeaderHash,
                    source: SourceId::from_digest([6; 32]),
                }
            )),
            BodyEvidence::PayloadMismatch(BodyPayloadMismatch { requested, .. }) if requested == hash
        ));
        assert!(matches!(
            BodyEvidence::from(BodyVerificationOutcome::ConsensusInvalid(
                ConsensusBodyInvalid {
                    hash,
                    evidence,
                    rule: BodyRuleId::new("body.rule"),
                    source: SourceId::from_digest([5; 32]),
                }
            )),
            BodyEvidence::ConsensusInvalid(ConsensusBodyInvalid { hash: actual, .. }) if actual == hash
        ));
        assert!(matches!(
            BodyEvidence::from(BodyVerificationOutcome::Retryable(TransientBodyFailure {
                hash,
                evidence,
                kind: TransientBodyFailureKind::MissingContext,
                availability: BodyUnavailableSummary {
                    attempts: 1,
                    suppliers: 1,
                    alarmed: false,
                    ..Default::default()
                },
            })),
            BodyEvidence::Transient(TransientBodyFailure { hash: actual, .. }) if actual == hash
        ));
    }

    #[test]
    fn transition_event_surface_is_complete_and_contains_no_requested_consequences() {
        let source = include_str!("types.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("the production type surface precedes its tests");
        for variant in [
            "InsertHeaders(Box<InsertHeaders>)",
            "VerifiedChainChanged(VerifiedChainChanged)",
            "BodyEvidence(BodyEvidence)",
            "BodySupplierDiscovered(BodySupplierDiscovered)",
            "OperatorInvalidate(OperatorInvalidate)",
            "OperatorReconsider(OperatorReconsider)",
            "FullStateFinalized(FullStateFinalized)",
            "MigratedPinRefutation(MigratedPinRefutation)",
            "AdvanceLocalCheckpoint(AdvanceLocalCheckpoint)",
            "AuxEvidence(Box<AuxEvidence>)",
            "ReevaluateDeferred",
            "Recover(RecoveryEvidence)",
        ] {
            assert!(source.contains(variant), "missing event variant {variant}");
        }
        for forbidden in [
            "pub new_header_best",
            "pub new_generation",
            "pub prune",
            "pub publish",
        ] {
            assert!(
                !source.contains(forbidden),
                "event inputs must contain evidence, not requested consequence {forbidden}"
            );
        }
    }
}
