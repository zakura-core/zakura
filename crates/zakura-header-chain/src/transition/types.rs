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
    BodyRuleId, BodyUnavailableSummary, BodyWorkOwner, BranchId, ChainScore, EligibilityState,
    EngineMode, EvidenceId, FinalityEpoch, Frontier, FrontierSet, HeaderGeneration, HeaderNode,
    HeaderSyncWorkOwner, HeaderValidationState, OperatorInvalidationId, SourceId, StateVersion,
    VerifiedGeneration,
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
    pub(crate) parent: Frontier,
    /// Up to 28 facts in reverse height order, beginning with `parent`.
    pub(crate) predecessors: Vec<HeaderContextFact>,
    /// Digest of current trust anchors.
    pub(crate) trust_anchor_digest: [u8; 32],
    /// Digest binding the complete lease contents.
    pub(crate) context_digest: [u8; 32],
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

    /// Return the exact known parent.
    pub const fn parent(&self) -> Frontier {
        self.parent
    }

    /// Return the reverse-height predecessor context beginning with the parent.
    pub fn predecessors(&self) -> &[HeaderContextFact] {
        &self.predecessors
    }

    /// Return the digest of the trust anchors used to issue this lease.
    pub const fn trust_anchor_digest(&self) -> [u8; 32] {
        self.trust_anchor_digest
    }

    /// Return the digest binding all lease contents.
    pub const fn context_digest(&self) -> [u8; 32] {
        self.context_digest
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

/// Sealed evidence that preparation completed every graph-independent rule.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ContextFreePreparationReceipt {
    parent: Frontier,
    trust_anchor_digest: [u8; 32],
}

impl ContextFreePreparationReceipt {
    /// Return the caller-supplied parent used for height-dependent local rules.
    pub const fn parent(&self) -> Frontier {
        self.parent
    }

    /// Return the authenticated immutable rule-set identity.
    pub const fn trust_anchor_digest(&self) -> [u8; 32] {
        self.trust_anchor_digest
    }
}

/// Sealed nonempty batch carrying explicit graph-independent validation evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedHeaderBatch {
    headers: Vec<PreparedHeader>,
    receipt: ContextFreePreparationReceipt,
    evidence: EvidenceId,
}

impl PreparedHeaderBatch {
    #[allow(dead_code)] // Called by the public preparation pipeline introduced in PR-11.
    pub(crate) fn new(
        headers: Vec<PreparedHeader>,
        parent: Frontier,
        trust_anchor_digest: [u8; 32],
        evidence: EvidenceId,
    ) -> Result<Self, TransitionTypeError> {
        if headers.is_empty() {
            return Err(TransitionTypeError::EmptyHeaderBatch);
        }
        Ok(Self {
            headers,
            receipt: ContextFreePreparationReceipt {
                parent,
                trust_anchor_digest,
            },
            evidence,
        })
    }

    /// Return the prepared headers in exact parent-first order.
    pub fn headers(&self) -> &[PreparedHeader] {
        &self.headers
    }

    /// Return the sealed graph-independent preparation receipt.
    pub const fn receipt(&self) -> ContextFreePreparationReceipt {
        self.receipt
    }

    /// Return the batch's stable validation-evidence identity.
    pub const fn evidence(&self) -> EvidenceId {
        self.evidence
    }

    /// Rebase this sealed batch after an exact prepared header that became finalized.
    ///
    /// The remaining headers retain their validated results and absolute heights; the suffix is
    /// resealed to the now-durable parent. Returns the removed header count.
    pub(crate) fn rebase_after(&mut self, parent: Frontier) -> Result<usize, TransitionTypeError> {
        if self.receipt.parent == parent {
            return Ok(0);
        }
        let Some(index) = self
            .headers
            .iter()
            .position(|header| Frontier::new(header.height, header.hash) == parent)
        else {
            return Err(TransitionTypeError::InvalidPreparedRebase);
        };
        let removed = index.saturating_add(1);
        self.headers.drain(..removed);
        self.receipt.parent = parent;
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-context-free-batch-v1");
        hasher.update(parent.height.0.to_le_bytes());
        hasher.update(parent.hash.0);
        hasher.update(self.receipt.trust_anchor_digest);
        for header in &self.headers {
            hasher.update(header.height.0.to_le_bytes());
            hasher.update(header.hash.0);
        }
        self.evidence = EvidenceId::from_digest(hasher.finalize().into());
        Ok(removed)
    }

    pub(crate) fn clear_already_applied(&mut self) {
        self.headers.clear();
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
    pub owner: HeaderSyncWorkOwner,
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
    /// A bounded prefix of a larger peer-advertised target was completed.
    ///
    /// Prefix admission bounds requester memory while preserving exact validation and
    /// ownership for the last header actually supplied in this batch.
    TargetPrefix {
        /// Exact locator intersection.
        common_ancestor: Frontier,
    },
    /// One already-selected interior header was redelivered solely to replace auxiliary metadata.
    SelectedAuxiliaryRepair {
        /// Exact selected predecessor used as the single-entry locator.
        common_ancestor: Frontier,
        /// Exact already-selected header whose metadata was redelivered.
        selected_target: Frontier,
    },
}

impl TargetCompletion {
    pub(crate) fn rebase_common_ancestor(
        &mut self,
        common_ancestor: Frontier,
    ) -> Result<(), TransitionTypeError> {
        match self {
            Self::TargetComplete {
                common_ancestor: current,
            }
            | Self::TargetPrefix {
                common_ancestor: current,
            } => {
                *current = common_ancestor;
                Ok(())
            }
            Self::SelectedAuxiliaryRepair { .. } => Err(TransitionTypeError::InvalidPreparedRebase),
        }
    }
}

/// Atomically insert one complete prepared header range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertHeaders {
    /// Current asynchronous work owner.
    pub owner: HeaderSyncWorkOwner,
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

/// Full-state acceptance of a block on a path that did not become the verified winner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedBlockAccepted {
    /// Internal full-state transition identity and authority proof.
    pub full_state_transition_id: EvidenceId,
    /// Exact finalized-rooted path through the accepted block.
    pub path: Vec<VerifiedHeaderRef>,
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
    /// Exact selected header whose persistent retry episode gains a supplier.
    pub hash: block::Hash,
    /// Stable identity of the authenticated supplier-set observation.
    pub evidence: EvidenceId,
    /// Existing alarm episode with updated supplier evidence and a due probe.
    pub availability: BodyUnavailableSummary,
}

/// Authenticated operator request to restart one persistent body retry episode.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperatorBodyRetry {
    /// Exact selected header whose retry episode restarts.
    pub hash: block::Hash,
    /// Stable identity of the authenticated operator request.
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

/// Auxiliary metadata authentication update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuxEvidence {
    /// Current work owner.
    pub owner: BodyWorkOwner,
    /// One or two exact deliveries and their immutable provenance.
    pub deliveries: Vec<PreparedAuxDelivery>,
    /// New authentication state applied atomically to every named delivery.
    pub authentication: AuxAuthentication,
}

/// Dependency-neutral VCT metadata repair status published by the state writer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct VctRootRepairStatus {
    /// Current repair need.
    pub state: VctRootRepairState,
    /// Monotonic replacement-attempt generation.
    pub generation: u64,
}

impl Default for VctRootRepairStatus {
    fn default() -> Self {
        Self {
            state: VctRootRepairState::Idle,
            generation: 0,
        }
    }
}

/// Exact VCT metadata repair need, independent of state/network service types.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VctRootRepairState {
    /// No VCT metadata repair is currently required.
    Idle,
    /// The finalized writer needs a replacement delivery for one exact height.
    Unavailable {
        /// Height whose selected-header metadata is unavailable or rejected.
        height: block::Height,
    },
}

/// Every chain-changing input accepted by the sole transition planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionEvent {
    /// Prepared header admission.
    InsertHeaders(Box<InsertHeaders>),
    /// Full-state selected path changed.
    VerifiedChainChanged(VerifiedChainChanged),
    /// Full state accepted a block without changing its selected path.
    VerifiedBlockAccepted(VerifiedBlockAccepted),
    /// Body delivery/verification evidence.
    BodyEvidence(BodyEvidence),
    /// A newly eligible supplier restarted body acquisition.
    BodySupplierDiscovered(BodySupplierDiscovered),
    /// An authenticated operator restarted body acquisition.
    OperatorBodyRetry(OperatorBodyRetry),
    /// Reversible operator invalidation.
    OperatorInvalidate(OperatorInvalidate),
    /// Reason-scoped operator reconsideration.
    OperatorReconsider(OperatorReconsider),
    /// Integrated full-state finality advancement.
    FullStateFinalized(FullStateFinalized),
    /// Integrated full state refuted an imported headers-only pin.
    MigratedPinRefutation(MigratedPinRefutation),
    /// Hash-scoped auxiliary evidence.
    AuxEvidence(Box<AuxEvidence>),
    /// Reevaluate all locally due future-time deferrals.
    ReevaluateDeferred,
}

/// Authority/mode gate checked before any transition effect.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EventAdmission {
    /// Valid in integrated and headers-only modes.
    AnyMode,
    /// Requires authenticated integrated full-state authority.
    IntegratedFullState,
}

impl TransitionEvent {
    /// Return the authority gate fixed for this event category.
    pub fn admission(&self) -> EventAdmission {
        match self {
            Self::VerifiedChainChanged(_)
            | Self::VerifiedBlockAccepted(_)
            | Self::BodyEvidence(_)
            | Self::BodySupplierDiscovered(_)
            | Self::FullStateFinalized(_)
            | Self::MigratedPinRefutation(_)
            | Self::AuxEvidence(_) => EventAdmission::IntegratedFullState,
            Self::InsertHeaders(_)
            | Self::OperatorBodyRetry(_)
            | Self::OperatorInvalidate(_)
            | Self::OperatorReconsider(_)
            | Self::ReevaluateDeferred => EventAdmission::AnyMode,
        }
    }

    /// Return this event's stable idempotency identity when it carries durable evidence.
    pub fn idempotency_key(&self) -> Option<EvidenceId> {
        match self {
            Self::InsertHeaders(event) => match event.completion {
                TargetCompletion::SelectedAuxiliaryRepair { .. } => {
                    event.aux.first().map(|delivery| delivery.delivery_id)
                }
                TargetCompletion::TargetComplete { .. } | TargetCompletion::TargetPrefix { .. } => {
                    Some(event.batch.evidence())
                }
            },
            Self::VerifiedChainChanged(event) => Some(event.full_state_transition_id),
            Self::VerifiedBlockAccepted(event) => Some(event.full_state_transition_id),
            Self::BodyEvidence(BodyEvidence::PayloadMismatch(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::Transient(event)) => Some(event.evidence),
            Self::BodyEvidence(BodyEvidence::Verified(event)) => Some(event.evidence),
            Self::BodySupplierDiscovered(event) => Some(event.evidence),
            Self::OperatorBodyRetry(event) => Some(event.evidence),
            Self::OperatorInvalidate(event) => Some(event.evidence),
            Self::OperatorReconsider(event) => Some(event.evidence),
            Self::FullStateFinalized(event) => Some(event.full_state_transition_id),
            Self::MigratedPinRefutation(event) => Some(event.full_state_transition_id),
            Self::AuxEvidence(event) => match event.authentication {
                AuxAuthentication::Unauthenticated => None,
                AuxAuthentication::Authenticated { evidence, .. }
                | AuxAuthentication::Rejected { evidence } => Some(evidence),
            },
            Self::ReevaluateDeferred => None,
        }
    }

    /// Return explicit branch ownership for asynchronous network-originated events.
    pub fn header_sync_owner(&self) -> Option<HeaderSyncWorkOwner> {
        match self {
            Self::InsertHeaders(event) => Some(event.owner),
            _ => None,
        }
    }

    /// Return body authority for asynchronous body-originated evidence.
    pub fn body_owner(&self) -> Option<BodyWorkOwner> {
        match self {
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
    /// Exclusive upper height for retired prefix rows.
    pub remove_before: Option<block::Height>,
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
    Delete {
        /// Header whose auxiliary record is deleted.
        header_hash: block::Hash,
        /// Exact delivery identity deleted from that header.
        delivery_id: EvidenceId,
    },
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
    /// Ordinary header work was admitted after a durable monotone-finality rebase.
    HeaderWorkRebased,
    /// The complete prepared range was already consumed by monotone finality.
    HeaderWorkAlreadyApplied,
    /// Admission was refused without applying the event because protected state filled a limit.
    ResourceStalled,
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
    pub owners: Vec<HeaderSyncWorkOwner>,
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

/// A resource refusal whose alarm state has already been durably committed.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CommittedStallReceipt {
    /// Durable version after recording or retaining the resource-stall alarm.
    pub state_version: StateVersion,
    /// True when this refusal changed and published the alarm state.
    pub alarm_changed: bool,
    /// Exact attempted branch when the refused event was branch-sensitive.
    pub attempted_branch: Option<BranchId>,
}

/// Serialized transition outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    /// State adapter durably committed.
    Committed,
    /// Idempotent evidence made no change.
    NoChange(NoChangeReceipt),
    /// Ownership/version was stale before effects.
    Stale(StaleReceipt),
    /// Admission was refused after durably recording or retaining its resource alarm.
    ResourceStalled(CommittedStallReceipt),
}

/// Invalid construction at the transition type boundary.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum TransitionTypeError {
    /// Header insertion batches must be nonempty.
    #[error("prepared header batch must be nonempty")]
    EmptyHeaderBatch,
    /// A new finalized parent was not an exact member of the prepared path.
    #[error("prepared header batch cannot rebase to the requested parent")]
    InvalidPreparedRebase,
    /// Advisory body size exceeded the canonical block limit.
    #[error("invalid advisory body size {0}")]
    InvalidBodySize(u32),
}
