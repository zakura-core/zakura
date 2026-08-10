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
    work::difficulty::{ParameterDifficulty, Work},
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
    /// Domain- and payload-bound identity of the most recent committed transition.
    pub last_transition: Option<TransitionFingerprint>,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderContextFact {
    /// Exact predecessor frontier.
    pub frontier: Frontier,
    /// Canonical predecessor header whose hash authenticates all contextual fields.
    pub header: Arc<block::Header>,
}

/// Exact branch-local context used to prepare a header batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationLease {
    /// Exact known parent.
    pub(crate) parent: Frontier,
    /// Up to 28 facts in reverse height order, beginning with `parent`.
    pub(crate) predecessors: Vec<HeaderContextFact>,
    /// Exact network policy used by the issuing engine.
    pub(crate) network: zakura_chain::parameters::Network,
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
        network: zakura_chain::parameters::Network,
        trust_anchor_digest: [u8; 32],
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-validation-lease-v1");
        hasher.update(parent.height.0.to_le_bytes());
        hasher.update(parent.hash.0);
        hasher.update(trust_anchor_digest);
        hash_network_policy(&mut hasher, &network);
        for fact in &predecessors {
            hasher.update(fact.frontier.height.0.to_le_bytes());
            hasher.update(fact.frontier.hash.0);
            hasher.update(fact.header.hash().0);
        }
        Self {
            parent,
            predecessors,
            network,
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

    /// Return the exact authenticated network policy used to issue this lease.
    pub fn network(&self) -> &zakura_chain::parameters::Network {
        &self.network
    }

    /// Return the digest of the trust anchors used to issue this lease.
    pub const fn trust_anchor_digest(&self) -> [u8; 32] {
        self.trust_anchor_digest
    }

    /// Return the digest binding all lease contents.
    pub const fn context_digest(&self) -> [u8; 32] {
        self.context_digest
    }

    pub(crate) fn is_coherent(
        &self,
        network: &zakura_chain::parameters::Network,
        trust_anchor_digest: [u8; 32],
    ) -> bool {
        let required = usize::try_from(self.parent.height.0)
            .ok()
            .and_then(|height| height.checked_add(1))
            .map(|height| height.min(crate::POW_ADJUSTMENT_BLOCK_SPAN));
        if self.network != *network
            || self.trust_anchor_digest != trust_anchor_digest
            || required != Some(self.predecessors.len())
            || self.predecessors.first().map(|fact| fact.frontier) != Some(self.parent)
        {
            return false;
        }
        for (index, fact) in self.predecessors.iter().enumerate() {
            if fact.header.hash() != fact.frontier.hash {
                return false;
            }
            if let Some(newer) = index
                .checked_sub(1)
                .and_then(|index| self.predecessors.get(index))
            {
                if newer.header.previous_block_hash != fact.frontier.hash
                    || newer.frontier.height.previous().ok() != Some(fact.frontier.height)
                {
                    return false;
                }
            }
        }
        Self::new(
            self.parent,
            self.predecessors.clone(),
            self.network.clone(),
            self.trust_anchor_digest,
        )
        .context_digest
            == self.context_digest
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextFreePreparationReceipt {
    parent: Frontier,
    network: zakura_chain::parameters::Network,
    trust_anchor_digest: [u8; 32],
}

impl ContextFreePreparationReceipt {
    /// Return the caller-supplied parent used for height-dependent local rules.
    pub const fn parent(&self) -> Frontier {
        self.parent
    }

    /// Return the exact network policy used for graph-independent validation.
    pub fn network(&self) -> &zakura_chain::parameters::Network {
        &self.network
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
        network: zakura_chain::parameters::Network,
        trust_anchor_digest: [u8; 32],
        evidence: EvidenceId,
    ) -> Result<Self, TransitionTypeError> {
        if headers.is_empty() {
            return Err(TransitionTypeError::EmptyHeaderBatch);
        }
        if headers.len() > crate::MAX_HEADERS_PER_TRANSITION_V1 {
            return Err(TransitionTypeError::OversizedHeaderBatch);
        }
        Ok(Self {
            headers,
            receipt: ContextFreePreparationReceipt {
                parent,
                network,
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
    pub const fn receipt(&self) -> &ContextFreePreparationReceipt {
        &self.receipt
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
    /// End-of-block Ironwood root, empty below NU6.3.
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
    /// Checkpoint-verified growth that atomically advances integrated full-state finality.
    CheckpointFinalizedGrow,
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
    /// Exact currently installed invalidation evidence, or `None` if it is absent.
    pub invalidation_evidence: Option<EvidenceId>,
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

/// Stable domain of one replay-protected transition event.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransitionDomain {
    /// Prepared header admission.
    InsertHeaders,
    /// Full-state selected-path replacement.
    VerifiedChainChanged,
    /// Full-state side-path acceptance.
    VerifiedBlockAccepted,
    /// Supplier-attributed body payload mismatch.
    BodyPayloadMismatch,
    /// Deterministic body invalidity.
    ConsensusBodyInvalid,
    /// Transient body failure.
    TransientBodyFailure,
    /// Verified body acceptance.
    VerifiedBody,
    /// Body supplier-set discovery.
    BodySupplierDiscovered,
    /// Scheduler/operator body retry.
    OperatorBodyRetry,
    /// Operator invalidation.
    OperatorInvalidate,
    /// Operator reconsideration.
    OperatorReconsider,
    /// Full-state finality.
    FullStateFinalized,
    /// Migrated-pin refutation.
    MigratedPinRefutation,
    /// Auxiliary authentication evidence.
    AuxEvidence,
}

impl TransitionDomain {
    /// Return the stable version-one disk discriminant.
    pub const fn code(self) -> u8 {
        match self {
            Self::InsertHeaders => 0,
            Self::VerifiedChainChanged => 1,
            Self::VerifiedBlockAccepted => 2,
            Self::BodyPayloadMismatch => 3,
            Self::ConsensusBodyInvalid => 4,
            Self::TransientBodyFailure => 5,
            Self::VerifiedBody => 6,
            Self::BodySupplierDiscovered => 7,
            Self::OperatorBodyRetry => 8,
            Self::OperatorInvalidate => 9,
            Self::OperatorReconsider => 10,
            Self::FullStateFinalized => 11,
            Self::MigratedPinRefutation => 12,
            Self::AuxEvidence => 13,
        }
    }

    /// Decode a stable version-one disk discriminant.
    pub const fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            0 => Self::InsertHeaders,
            1 => Self::VerifiedChainChanged,
            2 => Self::VerifiedBlockAccepted,
            3 => Self::BodyPayloadMismatch,
            4 => Self::ConsensusBodyInvalid,
            5 => Self::TransientBodyFailure,
            6 => Self::VerifiedBody,
            7 => Self::BodySupplierDiscovered,
            8 => Self::OperatorBodyRetry,
            9 => Self::OperatorInvalidate,
            10 => Self::OperatorReconsider,
            11 => Self::FullStateFinalized,
            12 => Self::MigratedPinRefutation,
            13 => Self::AuxEvidence,
            _ => return None,
        })
    }
}

/// Exact replay identity of one committed transition.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransitionFingerprint {
    domain: TransitionDomain,
    evidence: EvidenceId,
    payload_digest: [u8; 32],
}

impl TransitionFingerprint {
    /// Reconstruct one persisted fingerprint from its canonical fields.
    pub const fn from_parts(
        domain: TransitionDomain,
        evidence: EvidenceId,
        payload_digest: [u8; 32],
    ) -> Self {
        Self {
            domain,
            evidence,
            payload_digest,
        }
    }

    /// Return the stable event domain.
    pub const fn domain(self) -> TransitionDomain {
        self.domain
    }

    /// Return the domain-local idempotency evidence.
    pub const fn evidence(self) -> EvidenceId {
        self.evidence
    }

    /// Return the canonical effect-bearing payload digest.
    pub const fn payload_digest(self) -> [u8; 32] {
        self.payload_digest
    }

    /// True when two events reuse one domain-local key with different effects.
    pub fn conflicts_with(self, other: Self) -> bool {
        self.domain.code() == other.domain.code()
            && self.evidence.digest() == other.evidence.digest()
            && self.payload_digest != other.payload_digest
    }
}

/// Authority/mode gate checked before any transition effect.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EventAdmission {
    /// Valid in integrated and headers-only modes.
    AnyMode,
    /// Requires authenticated integrated full-state authority.
    IntegratedFullState,
    /// Requires an exact retry action staged by the serialized scheduler boundary.
    RegisteredScheduler,
    /// Requires an exact header completion registered by the serialized authority boundary.
    RegisteredHeaderCompletion,
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
            | Self::AuxEvidence(_)
            | Self::OperatorInvalidate(_)
            | Self::OperatorReconsider(_) => EventAdmission::IntegratedFullState,
            Self::OperatorBodyRetry(_) => EventAdmission::RegisteredScheduler,
            Self::InsertHeaders(_) => EventAdmission::RegisteredHeaderCompletion,
            Self::ReevaluateDeferred => EventAdmission::AnyMode,
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

    /// Return the domain-separated canonical replay fingerprint, when replay protection applies.
    pub fn fingerprint(&self) -> Option<TransitionFingerprint> {
        let evidence = self.idempotency_key()?;
        let domain = match self {
            Self::InsertHeaders(_) => TransitionDomain::InsertHeaders,
            Self::VerifiedChainChanged(_) => TransitionDomain::VerifiedChainChanged,
            Self::VerifiedBlockAccepted(_) => TransitionDomain::VerifiedBlockAccepted,
            Self::BodyEvidence(BodyEvidence::PayloadMismatch(_)) => {
                TransitionDomain::BodyPayloadMismatch
            }
            Self::BodyEvidence(BodyEvidence::ConsensusInvalid(_)) => {
                TransitionDomain::ConsensusBodyInvalid
            }
            Self::BodyEvidence(BodyEvidence::Transient(_)) => {
                TransitionDomain::TransientBodyFailure
            }
            Self::BodyEvidence(BodyEvidence::Verified(_)) => TransitionDomain::VerifiedBody,
            Self::BodySupplierDiscovered(_) => TransitionDomain::BodySupplierDiscovered,
            Self::OperatorBodyRetry(_) => TransitionDomain::OperatorBodyRetry,
            Self::OperatorInvalidate(_) => TransitionDomain::OperatorInvalidate,
            Self::OperatorReconsider(_) => TransitionDomain::OperatorReconsider,
            Self::FullStateFinalized(_) => TransitionDomain::FullStateFinalized,
            Self::MigratedPinRefutation(_) => TransitionDomain::MigratedPinRefutation,
            Self::AuxEvidence(_) => TransitionDomain::AuxEvidence,
            Self::ReevaluateDeferred => return None,
        };
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-transition-payload-v1");
        hasher.update([domain.code()]);
        hash_transition_payload(&mut hasher, self);
        Some(TransitionFingerprint::from_parts(
            domain,
            evidence,
            hasher.finalize().into(),
        ))
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

fn hash_transition_payload(hasher: &mut Sha256, event: &TransitionEvent) {
    match event {
        TransitionEvent::InsertHeaders(event) => {
            hash_sync_owner(hasher, event.owner);
            hasher.update(event.source.digest());
            hasher.update(event.parent_hash.0);
            hasher.update(event.target_tip_hash.0);
            match event.completion {
                TargetCompletion::TargetComplete { common_ancestor } => {
                    hasher.update([0]);
                    hash_frontier(hasher, common_ancestor);
                }
                TargetCompletion::TargetPrefix { common_ancestor } => {
                    hasher.update([1]);
                    hash_frontier(hasher, common_ancestor);
                }
                TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor,
                    selected_target,
                } => {
                    hasher.update([2]);
                    hash_frontier(hasher, common_ancestor);
                    hash_frontier(hasher, selected_target);
                }
            }
            let receipt = event.batch.receipt();
            hash_frontier(hasher, receipt.parent());
            hasher.update(receipt.trust_anchor_digest());
            hash_network_policy(hasher, receipt.network());
            for header in event.batch.headers() {
                hasher.update(header.height.0.to_le_bytes());
                hasher.update(header.hash.0);
                hasher.update(header.block_work.as_u256().to_big_endian());
                hash_validation_state(hasher, header.validation);
            }
            for delivery in &event.aux {
                hash_aux_delivery(hasher, *delivery);
            }
        }
        TransitionEvent::VerifiedChainChanged(event) => {
            hash_frontier(hasher, event.old_tip);
            hasher.update([match event.cause {
                VerifiedChangeCause::Grow => 0,
                VerifiedChangeCause::Reset => 1,
                VerifiedChangeCause::CheckpointFinalizedGrow => 2,
            }]);
            hash_verified_path(hasher, &event.new_path);
        }
        TransitionEvent::VerifiedBlockAccepted(event) => hash_verified_path(hasher, &event.path),
        TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(event)) => {
            hasher.update(event.requested.0);
            hasher.update(event.delivered.0);
            hasher.update(event.source.digest());
            match event.kind {
                BodyCommitmentKind::HeaderHash => hasher.update([0]),
                BodyCommitmentKind::TransactionMerkleRoot => hasher.update([1]),
                BodyCommitmentKind::AuthDataRoot => hasher.update([2]),
                BodyCommitmentKind::Other(rule) => {
                    hasher.update([3]);
                    hash_bytes(hasher, rule.as_bytes());
                }
            }
        }
        TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => {
            hasher.update(event.hash.0);
            hash_bytes(hasher, event.rule.as_str().as_bytes());
            hasher.update(event.source.digest());
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Transient(event)) => {
            hasher.update(event.hash.0);
            hasher.update([match event.kind {
                TransientBodyFailureKind::MissingContext => 0,
                TransientBodyFailureKind::Canceled => 1,
                TransientBodyFailureKind::Storage => 2,
                TransientBodyFailureKind::VerifierUnavailable => 3,
                TransientBodyFailureKind::Timeout => 4,
                TransientBodyFailureKind::ResourceExhausted => 5,
            }]);
            hash_availability(hasher, event.availability);
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Verified(event)) => {
            hasher.update(event.hash.0);
        }
        TransitionEvent::BodySupplierDiscovered(event) => {
            hasher.update(event.hash.0);
            hash_availability(hasher, event.availability);
        }
        TransitionEvent::OperatorBodyRetry(event) => {
            hasher.update(event.hash.0);
            hash_availability(hasher, event.availability);
        }
        TransitionEvent::OperatorInvalidate(event) => {
            hasher.update(event.target.0);
            hasher.update(event.id.bytes());
            hasher.update(event.operator_reason_digest);
        }
        TransitionEvent::OperatorReconsider(event) => {
            hasher.update(event.target.0);
            hasher.update(event.id.bytes());
            match event.invalidation_evidence {
                Some(evidence) => {
                    hasher.update([1]);
                    hasher.update(evidence.digest());
                }
                None => hasher.update([0]),
            }
        }
        TransitionEvent::FullStateFinalized(event) => {
            hash_frontier(hasher, event.new_finalized);
            for hash in &event.verified_path_proof {
                hasher.update(hash.0);
            }
        }
        TransitionEvent::MigratedPinRefutation(event) => {
            hash_frontier(hasher, event.pin);
            hash_frontier(hasher, event.invalid_header);
            hash_bytes(hasher, event.rule.as_str().as_bytes());
        }
        TransitionEvent::AuxEvidence(event) => {
            hash_body_owner(hasher, event.owner);
            for delivery in &event.deliveries {
                hash_aux_delivery(hasher, *delivery);
            }
            hash_aux_authentication(hasher, event.authentication);
        }
        TransitionEvent::ReevaluateDeferred => {}
    }
}

fn hash_frontier(hasher: &mut Sha256, frontier: Frontier) {
    hasher.update(frontier.height.0.to_le_bytes());
    hasher.update(frontier.hash.0);
}

fn hash_network_policy(hasher: &mut Sha256, network: &zakura_chain::parameters::Network) {
    hasher.update([match network.kind() {
        NetworkKind::Mainnet => 0,
        NetworkKind::Testnet => 1,
        NetworkKind::Regtest => 2,
    }]);
    hasher.update(network.genesis_hash().0);
    let target: zakura_chain::work::difficulty::U256 = network.target_difficulty_limit().into();
    hasher.update(target.to_big_endian());
    hasher.update([u8::from(network.disable_pow())]);
    let max_time_height = match network {
        zakura_chain::parameters::Network::Mainnet => block::Height::MIN,
        zakura_chain::parameters::Network::Testnet(parameters) => {
            parameters.max_block_time_start_height()
        }
    };
    hasher.update(max_time_height.0.to_le_bytes());
    for (height, upgrade) in network.activation_list() {
        hasher.update(height.0.to_le_bytes());
        let (branch_tag, upgrade_code) = match upgrade.branch_id() {
            Some(branch) => (1_u8, u32::from(branch)),
            None => (
                0,
                match upgrade {
                    zakura_chain::parameters::NetworkUpgrade::Genesis => 0,
                    zakura_chain::parameters::NetworkUpgrade::BeforeOverwinter => 1,
                    zakura_chain::parameters::NetworkUpgrade::Nu7 => 2,
                    _ => u32::MAX,
                },
            ),
        };
        hasher.update([branch_tag]);
        hasher.update(upgrade_code.to_le_bytes());
    }
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(
        u64::try_from(bytes.len())
            .expect("in-memory payload length fits in u64")
            .to_le_bytes(),
    );
    hasher.update(bytes);
}

fn hash_time(hasher: &mut Sha256, time: DateTime<Utc>) {
    hasher.update(time.timestamp().to_le_bytes());
    hasher.update(time.timestamp_subsec_nanos().to_le_bytes());
}

fn hash_validation_state(hasher: &mut Sha256, validation: HeaderValidationState) {
    match validation {
        HeaderValidationState::Valid => hasher.update([0]),
        HeaderValidationState::DeferredUntil(until) => {
            hasher.update([1]);
            hash_time(hasher, until);
        }
    }
}

fn hash_header_owner(hasher: &mut Sha256, owner: crate::HeaderWorkOwner) {
    hasher.update(owner.authority.header_generation.get().to_le_bytes());
    hasher.update(owner.authority.branch.anchor_hash.0);
    hasher.update(owner.authority.branch.target_tip_hash.0);
    hasher.update(owner.session_id.to_le_bytes());
    hasher.update(owner.request_id.get().to_le_bytes());
}

fn hash_body_owner(hasher: &mut Sha256, owner: BodyWorkOwner) {
    hash_header_owner(
        hasher,
        crate::HeaderWorkOwner {
            authority: owner.authority.header,
            session_id: owner.session_id,
            request_id: owner.request_id,
        },
    );
    hasher.update(owner.authority.verified_generation.get().to_le_bytes());
}

fn hash_sync_owner(hasher: &mut Sha256, owner: HeaderSyncWorkOwner) {
    match owner {
        HeaderSyncWorkOwner::Header(owner) => {
            hasher.update([0]);
            hash_header_owner(hasher, owner);
        }
        HeaderSyncWorkOwner::BodyRepair(owner) => {
            hasher.update([1]);
            hash_body_owner(hasher, owner);
        }
    }
}

fn hash_availability(hasher: &mut Sha256, availability: BodyUnavailableSummary) {
    hash_time(hasher, availability.started_at);
    hasher.update(availability.attempts.to_le_bytes());
    hasher.update(availability.suppliers.to_le_bytes());
    hasher.update(availability.supplier_set_digest);
    hasher.update([u8::from(availability.alarmed)]);
    hash_time(hasher, availability.next_probe_at);
}

fn hash_verified_path(hasher: &mut Sha256, path: &[VerifiedHeaderRef]) {
    for header in path {
        hasher.update(header.height.0.to_le_bytes());
        hasher.update(header.hash.0);
        hasher.update(header.header.hash().0);
    }
}

fn hash_aux_delivery(hasher: &mut Sha256, delivery: AuxDelivery) {
    hasher.update(delivery.delivery_id.digest());
    hasher.update(delivery.header_hash.0);
    hasher.update(delivery.source.digest());
    hash_sync_owner(hasher, delivery.owner);
    hasher.update(
        match delivery.body_size {
            BodySizeHint::Unknown => 0_u32,
            BodySizeHint::Known(size) => size.get(),
        }
        .to_le_bytes(),
    );
    match delivery.tree_aux {
        None => hasher.update([0]),
        Some(aux) => {
            hasher.update([1]);
            hasher.update(aux.height.0.to_le_bytes());
            hasher.update(<[u8; 32]>::from(aux.sapling_root));
            hasher.update(<[u8; 32]>::from(aux.orchard_root));
            hasher.update(<[u8; 32]>::from(aux.ironwood_root));
            hasher.update(aux.sapling_tx_count.to_le_bytes());
            hasher.update(aux.orchard_tx_count.to_le_bytes());
            hasher.update(aux.ironwood_tx_count.to_le_bytes());
            hasher.update(<[u8; 32]>::from(aux.auth_data_root));
        }
    }
    hash_aux_authentication(hasher, delivery.authentication);
}

fn hash_aux_authentication(hasher: &mut Sha256, authentication: AuxAuthentication) {
    match authentication {
        AuxAuthentication::Unauthenticated => hasher.update([0]),
        AuxAuthentication::Authenticated {
            evidence,
            boundary_hash,
        } => {
            hasher.update([1]);
            hasher.update(evidence.digest());
            hasher.update(boundary_hash.0);
        }
        AuxAuthentication::Rejected { evidence } => {
            hasher.update([2]);
            hasher.update(evidence.digest());
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
    /// Checkpoint body growth advanced integrated finality on the retained selected path.
    CheckpointFinality,
    /// Full state authenticated or rejected auxiliary metadata without changing the DAG.
    AuxAuthentication,
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
    /// Header insertion batches must fit the frozen engine transition bound.
    #[error("prepared header batch exceeds the engine transition limit")]
    OversizedHeaderBatch,
    /// A new finalized parent was not an exact member of the prepared path.
    #[error("prepared header batch cannot rebase to the requested parent")]
    InvalidPreparedRebase,
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
            invalidation_evidence: Some(EvidenceId::from_digest([3; 32])),
            evidence,
        });
        assert_eq!(reconsider.admission(), EventAdmission::IntegratedFullState);
        assert_eq!(reconsider.idempotency_key(), Some(evidence));
        assert_eq!(reconsider.header_sync_owner(), None);
        assert_eq!(reconsider.body_owner(), None);

        let refutation = TransitionEvent::MigratedPinRefutation(MigratedPinRefutation {
            full_state_transition_id: evidence,
            pin: Frontier::new(block::Height(2), block::Hash([4; 32])),
            invalid_header: Frontier::new(block::Height(1), block::Hash([5; 32])),
            rule: BodyRuleId::new("body.rule"),
        });
        assert_eq!(refutation.admission(), EventAdmission::IntegratedFullState);
        assert_eq!(refutation.idempotency_key(), Some(evidence));
        assert_eq!(refutation.header_sync_owner(), None);
        assert_eq!(refutation.body_owner(), None);

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
    fn all_named_inputs_use_their_single_serialized_transition_path() {
        let source = include_str!("types.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("the production type surface precedes its tests");
        for variant in [
            "InsertHeaders(Box<InsertHeaders>)",
            "VerifiedChainChanged(VerifiedChainChanged)",
            "VerifiedBlockAccepted(VerifiedBlockAccepted)",
            "BodyEvidence(BodyEvidence)",
            "BodySupplierDiscovered(BodySupplierDiscovered)",
            "OperatorBodyRetry(OperatorBodyRetry)",
            "OperatorInvalidate(OperatorInvalidate)",
            "OperatorReconsider(OperatorReconsider)",
            "FullStateFinalized(FullStateFinalized)",
            "MigratedPinRefutation(MigratedPinRefutation)",
            "AuxEvidence(Box<AuxEvidence>)",
            "ReevaluateDeferred",
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
        for obsolete_facade in [
            "AdvanceLocalCheckpoint",
            "InternalFullState",
            "RecoveryEvidence",
            "TransitionEvent::Recover",
        ] {
            assert!(
                !source.contains(obsolete_facade),
                "the event surface must not duplicate a real transition path with {obsolete_facade}"
            );
        }
    }
}
