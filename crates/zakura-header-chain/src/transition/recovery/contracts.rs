//! Public recovery contracts, diagnostics, and uncommitted repair plans.

use std::{collections::BTreeSet, sync::Arc};

use chrono::{DateTime, Utc};
use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelivery, BodyValidationState, ConsensusInvalidBodyTombstone, CounterExhausted,
    EligibilityReason, EngineMetadata, EngineSnapshot, FinalityRecord, Frontier, HeaderNode,
    StoreError,
};

/// One immutable predecessor record stored below the selectable finalized anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationContextRecord {
    /// Canonical context header, including its backward link.
    pub header: Arc<block::Header>,
    /// Authenticated context height.
    pub height: block::Height,
}

/// The startup audit uses this complete row and index view while publication is disabled.
///
/// # Contract
///
/// - **Cross-row `state_version` consistency.** Every method on one audit pass
///   must observe the same durable version as [`Self::snapshot`] /
///   [`Self::metadata`]. Mixing rows from concurrent commits is undefined and
///   must surface as [`StoreError::Incoherent`] or fail closed in the audit.
/// - **Visit ordering.** [`Self::visit_finality_history`] yields records in
///   ascending finality-epoch order, contiguous from the bootstrap epoch.
/// - **No side effects.** Implementations are read-only: no writes, no
///   publication, no repair mutations. Reconstruction plans are returned by
///   the audit API, not applied through this trait.
pub trait StoreAuditRead {
    /// Return the atomic externally meaningful snapshot.
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError>;
    /// Return complete singleton metadata from the same version as [`Self::snapshot`].
    fn metadata(&self) -> Result<EngineMetadata, StoreError>;
    /// Return every header-node row, including disconnected rows.
    fn all_header_nodes(&self) -> Result<Vec<HeaderNode>, StoreError>;
    /// Return every append-only consensus-invalid body tombstone, including pruned hashes.
    fn all_consensus_invalid_body_tombstones(
        &self,
    ) -> Result<Vec<ConsensusInvalidBodyTombstone>, StoreError>;
    /// Return whether full-state evidence attests to this exact body-validation state.
    fn full_state_attests_to_body_validation_state(
        &self,
        header_hash: block::Hash,
        body_validation_state: &BodyValidationState,
    ) -> Result<bool, StoreError>;
    /// Return every persisted header parent-child edge.
    fn header_child_edges(&self) -> Result<Vec<(block::Hash, block::Hash)>, StoreError>;
    /// Complete selected projection.
    fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError>;
    /// Complete verified projection.
    fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError>;
    /// Complete deferred-time index.
    fn deferred_entries(&self) -> Result<Vec<(DateTime<Utc>, block::Hash)>, StoreError>;
    /// Every authoritative direct-reason root.
    fn eligibility_roots(&self) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError>;
    /// Every auxiliary delivery, including dangling rows.
    fn all_aux_deliveries(&self) -> Result<Vec<AuxDelivery>, StoreError>;
    /// Every immutable below-finalized context row.
    fn validation_context_records(&self) -> Result<Vec<ValidationContextRecord>, StoreError>;
    /// Return the independently authenticated canonical hash at `height`, when available.
    fn authenticated_canonical_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError>;
    /// Visit append-only finality provenance in ascending epoch order.
    ///
    /// The visitor must see each record exactly once, oldest epoch first, with
    /// no durable mutation between visits.
    fn visit_finality_history(
        &self,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError>;
}

/// Stable exhaustive-audit violation categories.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuditViolation {
    /// The audit found conflicting canonical-header and stored hashes.
    NodeHash(block::Hash),
    /// The audit found a non-anchor node without an exact height-minus-one parent.
    Parent(block::Hash),
    /// The audit found cumulative work that did not equal parent plus block work.
    Work(block::Hash),
    /// The audit found header validation state that contradicted deterministic header facts.
    HeaderValidation(block::Hash),
    /// The audit found a missing or contradictory permanent invalidity tombstone.
    ConsensusInvalidBodyTombstone(block::Hash),
    /// The audit found a body projection without exact full-state evidence authority.
    BodyValidationEvidenceAuthority(block::Hash),
    /// The audit found an absent trust pin or an absent conflict reason.
    TrustPin(block::Height, block::Hash),
    /// The audit found authoritative reason roots that disagreed with node source rows.
    EligibilityRoot(block::Hash),
    /// The audit found invalid auxiliary provenance or an invalid node foreign key.
    Auxiliary(block::Hash),
    /// The audit found malformed or discontinuous immutable validation context.
    ValidationContext(block::Hash),
    /// The audit found finality history that contradicted finalized metadata.
    Finality,
    /// The audit found mode, network, manifest, schema, or snapshot data that contradicted configuration.
    Configuration,
    /// The audit found an absent or discontinuous protected source path.
    ProtectedPath(block::Hash),
    /// The audit found authoritative rows above frozen limits without the permitted alarm.
    Limits,
}

/// Reconstructible categories replaced by one atomic recovery transaction.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RecoveryRepair {
    /// Recovery rebinds a fully audited store to the configured trust-anchor manifest.
    TrustAnchorConfiguration,
    /// Recovery rebuilds parent/child adjacency from source nodes.
    ChildIndex,
    /// Recovery rebuilds the future-time index from node states.
    DeferredIndex,
    /// Recovery promotes elapsed future-time deferrals before publication.
    ElapsedDeferrals,
    /// Recovery replaces the selected projection and frontier with recomputed values.
    SelectedProjection,
    /// Recovery rebuilds the verified projection from its authoritative frontier.
    VerifiedProjection,
    /// Recovery rebuilds cached inherited eligibility from ancestry.
    InheritedEligibility,
    /// Recovery rebuilds oldest-retained metadata from source nodes.
    RetentionMetadata,
    /// Recovery rebuilds the selected-tip body-unavailability alarm from its durable node.
    BodyAvailabilityAlarm,
}

/// Exact source-derived state to install before startup publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryPlan {
    /// Snapshot that recovery observed before repair.
    pub snapshot_before_repair: EngineSnapshot,
    /// Corrected metadata with counters advanced exactly once when required.
    pub metadata: EngineMetadata,
    /// Header nodes with reconstructed inherited eligibility caches.
    pub header_nodes: Vec<HeaderNode>,
    /// Complete expected adjacency index.
    pub header_child_edges: Vec<(block::Hash, block::Hash)>,
    /// Complete selected projection.
    pub selected_projection: Vec<Frontier>,
    /// Complete verified projection.
    pub verified_projection: Vec<Frontier>,
    /// Complete deferred index.
    pub deferred_entries: Vec<(DateTime<Utc>, block::Hash)>,
    /// Exact repairs, empty for a coherent store.
    pub repairs: BTreeSet<RecoveryRepair>,
}

impl RecoveryPlan {
    /// Return true when startup may publish without a repair transaction.
    pub fn is_clean(&self) -> bool {
        self.repairs.is_empty()
    }
}

/// Startup audit failed before publication became available.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum RecoveryFailure {
    /// The store could not read the exhaustive rows.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Authoritative source invariants failed.
    #[error("authoritative header-chain source rows failed startup audit")]
    Source {
        /// Deterministically ordered violations.
        violations: Vec<AuditViolation>,
    },
    /// Recovery exhausted a monotonic counter that the repair requires.
    #[error(transparent)]
    Counter(#[from] CounterExhausted),
}

pub(in crate::transition::recovery) fn violation_key(
    violation: &AuditViolation,
) -> (u8, u32, [u8; 32]) {
    match violation {
        AuditViolation::NodeHash(hash) => (0, 0, hash.0),
        AuditViolation::Parent(hash) => (1, 0, hash.0),
        AuditViolation::Work(hash) => (2, 0, hash.0),
        AuditViolation::HeaderValidation(hash) => (3, 0, hash.0),
        AuditViolation::ConsensusInvalidBodyTombstone(hash) => (4, 0, hash.0),
        AuditViolation::BodyValidationEvidenceAuthority(hash) => (5, 0, hash.0),
        AuditViolation::TrustPin(height, hash) => (6, height.0, hash.0),
        AuditViolation::EligibilityRoot(hash) => (7, 0, hash.0),
        AuditViolation::Auxiliary(hash) => (8, 0, hash.0),
        AuditViolation::ValidationContext(hash) => (9, 0, hash.0),
        AuditViolation::Finality => (10, 0, [0; 32]),
        AuditViolation::Configuration => (11, 0, [0; 32]),
        AuditViolation::ProtectedPath(hash) => (12, 0, hash.0),
        AuditViolation::Limits => (13, 0, [0; 32]),
    }
}

pub(in crate::transition::recovery) fn source_failure(
    violation: AuditViolation,
) -> RecoveryFailure {
    RecoveryFailure::Source {
        violations: vec![violation],
    }
}
