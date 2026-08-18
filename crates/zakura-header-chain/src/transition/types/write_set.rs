//! Atomic durable write-plan DTOs applied by the state adapter.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use zakura_chain::block;

use crate::{EligibilityState, EvidenceId, FinalityEpoch, Frontier, HeaderNode, StateVersion};

use super::auxiliary::AuxDelivery;
use super::snapshot::EngineMetadata;

/// A `ProjectionDelta` replaces a selected or verified chain-prefix projection.
///
/// Each `put` entry records the [`Frontier`] of the chain prefix at that height.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProjectionDelta {
    /// Exclusive upper height for retired prefix rows.
    pub remove_before: Option<block::Height>,
    /// First height at which the plan removes the old suffix.
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
        /// Header whose auxiliary record the plan deletes.
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
        /// Original state-writer provenance for the authorized transition.
        provenance: FullStateFinalityProvenance,
    },
    /// Disclosed 1,000-deep headers-only local trust rule.
    HeadersOnlyDepth {
        /// Selected tip whose depth proved the new pin.
        selected_tip: Frontier,
    },
    /// Preserved local trust pin imported during an explicit mode migration.
    MigratedHeadersOnly,
    /// One authenticated replacement for unverifiable legacy finality history.
    DiskMigration {
        /// Durable schema version that the migration replaced.
        from_version: crate::HeaderChainDiskVersion,
        /// Exact configured network-policy identity used for the migration.
        network_policy_digest: [u8; 32],
        /// Independent authority that authenticated the active frontier.
        authentication: DiskMigrationAuthentication,
    },
}

/// Independent authority used to authenticate one disk migration.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DiskMigrationAuthentication {
    /// The canonical full-state height index authenticated the frontier.
    FullState,
    /// A complete retained selected-chain depth proof authenticated the frontier.
    HeadersOnlyDepth {
        /// Selected tip at the end of the complete retained proof.
        selected_tip: Frontier,
    },
}

/// Full-state event kind that authorized one finality advance.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FullStateFinalityKind {
    /// A checkpoint block extended the verified and finalized frontiers together.
    CheckpointGrow,
    /// The full-state writer finalized an already verified path.
    Finalized,
    /// Full-state initialization authenticated the starting frontier.
    Initialization,
}

/// Durable identity of the exact full-state transition that authorized finality.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FullStateFinalityProvenance {
    /// Original full-state transition identity.
    pub evidence: EvidenceId,
    /// Header-engine state version that the authorized event consumed.
    pub state_version: StateVersion,
    /// Exact full-state event kind.
    pub kind: FullStateFinalityKind,
}

impl FullStateFinalityProvenance {
    /// Construct checkpoint-grow provenance from the exact authorized event fields.
    pub fn checkpoint_grow(state_version: StateVersion, current: Frontier) -> Self {
        Self {
            evidence: checkpoint_finality_evidence(state_version, current),
            state_version,
            kind: FullStateFinalityKind::CheckpointGrow,
        }
    }

    /// Construct ordinary full-state finality provenance from its verified path proof.
    pub fn finalized(
        state_version: StateVersion,
        current: Frontier,
        verified_path_proof: &[block::Hash],
    ) -> Self {
        Self {
            evidence: full_state_finality_evidence(state_version, current, verified_path_proof),
            state_version,
            kind: FullStateFinalityKind::Finalized,
        }
    }

    /// Construct initialization provenance from the authenticated full-state anchor.
    pub fn initialization(state_version: StateVersion, current: Frontier) -> Self {
        Self {
            evidence: full_state_initialization_evidence(current),
            state_version,
            kind: FullStateFinalityKind::Initialization,
        }
    }
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

impl FinalityRecord {
    /// Construct a full-state initialization record.
    pub fn full_state(previous: Frontier, current: Frontier, epoch: FinalityEpoch) -> Self {
        Self::full_state_with_provenance(
            previous,
            current,
            epoch,
            FullStateFinalityProvenance::initialization(StateVersion::new(0), current),
        )
    }

    /// Construct a full-state record with its original authorized provenance.
    pub const fn full_state_with_provenance(
        previous: Frontier,
        current: Frontier,
        epoch: FinalityEpoch,
        provenance: FullStateFinalityProvenance,
    ) -> Self {
        Self {
            previous,
            current,
            source: FinalitySource::FullState { provenance },
            epoch,
        }
    }

    /// Return the selected-tip witness when this record has the exact headers-only depth shape.
    pub(crate) fn headers_only_depth_witness(self, depth: u32) -> Option<Frontier> {
        let FinalitySource::HeadersOnlyDepth { selected_tip } = self.source else {
            let FinalitySource::DiskMigration {
                authentication: DiskMigrationAuthentication::HeadersOnlyDepth { selected_tip },
                ..
            } = self.source
            else {
                return None;
            };
            return (selected_tip.height.0.checked_sub(self.current.height.0) == Some(depth))
                .then_some(selected_tip);
        };
        (self.current.height > self.previous.height
            && selected_tip.height.0.checked_sub(self.current.height.0) == Some(depth))
        .then_some(selected_tip)
    }
}

/// Return the state writer's checkpoint-grow transition identity.
pub fn checkpoint_finality_evidence(state_version: StateVersion, current: Frontier) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-full-state-header-transition-v1");
    hasher.update(b"checkpoint-grow");
    hasher.update(state_version.get().to_be_bytes());
    hasher.update(current.hash.0);
    hasher.update(current.height.0.to_be_bytes());
    hasher.update(current.hash.0);
    EvidenceId::from_digest(hasher.finalize().into())
}

/// Return the state writer's finalized-path transition identity.
pub fn full_state_finality_evidence(
    state_version: StateVersion,
    current: Frontier,
    verified_path_proof: &[block::Hash],
) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-full-state-finalized-v1");
    hasher.update(state_version.get().to_be_bytes());
    hasher.update(current.height.0.to_be_bytes());
    hasher.update(current.hash.0);
    for hash in verified_path_proof {
        hasher.update(hash.0);
    }
    EvidenceId::from_digest(hasher.finalize().into())
}

/// Return the authenticated full-state initialization identity.
pub fn full_state_initialization_evidence(current: Frontier) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-full-state-initialization-v1");
    hasher.update(current.height.0.to_be_bytes());
    hasher.update(current.hash.0);
    EvidenceId::from_digest(hasher.finalize().into())
}

/// Authenticated frontier immediately before the retained finality-history window.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FinalityHistoryCheckpoint {
    /// Last finality epoch removed from the retained window.
    pub epoch: crate::FinalityEpoch,
    /// Canonical frontier reached by `epoch`.
    pub frontier: Frontier,
}

/// One canonical header in a headers-only finality ancestry proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalityAncestryHeader {
    /// Canonical decoded header.
    pub header: Arc<block::Header>,
    /// Cached height-and-hash frontier authenticated by this proof position.
    pub frontier: Frontier,
}

/// One immutable shared headers-only finality proof.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FinalityWitnessProof(Arc<[FinalityAncestryHeader]>);

impl FinalityWitnessProof {
    /// Share one validated proof without cloning its headers.
    pub fn new(headers: Vec<FinalityAncestryHeader>) -> Self {
        Self(headers.into())
    }

    /// Return true when no headers-only proof accompanies the transition.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::ops::Deref for FinalityWitnessProof {
    type Target = [FinalityAncestryHeader];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Complete pure write plan applied atomically by the state adapter.
///
/// # Authoritative vs reconstructible
///
/// **Authoritative** fields are durable source of truth; recovery fails closed
/// when they contradict configuration or each other:
/// - [`Self::put_nodes`] / [`Self::delete_nodes`]
/// - [`Self::put_consensus_invalid_body_tombstones`]
/// - direct reasons inside [`Self::eligibility_changes`]
/// - [`Self::aux_changes`]
/// - [`Self::finality_append`]
/// - mode, network, anchors, counters, frontiers, and alarms in [`Self::metadata`]
///
/// **Reconstructible** fields may be rebuilt from authoritative rows after
/// audit (see [`crate::RecoveryRepair`]):
/// - [`Self::index_changes`] (hash/parent/height adjacency and deferred indexes)
/// - [`Self::selected_projection`] / [`Self::verified_projection`]
/// - inherited eligibility caches carried on nodes / eligibility deltas
/// - retention and body-unavailability alarm fields derived from the selected tip
///
/// # Atomic apply
///
/// The adapter must persist the entire set in one durable transaction (metadata
/// last), then install the same delta into the in-memory engine. Partial apply
/// is undefined: either every field lands together or none do. Empty mutations
/// still require a coherent metadata row when the plan is not a verified
/// no-change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeSet {
    /// New or replaced nodes (authoritative).
    pub put_nodes: Vec<HeaderNode>,
    /// Evicted or finalized-away nodes (authoritative).
    pub delete_nodes: Vec<block::Hash>,
    /// New append-only consensus-invalid tombstones (authoritative).
    pub put_consensus_invalid_body_tombstones: Vec<crate::ConsensusInvalidBodyTombstone>,
    /// Reconstructible indexes changed with the nodes.
    pub index_changes: IndexChanges,
    /// Selected-header height projection change (reconstructible cache).
    pub selected_projection: ProjectionDelta,
    /// Full-state verified height projection change (reconstructible cache).
    pub verified_projection: ProjectionDelta,
    /// Direct (authoritative) or inherited (reconstructible cache) eligibility changes.
    pub eligibility_changes: Vec<EligibilityDelta>,
    /// Hash-keyed auxiliary provenance changes. Outcomes remain process-local derived state.
    pub aux_changes: Vec<AuxDelta>,
    /// Optional append-only finality record (authoritative).
    pub finality_append: Option<FinalityRecord>,
    /// Exact headers from the prior frontier through a headers-only depth witness.
    pub finality_ancestry: FinalityWitnessProof,
    /// New singleton metadata written last in the atomic batch (authoritative root).
    pub metadata: EngineMetadata,
}
