//! Atomic durable write-plan DTOs applied by the state adapter.

use zakura_chain::block;

use crate::{EligibilityState, EvidenceId, FinalityEpoch, Frontier, HeaderNode};

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
    /// Hash-keyed auxiliary changes (authoritative).
    pub aux_changes: Vec<AuxDelta>,
    /// Optional append-only finality record (authoritative).
    pub finality_append: Option<FinalityRecord>,
    /// New singleton metadata written last in the atomic batch (authoritative root).
    pub metadata: EngineMetadata,
}
