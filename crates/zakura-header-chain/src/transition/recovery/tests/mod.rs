//! Recovery policy tests.

mod authority;
mod repair;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use chrono::{DateTime, Duration, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};

use super::*;
use crate::{
    AlarmSet, AuxDelivery, BodyValidationState, ChainScore, CheckpointSet,
    ConsensusInvalidBodyTombstone, EligibilityReason, EligibilityState, EngineMetadata, EngineMode,
    EngineSnapshot, FinalityEpoch, FinalityRecord, Frontier, FrontierSet, HeaderChainDiskVersion,
    HeaderGeneration, HeaderNode, HeaderValidationState, RowLimit, StateVersion, StoreCollection,
    StoreError, SuffixWork, TrustedAnchor, UntrustedAuxDeliveryRow, VerifiedGeneration,
    WorkCoordinate,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum AuditRead {
    Snapshot,
    Metadata,
    HeaderNodes,
    Tombstones,
    BodyStateAuthority,
    HeaderChildEdges,
    SelectedProjection,
    VerifiedProjection,
    DeferredEntries,
    EligibilityRoots,
    AuxDeliveries,
    ValidationContexts,
    CanonicalHash,
    FinalityCheckpoint,
    FinalityCount,
    FinalityHistory,
}

impl AuditRead {
    pub(super) const ALL: [Self; 16] = [
        Self::Snapshot,
        Self::Metadata,
        Self::HeaderNodes,
        Self::Tombstones,
        Self::BodyStateAuthority,
        Self::HeaderChildEdges,
        Self::SelectedProjection,
        Self::VerifiedProjection,
        Self::DeferredEntries,
        Self::EligibilityRoots,
        Self::AuxDeliveries,
        Self::ValidationContexts,
        Self::CanonicalHash,
        Self::FinalityCheckpoint,
        Self::FinalityCount,
        Self::FinalityHistory,
    ];
}

#[derive(Clone)]
pub(super) struct AuditStore {
    pub(super) metadata: EngineMetadata,
    pub(super) snapshot: EngineSnapshot,
    pub(super) nodes: Vec<HeaderNode>,
    pub(super) tombstones: Vec<ConsensusInvalidBodyTombstone>,
    pub(super) body_state_authority: bool,
    pub(super) children: Vec<(block::Hash, block::Hash)>,
    pub(super) selected: Vec<Frontier>,
    pub(super) verified: Vec<Frontier>,
    pub(super) deferred: Vec<(DateTime<Utc>, block::Hash)>,
    pub(super) reasons: Vec<(block::Hash, EligibilityReason)>,
    pub(super) aux: Vec<AuxDelivery>,
    pub(super) contexts: Vec<ValidationContextRecord>,
    pub(super) canonical: HashMap<block::Height, block::Hash>,
    pub(super) canonical_reads: Arc<AtomicUsize>,
    pub(super) finality_checkpoint: Option<crate::FinalityHistoryCheckpoint>,
    pub(super) finality: Vec<FinalityRecord>,
    pub(super) failed_read: Option<AuditRead>,
}

impl AuditStore {
    fn check_read(&self, read: AuditRead) -> Result<(), StoreError> {
        if self.failed_read == Some(read) {
            Err(injected_store_error())
        } else {
            Ok(())
        }
    }

    fn visit_bounded<T: Clone>(
        &self,
        read: AuditRead,
        collection: StoreCollection,
        rows: &[T],
        limit: RowLimit,
        visitor: &mut dyn FnMut(T) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        if rows.len() > limit.get() {
            return Err(StoreError::LimitExceeded { collection, limit });
        }
        self.check_read(read)?;
        for row in rows {
            visitor(row.clone())?;
        }
        Ok(())
    }
}

impl StoreAuditRead for AuditStore {
    type Snapshot<'a> = &'a Self;

    fn audit_snapshot(&self) -> Result<Self::Snapshot<'_>, StoreError> {
        Ok(self)
    }
}

impl StoreAuditSnapshot for &AuditStore {
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        self.check_read(AuditRead::Snapshot)?;
        Ok(self.snapshot.clone())
    }

    fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        self.check_read(AuditRead::Metadata)?;
        Ok(self.metadata.clone())
    }

    fn visit_header_nodes(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(HeaderNode) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::HeaderNodes,
            StoreCollection::HeaderNodes,
            &self.nodes,
            limit,
            visitor,
        )
    }

    fn visit_consensus_invalid_body_tombstones(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(ConsensusInvalidBodyTombstone) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::Tombstones,
            StoreCollection::ConsensusInvalidBodyTombstones,
            &self.tombstones,
            limit,
            visitor,
        )
    }

    fn consensus_invalid_body_tombstone_count(&self) -> Result<usize, StoreError> {
        Ok(self.tombstones.len())
    }

    fn full_state_attests_to_body_validation_state(
        &self,
        _hash: block::Hash,
        _state: &BodyValidationState,
    ) -> Result<bool, StoreError> {
        self.check_read(AuditRead::BodyStateAuthority)?;
        Ok(self.body_state_authority)
    }

    fn visit_header_child_edges(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut((block::Hash, block::Hash)) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::HeaderChildEdges,
            StoreCollection::HeaderChildEdges,
            &self.children,
            limit,
            visitor,
        )
    }

    fn visit_selected_projection(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(Frontier) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::SelectedProjection,
            StoreCollection::SelectedProjection,
            &self.selected,
            limit,
            visitor,
        )
    }

    fn visit_verified_projection(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(Frontier) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::VerifiedProjection,
            StoreCollection::VerifiedProjection,
            &self.verified,
            limit,
            visitor,
        )
    }

    fn visit_deferred_entries(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut((DateTime<Utc>, block::Hash)) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::DeferredEntries,
            StoreCollection::DeferredHeaderEntries,
            &self.deferred,
            limit,
            visitor,
        )
    }

    fn visit_eligibility_roots(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut((block::Hash, EligibilityReason)) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::EligibilityRoots,
            StoreCollection::EligibilityReasonRoots,
            &self.reasons,
            limit,
            visitor,
        )
    }

    fn visit_aux_deliveries(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(UntrustedAuxDeliveryRow) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let rows: Vec<_> = self
            .aux
            .iter()
            .map(|delivery| {
                let status = match delivery.outcome().status() {
                    crate::AuxOutcomeStatus::Unauthenticated => 0,
                    crate::AuxOutcomeStatus::Authenticated => 1,
                    crate::AuxOutcomeStatus::Rejected => 2,
                    crate::AuxOutcomeStatus::Disputed => 3,
                };
                let observations = delivery
                    .observation_ids()
                    .map(|id| id.map(|id| id.digest()));
                let base = AuxDelivery::new(
                    delivery.delivery_id,
                    delivery.header_hash,
                    delivery.source,
                    delivery.owner,
                    delivery.body_size,
                    delivery.tree_aux,
                );
                UntrustedAuxDeliveryRow::new(
                    base,
                    status,
                    observations,
                    delivery.outcome_boundary_hash(),
                )
            })
            .collect();
        self.visit_bounded(
            AuditRead::AuxDeliveries,
            StoreCollection::AuxiliaryDeliveries,
            &rows,
            limit,
            visitor,
        )
    }

    fn visit_validation_context_records(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(ValidationContextRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::ValidationContexts,
            StoreCollection::ValidationContexts,
            &self.contexts,
            limit,
            visitor,
        )
    }

    fn authenticated_canonical_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        self.check_read(AuditRead::CanonicalHash)?;
        self.canonical_reads.fetch_add(1, Ordering::Relaxed);
        Ok(self.canonical.get(&height).copied())
    }

    fn visit_finality_history(
        &self,
        limit: RowLimit,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_bounded(
            AuditRead::FinalityHistory,
            StoreCollection::FinalityHistory,
            &self.finality,
            limit,
            visitor,
        )
    }

    fn finality_history_checkpoint(
        &self,
    ) -> Result<Option<crate::FinalityHistoryCheckpoint>, StoreError> {
        self.check_read(AuditRead::FinalityCheckpoint)?;
        Ok(self.finality_checkpoint)
    }

    fn finality_history_count(&self) -> Result<usize, StoreError> {
        self.check_read(AuditRead::FinalityCount)?;
        Ok(self.finality.len())
    }
}

pub(super) fn fixture() -> (AuditStore, EngineConfig) {
    let network = Network::new_regtest(RegtestParameters::default());
    let block = regtest_genesis_block();
    let anchor = Frontier::new(block::Height(0), block.hash());
    let config = EngineConfig::new(
        EngineMode::Integrated,
        network,
        TrustedAnchor {
            frontier: anchor,
            header: block.header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the audit fixture configuration is coherent");
    let anchor_work = block
        .header
        .difficulty_threshold
        .to_work()
        .expect("the fixture target has work");
    let anchor_node = HeaderNode::from_durable_parts(
        block.header.clone(),
        anchor.hash,
        block.header.previous_block_hash,
        anchor.height,
        anchor_work,
        WorkCoordinate::new(anchor.hash, anchor_work.as_u256()),
        HeaderValidationState::Valid,
        EligibilityState::default(),
        BodyValidationState::Unknown,
        Vec::new(),
    )
    .expect("the canonical anchor fields agree");
    let mut child_header = *block.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time += Duration::seconds(1);
    child_header.nonce = [1; 32].into();
    let child_header = Arc::new(child_header);
    let child_hash = child_header.hash();
    let child_work = child_header
        .difficulty_threshold
        .to_work()
        .expect("the fixture child target has work");
    let child = Frontier::new(block::Height(1), child_hash);
    let child_node = HeaderNode::from_durable_parts(
        child_header,
        child_hash,
        anchor.hash,
        child.height,
        child_work,
        anchor_node
            .work_coordinate()
            .checked_add(child_work)
            .expect("the fixture work fits"),
        HeaderValidationState::Valid,
        EligibilityState::default(),
        BodyValidationState::Unknown,
        Vec::new(),
    )
    .expect("the canonical child fields agree");
    let score = ChainScore::new(SuffixWork::new(child_work.as_u256()), child.hash);
    let metadata = EngineMetadata {
        disk_format: HeaderChainDiskVersion::CURRENT,
        mode: EngineMode::Integrated,
        network_id: config.network().kind(),
        network_policy_digest: config.network_policy_digest(),
        anchor_manifest_digest: config.trust_anchor_digest(),
        work_origin: anchor,
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(1),
        verified_generation: VerifiedGeneration::new(1),
        finality_epoch: FinalityEpoch::new(0),
        headers_only_migration_epoch: None,
        frontiers: FrontierSet {
            finalized: anchor,
            header_best: child,
            verified_best: anchor,
        },
        header_best_score: score,
        oldest_retained_height: anchor.height,
        alarms: AlarmSet::default(),
        last_transition: None,
    };
    (
        AuditStore {
            snapshot: metadata.snapshot(),
            metadata,
            nodes: vec![anchor_node, child_node],
            tombstones: Vec::new(),
            body_state_authority: true,
            children: vec![(anchor.hash, child.hash)],
            selected: vec![anchor, child],
            verified: vec![anchor],
            deferred: Vec::new(),
            reasons: Vec::new(),
            aux: Vec::new(),
            contexts: Vec::new(),
            canonical: HashMap::from([(anchor.height, anchor.hash), (child.height, child.hash)]),
            canonical_reads: Arc::new(AtomicUsize::new(0)),
            finality_checkpoint: None,
            finality: vec![FinalityRecord::full_state(
                anchor,
                anchor,
                FinalityEpoch::new(0),
            )],
            failed_read: None,
        },
        config,
    )
}

pub(super) fn injected_store_error() -> StoreError {
    StoreError::Unavailable("injected recovery audit read failure")
}

pub(super) fn violations(store: &AuditStore, config: &EngineConfig) -> Vec<AuditViolation> {
    match audit_store(store, config) {
        Err(RecoveryFailure::Source { violations }) => violations,
        other => panic!("expected source audit failure, got {other:?}"),
    }
}
