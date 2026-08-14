//! Recovery policy tests.

mod authority;
mod repair;

use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};

use super::*;
use crate::{
    AlarmSet, AuxDelivery, BodyValidationState, ChainScore, CheckpointSet,
    ConsensusInvalidBodyTombstone, EligibilityReason, EligibilityState, EngineMetadata, EngineMode,
    EngineSnapshot, EvidenceId, FinalityEpoch, FinalityRecord, FinalitySource, Frontier,
    FrontierSet, HeaderChainDiskVersion, HeaderGeneration, HeaderNode, HeaderValidationState,
    StateVersion, StoreError, SuffixWork, TrustedAnchor, VerifiedGeneration, WorkCoordinate,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum AuditRead {
    Snapshot,
    Metadata,
    HeaderNodeCount,
    HeaderNodes,
    Tombstones,
    BodyStateAuthority,
    ChildEdges,
    SelectedProjection,
    VerifiedProjection,
    DeferredEntries,
    EligibilityRoots,
    AuxDeliveryCount,
    AuxDeliveries,
    ValidationContextCount,
    ValidationContexts,
    CanonicalHash,
    FinalityHistory,
}

impl AuditRead {
    pub(super) const ALL: [Self; 17] = [
        Self::Snapshot,
        Self::Metadata,
        Self::HeaderNodeCount,
        Self::HeaderNodes,
        Self::Tombstones,
        Self::BodyStateAuthority,
        Self::ChildEdges,
        Self::SelectedProjection,
        Self::VerifiedProjection,
        Self::DeferredEntries,
        Self::EligibilityRoots,
        Self::AuxDeliveryCount,
        Self::AuxDeliveries,
        Self::ValidationContextCount,
        Self::ValidationContexts,
        Self::CanonicalHash,
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
}

impl StoreAuditRead for AuditStore {
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        self.check_read(AuditRead::Snapshot)?;
        Ok(self.snapshot.clone())
    }

    fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        self.check_read(AuditRead::Metadata)?;
        Ok(self.metadata.clone())
    }

    fn header_node_count_up_to(&self, limit: usize) -> Result<usize, StoreError> {
        self.check_read(AuditRead::HeaderNodeCount)?;
        Ok(self.nodes.len().min(limit.saturating_add(1)))
    }

    fn all_header_nodes(&self) -> Result<Vec<HeaderNode>, StoreError> {
        self.check_read(AuditRead::HeaderNodes)?;
        Ok(self.nodes.clone())
    }

    fn all_consensus_invalid_body_tombstones(
        &self,
    ) -> Result<Vec<ConsensusInvalidBodyTombstone>, StoreError> {
        self.check_read(AuditRead::Tombstones)?;
        Ok(self.tombstones.clone())
    }

    fn full_state_attests_to_body_validation_state(
        &self,
        _hash: block::Hash,
        _state: &BodyValidationState,
    ) -> Result<bool, StoreError> {
        self.check_read(AuditRead::BodyStateAuthority)?;
        Ok(self.body_state_authority)
    }

    fn header_child_edges(&self) -> Result<Vec<(block::Hash, block::Hash)>, StoreError> {
        self.check_read(AuditRead::ChildEdges)?;
        Ok(self.children.clone())
    }

    fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        self.check_read(AuditRead::SelectedProjection)?;
        Ok(self.selected.clone())
    }

    fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        self.check_read(AuditRead::VerifiedProjection)?;
        Ok(self.verified.clone())
    }

    fn deferred_entries(&self) -> Result<Vec<(DateTime<Utc>, block::Hash)>, StoreError> {
        self.check_read(AuditRead::DeferredEntries)?;
        Ok(self.deferred.clone())
    }

    fn eligibility_roots(&self) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError> {
        self.check_read(AuditRead::EligibilityRoots)?;
        Ok(self.reasons.clone())
    }

    fn aux_delivery_count_up_to(&self, limit: usize) -> Result<usize, StoreError> {
        self.check_read(AuditRead::AuxDeliveryCount)?;
        Ok(self.aux.len().min(limit.saturating_add(1)))
    }

    fn all_aux_deliveries(&self) -> Result<Vec<AuxDelivery>, StoreError> {
        self.check_read(AuditRead::AuxDeliveries)?;
        Ok(self.aux.clone())
    }

    fn validation_context_count_up_to(&self, limit: usize) -> Result<usize, StoreError> {
        self.check_read(AuditRead::ValidationContextCount)?;
        Ok(self.contexts.len().min(limit.saturating_add(1)))
    }

    fn validation_context_records(&self) -> Result<Vec<ValidationContextRecord>, StoreError> {
        self.check_read(AuditRead::ValidationContexts)?;
        Ok(self.contexts.clone())
    }

    fn authenticated_canonical_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        self.check_read(AuditRead::CanonicalHash)?;
        Ok(self.canonical.get(&height).copied())
    }

    fn visit_finality_history(
        &self,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.check_read(AuditRead::FinalityHistory)?;
        for record in &self.finality {
            visitor(*record)?;
        }
        Ok(())
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
        disk_format: HeaderChainDiskVersion(1),
        mode: EngineMode::Integrated,
        network_id: config.network.kind(),
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
            finality: vec![FinalityRecord {
                previous: anchor,
                current: anchor,
                source: FinalitySource::FullState {
                    evidence: EvidenceId::from_digest([0x44; 32]),
                },
                epoch: FinalityEpoch::new(0),
            }],
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
