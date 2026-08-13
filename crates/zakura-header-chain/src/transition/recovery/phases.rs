//! Private phase values that enforce the recovery pipeline order.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use zakura_chain::block;

use crate::{
    BodyValidationState, ChainScore, ConsensusInvalidBodyTombstone, EngineConfig, EngineMetadata,
    EngineSnapshot, Frontier, HeaderNode, StoreError,
};

use super::contracts::{AuditViolation, StoreAuditRead, ValidationContextRecord};

/// Exhaustive durable rows loaded before any authoritative audit.
pub(super) struct PreAuditStoreRows {
    pub(super) snapshot_before_repair: EngineSnapshot,
    pub(super) metadata: EngineMetadata,
    pub(super) source_nodes: Vec<HeaderNode>,
    pub(super) tombstones: Vec<ConsensusInvalidBodyTombstone>,
    pub(super) validation_contexts: Vec<ValidationContextRecord>,
    pub(super) trust_anchor_changed: bool,
    pub(super) early_violations: Vec<AuditViolation>,
}

/// Authoritative source that passed fail-closed audit.
pub(super) struct AuditedSource {
    pub(super) snapshot_before_repair: EngineSnapshot,
    pub(super) metadata: EngineMetadata,
    pub(super) source_nodes: Vec<HeaderNode>,
    pub(super) tombstones: Vec<ConsensusInvalidBodyTombstone>,
    pub(super) trust_anchor_changed: bool,
}

/// Deterministic derived views reconstructed only from an audited source.
pub(super) struct ReconstructedDerivedViews {
    /// Source rows after elapsed-deferral promotion and before eligibility recompute.
    ///
    /// Repair classification compares this image to [`Self::header_nodes`] so promotion
    /// itself does not look like an inherited-eligibility cache repair.
    pub(super) promoted_source_nodes: Vec<HeaderNode>,
    pub(super) header_nodes: Vec<HeaderNode>,
    pub(super) header_child_edges: Vec<(block::Hash, block::Hash)>,
    pub(super) selected_projection: Vec<Frontier>,
    pub(super) verified_projection: Vec<Frontier>,
    pub(super) deferred_entries: Vec<(DateTime<Utc>, block::Hash)>,
    pub(super) selected_tip: Frontier,
    pub(super) selected_score: ChainScore,
    pub(super) elapsed_deferrals: bool,
    pub(super) oldest_retained_height: block::Height,
    pub(super) body_unavailable_alarm: Option<crate::BodyUnavailableSummary>,
}

/// Load the complete durable rows used by startup audit.
pub(super) fn load_pre_audit_store_rows<S: StoreAuditRead>(
    store: &S,
    config: &EngineConfig,
    allow_trust_anchor_update: bool,
) -> Result<PreAuditStoreRows, StoreError> {
    let snapshot_before_repair = store.snapshot()?;
    let metadata = store.metadata()?;
    let mut early_violations = Vec::new();
    let trust_anchor_changed = metadata.anchor_manifest_digest != config.trust_anchor_digest();
    if snapshot_before_repair != metadata.snapshot()
        || metadata.disk_format.0 != 1
        || metadata.mode != config.mode
        || metadata.network_id != config.network.kind()
        || trust_anchor_changed && !allow_trust_anchor_update
    {
        early_violations.push(AuditViolation::Configuration);
    }

    let mut source_nodes = store.all_header_nodes()?;
    let tombstones = store.all_consensus_invalid_body_tombstones()?;
    let mut tombstone_hashes = HashSet::new();
    for tombstone in &tombstones {
        if !tombstone_hashes.insert(tombstone.hash) {
            early_violations.push(AuditViolation::ConsensusInvalidBodyTombstone(
                tombstone.hash,
            ));
        }
    }
    for tombstone in &tombstones {
        let state = BodyValidationState::ConsensusInvalid {
            evidence: tombstone.evidence,
            rule: tombstone.rule.clone(),
        };
        if !store.full_state_attests_to_body_validation_state(tombstone.hash, &state)? {
            early_violations.push(AuditViolation::BodyValidationEvidenceAuthority(
                tombstone.hash,
            ));
        }
    }
    source_nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    let mut unique = HashSet::new();
    for node in &source_nodes {
        if !unique.insert(node.hash) || node.header.hash() != node.hash {
            early_violations.push(AuditViolation::NodeHash(node.hash));
        }
        if matches!(
            node.body_validation_state,
            BodyValidationState::Verified { .. } | BodyValidationState::ConsensusInvalid { .. }
        ) && !store
            .full_state_attests_to_body_validation_state(node.hash, &node.body_validation_state)?
        {
            early_violations.push(AuditViolation::BodyValidationEvidenceAuthority(node.hash));
        }
    }
    let validation_contexts = store.validation_context_records()?;
    Ok(PreAuditStoreRows {
        snapshot_before_repair,
        metadata,
        source_nodes,
        tombstones,
        validation_contexts,
        trust_anchor_changed,
        early_violations,
    })
}
