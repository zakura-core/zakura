//! Fail-closed authoritative source audit.

mod authoritative_rows;
mod connectivity;
mod nodes;
mod trust_pins;

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::{
    transition::engine::validate_recovered_auxiliary_rows, BodyValidationState, EngineConfig,
    HeaderGraphReconstruction, MemHeaderStore, RowLimit,
};

use super::contracts::{violation_key, AuditViolation, RecoveryFailure, StoreAuditSnapshot};
use super::phases::{AuditedSource, PreAuditStoreRows};

use authoritative_rows::check_authoritative_rows;
use connectivity::check_finalized_connectivity;
use nodes::check_nodes;
use trust_pins::check_trust_pins;

/// Audit every authoritative row and reject before any reconstruction.
pub(super) fn audit_authoritative<S: StoreAuditSnapshot>(
    store: &S,
    rows: PreAuditStoreRows,
    config: &EngineConfig,
    now: DateTime<Utc>,
) -> Result<AuditedSource, RecoveryFailure> {
    let PreAuditStoreRows {
        snapshot_before_repair,
        metadata,
        source_header_nodes,
        consensus_invalid_body_tombstones,
        validation_contexts,
        trust_anchor_changed,
        early_violations,
    } = rows;
    let mut violations = early_violations;
    let by_hash: HashMap<_, _> = source_header_nodes
        .iter()
        .map(|header_node| (header_node.hash, header_node))
        .collect();
    let archived_contexts: HashMap<_, _> = validation_contexts
        .iter()
        .map(|record| (record.header.hash(), record.header.as_ref()))
        .collect();
    let finalized = metadata.frontiers.finalized;
    if by_hash
        .get(&finalized.hash)
        .is_none_or(|node| node.height != finalized.height)
    {
        violations.push(AuditViolation::ProtectedPath(finalized.hash));
    }
    let tombstones_by_hash: HashMap<_, _> = consensus_invalid_body_tombstones
        .iter()
        .map(|tombstone| (tombstone.hash, tombstone))
        .collect();
    check_nodes(
        &source_header_nodes,
        &by_hash,
        &archived_contexts,
        &metadata,
        config,
        now,
        &mut violations,
    );
    for node in &source_header_nodes {
        match (
            &node.body_validation_state,
            tombstones_by_hash.get(&node.hash),
        ) {
            (BodyValidationState::ConsensusInvalid { evidence, rule }, Some(tombstone))
                if *evidence == tombstone.evidence
                    && *rule == tombstone.rule
                    && node.height == tombstone.height => {}
            (BodyValidationState::ConsensusInvalid { .. }, _) | (_, Some(_)) => {
                violations.push(AuditViolation::ConsensusInvalidBodyTombstone(node.hash));
            }
            (_, None) => {}
        }
    }
    for tombstone in &consensus_invalid_body_tombstones {
        if tombstone.height <= finalized.height {
            violations.push(AuditViolation::ConsensusInvalidBodyTombstone(
                tombstone.hash,
            ));
        }
    }
    check_finalized_connectivity(&source_header_nodes, finalized, &mut violations);
    check_trust_pins(&source_header_nodes, finalized, config, &mut violations);
    let maximum_aux = config.limits.max_aux_deliveries_total.get();
    let mut untrusted_deliveries = Vec::with_capacity(maximum_aux.min(source_header_nodes.len()));
    store.visit_aux_deliveries(RowLimit::new(maximum_aux), &mut |delivery| {
        untrusted_deliveries.push(delivery);
        Ok(())
    })?;
    let recovered_deliveries = if untrusted_deliveries.is_empty() {
        Some(Vec::new())
    } else {
        MemHeaderStore::reconstruct(HeaderGraphReconstruction::new(
            finalized,
            source_header_nodes.clone(),
            consensus_invalid_body_tombstones.clone(),
        ))
        .ok()
        .and_then(|graph| validate_recovered_auxiliary_rows(&graph, untrusted_deliveries).ok())
    };
    if recovered_deliveries.is_none() {
        violations.push(AuditViolation::Auxiliary(zakura_chain::block::Hash(
            [0; 32],
        )));
    }
    check_authoritative_rows(
        store,
        &source_header_nodes,
        recovered_deliveries.as_deref().unwrap_or_default(),
        &validation_contexts,
        &metadata,
        config,
        &mut violations,
    )?;
    if source_header_nodes.len().saturating_sub(1) > config.limits.max_non_finalized_nodes.get()
        && !metadata.alarms.resource_stalled
    {
        violations.push(AuditViolation::Limits);
    }
    violations.sort_by_key(violation_key);
    violations.dedup();
    if !violations.is_empty() {
        return Err(RecoveryFailure::Source { violations });
    }
    Ok(AuditedSource {
        snapshot_before_repair,
        metadata,
        source_header_nodes,
        consensus_invalid_body_tombstones,
        trust_anchor_changed,
    })
}
