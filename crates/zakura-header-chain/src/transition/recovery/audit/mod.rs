//! Fail-closed authoritative source audit.

mod authoritative_rows;
mod connectivity;
mod nodes;
mod trust_pins;

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::{BodyValidationState, EngineConfig};

use super::contracts::{violation_key, AuditViolation, RecoveryFailure, StoreAuditRead};
use super::phases::{AuditedSource, PreAuditStoreRows};

use authoritative_rows::check_authoritative_rows;
use connectivity::check_finalized_connectivity;
use nodes::check_nodes;
use trust_pins::check_trust_pins;

/// Audit every authoritative row and reject before any reconstruction.
pub(super) fn audit_authoritative<S: StoreAuditRead>(
    store: &S,
    rows: PreAuditStoreRows,
    config: &EngineConfig,
    now: DateTime<Utc>,
) -> Result<AuditedSource, RecoveryFailure> {
    let PreAuditStoreRows {
        snapshot_before_repair,
        metadata,
        source_nodes,
        tombstones,
        validation_contexts,
        trust_anchor_changed,
        early_violations,
    } = rows;
    let mut violations = early_violations;
    let by_hash: HashMap<_, _> = source_nodes.iter().map(|node| (node.hash, node)).collect();
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
    let tombstones_by_hash: HashMap<_, _> = tombstones
        .iter()
        .map(|tombstone| (tombstone.hash, tombstone))
        .collect();
    check_nodes(
        &source_nodes,
        &by_hash,
        &archived_contexts,
        &metadata,
        config,
        now,
        &mut violations,
    );
    for node in &source_nodes {
        match (
            &node.body_validation_state,
            tombstones_by_hash.get(&node.hash),
        ) {
            (BodyValidationState::ConsensusInvalid { evidence, rule }, Some(tombstone))
                if *evidence == tombstone.evidence && *rule == tombstone.rule => {}
            (BodyValidationState::ConsensusInvalid { .. }, _) | (_, Some(_)) => {
                violations.push(AuditViolation::ConsensusInvalidBodyTombstone(node.hash));
            }
            (_, None) => {}
        }
    }
    check_finalized_connectivity(&source_nodes, finalized, &mut violations);
    check_trust_pins(&source_nodes, finalized, config, &mut violations);
    check_authoritative_rows(
        store,
        &source_nodes,
        &validation_contexts,
        &metadata,
        config,
        &mut violations,
    )?;
    if source_nodes.len().saturating_sub(1) > config.limits.max_non_finalized_nodes.get() {
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
        source_nodes,
        tombstones,
        trust_anchor_changed,
    })
}
