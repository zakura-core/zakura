//! Authoritative node consensus, work, parent, and time checks.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use zakura_chain::block;

use crate::{EngineConfig, EngineMetadata, HeaderNode};

use super::super::contracts::AuditViolation;

pub(super) fn check_nodes(
    nodes: &[HeaderNode],
    by_hash: &HashMap<block::Hash, &HeaderNode>,
    archived_contexts: &HashMap<block::Hash, &block::Header>,
    metadata: &EngineMetadata,
    config: &EngineConfig,
    now: DateTime<Utc>,
    violations: &mut Vec<AuditViolation>,
) {
    for node in nodes {
        if node.hash != metadata.frontiers.finalized.hash
            && !header_consensus_is_valid(node, by_hash, archived_contexts, config)
        {
            violations.push(AuditViolation::HeaderValidation(node.hash));
        }
        if node.header.difficulty_threshold.to_work() != Some(node.block_work) {
            violations.push(AuditViolation::Work(node.hash));
        }
        if node.work_coordinate().origin_hash() != metadata.work_origin.hash {
            violations.push(AuditViolation::Work(node.hash));
        }
        if node.hash == metadata.frontiers.finalized.hash {
            if node.eligibility.inherited_from.is_some() {
                violations.push(AuditViolation::Parent(node.hash));
            }
        } else if let Some(parent) = by_hash.get(&node.parent_hash) {
            if parent.height.next().ok() != Some(node.height)
                || node.header.previous_block_hash != parent.hash
            {
                violations.push(AuditViolation::Parent(node.hash));
            }
            if parent.work_coordinate().checked_add(node.block_work).ok()
                != Some(node.work_coordinate())
            {
                violations.push(AuditViolation::Work(node.hash));
            }
        } else {
            violations.push(AuditViolation::Parent(node.hash));
        }
        let future_limit = now.checked_add_signed(Duration::hours(2));
        let expected_deferred = node.header.time.checked_sub_signed(Duration::hours(2));
        let valid_time_state = match node.validation {
            crate::HeaderValidationState::Valid => {
                future_limit.is_some_and(|limit| node.header.time <= limit)
            }
            crate::HeaderValidationState::DeferredUntil(until) => expected_deferred == Some(until),
        };
        if !valid_time_state {
            violations.push(AuditViolation::HeaderValidation(node.hash));
        }
    }
}

fn header_consensus_is_valid(
    node: &HeaderNode,
    by_hash: &HashMap<block::Hash, &HeaderNode>,
    archived_contexts: &HashMap<block::Hash, &block::Header>,
    config: &EngineConfig,
) -> bool {
    if crate::validation::validate_trusted_anchor_observables(
        &node.header,
        config.network(),
        node.height,
    ) != Ok(node.hash)
    {
        return false;
    }
    let Ok(parent_height) = node.height.previous() else {
        return false;
    };
    let required = usize::try_from(node.height.0)
        .unwrap_or(usize::MAX)
        .min(crate::POW_ADJUSTMENT_BLOCK_SPAN);
    let mut hash = node.parent_hash;
    let mut context = Vec::with_capacity(required);
    while context.len() < required {
        let header = if let Some(predecessor) = by_hash.get(&hash) {
            predecessor.header.as_ref()
        } else if let Some(predecessor) = archived_contexts.get(&hash) {
            *predecessor
        } else {
            return false;
        };
        context.push((header.difficulty_threshold, header.time));
        hash = header.previous_block_hash;
    }
    let Ok(adjustment) = crate::AdjustedDifficulty::new_from_header_time(
        node.header.time,
        parent_height,
        config.network(),
        context,
    ) else {
        return false;
    };
    crate::validate_contextual_difficulty_and_time(node.header.difficulty_threshold, adjustment)
        .is_ok()
}
