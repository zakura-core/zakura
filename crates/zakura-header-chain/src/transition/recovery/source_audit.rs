//! Fail-closed authoritative source audit.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use zakura_chain::block;

use crate::{
    BodyValidationState, EligibilityReason, EngineConfig, EngineMetadata, EngineMode,
    FinalityRecord, FinalitySource, Frontier, HeaderNode, StoreError,
};

use super::contracts::{
    violation_key, AuditViolation, RecoveryFailure, StoreAuditRead, ValidationContextRecord,
};
use super::model::{AuditedSource, StoreImage};

/// Audit every authoritative row and reject before any reconstruction.
pub(super) fn audit_authoritative<S: StoreAuditRead>(
    store: &S,
    image: StoreImage,
    config: &EngineConfig,
    now: DateTime<Utc>,
) -> Result<AuditedSource, RecoveryFailure> {
    let StoreImage {
        before,
        metadata,
        source_nodes,
        tombstones,
        validation_contexts,
        trust_anchor_changed,
        early_violations,
    } = image;
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
    if source_nodes.len().saturating_sub(1) > config.limits.max_non_finalized_nodes.get()
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
        before,
        metadata,
        source_nodes,
        tombstones,
        trust_anchor_changed,
    })
}

fn check_nodes(
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
        &config.network,
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
        &config.network,
        context,
    ) else {
        return false;
    };
    crate::validate_contextual_difficulty_and_time(node.header.difficulty_threshold, adjustment)
        .is_ok()
}

fn check_finalized_connectivity(
    nodes: &[HeaderNode],
    finalized: Frontier,
    violations: &mut Vec<AuditViolation>,
) {
    let mut connected = HashSet::from([finalized.hash]);
    for node in nodes {
        if node.hash == finalized.hash {
            continue;
        }
        if connected.contains(&node.parent_hash) {
            connected.insert(node.hash);
        } else {
            violations.push(AuditViolation::ProtectedPath(node.hash));
        }
    }
}

fn check_trust_pins(
    nodes: &[HeaderNode],
    finalized: Frontier,
    config: &EngineConfig,
    violations: &mut Vec<AuditViolation>,
) {
    let settled = config.settled_manifest().pin_for_network(&config.network);
    for node in nodes {
        for reason in &node.eligibility.direct_reasons {
            let valid = match reason {
                EligibilityReason::SettledUpgradeConflict { height, expected } => settled
                    .is_some_and(|pin| {
                        *height == node.height
                            && pin.activation == Frontier::new(*height, *expected)
                            && node.hash != *expected
                    }),
                EligibilityReason::CheckpointConflict { height, expected } => config
                    .local_checkpoints()
                    .hash(*height)
                    .is_some_and(|configured| {
                        configured == *expected && *height == node.height && node.hash != *expected
                    }),
                EligibilityReason::FinalityConflict {
                    finalized: reason_finalized,
                } => {
                    *reason_finalized == finalized
                        && node.height <= reason_finalized.height
                        && node.hash != reason_finalized.hash
                }
                EligibilityReason::ConsensusBodyInvalid { evidence, rule } => matches!(
                    &node.body_validation_state,
                    BodyValidationState::ConsensusInvalid {
                        evidence: body_evidence,
                        rule: body_rule,
                    } if body_evidence == evidence && body_rule == rule
                ),
                EligibilityReason::OperatorInvalid {
                    id, reason_digest, ..
                } => {
                    let mut hasher = sha2::Sha256::new();
                    use sha2::Digest as _;
                    hasher.update(b"zakura-operator-invalidation-v1");
                    hasher.update(node.hash.0);
                    hasher.update(id.bytes());
                    let digest: [u8; 32] = hasher.finalize().into();
                    *reason_digest == digest
                }
            };
            if !valid {
                violations.push(AuditViolation::EligibilityRoot(node.hash));
            }
        }
        let expected = if settled.is_some_and(|pin| pin.activation.height == node.height) {
            settled.map(|pin| (pin.activation.hash, true))
        } else {
            config
                .local_checkpoints()
                .hash(node.height)
                .map(|hash| (hash, false))
        };
        let Some((expected, settled_reason)) = expected else {
            continue;
        };
        let reason = node
            .eligibility
            .direct_reasons
            .iter()
            .any(|reason| match reason {
                EligibilityReason::SettledUpgradeConflict {
                    height,
                    expected: hash,
                } if settled_reason => *height == node.height && *hash == expected,
                EligibilityReason::CheckpointConflict {
                    height,
                    expected: hash,
                } if !settled_reason => *height == node.height && *hash == expected,
                _ => false,
            });
        if (node.hash == expected && reason) || (node.hash != expected && !reason) {
            violations.push(AuditViolation::TrustPin(node.height, node.hash));
        }
    }
}

fn check_authoritative_rows<S: StoreAuditRead>(
    store: &S,
    nodes: &[HeaderNode],
    validation_contexts: &[ValidationContextRecord],
    metadata: &EngineMetadata,
    config: &EngineConfig,
    violations: &mut Vec<AuditViolation>,
) -> Result<(), StoreError> {
    let mut expected: Vec<_> = nodes
        .iter()
        .flat_map(|node| {
            node.eligibility
                .direct_reasons
                .iter()
                .cloned()
                .map(move |reason| (node.hash, reason))
        })
        .collect();
    let mut actual = store.eligibility_roots()?;
    expected.sort_by_key(|(hash, reason)| (hash.0, reason.clone()));
    actual.sort_by_key(|(hash, reason)| (hash.0, reason.clone()));
    if expected != actual {
        let hash = expected
            .iter()
            .zip(&actual)
            .find(|(left, right)| left != right)
            .map(|(left, _)| left.0)
            .or_else(|| {
                expected
                    .get(actual.len())
                    .or_else(|| actual.get(expected.len()))
                    .map(|(hash, _)| *hash)
            })
            .unwrap_or(block::Hash([0; 32]));
        violations.push(AuditViolation::EligibilityRoot(hash));
    }

    let by_hash: HashMap<_, _> = nodes.iter().map(|node| (node.hash, node)).collect();
    let deliveries = store.all_aux_deliveries()?;
    if deliveries.len() > config.limits.max_aux_deliveries_total.get() {
        violations.push(AuditViolation::Limits);
    }
    let delivery_ids: HashSet<_> = deliveries.iter().map(|row| row.delivery_id).collect();
    if delivery_ids.len() != deliveries.len() {
        violations.push(AuditViolation::Auxiliary(block::Hash([0; 32])));
    }
    for delivery in &deliveries {
        if by_hash
            .get(&delivery.header_hash)
            .is_none_or(|node| !node.aux_delivery_ids.contains(&delivery.delivery_id))
        {
            violations.push(AuditViolation::Auxiliary(delivery.header_hash));
        }
    }
    for node in nodes {
        let node_ids: HashSet<_> = node.aux_delivery_ids.iter().copied().collect();
        if node.aux_delivery_ids.len() > config.limits.max_aux_deliveries_per_header.get() {
            violations.push(AuditViolation::Limits);
        }
        if node_ids.len() != node.aux_delivery_ids.len()
            || node_ids.iter().any(|id| !delivery_ids.contains(id))
        {
            violations.push(AuditViolation::Auxiliary(node.hash));
        }
    }

    let mut contexts = validation_contexts.to_vec();
    contexts.sort_unstable_by_key(|record| record.height);
    let predecessor_span = u32::try_from(crate::POW_PREDECESSOR_CONTEXT_SPAN)
        .map_err(|_| StoreError::Incoherent("validation context bound does not fit in u32"))?;
    let required_contexts = usize::try_from(
        metadata.frontiers.finalized.height.0.min(predecessor_span),
    )
    .map_err(|_| StoreError::Incoherent("validation context bound does not fit in usize"))?;
    if contexts.len() != required_contexts {
        violations.push(AuditViolation::ValidationContext(
            metadata.frontiers.finalized.hash,
        ));
    }
    for pair in contexts.windows(2) {
        if pair[0].height.next().ok() != Some(pair[1].height)
            || pair[1].header.previous_block_hash != pair[0].header.hash()
        {
            violations.push(AuditViolation::ValidationContext(pair[1].header.hash()));
        }
    }
    if let (Some(last), Some(finalized_node)) = (
        contexts.last(),
        by_hash.get(&metadata.frontiers.finalized.hash),
    ) {
        if last.height.next().ok() != Some(finalized_node.height)
            || finalized_node.header.previous_block_hash != last.header.hash()
        {
            violations.push(AuditViolation::ValidationContext(last.header.hash()));
        }
    }

    let mut previous = None;
    let mut first = None;
    let mut last = None;
    let mut invalid_history = false;
    let mut work_origin_seen = metadata.work_origin == config.bootstrap_anchor().frontier;
    store.visit_finality_history(&mut |record| {
        first.get_or_insert(record);
        if previous.is_some_and(|previous: FinalityRecord| {
            previous.current != record.previous
                || previous.epoch.get().checked_add(1) != Some(record.epoch.get())
                || record.current.height <= record.previous.height
        }) || !source_matches_mode(&record, metadata.mode, config)
        {
            invalid_history = true;
        }
        previous = Some(record);
        last = Some(record);
        work_origin_seen |= record.current == metadata.work_origin;
        Ok(())
    })?;
    if first.is_none_or(|record| {
        record.epoch != crate::FinalityEpoch::new(0)
            || record.previous != config.bootstrap_anchor().frontier
            || record.current.height < record.previous.height
            || record.current.height == record.previous.height && record.current != record.previous
    }) || invalid_history
        || last.is_some_and(|record| {
            record.current != metadata.frontiers.finalized
                || record.epoch != metadata.finality_epoch
        })
        || last.is_none()
    {
        violations.push(AuditViolation::Finality);
    }
    let work_origin_is_authenticated = metadata.work_origin == config.bootstrap_anchor().frontier
        || store.authenticated_canonical_hash(metadata.work_origin.height)?
            == Some(metadata.work_origin.hash);
    if !work_origin_seen || !work_origin_is_authenticated {
        violations.push(AuditViolation::Configuration);
    }

    let finalized = metadata.frontiers.finalized;
    if finalized != config.bootstrap_anchor().frontier
        && store.authenticated_canonical_hash(finalized.height)? != Some(finalized.hash)
    {
        violations.push(AuditViolation::Finality);
    }
    let settled = config.settled_manifest().pin_for_network(&config.network);
    let pins = config
        .local_checkpoints()
        .iter()
        .chain(settled.into_iter().map(|pin| pin.activation));
    for pin in pins.filter(|pin| pin.height <= metadata.frontiers.finalized.height) {
        if store.authenticated_canonical_hash(pin.height)? != Some(pin.hash) {
            violations.push(AuditViolation::TrustPin(pin.height, pin.hash));
        }
    }
    Ok(())
}

fn source_matches_mode(record: &FinalityRecord, mode: EngineMode, config: &EngineConfig) -> bool {
    match (mode, record.source) {
        (EngineMode::Integrated, FinalitySource::FullState { .. })
        | (_, FinalitySource::MigratedHeadersOnly) => true,
        (EngineMode::HeadersOnly, FinalitySource::HeadersOnlyDepth { selected_tip }) => {
            record.current.height > record.previous.height
                && selected_tip
                    .height
                    .0
                    .saturating_sub(record.current.height.0)
                    == config.limits.local_finality_depth.get()
        }
        _ => false,
    }
}
