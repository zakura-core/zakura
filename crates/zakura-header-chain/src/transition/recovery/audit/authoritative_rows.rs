//! Authoritative durable-row cross-checks (eligibility, aux, contexts, finality).

use std::collections::{HashMap, HashSet};

use zakura_chain::block;

use crate::{
    EngineConfig, EngineMetadata, EngineMode, FinalityRecord, FinalitySource, HeaderNode,
    StoreError,
};

use super::super::contracts::{AuditViolation, StoreAuditRead, ValidationContextRecord};

pub(super) fn check_authoritative_rows<S: StoreAuditRead>(
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
        }) || !source_matches_mode(&record, metadata, config)
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
    }) || metadata
        .headers_only_migration_epoch
        .is_some_and(|boundary| {
            metadata.mode != EngineMode::Integrated || boundary > metadata.finality_epoch
        })
        || invalid_history
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

fn source_matches_mode(
    record: &FinalityRecord,
    metadata: &EngineMetadata,
    config: &EngineConfig,
) -> bool {
    match (
        metadata.mode,
        metadata.headers_only_migration_epoch,
        record.source,
    ) {
        (EngineMode::Integrated, None, FinalitySource::FullState { .. }) => true,
        (EngineMode::Integrated, Some(boundary), FinalitySource::MigratedHeadersOnly) => {
            record.epoch <= boundary
        }
        (EngineMode::Integrated, Some(boundary), FinalitySource::FullState { .. }) => {
            record.epoch > boundary
        }
        (EngineMode::HeadersOnly, None, FinalitySource::MigratedHeadersOnly) => {
            record.epoch == crate::FinalityEpoch::new(0)
        }
        (EngineMode::HeadersOnly, None, FinalitySource::HeadersOnlyDepth { selected_tip }) => {
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
