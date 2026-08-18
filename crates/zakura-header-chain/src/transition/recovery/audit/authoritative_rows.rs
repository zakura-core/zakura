//! Authoritative durable-row cross-checks (eligibility, aux, contexts, finality).

use std::collections::{HashMap, HashSet};

use zakura_chain::block;

use crate::{
    EngineConfig, EngineMetadata, EngineMode, FinalityRecord, FinalitySource, HeaderNode, RowLimit,
    StoreError,
};

use super::super::contracts::{AuditViolation, StoreAuditSnapshot, ValidationContextRecord};

pub(super) fn check_authoritative_rows<S: StoreAuditSnapshot>(
    store: &S,
    nodes: &[HeaderNode],
    deliveries: &[crate::AuxDelivery],
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
    let reason_limit = nodes
        .len()
        .checked_mul(crate::MAX_DIRECT_ELIGIBILITY_REASONS_V1)
        .ok_or(StoreError::Incoherent(
            "eligibility-reason recovery limit overflow",
        ))?;
    let mut actual = Vec::with_capacity(reason_limit.min(expected.len()));
    store.visit_eligibility_roots(RowLimit::new(reason_limit), &mut |reason| {
        actual.push(reason);
        Ok(())
    })?;
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
    if deliveries.len() > config.limits.max_aux_deliveries_total.get() {
        violations.push(AuditViolation::Limits);
    }
    let delivery_ids: HashSet<_> = deliveries.iter().map(|row| row.delivery_id).collect();
    if delivery_ids.len() != deliveries.len() {
        violations.push(AuditViolation::Auxiliary(block::Hash([0; 32])));
    }
    for delivery in deliveries {
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
    let mut durable_parent_cache = HashMap::new();
    let mut expected_witness_roots: HashMap<crate::Frontier, u32> = HashMap::new();
    let expected_history_count = store.finality_history_count()?;
    let mut history_count = 0_usize;
    let checkpoint = store.finality_history_checkpoint()?;
    if let Some(checkpoint) = checkpoint {
        if checkpoint.frontier.height > metadata.frontiers.finalized.height
            || checkpoint.frontier != config.bootstrap_anchor().frontier
                && store.authenticated_canonical_hash(checkpoint.frontier.height)?
                    != Some(checkpoint.frontier.hash)
        {
            violations.push(AuditViolation::Finality);
        }
    }
    let mut work_origin_seen = metadata.work_origin == config.bootstrap_anchor().frontier
        || checkpoint
            .is_some_and(|checkpoint| metadata.work_origin.height <= checkpoint.frontier.height);
    store.visit_finality_history(RowLimit::new(65_536), &mut |record| {
        history_count = history_count.saturating_add(1);
        first.get_or_insert(record);
        let source_matches = source_matches_mode(&record, metadata, config)
            && (!matches!(record.source, FinalitySource::FullState { .. })
                || store.authenticates_full_state_finality(
                    record,
                    config.bootstrap_anchor().frontier,
                )?);
        if previous.is_some_and(|previous: FinalityRecord| {
            previous.current != record.previous
                || previous.epoch.get().checked_add(1) != Some(record.epoch.get())
                || record.current.height <= record.previous.height
        }) || !source_matches
        {
            invalid_history = true;
        }
        if source_matches {
            let context_authenticates_current = contexts
                .binary_search_by_key(&record.current.height, |context| context.height)
                .is_ok_and(|index| contexts[index].header.hash() == record.current.hash);
            let current_is_authentic = record.current == metadata.frontiers.finalized
                || record.current == config.bootstrap_anchor().frontier
                || context_authenticates_current
                || store.authenticated_canonical_hash(record.current.height)?
                    == Some(record.current.hash);
            if !current_is_authentic {
                invalid_history = true;
            }
            if let Some(selected_tip) =
                record.headers_only_depth_witness(config.limits.local_finality_depth.get())
            {
                let root_references = expected_witness_roots.entry(selected_tip).or_default();
                *root_references = root_references.saturating_add(1);
                let canonical_witness = selected_tip.height <= metadata.frontiers.finalized.height
                    && store.authenticated_canonical_hash(selected_tip.height)?
                        == Some(selected_tip.hash);
                let retained_witness = selected_tip.height > metadata.frontiers.finalized.height
                    && witness_descends_to(&by_hash, selected_tip, metadata.frontiers.finalized);
                let durable_witness = if canonical_witness || retained_witness {
                    false
                } else {
                    historical_witness_descends_to(
                        store,
                        selected_tip,
                        record.current,
                        &mut durable_parent_cache,
                    )?
                };
                let witness_is_authentic = canonical_witness || retained_witness || durable_witness;
                if !witness_is_authentic {
                    invalid_history = true;
                }
            }
        }
        previous = Some(record);
        last = Some(record);
        // v3→v4 migration replaces history with one DiskMigration row at finalized.
        // Production nodes rebase work_origin to the init tip, which can sit below that frontier.
        work_origin_seen |= record.current == metadata.work_origin
            || matches!(record.source, FinalitySource::DiskMigration { .. })
                && metadata.work_origin.height <= record.current.height;
        Ok(())
    })?;
    let history_has_expected_count = history_count == expected_history_count;
    let history_has_valid_start = first.is_some_and(|record| {
        finality_history_starts_validly(record, checkpoint, config.bootstrap_anchor().frontier)
    });
    let migration_boundary_is_valid =
        metadata
            .headers_only_migration_epoch
            .is_none_or(|boundary| {
                metadata.mode == EngineMode::Integrated && boundary <= metadata.finality_epoch
            });
    let history_rows_are_valid = !invalid_history;
    let history_has_valid_end = last.is_some_and(|record| {
        record.current == metadata.frontiers.finalized && record.epoch == metadata.finality_epoch
    });
    let history_is_valid = history_has_expected_count
        && history_has_valid_start
        && migration_boundary_is_valid
        && history_rows_are_valid
        && history_has_valid_end;
    if !history_is_valid {
        violations.push(AuditViolation::Finality);
    }

    let expected_witness_count = store.finality_witness_count()?;
    let mut witness_rows = HashMap::with_capacity(expected_witness_count);
    store.visit_finality_witnesses(RowLimit::new(132_072), &mut |entry, roots, children| {
        let frontier = entry.frontier;
        let parent = crate::Frontier::new(
            block::Height(entry.frontier.height.0.saturating_sub(1)),
            entry.header.previous_block_hash,
        );
        if witness_rows
            .insert(frontier, (parent, roots, children))
            .is_some()
        {
            invalid_history = true;
        }
        Ok(())
    })?;
    if witness_rows.len() != expected_witness_count || witness_rows.len() > 132_072 {
        invalid_history = true;
    }
    let mut actual_children: HashMap<crate::Frontier, u32> = HashMap::new();
    for (frontier, (parent, roots, _)) in &witness_rows {
        if *roots != expected_witness_roots.get(frontier).copied().unwrap_or(0) {
            invalid_history = true;
        }
        if witness_rows.contains_key(parent) {
            let children = actual_children.entry(*parent).or_default();
            *children = children.saturating_add(1);
        }
    }
    if (expected_witness_count != 0
        && expected_witness_roots
            .keys()
            .any(|root| !witness_rows.contains_key(root)))
        || witness_rows.iter().any(|(frontier, (_, _, children))| {
            *children != actual_children.get(frontier).copied().unwrap_or(0)
        })
    {
        invalid_history = true;
    }
    if invalid_history && !violations.contains(&AuditViolation::Finality) {
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
    let settled = config.settled_manifest().pin_for_network(config.network());
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

fn historical_witness_descends_to<S: StoreAuditSnapshot>(
    store: &S,
    witness: crate::Frontier,
    frontier: crate::Frontier,
    parent_cache: &mut HashMap<crate::Frontier, Option<crate::Frontier>>,
) -> Result<bool, StoreError> {
    let mut cursor = witness;
    while cursor.height > frontier.height {
        let parent = if let Some(parent) = parent_cache.get(&cursor) {
            *parent
        } else {
            let parent = store.finality_witness_header(cursor)?.map(|row| {
                crate::Frontier::new(
                    block::Height(cursor.height.0 - 1),
                    row.header.previous_block_hash,
                )
            });
            parent_cache.insert(cursor, parent);
            parent
        };
        let Some(parent) = parent else {
            return Ok(false);
        };
        cursor = parent;
    }
    Ok(cursor == frontier)
}

/// Return true when the audited header rows prove `witness` descends to `frontier`.
///
/// Retention keeps the current finalized frontier and every descendant. The walk therefore
/// avoids a historical frontier that retention may have pruned.
fn witness_descends_to(
    by_hash: &HashMap<block::Hash, &HeaderNode>,
    witness: crate::Frontier,
    frontier: crate::Frontier,
) -> bool {
    let Some(mut node) = by_hash.get(&witness.hash).copied() else {
        return false;
    };
    if node.height != witness.height {
        return false;
    }
    while node.height > frontier.height {
        let Some(parent) = by_hash.get(&node.parent_hash).copied() else {
            return false;
        };
        // Reject a parent link that does not descend exactly one height, which also bounds
        // this walk on a store whose height rows contradict its parent rows.
        if parent.height.next().ok() != Some(node.height) {
            return false;
        }
        node = parent;
    }
    node.hash == frontier.hash && node.height == frontier.height
}

fn finality_history_starts_validly(
    record: FinalityRecord,
    checkpoint: Option<crate::FinalityHistoryCheckpoint>,
    bootstrap_frontier: crate::Frontier,
) -> bool {
    match checkpoint {
        Some(checkpoint) => {
            checkpoint.epoch.get().checked_add(1) == Some(record.epoch.get())
                && record.previous == checkpoint.frontier
        }
        None => {
            (record.epoch == crate::FinalityEpoch::new(0)
                && record.previous == bootstrap_frontier
                && (record.current.height > record.previous.height
                    || record.previous == record.current
                        && matches!(
                            record.source,
                            FinalitySource::FullState {
                                provenance: crate::FullStateFinalityProvenance {
                                    kind: crate::FullStateFinalityKind::Initialization,
                                    ..
                                }
                            } | FinalitySource::MigratedHeadersOnly
                        )))
                || (record.previous == record.current
                    && matches!(record.source, FinalitySource::DiskMigration { .. }))
        }
    }
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
        (EngineMode::HeadersOnly, None, FinalitySource::HeadersOnlyDepth { .. }) => record
            .headers_only_depth_witness(config.limits.local_finality_depth.get())
            .is_some(),
        (
            EngineMode::Integrated,
            _,
            FinalitySource::DiskMigration {
                from_version,
                network_policy_digest,
                authentication: crate::DiskMigrationAuthentication::FullState,
            },
        ) => {
            (1..=3).contains(&from_version.0)
                && network_policy_digest == metadata.network_policy_digest
                && record.previous == record.current
        }
        (
            EngineMode::HeadersOnly,
            None,
            FinalitySource::DiskMigration {
                from_version,
                network_policy_digest,
                authentication: crate::DiskMigrationAuthentication::HeadersOnlyDepth { .. },
            },
        ) => {
            (1..=3).contains(&from_version.0)
                && network_policy_digest == metadata.network_policy_digest
                && record.previous == record.current
                && record
                    .headers_only_depth_witness(config.limits.local_finality_depth.get())
                    .is_some()
        }
        _ => false,
    }
}
