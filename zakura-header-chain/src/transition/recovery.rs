//! Exhaustive startup audit and deterministic reconstruction planning.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelivery, BodyValidationState, CounterExhausted, EligibilityReason, EngineConfig,
    EngineMetadata, EngineMode, EngineSnapshot, FinalityRecord, FinalitySource, Frontier,
    HeaderNode, MemHeaderStore, StoreError,
};

/// One immutable predecessor record stored below the selectable finalized anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationContextRecord {
    /// Canonical context header, including its backward link.
    pub header: Arc<block::Header>,
    /// Authenticated context height.
    pub height: block::Height,
}

/// Complete exhaustive row/index view used only while publication is disabled.
pub trait StoreAuditRead {
    /// Return the atomic externally meaningful snapshot.
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError>;
    /// Return complete singleton metadata from the same version.
    fn metadata(&self) -> Result<EngineMetadata, StoreError>;
    /// Every node row, including disconnected rows.
    fn all_nodes(&self) -> Result<Vec<HeaderNode>, StoreError>;
    /// Every persisted parent/child edge.
    fn child_edges(&self) -> Result<Vec<(block::Hash, block::Hash)>, StoreError>;
    /// Complete selected projection.
    fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError>;
    /// Complete verified projection.
    fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError>;
    /// Complete deferred-time index.
    fn deferred_entries(&self) -> Result<Vec<(DateTime<Utc>, block::Hash)>, StoreError>;
    /// Every authoritative direct-reason root.
    fn eligibility_roots(&self) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError>;
    /// Every auxiliary delivery, including dangling rows.
    fn all_aux_deliveries(&self) -> Result<Vec<AuxDelivery>, StoreError>;
    /// Every immutable below-finalized context row.
    fn validation_context_records(&self) -> Result<Vec<ValidationContextRecord>, StoreError>;
    /// Visit append-only finality provenance in ascending epoch order.
    fn visit_finality_history(
        &self,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError>;
}

/// Stable exhaustive-audit violation categories.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuditViolation {
    /// Canonical header and stored hash disagreed.
    NodeHash(block::Hash),
    /// A non-anchor node had no exact height-minus-one parent.
    Parent(block::Hash),
    /// Exact cumulative work did not equal parent plus block work.
    Work(block::Hash),
    /// Body invalidity and direct eligibility evidence disagreed.
    BodyEligibility(block::Hash),
    /// Header validation state contradicted deterministic header facts.
    HeaderValidation(block::Hash),
    /// A trust pin was absent or lacked its exact conflict reason.
    TrustPin(block::Height, block::Hash),
    /// Authoritative reason roots disagreed with node source rows.
    EligibilityRoot(block::Hash),
    /// Auxiliary provenance or a node foreign key was invalid.
    Auxiliary(block::Hash),
    /// Immutable validation context was malformed or discontinuous.
    ValidationContext(block::Hash),
    /// Finality history contradicted finalized metadata.
    Finality,
    /// Mode, network, manifest, schema, or snapshot contradicted configuration.
    Configuration,
    /// A protected source path was absent or discontinuous.
    ProtectedPath(block::Hash),
    /// Authoritative rows exceeded frozen limits without the permitted alarm.
    Limits,
}

/// Reconstructible categories replaced by one atomic recovery transaction.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RecoveryRepair {
    /// Parent/child adjacency differed from source nodes.
    ChildIndex,
    /// Future-time index differed from node states.
    DeferredIndex,
    /// Selected projection/frontier differed from recomputation.
    SelectedProjection,
    /// Verified projection differed from its authoritative frontier.
    VerifiedProjection,
    /// Cached inherited eligibility differed from ancestry.
    InheritedEligibility,
    /// Oldest-retained metadata differed from source nodes.
    RetentionMetadata,
    /// Selected-tip body-unavailability alarm differed from its durable node.
    BodyAvailabilityAlarm,
}

/// Exact source-derived state to install before startup publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryPlan {
    /// Snapshot observed before repair.
    pub before: EngineSnapshot,
    /// Corrected metadata with counters advanced exactly once when required.
    pub metadata: EngineMetadata,
    /// Nodes with reconstructed inherited eligibility caches.
    pub nodes: Vec<HeaderNode>,
    /// Complete expected adjacency index.
    pub child_edges: Vec<(block::Hash, block::Hash)>,
    /// Complete selected projection.
    pub selected_projection: Vec<Frontier>,
    /// Complete verified projection.
    pub verified_projection: Vec<Frontier>,
    /// Complete deferred index.
    pub deferred_entries: Vec<(DateTime<Utc>, block::Hash)>,
    /// Exact repairs, empty for a coherent store.
    pub repairs: BTreeSet<RecoveryRepair>,
}

impl RecoveryPlan {
    /// Return true when startup may publish without a repair transaction.
    pub fn is_clean(&self) -> bool {
        self.repairs.is_empty()
    }
}

/// Startup audit failed before publication became available.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum RecoveryFailure {
    /// Exhaustive rows could not be read.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Authoritative source invariants failed.
    #[error("authoritative header-chain source rows failed startup audit")]
    Source {
        /// Deterministically ordered violations.
        violations: Vec<AuditViolation>,
    },
    /// A repair-required monotonic counter was exhausted.
    #[error(transparent)]
    Counter(#[from] CounterExhausted),
}

/// Audit authoritative rows and derive only reconstructible repairs.
pub fn audit_store<S: StoreAuditRead>(
    store: &S,
    config: &EngineConfig,
) -> Result<RecoveryPlan, RecoveryFailure> {
    let before = store.snapshot()?;
    let mut metadata = store.metadata()?;
    let mut violations = Vec::new();
    if before != metadata.snapshot()
        || metadata.disk_format.0 != 1
        || metadata.mode != config.mode
        || metadata.network_id != config.network.kind()
        || metadata.anchor_manifest_digest != config.trust_anchor_digest()
        || metadata.work_origin != config.bootstrap_anchor.frontier
    {
        violations.push(AuditViolation::Configuration);
    }

    let mut source_nodes = store.all_nodes()?;
    source_nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    let mut unique = HashSet::new();
    for node in &source_nodes {
        if !unique.insert(node.hash) || node.header.hash() != node.hash {
            violations.push(AuditViolation::NodeHash(node.hash));
        }
    }
    let by_hash: HashMap<_, _> = source_nodes.iter().map(|node| (node.hash, node)).collect();
    let finalized = metadata.frontiers.finalized;
    if by_hash
        .get(&finalized.hash)
        .is_none_or(|node| node.height != finalized.height)
    {
        violations.push(AuditViolation::ProtectedPath(finalized.hash));
    }
    check_nodes(&source_nodes, &by_hash, &metadata, &mut violations);
    check_finalized_connectivity(&source_nodes, finalized, &mut violations);
    check_trust_pins(&source_nodes, config, &mut violations);
    check_authoritative_rows(store, &source_nodes, &metadata, config, &mut violations)?;
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

    let mut graph = MemHeaderStore::from_nodes(finalized, source_nodes.clone()).map_err(|_| {
        RecoveryFailure::Source {
            violations: vec![AuditViolation::ProtectedPath(finalized.hash)],
        }
    })?;
    graph
        .recompute_all_eligibility()
        .map_err(|_| RecoveryFailure::Source {
            violations: vec![AuditViolation::ProtectedPath(finalized.hash)],
        })?;
    let mut nodes: Vec<_> = graph.nodes().cloned().collect();
    nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    let node_map: HashMap<_, _> = nodes.iter().map(|node| (node.hash, node.clone())).collect();

    let mut child_edges: Vec<_> = nodes
        .iter()
        .filter(|node| node.hash != finalized.hash)
        .map(|node| (node.parent_hash, node.hash))
        .collect();
    child_edges.sort_unstable_by_key(|(parent, child)| (parent.0, child.0));
    let mut deferred_entries: Vec<_> = nodes
        .iter()
        .filter_map(|node| match node.validation {
            crate::HeaderValidationState::Valid => None,
            crate::HeaderValidationState::DeferredUntil(until) => Some((until, node.hash)),
        })
        .collect();
    deferred_entries.sort_unstable_by_key(|(until, hash)| (*until, hash.0));
    if graph.eligible_tips().len() > config.limits.max_candidate_tips.get() {
        return Err(source_failure(AuditViolation::Limits));
    }
    let (selected_tip, selected_score) = graph
        .select_header_best()
        .map_err(|_| source_failure(AuditViolation::ProtectedPath(finalized.hash)))?;
    let selected_projection = path_to(&node_map, finalized, selected_tip)?;
    let verified_projection = verified_path(&node_map, &metadata)?;

    let mut repairs = BTreeSet::new();
    compare_by_key(
        store.child_edges()?,
        &child_edges,
        |(parent, child)| (parent.0, child.0),
        RecoveryRepair::ChildIndex,
        &mut repairs,
    );
    compare_by_key(
        store.deferred_entries()?,
        &deferred_entries,
        |(until, hash)| (until.timestamp(), until.timestamp_subsec_nanos(), hash.0),
        RecoveryRepair::DeferredIndex,
        &mut repairs,
    );
    if store.selected_projection()? != selected_projection
        || metadata.frontiers.header_best != selected_tip
        || metadata.header_best_score != selected_score
    {
        repairs.insert(RecoveryRepair::SelectedProjection);
    }
    if store.verified_projection()? != verified_projection {
        repairs.insert(RecoveryRepair::VerifiedProjection);
    }
    if source_nodes != nodes {
        repairs.insert(RecoveryRepair::InheritedEligibility);
    }
    let oldest_retained_height = nodes
        .iter()
        .map(|node| node.height)
        .min()
        .unwrap_or(finalized.height);
    if metadata.oldest_retained_height != oldest_retained_height {
        repairs.insert(RecoveryRepair::RetentionMetadata);
    }
    let body_unavailable_alarm = match &node_map
        .get(&selected_tip.hash)
        .ok_or_else(|| source_failure(AuditViolation::ProtectedPath(selected_tip.hash)))?
        .body
    {
        crate::BodyValidationState::Unavailable(summary) if summary.alarmed => Some(*summary),
        _ => None,
    };
    if metadata.alarms.header_best_body_unavailable != body_unavailable_alarm {
        repairs.insert(RecoveryRepair::BodyAvailabilityAlarm);
    }

    if !repairs.is_empty() {
        metadata.state_version = metadata.state_version.checked_next()?;
        if repairs.contains(&RecoveryRepair::SelectedProjection)
            || repairs.contains(&RecoveryRepair::InheritedEligibility)
        {
            metadata.header_generation = metadata.header_generation.checked_next()?;
        }
        if repairs.contains(&RecoveryRepair::VerifiedProjection) {
            metadata.verified_generation = metadata.verified_generation.checked_next()?;
        }
        metadata.frontiers.header_best = selected_tip;
        metadata.header_best_score = selected_score;
        metadata.oldest_retained_height = oldest_retained_height;
        metadata.alarms.header_best_body_unavailable = body_unavailable_alarm;
    }

    Ok(RecoveryPlan {
        before,
        metadata,
        nodes,
        child_edges,
        selected_projection,
        verified_projection,
        deferred_entries,
        repairs,
    })
}

fn check_nodes(
    nodes: &[HeaderNode],
    by_hash: &HashMap<block::Hash, &HeaderNode>,
    metadata: &EngineMetadata,
    violations: &mut Vec<AuditViolation>,
) {
    for node in nodes {
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
        let body_reason = node
            .eligibility
            .direct_reasons
            .iter()
            .find(|reason| matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }));
        let matches = match (&node.body, body_reason) {
            (
                BodyValidationState::ConsensusInvalid {
                    evidence: left_evidence,
                    rule: left_rule,
                },
                Some(EligibilityReason::ConsensusBodyInvalid {
                    evidence: right_evidence,
                    rule: right_rule,
                }),
            ) => left_evidence == right_evidence && left_rule == right_rule,
            (BodyValidationState::ConsensusInvalid { .. }, _) => false,
            (_, None) => true,
            (_, Some(_)) => false,
        };
        if !matches {
            violations.push(AuditViolation::BodyEligibility(node.hash));
        }
        if let crate::HeaderValidationState::DeferredUntil(until) = node.validation {
            let expected = node.header.time.checked_sub_signed(Duration::hours(2));
            if expected != Some(until) {
                violations.push(AuditViolation::HeaderValidation(node.hash));
            }
        }
    }
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
    config: &EngineConfig,
    violations: &mut Vec<AuditViolation>,
) {
    let settled = config.settled_manifest.pin_for_network(&config.network);
    for node in nodes {
        let expected = if settled.is_some_and(|pin| pin.activation.height == node.height) {
            settled.map(|pin| (pin.activation.hash, true))
        } else {
            config
                .local_checkpoints
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
        if node_ids.len() != node.aux_delivery_ids.len()
            || node_ids.iter().any(|id| !delivery_ids.contains(id))
        {
            violations.push(AuditViolation::Auxiliary(node.hash));
        }
    }

    let mut contexts = store.validation_context_records()?;
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
    let mut last = None;
    let mut invalid_history = false;
    store.visit_finality_history(&mut |record| {
        if previous.is_some_and(|previous: FinalityRecord| {
            previous.current != record.previous
                || previous.epoch.get().checked_add(1) != Some(record.epoch.get())
        }) || !source_matches_mode(&record, metadata.mode, config)
        {
            invalid_history = true;
        }
        previous = Some(record);
        last = Some(record);
        Ok(())
    })?;
    if invalid_history
        || last.is_some_and(|record| {
            record.current != metadata.frontiers.finalized
                || record.epoch != metadata.finality_epoch
        })
        || last.is_none() && metadata.finality_epoch.get() != 0
    {
        violations.push(AuditViolation::Finality);
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

fn verified_path(
    nodes: &HashMap<block::Hash, HeaderNode>,
    metadata: &EngineMetadata,
) -> Result<Vec<Frontier>, RecoveryFailure> {
    if metadata.mode == EngineMode::HeadersOnly {
        if metadata.frontiers.verified_best != metadata.frontiers.finalized {
            return Err(source_failure(AuditViolation::ProtectedPath(
                metadata.frontiers.verified_best.hash,
            )));
        }
        return Ok(vec![metadata.frontiers.finalized]);
    }
    let path = path_to(
        nodes,
        metadata.frontiers.finalized,
        metadata.frontiers.verified_best,
    )?;
    if path.iter().skip(1).any(|frontier| {
        nodes
            .get(&frontier.hash)
            .is_none_or(|node| !matches!(node.body, BodyValidationState::Verified { .. }))
    }) {
        return Err(source_failure(AuditViolation::ProtectedPath(
            metadata.frontiers.verified_best.hash,
        )));
    }
    Ok(path)
}

fn path_to(
    nodes: &HashMap<block::Hash, HeaderNode>,
    finalized: Frontier,
    tip: Frontier,
) -> Result<Vec<Frontier>, RecoveryFailure> {
    let mut current = tip;
    let mut path = Vec::new();
    loop {
        let node = nodes
            .get(&current.hash)
            .filter(|node| node.height == current.height)
            .ok_or_else(|| source_failure(AuditViolation::ProtectedPath(current.hash)))?;
        path.push(current);
        if current == finalized {
            break;
        }
        current = Frontier::new(
            current
                .height
                .previous()
                .map_err(|_| source_failure(AuditViolation::ProtectedPath(current.hash)))?,
            node.parent_hash,
        );
    }
    path.reverse();
    Ok(path)
}

fn compare_by_key<T, K: Ord, F: FnMut(&T) -> K>(
    mut actual: Vec<T>,
    expected: &[T],
    mut key: F,
    repair: RecoveryRepair,
    repairs: &mut BTreeSet<RecoveryRepair>,
) where
    T: Clone + Eq,
{
    let mut expected = expected.to_vec();
    actual.sort_by_key(&mut key);
    expected.sort_by_key(key);
    if actual != expected {
        repairs.insert(repair);
    }
}

fn violation_key(violation: &AuditViolation) -> (u8, u32, [u8; 32]) {
    match violation {
        AuditViolation::NodeHash(hash) => (0, 0, hash.0),
        AuditViolation::Parent(hash) => (1, 0, hash.0),
        AuditViolation::Work(hash) => (2, 0, hash.0),
        AuditViolation::BodyEligibility(hash) => (3, 0, hash.0),
        AuditViolation::HeaderValidation(hash) => (4, 0, hash.0),
        AuditViolation::TrustPin(height, hash) => (5, height.0, hash.0),
        AuditViolation::EligibilityRoot(hash) => (6, 0, hash.0),
        AuditViolation::Auxiliary(hash) => (7, 0, hash.0),
        AuditViolation::ValidationContext(hash) => (8, 0, hash.0),
        AuditViolation::Finality => (9, 0, [0; 32]),
        AuditViolation::Configuration => (10, 0, [0; 32]),
        AuditViolation::ProtectedPath(hash) => (11, 0, hash.0),
        AuditViolation::Limits => (12, 0, [0; 32]),
    }
}

fn source_failure(violation: AuditViolation) -> RecoveryFailure {
    RecoveryFailure::Source {
        violations: vec![violation],
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use zakura_chain::{
        block::genesis::regtest_genesis_block,
        parameters::{testnet::RegtestParameters, Network},
    };

    use super::*;
    use crate::{
        AlarmSet, AuxAuthentication, BodyRuleId, BodySizeHint, BodyUnavailableSummary, BranchId,
        ChainScore, CheckpointSet, EligibilityState, EngineMode, EvidenceId, FinalityEpoch,
        FrontierSet, HeaderChainDiskVersion, HeaderGeneration, HeaderValidationState, SourceId,
        StateVersion, SuffixWork, TrustedAnchor, VerifiedGeneration, WorkCoordinate, WorkOwner,
    };

    #[derive(Clone)]
    struct AuditStore {
        metadata: EngineMetadata,
        snapshot: EngineSnapshot,
        nodes: Vec<HeaderNode>,
        children: Vec<(block::Hash, block::Hash)>,
        selected: Vec<Frontier>,
        verified: Vec<Frontier>,
        deferred: Vec<(DateTime<Utc>, block::Hash)>,
        reasons: Vec<(block::Hash, EligibilityReason)>,
        aux: Vec<AuxDelivery>,
        contexts: Vec<ValidationContextRecord>,
        finality: Vec<FinalityRecord>,
    }

    impl StoreAuditRead for AuditStore {
        fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
            Ok(self.snapshot.clone())
        }

        fn metadata(&self) -> Result<EngineMetadata, StoreError> {
            Ok(self.metadata.clone())
        }

        fn all_nodes(&self) -> Result<Vec<HeaderNode>, StoreError> {
            Ok(self.nodes.clone())
        }

        fn child_edges(&self) -> Result<Vec<(block::Hash, block::Hash)>, StoreError> {
            Ok(self.children.clone())
        }

        fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError> {
            Ok(self.selected.clone())
        }

        fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError> {
            Ok(self.verified.clone())
        }

        fn deferred_entries(&self) -> Result<Vec<(DateTime<Utc>, block::Hash)>, StoreError> {
            Ok(self.deferred.clone())
        }

        fn eligibility_roots(&self) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError> {
            Ok(self.reasons.clone())
        }

        fn all_aux_deliveries(&self) -> Result<Vec<AuxDelivery>, StoreError> {
            Ok(self.aux.clone())
        }

        fn validation_context_records(&self) -> Result<Vec<ValidationContextRecord>, StoreError> {
            Ok(self.contexts.clone())
        }

        fn visit_finality_history(
            &self,
            visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
        ) -> Result<(), StoreError> {
            for record in &self.finality {
                visitor(*record)?;
            }
            Ok(())
        }
    }

    fn fixture() -> (AuditStore, EngineConfig) {
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
            frontiers: FrontierSet {
                finalized: anchor,
                header_best: child,
                verified_best: anchor,
            },
            header_best_score: score,
            oldest_retained_height: anchor.height,
            alarms: AlarmSet::default(),
            last_transition_id: EvidenceId::from_digest([0; 32]),
        };
        (
            AuditStore {
                snapshot: metadata.snapshot(),
                metadata,
                nodes: vec![anchor_node, child_node],
                children: vec![(anchor.hash, child.hash)],
                selected: vec![anchor, child],
                verified: vec![anchor],
                deferred: Vec::new(),
                reasons: Vec::new(),
                aux: Vec::new(),
                contexts: Vec::new(),
                finality: Vec::new(),
            },
            config,
        )
    }

    fn violations(store: &AuditStore, config: &EngineConfig) -> Vec<AuditViolation> {
        match audit_store(store, config) {
            Err(RecoveryFailure::Source { violations }) => violations,
            other => panic!("expected source audit failure, got {other:?}"),
        }
    }

    #[test]
    fn coherent_source_and_indexes_need_no_recovery_write() {
        let (store, config) = fixture();
        let plan = audit_store(&store, &config).expect("the coherent fixture audits cleanly");
        assert!(plan.is_clean());
        assert_eq!(plan.metadata, store.metadata);
    }

    #[test]
    fn body_unavailability_alarm_is_reconstructed_from_the_selected_node() {
        let (mut store, config) = fixture();
        let summary = BodyUnavailableSummary {
            attempts: 10,
            suppliers: 2,
            alarmed: true,
            ..Default::default()
        };
        store.nodes[1].body = crate::BodyValidationState::Unavailable(summary);

        let plan = audit_store(&store, &config).expect("the derived alarm is reconstructible");
        assert_eq!(
            plan.repairs,
            BTreeSet::from([RecoveryRepair::BodyAvailabilityAlarm])
        );
        assert_eq!(
            plan.metadata.alarms.header_best_body_unavailable,
            Some(summary)
        );
        assert_eq!(plan.metadata.state_version, StateVersion::new(2));
        assert_eq!(plan.metadata.header_generation, HeaderGeneration::new(1));
    }

    #[test]
    fn recompute_not_cached_projection() {
        let (mut store, config) = fixture();
        let anchor = store.metadata.frontiers.finalized;
        let child_hash = store.metadata.frontiers.header_best.hash;
        store.children.clear();
        store.selected = vec![anchor];
        store.verified.clear();
        store.deferred.push((Utc::now(), child_hash));
        store.nodes[1].eligibility.inherited_from = Some(anchor.hash);

        let plan = audit_store(&store, &config).expect("cache corruption is reconstructible");
        assert_eq!(
            plan.repairs,
            BTreeSet::from([
                RecoveryRepair::ChildIndex,
                RecoveryRepair::DeferredIndex,
                RecoveryRepair::SelectedProjection,
                RecoveryRepair::VerifiedProjection,
                RecoveryRepair::InheritedEligibility,
            ])
        );
        assert_eq!(plan.metadata.state_version, StateVersion::new(2));
        assert_eq!(plan.metadata.header_generation, HeaderGeneration::new(2));
        assert_eq!(
            plan.metadata.verified_generation,
            VerifiedGeneration::new(2)
        );
    }

    #[test]
    fn audits_each_normative_invariant() {
        let (base, config) = fixture();
        let child_hash = base.metadata.frontiers.header_best.hash;

        let mut store = base.clone();
        store.metadata.anchor_manifest_digest[0] ^= 1;
        store.snapshot = store.metadata.snapshot();
        assert!(violations(&store, &config).contains(&AuditViolation::Configuration));

        let mut store = base.clone();
        store.nodes[1].hash = block::Hash([8; 32]);
        assert!(
            violations(&store, &config).contains(&AuditViolation::NodeHash(block::Hash([8; 32])))
        );

        let mut store = base.clone();
        let missing = block::Hash([9; 32]);
        store.nodes[1].parent_hash = missing;
        store.nodes[1].header = Arc::new(block::Header {
            previous_block_hash: missing,
            ..*store.nodes[1].header
        });
        store.nodes[1].hash = store.nodes[1].header.hash();
        assert!(violations(&store, &config)
            .iter()
            .any(|violation| matches!(violation, AuditViolation::Parent(_))));

        let mut store = base.clone();
        store.nodes[1] = HeaderNode::from_durable_parts(
            store.nodes[1].header.clone(),
            child_hash,
            store.nodes[1].parent_hash,
            store.nodes[1].height,
            store.nodes[1].block_work,
            WorkCoordinate::new(store.metadata.work_origin.hash, Default::default()),
            store.nodes[1].validation,
            store.nodes[1].eligibility.clone(),
            store.nodes[1].body.clone(),
            Vec::new(),
        )
        .expect("the isolated node fields remain canonical");
        assert!(violations(&store, &config).contains(&AuditViolation::Work(child_hash)));

        let mut store = base.clone();
        store.nodes[1].body = BodyValidationState::ConsensusInvalid {
            evidence: EvidenceId::from_digest([2; 32]),
            rule: BodyRuleId::new("body.rule"),
        };
        assert!(violations(&store, &config).contains(&AuditViolation::BodyEligibility(child_hash)));

        let mut store = base.clone();
        let corrupted_until = store.nodes[1].header.time + Duration::hours(100);
        store.nodes[1].validation = crate::HeaderValidationState::DeferredUntil(corrupted_until);
        store.deferred = vec![(corrupted_until, child_hash)];
        assert!(violations(&store, &config).contains(&AuditViolation::HeaderValidation(child_hash)));

        let mut store = base.clone();
        store.reasons.push((
            child_hash,
            EligibilityReason::OperatorInvalid {
                id: crate::OperatorInvalidationId::new([3; 16]),
            },
        ));
        assert!(violations(&store, &config).contains(&AuditViolation::EligibilityRoot(child_hash)));

        let mut checkpointed = config.clone();
        checkpointed.local_checkpoints =
            CheckpointSet::new([Frontier::new(block::Height(1), block::Hash([0xaa; 32]))])
                .expect("the checkpoint fixture is unique");
        let mut store = base.clone();
        store.metadata.anchor_manifest_digest = checkpointed.trust_anchor_digest();
        store.snapshot = store.metadata.snapshot();
        assert!(violations(&store, &checkpointed)
            .contains(&AuditViolation::TrustPin(block::Height(1), child_hash)));

        let mut store = base.clone();
        store.nodes[1]
            .aux_delivery_ids
            .push(EvidenceId::from_digest([4; 32]));
        assert!(violations(&store, &config).contains(&AuditViolation::Auxiliary(child_hash)));

        let mut store = base.clone();
        store.contexts.push(ValidationContextRecord {
            header: regtest_genesis_block().header.clone(),
            height: block::Height(7),
        });
        assert!(violations(&store, &config)
            .iter()
            .any(|violation| matches!(violation, AuditViolation::ValidationContext(_))));

        let mut store = base.clone();
        store.metadata.frontiers.finalized = Frontier::new(block::Height(1), child_hash);
        store.snapshot = store.metadata.snapshot();
        assert!(violations(&store, &config)
            .iter()
            .any(|violation| matches!(violation, AuditViolation::ValidationContext(_))));

        let mut store = base.clone();
        store.metadata.finality_epoch = FinalityEpoch::new(1);
        store.snapshot = store.metadata.snapshot();
        assert!(violations(&store, &config).contains(&AuditViolation::Finality));

        let mut headers_only = config.clone();
        headers_only.mode = EngineMode::HeadersOnly;
        let mut store = base.clone();
        store.metadata.mode = EngineMode::HeadersOnly;
        store.snapshot = store.metadata.snapshot();
        store.finality.push(FinalityRecord {
            previous: store.metadata.frontiers.finalized,
            current: store.metadata.frontiers.finalized,
            source: FinalitySource::HeadersOnlyDepth {
                selected_tip: store.metadata.frontiers.header_best,
            },
            epoch: FinalityEpoch::new(0),
        });
        assert!(violations(&store, &headers_only).contains(&AuditViolation::Finality));

        let mut limited = config.clone();
        limited.limits.max_non_finalized_nodes = NonZeroUsize::new(1).expect("one is nonzero");
        let mut oversized = base.clone();
        oversized.nodes.push(oversized.nodes[1].clone());
        assert!(violations(&oversized, &limited).contains(&AuditViolation::Limits));

        let mut store = base.clone();
        let evidence = EvidenceId::from_digest([5; 32]);
        store.aux.push(AuxDelivery {
            delivery_id: evidence,
            header_hash: block::Hash([6; 32]),
            source: SourceId::from_digest([7; 32]),
            owner: WorkOwner {
                state_version: StateVersion::new(1),
                header_generation: HeaderGeneration::new(1),
                verified_generation: None,
                branch: BranchId::new(base.metadata.work_origin.hash, child_hash),
                session_id: 1,
                request_id: NonZeroU64::new(1).expect("one is nonzero"),
            },
            body_size: BodySizeHint::Unknown,
            tree_aux: None,
            authentication: AuxAuthentication::Unauthenticated,
        });
        assert!(violations(&store, &config)
            .iter()
            .any(|violation| matches!(violation, AuditViolation::Auxiliary(_))));
    }
}
