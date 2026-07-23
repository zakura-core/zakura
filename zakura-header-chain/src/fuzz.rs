//! Feature-gated entry points shared by libFuzzer and deterministic corpus tests.

use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};

use crate::{
    apply_transition, AlarmSet, AuxDelivery, AuxDelta, BranchId, ChainScore, CheckpointSet, Clock,
    EngineConfig, EngineMetadata, EngineMode, EngineSnapshot, EvidenceId, FinalityEpoch,
    FinalityRecord, Frontier, FrontierSet, HeaderChainDiskVersion, HeaderContextFact,
    HeaderGeneration, HeaderNode, HeaderValidationState, InsertHeaders, MemHeaderStore,
    OperatorInvalidate, OperatorInvalidationId, OperatorReconsider, PreparedHeader,
    PreparedHeaderBatch, ProjectionDelta, SourceId, StateVersion, StoreError, StoreRead,
    SuffixWork, TargetCompletion, TransitionContext, TransitionFailure, TransitionPlan,
    TransitionRequest, TrustedAnchor, ValidationLease, VerifiedGeneration, WorkOwner,
};

/// Deterministic summary of one bounded structured-operation replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkReplaySummary {
    /// Number of complete operation bytes consumed.
    pub operations: u16,
    /// Successful durable transitions.
    pub commits: u16,
    /// Expected stale or invalid operations with no durable effect.
    pub refused: u16,
    /// Final authoritative snapshot.
    pub snapshot: EngineSnapshot,
    /// Stable digest of operation outcomes and snapshots.
    pub replay_digest: [u8; 32],
    /// Stable digest of every retained node and projection after the final operation.
    pub retained_digest: [u8; 32],
}

#[derive(Clone)]
struct FuzzStore {
    graph: MemHeaderStore,
    metadata: EngineMetadata,
    selected: Vec<Frontier>,
    verified: Vec<Frontier>,
    finality: Vec<FinalityRecord>,
    aux: Vec<AuxDelivery>,
    config: EngineConfig,
}

impl FuzzStore {
    fn new(mode: EngineMode) -> Self {
        let genesis = regtest_genesis_block();
        let frontier = Frontier::new(block::Height(0), genesis.hash());
        let work = genesis
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest genesis target has valid work");
        let graph = MemHeaderStore::new(frontier, genesis.header.clone(), work, work.as_u256())
            .expect("the fixed fuzz anchor is coherent");
        let config = EngineConfig::new(
            mode,
            Network::new_regtest(RegtestParameters::default()),
            TrustedAnchor {
                frontier,
                header: genesis.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the fixed fuzz configuration is coherent");
        let metadata = EngineMetadata {
            disk_format: HeaderChainDiskVersion(1),
            mode,
            network_id: config.network.kind(),
            anchor_manifest_digest: config.trust_anchor_digest(),
            work_origin: frontier,
            state_version: StateVersion::new(0),
            header_generation: HeaderGeneration::new(0),
            verified_generation: VerifiedGeneration::new(0),
            finality_epoch: FinalityEpoch::new(0),
            frontiers: FrontierSet {
                finalized: frontier,
                header_best: frontier,
                verified_best: frontier,
            },
            header_best_score: graph.score(frontier.hash).expect("the anchor has a score"),
            oldest_retained_height: frontier.height,
            alarms: AlarmSet::default(),
            last_transition_id: EvidenceId::from_digest([0xff; 32]),
        };
        Self {
            graph,
            metadata,
            selected: vec![frontier],
            verified: vec![frontier],
            finality: Vec::new(),
            aux: Vec::new(),
            config,
        }
    }

    fn snapshot(&self) -> EngineSnapshot {
        self.metadata.snapshot()
    }

    fn commit(&mut self, plan: &TransitionPlan) {
        self.graph = plan.fuzz_projected().clone();
        self.metadata = plan.change_set().metadata.clone();
        apply_projection(&mut self.selected, &plan.change_set().selected_projection);
        apply_projection(&mut self.verified, &plan.change_set().verified_projection);
        if let Some(record) = plan.change_set().finality_append {
            self.finality.push(record);
        }
        for change in &plan.change_set().aux_changes {
            match change {
                AuxDelta::Put(delivery) => {
                    self.aux
                        .retain(|existing| existing.delivery_id != delivery.delivery_id);
                    self.aux.push(**delivery);
                }
                AuxDelta::Delete(delivery_id) => self
                    .aux
                    .retain(|existing| existing.delivery_id != *delivery_id),
            }
        }
    }

    fn lease(&self, parent: Frontier) -> ValidationLease {
        let node = self
            .graph
            .node(parent.hash)
            .expect("an operation selects only a retained parent");
        let mut digest = [0u8; 32];
        digest[..4].copy_from_slice(&parent.height.0.to_le_bytes());
        digest[4..].copy_from_slice(&parent.hash.0[..28]);
        ValidationLease {
            parent,
            predecessors: vec![HeaderContextFact {
                frontier: parent,
                difficulty_threshold: node.header.difficulty_threshold,
                time: node.header.time,
            }],
            trust_anchor_digest: self.config.trust_anchor_digest(),
            context_digest: digest,
        }
    }

    fn insertion(
        &self,
        parent: Frontier,
        count: u32,
        operation: usize,
        branch: u8,
    ) -> TransitionRequest {
        let lease = self.lease(parent);
        let evidence = evidence(operation, branch);
        let mut headers = Vec::with_capacity(usize::try_from(count).unwrap_or(8));
        let mut parent_hash = parent.hash;
        for offset in 1..=count {
            let mut header = *regtest_genesis_block().header;
            header.previous_block_hash = parent_hash;
            header.nonce.0[..8].copy_from_slice(&operation_u64(operation).to_le_bytes());
            header.nonce.0[8] = branch;
            header.nonce.0[9..13].copy_from_slice(&offset.to_le_bytes());
            let header = Arc::new(header);
            let hash = header.hash();
            headers.push(PreparedHeader {
                header: header.clone(),
                hash,
                height: block::Height(parent.height.0.saturating_add(offset)),
                block_work: header
                    .difficulty_threshold
                    .to_work()
                    .expect("the fixed target has valid work"),
                validation: HeaderValidationState::Valid,
            });
            parent_hash = hash;
        }
        let batch = PreparedHeaderBatch::new(headers, lease.context_digest, evidence)
            .expect("the operation count is nonzero");
        let target = batch.headers().last().expect("the batch is nonempty").hash;
        TransitionRequest {
            expected_version: self.metadata.state_version,
            event: crate::TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                owner: WorkOwner {
                    state_version: self.metadata.state_version,
                    header_generation: self.metadata.header_generation,
                    verified_generation: None,
                    branch: BranchId::new(self.metadata.frontiers.finalized.hash, target),
                    session_id: u64::try_from(operation).unwrap_or(u64::MAX),
                    request_id: NonZeroU64::new(
                        u64::try_from(operation)
                            .unwrap_or(u64::MAX)
                            .saturating_add(1),
                    )
                    .expect("the request identity is nonzero"),
                },
                source: SourceId::from_digest([branch; 32]),
                parent_hash: parent.hash,
                target_tip_hash: target,
                completion: TargetCompletion::TargetComplete {
                    common_ancestor: parent,
                },
                batch,
                aux: Vec::new(),
            })),
        }
    }

    fn retained_parent(&self, selector: u8) -> Frontier {
        let finalized = self.metadata.frontiers.finalized;
        let best = self.metadata.frontiers.header_best;
        let span = best.height.0.saturating_sub(finalized.height.0);
        let height = finalized
            .height
            .0
            .saturating_add(u32::from(selector) % span.saturating_add(1));
        let hash = self
            .selected
            .iter()
            .find(|frontier| {
                frontier.height == block::Height(height) && self.graph.node(frontier.hash).is_some()
            })
            .map(|frontier| frontier.hash)
            .unwrap_or(finalized.hash);
        Frontier::new(block::Height(height), hash)
    }
}

impl StoreRead for FuzzStore {
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        Ok(self.snapshot())
    }
    fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        Ok(self.metadata.clone())
    }
    fn node(&self, hash: block::Hash) -> Result<Option<HeaderNode>, StoreError> {
        Ok(self.graph.node(hash).cloned())
    }
    fn children(&self, parent: block::Hash) -> Result<Vec<block::Hash>, StoreError> {
        Ok(self.graph.children(parent))
    }
    fn hashes_at_height(&self, height: block::Height) -> Result<Vec<block::Hash>, StoreError> {
        Ok(self.graph.hashes_at_height(height))
    }
    fn selected_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        Ok(frontier_hash_at(&self.selected, height))
    }
    fn verified_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
        Ok(frontier_hash_at(&self.verified, height))
    }
    fn candidate_tips(&self) -> Result<Vec<(ChainScore, block::Hash)>, StoreError> {
        self.graph
            .eligible_tips()
            .into_iter()
            .map(|tip| {
                self.graph
                    .score(tip.hash)
                    .map(|score| (score, tip.hash))
                    .map_err(|_| StoreError::Incoherent("candidate score is unavailable"))
            })
            .collect()
    }
    fn validation_context(&self, parent: block::Hash) -> Result<ValidationLease, StoreError> {
        let node = self
            .graph
            .node(parent)
            .ok_or(StoreError::Incoherent("validation parent is not retained"))?;
        Ok(self.lease(Frontier::new(node.height, parent)))
    }
    fn aux_deliveries(&self, hash: block::Hash) -> Result<Vec<AuxDelivery>, StoreError> {
        Ok(self
            .aux
            .iter()
            .filter(|delivery| delivery.header_hash == hash)
            .copied()
            .collect())
    }
    fn finality_history(&self) -> Result<Vec<FinalityRecord>, StoreError> {
        Ok(self.finality.clone())
    }
}

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::<Utc>::UNIX_EPOCH
    }
}

/// Replay up to 512 structured operations through the production transition engine.
pub fn replay_fork_transition_bytes(bytes: &[u8]) -> ForkReplaySummary {
    let bounded = &bytes[..bytes.len().min(512)];
    let mode = if bounded.first().is_some_and(|byte| byte & 1 == 1) {
        EngineMode::HeadersOnly
    } else {
        EngineMode::Integrated
    };
    let mut store = FuzzStore::new(mode);
    let mut commits = 0u16;
    let mut refused = 0u16;
    let mut transcript = Sha256::new();
    assert_exhaustive_oracle(&store);

    for (operation, byte) in bounded.iter().copied().enumerate() {
        let before = store.snapshot();
        let count = u32::from(byte & 0x07).saturating_add(1);
        let branch = byte.rotate_left(3);
        let request = match (byte >> 3) & 0x07 {
            0 | 1 => store.insertion(
                if byte & 0x08 == 0 {
                    store.metadata.frontiers.header_best
                } else {
                    store.retained_parent(branch)
                },
                count,
                operation,
                branch,
            ),
            2 => {
                let mut request = store.insertion(
                    store.metadata.frontiers.header_best,
                    count,
                    operation,
                    branch,
                );
                request.expected_version =
                    StateVersion::new(store.metadata.state_version.get().saturating_add(1));
                request
            }
            3 => {
                let target = store.retained_parent(branch);
                if target == store.metadata.frontiers.finalized {
                    refused = refused.saturating_add(1);
                    transcript.update([byte, 0]);
                    assert_exhaustive_oracle(&store);
                    continue;
                }
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: crate::TransitionEvent::OperatorInvalidate(OperatorInvalidate {
                        target: target.hash,
                        id: operator_id(operation, branch),
                        operator_reason_digest: [branch.wrapping_add(1); 32],
                        evidence: evidence(operation, branch),
                    }),
                }
            }
            4 => {
                let target = store.retained_parent(branch);
                if target == store.metadata.frontiers.finalized {
                    refused = refused.saturating_add(1);
                    transcript.update([byte, 0]);
                    assert_exhaustive_oracle(&store);
                    continue;
                }
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: crate::TransitionEvent::OperatorReconsider(OperatorReconsider {
                        target: target.hash,
                        id: operator_id(operation.saturating_sub(1), branch),
                        evidence: evidence(operation, branch),
                    }),
                }
            }
            5 => {
                // Crash/reopen oracle: cloning the coherent logical rows must preserve publication.
                let reopened = store.clone();
                assert_eq!(reopened.snapshot(), store.snapshot());
                assert_eq!(retained_digest(&reopened), retained_digest(&store));
                transcript.update(b"reopen");
                assert_exhaustive_oracle(&store);
                continue;
            }
            _ => {
                // Explicit stale/no-op references are part of the operation language.
                refused = refused.saturating_add(1);
                transcript.update([byte, 0]);
                assert_exhaustive_oracle(&store);
                continue;
            }
        };
        let context = TransitionContext {
            config: &store.config,
            clock: &FixedClock,
            full_state_authority: None,
            startup_capability: None,
            retention_references: &[],
        };
        match apply_transition(&store, request, &context) {
            Ok(plan) => {
                assert_eq!(plan.before(), &before);
                assert_generation_delta(&before, &plan.change_set().metadata.snapshot());
                let no_change = plan.is_no_change();
                store.commit(&plan);
                assert_eq!(store.snapshot(), plan.change_set().metadata.snapshot());
                if no_change {
                    refused = refused.saturating_add(1);
                } else {
                    commits = commits.saturating_add(1);
                }
            }
            Err(TransitionFailure::Stale { .. })
            | Err(TransitionFailure::InvalidEvidence(_))
            | Err(TransitionFailure::Mode)
            | Err(TransitionFailure::StalePreparation)
            | Err(TransitionFailure::ResourceStalled) => {
                assert_eq!(store.snapshot(), before);
                refused = refused.saturating_add(1);
            }
            Err(error) => panic!(
                "structured operation {operation} ({byte:#04x}) produced unexpected local failure: {error}"
            ),
        }
        assert_exhaustive_oracle(&store);
        append_snapshot(&mut transcript, &store.snapshot());
    }

    ForkReplaySummary {
        operations: u16::try_from(bounded.len()).expect("the operation cap fits in u16"),
        commits,
        refused,
        snapshot: store.snapshot(),
        replay_digest: transcript.finalize().into(),
        retained_digest: retained_digest(&store),
    }
}

fn assert_exhaustive_oracle(store: &FuzzStore) {
    let finalized = store.metadata.frontiers.finalized;
    let mut nodes: Vec<_> = store.graph.nodes().collect();
    nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    assert!(!nodes.is_empty(), "the finalized anchor must be retained");

    let mut independently_eligible: HashMap<block::Hash, bool> = HashMap::new();
    let mut suffix_work = HashMap::new();
    let mut indexed_children: HashMap<block::Hash, Vec<block::Hash>> = HashMap::new();
    for node in &nodes {
        assert_eq!(
            store.graph.node(node.hash),
            Some(*node),
            "the primary hash index must return every retained node"
        );
        assert!(
            store
                .graph
                .hashes_at_height(node.height)
                .contains(&node.hash),
            "the height index must contain every retained node"
        );

        if node.hash == finalized.hash {
            assert_eq!(node.height, finalized.height);
            assert_eq!(node.eligibility.inherited_from, None);
            suffix_work.insert(node.hash, SuffixWork::zero());
        } else {
            let parent = store
                .graph
                .node(node.parent_hash)
                .expect("every non-finalized retained node has a retained parent");
            assert_eq!(
                node.height,
                block::Height(parent.height.0.saturating_add(1)),
                "retained height is exactly parent height plus one"
            );
            let parent_eligible = *independently_eligible
                .get(&parent.hash)
                .expect("parents sort before children by height");
            assert_eq!(
                node.eligibility.inherited_from,
                (!parent_eligible).then_some(parent.hash),
                "inherited eligibility is recomputed from the parent"
            );
            let parent_work = *suffix_work
                .get(&parent.hash)
                .expect("parents sort before children by height");
            suffix_work.insert(
                node.hash,
                parent_work
                    .checked_add(node.block_work)
                    .expect("the production transition already rejected work overflow"),
            );
            indexed_children
                .entry(parent.hash)
                .or_default()
                .push(node.hash);
        }

        let eligible = node.validation == HeaderValidationState::Valid
            && node.eligibility.direct_reasons.is_empty()
            && node.eligibility.inherited_from.is_none();
        assert_eq!(
            node.is_eligible(),
            eligible,
            "node eligibility must equal its independently recomputed facts"
        );
        independently_eligible.insert(node.hash, eligible);
    }

    for node in &nodes {
        let mut expected_children = indexed_children.remove(&node.hash).unwrap_or_default();
        expected_children.sort_unstable_by_key(|hash| hash.0);
        assert_eq!(
            store.graph.children(node.hash),
            expected_children,
            "the child index must exactly match retained parent links"
        );
    }

    let expected_header_best = nodes
        .iter()
        .filter(|node| independently_eligible[&node.hash])
        .map(|node| {
            (
                ChainScore::new(suffix_work[&node.hash], node.hash),
                Frontier::new(node.height, node.hash),
            )
        })
        .max_by_key(|(score, _)| *score)
        .expect("the finalized anchor is independently eligible");
    assert_eq!(
        store.metadata.frontiers.header_best, expected_header_best.1,
        "fork choice must equal independent work/hash ordering"
    );
    assert_eq!(
        store.metadata.header_best_score, expected_header_best.0,
        "published work must equal the independently accumulated score"
    );
    assert_eq!(
        store.selected,
        independent_path(store, expected_header_best.1),
        "the selected projection must exactly match parent links"
    );
    assert_eq!(
        store.verified,
        independent_path(store, store.metadata.frontiers.verified_best),
        "the verified projection must exactly match parent links"
    );
}

fn independent_path(store: &FuzzStore, tip: Frontier) -> Vec<Frontier> {
    let finalized = store.metadata.frontiers.finalized;
    let mut current = tip;
    let mut reversed = Vec::new();
    loop {
        reversed.push(current);
        if current == finalized {
            break;
        }
        let node = store
            .graph
            .node(current.hash)
            .expect("published projection members are retained");
        current = Frontier::new(
            block::Height(current.height.0.saturating_sub(1)),
            node.parent_hash,
        );
    }
    reversed.reverse();
    reversed
}

fn retained_digest(store: &FuzzStore) -> [u8; 32] {
    let mut nodes: Vec<_> = store.graph.nodes().collect();
    nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-fuzz-retained-v1");
    for node in nodes {
        hasher.update(node.height.0.to_le_bytes());
        hasher.update(node.hash.0);
        hasher.update(node.parent_hash.0);
        hasher.update(node.work_coordinate().cumulative_work().to_big_endian());
        hasher.update(format!("{:?}", node.validation));
        hasher.update(format!("{:?}", node.eligibility));
        hasher.update(format!("{:?}", node.body));
        for delivery in &node.aux_delivery_ids {
            hasher.update(delivery.digest());
        }
    }
    for frontier in &store.selected {
        hasher.update(b"selected");
        hasher.update(frontier.height.0.to_le_bytes());
        hasher.update(frontier.hash.0);
    }
    for frontier in &store.verified {
        hasher.update(b"verified");
        hasher.update(frontier.height.0.to_le_bytes());
        hasher.update(frontier.hash.0);
    }
    hasher.finalize().into()
}

fn apply_projection(path: &mut Vec<Frontier>, delta: &ProjectionDelta) {
    if let Some(remove_from) = delta.remove_from {
        path.retain(|frontier| frontier.height < remove_from);
    }
    path.extend(delta.put.iter().copied());
}

fn frontier_hash_at(path: &[Frontier], height: block::Height) -> Option<block::Hash> {
    path.iter()
        .find(|frontier| frontier.height == height)
        .map(|frontier| frontier.hash)
}

fn evidence(operation: usize, branch: u8) -> EvidenceId {
    let mut digest = [branch; 32];
    digest[..8].copy_from_slice(&operation_u64(operation).to_le_bytes());
    EvidenceId::from_digest(digest)
}

fn operator_id(operation: usize, branch: u8) -> OperatorInvalidationId {
    let mut id = [branch; 16];
    id[..8].copy_from_slice(&operation_u64(operation).to_le_bytes());
    OperatorInvalidationId::new(id)
}

fn operation_u64(operation: usize) -> u64 {
    u64::try_from(operation).expect("the 512-operation cap fits in u64")
}

fn assert_generation_delta(before: &EngineSnapshot, after: &EngineSnapshot) {
    let header_changed = before.frontiers.finalized != after.frontiers.finalized
        || before.frontiers.header_best != after.frontiers.header_best;
    let verified_changed = before.frontiers.verified_best != after.frontiers.verified_best;
    assert_eq!(
        before.header_generation != after.header_generation,
        header_changed,
        "header generation must change exactly with its owned frontiers"
    );
    assert_eq!(
        before.verified_generation != after.verified_generation,
        verified_changed,
        "verified generation must change exactly with the verified path"
    );
}

fn append_snapshot(hasher: &mut Sha256, snapshot: &EngineSnapshot) {
    hasher.update(snapshot.state_version.get().to_le_bytes());
    hasher.update(snapshot.header_generation.get().to_le_bytes());
    hasher.update(snapshot.verified_generation.get().to_le_bytes());
    hasher.update(snapshot.frontiers.finalized.height.0.to_le_bytes());
    hasher.update(snapshot.frontiers.finalized.hash.0);
    hasher.update(snapshot.frontiers.header_best.height.0.to_le_bytes());
    hasher.update(snapshot.frontiers.header_best.hash.0);
    hasher.update(snapshot.frontiers.verified_best.height.0.to_le_bytes());
    hasher.update(snapshot.frontiers.verified_best.hash.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_replay_is_bounded_and_deterministic() {
        let bytes: Vec<_> = (0..600)
            .map(|index| u8::try_from(index % 256).expect("the modulo value fits in u8"))
            .collect();
        let first = replay_fork_transition_bytes(&bytes);
        let second = replay_fork_transition_bytes(&bytes);
        assert_eq!(first, second);
        assert_eq!(first.operations, 512);
        assert!(first.commits > 0);
        assert!(first.refused > 0);
    }

    #[test]
    #[should_panic(expected = "selected projection must exactly match parent links")]
    fn exhaustive_oracle_rejects_a_projection_gap() {
        let mut store = FuzzStore::new(EngineMode::Integrated);
        store.selected.clear();

        assert_exhaustive_oracle(&store);
    }

    #[test]
    fn fork_transition_regression_corpus_replays_green() {
        let corpus: &[(&str, &[u8])] = &[
            (
                "linear_growth",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/linear_growth"),
            ),
            (
                "fork_replacement",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/fork_replacement"),
            ),
            (
                "stale_completion",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/stale_completion"),
            ),
            (
                "operator_cycle",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/operator_cycle"),
            ),
            (
                "crash_reopen",
                include_bytes!("../../fuzz/header-chain/corpus/fork_transitions/crash_reopen"),
            ),
        ];
        for (name, bytes) in corpus {
            let first = replay_fork_transition_bytes(bytes);
            let second = replay_fork_transition_bytes(bytes);
            assert_eq!(first, second, "{name} must replay deterministically");
        }
    }
}
