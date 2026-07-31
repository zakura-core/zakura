mod auxiliary;
mod body_evidence;
mod finality;
mod guards;
mod insertion;
mod operator;
mod selection;

use std::{num::NonZeroU64, sync::Arc};

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{genesis::regtest_genesis_block, Block},
    parameters::{testnet::RegtestParameters, Network},
    serialization::ZcashDeserialize,
};

use super::*;
use crate::{
    verify_plan, AlarmSet, BranchId, CheckpointSet, EngineConfig, FinalityEpoch,
    HeaderChainDiskVersion, HeaderContextFact, HeaderGeneration, PreparedHeader,
    PreparedHeaderBatch, SourceId, TargetCompletion, TrustedAnchor, ValidationLease,
    VerifiedGeneration,
};

#[derive(Clone)]
struct TestStore {
    graph: MemHeaderStore,
    metadata: EngineMetadata,
    selected: Vec<Frontier>,
    verified: Vec<Frontier>,
    lease: ValidationLease,
    finality: Vec<FinalityRecord>,
    aux: Vec<crate::AuxDelivery>,
}

impl TestStore {
    fn new(mode: EngineMode) -> (Self, EngineConfig) {
        let block = regtest_genesis_block();
        let frontier = Frontier::new(block::Height(0), block.hash());
        let work = block
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest genesis target has valid work");
        let graph = MemHeaderStore::new(frontier, block.header.clone(), work, work.as_u256())
            .expect("the fixture anchor header matches its hash");
        let config = EngineConfig::new(
            mode,
            Network::new_regtest(RegtestParameters::default()),
            TrustedAnchor {
                frontier,
                header: block.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the fixture configuration is coherent");
        let score = graph.score(frontier.hash).expect("the anchor is retained");
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
            header_best_score: score,
            oldest_retained_height: frontier.height,
            alarms: AlarmSet::default(),
            last_transition_id: EvidenceId::from_digest([0xff; 32]),
        };
        let lease = ValidationLease {
            parent: frontier,
            predecessors: vec![HeaderContextFact {
                frontier,
                difficulty_threshold: block.header.difficulty_threshold,
                time: block.header.time,
            }],
            trust_anchor_digest: config.trust_anchor_digest(),
            context_digest: [7; 32],
        };
        (
            Self {
                graph,
                metadata,
                selected: vec![frontier],
                verified: vec![frontier],
                lease,
                finality: Vec::new(),
                aux: Vec::new(),
            },
            config,
        )
    }

    fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            mode: self.metadata.mode,
            state_version: self.metadata.state_version,
            header_generation: self.metadata.header_generation,
            verified_generation: self.metadata.verified_generation,
            frontiers: self.metadata.frontiers,
            header_best_score: self.metadata.header_best_score,
            oldest_retained_height: self.metadata.oldest_retained_height,
            alarms: self.metadata.alarms.clone(),
        }
    }

    fn commit(&mut self, plan: &TransitionPlan) {
        self.graph = plan.projected.clone();
        self.metadata = plan.change_set.metadata.clone();
        apply_projection(&mut self.selected, &plan.change_set.selected_projection);
        apply_projection(&mut self.verified, &plan.change_set.verified_projection);
        if let Some(record) = plan.change_set.finality_append {
            self.finality.push(record);
        }
        for change in &plan.change_set.aux_changes {
            match change {
                crate::AuxDelta::Put(delivery) => {
                    self.aux
                        .retain(|existing| existing.delivery_id != delivery.delivery_id);
                    self.aux.push(**delivery);
                }
                crate::AuxDelta::Delete { delivery_id, .. } => {
                    self.aux
                        .retain(|existing| existing.delivery_id != *delivery_id);
                }
            }
        }
        self.lease.parent = self.metadata.frontiers.header_best;
        self.lease.context_digest[0] = self.lease.context_digest[0].wrapping_add(1);
    }
}

struct ManualClock(DateTime<Utc>);
impl super::super::Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

struct Authority;
impl super::super::FullStateEvidenceAuthority for Authority {
    fn authorizes(&self, _evidence: EvidenceId) -> bool {
        true
    }
}

fn context<'a>(
    config: &'a EngineConfig,
    clock: &'a ManualClock,
    authority: Option<&'a Authority>,
) -> TransitionContext<'a> {
    let full_state_authority = authority.map(|item| {
        // This trait-object coercion preserves the borrowed fixture's lifetime and identity.
        item as &dyn super::super::FullStateEvidenceAuthority
    });
    TransitionContext {
        config,
        clock,
        full_state_authority,
        retention_references: &[],
    }
}

fn test_engine(store: &TestStore) -> crate::HeaderChainEngine {
    crate::HeaderChainEngine::from_audited_state(
        store.graph.clone(),
        store.metadata.clone(),
        store.selected.clone(),
        store.verified.clone(),
        store.aux.clone(),
    )
    .expect("the planner fixture is coherent before transition")
}

fn apply_transition(
    store: &TestStore,
    request: TransitionRequest,
    context: &TransitionContext<'_>,
) -> Result<TransitionPlan, TransitionFailure> {
    let engine = crate::HeaderChainEngine::from_audited_state(
        store.graph.clone(),
        store.metadata.clone(),
        store.selected.clone(),
        store.verified.clone(),
        store.aux.clone(),
    )
    .map_err(|_| TransitionFailure::InvalidEvidence("planner fixture engine is incoherent"))?;
    let durable = match &request.event {
        TransitionEvent::InsertHeaders(_) => {
            DurableTransitionFacts::ValidationContext(store.lease.clone())
        }
        TransitionEvent::MigratedPinRefutation(event) => {
            DurableTransitionFacts::MigratedFinalityPin(
                store
                    .finality
                    .iter()
                    .any(|record| {
                        record.current == event.pin
                            && matches!(record.source, FinalitySource::MigratedHeadersOnly)
                    })
                    .then_some(event.pin),
            )
        }
        _ => DurableTransitionFacts::None,
    };
    engine
        .apply(request, context, durable)
        .map(crate::EngineTransition::into_plan)
}

fn batch(
    parent: Frontier,
    count: u32,
    trust_anchor_digest: [u8; 32],
    evidence: EvidenceId,
) -> PreparedHeaderBatch {
    let mut headers = Vec::new();
    let mut parent_hash = parent.hash;
    for offset in 1..=count {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent_hash;
        let seconds = i64::from(parent.height.0)
            .checked_add(i64::from(offset))
            .expect("the fixture height fits in timestamp arithmetic");
        header.time = regtest_genesis_block()
            .header
            .time
            .checked_add_signed(chrono::Duration::seconds(seconds))
            .expect("the fixture timestamp remains representable");
        let mut nonce = [0; 32];
        nonce[..4].copy_from_slice(&offset.to_le_bytes());
        header.nonce = nonce.into();
        let header = Arc::new(header);
        let hash = header.hash();
        headers.push(PreparedHeader {
            header: header.clone(),
            hash,
            height: block::Height(parent.height.0 + offset),
            block_work: header
                .difficulty_threshold
                .to_work()
                .expect("the fixture target has valid work"),
            validation: HeaderValidationState::Valid,
        });
        parent_hash = hash;
    }
    PreparedHeaderBatch::new(headers, parent, trust_anchor_digest, evidence)
        .expect("the fixture batch is nonempty")
}

fn insertion(store: &TestStore, count: u32, evidence: EvidenceId) -> TransitionRequest {
    let batch = batch(
        store.lease.parent,
        count,
        store.lease.trust_anchor_digest,
        evidence,
    );
    let target = batch.headers().last().expect("the batch is nonempty").hash;
    TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::InsertHeaders(Box::new(crate::InsertHeaders {
            owner: WorkOwner {
                state_version: store.metadata.state_version,
                header_generation: store.metadata.header_generation,
                verified_generation: None,
                branch: BranchId::new(store.metadata.frontiers.finalized.hash, target),
                session_id: 1,
                request_id: NonZeroU64::new(1).expect("one is nonzero"),
            },
            source: SourceId::from_digest([3; 32]),
            parent_hash: store.lease.parent.hash,
            target_tip_hash: target,
            completion: TargetCompletion::TargetComplete {
                common_ancestor: store.lease.parent,
            },
            batch,
            aux: Vec::new(),
        })),
    }
}

fn assert_next_child_commits(
    store: &TestStore,
    config: &EngineConfig,
    clock: &ManualClock,
    expected_parent: Frontier,
    evidence: u8,
) {
    assert_eq!(store.metadata.frontiers.header_best, expected_parent);
    let mut committed = store.clone();
    let request = insertion(&committed, 1, EvidenceId::from_digest([evidence; 32]));
    let child = match &request.event {
        TransitionEvent::InsertHeaders(event) => {
            assert_eq!(event.parent_hash, expected_parent.hash);
            assert_eq!(
                event.completion,
                TargetCompletion::TargetComplete {
                    common_ancestor: expected_parent,
                }
            );
            event.target_tip_hash
        }
        _ => unreachable!("the next-child helper constructs one insertion"),
    };
    let plan = apply_transition(&committed, request, &context(config, clock, None))
        .expect("the exact selected parent accepts its next child");
    assert_eq!(
        plan.change_set.metadata.frontiers.header_best,
        Frontier::new(
            expected_parent
                .height
                .next()
                .expect("the bounded test parent has a next height"),
            child,
        )
    );
    committed.commit(&plan);
    assert_eq!(
        committed
            .graph
            .node(child)
            .expect("the committed next child is retained")
            .parent_hash,
        expected_parent.hash
    );
}

fn insert_verified_branch(
    graph: &mut MemHeaderStore,
    parent: Frontier,
    count: u32,
    difficulty: zakura_chain::work::difficulty::CompactDifficulty,
    nonce_seed: u8,
) -> Frontier {
    let mut parent = parent;
    for offset in 0..count {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent.hash;
        header.difficulty_threshold = difficulty;
        header.nonce.0[0] = nonce_seed;
        header.nonce.0[1..5].copy_from_slice(&offset.to_be_bytes());
        let header = Arc::new(header);
        let work = header
            .difficulty_threshold
            .to_work()
            .expect("the fixture target has valid work");
        parent = match graph
            .insert(
                header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Verified {
                    evidence: EvidenceId::from_digest([nonce_seed; 32]),
                },
            )
            .expect("the verified fixture branch links to its parent")
        {
            crate::InsertResult::Inserted(frontier)
            | crate::InsertResult::AlreadyPresent(frontier) => frontier,
        };
    }
    parent
}

fn synchronize_fixture(store: &mut TestStore, verified_tip: Frontier) {
    store
        .graph
        .recompute_all_eligibility()
        .expect("the fixture eligibility cache recomputes");
    let header_best = store
        .graph
        .select_header_best()
        .expect("the fixture has an eligible tip")
        .0;
    store.selected = path(&store.graph, header_best).expect("the selected path is retained");
    store.verified = path(&store.graph, verified_tip).expect("the verified path is retained");
    store.metadata.frontiers.header_best = header_best;
    store.metadata.frontiers.verified_best = verified_tip;
    store.metadata.header_best_score = store
        .graph
        .score(header_best.hash)
        .expect("the selected score is exact");
}

fn operator_invalidate(
    store: &TestStore,
    target: block::Hash,
    id: crate::OperatorInvalidationId,
    evidence: u8,
) -> TransitionRequest {
    TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::OperatorInvalidate(crate::OperatorInvalidate {
            target,
            id,
            operator_reason_digest: [evidence.wrapping_add(1); 32],
            evidence: EvidenceId::from_digest([evidence; 32]),
        }),
    }
}

fn operator_reconsider(
    store: &TestStore,
    target: block::Hash,
    id: crate::OperatorInvalidationId,
    evidence: u8,
) -> TransitionRequest {
    TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::OperatorReconsider(crate::OperatorReconsider {
            target,
            id,
            evidence: EvidenceId::from_digest([evidence; 32]),
        }),
    }
}

fn apply_projection(projection: &mut Vec<Frontier>, delta: &ProjectionDelta) {
    if let Some(height) = delta.remove_before {
        projection.retain(|frontier| frontier.height >= height);
    }
    if let Some(height) = delta.remove_from {
        projection.retain(|frontier| frontier.height < height);
    }
    projection.extend(delta.put.iter().copied());
}
