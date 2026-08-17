mod auxiliary;
mod body_evidence;
mod coherence;
mod counters;
mod deferred;
mod finality;
mod guards;
mod insertion;
mod operator;
mod predecessor_context;
mod selection;

use std::{borrow::Cow, collections::HashMap, num::NonZeroU64, sync::Arc};

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block, Block},
    parameters::{testnet::RegtestParameters, Network},
    serialization::ZcashDeserialize,
};

use super::{projected_state::path, EngineTransition, TransitionFailure};
use crate::{
    verify_plan, AlarmSet, AuxiliaryViolation, BodyEvidence, BodyValidationState, BodyViolation,
    BranchId, CheckpointSet, EligibilityReason, EngineConfig, EngineMetadata, EngineMode,
    EngineSnapshot, EvidenceId, FinalityEpoch, FinalityRecord, FinalitySource, FinalityViolation,
    Frontier, FrontierSet, GraphError, HeaderChainDiskVersion, HeaderContextFact, HeaderGeneration,
    HeaderInsertionFacts, HeaderPathKind, HeaderPathProblem, HeaderValidationCheck,
    HeaderValidationFacts, HeaderValidationState, HeaderViolation, HeaderWorkEffect,
    InvalidTransitionEvidence, LimitViolation, MemHeaderStore, OperatorViolation, PreparedHeader,
    PreparedHeaderBatch, ProjectionDelta, SourceId, StateVersion, TargetCompletion,
    TransitionContext, TransitionDomain, TransitionEffect, TransitionEvent, TransitionInput,
    TransitionRequest, TrustedAnchor, ValidationLease, VerifiedGeneration,
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
    context_archive: HashMap<block::Hash, (block::Height, Arc<block::Header>)>,
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
        let score = graph
            .header_chain_score(frontier.hash)
            .expect("the anchor is retained");
        let metadata = EngineMetadata {
            disk_format: HeaderChainDiskVersion::CURRENT,
            mode,
            network_id: config.network.kind(),
            anchor_manifest_digest: config.trust_anchor_digest(),
            work_origin: frontier,
            state_version: StateVersion::new(0),
            header_generation: HeaderGeneration::new(0),
            verified_generation: VerifiedGeneration::new(0),
            finality_epoch: FinalityEpoch::new(0),
            headers_only_migration_epoch: None,
            frontiers: FrontierSet {
                finalized: frontier,
                header_best: frontier,
                verified_best: frontier,
            },
            header_best_score: score,
            oldest_retained_height: frontier.height,
            alarms: AlarmSet::default(),
            last_transition: None,
        };
        let lease = ValidationLease {
            parent: frontier,
            predecessors: vec![HeaderContextFact {
                frontier,
                header: block.header.clone(),
            }],
            network: config.network.clone(),
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
                context_archive: HashMap::from([(
                    frontier.hash,
                    (frontier.height, block.header.clone()),
                )]),
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

    fn commit(&mut self, plan: &EngineTransition) {
        for node in self.graph.header_nodes() {
            self.context_archive
                .insert(node.hash, (node.height, node.header.clone()));
        }
        self.graph
            .apply_delta(plan.graph_delta())
            .expect("the verified transition delta applies to its base graph");
        for node in self.graph.header_nodes() {
            self.context_archive
                .insert(node.hash, (node.height, node.header.clone()));
        }
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
        let parent = self.metadata.frontiers.header_best;
        let required = usize::try_from(parent.height.0)
            .expect("the test height fits in memory")
            .saturating_add(1)
            .min(crate::POW_ADJUSTMENT_BLOCK_SPAN);
        let mut predecessors = Vec::with_capacity(required);
        let mut hash = parent.hash;
        while predecessors.len() < required {
            let (height, header) = self
                .graph
                .header_node(hash)
                .map(|node| (node.height, node.header.clone()))
                .or_else(|| self.context_archive.get(&hash).cloned())
                .expect("the test archive retains every contextual predecessor");
            predecessors.push(HeaderContextFact {
                frontier: Frontier::new(height, hash),
                header: header.clone(),
            });
            hash = header.previous_block_hash;
        }
        self.lease = ValidationLease::new(
            parent,
            predecessors,
            self.lease.network().clone(),
            self.lease.trust_anchor_digest(),
        );
    }
}

fn projected_graph(base_graph: &MemHeaderStore, plan: &EngineTransition) -> MemHeaderStore {
    let mut projected_graph = base_graph.clone();
    projected_graph
        .apply_delta(plan.graph_delta())
        .expect("the planned graph transition applies to its base graph");
    projected_graph
}

struct ManualClock(DateTime<Utc>);
impl super::super::Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

struct Authority;
impl super::super::FullStateEvidenceAuthority for Authority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        true
    }

    fn authorizes_scheduler_retry(&self, _retry: &crate::OperatorBodyRetry) -> bool {
        true
    }

    fn authorizes_header_completion(&self, _insert: &crate::InsertHeaders) -> bool {
        true
    }

    fn authorizes_validation_lease(&self, _lease: &crate::ValidationLease) -> bool {
        true
    }
}

struct CompletionAuthority;
impl super::super::FullStateEvidenceAuthority for CompletionAuthority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        false
    }

    fn authorizes_header_completion(&self, _insert: &crate::InsertHeaders) -> bool {
        true
    }

    fn authorizes_scheduler_retry(&self, _retry: &crate::OperatorBodyRetry) -> bool {
        true
    }

    fn authorizes_validation_lease(&self, _lease: &crate::ValidationLease) -> bool {
        true
    }
}

static COMPLETION_AUTHORITY: CompletionAuthority = CompletionAuthority;

fn context<'a>(
    config: &'a EngineConfig,
    clock: &'a ManualClock,
    authority: Option<&'a Authority>,
) -> TransitionContext<'a> {
    let full_state_authority = Some(match authority {
        Some(item) => item as &dyn super::super::FullStateEvidenceAuthority,
        None => &COMPLETION_AUTHORITY as &dyn super::super::FullStateEvidenceAuthority,
    });
    TransitionContext {
        config,
        clock,
        full_state_authority,
        retention_references: &[],
    }
}

fn test_engine(store: &TestStore) -> crate::HeaderChainEngine {
    crate::HeaderChainEngine::from_untrusted_durable_state(
        store.graph.clone(),
        store.metadata.clone(),
        store.selected.clone(),
        store.verified.clone(),
        untrusted_aux_rows(store),
    )
    .expect("the planner fixture is coherent before transition")
}

fn untrusted_aux_rows(store: &TestStore) -> Vec<crate::UntrustedAuxDeliveryRow> {
    store
        .aux
        .iter()
        .map(|delivery| {
            let status = match delivery.outcome().status() {
                crate::AuxOutcomeStatus::Unauthenticated => 0,
                crate::AuxOutcomeStatus::Authenticated => 1,
                crate::AuxOutcomeStatus::Rejected => 2,
                crate::AuxOutcomeStatus::Disputed => 3,
            };
            let observations = delivery
                .observation_ids()
                .map(|id| id.map(|id| id.digest()));
            let base = crate::AuxDelivery::new(
                delivery.delivery_id,
                delivery.header_hash,
                delivery.source,
                delivery.owner,
                delivery.body_size,
                delivery.tree_aux,
            );
            crate::UntrustedAuxDeliveryRow::new(
                base,
                status,
                observations,
                delivery.outcome_boundary_hash(),
            )
        })
        .collect()
}

fn fixture_transition_input(store: &TestStore, request: TransitionRequest) -> TransitionInput {
    let expected_version = request.expected_version;
    match request.event {
        TransitionEvent::InsertHeaders(event) => TransitionInput::InsertHeaders {
            event,
            facts: HeaderInsertionFacts {
                validation: HeaderValidationFacts {
                    validation_leases: vec![store.lease.clone()],
                },
                finality_rebase_history: Vec::new(),
            },
        },
        TransitionEvent::VerifiedChainChanged(event) => TransitionInput::VerifiedChainChanged {
            expected_version,
            event,
            facts: HeaderValidationFacts {
                validation_leases: vec![store.lease.clone()],
            },
        },
        TransitionEvent::VerifiedBlockAccepted(event) => TransitionInput::VerifiedBlockAccepted {
            expected_version,
            event,
            facts: HeaderValidationFacts {
                validation_leases: vec![store.lease.clone()],
            },
        },
        TransitionEvent::BodyEvidence(event) => TransitionInput::BodyEvidence {
            expected_version,
            event,
        },
        TransitionEvent::BodySupplierDiscovered(event) => TransitionInput::BodySupplierDiscovered {
            expected_version,
            event,
        },
        TransitionEvent::OperatorBodyRetry(event) => TransitionInput::OperatorBodyRetry {
            expected_version,
            event,
        },
        TransitionEvent::OperatorInvalidate(event) => TransitionInput::OperatorInvalidate {
            expected_version,
            event,
        },
        TransitionEvent::OperatorReconsider(event) => TransitionInput::OperatorReconsider {
            expected_version,
            event,
        },
        TransitionEvent::FullStateFinalized(event) => TransitionInput::FullStateFinalized {
            expected_version,
            event,
        },
        TransitionEvent::MigratedPinRefutation(event) => {
            let preserved_pin = store
                .finality
                .iter()
                .any(|record| {
                    record.current == event.pin
                        && matches!(record.source, FinalitySource::MigratedHeadersOnly)
                })
                .then_some(event.pin);
            TransitionInput::MigratedPinRefutation {
                expected_version,
                event,
                preserved_pin,
            }
        }
        TransitionEvent::AuxEvidence(event) => TransitionInput::AuxEvidence { event },
        TransitionEvent::ReevaluateDeferred => {
            TransitionInput::ReevaluateDeferred { expected_version }
        }
    }
}

#[allow(clippy::unwrap_in_result)]
fn apply_transition(
    store: &TestStore,
    request: TransitionRequest,
    context: &TransitionContext<'_>,
) -> Result<EngineTransition, TransitionFailure> {
    let engine = crate::HeaderChainEngine::from_untrusted_durable_state(
        store.graph.clone(),
        store.metadata.clone(),
        store.selected.clone(),
        store.verified.clone(),
        untrusted_aux_rows(store),
    )
    .expect("the planner fixture is coherent before transition");
    let input = fixture_transition_input(store, request);
    let plan = engine.plan_transition(input, context)?;
    if let Err(error) = crate::verify_plan_production(&engine, &plan) {
        panic!(
            "production overlay and boundary-node verification must accept plans that pass the exhaustive verifier: {error:?}"
        );
    }
    Ok(plan)
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
    PreparedHeaderBatch::new(
        headers,
        parent,
        Network::new_regtest(RegtestParameters::default()),
        trust_anchor_digest,
        evidence,
    )
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
            owner: crate::HeaderWorkOwner {
                authority: crate::HeaderWorkAuthority {
                    header_generation: store.metadata.header_generation,
                    branch: BranchId::new(store.metadata.frontiers.finalized.hash, target),
                },
                session_id: 1,
                request_id: NonZeroU64::new(1).expect("one is nonzero"),
            }
            .into(),
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
            .header_node(child)
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
        parent = match graph
            .insert(
                header,
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
        .recompute_all_header_eligibility()
        .expect("the fixture eligibility cache recomputes");
    let header_best = store
        .graph
        .select_best_header_chain()
        .expect("the fixture has an eligible tip")
        .0;
    store.selected = path(&store.graph, header_best).expect("the selected path is retained");
    store.verified = path(&store.graph, verified_tip).expect("the verified path is retained");
    store.metadata.frontiers.header_best = header_best;
    store.metadata.frontiers.verified_best = verified_tip;
    store.metadata.header_best_score = store
        .graph
        .header_chain_score(header_best.hash)
        .expect("the selected score is exact");
}

fn operator_invalidate(
    store: &TestStore,
    target: block::Hash,
    id: crate::OperatorInvalidationId,
    evidence: u8,
) -> TransitionRequest {
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest as _;
    hasher.update(b"zakura-operator-invalidation-v1");
    hasher.update(target.0);
    hasher.update(id.bytes());
    TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::OperatorInvalidate(crate::OperatorInvalidate {
            target,
            id,
            operator_reason_digest: hasher.finalize().into(),
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
    let invalidation_evidence = store.graph.header_node(target).and_then(|node| {
        node.eligibility
            .direct_reasons
            .iter()
            .find_map(|reason| match reason {
                EligibilityReason::OperatorInvalid {
                    id: existing,
                    evidence,
                    ..
                } if *existing == id => Some(*evidence),
                _ => None,
            })
    });
    TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::OperatorReconsider(crate::OperatorReconsider {
            target,
            id,
            invalidation_evidence,
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

#[test]
fn path_rejects_zero_height_tip_that_is_not_finalized() {
    let (mut store, _) = TestStore::new(EngineMode::HeadersOnly);
    let anchor = store.metadata.frontiers.finalized;
    let difficulty = regtest_genesis_block().header.difficulty_threshold;
    let child = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0xa1);
    let malformed = Frontier::new(block::Height::MIN, child.hash);

    assert_eq!(
        path(&store.graph, malformed),
        Err(TransitionFailure::Graph(
            GraphError::FinalizedFrontierNotDescendant {
                current: anchor.hash,
                candidate: child.hash,
            }
        ))
    );
}
