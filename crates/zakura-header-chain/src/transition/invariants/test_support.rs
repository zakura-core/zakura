use std::{num::NonZeroU64, sync::Arc};

use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};

use crate::graph::{GraphDelta, GraphOverlay};
use crate::{
    AlarmSet, AuxDelivery, BodySizeHint, BodyValidationState, ChangeSet, CheckpointSet,
    EngineConfig, EngineLimits, EngineMetadata, EngineMode, EvidenceId, FinalityEffect,
    FinalityRecord, FinalitySource, Frontier, FrontierSet, HeaderChainDiskVersion,
    HeaderChainEngine, HeaderGeneration, HeaderValidationState, IndexChanges, PlanCandidate,
    ProjectionDelta, SourceId, StateVersion, TransitionDomain, TransitionEffect, TrustedAnchor,
    VerifiedGeneration,
};

pub(super) struct Fixture {
    pub(super) engine: HeaderChainEngine,
    pub(super) anchor: Frontier,
    pub(super) child: Frontier,
}

pub(super) fn hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

pub(super) fn fixture(mode: EngineMode) -> Fixture {
    let block = regtest_genesis_block();
    let anchor = Frontier::new(block::Height(0), block.hash());
    let work = block
        .header
        .difficulty_threshold
        .to_work()
        .expect("the regtest genesis target has valid work");
    let mut graph = crate::MemHeaderStore::new(anchor, block.header.clone(), work, work.as_u256())
        .expect("the fixture anchor header matches its hash");
    let config = EngineConfig::new(
        mode,
        Network::new_regtest(RegtestParameters::default()),
        TrustedAnchor {
            frontier: anchor,
            header: block.header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the fixture configuration is coherent");

    let mut child_header = *block.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.nonce.0[0] = 1;
    let child_header = Arc::new(child_header);
    let child = match graph
        .insert(
            child_header,
            HeaderValidationState::Valid,
            [],
            BodyValidationState::Unknown,
        )
        .expect("the fixture child links to the anchor")
    {
        crate::InsertResult::Inserted(frontier) | crate::InsertResult::AlreadyPresent(frontier) => {
            frontier
        }
    };
    let score = graph
        .header_chain_score(child.hash)
        .expect("the fixture child has an exact score");
    let metadata = EngineMetadata {
        disk_format: HeaderChainDiskVersion::CURRENT,
        mode,
        network_id: config.network.kind(),
        anchor_manifest_digest: config.trust_anchor_digest(),
        work_origin: anchor,
        state_version: StateVersion::new(0),
        header_generation: HeaderGeneration::new(0),
        verified_generation: VerifiedGeneration::new(0),
        finality_epoch: crate::FinalityEpoch::new(0),
        headers_only_migration_epoch: None,
        frontiers: FrontierSet {
            finalized: anchor,
            header_best: child,
            verified_best: anchor,
        },
        header_best_score: score,
        oldest_retained_height: anchor.height,
        alarms: AlarmSet::default(),
        last_transition: None,
    };
    let engine = HeaderChainEngine::from_audited_state(
        graph,
        metadata,
        vec![anchor, child],
        vec![anchor],
        [],
    )
    .expect("the invariant fixture is coherent");

    Fixture {
        engine,
        anchor,
        child,
    }
}

pub(super) fn candidate_with_delta(
    engine: &HeaderChainEngine,
    graph_delta: GraphDelta,
) -> PlanCandidate {
    let put_nodes = graph_delta.updated_header_nodes().to_vec();
    let delete_nodes = graph_delta.deleted_header_hashes().to_vec();
    let inserted = put_nodes
        .iter()
        .filter(|node| engine.graph().header_node(node.hash).is_none())
        .map(|node| Frontier::new(node.height, node.hash))
        .collect();
    PlanCandidate {
        snapshot_before_commit: engine.snapshot(),
        change_set: ChangeSet {
            put_nodes,
            delete_nodes: delete_nodes.clone(),
            put_consensus_invalid_body_tombstones: graph_delta
                .new_consensus_invalid_body_tombstones()
                .to_vec(),
            index_changes: IndexChanges {
                inserted,
                deleted: delete_nodes,
            },
            selected_projection: ProjectionDelta::default(),
            verified_projection: ProjectionDelta::default(),
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: None,
            metadata: engine.metadata().clone(),
        },
        graph_delta,
        domain: TransitionDomain::ReevaluateDeferred,
        effect: TransitionEffect::none(),
        trust_pins: Vec::new().into(),
        limits: EngineLimits::default(),
    }
}

pub(super) fn no_change_candidate(engine: &HeaderChainEngine) -> PlanCandidate {
    candidate_with_delta(engine, GraphDelta::empty(engine.graph()))
}

pub(super) fn projected_graph(
    engine: &HeaderChainEngine,
    plan: &PlanCandidate,
) -> crate::MemHeaderStore {
    let mut graph = engine.graph().clone();
    graph
        .apply_delta(plan.graph_delta())
        .expect("the fixture plan has a valid graph delta");
    graph
}

pub(super) fn checkpoint_fixture() -> (Fixture, PlanCandidate) {
    let fixture = fixture(EngineMode::Integrated);
    let mut overlay = GraphOverlay::new(fixture.engine.graph());
    overlay
        .set_body_validation_state(
            fixture.child.hash,
            BodyValidationState::Verified {
                evidence: EvidenceId::from_digest([0x41; 32]),
            },
        )
        .expect("the checkpoint child accepts verified body state");
    overlay
        .advance_finalized_frontier(fixture.child)
        .expect("the checkpoint child is an eligible finalized descendant");
    let graph_delta = overlay.delta();
    let mut plan = candidate_with_delta(&fixture.engine, graph_delta);
    let metadata = &mut plan.change_set.metadata;
    metadata.state_version = metadata
        .state_version
        .checked_next()
        .expect("the fixture state version can advance");
    metadata.header_generation = metadata
        .header_generation
        .checked_next()
        .expect("the fixture header generation can advance");
    metadata.verified_generation = metadata
        .verified_generation
        .checked_next()
        .expect("the fixture verified generation can advance");
    metadata.finality_epoch = metadata
        .finality_epoch
        .checked_next()
        .expect("the fixture finality epoch can advance");
    metadata.frontiers.finalized = fixture.child;
    metadata.frontiers.verified_best = fixture.child;
    metadata.header_best_score = overlay
        .header_chain_score(fixture.child.hash)
        .expect("the projected finalized child has an exact score");
    metadata.oldest_retained_height = fixture.child.height;
    plan.change_set.selected_projection = ProjectionDelta {
        remove_before: Some(fixture.child.height),
        remove_from: None,
        put: Vec::new(),
    };
    plan.change_set.verified_projection = ProjectionDelta {
        remove_before: Some(fixture.child.height),
        remove_from: Some(fixture.child.height),
        put: vec![fixture.child],
    };
    plan.change_set.finality_append = Some(FinalityRecord {
        previous: fixture.anchor,
        current: fixture.child,
        source: FinalitySource::FullState {
            evidence: EvidenceId::from_digest([0x42; 32]),
        },
        epoch: metadata.finality_epoch,
    });
    plan.domain = TransitionDomain::VerifiedChainChanged;
    plan.effect = TransitionEffect {
        finality: Some(FinalityEffect::Checkpoint),
        ..TransitionEffect::none()
    };
    (fixture, plan)
}

pub(super) fn delivery(
    engine: &HeaderChainEngine,
    header_hash: block::Hash,
    delivery_id: EvidenceId,
) -> AuxDelivery {
    let owner = crate::HeaderWorkAuthority::for_target(&engine.snapshot(), header_hash)
        .bind(
            1,
            NonZeroU64::new(1).expect("the fixture request identifier is nonzero"),
        )
        .into();
    AuxDelivery::new(
        delivery_id,
        header_hash,
        SourceId::from_digest([0x51; 32]),
        owner,
        BodySizeHint::Unknown,
        None,
    )
}
