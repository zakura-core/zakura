//! This integration test covers production transition verification and application.

use std::{num::NonZeroU64, sync::Arc};

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};
use zakura_header_chain::{
    prepare_headers, AlarmSet, AuxDelivery, AuxEvidence, AuxObservationV1, AuxVerificationFactV1,
    AuxiliaryViolation, BodySizeHint, BodyWorkAuthority, BranchId, CheckpointSet, Clock,
    EngineConfig, EngineMetadata, EngineMode, EvidenceId, FinalityEpoch, Frontier, FrontierSet,
    FullStateEvidenceAuthority, HeaderBatchInput, HeaderChainDiskVersion, HeaderChainEngine,
    HeaderContextFact, HeaderGeneration, HeaderInsertionFacts, HeaderRules, HeaderValidationFacts,
    HeaderWorkAuthority, InvalidTransitionEvidence, MemHeaderStore, SourceId, StateVersion,
    TargetCompletion, TransitionContext, TransitionEvent, TransitionFailure, TransitionInput,
    TransitionRequest, TreeAuxRecordV1, TrustedAnchor, ValidationLease, VerifiedGeneration,
};

struct FixedClock(DateTime<Utc>);

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

struct Authority;

impl FullStateEvidenceAuthority for Authority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        true
    }

    fn authorizes_header_completion(&self, _insert: &zakura_header_chain::InsertHeaders) -> bool {
        true
    }

    fn authorizes_validation_lease(&self, _lease: &ValidationLease) -> bool {
        true
    }
}

fn engine_fixture() -> (HeaderChainEngine, EngineConfig, ValidationLease) {
    let genesis = regtest_genesis_block();
    let anchor = Frontier::new(block::Height(0), genesis.hash());
    let work = genesis
        .header
        .difficulty_threshold
        .to_work()
        .expect("the regtest target has valid work");
    let graph = MemHeaderStore::new(anchor, genesis.header.clone(), work, work.as_u256())
        .expect("the fixture anchor is coherent");
    let config = EngineConfig::new(
        EngineMode::Integrated,
        Network::new_regtest(RegtestParameters::default()),
        TrustedAnchor {
            frontier: anchor,
            header: genesis.header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the fixture configuration is coherent");
    let metadata = EngineMetadata {
        disk_format: HeaderChainDiskVersion::CURRENT,
        mode: EngineMode::Integrated,
        network_id: config.network().kind(),
        network_policy_digest: config.network_policy_digest(),
        anchor_manifest_digest: config.trust_anchor_digest(),
        work_origin: anchor,
        state_version: StateVersion::new(0),
        header_generation: HeaderGeneration::new(0),
        verified_generation: VerifiedGeneration::new(0),
        finality_epoch: FinalityEpoch::new(0),
        headers_only_migration_epoch: None,
        frontiers: FrontierSet {
            finalized: anchor,
            header_best: anchor,
            verified_best: anchor,
        },
        header_best_score: graph
            .header_chain_score(anchor.hash)
            .expect("the anchor has a score"),
        oldest_retained_height: anchor.height,
        alarms: AlarmSet::default(),
        last_transition: None,
    };
    let lease = ValidationLease::new(
        anchor,
        vec![HeaderContextFact {
            frontier: anchor,
            header: genesis.header.clone(),
        }],
        config.network().clone(),
        config.trust_anchor_digest(),
    );
    let engine =
        HeaderChainEngine::from_audited_state(graph, metadata, vec![anchor], vec![anchor], [])
            .expect("the fixture engine is coherent");
    (engine, config, lease)
}

#[test]
fn production_incremental_verifier_accepts_exact_auxiliary_evidence() {
    let (mut engine, config, lease) = engine_fixture();
    let authority = Authority;
    let clock = FixedClock(Utc::now());
    let context = TransitionContext {
        config: &config,
        clock: &clock,
        full_state_authority: Some(&authority),
        retention_references: &[],
    };
    let mut parent_hash = lease.parent().hash;
    let mut headers = Vec::new();
    for marker in 1..=2 {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent_hash;
        header.time = header
            .time
            .checked_add_signed(chrono::Duration::seconds(marker))
            .expect("the fixture timestamp remains representable");
        header.nonce = [marker as u8; 32].into();
        let header = Arc::new(header);
        parent_hash = header.hash();
        headers.push(header);
    }
    let rules = HeaderRules::for_validation_lease(&lease).expect("the fixture rules are valid");
    let batch = prepare_headers(
        HeaderBatchInput::new(&headers),
        lease.parent(),
        &rules,
        &clock,
    )
    .expect("the fixture headers prepare");
    let header_hash = batch.headers()[0].hash;
    let boundary_hash = batch.headers()[1].hash;
    let target_tip_hash = boundary_hash;
    let owner = HeaderWorkAuthority {
        header_generation: engine.snapshot().header_generation,
        branch: BranchId::new(engine.snapshot().frontiers.finalized.hash, target_tip_hash),
    }
    .bind(
        1,
        NonZeroU64::new(1).expect("the fixture request ID is nonzero"),
    );
    let source = SourceId::from_digest([3; 32]);
    let delivery = AuxDelivery::new(
        EvidenceId::from_digest([4; 32]),
        header_hash,
        source,
        owner.into(),
        BodySizeHint::Unknown,
        Some(TreeAuxRecordV1 {
            height: block::Height(1),
            sapling_root: zakura_chain::sapling::tree::Root::default(),
            orchard_root: zakura_chain::orchard::tree::Root::default(),
            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
            sapling_tx_count: 0,
            orchard_tx_count: 0,
            ironwood_tx_count: 0,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([5; 32]),
        }),
    );
    let insertion = TransitionRequest {
        expected_version: engine.snapshot().state_version,
        event: TransitionEvent::InsertHeaders(Box::new(zakura_header_chain::InsertHeaders {
            owner: owner.into(),
            source,
            parent_hash: lease.parent().hash,
            target_tip_hash,
            completion: TargetCompletion::TargetComplete {
                common_ancestor: lease.parent(),
            },
            batch,
            aux: vec![delivery],
        })),
    };
    let TransitionEvent::InsertHeaders(event) = insertion.event else {
        panic!("fixture constructs InsertHeaders");
    };
    let transition = engine
        .plan_transition(
            TransitionInput::InsertHeaders {
                event,
                facts: HeaderInsertionFacts {
                    validation: HeaderValidationFacts {
                        validation_leases: vec![lease],
                    },
                    finality_rebase_history: Vec::new(),
                },
            },
            &context,
        )
        .expect("the fixture insertion verifies");
    engine
        .install_committed_transition(transition)
        .expect("the fixture insertion commits");

    let request = |delivery| TransitionRequest {
        expected_version: engine.snapshot().state_version,
        event: TransitionEvent::AuxEvidence(Box::new(AuxEvidence::observed(
            AuxObservationV1::from_vct(
                BodyWorkAuthority::for_snapshot(&engine.snapshot()).bind(
                    2,
                    NonZeroU64::new(2).expect("the fixture request ID is nonzero"),
                ),
                vec![delivery],
                AuxVerificationFactV1::current_delivery_verified(),
                Some([6; 32].into()),
            )
            .expect("the observation fixture is valid"),
        ))),
    };
    let mut altered = delivery;
    altered.source = SourceId::from_digest([7; 32]);
    assert!(matches!(
        engine.plan_transition(
            TransitionInput::AuxEvidence {
                event: {
                    let TransitionEvent::AuxEvidence(event) = request(altered).event else {
                        panic!("fixture constructs AuxEvidence");
                    };
                    event
                },
            },
            &context,
        ),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::ProvenanceMismatch)
        ))
    ));

    let TransitionEvent::AuxEvidence(event) = request(delivery).event else {
        panic!("fixture constructs AuxEvidence");
    };
    let transition = engine
        .plan_transition(
            TransitionInput::AuxEvidence {
                event: event.clone(),
            },
            &context,
        )
        .expect("the production incremental verifier accepts exact evidence");
    let stale_after_auxiliary = engine
        .plan_transition(TransitionInput::AuxEvidence { event }, &context)
        .expect("the unchanged source can plan an equivalent auxiliary transition");
    assert!(transition.effect().is_aux_authentication());
    assert_eq!(
        transition.domain(),
        zakura_header_chain::TransitionDomain::AuxEvidence
    );
    engine
        .install_committed_transition(transition)
        .expect("the verified transition applies to its exact source engine");
    assert!(engine.aux_deliveries(header_hash)[0].is_authenticated());
    assert_eq!(
        engine.aux_deliveries(header_hash)[0].outcome_boundary_hash(),
        Some(boundary_hash)
    );
    assert_eq!(
        engine.install_committed_transition(stale_after_auxiliary),
        Err(zakura_header_chain::CommittedTransitionError::StaleSource),
        "an auxiliary-only install consumes the exact source revision",
    );
}
