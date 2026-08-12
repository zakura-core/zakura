//! Authoritative source-audit fail-closed coverage.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use chrono::Duration;
use zakura_chain::{block, block::genesis::regtest_genesis_block};

use super::super::{
    audit_store, audit_store_at, AuditViolation, RecoveryFailure, RecoveryRepair,
    ValidationContextRecord,
};
use super::{fixture, violations};
use crate::{
    AuxAuthentication, AuxDelivery, BodyRuleId, BodySizeHint, BodyValidationState, BranchId,
    ChainScore, CheckpointSet, EligibilityReason, EligibilityState, EngineMode, EvidenceId,
    FinalityEpoch, FinalityRecord, FinalitySource, Frontier, HeaderGeneration, HeaderNode,
    HeaderValidationState, HeaderWorkAuthority, HeaderWorkOwner, SourceId, SuffixWork,
    WorkCoordinate,
};

#[test]
fn canonical_work_and_recovery_time_are_authoritative() {
    let (base, config) = fixture();
    let child_hash = base.nodes[1].hash;

    let mut forged_work = base.clone();
    let forged = zakura_chain::work::difficulty::Work::zero();
    forged_work.nodes[1].block_work = forged;
    forged_work.nodes[1].work_coordinate = forged_work.nodes[0]
        .work_coordinate()
        .checked_add(forged)
        .expect("the forged fixture work remains in range");
    assert!(violations(&forged_work, &config).contains(&AuditViolation::Work(child_hash)));

    let future_now = base.nodes[1].header.time - Duration::hours(3);
    assert!(matches!(
        audit_store_at(&base, &config, future_now),
        Err(RecoveryFailure::Source { violations })
            if violations.contains(&AuditViolation::HeaderValidation(child_hash))
    ));

    let mut elapsed = base.clone();
    let until = elapsed.nodes[1].header.time - Duration::hours(2);
    elapsed.nodes[1].validation = crate::HeaderValidationState::DeferredUntil(until);
    elapsed.deferred = vec![(until, child_hash)];
    let plan = audit_store_at(&elapsed, &config, elapsed.nodes[1].header.time)
        .expect("an exact elapsed deferral is a reconstructible startup transition");
    assert!(plan.repairs.contains(&RecoveryRepair::ElapsedDeferrals));
    assert_eq!(
        plan.header_nodes
            .iter()
            .find(|node| node.hash == child_hash)
            .expect("the child remains retained")
            .validation,
        crate::HeaderValidationState::Valid
    );
}

#[test]
fn persisted_valid_flags_do_not_bypass_header_consensus_validation() {
    let (mut store, config) = fixture();
    let anchor = store.metadata.frontiers.finalized;
    let mut invalid_header = *store.nodes[1].header;
    invalid_header.solution = zakura_chain::work::equihash::Solution::for_proposal();
    let invalid_header = Arc::new(invalid_header);
    let invalid_hash = invalid_header.hash();
    let block_work = invalid_header
        .difficulty_threshold
        .to_work()
        .expect("the independently invalid version retains valid work");
    store.nodes[1] = HeaderNode::from_durable_parts(
        invalid_header,
        invalid_hash,
        anchor.hash,
        block::Height(1),
        block_work,
        store.nodes[0]
            .work_coordinate()
            .checked_add(block_work)
            .expect("the one-block fixture work fits"),
        HeaderValidationState::Valid,
        EligibilityState::default(),
        BodyValidationState::Unknown,
        Vec::new(),
    )
    .expect("durable shape validation is intentionally weaker than consensus validation");
    let invalid = Frontier::new(block::Height(1), invalid_hash);
    store.children = vec![(anchor.hash, invalid_hash)];
    store.selected = vec![anchor, invalid];
    store.metadata.frontiers.header_best = invalid;
    store.metadata.header_best_score =
        ChainScore::new(SuffixWork::new(block_work.as_u256()), invalid_hash);
    store.snapshot = store.metadata.snapshot();
    store.canonical.insert(block::Height(1), invalid_hash);

    assert!(violations(&store, &config).contains(&AuditViolation::HeaderValidation(invalid_hash)));
}

#[test]
fn rebased_work_origin_requires_finality_history_and_canonical_authentication() {
    let (mut store, config) = fixture();
    let anchor_node = store.nodes[0].clone();
    let child = store.metadata.frontiers.header_best;
    let child_node = &mut store.nodes[1];
    child_node.work_coordinate = WorkCoordinate::new(child.hash, Default::default());
    child_node.eligibility.inherited_from = None;
    store.nodes.remove(0);
    store.children.clear();
    store.selected = vec![child];
    store.verified = vec![child];
    store.contexts = vec![ValidationContextRecord {
        header: anchor_node.header,
        height: anchor_node.height,
    }];
    store.metadata.work_origin = child;
    store.metadata.frontiers.finalized = child;
    store.metadata.frontiers.header_best = child;
    store.metadata.frontiers.verified_best = child;
    store.metadata.header_best_score = ChainScore::new(SuffixWork::zero(), child.hash);
    store.metadata.oldest_retained_height = child.height;
    store.metadata.finality_epoch = FinalityEpoch::new(1);
    store.finality.push(FinalityRecord {
        previous: config.bootstrap_anchor().frontier,
        current: child,
        source: FinalitySource::FullState {
            evidence: EvidenceId::from_digest([0x91; 32]),
        },
        epoch: FinalityEpoch::new(1),
    });
    store.snapshot = store.metadata.snapshot();

    audit_store(&store, &config).expect("the authenticated rebased origin recovers");

    store
        .canonical
        .insert(child.height, block::Hash([0x92; 32]));
    assert!(violations(&store, &config).contains(&AuditViolation::Configuration));
}

#[test]
fn finality_and_historical_pins_require_an_independent_canonical_index() {
    let (mut store, mut config) = fixture();
    let child = store.metadata.frontiers.header_best;
    store.metadata.frontiers.finalized = child;
    store.metadata.frontiers.verified_best = child;
    store.metadata.finality_epoch = FinalityEpoch::new(1);
    store.snapshot = store.metadata.snapshot();
    store.verified = vec![child];
    store.nodes[1].body_validation_state = BodyValidationState::Verified {
        evidence: EvidenceId::from_digest([0x71; 32]),
    };
    store.finality.push(FinalityRecord {
        previous: config.bootstrap_anchor().frontier,
        current: child,
        source: FinalitySource::FullState {
            evidence: EvidenceId::from_digest([0x72; 32]),
        },
        epoch: FinalityEpoch::new(1),
    });
    store
        .canonical
        .insert(child.height, block::Hash([0x73; 32]));
    assert!(violations(&store, &config).contains(&AuditViolation::Finality));

    store.canonical.insert(child.height, child.hash);
    config.replace_local_checkpoints(
        CheckpointSet::new([child]).expect("the one-pin fixture is unique"),
    );
    store.metadata.anchor_manifest_digest = config.trust_anchor_digest();
    store.snapshot = store.metadata.snapshot();
    store
        .canonical
        .insert(child.height, block::Hash([0x74; 32]));
    assert!(
        violations(&store, &config).contains(&AuditViolation::TrustPin(child.height, child.hash,))
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
    assert!(violations(&store, &config).contains(&AuditViolation::NodeHash(block::Hash([8; 32]))));

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
        store.nodes[1].body_validation_state.clone(),
        Vec::new(),
    )
    .expect("the isolated node fields remain canonical");
    assert!(violations(&store, &config).contains(&AuditViolation::Work(child_hash)));

    let mut store = base.clone();
    store.nodes[1].body_validation_state = BodyValidationState::ConsensusInvalid {
        evidence: EvidenceId::from_digest([2; 32]),
        rule: BodyRuleId::new("body.rule"),
    };
    let plan = audit_store(&store, &config)
        .expect("body invalidity is authoritative without an eligibility reason");
    assert_eq!(
        plan.selected_projection,
        vec![base.metadata.frontiers.finalized]
    );
    assert!(plan.repairs.contains(&RecoveryRepair::SelectedProjection));

    let mut store = base.clone();
    let corrupted_until = store.nodes[1].header.time + Duration::hours(100);
    store.nodes[1].validation = crate::HeaderValidationState::DeferredUntil(corrupted_until);
    store.deferred = vec![(corrupted_until, child_hash)];
    assert!(violations(&store, &config).contains(&AuditViolation::HeaderValidation(child_hash)));

    let mut store = base.clone();
    store.reasons.push((
        child_hash,
        EligibilityReason::operator_invalid(
            child_hash,
            crate::OperatorInvalidationId::new([3; 16]),
            EvidenceId::from_digest([3; 32]),
        ),
    ));
    assert!(violations(&store, &config).contains(&AuditViolation::EligibilityRoot(child_hash)));

    let mut checkpointed = config.clone();
    checkpointed.replace_local_checkpoints(
        CheckpointSet::new([Frontier::new(block::Height(1), block::Hash([0xaa; 32]))])
            .expect("the checkpoint fixture is unique"),
    );
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
        owner: HeaderWorkOwner {
            authority: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId::new(base.metadata.work_origin.hash, child_hash),
            },
            session_id: 1,
            request_id: NonZeroU64::new(1).expect("one is nonzero"),
        }
        .into(),
        body_size: BodySizeHint::Unknown,
        tree_aux: None,
        authentication: AuxAuthentication::Unauthenticated,
    });
    assert!(violations(&store, &config)
        .iter()
        .any(|violation| matches!(violation, AuditViolation::Auxiliary(_))));
}
