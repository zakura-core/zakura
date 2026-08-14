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
use super::{fixture, injected_store_error, violations, AuditRead};
use crate::{
    AuxDelivery, BodyRuleId, BodySizeHint, BodyValidationState, BranchId, ChainScore,
    CheckpointSet, ConsensusInvalidBodyTombstone, EligibilityReason, EligibilityState, EngineMode,
    EvidenceId, FinalityEpoch, FinalityRecord, FinalitySource, Frontier, HeaderGeneration,
    HeaderNode, HeaderValidationState, HeaderWorkAuthority, HeaderWorkOwner, SourceId, StoreError,
    SuffixWork, WorkCoordinate,
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
    let anchor = elapsed.metadata.frontiers.finalized;
    let until = elapsed.nodes[1].header.time - Duration::hours(2);
    elapsed.nodes[1].validation = crate::HeaderValidationState::DeferredUntil(until);
    elapsed.metadata.frontiers.header_best = anchor;
    elapsed.metadata.header_best_score = ChainScore::new(SuffixWork::zero(), anchor.hash);
    elapsed.snapshot = elapsed.metadata.snapshot();
    elapsed.selected = vec![anchor];
    elapsed.deferred = vec![(until, child_hash)];
    let plan = audit_store_at(&elapsed, &config, elapsed.nodes[1].header.time)
        .expect("an elapsed deferral remains authoritative until normal reevaluation");
    assert!(plan.is_clean());
    assert_eq!(plan.metadata, elapsed.metadata);
    assert_eq!(plan.deferred_entries, elapsed.deferred);
    assert_eq!(
        plan.header_nodes
            .iter()
            .find(|node| node.hash == child_hash)
            .expect("the child remains retained")
            .validation,
        crate::HeaderValidationState::DeferredUntil(until)
    );
}

#[test]
fn headers_only_recovery_rejects_an_unsettled_selected_suffix() {
    let (mut store, mut config) = fixture();
    let anchor = store.metadata.frontiers.finalized;
    let child = store.metadata.frontiers.header_best;
    let child_node = store.nodes[1].clone();
    let mut grandchild_header = *child_node.header;
    grandchild_header.previous_block_hash = child.hash;
    grandchild_header.time += Duration::seconds(1);
    grandchild_header.nonce = [2; 32].into();
    let grandchild_header = Arc::new(grandchild_header);
    let grandchild_hash = grandchild_header.hash();
    let grandchild_work = grandchild_header
        .difficulty_threshold
        .to_work()
        .expect("the fixture grandchild target has work");
    let grandchild = Frontier::new(block::Height(2), grandchild_hash);
    let grandchild_node = HeaderNode::from_durable_parts(
        grandchild_header,
        grandchild_hash,
        child.hash,
        grandchild.height,
        grandchild_work,
        child_node
            .work_coordinate()
            .checked_add(grandchild_work)
            .expect("the fixture grandchild work fits"),
        HeaderValidationState::Valid,
        EligibilityState::default(),
        BodyValidationState::Unknown,
        Vec::new(),
    )
    .expect("the fixture grandchild fields agree");

    config.mode = EngineMode::HeadersOnly;
    config.limits.local_finality_depth = std::num::NonZeroU32::new(1).expect("one is nonzero");
    store.metadata.mode = EngineMode::HeadersOnly;
    store.metadata.frontiers.header_best = grandchild;
    store.metadata.header_best_score = ChainScore::new(
        SuffixWork::new(
            child_node
                .block_work
                .as_u256()
                .checked_add(grandchild_work.as_u256())
                .expect("the two-block suffix work fits"),
        ),
        grandchild.hash,
    );
    store.snapshot = store.metadata.snapshot();
    store.nodes.push(grandchild_node);
    store.children.push((child.hash, grandchild.hash));
    store.selected = vec![anchor, child, grandchild];
    store.finality[0].source = FinalitySource::MigratedHeadersOnly;

    assert_eq!(
        audit_store(&store, &config),
        Err(RecoveryFailure::Source {
            violations: vec![AuditViolation::Finality],
        })
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
fn rejects_missing_duplicate_orphan_and_mismatched_tombstones() {
    let (base, config) = fixture();
    let child_hash = base.nodes[1].hash;
    let evidence = EvidenceId::from_digest([0x21; 32]);
    let rule = BodyRuleId::new("body.rule");
    let invalid = BodyValidationState::ConsensusInvalid {
        evidence,
        rule: rule.clone(),
    };
    let matching = ConsensusInvalidBodyTombstone {
        hash: child_hash,
        evidence,
        rule,
    };
    let cases = [
        ("missing", invalid.clone(), Vec::new()),
        (
            "duplicate",
            invalid.clone(),
            vec![matching.clone(), matching.clone()],
        ),
        (
            "orphan",
            BodyValidationState::Unknown,
            vec![matching.clone()],
        ),
        (
            "mismatched",
            invalid,
            vec![ConsensusInvalidBodyTombstone {
                evidence: EvidenceId::from_digest([0x22; 32]),
                ..matching
            }],
        ),
    ];

    for (name, body_state, tombstones) in cases {
        let mut store = base.clone();
        store.nodes[1].body_validation_state = body_state;
        store.tombstones = tombstones;
        assert_eq!(
            violations(&store, &config),
            vec![AuditViolation::ConsensusInvalidBodyTombstone(child_hash)],
            "{name}"
        );
    }
}

#[test]
fn denies_body_states_without_full_state_authority() {
    let (base, config) = fixture();
    let child_hash = base.nodes[1].hash;
    let evidence = EvidenceId::from_digest([0x31; 32]);
    let rule = BodyRuleId::new("body.rule");
    let cases = [
        (
            "verified",
            BodyValidationState::Verified { evidence },
            Vec::new(),
        ),
        (
            "consensus invalid",
            BodyValidationState::ConsensusInvalid {
                evidence,
                rule: rule.clone(),
            },
            vec![ConsensusInvalidBodyTombstone {
                hash: child_hash,
                evidence,
                rule,
            }],
        ),
    ];

    for (name, body_state, tombstones) in cases {
        let mut store = base.clone();
        store.nodes[1].body_validation_state = body_state;
        store.tombstones = tombstones;
        store.body_state_authority = false;
        assert_eq!(
            violations(&store, &config),
            vec![AuditViolation::BodyValidationEvidenceAuthority(child_hash)],
            "{name}"
        );
    }
}

#[test]
fn propagates_every_store_audit_read_error() {
    for failed_read in AuditRead::ALL {
        let (mut store, mut config) = fixture();
        match failed_read {
            AuditRead::BodyStateAuthority => {
                store.nodes[1].body_validation_state = BodyValidationState::Verified {
                    evidence: EvidenceId::from_digest([0x41; 32]),
                };
            }
            AuditRead::CanonicalHash => {
                let anchor = store.metadata.frontiers.finalized;
                config.replace_local_checkpoints(
                    CheckpointSet::new([anchor]).expect("the anchor checkpoint is unique"),
                );
                store.metadata.anchor_manifest_digest = config.trust_anchor_digest();
                store.snapshot = store.metadata.snapshot();
            }
            _ => {}
        }
        store.failed_read = Some(failed_read);

        assert_eq!(
            audit_store(&store, &config),
            Err(RecoveryFailure::Store(injected_store_error())),
            "{failed_read:?}"
        );
    }
}

#[test]
fn violations_are_sorted_and_deduplicated() {
    let (mut store, config) = fixture();
    let child_hash = store.nodes[1].hash;
    store.nodes[1].block_work = zakura_chain::work::difficulty::Work::zero();
    store.nodes.push(store.nodes[1].clone());
    store.tombstones.push(ConsensusInvalidBodyTombstone {
        hash: child_hash,
        evidence: EvidenceId::from_digest([0x51; 32]),
        rule: BodyRuleId::new("body.rule"),
    });
    store.body_state_authority = false;

    assert_eq!(
        violations(&store, &config),
        vec![
            AuditViolation::NodeHash(child_hash),
            AuditViolation::Work(child_hash),
            AuditViolation::ConsensusInvalidBodyTombstone(child_hash),
            AuditViolation::BodyValidationEvidenceAuthority(child_hash),
        ]
    );
}

#[test]
fn rejects_invalid_verified_paths_in_both_modes() {
    for mode in [EngineMode::HeadersOnly, EngineMode::Integrated] {
        let (mut store, mut config) = fixture();
        let child = store.metadata.frontiers.header_best;
        config.mode = mode;
        store.metadata.mode = mode;
        store.metadata.frontiers.verified_best = child;
        store.snapshot = store.metadata.snapshot();
        if mode == EngineMode::HeadersOnly {
            store.finality[0].source = FinalitySource::MigratedHeadersOnly;
        }

        assert_eq!(
            audit_store(&store, &config),
            Err(RecoveryFailure::Source {
                violations: vec![AuditViolation::ProtectedPath(child.hash)],
            }),
            "{mode:?}"
        );
    }
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
fn migrated_finality_requires_an_explicit_integrated_migration_boundary() {
    let (mut store, config) = fixture();
    store.finality[0].source = FinalitySource::MigratedHeadersOnly;

    assert!(violations(&store, &config).contains(&AuditViolation::Finality));

    store.metadata.headers_only_migration_epoch = Some(FinalityEpoch::new(0));
    store.snapshot = store.metadata.snapshot();
    audit_store(&store, &config).expect("the explicit migration boundary authenticates the prefix");

    store.metadata.headers_only_migration_epoch = Some(FinalityEpoch::new(1));
    store.snapshot = store.metadata.snapshot();
    assert!(violations(&store, &config).contains(&AuditViolation::Finality));
}

#[test]
fn migrated_finality_is_rejected_after_the_migration_boundary() {
    let (mut store, config) = fixture();
    let anchor = store.metadata.frontiers.finalized;
    let child = store.metadata.frontiers.header_best;
    let anchor_header = store.nodes[0].header.clone();

    store.finality[0].source = FinalitySource::MigratedHeadersOnly;
    store.finality.push(FinalityRecord {
        previous: anchor,
        current: child,
        source: FinalitySource::FullState {
            evidence: EvidenceId::from_digest([0x81; 32]),
        },
        epoch: FinalityEpoch::new(1),
    });
    store.metadata.headers_only_migration_epoch = Some(FinalityEpoch::new(0));
    store.metadata.finality_epoch = FinalityEpoch::new(1);
    store.metadata.frontiers.finalized = child;
    store.metadata.frontiers.verified_best = child;
    store.metadata.header_best_score = ChainScore::new(SuffixWork::zero(), child.hash);
    store.metadata.oldest_retained_height = child.height;
    store.snapshot = store.metadata.snapshot();
    store.nodes[1].body_validation_state = BodyValidationState::Verified {
        evidence: EvidenceId::from_digest([0x82; 32]),
    };
    store.nodes.remove(0);
    store.children.clear();
    store.selected = vec![child];
    store.verified = vec![child];
    store.contexts = vec![ValidationContextRecord {
        header: anchor_header,
        height: anchor.height,
    }];

    audit_store(&store, &config).expect("full-state provenance after the boundary audits cleanly");

    store.finality[1].source = FinalitySource::MigratedHeadersOnly;
    assert!(violations(&store, &config).contains(&AuditViolation::Finality));
}

#[test]
fn resource_stall_alarm_does_not_exempt_startup_node_limit() {
    let (mut store, mut config) = fixture();
    config.limits.max_non_finalized_nodes =
        NonZeroUsize::new(1).expect("one is a valid node limit");
    let anchor = store.metadata.frontiers.finalized;
    let mut sibling_header = *store.nodes[1].header;
    sibling_header.nonce = [2; 32].into();
    let sibling_header = Arc::new(sibling_header);
    let sibling_hash = sibling_header.hash();
    let sibling_work = sibling_header
        .difficulty_threshold
        .to_work()
        .expect("the fixture sibling target has work");
    store.nodes.push(
        HeaderNode::from_durable_parts(
            sibling_header,
            sibling_hash,
            anchor.hash,
            block::Height(1),
            sibling_work,
            store.nodes[0]
                .work_coordinate()
                .checked_add(sibling_work)
                .expect("the sibling work fits"),
            HeaderValidationState::Valid,
            EligibilityState::default(),
            BodyValidationState::Unknown,
            Vec::new(),
        )
        .expect("the canonical sibling fields agree"),
    );
    store.metadata.alarms.resource_stalled = true;
    store.snapshot = store.metadata.snapshot();

    assert_eq!(
        audit_store(&store, &config),
        Err(RecoveryFailure::Store(StoreError::LimitExceeded {
            collection: "header nodes",
            limit: 2,
        }))
    );
}

#[test]
fn oversized_node_table_fails_before_node_rows_are_loaded() {
    let (mut store, mut config) = fixture();
    config.limits.max_non_finalized_nodes =
        NonZeroUsize::new(1).expect("one is a valid node limit");
    store.nodes.push(store.nodes[1].clone());
    store.failed_read = Some(AuditRead::HeaderNodes);

    assert_eq!(
        audit_store(&store, &config),
        Err(RecoveryFailure::Store(StoreError::LimitExceeded {
            collection: "header nodes",
            limit: 2,
        }))
    );
}

#[test]
fn oversized_auxiliary_and_context_tables_fail_before_rows_are_loaded() {
    let (mut store, mut config) = fixture();
    config.limits.max_aux_deliveries_total =
        NonZeroUsize::new(1).expect("one is a valid auxiliary limit");
    let delivery = AuxDelivery::new(
        EvidenceId::from_digest([0x51; 32]),
        store.nodes[1].hash,
        SourceId::from_digest([0x52; 32]),
        HeaderWorkOwner {
            authority: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId::new(store.metadata.work_origin.hash, store.nodes[1].hash),
            },
            session_id: 1,
            request_id: NonZeroU64::new(1).expect("one is nonzero"),
        }
        .into(),
        BodySizeHint::Unknown,
        None,
    );
    store.aux = vec![delivery, delivery];
    store.failed_read = Some(AuditRead::AuxDeliveries);

    assert_eq!(
        audit_store(&store, &config),
        Err(RecoveryFailure::Store(StoreError::LimitExceeded {
            collection: "auxiliary deliveries",
            limit: 1,
        }))
    );

    let (mut store, config) = fixture();
    store.contexts = vec![
        ValidationContextRecord {
            header: store.nodes[0].header.clone(),
            height: block::Height(0),
        };
        crate::POW_PREDECESSOR_CONTEXT_SPAN + 1
    ];
    store.failed_read = Some(AuditRead::ValidationContexts);

    assert_eq!(
        audit_store(&store, &config),
        Err(RecoveryFailure::Store(StoreError::LimitExceeded {
            collection: "validation contexts",
            limit: crate::POW_PREDECESSOR_CONTEXT_SPAN,
        }))
    );
}

#[test]
fn fatal_configuration_mismatch_fails_before_collection_preflight() {
    let (mut store, config) = fixture();
    store.metadata.mode = EngineMode::HeadersOnly;
    store.snapshot = store.metadata.snapshot();
    store.failed_read = Some(AuditRead::HeaderNodeCount);

    assert_eq!(
        audit_store(&store, &config),
        Err(RecoveryFailure::Source {
            violations: vec![AuditViolation::Configuration],
        })
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
    let evidence = EvidenceId::from_digest([2; 32]);
    let rule = BodyRuleId::new("body.rule");
    store.nodes[1].body_validation_state = BodyValidationState::ConsensusInvalid {
        evidence,
        rule: rule.clone(),
    };
    store.tombstones.push(ConsensusInvalidBodyTombstone {
        hash: child_hash,
        evidence,
        rule,
    });
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
    assert_eq!(
        audit_store(&oversized, &limited),
        Err(RecoveryFailure::Store(StoreError::LimitExceeded {
            collection: "header nodes",
            limit: 2,
        }))
    );

    let mut store = base.clone();
    let evidence = EvidenceId::from_digest([5; 32]);
    store.aux.push(AuxDelivery::new(
        evidence,
        block::Hash([6; 32]),
        SourceId::from_digest([7; 32]),
        HeaderWorkOwner {
            authority: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId::new(base.metadata.work_origin.hash, child_hash),
            },
            session_id: 1,
            request_id: NonZeroU64::new(1).expect("one is nonzero"),
        }
        .into(),
        BodySizeHint::Unknown,
        None,
    ));
    assert!(violations(&store, &config)
        .iter()
        .any(|violation| matches!(violation, AuditViolation::Auxiliary(_))));
}
