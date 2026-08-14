//! Reconstructible repair and trust-anchor rebind coverage.

use std::collections::BTreeSet;

use chrono::Utc;
use zakura_chain::block;

use super::super::{
    audit_store, audit_store_for_trust_anchor_update, AuditViolation, RecoveryFailure,
    RecoveryRepair,
};
use super::fixture;
use crate::{
    BodyUnavailableSummary, BodyValidationState, CheckpointSet, EngineMode, Frontier,
    HeaderGeneration, StateVersion, VerifiedGeneration,
};

#[test]
fn coherent_source_and_indexes_need_no_recovery_write() {
    let (store, config) = fixture();
    let plan = audit_store(&store, &config).expect("the coherent fixture audits cleanly");
    assert!(plan.is_clean());
    assert_eq!(plan.metadata, store.metadata);
}

#[test]
fn trust_anchor_update_rebinds_only_after_current_pins_audit() {
    let (mut store, mut config) = fixture();
    let previous_state_version = store.metadata.state_version;
    config.replace_local_checkpoints(
        CheckpointSet::new([Frontier::new(block::Height(10), block::Hash([0x91; 32]))])
            .expect("the extension checkpoint is unique"),
    );

    assert!(matches!(
        audit_store(&store, &config),
        Err(RecoveryFailure::Source { violations })
            if violations == vec![AuditViolation::Configuration]
    ));
    let plan = audit_store_for_trust_anchor_update(&store, &config)
        .expect("a future checkpoint extension can rebind an otherwise coherent store");
    assert_eq!(
        plan.repairs,
        BTreeSet::from([RecoveryRepair::TrustAnchorConfiguration])
    );
    assert_eq!(
        plan.metadata.state_version,
        previous_state_version
            .checked_next()
            .expect("the fixture state version can advance")
    );
    assert_eq!(
        plan.metadata.anchor_manifest_digest,
        config.trust_anchor_digest()
    );
    assert_eq!(
        plan.metadata.header_generation,
        store.metadata.header_generation
    );
    assert_eq!(
        plan.metadata.verified_generation,
        store.metadata.verified_generation
    );

    store.metadata = plan.metadata;
    store.snapshot = store.metadata.snapshot();
    assert!(audit_store(&store, &config)
        .expect("the rebound store passes the strict audit")
        .is_clean());

    let mut wrong_mode = config.clone();
    wrong_mode.mode = EngineMode::HeadersOnly;
    assert!(matches!(
        audit_store_for_trust_anchor_update(&store, &wrong_mode),
        Err(RecoveryFailure::Source { violations })
            if violations.contains(&AuditViolation::Configuration)
    ));

    let mut conflicting = config;
    conflicting.replace_local_checkpoints(
        CheckpointSet::new([Frontier::new(
            store.nodes[1].height,
            block::Hash([0x92; 32]),
        )])
        .expect("the conflicting checkpoint is unique"),
    );
    assert!(matches!(
        audit_store_for_trust_anchor_update(&store, &conflicting),
        Err(RecoveryFailure::Source { violations })
            if violations.iter().any(|violation| matches!(violation, AuditViolation::TrustPin(_, _)))
    ));
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
    store.nodes[1].body_validation_state = BodyValidationState::Unavailable(summary);

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
fn repairs_retention_metadata_without_advancing_projection_generations() {
    let (mut store, config) = fixture();
    let retained_height = store.metadata.frontiers.finalized.height;
    store.metadata.oldest_retained_height = store.metadata.frontiers.header_best.height;
    store.snapshot = store.metadata.snapshot();

    let plan = audit_store(&store, &config).expect("retention metadata is reconstructible");

    assert_eq!(
        plan.repairs,
        BTreeSet::from([RecoveryRepair::RetentionMetadata])
    );
    assert_eq!(plan.metadata.oldest_retained_height, retained_height);
    assert_eq!(plan.metadata.state_version, StateVersion::new(2));
    assert_eq!(plan.metadata.header_generation, HeaderGeneration::new(1));
    assert_eq!(
        plan.metadata.verified_generation,
        VerifiedGeneration::new(1)
    );
}

#[test]
fn fails_closed_when_each_repair_counter_is_exhausted() {
    enum Counter {
        State,
        Header,
        Verified,
    }

    let cases = [
        (Counter::State, "state version"),
        (Counter::Header, "header generation"),
        (Counter::Verified, "verified generation"),
    ];
    for (counter, label) in cases {
        let (mut store, config) = fixture();
        match counter {
            Counter::State => {
                store.metadata.state_version = StateVersion::new(u64::MAX);
                store.metadata.oldest_retained_height = store.metadata.frontiers.header_best.height;
            }
            Counter::Header => {
                store.metadata.header_generation = HeaderGeneration::new(u64::MAX);
                store.selected.clear();
            }
            Counter::Verified => {
                store.metadata.verified_generation = VerifiedGeneration::new(u64::MAX);
                store.verified.clear();
            }
        }
        store.snapshot = store.metadata.snapshot();

        let error = match audit_store(&store, &config) {
            Err(RecoveryFailure::Counter(error)) => error,
            other => panic!("expected {label} exhaustion, got {other:?}"),
        };
        assert_eq!(
            error.to_string(),
            format!("header-chain {label} counter is exhausted at u64::MAX")
        );
    }
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
