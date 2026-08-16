//! Snapshot and configuration coherence characterization.

use super::super::admission::validate_snapshot;
use super::*;
use zakura_chain::parameters::NetworkKind;

#[test]
fn hydration_rejects_metadata_work_origin_that_disagrees_with_graph() {
    let (mut store, _config) = TestStore::new(EngineMode::Integrated);
    store.metadata.work_origin =
        Frontier::new(store.metadata.work_origin.height, block::Hash([0x42; 32]));

    let error = crate::HeaderChainEngine::from_audited_state(
        store.graph,
        store.metadata,
        store.selected,
        store.verified,
        store.aux,
    )
    .expect_err("metadata work origin must agree with every graph node");

    assert_eq!(
        error,
        crate::EngineHydrationError::Incoherent("graph work origin disagrees with metadata")
    );
}

#[test]
fn validate_snapshot_rejects_configuration_and_metadata_mismatches() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let ctx = context(&config, &clock, None);
    let snapshot = store.snapshot();
    let metadata = store.metadata.clone();

    let cases = [
        ("snapshot mode", {
            let mut snapshot = snapshot.clone();
            snapshot.mode = EngineMode::HeadersOnly;
            (snapshot, metadata.clone())
        }),
        ("metadata mode", {
            let mut metadata = metadata.clone();
            metadata.mode = EngineMode::HeadersOnly;
            (snapshot.clone(), metadata)
        }),
        ("network identity", {
            let mut metadata = metadata.clone();
            metadata.network_id = NetworkKind::Mainnet;
            (snapshot.clone(), metadata)
        }),
        ("trust-anchor digest", {
            let mut metadata = metadata.clone();
            metadata.anchor_manifest_digest = [0xab; 32];
            (snapshot.clone(), metadata)
        }),
        ("state version", {
            let mut snapshot = snapshot.clone();
            snapshot.state_version = StateVersion::new(9);
            (snapshot, metadata.clone())
        }),
        ("frontiers", {
            let mut snapshot = snapshot.clone();
            snapshot.frontiers.header_best = Frontier::new(block::Height(1), block::Hash([1; 32]));
            (snapshot, metadata.clone())
        }),
    ];

    for (label, (snapshot, metadata)) in cases {
        assert_eq!(
            validate_snapshot(&snapshot, &metadata, &ctx),
            Err(TransitionFailure::ConfigurationMismatch),
            "{label} must fail closed"
        );
    }

    validate_snapshot(&snapshot, &metadata, &ctx)
        .expect("the coherent fixture snapshot must be accepted");
}

#[test]
fn validate_snapshot_does_not_require_startup_audit_fields() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let ctx = context(&config, &clock, None);
    let mut snapshot = store.snapshot();
    let metadata = store.metadata.clone();

    // These fields are compared by startup audit, not by the planner helper.
    snapshot.header_generation = HeaderGeneration::new(77);
    snapshot.verified_generation = VerifiedGeneration::new(88);
    snapshot.header_best_score.tip_hash = block::Hash([0x42; 32]);
    snapshot.oldest_retained_height = block::Height(99);
    snapshot.alarms.resource_stalled = true;

    validate_snapshot(&snapshot, &metadata, &ctx).expect(
        "planner snapshot checks intentionally leave generation, score, retention height, and alarms to startup audit",
    );
}
