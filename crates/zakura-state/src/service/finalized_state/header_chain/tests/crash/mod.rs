use super::*;

mod auxiliary;
mod deferred;
mod finality;
mod verified_and_body;
mod writes;

use auxiliary::*;
use deferred::*;
use finality::*;
use verified_and_body::*;
use writes::*;

struct CrashObservation {
    durable: EngineSnapshot,
    reopened: HeaderChainRuntime,
    startup: StartupReport,
}

struct CrashFixture {
    name: &'static str,
    run: fn(),
}

#[test]
// AUD-14: every durable write boundary is interrupted, and reopening must
// expose either the complete before-state or complete after-state.
fn every_crash_boundary_reopens_to_complete_transition() {
    const FIXTURES: &[CrashFixture] = &[
        CrashFixture {
            name: "startup recovery",
            run:
                crash_fixture_startup_recovery_reopens_complete_before_or_after_without_publication,
        },
        CrashFixture {
            name: "ordinary state writer transition",
            run: crash_fixture_every_state_writer_crash_point_reopens_complete_before_or_after,
        },
        CrashFixture {
            name: "requester insertion",
            run: crash_fixture_requester_insertion_reopens_complete_before_or_after,
        },
        CrashFixture {
            name: "finality advance",
            run: crash_fixture_finality_advance_reopens_complete_before_or_after,
        },
        CrashFixture {
            name: "operator eligibility change",
            run: crash_fixture_operator_reason_changes_reopen_complete_before_or_after,
        },
        CrashFixture {
            name: "verified grow and reset",
            run: crash_fixture_verified_grow_and_reset_reopen_complete_before_or_after,
        },
        CrashFixture {
            name: "body retry",
            run: crash_fixture_body_retry_restarts_reopen_complete_before_or_after,
        },
        CrashFixture {
            name: "body conclusion",
            run: crash_fixture_body_conclusions_reopen_complete_before_or_after,
        },
        CrashFixture {
            name: "deferred header reevaluation",
            run: crash_fixture_deferred_header_reevaluation_reopens_complete_before_or_after,
        },
        CrashFixture {
            name: "selected auxiliary repair",
            run: crash_fixture_selected_auxiliary_repair_reopens_complete_before_or_after,
        },
        CrashFixture {
            name: "auxiliary authentication",
            run: crash_fixture_aux_authentication_reopens_complete_before_or_after,
        },
        CrashFixture {
            name: "two-delivery auxiliary rejection",
            run: crash_fixture_two_delivery_aux_rejection_never_partially_commits,
        },
        CrashFixture {
            name: "migrated pin refutation",
            run: crash_fixture_migrated_pin_refutation_fails_closed_at_every_reachable_boundary,
        },
        CrashFixture {
            name: "no-change transition",
            run: crash_fixture_no_change_crash_points_preserve_the_paired_full_state_transaction,
        },
    ];

    for fixture in FIXTURES {
        tracing::debug!(fixture = fixture.name, "running header-chain crash fixture");
        (fixture.run)();
    }
}

#[allow(clippy::too_many_arguments)]
fn observe_transition_crash(
    target: FaultPoint,
    runtime: HeaderChainRuntime,
    db: DiskDb,
    db_config: &Config,
    network: &Network,
    engine_config: &EngineConfig,
    before: &EngineSnapshot,
    memory_swapped: &AtomicBool,
    marker_key: Option<[u8; 4]>,
) -> CrashObservation {
    observe_transition_crash_with_allowed_startup_repairs(
        target,
        runtime,
        db,
        db_config,
        network,
        engine_config,
        before,
        memory_swapped,
        marker_key,
        &BTreeSet::new(),
    )
}

#[allow(clippy::too_many_arguments)]
fn observe_transition_crash_with_allowed_startup_repairs(
    target: FaultPoint,
    runtime: HeaderChainRuntime,
    db: DiskDb,
    db_config: &Config,
    network: &Network,
    engine_config: &EngineConfig,
    before: &EngineSnapshot,
    memory_swapped: &AtomicBool,
    marker_key: Option<[u8; 4]>,
    allowed_startup_repairs: &BTreeSet<RecoveryRepair>,
) -> CrashObservation {
    let durable_before_startup = runtime
        .store
        .snapshot()
        .expect("the post-crash durable snapshot is readable");
    if let Some(marker_key) = marker_key {
        let marker_cf = runtime
            .store
            .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
            .expect("the paired full-state marker column family is open");
        assert_eq!(
            runtime
                .store
                .db
                .raw_get_cf(&marker_cf, &marker_key)
                .expect("the paired full-state marker read succeeds")
                .is_some(),
            target.commit_completed(),
            "{target:?}"
        );
    }
    assert_eq!(
        memory_swapped.load(Ordering::SeqCst),
        target.memory_swap_completed(),
        "{target:?}"
    );
    assert_eq!(
        runtime.publisher().snapshot(),
        if target.publication_completed() {
            durable_before_startup.clone()
        } else {
            before.clone()
        },
        "{target:?}"
    );
    let engine_snapshot = runtime
        .transition_engine
        .lock()
        .expect("the transition engine mutex is not poisoned")
        .snapshot();
    assert_eq!(
        engine_snapshot,
        if target.commit_completed() {
            durable_before_startup.clone()
        } else {
            before.clone()
        },
        "{target:?}"
    );
    drop(runtime);
    drop(db);

    let (reopened, startup) = HeaderChainStore::new(open(db_config, network))
        .startup(engine_config)
        .expect("the crash boundary reopens to one coherent transaction");
    assert_eq!(startup.previous, durable_before_startup, "{target:?}");
    assert!(
        startup.repairs.is_empty() || startup.repairs == *allowed_startup_repairs,
        "unexpected startup repairs at {target:?}: {:?}",
        startup.repairs,
    );
    let durable = startup.current.clone();
    assert_eq!(reopened.publisher().snapshot(), durable, "{target:?}");
    assert_eq!(
        reopened
            .store
            .snapshot()
            .expect("the reopened durable snapshot is readable"),
        durable,
        "{target:?}"
    );
    assert_transition_engine_matches_store(&reopened);

    CrashObservation {
        durable,
        reopened,
        startup,
    }
}
