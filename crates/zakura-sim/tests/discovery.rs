//! S4.7: discovery's `wait_complete` wakes once the initial exchange
//! completes. The scenario and its check are the `discovery/wait-complete`
//! harness, `zakura_sim::seed` builds its input, and `tests/witness.rs`
//! holds the regression. This file holds the seed sweep.
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::RefCell,
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use support::{inputs, recorded};
use zakura_network::zakura::{ZakuraDiscoveryHandle, ZakuraNodeRecord};
use zakura_quic::NodeId;
use zakura_sim::{
    domain::discovery::{wait_complete_wakes, WAIT_COMPLETE_WAKES as PROPERTY},
    harness::discovery::{run_config, spawn, Spawned, WaitComplete},
    property::{Assumptions, Registry, RegistryChecker, Source, Sources},
    seed, Outcome, Plan, PropertyOutcome, RunReport,
};

/// The registry's check of [`PROPERTY`] alone, for runs outside the
/// harness.
fn wakes_checker() -> RegistryChecker<WaitComplete> {
    RegistryChecker::new(
        Registry::new(vec![wait_complete_wakes()]).expect("one property"),
        Rc::new(WaitComplete),
        Assumptions::none(),
        &Sources::observed(&[Source::Schedule]),
    )
}

/// Asserts `report` ended cleanly, held the property at least once, and
/// never timed out the exchange.
fn assert_wakes(report: &RunReport) {
    assert_ne!(report.end.outcome, Outcome::Harness, "{:?}", report.end);
    assert_eq!(report.cleanup.leaked, 0, "shutdown leaked no task");
    assert_eq!(report.end.violation, None);
    let wakes = report
        .properties
        .iter()
        .find(|r| r.property == PROPERTY)
        .expect("the harness reports the property");
    assert!(
        matches!(wakes.outcome, PropertyOutcome::Held { applicable } if applicable > 0),
        "the run reached the point the property checks: {:?}",
        report.properties
    );
    assert!(
        !report
            .picks
            .iter()
            .any(|p| p.identity == "advance-time:2000000000"),
        "the exchange completed before its timeout"
    );
}

/// The book's record for `node`, read once the run has ended. Every task has
/// stopped by then, so the book's lock is free and the read finishes in one
/// poll.
fn book_record(handle: &ZakuraDiscoveryHandle, node: NodeId) -> Option<ZakuraNodeRecord> {
    let read = std::pin::pin!(handle.record_for(node));
    match read.poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(record) => record,
        Poll::Pending => panic!("the book's lock is free after the run"),
    }
}

/// The `discovery-wait-complete` seed holds the source back at its
/// `notified` until the sink has run all three marks, and the source still
/// wakes. The peer's Hello record is valid for the run's clock, so it enters
/// the book rather than being dropped as expired or too far ahead.
#[test]
fn the_peer_record_enters_the_book() {
    let input = seed::find("discovery-wait-complete")
        .expect("the seed is registered")
        .input;
    let spawned = RefCell::new(None);
    let (report, _) = recorded(
        run_config(),
        Plan::from_fuzz(&input),
        |sim| *spawned.borrow_mut() = Some(spawn(sim)),
        &mut wakes_checker(),
    );
    assert_wakes(&report);
    let Spawned { handle, peer } = spawned.into_inner().expect("the setup ran");
    assert!(
        book_record(&handle, peer).is_some(),
        "the peer's Hello record passed validation and entered the book"
    );
}

/// The sweep covers the worker counts that change how the source and the sink
/// interleave.
#[test]
fn seed_sweep_wakes() {
    for workers in [1, 2, 4] {
        for plan in inputs(200, 64) {
            let mut config = run_config();
            config.scheduler.workers = workers;
            let (report, _) = recorded(
                config,
                plan,
                |sim| {
                    spawn(sim);
                },
                &mut wakes_checker(),
            );
            assert_wakes(&report);
        }
    }
}

/// The slow holder saw a parked result, and the slots still bound it.
#[test]
fn verification_results_keep_their_slots() {
    use std::sync::{atomic::Ordering, Arc};
    use zakura_sim::{
        domain::discovery::VERIFICATION_CAPACITY,
        harness::{
            discovery::{
                CapacityEvidence, CapacityScenario, VerificationCapacity, VERIFICATION_READERS,
            },
            run_prepared,
        },
        witness::Expect,
    };
    let evidence = Arc::new(CapacityEvidence::default());
    let report = run_prepared::<VerificationCapacity>(
        &CapacityScenario::default(),
        Arc::clone(&evidence),
        zakura_sim::PlanInput::Bytes(Vec::new()),
        &mut zakura_sim::NullSink,
        None,
    )
    .expect("the run ends without a harness failure");
    zakura_sim::witness::judge_report::<VerificationCapacity>(
        &report,
        VERIFICATION_CAPACITY,
        Expect::Holds,
    )
    .unwrap_or_else(|error| panic!("{error}: {:?}", report.end));
    assert_eq!(
        evidence.done.load(Ordering::SeqCst),
        VERIFICATION_READERS,
        "every reader installs its record"
    );
    assert_eq!(
        evidence.peak_unsettled.load(Ordering::SeqCst),
        1,
        "the holder keeps the lock while a verified result waits to install"
    );
}
