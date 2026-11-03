//! A reactor tap's two run identities (M4-B3a), in a binary of its own so
//! its first `Sim` is the process's first, with registration ID 1.
#![cfg(zakura_sim)]

mod support;

use std::sync::Arc;

use support::config;
use zakura_network::zakura::observe::ReactorEvent;
use zakura_scheduler::{Scheduler, SimInstant};
use zakura_sim::{
    harness::header_node::tap::ReactorTap, observe::ReactorFact, HarnessFailure, IdleStatus,
    MemorySink, Observation, Outcome, PlanInput, Sim,
};
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

/// A tap's emission guard binds to the scheduler's runtime run and its
/// `ForeignRun` check to the registration run, which come from independent
/// counters. A standalone `Scheduler` made and dropped first takes runtime
/// ID 1, so the first `Sim` has registration ID 1 and runtime ID 2. Its
/// tap's events are still accepted and logged, and a tap from a dropped
/// run still fails a later one with `ForeignRun`.
#[test]
fn registration_and_runtime_run_ids_can_differ() {
    drop(Scheduler::new(config(1, 1).scheduler, SimInstant::ZERO));
    let mut sim = Sim::new(config(2, 100), PlanInput::Bytes(Vec::new()));
    assert_eq!(
        zakura_rt::sim::run_id().map(|run| run.get()),
        Some(2),
        "the runtime counter is ahead of the registration counter"
    );
    let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
    let (observer, tap) = ReactorTap::new(&mut sim, &port, "node");
    let emitter = observer.clone();
    sim.scheduler().spawn_root("reactor", async move {
        emitter.observe(ReactorEvent::Handled {
            call: 1,
            panicked: false,
        });
    });
    sim.add_source(Box::new(tap));
    let report = sim.run(&mut zakura_sim::NoChecks, &mut MemorySink::default());
    drop(observer);
    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "{:?}",
        report.end
    );
    assert!(
        report.log.entries().iter().any(|entry| matches!(
            entry.obs,
            Observation::Reactor {
                event: ReactorFact::Handled { call: 1, .. },
                ..
            }
        )),
        "the emission was logged"
    );

    let mut first = Sim::new(config(2, 100), PlanInput::Bytes(Vec::new()));
    let (_observer, tap) = ReactorTap::new(&mut first, &port, "node");
    drop(first);
    let mut second = Sim::new(config(2, 100), PlanInput::Bytes(Vec::new()));
    second.add_source(Box::new(tap));
    let report = second.run(&mut zakura_sim::NoChecks, &mut MemorySink::default());
    let Some(HarnessFailure::SourceEvidence { detail, .. }) = &report.end.harness else {
        panic!("expected a source evidence failure, got {:?}", report.end);
    };
    assert!(detail.starts_with("ForeignRun"), "{detail}");
}
