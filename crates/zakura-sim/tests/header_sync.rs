//! The `header-sync/smoke` harness: the reactor on the step loop, with inert
//! storage and no peers. It runs on a `Plan`, never panics, and replays.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_sim::{
    harness::{header_sync::Smoke, run_harness},
    observe::Exit,
    MemorySink, Observation, Outcome, PlanInput, RunReport,
};

/// Runs the harness under `plan`. Returns the report and the records.
fn run(plan: PlanInput) -> (RunReport, Vec<String>) {
    let mut sink = MemorySink::default();
    let report = run_harness::<Smoke>(&Default::default(), plan, &mut sink, None)
        .unwrap_or_else(|error| panic!("{error}"));
    (report, sink.lines())
}

#[test]
fn header_sync_runs_and_replays() {
    let mut runs = std::collections::BTreeSet::new();
    for plan in inputs(8, 64) {
        let (report, first) = run(plan.clone());
        assert_eq!(report.cleanup.panicked, 0, "no task panicked in cleanup");
        let picks: Vec<String> = report.picks.iter().map(|p| p.identity.clone()).collect();
        runs.insert(picks);
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{:?}",
            report.end
        );
        let panicked = report.log.entries().iter().any(|e| {
            matches!(
                e.obs,
                Observation::TaskExited {
                    exit: Exit::Panicked,
                    ..
                }
            )
        });
        assert!(!panicked);
        assert!(
            report
                .picks
                .iter()
                .any(|p| p.identity.starts_with("task:header_sync_reactor.1:")),
            "the reactor ran under its spawn name"
        );
        assert_eq!(run(plan).1, first, "the same plan replays the same records");
    }
    // Otherwise replaying would pass even if the plan were ignored.
    assert!(runs.len() > 1, "different plans took different paths");
}
