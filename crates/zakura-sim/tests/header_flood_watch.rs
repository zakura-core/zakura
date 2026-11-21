//! hdr-7c: the lifecycle watch property on `header-sync/flood`. A node-side
//! commit publishes a new committed state while 16 mirrors churn the
//! reactor's lifecycle queue, and the run loop must read that state from
//! its committed watch before it takes a third lifecycle event.
#![cfg(zakura_sim)]

mod support;

use std::{io::Write, path::Path, process::Command, sync::OnceLock};

use support::header_inputs;
use tempfile::TempDir;
use zakura_sim::{
    domain::header_sync::WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_sync::{Flood, FloodScenario},
        record, run_prepared, RecordInput, Subsystem,
    },
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    MemorySink, NullSink, Outcome, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// How many Plans [`seed_input`]'s search draws.
const SEARCH_PLANS: usize = 16;

/// The longest Plan [`seed_input`]'s search draws.
const SEARCH_PLAN_BYTES: usize = 256;

/// [`FloodScenario::Lifecycle`]'s scenario byte, then the first of
/// [`SEARCH_PLANS`] Plans whose run reaches the lifecycle watch property's
/// trigger history and holds it: the lifecycle queue delivers an event after
/// its commit and before the watch read. On that order, reverting #1566's
/// top-of-turn watch check breaks the rule. Not every Plan's queue competes
/// with the watch read, so the search skips the ones that don't. The process
/// keeps the found input.
fn seed_input() -> Vec<u8> {
    static FOUND: OnceLock<Vec<u8>> = OnceLock::new();
    FOUND
        .get_or_init(|| {
            let entry = harness::find(Flood::NAME).expect("the harness is registered");
            header_inputs::first(
                "header-flood-lifecycle",
                support::byte_inputs(SEARCH_PLANS, SEARCH_PLAN_BYTES)
                    .into_iter()
                    .map(|plan| [&[1][..], &plan].concat()),
                |input| header_inputs::holds(entry, input, &[WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD]),
            )
        })
        .clone()
}

/// `input`'s Plan, after its scenario byte.
fn plan(input: &[u8]) -> PlanInput {
    Plan::from_fuzz(&input[Flood::SCENARIO_BYTES..])
}

/// `scenario`'s run under `input`, and the lifecycle watch property's
/// outcome.
fn run(scenario: FloodScenario, input: PlanInput) -> (RunReport, PropertyOutcome) {
    let ports = Flood::prepare(&scenario);
    let report = run_prepared::<Flood>(&scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD)
        .map(|property| property.outcome.clone())
        .expect("the harness checks the lifecycle watch property");
    (report, outcome)
}

/// The searched input holds its property through the registered harness and reaches
/// its trigger history: the lifecycle queue delivered an event after the
/// commit and before the watch read.
#[test]
fn the_flood_seed_holds_its_watch_rule() {
    let entry = harness::find(Flood::NAME).expect("the harness is registered");
    let input = seed_input();
    let result = (entry.witness)(
        &entry.scenario(&input[..Flood::SCENARIO_BYTES]),
        plan(&input),
        WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
        Expect::Holds,
        &[],
        &mut MemorySink::default(),
    );
    assert_eq!(result, Ok(()));
}

/// The lifecycle watch property holds under every plan in both fixtures.
/// Not every plan's queue competes with the commit's watch read, which is
/// why the witness searches for its Plan.
#[test]
fn the_flood_fixtures_hold_the_watch_rule_under_every_plan() {
    for scenario in [FloodScenario::Wire, FloodScenario::Lifecycle] {
        for input in support::inputs(8, 256) {
            let (report, outcome) = run(scenario, input);
            assert_eq!(report.end.violation, None, "{scenario:?}: {:?}", report.end);
            assert!(
                matches!(outcome, PropertyOutcome::Held { .. }),
                "{scenario:?}: {outcome:?}"
            );
        }
    }
}

/// The scenario byte picks the fixture by bit 0 alone.
#[test]
fn the_scenario_byte_picks_the_flood_fixture() {
    for byte in 0..=u8::MAX {
        let expected = if byte & 1 == 1 {
            FloodScenario::Lifecycle
        } else {
            FloodScenario::Wire
        };
        assert_eq!(Flood::draw(&[byte]), expected, "{byte}");
    }
}

/// The run's log and the watch outcome depend only on the plan: the searched
/// input's plan A twice, then B, then A again, gives A's every time, and B's log
/// differs.
#[test]
fn flood_runs_repeat_a_a_and_a_b_a() {
    let lifecycle = seed_input();
    let plans: [&[u8]; 2] = [&lifecycle[Flood::SCENARIO_BYTES..], &[7; 256]];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, outcome) = run(FloodScenario::Lifecycle, Plan::from_fuzz(plan));
            (
                format!("{:?}", report.log.entries()),
                format!("{outcome:?}"),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to a directory holding the searched input's recorded trace, in the child
/// process that [`the_flood_witness_replays_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_FLOOD_TRACE";

/// The searched input, recorded in this process, replays exactly in a fresh process
/// of the same test binary, where its run gives the same watch outcome.
#[test]
fn the_flood_witness_replays_in_a_fresh_process() {
    let entry = harness::find(Flood::NAME).expect("the harness is registered");
    let input = seed_input();
    if let Some(dir) = std::env::var_os(CHILD) {
        let trace = TraceReader::open(Path::new(&dir)).expect("the trace was recorded");
        // A replay that leaves the trace's picks, draws or observations, or
        // fails where the recording didn't, errs.
        replay_with(&trace, entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        let (_, outcome) = run(FloodScenario::Lifecycle, plan(&input));
        writeln!(std::io::stdout(), "FLOOD-BEGIN\n{outcome:?}\nFLOOD-END")
            .expect("the test harness captures stdout");
        return;
    }
    let dir = TempDir::new().expect("a temporary directory");
    record(entry, RecordInput::Input(&input), dir.path()).expect("the witness records");
    let (_, outcome) = run(FloodScenario::Lifecycle, plan(&input));
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_flood_witness_replays_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("FLOOD-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("\nFLOOD-END"))
        .map(|(outcome, _)| outcome)
        .unwrap_or_else(|| panic!("the child prints its outcome: {stdout}"));
    assert_eq!(replayed, format!("{outcome:?}"));
}

/// The searched input draws the lifecycle fixture, and runs through the registered
/// harness to a defined end that the explorer classifies clean.
#[test]
fn the_flood_seed_draws_its_fixture_and_runs_clean() {
    let entry = harness::find(Flood::NAME).expect("the harness is registered");
    let input = seed_input();
    assert_eq!(Flood::draw(&input), FloodScenario::Lifecycle);
    let run = (entry.run_bytes)(&input, &mut NullSink, None);
    let report = run.report().unwrap_or_else(|| panic!("{:?}", run.result));
    assert!(
        matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
        "{:?}",
        report.end
    );
    assert_eq!(run.leaked(), 0);
    assert_eq!(classify(entry, &run).outcome, ExploreOutcome::Clean);
}
