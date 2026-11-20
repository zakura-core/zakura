//! hdr-7c: the watch properties on `header-sync/flood`. A node-side commit
//! publishes a new committed state while 16 mirrors flood the reactor's
//! wire queue or churn its lifecycle queue, and the run loop must read
//! that state from its committed watch before it takes a third event from
//! the flooded queue.
#![cfg(zakura_sim)]

mod support;

use std::{io::Write, path::Path, process::Command, sync::OnceLock};

use support::header_inputs;
use tempfile::TempDir;
use zakura_sim::{
    domain::header_sync::{WATCH_OBSERVED_UNDER_FLOOD, WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD},
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

/// A searched input's name, its fixture and its property.
type Seed = (&'static str, FloodScenario, &'static str);

/// The searched inputs: a fixture's scenario byte, then Plan bytes. Each
/// Plan is one whose flooded queue competes with the commit's watch read:
/// the queue delivers an event after the commit and before the watch read.
/// That order is the property's trigger history, and the one on which
/// reverting #1566's top-of-turn watch check breaks the queue's rule.
const SEEDS: [Seed; 2] = [
    (
        "header-flood-wire",
        FloodScenario::Wire,
        WATCH_OBSERVED_UNDER_FLOOD,
    ),
    (
        "header-flood-lifecycle",
        FloodScenario::Lifecycle,
        WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
    ),
];

/// How many Plans [`seed_input`]'s search draws.
const SEARCH_PLANS: usize = 16;

/// The longest Plan [`seed_input`]'s search draws.
const SEARCH_PLAN_BYTES: usize = 256;

/// `scenario`'s fixture byte.
fn scenario_byte(scenario: FloodScenario) -> u8 {
    match scenario {
        FloodScenario::Wire => 0,
        FloodScenario::Lifecycle => 1,
    }
}

/// The input for a [`SEEDS`] row: its fixture byte, then the first of
/// [`SEARCH_PLANS`] Plans whose run reaches the property's trigger history
/// and holds it. Not every Plan's queue competes with the commit's watch
/// read, so the search skips the ones that don't. The process keeps each
/// found input.
fn seed_input((name, scenario, property): Seed) -> Vec<u8> {
    static FOUND: [OnceLock<Vec<u8>>; 2] = [OnceLock::new(), OnceLock::new()];
    let slot = &FOUND[usize::from(scenario_byte(scenario))];
    slot.get_or_init(|| {
        let entry = harness::find(Flood::NAME).expect("the harness is registered");
        header_inputs::first(
            name,
            support::byte_inputs(SEARCH_PLANS, SEARCH_PLAN_BYTES)
                .into_iter()
                .map(|plan| [&[scenario_byte(scenario)][..], &plan].concat()),
            |input| header_inputs::holds(entry, input, &[property]),
        )
    })
    .clone()
}

/// `input`'s Plan, after its scenario byte.
fn plan(input: &[u8]) -> PlanInput {
    Plan::from_fuzz(&input[Flood::SCENARIO_BYTES..])
}

/// `scenario`'s run under `input`, and the outcomes of the two watch
/// properties: wire, then lifecycle.
fn run(scenario: FloodScenario, input: PlanInput) -> (RunReport, [PropertyOutcome; 2]) {
    let ports = Flood::prepare(&scenario);
    let report = run_prepared::<Flood>(&scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    let outcome = |id: &str| {
        report
            .properties
            .iter()
            .find(|property| property.property == id)
            .map(|property| property.outcome.clone())
            .expect("the harness checks both watch properties")
    };
    let outcomes = [
        outcome(WATCH_OBSERVED_UNDER_FLOOD),
        outcome(WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD),
    ];
    (report, outcomes)
}

/// Each searched input holds its property through the registered harness
/// and reaches its trigger history: its queue delivered an event after the
/// commit and before the watch read.
#[test]
fn the_flood_seeds_hold_their_watch_rules() {
    let entry = harness::find(Flood::NAME).expect("the harness is registered");
    for seed in SEEDS {
        let input = seed_input(seed);
        let result = (entry.witness)(
            &entry.scenario(&input[..Flood::SCENARIO_BYTES]),
            plan(&input),
            seed.2,
            Expect::Holds,
            &[],
            &mut MemorySink::default(),
        );
        assert_eq!(result, Ok(()), "{}", seed.0);
    }
}

/// Both watch properties hold under every plan in both fixtures. Not every
/// plan's queue competes with the commit's watch read, which is why the
/// witnesses search for their Plans.
#[test]
fn the_flood_fixtures_hold_both_watch_rules_under_every_plan() {
    for scenario in [FloodScenario::Wire, FloodScenario::Lifecycle] {
        for input in support::inputs(8, 256) {
            let (report, outcomes) = run(scenario, input);
            assert_eq!(report.end.violation, None, "{scenario:?}: {:?}", report.end);
            for outcome in outcomes {
                assert!(
                    matches!(outcome, PropertyOutcome::Held { .. }),
                    "{scenario:?}: {outcome:?}"
                );
            }
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

/// The run's log and both watch outcomes depend only on the plan: the
/// wire input's plan A twice, then B, then A again, gives A's every
/// time, and B's log differs.
#[test]
fn flood_runs_repeat_a_a_and_a_b_a() {
    let wire = seed_input(SEEDS[0]);
    let plans: [&[u8]; 2] = [&wire[Flood::SCENARIO_BYTES..], &[7; 256]];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, outcomes) = run(FloodScenario::Wire, Plan::from_fuzz(plan));
            (
                format!("{:?}", report.log.entries()),
                format!("{outcomes:?}"),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to an input's index in [`SEEDS`] and a directory holding
/// its recorded trace, in the child process that
/// [`the_flood_witnesses_replay_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_FLOOD_TRACE";

/// Each searched input, recorded in this process, replays exactly in a
/// fresh process of the same test binary, where its run gives the same
/// watch outcomes.
#[test]
fn the_flood_witnesses_replay_in_a_fresh_process() {
    let entry = harness::find(Flood::NAME).expect("the harness is registered");
    if let Some(child) = std::env::var_os(CHILD) {
        let child = child.to_string_lossy().into_owned();
        let (index, dir) = child.split_once(':').expect("the parent sets index:dir");
        let seed = SEEDS[index.parse::<usize>().expect("the parent sets an index")];
        let trace = TraceReader::open(Path::new(dir)).expect("the trace was recorded");
        // A replay that leaves the trace's picks, draws or observations, or
        // fails where the recording didn't, errs.
        replay_with(&trace, entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        let input = seed_input(seed);
        let (_, outcomes) = run(seed.1, plan(&input));
        writeln!(std::io::stdout(), "FLOOD-BEGIN\n{outcomes:?}\nFLOOD-END")
            .expect("the test harness captures stdout");
        return;
    }
    for (index, seed) in SEEDS.into_iter().enumerate() {
        let input = seed_input(seed);
        let dir = TempDir::new().expect("a temporary directory");
        record(entry, RecordInput::Input(&input), dir.path()).expect("the witness records");
        let (_, outcomes) = run(seed.1, plan(&input));
        let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
            .args([
                "--exact",
                "the_flood_witnesses_replay_in_a_fresh_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, format!("{index}:{}", dir.path().display()))
            .output()
            .expect("the test binary runs");
        let stdout = String::from_utf8_lossy(&child.stdout);
        assert!(child.status.success(), "{}: {child:?}", seed.0);
        let replayed = stdout
            .split_once("FLOOD-BEGIN\n")
            .and_then(|(_, rest)| rest.split_once("\nFLOOD-END"))
            .map(|(outcomes, _)| outcomes)
            .unwrap_or_else(|| panic!("the child prints its outcomes: {stdout}"));
        assert_eq!(replayed, format!("{outcomes:?}"), "{}", seed.0);
    }
}

/// Each searched input draws its fixture, and runs through the registered
/// harness to a defined end that the explorer classifies clean.
#[test]
fn the_flood_seeds_draw_their_fixtures_and_run_clean() {
    let entry = harness::find(Flood::NAME).expect("the harness is registered");
    for seed in SEEDS {
        let stem = seed.0;
        let input = seed_input(seed);
        assert_eq!(Flood::draw(&input), seed.1, "{stem}");
        let run = (entry.run_bytes)(&input, &mut NullSink, None);
        let report = run
            .report()
            .unwrap_or_else(|| panic!("{stem}: {:?}", run.result));
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{stem}: {:?}",
            report.end
        );
        assert_eq!(run.leaked(), 0, "{stem}");
        assert_eq!(
            classify(entry, &run).outcome,
            ExploreOutcome::Clean,
            "{stem}"
        );
    }
}
