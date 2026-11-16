//! hdr-7c: the watch properties on `header-sync/flood`. A node-side commit
//! publishes a new committed state while 16 mirrors flood the reactor's
//! wire queue or churn its lifecycle queue, and the run loop must read
//! that state from its committed watch before it takes a third event from
//! the flooded queue.
#![cfg(zakura_sim)]

mod support;

use std::{fs, io::Write, path::Path, path::PathBuf, process::Command};

use tempfile::TempDir;
use zakura_sim::{
    domain::header_sync::{WATCH_OBSERVED_UNDER_FLOOD, WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD},
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_sync::{Flood, FloodScenario},
        record, run_prepared, RecordInput, Subsystem,
    },
    seed,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    MemorySink, NullSink, Outcome, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The committed wire input, `tests/fixtures/header-flood-wire.bin`:
/// [`FloodScenario::Wire`]'s scenario byte, then Plan bytes. The wire
/// queue delivers an event after its commit and before the watch read. `cargo xtask planted
/// header-sync/watch-observed-under-flood` checks that reverting #1566's
/// top-of-turn watch check violates the wire rule on it, and only that.
const WIRE_INPUT: &[u8] = include_bytes!("fixtures/header-flood-wire.bin");

/// The committed lifecycle input, `tests/fixtures/header-flood-lifecycle.bin`,
/// as [`WIRE_INPUT`] is for the lifecycle queue and
/// `header-sync/watch-observed-under-lifecycle-flood`.
const LIFECYCLE_INPUT: &[u8] = include_bytes!("fixtures/header-flood-lifecycle.bin");

/// A committed input's file stem, its fixture, its property, and which of
/// 24 [`support::byte_inputs`] of up to 256 bytes is its Plan.
type Seed = (&'static str, FloodScenario, &'static str, usize);

/// Every committed input. Each Plan is one whose flooded queue competes
/// with the commit's watch read, and on which the reverted reactor breaks
/// that queue's rule while still adopting the published state before
/// either queue's third event: through the held read's completion in the
/// wire fixture, and a churn connect in the lifecycle one.
const SEEDS: [Seed; 2] = [
    (
        "header-flood-wire",
        FloodScenario::Wire,
        WATCH_OBSERVED_UNDER_FLOOD,
        14,
    ),
    (
        "header-flood-lifecycle",
        FloodScenario::Lifecycle,
        WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
        2,
    ),
];

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// The input [`regenerate_flood_seeds`] builds for `seed`: the fixture's
/// scenario byte, then its Plan.
fn generated((_, scenario, _, index): Seed) -> Vec<u8> {
    let byte = match scenario {
        FloodScenario::Wire => 0,
        FloodScenario::Lifecycle => 1,
    };
    let mut input = vec![byte];
    input.extend(support::byte_inputs(24, 256).swap_remove(index));
    input
}

/// `input`'s Plan, after its scenario byte.
fn plan(input: &[u8]) -> PlanInput {
    Plan::from_fuzz(&input[Flood::SCENARIO_BYTES..])
}

/// The committed input for `seed`.
fn committed(seed: Seed) -> &'static [u8] {
    match seed.1 {
        FloodScenario::Wire => WIRE_INPUT,
        FloodScenario::Lifecycle => LIFECYCLE_INPUT,
    }
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

/// Each committed input holds its property through the registered harness
/// and reaches its trigger history: its queue delivered an event after the
/// commit and before the watch read.
#[test]
fn the_committed_flood_inputs_hold_their_watch_rules() {
    let entry = harness::find(Flood::NAME).expect("the harness is registered");
    for seed in SEEDS {
        let input = committed(seed);
        let result = (entry.witness)(
            &entry.scenario(&input[..Flood::SCENARIO_BYTES]),
            plan(input),
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
/// committed inputs are pinned.
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
/// committed wire plan A twice, then B, then A again, gives A's every
/// time, and B's log differs.
#[test]
fn flood_runs_repeat_a_a_and_a_b_a() {
    let plans: [&[u8]; 2] = [&WIRE_INPUT[Flood::SCENARIO_BYTES..], &[7; 256]];
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

/// Set, to a committed input's index in [`SEEDS`] and a directory holding
/// its recorded trace, in the child process that
/// [`the_flood_witnesses_replay_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_FLOOD_TRACE";

/// Each committed input, recorded in this process, replays exactly in a
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
        let input = committed(seed);
        let (_, outcomes) = run(seed.1, plan(input));
        writeln!(std::io::stdout(), "FLOOD-BEGIN\n{outcomes:?}\nFLOOD-END")
            .expect("the test harness captures stdout");
        return;
    }
    for (index, seed) in SEEDS.into_iter().enumerate() {
        let input = committed(seed);
        let dir = TempDir::new().expect("a temporary directory");
        record(entry, RecordInput::Input(input), dir.path()).expect("the witness records");
        let (_, outcomes) = run(seed.1, plan(input));
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

/// Each committed input is its [`generated`] input, and runs through the
/// registered harness to a defined end that the explorer classifies clean.
#[test]
fn the_committed_flood_inputs_are_their_plans_and_run_clean() {
    let entry = harness::find(Flood::NAME).expect("the harness is registered");
    for seed in SEEDS {
        let stem = seed.0;
        let input = committed(seed);
        assert_eq!(
            input,
            generated(seed),
            "{stem}: the committed input changed; regenerate it if that's intended"
        );
        assert_eq!(Flood::draw(input), seed.1, "{stem}");
        let run = (entry.run_bytes)(input, &mut NullSink, None);
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

/// Rewrites the committed inputs and their sidecars. Run it with
/// `RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --test
/// header_flood_watch -- --ignored regenerate_flood_seeds`.
#[test]
#[ignore = "rewrites the committed inputs"]
fn regenerate_flood_seeds() {
    for seed in SEEDS {
        let (stem, scenario, property, _) = seed;
        let input = generated(seed);
        let base = fixtures().join(stem);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let sidecar = format!(
            "# The committed fuzz input `{stem}.bin`: the harness that runs it, the\n\
             # generator version it was built for, and its SHA-256. Its first byte\n\
             # picks the flood fixture, then come the Plan bytes.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/header_flood_watch.rs\"\n\
             \n\
             [[witness]]\n\
             property = \"{property}\"\n\
             expect = \"holds\"\n",
            harness = Flood::NAME,
            generator = Flood::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(
                &serde_json::to_value(scenario).expect("scenarios serialize")
            ),
        );
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}
