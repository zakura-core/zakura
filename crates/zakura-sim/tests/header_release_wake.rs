//! hdr-7a: `header-sync/capacity-release-wakes` on `header-sync/release-wake`,
//! the wake fixture. Two filling mirrors' rounds use the node's 500 headers
//! of body window with their prepares held, a waiting mirror's offer waits
//! for room, and a released round's leases coming back must get the waiting
//! offer's locator read to storage before the reactor goes quiet.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, fs, io::Write, path::Path, path::PathBuf, process::Command};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::{
    domain::header_sync::CAPACITY_RELEASE_WAKES,
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_node::wake::{WakeEvidence, WakeFact},
        header_sync::{ReleaseWake, StorageScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    record::RecordSink,
    seed,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    MemorySink, NullSink, Outcome, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The committed input, `tests/fixtures/header-release-wake.bin`: Plan
/// bytes alone, since the harness has one scenario. Its run's release
/// comes from a completion drained at the top of the reactor's loop, so
/// only the budget's release wake gets the reactor back to the waiting
/// offer. `cargo xtask planted header-sync/capacity-release-wakes` checks
/// that removing that wake violates the property on it.
const INPUT: &[u8] = include_bytes!("fixtures/header-release-wake.bin");

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// The committed input as [`regenerate_release_wake_seed`] builds it: the
/// 156th of 400 [`support::byte_inputs`] of up to 512 bytes, the first
/// whose release is drained at the top of the loop.
fn generated() -> Vec<u8> {
    support::byte_inputs(400, 512).swap_remove(155)
}

/// The fixture's run under `input`, and its wake facts.
fn run(input: PlanInput) -> (RunReport, Vec<(u64, WakeFact)>) {
    let scenario = StorageScenario::release_wake();
    let ports = ReleaseWake::prepare(&scenario);
    let wake = ports.wake.clone();
    let report =
        run_prepared::<ReleaseWake>(&scenario, ports, input, &mut MemorySink::default(), None)
            .unwrap_or_else(|error| panic!("{error}"));
    (report, wake.facts())
}

/// Checks that `report`'s run opened a wake for a waiting offer, got that
/// offer's locator read to storage after it, and held
/// `header-sync/capacity-release-wakes`.
fn wake_held(report: &RunReport, facts: &[(u64, WakeFact)]) {
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    let (opened, peer) = facts
        .iter()
        .find_map(|(stamp, fact)| match fact {
            WakeFact::Opened { peer, .. } => Some((*stamp, peer.clone())),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no release opened a wake: {facts:?}"));
    assert!(
        facts.iter().any(|(stamp, fact)| *stamp > opened
            && matches!(fact, WakeFact::Submitted { peer: read } if *read == peer)),
        "the waiting offer's locator reached storage: {facts:?}"
    );
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == CAPACITY_RELEASE_WAKES)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::Held { .. })),
        "{outcome:?}"
    );
}

/// `header-sync/capacity-release-wakes` holds on the committed input
/// through the registered harness, and the run reaches its trigger
/// history: a wake opens and the waiting offer's locator reaches storage.
#[test]
fn the_release_wake_input_holds_capacity_release_wakes() {
    let entry = harness::find(ReleaseWake::NAME).expect("the harness is registered");
    let result = (entry.witness)(
        &entry.scenario(&[]),
        Plan::from_fuzz(INPUT),
        CAPACITY_RELEASE_WAKES,
        Expect::Holds,
        &mut MemorySink::default(),
    );
    assert_eq!(result, Ok(()));
    let (report, facts) = run(Plan::from_fuzz(INPUT));
    wake_held(&report, &facts);
}

/// The fixture opens a wake, gets the waiting offer's locator read to
/// storage and holds the property under every plan, not only the committed
/// one.
#[test]
fn the_wake_fixture_holds_under_every_plan() {
    for input in support::inputs(16, 256) {
        let (report, facts) = run(input);
        wake_held(&report, &facts);
    }
}

/// A fixture without the wake declared opens no wake, so the property is
/// not applicable there.
#[test]
fn an_undeclared_fixture_opens_no_wake() {
    let scenario = StorageScenario::release_wake();
    let ports = harness::header_sync::Ports {
        wake: WakeEvidence::default(),
        ..ReleaseWake::prepare(&scenario)
    };
    let wake = ports.wake.clone();
    let report = run_prepared::<ReleaseWake>(
        &scenario,
        ports,
        Plan::from_fuzz(INPUT),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(wake.facts(), []);
    let outcome = report
        .properties
        .iter()
        .find(|property| property.property == CAPACITY_RELEASE_WAKES)
        .map(|property| property.outcome.clone());
    assert!(
        matches!(outcome, Some(PropertyOutcome::NotApplicable)),
        "{outcome:?}"
    );
}

/// The wake facts, the property's report and the full log depend only on
/// the plan: the committed plan A twice, then B, then A again, gives A's
/// every time, and B's log differs.
#[test]
fn wake_evidence_repeats_a_a_and_a_b_a() {
    let plans: [&[u8]; 2] = [INPUT, &[7; 256]];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let (report, facts) = run(Plan::from_fuzz(plan));
            let property = report
                .properties
                .iter()
                .find(|property| property.property == CAPACITY_RELEASE_WAKES)
                .expect("the property reports");
            (
                format!("{:?}", report.log.entries()),
                format!("{facts:?}\n{property:?}"),
            )
        })
        .collect();
    assert!(runs[0].1.contains("Opened"), "{}", runs[0].1);
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to a directory holding a recorded trace, in the child process that
/// [`the_release_wake_witness_replays_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_RELEASE_WAKE_TRACE";

thread_local! {
    /// The wake facts of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<WakeEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/release-wake`'s own run, keeping its wake facts in
/// [`CAPTURED`] for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: StorageScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = ReleaseWake::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.wake.clone()));
    run_prepared::<ReleaseWake>(&scenario, ports, input, sink, None)
}

/// The wake facts [`capture_run`] last kept, which must open a wake, as
/// text.
fn captured() -> String {
    let wake = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    assert!(wake.any_opened(), "{:?}", wake.facts());
    format!("{:?}\n", wake.facts())
}

/// The committed input, recorded in this process and replayed exactly in a
/// fresh process of the same test binary, finds the same wake facts with
/// the same stamps.
#[test]
fn the_release_wake_witness_replays_in_a_fresh_process() {
    let entry = HarnessEntry {
        run: capture_run,
        ..*harness::find(ReleaseWake::NAME).expect("the harness is registered")
    };
    if let Some(dir) = std::env::var_os(CHILD) {
        let trace = TraceReader::open(Path::new(&dir)).expect("the trace was recorded");
        // A replay that leaves the trace's picks, draws or observations, or
        // fails where the recording didn't, errs.
        replay_with(&trace, &entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        // The parent reads the wake facts from the child's stdout.
        writeln!(std::io::stdout(), "WAKE-BEGIN\n{}WAKE-END", captured())
            .expect("the test harness captures stdout");
        return;
    }
    let dir = TempDir::new().expect("a temporary directory");
    record(&entry, RecordInput::Input(INPUT), dir.path()).expect("the witness records");
    let recorded = captured();
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_release_wake_witness_replays_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("WAKE-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("WAKE-END"))
        .map(|(wake, _)| wake)
        .unwrap_or_else(|| panic!("the child prints its wake facts: {stdout}"));
    assert_eq!(replayed, recorded);
}

/// The committed input is [`generated`], and runs through the registered
/// harness to a defined end that the explorer classifies clean.
#[test]
fn the_committed_input_is_its_plan_and_runs_clean() {
    assert_eq!(
        INPUT,
        generated(),
        "the committed input changed; regenerate it if that's intended"
    );
    let entry = harness::find(ReleaseWake::NAME).expect("the harness is registered");
    let run = (entry.run_bytes)(INPUT, &mut NullSink, None);
    let report = run.report().unwrap_or_else(|| panic!("{:?}", run.result));
    assert!(
        matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
        "{:?}",
        report.end
    );
    assert_eq!(run.leaked(), 0);
    assert_eq!(classify(entry, &run).outcome, ExploreOutcome::Clean);
}

/// Rewrites the committed input and its sidecar. Run it with
/// `RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --test
/// header_release_wake -- --ignored regenerate_release_wake_seed`.
#[test]
#[ignore = "rewrites the committed input"]
fn regenerate_release_wake_seed() {
    let input = generated();
    let base = fixtures().join("header-release-wake");
    fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
    let sidecar = format!(
        "# The committed fuzz input `header-release-wake.bin`: the harness that runs it,\n\
         # the generator version it was built for, and its SHA-256. The harness\n\
         # has one scenario, so the input is Plan bytes alone.\n\
         harness = \"{harness}\"\n\
         generator = {generator}\n\
         sha256 = \"{digest}\"\n\
         scenario_sha256 = \"{scenario}\"\n\
         source = \"crates/zakura-sim/tests/header_release_wake.rs\"\n\
         \n\
         [[witness]]\n\
         property = \"{CAPACITY_RELEASE_WAKES}\"\n\
         expect = \"holds\"\n",
        harness = ReleaseWake::NAME,
        generator = ReleaseWake::GENERATOR,
        digest = seed::sha256(&input),
        scenario = seed::scenario_sha256(
            &serde_json::to_value(StorageScenario::release_wake()).expect("scenarios serialize")
        ),
    );
    fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
}
