//! hdr-6b: `header-sync/stale-completion-inert` on `header-sync/storage`. A
//! stale prepare or apply's handler must leave other work alone. The tap
//! finds each stale call and judges its handler's facts by the cause the
//! reactor recorded, and the property reports a break at the step whose
//! drain found it.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, io::Write, path::Path, process::Command};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::{
    domain::header_sync::STALE_COMPLETION_INERT,
    harness::{
        self,
        header_node::stale::StaleEvidence,
        header_sync::{Storage, StorageScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    MemorySink, Plan, PlanInput, RunReport,
};

/// The committed stale-prepare seed (`tests/header_storage_seeds.rs`),
/// whose node ends a prepare after its round retired while the same peer's
/// next round is active.
const STALE_PREPARE_INPUT: &[u8] = include_bytes!("fixtures/header-storage-stale-prepare.bin");

/// `header-sync/stale-completion-inert` holds on the committed stale-prepare
/// seed, and the run reaches its trigger history: a call ends stale.
/// `cargo xtask planted header-sync/stale-completion-inert` checks that the
/// same input violates it under each planted patch.
#[test]
fn the_stale_prepare_input_holds_stale_completion_inert() {
    let entry = harness::find(Storage::NAME).expect("the storage harness is registered");
    let (prefix, plan) = STALE_PREPARE_INPUT.split_at(Storage::SCENARIO_BYTES);
    let result = (entry.witness)(
        &entry.scenario(prefix),
        Plan::from_fuzz(plan),
        STALE_COMPLETION_INERT,
        Expect::Holds,
        &mut MemorySink::default(),
    );
    assert_eq!(result, Ok(()));
}

/// The node's stale calls, the property's report and the full log depend
/// only on the scenario and the plan: the stale-prepare seed's scenario
/// under plan A twice, then B, then A again, gives A's every time, and B's
/// log differs.
#[test]
fn stale_evidence_repeats_a_a_and_a_b_a() {
    let scenario: StorageScenario = serde_json::from_value(
        harness::find(Storage::NAME)
            .expect("the storage harness is registered")
            .scenario(&STALE_PREPARE_INPUT[..Storage::SCENARIO_BYTES]),
    )
    .expect("the seed's scenario is a storage scenario");
    let plans: [&[u8]; 2] = [&STALE_PREPARE_INPUT[Storage::SCENARIO_BYTES..], &[7; 256]];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let ports = Storage::prepare(&scenario);
            let stale = ports.stale_calls.clone();
            let report = run_prepared::<Storage>(
                &scenario,
                ports,
                Plan::from_fuzz(plan),
                &mut MemorySink::default(),
                None,
            )
            .unwrap_or_else(|error| panic!("{error}"));
            let property = report
                .properties
                .iter()
                .find(|property| property.property == STALE_COMPLETION_INERT)
                .expect("the property reports");
            (
                format!("{:?}", report.log.entries()),
                format!("{:?}\n{property:?}", stale.windows()),
            )
        })
        .collect();
    assert!(runs[0].1.contains("apply: false"), "{}", runs[0].1);
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to a directory holding a recorded trace, in the child process that
/// [`the_stale_witness_replays_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_STALE_TRACE";

thread_local! {
    /// The stale calls of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<StaleEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/storage`'s own run, keeping its stale calls in [`CAPTURED`]
/// for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: StorageScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = Storage::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.stale_calls.clone()));
    run_prepared::<Storage>(&scenario, ports, input, sink, None)
}

/// The stale calls [`capture_run`] last kept, which must include one, as
/// text.
fn captured() -> String {
    let stale = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    assert!(stale.any(), "{:?}", stale.windows());
    format!("{:?}\n", stale.windows())
}

/// The committed stale-prepare input, recorded in this process and replayed
/// exactly in a fresh process of the same test binary, finds the same stale
/// calls at the same drains.
#[test]
fn the_stale_witness_replays_in_a_fresh_process() {
    let entry = HarnessEntry {
        run: capture_run,
        ..*harness::find(Storage::NAME).expect("the storage harness is registered")
    };
    if let Some(dir) = std::env::var_os(CHILD) {
        let trace = TraceReader::open(Path::new(&dir)).expect("the trace was recorded");
        // A replay that leaves the trace's picks, draws or observations, or
        // fails where the recording didn't, errs.
        replay_with(&trace, &entry, ReplayOptions::default())
            .unwrap_or_else(|error| panic!("{error}"));
        // The parent reads the stale calls from the child's stdout.
        writeln!(std::io::stdout(), "STALE-BEGIN\n{}STALE-END", captured())
            .expect("the test harness captures stdout");
        return;
    }
    let dir = TempDir::new().expect("a temporary directory");
    record(&entry, RecordInput::Input(STALE_PREPARE_INPUT), dir.path())
        .expect("the witness records");
    let recorded = captured();
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_stale_witness_replays_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("STALE-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("STALE-END"))
        .map(|(stale, _)| stale)
        .unwrap_or_else(|| panic!("the child prints its stale calls: {stdout}"));
    assert_eq!(replayed, recorded);
}

/// `record --witness` judges a typed scenario and its Plan bytes, as
/// `cargo xtask planted` runs a typed case: the stale-prepare seed's
/// scenario as JSON, with its Plan bytes in a file. The witness passes,
/// the trace has no `input.bin`, and `replay` reruns it Exact in a fresh
/// process.
#[test]
fn a_typed_witness_records_and_replays_in_a_fresh_process() {
    let entry = harness::find(Storage::NAME).expect("the storage harness is registered");
    let (prefix, plan) = STALE_PREPARE_INPUT.split_at(Storage::SCENARIO_BYTES);
    let dir = TempDir::new().expect("a temporary directory");
    let plan_file = dir.path().join("plan.bin");
    let trace = dir.path().join("trace");
    std::fs::write(&plan_file, plan).expect("the plan is written");
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    let recorded = zakura_sim(&[
        "record",
        "--sim",
        Storage::NAME,
        "--scenario",
        &entry.scenario(prefix).to_string(),
        "--plan",
        plan_file.to_str().expect("the path is UTF-8"),
        "--out",
        trace.to_str().expect("the path is UTF-8"),
        "--witness",
        STALE_COMPLETION_INERT,
        "--expect",
        "holds",
    ]);
    assert!(recorded.status.success(), "{recorded:?}");
    let witness: Value = serde_json::from_str(
        &std::fs::read_to_string(trace.join(harness::WITNESS_FILE)).expect("witness.json"),
    )
    .expect("witness.json is JSON");
    assert_eq!(witness["passed"], Value::Bool(true), "{witness}");
    assert!(!trace.join("input.bin").exists());
    let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
    let stdout = String::from_utf8_lossy(&replayed.stdout);
    assert!(
        replayed.status.success() && stdout.contains("every pick, draw and observation matched"),
        "{replayed:?}"
    );
}
