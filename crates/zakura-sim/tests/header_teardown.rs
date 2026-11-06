//! hdr-6a: `header-sync/teardown-returns-baseline`'s round settlement on
//! `header-sync/storage`. Each released round returns its reservation and
//! leases once its calls are handled. The ownership ledger checks that at
//! the events, and the property reports a break at the step that found it.
#![cfg(zakura_sim)]

use std::{cell::RefCell, io::Write, path::Path, process::Command};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::{
    domain::header_sync::{CHUNK_BUDGET_BOUND, TEARDOWN_RETURNS_BASELINE},
    harness::{
        self,
        header_node::claim::RoundEvidence,
        header_sync::{Storage, StorageScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    MemorySink, Plan, PlanInput, RunReport,
};

/// The committed close-and-relink seed (`tests/header_storage_seeds.rs`),
/// whose node releases rounds as its mirrors close and finish.
const CLOSE_RELINK_INPUT: &[u8] = include_bytes!("fixtures/header-storage-close-relink.bin");

/// `header-sync/teardown-returns-baseline` holds on the committed
/// close-and-relink seed, and the run reaches its trigger history: the
/// queue releases a round. `cargo xtask planted
/// header-sync/teardown-returns-baseline` checks that the same input
/// violates it under each planted patch.
#[test]
fn the_close_relink_input_holds_teardown_returns_baseline() {
    let entry = harness::find(Storage::NAME).expect("the storage harness is registered");
    let (prefix, plan) = CLOSE_RELINK_INPUT.split_at(Storage::SCENARIO_BYTES);
    let result = (entry.witness)(
        &entry.scenario(prefix),
        Plan::from_fuzz(plan),
        TEARDOWN_RETURNS_BASELINE,
        Expect::Holds,
        &mut MemorySink::default(),
    );
    assert_eq!(result, Ok(()));
}

/// The node's released rounds at each drain, both header-sync properties'
/// reports and the full log depend only on the scenario and the plan: the
/// close-and-relink seed's scenario under plan A twice, then B, then A
/// again, gives A's every time, and B's log differs.
#[test]
fn round_evidence_repeats_a_a_and_a_b_a() {
    let scenario: StorageScenario = serde_json::from_value(
        harness::find(Storage::NAME)
            .expect("the storage harness is registered")
            .scenario(&CLOSE_RELINK_INPUT[..Storage::SCENARIO_BYTES]),
    )
    .expect("the seed's scenario is a storage scenario");
    let plans: [&[u8]; 2] = [&CLOSE_RELINK_INPUT[Storage::SCENARIO_BYTES..], &[7; 256]];
    let runs: Vec<(String, String)> = [plans[0], plans[0], plans[1], plans[0]]
        .into_iter()
        .map(|plan| {
            let ports = Storage::prepare(&scenario);
            let rounds = ports.rounds.clone();
            let report = run_prepared::<Storage>(
                &scenario,
                ports,
                Plan::from_fuzz(plan),
                &mut MemorySink::default(),
                None,
            )
            .unwrap_or_else(|error| panic!("{error}"));
            assert!(rounds.any_released(), "{:?}", rounds.rows());
            let reports: Vec<_> = report
                .properties
                .iter()
                .filter(|property| {
                    [TEARDOWN_RETURNS_BASELINE, CHUNK_BUDGET_BOUND]
                        .contains(&property.property.as_str())
                })
                .collect();
            assert_eq!(reports.len(), 2, "{:?}", report.properties);
            (
                format!("{:?}", report.log.entries()),
                format!("{:?}\n{reports:?}", rounds.rows()),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to a directory holding a recorded trace, in the child process that
/// [`the_teardown_witness_replays_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_TEARDOWN_TRACE";

thread_local! {
    /// The released rounds of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<RoundEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/storage`'s own run, keeping its released rounds in
/// [`CAPTURED`] for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: StorageScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = Storage::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.rounds.clone()));
    run_prepared::<Storage>(&scenario, ports, input, sink, None)
}

/// The rounds [`capture_run`] last kept, which must include a released
/// round, as text.
fn captured() -> String {
    let rounds = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    assert!(rounds.any_released(), "{:?}", rounds.rows());
    format!("{:?}\n", rounds.rows())
}

/// The committed close-and-relink witness, recorded in this process and
/// replayed exactly in a fresh process of the same test binary, gives the
/// released rounds at each drain its recorded run gave.
#[test]
fn the_teardown_witness_replays_in_a_fresh_process() {
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
        // The parent reads the rounds from the child's stdout.
        writeln!(std::io::stdout(), "ROUNDS-BEGIN\n{}ROUNDS-END", captured())
            .expect("the test harness captures stdout");
        return;
    }
    let dir = TempDir::new().expect("a temporary directory");
    record(&entry, RecordInput::Input(CLOSE_RELINK_INPUT), dir.path())
        .expect("the witness records");
    let recorded = captured();
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_teardown_witness_replays_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("ROUNDS-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("ROUNDS-END"))
        .map(|(rounds, _)| rounds)
        .unwrap_or_else(|| panic!("the child prints its rounds: {stdout}"));
    assert_eq!(replayed, recorded);
}
