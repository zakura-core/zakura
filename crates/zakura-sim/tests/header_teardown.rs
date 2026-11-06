//! hdr-6a: `header-sync/teardown-returns-baseline`'s round settlement on
//! `header-sync/storage`. Each released round returns its reservation and
//! leases once its calls are handled. The ownership ledger checks that at
//! the events, and the property reports a break at the step that found it.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, io::Write, path::Path, process::Command};

use serde_json::Value;
use tempfile::TempDir;
use zakura_sim::{
    domain::header_sync::{CHUNK_BUDGET_BOUND, TEARDOWN_RETURNS_BASELINE},
    harness::{
        self,
        header_node::{
            claim::SettlementEvidence,
            policy::{Allow, Hold, OpKind, Select, Trigger},
            ControlAction, ControlStep, Policy,
        },
        header_sync::{Storage, StorageScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    MemorySink, Plan, PlanInput, PropertyOutcome, RunReport,
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
    static CAPTURED: RefCell<Option<SettlementEvidence>> = const { RefCell::new(None) };
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

/// The plan's teardown witness: the node's first prepare is held, and
/// `mirror-0`'s link closes once that prepare is submitted, so the queue
/// releases its round while the call still holds the round's headers. The
/// close lifts the hold, the prepare is handled, and the round settles.
/// The node shuts down once that prepare is handled, and its teardown
/// settles. `header-sync/teardown-returns-baseline` holds at both levels,
/// and the run never fails `ShutdownIncomplete`.
fn held_lease_witness() -> StorageScenario {
    let first_prepare = || Select::nth(OpKind::Prepare, 1);
    StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: first_prepare(),
                until: Trigger::Closed("mirror-0".to_owned()),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![
            ControlStep {
                peer: "mirror-0".to_owned(),
                action: ControlAction::Close,
                when: Some(Trigger::Submitted(first_prepare())),
            },
            ControlStep {
                peer: "node".to_owned(),
                action: ControlAction::Shutdown,
                when: Some(Trigger::Handled(first_prepare())),
            },
        ],
        ..StorageScenario::default()
    }
}

/// [`held_lease_witness`]: under every plan, a released round stays
/// unsettled while its prepare holds it, then settles, and the stopped node
/// settles too, with the property applying and holding.
#[test]
fn teardown_waits_for_held_lease() {
    let scenario = held_lease_witness();
    let mut held = 0;
    for input in support::inputs(8, 256) {
        let ports = Storage::prepare(&scenario);
        let settlement = ports.rounds.clone();
        let report =
            run_prepared::<Storage>(&scenario, ports, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let rows = settlement.rows();
        let teardown = report
            .properties
            .iter()
            .find(|property| property.property == TEARDOWN_RETURNS_BASELINE)
            .map(|property| property.outcome.clone());
        assert!(
            matches!(teardown, Some(PropertyOutcome::Held { .. })),
            "{teardown:?}: {rows:?}"
        );
        assert!(settlement.settled(), "the stopped node settles: {rows:?}");
        let waited = rows
            .iter()
            .position(|row| row.released > 0 && row.unsettled > 0);
        if let Some(waited) = waited {
            assert!(
                rows[waited..]
                    .iter()
                    .any(|row| row.unsettled == 0 && !row.settled),
                "the round settles before the node does: {rows:?}"
            );
            held += 1;
        }
    }
    assert!(
        held > 0,
        "no plan released the round while its prepare held it"
    );
}

/// The node's first locator read is held until `mirror-0`'s link closes,
/// and the node shuts down once that read is submitted, so storage can
/// still hold the read's permit when the reactor goes and return it after.
/// Every teardown read is checked against storage's permits at that read,
/// so an honest run holds throughout.
fn held_locator_witness() -> StorageScenario {
    let first_locator = || Select::nth(OpKind::Locator, 1);
    StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: first_locator(),
                until: Trigger::Closed("mirror-0".to_owned()),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "node".to_owned(),
            action: ControlAction::Shutdown,
            when: Some(Trigger::Submitted(first_locator())),
        }],
        ..StorageScenario::default()
    }
}

/// [`held_locator_witness`] settles the node and holds under every plan.
/// With `forgotten-locator-permit` applied, a plan whose locator completes
/// after the reactor goes finds the leaked permit on a later teardown read.
#[test]
fn a_locator_held_across_reactor_exit_holds() {
    let scenario = held_locator_witness();
    for input in support::inputs(8, 256) {
        let ports = Storage::prepare(&scenario);
        let settlement = ports.rounds.clone();
        let report =
            run_prepared::<Storage>(&scenario, ports, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        assert!(settlement.settled(), "{:?}", settlement.rows());
    }
}
