//! hdr-4's capacity probe on `header-sync/storage`: the node's reactor hands
//! its tap a resource probe, which reads the header budget's reserved and
//! owned counts and the free locator permits at every boundary. The tap
//! checks every read against the budget's capacity, each read at a quiet
//! boundary against the work queue's own accounting, and every read once a
//! stopped reactor is gone against the zero baseline. So every run here also
//! checks those rules. These witnesses show the runs reach the cases they
//! name.
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, io::Write, path::Path, process::Command, sync::Arc};

use serde_json::Value;
use support::{config, inputs};
use tempfile::TempDir;
use zakura_network::zakura::observe::HeaderSyncObserver;
use zakura_sim::{
    harness::{
        self,
        header_node::{
            capacity::{CapacityEvidence, CapacityReading},
            policy::{OpKind, Select, Trigger},
            reservation::ReservationEvidence,
            session::SessionEvidence,
            tap::ReactorTap,
            Chain, ControlAction, ControlStep, DispatchFlag, HeaderNode,
        },
        header_sync::{Dialer, MirrorSpec, Storage, StorageScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    HarnessFailure, MemorySink, NoChecks, PlanInput, RunReport, Sim,
};
use zakura_storage::header::{adapter::EnginePort, fixture::HeaderFixture};

/// Runs `scenario` on freshly prepared ports under `input`. The run must end
/// without a violation or harness failure. Returns its report and the
/// probe's readings.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, Vec<CapacityReading>) {
    let ports = Storage::prepare(scenario);
    let capacity = ports.capacity.clone();
    let report = run_prepared::<Storage>(scenario, ports, input, &mut MemorySink::default(), None)
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(report.end.harness, None, "{:?}", report.end);
    (report, capacity.readings())
}

/// `action` on `peer`, offered once `when` holds.
fn step(peer: &str, action: ControlAction, when: Trigger) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when: Some(when),
    }
}

/// The capacity witness: `mirror-0`, at A300 so its round continues past
/// one fair share, has its link closed once the node reserves headers for
/// it, then relinks once the close is made, and the node shuts down once it
/// submits its first prepare, which holds that round's staged headers.
pub fn witness() -> StorageScenario {
    StorageScenario {
        mirrors: vec![MirrorSpec {
            chain: Chain::main(300),
            dialer: Dialer::Mirror,
        }],
        controls: vec![
            step(
                "mirror-0",
                ControlAction::Close,
                Trigger::ReservationOpened("mirror-0".to_owned()),
            ),
            step(
                "mirror-0",
                ControlAction::Relink,
                Trigger::Closed("mirror-0".to_owned()),
            ),
            step(
                "node",
                ControlAction::Shutdown,
                Trigger::Submitted(Select::nth(OpKind::Prepare, 1)),
            ),
        ],
        ..StorageScenario::default()
    }
}

/// Whether `reading` holds any header capacity.
fn claims(reading: &CapacityReading) -> bool {
    reading.reserved + reading.owned > 0
}

/// Through a close, a relink and a shutdown, the probe's reads match the
/// work queue's accounting at quiet boundaries while the node holds
/// capacity, and once the stopped reactor is gone they read nothing
/// reserved or owned. The node shuts down holding the headers its prepare
/// took, so the teardown reads show they all came back.
#[test]
fn the_probe_matches_the_accounting_through_close_relink_and_shutdown() {
    let scenario = witness();
    for input in inputs(16, 256) {
        let (report, readings) = run(&scenario, input);
        assert!(
            readings
                .iter()
                .any(|reading| reading.compared && claims(reading)),
            "a quiet read compares held capacity: {readings:#?}"
        );
        let stop = report
            .picks
            .iter()
            .find(|pick| pick.identity == "peer:node:shutdown")
            .expect("the node shuts down")
            .step;
        assert!(
            readings
                .iter()
                .rev()
                .find(|reading| reading.step <= stop)
                .is_some_and(claims),
            "the node holds capacity as it shuts down: {readings:#?}"
        );
        let teardown = readings
            .iter()
            .position(|reading| reading.teardown)
            .unwrap_or_else(|| panic!("the stopped reactor is read once gone: {readings:#?}"));
        assert!(
            readings[teardown..]
                .iter()
                .all(|reading| reading.teardown && !claims(reading)),
            "{readings:#?}"
        );
    }
}

/// The witness's probe readings and full log depend only on scenario and
/// plan: plan A twice in a row, then B, then A again, gives A's every time,
/// and B's log differs.
#[test]
fn the_capacity_witness_repeats_a_a_and_a_b_a() {
    let plans = inputs(3, 256);
    let scenario = witness();
    let runs: Vec<(String, String)> = [&plans[1], &plans[1], &plans[2], &plans[1]]
        .into_iter()
        .map(|input| {
            let (report, readings) = run(&scenario, input.clone());
            (
                format!("{:?}", report.log.entries()),
                format!("{readings:?}"),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// Set, to a directory holding a recorded trace, in the child process that
/// [`the_capacity_witness_replays_in_a_fresh_process`] starts.
const CHILD: &str = "ZAKURA_SIM_CAPACITY_TRACE";

thread_local! {
    /// The probe readings of the run [`capture_run`] last prepared.
    static CAPTURED: RefCell<Option<CapacityEvidence>> = const { RefCell::new(None) };
}

/// `header-sync/storage`'s own run, keeping its probe readings in
/// [`CAPTURED`] for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: StorageScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = Storage::prepare(&scenario);
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(ports.capacity.clone()));
    run_prepared::<Storage>(&scenario, ports, input, sink, None)
}

/// The probe readings [`capture_run`] last kept, as text.
fn captured() -> String {
    let evidence = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    format!("{:?}\n", evidence.readings())
}

/// The witness recorded under one plan in this process, then replayed
/// exactly in a fresh process of the same test binary, gives the probe
/// readings its recorded run gave.
#[test]
fn the_capacity_witness_replays_in_a_fresh_process() {
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
        // The parent reads the readings from the child's stdout.
        writeln!(
            std::io::stdout(),
            "READINGS-BEGIN\n{}READINGS-END",
            captured()
        )
        .expect("the test harness captures stdout");
        return;
    }
    let PlanInput::Bytes(plan) = inputs(1, 256).remove(0) else {
        unreachable!("fuzz inputs are bytes");
    };
    let dir = TempDir::new().expect("a temporary directory");
    let scenario = serde_json::to_value(witness()).expect("a scenario serializes");
    let what = RecordInput::Plan {
        scenario: &scenario,
        plan: &plan,
    };
    record(&entry, what, dir.path()).expect("the witness records");
    let recorded = captured();
    let child = Command::new(std::env::current_exe().expect("the test binary has a path"))
        .args([
            "--exact",
            "the_capacity_witness_replays_in_a_fresh_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path())
        .output()
        .expect("the test binary runs");
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(child.status.success(), "{child:?}");
    let replayed = stdout
        .split_once("READINGS-BEGIN\n")
        .and_then(|(_, rest)| rest.split_once("READINGS-END"))
        .map(|(readings, _)| readings)
        .unwrap_or_else(|| panic!("the child prints its readings: {stdout}"));
    assert_eq!(replayed, recorded);
}

/// Runs a tap that reads a resource probe, handing its observer to the
/// reactors `spawn` starts, which live until the run ends. Returns the
/// run's source evidence failure.
fn probe_failure(
    spawn: impl FnOnce(&mut Sim, &HeaderFixture, &Arc<dyn HeaderSyncObserver>) -> Vec<HeaderNode>,
) -> String {
    let fixture = HeaderFixture::regtest();
    let ports = Storage::prepare(&StorageScenario::default());
    let mut sim = Sim::new(config(2, 100), PlanInput::Bytes(Vec::new()));
    let (observer, tap) = ReactorTap::with_evidence(
        &mut sim,
        &ports.node,
        "node",
        SessionEvidence::default(),
        ReservationEvidence::default(),
        CapacityEvidence::default(),
    );
    sim.add_source(Box::new(tap));
    let nodes = spawn(&mut sim, &fixture, &observer);
    let report = sim.run(&mut NoChecks, &mut MemorySink::default());
    drop((nodes, observer));
    match report.end.harness {
        Some(HarnessFailure::SourceEvidence { detail, .. }) => detail,
        end => panic!("expected a source evidence failure, got {end:?}"),
    }
}

/// A tap that reads a probe, whose observer no reactor took, has no probe at
/// its first drain, so the run fails as `ProbeMissing`. The test holds the
/// observer, so the tap never sees a reactor go.
#[test]
fn a_tap_whose_reactor_never_hands_over_a_probe_fails() {
    let detail = probe_failure(|_, _, _| Vec::new());
    assert!(detail.starts_with("ProbeMissing"), "{detail}");
}

/// Two reactors started with one tap's observer each hand it a probe, so the
/// tap can't tell which reactor its reads are of, and the run fails as
/// `ProbeReplaced`.
#[test]
fn a_tap_handed_a_second_probe_fails() {
    let detail = probe_failure(|sim, fixture, observer| {
        [("node", 1), ("again", 2)]
            .into_iter()
            .map(|(name, seed)| {
                HeaderNode::spawn(
                    sim,
                    name,
                    seed,
                    Arc::new(EnginePort::new(fixture)),
                    fixture,
                    fixture.anchor(),
                    DispatchFlag::default(),
                    Some(Arc::clone(observer)),
                )
            })
            .collect()
    });
    assert!(detail.starts_with("ProbeReplaced"), "{detail}");
}
