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
    domain::header_sync::CHUNK_BUDGET_BOUND,
    harness::{
        self,
        header_node::{
            capacity::{BoundaryRead, CapacityEvidence, CapacityReading},
            claim::SettlementEvidence,
            policy::{Allow, Hold, OpKind, Select, Trigger},
            reservation::ReservationEvidence,
            session::SessionEvidence,
            stale::StaleEvidence,
            tap::ReactorTap,
            Chain, ControlAction, ControlStep, DispatchFlag, HeaderNode, Policy,
        },
        header_sync::{Dialer, MirrorSpec, Storage, StorageScenario},
        record, run_prepared, HarnessEntry, HarnessError, RecordInput, Subsystem,
    },
    observe::{ReactorFact, StorageFact, StorageRequest},
    property::{Assumptions, Registry, RegistryChecker},
    record::RecordSink,
    trace::{replay_with, ReplayOptions, TraceReader},
    witness::Expect,
    HarnessFailure, MemorySink, NoChecks, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
    Sim,
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
/// submits its first prepare, which holds that round's staged headers. The
/// prepare is held until the shutdown closes the second link, so the stop
/// finds those headers still held.
pub fn witness() -> StorageScenario {
    let first_prepare = || Select::nth(OpKind::Prepare, 1);
    StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: first_prepare(),
                until: Trigger::Count(2, Box::new(Trigger::Closed("mirror-0".to_owned()))),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        mirrors: vec![MirrorSpec {
            chain: Chain::main(300),
            dialer: Dialer::Mirror,
            max_headers_per_response: None,
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
                Trigger::Submitted(first_prepare()),
            ),
        ],
        ..StorageScenario::default()
    }
}

/// Whether `reading` holds any header capacity.
fn claims(reading: &CapacityReading) -> bool {
    reading.reserved + reading.owned > 0
}

/// How the node's first prepare call ended, as its tap logged it, if it
/// did. The call is joined to its storage operation by the operation's id.
fn prepare_end(report: &RunReport) -> Option<String> {
    let prepare = report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Submitted {
                        id,
                        request: StorageRequest::PrepareHeaderTarget { .. },
                        ..
                    },
                ..
            } => Some(*id),
            _ => None,
        })?;
    report
        .log
        .entries()
        .iter()
        .find_map(|entry| match &entry.obs {
            Observation::Reactor {
                event: ReactorFact::CallEnded { end, op, .. },
                ..
            } if *op == Some(prepare) => Some(end.clone()),
            _ => None,
        })
}

/// Through a close, a relink and a shutdown, the probe's reads match the
/// work queue's accounting at quiet boundaries while the node holds
/// capacity, and once the stopped reactor is gone they read nothing
/// reserved or owned. The shutdown drops the held prepare, which held its
/// round's headers until then, so the teardown reads show they all came
/// back.
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
        assert_eq!(
            prepare_end(&report).as_deref(),
            Some("dropped"),
            "the shutdown drops the held prepare, so the reactor held its headers until it stopped"
        );
        assert!(
            readings
                .iter()
                .any(|reading| !reading.teardown && reading.owned > 0),
            "the probe reads the prepare's headers owned: {readings:#?}"
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

/// The committed close-and-relink seed (`tests/header_storage_seeds.rs`),
/// whose node reserves headers from both mirrors.
const CLOSE_RELINK_INPUT: &[u8] = include_bytes!("fixtures/header-storage-close-relink.bin");

/// `header-sync/chunk-budget-bound` holds on the committed close-and-relink
/// seed, and the run reaches its trigger history: the probe reads the budget
/// holding headers. `cargo xtask planted header-sync/chunk-budget-bound`
/// checks that the same input violates it under each planted patch.
#[test]
fn the_close_relink_input_holds_chunk_budget_bound() {
    let entry = harness::find(Storage::NAME).expect("the storage harness is registered");
    let (prefix, plan) = CLOSE_RELINK_INPUT.split_at(Storage::SCENARIO_BYTES);
    let result = (entry.witness)(
        &entry.scenario(prefix),
        Plan::from_fuzz(plan),
        CHUNK_BUDGET_BOUND,
        Expect::Holds,
        &[],
        &mut MemorySink::default(),
    );
    assert_eq!(result, Ok(()));
}

/// The witness's probe readings, its released rounds at each drain and its
/// full log depend only on scenario and plan: plan A twice in a row, then B,
/// then A again, gives A's every time, and B's log differs.
#[test]
fn the_capacity_witness_repeats_a_a_and_a_b_a() {
    let plans = inputs(3, 256);
    let scenario = witness();
    let runs: Vec<(String, String)> = [&plans[1], &plans[1], &plans[2], &plans[1]]
        .into_iter()
        .map(|input| {
            let ports = Storage::prepare(&scenario);
            let (capacity, rounds) = (ports.capacity.clone(), ports.rounds.clone());
            let report = run_prepared::<Storage>(
                &scenario,
                ports,
                input.clone(),
                &mut MemorySink::default(),
                None,
            )
            .unwrap_or_else(|error| panic!("{error}"));
            assert!(
                rounds.any_released(),
                "the close releases a round: {:?}",
                rounds.rows()
            );
            (
                format!("{:?}", report.log.entries()),
                format!("{:?}\n{:?}", capacity.readings(), rounds.rows()),
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
    /// The probe readings and released rounds of the run [`capture_run`]
    /// last prepared.
    static CAPTURED: RefCell<Option<(CapacityEvidence, SettlementEvidence)>> =
        const { RefCell::new(None) };
}

/// `header-sync/storage`'s own run, keeping its probe readings and released
/// rounds in [`CAPTURED`] for the replay to read.
fn capture_run(
    scenario: &Value,
    input: PlanInput,
    sink: &mut dyn RecordSink,
) -> Result<RunReport, HarnessError> {
    let scenario: StorageScenario =
        serde_json::from_value(scenario.clone()).map_err(HarnessError::Scenario)?;
    let ports = Storage::prepare(&scenario);
    CAPTURED.with(|captured| {
        *captured.borrow_mut() = Some((ports.capacity.clone(), ports.rounds.clone()));
    });
    run_prepared::<Storage>(&scenario, ports, input, sink, None)
}

/// The probe readings and released rounds [`capture_run`] last kept, as
/// text.
fn captured() -> String {
    let (capacity, rounds) = CAPTURED
        .with(|captured| captured.borrow_mut().take())
        .expect("the run prepared its ports");
    format!("{:?}\n{:?}\n", capacity.readings(), rounds.rows())
}

/// The witness recorded under one plan in this process, then replayed
/// exactly in a fresh process of the same test binary, gives the probe
/// readings and released rounds its recorded run gave.
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
        SettlementEvidence::default(),
        StaleEvidence::default(),
        Default::default(),
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

/// Runs `scenario` under `input` with its live phase cut after `budget`
/// steps and the harness's own drain after it, through the storage
/// harness's properties, and returns the report and the capacity evidence.
fn cut_run(
    scenario: &StorageScenario,
    input: PlanInput,
    budget: u64,
) -> (RunReport, CapacityEvidence) {
    let config = zakura_sim::RunConfig {
        budget,
        ..Storage::config(scenario)
    };
    let mut sim = Sim::new(config, input);
    let ports = Storage::prepare(scenario);
    let capacity = ports.capacity.clone();
    let storage = std::rc::Rc::new(Storage::build(&mut sim, scenario, ports));
    let registry = Registry::new(Storage::properties()).expect("unique IDs");
    let mut checker = RegistryChecker::new(
        registry,
        std::rc::Rc::clone(&storage),
        Assumptions::none(),
        &Storage::sources(),
    );
    let report = sim.run(&mut checker, &mut MemorySink::default());
    drop((checker, storage));
    (report, capacity)
}

/// A live phase cut at a boundary whose read found the budget's lock held
/// with the reactor stopped mid-poll leaves that boundary to the drain. The
/// drain resumes the reactor, a later boundary reads the budget, and
/// `header-sync/chunk-budget-bound` holds with no harness failure, so an
/// unread boundary at the cut concludes rather than failing the run (C19).
#[test]
fn a_drain_reads_past_an_unread_boundary_at_the_cut() {
    let mut cuts = 0;
    for input in inputs(16, 256) {
        let ports = Storage::prepare(&witness());
        let capacity = ports.capacity.clone();
        let full = run_prepared::<Storage>(
            &witness(),
            ports,
            input.clone(),
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let Some(budget) = (1..=full.end.steps)
            .find(|step| capacity.boundary(*step) == Some(BoundaryRead::UnreadMidPoll))
        else {
            continue;
        };
        let (cut, capacity) = cut_run(&witness(), input, budget);
        assert_eq!(cut.end.harness, None, "cut at {budget}: {:?}", cut.end);
        assert_eq!(cut.end.violation, None, "cut at {budget}: {:?}", cut.end);
        assert!(cut.end.drain.is_some(), "cut at {budget}: {:?}", cut.end);
        assert_eq!(
            capacity.boundary(budget),
            Some(BoundaryRead::UnreadMidPoll),
            "the cut run reaches the same unread boundary"
        );
        assert!(
            (budget + 1..=cut.end.steps)
                .any(|step| capacity.boundary(step) == Some(BoundaryRead::Read)),
            "a drain step reads the budget after the cut at {budget}"
        );
        let outcome = cut
            .properties
            .iter()
            .find(|property| property.property == CHUNK_BUDGET_BOUND)
            .map(|property| property.outcome.clone());
        assert!(
            matches!(outcome, Some(PropertyOutcome::Held { .. })),
            "cut at {budget}: {outcome:?}"
        );
        cuts += 1;
    }
    assert!(cuts > 0, "no input reached an unread boundary");
}

/// The capacity witness's close removes a round whose leases go back one
/// at a time, each a release that holds the budget's lock across its wake,
/// so the reactor stops mid-poll at several boundaries in a row with the
/// lock held. `header-sync/chunk-budget-bound` judges none of those
/// boundaries, never finds one unread with the reactor not mid-poll, and
/// holds.
#[test]
fn unread_boundaries_in_a_row_hold_chunk_budget_bound() {
    for input in inputs(16, 256) {
        let ports = Storage::prepare(&witness());
        let capacity = ports.capacity.clone();
        let report =
            run_prepared::<Storage>(&witness(), ports, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{error}"));
        let reads: Vec<Option<BoundaryRead>> = (1..=report.end.steps)
            .map(|step| capacity.boundary(step))
            .collect();
        assert!(
            reads.windows(2).any(|pair| pair
                .iter()
                .all(|read| *read == Some(BoundaryRead::UnreadMidPoll))),
            "two unread boundaries in a row: {reads:?}"
        );
        assert!(
            !reads.contains(&Some(BoundaryRead::UnreadQuiet)),
            "{reads:?}"
        );
        let outcome = report
            .properties
            .iter()
            .find(|property| property.property == CHUNK_BUDGET_BOUND)
            .map(|property| property.outcome.clone());
        assert!(
            matches!(outcome, Some(PropertyOutcome::Held { .. })),
            "{outcome:?}"
        );
    }
}
