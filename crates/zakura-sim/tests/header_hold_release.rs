//! hdr-10a: `header-sync/hold-release-wakes` on `header-sync/liveness`.
//! Each hold fixture holds the property and applies, with the hdr-6 and
//! hdr-9 controls holding or not applying beside it. `cargo xtask planted
//! header-sync/hold-release-wakes` checks that its bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf, process::Command, time::Duration};

use support::{byte_inputs, inputs};
use zakura_sim::harness::header_acceptance::history::run_declared;
use zakura_sim::NaReason;
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::{
        hold_release_wakes, CHUNK_BUDGET_BOUND, COMMIT_ONLY_AUTHORIZED, HOLD_RELEASE_WAKES,
        RESET_INVALIDATES_WORK, STALE_CHAIN_EFFECT_INERT, STALE_COMPLETION_INERT,
        TEARDOWN_RETURNS_BASELINE,
    },
    harness::{
        header_node::{
            ends::{EndFact, EnvironmentAction},
            hold_wake::HoldWakeFact,
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Policy,
        },
        header_sync::{
            liveness::{self, HeldKind, LivenessScenario, RecoveryFixture, LIVENESS_BUDGET},
            storage_config, storage_properties, storage_sources, Liveness, Ports, Storage,
            StorageScenario, StorageView,
        },
        run_prepared, Subsystem,
    },
    observe::{ReactorFact, StorageFact, StorageResult},
    property::{Assume, Assumptions, EndViolation, HasView, Property, Sources},
    sim::NO_LIVELOCK,
    MemorySink, Observation, ObservationLog, Outcome, PlanInput, RunConfig, RunEnd, RunReport, Sim,
};

/// What a run's hold-wake and end evidence kept: each hold-wake fact with
/// its stamp, and the stamp of the harness's `Shutdown` step, if any.
struct Reading {
    wakes: Vec<(u64, HoldWakeFact)>,
    shutdown: Option<u64>,
}

/// Runs `fixture` under `input`, which must end idle, after its finish,
/// never at its step budget (Rule 13), and without a violation or harness
/// failure.
fn run(fixture: LivenessScenario, input: PlanInput) -> RunReport {
    run_reading(fixture, input).0
}

/// [`run`], with what [`Reading`] keeps.
fn run_reading(fixture: LivenessScenario, input: PlanInput) -> (RunReport, Reading) {
    run_reading_into(fixture, input, &mut MemorySink::default())
}

/// [`run_reading`], recording into `sink`.
fn run_reading_into(
    fixture: LivenessScenario,
    input: PlanInput,
    sink: &mut dyn zakura_sim::RecordSink,
) -> (RunReport, Reading) {
    let ports = Liveness::prepare(&fixture);
    let (hold_wake, ends) = (ports.hold_wake.clone(), ports.ends.clone());
    let report = run_prepared::<Liveness>(&fixture, ports, input, sink, None)
        .unwrap_or_else(|error| panic!("{fixture:?}: {error}"));
    assert!(
        matches!(report.end.outcome, Outcome::Idle { .. }),
        "{fixture:?}: {:?}",
        report.end
    );
    assert_eq!(report.end.violation, None, "{fixture:?}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{fixture:?}: {:?}", report.end);
    let shutdown = ends.facts().into_iter().find_map(|(stamp, fact)| {
        matches!(
            fact,
            EndFact::Environment {
                action: EnvironmentAction::Shutdown,
                ..
            }
        )
        .then_some(stamp)
    });
    let reading = Reading {
        wakes: hold_wake.facts(),
        shutdown,
    };
    (report, reading)
}

/// `property`'s outcome in `report`.
fn outcome<'r>(report: &'r RunReport, property: &str) -> &'r PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|reported| reported.property == property)
        .unwrap_or_else(|| panic!("{property} reports"))
        .outcome
}

/// The regression controls every fixture runs beside the property.
const CONTROLS: [&str; 6] = [
    CHUNK_BUDGET_BOUND,
    TEARDOWN_RETURNS_BASELINE,
    STALE_COMPLETION_INERT,
    RESET_INVALIDATES_WORK,
    STALE_CHAIN_EFFECT_INERT,
    COMMIT_ONLY_AUTHORIZED,
];

/// Asserts that `report` holds `header-sync/hold-release-wakes` and applies,
/// and that the controls hold or don't apply beside it.
fn assert_holds_and_applies(fixture: LivenessScenario, report: &RunReport) {
    assert!(
        matches!(
            outcome(report, HOLD_RELEASE_WAKES),
            PropertyOutcome::Held { applicable } if *applicable > 0
        ),
        "{fixture:?}: {:?}",
        outcome(report, HOLD_RELEASE_WAKES)
    );
    for control in CONTROLS {
        assert!(
            matches!(
                outcome(report, control),
                PropertyOutcome::Held { .. }
                    | PropertyOutcome::NotApplicable {
                        reason: NaReason::NeverApplied
                    }
            ),
            "{fixture:?}: {control}: {:?}",
            outcome(report, control)
        );
    }
}

/// One of the node's logged facts.
enum Fact<'r> {
    Storage(&'r StorageFact),
    Reactor(&'r ReactorFact),
}

/// The node's storage facts and reactor facts, in log order.
fn facts(report: &RunReport) -> Vec<Fact<'_>> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0, fact, ..
            } => Some(Fact::Storage(fact)),
            Observation::Reactor { event, .. } => Some(Fact::Reactor(event)),
            _ => None,
        })
        .collect()
}

/// Each fixture holds `header-sync/hold-release-wakes` and applies, with
/// the controls holding or not applying beside it, and takes the path it
/// names: one held reply reaches the
/// reactor's call, and the reactor handles that call after the reply. A
/// serving fixture's held reply comes after a page read completed. A
/// reply-held apply commits first. Every run stops the node after the
/// reply. Only the shutdown fixture's stop can come before the reactor
/// handles it, and under some plan does, so the shutdown, not the handling,
/// ends its obligation. Every other run's stop is its finish, after the
/// handling.
#[test]
fn every_fixture_holds_and_takes_its_path() {
    let mut shut_first = 0;
    for fixture in LivenessScenario::holds() {
        let shutdown_fixture = fixture == LivenessScenario::ShutdownAfterRelease;
        let plans = if shutdown_fixture { 32 } else { 6 };
        for input in inputs(plans, 256) {
            let (report, reading) = run_reading(fixture, input);
            assert_holds_and_applies(fixture, &report);
            let wakes: Vec<HoldWakeFact> =
                reading.wakes.iter().map(|(_, fact)| fact.clone()).collect();
            let [HoldWakeFact::Opened { call, op, .. }, rest @ ..] = &wakes[..] else {
                panic!("{fixture:?}: one held reply opens first: {wakes:?}");
            };
            // Every run stops the node, after the reply: by the shutdown
            // fixture's own step, or else by the finish, which waits for
            // the handling.
            let opened = reading.wakes[0].0;
            let shutdown = reading.shutdown.expect("every run stops the node");
            assert!(shutdown > opened, "{fixture:?}");
            let handled_stamp = reading.wakes.get(1).map(|(stamp, _)| *stamp);
            let shut_before_handled = handled_stamp.is_none_or(|handled| shutdown < handled);
            assert!(
                shutdown_fixture || !shut_before_handled,
                "{fixture:?}: the finish came before the handling"
            );
            if shut_before_handled {
                shut_first += 1;
            }
            let logged = facts(&report);
            let delivered = logged
                .iter()
                .position(|fact| {
                    matches!(fact, Fact::Storage(StorageFact::Completed { id, delivered: true, .. })
                        if id == op)
                })
                .expect("the held reply is delivered");
            let handled = logged.iter().position(|fact| {
                matches!(fact, Fact::Reactor(ReactorFact::Handled { call: handled, .. })
                    if handled == call)
            });
            match (rest, handled) {
                ([HoldWakeFact::Handled { call: c, op: o }], Some(handled)) => {
                    assert_eq!((c, o), (call, op), "{fixture:?}");
                    assert!(handled > delivered, "{fixture:?}: handled before delivery");
                }
                ([], None) => assert!(shutdown_fixture, "{fixture:?}: {wakes:?}"),
                _ => panic!("{fixture:?}: {wakes:?}, handled at {handled:?}"),
            }
            if let LivenessScenario::HeldWhileServing(_) = fixture {
                let read = logged.iter().position(|fact| {
                    matches!(
                        fact,
                        Fact::Storage(StorageFact::Completed {
                            result: StorageResult::Page { .. },
                            ..
                        })
                    )
                });
                assert!(
                    read.is_some_and(|read| read < delivered),
                    "{fixture:?}: a page read completes before the held reply"
                );
            }
            if fixture == LivenessScenario::ReplyHeld {
                assert!(
                    logged[..delivered].iter().any(|fact| matches!(fact,
                        Fact::Storage(StorageFact::Committed { id, .. }) if id == op)),
                    "the reply-held apply commits before its reply"
                );
            }
        }
    }
    assert!(
        shut_first > 0,
        "no plan began the shutdown before the reactor handled the reply"
    );
}

/// The property's report and the full log depend only on the fixture and
/// the plan: the held-prepare fixture under plan A twice, then B, then A
/// again, gives A's every time, and B's log differs.
#[test]
fn the_hold_fixture_repeats_a_a_and_a_b_a() {
    let fixture = LivenessScenario::Held(HeldKind::Prepare);
    let plans = [committed_plan(), vec![7; 256]];
    let runs: Vec<(String, String)> = [&plans[0], &plans[0], &plans[1], &plans[0]]
        .into_iter()
        .map(|plan| {
            let report = run(fixture, zakura_sim::Plan::from_fuzz(plan));
            (
                format!("{:?}", report.log.entries()),
                format!("{:?}", outcome(&report, HOLD_RELEASE_WAKES)),
            )
        })
        .collect();
    assert_eq!(runs[0], runs[1], "plan A, then A");
    assert_eq!(runs[0], runs[3], "plan A, then B, then A");
    assert_ne!(runs[0].0, runs[2].0, "plan B's log differs");
}

/// The fixtures `cargo xtask planted` runs as typed cases, each committed
/// as `tests/fixtures/header-hold-<name>.json` with its Plan bytes beside
/// it in `header-hold-<name>.plan`.
fn committed_fixtures() -> [(&'static str, LivenessScenario); 4] {
    [
        ("held-locator", LivenessScenario::Held(HeldKind::Locator)),
        ("held-prepare", LivenessScenario::Held(HeldKind::Prepare)),
        ("held-apply", LivenessScenario::Held(HeldKind::Apply)),
        ("serving-read", LivenessScenario::ServingHeld),
    ]
}

/// The Plan bytes every committed fixture runs under: a fixed draw.
fn committed_plan() -> Vec<u8> {
    byte_inputs(2, 256).remove(1)
}

/// The committed fixture file `header-hold-<name>.<extension>`.
fn fixture_file(name: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("header-hold-{name}.{extension}"))
}

/// Each committed fixture is its named fixture, as JSON, with the fixed
/// Plan bytes, and holds and applies under them. Each also holds and
/// applies `sim/no-livelock`: every liveness fixture declares
/// `BoundedAdversary`, so the node waits out its held call without
/// spinning at one instant, and the held-locator run is that ID's counted
/// header history.
#[test]
fn the_committed_fixtures_match_their_names() {
    for (name, fixture) in committed_fixtures() {
        let committed: LivenessScenario = serde_json::from_str(
            &fs::read_to_string(fixture_file(name, "json"))
                .unwrap_or_else(|error| panic!("{name}: {error}")),
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(committed, fixture, "{name}: run `regenerate_hold_fixtures`");
        let plan =
            fs::read(fixture_file(name, "plan")).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            plan,
            committed_plan(),
            "{name}: run `regenerate_hold_fixtures`"
        );
        // Each run is a manifest history, which the acceptance tool
        // records from this declaration (C18).
        let report = run_declared(
            Liveness::NAME,
            &format!("header-hold-{name}"),
            &fixture,
            zakura_sim::Plan::from_fuzz(&plan),
            |fixture, input, sink| run_reading_into(*fixture, input, sink).0,
        );
        assert!(
            matches!(
                outcome(&report, HOLD_RELEASE_WAKES),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{name}"
        );
        assert!(
            matches!(
                outcome(&report, NO_LIVELOCK),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{name}: {:?}",
            outcome(&report, NO_LIVELOCK)
        );
    }
}

/// Rewrites the committed fixtures from their names.
#[test]
#[ignore = "rewrites the committed hold fixtures"]
fn regenerate_hold_fixtures() {
    for (name, fixture) in committed_fixtures() {
        let json = serde_json::to_string_pretty(&fixture).expect("fixtures serialize");
        fs::write(fixture_file(name, "json"), json + "\n").expect("the fixture directory exists");
        fs::write(fixture_file(name, "plan"), committed_plan())
            .expect("the fixture directory exists");
    }
}

/// `record --witness` judges each committed fixture as `cargo xtask
/// planted` runs it, expecting a hold, and `replay` reruns the trace Exact
/// in a fresh process.
#[test]
fn the_committed_fixtures_record_and_replay_in_a_fresh_process() {
    let zakura_sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_zakura-sim"))
            .args(args)
            .output()
            .expect("the binary runs")
    };
    for (name, _) in committed_fixtures() {
        let dir = tempfile::TempDir::new().expect("a temporary directory");
        let trace = dir.path().join("trace");
        let scenario =
            fs::read_to_string(fixture_file(name, "json")).expect("the fixture is committed");
        let recorded = zakura_sim(&[
            "record",
            "--sim",
            Liveness::NAME,
            "--scenario",
            &scenario,
            "--plan",
            fixture_file(name, "plan")
                .to_str()
                .expect("the path is UTF-8"),
            "--out",
            trace.to_str().expect("the path is UTF-8"),
            "--witness",
            HOLD_RELEASE_WAKES,
            "--expect",
            "holds",
        ]);
        assert!(recorded.status.success(), "{name}: {recorded:?}");
        let witness: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(trace.join(zakura_sim::harness::WITNESS_FILE))
                .expect("witness.json"),
        )
        .expect("witness.json is JSON");
        assert_eq!(
            witness["passed"],
            serde_json::Value::Bool(true),
            "{name}: {witness}"
        );
        let end: serde_json::Value = fs::read_to_string(trace.join("trace.jsonl"))
            .expect("the trace is written")
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find_map(|line| line.get("end").cloned())
            .expect("the trace ends");
        assert_eq!(end["outcome"], "idle", "{name}: {end}");
        let replayed = zakura_sim(&["replay", trace.to_str().expect("the path is UTF-8")]);
        let stdout = String::from_utf8_lossy(&replayed.stdout);
        assert!(
            replayed.status.success()
                && stdout.contains("every pick, draw and observation matched"),
            "{name}: {replayed:?}"
        );
    }
}

/// A storage scenario outside `header-sync/liveness`'s named fixtures, on
/// its hold-wake evidence and properties, with `header-sync/storage`'s
/// config and no assumptions. For controls only.
struct Probe(Storage);

impl HasView for Probe {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The inner harness's stop.
    fn stopping(&self) -> bool {
        self.0.stopping()
    }

    /// The storage harness's stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Probe {
    const NAME: &'static str = "test/hold-release-probe";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = StorageScenario;
    type Prepared = Ports;

    /// [`storage_config`].
    fn config(_: &StorageScenario) -> RunConfig {
        storage_config()
    }

    /// The storage harness's ports, with the hold-wake evidence armed.
    fn prepare(scenario: &StorageScenario) -> Ports {
        let ports = Storage::prepare(scenario);
        ports.hold_wake.arm();
        ports
    }

    /// The storage harness.
    fn build(sim: &mut Sim, scenario: &StorageScenario, ports: Ports) -> Self {
        Probe(Storage::build(sim, scenario, ports))
    }

    /// [`Liveness`]'s, in its order.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(hold_release_wakes());
        properties
    }

    /// [`Liveness`]'s.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Never drawn: probe runs are typed.
    fn draw(_: &[u8]) -> StorageScenario {
        unreachable!("probe runs are typed")
    }
}

/// Runs `storage` on [`Probe`] under `input`.
fn probe(storage: &StorageScenario, input: PlanInput) -> RunReport {
    run_prepared::<Probe>(
        storage,
        Probe::prepare(storage),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"))
}

/// `header-sync/liveness` with its own livelock limit in place of the one
/// `BoundedAdversary` sets, and no assumptions, so a test can measure a
/// fixture's honest runs against a lower limit.
struct Limited(Liveness);

impl HasView for Limited {
    type View = StorageView;

    /// The liveness harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The liveness harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The liveness harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The inner harness's stop.
    fn stopping(&self) -> bool {
        self.0.stopping()
    }

    /// The liveness harness's stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Limited {
    const NAME: &'static str = "test/hold-release-limited";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    /// The fixture, and the steps at one instant past which the run
    /// violates `sim/no-livelock`.
    type Scenario = (LivenessScenario, u64);
    type Prepared = Ports;

    /// [`Liveness`]'s, with the scenario's livelock limit.
    fn config((fixture, limit): &(LivenessScenario, u64)) -> RunConfig {
        RunConfig {
            enforce_no_livelock: true,
            max_steps_per_instant: *limit,
            ..Liveness::config(fixture)
        }
    }

    /// [`Liveness`]'s.
    fn prepare((fixture, _): &(LivenessScenario, u64)) -> Ports {
        Liveness::prepare(fixture)
    }

    /// The liveness harness, finish included.
    fn build(sim: &mut Sim, (fixture, _): &(LivenessScenario, u64), ports: Ports) -> Self {
        Limited(Liveness::build(sim, fixture, ports))
    }

    /// [`Liveness`]'s, in its order.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(hold_release_wakes());
        properties
    }

    /// [`Liveness`]'s.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Never drawn: limited runs are typed.
    fn draw(_: &[u8]) -> (LivenessScenario, u64) {
        unreachable!("limited runs are typed")
    }
}

/// Every fixture's honest runs take at most half the livelock limit at one
/// instant, so the limit has room for plans these don't sample. Each run
/// ends idle at its finish.
#[test]
fn every_fixture_stays_under_half_the_livelock_limit() {
    let half = LIVENESS_BUDGET / 10 / 2;
    for fixture in LivenessScenario::all() {
        for input in inputs(16, 256) {
            let scenario = (fixture, half);
            let report = run_prepared::<Limited>(
                &scenario,
                Limited::prepare(&scenario),
                input,
                &mut MemorySink::default(),
                None,
            )
            .unwrap_or_else(|error| panic!("{error}"));
            assert_eq!(report.end.violation, None, "{fixture:?}: {:?}", report.end);
            assert!(
                matches!(report.end.outcome, Outcome::Idle { .. }),
                "{fixture:?}: {:?}",
                report.end
            );
        }
    }
}

/// A run with no hold, and one whose only hold picks an operation the
/// reactor never submits, open nothing, so the property doesn't apply.
#[test]
fn a_run_without_a_held_reply_doesnt_apply() {
    let mut unpicked = LivenessScenario::Held(HeldKind::Locator).storage();
    unpicked.policy = Policy {
        holds: vec![Hold {
            select: Select::nth(OpKind::VctContext, 1),
            until: Trigger::Released,
            then: vec![Allow::Engine],
            deadline_ms: None,
        }],
        ..Policy::converging()
    };
    unpicked.controls.clear();
    for storage in [StorageScenario::default(), unpicked] {
        for input in inputs(4, 256) {
            let report = probe(&storage, input);
            assert_eq!(report.end.violation, None, "{:?}", report.end);
            assert_eq!(
                outcome(&report, HOLD_RELEASE_WAKES),
                &PropertyOutcome::not_applicable(NaReason::NeverApplied)
            );
        }
    }
}

thread_local! {
    /// Whether the run's spinning task began spinning with a held reply
    /// delivered and its call unhandled.
    static SPUN_OPEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// `header-sync/liveness`'s evidence, properties, budget and assumptions on
/// a storage scenario, with one more task: it sleeps until one virtual
/// second, then waits there for a held reply to reach the reactor, and
/// spins at that instant from then on. It stands in for a reactor that
/// keeps the clock still with an obligation open.
struct Spinning(Storage);

impl HasView for Spinning {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The inner harness's stop.
    fn stopping(&self) -> bool {
        self.0.stopping()
    }

    /// The storage harness's stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Spinning {
    const NAME: &'static str = "test/hold-release-spinning";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = StorageScenario;
    type Prepared = Ports;

    /// [`Liveness`]'s budget.
    fn config(_: &StorageScenario) -> RunConfig {
        RunConfig {
            budget: LIVENESS_BUDGET,
            ..storage_config()
        }
    }

    /// The storage harness's ports, with the hold-wake evidence armed.
    fn prepare(scenario: &StorageScenario) -> Ports {
        let ports = Storage::prepare(scenario);
        ports.hold_wake.arm();
        ports
    }

    /// The storage harness, then the spinning task.
    fn build(sim: &mut Sim, scenario: &StorageScenario, ports: Ports) -> Self {
        let hold_wake = ports.hold_wake.clone();
        let storage = Storage::build(sim, scenario, ports);
        SPUN_OPEN.with(|spun| spun.set(false));
        sim.scheduler().spawn_root("spin", async move {
            zakura_rt::time::sleep(Duration::from_secs(1)).await;
            loop {
                let facts = hold_wake.facts();
                let opened = facts.iter().find_map(|(_, fact)| match fact {
                    HoldWakeFact::Opened { call, .. } => Some(*call),
                    _ => None,
                });
                if let Some(call) = opened {
                    let handled = facts.iter().any(|(_, fact)| {
                        matches!(fact, HoldWakeFact::Handled { call: handled, .. } if *handled == call)
                    });
                    SPUN_OPEN.with(|spun| spun.set(!handled));
                    break;
                }
                zakura_rt::task::yield_now().await;
            }
            loop {
                zakura_rt::task::yield_now().await;
            }
        });
        Spinning(storage)
    }

    /// [`Liveness`]'s, in its order.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(hold_release_wakes());
        properties
    }

    /// [`Liveness`]'s.
    fn sources() -> Sources {
        storage_sources()
    }

    /// [`Liveness`]'s.
    fn assumptions(_: &StorageScenario) -> Assumptions<Self> {
        liveness::assumptions()
    }

    /// Never drawn: spinning runs are typed.
    fn draw(_: &[u8]) -> StorageScenario {
        unreachable!("spinning runs are typed")
    }
}

/// `header-sync/liveness` declares `BoundedAdversary`, so a task that spins
/// at one instant violates `sim/no-livelock` without a panic, instead of
/// ending the run at its step budget. The held-locator fixture's release
/// waits for one virtual second here, and the spin begins then, so under
/// some plan it begins with the held reply delivered and unhandled: an
/// obligation open, as a spinning reactor's would be. An honest reactor
/// still handles the reply between the spinner's steps, so the obligation
/// can close before the limit trips. `header-sync/storage` declares
/// nothing.
#[test]
fn a_spin_with_a_held_reply_open_violates_no_livelock() {
    let fixture = LivenessScenario::Held(HeldKind::Locator);
    assert!(Liveness::assumptions(&fixture).declares(Assume::BoundedAdversary));
    assert!(Storage::assumptions(&fixture.storage())
        .records()
        .is_empty());
    let mut scenario = fixture.storage();
    scenario.controls[0].when = Some(Trigger::All(vec![
        Trigger::Submitted(Select::nth(OpKind::Locator, 1)),
        Trigger::VirtualTime(1_000),
    ]));
    let mut spun_open = 0;
    for input in inputs(16, 256) {
        let report = run_prepared::<Spinning>(
            &scenario,
            Spinning::prepare(&scenario),
            input,
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let violation = report.end.violation.as_ref().expect("the spin violates");
        assert_eq!(violation.property, NO_LIVELOCK, "{:?}", report.end);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        assert!(
            report.log.entries().iter().all(|entry| !matches!(
                &entry.obs,
                Observation::TaskExited {
                    exit: zakura_sim::observe::Exit::Panicked,
                    ..
                }
            )),
            "nothing panics"
        );
        if SPUN_OPEN.with(std::cell::Cell::get) {
            spun_open += 1;
        }
    }
    assert!(
        spun_open > 0,
        "no plan began the spin with the held reply unhandled"
    );
}

/// The scenario byte picks each fixture in [`LivenessScenario::all`]'s
/// order, wrapping.
#[test]
fn the_scenario_byte_picks_each_fixture() {
    let all = LivenessScenario::all();
    for (index, fixture) in all.iter().enumerate() {
        let byte = u8::try_from(index).expect("few fixtures");
        assert_eq!(Liveness::draw(&[byte]), *fixture);
        let wrapped = u8::try_from(index + all.len()).expect("few fixtures");
        assert_eq!(Liveness::draw(&[wrapped]), *fixture);
    }
    // A change to the fixture list moves this mapping, so it bumps the
    // generator, which corpus carry checks.
    assert_eq!(
        (Liveness::GENERATOR, all.len()),
        (4, 42),
        "a new fixture list needs a new generator"
    );
    assert_eq!(Liveness::draw(&[30]), LivenessScenario::QuietReset);
    assert_eq!(
        Liveness::draw(&[39]),
        LivenessScenario::Recovery(RecoveryFixture::Closes)
    );
}

/// The serving-read fixture holds the property and applies, with the
/// controls holding or not applying beside it, and takes its path: the
/// publisher's held page read for the mirror's session is served, a page
/// read that completes with a page, and that session's publisher then
/// queues a page, which answers it. The finish stops the node after the
/// page.
#[test]
fn the_serving_fixture_holds_and_takes_its_path() {
    let fixture = LivenessScenario::ServingHeld;
    for input in inputs(16, 256) {
        let (report, reading) = run_reading(fixture, input);
        assert_holds_and_applies(fixture, &report);
        let wakes: Vec<HoldWakeFact> = reading.wakes.iter().map(|(_, fact)| fact.clone()).collect();
        let [HoldWakeFact::Served {
            op,
            peer,
            session_id,
            ..
        }, HoldWakeFact::Queued {
            op: queued,
            session_id: session,
        }] = &wakes[..]
        else {
            panic!("one served read, then its page: {wakes:?}");
        };
        assert_eq!((queued, session), (op, session_id), "{wakes:?}");
        assert_eq!(peer, "mirror-0");
        let shutdown = reading.shutdown.expect("every run stops the node");
        assert!(
            shutdown > reading.wakes[1].0,
            "the finish came before the page"
        );
        let served = facts(&report).iter().any(|fact| {
            matches!(fact, Fact::Storage(StorageFact::Completed {
                id,
                delivered: true,
                result: StorageResult::Page { .. },
                ..
            }) if id == op)
        });
        assert!(served, "the held read completes with a page");
    }
}

/// The serving fixture's passing controls open nothing, so the property
/// doesn't apply: a held page read that fails, which the publisher may
/// stall on, and a mirror level with the node, for which the publisher
/// reads no page.
#[test]
fn the_serving_controls_open_nothing() {
    for fixture in [
        LivenessScenario::ServingFailed,
        LivenessScenario::ServingCaughtUp,
    ] {
        for input in inputs(6, 256) {
            let (report, reading) = run_reading(fixture, input);
            assert!(reading.wakes.is_empty(), "{fixture:?}: {:?}", reading.wakes);
            assert!(
                matches!(
                    outcome(&report, HOLD_RELEASE_WAKES),
                    PropertyOutcome::NotApplicable {
                        reason: NaReason::NeverApplied
                    }
                ),
                "{fixture:?}: {:?}",
                outcome(&report, HOLD_RELEASE_WAKES)
            );
            for control in CONTROLS {
                assert!(
                    matches!(
                        outcome(&report, control),
                        PropertyOutcome::Held { .. }
                            | PropertyOutcome::NotApplicable {
                                reason: NaReason::NeverApplied
                            }
                    ),
                    "{fixture:?}: {control}: {:?}",
                    outcome(&report, control)
                );
            }
            let failed_read = facts(&report).iter().any(|fact| {
                matches!(
                    fact,
                    Fact::Storage(StorageFact::Completed {
                        result: StorageResult::Fault { .. },
                        ..
                    })
                )
            });
            assert_eq!(
                failed_read,
                fixture == LivenessScenario::ServingFailed,
                "{fixture:?}: only the failing control's held read fails"
            );
        }
    }
}
