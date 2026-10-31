//! T1d: the fuzz loop's fast run skips step records, and `record_failure`
//! saves the failure it found only when a recording rerun and an Exact
//! replay both agree with it (traces.md, Trace directory).
#![cfg(zakura_sim)]

mod support;

use std::{
    cell::{Cell, RefCell},
    path::Path,
    sync::{mpsc, OnceLock},
    thread,
    time::Instant,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tempfile::TempDir;
use zakura_rt::sync::atomic::{AtomicU64, Ordering};
use zakura_scheduler::{SchedulerConfig, SimInstant};
use zakura_sim::{
    action::Boundary,
    explore::{input_path, save_failure, Bucket, ExploreRun},
    harness::{self, entry, HarnessEntry, RecordError, Subsystem},
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Verdict},
    record::{EndRecord, OutcomeKind},
    trace::{
        record_failure, Trace, TraceReader, DIVERGENCE_FILE, FIRST_END_FILE, INPUT_FILE,
        NONDETERMINISTIC_DIR, PROPERTIES_FILE, RECORDING_DIR, REPLAY_DIR, TRACES_DIR,
    },
    ActionSource, Identity, MemorySink, NullSink, Observation, ObservationLog, Plan,
    PropertyOutcome, Record, RecordSink, RunConfig, RunReport, Sim,
};

/// The fast run of the full fuzz `input` on `harness`, as the fuzz loop
/// runs it, with the scenario it drew.
fn fast_run(harness: &HarnessEntry, input: &[u8]) -> (Value, RunReport) {
    let split = harness.scenario_bytes.min(input.len());
    let mut prefix = input[..split].to_vec();
    prefix.resize(harness.scenario_bytes, 0);
    let scenario = (harness.draw)(&prefix);
    let report = (harness.run)(&scenario, Plan::from_fuzz(&input[split..]), &mut NullSink)
        .unwrap_or_else(|error| panic!("{}: {error}", harness.name));
    (scenario, report)
}

/// The directory `record_failure` saves `property`'s traces under.
fn property_dir(root: &Path, property: &str) -> std::path::PathBuf {
    property
        .split('/')
        .fold(root.join(TRACES_DIR), |dir, segment| dir.join(segment))
}

/// The names in `dir`, sorted.
fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// `property`'s report in the saved trace's `end` record.
fn end_outcome<'t>(trace: &'t Trace, property: &str) -> &'t PropertyOutcome {
    &trace
        .end
        .as_ref()
        .expect("a saved trace is complete")
        .properties
        .iter()
        .find(|report| report.property == property)
        .expect("the trace reports the property")
        .outcome
}

/// The step of `property`'s first violation in `report`.
fn first_violation(report: &RunReport, property: &str) -> u64 {
    report
        .properties
        .iter()
        .find(|report| report.property == property)
        .and_then(|report| report.violations.first())
        .expect("the run violated the property")
        .step
}

/// A sink that keeps records and wants no steps, as [`NullSink`] does.
#[derive(Default)]
struct NoSteps(Vec<Record>);

impl RecordSink for NoSteps {
    fn record(&mut self, record: Record) {
        self.0.push(record);
    }

    fn wants_steps(&self) -> bool {
        false
    }
}

#[test]
fn a_sink_without_steps_gets_init_and_end() {
    let race = harness::find("race/lost-update").unwrap();
    let scenario = (race.draw)(&[0, 1]);
    let mut full = MemorySink::default();
    (race.run)(&scenario, Plan::from_fuzz(&[3, 1, 4, 1, 5]), &mut full).unwrap();
    let mut fast = NoSteps::default();
    (race.run)(&scenario, Plan::from_fuzz(&[3, 1, 4, 1, 5]), &mut fast).unwrap();
    assert!(full.records.len() > 2, "the full run has steps");
    assert!(matches!(fast.0[..], [Record::Init(_), Record::End(_)]));
    assert_eq!(fast.0[0], full.records[0], "the same init record");
    assert_eq!(
        fast.0[1],
        *full.records.last().unwrap(),
        "the same end record"
    );
}

#[test]
fn record_failure_writes_the_same_violation() {
    const PROPERTY: &str = "race/lost-update";
    let race = harness::find(PROPERTY).unwrap();
    // The planted race on two workers, then Plan bytes.
    let (input, scenario, first) = support::byte_inputs(500, 64)
        .into_iter()
        .map(|plan| [&[0, 1][..], &plan].concat())
        .find_map(|input| {
            let (scenario, first) = fast_run(race, &input);
            first
                .end
                .violation
                .is_some()
                .then_some((input, scenario, first))
        })
        .expect("some input finds the planted race");
    let root = TempDir::new().unwrap();
    let id = PropertyId::new(PROPERTY);
    let trace = record_failure(race, &scenario, &input, &id, &first, root.path()).unwrap();

    let end = trace.end.as_ref().unwrap();
    assert_eq!(end.outcome, OutcomeKind::Violation);
    let violation = end.violation.as_ref().unwrap();
    assert_eq!(violation.property, PROPERTY);
    assert_eq!(
        violation.step,
        first.end.violation.as_ref().unwrap().step,
        "the trace violates the property at the fast run's step"
    );
    assert!(matches!(
        end_outcome(&trace, PROPERTY),
        PropertyOutcome::Violated { .. }
    ));
    assert_eq!(trace.steps.len() as u64, end.steps, "every step recorded");

    // Saved under the property's ID, named by the trace's ID, with the
    // full input, and nothing partial left beside it.
    let parent = property_dir(root.path(), PROPERTY);
    assert_eq!(trace.dir, parent.join(&trace.id[..12]));
    assert_eq!(entries(&parent), [trace.id[..12].to_owned()]);
    assert_eq!(std::fs::read(trace.dir.join(INPUT_FILE)).unwrap(), input);

    // The same failure again is the same trace, saved once.
    let again = record_failure(race, &scenario, &input, &id, &first, root.path()).unwrap();
    assert_eq!((&again.dir, &again.id), (&trace.dir, &trace.id));
    assert_eq!(entries(&parent).len(), 1);
}

#[test]
fn record_failure_refuses_a_run_without_the_violation() {
    let race = harness::find("race/lost-update").unwrap();
    // The fix never fails.
    let input = [1, 1, 3, 1, 4];
    let (scenario, first) = fast_run(race, &input);
    let root = TempDir::new().unwrap();
    let id = PropertyId::new("race/lost-update");
    match record_failure(race, &scenario, &input, &id, &first, root.path()) {
        Err(RecordError::NotViolated { property }) => assert_eq!(property, "race/lost-update"),
        other => panic!("expected NotViolated, got {other:?}"),
    }
}

/// The host clock, as nanoseconds since this process first read it.
fn host_nanos() -> u128 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos()
}

/// A [`Ticks`] run's scenario.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TickScenario {
    /// How many ticks the source offers.
    ticks: u32,
    /// Whether tick 2's observation reads the host clock: a planted
    /// nondeterminism that changes an observation and nothing else.
    stamp: bool,
    /// Whether tick 2's observation says if [`PARITY_RUNS`] was even: a
    /// planted nondeterminism that only every other run shows.
    parity: bool,
}

/// Runs that observed tick 2 of a `parity` scenario.
static PARITY_RUNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A source that offers `ticks` ticks, one per step, and observes each.
struct Clock {
    next: Cell<u32>,
    scenario: TickScenario,
}

impl ActionSource for Clock {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let n = self.next.get();
        if n < self.scenario.ticks {
            out.push((Identity::peer("clock", &format!("tick{n}")), 0));
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        let n = self.next.get();
        self.next.set(n + 1);
        let event = match n {
            2 if self.scenario.stamp => format!("tick 2 at {}", host_nanos()),
            2 if self.scenario.parity => {
                let runs = PARITY_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                format!("tick 2 after {} runs", runs % 2)
            }
            _ => format!("tick {n}"),
        };
        log.push(Observation::Fake {
            source: "clock".into(),
            event,
        });
    }
}

/// The [`Clock`] source alone, with two properties that tick `n` breaks.
struct Ticks;

impl HasView for Ticks {
    type View = ();

    /// Nothing: the checks read observations.
    fn view(&self) {}
}

/// Violated at the step that observes tick `N`.
fn no_tick<const N: u32>(cx: &mut CheckCtx<'_, Ticks>) -> Verdict {
    let tick = format!("tick {N}");
    let seen = cx.observations().any(|obs| {
        matches!(obs, Observation::Fake { source, event }
            if source == "clock" && (*event == tick || event.starts_with(&format!("{tick} "))))
    });
    if seen {
        cx.violated(format!("no tick {N}"), format!("tick {N} observed"))
    } else {
        Verdict::Holds
    }
}

/// A `Known` property that tick 1 violates, without ending the run.
const KNOWN: &str = "test/ticks/known-tick-one";

/// An `Active` property that tick 3 violates, ending the run.
const ACTIVE: &str = "test/ticks/active-tick-three";

impl Subsystem for Ticks {
    const NAME: &'static str = "test/ticks";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = TickScenario;
    type Prepared = ();

    /// Room for every tick.
    fn config(_: &TickScenario) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 1,
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            budget: 100,
            drain_bound: 10,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &TickScenario) {}

    /// Adds the clock.
    fn build(sim: &mut Sim, scenario: &TickScenario, (): ()) -> Self {
        sim.add_source(Box::new(Clock {
            next: Cell::new(0),
            scenario: scenario.clone(),
        }));
        Ticks
    }

    /// The two tick properties.
    fn properties() -> Vec<Property<Self>> {
        vec![
            Property {
                id: PropertyId::new(KNOWN),
                claim: "no tick 1",
                kind: Kind::Safety,
                assumes: &[],
                needs: &[Source::Schedule],
                status: Status::Known { issue: 1 },
                check: no_tick::<1>,
                witness: None,
            },
            Property {
                id: PropertyId::new(ACTIVE),
                claim: "no tick 3",
                kind: Kind::Safety,
                assumes: &[],
                needs: &[Source::Schedule],
                status: Status::Active,
                check: no_tick::<3>,
                witness: None,
            },
        ]
    }

    /// Five unstamped ticks.
    fn draw(_: &[u8]) -> TickScenario {
        TickScenario {
            ticks: 5,
            stamp: false,
            parity: false,
        }
    }
}

/// The [`Ticks`] harness.
static TICKS: HarnessEntry = entry::<Ticks>();

/// The fast run of a [`Ticks`] scenario.
fn ticks_run(scenario: &TickScenario) -> (&'static HarnessEntry, Value, RunReport) {
    let scenario = serde_json::to_value(scenario).unwrap();
    let report = (TICKS.run)(&scenario, Plan::from_fuzz(&[]), &mut NullSink).unwrap();
    (&TICKS, scenario, report)
}

#[test]
fn record_failure_catches_a_planted_nondeterminism() {
    let (ticks, scenario, first) = ticks_run(&TickScenario {
        ticks: 5,
        stamp: true,
        parity: false,
    });
    assert_eq!(first.end.violation.as_ref().unwrap().property, ACTIVE);
    assert_eq!(first_violation(&first, ACTIVE), 4);
    let root = TempDir::new().unwrap();
    // The first rerun violates the property at the same step, so only the
    // replay can catch the stamped observation.
    match record_failure(
        ticks,
        &scenario,
        &[],
        &PropertyId::new(ACTIVE),
        &first,
        root.path(),
    ) {
        Err(RecordError::Nondeterministic { detail, evidence }) => {
            assert!(
                detail.contains("step 3 recorded a different obs"),
                "{detail}"
            );
            let evidence = evidence.expect("record_failure keeps the evidence");
            let divergence = divergence(root.path(), &evidence);
            assert_eq!(divergence["check"], "replay");
            // The replay stamps another time at the same step.
            assert_eq!(
                divergence["replay_difference"],
                json!({ "step": 3, "field": "obs" })
            );
            // The first run and the recording end alike.
            assert_eq!(divergence["end_difference"], Value::Null);
            let first = first_end(&evidence);
            let violation = first.violation.expect("the first run violated ACTIVE");
            assert_eq!((violation.property.as_str(), violation.step), (ACTIVE, 4));
        }
        other => panic!("expected Nondeterministic, got {other:?}"),
    }
    assert_eq!(
        entries(&property_dir(root.path(), ACTIVE)),
        Vec::<String>::new(),
        "no trace is saved under traces/"
    );
}

/// E1c: a recording run that doesn't violate the property where the first
/// run did keeps both runs' `end` records and their first difference.
#[test]
fn record_failure_keeps_a_recording_that_disagrees() {
    // The first run is of five ticks, but the reruns get three, which never
    // reach the tick ACTIVE forbids.
    let (ticks, _, first) = ticks_run(&TickScenario {
        ticks: 5,
        stamp: false,
        parity: false,
    });
    let three = serde_json::to_value(TickScenario {
        ticks: 3,
        stamp: false,
        parity: false,
    })
    .unwrap();
    let root = TempDir::new().unwrap();
    let evidence = match record_failure(
        ticks,
        &three,
        &[],
        &PropertyId::new(ACTIVE),
        &first,
        root.path(),
    ) {
        Err(RecordError::Nondeterministic { detail, evidence }) => {
            assert!(detail.contains("the recording run didn't"), "{detail}");
            evidence.expect("record_failure keeps the evidence")
        }
        other => panic!("expected Nondeterministic, got {other:?}"),
    };
    let divergence = divergence(root.path(), &evidence);
    assert_eq!(divergence["check"], "recording");
    assert_eq!(divergence["end_difference"], "end.outcome");
    // No replay ran.
    assert_eq!(divergence["replay_difference"], Value::Null);
    let recording = TraceReader::open(&evidence.join(RECORDING_DIR)).unwrap();
    assert_eq!(recording.end.unwrap().violation, None);
    assert!(first_end(&evidence).violation.is_some());
    let properties = properties(&evidence);
    assert_eq!(violation_step(&properties["first"]), Some(4));
    assert_eq!(violation_step(&properties["recording"]), None);
    assert_eq!(properties["replay"], Value::Null);
}

/// The evidence holds the records and property reports of the replay that
/// disagreed with the recording. A third run would hide this
/// nondeterminism: it shows only in every other run, so a fresh recording
/// after the replay matches the first recording.
#[test]
fn record_failure_keeps_the_replay_that_diverged() {
    let (ticks, scenario, first) = ticks_run(&TickScenario {
        ticks: 5,
        stamp: false,
        parity: true,
    });
    let root = TempDir::new().unwrap();
    let evidence = match record_failure(
        ticks,
        &scenario,
        &[],
        &PropertyId::new(ACTIVE),
        &first,
        root.path(),
    ) {
        Err(RecordError::Nondeterministic { detail, evidence }) => {
            assert!(
                detail.contains("step 3 recorded a different obs"),
                "{detail}"
            );
            evidence.expect("record_failure keeps the evidence")
        }
        other => panic!("expected Nondeterministic, got {other:?}"),
    };
    let divergence = divergence(root.path(), &evidence);
    assert_eq!(divergence["check"], "replay");
    assert_eq!(
        divergence["replay_difference"],
        json!({ "step": 3, "field": "obs" })
    );
    assert_eq!(divergence["replay_error"], Value::Null);
    let recording = TraceReader::open(&evidence.join(RECORDING_DIR)).unwrap();
    let replay = TraceReader::open(&evidence.join(REPLAY_DIR)).unwrap();
    assert_eq!(replay.meta, recording.meta);
    assert_eq!(replay.steps.len(), recording.steps.len());
    // The replay took the recording's picks, and only step 3's `obs`
    // differs.
    for (recorded, replayed) in recording.steps.iter().zip(&replay.steps) {
        assert_eq!(recorded.pick, replayed.pick);
        assert_eq!(recorded.obs == replayed.obs, recorded.step != 3);
    }
    let properties = properties(&evidence);
    for run in ["first", "recording", "replay"] {
        assert_eq!(violation_step(&properties[run]), Some(4), "{run}");
    }
}

/// The step of ACTIVE's first violation in `reports`, a run's property
/// reports as `properties.json` holds them.
fn violation_step(reports: &Value) -> Option<u64> {
    let reports = reports.as_array();
    assert!(reports.is_some(), "a run's property reports");
    reports?
        .iter()
        .find(|report| report["property"] == ACTIVE)
        .and_then(|report| report["violations"][0]["step"].as_u64())
}

/// `properties.json` in `evidence`.
fn properties(evidence: &Path) -> Value {
    serde_json::from_str(&std::fs::read_to_string(evidence.join(PROPERTIES_FILE)).unwrap()).unwrap()
}

/// `divergence.json` in `evidence`, a nondeterminism evidence directory
/// under `root`, once its layout checks out: it sits under ACTIVE's
/// directory, it holds the replay's records only for a failed replay check,
/// and each trace reads back complete.
fn divergence(root: &Path, evidence: &Path) -> Value {
    assert_eq!(
        evidence.parent().unwrap(),
        ACTIVE
            .split('/')
            .fold(root.join(NONDETERMINISTIC_DIR), |dir, segment| dir
                .join(segment))
    );
    let divergence: Value =
        serde_json::from_str(&std::fs::read_to_string(evidence.join(DIVERGENCE_FILE)).unwrap())
            .unwrap();
    let mut expected = vec![
        DIVERGENCE_FILE,
        FIRST_END_FILE,
        PROPERTIES_FILE,
        RECORDING_DIR,
    ];
    if divergence["check"] == "replay" {
        expected.push(REPLAY_DIR);
    }
    assert_eq!(entries(evidence), expected);
    for dir in &expected[3..] {
        let trace = TraceReader::open(&evidence.join(dir)).unwrap();
        assert!(trace.is_complete(), "{dir}");
    }
    divergence
}

/// The first run's `end` record, as the evidence keeps it.
fn first_end(evidence: &Path) -> EndRecord {
    serde_json::from_str(&std::fs::read_to_string(evidence.join(FIRST_END_FILE)).unwrap()).unwrap()
}

/// E1c: an input whose rerun disagrees moves from its failure's bucket to
/// `inputs/nondeterministic/`, since nondeterminism isn't a property
/// failure.
#[test]
fn save_failure_files_a_nondeterministic_input_once() {
    let (ticks, scenario, first) = ticks_run(&TickScenario {
        ticks: 5,
        stamp: true,
        parity: false,
    });
    let run = ExploreRun {
        scenario: Some(scenario),
        result: Ok(first),
    };
    let root = TempDir::new().unwrap();
    let id = PropertyId::new(ACTIVE);
    let saved = save_failure(ticks, &[], &run, &id, Bucket::Active, root.path()).unwrap();
    assert!(
        matches!(
            saved.trace,
            Err(RecordError::Nondeterministic {
                evidence: Some(_),
                ..
            })
        ),
        "{:?}",
        saved.trace
    );
    let nondeterministic = input_path(root.path(), Bucket::Nondeterministic, Some(&id), &[]);
    assert_eq!(saved.input, nondeterministic);
    assert_eq!(std::fs::read(&nondeterministic).unwrap(), b"");
    let active = input_path(root.path(), Bucket::Active, Some(&id), &[]);
    assert!(
        std::fs::symlink_metadata(&active).is_err(),
        "the input isn't also filed as an Active failure"
    );
}

#[test]
fn record_failure_records_a_known_violation() {
    let (ticks, scenario, first) = ticks_run(&TickScenario {
        ticks: 3,
        stamp: false,
        parity: false,
    });
    // The Known violation didn't end the run.
    assert_eq!(first.end.violation, None);
    assert_eq!(first_violation(&first, KNOWN), 2);
    let root = TempDir::new().unwrap();
    let trace = record_failure(
        ticks,
        &scenario,
        &[],
        &PropertyId::new(KNOWN),
        &first,
        root.path(),
    )
    .unwrap();
    let end = trace.end.as_ref().unwrap();
    assert_eq!(end.outcome, OutcomeKind::Idle);
    assert_eq!(end.violation, None);
    assert_eq!(
        end_outcome(&trace, KNOWN),
        &PropertyOutcome::Violated { witness: vec![2] }
    );
    assert_eq!(
        trace.dir,
        property_dir(root.path(), KNOWN).join(&trace.id[..12])
    );
}

thread_local! {
    /// What a [`Leaky`] run's spinner borrows and never gives back.
    static BORROWED: RefCell<()> = const { RefCell::new(()) };
}

/// The [`Leaky`] property.
const LEAKY: &str = "test/leaky/no-borrow-left";

/// A harness whose task spins forever holding a thread-local borrow, so
/// shutdown leaks it, and the borrow stays on the run's thread.
struct Leaky;

impl HasView for Leaky {
    type View = ();

    /// Nothing: the check reads the thread-local.
    fn view(&self) {}
}

impl Subsystem for Leaky {
    const NAME: &'static str = "test/leaky";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = ();
    type Prepared = ();

    /// One worker, and a short run.
    fn config((): &()) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 1,
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            budget: 50,
            drain_bound: 10,
            ..RunConfig::default()
        }
    }

    /// Takes the borrow and gives it back, which panics if a leaked task
    /// from an earlier run on this thread still holds it.
    fn prepare((): &()) {
        BORROWED.with(|borrowed| drop(borrowed.borrow_mut()));
    }

    /// Spawns the spinner.
    fn build(sim: &mut Sim, (): &(), (): ()) -> Self {
        sim.scheduler().spawn_root("spinner", async {
            BORROWED.with(|borrowed| {
                let _held = borrowed.borrow_mut();
                // Each load is a preemption point, so the run goes on.
                let never = AtomicU64::new(0);
                while never.load(Ordering::SeqCst) == 0 {}
            });
        });
        Leaky
    }

    /// Violated once the spinner holds the borrow.
    fn properties() -> Vec<Property<Self>> {
        vec![Property {
            id: PropertyId::new(LEAKY),
            claim: "no task keeps the borrow",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Schedule],
            status: Status::Active,
            check: |cx: &mut CheckCtx<'_, Leaky>| {
                if BORROWED.with(|borrowed| borrowed.try_borrow_mut().is_err()) {
                    cx.violated("no borrow left", "the spinner holds the borrow")
                } else {
                    Verdict::Holds
                }
            },
            witness: None,
        }]
    }

    /// The only scenario.
    fn draw(_: &[u8]) {}
}

/// The [`Leaky`] harness.
static LEAKY_HARNESS: HarnessEntry = entry::<Leaky>();

#[test]
fn record_failure_survives_a_run_that_leaks_tasks() {
    // The fast run leaks the spinner, so its thread parks for good.
    let (done, first) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = done.send(fast_run(&LEAKY_HARNESS, &[]));
        loop {
            thread::park();
        }
    });
    let (scenario, first) = first.recv().unwrap();
    assert!(first.cleanup.leaked > 0, "the spinner leaks");
    let step = first_violation(&first, LEAKY);
    // The recording rerun leaks the spinner too. The replay must not reuse
    // its thread, whose borrow is still held.
    let root = TempDir::new().unwrap();
    let trace = record_failure(
        &LEAKY_HARNESS,
        &scenario,
        &[],
        &PropertyId::new(LEAKY),
        &first,
        root.path(),
    )
    .unwrap();
    assert_eq!(
        trace.end.as_ref().unwrap().violation.as_ref().unwrap().step,
        step
    );
}
