//! E1a: the explorer's run of one fuzz input ends on any input, leaves no
//! run state behind, and classifies from every property report. A thread
//! that ran an abandoned run is retired (#1694).
#![cfg(zakura_sim)]

mod support;

use std::thread;

use serde::{Deserialize, Serialize};
use zakura_rt::sync::atomic::{AtomicU64, Ordering::SeqCst};
use zakura_scheduler::{stack_pool, CleanupReport, SchedulerConfig};
use zakura_sim::{
    end::IdleStatus,
    explore::{classify, Bucket, Classified, ExploreOutcome, ExploreRun, Worker},
    harness::{self, entry, HarnessEntry, HarnessError, Subsystem, HARNESSES, NO_TASK_PANIC},
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Verdict},
    Checker, HarnessFailure, MemorySink, NullSink, ObservationLog, Outcome, Plan, PropertyOutcome,
    PropertyReport, Record, RunConfig, RunEnd, RunReport, Sim, StepCtx, Violation,
};

/// A test harness's fixed scenario.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Fixed {}

/// A run config with one worker, small stacks and these step limits.
fn limits(budget: u64, drain_bound: u64) -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers: 1,
            stack_size: 64 * 1024,
            ..SchedulerConfig::default()
        },
        budget,
        drain_bound,
        ..RunConfig::default()
    }
}

/// Implements a fixed-scenario harness with no view, whose `build` is
/// `$build` and whose properties are `$properties`.
macro_rules! test_harness {
    ($harness:ident, $name:literal, $build:expr, $properties:expr) => {
        struct $harness;

        impl HasView for $harness {
            type View = ();

            /// Nothing: the checks read observations, if any.
            fn view(&self) {}
        }

        impl Subsystem for $harness {
            const NAME: &'static str = $name;
            const GENERATOR: u32 = 1;
            const SCENARIO_BYTES: usize = 0;
            type Scenario = Fixed;
            type Prepared = ();

            /// Short limits.
            fn config(_: &Fixed) -> RunConfig {
                limits(50, 10)
            }

            /// Nothing to set up.
            fn prepare(_: &Fixed) {}

            /// Spawns the harness's tasks.
            fn build(sim: &mut Sim, _: &Fixed, (): ()) -> Self {
                let build: fn(&mut Sim) = $build;
                build(sim);
                $harness
            }

            /// The harness's properties.
            fn properties() -> Vec<Property<Self>> {
                $properties
            }

            /// The fixed scenario.
            fn draw(_: &[u8]) -> Fixed {
                Fixed {}
            }
        }
    };
}

/// A check that always holds. The table's reports are built by hand.
fn holds<S: HasView>(_: &mut CheckCtx<'_, S>) -> Verdict {
    Verdict::Holds
}

/// A property of `S` with `id` and `status` that always holds.
fn property<S: HasView>(id: &'static str, status: Status) -> Property<S> {
    Property {
        id: PropertyId::new(id),
        claim: "holds",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Schedule],
        status,
        check: holds::<S>,
        witness: None,
    }
}

const ACTIVE: &str = "test/table/active";
const KNOWN: &str = "test/table/known";
const KNOWN_TOO: &str = "test/table/known-too";
const DRAFT: &str = "test/table/draft";

test_harness!(Table, "test/table", |_| {}, {
    vec![
        property(ACTIVE, Status::Active),
        property(KNOWN, Status::Known { issue: 1 }),
        property(KNOWN_TOO, Status::Known { issue: 2 }),
        property(DRAFT, Status::Draft),
    ]
});

static TABLE: HarnessEntry = entry::<Table>();

test_harness!(
    Colon,
    "test/colon",
    |sim| {
        sim.scheduler().spawn_root("bad:name", async {});
    },
    Vec::new()
);

static COLON: HarnessEntry = entry::<Colon>();

test_harness!(
    Pending,
    "test/pending",
    |sim| {
        sim.scheduler()
            .spawn_root("forever", std::future::pending::<()>());
    },
    Vec::new()
);

static PENDING: HarnessEntry = entry::<Pending>();

test_harness!(
    Spinner,
    "test/spinner",
    |sim| {
        sim.scheduler().spawn_root("spinner", async {
            // Nothing ever stores, and every load is a point, so cleanup
            // gets stuck and shutdown abandons the run.
            let flag = AtomicU64::new(0);
            while flag.load(SeqCst) == 0 {}
        });
    },
    Vec::new()
);

static SPINNER: HarnessEntry = entry::<Spinner>();

/// `input`'s scenario prefix, zero-padded, as `run_bytes` draws it.
fn prefix(entry: &HarnessEntry, input: &[u8]) -> Vec<u8> {
    let mut prefix = input[..entry.scenario_bytes.min(input.len())].to_vec();
    prefix.resize(entry.scenario_bytes, 0);
    prefix
}

#[test]
fn empty_malformed_and_exhausted_inputs_end() {
    for entry in HARNESSES {
        let lengths = [0, 1, entry.scenario_bytes.saturating_sub(1), 4_096];
        for len in lengths {
            let input = vec![0xff; len];
            let run = (entry.run_bytes)(&input, &mut NullSink, None);
            let what = format!("{} on {len} bytes", entry.name);
            let report = match &run.result {
                Ok(report) => report,
                Err(error) => panic!("{what}: {error}"),
            };
            let config = (entry.config)(&(entry.draw)(&prefix(entry, &input))).unwrap();
            assert!(
                matches!(
                    report.end.outcome,
                    Outcome::Budget | Outcome::Idle { .. } | Outcome::Violation
                ),
                "{what}: {:?}",
                report.end
            );
            assert!(
                report.end.steps <= config.budget + config.drain_bound,
                "{what}: {} steps",
                report.end.steps
            );
            assert_eq!(report.cleanup.leaked, 0, "{what}");
            assert_eq!(run.leaked(), 0, "{what}");
        }
    }
}

/// Stacks this thread's runs still hold: allocated, less freed and idle.
fn stacks_held() -> u64 {
    let pool = stack_pool();
    pool.allocated - pool.freed - pool.idle as u64
}

/// What two runs of one input must share: the end, every property report,
/// the cleanup, the picks, and every record, digests included.
#[derive(Debug, PartialEq)]
struct Seen {
    end: RunEnd,
    properties: Vec<PropertyReport>,
    cleanup: CleanupReport,
    picks: Vec<String>,
    records: Vec<String>,
}

/// Runs `input` on `entry` with every record kept, and checks that the run
/// left no task or stack behind.
fn run_and_check(entry: &HarnessEntry, input: &[u8]) -> (ExploreRun, Vec<String>) {
    let mut sink = MemorySink::default();
    let run = (entry.run_bytes)(input, &mut sink, None);
    let report = run.report().expect("the harness ran the scenario");
    assert_eq!(report.cleanup.leaked, 0, "{}", entry.name);
    assert_eq!(stacks_held(), 0, "{}: no run holds a stack", entry.name);
    (run, sink.lines())
}

/// What [`Seen`] keeps of a run.
fn seen(run: &ExploreRun, records: Vec<String>) -> Seen {
    let report = run.report().unwrap();
    Seen {
        end: report.end.clone(),
        properties: report.properties.clone(),
        cleanup: report.cleanup,
        picks: support::picks(report)
            .into_iter()
            .map(str::to_owned)
            .collect(),
        records,
    }
}

/// The first `race/lost-update` input that finds the planted race.
fn lost_update_failure() -> Vec<u8> {
    let race = harness::find("race/lost-update").unwrap();
    support::byte_inputs(500, 64)
        .into_iter()
        .map(|plan| [&[0, 1][..], &plan].concat())
        .find(|input| {
            let run = (race.run_bytes)(input, &mut NullSink, None);
            run.report().unwrap().end.violation.is_some()
        })
        .expect("some input finds the planted race")
}

#[test]
fn aba_in_one_process() {
    let discovery = harness::find("discovery/wait-complete").unwrap();
    let race = harness::find("race/lost-update").unwrap();
    let a: &[u8] = &[0, 1, 7, 3, 9, 1, 4, 2, 5, 5, 8, 0, 6, 2];
    let violation = lost_update_failure();
    let bs: [(&str, &HarnessEntry, &[u8]); 4] = [
        // The fix never fails.
        ("clean", race, &[1, 1, 3, 1, 4]),
        ("violation", race, &violation),
        ("harness error", &COLON, &[]),
        ("cancelled", &PENDING, &[]),
    ];
    for (form, b, input) in bs {
        let (first, records) = run_and_check(discovery, a);
        let first = seen(&first, records);
        let (run, _) = run_and_check(b, input);
        let report = run.report().unwrap();
        match form {
            "clean" => assert_eq!(classify(b, &run).outcome, ExploreOutcome::Clean),
            "violation" => assert!(matches!(
                classify(b, &run).outcome,
                ExploreOutcome::Fail { .. }
            )),
            "harness error" => assert!(matches!(
                &run.result,
                Err(HarnessError::Run {
                    failure: HarnessFailure::TaskNameHasColon { .. },
                    ..
                })
            )),
            _ => assert_eq!(report.cleanup.cancelled, 1, "{:?}", report.cleanup),
        }
        let (again, records) = run_and_check(discovery, a);
        assert_eq!(seen(&again, records), first, "A, then {form}, then A");
    }
}

/// A run report that ended with `outcome`, `violation` and `harness`, with
/// `properties`, and `leaked` tasks leaked.
fn synthetic(
    outcome: Outcome,
    violation: Option<&str>,
    harness: Option<HarnessFailure>,
    properties: Vec<PropertyReport>,
    leaked: usize,
) -> RunReport {
    RunReport {
        end: RunEnd {
            outcome,
            steps: 3,
            violation: violation.map(|property| Violation {
                property: property.to_owned(),
                step: 3,
                witness: vec![3],
                expected: "expected".to_owned(),
                actual: "actual".to_owned(),
            }),
            drain: None,
            harness,
        },
        properties,
        cleanup: CleanupReport {
            leaked,
            ..CleanupReport::default()
        },
        log: ObservationLog::default(),
        picks: Vec::new(),
        resolved: Plan::default(),
    }
}

/// `property` violated at step 3.
fn violated(property: &str) -> PropertyReport {
    PropertyReport::new(property, PropertyOutcome::Violated { witness: vec![3] })
}

/// `property` held.
fn held(property: &str) -> PropertyReport {
    PropertyReport::new(property, PropertyOutcome::Held { applicable: 1 })
}

/// A run with `result`.
fn run_of(result: Result<RunReport, HarnessError>) -> ExploreRun {
    ExploreRun {
        scenario: None,
        result,
    }
}

/// A harness failure that ended a run with `report`.
fn harness_error(report: RunReport) -> HarnessError {
    HarnessError::Run {
        failure: HarnessFailure::UnknownDrawKind,
        report: Box::new(report),
    }
}

/// One row of the outcome table: its name, the run, and the target action,
/// bucket and notes it gets.
type Row = (
    &'static str,
    ExploreRun,
    ExploreOutcome,
    Option<Bucket>,
    Vec<(String, Bucket)>,
);

#[test]
fn classify_table() {
    let idle = Outcome::Idle {
        status: IdleStatus::Quiescent,
    };
    // explorer.md's outcome table, one row each: the run, then the target
    // action and its bucket.
    let rows: Vec<Row> = vec![
        (
            "Active property violated",
            run_of(Ok(synthetic(
                Outcome::Violation,
                Some(ACTIVE),
                None,
                vec![violated(ACTIVE)],
                0,
            ))),
            ExploreOutcome::Fail {
                property: ACTIVE.to_owned(),
            },
            Some(Bucket::Active),
            vec![],
        ),
        (
            "sim/no-task-panic violated",
            run_of(Ok(synthetic(
                Outcome::Violation,
                Some(NO_TASK_PANIC),
                None,
                vec![violated(NO_TASK_PANIC)],
                0,
            ))),
            ExploreOutcome::Fail {
                property: NO_TASK_PANIC.to_owned(),
            },
            Some(Bucket::Active),
            vec![],
        ),
        (
            "Known property violated",
            run_of(Ok(synthetic(
                Outcome::Budget,
                None,
                None,
                vec![violated(KNOWN)],
                0,
            ))),
            ExploreOutcome::Clean,
            None,
            vec![(KNOWN.to_owned(), Bucket::Known)],
        ),
        (
            "Draft property violated",
            run_of(Ok(synthetic(idle, None, None, vec![violated(DRAFT)], 0))),
            ExploreOutcome::Clean,
            None,
            vec![(DRAFT.to_owned(), Bucket::Draft)],
        ),
        (
            "harness failure",
            run_of(Err(harness_error(synthetic(
                Outcome::Harness,
                None,
                Some(HarnessFailure::UnknownDrawKind),
                vec![],
                0,
            )))),
            // The failure's own text, as the run's `end` record holds it.
            ExploreOutcome::HarnessFailure {
                reason: HarnessFailure::UnknownDrawKind.to_string(),
            },
            Some(Bucket::Harness),
            vec![],
        ),
        (
            "budget",
            run_of(Ok(synthetic(
                Outcome::Budget,
                None,
                None,
                vec![held(ACTIVE)],
                0,
            ))),
            ExploreOutcome::Clean,
            None,
            vec![],
        ),
        (
            "idle",
            run_of(Ok(synthetic(idle, None, None, vec![held(ACTIVE)], 0))),
            ExploreOutcome::Clean,
            None,
            vec![],
        ),
        (
            "abandoned (#1694)",
            run_of(Ok(synthetic(
                Outcome::Budget,
                None,
                None,
                vec![held(ACTIVE)],
                2,
            ))),
            ExploreOutcome::Abandoned { leaked: 2 },
            Some(Bucket::Abandoned),
            vec![],
        ),
    ];
    for (row, run, outcome, bucket, notes) in rows {
        let classified = classify(&TABLE, &run);
        let leaked = run.leaked();
        assert_eq!(
            classified,
            Classified {
                outcome,
                notes,
                leaked
            },
            "{row}"
        );
        assert_eq!(classified.outcome.bucket(), bucket, "{row}");
    }
}

/// A checker that reports `ACTIVE` violated from `at_end` only.
struct ViolatesAtEnd;

impl Checker for ViolatesAtEnd {
    fn after_step(&mut self, _: &StepCtx<'_>) -> Option<Violation> {
        None
    }

    fn at_end(&mut self, _: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        vec![PropertyReport::new(
            ACTIVE,
            PropertyOutcome::Violated {
                witness: Vec::new(),
            },
        )]
    }
}

#[test]
fn classify_reads_every_report() {
    // An Active violation returned only from at_end fails a run that ended
    // idle.
    let run = (TABLE.run_bytes)(&[], &mut NullSink, Some(&mut ViolatesAtEnd));
    let report = run.report().unwrap();
    assert!(matches!(report.end.outcome, Outcome::Idle { .. }));
    assert_eq!(report.end.violation, None);
    assert_eq!(
        classify(&TABLE, &run).outcome,
        ExploreOutcome::Fail {
            property: ACTIVE.to_owned()
        }
    );
    assert!(run.scenario.is_some(), "a failing run keeps its scenario");

    // Two Known and one Draft violation keep three notes.
    let run = run_of(Ok(synthetic(
        Outcome::Budget,
        None,
        None,
        vec![
            violated(KNOWN),
            held(ACTIVE),
            violated(DRAFT),
            violated(KNOWN_TOO),
        ],
        0,
    )));
    assert_eq!(
        classify(&TABLE, &run),
        Classified {
            outcome: ExploreOutcome::Clean,
            notes: vec![
                (KNOWN.to_owned(), Bucket::Known),
                (DRAFT.to_owned(), Bucket::Draft),
                (KNOWN_TOO.to_owned(), Bucket::Known),
            ],
            leaked: 0,
        }
    );
}

/// A harness failure after a Known and an Active violation ranks first, and
/// keeps both violations as notes, the Active one under `active`.
#[test]
fn a_harness_failure_keeps_its_active_violation() {
    let run = run_of(Err(harness_error(synthetic(
        Outcome::Harness,
        None,
        Some(HarnessFailure::UnknownDrawKind),
        vec![violated(KNOWN), violated(ACTIVE)],
        0,
    ))));
    let classified = classify(&TABLE, &run);
    assert!(
        matches!(classified.outcome, ExploreOutcome::HarnessFailure { .. }),
        "{classified:?}"
    );
    assert_eq!(
        classified.notes,
        [
            (KNOWN.to_owned(), Bucket::Known),
            (ACTIVE.to_owned(), Bucket::Active),
        ]
    );
    assert_eq!(
        classified.buckets(),
        [Bucket::Active, Bucket::Known, Bucket::Harness]
    );
}

#[test]
fn committed_input_records_and_classifies() {
    let discovery = harness::find("discovery/wait-complete").unwrap();
    let input = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/discovery-wait-complete.bin"
    ))
    .unwrap();
    let mut sink = MemorySink::default();
    let recorded = (discovery.run_bytes)(&input, &mut sink, None);
    assert_eq!(
        classify(discovery, &recorded).outcome,
        ExploreOutcome::Clean
    );
    assert!(
        recorded.scenario.is_none(),
        "a clean run serializes nothing"
    );
    let steps = sink
        .records
        .iter()
        .filter(|r| matches!(r, Record::Step(_)))
        .count();
    let report = recorded.report().unwrap();
    assert_eq!(steps as u64, report.end.steps);
    // The fast run ends the same way.
    let fast = (discovery.run_bytes)(&input, &mut NullSink, None);
    assert_eq!(fast.report().unwrap().end, report.end);
    assert_eq!(fast.report().unwrap().properties, report.properties);
}

#[test]
fn a_violation_only_an_extra_checker_reports_keeps_its_scenario() {
    let mut extra = zakura_sim::check::after_step(|cx: &StepCtx<'_>| {
        Some(Violation {
            property: "test/extra".to_owned(),
            step: cx.step,
            witness: Vec::new(),
            expected: "nothing".to_owned(),
            actual: "a step".to_owned(),
        })
    });
    let run = (PENDING.run_bytes)(&[], &mut NullSink, Some(&mut extra));
    let report = run.report().expect("the harness ran the scenario");
    assert_eq!(
        report.end.violation.as_ref().map(|v| v.property.as_str()),
        Some("test/extra")
    );
    assert!(
        report.properties.iter().all(|p| p.property != "test/extra"),
        "only end.violation names the extra checker's property"
    );
    assert_eq!(
        classify(&PENDING, &run).outcome,
        ExploreOutcome::Fail {
            property: "test/extra".to_owned()
        }
    );
    assert!(
        run.scenario.is_some(),
        "a run classify fails keeps its scenario for record_failure"
    );
}

#[test]
fn abandoned_run_retires_the_worker() {
    let mut worker = Worker::new();
    let on = |worker: &mut Worker, entry: &'static HarnessEntry| {
        worker.explore(entry, &[], |_, run, classified| {
            (
                classified.outcome.clone(),
                run.leaked(),
                thread::current().id(),
            )
        })
    };
    let (outcome, _, first) = on(&mut worker, &PENDING);
    assert_eq!(outcome, ExploreOutcome::Clean);
    let (outcome, leaked, spun) = on(&mut worker, &SPINNER);
    assert_eq!(outcome, ExploreOutcome::Abandoned { leaked: 1 });
    assert_eq!(leaked, 1);
    assert_eq!(spun, first, "the thread is reused until a run abandons");
    assert_eq!(worker.retired(), 1);
    let (outcome, _, fresh) = on(&mut worker, &PENDING);
    assert_eq!(outcome, ExploreOutcome::Clean);
    assert_ne!(fresh, spun, "the next input runs on a fresh thread");
    assert_ne!(fresh, thread::current().id());
}

#[test]
fn a_panicking_job_retires_the_worker_and_resumes() {
    let mut worker = Worker::new();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        worker.explore(&PENDING, &[], |_, _, _| panic!("the target crashed"))
    }));
    assert!(panicked.is_err());
    assert_eq!(worker.retired(), 1);
    let outcome = worker.explore(&PENDING, &[], |_, _, classified| classified.outcome.clone());
    assert_eq!(outcome, ExploreOutcome::Clean);
}
