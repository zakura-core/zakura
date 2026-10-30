//! The fuzz targets' one iteration (explorer.md, One iteration).

use std::{
    cell::RefCell,
    collections::BTreeSet,
    env,
    path::PathBuf,
    sync::{Mutex, OnceLock, PoisonError},
    time::Instant,
};

use zakura_sim::{
    explore::{
        feedback::{Feedback, NewCollector},
        Bucket, Classified, ExploreOutcome, ExploreRun, Worker,
    },
    harness::{fixtures, HarnessEntry, RecordError},
    property::PropertyId,
    trace::{artifacts_root, record_failure, TRACES_DIR},
};

/// The variable that makes a target panic only on one property's
/// violation, for `cargo fuzz tmin`.
pub const EXPECT_VAR: &str = "ZAKURA_SIM_EXPECT";

/// What the filter prints before its panic, followed by the harness's name,
/// so `cargo xtask sim-minimize` can check which harness a target runs.
pub const HARNESS_LINE: &str = "zakura-sim-fuzz harness: ";

/// A fuzz target over harness `$harness`: `sim_target!(discovery::WaitComplete)`
/// for a harness under `zakura_sim::harness`. With a second argument,
/// `$collector` makes each input's collector, whose features libFuzzer reads
/// beside its code edges: `sim_target!(feedback::Probe, feedback::probe_collector)`.
#[macro_export]
macro_rules! sim_target {
    ($harness:ty) => {
        static ENTRY: zakura_sim::harness::HarnessEntry = zakura_sim::harness::entry::<$harness>();

        libfuzzer_sys::fuzz_target!(init: $crate::unwind_task_panics(), |input: &[u8]| {
            $crate::fuzz_one(&ENTRY, input);
        });
    };
    ($harness:ty, $collector:path) => {
        static ENTRY: zakura_sim::harness::HarnessEntry = zakura_sim::harness::entry::<$harness>();

        libfuzzer_sys::fuzz_target!(
            init: {
                $crate::unwind_task_panics();
                $crate::init_feedback();
            },
            |input: &[u8]| {
                $crate::fuzz_one_with(&ENTRY, $collector, input);
            }
        );
    };
}

/// Replaces libFuzzer's panic hook, which aborts at any panic, with the
/// default hook. A task's panic then unwinds to the scheduler, which reports
/// it as a `sim/no-task-panic` violation, so the target records it and
/// `ZAKURA_SIM_EXPECT` can filter it. The target's own panics still abort
/// the process once they unwind out of the `fuzz_target!` body. Each target
/// calls this in its `init`.
pub fn unwind_task_panics() {
    drop(std::panic::take_hook());
}

thread_local! {
    /// The thread each input runs on, replaced after an abandoned run.
    static WORKER: RefCell<Worker> = RefCell::new(Worker::new());
}

/// Runs `input` on `entry`'s harness and acts on the run's class:
///
/// - an `Active` violation records the run with `record_failure`, then
///   panics with the property ID and the trace's directory, or with
///   `nondeterministic:` when a rerun disagrees;
/// - a harness failure panics with `harness:` and the reason;
/// - each `Known` and `Draft` violation saves a trace once per property
///   under its bucket, and the target returns;
/// - an abandoned run is logged and returns, and the worker retires its
///   thread (#1694);
/// - a clean run returns.
///
/// With `ZAKURA_SIM_EXPECT=<property-id>` set, only an `Active` violation
/// of that property panics, and nothing is recorded. An abort, a stack
/// overflow, a sanitizer report or a timeout still crashes the process, so
/// `cargo fuzz tmin`'s output is a candidate that triage revalidates.
pub fn fuzz_one(entry: &'static HarnessEntry, input: &[u8]) {
    run(entry, input, None, act);
}

/// [`fuzz_one`], with `collector`'s features in the counters
/// `ZAKURA_SIM_FEEDBACK` picks ([`crate::feedback`]).
pub fn fuzz_one_with(entry: &'static HarnessEntry, collector: NewCollector, input: &[u8]) {
    run(entry, input, Some(crate::feedback(collector)), act);
}

/// Runs `input` on `entry`'s harness on this thread's worker, with
/// `feedback`'s collector if there is one, and calls `then` on the worker's
/// thread with the run and its class.
fn run(
    entry: &'static HarnessEntry,
    input: &[u8],
    feedback: Option<Feedback>,
    then: fn(&'static HarnessEntry, &[u8], &ExploreRun, &Classified),
) {
    WORKER.with(|worker| {
        worker
            .borrow_mut()
            .explore_with(entry, input, feedback, move |input, run, classified| {
                then(entry, input, run, classified)
            })
    });
}

/// [`fuzz_one`]'s action on one classified run.
fn act(entry: &'static HarnessEntry, input: &[u8], run: &ExploreRun, classified: &Classified) {
    if met_expectation(entry, classified) {
        return;
    }
    for (property, bucket) in &classified.notes {
        save_once(entry, input, run, property, *bucket);
    }
    match &classified.outcome {
        ExploreOutcome::Fail { property } => fail(entry, input, run, property),
        ExploreOutcome::HarnessFailure { reason } => panic!("harness: {reason}"),
        ExploreOutcome::Abandoned { leaked } => abandoned(*leaked),
        ExploreOutcome::Clean => {}
    }
}

/// Logs an abandoned run. The worker parks the run's thread for good, so a
/// campaign that keeps abandoning runs keeps growing; the log shows why.
fn abandoned(leaked: usize) {
    eprintln!("abandoned: shutdown leaked {leaked} tasks; the worker retired its thread");
}

/// Records the failing run, then panics with the property and the trace's
/// directory.
fn fail(entry: &'static HarnessEntry, input: &[u8], run: &ExploreRun, property: &str) -> ! {
    match record(entry, input, run, property, &artifacts_root()) {
        Ok(dir) => panic!("{property}: trace at {}", dir.display()),
        Err(error) => record_failed(property, error),
    }
}

/// Panics for a violation of `property` that `record_failure` couldn't
/// record: with `nondeterministic:` when a rerun disagreed with the run,
/// else with `harness:`.
fn record_failed(property: &str, error: RecordError) -> ! {
    match error {
        RecordError::Nondeterministic { detail } => {
            panic!("nondeterministic: {property}: {detail}")
        }
        error => panic!("harness: record_failure: {error}"),
    }
}

/// Records `property`'s violation in `run` under `root`, and returns the
/// trace's directory.
fn record(
    entry: &'static HarnessEntry,
    input: &[u8],
    run: &ExploreRun,
    property: &str,
    root: &std::path::Path,
) -> Result<PathBuf, RecordError> {
    let id = PropertyId::generated(property.to_owned())
        .unwrap_or_else(|error| panic!("harness: {error}"));
    let scenario = run
        .scenario
        .as_ref()
        .expect("run_bytes keeps the scenario of a run that reports a violation");
    let report = run
        .report()
        .expect("a run that reports a violation has a report");
    record_failure(entry, scenario, input, &id, report, root).map(|trace| trace.dir)
}

/// Saves a trace of the `Known` or `Draft` violation of `property` under
/// `$ZAKURA_SIM_ARTIFACTS/<bucket>/`, unless this process or an earlier one
/// already saved one. A property's ID is its signature, so one trace per
/// property is enough. A rerun that disagrees panics with
/// `nondeterministic:`.
fn save_once(
    entry: &'static HarnessEntry,
    input: &[u8],
    run: &ExploreRun,
    property: &str,
    bucket: Bucket,
) {
    static SAVED: Mutex<BTreeSet<(String, String)>> = Mutex::new(BTreeSet::new());
    let name = match bucket {
        Bucket::Known => "known",
        Bucket::Draft => "draft",
        Bucket::Active | Bucket::Harness | Bucket::Crash => return,
    };
    let mut saved = SAVED.lock().unwrap_or_else(PoisonError::into_inner);
    if !saved.insert((name.to_owned(), property.to_owned())) {
        return;
    }
    let root = artifacts_root().join(name);
    let dir = property
        .split('/')
        .fold(root.join(TRACES_DIR), |dir, segment| dir.join(segment));
    let earlier = std::fs::read_dir(&dir).is_ok_and(|mut entries| {
        entries.any(|entry| {
            entry.is_ok_and(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
        })
    });
    if earlier {
        return;
    }
    if let Err(error) = record(entry, input, run, property, &root) {
        record_failed(property, error);
    }
}

/// With `ZAKURA_SIM_EXPECT` set, prints [`HARNESS_LINE`] with `entry`'s
/// name and panics on an `Active` violation of that property, and returns
/// `true` for any other run, so the caller does nothing else with it.
/// Returns `false` when the variable is unset.
fn met_expectation(entry: &HarnessEntry, classified: &Classified) -> bool {
    let Some(want) = expected() else {
        return false;
    };
    if matches!(&classified.outcome, ExploreOutcome::Fail { property } if property == want) {
        eprintln!("{HARNESS_LINE}{}", entry.name);
        panic!("{want}");
    }
    true
}

/// `ZAKURA_SIM_EXPECT`, read once per process. Empty counts as unset.
fn expected() -> Option<&'static str> {
    static EXPECT: OnceLock<Option<String>> = OnceLock::new();
    EXPECT
        .get_or_init(|| env::var(EXPECT_VAR).ok().filter(|id| !id.is_empty()))
        .as_deref()
}

/// The `race_fixtures` target's iteration, under the fixtures'
/// expected-failure policy ([`fixtures::EXPECTED`]):
///
/// - a failure in a `Fixed` variant panics, as does a harness failure;
/// - a planted variant's failure of its own fixture's property is expected:
///   the first find of each fixture's `Base` variant is logged with the
///   time since the target started, and the target returns;
/// - a failure of any other property panics;
/// - once a fixture's discovery limit has passed with no `Base` find, the
///   next input panics with `missed:`.
///
/// `ZAKURA_SIM_EXPECT` works as in [`fuzz_one`], and replaces the policy.
pub fn fuzz_fixtures(input: &[u8]) {
    static ENTRY: HarnessEntry = zakura_sim::harness::entry::<fixtures::All>();
    run(&ENTRY, input, None, act_fixtures);
}

/// When the target first ran, and each fixture whose `Base` variant it
/// found.
struct Finds {
    start: Instant,
    found: BTreeSet<&'static str>,
}

/// [`fuzz_fixtures`]'s action on one classified run.
fn act_fixtures(
    entry: &'static HarnessEntry,
    input: &[u8],
    run: &ExploreRun,
    classified: &Classified,
) {
    if met_expectation(entry, classified) {
        return;
    }
    static FINDS: OnceLock<Mutex<Finds>> = OnceLock::new();
    let finds = FINDS.get_or_init(|| {
        Mutex::new(Finds {
            start: Instant::now(),
            found: BTreeSet::new(),
        })
    });
    let mut finds = finds.lock().unwrap_or_else(PoisonError::into_inner);
    match &classified.outcome {
        ExploreOutcome::HarnessFailure { reason } => panic!("harness: {reason}"),
        ExploreOutcome::Fail { property } => {
            let scenario: fixtures::PickScenario = run
                .scenario
                .as_ref()
                .and_then(|scenario| serde_json::from_value(scenario.clone()).ok())
                .unwrap_or_else(|| panic!("harness: {property}: the run kept no scenario"));
            let own = scenario.fixture.property();
            // Only a planted variant's failure of its own property is expected.
            if scenario.variant == fixtures::Variant::Fixed || property != own {
                fail(entry, input, run, property);
            }
            if scenario.variant == fixtures::Variant::Base && finds.found.insert(own) {
                eprintln!(
                    "race_fixtures: found {own} in its base variant after {:.1} s",
                    finds.start.elapsed().as_secs_f64()
                );
            }
        }
        ExploreOutcome::Abandoned { leaked } => abandoned(*leaked),
        ExploreOutcome::Clean => {}
    }
    let elapsed = finds.start.elapsed();
    if let Some((property, limit)) = fixtures::EXPECTED
        .iter()
        .find(|(property, limit)| !finds.found.contains(property) && elapsed > *limit)
    {
        panic!(
            "missed: {property}: no find in its base variant within {} s",
            limit.as_secs()
        );
    }
}
