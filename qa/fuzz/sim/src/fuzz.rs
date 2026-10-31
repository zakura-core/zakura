//! The fuzz targets' one iteration (explorer.md, One iteration).

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    env,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock, PoisonError},
    time::Instant,
};

pub use zakura_sim::explore::EXPECT_VAR;
use zakura_sim::{
    explore::{
        feedback::{Feedback, NewCollector},
        input_path, save_failure, save_input, traces_root, Bucket, Classified, ExploreOutcome,
        ExploreRun, Worker, ABANDON_LIMIT_PANIC, ABANDON_LIMIT_VAR,
    },
    harness::{fixtures, HarnessEntry, RecordError},
    property::PropertyId,
    trace::{artifacts_root, TRACES_DIR},
};

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
/// - an `Active` violation saves the input under
///   `$ZAKURA_SIM_ARTIFACTS/inputs/active/<property-id>/`, records the run
///   with `record_failure`, then panics with the property ID and the trace's
///   directory, or with `nondeterministic:` when a rerun disagrees, which
///   moves the input to `inputs/nondeterministic/<property-id>/`;
/// - a harness failure panics with `harness:` and the reason;
/// - each `Known` and `Draft` violation saves a trace once per property
///   under its bucket, keeps each distinct input by digest, and the target
///   returns;
/// - a run whose shutdown abandoned tasks saves its input under
///   `inputs/abandoned/`, whatever its outcome, and the worker retires its
///   thread (#1694). Once the process has abandoned
///   `$ZAKURA_SIM_ABANDON_LIMIT` runs, by default 1, the target panics with
///   `abandoned:`, so its supervisor restarts the process;
/// - a clean run returns.
///
/// With `ZAKURA_SIM_EXPECT=<property-id>` set, only an `Active` violation
/// of that property panics, and nothing is saved or recorded. An abort, a
/// stack overflow, a sanitizer report or a timeout still crashes the
/// process, so `cargo fuzz tmin`'s output is a candidate that triage
/// revalidates.
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
/// thread with the run and its class. Then panics with `abandoned:` if the
/// process has reached its abandonment limit.
///
/// Under `ZAKURA_SIM_EXPECT` the limit doesn't apply: `tmin` must crash
/// only on the expected property, and its own run count bounds it.
fn run(
    entry: &'static HarnessEntry,
    input: &[u8],
    feedback: Option<Feedback>,
    then: fn(&'static HarnessEntry, &[u8], &ExploreRun, &Classified),
) {
    WORKER.with(|worker| {
        let mut worker = worker.borrow_mut();
        worker.explore_with(entry, input, feedback, move |input, run, classified| {
            then(entry, input, run, classified)
        });
        let limit = abandon_limit();
        if expected().is_none() && worker.abandoned() >= limit {
            // The count grows only on an abandoned run, so this input is
            // the one that reached the limit.
            panic!(
                "{ABANDON_LIMIT_PANIC}: {} runs abandoned their cleanup, and {ABANDON_LIMIT_VAR} \
                 is {limit}; \
                 the process exits for its supervisor to restart it; input at {}",
                worker.abandoned(),
                input_path(&artifacts_root(), Bucket::Abandoned, None, input).display()
            );
        }
    });
}

/// `ZAKURA_SIM_ABANDON_LIMIT`, read once per process.
fn abandon_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        zakura_sim::explore::abandon_limit(env::var(ABANDON_LIMIT_VAR).ok().as_deref())
            .unwrap_or_else(|error| panic!("{error}"))
    })
}

/// [`fuzz_one`]'s action on one classified run.
fn act(entry: &'static HarnessEntry, input: &[u8], run: &ExploreRun, classified: &Classified) {
    if met_expectation(entry, classified) {
        return;
    }
    if classified.leaked > 0 {
        save_abandoned(input, classified.leaked);
    }
    for (property, bucket) in &classified.notes {
        save_once(entry, input, run, property, *bucket);
    }
    match &classified.outcome {
        ExploreOutcome::Fail { property } => fail(entry, input, run, property),
        ExploreOutcome::HarnessFailure { reason } => panic!("harness: {reason}"),
        ExploreOutcome::Abandoned { .. } | ExploreOutcome::Clean => {}
    }
}

/// Saves the input of a run whose shutdown leaked `leaked` tasks, and logs
/// where. The worker parks the run's thread for good.
fn save_abandoned(input: &[u8], leaked: usize) {
    let saved = save_input(&artifacts_root(), Bucket::Abandoned, None, input)
        .unwrap_or_else(|error| panic!("harness: saving an abandoned input failed: {error}"));
    eprintln!(
        "abandoned: shutdown leaked {leaked} tasks; the worker retired its thread; input at {}",
        saved.display()
    );
}

/// `property` as an ID. The run reported it, so it fits the grammar.
fn property_id(property: &str) -> PropertyId {
    PropertyId::generated(property.to_owned()).unwrap_or_else(|error| panic!("harness: {error}"))
}

/// Saves the input, records the failing run, then panics with the property
/// and the trace's directory.
fn fail(entry: &'static HarnessEntry, input: &[u8], run: &ExploreRun, property: &str) -> ! {
    let saved = save_failure(
        entry,
        input,
        run,
        &property_id(property),
        Bucket::Active,
        &artifacts_root(),
    )
    .unwrap_or_else(|error| panic!("harness: saving the input failed: {error}"));
    match saved.trace {
        Ok(trace) => panic!(
            "{property}: trace at {}; input at {}",
            trace.dir.display(),
            saved.input.display()
        ),
        Err(error) => record_failed(property, error, &saved.input),
    }
}

/// Panics for a violation of `property` that `record_failure` couldn't
/// record, whose input is at `input`: with `nondeterministic:` and the
/// evidence's directory when a rerun disagreed with the run, else with
/// `harness:`.
fn record_failed(property: &str, error: RecordError, input: &Path) -> ! {
    match error {
        RecordError::Nondeterministic {
            detail,
            evidence: Some(dir),
        } => panic!(
            "nondeterministic: {property}: {detail}; evidence in {}; input at {}",
            dir.display(),
            input.display()
        ),
        RecordError::Nondeterministic {
            detail,
            evidence: None,
        } => panic!(
            "nondeterministic: {property}: {detail}; input at {}",
            input.display()
        ),
        error => panic!(
            "harness: record_failure: {error}; input at {}",
            input.display()
        ),
    }
}

/// Distinct inputs one process keeps per bucket and property. Past it, the
/// target logs once and keeps no more, so a property that fails on most
/// inputs can't fill the disk.
const MAX_KEPT_INPUTS: usize = 256;

/// The distinct inputs one process kept for one bucket and property, by
/// path, and whether it logged that it reached [`MAX_KEPT_INPUTS`].
#[derive(Default)]
struct Kept {
    inputs: BTreeSet<PathBuf>,
    logged: bool,
}

/// What [`save_once`] does with one input.
#[derive(Debug, PartialEq, Eq)]
enum Keep {
    /// The process's first input of the property: save it and record a
    /// trace.
    First,
    /// A later distinct input: save it.
    Later,
    /// The first distinct input past [`MAX_KEPT_INPUTS`]: log it.
    Full,
    /// An input already kept, or a later one past the limit: nothing.
    Skip,
}

impl Kept {
    /// What to do with the input at `path`, counting it if it's new.
    fn admit(&mut self, path: PathBuf) -> Keep {
        if self.inputs.contains(&path) {
            return Keep::Skip;
        }
        if self.inputs.len() >= MAX_KEPT_INPUTS {
            if std::mem::replace(&mut self.logged, true) {
                return Keep::Skip;
            }
            return Keep::Full;
        }
        let first = self.inputs.is_empty();
        self.inputs.insert(path);
        if first {
            Keep::First
        } else {
            Keep::Later
        }
    }
}

/// Saves the input of a violation of `property` that [`Classified::notes`]
/// lists, under `$ZAKURA_SIM_ARTIFACTS/inputs/<bucket>/<property-id>/`, by
/// digest, up to [`MAX_KEPT_INPUTS`] distinct inputs per process. The first
/// such input in a process also records a trace under the bucket's
/// [`traces_root`], unless an earlier process already saved one: a
/// property's ID is its signature, so one trace per property is enough. A
/// rerun that disagrees panics with `nondeterministic:`.
fn save_once(
    entry: &'static HarnessEntry,
    input: &[u8],
    run: &ExploreRun,
    property: &str,
    bucket: Bucket,
) {
    static KEPT: Mutex<BTreeMap<(Bucket, String), Kept>> = Mutex::new(BTreeMap::new());
    let root = artifacts_root();
    let id = property_id(property);
    let keep = KEPT
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry((bucket, property.to_owned()))
        .or_default()
        .admit(input_path(&root, bucket, Some(&id), input));
    match keep {
        Keep::First if !has_trace(&traces_root(&root, bucket), property) => {
            let saved = save_failure(entry, input, run, &id, bucket, &root)
                .unwrap_or_else(|error| panic!("harness: saving the input failed: {error}"));
            if let Err(error) = saved.trace {
                record_failed(property, error, &saved.input);
            }
        }
        Keep::First | Keep::Later => {
            save_input(&root, bucket, Some(&id), input)
                .unwrap_or_else(|error| panic!("harness: saving the input failed: {error}"));
        }
        Keep::Full => eprintln!(
            "{}: {property}: kept {MAX_KEPT_INPUTS} distinct inputs in this process; \
             later ones aren't saved",
            bucket.name()
        ),
        Keep::Skip => {}
    }
}

/// True if `root/traces/<property-id>/` holds a saved trace.
fn has_trace(root: &Path, property: &str) -> bool {
    let dir = property
        .split('/')
        .fold(root.join(TRACES_DIR), |dir, segment| dir.join(segment));
    std::fs::read_dir(&dir).is_ok_and(|mut entries| {
        entries.any(|entry| {
            entry.is_ok_and(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
        })
    })
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
///   next input panics with `missed:`;
/// - an abandoned run saves its input, and counts toward the abandonment
///   limit, as in [`fuzz_one`].
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
    if classified.leaked > 0 {
        save_abandoned(input, classified.leaked);
    }
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
        ExploreOutcome::Abandoned { .. } | ExploreOutcome::Clean => {}
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{Keep, Kept, MAX_KEPT_INPUTS};

    /// The cap counts distinct inputs: a repeated input neither counts nor
    /// saves again, and the first distinct input past the cap logs once.
    #[test]
    fn kept_counts_distinct_inputs() {
        let mut kept = Kept::default();
        let path = |n: usize| PathBuf::from(format!("{n}.bin"));
        assert_eq!(kept.admit(path(0)), Keep::First);
        assert_eq!(kept.admit(path(0)), Keep::Skip);
        for n in 1..MAX_KEPT_INPUTS {
            assert_eq!(kept.admit(path(n)), Keep::Later, "{n}");
            assert_eq!(kept.admit(path(n)), Keep::Skip, "{n}");
        }
        assert_eq!(kept.admit(path(0)), Keep::Skip);
        assert_eq!(kept.admit(path(MAX_KEPT_INPUTS)), Keep::Full);
        assert_eq!(kept.admit(path(MAX_KEPT_INPUTS + 1)), Keep::Skip);
    }

    /// One input repeated past the cap still leaves room for a second
    /// distinct input (review of bc06ef3a2).
    #[test]
    fn a_repeated_input_leaves_room_for_another() {
        let mut kept = Kept::default();
        assert_eq!(kept.admit(PathBuf::from("known.bin")), Keep::First);
        for _ in 1..=MAX_KEPT_INPUTS {
            assert_eq!(kept.admit(PathBuf::from("known.bin")), Keep::Skip);
        }
        assert_eq!(kept.admit(PathBuf::from("known-1.bin")), Keep::Later);
    }
}
