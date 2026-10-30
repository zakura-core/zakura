//! The explorer's run of one fuzz input, and the classifier that says what
//! the fuzz target does with it (explorer.md, One iteration and Outcome
//! handling).
//!
//! [`run_bytes`] splits the input into the harness's scenario prefix and the
//! Plan bytes, and runs the scenario. [`classify`] turns the run into the
//! target's action. Both live in `zakura-sim`, so the fuzz targets and
//! triage share one classifier.
//!
//! A [`Worker`] runs inputs one at a time on a thread of its own. After a
//! run whose shutdown abandoned tasks, it retires that thread and starts a
//! fresh one (#1694).
//!
//! A run's result has independent parts: its property reports, a harness
//! failure and its cleanup. [`Classified`] keeps the target's action, each
//! `Known` and `Draft` violation, an `Active` violation in a run that ended
//! at a harness failure, and an abandoned cleanup apart, so the action never
//! hides the rest. [`classify_process`] classifies a run in a fresh process
//! from the trace it wrote, by the same rules, and adds how the process
//! ended. [`save_input`] and [`save_failure`] keep a failing input on disk
//! before anything reruns it.
//!
//! [`feedback`] turns abstract protocol states into features libFuzzer
//! reads beside its code edges (E2a), and [`state`] collects the protocol
//! states of the feature table as those features (E2b).

pub mod feedback;
mod process;
mod save;
pub mod state;
mod worker;

use serde::Serialize;
use serde_json::Value;

pub use process::{
    classify_process, ProcessClassified, ProcessExit, ProcessFailure, ProcessResult,
};
pub use save::{input_path, save_failure, save_input, traces_root, SavedFailure, INPUTS_DIR};
pub use worker::{abandon_limit, Worker, ABANDON_LIMIT_VAR, DEFAULT_ABANDON_LIMIT};

use crate::{
    check::{Checker, PropertyOutcome, PropertyReport},
    harness::{run_harness, HarnessEntry, HarnessError, Subsystem},
    property::Status,
    record::EndRecord,
    Plan, RecordSink, RunReport,
};

/// One run of a fuzz input.
#[derive(Debug)]
pub struct ExploreRun {
    /// The scenario the input's prefix drew, serialized for
    /// `trace::record_failure` and triage bundles. [`run_bytes`] builds it
    /// only for a run [`classify`] may not call clean: one that reports a
    /// violation, failed, or abandoned tasks. `None` for any other run.
    pub scenario: Option<Value>,
    /// The run's report, or why the harness couldn't finish it.
    pub result: Result<RunReport, HarnessError>,
}

impl ExploreRun {
    /// The run's report, also for a run that ended at a harness failure.
    /// `None` if the harness couldn't run the scenario at all.
    pub fn report(&self) -> Option<&RunReport> {
        match &self.result {
            Ok(report) => Some(report),
            Err(HarnessError::Run { report, .. }) => Some(report),
            Err(_) => None,
        }
    }

    /// How many tasks the run's shutdown leaked when it abandoned the run.
    /// Nonzero exactly when the run was abandoned. The thread then still
    /// holds the leaked tasks and anything they share, so it must run no
    /// more inputs (#1694).
    pub fn leaked(&self) -> usize {
        self.report().map_or(0, |report| report.cleanup.leaked)
    }
}

/// Runs the full fuzz `input` on harness `S`, emitting its records to
/// `sink`. `extra` runs after the harness's checker, as in
/// [`run_harness`].
///
/// The first [`Subsystem::SCENARIO_BYTES`] bytes draw the scenario. A
/// shorter input is zero-padded to that length, so the harness never reads
/// past its prefix. The rest are the Plan bytes.
pub fn run_bytes<S: Subsystem>(
    input: &[u8],
    sink: &mut dyn RecordSink,
    extra: Option<&mut dyn Checker>,
) -> ExploreRun {
    let mut prefix = vec![0u8; S::SCENARIO_BYTES];
    let n = input.len().min(S::SCENARIO_BYTES);
    prefix[..n].copy_from_slice(&input[..n]);
    let scenario = S::draw(&prefix);
    let result = run_harness::<S>(&scenario, Plan::from_fuzz(&input[n..]), sink, extra);
    let unclean = match &result {
        Ok(report) => {
            report.cleanup.leaked > 0
                || report.end.violation.is_some()
                || report.properties.iter().any(violated)
        }
        Err(_) => true,
    };
    ExploreRun {
        scenario: unclean.then(|| {
            serde_json::to_value(&scenario)
                .expect("scenarios are plain data, and serde_json serializes plain data")
        }),
        result,
    }
}

/// Where a failure is filed (explorer.md, Outcome handling). Each bucket
/// means one thing, and none of them is a pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Bucket {
    /// An `Active` property was violated.
    Active,
    /// A `Known` property was violated.
    Known,
    /// A `Draft` property was violated.
    Draft,
    /// The simulation itself failed.
    Harness,
    /// The process failed: it aborted, a sanitizer reported an error, it ran
    /// out of memory, it timed out, a signal killed it, or it exited with a
    /// nonzero code (see [`ProcessFailure`]).
    Crash,
    /// Shutdown abandoned the run: a task got stuck in cleanup, and the run's
    /// tasks were leaked (#1694).
    Abandoned,
    /// A rerun of the input didn't do what the run before it did.
    Nondeterministic,
    /// The process exited with code 0 but wrote no `end` record, so the run
    /// never finished.
    NoEnd,
}

impl Bucket {
    /// The bucket's directory name, such as `known`, as it serializes.
    pub fn name(self) -> &'static str {
        match self {
            Bucket::Active => "active",
            Bucket::Known => "known",
            Bucket::Draft => "draft",
            Bucket::Harness => "harness",
            Bucket::Crash => "crash",
            Bucket::Abandoned => "abandoned",
            Bucket::Nondeterministic => "nondeterministic",
            Bucket::NoEnd => "no-end",
        }
    }
}

/// What the fuzz target does with a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExploreOutcome {
    /// Nothing failed: return.
    Clean,
    /// Crash the target: an `Active` property was violated, in
    /// `end.violation` or in any report in `end.properties`. A task panic
    /// violates `sim/no-task-panic`, which is `Active`.
    Fail {
        /// The violated property: `end.violation`'s, else the first
        /// `Active` report's.
        property: String,
    },
    /// Crash the target with `harness:`: the harness couldn't run the
    /// scenario, or the run ended at a harness failure.
    HarnessFailure {
        /// The failure.
        reason: String,
    },
    /// No property failed, but shutdown abandoned the run: a task got stuck
    /// in cleanup, and `leaked` tasks were leaked with everything they hold.
    /// Retire the thread the run used (#1694).
    Abandoned {
        /// Tasks leaked, from `cleanup.leaked`.
        leaked: usize,
    },
}

impl ExploreOutcome {
    /// Where the outcome's failure is filed: `active` for a failure,
    /// `harness` for a harness failure, `abandoned` for an abandoned run,
    /// and none for a clean one.
    pub fn bucket(&self) -> Option<Bucket> {
        match self {
            ExploreOutcome::Fail { .. } => Some(Bucket::Active),
            ExploreOutcome::HarnessFailure { .. } => Some(Bucket::Harness),
            ExploreOutcome::Abandoned { .. } => Some(Bucket::Abandoned),
            ExploreOutcome::Clean => None,
        }
    }
}

/// A run's outcome and the rest of its result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classified {
    /// What the target does.
    pub outcome: ExploreOutcome,
    /// Each `Known` and `Draft` property the run violated, with its bucket,
    /// in report order, whatever the outcome. After them, for a harness
    /// failure, the `Active` property [`ExploreOutcome::Fail`] would have
    /// named, under `active`. One entry per property, since a property's ID
    /// is its failure signature.
    pub notes: Vec<(String, Bucket)>,
    /// Tasks the run's shutdown leaked, whatever the outcome. Nonzero
    /// exactly when cleanup abandoned the run, so a run that failed and was
    /// abandoned keeps both.
    pub leaked: usize,
}

impl Classified {
    /// Every bucket the run files under, without repeats, in [`Bucket`]
    /// order: the outcome's, `abandoned` if cleanup abandoned the run, and
    /// each note's. Empty only for a clean run.
    pub fn buckets(&self) -> Vec<Bucket> {
        let mut buckets: Vec<Bucket> = self
            .outcome
            .bucket()
            .into_iter()
            .chain((self.leaked > 0).then_some(Bucket::Abandoned))
            .chain(self.notes.iter().map(|(_, bucket)| *bucket))
            .collect();
        buckets.sort();
        buckets.dedup();
        buckets
    }
}

/// Classifies `run` of `entry`'s harness.
///
/// A harness failure ranks first, and the run keeps its partial property
/// reports for diagnosis. Its `Active` violation, if any, joins
/// [`Classified::notes`]. Next, a violation in `end.violation`, or any
/// `Violated` report whose property is `Active`, is a failure. The second
/// case covers an obligation that expires in `at_end`, which the step loop
/// calls after it picks the run's end (review E-01). Then an abandoned run
/// is its own outcome. Every other run is clean. [`Classified::leaked`]
/// keeps an abandonment whatever the outcome.
///
/// `entry.status` names each property's status. A property the harness
/// doesn't register, such as one an extra checker reported, counts as
/// `Active`.
pub fn classify(entry: &HarnessEntry, run: &ExploreRun) -> Classified {
    match &run.result {
        Ok(report) => classify_parts(
            entry,
            report.end.harness.as_ref().map(ToString::to_string),
            report.end.violation.as_ref().map(|v| v.property.as_str()),
            &report.properties,
            report.cleanup.leaked,
        ),
        // The failure's own text, as the run's `end` record holds it, so a
        // fresh process's trace classifies the same.
        Err(HarnessError::Run { failure, report }) => classify_parts(
            entry,
            Some(failure.to_string()),
            report.end.violation.as_ref().map(|v| v.property.as_str()),
            &report.properties,
            report.cleanup.leaked,
        ),
        Err(error) => classify_parts(entry, Some(error.to_string()), None, &[], 0),
    }
}

/// Classifies the run that wrote `end`, by [`classify`]'s rules, so a trace
/// classifies as its run did.
pub fn classify_end(entry: &HarnessEntry, end: &EndRecord) -> Classified {
    classify_parts(
        entry,
        end.harness.clone(),
        end.violation.as_ref().map(|v| v.property.as_str()),
        &end.properties,
        end.cleanup.leaked,
    )
}

/// [`classify`] on a run's parts: its harness failure, its `end.violation`'s
/// property, its property reports and the tasks its shutdown leaked.
fn classify_parts(
    entry: &HarnessEntry,
    harness: Option<String>,
    violation: Option<&str>,
    properties: &[PropertyReport],
    leaked: usize,
) -> Classified {
    let status = |property: &str| entry.status(property).unwrap_or(Status::Active);
    let mut notes: Vec<(String, Bucket)> = properties
        .iter()
        .filter(|report| violated(report))
        .filter_map(|report| match status(&report.property) {
            Status::Active => None,
            Status::Known { .. } => Some((report.property.clone(), Bucket::Known)),
            Status::Draft => Some((report.property.clone(), Bucket::Draft)),
        })
        .collect();
    let failed = violation.map(str::to_owned).or_else(|| {
        properties
            .iter()
            .find(|report| violated(report) && status(&report.property) == Status::Active)
            .map(|report| report.property.clone())
    });
    let outcome = match (harness, failed) {
        (Some(reason), failed) => {
            notes.extend(failed.map(|property| (property, Bucket::Active)));
            ExploreOutcome::HarnessFailure { reason }
        }
        (None, Some(property)) => ExploreOutcome::Fail { property },
        (None, None) if leaked > 0 => ExploreOutcome::Abandoned { leaked },
        (None, None) => ExploreOutcome::Clean,
    };
    Classified {
        outcome,
        notes,
        leaked,
    }
}

/// True if `report` says its property was violated.
fn violated(report: &PropertyReport) -> bool {
    matches!(report.outcome, PropertyOutcome::Violated { .. })
}
