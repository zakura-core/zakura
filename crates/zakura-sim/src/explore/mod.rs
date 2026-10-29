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

mod worker;

use serde::Serialize;
use serde_json::Value;

pub use worker::Worker;

use crate::{
    check::{Checker, PropertyOutcome, PropertyReport},
    harness::{run_harness, HarnessEntry, HarnessError, Subsystem},
    property::Status,
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

/// Where a failure is filed (explorer.md, Outcome handling).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
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
    /// The process crashed without an `end` record.
    Crash,
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
    /// `harness` for a harness failure, and none for the rest.
    pub fn bucket(&self) -> Option<Bucket> {
        match self {
            ExploreOutcome::Fail { .. } => Some(Bucket::Active),
            ExploreOutcome::HarnessFailure { .. } => Some(Bucket::Harness),
            ExploreOutcome::Clean | ExploreOutcome::Abandoned { .. } => None,
        }
    }
}

/// A run's outcome and its notes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classified {
    /// What the target does.
    pub outcome: ExploreOutcome,
    /// Each `Known` and `Draft` property the run violated, with its bucket,
    /// in report order, whatever the outcome. One entry per property, since
    /// a property's ID is its failure signature.
    pub notes: Vec<(String, Bucket)>,
}

/// Classifies `run` of `entry`'s harness.
///
/// A harness failure ranks first, and the run keeps its partial property
/// reports for diagnosis. Next, a violation in `end.violation`, or any
/// `Violated` report whose property is `Active`, is a failure. The second
/// case covers an obligation that expires in `at_end`, which the step loop
/// calls after it picks the run's end (review E-01). Then an abandoned run
/// is its own outcome. Every other run is clean.
///
/// `entry.status` names each property's status. A property the harness
/// doesn't register, such as one an extra checker reported, counts as
/// `Active`.
pub fn classify(entry: &HarnessEntry, run: &ExploreRun) -> Classified {
    let status = |property: &str| entry.status(property).unwrap_or(Status::Active);
    let notes = run
        .report()
        .map(|report| {
            report
                .properties
                .iter()
                .filter(|report| violated(report))
                .filter_map(|report| match status(&report.property) {
                    Status::Active => None,
                    Status::Known { .. } => Some((report.property.clone(), Bucket::Known)),
                    Status::Draft => Some((report.property.clone(), Bucket::Draft)),
                })
                .collect()
        })
        .unwrap_or_default();
    let outcome = match &run.result {
        Err(error) => ExploreOutcome::HarnessFailure {
            reason: error.to_string(),
        },
        // `run_harness` returns a harness end as an error, so only a report
        // built elsewhere gets here with one.
        Ok(report) => match &report.end.harness {
            Some(failure) => ExploreOutcome::HarnessFailure {
                reason: failure.to_string(),
            },
            None => match failed(report, status) {
                Some(property) => ExploreOutcome::Fail { property },
                None if report.cleanup.leaked > 0 => ExploreOutcome::Abandoned {
                    leaked: report.cleanup.leaked,
                },
                None => ExploreOutcome::Clean,
            },
        },
    };
    Classified { outcome, notes }
}

/// The property `report`'s run failed: `end.violation`'s, else the first
/// `Active` property reported `Violated`.
fn failed(report: &RunReport, status: impl Fn(&str) -> Status) -> Option<String> {
    let property = match &report.end.violation {
        Some(violation) => &violation.property,
        None => {
            &report
                .properties
                .iter()
                .find(|report| violated(report) && status(&report.property) == Status::Active)?
                .property
        }
    };
    Some(property.clone())
}

/// True if `report` says its property was violated.
fn violated(report: &PropertyReport) -> bool {
    matches!(report.outcome, PropertyOutcome::Violated { .. })
}
