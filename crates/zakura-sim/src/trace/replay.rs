//! Exact replay: rerun a trace on an equivalent build and check that the run
//! does exactly what the trace recorded (traces.md, Replay).
//!
//! Replay drives the same step loop as recording. The decider answers every
//! pick and draw from the trace and checks it first, and a [`VerifySink`]
//! compares each record the replay emits with the recorded one. Setup's step
//! 0 observations have no digest in the trace, so replay can't check them.

use std::{
    fmt,
    fs::{File, OpenOptions},
    io::{self, Write},
};

use serde::Serialize;

use super::{
    verify::{end_difference, VerifySink},
    BuildInfo, Invalid, MetaRecord, Trace,
};
use crate::{
    check::PropertyReport,
    harness::{self, HarnessEntry, HarnessError},
    plan::TraceScript,
    record::EndRecord,
    Divergence, HarnessFailure, Observation, ObservationLog, PlanError, PlanInput, Record,
};

/// The full observation log `--observe` writes beside `trace.jsonl`.
/// Replay never reads it.
pub const OBSERVATIONS_FILE: &str = "observations.jsonl";

/// How to replay.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplayOptions {
    /// Replay on a build that isn't equivalent to the recording build, for
    /// diagnosis. Picks and draws still must match. The report is never a
    /// success.
    pub allow_build_mismatch: bool,
    /// Accept a trace without its `end` record, and replay it to its last
    /// step. A trace with `end` is still replayed in full. The report is
    /// never a success.
    pub prefix: bool,
    /// Write [`OBSERVATIONS_FILE`] into the trace directory. It fails if the
    /// file exists.
    pub observe: bool,
}

/// What a replay that ran to its end found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayReport {
    /// The replay's `end` record. `None` when a prefix stopped at its last
    /// step.
    pub end: Option<EndRecord>,
    /// The rerun's property reports with every violation, which `end`
    /// leaves out.
    pub properties: Vec<PropertyReport>,
    /// True if the trace had its `end` record.
    pub complete: bool,
    /// True if the build isn't equivalent to the recording build, which only
    /// [`ReplayOptions::allow_build_mismatch`] allows.
    pub build_mismatch: bool,
    /// Records that differed. Nonzero only on a build mismatch.
    pub obs_mismatches: u64,
    /// The first record that differed: its step, 0 for `init`, and field.
    pub first_obs_mismatch: Option<(u64, &'static str)>,
    /// The first `end` field that differed, such as `end.properties`. Set
    /// only on a build mismatch.
    pub end_mismatch: Option<&'static str>,
    /// The options it ran with.
    pub options: ReplayOptions,
}

impl ReplayReport {
    /// True only for a complete trace replayed on an equivalent build, with
    /// neither diagnostic option set, where every pick, draw, observation
    /// digest and `end` field matched. A matching violation is a success.
    /// An option can make a replay fail, but never makes it verified.
    pub fn verified(&self) -> bool {
        let ReplayOptions {
            allow_build_mismatch,
            prefix,
            observe: _,
        } = self.options;
        self.complete && !self.build_mismatch && !allow_build_mismatch && !prefix
    }
}

/// Why a replay failed.
#[derive(Debug)]
pub enum ReplayError {
    /// The build isn't equivalent to the one that recorded the trace.
    BuildMismatch {
        /// `meta.build`.
        recorded: Box<BuildInfo>,
        /// This build.
        current: Box<BuildInfo>,
    },
    /// The trace has no `end` record, or holds a value no run could record.
    InvalidTrace(Invalid),
    /// This build can't run what `meta` describes: no harness has its name,
    /// its scenario doesn't fit, or the harness describes the scenario
    /// differently.
    UnsupportedScenario {
        /// What doesn't fit, such as `meta.budget`.
        what: String,
    },
    /// The run left the trace's picks and draws.
    ScheduleDiverged(Divergence),
    /// The run took the trace's picks and draws but recorded something else.
    Nondeterministic {
        /// The step, 0 for `init`, or the run's last step for an `end` field.
        step: u64,
        /// The field that differed, such as `obs` or `end.properties`.
        field: &'static str,
    },
    /// The replay hit a harness failure the recording didn't.
    Harness(HarnessFailure),
    /// [`OBSERVATIONS_FILE`] couldn't be written.
    Io(io::Error),
}

impl fmt::Display for ReplayError {
    /// Names the error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplayError::BuildMismatch { recorded, current } => write!(
                f,
                "this build isn't equivalent to the one that recorded the trace \
                 (recorded {} dirty={} {}, current {} dirty={} {})",
                recorded.git,
                recorded.dirty,
                recorded.rustc,
                current.git,
                current.dirty,
                current.rustc
            ),
            ReplayError::InvalidTrace(invalid) => write!(f, "the trace is invalid: {invalid}"),
            ReplayError::UnsupportedScenario { what } => {
                write!(f, "this build can't run the trace's scenario: {what}")
            }
            ReplayError::ScheduleDiverged(divergence) => {
                write!(f, "the replay diverged at {divergence}")
            }
            ReplayError::Nondeterministic { step, field } => write!(
                f,
                "the replay is nondeterministic: step {step} recorded a different {field}"
            ),
            ReplayError::Harness(failure) => {
                write!(f, "the replay hit a harness failure: {failure}")
            }
            ReplayError::Io(error) => write!(f, "writing {OBSERVATIONS_FILE} failed: {error}"),
        }
    }
}

impl std::error::Error for ReplayError {}

/// Replays `trace` Exact on the harness its `meta.sim` names.
///
/// The build check comes first: on a build that isn't equivalent to
/// `meta.build`, replay fails before any harness code runs, unless
/// [`ReplayOptions::allow_build_mismatch`] is set.
pub fn replay(trace: &Trace, options: ReplayOptions) -> Result<ReplayReport, ReplayError> {
    replay_on(
        trace,
        options,
        registered(trace),
        &mut ObservationLog::default(),
        None,
    )
}

/// [`replay`] on `harness`, whatever `meta.sim` names, for harnesses outside
/// the registry, such as test-only ones.
pub fn replay_with(
    trace: &Trace,
    harness: &HarnessEntry,
    options: ReplayOptions,
) -> Result<ReplayReport, ReplayError> {
    replay_on(
        trace,
        options,
        || Ok(harness),
        &mut ObservationLog::default(),
        None,
    )
}

/// What a replay's run emitted, kept whether or not the replay succeeded.
#[derive(Clone, Debug, Default)]
pub(crate) struct ReplayRun {
    /// Every record the run emitted, in order. Empty if the replay failed
    /// before its run started, since every run emits `init`.
    pub(crate) records: Vec<Record>,
    /// The run's property reports with every violation.
    pub(crate) properties: Vec<PropertyReport>,
}

/// [`replay_with`] with default options, and what its run emitted.
pub(crate) fn replay_keeping(
    trace: &Trace,
    harness: &HarnessEntry,
) -> (Result<ReplayReport, ReplayError>, ReplayRun) {
    let mut kept = ReplayRun::default();
    let replayed = replay_on(
        trace,
        ReplayOptions::default(),
        || Ok(harness),
        &mut ObservationLog::default(),
        Some(&mut kept),
    );
    (replayed, kept)
}

/// [`replay`], then the trace as [`explain`](super::explain::explain)
/// renders it with the replay's observations.
pub fn replay_explained(
    trace: &Trace,
    options: ReplayOptions,
) -> Result<(ReplayReport, String), ReplayError> {
    let mut log = ObservationLog::default();
    let report = replay_on(trace, options, registered(trace), &mut log, None)?;
    Ok((report, super::explain::explain(trace, &log)))
}

/// [`replay_explained`] on `harness`, whatever `meta.sim` names.
pub fn replay_explained_with(
    trace: &Trace,
    harness: &HarnessEntry,
    options: ReplayOptions,
) -> Result<(ReplayReport, String), ReplayError> {
    let mut log = ObservationLog::default();
    let report = replay_on(trace, options, || Ok(harness), &mut log, None)?;
    Ok((report, super::explain::explain(trace, &log)))
}

/// The registered harness `trace`'s `meta.sim` names.
fn registered(trace: &Trace) -> impl FnOnce() -> Result<&'static HarnessEntry, ReplayError> + '_ {
    || {
        harness::find(&trace.meta.sim).map_err(|error| ReplayError::UnsupportedScenario {
            what: error.to_string(),
        })
    }
}

/// Checks the trace and build, then looks up the harness, checks `meta`
/// against it, and runs the replay. The run's observations go to `log`,
/// once the run ends. With `kept`, the run's records and property reports
/// go there too.
fn replay_on<'h>(
    trace: &Trace,
    options: ReplayOptions,
    harness: impl FnOnce() -> Result<&'h HarnessEntry, ReplayError>,
    log: &mut ObservationLog,
    kept: Option<&mut ReplayRun>,
) -> Result<ReplayReport, ReplayError> {
    let steps = trace.steps.last().map_or(0, |step| step.step);
    // A prefix that stops before `init` has nothing to replay.
    if (trace.end.is_none() && !options.prefix) || trace.init.is_none() {
        return Err(ReplayError::InvalidTrace(Invalid::Truncated { steps }));
    }
    let current = BuildInfo::current();
    let build_mismatch = !trace.meta.build.equivalent(&current);
    if build_mismatch && !options.allow_build_mismatch {
        return Err(ReplayError::BuildMismatch {
            recorded: Box::new(trace.meta.build.clone()),
            current: Box::new(current),
        });
    }
    let harness = harness()?;
    let rebuilt = (harness.meta)(&trace.meta.scenario).map_err(unsupported)?;
    if let Some(field) = meta_difference(&trace.meta, &rebuilt) {
        return Err(ReplayError::UnsupportedScenario {
            what: format!("the harness describes this scenario with a different {field}"),
        });
    }
    // Only a trace without `end` stops at its last step. A complete one is
    // replayed in full under `prefix` too.
    let script = TraceScript::new(trace, options.prefix && trace.end.is_none())
        .map_err(ReplayError::InvalidTrace)?;

    // Created before the run, so an existing log fails the replay up front
    // instead of hiding its result.
    let observations = options
        .observe
        .then(|| create_observations(trace))
        .transpose()
        .map_err(ReplayError::Io)?;

    let mut sink = VerifySink::new(trace, kept.is_some());
    let (mut report, failure) =
        match (harness.run)(&trace.meta.scenario, PlanInput::Trace(script), &mut sink) {
            Ok(report) => (report, None),
            Err(HarnessError::Run { failure, report }) => (*report, Some(failure)),
            Err(error) => return Err(unsupported(error)),
        };
    if let Some(kept) = kept {
        kept.records = std::mem::take(&mut sink.kept);
        kept.properties = report.properties.clone();
    }
    if let Some(file) = observations {
        write_observations(file, &report.log, report.end.steps).map_err(ReplayError::Io)?;
    }
    *log = std::mem::take(&mut report.log);

    let strict = !build_mismatch;
    match failure {
        Some(HarnessFailure::Plan(PlanError::Diverged(divergence))) => {
            // A record that already differed before the divergence is the
            // root cause on an equivalent build.
            return Err(match sink.first {
                Some((step, field)) if strict && step < divergence.step() => {
                    ReplayError::Nondeterministic { step, field }
                }
                _ => ReplayError::ScheduleDiverged(divergence),
            });
        }
        // `Sim::run` doesn't check for unreached entries after a harness
        // failure, so a failure before the trace's last step is caught here.
        Some(failure) if sink.steps_seen < trace.steps.len() => {
            return Err(match sink.first {
                Some((step, field)) if strict => ReplayError::Nondeterministic { step, field },
                _ => ReplayError::Harness(failure),
            });
        }
        Some(HarnessFailure::Plan(PlanError::PrefixEnd { .. })) => {
            return finish(
                strict,
                &sink,
                None,
                report.properties,
                false,
                build_mismatch,
                options,
            );
        }
        _ => {}
    }
    let produced = sink
        .end
        .clone()
        .expect("every run emits its end record, after its steps");
    let Some(recorded) = &trace.end else {
        // A prefix whose run ended on its own at the prefix's last step.
        return finish(
            strict,
            &sink,
            Some(produced),
            report.properties,
            false,
            build_mismatch,
            options,
        );
    };
    let end_mismatch = end_difference(recorded, &produced);
    if strict && end_mismatch.is_some() {
        if let Some(failure) = failure.filter(|f| recorded.harness != Some(f.to_string())) {
            return Err(ReplayError::Harness(failure));
        }
    }
    let mut report = finish(
        strict,
        &sink,
        Some(produced),
        report.properties,
        true,
        build_mismatch,
        options,
    )?;
    match end_mismatch {
        Some(field) if strict => Err(ReplayError::Nondeterministic {
            step: report.end.as_ref().map_or(0, |end| end.steps),
            field,
        }),
        _ => {
            report.end_mismatch = end_mismatch;
            Ok(report)
        }
    }
}

/// The report for a replay that kept to the trace's picks and draws, or
/// [`ReplayError::Nondeterministic`] for the first record that differed on
/// an equivalent build.
fn finish(
    strict: bool,
    sink: &VerifySink<'_>,
    end: Option<EndRecord>,
    properties: Vec<PropertyReport>,
    complete: bool,
    build_mismatch: bool,
    options: ReplayOptions,
) -> Result<ReplayReport, ReplayError> {
    if let (true, Some((step, field))) = (strict, sink.first) {
        return Err(ReplayError::Nondeterministic { step, field });
    }
    Ok(ReplayReport {
        end,
        properties,
        complete,
        build_mismatch,
        obs_mismatches: sink.mismatches,
        first_obs_mismatch: sink.first,
        end_mismatch: None,
        options,
    })
}

/// A harness error before the run as [`ReplayError::UnsupportedScenario`].
fn unsupported(error: HarnessError) -> ReplayError {
    ReplayError::UnsupportedScenario {
        what: error.to_string(),
    }
}

/// The first `meta` field where the harness's description of the recorded
/// scenario differs from the trace's. `build`, `input`, `fixtures` and
/// `derived_from` describe the recording, not the scenario, so they're left
/// out. So is `generator`, which only turns input bytes into a scenario: a
/// trace replays without it (traces.md, Versions).
fn meta_difference(recorded: &MetaRecord, rebuilt: &MetaRecord) -> Option<&'static str> {
    // Destructured, so a new field can't be left out of the comparison.
    let MetaRecord {
        format,
        contract,
        generator: _,
        build: _,
        sim,
        scenario,
        assumptions,
        scheduler,
        budget,
        rng,
        fixtures: _,
        properties,
        input: _,
        derived_from: _,
    } = recorded;
    [
        ("meta.format", *format == rebuilt.format),
        ("meta.contract", *contract == rebuilt.contract),
        ("meta.sim", *sim == rebuilt.sim),
        ("meta.scenario", *scenario == rebuilt.scenario),
        ("meta.assumptions", *assumptions == rebuilt.assumptions),
        ("meta.scheduler", *scheduler == rebuilt.scheduler),
        ("meta.budget", *budget == rebuilt.budget),
        ("meta.rng", *rng == rebuilt.rng),
        ("meta.properties", *properties == rebuilt.properties),
    ]
    .into_iter()
    .find_map(|(field, same)| (!same).then_some(field))
}

/// One line of [`OBSERVATIONS_FILE`]: a step's observations, in order. The
/// `obs` array serializes as the step's `obs` digest hashes it.
#[derive(Serialize)]
struct ObservationLine<'a> {
    step: u64,
    obs: Vec<&'a Observation>,
}

/// Creates [`OBSERVATIONS_FILE`] in `trace`'s directory. Fails if anything,
/// a symlink included, is already there.
fn create_observations(trace: &Trace) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(trace.dir.join(OBSERVATIONS_FILE))
}

/// Writes `log`'s observations for steps 0 to `steps` to `file`, one line
/// per step.
fn write_observations(mut file: File, log: &ObservationLog, steps: u64) -> io::Result<()> {
    let mut out = Vec::new();
    for step in 0..=steps {
        let line = ObservationLine {
            step,
            obs: log.of_step(step).collect(),
        };
        serde_json::to_writer(&mut out, &line)?;
        out.push(b'\n');
    }
    file.write_all(&out)?;
    file.sync_all()
}
