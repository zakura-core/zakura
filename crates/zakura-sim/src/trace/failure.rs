//! Recording a failure the fuzz loop found (traces.md, Trace directory).
//!
//! The fuzz loop runs each input with a [`crate::NullSink`], so a failing
//! run leaves no trace and no `obs` digests. [`record_failure`] reruns the
//! input twice, each on a new thread. The first rerun records the trace and
//! must violate the same property at the same step: the failure-consistency
//! check. The second rerun replays that trace Exact: the determinism check.
//! A trace that passes both is saved under the property's directory, named
//! by its ID. A trace that fails either check is kept as evidence of
//! nondeterminism, with the records and property reports of the run it was
//! compared against and the first difference between them.

use std::{
    env, fs, io,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

use serde_json::{json, Value};

use super::{
    replay::{replay_keeping, ReplayRun},
    replay_with,
    verify::{end_difference, trace_difference},
    FileSink, ReplayError, ReplayOptions, ReplayReport, Trace, TraceReader, BLOBS_DIR, INPUT_FILE,
    TRACE_FILE,
};
use crate::{
    check::{PropertyOutcome, PropertyReport},
    harness::{write_trace, HarnessEntry, HarnessError, RecordError},
    property::PropertyId,
    Plan, RecordSink, RunReport,
};

/// The variable that names the directory saved failures go under.
pub const ARTIFACTS_VAR: &str = "ZAKURA_SIM_ARTIFACTS";

/// The directory under the artifacts root that holds saved traces.
pub const TRACES_DIR: &str = "traces";

/// The directory under the artifacts root that keeps the evidence of each
/// nondeterministic rerun.
pub const NONDETERMINISTIC_DIR: &str = "nondeterministic";

/// The recording rerun's trace, in a nondeterminism directory.
pub const RECORDING_DIR: &str = "recording";

/// The records of the Exact replay that disagreed with the recording, in a
/// nondeterminism directory.
pub const REPLAY_DIR: &str = "replay";

/// The first run's `end` record, in a nondeterminism directory.
pub const FIRST_END_FILE: &str = "first-end.json";

/// Each compared run's property reports, with every violation, in a
/// nondeterminism directory.
pub const PROPERTIES_FILE: &str = "properties.json";

/// The check that failed and the first difference it found, in a
/// nondeterminism directory.
pub const DIVERGENCE_FILE: &str = "divergence.json";

/// Characters of the trace ID that name a saved trace's directory.
const ID_PREFIX: usize = 12;

/// The partial directory's number within this process.
static NEXT_PARTIAL: AtomicU64 = AtomicU64::new(0);

/// Where saved failures go: `$ZAKURA_SIM_ARTIFACTS`, or `zakura-sim` under
/// the temporary directory (`$TMPDIR` on Unix) when the variable is unset or
/// empty.
pub fn artifacts_root() -> PathBuf {
    match env::var_os(ARTIFACTS_VAR) {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => env::temp_dir().join("zakura-sim"),
    }
}

/// Records the failure `first` found, and returns the saved trace.
///
/// `first` is the fast run of `input`, the full fuzz input, on `harness`,
/// and `scenario` is the scenario that run drew from `input`'s prefix.
/// `first` must report `property` `Violated` in its property reports, so a
/// `Known` violation that didn't end the run is recorded too.
///
/// Each rerun builds a fresh `Sim` on a new thread. A thread whose run
/// leaked tasks parks for good, because the leaked tasks can still hold its
/// thread-local state (#1694). So a run that leaked tasks, on the caller's
/// thread or in the first rerun, never breaks a later rerun:
///
/// 1. The first rerun writes the trace, with `input` as its `input.bin`. It
///    must report `property` `Violated` at the step `first` did.
/// 2. The second rerun replays that trace Exact on this build. Every pick,
///    draw, `obs` digest and `end` field must match, and so must every
///    property's violation evidence.
///
/// A trace that passes both moves to
/// `out_root/traces/<property-id>/<first 12 hex of the trace ID>/`, with
/// each segment of the property ID a directory. If that directory already
/// holds the same trace, the new copy is deleted and the saved one is
/// returned.
///
/// Either failure returns [`RecordError::Nondeterministic`], and keeps its
/// evidence in
/// `out_root/nondeterministic/<property-id>/<first 12 hex of the trace ID>/`:
///
/// - `recording/`, the first rerun's trace;
/// - `replay/`, for a failed replay check, the records the replay emitted,
///   after the recording's `meta`, unless the replay failed before its run
///   started;
/// - `first-end.json`, the `end` record of `first`, which wrote no trace;
/// - `properties.json`, the property reports of `first`, the recording and
///   the replay (`null` if no replay ran);
/// - `divergence.json`, which names the failed check (`recording` or
///   `replay`) and its detail, the first `end` field where `first` and the
///   recording differ, the first record where the recording and the replay
///   differ, and any error writing or reading the replay's records.
pub fn record_failure(
    harness: &'static HarnessEntry,
    scenario: &Value,
    input: &[u8],
    property: &PropertyId,
    first: &RunReport,
    out_root: &Path,
) -> Result<Trace, RecordError> {
    let Some(step) = violated_at(first, property) else {
        return Err(RecordError::NotViolated {
            property: property.to_string(),
        });
    };
    let parent = property
        .as_str()
        .split('/')
        .fold(out_root.join(TRACES_DIR), |dir, segment| dir.join(segment));
    fs::create_dir_all(&parent).map_err(RecordError::Io)?;
    let partial = parent.join(format!(
        ".partial-{}-{}",
        process::id(),
        NEXT_PARTIAL.fetch_add(1, Ordering::Relaxed)
    ));
    // Only a crashed process with this process's ID could have left one.
    remove(&partial).map_err(RecordError::Io)?;
    let checked = record_and_replay(harness, scenario, input, property, step, &partial);
    let error = match checked {
        Ok(trace) => return save(&trace, &partial, &parent),
        Err(Unchecked::Diverged(diverged)) => {
            let Runs {
                check,
                detail,
                recording,
                recorded,
                replay,
            } = *diverged;
            let diverged = Diverged {
                first,
                recording: &recording,
                recorded: &recorded,
                replay: replay.as_ref(),
                check,
            };
            match keep_divergence(
                &diverged,
                &detail,
                &partial,
                &property_dir(out_root, property),
            ) {
                Ok(dir) => RecordError::Nondeterministic {
                    detail,
                    evidence: Some(dir),
                },
                Err(error) => RecordError::Nondeterministic {
                    detail: format!("{detail}; saving the evidence failed: {error}"),
                    evidence: None,
                },
            }
        }
        Err(Unchecked::Error(error)) => error,
    };
    // The run's error matters more than a failed cleanup.
    let _ = remove(&partial);
    Err(error)
}

/// `out_root/nondeterministic/<property-id>/`, with each segment of the
/// property ID a directory.
fn property_dir(out_root: &Path, property: &PropertyId) -> PathBuf {
    property
        .as_str()
        .split('/')
        .fold(out_root.join(NONDETERMINISTIC_DIR), |dir, segment| {
            dir.join(segment)
        })
}

/// Why [`record_and_replay`] checked no trace.
enum Unchecked {
    /// The recording failed.
    Error(RecordError),
    /// A rerun didn't do what the run before it did. Boxed, since the runs
    /// are far larger than an error.
    Diverged(Box<Runs>),
}

/// The reruns of a failed check.
struct Runs {
    /// The check that failed.
    check: Check,
    /// What differed.
    detail: String,
    /// The recording rerun's trace.
    recording: Trace,
    /// The recording rerun's property reports.
    recorded: Vec<PropertyReport>,
    /// What the replay emitted, for a failed replay check.
    replay: Option<ReplayRun>,
}

/// A check [`record_failure`] runs on a rerun.
#[derive(Clone, Copy)]
enum Check {
    /// The recording rerun must violate the property at the first run's
    /// step.
    Recording,
    /// The Exact replay of the recording must match it.
    Replay,
}

impl Check {
    /// The check's name in `divergence.json`.
    fn name(self) -> &'static str {
        match self {
            Check::Recording => "recording",
            Check::Replay => "replay",
        }
    }
}

/// Records `input` to `partial`, checks that the recording run violates
/// `property` at `step`, then replays the trace Exact.
fn record_and_replay(
    harness: &'static HarnessEntry,
    scenario: &Value,
    input: &[u8],
    property: &PropertyId,
    step: u64,
    partial: &Path,
) -> Result<Trace, Unchecked> {
    let (trace, recorded) =
        record_on_new_thread(harness, scenario, input, partial).map_err(Unchecked::Error)?;
    let found = violated_at(&recorded, property);
    if found != Some(step) {
        return Err(Unchecked::Diverged(Box::new(Runs {
            check: Check::Recording,
            detail: format!(
                "the first run violated {property} at step {step}, and the recording run {}",
                match found {
                    Some(other) => format!("at step {other}"),
                    None => "didn't".to_owned(),
                }
            ),
            recording: trace,
            recorded: recorded.properties,
            replay: None,
        })));
    }
    let (replayed, replay) = replay_keeping_on_new_thread(trace.clone(), harness);
    let detail = match replayed {
        Err(error) => format!("the replay of the recording failed: {error}"),
        Ok(replayed) if !replayed.verified() => {
            "the replay of the recording wasn't verified".to_owned()
        }
        Ok(replayed) if replayed.properties != recorded.properties => {
            "the replay found different violation evidence".to_owned()
        }
        Ok(_) => return Ok(trace),
    };
    Err(Unchecked::Diverged(Box::new(Runs {
        check: Check::Replay,
        detail,
        recording: trace,
        recorded: recorded.properties,
        replay: Some(replay),
    })))
}

/// Records `input`, the full fuzz input, against `scenario` on `harness` to
/// `out`, on a new thread with [`on_new_thread`]'s isolation. Returns the
/// trace and the run's report, also for a run that ended at a harness
/// failure.
fn record_on_new_thread(
    harness: &'static HarnessEntry,
    scenario: &Value,
    input: &[u8],
    out: &Path,
) -> Result<(Trace, RunReport), RecordError> {
    let (scenario, input, out) = (scenario.clone(), input.to_vec(), out.to_path_buf());
    on_new_thread(move || {
        let plan = Plan::from_fuzz(&input[harness.scenario_bytes.min(input.len())..]);
        let written = write_trace(
            harness,
            &scenario,
            plan,
            Some(&input),
            None,
            &out,
            |scenario, plan, sink| match (harness.run)(scenario, plan, sink) {
                Ok(report) => Ok(report),
                Err(HarnessError::Run { report, .. }) => Ok(*report),
                Err(error) => Err(RecordError::Harness(error)),
            },
        );
        let clean = matches!(&written, Ok((_, recorded)) if recorded.cleanup.leaked == 0);
        (written, clean)
    })
}

/// A failed check of [`record_failure`]'s reruns, with what it compared.
struct Diverged<'a> {
    /// The first run, which wrote no trace.
    first: &'a RunReport,
    /// The recording rerun's trace, still at the partial directory.
    recording: &'a Trace,
    /// The recording rerun's property reports.
    recorded: &'a [PropertyReport],
    /// What the replay emitted, for a failed replay check.
    replay: Option<&'a ReplayRun>,
    check: Check,
}

/// Moves the recording at `partial` into a new evidence directory under
/// `parent`, beside the replay's records, the first run's `end` record,
/// each run's property reports and `divergence.json`, as [`record_failure`]
/// describes.
/// Returns the directory. An evidence directory that already holds the same
/// recording is kept, and the new copy deleted.
fn keep_divergence(
    diverged: &Diverged<'_>,
    detail: &str,
    partial: &Path,
    parent: &Path,
) -> io::Result<PathBuf> {
    fs::create_dir_all(parent)?;
    let staging = parent.join(format!(
        ".partial-{}-{}",
        process::id(),
        NEXT_PARTIAL.fetch_add(1, Ordering::Relaxed)
    ));
    // Only a crashed process with this process's ID could have left one.
    remove(&staging)?;
    fs::create_dir(&staging)?;
    if let Err(error) = write_divergence(diverged, detail, partial, &staging) {
        let _ = remove(&staging);
        return Err(error);
    }
    let dir = parent.join(&diverged.recording.id[..ID_PREFIX]);
    if let Err(error) = fs::rename(&staging, &dir) {
        let _ = remove(&staging);
        // A rename fails onto a directory that holds files: the same
        // recording, kept by an earlier failure.
        if fs::symlink_metadata(dir.join(DIVERGENCE_FILE)).is_err() {
            return Err(error);
        }
    }
    Ok(dir)
}

/// Fills `staging` with [`keep_divergence`]'s evidence.
fn write_divergence(
    diverged: &Diverged<'_>,
    detail: &str,
    partial: &Path,
    staging: &Path,
) -> io::Result<()> {
    fs::rename(partial, staging.join(RECORDING_DIR))?;
    let recording = diverged.recording;
    // An error writing the replay's records goes in `divergence.json`, and
    // the rest of the evidence is kept.
    let replayed = diverged
        .replay
        .filter(|replay| !replay.records.is_empty())
        .map(|replay| {
            write_replay(
                recording,
                &staging.join(RECORDING_DIR),
                replay,
                &staging.join(REPLAY_DIR),
            )
        });
    let replay_difference = replayed
        .as_ref()
        .and_then(|replayed| replayed.as_ref().ok())
        .and_then(|replayed| trace_difference(recording, replayed))
        .map(|(step, field)| json!({ "step": step, "field": field }));
    let first = diverged.first.end_record();
    let divergence = json!({
        "check": diverged.check.name(),
        "detail": detail,
        "end_difference": recording.end.as_ref().and_then(|end| end_difference(&first, end)),
        "replay_difference": replay_difference,
        "replay_error": replayed.and_then(Result::err),
    });
    let properties = json!({
        "first": reports_json(&diverged.first.properties),
        "recording": reports_json(diverged.recorded),
        "replay": diverged.replay.map(|replay| reports_json(&replay.properties)),
    });
    let first = serde_json::to_string(&first).map_err(io::Error::other)?;
    fs::write(staging.join(FIRST_END_FILE), format!("{first}\n"))?;
    fs::write(staging.join(PROPERTIES_FILE), format!("{properties}\n"))?;
    fs::write(staging.join(DIVERGENCE_FILE), format!("{divergence}\n"))
}

/// `reports` as JSON, each with its `violations`, which a report's own
/// serialization leaves out.
fn reports_json(reports: &[PropertyReport]) -> Value {
    reports
        .iter()
        .map(|report| {
            let mut value = json!(report);
            value["violations"] = json!(report.violations);
            value
        })
        .collect()
}

/// Writes `replay`'s records to `dir` as a trace, after `recording`'s
/// `meta`, beside copies of the `input.bin` and fixtures in `recorded`,
/// the recording's directory, and reads it back. A replay emits no `meta`,
/// and replays `recording`'s. Returns why the trace couldn't be written or
/// read.
fn write_replay(
    recording: &Trace,
    recorded: &Path,
    replay: &ReplayRun,
    dir: &Path,
) -> Result<Trace, String> {
    let mut sink = FileSink::create(dir, &recording.meta).map_err(|error| error.to_string())?;
    for record in &replay.records {
        sink.record(record.clone());
    }
    // A record past the size limits ends the file early, and the prefix
    // before it is still evidence, so the artifacts go in first.
    let finished = sink.finish();
    copy_artifacts(recorded, dir).map_err(|error| error.to_string())?;
    finished.map_err(|error| error.to_string())?;
    TraceReader::open_prefix(dir).map_err(|error| error.to_string())
}

/// Copies `from`'s `input.bin` and `blobs/` files, those that exist, to
/// `to`.
fn copy_artifacts(from: &Path, to: &Path) -> io::Result<()> {
    if fs::symlink_metadata(from.join(INPUT_FILE)).is_ok() {
        fs::copy(from.join(INPUT_FILE), to.join(INPUT_FILE))?;
    }
    let blobs = match fs::read_dir(from.join(BLOBS_DIR)) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        blobs => blobs?,
    };
    fs::create_dir_all(to.join(BLOBS_DIR))?;
    for blob in blobs {
        let blob = blob?;
        fs::copy(blob.path(), to.join(BLOBS_DIR).join(blob.file_name()))?;
    }
    Ok(())
}

/// Replays `trace` Exact on `harness` on a new thread, with
/// [`on_new_thread`]'s isolation.
pub(super) fn replay_on_new_thread(
    trace: Trace,
    harness: &'static HarnessEntry,
) -> Result<ReplayReport, ReplayError> {
    on_new_thread(move || {
        let replayed = replay_with(&trace, harness, ReplayOptions::default());
        let clean = matches!(&replayed, Ok(report)
            if report.end.as_ref().is_some_and(|end| end.cleanup.leaked == 0));
        (replayed, clean)
    })
}

/// [`replay_on_new_thread`], also returning what the replay's run emitted.
fn replay_keeping_on_new_thread(
    trace: Trace,
    harness: &'static HarnessEntry,
) -> (Result<ReplayReport, ReplayError>, ReplayRun) {
    on_new_thread(move || {
        let (replayed, kept) = replay_keeping(&trace, harness);
        let clean = matches!(&replayed, Ok(report)
            if report.end.as_ref().is_some_and(|end| end.cleanup.leaked == 0));
        ((replayed, kept), clean)
    })
}

/// Runs `job` on a new thread and returns its value. `job` also returns
/// whether its run ended with no leaked task. If not, or if `job` panicked,
/// the thread parks for good, so nothing the leaked tasks hold is dropped or
/// reused. A panic resumes on the caller's thread.
pub(super) fn on_new_thread<T: Send + 'static>(
    job: impl FnOnce() -> (T, bool) + Send + 'static,
) -> T {
    let (done, result) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("zakura-sim-run".to_owned())
        .spawn(move || {
            let outcome = panic::catch_unwind(AssertUnwindSafe(job));
            let clean = matches!(outcome, Ok((_, true)));
            // The caller waits on this, so the send can't fail.
            let _ = done.send(outcome.map(|(value, _)| value));
            if !clean {
                loop {
                    thread::park();
                }
            }
        })
        .expect("the OS can start a thread");
    match result
        .recv()
        .expect("the thread sends its result before it parks or exits")
    {
        Ok(value) => value,
        Err(payload) => panic::resume_unwind(payload),
    }
}

/// The step of `property`'s first violation in `report`, if `report` says
/// it was violated.
fn violated_at(report: &RunReport, property: &PropertyId) -> Option<u64> {
    report
        .properties
        .iter()
        .find(|report| report.property == property.as_str())
        .filter(|report| matches!(report.outcome, PropertyOutcome::Violated { .. }))
        .and_then(|report| report.violations.first())
        .map(|violation| violation.step)
}

/// Moves the checked trace at `partial` into `parent`, named by its ID, and
/// returns it as read from there.
fn save(trace: &Trace, partial: &Path, parent: &Path) -> Result<Trace, RecordError> {
    let dir = parent.join(&trace.id[..ID_PREFIX]);
    if let Err(error) = fs::rename(partial, &dir) {
        // A rename fails onto a directory that holds files. That's the same
        // trace only if its ID matches.
        let _ = remove(partial);
        let same = fs::symlink_metadata(dir.join(TRACE_FILE)).is_ok()
            && TraceReader::open(&dir).is_ok_and(|saved| saved.id == trace.id);
        if !same {
            return Err(RecordError::Io(error));
        }
    }
    TraceReader::open(&dir).map_err(RecordError::Read)
}

/// Deletes `dir` and everything in it, if it exists.
fn remove(dir: &Path) -> io::Result<()> {
    match fs::remove_dir_all(dir) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}
