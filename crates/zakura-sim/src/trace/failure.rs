//! Recording a failure the fuzz loop found (traces.md, Trace directory).
//!
//! The fuzz loop runs each input with a [`crate::NullSink`], so a failing
//! run leaves no trace and no `obs` digests. [`record_failure`] reruns the
//! input twice, each on a new thread. The first rerun records the trace and
//! must violate the same property at the same step: the failure-consistency
//! check. The second rerun replays that trace Exact: the determinism check.
//! A trace that passes both is saved under the property's directory, named
//! by its ID.

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

use serde_json::Value;

use super::{replay_with, ReplayOptions, Trace, TraceReader, TRACE_FILE};
use crate::{
    check::PropertyOutcome,
    harness::{write_trace, HarnessEntry, HarnessError, RecordError},
    property::PropertyId,
    RunReport,
};

/// The variable that names the directory saved failures go under.
pub const ARTIFACTS_VAR: &str = "ZAKURA_SIM_ARTIFACTS";

/// The directory under the artifacts root that holds saved traces.
pub const TRACES_DIR: &str = "traces";

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
/// Either failure deletes the partial directory and returns
/// [`RecordError::Nondeterministic`]. A trace that passes both moves to
/// `out_root/traces/<property-id>/<first 12 hex of the trace ID>/`, with
/// each segment of the property ID a directory. If that directory already
/// holds the same trace, the new copy is deleted and the saved one is
/// returned.
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
    match checked {
        Ok(trace) => save(&trace, &partial, &parent),
        Err(error) => {
            // The run's error matters more than a failed cleanup.
            let _ = remove(&partial);
            Err(error)
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
) -> Result<Trace, RecordError> {
    let (scenario, input, partial) = (scenario.clone(), input.to_vec(), partial.to_path_buf());
    let (trace, recorded) = on_new_thread(move || {
        let written = write_trace(
            harness,
            &scenario,
            &input[harness.scenario_bytes.min(input.len())..],
            Some(&input),
            &partial,
            |scenario, plan, sink| match (harness.run)(scenario, plan, sink) {
                Ok(report) => Ok(report),
                Err(HarnessError::Run { report, .. }) => Ok(*report),
                Err(error) => Err(RecordError::Harness(error)),
            },
        );
        let clean = matches!(&written, Ok((_, recorded)) if recorded.cleanup.leaked == 0);
        (written, clean)
    })?;
    let found = violated_at(&recorded, property);
    if found != Some(step) {
        return Err(RecordError::Nondeterministic {
            detail: format!(
                "the first run violated {property} at step {step}, and the recording run {}",
                match found {
                    Some(other) => format!("at step {other}"),
                    None => "didn't".to_owned(),
                }
            ),
        });
    }
    let recording = trace.clone();
    let replayed = on_new_thread(move || {
        let replayed = replay_with(&recording, harness, ReplayOptions::default());
        let clean = matches!(&replayed, Ok(report)
            if report.end.as_ref().is_some_and(|end| end.cleanup.leaked == 0));
        (replayed, clean)
    })
    .map_err(|error| RecordError::Nondeterministic {
        detail: format!("the replay of the recording failed: {error}"),
    })?;
    if !replayed.verified() {
        return Err(RecordError::Nondeterministic {
            detail: "the replay of the recording wasn't verified".to_owned(),
        });
    }
    if replayed.properties != recorded.properties {
        return Err(RecordError::Nondeterministic {
            detail: "the replay found different violation evidence".to_owned(),
        });
    }
    Ok(trace)
}

/// Runs `job` on a new thread and returns its value. `job` also returns
/// whether its run ended with no leaked task. If not, or if `job` panicked,
/// the thread parks for good, so nothing the leaked tasks hold is dropped or
/// reused. A panic resumes on the caller's thread.
fn on_new_thread<T: Send + 'static>(job: impl FnOnce() -> (T, bool) + Send + 'static) -> T {
    let (done, result) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("zakura-sim-record".to_owned())
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
