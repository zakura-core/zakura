//! Record mode: one run of a harness, written as a trace directory.

use std::{
    fmt,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{HarnessEntry, HarnessError};
use crate::{
    trace::{
        meta::{hex, Derivation, InputRef},
        read::refuse_symlinked_dir,
        FileSink, ReadError, Trace, TraceReader, INPUT_FILE, MAX_INPUT_BYTES, TRACE_FILE,
    },
    witness::{Expect, WitnessError},
    Plan, PlanInput, RecordSink,
};

/// The file beside a trace that [`record_witness`]'s callers write its
/// witness test's result to.
pub const WITNESS_FILE: &str = "witness.json";

/// What [`record`] runs.
#[derive(Clone, Copy, Debug)]
pub enum RecordInput<'a> {
    /// A full fuzz input: the harness's scenario prefix, then the Plan
    /// bytes. It's saved as `input.bin`.
    Input(&'a [u8]),
    /// Plan bytes against a given scenario. No `input.bin` is written.
    Plan {
        /// The serialized scenario.
        scenario: &'a Value,
        /// The Plan bytes.
        plan: &'a [u8],
    },
    /// A scripted Plan against a given scenario. No `input.bin` is written.
    Script {
        /// The serialized scenario.
        scenario: &'a Value,
        /// The Plan.
        plan: &'a Plan,
        /// The trace the Plan came from, for `meta.derived_from`.
        derived_from: Option<&'a Derivation>,
    },
}

/// Why [`record`] wrote no complete trace.
#[derive(Debug)]
pub enum RecordError {
    /// The scenario didn't fit the harness.
    Harness(HarnessError),
    /// The input is longer than `input.bin` may be, which the reader would
    /// refuse.
    InputTooLarge {
        /// Its length.
        len: usize,
    },
    /// `out` is a symlink, which the reader would refuse. Nothing was
    /// written.
    Refused(ReadError),
    /// Writing the trace failed, as when `out` already holds one.
    Io(io::Error),
    /// The written trace didn't read back.
    Read(ReadError),
    /// The witness test couldn't run: the harness doesn't register the
    /// property, or the property has no trigger-history check.
    Witness(WitnessError),
    /// The run [`crate::trace::record_failure`] was handed doesn't violate
    /// the property it was asked to record.
    NotViolated {
        /// The property.
        property: String,
    },
    /// A rerun of a failing run didn't do what that run did, so
    /// [`crate::trace::record_failure`] saved no trace under `traces/`.
    Nondeterministic {
        /// What differed.
        detail: String,
        /// The directory that keeps the compared records and their first
        /// difference, or `None` if saving them failed.
        evidence: Option<PathBuf>,
    },
}

impl fmt::Display for RecordError {
    /// Names the error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordError::Harness(error) => error.fmt(f),
            RecordError::InputTooLarge { len } => write!(
                f,
                "the input is {len} bytes, and input.bin holds at most {MAX_INPUT_BYTES}"
            ),
            RecordError::Refused(error) => write!(f, "the trace can't go there: {error}"),
            RecordError::Io(error) => write!(f, "writing the trace failed: {error}"),
            RecordError::Read(error) => write!(f, "the trace didn't read back: {error}"),
            RecordError::Witness(error) => write!(f, "the witness test can't run: {error}"),
            RecordError::NotViolated { property } => {
                write!(f, "the run doesn't violate {property}")
            }
            RecordError::Nondeterministic {
                detail,
                evidence: Some(dir),
            } => write!(
                f,
                "the rerun is nondeterministic: {detail} (evidence in {})",
                dir.display()
            ),
            RecordError::Nondeterministic {
                detail,
                evidence: None,
            } => write!(f, "the rerun is nondeterministic: {detail}"),
        }
    }
}

impl std::error::Error for RecordError {}

/// Runs `what` on `harness` and writes the trace to `out`, which must not
/// already hold one. Returns the trace as the reader reads it back.
///
/// `out` gets the reader's directory check before anything is written, so a
/// symlinked `out` is refused with nothing written through it. So is an `out`
/// that already holds a `trace.jsonl`, which keeps an old trace's files as its
/// meta names them.
///
/// For a full input, the first [`HarnessEntry::scenario_bytes`] bytes
/// decode the scenario. An input shorter than that reads the missing
/// scenario bytes as 0. `input.bin` is written before `trace.jsonl`, so a
/// trace that names it never lacks it.
///
/// A run that ends in a harness failure still writes a complete trace, whose
/// `end` record says so.
pub fn record(
    harness: &HarnessEntry,
    what: RecordInput<'_>,
    out: &Path,
) -> Result<Trace, RecordError> {
    let (trace, ()) = record_with(harness, what, out, |scenario, plan, sink| {
        match (harness.run)(scenario, plan, sink) {
            Ok(_) | Err(HarnessError::Run { .. }) => Ok(()),
            Err(error) => Err(RecordError::Harness(error)),
        }
    })?;
    Ok(trace)
}

/// [`record`], judging the run as a witness test of `property` that
/// expects `expect`, with the rule [`crate::witness::witness`] applies.
/// Returns the trace and the test's result. A run the test fails still
/// writes its trace, as evidence.
pub fn record_witness(
    harness: &HarnessEntry,
    what: RecordInput<'_>,
    out: &Path,
    property: &str,
    expect: Expect,
) -> Result<(Trace, Result<(), WitnessError>), RecordError> {
    if harness.status(property).is_none() {
        return Err(RecordError::Witness(WitnessError::Unregistered));
    }
    record_with(harness, what, out, |scenario, plan, sink| {
        match (harness.witness)(scenario, plan, property, expect, sink) {
            Err(error @ WitnessError::NoTriggerHistory) => Err(RecordError::Witness(error)),
            result => Ok(result),
        }
    })
}

/// Writes the trace of `run` on `what`'s scenario and Plan to `out`, as
/// [`record`] describes, and returns it with `run`'s result.
fn record_with<T>(
    harness: &HarnessEntry,
    what: RecordInput<'_>,
    out: &Path,
    run: impl FnOnce(&Value, PlanInput, &mut dyn RecordSink) -> Result<T, RecordError>,
) -> Result<(Trace, T), RecordError> {
    let (scenario, plan, input, derived_from) = match what {
        RecordInput::Input(input) => {
            let split = harness.scenario_bytes.min(input.len());
            let mut prefix = input[..split].to_vec();
            prefix.resize(harness.scenario_bytes, 0);
            let plan = Plan::from_fuzz(&input[split..]);
            ((harness.draw)(&prefix), plan, Some(input), None)
        }
        RecordInput::Plan { scenario, plan } => {
            (scenario.clone(), Plan::from_fuzz(plan), None, None)
        }
        RecordInput::Script {
            scenario,
            plan,
            derived_from,
        } => (
            scenario.clone(),
            PlanInput::Script(plan.clone()),
            None,
            derived_from.cloned(),
        ),
    };
    write_trace(harness, &scenario, plan, input, derived_from, out, run)
}

/// Writes the trace of `run` on `scenario` and `plan` to `out`, as
/// [`record`] describes, and returns it with `run`'s result. `input`, the
/// full fuzz input, becomes `input.bin` when it's given, and
/// `derived_from` becomes `meta.derived_from`.
pub(crate) fn write_trace<T>(
    harness: &HarnessEntry,
    scenario: &Value,
    plan: PlanInput,
    input: Option<&[u8]>,
    derived_from: Option<Derivation>,
    out: &Path,
    run: impl FnOnce(&Value, PlanInput, &mut dyn RecordSink) -> Result<T, RecordError>,
) -> Result<(Trace, T), RecordError> {
    let mut meta = (harness.meta)(scenario).map_err(RecordError::Harness)?;
    meta.derived_from = derived_from;
    refuse_symlinked_dir(out).map_err(RecordError::Refused)?;
    if fs::symlink_metadata(out.join(TRACE_FILE)).is_ok() {
        return Err(RecordError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "out already holds a trace",
        )));
    }
    if let Some(input) = input {
        // A usize always fits in a u64 on supported targets.
        let len = input.len() as u64;
        if len > MAX_INPUT_BYTES {
            return Err(RecordError::InputTooLarge { len: input.len() });
        }
        meta.input = Some(InputRef {
            sha256: hex(&Sha256::digest(input)),
            len,
        });
        write_input(out, input).map_err(RecordError::Io)?;
    }
    let mut sink = FileSink::create(out, &meta).map_err(RecordError::Io)?;
    let result = run(&meta.scenario, plan, &mut sink)?;
    sink.finish().map_err(RecordError::Io)?;
    let trace = TraceReader::open(out).map_err(RecordError::Read)?;
    Ok((trace, result))
}

/// Writes `input` to `out`'s `input.bin`, creating `out` if needed. Fails if
/// the file already exists, so a trace's input is never overwritten.
fn write_input(out: &Path, input: &[u8]) -> io::Result<()> {
    fs::create_dir_all(out)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(out.join(INPUT_FILE))?;
    file.write_all(input)?;
    file.sync_all()
}
