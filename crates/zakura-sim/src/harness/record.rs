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
    record::{FullReport, Record, FULL_REPORT_FILE},
    trace::{
        meta::{hex, Derivation, InputRef, PlanRef},
        read::refuse_symlinked_dir,
        FileSink, ReadError, Trace, TraceReader, INPUT_FILE, MAX_INPUT_BYTES, MAX_REPORT_BYTES,
        TRACE_FILE,
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
    /// The run's `Draft` override ([`super::with_draft`]) names a property
    /// the harness doesn't register, or overrides a machine harness, whose
    /// one property can't run as `Draft`. Nothing was written.
    UnregisteredDraft {
        /// The property.
        property: String,
    },
    /// The run wrote its `end` record but sent no full report, so its
    /// violations and stores would be lost (C8). A harness run always sends
    /// one, so a sink that wraps another without forwarding it does this.
    NoReport,
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
            RecordError::NoReport => {
                f.write_str("the run ended but sent no full report for full-report.json")
            }
            RecordError::UnregisteredDraft { property } => {
                write!(
                    f,
                    "the Draft override names {property}, which the harness can't check as Draft"
                )
            }
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
/// decode the scenario ([`HarnessEntry::scenario`]). `input.bin` is written
/// before `trace.jsonl`, so a trace that names it never lacks it.
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
/// expects `expect`, with the rule [`crate::witness::witness_into`] applies
/// to `also_violates`. Returns the trace and the test's result. A run the
/// test fails still writes its trace, as evidence.
pub fn record_witness(
    harness: &HarnessEntry,
    what: RecordInput<'_>,
    out: &Path,
    property: &str,
    expect: Expect,
    also_violates: &[String],
) -> Result<(Trace, Result<(), WitnessError>), RecordError> {
    if harness.status(property).is_none() {
        return Err(RecordError::Witness(WitnessError::Unregistered));
    }
    record_with(harness, what, out, |scenario, plan, sink| {
        match (harness.witness)(scenario, plan, property, expect, also_violates, sink) {
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
            let plan = Plan::from_fuzz(&input[split..]);
            (harness.scenario(input), plan, Some(input), None)
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
    if let Some(property) = super::draft()
        .into_iter()
        .find(|id| harness.machine || harness.status(id).is_none())
    {
        return Err(RecordError::UnregisteredDraft { property });
    }
    let mut meta = (harness.meta)(scenario).map_err(RecordError::Harness)?;
    meta.derived_from = derived_from;
    // A full input names its Plan bytes through `input`.
    meta.plan = match (&plan, input) {
        (_, Some(_)) => None,
        (PlanInput::Bytes(bytes), None) => Some(PlanRef::bytes(bytes)),
        (PlanInput::Script(script), None) => Some(PlanRef::script(script)),
        (PlanInput::Trace(_) | PlanInput::Skip(_), None) => None,
    };
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
    let mut sink = Reported {
        file: FileSink::create(out, &meta).map_err(RecordError::Io)?,
        held: Vec::new(),
        report: None,
    };
    let result = run(&meta.scenario, plan, &mut sink)?;
    let Reported {
        mut file,
        held,
        report,
    } = sink;
    // Beside the trace, so a judge reads every violation and the stores
    // without rerunning (C8). A run that never reached its end sends none.
    // The `end` record is written only once the report is, so a recording
    // that fails here leaves an incomplete trace, never a complete one
    // without its report.
    match (report, held.is_empty()) {
        (Some(report), _) => {
            // Bound to the trace as it will be once `held` is written.
            let report = report.of_trace(&file.id_with(&held));
            let bytes = report_bytes(&report, MAX_REPORT_BYTES);
            let written = bytes.and_then(|bytes| write_new(&out.join(FULL_REPORT_FILE), &bytes));
            if let Err(error) = written {
                file.finish().map_err(RecordError::Io)?;
                return Err(RecordError::Io(error));
            }
        }
        (None, false) => {
            file.finish().map_err(RecordError::Io)?;
            return Err(RecordError::NoReport);
        }
        (None, true) => {}
    }
    for record in held {
        file.record(record);
    }
    file.finish().map_err(RecordError::Io)?;
    let trace = TraceReader::open(out).map_err(RecordError::Read)?;
    Ok((trace, result))
}

/// A trace's file sink that keeps the run's full report for
/// [`FULL_REPORT_FILE`].
struct Reported {
    file: FileSink,
    /// The run's `end` record and any after it, held until the report is
    /// written.
    held: Vec<Record>,
    report: Option<FullReport>,
}

impl RecordSink for Reported {
    /// Writes the record, or holds it from the `end` record on.
    fn record(&mut self, record: Record) {
        if self.held.is_empty() && !matches!(record, Record::End(_)) {
            self.file.record(record);
        } else {
            self.held.push(record);
        }
    }

    /// Keeps the report.
    fn report(&mut self, report: &FullReport) {
        self.report = Some(report.clone());
    }
}

/// `report` as [`FULL_REPORT_FILE`] holds it, one line. Fails if it's over
/// `limit` bytes, as a replay would refuse it.
fn report_bytes(report: &FullReport, limit: u64) -> io::Result<Vec<u8>> {
    let bytes = format!("{}\n", report.to_line()).into_bytes();
    // A usize always fits in a u64 on supported targets.
    if bytes.len() as u64 > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "the full report is {} bytes, over the {limit} a replay reads",
                bytes.len()
            ),
        ));
    }
    Ok(bytes)
}

/// Writes `input` to `out`'s `input.bin`, creating `out` if needed. Fails if
/// the file already exists, so a trace's input is never overwritten.
fn write_input(out: &Path, input: &[u8]) -> io::Result<()> {
    fs::create_dir_all(out)?;
    write_new(&out.join(INPUT_FILE), input)
}

/// Writes `bytes` to a new file at `path`. Fails if anything, a symlink
/// included, is already there, so nothing is overwritten or written through
/// a link.
fn write_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Passes records on and drops the run's full report, as a wrapping
    /// sink that forgets to forward it would.
    struct DropsReport<'a>(&'a mut dyn RecordSink);

    impl RecordSink for DropsReport<'_> {
        /// Forwards the record.
        fn record(&mut self, record: Record) {
            self.0.record(record);
        }
    }

    /// A full report over the replay's read limit fails the recording
    /// rather than leave a sidecar every replay would refuse.
    #[test]
    fn a_report_over_the_read_limit_is_refused() {
        let report = FullReport::new(&[], None);
        let line = report_bytes(&report, MAX_REPORT_BYTES).expect("an empty report fits");
        // A usize always fits in a u64 on supported targets.
        let len = line.len() as u64;
        assert!(report_bytes(&report, len).is_ok());
        let error = report_bytes(&report, len - 1).expect_err("one byte over");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    /// C8: a run that wrote its `end` record but sent no full report fails
    /// the recording, and leaves its trace without the `end` record, so a
    /// sink that loses the report can't leave a trace that looks complete
    /// without it.
    #[test]
    fn a_finished_run_without_its_report_fails_the_recording() {
        let harness = super::super::find("race/lost-update").expect("the race is registered");
        let dir = tempfile::tempdir().expect("a temporary directory");
        let scenario = harness.scenario(&[0, 1]);
        let result = write_trace(
            harness,
            &scenario,
            Plan::from_fuzz(&[]),
            None,
            None,
            &dir.path().join("trace"),
            |scenario, plan, sink| {
                (harness.run)(scenario, plan, &mut DropsReport(sink))
                    .map(drop)
                    .map_err(RecordError::Harness)
            },
        );
        assert!(matches!(result, Err(RecordError::NoReport)), "{result:?}");
        let left = TraceReader::open(&dir.path().join("trace"));
        assert!(
            matches!(
                left,
                Err(crate::trace::ReadError::Invalid(
                    crate::trace::Invalid::Truncated { .. }
                ))
            ),
            "{left:?}"
        );
        assert!(!dir.path().join("trace").join(FULL_REPORT_FILE).exists());
    }
}
