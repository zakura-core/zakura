//! Classifying a run of one input in a fresh process, from how the process
//! ended and the trace it wrote (explorer plan, Result contract).
//!
//! The child writes its trace as `zakura-sim record --input` does: one line
//! at a time, flushed, with `end` last. A child that dies mid-run leaves the
//! prefix it wrote. A run without its `end` record never counts as a pass:
//! a child that failed is a [`ProcessResult::Crashed`], and one that exited
//! with code 0 is [`ProcessResult::NoEnd`]. A child that wrote `end`
//! classifies as [`classify_end`] says, as the run did in the parent.

use std::{io, path::Path, process::ExitStatus};

use serde::Serialize;

use super::{classify_end, Bucket, Classified};
use crate::{
    harness::HarnessEntry,
    trace::{Invalid, ReadError, TraceReader},
};

/// How a child process ended, as its supervisor saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProcessExit {
    /// It exited with this code.
    Code(i32),
    /// This signal killed it.
    Signal(i32),
    /// The supervisor killed it at its time limit.
    TimedOut,
}

impl From<ExitStatus> for ProcessExit {
    /// The code the process exited with, else the signal that killed it.
    fn from(status: ExitStatus) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(signal) = status.signal() {
                return ProcessExit::Signal(signal);
            }
        }
        // Off Unix, every exit has a code.
        ProcessExit::Code(status.code().unwrap_or(-1))
    }
}

/// How a child process failed. Each kind stays a process failure until a
/// rerun confirms what caused it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ProcessFailure {
    /// The supervisor killed it at its time limit, or libFuzzer reported a
    /// timeout.
    Timeout,
    /// A sanitizer reported an error.
    Sanitizer {
        /// The report's first line, such as
        /// `==1==ERROR: AddressSanitizer: heap-use-after-free ...`.
        report: String,
    },
    /// An allocation failed, or libFuzzer or a sanitizer reported the
    /// process out of memory. A kernel out-of-memory kill shows as `signal`
    /// 9.
    OutOfMemory,
    /// It aborted, with no sanitizer report and no allocation failure.
    Abort,
    /// Another signal killed it.
    Signal {
        /// The signal's number.
        signal: i32,
    },
    /// It exited with a nonzero code. A Rust panic outside the run exits
    /// with 101.
    Exit {
        /// The code.
        code: i32,
    },
}

/// `SIGABRT` on Linux and macOS.
const SIGABRT: i32 = 6;

impl ProcessFailure {
    /// How the process failed, from its exit and its standard error. `None`
    /// for an exit with code 0 and no error report.
    ///
    /// A report in `stderr` ranks before the exit, since a sanitizer and an
    /// allocation failure both abort or exit with a code of their own. An
    /// out-of-memory report ranks before a sanitizer's other reports, so a
    /// sanitizer's out-of-memory report is `out-of-memory`.
    pub fn of(exit: ProcessExit, stderr: &[u8]) -> Option<ProcessFailure> {
        let stderr = String::from_utf8_lossy(stderr);
        let line = |marker: &str| stderr.lines().find(|line| line.contains(marker));
        if exit == ProcessExit::TimedOut || line("ERROR: libFuzzer: timeout").is_some() {
            return Some(ProcessFailure::Timeout);
        }
        if line("ERROR: libFuzzer: out-of-memory").is_some()
            || line("Sanitizer: out of memory").is_some()
            || line("Sanitizer: hard rss limit exhausted").is_some()
            || stderr
                .lines()
                .any(|line| line.contains("memory allocation of") && line.contains("failed"))
        {
            return Some(ProcessFailure::OutOfMemory);
        }
        // MemorySanitizer and ThreadSanitizer start their reports with
        // `WARNING:`, and the others with `ERROR:`.
        if let Some(report) = stderr.lines().find(|line| {
            (line.contains("ERROR: ") && line.contains("Sanitizer"))
                || line.contains("WARNING: MemorySanitizer: ")
                || line.contains("WARNING: ThreadSanitizer: ")
        }) {
            return Some(ProcessFailure::Sanitizer {
                report: report.trim().to_owned(),
            });
        }
        match exit {
            ProcessExit::Code(0) => None,
            ProcessExit::Code(code) => Some(ProcessFailure::Exit { code }),
            ProcessExit::Signal(SIGABRT) => Some(ProcessFailure::Abort),
            ProcessExit::Signal(signal) => Some(ProcessFailure::Signal { signal }),
            ProcessExit::TimedOut => Some(ProcessFailure::Timeout),
        }
    }
}

/// What a child process's run came to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "result", rename_all = "kebab-case")]
pub enum ProcessResult {
    /// The child wrote its `end` record and didn't fail.
    Ended(Classified),
    /// The child failed.
    Crashed {
        /// How.
        failure: ProcessFailure,
        /// The run's class, if the child wrote its `end` record first.
        end: Option<Classified>,
    },
    /// The child exited with code 0, with no error report, but wrote no
    /// `end` record.
    NoEnd,
}

/// A child process's run, classified.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProcessClassified {
    /// What the run came to.
    #[serde(flatten)]
    pub result: ProcessResult,
    /// The last step the trace holds, 0 if it holds none or doesn't read
    /// as a valid prefix.
    pub steps: u64,
}

impl ProcessClassified {
    /// Where the result is filed: `crash` for a failed child, `no-end` for
    /// a run without its `end` record, and else the run's outcome's
    /// bucket, which is none for a clean run.
    pub fn bucket(&self) -> Option<Bucket> {
        match &self.result {
            ProcessResult::Ended(classified) => classified.outcome.bucket(),
            ProcessResult::Crashed { .. } => Some(Bucket::Crash),
            ProcessResult::NoEnd => Some(Bucket::NoEnd),
        }
    }

    /// Every bucket the result files under, without repeats, in [`Bucket`]
    /// order: `crash` for a failed child, with each bucket of the `end`
    /// record it wrote first; `no-end` for a run without its `end` record;
    /// and else [`Classified::buckets`]. Empty only for a clean run.
    pub fn buckets(&self) -> Vec<Bucket> {
        match &self.result {
            ProcessResult::Ended(classified) => classified.buckets(),
            ProcessResult::Crashed { end, .. } => {
                let mut buckets: Vec<Bucket> = end
                    .iter()
                    .flat_map(Classified::buckets)
                    .chain([Bucket::Crash])
                    .collect();
                buckets.sort();
                buckets.dedup();
                buckets
            }
            ProcessResult::NoEnd => vec![Bucket::NoEnd],
        }
    }
}

/// Classifies the run of one input on `entry`'s harness by a child process
/// that ended with `exit`, wrote `stderr`, and was writing its trace to
/// `dir`.
///
/// A missing or empty `trace.jsonl` counts as a trace with no records, as
/// when the child died before it wrote one. A failed child is
/// [`ProcessResult::Crashed`] whatever its trace holds: one whose trace
/// doesn't read as a prefix of a valid trace has no `end` and no steps.
/// Returns an error only for such a trace from a child that didn't fail.
pub fn classify_process(
    entry: &HarnessEntry,
    exit: ProcessExit,
    stderr: &[u8],
    dir: &Path,
) -> Result<ProcessClassified, ReadError> {
    let failure = ProcessFailure::of(exit, stderr);
    let (end, steps) = match TraceReader::open_prefix(dir) {
        Ok(trace) => (
            trace.end.as_ref().map(|end| classify_end(entry, end)),
            trace.steps.last().map_or(0, |step| step.step),
        ),
        Err(ReadError::Io(error)) if error.kind() == io::ErrorKind::NotFound => (None, 0),
        Err(ReadError::Invalid(Invalid::MissingMeta)) => (None, 0),
        // A failed child may have written anything, and its failure is
        // its class.
        Err(_) if failure.is_some() => (None, 0),
        Err(error) => return Err(error),
    };
    let result = match (failure, end) {
        (Some(failure), end) => ProcessResult::Crashed { failure, end },
        (None, Some(classified)) => ProcessResult::Ended(classified),
        (None, None) => ProcessResult::NoEnd,
    };
    Ok(ProcessClassified { result, steps })
}
