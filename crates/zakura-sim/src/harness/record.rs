//! Record mode: one run of a harness, written as a trace directory.

use std::{
    fmt,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::Path,
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{HarnessEntry, HarnessError};
use crate::{
    trace::{
        meta::{hex, InputRef},
        read::refuse_symlinked_dir,
        FileSink, ReadError, Trace, TraceReader, INPUT_FILE, MAX_INPUT_BYTES, TRACE_FILE,
    },
    Plan,
};

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
    let (scenario, plan, input) = match what {
        RecordInput::Input(input) => {
            let split = harness.scenario_bytes.min(input.len());
            let mut prefix = input[..split].to_vec();
            prefix.resize(harness.scenario_bytes, 0);
            ((harness.draw)(&prefix), &input[split..], Some(input))
        }
        RecordInput::Plan { scenario, plan } => (scenario.clone(), plan, None),
    };
    let mut meta = (harness.meta)(&scenario).map_err(RecordError::Harness)?;
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
    match (harness.run)(&meta.scenario, Plan::from_fuzz(plan), &mut sink) {
        Ok(_) | Err(HarnessError::Run(_)) => {}
        Err(error) => return Err(RecordError::Harness(error)),
    }
    sink.finish().map_err(RecordError::Io)?;
    TraceReader::open(out).map_err(RecordError::Read)
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
