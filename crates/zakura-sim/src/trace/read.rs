//! Reading a trace directory back, strictly.

use std::{
    fmt,
    fs::OpenOptions,
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::{
    meta::{hex, MetaRecord},
    write::{is_sha256, BlobStore},
    INPUT_FILE, MAX_INPUT_BYTES, MAX_LINE_BYTES, MAX_TRACE_BYTES, TRACE_FILE,
};
use crate::{
    check::PropertyOutcome,
    record::{EndRecord, InitRecord, Record, StepRecord, CONTRACT_VERSION, FORMAT_VERSION},
};

/// A trace read from disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trace {
    /// The trace directory.
    pub dir: PathBuf,
    /// The trace's ID: the SHA-256 of its `trace.jsonl`, in lowercase hex.
    pub id: String,
    /// Line 1.
    pub meta: MetaRecord,
    /// Line 2. `None` only in a prefix that stops before it.
    pub init: Option<InitRecord>,
    /// Every step, in order.
    pub steps: Vec<StepRecord>,
    /// The last line. `None` only in a prefix.
    pub end: Option<EndRecord>,
}

impl Trace {
    /// True if the trace has its `end` record.
    pub fn is_complete(&self) -> bool {
        self.end.is_some()
    }
}

/// Reads trace directories.
#[derive(Clone, Copy, Debug)]
pub struct TraceReader;

impl TraceReader {
    /// Reads the complete trace in `dir`. A file without an `end` record is
    /// [`Invalid::Truncated`].
    pub fn open(dir: &Path) -> Result<Trace, ReadError> {
        let trace = Self::open_prefix(dir)?;
        if trace.end.is_none() {
            return Err(ReadError::Invalid(Invalid::Truncated {
                steps: trace.steps.last().map_or(0, |step| step.step),
            }));
        }
        Ok(trace)
    }

    /// Reads the trace in `dir`, which may stop before its `end` record, as
    /// a crashed run's does. A final line cut off before its newline counts
    /// as never written. Nothing may treat a trace without `end` as complete.
    ///
    /// Checks the versions on line 1 first, then each line's shape, its place
    /// in the record order, and the step numbers it names, then every blob
    /// `meta` names.
    pub fn open_prefix(dir: &Path) -> Result<Trace, ReadError> {
        refuse_symlinked_dir(dir)?;
        let path = dir.join(TRACE_FILE);
        let bytes = read_bounded(&path, MAX_TRACE_BYTES)?.ok_or_else(|| {
            ReadError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} doesn't exist", path.display()),
            ))
        })?;
        let id = hex(&Sha256::digest(&bytes));
        // A crash mid-write leaves a final line without its newline. That
        // line was never fully written, even if what's there parses.
        let written = bytes
            .iter()
            .rposition(|&byte| byte == b'\n')
            .map_or(0, |last| last + 1);
        let mut lines = bytes[..written].split_inclusive(|&byte| byte == b'\n');
        let first = lines.next().ok_or(Invalid::MissingMeta)?;
        if first.len() > MAX_LINE_BYTES {
            return Err(Invalid::LineTooLong { line: 1 }.into());
        }
        let meta = read_meta(first)?;

        let mut parsed = Parsed::default();
        let mut number = 1;
        for line in lines {
            number += 1;
            if line.len() > MAX_LINE_BYTES {
                return Err(Invalid::LineTooLong { line: number }.into());
            }
            let record =
                serde_json::from_slice::<Record>(strip_newline(line)).map_err(|error| {
                    Invalid::Malformed {
                        line: number,
                        reason: error.to_string(),
                    }
                })?;
            parsed.push(number, record)?;
        }

        check_artifacts(dir, &meta)?;
        Ok(Trace {
            dir: dir.to_owned(),
            id,
            meta,
            init: parsed.init,
            steps: parsed.steps,
            end: parsed.end,
        })
    }
}

/// The records after `meta`, as the reader checks them.
#[derive(Default)]
struct Parsed {
    init: Option<InitRecord>,
    steps: Vec<StepRecord>,
    end: Option<EndRecord>,
    /// The last `advance-time` instant, in nanoseconds.
    clock: u64,
}

impl Parsed {
    /// Takes `record`, read from line `line`, if it may come next.
    fn push(&mut self, line: usize, record: Record) -> Result<(), Invalid> {
        match record {
            Record::Meta(_) => {
                return Err(Invalid::Duplicate {
                    line,
                    record: "meta",
                })
            }
            Record::End(_) if self.end.is_some() => {
                return Err(Invalid::Duplicate {
                    line,
                    record: "end",
                })
            }
            _ if self.end.is_some() => return Err(Invalid::AfterEnd { line }),
            Record::Init(_) if self.init.is_some() => {
                return Err(Invalid::Duplicate {
                    line,
                    record: "init",
                })
            }
            Record::Init(init) => self.init = Some(init),
            Record::Step(_) | Record::End(_) if self.init.is_none() => {
                return Err(Invalid::OutOfOrder { line })
            }
            Record::Step(step) => {
                let next = self
                    .steps
                    .last()
                    .map_or(1, |last| last.step.saturating_add(1));
                if step.step != next {
                    return Err(Invalid::OutOfOrder { line });
                }
                self.tick(line, &step.pick)?;
                self.steps.push(step);
            }
            Record::End(end) => {
                self.check_references(line, &end)?;
                self.end = Some(end);
            }
        }
        Ok(())
    }

    /// Checks that an `advance-time` pick doesn't move the clock back.
    fn tick(&mut self, line: usize, pick: &str) -> Result<(), Invalid> {
        let Some(ns) = pick.strip_prefix("advance-time:") else {
            return Ok(());
        };
        let ns: u64 = ns.parse().map_err(|_| Invalid::Malformed {
            line,
            reason: format!("`{pick}` names no instant"),
        })?;
        if ns < self.clock {
            return Err(Invalid::ClockRegression { line });
        }
        self.clock = ns;
        Ok(())
    }

    /// Checks that `end` counts the file's steps, that its violation names
    /// only steps in the file, with one match key per evidence step, and that
    /// each property report's evidence and `holds_since` do too.
    fn check_references(&self, line: usize, end: &EndRecord) -> Result<(), Invalid> {
        let steps = self.steps.last().map_or(0, |step| step.step);
        let in_trace = |step: u64| (1..=steps).contains(&step);
        let good = end.steps == steps
            && end.violation.as_ref().is_none_or(|violation| {
                in_trace(violation.step)
                    && violation.witness.iter().all(|&step| in_trace(step))
                    && violation.witness_keys.len() == violation.witness.len()
            })
            && end.properties.iter().all(|report| {
                // 0 means the assumptions held from the start.
                report.holds_since.is_none_or(|step| step <= steps)
                    && match &report.outcome {
                        PropertyOutcome::Violated { witness } => {
                            witness.iter().all(|&step| in_trace(step))
                        }
                        PropertyOutcome::Held { .. }
                        | PropertyOutcome::NotApplicable
                        | PropertyOutcome::Inconclusive { .. } => true,
                    }
            });
        if good {
            Ok(())
        } else {
            Err(Invalid::BadReference { line })
        }
    }
}

/// The versions on line 1, read before the rest of `meta`, so a newer format
/// reports its version rather than its unknown fields.
#[derive(Deserialize)]
struct Head {
    meta: Versions,
}

/// The two versions a reader must support.
#[derive(Deserialize)]
struct Versions {
    format: u32,
    contract: u32,
}

/// Reads line 1: checks it's `meta`, then its versions, then the rest, and
/// that every digest it names is one.
fn read_meta(line: &[u8]) -> Result<MetaRecord, Invalid> {
    let line = strip_newline(line);
    let malformed = |reason: String| Invalid::Malformed { line: 1, reason };
    let value: serde_json::Value =
        serde_json::from_slice(line).map_err(|error| malformed(error.to_string()))?;
    let head = Head::deserialize(&value).map_err(|_| Invalid::MissingMeta)?;
    if head.meta.format != FORMAT_VERSION {
        return Err(Invalid::UnsupportedVersion {
            field: "format",
            found: head.meta.format,
        });
    }
    if head.meta.contract != CONTRACT_VERSION {
        return Err(Invalid::UnsupportedVersion {
            field: "contract",
            found: head.meta.contract,
        });
    }
    let Record::Meta(meta) =
        serde_json::from_slice(line).map_err(|error| malformed(error.to_string()))?
    else {
        return Err(Invalid::MissingMeta);
    };
    let digests = meta
        .fixtures
        .iter()
        .map(|blob| &blob.sha256)
        .chain(meta.input.iter().map(|input| &input.sha256))
        .chain(meta.derived_from.iter().map(|from| &from.trace));
    for digest in digests {
        if !is_sha256(digest) {
            return Err(malformed(format!("`{digest}` isn't a SHA-256")));
        }
    }
    if let Some(field) = meta.too_deep_field() {
        return Err(malformed(format!("meta.{field} nests too deep")));
    }
    if let Some(field) = meta.float_field() {
        return Err(malformed(format!("meta.{field} holds a float")));
    }
    Ok(*meta)
}

/// Checks that every fixture and `input.bin` that `meta` names is present
/// with the right digest.
fn check_artifacts(dir: &Path, meta: &MetaRecord) -> Result<(), ReadError> {
    let store = BlobStore::for_trace(dir);
    for blob in &meta.fixtures {
        store.get(&blob.sha256)?;
    }
    if let Some(input) = &meta.input {
        let Some(bytes) = read_bounded(&dir.join(INPUT_FILE), MAX_INPUT_BYTES)? else {
            return Err(Missing::MissingBlob {
                sha256: input.sha256.clone(),
            }
            .into());
        };
        // A usize always fits in a u64 on supported targets.
        if hex(&Sha256::digest(&bytes)) != input.sha256 || bytes.len() as u64 != input.len {
            return Err(Missing::DigestMismatch {
                sha256: input.sha256.clone(),
            }
            .into());
        }
    }
    Ok(())
}

/// Refuses a directory that is itself a symlink. A missing one passes, since
/// its files then read as missing.
pub(crate) fn refuse_symlinked_dir(dir: &Path) -> Result<(), ReadError> {
    // On Unix, `lstat("link/")` and `lstat("link/.")` follow the link. Path
    // components drop a trailing separator and every `.` but a leading one,
    // so the check sees the link itself.
    let link: PathBuf = dir.components().collect();
    match link.symlink_metadata() {
        Ok(meta) if meta.is_symlink() => Err(ReadError::Refused {
            path: dir.to_owned(),
            reason: Refusal::SymlinkedDirectory,
        }),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ReadError::Io(error)),
    }
}

/// Reads the regular file at `path`, or `None` if nothing is there. Refuses
/// a symlink, anything else that isn't a regular file, such as a FIFO or a
/// device, and a file over `limit` bytes, and never allocates past `limit`.
pub(crate) fn read_bounded(path: &Path, limit: u64) -> Result<Option<Vec<u8>>, ReadError> {
    let refused = |reason| ReadError::Refused {
        path: path.to_owned(),
        reason,
    };
    match path.symlink_metadata() {
        Ok(meta) if !meta.is_file() => return Err(refused(Refusal::NotRegularFile)),
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ReadError::Io(error)),
    }
    let mut options = OpenOptions::new();
    options.read(true);
    // The path could change between the check above and the open. Refusing
    // to follow a symlink, and not blocking on a FIFO, keeps the open safe.
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::custom_flags(
        &mut options,
        libc::O_NOFOLLOW | libc::O_NONBLOCK,
    );
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) if path.symlink_metadata().is_ok_and(|meta| !meta.is_file()) => {
            return Err(refused(Refusal::NotRegularFile))
        }
        Err(error) => return Err(ReadError::Io(error)),
    };
    let meta = file.metadata().map_err(ReadError::Io)?;
    if !meta.is_file() {
        return Err(refused(Refusal::NotRegularFile));
    }
    if meta.len() > limit {
        return Err(refused(Refusal::TooLarge { limit }));
    }
    let mut bytes = Vec::new();
    // The file can grow after the size check, so the read stops one byte
    // past the limit.
    file.take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(ReadError::Io)?;
    // A usize always fits in a u64 on supported targets.
    if bytes.len() as u64 > limit {
        return Err(refused(Refusal::TooLarge { limit }));
    }
    Ok(Some(bytes))
}

/// `line` without its trailing newline, if any.
fn strip_newline(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

/// Why a trace couldn't be read.
#[derive(Debug)]
pub enum ReadError {
    /// The file isn't a valid trace.
    Invalid(Invalid),
    /// A blob or `input.bin` the trace names is missing or wrong.
    Missing(Missing),
    /// The reader refused to read a file.
    Refused {
        /// The file.
        path: PathBuf,
        /// Why.
        reason: Refusal,
    },
    /// A file couldn't be read.
    Io(io::Error),
}

/// Why the reader refused a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// It's a symlink, a FIFO, a device or a directory.
    NotRegularFile,
    /// It's a trace or blob directory that is itself a symlink.
    SymlinkedDirectory,
    /// It's over its size limit (see [`super::MAX_TRACE_BYTES`]).
    TooLarge {
        /// The limit, in bytes.
        limit: u64,
    },
}

/// How a trace file is invalid. Lines count from 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Invalid {
    /// The line isn't one record: bad JSON, an unknown field, or an unknown
    /// variant.
    Malformed {
        /// The line.
        line: usize,
        /// The parser's message.
        reason: String,
    },
    /// The file is empty, or line 1 isn't `meta`.
    MissingMeta,
    /// The line is over [`super::MAX_LINE_BYTES`].
    LineTooLong {
        /// The line.
        line: usize,
    },
    /// This build can't read the trace's `format`, or it was written under
    /// another `contract`.
    UnsupportedVersion {
        /// `format` or `contract`.
        field: &'static str,
        /// The trace's value.
        found: u32,
    },
    /// A second `meta`, `init` or `end`.
    Duplicate {
        /// The line.
        line: usize,
        /// The record repeated.
        record: &'static str,
    },
    /// A record after `end`.
    AfterEnd {
        /// The line.
        line: usize,
    },
    /// A step or `end` before `init`, or a step whose number isn't the last
    /// one plus 1.
    OutOfOrder {
        /// The line.
        line: usize,
    },
    /// The file stops before its `end` record.
    Truncated {
        /// The last step it has.
        steps: u64,
    },
    /// `end` counts other steps than the file has, its violation names a step
    /// outside the file or lacks a match key per evidence step, or a property
    /// report's evidence or `holds_since` names a step outside the file.
    BadReference {
        /// The `end` line.
        line: usize,
    },
    /// An `advance-time` pick goes back in time.
    ClockRegression {
        /// The line.
        line: usize,
    },
}

/// A missing or corrupt artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Missing {
    /// No blob store has this blob, or `input.bin` is missing.
    MissingBlob {
        /// The blob's digest.
        sha256: String,
    },
    /// The blob, or `input.bin`, doesn't hash to its recorded digest.
    DigestMismatch {
        /// The recorded digest.
        sha256: String,
    },
}

impl From<Invalid> for ReadError {
    fn from(invalid: Invalid) -> Self {
        ReadError::Invalid(invalid)
    }
}

impl From<Missing> for ReadError {
    fn from(missing: Missing) -> Self {
        ReadError::Missing(missing)
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::Invalid(invalid) => invalid.fmt(f),
            ReadError::Missing(missing) => missing.fmt(f),
            ReadError::Refused {
                path,
                reason: Refusal::NotRegularFile,
            } => write!(f, "{} isn't a regular file", path.display()),
            ReadError::Refused {
                path,
                reason: Refusal::TooLarge { limit },
            } => write!(f, "{} is over {limit} bytes", path.display()),
            ReadError::Refused {
                path,
                reason: Refusal::SymlinkedDirectory,
            } => write!(f, "{} is a symlinked directory", path.display()),
            ReadError::Io(error) => write!(f, "couldn't read the trace: {error}"),
        }
    }
}

impl std::error::Error for ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReadError::Io(error) => Some(error),
            ReadError::Invalid(_) | ReadError::Missing(_) | ReadError::Refused { .. } => None,
        }
    }
}

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Invalid::Malformed { line, reason } => write!(f, "line {line} is malformed: {reason}"),
            Invalid::MissingMeta => f.write_str("line 1 isn't a meta record"),
            Invalid::LineTooLong { line } => write!(f, "line {line} is too long"),
            Invalid::UnsupportedVersion { field, found } => {
                write!(f, "unsupported {field} version {found}")
            }
            Invalid::Duplicate { line, record } => write!(f, "line {line} is a second {record}"),
            Invalid::AfterEnd { line } => write!(f, "line {line} comes after end"),
            Invalid::OutOfOrder { line } => write!(f, "line {line} is out of order"),
            Invalid::Truncated { steps } => {
                write!(f, "the trace stops after step {steps}, before its end")
            }
            Invalid::BadReference { line } => {
                write!(f, "end on line {line} names a step the trace doesn't have")
            }
            Invalid::ClockRegression { line } => {
                write!(f, "line {line} moves the clock backwards")
            }
        }
    }
}

impl fmt::Display for Missing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Missing::MissingBlob { sha256 } => write!(f, "blob {sha256} is missing"),
            Missing::DigestMismatch { sha256 } => {
                write!(f, "blob {sha256} doesn't match its digest")
            }
        }
    }
}
