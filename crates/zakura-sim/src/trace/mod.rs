//! Trace files: a run's records on disk, and reading them back (traces.md).
//!
//! A trace directory holds `trace.jsonl`, whose line 1 is `meta`, line 2 is
//! `init`, then one line per step and `end` last. Beside it, `input.bin`
//! holds the fuzz input that produced the run, if bytes did, and `blobs/`
//! holds fixtures by SHA-256.

pub mod meta;
pub mod read;
pub mod write;

pub use meta::{BuildInfo, MetaRecord};
pub use read::{Invalid, Missing, ReadError, Trace, TraceReader};
pub use write::{BlobStore, FileSink};

/// The records, one JSON object per line.
pub const TRACE_FILE: &str = "trace.jsonl";

/// The fuzz input that produced the run.
pub const INPUT_FILE: &str = "input.bin";

/// Fixtures, each named by its SHA-256.
pub const BLOBS_DIR: &str = "blobs";
