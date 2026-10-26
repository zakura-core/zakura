//! Trace files: a run's records on disk, and reading them back (traces.md).
//!
//! A trace directory holds `trace.jsonl`, whose line 1 is `meta`, line 2 is
//! `init`, then one line per step and `end` last. Beside it, `input.bin`
//! holds the fuzz input that produced the run, if bytes did, and `blobs/`
//! holds fixtures by SHA-256.
//!
//! A trace directory isn't a security boundary. The reader bounds what it
//! allocates (see [`MAX_TRACE_BYTES`] and the limits beside it), and refuses
//! symlinks, special files such as FIFOs, and a trace or `blobs/` directory
//! that is itself a symlink, so a bad trace can't exhaust memory, block a
//! read, or point the store elsewhere. It doesn't defend against a hostile
//! process changing the filesystem while it reads: links in a directory's
//! own ancestors are followed, and on Windows a path can change between the
//! check and the open.

pub mod meta;
pub mod read;
pub mod write;

pub use meta::{BuildInfo, MetaRecord};
pub use read::{Invalid, Missing, ReadError, Refusal, Trace, TraceReader};
pub use write::{BlobStore, FileSink};

/// The records, one JSON object per line.
pub const TRACE_FILE: &str = "trace.jsonl";

/// The fuzz input that produced the run.
pub const INPUT_FILE: &str = "input.bin";

/// Fixtures, each named by its SHA-256.
pub const BLOBS_DIR: &str = "blobs";

// The reader refuses files past these sizes before reading them, so a bad
// trace directory can't exhaust memory. A step line is about 100 bytes, so
// a 100,000-step run is about 10 MiB.

/// The largest `trace.jsonl` the reader accepts.
pub const MAX_TRACE_BYTES: u64 = 256 << 20;

/// The longest line the reader accepts, newline included. `meta` holds the
/// whole scenario, so it gets more room than a step needs.
pub const MAX_LINE_BYTES: usize = 4 << 20;

/// The largest blob the reader accepts.
pub const MAX_BLOB_BYTES: u64 = 256 << 20;

/// The largest `input.bin` the reader accepts.
pub const MAX_INPUT_BYTES: u64 = 16 << 20;

/// The deepest nesting of arrays and objects that `meta.scenario` and an
/// assumption's `params` may hold. serde_json stops parsing at 128 levels,
/// and the record around these values adds up to 4 (`{"meta":{"assumptions":
/// [{"params":...`), so this keeps a written line well inside what the reader
/// can parse.
pub const MAX_JSON_DEPTH: usize = 100;
