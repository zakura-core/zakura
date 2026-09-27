//! Shared non-blocking CSV tracing support for Zebra components.

pub mod decode;
mod encode;
pub mod files;

use files::{open_entry, TraceLock};

use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use serde_json::{Map, Value};
use tokio::{
    runtime::Handle,
    sync::mpsc::{self, error::TryRecvError, error::TrySendError},
    task::JoinHandle,
    time::{self, Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

/// Default trace channel capacity.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 16_384;

/// Default maximum number of events to drain into a single write batch.
pub const DEFAULT_MAX_BATCH_EVENTS: usize = 256;

/// Default amount of time the writer waits for more events after receiving the
/// first event in a batch.
pub const DEFAULT_BATCH_LINGER: Duration = Duration::from_millis(5);

/// Default number of buffered bytes before the writer flushes to the file.
pub const DEFAULT_BUFFER_FLUSH_BYTES: usize = 256 * 1024;

/// Default interval between forced file flushes and syncs.
///
/// Kept short (1s) so a low-volume table's tail rows — e.g. the final
/// `commit_finish` rows at the end of a sync, which never reach
/// [`DEFAULT_BUFFER_FLUSH_BYTES`] — are durable within ~1s of being emitted. A
/// long interval here is the root cause of the e2e trace-oracle "flush race":
/// the oracle reads the CSV only a few seconds after the final commits, so a
/// 17s flush window left those rows unwritten and the oracle saw a `commit_start`
/// with no matching `commit_finish` even though the node had committed (the live
/// metrics show `applying = 0`). Tracing is opt-in (a `trace_dir` must be set),
/// so the extra fsync cadence is negligible against the debuggability win.
pub const DEFAULT_FILE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Default maximum size of a current CSV segment.
pub const DEFAULT_CSV_ROTATION_BYTES: u64 = 128 * 1024 * 1024;

/// Default number of retained CSV segments older than the current segment.
pub const DEFAULT_CSV_ROTATION_SEGMENTS: usize = 2;

/// Env var used to label every CSV trace record with a stable node identifier.
pub const NODE_ID_ENV: &str = "ZEBRA_NODE_ID";

/// Environment variables consumed by this runtime, rather than TOML config.
pub const RUNTIME_ENV_VARS: &[&str] = &[
    NODE_ID_ENV,
    "ZAKURA_TRACE_FILE_BYTES",
    "ZAKURA_TRACE_FILE_SEGMENTS",
    "ZAKURA_TRACE_CAPTURE_RUN",
];

/// Envelope columns written for every trace row, in output order.
pub const ENVELOPE_COLUMNS: &[&str] =
    &["ts", "wall_ts", "node", "process_trace_id", "trace_version"];

/// Column names and value types for the node's structured CSV traces.
pub const SCHEMA_JSON: &str = include_str!("../schema.json");

/// Trailing CSV column carrying any fields missing from a table's declared header.
///
/// A CSV table has a fixed header, so a row field that no column matches would
/// otherwise be dropped silently. Collecting those fields into a JSON object in
/// this column keeps schema drift visible and non-lossy.
pub const EXTRA_COLUMN: &str = "extra";

/// The on-disk encoding of a trace table.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TraceFormat {
    /// RFC 4180 CSV with a fixed header in every segment.
    Csv,
}

/// A logical trace table and its output file.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct JsonlTraceTable {
    table: &'static str,
    file_name: &'static str,
    format: TraceFormat,
    header: &'static [&'static str],
}

impl JsonlTraceTable {
    /// Create a CSV trace table definition.
    ///
    /// `header` lists this table's own columns; the [`ENVELOPE_COLUMNS`] are
    /// prepended and [`EXTRA_COLUMN`] is appended automatically. Every event
    /// type routed to the table shares one file, so `header` must be the union
    /// of their fields — anything omitted still reaches disk, via
    /// [`EXTRA_COLUMN`].
    pub const fn csv(
        table: &'static str,
        file_name: &'static str,
        header: &'static [&'static str],
    ) -> Self {
        Self {
            table,
            file_name,
            format: TraceFormat::Csv,
            header,
        }
    }

    /// Return the logical table name used for diagnostics.
    pub const fn table(self) -> &'static str {
        self.table
    }

    /// Return the trace output file name.
    pub const fn file_name(self) -> &'static str {
        self.file_name
    }

    /// Return the on-disk encoding for this table.
    pub const fn format(self) -> TraceFormat {
        self.format
    }

    /// Return this table's own columns, excluding the envelope and extra columns.
    pub const fn header(self) -> &'static [&'static str] {
        self.header
    }
}

/// A serializable typed CSV trace event.
pub trait JsonlTraceEvent: Serialize {
    /// The table that receives this event.
    const TABLE: JsonlTraceTable;
}

/// A borrowed value serialized using its [`fmt::Display`] representation.
///
/// This adapter keeps hash and error formatting inside the lazy serialization
/// path instead of allocating strings at trace call sites.
#[derive(Copy, Clone, Debug)]
pub struct JsonlDisplay<'a, T: ?Sized>(pub &'a T);

impl<T> Serialize for JsonlDisplay<'_, T>
where
    T: fmt::Display + ?Sized,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self.0)
    }
}

/// Convert a platform-sized count to the stable trace integer type.
pub fn saturating_count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Convert a duration to saturating whole milliseconds.
pub fn saturating_millis(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

/// Convert a duration to saturating whole microseconds.
pub fn saturating_micros(value: Duration) -> u64 {
    u64::try_from(value.as_micros()).unwrap_or(u64::MAX)
}

/// Return a CSV table's full column sequence, in output order.
fn csv_columns(header: &'static [&'static str]) -> impl Iterator<Item = &'static str> {
    ENVELOPE_COLUMNS
        .iter()
        .copied()
        .chain(header.iter().copied())
        .chain(std::iter::once(EXTRA_COLUMN))
}

/// Append `field` to `out` as a CSV field, quoting it only when required.
fn write_csv_field(out: &mut Vec<u8>, field: &str) {
    if field.contains([',', '"', '\n', '\r']) {
        out.push(b'"');
        for byte in field.bytes() {
            if byte == b'"' {
                out.push(b'"');
            }
            out.push(byte);
        }
        out.push(b'"');
    } else {
        out.extend_from_slice(field.as_bytes());
    }
}

/// Render a CSV header line for a table.
fn render_csv_header(header: &'static [&'static str]) -> Vec<u8> {
    let mut out = Vec::new();
    for (index, column) in csv_columns(header).enumerate() {
        if index > 0 {
            out.push(b',');
        }
        write_csv_field(&mut out, column);
    }
    out
}

/// Flatten nested objects into dotted column names.
///
/// This is the reference encoding that [`encode`] must match.
///
/// `{"summary": {"height": 7}}` becomes `summary.height`, which is the column
/// name DuckDB's `read_json_auto` already produced for the same rows, so
/// switching a table to CSV does not move any column names.
#[cfg(test)]
fn flatten_row(prefix: &str, value: Map<String, Value>, out: &mut Map<String, Value>) {
    for (key, value) in value {
        let key = if prefix.is_empty() {
            key
        } else {
            format!("{prefix}.{key}")
        };

        match value {
            Value::Object(nested) => flatten_row(&key, nested, out),
            value => {
                out.insert(key, value);
            }
        }
    }
}

/// Render a scalar row value into a CSV field.
fn write_csv_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        // An absent or null field is an empty CSV field, which DuckDB, pandas,
        // and `csv.DictReader` all read back as null/empty.
        Value::Null => {}
        Value::Bool(value) => out.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => out.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => write_csv_field(out, value),
        // Arrays survive flattening, so they are stored as embedded JSON.
        value => write_csv_field(out, &value.to_string()),
    }
}

/// Render one serialized trace row as a CSV line, aligned to `header`.
///
/// Fields with no matching column are collected into [`EXTRA_COLUMN`] rather
/// than dropped.
#[cfg(test)]
fn render_csv_row(header: &'static [&'static str], row: Map<String, Value>) -> Vec<u8> {
    let mut flat = Map::new();
    flatten_row("", row, &mut flat);

    let mut out = Vec::new();
    for (index, column) in csv_columns(header).enumerate() {
        if index > 0 {
            out.push(b',');
        }

        if column == EXTRA_COLUMN {
            continue;
        }

        if let Some(value) = flat.remove(column) {
            write_csv_value(&mut out, &value);
        }
    }

    // Every declared column was removed above, so anything still in `flat` is a
    // field the header does not name.
    if !flat.is_empty() {
        write_csv_field(&mut out, &Value::Object(flat).to_string());
    }

    out
}

/// Serialize a row into `format`'s on-disk encoding.
fn encode_row<T>(format: TraceFormat, header: &'static [&'static str], row: &T) -> Option<Vec<u8>>
where
    T: Serialize,
{
    match format {
        TraceFormat::Csv => encode::encode_csv_row(header, row),
    }
}

/// Implement [`JsonlTraceEvent`] for an event type.
#[macro_export]
macro_rules! impl_jsonl_trace_event {
    ($event:ty, $table:expr) => {
        impl $crate::JsonlTraceEvent for $event {
            const TABLE: $crate::JsonlTraceTable = $table;
        }
    };
}

/// A typed, non-blocking CSV event emitter.
#[derive(Clone, Debug)]
pub struct JsonlEventEmitter {
    tracer: JsonlTracer,
    node: std::sync::Arc<str>,
}

impl JsonlEventEmitter {
    /// Create a no-op event emitter.
    pub fn noop() -> Self {
        Self::new(JsonlTracer::noop(), node_id())
    }

    /// Create an event emitter with an explicit node label.
    pub fn new(tracer: JsonlTracer, node: impl Into<std::sync::Arc<str>>) -> Self {
        Self {
            tracer,
            node: node.into(),
        }
    }

    /// Return the underlying CSV tracer.
    pub fn tracer(&self) -> &JsonlTracer {
        &self.tracer
    }

    /// Return true when this emitter can currently reserve output capacity.
    pub fn is_enabled(&self) -> bool {
        self.tracer.is_enabled()
    }

    /// Lazily build and emit a typed event.
    ///
    /// The queue slot is reserved before `build` is invoked, so domain
    /// projections and serialization are skipped when tracing is disabled or
    /// when the bounded channel is full or closed.
    pub fn emit_event<E>(&self, build: impl FnOnce() -> E)
    where
        E: JsonlTraceEvent,
    {
        let Ok(permit) = self.tracer.try_reserve() else {
            return;
        };

        let event = build();
        let row = JsonlEventEnvelope {
            ts: process_elapsed_micros(),
            trace_version: 2,
            wall_ts: WallClock::now(),
            node: &self.node,
            process_trace_id: process_trace_id(),
            event: &event,
        };

        if let Some(line) = encode_row(E::TABLE.format(), E::TABLE.header(), &row) {
            permit.send(JsonlWriteEvent {
                table: E::TABLE.table(),
                file_name: E::TABLE.file_name(),
                format: E::TABLE.format(),
                header: E::TABLE.header(),
                line,
            });
        } else {
            permit.counts.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Lazily build and emit a raw JSON object for compatibility callers.
    pub fn emit_with(&self, table: JsonlTraceTable, build: impl FnOnce(&mut Map<String, Value>)) {
        let Ok(permit) = self.tracer.try_reserve() else {
            return;
        };

        let mut fields = Map::new();
        build(&mut fields);
        let row = JsonlEventEnvelope {
            ts: process_elapsed_micros(),
            trace_version: 2,
            wall_ts: WallClock::now(),
            node: &self.node,
            process_trace_id: process_trace_id(),
            event: &fields,
        };

        if let Some(line) = encode_row(table.format(), table.header(), &row) {
            permit.send(JsonlWriteEvent {
                table: table.table(),
                file_name: table.file_name(),
                format: table.format(),
                header: table.header(),
                line,
            });
        } else {
            permit.counts.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl Default for JsonlEventEmitter {
    fn default() -> Self {
        Self::noop()
    }
}

#[derive(Serialize)]
struct JsonlEventEnvelope<'a, E> {
    trace_version: u64,
    ts: u64,
    wall_ts: WallClock,
    node: &'a str,
    process_trace_id: &'static str,
    #[serde(flatten)]
    event: &'a E,
}

/// An absolute UTC timestamp, rendered as RFC 3339 with millisecond precision.
///
/// [`JsonlEventEnvelope::ts`] counts microseconds from one process-wide origin.
/// Compare `ts` only within the same `process_trace_id`. Use `wall_ts` for
/// cross-process correlation; wall-clock adjustments can change those durations.
#[derive(Copy, Clone, Debug)]
struct WallClock(chrono::DateTime<chrono::Utc>);

impl WallClock {
    fn now() -> Self {
        Self(chrono::Utc::now())
    }
}

impl fmt::Display for WallClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use chrono::{Datelike, Timelike};
        // Equivalent to `%Y-%m-%dT%H:%M:%S%.3fZ`, without parsing the format
        // string on every trace row.
        let time = self.0;
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            time.year(),
            time.month(),
            time.day(),
            time.hour(),
            time.minute(),
            time.second(),
            time.timestamp_subsec_millis(),
        )
    }
}

impl Serialize for WallClock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

fn process_elapsed_micros() -> u64 {
    static STARTED: OnceLock<std::time::Instant> = OnceLock::new();
    elapsed_micros(STARTED.get_or_init(std::time::Instant::now).elapsed())
}

fn elapsed_micros(elapsed: Duration) -> u64 {
    saturating_micros(elapsed)
}

/// Returns the process-wide node identifier used to tag CSV trace records.
///
/// Resolution order: `ZEBRA_NODE_ID`, `HOSTNAME`, `/etc/hostname`, then
/// `"unknown"`. The value is resolved once on first call and cached for the
/// lifetime of the process so every trace record from this node reports the
/// same id.
pub fn node_id() -> &'static str {
    static NODE_ID: OnceLock<String> = OnceLock::new();
    NODE_ID
        .get_or_init(|| {
            std::env::var(NODE_ID_ENV)
                .ok()
                .or_else(|| std::env::var("HOSTNAME").ok())
                .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "unknown".to_string())
        })
        .as_str()
}

/// Returns an opaque identifier shared by every CSV emitter in this process.
///
/// The identifier disambiguates appended trace rows across process restarts.
/// Process-local monotonic timestamps restart from zero after each restart.
/// The identifier provides only a correlation label.
/// The identifier does not provide randomness or security identity.
pub fn process_trace_id() -> &'static str {
    static PROCESS_TRACE_ID: OnceLock<String> = OnceLock::new();
    PROCESS_TRACE_ID
        .get_or_init(|| {
            let started = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            format!("{}-{started}", std::process::id())
        })
        .as_str()
}

/// A pre-serialized trace record to be written to a per-table file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonlWriteEvent {
    /// Logical table name used for diagnostics.
    pub table: &'static str,
    /// Output file name for this table.
    pub file_name: &'static str,
    /// On-disk encoding for this table.
    pub format: TraceFormat,
    /// This table's own CSV columns, used to write the header on file creation.
    pub header: &'static [&'static str],
    /// Pre-serialized bytes for a single record, without a trailing newline.
    pub line: Vec<u8>,
}

/// Settings for the background CSV writer.
#[derive(Clone, Debug)]
pub struct JsonlTraceConfig {
    /// Bounded queue capacity.
    pub channel_capacity: usize,
    /// Require an unrotated validation capture attributed to this run.
    pub capture_run_id: Option<String>,
    /// Maximum number of events to write in a single batch.
    pub max_batch_events: usize,
    /// How long to wait for more events after receiving the first batch event.
    pub batch_linger: Duration,
    /// Buffered bytes threshold before flushing to the underlying file.
    pub buffer_flush_bytes: usize,
    /// Maximum time between forced file flushes and syncs.
    pub file_flush_interval: Duration,
    /// Target size for the current CSV segment. A single row may exceed it.
    pub csv_rotation_bytes: u64,
    /// Number of older CSV segments to retain.
    pub csv_rotation_segments: usize,
}

impl Default for JsonlTraceConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            capture_run_id: None,
            max_batch_events: DEFAULT_MAX_BATCH_EVENTS,
            batch_linger: DEFAULT_BATCH_LINGER,
            buffer_flush_bytes: DEFAULT_BUFFER_FLUSH_BYTES,
            file_flush_interval: DEFAULT_FILE_FLUSH_INTERVAL,
            csv_rotation_bytes: DEFAULT_CSV_ROTATION_BYTES,
            csv_rotation_segments: DEFAULT_CSV_ROTATION_SEGMENTS,
        }
    }
}

impl JsonlTraceConfig {
    /// Return defaults with the controller-provided CSV limits, when valid.
    ///
    /// `ZAKURA_TRACE_FILE_SEGMENTS=0` stops the writer from rotating. An
    /// external rotator can then rename the current file while it holds the
    /// table's `.lock` file. The writer creates a new file with a header on its
    /// next append.
    pub fn from_environment() -> Self {
        let mut config = Self::default();
        if let Ok(value) = std::env::var("ZAKURA_TRACE_FILE_BYTES") {
            match value.parse::<u64>() {
                Ok(bytes) if bytes > 0 => config.csv_rotation_bytes = bytes,
                _ => tracing::warn!(
                    %value,
                    "ignoring invalid ZAKURA_TRACE_FILE_BYTES; expected a positive byte count"
                ),
            }
        }
        if let Ok(value) = std::env::var("ZAKURA_TRACE_FILE_SEGMENTS") {
            match value.parse::<usize>() {
                Ok(segments) => config.csv_rotation_segments = segments,
                Err(_) => tracing::warn!(
                    %value,
                    "ignoring invalid ZAKURA_TRACE_FILE_SEGMENTS; expected a segment count"
                ),
            }
        }
        if let Ok(run_id) = std::env::var("ZAKURA_TRACE_CAPTURE_RUN") {
            if !run_id.is_empty() {
                tracing::warn!(
                    %run_id,
                    "trace validation capture enabled: rotation is off and every batch is synced"
                );
                config.capture_run_id = Some(run_id);
                config.csv_rotation_segments = 0;
            }
        }
        config
    }
}

#[derive(Debug, Default)]
struct TraceCounts {
    accepted: AtomicU64,
    dropped: AtomicU64,
    sealed: AtomicBool,
}

#[derive(Clone)]
struct TraceRuntime {
    tx: mpsc::Sender<JsonlWriteEvent>,
    counts: Arc<TraceCounts>,
}

/// A non-blocking handle for emitting CSV trace records.
#[derive(Clone)]
pub struct JsonlTracer {
    inner: TraceState,
}

#[derive(Clone)]
enum TraceState {
    Disabled,
    Enabled(TraceRuntime),
}

/// A spawned CSV writer plus its tracer.
///
/// Dropping this guard leaves the writer's normal channel-close shutdown
/// behavior in place. Calling [`JsonlTraceGuard::shutdown`] explicitly asks the
/// writer to stop accepting new rows, drain queued rows, flush all open files,
/// and exit.
pub struct JsonlTraceGuard {
    tracer: JsonlTracer,
    shutdown: CancellationToken,
    writer: Option<JoinHandle<()>>,
}

/// Reserve errors for the bounded CSV trace queue.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum JsonlTraceReserveError {
    /// Tracing is disabled.
    Disabled,
    /// The queue is full.
    Full,
    /// The writer task has closed.
    Closed,
}

/// Send errors for the bounded CSV trace queue.
#[derive(Debug)]
pub enum JsonlTraceSendError {
    /// Tracing is disabled.
    Disabled(JsonlWriteEvent),
    /// The queue is full.
    Full(JsonlWriteEvent),
    /// The writer task has closed.
    Closed(JsonlWriteEvent),
}

/// A reserved queue slot for a trace record.
#[derive(Debug)]
pub struct JsonlTracePermit {
    permit: mpsc::OwnedPermit<JsonlWriteEvent>,
    counts: Arc<TraceCounts>,
}

impl JsonlTracePermit {
    /// Send a record into the reserved queue slot.
    pub fn send(self, event: JsonlWriteEvent) {
        self.counts.accepted.fetch_add(1, Ordering::SeqCst);
        self.permit.send(event);
    }
}

impl JsonlTracer {
    /// Create a tracer backed by the supplied sender.
    pub fn new(tx: mpsc::Sender<JsonlWriteEvent>) -> Self {
        Self {
            inner: TraceState::Enabled(TraceRuntime {
                tx,
                counts: Arc::default(),
            }),
        }
    }

    /// Create a no-op tracer.
    pub fn noop() -> Self {
        Self {
            inner: TraceState::Disabled,
        }
    }

    /// Spawn a background writer on the current Tokio runtime.
    ///
    /// If there is no current Tokio runtime, tracing is disabled and a no-op
    /// tracer is returned.
    pub fn spawn(trace_dir: PathBuf) -> Self {
        Self::spawn_with_config(trace_dir, JsonlTraceConfig::from_environment())
    }

    /// Spawn a background writer using `config` on the current Tokio runtime.
    ///
    /// If there is no current Tokio runtime, tracing is disabled and a no-op
    /// tracer is returned.
    pub fn spawn_with_config(trace_dir: PathBuf, config: JsonlTraceConfig) -> Self {
        Self::spawn_guard_with_config(trace_dir, config).into_tracer()
    }

    /// Spawn a background writer and return a guard that can flush it
    /// explicitly.
    ///
    /// If there is no current Tokio runtime, tracing is disabled and the guard
    /// contains a no-op tracer.
    pub fn spawn_guard(trace_dir: PathBuf) -> JsonlTraceGuard {
        Self::spawn_guard_with_config(trace_dir, JsonlTraceConfig::from_environment())
    }

    /// Spawn a guarded background writer using `config` on the current Tokio
    /// runtime.
    ///
    /// If there is no current Tokio runtime, tracing is disabled and the guard
    /// contains a no-op tracer.
    pub fn spawn_guard_with_config(
        trace_dir: PathBuf,
        config: JsonlTraceConfig,
    ) -> JsonlTraceGuard {
        let Ok(handle) = Handle::try_current() else {
            tracing::warn!(
                ?trace_dir,
                "CSV tracing requested without an active Tokio runtime, disabling tracing"
            );
            return JsonlTraceGuard::disabled();
        };

        let (tx, rx) = mpsc::channel(config.channel_capacity.max(1));
        let writer = TraceWriter::new(trace_dir.clone(), config);
        let counts = writer.counts.clone();
        let shutdown = CancellationToken::new();
        let writer = handle.spawn(run_trace_writer(rx, writer, shutdown.clone()));
        tracing::info!(?trace_dir, "CSV tracing enabled");

        JsonlTraceGuard {
            tracer: Self {
                inner: TraceState::Enabled(TraceRuntime { tx, counts }),
            },
            shutdown,
            writer: Some(writer),
        }
    }

    /// Returns `true` if this tracer will emit records.
    ///
    /// Reports `false` for a disabled tracer and also once the writer task has
    /// closed the receiver, so callers stop building rows after writer death.
    pub fn is_enabled(&self) -> bool {
        match &self.inner {
            TraceState::Disabled => false,
            TraceState::Enabled(runtime) => {
                !runtime.tx.is_closed() && !runtime.counts.sealed.load(Ordering::SeqCst)
            }
        }
    }

    /// Returns the remaining queue capacity.
    pub fn capacity(&self) -> usize {
        let TraceState::Enabled(runtime) = &self.inner else {
            return 0;
        };

        runtime.tx.capacity()
    }

    /// Try to reserve a queue slot for a trace record.
    pub fn try_reserve(&self) -> Result<JsonlTracePermit, JsonlTraceReserveError> {
        let TraceState::Enabled(runtime) = &self.inner else {
            return Err(JsonlTraceReserveError::Disabled);
        };

        if runtime.counts.sealed.load(Ordering::SeqCst) {
            return Err(JsonlTraceReserveError::Disabled);
        }
        runtime
            .tx
            .clone()
            .try_reserve_owned()
            .map(|permit| JsonlTracePermit {
                permit,
                counts: runtime.counts.clone(),
            })
            .map_err(|error| {
                if !runtime.counts.sealed.load(Ordering::SeqCst) {
                    runtime.counts.dropped.fetch_add(1, Ordering::SeqCst);
                }
                match error {
                    TrySendError::Full(_) => JsonlTraceReserveError::Full,
                    TrySendError::Closed(_) => JsonlTraceReserveError::Closed,
                }
            })
    }

    /// Try to send a trace record without blocking.
    pub fn try_send(&self, event: JsonlWriteEvent) -> Result<(), JsonlTraceSendError> {
        match self.try_reserve() {
            Ok(permit) => {
                permit.send(event);
                Ok(())
            }
            Err(JsonlTraceReserveError::Full) => Err(JsonlTraceSendError::Full(event)),
            Err(JsonlTraceReserveError::Closed) => Err(JsonlTraceSendError::Closed(event)),
            Err(JsonlTraceReserveError::Disabled) => Err(JsonlTraceSendError::Disabled(event)),
        }
    }
}

impl JsonlTraceGuard {
    fn disabled() -> Self {
        Self {
            tracer: JsonlTracer::noop(),
            shutdown: CancellationToken::new(),
            writer: None,
        }
    }

    /// Return a cloneable tracer handle for this writer.
    pub fn tracer(&self) -> JsonlTracer {
        self.tracer.clone()
    }

    /// Consume the guard and return only its tracer handle.
    ///
    /// The writer keeps the original channel-close behavior: it flushes and
    /// exits after all tracer clones have been dropped.
    pub fn into_tracer(mut self) -> JsonlTracer {
        self.writer.take();
        self.tracer.clone()
    }

    /// Stop the writer, drain queued rows, flush all files, and wait for exit.
    pub async fn shutdown(mut self) {
        self.tracer = JsonlTracer::noop();
        self.shutdown.cancel();

        if let Some(writer) = self.writer.take() {
            if time::timeout(Duration::from_secs(5), writer).await.is_err() {
                tracing::warn!("trace writer did not finish within the shutdown deadline");
            }
        }
    }
}

impl std::fmt::Debug for JsonlTracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonlTracer").finish()
    }
}

/// The open current segment of a CSV table.
struct CurrentSegment {
    file: File,
    /// Device and inode, used to notice a rename by another process.
    identity: Option<(u64, u64)>,
}

fn file_identity(metadata: &fs::Metadata) -> Option<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some((metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

struct TableWriter {
    trace_dir: PathBuf,
    file_name: &'static str,
    /// The rendered header, including its newline.
    header_line: Vec<u8>,
    rotation_bytes: u64,
    rotation_segments: usize,
    current: Option<CurrentSegment>,
    checked_retained_segments: bool,
    /// Whether written bytes still need `sync_data`.
    dirty: bool,
}

impl TableWriter {
    fn new(
        trace_dir: PathBuf,
        file_name: &'static str,
        header: &'static [&'static str],
        config: &JsonlTraceConfig,
    ) -> Self {
        let mut header_line = render_csv_header(header);
        header_line.push(b'\n');
        Self {
            trace_dir,
            file_name,
            header_line,
            rotation_bytes: config.csv_rotation_bytes,
            rotation_segments: config.csv_rotation_segments,
            current: None,
            checked_retained_segments: false,
            dirty: false,
        }
    }

    fn segment_path(&self, index: usize) -> PathBuf {
        self.trace_dir.join(format!("{}.{}", self.file_name, index))
    }

    /// Append `lines` with one write, rotating first when a line would cross
    /// the size limit. The caller holds the directory lock.
    fn append_lines(&mut self, lines: &[Vec<u8>], sync_file: bool) -> io::Result<()> {
        if lines.is_empty() {
            if sync_file && self.dirty {
                if let Some(current) = &self.current {
                    current.file.sync_data()?;
                }
                self.dirty = false;
            }
            return Ok(());
        }

        let header_length = saturating_count(self.header_line.len());
        let mut length = self.current_file()?.metadata()?.len();
        let mut buffer = Vec::with_capacity(lines.iter().map(|line| line.len() + 1).sum());
        for line in lines {
            let line_length = saturating_count(line.len() + 1);
            let pending = length.saturating_add(saturating_count(buffer.len()));
            if self.rotation_segments > 0
                && pending > header_length
                && pending.saturating_add(line_length) > self.rotation_bytes
            {
                let file = self.current_file()?;
                file.write_all(&buffer)?;
                file.sync_data()?;
                buffer.clear();
                self.current = None;
                self.rotate_segments()?;
                length = self.current_file()?.metadata()?.len();
            }
            buffer.extend_from_slice(line);
            buffer.push(b'\n');
        }

        self.current_file()?.write_all(&buffer)?;
        self.dirty = true;
        if sync_file {
            self.current_file()?.sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Return the current segment, reopening it when it is missing or when
    /// another writer or an external rotator renamed it.
    fn current_file(&mut self) -> io::Result<&mut File> {
        let path = self.trace_dir.join(self.file_name);
        let replaced = match (&self.current, fs::symlink_metadata(&path)) {
            (Some(current), Ok(metadata)) => {
                current.identity.is_none() || current.identity != file_identity(&metadata)
            }
            _ => true,
        };
        if replaced {
            self.current = None;
            if !self.checked_retained_segments {
                self.validate_retained_segments()?;
                self.checked_retained_segments = true;
            }
            let file = self.open_current(&path)?;
            let identity = file_identity(&file.metadata()?);
            self.current = Some(CurrentSegment { file, identity });
        }
        Ok(&mut self
            .current
            .as_mut()
            .expect("the current segment was opened above")
            .file)
    }

    /// Open the current segment, writing the header into an empty file.
    ///
    /// After a valid header, a trailing partial row from a crash is removed so
    /// the next row starts on its own line.
    fn open_current(&mut self, path: &Path) -> io::Result<File> {
        let mut file = open_entry(
            path,
            OpenOptions::new().create(true).read(true).append(true),
        )?;
        let length = file.metadata()?.len();
        if length == 0 {
            file.write_all(&self.header_line)?;
            self.dirty = true;
            return Ok(file);
        }
        check_csv_header(&mut file, &self.header_line, path)?;
        let complete = complete_length(&mut file, length)?;
        if complete != length {
            file.set_len(complete)?;
        }
        Ok(file)
    }

    /// Reject retained segments written with another schema.
    fn validate_retained_segments(&self) -> io::Result<()> {
        for index in 1..=self.rotation_segments {
            let path = self.segment_path(index);
            let mut file = match open_entry(&path, OpenOptions::new().read(true)) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if file.metadata()?.len() > 0 {
                check_csv_header(&mut file, &self.header_line, &path)?;
            }
        }
        Ok(())
    }

    fn rotate_segments(&self) -> io::Result<()> {
        let oldest = self.segment_path(self.rotation_segments);
        if oldest.exists() {
            fs::remove_file(oldest)?;
        }
        for index in (1..self.rotation_segments).rev() {
            let source = self.segment_path(index);
            if source.exists() {
                fs::rename(source, self.segment_path(index + 1))?;
            }
        }
        let current = self.trace_dir.join(self.file_name);
        if current.exists() {
            fs::rename(current, self.segment_path(1))?;
        }
        Ok(())
    }
}

/// Return the length of `file` up to and including its last newline.
fn complete_length(file: &mut File, length: u64) -> io::Result<u64> {
    const CHUNK: usize = 64 * 1024;
    let mut end = length;
    let mut chunk = vec![0; CHUNK];
    while end > 0 {
        let start = end.saturating_sub(saturating_count(CHUNK));
        let chunk = &mut chunk[..usize::try_from(end - start).unwrap_or(CHUNK)];
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(chunk)?;
        if let Some(position) = chunk.iter().rposition(|&byte| byte == b'\n') {
            return Ok(start + saturating_count(position) + 1);
        }
        end = start;
    }
    Ok(0)
}

/// Check that `file` starts with `header_line`.
fn check_csv_header(file: &mut File, header_line: &[u8], path: &Path) -> io::Result<()> {
    let mut header = Vec::new();
    BufReader::new(file.take(saturating_count(header_line.len()).saturating_add(1)))
        .read_until(b'\n', &mut header)?;
    if header != header_line {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("CSV header mismatch in {}", path.display()),
        ));
    }
    Ok(())
}

struct TraceWriter {
    trace_dir: PathBuf,
    config: JsonlTraceConfig,
    tables: HashMap<&'static str, TableWriter>,
    disabled_tables: HashSet<&'static str>,
    last_file_flush: Instant,
    counts: Arc<TraceCounts>,
    capture_id: u64,
    table_counts: HashMap<&'static str, u64>,
}

impl TraceWriter {
    fn new(trace_dir: PathBuf, mut config: JsonlTraceConfig) -> Self {
        if config.capture_run_id.is_some() {
            config.csv_rotation_segments = 0;
        }
        static NEXT_CAPTURE_ID: AtomicU64 = AtomicU64::new(0);
        Self {
            trace_dir,
            config,
            tables: HashMap::new(),
            disabled_tables: HashSet::new(),
            last_file_flush: Instant::now(),
            counts: Arc::default(),
            capture_id: NEXT_CAPTURE_ID.fetch_add(1, Ordering::Relaxed),
            table_counts: HashMap::new(),
        }
    }

    fn drop_rows(&self, rows: u64) {
        self.counts.dropped.fetch_add(rows, Ordering::SeqCst);
    }

    async fn write_batch(&mut self, batch: Vec<JsonlWriteEvent>, force_flush: bool) {
        let sync_file = force_flush
            || self.config.capture_run_id.is_some()
            || self.last_file_flush.elapsed() >= self.config.file_flush_interval;
        let mut grouped: HashMap<&'static str, Vec<Vec<u8>>> = HashMap::new();
        for event in batch {
            if self.disabled_tables.contains(event.table) {
                self.drop_rows(1);
                continue;
            }
            if !self.tables.contains_key(event.table) {
                let table = TableWriter::new(
                    self.trace_dir.clone(),
                    event.file_name,
                    event.header,
                    &self.config,
                );
                self.tables.insert(event.table, table);
            }
            grouped.entry(event.table).or_default().push(event.line);
        }

        if !grouped.is_empty() || (sync_file && !self.tables.is_empty()) {
            self.write_tables(grouped, sync_file).await;
        }
        if sync_file {
            self.last_file_flush = Instant::now();
        }
        if self.config.capture_run_id.is_some() {
            self.publish_capture_status().await;
        }
    }

    /// Write every table under one directory lock in one blocking task.
    async fn write_tables(
        &mut self,
        mut grouped: HashMap<&'static str, Vec<Vec<u8>>>,
        sync_file: bool,
    ) {
        let batch_rows = saturating_count(grouped.values().map(Vec::len).sum());
        let mut tables = std::mem::take(&mut self.tables);
        let trace_dir = self.trace_dir.clone();
        let written = tokio::task::spawn_blocking(move || {
            let locked = fs::create_dir_all(&trace_dir)
                .and_then(|()| TraceLock::acquire(&trace_dir.join(".trace.lock")));
            let results = locked.map(|_lock| {
                tables
                    .iter_mut()
                    .map(|(&table, writer)| {
                        let lines = grouped.remove(table).unwrap_or_default();
                        let rows = saturating_count(lines.len());
                        (table, rows, writer.append_lines(&lines, sync_file))
                    })
                    .collect::<Vec<_>>()
            });
            (tables, results)
        })
        .await;

        let results = match written {
            Ok((tables, Ok(results))) => {
                self.tables = tables;
                results
            }
            Ok((tables, Err(error))) => {
                self.tables = tables;
                self.drop_rows(batch_rows);
                tracing::warn!(?error, trace_dir = ?self.trace_dir, "could not lock trace directory");
                return;
            }
            Err(error) => {
                // The table writers were lost with the task; they reopen on demand.
                self.drop_rows(batch_rows);
                tracing::warn!(?error, trace_dir = ?self.trace_dir, "trace write task failed");
                return;
            }
        };

        for (table, rows, result) in results {
            match result {
                Ok(()) => *self.table_counts.entry(table).or_default() += rows,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        table,
                        trace_dir = ?self.trace_dir,
                        "disabling trace table after write failure"
                    );
                    self.drop_rows(rows);
                    self.tables.remove(table);
                    self.disabled_tables.insert(table);
                }
            }
        }
    }

    async fn publish_capture_status(&self) {
        let Some(run_id) = &self.config.capture_run_id else {
            return;
        };
        let status = serde_json::json!({
            "version": 2,
            "run_id": run_id,
            "process_trace_id": process_trace_id(),
            "accepted": self.counts.accepted.load(Ordering::SeqCst),
            "sealed": self.counts.sealed.load(Ordering::SeqCst),
            "dropped": self.counts.dropped.load(Ordering::SeqCst),
            "tables": self.table_counts,
            "failed_tables": self.disabled_tables,
            "rotation_segments": self.config.csv_rotation_segments,
        });
        let path = self.trace_dir.join(format!(
            "capture-{}-{}.json",
            process_trace_id(),
            self.capture_id
        ));
        if let Err(error) = tokio::task::spawn_blocking(move || -> io::Result<()> {
            let temporary = path.with_extension("pending");
            // Truncation lets a later publish replace one that failed midway.
            let file = open_entry(
                &temporary,
                OpenOptions::new().create(true).truncate(true).write(true),
            )?;
            let mut file = io::BufWriter::new(file);
            serde_json::to_writer(&mut file, &status)?;
            file.into_inner()
                .map_err(|error| error.into_error())?
                .sync_all()?;
            fs::rename(temporary, path)?;
            Ok(())
        })
        .await
        .map_err(|error| io::Error::other(error.to_string()))
        .and_then(|result| result)
        {
            tracing::warn!(?error, "could not publish trace capture status");
        }
    }

    async fn seal_requested(&self) -> bool {
        if self.config.capture_run_id.is_none() {
            return false;
        }
        let path = self.trace_dir.join(format!(".seal-{}", process_trace_id()));
        matches!(
            tokio::task::spawn_blocking(move || open_entry(&path, OpenOptions::new().read(true)))
                .await,
            Ok(Ok(_))
        )
    }

    async fn seal(&mut self, rx: &mut mpsc::Receiver<JsonlWriteEvent>) {
        self.counts.sealed.store(true, Ordering::SeqCst);
        rx.close();
        let mut batch = Vec::new();
        // Closing the receiver also waits for outstanding reserved permits.
        if time::timeout(Duration::from_secs(2), async {
            while let Some(event) = rx.recv().await {
                batch.push(event);
            }
        })
        .await
        .is_err()
        {
            self.drop_rows(1);
        }
        self.write_batch(batch, true).await;
    }

    async fn flush_all(&mut self) {
        self.write_batch(Vec::new(), true).await;
    }
}

async fn run_trace_writer(
    mut rx: mpsc::Receiver<JsonlWriteEvent>,
    mut writer: TraceWriter,
    shutdown: CancellationToken,
) {
    let mut flush_tick = time::interval(writer.config.file_flush_interval);
    flush_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        if writer.seal_requested().await {
            writer.seal(&mut rx).await;
            break;
        }
        let mut batch = Vec::with_capacity(writer.config.max_batch_events);
        let mut receiver_closed = false;
        let mut force_flush = false;

        tokio::select! {
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => batch.push(event),
                    None => receiver_closed = true,
                }
            }
            // The tick also wakes the loop to notice a seal request.
            _ = flush_tick.tick() => {
                writer.flush_all().await;
                continue;
            }
            _ = shutdown.cancelled() => {
                rx.close();
                receiver_closed = true;
            }
        }

        if receiver_closed {
            while let Ok(event) = rx.try_recv() {
                batch.push(event);
            }
            writer.write_batch(batch, true).await;
            writer.seal(&mut rx).await;
            break;
        }

        let deadline = Instant::now() + writer.config.batch_linger;
        let sleep = time::sleep_until(deadline);
        tokio::pin!(sleep);

        while batch.len() < writer.config.max_batch_events {
            match rx.try_recv() {
                Ok(event) => {
                    batch.push(event);
                    continue;
                }
                Err(TryRecvError::Disconnected) => {
                    receiver_closed = true;
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }

            tokio::select! {
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(event) => batch.push(event),
                        None => {
                            receiver_closed = true;
                            break;
                        }
                    }
                }
                _ = flush_tick.tick() => {
                    force_flush = true;
                    break;
                }
                _ = shutdown.cancelled() => {
                    rx.close();
                    receiver_closed = true;
                    break;
                }
                _ = &mut sleep => break,
            }
        }

        if receiver_closed {
            while let Ok(event) = rx.try_recv() {
                batch.push(event);
            }
        }

        writer
            .write_batch(batch, force_flush || receiver_closed)
            .await;

        if receiver_closed {
            writer.seal(&mut rx).await;
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use super::*;

    const TEST_TABLE: JsonlTraceTable =
        JsonlTraceTable::csv("typed", "typed.csv", &["event", "value", "optional"]);

    #[derive(Serialize)]
    struct TestEvent {
        event: &'static str,
        value: u64,
        optional: Option<u64>,
    }

    impl_jsonl_trace_event!(TestEvent, TEST_TABLE);

    const CSV_TABLE: JsonlTraceTable =
        JsonlTraceTable::csv("csv", "csv.csv", &["event", "value", "optional"]);

    #[derive(Serialize)]
    struct CsvEvent {
        event: &'static str,
        value: u64,
        optional: Option<u64>,
    }

    impl_jsonl_trace_event!(CsvEvent, CSV_TABLE);

    /// Append as the writer task does: create the directory and hold its lock.
    fn append_locked(writer: &mut TableWriter, lines: &[Vec<u8>]) -> io::Result<()> {
        fs::create_dir_all(&writer.trace_dir)?;
        let _lock = TraceLock::acquire(&writer.trace_dir.join(".trace.lock"))?;
        writer.append_lines(lines, true)
    }

    /// Split a rendered CSV line into fields, honouring RFC 4180 quoting.
    fn csv_fields(line: &str) -> Vec<String> {
        let mut fields = vec![String::new()];
        let mut quoted = false;
        let mut chars = line.chars().peekable();

        while let Some(character) = chars.next() {
            match character {
                '"' if quoted && chars.peek() == Some(&'"') => {
                    chars.next();
                    fields.last_mut().expect("a field is always open").push('"');
                }
                '"' => quoted = !quoted,
                ',' if !quoted => fields.push(String::new()),
                character => fields
                    .last_mut()
                    .expect("a field is always open")
                    .push(character),
            }
        }

        fields
    }

    fn decoded_test_row(event: &JsonlWriteEvent) -> Value {
        let fields = csv_fields(std::str::from_utf8(&event.line).expect("CSV UTF-8"));
        Value::Object(
            csv_columns(event.header)
                .zip(fields)
                .filter_map(|(key, field)| {
                    if field.is_empty() || key == EXTRA_COLUMN {
                        return None;
                    }
                    let value = if matches!(key, "ts" | "trace_version" | "value" | "optional") {
                        serde_json::from_str(&field).expect("numeric field")
                    } else {
                        Value::String(field)
                    };
                    Some((key.to_owned(), value))
                })
                .collect(),
        )
    }

    #[test]
    fn common_adapters_use_display_and_saturating_numeric_forms() {
        assert_eq!(
            serde_json::to_value(JsonlDisplay(&"displayed")).expect("display adapter serializes"),
            Value::String("displayed".to_string())
        );
        assert_eq!(saturating_count(7), 7);
        assert_eq!(saturating_millis(Duration::from_millis(9)), 9);
        assert_eq!(saturating_micros(Duration::from_micros(11)), 11);
    }

    #[test]
    fn typed_emitter_serializes_the_event_envelope() {
        let (tx, mut rx) = mpsc::channel(1);
        let emitter = JsonlEventEmitter::new(JsonlTracer::new(tx), "node-typed");

        emitter.emit_event(|| TestEvent {
            event: "typed_event",
            value: 7,
            optional: None,
        });

        let written = rx.try_recv().expect("typed event uses the reserved slot");
        assert_eq!(written.table, "typed");
        assert_eq!(written.file_name, "typed.csv");

        let row: Value = decoded_test_row(&written);
        assert_eq!(row["node"], "node-typed");
        assert_eq!(row["process_trace_id"], process_trace_id());
        assert_eq!(row["event"], "typed_event");
        assert_eq!(row["value"], 7);
        assert_eq!(row["optional"], Value::Null);
        assert!(row["ts"].is_u64());
    }

    #[test]
    fn compatibility_emitter_serializes_the_common_process_envelope() {
        let (tx, mut rx) = mpsc::channel(1);
        let emitter = JsonlEventEmitter::new(JsonlTracer::new(tx), "node-raw");

        emitter.emit_with(TEST_TABLE, |row| {
            row.insert("event".to_string(), Value::String("raw_event".to_string()));
        });

        let written = rx.try_recv().expect("raw event uses the reserved slot");
        let row: Value = decoded_test_row(&written);
        assert_eq!(row["node"], "node-raw");
        assert_eq!(row["process_trace_id"], process_trace_id());
        assert_eq!(row["event"], "raw_event");
        assert!(row["ts"].is_u64());
    }

    #[test]
    fn envelope_carries_an_absolute_wall_clock() {
        let (tx, mut rx) = mpsc::channel(1);
        let emitter = JsonlEventEmitter::new(JsonlTracer::new(tx), "node-typed");

        emitter.emit_event(|| TestEvent {
            event: "typed_event",
            value: 7,
            optional: None,
        });

        let written = rx.try_recv().expect("typed event uses the reserved slot");
        let row: Value = decoded_test_row(&written);

        let wall_ts = row["wall_ts"].as_str().expect("wall_ts is a string");
        let parsed = chrono::DateTime::parse_from_rfc3339(wall_ts).expect("wall_ts is RFC 3339");

        // Millisecond precision, UTC, and the same rendering the prior campaign's
        // analyzers already parse.
        assert_eq!(wall_ts.len(), "2026-04-23T19:12:25.341Z".len());
        assert!(wall_ts.ends_with('Z'));
        assert_eq!(parsed.timezone().local_minus_utc(), 0);
    }

    #[test]
    fn csv_emitter_renders_rows_aligned_to_the_declared_header() {
        let (tx, mut rx) = mpsc::channel(1);
        let emitter = JsonlEventEmitter::new(JsonlTracer::new(tx), "node-csv");

        emitter.emit_event(|| CsvEvent {
            event: "csv_event",
            value: 7,
            optional: None,
        });

        let written = rx.try_recv().expect("csv event uses the reserved slot");
        assert_eq!(written.file_name, "csv.csv");
        assert_eq!(written.format, TraceFormat::Csv);

        let line = String::from_utf8(written.line).expect("csv line is utf-8");
        let fields = csv_fields(&line);
        let header = String::from_utf8(render_csv_header(CSV_TABLE.header())).expect("utf-8");

        assert_eq!(
            header,
            "ts,wall_ts,node,process_trace_id,trace_version,event,value,optional,extra"
        );
        assert_eq!(fields.len(), csv_fields(&header).len());
        assert_eq!(fields[2], "node-csv");
        assert_eq!(fields[3], process_trace_id());
        assert_eq!(fields[5], "csv_event");
        assert_eq!(fields[6], "7");
        // `None` renders as an empty field, which DuckDB and pandas read as null.
        assert_eq!(fields[7], "");
        assert_eq!(fields[8], "");
    }

    #[test]
    fn csv_rows_flatten_nested_objects_into_dotted_columns() {
        const TABLE: JsonlTraceTable = JsonlTraceTable::csv(
            "nested",
            "nested.csv",
            &["summary.height", "summary.hashes"],
        );

        let mut row = Map::new();
        row.insert("node".to_string(), Value::String("n1".to_string()));
        row.insert(
            "summary".to_string(),
            serde_json::json!({"height": 7, "hashes": 2}),
        );

        let line = String::from_utf8(render_csv_row(TABLE.header(), row)).expect("utf-8");
        let fields = csv_fields(&line);

        assert_eq!(fields[2], "n1");
        assert_eq!(fields[5], "7");
        assert_eq!(fields[6], "2");
        assert_eq!(fields[7], "", "no undeclared fields remain");
    }

    #[test]
    fn csv_rows_route_undeclared_fields_to_the_extra_column() {
        const TABLE: JsonlTraceTable = JsonlTraceTable::csv("drift", "drift.csv", &["event"]);

        let mut row = Map::new();
        row.insert("event".to_string(), Value::String("known".to_string()));
        row.insert("added_later".to_string(), Value::from(9));

        let line = String::from_utf8(render_csv_row(TABLE.header(), row)).expect("utf-8");
        let fields = csv_fields(&line);

        assert_eq!(fields[5], "known");
        assert_eq!(
            serde_json::from_str::<Value>(&fields[6]).expect("extra holds a JSON object"),
            serde_json::json!({"added_later": 9}),
            "a field the header does not name must survive, not vanish"
        );
    }

    #[test]
    fn csv_fields_are_quoted_only_when_required() {
        const TABLE: JsonlTraceTable = JsonlTraceTable::csv(
            "quoting",
            "quoting.csv",
            &["plain", "comma", "quote", "list"],
        );

        let mut row = Map::new();
        row.insert("plain".to_string(), Value::String("peer:1".to_string()));
        row.insert("comma".to_string(), Value::String("a,b".to_string()));
        row.insert("quote".to_string(), Value::String("say \"hi\"".to_string()));
        row.insert("list".to_string(), serde_json::json!([1, 2]));

        let line = String::from_utf8(render_csv_row(TABLE.header(), row)).expect("utf-8");

        assert!(
            line.contains("peer:1,"),
            "plain fields stay unquoted: {line}"
        );
        assert!(line.contains("\"a,b\""), "commas force quoting: {line}");
        assert!(
            line.contains("\"say \"\"hi\"\"\""),
            "embedded quotes are doubled: {line}"
        );

        let fields = csv_fields(&line);
        assert_eq!(fields[5], "peer:1");
        assert_eq!(fields[6], "a,b");
        assert_eq!(fields[7], "say \"hi\"");
        assert_eq!(fields[8], "[1,2]", "arrays are stored as embedded JSON");
    }

    #[tokio::test]
    async fn writer_preserves_existing_csv_with_a_different_or_partial_header() {
        for existing in [
            "ts,wall_ts,node,process_trace_id,trace_version,old_field,extra\n1,t,n,p,old,\n",
            "ts,wall_ts,node",
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("csv.csv");
            tokio::fs::write(&path, existing)
                .await
                .expect("existing CSV");
            let mut writer = TraceWriter::new(dir.path().to_owned(), JsonlTraceConfig::default());
            writer
                .write_batch(
                    vec![JsonlWriteEvent {
                        table: "csv",
                        file_name: "csv.csv",
                        format: TraceFormat::Csv,
                        header: &["event"],
                        line: b"1,t,n,p,new,".to_vec(),
                    }],
                    true,
                )
                .await;
            assert!(writer.disabled_tables.contains("csv"));
            assert_eq!(
                tokio::fs::read_to_string(path).await.expect("CSV"),
                existing
            );
        }
    }

    #[test]
    fn direct_encoder_matches_the_json_value_reference() {
        #[derive(Serialize)]
        enum Kind {
            Unit,
            Newtype(u64),
            Tuple(u8, u8),
            Struct { inner: &'static str },
        }
        #[derive(Serialize)]
        struct Nested {
            height: u64,
            unknown: Option<&'static str>,
            deeper: Deeper,
        }
        #[derive(Serialize)]
        struct Deeper {
            leaf: i32,
            extra_leaf: bool,
        }
        #[derive(Serialize)]
        struct Row {
            event: &'static str,
            quoted: &'static str,
            signed: i64,
            float: f64,
            nan: f64,
            flag: bool,
            none: Option<u64>,
            list: Vec<u32>,
            empty: Map<String, Value>,
            summary: Nested,
            kind: Kind,
            newtype: Kind,
            tuple: Kind,
            variant: Kind,
            undeclared: &'static str,
            display: JsonlDisplay<'static, str>,
            map: std::collections::BTreeMap<u16, &'static str>,
        }
        const HEADER: &[&str] = &[
            "event",
            "quoted",
            "signed",
            "float",
            "nan",
            "flag",
            "none",
            "list",
            "summary.height",
            "summary.deeper.leaf",
            "kind",
            "newtype.Newtype",
            "display",
            "map.7",
        ];
        let row = Row {
            event: "state",
            quoted: "say \"hi\", then\nleave",
            signed: -42,
            float: 1.5,
            nan: f64::NAN,
            flag: true,
            none: None,
            list: vec![1, 2],
            empty: Map::new(),
            summary: Nested {
                height: 7,
                unknown: Some("x,y"),
                deeper: Deeper {
                    leaf: -1,
                    extra_leaf: false,
                },
            },
            kind: Kind::Unit,
            newtype: Kind::Newtype(3),
            tuple: Kind::Tuple(1, 2),
            variant: Kind::Struct { inner: "in" },
            undeclared: "extra",
            display: JsonlDisplay("shown"),
            map: [(7, "seven"), (8, "eight")].into_iter().collect(),
        };
        let Value::Object(fields) = serde_json::to_value(&row).expect("row serializes") else {
            panic!("row is an object");
        };
        assert_eq!(
            String::from_utf8(encode::encode_csv_row(HEADER, &row).expect("row encodes")),
            String::from_utf8(render_csv_row(HEADER, fields))
        );

        let mut map = Map::new();
        map.insert("event".to_owned(), Value::from("raw"));
        map.insert(
            "summary".to_owned(),
            serde_json::json!({"height": 1, "other": [1, {"a": 2}]}),
        );
        map.insert("ts".to_owned(), Value::from(9));
        assert_eq!(
            encode::encode_csv_row(HEADER, &map),
            Some(render_csv_row(HEADER, map.clone()))
        );
        assert_eq!(encode::encode_csv_row(HEADER, &7_u64), None);
        assert_eq!(encode::encode_csv_row(HEADER, &vec![1]), None);
    }

    #[tokio::test]
    async fn one_failed_table_leaves_the_writer_running() {
        const OTHER: JsonlTraceTable = JsonlTraceTable::csv("other", "other.csv", &["event"]);
        let dir = tempfile::tempdir().expect("tempdir");
        // The first table fails its header check; the writer must keep going.
        std::fs::write(dir.path().join("csv.csv"), "stale,header\n").expect("stale CSV");
        let guard = JsonlTracer::spawn_guard_with_config(
            dir.path().to_owned(),
            JsonlTraceConfig::default(),
        );
        let emitter = JsonlEventEmitter::new(guard.tracer(), "node");
        emitter.emit_event(|| CsvEvent {
            event: "first",
            value: 1,
            optional: None,
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        emitter.emit_with(OTHER, |row| {
            row.insert("event".to_owned(), Value::from("second"));
        });
        guard.shutdown().await;

        let other = std::fs::read_to_string(dir.path().join("other.csv")).expect("other CSV");
        assert!(other.contains("second"));
    }

    #[test]
    fn writer_drops_a_torn_row_before_appending() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = TableWriter::new(
            dir.path().to_owned(),
            "csv.csv",
            &["event"],
            &JsonlTraceConfig::default(),
        );
        let header = "ts,wall_ts,node,process_trace_id,trace_version,event,extra\n";
        std::fs::write(
            dir.path().join("csv.csv"),
            format!("{header}1,t,n,p,2,whole,\n1,t,n,p,2,to"),
        )
        .expect("torn CSV");
        append_locked(&mut writer, &[b"1,t,n,p,2,next,".to_vec()]).expect("append");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("csv.csv")).expect("CSV"),
            format!("{header}1,t,n,p,2,whole,\n1,t,n,p,2,next,\n")
        );
    }

    #[test]
    fn writer_follows_an_external_rename_under_the_directory_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = JsonlTraceConfig {
            csv_rotation_segments: 0,
            ..JsonlTraceConfig::default()
        };
        let mut writer = TableWriter::new(dir.path().to_owned(), "csv.csv", &["event"], &config);
        append_locked(&mut writer, &[b"1,t,n,p,2,before,".to_vec()]).expect("append");
        {
            let _lock = TraceLock::acquire(&dir.path().join(".trace.lock")).expect("lock");
            std::fs::rename(
                dir.path().join("csv.csv"),
                dir.path().join("csv.csv.rotating"),
            )
            .expect("external rotation");
        }
        append_locked(&mut writer, &[b"1,t,n,p,2,after,".to_vec()]).expect("append");

        let header = "ts,wall_ts,node,process_trace_id,trace_version,event,extra\n";
        assert_eq!(
            std::fs::read_to_string(dir.path().join("csv.csv.rotating")).expect("detached CSV"),
            format!("{header}1,t,n,p,2,before,\n")
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("csv.csv")).expect("new CSV"),
            format!("{header}1,t,n,p,2,after,\n")
        );
    }

    #[tokio::test]
    async fn writer_writes_the_csv_header_once_per_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let trace_dir = dir.path().join("traces");
        let path = trace_dir.join("csv.csv");

        // Two writer generations against the same directory, as a node restarting
        // into an existing trace dir produces.
        for generation in 0..2 {
            let (tx, rx) = mpsc::channel(16);
            let writer = TraceWriter::new(trace_dir.clone(), JsonlTraceConfig::default());
            let handle = tokio::spawn(run_trace_writer(rx, writer, CancellationToken::new()));

            tx.send(JsonlWriteEvent {
                table: "csv",
                file_name: "csv.csv",
                format: TraceFormat::Csv,
                header: &["event"],
                line: format!("1,t,n,process,run{generation},").into_bytes(),
            })
            .await
            .expect("send should succeed");

            drop(tx);
            handle.await.expect("writer task should complete");
        }

        let written = tokio::fs::read_to_string(&path).await.expect("csv file");
        let lines: Vec<_> = written.lines().collect();

        assert_eq!(lines.len(), 3, "one header plus two rows: {written}");
        assert_eq!(
            lines[0],
            "ts,wall_ts,node,process_trace_id,trace_version,event,extra"
        );
        assert_eq!(lines[1], "1,t,n,process,run0,");
        assert_eq!(
            lines[2], "1,t,n,process,run1,",
            "a restart appends rows without repeating the header"
        );
    }

    #[tokio::test]
    async fn writer_rotates_csv_segments_with_one_header_per_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let trace_dir = dir.path().join("traces");
        let config = JsonlTraceConfig {
            csv_rotation_bytes: 56,
            csv_rotation_segments: 3,
            ..JsonlTraceConfig::default()
        };
        let mut writer = TableWriter::new(trace_dir.clone(), "csv.csv", &["event"], &config);
        let lines = (0..4)
            .map(|index| format!("1,t,n,process,run{index},").into_bytes())
            .collect::<Vec<_>>();

        append_locked(&mut writer, &lines).expect("CSV write and rotation should succeed");

        let expected_header = "ts,wall_ts,node,process_trace_id,trace_version,event,extra";
        for suffix in ["", ".1", ".2", ".3"] {
            let path = trace_dir.join(format!("csv.csv{suffix}"));
            let content = std::fs::read_to_string(path).expect("retained CSV segment");
            assert_eq!(content.lines().next(), Some(expected_header));
            assert_eq!(
                content
                    .lines()
                    .filter(|line| *line == expected_header)
                    .count(),
                1
            );
        }
        let rows = ["run0", "run1", "run2", "run3"];
        let all_segments = ["", ".1", ".2", ".3"]
            .into_iter()
            .map(|suffix| {
                std::fs::read_to_string(trace_dir.join(format!("csv.csv{suffix}")))
                    .expect("segment")
            })
            .collect::<Vec<_>>();
        for row in rows {
            assert_eq!(
                all_segments
                    .iter()
                    .filter(|segment| segment.contains(row))
                    .count(),
                1,
                "each row must occur in one retained segment"
            );
        }
    }

    #[tokio::test]
    async fn concurrent_csv_writers_share_the_lock_and_header() {
        let dir = tempfile::tempdir().expect("tempdir");
        let trace_dir = dir.path().join("traces");
        let config = JsonlTraceConfig::default();
        let mut first = TableWriter::new(trace_dir.clone(), "csv.csv", &["event"], &config);
        let mut second = TableWriter::new(trace_dir.clone(), "csv.csv", &["event"], &config);
        let first_task = tokio::task::spawn_blocking(move || {
            append_locked(&mut first, &[b"1,t,n,process,first,".to_vec()])
        });
        let second_task = tokio::task::spawn_blocking(move || {
            append_locked(&mut second, &[b"1,t,n,process,second,".to_vec()])
        });
        first_task
            .await
            .expect("first writer task")
            .expect("first write");
        second_task
            .await
            .expect("second writer task")
            .expect("second write");

        let content = std::fs::read_to_string(trace_dir.join("csv.csv")).expect("CSV");
        assert_eq!(
            content
                .matches("ts,wall_ts,node,process_trace_id,trace_version,event,extra\n")
                .count(),
            1
        );
        assert!(content.contains("first"));
        assert!(content.contains("second"));
    }

    #[tokio::test]
    async fn zero_csv_rotation_segments_do_not_repeat_headers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let trace_dir = dir.path().join("traces");
        let config = JsonlTraceConfig {
            csv_rotation_bytes: 1,
            csv_rotation_segments: 0,
            ..JsonlTraceConfig::default()
        };
        let mut writer = TableWriter::new(trace_dir.clone(), "csv.csv", &["event"], &config);
        append_locked(
            &mut writer,
            &[
                b"1,t,n,process,one,".to_vec(),
                b"1,t,n,process,two,".to_vec(),
            ],
        )
        .expect("CSV write without rotation should succeed");
        let content = std::fs::read_to_string(trace_dir.join("csv.csv")).expect("CSV");
        assert_eq!(
            content
                .matches("ts,wall_ts,node,process_trace_id,trace_version,event,extra\n")
                .count(),
            1
        );
        assert_eq!(content.lines().count(), 3);
    }

    #[test]
    fn typed_emitter_is_lazy_when_disabled_full_or_closed() {
        fn assert_not_built(emitter: &JsonlEventEmitter) {
            let called = Arc::new(AtomicBool::new(false));
            let called_in_build = called.clone();

            emitter.emit_event(|| {
                called_in_build.store(true, Ordering::SeqCst);
                TestEvent {
                    event: "must_not_build",
                    value: 0,
                    optional: None,
                }
            });

            assert!(!called.load(Ordering::SeqCst));
        }

        assert_not_built(&JsonlEventEmitter::noop());

        let (full_tx, _full_rx) = mpsc::channel(1);
        let full = JsonlEventEmitter::new(JsonlTracer::new(full_tx), "full");
        full.emit_event(|| TestEvent {
            event: "fills_queue",
            value: 0,
            optional: None,
        });
        assert_not_built(&full);

        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        let closed = JsonlEventEmitter::new(JsonlTracer::new(closed_tx), "closed");
        assert_not_built(&closed);
    }

    #[tokio::test]
    async fn writer_task_produces_per_table_csv() {
        let dir = tempfile::tempdir().expect("tempdir");
        let trace_dir = dir.path().join("traces");

        let (tx, rx) = mpsc::channel(16);
        let writer = TraceWriter::new(trace_dir.clone(), JsonlTraceConfig::default());
        let handle = tokio::spawn(run_trace_writer(rx, writer, CancellationToken::new()));

        tx.send(JsonlWriteEvent {
            table: "alpha",
            file_name: "alpha.csv",
            format: TraceFormat::Csv,
            header: &["value"],
            line: b",,,,,1,".to_vec(),
        })
        .await
        .expect("send should succeed");

        tx.send(JsonlWriteEvent {
            table: "beta",
            file_name: "beta.csv",
            format: TraceFormat::Csv,
            header: &["value"],
            line: b",,,,,2,".to_vec(),
        })
        .await
        .expect("send should succeed");

        drop(tx);
        handle.await.expect("writer task should complete");

        let alpha = tokio::fs::read_to_string(trace_dir.join("alpha.csv"))
            .await
            .expect("alpha file");
        let beta = tokio::fs::read_to_string(trace_dir.join("beta.csv"))
            .await
            .expect("beta file");

        assert_eq!(
            alpha,
            "ts,wall_ts,node,process_trace_id,trace_version,value,extra\n,,,,,1,\n"
        );
        assert_eq!(
            beta,
            "ts,wall_ts,node,process_trace_id,trace_version,value,extra\n,,,,,2,\n"
        );
    }

    #[tokio::test]
    async fn writer_flushes_idle_buffers_on_timer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let trace_dir = dir.path().join("traces");

        let config = JsonlTraceConfig {
            batch_linger: Duration::from_millis(1),
            buffer_flush_bytes: 1024,
            file_flush_interval: Duration::from_millis(25),
            ..JsonlTraceConfig::default()
        };

        let (tx, rx) = mpsc::channel(16);
        let writer = TraceWriter::new(trace_dir.clone(), config);
        let handle = tokio::spawn(run_trace_writer(rx, writer, CancellationToken::new()));

        tx.send(JsonlWriteEvent {
            table: "alpha",
            file_name: "alpha.csv",
            format: TraceFormat::Csv,
            header: &["value"],
            line: b",,,,,1,".to_vec(),
        })
        .await
        .expect("send should succeed");

        time::sleep(Duration::from_millis(80)).await;

        let alpha = tokio::fs::read_to_string(trace_dir.join("alpha.csv"))
            .await
            .expect("alpha file should be flushed while the writer is idle");

        assert_eq!(
            alpha,
            "ts,wall_ts,node,process_trace_id,trace_version,value,extra\n,,,,,1,\n"
        );

        drop(tx);
        handle.await.expect("writer task should complete");
    }

    #[test]
    fn noop_tracer_returns_disabled_errors() {
        let tracer = JsonlTracer::noop();

        assert!(matches!(
            tracer.try_reserve(),
            Err(JsonlTraceReserveError::Disabled)
        ));

        let send_result = tracer.try_send(JsonlWriteEvent {
            table: "alpha",
            file_name: "alpha.csv",
            format: TraceFormat::Csv,
            header: &["value"],
            line: b",,,,,1,".to_vec(),
        });

        assert!(matches!(send_result, Err(JsonlTraceSendError::Disabled(_))));
    }

    #[tokio::test]
    async fn guarded_shutdown_drains_queued_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let trace_dir = dir.path().join("traces");

        let config = JsonlTraceConfig {
            batch_linger: Duration::from_secs(60),
            file_flush_interval: Duration::from_secs(60),
            ..JsonlTraceConfig::default()
        };
        let guard = JsonlTracer::spawn_guard_with_config(trace_dir.clone(), config);
        let tracer = guard.tracer();

        tracer
            .try_send(JsonlWriteEvent {
                table: "alpha",
                file_name: "alpha.csv",
                format: TraceFormat::Csv,
                header: &["value"],
                line: b",,,,,1,".to_vec(),
            })
            .expect("queued row");

        guard.shutdown().await;

        let alpha = tokio::fs::read_to_string(trace_dir.join("alpha.csv"))
            .await
            .expect("alpha file");
        assert_eq!(
            alpha,
            "ts,wall_ts,node,process_trace_id,trace_version,value,extra\n,,,,,1,\n"
        );
    }

    #[test]
    fn is_enabled_false_after_receiver_dropped() {
        let (tx, rx) = mpsc::channel(1);
        let tracer = JsonlTracer::new(tx);
        assert!(
            tracer.is_enabled(),
            "tracer is enabled while the receiver lives"
        );

        drop(rx);
        assert!(
            !tracer.is_enabled(),
            "tracer reports disabled once the receiver is dropped"
        );

        assert!(matches!(
            tracer.try_reserve(),
            Err(JsonlTraceReserveError::Closed)
        ));
    }

    #[test]
    fn tracer_drops_rows_when_queue_is_full() {
        let (tx, _rx) = mpsc::channel(1);
        let tracer = JsonlTracer::new(tx);

        tracer
            .try_send(JsonlWriteEvent {
                table: "alpha",
                file_name: "alpha.csv",
                format: TraceFormat::Csv,
                header: &["value"],
                line: b",,,,,1,".to_vec(),
            })
            .expect("first row fits");
        let full = tracer.try_send(JsonlWriteEvent {
            table: "alpha",
            file_name: "alpha.csv",
            format: TraceFormat::Csv,
            header: &["value"],
            line: b",,,,,2,".to_vec(),
        });

        assert!(matches!(full, Err(JsonlTraceSendError::Full(_))));
    }

    #[test]
    fn tracer_drops_flood_without_blocking() {
        let (tx, _rx) = mpsc::channel(1);
        let tracer = JsonlTracer::new(tx);

        tracer
            .try_send(JsonlWriteEvent {
                table: "alpha",
                file_name: "alpha.csv",
                format: TraceFormat::Csv,
                header: &["value"],
                line: b",,,,,0,".to_vec(),
            })
            .expect("first row fits");

        let start = Instant::now();
        let mut full = 0;
        for value in 0..10_000 {
            let result = tracer.try_send(JsonlWriteEvent {
                table: "alpha",
                file_name: "alpha.csv",
                format: TraceFormat::Csv,
                header: &["value"],
                line: format!(",,,,,{value},").into_bytes(),
            });
            if matches!(result, Err(JsonlTraceSendError::Full(_))) {
                full += 1;
            }
        }

        assert_eq!(full, 10_000);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "full queue path should not block the emitter"
        );
    }
    #[test]
    fn independent_emitters_share_one_monotonic_clock() {
        let (tx, mut rx) = mpsc::channel(4);
        let first = JsonlEventEmitter::new(JsonlTracer::new(tx.clone()), "node");
        first.emit_event(|| CsvEvent {
            event: "first",
            value: 1,
            optional: None,
        });
        let before = decoded_test_row(&rx.try_recv().expect("first event was enqueued"));
        std::thread::sleep(Duration::from_millis(2));
        let second = JsonlEventEmitter::new(JsonlTracer::new(tx), "node");
        second.emit_event(|| CsvEvent {
            event: "second",
            value: 2,
            optional: None,
        });
        let after = decoded_test_row(&rx.try_recv().expect("second event was enqueued"));
        assert!(
            after["ts"].as_u64().expect("timestamp is numeric")
                >= before["ts"].as_u64().expect("timestamp is numeric") + 1_000
        );
        assert_eq!(after["trace_version"], 2);
    }

    #[tokio::test]
    async fn validation_capture_records_drops_and_disables_rotation() {
        let dir = tempfile::tempdir().expect("temporary capture directory");
        let config = JsonlTraceConfig {
            capture_run_id: Some("test-run".to_owned()),
            channel_capacity: 1,
            csv_rotation_bytes: 1,
            ..JsonlTraceConfig::default()
        };
        let guard = JsonlTracer::spawn_guard_with_config(dir.path().to_owned(), config);
        let emitter = JsonlEventEmitter::new(guard.tracer(), "node");
        emitter.emit_event(|| CsvEvent {
            event: "first",
            value: 1,
            optional: None,
        });
        emitter.emit_event(|| CsvEvent {
            event: "dropped",
            value: 2,
            optional: None,
        });
        guard.shutdown().await;
        let path = fs::read_dir(dir.path())
            .expect("capture directory exists")
            .map(|entry| entry.expect("directory entry is readable").path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .expect("writer publishes capture status");
        let status: Value =
            serde_json::from_str(&fs::read_to_string(path).expect("status is readable"))
                .expect("status is JSON");
        assert_eq!(status["run_id"], "test-run");
        assert_eq!(status["accepted"], 1);
        assert_eq!(status["dropped"], 1);
        assert_eq!(status["rotation_segments"], 0);
        assert_eq!(status["tables"]["csv"], 1);
        assert_eq!(status["failed_tables"], serde_json::json!([]));
    }

    #[cfg(unix)]
    #[test]
    fn writer_rejects_symlinked_current_retained_and_lock_entries() {
        use std::os::unix::fs::symlink;
        for name in ["csv.csv", "csv.csv.1", ".trace.lock"] {
            let dir = tempfile::tempdir().expect("temporary trace directory");
            let target = dir.path().join("outside");
            fs::write(&target, "").expect("fixture target is writable");
            symlink(&target, dir.path().join(name)).expect("fixture symlink");
            let mut writer = TableWriter::new(
                dir.path().to_owned(),
                "csv.csv",
                &["event"],
                &JsonlTraceConfig::default(),
            );
            assert!(
                append_locked(&mut writer, &[b"row".to_vec()]).is_err(),
                "{name}"
            );
            assert_eq!(fs::read(&target).expect("target is readable"), b"");
        }
    }
    #[tokio::test]
    async fn requested_seal_drains_rows_and_stops_recording() {
        let dir = tempfile::tempdir().expect("temporary capture directory");
        let guard = JsonlTracer::spawn_guard_with_config(
            dir.path().to_owned(),
            JsonlTraceConfig {
                capture_run_id: Some("run".to_owned()),
                file_flush_interval: Duration::from_millis(10),
                ..JsonlTraceConfig::default()
            },
        );
        let emitter = JsonlEventEmitter::new(guard.tracer(), "node");
        emitter.emit_event(|| CsvEvent {
            event: "before",
            value: 1,
            optional: None,
        });
        fs::write(dir.path().join(format!(".seal-{}", process_trace_id())), "")
            .expect("seal request is writable");
        time::timeout(Duration::from_secs(3), async {
            while !guard
                .writer
                .as_ref()
                .expect("writer was spawned")
                .is_finished()
            {
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("writer acknowledges the seal");
        assert!(!emitter.is_enabled());
        emitter.emit_event::<CsvEvent>(|| panic!("sealed emitters must not build another row"));
        let status_path = fs::read_dir(dir.path())
            .expect("capture directory exists")
            .map(|entry| entry.expect("directory entry is readable").path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .expect("writer publishes capture status");
        let status: Value =
            serde_json::from_str(&fs::read_to_string(status_path).expect("status is readable"))
                .expect("status is JSON");
        assert_eq!(status["sealed"], true);
        assert_eq!(status["accepted"], 1);
        assert_eq!(status["dropped"], 0);
        assert_eq!(status["tables"]["csv"], 1);
        guard.shutdown().await;
    }

    #[cfg(unix)]
    #[test]
    fn writer_rejects_fifo_without_waiting_for_a_peer() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        assert!(std::process::Command::new("mkfifo")
            .arg(dir.path().join("csv.csv"))
            .status()
            .expect("POSIX mkfifo is installed")
            .success());
        let mut writer = TableWriter::new(
            dir.path().to_owned(),
            "csv.csv",
            &["event"],
            &JsonlTraceConfig::default(),
        );
        assert!(append_locked(&mut writer, &[b"row".to_vec()]).is_err());
    }
}
