//! Shared non-blocking JSONL tracing support for Zebra components.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::PathBuf,
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use serde_json::{Map, Value};
use tokio::{
    io::AsyncWriteExt,
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
/// the oracle reads the JSONL only a few seconds after the final commits, so a
/// 17s flush window left those rows unwritten and the oracle saw a `commit_start`
/// with no matching `commit_finish` even though the node had committed (the live
/// metrics show `applying = 0`). Tracing is opt-in (a `trace_dir` must be set),
/// so the extra fsync cadence is negligible against the debuggability win.
pub const DEFAULT_FILE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Env var used to label every JSONL trace record with a stable node identifier.
pub const NODE_ID_ENV: &str = "ZEBRA_NODE_ID";

/// Envelope columns written for every trace row, in output order.
pub const ENVELOPE_COLUMNS: &[&str] = &["ts", "wall_ts", "node", "process_trace_id"];

/// Trailing CSV column carrying any fields missing from a table's declared header.
///
/// A CSV table has a fixed header, so a row field that no column matches would
/// otherwise be dropped silently. Collecting those fields into a JSON object in
/// this column keeps schema drift visible and non-lossy.
pub const EXTRA_COLUMN: &str = "extra";

/// The on-disk encoding of a trace table.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TraceFormat {
    /// One JSON object per line.
    ///
    /// Self-describing and tolerant of sparse rows, at the cost of repeating
    /// every key name on every row.
    Jsonl,
    /// RFC 4180 CSV with a fixed header written once, when the file is created.
    ///
    /// Worth roughly a third of the uncompressed bytes on dense tables whose
    /// rows populate most columns. Sparse tables with many optional fields pad
    /// every row with empty columns instead, and should stay [`Jsonl`].
    ///
    /// [`Jsonl`]: TraceFormat::Jsonl
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
    /// Create a JSONL trace table definition.
    pub const fn new(table: &'static str, file_name: &'static str) -> Self {
        Self {
            table,
            file_name,
            format: TraceFormat::Jsonl,
            header: &[],
        }
    }

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

/// A serializable typed JSONL trace event.
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
/// `{"summary": {"height": 7}}` becomes `summary.height`, which is the column
/// name DuckDB's `read_json_auto` already produced for the same rows, so
/// switching a table to CSV does not move any column names.
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
        TraceFormat::Jsonl => serde_json::to_vec(row).ok(),
        TraceFormat::Csv => match serde_json::to_value(row).ok()? {
            Value::Object(row) => Some(render_csv_row(header, row)),
            _ => None,
        },
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

/// A typed, non-blocking JSONL event emitter.
#[derive(Clone, Debug)]
pub struct JsonlEventEmitter {
    tracer: JsonlTracer,
    node: std::sync::Arc<str>,
    started: Instant,
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
            started: Instant::now(),
        }
    }

    /// Return the underlying JSONL tracer.
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
        let _ = self.try_emit_event(build);
    }

    /// Lazily build and emit a typed event, reporting why it was not queued.
    ///
    /// Identical to [`emit_event`], but surfaces the reserve outcome so callers
    /// that need to account for dropped rows can do so. Without this, a row
    /// lost to a full queue is indistinguishable from an event that never
    /// happened, which silently biases any measurement drawn from the table.
    ///
    /// [`emit_event`]: JsonlEventEmitter::emit_event
    pub fn try_emit_event<E>(&self, build: impl FnOnce() -> E) -> Result<(), JsonlTraceReserveError>
    where
        E: JsonlTraceEvent,
    {
        let permit = self.tracer.try_reserve()?;

        let event = build();
        let row = JsonlEventEnvelope {
            ts: elapsed_micros(self.started.elapsed()),
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
        }

        Ok(())
    }

    /// Lazily build and emit a raw JSON object for compatibility callers.
    pub fn emit_with(&self, table: JsonlTraceTable, build: impl FnOnce(&mut Map<String, Value>)) {
        let Ok(permit) = self.tracer.try_reserve() else {
            return;
        };

        let mut row = Map::new();
        row.insert(
            "ts".to_string(),
            Value::from(elapsed_micros(self.started.elapsed())),
        );
        row.insert(
            "wall_ts".to_string(),
            Value::String(WallClock::now().to_string()),
        );
        row.insert("node".to_string(), Value::String(self.node.to_string()));
        row.insert(
            "process_trace_id".to_string(),
            Value::String(process_trace_id().to_string()),
        );
        build(&mut row);

        let line = match table.format() {
            TraceFormat::Jsonl => serde_json::to_vec(&Value::Object(row)).ok(),
            TraceFormat::Csv => Some(render_csv_row(table.header(), row)),
        };

        if let Some(line) = line {
            permit.send(JsonlWriteEvent {
                table: table.table(),
                file_name: table.file_name(),
                format: table.format(),
                header: table.header(),
                line,
            });
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
    ts: u64,
    wall_ts: WallClock,
    node: &'a str,
    process_trace_id: &'static str,
    #[serde(flatten)]
    event: &'a E,
}

/// An absolute UTC timestamp, rendered as RFC 3339 with millisecond precision.
///
/// [`JsonlEventEnvelope::ts`] counts microseconds since its emitter was
/// constructed, and a node builds several emitters with independent origins, so
/// `ts` orders rows only within one emitter. Comparing rows across emitters — or
/// across nodes, which is the whole point of a propagation measurement — needs
/// an absolute clock.
///
/// This reads the system clock per event rather than deriving it from the
/// monotonic origin: NTP keeps the system clock disciplined for the length of a
/// run, whereas an offset applied to a monotonic origin accumulates the local
/// crystal's drift (tens of ppm, so hundreds of milliseconds over a few hours —
/// the same order as the propagation delays being measured).
#[derive(Copy, Clone, Debug)]
struct WallClock(chrono::DateTime<chrono::Utc>);

impl WallClock {
    fn now() -> Self {
        Self(chrono::Utc::now())
    }
}

impl fmt::Display for WallClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
    }
}

impl Serialize for WallClock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
    }
}

fn elapsed_micros(elapsed: Duration) -> u64 {
    saturating_micros(elapsed)
}

/// Returns the process-wide node identifier used to tag JSONL trace records.
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

/// Returns an opaque identifier shared by every JSONL emitter in this process.
///
/// The identifier disambiguates appended trace rows across process restarts.
/// Emitter-local monotonic timestamps restart from zero after each restart.
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

/// Settings for the background JSONL writer.
#[derive(Clone, Debug)]
pub struct JsonlTraceConfig {
    /// Bounded queue capacity.
    pub channel_capacity: usize,
    /// Maximum number of events to write in a single batch.
    pub max_batch_events: usize,
    /// How long to wait for more events after receiving the first batch event.
    pub batch_linger: Duration,
    /// Buffered bytes threshold before flushing to the underlying file.
    pub buffer_flush_bytes: usize,
    /// Maximum time between forced file flushes and syncs.
    pub file_flush_interval: Duration,
}

impl Default for JsonlTraceConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            max_batch_events: DEFAULT_MAX_BATCH_EVENTS,
            batch_linger: DEFAULT_BATCH_LINGER,
            buffer_flush_bytes: DEFAULT_BUFFER_FLUSH_BYTES,
            file_flush_interval: DEFAULT_FILE_FLUSH_INTERVAL,
        }
    }
}

#[derive(Clone)]
struct TraceRuntime {
    tx: mpsc::Sender<JsonlWriteEvent>,
}

/// A non-blocking handle for emitting JSONL trace records.
#[derive(Clone)]
pub struct JsonlTracer {
    inner: TraceState,
}

#[derive(Clone)]
enum TraceState {
    Disabled,
    Enabled(TraceRuntime),
}

/// A spawned JSONL writer plus its tracer.
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

/// Reserve errors for the bounded JSONL trace queue.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum JsonlTraceReserveError {
    /// Tracing is disabled.
    Disabled,
    /// The queue is full.
    Full,
    /// The writer task has closed.
    Closed,
}

/// Send errors for the bounded JSONL trace queue.
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
}

impl JsonlTracePermit {
    /// Send a record into the reserved queue slot.
    pub fn send(self, event: JsonlWriteEvent) {
        self.permit.send(event);
    }
}

impl JsonlTracer {
    /// Create a tracer backed by the supplied sender.
    pub fn new(tx: mpsc::Sender<JsonlWriteEvent>) -> Self {
        Self {
            inner: TraceState::Enabled(TraceRuntime { tx }),
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
        Self::spawn_with_config(trace_dir, JsonlTraceConfig::default())
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
        Self::spawn_guard_with_config(trace_dir, JsonlTraceConfig::default())
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
                "JSONL tracing requested without an active Tokio runtime, disabling tracing"
            );
            return JsonlTraceGuard::disabled();
        };

        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let writer = TraceWriter::new(trace_dir.clone(), config);
        let shutdown = CancellationToken::new();
        let writer = handle.spawn(run_trace_writer(rx, writer, shutdown.clone()));
        tracing::info!(?trace_dir, "JSONL tracing enabled");

        JsonlTraceGuard {
            tracer: Self::new(tx),
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
            TraceState::Enabled(runtime) => !runtime.tx.is_closed(),
        }
    }

    /// Returns the remaining queue capacity.
    pub fn capacity(&self) -> usize {
        let TraceState::Enabled(runtime) = &self.inner else {
            return 0;
        };

        runtime.tx.capacity()
    }

    /// Returns the total queue capacity.
    ///
    /// Callers that shed load based on how full the queue is need this as the
    /// denominator for [`capacity`]. Reading it from the channel keeps the two
    /// from disagreeing when the tracer was built with a non-default config.
    ///
    /// [`capacity`]: JsonlTracer::capacity
    pub fn max_capacity(&self) -> usize {
        let TraceState::Enabled(runtime) = &self.inner else {
            return 0;
        };

        runtime.tx.max_capacity()
    }

    /// Try to reserve a queue slot for a trace record.
    pub fn try_reserve(&self) -> Result<JsonlTracePermit, JsonlTraceReserveError> {
        let TraceState::Enabled(runtime) = &self.inner else {
            return Err(JsonlTraceReserveError::Disabled);
        };

        runtime
            .tx
            .clone()
            .try_reserve_owned()
            .map(|permit| JsonlTracePermit { permit })
            .map_err(|error| match error {
                TrySendError::Full(_) => JsonlTraceReserveError::Full,
                TrySendError::Closed(_) => JsonlTraceReserveError::Closed,
            })
    }

    /// Try to send a trace record without blocking.
    pub fn try_send(&self, event: JsonlWriteEvent) -> Result<(), JsonlTraceSendError> {
        let TraceState::Enabled(runtime) = &self.inner else {
            return Err(JsonlTraceSendError::Disabled(event));
        };

        runtime.tx.try_send(event).map_err(|error| match error {
            TrySendError::Full(event) => JsonlTraceSendError::Full(event),
            TrySendError::Closed(event) => JsonlTraceSendError::Closed(event),
        })
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
            let _ = writer.await;
        }
    }
}

impl std::fmt::Debug for JsonlTracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonlTracer").finish()
    }
}

struct TableWriter {
    file: tokio::fs::File,
    encode_buf: Vec<u8>,
}

impl TableWriter {
    fn new(file: tokio::fs::File, buffer_flush_bytes: usize) -> Self {
        Self {
            file,
            encode_buf: Vec::with_capacity(buffer_flush_bytes),
        }
    }

    fn append_line(&mut self, line: &[u8]) {
        self.encode_buf.extend_from_slice(line);
        self.encode_buf.push(b'\n');
    }

    async fn flush_buffer(&mut self, sync_file: bool) -> std::io::Result<()> {
        if !self.encode_buf.is_empty() {
            self.file.write_all(&self.encode_buf).await?;
            self.encode_buf.clear();
            self.file.flush().await?;
        }

        if sync_file {
            self.file.sync_data().await?;
        }

        Ok(())
    }
}

struct TraceWriter {
    trace_dir: PathBuf,
    config: JsonlTraceConfig,
    tables: HashMap<&'static str, TableWriter>,
    disabled_tables: HashSet<&'static str>,
    trace_dir_created: bool,
    last_file_flush: Instant,
}

impl TraceWriter {
    fn new(trace_dir: PathBuf, config: JsonlTraceConfig) -> Self {
        Self {
            trace_dir,
            config,
            tables: HashMap::new(),
            disabled_tables: HashSet::new(),
            trace_dir_created: false,
            last_file_flush: Instant::now(),
        }
    }

    fn has_open_files(&self) -> bool {
        !self.tables.is_empty() || self.disabled_tables.is_empty()
    }

    async fn write_batch(&mut self, batch: Vec<JsonlWriteEvent>, force_flush: bool) {
        for event in batch {
            if self.disabled_tables.contains(event.table) {
                continue;
            }

            let append_result = match self
                .table_writer_mut(event.table, event.file_name, event.format, event.header)
                .await
            {
                Some(table_writer) => {
                    table_writer.append_line(&event.line);
                    Ok(())
                }
                None => Err(()),
            };

            if append_result.is_err() {
                self.disable_table(event.table);
            }
        }

        let flush_file =
            force_flush || self.last_file_flush.elapsed() >= self.config.file_flush_interval;
        let mut failed_tables = Vec::new();

        for (&table_name, table_writer) in &mut self.tables {
            let should_flush_buffer =
                table_writer.encode_buf.len() >= self.config.buffer_flush_bytes;

            if !should_flush_buffer && !flush_file && !force_flush {
                continue;
            }

            if let Err(error) = table_writer.flush_buffer(flush_file || force_flush).await {
                tracing::warn!(
                    ?error,
                    table = table_name,
                    trace_dir = ?self.trace_dir,
                    "disabling trace table after write failure"
                );
                failed_tables.push(table_name);
            }
        }

        if flush_file || force_flush {
            self.last_file_flush = Instant::now();
        }

        for table in failed_tables {
            self.disable_table(table);
        }
    }

    async fn flush_all(&mut self) {
        self.write_batch(Vec::new(), true).await;
    }

    fn disable_table(&mut self, table: &'static str) {
        self.tables.remove(table);
        self.disabled_tables.insert(table);
    }

    async fn table_writer_mut(
        &mut self,
        table: &'static str,
        file_name: &'static str,
        format: TraceFormat,
        header: &'static [&'static str],
    ) -> Option<&mut TableWriter> {
        if self.disabled_tables.contains(table) {
            return None;
        }

        if !self.tables.contains_key(table) {
            if !self.trace_dir_created {
                if let Err(error) = tokio::fs::create_dir_all(&self.trace_dir).await {
                    tracing::warn!(
                        ?error,
                        trace_dir = ?self.trace_dir,
                        "failed to create trace directory, disabling trace table"
                    );
                    self.disabled_tables.insert(table);
                    return None;
                }
                self.trace_dir_created = true;
            }

            let file = match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.trace_dir.join(file_name))
                .await
            {
                Ok(file) => file,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        table,
                        trace_dir = ?self.trace_dir,
                        "failed to open trace table, disabling trace table"
                    );
                    self.disabled_tables.insert(table);
                    return None;
                }
            };

            // A CSV file carries its header on the first line. The file is
            // opened in append mode, so only write it when the file is new —
            // a node restarting into an existing trace dir must not interleave
            // a second header into the middle of the table.
            let write_header = format == TraceFormat::Csv
                && file
                    .metadata()
                    .await
                    .map(|metadata| metadata.len() == 0)
                    .unwrap_or(false);

            let mut table_writer = TableWriter::new(file, self.config.buffer_flush_bytes);
            if write_header {
                table_writer.append_line(&render_csv_header(header));
            }

            self.tables.insert(table, table_writer);
        }

        self.tables.get_mut(table)
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
            _ = flush_tick.tick(), if writer.has_open_files() => {
                writer.flush_all().await;

                if !writer.has_open_files() {
                    tracing::warn!(trace_dir = ?writer.trace_dir, "all trace tables have been disabled");
                    break;
                }

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
                _ = flush_tick.tick(), if writer.has_open_files() => {
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

        if !writer.has_open_files() {
            tracing::warn!(trace_dir = ?writer.trace_dir, "all trace tables have been disabled");
            break;
        }

        if receiver_closed {
            writer.flush_all().await;
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

    const TEST_TABLE: JsonlTraceTable = JsonlTraceTable::new("typed", "typed.jsonl");

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
        assert_eq!(written.file_name, "typed.jsonl");

        let row: Value = serde_json::from_slice(&written.line).expect("valid typed event JSON");
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
        let row: Value = serde_json::from_slice(&written.line).expect("valid raw event JSON");
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
        let row: Value = serde_json::from_slice(&written.line).expect("valid typed event JSON");

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
            "ts,wall_ts,node,process_trace_id,event,value,optional,extra"
        );
        assert_eq!(fields.len(), csv_fields(&header).len());
        assert_eq!(fields[2], "node-csv");
        assert_eq!(fields[3], process_trace_id());
        assert_eq!(fields[4], "csv_event");
        assert_eq!(fields[5], "7");
        // `None` renders as an empty field, which DuckDB and pandas read as null.
        assert_eq!(fields[6], "");
        assert_eq!(fields[7], "");
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
        assert_eq!(fields[4], "7");
        assert_eq!(fields[5], "2");
        assert_eq!(fields[6], "", "no undeclared fields remain");
    }

    #[test]
    fn csv_rows_route_undeclared_fields_to_the_extra_column() {
        const TABLE: JsonlTraceTable = JsonlTraceTable::csv("drift", "drift.csv", &["event"]);

        let mut row = Map::new();
        row.insert("event".to_string(), Value::String("known".to_string()));
        row.insert("added_later".to_string(), Value::from(9));

        let line = String::from_utf8(render_csv_row(TABLE.header(), row)).expect("utf-8");
        let fields = csv_fields(&line);

        assert_eq!(fields[4], "known");
        assert_eq!(
            serde_json::from_str::<Value>(&fields[5]).expect("extra holds a JSON object"),
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
        assert_eq!(fields[4], "peer:1");
        assert_eq!(fields[5], "a,b");
        assert_eq!(fields[6], "say \"hi\"");
        assert_eq!(fields[7], "[1,2]", "arrays are stored as embedded JSON");
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
                line: format!("1,t,n,run{generation},").into_bytes(),
            })
            .await
            .expect("send should succeed");

            drop(tx);
            handle.await.expect("writer task should complete");
        }

        let written = tokio::fs::read_to_string(&path).await.expect("csv file");
        let lines: Vec<_> = written.lines().collect();

        assert_eq!(lines.len(), 3, "one header plus two rows: {written}");
        assert_eq!(lines[0], "ts,wall_ts,node,process_trace_id,event,extra");
        assert_eq!(lines[1], "1,t,n,run0,");
        assert_eq!(
            lines[2], "1,t,n,run1,",
            "a restart appends rows without repeating the header"
        );
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
    async fn writer_task_produces_per_table_jsonl() {
        let dir = tempfile::tempdir().expect("tempdir");
        let trace_dir = dir.path().join("traces");

        let (tx, rx) = mpsc::channel(16);
        let writer = TraceWriter::new(trace_dir.clone(), JsonlTraceConfig::default());
        let handle = tokio::spawn(run_trace_writer(rx, writer, CancellationToken::new()));

        tx.send(JsonlWriteEvent {
            table: "alpha",
            file_name: "alpha.jsonl",
            format: TraceFormat::Jsonl,
            header: &[],
            line: br#"{"value":1}"#.to_vec(),
        })
        .await
        .expect("send should succeed");

        tx.send(JsonlWriteEvent {
            table: "beta",
            file_name: "beta.jsonl",
            format: TraceFormat::Jsonl,
            header: &[],
            line: br#"{"value":2}"#.to_vec(),
        })
        .await
        .expect("send should succeed");

        drop(tx);
        handle.await.expect("writer task should complete");

        let alpha = tokio::fs::read_to_string(trace_dir.join("alpha.jsonl"))
            .await
            .expect("alpha file");
        let beta = tokio::fs::read_to_string(trace_dir.join("beta.jsonl"))
            .await
            .expect("beta file");

        assert_eq!(alpha.trim(), "{\"value\":1}");
        assert_eq!(beta.trim(), "{\"value\":2}");
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
            file_name: "alpha.jsonl",
            format: TraceFormat::Jsonl,
            header: &[],
            line: br#"{"value":1}"#.to_vec(),
        })
        .await
        .expect("send should succeed");

        time::sleep(Duration::from_millis(80)).await;

        let alpha = tokio::fs::read_to_string(trace_dir.join("alpha.jsonl"))
            .await
            .expect("alpha file should be flushed while the writer is idle");

        assert_eq!(alpha.trim(), "{\"value\":1}");

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
            file_name: "alpha.jsonl",
            format: TraceFormat::Jsonl,
            header: &[],
            line: br#"{"value":1}"#.to_vec(),
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
                file_name: "alpha.jsonl",
                format: TraceFormat::Jsonl,
                header: &[],
                line: br#"{"value":1}"#.to_vec(),
            })
            .expect("queued row");

        guard.shutdown().await;

        let alpha = tokio::fs::read_to_string(trace_dir.join("alpha.jsonl"))
            .await
            .expect("alpha file");
        assert_eq!(alpha.trim(), "{\"value\":1}");
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
                file_name: "alpha.jsonl",
                format: TraceFormat::Jsonl,
                header: &[],
                line: br#"{"value":1}"#.to_vec(),
            })
            .expect("first row fits");
        let full = tracer.try_send(JsonlWriteEvent {
            table: "alpha",
            file_name: "alpha.jsonl",
            format: TraceFormat::Jsonl,
            header: &[],
            line: br#"{"value":2}"#.to_vec(),
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
                file_name: "alpha.jsonl",
                format: TraceFormat::Jsonl,
                header: &[],
                line: br#"{"value":0}"#.to_vec(),
            })
            .expect("first row fits");

        let start = Instant::now();
        let mut full = 0;
        for value in 0..10_000 {
            let result = tracer.try_send(JsonlWriteEvent {
                table: "alpha",
                file_name: "alpha.jsonl",
                format: TraceFormat::Jsonl,
                header: &[],
                line: format!(r#"{{"value":{value}}}"#).into_bytes(),
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
}
