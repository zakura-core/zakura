//! Opt-in, monotonic transaction and batch pipeline observations.
//!
//! `FLUSH_EXPERIMENT_TRACE_DIR` enables a bounded writer and `pipeline.jsonl` output.
//! The accompanying statistics file makes queue loss and writer errors observable. Observations
//! never wait for disk or queue capacity and carry no transaction bodies or spending material.

use std::{
    fmt::Display,
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError},
        Arc, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use serde_json::{json, Value};

static SINK: OnceLock<Option<Sink>> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

tokio::task_local! {
    static CURRENT: Context;
}

/// Shared, public identifiers for observations belonging to one verification request.
#[derive(Clone, Debug)]
pub struct Context(Arc<ContextData>);

#[derive(Debug, Serialize)]
struct ContextData {
    txid: Option<String>,
    block: Option<String>,
    source: &'static str,
}

impl Serialize for Context {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_ref().serialize(serializer)
    }
}

/// Correlation captured when a request enters a shared batch queue.
#[derive(Clone, Debug, Default)]
pub struct BatchRequest {
    /// Unique identity within this process, absent when tracing is disabled.
    pub id: Option<u64>,
    /// The transaction or block whose future submitted the item.
    pub context: Option<Context>,
}

#[derive(Default)]
struct Counts {
    enqueued: AtomicU64,
    written: AtomicU64,
    dropped: AtomicU64,
    errors: AtomicU64,
    clock_origin_unix_ns: u64,
    clock_calibration_uncertainty_ns: u64,
}

struct Sink {
    sender: mpsc::SyncSender<Vec<u8>>,
    counts: Arc<Counts>,
    started: Instant,
}

impl Context {
    /// Starts a transaction observation, retaining its parent's block identity when present.
    pub fn transaction(txid: impl Display, mempool: bool) -> Option<Self> {
        enabled().then(|| {
            let block = current().and_then(|parent| parent.0.block.clone());
            Self(Arc::new(ContextData {
                txid: Some(txid.to_string()),
                block,
                source: if mempool { "mempool" } else { "block" },
            }))
        })
    }

    /// Starts an observation for a full semantic block-verification request.
    pub fn block(hash: impl Display) -> Option<Self> {
        enabled().then(|| {
            Self(Arc::new(ContextData {
                txid: None,
                block: Some(hash.to_string()),
                source: "block",
            }))
        })
    }
}

/// Whether the process opted into pipeline observations.
pub fn enabled() -> bool {
    sink().is_some()
}

/// Returns the current request's correlation, if observations are enabled.
pub fn current() -> Option<Context> {
    enabled()
        .then(|| CURRENT.try_with(Clone::clone).ok())
        .flatten()
}

/// Returns a process-unique observation identity without an atomic operation when disabled.
pub fn next_id() -> Option<u64> {
    enabled().then(|| NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

/// Polls a future with its request's correlation; verification behavior is unchanged.
pub async fn scope<F: Future>(context: Option<Context>, future: F) -> F::Output {
    match context {
        Some(context) => CURRENT.scope(context, future).await,
        None => future.await,
    }
}

/// Builds synchronous request objects under their eventual future's correlation.
pub fn sync_scope<T>(context: Option<Context>, build: impl FnOnce() -> T) -> T {
    match context {
        Some(context) => CURRENT.sync_scope(context, build),
        None => build(),
    }
}

/// Captures correlation for a request that will run on a shared batch worker.
pub fn batch_request() -> BatchRequest {
    BatchRequest {
        id: next_id(),
        context: current(),
    }
}

/// Records a stage lazily, using the current transaction or block correlation.
pub fn event(stage: &'static str, details: impl FnOnce() -> Value) {
    if enabled() {
        event_for(current().as_ref(), stage, details);
    }
}

/// Records a stage after work has crossed a task boundary, without blocking its caller.
pub fn event_for(context: Option<&Context>, stage: &'static str, details: impl FnOnce() -> Value) {
    let Some(sink) = sink() else { return };
    let ts_ns = u64::try_from(sink.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let row = json!({"ts_ns": ts_ns, "stage": stage, "context": context, "details": details()});
    let Ok(mut line) = serde_json::to_vec(&row) else {
        sink.counts.errors.fetch_add(1, Ordering::Relaxed);
        return;
    };
    line.push(b'\n');
    sink.counts.enqueued.fetch_add(1, Ordering::Relaxed);
    if sink.sender.try_send(line).is_err() {
        sink.counts.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

fn sink() -> Option<&'static Sink> {
    SINK.get_or_init(|| {
        let directory = std::env::var_os("FLUSH_EXPERIMENT_TRACE_DIR")?;
        let directory = PathBuf::from(directory);
        let started = Instant::now();
        let wall = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        let uncertainty = u64::try_from(started.elapsed().as_nanos()).ok()?;
        let origin = u64::try_from(wall.as_nanos())
            .ok()?
            .saturating_sub(uncertainty / 2);
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(65_536);
        let counts = Arc::new(Counts {
            clock_origin_unix_ns: origin,
            clock_calibration_uncertainty_ns: uncertainty,
            ..Counts::default()
        });
        let writer_counts = counts.clone();
        let writer = std::thread::Builder::new()
            .name("pipeline-trace".into())
            .spawn(move || {
                if write_events(&directory, receiver, &writer_counts).is_err() {
                    writer_counts.errors.fetch_add(1, Ordering::Relaxed);
                    let _ = write_counts(&directory, &writer_counts);
                }
            });
        if writer.is_err() {
            counts.errors.fetch_add(1, Ordering::Relaxed);
        }
        Some(Sink {
            sender,
            counts,
            started,
        })
    })
    .as_ref()
}

fn write_counts(directory: &Path, counts: &Counts) -> std::io::Result<()> {
    let path = directory.join("pipeline-stats.json");
    let temporary = directory.join("pipeline-stats.tmp");
    fs::write(
        &temporary,
        serde_json::to_vec(&json!({
            "enqueued": counts.enqueued.load(Ordering::Relaxed),
            "written": counts.written.load(Ordering::Relaxed),
            "dropped": counts.dropped.load(Ordering::Relaxed),
            "errors": counts.errors.load(Ordering::Relaxed),
            "clock_origin_unix_ns": counts.clock_origin_unix_ns,
            "clock_calibration_uncertainty_ns": counts.clock_calibration_uncertainty_ns,
            "pid": std::process::id(),
        }))?,
    )?;
    fs::rename(temporary, path)
}

fn write_events(
    directory: &Path,
    receiver: mpsc::Receiver<Vec<u8>>,
    counts: &Counts,
) -> std::io::Result<()> {
    fs::create_dir_all(directory)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join("pipeline.jsonl"))?;
    let mut last_counts = Instant::now();
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(line) => {
                let mut buffer = line;
                let mut records = 1;
                while buffer.len() < 256 * 1024 {
                    let Ok(line) = receiver.try_recv() else { break };
                    buffer.extend_from_slice(&line);
                    records += 1;
                }
                file.write_all(&buffer)?;
                counts.written.fetch_add(records, Ordering::Relaxed);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                write_counts(directory, counts)?;
                return Ok(());
            }
        }
        if last_counts.elapsed() >= Duration::from_millis(100) {
            write_counts(directory, counts)?;
            last_counts = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_writer_drains_queued_rows_before_its_final_statistics() {
        let directory = tempfile::tempdir().expect("test directory can be created");
        let (sender, receiver) = mpsc::sync_channel(4);
        sender
            .send(b"{\"stage\":\"first\"}\n".to_vec())
            .expect("queue has capacity");
        sender
            .send(b"{\"stage\":\"second\"}\n".to_vec())
            .expect("queue has capacity");
        drop(sender);
        let counts = Counts::default();
        counts.enqueued.store(2, Ordering::Relaxed);

        write_events(directory.path(), receiver, &counts).expect("the writer can finish its queue");

        let rows =
            fs::read_to_string(directory.path().join("pipeline.jsonl")).expect("trace was written");
        assert_eq!(rows.lines().count(), 2);
        let statistics: Value = serde_json::from_slice(
            &fs::read(directory.path().join("pipeline-stats.json"))
                .expect("final statistics were written"),
        )
        .expect("statistics are JSON");
        assert_eq!(statistics["enqueued"], 2);
        assert_eq!(statistics["written"], 2);
        assert_eq!(statistics["dropped"], 0);
        assert_eq!(statistics["errors"], 0);
    }

    #[test]
    fn an_existing_trace_is_preserved_instead_of_overwritten() {
        let directory = tempfile::tempdir().expect("test directory can be created");
        let trace = directory.path().join("pipeline.jsonl");
        fs::write(&trace, b"previous run\n").expect("the previous trace can be created");
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(sender);

        assert!(write_events(directory.path(), receiver, &Counts::default()).is_err());
        assert_eq!(
            fs::read(trace).expect("previous trace remains readable"),
            b"previous run\n"
        );
    }
}
