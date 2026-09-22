//! Optional block timelines. Producers never serialize, write files, or wait for the collector.
//!
//! Context crosses async boundaries only while a future is polled. Synchronous worker jobs
//! must explicitly enter their captured context. This module does not require an async runtime.

use std::{
    cell::RefCell,
    future::Future,
    marker::PhantomData,
    path::PathBuf,
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crossbeam_channel::{bounded, Receiver, Sender};
use serde::{Deserialize, Serialize};

/// Version of the wire schema and timing definitions.
pub const SCHEMA_VERSION: u32 = 1;
/// Maximum simultaneous retained attempt contexts.
const MAX_ATTEMPTS: usize = 1024;
/// Fine-detail records per attempt. Summaries use a separate queue.
const MAX_SPANS: u64 = 256;
/// Fixed-size event queue slots. Together with summaries and contexts this is below 64 MiB.
const DETAIL_CAPACITY: usize = 16_384;
const SUMMARY_CAPACITY: usize = 2048;

static RECORDER: OnceLock<Arc<Recorder>> = OnceLock::new();
thread_local! { static CURRENT: RefCell<Context> = const { RefCell::new(Context(None)) }; }

/// Disabled unless a local collector socket is explicitly configured.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Local Unix datagram socket owned by the collector. Missing collectors never block a node.
    pub socket: Option<PathBuf>,
    /// Operator-assigned node label, limited to 128 bytes.
    pub node: String,
    /// Session label shared across restarts, limited to 128 bytes.
    pub session: String,
}

/// Run identity, build and timing boundaries. No credentials or arbitrary configuration is sent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    /// Unique process invocation, independent of reused PIDs and block heights.
    pub id: String,
    /// Operator node label.
    pub node: String,
    /// Operator session label.
    pub session: String,
    /// Network name.
    pub network: String,
    /// Build identifier provided by the node.
    pub build: String,
    /// Archive or pruned storage configuration, without paths or secrets.
    pub storage: String,
    /// Operating-system process ID.
    pub pid: u32,
    /// UTC anchor for display. Durations always use the monotonic clock.
    pub utc_start_ms: u64,
    /// Linux CLOCK_MONOTONIC anchor, for perf --clockid mono correlation.
    pub monotonic_start_us: Option<u64>,
    /// Maximum anchor acquisition interval. Samples too close to boundaries remain unattributed.
    pub clock_error_us: u64,
}

/// Verification route, kept separate in latency distributions.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Full semantic verification and state application.
    Semantic,
    /// Checkpoint range residence and application.
    Checkpoint,
    /// Proposal validation without commit.
    Proposal,
    /// Cached mining preparation without commit.
    Preparation,
}

/// Stable timing categories. Elapsed spans can overlap and must not be summed blindly.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// Root verifier request, beginning at router entry after caller readiness.
    VerifierRequest,
    /// Existing-block and pending-commit lookup.
    KnownBlock,
    /// Header, commitments and block structure checks.
    BlockChecks,
    /// Parent state needed by verification.
    ParentState,
    /// Transaction fanout and join, including async dependencies.
    Transactions,
    /// One transaction's verification envelope.
    Transaction,
    /// Async transaction checks. May include shared cryptographic batches.
    TransactionChecks,
    /// Transaction UTXO dependency wait.
    TransactionInputs,
    /// Sapling request through its result, including batch wait and cache hits.
    SaplingRequest,
    /// Orchard or Ironwood Halo2 request through its result, including batch wait.
    Halo2Request,
    /// Readiness of the state service before submitting a commit.
    StateReady,
    /// State request through caller response, including queues.
    StateResponse,
    /// A synchronous worker's dispatch-to-start delay.
    WorkerQueue,
    /// Elapsed execution of a synchronous worker, not CPU time.
    WorkerExecution,
    /// State writer queue residence.
    WriterQueue,
    /// Writer processing, which can continue after caller response.
    WriterOccupied,
    /// Contextual checks and state application.
    Contextual,
    /// Parent chain selection.
    ParentChain,
    /// Construction of a new chain.
    ChainNew,
    /// Snapshot of unspent outputs.
    UtxoSnapshot,
    /// Transparent spend checks.
    TransparentSpend,
    /// Shielded anchor checks.
    ShieldedAnchors,
    /// Fetching Sprout anchors.
    SproutAnchors,
    /// Contextual block construction.
    BlockConstruction,
    /// Parallel contextual updates, represented as an envelope.
    ParallelUpdate,
    /// Block commitment work.
    BlockCommitment,
    /// Sprout anchor validation.
    SproutAnchorCheck,
    /// Chain snapshot cloning.
    ChainClone,
    /// Updating the chain and commitment trees.
    ChainPush,
    /// Initial contextual checks.
    InitialChecks,
    /// State snapshot cloning.
    SnapshotClone,
    /// Preparing header/state transition.
    HeaderTransitionPrepare,
    /// Committing header/state transition.
    HeaderTransitionCommit,
    /// Publishing result and sending response.
    Publication,
    /// Finalizing older blocks after a semantic response.
    Finalization,
}

/// One attempt's immutable identity. Hashes use their native byte representation on the wire.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Block {
    /// Hash of this block, in internal byte order.
    pub hash: [u8; 32],
    /// Parent hash, in internal byte order.
    pub parent: [u8; 32],
    /// Claimed height, before verification.
    pub height: Option<u32>,
    /// Number of transactions, without payloads or transaction identifiers.
    pub transactions: u32,
    /// Selected verification route.
    pub mode: Mode,
}

/// The caller's outcome. Failure is not necessarily a consensus rejection.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Committed or successfully checked for the recorded mode.
    Success,
    /// Request was already known.
    Duplicate,
    /// Verifier returned an error.
    Failed,
    /// Future dropped before returning. Submitted state work may continue.
    Abandoned,
}

/// Fixed-size hot-path record. Summary records contain their own start and block metadata.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// All owners released this attempt, including work after the caller response.
    Seal {
        /// Attempt identity.
        attempt: u64,
        /// Number of detail spans started, including spans omitted by the cap.
        spans: u64,
        /// Detail records lost before export.
        dropped: u64,
    },
    /// An admitted profiling attempt.
    Start {
        /// Attempt identity within a run.
        attempt: u64,
        /// Begin offset from the run's monotonic epoch.
        start_us: u64,
        /// Block identity.
        block: Block,
    },
    /// A completed elapsed span, including spans finishing after a caller response.
    Span {
        /// Owning attempt.
        attempt: u64,
        /// Unique span ID within this attempt.
        span: u64,
        /// Parent span ID, zero for the root.
        parent: u64,
        /// Timing category.
        stage: Stage,
        /// Begin offset.
        start_us: u64,
        /// End offset.
        end_us: u64,
        /// Thread at completion, not proof of exclusive CPU ownership.
        completion_thread: Option<u64>,
    },
    /// Self-contained root completion.
    Finish {
        /// Attempt identity.
        attempt: u64,
        /// Begin offset.
        start_us: u64,
        /// End offset.
        end_us: u64,
        /// Block identity.
        block: Block,
        /// Caller result.
        outcome: Outcome,
        /// Detail records omitted for this attempt so far.
        dropped: u64,
    },
}

/// IPC frames. Run metadata is repeated so collector restarts can recover without node restarts.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// Run metadata.
    Run {
        /// Wire/timing version.
        schema: u32,
        /// Run identity and anchors.
        run: Run,
    },
    /// Sequenced application data.
    Event {
        /// Wire/timing version.
        schema: u32,
        /// Run identity.
        run_id: String,
        /// Producer sequence, exposing transport loss.
        sequence: u64,
        /// Fixed-size payload.
        data: Event,
    },
    /// Independent coverage and backpressure counters.
    Health {
        /// Wire/timing version.
        schema: u32,
        /// Run identity.
        run_id: String,
        /// Monotonic offset.
        at_us: u64,
        /// Attempts seen, including attempts omitted under pressure.
        attempts: u64,
        /// Events/context allocation omitted in the node.
        dropped: u64,
        /// Datagrams not delivered.
        transport_dropped: u64,
    },
}

struct Recorder {
    started: Instant,
    detail: Sender<Event>,
    summary: Sender<Event>,
    attempts: AtomicU64,
    active: AtomicUsize,
    dropped: AtomicU64,
    stopped: AtomicBool,
}

impl Recorder {
    fn now(&self) -> u64 {
        micros(self.started.elapsed())
    }
    fn emit(&self, event: Event, summary: bool) -> bool {
        let queue = if summary { &self.summary } else { &self.detail };
        if queue.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            false
        } else {
            true
        }
    }
}

struct Attempt {
    recorder: Arc<Recorder>,
    id: u64,
    next_span: AtomicU64,
    requested_spans: AtomicU64,
    fine_spans: AtomicU64,
    dropped: AtomicU64,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        self.recorder.emit(
            Event::Seal {
                attempt: self.id,
                spans: self.requested_spans.load(Ordering::Relaxed),
                dropped: self.dropped.load(Ordering::Relaxed),
            },
            true,
        );
        self.recorder.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Propagated profiling context. Does not own blocks, transactions, or consensus state.
#[derive(Clone, Default)]
pub struct Context(Option<(Arc<Attempt>, u64)>);
impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileContext")
            .field("attempt", &self.0.as_ref().map(|(a, _)| a.id))
            .finish()
    }
}
impl Context {
    /// Context of the currently executing poll or explicitly entered worker.
    pub fn current() -> Self {
        CURRENT.with(|c| c.borrow().clone())
    }
    /// Run synchronous work under this context, restoring the previous scope on panic.
    pub fn in_scope<T>(&self, f: impl FnOnce() -> T) -> T {
        let _entered = self.enter();
        f()
    }
    /// Enter a synchronous scope. Never hold this guard across an await.
    pub fn enter(&self) -> Entered {
        Entered {
            previous: Some(CURRENT.with(|c| c.replace(self.clone()))),
            _not_send: PhantomData,
        }
    }
    /// Enter context only while polling, never while a future is pending on a shared worker.
    pub fn wrap<F: Future>(&self, future: F) -> Instrumented<F> {
        Instrumented {
            future,
            context: self.clone(),
        }
    }
    /// Start an elapsed child span. The guard can safely live across await points.
    pub fn span(&self, stage: Stage) -> Span {
        let Some((attempt, parent)) = &self.0 else {
            return Span::default();
        };
        attempt.requested_spans.fetch_add(1, Ordering::Relaxed);
        let fine = matches!(
            stage,
            Stage::Transaction
                | Stage::TransactionChecks
                | Stage::TransactionInputs
                | Stage::SaplingRequest
                | Stage::Halo2Request
                | Stage::WorkerExecution
                | Stage::WorkerQueue
        );
        if fine && attempt.fine_spans.fetch_add(1, Ordering::Relaxed) >= 128 {
            attempt.dropped.fetch_add(1, Ordering::Relaxed);
            attempt.recorder.dropped.fetch_add(1, Ordering::Relaxed);
            return Span::default();
        }
        let id = attempt.next_span.fetch_add(1, Ordering::Relaxed);
        if id > MAX_SPANS {
            attempt.dropped.fetch_add(1, Ordering::Relaxed);
            attempt.recorder.dropped.fetch_add(1, Ordering::Relaxed);
            return Span::default();
        }
        Span {
            context: Self(Some((attempt.clone(), id))),
            parent: *parent,
            stage,
            start_us: attempt.recorder.now(),
            finished: false,
        }
    }
    /// Record an already measured synchronous phase without allocating or formatting its name.
    pub fn duration(&self, stage: Stage, elapsed: Duration) {
        let mut span = self.span(stage);
        span.start_us = span.start_us.saturating_sub(micros(elapsed));
    }
}

/// Synchronous scope restoration guard. It cannot move between threads.
pub struct Entered {
    previous: Option<Context>,
    _not_send: PhantomData<Rc<()>>,
}
impl Drop for Entered {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            CURRENT.with(|c| {
                c.replace(previous);
            });
        }
    }
}

#[pin_project::pin_project]
/// A future that restores its profile context on every poll, including worker migration.
pub struct Instrumented<F> {
    #[pin]
    future: F,
    context: Context,
}
impl<F: Future> Future for Instrumented<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if this.context.0.is_none() && !enabled() {
            return this.future.poll(cx);
        }
        let _entered = this.context.enter();
        this.future.poll(cx)
    }
}

/// A bounded elapsed span. Dropping emits one complete record, not a blocking export.
pub struct Span {
    context: Context,
    parent: u64,
    stage: Stage,
    start_us: u64,
    finished: bool,
}
impl Default for Span {
    fn default() -> Self {
        Self {
            context: Context::default(),
            parent: 0,
            stage: Stage::VerifierRequest,
            start_us: 0,
            finished: true,
        }
    }
}
impl Span {
    /// Context for child work. Pass this explicitly to synchronous workers.
    pub fn context(&self) -> Context {
        self.context.clone()
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some((attempt, id)) = &self.context.0 {
            if !attempt.recorder.emit(
                Event::Span {
                    attempt: attempt.id,
                    span: *id,
                    parent: self.parent,
                    stage: self.stage,
                    start_us: self.start_us,
                    end_us: attempt.recorder.now(),
                    completion_thread: thread_id(),
                },
                false,
            ) {
                attempt.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Self-contained root result, emitted even if fine detail filled its queue.
pub struct Root {
    context: Context,
    block: Block,
    start_us: u64,
    outcome: Outcome,
}
impl Root {
    /// Root context to carry into verifier futures and state work.
    pub fn context(&self) -> Context {
        self.context.clone()
    }
    /// Record the result actually returned to this caller.
    pub fn finish(mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        if let Some((attempt, _)) = &self.context.0 {
            attempt.recorder.emit(
                Event::Finish {
                    attempt: attempt.id,
                    block: self.block,
                    start_us: self.start_us,
                    end_us: attempt.recorder.now(),
                    outcome: self.outcome,
                    dropped: attempt.dropped.load(Ordering::Relaxed),
                },
                true,
            );
        }
    }
}

/// Whether recording was configured. Check before computing block metadata solely for profiling.
pub fn enabled() -> bool {
    RECORDER
        .get()
        .is_some_and(|r| !r.stopped.load(Ordering::Relaxed))
}

/// Report a best-effort handoff that could not recover its attempt context.
/// It is run-level loss because assigning it to a guessed attempt would be misleading.
pub fn context_lost() {
    if let Some(recorder) = RECORDER.get() {
        recorder.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Begin a router-entry attempt, returning no guard if disabled or all contexts are occupied.
pub fn begin(block: Block) -> Option<Root> {
    begin_with(RECORDER.get()?, block)
}
fn begin_with(recorder: &Arc<Recorder>, block: Block) -> Option<Root> {
    let id = recorder
        .attempts
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    if recorder.stopped.load(Ordering::Relaxed)
        || recorder
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < MAX_ATTEMPTS).then_some(n + 1)
            })
            .is_err()
    {
        recorder.dropped.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    let start_us = recorder.now();
    let context = Context(Some((
        Arc::new(Attempt {
            recorder: recorder.clone(),
            id,
            next_span: AtomicU64::new(1),
            requested_spans: AtomicU64::new(0),
            fine_spans: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }),
        0,
    )));
    recorder.emit(
        Event::Start {
            attempt: id,
            start_us,
            block,
        },
        true,
    );
    Some(Root {
        context,
        block,
        start_us,
        outcome: Outcome::Abandoned,
    })
}

/// Stops the exporter without waiting for the collector. Profiling cannot delay node shutdown.
pub struct Runtime {
    recorder: Arc<Recorder>,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.recorder.stopped.store(true, Ordering::Release);
    }
}

/// Install one recorder for this process. Invalid configuration returns an error to log, not panic.
/// A missing collector is allowed and recovered automatically. Call once during node startup.
pub fn start(
    config: &Config,
    network: String,
    build: String,
    storage: String,
) -> std::io::Result<Option<Runtime>> {
    let Some(path) = &config.socket else {
        return Ok(None);
    };
    if config.node.len() > 128
        || config.session.len() > 128
        || network.len() > 128
        || build.len() > 256
        || storage.len() > 256
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "profile metadata exceeds limits",
        ));
    }
    let (recorder, detail, summary) = recorder();
    let before = recorder.now();
    let mono = monotonic_us();
    let utc = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let after = recorder.now();
    let run = Run {
        id: format!("{:032x}", rand::random::<u128>()),
        node: config.node.clone(),
        session: config.session.clone(),
        network,
        build,
        storage,
        pid: std::process::id(),
        utc_start_ms: micros(utc).saturating_sub(before) / 1000,
        monotonic_start_us: mono.map(|t| t.saturating_sub(before)),
        clock_error_us: after.saturating_sub(before).saturating_add(1),
    };
    RECORDER.set(recorder.clone()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "profile recorder already installed",
        )
    })?;
    let path = path.clone();
    let exporter = recorder.clone();
    if let Err(error) = std::thread::Builder::new()
        .name("block-profile-export".into())
        .spawn(move || export(path, run, exporter, detail, summary))
    {
        recorder.stopped.store(true, Ordering::Release);
        return Err(error);
    }
    Ok(Some(Runtime { recorder }))
}

fn recorder() -> (Arc<Recorder>, Receiver<Event>, Receiver<Event>) {
    let (detail_tx, detail) = bounded(DETAIL_CAPACITY);
    let (summary_tx, summary) = bounded(SUMMARY_CAPACITY);
    (
        Arc::new(Recorder {
            started: Instant::now(),
            detail: detail_tx,
            summary: summary_tx,
            attempts: AtomicU64::new(0),
            active: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        }),
        detail,
        summary,
    )
}

#[cfg(unix)]
fn export(
    path: PathBuf,
    run: Run,
    recorder: Arc<Recorder>,
    detail: Receiver<Event>,
    summary: Receiver<Event>,
) {
    let Ok(socket) = std::os::unix::net::UnixDatagram::unbound() else {
        recorder.stopped.store(true, Ordering::Release);
        return;
    };
    if socket.set_nonblocking(true).is_err() {
        recorder.stopped.store(true, Ordering::Release);
        return;
    }
    let mut sequence = 0u64;
    let transport_dropped = std::cell::Cell::new(0u64);
    let mut health = Instant::now() - Duration::from_secs(2);
    let mut connected = false;
    let send = |frame: &Frame| {
        let result = serde_json::to_vec(frame)
            .ok()
            .filter(|bytes| bytes.len() <= 8192)
            .is_some_and(|bytes| socket.send_to(&bytes, &path).is_ok());
        if !result {
            transport_dropped.set(transport_dropped.get().saturating_add(1));
        }
        result
    };
    let mut stopping = None;
    loop {
        if recorder.stopped.load(Ordering::Acquire) {
            let since = stopping.get_or_insert_with(Instant::now);
            if (detail.is_empty() && summary.is_empty())
                || since.elapsed() > Duration::from_millis(100)
            {
                break;
            }
        }
        if health.elapsed() >= Duration::from_secs(1) {
            connected = send(&Frame::Run {
                schema: SCHEMA_VERSION,
                run: run.clone(),
            });
            // Counts are independent of the fine-detail queue, which may be saturated.
            send(&Frame::Health {
                schema: SCHEMA_VERSION,
                run_id: run.id.clone(),
                at_us: recorder.now(),
                attempts: recorder.attempts.load(Ordering::Relaxed),
                dropped: recorder.dropped.load(Ordering::Relaxed),
                transport_dropped: transport_dropped.get(),
            });
            health = Instant::now();
        }
        for _ in 0..256 {
            let event = summary.try_recv().or_else(|_| detail.try_recv());
            let Ok(data) = event else {
                break;
            };
            sequence = sequence.saturating_add(1);
            if connected {
                send(&Frame::Event {
                    schema: SCHEMA_VERSION,
                    run_id: run.id.clone(),
                    sequence,
                    data,
                });
            } else {
                transport_dropped.set(transport_dropped.get().saturating_add(1));
            }
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}
#[cfg(not(unix))]
fn export(_: PathBuf, _: Run, recorder: Arc<Recorder>, _: Receiver<Event>, _: Receiver<Event>) {
    recorder.stopped.store(true, Ordering::Release);
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
#[cfg(target_os = "linux")]
fn thread_id() -> Option<u64> {
    u64::try_from(rustix::thread::gettid().as_raw_nonzero().get()).ok()
}
#[cfg(not(target_os = "linux"))]
fn thread_id() -> Option<u64> {
    None
}
#[cfg(target_os = "linux")]
fn monotonic_us() -> Option<u64> {
    let value = rustix::time::clock_gettime(rustix::time::ClockId::Monotonic);
    Some(
        u64::try_from(value.tv_sec)
            .ok()?
            .checked_mul(1_000_000)?
            .checked_add(u64::try_from(value.tv_nsec).ok()? / 1000)?,
    )
}
#[cfg(not(target_os = "linux"))]
fn monotonic_us() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests;
