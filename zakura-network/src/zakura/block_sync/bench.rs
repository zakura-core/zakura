//! Offline benchmark helper for the block-sync [`Sequencer`].
//!
//! Spawns the **real** [`SequencerTask`] — the
//! body reorder + ordered-submit pipeline — with no peers and no reactor, and exposes
//! a minimal handle (split into parts via [`BenchSequencerHandle::into_parts`]) to
//! drive it from an offline benchmark:
//!
//! * [`BenchBodyFeeder::feed_body`] feeds a downloaded body into the reorder queue
//!   (the task's real bounded body input);
//! * [`BenchSubmissions::next_submit`] drains the ordered `SubmitBlock` actions the
//!   sequencer emits once a body is contiguous above the verified tip;
//! * [`BenchCommitter::apply_committed`] reports a commit back, which advances the
//!   sequencer frontier and releases the next contiguous blocks.
//!
//! The caller (e.g. `zebra-replay-bench`'s `apply-sequencer`) supplies the
//! verify+commit between `next_submit` and `apply_committed`, using the same real
//! checkpoint verifier + state service as the `apply-verifier` rung. This is
//! feature-gated (`internal-bench`) and is not part of the production API.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use zakura_chain::block::{self, Block};
use zakura_jsonl_trace::{JsonlTraceGuard, JsonlTracer};

use crate::zakura::{
    trace::{block_sync_trace as bs_trace, BLOCK_SYNC_TABLE},
    transport::ByteBudget,
    ZakuraPeerId, ZakuraTrace,
};

use super::{
    events::{BlockApplyResult, BlockApplyToken, BlockSyncAction},
    reactor::{bs_insert_height, bs_insert_u64},
    reorder::BufferedBlockBody,
    sequencer::Sequencer,
    sequencer_task::{
        initial_view, SequencedBody, SequencerControlInput, SequencerTask, SequencerView,
    },
    state::{BlockSyncFrontiers, ThroughputMeter},
    work_queue::WorkQueue,
};

/// How long the sequencer task waits to send an action before giving up. Generous
/// for the bench (the action channel is drained by the caller's commit loop).
const BENCH_ACTION_SEND_TIMEOUT: Duration = Duration::from_secs(60);

/// A block the sequencer ordered and asked the caller to commit. The `token` must
/// be echoed back via [`BenchCommitter::apply_committed`] after committing.
#[derive(Clone, Debug)]
pub struct BenchSubmit {
    /// Submission token to echo back on commit.
    pub token: BlockApplyToken,
    /// The block to verify and commit (contiguous above the verified tip).
    pub block: Arc<Block>,
}

/// A read-only progress snapshot copied from the internal sequencer view.
#[derive(Copy, Clone, Debug)]
pub struct SequencerProgress {
    /// Verified block tip (last committed height the sequencer knows about).
    pub verified_tip: block::Height,
    /// Bodies buffered out-of-order in the reorder queue.
    pub reorder_len: u64,
    /// Bodies drained into the contiguous `applying` set.
    pub applying_len: u64,
    /// The sequencer's own committed-throughput estimate.
    pub committed_blocks_per_sec: u64,
}

/// Drives the real block-sync `SequencerTask` for an offline benchmark. Split into
/// independent parts via [`into_parts`](Self::into_parts) so the feed, submission
/// drain, and commit feedback can run concurrently without borrow conflicts.
pub struct BenchSequencerHandle {
    feeder: BenchBodyFeeder,
    submissions: BenchSubmissions,
    committer: BenchCommitter,
}

/// A cloneable handle for feeding bodies into the sequencer's reorder queue.
#[derive(Clone)]
pub struct BenchBodyFeeder {
    body_input: mpsc::Sender<SequencedBody>,
    body_input_bytes: Arc<AtomicU64>,
    body_input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
    bench_peer: ZakuraPeerId,
}

/// Drains the ordered `SubmitBlock`s the sequencer emits (the `&mut` side).
pub struct BenchSubmissions {
    actions: mpsc::Receiver<BlockSyncAction>,
}

/// Reports commit completions back to the sequencer and reads its progress view.
pub struct BenchCommitter {
    control: mpsc::UnboundedSender<SequencerControlInput>,
    view: watch::Receiver<SequencerView>,
    /// Bytes currently queued in the sequencer body-input channel
    body_input_bytes: Arc<AtomicU64>,
    body_input_decoded_attributed_memory_bytes: Arc<AtomicU64>,
    // A clone of the sequencer's trace emitter, so the bench driver can write the
    // periodic `block_sync_state` snapshot rows the full reactor emits in production
    // (the rows the zakura-trace-plots skill consumes).
    trace: ZakuraTrace,
    finalized_height: block::Height,
    // The JSONL trace writer guard (when `trace_dir` was supplied). Flushed via
    // [`BenchCommitter::flush_trace`] so the trace tables are complete for review.
    trace_guard: Option<JsonlTraceGuard>,
    // Keeps the sequencer task alive for the lifetime of the committer.
    _join: JoinHandle<()>,
}

/// Spawns the real `SequencerTask` starting from `verified_block_tip` (typically
/// `start - 1`), with no peers and no reactor.
///
/// `submit_in_flight_limit` caps blocks submitted-but-not-applied; `max_inflight_bytes`
/// caps total in-flight body bytes (reorder + applying), which backpressures the feed
/// so the `applying` buffer can't grow unbounded — keep it finite for large windows.
///
/// When `trace_dir` is `Some`, the sequencer's structured Zakura JSONL trace tables
/// (the `BLOCK_SYNC_STATE` body lifecycle, etc.) are written there — the same tables
/// `perf-run-mainnet` produces via `[network.zakura] trace_dir`. The writer is flushed
/// by [`BenchCommitter::flush_trace`]. `None` runs with a no-op tracer (zero overhead).
pub fn spawn_bench_sequencer(
    finalized_height: block::Height,
    verified_block_tip: block::Height,
    verified_block_hash: block::Hash,
    submit_in_flight_limit: usize,
    max_inflight_bytes: u64,
    trace_dir: Option<PathBuf>,
) -> BenchSequencerHandle {
    let frontiers = BlockSyncFrontiers {
        finalized_height,
        verified_block_tip,
        verified_block_hash,
    };
    let limit = submit_in_flight_limit.max(1);

    // Real JSONL trace (same path as production) when a directory is supplied; the
    // guard is handed to the committer so the bench can flush+drain it at the end.
    let (trace, trace_guard) = match trace_dir {
        Some(dir) => {
            let guard = JsonlTracer::spawn_guard(dir);
            let trace = ZakuraTrace::new(guard.tracer(), "01");
            (trace, Some(guard))
        }
        None => (ZakuraTrace::noop(), None),
    };

    let sequencer = Sequencer::new(verified_block_tip, limit);
    let throughput = ThroughputMeter::new(Instant::now());
    let budget = ByteBudget::new(max_inflight_bytes.max(1));
    let work = Arc::new(WorkQueue::new(verified_block_tip));

    let (actions_tx, actions_rx) = mpsc::channel(limit + 128);
    let (body_input_tx, body_input_rx) = mpsc::channel(limit);
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let body_input_bytes = Arc::new(AtomicU64::new(0));
    let body_input_decoded_attributed_memory_bytes = Arc::new(AtomicU64::new(0));
    let (view_tx, view_rx) = watch::channel(initial_view(frontiers));

    let task = SequencerTask::new(
        sequencer,
        budget,
        work,
        actions_tx,
        throughput,
        frontiers,
        body_input_rx,
        control_rx,
        body_input_bytes.clone(),
        body_input_decoded_attributed_memory_bytes.clone(),
        view_tx,
        BENCH_ACTION_SEND_TIMEOUT,
        trace.clone(),
    );
    let join = tokio::spawn(task.run());

    BenchSequencerHandle {
        feeder: BenchBodyFeeder {
            body_input: body_input_tx,
            body_input_bytes: body_input_bytes.clone(),
            body_input_decoded_attributed_memory_bytes: body_input_decoded_attributed_memory_bytes
                .clone(),
            bench_peer: ZakuraPeerId::new(vec![0xB1; 32]).expect("32-byte bench peer id is valid"),
        },
        submissions: BenchSubmissions {
            actions: actions_rx,
        },
        committer: BenchCommitter {
            control: control_tx,
            view: view_rx,
            body_input_bytes,
            body_input_decoded_attributed_memory_bytes,
            trace,
            finalized_height,
            trace_guard,
            _join: join,
        },
    }
}

impl BenchSequencerHandle {
    /// Splits into the (feeder, submissions, committer) parts so each can be driven
    /// on its own task.
    pub fn into_parts(self) -> (BenchBodyFeeder, BenchSubmissions, BenchCommitter) {
        (self.feeder, self.submissions, self.committer)
    }
}

impl BenchBodyFeeder {
    /// Feeds one body into the reorder queue (awaits on backpressure). Returns
    /// `false` if the sequencer task has gone away.
    pub async fn feed_body(
        &self,
        height: block::Height,
        hash: block::Hash,
        block: Arc<Block>,
        bytes: u64,
    ) -> bool {
        let previous_block_hash = block.header.previous_block_hash;
        let body = BufferedBlockBody::from_decoded_block(block, None);
        let body = SequencedBody::new_queued(
            height,
            hash,
            previous_block_hash,
            body,
            bytes,
            self.bench_peer.clone(),
            Instant::now(),
            self.body_input_bytes.clone(),
            self.body_input_decoded_attributed_memory_bytes.clone(),
        );
        self.body_input.send(body).await.is_ok()
    }
}

impl BenchSubmissions {
    /// Drains the next ordered submission, skipping non-submit actions (the bench
    /// has no peers/reactor, so only `SubmitBlock`/`Misbehavior` appear). Returns
    /// `None` once the action channel closes.
    pub async fn next_submit(&mut self) -> Option<BenchSubmit> {
        while let Some(action) = self.actions.recv().await {
            if let BlockSyncAction::SubmitBlock { token, block } = action {
                return Some(BenchSubmit { token, block });
            }
        }
        None
    }
}

impl BenchCommitter {
    /// Reports that a submitted block committed, advancing the sequencer frontier so
    /// it releases the next contiguous blocks.
    pub fn apply_committed(
        &self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
    ) {
        let local_frontier = BlockSyncFrontiers {
            finalized_height: self.finalized_height,
            verified_block_tip: height,
            verified_block_hash: hash,
        };
        let _ = self.control.send(SequencerControlInput::ApplyFinished {
            token,
            height,
            hash,
            result: BlockApplyResult::Committed,
            local_frontier: Some(local_frontier),
        });
    }

    /// Emit one `block_sync_state` snapshot row into `block_sync.jsonl`, mirroring the
    /// periodic row the full block-sync reactor writes in production (the row the
    /// zakura-trace-plots skill reads: `verified_block_tip`, `applying`, `reorder`,
    /// `submitted_applies`, and the in-flight byte counters). Cheap and non-blocking;
    /// a no-op when tracing is disabled. Call it on a cadence from the bench driver.
    pub fn emit_state_snapshot(&self) {
        let view = *self.view.borrow();
        let sequencer_input_decoded_attributed_memory_bytes = self
            .body_input_decoded_attributed_memory_bytes
            .load(Ordering::Relaxed);
        self.trace.emit_with(BLOCK_SYNC_TABLE, |row| {
            row.insert(
                bs_trace::EVENT.to_string(),
                Value::String(bs_trace::BLOCK_SYNC_STATE.to_string()),
            );
            bs_insert_height(row, bs_trace::VERIFIED_BLOCK_TIP, view.verified_tip);
            bs_insert_u64(row, bs_trace::APPLYING, view.applying_len);
            bs_insert_u64(row, bs_trace::REORDER, view.reorder_len);
            bs_insert_u64(
                row,
                bs_trace::SUBMITTED_APPLIES,
                view.in_flight_submission_count,
            );
            bs_insert_u64(row, "applying_buffered_bytes", view.applying_buffered_bytes);
            bs_insert_u64(row, "reorder_buffered_bytes", view.reorder_buffered_bytes);
            bs_insert_u64(
                row,
                bs_trace::SEQUENCER_INPUT_DECODED_ATTRIBUTED_MEMORY_BYTES,
                sequencer_input_decoded_attributed_memory_bytes,
            );
            bs_insert_u64(
                row,
                bs_trace::REORDER_DECODED_ATTRIBUTED_MEMORY_BYTES,
                view.reorder_decoded_attributed_memory_bytes,
            );
            bs_insert_u64(
                row,
                bs_trace::APPLYING_DECODED_ATTRIBUTED_MEMORY_BYTES,
                view.applying_decoded_attributed_memory_bytes,
            );
            bs_insert_u64(
                row,
                bs_trace::ACTIVE_PIPELINE_DECODED_ATTRIBUTED_MEMORY_BYTES,
                sequencer_input_decoded_attributed_memory_bytes
                    .saturating_add(view.reorder_decoded_attributed_memory_bytes)
                    .saturating_add(view.applying_decoded_attributed_memory_bytes),
            );
            bs_insert_u64(
                row,
                "retained_pipeline_wire_bytes",
                view.reorder_buffered_bytes
                    .saturating_add(view.applying_buffered_bytes)
                    .saturating_add(self.body_input_bytes.load(Ordering::Relaxed)),
            );
        });
    }

    /// Flush and drain the JSONL trace writer (if tracing was enabled), so the trace
    /// tables on disk are complete before the bench process exits. A no-op when no
    /// `trace_dir` was supplied. Call after the drive loop finishes.
    pub async fn flush_trace(&mut self) {
        if let Some(guard) = self.trace_guard.take() {
            guard.shutdown().await;
        }
    }

    /// Latest progress snapshot from the sequencer view.
    pub fn progress(&self) -> SequencerProgress {
        let view = *self.view.borrow();
        SequencerProgress {
            verified_tip: view.verified_tip,
            reorder_len: view.reorder_len,
            applying_len: view.applying_len,
            committed_blocks_per_sec: view.committed_blocks_per_sec,
        }
    }
}
