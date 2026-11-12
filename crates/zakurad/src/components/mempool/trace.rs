//! The `mempool` JSONL trace table.
//!
//! The mempool appends one `mempool_tip_processed` row each time it applies a best tip change,
//! so a trace shows when a new tip's transactions became available to block templates.
//!
//! Each row records the work the mempool published when it started on the change, in its
//! separate parts, and two times: `process_us`, the time it spent removing transactions, and
//! `ready_us`, the time from publishing that it started to publishing that it finished. The
//! second is what a long poll waiting for the tip's transactions sees: it also covers storing
//! transactions verified meanwhile, counted in `drained`, and rebuilding the published list.

use std::time::{Duration, Instant};

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    saturating_count, saturating_micros, JsonlDisplay, JsonlEventEmitter, JsonlTraceTable,
};
use zakura_node_services::mempool::MempoolTipWork;

const TABLE: JsonlTraceTable = JsonlTraceTable::new("mempool", "mempool.jsonl");

/// How the mempool applied a best tip change.
#[derive(Copy, Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum TipProcessing {
    /// The tip grew by one block: the mempool removed that block's transactions.
    Grow,
    /// The best chain changed: the mempool emptied and re-queued its transactions for verification.
    Reset,
}

/// What the mempool held and published when it started on a best tip change.
#[derive(Copy, Clone, Debug)]
pub(super) struct TipStarted {
    /// The number of verified transactions the mempool held before it applied the change.
    pub(super) transactions_before: usize,
    /// The work the mempool published for the change: the block's (see `Storage::tip_work`),
    /// or none for a reset, which empties the mempool whatever the block holds.
    pub(super) work: MempoolTipWork,
    /// When the mempool published that it started.
    pub(super) published_at: Instant,
}

/// How the mempool applied a best tip change.
#[derive(Copy, Clone, Debug)]
pub(super) struct TipApplied {
    /// Whether the tip grew or the mempool reset.
    pub(super) action: TipProcessing,
    /// The new tip's hash and height.
    pub(super) tip: (block::Hash, block::Height),
    /// The number of verified transactions left in the mempool.
    pub(super) tx_count_after: usize,
    /// The number of transactions queued for re-verification.
    pub(super) requeued: usize,
    /// The number of finished downloads the mempool handled in the same pass, before it
    /// removed the block's transactions. Storing them is part of `ready_us` but not of the
    /// reported work.
    pub(super) drained: usize,
    /// The time the mempool spent removing transactions.
    pub(super) process: Duration,
}

#[derive(Serialize)]
#[serde(tag = "event", rename = "mempool_tip_processed")]
struct MempoolTipProcessed<'a> {
    tip_height: u32,
    tip_hash: JsonlDisplay<'a, block::Hash>,
    action: TipProcessing,
    tx_count_before: u64,
    tx_count_after: u64,
    requeued: u64,
    drained: u64,
    block_parts: u64,
    mempool_parts: u64,
    work: u64,
    process_us: u64,
    ready_us: u64,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(MempoolTipProcessed<'_>, TABLE);

/// Writes `mempool.jsonl` rows, or nothing when tracing is disabled.
#[derive(Debug, Default)]
pub(super) struct MempoolTrace(pub(super) JsonlEventEmitter);

impl MempoolTrace {
    /// Records that the mempool `applied` a best tip change it `started` on, once it has
    /// published that it finished.
    pub(super) fn tip_processed(&self, started: TipStarted, applied: TipApplied) {
        let ready = started.published_at.elapsed();
        let (tip_hash, tip_height) = applied.tip;
        self.0.emit_event(|| MempoolTipProcessed {
            tip_height: tip_height.0,
            tip_hash: JsonlDisplay(&tip_hash),
            action: applied.action,
            tx_count_before: saturating_count(started.transactions_before),
            tx_count_after: saturating_count(applied.tx_count_after),
            requeued: saturating_count(applied.requeued),
            drained: saturating_count(applied.drained),
            block_parts: saturating_count(started.work.block_parts),
            mempool_parts: saturating_count(started.work.mempool_parts),
            work: saturating_count(started.work.work()),
            process_us: saturating_micros(applied.process),
            ready_us: saturating_micros(ready),
        });
    }
}
