//! The `mempool` JSONL trace table.
//!
//! The mempool appends one `mempool_tip_processed` row each time it applies a best tip change,
//! so a trace shows when a new tip's transactions became available to block templates.

use std::time::Duration;

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    saturating_count, saturating_micros, JsonlDisplay, JsonlEventEmitter, JsonlTraceTable,
};

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

#[derive(Serialize)]
#[serde(tag = "event", rename = "mempool_tip_processed")]
struct MempoolTipProcessed<'a> {
    tip_height: u32,
    tip_hash: JsonlDisplay<'a, block::Hash>,
    action: TipProcessing,
    tx_count_after: u64,
    requeued: u64,
    process_us: u64,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(MempoolTipProcessed<'_>, TABLE);

/// Writes `mempool.jsonl` rows, or nothing when tracing is disabled.
#[derive(Debug, Default)]
pub(super) struct MempoolTrace(pub(super) JsonlEventEmitter);

impl MempoolTrace {
    /// Records that the mempool applied the tip `tip` with `action`.
    ///
    /// `tx_count_after` is the number of verified transactions left in the mempool.
    /// `requeued` is the number of transactions queued for re-verification.
    /// `process` is the time the mempool spent applying the change.
    pub(super) fn tip_processed(
        &self,
        action: TipProcessing,
        tip: (block::Hash, block::Height),
        tx_count_after: usize,
        requeued: usize,
        process: Duration,
    ) {
        let (tip_hash, tip_height) = tip;
        self.0.emit_event(|| MempoolTipProcessed {
            tip_height: tip_height.0,
            tip_hash: JsonlDisplay(&tip_hash),
            action,
            tx_count_after: saturating_count(tx_count_after),
            requeued: saturating_count(requeued),
            process_us: saturating_micros(process),
        });
    }
}
