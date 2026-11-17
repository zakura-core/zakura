//! The `chain_tip` JSONL trace table.
//!
//! The block writer appends one `tip_published` row each time a committed block becomes the
//! best tip. Rows split the time a block spent before its tip was published into the wait for
//! the writer and the writer's own commit work.

use std::{path::PathBuf, time::Duration};

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    saturating_micros, JsonlDisplay, JsonlEventEmitter, JsonlTraceTable, JsonlTracer,
};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("chain_tip", "chain_tip.jsonl");

/// Whether a new tip extends the previous tip or replaces it with another chain.
#[derive(Copy, Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum TipChange {
    /// The new tip's parent is the previous tip.
    Extend,
    /// The new tip's parent is not the previous tip.
    Reorg,
}

#[derive(Serialize)]
#[serde(tag = "event", rename = "tip_published")]
struct TipPublished<'a> {
    tip_height: u32,
    tip_hash: JsonlDisplay<'a, block::Hash>,
    prev_tip_hash: Option<JsonlDisplay<'a, block::Hash>>,
    kind: TipChange,
    queue_us: u64,
    commit_us: u64,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(TipPublished<'_>, TABLE);

/// Writes `chain_tip.jsonl` rows, or nothing when no trace directory is configured.
#[derive(Debug)]
pub(super) struct ChainTipTrace(JsonlEventEmitter);

impl ChainTipTrace {
    /// Spawns a writer for `trace_dir`, or returns a no-op trace when it is `None`.
    pub(super) fn new(trace_dir: Option<PathBuf>) -> Self {
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);
        Self(JsonlEventEmitter::new(
            tracer,
            zakura_jsonl_trace::node_id(),
        ))
    }

    /// Records that the committed block `tip` became the best tip, replacing `prev_tip_hash`.
    ///
    /// `queue` is the time the block waited for the writer.
    /// `commit` is the time from the writer starting on the block to the tip publication.
    pub(super) fn tip_published(
        &self,
        tip: (block::Height, block::Hash),
        parent_hash: block::Hash,
        prev_tip_hash: Option<block::Hash>,
        queue: Duration,
        commit: Duration,
    ) {
        let (tip_height, tip_hash) = tip;
        self.0.emit_event(|| TipPublished {
            tip_height: tip_height.0,
            tip_hash: JsonlDisplay(&tip_hash),
            prev_tip_hash: prev_tip_hash.as_ref().map(JsonlDisplay),
            kind: if prev_tip_hash == Some(parent_hash) {
                TipChange::Extend
            } else {
                TipChange::Reorg
            },
            queue_us: saturating_micros(queue),
            commit_us: saturating_micros(commit),
        });
    }
}
