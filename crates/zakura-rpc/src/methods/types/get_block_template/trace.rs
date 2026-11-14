//! The `mining_template` JSONL trace table.
//!
//! Each `getblocktemplate` call gets a process-wide `request_id`. A long poll that sees a new
//! tip writes `gbt_tip_woke` and then `gbt_empty_returned` for the coinbase-only template it
//! returns. Every other template a call returns writes `gbt_full_returned`.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    saturating_count, saturating_micros, JsonlDisplay, JsonlEventEmitter, JsonlTraceTable,
};

use super::BlockTemplateResponse;

const TABLE: JsonlTraceTable = JsonlTraceTable::new("mining_template", "mining_template.jsonl");

/// Returns a new `getblocktemplate` request ID, unique within this process.
pub(crate) fn next_request_id() -> u64 {
    static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(0);
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

/// The last event that ended a long poll's wait before it returned a template.
#[derive(Copy, Clone, Debug, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WaitReason {
    /// The call returned without waiting.
    #[default]
    Immediate,
    /// The periodic mempool check timer fired.
    Timer,
    /// A template rejection changed the rejection state.
    Rejection,
    /// The template's time range expired.
    MaxTime,
}

/// How long one template build waited to start, and how long it ran.
#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct BuildTimes {
    /// The time from the build request to the build starting, including the wait for the
    /// build permit and a blocking thread.
    pub(crate) wait: Duration,
    /// The time the build ran.
    pub(crate) run: Duration,
}

#[derive(Serialize)]
#[serde(tag = "event")]
enum MiningTemplateEvent<'a> {
    #[serde(rename = "gbt_tip_woke")]
    TipWoke {
        request_id: u64,
        tip_height: u32,
        tip_hash: JsonlDisplay<'a, block::Hash>,
    },
    #[serde(rename = "gbt_empty_returned")]
    EmptyReturned {
        request_id: u64,
        tip_hash: JsonlDisplay<'a, block::Hash>,
        work_id: &'a str,
        permit_wait_us: u64,
        build_us: u64,
    },
    #[serde(rename = "gbt_full_returned")]
    FullReturned {
        request_id: u64,
        tip_hash: JsonlDisplay<'a, block::Hash>,
        work_id: &'a str,
        tx_count: u64,
        wait_reason: WaitReason,
        selection_us: u64,
        permit_wait_us: u64,
        build_us: u64,
        total_us: u64,
    },
}

zakura_jsonl_trace::impl_jsonl_trace_event!(MiningTemplateEvent<'_>, TABLE);

/// Writes `mining_template.jsonl` rows, or nothing when tracing is disabled.
#[derive(Clone, Debug, Default)]
pub(crate) struct MiningTemplateTrace(pub(crate) JsonlEventEmitter);

impl MiningTemplateTrace {
    /// Records that request `request_id` saw the new best tip `tip`.
    pub(crate) fn tip_woke(&self, request_id: u64, tip: Option<(block::Height, block::Hash)>) {
        let Some((tip_height, tip_hash)) = tip else {
            return;
        };
        self.0.emit_event(|| MiningTemplateEvent::TipWoke {
            request_id,
            tip_height: tip_height.0,
            tip_hash: JsonlDisplay(&tip_hash),
        });
    }

    /// Records that request `request_id` returned the coinbase-only `template` for a new tip.
    pub(crate) fn empty_returned(
        &self,
        request_id: u64,
        template: &BlockTemplateResponse,
        build: BuildTimes,
    ) {
        self.0.emit_event(|| MiningTemplateEvent::EmptyReturned {
            request_id,
            tip_hash: JsonlDisplay(&template.previous_block_hash),
            work_id: &template.work_id,
            permit_wait_us: saturating_micros(build.wait),
            build_us: saturating_micros(build.run),
        });
    }

    /// Records that request `request_id` returned `template`, built from the mempool.
    ///
    /// `selection` is the transaction selection time, and `total` is the time since the request
    /// started.
    pub(crate) fn full_returned(
        &self,
        request_id: u64,
        template: &BlockTemplateResponse,
        wait_reason: WaitReason,
        selection: Duration,
        build: BuildTimes,
        total: Duration,
    ) {
        self.0.emit_event(|| MiningTemplateEvent::FullReturned {
            request_id,
            tip_hash: JsonlDisplay(&template.previous_block_hash),
            work_id: &template.work_id,
            tx_count: saturating_count(template.transactions.len()),
            wait_reason,
            selection_us: saturating_micros(selection),
            permit_wait_us: saturating_micros(build.wait),
            build_us: saturating_micros(build.run),
            total_us: saturating_micros(total),
        });
    }
}
