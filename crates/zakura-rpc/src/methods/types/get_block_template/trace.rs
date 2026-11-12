//! The `mining_template` JSONL trace table.
//!
//! Each `getblocktemplate` call gets a process-wide `request_id`. A long poll that sees a new
//! tip writes `gbt_tip_woke`, followed by `gbt_empty_returned` for a coinbase-only answer, with
//! the reason it holds no mempool transactions, or `gbt_full_returned` for an answer built
//! from the mempool. Other returned templates also write `gbt_full_returned`.

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
    /// A chain tip notification woke the call.
    Tip,
    /// The periodic mempool check timer fired.
    Timer,
    /// A template rejection changed the rejection state.
    Rejection,
    /// The template's time range expired.
    MaxTime,
}

/// Why a long poll's first answer on a new tip is the coinbase-only template.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EmptyReason {
    /// A shielded coinbase built in advance saves the miner its proof time.
    CoinbaseBuiltAhead,
    /// As many long polls as may build a full first answer at once were already doing so.
    NoBuildSlot,
    /// The mempool holds no transactions for the new tip.
    MempoolEmpty,
    /// The mempool reported more work to remove the new block's transactions than it is
    /// expected to finish within the wait for it.
    MempoolWorkHeavy,
    /// The mempool answered a request while it was still on an older tip.
    MempoolOnOlderTip,
    /// The mempool neither published its transactions for the new tip nor answered a request
    /// within the wait for it.
    MempoolWaitElapsed,
    /// The request to the mempool failed.
    MempoolError,
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
        reason: EmptyReason,
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

    /// Records that request `request_id` returned the coinbase-only `template` for a new tip,
    /// because of `reason`.
    pub(crate) fn empty_returned(
        &self,
        request_id: u64,
        template: &BlockTemplateResponse,
        reason: EmptyReason,
        build: BuildTimes,
    ) {
        self.0.emit_event(|| MiningTemplateEvent::EmptyReturned {
            request_id,
            tip_hash: JsonlDisplay(&template.previous_block_hash),
            work_id: &template.work_id,
            reason,
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
