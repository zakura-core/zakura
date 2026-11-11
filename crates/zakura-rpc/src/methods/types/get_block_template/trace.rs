//! The `mining_template` JSONL trace table.
//!
//! Each `getblocktemplate` call gets a process-wide `request_id`. A long poll that sees a new
//! tip writes `gbt_tip_woke`, then `gbt_new_tip_mempool` for the mempool request it makes there,
//! followed by `gbt_empty_returned` for a coinbase-only answer or `gbt_full_returned` for an
//! answer built from the mempool. Other returned templates also write `gbt_full_returned`.
//!
//! The task that builds templates ahead of requests writes one `gbt_ahead` row for each tip it
//! works on.

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

/// What the mempool offered for a long poll's first answer on a new tip.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NewTipMempool {
    /// The mempool was on the new tip and held transactions.
    Populated,
    /// The mempool was on the new tip and held no transactions.
    Empty,
    /// The mempool answered that it was still on an older tip.
    OlderTip,
    /// The mempool had not processed the new tip's block within the wait, or is not active.
    TimedOut,
    /// The mempool request failed.
    Failed,
    /// The mempool was not asked, because a shielded coinbase was built in advance.
    Skipped,
    /// The mempool was not asked, because as many long polls as are allowed were already
    /// building a full first answer.
    OverLimit,
}

/// Where a returned template came from.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TemplateSource {
    /// The request chose the transactions and built the template itself.
    Built,
    /// The request took the template another request built.
    Shared,
    /// The request took the template the node built ahead of requests.
    Ahead,
}

/// What the task that builds templates ahead of requests did on one tip.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AheadOutcome {
    /// It built and stored the template.
    Stored,
    /// It left the template to a long poll that waited for this tip.
    LongPollWaiting,
    /// The node is not synced to the tip.
    NotSynced,
    /// The miner's reward needs a proof.
    NeedsProof,
    /// The mempool shut down before it processed the tip's block.
    NoMempool,
    /// The tip's templates are being recovered after a rejection.
    Rejection,
    /// A request had already built and stored this template.
    AlreadyBuilt,
    /// A request was building a template on this tip.
    BuildRunning,
    /// A build on a newer tip replaced this one before it was stored.
    Replaced,
    /// The tip changed before the template was stored.
    TipChanged,
    /// The state read or the build failed.
    Failed,
}

/// The steps the task that builds templates ahead of requests reached on one tip. A step it did
/// not reach is zero.
#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct AheadTimes {
    /// The number of transactions in the stored template.
    pub(crate) tx_count: usize,
    /// The wait for the mempool to process the tip's block.
    pub(crate) mempool_wait: Duration,
    /// The time choosing transactions.
    pub(crate) selection: Duration,
    /// The time the build ran.
    pub(crate) build: Duration,
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
    #[serde(rename = "gbt_new_tip_mempool")]
    NewTipMempool {
        request_id: u64,
        tip_hash: JsonlDisplay<'a, block::Hash>,
        outcome: NewTipMempool,
        tx_count: u64,
        wait_us: u64,
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
        long_poll: bool,
        source: TemplateSource,
        wait_reason: WaitReason,
        selection_us: u64,
        permit_wait_us: u64,
        build_us: u64,
        total_us: u64,
    },
    #[serde(rename = "gbt_ahead")]
    Ahead {
        tip_hash: Option<JsonlDisplay<'a, block::Hash>>,
        outcome: AheadOutcome,
        tx_count: u64,
        mempool_wait_us: u64,
        selection_us: u64,
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

    /// Records what the mempool offered request `request_id` for its first answer on the new
    /// tip `tip_hash`: the `outcome`, the number of transactions, and how long the request waited.
    pub(crate) fn new_tip_mempool(
        &self,
        request_id: u64,
        tip_hash: block::Hash,
        outcome: NewTipMempool,
        tx_count: usize,
        wait: Duration,
    ) {
        self.0.emit_event(|| MiningTemplateEvent::NewTipMempool {
            request_id,
            tip_hash: JsonlDisplay(&tip_hash),
            outcome,
            tx_count: saturating_count(tx_count),
            wait_us: saturating_micros(wait),
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
    /// `long_poll` is whether the request named a long poll ID, and `source` is where the
    /// template came from. `selection` is the transaction selection time, and `total` is the
    /// time since the request started.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn full_returned(
        &self,
        request_id: u64,
        template: &BlockTemplateResponse,
        long_poll: bool,
        source: TemplateSource,
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
            long_poll,
            source,
            wait_reason,
            selection_us: saturating_micros(selection),
            permit_wait_us: saturating_micros(build.wait),
            build_us: saturating_micros(build.run),
            total_us: saturating_micros(total),
        });
    }

    /// Records what the task that builds templates ahead of requests did on the tip `tip_hash`,
    /// which is `None` when the task stopped before it read the tip from the state.
    ///
    /// `ahead` holds the times of the steps the task reached, and `total` is the time since the
    /// task started on this tip.
    pub(crate) fn ahead(
        &self,
        tip_hash: Option<block::Hash>,
        outcome: AheadOutcome,
        ahead: AheadTimes,
        total: Duration,
    ) {
        self.0.emit_event(|| MiningTemplateEvent::Ahead {
            tip_hash: tip_hash.as_ref().map(JsonlDisplay),
            outcome,
            tx_count: saturating_count(ahead.tx_count),
            mempool_wait_us: saturating_micros(ahead.mempool_wait),
            selection_us: saturating_micros(ahead.selection),
            build_us: saturating_micros(ahead.build),
            total_us: saturating_micros(total),
        });
    }
}
