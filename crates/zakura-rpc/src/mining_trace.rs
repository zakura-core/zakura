//! Experiment-only mining RPC traces.
//!
//! Writes one `template_event` row per `getblocktemplate` call and one `submit_event`
//! row per `submitblock` call, so a fleet run can compare miner-facing latency and
//! coinbase contents across network upgrades. Tracing is off until [`install`] is
//! called with a trace directory.

use std::{cell::Cell, future::Future, path::PathBuf, sync::OnceLock, time::Instant};

use serde::Serialize;
use zakura_chain::{
    amount::{Amount, NegativeAllowed},
    block,
    parameters::{subsidy::halving_block_subsidy, Network},
    serialization::ZcashDeserializeInto,
    transaction::Transaction,
};
use zakura_jsonl_trace::{JsonlDisplay, JsonlEventEmitter, JsonlTraceTable, JsonlTracer};

use jsonrpsee::core::RpcResult as Result;

use crate::methods::types::{
    get_block_template::GetBlockTemplateResponse,
    submit_block::{
        SubmitBlockErrorResponse, SubmitBlockResponse, SubmitSolutionErrorResponse,
        SubmitSolutionResponse,
    },
};

/// One row per `getblocktemplate` call.
pub const TEMPLATE_TABLE: JsonlTraceTable = JsonlTraceTable::csv(
    "template_event",
    "template_event.csv",
    &[
        "event",
        "mode",
        "long_poll",
        "height",
        "parent",
        "work_id",
        "bits",
        "cur_time",
        "tx_count",
        "tx_fees",
        "miner_fee_share",
        "nsm_contribution",
        "coinbase_value",
        "subsidy",
        "scheduled_subsidy",
        "bonus",
        "precomputed",
        "latency_ms",
        "result",
        "error",
    ],
);

/// One row per `submitblock` or `submitsolution` call.
pub const SUBMIT_TABLE: JsonlTraceTable = JsonlTraceTable::csv(
    "submit_event",
    "submit_event.csv",
    &[
        "event",
        "rpc_method",
        "height",
        "hash",
        "work_id",
        "latency_ms",
        "result",
        "error",
    ],
);

static EMITTER: OnceLock<JsonlEventEmitter> = OnceLock::new();

/// Starts writing mining RPC traces to `trace_dir`. Later calls have no effect.
pub fn install(trace_dir: Option<PathBuf>) {
    let Some(trace_dir) = trace_dir else {
        return;
    };
    let tracer = JsonlTracer::spawn(trace_dir);
    if tracer.is_enabled() {
        let _ = EMITTER.set(JsonlEventEmitter::new(
            tracer,
            zakura_jsonl_trace::node_id(),
        ));
    }
}

fn emitter() -> Option<&'static JsonlEventEmitter> {
    EMITTER.get()
}

tokio::task_local! {
    static TEMPLATE_CONTEXT: TemplateContext;
    static SUBMIT_CONTEXT: SubmitContext;
}

#[derive(Default)]
struct TemplateContext {
    precomputed: Cell<Option<bool>>,
}

#[derive(Default)]
struct SubmitContext {
    height: Cell<Option<block::Height>>,
    hash: Cell<Option<block::Hash>>,
    error: Cell<Option<String>>,
}

/// Records whether the tip-change fast path used a precomputed coinbase.
pub(crate) fn note_precomputed(hit: bool) {
    let _ = TEMPLATE_CONTEXT.try_with(|context| context.precomputed.set(Some(hit)));
}

/// Records the height and hash of the block being submitted.
pub(crate) fn note_submitted_block(height: block::Height, hash: block::Hash) {
    let _ = SUBMIT_CONTEXT.try_with(|context| {
        context.height.set(Some(height));
        context.hash.set(Some(hash));
    });
}

/// Records why a submitted block failed verification.
pub(crate) fn note_submit_error(error: &dyn std::fmt::Display) {
    let _ = SUBMIT_CONTEXT.try_with(|context| context.error.set(Some(error.to_string())));
}

#[derive(Serialize)]
struct TemplateEvent {
    event: &'static str,
    mode: &'static str,
    long_poll: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    work_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bits: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cur_time: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_fees: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    miner_fee_share: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nsm_contribution: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    coinbase_value: Option<i64>,
    /// `coinbase_value - miner_fee_share`. Equals the block subsidy on networks
    /// without funding streams or lockbox deferrals.
    #[serde(skip_serializing_if = "Option::is_none")]
    subsidy: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scheduled_subsidy: Option<i64>,
    /// `subsidy - scheduled_subsidy`: the NSM reissuance bonus.
    #[serde(skip_serializing_if = "Option::is_none")]
    bonus: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    precomputed: Option<bool>,
    latency_ms: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(TemplateEvent, TEMPLATE_TABLE);

#[derive(Serialize)]
struct SubmitEvent<'a> {
    event: &'static str,
    rpc_method: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<JsonlDisplay<'a, block::Hash>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    work_id: Option<&'a str>,
    latency_ms: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(SubmitEvent<'_>, SUBMIT_TABLE);

/// Runs a `getblocktemplate` call and records it.
pub(crate) async fn trace_template(
    network: &Network,
    mode: &'static str,
    long_poll: bool,
    call: impl Future<Output = Result<GetBlockTemplateResponse>>,
) -> Result<GetBlockTemplateResponse> {
    let Some(emitter) = emitter() else {
        return call.await;
    };

    let started = Instant::now();
    let (result, precomputed) = TEMPLATE_CONTEXT
        .scope(TemplateContext::default(), async {
            let result = call.await;
            (
                result,
                TEMPLATE_CONTEXT.with(|context| context.precomputed.get()),
            )
        })
        .await;
    let latency_ms = zakura_jsonl_trace::saturating_millis(started.elapsed());

    emitter.emit_event(|| {
        let mut event = TemplateEvent {
            event: "template",
            mode,
            long_poll,
            height: None,
            parent: None,
            work_id: None,
            bits: None,
            cur_time: None,
            tx_count: None,
            tx_fees: None,
            miner_fee_share: None,
            nsm_contribution: None,
            coinbase_value: None,
            subsidy: None,
            scheduled_subsidy: None,
            bonus: None,
            precomputed,
            latency_ms,
            result: "ok",
            error: None,
        };

        match &result {
            Ok(GetBlockTemplateResponse::TemplateMode(template)) => {
                let height = block::Height(template.height);
                let tx_fees = template
                    .transactions
                    .iter()
                    .map(|tx| i64::from(tx.fee))
                    .sum::<i64>();
                let miner_fee_share = -i64::from(template.coinbase_txn.fee);
                let coinbase_value = coinbase_value(template.coinbase_txn.data.as_ref());
                let subsidy = coinbase_value.map(|value| value - miner_fee_share);
                let scheduled_subsidy = halving_block_subsidy(height, network).ok().map(i64::from);

                event.height = Some(template.height);
                event.parent = Some(template.previous_block_hash.to_string());
                event.work_id = Some(template.work_id.clone());
                event.bits = Some(template.bits.to_string());
                event.cur_time = Some(template.cur_time.timestamp());
                event.tx_count = Some(zakura_jsonl_trace::saturating_count(
                    template.transactions.len(),
                ));
                event.tx_fees = Some(tx_fees);
                event.miner_fee_share = Some(miner_fee_share);
                event.nsm_contribution = Some(tx_fees - miner_fee_share);
                event.coinbase_value = coinbase_value;
                event.subsidy = subsidy;
                event.scheduled_subsidy = scheduled_subsidy;
                event.bonus = subsidy.zip(scheduled_subsidy).map(|(s, h)| s - h);
            }
            Ok(_) => event.result = "proposal",
            Err(error) => {
                event.result = "error";
                event.error = Some(error.message().to_string());
            }
        }

        event
    });

    result
}

/// Classifies the response without changing the RPC's return type.
pub(crate) trait SubmissionResponse {
    fn trace_label(&self) -> &'static str;
}

impl SubmissionResponse for SubmitBlockResponse {
    fn trace_label(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::ErrorResponse(SubmitBlockErrorResponse::Duplicate) => "duplicate",
            Self::ErrorResponse(SubmitBlockErrorResponse::DuplicateInconclusive) => {
                "duplicate_inconclusive"
            }
            Self::ErrorResponse(SubmitBlockErrorResponse::Inconclusive) => "inconclusive",
            Self::ErrorResponse(SubmitBlockErrorResponse::Rejected) => "rejected",
        }
    }
}

impl SubmissionResponse for SubmitSolutionResponse {
    fn trace_label(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::ErrorResponse(SubmitSolutionErrorResponse::Duplicate) => "duplicate",
            Self::ErrorResponse(SubmitSolutionErrorResponse::DuplicateInconclusive) => {
                "duplicate_inconclusive"
            }
            Self::ErrorResponse(SubmitSolutionErrorResponse::Inconclusive) => "inconclusive",
            Self::ErrorResponse(SubmitSolutionErrorResponse::Rejected) => "rejected",
            Self::ErrorResponse(SubmitSolutionErrorResponse::StaleWork) => "stale_work",
            Self::ErrorResponse(SubmitSolutionErrorResponse::CandidateMismatch) => {
                "candidate_mismatch"
            }
        }
    }
}

/// Records the complete RPC interval, including decoding and reconstruction.
pub(crate) async fn trace_submit<T: SubmissionResponse>(
    rpc_method: &'static str,
    work_id: Option<String>,
    call: impl Future<Output = Result<T>>,
) -> Result<T> {
    let Some(emitter) = emitter() else {
        return call.await;
    };

    let started = Instant::now();
    let (result, height, hash, error) = SUBMIT_CONTEXT
        .scope(SubmitContext::default(), async {
            let result = call.await;
            SUBMIT_CONTEXT.with(|context| {
                (
                    result,
                    context.height.get(),
                    context.hash.get(),
                    context.error.take(),
                )
            })
        })
        .await;
    let latency_ms = zakura_jsonl_trace::saturating_millis(started.elapsed());

    emitter.emit_event(|| {
        let (label, error) = match &result {
            Ok(response) => (response.trace_label(), error),
            Err(rpc_error) => ("error", Some(rpc_error.message().to_string())),
        };

        SubmitEvent {
            event: "submit",
            rpc_method,
            height: height.map(|height| height.0),
            hash: hash.as_ref().map(JsonlDisplay),
            work_id: work_id.as_deref(),
            latency_ms,
            result: label,
            error,
        }
    });

    result
}

/// Returns the value a coinbase transaction claims: transparent outputs minus the
/// shielded value balances, or `None` if it does not parse.
fn coinbase_value(data: &[u8]) -> Option<i64> {
    let transaction: Transaction = data.zcash_deserialize_into().ok()?;
    let transparent = transaction
        .outputs()
        .iter()
        .map(|output| i64::from(output.value()))
        .sum::<i64>();
    let shielded: Amount<NegativeAllowed> = (transaction.sapling_value_balance().sapling_amount()
        + transaction.orchard_value_balance().orchard_amount()
        + transaction.ironwood_value_balance().ironwood_amount())
    .ok()?;

    Some(transparent - i64::from(shielded))
}
