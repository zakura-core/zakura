//! Mining RPC events for the `mining` JSONL trace table.

use std::{fmt, time::Duration};

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    impl_jsonl_trace_event, saturating_count, saturating_micros, tables::MINING, JsonlDisplay,
    JsonlEventEmitter,
};

use crate::server::jsonrpc::ErrorObject;

use super::types::{
    get_block_template::{
        proposal::BlockProposalResponse, BlockTemplateResponse, GetBlockTemplateResponse,
    },
    long_poll::LongPollId,
    submit_block::{MinedBlockSubmission, SubmitBlockErrorResponse, SubmitBlockResponse},
};

/// Emits mining RPC events to the `mining` trace table.
#[derive(Clone, Debug, Default)]
pub(crate) struct MiningTrace {
    emitter: JsonlEventEmitter,
}

/// The outcome of one `submitblock` or `submitsolution` request.
pub(crate) struct BlockSubmission<'a> {
    /// The RPC method that received the block.
    pub(crate) submission: MinedBlockSubmission,
    /// The block hash, if the request decoded into a block.
    pub(crate) hash: Option<block::Hash>,
    /// The block height, if the request decoded into a block with a coinbase height.
    pub(crate) height: Option<block::Height>,
    /// A bounded outcome label, such as `accepted` or `rejected`.
    pub(crate) result: &'static str,
    /// Why the node did not accept the block, if it did not.
    pub(crate) reason: Option<&'a dyn fmt::Display>,
    /// Time from receiving the request to producing the response.
    pub(crate) elapsed: Duration,
}

impl MiningTrace {
    /// Creates a mining trace that writes through `emitter`.
    pub(crate) fn new(emitter: JsonlEventEmitter) -> Self {
        Self { emitter }
    }

    /// Records the response to one `getblocktemplate` request.
    ///
    /// `elapsed` includes any long-poll wait.
    pub(crate) fn block_template(
        &self,
        response: &Result<GetBlockTemplateResponse, ErrorObject>,
        client_long_poll_id: Option<&LongPollId>,
        elapsed: Duration,
    ) {
        let elapsed_us = saturating_micros(elapsed);
        match response {
            Ok(GetBlockTemplateResponse::TemplateMode(template)) => {
                self.emitter.emit_event(|| TemplateServed {
                    event: "template_served",
                    template: TemplateFields::new(template),
                    client_long_poll_id: client_long_poll_id.map(JsonlDisplay),
                    elapsed_us,
                })
            }
            Ok(GetBlockTemplateResponse::ProposalMode(proposal)) => {
                self.emitter.emit_event(|| ProposalChecked {
                    event: "proposal_checked",
                    result: match proposal {
                        BlockProposalResponse::Valid => "valid",
                        BlockProposalResponse::Rejected(_) => "rejected",
                    },
                    reason: match proposal {
                        BlockProposalResponse::Valid => None,
                        BlockProposalResponse::Rejected(reason) => Some(reason),
                    },
                    elapsed_us,
                })
            }
            Err(error) => self.emitter.emit_event(|| TemplateFailed {
                event: "template_failed",
                error: JsonlDisplay(error),
                client_long_poll_id: client_long_poll_id.map(JsonlDisplay),
                elapsed_us,
            }),
        }
    }

    /// Records the outcome of one mined-block submission.
    pub(crate) fn block_submitted(&self, submission: BlockSubmission<'_>) {
        self.emitter.emit_event(|| BlockSubmitted {
            event: "block_submitted",
            method: submission.submission.rpc_method(),
            hash: submission.hash.as_ref().map(JsonlDisplay),
            height: submission.height.map(|height| height.0),
            result: submission.result,
            reason: submission.reason.map(JsonlDisplay),
            elapsed_us: saturating_micros(submission.elapsed),
        });
    }
}

/// Returns the bounded `block_submitted` result label for `response`.
pub(crate) fn submit_result(response: &SubmitBlockResponse) -> &'static str {
    match response {
        SubmitBlockResponse::Accepted => "accepted",
        SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Duplicate) => "duplicate",
        SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::DuplicateInconclusive) => {
            "duplicate_inconclusive"
        }
        SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Inconclusive) => {
            "inconclusive"
        }
        SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Rejected) => "rejected",
    }
}

#[derive(Serialize)]
struct TemplateFields<'a> {
    height: u32,
    previous_block_hash: JsonlDisplay<'a, block::Hash>,
    transactions: u64,
    long_poll_id: JsonlDisplay<'a, LongPollId>,
    work_id: &'a str,
    submit_old: Option<bool>,
    cur_time: u32,
}

impl<'a> TemplateFields<'a> {
    fn new(template: &'a BlockTemplateResponse) -> Self {
        Self {
            height: template.height,
            previous_block_hash: JsonlDisplay(&template.previous_block_hash),
            transactions: saturating_count(template.transactions.len()),
            long_poll_id: JsonlDisplay(&template.long_poll_id),
            work_id: &template.work_id,
            submit_old: template.submit_old,
            cur_time: template.cur_time.timestamp(),
        }
    }
}

#[derive(Serialize)]
struct TemplateServed<'a> {
    event: &'static str,
    #[serde(flatten)]
    template: TemplateFields<'a>,
    client_long_poll_id: Option<JsonlDisplay<'a, LongPollId>>,
    elapsed_us: u64,
}

impl_jsonl_trace_event!(TemplateServed<'_>, MINING);

#[derive(Serialize)]
struct TemplateFailed<'a> {
    event: &'static str,
    error: JsonlDisplay<'a, ErrorObject>,
    client_long_poll_id: Option<JsonlDisplay<'a, LongPollId>>,
    elapsed_us: u64,
}

impl_jsonl_trace_event!(TemplateFailed<'_>, MINING);

#[derive(Serialize)]
struct ProposalChecked<'a> {
    event: &'static str,
    result: &'static str,
    reason: Option<&'a str>,
    elapsed_us: u64,
}

impl_jsonl_trace_event!(ProposalChecked<'_>, MINING);

#[derive(Serialize)]
struct BlockSubmitted<'a> {
    event: &'static str,
    method: &'static str,
    hash: Option<JsonlDisplay<'a, block::Hash>>,
    height: Option<u32>,
    result: &'static str,
    reason: Option<JsonlDisplay<'a, dyn fmt::Display + 'a>>,
    elapsed_us: u64,
}

impl_jsonl_trace_event!(BlockSubmitted<'_>, MINING);

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use tokio::sync::mpsc;
    use zakura_jsonl_trace::{JsonlTracer, JsonlWriteEvent};

    use super::*;

    fn capture() -> (MiningTrace, mpsc::Receiver<JsonlWriteEvent>) {
        let (tx, rx) = mpsc::channel(4);
        let trace = MiningTrace::new(JsonlEventEmitter::new(JsonlTracer::new(tx), "miner"));
        (trace, rx)
    }

    fn row(rx: &mut mpsc::Receiver<JsonlWriteEvent>) -> Value {
        let written = rx.try_recv().expect("the event was queued");
        assert_eq!(written.table, MINING.table());
        serde_json::from_slice(&written.line).expect("the event is valid JSON")
    }

    #[test]
    fn block_submissions_record_method_block_result_and_reason() {
        let (trace, mut rx) = capture();
        let hash = block::Hash([7; 32]);

        trace.block_submitted(BlockSubmission {
            submission: MinedBlockSubmission::FullBlock,
            hash: Some(hash),
            height: Some(block::Height(42)),
            result: submit_result(&SubmitBlockErrorResponse::Rejected.into()),
            reason: Some(&"bad coinbase"),
            elapsed: Duration::from_micros(1_500),
        });

        let row = row(&mut rx);
        assert_eq!(row["node"], "miner");
        assert_eq!(row["event"], "block_submitted");
        assert_eq!(row["method"], "submitblock");
        assert_eq!(row["hash"], hash.to_string());
        assert_eq!(row["height"], 42);
        assert_eq!(row["result"], "rejected");
        assert_eq!(row["reason"], "bad coinbase");
        assert_eq!(row["elapsed_us"], 1_500);
    }

    #[test]
    fn template_failures_record_the_rpc_error() {
        let (trace, mut rx) = capture();

        trace.block_template(
            &Err(ErrorObject::new(0, "not synced to tip")),
            None,
            Duration::from_millis(2),
        );

        let row = row(&mut rx);
        assert_eq!(row["event"], "template_failed");
        assert!(row["error"]
            .as_str()
            .expect("the error is a string")
            .contains("not synced to tip"));
        assert_eq!(row["client_long_poll_id"], Value::Null);
        assert_eq!(row["elapsed_us"], 2_000);
    }
}
