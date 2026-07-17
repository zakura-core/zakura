//! Typed JSONL events for legacy request compatibility.

use serde::Serialize;

use crate::protocol::internal::Response;

use super::{bounded_u64, LegacyRequestKind};
use crate::zakura::{trace::peer_label, ZakuraPeerId, ZakuraTrace, LEGACY_REQUEST_TABLE};

pub(super) trait LegacyRequestTraceExt {
    fn trace_legacy_request_start(
        &self,
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: LegacyRequestKind,
        message_type: u16,
    );
    fn trace_legacy_request_response(
        &self,
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: &'static str,
        response: &Response,
    );
    fn trace_legacy_request_error(
        &self,
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: &'static str,
        error: String,
    );
}

impl LegacyRequestTraceExt for ZakuraTrace {
    fn trace_legacy_request_start(
        &self,
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: LegacyRequestKind,
        message_type: u16,
    ) {
        self.emit_event(|| LegacyRequestStart::new(event, peer, request_id, request, message_type));
    }

    fn trace_legacy_request_response(
        &self,
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: &'static str,
        response: &Response,
    ) {
        self.emit_event(|| LegacyRequestResponse::new(event, peer, request_id, request, response));
    }

    fn trace_legacy_request_error(
        &self,
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: &'static str,
        error: String,
    ) {
        self.emit_event(|| LegacyRequestError::new(event, peer, request_id, request, error));
    }
}

#[derive(Debug, Serialize)]
pub(super) struct LegacyRequestStart {
    event: &'static str,
    peer: Option<String>,
    request_id: u64,
    request: &'static str,
    message_type: u64,
}

impl LegacyRequestStart {
    pub(super) fn new(
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: LegacyRequestKind,
        message_type: u16,
    ) -> Self {
        Self {
            event,
            peer: peer.map(peer_label),
            request_id,
            request: request.command(),
            message_type: u64::from(message_type),
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(LegacyRequestStart, LEGACY_REQUEST_TABLE);

#[derive(Debug, Serialize)]
pub(super) struct LegacyRequestResponse {
    event: &'static str,
    peer: Option<String>,
    request_id: u64,
    request: &'static str,
    response: &'static str,
    item_count: u64,
    missing_count: u64,
}

impl LegacyRequestResponse {
    pub(super) fn new(
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: &'static str,
        response: &Response,
    ) -> Self {
        let (response, item_count, missing_count) = response_summary(response);
        Self {
            event,
            peer: peer.map(peer_label),
            request_id,
            request,
            response,
            item_count,
            missing_count,
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(LegacyRequestResponse, LEGACY_REQUEST_TABLE);

#[derive(Debug, Serialize)]
pub(super) struct LegacyRequestError {
    event: &'static str,
    peer: Option<String>,
    request_id: u64,
    request: &'static str,
    error: String,
}

impl LegacyRequestError {
    pub(super) fn new(
        event: &'static str,
        peer: Option<&ZakuraPeerId>,
        request_id: u64,
        request: &'static str,
        error: String,
    ) -> Self {
        Self {
            event,
            peer: peer.map(peer_label),
            request_id,
            request,
            error,
        }
    }
}

zakura_jsonl_trace::impl_jsonl_trace_event!(LegacyRequestError, LEGACY_REQUEST_TABLE);

fn response_summary(response: &Response) -> (&'static str, u64, u64) {
    match response {
        Response::Blocks(blocks) => (
            "Blocks",
            bounded_u64(blocks.len()),
            bounded_u64(blocks.iter().filter(|block| block.is_missing()).count()),
        ),
        Response::Transactions(transactions) => (
            "Transactions",
            bounded_u64(transactions.len()),
            bounded_u64(
                transactions
                    .iter()
                    .filter(|transaction| transaction.is_missing())
                    .count(),
            ),
        ),
        Response::BlockHashes(hashes) => ("BlockHashes", bounded_u64(hashes.len()), 0),
        Response::BlockHeaders(headers) => ("BlockHeaders", bounded_u64(headers.len()), 0),
        Response::TransactionIds(ids) => ("TransactionIds", bounded_u64(ids.len()), 0),
        Response::Pong(_) => ("Pong", 1, 0),
        Response::Nil => ("Nil", 0, 0),
        response => (response.command(), 0, 0),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn start_schema_preserves_null_peer() {
        let event = LegacyRequestStart::new("request_start", None, 7, LegacyRequestKind::Blocks, 3);
        assert_eq!(
            serde_json::to_value(event).expect("event serializes"),
            json!({
                "event": "request_start",
                "peer": null,
                "request_id": 7,
                "request": "BlocksByHash",
                "message_type": 3,
            })
        );
    }

    #[test]
    fn error_schema_preserves_detail() {
        let event = LegacyRequestError::new("request_error", None, 9, "getheaders", "boom".into());
        assert_eq!(
            serde_json::to_value(event).expect("event serializes"),
            json!({
                "event": "request_error",
                "peer": null,
                "request_id": 9,
                "request": "getheaders",
                "error": "boom",
            })
        );
    }
}
