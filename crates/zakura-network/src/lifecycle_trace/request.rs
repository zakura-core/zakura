//! `p2p_request` rows: an outbound request's path from peer selection to its response.
//!
//! - `dispatch`: the peer set selected this connection. Records the selection route, the number
//!   of candidate peers, and the request the connection was already serving with the number of
//!   requests already queued for it.
//! - `enqueue`: the sender is publishing the request to the connection channel.
//! - `dequeue`: the connection task took the request from the channel. `queue_us` is the time
//!   since `enqueue`.
//! - `write`: the connection task finished writing the request message. `write_us` is the time
//!   since `dequeue`.
//! - `finish`: the connection task stopped serving the request. `elapsed_us` is the time since
//!   `dequeue`.
//!
//! The peer set records `dispatch` and then calls the connection's client, which stamps the
//! request with the dispatched identifier. The peer set holds the client exclusively between the
//! two steps, so the identifier cannot reach another request.
//!
//! The queued count includes requests from the peer set and block gossip. Heartbeat pings are
//! not stamped, so they are not counted while queued, but a ping being served is reported as
//! the active request.

use std::{sync::atomic::Ordering, time::Instant};

use serde::Serialize;
use zakura_chain::{block, transaction};
use zakura_jsonl_trace::{impl_jsonl_trace_event, saturating_count, saturating_micros, tables};

use super::{ConnectionState, ConnectionTrace};
use crate::{PeerError, Request};

/// The queueing context of a request sent on a connection channel.
///
/// Empty when the tracer does not write the `p2p_request` table.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RequestTrace(Option<Stamp>);

#[derive(Clone, Copy, Debug)]
struct Stamp {
    request_id: Option<u64>,
    desc: RequestDesc,
    enqueued_at: Instant,
}

/// The request a connection task is serving.
#[derive(Clone, Copy, Debug)]
pub(super) struct ActiveRequest {
    request_id: Option<u64>,
    desc: RequestDesc,
    since: Instant,
}

/// The identifying fields of a request.
#[derive(Clone, Copy, Debug)]
struct RequestDesc {
    command: &'static str,
    hash: Option<RequestHash>,
    item_count: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
enum RequestHash {
    Block(block::Hash),
    Tx(transaction::Hash),
}

impl RequestDesc {
    fn new(request: &Request) -> Self {
        let (hash, item_count) = match request {
            Request::BlocksByHash(hashes) | Request::BlocksByHashFrom { hashes, .. } => (
                hashes.iter().next().copied().map(RequestHash::Block),
                Some(hashes.len()),
            ),
            Request::TransactionsById(ids) | Request::TransactionsByIdFrom { ids, .. } => (
                ids.iter().next().map(|id| RequestHash::Tx(id.mined_id())),
                Some(ids.len()),
            ),
            Request::AdvertiseBlock(hash, _)
            | Request::AdvertiseBlockToAll(hash)
            | Request::AdvertiseMinedBlock(hash) => (Some(RequestHash::Block(*hash)), None),
            Request::AdvertiseTransactionIds(ids, _) => (None, Some(ids.len())),
            _ => (None, None),
        };

        Self {
            command: request.command(),
            hash,
            item_count,
        }
    }
}

impl Serialize for RequestHash {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            RequestHash::Block(hash) => serializer.collect_str(hash),
            RequestHash::Tx(hash) => serializer.collect_str(hash),
        }
    }
}

#[derive(Default, Serialize)]
struct RequestRow<'a> {
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<u64>,
    conn: u64,
    direction: &'static str,
    peer: &'a str,
    request: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<RequestHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    route: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidates: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_request: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_request_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_age_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queued: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl_jsonl_trace_event!(RequestRow<'_>, tables::P2P_REQUEST);

impl ConnectionState {
    fn request_row(
        &self,
        conn: u64,
        event: &'static str,
        request_id: Option<u64>,
        desc: RequestDesc,
    ) -> RequestRow<'_> {
        RequestRow {
            event,
            request_id,
            conn,
            direction: self.direction,
            peer: &self.peer,
            request: desc.command,
            hash: desc.hash,
            item_count: desc.item_count.map(saturating_count),
            ..RequestRow::default()
        }
    }

    fn lock<'a, T>(&self, mutex: &'a std::sync::Mutex<T>) -> std::sync::MutexGuard<'a, T> {
        mutex
            .lock()
            .expect("lifecycle trace locks are never held across a panic")
    }
}

impl ConnectionTrace {
    /// Records that the peer set selected this connection for `request`, and hands
    /// `request_id` to the client's next [`Self::stamp_dispatched`].
    ///
    /// `route` names the selection policy and `candidates` counts the peers it chose from.
    pub(crate) fn dispatched(
        &self,
        request_id: u64,
        request: &Request,
        route: &'static str,
        candidates: usize,
    ) {
        let Some(state) = self.requests() else {
            return;
        };

        *state.lock(&state.dispatching) = Some(request_id);
        let active = *state.lock(&state.active);
        let queued = state.queued.load(Ordering::Relaxed);
        state.emitter.emit_event(|| RequestRow {
            route: Some(route),
            candidates: Some(saturating_count(candidates)),
            active_request: active.map(|active| active.desc.command),
            active_request_id: active.and_then(|active| active.request_id),
            active_age_us: active.map(|active| saturating_micros(active.since.elapsed())),
            queued: Some(queued),
            ..state.request_row(
                self.id,
                "dispatch",
                Some(request_id),
                RequestDesc::new(request),
            )
        });
    }

    /// Stamps a request from the peer set before the client sends it on this connection's
    /// channel, tagging it with the identifier from the preceding [`Self::dispatched`].
    pub(crate) fn stamp_dispatched(&self, request: &Request) -> RequestTrace {
        let request_id = self
            .requests()
            .and_then(|state| state.lock(&state.dispatching).take());
        self.stamp(request_id, request)
    }

    /// Stamps `request` before it is sent on this connection's channel.
    pub(crate) fn stamp(&self, request_id: Option<u64>, request: &Request) -> RequestTrace {
        RequestTrace(self.requests().map(|_| Stamp {
            request_id,
            desc: RequestDesc::new(request),
            enqueued_at: Instant::now(),
        }))
    }

    /// Records the request before publishing it to this connection's channel.
    ///
    /// The receiver can run as soon as the sender publishes the request. Call
    /// [`Self::enqueue_failed`] if the channel rejects it.
    pub(crate) fn enqueued(&self, trace: &RequestTrace) {
        let (Some(state), Some(stamp)) = (self.requests(), trace.0) else {
            return;
        };

        let queued = state
            .queued
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        state.emitter.emit_event(|| RequestRow {
            queued: Some(queued),
            ..state.request_row(self.id, "enqueue", stamp.request_id, stamp.desc)
        });
    }

    /// Releases the queue count when the channel rejects a stamped request.
    pub(crate) fn enqueue_failed(&self, trace: &RequestTrace) {
        let (Some(state), Some(stamp)) = (self.requests(), trace.0) else {
            return;
        };
        state.queued.fetch_sub(1, Ordering::Relaxed);
        state.emitter.emit_event(|| RequestRow {
            result: Some("enqueue_failed"),
            ..state.request_row(self.id, "finish", stamp.request_id, stamp.desc)
        });
    }

    /// Records that the connection task took `request` from its channel.
    ///
    /// A request that is not `canceled` becomes the connection's active request.
    pub(crate) fn dequeued(&self, trace: &RequestTrace, request: &Request, canceled: bool) {
        let Some(state) = self.requests() else {
            return;
        };

        let now = Instant::now();
        let (request_id, desc, queue_us) = match trace.0 {
            Some(stamp) => {
                // Every stamped request was counted when the channel accepted it.
                let _ = state
                    .queued
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |queued| {
                        queued.checked_sub(1)
                    });
                (
                    stamp.request_id,
                    stamp.desc,
                    Some(saturating_micros(now.duration_since(stamp.enqueued_at))),
                )
            }
            None => (None, RequestDesc::new(request), None),
        };

        if !canceled {
            *state.lock(&state.active) = Some(ActiveRequest {
                request_id,
                desc,
                since: now,
            });
        }

        let queued = state.queued.load(Ordering::Relaxed);
        state.emitter.emit_event(|| RequestRow {
            queued: Some(queued),
            queue_us,
            result: canceled.then_some("canceled"),
            ..state.request_row(self.id, "dequeue", request_id, desc)
        });
    }

    /// Records that the connection task finished writing its active request.
    ///
    /// A write that failed with `error` ends the active request.
    pub(crate) fn request_written(&self, error: Option<&PeerError>) {
        let Some(state) = self.requests() else {
            return;
        };

        let active = *state.lock(&state.active);
        let Some(active) = active else {
            return;
        };

        state.emitter.emit_event(|| RequestRow {
            write_us: Some(saturating_micros(active.since.elapsed())),
            result: Some(if error.is_none() { "ok" } else { "error" }),
            error: error.map(ToString::to_string),
            ..state.request_row(self.id, "write", active.request_id, active.desc)
        });
        if error.is_some() {
            self.request_finished("error");
        }
    }

    /// Records that the connection task stopped serving its active request with `result`.
    pub(crate) fn request_finished(&self, result: &'static str) {
        let Some(state) = self.requests() else {
            return;
        };
        let Some(active) = state.lock(&state.active).take() else {
            return;
        };

        state.emitter.emit_event(|| RequestRow {
            elapsed_us: Some(saturating_micros(active.since.elapsed())),
            result: Some(result),
            ..state.request_row(self.id, "finish", active.request_id, active.desc)
        });
    }
}
