//! Block lifecycle trace boundaries for the legacy P2P stack.
//!
//! Three CSV tables record where a block spends time between "this node has it" and "a peer has
//! it":
//!
//! - `p2p`: every wire message on a legacy connection. Sends record both the attempt
//!   (`send_start`) and the completed framed write and flush (`send_done`); receives record the
//!   decoded message (`recv`). Inventory rows list every block hash in the batch.
//! - `p2p_request`: an outbound request's path through the peer set and the connection: peer
//!   selection (`dispatch`), the connection channel (`enqueue`, `dequeue`), the request write
//!   (`write`), and its completion (`finish`).
//! - `block_gossip`: a block broadcast's path through the shared upload slots: broadcast start,
//!   per-peer task start, slot wait and acquisition, inventory queueing and write, and slot
//!   release with its reason.
//!
//! Every row carries the envelope's process-wide monotonic `ts`, so intervals can be subtracted
//! across these tables and across other tables written by the same process. Rows about one
//! connection share `conn`, which is also the `peer_id` in `legacy_peer_request` rows. Request
//! rows share `request_id` with `legacy_peer_request` rows for the same request.
//!
//! With no tracer configured, or with none of these tables selected, every hook returns before
//! reading the clock or building a row.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use zakura_jsonl_trace::{tables, JsonlEventEmitter, JsonlTracer};

mod gossip;
mod p2p;
mod request;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) use gossip::{AdvertTrace, SlotRelease};
pub(crate) use request::RequestTrace;

/// The next process-local connection identifier.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// A cloneable handle that creates lifecycle traces for legacy connections and broadcasts.
#[derive(Clone, Debug, Default)]
pub(crate) struct LifecycleTrace {
    emitter: Option<JsonlEventEmitter>,
    next_broadcast_id: Arc<AtomicU64>,
}

impl LifecycleTrace {
    /// Creates a lifecycle trace that writes the `p2p`, `p2p_request`, and `block_gossip` tables
    /// that `tracer` selects.
    pub(crate) fn new(tracer: JsonlTracer) -> Self {
        let selected = [tables::P2P, tables::P2P_REQUEST, tables::BLOCK_GOSSIP]
            .iter()
            .any(|table| tracer.is_table_enabled(table.table()));

        Self {
            emitter: selected
                .then(|| JsonlEventEmitter::new(tracer, zakura_jsonl_trace::node_id())),
            next_broadcast_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Creates a lifecycle trace that writes nothing.
    pub(crate) fn noop() -> Self {
        Self::default()
    }

    /// Allocates an identifier for a new legacy connection and its trace context.
    ///
    /// `inbound` is the connection direction, and `peer` is the configured privacy-preserving
    /// address label.
    pub(crate) fn connection(&self, inbound: bool, peer: impl Into<Arc<str>>) -> ConnectionTrace {
        let id = next_connection_id();
        let Some(emitter) = &self.emitter else {
            return ConnectionTrace { id, state: None };
        };

        let tracer = emitter.tracer();
        ConnectionTrace {
            id,
            state: Some(Arc::new(ConnectionState {
                emitter: emitter.clone(),
                direction: if inbound { "inbound" } else { "outbound" },
                peer: peer.into(),
                p2p: tracer.is_table_enabled(tables::P2P.table()),
                requests: tracer.is_table_enabled(tables::P2P_REQUEST.table()),
                gossip: tracer.is_table_enabled(tables::BLOCK_GOSSIP.table()),
                next_seq: AtomicU64::new(0),
                queued: AtomicU64::new(0),
                active: Mutex::new(None),
                dispatching: Mutex::new(None),
            })),
        }
    }
}

/// Returns a new process-local connection identifier.
fn next_connection_id() -> u64 {
    // Wrap-around is unreachable for a process-local u64 counter.
    NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

/// One legacy connection's identifier and trace state.
///
/// The client, the connection task, its message sender, and the block gossip sender hold clones,
/// so they agree on the connection ID, the send sequence, the queued request count, and the
/// request the connection is currently serving.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionTrace {
    id: u64,
    state: Option<Arc<ConnectionState>>,
}

#[derive(Debug)]
struct ConnectionState {
    emitter: JsonlEventEmitter,
    direction: &'static str,
    peer: Arc<str>,
    /// Whether the tracer writes the `p2p` table.
    p2p: bool,
    /// Whether the tracer writes the `p2p_request` table.
    requests: bool,
    /// Whether the tracer writes the `block_gossip` table.
    gossip: bool,
    /// The sequence number of the next message sent on this connection.
    next_seq: AtomicU64,
    /// Stamped requests accepted by the connection channel and not yet dequeued.
    queued: AtomicU64,
    /// The client request the connection task is serving, from dequeue to completion.
    active: Mutex<Option<request::ActiveRequest>>,
    /// The identifier of the request the peer set is handing to this connection's client.
    dispatching: Mutex<Option<u64>>,
}

impl ConnectionTrace {
    /// Allocates a connection identifier without trace output.
    pub(crate) fn untraced() -> Self {
        Self {
            id: next_connection_id(),
            state: None,
        }
    }

    /// Returns the process-local connection identifier.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Returns the trace state when the tracer writes the `p2p` table.
    fn p2p(&self) -> Option<&ConnectionState> {
        self.state.as_deref().filter(|state| state.p2p)
    }

    /// Returns the trace state when the tracer writes the `p2p_request` table.
    fn requests(&self) -> Option<&ConnectionState> {
        self.state.as_deref().filter(|state| state.requests)
    }

    /// Returns the trace state when the tracer writes the `block_gossip` table.
    fn gossip(&self) -> Option<&ConnectionState> {
        self.state.as_deref().filter(|state| state.gossip)
    }
}
