//! Non-blocking per-message P2P tracing for network experiments.
//!
//! Records one row per wire message sent or received, into the `peer_message`
//! table. Block propagation and orphan-rate measurements are derived from this
//! table by correlating the same block hash across every node's rows, so the
//! row for a given `(node, block)` pair either exists or the block appears
//! never to have reached that node.
//!
//! That makes dropped rows a correctness problem rather than a cosmetic one.
//! The trace queue is bounded and non-blocking — the connection task must never
//! wait on disk I/O — so rows *are* dropped under load. Two mechanisms keep
//! that visible:
//!
//! - Adaptive sampling sheds rows progressively as the queue fills, so pressure
//!   degrades the sample rate instead of truncating a burst.
//! - Every dropped row is counted, and the counts are flushed to the
//!   `trace_dropped` table. A run whose `trace_dropped` rows are all zero is one
//!   whose `peer_message` table is complete.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use serde::Serialize;
use zakura_chain::{block, serialization::ZcashSerialize, transaction};
use zakura_jsonl_trace::{
    JsonlEventEmitter, JsonlTraceConfig, JsonlTraceReserveError, JsonlTraceTable, JsonlTracer,
};

use crate::protocol::external::{InventoryHash, Message, Nonce};

#[cfg(test)]
mod tests;

/// Max number of hashes to include in a payload summary.
///
/// An `inv` may carry thousands of hashes; recording them all would make the
/// trace larger than the traffic it describes. The first few are enough to
/// correlate a message across nodes.
const MAX_SUMMARY_HASHES: usize = 5;

/// Remaining queue capacity thresholds for adaptive sampling.
const TRACE_SAMPLE_RATE_LOW_PRESSURE: u64 = 2;
const TRACE_SAMPLE_RATE_MEDIUM_PRESSURE: u64 = 8;
const TRACE_SAMPLE_RATE_HIGH_PRESSURE: u64 = 32;

/// Global connection ID counter.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Per-message wire traffic.
///
/// The column names match the table the prior campaign's analyzers already
/// read, so they run against this table unchanged apart from the timestamp
/// column.
pub const PEER_MESSAGE_TABLE: JsonlTraceTable = JsonlTraceTable::csv(
    "peer_message",
    "peer_message.csv",
    &[
        "dir",
        "msg",
        "peer",
        "conn",
        "mid",
        "summary.count",
        "summary.hashes",
        "summary.height",
        "summary.nonce",
        "summary.body_bytes",
    ],
);

/// Trace rows discarded under queue pressure.
pub const TRACE_DROPPED_TABLE: JsonlTraceTable = JsonlTraceTable::csv(
    "trace_dropped",
    "trace_dropped.csv",
    &["table", "queue_full_dropped", "sampled_dropped"],
);

/// Returns a unique, monotonically increasing connection ID.
pub(crate) fn next_connection_id() -> u64 {
    NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

/// One row of per-message wire traffic.
#[derive(Serialize)]
struct PeerMessageEvent<'a> {
    /// `send` or `recv`.
    dir: &'static str,
    /// Wire message type, for example `inv`, `block`, or `tx`.
    msg: &'static str,
    /// Peer address label.
    peer: &'a str,
    /// Process-local connection ID.
    conn: u64,
    /// Message identifier used to correlate the same message across nodes.
    ///
    /// Content-addressed messages use a stable `<command>:<hash>` form; the
    /// rest fall back to connection-local sequencing.
    mid: String,
    /// Bounded payload summary. Never the payload itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<PayloadSummary>,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(PeerMessageEvent<'_>, PEER_MESSAGE_TABLE);

/// A count of trace rows that never reached the table.
#[derive(Serialize)]
struct TraceDroppedEvent {
    /// The table that dropped rows.
    table: &'static str,
    /// Rows dropped because the queue was full.
    queue_full_dropped: u64,
    /// Rows dropped by adaptive sampling while the queue was under pressure.
    sampled_dropped: u64,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(TraceDroppedEvent, TRACE_DROPPED_TABLE);

/// Bounded summary of a message payload.
#[derive(Serialize)]
struct PayloadSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    hashes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nonce: Option<u64>,
    /// Serialized body size in bytes. Set for `block` and `tx` messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    body_bytes: Option<usize>,
}

/// A payload summary that has not yet rendered its hashes to strings.
///
/// Hash formatting allocates, so it is deferred until after the queue slot is
/// reserved and the row is known to be kept.
struct CompactPayloadSummary {
    count: Option<usize>,
    hashes: Vec<TraceHash>,
    height: Option<u32>,
    nonce: Option<u64>,
    body_bytes: Option<usize>,
}

enum TraceHash {
    Error,
    Block(block::Hash),
    Tx(transaction::Hash),
    Wtx(transaction::WtxId),
    Text(Box<str>),
}

enum TraceMessageId {
    Nonce {
        prefix: &'static str,
        nonce: u64,
    },
    Hash {
        prefix: &'static str,
        hash: TraceHash,
    },
    HashList {
        prefix: &'static str,
        first: Option<TraceHash>,
        count: usize,
    },
    Addr {
        conn: u64,
        seq: u64,
        count: usize,
    },
    ConnectionSeq {
        prefix: &'static str,
        conn: u64,
        seq: u64,
    },
}

enum TraceDropReason {
    QueueFull,
    Sampled,
}

#[derive(Clone)]
struct TraceRuntime {
    emitter: JsonlEventEmitter,
    queue_full_drops: Arc<AtomicU64>,
    sampled_drops: Arc<AtomicU64>,
    sample_counter: Arc<AtomicU64>,
}

impl TraceRuntime {
    fn new(emitter: JsonlEventEmitter) -> Self {
        Self {
            emitter,
            queue_full_drops: Arc::new(AtomicU64::new(0)),
            sampled_drops: Arc::new(AtomicU64::new(0)),
            sample_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record_drop(&self, reason: TraceDropReason) {
        match reason {
            TraceDropReason::QueueFull => self.queue_full_drops.fetch_add(1, Ordering::Relaxed),
            TraceDropReason::Sampled => self.sampled_drops.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Keep one row in N, where N rises as the queue fills.
    ///
    /// Shedding early leaves headroom for the rows that follow, so a burst
    /// degrades to a usable sample instead of filling the queue and then
    /// dropping every remaining row in the burst outright.
    fn adaptive_sample_rate(&self) -> u64 {
        let tracer = self.emitter.tracer();
        let remaining = tracer.capacity();
        let capacity = tracer.max_capacity();

        if remaining <= capacity / 32 {
            TRACE_SAMPLE_RATE_HIGH_PRESSURE
        } else if remaining <= capacity / 16 {
            TRACE_SAMPLE_RATE_MEDIUM_PRESSURE
        } else if remaining <= capacity / 8 {
            TRACE_SAMPLE_RATE_LOW_PRESSURE
        } else {
            1
        }
    }

    fn should_sample_drop(&self) -> bool {
        let sample_rate = self.adaptive_sample_rate();
        sample_rate > 1
            && self
                .sample_counter
                .fetch_add(1, Ordering::Relaxed)
                .checked_rem(sample_rate)
                .is_some_and(|remainder| remainder != 0)
    }

    /// Flush the pending drop counts into the `trace_dropped` table.
    ///
    /// The counts are taken with `swap`, so a failed emit must put them back or
    /// the drops go unreported.
    fn try_emit_drop_record(&self) {
        let queue_full_dropped = self.queue_full_drops.swap(0, Ordering::Relaxed);
        let sampled_dropped = self.sampled_drops.swap(0, Ordering::Relaxed);

        if queue_full_dropped == 0 && sampled_dropped == 0 {
            return;
        }

        let emitted = self.emitter.try_emit_event(|| TraceDroppedEvent {
            table: PEER_MESSAGE_TABLE.table(),
            queue_full_dropped,
            sampled_dropped,
        });

        if matches!(emitted, Err(JsonlTraceReserveError::Full)) {
            self.queue_full_drops
                .fetch_add(queue_full_dropped, Ordering::Relaxed);
            self.sampled_drops
                .fetch_add(sampled_dropped, Ordering::Relaxed);
        }
    }
}

/// A handle for emitting P2P message traces. Cloning is cheap.
#[derive(Clone, Default)]
pub struct P2pTracer {
    runtime: Option<TraceRuntime>,
}

impl P2pTracer {
    /// Create a tracer writing to `trace_dir`, or a no-op tracer if it is `None`.
    pub fn new(trace_dir: Option<PathBuf>) -> Self {
        let Some(trace_dir) = trace_dir else {
            return Self::noop();
        };

        let tracer = JsonlTracer::spawn_with_config(trace_dir, JsonlTraceConfig::default());

        if !tracer.is_enabled() {
            return Self::noop();
        }

        Self {
            runtime: Some(TraceRuntime::new(JsonlEventEmitter::new(
                tracer,
                zakura_jsonl_trace::node_id(),
            ))),
        }
    }

    /// Create a tracer from an existing emitter, so a test can drive the
    /// writer directly instead of going through a trace directory.
    #[cfg(test)]
    pub fn from_emitter(emitter: JsonlEventEmitter) -> Self {
        Self {
            runtime: emitter.is_enabled().then(|| TraceRuntime::new(emitter)),
        }
    }

    /// Create a no-op tracer. Every trace call returns immediately.
    pub fn noop() -> Self {
        Self { runtime: None }
    }

    /// Returns true when this tracer emits rows.
    pub fn is_enabled(&self) -> bool {
        self.runtime.is_some()
    }

    /// Record one wire message. Never blocks, and never fails the connection.
    pub fn trace_msg(
        &self,
        direction: &'static str,
        msg: &Message,
        peer: &str,
        connection_id: u64,
        seq: &AtomicU64,
    ) {
        let Some(runtime) = &self.runtime else {
            return;
        };

        if runtime.should_sample_drop() {
            runtime.record_drop(TraceDropReason::Sampled);
            return;
        }

        let emitted = runtime.emitter.try_emit_event(|| {
            let (msg_type, summary) = summarize_message(msg);

            PeerMessageEvent {
                dir: direction,
                msg: msg_type,
                peer,
                conn: connection_id,
                mid: render_message_id(message_id(msg, connection_id, seq)),
                summary: summary.map(render_summary),
            }
        });

        match emitted {
            Err(JsonlTraceReserveError::Full) => {
                runtime.record_drop(TraceDropReason::QueueFull);
                return;
            }
            Err(JsonlTraceReserveError::Disabled | JsonlTraceReserveError::Closed) => return,
            Ok(()) => {}
        }

        runtime.try_emit_drop_record();
    }
}

impl std::fmt::Debug for P2pTracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P2pTracer")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

/// One connection's share of the P2P tracer.
///
/// The send and receive paths live in different types — [`PeerTx`] owns the
/// sink, [`Connection`] owns the stream — but they must agree on the connection
/// ID and draw sequence numbers from one counter, or the `mid` values they
/// produce for the same connection collide. Cloning this hands out that shared
/// state.
///
/// [`PeerTx`]: crate::peer::connection::peer_tx::PeerTx
/// [`Connection`]: crate::peer::connection::Connection
#[derive(Clone, Debug, Default)]
pub(crate) struct P2pTraceContext {
    tracer: P2pTracer,
    peer: Option<Arc<str>>,
    conn: u64,
    seq: Arc<AtomicU64>,
}

impl P2pTraceContext {
    /// Create a context for a new connection, allocating its connection ID.
    pub(crate) fn new(tracer: P2pTracer, peer: impl Into<Arc<str>>) -> Self {
        if !tracer.is_enabled() {
            return Self::noop();
        }

        Self {
            tracer,
            peer: Some(peer.into()),
            conn: next_connection_id(),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a context that never emits.
    pub(crate) fn noop() -> Self {
        Self::default()
    }

    /// Record one message in `direction` on this connection.
    pub(crate) fn trace(&self, direction: &'static str, msg: &Message) {
        let Some(peer) = &self.peer else {
            return;
        };

        self.tracer
            .trace_msg(direction, msg, peer, self.conn, &self.seq);
    }
}

/// Extract a bounded summary from a message without cloning its payload.
fn summarize_message(msg: &Message) -> (&'static str, Option<CompactPayloadSummary>) {
    let summary = |summary: CompactPayloadSummary| Some(summary);
    let counted = |count: usize, hashes: Vec<TraceHash>| {
        Some(CompactPayloadSummary {
            count: Some(count),
            hashes,
            height: None,
            nonce: None,
            body_bytes: None,
        })
    };
    let nonce_only = |nonce: u64| {
        Some(CompactPayloadSummary {
            count: None,
            hashes: Vec::new(),
            height: None,
            nonce: Some(nonce),
            body_bytes: None,
        })
    };

    match msg {
        Message::Version(version) => (
            "version",
            summary(CompactPayloadSummary {
                count: None,
                hashes: Vec::new(),
                height: Some(version.start_height.0),
                nonce: Some(version.nonce.0),
                body_bytes: None,
            }),
        ),
        Message::Verack => ("verack", None),
        Message::Ping(Nonce(nonce)) => ("ping", nonce_only(*nonce)),
        Message::Pong(Nonce(nonce)) => ("pong", nonce_only(*nonce)),
        Message::Reject { message, ccode, .. } => (
            "reject",
            summary(CompactPayloadSummary {
                count: None,
                hashes: vec![TraceHash::Text(
                    format!("{message}:{ccode:?}").into_boxed_str(),
                )],
                height: None,
                nonce: None,
                body_bytes: None,
            }),
        ),
        Message::P2pV2Upgrade(payload) => (
            "p2pv2upgrade",
            summary(CompactPayloadSummary {
                count: None,
                hashes: Vec::new(),
                height: None,
                nonce: None,
                body_bytes: Some(payload.len()),
            }),
        ),
        Message::GetAddr => ("getaddr", None),
        Message::Addr(addrs) => ("addr", counted(addrs.len(), Vec::new())),
        Message::GetBlocks { known_blocks, .. } => (
            "getblocks",
            counted(
                known_blocks.len(),
                first_n_block_hashes(known_blocks, MAX_SUMMARY_HASHES),
            ),
        ),
        Message::Inv(items) => (
            "inv",
            counted(items.len(), first_n_inv_hashes(items, MAX_SUMMARY_HASHES)),
        ),
        Message::GetHeaders { known_blocks, .. } => (
            "getheaders",
            counted(
                known_blocks.len(),
                first_n_block_hashes(known_blocks, MAX_SUMMARY_HASHES),
            ),
        ),
        Message::Headers(headers) => (
            "headers",
            counted(
                headers.len(),
                headers
                    .iter()
                    .take(MAX_SUMMARY_HASHES)
                    .map(|header| TraceHash::Block(header.header.hash()))
                    .collect(),
            ),
        ),
        Message::GetData(items) => (
            "getdata",
            counted(items.len(), first_n_inv_hashes(items, MAX_SUMMARY_HASHES)),
        ),
        Message::Block(block) => (
            "block",
            summary(CompactPayloadSummary {
                count: None,
                hashes: vec![TraceHash::Block(block.hash())],
                height: block.coinbase_height().map(|height| height.0),
                nonce: None,
                body_bytes: Some(block.zcash_serialized_size()),
            }),
        ),
        Message::Tx(tx) => (
            "tx",
            summary(CompactPayloadSummary {
                count: None,
                hashes: vec![TraceHash::Tx(tx.id().mined_id())],
                height: None,
                nonce: None,
                body_bytes: Some(tx.size()),
            }),
        ),
        Message::NotFound(items) => (
            "notfound",
            counted(items.len(), first_n_inv_hashes(items, MAX_SUMMARY_HASHES)),
        ),
        Message::Mempool => ("mempool", None),
    }
}

/// Build an identifier used to correlate one message across nodes.
fn message_id(msg: &Message, conn: u64, seq: &AtomicU64) -> TraceMessageId {
    match msg {
        Message::Ping(Nonce(nonce)) => TraceMessageId::Nonce {
            prefix: "ping",
            nonce: *nonce,
        },
        Message::Pong(Nonce(nonce)) => TraceMessageId::Nonce {
            prefix: "pong",
            nonce: *nonce,
        },
        // Block IDs are the join key for every propagation measurement, and the
        // analyzers recover the hash as `substr(mid, 7)` — everything after
        // `block:`. Changing this prefix silently breaks them.
        Message::Block(block) => TraceMessageId::Hash {
            prefix: "block",
            hash: TraceHash::Block(block.hash()),
        },
        Message::Tx(tx) => TraceMessageId::Hash {
            prefix: "tx",
            hash: TraceHash::Tx(tx.id().mined_id()),
        },
        Message::Inv(items) => inv_id("inv", items),
        Message::GetData(items) => inv_id("getdata", items),
        Message::NotFound(items) => inv_id("notfound", items),
        Message::GetBlocks { known_blocks, .. } => block_hash_list_id("getblocks", known_blocks),
        Message::GetHeaders { known_blocks, .. } => block_hash_list_id("getheaders", known_blocks),
        Message::Headers(headers) => TraceMessageId::HashList {
            prefix: "headers",
            first: headers
                .first()
                .map(|header| TraceHash::Block(header.header.hash())),
            count: headers.len(),
        },
        Message::Addr(addrs) => TraceMessageId::Addr {
            conn,
            seq: seq_next(seq),
            count: addrs.len(),
        },
        // Parameterless or rarely-correlated messages use connection+sequence.
        _ => TraceMessageId::ConnectionSeq {
            prefix: msg.command(),
            conn,
            seq: seq_next(seq),
        },
    }
}

fn seq_next(seq: &AtomicU64) -> u64 {
    seq.fetch_add(1, Ordering::Relaxed)
}

fn inv_id(prefix: &'static str, items: &[InventoryHash]) -> TraceMessageId {
    TraceMessageId::HashList {
        prefix,
        first: items.first().map(trace_hash_from_inventory),
        count: items.len(),
    }
}

fn block_hash_list_id(prefix: &'static str, hashes: &[block::Hash]) -> TraceMessageId {
    TraceMessageId::HashList {
        prefix,
        first: hashes.first().map(|hash| TraceHash::Block(*hash)),
        count: hashes.len(),
    }
}

fn trace_hash_from_inventory(hash: &InventoryHash) -> TraceHash {
    match hash {
        InventoryHash::Block(hash) | InventoryHash::FilteredBlock(hash) => TraceHash::Block(*hash),
        InventoryHash::Tx(hash) => TraceHash::Tx(*hash),
        InventoryHash::Wtx(wtx_id) => TraceHash::Wtx(*wtx_id),
        InventoryHash::Error => TraceHash::Error,
    }
}

fn first_n_inv_hashes(items: &[InventoryHash], n: usize) -> Vec<TraceHash> {
    items
        .iter()
        .take(n)
        .map(trace_hash_from_inventory)
        .collect()
}

fn first_n_block_hashes(hashes: &[block::Hash], n: usize) -> Vec<TraceHash> {
    hashes
        .iter()
        .take(n)
        .map(|hash| TraceHash::Block(*hash))
        .collect()
}

fn render_message_id(message_id: TraceMessageId) -> String {
    match message_id {
        TraceMessageId::Nonce { prefix, nonce } => format!("{prefix}:{nonce}"),
        TraceMessageId::Hash { prefix, hash } => format!("{prefix}:{}", render_trace_hash(hash)),
        TraceMessageId::HashList {
            prefix,
            first,
            count,
        } => {
            let first = first.map(render_trace_hash).unwrap_or_default();
            format!("{prefix}:{first}+{count}")
        }
        TraceMessageId::Addr { conn, seq, count } => format!("addr:{conn}:{seq}:{count}"),
        TraceMessageId::ConnectionSeq { prefix, conn, seq } => format!("{prefix}:{conn}:{seq}"),
    }
}

fn render_trace_hash(hash: TraceHash) -> String {
    match hash {
        TraceHash::Error => "error".to_string(),
        TraceHash::Block(hash) => hash.to_string(),
        TraceHash::Tx(hash) => hash.to_string(),
        TraceHash::Wtx(wtx_id) => wtx_id.id.to_string(),
        TraceHash::Text(text) => text.into_string(),
    }
}

fn render_summary(summary: CompactPayloadSummary) -> PayloadSummary {
    PayloadSummary {
        count: summary.count,
        hashes: summary.hashes.into_iter().map(render_trace_hash).collect(),
        height: summary.height,
        nonce: summary.nonce,
        body_bytes: summary.body_bytes,
    }
}
