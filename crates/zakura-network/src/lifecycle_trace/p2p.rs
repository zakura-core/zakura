//! `p2p` rows: legacy wire messages.
//!
//! A send writes two rows that share `seq`: `send_start` before the framed write, and `send_done`
//! once the write and flush return. The interval between them is the time the connection task
//! spent handing the message to the socket, which includes waiting for socket buffer space.
//!
//! A block body send on a connection with upload progress also records the kernel's TCP state
//! on its `send_done` row (`tcp_*` columns), and a third row with the same `seq` when the watch
//! of its delivery ends: `body_acked` once the peer acknowledged every byte, or
//! `body_ack_timeout` if the connection closed or the watch timed out first. The interval from
//! `send_done` to `body_acked` is time the bytes spent in the kernel and on the network after the
//! write returned.

use std::{fmt, sync::atomic::Ordering, time::Instant};

use serde::{Serialize, Serializer};
use zakura_chain::{block, serialization::ZcashSerialize};
use zakura_jsonl_trace::{impl_jsonl_trace_event, saturating_count, saturating_micros, tables};

use super::{ConnectionState, ConnectionTrace};
use crate::{
    peer::tcp_progress::{TcpInfo, UploadOutcome, UploadReport},
    protocol::external::{InventoryHash, Message, Nonce},
    PeerError,
};

/// A send recorded by [`ConnectionTrace::p2p_send_start`], completed by
/// [`ConnectionTrace::p2p_send_done`].
#[derive(Debug)]
pub(crate) struct SendTrace {
    seq: u64,
    msg: &'static str,
    started: Instant,
}

impl SendTrace {
    /// The send's sequence number on its connection.
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }
}

#[derive(Serialize)]
struct P2pRow<'a> {
    event: &'static str,
    conn: u64,
    direction: &'static str,
    peer: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    msg: &'static str,
    #[serde(flatten)]
    summary: Option<MessageSummary<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_us: Option<u64>,
    #[serde(flatten)]
    tcp: Option<TcpColumns>,
    /// Time from the start of the write to the peer's acknowledgement of the last byte.
    #[serde(skip_serializing_if = "Option::is_none")]
    ack_us: Option<u64>,
    /// Bytes of the encoded message the peer acknowledged.
    #[serde(skip_serializing_if = "Option::is_none")]
    acked_bytes: Option<u64>,
    /// Segments retransmitted on the connection while the message was being delivered.
    #[serde(skip_serializing_if = "Option::is_none")]
    tcp_retrans: Option<u32>,
    /// Socket samples taken while watching the delivery.
    #[serde(skip_serializing_if = "Option::is_none")]
    samples: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl_jsonl_trace_event!(P2pRow<'_>, tables::P2P);

/// Identifying fields of a message, without its payload.
#[derive(Default, Serialize)]
struct MessageSummary<'a> {
    /// The block or transaction carried by a `block` or `tx` message.
    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_count: Option<u64>,
    /// Every block hash in an inventory batch, in message order.
    #[serde(skip_serializing_if = "Option::is_none")]
    block_hashes: Option<BlockHashes<'a>>,
    /// The number of transaction entries in an inventory batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    tx_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nonce: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_bytes: Option<u64>,
}

/// A connection's kernel TCP state; see [`TcpInfo`].
#[derive(Serialize)]
struct TcpColumns {
    #[serde(skip_serializing_if = "Option::is_none")]
    tcp_notsent_bytes: Option<u32>,
    tcp_unacked: u32,
    tcp_cwnd: u32,
    tcp_rtt_us: u32,
    tcp_total_retrans: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    tcp_delivery_rate: Option<u64>,
}

impl From<&TcpInfo> for TcpColumns {
    fn from(info: &TcpInfo) -> Self {
        Self {
            tcp_notsent_bytes: info.notsent_bytes,
            tcp_unacked: info.unacked,
            tcp_cwnd: info.snd_cwnd,
            tcp_rtt_us: info.rtt_us,
            tcp_total_retrans: info.total_retrans,
            tcp_delivery_rate: info.delivery_rate,
        }
    }
}

/// A value serialized with its [`fmt::Display`] representation.
struct Shown<T>(T);

impl<T: fmt::Display> Serialize for Shown<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

/// The block hashes in an inventory list, serialized as a list of hash strings.
struct BlockHashes<'a>(&'a [InventoryHash]);

impl Serialize for BlockHashes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().filter_map(block_hash).map(Shown))
    }
}

fn block_hash(item: &InventoryHash) -> Option<block::Hash> {
    match item {
        InventoryHash::Block(hash) | InventoryHash::FilteredBlock(hash) => Some(*hash),
        InventoryHash::Tx(_) | InventoryHash::Wtx(_) | InventoryHash::Error => None,
    }
}

impl ConnectionTrace {
    /// Records that the connection task is about to write `msg`.
    ///
    /// Returns `None` when the tracer does not write the `p2p` table.
    pub(crate) fn p2p_send_start(&self, msg: &Message) -> Option<SendTrace> {
        let state = self.p2p()?;
        let seq = state.next_seq.fetch_add(1, Ordering::Relaxed);
        let msg_type = msg.command();
        state.emitter.emit_event(|| P2pRow {
            summary: Some(summarize(msg)),
            ..state.row(self.id, "send_start", Some(seq), msg_type)
        });

        Some(SendTrace {
            seq,
            msg: msg_type,
            started: Instant::now(),
        })
    }

    /// Records that the write started by [`Self::p2p_send_start`] returned `result`, with the
    /// connection's TCP state when it returned, if it was sampled.
    pub(crate) fn p2p_send_done(
        &self,
        send: Option<SendTrace>,
        result: &Result<(), PeerError>,
        tcp: Option<&TcpInfo>,
    ) {
        let (Some(state), Some(send)) = (self.p2p(), send) else {
            return;
        };

        let write_us = saturating_micros(send.started.elapsed());
        state.emitter.emit_event(|| P2pRow {
            write_us: Some(write_us),
            tcp: tcp.map(TcpColumns::from),
            result: Some(if result.is_ok() { "ok" } else { "error" }),
            error: result.as_ref().err().map(ToString::to_string),
            ..state.row(self.id, "send_done", Some(send.seq), send.msg)
        });
    }

    /// Records how the delivery of the block body sent with sequence number `seq` ended.
    pub(crate) fn p2p_body_upload(&self, seq: u64, report: &UploadReport) {
        let Some(state) = self.p2p() else {
            return;
        };

        let (event, result) = match report.outcome {
            UploadOutcome::Acked => ("body_acked", "acked"),
            UploadOutcome::TimedOut => ("body_ack_timeout", "timeout"),
            UploadOutcome::Closed => ("body_ack_timeout", "closed"),
        };
        state.emitter.emit_event(|| P2pRow {
            tcp: report.last().map(TcpColumns::from),
            ack_us: (report.outcome == UploadOutcome::Acked)
                .then(|| saturating_micros(report.elapsed)),
            acked_bytes: Some(report.acked_message_bytes()),
            tcp_retrans: report.retransmits(),
            samples: Some(saturating_count(report.samples.len())),
            result: Some(result),
            ..state.row(self.id, event, Some(seq), "block")
        });
    }

    /// Records a decoded message received on this connection.
    pub(crate) fn p2p_recv(&self, msg: &Message) {
        let Some(state) = self.p2p() else {
            return;
        };

        state.emitter.emit_event(|| P2pRow {
            summary: Some(summarize(msg)),
            ..state.row(self.id, "recv", None, msg.command())
        });
    }
}

impl ConnectionState {
    fn row(
        &self,
        conn: u64,
        event: &'static str,
        seq: Option<u64>,
        msg: &'static str,
    ) -> P2pRow<'_> {
        P2pRow {
            event,
            conn,
            direction: self.direction,
            peer: &self.peer,
            seq,
            msg,
            summary: None,
            write_us: None,
            tcp: None,
            ack_us: None,
            acked_bytes: None,
            tcp_retrans: None,
            samples: None,
            result: None,
            error: None,
        }
    }
}

/// Extracts identifying fields from `msg` without copying its payload.
fn summarize(msg: &Message) -> MessageSummary<'_> {
    match msg {
        Message::Version(version) => MessageSummary {
            height: Some(version.start_height.0),
            nonce: Some(version.nonce.0),
            ..MessageSummary::default()
        },
        Message::Ping(Nonce(nonce)) | Message::Pong(Nonce(nonce)) => MessageSummary {
            nonce: Some(*nonce),
            ..MessageSummary::default()
        },
        Message::Inv(items) | Message::GetData(items) | Message::NotFound(items) => {
            let block_count = items.iter().filter_map(block_hash).count();
            MessageSummary {
                item_count: Some(saturating_count(items.len())),
                block_hashes: (block_count > 0).then_some(BlockHashes(items)),
                tx_count: Some(saturating_count(
                    items
                        .iter()
                        .filter(|item| matches!(item, InventoryHash::Tx(_) | InventoryHash::Wtx(_)))
                        .count(),
                )),
                ..MessageSummary::default()
            }
        }
        Message::GetBlocks { known_blocks, .. } | Message::GetHeaders { known_blocks, .. } => {
            MessageSummary {
                item_count: Some(saturating_count(known_blocks.len())),
                ..MessageSummary::default()
            }
        }
        Message::Headers(headers) => MessageSummary {
            item_count: Some(saturating_count(headers.len())),
            ..MessageSummary::default()
        },
        Message::Addr(addrs) => MessageSummary {
            item_count: Some(saturating_count(addrs.len())),
            ..MessageSummary::default()
        },
        Message::Block(block) => MessageSummary {
            hash: Some(block.hash().to_string()),
            height: block.coinbase_height().map(|height| height.0),
            body_bytes: Some(saturating_count(block.zcash_serialized_size())),
            ..MessageSummary::default()
        },
        Message::Tx(tx) => MessageSummary {
            hash: Some(tx.id().mined_id().to_string()),
            body_bytes: Some(saturating_count(tx.size())),
            ..MessageSummary::default()
        },
        Message::P2pV2Upgrade(payload) => MessageSummary {
            body_bytes: Some(saturating_count(payload.len())),
            ..MessageSummary::default()
        },
        Message::Verack | Message::Reject { .. } | Message::GetAddr | Message::Mempool => {
            MessageSummary::default()
        }
    }
}
