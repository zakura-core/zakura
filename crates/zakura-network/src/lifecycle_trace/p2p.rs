//! `p2p` rows: legacy wire messages.
//!
//! A send writes two rows that share `seq`: `send_start` before the framed write, and `send_done`
//! once the write and flush return. The interval between them is the time the connection task
//! spent handing the message to the socket, which includes waiting for socket buffer space.
//!
//! Transport samples (TCP RTT, congestion window, queued bytes) belong on `send_done` rows: add
//! their fields to [`P2pRow`] and their columns to `tables::P2P`.

use std::{fmt, sync::atomic::Ordering, time::Instant};

use serde::{Serialize, Serializer};
use zakura_chain::{block, serialization::ZcashSerialize};
use zakura_jsonl_trace::{impl_jsonl_trace_event, saturating_count, saturating_micros, tables};

use super::{ConnectionState, ConnectionTrace};
use crate::{
    protocol::external::{InventoryHash, Message, Nonce},
    PeerError,
};

/// The most block hashes one `p2p` row lists.
///
/// A peer answers `getblocks` with at most 500 hashes, so this keeps every hash of an honest
/// message. A peer can send a far longer inventory list, and without a limit one message would
/// produce a row of several megabytes. `item_count` and `tx_count` still count the whole message.
pub(super) const MAX_ROW_BLOCK_HASHES: usize = 500;

/// A send recorded by [`ConnectionTrace::p2p_send_start`], completed by
/// [`ConnectionTrace::p2p_send_done`].
#[derive(Debug)]
pub(crate) struct SendTrace {
    seq: u64,
    msg: &'static str,
    started: Instant,
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
    /// The block hashes in an inventory batch, in message order, up to
    /// [`MAX_ROW_BLOCK_HASHES`].
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

/// A value serialized with its [`fmt::Display`] representation.
struct Shown<T>(T);

impl<T: fmt::Display> Serialize for Shown<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

/// The first [`MAX_ROW_BLOCK_HASHES`] block hashes in an inventory list, serialized as a list of
/// hash strings.
struct BlockHashes<'a>(&'a [InventoryHash]);

impl Serialize for BlockHashes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            self.0
                .iter()
                .filter_map(block_hash)
                .take(MAX_ROW_BLOCK_HASHES)
                .map(Shown),
        )
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

    /// Records that the write started by [`Self::p2p_send_start`] returned `result`.
    pub(crate) fn p2p_send_done(&self, send: Option<SendTrace>, result: &Result<(), PeerError>) {
        let (Some(state), Some(send)) = (self.p2p(), send) else {
            return;
        };

        let write_us = saturating_micros(send.started.elapsed());
        state.emitter.emit_event(|| P2pRow {
            write_us: Some(write_us),
            result: Some(if result.is_ok() { "ok" } else { "error" }),
            error: result.as_ref().err().map(ToString::to_string),
            ..state.row(self.id, "send_done", Some(send.seq), send.msg)
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
