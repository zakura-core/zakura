//! Header sync version 9, which the peer message regulation specification
//! defines in "Header sync — stream 5, version 9".
//!
//! This module holds the wire codec, the message table, the publisher, and the
//! subscriber. The service runs one publisher and one subscriber per session.
//! The codec and the message table are public, so simulator peers can build
//! and read every version 9 message.

mod publisher;
mod subscriber;
mod wire;

#[cfg(test)]
pub(crate) use publisher::FakePort;
pub(crate) use publisher::{LeaseOwner, PageLimits, Publisher};
pub(crate) use subscriber::{Admitted, Ended, SubscribeError, Subscriber};

use super::config::COMMON_HEADER_BYTES;
use crate::zakura::regulation::{sizing, ServeCapacity, ServeLimits};
#[cfg(any(test, feature = "header-fuzz"))]
use crate::zakura::{
    wire_codec::{decode_frame, encode_frame, WireError, WireMessage},
    Frame,
};
pub(crate) use wire::*;
pub use wire::{
    budget, layout, rows, CommonMessage, Cursor, Headers, HeadersOutcome, HeadersOutcomeCode,
    Message, Operation, RegtestMessage, Status, SubscribeHeaders, HEADER_SYNC_BUDGET,
};

/// Page reads that may run at once across the node. Each read holds a
/// retained-path lease in state, which bounds its own capacity, so this keeps
/// many subscriptions from queueing reads that state cannot run yet.
const NODE_EXECUTION: usize = 8;

/// Page reads that may run at once for one peer: its session and a
/// replacement that overlaps it. Each session's publisher reads one page at a
/// time.
const PEER_EXECUTION: usize = 2;

/// The toolkit's serving defaults for responses of the largest page across
/// `peers` header-sync sessions.
const fn default_limits(peers: usize) -> ServeLimits {
    // The largest page is about 2 MiB, so widening it to u64 is lossless.
    sizing::serve_limits(
        largest_page(COMMON_HEADER_BYTES) as u64,
        sizing::TARGET_RTT,
        peers,
    )
}

// A full window of one-header pages fits the per-peer response slots, so only
// output bytes can make a publisher at full credit wait.
const _: () = assert!(
    // Widening u32 to usize is lossless on every supported target.
    default_limits(1).peer_output_responses >= MAX_HS_PUSH_CREDIT_HEADERS as usize,
    "a full window of pages fits the per-peer response slots"
);

/// Unsent pages one peer may hold in production: the toolkit's default,
/// 64 MiB.
///
/// Output is keyed by peer ID, so a replacement session that overlaps its
/// predecessor ([`PEER_EXECUTION`]) shares this budget. The default holds
/// [`PEER_EXECUTION`] × [`Message::BUDGET`]'s output share on every network.
pub(crate) const SERVE_PEER_OUTPUT_BYTES: u64 = default_limits(1).peer_output_bytes;

// Widening usize to u64 is lossless on every supported target.
const _: () = assert!(
    SERVE_PEER_OUTPUT_BYTES >= PEER_EXECUTION as u64 * CommonMessage::BUDGET.output
        && SERVE_PEER_OUTPUT_BYTES >= PEER_EXECUTION as u64 * RegtestMessage::BUDGET.output,
    "every overlapping session of one peer holds a full output share"
);

/// Serving limits for published pages: [`NODE_EXECUTION`] and
/// [`PEER_EXECUTION`] page reads, `peer_output_bytes` of unsent pages per
/// peer, and the toolkit's defaults across `peers` sessions for the rest.
/// `node_output_bytes` replaces the node output budget when set.
///
/// [`Message::BUDGET`]'s output share is the derived value of
/// `peer_output_bytes`.
pub(crate) fn serving_limits(
    peer_output_bytes: u64,
    peers: usize,
    node_output_bytes: Option<u64>,
) -> ServeLimits {
    let derived = ServeLimits {
        node_execution: NODE_EXECUTION,
        peer_execution: PEER_EXECUTION,
        peer_output_bytes,
        ..default_limits(peers)
    };
    sizing::with_node_output_bytes(derived, node_output_bytes)
}

/// The production serving capacity for published pages, with
/// [`SERVE_PEER_OUTPUT_BYTES`] per peer.
pub(crate) fn serving_capacity(peers: usize, node_output_bytes: Option<u64>) -> ServeCapacity {
    ServeCapacity::new(
        "header-sync",
        &SUBSCRIBE_HEADERS,
        serving_limits(SERVE_PEER_OUTPUT_BYTES, peers, node_output_bytes),
    )
    .expect("the header-sync serving limits are positive")
}

impl Status {
    /// This node's advertisement of a committed `snapshot` under its serving
    /// `limits`.
    pub(crate) fn from_snapshot(
        snapshot: &zakura_header_chain::EngineSnapshot,
        limits: PageLimits,
    ) -> Self {
        Self {
            work_anchor_height: snapshot.frontiers.finalized.height,
            work_anchor_hash: snapshot.frontiers.finalized.hash,
            selected_tip_height: snapshot.frontiers.header_best.height,
            selected_tip_hash: snapshot.frontiers.header_best.hash,
            suffix_cumulative_work: snapshot.header_best_score.suffix_work.as_u256(),
            oldest_retained_height: snapshot.oldest_retained_height,
            max_headers_per_response: limits.max_headers,
            max_subscriptions: MAX_HS_SUBSCRIPTIONS,
            max_message_bytes: limits.max_bytes,
            tree_aux_schema_mask: limits.aux_schema_mask,
        }
    }
}

#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod wire_tests;

/// Decode fuzz bytes as one version 9 frame, and check that a decoded message
/// re-encodes to the same payload.
///
/// Bit 7 of the first byte picks Regtest headers over Mainnet and Testnet
/// headers, and its low two bits pick the message type. The remaining bytes
/// are the payload. Returns whether the frame decoded.
///
/// # Panics
///
/// If a decoded message does not re-encode to its payload.
#[cfg(any(test, feature = "header-fuzz"))]
pub fn replay_header_sync_v9_codec_bytes(bytes: &[u8]) -> bool {
    let Some((&selector, payload)) = bytes.split_first() else {
        return false;
    };
    let frame = Frame {
        message_type: u16::from(selector & 3) + 1,
        flags: 0,
        payload: payload.to_vec(),
    };
    if selector & 0x80 == 0 {
        replay::<CommonMessage>(&frame)
    } else {
        replay::<RegtestMessage>(&frame)
    }
}

/// Decode `frame` as an `M`, and check that it re-encodes to the same payload.
/// Returns whether it decoded.
#[cfg(any(test, feature = "header-fuzz"))]
fn replay<M: WireMessage<Error = WireError>>(frame: &Frame) -> bool {
    let Ok(message) = decode_frame::<M>(frame) else {
        return false;
    };
    let encoded = encode_frame(&message).expect("a decoded message encodes");
    assert_eq!(
        encoded.payload, frame.payload,
        "a decoded message re-encodes to its payload"
    );
    true
}

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use publisher::{
    trace_header_publisher_bytes, HeaderPublisherHistory, HeaderPublisherStep,
    HeaderPublisherSubscriptionView, HeaderPublisherView,
};
