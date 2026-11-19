//! A trace row for every measured answer time of a legacy connection, and for every announced
//! block the peer did not deliver.
//!
//! Each `delivery_sample` row says what was measured (`kind`), the measured time (`sample_ms`),
//! the score before and after (`score_before_ms`, `score_ms`), and `weight`, the decayed number of
//! samples the score rests on. Together they show why a peer's score is what it is, and how
//! much of it comes from pings rather than blocks.
//!
//! `kind` is `ping`, `block` or `large_block`. Both block kinds carry `wire_bytes`, the
//! serialized size of the blocks in the reply. A `large_block` reply was too big to arrive in
//! one round trip, so it does not change the score: its `score_before_ms` and `score_ms` are
//! equal, and both are missing when the peer has no score yet. It updates the peer's large-block
//! slowdown factor instead. Its row has `expected_ms`, what the peer was expected to take for a
//! block of that size just before this sample, and `slowdown`, the factor after it: how many
//! times longer than a new connection's round trips for the size the peer's large blocks take.
//! Both are missing when the peer has no score yet. Comparing `sample_ms` with `expected_ms`
//! shows how well the expectation fits.
//!
//! A `block_missed` row says the peer announced a block and then did not deliver it to a first or
//! a second gossip block request (`request`). The miss preserves the score. With scoring enabled,
//! routing passes over the peer until a later body from that connection verifies or the miss expires.
//!
//! `connection_id` is the number the connection's `legacy_gossip` and `legacy_peer_request` rows
//! carry, so the tables join.

use std::time::Duration;

use serde::Serialize;

use super::delivery::{DeliveryEstimate, DeliverySampleKind};
use zakura_jsonl_trace::{saturating_millis, JsonlEventEmitter, JsonlTraceEvent, JsonlTraceTable};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("delivery_score", "delivery_score.jsonl");

/// The `kind` column of a sample's row.
fn label(kind: DeliverySampleKind) -> &'static str {
    match kind {
        DeliverySampleKind::Ping => "ping",
        DeliverySampleKind::Block { .. } if kind.is_round_trip() => "block",
        DeliverySampleKind::Block { .. } => "large_block",
    }
}

#[derive(Serialize)]
#[serde(tag = "event", rename = "delivery_sample")]
struct DeliverySampleEvent {
    connection_id: u64,
    kind: &'static str,
    sample_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    wire_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score_before_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    score_ms: Option<u64>,
    weight: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slowdown: Option<f64>,
}

impl JsonlTraceEvent for DeliverySampleEvent {
    const TABLE: JsonlTraceTable = TABLE;
}

#[derive(Serialize)]
#[serde(tag = "event", rename = "block_missed")]
struct BlockMissedEvent {
    connection_id: u64,
    request: &'static str,
}

impl JsonlTraceEvent for BlockMissedEvent {
    const TABLE: JsonlTraceTable = TABLE;
}

/// Writes one legacy connection's delivery samples. The default value writes nothing.
#[derive(Clone, Debug, Default)]
pub(crate) struct DeliveryTrace {
    emitter: JsonlEventEmitter,
    connection_id: u64,
}

impl DeliveryTrace {
    /// Writes the samples of the connection numbered `connection_id` with `emitter`.
    pub(crate) fn new(emitter: JsonlEventEmitter, connection_id: u64) -> Self {
        Self {
            emitter,
            connection_id,
        }
    }

    /// Records a `kind` sample of `sample`, with the peer's estimate `before` it and `after`
    /// it. A sample that is not a round trip has the same score on both sides, and its row
    /// says what `before` expected for a block of its size.
    pub(super) fn sample(
        &self,
        kind: DeliverySampleKind,
        sample: Duration,
        before: &DeliveryEstimate,
        after: &DeliveryEstimate,
    ) {
        // Two decimals are enough to compare peers and to see how many samples a score rests on.
        let two_decimals = |value: f64| (value * 100.0).round() / 100.0;
        let large_block = !kind.is_round_trip();
        self.emitter.emit_event(|| DeliverySampleEvent {
            connection_id: self.connection_id,
            kind: label(kind),
            sample_ms: saturating_millis(sample),
            wire_bytes: kind.wire_bytes(),
            score_before_ms: before.score().map(saturating_millis),
            score_ms: after.score().map(saturating_millis),
            weight: two_decimals(after.weight()),
            expected_ms: large_block
                .then(|| before.expected(kind.wire_bytes()))
                .flatten()
                .map(saturating_millis),
            slowdown: large_block
                .then(|| after.large_block_slowdown())
                .flatten()
                .map(two_decimals),
        });
    }

    /// Records that the peer announced a block and did not deliver it to a first request, or to
    /// a second request when `second_request` is set.
    pub(crate) fn block_missed(&self, second_request: bool) {
        self.emitter.emit_event(|| BlockMissedEvent {
            connection_id: self.connection_id,
            request: if second_request { "second" } else { "first" },
        });
    }
}
