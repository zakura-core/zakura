//! A trace row for every sample that changes a legacy connection's delivery score.
//!
//! Each `delivery_sample` row says what was measured (`kind`), the measured time (`sample_ms`),
//! the score before and after (`score_before_ms`, `score_ms`), and `weight`, the decayed number of
//! samples the new score rests on. Together they show why a peer's score is what it is: how far a
//! missed block moved it, how quickly it recovered, and how much of it comes from pings and
//! transaction replies rather than blocks. `connection_id` is the number the connection's
//! `legacy_gossip` and `legacy_peer_request` rows carry, so the tables join.

use std::time::Duration;

use serde::Serialize;
use zakura_jsonl_trace::{saturating_millis, JsonlEventEmitter, JsonlTraceEvent, JsonlTraceTable};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("delivery_score", "delivery_score.jsonl");

/// What a delivery sample measured.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliverySampleKind {
    /// A heartbeat ping's round trip.
    Ping,
    /// A reply to a transaction request that held at least one transaction.
    Transaction,
    /// A reply to a block request whose blocks were all small.
    Block,
    /// A first block request to a peer that announced the block, which it did not deliver.
    MissedBlock,
    /// A second block request to a peer that announced the block, which it did not deliver.
    MissedSecondBlock,
}

impl DeliverySampleKind {
    fn label(self) -> &'static str {
        match self {
            Self::Ping => "ping",
            Self::Transaction => "transaction",
            Self::Block => "block",
            Self::MissedBlock => "missed_block",
            Self::MissedSecondBlock => "missed_second_block",
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "event", rename = "delivery_sample")]
struct DeliverySampleEvent {
    connection_id: u64,
    kind: &'static str,
    sample_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    score_before_ms: Option<u64>,
    score_ms: u64,
    weight: f64,
}

impl JsonlTraceEvent for DeliverySampleEvent {
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

    /// Records that a `kind` sample of `sample` moved the score from `score_before` to `score`,
    /// which now rests on `weight` decayed samples.
    pub(super) fn sample(
        &self,
        kind: DeliverySampleKind,
        sample: Duration,
        score_before: Option<Duration>,
        score: Duration,
        weight: f64,
    ) {
        self.emitter.emit_event(|| DeliverySampleEvent {
            connection_id: self.connection_id,
            kind: kind.label(),
            sample_ms: saturating_millis(sample),
            score_before_ms: score_before.map(saturating_millis),
            score_ms: saturating_millis(score),
            // Two decimals are enough to see how many recent samples a score rests on.
            weight: (weight * 100.0).round() / 100.0,
        });
    }
}
