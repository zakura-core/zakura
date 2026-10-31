//! `block_verify` rows: download, verification, and commit timing for gossiped and synced blocks.
//!
//! Propagation rows say when a block reached this node's socket. A miner can build on the block
//! only after it is committed, so this table records the rest: how long the download took, how
//! long the block waited before verification started, and how long verification and the state
//! commit took. Each row is written as soon as the verifier returns, so its `ts` is the commit
//! completion time.
//!
//! `parent` classifies the block's parent against the best chain tip when the block arrived:
//! `tip` when the parent was the tip, `above_tip` when the block's height was beyond the tip's
//! child (so its parent was not yet committed to the best chain, and verification had to wait for
//! it), and `not_tip` otherwise.

use std::{fmt, time::Duration};

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    impl_jsonl_trace_event, saturating_micros, tables, JsonlDisplay, JsonlEventEmitter, JsonlTracer,
};

/// Where a verified block came from.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockSource {
    /// Advertised by a peer and downloaded by the inbound gossip downloader.
    Gossip,
    /// Downloaded by the syncer.
    Sync,
}

impl BlockSource {
    fn label(self) -> &'static str {
        match self {
            BlockSource::Gossip => "gossip",
            BlockSource::Sync => "sync",
        }
    }
}

/// The block's parent relative to the best chain tip when the block arrived.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParentAtArrival {
    /// The parent was the best chain tip.
    Tip,
    /// The block was more than one block above the tip, so its parent was not on the best chain.
    AboveTip,
    /// The parent was not the tip, and the block was not above the tip's child.
    NotTip,
    /// The node had no chain tip.
    Unknown,
}

impl ParentAtArrival {
    /// Classifies a block at `height` with `parent` against the best chain tip `best_tip`.
    pub fn classify(
        height: block::Height,
        parent: block::Hash,
        best_tip: Option<(block::Height, block::Hash)>,
    ) -> Self {
        match best_tip {
            None => ParentAtArrival::Unknown,
            Some((_, tip_hash)) if tip_hash == parent => ParentAtArrival::Tip,
            Some((tip_height, _)) if height.0 > tip_height.0.saturating_add(1) => {
                ParentAtArrival::AboveTip
            }
            Some(_) => ParentAtArrival::NotTip,
        }
    }

    fn label(self) -> Option<&'static str> {
        match self {
            ParentAtArrival::Tip => Some("tip"),
            ParentAtArrival::AboveTip => Some("above_tip"),
            ParentAtArrival::NotTip => Some("not_tip"),
            ParentAtArrival::Unknown => None,
        }
    }
}

/// One completed verification attempt.
pub struct BlockVerification<'a> {
    /// Where the block came from.
    pub source: BlockSource,
    /// The block hash.
    pub hash: block::Hash,
    /// The block's coinbase height.
    pub height: block::Height,
    /// The block's parent hash.
    pub parent_hash: block::Hash,
    /// The parent relative to the best chain tip when the block arrived.
    pub parent: ParentAtArrival,
    /// The best chain tip height when the block arrived.
    pub tip_height: Option<block::Height>,
    /// From the block request to the received block.
    pub download: Duration,
    /// From the received block to the verifier call.
    pub wait: Duration,
    /// From the verifier call to its result, including the state commit.
    pub verify: Duration,
    /// The verification error, or `None` when the block was committed.
    pub error: Option<&'a dyn fmt::Display>,
}

#[derive(Serialize)]
struct BlockVerifyRow<'a> {
    event: &'static str,
    source: &'static str,
    hash: JsonlDisplay<'a, block::Hash>,
    height: u32,
    parent_hash: JsonlDisplay<'a, block::Hash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tip_height: Option<u32>,
    download_us: u64,
    wait_us: u64,
    verify_us: u64,
    total_us: u64,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl_jsonl_trace_event!(BlockVerifyRow<'_>, tables::BLOCK_VERIFY);

/// A cloneable handle for recording block verification timing.
#[derive(Clone, Debug, Default)]
pub struct BlockVerifyTrace {
    emitter: Option<JsonlEventEmitter>,
}

impl BlockVerifyTrace {
    /// Creates a trace that writes the `block_verify` table when `tracer` selects it.
    pub fn new(tracer: JsonlTracer) -> Self {
        Self {
            emitter: tracer
                .is_table_enabled(tables::BLOCK_VERIFY.table())
                .then(|| JsonlEventEmitter::new(tracer, zakura_jsonl_trace::node_id())),
        }
    }

    /// Creates a trace that writes nothing.
    pub fn noop() -> Self {
        Self::default()
    }

    /// Returns `true` if this trace writes rows, so callers can skip timing work otherwise.
    pub fn is_enabled(&self) -> bool {
        self.emitter.is_some()
    }

    /// Records a completed verification attempt.
    pub fn record(&self, verification: BlockVerification<'_>) {
        let Some(emitter) = &self.emitter else {
            return;
        };

        emitter.emit_event(|| BlockVerifyRow {
            event: "block_verify",
            source: verification.source.label(),
            hash: JsonlDisplay(&verification.hash),
            height: verification.height.0,
            parent_hash: JsonlDisplay(&verification.parent_hash),
            parent: verification.parent.label(),
            tip_height: verification.tip_height.map(|height| height.0),
            download_us: saturating_micros(verification.download),
            wait_us: saturating_micros(verification.wait),
            verify_us: saturating_micros(verification.verify),
            total_us: saturating_micros(
                verification
                    .download
                    .saturating_add(verification.wait)
                    .saturating_add(verification.verify),
            ),
            result: if verification.error.is_some() {
                "failure"
            } else {
                "success"
            },
            error: verification.error.map(ToString::to_string),
        });
    }
}

#[cfg(test)]
mod tests {
    use zakura_jsonl_trace::JsonlTraceTables;
    use zakura_network::zakura::testkit::{TraceReader, TraceValue};

    use super::*;

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    #[test]
    fn parent_is_classified_against_the_tip_at_arrival() {
        let tip = Some((block::Height(10), hash(10)));
        let classify =
            |height, parent| ParentAtArrival::classify(block::Height(height), parent, tip);

        assert_eq!(classify(11, hash(10)), ParentAtArrival::Tip);
        assert_eq!(classify(12, hash(11)), ParentAtArrival::AboveTip);
        assert_eq!(classify(11, hash(9)), ParentAtArrival::NotTip);
        assert_eq!(
            ParentAtArrival::classify(block::Height(1), hash(0), None),
            ParentAtArrival::Unknown
        );
    }

    #[tokio::test]
    async fn rows_record_source_parent_and_each_interval() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = BlockVerifyTrace::new(guard.tracer());
        assert!(trace.is_enabled());

        trace.record(BlockVerification {
            source: BlockSource::Gossip,
            hash: hash(2),
            height: block::Height(12),
            parent_hash: hash(1),
            parent: ParentAtArrival::AboveTip,
            tip_height: Some(block::Height(10)),
            download: Duration::from_millis(120),
            wait: Duration::from_millis(5),
            verify: Duration::from_millis(80),
            error: None,
        });
        trace.record(BlockVerification {
            source: BlockSource::Sync,
            hash: hash(3),
            height: block::Height(13),
            parent_hash: hash(2),
            parent: ParentAtArrival::Unknown,
            tip_height: None,
            download: Duration::from_millis(1),
            wait: Duration::ZERO,
            verify: Duration::from_millis(2),
            error: Some(&"invalid block"),
        });
        drop(trace);
        guard.shutdown().await;

        let reader = TraceReader::load(dir.path()).expect("CSV traces load");
        let rows = reader.table("block_verify");
        rows.assert_row(
            "block_verify",
            &[
                ("source", TraceValue::Str("gossip")),
                ("hash", TraceValue::Str(&hash(2).to_string())),
                ("height", TraceValue::U64(12)),
                ("parent_hash", TraceValue::Str(&hash(1).to_string())),
                ("parent", TraceValue::Str("above_tip")),
                ("tip_height", TraceValue::U64(10)),
                ("download_us", TraceValue::U64(120_000)),
                ("wait_us", TraceValue::U64(5_000)),
                ("verify_us", TraceValue::U64(80_000)),
                ("total_us", TraceValue::U64(205_000)),
                ("result", TraceValue::Str("success")),
            ],
        );
        let failure = rows.last().expect("two rows");
        assert_eq!(failure["source"], "sync");
        assert_eq!(failure["result"], "failure");
        assert_eq!(failure["error"], "invalid block");
        assert!(failure["parent"].is_null());
    }

    #[test]
    fn unselected_table_disables_the_trace() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let tracer = JsonlTracer::new(tx)
            .with_tables(JsonlTraceTables::from_names(["mining"]).expect("known table"));
        let trace = BlockVerifyTrace::new(tracer);
        assert!(!trace.is_enabled());
        trace.record(BlockVerification {
            source: BlockSource::Gossip,
            hash: hash(1),
            height: block::Height(1),
            parent_hash: hash(0),
            parent: ParentAtArrival::Tip,
            tip_height: Some(block::Height(0)),
            download: Duration::ZERO,
            wait: Duration::ZERO,
            verify: Duration::ZERO,
            error: None,
        });
        assert!(rx.try_recv().is_err());
    }
}
