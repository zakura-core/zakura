//! Download and verification timing for individual blocks.
//!
//! Emits one row per block verification attempt to the `block_verify_event`
//! table, recording where the block came from, how long it spent downloading
//! and verifying, and whether it verified.
//!
//! Propagation traces say when a block reached a node's socket. They cannot say
//! when that node was able to *build on* it, which is what determines whether a
//! competing block becomes stale. The gap between the two is the download and
//! verification time recorded here.

use std::{path::PathBuf, time::Duration};

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{JsonlDisplay, JsonlEventEmitter, JsonlTraceTable, JsonlTracer};

/// Per-block download and verification timing.
///
/// Rows are flat and every column is populated on the success path, so this
/// table is written as CSV.
pub const BLOCK_VERIFY_TABLE: JsonlTraceTable = JsonlTraceTable::csv(
    "block_verify_event",
    "block_verify_event.csv",
    &[
        "event",
        "source",
        "height",
        "hash",
        "download_ms",
        "verify_ms",
        "total_ms",
        "result",
        "error_class",
    ],
);

/// Where a block came from.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockSource {
    /// Downloaded during chain sync.
    Sync,
    /// Received by gossip from a peer.
    Gossip,
}

impl BlockSource {
    fn label(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Gossip => "gossip",
        }
    }
}

#[derive(Serialize)]
struct BlockVerifyEvent<'a> {
    event: &'static str,
    source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    hash: JsonlDisplay<'a, block::Hash>,
    download_ms: u64,
    verify_ms: u64,
    total_ms: u64,
    result: &'static str,
    /// The verification error, when this attempt failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    error_class: Option<String>,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(BlockVerifyEvent<'_>, BLOCK_VERIFY_TABLE);

/// A handle for recording block verification timing. Cloning is cheap.
#[derive(Clone, Debug, Default)]
pub struct BlockVerifyTrace {
    emitter: Option<JsonlEventEmitter>,
}

impl BlockVerifyTrace {
    /// Create a tracer writing to `trace_dir`, or a no-op tracer if it is `None`.
    pub fn new(trace_dir: Option<PathBuf>) -> Self {
        let Some(trace_dir) = trace_dir else {
            return Self::noop();
        };

        let tracer = JsonlTracer::spawn(trace_dir);
        if !tracer.is_enabled() {
            return Self::noop();
        }

        Self {
            emitter: Some(JsonlEventEmitter::new(
                tracer,
                zakura_jsonl_trace::node_id(),
            )),
        }
    }

    /// Create a tracer from an existing emitter.
    pub fn from_emitter(emitter: JsonlEventEmitter) -> Self {
        Self {
            emitter: emitter.is_enabled().then_some(emitter),
        }
    }

    /// Create a no-op tracer. Every record call returns immediately.
    pub fn noop() -> Self {
        Self { emitter: None }
    }

    /// Returns true when this tracer emits rows.
    pub fn is_enabled(&self) -> bool {
        self.emitter.is_some()
    }

    /// Record one verification attempt.
    ///
    /// `error` is `None` on success.
    pub fn record(
        &self,
        source: BlockSource,
        height: Option<block::Height>,
        hash: block::Hash,
        download: Duration,
        verify: Duration,
        error: Option<&dyn std::fmt::Display>,
    ) {
        let Some(emitter) = &self.emitter else {
            return;
        };

        emitter.emit_event(|| BlockVerifyEvent {
            event: "block_verify",
            source: source.label(),
            height: height.map(|height| height.0),
            hash: JsonlDisplay(&hash),
            download_ms: zakura_jsonl_trace::saturating_millis(download),
            verify_ms: zakura_jsonl_trace::saturating_millis(verify),
            total_ms: zakura_jsonl_trace::saturating_millis(download.saturating_add(verify)),
            result: if error.is_some() {
                "failure"
            } else {
                "success"
            },
            error_class: error.map(|error| error.to_string()),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split_csv(line: &str) -> Vec<String> {
        let mut fields = vec![String::new()];
        let mut quoted = false;
        let mut characters = line.chars().peekable();

        while let Some(character) = characters.next() {
            match character {
                '"' if quoted && characters.peek() == Some(&'"') => {
                    characters.next();
                    fields.last_mut().expect("a field is open").push('"');
                }
                '"' => quoted = !quoted,
                ',' if !quoted => fields.push(String::new()),
                character => fields.last_mut().expect("a field is open").push(character),
            }
        }

        fields
    }

    fn field<'a>(header: &[String], row: &'a [String], name: &str) -> &'a str {
        let index = header
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("table has a {name} column: {header:?}"));
        &row[index]
    }

    #[tokio::test]
    async fn records_the_columns_the_stale_rate_model_reads() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace =
            BlockVerifyTrace::from_emitter(JsonlEventEmitter::new(guard.tracer(), "node-1"));

        let hash = block::Hash([7; 32]);
        trace.record(
            BlockSource::Gossip,
            Some(block::Height(42)),
            hash,
            Duration::from_millis(120),
            Duration::from_millis(80),
            None,
        );
        trace.record(
            BlockSource::Sync,
            None,
            hash,
            Duration::from_millis(5),
            Duration::from_millis(1),
            Some(&"invalid block"),
        );

        drop(trace);
        guard.shutdown().await;

        let contents = std::fs::read_to_string(dir.path().join(BLOCK_VERIFY_TABLE.file_name()))
            .expect("trace table is written");
        let mut lines = contents.lines();
        let header = split_csv(lines.next().expect("table has a header"));
        let rows: Vec<_> = lines.map(split_csv).collect();
        assert_eq!(rows.len(), 2);

        // The analyzer filters on `event = 'block_verify' AND result = 'success'`
        // and groups by hash, so these four are load-bearing.
        assert_eq!(field(&header, &rows[0], "event"), "block_verify");
        assert_eq!(field(&header, &rows[0], "result"), "success");
        assert_eq!(field(&header, &rows[0], "hash"), hash.to_string());
        assert_eq!(field(&header, &rows[0], "source"), "gossip");

        assert_eq!(field(&header, &rows[0], "height"), "42");
        assert_eq!(field(&header, &rows[0], "download_ms"), "120");
        assert_eq!(field(&header, &rows[0], "verify_ms"), "80");
        assert_eq!(
            field(&header, &rows[0], "total_ms"),
            "200",
            "total covers download plus verify, which is the delay before this \
             node can build on the block"
        );
        assert!(field(&header, &rows[0], "error_class").is_empty());

        assert_eq!(field(&header, &rows[1], "result"), "failure");
        assert_eq!(field(&header, &rows[1], "source"), "sync");
        assert_eq!(field(&header, &rows[1], "error_class"), "invalid block");
        assert!(field(&header, &rows[1], "height").is_empty());
    }

    #[test]
    fn a_disabled_tracer_records_nothing() {
        let trace = BlockVerifyTrace::noop();
        assert!(!trace.is_enabled());

        trace.record(
            BlockSource::Gossip,
            None,
            block::Hash([0; 32]),
            Duration::ZERO,
            Duration::ZERO,
            None,
        );
    }
}
