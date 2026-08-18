//! Hash-correlated diagnostics for near-tip block propagation.

use std::time::Duration;

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    saturating_millis, JsonlDisplay, JsonlEventEmitter, JsonlTraceEvent, JsonlTraceTable,
    JsonlTracer,
};

use super::{zakura_trace_peer_label, BlockApplyResult, ZakuraPeerId};
use crate::{Config, PeerSocketAddr, PeerSource};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("block_propagation", "block_propagation.jsonl");

/// Non-blocking block propagation event emitter.
#[derive(Clone, Debug)]
pub struct BlockPropagationTrace {
    emitter: JsonlEventEmitter,
    expose_peer_addresses: bool,
    emit_native_events: bool,
}

impl BlockPropagationTrace {
    /// Create a propagation trace from the network tracing configuration.
    ///
    /// The dedicated directory takes the narrow native event path. The general
    /// directory keeps the previous behavior, where native events come from the
    /// broader header, block, and commit trace tables.
    pub fn from_config(config: &Config) -> Self {
        let emit_native_events = config.zakura.block_propagation_trace_dir.is_some();
        let trace_dir = config
            .zakura
            .block_propagation_trace_dir
            .clone()
            .or_else(|| config.zakura.trace_dir.clone());
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);
        Self {
            emitter: JsonlEventEmitter::new(tracer, zakura_jsonl_trace::node_id()),
            expose_peer_addresses: config.expose_peer_addresses,
            emit_native_events,
        }
    }

    /// Create a disabled propagation trace.
    pub fn noop() -> Self {
        Self {
            emitter: JsonlEventEmitter::noop(),
            expose_peer_addresses: false,
            emit_native_events: false,
        }
    }

    /// Record the point where a locally submitted block starts network broadcast.
    pub fn mined_block_broadcast_started(&self, hash: block::Hash, height: block::Height) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::MinedBlockBroadcastStarted {
                hash: JsonlDisplay(&hash),
                height: height.0,
            });
    }

    /// Record completion of the local peer-set broadcast request.
    pub fn mined_block_broadcast_finished(
        &self,
        hash: block::Hash,
        height: block::Height,
        result: &'static str,
        elapsed: Duration,
    ) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::MinedBlockBroadcastFinished {
                hash: JsonlDisplay(&hash),
                height: height.0,
                result,
                elapsed_ms: saturating_millis(elapsed),
            });
    }

    /// Record a legacy-compatible block advertisement and its local admission result.
    pub fn block_announced(
        &self,
        hash: block::Hash,
        source: Option<&PeerSource>,
        transport: &'static str,
        disposition: &'static str,
    ) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::BlockAnnounced {
                hash: JsonlDisplay(&hash),
                transport,
                source: source.map(|source| self.source_label(source)),
                disposition,
            });
    }

    /// Record a complete legacy block body returned by the network service.
    pub fn legacy_block_downloaded(
        &self,
        hash: block::Hash,
        height: block::Height,
        peer: Option<PeerSocketAddr>,
        elapsed: Duration,
    ) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::LegacyBlockDownloaded {
                hash: JsonlDisplay(&hash),
                height: height.0,
                source: peer.map(|peer| self.legacy_peer_label(peer)),
                elapsed_ms: saturating_millis(elapsed),
            });
    }

    /// Record the terminal result of legacy gossip download and verification.
    pub fn legacy_block_finished(
        &self,
        hash: block::Hash,
        height: Option<block::Height>,
        result: &'static str,
        reason: Option<&str>,
        elapsed: Duration,
    ) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::LegacyBlockFinished {
                hash: JsonlDisplay(&hash),
                height: height.map(|height| height.0),
                result,
                reason: reason.map(bounded_reason),
                elapsed_ms: saturating_millis(elapsed),
            });
    }

    /// Record a valid native status that announces a selected tip.
    pub fn native_header_status_received(
        &self,
        peer: &ZakuraPeerId,
        hash: block::Hash,
        height: block::Height,
    ) {
        if !self.emit_native_events {
            return;
        }
        self.emitter
            .emit_event(|| BlockPropagationEvent::HeaderStatusReceived {
                peer: zakura_trace_peer_label(peer),
                selected_tip_hash: JsonlDisplay(&hash),
                selected_tip_height: height.0,
            });
    }

    /// Record a complete native block body.
    pub fn native_block_body_received(
        &self,
        peer: &ZakuraPeerId,
        hash: block::Hash,
        height: block::Height,
    ) {
        if !self.emit_native_events {
            return;
        }
        self.emitter
            .emit_event(|| BlockPropagationEvent::BlockBodyReceived {
                peer: zakura_trace_peer_label(peer),
                hash: JsonlDisplay(&hash),
                height: height.0,
            });
    }

    /// Record the terminal result of a native block commit.
    pub fn native_commit_finished(
        &self,
        hash: block::Hash,
        height: block::Height,
        result: BlockApplyResult,
    ) {
        if !self.emit_native_events {
            return;
        }
        self.emitter
            .emit_event(|| BlockPropagationEvent::CommitFinish {
                hash: JsonlDisplay(&hash),
                height: height.0,
                result: block_apply_result_label(result),
            });
    }

    fn source_label(&self, source: &PeerSource) -> String {
        match source {
            PeerSource::LegacySocket(peer) => self.legacy_peer_label(*peer),
            PeerSource::Zakura(peer) => {
                format!("native:{}", zakura_trace_peer_label(peer))
            }
        }
    }

    fn legacy_peer_label(&self, peer: PeerSocketAddr) -> String {
        if self.expose_peer_addresses {
            format!("legacy:{}", peer.remove_socket_addr_privacy())
        } else {
            "legacy:redacted".to_string()
        }
    }
}

impl Default for BlockPropagationTrace {
    fn default() -> Self {
        Self::noop()
    }
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum BlockPropagationEvent<'a> {
    MinedBlockBroadcastStarted {
        hash: JsonlDisplay<'a, block::Hash>,
        height: u32,
    },
    MinedBlockBroadcastFinished {
        hash: JsonlDisplay<'a, block::Hash>,
        height: u32,
        result: &'static str,
        elapsed_ms: u64,
    },
    BlockAnnounced {
        hash: JsonlDisplay<'a, block::Hash>,
        transport: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        disposition: &'static str,
    },
    LegacyBlockDownloaded {
        hash: JsonlDisplay<'a, block::Hash>,
        height: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        elapsed_ms: u64,
    },
    LegacyBlockFinished {
        hash: JsonlDisplay<'a, block::Hash>,
        #[serde(skip_serializing_if = "Option::is_none")]
        height: Option<u32>,
        result: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        elapsed_ms: u64,
    },
    HeaderStatusReceived {
        peer: String,
        selected_tip_hash: JsonlDisplay<'a, block::Hash>,
        selected_tip_height: u32,
    },
    BlockBodyReceived {
        peer: String,
        hash: JsonlDisplay<'a, block::Hash>,
        height: u32,
    },
    CommitFinish {
        hash: JsonlDisplay<'a, block::Hash>,
        height: u32,
        result: &'static str,
    },
}

impl JsonlTraceEvent for BlockPropagationEvent<'_> {
    const TABLE: JsonlTraceTable = TABLE;
}

fn block_apply_result_label(result: BlockApplyResult) -> &'static str {
    match result {
        BlockApplyResult::Committed => "committed",
        BlockApplyResult::Duplicate => "duplicate",
        BlockApplyResult::Rejected => "rejected",
        BlockApplyResult::Unavailable => "unavailable",
        BlockApplyResult::TimedOut => "timed_out",
    }
}

fn bounded_reason(reason: &str) -> String {
    const MAX_REASON_CHARS: usize = 256;
    reason.chars().take(MAX_REASON_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propagation_events_have_hash_correlated_schema() {
        let hash = block::Hash([0x2a; 32]);
        let event = BlockPropagationEvent::BlockAnnounced {
            hash: JsonlDisplay(&hash),
            transport: "legacy",
            source: Some("legacy:peer:1234".to_string()),
            disposition: "queued",
        };

        let value = serde_json::to_value(event).expect("body event serializes");
        assert_eq!(value["event"], "block_announced");
        assert_eq!(value["hash"], hash.to_string());
        assert_eq!(value["transport"], "legacy");
        assert_eq!(value["source"], "legacy:peer:1234");
        assert_eq!(value["disposition"], "queued");
    }

    #[test]
    fn native_events_keep_report_compatible_names() {
        let hash = block::Hash([0x2a; 32]);
        let event = BlockPropagationEvent::CommitFinish {
            hash: JsonlDisplay(&hash),
            height: 42,
            result: block_apply_result_label(BlockApplyResult::Committed),
        };

        let value = serde_json::to_value(event).expect("commit event serializes");
        assert_eq!(value["event"], "commit_finish");
        assert_eq!(value["hash"], hash.to_string());
        assert_eq!(value["height"], 42);
        assert_eq!(value["result"], "committed");
    }

    #[tokio::test]
    async fn dedicated_native_commit_writes_only_propagation_table() {
        let directory = tempfile::tempdir().expect("temporary trace directory is created");
        let guard = JsonlTracer::spawn_guard(directory.path().to_path_buf());
        let trace = BlockPropagationTrace {
            emitter: JsonlEventEmitter::new(guard.tracer(), "observer"),
            expose_peer_addresses: false,
            emit_native_events: true,
        };
        let hash = block::Hash([0x2a; 32]);

        trace.native_commit_finished(hash, block::Height(42), BlockApplyResult::Committed);
        guard.shutdown().await;

        let files: Vec<_> = std::fs::read_dir(directory.path())
            .expect("trace directory is readable")
            .map(|entry| {
                entry
                    .expect("trace file entry is readable")
                    .file_name()
                    .into_string()
                    .expect("trace file name is UTF-8")
            })
            .collect();
        assert_eq!(files, ["block_propagation.jsonl"]);
        let row: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(directory.path().join("block_propagation.jsonl"))
                .expect("propagation trace is readable"),
        )
        .expect("propagation row is valid JSON");
        assert_eq!(row["event"], "commit_finish");
        assert_eq!(row["hash"], hash.to_string());
    }

    #[test]
    fn legacy_peer_labels_follow_address_privacy_policy() {
        let peer = PeerSocketAddr::from(([203, 0, 113, 7], 18233));
        let private_trace = BlockPropagationTrace::noop();
        let public_trace = BlockPropagationTrace {
            emitter: JsonlEventEmitter::noop(),
            expose_peer_addresses: true,
            emit_native_events: false,
        };

        assert_eq!(private_trace.legacy_peer_label(peer), "legacy:redacted");
        assert_eq!(
            public_trace.legacy_peer_label(peer),
            "legacy:203.0.113.7:18233"
        );
    }

    #[test]
    fn failure_reasons_are_bounded() {
        let reason = "x".repeat(300);
        assert_eq!(bounded_reason(&reason).len(), 256);
    }
}
