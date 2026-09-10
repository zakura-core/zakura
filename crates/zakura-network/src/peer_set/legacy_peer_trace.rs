//! Structured attribution for legacy peer discovery and block download requests.
//!
//! Legacy sync learns ordered hashes from `FindBlocks`, then distributes individual block
//! downloads across the peer set. A download routed to `maybe` has no exact inventory claim from
//! that peer; `advertised` means the selected peer recently sent an `inv` for that exact hash.
//! `peer_start_height` is the peer's untrusted, point-in-time handshake claim and is not used as an
//! inventory guarantee. Process-local `peer_id` values correlate events without recording peer IPs.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use serde::Serialize;
#[cfg(test)]
use serde_json::Value;
use zakura_chain::block::{self, Height};
use zakura_jsonl_trace::{
    saturating_count, saturating_millis, JsonlDisplay, JsonlEventEmitter, JsonlTraceEvent,
    JsonlTraceTable, JsonlTracer,
};

use crate::{protocol::internal::Response, NotFoundClass, PeerSocketAddr, SharedPeerError};

const TABLE: JsonlTraceTable =
    JsonlTraceTable::new("legacy_peer_request", "legacy_peer_request.jsonl");

#[derive(Clone, Debug)]
pub(super) struct LegacyPeerTrace {
    emitter: JsonlEventEmitter,
    next_request_id: Arc<AtomicU64>,
}

#[derive(Copy, Clone, Debug)]
pub(super) struct PeerTraceContext {
    pub(super) peer_id: u64,
    pub(super) peer: PeerSocketAddr,
    pub(super) peer_start_height: Height,
    pub(super) local_tip_height: Option<Height>,
}

#[derive(Serialize)]
#[serde(tag = "event")]
enum LegacyPeerEvent<'a> {
    #[serde(rename = "find_blocks_finish")]
    FindBlocksFinish {
        request_id: u64,
        peer_id: u64,
        peer: JsonlDisplay<'a, PeerSocketAddr>,
        peer_start_height: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        local_tip_height: Option<u32>,
        elapsed_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        locator_tip: Option<JsonlDisplay<'a, block::Hash>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stop: Option<JsonlDisplay<'a, block::Hash>>,
        #[serde(flatten)]
        result: FindBlocksResult<'a>,
    },
    #[serde(rename = "block_request_finish")]
    BlockRequestFinish {
        request_id: u64,
        peer_id: u64,
        peer: JsonlDisplay<'a, PeerSocketAddr>,
        peer_start_height: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        local_tip_height: Option<u32>,
        elapsed_ms: u64,
        requested_hash: JsonlDisplay<'a, block::Hash>,
        route: &'static str,
        #[serde(flatten)]
        result: BlockRequestResult<'a>,
    },
}

impl JsonlTraceEvent for LegacyPeerEvent<'_> {
    const TABLE: JsonlTraceTable = TABLE;
}

#[derive(Serialize)]
#[serde(untagged)]
enum FindBlocksResult<'a> {
    BlockHashes {
        result: &'static str,
        hash_count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        inferred_start_height: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        inferred_end_height: Option<u32>,
    },
    UnexpectedResponse {
        result: &'static str,
        response: &'static str,
    },
    Error {
        result: &'static str,
        error: JsonlDisplay<'a, SharedPeerError>,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
enum BlockRequestResult<'a> {
    Available {
        result: &'static str,
        returned_hash: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        returned_height: Option<u32>,
    },
    Missing {
        result: &'static str,
        missing_hash: String,
    },
    Empty {
        result: &'static str,
    },
    UnexpectedResponse {
        result: &'static str,
        response: &'static str,
    },
    Error {
        result: &'static str,
        error: JsonlDisplay<'a, SharedPeerError>,
    },
}

impl LegacyPeerTrace {
    pub(super) fn new(trace_dir: Option<PathBuf>) -> Self {
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);

        Self {
            emitter: JsonlEventEmitter::new(tracer, zakura_jsonl_trace::node_id()),
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(super) fn next_request_id(&self) -> u64 {
        // Wrap-around is unreachable for a process-local u64 counter.
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn find_blocks_finish(
        &self,
        request_id: u64,
        peer: PeerTraceContext,
        locator_tip: Option<block::Hash>,
        stop: Option<block::Hash>,
        elapsed: Duration,
        result: &Result<Response, SharedPeerError>,
    ) {
        self.emitter.emit_event(|| {
            let result = match result {
                Ok(Response::BlockHashes(hashes)) => {
                    let (inferred_start_height, inferred_end_height) =
                        inferred_height_range(peer.local_tip_height, hashes.len());
                    FindBlocksResult::BlockHashes {
                        result: "block_hashes",
                        hash_count: saturating_count(hashes.len()),
                        inferred_start_height,
                        inferred_end_height,
                    }
                }
                Ok(response) => FindBlocksResult::UnexpectedResponse {
                    result: "unexpected_response",
                    response: response.command(),
                },
                Err(error) => FindBlocksResult::Error {
                    result: error_result_label(error),
                    error: JsonlDisplay(error),
                },
            };

            LegacyPeerEvent::FindBlocksFinish {
                request_id,
                peer_id: peer.peer_id,
                peer: JsonlDisplay(&peer.peer),
                peer_start_height: peer.peer_start_height.0,
                local_tip_height: peer.local_tip_height.map(|height| height.0),
                elapsed_ms: saturating_millis(elapsed),
                locator_tip: locator_tip.as_ref().map(JsonlDisplay),
                stop: stop.as_ref().map(JsonlDisplay),
                result,
            }
        });
    }

    pub(super) fn block_request_finish(
        &self,
        request_id: u64,
        peer: PeerTraceContext,
        requested_hash: block::Hash,
        route: &'static str,
        elapsed: Duration,
        result: &Result<Response, SharedPeerError>,
    ) {
        self.emitter.emit_event(|| {
            let result = match result {
                Ok(Response::Blocks(blocks)) => {
                    let available = blocks.iter().find_map(|status| status.available());
                    let missing = blocks.iter().find_map(|status| status.missing());

                    if let Some((block, _source)) = available {
                        let returned_hash = block.hash();
                        BlockRequestResult::Available {
                            result: if returned_hash == requested_hash {
                                "available"
                            } else {
                                "mismatched_block"
                            },
                            returned_hash: returned_hash.to_string(),
                            returned_height: block.coinbase_height().map(|height| height.0),
                        }
                    } else if let Some(missing_hash) = missing {
                        BlockRequestResult::Missing {
                            result: "missing",
                            missing_hash: missing_hash.to_string(),
                        }
                    } else {
                        BlockRequestResult::Empty {
                            result: "empty_blocks",
                        }
                    }
                }
                Ok(response) => BlockRequestResult::UnexpectedResponse {
                    result: "unexpected_response",
                    response: response.command(),
                },
                Err(error) => BlockRequestResult::Error {
                    result: error_result_label(error),
                    error: JsonlDisplay(error),
                },
            };

            LegacyPeerEvent::BlockRequestFinish {
                request_id,
                peer_id: peer.peer_id,
                peer: JsonlDisplay(&peer.peer),
                peer_start_height: peer.peer_start_height.0,
                local_tip_height: peer.local_tip_height.map(|height| height.0),
                elapsed_ms: saturating_millis(elapsed),
                requested_hash: JsonlDisplay(&requested_hash),
                route,
                result,
            }
        });
    }
}

fn error_result_label(error: &SharedPeerError) -> &'static str {
    match error.not_found_class() {
        Some(NotFoundClass::Response) => "notfound_response",
        Some(NotFoundClass::Registry) => "notfound_registry",
        Some(NotFoundClass::Other) | None => "error",
    }
}

/// Return the response range assuming the peer matched the first locator hash.
///
/// The legacy protocol does not identify which locator hash matched, so these heights are not an
/// exact or authenticated claim about the peer's response.
fn inferred_height_range(
    local_tip_height: Option<Height>,
    hash_count: usize,
) -> (Option<u32>, Option<u32>) {
    if hash_count == 0 {
        return (None, None);
    }

    let Some(local_tip_height) = local_tip_height else {
        return (None, None);
    };
    let hash_count = u32::try_from(hash_count).unwrap_or(u32::MAX);
    (
        Some(local_tip_height.0.saturating_add(1)),
        Some(local_tip_height.0.saturating_add(hash_count)),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{protocol::external::InventoryHash, PeerError};

    const MAX_RECEIVED_BLOCK_HASHES: usize = 50_000;

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn assert_key_order(line: &str, keys: &[&str]) {
        let mut remainder = line;
        for key in keys {
            let marker = format!("\"{key}\":");
            let position = remainder
                .find(&marker)
                .unwrap_or_else(|| panic!("trace row is missing key {key}: {line}"));
            remainder = &remainder[position + marker.len()..];
        }
    }

    #[tokio::test]
    async fn writes_attributed_find_blocks_and_notfound_events() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacyPeerTrace {
            emitter: JsonlEventEmitter::new(guard.tracer(), "test-node"),
            next_request_id: Arc::new(AtomicU64::new(1)),
        };
        let peer = PeerTraceContext {
            peer_id: 7,
            peer: "127.0.0.1:8233".parse().expect("valid peer address"),
            peer_start_height: Height(100),
            local_tip_height: Some(Height(90)),
        };

        trace.find_blocks_finish(
            1,
            peer,
            Some(hash(1)),
            None,
            Duration::from_millis(12),
            &Ok(Response::BlockHashes(vec![hash(2), hash(3)])),
        );
        trace.block_request_finish(
            2,
            peer,
            hash(2),
            "advertised",
            Duration::from_millis(4),
            &Err(SharedPeerError::from(PeerError::NotFoundResponse(vec![
                InventoryHash::Block(hash(2)),
            ]))),
        );

        drop(trace);
        guard.shutdown().await;

        let events = std::fs::read_to_string(dir.path().join(TABLE.file_name()))
            .expect("legacy peer trace file is written");
        let lines: Vec<_> = events.lines().collect();
        let events: Vec<Value> = lines
            .iter()
            .map(|line| serde_json::from_str(line).expect("trace row is valid JSON"))
            .collect();

        assert_eq!(TABLE.table(), "legacy_peer_request");
        assert_eq!(TABLE.file_name(), "legacy_peer_request.jsonl");
        assert_key_order(
            lines[0],
            &[
                "ts",
                "node",
                "event",
                "request_id",
                "peer_id",
                "peer",
                "peer_start_height",
                "local_tip_height",
                "elapsed_ms",
                "locator_tip",
                "result",
                "hash_count",
                "inferred_start_height",
                "inferred_end_height",
            ],
        );
        assert_key_order(
            lines[1],
            &[
                "ts",
                "node",
                "event",
                "request_id",
                "peer_id",
                "peer",
                "peer_start_height",
                "local_tip_height",
                "elapsed_ms",
                "requested_hash",
                "route",
                "result",
                "error",
            ],
        );
        assert_eq!(events[0]["event"], "find_blocks_finish");
        assert_eq!(events[0]["peer_id"], 7);
        assert_eq!(events[0]["peer_start_height"], 100);
        assert_eq!(events[0]["hash_count"], 2);
        assert_eq!(events[0]["inferred_start_height"], 91);
        assert_eq!(events[0]["inferred_end_height"], 92);
        assert!(events[0].get("hashes").is_none());
        assert_eq!(events[1]["event"], "block_request_finish");
        assert_eq!(events[1]["route"], "advertised");
        assert_eq!(events[1]["result"], "notfound_response");
    }

    #[tokio::test]
    async fn omits_hashes_from_maximum_find_blocks_response() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacyPeerTrace {
            emitter: JsonlEventEmitter::new(guard.tracer(), "test-node"),
            next_request_id: Arc::new(AtomicU64::new(1)),
        };
        let peer = PeerTraceContext {
            peer_id: 7,
            peer: "127.0.0.1:8233".parse().expect("valid peer address"),
            peer_start_height: Height(100),
            local_tip_height: Some(Height(90)),
        };
        let hash_count = MAX_RECEIVED_BLOCK_HASHES;
        let hashes: Vec<_> = (0..hash_count)
            .map(|index| {
                let mut bytes = [0; 32];
                bytes[..8].copy_from_slice(&index.to_le_bytes());
                block::Hash(bytes)
            })
            .collect();
        trace.find_blocks_finish(
            1,
            peer,
            None,
            None,
            Duration::from_millis(12),
            &Ok(Response::BlockHashes(hashes)),
        );

        drop(trace);
        guard.shutdown().await;

        let event = std::fs::read_to_string(dir.path().join(TABLE.file_name()))
            .expect("legacy peer trace file is written");
        let event: Value = serde_json::from_str(event.trim()).expect("trace row is valid JSON");

        assert_eq!(event["hash_count"], hash_count);
        assert_eq!(event["inferred_start_height"], 91);
        assert_eq!(event["inferred_end_height"], 50_090);
        assert!(event.get("hashes").is_none());
        assert!(event.get("hashes_truncated").is_none());
        assert!(
            event.to_string().len() < 1_000,
            "maximum response trace row must remain bounded"
        );
    }
}
