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
    time::{Duration, Instant},
};

use serde_json::{Map, Value};
use zakura_chain::block::{self, Height};
use zakura_jsonl_trace::{JsonlTracer, JsonlWriteEvent};

use crate::{protocol::internal::Response, NotFoundClass, PeerSocketAddr, SharedPeerError};

const TABLE: &str = "legacy_peer_request";
const FILE_NAME: &str = "legacy_peer_request.jsonl";
const HASH_SAMPLE_EDGE_SIZE: usize = 8;
const MAX_HASH_SAMPLE_SIZE: usize = HASH_SAMPLE_EDGE_SIZE * 2;

#[derive(Clone, Debug)]
pub(super) struct LegacyPeerTrace {
    tracer: JsonlTracer,
    node: Arc<str>,
    started: Instant,
    next_request_id: Arc<AtomicU64>,
}

#[derive(Copy, Clone, Debug)]
pub(super) struct PeerTraceContext {
    pub(super) peer_id: u64,
    pub(super) peer: PeerSocketAddr,
    pub(super) peer_start_height: Height,
    pub(super) local_tip_height: Option<Height>,
}

impl LegacyPeerTrace {
    pub(super) fn new(trace_dir: Option<PathBuf>) -> Self {
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);

        Self {
            tracer,
            node: zakura_jsonl_trace::node_id().into(),
            started: Instant::now(),
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(super) fn next_request_id(&self) -> u64 {
        self.next_request_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
                Some(id.saturating_add(1))
            })
            .expect("request ID update succeeds because its closure always returns Some")
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
        self.emit("find_blocks_finish", |row| {
            insert_request_context(row, request_id, peer, elapsed);
            insert_hash(row, "locator_tip", locator_tip);
            insert_hash(row, "stop", stop);

            match result {
                Ok(Response::BlockHashes(hashes)) => {
                    insert_str(row, "result", "block_hashes");
                    insert_count(row, "hash_count", hashes.len());
                    let hashes_truncated = hashes.len() > MAX_HASH_SAMPLE_SIZE;
                    row.insert(
                        "hashes_truncated".to_string(),
                        Value::Bool(hashes_truncated),
                    );

                    let sampled_hashes: Box<dyn Iterator<Item = &block::Hash>> =
                        if hashes_truncated {
                            Box::new(
                                hashes.iter().take(HASH_SAMPLE_EDGE_SIZE).chain(
                                    hashes.iter().skip(hashes.len() - HASH_SAMPLE_EDGE_SIZE),
                                ),
                            )
                        } else {
                            Box::new(hashes.iter())
                        };
                    row.insert(
                        "hashes".to_string(),
                        Value::Array(
                            sampled_hashes
                                .map(|hash| Value::String(hash.to_string()))
                                .collect(),
                        ),
                    );
                }
                Ok(response) => {
                    insert_str(row, "result", "unexpected_response");
                    insert_str(row, "response", response.command());
                }
                Err(error) => insert_error(row, error),
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
        self.emit("block_request_finish", |row| {
            insert_request_context(row, request_id, peer, elapsed);
            insert_hash(row, "requested_hash", Some(requested_hash));
            insert_str(row, "route", route);

            match result {
                Ok(Response::Blocks(blocks)) => {
                    let available = blocks.iter().find_map(|status| status.available());
                    let missing = blocks.iter().find_map(|status| status.missing());

                    if let Some((block, _source)) = available {
                        let returned_hash = block.hash();
                        insert_str(
                            row,
                            "result",
                            if returned_hash == requested_hash {
                                "available"
                            } else {
                                "mismatched_block"
                            },
                        );
                        insert_hash(row, "returned_hash", Some(returned_hash));
                        insert_height(row, "returned_height", block.coinbase_height());
                    } else if let Some(missing_hash) = missing {
                        insert_str(row, "result", "missing");
                        insert_hash(row, "missing_hash", Some(missing_hash));
                    } else {
                        insert_str(row, "result", "empty_blocks");
                    }
                }
                Ok(response) => {
                    insert_str(row, "result", "unexpected_response");
                    insert_str(row, "response", response.command());
                }
                Err(error) => insert_error(row, error),
            }
        });
    }

    fn emit(&self, event: &'static str, build: impl FnOnce(&mut Map<String, Value>)) {
        let Ok(permit) = self.tracer.try_reserve() else {
            return;
        };

        let mut row = Map::new();
        row.insert("ts".to_string(), elapsed_micros(self.started.elapsed()));
        row.insert("node".to_string(), Value::String(self.node.to_string()));
        insert_str(&mut row, "event", event);
        build(&mut row);

        if let Ok(line) = serde_json::to_vec(&Value::Object(row)) {
            permit.send(JsonlWriteEvent {
                table: TABLE,
                file_name: FILE_NAME,
                line,
            });
        }
    }
}

fn insert_request_context(
    row: &mut Map<String, Value>,
    request_id: u64,
    peer: PeerTraceContext,
    elapsed: Duration,
) {
    row.insert("request_id".to_string(), Value::from(request_id));
    row.insert("peer_id".to_string(), Value::from(peer.peer_id));
    row.insert("peer".to_string(), Value::String(peer.peer.to_string()));
    insert_height(row, "peer_start_height", Some(peer.peer_start_height));
    insert_height(row, "local_tip_height", peer.local_tip_height);
    row.insert("elapsed_ms".to_string(), elapsed_millis(elapsed));
}

fn insert_error(row: &mut Map<String, Value>, error: &SharedPeerError) {
    let result = match error.not_found_class() {
        Some(NotFoundClass::Response) => "notfound_response",
        Some(NotFoundClass::Registry) => "notfound_registry",
        Some(NotFoundClass::Other) | None => "error",
    };

    insert_str(row, "result", result);
    row.insert("error".to_string(), Value::String(error.to_string()));
}

fn insert_str(row: &mut Map<String, Value>, key: &'static str, value: &'static str) {
    row.insert(key.to_string(), Value::String(value.to_string()));
}

fn insert_hash(row: &mut Map<String, Value>, key: &'static str, hash: Option<block::Hash>) {
    if let Some(hash) = hash {
        row.insert(key.to_string(), Value::String(hash.to_string()));
    }
}

fn insert_height(row: &mut Map<String, Value>, key: &'static str, height: Option<Height>) {
    if let Some(height) = height {
        row.insert(key.to_string(), Value::from(height.0));
    }
}

fn insert_count(row: &mut Map<String, Value>, key: &'static str, count: usize) {
    row.insert(
        key.to_string(),
        Value::from(u64::try_from(count).unwrap_or(u64::MAX)),
    );
}

fn elapsed_millis(duration: Duration) -> Value {
    Value::from(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

fn elapsed_micros(duration: Duration) -> Value {
    Value::from(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
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

    #[tokio::test]
    async fn writes_attributed_find_blocks_and_notfound_events() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacyPeerTrace {
            tracer: guard.tracer(),
            node: "test-node".into(),
            started: Instant::now(),
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

        let events = std::fs::read_to_string(dir.path().join(FILE_NAME))
            .expect("legacy peer trace file is written");
        let events: Vec<Value> = events
            .lines()
            .map(|line| serde_json::from_str(line).expect("trace row is valid JSON"))
            .collect();

        assert_eq!(events[0]["event"], "find_blocks_finish");
        assert_eq!(events[0]["peer_id"], 7);
        assert_eq!(events[0]["peer_start_height"], 100);
        assert_eq!(events[0]["hash_count"], 2);
        assert_eq!(events[0]["hashes_truncated"], false);
        assert_eq!(events[1]["event"], "block_request_finish");
        assert_eq!(events[1]["route"], "advertised");
        assert_eq!(events[1]["result"], "notfound_response");
    }

    #[tokio::test]
    async fn bounds_maximum_find_blocks_hash_sample() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacyPeerTrace {
            tracer: guard.tracer(),
            node: "test-node".into(),
            started: Instant::now(),
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
        let expected_sample = [
            hashes[0].to_string(),
            hashes[HASH_SAMPLE_EDGE_SIZE - 1].to_string(),
            hashes[hash_count - HASH_SAMPLE_EDGE_SIZE].to_string(),
            hashes[hash_count - 1].to_string(),
        ];

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

        let event = std::fs::read_to_string(dir.path().join(FILE_NAME))
            .expect("legacy peer trace file is written");
        let event: Value = serde_json::from_str(event.trim()).expect("trace row is valid JSON");
        let sampled_hashes = event["hashes"].as_array().expect("hashes is an array");

        assert_eq!(event["hash_count"], hash_count);
        assert_eq!(event["hashes_truncated"], true);
        assert_eq!(sampled_hashes.len(), MAX_HASH_SAMPLE_SIZE);
        assert_eq!(sampled_hashes[0], expected_sample[0]);
        assert_eq!(
            sampled_hashes[HASH_SAMPLE_EDGE_SIZE - 1],
            expected_sample[1]
        );
        assert_eq!(sampled_hashes[HASH_SAMPLE_EDGE_SIZE], expected_sample[2]);
        assert_eq!(sampled_hashes[MAX_HASH_SAMPLE_SIZE - 1], expected_sample[3]);
        assert!(
            event.to_string().len() < 2_000,
            "maximum response trace row must remain bounded"
        );
    }
}
