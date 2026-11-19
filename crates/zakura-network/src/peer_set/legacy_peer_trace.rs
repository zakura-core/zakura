//! Structured attribution for legacy peer discovery and block download requests.
//!
//! Legacy sync learns ordered hashes from `FindBlocks`, then distributes individual block
//! downloads across the peer set. A download routed to `maybe` has no exact inventory claim from
//! that peer; `advertised` means the selected peer recently sent an `inv` for that exact hash.
//! An `advertised` row also says how the peer was chosen: `candidates` is the number of idle
//! peers that had announced the block, `score_ms` the chosen peer's expected answer time and
//! `best_score_ms` the lowest among the candidates (see
//! `[network.block_download_score_and_hedge]`). A score is missing for a peer whose answers have
//! not been measured yet.
//! A `hedge` row is a second request for a block whose first request had not delivered it yet
//! (see `[network.block_download_score_and_hedge]`). `hedge_reason` says why it was sent:
//! `predicted` when another announcer was expected to deliver clearly sooner, `stalled` when the
//! first request ran well past its expected time. `first_request_waited_ms` is how long the first
//! request had been outstanding since dispatch to a peer, excluding local routing queue time, and
//! `first_score_ms` is the first peer's expected answer time. `decision_block_bytes` is the
//! block's serialized size, if the first peer's reply had started to arrive when the second
//! request was decided. With it, `first_score_ms` and `score_ms` are the times expected for a
//! block of that size from each peer; without it, they are the peers' round trips.
//! A delivered block's row has `block_bytes`, its decoded size, so answer times can be compared
//! with expected ones by block size.
//! `peer_start_height` is the peer's untrusted, point-in-time handshake claim and is not used as an
//! inventory guarantee. A connection's process-local
//! `connection_id` is the same number its `legacy_gossip` rows carry, so the two tables join.
//! The `peer` column holds the peer's address only when the node's `expose_peer_addresses` setting
//! is on, and is redacted otherwise. `user_agent` is the software the peer says it runs.

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

use super::second_request::SecondRequestReason;
use crate::{protocol::internal::Response, NotFoundClass, PeerSocketAddr, SharedPeerError};

const TABLE: JsonlTraceTable =
    JsonlTraceTable::new("legacy_peer_request", "legacy_peer_request.jsonl");

#[derive(Clone, Debug)]
pub(super) struct LegacyPeerTrace {
    emitter: JsonlEventEmitter,
    expose_peer_addresses: bool,
    next_request_id: Arc<AtomicU64>,
}

/// How the peer set chose the peer for a block request, written as the row's `route`.
#[derive(Clone, Copy, Debug)]
pub(super) enum BlockRequestRoute {
    /// The peer recently announced the block.
    Advertised(BlockSourceChoice),
    /// No ready peer had announced the block, so the peer may not have it.
    Maybe,
    /// A second request for a block the first request had not delivered yet.
    Hedge(SecondRequestChoice),
}

/// Why and to whom a second request for a block was sent.
#[derive(Clone, Copy, Debug)]
pub(super) struct SecondRequestChoice {
    /// How long the first request had been outstanding.
    pub(super) first_request_waited: Duration,
    /// Why the second request was sent.
    pub(super) reason: SecondRequestReason,
    /// The number of idle peers, other than the first one, that had announced the block.
    pub(super) candidates: usize,
    /// The chosen peer's expected answer time for the block, if it has been measured.
    pub(super) score: Option<Duration>,
    /// The first peer's expected answer time for the block, from its measurements when it was
    /// asked.
    pub(super) first_score: Duration,
    /// The block's serialized size the two expected times are for, or `None` if the first
    /// peer's reply had not started to arrive, in which case they are round trips.
    pub(super) block_bytes: Option<u64>,
}

/// The idle announcers a block request chose among, and their expected answer times.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct BlockSourceChoice {
    /// The number of idle peers that had announced the block.
    pub(super) candidates: usize,
    /// The chosen peer's expected answer time, if it has been measured.
    pub(super) score: Option<Duration>,
    /// The lowest expected answer time among the candidates, if any has been measured.
    pub(super) best_score: Option<Duration>,
}

#[derive(Clone, Debug)]
pub(super) struct PeerTraceContext {
    pub(super) connection_id: u64,
    pub(super) peer: PeerSocketAddr,
    /// The sanitized user agent the peer advertised in its handshake.
    pub(super) user_agent: Arc<str>,
    pub(super) peer_start_height: Height,
    pub(super) local_tip_height: Option<Height>,
}

#[derive(Serialize)]
#[serde(tag = "event")]
enum LegacyPeerEvent<'a> {
    #[serde(rename = "find_blocks_finish")]
    FindBlocksFinish {
        request_id: u64,
        connection_id: u64,
        peer: String,
        user_agent: &'a str,
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
        connection_id: u64,
        peer: String,
        user_agent: &'a str,
        peer_start_height: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        local_tip_height: Option<u32>,
        elapsed_ms: u64,
        requested_hash: JsonlDisplay<'a, block::Hash>,
        route: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        candidates: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        score_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        best_score_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        hedge_reason: Option<&'static str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        first_request_waited_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        first_score_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        decision_block_bytes: Option<u64>,
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
        block_bytes: u64,
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
    /// Creates a trace that writes to `trace_dir`, or nowhere when it is `None`.
    ///
    /// Rows hold peer addresses only when `expose_peer_addresses` is set.
    pub(super) fn new(trace_dir: Option<PathBuf>, expose_peer_addresses: bool) -> Self {
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);

        Self {
            emitter: JsonlEventEmitter::new(tracer, zakura_jsonl_trace::node_id()),
            expose_peer_addresses,
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    #[cfg(test)]
    pub(super) fn for_tests(
        guard: &zakura_jsonl_trace::JsonlTraceGuard,
        expose_peer_addresses: bool,
    ) -> LegacyPeerTrace {
        LegacyPeerTrace {
            emitter: JsonlEventEmitter::new(guard.tracer(), "test-node"),
            expose_peer_addresses,
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
                connection_id: peer.connection_id,
                peer: peer.peer.addr_label(self.expose_peer_addresses),
                user_agent: &peer.user_agent,
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
        route: BlockRequestRoute,
        elapsed: Duration,
        result: &Result<Response, SharedPeerError>,
    ) {
        let (label, candidates, score, best_score, hedge) = match route {
            BlockRequestRoute::Advertised(choice) => (
                "advertised",
                Some(choice.candidates),
                choice.score,
                choice.best_score,
                None,
            ),
            BlockRequestRoute::Maybe => ("maybe", None, None, None, None),
            BlockRequestRoute::Hedge(choice) => (
                "hedge",
                Some(choice.candidates),
                choice.score,
                None,
                Some(choice),
            ),
        };
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
                            block_bytes: block.decoded_bytes(),
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
                connection_id: peer.connection_id,
                peer: peer.peer.addr_label(self.expose_peer_addresses),
                user_agent: &peer.user_agent,
                peer_start_height: peer.peer_start_height.0,
                local_tip_height: peer.local_tip_height.map(|height| height.0),
                elapsed_ms: saturating_millis(elapsed),
                requested_hash: JsonlDisplay(&requested_hash),
                route: label,
                candidates,
                score_ms: score.map(saturating_millis),
                best_score_ms: best_score.map(saturating_millis),
                hedge_reason: hedge.map(|choice| choice.reason.label()),
                first_request_waited_ms: hedge
                    .map(|choice| saturating_millis(choice.first_request_waited)),
                first_score_ms: hedge.map(|choice| saturating_millis(choice.first_score)),
                decision_block_bytes: hedge.and_then(|choice| choice.block_bytes),
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

    fn test_peer() -> PeerTraceContext {
        PeerTraceContext {
            connection_id: 7,
            peer: "192.0.2.7:8233".parse().expect("valid peer address"),
            user_agent: Arc::from("/Zakura:1.8.7/"),
            peer_start_height: Height(100),
            local_tip_height: Some(Height(90)),
        }
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
        let trace = LegacyPeerTrace::for_tests(&guard, false);
        let peer = test_peer();

        trace.find_blocks_finish(
            1,
            peer.clone(),
            Some(hash(1)),
            None,
            Duration::from_millis(12),
            &Ok(Response::BlockHashes(vec![hash(2), hash(3)])),
        );
        trace.block_request_finish(
            2,
            peer,
            hash(2),
            BlockRequestRoute::Advertised(BlockSourceChoice::default()),
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
                "connection_id",
                "peer",
                "user_agent",
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
                "connection_id",
                "peer",
                "user_agent",
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
        assert_eq!(events[0]["connection_id"], 7);
        assert_eq!(events[0]["peer"], "v4redacted:8233");
        assert_eq!(events[0]["user_agent"], "/Zakura:1.8.7/");
        assert_eq!(events[1]["peer"], "v4redacted:8233");
        assert_eq!(events[1]["user_agent"], "/Zakura:1.8.7/");
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
    async fn names_the_peer_address_only_when_the_node_exposes_peer_addresses() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacyPeerTrace::for_tests(&guard, true);

        trace.block_request_finish(
            1,
            test_peer(),
            hash(2),
            BlockRequestRoute::Maybe,
            Duration::from_millis(4),
            &Err(SharedPeerError::from(PeerError::NotFoundResponse(vec![
                InventoryHash::Block(hash(2)),
            ]))),
        );

        drop(trace);
        guard.shutdown().await;

        let event = std::fs::read_to_string(dir.path().join(TABLE.file_name()))
            .expect("legacy peer trace file is written");
        let event: Value = serde_json::from_str(event.trim()).expect("trace row is valid JSON");

        assert_eq!(event["peer"], "192.0.2.7:8233");
        assert_eq!(event["user_agent"], "/Zakura:1.8.7/");
    }

    #[tokio::test]
    async fn an_advertised_row_says_how_its_peer_was_chosen() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacyPeerTrace::for_tests(&guard, false);
        let missing = || {
            Err(SharedPeerError::from(PeerError::NotFoundResponse(vec![
                InventoryHash::Block(hash(2)),
            ])))
        };

        trace.block_request_finish(
            1,
            test_peer(),
            hash(2),
            BlockRequestRoute::Advertised(BlockSourceChoice {
                candidates: 3,
                score: Some(Duration::from_millis(40)),
                best_score: Some(Duration::from_millis(12)),
            }),
            Duration::from_millis(90),
            &missing(),
        );
        trace.block_request_finish(
            2,
            test_peer(),
            hash(2),
            BlockRequestRoute::Maybe,
            Duration::from_millis(90),
            &missing(),
        );

        drop(trace);
        guard.shutdown().await;

        let events = std::fs::read_to_string(dir.path().join(TABLE.file_name()))
            .expect("legacy peer trace file is written");
        let events: Vec<Value> = events
            .lines()
            .map(|line| serde_json::from_str(line).expect("trace row is valid JSON"))
            .collect();

        assert_eq!(events[0]["route"], "advertised");
        assert_eq!(events[0]["candidates"], 3);
        assert_eq!(events[0]["score_ms"], 40);
        assert_eq!(events[0]["best_score_ms"], 12);
        assert_eq!(events[1]["route"], "maybe");
        assert!(events[1].get("candidates").is_none());
        assert!(events[1].get("score_ms").is_none());
    }

    #[tokio::test]
    async fn a_hedge_row_says_why_it_was_sent_and_how_long_the_first_request_waited() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacyPeerTrace::for_tests(&guard, false);

        trace.block_request_finish(
            1,
            test_peer(),
            hash(2),
            BlockRequestRoute::Hedge(SecondRequestChoice {
                first_request_waited: Duration::from_millis(70),
                reason: SecondRequestReason::Predicted,
                candidates: 2,
                score: Some(Duration::from_millis(50)),
                first_score: Duration::from_millis(230),
                block_bytes: Some(1_500_000),
            }),
            Duration::from_millis(48),
            &Err(SharedPeerError::from(PeerError::NotFoundResponse(vec![
                InventoryHash::Block(hash(2)),
            ]))),
        );

        drop(trace);
        guard.shutdown().await;

        let event = std::fs::read_to_string(dir.path().join(TABLE.file_name()))
            .expect("legacy peer trace file is written");
        let event: Value = serde_json::from_str(event.trim()).expect("trace row is valid JSON");

        assert_eq!(event["route"], "hedge");
        assert_eq!(event["hedge_reason"], "predicted");
        assert_eq!(event["first_request_waited_ms"], 70);
        assert_eq!(event["first_score_ms"], 230);
        assert_eq!(event["decision_block_bytes"], 1_500_000);
        assert_eq!(event["candidates"], 2);
        assert_eq!(event["score_ms"], 50);
        assert!(event.get("best_score_ms").is_none());
    }

    #[tokio::test]
    async fn omits_hashes_from_maximum_find_blocks_response() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacyPeerTrace::for_tests(&guard, false);
        let peer = test_peer();
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
