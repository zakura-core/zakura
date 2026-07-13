use std::{
    fmt::Display,
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

const TABLE: &str = "legacy_sync";
const FILE_NAME: &str = "legacy_sync.jsonl";

#[derive(Clone, Debug)]
pub(super) struct LegacyTaskState {
    pub(super) phase: &'static str,
    pub(super) height: Option<Height>,
    pub(super) started: Instant,
    pub(super) phase_started: Instant,
}

pub(super) enum LegacyBlockOutcome<'a> {
    Verified(Height),
    Error(&'a dyn Display),
}

/// Non-blocking structured diagnostics for the legacy block sync pipeline.
#[derive(Clone, Debug)]
pub(super) struct LegacySyncTrace {
    tracer: JsonlTracer,
    node: Arc<str>,
    started: Instant,
    next_fanout_id: Arc<AtomicU64>,
}

impl LegacySyncTrace {
    pub(super) fn new(trace_dir: Option<PathBuf>) -> Self {
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);

        Self {
            tracer,
            node: zakura_jsonl_trace::node_id().into(),
            started: Instant::now(),
            next_fanout_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(super) fn next_fanout_id(&self) -> u64 {
        self.next_fanout_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn emit(&self, event: &'static str, build: impl FnOnce(&mut Map<String, Value>)) {
        let Ok(permit) = self.tracer.try_reserve() else {
            return;
        };

        let mut row = Map::new();
        row.insert("ts".to_string(), elapsed_micros(self.started.elapsed()));
        row.insert("node".to_string(), Value::String(self.node.to_string()));
        row.insert("event".to_string(), Value::String(event.to_string()));
        build(&mut row);

        if let Ok(line) = serde_json::to_vec(&Value::Object(row)) {
            permit.send(JsonlWriteEvent {
                table: TABLE,
                file_name: FILE_NAME,
                line,
            });
        }
    }

    pub(super) fn round_start(&self, state_tip: Option<Height>) {
        self.emit("round_start", |row| {
            insert_height(row, "state_tip", state_tip)
        });
    }

    pub(super) fn round_finish(
        &self,
        reason: &'static str,
        state_tip: Option<Height>,
        error: Option<&dyn Display>,
    ) {
        self.emit("round_finish", |row| {
            row.insert("reason".to_string(), Value::String(reason.to_string()));
            insert_height(row, "state_tip", state_tip);
            if let Some(error) = error {
                row.insert("error".to_string(), Value::String(error.to_string()));
            }
        });
    }

    pub(super) fn tips_obtained(&self, reserve: usize, prospective_tips: usize) {
        self.emit("tips_obtained", |row| {
            insert_count(row, "reserve", reserve);
            insert_count(row, "prospective_tips", prospective_tips);
        });
    }

    pub(super) fn tips_extended(&self, discovered: usize, prospective_tips: usize) {
        self.emit("tips_extended", |row| {
            insert_count(row, "discovered", discovered);
            insert_count(row, "prospective_tips", prospective_tips);
        });
    }

    pub(super) fn tips_request(
        &self,
        fanout_id: u64,
        round_id: u64,
        mode: &'static str,
        state_tip: Option<Height>,
        locator: &[block::Hash],
        first_locator_height: Option<Height>,
    ) {
        self.emit("tips_request", |row| {
            row.insert("fanout_id".to_string(), Value::from(fanout_id));
            row.insert("round_id".to_string(), Value::from(round_id));
            row.insert("mode".to_string(), Value::String(mode.to_string()));
            insert_height(row, "state_tip", state_tip);
            insert_count(row, "locator_count", locator.len());
            insert_hash(row, "first_locator_hash", locator.first().copied());
            insert_hash(row, "last_locator_hash", locator.last().copied());
            insert_height(row, "first_locator_height", first_locator_height);
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tips_response(
        &self,
        fanout_id: u64,
        round_id: u64,
        mode: &'static str,
        attempt: usize,
        peer: Option<zakura_network::PeerSocketAddr>,
        peer_reused: bool,
        latency: Option<Duration>,
        hashes: &[block::Hash],
        genesis_hash: block::Hash,
        first_unknown_index: Option<usize>,
        known_hashes: usize,
        usable_hashes: usize,
        classification: &'static str,
        error: Option<&dyn Display>,
    ) {
        self.emit("tips_response", |row| {
            row.insert("fanout_id".to_string(), Value::from(fanout_id));
            row.insert("round_id".to_string(), Value::from(round_id));
            row.insert("mode".to_string(), Value::String(mode.to_string()));
            insert_count(row, "attempt", attempt);
            if let Some(peer) = peer {
                // These operator-only local traces intentionally bypass the normal peer-address
                // redaction so repeated selections can be correlated within a fanout.
                row.insert(
                    "peer".to_string(),
                    Value::String(peer.remove_socket_addr_privacy().to_string()),
                );
            }
            row.insert("peer_reused".to_string(), Value::Bool(peer_reused));
            if let Some(latency) = latency {
                row.insert("response_latency_ms".to_string(), elapsed_millis(latency));
            }
            insert_count(row, "response_len", hashes.len());
            insert_hash(row, "first_hash", hashes.first().copied());
            insert_hash(row, "last_hash", hashes.last().copied());
            row.insert(
                "starts_with_genesis".to_string(),
                Value::Bool(hashes.first() == Some(&genesis_hash)),
            );
            if let Some(index) = first_unknown_index {
                insert_count(row, "first_unknown_index", index);
            }
            insert_count(row, "known_hashes", known_hashes);
            insert_count(row, "usable_hashes", usable_hashes);
            row.insert(
                "classification".to_string(),
                Value::String(classification.to_string()),
            );
            if let Some(error) = error {
                row.insert("error".to_string(), Value::String(error.to_string()));
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tips_fanout_finish(
        &self,
        fanout_id: u64,
        round_id: u64,
        mode: &'static str,
        responses: usize,
        unique_peers: usize,
        usable_hashes: usize,
        all_genesis_fallback: bool,
    ) {
        self.emit("tips_fanout_finish", |row| {
            row.insert("fanout_id".to_string(), Value::from(fanout_id));
            row.insert("round_id".to_string(), Value::from(round_id));
            row.insert("mode".to_string(), Value::String(mode.to_string()));
            insert_count(row, "responses", responses);
            insert_count(row, "unique_peers_selected", unique_peers);
            insert_count(row, "usable_hashes", usable_hashes);
            row.insert(
                "all_genesis_fallback".to_string(),
                Value::Bool(all_genesis_fallback),
            );
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn tip_transition(
        &self,
        fanout_id: u64,
        round_id: u64,
        mode: &'static str,
        old_tip: block::Hash,
        old_expected_next: block::Hash,
        expected_next_position: Option<usize>,
        new_tip: Option<block::Hash>,
        new_expected_next: Option<block::Hash>,
        discard_reason: Option<&'static str>,
        old_tip_retained: bool,
    ) {
        self.emit("tip_transition", |row| {
            row.insert("fanout_id".to_string(), Value::from(fanout_id));
            row.insert("round_id".to_string(), Value::from(round_id));
            row.insert("mode".to_string(), Value::String(mode.to_string()));
            row.insert("old_tip".to_string(), Value::String(old_tip.to_string()));
            row.insert(
                "old_expected_next".to_string(),
                Value::String(old_expected_next.to_string()),
            );
            if let Some(position) = expected_next_position {
                insert_count(row, "expected_next_position", position);
            }
            insert_hash(row, "new_tip", new_tip);
            insert_hash(row, "new_expected_next", new_expected_next);
            if let Some(reason) = discard_reason {
                row.insert(
                    "discard_reason".to_string(),
                    Value::String(reason.to_string()),
                );
            }
            row.insert(
                "old_tip_retained".to_string(),
                Value::Bool(old_tip_retained),
            );
        });
    }

    pub(super) fn block_finish(
        &self,
        hash: block::Hash,
        outcome: LegacyBlockOutcome<'_>,
        state: Option<LegacyTaskState>,
    ) {
        self.emit("block_finish", |row| {
            row.insert("hash".to_string(), Value::String(hash.to_string()));
            match outcome {
                LegacyBlockOutcome::Verified(height) => {
                    row.insert("height".to_string(), Value::from(height.0));
                    row.insert("result".to_string(), Value::String("verified".to_string()));
                }
                LegacyBlockOutcome::Error(error) => {
                    row.insert("result".to_string(), Value::String("error".to_string()));
                    row.insert("error".to_string(), Value::String(error.to_string()));
                }
            }

            if let Some(state) = state {
                row.insert("phase".to_string(), Value::String(state.phase.to_string()));
                insert_height(row, "height", state.height);
                row.insert(
                    "elapsed_ms".to_string(),
                    elapsed_millis(state.started.elapsed()),
                );
                row.insert(
                    "phase_elapsed_ms".to_string(),
                    elapsed_millis(state.phase_started.elapsed()),
                );
            }
        });
    }
}

pub(super) fn elapsed_millis(duration: Duration) -> Value {
    Value::from(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

fn elapsed_micros(duration: Duration) -> Value {
    Value::from(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
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

fn insert_hash(row: &mut Map<String, Value>, key: &'static str, hash: Option<block::Hash>) {
    if let Some(hash) = hash {
        row.insert(key.to_string(), Value::String(hash.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writes_legacy_sync_event() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacySyncTrace {
            tracer: guard.tracer(),
            node: "test-node".into(),
            started: Instant::now(),
            next_fanout_id: Arc::new(AtomicU64::new(1)),
        };

        trace.round_start(Some(Height(42)));
        drop(trace);
        guard.shutdown().await;

        let event = std::fs::read_to_string(dir.path().join(FILE_NAME))
            .expect("legacy trace file is written");
        let event: Value = serde_json::from_str(event.trim()).expect("trace row is valid JSON");
        assert_eq!(event["event"], "round_start");
        assert_eq!(event["node"], "test-node");
        assert_eq!(event["state_tip"], 42);
    }

    #[tokio::test]
    async fn correlates_tip_requests_with_peer_responses() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacySyncTrace {
            tracer: guard.tracer(),
            node: "test-node".into(),
            started: Instant::now(),
            next_fanout_id: Arc::new(AtomicU64::new(1)),
        };
        let genesis = block::Hash::from([0; 32]);
        let peer = "127.0.0.1:8233"
            .parse()
            .expect("test peer address is valid");

        trace.tips_request(
            7,
            3,
            "refresh",
            Some(Height(57_696)),
            &[genesis],
            Some(Height(57_696)),
        );
        trace.tips_response(
            7,
            3,
            "refresh",
            0,
            Some(peer),
            false,
            Some(Duration::from_millis(12)),
            &[genesis],
            genesis,
            None,
            1,
            0,
            "genesis_fallback",
            None,
        );
        drop(trace);
        guard.shutdown().await;

        let events = std::fs::read_to_string(dir.path().join(FILE_NAME))
            .expect("legacy trace file is written");
        let events = events
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("trace row is valid JSON"))
            .collect::<Vec<_>>();

        assert_eq!(events[0]["event"], "tips_request");
        assert_eq!(events[0]["fanout_id"], 7);
        assert_eq!(events[0]["first_locator_height"], 57_696);
        assert_eq!(events[1]["event"], "tips_response");
        assert_eq!(events[1]["fanout_id"], 7);
        assert_eq!(events[1]["peer"], "127.0.0.1:8233");
        assert_eq!(events[1]["classification"], "genesis_fallback");
        assert_eq!(events[1]["starts_with_genesis"], true);
    }
}
