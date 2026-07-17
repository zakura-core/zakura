use std::{
    collections::HashMap,
    fmt::Display,
    path::PathBuf,
    sync::Mutex,
    time::{Duration, Instant},
};

use serde::Serialize;
use zakura_chain::block::{self, Height};
use zakura_jsonl_trace::{JsonlEventEmitter, JsonlTraceTable, JsonlTracer};
use zakura_network::PeerSocketAddr;

const TABLE: JsonlTraceTable = JsonlTraceTable::new("legacy_sync", "legacy_sync.jsonl");

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

pub(super) struct LegacyDiagnosticSnapshot<'a> {
    pub(super) event: &'static str,
    pub(super) state_tip: Option<Height>,
    pub(super) in_flight: usize,
    pub(super) reserve: usize,
    pub(super) prospective_tips: usize,
    pub(super) registry_retries: usize,
    pub(super) task_states: &'a Mutex<HashMap<block::Hash, LegacyTaskState>>,
}

/// Non-blocking structured diagnostics for the legacy block sync pipeline.
#[derive(Clone, Debug)]
pub(super) struct LegacySyncTrace {
    emitter: JsonlEventEmitter,
    expose_peer_addresses: bool,
}

impl LegacySyncTrace {
    pub(super) fn new(trace_dir: Option<PathBuf>, expose_peer_addresses: bool) -> Self {
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);
        Self {
            emitter: JsonlEventEmitter::new(tracer, zakura_jsonl_trace::node_id()),
            expose_peer_addresses,
        }
    }

    /// Returns a legacy peer address using this trace's configured privacy policy.
    pub(super) fn peer_label(&self, addr: PeerSocketAddr) -> String {
        peer_addr_label(addr, self.expose_peer_addresses)
    }

    pub(super) fn round_start(&self, state_tip: Option<Height>) {
        self.emitter.emit_event(|| RoundStart {
            event: "round_start",
            state_tip: state_tip.map(|height| height.0),
        });
    }

    pub(super) fn round_finish(
        &self,
        reason: &'static str,
        state_tip: Option<Height>,
        error: Option<&dyn Display>,
    ) {
        self.emitter.emit_event(|| RoundFinish {
            event: "round_finish",
            reason,
            state_tip: state_tip.map(|height| height.0),
            error: error.map(ToString::to_string),
        });
    }

    pub(super) fn tips_obtained(&self, reserve: usize, prospective_tips: usize) {
        self.emitter.emit_event(|| TipsObtained {
            event: "tips_obtained",
            reserve: bounded_count(reserve),
            prospective_tips: bounded_count(prospective_tips),
        });
    }

    pub(super) fn tips_extended(&self, discovered: usize, prospective_tips: usize) {
        self.emitter.emit_event(|| TipsExtended {
            event: "tips_extended",
            discovered: bounded_count(discovered),
            prospective_tips: bounded_count(prospective_tips),
        });
    }

    pub(super) fn block_finish(
        &self,
        hash: block::Hash,
        outcome: LegacyBlockOutcome<'_>,
        state: Option<LegacyTaskState>,
    ) {
        self.emitter
            .emit_event(|| BlockFinish::new(hash, outcome, state));
    }

    pub(super) fn block_phase(
        &self,
        hash: block::Hash,
        phase: &'static str,
        previous_phase: &'static str,
        state: LegacyTaskState,
        phase_elapsed: Duration,
    ) {
        self.emitter.emit_event(|| BlockPhase {
            event: "block_phase",
            hash: hash.to_string(),
            phase,
            previous_phase,
            height: state.height.map(|height| height.0),
            phase_elapsed_ms: elapsed_millis(phase_elapsed),
            elapsed_ms: elapsed_millis(state.started.elapsed()),
        });
    }

    pub(super) fn diagnostic_snapshot(&self, snapshot: LegacyDiagnosticSnapshot<'_>) {
        self.emitter.emit_event(|| {
            let states = snapshot
                .task_states
                .lock()
                .expect("legacy task state lock is only held for synchronous updates");
            let mut tasks: Vec<_> = states
                .iter()
                .map(|(hash, state)| LegacyTaskTrace::new(*hash, state))
                .collect();
            tasks.sort_by_key(|task| task.height().map(u64::from).unwrap_or(u64::MAX));

            DiagnosticSnapshot {
                event: snapshot.event,
                state_tip: snapshot.state_tip.map(|height| height.0),
                in_flight: bounded_count(snapshot.in_flight),
                reserve: bounded_count(snapshot.reserve),
                prospective_tips: bounded_count(snapshot.prospective_tips),
                registry_retries: bounded_count(snapshot.registry_retries),
                tasks,
            }
        });
    }

    pub(super) fn block_downloaded(
        &self,
        hash: block::Hash,
        height: Height,
        download_elapsed: Duration,
        peer: Option<PeerSocketAddr>,
    ) {
        self.emitter.emit_event(|| BlockDownloaded {
            event: "block_downloaded",
            hash: hash.to_string(),
            height: height.0,
            download_elapsed_ms: elapsed_millis(download_elapsed),
            peer: peer.map(|peer| peer_addr_label(peer, self.expose_peer_addresses)),
        });
    }
}

/// Returns a legacy peer address using the configured privacy policy.
pub(super) fn peer_addr_label(addr: PeerSocketAddr, expose_peer_addresses: bool) -> String {
    if expose_peer_addresses {
        addr.remove_socket_addr_privacy().to_string()
    } else {
        addr.to_string()
    }
}

pub(super) fn elapsed_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn bounded_count(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

#[derive(Debug, Serialize)]
struct RoundStart {
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_tip: Option<u32>,
}
zakura_jsonl_trace::impl_jsonl_trace_event!(RoundStart, TABLE);

#[derive(Debug, Serialize)]
struct RoundFinish {
    event: &'static str,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_tip: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}
zakura_jsonl_trace::impl_jsonl_trace_event!(RoundFinish, TABLE);

#[derive(Debug, Serialize)]
struct TipsObtained {
    event: &'static str,
    reserve: u64,
    prospective_tips: u64,
}
zakura_jsonl_trace::impl_jsonl_trace_event!(TipsObtained, TABLE);

#[derive(Debug, Serialize)]
struct TipsExtended {
    event: &'static str,
    discovered: u64,
    prospective_tips: u64,
}
zakura_jsonl_trace::impl_jsonl_trace_event!(TipsExtended, TABLE);

#[derive(Debug, Serialize)]
struct BlockFinish {
    event: &'static str,
    hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase_elapsed_ms: Option<u64>,
}

impl BlockFinish {
    fn new(
        hash: block::Hash,
        outcome: LegacyBlockOutcome<'_>,
        state: Option<LegacyTaskState>,
    ) -> Self {
        let (mut height, result, error) = match outcome {
            LegacyBlockOutcome::Verified(height) => (Some(height.0), "verified", None),
            LegacyBlockOutcome::Error(error) => (None, "error", Some(error.to_string())),
        };
        let (phase, elapsed_ms, phase_elapsed_ms) = state.map_or((None, None, None), |state| {
            height = state.height.map(|height| height.0).or(height);
            (
                Some(state.phase),
                Some(elapsed_millis(state.started.elapsed())),
                Some(elapsed_millis(state.phase_started.elapsed())),
            )
        });
        Self {
            event: "block_finish",
            hash: hash.to_string(),
            height,
            result,
            error,
            phase,
            elapsed_ms,
            phase_elapsed_ms,
        }
    }
}
zakura_jsonl_trace::impl_jsonl_trace_event!(BlockFinish, TABLE);

#[derive(Debug, Serialize)]
struct BlockPhase {
    event: &'static str,
    hash: String,
    phase: &'static str,
    previous_phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    phase_elapsed_ms: u64,
    elapsed_ms: u64,
}
zakura_jsonl_trace::impl_jsonl_trace_event!(BlockPhase, TABLE);

#[derive(Debug, Serialize)]
pub(super) struct LegacyTaskTrace {
    hash: String,
    phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    elapsed_ms: u64,
    phase_elapsed_ms: u64,
}

impl LegacyTaskTrace {
    pub(super) fn new(hash: block::Hash, state: &LegacyTaskState) -> Self {
        Self {
            hash: hash.to_string(),
            phase: state.phase,
            height: state.height.map(|height| height.0),
            elapsed_ms: elapsed_millis(state.started.elapsed()),
            phase_elapsed_ms: elapsed_millis(state.phase_started.elapsed()),
        }
    }

    pub(super) fn height(&self) -> Option<u32> {
        self.height
    }
}

#[derive(Debug, Serialize)]
struct DiagnosticSnapshot {
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_tip: Option<u32>,
    in_flight: u64,
    reserve: u64,
    prospective_tips: u64,
    registry_retries: u64,
    tasks: Vec<LegacyTaskTrace>,
}
zakura_jsonl_trace::impl_jsonl_trace_event!(DiagnosticSnapshot, TABLE);

#[derive(Debug, Serialize)]
struct BlockDownloaded {
    event: &'static str,
    hash: String,
    height: u32,
    download_elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer: Option<String>,
}
zakura_jsonl_trace::impl_jsonl_trace_event!(BlockDownloaded, TABLE);

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn absent_round_tip_is_omitted() {
        assert_eq!(
            serde_json::to_value(RoundStart {
                event: "round_start",
                state_tip: None
            })
            .expect("event serializes"),
            json!({"event": "round_start"})
        );
    }

    #[tokio::test]
    async fn writes_legacy_sync_event() {
        let dir = tempfile::tempdir().expect("temporary trace directory");
        let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
        let trace = LegacySyncTrace {
            emitter: JsonlEventEmitter::new(guard.tracer(), "test-node"),
            expose_peer_addresses: false,
        };

        trace.round_start(Some(Height(42)));
        drop(trace);
        guard.shutdown().await;

        let event = std::fs::read_to_string(dir.path().join(TABLE.file_name()))
            .expect("legacy trace file is written");
        let event: Value = serde_json::from_str(event.trim()).expect("trace row is valid JSON");
        assert_eq!(event["event"], "round_start");
        assert_eq!(event["node"], "test-node");
        assert_eq!(event["state_tip"], 42);
    }

    #[test]
    fn peer_labels_require_explicit_opt_in() {
        let addr: PeerSocketAddr = "192.0.2.1:8233".parse().expect("valid test address");

        assert_eq!(
            LegacySyncTrace::new(None, false).peer_label(addr),
            "v4redacted:8233"
        );
        assert_eq!(
            LegacySyncTrace::new(None, true).peer_label(addr),
            "192.0.2.1:8233"
        );
    }
}
