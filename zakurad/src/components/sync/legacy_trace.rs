use std::{
    fmt::Display,
    path::PathBuf,
    sync::Arc,
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
        }
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

    /// Records what `obtain_tips` did with one peer's `FindBlocks` response.
    ///
    /// `tips_obtained` only reports the totals for a whole fanout, so a syncer that discovers
    /// nothing looks the same whether its peers answered with nothing or answered with hashes that
    /// were all thrown away. `reason` distinguishes those, and `hashes`/`unknown` say how much the
    /// response carried and how much survived the state check.
    pub(super) fn tips_response(
        &self,
        reason: &'static str,
        hashes: usize,
        unknown: usize,
        new_hashes: usize,
    ) {
        self.emit("tips_response", |row| {
            row.insert("reason".to_string(), Value::String(reason.to_string()));
            insert_count(row, "hashes", hashes);
            insert_count(row, "unknown", unknown);
            insert_count(row, "new_hashes", new_hashes);
        });
    }

    /// Records where the state claims each hash of a fully-known `FindBlocks` response lives, plus
    /// the best-chain depth of the response's endpoints.
    ///
    /// A response whose hashes are all `finalized` at a large depth means the peer answered from an
    /// old intersection. Any `queue` or `write_channel` hash means the state is reporting blocks it
    /// has not committed and may never commit.
    pub(super) fn tips_known_probe(
        &self,
        first: Option<(block::Hash, Option<u32>)>,
        last: Option<(block::Hash, Option<u32>)>,
        locations: &[(&'static str, usize)],
    ) {
        self.emit("tips_known_probe", |row| {
            if let Some((hash, depth)) = first {
                row.insert("first_hash".to_string(), Value::String(hash.to_string()));
                if let Some(depth) = depth {
                    row.insert("first_depth".to_string(), Value::from(depth));
                }
            }
            if let Some((hash, depth)) = last {
                row.insert("last_hash".to_string(), Value::String(hash.to_string()));
                if let Some(depth) = depth {
                    row.insert("last_depth".to_string(), Value::from(depth));
                }
            }
            for (location, count) in locations {
                insert_count(row, location, *count);
            }
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
}
