//! Evidence for the private transport experiment.

use crate::zakura::{DiscoveryMessage, ZakuraPeerId};
use serde::Serialize;
use std::{path::PathBuf, sync::OnceLock};
use zakura_jsonl_trace::{JsonlEventEmitter, JsonlTraceTable, JsonlTracer};

static EMITTER: OnceLock<JsonlEventEmitter> = OnceLock::new();
const TABLE: JsonlTraceTable = JsonlTraceTable::csv(
    "smoke_event",
    "smoke_event.csv",
    &["event", "peer", "kind", "details"],
);

#[derive(Serialize)]
struct Event {
    event: &'static str,
    peer: String,
    kind: &'static str,
    details: String,
}
zakura_jsonl_trace::impl_jsonl_trace_event!(Event, TABLE);

/// Enables experiment telemetry when the operator configures a trace directory.
pub fn install(directory: Option<PathBuf>) {
    if let Some(directory) = directory {
        let tracer = JsonlTracer::spawn(directory);
        let _ = EMITTER.set(JsonlEventEmitter::new(
            tracer,
            zakura_jsonl_trace::node_id(),
        ));
    }
}

pub(crate) fn discovery(event: &'static str, peer: &ZakuraPeerId, message: &DiscoveryMessage) {
    if let Some(emitter) = EMITTER.get() {
        emitter.emit_event(|| Event {
            event,
            peer: hex::encode(peer.as_bytes()),
            kind: match message {
                DiscoveryMessage::Hello { .. } => "hello",
                DiscoveryMessage::GetPeers { .. } => "get_peers",
                DiscoveryMessage::Peers { .. } => "peers",
                DiscoveryMessage::GetServices(_) => "get_services",
                DiscoveryMessage::Services(_) => "services",
            },
            details: format!("{message:?}"),
        });
    }
}

pub(crate) fn body(hash: zakura_chain::block::Hash, source: &'static str) {
    if let Some(emitter) = EMITTER.get() {
        emitter.emit_event(|| Event {
            event: "body_received",
            peer: String::new(),
            kind: source,
            details: hash.to_string(),
        });
    }
}
