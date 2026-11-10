//! Opt-in diagnostics for legacy peer recovery; never influences peer selection.

use std::time::Instant;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use zakura_jsonl_trace::{JsonlEventEmitter, JsonlTraceTable, JsonlTracer};

use crate::{Config, PeerSocketAddr};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("legacy_peers", "legacy_peers.jsonl");

/// Shared non-blocking event writer for one legacy networking instance.
#[derive(Clone, Debug, Default)]
pub(crate) struct LegacyPeerTrace {
    emitter: JsonlEventEmitter,
    expose_addresses: bool,
}

impl LegacyPeerTrace {
    pub(crate) fn new(config: &Config) -> Self {
        let tracer = config
            .zakura
            .trace_dir
            .clone()
            .filter(|_| config.legacy_p2p())
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);
        let trace = Self {
            emitter: JsonlEventEmitter::new(tracer, zakura_jsonl_trace::node_id()),
            expose_addresses: config.expose_peer_addresses,
        };
        trace.event("network_start", None, || {
            json!({
                "wall_time_ms": chrono::Utc::now().timestamp_millis(),
                "initial_target": config.peerset_initial_target_size,
                "inbound_limit": config.peerset_inbound_connection_limit(),
                "outbound_limit": config.peerset_outbound_connection_limit(),
            })
        });
        trace
    }

    pub(crate) fn enabled(&self) -> bool {
        self.emitter.is_enabled()
    }

    /// Records bounded diagnostic fields only after reserving non-blocking queue space.
    pub(crate) fn event(
        &self,
        event: &'static str,
        addr: Option<PeerSocketAddr>,
        fields: impl FnOnce() -> Value,
    ) {
        self.emitter.emit_with(TABLE, |row| {
            row.insert("event".into(), event.into());
            if let Some(addr) = addr {
                // Stable identifiers correlate endpoints across restarts without exposing raw
                // addresses by default. These hashes are pseudonyms, not anonymization.
                row.insert("peer".into(), peer_id(addr, self.expose_addresses).into());
                row.insert(
                    "ip_id".into(),
                    format!("{:x}", Sha256::digest(addr.ip().to_string().as_bytes())).into(),
                );
            }
            if let Value::Object(fields) = fields() {
                row.extend(fields);
            }
        });
    }

    pub(crate) fn dial(&self, addr: PeerSocketAddr) -> DialTrace {
        self.event("dial_start", Some(addr), || json!({}));
        metrics::counter!("peer.restart.dial.started").increment(1);
        DialTrace {
            trace: self.clone(),
            addr,
            started: Instant::now(),
            outcome: "cancelled",
            phase: "tcp",
        }
    }
}

fn peer_id(addr: PeerSocketAddr, expose: bool) -> String {
    let endpoint = addr.remove_socket_addr_privacy().to_string();
    if expose {
        endpoint
    } else {
        format!("{:x}", Sha256::digest(endpoint.as_bytes()))
    }
}

/// Records cancellation as well as completion, including an outer service timeout.
pub(crate) struct DialTrace {
    trace: LegacyPeerTrace,
    addr: PeerSocketAddr,
    started: Instant,
    pub(crate) outcome: &'static str,
    pub(crate) phase: &'static str,
}

impl Drop for DialTrace {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed().as_secs_f64();
        self.trace.event("dial_finish", Some(self.addr), || {
            json!({
                "outcome": self.outcome, "phase": self.phase, "elapsed_seconds": elapsed,
            })
        });
        metrics::counter!("peer.restart.dial.finished", "outcome" => self.outcome, "phase" => self.phase).increment(1);
        metrics::histogram!("peer.restart.dial.duration_seconds", "outcome" => self.outcome)
            .record(elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_identity_survives_restart_and_honors_address_exposure() {
        let addr = "192.0.2.1:8233".parse().unwrap();
        assert_eq!(peer_id(addr, false).len(), 64);
        assert_ne!(
            peer_id(addr, false),
            peer_id("192.0.2.1:8234".parse().unwrap(), false)
        );
        assert_eq!(peer_id(addr, true), "192.0.2.1:8233");
        assert!(!peer_id(addr, false).contains("192.0.2.1"));
    }

    #[test]
    fn disabled_trace_does_not_build_event_fields() {
        LegacyPeerTrace::default()
            .event("unused", None, || panic!("disabled trace evaluated fields"));
    }

    #[test]
    fn cancelled_dial_is_recorded_without_leaking_the_endpoint() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let trace = LegacyPeerTrace {
            emitter: JsonlEventEmitter::new(JsonlTracer::new(tx), "baseline"),
            expose_addresses: false,
        };
        drop(trace.dial("192.0.2.1:8233".parse().unwrap()));
        let start: Value = serde_json::from_slice(&rx.try_recv().unwrap().line).unwrap();
        let finish: Value = serde_json::from_slice(&rx.try_recv().unwrap().line).unwrap();
        assert_eq!(start["event"], "dial_start");
        assert_eq!(finish["event"], "dial_finish");
        assert_eq!(finish["outcome"], "cancelled");
        assert_eq!(start["peer"], finish["peer"]);
        assert_eq!(start["process_trace_id"], finish["process_trace_id"]);
        assert!(!start.to_string().contains("192.0.2.1"));
    }
}
