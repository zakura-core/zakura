use serde::{Serialize, Serializer};
use zakura_jsonl_trace::{JsonlTraceEvent, JsonlTraceTable};

use crate::zakura::{
    trace::{peer_label, CONN_TABLE, HANDSHAKE_TABLE, RATELIMIT_TABLE, STREAM_TABLE},
    ZakuraPeerId, ZakuraTrace,
};

const HANDSHAKE: u8 = 0;
const CONNECTION: u8 = 1;
const STREAM: u8 = 2;
const RATE_LIMIT: u8 = 3;

pub(super) type HandshakeTraceEvent<'a> = HandlerTraceEvent<'a, HANDSHAKE>;
pub(super) type ConnectionTraceEvent<'a> = HandlerTraceEvent<'a, CONNECTION>;
pub(super) type StreamTraceEvent<'a> = HandlerTraceEvent<'a, STREAM>;
pub(super) type RateLimitTraceEvent<'a> = HandlerTraceEvent<'a, RATE_LIMIT>;

#[derive(Clone, Debug)]
pub(super) struct ZakuraConnTrace {
    trace: ZakuraTrace,
    pub(super) id: u64,
    peer_id: Option<ZakuraPeerId>,
}

impl ZakuraConnTrace {
    pub(super) fn new(trace: &ZakuraTrace, id: u64, peer_id: &ZakuraPeerId) -> Self {
        Self {
            trace: trace.clone(),
            id,
            peer_id: trace.is_enabled().then(|| peer_id.clone()),
        }
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    pub(super) fn without_peer(id: u64) -> Self {
        Self {
            trace: ZakuraTrace::noop(),
            id,
            peer_id: None,
        }
    }

    pub(super) fn without_peer_on(trace: &ZakuraTrace, id: u64) -> Self {
        Self {
            trace: trace.clone(),
            id,
            peer_id: None,
        }
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    pub(super) fn placeholder() -> Self {
        Self::without_peer(0)
    }

    fn peer(&self) -> Option<&ZakuraPeerId> {
        self.peer_id.as_ref()
    }

    pub(super) fn connection_event(&self, event: &'static str) -> ConnectionTraceEvent<'_> {
        ConnectionTraceEvent::new(event, self.id, self.peer())
    }

    pub(super) fn handshake_event(&self, event: &'static str) -> HandshakeTraceEvent<'_> {
        HandshakeTraceEvent::new(event, self.id, self.peer())
    }

    pub(super) fn stream_event(&self, event: &'static str, stream: u64) -> StreamTraceEvent<'_> {
        StreamTraceEvent::new(event, self.id, self.peer()).stream(stream)
    }

    pub(super) fn rate_limit_event(
        &self,
        event: &'static str,
        stream: u64,
    ) -> RateLimitTraceEvent<'_> {
        RateLimitTraceEvent::new(event, self.id, self.peer()).stream(stream)
    }

    pub(super) fn trace_connection(
        &self,
        event: &'static str,
        role: Option<&'static str>,
        direction: Option<&'static str>,
        reason: Option<&'static str>,
    ) {
        self.trace.emit_event(|| {
            let mut row = self.connection_event(event);
            if let Some(role) = role {
                row = row.role(role);
            }
            if let Some(direction) = direction {
                row = row.direction(direction);
            }
            if let Some(reason) = reason {
                row = row.reason(reason);
            }
            row
        });
    }

    pub(super) fn trace_handshake(
        &self,
        event: &'static str,
        role: &'static str,
        selected_protocol: Option<u16>,
        network: &'static str,
    ) {
        self.trace.emit_event(|| {
            let mut row = self
                .handshake_event(event)
                .role(role)
                .phase("control")
                .network(network);
            if let Some(selected_protocol) = selected_protocol {
                row = row.selected_protocol(selected_protocol);
            }
            row
        });
    }

    pub(super) fn trace_stream(
        &self,
        event: &'static str,
        stream: u64,
        stream_kind: Option<&'static str>,
    ) {
        self.trace.emit_event(|| {
            let mut row = self.stream_event(event, stream);
            if let Some(stream_kind) = stream_kind {
                row = row.stream_kind(stream_kind);
            }
            row
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn trace_rate_limit(
        &self,
        event: &'static str,
        stream: u64,
        stream_kind: &'static str,
        payload_len: Option<u64>,
        frame_len: Option<u64>,
        max_frame_bytes: Option<u64>,
    ) {
        self.trace.emit_event(|| {
            let mut row = self
                .rate_limit_event(event, stream)
                .stream_kind(stream_kind);
            if let Some(payload_len) = payload_len {
                row = row.payload_len(payload_len);
            }
            if let Some(frame_len) = frame_len {
                row = row.frame_len(frame_len);
            }
            if let Some(max_frame_bytes) = max_frame_bytes {
                row = row.max_frame_bytes(max_frame_bytes);
            }
            row
        });
    }
}

/// A transport event whose table is fixed by its const-generic local alias.
///
/// Every optional field intentionally serializes as JSON null. This is the
/// established handshake/connection/stream/rate-limit schema.
#[derive(Clone, Debug, Serialize)]
pub(super) struct HandlerTraceEvent<'a, const TABLE: u8> {
    event: &'static str,
    conn: Option<u64>,
    stream: Option<u64>,
    payload_len: Option<u64>,
    frame_len: Option<u64>,
    max_frame_bytes: Option<u64>,
    #[serde(serialize_with = "serialize_peer")]
    peer: Option<&'a ZakuraPeerId>,
    role: Option<&'static str>,
    phase: Option<&'static str>,
    reason: Option<&'static str>,
    selected_protocol: Option<u16>,
    direction: Option<&'static str>,
    stream_kind: Option<&'static str>,
    network: Option<&'static str>,
}

impl<'a, const TABLE: u8> HandlerTraceEvent<'a, TABLE> {
    pub(super) fn new(event: &'static str, conn: u64, peer: Option<&'a ZakuraPeerId>) -> Self {
        Self {
            event,
            conn: Some(conn),
            stream: None,
            payload_len: None,
            frame_len: None,
            max_frame_bytes: None,
            peer,
            role: None,
            phase: None,
            reason: None,
            selected_protocol: None,
            direction: None,
            stream_kind: None,
            network: None,
        }
    }

    pub(super) fn stream(mut self, stream: u64) -> Self {
        self.stream = Some(stream);
        self
    }

    pub(super) fn payload_len(mut self, payload_len: u64) -> Self {
        self.payload_len = Some(payload_len);
        self
    }

    pub(super) fn frame_len(mut self, frame_len: u64) -> Self {
        self.frame_len = Some(frame_len);
        self
    }

    pub(super) fn max_frame_bytes(mut self, max_frame_bytes: u64) -> Self {
        self.max_frame_bytes = Some(max_frame_bytes);
        self
    }

    pub(super) fn role(mut self, role: &'static str) -> Self {
        self.role = Some(role);
        self
    }

    pub(super) fn phase(mut self, phase: &'static str) -> Self {
        self.phase = Some(phase);
        self
    }

    pub(super) fn reason(mut self, reason: &'static str) -> Self {
        self.reason = Some(reason);
        self
    }

    pub(super) fn selected_protocol(mut self, selected_protocol: u16) -> Self {
        self.selected_protocol = Some(selected_protocol);
        self
    }

    pub(super) fn direction(mut self, direction: &'static str) -> Self {
        self.direction = Some(direction);
        self
    }

    pub(super) fn stream_kind(mut self, stream_kind: &'static str) -> Self {
        self.stream_kind = Some(stream_kind);
        self
    }

    pub(super) fn network(mut self, network: &'static str) -> Self {
        self.network = Some(network);
        self
    }
}

impl<'a, const TABLE: u8> JsonlTraceEvent for HandlerTraceEvent<'a, TABLE> {
    const TABLE: JsonlTraceTable = match TABLE {
        HANDSHAKE => HANDSHAKE_TABLE,
        CONNECTION => CONN_TABLE,
        STREAM => STREAM_TABLE,
        RATE_LIMIT => RATELIMIT_TABLE,
        _ => panic!("invalid handler trace table"),
    };
}

fn serialize_peer<S>(peer: &Option<&ZakuraPeerId>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match peer {
        Some(peer) => serializer.serialize_some(&peer_label(peer)),
        None => serializer.serialize_none(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn handler_event_preserves_null_fields_and_variant_tables() {
        let event = ConnectionTraceEvent::new("accepted", 3, None)
            .direction("inbound")
            .reason("admission");
        let row = serde_json::to_value(event).expect("handler event serializes");

        assert_eq!(
            row,
            json!({
                "event": "accepted",
                "conn": 3,
                "stream": null,
                "payload_len": null,
                "frame_len": null,
                "max_frame_bytes": null,
                "peer": null,
                "role": null,
                "phase": null,
                "reason": "admission",
                "selected_protocol": null,
                "direction": "inbound",
                "stream_kind": null,
                "network": null,
            })
        );
        assert_eq!(ConnectionTraceEvent::TABLE, CONN_TABLE);
        assert_eq!(HandshakeTraceEvent::TABLE, HANDSHAKE_TABLE);
        assert_eq!(StreamTraceEvent::TABLE, STREAM_TABLE);
        assert_eq!(RateLimitTraceEvent::TABLE, RATELIMIT_TABLE);
        assert_ne!(row, Value::Null);
    }
}
