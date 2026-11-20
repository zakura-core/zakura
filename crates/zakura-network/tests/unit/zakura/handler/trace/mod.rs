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
