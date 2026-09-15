//! Check payload size and the service's message-rate policy before forwarding.
//!
//! Fast replies to our own requests must not spend the allowance for infrequent
//! metadata messages. Services opt in by message type once their work, buffers
//! and response authorization are bounded. Other messages keep the rate check.

use super::{stream_kind_label, Clock, Frame, MessageRatePolicy, StreamWorkerContext};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum InboundMessageAdmission {
    Admit,
    Oversize,
    Throttled,
}

pub(super) fn admit_inbound_message<C: Clock>(
    frame: &Frame,
    context: &StreamWorkerContext<C>,
    stream_kind: u16,
) -> InboundMessageAdmission {
    let payload_len = frame.payload.len();
    let stream_kind = stream_kind_label(stream_kind);
    let max_message_bytes = usize::try_from(context.limits.max_message_bytes)
        .expect("u32 message byte limit fits in usize");
    if payload_len > max_message_bytes {
        metrics::counter!(
            "zakura.p2p.ratelimit.message.oversize",
            "stream_kind" => stream_kind,
        )
        .increment(1);
        context.conn.trace_rate_limit(
            "message.oversize",
            context.stream_id,
            stream_kind,
            None,
            None,
            None,
        );
        return InboundMessageAdmission::Oversize;
    }

    // This only selects the rate check. Bounded queues and the service's work,
    // decode and response-authorization checks still decide what can proceed.
    if let MessageRatePolicy::CapacityBounded(message_types) = context.message_rate_policy {
        if message_types.contains(&frame.message_type) {
            return InboundMessageAdmission::Admit;
        }
    }

    let admitted = {
        let mut bucket = context
            .message_bucket
            .lock()
            .expect("Zakura message-rate bucket mutex is never poisoned");
        bucket.try_take()
    };
    if !admitted {
        metrics::counter!(
            "zakura.p2p.ratelimit.message.throttled",
            "stream_kind" => stream_kind,
        )
        .increment(1);
        context.conn.trace_rate_limit(
            "message.throttled",
            context.stream_id,
            stream_kind,
            None,
            None,
            None,
        );
        return InboundMessageAdmission::Throttled;
    }

    InboundMessageAdmission::Admit
}
