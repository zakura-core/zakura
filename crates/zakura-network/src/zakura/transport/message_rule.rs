//! Static message tables that the transport checks from each frame header.
//!
//! A service declares one [`MessageRule`] per message type it accepts on a
//! stream. The reader checks the frame's type, flags, and payload length
//! against that table before it allocates or reads the payload. The service's
//! codec uses the same table, so the header check and the decoder cannot
//! disagree about which messages exist.
//!
//! A service that returns no table keeps the legacy behavior: the reader admits
//! any type and any flags up to the stream's frame cap.

use crate::zakura::FRAME_HEADER_BYTES;

#[cfg(test)]
mod tests;

/// The protocol role of one message type.
///
/// Roles follow the peer-message regulation specification. The transport uses
/// them to keep request and response readers apart; services use them to pick
/// the admission path for a decoded message.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum MessageRole {
    /// Unsolicited state or inventory that the sender pushes at its own cadence.
    Announcement,
    /// A message that asks the receiver to do work and send a response.
    Request,
    /// A message that answers a request the receiver sent earlier.
    Response,
}

/// Inclusive payload length bounds for one message type.
///
/// The bounds exclude the frame header. A `max` of [`usize::MAX`] means the
/// message is open-ended and the stream's frame cap is its only upper bound.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PayloadLen {
    /// Smallest valid payload in bytes.
    pub min: usize,
    /// Largest valid payload in bytes.
    pub max: usize,
}

impl PayloadLen {
    /// A payload with exactly `len` bytes.
    pub const fn exact(len: usize) -> Self {
        Self { min: len, max: len }
    }

    /// A payload between `min` and `max` bytes, inclusive.
    pub const fn between(min: usize, max: usize) -> Self {
        assert!(min <= max, "payload minimum must not exceed its maximum");
        Self { min, max }
    }

    /// A payload of at least `min` bytes, bounded above only by the stream cap.
    pub const fn at_least(min: usize) -> Self {
        Self {
            min,
            max: usize::MAX,
        }
    }

    /// Whether only the stream cap bounds this payload from above.
    pub const fn is_open_ended(self) -> bool {
        self.max == usize::MAX
    }
}

/// One accepted message type on a stream: its role and its payload bounds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MessageRule {
    /// Frame-header message type.
    pub message_type: u16,
    /// Protocol role of this message.
    pub role: MessageRole,
    /// Payload bounds checked before the payload is read.
    pub payload: PayloadLen,
}

impl MessageRule {
    /// An announcement rule.
    pub const fn announcement(message_type: u16, payload: PayloadLen) -> Self {
        Self {
            message_type,
            role: MessageRole::Announcement,
            payload,
        }
    }

    /// A request rule.
    pub const fn request(message_type: u16, payload: PayloadLen) -> Self {
        Self {
            message_type,
            role: MessageRole::Request,
            payload,
        }
    }

    /// A response rule.
    pub const fn response(message_type: u16, payload: PayloadLen) -> Self {
        Self {
            message_type,
            role: MessageRole::Response,
            payload,
        }
    }

    /// Return the rule for `message_type`, if the table declares one.
    pub fn find(rules: &[Self], message_type: u16) -> Option<&Self> {
        rules.iter().find(|rule| rule.message_type == message_type)
    }
}

/// Why the transport rejected a frame from its header.
///
/// Every rejection is a framing violation and disconnects the peer. A payload
/// above its maximum is reported as an oversize frame instead, so existing
/// oversize diagnostics keep one path.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FrameRejection {
    /// The table has no rule for this type on this reader.
    UnknownMessageType,
    /// The frame sets flag bits that no rule defines.
    ReservedFlags,
    /// The payload is shorter than the rule's minimum.
    PayloadTooShort {
        /// The rule's minimum payload bytes.
        min: usize,
    },
}

impl FrameRejection {
    /// Stable metric and trace label for this rejection.
    pub fn label(self) -> &'static str {
        match self {
            Self::UnknownMessageType => "unknown_message_type",
            Self::ReservedFlags => "reserved_flags",
            Self::PayloadTooShort { .. } => "payload_too_short",
        }
    }
}

/// The kind of reader that receives a frame.
///
/// A request stream carries one request inbound and responses outbound, so its
/// two ends accept disjoint roles. A persistent stream accepts every role.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum InboundReader {
    /// A long-lived ordered stream.
    Persistent,
    /// The responder's end of a request stream, which reads the request.
    RequestStream,
    /// The requester's end of a request stream, which reads responses.
    ResponseStream,
}

impl InboundReader {
    fn admits(self, role: MessageRole) -> bool {
        match self {
            Self::Persistent => true,
            Self::RequestStream => role == MessageRole::Request,
            Self::ResponseStream => role == MessageRole::Response,
        }
    }
}

/// Header check for one reader: the service's table plus the reader kind.
#[derive(Copy, Clone, Debug)]
pub(crate) struct FrameFilter {
    rules: Option<&'static [MessageRule]>,
    reader: InboundReader,
}

impl FrameFilter {
    pub(crate) const fn new(rules: Option<&'static [MessageRule]>, reader: InboundReader) -> Self {
        Self { rules, reader }
    }

    /// Check a frame header and return the largest frame this reader accepts.
    ///
    /// The check runs in order: message type, flags, then minimum length. The
    /// returned cap never exceeds `stream_frame_cap`; the caller rejects a
    /// longer frame as oversize before it allocates the payload.
    pub(crate) fn check_header(
        &self,
        message_type: u16,
        flags: u16,
        payload_len: usize,
        stream_frame_cap: usize,
    ) -> Result<usize, FrameRejection> {
        let Some(rules) = self.rules else {
            return Ok(stream_frame_cap);
        };
        let rule = MessageRule::find(rules, message_type)
            .filter(|rule| self.reader.admits(rule.role))
            .ok_or(FrameRejection::UnknownMessageType)?;
        // No native message defines a flag bit yet.
        if flags != 0 {
            return Err(FrameRejection::ReservedFlags);
        }
        if payload_len < rule.payload.min {
            return Err(FrameRejection::PayloadTooShort {
                min: rule.payload.min,
            });
        }
        Ok(stream_frame_cap.min(rule.payload.max.saturating_add(FRAME_HEADER_BYTES)))
    }
}
