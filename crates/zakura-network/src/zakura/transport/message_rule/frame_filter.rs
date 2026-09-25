//! The header check that every stream reader runs before reading a payload.

use thiserror::Error;

use super::{MessageRole, MessageRule};
use crate::zakura::FRAME_HEADER_BYTES;

/// Why a reader rejected a frame from its header.
///
/// Every rejection is a framing violation that disconnects the peer. A payload
/// above its row's maximum is reported as an oversize frame instead, so the
/// existing oversize diagnostics keep one path.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum FrameRejection {
    /// No row on this reader declares the frame's message type.
    #[error("no row on this reader declares the message type")]
    UnknownMessageType,
    /// The frame sets flag bits, and no row defines any.
    #[error("the frame sets reserved flags")]
    ReservedFlags,
    /// The payload is shorter than its row's minimum.
    #[error("the payload is shorter than its row's {min}-byte minimum")]
    PayloadTooShort {
        /// The row's minimum payload bytes.
        min: usize,
    },
}

impl FrameRejection {
    /// Stable metric and trace label.
    pub fn label(self) -> &'static str {
        match self {
            Self::UnknownMessageType => "unknown_message_type",
            Self::ReservedFlags => "reserved_flags",
            Self::PayloadTooShort { .. } => "payload_too_short",
        }
    }
}

/// The end of a stream that reads a frame.
///
/// A persistent stream's reader accepts every row of its stream. The two ends
/// of a request/response stream accept disjoint roles: the responder reads the
/// request, and the requester reads the responses.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum InboundReader {
    /// The reader of a persistent stream.
    Persistent,
    /// The responder's end of a request/response stream.
    Responder,
    /// The requester's end of a request/response stream.
    Requester,
}

impl InboundReader {
    const fn admits(self, role: MessageRole) -> bool {
        match self {
            Self::Persistent => true,
            Self::Responder => matches!(role, MessageRole::Request { .. }),
            Self::Requester => matches!(role, MessageRole::Response { .. }),
        }
    }
}

/// One reader's header check: its stream's table and the reader's end.
#[derive(Copy, Clone, Debug)]
pub(crate) struct FrameFilter {
    rules: Option<&'static [MessageRule]>,
    reader: InboundReader,
}

impl FrameFilter {
    pub(crate) const fn new(rules: Option<&'static [MessageRule]>, reader: InboundReader) -> Self {
        Self { rules, reader }
    }

    /// Check a frame header and return the largest frame this reader accepts
    /// for it.
    ///
    /// The checks run in order: message type, flags, then minimum length. The
    /// returned cap never exceeds `frame_cap`. The caller rejects a longer frame
    /// as oversize before it allocates the payload. A stream without a table
    /// accepts any header up to `frame_cap`.
    pub(crate) fn check_header(
        &self,
        message_type: u16,
        flags: u16,
        payload_len: usize,
        frame_cap: usize,
    ) -> Result<usize, FrameRejection> {
        let Some(rules) = self.rules else {
            return Ok(frame_cap);
        };
        let rule = MessageRule::find(rules, message_type)
            .filter(|rule| self.reader.admits(rule.role))
            .ok_or(FrameRejection::UnknownMessageType)?;
        // No native message defines a flag bit.
        if flags != 0 {
            return Err(FrameRejection::ReservedFlags);
        }
        if payload_len < rule.payload.min() {
            return Err(FrameRejection::PayloadTooShort {
                min: rule.payload.min(),
            });
        }
        Ok(frame_cap.min(rule.payload.max().saturating_add(FRAME_HEADER_BYTES)))
    }
}
