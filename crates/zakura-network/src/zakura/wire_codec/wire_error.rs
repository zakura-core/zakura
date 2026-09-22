//! Structural decode and encode errors shared by every typed codec.

use thiserror::Error;
use zakura_chain::serialization::SerializationError;

/// A structural wire error.
///
/// Message families wrap this type in their own error enum and keep only
/// their domain variants. The variants carry static labels, not heap strings,
/// so an error costs no allocation on a hostile input.
#[derive(Clone, Debug, Error)]
pub enum WireError {
    /// The frame sets flag bits that no rule defines.
    #[error("reserved frame flags {0:#06x}")]
    ReservedFlags(u16),
    /// The frame names a message type that the family does not declare.
    #[error("unknown message type {0}")]
    UnknownMessageType(u16),
    /// The payload length is outside the message rule's bounds.
    #[error("message type {message_type} payload length {actual} is outside {min}..={max}")]
    PayloadLength {
        /// Frame message type.
        message_type: u16,
        /// Actual payload bytes.
        actual: usize,
        /// Rule minimum.
        min: usize,
        /// Rule maximum.
        max: usize,
    },
    /// The payload ended before a field was complete.
    #[error("payload truncated while reading {0}")]
    Truncated(&'static str),
    /// The payload has bytes after the last field.
    #[error("payload has trailing bytes")]
    TrailingBytes,
    /// A list count is outside its declared bounds.
    #[error("{list} count {count} is outside {min}..={max}")]
    CountOutOfRange {
        /// List name.
        list: &'static str,
        /// Declared count.
        count: u64,
        /// Minimum count.
        min: usize,
        /// Maximum count.
        max: usize,
    },
    /// A list count needs more bytes than the payload has left.
    #[error("{list} count {count} needs more than the {remaining} remaining bytes")]
    CountExceedsPayload {
        /// List name.
        list: &'static str,
        /// Declared count.
        count: u64,
        /// Bytes left in the payload.
        remaining: usize,
    },
    /// A numeric field is outside its valid range.
    #[error("{0} is out of range")]
    OutOfRange(&'static str),
    /// A decoded item failed a context-free validity check.
    #[error("invalid {0}")]
    InvalidItem(&'static str),
    /// A Zcash-serialized item failed to decode.
    #[error(transparent)]
    Item(#[from] SerializationError),
}

impl From<std::io::Error> for WireError {
    fn from(error: std::io::Error) -> Self {
        Self::Item(error.into())
    }
}
