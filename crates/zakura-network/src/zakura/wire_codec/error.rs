//! Structural errors shared by every typed codec.

use thiserror::Error;

/// A structural encode or decode error.
///
/// Message families return this type directly or wrap it in their own error
/// enum. Every variant holds static labels and numbers, so reporting an error
/// on a hostile input allocates nothing.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum WireError {
    /// The frame sets flag bits, and no row defines any.
    #[error("reserved frame flags {0:#06x}")]
    ReservedFlags(u16),
    /// The frame names a message type that the family does not declare.
    #[error("unknown message type {0}")]
    UnknownMessageType(u16),
    /// The payload length is outside its row's bounds.
    #[error("message type {message_type} payload length {actual} is outside {min}..={max}")]
    PayloadLength {
        /// Frame message type.
        message_type: u16,
        /// Actual payload bytes.
        actual: usize,
        /// Row minimum.
        min: usize,
        /// Row maximum.
        max: usize,
    },
    /// The payload ended before an item was complete.
    #[error("payload truncated while reading {0}")]
    Truncated(&'static str),
    /// The payload has bytes after its last item.
    #[error("payload has trailing bytes")]
    TrailingBytes,
    /// A list count prefix is not a canonical CompactSize.
    #[error("non-canonical list count")]
    NonCanonicalCount,
    /// A list count is outside the list's bounds.
    #[error("list count {count} is outside {min}..={max}")]
    CountOutOfRange {
        /// Declared count.
        count: u64,
        /// Fewest items the list holds.
        min: usize,
        /// Most items the list holds.
        max: usize,
    },
    /// A list count needs more bytes than the payload has left.
    #[error("list count {count} needs more than the {remaining} remaining bytes")]
    CountExceedsPayload {
        /// Declared count.
        count: u64,
        /// Bytes left in the payload.
        remaining: usize,
    },
    /// A value is outside its valid range.
    #[error("{0} is out of range")]
    OutOfRange(&'static str),
}
