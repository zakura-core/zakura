//! The frame-level codec shared by every message family.

use super::{BoundedReader, WireError};
use crate::zakura::{Frame, MessageRule};

/// A message family: the messages of one reactor, whose frames follow its rows.
///
/// A family spans its reactor's whole layout. `RULES` lists every row of the
/// family, and each stream of a layout carries a subset of them. The message
/// suite checks that every layout's rows are exactly the family's rows.
///
/// Implementations are plain `match`es from message type to payload item:
/// `encode_payload`, `decode_payload`, and `max_heap_bytes` must agree. The
/// message suite checks that they do, against real encodings.
pub trait WireMessage: Sized {
    /// The family's error type. Structural failures convert from [`WireError`].
    type Error: From<WireError>;

    /// Every row of the family.
    const RULES: &'static [MessageRule];

    /// The frame message type of this value.
    fn message_type(&self) -> u16;

    /// Append this message's payload, without the frame header.
    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decode a payload whose type and length already passed its row.
    ///
    /// The caller rejects trailing bytes after this returns.
    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, Self::Error>;

    /// Most heap bytes that decoding a `payload_len`-byte payload of
    /// `message_type` may request. Usually the payload item's
    /// [`Wire::max_heap_bytes`](super::Wire::max_heap_bytes).
    ///
    /// A type the family does not declare may return zero: decoding it fails
    /// before any allocation.
    fn max_heap_bytes(message_type: u16, payload_len: usize) -> usize;
}

/// Return the family's row for `message_type`.
fn rule_for<M: WireMessage>(message_type: u16) -> Result<&'static MessageRule, WireError> {
    MessageRule::find(M::RULES, message_type).ok_or(WireError::UnknownMessageType(message_type))
}

/// Check a payload length against its row.
fn check_payload_len(rule: &MessageRule, actual: usize) -> Result<(), WireError> {
    if !rule.payload.contains(actual) {
        return Err(WireError::PayloadLength {
            message_type: rule.message_type,
            actual,
            min: rule.payload.min(),
            max: rule.payload.max(),
        });
    }
    Ok(())
}

/// Encode `message` as a frame, checking its payload against its row.
pub fn encode_frame<M: WireMessage>(message: &M) -> Result<Frame, M::Error> {
    let message_type = message.message_type();
    let rule = rule_for::<M>(message_type)?;
    let mut payload = Vec::new();
    message.encode_payload(&mut payload)?;
    check_payload_len(rule, payload.len())?;
    Ok(Frame {
        message_type,
        flags: 0,
        payload,
    })
}

/// Decode a frame: its flags, type, and length, then its payload.
///
/// A reader's frame filter already checked the header. This function checks
/// it again because tests and in-process channels hand frames to it directly.
pub fn decode_frame<M: WireMessage>(frame: &Frame) -> Result<M, M::Error> {
    if frame.flags != 0 {
        return Err(WireError::ReservedFlags(frame.flags).into());
    }
    let rule = rule_for::<M>(frame.message_type)?;
    check_payload_len(rule, frame.payload.len())?;
    let mut reader = BoundedReader::new(&frame.payload);
    let message = M::decode_payload(frame.message_type, &mut reader)?;
    reader.finish()?;
    Ok(message)
}
