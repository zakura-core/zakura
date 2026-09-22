//! The frame-level codec shared by every message family.

use crate::zakura::{Frame, MessageRule};

use super::{BoundedReader, WireError};

/// A message family whose frames follow one static rule table.
///
/// `RULES` is the same table the family's service returns from
/// `Service::message_rules`, so the transport's header check and this codec
/// read one source of truth. The codec repeats the header checks because
/// tests, test kits, and in-process channels hand frames to it directly.
pub trait WireMessage: Sized {
    /// The family's error type. Structural failures convert from [`WireError`].
    type Error: From<WireError>;

    /// One rule per message type this family accepts.
    const RULES: &'static [MessageRule];

    /// Frame message type for this value.
    fn message_type(&self) -> u16;

    /// Append this message's payload, without the frame header.
    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decode a payload whose type and length already passed its rule.
    ///
    /// The caller rejects trailing bytes after this returns.
    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, Self::Error>;
}

/// Find the rule for `message_type` or report it as unknown.
pub(super) fn rule_for<M: WireMessage>(
    message_type: u16,
) -> Result<&'static MessageRule, WireError> {
    MessageRule::find(M::RULES, message_type).ok_or(WireError::UnknownMessageType(message_type))
}

/// Check a payload length against its rule.
fn check_payload_len(rule: &MessageRule, actual: usize) -> Result<(), WireError> {
    if actual < rule.payload.min || actual > rule.payload.max {
        return Err(WireError::PayloadLength {
            message_type: rule.message_type,
            actual,
            min: rule.payload.min,
            max: rule.payload.max,
        });
    }
    Ok(())
}

/// Encode `message` as a frame and check its payload against its rule.
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

/// Decode a frame: flags, type, and length first, then the payload.
pub fn decode_frame<M: WireMessage>(frame: &Frame) -> Result<M, M::Error> {
    if frame.flags != 0 {
        return Err(WireError::ReservedFlags(frame.flags).into());
    }
    decode_payload_exact(frame.message_type, &frame.payload)
}

/// Decode one complete payload of `message_type` and reject trailing bytes.
pub fn decode_payload_exact<M: WireMessage>(
    message_type: u16,
    payload: &[u8],
) -> Result<M, M::Error> {
    let rule = rule_for::<M>(message_type)?;
    check_payload_len(rule, payload.len())?;
    let mut reader = BoundedReader::new(payload);
    let message = M::decode_payload(message_type, &mut reader)?;
    reader.finish()?;
    Ok(message)
}
