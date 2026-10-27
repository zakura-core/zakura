//! The consequence table (properties.md, Consequence table): what the node
//! must do after it reads a labelled frame. It comes from the regulation
//! specification, not from production code.

use super::Label;

/// What the node must do after it reads a frame with a given label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consequence {
    /// Close the connection with a fault cause before quiescence.
    FaultClose,
    /// The family's content consequence, such as block sync's `Misbehavior`
    /// request.
    FamilyContent,
    /// No fault close and no `Misbehavior`.
    NoPunishment,
}

/// The consequence of a frame labelled `label`.
pub const fn consequence(label: Label) -> Consequence {
    match label {
        Label::UndeclaredType
        | Label::ReservedFlags
        | Label::ShortPayload
        | Label::LongPayload
        | Label::OverMargin
        | Label::Unsolicited
        | Label::OverOwed
        | Label::OverLive
        | Label::OverCredit
        | Label::AfterClose
        | Label::BadCursor => Consequence::FaultClose,
        Label::BadContent => Consequence::FamilyContent,
        Label::Conformant => Consequence::NoPunishment,
    }
}

/// The close causes that assign the peer a fault (properties.md, Close
/// cause until P3c). Until P3c sends close code 6, a fault close is a close
/// whose first recorded cause is one of these. Timeouts, transport errors
/// and resource decisions are neutral.
pub const FAULT_CAUSES: &[&str] = &[
    "service_protocol_reject",
    "header_peer_fault",
    "request_protocol_reject",
    "ordered_oversize",
    "request_oversize",
    "ordered_read_rejected",
    "request_read_rejected",
];

/// True if a close with first recorded cause `cause` assigns the peer a
/// fault.
pub fn is_fault_cause(cause: &str) -> bool {
    FAULT_CAUSES.contains(&cause)
}
