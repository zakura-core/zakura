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
        | Label::BadCursor
        | Label::OverRate => Consequence::FaultClose,
        Label::BadContent => Consequence::FamilyContent,
        Label::Conformant => Consequence::NoPunishment,
    }
}
