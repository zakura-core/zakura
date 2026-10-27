//! Violation labels (properties.md, Violation labels).

use std::fmt;

/// The violation a simulated peer commits with one frame, or `Conformant`.
///
/// The variants are in the table's order. A frame that matches several
/// labels takes the first one: see [`Labels::first`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Label {
    /// The type is not in the stream's table, or the reader does not admit
    /// its role.
    UndeclaredType,
    /// A flag bit is set.
    ReservedFlags,
    /// The payload is shorter than the row's minimum.
    ShortPayload,
    /// The payload is longer than the row's maximum.
    LongPayload,
    /// Open requests from this session exceed twice the highest limit the
    /// node advertised.
    OverMargin,
    /// No open exchange of the named request row has this exchange key.
    Unsolicited,
    /// The exchange has already received its allowed frames or bytes.
    OverOwed,
    /// Live or closing subscriptions exceed twice `max_live`.
    OverLive,
    /// A page exceeds the unspent object or byte credit, or carries no
    /// object.
    OverCredit,
    /// An update follows the subscription's `Close`.
    AfterClose,
    /// An update names an unknown key or sequence, acknowledges an unknown
    /// cursor, adds no credit, grants beyond the window, or reuses a key.
    BadCursor,
    /// The frame is well formed, but its content fails a family check.
    BadContent,
    /// None of the above.
    Conformant,
}

impl Label {
    /// Every label, in table order.
    pub const ALL: [Label; 13] = [
        Label::UndeclaredType,
        Label::ReservedFlags,
        Label::ShortPayload,
        Label::LongPayload,
        Label::OverMargin,
        Label::Unsolicited,
        Label::OverOwed,
        Label::OverLive,
        Label::OverCredit,
        Label::AfterClose,
        Label::BadCursor,
        Label::BadContent,
        Label::Conformant,
    ];

    /// The label's position in the table.
    const fn bit(self) -> u16 {
        // The enum has 13 variants, so the shift stays in range.
        1 << self as u16
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Every violation one frame commits. Empty for a conformant frame.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Labels(u16);

impl Labels {
    /// No violation.
    pub const NONE: Labels = Labels(0);

    /// Only `label`. `Conformant` gives [`Labels::NONE`].
    pub const fn of(label: Label) -> Labels {
        match label {
            Label::Conformant => Labels::NONE,
            label => Labels(label.bit()),
        }
    }

    /// These labels and `label`.
    #[must_use]
    pub const fn with(self, label: Label) -> Labels {
        Labels(self.0 | Labels::of(label).0)
    }

    /// These labels and `other`'s.
    #[must_use]
    pub const fn union(self, other: Labels) -> Labels {
        Labels(self.0 | other.0)
    }

    /// `label` too, if `holds`.
    #[must_use]
    pub const fn with_if(self, holds: bool, label: Label) -> Labels {
        if holds {
            self.with(label)
        } else {
            self
        }
    }

    /// True if the frame commits no violation.
    pub const fn is_conformant(self) -> bool {
        self.0 == 0
    }

    /// True if the frame commits `label`. Every frame "commits"
    /// `Conformant` exactly when it commits nothing else.
    pub const fn contains(self, label: Label) -> bool {
        match label {
            Label::Conformant => self.is_conformant(),
            label => self.0 & label.bit() != 0,
        }
    }

    /// The frame's label: the first violation in table order, or
    /// `Conformant`.
    pub fn first(self) -> Label {
        self.iter().next().unwrap_or(Label::Conformant)
    }

    /// The violations, in table order.
    pub fn iter(self) -> impl Iterator<Item = Label> {
        Label::ALL
            .into_iter()
            .filter(move |label| *label != Label::Conformant && self.contains(*label))
    }
}

impl fmt::Debug for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_conformant() {
            return f.write_str("{Conformant}");
        }
        f.debug_set().entries(self.iter()).finish()
    }
}

/// Which way a labelled frame travels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Direction {
    /// From a simulated peer to the node: a receiver property.
    ToNode,
    /// From the node to a simulated peer: a sender property.
    FromNode,
}

/// What the receiver did with a labelled frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LabelOutcome {
    /// No observation shows the receiver's decision yet.
    Unobserved,
    /// The receiver admitted the frame.
    Admitted,
    /// The receiver refused the frame or closed the connection for it.
    Refused,
}

/// One frame a role model labelled, for [`CheckEvent::Label`].
///
/// [`CheckEvent::Label`]: crate::property::CheckEvent::Label
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelledFrame {
    /// The row's family, such as `header-sync`.
    pub family: &'static str,
    /// The stream version.
    pub version: u16,
    /// The row's name.
    pub row: &'static str,
    /// Which way the frame travels.
    pub direction: Direction,
    /// The model's label.
    pub label: Label,
    /// What the receiver did with it.
    pub outcome: LabelOutcome,
}
