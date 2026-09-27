//! Message tables: the messages each stream carries and the rules they follow.
//!
//! A service declares one [`MessageRule`] per message type in
//! [`Stream::messages`]. The table is the single source of truth for that
//! stream's messages:
//!
//! - the reader checks each frame header against it before reading the payload
//!   ([`frame_filter`]);
//! - [`Stream::validate_layout`] checks the tables of a whole layout when the
//!   crate builds, and the registry checks them again at startup ([`layout`]);
//! - the message family's codec and the generated test suites read the same
//!   rows, so the header check, the decoder, and the tests cannot disagree.
//!
//! Tables hold values only: numbers, roles, message types, and durations. A bound
//! that depends on a message's contents, such as a response cap chosen by its
//! request, belongs in the codec or the reactor.
//!
//! A stream whose `messages` is `None` keeps the legacy behavior: its reader
//! admits any message type and any flags up to the stream's frame cap.

use std::time::Duration;

#[cfg(doc)]
use super::Stream;

pub(crate) mod frame_filter;
pub(crate) mod layout;

#[cfg(test)]
pub(crate) mod frame_suite;
#[cfg(test)]
mod tests;

/// One message type that a stream accepts.
///
/// ```
/// # use zakura_network::zakura::{MessageRole, MessageRule, PayloadLen};
/// const GET_ITEMS: MessageRule = MessageRule {
///     message_type: 2,
///     payload: PayloadLen::exact(8),
///     role: MessageRole::Request {
///         max_in_flight: 4,
///         cadence: None,
///     },
/// };
/// const ITEMS_DONE: MessageRule = MessageRule {
///     message_type: 4,
///     payload: PayloadLen::exact(8),
///     role: MessageRole::Response {
///         request: GET_ITEMS.message_type,
///         ends_exchange: true,
///     },
/// };
/// ```
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MessageRule {
    /// Frame-header message type.
    pub message_type: u16,
    /// Payload length bounds, checked from the frame header before the payload
    /// is read.
    pub payload: PayloadLen,
    /// The message's protocol role and the limits that role needs.
    pub role: MessageRole,
}

impl MessageRule {
    /// Return the row for `message_type`, if `rules` declares one.
    pub fn find(rules: &[Self], message_type: u16) -> Option<&Self> {
        rules.iter().find(|rule| rule.message_type == message_type)
    }
}

/// A message's role in the peer-message regulation specification.
///
/// Each role carries exactly the limits the specification requires of it, so a
/// row cannot declare a limit that its role does not use.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MessageRole {
    /// Unsolicited state that the sender pushes at its own cadence.
    Announcement {
        /// Receiver-side bucket for each `(peer, message_type)`.
        cadence: Cadence,
    },
    /// A message that asks the receiver to do work and respond.
    Request {
        /// Most exchanges of this request type a peer session may have open.
        ///
        /// This is the protocol maximum. A peer may advertise a lower limit at
        /// runtime.
        max_in_flight: u32,
        /// Receiver-side bucket, for requests that exchange metadata at a
        /// limited rate.
        cadence: Option<Cadence>,
    },
    /// Part of the answer to a request that the receiver sent earlier.
    Response {
        /// Message type of the request this message answers.
        ///
        /// The request row may sit on another stream of the same layout.
        request: u16,
        /// Whether this message is the exchange's final message.
        ///
        /// A request's exchange ends with exactly one ending message. Messages
        /// that do not end it may precede the ending, as bounded by the reactor.
        ends_exchange: bool,
    },
}

/// A message-count token bucket, kept for each `(peer, message_type)`.
///
/// A full bucket holds `capacity` messages. It regains one message every
/// `refill_interval`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Cadence {
    /// Messages a full bucket admits back to back.
    pub capacity: u32,
    /// Time to regain one message.
    pub refill_interval: Duration,
}

/// Inclusive payload length bounds for one message type.
///
/// The bounds exclude the frame header. Every row has a finite maximum, which
/// must equal the codec's largest valid encoding.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PayloadLen {
    min: usize,
    max: usize,
}

impl PayloadLen {
    /// A payload of exactly `len` bytes.
    pub const fn exact(len: usize) -> Self {
        Self { min: len, max: len }
    }

    /// A payload of `min..=max` bytes.
    ///
    /// # Panics
    ///
    /// If `min > max`. In a `const` declaration, this fails the build.
    pub const fn between(min: usize, max: usize) -> Self {
        assert!(min <= max, "a payload minimum must not exceed its maximum");
        Self { min, max }
    }

    /// Smallest valid payload in bytes.
    pub const fn min(self) -> usize {
        self.min
    }

    /// Largest valid payload in bytes.
    pub const fn max(self) -> usize {
        self.max
    }

    /// Whether `len` is within these bounds.
    pub const fn contains(self, len: usize) -> bool {
        self.min <= len && len <= self.max
    }
}
