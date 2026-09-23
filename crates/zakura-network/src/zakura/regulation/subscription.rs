//! Subscriptions: one request that opens a bounded stream of pushed pages.
//!
//! A subscription row ([`MessageRole::Subscription`]) carries three updates:
//! `Open`, `Grant`, and `Close`. The subscriber grants object and byte credit.
//! The publisher pushes pages while credit lasts, then ends the subscription
//! with one terminal outcome. Two tools hold the state, one for each side of
//! a session:
//!
//! - [`Subscriptions`], the subscriber's side, records each update before it
//!   is written and admits each page and terminal outcome;
//! - [`Publications`], the publisher's side, checks each update and reserves
//!   credit and a cursor for each page before it is produced.
//!
//! Both read their limits from the row, and both call the same window check,
//! so a conformant subscriber never sends an update that the publisher
//! refuses.
//!
//! # The credit window
//!
//! Granted credit minus acknowledged progress never exceeds the row's
//! [`Credit`], in objects and in bytes. Pages in flight count the same on both
//! sides: they are unspent credit to one side and unacknowledged pages to the
//! other, and the window counts both. So a grant that breaks the window is a
//! protocol violation.
//!
//! Every page spends at least one object, so the publisher never holds more
//! unacknowledged pages than `credit.objects`. The validator requires
//! `cursor_history >= credit.objects`, so the cursor history never fills.
//!
//! # Slots and the margin
//!
//! A subscription holds a slot from `Open` until its terminal outcome's write
//! finishes. The slot is also the terminal outcome's reserved output, so
//! spent credit and busy output cannot prevent closure. A subscriber must
//! receive a terminal outcome before an `Open` beyond `max_live`, so a
//! conformant subscriber holds at most `max_live` slots on the publisher's
//! tally when its `Open` arrives.
//!
//! As with serving, the tools act only above a proven margin. A slot returns
//! after its terminal's write, which precedes the peer's receipt, so even
//! counting terminals in transit a conformant subscriber holds at most
//! `2 × max_live` slots. An `Open` beyond `max_live` within that is admitted
//! and counted in `zakura.p2p.subscription.over_limit`. One beyond it is a
//! protocol violation.
//!
//! # Tombstones
//!
//! An ended subscription leaves a tombstone, so updates that crossed its
//! terminal outcome are recognized. A crossed `Grant` is dropped and keeps the
//! tombstone. A crossed `Close` is dropped and consumes it.
//!
//! An `Open` proves the subscriber received some terminal outcomes. The
//! subscriber held fewer than `max_live` subscriptions when it sent `Open`,
//! and each live publication and each tombstone whose terminal outcome it had
//! not received counts among them. The publisher writes terminal outcomes in
//! the order [`Publications::end`] returns them, so the subscriber receives
//! them in that order. An `Open` therefore clears the oldest tombstones until
//! live publications plus tombstones number at most `max_live - 1`. With one
//! live subscription, as in header sync version 9, the next `Open` clears the
//! tombstone. An `Open` that reuses a live or tombstoned key is a protocol
//! violation.
//!
//! # Properties and their tests
//!
//! | Property | Test |
//! | --- | --- |
//! | Grants bound the window; consumption is cumulative; a failed grant changes nothing | `renewable_credit_histories_bound_the_window_and_preserve_consumption`, `credit_grant_overflow_and_limit_failure_are_atomic` |
//! | Pages spend exact credit; pages crossing `Close` are admitted; a terminal spends none | `subscriber_operation_sequences_follow_the_model` |
//! | Each publisher outcome row | `every_publisher_outcome_row`, `publisher_operation_sequences_follow_the_model` |
//! | Crossed updates and outcomes never fault a conformant peer; both ledgers agree | `crossed_updates_never_fault_a_conformant_peer` |
//! | The cursor history never fills; a slow acknowledger never faults | `crossed_updates_never_fault_a_conformant_peer`, `a_slow_acknowledger_is_held_by_the_window` |
//! | A grant beyond the window faults | `a_grant_beyond_the_window_faults` |
//! | An idle subscription holds no execution and needs no outcome | `an_idle_subscription_holds_nothing_and_needs_no_outcome` |
//! | Closure needs no credit and no execution | `close_needs_no_credit_and_no_execution` |
//! | Retiring a session with a live subscription closes the connection | `retirement_with_a_live_subscription_closes_the_connection` |

mod credit;
mod publisher;
mod subscriber;

#[cfg(test)]
mod tests;

use thiserror::Error;

pub(crate) use credit::{within_window, CreditExceeded, ResponseCredit, Totals};
pub(crate) use publisher::{Applied, PageStall, Publications, TerminalPermit};
pub(crate) use subscriber::{
    SharedSubscriptions, SubscribeRefused, SubscriptionEnded, Subscriptions, Update,
};

use crate::zakura::{Credit, MessageRole, MessageRule};

/// The limits of one subscription row.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct SubscriptionLimits {
    /// The subscription row's message type.
    pub(crate) message_type: u16,
    /// Most live or closing subscriptions one session may hold.
    pub(crate) max_live: u32,
    /// The credit window.
    pub(crate) credit: Credit,
    /// Sent pages the publisher remembers.
    pub(crate) cursor_history: u32,
}

impl SubscriptionLimits {
    /// The limits of `row`, if it is a subscription row.
    pub(crate) fn from_rule(row: &MessageRule) -> Option<Self> {
        let MessageRole::Subscription {
            max_live,
            credit,
            cursor_history,
            ..
        } = row.role
        else {
            return None;
        };
        Some(Self {
            message_type: row.message_type,
            max_live,
            credit,
            cursor_history,
        })
    }

    fn max_live(self) -> usize {
        usize::try_from(self.max_live).unwrap_or(usize::MAX)
    }
}

/// A peer broke a subscription rule. Each case is a protocol violation:
/// `Disconnect`.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum SubscriptionFault {
    /// A page or terminal outcome for no live subscription, or an update for
    /// no live or tombstoned one.
    #[error("no live subscription matches")]
    Unknown,
    /// A page above the unspent object or byte credit, or with no object.
    #[error("the page exceeds its subscription's credit or carries no object")]
    OverCredit,
    /// An update sequence that is not the previous one plus one, or an `Open`
    /// whose sequence is not zero.
    #[error("update sequence {got} where {expected} was due")]
    Sequence {
        /// The sequence due.
        expected: u32,
        /// The sequence received.
        got: u32,
    },
    /// An acknowledgement that is neither the current one nor a sent page.
    #[error("the acknowledgement names no sent page")]
    UnknownAcknowledgement,
    /// An `Open` or `Grant` that adds no credit, or an `Open` without both
    /// units.
    #[error("the update adds no credit")]
    EmptyCredit,
    /// A grant that would push the window past the row's credit.
    #[error("the grant exceeds the credit window")]
    AboveWindow,
    /// A `Grant` or a second `Close` after `Close`.
    #[error("an update after Close")]
    AfterClose,
    /// An `Open` that reuses a live or tombstoned key.
    #[error("the Open reuses a live or tombstoned key")]
    ReusedKey,
    /// An `Open` beyond twice `max_live` live or closing subscriptions.
    #[error("{held} live or closing subscriptions exceed twice the limit of {limit}")]
    NoSlot {
        /// Slots held when the `Open` arrived.
        held: usize,
        /// The row's `max_live`.
        limit: u32,
    },
}

impl SubscriptionFault {
    /// Stable metric and trace label.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::OverCredit => "over_credit",
            Self::Sequence { .. } => "sequence",
            Self::UnknownAcknowledgement => "unknown_acknowledgement",
            Self::EmptyCredit => "empty_credit",
            Self::AboveWindow => "above_window",
            Self::AfterClose => "after_close",
            Self::ReusedKey => "reused_key",
            Self::NoSlot { .. } => "no_slot",
        }
    }

    fn counted(self, message_type: u16) -> Self {
        metrics::counter!(
            "zakura.p2p.subscription.fault",
            "message_type" => message_type.to_string(),
            "reason" => self.label(),
        )
        .increment(1);
        self
    }
}

fn is_empty(credit: Credit) -> bool {
    credit.objects == 0 && credit.bytes == 0
}
