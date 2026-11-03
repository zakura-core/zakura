//! Per-connection memory budgets that reactors derive from their message tables.
//!
//! Each reactor declares a [`ReactorBudget`]: the bytes one connection may make
//! it hold on receive, and the bytes it may queue for that connection's peer.
//! A connection's QUIC receive window is set once, at handshake, to the control
//! stream's share plus the receive shares of the reactors the connection
//! negotiated. The node's receive bound is then `max_connections` times that
//! sum, plus handshakes and socket queues.
//!
//! [`Stream::receive_bound`] derives a reactor's receive share from its
//! layout's rows. A reactor adds the terms its table cannot express, such as
//! block sync's congestion window cap, and checks the result with
//! [`ReactorBudget::assert_frame_cap`].

use std::fmt;

use super::{
    layout::{LayoutError, Rows},
    MessageRole, MessageRule,
};
use crate::zakura::Stream;

#[cfg(test)]
mod tests;

/// `?` for a `const fn`, where the `Try` trait is not available.
macro_rules! ok {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(error) => return Err(error),
        }
    };
}

/// One reactor's memory share on one connection, in bytes.
///
/// `receive` bounds the payload bytes a conformant peer may have sent the
/// reactor that the node has not released. `output` bounds the encoded bytes
/// the reactor may queue for the peer. Node totals are checked sums and
/// products of these values.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct ReactorBudget {
    /// Received payload bytes held per connection.
    pub receive: u64,
    /// Queued output bytes per connection.
    pub output: u64,
}

impl ReactorBudget {
    /// A budget that holds nothing.
    pub const ZERO: Self = Self {
        receive: 0,
        output: 0,
    };

    /// This budget plus `other`, or `None` if either field overflows.
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        let Some(receive) = self.receive.checked_add(other.receive) else {
            return None;
        };
        let Some(output) = self.output.checked_add(other.output) else {
            return None;
        };
        Some(Self { receive, output })
    }

    /// The sum of `budgets`, or `None` if either field overflows.
    pub const fn checked_sum(budgets: &[Self]) -> Option<Self> {
        let mut total = Self::ZERO;
        let mut index = 0;
        while index < budgets.len() {
            let Some(sum) = total.checked_add(budgets[index]) else {
                return None;
            };
            total = sum;
            index += 1;
        }
        Some(total)
    }

    /// This budget for `connections` connections, or `None` if either field
    /// overflows.
    pub const fn checked_mul(self, connections: usize) -> Option<Self> {
        // Widening usize to u64 is lossless on every supported target.
        let connections = connections as u64;
        let Some(receive) = self.receive.checked_mul(connections) else {
            return None;
        };
        let Some(output) = self.output.checked_mul(connections) else {
            return None;
        };
        Some(Self { receive, output })
    }

    /// This budget, if its receive share holds one frame of `frame_cap` bytes.
    ///
    /// A share below the frame cap could refuse a frame that the reactor
    /// waits for, even with nothing else held.
    pub const fn check_frame_cap(self, frame_cap: u32) -> Result<Self, BudgetError> {
        // Widening u32 to u64 is lossless.
        if self.receive < frame_cap as u64 {
            return Err(BudgetError::BelowFrameCap {
                receive: self.receive,
                frame_cap,
            });
        }
        Ok(self)
    }

    /// This budget, or a panic if [`ReactorBudget::check_frame_cap`] fails. In
    /// a `const` item, the panic fails the build:
    ///
    /// ```compile_fail,E0080
    /// # use zakura_network::zakura::ReactorBudget;
    /// const _: ReactorBudget = ReactorBudget {
    ///     receive: 1023,
    ///     output: 0,
    /// }
    /// .assert_frame_cap(1024);
    /// ```
    ///
    /// # Panics
    ///
    /// If the receive share is below `frame_cap`.
    pub const fn assert_frame_cap(self, frame_cap: u32) -> Self {
        match self.check_frame_cap(frame_cap) {
            Ok(budget) => budget,
            Err(error) => panic!("{}", error.rule()),
        }
    }
}

/// The side of a layout's exchanges that this node takes on a connection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LocalRole {
    /// This node sends requests and opens subscriptions, and the peer serves.
    Requester,
    /// This node serves the peer's requests and subscriptions.
    Responder,
    /// This node does both, as on a symmetric session.
    Both,
}

impl LocalRole {
    const fn requests(self) -> bool {
        matches!(self, Self::Requester | Self::Both)
    }

    const fn serves(self) -> bool {
        matches!(self, Self::Responder | Self::Both)
    }
}

/// A reason a budget has no finite bound or breaks its frame cap.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BudgetError {
    /// The layout breaks a rule of [`Stream::check_layout`].
    Layout(LayoutError),
    /// The layout declares no message tables, so it bounds no message.
    NoTables,
    /// This node sends a request whose exchange may carry responses that do
    /// not end it. The table does not bound how many arrive.
    UnboundedResponses {
        /// The request row.
        message_type: u16,
    },
    /// This node publishes a subscription whose row declares no cadence. The
    /// table does not bound how many updates arrive.
    UnboundedUpdates {
        /// The subscription row.
        message_type: u16,
    },
    /// A row's share or the layout's sum overflows `u64`.
    Overflow {
        /// The row whose share overflowed or made the sum overflow.
        message_type: u16,
    },
    /// The receive share cannot hold one frame.
    BelowFrameCap {
        /// The receive share.
        receive: u64,
        /// The frame cap it must hold.
        frame_cap: u32,
    },
}

impl BudgetError {
    /// The broken rule, without the offending values.
    pub const fn rule(self) -> &'static str {
        match self {
            Self::Layout(error) => error.rule(),
            Self::NoTables => "a receive bound needs message tables",
            Self::UnboundedResponses { .. } => {
                "a request this node sends needs responses that each end the exchange"
            }
            Self::UnboundedUpdates { .. } => {
                "a subscription this node publishes needs an update cadence"
            }
            Self::Overflow { .. } => "a receive bound fits in u64",
            Self::BelowFrameCap { .. } => "a receive share holds one frame of its frame cap",
        }
    }
}

impl fmt::Display for BudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Layout(error) => fmt::Display::fmt(&error, f),
            Self::NoTables => f.write_str(self.rule()),
            Self::UnboundedResponses { message_type }
            | Self::UnboundedUpdates { message_type }
            | Self::Overflow { message_type } => {
                write!(f, "{} (message type {message_type})", self.rule())
            }
            Self::BelowFrameCap { receive, frame_cap } => write!(
                f,
                "{} (receive {receive} bytes, frame cap {frame_cap} bytes)",
                self.rule()
            ),
        }
    }
}

impl std::error::Error for BudgetError {}

impl Stream {
    /// The most payload bytes a conformant peer may send this node over
    /// `layout` on one connection, when this node takes the `local` side.
    ///
    /// Only frames the peer sends count:
    ///
    /// - every announcement row, at its cadence: `capacity × max payload`;
    /// - if this node serves, the peer's requests and subscription updates:
    ///   `max_in_flight × max payload` per request row, and
    ///   `capacity × max payload` of each subscription row's cadence;
    /// - if this node requests, the answers: `max_in_flight × the largest
    ///   response` per request row, and `max_live × (credit bytes + the
    ///   largest response)` per subscription row: the credit plus one more
    ///   page or the terminal outcome.
    ///
    /// Response rows add nothing themselves: their request row counts them.
    /// The bound excludes frame headers, as [`PayloadLen`] does.
    ///
    /// A cadence term counts what a conformant sender queues across a
    /// connection-wide outage of up to [`Cadence::MAX_OUTAGE`]. A longer read
    /// pause on one stream lets the sender keep queuing that row's messages
    /// past the term. QUIC's stream and connection receive windows bound those
    /// bytes, and the peer memory budget charges those windows (CTRL-26), so
    /// the node's memory bound holds. The excess spends connection credit
    /// that sibling rows would otherwise use.
    ///
    /// [`Cadence::MAX_OUTAGE`]: super::Cadence::MAX_OUTAGE
    ///
    /// The result is an error, never a default, when the table does not bound
    /// a count: a request this node sends whose responses need not end the
    /// exchange, or a subscription this node publishes without an update
    /// cadence. The reactor then computes that term itself.
    ///
    /// [`PayloadLen`]: super::PayloadLen
    pub const fn receive_bound(layout: &[Stream], local: LocalRole) -> Result<u64, BudgetError> {
        if let Err(error) = Self::check_layout(layout) {
            return Err(BudgetError::Layout(error));
        }
        let mut rows = Rows::new(layout);
        let mut total = None;
        while let Some(row) = rows.next() {
            let share = ok!(row_receive(layout, row, local));
            let sum = match total {
                Some(total) => add(total, share, row.message_type),
                None => Ok(share),
            };
            total = Some(ok!(sum));
        }
        match total {
            Some(total) => Ok(total),
            None => Err(BudgetError::NoTables),
        }
    }
}

/// The bytes the peer may send this node under `row`.
const fn row_receive(
    layout: &[Stream],
    row: MessageRule,
    local: LocalRole,
) -> Result<u64, BudgetError> {
    let message_type = row.message_type;
    let payload = row.payload.max();
    match row.role {
        MessageRole::Announcement { cadence } => times(cadence.capacity, payload, message_type),
        MessageRole::Request { max_in_flight, .. } => {
            let mut share = 0;
            if local.serves() {
                share = ok!(times(max_in_flight, payload, message_type));
            }
            if local.requests() {
                let (largest, open_ended) = responses(layout, message_type);
                if open_ended {
                    return Err(BudgetError::UnboundedResponses { message_type });
                }
                let answers = ok!(times(max_in_flight, largest, message_type));
                share = ok!(add(share, answers, message_type));
            }
            Ok(share)
        }
        MessageRole::Subscription {
            max_live,
            credit,
            cadence,
            ..
        } => {
            let mut share = 0;
            if local.serves() {
                let Some(cadence) = cadence else {
                    return Err(BudgetError::UnboundedUpdates { message_type });
                };
                share = ok!(times(cadence.capacity, payload, message_type));
            }
            if local.requests() {
                let (largest, _) = responses(layout, message_type);
                // Widening u32 to usize is lossless on every supported target.
                let Some(per_live) = largest.checked_add(credit.bytes as usize) else {
                    return Err(BudgetError::Overflow { message_type });
                };
                let pages = ok!(times(max_live, per_live, message_type));
                share = ok!(add(share, pages, message_type));
            }
            Ok(share)
        }
        MessageRole::Response { .. } => Ok(0),
    }
}

/// The largest response payload that answers `request` in `layout`, and
/// whether a response row answers it without ending the exchange.
const fn responses(layout: &[Stream], request: u16) -> (usize, bool) {
    let mut rows = Rows::new(layout);
    let mut largest = 0;
    let mut open_ended = false;
    while let Some(row) = rows.next() {
        if let MessageRole::Response {
            request: answered,
            ends_exchange,
        } = row.role
        {
            if answered == request {
                if row.payload.max() > largest {
                    largest = row.payload.max();
                }
                open_ended |= !ends_exchange;
            }
        }
    }
    (largest, open_ended)
}

/// `count × bytes`, or an overflow of `message_type`'s share.
const fn times(count: u32, bytes: usize, message_type: u16) -> Result<u64, BudgetError> {
    // Widening u32 and usize to u64 is lossless on every supported target.
    match (count as u64).checked_mul(bytes as u64) {
        Some(product) => Ok(product),
        None => Err(BudgetError::Overflow { message_type }),
    }
}

/// `a + b`, or an overflow of `message_type`'s share.
const fn add(a: u64, b: u64, message_type: u16) -> Result<u64, BudgetError> {
    match a.checked_add(b) {
        Some(sum) => Ok(sum),
        None => Err(BudgetError::Overflow { message_type }),
    }
}
