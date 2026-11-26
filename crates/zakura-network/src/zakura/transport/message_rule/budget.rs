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

/// Split of a reactor's per-connection receive bytes into static unsolicited
/// bytes (charged in the connection's base) and request-answered bytes (funded
/// by request leases at request time).
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct ReceiveBudget {
    /// Bytes a peer may send without our request (announcements, metered
    /// pushes, peer requests/updates we serve), plus one maximal frame of slack
    /// on request-answering streams so every reactor always passes
    /// [`ReactorBudget::check_frame_cap`].
    pub unsolicited: u64,
    /// Bytes a peer sends only in answer to our own requests or subscriptions.
    pub per_request: u64,
}

impl ReceiveBudget {
    /// A receive budget that holds nothing.
    pub const ZERO: Self = Self {
        unsolicited: 0,
        per_request: 0,
    };

    /// A receive budget whose bytes are all unsolicited (static base).
    pub const fn unsolicited_only(bytes: u64) -> Self {
        Self {
            unsolicited: bytes,
            per_request: 0,
        }
    }

    /// Total receive bytes: `unsolicited + per_request` (saturating).
    pub const fn total(self) -> u64 {
        self.unsolicited.saturating_add(self.per_request)
    }

    /// Total receive bytes, or `None` on overflow.
    pub const fn checked_total(self) -> Option<u64> {
        self.unsolicited.checked_add(self.per_request)
    }

    /// Component-wise checked addition.
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        let Some(unsolicited) = self.unsolicited.checked_add(other.unsolicited) else {
            return None;
        };
        let Some(per_request) = self.per_request.checked_add(other.per_request) else {
            return None;
        };
        Some(Self {
            unsolicited,
            per_request,
        })
    }

    /// Component-wise saturating addition.
    pub const fn saturating_add(self, other: Self) -> Self {
        Self {
            unsolicited: self.unsolicited.saturating_add(other.unsolicited),
            per_request: self.per_request.saturating_add(other.per_request),
        }
    }

    /// Component-wise checked multiplication by `connections`.
    pub const fn checked_mul(self, connections: usize) -> Option<Self> {
        // Widening usize to u64 is lossless on supported targets.
        let connections = connections as u64;
        let Some(unsolicited) = self.unsolicited.checked_mul(connections) else {
            return None;
        };
        let Some(per_request) = self.per_request.checked_mul(connections) else {
            return None;
        };
        Some(Self {
            unsolicited,
            per_request,
        })
    }
}

/// One reactor's memory share on one connection, in bytes.
///
/// `receive` splits the payload bytes a conformant peer may send the reactor
/// into `unsolicited` (base) and `per_request` (answers to our requests).
/// `output` bounds the encoded bytes the reactor may queue for the peer.
/// Node totals are checked sums and products of these values.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct ReactorBudget {
    /// Received payload bytes held per connection, split into unsolicited and
    /// per-request bytes.
    pub receive: ReceiveBudget,
    /// Queued output bytes per connection.
    pub output: u64,
}

impl ReactorBudget {
    /// A budget that holds nothing.
    pub const ZERO: Self = Self {
        receive: ReceiveBudget::ZERO,
        output: 0,
    };

    /// Total received bytes (`unsolicited + per_request`).
    pub const fn receive_total(self) -> u64 {
        self.receive.total()
    }

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
    #[cfg(test)]
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
        let Some(receive) = self.receive.checked_mul(connections) else {
            return None;
        };
        let Some(output) = self.output.checked_mul(connections as u64) else {
            return None;
        };
        Some(Self { receive, output })
    }

    /// This budget, if its unsolicited receive base holds one frame of
    /// `frame_cap` bytes.
    ///
    /// A base below the frame cap could refuse a frame that the reactor waits
    /// for, even when no request leases are held.
    pub const fn check_frame_cap(self, frame_cap: u32) -> Result<Self, BudgetError> {
        // Widening u32 to u64 is lossless.
        if self.receive.unsolicited < frame_cap as u64 {
            return Err(BudgetError::BelowFrameCap {
                receive: self.receive.unsolicited,
                frame_cap,
            });
        }
        Ok(self)
    }

    /// This budget, or a panic if [`ReactorBudget::check_frame_cap`] fails. In
    /// a `const` item, the panic fails the build:
    ///
    /// ```compile_fail,E0080
    /// # use zakura_network::zakura::{ReactorBudget, ReceiveBudget};
    /// const _: ReactorBudget = ReactorBudget {
    ///     receive: ReceiveBudget::unsolicited_only(1023),
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
    /// `layout` on one connection, split into `unsolicited` and `per_request`
    /// bytes.
    ///
    /// - `unsolicited`: every announcement row (`capacity × max payload`),
    ///   every metered row (`opening_allowance + sender_allowance`), and when
    ///   `local.serves()`, the peer's requests (`max_in_flight × max payload`)
    ///   and paced subscription updates (`capacity × max payload`).
    /// - `per_request`: when `local.requests()`, the answers to our requests
    ///   (`max_in_flight × largest response`) and subscriptions
    ///   (`max_live × (credit bytes + largest response)`).
    ///
    /// Response rows add nothing themselves: their request row counts them.
    /// The bound excludes frame headers, as [`PayloadLen`] does.
    ///
    /// [`PayloadLen`]: super::PayloadLen
    pub const fn receive_bound_split(
        layout: &[Stream],
        local: LocalRole,
    ) -> Result<ReceiveBudget, BudgetError> {
        if let Err(error) = Self::check_layout(layout) {
            return Err(BudgetError::Layout(error));
        }
        let mut rows = Rows::new(layout);
        let mut total: Option<ReceiveBudget> = None;
        while let Some(row) = rows.next() {
            let share = ok!(row_receive_split(layout, row, local));
            let sum = match total {
                Some(total) => match total.checked_add(share) {
                    Some(sum) => Ok(sum),
                    None => Err(BudgetError::Overflow {
                        message_type: row.message_type,
                    }),
                },
                None => Ok(share),
            };
            total = Some(ok!(sum));
        }
        match total {
            Some(total) => Ok(total),
            None => Err(BudgetError::NoTables),
        }
    }

    /// Total receive bound (`unsolicited + per_request`), derived from
    /// [`Self::receive_bound_split`].
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
    pub const fn receive_bound(layout: &[Stream], local: LocalRole) -> Result<u64, BudgetError> {
        let split = ok!(Self::receive_bound_split(layout, local));
        match split.checked_total() {
            Some(total) => Ok(total),
            None => {
                // Find the first row's message type for the overflow error.
                let mut rows = Rows::new(layout);
                let message_type = match rows.next() {
                    Some(row) => row.message_type,
                    None => 0,
                };
                Err(BudgetError::Overflow { message_type })
            }
        }
    }
}

/// The bytes the peer may send this node under `row`, split into unsolicited
/// and per-request bytes.
const fn row_receive_split(
    layout: &[Stream],
    row: MessageRule,
    local: LocalRole,
) -> Result<ReceiveBudget, BudgetError> {
    let message_type = row.message_type;
    let payload = row.payload.max();
    match row.role {
        MessageRole::Announcement { cadence } => Ok(ReceiveBudget::unsolicited_only(ok!(times(
            cadence.capacity,
            payload,
            message_type
        )))),
        // A conformant sender has paid for at most its opening allowance and
        // holds at most its sender allowance unwritten. The stream receive
        // window, which flow control caps, is the connection's, not the row's.
        MessageRole::Metered { rate } => Ok(ReceiveBudget::unsolicited_only(ok!(add(
            // Widening u32 to u64 is lossless.
            rate.opening_allowance as u64,
            rate.sender_allowance as u64,
            message_type,
        )))),
        MessageRole::Request { max_in_flight, .. } => {
            let mut unsolicited = 0;
            let mut per_request = 0;
            if local.serves() {
                unsolicited = ok!(times(max_in_flight, payload, message_type));
            }
            if local.requests() {
                let (largest, open_ended) = responses(layout, message_type);
                if open_ended {
                    return Err(BudgetError::UnboundedResponses { message_type });
                }
                per_request = ok!(times(max_in_flight, largest, message_type));
            }
            Ok(ReceiveBudget {
                unsolicited,
                per_request,
            })
        }
        MessageRole::Subscription {
            max_live,
            credit,
            cadence,
            ..
        } => {
            let mut unsolicited = 0;
            let mut per_request = 0;
            if local.serves() {
                let Some(cadence) = cadence else {
                    return Err(BudgetError::UnboundedUpdates { message_type });
                };
                unsolicited = ok!(times(cadence.capacity, payload, message_type));
            }
            if local.requests() {
                let (largest, _) = responses(layout, message_type);
                // Widening u32 to usize is lossless on every supported target.
                let Some(per_live) = largest.checked_add(credit.bytes as usize) else {
                    return Err(BudgetError::Overflow { message_type });
                };
                per_request = ok!(times(max_live, per_live, message_type));
            }
            Ok(ReceiveBudget {
                unsolicited,
                per_request,
            })
        }
        MessageRole::Response { .. } => Ok(ReceiveBudget::ZERO),
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
