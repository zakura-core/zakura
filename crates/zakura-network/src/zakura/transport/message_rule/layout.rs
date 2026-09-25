//! Checks for the message tables of one stream layout.
//!
//! A layout is the set of streams that one exchange can span: a
//! request/response stream on its own, or the persistent streams of one
//! service session. A session's streams share one capability, and the
//! transport admits and retires them together.
//!
//! A reactor checks each layout it declares with [`Stream::validate_layout`] in
//! a `const` item, so a bad table fails the build. The registry runs the same
//! [`Stream::check_layout`] on every registered layout at startup, so a table
//! that skipped the `const` check still never reaches a reader.

use std::fmt;

use super::{Cadence, MessageRole, MessageRule};
use crate::zakura::{transport::StreamMode, Stream, FRAME_HEADER_BYTES};

/// A rule that a layout's message tables break.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LayoutError {
    /// The layout has no streams.
    Empty,
    /// A request/response stream shares its layout with another stream.
    RequestResponseNotAlone {
        /// The request/response stream.
        kind: u16,
    },
    /// Members of a session declare different capabilities.
    MixedCapabilities {
        /// The first member whose capability differs from the first stream's.
        kind: u16,
    },
    /// Two members share a stream kind.
    DuplicateKind {
        /// The repeated kind.
        kind: u16,
    },
    /// Some members declare a message table and others do not.
    PartialTables {
        /// The first member whose choice differs from the first stream's.
        kind: u16,
    },
    /// A stream declares a table with no rows, so it accepts no message.
    EmptyTable {
        /// The stream with the empty table.
        kind: u16,
    },
    /// A message type appears in more than one row of the layout.
    DuplicateMessageType {
        /// The repeated message type.
        message_type: u16,
    },
    /// A row's largest payload does not fit its stream's frame cap.
    PayloadAboveFrameCap {
        /// The stream carrying the row.
        kind: u16,
        /// The row's message type.
        message_type: u16,
    },
    /// An announcement sits on a request/response stream.
    AnnouncementOnRequestResponse {
        /// The request/response stream.
        kind: u16,
        /// The announcement's message type.
        message_type: u16,
    },
    /// A request allows no exchange in flight, so a peer can never send it.
    NoRequestsInFlight {
        /// The request's message type.
        message_type: u16,
    },
    /// A cadence admits no message or never refills.
    EmptyCadence {
        /// The row's message type.
        message_type: u16,
    },
    /// A cadence refills no faster than its sender sends, so a conformant
    /// sender could empty the bucket.
    RefillNotFasterThanSender {
        /// The row's message type.
        message_type: u16,
    },
    /// A cadence's capacity is below [`Cadence::min_capacity`], so a burst
    /// after an outage could empty the bucket.
    CapacityBelowStall {
        /// The row's message type.
        message_type: u16,
    },
    /// A response names a message type that is not a request row in the layout.
    ResponseWithoutRequest {
        /// The response's message type.
        message_type: u16,
    },
    /// No response row ends a request's exchange.
    RequestWithoutEnding {
        /// The request's message type.
        message_type: u16,
    },
}

impl LayoutError {
    /// The broken rule, without the offending values.
    ///
    /// [`Stream::validate_layout`] reports this text as its build error.
    pub const fn rule(self) -> &'static str {
        match self {
            Self::Empty => "a layout needs at least one stream",
            Self::RequestResponseNotAlone { .. } => {
                "a request/response stream must form a layout on its own"
            }
            Self::MixedCapabilities { .. } => "every stream of a session shares one capability",
            Self::DuplicateKind { .. } => "every stream of a layout needs its own kind",
            Self::PartialTables { .. } => {
                "every stream of a layout declares a message table, or none does"
            }
            Self::EmptyTable { .. } => "a message table needs at least one row",
            Self::DuplicateMessageType { .. } => {
                "each message type appears in exactly one row of its layout"
            }
            Self::PayloadAboveFrameCap { .. } => "each row's largest frame fits its stream's cap",
            Self::AnnouncementOnRequestResponse { .. } => {
                "a request/response stream carries only requests and responses"
            }
            Self::NoRequestsInFlight { .. } => {
                "each request allows at least one exchange in flight"
            }
            Self::EmptyCadence { .. } => "each cadence admits a message and refills",
            Self::RefillNotFasterThanSender { .. } => {
                "each cadence refills faster than its sender sends"
            }
            Self::CapacityBelowStall { .. } => {
                "each cadence holds every message a conformant sender queues during an outage"
            }
            Self::ResponseWithoutRequest { .. } => {
                "each response answers a request row in its layout"
            }
            Self::RequestWithoutEnding { .. } => "each request has a response row that ends it",
        }
    }
}

impl fmt::Display for LayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.rule())?;
        match *self {
            Self::Empty => Ok(()),
            Self::RequestResponseNotAlone { kind }
            | Self::MixedCapabilities { kind }
            | Self::DuplicateKind { kind }
            | Self::PartialTables { kind }
            | Self::EmptyTable { kind } => write!(f, " (stream kind {kind})"),
            Self::DuplicateMessageType { message_type }
            | Self::NoRequestsInFlight { message_type }
            | Self::EmptyCadence { message_type }
            | Self::RefillNotFasterThanSender { message_type }
            | Self::CapacityBelowStall { message_type }
            | Self::ResponseWithoutRequest { message_type }
            | Self::RequestWithoutEnding { message_type } => {
                write!(f, " (message type {message_type})")
            }
            Self::PayloadAboveFrameCap { kind, message_type }
            | Self::AnnouncementOnRequestResponse { kind, message_type } => {
                write!(f, " (stream kind {kind}, message type {message_type})")
            }
        }
    }
}

impl std::error::Error for LayoutError {}

impl Stream {
    /// Fail the build if `layout`'s message tables break a rule of
    /// [`Stream::check_layout`].
    ///
    /// Call it once for each layout a reactor declares:
    ///
    /// ```
    /// # use zakura_network::zakura::{Cadence, MessageRole, MessageRule, PayloadLen, Stream};
    /// # use std::time::Duration;
    /// const STATUS: MessageRule = MessageRule {
    ///     message_type: 1,
    ///     payload: PayloadLen::exact(8),
    ///     role: MessageRole::Announcement {
    ///         cadence: Cadence {
    ///             capacity: 22,
    ///             refill_interval: Duration::from_secs(15),
    ///             send_interval: Duration::from_secs(30),
    ///         },
    ///     },
    /// };
    /// const EVENTS: [Stream; 1] = [Stream {
    ///     kind: 64,
    ///     version: 1,
    ///     frame_cap: 1024,
    ///     capability: 1 << 16,
    ///     messages: Some(&[STATUS]),
    ///     ..Stream::PERSISTENT
    /// }];
    /// const _: () = Stream::validate_layout(&EVENTS);
    /// ```
    ///
    /// A row that breaks a rule fails to compile. This announcement cannot fit
    /// the stream's 1024-byte frame cap:
    ///
    /// ```compile_fail,E0080
    /// # use zakura_network::zakura::{Cadence, MessageRole, MessageRule, PayloadLen, Stream};
    /// # use std::time::Duration;
    /// const STATUS: MessageRule = MessageRule {
    ///     message_type: 1,
    ///     payload: PayloadLen::exact(1024),
    ///     role: MessageRole::Announcement {
    ///         cadence: Cadence {
    ///             capacity: 22,
    ///             refill_interval: Duration::from_secs(15),
    ///             send_interval: Duration::from_secs(30),
    ///         },
    ///     },
    /// };
    /// const EVENTS: [Stream; 1] = [Stream {
    ///     kind: 64,
    ///     version: 1,
    ///     frame_cap: 1024,
    ///     capability: 1 << 16,
    ///     messages: Some(&[STATUS]),
    ///     ..Stream::PERSISTENT
    /// }];
    /// const _: () = Stream::validate_layout(&EVENTS);
    /// ```
    ///
    /// # Panics
    ///
    /// If [`Stream::check_layout`] returns an error. In a `const` item, the
    /// panic fails the build with the broken rule's [`LayoutError::rule`].
    pub const fn validate_layout(layout: &[Stream]) {
        if let Err(error) = Self::check_layout(layout) {
            panic!("{}", error.rule());
        }
    }

    /// Check the message tables of one layout.
    ///
    /// A layout is one request/response stream, or the persistent streams of
    /// one session. It passes when:
    ///
    /// - its streams form a layout: a request/response stream stands alone,
    ///   and a session's streams have distinct kinds and share one capability;
    /// - every stream declares a message table, or none does; a layout without
    ///   tables passes the remaining rules trivially;
    /// - every table has a row, and each message type appears in exactly one
    ///   row of the layout;
    /// - each row's largest frame fits its stream's frame cap;
    /// - a request/response stream carries no announcement;
    /// - each request allows an exchange in flight;
    /// - each cadence admits a message, refills faster than its sender sends,
    ///   and holds every message a conformant sender queues during an outage
    ///   ([`Cadence`]);
    /// - each response answers a request row of the layout, which may sit on
    ///   another stream, and each request has a response row that ends it.
    pub const fn check_layout(layout: &[Stream]) -> Result<(), LayoutError> {
        let Some(first) = layout.first() else {
            return Err(LayoutError::Empty);
        };
        let mut index = 0;
        while index < layout.len() {
            let stream = &layout[index];
            if matches!(stream.mode, StreamMode::RequestResponse) && layout.len() > 1 {
                return Err(LayoutError::RequestResponseNotAlone { kind: stream.kind });
            }
            if stream.capability != first.capability {
                return Err(LayoutError::MixedCapabilities { kind: stream.kind });
            }
            let mut earlier = 0;
            while earlier < index {
                if layout[earlier].kind == stream.kind {
                    return Err(LayoutError::DuplicateKind { kind: stream.kind });
                }
                earlier += 1;
            }
            if stream.messages.is_some() != first.messages.is_some() {
                return Err(LayoutError::PartialTables { kind: stream.kind });
            }
            index += 1;
        }

        let mut index = 0;
        while index < layout.len() {
            if let Err(error) = check_stream(layout, &layout[index]) {
                return Err(error);
            }
            index += 1;
        }
        Ok(())
    }
}

/// Check every row of one stream against the stream and the whole layout.
const fn check_stream(layout: &[Stream], stream: &Stream) -> Result<(), LayoutError> {
    let Some(rows) = stream.messages else {
        return Ok(());
    };
    if rows.is_empty() {
        return Err(LayoutError::EmptyTable { kind: stream.kind });
    }
    let mut index = 0;
    while index < rows.len() {
        let row = rows[index];
        let message_type = row.message_type;
        if count_rows(layout, message_type) > 1 {
            return Err(LayoutError::DuplicateMessageType { message_type });
        }
        // Widening a u32 to usize is lossless on every supported target.
        let frame_cap = stream.frame_cap as usize;
        if row.payload.max() > frame_cap.saturating_sub(FRAME_HEADER_BYTES) {
            return Err(LayoutError::PayloadAboveFrameCap {
                kind: stream.kind,
                message_type,
            });
        }
        match row.role {
            MessageRole::Announcement { cadence } => {
                if matches!(stream.mode, StreamMode::RequestResponse) {
                    return Err(LayoutError::AnnouncementOnRequestResponse {
                        kind: stream.kind,
                        message_type,
                    });
                }
                if let Err(error) = check_cadence(message_type, cadence) {
                    return Err(error);
                }
            }
            MessageRole::Request {
                max_in_flight,
                cadence,
            } => {
                if max_in_flight == 0 {
                    return Err(LayoutError::NoRequestsInFlight { message_type });
                }
                if let Some(cadence) = cadence {
                    if let Err(error) = check_cadence(message_type, cadence) {
                        return Err(error);
                    }
                }
                if !has_ending(layout, message_type) {
                    return Err(LayoutError::RequestWithoutEnding { message_type });
                }
            }
            MessageRole::Response { request, .. } => {
                if !is_request(layout, request) {
                    return Err(LayoutError::ResponseWithoutRequest { message_type });
                }
            }
        }
        index += 1;
    }
    Ok(())
}

/// Check that a conformant sender can never empty `cadence`'s bucket.
const fn check_cadence(message_type: u16, cadence: Cadence) -> Result<(), LayoutError> {
    if cadence.capacity == 0 || cadence.refill_interval.is_zero() {
        return Err(LayoutError::EmptyCadence { message_type });
    }
    // `Duration`'s ordering is not `const`; compare nanoseconds instead.
    if cadence.refill_interval.as_nanos() >= cadence.send_interval.as_nanos() {
        return Err(LayoutError::RefillNotFasterThanSender { message_type });
    }
    if cadence.capacity < Cadence::min_capacity(cadence.send_interval) {
        return Err(LayoutError::CapacityBelowStall { message_type });
    }
    Ok(())
}

/// Visit every row of every stream in `layout`.
///
/// `const fn` cannot take closures, so the three queries below share this
/// cursor instead of a visitor.
struct Rows<'a> {
    layout: &'a [Stream],
    stream: usize,
    row: usize,
}

impl<'a> Rows<'a> {
    const fn new(layout: &'a [Stream]) -> Self {
        Self {
            layout,
            stream: 0,
            row: 0,
        }
    }

    const fn next(&mut self) -> Option<MessageRule> {
        while self.stream < self.layout.len() {
            if let Some(rows) = self.layout[self.stream].messages {
                if self.row < rows.len() {
                    self.row += 1;
                    return Some(rows[self.row - 1]);
                }
            }
            self.stream += 1;
            self.row = 0;
        }
        None
    }
}

/// Number of rows in `layout` that declare `message_type`.
const fn count_rows(layout: &[Stream], message_type: u16) -> usize {
    let mut rows = Rows::new(layout);
    let mut count = 0;
    while let Some(row) = rows.next() {
        if row.message_type == message_type {
            count += 1;
        }
    }
    count
}

/// Whether `message_type` is a request row in `layout`.
const fn is_request(layout: &[Stream], message_type: u16) -> bool {
    let mut rows = Rows::new(layout);
    while let Some(row) = rows.next() {
        if row.message_type == message_type && matches!(row.role, MessageRole::Request { .. }) {
            return true;
        }
    }
    false
}

/// Whether a response row in `layout` ends the exchange of `request`.
const fn has_ending(layout: &[Stream], request: u16) -> bool {
    let mut rows = Rows::new(layout);
    while let Some(row) = rows.next() {
        if let MessageRole::Response {
            request: answered,
            ends_exchange: true,
        } = row.role
        {
            if answered == request {
                return true;
            }
        }
    }
    false
}
