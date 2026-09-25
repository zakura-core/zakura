//! A test-only reactor that shows how a reactor declares its messages.
//!
//! It mirrors the hard parts of block sync's `GetBlocks` at a small scale: a
//! request answered by several response frames, two messages that can end an
//! exchange, and a choice between one stream and a stream pair. A `Watch`
//! subscription mirrors header sync version 9's push: pages linked by height,
//! renewable credit, and a terminal outcome whose legality depends on its
//! window. No production code uses it.
//!
//! Read it in this order:
//!
//! 1. **Rows.** One constant per message, holding values only.
//! 2. **Layouts.** [`SINGLE`] carries every row on one stream. [`PAIRED`] moves
//!    the request row to its own stream. Serving never waits in the reader, so
//!    both layouts keep responses flowing; the pair remains a layout choice.
//!    Both layouts carry the same rows, and a `const` check validates each one.
//! 3. **Codec.** [`ExampleMessage`] implements [`WireMessage`] as plain
//!    `match`es from message type to payload item.
//! 4. **Exchange.** `exchange` serves ranges through `Serve`, downloads them
//!    through `Reservations`, and paces `Status` through `CadenceSender`. One
//!    function bounds a range's response for both sides. It pushes watched
//!    items through `Publications` and receives them through `Subscriptions`.
//! 5. **Tests.** The generated suites run once for the message family and once
//!    for each layout. The reactor writes no bound or header test of its own.

use std::time::Duration;

use zakura_chain::block::Height;

use super::wire_codec::{BoundedReader, HeightLe, LeU32, List, Wire, WireError, WireMessage, U8};
use crate::zakura::{
    Cadence, Credit, MessageRole, MessageRule, PayloadLen, Stream, StreamQueueDepths,
    StreamWritePolicy,
};

mod exchange;
mod tests;

/// Most items one `GetItems` asks for.
const MAX_ITEMS_PER_REQUEST: u32 = 8;

/// Most bytes one `Item` carries.
const MAX_ITEM_BYTES: usize = 1024;

/// The lowest and highest height the sender serves.
type StatusPayload = (HeightLe, HeightLe);
/// A range's first height and its item count.
type RangePayload = (HeightLe, LeU32);
/// An item's bytes.
type ItemBytes = List<U8, 1, MAX_ITEM_BYTES>;
/// An item's height and its bytes.
type ItemPayload = (HeightLe, ItemBytes);
/// A watch update: operation, watch id, update sequence, acknowledged height,
/// and added item and byte credit.
type WatchPayload = (U8, LeU32, LeU32, HeightLe, LeU32, LeU32);
/// A pushed item: watch id, height, and bytes.
type PushedPayload = (LeU32, HeightLe, ItemBytes);
/// A watch's end: watch id and reason.
type WatchEndedPayload = (LeU32, U8);

/// Pushed items one watch may hold beyond its acknowledged height.
const WATCH_ITEMS: u32 = 16;

/// Frame message types.
mod message_type {
    pub(super) const STATUS: u16 = 1;
    pub(super) const GET_ITEMS: u16 = 2;
    pub(super) const ITEM: u16 = 3;
    pub(super) const ITEMS_DONE: u16 = 4;
    pub(super) const RANGE_UNAVAILABLE: u16 = 5;
    pub(super) const WATCH: u16 = 6;
    pub(super) const PUSHED: u16 = 7;
    pub(super) const WATCH_ENDED: u16 = 8;
}

/// The sender's servable range, sent at most every 30 seconds.
///
/// The receiver refills twice as fast, and holds the 20 messages a sender
/// can queue during a 10-minute outage plus two.
const STATUS: MessageRule = MessageRule {
    message_type: message_type::STATUS,
    payload: PayloadLen::of::<StatusPayload>(),
    role: MessageRole::Announcement {
        cadence: Cadence {
            capacity: 22,
            refill_interval: Duration::from_secs(15),
            send_interval: Duration::from_secs(30),
        },
    },
};

/// Asks for a range of items. A peer may have four ranges in flight.
const GET_ITEMS: MessageRule = MessageRule {
    message_type: message_type::GET_ITEMS,
    payload: PayloadLen::of::<RangePayload>(),
    role: MessageRole::Request {
        max_in_flight: 4,
        cadence: None,
    },
};

/// One item of a range. Items do not end the exchange.
const ITEM: MessageRule = MessageRule {
    message_type: message_type::ITEM,
    payload: PayloadLen::of::<ItemPayload>(),
    role: MessageRole::Response {
        request: GET_ITEMS.message_type,
        ends_exchange: false,
    },
};

/// Ends a range after at least one item.
const ITEMS_DONE: MessageRule = MessageRule {
    message_type: message_type::ITEMS_DONE,
    payload: PayloadLen::of::<RangePayload>(),
    role: MessageRole::Response {
        request: GET_ITEMS.message_type,
        ends_exchange: true,
    },
};

/// Ends a range that the sender cannot serve.
const RANGE_UNAVAILABLE: MessageRule = MessageRule {
    message_type: message_type::RANGE_UNAVAILABLE,
    payload: PayloadLen::of::<RangePayload>(),
    role: MessageRole::Response {
        request: GET_ITEMS.message_type,
        ends_exchange: true,
    },
};

/// Opens, renews, and closes a watch on new items. A peer may hold one live
/// watch with a window of 16 items and their bytes.
const WATCH: MessageRule = MessageRule {
    message_type: message_type::WATCH,
    payload: PayloadLen::of::<WatchPayload>(),
    role: MessageRole::Subscription {
        max_live: 1,
        credit: Credit {
            objects: WATCH_ITEMS,
            // 16 largest pushes are about 16 KiB, well within u32.
            bytes: WATCH_ITEMS * PushedPayload::MAX_LEN as u32,
        },
        cursor_history: WATCH_ITEMS,
        cadence: None,
    },
};

/// One watched item, the next height after the previous one.
const PUSHED: MessageRule = MessageRule {
    message_type: message_type::PUSHED,
    payload: PayloadLen::of::<PushedPayload>(),
    role: MessageRole::Response {
        request: WATCH.message_type,
        ends_exchange: false,
    },
};

/// Ends a watch.
const WATCH_ENDED: MessageRule = MessageRule {
    message_type: message_type::WATCH_ENDED,
    payload: PayloadLen::of::<WatchEndedPayload>(),
    role: MessageRole::Response {
        request: WATCH.message_type,
        ends_exchange: true,
    },
};

/// The fields every example stream shares.
const EXAMPLE_STREAM: Stream = Stream {
    kind: 0,
    version: 1,
    frame_cap: 64 * 1024,
    capability: 1 << 48,
    ..Stream::PERSISTENT
};

/// Every message on one stream.
pub(crate) const SINGLE: [Stream; 1] = [Stream {
    kind: 900,
    messages: Some(&[
        STATUS,
        GET_ITEMS,
        ITEM,
        ITEMS_DONE,
        RANGE_UNAVAILABLE,
        WATCH,
        PUSHED,
        WATCH_ENDED,
    ]),
    ..EXAMPLE_STREAM
}];

/// A stream pair: requests get their own stream. Watch updates stay on the
/// first stream: they change a live exchange, and serving never sees them.
///
/// The request stream holds one frame in each direction, and its writes wait
/// until the session is cancelled rather than timing out.
pub(crate) const PAIRED: [Stream; 2] = [
    Stream {
        kind: 900,
        messages: Some(&[
            STATUS,
            ITEM,
            ITEMS_DONE,
            RANGE_UNAVAILABLE,
            WATCH,
            PUSHED,
            WATCH_ENDED,
        ]),
        ..EXAMPLE_STREAM
    },
    Stream {
        kind: 901,
        // A frame header and one `GetItems`.
        frame_cap: 16,
        queue_depths: Some(StreamQueueDepths {
            inbound: 1,
            outbound: 1,
        }),
        write_policy: StreamWritePolicy::UntilCancelled,
        messages: Some(&[GET_ITEMS]),
        ..EXAMPLE_STREAM
    },
];

const _: () = Stream::validate_layout(&SINGLE);
const _: () = Stream::validate_layout(&PAIRED);

/// A range of items: its first height and its item count.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct ItemRange {
    pub(crate) start: Height,
    pub(crate) count: u32,
}

/// A watch update's operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum WatchOp {
    Open = 0,
    Grant = 1,
    Close = 2,
}

/// One watch update.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct WatchUpdate {
    pub(crate) op: WatchOp,
    pub(crate) id: u32,
    pub(crate) sequence: u32,
    /// The start for `Open`; afterwards the last height the handler accepted.
    pub(crate) acknowledged: Height,
    pub(crate) added: Credit,
}

/// Why a watch ended. Each reason has a window, which the reactor checks.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum EndReason {
    /// The publisher cannot serve the start. Only before the first item.
    Unavailable = 0,
    /// The publisher stopped the watch. Any time.
    Superseded = 1,
    /// The watch ended after the subscriber's `Close`. Only after `Close`.
    Closed = 2,
}

/// Every message of the example reactor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExampleMessage {
    Status {
        low: Height,
        high: Height,
    },
    GetItems(ItemRange),
    Item {
        height: Height,
        bytes: Vec<u8>,
    },
    ItemsDone {
        start: Height,
        returned: u32,
    },
    RangeUnavailable(ItemRange),
    Watch(WatchUpdate),
    Pushed {
        id: u32,
        height: Height,
        bytes: Vec<u8>,
    },
    WatchEnded {
        id: u32,
        reason: EndReason,
    },
}

impl ExampleMessage {
    /// Check the rules that the payload items cannot express.
    fn check(&self) -> Result<(), WireError> {
        match self {
            Self::Status { low, high } if low > high => {
                Err(WireError::OutOfRange("servable range"))
            }
            Self::GetItems(range) | Self::RangeUnavailable(range) => {
                let last = range
                    .count
                    .checked_sub(1)
                    .and_then(|extra| range.start.0.checked_add(extra));
                if range.count > MAX_ITEMS_PER_REQUEST
                    || last.is_none_or(|last| last > Height::MAX.0)
                {
                    return Err(WireError::OutOfRange("item range"));
                }
                Ok(())
            }
            Self::ItemsDone { returned, .. }
                if *returned == 0 || *returned > MAX_ITEMS_PER_REQUEST =>
            {
                Err(WireError::OutOfRange("returned count"))
            }
            Self::Watch(update)
                if update.op == WatchOp::Close
                    && (update.added.objects != 0 || update.added.bytes != 0) =>
            {
                Err(WireError::OutOfRange("close credit"))
            }
            _ => Ok(()),
        }
    }
}

impl WireMessage for ExampleMessage {
    type Error = WireError;

    const RULES: &'static [MessageRule] = &[
        STATUS,
        GET_ITEMS,
        ITEM,
        ITEMS_DONE,
        RANGE_UNAVAILABLE,
        WATCH,
        PUSHED,
        WATCH_ENDED,
    ];

    fn message_type(&self) -> u16 {
        match self {
            Self::Status { .. } => message_type::STATUS,
            Self::GetItems(_) => message_type::GET_ITEMS,
            Self::Item { .. } => message_type::ITEM,
            Self::ItemsDone { .. } => message_type::ITEMS_DONE,
            Self::RangeUnavailable(_) => message_type::RANGE_UNAVAILABLE,
            Self::Watch(_) => message_type::WATCH,
            Self::Pushed { .. } => message_type::PUSHED,
            Self::WatchEnded { .. } => message_type::WATCH_ENDED,
        }
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        self.check()?;
        match self {
            Self::Status { low, high } => StatusPayload::encode(&(*low, *high), out),
            Self::GetItems(range) | Self::RangeUnavailable(range) => {
                RangePayload::encode(&(range.start, range.count), out)
            }
            Self::Item { height, bytes } => {
                HeightLe::encode(height, out)?;
                ItemBytes::encode(bytes, out)
            }
            Self::ItemsDone { start, returned } => RangePayload::encode(&(*start, *returned), out),
            // A fieldless enum's discriminant is its declared `u8` value.
            Self::Watch(update) => WatchPayload::encode(
                &(
                    update.op as u8,
                    update.id,
                    update.sequence,
                    update.acknowledged,
                    update.added.objects,
                    update.added.bytes,
                ),
                out,
            ),
            Self::Pushed { id, height, bytes } => {
                LeU32::encode(id, out)?;
                HeightLe::encode(height, out)?;
                ItemBytes::encode(bytes, out)
            }
            // A fieldless enum's discriminant is its declared `u8` value.
            Self::WatchEnded { id, reason } => {
                WatchEndedPayload::encode(&(*id, *reason as u8), out)
            }
        }
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        let message = match message_type {
            message_type::STATUS => {
                let (low, high) = reader.read::<StatusPayload>()?;
                Self::Status { low, high }
            }
            message_type::GET_ITEMS => {
                let (start, count) = reader.read::<RangePayload>()?;
                Self::GetItems(ItemRange { start, count })
            }
            message_type::ITEM => {
                let (height, bytes) = reader.read::<ItemPayload>()?;
                Self::Item { height, bytes }
            }
            message_type::ITEMS_DONE => {
                let (start, returned) = reader.read::<RangePayload>()?;
                Self::ItemsDone { start, returned }
            }
            message_type::RANGE_UNAVAILABLE => {
                let (start, count) = reader.read::<RangePayload>()?;
                Self::RangeUnavailable(ItemRange { start, count })
            }
            message_type::WATCH => {
                let (op, id, sequence, acknowledged, objects, bytes) =
                    reader.read::<WatchPayload>()?;
                let op = match op {
                    0 => WatchOp::Open,
                    1 => WatchOp::Grant,
                    2 => WatchOp::Close,
                    _ => return Err(WireError::OutOfRange("watch operation")),
                };
                Self::Watch(WatchUpdate {
                    op,
                    id,
                    sequence,
                    acknowledged,
                    added: Credit { objects, bytes },
                })
            }
            message_type::PUSHED => {
                let (id, height, bytes) = reader.read::<PushedPayload>()?;
                Self::Pushed { id, height, bytes }
            }
            message_type::WATCH_ENDED => {
                let (id, reason) = reader.read::<WatchEndedPayload>()?;
                let reason = match reason {
                    0 => EndReason::Unavailable,
                    1 => EndReason::Superseded,
                    2 => EndReason::Closed,
                    _ => return Err(WireError::OutOfRange("end reason")),
                };
                Self::WatchEnded { id, reason }
            }
            _ => return Err(WireError::UnknownMessageType(message_type)),
        };
        message.check()?;
        Ok(message)
    }

    fn max_heap_bytes(message_type: u16, payload_len: usize) -> usize {
        match message_type {
            message_type::STATUS => StatusPayload::max_heap_bytes(payload_len),
            message_type::ITEM => ItemPayload::max_heap_bytes(payload_len),
            message_type::PUSHED => PushedPayload::max_heap_bytes(payload_len),
            message_type::WATCH => WatchPayload::max_heap_bytes(payload_len),
            message_type::WATCH_ENDED => WatchEndedPayload::max_heap_bytes(payload_len),
            message_type::GET_ITEMS
            | message_type::ITEMS_DONE
            | message_type::RANGE_UNAVAILABLE => RangePayload::max_heap_bytes(payload_len),
            _ => 0,
        }
    }
}
