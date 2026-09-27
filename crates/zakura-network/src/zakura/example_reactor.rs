//! A test-only reactor that shows how a reactor declares its messages.
//!
//! It mirrors the hard parts of block sync's `GetBlocks` at a small scale: a
//! request answered by several response frames, two messages that can end an
//! exchange, and a choice between one stream and a stream pair. No production
//! code uses it.
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
//!    function bounds a range's response for both sides.
//! 5. **Tests.** The generated suites run once for the message family and once
//!    for each layout. The reactor writes no bound or header test of its own.

use std::time::Duration;

use zakura_chain::block::Height;

use super::wire_codec::{BoundedReader, HeightLe, LeU32, List, Wire, WireError, WireMessage, U8};
use crate::zakura::{
    Cadence, MessageRole, MessageRule, PayloadLen, Stream, StreamQueueDepths, StreamWritePolicy,
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

/// Frame message types.
mod message_type {
    pub(super) const STATUS: u16 = 1;
    pub(super) const GET_ITEMS: u16 = 2;
    pub(super) const ITEM: u16 = 3;
    pub(super) const ITEMS_DONE: u16 = 4;
    pub(super) const RANGE_UNAVAILABLE: u16 = 5;
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
    messages: Some(&[STATUS, GET_ITEMS, ITEM, ITEMS_DONE, RANGE_UNAVAILABLE]),
    ..EXAMPLE_STREAM
}];

/// A stream pair: requests get their own stream.
///
/// The request stream holds one frame in each direction, and its writes wait
/// until the session is cancelled rather than timing out.
pub(crate) const PAIRED: [Stream; 2] = [
    Stream {
        kind: 900,
        messages: Some(&[STATUS, ITEM, ITEMS_DONE, RANGE_UNAVAILABLE]),
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

/// Every message of the example reactor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExampleMessage {
    Status { low: Height, high: Height },
    GetItems(ItemRange),
    Item { height: Height, bytes: Vec<u8> },
    ItemsDone { start: Height, returned: u32 },
    RangeUnavailable(ItemRange),
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
            _ => Ok(()),
        }
    }
}

impl WireMessage for ExampleMessage {
    type Error = WireError;

    const RULES: &'static [MessageRule] = &[STATUS, GET_ITEMS, ITEM, ITEMS_DONE, RANGE_UNAVAILABLE];

    fn message_type(&self) -> u16 {
        match self {
            Self::Status { .. } => message_type::STATUS,
            Self::GetItems(_) => message_type::GET_ITEMS,
            Self::Item { .. } => message_type::ITEM,
            Self::ItemsDone { .. } => message_type::ITEMS_DONE,
            Self::RangeUnavailable(_) => message_type::RANGE_UNAVAILABLE,
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
            _ => return Err(WireError::UnknownMessageType(message_type)),
        };
        message.check()?;
        Ok(message)
    }

    fn max_heap_bytes(message_type: u16, payload_len: usize) -> usize {
        match message_type {
            message_type::STATUS => StatusPayload::max_heap_bytes(payload_len),
            message_type::ITEM => ItemPayload::max_heap_bytes(payload_len),
            message_type::GET_ITEMS
            | message_type::ITEMS_DONE
            | message_type::RANGE_UNAVAILABLE => RangePayload::max_heap_bytes(payload_len),
            _ => 0,
        }
    }
}
