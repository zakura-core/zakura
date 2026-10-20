//! A small message family for the regulation tools' tests.
//!
//! `Get` asks for parts; `Part` answers it; `Done` or `Failed` ends it.
//! `Status` is an announcement and `Ping` a request with a cadence; `Pong`
//! ends a `Ping`.

use std::time::Duration;

use crate::zakura::{
    wire_codec::{BoundedReader, LeU32, List, Wire, WireError, WireMessage, U8},
    Cadence, Frame, MessageRole, MessageRule, PayloadLen,
};

pub(crate) mod message_type {
    pub(crate) const GET: u16 = 1;
    pub(crate) const PART: u16 = 2;
    pub(crate) const DONE: u16 = 3;
    pub(crate) const FAILED: u16 = 4;
    pub(crate) const STATUS: u16 = 5;
    pub(crate) const PING: u16 = 6;
    pub(crate) const PONG: u16 = 7;
}

/// Bytes of one part.
pub(crate) type PartBytes = List<U8, 1, 64>;

/// A sender every 30 seconds, refilled every 15.
pub(crate) const EVERY_30_SECONDS: Cadence = Cadence {
    capacity: 22,
    refill_interval: Duration::from_secs(15),
    send_interval: Duration::from_secs(30),
};

pub(crate) const GET: MessageRule = MessageRule {
    message_type: message_type::GET,
    payload: PayloadLen::of::<LeU32>(),
    role: MessageRole::Request {
        max_in_flight: 4,
        cadence: None,
    },
};

pub(crate) const PART: MessageRule = MessageRule {
    message_type: message_type::PART,
    payload: PayloadLen::of::<PartBytes>(),
    role: MessageRole::Response {
        request: message_type::GET,
        ends_exchange: false,
    },
};

pub(crate) const DONE: MessageRule = MessageRule {
    message_type: message_type::DONE,
    payload: PayloadLen::of::<LeU32>(),
    role: MessageRole::Response {
        request: message_type::GET,
        ends_exchange: true,
    },
};

pub(crate) const FAILED: MessageRule = MessageRule {
    message_type: message_type::FAILED,
    payload: PayloadLen::of::<LeU32>(),
    role: MessageRole::Response {
        request: message_type::GET,
        ends_exchange: true,
    },
};

pub(crate) const STATUS: MessageRule = MessageRule {
    message_type: message_type::STATUS,
    payload: PayloadLen::of::<LeU32>(),
    role: MessageRole::Announcement {
        cadence: EVERY_30_SECONDS,
    },
};

pub(crate) const PING: MessageRule = MessageRule {
    message_type: message_type::PING,
    payload: PayloadLen::of::<LeU32>(),
    role: MessageRole::Request {
        max_in_flight: 2,
        cadence: Some(EVERY_30_SECONDS),
    },
};

pub(crate) const PONG: MessageRule = MessageRule {
    message_type: message_type::PONG,
    payload: PayloadLen::of::<LeU32>(),
    role: MessageRole::Response {
        request: message_type::PING,
        ends_exchange: true,
    },
};

pub(crate) const RULES: &[MessageRule] = &[GET, PART, DONE, FAILED, STATUS, PING, PONG];

/// Every message of the family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Probe {
    Get(u32),
    Part(Vec<u8>),
    Done(u32),
    Failed(u32),
    Status(u32),
    Ping(u32),
    Pong(u32),
}

impl WireMessage for Probe {
    type Error = WireError;

    const RULES: &'static [MessageRule] = RULES;

    fn message_type(&self) -> u16 {
        match self {
            Self::Get(_) => message_type::GET,
            Self::Part(_) => message_type::PART,
            Self::Done(_) => message_type::DONE,
            Self::Failed(_) => message_type::FAILED,
            Self::Status(_) => message_type::STATUS,
            Self::Ping(_) => message_type::PING,
            Self::Pong(_) => message_type::PONG,
        }
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        match self {
            Self::Part(bytes) => PartBytes::encode(bytes, out),
            Self::Get(value)
            | Self::Done(value)
            | Self::Failed(value)
            | Self::Status(value)
            | Self::Ping(value)
            | Self::Pong(value) => LeU32::encode(value, out),
        }
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        Ok(match message_type {
            message_type::PART => Self::Part(reader.read::<PartBytes>()?),
            message_type::GET => Self::Get(reader.read::<LeU32>()?),
            message_type::DONE => Self::Done(reader.read::<LeU32>()?),
            message_type::FAILED => Self::Failed(reader.read::<LeU32>()?),
            message_type::STATUS => Self::Status(reader.read::<LeU32>()?),
            message_type::PING => Self::Ping(reader.read::<LeU32>()?),
            message_type::PONG => Self::Pong(reader.read::<LeU32>()?),
            _ => return Err(WireError::UnknownMessageType(message_type)),
        })
    }

    fn max_heap_bytes(message_type: u16, payload_len: usize) -> usize {
        match message_type {
            message_type::PART => PartBytes::max_heap_bytes(payload_len),
            _ => 0,
        }
    }
}

/// Decode a frame of the family.
pub(crate) fn decode(frame: &Frame) -> Probe {
    crate::zakura::wire_codec::decode_frame(frame).expect("the tools only emit valid frames")
}
