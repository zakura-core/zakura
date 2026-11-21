//! `tx-relay`'s wire: two streams behind one capability bit, and five
//! messages.
//!
//! The data stream carries `Have` and `Tx`, so a peer learns of new
//! transactions only as fast as it drains bodies. The state stream carries
//! `Hello`, `GetTxs` and `NotFound`, so this node's wants never wait behind
//! its uploads. A `GetTxs` is answered on either stream: `Tx` on the data
//! stream, `NotFound` on the state stream.

use std::time::Duration;

use zakura_chain::{
    serialization::ZcashSerialize,
    transaction::{AuthDigest, Hash, UnminedTx, UnminedTxId, WtxId},
};

use crate::zakura::{
    wire_codec::{BoundedReader, LeU16, LeU32, List, Wire, WireError, WireMessage},
    ByteRate, Cadence, Frame, MessageRole, MessageRule, PayloadLen, ReactorBudget, Stream,
    StreamQueueDepths,
};

/// Capability bit for `tx-relay`.
pub const ZAKURA_CAP_TX_RELAY: u64 = 1 << 11;

/// The data stream's kind: `Have` and `Tx`.
pub const TX_RELAY_DATA_KIND: u16 = 9;

/// The state stream's kind: `Hello`, `GetTxs` and `NotFound`.
pub const TX_RELAY_STATE_KIND: u16 = 10;

/// The protocol's largest transaction, in bytes.
pub(crate) const MAX_TX_BYTES: u32 = 2_000_000;

/// Most ids one `Have` carries.
pub const MAX_HAVE_ENTRIES: usize = 1_024;

/// The `Tx` frame that serves `transaction` as `id`, serialized straight
/// into the frame, so the body exists once. `None` if it does not encode
/// within the row's bounds. It is the frame `encode_frame` makes of the
/// same `Tx`.
pub(crate) fn tx_frame(id: UnminedTxId, transaction: &UnminedTx) -> Option<Frame> {
    // The buffer is the frame's exact size, so a body never holds the spare
    // capacity a growing buffer would, which no output share counts.
    let mut payload = Vec::with_capacity(TX_ID_BYTES.saturating_add(transaction.size()));
    let transaction = transaction.transaction();
    TxIdItem::encode(&id, &mut payload).ok()?;
    let start = payload.len();
    transaction.zcash_serialize(&mut payload).ok()?;
    let body = payload.len() - start;
    // A `u32` limit always fits a `usize` on the platforms the node runs on.
    if body == 0 || body > MAX_TX_BYTES as usize {
        return None;
    }
    Some(Frame {
        message_type: message_type::TX,
        flags: 0,
        payload,
    })
}

/// The most payload bytes a `Have` of `entries` ids takes: a count of at
/// most 3 bytes, then 68 bytes for each id and its size.
pub(crate) const fn have_payload_bound(entries: usize) -> usize {
    3 + entries * 68
}

/// The bytes of an exact id on the wire: the mined id, then the
/// authorizing-data digest.
const TX_ID_BYTES: usize = 64;

/// The authorizing-data digest a transaction before v5 has, as ZIP 239
/// defines it for inventory.
const LEGACY_AUTH_DIGEST: [u8; 32] = [0xFF; 32];

/// Frame message types.
mod message_type {
    pub(super) const HELLO: u16 = 1;
    pub(super) const HAVE: u16 = 2;
    pub(super) const GET_TXS: u16 = 3;
    pub(super) const TX: u16 = 4;
    pub(super) const NOT_FOUND: u16 = 5;
}

/// An exact transaction id: its mined id and its authorizing-data digest, or
/// the ZIP 239 placeholder for a transaction before v5.
#[derive(Debug)]
pub(crate) enum TxIdItem {}

impl Wire for TxIdItem {
    type Value = UnminedTxId;
    const MIN_LEN: usize = TX_ID_BYTES;
    const MAX_LEN: usize = TX_ID_BYTES;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &UnminedTxId, out: &mut Vec<u8>) -> Result<(), WireError> {
        out.extend_from_slice(&value.mined_id().0);
        match value.auth_digest() {
            Some(digest) => out.extend_from_slice(&digest.0),
            None => out.extend_from_slice(&LEGACY_AUTH_DIGEST),
        }
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<UnminedTxId, WireError> {
        let id = Hash(reader.array("transaction id")?);
        let digest: [u8; 32] = reader.array("authorizing data digest")?;
        Ok(if digest == LEGACY_AUTH_DIGEST {
            UnminedTxId::from_legacy_id(id)
        } else {
            UnminedTxId::from(WtxId {
                id,
                auth_digest: AuthDigest(digest),
            })
        })
    }
}

/// A transaction's serialized bytes, the rest of the payload.
#[derive(Debug)]
pub(crate) enum TxBody {}

impl Wire for TxBody {
    type Value = Vec<u8>;
    const MIN_LEN: usize = 1;
    // A `u32` limit always fits a `usize` on the platforms the node runs on.
    const MAX_LEN: usize = MAX_TX_BYTES as usize;

    fn max_heap_bytes(input_len: usize) -> usize {
        input_len.min(Self::MAX_LEN)
    }

    fn encode(value: &Vec<u8>, out: &mut Vec<u8>) -> Result<(), WireError> {
        if value.is_empty() || value.len() > Self::MAX_LEN {
            return Err(WireError::OutOfRange("transaction body"));
        }
        out.extend_from_slice(value);
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<Vec<u8>, WireError> {
        let bytes = reader.take_remaining();
        if bytes.is_empty() || bytes.len() > Self::MAX_LEN {
            return Err(WireError::OutOfRange("transaction body"));
        }
        Ok(bytes.to_vec())
    }
}

/// `Hello`: requests the sender serves at once, and its largest transaction.
type HelloPayload = (LeU16, LeU32);
/// One announced id and its serialized size.
type HaveEntry = (TxIdItem, LeU32);
/// `Have`: 1 to 1,024 announced ids.
type HavePayload = List<HaveEntry, 1, MAX_HAVE_ENTRIES>;
/// `Tx`: the requested id, then its body.
type TxPayload = (TxIdItem, TxBody);

/// `Hello`, sent first on the state stream, once. The cadence allows a few
/// in ten minutes, enough for one per session.
const HELLO: MessageRule = MessageRule {
    name: "hello",
    message_type: message_type::HELLO,
    payload: PayloadLen::of::<HelloPayload>(),
    role: MessageRole::Announcement {
        cadence: Cadence {
            capacity: 3,
            refill_interval: Duration::from_secs(300),
            send_interval: Duration::from_secs(600),
        },
    },
};

/// `Have`'s byte rate: 256 KB a second, about 3,800 ids, far above Zcash's
/// transaction rate. A sender opens with 8,000 entries and holds at most
/// 512 KiB of paid frames unwritten. *Provisional.*
pub const HAVE_RATE: ByteRate = ByteRate {
    bytes_per_second: 256_000,
    opening_allowance: 8_000 * 68,
    sender_allowance: 512 * 1024,
};

/// `Have`: charged by payload bytes at [`HAVE_RATE`].
const HAVE: MessageRule = MessageRule {
    name: "have",
    message_type: message_type::HAVE,
    payload: PayloadLen::of::<HavePayload>(),
    role: MessageRole::Metered { rate: HAVE_RATE },
};

/// `GetTxs`: one exact id. A peer may have as many outstanding as this
/// node's `Hello` allows, at most [`super::machine::MAX_SERVES`].
const GET_TXS: MessageRule = MessageRule {
    name: "get-txs",
    message_type: message_type::GET_TXS,
    payload: PayloadLen::of::<TxIdItem>(),
    role: MessageRole::Request {
        max_in_flight: 16,
        cadence: None,
    },
};

/// `Tx`: the body that answers a `GetTxs`.
const TX: MessageRule = MessageRule {
    name: "tx",
    message_type: message_type::TX,
    payload: PayloadLen::of::<TxPayload>(),
    role: MessageRole::Response {
        request: message_type::GET_TXS,
        ends_exchange: true,
    },
};

/// `NotFound`: the answer to a `GetTxs` for an id the sender cannot serve.
const NOT_FOUND: MessageRule = MessageRule {
    name: "not-found",
    message_type: message_type::NOT_FOUND,
    payload: PayloadLen::of::<TxIdItem>(),
    role: MessageRole::Response {
        request: message_type::GET_TXS,
        ends_exchange: true,
    },
};

/// The data stream's frame cap: the largest `Tx` with its id, and room for
/// the frame header.
const DATA_FRAME_CAP: u32 = MAX_TX_BYTES + 4 * 1024;

/// Bytes one session holds for the transport outside its output gate: a
/// served `Tx`, built at its full size before it waits for its output
/// grant, and one unwritten `Have`, whose frame is guarded and skips the
/// gate. A `Tx` is built in a buffer of its exact size. A `Have` is encoded
/// in a buffer that grows by doubling, so it is counted at twice its bound.
/// The node envelope counts them for each connection.
// Widening `u32` and `usize` to `u64` is lossless on supported targets.
pub(crate) const SESSION_HELD_OUTPUT_BYTES: u64 =
    DATA_FRAME_CAP as u64 + 2 * have_payload_bound(MAX_HAVE_ENTRIES) as u64;

/// The state stream's frame cap.
const STATE_FRAME_CAP: u32 = 4 * 1024;

/// The data stream's share of one connection: room for the largest body
/// with space to spare, for both what the peer sends and what this node
/// queues. *Provisional.*
pub(crate) const TX_RELAY_DATA_BUDGET: ReactorBudget = ReactorBudget {
    receive: 4 * 1024 * 1024,
    output: 4 * 1024 * 1024,
}
.assert_frame_cap(DATA_FRAME_CAP);

/// The state stream's share of one connection. Its frames are small and
/// bounded in number by the per-peer request limits. *Provisional.*
pub(crate) const TX_RELAY_STATE_BUDGET: ReactorBudget = ReactorBudget {
    receive: 64 * 1024,
    output: 64 * 1024,
}
.assert_frame_cap(STATE_FRAME_CAP);

/// `tx-relay`'s two streams, admitted together as one session.
pub const TX_RELAY_STREAMS: [Stream; 2] = [
    Stream {
        kind: TX_RELAY_DATA_KIND,
        version: 1,
        frame_cap: DATA_FRAME_CAP,
        capability: ZAKURA_CAP_TX_RELAY,
        messages: Some(&[HAVE, TX]),
        // A small outbound queue keeps paid-for `Have` bytes close to the
        // wire.
        queue_depths: Some(StreamQueueDepths {
            inbound: 16,
            outbound: 4,
        }),
        ..Stream::PERSISTENT
    },
    Stream {
        kind: TX_RELAY_STATE_KIND,
        version: 1,
        frame_cap: STATE_FRAME_CAP,
        capability: ZAKURA_CAP_TX_RELAY,
        messages: Some(&[HELLO, GET_TXS, NOT_FOUND]),
        queue_depths: Some(StreamQueueDepths {
            inbound: 64,
            outbound: 64,
        }),
        ..Stream::PERSISTENT
    },
];

const _: () = Stream::validate_layout(&TX_RELAY_STREAMS);

/// One `tx-relay` message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxRelayMessage {
    /// Requests the sender serves at once, and its largest transaction.
    Hello {
        /// The requests the sender serves at once.
        serves: u16,
        /// The sender's largest transaction, in bytes.
        max_tx_bytes: u32,
    },
    /// Announced ids with their serialized sizes.
    Have(Vec<(UnminedTxId, u32)>),
    /// A request for one exact id.
    GetTxs(UnminedTxId),
    /// The requested id and its body.
    Tx {
        /// The requested id.
        id: UnminedTxId,
        /// The serialized transaction.
        body: Vec<u8>,
    },
    /// The requested id, which the sender cannot serve.
    NotFound(UnminedTxId),
}

impl TxRelayMessage {
    /// The stream kind this message travels on.
    #[cfg(test)]
    pub(crate) fn stream_kind(&self) -> u16 {
        match self {
            Self::Have(_) | Self::Tx { .. } => TX_RELAY_DATA_KIND,
            Self::Hello { .. } | Self::GetTxs(_) | Self::NotFound(_) => TX_RELAY_STATE_KIND,
        }
    }
}

impl WireMessage for TxRelayMessage {
    type Error = WireError;
    const RULES: &'static [MessageRule] = &[HELLO, HAVE, GET_TXS, TX, NOT_FOUND];

    fn message_type(&self) -> u16 {
        match self {
            Self::Hello { .. } => message_type::HELLO,
            Self::Have(_) => message_type::HAVE,
            Self::GetTxs(_) => message_type::GET_TXS,
            Self::Tx { .. } => message_type::TX,
            Self::NotFound(_) => message_type::NOT_FOUND,
        }
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        match self {
            Self::Hello {
                serves,
                max_tx_bytes,
            } => HelloPayload::encode(&(*serves, *max_tx_bytes), out),
            Self::Have(entries) => HavePayload::encode(entries, out),
            Self::GetTxs(id) | Self::NotFound(id) => TxIdItem::encode(id, out),
            Self::Tx { id, body } => {
                TxIdItem::encode(id, out)?;
                TxBody::encode(body, out)
            }
        }
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        Ok(match message_type {
            message_type::HELLO => {
                let (serves, max_tx_bytes) = reader.read::<HelloPayload>()?;
                Self::Hello {
                    serves,
                    max_tx_bytes,
                }
            }
            message_type::HAVE => Self::Have(reader.read::<HavePayload>()?),
            message_type::GET_TXS => Self::GetTxs(reader.read::<TxIdItem>()?),
            message_type::TX => {
                let (id, body) = reader.read::<TxPayload>()?;
                Self::Tx { id, body }
            }
            message_type::NOT_FOUND => Self::NotFound(reader.read::<TxIdItem>()?),
            other => return Err(WireError::UnknownMessageType(other)),
        })
    }

    fn max_heap_bytes(message_type: u16, payload_len: usize) -> usize {
        match message_type {
            message_type::HAVE => HavePayload::max_heap_bytes(payload_len),
            message_type::TX => TxPayload::max_heap_bytes(payload_len),
            _ => 0,
        }
    }
}
