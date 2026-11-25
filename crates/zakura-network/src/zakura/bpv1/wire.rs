//! BPv1 wire format, message tables, `WireMessage` codecs, and shared FIFO
//! stream writer (decision-6, decision-7, decision-13, decision-14).
//!
//! BPv1 carries seven messages across two persistent ordered streams under one
//! capability bit (`ZAKURA_CAP_BPV1 = 1 << 12`):
//! - Control stream (`BPV1_CONTROL_KIND = 11`): `HeaderMeta`, `Want`, `StripeDone`, `BlockDone`
//! - Data stream (`BPV1_DATA_KIND = 12`): `Part`, `Have`, `WantEnd`

use std::{collections::VecDeque, time::Duration};

use zakura_chain::block;
use zakura_quic::NodeId;

use super::{
    commitment::CoinbaseBinding,
    profile::{
        Coding, CODING_BYTES, W2_MAX_COINBASE_BYTES, W2_MAX_COMMITMENT_PATH_DEPTH,
        W2_MAX_HAVE_INDICES, W2_MAX_HEADER_BYTES, W2_MAX_N, W2_MAX_PROOF_DEPTH, W2_S_MAX,
    },
    tree::VerifiedPart,
};
use crate::zakura::{
    wire_codec::{
        encode_frame, BlockHash, BoundedReader, LeU16, LeU32, List, Wire, WireError, WireMessage,
        U8,
    },
    Cadence, Frame, MessageRole, MessageRule, PayloadLen, ReactorBudget, Stream, StreamQueueDepths,
    FRAME_HEADER_BYTES,
};

/// Capability bit for BPv1 block propagation.
pub const ZAKURA_CAP_BPV1: u64 = 1 << 12;

/// Control stream kind: `HeaderMeta`, `Want`, `StripeDone`, `BlockDone`.
pub const BPV1_CONTROL_KIND: u16 = 11;

/// Data stream kind: `Part`, `Have`, `WantEnd`.
pub const BPV1_DATA_KIND: u16 = 12;

/// Current stream version for both BPv1 streams.
pub const BPV1_STREAM_VERSION: u16 = 1;

/// Protocol maximum live `Want`s per connection direction; server closes only above 2×.
pub const MAX_WANTS_INFLIGHT: u32 = 64;

/// Propagation window depth in blocks below the highest admitted height.
pub const PROPAGATION_DEPTH: u32 = 16;

/// Fixed byte length of a `Want` exact-index bitmap (`ceil(MAX_N / 8) = 11`).
pub const WANT_BITMAP_BYTES: usize = 11;

/// Frame discriminators, numbered as in the BPv1 spec.
pub mod message_type {
    /// `HeaderMeta` (control, Announcement).
    pub const HEADER_META: u16 = 1;
    /// `Want` (control, Request).
    pub const WANT: u16 = 4;
    /// `StripeDone` (control, Announcement).
    pub const STRIPE_DONE: u16 = 5;
    /// `BlockDone` (control, Announcement).
    pub const BLOCK_DONE: u16 = 6;
    /// `Part` (data, Item answering `Want` or a push flag).
    pub const PART: u16 = 8;
    /// `Have` (data, Announcement).
    pub const HAVE: u16 = 9;
    /// `WantEnd` (data, Response ending `Want`).
    pub const WANT_END: u16 = 11;
}

/// Exact-index 11-byte bitmap carried by `Want` (decision-14).
///
/// Bit `j mod 8` of byte `j / 8` requests stripe-relative index `j` (`0..MAX_N`).
/// Bits at or above `W2_MAX_N` (86) must be zero, and the bitmap must be non-empty.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct WantBitmap([u8; WANT_BITMAP_BYTES]);

impl WantBitmap {
    /// Empty bitmap (invalid on the wire; populate before sending).
    pub const EMPTY: Self = Self([0u8; WANT_BITMAP_BYTES]);

    /// Constructs a bitmap from raw bytes, rejecting empty bitmaps or set bits
    /// at or above [`W2_MAX_N`].
    pub fn from_bytes(bytes: [u8; WANT_BITMAP_BYTES]) -> Result<Self, WireError> {
        // Byte 10 holds bits 80..87; bits 86 and 87 (0xC0) are above MAX_N (86).
        if bytes[10] & 0b1100_0000 != 0 {
            return Err(WireError::OutOfRange("want bitmap high bits"));
        }
        let bitmap = Self(bytes);
        if bitmap.is_empty() {
            return Err(WireError::OutOfRange("empty want bitmap"));
        }
        Ok(bitmap)
    }

    /// Sets stripe-relative index `slot` (`0..W2_MAX_N`). Returns `false` if out of range.
    pub fn set(&mut self, slot: u32) -> bool {
        if slot >= W2_MAX_N {
            return false;
        }
        let byte = (slot / 8) as usize;
        let bit = (slot % 8) as u8;
        self.0[byte] |= 1u8 << bit;
        true
    }

    /// Whether stripe-relative index `slot` is set.
    pub const fn contains(&self, slot: u32) -> bool {
        if slot >= W2_MAX_N {
            return false;
        }
        let byte = (slot / 8) as usize;
        let bit = (slot % 8) as u8;
        (self.0[byte] & (1u8 << bit)) != 0
    }

    /// Number of requested indices (the maximum parts the server may send).
    pub fn count(&self) -> u16 {
        // Each byte has at most 8 set bits, so `count_ones()` fits in u16.
        self.0.iter().map(|b| b.count_ones() as u16).sum()
    }

    /// Whether no bits are set.
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|&b| b == 0)
    }

    /// Whether `self` and `other` share any requested bit.
    pub fn intersects(&self, other: &Self) -> bool {
        self.0.iter().zip(other.0.iter()).any(|(a, b)| (a & b) != 0)
    }

    /// Whether every set bit is strictly below `n`.
    pub fn all_below(&self, n: u32) -> bool {
        self.iter().all(|slot| slot < n)
    }

    /// Iterates set stripe-relative slots in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        (0..W2_MAX_N).filter(|&slot| self.contains(slot))
    }
}

/// Terminal reason carried by `WantEnd`.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum WantEndReason {
    /// Server sent the requested parts it held.
    Served = 0,
    /// Server has not admitted the block.
    UnknownBlock = 1,
    /// Server has retired the block.
    Retired = 2,
}

impl WantEndReason {
    /// Decodes a wire reason byte.
    pub fn from_u8(value: u8) -> Result<Self, WireError> {
        match value {
            0 => Ok(Self::Served),
            1 => Ok(Self::UnknownBlock),
            2 => Ok(Self::Retired),
            _ => Err(WireError::OutOfRange("want end reason")),
        }
    }
}

/// Wire item for a 32-byte raw hash (`[u8; 32]`).
#[derive(Debug)]
pub struct Hash32Item;

impl Wire for Hash32Item {
    type Value = [u8; 32];
    const MIN_LEN: usize = 32;
    const MAX_LEN: usize = 32;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &[u8; 32], out: &mut Vec<u8>) -> Result<(), WireError> {
        out.extend_from_slice(value);
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<[u8; 32], WireError> {
        reader.array("32-byte hash")
    }
}

/// Wire item for a nonzero little-endian `u32` request id (`want_id`).
#[derive(Debug)]
pub struct NonZeroLeU32;

impl Wire for NonZeroLeU32 {
    type Value = u32;
    const MIN_LEN: usize = 4;
    const MAX_LEN: usize = 4;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &u32, out: &mut Vec<u8>) -> Result<(), WireError> {
        if *value == 0 {
            return Err(WireError::OutOfRange("want_id must be nonzero"));
        }
        LeU32::encode(value, out)
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<u32, WireError> {
        match reader.read::<LeU32>()? {
            0 => Err(WireError::OutOfRange("want_id must be nonzero")),
            value => Ok(value),
        }
    }
}

/// Wire item for a 32-byte [`NodeId`].
#[derive(Debug)]
pub struct NodeIdItem;

impl Wire for NodeIdItem {
    type Value = NodeId;
    const MIN_LEN: usize = 32;
    const MAX_LEN: usize = 32;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &NodeId, out: &mut Vec<u8>) -> Result<(), WireError> {
        out.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<NodeId, WireError> {
        let bytes = reader.array("node id")?;
        Ok(NodeId::from_bytes(&bytes))
    }
}

/// `HeaderMeta` flag bit 0: `proposer_node` follows the flags byte. The
/// sender clears it exactly when the coinbase's miner mesh output names the
/// proposer (decision-34).
pub const HEADER_META_FLAG_PROPOSER_NODE: u8 = 0b0000_0001;

/// Wire item for `HeaderMeta`'s proposer: a flags byte, then the 32-byte
/// [`NodeId`] when [`HEADER_META_FLAG_PROPOSER_NODE`] is set (decision-34).
/// Any other flags value is reserved, which closes with row 1.
#[derive(Debug)]
pub struct ProposerNodeItem;

impl Wire for ProposerNodeItem {
    type Value = Option<NodeId>;
    const MIN_LEN: usize = 1;
    const MAX_LEN: usize = 1 + 32;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &Option<NodeId>, out: &mut Vec<u8>) -> Result<(), WireError> {
        match value {
            Some(node) => {
                out.push(HEADER_META_FLAG_PROPOSER_NODE);
                NodeIdItem::encode(node, out)
            }
            None => {
                out.push(0);
                Ok(())
            }
        }
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<Option<NodeId>, WireError> {
        match reader.read::<U8>()? {
            0 => Ok(None),
            HEADER_META_FLAG_PROPOSER_NODE => reader.read::<NodeIdItem>().map(Some),
            flags => Err(WireError::ReservedFlags(u16::from(flags))),
        }
    }
}

/// Wire item for a 30-byte [`Coding`] tuple.
#[derive(Debug)]
pub struct CodingItem;

impl Wire for CodingItem {
    type Value = Coding;
    const MIN_LEN: usize = CODING_BYTES;
    const MAX_LEN: usize = CODING_BYTES;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &Coding, out: &mut Vec<u8>) -> Result<(), WireError> {
        out.extend_from_slice(&value.to_bytes());
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<Coding, WireError> {
        let bytes = reader.array::<CODING_BYTES>("coding tuple")?;
        Ok(Coding::from_bytes(&bytes))
    }
}

/// Wire item for [`WantBitmap`].
#[derive(Debug)]
pub struct WantBitmapItem;

impl Wire for WantBitmapItem {
    type Value = WantBitmap;
    const MIN_LEN: usize = WANT_BITMAP_BYTES;
    const MAX_LEN: usize = WANT_BITMAP_BYTES;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &WantBitmap, out: &mut Vec<u8>) -> Result<(), WireError> {
        if value.is_empty() || value.0[10] & 0b1100_0000 != 0 {
            return Err(WireError::OutOfRange("want bitmap"));
        }
        out.extend_from_slice(&value.0);
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<WantBitmap, WireError> {
        let bytes = reader.array::<WANT_BITMAP_BYTES>("want bitmap")?;
        WantBitmap::from_bytes(bytes)
    }
}

/// Wire item for a length-prefixed byte slice (`u16` length, `1..=MAX`).
#[derive(Debug)]
pub struct BytesU16<const MIN: usize, const MAX: usize>;

impl<const MIN: usize, const MAX: usize> Wire for BytesU16<MIN, MAX> {
    type Value = Vec<u8>;
    const MIN_LEN: usize = 2 + MIN;
    const MAX_LEN: usize = 2 + MAX;

    fn max_heap_bytes(input_len: usize) -> usize {
        input_len.saturating_sub(2).min(MAX)
    }

    fn encode(value: &Vec<u8>, out: &mut Vec<u8>) -> Result<(), WireError> {
        if value.len() < MIN || value.len() > MAX {
            return Err(WireError::OutOfRange("u16-prefixed bytes"));
        }
        let len = u16::try_from(value.len()).map_err(|_| WireError::OutOfRange("u16 length"))?;
        LeU16::encode(&len, out)?;
        out.extend_from_slice(value);
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<Vec<u8>, WireError> {
        let len = usize::from(reader.read::<LeU16>()?);
        if len < MIN || len > MAX {
            return Err(WireError::OutOfRange("u16-prefixed bytes"));
        }
        Ok(reader.bytes(len, "u16-prefixed bytes")?.to_vec())
    }
}

/// Index-0 commitment path siblings (`0..=32`). A CompactSize count below
/// `0xfd` is one byte, so this is the specification's `u8` count followed by
/// the hashes.
type CommitmentPath = List<Hash32Item, 0, W2_MAX_COMMITMENT_PATH_DEPTH>;

/// Wire item for [`CoinbaseBinding`]:
/// `u32(coinbase_len) || coinbase || u8(n) || txid_siblings || u8(m) || auth_siblings || light_client_root`.
#[derive(Debug)]
pub struct CoinbaseBindingItem;

impl Wire for CoinbaseBindingItem {
    type Value = CoinbaseBinding;
    const MIN_LEN: usize = 4 + 1 + 2 * CommitmentPath::MIN_LEN + 32;
    const MAX_LEN: usize = 4 + W2_MAX_COINBASE_BYTES + 2 * CommitmentPath::MAX_LEN + 32;

    fn max_heap_bytes(input_len: usize) -> usize {
        input_len
            .min(W2_MAX_COINBASE_BYTES)
            .saturating_add(CommitmentPath::max_heap_bytes(input_len).saturating_mul(2))
    }

    fn encode(value: &CoinbaseBinding, out: &mut Vec<u8>) -> Result<(), WireError> {
        if value.coinbase.is_empty() || value.coinbase.len() > W2_MAX_COINBASE_BYTES {
            return Err(WireError::OutOfRange("coinbase length"));
        }
        let cb_len = u32::try_from(value.coinbase.len())
            .map_err(|_| WireError::OutOfRange("coinbase length"))?;
        LeU32::encode(&cb_len, out)?;
        out.extend_from_slice(&value.coinbase);
        CommitmentPath::encode(&value.txid_siblings, out)?;
        CommitmentPath::encode(&value.auth_siblings, out)?;
        Hash32Item::encode(&value.light_client_root, out)
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<CoinbaseBinding, WireError> {
        let cb_len = usize::try_from(reader.read::<LeU32>()?)
            .map_err(|_| WireError::OutOfRange("coinbase length"))?;
        if cb_len == 0 || cb_len > W2_MAX_COINBASE_BYTES {
            return Err(WireError::OutOfRange("coinbase length"));
        }
        Ok(CoinbaseBinding {
            coinbase: reader.bytes(cb_len, "coinbase")?.to_vec(),
            txid_siblings: reader.read::<CommitmentPath>()?,
            auth_siblings: reader.read::<CommitmentPath>()?,
            light_client_root: reader.read::<Hash32Item>()?,
        })
    }
}

/// Merkle siblings of one part (`0..=16`). A CompactSize count below `0xfd`
/// is one byte, so this is the specification's `u8` count followed by the
/// hashes.
type PartSiblings = List<Hash32Item, 0, W2_MAX_PROOF_DEPTH>;

/// Wire item for a part payload: the rest of the frame, `wire_len(i)` bytes
/// (`0..=S_MAX`, decision-18). Its exact length depends on the admitted
/// coding, so `PeerState` checks it against the frame length (bpv1-66).
#[derive(Debug)]
pub struct PartPayloadItem;

impl Wire for PartPayloadItem {
    type Value = Vec<u8>;
    const MIN_LEN: usize = 0;
    const MAX_LEN: usize = W2_S_MAX as usize;

    fn max_heap_bytes(input_len: usize) -> usize {
        input_len.min(W2_S_MAX as usize)
    }

    fn encode(value: &Vec<u8>, out: &mut Vec<u8>) -> Result<(), WireError> {
        if value.len() > W2_S_MAX as usize {
            return Err(WireError::OutOfRange("part payload length"));
        }
        out.extend_from_slice(value);
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<Vec<u8>, WireError> {
        let len = reader.remaining();
        if len > W2_S_MAX as usize {
            return Err(WireError::OutOfRange("part payload length"));
        }
        Ok(reader.bytes(len, "part payload")?.to_vec())
    }
}

/// Exact `Part` frame payload length for `index` under `coding`:
/// `block_id(32) || index(4) || depth(1) || siblings(32 × depth) || wire_len(index)`
/// (bpv1-66). `None` when `index` is outside the layout.
pub fn part_payload_len(coding: &Coding, index: u32) -> Option<usize> {
    // Widening u32 to usize is lossless on supported targets.
    let wire_len = coding.wire_len(index)? as usize;
    Some(32 + 4 + 1 + 32 * coding.tree_depth() + wire_len)
}

/// Wire item for a `u16`-counted strictly increasing list of `MIN..=MAX`
/// part indices.
#[derive(Debug)]
pub struct IndexListItem<const MIN: usize, const MAX: usize>;

/// `Have` indices (`1..=4,096`).
type HaveIndices = IndexListItem<1, W2_MAX_HAVE_INDICES>;

/// `Have` flag bit 0: the sender pushes the listed parts behind this `Have`
/// on the same data stream (decision-24).
pub const HAVE_FLAG_PUSH: u8 = 0b0000_0001;

/// `Have` flag bit 1: the receiver pushes the listed parts onward as their
/// holder (decision-24). Valid only beside [`HAVE_FLAG_PUSH`], and only from
/// the block's proposer.
pub const HAVE_FLAG_RELAY: u8 = 0b0000_0010;

/// The flags of one `Have` (decision-19, decision-24).
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum HaveFlags {
    /// An announcement: the receiver may pull the listed parts.
    #[default]
    Plain,
    /// The sender pushes the listed parts behind this `Have`.
    Push,
    /// The proposer pushes the listed parts behind this `Have`, and the
    /// receiver pushes them onward as their holder.
    Relay,
}

impl HaveFlags {
    /// Whether the sender pushes the listed parts behind this `Have`.
    pub const fn push(self) -> bool {
        matches!(self, Self::Push | Self::Relay)
    }

    /// Whether the receiver pushes the listed parts onward.
    pub const fn relay(self) -> bool {
        matches!(self, Self::Relay)
    }

    /// The wire byte.
    pub const fn bits(self) -> u8 {
        match self {
            Self::Plain => 0,
            Self::Push => HAVE_FLAG_PUSH,
            Self::Relay => HAVE_FLAG_PUSH | HAVE_FLAG_RELAY,
        }
    }
}

/// Wire item for the `Have` flags byte: `0`, push, or push and relay. Any
/// other value, relay without push included, is a reserved flag, which
/// closes with row 1.
#[derive(Debug)]
pub struct HaveFlagsItem;

impl Wire for HaveFlagsItem {
    type Value = HaveFlags;
    const MIN_LEN: usize = 1;
    const MAX_LEN: usize = 1;

    fn max_heap_bytes(_input_len: usize) -> usize {
        0
    }

    fn encode(value: &HaveFlags, out: &mut Vec<u8>) -> Result<(), WireError> {
        out.push(value.bits());
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<HaveFlags, WireError> {
        let flags = reader.read::<U8>()?;
        [HaveFlags::Plain, HaveFlags::Push, HaveFlags::Relay]
            .into_iter()
            .find(|f| f.bits() == flags)
            .ok_or(WireError::ReservedFlags(u16::from(flags)))
    }
}

impl<const MIN: usize, const MAX: usize> Wire for IndexListItem<MIN, MAX> {
    type Value = Vec<u32>;
    const MIN_LEN: usize = 2 + MIN * 4;
    const MAX_LEN: usize = 2 + MAX * 4;

    fn max_heap_bytes(input_len: usize) -> usize {
        input_len.saturating_sub(2).min(MAX * 4)
    }

    fn encode(value: &Vec<u32>, out: &mut Vec<u8>) -> Result<(), WireError> {
        if !(MIN..=MAX).contains(&value.len()) {
            return Err(WireError::OutOfRange("index count"));
        }
        if value.windows(2).any(|pair| pair[1] <= pair[0]) {
            return Err(WireError::OutOfRange("indices not strictly increasing"));
        }
        // `MAX` fits `u16` for every list this file declares.
        LeU16::encode(&(value.len() as u16), out)?;
        for &idx in value {
            LeU32::encode(&idx, out)?;
        }
        Ok(())
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<Vec<u32>, WireError> {
        let count = usize::from(reader.read::<LeU16>()?);
        if !(MIN..=MAX).contains(&count) {
            return Err(WireError::OutOfRange("index count"));
        }
        if count.saturating_mul(4) > reader.remaining() {
            return Err(WireError::Truncated("indices"));
        }
        let mut indices = Vec::with_capacity(count);
        for _ in 0..count {
            let idx = reader.read::<LeU32>()?;
            if indices.last().is_some_and(|&prev| idx <= prev) {
                return Err(WireError::OutOfRange("indices not strictly increasing"));
            }
            indices.push(idx);
        }
        Ok(indices)
    }
}

/// Payload type aliases tying each row's [`PayloadLen`] directly to its codec items.
type HeaderMetaPayload = (
    BytesU16<1, W2_MAX_HEADER_BYTES>,
    CoinbaseBindingItem,
    CodingItem,
    Hash32Item,
    ProposerNodeItem,
);

type WantPayload = (NonZeroLeU32, BlockHash, LeU32, WantBitmapItem);
type StripeDonePayload = (BlockHash, LeU32);
type BlockDonePayload = BlockHash;
type PartPayload = (BlockHash, LeU32, PartSiblings, PartPayloadItem);
type HavePayload = (BlockHash, HaveFlagsItem, HaveIndices);
type WantEndPayload = (NonZeroLeU32, LeU16, U8);

/// Longest time a buffered `Have` index waits before `PeerState` flushes it
/// (opt-11). The `Have` row's declared cadence derives from it.
pub const HAVE_FLUSH_INTERVAL: Duration = Duration::from_millis(2);

/// The cadence of an announcement whose sender leaves at least
/// `send_interval` between two messages: the receiver refills twice as fast
/// and holds a connection-wide outage's backlog.
const fn announcement_cadence(send_interval: Duration) -> Cadence {
    Cadence {
        capacity: Cadence::min_capacity(send_interval),
        // Half of a sub-second interval in nanoseconds fits u64.
        refill_interval: Duration::from_nanos((send_interval.as_nanos() / 2) as u64),
        send_interval,
    }
}

/// `HeaderMeta` cadence: one message per admitted block, with no sender
/// timer; the receiver refills 16 messages a second.
const HEADER_META_CADENCE: Cadence = announcement_cadence(Duration::from_millis(125));

/// `Have` cadence, set by the sender's flush timer.
const HAVE_CADENCE: Cadence = announcement_cadence(HAVE_FLUSH_INTERVAL);

/// `StripeDone` / `BlockDone` cadence: each is sent at most once per stripe or
/// block per connection, with no sender timer.
const DONE_CADENCE: Cadence = announcement_cadence(Duration::from_millis(10));

/// `HeaderMeta` message rule.
pub const HEADER_META_RULE: MessageRule = MessageRule {
    name: "header-meta",
    message_type: message_type::HEADER_META,
    payload: PayloadLen::of::<HeaderMetaPayload>(),
    role: MessageRole::Announcement {
        cadence: HEADER_META_CADENCE,
    },
};

/// `Want` message rule (Request, `max_in_flight = 64`).
pub const WANT_RULE: MessageRule = MessageRule {
    name: "want",
    message_type: message_type::WANT,
    payload: PayloadLen::of::<WantPayload>(),
    role: MessageRole::Request {
        max_in_flight: MAX_WANTS_INFLIGHT,
        cadence: None,
    },
};

/// `StripeDone` message rule.
pub const STRIPE_DONE_RULE: MessageRule = MessageRule {
    name: "stripe-done",
    message_type: message_type::STRIPE_DONE,
    payload: PayloadLen::of::<StripeDonePayload>(),
    role: MessageRole::Announcement {
        cadence: DONE_CADENCE,
    },
};

/// `BlockDone` message rule.
pub const BLOCK_DONE_RULE: MessageRule = MessageRule {
    name: "block-done",
    message_type: message_type::BLOCK_DONE,
    payload: PayloadLen::of::<BlockDonePayload>(),
    role: MessageRole::Announcement {
        cadence: DONE_CADENCE,
    },
};

/// `Part` message rule: a non-ending `Response` to `Want`, or a push
/// (decision-6, decision-24).
pub const PART_RULE: MessageRule = MessageRule {
    name: "part",
    message_type: message_type::PART,
    payload: PayloadLen::of::<PartPayload>(),
    role: MessageRole::Response {
        request: message_type::WANT,
        ends_exchange: false,
    },
};

/// `Have` message rule.
pub const HAVE_RULE: MessageRule = MessageRule {
    name: "have",
    message_type: message_type::HAVE,
    payload: PayloadLen::of::<HavePayload>(),
    role: MessageRole::Announcement {
        cadence: HAVE_CADENCE,
    },
};

/// `WantEnd` message rule (`Response` ending `Want` on the data stream).
pub const WANT_END_RULE: MessageRule = MessageRule {
    name: "want-end",
    message_type: message_type::WANT_END,
    payload: PayloadLen::of::<WantEndPayload>(),
    role: MessageRole::Response {
        request: message_type::WANT,
        ends_exchange: true,
    },
};

/// Control-stream frame cap derived directly from `HeaderMeta`'s max payload.
pub const CONTROL_FRAME_CAP: u32 = (HEADER_META_RULE.payload.max() + FRAME_HEADER_BYTES) as u32;

/// Data-stream frame cap derived directly from `Part`'s max payload.
pub const DATA_FRAME_CAP: u32 = (PART_RULE.payload.max() + FRAME_HEADER_BYTES) as u32;

/// Per-connection budget for the BPv1 control stream: one maximum
/// `HeaderMeta` frame, whose coinbase may reach `MAX_BLOCK_BYTES`
/// (decision-23), plus 512 KiB for the small control messages beside it.
pub const BPV1_CONTROL_BUDGET: ReactorBudget = ReactorBudget {
    // Widening `u32` to `u64` is lossless.
    receive: CONTROL_FRAME_CAP as u64 + 512 * 1024,
    output: CONTROL_FRAME_CAP as u64 + 512 * 1024,
}
.assert_frame_cap(CONTROL_FRAME_CAP);

/// Per-connection budget for the BPv1 data stream (covers `PULL_WINDOW = 64` parts).
pub const BPV1_DATA_BUDGET: ReactorBudget = ReactorBudget {
    receive: 8 * 1024 * 1024,
    output: 8 * 1024 * 1024,
}
.assert_frame_cap(DATA_FRAME_CAP);

/// BPv1's two persistent ordered streams, admitted together as one session.
pub const BPV1_STREAMS: [Stream; 2] = [
    Stream {
        kind: BPV1_CONTROL_KIND,
        version: BPV1_STREAM_VERSION,
        frame_cap: CONTROL_FRAME_CAP,
        capability: ZAKURA_CAP_BPV1,
        messages: Some(&[
            HEADER_META_RULE,
            WANT_RULE,
            STRIPE_DONE_RULE,
            BLOCK_DONE_RULE,
        ]),
        queue_depths: Some(StreamQueueDepths {
            inbound: 128,
            outbound: 128,
        }),
        ..Stream::PERSISTENT
    },
    Stream {
        kind: BPV1_DATA_KIND,
        version: BPV1_STREAM_VERSION,
        frame_cap: DATA_FRAME_CAP,
        capability: ZAKURA_CAP_BPV1,
        messages: Some(&[PART_RULE, HAVE_RULE, WANT_END_RULE]),
        queue_depths: Some(StreamQueueDepths {
            inbound: 128,
            outbound: 128,
        }),
        ..Stream::PERSISTENT
    },
];

/// Returns `true` if `stream` declares `message_type`.
const fn carries(stream: &Stream, message_type: u16) -> bool {
    let Some(messages) = stream.messages else {
        return false;
    };
    let mut i = 0;
    while i < messages.len() {
        if messages[i].message_type == message_type {
            return true;
        }
        i += 1;
    }
    false
}

/// Compile-time layout validation: every cap fits, all rules are well formed,
/// and `Part` shares its stream with the `WantEnd` that ends its exchange
/// (decision-6).
const _: () = {
    Stream::validate_layout(&BPV1_STREAMS);
    assert!(carries(&BPV1_STREAMS[1], message_type::PART));
    assert!(carries(&BPV1_STREAMS[1], message_type::WANT_END));
    // Exact wire payload bounds sanity checks.
    assert!(WANT_RULE.payload.min() == 51 && WANT_RULE.payload.max() == 51);
    assert!(STRIPE_DONE_RULE.payload.min() == 36 && STRIPE_DONE_RULE.payload.max() == 36);
    assert!(BLOCK_DONE_RULE.payload.min() == 32 && BLOCK_DONE_RULE.payload.max() == 32);
    assert!(WANT_END_RULE.payload.min() == 7 && WANT_END_RULE.payload.max() == 7);
    assert!(HAVE_RULE.payload.min() == 39 && HAVE_RULE.payload.max() == 16_419);
    assert!(PART_RULE.payload.min() == 32 + 4 + 1);
    assert!(PART_RULE.payload.max() == 32 + 4 + 1 + 16 * 32 + 65_536);
};

/// Prefix read from a raw `Part` frame payload before reading proof/payload (bpv1-13).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PartPrefix {
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Wire part index.
    pub index: u32,
}

/// Reads only the 36-byte `(block_id, index)` prefix of a `Part` payload without
/// allocating or touching the proof or 64 KiB payload (bpv1-13).
pub fn read_part_prefix(payload: &[u8]) -> Result<PartPrefix, WireError> {
    let (block_id, index) = BoundedReader::new(payload).read::<(BlockHash, LeU32)>()?;
    Ok(PartPrefix { block_id, index })
}

/// One of the seven BPv1 wire messages (decision-13).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Bpv1Message {
    /// Block header, coinbase commitment binding, coding tuple, `part_root`,
    /// and `proposer_node` unless the coinbase's miner mesh output names the
    /// proposer (decision-34).
    HeaderMeta {
        /// Serialized consensus block header (always full header, decision-2).
        header: Vec<u8>,
        /// Coinbase transaction + txid and auth-data index-0 proofs + light-client root.
        binding: CoinbaseBinding,
        /// Body coding parameters.
        coding: Coding,
        /// Merkle root of all coded parts.
        part_root: [u8; 32],
        /// Transport identity of the proposer. `None` exactly when the
        /// coinbase's miner mesh output names the proposer (decision-34).
        proposer_node: Option<NodeId>,
    },
    /// Exact-index pull request for one stripe (decision-14).
    Want {
        /// Request identifier (`!= 0`, unique among live `Want`s on this connection).
        want_id: u32,
        /// Block consensus hash.
        block_id: block::Hash,
        /// Target stripe index.
        stripe: u32,
        /// Exact stripe-relative indices requested.
        requested: WantBitmap,
    },
    /// Sender holds `k` verified parts of `stripe`.
    StripeDone {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Completed stripe index.
        stripe: u32,
    },
    /// Sender assembled `block_id` (or is the proposer).
    BlockDone {
        /// Block consensus hash.
        block_id: block::Hash,
    },
    /// One coded part and its Merkle inclusion proof.
    Part {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Wire part index.
        index: u32,
        /// Leaf-to-root Merkle siblings.
        siblings: Vec<[u8; 32]>,
        /// The payload's first `wire_len(index)` bytes (decision-18).
        payload: Vec<u8>,
    },
    /// Strictly increasing list of verified part indices held by the sender.
    Have {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Push and relay flags (decision-24).
        flags: HaveFlags,
        /// Strictly increasing wire part indices (`1..=4,096`).
        indices: Vec<u32>,
    },
    /// Terminal response ending `want_id` on the data stream.
    WantEnd {
        /// Request identifier being ended.
        want_id: u32,
        /// Number of parts sent in answer to this `Want`.
        sent: u16,
        /// Reason the `Want` ended.
        reason: WantEndReason,
    },
}

impl WireMessage for Bpv1Message {
    type Error = WireError;
    const RULES: &'static [MessageRule] = &[
        HEADER_META_RULE,
        WANT_RULE,
        STRIPE_DONE_RULE,
        BLOCK_DONE_RULE,
        PART_RULE,
        HAVE_RULE,
        WANT_END_RULE,
    ];

    fn message_type(&self) -> u16 {
        match self {
            Self::HeaderMeta { .. } => message_type::HEADER_META,
            Self::Want { .. } => message_type::WANT,
            Self::StripeDone { .. } => message_type::STRIPE_DONE,
            Self::BlockDone { .. } => message_type::BLOCK_DONE,
            Self::Part { .. } => message_type::PART,
            Self::Have { .. } => message_type::HAVE,
            Self::WantEnd { .. } => message_type::WANT_END,
        }
    }

    // `HeaderMeta`, `Part`, and `Have` write each field through its item from
    // a reference, so a 64 KiB part payload or a coinbase is never copied into
    // an owned tuple first. The items hold every bound.
    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        match self {
            Self::HeaderMeta {
                header,
                binding,
                coding,
                part_root,
                proposer_node,
            } => {
                BytesU16::<1, W2_MAX_HEADER_BYTES>::encode(header, out)?;
                CoinbaseBindingItem::encode(binding, out)?;
                CodingItem::encode(coding, out)?;
                Hash32Item::encode(part_root, out)?;
                ProposerNodeItem::encode(proposer_node, out)
            }
            Self::Want {
                want_id,
                block_id,
                stripe,
                requested,
            } => WantPayload::encode(&(*want_id, *block_id, *stripe, *requested), out),
            Self::StripeDone { block_id, stripe } => {
                StripeDonePayload::encode(&(*block_id, *stripe), out)
            }
            Self::BlockDone { block_id } => BlockDonePayload::encode(block_id, out),
            Self::Part {
                block_id,
                index,
                siblings,
                payload,
            } => {
                BlockHash::encode(block_id, out)?;
                LeU32::encode(index, out)?;
                PartSiblings::encode(siblings, out)?;
                PartPayloadItem::encode(payload, out)
            }
            Self::Have {
                block_id,
                flags,
                indices,
            } => {
                BlockHash::encode(block_id, out)?;
                HaveFlagsItem::encode(flags, out)?;
                HaveIndices::encode(indices, out)
            }
            // A fieldless `repr(u8)` enum's discriminant is its declared value.
            Self::WantEnd {
                want_id,
                sent,
                reason,
            } => WantEndPayload::encode(&(*want_id, *sent, *reason as u8), out),
        }
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, WireError> {
        Ok(match message_type {
            message_type::HEADER_META => {
                let (header, binding, coding, part_root, proposer_node) =
                    reader.read::<HeaderMetaPayload>()?;
                Self::HeaderMeta {
                    header,
                    binding,
                    coding,
                    part_root,
                    proposer_node,
                }
            }
            message_type::WANT => {
                let (want_id, block_id, stripe, requested) = reader.read::<WantPayload>()?;
                Self::Want {
                    want_id,
                    block_id,
                    stripe,
                    requested,
                }
            }
            message_type::STRIPE_DONE => {
                let (block_id, stripe) = reader.read::<StripeDonePayload>()?;
                Self::StripeDone { block_id, stripe }
            }
            message_type::BLOCK_DONE => Self::BlockDone {
                block_id: reader.read::<BlockDonePayload>()?,
            },
            message_type::PART => {
                let (block_id, index, siblings, payload) = reader.read::<PartPayload>()?;
                Self::Part {
                    block_id,
                    index,
                    siblings,
                    payload,
                }
            }
            message_type::HAVE => {
                let (block_id, flags, indices) = reader.read::<HavePayload>()?;
                Self::Have {
                    block_id,
                    flags,
                    indices,
                }
            }
            message_type::WANT_END => {
                let (want_id, sent, reason) = reader.read::<WantEndPayload>()?;
                Self::WantEnd {
                    want_id,
                    sent,
                    reason: WantEndReason::from_u8(reason)?,
                }
            }
            other => return Err(WireError::UnknownMessageType(other)),
        })
    }

    fn max_heap_bytes(message_type: u16, payload_len: usize) -> usize {
        match message_type {
            message_type::HEADER_META => HeaderMetaPayload::max_heap_bytes(payload_len),
            message_type::PART => PartPayload::max_heap_bytes(payload_len),
            message_type::HAVE => HavePayload::max_heap_bytes(payload_len),
            _ => 0,
        }
    }
}

/// An item queued inside [`Bpv1FifoQueue`], preserving strict FIFO order across
/// `Part`s, `Have`s, and `WantEnd`s (decision-7). Items wait unencoded: the
/// writer encodes only the head of the queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataQueueItem {
    /// A part belonging to `(block_id, stripe, index)` that may be cancelled
    /// before dequeue on `StripeDone`, `BlockDone`, or `Retire`.
    Part {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Stripe index.
        stripe: u32,
        /// The verified part to write. Its payload is shared with `BlockTable`.
        part: VerifiedPart,
        /// Whether the part follows a flagged `Have` this node sent: a
        /// proposer seed or a holder push (decision-24).
        push: bool,
    },
    /// A `Have` batch queued behind preceding parts (bpv1-26). A flagged
    /// `Have` queues ahead of the parts it flags (bpv1-72).
    Have {
        /// Block consensus hash.
        block_id: block::Hash,
        /// Push and relay flags (decision-24).
        flags: HaveFlags,
        /// Strictly increasing wire indices.
        indices: Vec<u32>,
    },
    /// A `WantEnd` queued behind all parts it answered (bpv1-29, bpv1-55).
    WantEnd {
        /// Request identifier being ended.
        want_id: u32,
        /// Parts sent for this `Want`.
        sent: u16,
        /// End reason.
        reason: WantEndReason,
    },
}

impl DataQueueItem {
    /// Encodes the item into its wire [`Frame`].
    pub fn encode(&self) -> Option<Frame> {
        match self {
            Self::Part { block_id, part, .. } => Some(encode_part_frame(*block_id, part)),
            Self::Have {
                block_id,
                flags,
                indices,
            } => encode_frame(&Bpv1Message::Have {
                block_id: *block_id,
                flags: *flags,
                indices: indices.clone(),
            })
            .ok(),
            Self::WantEnd {
                want_id,
                sent,
                reason,
                ..
            } => encode_frame(&Bpv1Message::WantEnd {
                want_id: *want_id,
                sent: *sent,
                reason: *reason,
            })
            .ok(),
        }
    }
}

/// Encodes `part` of `block_id` as a W2 `Part` frame:
/// `block_id(32) || index(4) || depth(1) || siblings(32 × depth) || payload`,
/// with the payload trimmed to `wire_len(index)` (decision-18).
pub fn encode_part_frame(block_id: block::Hash, part: &VerifiedPart) -> Frame {
    let siblings = part.siblings();
    let wire_payload = part.wire_payload();
    let mut payload = Vec::with_capacity(32 + 4 + 1 + siblings.len() * 32 + wire_payload.len());
    payload.extend_from_slice(&block_id.0);
    payload.extend_from_slice(&part.index().to_le_bytes());
    // A verified part's proof depth is at most `W2_MAX_PROOF_DEPTH`, which fits a byte.
    payload.push(siblings.len() as u8);
    for sib in siblings {
        payload.extend_from_slice(sib);
    }
    payload.extend_from_slice(wire_payload);
    Frame {
        message_type: message_type::PART,
        flags: 0,
        payload,
    }
}

/// Pure FIFO output queue for one BPv1 data stream (decision-7).
///
/// `Serve` responses, pushed parts, `Have` announcements, and
/// `WantEnd`s all enqueue here, so `WantEnd`-behind-parts (bpv1-29) and
/// `Have`-behind-parts (bpv1-26) hold by construction. The queue never refuses
/// an item: send-once and the live-`Want` limit bound what it holds, and the
/// transport's output gate bounds the encoded bytes (bpv1-51, bpv1-55).
#[derive(Debug, Default)]
pub struct Bpv1FifoQueue {
    items: VecDeque<DataQueueItem>,
    part_bytes: usize,
}

impl Bpv1FifoQueue {
    /// Payload bytes of the queued parts (`queue.bytes`).
    pub fn part_bytes(&self) -> usize {
        self.part_bytes
    }

    /// Total queued items.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Queues `item` behind every queued item.
    pub fn push_back(&mut self, item: DataQueueItem) {
        self.part_bytes += Self::bytes(&item);
        self.items.push_back(item);
    }

    /// Pops the next item in strict FIFO order.
    pub fn pop_front(&mut self) -> Option<DataQueueItem> {
        let item = self.items.pop_front()?;
        self.part_bytes -= Self::bytes(&item);
        Some(item)
    }

    /// Cancels queued unsent `Part`s of `(block_id, stripe)` (on `StripeDone`, bpv1-38).
    /// Returns the number of cancelled parts. Never removes `WantEnd`s.
    pub fn cancel_stripe(&mut self, block_id: block::Hash, stripe: u32) -> usize {
        self.remove(|item| {
            matches!(item, DataQueueItem::Part { block_id: b, stripe: s, .. }
                if *b == block_id && *s == stripe)
        })
    }

    /// Cancels the queued unsent push of part `index` of `block_id`
    /// (03 §5.4). Returns the number of cancelled parts.
    pub fn cancel_push(&mut self, block_id: block::Hash, index: u32) -> usize {
        self.remove(|item| {
            matches!(item, DataQueueItem::Part { block_id: b, part, push: true, .. }
                if *b == block_id && part.index() == index)
        })
    }

    /// Purges queued unsent `Part`s and `Have`s for `block_id` (on `BlockDone` or
    /// `Retire`, bpv1-38, bpv1-46). Never removes `WantEnd`s.
    pub fn purge_block(&mut self, block_id: block::Hash, include_haves: bool) -> usize {
        self.remove(|item| match item {
            DataQueueItem::Part { block_id: b, .. } => *b == block_id,
            DataQueueItem::Have { block_id: b, .. } => include_haves && *b == block_id,
            DataQueueItem::WantEnd { .. } => false,
        })
    }

    fn remove(&mut self, mut cancel: impl FnMut(&DataQueueItem) -> bool) -> usize {
        let before = self.items.len();
        let mut freed = 0;
        self.items.retain(|item| {
            let drop = cancel(item);
            if drop {
                freed += Self::bytes(item);
            }
            !drop
        });
        self.part_bytes -= freed;
        before - self.items.len()
    }

    fn bytes(item: &DataQueueItem) -> usize {
        match item {
            DataQueueItem::Part { part, .. } => part.wire_payload().len(),
            DataQueueItem::Have { .. } | DataQueueItem::WantEnd { .. } => 0,
        }
    }
}
