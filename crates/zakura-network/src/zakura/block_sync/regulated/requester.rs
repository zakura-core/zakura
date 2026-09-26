//! Exact GetBlocks identity and ending checks around shared reservations.

use std::collections::{BTreeMap, HashMap};

use thiserror::Error;
use zakura_chain::block::{Hash, Height};

use super::wire::{Message, Range, GET_BLOCKS, RULES};
use crate::zakura::{
    block_sync::{config::MAX_BS_INFLIGHT_REQUESTS, MAX_BS_RESPONSE_BYTES},
    regulation::{
        ClaimRefused, Claimed, Ended, Exchange, ExchangeWriter, PoolEntry, Reservations,
        ReserveRefused,
    },
    wire_codec::WireError,
};

#[derive(Debug, Error)]
pub(super) enum RequestError {
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Capacity(#[from] ReserveRefused),
    #[error("the request overlaps a live range")]
    Overlap,
    #[error("the expected headers do not describe this range")]
    ExpectedHeaders,
}

#[derive(Debug, Error)]
pub(super) enum ResponseError {
    #[error(transparent)]
    Claim(#[from] ClaimRefused),
    #[error("the block is not the next expected header of a live range")]
    Identity,
    #[error("the response exceeds its body-byte limit")]
    BodyBytes,
    #[error("the ending does not match the consumed prefix")]
    Ending,
}

struct Pending {
    range: Range,
    // An exact-sized allocation avoids retaining a caller's excess Vec capacity.
    expected: Box<[Hash]>,
    received: usize,
    body_bytes: usize,
    max_body_bytes: u32,
}

/// Each range owns one node pool entry and at most 128 expected hashes.
/// The session owns these maps, including their retained allocation capacity.
/// Its SessionCapacity lease must therefore outlive this object.
///
/// `next` has at most one entry per live range. It avoids an O(live requests)
/// scan on every block without adding a generic index or allocation budget.
pub(super) struct Requester {
    reservations: Reservations<Height>,
    ranges: BTreeMap<Height, Pending>,
    next: HashMap<Hash, Height>,
}

impl Requester {
    pub(super) fn new(capacity: usize) -> Self {
        // The protocol ceiling of 32,768 fits usize on supported targets.
        Self {
            reservations: Reservations::new(RULES, capacity.min(MAX_BS_INFLIGHT_REQUESTS as usize)),
            ranges: BTreeMap::new(),
            next: HashMap::new(),
        }
    }

    /// Reserve before publishing the request. `expected` must come from the
    /// already validated header chain, in ascending height order. The request
    /// must also fit the peer's advertised block count and download window.
    /// On a failed publication call `retract`; after publication use `abandon`.
    pub(super) fn reserve(
        &mut self,
        range: Range,
        expected: &[Hash],
        max_body_bytes: u32,
        entry: PoolEntry,
        exchange: Exchange,
    ) -> Result<ExchangeWriter, RequestError> {
        Range::new(range.start, range.count)?;
        if !(1..=MAX_BS_RESPONSE_BYTES).contains(&max_body_bytes) {
            return Err(WireError::OutOfRange("response body bytes").into());
        }
        // The protocol count is at most 128 and fits usize on supported targets.
        if expected.len() != range.count as usize || self.next.contains_key(&expected[0]) {
            return Err(RequestError::ExpectedHeaders);
        }
        let last = Height(range.start.0 + range.count - 1);
        if self
            .ranges
            .range(..=last)
            .next_back()
            .is_some_and(|(_, pending)| {
                pending.range.start.0 + pending.range.count - 1 >= range.start.0
            })
        {
            return Err(RequestError::Overlap);
        }
        let writer = exchange.writer();
        self.reservations.reserve_fenced(
            range.start,
            GET_BLOCKS.message_type,
            range.response_cap(max_body_bytes),
            entry,
            exchange,
        )?;
        self.next.insert(expected[0], range.start);
        self.ranges.insert(
            range.start,
            Pending {
                range,
                expected: expected.into(),
                received: 0,
                body_bytes: 0,
                max_body_bytes,
            },
        );
        Ok(writer)
    }

    /// Only for a request whose publication failed. Dropping a started exchange
    /// through this path closes the connection via the shared writer fence.
    pub(super) fn retract(&mut self, start: Height) {
        self.remove_range(start);
        self.reservations.retract(&start);
    }

    /// A scheduler timeout changes local interest, never the peer's authority.
    pub(super) fn abandon(&mut self, start: Height) {
        self.reservations.abandon(&start);
    }

    /// Header-only gate for the transport, before allocating a frame payload.
    pub(super) fn precheck(&self, tag: u16, len: usize) -> Result<(), ClaimRefused> {
        self.reservations.precheck(tag, len)
    }

    /// Claim the hash computed from this frame's header before decoding its
    /// transactions or performing body verification. Frame checks must already
    /// have established the block discriminator and its payload length.
    #[allow(clippy::unwrap_in_result)] // A broken private index is a local invariant failure.
    pub(super) fn claim_block(
        &mut self,
        hash: Hash,
        payload_len: usize,
    ) -> Result<(Height, Claimed), ResponseError> {
        let start = *self.next.get(&hash).ok_or(ResponseError::Identity)?;
        let pending = self
            .ranges
            .get_mut(&start)
            .expect("the next-hash index only contains live ranges");
        let body_len = payload_len.checked_sub(1).ok_or(ResponseError::BodyBytes)?;
        // The response cap is at most 32 MiB and fits usize on supported targets.
        if body_len > (pending.max_body_bytes as usize).saturating_sub(pending.body_bytes) {
            return Err(ResponseError::BodyBytes);
        }
        let claimed = self.reservations.claim_frame(&start, 3, payload_len)?;
        // received is bounded by the requested count, which is at most 128.
        let height = Height(start.0 + pending.received as u32);
        self.next.remove(&hash);
        pending.received += 1;
        pending.body_bytes += body_len;
        if let Some(next) = pending.expected.get(pending.received) {
            self.next.insert(*next, start);
        }
        Ok((height, claimed))
    }

    /// Validate the small ending payload before consuming the reservation or
    /// ending its writer fence. A rejected ending changes no authorization.
    pub(super) fn finish(&mut self, message: &Message) -> Result<(Range, Ended), ResponseError> {
        let (start, count, tag) = match message {
            Message::BlocksDone { start, returned } => (*start, *returned, 4),
            Message::RangeUnavailable(range) => (range.start, range.count, 5),
            _ => return Err(ResponseError::Ending),
        };
        let pending = self.ranges.get(&start).ok_or(ResponseError::Ending)?;
        // Counts fit usize on supported targets.
        if (tag == 4 && (count == 0 || count as usize != pending.received))
            || (tag == 5 && (pending.received != 0 || count != pending.range.count))
        {
            return Err(ResponseError::Ending);
        }
        let range = pending.range;
        let ended = self.reservations.claim_end(&start, tag, 9)?;
        self.remove_range(start);
        Ok((range, ended))
    }

    fn remove_range(&mut self, start: Height) {
        if let Some(pending) = self.ranges.remove(&start) {
            if let Some(next) = pending.expected.get(pending.received) {
                self.next.remove(next);
            }
        }
    }
}
