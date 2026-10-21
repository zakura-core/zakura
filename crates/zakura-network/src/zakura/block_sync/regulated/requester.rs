//! Exact GetBlocks identity and ending checks around shared reservations.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use thiserror::Error;
use zakura_chain::block::{Hash, Height};
use zakura_rt::{collections::HashMap, time};

use super::wire::{Outcome, Range, RangeEnding, GET_BLOCKS, RULES};
use crate::zakura::{
    block_sync::{config::MAX_BS_INFLIGHT_REQUESTS, MAX_BS_RESPONSE_BYTES},
    regulation::{
        shrink_after_peak, ClaimRefused, Claimed, Exchange, ExchangeWriter, PoolEntry,
        Reservations, ReserveRefused,
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

/// A consumed ending. It names the original request, not the reported count,
/// so the scheduler reconsiders every unreceived height of that request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedRange {
    pub(crate) range: Range,
    pub(crate) outcome: Outcome,
}

#[derive(Debug)]
struct Pending {
    range: Range,
    // An exact-sized allocation avoids retaining a caller's excess Vec capacity.
    expected: Box<[Hash]>,
    received: usize,
    body_bytes: usize,
    max_body_bytes: u32,
    retirement_started_at: Option<Instant>,
}

/// Each range owns one node pool entry and at most 128 expected hashes.
/// The session owns these maps, including their retained allocation capacity.
/// Its SessionCapacity lease must therefore outlive this object.
///
/// `next` has at most one entry per live range. It avoids an O(live requests)
/// scan on every block without adding a generic index or allocation budget.
#[derive(Debug)]
pub(super) struct Requester {
    reservations: Reservations<Height>,
    ranges: BTreeMap<Height, Pending>,
    next: HashMap<Hash, Height>,
    // Abandoned work and fully received ranges still waiting for an ending.
    retiring: BTreeSet<(Instant, Height)>,
    // Ranges in `ranges` whose every body arrived.
    pending_endings: usize,
    read_paused: Duration,
    capacity: usize,
}

impl Requester {
    /// `capacity` is a local share of the node pool, independent of the peer's
    /// advertisement. Abandoned and fully received ranges still consume it.
    pub(super) fn new(capacity: usize) -> Self {
        // The protocol ceiling of 32,768 fits usize on supported targets.
        let capacity = capacity.min(MAX_BS_INFLIGHT_REQUESTS as usize);
        Self {
            reservations: Reservations::new(RULES, capacity),
            ranges: BTreeMap::new(),
            next: HashMap::new(),
            retiring: BTreeSet::new(),
            pending_endings: 0,
            read_paused: Duration::ZERO,
            capacity,
        }
    }

    /// Unreceived bodies from every live exchange share a supplier cooldown
    /// after an incompatible header makes the whole connection unusable.
    /// Returns at most `limit`, lowest ranges first, so the snapshot never
    /// outgrows the cooldown map that receives it.
    pub(super) fn unreceived_expected(&self, limit: usize) -> Vec<(Height, Hash)> {
        self.ranges
            .values()
            .flat_map(|pending| {
                pending
                    .expected
                    .iter()
                    .enumerate()
                    .skip(pending.received)
                    .map(|(offset, hash)| {
                        // A validated range contains at most 128 heights without overflow.
                        (Height(pending.range.start().0 + offset as u32), *hash)
                    })
            })
            .take(limit)
            .collect()
    }

    pub(super) fn len(&self) -> usize {
        self.reservations.len()
    }

    pub(super) fn at_capacity(&self) -> bool {
        self.len() >= self.capacity
    }

    /// Live ranges whose every body arrived but whose ending has not.
    pub(super) fn pending_endings(&self) -> usize {
        self.pending_endings
    }

    /// Old abandoned work must not pin a height forever while other work advances.
    pub(super) fn retirement_deadline(&self, grace: Duration) -> Option<Instant> {
        self.retiring
            .first()
            .map(|(at, _)| *at + grace + self.read_paused)
    }

    /// Local waits stop reading endings. Keep the timer in read-active time
    /// without walking every pending request after each wait.
    pub(super) fn credit_read_pause(&mut self, elapsed: Duration) {
        self.read_paused += elapsed;
    }

    /// An unended range also blocks a new request after a reorg. Arm cleanup
    /// even if its old bodies all arrived, because the height is needed again.
    pub(super) fn blocks_retry(&mut self, height: Height) -> bool {
        let Some((start, pending)) = self.ranges.range_mut(..=height).next_back() else {
            return false;
        };
        if u64::from(pending.range.start().0) + u64::from(pending.range.count())
            <= u64::from(height.0)
        {
            return false;
        }
        if let Some(at) = pending.retirement_started_at {
            self.retiring.insert((at, *start));
        }
        true
    }

    /// Reserve before publishing the request. `expected` must come from the
    /// already validated header chain, in ascending height order. The request
    /// must also fit the peer's advertised block count and download window.
    /// `max_body_bytes` must equal the peer's advertised, clamped response-byte
    /// limit when issuing this request, not a smaller local download budget.
    /// On a failed publication call `retract`; after publication use `abandon`.
    pub(super) fn reserve(
        &mut self,
        range: Range,
        expected: &[Hash],
        max_body_bytes: u32,
        entry: PoolEntry,
        exchange: Exchange,
    ) -> Result<ExchangeWriter, RequestError> {
        if !(1..=MAX_BS_RESPONSE_BYTES).contains(&max_body_bytes) {
            return Err(WireError::OutOfRange("response body bytes").into());
        }
        // The protocol count is at most 128 and fits usize on supported targets.
        if expected.len() != range.count() as usize || self.next.contains_key(&expected[0]) {
            return Err(RequestError::ExpectedHeaders);
        }
        let last = range.last();
        if self
            .ranges
            .range(..=last)
            .next_back()
            .is_some_and(|(_, pending)| {
                u64::from(pending.range.start().0) + u64::from(pending.range.count())
                    > u64::from(range.start().0)
            })
        {
            return Err(RequestError::Overlap);
        }
        let writer = exchange.writer();
        self.reservations.reserve_fenced(
            range.start(),
            GET_BLOCKS.message_type,
            range.response_cap(max_body_bytes),
            entry,
            exchange,
        )?;
        self.next.insert(expected[0], range.start());
        self.ranges.insert(
            range.start(),
            Pending {
                range,
                expected: expected.into(),
                received: 0,
                body_bytes: 0,
                max_body_bytes,
                retirement_started_at: None,
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
        let now = time::Instant::now().into_std() - self.read_paused;
        self.reservations.abandon(&start);
        if let Some(pending) = self.ranges.get_mut(&start) {
            let at = *pending.retirement_started_at.get_or_insert(now);
            self.retiring.insert((at, start));
        }
    }

    /// Header-only gate, before allocating a frame payload.
    pub(super) fn precheck(&self, tag: u16, len: usize) -> Result<(), ClaimRefused> {
        self.reservations.precheck(tag, len)
    }

    /// The transport's gate: [`Self::precheck`], and the queue the reader
    /// fills ahead of the routine stays within what the live ranges owe.
    pub(super) fn admit(&mut self, tag: u16, len: usize) -> Result<(), ClaimRefused> {
        self.reservations.admit(tag, len)
    }

    /// Release a frame the transport admitted, when the routine takes it.
    pub(super) fn take_admitted(&mut self, tag: u16, len: usize) {
        self.reservations.take_admitted(tag, len);
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
        let now = time::Instant::now().into_std() - self.read_paused;
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
            let previous = self.next.insert(*next, start);
            debug_assert!(
                previous.is_none(),
                "live ranges expect distinct next hashes"
            );
        } else {
            // The ordered response must still end. Keep an earlier abandonment
            // deadline, or start one when the last block is claimed.
            let at = *pending.retirement_started_at.get_or_insert(now);
            self.retiring.insert((at, start));
            self.pending_endings += 1;
        }
        Ok((height, claimed))
    }

    /// Validate the small ending payload before consuming the reservation or
    /// ending its writer fence. A rejected ending changes no authorization.
    pub(super) fn finish(&mut self, ending: RangeEnding) -> Result<CompletedRange, ResponseError> {
        let pending = self
            .ranges
            .get(&ending.start)
            .ok_or(ResponseError::Ending)?;
        // Counts fit usize on supported targets.
        let matches = ending.count != 0
            && match ending.outcome {
                Outcome::Done => ending.count as usize == pending.received,
                Outcome::Unavailable => {
                    pending.received == 0 && ending.count <= pending.range.count()
                }
            };
        if !matches {
            return Err(ResponseError::Ending);
        }
        let range = pending.range;
        self.reservations
            .claim_end(&ending.start, u16::from(ending.message_type()), 9)?;
        self.remove_range(ending.start);
        Ok(CompletedRange {
            range,
            outcome: ending.outcome,
        })
    }

    fn remove_range(&mut self, start: Height) {
        if let Some(pending) = self.ranges.remove(&start) {
            if let Some(next) = pending.expected.get(pending.received) {
                self.next.remove(next);
            } else {
                self.pending_endings = self
                    .pending_endings
                    .checked_sub(1)
                    .expect("claiming a range's last body counted it as owing an ending");
            }
            // A complete range already left `next` when its last block was
            // claimed, so shrink on every removal, not only an unfinished one.
            shrink_after_peak(&mut self.next);
            if let Some(at) = pending.retirement_started_at {
                self.retiring.remove(&(at, start));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::zakura::{
        regulation::{ReservationPool, WriterFence},
        CloseCause,
    };

    /// Ranges that end after every block arrived still return the `next`
    /// map's peak capacity.
    #[test]
    fn complete_responses_shrink_the_next_hash_map() {
        const RANGES: u32 = 256;
        let slots = usize::try_from(RANGES).expect("256 fits usize");
        let fence = WriterFence::new(CancellationToken::new(), CloseCause::default());
        let pool = ReservationPool::new(slots).unwrap();
        let mut book = Requester::new(slots);
        let hash = |index: u32| {
            let mut bytes = [0; 32];
            bytes[..4].copy_from_slice(&index.to_le_bytes());
            Hash(bytes)
        };
        for index in 1..=RANGES {
            book.reserve(
                Range::new(Height(index), 1).unwrap(),
                &[hash(index)],
                100,
                pool.try_entry().unwrap(),
                fence.open().unwrap(),
            )
            .unwrap();
        }
        assert!(book.next.capacity() >= slots);

        for index in 1..=RANGES {
            book.claim_block(hash(index), 3).unwrap();
            book.finish(RangeEnding::done(Height(index), 1)).unwrap();
        }
        assert!(book.next.is_empty());
        assert!(
            book.next.capacity() <= 64,
            "a drained map kept {} slots",
            book.next.capacity()
        );
    }
}
