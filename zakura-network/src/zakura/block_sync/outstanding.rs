use std::time::Instant;

use zakura_chain::block;

use super::{request::BlockRangeRequest, wire::MAX_BS_BLOCKS_PER_REQUEST};

/// One block-range request sent to a peer, including response-correlation state.
#[derive(Clone, Debug)]
pub(super) struct OutstandingBlockRange {
    /// Session-local identity used to reject stale watchdog actions.
    pub(super) token: BlockRequestToken,
    /// Whether this request still owns scheduling resources.
    pub(super) state: OutstandingRequestState,
    /// Requested heights, hashes, and size estimates.
    pub(super) request: BlockRangeRequest,
    /// Time the request was queued to the peer.
    pub(super) queued_at: Instant,
    /// Deadline for the peer's response.
    pub(super) deadline: Instant,
    /// BBR delivery state captured when the request was queued.
    pub(super) delivery_snapshot: DeliverySnapshot,
    /// Serialized bytes received for this request so far.
    pub(super) delivered_bytes: u64,
    /// Requested offsets whose bodies have arrived.
    pub(super) received: ReceivedBlockTracker,
    /// Whether this timed-out request has already received its one reliability credit.
    pub(super) late_reliability_credited: bool,
}

/// Monotonic request identity scoped to one peer-routine generation.
pub(super) type BlockRequestToken = u64;

/// Why a request stopped owning active scheduling resources.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RetirementReason {
    /// Another peer supplied the range and the download floor advanced past it.
    Covered,
    /// The peer did not complete the request before its request deadline.
    RequestTimeout,
    /// The reactor cancelled an expired floor claim to let another peer retry it.
    FloorWatchdog,
}

/// Scheduling and late-response correlation state for a sent request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum OutstandingRequestState {
    /// The request owns scheduling slots, registry claims, and byte reservations.
    Active,
    /// The request no longer owns scheduling resources but remains for bounded
    /// late-response correlation and peer accountability.
    Retired {
        /// Why active ownership ended.
        reason: RetirementReason,
        /// When the request transitioned out of active scheduling.
        retired_at: Instant,
        /// When its late-response correlation record can be discarded.
        correlation_deadline: Instant,
    },
}

/// Per-peer request records with cached active scheduling counters.
///
/// All mutations go through this type so the counters cannot drift from `entries`.
#[derive(Clone, Debug, Default)]
pub(super) struct OutstandingRequests {
    /// Active requests and bounded retired correlation records.
    entries: Vec<OutstandingBlockRange>,
    /// Requests that still own scheduling resources.
    active_count: usize,
    /// Requests retained only for correlation and accountability.
    retired_count: usize,
    /// Estimated bytes still reserved by unreceived heights in active requests.
    active_reserved_bytes: u64,
}

impl OutstandingRequests {
    /// Creates an empty request collection with zeroed counters.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Returns the total active and retired record count.
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true when there are no active or retired records.
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the cached number of requests that still own scheduling resources.
    pub(super) fn active_len(&self) -> usize {
        self.active_count
    }

    /// Returns the cached number of retired correlation records.
    pub(super) fn retired_len(&self) -> usize {
        self.retired_count
    }

    /// Returns cached estimated bytes reserved by unreceived active heights.
    pub(super) fn active_reserved_bytes(&self) -> u64 {
        self.active_reserved_bytes
    }

    /// Iterates over active and retired records in insertion order.
    pub(super) fn iter(&self) -> impl Iterator<Item = &OutstandingBlockRange> {
        self.entries.iter()
    }

    /// Iterates over records that still own scheduling resources.
    pub(super) fn active_iter(&self) -> impl Iterator<Item = &OutstandingBlockRange> {
        self.entries
            .iter()
            .filter(|outstanding| outstanding.is_active())
    }

    /// Returns the record at `index`, or `None` when it is out of bounds.
    pub(super) fn get(&self, index: usize) -> Option<&OutstandingBlockRange> {
        self.entries.get(index)
    }

    /// Finds an active record by its session-local request token.
    pub(super) fn active_index_for_token(&self, token: BlockRequestToken) -> Option<usize> {
        self.entries
            .iter()
            .position(|outstanding| outstanding.token == token && outstanding.is_active())
    }

    /// Returns whether the indexed request has received every expected body.
    ///
    /// Returns false for an out-of-bounds index.
    pub(super) fn is_complete(&self, index: usize) -> bool {
        self.entries
            .get(index)
            .is_some_and(OutstandingBlockRange::is_complete)
    }

    /// Inserts a request and charges its current state to the cached counters.
    pub(super) fn insert(&mut self, outstanding: OutstandingBlockRange) {
        if outstanding.is_active() {
            self.active_count = self.active_count.saturating_add(1);
            self.active_reserved_bytes = self
                .active_reserved_bytes
                .saturating_add(outstanding.reserved_bytes());
        } else {
            self.retired_count = self.retired_count.saturating_add(1);
        }
        self.entries.push(outstanding);
        self.debug_assert_invariants();
    }

    #[cfg(test)]
    /// Test-only alias for [`Self::insert`].
    pub(super) fn push(&mut self, outstanding: OutstandingBlockRange) {
        self.insert(outstanding);
    }

    /// Removes and returns a record while releasing its cached counter contribution.
    ///
    /// # Panics
    ///
    /// Panics when `index` is out of bounds.
    pub(super) fn remove(&mut self, index: usize) -> OutstandingBlockRange {
        let outstanding = self.entries.remove(index);
        if outstanding.is_active() {
            self.active_count = self.active_count.saturating_sub(1);
            self.active_reserved_bytes = self
                .active_reserved_bytes
                .saturating_sub(outstanding.reserved_bytes());
        } else {
            self.retired_count = self.retired_count.saturating_sub(1);
        }
        self.debug_assert_invariants();
        outstanding
    }

    /// Transitions an active request to retired correlation state.
    ///
    /// Returns false when `index` is absent or the request was already retired.
    pub(super) fn retire(
        &mut self,
        index: usize,
        reason: RetirementReason,
        retired_at: Instant,
        correlation_deadline: Instant,
    ) -> bool {
        let Some(outstanding) = self.entries.get_mut(index) else {
            return false;
        };
        if outstanding.is_retired() {
            return false;
        }
        let reserved_bytes = outstanding.reserved_bytes();
        outstanding.retire(reason, retired_at, correlation_deadline);
        self.active_count = self.active_count.saturating_sub(1);
        self.retired_count = self.retired_count.saturating_add(1);
        self.active_reserved_bytes = self.active_reserved_bytes.saturating_sub(reserved_bytes);
        self.debug_assert_invariants();
        true
    }

    /// Retires every active request fully covered by `floor`.
    ///
    /// Calls `before_retire` once per matching request while it is still active,
    /// then updates all counters. Returns the number of requests retired.
    pub(super) fn retire_covered(
        &mut self,
        floor: block::Height,
        retired_at: Instant,
        correlation_deadline: Instant,
        mut before_retire: impl FnMut(&OutstandingBlockRange),
    ) -> usize {
        let mut retired = 0usize;
        for outstanding in &mut self.entries {
            if !outstanding.is_active() || outstanding.request.end_height() > floor {
                continue;
            }
            before_retire(outstanding);
            let reserved_bytes = outstanding.reserved_bytes();
            outstanding.retire(RetirementReason::Covered, retired_at, correlation_deadline);
            self.active_count = self.active_count.saturating_sub(1);
            self.retired_count = self.retired_count.saturating_add(1);
            self.active_reserved_bytes = self.active_reserved_bytes.saturating_sub(reserved_bytes);
            retired = retired.saturating_add(1);
        }
        self.debug_assert_invariants();
        retired
    }

    /// Marks one expected height received and updates active reserved bytes.
    ///
    /// Returns true only for the first receipt of that height.
    pub(super) fn mark_received(&mut self, index: usize, height: block::Height) -> bool {
        let Some(outstanding) = self.entries.get_mut(index) else {
            return false;
        };
        let newly_received = !outstanding.has_received(height);
        let released_estimate = newly_received
            .then(|| outstanding.estimated_bytes_for_height(height))
            .flatten()
            .unwrap_or(0);
        outstanding.mark_received(height);
        if outstanding.is_active() {
            self.active_reserved_bytes =
                self.active_reserved_bytes.saturating_sub(released_estimate);
        }
        self.debug_assert_invariants();
        newly_received
    }

    /// Marks all expected heights through `tip` received.
    ///
    /// Returns the newly released estimated-byte total, or zero for an absent index.
    pub(super) fn mark_received_through(&mut self, index: usize, tip: block::Height) -> u64 {
        let Some(outstanding) = self.entries.get_mut(index) else {
            return 0;
        };
        let released = outstanding.mark_received_through(tip);
        if outstanding.is_active() {
            self.active_reserved_bytes = self.active_reserved_bytes.saturating_sub(released);
        }
        self.debug_assert_invariants();
        released
    }

    /// Adds delivered body bytes to the indexed request, if present.
    pub(super) fn record_body_bytes(&mut self, index: usize, bytes: u64) {
        if let Some(outstanding) = self.entries.get_mut(index) {
            outstanding.record_body_bytes(bytes);
        }
    }

    /// Claims the indexed timed-out request's one late reliability credit.
    ///
    /// Returns false when the request is absent, ineligible, or already credited.
    pub(super) fn take_late_reliability_credit(&mut self, index: usize) -> bool {
        self.entries
            .get_mut(index)
            .is_some_and(OutstandingBlockRange::take_late_reliability_credit)
    }

    /// Removes retired records whose correlation deadline is due.
    ///
    /// Returns the number of records removed.
    pub(super) fn prune_expired_retired(&mut self, now: Instant) -> usize {
        let previous_len = self.entries.len();
        self.entries.retain(|outstanding| {
            !matches!(
                outstanding.state,
                OutstandingRequestState::Retired {
                    correlation_deadline,
                    ..
                } if correlation_deadline <= now
            )
        });
        let removed = previous_len.saturating_sub(self.entries.len());
        self.retired_count = self.retired_count.saturating_sub(removed);
        self.debug_assert_invariants();
        removed
    }

    /// Removes and returns every record, resetting all cached counters.
    pub(super) fn drain_all(&mut self) -> Vec<OutstandingBlockRange> {
        let entries = std::mem::take(&mut self.entries);
        self.active_count = 0;
        self.retired_count = 0;
        self.active_reserved_bytes = 0;
        entries
    }

    #[cfg(test)]
    /// Clears all records through the same counter-safe drain path.
    pub(super) fn clear(&mut self) {
        let _ = self.drain_all();
    }

    #[cfg(test)]
    /// Removes records from the end until `len` remain.
    pub(super) fn truncate(&mut self, len: usize) {
        while self.entries.len() > len {
            self.remove(self.entries.len() - 1);
        }
    }

    /// Checks cached counters after mutation in debug builds.
    fn debug_assert_invariants(&self) {
        debug_assert!(self.invariants_hold());
    }

    /// Recomputes all counters from the records and compares them with the cache.
    fn invariants_hold(&self) -> bool {
        self.active_count
            == self
                .entries
                .iter()
                .filter(|entry| entry.is_active())
                .count()
            && self.retired_count
                == self
                    .entries
                    .iter()
                    .filter(|entry| entry.is_retired())
                    .count()
            && self.active_reserved_bytes
                == self
                    .entries
                    .iter()
                    .filter(|entry| entry.is_active())
                    .fold(0u64, |sum, entry| {
                        sum.saturating_add(entry.reserved_bytes())
                    })
            && self.entries.len() == self.active_count.saturating_add(self.retired_count)
    }

    #[cfg(test)]
    /// Fails the current test if any cached counter has drifted.
    pub(super) fn assert_invariants(&self) {
        assert!(
            self.invariants_hold(),
            "outstanding request counters drifted"
        );
    }
}

impl std::ops::Index<usize> for OutstandingRequests {
    type Output = OutstandingBlockRange;

    /// Returns the indexed record.
    ///
    /// # Panics
    ///
    /// Panics when `index` is out of bounds.
    fn index(&self, index: usize) -> &Self::Output {
        &self.entries[index]
    }
}

/// BBR delivery counters captured when a request is queued.
#[derive(Copy, Clone, Debug)]
pub(super) struct DeliverySnapshot {
    /// Cumulative delivered units at request creation.
    pub(super) delivered: u64,
    /// Time corresponding to the cumulative delivery counter.
    pub(super) delivered_at: Instant,
}

impl OutstandingBlockRange {
    /// Returns true while this request owns active scheduling resources.
    pub(super) fn is_active(&self) -> bool {
        self.state == OutstandingRequestState::Active
    }

    /// Returns true after this request releases active scheduling ownership.
    pub(super) fn is_retired(&self) -> bool {
        !self.is_active()
    }

    /// Returns the retirement reason, or `None` while active.
    pub(super) fn retirement_reason(&self) -> Option<RetirementReason> {
        match self.state {
            OutstandingRequestState::Active => None,
            OutstandingRequestState::Retired { reason, .. } => Some(reason),
        }
    }

    /// Claims the single late-success credit earned after a charged timeout.
    ///
    /// Floor-watchdog and request-timeout retirements are eligible exactly once.
    pub(super) fn take_late_reliability_credit(&mut self) -> bool {
        if !matches!(
            self.retirement_reason(),
            Some(RetirementReason::RequestTimeout | RetirementReason::FloorWatchdog)
        ) || self.late_reliability_credited
        {
            return false;
        }
        self.late_reliability_credited = true;
        true
    }

    /// Changes an active record to retired correlation state.
    ///
    /// Returns false without modifying an already retired record.
    pub(super) fn retire(
        &mut self,
        reason: RetirementReason,
        retired_at: Instant,
        correlation_deadline: Instant,
    ) -> bool {
        if self.is_retired() {
            return false;
        }
        self.state = OutstandingRequestState::Retired {
            reason,
            retired_at,
            correlation_deadline,
        };
        true
    }

    /// Sums size estimates for expected heights not yet received.
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.request
            .expected_blocks
            .iter()
            .filter(|expected| !self.has_received(expected.height))
            .fold(0u64, |acc, expected| {
                acc.saturating_add(expected.estimated_bytes)
            })
    }

    /// Returns the expected size estimate for `height` when it belongs to this range.
    pub(super) fn estimated_bytes_for_height(&self, height: block::Height) -> Option<u64> {
        self.request.estimated_bytes_for_height(height)
    }

    /// Returns whether the expected offset for `height` has been received.
    pub(super) fn has_received(&self, height: block::Height) -> bool {
        self.request
            .offset_for_height(height)
            .is_some_and(|offset| self.received.contains_offset(offset))
    }

    /// Records receipt of `height` when it belongs to this request.
    pub(super) fn mark_received(&mut self, height: block::Height) {
        if let Some(offset) = self.request.offset_for_height(height) {
            self.received.insert_offset(offset);
        }
    }

    /// Adds serialized body bytes delivered for this request.
    pub(super) fn record_body_bytes(&mut self, bytes: u64) {
        self.delivered_bytes = self.delivered_bytes.saturating_add(bytes);
    }

    /// Marks expected heights through `tip` received.
    ///
    /// Returns the size estimates newly released by this operation.
    pub(super) fn mark_received_through(&mut self, tip: block::Height) -> u64 {
        self.request
            .expected_blocks
            .iter()
            .filter(|expected| {
                expected.height <= tip
                    && self
                        .request
                        .offset_for_height(expected.height)
                        .is_some_and(|offset| self.received.insert_offset(offset))
            })
            .fold(0u64, |acc, expected| {
                acc.saturating_add(expected.estimated_bytes)
            })
    }

    /// Returns true when every expected request offset has been received.
    pub(super) fn is_complete(&self) -> bool {
        self.received.len() == self.request.expected_blocks.len()
    }
}

/// Number of request offsets representable by [`ReceivedBlockTracker`].
const RECEIVED_TRACKER_OFFSET_CAPACITY: u32 = u128::BITS;
// Keep the wire request cap within the fixed tracker representation.
const _: () = assert!(MAX_BS_BLOCKS_PER_REQUEST <= RECEIVED_TRACKER_OFFSET_CAPACITY);

/// Compact set of received offsets within one bounded block-range request.
#[derive(Clone, Debug, Default)]
pub(super) struct ReceivedBlockTracker {
    /// One bit per received zero-based request offset.
    bits: u128,
    /// Cached population count for constant-time completion checks.
    count: usize,
}

impl ReceivedBlockTracker {
    /// Returns the number of distinct offsets recorded.
    pub(super) fn len(&self) -> usize {
        self.count
    }

    /// Returns whether `offset` is representable and already recorded.
    fn contains_offset(&self, offset: u32) -> bool {
        Self::bit_for_offset(offset).is_some_and(|bit| self.bits & bit != 0)
    }

    /// Records `offset`, returning true only when it was newly inserted.
    fn insert_offset(&mut self, offset: u32) -> bool {
        let Some(bit) = Self::bit_for_offset(offset) else {
            return false;
        };
        if self.bits & bit != 0 {
            return false;
        }
        self.bits |= bit;
        self.count = self.count.saturating_add(1);
        true
    }

    /// Returns the bit corresponding to a representable request offset.
    fn bit_for_offset(offset: u32) -> Option<u128> {
        1u128.checked_shl(offset)
    }
}
