//! Download work source for Zakura block sync.
//!
//! The [`WorkQueue`] is the sole shared download-scheduling primitive: a sorted
//! set of needed block heights the per-peer issuance path pulls from. It replaces
//! the central `BlockRangeScheduler`'s eligibility/dedup/retry roles with a small
//! API the caller drives from its own per-peer state (see the ):
//!
//! - a height is in **exactly one** of `{below-floor (gone), pending, in_flight}`;
//! - [`take_in_range`](WorkQueue::take_in_range) moves a contiguous-ascending run
//!   `pending → in_flight` (so one taken chunk maps to one `BlockRangeRequest`),
//!   bounded only by the caller's servable range and a count cap — never by how
//!   far above the download floor the heights already are;
//! - only `return_items` (timeout/disconnect retry) and
//!   [`reset_above`](WorkQueue::reset_above) move `in_flight → pending`;
//! - [`advance_floor`](WorkQueue::advance_floor) is garbage collection only — the
//!   download floor never throttles the fetch decision.
//!
//! Internals are a brief `std::sync::Mutex` whose critical sections are tiny map
//! splices held **never across `.await`** (the anti-block rule). `estimated_bytes`
//! on a [`WorkItem`] is the block's size *estimate* (not its worst-case
//! reservation); it exists only to carry the `SizeMismatch` tolerance check
//! through to the reactor's receive path and request budget.

use std::sync::Mutex as StdMutex;

use tokio::sync::Notify;
use zakura_chain::block;

use super::{outstanding::BlockRequestToken, request::BlockSizeEstimate, state::BlockBudgetLedger};

/// Lower clamp on a body-size estimate.
pub(super) const DEFAULT_BS_SIZE_FLOOR_BYTES: u64 = 1024;

/// Identity of the request that owns a reserved work item.
///
/// Routine generations are allocated globally, so the pair is unique across
/// peers and routine replacements without retaining a peer ID in every item.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct ReservationOwner {
    pub(super) generation: u64,
    pub(super) request_token: BlockRequestToken,
}

/// Per-height download metadata held in the [`WorkQueue`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct WorkItem {
    /// Expected hash of the block at this height (drives the response match).
    pub(super) hash: block::Hash,
    /// The block's size estimate. Used for request budget reservation and the
    /// receive-path `SizeMismatch` tolerance check.
    pub(super) estimated_bytes: u64,
    /// Request reservation; received bodies use `Released`.
    pub(super) budget: BlockBudgetLedger,
    /// Request that owns this in-flight item across budget admission and release.
    ///
    /// Assigned atomically with `pending -> in_flight`, before the issuing routine
    /// can await budget funding. Cleared whenever the item becomes pending, held,
    /// or otherwise released.
    pub(super) reservation_owner: Option<ReservationOwner>,
    /// Previous-block hash after a received body claims this item.
    pub(super) received_previous_block_hash: Option<block::Hash>,
}

/// Diagnostics for an attempted `in_flight -> pending` retry transition.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct WorkReturnOutcome {
    /// Reserved bytes released while moving items back to `pending`.
    pub(super) released_bytes: u64,
    /// Reserved items successfully moved back to `pending`.
    pub(super) returned_count: u64,
    /// Requested heights that were already back in `pending`.
    pub(super) already_pending_count: u64,
    /// Received items still present in `in_flight` with a `Released` ledger.
    pub(super) released_count: u64,
    /// Reserved items now owned by a replacement request.
    pub(super) owner_mismatch_count: u64,
    /// Requested heights absent from both `pending` and `in_flight`.
    pub(super) missing_count: u64,
    /// Lowest height considered by the cleanup.
    pub(super) min_height: Option<block::Height>,
    /// Highest height considered by the cleanup.
    pub(super) max_height: Option<block::Height>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum LateBodyClaim {
    ClaimedPending {
        reset_epoch: u64,
    },
    ReleasedReserved {
        released_bytes: u64,
        reset_epoch: u64,
    },
    AlreadyReceived,
    Missing,
    PendingAdmissionRequired,
    HashMismatch,
}

#[derive(Debug)]
struct WorkQueueInner {
    pending: std::collections::BTreeMap<block::Height, WorkItem>,
    in_flight: std::collections::BTreeMap<block::Height, WorkItem>,
    floor: block::Height,
    /// Floor clamp for size estimates (overridable for tests).
    floor_estimate_bytes: u64,
    /// Running sum of `reserved_charge()` across every `pending` + `in_flight`
    /// item, maintained incrementally at each ledger transition so
    /// [`WorkQueue::reserved_bytes`]
    reserved_bytes: u64,
    /// Destructive-reset generation captured atomically by successful claims.
    reset_epoch: u64,
}

impl WorkQueueInner {
    fn estimate_bytes(&self, estimate: BlockSizeEstimate) -> u64 {
        estimate_bytes_with(estimate, self.floor_estimate_bytes)
    }
}

/// Compute a clamped body-size estimate from a [`BlockSizeEstimate`] hint.
///
/// `Confirmed`/`Advertised` use the hinted size; `Unknown` reserves the
/// per-block worst case. The result is clamped to `[floor, MAX_BLOCK_BYTES]`.
fn estimate_bytes_with(estimate: BlockSizeEstimate, floor: u64) -> u64 {
    let hinted = match estimate {
        BlockSizeEstimate::Confirmed(size) | BlockSizeEstimate::Advertised(size) => u64::from(size),
        BlockSizeEstimate::Unknown => block::MAX_BLOCK_BYTES,
    };
    hinted.max(floor).min(block::MAX_BLOCK_BYTES)
}

/// The shared download work source. See the module docs for the invariants.
#[derive(Debug)]
pub(super) struct WorkQueue {
    inner: StdMutex<WorkQueueInner>,
    available: Notify,
}

impl WorkQueue {
    pub(super) fn new(floor: block::Height) -> Self {
        Self {
            inner: StdMutex::new(WorkQueueInner {
                pending: std::collections::BTreeMap::new(),
                in_flight: std::collections::BTreeMap::new(),
                floor,
                floor_estimate_bytes: DEFAULT_BS_SIZE_FLOOR_BYTES,
                reserved_bytes: 0,
                reset_epoch: 0,
            }),
            available: Notify::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn set_estimate_floor_for_tests(&self, floor: u64) {
        let mut inner = self.lock();
        inner.floor_estimate_bytes = floor.max(1);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WorkQueueInner> {
        self.inner
            .lock()
            .expect("work queue mutex is never poisoned")
    }

    /// Add `(height, hash, size)` items to `pending`. Each is inserted iff its
    /// height is `> floor` and not already in `pending` or `in_flight`
    /// (idempotent — already-buffered/fetched heights are never re-queued).
    /// Returns the number of newly-inserted heights and wakes waiters if any.
    pub(super) fn extend(
        &self,
        items: impl IntoIterator<Item = (block::Height, block::Hash, BlockSizeEstimate)>,
    ) -> usize {
        let mut inserted = 0usize;
        {
            let mut inner = self.lock();
            for (height, hash, size) in items {
                if height <= inner.floor
                    || inner.pending.contains_key(&height)
                    || inner.in_flight.contains_key(&height)
                {
                    continue;
                }
                let estimated_bytes = inner.estimate_bytes(size);
                inner.pending.insert(
                    height,
                    WorkItem {
                        hash,
                        estimated_bytes,
                        budget: BlockBudgetLedger::Released,
                        reservation_owner: None,
                        received_previous_block_hash: None,
                    },
                );
                inserted += 1;
            }
        }
        if inserted > 0 {
            self.available.notify_waiters();
        }
        inserted
    }

    /// Move up to `max` contiguous-ascending `pending` heights within
    /// `low..=high` from `pending` to `in_flight`, returned in ascending order.
    ///
    /// "Contiguous-ascending" stops at the first gap, so the returned chunk maps
    /// to a single `BlockRangeRequest`. `high` is the caller's `servable_high`
    /// and is **NOT** clamped to the floor (the download floor is never an upper
    /// bound on the fetch). Returns empty if nothing is eligible.
    #[cfg(test)]
    pub(super) fn take_in_range(
        &self,
        low: block::Height,
        high: block::Height,
        max: usize,
    ) -> Vec<(block::Height, WorkItem)> {
        if max == 0 || low > high {
            return Vec::new();
        }
        let mut inner = self.lock();
        let mut taken: Vec<(block::Height, WorkItem)> = Vec::new();
        let mut next_expected: Option<block::Height> = None;
        for (height, item) in inner.pending.range(low..=high) {
            if let Some(expected) = next_expected {
                if *height != expected {
                    break;
                }
            }
            taken.push((*height, *item));
            if taken.len() >= max {
                break;
            }
            // Stop the run at the end of the height space rather than overflowing.
            match height.0.checked_add(1) {
                Some(raw) => next_expected = Some(block::Height(raw)),
                None => break,
            }
        }
        for (height, item) in &taken {
            inner.pending.remove(height);
            inner.in_flight.insert(*height, *item);
        }
        taken
    }

    /// Move up to `max_count` contiguous-ascending `pending` heights within
    /// `low..=high` from `pending` to `in_flight`, also stopping before the
    /// sum of stored size estimates would exceed `max_estimated_bytes`.
    ///
    /// The estimate cap bounds the request's summed byte reservation. To
    /// guarantee progress, the first eligible item is always taken when
    /// `max_count > 0`, even if its estimate alone exceeds the cap.
    #[cfg(test)]
    pub(super) fn take_in_range_budgeted(
        &self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
    ) -> Vec<(block::Height, WorkItem)> {
        self.take_in_range_budgeted_with_owner(low, high, max_count, max_estimated_bytes, None)
    }

    /// Budgeted take that atomically binds the resulting in-flight items to `owner`.
    ///
    /// The owner is assigned in the same queue critical section as the take so an
    /// async budget wait cannot later mutate an ABA replacement at the same height.
    pub(super) fn take_in_range_budgeted_owned(
        &self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
        owner: ReservationOwner,
    ) -> Vec<(block::Height, WorkItem)> {
        self.take_in_range_budgeted_with_owner(
            low,
            high,
            max_count,
            max_estimated_bytes,
            Some(owner),
        )
    }

    fn take_in_range_budgeted_with_owner(
        &self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
        owner: Option<ReservationOwner>,
    ) -> Vec<(block::Height, WorkItem)> {
        // An empty count or inverted range is a caller bug, not a real "nothing to
        // take": every caller computes `low <= high` and a positive count before
        // calling. Assert it in debug/test builds; still return empty in release so
        // a miscomputation degrades to a no-op rather than panicking a live node.
        debug_assert!(
            max_count > 0 && low <= high,
            "take_in_range_budgeted requires a positive count and low <= high, \
             got max_count={max_count}, low={low:?}, high={high:?}"
        );
        if max_count == 0 || low > high {
            return Vec::new();
        }
        let mut inner = self.lock();
        let mut taken: Vec<(block::Height, WorkItem)> = Vec::new();
        let mut estimated_bytes = 0u64;
        let mut next_expected: Option<block::Height> = None;
        for (height, item) in inner.pending.range(low..=high) {
            if let Some(expected) = next_expected {
                if *height != expected {
                    break;
                }
            }

            let next_estimated_bytes = estimated_bytes.saturating_add(item.estimated_bytes);
            if !taken.is_empty() && next_estimated_bytes > max_estimated_bytes {
                break;
            }

            let mut item = *item;
            item.reservation_owner = owner;
            taken.push((*height, item));
            estimated_bytes = next_estimated_bytes;
            if taken.len() >= max_count {
                break;
            }
            // Stop the run at the end of the height space rather than overflowing.
            match height.0.checked_add(1) {
                Some(raw) => next_expected = Some(block::Height(raw)),
                None => break,
            }
        }
        for (height, item) in &taken {
            inner.pending.remove(height);
            inner.in_flight.insert(*height, *item);
        }
        taken
    }

    /// Move each given height `in_flight → pending`, preserving its stored
    /// [`WorkItem`]. Heights not currently `in_flight` are skipped (idempotent).
    /// Wakes waiters if anything moved.
    #[cfg(test)]
    pub(super) fn return_items(&self, heights: impl IntoIterator<Item = block::Height>) {
        let mut moved = false;
        {
            let mut inner = self.lock();
            for height in heights {
                if let Some(mut item) = inner.in_flight.remove(&height) {
                    item.received_previous_block_hash = None;
                    inner.pending.insert(height, item);
                    moved = true;
                }
            }
        }
        if moved {
            self.available.notify_waiters();
        }
    }

    /// Return owner-matching, not-yet-funded items without notifying waiters.
    ///
    /// Used by a peer routine to put back a chunk it took but chose not to issue
    /// (e.g. the heights are in its own short retry-avoid window after it just
    /// failed them). Notifying here would re-wake the returning routine's own
    /// freshly-registered `available` future and busy-loop the want-work arm
    /// (a self-wake spin); other peers were already woken by the original failure
    /// `return_items`, so suppressing the notify only affects the caller.
    pub(super) fn return_items_quiet(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
        owner: ReservationOwner,
    ) {
        let mut inner = self.lock();
        for height in heights {
            let owned_and_unfunded = inner.in_flight.get(&height).is_some_and(|item| {
                item.reservation_owner == Some(owner) && !item.budget.is_reserved()
            });
            if owned_and_unfunded {
                let mut item = inner
                    .in_flight
                    .remove(&height)
                    .expect("owned in-flight item exists because it was just checked");
                item.reservation_owner = None;
                inner.pending.insert(height, item);
            }
        }
    }

    /// Fund already-taken heights owned by `owner` with their estimated byte reservation.
    ///
    /// Returns the sum marked. The caller must have already admitted the same
    /// byte total through [`ByteBudget`](crate::zakura::transport::ByteBudget).
    pub(super) fn mark_reserved(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
        owner: ReservationOwner,
    ) -> u64 {
        let mut marked = 0u64;
        let mut inner = self.lock();
        for height in heights {
            let Some(item) = inner.in_flight.get_mut(&height) else {
                continue;
            };
            if item.budget.is_reserved() || item.reservation_owner != Some(owner) {
                continue;
            }
            item.budget = BlockBudgetLedger::reserved(item.estimated_bytes);
            marked = marked.saturating_add(item.estimated_bytes);
        }
        // Released (0) -> Reserved(estimate): the reserved total grows by exactly
        // the bytes just marked.
        inner.reserved_bytes = inner.reserved_bytes.saturating_add(marked);
        marked
    }

    /// End an active request reservation at receipt.
    #[cfg(test)]
    pub(super) fn release_active_reserved_height(&self, height: block::Height) -> Option<u64> {
        let mut inner = self.lock();
        let released = {
            let item = inner.in_flight.get_mut(&height)?;
            if !item.budget.is_reserved() {
                return None;
            }
            item.budget.release_reserved()
        };
        inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released);
        Some(released)
    }

    /// Atomically claim a late body against the current WorkQueue owner.
    ///
    /// This combines the pending/reserved classification with its state
    /// transition so two peer routines cannot both decide they won the same
    /// height and forward duplicate bodies. It ends any request reservation,
    /// but the caller keeps the returned bytes charged globally until the body
    /// is visible to resident-memory accounting.
    pub(super) fn claim_late_body(
        &self,
        height: block::Height,
        expected_hash: block::Hash,
        previous_block_hash: block::Hash,
        pending_admitted: bool,
    ) -> LateBodyClaim {
        let mut inner = self.lock();
        Self::claim_late_body_locked(
            &mut inner,
            height,
            expected_hash,
            previous_block_hash,
            pending_admitted,
        )
    }

    #[cfg(test)]
    pub(super) fn claim_late_body_with_lock_hook(
        &self,
        height: block::Height,
        expected_hash: block::Hash,
        previous_block_hash: block::Hash,
        pending_admitted: bool,
        lock_hook: impl FnOnce(),
    ) -> LateBodyClaim {
        let mut inner = self.lock();
        lock_hook();
        Self::claim_late_body_locked(
            &mut inner,
            height,
            expected_hash,
            previous_block_hash,
            pending_admitted,
        )
    }

    fn claim_late_body_locked(
        inner: &mut WorkQueueInner,
        height: block::Height,
        expected_hash: block::Hash,
        previous_block_hash: block::Hash,
        pending_admitted: bool,
    ) -> LateBodyClaim {
        let reset_epoch = inner.reset_epoch;
        if let Some(item) = inner.in_flight.get_mut(&height) {
            if item.hash != expected_hash {
                return LateBodyClaim::HashMismatch;
            }
            if item.received_previous_block_hash.is_some() {
                return LateBodyClaim::AlreadyReceived;
            }
            let released_bytes = item.budget.release_reserved();
            item.reservation_owner = None;
            item.received_previous_block_hash = Some(previous_block_hash);
            inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released_bytes);
            return LateBodyClaim::ReleasedReserved {
                released_bytes,
                reset_epoch,
            };
        }

        if inner
            .pending
            .get(&height)
            .is_some_and(|item| item.hash != expected_hash)
        {
            return LateBodyClaim::HashMismatch;
        }
        let Some(mut item) = inner.pending.remove(&height) else {
            return LateBodyClaim::Missing;
        };
        if !pending_admitted {
            inner.pending.insert(height, item);
            return LateBodyClaim::PendingAdmissionRequired;
        }
        let released_bytes = item.budget.release_reserved();
        item.reservation_owner = None;
        item.received_previous_block_hash = Some(previous_block_hash);
        inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released_bytes);
        inner.in_flight.insert(height, item);
        LateBodyClaim::ClaimedPending { reset_epoch }
    }

    /// Claim a received height and end any request reservation it owned.
    #[cfg(test)]
    pub(super) fn claim_received(&self, height: block::Height) -> u64 {
        let mut inner = self.lock();
        if let Some(item) = inner.in_flight.get_mut(&height) {
            let released = item.budget.release_reserved();
            inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released);
            return released;
        }
        if let Some(mut item) = inner.pending.remove(&height) {
            let released = item.budget.release_reserved();
            inner.in_flight.insert(height, item);
            inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released);
            return released;
        }
        0
    }

    /// Release active request reservations, leaving received heights in place.
    pub(super) fn release_reserved_heights(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
        owner: ReservationOwner,
    ) -> u64 {
        let mut released = 0u64;
        let mut inner = self.lock();
        for height in heights {
            if let Some(item) = inner.in_flight.get_mut(&height) {
                if item.budget.is_reserved() && item.reservation_owner == Some(owner) {
                    released = released.saturating_add(item.budget.release_reserved());
                    item.reservation_owner = None;
                }
            } else if let Some(item) = inner.pending.get_mut(&height) {
                if item.budget.is_reserved() && item.reservation_owner == Some(owner) {
                    released = released.saturating_add(item.budget.release_reserved());
                    item.reservation_owner = None;
                }
            }
        }
        inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released);
        released
    }

    /// Release and return heights, including received ones, in tests.
    #[cfg(test)]
    pub(super) fn release_and_return_items(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        let mut moved = false;
        let mut released = 0u64;
        {
            let mut inner = self.lock();
            for height in heights {
                if let Some(mut item) = inner.in_flight.remove(&height) {
                    released = released.saturating_add(item.budget.release_reserved());
                    item.reservation_owner = None;
                    item.received_previous_block_hash = None;
                    inner.pending.insert(height, item);
                    moved = true;
                }
            }
            inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released);
        }
        if moved {
            self.available.notify_waiters();
        }
        released
    }

    /// Release and return only unreceived heights.
    pub(super) fn release_reserved_and_return_items(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
        owner: ReservationOwner,
    ) -> u64 {
        self.release_reserved_and_return_items_detailed(heights, owner)
            .released_bytes
    }

    /// Release and return still-reserved items, preserving the outcome of every
    /// requested height for low-volume lifecycle tracing.
    pub(super) fn release_reserved_and_return_items_detailed(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
        owner: ReservationOwner,
    ) -> WorkReturnOutcome {
        let mut moved = false;
        let mut outcome = WorkReturnOutcome::default();
        {
            let mut inner = self.lock();
            for height in heights {
                outcome.min_height = Some(
                    outcome
                        .min_height
                        .map_or(height, |current| current.min(height)),
                );
                outcome.max_height = Some(
                    outcome
                        .max_height
                        .map_or(height, |current| current.max(height)),
                );
                let Some(item) = inner.in_flight.get(&height) else {
                    if inner.pending.contains_key(&height) {
                        outcome.already_pending_count =
                            outcome.already_pending_count.saturating_add(1);
                    } else {
                        outcome.missing_count = outcome.missing_count.saturating_add(1);
                    }
                    continue;
                };
                match item.budget {
                    BlockBudgetLedger::Released => {
                        outcome.released_count = outcome.released_count.saturating_add(1);
                        continue;
                    }
                    BlockBudgetLedger::Reserved(_) => {}
                }
                if item.reservation_owner != Some(owner) {
                    outcome.owner_mismatch_count = outcome.owner_mismatch_count.saturating_add(1);
                    continue;
                }
                let mut item = inner
                    .in_flight
                    .remove(&height)
                    .expect("reserved item exists because it was just checked");
                // Only reserved items reach here, so the released bytes are exactly
                // the reserved charge leaving the queue.
                outcome.released_bytes = outcome
                    .released_bytes
                    .saturating_add(item.budget.release_reserved());
                item.reservation_owner = None;
                item.received_previous_block_hash = None;
                outcome.returned_count = outcome.returned_count.saturating_add(1);
                inner.pending.insert(height, item);
                moved = true;
            }
            inner.reserved_bytes = inner.reserved_bytes.saturating_sub(outcome.released_bytes);
        }
        if moved {
            self.available.notify_waiters();
        }
        outcome
    }

    /// Garbage-collect committed heights: raise the floor to `max(self.floor,
    /// floor)` and drop every `pending`/`in_flight` entry `<= floor`.
    ///
    /// Returns request-estimate bytes that were still reserved for unreceived
    /// heights, so the caller returns them to the `ByteBudget`. Received bodies
    /// carry no charge here; the Sequencer's buffers own them.
    pub(super) fn advance_floor(&self, floor: block::Height) -> u64 {
        let mut inner = self.lock();
        inner.floor = inner.floor.max(floor);
        let floor = inner.floor;
        // Pop only the committed `<= floor` prefix from each map. `pending` can hold
        // the entire header-ahead lag (100k+ heights), so a `retain` over the whole
        // map on every floor advance is O(total) and serializes the work-queue lock;
        // popping the prefix is O(removed · log n).
        let mut released = 0u64;
        while let Some((&height, _)) = inner.pending.first_key_value() {
            if height > floor {
                break;
            }
            let (_, mut item) = inner
                .pending
                .pop_first()
                .expect("first_key_value returned Some");
            released = released.saturating_add(item.budget.release_reserved());
        }
        while let Some((&height, _)) = inner.in_flight.first_key_value() {
            if height > floor {
                break;
            }
            let (_, mut item) = inner
                .in_flight
                .pop_first()
                .expect("first_key_value returned Some");
            released = released.saturating_add(item.budget.release_reserved());
        }
        inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released);
        released
    }

    /// Frontier reset: pin the floor and drop every `pending`/`in_flight` entry
    /// `> floor` (their buffers were dropped; the producer re-fills via the next
    /// query).
    ///
    /// Returns request-estimate bytes still reserved for unreceived heights, as
    /// in [`advance_floor`](Self::advance_floor).
    #[cfg(test)]
    pub(super) fn reset_above(&self, floor: block::Height) -> u64 {
        let mut inner = self.lock();
        Self::reset_above_locked(&mut inner, floor)
    }

    /// Clear successor ownership and bump the claim epoch atomically.
    pub(super) fn destructive_reset_above(&self, floor: block::Height) -> (u64, u64) {
        let mut inner = self.lock();
        Self::destructive_reset_above_locked(&mut inner, floor)
    }

    #[cfg(test)]
    pub(super) fn destructive_reset_above_with_lock_hook(
        &self,
        floor: block::Height,
        lock_hook: impl FnOnce(),
    ) -> (u64, u64) {
        let mut inner = self.lock();
        lock_hook();
        Self::destructive_reset_above_locked(&mut inner, floor)
    }

    fn destructive_reset_above_locked(
        inner: &mut WorkQueueInner,
        floor: block::Height,
    ) -> (u64, u64) {
        inner.reset_epoch = inner.reset_epoch.saturating_add(1);
        let released = Self::reset_above_locked(inner, floor);
        (released, inner.reset_epoch)
    }

    fn reset_above_locked(inner: &mut WorkQueueInner, floor: block::Height) -> u64 {
        inner.floor = floor;
        // Pop only the `> floor` suffix from each map (O(removed · log n)); see the
        // note in `advance_floor` on why a full-map `retain` is too expensive here.
        let mut released = 0u64;
        while let Some((&height, _)) = inner.pending.last_key_value() {
            if height <= floor {
                break;
            }
            let (_, mut item) = inner
                .pending
                .pop_last()
                .expect("last_key_value returned Some");
            released = released.saturating_add(item.budget.release_reserved());
        }
        while let Some((&height, _)) = inner.in_flight.last_key_value() {
            if height <= floor {
                break;
            }
            let (_, mut item) = inner
                .in_flight
                .pop_last()
                .expect("last_key_value returned Some");
            released = released.saturating_add(item.budget.release_reserved());
        }
        inner.reserved_bytes = inner.reserved_bytes.saturating_sub(released);
        released
    }

    /// The "work added" notifier (per-peer routines wake source).
    #[allow(dead_code)]
    pub(super) fn subscribe_available(&self) -> &Notify {
        &self.available
    }

    // ---- diagnostics (trace + late-response classification) ----

    pub(super) fn pending_len(&self) -> usize {
        self.lock().pending.len()
    }

    pub(super) fn in_flight_len(&self) -> usize {
        self.lock().in_flight.len()
    }

    pub(super) fn reserved_above(&self, floor: block::Height) -> (u64, u64) {
        let inner = self.lock();
        inner
            .in_flight
            .range((std::ops::Bound::Excluded(floor), std::ops::Bound::Unbounded))
            .fold((0u64, 0u64), |(bytes, count), (_, item)| {
                let charge = item.budget.reserved_charge();
                if charge == 0 {
                    (bytes, count)
                } else {
                    (bytes.saturating_add(charge), count.saturating_add(1))
                }
            })
    }

    /// Sum of reserved request-estimate bytes across `pending` + `in_flight`.
    ///
    /// O(1): returns the incrementally-maintained counter (see
    /// [`WorkQueueInner::reserved_bytes`]). This is on the sequencer's hot path via
    /// `publish_view`, so it must not scan the maps. `reserved_bytes_scanned` is
    /// the O(n) ground-truth recomputation used by the audit / tests to catch drift.
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.lock().reserved_bytes
    }

    /// Ground-truth O(pending + in_flight) recomputation of [`reserved_bytes`],
    /// used by tests to assert the maintained counter never drifts.
    #[cfg(test)]
    pub(super) fn reserved_bytes_scanned(&self) -> u64 {
        let inner = self.lock();
        inner
            .pending
            .values()
            .chain(inner.in_flight.values())
            .map(|item| item.budget.reserved_charge())
            .fold(0u64, u64::saturating_add)
    }

    /// Number of contiguous runs across `pending` (one queued range per maximal
    /// contiguous run of heights).
    pub(super) fn pending_run_count(&self) -> usize {
        let inner = self.lock();
        let mut runs = 0usize;
        let mut previous: Option<block::Height> = None;
        for height in inner.pending.keys() {
            let contiguous =
                previous.and_then(|previous| previous.0.checked_add(1)) == Some(height.0);
            if !contiguous {
                runs += 1;
            }
            previous = Some(*height);
        }
        runs
    }

    pub(super) fn min_pending(&self) -> Option<block::Height> {
        self.lock().pending.keys().next().copied()
    }

    pub(super) fn first_pending_in_range(
        &self,
        low: block::Height,
        high: block::Height,
    ) -> Option<block::Height> {
        if low > high {
            return None;
        }
        self.lock()
            .pending
            .range(low..=high)
            .next()
            .map(|(height, _)| *height)
    }

    pub(super) fn max_in_flight(&self) -> Option<block::Height> {
        self.lock().in_flight.keys().next_back().copied()
    }

    /// Returns true only for the exact received successor linked to `anchor_hash`.
    pub(super) fn held_successor_links_to(
        &self,
        successor: block::Height,
        anchor_hash: block::Hash,
    ) -> bool {
        self.lock().in_flight.get(&successor).is_some_and(|item| {
            !item.budget.is_reserved() && item.received_previous_block_hash == Some(anchor_hash)
        })
    }

    pub(super) fn max_claimed(&self) -> Option<block::Height> {
        let inner = self.lock();
        inner
            .pending
            .keys()
            .next_back()
            .copied()
            .max(inner.in_flight.keys().next_back().copied())
    }

    /// Expected hash for a height in `pending` or `in_flight` (late-response
    /// recovery).
    pub(super) fn hash_for_height(&self, height: block::Height) -> Option<block::Hash> {
        let inner = self.lock();
        inner
            .pending
            .get(&height)
            .or_else(|| inner.in_flight.get(&height))
            .map(|item| item.hash)
    }

    pub(super) fn pending_contains(&self, height: block::Height) -> bool {
        self.lock().pending.contains_key(&height)
    }

    pub(super) fn in_flight_contains(&self, height: block::Height) -> bool {
        self.lock().in_flight.contains_key(&height)
    }
}
