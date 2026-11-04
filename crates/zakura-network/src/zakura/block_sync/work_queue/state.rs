//! Logical owner of block-sync download work.
//!
//! [`WorkQueueState`] owns pending work, in-flight ownership, the download floor,
//! the current work authority, and the logical reservation ledger. It reads no
//! clock, lock, atomic, or channel. [`WorkQueue`](super::WorkQueue) calls each
//! transition under the queue lock and delivers the returned [`WorkWake`] at the
//! transition's existing linearization point. Request-write registrations, their
//! write-status fences, and pooled byte funding stay in IO.
//!
//! Every height is in exactly one of `{below-floor (gone), pending, in_flight}`.
//! A charge enters the ledger only at claim publication and leaves it exactly once,
//! through the `released_bytes` of the transition that ended it. IO settles those
//! bytes from the queue's pooled funding. The ledger also keeps each session's in-flight
//! charge, which admission compares with the [`OwnerReservationCap`]. Ledger arithmetic is
//! checked: a failure keeps
//! the last valid values and latches a [`LedgerFault`], which closes the budget.

use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};

use zakura_chain::block;
use zakura_header_chain::{BodyWorkAuthority, BodyWorkOwner};

use super::super::{
    admission::RequestPriority, request::BlockSizeEstimate, state::BlockBudgetLedger,
};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use tests::{
    trace_work_funding_bytes, trace_work_ownership_bytes, FundingReceiptView, FundingRequestPhase,
    FundingRequestView, WorkFundingHistory, WorkFundingStep, WorkFundingView, WorkOwnershipEntry,
    WorkOwnershipFailure, WorkOwnershipHistory, WorkOwnershipPhase, WorkOwnershipReplaySummary,
    WorkOwnershipStep, WorkOwnershipView,
};

/// Lower clamp on a body-size estimate.
pub(in crate::zakura::block_sync) const DEFAULT_BS_SIZE_FLOOR_BYTES: u64 = 1024;

/// Per-height download metadata held in the [`WorkQueue`](super::WorkQueue).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) struct WorkItem {
    /// Exact durable coordinates that authorized this body download.
    pub(in crate::zakura::block_sync) scope: zakura_header_chain::BodyWorkAuthority,
    /// Exact attempt, retained from provisional take through response receipt.
    pub(in crate::zakura::block_sync) owner: Option<zakura_header_chain::BodyWorkOwner>,
    /// Taken by an attempt that has not yet published its reservation and frame.
    provisional: bool,
    /// Expected hash of the block at this height (drives the response match).
    pub(in crate::zakura::block_sync) hash: block::Hash,
    /// The block's size estimate. Used for request budget reservation and the receive-path size-hint mismatch metric.
    pub(in crate::zakura::block_sync) estimated_bytes: u64,
    /// Largest payload seen locally. Peer hints cannot shrink a retry below it.
    observed_bytes: u64,
    /// Refreshed hint for a future retry while an issued reservation stays fixed.
    retry_estimated_bytes: Option<u64>,
    /// A pressure refusal waits for verified progress before it can be retried.
    retry_after_tip: Option<block::Height>,
    /// Request reservation; received bodies use `Released`.
    pub(in crate::zakura::block_sync) budget: BlockBudgetLedger,
    /// Another attempt's body ended this owner's reservation. The owner's request
    /// still claims the height, so dropping or moving the item is a revocation.
    answered_elsewhere: bool,
}

/// Where a queued item sits in its download lifecycle.
#[cfg(any(test, zakura_sim))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WorkItemPlace {
    /// Waiting for a request.
    Pending,
    /// Taken by a request that has not yet published its reservation here.
    Provisional,
    /// Taken by a published request.
    InFlight,
}

/// One queued item's owner and byte quantities.
#[cfg(any(test, zakura_sim))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WorkItemOwnership {
    /// The item's height.
    pub height: block::Height,
    /// The request that took the item, if one did.
    pub owner: Option<zakura_header_chain::BodyWorkOwner>,
    /// Where the item sits.
    pub place: WorkItemPlace,
    /// The size estimate a request for this item reserves.
    pub estimated_bytes: u64,
    /// The estimate bytes this item currently charges to the queue's tally.
    pub reserved_bytes: u64,
}

/// Diagnostics for an attempted `in_flight -> pending` retry transition.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::zakura::block_sync) struct WorkReturnOutcome {
    /// This owner's frame was skipped before writing, atomically with settlement.
    pub(in crate::zakura::block_sync) request_was_unwritten: bool,
    /// Reserved bytes released while returning or discarding items.
    pub(in crate::zakura::block_sync) released_bytes: u64,
    /// Reserved items successfully moved back to `pending`.
    pub(in crate::zakura::block_sync) returned_count: u64,
    /// Owned heights discarded because they are already committed.
    pub(in crate::zakura::block_sync) committed_count: u64,
    /// Requested heights that were already back in `pending`.
    pub(in crate::zakura::block_sync) already_pending_count: u64,
    /// Received items still present in `in_flight` with a `Released` ledger.
    pub(in crate::zakura::block_sync) released_count: u64,
    /// Requested heights absent from both `pending` and `in_flight`.
    pub(in crate::zakura::block_sync) missing_count: u64,
    /// Lowest height considered by the cleanup.
    pub(in crate::zakura::block_sync) min_height: Option<block::Height>,
    /// Highest height considered by the cleanup.
    pub(in crate::zakura::block_sync) max_height: Option<block::Height>,
}

impl WorkReturnOutcome {
    /// Whether the cleanup took unreceived work away from its owner: it returned
    /// a reserved height or discarded a committed one.
    pub(in crate::zakura::block_sync) fn revoked_work(&self) -> bool {
        self.returned_count > 0 || self.committed_count > 0
    }
}

/// Wakeups that a logical work transition requires.
#[must_use]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::zakura::block_sync) struct WorkWake {
    /// Pending work changed, so a waiting routine may now take it.
    pub(in crate::zakura::block_sync) available: bool,
    /// An unreceived height left its owner without that owner retiring the
    /// request. Routines rescan their requests for lost ownership.
    pub(in crate::zakura::block_sync) revocation: bool,
}

/// The logical change of a settling transition.
#[must_use]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::zakura::block_sync) struct WorkDelta {
    /// Reservation bytes that left the ledger. The queue settles exactly these
    /// bytes from its pooled funding, once.
    pub(in crate::zakura::block_sync) released_bytes: u64,
    /// Wakeups the change requires.
    pub(in crate::zakura::block_sync) wake: WorkWake,
}

/// A running count and byte sum of nonzero reservations.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct ReservedTally {
    bytes: u64,
    items: u64,
}

impl ReservedTally {
    fn checked_add(self, charge: u64) -> Option<Self> {
        if charge == 0 {
            return Some(self);
        }
        Some(Self {
            bytes: self.bytes.checked_add(charge)?,
            items: self.items.checked_add(1)?,
        })
    }

    fn checked_sub(self, charge: u64) -> Option<Self> {
        if charge == 0 {
            return Some(self);
        }
        Some(Self {
            bytes: self.bytes.checked_sub(charge)?,
            items: self.items.checked_sub(1)?,
        })
    }
}

/// The work queue's cap on the reservations one session may publish.
///
/// Each field is the total a session's published reservations may reach once
/// the admitted request publishes. A routine admits and publishes each request
/// before it takes the next, so a session has at most that one admitted,
/// unpublished request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) struct OwnerReservationCap {
    /// The total an above-floor request may bring the session to.
    pub(in crate::zakura::block_sync) above_floor: u64,
    /// The total a floor request may bring the session to: the above-floor
    /// total plus the floor bypass.
    pub(in crate::zakura::block_sync) floor: u64,
}

impl OwnerReservationCap {
    /// A cap that admits every request.
    pub(in crate::zakura::block_sync) const UNLIMITED: Self = Self {
        above_floor: u64::MAX,
        floor: u64::MAX,
    };

    /// Whether a session that holds `held` bytes may admit a request of
    /// `bytes` at `priority`.
    pub(in crate::zakura::block_sync) fn admits(
        self,
        held: u64,
        bytes: u64,
        priority: RequestPriority,
    ) -> bool {
        let limit = match priority {
            RequestPriority::Floor => self.floor,
            RequestPriority::AboveFloor => self.above_floor,
        };
        held.checked_add(bytes).is_some_and(|total| total <= limit)
    }
}

/// The first failed arithmetic in the logical ledger. The ledger keeps its last
/// valid values, and IO closes the budget under the same lock.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) struct LedgerFault {
    /// The ledger operation that failed.
    pub(in crate::zakura::block_sync) operation: &'static str,
    /// The value the operation started from.
    pub(in crate::zakura::block_sync) total: u64,
    /// The amount the operation could not apply.
    pub(in crate::zakura::block_sync) amount: u64,
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

/// Pending work, in-flight ownership, floor, authority, and logical charges.
#[derive(Debug)]
pub(in crate::zakura::block_sync) struct WorkQueueState {
    pending: BTreeMap<block::Height, WorkItem>,
    in_flight: BTreeMap<block::Height, WorkItem>,
    floor: block::Height,
    current_authority: Option<BodyWorkAuthority>,
    /// Floor clamp for size estimates (overridable for tests).
    floor_estimate_bytes: u64,
    /// Running sum of `reserved_charge()` across every `pending` + `in_flight`
    /// item, maintained incrementally at each ledger transition.
    reserved_bytes: u64,
    /// Nonzero reservations held by `in_flight` items, maintained at each
    /// `in_flight` ledger transition so admission never scans the map.
    in_flight_reserved: ReservedTally,
    /// Nonzero reservations held by `in_flight` items, by their owner's
    /// session, maintained with `in_flight_reserved`. A session without
    /// reservations has no entry.
    session_reserved: BTreeMap<u64, u64>,
    /// The first ledger arithmetic failure, if any. It never clears.
    ledger_fault: Option<LedgerFault>,
}

impl WorkQueueState {
    /// Read each item's ownership and charge from the maps, not the tallies.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn ownership(&self) -> Vec<WorkItemOwnership> {
        let pending = self
            .pending
            .iter()
            .map(|(height, item)| (height, item, WorkItemPlace::Pending));
        let in_flight = self.in_flight.iter().map(|(height, item)| {
            let place = if item.provisional {
                WorkItemPlace::Provisional
            } else {
                WorkItemPlace::InFlight
            };
            (height, item, place)
        });
        pending
            .chain(in_flight)
            .map(|(height, item, place)| WorkItemOwnership {
                height: *height,
                owner: item.owner,
                place,
                estimated_bytes: item.estimated_bytes,
                reserved_bytes: item.budget.reserved_charge(),
            })
            .collect()
    }

    pub(in crate::zakura::block_sync) fn new(floor: block::Height) -> Self {
        Self {
            pending: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            floor,
            current_authority: None,
            floor_estimate_bytes: DEFAULT_BS_SIZE_FLOOR_BYTES,
            reserved_bytes: 0,
            in_flight_reserved: ReservedTally::default(),
            session_reserved: BTreeMap::new(),
            ledger_fault: None,
        }
    }

    // ---- checked ledger arithmetic ----

    /// The first ledger arithmetic failure. IO must close the budget on one.
    pub(in crate::zakura::block_sync) fn ledger_fault(&self) -> Option<LedgerFault> {
        self.ledger_fault
    }

    fn note_fault(&mut self, operation: &'static str, total: u64, amount: u64) {
        self.ledger_fault.get_or_insert(LedgerFault {
            operation,
            total,
            amount,
        });
    }

    /// Add to a transition's running byte total. Overflow keeps `total`.
    fn add_to_total(&mut self, total: u64, amount: u64) -> u64 {
        total.checked_add(amount).unwrap_or_else(|| {
            self.note_fault("transition total", total, amount);
            total
        })
    }

    fn charge_ledger(&mut self, bytes: u64) {
        match self.reserved_bytes.checked_add(bytes) {
            Some(total) => self.reserved_bytes = total,
            None => self.note_fault("ledger charge", self.reserved_bytes, bytes),
        }
    }

    fn settle_ledger(&mut self, bytes: u64) {
        match self.reserved_bytes.checked_sub(bytes) {
            Some(total) => self.reserved_bytes = total,
            None => self.note_fault("ledger settlement", self.reserved_bytes, bytes),
        }
    }

    fn charge_in_flight(&mut self, owner: Option<BodyWorkOwner>, charge: u64) {
        match self.in_flight_reserved.checked_add(charge) {
            Some(tally) => self.in_flight_reserved = tally,
            None => self.note_fault("in-flight charge", self.in_flight_reserved.bytes, charge),
        }
        let Some(owner) = owner.filter(|_| charge > 0) else {
            return;
        };
        let held = self.session_reserved(owner.session_id);
        match held.checked_add(charge) {
            Some(total) => {
                self.session_reserved.insert(owner.session_id, total);
            }
            None => self.note_fault("session charge", held, charge),
        }
    }

    fn settle_in_flight(&mut self, owner: Option<BodyWorkOwner>, charge: u64) {
        match self.in_flight_reserved.checked_sub(charge) {
            Some(tally) => self.in_flight_reserved = tally,
            None => self.note_fault(
                "in-flight settlement",
                self.in_flight_reserved.bytes,
                charge,
            ),
        }
        let Some(owner) = owner.filter(|_| charge > 0) else {
            return;
        };
        let held = self.session_reserved(owner.session_id);
        match held.checked_sub(charge) {
            Some(0) => {
                self.session_reserved.remove(&owner.session_id);
            }
            Some(total) => {
                self.session_reserved.insert(owner.session_id, total);
            }
            None => self.note_fault("session settlement", held, charge),
        }
    }

    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(in crate::zakura::block_sync) fn set_estimate_floor(&mut self, floor: u64) {
        self.floor_estimate_bytes = floor.max(1);
    }

    fn estimate_bytes(&self, estimate: BlockSizeEstimate) -> u64 {
        estimate_bytes_with(estimate, self.floor_estimate_bytes)
    }

    // ---- extend and refresh ----

    /// Add scoped `(height, hash, size)` items to `pending`.
    /// Insert a height above `floor` only when no pending or in-flight item owns it.
    /// Return the number of inserted heights. Inserting any height wakes waiters.
    pub(in crate::zakura::block_sync) fn extend(
        &mut self,
        scope: BodyWorkAuthority,
        items: impl IntoIterator<Item = (block::Height, block::Hash, BlockSizeEstimate)>,
    ) -> (usize, WorkWake) {
        let mut inserted = 0usize;
        self.current_authority = Some(scope);
        for (height, hash, size) in items {
            if height <= self.floor {
                continue;
            }
            let estimated_bytes = self.estimate_bytes(size);
            if let Some(item) = self.pending.get_mut(&height) {
                if item.hash == hash
                    && item.scope == scope
                    && !matches!(size, BlockSizeEstimate::Unknown)
                {
                    item.estimated_bytes = estimated_bytes.max(item.observed_bytes);
                }
                continue;
            }
            if self.in_flight.contains_key(&height) {
                continue;
            }
            self.pending.insert(
                height,
                WorkItem {
                    scope,
                    owner: None,
                    provisional: false,
                    hash,
                    estimated_bytes,
                    observed_bytes: 0,
                    retry_estimated_bytes: None,
                    retry_after_tip: None,
                    budget: BlockBudgetLedger::Released,
                    answered_elsewhere: false,
                },
            );
            inserted += 1;
        }
        let wake = WorkWake {
            available: inserted > 0,
            ..WorkWake::default()
        };
        (inserted, wake)
    }

    /// Refresh scheduling metadata without rewriting an issued reservation.
    /// A changed pending estimate wakes waiters.
    pub(in crate::zakura::block_sync) fn refresh_size_estimates(
        &mut self,
        scope: BodyWorkAuthority,
        items: impl IntoIterator<Item = (block::Height, block::Hash, BlockSizeEstimate)>,
    ) -> WorkWake {
        let mut changed = false;
        for (height, hash, size) in items {
            if matches!(size, BlockSizeEstimate::Unknown) {
                continue;
            }
            let estimated = self.estimate_bytes(size);
            if let Some(item) = self.pending.get_mut(&height) {
                if item.scope == scope && item.hash == hash {
                    let estimated = estimated.max(item.observed_bytes);
                    changed |= item.estimated_bytes != estimated;
                    item.estimated_bytes = estimated;
                }
            } else if let Some(item) = self.in_flight.get_mut(&height) {
                if item.scope.body_work_epoch == scope.body_work_epoch && item.hash == hash {
                    item.retry_estimated_bytes = Some(estimated.max(item.observed_bytes));
                }
            }
        }
        WorkWake {
            available: changed,
            ..WorkWake::default()
        }
    }

    /// Reauthorize queued work while in-flight requests retain their registered
    /// owners. Always wakes waiters.
    pub(in crate::zakura::block_sync) fn refresh_authority(
        &mut self,
        authority: BodyWorkAuthority,
    ) -> WorkWake {
        self.current_authority = Some(authority);
        for item in self.pending.values_mut() {
            item.scope = authority;
        }
        WorkWake {
            available: true,
            ..WorkWake::default()
        }
    }

    // ---- take and publication ----

    /// Move up to `max` contiguous-ascending `pending` heights within
    /// `low..=high` from `pending` to `in_flight`, returned in ascending order.
    ///
    /// The taken items keep their owner and publication state. "Contiguous-ascending"
    /// stops at the first gap, so the returned chunk maps to a single
    /// `BlockRangeRequest`. `high` is **NOT** clamped to the floor.
    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn take_in_range(
        &mut self,
        low: block::Height,
        high: block::Height,
        max: usize,
    ) -> Vec<(block::Height, WorkItem)> {
        if max == 0 || low > high {
            return Vec::new();
        }
        let mut taken: Vec<(block::Height, WorkItem)> = Vec::new();
        let mut next_expected: Option<block::Height> = None;
        let mut scope = None;
        for (height, item) in self.pending.range(low..=high) {
            if item.retry_after_tip.is_some_and(|tip| self.floor <= tip) {
                break;
            }
            if scope.is_some_and(|scope| scope != item.scope) {
                break;
            }
            if let Some(expected) = next_expected {
                if *height != expected {
                    break;
                }
            }
            taken.push((*height, *item));
            scope = Some(item.scope);
            if taken.len() >= max {
                break;
            }
            // Stop the run at the end of the height space rather than overflowing.
            match height.0.checked_add(1) {
                Some(raw) => next_expected = Some(block::Height(raw)),
                None => break,
            }
        }
        if let Some(authority) = self.current_authority {
            for (_, item) in &mut taken {
                item.scope = authority;
            }
        }
        for (height, item) in &taken {
            self.pending.remove(height);
            self.charge_in_flight(item.owner, item.budget.reserved_charge());
            self.in_flight.insert(*height, *item);
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
    ///
    /// Every taken item becomes a provisional, unpublished claim of `attempt`'s
    /// exact owner. A reset followed by a new take cannot be undone by this
    /// attempt's cleanup. The take charges nothing: the attempt's own funding
    /// holds its bytes until publication.
    pub(in crate::zakura::block_sync) fn take_budgeted(
        &mut self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
        attempt: (u64, std::num::NonZeroU64),
    ) -> Vec<(block::Height, WorkItem)> {
        // An empty count or inverted range is a caller bug, not a real "nothing to
        // take": every caller computes `low <= high` and a positive count before
        // calling. Assert it in debug/test builds; still return empty in release so
        // a miscomputation degrades to a no-op rather than panicking a live node.
        debug_assert!(
            max_count > 0 && low <= high,
            "take_budgeted requires a positive count and low <= high, \
             got max_count={max_count}, low={low:?}, high={high:?}"
        );
        if max_count == 0 || low > high {
            return Vec::new();
        }
        let mut taken: Vec<(block::Height, WorkItem)> = Vec::new();
        let mut estimated_bytes = 0u64;
        let mut next_expected: Option<block::Height> = None;
        let mut scope = None;
        for (height, item) in self.pending.range(low..=high) {
            if item.retry_after_tip.is_some_and(|tip| self.floor <= tip) {
                break;
            }
            if scope.is_some_and(|scope| scope != item.scope) {
                break;
            }
            if let Some(expected) = next_expected {
                if *height != expected {
                    break;
                }
            }

            let next_estimated_bytes = estimated_bytes.saturating_add(item.estimated_bytes);
            if !taken.is_empty() && next_estimated_bytes > max_estimated_bytes {
                break;
            }

            taken.push((*height, *item));
            scope = Some(item.scope);
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
        if let Some(authority) = self.current_authority {
            for (_, item) in &mut taken {
                item.scope = authority;
            }
        }
        for (_, item) in &mut taken {
            item.owner = Some(item.scope.bind(attempt.0, attempt.1));
            item.provisional = true;
            item.answered_elsewhere = false;
        }
        for (height, item) in &taken {
            self.pending.remove(height);
            self.charge_in_flight(item.owner, item.budget.reserved_charge());
            self.in_flight.insert(*height, *item);
        }
        taken
    }

    /// Put back an unpublished claim. Only heights that `owner` still holds
    /// provisionally move back to `pending`, so a stale claim cannot return a
    /// replacement owner's work. Returns no wake: the returning routine would
    /// otherwise wake itself, and the failure that freed the work already woke
    /// other routines. Releases no ledger charge: the attempt's own funding
    /// still holds its bytes.
    pub(in crate::zakura::block_sync) fn return_unpublished(
        &mut self,
        items: &[(block::Height, WorkItem)],
    ) {
        for (height, taken) in items {
            if !self.in_flight.get(height).is_some_and(|item| {
                item.owner == taken.owner && item.owner.is_some() && item.provisional
            }) {
                continue;
            }
            if let Some(mut item) = self.in_flight.remove(height) {
                self.settle_in_flight(item.owner, item.budget.reserved_charge());
                item.owner = None;
                if let Some(estimate) = item.retry_estimated_bytes.take() {
                    item.estimated_bytes = estimate.max(item.observed_bytes);
                }
                item.provisional = false;
                self.pending.insert(*height, item);
            }
        }
    }

    /// Whether `owner` still holds every item of this nonempty claim provisionally,
    /// with the hash it took.
    pub(in crate::zakura::block_sync) fn is_unpublished_claim(
        &self,
        owner: BodyWorkOwner,
        items: &[(block::Height, WorkItem)],
    ) -> bool {
        !items.is_empty()
            && items.iter().all(|(height, taken)| {
                self.in_flight.get(height).is_some_and(|item| {
                    item.owner == Some(owner) && item.hash == taken.hash && item.provisional
                })
            })
    }

    /// Publish a claim that [`Self::is_unpublished_claim`] accepted: its funded
    /// estimate transfers into the ledger. Returns the transferred bytes.
    /// A missing item returns `None` after accounting for the transferred prefix;
    /// the runtime must close admission and retain any unmatched funding.
    pub(in crate::zakura::block_sync) fn publish_claim(
        &mut self,
        items: &[(block::Height, WorkItem)],
    ) -> Option<u64> {
        let mut transferred = 0u64;
        let mut complete = true;
        for (height, _) in items {
            let Some(item) = self.in_flight.get_mut(height) else {
                complete = false;
                break;
            };
            item.budget = BlockBudgetLedger::reserved(item.estimated_bytes);
            item.provisional = false;
            let charge = item.estimated_bytes;
            let owner = item.owner;
            self.charge_in_flight(owner, charge);
            transferred = self.add_to_total(transferred, charge);
        }
        self.charge_ledger(transferred);
        complete.then_some(transferred)
    }

    /// Whether `owner` still holds a reservation on every height of its claim.
    /// The writer may start the frame only while this holds.
    pub(in crate::zakura::block_sync) fn is_published_claim(
        &self,
        owner: BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> bool {
        heights.into_iter().all(|height| {
            self.in_flight
                .get(&height)
                .is_some_and(|item| item.owner == Some(owner) && item.budget.is_reserved())
        })
    }

    // ---- receipt ----

    /// End an active reservation at receipt. `answered_elsewhere` records that
    /// another attempt's body ended this owner's reservation. Returns the bytes
    /// that left the ledger, or `None` when `owner` does not hold a reservation on
    /// `height`.
    pub(in crate::zakura::block_sync) fn end_reservation_at_receipt(
        &mut self,
        owner: BodyWorkOwner,
        height: block::Height,
        answered_elsewhere: bool,
    ) -> Option<u64> {
        let item = self.in_flight.get_mut(&height)?;
        if item.owner != Some(owner) || !item.budget.is_reserved() {
            return None;
        }
        item.answered_elsewhere = answered_elsewhere;
        let released = item.budget.release_reserved();
        self.settle_ledger(released);
        self.settle_in_flight(Some(owner), released);
        Some(released)
    }

    /// End the reservation of the published request that holds `height` for
    /// `hash`, recording that another attempt's body answered it. Returns that
    /// owner and the bytes that left the ledger, or `None` when no published
    /// request reserves `height` for `hash`. Pending work has no owner, so it
    /// returns `None`. A request reissued for another hash keeps its reservation.
    pub(in crate::zakura::block_sync) fn end_competing_reservation_at_receipt(
        &mut self,
        height: block::Height,
        hash: block::Hash,
    ) -> Option<(BodyWorkOwner, u64)> {
        let owner = self
            .in_flight
            .get(&height)
            .filter(|item| !item.provisional && item.hash == hash)?
            .owner?;
        self.end_reservation_at_receipt(owner, height, true)
            .map(|charge| (owner, charge))
    }

    /// End any reservation `owner` holds on a received height. Returns the bytes
    /// that left the ledger, or `None` when `owner` does not hold the in-flight height.
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(in crate::zakura::block_sync) fn claim_received(
        &mut self,
        owner: BodyWorkOwner,
        height: block::Height,
    ) -> Option<u64> {
        let item = self.in_flight.get_mut(&height)?;
        if item.owner != Some(owner) {
            return None;
        }
        let released = item.budget.release_reserved();
        self.settle_in_flight(Some(owner), released);
        self.settle_ledger(released);
        Some(released)
    }

    /// Return an exact received attempt without releasing its request credit twice.
    /// The height waits for verified progress past `verified_tip` before a retry.
    /// Returns `None` when the attempt no longer owns that received body.
    pub(in crate::zakura::block_sync) fn defer_received(
        &mut self,
        owner: BodyWorkOwner,
        height: block::Height,
        hash: block::Hash,
        bytes: u64,
        verified_tip: block::Height,
    ) -> Option<WorkWake> {
        let Entry::Occupied(entry) = self.in_flight.entry(height) else {
            return None;
        };
        let item = entry.get();
        if height <= self.floor
            || item.owner != Some(owner)
            || item.hash != hash
            || item.budget != BlockBudgetLedger::Released
        {
            return None;
        }
        let mut item = entry.remove();
        item.observed_bytes = item.observed_bytes.max(bytes);
        item.estimated_bytes = item
            .retry_estimated_bytes
            .take()
            .unwrap_or(item.estimated_bytes)
            .max(item.observed_bytes);
        item.retry_after_tip = Some(verified_tip);
        item.owner = None;
        let revocation = std::mem::take(&mut item.answered_elsewhere);
        self.pending.insert(height, item);
        Some(WorkWake {
            revocation,
            ..WorkWake::default()
        })
    }

    // ---- return, expiry, revocation, and peer drop ----

    /// Release reservations that `owner` holds, leaving received heights in place.
    /// Returns the released bytes.
    pub(in crate::zakura::block_sync) fn release_reserved_heights(
        &mut self,
        owner: BodyWorkOwner,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        let mut released = 0u64;
        for height in heights {
            let Some(item) = self.in_flight.get_mut(&height) else {
                continue;
            };
            if item.owner != Some(owner) {
                continue;
            }
            let charge = item.budget.release_reserved();
            released = self.add_to_total(released, charge);
            self.settle_in_flight(Some(owner), charge);
        }
        self.settle_ledger(released);
        released
    }

    /// Settle `owner`'s unreceived heights: discard committed ones and move
    /// reserved ones back to `pending`. Heights another owner holds are left
    /// untouched. Returning any height wakes waiters.
    ///
    /// IO adds the heights of a queued frame that expiry skipped, and records
    /// that observation in the outcome.
    pub(in crate::zakura::block_sync) fn return_owned(
        &mut self,
        owner: BodyWorkOwner,
        heights: BTreeSet<block::Height>,
    ) -> (WorkReturnOutcome, WorkWake) {
        let mut outcome = WorkReturnOutcome::default();
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
            let Some(item) = self.in_flight.get(&height) else {
                if self.pending.contains_key(&height) {
                    outcome.already_pending_count = outcome.already_pending_count.saturating_add(1);
                } else {
                    outcome.missing_count = outcome.missing_count.saturating_add(1);
                }
                continue;
            };
            if item.owner != Some(owner) {
                outcome.missing_count = outcome.missing_count.saturating_add(1);
                continue;
            }
            if height <= self.floor {
                let mut item = self
                    .in_flight
                    .remove(&height)
                    .expect("owned item exists because it was just checked");
                let charge = item.budget.release_reserved();
                self.settle_in_flight(Some(owner), charge);
                outcome.released_bytes = self.add_to_total(outcome.released_bytes, charge);
                outcome.committed_count = outcome.committed_count.saturating_add(1);
                continue;
            }
            match item.budget {
                BlockBudgetLedger::Released => {
                    outcome.released_count = outcome.released_count.saturating_add(1);
                    continue;
                }
                BlockBudgetLedger::Reserved(_) => {}
            }
            let mut item = self
                .in_flight
                .remove(&height)
                .expect("reserved item exists because it was just checked");
            let charge = item.budget.release_reserved();
            self.settle_in_flight(Some(owner), charge);
            outcome.released_bytes = self.add_to_total(outcome.released_bytes, charge);
            item.owner = None;
            if let Some(estimate) = item.retry_estimated_bytes.take() {
                item.estimated_bytes = estimate.max(item.observed_bytes);
            }
            outcome.returned_count = outcome.returned_count.saturating_add(1);
            self.pending.insert(height, item);
        }
        self.settle_ledger(outcome.released_bytes);
        let wake = WorkWake {
            available: outcome.returned_count > 0,
            ..WorkWake::default()
        };
        (outcome, wake)
    }

    // ---- floor advance and reset ----

    /// Garbage-collect committed heights: raise the floor to `max(self.floor,
    /// floor)` and drop every `pending`/`in_flight` entry `<= floor`.
    ///
    /// Releases request-estimate bytes still reserved for unreceived heights.
    /// Received bodies carry no charge here; the Sequencer's buffers own them.
    /// Dropping a reserved or answered-elsewhere height is a revocation.
    pub(in crate::zakura::block_sync) fn advance_floor(
        &mut self,
        floor: block::Height,
    ) -> WorkDelta {
        self.floor = self.floor.max(floor);
        let floor = self.floor;
        // Pop only the committed `<= floor` prefix from each map. `pending` can hold
        // the entire header-ahead lag (100k+ heights), so a `retain` over the whole
        // map on every floor advance is O(total) and serializes the work-queue lock;
        // popping the prefix is O(removed · log n).
        let mut released = 0u64;
        while let Some((&height, _)) = self.pending.first_key_value() {
            if height > floor {
                break;
            }
            let (_, mut item) = self
                .pending
                .pop_first()
                .expect("first_key_value returned Some");
            released = self.add_to_total(released, item.budget.release_reserved());
        }
        let mut revocation = false;
        while let Some((&height, _)) = self.in_flight.first_key_value() {
            if height > floor {
                break;
            }
            let (_, mut item) = self
                .in_flight
                .pop_first()
                .expect("first_key_value returned Some");
            revocation |= item.budget.is_reserved() || item.answered_elsewhere;
            let charge = item.budget.release_reserved();
            self.settle_in_flight(item.owner, charge);
            released = self.add_to_total(released, charge);
        }
        self.settle_ledger(released);
        WorkDelta {
            released_bytes: released,
            wake: WorkWake {
                revocation,
                ..WorkWake::default()
            },
        }
    }

    /// Frontier reset: pin the floor and drop every `pending`/`in_flight` entry
    /// `> floor` (their buffers were dropped; the producer re-fills via the next
    /// query). Releases charges as in [`Self::advance_floor`].
    ///
    /// IO resets the affected request writes under the same lock first.
    pub(in crate::zakura::block_sync) fn reset_above(&mut self, floor: block::Height) -> WorkDelta {
        self.floor = floor;
        // Pop only the `> floor` suffix from each map (O(removed · log n)); see the
        // note in `advance_floor` on why a full-map `retain` is too expensive here.
        let mut released = 0u64;
        while let Some((&height, _)) = self.pending.last_key_value() {
            if height <= floor {
                break;
            }
            let (_, mut item) = self
                .pending
                .pop_last()
                .expect("last_key_value returned Some");
            released = self.add_to_total(released, item.budget.release_reserved());
        }
        let mut revocation = false;
        while let Some((&height, _)) = self.in_flight.last_key_value() {
            if height <= floor {
                break;
            }
            let (_, mut item) = self
                .in_flight
                .pop_last()
                .expect("last_key_value returned Some");
            revocation |= item.budget.is_reserved() || item.answered_elsewhere;
            let charge = item.budget.release_reserved();
            self.settle_in_flight(item.owner, charge);
            released = self.add_to_total(released, charge);
        }
        self.settle_ledger(released);
        WorkDelta {
            released_bytes: released,
            wake: WorkWake {
                revocation,
                ..WorkWake::default()
            },
        }
    }

    // ---- immutable observations ----

    /// Bound a correction refresh to work already queued locally.
    pub(in crate::zakura::block_sync) fn queued_bounds(
        &self,
    ) -> Option<(block::Height, block::Height)> {
        Some((
            [
                self.pending.first_key_value(),
                self.in_flight.first_key_value(),
            ]
            .into_iter()
            .flatten()
            .map(|(height, _)| *height)
            .min()?,
            [
                self.pending.last_key_value(),
                self.in_flight.last_key_value(),
            ]
            .into_iter()
            .flatten()
            .map(|(height, _)| *height)
            .max()?,
        ))
    }

    pub(in crate::zakura::block_sync) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(in crate::zakura::block_sync) fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    /// Reserved bytes and reserved items in `in_flight` above `floor`.
    ///
    /// Admission reads this once per issued request, so it must not scan the
    /// map. `in_flight` holds heights above the queue floor, which normally
    /// matches `floor`, so only the heights between the two floors are scanned.
    pub(in crate::zakura::block_sync) fn reserved_above(&self, floor: block::Height) -> (u64, u64) {
        let mut above = self.in_flight_reserved;
        for (_, item) in self.in_flight.range(..=floor) {
            // The tally falls below its items only after a transition latched a fault.
            if let Some(remaining) = above.checked_sub(item.budget.reserved_charge()) {
                above = remaining;
            }
        }
        debug_assert_eq!(
            (above.bytes, above.items),
            self.in_flight
                .range((std::ops::Bound::Excluded(floor), std::ops::Bound::Unbounded))
                .fold((0u64, 0u64), |(bytes, count), (_, item)| {
                    let charge = item.budget.reserved_charge();
                    if charge == 0 {
                        (bytes, count)
                    } else {
                        (bytes.saturating_add(charge), count.saturating_add(1))
                    }
                }),
            "the in-flight reservation tally matches a scan",
        );
        (above.bytes, above.items)
    }

    /// Reserved bytes that `session_id`'s in-flight items hold. O(log
    /// sessions): the tally is maintained at each ledger transition.
    pub(in crate::zakura::block_sync) fn session_reserved(&self, session_id: u64) -> u64 {
        self.session_reserved.get(&session_id).copied().unwrap_or(0)
    }

    /// Sum of reserved request-estimate bytes across `pending` + `in_flight`.
    /// O(1): the counter is maintained at each ledger transition.
    pub(in crate::zakura::block_sync) fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    /// Ground-truth O(pending + in_flight) recomputation of [`Self::reserved_bytes`].
    #[cfg(any(test, feature = "p2p-fuzz"))]
    pub(in crate::zakura::block_sync) fn reserved_bytes_scanned(&self) -> u64 {
        self.pending
            .values()
            .chain(self.in_flight.values())
            .map(|item| item.budget.reserved_charge())
            .fold(0u64, u64::saturating_add)
    }

    /// Number of contiguous runs across `pending`.
    pub(in crate::zakura::block_sync) fn pending_run_count(&self) -> usize {
        let mut runs = 0usize;
        let mut previous: Option<block::Height> = None;
        for height in self.pending.keys() {
            let contiguous =
                previous.and_then(|previous| previous.0.checked_add(1)) == Some(height.0);
            if !contiguous {
                runs += 1;
            }
            previous = Some(*height);
        }
        runs
    }

    pub(in crate::zakura::block_sync) fn min_pending(&self) -> Option<block::Height> {
        self.pending.keys().next().copied()
    }

    pub(in crate::zakura::block_sync) fn first_pending_in_range(
        &self,
        low: block::Height,
        high: block::Height,
    ) -> Option<block::Height> {
        if low > high {
            return None;
        }
        self.pending
            .range(low..=high)
            .next()
            .map(|(height, _)| *height)
    }

    pub(in crate::zakura::block_sync) fn max_in_flight(&self) -> Option<block::Height> {
        self.in_flight.keys().next_back().copied()
    }

    /// The lowest height above the floor that is in neither `pending` nor
    /// `in_flight`. See [`WorkQueue::first_unclaimed_above`](super::WorkQueue::first_unclaimed_above).
    pub(in crate::zakura::block_sync) fn first_unclaimed_above(
        &self,
        floor: block::Height,
    ) -> Option<block::Height> {
        let anchor = self.floor.max(floor);
        let start = anchor.next().ok()?;

        let max_claimed = self
            .pending
            .keys()
            .next_back()
            .copied()
            .max(self.in_flight.keys().next_back().copied());
        let Some(max_claimed) = max_claimed.filter(|max| *max >= start) else {
            return Some(start);
        };

        // The length check only decides contiguity when *every* key is above the
        // anchor, so it is gated on the lowest claimed height being `start` itself.
        // Entries at or below the anchor otherwise inflate the count and make a
        // sparse range look contiguous: a forward `reset_above` pins the floor and
        // pops only the `> floor` suffix, so it leaves the whole
        // `(old_floor, new_floor]` prefix claimed at or below the new floor. A
        // lagging caller `floor` mirror is excluded the same way.
        let min_claimed = match (
            self.pending.keys().next().copied(),
            self.in_flight.keys().next().copied(),
        ) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (single, None) | (None, single) => single,
        };
        if min_claimed == Some(start) {
            let span = u64::from(max_claimed.0 - anchor.0);
            let claimed = self.pending.len().saturating_add(self.in_flight.len()) as u64;
            if claimed == span {
                return max_claimed.next().ok();
            }
        }

        let mut pending = self.pending.range(start..).peekable();
        let mut in_flight = self.in_flight.range(start..).peekable();
        let mut expected = start;
        loop {
            let next = match (pending.peek(), in_flight.peek()) {
                (None, None) => return Some(expected),
                (Some((height, _)), None) | (None, Some((height, _))) => **height,
                (Some((left, _)), Some((right, _))) => **left.min(right),
            };
            if next > expected {
                return Some(expected);
            }
            pending.next_if(|(height, _)| **height == next);
            in_flight.next_if(|(height, _)| **height == next);
            expected = next.next().ok()?;
        }
    }

    /// Current authority and identity, including work returned by a timed-out request.
    pub(in crate::zakura::block_sync) fn item_for_height(
        &self,
        height: block::Height,
    ) -> Option<WorkItem> {
        self.pending
            .get(&height)
            .or_else(|| self.in_flight.get(&height))
            .copied()
    }

    /// Published request owner for a height. A provisional claim owns nothing yet.
    pub(in crate::zakura::block_sync) fn owner_for_height(
        &self,
        height: block::Height,
    ) -> Option<BodyWorkOwner> {
        self.pending
            .get(&height)
            .or_else(|| self.in_flight.get(&height))
            .filter(|item| !item.provisional)
            .and_then(|item| item.owner)
    }

    pub(in crate::zakura::block_sync) fn pending_contains(&self, height: block::Height) -> bool {
        self.pending.contains_key(&height)
    }

    pub(in crate::zakura::block_sync) fn in_flight_contains(&self, height: block::Height) -> bool {
        self.in_flight.contains_key(&height)
    }

    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn reserved_in_flight_charge(
        &self,
        height: block::Height,
    ) -> Option<u64> {
        self.in_flight.get(&height).and_then(|item| {
            item.budget
                .is_reserved()
                .then(|| item.budget.reserved_charge())
        })
    }
}
