//! The retained-path lease registry, ported from zakura-state's
//! `RetainedPathLeaseRegistry`.
//!
//! The port keeps production's capacity classes, refusals, idle sweep and
//! continuation cache. It differs in three ways. Maps are `BTreeMap`s, so
//! sweeps and evictions run in a deterministic order. Time is
//! `zakura_rt::time::Instant`, which is virtual inside a run. And there are no
//! reservations: production reserves a slot before it resolves the path, but
//! here an acquire runs atomically at its completion (the contract's
//! excluded-histories rule), so a reservation never outlives one acquire and
//! no other operation can see it.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use zakura_chain::block;
use zakura_header_chain::{Frontier, HeaderWorkAuthority, SourceId};
use zakura_node_services::header_chain::{AcquirePathReply, ServingCapacitySignal};
use zakura_rt::time::Instant;

/// Most leases held at once, zakura-state's `MAX_RETAINED_PATH_LEASES`.
pub const MAX_RETAINED_PATH_LEASES: usize = zakura_header_chain::MAX_STAGED_TARGETS_V1;

/// How long a lease lives without a read, zakura-state's
/// `RETAINED_PATH_LEASE_IDLE`.
pub const RETAINED_PATH_LEASE_IDLE: Duration = Duration::from_secs(30);

/// The most headers one page or one bounded path covers, zakura-state's
/// `MAX_HEADER_SYNC_HEIGHT_RANGE`.
pub const MAX_HEADER_SYNC_HEIGHT_RANGE: u32 = 4_000;

/// Which slots a lease may use. Ordinary leases use all but one slot. A
/// path short enough for one page may also use the last, reserve slot.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Capacity {
    General,
    Bounded,
}

impl Capacity {
    /// `Bounded` if the path from `ancestor` to `target` fits one page.
    pub(super) fn for_range(ancestor: Frontier, target: Frontier) -> Self {
        let count = target.height.0.saturating_sub(ancestor.height.0);
        if count <= MAX_HEADER_SYNC_HEIGHT_RANGE {
            Capacity::Bounded
        } else {
            Capacity::General
        }
    }

    /// How many slots the class may fill.
    fn limit(self) -> usize {
        match self {
            Capacity::General => MAX_RETAINED_PATH_LEASES.saturating_sub(1),
            Capacity::Bounded => MAX_RETAINED_PATH_LEASES,
        }
    }
}

/// Where a lease's next page starts. Finality never leaves the genesis
/// anchor, so production's finalized position never arises.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Position {
    /// The next page starts at `retained_path[next]`.
    Retained { next: usize },
    /// Every header has been served.
    Complete,
}

/// One peer's lease, or a consumed lease kept for continuation.
#[derive(Clone, Debug)]
pub(super) struct Cursor {
    capacity_signal: Option<ServingCapacitySignal>,
    pub(super) lease_id: u64,
    pub(super) session_id: u64,
    pub(super) target: Frontier,
    pub(super) scope: HeaderWorkAuthority,
    pub(super) position: Position,
    /// The last header served, or the common ancestor before any page.
    pub(super) last_frontier: Frontier,
    /// Hashes above the common ancestor, parent first.
    pub(super) retained_path: Arc<[block::Hash]>,
    idle_deadline: Instant,
    pub(super) capacity: Capacity,
}

/// What a new lease covers.
#[derive(Debug)]
pub(super) struct Spec {
    pub(super) session_id: u64,
    pub(super) target: Frontier,
    pub(super) common_ancestor: Frontier,
    pub(super) scope: HeaderWorkAuthority,
    pub(super) position: Position,
    pub(super) retained_path: Arc<[block::Hash]>,
    pub(super) capacity: Capacity,
}

/// Every live lease, at most one per peer, and the continuation cache.
#[derive(Debug, Default)]
pub(super) struct Registry {
    next_lease_id: u64,
    by_peer: BTreeMap<SourceId, Cursor>,
    general_capacity: Option<ServingCapacitySignal>,
    fallback_capacity: Option<ServingCapacitySignal>,
    /// Consumed cursors kept so a peer's next page needs no new walk. They
    /// hold no capacity.
    continuations: BTreeMap<SourceId, Cursor>,
    /// How many acquires continued from a cached cursor.
    continuation_hits: u64,
}

impl Registry {
    /// Ends every lease past its idle deadline, whoever holds it.
    fn expire(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .by_peer
            .iter()
            .filter_map(|(peer, cursor)| (cursor.idle_deadline <= now).then_some(*peer))
            .collect();
        for peer in expired {
            self.remove_peer(peer);
        }
    }

    /// Removes `peer`'s lease, fires its signal and any capacity waiters.
    fn remove_peer(&mut self, peer: SourceId) -> Option<Cursor> {
        let mut cursor = self.by_peer.remove(&peer)?;
        if let Some(signal) = cursor.capacity_signal.take() {
            signal.release();
        }
        self.notify_capacity();
        Some(cursor)
    }

    /// Counts leases admitted to ordinary capacity.
    fn general_count(&self) -> usize {
        self.by_peer
            .values()
            .filter(|cursor| cursor.capacity == Capacity::General)
            .count()
    }

    /// Fires each class's waiting signal once the class has room.
    fn notify_capacity(&mut self) {
        if self.general_count() < Capacity::General.limit() {
            if let Some(signal) = self.general_capacity.take() {
                signal.release();
            }
        }
        if self.by_peer.len() < Capacity::Bounded.limit() {
            if let Some(signal) = self.fallback_capacity.take() {
                signal.release();
            }
        }
    }

    /// Sweeps expired leases, then admits `peer` to a class, as
    /// `RetainedPathLeaseRegistry::reserve` does. A peer that already holds a
    /// lease, or a full class, gets `CapacityBusy` with the signal it waits
    /// on.
    pub(super) fn admit(
        &mut self,
        peer: SourceId,
        now: Instant,
        capacity: Capacity,
    ) -> Result<Capacity, AcquirePathReply> {
        self.expire(now);
        if let Some(cursor) = self.by_peer.get_mut(&peer) {
            return Err(AcquirePathReply::CapacityBusy(
                cursor.capacity_signal.get_or_insert_default().clone(),
            ));
        }
        if self.general_count() < Capacity::General.limit() {
            return Ok(Capacity::General);
        }
        if capacity == Capacity::Bounded && self.by_peer.len() < Capacity::Bounded.limit() {
            return Ok(Capacity::Bounded);
        }
        let signal = match capacity {
            Capacity::General => &mut self.general_capacity,
            Capacity::Bounded => &mut self.fallback_capacity,
        };
        Err(AcquirePathReply::CapacityBusy(
            signal.get_or_insert_default().clone(),
        ))
    }

    /// The refusal for a reserve admission whose path needs an ordinary
    /// slot, as `reject_beyond_reserve` gives it.
    pub(super) fn beyond_reserve(&mut self) -> AcquirePathReply {
        let signal = self.general_capacity.get_or_insert_default().clone();
        self.notify_capacity();
        AcquirePathReply::CapacityBusy(signal)
    }

    /// Takes `peer`'s continuation, if it has one. A caller that then
    /// rejects it drops it, as production does.
    pub(super) fn take_continuation(&mut self, peer: SourceId) -> Option<Cursor> {
        self.continuations.remove(&peer)
    }

    /// Counts an acquire that continued from a cached cursor.
    pub(super) fn record_continuation(&mut self) {
        self.continuation_hits = self.continuation_hits.saturating_add(1);
    }

    /// Installs `peer`'s lease, as `commit_reservation` does. Returns its
    /// lease ID, or `None` if the ID counter ran out, which production
    /// answers with `Busy`.
    pub(super) fn commit(&mut self, peer: SourceId, spec: Spec, now: Instant) -> Option<u64> {
        let lease_id = self.next_lease_id.checked_add(1)?;
        self.next_lease_id = lease_id;
        self.by_peer.insert(
            peer,
            Cursor {
                capacity_signal: None,
                lease_id,
                session_id: spec.session_id,
                target: spec.target,
                scope: spec.scope,
                position: spec.position,
                last_frontier: spec.common_ancestor,
                retained_path: spec.retained_path,
                idle_deadline: now + RETAINED_PATH_LEASE_IDLE,
                capacity: spec.capacity,
            },
        );
        Some(lease_id)
    }

    /// Sweeps expired leases, then returns `peer`'s lease if it matches.
    pub(super) fn get(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        now: Instant,
    ) -> Option<Cursor> {
        self.expire(now);
        let cursor = self.by_peer.get(&peer)?;
        (cursor.session_id == session_id && cursor.lease_id == lease_id).then(|| cursor.clone())
    }

    /// Consumes `peer`'s lease after a page that ended at `last_frontier`, as
    /// `finish_page` does. An unfinished cursor moves to the continuation
    /// cache, which evicts its lowest lease ID when full. Returns `false` if
    /// the lease changed or expired, so the page isn't served.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn finish_page(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        expected_after: Frontier,
        position: Position,
        last_frontier: Frontier,
        now: Instant,
    ) -> bool {
        if self
            .by_peer
            .get(&peer)
            .is_some_and(|cursor| cursor.idle_deadline <= now)
        {
            self.remove_peer(peer);
            return false;
        }
        let Some(cursor) = self.by_peer.get_mut(&peer) else {
            return false;
        };
        if cursor.session_id != session_id
            || cursor.lease_id != lease_id
            || cursor.last_frontier != expected_after
        {
            return false;
        }
        cursor.position = position;
        cursor.last_frontier = last_frontier;
        let cursor = self.remove_peer(peer).expect("the lease was checked above");
        if cursor.position != Position::Complete {
            if self.continuations.len() >= MAX_RETAINED_PATH_LEASES {
                let oldest = self
                    .continuations
                    .iter()
                    .min_by_key(|(_, cursor)| cursor.lease_id)
                    .map(|(peer, _)| *peer)
                    .expect("a full continuation cache is nonempty");
                self.continuations.remove(&oldest);
            }
            self.continuations.insert(peer, cursor);
        }
        true
    }

    /// Ends `peer`'s lease if session, lease and scope all match. Returns
    /// whether it did.
    pub(super) fn release(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: HeaderWorkAuthority,
    ) -> bool {
        let matches = self.by_peer.get(&peer).is_some_and(|cursor| {
            cursor.session_id == session_id && cursor.lease_id == lease_id && cursor.scope == scope
        });
        if matches {
            self.remove_peer(peer);
        }
        matches
    }

    /// How many leases are live.
    pub(super) fn live(&self) -> usize {
        self.by_peer.len()
    }

    /// The peers with a cached continuation, in peer order.
    pub(super) fn continued(&self) -> Vec<SourceId> {
        self.continuations.keys().copied().collect()
    }

    /// How many acquires continued from a cached cursor.
    pub(super) fn continuation_hits(&self) -> u64 {
        self.continuation_hits
    }
}
