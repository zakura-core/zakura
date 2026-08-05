use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
};

use zakura_header_chain::{
    EngineSnapshot, Frontier, HeaderLocator, HeaderSyncWorkOwner, HeaderWorkAuthority, SourceId,
    MAX_STAGED_TARGETS_V1,
};

use super::super::{
    AuxSchema, HeaderEntry, HeaderSyncRequestId, Status, ZakuraPeerId, MAX_HS_RANGE,
};

/// Exact aggregate header count owned across receiving, preparation, and application.
// `MAX_HS_RANGE` is 4,000 and therefore fits every supported `usize` target.
pub(in crate::zakura::header_sync) const HEADER_CHUNK_BUDGET_CAPACITY_V1: usize =
    MAX_HS_RANGE as usize;

/// Fair per-request share when all bounded target slots are active.
pub(in crate::zakura::header_sync) const MAX_HEADER_CHUNK_RESERVATION_V1: usize =
    HEADER_CHUNK_BUDGET_CAPACITY_V1 / MAX_STAGED_TARGETS_V1;

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct HeaderChunkUsage {
    reserved: usize,
    owned: usize,
}

#[derive(Debug)]
struct HeaderChunkBudgetInner {
    capacity: usize,
    usage: Mutex<HeaderChunkUsage>,
}

#[derive(Clone, Debug)]
struct HeaderChunkBudget(Arc<HeaderChunkBudgetInner>);

impl Default for HeaderChunkBudget {
    fn default() -> Self {
        Self(Arc::new(HeaderChunkBudgetInner {
            capacity: HEADER_CHUNK_BUDGET_CAPACITY_V1,
            usage: Mutex::new(HeaderChunkUsage::default()),
        }))
    }
}

impl HeaderChunkBudget {
    fn usage(&self) -> HeaderChunkUsage {
        *self
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn remaining(&self) -> usize {
        let usage = self.usage();
        self.0
            .capacity
            .saturating_sub(usage.reserved.saturating_add(usage.owned))
    }

    fn claimed(&self) -> usize {
        let usage = self.usage();
        usage.reserved.saturating_add(usage.owned)
    }

    fn reserve(&self, count: usize) -> Option<HeaderCountReservation> {
        if count == 0 {
            return None;
        }
        let mut usage = self
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if count
            > self
                .0
                .capacity
                .saturating_sub(usage.reserved.saturating_add(usage.owned))
        {
            return None;
        }
        usage.reserved = usage.reserved.checked_add(count)?;
        self.publish(*usage);
        drop(usage);
        Some(HeaderCountReservation(Arc::new(
            HeaderCountReservationInner {
                budget: self.clone(),
                remaining: AtomicUsize::new(count),
            },
        )))
    }

    fn consume(&self, reserved: usize, owned: usize) -> Result<Option<HeaderCapacityLease>, ()> {
        if owned > reserved {
            return Err(());
        }
        let mut usage = self
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if usage.reserved < reserved {
            return Err(());
        }
        let new_owned = usage.owned.checked_add(owned).ok_or(())?;
        usage.reserved -= reserved;
        usage.owned = new_owned;
        self.publish(*usage);
        drop(usage);
        Ok((owned != 0).then(|| {
            HeaderCapacityLease(Arc::new(HeaderCapacityLeaseInner {
                budget: self.clone(),
                count: owned,
            }))
        }))
    }

    fn release_reserved(&self, count: usize) {
        let mut usage = self
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(usage.reserved >= count);
        usage.reserved = usage.reserved.saturating_sub(count);
        self.publish(*usage);
    }

    fn release_owned(&self, count: usize) {
        let mut usage = self
            .0
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(usage.owned >= count);
        usage.owned = usage.owned.saturating_sub(count);
        self.publish(*usage);
    }

    fn publish(&self, usage: HeaderChunkUsage) {
        debug_assert!(usage.reserved.saturating_add(usage.owned) <= self.0.capacity);
        // Counts are bounded by 4,000 and are exactly representable as f64.
        metrics::gauge!("sync.header.chunk_budget.capacity").set(self.0.capacity as f64);
        metrics::gauge!("sync.header.chunk_budget.reserved").set(usage.reserved as f64);
        metrics::gauge!("sync.header.chunk_budget.owned").set(usage.owned as f64);
        metrics::gauge!("sync.header.chunk_budget.staged").set(usage.owned as f64);
    }
}

#[derive(Clone, Debug)]
struct HeaderCountReservation(Arc<HeaderCountReservationInner>);

#[derive(Debug)]
struct HeaderCountReservationInner {
    budget: HeaderChunkBudget,
    remaining: AtomicUsize,
}

impl HeaderCountReservation {
    fn remaining(&self) -> usize {
        self.0.remaining.load(AtomicOrdering::SeqCst)
    }

    fn consume(&self, owned: usize) -> Result<Option<HeaderCapacityLease>, ()> {
        let reserved = self.0.remaining.swap(0, AtomicOrdering::SeqCst);
        if reserved == 0 || owned > reserved {
            if reserved != 0 {
                self.0.remaining.store(reserved, AtomicOrdering::SeqCst);
            }
            return Err(());
        }
        match self.0.budget.consume(reserved, owned) {
            Ok(lease) => Ok(lease),
            Err(()) => {
                self.0.remaining.store(reserved, AtomicOrdering::SeqCst);
                Err(())
            }
        }
    }
}

impl PartialEq for HeaderCountReservation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for HeaderCountReservation {}

impl Drop for HeaderCountReservationInner {
    fn drop(&mut self) {
        let remaining = self.remaining.swap(0, AtomicOrdering::SeqCst);
        if remaining != 0 {
            self.budget.release_reserved(remaining);
        }
    }
}

#[derive(Clone, Debug)]
struct HeaderCapacityLease(Arc<HeaderCapacityLeaseInner>);

#[derive(Debug)]
struct HeaderCapacityLeaseInner {
    budget: HeaderChunkBudget,
    count: usize,
}

impl PartialEq for HeaderCapacityLease {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for HeaderCapacityLease {}

impl Drop for HeaderCapacityLeaseInner {
    fn drop(&mut self) {
        self.budget.release_owned(self.count);
    }
}

/// One peer's exact, session-bound target claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvertisedHeaderTarget {
    /// Durable generation and exact branch captured before locator work is scheduled.
    pub scope: HeaderWorkAuthority,
    /// Ordered-stream generation that supplied this status.
    pub session_id: u64,
    /// Exact advisory snapshot supplied by the peer.
    pub status: Status,
}

impl AdvertisedHeaderTarget {
    /// Whether this target still has header-generation and branch authority.
    ///
    /// Header work deliberately ignores the global state version because unrelated body commits
    /// can advance it without changing the selected header graph or finality anchor.
    pub fn is_current(&self, local: &EngineSnapshot) -> bool {
        self.scope.header_generation == local.header_generation
            && self.scope.branch.anchor_hash == local.frontiers.finalized.hash
            && self.scope.branch.target_tip_hash == self.status.selected_tip_hash
    }

    /// Compare claimed suffix work only when both snapshots use the same anchor.
    pub fn claimed_work_order(&self, local: &EngineSnapshot) -> Option<Ordering> {
        let local_anchor = local.frontiers.finalized;
        (self.status.work_anchor_height == local_anchor.height
            && self.status.work_anchor_hash == local_anchor.hash)
            .then(|| {
                self.status
                    .suffix_cumulative_work
                    .cmp(&local.header_best_score.suffix_work.as_u256())
            })
    }

    /// Whether this status names a different target that can actually serve a request.
    pub fn is_discovery_eligible(&self, local: &EngineSnapshot) -> bool {
        self.status.selected_tip_hash != local.frontiers.header_best.hash
            && self.status.max_headers_per_response != 0
            && self.status.max_inflight_requests != 0
            && self.status.max_message_bytes != 0
    }
}

/// One published request for an exact advertised target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveHeaderRequest {
    /// Admission purpose carried through the shared target lifecycle.
    pub purpose: HeaderTargetPurpose,
    /// Peer whose current session owns the request.
    pub peer: ZakuraPeerId,
    /// Stable source identity used by completion ownership.
    pub source: SourceId,
    /// Exact status snapshot being pursued.
    pub target: AdvertisedHeaderTarget,
    /// Exact coherent state locator sent in the request.
    pub sent_locator: HeaderLocator,
    /// Nonzero request correlation identifier.
    pub request_id: HeaderSyncRequestId,
    /// Durable generation and exact branch ownership fixed by the first request.
    pub owner: HeaderSyncWorkOwner,
    /// Exact authenticated intersection fixed by the first response.
    pub common_ancestor: Option<Frontier>,
    /// Complete response pages staged without intermediate state mutation.
    pub entries: Vec<HeaderEntry>,
    /// Exact phase of complete-target processing.
    pub phase: HeaderTargetPhase,
    /// Effective count bound preserved across continuation requests.
    pub max_header_count: u32,
    /// Requested auxiliary schema preserved across continuation requests.
    pub tree_aux_schema: AuxSchema,
}

/// Policy that distinguishes ordinary branch pursuit from exact auxiliary repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderTargetPurpose {
    /// Admit a complete parent-linked branch target.
    Normal,
    /// Redeliver auxiliary metadata for one exact selected header.
    SelectedAuxiliaryRepair {
        /// Selected target fixed by the durable repair context.
        selected_target: Frontier,
        /// Durable repair-signal generation that owns this attempt.
        repair_generation: u64,
    },
}

impl HeaderTargetPurpose {
    /// Return the target purpose's exact response-count requirement, when fixed.
    pub fn exact_header_count(&self) -> Option<usize> {
        match self {
            Self::Normal => None,
            Self::SelectedAuxiliaryRepair { .. } => Some(1),
        }
    }

    /// Return the selected target fixed by an auxiliary repair.
    pub fn selected_repair_target(&self) -> Option<Frontier> {
        match self {
            Self::Normal => None,
            Self::SelectedAuxiliaryRepair {
                selected_target, ..
            } => Some(*selected_target),
        }
    }
}

/// Reactor-owned phase that permits each target preparation and state submission once.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderTargetPhase {
    /// Response pages are still being received and staged.
    Receiving,
    /// The complete target is being validated outside the reactor.
    Preparing,
    /// Sealed evidence has passed the gate and one state call is pending.
    Applying,
}

impl ActiveHeaderRequest {
    /// Whether one page preserves the active phase, target, and exact ancestry.
    pub fn matches_response_page(
        &self,
        target_tip_hash: zakura_chain::block::Hash,
        returned_ancestor: Frontier,
    ) -> bool {
        let expected_ancestor = match self.common_ancestor {
            Some(_) => self.staged_tip(),
            None => self
                .sent_locator
                .entries()
                .iter()
                .copied()
                .find(|entry| *entry == returned_ancestor),
        };
        self.phase == HeaderTargetPhase::Receiving
            && self.target.status.selected_tip_hash == target_tip_hash
            && expected_ancestor == Some(returned_ancestor)
    }

    /// Whether one explicit outcome exactly matches the active request.
    pub fn accepts_outcome(
        &self,
        request_id: HeaderSyncRequestId,
        target_tip_hash: zakura_chain::block::Hash,
    ) -> bool {
        self.phase == HeaderTargetPhase::Receiving
            && self.request_id == request_id
            && self.target.status.selected_tip_hash == target_tip_hash
    }

    /// Select the continuation-only locator without changing this request's target.
    pub fn continuation_locator(
        &self,
        returned_suffix_tip: zakura_header_chain::Frontier,
    ) -> HeaderLocator {
        HeaderLocator::for_continuation(returned_suffix_tip)
    }

    /// Return the last staged frontier, inferred only from authenticated local heights.
    pub fn staged_tip(&self) -> Option<Frontier> {
        let ancestor = self.common_ancestor?;
        let last = self.entries.last()?;
        let count = u32::try_from(self.entries.len()).ok()?;
        let height = ancestor
            .height
            .0
            .checked_add(count)
            .filter(|height| *height <= zakura_chain::block::Height::MAX.0)
            .map(zakura_chain::block::Height)?;
        Some(Frontier::new(height, last.header.hash()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PeerWorkState {
    AwaitingLocator {
        target: Box<AdvertisedHeaderTarget>,
        priority: PeerWorkPriority,
    },
    Active(Box<ActiveHeaderRequest>),
}

/// Advisory discovery priority. Incomparable claims deliberately map to normal.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::zakura::header_sync) enum PeerWorkPriority {
    LowerComparableWork,
    Normal,
    HigherComparableWork,
}

impl PeerWorkPriority {
    pub(in crate::zakura::header_sync) fn from_work_order(order: Option<Ordering>) -> Self {
        match order {
            Some(Ordering::Greater) => Self::HigherComparableWork,
            Some(Ordering::Less) => Self::LowerComparableWork,
            Some(Ordering::Equal) | None => Self::Normal,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum QueueWorkResult {
    NeedsLocator,
    AlreadyActive,
    AtCapacity,
}

/// Bounded queue of exact, session-bound peer work.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::header_sync) struct PeerWorkQueue {
    work_by_peer: HashMap<ZakuraPeerId, PeerWorkState>,
    budget: HeaderChunkBudget,
    request_reservations: HashMap<ZakuraPeerId, HeaderCountReservation>,
    staged_capacity: HashMap<ZakuraPeerId, Vec<HeaderCapacityLease>>,
}

impl PeerWorkQueue {
    #[cfg(any(test, feature = "header-fuzz"))]
    pub(in crate::zakura::header_sync) fn len(&self) -> usize {
        self.work_by_peer.len()
    }

    /// Retire locator work captured from an obsolete committed snapshot.
    ///
    /// Active requests are retired separately so their network request can be canceled.
    pub(in crate::zakura::header_sync) fn retire_obsolete_unstarted(
        &mut self,
        current: &EngineSnapshot,
    ) -> usize {
        let obsolete: Vec<_> = self
            .work_by_peer
            .iter()
            .filter_map(|(peer, work)| match work {
                PeerWorkState::AwaitingLocator { target, .. } if !target.is_current(current) => {
                    Some(peer.clone())
                }
                _ => None,
            })
            .collect();
        for peer in &obsolete {
            self.remove_all(peer);
        }
        obsolete.len()
    }

    /// Retire exact body-authorized repairs whose generation is no longer current.
    ///
    /// Ordinary header targets remain provisionally owned until the serialized state planner
    /// proves a monotone rebase or rejects their retained ancestry.
    pub(in crate::zakura::header_sync) fn retire_obsolete_active(
        &mut self,
        current: &EngineSnapshot,
    ) -> Vec<ActiveHeaderRequest> {
        let peers: Vec<_> = self
            .work_by_peer
            .iter()
            .filter_map(|(peer, work)| match work {
                PeerWorkState::Active(request) => {
                    let header = request.owner.header_authority();
                    let obsolete = request.owner.body_authority().map_or_else(
                        || {
                            header.header_generation != current.header_generation
                                && header.branch.anchor_hash == current.frontiers.finalized.hash
                        },
                        |authority| {
                            authority.header.header_generation != current.header_generation
                                || authority.verified_generation != current.verified_generation
                                || authority.header.branch.anchor_hash
                                    != current.frontiers.finalized.hash
                        },
                    );
                    obsolete.then(|| peer.clone())
                }
                _ => None,
            })
            .collect();
        peers
            .into_iter()
            .filter_map(|peer| self.remove(&peer))
            .collect()
    }

    /// Return the exact attempt identity structurally owned by one peer slot.
    pub(in crate::zakura::header_sync) fn registered_attempt(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<(SourceId, HeaderSyncWorkOwner)> {
        self.active(peer)
            .map(|request| (request.source, request.owner))
    }

    pub(in crate::zakura::header_sync) fn stage(
        &mut self,
        peer: ZakuraPeerId,
        target: AdvertisedHeaderTarget,
        priority: PeerWorkPriority,
    ) -> QueueWorkResult {
        if let Some(work) = self.work_by_peer.get_mut(&peer) {
            return match work {
                PeerWorkState::AwaitingLocator {
                    target: current,
                    priority: current_priority,
                } => {
                    self.request_reservations.remove(&peer);
                    self.staged_capacity.remove(&peer);
                    **current = target;
                    *current_priority = priority;
                    QueueWorkResult::NeedsLocator
                }
                PeerWorkState::Active(_) => QueueWorkResult::AlreadyActive,
            };
        }
        if self.work_by_peer.len() >= MAX_STAGED_TARGETS_V1 {
            let replace = self
                .work_by_peer
                .iter()
                .filter_map(|(peer, work)| match work {
                    PeerWorkState::AwaitingLocator {
                        priority: current, ..
                    } if *current < priority => Some((peer.clone(), *current)),
                    _ => None,
                })
                .min_by(|(left_peer, left_priority), (right_peer, right_priority)| {
                    left_priority
                        .cmp(right_priority)
                        .then_with(|| left_peer.as_bytes().cmp(right_peer.as_bytes()))
                })
                .map(|(peer, _)| peer);
            let Some(replace) = replace else {
                return QueueWorkResult::AtCapacity;
            };
            self.remove_all(&replace);
        }
        self.work_by_peer.insert(
            peer,
            PeerWorkState::AwaitingLocator {
                target: Box::new(target),
                priority,
            },
        );
        QueueWorkResult::NeedsLocator
    }

    pub(in crate::zakura::header_sync) fn awaiting(
        &self,
        peer: &ZakuraPeerId,
        session_id: u64,
        target_tip_hash: zakura_chain::block::Hash,
        scope: HeaderWorkAuthority,
    ) -> Option<&AdvertisedHeaderTarget> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::AwaitingLocator { target, .. })
                if target.session_id == session_id
                    && target.status.selected_tip_hash == target_tip_hash
                    && target.scope == scope =>
            {
                Some(target.as_ref())
            }
            _ => None,
        }
    }

    pub(in crate::zakura::header_sync) fn start(&mut self, request: ActiveHeaderRequest) -> bool {
        let peer = request.peer.clone();
        let matches = self.awaiting(
            &peer,
            request.target.session_id,
            request.target.status.selected_tip_hash,
            request.target.scope,
        ) == Some(&request.target);
        if matches && self.request_reservations.contains_key(&peer) {
            self.work_by_peer
                .insert(peer, PeerWorkState::Active(Box::new(request)));
            true
        } else {
            false
        }
    }

    pub(in crate::zakura::header_sync) fn remove(
        &mut self,
        peer: &ZakuraPeerId,
    ) -> Option<ActiveHeaderRequest> {
        self.request_reservations.remove(peer);
        self.staged_capacity.remove(peer);
        match self.work_by_peer.remove(peer) {
            Some(PeerWorkState::Active(request)) => Some(*request),
            Some(PeerWorkState::AwaitingLocator { .. }) | None => None,
        }
    }

    pub(in crate::zakura::header_sync) fn remove_owner(
        &mut self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<ActiveHeaderRequest> {
        let peer = self
            .work_by_peer
            .iter()
            .find_map(|(peer, work)| match work {
                PeerWorkState::Active(request) if request.owner == owner => Some(peer.clone()),
                _ => None,
            })?;
        self.remove(&peer)
    }

    /// Return the active target carrying one exact durable owner.
    pub(in crate::zakura::header_sync) fn active_owner(
        &self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<&ActiveHeaderRequest> {
        self.work_by_peer.values().find_map(|work| match work {
            PeerWorkState::Active(request) if request.owner == owner => Some(request.as_ref()),
            _ => None,
        })
    }

    pub(in crate::zakura::header_sync) fn remove_unstarted(&mut self, peer: &ZakuraPeerId) {
        if matches!(
            self.work_by_peer.get(peer),
            Some(PeerWorkState::AwaitingLocator { .. })
        ) {
            self.remove_all(peer);
        }
    }

    pub(in crate::zakura::header_sync) fn remove_awaiting(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        target_tip_hash: zakura_chain::block::Hash,
        scope: HeaderWorkAuthority,
    ) {
        if self
            .awaiting(peer, session_id, target_tip_hash, scope)
            .is_some()
        {
            self.remove_all(peer);
        }
    }

    pub(in crate::zakura::header_sync) fn active(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<&ActiveHeaderRequest> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::Active(request)) => Some(request),
            _ => None,
        }
    }

    pub(in crate::zakura::header_sync) fn active_mut(
        &mut self,
        peer: &ZakuraPeerId,
    ) -> Option<&mut ActiveHeaderRequest> {
        match self.work_by_peer.get_mut(peer) {
            Some(PeerWorkState::Active(request)) => Some(request),
            _ => None,
        }
    }

    fn remove_all(&mut self, peer: &ZakuraPeerId) {
        self.request_reservations.remove(peer);
        self.staged_capacity.remove(peer);
        self.work_by_peer.remove(peer);
    }

    /// Bound one request by its fair share and currently unowned capacity.
    pub(in crate::zakura::header_sync) fn reservable_header_count(&self, desired: u32) -> u32 {
        let desired = usize::try_from(desired).unwrap_or(usize::MAX);
        u32::try_from(
            desired
                .min(MAX_HEADER_CHUNK_RESERVATION_V1)
                .min(self.budget.remaining()),
        )
        .expect("the header budget capacity fits u32")
    }

    /// Reserve capacity before publishing one wire request.
    pub(in crate::zakura::header_sync) fn reserve_request(
        &mut self,
        peer: &ZakuraPeerId,
        count: u32,
    ) -> bool {
        if self.request_reservations.contains_key(peer) {
            return false;
        }
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        if count > MAX_HEADER_CHUNK_RESERVATION_V1 {
            return false;
        }
        let Some(reservation) = self.budget.reserve(count) else {
            return false;
        };
        self.request_reservations.insert(peer.clone(), reservation);
        true
    }

    /// Release a request reservation after the wire send fails.
    pub(in crate::zakura::header_sync) fn cancel_request_reservation(
        &mut self,
        peer: &ZakuraPeerId,
    ) {
        self.request_reservations.remove(peer);
    }

    /// Transfer one response's consumed count into owned staged capacity.
    pub(in crate::zakura::header_sync) fn consume_response_capacity(
        &mut self,
        peer: &ZakuraPeerId,
        returned: usize,
    ) -> bool {
        let Some(reservation) = self.request_reservations.remove(peer) else {
            return false;
        };
        let Ok(lease) = reservation.consume(returned) else {
            return false;
        };
        if let Some(lease) = lease {
            self.staged_capacity
                .entry(peer.clone())
                .or_default()
                .push(lease);
        }
        self.publish_phase_metrics();
        true
    }

    pub(in crate::zakura::header_sync) fn owned_header_count(&self, peer: &ZakuraPeerId) -> usize {
        self.staged_capacity
            .get(peer)
            .into_iter()
            .flatten()
            .map(|lease| lease.0.count)
            .fold(0usize, usize::saturating_add)
    }

    pub(in crate::zakura::header_sync) fn reserved_header_count(
        &self,
        peer: &ZakuraPeerId,
    ) -> usize {
        self.request_reservations
            .get(peer)
            .map_or(0, HeaderCountReservation::remaining)
    }

    pub(in crate::zakura::header_sync) fn budget_is_full(&self) -> bool {
        self.budget.remaining() == 0
    }

    /// Return all response reservations and staged headers that can consume durable headroom.
    pub(in crate::zakura::header_sync) fn claimed_header_count(&self) -> usize {
        self.budget.claimed()
    }

    /// Return capacity that no live reservation or staged chunk owns.
    #[cfg(test)]
    pub(in crate::zakura::header_sync) fn unowned_chunk_capacity(&self) -> usize {
        self.budget.remaining()
    }

    #[cfg(any(test, feature = "header-fuzz"))]
    pub(in crate::zakura::header_sync) fn chunk_budget_usage(&self) -> (usize, usize) {
        let usage = self.budget.usage();
        (usage.reserved, usage.owned)
    }

    pub(in crate::zakura::header_sync) fn publish_phase_metrics(&self) {
        let mut receiving = 0usize;
        let mut preparing = 0usize;
        let mut applying = 0usize;
        for (peer, work) in &self.work_by_peer {
            let PeerWorkState::Active(request) = work else {
                continue;
            };
            let count = self.owned_header_count(peer);
            match request.phase {
                HeaderTargetPhase::Receiving => receiving = receiving.saturating_add(count),
                HeaderTargetPhase::Preparing => preparing = preparing.saturating_add(count),
                HeaderTargetPhase::Applying => applying = applying.saturating_add(count),
            }
        }
        // Counts are bounded by 4,000 and are exactly representable as f64.
        metrics::gauge!("sync.header.chunk_budget.receiving").set(receiving as f64);
        metrics::gauge!("sync.header.chunk_budget.preparing").set(preparing as f64);
        metrics::gauge!("sync.header.chunk_budget.applying").set(applying as f64);
    }

    #[cfg(test)]
    pub(in crate::zakura::header_sync) fn set_capacity_for_test(
        &mut self,
        peer: &ZakuraPeerId,
        staged: usize,
        reserved: usize,
    ) {
        self.request_reservations.remove(peer);
        self.staged_capacity.remove(peer);
        if staged != 0 {
            let reservation = self
                .budget
                .reserve(staged)
                .expect("the test staged count fits the aggregate budget");
            let lease = reservation
                .consume(staged)
                .expect("the test consumes its exact staged reservation")
                .expect("a nonzero staged reservation returns an owned lease");
            self.staged_capacity.insert(peer.clone(), vec![lease]);
        }
        if reserved != 0 {
            let reservation = self
                .budget
                .reserve(reserved)
                .expect("the test request count fits remaining capacity");
            self.request_reservations.insert(peer.clone(), reservation);
        }
    }
}
