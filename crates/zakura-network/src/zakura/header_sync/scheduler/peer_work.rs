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
// The conversion is safe because `MAX_HS_RANGE` is 4,000 on every supported target.
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
        // The conversion is exact because the count is at most 4,000.
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
    /// Durable generation and branch that own the scheduled locator work.
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
    /// Exact status snapshot that the request pursues.
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
    /// The reactor receives and stages response pages.
    Receiving,
    /// A worker validates the complete target outside the reactor.
    Preparing,
    /// Sealed evidence passed the gate.
    /// One state call remains pending.
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

/// Advisory discovery priority.
/// The scheduler maps incomparable claims to normal priority.
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
    TargetAlreadyAssigned,
    AtCapacity,
}

/// Bounded queue of exact, session-bound peer work.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::header_sync) struct PeerWorkQueue {
    work_by_peer: HashMap<ZakuraPeerId, PeerWorkState>,
    repair_episodes: HashMap<HeaderSyncWorkOwner, zakura_header_chain::AuxiliaryRequirementEpisode>,
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
    /// Retire active requests separately so the reactor can cancel their network requests.
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

    /// Stage one ordinary branch target only when no other peer already owns it.
    ///
    /// All peers derive locators from shared local state.
    /// Two peers pursuing one target would download and authenticate overlapping prefixes.
    /// The reactor retains the alternate peer's status until the owner retires.
    pub(in crate::zakura::header_sync) fn stage_distinct_target(
        &mut self,
        peer: ZakuraPeerId,
        target: AdvertisedHeaderTarget,
        priority: PeerWorkPriority,
    ) -> QueueWorkResult {
        let owned_by_other_peer = self.work_by_peer.iter().any(|(owner, work)| {
            let owned_target = match work {
                PeerWorkState::AwaitingLocator { target, .. } => target.status.selected_tip_hash,
                PeerWorkState::Active(request) => request.target.status.selected_tip_hash,
            };
            owner != &peer && owned_target == target.status.selected_tip_hash
        });
        if owned_by_other_peer {
            self.remove_unstarted(&peer);
            return QueueWorkResult::TargetAlreadyAssigned;
        }

        self.stage(peer, target, priority)
    }

    /// Return the peer's current unstarted target, if any.
    pub(in crate::zakura::header_sync) fn awaiting_target(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<&AdvertisedHeaderTarget> {
        match self.work_by_peer.get(peer) {
            Some(PeerWorkState::AwaitingLocator { target, .. }) => Some(target.as_ref()),
            _ => None,
        }
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

    /// Start one repair request and bind its durable evidence episode to its private queue state.
    pub(in crate::zakura::header_sync) fn start_repair(
        &mut self,
        request: ActiveHeaderRequest,
        episode: zakura_header_chain::AuxiliaryRequirementEpisode,
    ) -> bool {
        if !matches!(
            request.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        ) {
            return false;
        }
        let owner = request.owner;
        if !self.start(request) {
            return false;
        }
        assert!(
            self.repair_episodes.insert(owner, episode).is_none(),
            "an active repair owner binds exactly one evidence episode"
        );
        true
    }

    /// Return the durable evidence episode bound to one active repair request.
    pub(in crate::zakura::header_sync) fn repair_episode(
        &self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<zakura_header_chain::AuxiliaryRequirementEpisode> {
        self.repair_episodes.get(&owner).copied()
    }

    #[cfg(test)]
    pub(in crate::zakura::header_sync) fn bind_repair_episode_for_test(
        &mut self,
        owner: HeaderSyncWorkOwner,
        episode: zakura_header_chain::AuxiliaryRequirementEpisode,
    ) {
        assert!(self.active_owner(owner).is_some());
        self.repair_episodes.insert(owner, episode);
    }

    pub(in crate::zakura::header_sync) fn remove(
        &mut self,
        peer: &ZakuraPeerId,
    ) -> Option<ActiveHeaderRequest> {
        self.request_reservations.remove(peer);
        self.staged_capacity.remove(peer);
        match self.work_by_peer.remove(peer) {
            Some(PeerWorkState::Active(request)) => {
                self.repair_episodes.remove(&request.owner);
                Some(*request)
            }
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
        if let Some(PeerWorkState::Active(request)) = self.work_by_peer.remove(peer) {
            self.repair_episodes.remove(&request.owner);
        }
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
        // The conversion is exact because the count is at most 4,000.
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use zakura_chain::{block, block::genesis::regtest_genesis_block, work::difficulty::U256};
    use zakura_header_chain::{
        AlarmSet, ChainScore, EngineMode, Frontier, FrontierSet, HeaderGeneration, StateVersion,
        SuffixWork, VerifiedGeneration,
    };

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn snapshot() -> EngineSnapshot {
        let finalized = Frontier::new(block::Height(10), hash(10));
        let tip = Frontier::new(block::Height(100), hash(100));
        EngineSnapshot {
            mode: EngineMode::Integrated,
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(1),
            verified_generation: VerifiedGeneration::new(1),
            frontiers: FrontierSet {
                finalized,
                header_best: tip,
                verified_best: finalized,
            },
            header_best_score: ChainScore::new(SuffixWork::new(U256::from(100_u32)), tip.hash),
            oldest_retained_height: finalized.height,
            alarms: AlarmSet::default(),
        }
    }

    fn advertisement(marker: u8) -> AdvertisedHeaderTarget {
        let local = snapshot();
        AdvertisedHeaderTarget {
            scope: HeaderWorkAuthority::for_target(&local, hash(marker)),
            session_id: 7,
            status: Status {
                work_anchor_height: block::Height(10),
                work_anchor_hash: hash(10),
                selected_tip_height: block::Height(u32::from(marker)),
                selected_tip_hash: hash(marker),
                suffix_cumulative_work: U256::from(u32::from(marker)),
                oldest_retained_height: block::Height(10),
                max_headers_per_response: 1_000,
                max_inflight_requests: 1,
                max_message_bytes: 2_000_000,
                tree_aux_schema_mask: 1,
            },
        }
    }

    fn peer(marker: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![marker; 32]).expect("the test peer ID has the required length")
    }

    fn active_request(
        marker: u8,
        target: AdvertisedHeaderTarget,
        local: &EngineSnapshot,
        entries: Vec<HeaderEntry>,
    ) -> ActiveHeaderRequest {
        let request_id =
            HeaderSyncRequestId::new(u64::from(marker)).expect("the marker is nonzero");
        ActiveHeaderRequest {
            purpose: HeaderTargetPurpose::Normal,
            peer: peer(marker),
            source: SourceId::from_digest([marker; 32]),
            sent_locator: HeaderLocator::for_continuation(local.frontiers.finalized),
            owner: zakura_header_chain::HeaderWorkOwner {
                authority: HeaderWorkAuthority::for_target(local, target.status.selected_tip_hash),
                session_id: target.session_id,
                request_id: std::num::NonZeroU64::new(request_id.get())
                    .expect("header-sync request IDs are nonzero"),
            }
            .into(),
            target,
            request_id,
            common_ancestor: Some(local.frontiers.finalized),
            entries,
            phase: HeaderTargetPhase::Receiving,
            max_header_count: 1_000,
            tree_aux_schema: AuxSchema::None,
        }
    }

    #[test]
    fn unknown_status_targets_remain_eligible_regardless_of_advisory_shape() {
        let local = snapshot();

        let mut same_height_fork = advertisement(1);
        same_height_fork.status.selected_tip_height = local.frontiers.header_best.height;
        same_height_fork.status.suffix_cumulative_work = U256::from(1_u32);
        assert!(same_height_fork.is_discovery_eligible(&local));
        assert_eq!(
            same_height_fork.claimed_work_order(&local),
            Some(Ordering::Less)
        );

        let mut shorter_higher_work = advertisement(2);
        shorter_higher_work.status.selected_tip_height = block::Height(90);
        shorter_higher_work.status.suffix_cumulative_work = U256::from(101_u32);
        assert!(shorter_higher_work.is_discovery_eligible(&local));
        assert_eq!(
            shorter_higher_work.claimed_work_order(&local),
            Some(Ordering::Greater)
        );

        let mut incomparable = advertisement(3);
        incomparable.status.work_anchor_hash = hash(11);
        incomparable.status.suffix_cumulative_work = U256::MAX;
        assert!(incomparable.is_discovery_eligible(&local));
        assert_eq!(incomparable.claimed_work_order(&local), None);

        let mut known = advertisement(4);
        known.status.selected_tip_hash = local.frontiers.header_best.hash;
        assert!(!known.is_discovery_eligible(&local));

        let mut pure_requester = advertisement(5);
        pure_requester.status.max_headers_per_response = 0;
        assert!(!pure_requester.is_discovery_eligible(&local));
    }

    #[test]
    fn staged_tip_rejects_heights_above_the_protocol_maximum() {
        let local = snapshot();
        let target = advertisement(1);
        let mut request = active_request(
            1,
            target,
            &local,
            vec![HeaderEntry {
                header: regtest_genesis_block().header.clone(),
                body_size: 0,
                tree_aux: None,
            }],
        );
        request.common_ancestor = Some(Frontier::new(block::Height::MAX, hash(10)));

        assert_eq!(request.staged_tip(), None);
    }

    #[test]
    fn peer_work_queue_caps_targets_and_only_supersedes_unstarted_work() {
        let mut queue = PeerWorkQueue::default();
        for marker in 1..=16 {
            assert_eq!(
                queue.stage(
                    peer(marker),
                    advertisement(marker),
                    PeerWorkPriority::Normal
                ),
                QueueWorkResult::NeedsLocator
            );
        }
        assert_eq!(
            queue.stage(peer(17), advertisement(17), PeerWorkPriority::Normal),
            QueueWorkResult::AtCapacity
        );

        let replacement = advertisement(42);
        assert_eq!(
            queue.stage(peer(1), replacement.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert_eq!(
            queue.awaiting(&peer(1), 7, hash(42), replacement.scope),
            Some(&replacement)
        );

        let local = snapshot();
        let locator = HeaderLocator::for_selected_path(&local, |height| {
            let marker = u8::try_from(height.0).expect("the test heights fit in one byte");
            Ok(Some(hash(marker)))
        })
        .expect("the test projection contains every requested frontier");
        let request = ActiveHeaderRequest {
            purpose: HeaderTargetPurpose::Normal,
            peer: peer(1),
            source: SourceId::from_digest([1; 32]),
            target: replacement.clone(),
            sent_locator: locator.clone(),
            request_id: HeaderSyncRequestId::new(1).expect("one is a nonzero request ID"),
            owner: zakura_header_chain::HeaderWorkOwner {
                authority: HeaderWorkAuthority::for_target(
                    &local,
                    replacement.status.selected_tip_hash,
                ),
                session_id: 7,
                request_id: std::num::NonZeroU64::new(1).expect("one is nonzero"),
            }
            .into(),
            common_ancestor: None,
            entries: Vec::new(),
            phase: HeaderTargetPhase::Receiving,
            max_header_count: 1_000,
            tree_aux_schema: AuxSchema::None,
        };
        assert!(queue.reserve_request(&peer(1), 1));
        assert!(queue.start(request.clone()));
        assert_eq!(queue.active(&peer(1)), Some(&request));
        assert_eq!(
            queue.stage(
                peer(1),
                advertisement(43),
                PeerWorkPriority::HigherComparableWork
            ),
            QueueWorkResult::AlreadyActive
        );
        assert_eq!(queue.active(&peer(1)), Some(&request));
        assert_eq!(queue.active(&peer(1)).unwrap().sent_locator, locator);
        let continuation_tip = Frontier::new(block::Height(101), hash(101));
        assert_eq!(
            request.continuation_locator(continuation_tip).entries(),
            &[continuation_tip]
        );
        assert_eq!(request.target.status.selected_tip_hash, hash(42));
        assert_eq!(
            PeerWorkPriority::from_work_order(None),
            PeerWorkPriority::Normal
        );
        assert_eq!(
            queue.stage(
                peer(17),
                advertisement(17),
                PeerWorkPriority::HigherComparableWork,
            ),
            QueueWorkResult::NeedsLocator
        );
        let expected = advertisement(17);
        assert!(queue
            .awaiting(&peer(17), 7, hash(17), expected.scope)
            .is_some());
    }

    #[test]
    fn ordinary_target_has_one_peer_owner_with_failover_after_retirement() {
        let local = snapshot();
        let mut queue = PeerWorkQueue::default();
        let target = advertisement(1);

        assert_eq!(
            queue.stage_distinct_target(peer(1), target.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert_eq!(
            queue.stage_distinct_target(
                peer(2),
                target.clone(),
                PeerWorkPriority::HigherComparableWork
            ),
            QueueWorkResult::TargetAlreadyAssigned
        );
        assert_eq!(queue.len(), 1);

        assert!(queue.reserve_request(&peer(1), 1));
        assert!(queue.start(active_request(1, target.clone(), &local, Vec::new())));
        assert_eq!(
            queue.stage_distinct_target(peer(2), target.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::TargetAlreadyAssigned
        );

        queue.remove(&peer(1));
        assert_eq!(
            queue.stage_distinct_target(peer(2), target, PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator,
            "the alternate peer becomes eligible immediately after owner retirement"
        );
    }

    #[test]
    fn body_finality_reanchor_does_not_duplicate_the_same_wire_target() {
        let local = snapshot();
        let mut queue = PeerWorkQueue::default();
        let target = advertisement(1);
        let mut reanchored = target.clone();
        reanchored.scope.header_generation = reanchored
            .scope
            .header_generation
            .checked_next()
            .expect("the fixture header generation advances");
        reanchored.scope.branch.anchor_hash = hash(99);

        assert_ne!(target.scope, reanchored.scope);
        assert_eq!(
            queue.stage_distinct_target(peer(1), target.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert!(queue.reserve_request(&peer(1), 1));
        assert!(queue.start(active_request(1, target, &local, Vec::new())));

        assert_eq!(
            queue.stage_distinct_target(peer(2), reanchored, PeerWorkPriority::Normal),
            QueueWorkResult::TargetAlreadyAssigned,
            "body-only finality movement cannot create duplicate work for one target hash"
        );
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn duplicate_target_retires_different_unstarted_work_for_that_peer() {
        let mut queue = PeerWorkQueue::default();
        let owned = advertisement(1);
        let superseded = advertisement(2);

        assert_eq!(
            queue.stage_distinct_target(peer(1), owned.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert_eq!(
            queue.stage_distinct_target(peer(2), superseded.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert_eq!(
            queue.stage_distinct_target(peer(2), owned, PeerWorkPriority::Normal),
            QueueWorkResult::TargetAlreadyAssigned
        );
        assert!(
            queue
                .awaiting(
                    &peer(2),
                    superseded.session_id,
                    superseded.status.selected_tip_hash,
                    superseded.scope,
                )
                .is_none(),
            "a stale locator result cannot resurrect superseded work"
        );
    }

    #[test]
    fn generation_change_retires_unstarted_targets_before_new_scheduling() {
        let local = snapshot();
        let mut queue = PeerWorkQueue::default();
        let awaiting = advertisement(1);
        assert_eq!(
            queue.stage(peer(1), awaiting, PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        let active = advertisement(2);
        assert_eq!(
            queue.stage(peer(2), active.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert!(queue.reserve_request(&peer(2), 1));
        assert!(queue.start(active_request(2, active, &local, Vec::new())));

        let mut current = local;
        current.state_version = current
            .state_version
            .checked_next()
            .expect("the fixture state version advances");
        current.header_generation = current
            .header_generation
            .checked_next()
            .expect("the fixture generation advances");
        assert_eq!(queue.retire_obsolete_unstarted(&current), 1);
        assert_eq!(queue.len(), 1);
        assert!(
            queue.active(&peer(2)).is_some(),
            "active requests remain owned by the pending-owner retirement path"
        );
    }

    #[test]
    fn body_only_state_version_change_keeps_header_locator_work_current() {
        let local = snapshot();
        let mut queue = PeerWorkQueue::default();
        let awaiting = advertisement(1);
        let original_scope = awaiting.scope;
        let target_hash = awaiting.status.selected_tip_hash;
        assert_eq!(
            queue.stage(peer(1), awaiting, PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );

        let mut current = local;
        current.state_version = current
            .state_version
            .checked_next()
            .expect("the fixture state version advances");

        assert_eq!(queue.retire_obsolete_unstarted(&current), 0);
        assert!(
            queue
                .awaiting(&peer(1), 7, target_hash, original_scope)
                .is_some(),
            "an unrelated body commit preserves header-generation locator authority"
        );
    }

    #[test]
    fn aggregate_owned_header_budget_spans_all_peers_and_releases_on_retirement() {
        let local = snapshot();
        let entry = HeaderEntry {
            header: Arc::new(*regtest_genesis_block().header),
            body_size: 0,
            tree_aux: None,
        };
        let mut queue = PeerWorkQueue::default();
        let first_count = HEADER_CHUNK_BUDGET_CAPACITY_V1 * 3 / 4;
        let remaining = HEADER_CHUNK_BUDGET_CAPACITY_V1 - first_count;

        let first = advertisement(1);
        assert_eq!(
            queue.stage(peer(1), first.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert!(queue.reserve_request(&peer(1), 1));
        assert!(queue.start(active_request(
            1,
            first,
            &local,
            vec![entry.clone(); first_count],
        )));
        queue.set_capacity_for_test(&peer(1), first_count, 0);
        assert_eq!(queue.unowned_chunk_capacity(), remaining);

        let second = advertisement(2);
        assert_eq!(
            queue.stage(peer(2), second.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert!(queue.reserve_request(&peer(2), 1));
        assert!(queue.start(active_request(2, second, &local, vec![entry; remaining],)));
        queue.set_capacity_for_test(&peer(2), remaining, 0);
        assert_eq!(queue.unowned_chunk_capacity(), 0);

        queue.remove(&peer(1));
        assert_eq!(queue.unowned_chunk_capacity(), first_count);
    }

    #[test]
    fn header_chunk_leases_release_unused_and_owned_capacity_exactly_once() {
        let budget = HeaderChunkBudget::default();
        let reservation = budget
            .reserve(MAX_HEADER_CHUNK_RESERVATION_V1)
            .expect("the fair share fits an empty budget");
        assert_eq!(
            budget.usage(),
            HeaderChunkUsage {
                reserved: MAX_HEADER_CHUNK_RESERVATION_V1,
                owned: 0,
            }
        );

        let returned = MAX_HEADER_CHUNK_RESERVATION_V1 / 2;
        let lease = reservation
            .consume(returned)
            .expect("a partial response fits its reservation")
            .expect("a nonempty response returns an owned lease");
        assert_eq!(
            budget.usage(),
            HeaderChunkUsage {
                reserved: 0,
                owned: returned,
            },
            "unused response capacity is returned immediately"
        );
        drop(lease);
        assert_eq!(budget.usage(), HeaderChunkUsage::default());

        let empty = budget
            .reserve(MAX_HEADER_CHUNK_RESERVATION_V1)
            .expect("released capacity can be reserved again");
        assert!(empty
            .consume(0)
            .expect("an empty response consumes its reservation")
            .is_none());
        assert_eq!(budget.usage(), HeaderChunkUsage::default());
    }

    #[test]
    fn malformed_overreturn_preserves_reservation_until_terminal_cleanup() {
        let budget = HeaderChunkBudget::default();
        let reservation = budget.reserve(1).expect("one header fits the budget");
        assert!(reservation.consume(2).is_err());
        assert_eq!(
            budget.usage(),
            HeaderChunkUsage {
                reserved: 1,
                owned: 0,
            },
            "a rejected conversion remains owned by the request"
        );
        drop(reservation);
        assert_eq!(budget.usage(), HeaderChunkUsage::default());
    }

    #[test]
    fn fair_reservations_fill_the_aggregate_budget_without_overcommit() {
        let mut queue = PeerWorkQueue::default();
        for marker in 1..=MAX_STAGED_TARGETS_V1 {
            let marker = u8::try_from(marker).expect("the target-slot count fits u8");
            assert_eq!(
                queue.reservable_header_count(MAX_HS_RANGE),
                u32::try_from(MAX_HEADER_CHUNK_RESERVATION_V1)
                    .expect("the fair reservation fits u32")
            );
            assert!(queue.reserve_request(
                &peer(marker),
                u32::try_from(MAX_HEADER_CHUNK_RESERVATION_V1)
                    .expect("the fair reservation fits u32"),
            ));
        }
        assert_eq!(
            queue.chunk_budget_usage(),
            (HEADER_CHUNK_BUDGET_CAPACITY_V1, 0)
        );
        assert_eq!(queue.reservable_header_count(MAX_HS_RANGE), 0);
        assert!(!queue.reserve_request(&peer(17), 1));

        queue.cancel_request_reservation(&peer(1));
        assert_eq!(
            queue.unowned_chunk_capacity(),
            MAX_HEADER_CHUNK_RESERVATION_V1
        );
    }

    #[test]
    fn active_work_requires_a_preallocated_response_reservation() {
        let local = snapshot();
        let target = advertisement(1);
        let mut queue = PeerWorkQueue::default();
        assert_eq!(
            queue.stage(peer(1), target.clone(), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        let request = active_request(1, target, &local, Vec::new());
        assert!(!queue.start(request.clone()));
        assert!(queue.active(&peer(1)).is_none());
        assert!(queue.reserve_request(&peer(1), 1));
        assert!(queue.start(request));
    }

    #[test]
    fn repair_episode_follows_private_active_queue_ownership() {
        let local = snapshot();
        let target = advertisement(1);
        let selected_target = Frontier::new(
            target.status.selected_tip_height,
            target.status.selected_tip_hash,
        );
        let context = zakura_header_chain::VctRepairContext::unconstrained(
            selected_target,
            zakura_header_chain::HeaderLocator::for_continuation(local.frontiers.finalized),
            None,
        );
        let mut request = active_request(1, target.clone(), &local, Vec::new());
        request.purpose = HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target,
            repair_generation: 7,
        };
        let owner = request.owner;
        let mut queue = PeerWorkQueue::default();

        assert_eq!(
            queue.stage(peer(1), target, PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        assert!(queue.reserve_request(&peer(1), 1));
        assert!(queue.start_repair(request, context.episode));
        assert_eq!(queue.repair_episode(owner), Some(context.episode));

        assert!(queue.remove(&peer(1)).is_some());
        assert_eq!(queue.repair_episode(owner), None);
    }

    #[test]
    fn selected_auxiliary_repair_is_an_exact_one_header_target_purpose() {
        let selected_target = Frontier::new(zakura_chain::block::Height(11), hash(11));
        let purpose = HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target,
            repair_generation: 7,
        };

        assert_eq!(purpose.exact_header_count(), Some(1));
        assert_eq!(purpose.selected_repair_target(), Some(selected_target));
        assert_eq!(HeaderTargetPurpose::Normal.exact_header_count(), None);
    }
}
