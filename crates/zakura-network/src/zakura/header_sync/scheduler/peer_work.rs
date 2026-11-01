//! The runtime owner of header work: budget handles and their effectful drops.
//!
//! [`PeerWorkQueue`] asks [`policy::WorkPolicy`] for each decision and applies
//! its capacity effect. Dropping a [`HeaderCountReservation`] or the last clone
//! of a [`HeaderCapacityLease`] returns its count to the budget and wakes waiters.

use std::sync::Arc;

use zakura_header_chain::{EngineSnapshot, HeaderSyncWorkOwner, HeaderWorkAuthority, SourceId};
use zakura_rt::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        watch, Mutex, PoisonError,
    },
};

mod policy;

pub use policy::{
    ActiveHeaderRequest, AdvertisedHeaderTarget, HeaderTargetPhase, HeaderTargetPurpose,
    HeaderWorkEnd, StorageCallKind, WorkIncarnation,
};
use policy::{HeaderChunkUsage, StageDecision, WorkPolicy};
pub(in crate::zakura::header_sync) use policy::{
    PeerWorkPriority, QueueWorkResult, StorageCall, WorkCall, HEADER_CHUNK_BUDGET_CAPACITY_V1,
    MAX_HEADER_CHUNK_RESERVATION_V1,
};

#[cfg(any(test, zakura_sim))]
use super::super::observe::{ObserverSlot, ReactorEvent};
use super::super::ZakuraPeerId;

#[derive(Debug)]
struct HeaderChunkBudgetInner {
    capacity: usize,
    usage: Mutex<HeaderChunkUsage>,
    released: watch::Sender<u64>,
}

#[derive(Clone, Debug)]
struct HeaderChunkBudget(Arc<HeaderChunkBudgetInner>);

impl Default for HeaderChunkBudget {
    fn default() -> Self {
        Self(Arc::new(HeaderChunkBudgetInner {
            capacity: HEADER_CHUNK_BUDGET_CAPACITY_V1,
            usage: Mutex::new(HeaderChunkUsage::default()),
            released: watch::channel(0).0,
        }))
    }
}

impl HeaderChunkBudget {
    fn usage(&self) -> HeaderChunkUsage {
        *self.0.usage.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn remaining(&self) -> usize {
        self.usage().remaining(self.0.capacity)
    }

    fn claimed(&self) -> usize {
        self.usage().claimed()
    }

    fn reserve(&self, count: usize) -> Option<HeaderCountReservation> {
        let mut usage = self.0.usage.lock().unwrap_or_else(PoisonError::into_inner);
        *usage = usage.reserve(self.0.capacity, count)?;
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
        let mut usage = self.0.usage.lock().unwrap_or_else(PoisonError::into_inner);
        *usage = usage.consume(reserved, owned).ok_or(())?;
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
        let mut usage = self.0.usage.lock().unwrap_or_else(PoisonError::into_inner);
        *usage = usage.release_reserved(count);
        self.publish(*usage);
        if count != 0 {
            self.0
                .released
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
    }

    fn release_owned(&self, count: usize) {
        let mut usage = self.0.usage.lock().unwrap_or_else(PoisonError::into_inner);
        *usage = usage.release_owned(count);
        self.publish(*usage);
        if count != 0 {
            self.0
                .released
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
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

    /// Move `owned` headers of this reservation into owned capacity. The rest
    /// stays reserved for the pages that its credit still authorizes.
    fn consume(&self, owned: usize) -> Result<Option<HeaderCapacityLease>, ()> {
        let rest = policy::reservation_after_page(self.remaining(), owned).ok_or(())?;
        let lease = self.0.budget.consume(owned, owned)?;
        self.0.remaining.store(rest, AtomicOrdering::SeqCst);
        Ok(lease)
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
pub(in crate::zakura::header_sync) struct HeaderCapacityLease(Arc<HeaderCapacityLeaseInner>);

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

/// The header capacity one storage call holds for one round of work.
///
/// Its leases keep the headers charged until the call's result is handled, even
/// after the queue retires the round. A prepare's retained headers move to its
/// apply without a release, so no other work can claim their capacity between
/// the two. Dropping this value ends the call's ownership once.
#[derive(Debug)]
pub(in crate::zakura::header_sync) struct RetainedHeaders {
    peer: ZakuraPeerId,
    incarnation: WorkIncarnation,
    /// The call that holds the headers, once one does.
    call: Option<StorageCall>,
    leases: Vec<HeaderCapacityLease>,
    end: HeaderWorkEnd,
    #[cfg(any(test, zakura_sim))]
    observer: ObserverSlot,
}

impl RetainedHeaders {
    /// The peer whose work these headers belong to.
    pub(in crate::zakura::header_sync) fn peer(&self) -> &ZakuraPeerId {
        &self.peer
    }

    /// The round and call holding these headers, once a call does.
    pub(in crate::zakura::header_sync) fn work(&self) -> Option<WorkCall> {
        self.call.map(|call| WorkCall {
            incarnation: self.incarnation,
            call,
        })
    }

    /// The number of headers retained.
    pub(in crate::zakura::header_sync) fn headers(&self) -> usize {
        self.leases
            .iter()
            .map(|lease| lease.0.count)
            .fold(0usize, usize::saturating_add)
    }

    /// Report this ownership's events to `observer`.
    #[cfg(any(test, zakura_sim))]
    pub(in crate::zakura::header_sync) fn observed_by(&mut self, observer: &ObserverSlot) {
        self.observer = observer.clone();
    }

    /// Record how this ownership will end when it drops.
    pub(in crate::zakura::header_sync) fn end_as(&mut self, end: HeaderWorkEnd) {
        self.end = end;
    }

    /// Move these headers to `call`. A call that already holds them is a prepare,
    /// and `call` is its apply.
    pub(in crate::zakura::header_sync) fn enter_call(&mut self, call: StorageCall) {
        if let Some(previous) = self.call {
            debug_assert!(
                previous.kind == StorageCallKind::Prepare && call.kind == StorageCallKind::Apply,
                "retained headers move only from a prepare to its apply"
            );
            self.end = HeaderWorkEnd::HandedToApply { apply: call.number };
            self.record_end();
        }
        self.call = Some(call);
        self.end = HeaderWorkEnd::Dropped;
        #[cfg(any(test, zakura_sim))]
        if let Some(observer) = &self.observer.0 {
            observer.observe(ReactorEvent::HeadersHeld {
                call: call.number,
                kind: call.kind,
                peer: self.peer.clone(),
                incarnation: self.incarnation.get(),
                headers: self.headers(),
            });
        }
    }

    /// Count this ownership's end and report it before any lease can release.
    fn record_end(&self) {
        let Some(call) = self.call else {
            return;
        };
        metrics::counter!("sync.header.work.retained_end", "end" => self.end.label()).increment(1);
        metrics::counter!("sync.header.work.retained_end_headers", "end" => self.end.label())
            .increment(u64::try_from(self.headers()).unwrap_or(u64::MAX));
        #[cfg(any(test, zakura_sim))]
        if let Some(observer) = &self.observer.0 {
            // An unwinding reactor reports nothing, as `HeaderSyncObserver` says.
            if !std::thread::panicking() {
                observer.observe(ReactorEvent::HeadersEnded {
                    call: call.number,
                    incarnation: self.incarnation.get(),
                    headers: self.headers(),
                    end: self.end,
                });
            }
        }
        #[cfg(not(any(test, zakura_sim)))]
        let _ = call;
    }
}

impl Drop for RetainedHeaders {
    fn drop(&mut self) {
        self.record_end();
    }
}

/// Bounded queue of exact, session-bound peer work.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::header_sync) struct PeerWorkQueue {
    policy: WorkPolicy,
    budget: HeaderChunkBudget,
    request_reservations: HashMap<ZakuraPeerId, HeaderCountReservation>,
    staged_capacity: HashMap<ZakuraPeerId, Vec<HeaderCapacityLease>>,
}

impl PeerWorkQueue {
    /// Changes to target ownership or awaiting/active state invalidate cached offers.
    /// The reactor separately tracks effective virtual claims using negotiated limits.
    pub(in crate::zakura::header_sync) fn scheduling_revision(&self) -> u64 {
        self.policy.scheduling_revision()
    }

    pub(in crate::zakura::header_sync) fn capacity_releases(&self) -> watch::Receiver<u64> {
        self.budget.0.released.subscribe()
    }

    pub(in crate::zakura::header_sync) fn len(&self) -> usize {
        self.policy.len()
    }

    /// Whether `peer` holds unstarted or active work.
    pub(in crate::zakura::header_sync) fn has_work(&self, peer: &ZakuraPeerId) -> bool {
        self.policy.has_work(peer)
    }

    /// Whether any peer's unstarted or active work pursues `target_tip_hash`.
    pub(in crate::zakura::header_sync) fn is_target_assigned(
        &self,
        target_tip_hash: zakura_chain::block::Hash,
    ) -> bool {
        self.policy.is_target_assigned(target_tip_hash)
    }

    /// Return every peer's target that still waits for its locator.
    pub(in crate::zakura::header_sync) fn awaiting_targets(
        &self,
    ) -> impl Iterator<Item = (&ZakuraPeerId, &AdvertisedHeaderTarget)> {
        self.policy.awaiting_targets()
    }

    /// Retire locator work captured from an obsolete committed snapshot.
    ///
    /// Retire active requests separately so the reactor can cancel their network requests.
    pub(in crate::zakura::header_sync) fn retire_obsolete_unstarted(
        &mut self,
        current: &EngineSnapshot,
    ) -> usize {
        let obsolete = self.policy.obsolete_unstarted(current);
        for peer in &obsolete {
            self.remove(peer);
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
        self.policy
            .obsolete_active(current)
            .iter()
            .filter_map(|peer| self.remove(peer))
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

    /// Apply the capacity effect of one staging decision, and return its result.
    fn apply_stage(&mut self, decision: StageDecision) -> QueueWorkResult {
        if let Some(peer) = &decision.release {
            self.release_capacity(peer);
        }
        decision.result
    }

    /// Drop `peer`'s reservation and the queue's clones of its leases.
    fn release_capacity(&mut self, peer: &ZakuraPeerId) {
        self.request_reservations.remove(peer);
        self.staged_capacity.remove(peer);
    }

    pub(in crate::zakura::header_sync) fn stage(
        &mut self,
        peer: ZakuraPeerId,
        target: AdvertisedHeaderTarget,
        priority: PeerWorkPriority,
    ) -> QueueWorkResult {
        let decision = self.policy.stage(peer, target, priority);
        self.apply_stage(decision)
    }

    /// Stage one ordinary branch target only when no other peer already owns it.
    ///
    /// See [`WorkPolicy::stage_distinct_target`].
    pub(in crate::zakura::header_sync) fn stage_distinct_target(
        &mut self,
        peer: ZakuraPeerId,
        target: AdvertisedHeaderTarget,
        priority: PeerWorkPriority,
    ) -> QueueWorkResult {
        let decision = self.policy.stage_distinct_target(peer, target, priority);
        self.apply_stage(decision)
    }

    /// Return the peer's current unstarted target, if any.
    pub(in crate::zakura::header_sync) fn awaiting_target(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<&AdvertisedHeaderTarget> {
        self.policy.awaiting_target(peer)
    }

    pub(in crate::zakura::header_sync) fn awaiting(
        &self,
        peer: &ZakuraPeerId,
        session_id: u64,
        target_tip_hash: zakura_chain::block::Hash,
        scope: HeaderWorkAuthority,
    ) -> Option<&AdvertisedHeaderTarget> {
        self.policy
            .awaiting(peer, session_id, target_tip_hash, scope)
    }

    pub(in crate::zakura::header_sync) fn start(&mut self, request: ActiveHeaderRequest) -> bool {
        let reserved = self.request_reservations.contains_key(&request.peer);
        self.policy.start(request, reserved)
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
        self.policy.bind_repair_episode(owner, episode);
        true
    }

    /// Return the durable evidence episode bound to one active repair request.
    pub(in crate::zakura::header_sync) fn repair_episode(
        &self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<zakura_header_chain::AuxiliaryRequirementEpisode> {
        self.policy.repair_episode(owner)
    }

    #[cfg(test)]
    pub(in crate::zakura::header_sync) fn bind_repair_episode_for_test(
        &mut self,
        owner: HeaderSyncWorkOwner,
        episode: zakura_header_chain::AuxiliaryRequirementEpisode,
    ) {
        assert!(self.active_owner(owner).is_some());
        self.policy.set_repair_episode_for_test(owner, episode);
    }

    pub(in crate::zakura::header_sync) fn remove(
        &mut self,
        peer: &ZakuraPeerId,
    ) -> Option<ActiveHeaderRequest> {
        self.release_capacity(peer);
        self.policy.remove(peer)
    }

    pub(in crate::zakura::header_sync) fn remove_owner(
        &mut self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<ActiveHeaderRequest> {
        let peer = self.policy.owner_peer(owner)?;
        self.remove(&peer)
    }

    /// Return the active target carrying one exact durable owner.
    pub(in crate::zakura::header_sync) fn active_owner(
        &self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<&ActiveHeaderRequest> {
        self.policy.active_owner(owner)
    }

    pub(in crate::zakura::header_sync) fn remove_unstarted(&mut self, peer: &ZakuraPeerId) {
        if self.policy.remove_unstarted(peer) {
            self.release_capacity(peer);
        }
    }

    pub(in crate::zakura::header_sync) fn active(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<&ActiveHeaderRequest> {
        self.policy.active(peer)
    }

    pub(in crate::zakura::header_sync) fn active_mut(
        &mut self,
        peer: &ZakuraPeerId,
    ) -> Option<&mut ActiveHeaderRequest> {
        self.policy.active_mut(peer)
    }

    /// Bound one request by its fair share and currently unowned capacity.
    pub(in crate::zakura::header_sync) fn reservable_header_count(&self, desired: u32) -> u32 {
        policy::fair_request_count(desired, self.budget.remaining())
    }

    /// Bound one VCT repair request by all currently unowned aggregate capacity.
    pub(in crate::zakura::header_sync) fn reservable_repair_header_count(
        &self,
        desired: u32,
    ) -> u32 {
        policy::repair_request_count(desired, self.budget.remaining())
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

    /// Reserve aggregate capacity for one selected auxiliary repair range.
    pub(in crate::zakura::header_sync) fn reserve_repair_request(
        &mut self,
        peer: &ZakuraPeerId,
        count: u32,
    ) -> bool {
        if self.request_reservations.contains_key(peer) {
            return false;
        }
        let count = usize::try_from(count).unwrap_or(usize::MAX);
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

    /// Transfer one page's header count from its round's reservation into
    /// owned staged capacity.
    pub(in crate::zakura::header_sync) fn consume_response_capacity(
        &mut self,
        peer: &ZakuraPeerId,
        returned: usize,
    ) -> bool {
        let Some(reservation) = self.request_reservations.get(peer) else {
            return false;
        };
        let Ok(lease) = reservation.consume(returned) else {
            return false;
        };
        if reservation.remaining() == 0 {
            self.request_reservations.remove(peer);
        }
        if let Some(lease) = lease {
            self.staged_capacity
                .entry(peer.clone())
                .or_default()
                .push(lease);
        }
        self.publish_phase_metrics();
        true
    }

    /// Keep the headers of `peer`'s current round charged after its peer slot
    /// retires, or `None` when `peer` has no started round.
    ///
    /// The round comes from the same work that names the leases, so no caller
    /// can attribute them to another peer or an older round.
    pub(in crate::zakura::header_sync) fn retain_headers(
        &self,
        peer: &ZakuraPeerId,
    ) -> Option<RetainedHeaders> {
        let incarnation = self.policy.active(peer)?.incarnation;
        Some(RetainedHeaders {
            peer: peer.clone(),
            incarnation,
            call: None,
            leases: self.staged_capacity.get(peer).cloned().unwrap_or_default(),
            end: HeaderWorkEnd::Dropped,
            #[cfg(any(test, zakura_sim))]
            observer: ObserverSlot::default(),
        })
    }

    /// The queue's lease clones for `peer`, without any call's ownership.
    #[cfg(test)]
    pub(in crate::zakura::header_sync) fn retain_header_capacity(
        &self,
        peer: &ZakuraPeerId,
    ) -> Vec<HeaderCapacityLease> {
        self.staged_capacity.get(peer).cloned().unwrap_or_default()
    }

    /// Record that `peer`'s current round waits for `work`'s call.
    pub(in crate::zakura::header_sync) fn bind_call(
        &mut self,
        peer: &ZakuraPeerId,
        work: WorkCall,
    ) {
        self.policy.bind_call(peer, work);
    }

    /// Settle `work` on `peer`, and return whether `peer`'s current round waited for it.
    pub(in crate::zakura::header_sync) fn settle_call(
        &mut self,
        peer: &ZakuraPeerId,
        work: WorkCall,
    ) -> bool {
        self.policy.settle_call(peer, work)
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
        let counts = self
            .policy
            .phase_counts(|peer| self.owned_header_count(peer));
        // The conversion is exact because the count is at most 4,000.
        metrics::gauge!("sync.header.chunk_budget.receiving").set(counts.receiving as f64);
        metrics::gauge!("sync.header.chunk_budget.preparing").set(counts.preparing as f64);
        metrics::gauge!("sync.header.chunk_budget.applying").set(counts.applying as f64);
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
mod model_tests;

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, sync::Arc};

    use super::*;
    use crate::zakura::{
        header_sync::{wire::AuxSchema, HeaderEntry, HeaderSyncRequestId, Status, MAX_HS_RANGE},
        transport::{NodeResources, ResourceEnvelope, TransportLimits},
    };
    use zakura_chain::{block, block::genesis::regtest_genesis_block, work::difficulty::U256};
    use zakura_header_chain::{
        AlarmSet, ChainScore, EngineMode, Frontier, FrontierSet, HeaderGeneration, HeaderLocator,
        StateVersion, SuffixWork, VerifiedGeneration, MAX_STAGED_TARGETS_V1,
    };

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    /// The header chunk budget and the transport budgets fund different things, so
    /// saturating either leaves the other's capacity free.
    #[test]
    fn header_chunk_capacity_is_independent_of_transport_saturation() {
        let node = NodeResources::new(ResourceEnvelope::new(TransportLimits::with_windows(64, 1)));
        let stream = node.connection().stream();
        let budget = HeaderChunkBudget::default();

        let transport_hold = node.hold_node_budgets();
        let reservation = budget
            .reserve(HEADER_CHUNK_BUDGET_CAPACITY_V1)
            .expect("saturated frame budgets hold no header chunks");
        assert_eq!(budget.remaining(), 0);
        assert!(budget.reserve(1).is_none());

        drop(transport_hold);
        let frame = stream.raw.try_admit(64);
        assert!(
            frame.is_some(),
            "saturated header chunks hold no frame bytes"
        );
        assert!(node.usage().raw_bytes > 0);

        drop(reservation);
        assert_eq!(budget.remaining(), HEADER_CHUNK_BUDGET_CAPACITY_V1);
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
                max_subscriptions: 1,
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
            incarnation: WorkIncarnation::UNSTARTED,
            cursor: None,
            max_header_count: 1_000,
            tree_aux_schema: AuxSchema::None,
        }
    }

    #[test]
    fn unknown_targets_above_finality_remain_eligible_regardless_of_work_claims() {
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
        incomparable.status.selected_tip_height = block::Height(90);
        incomparable.status.work_anchor_hash = hash(11);
        incomparable.status.suffix_cumulative_work = U256::MAX;
        assert!(incomparable.is_discovery_eligible(&local));
        assert_eq!(incomparable.claimed_work_order(&local), None);

        let mut known = advertisement(4);
        known.status.selected_tip_height = local.frontiers.header_best.height;
        known.status.selected_tip_hash = local.frontiers.header_best.hash;
        assert!(!known.is_discovery_eligible(&local));

        let mut pure_requester = advertisement(5);
        pure_requester.status.selected_tip_height = local.frontiers.header_best.height;
        pure_requester.status.max_headers_per_response = 0;
        assert!(!pure_requester.is_discovery_eligible(&local));
    }

    #[test]
    fn discovery_skips_targets_at_or_below_finality_but_keeps_shorter_forks() {
        let local = snapshot();
        let mut target = advertisement(3);
        target.status.suffix_cumulative_work = U256::MAX;
        target.status.work_anchor_hash = hash(11);
        for height in [0, 9, 10] {
            target.status.selected_tip_height = block::Height(height);
            assert!(!target.is_discovery_eligible(&local));
        }
        target.status.selected_tip_height = block::Height(11);
        assert!(target.is_discovery_eligible(&local));
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
            incarnation: WorkIncarnation::UNSTARTED,
            cursor: None,
            max_header_count: 1_000,
            tree_aux_schema: AuxSchema::None,
        };
        assert!(queue.reserve_request(&peer(1), 1));
        assert!(queue.start(request.clone()));
        let request = queue.active(&peer(1)).expect("the request started").clone();
        assert_ne!(request.incarnation, WorkIncarnation::UNSTARTED);
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
            .expect("a partial page fits its reservation")
            .expect("a nonempty page returns an owned lease");
        assert_eq!(
            budget.usage(),
            HeaderChunkUsage {
                reserved: MAX_HEADER_CHUNK_RESERVATION_V1 - returned,
                owned: returned,
            },
            "the rest stays reserved for the credit still outstanding"
        );
        drop(reservation);
        assert_eq!(
            budget.usage(),
            HeaderChunkUsage {
                reserved: 0,
                owned: returned,
            },
            "ending the round returns its unused capacity"
        );
        drop(lease);
        assert_eq!(budget.usage(), HeaderChunkUsage::default());

        let empty = budget
            .reserve(MAX_HEADER_CHUNK_RESERVATION_V1)
            .expect("released capacity can be reserved again");
        assert!(empty
            .consume(0)
            .expect("an empty page fits its reservation")
            .is_none());
        drop(empty);
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
}
