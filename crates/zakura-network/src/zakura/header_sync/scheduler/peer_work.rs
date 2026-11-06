//! The runtime owner of header work: budget handles and their effectful drops.
//!
//! [`PeerWorkQueue`] asks [`policy::WorkPolicy`] for each decision and applies
//! its capacity effect. Dropping a [`HeaderCountReservation`] or the last clone
//! of a [`HeaderCapacityLease`] returns its count to the budget and wakes waiters.

use std::sync::Arc;

#[cfg(any(test, zakura_sim))]
use zakura_chain::block;
use zakura_header_chain::{EngineSnapshot, Frontier, HeaderSyncWorkOwner, SourceId};
#[cfg(any(test, zakura_sim))]
use zakura_rt::sync::counter;
use zakura_rt::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        watch, Mutex, PoisonError,
    },
};

mod policy;
#[cfg(zakura_sim)]
mod probe;

pub(in crate::zakura::header_sync) use policy::{
    decide_offer, page_next, repair_page, OfferDecision, OfferFacts, OfferedPeerWork, PageFacts,
    PageNext, PeerWorkPriority, QueueWorkResult, RepairPage, SpentOffer, StorageCall, WorkCall,
    HEADER_CHUNK_BUDGET_CAPACITY_V1, MAX_HEADER_CHUNK_RESERVATION_V1,
};
pub use policy::{
    ActiveHeaderRequest, AdvertisedHeaderTarget, HeaderTargetPhase, HeaderTargetPurpose,
    HeaderWorkEnd, StorageCallKind, WorkIncarnation,
};
use policy::{HeaderChunkUsage, StageDecision, WorkPolicy};
#[cfg(zakura_sim)]
pub(in crate::zakura::header_sync) use probe::ProbeTargets;
#[cfg(zakura_sim)]
pub use probe::{HeaderResourceProbe, HeaderResources};

#[cfg(any(test, zakura_sim))]
use super::super::observe::{
    HeaderSyncObserver, ObserverSlot, PageArrival, ReactorEvent, RequestRef, ReservationEnd,
    ReservationPurpose,
};
use super::super::{events::HeaderPortOperation, v9::Cursor, HeaderEntry, ZakuraPeerId};
use crate::zakura::transport::sim_arg;

#[derive(Debug)]
struct HeaderChunkBudgetInner {
    capacity: usize,
    usage: Mutex<HeaderChunkUsage>,
    released: watch::Sender<u64>,
    #[cfg(any(test, zakura_sim))]
    last_reservation: ReservationNumbers,
}

/// The last reservation number handed out, 0 before any. It prints nothing,
/// so an observed budget prints as an unobserved one does.
#[cfg(any(test, zakura_sim))]
#[derive(Default)]
struct ReservationNumbers(counter::AtomicU64);

#[cfg(any(test, zakura_sim))]
impl std::fmt::Debug for ReservationNumbers {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReservationNumbers")
    }
}

#[derive(Clone, Debug)]
struct HeaderChunkBudget(Arc<HeaderChunkBudgetInner>);

impl Default for HeaderChunkBudget {
    fn default() -> Self {
        Self(Arc::new(HeaderChunkBudgetInner {
            capacity: HEADER_CHUNK_BUDGET_CAPACITY_V1,
            usage: Mutex::new(HeaderChunkUsage::default()),
            released: watch::channel(0).0,
            #[cfg(any(test, zakura_sim))]
            last_reservation: ReservationNumbers::default(),
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

    /// Reserve `count` headers, reporting the reservation as `opening` says.
    fn reserve(
        &self,
        count: usize,
        #[cfg(any(test, zakura_sim))] opening: Opening<'_>,
    ) -> Option<HeaderCountReservation> {
        let mut usage = self.0.usage.lock().unwrap_or_else(PoisonError::into_inner);
        *usage = usage.reserve(self.0.capacity, count)?;
        self.publish(*usage);
        drop(usage);
        Some(HeaderCountReservation(Arc::new(
            HeaderCountReservationInner {
                budget: self.clone(),
                remaining: AtomicUsize::new(count),
                #[cfg(any(test, zakura_sim))]
                tag: opening.open(&self.0.last_reservation, count),
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
                #[cfg(any(test, zakura_sim))]
                tag: None,
            }))
        }))
    }

    /// Return `count` headers through `released`, and wake waiters when any return.
    fn release(
        &self,
        count: usize,
        released: impl FnOnce(HeaderChunkUsage, usize) -> HeaderChunkUsage,
    ) {
        let mut usage = self.0.usage.lock().unwrap_or_else(PoisonError::into_inner);
        *usage = released(*usage, count);
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
    /// What its end reports, once the queue opened it with an observer.
    #[cfg(any(test, zakura_sim))]
    tag: Option<ReservationTag>,
}

/// Who a new reservation is for, which the budget reports it with.
#[cfg(any(test, zakura_sim))]
struct Opening<'a> {
    /// Where to report it, if anywhere.
    observer: &'a ObserverSlot,
    peer: &'a ZakuraPeerId,
    purpose: ReservationPurpose,
    /// The running round a continuation extends.
    request: Option<RequestRef>,
}

#[cfg(any(test, zakura_sim))]
impl Opening<'_> {
    /// An opening nothing observes, for a test that seeds capacity directly.
    #[cfg(test)]
    fn unobserved(peer: &ZakuraPeerId) -> Opening<'_> {
        static NONE: ObserverSlot = ObserverSlot(None);
        Opening {
            observer: &NONE,
            peer,
            purpose: ReservationPurpose::Ordinary,
            request: None,
        }
    }

    /// Numbers a reservation of `headers` from `last`, reports it, and
    /// returns its tag, or nothing when no observer is installed.
    fn open(self, last: &ReservationNumbers, headers: usize) -> Option<ReservationTag> {
        let observer = self.observer.0.clone()?;
        let id = last.0.fetch_add(1, counter::Ordering::Relaxed) + 1;
        observer.observe(ReactorEvent::ReservationOpened {
            reservation: id,
            peer: self.peer.clone(),
            purpose: self.purpose,
            headers,
            request: self.request,
        });
        Some(ReservationTag {
            id,
            observer,
            end: ReservationEnd::Dropped,
        })
    }
}

/// An observed reservation's identity, the headers it still holds, and why
/// it will end.
#[cfg(any(test, zakura_sim))]
#[derive(Debug)]
struct ReservationTag {
    id: u64,
    observer: Arc<dyn HeaderSyncObserver>,
    end: ReservationEnd,
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
    /// Returns the unstaged headers, then reports the end if it's observed.
    fn drop(&mut self) {
        let remaining = self.remaining.swap(0, AtomicOrdering::SeqCst);
        if remaining != 0 {
            self.budget
                .release(remaining, HeaderChunkUsage::release_reserved);
        }
        #[cfg(any(test, zakura_sim))]
        if let Some(tag) = self.tag.as_ref().filter(|_| !std::thread::panicking()) {
            // An unwinding reactor reports nothing, as `HeaderSyncObserver` says.
            tag.observer.observe(ReactorEvent::ReservationEnded {
                reservation: tag.id,
                released: remaining,
                cause: tag.end,
            });
        }
    }
}

#[derive(Clone, Debug)]
pub(in crate::zakura::header_sync) struct HeaderCapacityLease(Arc<HeaderCapacityLeaseInner>);

#[derive(Debug)]
struct HeaderCapacityLeaseInner {
    budget: HeaderChunkBudget,
    count: usize,
    /// What its return reports, once the queue staged it with an observer.
    #[cfg(any(test, zakura_sim))]
    tag: Option<LeaseTag>,
}

/// An observed lease's round.
#[cfg(any(test, zakura_sim))]
#[derive(Debug)]
struct LeaseTag {
    peer: ZakuraPeerId,
    incarnation: u64,
    observer: Arc<dyn HeaderSyncObserver>,
}

impl PartialEq for HeaderCapacityLease {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for HeaderCapacityLease {}

impl Drop for HeaderCapacityLeaseInner {
    /// Returns the lease's headers, then reports the return if it's observed.
    fn drop(&mut self) {
        self.budget
            .release(self.count, HeaderChunkUsage::release_owned);
        #[cfg(any(test, zakura_sim))]
        if let Some(tag) = self.tag.take().filter(|_| !std::thread::panicking()) {
            // An unwinding reactor reports nothing, as `HeaderSyncObserver` says.
            tag.observer.observe(ReactorEvent::LeaseReturned {
                peer: tag.peer,
                incarnation: tag.incarnation,
                headers: self.count,
            });
        }
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
    #[cfg(any(test, zakura_sim))]
    peer: ZakuraPeerId,
    incarnation: WorkIncarnation,
    /// The call that holds the headers.
    call: StorageCall,
    /// The capacity these headers hold until this value drops.
    #[cfg_attr(not(any(test, zakura_sim)), allow(dead_code))]
    leases: Vec<HeaderCapacityLease>,
    end: HeaderWorkEnd,
    #[cfg(any(test, zakura_sim))]
    observer: ObserverSlot,
}

impl RetainedHeaders {
    /// The peer whose work these headers belong to.
    #[cfg(test)]
    pub(in crate::zakura::header_sync) fn peer(&self) -> &ZakuraPeerId {
        &self.peer
    }

    /// The round and call holding these headers.
    pub(in crate::zakura::header_sync) fn work(&self) -> WorkCall {
        WorkCall {
            incarnation: self.incarnation,
            call: self.call,
        }
    }

    /// The number of headers retained.
    #[cfg(any(test, zakura_sim))]
    pub(in crate::zakura::header_sync) fn headers(&self) -> usize {
        self.leases
            .iter()
            .map(|lease| lease.0.count)
            .fold(0usize, usize::saturating_add)
    }

    /// Report this ownership's events to `observer`, starting with its hold.
    #[cfg(any(test, zakura_sim))]
    pub(in crate::zakura::header_sync) fn observed_by(&mut self, observer: &ObserverSlot) {
        self.observer = observer.clone();
        self.report_held();
    }

    /// Record how this ownership will end when it drops.
    pub(in crate::zakura::header_sync) fn end_as(&mut self, end: HeaderWorkEnd) {
        self.end = end;
    }

    /// Move these headers from their prepare to its `apply`, with no release between.
    pub(in crate::zakura::header_sync) fn hand_to_apply(&mut self, apply: StorageCall) {
        debug_assert!(
            self.call.kind == StorageCallKind::Prepare && apply.kind == StorageCallKind::Apply,
            "retained headers move only from a prepare to its apply"
        );
        self.end = HeaderWorkEnd::HandedToApply {
            apply: apply.number,
        };
        self.record_end();
        self.call = apply;
        self.end = HeaderWorkEnd::Dropped;
        #[cfg(any(test, zakura_sim))]
        self.report_held();
    }

    /// Report that this ownership's call holds its headers.
    #[cfg(any(test, zakura_sim))]
    fn report_held(&self) {
        if let Some(observer) = &self.observer.0 {
            observer.observe(ReactorEvent::HeadersHeld {
                call: self.call.number,
                kind: self.call.kind,
                peer: self.peer.clone(),
                incarnation: self.incarnation.get(),
                headers: self.headers(),
                leases: self
                    .leases
                    .iter()
                    .filter_map(|lease| lease.0.tag.as_ref().map(|tag| tag.incarnation))
                    .collect(),
            });
        }
    }

    /// Report this ownership's end before any lease can release.
    fn record_end(&self) {
        #[cfg(any(test, zakura_sim))]
        if let Some(observer) = &self.observer.0 {
            // An unwinding reactor reports nothing, as `HeaderSyncObserver` says.
            if !std::thread::panicking() {
                observer.observe(ReactorEvent::HeadersEnded {
                    call: self.call.number,
                    incarnation: self.incarnation.get(),
                    headers: self.headers(),
                    end: self.end,
                });
            }
        }
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
    #[cfg(any(test, zakura_sim))]
    observing: QueueObservation,
}

/// What the queue needs to report its reservations, pages and leases.
#[cfg(any(test, zakura_sim))]
#[derive(Clone, Default)]
struct QueueObservation {
    /// Where the queue reports them: the observer's capacity handle.
    observer: ObserverSlot,
    /// The arrival of the page [`PeerWorkQueue::stage_page`] is staging.
    arriving: Option<PageArrival>,
    /// The staging that page's capacity took, reported once the page is in
    /// its round.
    staged: Option<StagedPage>,
    /// The reservation that page spent to the end, whose own end reports
    /// after the page's staging.
    exhausted: Option<HeaderCountReservation>,
    /// The observer itself. This struct is the queue's last field, so this
    /// drops after the queue's reservations and leases, and an observer
    /// that reads the reactor's lifetime from its handles sees their ends
    /// first.
    lifetime: ObserverSlot,
}

/// A page's capacity staging, as [`PeerWorkQueue::stage_page`] reports it
/// once the page is in its round.
#[cfg(any(test, zakura_sim))]
#[derive(Clone, Debug)]
struct StagedPage {
    observer: Arc<dyn HeaderSyncObserver>,
    arrival: PageArrival,
    request: RequestRef,
    reservation: u64,
    headers: usize,
    reserved_left: usize,
}

/// Prints nothing of the observation, so an observed queue prints as an
/// unobserved one does.
#[cfg(any(test, zakura_sim))]
impl std::fmt::Debug for QueueObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QueueObservation")
    }
}

/// Reads see the value-only policy. Every mutation goes through the queue, which
/// pairs it with its capacity effect.
impl std::ops::Deref for PeerWorkQueue {
    type Target = WorkPolicy;

    fn deref(&self) -> &WorkPolicy {
        &self.policy
    }
}

impl PeerWorkQueue {
    pub(in crate::zakura::header_sync) fn capacity_releases(&self) -> watch::Receiver<u64> {
        self.budget.0.released.subscribe()
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
        #[cfg(any(test, zakura_sim))]
        self.end_as(peer, ReservationEnd::Released);
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

    pub(in crate::zakura::header_sync) fn start(&mut self, request: ActiveHeaderRequest) -> bool {
        let reserved = self.request_reservations.contains_key(&request.peer);
        #[cfg(any(test, zakura_sim))]
        let peer = request.peer.clone();
        let started = self.policy.start(request, reserved);
        #[cfg(any(test, zakura_sim))]
        if started {
            self.report_bound(&peer);
        }
        started
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
        #[cfg(any(test, zakura_sim))]
        self.report_released(peer);
        self.policy.remove(peer)
    }

    pub(in crate::zakura::header_sync) fn remove_owner(
        &mut self,
        owner: HeaderSyncWorkOwner,
    ) -> Option<ActiveHeaderRequest> {
        let peer = self.policy.owner_peer(owner)?;
        self.remove(&peer)
    }

    pub(in crate::zakura::header_sync) fn remove_unstarted(&mut self, peer: &ZakuraPeerId) {
        if self.policy.remove_unstarted(peer) {
            self.release_capacity(peer);
        }
    }

    /// Mutable access for tests and models that seed a request's fields directly.
    #[cfg(any(test, feature = "header-fuzz"))]
    pub(in crate::zakura::header_sync) fn active_mut(
        &mut self,
        peer: &ZakuraPeerId,
    ) -> Option<&mut ActiveHeaderRequest> {
        self.policy.active_mut(peer)
    }

    /// Charge one admitted page to `peer`'s reservation and stage its entries.
    ///
    /// Return false, changing nothing, when `peer`'s work is not receiving or
    /// the page does not fit the reservation.
    pub(in crate::zakura::header_sync) fn stage_page(
        &mut self,
        peer: &ZakuraPeerId,
        ancestor: Frontier,
        entries: Vec<HeaderEntry>,
        cursor: Cursor,
        #[cfg(any(test, zakura_sim))] arrival: PageArrival,
    ) -> bool {
        #[cfg(any(test, zakura_sim))]
        let hashes: Vec<block::Hash> = {
            self.observing.arriving = Some(arrival);
            entries.iter().map(|entry| entry.header.hash()).collect()
        };
        if !self.policy.is_receiving(peer) || !self.consume_response_capacity(peer, entries.len()) {
            #[cfg(any(test, zakura_sim))]
            {
                self.observing.arriving = None;
            }
            return false;
        }
        self.policy.stage_page(peer, ancestor, entries, cursor);
        #[cfg(any(test, zakura_sim))]
        self.report_staged(peer, hashes);
        debug_assert_eq!(
            self.owned_header_count(peer),
            self.policy
                .active(peer)
                .map_or(0, |request| request.entries.len()),
            "every staged header retains exactly one owned budget unit",
        );
        true
    }

    /// Move `peer`'s staged headers into preparation. See [`WorkPolicy::begin_prepare`].
    pub(in crate::zakura::header_sync) fn begin_prepare(
        &mut self,
        peer: &ZakuraPeerId,
        bounded_prefix: bool,
    ) -> Option<HeaderPortOperation> {
        let operation = self.policy.begin_prepare(peer, bounded_prefix);
        self.publish_phase_metrics();
        operation
    }

    /// Move `peer`'s prepared work to its state call, and return whether it was preparing.
    pub(in crate::zakura::header_sync) fn begin_apply(&mut self, peer: &ZakuraPeerId) -> bool {
        let began = self.policy.begin_apply(peer);
        self.publish_phase_metrics();
        began
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
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        count <= MAX_HEADER_CHUNK_RESERVATION_V1
            && sim_arg!(self.reserve(peer, count); ReservationPurpose::Ordinary)
    }

    /// Reserve aggregate capacity for one selected auxiliary repair range.
    pub(in crate::zakura::header_sync) fn reserve_repair_request(
        &mut self,
        peer: &ZakuraPeerId,
        count: u32,
    ) -> bool {
        sim_arg!(self.reserve(peer, usize::try_from(count).unwrap_or(usize::MAX)); ReservationPurpose::Repair)
    }

    /// Give `peer`, which holds no reservation, one of `count` headers for
    /// `purpose`.
    fn reserve(
        &mut self,
        peer: &ZakuraPeerId,
        count: usize,
        #[cfg(any(test, zakura_sim))] purpose: ReservationPurpose,
    ) -> bool {
        if self.request_reservations.contains_key(peer) {
            return false;
        }
        let Some(reservation) = sim_arg!(self.budget.reserve(count); self.opening(peer, purpose))
        else {
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
        #[cfg(any(test, zakura_sim))]
        self.end_as(peer, ReservationEnd::Cancelled);
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
        #[cfg(any(test, zakura_sim))]
        let lease = {
            let (lease, staged) = staged_page(
                &self.policy,
                peer,
                reservation,
                lease,
                returned,
                self.observing.arriving.take(),
            );
            self.observing.staged = staged;
            lease
        };
        // The report takes what the reservation keeps from the queue's own
        // single read of it, so it adds no read and no scheduling point.
        #[cfg(any(test, zakura_sim))]
        {
            let remaining = reservation.remaining();
            if let Some(staged) = &mut self.observing.staged {
                staged.reserved_left = remaining;
            }
            if remaining == 0 {
                self.end_as(peer, ReservationEnd::Staged);
                // Its end reports after the page's staging, which spent it.
                let exhausted = self.request_reservations.remove(peer);
                if self.observing.staged.is_some() {
                    self.observing.exhausted = exhausted;
                }
            }
        }
        #[cfg(not(any(test, zakura_sim)))]
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
        call: StorageCall,
    ) -> Option<RetainedHeaders> {
        let incarnation = self.policy.active(peer)?.incarnation();
        Some(RetainedHeaders {
            #[cfg(any(test, zakura_sim))]
            peer: peer.clone(),
            incarnation,
            call,
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
                .reserve(staged, Opening::unobserved(peer))
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
                .reserve(reserved, Opening::unobserved(peer))
                .expect("the test request count fits remaining capacity");
            self.request_reservations.insert(peer.clone(), reservation);
        }
    }
}

/// The queue's reports to its observer.
#[cfg(any(test, zakura_sim))]
impl PeerWorkQueue {
    /// Report the page staged last, with its `hashes` and the headers its
    /// round now holds, as the policy has it. Its spent reservation's end
    /// reports after.
    fn report_staged(&mut self, peer: &ZakuraPeerId, hashes: Vec<block::Hash>) {
        if let Some(staged) = self.observing.staged.take() {
            let entries = self
                .policy
                .active(peer)
                .map_or(&[][..], |active| &active.entries[..]);
            let staged_total = entries.len();
            let stored = entries[staged_total.saturating_sub(staged.headers)..]
                .iter()
                .map(|entry| entry.header.hash())
                .collect();
            staged.observer.observe(ReactorEvent::PageStaged {
                from_peer: staged.arrival.from_peer,
                from_session: staged.arrival.from_session,
                request: staged.request,
                reservation: staged.reservation,
                subscription: staged.arrival.subscription,
                frame: staged.arrival.frame,
                headers: staged.headers,
                reserved_left: staged.reserved_left,
                hashes,
                stored,
                staged_total,
            });
        }
        self.observing.exhausted = None;
    }

    /// Report reservations, staged pages, lease returns and round releases
    /// from now on, to `observer`'s
    /// [`capacity_observer`](HeaderSyncObserver::capacity_observer).
    pub(in crate::zakura::header_sync) fn observed_by(&mut self, observer: &ObserverSlot) {
        self.observing.lifetime = observer.clone();
        self.observing.observer = ObserverSlot(observer.0.as_ref().map(|observer| {
            observer
                .capacity_observer()
                .unwrap_or_else(|| observer.clone())
        }));
    }

    /// How a reservation for `peer` and `purpose` reports: an ordinary one
    /// names `peer`'s running round, which only a continuation has.
    fn opening<'a>(&'a self, peer: &'a ZakuraPeerId, purpose: ReservationPurpose) -> Opening<'a> {
        Opening {
            observer: &self.observing.observer,
            peer,
            purpose,
            request: match purpose {
                ReservationPurpose::Ordinary => self.policy.active(peer).map(RequestRef::of),
                ReservationPurpose::Repair => None,
            },
        }
    }

    /// Record that `peer`'s reservation, which the queue is about to drop,
    /// ends as `end`. A handle someone else still shares keeps its end.
    fn end_as(&mut self, peer: &ZakuraPeerId, end: ReservationEnd) {
        if let Some(tag) = self
            .request_reservations
            .get_mut(peer)
            .and_then(|reservation| Arc::get_mut(&mut reservation.0))
            .and_then(|inner| inner.tag.as_mut())
        {
            tag.end = end;
        }
    }

    /// Report that the queue is removing `peer`'s started round, if it has
    /// one, with the headers its own lease clones still hold for `peer`.
    /// Called after the removal's capacity release, whatever that did.
    fn report_released(&self, peer: &ZakuraPeerId) {
        if let (Some(observer), Some(active)) =
            (&self.observing.observer.0, self.policy.active(peer))
        {
            observer.observe(ReactorEvent::RoundReleased {
                peer: peer.clone(),
                incarnation: active.incarnation().get(),
                queue_owned: self.owned_header_count(peer),
            });
        }
    }

    /// Report that `peer`'s reservation backs the round that just started.
    fn report_bound(&self, peer: &ZakuraPeerId) {
        let tag = self
            .request_reservations
            .get(peer)
            .and_then(|reservation| reservation.0.tag.as_ref());
        let (Some(tag), Some(active)) = (tag, self.policy.active(peer)) else {
            return;
        };
        tag.observer.observe(ReactorEvent::ReservationBound {
            reservation: tag.id,
            request: RequestRef::of(active),
            max_headers: usize::try_from(active.max_header_count).unwrap_or(usize::MAX),
        });
    }
}

/// Record the capacity a page of `headers` that `arrival` brought took from
/// `reservation` for `peer`'s round, to report once the page is in the round,
/// and tag its `lease` with that round so its return reports too. The caller
/// fills in what the reservation keeps from its own read of it. A page
/// staged without an arrival or on an unobserved reservation records nothing.
#[cfg(any(test, zakura_sim))]
fn staged_page(
    policy: &WorkPolicy,
    peer: &ZakuraPeerId,
    reservation: &HeaderCountReservation,
    mut lease: Option<HeaderCapacityLease>,
    headers: usize,
    arrival: Option<PageArrival>,
) -> (Option<HeaderCapacityLease>, Option<StagedPage>) {
    let (Some(tag), Some(active), Some(arrival)) =
        (reservation.0.tag.as_ref(), policy.active(peer), arrival)
    else {
        return (lease, None);
    };
    if let Some(inner) = lease.as_mut().and_then(|lease| Arc::get_mut(&mut lease.0)) {
        inner.tag = Some(LeaseTag {
            peer: peer.clone(),
            incarnation: active.incarnation().get(),
            observer: tag.observer.clone(),
        });
    }
    let staged = StagedPage {
        observer: tag.observer.clone(),
        arrival,
        request: RequestRef::of(active),
        reservation: tag.id,
        headers,
        reserved_left: 0,
    };
    (lease, Some(staged))
}

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod model_tests;

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, sync::Arc};

    use zakura_header_chain::HeaderWorkAuthority;

    use super::{
        fixtures::{self, hash, peer},
        *,
    };
    use crate::zakura::header_sync::{
        wire::AuxSchema, HeaderEntry, HeaderSyncRequestId, MAX_HS_RANGE,
    };
    use zakura_chain::{block, block::genesis::regtest_genesis_block, work::difficulty::U256};
    use zakura_header_chain::{Frontier, HeaderLocator, MAX_STAGED_TARGETS_V1};

    fn snapshot() -> EngineSnapshot {
        fixtures::snapshot(1)
    }

    fn advertisement(marker: u8) -> AdvertisedHeaderTarget {
        let local = snapshot();
        AdvertisedHeaderTarget {
            scope: HeaderWorkAuthority::for_target(&local, hash(marker)),
            session_id: 7,
            status: fixtures::status(u32::from(marker), hash(marker), u32::from(marker)),
        }
    }

    fn active_request(
        marker: u8,
        target: AdvertisedHeaderTarget,
        local: &EngineSnapshot,
        entries: Vec<HeaderEntry>,
    ) -> ActiveHeaderRequest {
        let request_id =
            HeaderSyncRequestId::new(u64::from(marker)).expect("the marker is nonzero");
        let mut request = ActiveHeaderRequest::new(
            HeaderTargetPurpose::Normal,
            peer(marker),
            SourceId::from_digest([marker; 32]),
            HeaderLocator::for_continuation(local.frontiers.finalized),
            zakura_header_chain::HeaderWorkOwner {
                authority: HeaderWorkAuthority::for_target(local, target.status.selected_tip_hash),
                session_id: target.session_id,
                request_id: std::num::NonZeroU64::new(request_id.get())
                    .expect("header-sync request IDs are nonzero"),
            }
            .into(),
            target,
            request_id,
            1_000,
            AuxSchema::None,
        );
        request.common_ancestor = Some(local.frontiers.finalized);
        request.entries = entries;
        request
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
        let request = ActiveHeaderRequest::new(
            HeaderTargetPurpose::Normal,
            peer(1),
            SourceId::from_digest([1; 32]),
            locator.clone(),
            zakura_header_chain::HeaderWorkOwner {
                authority: HeaderWorkAuthority::for_target(
                    &local,
                    replacement.status.selected_tip_hash,
                ),
                session_id: 7,
                request_id: std::num::NonZeroU64::new(1).expect("one is nonzero"),
            }
            .into(),
            replacement.clone(),
            HeaderSyncRequestId::new(1).expect("one is a nonzero request ID"),
            1_000,
            AuxSchema::None,
        );
        assert!(queue.reserve_request(&peer(1), 1));
        assert!(queue.start(request.clone()));
        let request = queue.active(&peer(1)).expect("the request started").clone();
        assert_ne!(request.incarnation(), WorkIncarnation::UNSTARTED);
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
            .reserve(
                MAX_HEADER_CHUNK_RESERVATION_V1,
                Opening::unobserved(&fixtures::peer(1)),
            )
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
            .reserve(
                MAX_HEADER_CHUNK_RESERVATION_V1,
                Opening::unobserved(&fixtures::peer(1)),
            )
            .expect("released capacity can be reserved again");
        assert!(empty
            .consume(0)
            .expect("an empty page fits its reservation")
            .is_none());
        drop(empty);
        assert_eq!(budget.usage(), HeaderChunkUsage::default());
    }

    /// The queue keeps its observer until its own reservations have
    /// reported their ends, so an observer that reads the reactor's lifetime
    /// from its handles outlives them, even when its capacity handle is a
    /// separate one.
    #[test]
    fn the_queue_keeps_its_observer_until_its_capacity_ends() {
        /// The observer whose drop marks the end of the reactor.
        #[derive(Debug)]
        struct Lifetime {
            alive: Arc<counter::AtomicU64>,
            seen: Arc<counter::AtomicU64>,
        }
        impl Drop for Lifetime {
            fn drop(&mut self) {
                self.alive.store(0, counter::Ordering::Relaxed);
            }
        }
        impl HeaderSyncObserver for Lifetime {
            fn observe(&self, _: ReactorEvent) {}
            fn capacity_observer(&self) -> Option<Arc<dyn HeaderSyncObserver>> {
                Some(Arc::new(Capacity {
                    alive: self.alive.clone(),
                    seen: self.seen.clone(),
                }))
            }
        }
        /// The capacity handle, which counts the ends it sees while the
        /// lifetime observer lives.
        #[derive(Debug)]
        struct Capacity {
            alive: Arc<counter::AtomicU64>,
            seen: Arc<counter::AtomicU64>,
        }
        impl HeaderSyncObserver for Capacity {
            fn observe(&self, event: ReactorEvent) {
                if matches!(event, ReactorEvent::ReservationEnded { .. })
                    && self.alive.load(counter::Ordering::Relaxed) == 1
                {
                    self.seen.fetch_add(1, counter::Ordering::Relaxed);
                }
            }
        }

        let alive = Arc::new(counter::AtomicU64::new(1));
        let seen = Arc::new(counter::AtomicU64::new(0));
        let mut queue = PeerWorkQueue::default();
        queue.observed_by(&ObserverSlot(Some(Arc::new(Lifetime {
            alive: alive.clone(),
            seen: seen.clone(),
        }))));
        assert!(queue.reserve_request(&peer(1), 5));
        drop(queue);
        assert_eq!(seen.load(counter::Ordering::Relaxed), 1);
        assert_eq!(alive.load(counter::Ordering::Relaxed), 0);
    }

    /// Records every event, in order.
    #[derive(Debug, Default)]
    struct Recorded(Mutex<Vec<ReactorEvent>>);

    impl HeaderSyncObserver for Recorded {
        fn observe(&self, event: ReactorEvent) {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(event);
        }
    }

    impl Recorded {
        /// The events recorded since the last call.
        fn take(&self) -> Vec<ReactorEvent> {
            std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
        }
    }

    /// A queue reporting to a new recorder, with `peer(1)`'s target staged,
    /// and the observer slot that recorder sits in.
    fn observed_queue() -> (PeerWorkQueue, Arc<Recorded>, ObserverSlot) {
        let recorder = Arc::new(Recorded::default());
        let observer = ObserverSlot(Some(recorder.clone()));
        let mut queue = PeerWorkQueue::default();
        queue.observed_by(&observer);
        assert_eq!(
            queue.stage(peer(1), advertisement(1), PeerWorkPriority::Normal),
            QueueWorkResult::NeedsLocator
        );
        (queue, recorder, observer)
    }

    /// A page entry whose header `version` sets its hash apart.
    fn versioned_entry(version: u32) -> HeaderEntry {
        let mut header = *regtest_genesis_block().header;
        header.version = version;
        HeaderEntry {
            header: Arc::new(header),
            body_size: 0,
            tree_aux: None,
        }
    }

    /// Stages `entries` on `peer(1)`'s round as a page that frame `frame` of
    /// subscription 3 brought on session 7.
    fn stage_arrival(queue: &mut PeerWorkQueue, entries: Vec<HeaderEntry>, frame: u64) -> bool {
        queue.stage_page(
            &peer(1),
            snapshot().frontiers.finalized,
            entries,
            Cursor {
                height: block::Height(0),
                hash: hash(0),
                headers: 0,
                bytes: 0,
            },
            PageArrival {
                from_peer: peer(1),
                from_session: 7,
                subscription: 3,
                frame: Some(frame),
            },
        )
    }

    /// An observed round reports its reservation's opening, binding and end,
    /// each page it stages with that page's values, a continuation's
    /// reservation and cancellation, the rounds of the leases a call holds,
    /// the round's release, and each lease's return once its last holder
    /// drops, in that order.
    #[test]
    fn an_observed_round_reports_its_capacity_in_order() {
        let (mut queue, recorder, observer) = observed_queue();
        let entries = [11, 12, 13].map(versioned_entry);
        let [first, second, third] = entries.each_ref().map(|entry| entry.header.hash());

        assert!(queue.reserve_request(&peer(1), 3));
        assert_eq!(
            recorder.take(),
            [ReactorEvent::ReservationOpened {
                reservation: 1,
                peer: peer(1),
                purpose: ReservationPurpose::Ordinary,
                headers: 3,
                request: None,
            }]
        );

        assert!(queue.start(active_request(1, advertisement(1), &snapshot(), Vec::new())));
        let round = RequestRef::of(queue.active(&peer(1)).expect("the round started"));
        let incarnation = round.incarnation;
        assert_eq!(
            recorder.take(),
            [ReactorEvent::ReservationBound {
                reservation: 1,
                request: round.clone(),
                max_headers: 1_000,
            }]
        );

        let staged = |hashes: Vec<block::Hash>, reserved_left, staged_total, frame| {
            ReactorEvent::PageStaged {
                from_peer: peer(1),
                from_session: 7,
                request: round.clone(),
                reservation: 1,
                subscription: 3,
                frame: Some(frame),
                headers: hashes.len(),
                reserved_left,
                stored: hashes.clone(),
                hashes,
                staged_total,
            }
        };
        assert!(stage_arrival(
            &mut queue,
            vec![entries[0].clone(), entries[1].clone()],
            9
        ));
        assert_eq!(recorder.take(), [staged(vec![first, second], 1, 2, 9)]);

        // The page that spends the reservation reports before its end.
        assert!(stage_arrival(&mut queue, vec![entries[2].clone()], 10));
        assert_eq!(
            recorder.take(),
            [
                staged(vec![third], 0, 3, 10),
                ReactorEvent::ReservationEnded {
                    reservation: 1,
                    released: 0,
                    cause: ReservationEnd::Staged,
                },
            ]
        );

        // A continuation names the running round, and its cancellation
        // returns every header it held.
        assert!(queue.reserve_request(&peer(1), 2));
        queue.cancel_request_reservation(&peer(1));
        assert_eq!(
            recorder.take(),
            [
                ReactorEvent::ReservationOpened {
                    reservation: 2,
                    peer: peer(1),
                    purpose: ReservationPurpose::Ordinary,
                    headers: 2,
                    request: Some(round.clone()),
                },
                ReactorEvent::ReservationEnded {
                    reservation: 2,
                    released: 2,
                    cause: ReservationEnd::Cancelled,
                },
            ]
        );

        let call = StorageCall {
            number: 1,
            kind: StorageCallKind::Prepare,
        };
        let mut held = queue
            .retain_headers(&peer(1), call)
            .expect("the round started");
        held.observed_by(&observer);
        assert_eq!(
            recorder.take(),
            [ReactorEvent::HeadersHeld {
                call: 1,
                kind: StorageCallKind::Prepare,
                peer: peer(1),
                incarnation,
                headers: 3,
                leases: vec![incarnation, incarnation],
            }]
        );

        // The call still holds both leases, so the release returns neither.
        assert!(queue.remove(&peer(1)).is_some());
        assert_eq!(
            recorder.take(),
            [ReactorEvent::RoundReleased {
                peer: peer(1),
                incarnation,
                queue_owned: 0,
            }]
        );

        drop(held);
        let returned = |headers| ReactorEvent::LeaseReturned {
            peer: peer(1),
            incarnation,
            headers,
        };
        assert_eq!(
            recorder.take(),
            [
                ReactorEvent::HeadersEnded {
                    call: 1,
                    incarnation,
                    headers: 3,
                    end: HeaderWorkEnd::Dropped,
                },
                returned(2),
                returned(1),
            ]
        );
    }

    /// Removing a round ends its reservation as released and returns the
    /// leases only the queue held before it reports the release, and the
    /// queue's own drop ends a reservation it still holds.
    #[test]
    fn a_removed_round_returns_its_capacity_before_its_release_reports() {
        let (mut queue, recorder, _observer) = observed_queue();
        assert!(queue.reserve_request(&peer(1), 2));
        assert!(queue.start(active_request(1, advertisement(1), &snapshot(), Vec::new())));
        let incarnation = queue
            .active(&peer(1))
            .expect("the round started")
            .incarnation()
            .get();
        assert!(stage_arrival(&mut queue, vec![versioned_entry(11)], 9));
        assert!(queue.reserve_request(&peer(2), 4));
        recorder.take();

        assert!(queue.remove(&peer(1)).is_some());
        assert_eq!(
            recorder.take(),
            [
                ReactorEvent::ReservationEnded {
                    reservation: 1,
                    released: 1,
                    cause: ReservationEnd::Released,
                },
                ReactorEvent::LeaseReturned {
                    peer: peer(1),
                    incarnation,
                    headers: 1,
                },
                ReactorEvent::RoundReleased {
                    peer: peer(1),
                    incarnation,
                    queue_owned: 0,
                },
            ]
        );

        drop(queue);
        assert_eq!(
            recorder.take(),
            [ReactorEvent::ReservationEnded {
                reservation: 2,
                released: 4,
                cause: ReservationEnd::Dropped,
            }]
        );
    }

    #[test]
    fn malformed_overreturn_preserves_reservation_until_terminal_cleanup() {
        let budget = HeaderChunkBudget::default();
        let reservation = budget
            .reserve(1, Opening::unobserved(&fixtures::peer(1)))
            .expect("one header fits the budget");
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

    /// An offer that an early check decides never reads its admission facts,
    /// and a held-back offer never reads its subscription.
    #[test]
    fn an_offer_reads_admission_facts_only_when_earlier_checks_pass() {
        let facts = |covered, work| OfferFacts {
            pruned_pause: false,
            covered,
            eligible: true,
            work,
        };
        let unread = || -> bool { panic!("an earlier check decides this offer") };
        assert!(matches!(
            decide_offer(facts(true, OfferedPeerWork::Idle), unread, unread),
            OfferDecision::Spend { .. }
        ));
        assert_eq!(
            decide_offer(facts(false, OfferedPeerWork::Other), unread, unread),
            OfferDecision::AlreadyActive { spend: false }
        );
        assert_eq!(
            decide_offer(facts(false, OfferedPeerWork::Idle), || true, unread),
            OfferDecision::HeldBack
        );
        assert_eq!(
            decide_offer(facts(false, OfferedPeerWork::Idle), || false, || true),
            OfferDecision::Stage
        );
    }

    /// A page that waits for credit or is ready for its checkpoint prefix never
    /// reads the budget or the durable prefix, and a full budget skips the
    /// durable prefix.
    #[test]
    fn a_page_reads_capacity_facts_only_when_earlier_checks_pass() {
        let local = snapshot();
        let active = active_request(
            1,
            advertisement(1),
            &local,
            vec![HeaderEntry {
                header: regtest_genesis_block().header.clone(),
                body_size: 0,
                tree_aux: None,
            }],
        );
        let staged_tip = active.staged_tip().expect("one header is staged");
        let facts = |reserved, checkpoint_prefix_ready| PageFacts {
            reaches_target: false,
            reserved,
            checkpoint_prefix_ready,
        };
        let unread = || -> bool { panic!("an earlier check decides this page") };
        assert_eq!(
            page_next(&active, facts(1, false), unread, unread),
            PageNext::WaitCredit
        );
        let bounded = PageNext::Prepare {
            bounded_prefix: true,
        };
        assert_eq!(page_next(&active, facts(0, true), unread, unread), bounded);
        assert_eq!(
            page_next(&active, facts(0, false), || true, unread),
            bounded
        );
        assert_eq!(
            page_next(&active, facts(0, false), || false, || true),
            bounded
        );
        assert_eq!(
            page_next(&active, facts(0, false), || false, || false),
            PageNext::Continue { staged_tip }
        );
    }
}
