//! The tap's half of `header-sync/outstanding-work-bounded` (hdr-9d.1): the
//! reactor's growable containers, as its [`ReactorEvent::Sizes`] reports
//! them at the end of each unit of work, stay within their bounds, and
//! every key of a map kept for a peer or a call names one that still has
//! what the entry is for.
//!
//! The bounds restate production's constants (Rule 6): the queue's 16
//! slots ([`MAX_STAGED_TARGETS`]), the 16 locator permits, the 4,000-header
//! chunk budget, and the admitted peer limit the harness configures. The
//! membership rules join the reactor's keys to its own facts, never to
//! another of its containers (Rule A):
//!
//! - Request deadlines and staged-capacity leases: every key holds an
//!   active slot, as the queue's last `Slots` lists it. Each peer's lease
//!   count is the number of `PageStaged` facts with headers on that slot's
//!   round, so a lease kept twice or past its round breaks.
//! - Request reservations: each is open by its `ReservationOpened` with no
//!   `ReservationEnded`, for its peer, and backs that peer's active round,
//!   by the round its opening or its `ReservationBound` names, so one kept
//!   past its round breaks even when the peer's next round reuses it.
//! - Work slots: every active slot's round has a `Started` and no
//!   `RoundReleased`, and its peer is admitted with the slot's session,
//!   except a repair's preparing or applying round, which a disconnect or
//!   replacement leaves to finish its local operation. Every unstarted
//!   slot's peer is admitted with the slot's session and has an open
//!   locator read, which is all an unstarted slot waits for: every other
//!   wait removes it.
//! - Staged entries: a receiving round holds exactly the headers its
//!   `PageStaged` facts carried, and a preparing or applying one holds
//!   none, since preparation takes them.
//! - Repair episodes: each one's owner is the owner its `Started` gave an
//!   active repair slot's round.
//! - Close deadlines and pending closes: every key is a peer admitted with
//!   the session the entry closes, by `SessionAdmitted` and
//!   `SessionEnded`, so an entry kept across a replacement breaks.
//! - Cached offers and the VCT supplier order: every key is a peer with an
//!   admitted session. The supplier order holds each peer at most once.
//! - Departed strikes and readmission cooldowns: retention only. Both maps
//!   stay within the admitted peer limit, restated here. Every strike entry
//!   holds at least one strike, for a peer with no admitted session. A
//!   maintenance unit ends with no entry past its own stored deadline. Stored
//!   counts and deadlines are production's values, read as they are. This
//!   property doesn't judge whether a count is what the session earned,
//!   whether a hold or cooldown has the configured length, whether a cooldown
//!   was owed, which entry the cap evicts, or whether admission resumes the
//!   right strikes. Those are penalty rules (Rule 5).
//!   `header-fault-penalized` owns a header fault's cooldown, in the wire
//!   harness. `bounded-adversary-progress` owns a pause that never lifts
//!   (hdr-13). An unproductive peer's strikes, hold and cooldown have no
//!   owner; see Open coverage.
//! - Session handles and scheduling state: exactly the admitted peers.
//! - Locator reads: every entry names a `Dispatched` locator call for its
//!   peer with no `CallEnded`.
//! - Storage calls: every key is a call with a `Dispatched` and no
//!   `CallEnded`, and every pending prepare or apply holds headers by a
//!   `HeadersHeld` with no `HeadersEnded`. The reactor polls exactly one
//!   port future per pending call (every direct call is accepted into
//!   `StorageCalls` and pushed as a future together, dispatch.rs:555-558,
//!   and settled before any unit ends). Open calls are bounded by kind:
//!   locator reads by the 16 permits, and prepares and applies by the
//!   headers they hold, which together stay within the 4,000-header chunk
//!   budget, so at most 4,000 of them.
//! - Completed targets: every key has the adopted generation and the
//!   adopted finalized anchor, is one a `TargetCompleted` named, and has a
//!   tip the node's engine holds, by the storage owner's own state. Both
//!   ways to complete a target, an admitted apply and a locator that
//!   already held it, leave its tip held, so the set is bounded by the
//!   engine's headers above the anchor, and a check that took an unknown
//!   target as known breaks.
//!
//! Three rows are listed and not checked: the pruned-peer map, whose cap
//! waits on its fix (#2209), and the VCT repair requirement's source
//! entries and its open context reads, since no fixture reaches a repair
//! episode (#2180's repair harness on main may now reach one). Cooldowns
//! are capped by #2131.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use zakura_chain::block;
use zakura_header_chain::{BranchId, Frontier, HeaderGeneration, HeaderSyncWorkOwner};
use zakura_network::zakura::{
    observe::{
        HeaderTargetPhase, PortCall, ReactorCause, ReactorEvent, ReactorSizes, StorageCallKind,
        WorkSlot,
    },
    ZakuraPeerId,
};

/// The queue's slot limit, `MAX_STAGED_TARGETS_V1`.
pub const MAX_STAGED_TARGETS: usize = 16;

/// The reactor's locator permits.
pub const LOCATOR_PERMITS: usize = 16;

/// The header chunk budget's capacity.
pub const CHUNK_HEADERS: usize = 4_000;

/// The peers the reactor admits at once, as production derives it from
/// `max_inbound_peers + max_outbound_peers`, capped by the node's
/// connection limit, and never below 8 (`MIN_PEER_IDENTITY_STATE`).
pub fn admitted_peer_limit(max_inbound: usize, max_outbound: usize, connections: usize) -> usize {
    max_inbound
        .saturating_add(max_outbound)
        .min(connections)
        .max(8)
}

/// Each row's largest size so far, shared by the tap and whoever reads it.
/// Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct OutstandingEvidence(Arc<Mutex<Store>>);

#[derive(Debug, Default)]
struct Store {
    /// Each row's peak, by name.
    peaks: BTreeMap<&'static str, usize>,
    /// The first step whose drain judged a `Sizes`.
    applicable: Option<u64>,
    /// Whether a `Sizes` was judged since the last drain.
    pending: bool,
}

impl OutstandingEvidence {
    /// Runs `change` on the store. Nothing else is called while the lock
    /// is held.
    fn with<R>(&self, change: impl FnOnce(&mut Store) -> R) -> R {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Each row's largest size so far, by name.
    pub fn peaks(&self) -> BTreeMap<&'static str, usize> {
        self.with(|store| store.peaks.clone())
    }

    /// Whether a drain up to and including `step`'s judged a `Sizes`.
    pub fn applicable_by(&self, step: u64) -> bool {
        self.with(|store| store.applicable.is_some_and(|found| found <= step))
    }

    /// Whether any drain judged a `Sizes`.
    pub fn any(&self) -> bool {
        self.with(|store| store.applicable.is_some())
    }
}

/// Reads the hashes of every header the node's engine holds, or `None` once
/// the engine is gone. The rule calls it only from [`Outstanding::drain`],
/// on the driver between steps, since the engine's lock is the simulator's
/// and a task that took it while the tap holds this rule's lock could be
/// parked there, and only for completed targets whose tips it hasn't seen
/// held yet.
pub(super) struct HeldHeaders(Box<dyn Fn() -> Option<HashSet<block::Hash>> + Send>);

impl HeldHeaders {
    /// Reads the held headers with `read`.
    pub(super) fn new(read: impl Fn() -> Option<HashSet<block::Hash>> + Send + 'static) -> Self {
        HeldHeaders(Box::new(read))
    }
}

impl std::fmt::Debug for HeldHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeldHeaders")
    }
}

/// A dispatched call that hasn't ended: what it asks for, and for which
/// peer when it's a locator read.
#[derive(Clone, Debug)]
enum Open {
    Locator(ZakuraPeerId),
    Prepare,
    Apply,
    Other,
}

/// The rule's state: what the reactor's facts say it should hold, and the
/// breaks not yet drained.
#[derive(Debug)]
pub(super) struct Outstanding {
    evidence: OutstandingEvidence,
    /// The admitted peer limit the harness configured.
    peer_limit: usize,
    /// Each admitted peer's current session.
    admitted: HashMap<ZakuraPeerId, u64>,
    /// Each dispatched call with no `CallEnded`.
    calls: HashMap<u64, Open>,
    /// Each call holding headers by a `HeadersHeld` with no `HeadersEnded`,
    /// with how many.
    holding: HashMap<u64, usize>,
    /// How many `PageStaged` facts with headers each round has, and the
    /// headers they carried, by peer and incarnation.
    pages: HashMap<(ZakuraPeerId, u64), (usize, usize)>,
    /// Every completion a `TargetCompleted` named.
    completed: HashSet<(HeaderGeneration, BranchId)>,
    /// Reads the engine's held headers.
    held: HeldHeaders,
    /// The completed targets' tips seen held. Nothing removes a held
    /// header in any fixture, so a tip seen held stays held.
    held_tips: HashSet<block::Hash>,
    /// Completed targets' tips a `Sizes` showed that aren't yet seen held,
    /// for the next drain to look up.
    unseen_tips: HashSet<block::Hash>,
    /// The unit of work the reactor is in, by its last `CauseSet`.
    cause: Option<ReactorCause>,
    /// The owner each started round's `Started` gave it, by peer and
    /// incarnation, until its `RoundReleased`.
    rounds: HashMap<(ZakuraPeerId, u64), HeaderSyncWorkOwner>,
    /// Each open reservation's peer and the round it backs, once known.
    reserved: HashMap<u64, (ZakuraPeerId, Option<u64>)>,
    /// The queue's slots, as its last `Slots` listed them.
    slots: Vec<WorkSlot>,
    /// The adopted generation and finalized anchor.
    adopted: Option<(HeaderGeneration, Frontier)>,
    breaks: Vec<String>,
}

impl Outstanding {
    /// The rule for a node whose admitted peer limit is `peer_limit`,
    /// keeping its peaks in `evidence` and reading its engine's headers
    /// with `held`.
    pub(super) fn new(evidence: OutstandingEvidence, peer_limit: usize, held: HeldHeaders) -> Self {
        Outstanding {
            evidence,
            peer_limit,
            admitted: HashMap::new(),
            calls: HashMap::new(),
            holding: HashMap::new(),
            pages: HashMap::new(),
            completed: HashSet::new(),
            held,
            held_tips: HashSet::new(),
            unseen_tips: HashSet::new(),
            rounds: HashMap::new(),
            cause: None,
            reserved: HashMap::new(),
            slots: Vec::new(),
            adopted: None,
            breaks: Vec::new(),
        }
    }

    /// Notes the reactor's `event`, judging a `Slots` list's length and
    /// every `Sizes`.
    pub(super) fn observe(&mut self, event: &ReactorEvent) {
        match event {
            ReactorEvent::SessionAdmitted {
                peer, session_id, ..
            } => {
                self.admitted.insert(peer.clone(), *session_id);
            }
            ReactorEvent::SessionEnded {
                peer, session_id, ..
            } => {
                if self.admitted.get(peer) == Some(session_id) {
                    self.admitted.remove(peer);
                }
            }
            ReactorEvent::Dispatched { call, op } => {
                let open = match op {
                    PortCall::Locator { peer, .. } => Open::Locator(peer.clone()),
                    PortCall::Prepare { .. } => Open::Prepare,
                    PortCall::Apply { .. } => Open::Apply,
                    _ => Open::Other,
                };
                self.calls.insert(*call, open);
            }
            ReactorEvent::CallEnded { call, .. } => {
                self.calls.remove(call);
            }
            ReactorEvent::Started { request, .. } => {
                self.rounds
                    .insert((request.peer.clone(), request.incarnation), request.owner);
            }
            ReactorEvent::CauseSet { cause } => self.cause = Some(*cause),
            ReactorEvent::TargetCompleted {
                header_generation,
                branch,
                ..
            } => {
                self.completed.insert((*header_generation, *branch));
            }
            ReactorEvent::HeadersHeld {
                call,
                kind,
                headers,
                ..
            } => {
                if matches!(kind, StorageCallKind::Prepare | StorageCallKind::Apply) {
                    self.holding.insert(*call, *headers);
                }
            }
            ReactorEvent::PageStaged {
                request, headers, ..
            } => {
                if *headers > 0 {
                    let (pages, staged) = self
                        .pages
                        .entry((request.peer.clone(), request.incarnation))
                        .or_default();
                    *pages += 1;
                    *staged += headers;
                }
            }
            ReactorEvent::ReservationOpened {
                reservation,
                peer,
                request,
                ..
            } => {
                self.reserved.insert(
                    *reservation,
                    (
                        peer.clone(),
                        request.as_ref().map(|round| round.incarnation),
                    ),
                );
            }
            ReactorEvent::ReservationBound {
                reservation,
                request,
                ..
            } => {
                if let Some((_, round)) = self.reserved.get_mut(reservation) {
                    *round = Some(request.incarnation);
                }
            }
            ReactorEvent::ReservationEnded { reservation, .. } => {
                self.reserved.remove(reservation);
            }
            ReactorEvent::RoundReleased {
                peer, incarnation, ..
            } => {
                self.pages.remove(&(peer.clone(), *incarnation));
                self.rounds.remove(&(peer.clone(), *incarnation));
            }
            ReactorEvent::HeadersEnded { call, .. } => {
                self.holding.remove(call);
            }
            ReactorEvent::SnapshotAdopted {
                header_generation,
                finalized,
                ..
            } => self.adopted = Some((*header_generation, *finalized)),
            ReactorEvent::Slots { slots } => {
                self.slots = slots.clone();
                self.peak("work slots", slots.len());
                if slots.len() > MAX_STAGED_TARGETS {
                    self.breaks.push(format!(
                        "OutstandingWork: the queue holds {} slots, past its {MAX_STAGED_TARGETS}",
                        slots.len()
                    ));
                }
            }
            ReactorEvent::Sizes { sizes } => self.judge(sizes),
            _ => {}
        }
    }

    /// Notes `size` as `row`'s size now.
    fn peak(&self, row: &'static str, size: usize) {
        self.evidence.with(|store| {
            let peak = store.peaks.entry(row).or_default();
            *peak = (*peak).max(size);
        });
    }

    /// Judges one `Sizes` report against every row.
    fn judge(&mut self, sizes: &ReactorSizes) {
        let ReactorSizes {
            sessions,
            scheduled,
            cached_offers,
            reservations,
            staged_capacity,
            staged_entries,
            repair_episodes,
            request_deadlines,
            close_deadlines,
            pending_closes,
            locator_reads,
            storage_calls,
            port_futures,
            supplier_order,
            departed_strikes,
            cooldowns,
            // Listed, not checked: its cap waits on its fix.
            pruned_peers: _,
            completed_targets,
            // Listed, not checked: no fixture reaches a repair episode.
            repair_sources: _,
        } = sizes;
        self.evidence.with(|store| store.pending = true);
        let limit = self.peer_limit;
        let mut found = Vec::new();
        let bounds = [
            ("session handles", sessions.len(), limit),
            ("scheduling state", scheduled.len(), limit),
            ("cached offers", cached_offers.len(), limit),
            ("reservations", reservations.len(), MAX_STAGED_TARGETS),
            ("staged capacity", staged_capacity.len(), MAX_STAGED_TARGETS),
            (
                "staged entries",
                staged_entries
                    .iter()
                    .fold(0, |total, (_, headers)| total.saturating_add(*headers)),
                CHUNK_HEADERS,
            ),
            ("repair episodes", repair_episodes.len(), MAX_STAGED_TARGETS),
            (
                "request deadlines",
                request_deadlines.len(),
                MAX_STAGED_TARGETS,
            ),
            ("close deadlines", close_deadlines.len(), limit),
            ("pending closes", pending_closes.len(), limit),
            ("locator reads", locator_reads.len(), LOCATOR_PERMITS),
            ("supplier order", supplier_order.len(), limit),
            ("departed strikes", departed_strikes.len(), limit),
            ("cooldowns", cooldowns.len(), limit),
        ];
        for (row, size, bound) in bounds {
            self.peak(row, size);
            if size > bound {
                found.push(format!("holds {size} {row}, past its {bound}"));
            }
        }
        self.peak("storage calls", storage_calls.len());
        self.peak("port futures", *port_futures);
        self.peak("completed targets", completed_targets.len());

        let admitted = |peer: &ZakuraPeerId| self.admitted.contains_key(peer);
        let mut expected: Vec<&ZakuraPeerId> = self.admitted.keys().collect();
        expected.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        for (row, peers) in [
            ("session handles", sessions),
            ("scheduling state", scheduled),
        ] {
            if peers.iter().collect::<Vec<_>>() != expected {
                found.push(format!(
                    "holds {row} for {peers:?}, but the admitted peers are {expected:?}"
                ));
            }
        }
        for (row, closes) in [
            ("close deadlines", close_deadlines),
            ("pending closes", pending_closes),
        ] {
            let stray: Vec<&(ZakuraPeerId, u64)> = closes
                .iter()
                .filter(|(peer, session_id)| self.admitted.get(peer) != Some(session_id))
                .collect();
            if !stray.is_empty() {
                found.push(format!(
                    "holds {row} for {stray:?}, which aren't admitted sessions"
                ));
            }
        }
        for slot in &self.slots {
            if let Some(phase) = slot.phase {
                if !self
                    .rounds
                    .contains_key(&(slot.peer.clone(), slot.incarnation))
                {
                    found.push(format!(
                        "holds an active slot for {:?} round {}, which isn't a started, \
                         unreleased round",
                        slot.peer, slot.incarnation
                    ));
                }
                let local_repair = slot.repair
                    && matches!(
                        phase,
                        HeaderTargetPhase::Preparing | HeaderTargetPhase::Applying
                    );
                if !local_repair && self.admitted.get(&slot.peer) != Some(&slot.session_id) {
                    found.push(format!(
                        "holds a {phase:?} slot for {:?} session {}, which isn't admitted \
                         and isn't a repair's local operation",
                        slot.peer, slot.session_id
                    ));
                }
                continue;
            }
            if self.admitted.get(&slot.peer) != Some(&slot.session_id) {
                found.push(format!(
                    "holds an unstarted slot for {:?} session {}, which isn't admitted",
                    slot.peer, slot.session_id
                ));
            }
            let reading = self
                .calls
                .values()
                .any(|open| matches!(open, Open::Locator(asked) if *asked == slot.peer));
            if !reading {
                found.push(format!(
                    "holds an unstarted slot for {:?}, which has no open locator read",
                    slot.peer
                ));
            }
        }
        let departed: Vec<&ZakuraPeerId> = departed_strikes
            .iter()
            .map(|(peer, _, _)| peer)
            .filter(|peer| self.admitted.contains_key(*peer))
            .collect();
        if !departed.is_empty() {
            found.push(format!(
                "holds departed strikes for {departed:?}, which have an admitted session"
            ));
        }
        let strikeless: Vec<&ZakuraPeerId> = departed_strikes
            .iter()
            .filter(|(_, strikes, _)| *strikes == 0)
            .map(|(peer, _, _)| peer)
            .collect();
        if !strikeless.is_empty() {
            found.push(format!(
                "holds departed strikes for {strikeless:?} with no strikes"
            ));
        }
        if self.cause == Some(ReactorCause::Maintenance) {
            let strikes: Vec<(ZakuraPeerId, std::time::Duration)> = departed_strikes
                .iter()
                .map(|(peer, _, left)| (peer.clone(), *left))
                .collect();
            for (row, entries) in [("departed strikes", &strikes), ("cooldowns", cooldowns)] {
                let expired: Vec<&ZakuraPeerId> = entries
                    .iter()
                    .filter(|(_, left)| left.is_zero())
                    .map(|(peer, _)| peer)
                    .collect();
                if !expired.is_empty() {
                    found.push(format!(
                        "ends maintenance holding expired {row} for {expired:?}"
                    ));
                }
            }
        }
        for (row, peers) in [
            ("cached offers", cached_offers),
            ("supplier order", supplier_order),
        ] {
            let stray: Vec<&ZakuraPeerId> = peers.iter().filter(|peer| !admitted(peer)).collect();
            if !stray.is_empty() {
                found.push(format!(
                    "holds {row} for {stray:?}, which have no admitted session"
                ));
            }
        }
        let distinct: HashSet<&ZakuraPeerId> = supplier_order.iter().collect();
        if distinct.len() != supplier_order.len() {
            found.push(format!(
                "holds the supplier order {supplier_order:?}, with a peer more than once"
            ));
        }
        let active: HashMap<&ZakuraPeerId, u64> = self
            .slots
            .iter()
            .filter(|slot| slot.phase.is_some())
            .map(|slot| (&slot.peer, slot.incarnation))
            .collect();
        let leased: Vec<&ZakuraPeerId> = staged_capacity.iter().map(|(peer, _)| peer).collect();
        for (row, peers) in [
            (
                "request deadlines",
                request_deadlines.iter().collect::<Vec<_>>(),
            ),
            ("staged capacity", leased),
        ] {
            let stray: Vec<&ZakuraPeerId> = peers
                .into_iter()
                .filter(|peer| !active.contains_key(peer))
                .collect();
            if !stray.is_empty() {
                found.push(format!(
                    "holds {row} for {stray:?}, which hold no active slot"
                ));
            }
        }
        let phases: HashMap<&ZakuraPeerId, (u64, HeaderTargetPhase)> = self
            .slots
            .iter()
            .filter_map(|slot| Some((&slot.peer, (slot.incarnation, slot.phase?))))
            .collect();
        for (peer, headers) in staged_entries {
            let expected = match phases.get(peer) {
                Some((incarnation, HeaderTargetPhase::Receiving)) => self
                    .pages
                    .get(&(peer.clone(), *incarnation))
                    .map_or(0, |(_, staged)| *staged),
                Some(_) => 0,
                None => {
                    found.push(format!(
                        "holds {headers} staged entries for {peer:?}, which holds no active slot"
                    ));
                    continue;
                }
            };
            if *headers != expected {
                found.push(format!(
                    "holds {headers} staged entries for {peer:?}, whose round should hold \
                     {expected}"
                ));
            }
        }
        for (peer, leases) in staged_capacity {
            let Some(incarnation) = active.get(peer) else {
                continue;
            };
            let (staged, _) = self
                .pages
                .get(&(peer.clone(), *incarnation))
                .copied()
                .unwrap_or_default();
            if *leases != staged {
                found.push(format!(
                    "holds {leases} leases for {peer:?}, whose round {incarnation} staged \
                     {staged} pages"
                ));
            }
        }
        for (peer, reservation) in reservations {
            let backs = reservation.and_then(|id| self.reserved.get(&id));
            let current = active.get(peer).copied();
            if !matches!(backs, Some((owner, Some(round))) if owner == peer && Some(*round) == current)
            {
                found.push(format!(
                    "holds reservation {reservation:?} for {peer:?}, which backs {backs:?}, \
                     not its active round {current:?}"
                ));
            }
        }
        let repairs: HashSet<HeaderSyncWorkOwner> = self
            .slots
            .iter()
            .filter(|slot| slot.phase.is_some() && slot.repair)
            .filter_map(|slot| {
                self.rounds
                    .get(&(slot.peer.clone(), slot.incarnation))
                    .copied()
            })
            .collect();
        let stray: Vec<&HeaderSyncWorkOwner> = repair_episodes
            .iter()
            .filter(|owner| !repairs.contains(owner))
            .collect();
        if !stray.is_empty() {
            found.push(format!(
                "holds repair episodes for {stray:?}, which own no active repair round"
            ));
        }
        let stray: Vec<&(ZakuraPeerId, u64)> = locator_reads
            .iter()
            .filter(|(peer, call)| {
                !matches!(self.calls.get(call), Some(Open::Locator(asked)) if asked == peer)
            })
            .collect();
        if !stray.is_empty() {
            found.push(format!(
                "holds locator reads {stray:?} with no pending locator call for their peer"
            ));
        }
        let ended: Vec<u64> = storage_calls
            .iter()
            .filter(|call| !self.calls.contains_key(call))
            .copied()
            .collect();
        if !ended.is_empty() {
            found.push(format!(
                "holds storage calls {ended:?}, which ended or never dispatched"
            ));
        }
        let unheld: Vec<u64> = storage_calls
            .iter()
            .filter(|call| {
                matches!(self.calls.get(call), Some(Open::Prepare | Open::Apply))
                    && !self.holding.get(call).is_some_and(|headers| *headers > 0)
            })
            .copied()
            .collect();
        if !unheld.is_empty() {
            found.push(format!(
                "holds prepare or apply calls {unheld:?} that hold no headers"
            ));
        }
        let locators = storage_calls
            .iter()
            .filter(|call| matches!(self.calls.get(call), Some(Open::Locator(_))))
            .count();
        if locators > LOCATOR_PERMITS {
            found.push(format!(
                "holds {locators} open locator calls, past its {LOCATOR_PERMITS} permits"
            ));
        }
        let held: usize = storage_calls
            .iter()
            .filter_map(|call| self.holding.get(call))
            .fold(0, |total, headers| total.saturating_add(*headers));
        if held > CHUNK_HEADERS {
            found.push(format!(
                "holds prepare and apply calls holding {held} headers, past its {CHUNK_HEADERS}"
            ));
        }
        if *port_futures != storage_calls.len() {
            found.push(format!(
                "polls {port_futures} port futures for {} pending calls",
                storage_calls.len()
            ));
        }
        let unnamed: Vec<_> = completed_targets
            .iter()
            .filter(|key| !self.completed.contains(key))
            .collect();
        if !unnamed.is_empty() {
            found.push(format!(
                "holds completed targets {unnamed:?} that no TargetCompleted named"
            ));
        }
        self.unseen_tips.extend(
            completed_targets
                .iter()
                .map(|(_, branch)| branch.target_tip_hash)
                .filter(|tip| !self.held_tips.contains(tip)),
        );
        if let Some((generation, finalized)) = self.adopted {
            let stale: Vec<_> = completed_targets
                .iter()
                .filter(|(held, branch)| {
                    *held != generation || branch.anchor_hash != finalized.hash
                })
                .collect();
            if !stale.is_empty() {
                found.push(format!(
                    "holds completed targets {stale:?} outside the adopted generation \
                     {generation:?} and anchor {finalized:?}"
                ));
            }
        }
        for detail in found {
            self.breaks
                .push(format!("OutstandingWork: the reactor {detail}"));
        }
    }

    /// The breaks found since the last call, after looking up the
    /// completed targets' tips the `Sizes` since showed. Marks the rule
    /// applicable at `step` if a `Sizes` was judged since the last call.
    pub(super) fn drain(&mut self, step: u64) -> Vec<String> {
        if !self.unseen_tips.is_empty() {
            let unseen = std::mem::take(&mut self.unseen_tips);
            if let Some(held) = (self.held.0)() {
                for tip in unseen {
                    if held.contains(&tip) {
                        self.held_tips.insert(tip);
                    } else {
                        self.breaks.push(format!(
                            "OutstandingWork: the reactor holds a completed target for {tip:?}, \
                             which the node's engine doesn't hold"
                        ));
                    }
                }
            }
        }
        self.evidence.with(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
        });
        std::mem::take(&mut self.breaks)
    }
}

#[cfg(test)]
mod tests;
