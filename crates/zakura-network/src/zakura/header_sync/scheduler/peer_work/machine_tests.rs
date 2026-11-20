//! The `header/work-ownership` machine target: interleaved work decisions on
//! the production [`PeerWorkQueue`], with its real budget and leases.
//!
//! [`Harness`] plays the reactor. It records which peer holds which work, which
//! reservations and leases it handed out, and which calls hold them, from the
//! inputs it chose and the queue's answers. After every input it checks
//! [`PeerWorkQueue::assert_invariants`] and compares the queue's counts and
//! rounds with that record. It never decides what the queue admits, evicts,
//! or decides for a page or an offer: [`decide_offer`] and [`page_next`] decide
//! those, and named scenarios pin their rules. Steps for different peers and
//! for calls that outlive their round interleave freely. At the end, the
//! history ends every call and retires every peer, and the budget must have
//! every header back.

use std::collections::{BTreeMap, BTreeSet};

use zakura_chain::block::{self, genesis::regtest_genesis_block};
use zakura_header_chain::{Frontier, HeaderLocator, HeaderWorkOwner, MAX_STAGED_TARGETS_V1};

use super::{
    decide_offer,
    fixtures::{self, hash, snapshot},
    page_next, ActiveHeaderRequest, AdvertisedHeaderTarget, HeaderTargetPhase, HeaderTargetPurpose,
    OfferDecision, OfferFacts, OfferedPeerWork, PageFacts, PageNext, PeerWorkPriority,
    PeerWorkQueue, QueueWorkResult, RetainedHeaders, SpentOffer, StorageCall, StorageCallKind,
    MAX_HEADER_CHUNK_RESERVATION_V1,
};
use crate::zakura::{
    header_sync::{
        events::HeaderPortOperation, v9::Cursor, wire::AuxSchema, HeaderEntry, HeaderSyncRequestId,
    },
    machine_test::{machine_target_tests, MAX_HISTORY_BYTES},
    ZakuraPeerId,
};

const PROPERTY: &str = "header/work-ownership";

/// A page from `peer` on session 1, subscription 1, with no transport frame.
/// The queue under test has no observer, so nothing reads it.
fn arrival(peer: &ZakuraPeerId) -> crate::zakura::header_sync::observe::PageArrival {
    crate::zakura::header_sync::observe::PageArrival {
        from_peer: peer.clone(),
        from_session: 1,
        subscription: 1,
        frame: None,
    }
}

/// One more peer than the queue has target slots, so the queue can fill.
const PEERS: usize = MAX_STAGED_TARGETS_V1 + 1;

/// Targets that peers can offer.
const TIPS: u8 = 6;

/// One decision the reactor can make.
#[derive(Clone, Debug)]
enum Op {
    /// Stage `tip` for `peer` at `priority`.
    Stage { peer: usize, tip: u8, priority: u8 },
    /// Stage `tip` for `peer` unless another peer owns it.
    StageDistinct { peer: usize, tip: u8, priority: u8 },
    /// Reserve `count` headers for `peer`.
    Reserve { peer: usize, count: u32 },
    /// Release `peer`'s reservation.
    Cancel { peer: usize },
    /// Start `peer`'s request for `tip`.
    Start { peer: usize, tip: u8 },
    /// Deliver a page of `returned` headers for `peer`. The page ends at the
    /// target when `reaches_target` holds.
    Page {
        peer: usize,
        returned: usize,
        reaches_target: bool,
    },
    /// Dispatch a prepare for `peer`'s current round, for a capacity-bounded
    /// prefix of its target when `bounded_prefix` holds.
    Prepare { peer: usize, bounded_prefix: bool },
    /// Complete the `hold`th live call. A current prepare moves its headers to an
    /// apply when `apply` holds. Every other completion ends its ownership.
    Complete { hold: usize, apply: bool },
    /// Retire `peer`'s work, as a timeout or disconnect does.
    Retire { peer: usize },
    /// A committed snapshot at header generation `generation` retires what it obsoletes.
    Commit { generation: u8 },
    /// `peer` offers `tip`, with the blockers that `blockers` names.
    Offer {
        peer: usize,
        tip: u8,
        priority: u8,
        blockers: OfferBlockers,
    },
}

/// The facts of one offer that the queue does not decide.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct OfferBlockers {
    pruned_pause: bool,
    covered: bool,
    eligible: bool,
    held_back: bool,
    subscription_ready: bool,
}

impl OfferBlockers {
    fn from_bits(bits: u8) -> Self {
        Self {
            pruned_pause: bits & 1 != 0,
            covered: bits & 2 != 0,
            eligible: bits & 4 == 0,
            held_back: bits & 8 != 0,
            subscription_ready: bits & 16 == 0,
        }
    }
}

/// One peer's work, as the queue's answers showed it.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Work {
    Awaiting {
        tip: u8,
        priority: u8,
        generation: u8,
    },
    Active {
        tip: u8,
        incarnation: u64,
        phase: HeaderTargetPhase,
        /// The number of the call this round waits for.
        call: Option<u64>,
        /// Headers staged and not yet handed to a prepare.
        staged: usize,
    },
}

impl Work {
    fn tip(&self) -> u8 {
        match self {
            Self::Awaiting { tip, .. } | Self::Active { tip, .. } => *tip,
        }
    }

    /// What the scheduling revision covers: the target, and the round once it
    /// started.
    fn ownership(&self) -> (u8, Option<u64>) {
        match self {
            Self::Awaiting { tip, .. } => (*tip, None),
            Self::Active {
                tip, incarnation, ..
            } => (*tip, Some(*incarnation)),
        }
    }
}

/// One owned lease the queue handed out.
#[derive(Debug)]
struct Lease {
    count: usize,
    /// The peer whose queue slot holds a clone.
    queued_for: Option<usize>,
    /// The live calls holding a clone.
    holders: BTreeSet<u64>,
}

impl Lease {
    fn alive(&self) -> bool {
        self.queued_for.is_some() || !self.holders.is_empty()
    }
}

/// One live call's ownership, with the real owner that keeps its leases alive.
#[derive(Debug)]
struct Hold {
    /// The real owner, which keeps real leases alive.
    retained: RetainedHeaders,
    /// Identifies this hold across a handoff.
    id: u64,
    peer: usize,
    incarnation: u64,
    call: u64,
    kind: StorageCallKind,
    headers: usize,
}

/// Transitions one history reached.
#[derive(Debug, Default, Eq, PartialEq)]
struct HeaderWorkReplaySummary {
    /// Operations decoded.
    transitions: usize,
    /// Rounds started.
    starts: usize,
    /// Prepares dispatched with their headers.
    prepares: usize,
    /// Headers moved from a prepare to its apply.
    handoffs: usize,
    /// Completions that matched no current round and call.
    stale_completions: usize,
    /// Unstarted work replaced to make room.
    evictions: usize,
    /// Headers that stayed charged to a call after their round retired.
    retained_after_retirement: usize,
    /// Pages that decided to wait for the rest of their round's credit.
    credit_waits: usize,
    /// Pages that decided to grant more credit.
    continuations: usize,
    /// Pages that decided to validate the staged headers.
    prepare_decisions: usize,
    /// Pages that decided their peer's pages cannot form the target.
    malformed_pages: usize,
    /// Offers that the decision staged.
    offers_staged: usize,
    /// Offers that the decision spent.
    offers_spent: usize,
}

struct Harness {
    queue: PeerWorkQueue,
    work: BTreeMap<usize, Work>,
    reservations: BTreeMap<usize, usize>,
    leases: Vec<Lease>,
    holds: Vec<Hold>,
    last_call: u64,
    last_hold: u64,
    summary: HeaderWorkReplaySummary,
}

fn peer_id(peer: usize) -> ZakuraPeerId {
    fixtures::peer(u8::try_from(peer).expect("the test uses few peers") + 1)
}

thread_local! {
    /// One real header entry, and its hash.
    static HEADER: (HeaderEntry, block::Hash) = {
        let header = regtest_genesis_block().header.clone();
        let hash = header.hash();
        (
            HeaderEntry {
                header,
                body_size: 0,
                tree_aux: None,
            },
            hash,
        )
    };
}

/// The selected tip hash of target `tip`. Every staged header is the same real
/// header, and it is the tip of target 0, so a page can reach only that target.
fn tip_hash(tip: u8) -> block::Hash {
    if tip == 0 {
        HEADER.with(|(_, hash)| *hash)
    } else {
        hash(tip)
    }
}

/// The ancestor that the first page of every round names.
fn ancestor() -> Frontier {
    Frontier::new(block::Height(10), hash(10))
}

/// The admitted page of `returned` headers.
fn page(returned: usize) -> Vec<HeaderEntry> {
    HEADER.with(|(entry, _)| vec![entry.clone(); returned])
}

/// The frontier of the `staged`th header after [`ancestor`].
fn staged_frontier(staged: usize) -> Frontier {
    let offset = u32::try_from(staged).expect("the test staging count fits u32");
    Frontier::new(block::Height(10 + offset), tip_hash(0))
}

fn priority_of(priority: u8) -> PeerWorkPriority {
    if priority & 1 == 0 {
        PeerWorkPriority::LowerComparableWork
    } else {
        PeerWorkPriority::Normal
    }
}

fn target(tip: u8, generation: u8) -> AdvertisedHeaderTarget {
    AdvertisedHeaderTarget {
        scope: zakura_header_chain::HeaderWorkAuthority::for_target(
            &snapshot(generation),
            tip_hash(tip),
        ),
        session_id: 1,
        status: fixtures::status(u32::from(tip) + 11, tip_hash(tip), 1),
    }
}

fn request(peer: usize, tip: u8, generation: u8) -> ActiveHeaderRequest {
    let target = target(tip, generation);
    let request_id = HeaderSyncRequestId::new(1).expect("one is nonzero");
    ActiveHeaderRequest::new(
        HeaderTargetPurpose::Normal,
        peer_id(peer),
        zakura_header_chain::SourceId::from_digest([1; 32]),
        HeaderLocator::for_continuation(ancestor()),
        HeaderWorkOwner {
            authority: target.scope,
            session_id: target.session_id,
            // Every round reuses one request ID, as a resumed subscription does.
            request_id: std::num::NonZeroU64::new(request_id.get()).expect("one is nonzero"),
        }
        .into(),
        target,
        request_id,
        1_000,
        AuxSchema::None,
    )
}

impl Harness {
    fn new() -> Self {
        Self {
            queue: PeerWorkQueue::default(),
            work: BTreeMap::new(),
            reservations: BTreeMap::new(),
            leases: Vec::new(),
            holds: Vec::new(),
            last_call: 0,
            last_hold: 0,
            summary: HeaderWorkReplaySummary::default(),
        }
    }

    /// Release what `peer`'s queue slot holds: its reservation and the queue's
    /// clones of its leases.
    fn release(&mut self, peer: usize) {
        self.reservations.remove(&peer);
        for lease in &mut self.leases {
            if lease.queued_for == Some(peer) {
                lease.queued_for = None;
            }
        }
        self.leases.retain(Lease::alive);
    }

    fn claimed(&self) -> usize {
        self.reservations.values().sum::<usize>()
            + self.leases.iter().map(|lease| lease.count).sum::<usize>()
    }

    /// End `peer`'s work if the queue no longer holds it.
    fn forget_if_gone(&mut self, peer: usize) {
        if self.work.contains_key(&peer) && !self.queue.has_work(&peer_id(peer)) {
            self.work.remove(&peer);
            self.release(peer);
        }
    }

    /// Record the queue's answer to staging `tip` for `peer`.
    ///
    /// The queue decides the result and which unstarted work makes room. This
    /// checks only what any such decision must satisfy: started work keeps its
    /// target, only a full queue refuses or evicts, it evicts at most one peer's
    /// unstarted work, and only work below `priority`. Replacing unstarted work
    /// releases its peer's capacity.
    fn record_stage(&mut self, peer: usize, tip: u8, priority: u8, result: QueueWorkResult) {
        let rank = priority & 1;
        let before = self.work.get(&peer).cloned();
        let full = before.is_none() && self.work.len() >= MAX_STAGED_TARGETS_V1;
        match result {
            QueueWorkResult::AlreadyActive => {
                assert!(
                    matches!(before, Some(Work::Active { .. })),
                    "{PROPERTY}: only started work is already active"
                );
                return;
            }
            QueueWorkResult::AtCapacity => {
                assert!(full, "{PROPERTY}: only a full queue refuses new work");
                return;
            }
            QueueWorkResult::TargetAlreadyAssigned => {
                panic!("{PROPERTY}: plain staging ignores other owners of the target")
            }
            QueueWorkResult::NeedsLocator => assert!(
                !matches!(before, Some(Work::Active { .. })),
                "{PROPERTY}: started work never takes a new target"
            ),
        }
        let evicted: Vec<usize> = self
            .work
            .keys()
            .copied()
            .filter(|other| *other != peer && !self.queue.has_work(&peer_id(*other)))
            .collect();
        assert_eq!(
            evicted.len(),
            usize::from(full),
            "{PROPERTY}: a full queue makes room by evicting exactly one peer's work"
        );
        for other in evicted {
            assert!(
                matches!(
                    self.work.remove(&other),
                    Some(Work::Awaiting { priority, .. }) if priority < rank
                ),
                "{PROPERTY}: only unstarted work below the new priority makes room"
            );
            self.release(other);
            self.summary.evictions += 1;
        }
        if before.is_some() {
            self.release(peer);
        }
        self.work.insert(
            peer,
            Work::Awaiting {
                tip,
                priority: rank,
                generation: 0,
            },
        );
    }

    /// Take the hold that `hold` selects, if any call is live.
    fn take_hold(&mut self, hold: usize) -> Option<Hold> {
        if self.holds.is_empty() {
            return None;
        }
        let index = hold % self.holds.len();
        Some(self.holds.remove(index))
    }

    /// End `hold` and drop its real owner.
    fn end_hold(&mut self, hold: Hold) {
        for lease in &mut self.leases {
            lease.holders.remove(&hold.id);
        }
        self.leases.retain(Lease::alive);
        drop(hold);
    }

    /// Each peer's target and round. The scheduling revision changes exactly
    /// when this does.
    fn ownership(&self) -> BTreeMap<usize, (u8, Option<u64>)> {
        self.work
            .iter()
            .map(|(peer, work)| (*peer, work.ownership()))
            .collect()
    }

    /// Apply `op`, then check the queue against the record, and check that the
    /// scheduling revision changed exactly when some peer's target or round
    /// did.
    fn apply(&mut self, op: Op) {
        let ownership = self.ownership();
        let revision = self.queue.scheduling_revision();
        self.step(op);
        self.check();
        assert_eq!(
            self.queue.scheduling_revision() != revision,
            self.ownership() != ownership,
            "{PROPERTY}: the revision changes exactly when ownership does"
        );
    }

    fn step(&mut self, op: Op) {
        match op {
            Op::Stage {
                peer,
                tip,
                priority,
            } => {
                let result = self
                    .queue
                    .stage(peer_id(peer), target(tip, 0), priority_of(priority));
                self.record_stage(peer, tip, priority, result);
            }
            Op::StageDistinct {
                peer,
                tip,
                priority,
            } => {
                let result = self.queue.stage_distinct_target(
                    peer_id(peer),
                    target(tip, 0),
                    priority_of(priority),
                );
                let owned_elsewhere = self
                    .work
                    .iter()
                    .any(|(owner, work)| *owner != peer && work.tip() == tip);
                assert_eq!(
                    result == QueueWorkResult::TargetAlreadyAssigned,
                    owned_elsewhere,
                    "{PROPERTY}: a target another peer owns is already assigned"
                );
                if owned_elsewhere {
                    self.forget_if_gone(peer);
                } else {
                    self.record_stage(peer, tip, priority, result);
                }
            }
            Op::Reserve { peer, count } => {
                let actual = self.queue.reserve_request(&peer_id(peer), count);
                let count = usize::try_from(count).expect("the test count fits usize");
                let fits = !self.reservations.contains_key(&peer)
                    && count != 0
                    && count <= MAX_HEADER_CHUNK_RESERVATION_V1
                    && count <= super::HEADER_CHUNK_BUDGET_CAPACITY_V1 - self.claimed();
                if fits {
                    self.reservations.insert(peer, count);
                }
                assert_eq!(actual, fits, "{PROPERTY}: reserve");
            }
            Op::Cancel { peer } => {
                self.queue.cancel_request_reservation(&peer_id(peer));
                self.reservations.remove(&peer);
            }
            Op::Start { peer, tip } => {
                let generation = match self.work.get(&peer) {
                    Some(Work::Awaiting { generation, .. }) => *generation,
                    _ => 0,
                };
                let actual = self.queue.start(request(peer, tip, generation));
                let expected = matches!(
                    self.work.get(&peer),
                    Some(Work::Awaiting { tip: awaited, .. }) if *awaited == tip
                ) && self.reservations.contains_key(&peer);
                assert_eq!(actual, expected, "{PROPERTY}: start");
                if expected {
                    let incarnation = self
                        .queue
                        .active(&peer_id(peer))
                        .expect("the round just started")
                        .incarnation()
                        .get();
                    self.summary.starts += 1;
                    self.work.insert(
                        peer,
                        Work::Active {
                            tip,
                            incarnation,
                            phase: HeaderTargetPhase::Receiving,
                            call: None,
                            staged: 0,
                        },
                    );
                }
            }
            Op::Page {
                peer,
                returned,
                reaches_target,
            } => {
                let id = peer_id(peer);
                let actual = self.queue.stage_page(
                    &id,
                    ancestor(),
                    page(returned),
                    Cursor {
                        height: block::Height(0),
                        hash: hash(0),
                        headers: 0,
                        bytes: 0,
                    },
                    arrival(&id),
                );
                let receiving = match self.work.get(&peer) {
                    Some(Work::Active {
                        staged,
                        phase: HeaderTargetPhase::Receiving,
                        ..
                    }) => Some(*staged + returned),
                    _ => None,
                };
                let staged = receiving.filter(|_| {
                    self.reservations
                        .get(&peer)
                        .is_some_and(|reserved| returned <= *reserved)
                });
                assert_eq!(actual, staged.is_some(), "{PROPERTY}: page");
                if let Some(staged) = staged {
                    let reserved = self
                        .reservations
                        .get_mut(&peer)
                        .expect("the reservation was just found");
                    *reserved -= returned;
                    if *reserved == 0 {
                        self.reservations.remove(&peer);
                    }
                    if returned != 0 {
                        self.leases.push(Lease {
                            count: returned,
                            queued_for: Some(peer),
                            holders: BTreeSet::new(),
                        });
                    }
                    if let Some(Work::Active { staged: count, .. }) = self.work.get_mut(&peer) {
                        *count = staged;
                    }
                    let next = page_next(
                        self.queue.active(&id).expect("the page was just staged"),
                        PageFacts {
                            reaches_target,
                            reserved: self.queue.reserved_header_count(&id),
                            checkpoint_prefix_ready: false,
                        },
                        || self.queue.budget_is_full(),
                        || false,
                    );
                    match next {
                        PageNext::WaitCredit => self.summary.credit_waits += 1,
                        PageNext::Continue { .. } => self.summary.continuations += 1,
                        PageNext::Prepare { .. } => self.summary.prepare_decisions += 1,
                        PageNext::Malformed { .. } => self.summary.malformed_pages += 1,
                    }
                }
            }
            Op::Prepare {
                peer,
                bounded_prefix,
            } => {
                let id = peer_id(peer);
                let operation = self.queue.begin_prepare(&id, bounded_prefix);
                let storage_call = StorageCall {
                    number: self.last_call + 1,
                    kind: StorageCallKind::Prepare,
                };
                let retained = self.queue.retain_headers(&id, storage_call);
                let Some(Work::Active {
                    incarnation,
                    phase,
                    call,
                    staged,
                    ..
                }) = self.work.get_mut(&peer)
                else {
                    assert!(operation.is_none(), "{PROPERTY}: only a round prepares");
                    assert!(
                        retained.is_none(),
                        "{PROPERTY}: only a started round retains headers"
                    );
                    return;
                };
                if *phase != HeaderTargetPhase::Receiving || *staged == 0 {
                    assert!(
                        operation.is_none(),
                        "{PROPERTY}: only staged receiving work prepares"
                    );
                    return;
                }
                let Some(HeaderPortOperation::PrepareHeaderTarget {
                    target, entries, ..
                }) = operation
                else {
                    panic!("{PROPERTY}: staged receiving work prepares");
                };
                assert_eq!(
                    target,
                    staged_frontier(*staged),
                    "{PROPERTY}: prepared target"
                );
                assert_eq!(entries.len(), *staged, "{PROPERTY}: prepared headers");
                let incarnation = *incarnation;
                let retained = retained.expect("the queue holds the round the record holds");
                self.last_call += 1;
                self.last_hold += 1;
                *phase = HeaderTargetPhase::Preparing;
                *call = Some(self.last_call);
                *staged = 0;
                self.queue.bind_call(&id, retained.work());
                let mut headers = 0;
                for lease in &mut self.leases {
                    if lease.queued_for == Some(peer) {
                        lease.holders.insert(self.last_hold);
                        headers += lease.count;
                    }
                }
                self.summary.prepares += 1;
                self.holds.push(Hold {
                    retained,
                    id: self.last_hold,
                    peer,
                    incarnation,
                    call: self.last_call,
                    kind: StorageCallKind::Prepare,
                    headers,
                });
            }
            Op::Complete { hold, apply } => {
                let Some(mut hold) = self.take_hold(hold) else {
                    return;
                };
                let work = hold.retained.work();
                let actual = self.queue.settle_call(&peer_id(hold.peer), work);
                let current = matches!(
                    self.work.get(&hold.peer),
                    Some(Work::Active { incarnation, call, .. })
                        if *incarnation == hold.incarnation && *call == Some(hold.call)
                );
                assert_eq!(actual, current, "{PROPERTY}: exact settlement");
                if let Some(Work::Active { call, .. }) = self.work.get_mut(&hold.peer) {
                    if current {
                        *call = None;
                    }
                }
                if !current {
                    self.summary.stale_completions += 1;
                }
                if current && apply && hold.kind == StorageCallKind::Prepare {
                    self.last_call += 1;
                    hold.call = self.last_call;
                    hold.kind = StorageCallKind::Apply;
                    hold.retained.hand_to_apply(StorageCall {
                        number: self.last_call,
                        kind: StorageCallKind::Apply,
                    });
                    self.queue
                        .bind_call(&peer_id(hold.peer), hold.retained.work());
                    assert!(
                        self.queue.begin_apply(&peer_id(hold.peer)),
                        "{PROPERTY}: a current prepare's work is preparing"
                    );
                    if let Some(Work::Active { call, phase, .. }) = self.work.get_mut(&hold.peer) {
                        *call = Some(hold.call);
                        *phase = HeaderTargetPhase::Applying;
                    }
                    self.summary.handoffs += 1;
                    self.holds.push(hold);
                } else {
                    self.end_hold(hold);
                }
            }
            Op::Retire { peer } => {
                let removed = self.queue.remove(&peer_id(peer));
                let existed = self.work.remove(&peer);
                assert_eq!(
                    removed.is_some(),
                    matches!(existed, Some(Work::Active { .. })),
                    "{PROPERTY}: retire"
                );
                self.summary.retained_after_retirement += usize::from(
                    self.holds
                        .iter()
                        .any(|hold| hold.peer == peer && hold.headers != 0),
                );
                self.release(peer);
            }
            Op::Commit { generation } => {
                let current = snapshot(generation);
                let retired = self.queue.retire_obsolete_unstarted(&current);
                let obsolete: Vec<usize> = self
                    .work
                    .iter()
                    .filter_map(|(peer, work)| match work {
                        Work::Awaiting {
                            generation: staged, ..
                        } if *staged != generation => Some(*peer),
                        _ => None,
                    })
                    .collect();
                assert_eq!(retired, obsolete.len(), "{PROPERTY}: unstarted retirement");
                for peer in obsolete {
                    self.work.remove(&peer);
                    self.release(peer);
                }
            }
            Op::Offer {
                peer,
                tip,
                priority,
                blockers,
            } => {
                let work =
                    self.queue
                        .active(&peer_id(peer))
                        .map_or(OfferedPeerWork::Idle, |active| {
                            if active.target.status.selected_tip_hash == tip_hash(tip) {
                                OfferedPeerWork::SameOrdinaryTarget
                            } else {
                                OfferedPeerWork::Other
                            }
                        });
                let decision = decide_offer(
                    OfferFacts {
                        pruned_pause: blockers.pruned_pause,
                        covered: blockers.covered,
                        eligible: blockers.eligible,
                        work,
                    },
                    || blockers.held_back,
                    || blockers.subscription_ready,
                );
                // The reactor ends unstarted work on a spent or held-back
                // offer, and stages the offer's target once nothing blocks it.
                match decision {
                    OfferDecision::Spend { .. } | OfferDecision::HeldBack => {
                        self.summary.offers_spent +=
                            usize::from(matches!(decision, OfferDecision::Spend { .. }));
                        self.queue.remove_unstarted(&peer_id(peer));
                        if matches!(self.work.get(&peer), Some(Work::Awaiting { .. })) {
                            self.work.remove(&peer);
                            self.release(peer);
                        }
                    }
                    OfferDecision::AlreadyActive { .. } | OfferDecision::AwaitSubscription => {}
                    OfferDecision::Stage => {
                        self.summary.offers_staged += 1;
                        self.step(Op::StageDistinct {
                            peer,
                            tip,
                            priority,
                        });
                    }
                }
            }
        }
    }

    /// Check the queue's invariants, and compare its counts and rounds with
    /// the record.
    fn check(&self) {
        let (reserved, owned) = self.queue.chunk_budget_usage();
        assert_eq!(
            reserved,
            self.reservations.values().sum::<usize>(),
            "{PROPERTY}: reserved headers"
        );
        assert_eq!(
            owned,
            self.leases.iter().map(|lease| lease.count).sum::<usize>(),
            "{PROPERTY}: owned headers"
        );
        assert_eq!(
            self.queue.claimed_header_count(),
            self.claimed(),
            "{PROPERTY}: claimed headers"
        );
        assert_eq!(self.queue.len(), self.work.len(), "{PROPERTY}: work count");
        for peer in 0..PEERS {
            let id = peer_id(peer);
            assert_eq!(
                self.queue.reserved_header_count(&id),
                self.reservations.get(&peer).copied().unwrap_or(0),
                "{PROPERTY}: peer {peer} reservation"
            );
            assert_eq!(
                self.queue.owned_header_count(&id),
                self.leases
                    .iter()
                    .filter(|lease| lease.queued_for == Some(peer))
                    .map(|lease| lease.count)
                    .sum::<usize>(),
                "{PROPERTY}: peer {peer} owned headers"
            );
            let actual = self.queue.active(&id).map(|request| {
                (
                    request.incarnation().get(),
                    request.phase,
                    request.target.status.selected_tip_hash,
                )
            });
            let expected = match self.work.get(&peer) {
                Some(Work::Active {
                    incarnation,
                    phase,
                    tip,
                    ..
                }) => Some((*incarnation, *phase, tip_hash(*tip))),
                _ => None,
            };
            assert_eq!(actual, expected, "{PROPERTY}: peer {peer} round");
            assert_eq!(
                self.queue
                    .awaiting_target(&id)
                    .map(|target| target.status.selected_tip_hash),
                match self.work.get(&peer) {
                    Some(Work::Awaiting { tip, .. }) => Some(tip_hash(*tip)),
                    _ => None,
                },
                "{PROPERTY}: peer {peer} awaiting target"
            );
        }
        for tip in 0..TIPS {
            assert_eq!(
                self.queue.is_target_assigned(tip_hash(tip)),
                self.work.values().any(|work| work.tip() == tip),
                "{PROPERTY}: target {tip} ownership"
            );
        }
        for hold in &self.holds {
            assert_eq!(
                hold.retained.headers(),
                hold.headers,
                "{PROPERTY}: call {} headers",
                hold.call
            );
            let work = hold.retained.work();
            assert_eq!(
                (work.incarnation.get(), work.call.number, work.call.kind),
                (hold.incarnation, hold.call, hold.kind),
                "{PROPERTY}: call {} identity",
                hold.call
            );
            assert_eq!(hold.retained.peer(), &peer_id(hold.peer), "{PROPERTY}");
        }
        let phases = self
            .queue
            .policy
            .phase_counts(|peer| self.queue.owned_header_count(peer));
        let mut expected = super::policy::PhaseCounts::default();
        for (peer, work) in &self.work {
            if let Work::Active { phase, .. } = work {
                let count: usize = self
                    .leases
                    .iter()
                    .filter(|lease| lease.queued_for == Some(*peer))
                    .map(|lease| lease.count)
                    .sum();
                match phase {
                    HeaderTargetPhase::Receiving => expected.receiving += count,
                    HeaderTargetPhase::Preparing => expected.preparing += count,
                    HeaderTargetPhase::Applying => expected.applying += count,
                }
            }
        }
        assert_eq!(phases, expected, "{PROPERTY}: phase counts");
        self.queue.assert_invariants();
    }

    /// End every call and retire every peer, as a teardown does, and check
    /// that the budget has every header back.
    fn teardown(&mut self) {
        for hold in std::mem::take(&mut self.holds) {
            self.end_hold(hold);
        }
        self.check();
        for peer in 0..PEERS {
            self.apply(Op::Retire { peer });
        }
        assert_eq!(
            self.queue.chunk_budget_usage(),
            (0, 0),
            "{PROPERTY}: teardown leaves no claim"
        );
    }
}

/// Decoder v1: four bytes per operation, at most [`MAX_HISTORY_BYTES`] bytes.
fn decode(data: &[u8]) -> impl Iterator<Item = Op> + '_ {
    data[..data.len().min(MAX_HISTORY_BYTES)]
        .chunks(4)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            let d = chunk.get(3).copied().unwrap_or(0);
            let peer = usize::from(b) % PEERS;
            let tip = c % TIPS;
            match a % 11 {
                0 => Op::Stage {
                    peer,
                    tip,
                    priority: d,
                },
                1 => Op::StageDistinct {
                    peer,
                    tip,
                    priority: d,
                },
                2 => Op::Reserve {
                    peer,
                    count: u32::from(u16::from_le_bytes([c, d])) % 300,
                },
                3 => Op::Cancel { peer },
                4 => Op::Start { peer, tip },
                5 => Op::Page {
                    peer,
                    returned: usize::from(u16::from_le_bytes([c, d])) % 300,
                    reaches_target: d & 0x80 != 0,
                },
                6 => Op::Prepare {
                    peer,
                    bounded_prefix: d & 1 != 0,
                },
                7 => Op::Complete {
                    hold: usize::from(b),
                    apply: d & 1 == 0,
                },
                8 => Op::Retire { peer },
                9 => Op::Commit { generation: b % 2 },
                _ => Op::Offer {
                    peer,
                    tip,
                    priority: d >> 5,
                    blockers: OfferBlockers::from_bits(d),
                },
            }
        })
}

/// Replay at most [`MAX_HISTORY_BYTES`] decoder-v1 bytes through the queue,
/// checking every property after each operation, then tear the history down
/// and require every claim back. Return the transitions reached.
fn replay_header_work_bytes(data: &[u8]) -> HeaderWorkReplaySummary {
    let mut harness = Harness::new();
    for op in decode(data) {
        harness.summary.transitions += 1;
        harness.apply(op);
    }
    harness.teardown();
    harness.summary
}

mod tests {
    use super::*;

    fn run(ops: impl IntoIterator<Item = Op>) -> Harness {
        let mut harness = Harness::new();
        for op in ops {
            harness.apply(op);
        }
        harness
    }

    fn page_op(peer: usize, returned: usize) -> Op {
        Op::Page {
            peer,
            returned,
            reaches_target: false,
        }
    }

    fn prepare_op(peer: usize) -> Op {
        Op::Prepare {
            peer,
            bounded_prefix: false,
        }
    }

    fn started(peer: usize, tip: u8, count: u32) -> [Op; 3] {
        [
            Op::Stage {
                peer,
                tip,
                priority: 1,
            },
            Op::Reserve { peer, count },
            Op::Start { peer, tip },
        ]
    }

    const OP_STAGE: u8 = 0;
    const OP_RESERVE: u8 = 2;
    const OP_START: u8 = 4;
    const OP_PAGE: u8 = 5;
    const OP_PREPARE: u8 = 6;
    const OP_COMPLETE: u8 = 7;
    const OP_RETIRE: u8 = 8;
    const OP_OFFER: u8 = 10;

    #[rustfmt::skip]
    const WITNESS: &[u8] = &[
        // Peer 0 hands one prepare to its apply.
        OP_STAGE, 0, 1, 1,    OP_RESERVE, 0, 10, 0,  OP_START, 0, 1, 0,
        OP_PAGE, 0, 10, 0,    OP_PREPARE, 0, 0, 0,   OP_COMPLETE, 0, 0, 0,
        // Peer 1's prepare outlives its round while peer 0's apply ends.
        OP_STAGE, 1, 2, 1,    OP_RESERVE, 1, 20, 0,  OP_START, 1, 2, 0,
        OP_PAGE, 1, 20, 0,    OP_PREPARE, 1, 0, 0,   OP_RETIRE, 1, 0, 0,
        OP_COMPLETE, 1, 0, 0, OP_COMPLETE, 0, 0, 1,
        // Peer 2's offer stages, a pruned pause then spends it, and peer 3
        // waits for the rest of its round's credit.
        OP_OFFER, 2, 3, 0,    OP_OFFER, 2, 3, 1,
        OP_STAGE, 3, 4, 1,    OP_RESERVE, 3, 20, 0,  OP_START, 3, 4, 0,
        OP_PAGE, 3, 10, 0,
    ];

    machine_target_tests! {
        replay: replay_header_work_bytes,
        target: "header_work_ownership",
        decoder: 1,
        witness: WITNESS => HeaderWorkReplaySummary {
            transitions: 20,
            starts: 3,
            prepares: 2,
            handoffs: 1,
            stale_completions: 1,
            evictions: 0,
            retained_after_retirement: 1,
            credit_waits: 1,
            continuations: 2,
            prepare_decisions: 0,
            malformed_pages: 0,
            offers_staged: 1,
            offers_spent: 1,
        },
    }

    /// One peer's call outlives its round while another peer's work advances, and
    /// the capacity comes back only when the call ends.
    #[test]
    fn a_retired_rounds_call_keeps_its_headers_charged_across_other_work() {
        let mut ops = started(0, 1, 100).to_vec();
        ops.extend([page_op(0, 100), prepare_op(0)]);
        ops.extend(started(1, 2, 50));
        ops.extend([page_op(1, 20), Op::Retire { peer: 0 }]);
        let mut harness = run(ops);
        assert_eq!(harness.queue.chunk_budget_usage(), (30, 120));

        harness.apply(Op::Complete {
            hold: 0,
            apply: true,
        });
        assert_eq!(
            harness.summary.stale_completions, 1,
            "the retired round's call settles nothing"
        );
        assert_eq!(harness.summary.handoffs, 0);
        assert_eq!(harness.queue.chunk_budget_usage(), (30, 20));
    }

    /// A later round reuses the owner, and the older round's call cannot settle it.
    #[test]
    fn an_older_rounds_call_cannot_settle_a_later_round_with_the_same_owner() {
        let mut ops = started(0, 1, 10).to_vec();
        ops.extend([page_op(0, 10), prepare_op(0), Op::Retire { peer: 0 }]);
        ops.extend(started(0, 1, 10));
        ops.extend([page_op(0, 10), prepare_op(0)]);
        let mut harness = run(ops);
        let old = harness.queue.active(&peer_id(0)).map(|active| active.owner);
        assert_eq!(harness.holds.len(), 2);

        harness.apply(Op::Complete {
            hold: 0,
            apply: true,
        });
        assert_eq!(harness.summary.stale_completions, 1);
        assert_eq!(harness.summary.handoffs, 0);
        assert_eq!(harness.queue.active(&peer_id(0)).map(|a| a.owner), old);

        harness.apply(Op::Complete {
            hold: 0,
            apply: true,
        });
        assert_eq!(harness.summary.handoffs, 1);
        assert_eq!(harness.queue.chunk_budget_usage(), (0, 10));
    }

    /// A full queue replaces the lowest-priority unstarted work and releases only
    /// that peer's capacity.
    #[test]
    fn eviction_releases_exactly_the_evicted_peers_capacity() {
        let mut ops = Vec::new();
        for peer in 0..MAX_STAGED_TARGETS_V1 {
            ops.push(Op::Stage {
                peer,
                tip: 0,
                priority: if peer == 3 { 0 } else { 1 },
            });
        }
        ops.push(Op::Reserve { peer: 3, count: 10 });
        ops.push(Op::Reserve { peer: 4, count: 20 });
        ops.push(Op::Stage {
            peer: MAX_STAGED_TARGETS_V1,
            tip: 1,
            priority: 1,
        });
        let harness = run(ops);
        assert_eq!(harness.summary.evictions, 1);
        assert!(!harness.queue.has_work(&peer_id(3)));
        assert_eq!(harness.queue.chunk_budget_usage(), (20, 0));
        assert_eq!(harness.queue.reserved_header_count(&peer_id(3)), 0);
    }

    /// Among unstarted work of equal lowest priority, a full queue evicts the
    /// peer whose ID sorts first, and never work of equal or higher priority.
    #[test]
    fn a_full_queue_evicts_the_first_peer_of_the_lowest_priority() {
        let mut ops = Vec::new();
        for peer in 0..MAX_STAGED_TARGETS_V1 {
            ops.push(Op::Stage {
                peer,
                tip: 0,
                priority: if peer < 2 { 0 } else { 1 },
            });
        }
        let new = MAX_STAGED_TARGETS_V1;
        let mut harness = run(ops);
        harness.apply(Op::Stage {
            peer: new,
            tip: 1,
            priority: 0,
        });
        assert!(
            !harness.queue.has_work(&peer_id(new)),
            "no lower priority to evict"
        );
        harness.apply(Op::Stage {
            peer: new,
            tip: 1,
            priority: 1,
        });
        let kept: Vec<bool> = (0..3)
            .map(|peer| harness.queue.has_work(&peer_id(peer)))
            .collect();
        assert_eq!(kept, [false, true, true]);
    }

    /// Each page decision follows from the round's credit, the budget, and the target.
    #[test]
    fn each_page_decides_what_follows_it() {
        let reach = |peer| Op::Page {
            peer,
            returned: 5,
            reaches_target: true,
        };
        let mut ops = started(0, 1, 10).to_vec();
        // The rest of the round's credit is still on its way.
        ops.push(page_op(0, 5));
        // The last of its credit arrived, and the budget has room.
        ops.push(page_op(0, 5));
        // Target 2 is not the staged header's hash, and target 0 is.
        ops.extend(started(1, 2, 10));
        ops.push(reach(1));
        ops.extend(started(2, 0, 10));
        ops.push(reach(2));
        // A page with no headers leaves no staged tip.
        ops.extend(started(3, 3, 10));
        ops.push(page_op(3, 0));

        let harness = run(ops);

        assert_eq!(harness.summary.credit_waits, 1);
        assert_eq!(harness.summary.continuations, 1);
        assert_eq!(harness.summary.malformed_pages, 2);
        assert_eq!(harness.summary.prepare_decisions, 1);
    }

    /// A full budget bounds a round to its staged prefix, which narrows the
    /// owner to the staged tip and prepares a prefix.
    #[test]
    fn a_full_budget_prepares_a_bounded_prefix() {
        let mut ops = Vec::new();
        for peer in 0..MAX_STAGED_TARGETS_V1 {
            let count = u32::try_from(MAX_HEADER_CHUNK_RESERVATION_V1).expect("the share fits");
            let tip = u8::try_from(peer % 5 + 1).expect("the tip is small");
            ops.extend(started(peer, tip, count));
        }
        ops.push(page_op(0, MAX_HEADER_CHUNK_RESERVATION_V1));
        let mut harness = run(ops);
        assert_eq!(harness.summary.prepare_decisions, 1);
        let operation = harness.queue.begin_prepare(&peer_id(0), true);
        let Some(HeaderPortOperation::PrepareHeaderTarget { completion, .. }) = operation else {
            panic!("the staged round prepares");
        };
        assert_eq!(
            completion,
            zakura_header_chain::TargetCompletion::TargetPrefix {
                common_ancestor: ancestor()
            }
        );
        let active = harness
            .queue
            .active(&peer_id(0))
            .expect("the round is active");
        assert_eq!(
            active.owner.header_authority().branch.target_tip_hash,
            tip_hash(0)
        );
    }

    /// A round that reached its target prepares all of it under its owner.
    #[test]
    fn a_complete_round_prepares_the_whole_target() {
        let mut harness = run(started(0, 0, 10));
        harness.apply(Op::Page {
            peer: 0,
            returned: 10,
            reaches_target: true,
        });
        let owner = harness.queue.active(&peer_id(0)).map(|active| active.owner);
        let operation = harness.queue.begin_prepare(&peer_id(0), false);
        let Some(HeaderPortOperation::PrepareHeaderTarget { completion, .. }) = operation else {
            panic!("the staged round prepares");
        };
        assert_eq!(
            completion,
            zakura_header_chain::TargetCompletion::TargetComplete {
                common_ancestor: ancestor()
            }
        );
        assert_eq!(
            harness.queue.active(&peer_id(0)).map(|active| active.owner),
            owner
        );
    }

    /// A prepare needs staged receiving work, and an apply needs a preparing round.
    #[test]
    fn phases_only_move_forward_through_the_policy() {
        let mut harness = run(started(0, 1, 10));
        assert!(harness.queue.begin_prepare(&peer_id(0), false).is_none());
        assert!(!harness.queue.begin_apply(&peer_id(0)));
        harness.apply(page_op(0, 10));
        harness.apply(prepare_op(0));
        assert_eq!(harness.summary.prepares, 1);
        harness.apply(prepare_op(0));
        assert_eq!(
            harness.summary.prepares, 1,
            "a preparing round cannot prepare"
        );
        assert!(
            !harness.queue.stage_page(
                &peer_id(0),
                ancestor(),
                page(1),
                Cursor {
                    height: block::Height(0),
                    hash: hash(0),
                    headers: 0,
                    bytes: 0,
                },
                arrival(&peer_id(0)),
            ),
            "a preparing round stages no page"
        );
    }

    /// What each offer decides, in the order its checks run.
    #[test]
    fn offer_decisions() {
        use OfferedPeerWork::{Idle, Other, SameOrdinaryTarget};
        let facts = |pruned_pause, covered, eligible, work| OfferFacts {
            pruned_pause,
            covered,
            eligible,
            work,
        };
        let spend = |cause, close_subscription| OfferDecision::Spend {
            cause,
            close_subscription,
        };
        // (pruned pause, covered, eligible, work, held back, subscription ready) => decision
        for (pruned_pause, covered, eligible, work, held_back, ready, decision) in [
            (
                true,
                true,
                false,
                Idle,
                true,
                false,
                spend(SpentOffer::PrunedPause, true),
            ),
            (
                true,
                false,
                true,
                Other,
                false,
                true,
                spend(SpentOffer::PrunedPause, false),
            ),
            (
                false,
                true,
                false,
                Idle,
                false,
                true,
                spend(SpentOffer::Covered, true),
            ),
            (
                false,
                true,
                true,
                SameOrdinaryTarget,
                false,
                true,
                spend(SpentOffer::Covered, false),
            ),
            (
                false,
                false,
                false,
                Idle,
                false,
                true,
                spend(SpentOffer::Ineligible, true),
            ),
            (
                false,
                false,
                false,
                Other,
                false,
                true,
                spend(SpentOffer::Ineligible, false),
            ),
            (
                false,
                false,
                true,
                SameOrdinaryTarget,
                true,
                false,
                OfferDecision::AlreadyActive { spend: true },
            ),
            (
                false,
                false,
                true,
                Other,
                true,
                false,
                OfferDecision::AlreadyActive { spend: false },
            ),
            (
                false,
                false,
                true,
                Idle,
                true,
                true,
                OfferDecision::HeldBack,
            ),
            (
                false,
                false,
                true,
                Idle,
                false,
                false,
                OfferDecision::AwaitSubscription,
            ),
            (false, false, true, Idle, false, true, OfferDecision::Stage),
        ] {
            assert_eq!(
                decide_offer(
                    facts(pruned_pause, covered, eligible, work),
                    || held_back,
                    || ready
                ),
                decision,
                "pruned={pruned_pause} covered={covered} eligible={eligible} {work:?} \
                 held_back={held_back} ready={ready}"
            );
        }
    }

    /// A blocker keeps a held-back offer from staging, and the same offer stages
    /// once nothing holds it.
    #[test]
    fn an_offer_stages_only_when_nothing_blocks_it() {
        let clear = OfferBlockers::from_bits(0);
        let harness = run([
            Op::Offer {
                peer: 0,
                tip: 1,
                priority: 1,
                blockers: OfferBlockers {
                    held_back: true,
                    ..clear
                },
            },
            Op::Offer {
                peer: 1,
                tip: 1,
                priority: 1,
                blockers: OfferBlockers {
                    subscription_ready: false,
                    ..clear
                },
            },
        ]);
        assert_eq!(harness.queue.len(), 0);

        let mut harness = run([Op::Offer {
            peer: 0,
            tip: 1,
            priority: 1,
            blockers: clear,
        }]);
        assert_eq!(harness.summary.offers_staged, 1);
        assert_eq!(harness.queue.len(), 1);

        // A pruned pause ends the unstarted work that the first offer staged.
        harness.apply(Op::Offer {
            peer: 0,
            tip: 1,
            priority: 1,
            blockers: OfferBlockers {
                pruned_pause: true,
                ..clear
            },
        });
        assert_eq!(harness.summary.offers_spent, 1);
        assert_eq!(harness.queue.len(), 0);
    }

    /// A planted capacity leak breaks the queue's invariants.
    #[test]
    #[should_panic(expected = "header/work-ownership")]
    fn the_harness_rejects_a_lost_lease() {
        let mut ops = started(0, 1, 10).to_vec();
        ops.push(page_op(0, 10));
        let mut harness = run(ops);
        harness.queue.staged_capacity.clear();
        harness.check();
    }

    /// A call settled behind the harness's back no longer settles its round.
    #[test]
    #[should_panic(expected = "header/work-ownership")]
    fn the_harness_rejects_a_hidden_settlement() {
        let mut ops = started(0, 1, 10).to_vec();
        ops.extend([page_op(0, 10), prepare_op(0)]);
        let mut harness = run(ops);
        let work = harness.holds[0].retained.work();
        assert!(harness.queue.settle_call(&peer_id(0), work));
        harness.apply(Op::Complete {
            hold: 0,
            apply: true,
        });
    }
}
