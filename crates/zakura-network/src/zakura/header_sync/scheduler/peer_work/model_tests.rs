//! Interleaved work decisions against an independent model.
//!
//! Each step drives the real [`PeerWorkQueue`], with its real budget and leases,
//! the way the reactor does, applies the same step to a model built from plain
//! maps, and compares every observable fact. Steps for different peers and for
//! calls that outlive their round interleave freely. The model never calls the
//! queue or its policy.

use std::collections::{BTreeMap, BTreeSet};

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{
    AlarmSet, ChainScore, EngineMode, Frontier, FrontierSet, HeaderGeneration, HeaderLocator,
    HeaderWorkOwner, StateVersion, SuffixWork, VerifiedGeneration, MAX_STAGED_TARGETS_V1,
};

use super::{
    ActiveHeaderRequest, AdvertisedHeaderTarget, HeaderTargetPhase, HeaderTargetPurpose,
    PeerWorkPriority, PeerWorkQueue, QueueWorkResult, RetainedHeaders, StorageCall,
    StorageCallKind, WorkIncarnation, MAX_HEADER_CHUNK_RESERVATION_V1,
};
use crate::zakura::{
    header_sync::{wire::AuxSchema, HeaderSyncRequestId, Status},
    ZakuraPeerId,
};

const PROPERTY: &str = "header/work-ownership";

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
    /// Deliver a page of `returned` headers for `peer`.
    Page { peer: usize, returned: usize },
    /// Dispatch a prepare for `peer`'s current round.
    Prepare { peer: usize },
    /// Complete the `hold`th live call. A current prepare moves its headers to an
    /// apply when `apply` holds. Every other completion ends its ownership.
    Complete { hold: usize, apply: bool },
    /// Retire `peer`'s work, as a timeout or disconnect does.
    Retire { peer: usize },
    /// A committed snapshot at header generation `generation` retires what it obsoletes.
    Commit { generation: u8 },
}

/// One peer's work in the model.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ModelWork {
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
    },
}

impl ModelWork {
    fn tip(&self) -> u8 {
        match self {
            Self::Awaiting { tip, .. } | Self::Active { tip, .. } => *tip,
        }
    }
}

/// One owned lease in the model.
#[derive(Debug)]
struct ModelLease {
    count: usize,
    /// The peer whose queue slot holds a clone.
    queued_for: Option<usize>,
    /// The live calls holding a clone.
    holders: BTreeSet<u64>,
}

impl ModelLease {
    fn alive(&self) -> bool {
        self.queued_for.is_some() || !self.holders.is_empty()
    }
}

/// One live call's ownership in the model and in the queue's runtime.
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

/// Applicable transitions in one replayed work history.
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
}

struct Harness {
    queue: PeerWorkQueue,
    work: BTreeMap<usize, ModelWork>,
    reservations: BTreeMap<usize, usize>,
    leases: Vec<ModelLease>,
    holds: Vec<Hold>,
    revision: u64,
    last_incarnation: u64,
    last_call: u64,
    last_hold: u64,
    summary: HeaderWorkReplaySummary,
}

fn peer_id(peer: usize) -> ZakuraPeerId {
    let byte = u8::try_from(peer).expect("the test uses few peers") + 1;
    ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length")
}

fn hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

fn priority_of(priority: u8) -> PeerWorkPriority {
    match priority % 3 {
        0 => PeerWorkPriority::LowerComparableWork,
        1 => PeerWorkPriority::Normal,
        _ => PeerWorkPriority::HigherComparableWork,
    }
}

fn snapshot(generation: u8) -> zakura_header_chain::EngineSnapshot {
    let finalized = Frontier::new(block::Height(10), hash(0xF0));
    let tip = Frontier::new(block::Height(100), hash(0xF1));
    zakura_header_chain::EngineSnapshot {
        mode: EngineMode::Integrated,
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(u64::from(generation)),
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

fn target(tip: u8, generation: u8) -> AdvertisedHeaderTarget {
    AdvertisedHeaderTarget {
        scope: zakura_header_chain::HeaderWorkAuthority::for_target(
            &snapshot(generation),
            hash(tip),
        ),
        session_id: 1,
        status: Status {
            work_anchor_height: block::Height(10),
            work_anchor_hash: hash(0xF0),
            selected_tip_height: block::Height(u32::from(tip) + 11),
            selected_tip_hash: hash(tip),
            suffix_cumulative_work: U256::from(1_u32),
            oldest_retained_height: block::Height(10),
            max_headers_per_response: 1_000,
            max_subscriptions: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: 0,
        },
    }
}

fn request(peer: usize, tip: u8, generation: u8) -> ActiveHeaderRequest {
    let target = target(tip, generation);
    let request_id = HeaderSyncRequestId::new(1).expect("one is nonzero");
    ActiveHeaderRequest {
        purpose: HeaderTargetPurpose::Normal,
        peer: peer_id(peer),
        source: zakura_header_chain::SourceId::from_digest([1; 32]),
        sent_locator: HeaderLocator::for_continuation(Frontier::new(block::Height(10), hash(0xF0))),
        owner: HeaderWorkOwner {
            authority: target.scope,
            session_id: target.session_id,
            // Every round reuses one request ID, as a resumed subscription does.
            request_id: std::num::NonZeroU64::new(request_id.get()).expect("one is nonzero"),
        }
        .into(),
        target,
        request_id,
        common_ancestor: None,
        entries: Vec::new(),
        phase: HeaderTargetPhase::Receiving,
        incarnation: WorkIncarnation::UNSTARTED,
        cursor: None,
        max_header_count: 1_000,
        tree_aux_schema: AuxSchema::None,
    }
}

impl Harness {
    fn new() -> Self {
        Self {
            queue: PeerWorkQueue::default(),
            work: BTreeMap::new(),
            reservations: BTreeMap::new(),
            leases: Vec::new(),
            holds: Vec::new(),
            revision: 0,
            last_incarnation: 0,
            last_call: 0,
            last_hold: 0,
            summary: HeaderWorkReplaySummary::default(),
        }
    }

    /// The model's capacity release for `peer`'s queue slot.
    fn release(&mut self, peer: usize) {
        self.reservations.remove(&peer);
        for lease in &mut self.leases {
            if lease.queued_for == Some(peer) {
                lease.queued_for = None;
            }
        }
        self.leases.retain(ModelLease::alive);
    }

    fn claimed(&self) -> usize {
        self.reservations.values().sum::<usize>()
            + self.leases.iter().map(|lease| lease.count).sum::<usize>()
    }

    /// The model's result of staging `tip` for `peer`.
    fn model_stage(
        &mut self,
        peer: usize,
        tip: u8,
        priority: u8,
        generation: u8,
    ) -> QueueWorkResult {
        let rank = priority % 3;
        match self.work.get(&peer) {
            Some(ModelWork::Active { .. }) => return QueueWorkResult::AlreadyActive,
            Some(ModelWork::Awaiting { tip: current, .. }) => {
                if *current != tip {
                    self.revision += 1;
                }
                self.work.insert(
                    peer,
                    ModelWork::Awaiting {
                        tip,
                        priority: rank,
                        generation,
                    },
                );
                self.release(peer);
                return QueueWorkResult::NeedsLocator;
            }
            None => {}
        }
        if self.work.len() >= MAX_STAGED_TARGETS_V1 {
            // Peer bytes sort with peer indexes, so the first lowest priority wins.
            let evicted = self
                .work
                .iter()
                .filter_map(|(peer, work)| match work {
                    ModelWork::Awaiting { priority, .. } if *priority < rank => {
                        Some((*priority, *peer))
                    }
                    _ => None,
                })
                .min();
            let Some((_, evicted)) = evicted else {
                return QueueWorkResult::AtCapacity;
            };
            self.work.remove(&evicted);
            self.release(evicted);
            self.revision += 1;
            self.summary.evictions += 1;
        }
        self.work.insert(
            peer,
            ModelWork::Awaiting {
                tip,
                priority: rank,
                generation,
            },
        );
        self.revision += 1;
        QueueWorkResult::NeedsLocator
    }

    /// Take the hold that `hold` selects, if any call is live.
    fn take_hold(&mut self, hold: usize) -> Option<Hold> {
        if self.holds.is_empty() {
            return None;
        }
        let index = hold % self.holds.len();
        Some(self.holds.remove(index))
    }

    /// End `hold` in the model and drop its real owner.
    fn end_hold(&mut self, hold: Hold) {
        for lease in &mut self.leases {
            lease.holders.remove(&hold.id);
        }
        self.leases.retain(ModelLease::alive);
        drop(hold);
    }

    fn apply(&mut self, op: Op) {
        self.summary.transitions += 1;
        match op {
            Op::Stage {
                peer,
                tip,
                priority,
            } => {
                let actual = self
                    .queue
                    .stage(peer_id(peer), target(tip, 0), priority_of(priority));
                let expected = self.model_stage(peer, tip, priority, 0);
                assert_eq!(actual, expected, "{PROPERTY}: stage");
            }
            Op::StageDistinct {
                peer,
                tip,
                priority,
            } => {
                let actual = self.queue.stage_distinct_target(
                    peer_id(peer),
                    target(tip, 0),
                    priority_of(priority),
                );
                let owned_elsewhere = self
                    .work
                    .iter()
                    .any(|(owner, work)| *owner != peer && work.tip() == tip);
                let expected = if owned_elsewhere {
                    if matches!(self.work.get(&peer), Some(ModelWork::Awaiting { .. })) {
                        self.work.remove(&peer);
                        self.release(peer);
                        self.revision += 1;
                    }
                    QueueWorkResult::TargetAlreadyAssigned
                } else {
                    self.model_stage(peer, tip, priority, 0)
                };
                assert_eq!(actual, expected, "{PROPERTY}: stage distinct");
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
                    Some(ModelWork::Awaiting { generation, .. }) => *generation,
                    _ => 0,
                };
                let actual = self.queue.start(request(peer, tip, generation));
                let expected = matches!(
                    self.work.get(&peer),
                    Some(ModelWork::Awaiting { tip: awaited, .. }) if *awaited == tip
                ) && self.reservations.contains_key(&peer);
                if expected {
                    self.last_incarnation += 1;
                    self.revision += 1;
                    self.summary.starts += 1;
                    self.work.insert(
                        peer,
                        ModelWork::Active {
                            tip,
                            incarnation: self.last_incarnation,
                            phase: HeaderTargetPhase::Receiving,
                            call: None,
                        },
                    );
                }
                assert_eq!(actual, expected, "{PROPERTY}: start");
            }
            Op::Page { peer, returned } => {
                let actual = self
                    .queue
                    .consume_response_capacity(&peer_id(peer), returned);
                let expected = self
                    .reservations
                    .get(&peer)
                    .is_some_and(|reserved| returned <= *reserved);
                if expected {
                    let reserved = self
                        .reservations
                        .get_mut(&peer)
                        .expect("the reservation was just found");
                    *reserved -= returned;
                    if *reserved == 0 {
                        self.reservations.remove(&peer);
                    }
                    if returned != 0 {
                        self.leases.push(ModelLease {
                            count: returned,
                            queued_for: Some(peer),
                            holders: BTreeSet::new(),
                        });
                    }
                }
                assert_eq!(actual, expected, "{PROPERTY}: page");
            }
            Op::Prepare { peer } => {
                let retained = self.queue.retain_headers(&peer_id(peer));
                let Some(ModelWork::Active {
                    incarnation,
                    phase,
                    call,
                    ..
                }) = self.work.get_mut(&peer)
                else {
                    assert!(
                        retained.is_none(),
                        "{PROPERTY}: only a started round retains headers"
                    );
                    return self.check();
                };
                let incarnation = *incarnation;
                let mut retained =
                    retained.expect("the queue holds the round that the model holds");
                self.last_call += 1;
                self.last_hold += 1;
                *phase = HeaderTargetPhase::Preparing;
                *call = Some(self.last_call);
                let storage_call = StorageCall {
                    number: self.last_call,
                    kind: StorageCallKind::Prepare,
                };
                retained.enter_call(storage_call);
                self.queue.bind_call(
                    &peer_id(peer),
                    retained.work().expect("the call was just entered"),
                );
                self.queue
                    .active_mut(&peer_id(peer))
                    .expect("the round is active")
                    .phase = HeaderTargetPhase::Preparing;
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
                    return self.check();
                };
                let work = hold.retained.work().expect("every hold entered a call");
                let actual = self.queue.settle_call(&peer_id(hold.peer), work);
                let current = matches!(
                    self.work.get(&hold.peer),
                    Some(ModelWork::Active { incarnation, call, .. })
                        if *incarnation == hold.incarnation && *call == Some(hold.call)
                );
                assert_eq!(actual, current, "{PROPERTY}: exact settlement");
                if let Some(ModelWork::Active { call, .. }) = self.work.get_mut(&hold.peer) {
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
                    hold.retained.enter_call(StorageCall {
                        number: self.last_call,
                        kind: StorageCallKind::Apply,
                    });
                    self.queue.bind_call(
                        &peer_id(hold.peer),
                        hold.retained.work().expect("the call was just entered"),
                    );
                    self.queue
                        .active_mut(&peer_id(hold.peer))
                        .expect("the current round is active")
                        .phase = HeaderTargetPhase::Applying;
                    if let Some(ModelWork::Active { call, phase, .. }) =
                        self.work.get_mut(&hold.peer)
                    {
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
                if existed.is_some() {
                    self.revision += 1;
                }
                assert_eq!(
                    removed.is_some(),
                    matches!(existed, Some(ModelWork::Active { .. })),
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
                        ModelWork::Awaiting {
                            generation: staged, ..
                        } if *staged != generation => Some(*peer),
                        _ => None,
                    })
                    .collect();
                assert_eq!(retired, obsolete.len(), "{PROPERTY}: unstarted retirement");
                for peer in obsolete {
                    self.work.remove(&peer);
                    self.release(peer);
                    self.revision += 1;
                }
            }
        }
        self.check();
    }

    /// Compare every observable fact of the queue with the model.
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
        assert_eq!(
            self.queue.scheduling_revision(),
            self.revision,
            "{PROPERTY}: revision"
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
                    request.incarnation.get(),
                    request.phase,
                    request.target.status.selected_tip_hash,
                )
            });
            let expected = match self.work.get(&peer) {
                Some(ModelWork::Active {
                    incarnation,
                    phase,
                    tip,
                    ..
                }) => Some((*incarnation, *phase, hash(*tip))),
                _ => None,
            };
            assert_eq!(actual, expected, "{PROPERTY}: peer {peer} round");
            assert_eq!(
                self.queue
                    .awaiting_target(&id)
                    .map(|target| target.status.selected_tip_hash),
                match self.work.get(&peer) {
                    Some(ModelWork::Awaiting { tip, .. }) => Some(hash(*tip)),
                    _ => None,
                },
                "{PROPERTY}: peer {peer} awaiting target"
            );
        }
        for tip in 0..TIPS {
            assert_eq!(
                self.queue.is_target_assigned(hash(tip)),
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
            let work = hold.retained.work().expect("every hold entered a call");
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
            if let ModelWork::Active { phase, .. } = work {
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
    }

    /// End every call, as a teardown does, and check that all capacity returned
    /// once the queue retires its work.
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

/// Decoder-v1: four bytes per operation, at most 512 bytes.
fn decode(data: &[u8]) -> impl Iterator<Item = Op> + '_ {
    data[..data.len().min(512)].chunks(4).map(|chunk| {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        let d = chunk.get(3).copied().unwrap_or(0);
        let peer = usize::from(b) % PEERS;
        let tip = c % TIPS;
        match a % 10 {
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
            },
            6 => Op::Prepare { peer },
            7 => Op::Complete {
                hold: usize::from(b),
                apply: d & 1 == 0,
            },
            8 => Op::Retire { peer },
            _ => Op::Commit { generation: b % 2 },
        }
    })
}

/// Replay decoder-v1 operations against the independent work model, then tear
/// the history down and require every claim back.
fn replay_header_work_bytes(data: &[u8]) -> HeaderWorkReplaySummary {
    let mut harness = Harness::new();
    for op in decode(data) {
        harness.apply(op);
    }
    harness.teardown();
    harness.summary
}

mod tests {
    use proptest::prelude::*;

    use super::*;

    fn run(ops: impl IntoIterator<Item = Op>) -> Harness {
        let mut harness = Harness::new();
        for op in ops {
            harness.apply(op);
        }
        harness
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

    fn op() -> impl Strategy<Value = Op> {
        let peer = 0..PEERS;
        let tip = 0..TIPS;
        prop_oneof![
            4 => (peer.clone(), tip.clone(), any::<u8>())
                .prop_map(|(peer, tip, priority)| Op::Stage { peer, tip, priority }),
            2 => (peer.clone(), tip.clone(), any::<u8>())
                .prop_map(|(peer, tip, priority)| Op::StageDistinct { peer, tip, priority }),
            4 => (peer.clone(), 0..300_u32).prop_map(|(peer, count)| Op::Reserve { peer, count }),
            1 => peer.clone().prop_map(|peer| Op::Cancel { peer }),
            4 => (peer.clone(), tip).prop_map(|(peer, tip)| Op::Start { peer, tip }),
            4 => (peer.clone(), 0..300_usize)
                .prop_map(|(peer, returned)| Op::Page { peer, returned }),
            3 => peer.clone().prop_map(|peer| Op::Prepare { peer }),
            4 => (any::<usize>(), any::<bool>())
                .prop_map(|(hold, apply)| Op::Complete { hold, apply }),
            2 => peer.prop_map(|peer| Op::Retire { peer }),
            1 => (0..2_u8).prop_map(|generation| Op::Commit { generation }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn interleaved_work_matches_the_model(ops in prop::collection::vec(op(), 1..96)) {
            run(ops).teardown();
        }

        #[test]
        fn work_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
            replay_header_work_bytes(&bytes);
        }
    }

    /// One peer's call outlives its round while another peer's work advances, and
    /// the capacity comes back only when the call ends.
    #[test]
    fn a_retired_rounds_call_keeps_its_headers_charged_across_other_work() {
        let mut ops = started(0, 1, 100).to_vec();
        ops.extend([
            Op::Page {
                peer: 0,
                returned: 100,
            },
            Op::Prepare { peer: 0 },
        ]);
        ops.extend(started(1, 2, 50));
        ops.extend([
            Op::Page {
                peer: 1,
                returned: 20,
            },
            Op::Retire { peer: 0 },
        ]);
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
        ops.extend([
            Op::Page {
                peer: 0,
                returned: 10,
            },
            Op::Prepare { peer: 0 },
            Op::Retire { peer: 0 },
        ]);
        ops.extend(started(0, 1, 10));
        ops.extend([
            Op::Page {
                peer: 0,
                returned: 10,
            },
            Op::Prepare { peer: 0 },
        ]);
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

    /// A handoff keeps the same headers charged, with no moment of release.
    #[test]
    fn a_prepare_hands_its_headers_to_its_apply_without_a_release() {
        let mut ops = started(0, 1, 10).to_vec();
        ops.extend([
            Op::Page {
                peer: 0,
                returned: 10,
            },
            Op::Prepare { peer: 0 },
            Op::Retire { peer: 1 },
        ]);
        let mut harness = run(ops);
        let mut releases = harness.queue.capacity_releases();
        releases.mark_unchanged();

        harness.apply(Op::Complete {
            hold: 0,
            apply: true,
        });

        assert_eq!(harness.summary.handoffs, 1);
        assert_eq!(harness.queue.chunk_budget_usage(), (0, 10));
        assert!(
            !releases.has_changed().expect("the budget is alive"),
            "the handoff releases nothing"
        );
        harness.apply(Op::Retire { peer: 0 });
        assert_eq!(harness.queue.chunk_budget_usage(), (0, 10));
        harness.apply(Op::Complete {
            hold: 0,
            apply: false,
        });
        assert_eq!(harness.queue.chunk_budget_usage(), (0, 0));
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
            priority: 2,
        });
        let harness = run(ops);
        assert_eq!(harness.summary.evictions, 1);
        assert_eq!(harness.queue.chunk_budget_usage(), (20, 0));
        assert_eq!(harness.queue.reserved_header_count(&peer_id(3)), 0);
    }

    /// A fixed decoder-v1 history reaches every kind of decision, so the
    /// property tests above are not vacuous.
    #[test]
    fn the_witness_reaches_every_decision() {
        const OP_STAGE: u8 = 0;
        const OP_RESERVE: u8 = 2;
        const OP_START: u8 = 4;
        const OP_PAGE: u8 = 5;
        const OP_PREPARE: u8 = 6;
        const OP_COMPLETE: u8 = 7;
        const OP_RETIRE: u8 = 8;
        #[rustfmt::skip]
        let witness = [
            // Peer 0 hands one prepare to its apply.
            OP_STAGE, 0, 1, 1,    OP_RESERVE, 0, 10, 0,  OP_START, 0, 1, 0,
            OP_PAGE, 0, 10, 0,    OP_PREPARE, 0, 0, 0,   OP_COMPLETE, 0, 0, 0,
            // Peer 1's prepare outlives its round while peer 0's apply ends.
            OP_STAGE, 1, 2, 1,    OP_RESERVE, 1, 20, 0,  OP_START, 1, 2, 0,
            OP_PAGE, 1, 20, 0,    OP_PREPARE, 1, 0, 0,   OP_RETIRE, 1, 0, 0,
            OP_COMPLETE, 1, 0, 0, OP_COMPLETE, 0, 0, 1,
        ];

        let summary = replay_header_work_bytes(&witness);

        assert_eq!(summary.starts, 2);
        assert_eq!(summary.prepares, 2);
        assert_eq!(summary.handoffs, 1);
        assert_eq!(summary.stale_completions, 1);
        assert_eq!(summary.retained_after_retirement, 1);
    }

    #[test]
    fn empty_input_replays_nothing() {
        assert_eq!(
            replay_header_work_bytes(&[]),
            HeaderWorkReplaySummary {
                transitions: PEERS,
                ..HeaderWorkReplaySummary::default()
            }
        );
    }

    /// A planted capacity leak fails the comparison.
    #[test]
    #[should_panic(expected = "header/work-ownership")]
    fn the_model_rejects_a_lost_lease() {
        let mut ops = started(0, 1, 10).to_vec();
        ops.push(Op::Page {
            peer: 0,
            returned: 10,
        });
        let mut harness = run(ops);
        harness.queue.staged_capacity.clear();
        harness.check();
    }

    /// A settlement that ignores the round fails the comparison.
    #[test]
    #[should_panic(expected = "header/work-ownership")]
    fn the_model_rejects_a_wrong_round_settlement() {
        let mut ops = started(0, 1, 10).to_vec();
        ops.extend([
            Op::Page {
                peer: 0,
                returned: 10,
            },
            Op::Prepare { peer: 0 },
        ]);
        let mut harness = run(ops);
        harness.holds[0].incarnation += 1;
        harness.check();
    }

    /// A revision bump that no decision made fails the comparison.
    #[test]
    #[should_panic(expected = "header/work-ownership")]
    fn the_model_rejects_a_wrong_revision() {
        let mut harness = run(started(0, 1, 10));
        harness.revision += 1;
        harness.check();
    }
}
