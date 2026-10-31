//! Machine tests for [`ReactorState`].
//!
//! Each step drives `ReactorState` the way the reactor does, applies the same
//! step to an independent model, and compares the two. The model never calls
//! `ReactorState`. A virtual clock supplies every `now`.

use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

#[cfg(test)]
use proptest::prelude::*;

use super::{state::*, *};

const PEERS: usize = 3;

/// How long a sent `Close` waits for its terminal outcome in these tests.
const OUTCOME_TIMEOUT: Duration = Duration::from_secs(5);

fn peer_id(peer: usize) -> ZakuraPeerId {
    let byte = u8::try_from(peer).expect("the test uses few peers") + 1;
    ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length")
}

fn hash(domain: u8, value: u8) -> block::Hash {
    let mut hash = [0; 32];
    hash[0] = domain;
    hash[1] = value;
    block::Hash(hash)
}

fn status(tip: u8) -> Status {
    Status {
        work_anchor_height: block::Height(0),
        work_anchor_hash: hash(0xA0, 0),
        selected_tip_height: block::Height(u32::from(tip) + 1),
        selected_tip_hash: hash(0xA1, tip),
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(1_u8),
        oldest_retained_height: block::Height(0),
        max_headers_per_response: 16,
        max_subscriptions: 1,
        max_message_bytes: 1_000_000,
        tree_aux_schema_mask: 0,
    }
}

/// Header authority: a header generation, a finality anchor, and a target tip.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Authority {
    generation: u8,
    anchor: u8,
    tip: u8,
}

impl Authority {
    #[cfg(test)]
    fn arbitrary() -> impl Strategy<Value = Self> {
        (0..2_u8, 0..2_u8, 0..3_u8).prop_map(|(generation, anchor, tip)| Self {
            generation,
            anchor,
            tip,
        })
    }

    fn scope(self) -> zakura_header_chain::HeaderWorkAuthority {
        zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(u64::from(
                self.generation,
            )),
            branch: zakura_header_chain::BranchId {
                anchor_hash: hash(0xB0, self.anchor),
                target_tip_hash: status(self.tip).selected_tip_hash,
            },
        }
    }

    /// An awaiting target under this authority from session `session_id`.
    fn target(self, session_id: u64) -> AdvertisedHeaderTarget {
        AdvertisedHeaderTarget {
            scope: self.scope(),
            session_id,
            status: status(self.tip),
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    /// Admit a new session for `peer`, replacing any current one.
    Connect { peer: usize },
    /// Remove `peer`'s session.
    Disconnect { peer: usize },
    /// Record a `Status` from `peer`'s session.
    Status { peer: usize, tip: u8 },
    /// Stage `peer`'s offer and request a locator read under `authority`.
    /// IO has locator capacity when `capacity` holds.
    RequestLocator {
        peer: usize,
        authority: Authority,
        capacity: bool,
    },
    /// Settle the `read`th started read with `found`, while its peer awaits `awaiting`.
    Complete {
        read: usize,
        found: bool,
        awaiting: Option<Authority>,
    },
    /// The `read`th started read panics.
    Panic { read: usize },
    /// Advance the virtual clock.
    Advance { millis: u64 },
    /// Try to close `peer`'s subscription. The session queue accepts the
    /// `Close` when `sent` holds, and the subscription is live when `live` holds.
    Close { peer: usize, sent: bool, live: bool },
    /// Run the maintenance deadlines for `Close` retries and outcomes.
    Deadlines,
    /// Request a full cached-offer pass under scheduling key `key`.
    CachedPass { key: u8 },
    /// A committed resource stall at state version `version`.
    ResourceStall { version: u8 },
    /// The committed snapshot moves to state version `version`.
    Commit { version: u8 },
    /// A repair supplier failed, so the reactor rotates `peer`.
    Rotate { peer: usize },
    /// The committed state asks for the repair `(generation, height)`, or for none.
    Repair { desired: Option<(u8, u8)> },
    /// The current repair stalled.
    RepairStall { supplier: bool },
    /// Run the repair stall's report and trace deadlines.
    RefreshStall,
}

#[cfg(test)]
fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        2 => (0..PEERS).prop_map(|peer| Op::Connect { peer }),
        1 => (0..PEERS).prop_map(|peer| Op::Disconnect { peer }),
        2 => (0..PEERS, 0..3_u8).prop_map(|(peer, tip)| Op::Status { peer, tip }),
        3 => (0..PEERS, Authority::arbitrary(), any::<bool>()).prop_map(
            |(peer, authority, capacity)| Op::RequestLocator { peer, authority, capacity }
        ),
        3 => (any::<usize>(), any::<bool>(), proptest::option::of(Authority::arbitrary()))
            .prop_map(|(read, found, awaiting)| Op::Complete { read, found, awaiting }),
        1 => any::<usize>().prop_map(|read| Op::Panic { read }),
        3 => prop_oneof![
            0..4_u64,
            0..2_000_u64,
            0..30_000_u64,
        ]
        .prop_map(|millis| Op::Advance { millis }),
        2 => (0..PEERS, any::<bool>(), any::<bool>())
            .prop_map(|(peer, sent, live)| Op::Close { peer, sent, live }),
        2 => Just(Op::Deadlines),
        3 => (0..3_u8).prop_map(|key| Op::CachedPass { key }),
        1 => (0..4_u8).prop_map(|version| Op::ResourceStall { version }),
        1 => (0..4_u8).prop_map(|version| Op::Commit { version }),
        1 => (0..PEERS).prop_map(|peer| Op::Rotate { peer }),
        1 => proptest::option::of((0..3_u8, 0..2_u8)).prop_map(|desired| Op::Repair { desired }),
        2 => any::<bool>().prop_map(|supplier| Op::RepairStall { supplier }),
        2 => Just(Op::RefreshStall),
    ]
}

fn repair_owner() -> zakura_header_chain::BodyWorkOwner {
    let anchor = zakura_header_chain::Frontier::new(block::Height(0), hash(0xC0, 0));
    zakura_header_chain::BodyWorkAuthority::for_snapshot(&committed_snapshot(anchor))
        .bind(1, std::num::NonZeroU64::new(1).expect("one is nonzero"))
}

fn repair(generation: u8, height: u8) -> RepairRequirement {
    RepairRequirement::new(
        repair_owner(),
        block::Height(u32::from(height) + 1),
        u64::from(generation),
    )
}

fn predecessor() -> zakura_header_chain::Frontier {
    zakura_header_chain::Frontier::new(block::Height(0), hash(0xC1, 0))
}

/// The model's view of one admitted session.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ModelSession {
    id: u64,
    status: Option<u8>,
    spent: bool,
}

/// The model's stall record for one repair generation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ModelStall {
    generation: u8,
    since: Instant,
    last_trace: Option<Instant>,
    reported: bool,
}

impl ModelStall {
    fn next_deadline(self) -> Instant {
        let trace_at = self
            .last_trace
            .map_or(self.since, |last| last + VCT_REPAIR_STALL_TRACE_INTERVAL);
        if self.reported {
            trace_at
        } else {
            trace_at.min(self.since + VCT_REPAIR_STALL_REPORT_AFTER)
        }
    }
}

/// One pending read in the model, by its index in [`Harness::started`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ModelRead {
    read: usize,
    authority: Authority,
}

#[derive(Debug)]
struct Model {
    sessions: BTreeMap<usize, ModelSession>,
    next_session: u64,
    pending: BTreeMap<usize, ModelRead>,
    revision: u64,
    /// Refused `Close`s: session ID and retry time.
    close_retries: BTreeMap<usize, (u64, Instant)>,
    /// Sent `Close`s on live subscriptions: session ID and outcome deadline.
    close_outcomes: BTreeMap<usize, (u64, Instant)>,
    /// Repair supplier order, front first.
    suppliers: VecDeque<usize>,
    /// Newest state version that a resource stall named.
    stalled_at: Option<u8>,
    committed: Option<u8>,
    /// Scheduling key of the last finished cached pass.
    cached_key: Option<u8>,
    cached_deferred: bool,
    cached_granted: u64,
    last_cached_grant: Option<Instant>,
    repair: Option<(u8, u8)>,
    stall: Option<ModelStall>,
    /// Error reports per repair generation.
    reports: BTreeMap<u8, u32>,
}

impl Model {
    fn new() -> Self {
        Self {
            sessions: BTreeMap::new(),
            next_session: 0,
            pending: BTreeMap::new(),
            revision: 0,
            close_retries: BTreeMap::new(),
            close_outcomes: BTreeMap::new(),
            suppliers: VecDeque::new(),
            stalled_at: None,
            committed: None,
            cached_key: None,
            cached_deferred: false,
            cached_granted: 0,
            last_cached_grant: None,
            repair: None,
            stall: None,
            reports: BTreeMap::new(),
        }
    }

    fn move_to_back(&mut self, peer: usize) {
        if let Some(index) = self.suppliers.iter().position(|queued| *queued == peer) {
            self.suppliers.remove(index);
            self.suppliers.push_back(peer);
        }
    }

    /// The entries of `entries` due by `now`, removed, in peer order.
    fn take_due(entries: &mut BTreeMap<usize, (u64, Instant)>, now: Instant) -> Vec<(usize, u64)> {
        let due: Vec<_> = entries
            .iter()
            .filter(|(_, (_, at))| *at <= now)
            .map(|(peer, (session, _))| (*peer, *session))
            .collect();
        for (peer, _) in &due {
            entries.remove(peer);
        }
        due
    }

    /// The disposition the existing locator rules give a read under
    /// `query` for a peer awaiting `awaiting`.
    fn disposition(
        query: Authority,
        awaiting: Option<Authority>,
        found: bool,
    ) -> LocatorDisposition {
        let Some(awaiting) = awaiting else {
            return LocatorDisposition::Stale;
        };
        let authority_moved =
            awaiting.generation != query.generation || awaiting.anchor != query.anchor;
        if authority_moved || (!found && awaiting.tip != query.tip) {
            LocatorDisposition::Refetch
        } else {
            LocatorDisposition::Ready
        }
    }
}

/// The machine, the model, the virtual clock, and every started read's key.
struct Harness {
    state: ReactorState,
    summary: HeaderReactorReplaySummary,
    model: Model,
    started: Vec<(usize, LocatorKey)>,
    start: Instant,
    now: Instant,
    /// The trace and report flags of the last [`Op::RefreshStall`] output.
    last_stall_due: Option<(bool, bool)>,
}

impl Harness {
    fn new() -> Self {
        let start = Instant::now();
        Self {
            state: ReactorState::new(start),
            summary: HeaderReactorReplaySummary::default(),
            model: Model::new(),
            started: Vec::new(),
            start,
            now: start,
            last_stall_due: None,
        }
    }

    fn peer_index(&self, peer: &ZakuraPeerId) -> usize {
        (0..PEERS)
            .find(|index| peer_id(*index) == *peer)
            .expect("the machine only names test peers")
    }

    fn indexed(&self, entries: Vec<(ZakuraPeerId, u64)>) -> Vec<(usize, u64)> {
        let mut entries: Vec<_> = entries
            .into_iter()
            .map(|(peer, session)| (self.peer_index(&peer), session))
            .collect();
        entries.sort_unstable();
        entries
    }

    fn apply(&mut self, op: Op) {
        self.summary.transitions += 1;
        match op {
            Op::Connect { peer } => {
                self.summary.admissions += 1;
                let replaced = self.state.admit_session(&peer_id(peer));
                if replaced {
                    self.state
                        .requeue_supplier(source_id_from_peer(&peer_id(peer)));
                }
                self.model.next_session += 1;
                let previous = self.model.sessions.insert(
                    peer,
                    ModelSession {
                        id: self.model.next_session,
                        ..ModelSession::default()
                    },
                );
                self.model.revision += 1;
                self.model.close_retries.remove(&peer);
                self.model.close_outcomes.remove(&peer);
                if previous.is_some() {
                    self.model.move_to_back(peer);
                } else {
                    self.model.suppliers.push_back(peer);
                }
                assert_eq!(replaced, previous.is_some(), "{PROPERTY}");
            }
            Op::Disconnect { peer } => {
                self.state.remove_session(&peer_id(peer));
                if self.model.sessions.remove(&peer).is_some() {
                    self.model.revision += 1;
                }
                self.model.close_retries.remove(&peer);
                self.model.close_outcomes.remove(&peer);
                self.model.suppliers.retain(|queued| *queued != peer);
            }
            Op::Status { peer, tip } => {
                self.state.record_status(&peer_id(peer), status(tip));
                if let Some(session) = self.model.sessions.get_mut(&peer) {
                    session.status = Some(tip);
                    session.spent = false;
                }
            }
            Op::RequestLocator {
                peer,
                authority,
                capacity,
            } => {
                // The reactor requests reads only for admitted sessions.
                let Some(session) = self.model.sessions.get_mut(&peer) else {
                    return;
                };
                // Staging spends the offer before dispatch.
                self.state.spend_status(&peer_id(peer));
                session.spent = true;
                let proposal = self.state.locators.propose(
                    &peer_id(peer),
                    authority.scope(),
                    &mut self.state.calls,
                );
                if let Some(pending) = self.model.pending.get(&peer) {
                    let (_, joined) = &self.started[pending.read];
                    assert_eq!(proposal, LocatorProposal::Joined(joined.call), "{PROPERTY}");
                    return;
                }
                let LocatorProposal::Start(key) = proposal else {
                    panic!("{PROPERTY}: a peer without a read must start one");
                };
                assert_eq!(&key.peer, &peer_id(peer), "{PROPERTY}");
                if capacity {
                    self.summary.reads_started += 1;
                    self.model.pending.insert(
                        peer,
                        ModelRead {
                            read: self.started.len(),
                            authority,
                        },
                    );
                    self.state.calls.accept(key.call);
                    self.started.push((peer, key));
                } else {
                    // A refusal for local capacity keeps the offer for a later pass.
                    self.summary.reads_refused += 1;
                    self.state.refuse_locator(&key);
                    session.spent = false;
                    self.model.revision += 1;
                }
            }
            Op::Complete {
                read,
                found,
                awaiting,
            } => {
                let Some((peer, key)) = self.pick(read) else {
                    return;
                };
                let target = awaiting.map(|authority| authority.target(0));
                let was_pending = self
                    .model
                    .pending
                    .get(&peer)
                    .is_some_and(|pending| pending.read == read % self.started.len());
                assert_eq!(
                    self.state.calls.settle(key.call).is_ok(),
                    was_pending,
                    "{PROPERTY}: only the pending call settles"
                );
                let disposition = self.state.locators.settle(&key, target.as_ref(), found);
                let expected = match self.model.pending.get(&peer) {
                    Some(pending) if pending.read == read % self.started.len() => {
                        let pending = self
                            .model
                            .pending
                            .remove(&peer)
                            .expect("the pending read was just found");
                        Model::disposition(pending.authority, awaiting, found)
                    }
                    // A late completion for a settled read leaves the current read pending.
                    _ => LocatorDisposition::Stale,
                };
                self.summary.completions += 1;
                self.summary.stale_completions +=
                    usize::from(expected == LocatorDisposition::Stale);
                assert_eq!(disposition, expected, "{PROPERTY}");
            }
            Op::Panic { read } => {
                let Some((peer, key)) = self.pick(read) else {
                    return;
                };
                let removed = self.state.locators.remove_exact(&key);
                let matches = self
                    .model
                    .pending
                    .get(&peer)
                    .is_some_and(|pending| pending.read == read % self.started.len());
                assert_eq!(
                    self.state.calls.settle(key.call).is_ok(),
                    matches,
                    "{PROPERTY}: only the pending call settles"
                );
                if matches {
                    self.model.pending.remove(&peer);
                }
                assert_eq!(removed.is_some(), matches, "{PROPERTY}");
            }
            Op::Advance { millis } => {
                self.now += Duration::from_millis(millis);
            }
            Op::Close { peer, sent, live } => {
                // The reactor closes only admitted sessions.
                let Some(session) = self.model.sessions.get(&peer).map(|session| session.id) else {
                    return;
                };
                self.summary.close_refusals += usize::from(!sent);
                let attempt = if sent {
                    CloseAttempt::Sent { live }
                } else {
                    CloseAttempt::Refused
                };
                self.state.record_close_attempt(
                    &peer_id(peer),
                    session,
                    attempt,
                    self.now,
                    OUTCOME_TIMEOUT,
                );
                if !sent {
                    // A retry already waiting for this session keeps its time.
                    if self
                        .model
                        .close_retries
                        .get(&peer)
                        .is_none_or(|(waiting, _)| *waiting != session)
                    {
                        self.model
                            .close_retries
                            .insert(peer, (session, self.now + CLOSE_RETRY_INTERVAL));
                    }
                } else {
                    self.model.close_retries.remove(&peer);
                    if live
                        && self
                            .model
                            .close_outcomes
                            .get(&peer)
                            .is_none_or(|(waiting, _)| *waiting != session)
                    {
                        self.model
                            .close_outcomes
                            .insert(peer, (session, self.now + OUTCOME_TIMEOUT));
                    }
                }
            }
            Op::Deadlines => {
                let due = self.state.take_due_closes(self.now);
                let due = self.indexed(due);
                let expired = self.state.take_expired_closes(self.now);
                let expired = self.indexed(expired);
                self.summary.close_retries += due.len();
                self.summary.close_expiries += expired.len();
                assert!(due.len() <= PEERS && expired.len() <= PEERS, "{PROPERTY}");
                assert_eq!(
                    due,
                    Model::take_due(&mut self.model.close_retries, self.now),
                    "{PROPERTY}"
                );
                assert_eq!(
                    expired,
                    Model::take_due(&mut self.model.close_outcomes, self.now),
                    "{PROPERTY}"
                );
            }
            Op::CachedPass { key } => {
                let opportunity = SchedulingOpportunity {
                    policy_revision: u64::from(key),
                    queue_revision: 0,
                    claimed_headers: 0,
                    available_locators: 0,
                    resource_stalled: false,
                };
                // The reactor's cached-pass gate, in order.
                let wanted = self.state.cached_pass_wanted(opportunity);
                self.model.cached_deferred = false;
                assert_eq!(wanted, self.model.cached_key != Some(key), "{PROPERTY}");
                if !wanted {
                    return self.check();
                }
                let taken = self.state.take_cached_pass(self.now);
                // A pass is always available one refill after the last one.
                if self
                    .model
                    .last_cached_grant
                    .is_none_or(|last| self.now >= last + CACHED_PASS_REFILL)
                {
                    assert!(taken, "{PROPERTY}: the budget refills between passes");
                }
                if taken {
                    self.summary.cached_passes += 1;
                    self.state.finish_cached_pass(opportunity, self.now);
                    self.model.cached_key = Some(key);
                    self.model.cached_granted += 1;
                    self.model.last_cached_grant = Some(self.now);
                } else {
                    self.model.cached_deferred = true;
                }
                let refills = self.now.saturating_duration_since(self.start).as_nanos()
                    / CACHED_PASS_REFILL.as_nanos();
                assert!(
                    u128::from(self.model.cached_granted)
                        <= u128::from(CACHED_PASS_BURST) + refills,
                    "{PROPERTY}: cached passes stay within the burst plus refills"
                );
            }
            Op::ResourceStall { version } => {
                self.state
                    .note_resource_stall(zakura_header_chain::StateVersion::new(u64::from(
                        version,
                    )));
                self.model.stalled_at = self.model.stalled_at.max(Some(version));
            }
            Op::Commit { version } => {
                self.model.committed = Some(version);
            }
            Op::Rotate { peer } => {
                self.state
                    .rotate_supplier(source_id_from_peer(&peer_id(peer)));
                self.model.move_to_back(peer);
            }
            Op::Repair { desired } => {
                // The reactor's repair adoption, in order.
                match desired {
                    Some((generation, height)) => {
                        let desired = repair(generation, height);
                        if !self.state.preserves_repair(&desired) {
                            let _ = self.state.retire_repair();
                            self.state.install_repair(desired);
                        }
                    }
                    None => {
                        let _ = self.state.retire_repair();
                    }
                }
                if desired.is_none() || self.model.repair != desired {
                    self.model.stall = None;
                }
                self.model.repair = desired;
            }
            Op::RepairStall { supplier } => {
                let Some((generation, _)) = self.model.repair else {
                    return;
                };
                let outcome = if supplier {
                    VctRepairStallOutcome::SupplierFailure
                } else {
                    VctRepairStallOutcome::LocalFailure
                };
                self.state.record_repair_stall(
                    u64::from(generation),
                    predecessor(),
                    VctSupplierRejections::default(),
                    outcome,
                    self.now,
                );
                if self
                    .model
                    .stall
                    .is_none_or(|stall| stall.generation != generation)
                {
                    self.model.stall = Some(ModelStall {
                        generation,
                        since: self.now,
                        last_trace: None,
                        reported: false,
                    });
                }
            }
            Op::RefreshStall => {
                let due = self.state.refresh_repair_stall(self.now).map(|due| {
                    (
                        due.trace,
                        due.report,
                        due.stalled_for,
                        due.best_peer_tip.map(|(height, _)| height),
                    )
                });
                assert_eq!(due, self.model_refresh_stall(), "{PROPERTY}");
                self.last_stall_due = due.map(|(trace, report, _, _)| (trace, report));
            }
        }
        self.check()
    }

    /// The model's stall output at `now`: trace, report, stall age, and best peer height.
    fn model_refresh_stall(&mut self) -> Option<(bool, bool, Duration, Option<block::Height>)> {
        let mut stall = self.model.stall?;
        if self.model.repair.map(|(generation, _)| generation) != Some(stall.generation) {
            self.model.stall = None;
            return None;
        }
        let stalled_for = self.now.saturating_duration_since(stall.since);
        let trace = stall.last_trace.is_none_or(|last| {
            self.now.saturating_duration_since(last) >= VCT_REPAIR_STALL_TRACE_INTERVAL
        });
        let report = !stall.reported && stalled_for >= VCT_REPAIR_STALL_REPORT_AFTER;
        if !trace && !report {
            return None;
        }
        if trace {
            stall.last_trace = Some(self.now);
        }
        if report {
            self.summary.repair_reports += 1;
            stall.reported = true;
            let reports = self.model.reports.entry(stall.generation).or_default();
            *reports += 1;
            assert_eq!(
                *reports, 1,
                "{PROPERTY}: one error report per repair generation"
            );
        }
        self.model.stall = Some(stall);
        let best_peer_height = self
            .model
            .sessions
            .values()
            .filter_map(|session| session.status)
            .max()
            .map(|tip| status(tip).selected_tip_height);
        Some((trace, report, stalled_for, best_peer_height))
    }

    /// The started read that `read` selects, if any read started.
    fn pick(&self, read: usize) -> Option<(usize, LocatorKey)> {
        if self.started.is_empty() {
            return None;
        }
        Some(self.started[read % self.started.len()].clone())
    }

    /// Compare every observable fact of the machine with the model.
    fn check(&self) {
        assert_eq!(
            self.state.scheduling_revision, self.model.revision,
            "{PROPERTY}"
        );
        assert_eq!(
            self.state.peers.len(),
            self.model.sessions.len(),
            "{PROPERTY}"
        );
        for peer in 0..PEERS {
            let id = peer_id(peer);
            let schedule = self.state.peers.get(&id).map(|schedule| {
                (
                    schedule
                        .last_status
                        .as_ref()
                        .map(|status| status.selected_tip_hash),
                    schedule.status_spent,
                    schedule.unproductive_requests,
                )
            });
            let expected = self.model.sessions.get(&peer).map(|session| {
                (
                    session.status.map(|tip| status(tip).selected_tip_hash),
                    session.spent,
                    0,
                )
            });
            assert_eq!(schedule, expected, "{PROPERTY}: peer {} schedule", peer);
            assert_eq!(
                self.state.locators.contains(&id),
                self.model.pending.contains_key(&peer),
                "{PROPERTY}: peer {} pending read",
                peer
            );
        }
        let mut expected_calls: Vec<_> = self
            .model
            .pending
            .values()
            .map(|pending| self.started[pending.read].1.call)
            .collect();
        expected_calls.sort_unstable_by_key(|call| call.number);
        assert_eq!(
            self.state.calls.pending(),
            expected_calls,
            "{PROPERTY}: accepted locator calls"
        );
        let suppliers: Vec<_> = self
            .state
            .vct_supplier_order
            .iter()
            .map(|peer| self.peer_index(peer))
            .collect();
        assert_eq!(
            &suppliers,
            &Vec::from(self.model.suppliers.clone()),
            "{PROPERTY}"
        );
        let committed = self
            .model
            .committed
            .map(|version| zakura_header_chain::StateVersion::new(u64::from(version)));
        let paused = self.model.stalled_at.is_some_and(|stalled| {
            self.model
                .committed
                .is_none_or(|version| version <= stalled)
        });
        assert_eq!(
            self.state.ordinary_discovery_paused(committed),
            paused,
            "{PROPERTY}"
        );
        assert_eq!(
            self.state
                .vct_repair
                .current()
                .map(|task| (task.repair_generation, task.height)),
            self.model.repair.map(|(generation, height)| (
                u64::from(generation),
                block::Height(u32::from(height) + 1)
            )),
            "{PROPERTY}"
        );
        assert_eq!(
            self.state.vct_repair_stall.map(|stall| (
                stall.generation,
                stall.since,
                stall.last_trace,
                stall.reported
            )),
            self.model.stall.map(|stall| (
                u64::from(stall.generation),
                stall.since,
                stall.last_trace,
                stall.reported
            )),
            "{PROPERTY}"
        );
        // Every deadline the model knows, except the cached-pass refill.
        let deadline = self
            .model
            .close_retries
            .values()
            .chain(self.model.close_outcomes.values())
            .map(|(_, at)| *at)
            .chain(self.model.stall.map(ModelStall::next_deadline))
            .min();
        if self.model.cached_deferred {
            // A deferred pass wakes the reactor within one refill.
            let refill = self.now + CACHED_PASS_REFILL;
            let next = self.state.next_deadline();
            assert!(next.is_some_and(|next| next <= refill), "{PROPERTY}");
            assert_eq!(
                next,
                deadline.map_or(next, |deadline| next.min(Some(deadline))),
                "{PROPERTY}"
            );
        } else {
            assert_eq!(self.state.next_deadline(), deadline, "{PROPERTY}");
        }
    }
}

/// Run `ops` from an empty state, checking the machine after every step.
fn run_with(
    ops: impl IntoIterator<Item = Op>,
    mut after_step: impl FnMut(&mut Harness),
) -> Harness {
    let mut harness = Harness::new();
    for op in ops {
        harness.apply(op);
        after_step(&mut harness);
        harness.check();
    }
    harness
}

#[test]
fn late_locator_after_session_replacement_leaves_the_replacement_unchanged() {
    let authority = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::Status { peer: 0, tip: 1 },
        Op::RequestLocator {
            peer: 0,
            authority,
            capacity: true,
        },
        // The replacement drops the first session's awaiting target.
        Op::Connect { peer: 0 },
        Op::Status { peer: 0, tip: 2 },
    ]);
    let replacement = harness.state.peers[&peer_id(0)].clone();
    let revision = harness.state.scheduling_revision;

    harness.apply(Op::Complete {
        read: 0,
        found: true,
        awaiting: None,
    });

    assert!(!harness.state.locators.contains(&peer_id(0)));
    assert_eq!(harness.state.peers[&peer_id(0)], replacement);
    assert_eq!(harness.state.scheduling_revision, revision);
}

#[test]
fn locator_requests_for_a_replacement_join_the_first_sessions_read() {
    let first = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let replacement = Authority { tip: 2, ..first };
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::RequestLocator {
            peer: 0,
            authority: first,
            capacity: true,
        },
        Op::Connect { peer: 0 },
        // The replacement's request joins the read already in flight.
        Op::RequestLocator {
            peer: 0,
            authority: replacement,
            capacity: true,
        },
    ]);
    assert_eq!(harness.started.len(), 1);

    // The read follows the local selected path, so it serves the replacement's
    // target under the same generation and anchor.
    let (_, key) = harness.started[0].clone();
    let target = replacement.target(2);
    assert_eq!(
        harness.state.locators.settle(&key, Some(&target), true),
        LocatorDisposition::Ready
    );
}

#[test]
fn refused_locator_dispatch_keeps_the_offer() {
    let authority = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let mut harness = run([Op::Connect { peer: 0 }, Op::Status { peer: 0, tip: 1 }]);
    let revision = harness.state.scheduling_revision;

    harness.apply(Op::RequestLocator {
        peer: 0,
        authority,
        capacity: false,
    });

    let schedule = &harness.state.peers[&peer_id(0)];
    assert!(!schedule.status_spent, "a capacity refusal keeps the offer");
    assert!(schedule.last_status.is_some());
    assert!(!harness.state.locators.contains(&peer_id(0)));
    assert_eq!(harness.state.scheduling_revision, revision + 1);

    // The next request starts a fresh read.
    harness.apply(Op::RequestLocator {
        peer: 0,
        authority,
        capacity: true,
    });
    assert!(harness.state.locators.contains(&peer_id(0)));
    assert!(harness.state.peers[&peer_id(0)].status_spent);
}

#[test]
fn late_completion_keeps_a_newer_read_for_the_same_peer() {
    let authority = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::RequestLocator {
            peer: 0,
            authority,
            capacity: true,
        },
        Op::Complete {
            read: 0,
            found: true,
            awaiting: Some(authority),
        },
        Op::RequestLocator {
            peer: 0,
            authority,
            capacity: true,
        },
    ]);

    let (_, settled) = harness.started[0].clone();
    assert_eq!(
        harness
            .state
            .locators
            .settle(&settled, Some(&authority.target(1)), true),
        LocatorDisposition::Stale
    );
    assert!(harness.state.locators.contains(&peer_id(0)));
}

#[test]
fn refused_closes_under_queue_pressure_keep_their_retry_time() {
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::Close {
            peer: 0,
            sent: false,
            live: true,
        },
    ]);
    let first_retry = harness.now + CLOSE_RETRY_INTERVAL;

    // Steady refusals for the same session cannot postpone the retry.
    for _ in 0..10 {
        harness.apply(Op::Advance { millis: 110 });
        harness.apply(Op::Close {
            peer: 0,
            sent: false,
            live: true,
        });
    }
    assert!(harness.now >= first_retry);
    assert_eq!(
        harness.state.take_due_closes(harness.now),
        vec![(peer_id(0), 1)],
        "the first refusal's retry is due"
    );
    assert!(harness.state.take_due_closes(harness.now).is_empty());
}

#[test]
fn repair_replacement_ends_the_previous_generations_stall() {
    let mut harness = run([
        Op::Repair {
            desired: Some((0, 0)),
        },
        Op::RepairStall { supplier: true },
        Op::Advance { millis: 61_000 },
        // The same repair keeps its stall record.
        Op::Repair {
            desired: Some((0, 0)),
        },
    ]);
    assert!(harness.state.vct_repair_stall.is_some());

    harness.apply(Op::Repair {
        desired: Some((1, 0)),
    });
    assert!(harness.state.vct_repair_stall.is_none());
    assert_eq!(
        harness
            .state
            .vct_repair
            .current()
            .map(|task| task.repair_generation),
        Some(1)
    );
    assert!(harness.state.refresh_repair_stall(harness.now).is_none());
}

#[test]
fn resource_stall_holds_until_a_newer_state_version() {
    let version = |version| Some(zakura_header_chain::StateVersion::new(version));
    let mut harness = run([Op::ResourceStall { version: 2 }]);
    assert!(harness.state.ordinary_discovery_paused(None));
    assert!(harness.state.ordinary_discovery_paused(version(2)));
    assert!(!harness.state.ordinary_discovery_paused(version(3)));

    // An older receipt cannot lower the stall's state version.
    harness.apply(Op::ResourceStall { version: 1 });
    assert!(harness.state.ordinary_discovery_paused(version(2)));
}

#[test]
fn repeated_status_and_deadline_events_stay_bounded() {
    let mut harness = run([Op::Connect { peer: 0 }, Op::Connect { peer: 1 }]);
    for round in 0..1_000_u32 {
        let key = u8::try_from(round % 3).expect("the key is small");
        for op in [
            Op::Status { peer: 0, tip: key },
            Op::CachedPass { key },
            Op::Close {
                peer: 1,
                sent: false,
                live: true,
            },
            Op::Deadlines,
        ] {
            harness.apply(op);
        }
    }
    // No time passed, so only the burst ran.
    assert_eq!(harness.model.cached_granted, u64::from(CACHED_PASS_BURST));
    assert_eq!(harness.state.pending_closes.len(), 1);

    // Each refill interval then buys one pass, however many keys change.
    for round in 0..100_u8 {
        harness.apply(Op::Advance { millis: 1 });
        for key in [round % 3, (round + 1) % 3] {
            harness.apply(Op::CachedPass { key });
        }
    }
    assert_eq!(
        harness.model.cached_granted,
        u64::from(CACHED_PASS_BURST) + 100
    );
}

#[test]
fn a_stalled_repair_reports_once_and_keeps_tracing() {
    let mut harness = run([
        Op::Repair {
            desired: Some((0, 0)),
        },
        Op::RepairStall { supplier: false },
        Op::RefreshStall,
    ]);
    let due = |harness: &mut Harness, millis| {
        harness.apply(Op::Advance { millis });
        harness.apply(Op::RefreshStall);
        harness.last_stall_due
    };

    assert_eq!(
        due(&mut harness, 5_000),
        None,
        "the trace interval has not passed"
    );
    assert_eq!(due(&mut harness, 55_000), Some((true, true)));
    assert_eq!(due(&mut harness, 1_000), None);
    assert_eq!(
        due(&mut harness, 10_000),
        Some((true, false)),
        "the generation reports at error level once"
    );
}

#[test]
fn supplier_order_is_deterministic() {
    let order = |harness: &Harness| -> Vec<usize> {
        harness
            .state
            .vct_supplier_order
            .iter()
            .map(|peer| harness.peer_index(peer))
            .collect()
    };
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::Connect { peer: 1 },
        Op::Connect { peer: 2 },
    ]);
    assert_eq!(order(&harness), [0, 1, 2]);

    for (op, expected) in [
        (Op::Rotate { peer: 0 }, [1, 2, 0]),
        // A replacement session moves to the back.
        (Op::Connect { peer: 1 }, [2, 0, 1]),
        (Op::Rotate { peer: 0 }, [2, 1, 0]),
    ] {
        harness.apply(op);
        assert_eq!(order(&harness), expected);
    }
    harness.apply(Op::Disconnect { peer: 2 });
    assert_eq!(order(&harness), [1, 0]);
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn reactor_state_matches_the_model(ops in prop::collection::vec(op(), 1..64)) {
        run(ops);
    }
}

#[cfg(test)]
fn run(ops: impl IntoIterator<Item = Op>) -> Harness {
    run_with(ops, |_| {})
}

pub(super) fn committed_snapshot(
    anchor: zakura_header_chain::Frontier,
) -> zakura_header_chain::EngineSnapshot {
    zakura_header_chain::EngineSnapshot {
        mode: zakura_header_chain::EngineMode::Integrated,
        state_version: zakura_header_chain::StateVersion::new(3),
        header_generation: zakura_header_chain::HeaderGeneration::new(4),
        verified_generation: zakura_header_chain::VerifiedGeneration::new(5),
        frontiers: zakura_header_chain::FrontierSet {
            finalized: anchor,
            header_best: anchor,
            verified_best: anchor,
        },
        header_best_score: zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            anchor.hash,
        ),
        oldest_retained_height: anchor.height,
        alarms: Default::default(),
    }
}

const PROPERTY: &str = "header/reactor-policy";

/// Applicable transitions in a decoder-v1 header reactor history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct HeaderReactorReplaySummary {
    /// Operations decoded.
    pub transitions: usize,
    /// Session admissions checked.
    pub admissions: usize,
    /// Locator reads started.
    pub reads_started: usize,
    /// Locator dispatches refused for capacity.
    pub reads_refused: usize,
    /// Locator completions checked.
    pub completions: usize,
    /// Locator completions rejected as stale.
    pub stale_completions: usize,
    /// Close dispatches refused by an admitted session.
    pub close_refusals: usize,
    /// Close retries drained at their deadline.
    pub close_retries: usize,
    /// Close outcome deadlines expired.
    pub close_expiries: usize,
    /// Cached-offer passes granted.
    pub cached_passes: usize,
    /// Repair generations reported after a sustained stall.
    pub repair_reports: usize,
}

/// Replay decoder-v1 quadruples against the existing header reactor model.
/// Read at most 512 bytes and keep every deadline relative to one clock origin.
pub fn replay_header_reactor_bytes(data: &[u8]) -> HeaderReactorReplaySummary {
    run_with(decode(data), |_| {}).summary
}

/// Where a header machine driver stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderMachinePhase {
    /// Applying the input and checking its immediate outcome.
    Transition,
    /// Comparing the resulting owner with the independent model.
    Invariant,
}

/// The first failure of a header machine history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderMachineFailure {
    /// Whether the independent checker named its property.
    pub property_failure: bool,
    /// Where execution stopped.
    pub phase: HeaderMachinePhase,
    /// Panic payload without the panic hook's source location.
    pub message: String,
}

impl HeaderMachineFailure {
    /// The failure a driver's panic `payload` reports in `phase`, for `property`.
    pub(in crate::zakura::header_sync) fn of_panic(
        property: &str,
        phase: HeaderMachinePhase,
        payload: &(dyn std::any::Any + Send),
    ) -> Self {
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
            })
            .unwrap_or_else(|| "non-string panic payload".to_owned());
        Self {
            property_failure: message == property
                || message.starts_with(&format!("{property}:"))
                || (message.starts_with("assertion")
                    && message.contains(&format!("failed: {property}"))),
            phase,
            message,
        }
    }
}

/// One admitted session in a [`HeaderReactorView`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderReactorSessionView {
    /// The peer's model index.
    pub peer: usize,
    /// The selected tip of the peer's last status.
    pub status: Option<block::Hash>,
    /// Whether the status is spent.
    pub spent: bool,
}

/// One pending locator read in a [`HeaderReactorView`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderReactorReadView {
    /// The peer's model index.
    pub peer: usize,
    /// The read's exact call: its number and kind.
    pub call: (u64, StorageCallKind),
}

/// A reactor state view, read from the machine or derived by its model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderReactorView {
    /// Admitted sessions, by peer.
    pub sessions: Vec<HeaderReactorSessionView>,
    /// Pending locator reads, by peer.
    pub reads: Vec<HeaderReactorReadView>,
    /// Accepted storage calls that have not settled, by number.
    pub calls: Vec<(u64, StorageCallKind)>,
    /// The scheduling revision.
    pub revision: u64,
    /// Repair supplier order, front first.
    pub suppliers: Vec<usize>,
    /// Whether a resource stall pauses ordinary discovery.
    pub paused: bool,
    /// The current repair: its generation and height.
    pub repair: Option<(u64, u32)>,
    /// Whether a changed cached-pass key waits for its budget.
    pub cached_deferred: bool,
    /// Milliseconds from the clock origin to the next deadline. A deferred
    /// cached pass adds a refill the model bounds rather than predicts, so
    /// this holds `None` on both sides then.
    pub next_deadline_millis: Option<u64>,
}

/// One decoder-v1 operation with both views.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderReactorStep {
    /// The operation's number, from 1.
    pub step: usize,
    /// Original causal bytes, including a short final operation.
    pub input: Vec<u8>,
    /// State read from the production pure owner.
    pub actual: HeaderReactorView,
    /// State derived independently by the model.
    pub expected: HeaderReactorView,
    /// First failure, if this operation stopped the run.
    pub failure: Option<HeaderMachineFailure>,
}

/// A decoder-v1 reactor history. It stops at the first failure.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HeaderReactorHistory {
    /// Every applied operation, at most 128.
    pub steps: Vec<HeaderReactorStep>,
}

impl Harness {
    /// Milliseconds from the clock origin to `at`.
    fn millis(&self, at: Instant) -> u64 {
        u64::try_from(at.saturating_duration_since(self.start).as_millis())
            .expect("a bounded history's virtual time fits u64")
    }

    /// The state read from the production owner.
    fn actual_view(&self) -> HeaderReactorView {
        let sessions = (0..PEERS)
            .filter_map(|peer| {
                let schedule = self.state.peers.get(&peer_id(peer))?;
                Some(HeaderReactorSessionView {
                    peer,
                    status: schedule
                        .last_status
                        .as_ref()
                        .map(|status| status.selected_tip_hash),
                    spent: schedule.status_spent,
                })
            })
            .collect();
        let reads = (0..PEERS)
            .filter_map(|peer| {
                let (call, _) = self.state.locators.pending_read(&peer_id(peer))?;
                Some(HeaderReactorReadView {
                    peer,
                    call: (call.number, call.kind),
                })
            })
            .collect();
        let committed = self
            .model
            .committed
            .map(|version| zakura_header_chain::StateVersion::new(u64::from(version)));
        HeaderReactorView {
            sessions,
            reads,
            calls: self
                .state
                .calls
                .pending()
                .into_iter()
                .map(|call| (call.number, call.kind))
                .collect(),
            revision: self.state.scheduling_revision,
            suppliers: self
                .state
                .vct_supplier_order
                .iter()
                .map(|peer| self.peer_index(peer))
                .collect(),
            paused: self.state.ordinary_discovery_paused(committed),
            repair: self
                .state
                .vct_repair
                .current()
                .map(|task| (task.repair_generation, task.height.0)),
            cached_deferred: self.state.cached_pass_deferred,
            next_deadline_millis: (!self.state.cached_pass_deferred)
                .then(|| self.state.next_deadline())
                .flatten()
                .map(|at| self.millis(at)),
        }
    }

    /// The state the model derives. It reads nothing from the production owner
    /// except the call numbers `Harness::started` recorded when each read began.
    fn expected_view(&self) -> HeaderReactorView {
        let reads: Vec<_> = self
            .model
            .pending
            .iter()
            .map(|(peer, pending)| {
                let call = self.started[pending.read].1.call;
                HeaderReactorReadView {
                    peer: *peer,
                    call: (call.number, call.kind),
                }
            })
            .collect();
        let mut calls: Vec<_> = reads.iter().map(|read| read.call).collect();
        calls.sort_unstable_by_key(|(number, _)| *number);
        HeaderReactorView {
            sessions: self
                .model
                .sessions
                .iter()
                .map(|(peer, session)| HeaderReactorSessionView {
                    peer: *peer,
                    status: session.status.map(|tip| status(tip).selected_tip_hash),
                    spent: session.spent,
                })
                .collect(),
            reads,
            calls,
            revision: self.model.revision,
            suppliers: self.model.suppliers.iter().copied().collect(),
            paused: self.model.stalled_at.is_some_and(|stalled| {
                self.model
                    .committed
                    .is_none_or(|version| version <= stalled)
            }),
            repair: self
                .model
                .repair
                .map(|(generation, height)| (u64::from(generation), u32::from(height) + 1)),
            cached_deferred: self.model.cached_deferred,
            next_deadline_millis: (!self.model.cached_deferred)
                .then(|| {
                    self.model
                        .close_retries
                        .values()
                        .chain(self.model.close_outcomes.values())
                        .map(|(_, at)| *at)
                        .chain(self.model.stall.map(ModelStall::next_deadline))
                        .min()
                })
                .flatten()
                .map(|at| self.millis(at)),
        }
    }
}

/// Capture decoder-v1 operations without changing the decoder or starting a runtime.
/// Driver panics stop the history and stay distinct from property failures.
pub fn trace_header_reactor_bytes(data: &[u8]) -> HeaderReactorHistory {
    let input = &data[..data.len().min(512)];
    let mut harness = Harness::new();
    let mut steps = Vec::new();
    for (index, op) in decode(input).enumerate() {
        let step = index + 1;
        let applied = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| harness.apply(op)))
            .map_err(|payload| (HeaderMachinePhase::Transition, payload))
            .and_then(|()| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| harness.check()))
                    .map_err(|payload| (HeaderMachinePhase::Invariant, payload))
            });
        let failure = applied.err().map(|(phase, payload)| {
            HeaderMachineFailure::of_panic(PROPERTY, phase, payload.as_ref())
        });
        let stop = failure.is_some();
        steps.push(HeaderReactorStep {
            step,
            input: input[index * 4..(step * 4).min(input.len())].to_vec(),
            actual: harness.actual_view(),
            expected: harness.expected_view(),
            failure,
        });
        if stop {
            break;
        }
    }
    HeaderReactorHistory { steps }
}

fn decode(data: &[u8]) -> impl Iterator<Item = Op> + '_ {
    data[..data.len().min(512)].chunks(4).map(|chunk| {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        let d = chunk.get(3).copied().unwrap_or(0);
        let peer = usize::from(b) % PEERS;
        let authority = Authority {
            generation: c % 2,
            anchor: c / 2 % 2,
            tip: c / 4 % 3,
        };
        match a % 16 {
            0 => Op::Connect { peer },
            1 => Op::Disconnect { peer },
            2 => Op::Status { peer, tip: c % 3 },
            3 => Op::RequestLocator {
                peer,
                authority,
                capacity: d & 1 == 0,
            },
            4 => Op::Complete {
                read: usize::from(b),
                found: d & 1 == 0,
                awaiting: (d & 2 == 0).then_some(authority),
            },
            5 => Op::Panic {
                read: usize::from(b),
            },
            6 => Op::Advance {
                millis: u64::from(u16::from_le_bytes([c, d])),
            },
            7 => Op::Close {
                peer,
                sent: c & 1 != 0,
                live: d & 1 != 0,
            },
            8 => Op::Deadlines,
            9 => Op::CachedPass { key: b % 3 },
            10 => Op::ResourceStall { version: b % 4 },
            11 => Op::Commit { version: b % 4 },
            12 => Op::Rotate { peer },
            13 => Op::Repair {
                desired: (d & 1 == 0).then_some((b % 3, c % 2)),
            },
            14 => Op::RepairStall {
                supplier: b & 1 != 0,
            },
            _ => Op::RefreshStall,
        }
    })
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn header_reactor_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_header_reactor_bytes(&bytes);
    }
}

#[cfg(test)]
const WITNESS: &[u8] = &[
    0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 2, 3, 0, 0, 0,
    4, 0, 0, 0, 4, 1, 0, 0, 7, 0, 0, 1, 6, 0, 232, 3, 8, 0, 0, 0, 7, 0, 1, 1, 6, 0, 136, 19, 8, 0,
    0, 0, 9, 0, 0, 0, 10, 2, 0, 0, 11, 3, 0, 0, 13, 0, 0, 0, 14, 0, 0, 0, 15, 0, 0, 0, 6, 0, 96,
    234, 15, 0, 0, 0, 15, 0, 0, 0, 12, 0, 0, 0,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn header_reactor_witness_exercises_locator_deadlines_and_repair() {
    let summary = replay_header_reactor_bytes(WITNESS);
    assert_eq!(
        summary,
        HeaderReactorReplaySummary {
            transitions: 26,
            admissions: 2,
            reads_started: 2,
            reads_refused: 1,
            completions: 3,
            stale_completions: 2,
            close_refusals: 1,
            close_retries: 1,
            close_expiries: 1,
            cached_passes: 1,
            repair_reports: 1,
        }
    );
    assert_eq!(
        replay_header_reactor_bytes(&[]),
        HeaderReactorReplaySummary::default()
    );
    println!("tier=Machine target=header_reactor decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "header/reactor-policy")]
fn header_reactor_witness_rejects_lost_locator() {
    run_with(decode(WITNESS), |harness| {
        for (_, key) in &harness.started {
            harness.state.locators.remove_exact(key);
        }
    });
}

#[test]
#[should_panic(expected = "header/reactor-policy")]
fn header_reactor_witness_rejects_wrong_revision() {
    run_with(decode(WITNESS), |harness| {
        harness.state.scheduling_revision += 1;
    });
}

#[test]
#[should_panic(expected = "header/reactor-policy")]
fn header_reactor_witness_rejects_premature_close_retry() {
    run_with(decode(WITNESS), |harness| {
        harness
            .state
            .take_due_closes(harness.now + Duration::from_secs(100));
    });
}
