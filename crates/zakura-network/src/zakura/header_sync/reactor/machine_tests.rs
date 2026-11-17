//! Machine tests for [`ReactorState`].
//!
//! Each step drives `ReactorState` and the real [`repair_candidates`] choice
//! in the order the reactor calls them, with a virtual clock for every `now`.
//! After every step the harness checks [`ReactorState::assert_invariants`],
//! and it compares state only with facts that follow from the inputs it chose
//! and the outputs it took:
//!
//! - locator reads: a peer's second request joins its pending read; a started
//!   read stays pending until its exact completion or panic; a stale
//!   completion changes nothing; and each started call settles once;
//! - sessions: a status is recorded unspent, a request spends it, and a
//!   capacity refusal restores it; admissions, departures, and refusals move
//!   the scheduling revision, which never moves back;
//! - closes: a refused `Close` waits for a retry, and a sent `Close` on a live
//!   subscription waits for its outcome, each no later than its first attempt
//!   plus its interval; a drain takes exactly the entries due;
//! - suppliers: an admission joins the back of the supplier order, a rotation
//!   moves its peer to the back, and a departure keeps the others in order;
//!   each supplier pass keeps supplier order, closes no candidate, and counts
//!   each supplier once;
//! - cached passes: a pass runs only for a new key, stays within the burst
//!   plus refills, and is always granted one refill after the last;
//! - repair stalls: the stall clock starts at the generation's first failure,
//!   a trace row waits its interval, a report waits its delay and comes once
//!   per generation, and the next deadline comes no later than either.
//!
//! Named scenarios pin each rule with literal inputs and outputs. The saved
//! `header/reactor-policy` input pins decoder v1. Offer and page decisions run
//! in the `peer_work` target instead.

use std::{collections::BTreeMap, time::Duration};

#[cfg(test)]
use proptest::prelude::*;

use super::{state::*, *};
#[cfg(test)]
use crate::zakura::machine_test::machine_target_tests;
use crate::zakura::machine_test::{panic_text, with_property, MAX_HISTORY_BYTES};

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
        tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
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

/// The local limits of every repair supplier pass in these tests.
fn supplier_limits() -> RepairSupplierLimits {
    RepairSupplierLimits {
        header_bytes: COMMON_HEADER_BYTES,
        page_limits: PageLimits {
            max_headers: 16,
            max_bytes: 1_000_000,
            aux_schema_mask: AuxSchema::V1.mask_bit(),
        },
        local_capacity: 1,
    }
}

/// `task`, resolved to a one-header context, and ready for supplier assignment.
fn ready_repair(
    mut task: RepairRequirement,
) -> (RepairRequirement, zakura_header_chain::VctRepairContext) {
    let context = zakura_header_chain::VctRepairContext::unconstrained(
        zakura_header_chain::Frontier::new(task.height, hash(0xC2, 0)),
        zakura_header_chain::HeaderLocator::for_continuation(predecessor()),
        None,
    );
    let now = Instant::now();
    task.mark_context_requested(now, now)
        .expect("a new repair needs context");
    task.resolve(context.clone())
        .expect("the context names the repair's height");
    (task, context)
}

/// One repair supplier's facts in a supplier pass.
#[derive(Clone, Debug)]
struct Supplier {
    peer: usize,
    status: Status,
    serving: bool,
    live: bool,
    tried: bool,
    excluded: bool,
    waiting: bool,
}

impl Supplier {
    /// A supplier at `tip` with no reason to wait.
    fn new(peer: usize, tip: u8) -> Self {
        Self {
            peer,
            status: status(tip),
            serving: false,
            live: false,
            tried: false,
            excluded: false,
            waiting: false,
        }
    }
}

/// Run the supplier pass for a fresh copy of `task` over `suppliers`, after
/// the task records the tried, excluded, and waiting suppliers.
fn supplier_pass(task: RepairRequirement, suppliers: &[Supplier]) -> RepairCandidates {
    let (mut task, context) = ready_repair(task_with_new_state(task));
    for supplier in suppliers {
        let source = source_id_from_peer(&peer_id(supplier.peer));
        if supplier.excluded {
            task.excluded_input_sources.insert(source);
        }
        if supplier.tried {
            task.tried_sources.insert(source);
        }
        if supplier.waiting {
            let owner = task.owner;
            task.assign(owner, context.clone(), supplier.status.clone())
                .expect("the repair is ready");
            task.wait_for_supplier_status(source)
                .expect("the repair is assigned");
        }
    }
    let ids: Vec<_> = suppliers
        .iter()
        .map(|supplier| peer_id(supplier.peer))
        .collect();
    repair_candidates(
        &task,
        &context,
        supplier_limits(),
        suppliers
            .iter()
            .zip(&ids)
            .map(|(supplier, peer)| RepairSupplierFacts {
                peer,
                status: &supplier.status,
                serving: supplier.serving,
                subscription_live: supplier.live,
            }),
    )
}

/// The peer indexes of `pass`: its candidates, the peers it closes, and its
/// tally. Every candidate of a one-header repair requests one header.
fn pass_indexes(pass: RepairCandidates) -> (Vec<usize>, Vec<usize>, VctSupplierRejections) {
    let index = |peer: &ZakuraPeerId| {
        (0..usize::from(u8::MAX))
            .find(|index| peer_id(*index) == *peer)
            .expect("the pass only names test peers")
    };
    (
        pass.candidates
            .iter()
            .map(|candidate| {
                assert_eq!(
                    candidate.request_count, 1,
                    "{PROPERTY}: a one-header repair"
                );
                index(&candidate.peer)
            })
            .collect(),
        pass.closing.iter().map(index).collect(),
        pass.rejections,
    )
}

/// Check what every supplier pass over `suppliers` meets for a one-header
/// repair under a context that retains no source: each list keeps supplier
/// order and names each input peer once, no closed peer is a candidate, and
/// the tally counts each supplier once. A peer the pass closes already counts
/// in the tally as serving.
fn check_supplier_pass(suppliers: &[Supplier], pass: RepairCandidates) {
    let (candidates, closing, tally) = pass_indexes(pass);
    let in_supplier_order = |peers: &[usize]| {
        let mut rest = suppliers.iter();
        peers
            .iter()
            .all(|peer| rest.any(|supplier| supplier.peer == *peer))
    };
    assert!(
        in_supplier_order(&candidates),
        "{PROPERTY}: candidates keep supplier order, once each"
    );
    assert!(
        in_supplier_order(&closing),
        "{PROPERTY}: closes keep supplier order, once each"
    );
    assert!(
        candidates.iter().all(|peer| !closing.contains(peer)),
        "{PROPERTY}: a closed peer is no candidate"
    );
    let count = |count: usize| u64::try_from(count).expect("the test has few suppliers");
    assert_eq!(
        tally.considered,
        count(suppliers.len()),
        "{PROPERTY}: the pass considers every supplier"
    );
    assert_eq!(
        tally.retained_source + tally.send_failed,
        0,
        "{PROPERTY}: the pass neither retains nor sends"
    );
    let rejected = tally.below_target_height
        + tally.insufficient_capacity
        + tally.unsupported_schema
        + tally.already_serving
        + tally.excluded_input
        + tally.already_tried
        + tally.supplier_status_wait;
    assert_eq!(
        rejected + count(candidates.len()),
        tally.considered,
        "{PROPERTY}: the tally counts each supplier once"
    );
}

/// A copy of `task` that has not started asking for its context.
fn task_with_new_state(task: RepairRequirement) -> RepairRequirement {
    RepairRequirement::new(task.owner, task.height, task.repair_generation)
}

/// The current repair's stall, as the inputs and outputs record it: the time
/// of the generation's first failure, the time of its last trace output, and
/// whether it reported.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Stall {
    generation: u64,
    since: Instant,
    last_trace: Option<Instant>,
    reported: bool,
}

/// The machine, the virtual clock, and what the test sent and took.
struct Harness {
    state: ReactorState,
    summary: HeaderReactorReplaySummary,
    /// Each admitted peer's session ID, numbered by `Connect`.
    sessions: BTreeMap<usize, u64>,
    next_session: u64,
    /// The tip of each admitted session's last status.
    statuses: BTreeMap<usize, u8>,
    /// Every started read: its peer and key.
    started: Vec<(usize, LocatorKey)>,
    /// Each peer's pending read: its index in `started` and its authority.
    pending: BTreeMap<usize, (usize, Authority)>,
    /// Refused `Close`s owed a retry: session ID and first refusal.
    close_retries: BTreeMap<usize, (u64, Instant)>,
    /// Sent `Close`s on live subscriptions owed an outcome: session ID and
    /// first send.
    close_outcomes: BTreeMap<usize, (u64, Instant)>,
    /// The newest state version a resource stall named.
    stalled_at: Option<u8>,
    committed: Option<u8>,
    /// The key of the last finished cached pass.
    cached_key: Option<u8>,
    cached_granted: u64,
    last_cached_grant: Option<Instant>,
    stall: Option<Stall>,
    /// The scheduling revision before the last step.
    revision_before: u64,
    /// Whether the last step must move the scheduling revision.
    must_bump: bool,
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
            sessions: BTreeMap::new(),
            next_session: 0,
            statuses: BTreeMap::new(),
            started: Vec::new(),
            pending: BTreeMap::new(),
            close_retries: BTreeMap::new(),
            close_outcomes: BTreeMap::new(),
            stalled_at: None,
            committed: None,
            cached_key: None,
            cached_granted: 0,
            last_cached_grant: None,
            stall: None,
            revision_before: 0,
            must_bump: false,
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

    /// The entries of `entries` due by now, by peer.
    fn due(&self, entries: &HashMap<ZakuraPeerId, (u64, Instant)>) -> Vec<(usize, u64)> {
        self.indexed(
            entries
                .iter()
                .filter(|(_, (_, at))| *at <= self.now)
                .map(|(peer, (session, _))| (peer.clone(), *session))
                .collect(),
        )
    }

    fn supplier_order(&self) -> Vec<usize> {
        self.state
            .vct_supplier_order
            .iter()
            .map(|peer| self.peer_index(peer))
            .collect()
    }

    /// The current repair's generation and height.
    fn repair(&self) -> Option<(u64, u32)> {
        self.state
            .vct_repair
            .current()
            .map(|task| (task.repair_generation, task.height.0))
    }

    fn apply(&mut self, op: Op) {
        self.summary.transitions += 1;
        self.revision_before = self.state.scheduling_revision;
        self.must_bump = false;
        match op {
            Op::Connect { peer } => {
                self.summary.admissions += 1;
                let order = self.supplier_order();
                let replaced = self.state.admit_session(&peer_id(peer), Instant::now());
                if replaced {
                    self.state
                        .rotate_supplier(source_id_from_peer(&peer_id(peer)));
                }
                self.next_session += 1;
                let previous = self.sessions.insert(peer, self.next_session);
                self.statuses.remove(&peer);
                self.close_retries.remove(&peer);
                self.close_outcomes.remove(&peer);
                self.must_bump = true;
                assert_eq!(replaced, previous.is_some(), "{PROPERTY}");
                assert_eq!(
                    self.supplier_order(),
                    moved_to_back(&order, peer),
                    "{PROPERTY}: an admitted peer is the last supplier"
                );
                let schedule = &self.state.peers[&peer_id(peer)];
                assert!(
                    schedule.last_status.is_none() && !schedule.status_spent,
                    "{PROPERTY}: a new session starts without an offer"
                );
            }
            Op::Disconnect { peer } => {
                let order = self.supplier_order();
                self.state
                    .remove_session(&peer_id(peer), Instant::now(), Duration::ZERO, 0);
                self.must_bump = self.sessions.remove(&peer).is_some();
                self.statuses.remove(&peer);
                self.close_retries.remove(&peer);
                self.close_outcomes.remove(&peer);
                let rest: Vec<_> = order.into_iter().filter(|queued| *queued != peer).collect();
                assert_eq!(
                    self.supplier_order(),
                    rest,
                    "{PROPERTY}: a departure keeps the other suppliers in order"
                );
            }
            Op::Status { peer, tip } => {
                self.state.record_status(&peer_id(peer), status(tip));
                if self.sessions.contains_key(&peer) {
                    self.statuses.insert(peer, tip);
                    assert!(
                        !self.state.peers[&peer_id(peer)].status_spent,
                        "{PROPERTY}: a status is an unspent offer"
                    );
                }
            }
            Op::RequestLocator {
                peer,
                authority,
                capacity,
            } => {
                // The reactor requests reads only for admitted sessions.
                if !self.sessions.contains_key(&peer) {
                    return;
                }
                // Staging spends the offer before dispatch.
                self.state.spend_status(&peer_id(peer));
                let issued = self.started.last().map(|(_, key)| key.call.number);
                let proposal = self.state.locators.propose(
                    &peer_id(peer),
                    authority.scope(),
                    &mut self.state.calls,
                );
                if let Some((read, _)) = self.pending.get(&peer) {
                    let (_, joined) = &self.started[*read];
                    assert_eq!(proposal, LocatorProposal::Joined(joined.call), "{PROPERTY}");
                    return;
                }
                let LocatorProposal::Start(key) = proposal else {
                    panic!("{PROPERTY}: a peer without a read must start one");
                };
                assert_eq!(&key.peer, &peer_id(peer), "{PROPERTY}");
                assert!(
                    issued.is_none_or(|issued| key.call.number > issued),
                    "{PROPERTY}: each read gets a new call"
                );
                if capacity {
                    self.summary.reads_started += 1;
                    self.pending.insert(peer, (self.started.len(), authority));
                    self.state.calls.accept(key.call);
                    self.started.push((peer, key));
                    assert!(
                        self.state.peers[&peer_id(peer)].status_spent,
                        "{PROPERTY}: a started read spends the offer"
                    );
                } else {
                    self.summary.reads_refused += 1;
                    self.state.refuse_locator(&key);
                    self.must_bump = true;
                    assert!(
                        !self.state.peers[&peer_id(peer)].status_spent,
                        "{PROPERTY}: a capacity refusal keeps the offer"
                    );
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
                let index = read % self.started.len();
                let query = self
                    .pending
                    .get(&peer)
                    .filter(|(pending, _)| *pending == index)
                    .map(|(_, authority)| *authority);
                assert_eq!(
                    self.state.calls.settle(key.call),
                    query.is_some(),
                    "{PROPERTY}: only the pending call settles"
                );
                let target = awaiting.map(|authority| authority.target(0));
                let disposition = self.state.locators.settle(&key, target.as_ref(), found);
                self.summary.completions += 1;
                self.summary.stale_completions +=
                    usize::from(disposition == LocatorDisposition::Stale);
                let Some(query) = query else {
                    // A late completion for a settled read leaves the current read pending.
                    assert_eq!(disposition, LocatorDisposition::Stale, "{PROPERTY}");
                    return;
                };
                self.pending.remove(&peer);
                match awaiting {
                    None => assert_eq!(
                        disposition,
                        LocatorDisposition::Stale,
                        "{PROPERTY}: a peer that awaits nothing takes no read"
                    ),
                    Some(awaiting) if awaiting == query => assert_eq!(
                        disposition,
                        LocatorDisposition::Ready,
                        "{PROPERTY}: a read serves the target it was started for"
                    ),
                    Some(_) => assert_ne!(
                        disposition,
                        LocatorDisposition::Stale,
                        "{PROPERTY}: a pending read is not stale"
                    ),
                }
            }
            Op::Panic { read } => {
                let Some((peer, key)) = self.pick(read) else {
                    return;
                };
                let index = read % self.started.len();
                let query = self
                    .pending
                    .get(&peer)
                    .filter(|(pending, _)| *pending == index)
                    .map(|(_, authority)| *authority);
                let removed = self.state.locators.remove_exact(&key);
                assert_eq!(
                    self.state.calls.settle(key.call),
                    query.is_some(),
                    "{PROPERTY}: only the pending call settles"
                );
                assert_eq!(
                    removed,
                    query.map(Authority::scope),
                    "{PROPERTY}: a panic removes exactly the pending read"
                );
                if query.is_some() {
                    self.pending.remove(&peer);
                }
            }
            Op::Advance { millis } => {
                self.now += Duration::from_millis(millis);
            }
            Op::Close { peer, sent, live } => {
                // The reactor closes only admitted sessions.
                let Some(session) = self.sessions.get(&peer).copied() else {
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
                let owed = if sent {
                    self.close_retries.remove(&peer);
                    live.then_some(&mut self.close_outcomes)
                } else {
                    Some(&mut self.close_retries)
                };
                if let Some(owed) = owed {
                    if owed
                        .get(&peer)
                        .is_none_or(|(waiting, _)| *waiting != session)
                    {
                        owed.insert(peer, (session, self.now));
                    }
                }
            }
            Op::Deadlines => {
                let retries = self.due(&self.state.pending_closes);
                let outcomes = self.due(&self.state.close_deadlines);
                let due = self.state.take_due_closes(self.now);
                let due = self.indexed(due);
                let expired = self.state.take_expired_closes(self.now);
                let expired = self.indexed(expired);
                self.summary.close_retries += due.len();
                self.summary.close_expiries += expired.len();
                assert_eq!(due, retries, "{PROPERTY}: a drain takes every due retry");
                assert_eq!(
                    expired, outcomes,
                    "{PROPERTY}: a drain takes every due outcome"
                );
                for (peer, _) in due {
                    self.close_retries.remove(&peer);
                }
                for (peer, _) in expired {
                    self.close_outcomes.remove(&peer);
                }
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
                assert!(
                    !self.state.cached_pass_deferred,
                    "{PROPERTY}: a new request clears the refill wait"
                );
                assert_eq!(
                    wanted,
                    self.cached_key != Some(key),
                    "{PROPERTY}: a pass runs only for a new key"
                );
                if !wanted {
                    return self.check();
                }
                let taken = self.state.take_cached_pass(self.now);
                // A pass is always available one refill after the last one.
                if self
                    .last_cached_grant
                    .is_none_or(|last| self.now >= last + CACHED_PASS_REFILL)
                {
                    assert!(taken, "{PROPERTY}: the budget refills between passes");
                }
                assert_eq!(
                    self.state.cached_pass_deferred, !taken,
                    "{PROPERTY}: only a refused pass waits for a refill"
                );
                if taken {
                    self.summary.cached_passes += 1;
                    self.state.finish_cached_pass(opportunity, self.now);
                    self.cached_key = Some(key);
                    self.cached_granted += 1;
                    self.last_cached_grant = Some(self.now);
                }
                let refills = self.now.saturating_duration_since(self.start).as_nanos()
                    / CACHED_PASS_REFILL.as_nanos();
                assert!(
                    u128::from(self.cached_granted) <= u128::from(CACHED_PASS_BURST) + refills,
                    "{PROPERTY}: cached passes stay within the burst plus refills"
                );
            }
            Op::ResourceStall { version } => {
                self.state
                    .note_resource_stall(zakura_header_chain::StateVersion::new(u64::from(
                        version,
                    )));
                self.stalled_at = self.stalled_at.max(Some(version));
            }
            Op::Commit { version } => {
                self.committed = Some(version);
            }
            Op::Rotate { peer } => {
                let order = self.supplier_order();
                self.state
                    .rotate_supplier(source_id_from_peer(&peer_id(peer)));
                let expected = if order.contains(&peer) {
                    moved_to_back(&order, peer)
                } else {
                    order
                };
                assert_eq!(
                    self.supplier_order(),
                    expected,
                    "{PROPERTY}: a rotation moves its supplier to the back"
                );
            }
            Op::Repair { desired } => {
                let before = self.repair();
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
                let after = self.repair();
                assert_eq!(
                    after,
                    desired
                        .map(|(generation, height)| (u64::from(generation), u32::from(height) + 1)),
                    "{PROPERTY}: the reactor runs the desired repair"
                );
                if after.is_none() || after != before {
                    // The decoder can reinstall a generation, and that repair
                    // starts a new stall and a new report budget.
                    self.stall = None;
                }
            }
            Op::RepairStall { supplier } => {
                let Some((generation, _)) = self.repair() else {
                    return;
                };
                let outcome = if supplier {
                    VctRepairStallOutcome::SupplierFailure
                } else {
                    VctRepairStallOutcome::LocalFailure
                };
                self.state.record_repair_stall(
                    generation,
                    predecessor(),
                    VctSupplierRejections::default(),
                    outcome,
                    self.now,
                );
                if self
                    .stall
                    .is_none_or(|stall| stall.generation != generation)
                {
                    self.stall = Some(Stall {
                        generation,
                        since: self.now,
                        last_trace: None,
                        reported: false,
                    });
                }
            }
            Op::RefreshStall => {
                let due = self.state.refresh_repair_stall(self.now);
                self.last_stall_due = due.as_ref().map(|due| (due.trace, due.report));
                self.check_stall_output(due);
            }
        }
        self.check()
    }

    /// Check the stall output `due` against the stall the inputs recorded and
    /// its earlier outputs, and record it.
    fn check_stall_output(&mut self, due: Option<RepairStallDue>) {
        let Some(due) = due else {
            assert!(
                self.state
                    .vct_repair_stall
                    .is_none_or(|stall| stall.next_deadline() > self.now),
                "{PROPERTY}: a stall with nothing due waits for a later deadline"
            );
            return;
        };
        let Some(stall) = self.stall.as_mut() else {
            panic!("{PROPERTY}: output comes only from a recorded stall");
        };
        let stalled_for = self.now.saturating_duration_since(stall.since);
        assert_eq!(due.stalled_for, stalled_for, "{PROPERTY}");
        assert!(due.trace || due.report, "{PROPERTY}: output carries a row");
        if due.trace {
            assert!(
                stall.last_trace.is_none_or(|last| {
                    self.now.saturating_duration_since(last) >= VCT_REPAIR_STALL_TRACE_INTERVAL
                }),
                "{PROPERTY}: a trace row waits its interval"
            );
            stall.last_trace = Some(self.now);
        }
        if due.report {
            assert!(
                !stall.reported,
                "{PROPERTY}: one error report per repair generation"
            );
            assert!(
                stalled_for >= VCT_REPAIR_STALL_REPORT_AFTER,
                "{PROPERTY}: a report waits for a sustained stall"
            );
            stall.reported = true;
            self.summary.repair_reports += 1;
        }
        let best_peer_height = self
            .statuses
            .values()
            .max()
            .map(|tip| status(*tip).selected_tip_height);
        assert_eq!(
            due.best_peer_tip.map(|(height, _)| height),
            best_peer_height,
            "{PROPERTY}: the report names the best peer tip"
        );
    }

    /// The started read that `read` selects, if any read started.
    fn pick(&self, read: usize) -> Option<(usize, LocatorKey)> {
        if self.started.is_empty() {
            return None;
        }
        Some(self.started[read % self.started.len()].clone())
    }

    /// Check the supplier pass the current repair would run. A pending read
    /// stands for started work, and a `Close` waiting for its outcome stands
    /// for a live subscription.
    fn check_repair_suppliers(&self) {
        let Some(task) = self.state.vct_repair.current().cloned() else {
            return;
        };
        let suppliers: Vec<_> = self
            .state
            .vct_supplier_order
            .iter()
            .filter_map(|peer| {
                let status = self.state.peers.get(peer)?.last_status.clone()?;
                Some(Supplier {
                    status,
                    serving: self.state.locators.contains(peer),
                    live: self.state.close_deadlines.contains_key(peer),
                    ..Supplier::new(self.peer_index(peer), 0)
                })
            })
            .collect();
        check_supplier_pass(&suppliers, supplier_pass(task, &suppliers));
    }

    /// Check the machine's invariants, and compare its state with what the
    /// test sent and took.
    fn check(&self) {
        let revision = self.state.scheduling_revision;
        assert!(
            revision >= self.revision_before,
            "{PROPERTY}: the scheduling revision never moves back"
        );
        if self.must_bump {
            assert_ne!(
                revision, self.revision_before,
                "{PROPERTY}: the step moves the scheduling revision"
            );
        }
        let admitted: Vec<usize> = (0..PEERS)
            .filter(|peer| self.state.peers.contains_key(&peer_id(*peer)))
            .collect();
        assert!(
            admitted.iter().eq(self.sessions.keys()),
            "{PROPERTY}: the admitted sessions"
        );
        for (peer, schedule) in &self.state.peers {
            let tip = self.statuses.get(&self.peer_index(peer));
            assert_eq!(
                schedule
                    .last_status
                    .as_ref()
                    .map(|status| status.selected_tip_hash),
                tip.map(|tip| status(*tip).selected_tip_hash),
                "{PROPERTY}: a session keeps its last status"
            );
        }
        self.check_reads();
        self.check_closes();
        let next = self.state.next_deadline();
        if self.state.cached_pass_deferred {
            assert!(
                next.is_some_and(|next| next <= self.now + CACHED_PASS_REFILL),
                "{PROPERTY}: a deferred pass waits at most one refill"
            );
        }
        assert_eq!(
            self.state.vct_repair_stall.map(|stall| Stall {
                generation: stall.generation,
                since: stall.since,
                last_trace: stall.last_trace,
                reported: stall.reported,
            }),
            self.stall,
            "{PROPERTY}: the repair stall"
        );
        if let Some(stall) = self.stall {
            let trace_at = stall
                .last_trace
                .map_or(stall.since, |last| last + VCT_REPAIR_STALL_TRACE_INTERVAL);
            let report_at =
                (!stall.reported).then_some(stall.since + VCT_REPAIR_STALL_REPORT_AFTER);
            assert!(
                next.is_some_and(|next| next <= trace_at && report_at.is_none_or(|at| next <= at)),
                "{PROPERTY}: the stall's next row is waited for"
            );
        }
        let paused = |version: Option<u8>| {
            self.state.ordinary_discovery_paused(
                version.map(|version| zakura_header_chain::StateVersion::new(u64::from(version))),
            )
        };
        match self.stalled_at {
            None => assert!(!paused(None), "{PROPERTY}: no stall, no pause"),
            Some(stalled) => assert!(
                paused(None) && paused(Some(stalled)) && !paused(Some(stalled + 1)),
                "{PROPERTY}: a resource stall holds until a newer state version"
            ),
        }
        self.check_repair_suppliers();
        with_property(PROPERTY, || self.state.assert_invariants());
    }

    /// Each started read stays pending with its call until it settles, and
    /// the calls pending are exactly the pending reads' calls.
    fn check_reads(&self) {
        for peer in 0..PEERS {
            assert_eq!(
                self.state.locators.pending_read(&peer_id(peer)),
                self.pending
                    .get(&peer)
                    .map(|(read, authority)| (self.started[*read].1.call, authority.scope())),
                "{PROPERTY}: the pending read"
            );
        }
        let mut calls: Vec<_> = self
            .pending
            .values()
            .map(|(read, _)| self.started[*read].1.call)
            .collect();
        calls.sort_unstable_by_key(|call| call.number);
        assert_eq!(
            self.state.calls.pending(),
            calls,
            "{PROPERTY}: each started call settles once"
        );
    }

    /// Each owed `Close` waits for its exact session, no later than its first
    /// attempt plus its interval, and the next deadline comes no later.
    fn check_closes(&self) {
        let next = self.state.next_deadline();
        for (entries, owed, interval) in [
            (
                &self.state.pending_closes,
                &self.close_retries,
                CLOSE_RETRY_INTERVAL,
            ),
            (
                &self.state.close_deadlines,
                &self.close_outcomes,
                OUTCOME_TIMEOUT,
            ),
        ] {
            let sessions: BTreeMap<usize, u64> = entries
                .iter()
                .map(|(peer, (session, _))| (self.peer_index(peer), *session))
                .collect();
            assert!(
                sessions
                    .iter()
                    .eq(owed.iter().map(|(peer, (session, _))| (peer, session))),
                "{PROPERTY}: each owed close waits for its session"
            );
            for (peer, (_, at)) in entries {
                let (_, first) = owed[&self.peer_index(peer)];
                assert!(
                    first < *at && *at <= first + interval,
                    "{PROPERTY}: a close waits its interval from its first attempt"
                );
                assert!(
                    next.is_some_and(|next| next <= *at),
                    "{PROPERTY}: the close deadline is waited for"
                );
            }
        }
    }
}

/// `order` with `peer` moved to, or added at, the back.
fn moved_to_back(order: &[usize], peer: usize) -> Vec<usize> {
    order
        .iter()
        .copied()
        .filter(|queued| *queued != peer)
        .chain([peer])
        .collect()
}

/// Run `ops` from an empty state, checking the machine after every step.
#[cfg(test)]
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

#[cfg(test)]
fn run(ops: impl IntoIterator<Item = Op>) -> Harness {
    run_with(ops, |_| {})
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
    assert_eq!(harness.cached_granted, u64::from(CACHED_PASS_BURST));
    assert_eq!(harness.state.pending_closes.len(), 1);

    // Each refill interval then buys one pass, however many keys change.
    for round in 0..100_u8 {
        harness.apply(Op::Advance { millis: 1 });
        for key in [round % 3, (round + 1) % 3] {
            harness.apply(Op::CachedPass { key });
        }
    }
    assert_eq!(harness.cached_granted, u64::from(CACHED_PASS_BURST) + 100);
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

#[test]
fn locator_dispositions() {
    let query = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let moved_tip = Authority { tip: 2, ..query };
    for (awaiting, found, expected) in [
        (None, true, LocatorDisposition::Stale),
        (Some(query), true, LocatorDisposition::Ready),
        (Some(query), false, LocatorDisposition::Ready),
        // Tip churn reuses a read that found the path.
        (Some(moved_tip), true, LocatorDisposition::Ready),
        (Some(moved_tip), false, LocatorDisposition::Refetch),
        (
            Some(Authority {
                generation: 1,
                ..query
            }),
            true,
            LocatorDisposition::Refetch,
        ),
        (
            Some(Authority { anchor: 1, ..query }),
            true,
            LocatorDisposition::Refetch,
        ),
    ] {
        let mut harness = run([
            Op::Connect { peer: 0 },
            Op::RequestLocator {
                peer: 0,
                authority: query,
                capacity: true,
            },
        ]);
        let (_, key) = harness.started[0].clone();
        let target = awaiting.map(|authority| authority.target(1));
        assert_eq!(
            harness.state.locators.settle(&key, target.as_ref(), found),
            expected,
            "awaiting {awaiting:?}, found {found}"
        );
    }
}

#[test]
fn a_cached_pass_runs_only_for_a_new_key() {
    let harness = run([
        Op::CachedPass { key: 0 },
        Op::CachedPass { key: 0 },
        Op::CachedPass { key: 1 },
        Op::CachedPass { key: 0 },
    ]);
    assert_eq!(harness.summary.cached_passes, 3);
}

#[test]
fn a_repeated_close_keeps_its_outcome_deadline() {
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::Close {
            peer: 0,
            sent: true,
            live: true,
        },
        Op::Advance { millis: 1_000 },
        // A second `Close` for the same session cannot postpone the outcome.
        Op::Close {
            peer: 0,
            sent: true,
            live: true,
        },
        Op::Advance { millis: 4_000 },
    ]);
    assert_eq!(
        harness.state.take_expired_closes(harness.now),
        vec![(peer_id(0), 1)]
    );
}

#[test]
fn a_status_restores_a_spent_offer() {
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
    ]);
    assert!(harness.state.peers[&peer_id(0)].status_spent);

    harness.apply(Op::Status { peer: 0, tip: 2 });
    assert!(!harness.state.peers[&peer_id(0)].status_spent);
}

#[test]
fn a_departure_drops_its_supplier() {
    let mut harness = run([Op::Connect { peer: 0 }, Op::Connect { peer: 1 }]);
    harness.apply(Op::Disconnect { peer: 0 });
    assert_eq!(harness.supplier_order(), [1]);
    harness.apply(Op::Connect { peer: 2 });
    assert_eq!(
        harness.supplier_order(),
        [1, 2],
        "a new peer joins the back"
    );
}

#[test]
fn a_stalled_generation_keeps_its_stall_clock() {
    let mut harness = run([
        Op::Repair {
            desired: Some((0, 0)),
        },
        Op::RepairStall { supplier: false },
        Op::Advance { millis: 30_000 },
        Op::RepairStall { supplier: true },
        Op::Advance { millis: 30_000 },
    ]);
    let due = harness
        .state
        .refresh_repair_stall(harness.now)
        .expect("the first trace row is due");
    assert_eq!(due.stalled_for, Duration::from_secs(60));
    assert!(due.report);
}

/// A refused `Close` retries, and a sent one expires, exactly at its
/// interval and not a millisecond before.
#[test]
fn closes_come_due_exactly_at_their_interval() {
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::Connect { peer: 1 },
        Op::Close {
            peer: 0,
            sent: false,
            live: true,
        },
        Op::Close {
            peer: 1,
            sent: true,
            live: true,
        },
    ]);
    let mut deadlines = |millis| {
        harness.apply(Op::Advance { millis });
        harness.apply(Op::Deadlines);
        (
            harness.summary.close_retries,
            harness.summary.close_expiries,
        )
    };
    assert_eq!(deadlines(999), (0, 0));
    assert_eq!(deadlines(1), (1, 0), "the retry is due at its interval");
    assert_eq!(deadlines(3_999), (1, 0));
    assert_eq!(deadlines(1), (1, 1), "the outcome is due at its timeout");
}

/// A stalled repair traces again exactly one trace interval after its last
/// row, and reports exactly when the stall reaches the report delay.
#[test]
fn a_stall_traces_and_reports_exactly_at_its_deadlines() {
    let mut harness = run([
        Op::Repair {
            desired: Some((0, 0)),
        },
        Op::RepairStall { supplier: false },
        Op::RefreshStall,
    ]);
    let mut due = |millis| {
        harness.apply(Op::Advance { millis });
        harness.apply(Op::RefreshStall);
        harness.last_stall_due
    };
    assert_eq!(due(9_999), None);
    assert_eq!(due(1), Some((true, false)), "a row at the trace interval");
    assert_eq!(due(49_999), Some((true, false)));
    assert_eq!(due(1), Some((false, true)), "a report at the report delay");
}

#[test]
fn supplier_pass_rejections_follow_the_documented_order() {
    let narrow = |mut supplier: Supplier| {
        supplier.status.max_headers_per_response = 0;
        supplier
    };
    let no_schema = |mut supplier: Supplier| {
        supplier.status.tree_aux_schema_mask = 0;
        supplier
    };
    // Each supplier fails the first check named, and some fail later ones too.
    let suppliers = [
        // Below the target height, and serving.
        Supplier {
            serving: true,
            ..Supplier::new(0, 0)
        },
        // Too narrow, and without schema 1.
        no_schema(narrow(Supplier::new(1, 3))),
        // Without schema 1, and serving.
        no_schema(Supplier {
            serving: true,
            ..Supplier::new(2, 3)
        }),
        // Serving, with a live subscription.
        Supplier {
            serving: true,
            live: true,
            ..Supplier::new(3, 3)
        },
        // A live subscription, and an excluded input.
        Supplier {
            live: true,
            excluded: true,
            ..Supplier::new(4, 3)
        },
        // An excluded input, and tried.
        Supplier {
            excluded: true,
            tried: true,
            ..Supplier::new(5, 3)
        },
        // Tried, and waiting for a status.
        Supplier {
            tried: true,
            waiting: true,
            ..Supplier::new(6, 3)
        },
        Supplier {
            waiting: true,
            ..Supplier::new(7, 3)
        },
        Supplier::new(8, 3),
    ];
    assert_eq!(
        pass_indexes(supplier_pass(repair(0, 1), &suppliers)),
        (
            vec![8],
            vec![4],
            VctSupplierRejections {
                considered: 9,
                below_target_height: 1,
                insufficient_capacity: 1,
                unsupported_schema: 1,
                already_serving: 2,
                excluded_input: 1,
                already_tried: 1,
                supplier_status_wait: 1,
                ..VctSupplierRejections::default()
            }
        )
    );
}

/// A supplier whose tip is exactly the repair's height can serve it, and a
/// live subscription on a supplier that cannot serve stays open.
#[test]
fn supplier_pass_takes_a_tip_at_the_target_and_closes_only_servers() {
    let suppliers = [
        Supplier::new(0, 1),
        Supplier {
            live: true,
            ..Supplier::new(1, 0)
        },
    ];
    assert_eq!(
        pass_indexes(supplier_pass(repair(0, 1), &suppliers)),
        (
            vec![0],
            vec![],
            VctSupplierRejections {
                considered: 2,
                below_target_height: 1,
                ..VctSupplierRejections::default()
            }
        )
    );
}

#[cfg(test)]
proptest! {
    #[test]
    fn repair_supplier_passes_keep_their_requirements(
        suppliers in prop::collection::vec(any::<(u8, u8)>(), 0..PEERS)
    ) {
        let suppliers: Vec<_> = suppliers
            .into_iter()
            .enumerate()
            .map(|(peer, (tip, flags))| {
                let mut supplier = Supplier {
                    serving: flags & 4 != 0,
                    live: flags & 8 != 0,
                    tried: flags & 16 != 0,
                    excluded: flags & 32 != 0,
                    waiting: flags & 64 != 0,
                    ..Supplier::new(peer, tip % 4)
                };
                supplier.status.max_headers_per_response = u32::from(flags & 1) * 16;
                supplier.status.tree_aux_schema_mask = u32::from(flags >> 1 & 1);
                supplier
            })
            .collect();
        check_supplier_pass(&suppliers, supplier_pass(repair(0, 1), &suppliers));
    }
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
struct HeaderReactorReplaySummary {
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

/// Replay at most [`MAX_HISTORY_BYTES`] decoder-v1 bytes through the reactor
/// state, checking it after every operation, and count the transitions
/// reached. Every deadline stays relative to one clock origin.
#[cfg(test)]
fn replay_header_reactor_bytes(data: &[u8]) -> HeaderReactorReplaySummary {
    run_with(decode(data), |_| {}).summary
}

/// Where a header machine driver stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderMachinePhase {
    /// Applying the input and checking its immediate outcome.
    Transition,
    /// Checking the machine's invariants and the facts the test recorded.
    Invariant,
}

/// The first failure of a header machine history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderMachineFailure {
    /// Whether the failing check named its property.
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
        let message = panic_text(payload);
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
    /// The peer's test index.
    pub peer: usize,
    /// The selected tip of the peer's last status.
    pub status: Option<block::Hash>,
    /// Whether the status is spent.
    pub spent: bool,
    /// The session's unproductive requests.
    pub unproductive_requests: u32,
}

/// A repair stall in a [`HeaderReactorView`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderReactorStallView {
    /// The stalled repair's generation.
    pub generation: u64,
    /// Milliseconds from the clock origin to the first failed attempt.
    pub since_millis: u64,
    /// Milliseconds from the clock origin to the last trace row.
    pub last_trace_millis: Option<u64>,
    /// Whether the reactor reported this generation at error level.
    pub reported: bool,
}

/// One pending locator read in a [`HeaderReactorView`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderReactorReadView {
    /// The peer's test index.
    pub peer: usize,
    /// The read's exact call: its number and kind.
    pub call: (u64, StorageCallKind),
}

/// A reactor state view, read from the machine.
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
    /// The current repair's stall.
    pub stall: Option<HeaderReactorStallView>,
    /// Whether a changed cached-pass key waits for its budget.
    pub cached_deferred: bool,
    /// Milliseconds from the clock origin to the next deadline.
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
                    unproductive_requests: schedule.unproductive_requests,
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
            stall: self
                .state
                .vct_repair_stall
                .map(|stall| HeaderReactorStallView {
                    generation: stall.generation,
                    since_millis: self.millis(stall.since),
                    last_trace_millis: stall.last_trace.map(|at| self.millis(at)),
                    reported: stall.reported,
                }),
            cached_deferred: self.state.cached_pass_deferred,
            next_deadline_millis: self.state.next_deadline().map(|at| self.millis(at)),
        }
    }
}

/// Capture decoder-v1 operations without changing the decoder or starting a runtime.
/// Driver panics stop the history and stay distinct from property failures.
pub fn trace_header_reactor_bytes(data: &[u8]) -> HeaderReactorHistory {
    let input = &data[..data.len().min(MAX_HISTORY_BYTES)];
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
            failure,
        });
        if stop {
            break;
        }
    }
    HeaderReactorHistory { steps }
}

fn decode(data: &[u8]) -> impl Iterator<Item = Op> + '_ {
    data[..data.len().min(MAX_HISTORY_BYTES)]
        .chunks(4)
        .map(|chunk| {
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
const WITNESS: &[u8] = &[
    0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 2, 3, 0, 0, 0,
    4, 0, 0, 0, 4, 1, 0, 0, 7, 0, 0, 1, 6, 0, 232, 3, 8, 0, 0, 0, 7, 0, 1, 1, 6, 0, 136, 19, 8, 0,
    0, 0, 9, 0, 0, 0, 10, 2, 0, 0, 11, 3, 0, 0, 13, 0, 0, 0, 14, 0, 0, 0, 15, 0, 0, 0, 6, 0, 96,
    234, 15, 0, 0, 0, 15, 0, 0, 0, 12, 0, 0, 0,
];

#[cfg(test)]
mod target {
    use super::*;

    machine_target_tests! {
        replay: replay_header_reactor_bytes,
        target: "header_reactor",
        decoder: 1,
        witness: WITNESS => HeaderReactorReplaySummary {
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
        },
    }
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

/// The shared adapter judges only a step's failure, so a broken check must
/// become a property failure.
#[test]
fn a_broken_check_is_a_property_failure() {
    let mut harness = run(decode(WITNESS));
    harness.state.scheduling_revision = harness.state.scheduling_revision.wrapping_sub(1);
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| harness.check()))
        .expect_err("the revision moved back");
    let failure =
        HeaderMachineFailure::of_panic(PROPERTY, HeaderMachinePhase::Invariant, payload.as_ref());
    assert!(failure.property_failure, "{}", failure.message);
}

#[test]
#[should_panic(expected = "header/reactor-policy")]
fn header_reactor_witness_rejects_a_revision_that_moves_back() {
    run_with(decode(WITNESS), |harness| {
        harness.state.scheduling_revision = harness.state.scheduling_revision.wrapping_sub(1);
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
