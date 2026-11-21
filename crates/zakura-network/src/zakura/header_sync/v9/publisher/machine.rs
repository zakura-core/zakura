//! The `header/publisher-policy` machine target: generated sequences of
//! updates, jobs, job results, terminal writes, and selected-chain and
//! capacity changes, applied to the production [`PublisherOwner`].
//!
//! [`Machine`] plays the publisher task. It asks state for jobs, answers them
//! with pages and results as storage would, takes terminal outcomes and
//! reports their writes, and cancels or closes the stream. After every input
//! it checks [`PublisherOwner::assert_invariants`], and it compares state only
//! with facts that follow from the inputs it chose and the outputs it took:
//!
//! - credit: the ledger holds what the test granted, spent what the pages it
//!   committed cost, and keeps the cursors the test has not acknowledged;
//! - updates: a well-formed update applies, and a repeated sequence, an
//!   unknown acknowledgement, an empty `Grant`, or any update after `Close`
//!   faults;
//! - jobs: state holds a job in progress only when it handed one to the
//!   task; no job starts beside another, while a terminal outcome waits, or
//!   after exit; a job serves a live subscription that has not closed,
//!   extends the last header sent, and asks for no more headers than the
//!   page limits and the unspent credit allow;
//! - results: a page is sent unless `Close` arrived, and a result of an
//!   abandoned or settled job changes nothing;
//! - terminals: every closing subscription ends, only a closing subscription
//!   ends as `SubscriptionClosed`, and the outcomes are written one at a time
//!   in the order they ended;
//! - exit: the first reason to stop wins, and a stopped publisher starts
//!   nothing.
//!
//! It never re-derives which job runs next or which outcome a subscription
//! ends with. Named scenarios in `machine/tests.rs` pin those rules.
//!
//! [`trace_header_publisher_bytes`] drives the same machine from fuzz bytes
//! and records what the publisher showed after each operation.

use std::{
    collections::{BTreeMap, VecDeque},
    ops::ControlFlow,
};

use zakura_chain::work::difficulty::U256;
use zakura_header_chain::{
    AlarmSet, ChainScore, EngineMode, FrontierSet, StateVersion, SuffixWork, VerifiedGeneration,
};

use super::*;
use crate::zakura::{
    header_sync::{
        config::REGTEST_HEADER_BYTES,
        reactor::{HeaderMachineFailure, HeaderMachinePhase},
        v9::{Cursor, Operation, RegtestMessage},
    },
    machine_test::{with_property, MAX_HISTORY_BYTES},
    wire_codec::decode_frame,
    Credit,
};

#[cfg(test)]
mod tests;

const HB: usize = REGTEST_HEADER_BYTES;

/// A selected chain ending at `tip`.
pub(super) fn snapshot(tip: Frontier, generation: u64, genesis: Frontier) -> EngineSnapshot {
    EngineSnapshot {
        mode: EngineMode::Integrated,
        state_version: StateVersion::new(generation),
        header_generation: HeaderGeneration::new(generation),
        verified_generation: VerifiedGeneration::new(1),
        frontiers: FrontierSet {
            finalized: genesis,
            header_best: tip,
            verified_best: genesis,
        },
        header_best_score: ChainScore::new(SuffixWork::new(U256::from(tip.height.0)), tip.hash),
        oldest_retained_height: block::Height(0),
        alarms: AlarmSet::default(),
    }
}

/// Subscription IDs the steps use, so updates collide and IDs are reused.
const IDS: u64 = 3;

/// Heights at or below this are the same on every branch.
const FORK: u32 = 2;

/// One schema-0 entry's bytes on Regtest.
fn entry() -> u64 {
    181
}

/// A page's fixed bytes.
fn fixed() -> u64 {
    82
}

/// Three headers per page, and bytes for exactly three entries.
fn page_limits() -> PageLimits {
    PageLimits {
        max_headers: 3,
        max_bytes: u32::try_from(fixed() + 3 * entry()).expect("a small page fits u32"),
        aux_schema_mask: AuxSchema::V1.mask_bit(),
    }
}

/// The header at `height` on `branch`. Branches share every header at or
/// below [`FORK`].
fn frontier(branch: u8, height: u32) -> Frontier {
    let lane = if height <= FORK { 0 } else { branch + 1 };
    let mut hash = [0; 32];
    hash[0] = 0xa0 | lane;
    hash[1..5].copy_from_slice(&height.to_le_bytes());
    Frontier::new(block::Height(height), block::Hash(hash))
}

/// A selected tip.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Tip {
    branch: u8,
    height: u32,
}

impl Tip {
    fn frontier(self) -> Frontier {
        frontier(self.branch, self.height)
    }

    fn snapshot(self) -> EngineSnapshot {
        snapshot(self.frontier(), 1, frontier(0, 0))
    }
}

/// Cumulative objects and bytes.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct Sum {
    objects: u64,
    bytes: u64,
}

impl Sum {
    fn add(self, objects: u64, bytes: u64) -> Self {
        Self {
            objects: self.objects + objects,
            bytes: self.bytes + bytes,
        }
    }
}

/// What the test sent and took for one live or closing subscription.
#[derive(Clone, Debug)]
struct Sub {
    target: Frontier,
    /// Whether the `Open`'s locator was the target.
    at_target: bool,
    /// The `Open`'s locator.
    start: Frontier,
    /// The last update sequence state applied.
    sequence: u32,
    granted: Sum,
    consumed: Sum,
    acknowledged: Cursor,
    /// Cursors of committed pages the test has not acknowledged, oldest
    /// first.
    sent: VecDeque<Cursor>,
    /// The header the next page extends: the last committed page's, or the
    /// target a `ReachedAtOpen` reported.
    last: Option<Frontier>,
    /// Whether a committed page or a `ReachedAtOpen` reached the target.
    following: bool,
    /// Whether state applied the subscriber's `Close`.
    closing: bool,
}

impl Sub {
    fn unspent(&self) -> Sum {
        Sum {
            objects: self.granted.objects - self.consumed.objects,
            bytes: self.granted.bytes - self.consumed.bytes,
        }
    }
}

/// A job the test started, with what it needs to answer it.
#[derive(Clone, Debug)]
struct JobRecord {
    job: Job,
    start: Frontier,
    target: Frontier,
    at_target: bool,
    seen: Option<Tip>,
}

/// Which update sequence a step sends.
#[derive(Copy, Clone, Debug)]
enum Sequence {
    Next,
    /// A duplicate of the last update.
    Repeat,
}

/// A job result other than a page.
#[derive(Copy, Clone, Debug)]
enum Answer {
    End(HeadersOutcomeCode),
    Local,
    Capacity,
    Retry,
    Extends,
    ReachedAtOpen,
    /// A storage reply, which state's selection functions read.
    Reply(Reply),
}

/// A storage reply to a job.
#[derive(Copy, Clone, Debug)]
enum Reply {
    ProbeSelected,
    ProbeNotSelected,
    ProbeNotRetained,
    Refused(LeaseRefused),
    /// A lease from the header the job must extend.
    Lease,
    /// A lease from another header.
    LeaseWrongParent,
    /// A page with no record for its first header.
    EmptyPage,
    CheckExtends,
    /// A check that finds another header at the last header's hash.
    CheckElsewhere,
    CheckNotSelected,
}

const REPLIES: [Reply; 13] = [
    Reply::ProbeSelected,
    Reply::ProbeNotSelected,
    Reply::ProbeNotRetained,
    Reply::Refused(LeaseRefused::NoLocatorIntersection),
    Reply::Refused(LeaseRefused::TargetNotRetained),
    Reply::Refused(LeaseRefused::HistoryPruned),
    Reply::Refused(LeaseRefused::Busy),
    Reply::Lease,
    Reply::LeaseWrongParent,
    Reply::EmptyPage,
    Reply::CheckExtends,
    Reply::CheckElsewhere,
    Reply::CheckNotSelected,
];

/// Whether storage can send `reply` to `job`: only a first page probes, only a
/// check without a page gets a check's reply, and only a page job leases.
fn reply_applies(reply: Reply, job: &Job) -> bool {
    match reply {
        Reply::ProbeSelected | Reply::ProbeNotSelected | Reply::ProbeNotRetained => job.probe,
        Reply::CheckExtends | Reply::CheckElsewhere | Reply::CheckNotSelected => job.count == 0,
        _ => job.count > 0,
    }
}

/// What state's selection functions make of `reply` to `record`'s job, as the
/// publisher task reads it. `None` means the job goes on.
fn selected_output(reply: Reply, record: &JobRecord) -> Option<Output> {
    use SelectedChainMembership::{NotRetained, NotSelected, Selected};
    let job = &record.job;
    let other = frontier(2, 99);
    let last = || job.parent.expect("a check's follower has a last header");
    match reply {
        Reply::ProbeSelected => on_probe(Selected(record.target)),
        Reply::ProbeNotSelected => on_probe(NotSelected),
        Reply::ProbeNotRetained => on_probe(NotRetained),
        Reply::Refused(refused) => Some(on_lease_refused(refused, job.following)),
        Reply::Lease | Reply::LeaseWrongParent => {
            let ancestor = match (reply, job.parent) {
                (Reply::LeaseWrongParent, Some(_)) => other,
                (_, Some(parent)) => parent,
                (_, None) if record.at_target => record.target,
                (_, None) => record.start,
            };
            on_lease_acquired(job.parent, ancestor, record.target)
        }
        Reply::EmptyPage => Some(on_empty_page(job.following)),
        Reply::CheckExtends => Some(on_following_check(
            Selected(last()),
            last(),
            job.lease_target,
        )),
        Reply::CheckElsewhere => Some(on_following_check(
            Selected(other),
            last(),
            job.lease_target,
        )),
        Reply::CheckNotSelected => Some(on_following_check(NotSelected, last(), job.lease_target)),
    }
}

#[derive(Clone, Debug)]
enum Step {
    Open {
        id: u64,
        at_target: bool,
        target: u32,
        objects: u32,
        units: u32,
    },
    Grant {
        id: u64,
        sequence: Sequence,
        /// Picks the acknowledged cursor, modulo the choices.
        ack: usize,
        objects: u32,
        units: u32,
    },
    Close {
        id: u64,
        sequence: Sequence,
        /// Picks the acknowledged cursor, modulo the choices.
        ack: usize,
    },
    NextJob,
    Page {
        headers: u32,
        shave: u64,
        stale: Option<usize>,
    },
    Finish {
        answer: Answer,
        stale: Option<usize>,
    },
    EndDue,
    NextTerminal,
    /// The stream's answer to the terminal outcome being written.
    TerminalWritten(bool),
    Cancel,
    /// The stream or the runtime closed under the job in progress.
    Closed,
    Select(Option<Tip>),
    Release(usize),
}

/// Transitions one history reached.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Default, Eq, PartialEq)]
struct PublisherCounters {
    /// `Open`s state applied.
    opened: usize,
    /// `Grant`s and `Close`s state applied to a live subscription.
    updated: usize,
    /// Updates state refused.
    refused: usize,
    /// Jobs started.
    jobs: usize,
    /// Pages committed.
    pages: usize,
    /// Results other than a page settled for the job in progress.
    results: usize,
    /// Pages and results of a settled or abandoned job.
    stale: usize,
    /// Capacity refusals reported.
    capacity_stalls: usize,
    /// Terminal outcomes `end_due` queued.
    ended: usize,
    /// Terminal outcomes written.
    written: usize,
    /// Reasons to stop.
    stopped: usize,
}

/// The test playing the publisher task, with what it sent and took.
struct Machine {
    owner: PublisherOwner,
    subs: BTreeMap<u64, Sub>,
    /// The last sequence state applied for each ended ID, so a crossed update
    /// can follow it.
    dead_sequences: BTreeMap<u64, u32>,
    selected: Option<Tip>,
    /// The job the test is running.
    job: Option<JobRecord>,
    /// Jobs that settled or that state abandoned.
    stale: Vec<JobRecord>,
    /// Subscriptions and outcomes `end_due` returned and the test has not
    /// taken, in order.
    queued: VecDeque<(u64, HeadersOutcomeCode)>,
    /// The terminal outcome the test is writing.
    writing: Option<OwnedTerminal>,
    exit: Option<Exit>,
    /// Each capacity refusal's wait and signal.
    signals: Vec<(CapacityWait, ServingCapacitySignal)>,
    counters: PublisherCounters,
    /// The instant each job request reads. It steps one walk-meter refill
    /// interval per request, so the meter never holds a job back here. The
    /// publisher test `walks_from_open_locators_spend_the_meter_at_exact_boundaries`
    /// checks the meter.
    clock: zakura_rt::time::Instant,
}

impl Machine {
    fn new() -> Self {
        Self {
            owner: PublisherOwner::new(page_limits()),
            subs: BTreeMap::new(),
            dead_sequences: BTreeMap::new(),
            selected: None,
            job: None,
            stale: Vec::new(),
            queued: VecDeque::new(),
            writing: None,
            exit: None,
            signals: Vec::new(),
            counters: PublisherCounters::default(),
            clock: zakura_rt::time::Instant::now(),
        }
    }

    /// The instant for the next job request. See [`Self::clock`].
    fn tick(&mut self) -> zakura_rt::time::Instant {
        self.clock += state::OPEN_WALKS.refill_interval;
        self.clock
    }

    fn selected(&self) -> Option<EngineSnapshot> {
        self.selected.map(Tip::snapshot)
    }

    /// Whether state may start no job now.
    fn busy(&self) -> bool {
        self.job.is_some()
            || self.writing.is_some()
            || !self.queued.is_empty()
            || self.exit.is_some()
    }

    /// What the publisher showed: its ledger and slots, and the outputs the
    /// test holds.
    fn view(&self) -> HeaderPublisherView {
        let ledger = self.owner.state.publications();
        let mut ids: Vec<u64> = ledger.keys().copied().collect();
        ids.sort_unstable();
        HeaderPublisherView {
            subscriptions: ids
                .into_iter()
                .map(|id| {
                    let credit = ledger.credit(&id).expect("a live subscription");
                    let last = ledger.last_sent(&id).expect("a live subscription");
                    HeaderPublisherSubscriptionView {
                        id,
                        granted: (credit.granted().objects, credit.granted().bytes),
                        consumed: (credit.consumed().objects, credit.consumed().bytes),
                        unacknowledged: ledger.unacknowledged(&id).unwrap_or(0),
                        last_height: last.height.0,
                        last_hash: last.hash,
                        closing: ledger.is_closing(&id),
                    }
                })
                .collect(),
            tombstones: ledger.tombstones(),
            job: self.job.as_ref().map(|record| record.job.id),
            terminals: self.queued.len(),
            writing: self.writing.is_some(),
            slots: self.owner.slots.held(),
            exit: self.exit.map(exit_name),
        }
    }

    /// Check the owner's invariants, and that its ledger holds what the test
    /// granted, spent, and has not acknowledged.
    fn check(&mut self) {
        with_property(PROPERTY, || self.owner.assert_invariants());
        if let Some(record) = &self.job {
            assert!(
                self.owner.state.is_current(record.job.key),
                "{PROPERTY}: the job in progress keeps its exact key"
            );
        }
        let (_, _, in_progress, _) = self.owner.state.serving();
        assert_eq!(
            in_progress,
            self.job.is_some(),
            "{PROPERTY}: the task holds every job state started"
        );
        if self.exit.is_some() {
            return;
        }
        let ledger = self.owner.state.publications();
        assert_eq!(
            ledger.keys().count(),
            self.subs.len(),
            "{PROPERTY}: the live subscriptions"
        );
        for (id, sub) in &self.subs {
            let credit = ledger.credit(id).map(|credit| {
                let (granted, consumed) = (credit.granted(), credit.consumed());
                (
                    granted.objects,
                    granted.bytes,
                    consumed.objects,
                    consumed.bytes,
                )
            });
            assert_eq!(
                credit,
                Some((
                    sub.granted.objects,
                    sub.granted.bytes,
                    sub.consumed.objects,
                    sub.consumed.bytes
                )),
                "{PROPERTY}: subscription {id}'s credit"
            );
            assert_eq!(
                ledger.last_sent(id),
                Some(sub.sent.back().unwrap_or(&sub.acknowledged)),
                "{PROPERTY}: subscription {id}'s last cursor"
            );
            assert_eq!(
                ledger.unacknowledged(id),
                Some(sub.sent.len()),
                "{PROPERTY}: subscription {id}'s unacknowledged pages"
            );
            assert_eq!(
                ledger.is_closing(id),
                sub.closing,
                "{PROPERTY}: subscription {id}'s Close"
            );
        }
        let mut waits = self.owner.state.capacity_waits();
        waits.sort_unstable_by_key(|wait| wait.0);
        waits.dedup();
        assert_eq!(
            self.owner.capacity_signals().len(),
            waits.len(),
            "{PROPERTY}: one capacity signal per distinct stalled wait"
        );
    }

    fn step(&mut self, step: &Step) {
        match *step {
            Step::Open {
                id,
                at_target,
                target,
                objects,
                units,
            } => self.open(id, at_target, target, objects, units),
            Step::Grant {
                id,
                sequence,
                ack,
                objects,
                units,
            } => {
                let added = Credit {
                    objects,
                    bytes: u32::try_from(u64::from(units) * entry()).expect("small credit"),
                };
                self.update(Operation::Grant, id, sequence, ack, added);
            }
            Step::Close { id, sequence, ack } => {
                let none = Credit {
                    objects: 0,
                    bytes: 0,
                };
                self.update(Operation::Close, id, sequence, ack, none);
            }
            Step::NextJob => self.next_job(),
            Step::Page {
                headers,
                shave,
                stale,
            } => self.page(headers, shave, stale),
            Step::Finish { answer, stale } => self.finish(answer, stale),
            Step::EndDue => self.end_due(),
            Step::NextTerminal => self.next_terminal(),
            Step::TerminalWritten(written) => self.terminal_written(written),
            Step::Cancel => self.stop(Exit::Cancelled),
            Step::Closed => self.stop(Exit::Closed),
            Step::Select(tip) => {
                self.selected = tip;
                self.owner.state.selected_changed();
            }
            Step::Release(which) => {
                if self.signals.is_empty() {
                    return;
                }
                let index = which % self.signals.len();
                self.signals[index].1.release();
                self.owner.capacity_released();
            }
        }
    }

    fn open(&mut self, id: u64, at_target: bool, target: u32, objects: u32, units: u32) {
        let target = frontier(0, target);
        let start = if at_target { target } else { frontier(0, 0) };
        let acknowledged = Cursor {
            height: start.height,
            hash: start.hash,
            headers: 0,
            bytes: 0,
        };
        let credit = Credit {
            objects,
            bytes: u32::try_from(fixed() + u64::from(units) * entry()).expect("small credit"),
        };
        let update = SubscribeHeaders {
            operation: Operation::Open,
            subscription_id: id,
            update_sequence: 0,
            initial_target_tip_hash: target.hash,
            acknowledged,
            locator_hashes: vec![start.hash],
            added_credit: credit,
            tree_aux_schema: AuxSchema::None,
        };
        let applied = self.owner.apply(&update);
        if self.subs.contains_key(&id) {
            assert!(applied.is_err(), "{PROPERTY}: an Open reuses a live ID");
        }
        if applied.is_err() {
            self.counters.refused += 1;
            return;
        }
        assert_eq!(applied, Ok(Applied::Live), "{PROPERTY}: an Open is live");
        self.counters.opened += 1;
        self.dead_sequences.remove(&id);
        self.subs.insert(
            id,
            Sub {
                target,
                at_target,
                start,
                sequence: 0,
                granted: Sum::default().add(u64::from(credit.objects), u64::from(credit.bytes)),
                consumed: Sum::default(),
                acknowledged,
                sent: VecDeque::new(),
                last: None,
                following: false,
                closing: false,
            },
        );
    }

    /// Send a `Grant` or `Close`. A well-formed update to a live subscription
    /// applies, and a malformed one faults. An update to an ended
    /// subscription never applies to a live one.
    fn update(
        &mut self,
        operation: Operation,
        id: u64,
        sequence: Sequence,
        ack: usize,
        added: Credit,
    ) {
        let live = self.subs.get(&id);
        let base = live
            .map(|sub| sub.sequence)
            .or_else(|| self.dead_sequences.get(&id).copied())
            .unwrap_or(0);
        let got = match sequence {
            Sequence::Next => base + 1,
            Sequence::Repeat => base,
        };
        let bogus = Cursor {
            height: block::Height(99),
            hash: block::Hash([0xee; 32]),
            headers: 0,
            bytes: 0,
        };
        // The current acknowledgement, a sent page, or an unknown cursor.
        let through = live.map(|sub| ack % (sub.sent.len() + 2));
        let acknowledged = match (live, through) {
            (Some(sub), Some(0)) => sub.acknowledged,
            (Some(sub), Some(index)) if index <= sub.sent.len() => sub.sent[index - 1],
            _ => bogus,
        };
        let update = SubscribeHeaders {
            operation,
            subscription_id: id,
            update_sequence: got,
            initial_target_tip_hash: live.map_or(block::Hash([0; 32]), |sub| sub.target.hash),
            acknowledged,
            locator_hashes: Vec::new(),
            added_credit: added,
            tree_aux_schema: AuxSchema::None,
        };
        let empty_grant = operation == Operation::Grant && added.objects == 0 && added.bytes == 0;
        let applied = self.owner.apply(&update);
        let Some(sub) = self.subs.get_mut(&id) else {
            assert_ne!(
                applied,
                Ok(Applied::Live),
                "{PROPERTY}: an update to an ended subscription"
            );
            match applied {
                Ok(_) => {
                    self.dead_sequences.insert(id, got);
                    self.counters.updated += 1;
                }
                Err(_) => self.counters.refused += 1,
            }
            return;
        };
        let known = through.is_some_and(|index| index <= sub.sent.len());
        let well_formed =
            matches!(sequence, Sequence::Next) && known && !sub.closing && !empty_grant;
        if well_formed {
            assert_eq!(
                applied,
                Ok(Applied::Live),
                "{PROPERTY}: a well-formed update applies"
            );
        } else {
            assert!(applied.is_err(), "{PROPERTY}: a malformed update faults");
            self.counters.refused += 1;
            return;
        }
        if let Some(index) = through.filter(|index| *index > 0) {
            sub.acknowledged = sub.sent.drain(..index).next_back().expect("a sent page");
        }
        sub.sequence = got;
        sub.granted = sub
            .granted
            .add(u64::from(added.objects), u64::from(added.bytes));
        sub.closing |= operation == Operation::Close;
        self.counters.updated += 1;
    }

    /// Ask for the next job, and check it against what the test sent: it
    /// serves a live subscription that has not closed, extends the last
    /// header sent, leases toward the target on the path or a higher selected
    /// tip after it, and fits the page limits and the unspent credit.
    fn next_job(&mut self) {
        let busy = self.busy();
        let selected = self.selected();
        let now = self.tick();
        let job = self.owner.state.next_job::<HB>(selected.as_ref(), now);
        if busy {
            assert!(
                job.is_none(),
                "{PROPERTY}: no job starts beside another, a waiting terminal outcome, or after exit"
            );
            return;
        }
        let Some(job) = job else {
            return;
        };
        let sub = self
            .subs
            .get(&job.id)
            .unwrap_or_else(|| panic!("{PROPERTY}: job {job:?} serves an ended subscription"));
        assert!(!sub.closing, "{PROPERTY}: a job after Close");
        assert_eq!(
            (job.parent, job.following, job.initial_target),
            (sub.last, sub.following, sub.target.hash),
            "{PROPERTY}: the job extends the last header sent"
        );
        if job.following {
            let tip = self
                .selected
                .unwrap_or_else(|| panic!("{PROPERTY}: a follower's job without a selected tip"));
            let last = sub.last.expect("a follower has a last header");
            assert_eq!(
                job.lease_target,
                tip.frontier().hash,
                "{PROPERTY}: a follower leases toward the selected tip"
            );
            assert!(
                tip.height > last.height.0,
                "{PROPERTY}: a follower's tip is above its last header"
            );
        } else {
            assert_eq!(
                job.lease_target, sub.target.hash,
                "{PROPERTY}: a path job leases toward its target"
            );
            assert!(job.count > 0, "{PROPERTY}: a path job has room for a page");
        }
        let unspent = sub.unspent();
        let count = u64::from(job.count);
        assert!(
            job.count <= page_limits().max_headers
                && count <= unspent.objects
                && (count == 0 || fixed() + count * entry() <= unspent.bytes),
            "{PROPERTY}: {count} headers fit the page limits and {unspent:?}"
        );
        self.job = Some(JobRecord {
            start: sub.start,
            target: sub.target,
            at_target: sub.at_target,
            seen: self.selected,
            job,
        });
        self.counters.jobs += 1;
    }

    /// The job a result answers: a stale one, or the one in progress.
    fn answering(&self, stale: Option<usize>) -> Option<(JobRecord, bool)> {
        match (stale, self.stale.is_empty()) {
            (Some(index), false) => Some((self.stale[index % self.stale.len()].clone(), true)),
            _ => self.job.clone().map(|record| (record, false)),
        }
    }

    fn page(&mut self, headers: u32, shave: u64, stale: Option<usize>) {
        let Some((record, is_stale)) = self.answering(stale) else {
            return;
        };
        let job = &record.job;
        // Storage answers a locator at the target, or a check, without a
        // page.
        if job.count == 0 {
            return self.answer(record, is_stale, Answer::Extends);
        }
        if record.at_target && job.parent.is_none() {
            return self.answer(record, is_stale, Answer::ReachedAtOpen);
        }
        let (parent, branch, end, target) = if job.following {
            let tip = record.seen.expect("a follower's job read a selected chain");
            let parent = job.parent.expect("a follower has a last header");
            (parent, tip.branch, tip.height, tip.frontier())
        } else {
            let parent = job.parent.unwrap_or(record.start);
            (parent, 0, record.target.height.0, record.target)
        };
        let count = headers.min(job.count).min(end - parent.height.0);
        let last = frontier(branch, parent.height.0 + count);
        let bytes = fixed() + u64::from(count) * entry() - shave;
        let page = Page {
            parent,
            last,
            target,
            count,
            reaches_target: !job.following && last == record.target,
            payload_len: usize::try_from(bytes).expect("small page"),
        };
        let before = self.view();
        let committed = self.owner.state.commit(job.key, &page);
        if is_stale {
            assert!(!committed, "{PROPERTY}: a stale page is never sent");
            assert_eq!(
                self.view(),
                before,
                "{PROPERTY}: a stale page spends nothing"
            );
            self.counters.stale += 1;
            return;
        }
        self.settle(record.clone());
        let sub = self
            .subs
            .get_mut(&job.id)
            .expect("a running job's subscription is live");
        assert_eq!(
            committed, !sub.closing,
            "{PROPERTY}: a page is sent unless Close arrived"
        );
        if !committed {
            return;
        }
        let unspent = sub.unspent();
        assert!(
            u64::from(count) <= unspent.objects && bytes <= unspent.bytes,
            "{PROPERTY}: the page spends only granted credit"
        );
        sub.consumed = sub.consumed.add(u64::from(count), bytes);
        sub.sent.push_back(Cursor {
            height: last.height,
            hash: last.hash,
            headers: sub.consumed.objects,
            bytes: sub.consumed.bytes,
        });
        sub.last = Some(last);
        sub.following |= page.reaches_target;
        self.counters.pages += 1;
    }

    fn finish(&mut self, answer: Answer, stale: Option<usize>) {
        let Some((record, is_stale)) = self.answering(stale) else {
            return;
        };
        self.answer(record, is_stale, answer);
    }

    /// Report a result other than a page for `record`'s job, as storage
    /// could answer it.
    fn answer(&mut self, record: JobRecord, is_stale: bool, answer: Answer) {
        let job = &record.job;
        let reachable = record.at_target && job.parent.is_none();
        let output = match answer {
            Answer::Reply(reply) if reply_applies(reply, job) => {
                selected_output(reply, &record).unwrap_or(Output::Retry)
            }
            Answer::End(code) => Output::End(code),
            Answer::Local => Output::Stall(Stall::Local, "test"),
            Answer::Capacity => {
                let signal = ServingCapacitySignal::default();
                let wait = self.owner.waits.register(signal.clone());
                self.signals.push((wait, signal));
                self.counters.capacity_stalls += 1;
                Output::Stall(Stall::Capacity(wait), "test")
            }
            Answer::Extends if job.count == 0 => Output::Extends(job.lease_target),
            Answer::ReachedAtOpen if reachable => Output::ReachedAtOpen(record.target),
            Answer::Reply(_) | Answer::Retry | Answer::Extends | Answer::ReachedAtOpen => {
                Output::Retry
            }
        };
        let latest = self.selected();
        let before = self.view();
        self.owner.state.finish(job.key, output, latest.as_ref());
        if is_stale {
            assert_eq!(
                self.view(),
                before,
                "{PROPERTY}: a stale result changes nothing"
            );
            self.counters.stale += 1;
            return;
        }
        self.settle(record.clone());
        if let Output::ReachedAtOpen(target) = output {
            let sub = self
                .subs
                .get_mut(&job.id)
                .expect("a running job's subscription is live");
            sub.following = true;
            sub.last = Some(target);
        }
        self.counters.results += 1;
    }

    /// Retire the job in progress after its result.
    fn settle(&mut self, record: JobRecord) {
        self.job = None;
        self.stale.push(record);
    }

    /// End what state says is due. Every closing subscription ends, and only
    /// a closing one ends as `SubscriptionClosed`.
    fn end_due(&mut self) {
        let outcomes = self.owner.end_due();
        let ledger = self.owner.state.publications();
        let ended: Vec<u64> = self
            .subs
            .keys()
            .copied()
            .filter(|id| ledger.credit(id).is_none())
            .collect();
        let mut named: Vec<u64> = outcomes.iter().map(|(id, _)| *id).collect();
        named.sort_unstable();
        assert_eq!(
            named, ended,
            "{PROPERTY}: one outcome for each ended subscription"
        );
        for (id, sub) in &self.subs {
            assert!(
                !sub.closing || ended.contains(id),
                "{PROPERTY}: closing subscription {id} ends"
            );
        }
        for (id, outcome) in &outcomes {
            assert!(
                *outcome != HeadersOutcomeCode::SubscriptionClosed || self.subs[id].closing,
                "{PROPERTY}: {id} ends as closed without a Close"
            );
        }
        for id in ended {
            let sub = self
                .subs
                .remove(&id)
                .expect("an ended subscription was live");
            if let Some(record) = self.job.take_if(|record| record.job.id == id) {
                assert!(
                    !self.owner.state.is_current(record.job.key),
                    "{PROPERTY}: ending a subscription abandons its job"
                );
                self.stale.push(record);
            }
            self.dead_sequences.insert(id, sub.sequence);
        }
        self.counters.ended += outcomes.len();
        self.queued.extend(outcomes);
    }

    /// Take the next terminal outcome to write: the oldest one queued, unless
    /// one is being written or the publisher stopped.
    fn next_terminal(&mut self) {
        let terminal = self.owner.next_terminal::<HB>();
        let expected = if self.writing.is_some() || self.exit.is_some() {
            None
        } else {
            self.queued.front().copied()
        };
        let found = terminal.as_ref().map(|terminal| {
            let Ok(Message::HeadersOutcome(outcome)) =
                decode_frame::<RegtestMessage>(&terminal.frame)
            else {
                panic!("a terminal frame is an outcome");
            };
            (outcome.subscription_id, outcome.outcome)
        });
        assert_eq!(
            found, expected,
            "{PROPERTY}: the next terminal outcome, one at a time and in order"
        );
        if found.is_some() {
            self.queued.pop_front();
            self.writing = terminal;
        }
    }

    /// Report the stream's answer to the terminal outcome being written.
    fn terminal_written(&mut self, written: bool) {
        // The task reports a write only for the outcome it took.
        let Some(terminal) = self.writing.take() else {
            return;
        };
        // The write finishes, so its slot is free again.
        let slot = terminal.slot;
        drop(terminal);
        let expected = if written {
            ControlFlow::Continue(())
        } else {
            self.exit = Some(Exit::Closed);
            ControlFlow::Break(Exit::Closed)
        };
        assert_eq!(
            self.owner.state.terminal_written(slot, written),
            expected,
            "{PROPERTY}: a refused terminal outcome stops the publisher"
        );
        self.counters.written += 1;
        self.check_stopped();
    }

    /// Cancel the session, or close the stream, and check that state stops.
    fn stop(&mut self, exit: Exit) {
        let stopped = |state: &mut PublisherState, exit| match exit {
            Exit::Cancelled => state.cancelled(),
            Exit::Closed => state.closed(),
        };
        assert_eq!(
            stopped(&mut self.owner.state, exit),
            exit,
            "{PROPERTY}: the reason to stop"
        );
        self.exit = Some(exit);
        let other = match exit {
            Exit::Cancelled => Exit::Closed,
            Exit::Closed => Exit::Cancelled,
        };
        assert_eq!(
            stopped(&mut self.owner.state, other),
            exit,
            "{PROPERTY}: the first reason to stop wins"
        );
        self.counters.stopped += 1;
        self.check_stopped();
    }

    /// A stopped publisher starts no job and offers no terminal outcome.
    fn check_stopped(&mut self) {
        if self.exit.is_none() {
            return;
        }
        let selected = self.selected();
        let now = self.tick();
        assert!(
            self.owner
                .state
                .next_job::<HB>(selected.as_ref(), now)
                .is_none(),
            "{PROPERTY}: no job starts after the publisher stops"
        );
        assert!(
            self.owner.state.next_terminal().is_none(),
            "{PROPERTY}: no terminal outcome is written after the publisher stops"
        );
    }

    /// Apply `step` and check the result, unless the publisher stopped: the
    /// task returns then, so no step follows.
    #[cfg(test)]
    fn run(&mut self, step: &Step) {
        if self.exit.is_some() {
            return;
        }
        self.step(step);
        self.check();
    }
}

const PROPERTY: &str = "header/publisher-policy";

/// One subscription in a [`HeaderPublisherView`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderPublisherSubscriptionView {
    /// The subscription's ID.
    pub id: u64,
    /// Credit granted so far: headers and bytes.
    pub granted: (u64, u64),
    /// Credit consumed so far: headers and bytes.
    pub consumed: (u64, u64),
    /// Sent pages the subscriber has not acknowledged.
    pub unacknowledged: usize,
    /// The height of the cursor the next page follows.
    pub last_height: u32,
    /// The hash of the cursor the next page follows.
    pub last_hash: block::Hash,
    /// Whether the subscriber sent `Close`.
    pub closing: bool,
}

/// What the publisher showed after one operation: its ledger and slots, and
/// the outputs the test holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderPublisherView {
    /// Live and closing subscriptions, by ID.
    pub subscriptions: Vec<HeaderPublisherSubscriptionView>,
    /// Tombstones held.
    pub tombstones: usize,
    /// The subscription of the job in progress.
    pub job: Option<u64>,
    /// Terminal outcomes queued and not yet taken to write.
    pub terminals: usize,
    /// Whether a terminal outcome is being written.
    pub writing: bool,
    /// Subscription slots held.
    pub slots: usize,
    /// Why the publisher stopped, if it did.
    pub exit: Option<&'static str>,
}

/// One decoder-v2 publisher operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderPublisherStep {
    /// The operation's number, from one.
    pub step: usize,
    /// Original causal bytes, including a short final operation.
    pub input: Vec<u8>,
    /// What the publisher showed after the operation.
    pub actual: HeaderPublisherView,
    /// First failure, if this operation stopped the run.
    pub failure: Option<HeaderMachineFailure>,
}

/// A decoder-v2 publisher history. It stops at the first failure or when the
/// publisher stops.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HeaderPublisherHistory {
    /// Every applied operation, at most 128.
    pub steps: Vec<HeaderPublisherStep>,
}

/// A stable name for `exit`.
fn exit_name(exit: Exit) -> &'static str {
    match exit {
        Exit::Cancelled => "cancelled",
        Exit::Closed => "closed",
    }
}

/// Capture decoder-v2 operations without changing the decoder or starting a
/// runtime. Driver panics stop the history and stay distinct from property
/// failures.
pub fn trace_header_publisher_bytes(data: &[u8]) -> HeaderPublisherHistory {
    let input = &data[..data.len().min(MAX_HISTORY_BYTES)];
    let mut machine = Machine::new();
    let mut steps = Vec::new();
    for (index, step) in decode(input).enumerate() {
        let number = index + 1;
        let applied =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| machine.step(&step)))
                .map_err(|payload| (HeaderMachinePhase::Transition, payload))
                .and_then(|()| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| machine.check()))
                        .map_err(|payload| (HeaderMachinePhase::Invariant, payload))
                });
        let failure = applied.err().map(|(phase, payload)| {
            HeaderMachineFailure::of_panic(PROPERTY, phase, payload.as_ref())
        });
        let stop = failure.is_some() || machine.exit.is_some();
        steps.push(HeaderPublisherStep {
            step: number,
            input: input[index * 4..(number * 4).min(input.len())].to_vec(),
            actual: machine.view(),
            failure,
        });
        if stop {
            break;
        }
    }
    HeaderPublisherHistory { steps }
}

/// Replay at most [`MAX_HISTORY_BYTES`] decoder-v2 bytes through the publisher,
/// checking every property after each operation, and return the transitions
/// reached.
#[cfg(test)]
fn replay_publisher_bytes(data: &[u8]) -> PublisherCounters {
    let mut machine = Machine::new();
    for step in decode(data) {
        machine.run(&step);
    }
    machine.counters
}

/// Decoder v2, the simulator's generator 2 for this machine. Version 1 read every
/// `Finish` as an [`Answer`] from its second byte. Version 2 reads a `Finish` with an
/// odd third byte as a [`Reply`], so bytes saved under version 1 can change meaning.
fn decode(data: &[u8]) -> impl Iterator<Item = Step> + '_ {
    data[..data.len().min(MAX_HISTORY_BYTES)]
        .chunks(4)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            let d = chunk.get(3).copied().unwrap_or(0);
            let id = u64::from(b) % IDS + 1;
            let sequence = if c & 128 == 0 {
                Sequence::Next
            } else {
                Sequence::Repeat
            };
            let stale = (d & 128 != 0).then_some(usize::from(d % 128));
            match a % 32 {
                0..=3 => Step::Open {
                    id,
                    at_target: a & 32 != 0,
                    target: u32::from(c % 5) + 1,
                    objects: u32::from(d % 6) + 1,
                    units: u32::from(d / 6 % 6) + 1,
                },
                4..=7 => Step::Grant {
                    id,
                    sequence,
                    ack: usize::from(c % 8),
                    objects: u32::from(d % 5),
                    units: u32::from(d / 5 % 5),
                },
                8..=9 => Step::Close {
                    id,
                    sequence,
                    ack: usize::from(c % 8),
                },
                10..=14 => Step::NextJob,
                15..=19 => Step::Page {
                    headers: u32::from(b % 3) + 1,
                    shave: u64::from(c % 3),
                    stale,
                },
                20..=22 if c & 1 == 1 => Step::Finish {
                    answer: Answer::Reply(REPLIES[usize::from(b) % REPLIES.len()]),
                    stale,
                },
                20..=22 => Step::Finish {
                    answer: match b % 10 {
                        0 => Answer::End(HeadersOutcomeCode::TargetNotRetained),
                        1 => Answer::End(HeadersOutcomeCode::NoLocatorIntersection),
                        2 => Answer::End(HeadersOutcomeCode::HistoryPruned),
                        3 => Answer::End(HeadersOutcomeCode::TargetNotSelected),
                        4 => Answer::End(HeadersOutcomeCode::SubscriptionSuperseded),
                        5 => Answer::Local,
                        6 => Answer::Capacity,
                        7 => Answer::Retry,
                        8 => Answer::Extends,
                        _ => Answer::ReachedAtOpen,
                    },
                    stale,
                },
                23 => Step::EndDue,
                24 => Step::NextTerminal,
                25 => Step::TerminalWritten(b % 10 != 9),
                26..=27 => Step::Select((d & 1 == 0).then_some(Tip {
                    branch: b % 3,
                    height: u32::from(c % 9),
                })),
                28..=29 => Step::Release(usize::from(b)),
                30 => Step::Cancel,
                _ => Step::Closed,
            }
        })
}
