//! A machine test for [`PublisherState`]: generated sequences of updates,
//! jobs, job results, terminal writes, and selected-chain and capacity
//! changes, checked against an independent model.
//!
//! The test plays the publisher task. It asks state for jobs, answers them
//! with pages and results as storage would, takes terminal outcomes and
//! reports their writes, and cancels or closes the stream. The model keeps
//! its own credit totals, cursors, update sequences, tombstones, slots,
//! stalls, serving order, terminal queue, and exit, and never calls the
//! publisher's helpers. Each step checks:
//!
//! - contiguous page position: every job and page extends the last header
//!   sent, or the start before the first page;
//! - authorized credit use: a job's header count is the room the model
//!   computes, and a page spends exactly what the model expects;
//! - one legal terminal per subscription, in the specification's window;
//! - terminal outcomes are written one at a time, in the order they ended;
//! - no job starts while a terminal outcome waits or after the publisher
//!   stops, and nothing at all happens after it stops;
//! - a result of an abandoned or settled job changes nothing.

use std::{
    collections::{BTreeMap, VecDeque},
    ops::ControlFlow,
};

use super::*;
#[cfg(test)]
use crate::zakura::header_sync::v9::test_support::OUTCOME_CODES;
use crate::zakura::header_sync::v9::{Cursor, Operation, RegtestMessage};
use crate::zakura::{wire_codec::decode_frame, Credit, Frame};
#[cfg(test)]
use proptest::prelude::*;
use zakura_chain::work::difficulty::U256;
use zakura_header_chain::{
    AlarmSet, ChainScore, EngineMode, FrontierSet, StateVersion, SuffixWork, VerifiedGeneration,
};

const HB: usize = crate::zakura::header_sync::config::REGTEST_HEADER_BYTES;
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

use crate::zakura::{
    header_sync::v9::{message_type, MAX_HS_SUBSCRIPTIONS},
    regulation::Totals,
};

/// Subscription IDs the steps use, so updates collide and IDs are reused.
const IDS: u64 = 3;

/// Heights at or below this are the same on every branch.
const FORK: u32 = 2;

/// The row's `max_live`.
const MAX_LIVE: usize = MAX_HS_SUBSCRIPTIONS as usize;

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

/// Why the model expects a subscription to wait.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ModelStall {
    Local,
    /// Waits on the test's capacity signal with this index.
    Capacity(usize),
}

/// The model of one live or closing subscription.
#[derive(Clone, Debug)]
struct ModelSub {
    target: Frontier,
    /// Whether the `Open`'s locator was the target.
    at_target: bool,
    /// The `Open`'s locator.
    start: Frontier,
    sequence: u32,
    granted: Sum,
    consumed: Sum,
    acknowledged: Cursor,
    /// Cursors sent and not acknowledged, oldest first.
    sent: VecDeque<Cursor>,
    pages: u64,
    /// The header the next page extends.
    last: Option<Frontier>,
    following: bool,
    closing: bool,
    /// This node's outcome, once queued.
    end: Option<HeadersOutcomeCode>,
    stall: Option<ModelStall>,
    checked_tip: Option<block::Hash>,
}

impl ModelSub {
    fn reached(&self) -> bool {
        self.following || self.at_target
    }

    /// The most headers the next page may carry, from the model's totals.
    fn room(&self) -> u32 {
        let limits = page_limits();
        let objects = self.granted.objects - self.consumed.objects;
        let bytes = self.granted.bytes - self.consumed.bytes;
        let fit = |bytes: u64| bytes.saturating_sub(fixed()) / entry();
        let room = u64::from(limits.max_headers)
            .min(objects)
            .min(fit(u64::from(limits.max_bytes)))
            .min(fit(bytes));
        u32::try_from(room).expect("at most three headers")
    }
}

/// A terminal outcome the model expects state to hold or the test to write.
#[derive(Clone, Debug)]
struct ModelTerminal {
    id: u64,
    outcome: HeadersOutcomeCode,
    abandons: Option<JobKey>,
}

/// An ended subscription's remains.
#[derive(Clone, Debug)]
struct ModelTombstone {
    id: u64,
    sequence: u32,
    close_received: bool,
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

/// The independent model of one session's publisher.
#[derive(Debug, Default)]
struct Model {
    subs: BTreeMap<u64, ModelSub>,
    tombstones: VecDeque<ModelTombstone>,
    /// Terminal outcomes queued in state, in order.
    queued: VecDeque<ModelTerminal>,
    /// The terminal outcome being written.
    writing: Option<ModelTerminal>,
    exit: Option<Exit>,
    selected: Option<Tip>,
    last_served: Option<u64>,
    in_progress: Option<JobRecord>,
}

impl Model {
    /// Terminal outcomes whose slot is still held: queued or being written.
    fn held_terminals(&self) -> usize {
        self.queued.len() + usize::from(self.writing.is_some())
    }

    /// Whether state may start no job now.
    fn busy(&self) -> bool {
        self.in_progress.is_some()
            || self.writing.is_some()
            || !self.queued.is_empty()
            || self.exit.is_some()
    }
}

/// The outcome window table of the specification.
fn model_legal(
    outcome: HeadersOutcomeCode,
    pages: bool,
    reached: bool,
    close_received: bool,
) -> bool {
    use HeadersOutcomeCode::*;
    match outcome {
        TargetNotRetained | NoLocatorIntersection | TargetNotSelected => !pages,
        HistoryPruned => !reached,
        SubscriptionSuperseded => true,
        SubscriptionClosed => close_received,
    }
}

/// The outcome this node sends for a job's `outcome`: the outcome if its
/// window is open, else `HistoryPruned` before the target, else
/// `SubscriptionSuperseded`.
fn model_adjust(outcome: HeadersOutcomeCode, pages: bool, reached: bool) -> HeadersOutcomeCode {
    if model_legal(outcome, pages, reached, false) {
        outcome
    } else if !reached {
        HeadersOutcomeCode::HistoryPruned
    } else {
        HeadersOutcomeCode::SubscriptionSuperseded
    }
}

/// The publisher's facts the model compares across a stale result.
fn observe(state: &PublisherState) -> Vec<(u64, Totals, Totals, Option<usize>)> {
    (1..=IDS)
        .filter_map(|id| {
            let credit = state.publications.credit(&id)?;
            Some((
                id,
                credit.granted(),
                credit.consumed(),
                state.publications.unacknowledged(&id),
            ))
        })
        .collect()
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

#[cfg(test)]
fn sequence() -> impl Strategy<Value = Sequence> {
    prop_oneof![4 => Just(Sequence::Next), 1 => Just(Sequence::Repeat)]
}

#[cfg(test)]
fn stale() -> impl Strategy<Value = Option<usize>> {
    prop::option::weighted(0.2, any::<usize>())
}

#[cfg(test)]
fn answer() -> impl Strategy<Value = Answer> {
    let own = OUTCOME_CODES
        .into_iter()
        .filter(|code| *code != HeadersOutcomeCode::SubscriptionClosed)
        .collect::<Vec<_>>();
    prop_oneof![
        prop::sample::select(own).prop_map(Answer::End),
        Just(Answer::Local),
        Just(Answer::Capacity),
        Just(Answer::Retry),
        Just(Answer::Extends),
        Just(Answer::ReachedAtOpen),
    ]
}

/// Steps that stop the publisher are rare, so most histories run long.
#[cfg(test)]
fn step() -> impl Strategy<Value = Step> {
    let id = 1..=IDS;
    prop_oneof![
        12 => (id.clone(), any::<bool>(), 1..=5u32, 1..=6u32, 1..=6u32).prop_map(
            |(id, at_target, target, objects, units)| Step::Open {
                id,
                at_target,
                target,
                objects,
                units,
            }
        ),
        12 => (id.clone(), sequence(), 0..8usize, 0..=4u32, 0..=4u32).prop_map(
            |(id, sequence, ack, objects, units)| Step::Grant {
                id,
                sequence,
                ack,
                objects,
                units,
            }
        ),
        12 => (id, sequence(), 0..8usize)
            .prop_map(|(id, sequence, ack)| Step::Close { id, sequence, ack }),
        16 => Just(Step::NextJob),
        10 => (1..=3u32, 0..=2u64, stale())
            .prop_map(|(headers, shave, stale)| Step::Page { headers, shave, stale }),
        10 => (answer(), stale()).prop_map(|(answer, stale)| Step::Finish { answer, stale }),
        12 => Just(Step::EndDue),
        12 => Just(Step::NextTerminal),
        12 => prop::bool::weighted(0.9).prop_map(Step::TerminalWritten),
        1 => Just(Step::Cancel),
        1 => Just(Step::Closed),
        10 => prop::option::weighted(0.9, (0..=2u8, 0..=8u32))
            .prop_map(|tip| Step::Select(tip.map(|(branch, height)| Tip { branch, height }))),
        4 => any::<usize>().prop_map(Step::Release),
    ]
}

/// The test playing the publisher task, with its model.
struct Machine {
    state: PublisherState,
    model: Model,
    /// Jobs that settled or that state abandoned.
    stale: Vec<JobRecord>,
    /// The terminal outcome the test is writing.
    writing: Option<Terminal>,
    signals: Vec<ServingCapacitySignal>,
}

impl Machine {
    fn new() -> Self {
        Self {
            state: PublisherState::new(),
            model: Model::default(),
            stale: Vec::new(),
            writing: None,
            signals: Vec::new(),
        }
    }

    fn selected(&self) -> Option<EngineSnapshot> {
        self.model.selected.map(Tip::snapshot)
    }

    fn run(&mut self, step: &Step) {
        // The task returns once the publisher stops, so no step follows.
        if self.model.exit.is_some() {
            return;
        }
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
                self.model.selected = tip;
                self.state.selected_changed();
                for sub in self.model.subs.values_mut() {
                    sub.stall = None;
                }
            }
            Step::Release(which) => {
                if self.signals.is_empty() {
                    return;
                }
                let index = which % self.signals.len();
                self.state
                    .capacity_released(std::slice::from_ref(&self.signals[index]));
                for sub in self.model.subs.values_mut() {
                    if sub.stall == Some(ModelStall::Capacity(index)) {
                        sub.stall = None;
                    }
                }
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
        let model = &mut self.model;
        let held = model.subs.len() + model.held_terminals();
        let expected =
            if model.subs.contains_key(&id) || model.tombstones.iter().any(|dead| dead.id == id) {
                Err(SubscriptionFault::ReusedKey.into())
            } else if held >= 2 * MAX_LIVE {
                Err(SubscriptionFault::NoSlot { held, limit: 1 }.into())
            } else {
                Ok(Applied::Live)
            };
        assert_eq!(
            self.state.apply(&update, page_limits()),
            expected,
            "{PROPERTY}"
        );
        if expected.is_err() {
            return;
        }
        let keep = MAX_LIVE.saturating_sub(1).saturating_sub(model.subs.len());
        while model.tombstones.len() > keep {
            model.tombstones.pop_front();
        }
        model.subs.insert(
            id,
            ModelSub {
                target,
                at_target,
                start,
                sequence: 0,
                granted: Sum::default().add(u64::from(credit.objects), u64::from(credit.bytes)),
                consumed: Sum::default(),
                acknowledged,
                sent: VecDeque::new(),
                pages: 0,
                last: None,
                following: false,
                closing: false,
                end: None,
                stall: None,
                checked_tip: None,
            },
        );
    }

    /// Send a `Grant` or `Close` and check the model's verdict.
    fn update(
        &mut self,
        operation: Operation,
        id: u64,
        sequence: Sequence,
        ack: usize,
        added: Credit,
    ) {
        let model = &mut self.model;
        let live = model.subs.get(&id);
        let base = live.map(|sub| sub.sequence).or_else(|| {
            model
                .tombstones
                .iter()
                .find(|dead| dead.id == id)
                .map(|dead| dead.sequence)
        });
        let got = match sequence {
            Sequence::Next => base.unwrap_or(0) + 1,
            Sequence::Repeat => base.unwrap_or(0),
        };
        let bogus = Cursor {
            height: block::Height(99),
            hash: block::Hash([0xee; 32]),
            headers: 0,
            bytes: 0,
        };
        // The current acknowledgement, a sent page, or an unknown cursor.
        let acknowledged = live.map_or(bogus, |sub| {
            let index = ack % (sub.sent.len() + 2);
            match index {
                0 => sub.acknowledged,
                index if index <= sub.sent.len() => sub.sent[index - 1],
                _ => bogus,
            }
        });
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
        let empty = added.objects == 0 && added.bytes == 0;
        let expected: Result<Applied, PublisherFault> = if let Some(sub) = model.subs.get_mut(&id) {
            let through = if acknowledged == sub.acknowledged {
                Some(None)
            } else {
                sub.sent
                    .iter()
                    .position(|cursor| *cursor == acknowledged)
                    .map(Some)
            };
            if sub.closing {
                Err(SubscriptionFault::AfterClose.into())
            } else if got != sub.sequence + 1 {
                Err(SubscriptionFault::Sequence {
                    expected: sub.sequence + 1,
                    got,
                }
                .into())
            } else if through.is_none() {
                Err(SubscriptionFault::UnknownAcknowledgement.into())
            } else if operation == Operation::Grant && empty {
                Err(SubscriptionFault::EmptyCredit.into())
            } else {
                if let Some(Some(index)) = through {
                    sub.acknowledged = sub.sent.drain(..=index).next_back().expect("found");
                }
                sub.sequence = got;
                sub.granted = sub
                    .granted
                    .add(u64::from(added.objects), u64::from(added.bytes));
                sub.closing |= operation == Operation::Close;
                sub.stall = None;
                Ok(Applied::Live)
            }
        } else if let Some(index) = model.tombstones.iter().position(|dead| dead.id == id) {
            let dead = &mut model.tombstones[index];
            if dead.close_received {
                Err(SubscriptionFault::AfterClose.into())
            } else if got != dead.sequence + 1 {
                Err(SubscriptionFault::Sequence {
                    expected: dead.sequence + 1,
                    got,
                }
                .into())
            } else if operation == Operation::Close {
                model.tombstones.remove(index);
                Ok(Applied::Crossed)
            } else if empty {
                Err(SubscriptionFault::EmptyCredit.into())
            } else {
                dead.sequence = got;
                Ok(Applied::Crossed)
            }
        } else {
            Err(SubscriptionFault::Unknown.into())
        };
        assert_eq!(
            self.state.apply(&update, page_limits()),
            expected,
            "{PROPERTY}"
        );
    }

    /// The job the model expects next, superseding followers on the way.
    fn expected_job(&mut self) -> Option<(u64, Option<Frontier>, bool, u32, block::Hash)> {
        let model = &mut self.model;
        if model.busy() {
            return None;
        }
        let ids: Vec<u64> = model.subs.keys().copied().collect();
        let order = match model.last_served {
            Some(last) => ids
                .iter()
                .filter(|id| **id > last)
                .chain(ids.iter().filter(|id| **id <= last))
                .copied()
                .collect(),
            None => ids,
        };
        for id in order {
            let tip = model.selected;
            let Some(sub) = model.subs.get_mut(&id) else {
                continue;
            };
            if sub.closing || sub.end.is_some() {
                continue;
            }
            // A follower's last header sent, or the target it reached.
            let followed = sub.last.filter(|_| sub.following);
            if let (Some(last), Some(tip)) = (followed, tip) {
                if tip.frontier() != last && tip.height <= last.height.0 {
                    sub.end = Some(HeadersOutcomeCode::SubscriptionSuperseded);
                    continue;
                }
            }
            let room = sub.room();
            if sub.stall.is_some() {
                continue;
            }
            let job = if let Some(last) = followed {
                let Some(tip) = tip else {
                    continue;
                };
                if tip.height <= last.height.0
                    || (room == 0 && sub.checked_tip == Some(tip.frontier().hash))
                {
                    continue;
                }
                (id, Some(last), true, room, tip.frontier().hash)
            } else {
                if room == 0 {
                    continue;
                }
                (id, sub.last, false, room, sub.target.hash)
            };
            model.last_served = Some(id);
            return Some(job);
        }
        None
    }

    fn next_job(&mut self) {
        let busy = self.model.busy();
        let expected = self.expected_job();
        let selected = self.selected();
        let job = self.state.next_job::<HB>(selected.as_ref(), page_limits());
        if busy {
            assert!(
                job.is_none(),
                "{PROPERTY}: no job starts beside another, a waiting terminal outcome, or after exit: {:?}",
                self.model);
            return;
        }
        let found = job.as_ref().map(|job| {
            (
                job.id,
                job.parent,
                job.following,
                job.count,
                job.lease_target,
            )
        });
        assert_eq!(found, expected, "{PROPERTY}: the next job");
        let Some(job) = job else {
            return;
        };
        let sub = &self.model.subs[&job.id];
        self.model.in_progress = Some(JobRecord {
            job,
            start: sub.start,
            target: sub.target,
            at_target: sub.at_target,
            seen: self.model.selected,
        });
    }

    /// The job a result answers: a stale one, or the one in progress.
    fn answering(&self, stale: Option<usize>) -> Option<(JobRecord, bool)> {
        match (stale, self.stale.is_empty()) {
            (Some(index), false) => Some((self.stale[index % self.stale.len()].clone(), true)),
            _ => self.model.in_progress.clone().map(|record| (record, false)),
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
            frame: Frame {
                message_type: message_type::HEADERS,
                flags: 0,
                payload: vec![0; usize::try_from(bytes).expect("small page")],
            },
        };
        let before = observe(&self.state);
        let committed = self.state.commit(job.key, &page);
        if is_stale {
            assert!(!committed, "{PROPERTY}: a stale page is never sent");
            assert_eq!(
                observe(&self.state),
                before,
                "{PROPERTY}: a stale page spends nothing"
            );
            return;
        }
        self.settle(record.clone());
        let sub = self
            .model
            .subs
            .get_mut(&job.id)
            .expect("a running job's subscription is live");
        assert_eq!(
            sub.last, job.parent,
            "{PROPERTY}: the page extends the last header sent"
        );
        let expected = !sub.closing;
        assert_eq!(committed, expected, "{PROPERTY}: the commit verdict");
        if !committed {
            return;
        }
        let unspent = Sum {
            objects: sub.granted.objects - sub.consumed.objects,
            bytes: sub.granted.bytes - sub.consumed.bytes,
        };
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
        sub.pages += 1;
        sub.last = Some(last);
        sub.following |= page.reaches_target;
        sub.checked_tip = None;
        assert_eq!(
            self.state
                .publications
                .credit(&job.id)
                .map(|credit| credit.consumed()),
            Some(Totals {
                objects: sub.consumed.objects,
                bytes: sub.consumed.bytes,
            }),
            "{PROPERTY}: state's consumed credit matches the model's"
        );
    }

    fn finish(&mut self, answer: Answer, stale: Option<usize>) {
        let Some((record, is_stale)) = self.answering(stale) else {
            return;
        };
        self.answer(record, is_stale, answer);
    }

    /// Report a result other than a page for `record`'s job.
    fn answer(&mut self, record: JobRecord, is_stale: bool, answer: Answer) {
        let job = &record.job;
        let reachable = record.at_target && job.parent.is_none();
        let answer = match answer {
            Answer::ReachedAtOpen if !reachable => Answer::Retry,
            Answer::Extends if job.count > 0 => Answer::Retry,
            answer => answer,
        };
        let output = match answer {
            Answer::End(code) => Output::End(code),
            Answer::Local => Output::Stall(Stall::Local, "test"),
            Answer::Capacity => {
                self.signals.push(ServingCapacitySignal::default());
                let signal = self.signals.last().expect("just pushed").clone();
                Output::Stall(Stall::Capacity(signal), "test")
            }
            Answer::Retry => Output::Retry("test"),
            Answer::Extends => Output::Extends(job.lease_target),
            Answer::ReachedAtOpen => Output::ReachedAtOpen(record.target),
        };
        let latest = self.selected();
        let before = observe(&self.state);
        self.state.finish(job.key, output, latest.as_ref());
        if is_stale {
            assert_eq!(
                observe(&self.state),
                before,
                "{PROPERTY}: a stale result changes nothing"
            );
            return;
        }
        self.settle(record.clone());
        let moved = self.model.selected.map(Tip::frontier) != record.seen.map(Tip::frontier);
        let signal = self.signals.len().checked_sub(1);
        let sub = self
            .model
            .subs
            .get_mut(&job.id)
            .expect("a running job's subscription is live");
        match answer {
            Answer::End(code) => {
                if !sub.closing {
                    sub.end = Some(model_adjust(code, sub.pages > 0, sub.reached()));
                }
            }
            Answer::Local if !moved => sub.stall = Some(ModelStall::Local),
            Answer::Capacity if !moved => {
                sub.stall = Some(ModelStall::Capacity(signal.expect("pushed")));
            }
            Answer::Extends if !moved => sub.checked_tip = Some(job.lease_target),
            Answer::ReachedAtOpen => {
                sub.following = true;
                sub.last = Some(record.target);
            }
            _ => {}
        }
    }

    /// Retire the job in progress after its result.
    fn settle(&mut self, record: JobRecord) {
        self.model.in_progress = None;
        self.stale.push(record);
    }

    fn end_due(&mut self) {
        let ended = self.state.end_due::<HB>();
        let model = &mut self.model;
        let expected: Vec<(u64, HeadersOutcomeCode)> = model
            .subs
            .iter()
            .filter_map(|(&id, sub)| match (sub.end, sub.closing) {
                (Some(code), _) => Some((id, code)),
                (None, true) => Some((id, HeadersOutcomeCode::SubscriptionClosed)),
                (None, false) => None,
            })
            .collect();
        let codes: Vec<HeadersOutcomeCode> = expected.iter().map(|(_, code)| *code).collect();
        assert_eq!(
            ended, codes,
            "{PROPERTY}: the terminal outcomes and their order"
        );
        for (id, outcome) in expected {
            let sub = model
                .subs
                .remove(&id)
                .expect("an ended subscription was live");
            assert!(
                model_legal(outcome, sub.pages > 0, sub.reached(), sub.closing),
                "{PROPERTY}: {outcome:?} is legal for {sub:?}"
            );
            let abandoned = model.in_progress.take_if(|record| record.job.id == id);
            model.queued.push_back(ModelTerminal {
                id,
                outcome,
                abandons: abandoned.as_ref().map(|record| record.job.key),
            });
            self.stale.extend(abandoned);
            model.tombstones.push_back(ModelTombstone {
                id,
                sequence: sub.sequence,
                close_received: sub.closing,
            });
            while model.tombstones.len() > 2 * MAX_LIVE {
                model.tombstones.pop_front();
            }
        }
    }

    /// Take the next terminal outcome to write, which the model expects only
    /// when none is being written.
    fn next_terminal(&mut self) {
        let terminal = self.state.next_terminal();
        let model = &mut self.model;
        let expected = if model.writing.is_some() {
            None
        } else {
            model.queued.pop_front()
        };
        let found = terminal.as_ref().map(|terminal| {
            let Ok(Message::HeadersOutcome(outcome)) =
                decode_frame::<RegtestMessage>(&terminal.frame)
            else {
                panic!("a terminal frame is an outcome");
            };
            (outcome.subscription_id, outcome.outcome, terminal.abandons)
        });
        assert_eq!(
            found,
            expected
                .as_ref()
                .map(|terminal| (terminal.id, terminal.outcome, terminal.abandons)),
            "{PROPERTY}: the next terminal outcome, one at a time and in order"
        );
        if terminal.is_some() {
            model.writing = expected;
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
        drop(terminal);
        self.model.writing = None;
        let expected = if written {
            ControlFlow::Continue(())
        } else {
            self.model.exit = Some(Exit::Closed);
            ControlFlow::Break(Exit::Closed)
        };
        assert_eq!(
            self.state.terminal_written(written),
            expected,
            "{PROPERTY}: a refused terminal outcome stops the publisher"
        );
        self.check_stopped();
    }

    /// Cancel the session, or close the stream, and check that state stops.
    fn stop(&mut self, exit: Exit) {
        let stopped = |state: &mut PublisherState, exit| match exit {
            Exit::Cancelled => state.cancelled(),
            Exit::Closed => state.closed(),
        };
        assert_eq!(
            stopped(&mut self.state, exit),
            exit,
            "{PROPERTY}: the reason to stop"
        );
        self.model.exit = Some(exit);
        let other = match exit {
            Exit::Cancelled => Exit::Closed,
            Exit::Closed => Exit::Cancelled,
        };
        assert_eq!(
            stopped(&mut self.state, other),
            exit,
            "{PROPERTY}: the first reason to stop wins"
        );
        self.check_stopped();
    }

    /// A stopped publisher starts no job and offers no terminal outcome.
    fn check_stopped(&mut self) {
        if self.model.exit.is_none() {
            return;
        }
        let selected = self.selected();
        assert!(
            self.state
                .next_job::<HB>(selected.as_ref(), page_limits())
                .is_none(),
            "{PROPERTY}: no job starts after the publisher stops"
        );
        assert!(
            self.state.next_terminal().is_none(),
            "{PROPERTY}: no terminal outcome is written after the publisher stops"
        );
    }
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(std::env::var("HEADER_SYNC_PROPTEST_CASES").ok().and_then(|cases| cases.parse().ok()).unwrap_or(256)))]

    /// Generated publisher histories keep page position contiguous, spend
    /// only granted credit, end each subscription once in its window, and
    /// let no stale job result change anything.
    #[test]
    fn publisher_state_matches_the_model(steps in prop::collection::vec(step(), 1..64)) {
        run(steps, |_, _| {});
    }
}

/// The worked scenario: a job for subscription A, A ends and a new A opens,
/// and the old job's page arrives under a changed selected chain. State sends
/// no old page, spends none of the new A's credit, and keeps serving it.
#[test]
fn a_stale_page_after_replacement_spends_no_replacement_credit() {
    let a = 1;
    let mut machine = Machine::new();
    machine.run(&Step::Open {
        id: a,
        at_target: false,
        target: 5,
        objects: 6,
        units: 6,
    });
    machine.run(&Step::NextJob);
    let old = machine
        .model
        .in_progress
        .clone()
        .expect("A has a page to read");
    let close = |id| Step::Close {
        id,
        sequence: Sequence::Next,
        ack: 0,
    };
    machine.run(&close(a));
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    machine.run(&Step::TerminalWritten(true));
    // Another subscription's `Open` clears A's tombstone, so A can open again.
    machine.run(&Step::Open {
        id: 2,
        at_target: false,
        target: 5,
        objects: 1,
        units: 1,
    });
    machine.run(&close(2));
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    machine.run(&Step::TerminalWritten(true));
    machine.run(&Step::Open {
        id: a,
        at_target: false,
        target: 5,
        objects: 6,
        units: 6,
    });
    machine.run(&Step::Select(Some(Tip {
        branch: 1,
        height: 7,
    })));
    let page = Page {
        parent: frontier(0, 0),
        last: frontier(0, 3),
        target: frontier(0, 5),
        count: 3,
        reaches_target: false,
        frame: Frame {
            message_type: message_type::HEADERS,
            flags: 0,
            payload: vec![0; usize::try_from(fixed() + 3 * entry()).expect("small page")],
        },
    };
    assert!(
        !machine.state.commit(old.job.key, &page),
        "no old page is sent"
    );
    let latest = machine.selected();
    machine.state.finish(
        old.job.key,
        Output::Stall(Stall::Local, "test"),
        latest.as_ref(),
    );
    assert_eq!(
        machine
            .state
            .publications
            .credit(&a)
            .map(|credit| credit.consumed()),
        Some(Totals::default()),
        "the new A spent no credit"
    );
    // The old result stalled nothing: the new A gets its first page job.
    machine.run(&Step::NextJob);
    let new = machine
        .model
        .in_progress
        .as_ref()
        .expect("the new A has a page to read");
    assert_eq!((new.job.id, new.job.parent), (a, None));
    assert_ne!(new.job.key, old.job.key);
}

/// A capacity stall waits for its own signal: releasing an older one leaves
/// it stalled, and releasing its own lets the next job start.
#[test]
fn a_capacity_stall_waits_for_its_own_signal() {
    let mut machine = Machine::new();
    machine.run(&Step::Open {
        id: 1,
        at_target: false,
        target: 5,
        objects: 6,
        units: 6,
    });
    for _ in 0..2 {
        // A selected-chain change clears the previous stall.
        machine.run(&Step::Select(None));
        machine.run(&Step::NextJob);
        assert!(machine.model.in_progress.is_some(), "the path has a page");
        machine.run(&Step::Finish {
            answer: Answer::Capacity,
            stale: None,
        });
    }
    let [older, own] = <[_; 2]>::try_from(machine.signals.clone()).expect("two refusals");
    machine.state.capacity_released(&[older]);
    machine.run(&Step::NextJob);
    assert!(machine.model.in_progress.is_none(), "still stalled");
    machine.state.capacity_released(&[own]);
    machine.model.subs.get_mut(&1).expect("live").stall = None;
    machine.run(&Step::NextJob);
    assert!(
        machine.model.in_progress.is_some(),
        "the release lets it retry"
    );
}

/// While a terminal outcome waits for the stream, another subscription's job
/// in progress still commits its page. No new job starts until the write
/// finishes.
#[test]
fn another_subscriptions_page_commits_while_a_terminal_waits() {
    let mut machine = Machine::new();
    for id in [1, 2] {
        machine.run(&Step::Open {
            id,
            at_target: false,
            target: 5,
            objects: 6,
            units: 6,
        });
    }
    machine.run(&Step::NextJob);
    let running = machine
        .model
        .in_progress
        .clone()
        .expect("subscription 1 has a page to read");
    assert_eq!(running.job.id, 1);
    machine.run(&Step::Close {
        id: 2,
        sequence: Sequence::Next,
        ack: 0,
    });
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    assert!(
        machine.writing.is_some(),
        "subscription 2's outcome is written"
    );
    machine.run(&Step::Page {
        headers: 3,
        shave: 0,
        stale: None,
    });
    assert_eq!(machine.model.subs[&1].pages, 1, "the page committed");
    machine.run(&Step::NextJob);
    assert!(machine.model.in_progress.is_none(), "the write blocks jobs");
    machine.run(&Step::TerminalWritten(true));
    machine.run(&Step::NextJob);
    assert!(
        machine.model.in_progress.is_some(),
        "the next job starts after the write"
    );
}

/// Two subscriptions that close together, with the first outcome taken.
#[cfg(test)]
fn two_closed_one_writing() -> Machine {
    let mut machine = Machine::new();
    for id in [1, 2] {
        machine.run(&Step::Open {
            id,
            at_target: false,
            target: 5,
            objects: 6,
            units: 6,
        });
        machine.run(&Step::Close {
            id,
            sequence: Sequence::Next,
            ack: 0,
        });
    }
    machine.run(&Step::EndDue);
    machine.run(&Step::NextTerminal);
    assert_eq!(machine.model.queued.len(), 1, "one outcome still waits");
    // The second outcome waits for the first write.
    machine.run(&Step::NextTerminal);
    assert_eq!(machine.model.queued.len(), 1, "one write at a time");
    machine
}

/// Cancellation during a terminal write stops the publisher: the queued
/// outcome is never offered, and no job starts.
#[test]
fn cancellation_during_a_terminal_write_stops_the_publisher() {
    let mut machine = two_closed_one_writing();
    machine.run(&Step::Cancel);
    assert_eq!(machine.model.exit, Some(Exit::Cancelled));
}

/// A stream that refuses a terminal outcome stops the publisher: the queued
/// outcome is never offered, and no job starts.
#[test]
fn a_refused_terminal_write_stops_the_publisher() {
    let mut machine = two_closed_one_writing();
    machine.run(&Step::TerminalWritten(false));
    assert_eq!(machine.model.exit, Some(Exit::Closed));
}

const PROPERTY: &str = "header/publisher-policy";

/// Applicable operations in a decoder-v1 publisher history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct HeaderPublisherReplaySummary {
    /// Operations executed before publisher shutdown.
    pub transitions: usize,
    /// New jobs issued.
    pub jobs: usize,
    /// Pages that consumed credit.
    pub pages: usize,
    /// Header credit consumed by those pages.
    pub headers: u64,
    /// Byte credit consumed by those pages.
    pub bytes: u64,
    /// Results delivered for an abandoned or settled job.
    pub stale_results: usize,
    /// Successful writes of a pending terminal outcome.
    pub terminals: usize,
}

/// Replay decoder-v1 quadruples against the existing publisher model.
/// Read at most 512 bytes. The model derives page sizes from the wire layout.
pub fn replay_header_publisher_bytes(data: &[u8]) -> HeaderPublisherReplaySummary {
    run(decode(data), |_, _| {})
}

fn decode(data: &[u8]) -> impl Iterator<Item = Step> + '_ {
    data[..data.len().min(512)].chunks(4).map(|chunk| {
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

fn run(
    steps: impl IntoIterator<Item = Step>,
    mut after_step: impl FnMut(&mut Machine, &Step),
) -> HeaderPublisherReplaySummary {
    let mut machine = Machine::new();
    let mut summary = HeaderPublisherReplaySummary::default();
    for step in steps {
        if machine.model.exit.is_some() {
            break;
        }
        summary.transitions += 1;
        let was_idle = machine.model.in_progress.is_none();
        let consumed = || {
            machine
                .model
                .subs
                .values()
                .fold((0u64, 0u64), |(objects, bytes), sub| {
                    (objects + sub.consumed.objects, bytes + sub.consumed.bytes)
                })
        };
        let before = consumed();
        match &step {
            Step::Page { stale, .. } | Step::Finish { stale, .. } => {
                summary.stale_results +=
                    usize::from(machine.answering(*stale).is_some_and(|(_, stale)| stale));
            }
            Step::TerminalWritten(true) => {
                summary.terminals += usize::from(machine.writing.is_some())
            }
            _ => {}
        }
        machine.run(&step);
        if matches!(step, Step::NextJob) && was_idle && machine.model.in_progress.is_some() {
            summary.jobs += 1;
        }
        if matches!(step, Step::Page { .. }) {
            let after = machine
                .model
                .subs
                .values()
                .fold((0u64, 0u64), |(objects, bytes), sub| {
                    (objects + sub.consumed.objects, bytes + sub.consumed.bytes)
                });
            summary.pages += usize::from(after.0 > before.0);
            summary.headers += after.0.saturating_sub(before.0);
            summary.bytes += after.1.saturating_sub(before.1);
        }
        after_step(&mut machine, &step);
        if machine.model.exit.is_none() {
            let expected: Vec<_> = machine
                .model
                .subs
                .iter()
                .map(|(id, sub)| {
                    (
                        *id,
                        Totals {
                            objects: sub.granted.objects,
                            bytes: sub.granted.bytes,
                        },
                        Totals {
                            objects: sub.consumed.objects,
                            bytes: sub.consumed.bytes,
                        },
                        Some(sub.sent.len()),
                    )
                })
                .collect();
            assert_eq!(
                observe(&machine.state),
                expected,
                "{PROPERTY}: credit ledger"
            );
        }
    }
    summary
}

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn publisher_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_header_publisher_bytes(&bytes);
    }
}

#[cfg(test)]
const WITNESS: &[u8] = &[
    0, 0, 4, 35, 10, 0, 0, 0, 15, 0, 0, 0, 8, 0, 0, 0, 23, 0, 0, 0, 24, 0, 0, 0, 25, 0, 0, 0, 15,
    0, 0, 128,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "CI retains these applicability counters"
)]
fn publisher_witness_exercises_credit_terminal_and_stale_page() {
    let summary = replay_header_publisher_bytes(WITNESS);
    assert_eq!(
        summary,
        HeaderPublisherReplaySummary {
            transitions: 8,
            jobs: 1,
            pages: 1,
            headers: 1,
            bytes: 263,
            stale_results: 1,
            terminals: 1
        }
    );
    assert_eq!(
        replay_header_publisher_bytes(&[]),
        HeaderPublisherReplaySummary::default()
    );
    println!("tier=Machine target=header_publisher decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_lost_credit() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::Open { .. }) {
            machine.state.publications.end(&1);
        }
    });
}

#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_lost_job() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::Open { .. }) {
            machine.state.next_job::<HB>(None, page_limits());
        }
    });
}

#[test]
#[should_panic(expected = "header/publisher-policy")]
fn publisher_witness_rejects_lost_terminal() {
    run(decode(WITNESS), |machine, step| {
        if matches!(step, Step::EndDue) {
            machine.state.next_terminal();
        }
    });
}
