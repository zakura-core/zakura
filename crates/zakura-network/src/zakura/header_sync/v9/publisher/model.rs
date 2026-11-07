//! The publisher credit and terminal model for [`PublisherState`]: generated sequences of updates,
//! jobs, job results, terminal writes, and selected-chain and capacity
//! changes, checked against an independent model.
//!
//! [`Machine`] plays the publisher task over the real [`PublisherOwner`]. It asks state for jobs, answers them
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
//! - a result of an abandoned or settled job changes nothing;
//! - each storage reply means what the model's own table says, through state's
//!   selection functions;
//! - the owner holds exactly one slot per live subscription and unwritten
//!   terminal outcome, and one capacity signal per distinct stalled wait.
//!
//! [`trace_header_publisher_bytes`] drives the model from fuzz bytes and records
//! each operation's state read from the owner beside the state the model
//! derived.

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
        v9::{Cursor, Operation, RegtestMessage, MAX_HS_SUBSCRIPTIONS},
    },
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

/// What state's selection functions make of `reply` to `record`'s job.
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

/// The model's own table of what `reply` to `record`'s job means. `None` means
/// the job goes on, which the machine plays as a lost read.
fn reply_meaning(reply: Reply, record: &JobRecord) -> Option<Answer> {
    use HeadersOutcomeCode::*;
    let following = record.job.following;
    let parent = record.job.parent;
    Some(match reply {
        Reply::ProbeSelected => return None,
        Reply::ProbeNotSelected => Answer::End(TargetNotSelected),
        Reply::ProbeNotRetained => Answer::End(TargetNotRetained),
        Reply::Refused(LeaseRefused::NoLocatorIntersection) if following => {
            Answer::End(SubscriptionSuperseded)
        }
        Reply::Refused(LeaseRefused::NoLocatorIntersection) => Answer::End(NoLocatorIntersection),
        Reply::Refused(LeaseRefused::TargetNotRetained) if following => Answer::Local,
        Reply::Refused(LeaseRefused::TargetNotRetained) => Answer::End(TargetNotRetained),
        Reply::Refused(LeaseRefused::HistoryPruned) => Answer::End(HistoryPruned),
        Reply::Refused(LeaseRefused::Busy) => Answer::Local,
        Reply::Lease | Reply::LeaseWrongParent if parent.is_none() && record.at_target => {
            Answer::ReachedAtOpen
        }
        Reply::LeaseWrongParent if parent.is_some() => Answer::Local,
        Reply::Lease | Reply::LeaseWrongParent => return None,
        Reply::EmptyPage if following => Answer::Local,
        Reply::EmptyPage => Answer::End(HistoryPruned),
        Reply::CheckExtends => Answer::Extends,
        Reply::CheckElsewhere | Reply::CheckNotSelected => Answer::End(SubscriptionSuperseded),
    })
}

/// The output state hears for `answer` to `record`'s job. A capacity refusal
/// needs the owner's wait, so its caller builds that output.
fn answer_output(answer: Answer, record: &JobRecord) -> Output {
    match answer {
        Answer::End(code) => Output::End(code),
        Answer::Local => Output::Stall(Stall::Local, "test"),
        Answer::Extends => Output::Extends(record.job.lease_target),
        Answer::ReachedAtOpen => Output::ReachedAtOpen(record.target),
        Answer::Retry | Answer::Capacity | Answer::Reply(_) => Output::Retry,
    }
}

/// `output` without its metric label, which only the runtime owner reads.
fn unlabelled(output: Output) -> Output {
    match output {
        Output::Stall(stall, _) => Output::Stall(stall, ""),
        output => output,
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

/// The test playing the publisher task, with its model.
struct Machine {
    owner: PublisherOwner,
    model: Model,
    /// Jobs that settled or that state abandoned.
    stale: Vec<JobRecord>,
    /// The terminal outcome the test is writing.
    writing: Option<OwnedTerminal>,
    /// Each capacity refusal's wait and signal.
    signals: Vec<(CapacityWait, ServingCapacitySignal)>,
}

impl Machine {
    fn new() -> Self {
        Self {
            owner: PublisherOwner::new(page_limits()),
            model: Model::default(),
            stale: Vec::new(),
            writing: None,
            signals: Vec::new(),
        }
    }

    fn selected(&self) -> Option<EngineSnapshot> {
        self.model.selected.map(Tip::snapshot)
    }

    /// The state read from the production owner.
    fn actual_view(&self) -> HeaderPublisherView {
        let state = &self.owner.state;
        let activity = state.activity();
        HeaderPublisherView {
            subscriptions: (1..=IDS)
                .filter_map(|id| {
                    let credit = state.publications().credit(&id)?;
                    let last = state.publications().last_sent(&id)?;
                    Some(HeaderPublisherSubscriptionView {
                        id,
                        granted: (credit.granted().objects, credit.granted().bytes),
                        consumed: (credit.consumed().objects, credit.consumed().bytes),
                        unacknowledged: state.publications().unacknowledged(&id)?,
                        last_height: last.height.0,
                        last_hash: last.hash,
                        closing: state.publications().is_closing(&id),
                    })
                })
                .collect(),
            tombstones: state.publications().tombstones(),
            job: activity.job,
            terminals: activity.terminals,
            writing: activity.writing,
            slots: self.owner.slots.held(),
            exit: activity.exit.map(exit_name),
        }
    }

    /// The state the model derived.
    fn expected_view(&self) -> HeaderPublisherView {
        let model = &self.model;
        HeaderPublisherView {
            subscriptions: model
                .subs
                .iter()
                .map(|(id, sub)| {
                    let last = sub.sent.back().unwrap_or(&sub.acknowledged);
                    HeaderPublisherSubscriptionView {
                        id: *id,
                        granted: (sub.granted.objects, sub.granted.bytes),
                        consumed: (sub.consumed.objects, sub.consumed.bytes),
                        unacknowledged: sub.sent.len(),
                        last_height: last.height.0,
                        last_hash: last.hash,
                        closing: sub.closing,
                    }
                })
                .collect(),
            tombstones: model.tombstones.len(),
            job: model.in_progress.as_ref().map(|record| record.job.id),
            terminals: model.queued.iter().map(|terminal| terminal.id).collect(),
            writing: model.writing.is_some(),
            slots: model.subs.len() + model.held_terminals(),
            exit: model.exit.map(exit_name),
        }
    }

    /// The owner matches the model after every operation, in every field of the
    /// view: credit, cursors, tombstones, the job, terminal outcomes, slots, and
    /// the exit. A trace's failure reports any difference, so the simulator's
    /// machine adapter sees it.
    fn check(&mut self) {
        assert_eq!(
            self.actual_view(),
            self.expected_view(),
            "{PROPERTY}: the owner's view"
        );
        if let Some(record) = &self.model.in_progress {
            assert!(
                self.owner.state.is_current(record.job.key),
                "{PROPERTY}: the job in progress keeps its exact key"
            );
        }
        if self.model.exit.is_some() {
            return;
        }
        let mut stalled: Vec<usize> = self
            .model
            .subs
            .values()
            .filter_map(|sub| match sub.stall {
                Some(ModelStall::Capacity(index)) => Some(index),
                _ => None,
            })
            .collect();
        stalled.sort_unstable();
        stalled.dedup();
        assert_eq!(
            self.owner.capacity_signals().len(),
            stalled.len(),
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
                self.model.selected = tip;
                self.owner.state.selected_changed();
                for sub in self.model.subs.values_mut() {
                    sub.stall = None;
                }
            }
            Step::Release(which) => {
                if self.signals.is_empty() {
                    return;
                }
                let index = which % self.signals.len();
                self.signals[index].1.release();
                self.owner.capacity_released();
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
        assert_eq!(self.owner.apply(&update), expected, "{PROPERTY}");
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
        assert_eq!(self.owner.apply(&update), expected, "{PROPERTY}");
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
        let job = self.owner.state.next_job::<HB>(selected.as_ref());
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
        let reaches = reaches_target(job.following, last.hash, job.initial_target);
        assert_eq!(
            reaches,
            !job.following && last == record.target,
            "{PROPERTY}: whether the page reaches its target"
        );
        let page = Page {
            parent,
            last,
            target,
            count,
            reaches_target: reaches,
            payload_len: usize::try_from(bytes).expect("small page"),
        };
        let before = self.actual_view();
        let committed = self.owner.state.commit(job.key, &page);
        if is_stale {
            assert!(!committed, "{PROPERTY}: a stale page is never sent");
            assert_eq!(
                self.actual_view(),
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
            Answer::Reply(reply) if reply_applies(reply, job) => {
                let meaning = reply_meaning(reply, &record);
                assert_eq!(
                    selected_output(reply, &record).map(unlabelled),
                    meaning.map(|meaning| unlabelled(answer_output(meaning, &record))),
                    "{PROPERTY}: what {reply:?} means for {job:?}"
                );
                meaning.unwrap_or(Answer::Retry)
            }
            Answer::Reply(_) => Answer::Retry,
            answer => answer,
        };
        let answer = match answer {
            Answer::ReachedAtOpen if !reachable => Answer::Retry,
            Answer::Extends if job.count > 0 => Answer::Retry,
            answer => answer,
        };
        let output = if let Answer::Capacity = answer {
            let signal = ServingCapacitySignal::default();
            let wait = self.owner.waits.register(signal.clone());
            self.signals.push((wait, signal));
            Output::Stall(Stall::Capacity(wait), "test")
        } else {
            answer_output(answer, &record)
        };
        let latest = self.selected();
        let before = self.actual_view();
        self.owner.state.finish(job.key, output, latest.as_ref());
        if is_stale {
            assert_eq!(
                self.actual_view(),
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
        let ended = self.owner.end_due();
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
            if let Some(record) = &abandoned {
                assert!(
                    !self.owner.state.is_current(record.job.key),
                    "{PROPERTY}: ending a subscription abandons its job"
                );
            }
            model.queued.push_back(ModelTerminal { id, outcome });
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
        let terminal = self.owner.next_terminal::<HB>();
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
            (outcome.subscription_id, outcome.outcome)
        });
        assert_eq!(
            found,
            expected
                .as_ref()
                .map(|terminal| (terminal.id, terminal.outcome)),
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
        let slot = terminal.slot;
        drop(terminal);
        self.model.writing = None;
        let expected = if written {
            ControlFlow::Continue(())
        } else {
            self.model.exit = Some(Exit::Closed);
            ControlFlow::Break(Exit::Closed)
        };
        assert_eq!(
            self.owner.state.terminal_written(slot, written),
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
            stopped(&mut self.owner.state, exit),
            exit,
            "{PROPERTY}: the reason to stop"
        );
        self.model.exit = Some(exit);
        let other = match exit {
            Exit::Cancelled => Exit::Closed,
            Exit::Closed => Exit::Cancelled,
        };
        assert_eq!(
            stopped(&mut self.owner.state, other),
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
            self.owner.state.next_job::<HB>(selected.as_ref()).is_none(),
            "{PROPERTY}: no job starts after the publisher stops"
        );
        assert!(
            self.owner.state.next_terminal().is_none(),
            "{PROPERTY}: no terminal outcome is written after the publisher stops"
        );
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

/// A publisher state view, read from the owner or derived by the model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderPublisherView {
    /// Live and closing subscriptions, by ID.
    pub subscriptions: Vec<HeaderPublisherSubscriptionView>,
    /// Tombstones held.
    pub tombstones: usize,
    /// The subscription of the job in progress.
    pub job: Option<u64>,
    /// Subscriptions whose terminal outcome waits to be written, in order.
    pub terminals: Vec<u64>,
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
    /// State read from the production owner.
    pub actual: HeaderPublisherView,
    /// State derived independently by the model.
    pub expected: HeaderPublisherView,
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
    let input = &data[..data.len().min(512)];
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
        let stop = failure.is_some() || machine.model.exit.is_some();
        steps.push(HeaderPublisherStep {
            step: number,
            input: input[index * 4..(number * 4).min(input.len())].to_vec(),
            actual: machine.actual_view(),
            expected: machine.expected_view(),
            failure,
        });
        if stop {
            break;
        }
    }
    HeaderPublisherHistory { steps }
}

/// Decoder v2, the simulator's generator 2 for this machine. Version 1 read every
/// `Finish` as an [`Answer`] from its second byte. Version 2 reads a `Finish` with an
/// odd third byte as a [`Reply`], so bytes saved under version 1 can change meaning.
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
