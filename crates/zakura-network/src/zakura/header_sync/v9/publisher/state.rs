//! The publisher's subscription policy: page position, credit, update
//! sequencing, terminal selection, job choice, and the revalidation rule. It
//! holds no lock, task, or storage handle and reads no clock.
//!
//! [`Publisher`](super::Publisher) holds a [`PublisherState`] under its
//! mutex. The publisher task asks it for the next [`Job`], runs the job
//! against storage, and reports the result under the job's [`JobKey`]. State
//! records that key, with the selected chain the job read, before the task
//! starts the job. It settles a result only under that exact key. A result of
//! a job that state abandoned changes nothing and spends no credit.
//!
//! Terminal outcomes queue in state until the task takes them one at a time,
//! writes each, and reports whether the stream took it under the outcome's
//! [`SlotId`]. No job starts while an outcome waits, but the job in progress
//! keeps settling. Cancellation, a closed stream, or a refused terminal write
//! stops the publisher: state records the [`Exit`] and starts nothing more.
//!
//! State holds values only. The runtime owner holds each permit, frame, and
//! capacity signal, and tells state about them by identity: a [`SlotOffer`]
//! for an `Open`, a [`CapacityWait`] for a capacity refusal, and a [`Page`]'s
//! facts for a page. Dropping this state releases nothing and wakes nobody.

use std::{
    collections::{BTreeMap, VecDeque},
    ops::{Bound, ControlFlow},
};

use zakura_chain::block;
use zakura_header_chain::{EngineSnapshot, Frontier};

use super::{
    super::{
        entry_bytes, Cursor, HeadersOutcomeCode, Operation, SubscribeHeaders,
        HEADERS_RESPONSE_FIXED_BYTES, SUBSCRIBE_HEADERS,
    },
    PageLimits, PublisherFault,
};
use crate::zakura::{
    header_sync::wire::{AuxSchema, MAX_HS_RANGE},
    regulation::{Applied, PublicationLedger, SlotId, SlotOffer, SubscriptionLimits, Totals},
};

/// The fields an update must repeat from its `Open`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Fixed {
    target: block::Hash,
    schema: AuxSchema,
}

/// The identity of one capacity release a stalled subscription waits for. The
/// runtime owner holds the signal behind it and assigns the identity.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct CapacityWait(pub(super) u64);

/// Why a subscription waits without a page in progress. Either kind retries
/// after a `Grant` or a selected-chain change.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Stall {
    /// Storage refused a lease for capacity. Also retry when this release
    /// arrives, because storage may release it only on a later lease.
    Capacity(CapacityWait),
    /// A local failure.
    Local,
}

/// Where a subscription's pages have reached.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Position {
    /// On the path to the initial target, after the last header sent, if
    /// any.
    Path(Option<Frontier>),
    /// Past the initial target, after the last header sent, or after the
    /// target if the `Open` reached it.
    Following(Frontier),
}

impl Position {
    /// The header the next page extends, once there is one.
    fn last(self) -> Option<Frontier> {
        match self {
            Self::Path(last) => last,
            Self::Following(last) => Some(last),
        }
    }

    /// Whether the subscription reached its initial target.
    fn reached(self) -> bool {
        matches!(self, Self::Following(_))
    }
}

/// One live subscription's header sync state.
#[derive(Debug)]
struct Subscription {
    fixed: Fixed,
    /// The `Open` locators, until the subscription reaches its target.
    locators: Vec<block::Hash>,
    position: Position,
    /// The selected tip a following run leases toward until a page reaches
    /// it, while the selected chain still contains it.
    pin: Option<Frontier>,
    /// A selected tip that a check without page credit found extending the
    /// last header sent; see [`Output::Extends`].
    checked_tip: Option<block::Hash>,
    /// This node's outcome, queued until the task writes it.
    end: Option<HeadersOutcomeCode>,
    stall: Option<Stall>,
}

impl Subscription {
    /// Whether the subscription reached its initial target, for the outcome
    /// windows. A locator at the target reaches it at `Open`, even before the
    /// publisher has served anything.
    fn reached(&self) -> bool {
        self.position.reached() || self.locators.contains(&self.fixed.target)
    }
}

/// The identity of one job, unique within a session's publisher.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct JobKey(u64);

/// The job in progress, under its key.
#[derive(Debug)]
struct InProgress {
    key: JobKey,
    subscription: u64,
    /// The selected chain the job read.
    seen: Option<EngineSnapshot>,
}

/// One page's work: the lease to take and the page it may produce.
#[derive(Clone, Debug)]
pub(super) struct Job {
    pub(super) key: JobKey,
    /// The subscription the job serves.
    pub(super) id: u64,
    pub(super) initial_target: block::Hash,
    /// The initial target on the path, then the selected tip.
    pub(super) lease_target: block::Hash,
    pub(super) locators: Vec<block::Hash>,
    /// The header the page must extend, once a page was sent or the target
    /// was reached.
    pub(super) parent: Option<Frontier>,
    /// Whether to check, at the first page, that the selected chain still
    /// contains the initial target.
    pub(super) probe: bool,
    /// An older selected tip a following page leases toward instead of
    /// `lease_target`, if the selected chain still contains it.
    pub(super) pin: Option<Frontier>,
    pub(super) following: bool,
    pub(super) count: u32,
    pub(super) schema: AuxSchema,
    /// The selected chain the lease scope comes from.
    pub(super) selected: Option<EngineSnapshot>,
}

/// The facts of an encoded page: where it leaves its subscription and what
/// it spends. The runtime owner holds the frame.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct Page {
    pub(super) parent: Frontier,
    pub(super) last: Frontier,
    /// The lease target the page lies toward.
    pub(super) target: Frontier,
    pub(super) count: u32,
    pub(super) reaches_target: bool,
    pub(super) payload_len: usize,
}

/// What a job found, other than a page.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Output {
    /// A locator was the initial target, so there is no path to serve.
    ReachedAtOpen(Frontier),
    End(HeadersOutcomeCode),
    Stall(Stall, &'static str),
    /// The page left the selected chain while in progress. Read again.
    Retry(&'static str),
    /// A check without page credit found this selected tip extending the
    /// last header sent.
    Extends(block::Hash),
}

/// A subscription's terminal outcome, ready to encode and write.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct Terminal {
    pub(super) id: u64,
    pub(super) target: block::Hash,
    pub(super) outcome: HeadersOutcomeCode,
    /// The ended subscription's slot, which holds this outcome's reserved
    /// output.
    pub(super) slot: SlotId,
    /// The job in progress for this subscription, which state abandoned.
    /// Drop its page when this outcome is written; a running job keeps its
    /// permit until storage answers.
    pub(super) abandons: Option<JobKey>,
}

/// Why the publisher task stops.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Exit {
    /// The session was cancelled.
    Cancelled,
    /// The stream refused a terminal outcome or a page's slot, or the
    /// runtime is shutting down.
    Closed,
}

/// How a following page's revalidation goes before it is written.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Recheck {
    /// The latest selected chain still contains the page.
    Keep,
    /// The latest selected chain cannot contain the page.
    Drop,
    /// Only state can tell: ask whether the selected chain contains the
    /// page's last header, and keep the page only if it does.
    Query,
}

/// The state shared between [`Publisher::apply`](super::Publisher::apply)
/// and the publisher task.
#[derive(Debug)]
pub(super) struct PublisherState {
    pub(super) publications: PublicationLedger<u64, Cursor>,
    /// Live and closing subscriptions, in the same set as `publications`.
    subscriptions: BTreeMap<u64, Subscription>,
    /// The subscription served last, so two subscriptions take turns.
    last_served: Option<u64>,
    /// At most one job runs at a time, so a `Grant` never queues jobs.
    in_progress: Option<InProgress>,
    /// Jobs started, which numbers the next [`JobKey`].
    jobs_started: u64,
    /// Terminal outcomes to write, in order.
    terminals: VecDeque<Terminal>,
    /// The slot of the terminal outcome the task is writing.
    writing: Option<SlotId>,
    exit: Option<Exit>,
}

impl PublisherState {
    /// A publisher with no subscription.
    pub(super) fn new() -> Self {
        let limits = SubscriptionLimits::from_rule(&SUBSCRIBE_HEADERS)
            .expect("SUBSCRIBE_HEADERS is a subscription row");
        Self {
            publications: PublicationLedger::new(limits),
            subscriptions: BTreeMap::new(),
            last_served: None,
            in_progress: None,
            jobs_started: 0,
            terminals: VecDeque::new(),
            writing: None,
            exit: None,
        }
    }

    /// This state, reporting its subscription transitions through `tap` if
    /// there is one.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn observed_by(mut self, tap: Option<crate::zakura::RegulationTap<u64>>) -> Self {
        self.publications = self.publications.observed_by(tap);
        self
    }

    /// Check an `Open` and apply it to the ledger under the slot `offer`. A
    /// refused `Open` changes nothing and leaves the offer with its owner.
    pub(super) fn open(
        &mut self,
        update: &SubscribeHeaders,
        limits: PageLimits,
        offer: SlotOffer,
    ) -> Result<(), PublisherFault> {
        let id = update.subscription_id;
        let schema = update.tree_aux_schema;
        if (limits.aux_schema_mask & schema.mask_bit()) != schema.mask_bit() {
            return Err(PublisherFault::UnadvertisedSchema);
        }
        self.publications.open(
            id,
            update.update_sequence,
            update.added_credit,
            update.acknowledged,
            offer,
        )?;
        self.subscriptions.insert(
            id,
            Subscription {
                fixed: Fixed {
                    target: update.initial_target_tip_hash,
                    schema,
                },
                locators: update.locator_hashes.clone(),
                position: Position::Path(None),
                pin: None,
                checked_tip: None,
                end: None,
                stall: None,
            },
        );
        Ok(())
    }

    /// Check a `Grant` or `Close` and apply it to the ledger. A refused update
    /// changes nothing.
    pub(super) fn update(&mut self, update: &SubscribeHeaders) -> Result<Applied, PublisherFault> {
        let id = update.subscription_id;
        let fixed = Fixed {
            target: update.initial_target_tip_hash,
            schema: update.tree_aux_schema,
        };
        // A tombstone matches by ID and sequence only, so only a live
        // subscription checks the fixed fields.
        let live = self.subscriptions.get(&id);
        if live.is_some_and(|subscription| subscription.fixed != fixed) {
            return Err(PublisherFault::ChangedFields);
        }
        let applied = if update.operation == Operation::Close {
            self.publications
                .close(&id, update.update_sequence, &update.acknowledged)?
        } else {
            self.publications.grant(
                &id,
                update.update_sequence,
                &update.acknowledged,
                update.added_credit,
            )?
        };
        // New credit lets a stalled subscription retry.
        if let Some(subscription) = self.subscriptions.get_mut(&id) {
            subscription.stall = None;
        }
        Ok(applied)
    }

    /// End every subscription that is closing or has an outcome queued, in
    /// order, queue each terminal outcome to write in that order, and return
    /// the outcomes.
    ///
    /// An outcome this node already queued wins over `SubscriptionClosed`,
    /// as the specification requires. Ending the subscription of the job in
    /// progress abandons that job.
    pub(super) fn end_due(&mut self) -> Vec<HeadersOutcomeCode> {
        let due: Vec<(u64, HeadersOutcomeCode)> = self
            .subscriptions
            .iter()
            .filter_map(|(&id, subscription)| match subscription.end {
                Some(outcome) => Some((id, outcome)),
                None if self.publications.is_closing(&id) => {
                    Some((id, HeadersOutcomeCode::SubscriptionClosed))
                }
                None => None,
            })
            .collect();
        due.into_iter()
            .filter_map(|(id, outcome)| {
                let subscription = self.subscriptions.remove(&id)?;
                debug_assert!(
                    is_legal(
                        outcome,
                        self.pages_sent(&id),
                        subscription.reached(),
                        self.publications.is_closing(&id),
                    ),
                    "the publisher queued {outcome:?} outside its window"
                );
                let slot = self.publications.end(&id)?;
                let abandons = self
                    .in_progress
                    .take_if(|job| job.subscription == id)
                    .map(|job| job.key);
                self.terminals.push_back(Terminal {
                    id,
                    target: subscription.fixed.target,
                    outcome,
                    slot,
                    abandons,
                });
                Some(outcome)
            })
            .collect()
    }

    /// Take the next terminal outcome to write, unless one is being written
    /// or the publisher stopped.
    pub(super) fn next_terminal(&mut self) -> Option<Terminal> {
        if self.writing.is_some() || self.exit.is_some() {
            return None;
        }
        let terminal = self.terminals.pop_front()?;
        self.writing = Some(terminal.slot);
        Some(terminal)
    }

    /// Record whether the stream took the terminal outcome being written
    /// under `slot`. A refusal stops the publisher. A report for any other
    /// slot changes nothing.
    pub(super) fn terminal_written(&mut self, slot: SlotId, written: bool) -> ControlFlow<Exit> {
        if self.writing != Some(slot) {
            return ControlFlow::Continue(());
        }
        self.writing = None;
        if written {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(self.stop(Exit::Closed))
        }
    }

    /// Record that the session was cancelled, which stops the publisher.
    pub(super) fn cancelled(&mut self) -> Exit {
        self.stop(Exit::Cancelled)
    }

    /// Record that the stream or the runtime closed under a job, which stops
    /// the publisher.
    pub(super) fn closed(&mut self) -> Exit {
        self.stop(Exit::Closed)
    }

    /// Stop the publisher for `exit`, keeping the first reason.
    fn stop(&mut self, exit: Exit) -> Exit {
        *self.exit.get_or_insert(exit)
    }

    /// Whether any subscription has this node's outcome queued.
    pub(super) fn end_queued(&self) -> bool {
        self.subscriptions
            .values()
            .any(|subscription| subscription.end.is_some())
    }

    /// Whether `id` has sent a page: the first page closes the window of
    /// the outcomes legal only before it.
    fn pages_sent(&self, id: &u64) -> bool {
        self.publications
            .credit(id)
            .is_some_and(|credit| credit.consumed().objects > 0)
    }

    /// Queue `outcome` for `id`, adjusted into its window. After `Close`
    /// arrived, the subscription ends with `SubscriptionClosed` instead.
    pub(super) fn queue_end(&mut self, id: u64, outcome: HeadersOutcomeCode) {
        if self.publications.is_closing(&id) {
            return;
        }
        let pages_sent = self.pages_sent(&id);
        if let Some(subscription) = self.subscriptions.get_mut(&id) {
            subscription.end = Some(in_window(outcome, pages_sent, subscription.reached()));
        }
    }

    /// Choose the next page or check to run, if no job is in progress, no
    /// terminal outcome waits, the publisher has not stopped, and any
    /// subscription has one, and record it as in progress.
    ///
    /// `selected` is the latest selected chain. A subscription that follows
    /// it ends here, without a state read and even while it is stalled, when
    /// the selected tip is no higher than its last header and differs from
    /// it.
    pub(super) fn next_job<const HEADER_BYTES: usize>(
        &mut self,
        selected: Option<&EngineSnapshot>,
        limits: PageLimits,
    ) -> Option<Job> {
        if self.in_progress.is_some()
            || self.writing.is_some()
            || !self.terminals.is_empty()
            || self.exit.is_some()
        {
            return None;
        }
        let ids: Vec<u64> = match self.last_served {
            Some(last) => self
                .subscriptions
                .range((Bound::Excluded(last), Bound::Unbounded))
                .chain(self.subscriptions.range(..=last))
                .map(|(&id, _)| id)
                .collect(),
            None => self.subscriptions.keys().copied().collect(),
        };
        for id in ids {
            let Some(unspent) = self.publications.unspent(&id) else {
                continue;
            };
            let Some(subscription) = self.subscriptions.get(&id) else {
                continue;
            };
            if subscription.end.is_some() {
                continue;
            }
            // Superseding a follower needs no state work, so a stalled one
            // ends as soon as the selected tip drops to or below its last
            // header.
            if let (Position::Following(last), Some(selected)) = (subscription.position, selected) {
                let tip = selected.frontiers.header_best;
                if tip != last && tip.height <= last.height {
                    self.queue_end(id, HeadersOutcomeCode::SubscriptionSuperseded);
                    continue;
                }
            }
            let schema = subscription.fixed.schema;
            let count = page_room(unspent, schema, limits, HEADER_BYTES);
            if subscription.stall.is_some() {
                continue;
            }
            let key = JobKey(self.jobs_started);
            let job = if let Position::Following(last) = subscription.position {
                let Some(selected) = selected else {
                    continue;
                };
                let tip = selected.frontiers.header_best;
                // The check above ended a tip at or below the last header.
                if tip.height <= last.height {
                    continue;
                }
                // With no room for a page, check each new tip once, without
                // a page.
                if count == 0 && subscription.checked_tip == Some(tip.hash) {
                    continue;
                }
                Job {
                    key,
                    id,
                    initial_target: subscription.fixed.target,
                    lease_target: tip.hash,
                    locators: vec![last.hash],
                    parent: Some(last),
                    probe: false,
                    pin: subscription.pin.filter(|pin| count > 0 && *pin != tip),
                    following: true,
                    count,
                    schema,
                    selected: Some(selected.clone()),
                }
            } else {
                if count == 0 {
                    continue;
                }
                let target = subscription.fixed.target;
                let last = subscription.position.last();
                Job {
                    key,
                    id,
                    initial_target: target,
                    lease_target: target,
                    locators: match last {
                        Some(last) => vec![last.hash],
                        None => subscription.locators.clone(),
                    },
                    parent: last,
                    // Before the first page, check that the target is still
                    // selected.
                    probe: last.is_none()
                        && selected
                            .is_some_and(|selected| selected.frontiers.header_best.hash != target),
                    pin: None,
                    following: false,
                    count,
                    schema,
                    selected: selected.cloned(),
                }
            };
            self.last_served = Some(id);
            self.jobs_started = self.jobs_started.wrapping_add(1);
            self.in_progress = Some(InProgress {
                key,
                subscription: id,
                seen: job.selected.clone(),
            });
            return Some(job);
        }
        None
    }

    /// Take the job in progress if it is `key`.
    fn settle(&mut self, key: JobKey) -> Option<InProgress> {
        self.in_progress.take_if(|job| job.key == key)
    }

    /// Record the result of job `key`, other than a page, for its
    /// subscription. `latest` is the latest selected chain. A result of any
    /// job but the one in progress changes nothing.
    ///
    /// A stall waits for the next selected-chain change, among other wakes,
    /// so it is not recorded when the chain moved since the job read it:
    /// that change already passed, and the next pass retries at once. Nor is
    /// a check's tip, because state may have answered on a chain the
    /// selection has since left, even if the tip hash is the same again.
    pub(super) fn finish(&mut self, key: JobKey, output: Output, latest: Option<&EngineSnapshot>) {
        let Some(job) = self.settle(key) else {
            return;
        };
        let id = job.subscription;
        let moved = latest != job.seen.as_ref();
        match output {
            Output::ReachedAtOpen(target) => {
                if let Some(subscription) = self.subscriptions.get_mut(&id) {
                    subscription.position = Position::Following(target);
                    subscription.locators = Vec::new();
                }
            }
            Output::End(outcome) => self.queue_end(id, outcome),
            Output::Extends(_) if moved => {}
            Output::Extends(tip) => {
                if let Some(subscription) = self.subscriptions.get_mut(&id) {
                    subscription.checked_tip = Some(tip);
                }
            }
            Output::Retry(_) => {}
            Output::Stall(stall, _) => {
                if moved {
                    return;
                }
                if let Some(subscription) = self.subscriptions.get_mut(&id) {
                    subscription.stall = Some(stall);
                }
            }
        }
    }

    /// Spend credit and a cursor for `page`, the result of job `key`.
    /// Returns whether to send it.
    ///
    /// A page of any job but the one in progress spends nothing. Neither does
    /// a page whose subscription ended, is closing, or has its own outcome
    /// queued.
    pub(super) fn commit(&mut self, key: JobKey, page: &Page) -> bool {
        let Some(job) = self.settle(key) else {
            return false;
        };
        let id = job.subscription;
        let Some(subscription) = self.subscriptions.get_mut(&id) else {
            return false;
        };
        if subscription.end.is_some() {
            return false;
        }
        debug_assert!(
            subscription
                .position
                .last()
                .is_none_or(|last| last == page.parent),
            "one page is in progress at a time, so it extends the last one sent"
        );
        let Some(consumed) = self
            .publications
            .credit(&id)
            .map(|credit| credit.consumed())
        else {
            return false;
        };
        // Widening usize to u64 is lossless on supported targets.
        let bytes = page.payload_len as u64;
        let cursor = Cursor {
            height: page.last.height,
            hash: page.last.hash,
            headers: consumed.objects.saturating_add(u64::from(page.count)),
            bytes: consumed.bytes.saturating_add(bytes),
        };
        if self
            .publications
            .reserve_page(&id, page.count, page.payload_len, cursor)
            .is_err()
        {
            return false;
        }
        subscription.checked_tip = None;
        subscription.position = if page.reaches_target || subscription.position.reached() {
            subscription.locators = Vec::new();
            subscription.pin = (page.target != page.last).then_some(page.target);
            Position::Following(page.last)
        } else {
            Position::Path(Some(page.last))
        };
        true
    }

    /// Clear every stall after the selected chain changed.
    pub(super) fn selected_changed(&mut self) {
        for subscription in self.subscriptions.values_mut() {
            subscription.stall = None;
        }
    }

    /// Clear the capacity stalls that wait on any of the `released` waits.
    pub(super) fn capacity_released(&mut self, released: &[CapacityWait]) {
        for subscription in self.subscriptions.values_mut() {
            if matches!(subscription.stall, Some(Stall::Capacity(wait)) if released.contains(&wait))
            {
                subscription.stall = None;
            }
        }
    }

    /// The capacity releases stalled subscriptions wait for.
    pub(super) fn capacity_waits(&self) -> Vec<CapacityWait> {
        self.subscriptions
            .values()
            .filter_map(|subscription| match subscription.stall {
                Some(Stall::Capacity(wait)) => Some(wait),
                _ => None,
            })
            .collect()
    }
}

/// How to revalidate a following `page` whose job read the selected chain
/// `seen`, now that the latest selected chain is `latest`.
///
/// Only a moved chain whose tip is neither the page's lease target nor its
/// last header needs a query. A tip at or below the page's last header
/// cannot contain the page.
pub(super) fn recheck(
    seen: Option<&EngineSnapshot>,
    latest: Option<&EngineSnapshot>,
    page: &Page,
) -> Recheck {
    let Some(latest) = latest else {
        return Recheck::Keep;
    };
    let tip = latest.frontiers.header_best;
    if seen == Some(latest) || tip == page.target || tip == page.last {
        return Recheck::Keep;
    }
    if tip.height <= page.last.height {
        return Recheck::Drop;
    }
    Recheck::Query
}

/// Whether `outcome` is legal for a subscription that has or has not sent a
/// page, reached its target, and received `Close`: the window table of the
/// specification.
pub(crate) fn is_legal(
    outcome: HeadersOutcomeCode,
    pages_sent: bool,
    reached: bool,
    close_received: bool,
) -> bool {
    use HeadersOutcomeCode::*;
    match outcome {
        NoLocatorIntersection | TargetNotRetained | TargetNotSelected => !pages_sent,
        HistoryPruned => !reached,
        SubscriptionSuperseded => true,
        SubscriptionClosed => close_received,
    }
}

/// `outcome`, adjusted into its window. `SubscriptionClosed` is not one of
/// this node's own outcomes.
pub(super) fn in_window(
    outcome: HeadersOutcomeCode,
    pages_sent: bool,
    reached: bool,
) -> HeadersOutcomeCode {
    use HeadersOutcomeCode::*;
    match outcome {
        NoLocatorIntersection | TargetNotRetained | TargetNotSelected if !pages_sent => outcome,
        NoLocatorIntersection | TargetNotRetained | TargetNotSelected | HistoryPruned
            if !reached =>
        {
            HistoryPruned
        }
        _ => SubscriptionSuperseded,
    }
}

/// The most headers the next page may carry: the advertised count and byte
/// limits, the unspent header and byte credit, and [`MAX_HS_RANGE`]. Zero
/// means no legal page fits.
pub(super) fn page_room(
    unspent: Totals,
    schema: AuxSchema,
    limits: PageLimits,
    header_bytes: usize,
) -> u32 {
    // Widening usize to u64 is lossless on supported targets.
    let entry = entry_bytes(header_bytes, schema) as u64;
    let fixed = HEADERS_RESPONSE_FIXED_BYTES as u64;
    let fit = |bytes: u64| bytes.saturating_sub(fixed) / entry;
    let count = u64::from(limits.max_headers.min(MAX_HS_RANGE))
        .min(unspent.objects)
        .min(fit(u64::from(limits.max_bytes)))
        .min(fit(unspent.bytes));
    u32::try_from(count).expect("the count is at most MAX_HS_RANGE")
}
