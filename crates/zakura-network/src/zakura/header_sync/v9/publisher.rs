//! The header sync version 9 publisher: one session's side of the
//! subscriptions a peer opens on this node.
//!
//! [`Publications`] holds the subscription rules: slots, credit, the sent
//! cursor ring, and tombstones. This module adds what only header sync knows:
//! which headers each page carries, and when a subscription ends.
//!
//! One task per session writes every page and terminal outcome, so an outcome
//! always follows its subscription's pages. [`Publisher::apply`] applies each
//! update under a short lock and wakes the task, which holds at most one page
//! in progress, so a `Grant` never queues page jobs. A `Close` ends its
//! subscription on the next wake and abandons that subscription's page in
//! progress. Its terminal outcome waits for no credit or output, as
//! [`TerminalPermit::send`] describes.
//!
//! Pages come from the retained-path leases in [`Port`], one lease per page,
//! acquired and read back to back, so a lease's idle deadline covers one read
//! and no subscription ends because a lease expired. Each page after the first
//! acquires with the last header sent as its only locator. State's
//! continuation index answers that without a new ancestry walk only while the
//! lease target stays the same. That holds on the path to the initial target.
//! After it, the leases run from the last header sent to the tip of
//! [`Port::selected_chain`], so each tip change costs one walk, and so does
//! each lease [`still_selected`] takes. Local failures wait for an update, a
//! selected-chain change, or a capacity release, never for a timer.

use std::{
    any::Any,
    collections::BTreeMap,
    future::Future,
    num::NonZeroU32,
    ops::Bound,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
};

use futures::FutureExt;
use thiserror::Error;
use tokio::sync::{mpsc, watch, Notify};
use tokio_util::sync::CancellationToken;
use zakura_chain::block;
use zakura_header_chain::{
    BranchId, EngineSnapshot, Frontier, HeaderGeneration, HeaderWorkAuthority, SourceId,
};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, Port, PortError, ReadPath, ReadPathReply, RetainedHeaderPath,
    RetainedHeaderPathPage, ServingCapacitySignal,
};

use super::{
    entry_bytes, Cursor, Headers, HeadersOutcome, HeadersOutcomeCode, Message, Operation,
    SubscribeHeaders, HEADERS_RESPONSE_FIXED_BYTES, SUBSCRIBE_HEADERS,
};
use crate::zakura::{
    header_sync::{
        reactor::selected_port_aux_delivery,
        wire::{AuxSchema, HeaderEntry, MAX_HS_RANGE},
    },
    regulation::{
        Applied, ExecutionSlots, Publications, Push, PushPermit, PushSend, SubscriptionFault,
        SubscriptionLimits, TerminalPermit, Totals,
    },
    wire_codec::encode_frame,
    Frame, FramedSend,
};

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use tests::FakePort;

/// Lease attempts one page makes before it waits for a wake.
const LEASE_ATTEMPTS: usize = 2;

/// Leases one following page's recheck takes while the selected tip keeps
/// moving; see [`still_selected`].
const RECHECK_LEASES: usize = 2;

/// The serving limits this node advertises in its `Status`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PageLimits {
    /// `max_headers_per_response`.
    pub(crate) max_headers: u32,
    /// `max_message_bytes`.
    pub(crate) max_bytes: u32,
    /// `tree_aux_schema_mask`.
    pub(crate) aux_schema_mask: u32,
}

/// One session's identity for state's serving leases.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeaseOwner {
    /// The peer's stable source identity.
    pub(crate) peer: SourceId,
    /// The session generation.
    pub(crate) session_id: u64,
}

/// An update that breaks a publisher rule. Each case is a protocol violation:
/// `Disconnect`.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum PublisherFault {
    /// A rule the subscription tools enforce.
    #[error(transparent)]
    Subscription(#[from] SubscriptionFault),
    /// A `Grant` or `Close` on a live subscription whose target or schema
    /// differs from its `Open`.
    #[error("the update changes its subscription's target or schema")]
    ChangedFields,
    /// An `Open` that asks for a schema this node does not advertise.
    #[error("the Open asks for an unadvertised auxiliary schema")]
    UnadvertisedSchema,
}

/// The fields an update must repeat from its `Open`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Fixed {
    target: block::Hash,
    schema: AuxSchema,
}

/// Why a subscription waits without a page in progress. Either kind retries
/// after a `Grant` or a selected-chain change.
#[derive(Clone, Debug)]
enum Stall {
    /// State refused a lease for capacity. Also retry when the signal is
    /// released, because state may release it only on a later lease.
    Capacity(ServingCapacitySignal),
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
    /// A selected tip that a check without page credit found extending the
    /// last header sent; see [`check_following`].
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

/// The state shared between [`Publisher::apply`] and the publisher task.
#[derive(Debug)]
struct Shared {
    publications: Publications<u64, Cursor>,
    /// Live and closing subscriptions, in the same set as `publications`.
    subscriptions: BTreeMap<u64, Subscription>,
    /// The subscription served last, so two subscriptions take turns.
    last_served: Option<u64>,
}

/// One session's publisher: the handle the session reader applies updates
/// through.
#[derive(Debug)]
pub(crate) struct Publisher {
    shared: Arc<Mutex<Shared>>,
    wake: Arc<Notify>,
    limits: PageLimits,
}

impl Publisher {
    /// Start publishing on one session, and return the handle with the task
    /// that writes pages and outcomes to `send` until `cancel` fires.
    ///
    /// The task re-raises a page job's panic, even one whose page it had
    /// abandoned, so the session's owner sees it through the returned handle
    /// and can close the session.
    ///
    /// `HEADER_BYTES` is the network's encoded header size, which selects the
    /// version 9 codec.
    pub(crate) fn spawn<const HEADER_BYTES: usize>(
        port: Arc<dyn Port>,
        push: Push,
        send: FramedSend,
        owner: LeaseOwner,
        limits: PageLimits,
        cancel: CancellationToken,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let limits_row = SubscriptionLimits::from_rule(&SUBSCRIBE_HEADERS)
            .expect("SUBSCRIBE_HEADERS is a subscription row");
        let shared = Arc::new(Mutex::new(Shared {
            publications: Publications::new(limits_row),
            subscriptions: BTreeMap::new(),
            last_served: None,
        }));
        let wake = Arc::new(Notify::new());
        // One panic ends the task, so the channel never needs more room.
        let (panics, panicked) = mpsc::channel(1);
        let task = tokio::spawn(publish::<HEADER_BYTES>(
            Task {
                shared: shared.clone(),
                wake: wake.clone(),
                port,
                push,
                owner,
                limits,
                panics,
            },
            send,
            panicked,
            cancel,
        ));
        (
            Self {
                shared,
                wake,
                limits,
            },
            task,
        )
    }

    /// Apply one `SubscribeHeaders` update, in stream order.
    ///
    /// `Applied::Crossed` means the update crossed its subscription's
    /// terminal outcome: drop it. An error is a protocol violation. The work
    /// is bounded, and the call yields when the caller's cooperative budget
    /// runs out, so a burst of updates lets other peers progress.
    pub(crate) async fn apply(&self, update: &SubscribeHeaders) -> Result<Applied, PublisherFault> {
        let applied = lock(&self.shared).apply(update, self.limits);
        if applied == Ok(Applied::Live) {
            self.wake.notify_one();
        }
        tokio::task::coop::consume_budget().await;
        applied
    }
}

impl Shared {
    /// Check `update` and apply it to the subscription tools. A refused
    /// update changes nothing.
    fn apply(
        &mut self,
        update: &SubscribeHeaders,
        limits: PageLimits,
    ) -> Result<Applied, PublisherFault> {
        let id = update.subscription_id;
        let fixed = Fixed {
            target: update.initial_target_tip_hash,
            schema: update.tree_aux_schema,
        };
        match update.operation {
            Operation::Open => {
                if (limits.aux_schema_mask & fixed.schema.mask_bit()) != fixed.schema.mask_bit() {
                    return Err(PublisherFault::UnadvertisedSchema);
                }
                self.publications.open(
                    id,
                    update.update_sequence,
                    update.added_credit,
                    update.acknowledged,
                )?;
                self.subscriptions.insert(
                    id,
                    Subscription {
                        fixed,
                        locators: update.locator_hashes.clone(),
                        position: Position::Path(None),
                        checked_tip: None,
                        end: None,
                        stall: None,
                    },
                );
                Ok(Applied::Live)
            }
            Operation::Grant | Operation::Close => {
                // A tombstone matches by ID and sequence only, so only a
                // live subscription checks the fixed fields.
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
        }
    }

    /// End every subscription that is closing or has an outcome queued, in
    /// order, and return each terminal frame with its permit.
    ///
    /// An outcome this node already queued wins over `SubscriptionClosed`,
    /// as the specification requires.
    fn end_due<const HEADER_BYTES: usize>(&mut self) -> Vec<(u64, Frame, TerminalPermit)> {
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
                let terminal = self.publications.end(&id)?;
                metrics::counter!(
                    "sync.header.v9.publisher.outcome",
                    "outcome" => outcome_label(outcome),
                )
                .increment(1);
                let frame =
                    encode_frame(&Message::<HEADER_BYTES>::HeadersOutcome(HeadersOutcome {
                        subscription_id: id,
                        initial_target_tip_hash: subscription.fixed.target,
                        outcome,
                    }))
                    .expect("an outcome for a nonzero subscription ID always encodes");
                Some((id, frame, terminal))
            })
            .collect()
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
    fn queue_end(&mut self, id: u64, outcome: HeadersOutcomeCode) {
        if self.publications.is_closing(&id) {
            return;
        }
        let pages_sent = self.pages_sent(&id);
        if let Some(subscription) = self.subscriptions.get_mut(&id) {
            subscription.end = Some(in_window(outcome, pages_sent, subscription.reached()));
        }
    }

    /// The next page or check to run, if any subscription has one.
    ///
    /// A subscription that follows the selected chain ends here, without a
    /// state read and even while it is stalled, when the selected tip is no
    /// higher than its last header and differs from it.
    fn next_job<const HEADER_BYTES: usize>(
        &mut self,
        selected: Option<&EngineSnapshot>,
        limits: PageLimits,
    ) -> Option<Job> {
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
                // A page must extend the last header. A check only needs to
                // reach it, and a tip it already checked descends from it.
                let locators = match subscription.checked_tip {
                    Some(checked) if count == 0 => vec![checked, last.hash],
                    _ => vec![last.hash],
                };
                Job {
                    id,
                    initial_target: subscription.fixed.target,
                    lease_target: tip.hash,
                    locators,
                    parent: Some(last),
                    probe: None,
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
                    probe: selected
                        .map(|selected| selected.frontiers.header_best)
                        .filter(|tip| last.is_none() && tip.hash != target),
                    following: false,
                    count,
                    schema,
                    selected: selected.cloned(),
                }
            };
            self.last_served = Some(id);
            return Some(job);
        }
        None
    }

    /// Record a finished job's result for its subscription.
    ///
    /// A stall waits for the next selected-chain change, among other wakes,
    /// so it is not recorded when the chain `moved` since the job read it:
    /// that change already passed, and the next pass retries at once.
    fn finish(&mut self, id: u64, output: Output, moved: bool) {
        match output {
            Output::ReachedAtOpen(target) => {
                if let Some(subscription) = self.subscriptions.get_mut(&id) {
                    subscription.position = Position::Following(target);
                    subscription.locators = Vec::new();
                }
            }
            Output::End(outcome) => self.queue_end(id, outcome),
            Output::Extends(tip) => {
                if let Some(subscription) = self.subscriptions.get_mut(&id) {
                    subscription.checked_tip = Some(tip);
                }
            }
            Output::Retry(reason) => {
                metrics::counter!("sync.header.v9.publisher.page_dropped", "reason" => reason)
                    .increment(1);
            }
            Output::Stall(stall, reason) => {
                metrics::counter!("sync.header.v9.publisher.stall", "reason" => reason)
                    .increment(1);
                if moved {
                    return;
                }
                if let Some(subscription) = self.subscriptions.get_mut(&id) {
                    subscription.stall = Some(stall);
                }
            }
            Output::Page(_) => unreachable!("pages commit through `commit`"),
        }
    }

    /// Spend credit and a cursor for `page`. Returns whether to send it.
    ///
    /// A subscription that ended, is closing, or has its own outcome queued
    /// sends no more pages.
    fn commit(&mut self, id: u64, page: &Page) -> bool {
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
        let bytes = page.frame.payload.len() as u64;
        let cursor = Cursor {
            height: page.last.height,
            hash: page.last.hash,
            headers: consumed.objects.saturating_add(u64::from(page.count)),
            bytes: consumed.bytes.saturating_add(bytes),
        };
        if self
            .publications
            .reserve_page(&id, page.count, page.frame.payload.len(), cursor)
            .is_err()
        {
            return false;
        }
        subscription.checked_tip = None;
        subscription.position = if page.reaches_target || subscription.position.reached() {
            subscription.locators = Vec::new();
            Position::Following(page.last)
        } else {
            Position::Path(Some(page.last))
        };
        true
    }

    /// Clear every stall after the selected chain changed, or else the
    /// capacity stalls whose signal was released.
    fn clear_stalls(&mut self, all: bool) {
        for subscription in self.subscriptions.values_mut() {
            let clear = match &subscription.stall {
                Some(Stall::Local) => all,
                Some(Stall::Capacity(signal)) => all || signal.is_released(),
                None => false,
            };
            if clear {
                subscription.stall = None;
            }
        }
    }

    /// The capacity signals stalled subscriptions wait on.
    fn capacity_signals(&self) -> Vec<ServingCapacitySignal> {
        self.subscriptions
            .values()
            .filter_map(|subscription| match &subscription.stall {
                Some(Stall::Capacity(signal)) => Some(signal.clone()),
                _ => None,
            })
            .collect()
    }
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
fn in_window(outcome: HeadersOutcomeCode, pages_sent: bool, reached: bool) -> HeadersOutcomeCode {
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

/// A stable metric label for `outcome`.
fn outcome_label(outcome: HeadersOutcomeCode) -> &'static str {
    use HeadersOutcomeCode::*;
    match outcome {
        TargetNotRetained => "target_not_retained",
        NoLocatorIntersection => "no_locator_intersection",
        HistoryPruned => "history_pruned",
        TargetNotSelected => "target_not_selected",
        SubscriptionSuperseded => "superseded",
        SubscriptionClosed => "closed",
    }
}

/// The most headers the next page may carry: the advertised count and byte
/// limits, the unspent header and byte credit, and [`MAX_HS_RANGE`]. Zero
/// means no legal page fits.
fn page_room(unspent: Totals, schema: AuxSchema, limits: PageLimits, header_bytes: usize) -> u32 {
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

/// The largest encoding of a page of `count` headers under `schema`.
fn page_bytes(count: u32, schema: AuxSchema, header_bytes: usize) -> usize {
    // Widening u32 to usize is lossless on supported targets.
    HEADERS_RESPONSE_FIXED_BYTES + count as usize * entry_bytes(header_bytes, schema)
}

/// Lock `shared`, recovering from a poisoned lock: every update leaves the
/// state consistent before it can panic.
fn lock(shared: &Mutex<Shared>) -> std::sync::MutexGuard<'_, Shared> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One page's work: the lease to take and the page it may produce.
#[derive(Clone, Debug)]
struct Job {
    id: u64,
    initial_target: block::Hash,
    /// The initial target on the path, then the selected tip.
    lease_target: block::Hash,
    locators: Vec<block::Hash>,
    /// The header the page must extend, once a page was sent or the target
    /// was reached.
    parent: Option<Frontier>,
    /// A selected tip to check the initial target against, at the first page.
    probe: Option<Frontier>,
    following: bool,
    count: u32,
    schema: AuxSchema,
    /// The selected chain the lease scope comes from.
    selected: Option<EngineSnapshot>,
}

/// An encoded page and where it leaves its subscription.
#[derive(Debug)]
struct Page {
    parent: Frontier,
    last: Frontier,
    count: u32,
    reaches_target: bool,
    frame: Frame,
}

/// What a job found.
#[derive(Debug)]
enum Output {
    Page(Page),
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

/// Everything the publisher task owns besides its stream.
struct Task {
    shared: Arc<Mutex<Shared>>,
    wake: Arc<Notify>,
    port: Arc<dyn Port>,
    push: Push,
    owner: LeaseOwner,
    limits: PageLimits,
    /// Page jobs report a panic here, even after their page was abandoned.
    panics: mpsc::Sender<Box<dyn Any + Send>>,
}

/// A page in progress: its permit wait, its job, and its transport slot.
type Work<'a> = Pin<Box<dyn Future<Output = Progress<'a>> + Send + 'a>>;

/// The budgets a job holds while it runs.
enum Admission {
    /// A page's output and execution slots.
    Page(PushPermit),
    /// A check's execution slots, held until it finishes. It writes nothing.
    Check { _slots: ExecutionSlots },
}

/// How a page in progress finished.
enum Progress<'a> {
    Page(Page, PushSend<'a>),
    Other(Output),
    Closed,
}

/// Write pages and outcomes for one session until `cancel` fires or the
/// stream closes.
async fn publish<const HEADER_BYTES: usize>(
    task: Task,
    send: FramedSend,
    mut panicked: mpsc::Receiver<Box<dyn Any + Send>>,
    cancel: CancellationToken,
) {
    let mut selected = task.port.selected_chain();
    let mut selected_open = true;
    // The page in progress, with its subscription and the selected chain its
    // job read.
    let mut work: Option<(u64, Option<EngineSnapshot>, Work<'_>)> = None;
    loop {
        let ended = lock(&task.shared).end_due::<HEADER_BYTES>();
        for (id, frame, terminal) in ended {
            // The subscription sends no more pages, so drop its page in
            // progress. A running job keeps its permit until state answers.
            if work.as_ref().is_some_and(|(working, _, _)| *working == id) {
                work = None;
            }
            let terminal = terminal.send(&send, frame);
            tokio::pin!(terminal);
            // A full queue must not keep a cancelled session's task alive,
            // hide a page job's panic, or stop another subscription's page.
            // That page settles at once: its job gives its execution slots
            // back, and a page that holds the queue slot this terminal waits
            // for is queued instead of kept. A page whose subscription ends
            // later in this batch fails its commit and is dropped.
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    Some(panic) = panicked.recv() => std::panic::resume_unwind(panic),
                    progress = async { work.as_mut().expect("guarded by is_some").2.as_mut().await },
                        if work.is_some() =>
                    {
                        let (id, seen, _) = work.take().expect("the page in progress just finished");
                        if !settle(&task.shared, &selected, id, seen.as_ref(), progress) {
                            return;
                        }
                    }
                    sent = &mut terminal => {
                        if !sent {
                            return;
                        }
                        break;
                    }
                }
            }
        }
        if work.is_none() {
            let snapshot = selected.borrow().clone();
            let mut shared = lock(&task.shared);
            let job = shared.next_job::<HEADER_BYTES>(snapshot.as_ref(), task.limits);
            let ending = shared
                .subscriptions
                .values()
                .any(|subscription| subscription.end.is_some());
            drop(shared);
            if let Some(job) = job {
                let id = job.id;
                let seen = job.selected.clone();
                let page = start_page::<HEADER_BYTES>(&task, job, &send, selected.clone());
                work = Some((id, seen, page));
            }
            if ending {
                continue;
            }
        }
        let capacity = lock(&task.shared).capacity_signals();
        // The page in progress goes first, so a stream of wakes cannot starve
        // it. Each wake still reaches `end_due` on the next pass.
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            // See `Publisher::spawn`.
            Some(panic) = panicked.recv() => std::panic::resume_unwind(panic),
            progress = async { work.as_mut().expect("guarded by is_some").2.as_mut().await },
                if work.is_some() =>
            {
                let (id, seen, _) = work.take().expect("the page in progress just finished");
                if !settle(&task.shared, &selected, id, seen.as_ref(), progress) {
                    return;
                }
            }
            () = task.wake.notified() => {}
            changed = selected.changed(), if selected_open => {
                if changed.is_ok() {
                    lock(&task.shared).clear_stalls(true);
                } else {
                    selected_open = false;
                }
            }
            () = any_released(capacity) => lock(&task.shared).clear_stalls(false),
        }
    }
}

/// Apply a finished page or job to subscription `id`, whose job read the
/// selected chain `seen`. Returns false when the stream closed.
fn settle(
    shared: &Mutex<Shared>,
    selected: &watch::Receiver<Option<EngineSnapshot>>,
    id: u64,
    seen: Option<&EngineSnapshot>,
    progress: Progress<'_>,
) -> bool {
    match progress {
        // A page whose subscription closed meanwhile spends nothing and drops
        // its slot.
        Progress::Page(page, slot) => {
            if lock(shared).commit(id, &page) {
                slot.send(page.frame);
            }
            true
        }
        Progress::Other(output) => {
            let moved = selected.borrow().as_ref() != seen;
            lock(shared).finish(id, output, moved);
            true
        }
        Progress::Closed => false,
    }
}

/// Wait until any of `signals` is released, or forever if there are none.
async fn any_released(signals: Vec<ServingCapacitySignal>) {
    if signals.is_empty() {
        return std::future::pending().await;
    }
    let waits = signals.iter().map(|signal| Box::pin(signal.released()));
    futures::future::select_all(waits).await;
}

/// Start one page: wait for its budgets, run its job holding them, then
/// reserve its transport slot. A check without a page writes nothing, so it
/// waits for execution slots alone.
///
/// The job keeps its permit until the state read finishes, even if the
/// session ends first. A following page whose selected tip moved meanwhile is
/// checked by [`still_selected`] before it is sent.
fn start_page<'a, const HEADER_BYTES: usize>(
    task: &Task,
    job: Job,
    send: &'a FramedSend,
    selected: watch::Receiver<Option<EngineSnapshot>>,
) -> Work<'a> {
    let push = task.push.clone();
    let port = task.port.clone();
    let check_port = task.port.clone();
    let panics = task.panics.clone();
    let owner = task.owner;
    let id = job.id;
    let leased_tip = job.following.then_some(job.lease_target);
    let bytes = page_bytes(job.count, job.schema, HEADER_BYTES);
    let check = job.count == 0;
    Box::pin(async move {
        let permit = if check {
            Admission::Check {
                _slots: push.execution().await,
            }
        } else {
            Admission::Page(push.acquire(bytes).await)
        };
        let running = tokio::spawn(async move {
            let job = AssertUnwindSafe(run_job::<HEADER_BYTES>(&*port, owner, &job));
            match job.catch_unwind().await {
                Ok(output) => Some((output, permit)),
                // The permit drops once the job has ended, panic or not.
                Err(panic) => {
                    tracing::error!(subscription_id = id, "a header sync v9 page job panicked");
                    // A full channel already holds a panic that ends the task.
                    let _ = panics.try_send(panic);
                    None
                }
            }
        });
        let (output, permit): (Output, Admission) = match running.await {
            Ok(Some(done)) => done,
            // The publisher task re-raises the job's panic.
            Ok(None) => return Progress::Other(Output::Stall(Stall::Local, "job_panicked")),
            // The runtime is shutting down.
            Err(_) => return Progress::Closed,
        };
        let (page, permit) = match (output, permit) {
            (Output::Page(page), Admission::Page(permit)) => (page, permit),
            (output, _) => return Progress::Other(output),
        };
        let Ok(slot) = permit.reserve_send(send).await else {
            return Progress::Closed;
        };
        if let Some(leased_tip) = leased_tip {
            let last = page.last;
            if !still_selected(&*check_port, &push, owner, id, &selected, leased_tip, last).await {
                return Progress::Other(Output::Retry("left_selected_chain"));
            }
        }
        Progress::Page(page, slot)
    })
}

/// Whether the latest selected chain still contains `last`, the last header
/// of a following page leased toward `leased_tip`.
///
/// It checks only when the selected tip moved, and only a lease that confirms
/// the page keeps it. Anything else drops the page, and the next pass leases
/// again from the last header sent, where the usual stall and outcome rules
/// apply. The lease is state work, so it waits for execution slots through
/// `push` as the page's read did.
///
/// The chain can move during that wait or the lease, so a lease whose tip is
/// no longer selected is taken again toward the newer tip, at most
/// [`RECHECK_LEASES`] times. After that the last verdict stands, judged
/// against a tip at most one lease old, and the slots go back to other work.
/// A reorganization can overtake any page after it is written anyway, and the
/// next lease ends that subscription as superseded.
async fn still_selected(
    port: &dyn Port,
    push: &Push,
    owner: LeaseOwner,
    id: u64,
    selected: &watch::Receiver<Option<EngineSnapshot>>,
    leased_tip: block::Hash,
    last: Frontier,
) -> bool {
    let mut execution = None;
    let mut leases = 0;
    loop {
        let Some(latest) = selected.borrow().clone() else {
            return true;
        };
        let tip = latest.frontiers.header_best;
        if tip.hash == leased_tip || tip == last {
            return true;
        }
        if tip.height <= last.height {
            return false;
        }
        if execution.is_none() {
            execution = Some(push.execution().await);
            continue;
        }
        let contains = match acquire(port, owner, Some(&latest), tip.hash, vec![last.hash]).await {
            Ok(AcquirePathReply::Acquired(path)) => {
                release(port, id, *path).await;
                true
            }
            Ok(_) => false,
            Err(error) => {
                log_port_failure(id, "recheck", &error);
                false
            }
        };
        leases += 1;
        let current = selected
            .borrow()
            .as_ref()
            .map(|selected| selected.frontiers.header_best);
        if current == Some(tip) || leases >= RECHECK_LEASES {
            return contains;
        }
    }
}

/// Read one page, or decide the subscription's outcome, through `port`.
async fn run_job<const HEADER_BYTES: usize>(
    port: &dyn Port,
    owner: LeaseOwner,
    job: &Job,
) -> Output {
    if job.count == 0 {
        return check_following(port, owner, job).await;
    }
    if let Some(tip) = job.probe {
        let selected = job.selected.as_ref();
        match acquire(port, owner, selected, tip.hash, vec![job.initial_target]).await {
            Ok(AcquirePathReply::Acquired(path)) => {
                let selected = path.common_ancestor.hash == job.initial_target;
                release(port, job.id, *path).await;
                if !selected {
                    return Output::Stall(Stall::Local, "probe_ancestor");
                }
            }
            Ok(
                reply @ (AcquirePathReply::NoLocatorIntersection | AcquirePathReply::HistoryPruned),
            ) => {
                // The probe found the target off the selected chain, or
                // behind pruned history that will not return. Tell a target
                // this node never retained from either.
                let found = if matches!(reply, AcquirePathReply::HistoryPruned) {
                    HeadersOutcomeCode::HistoryPruned
                } else {
                    HeadersOutcomeCode::TargetNotSelected
                };
                let locators = job.locators.clone();
                let path = acquire(port, owner, selected, job.lease_target, locators).await;
                return match path {
                    Ok(AcquirePathReply::Acquired(path)) => {
                        release(port, job.id, *path).await;
                        Output::End(found)
                    }
                    Ok(AcquirePathReply::TargetNotRetained) => {
                        Output::End(HeadersOutcomeCode::TargetNotRetained)
                    }
                    Ok(AcquirePathReply::CapacityBusy(signal)) => {
                        Output::Stall(Stall::Capacity(signal), "capacity")
                    }
                    // The probe already settled the outcome.
                    Ok(_) => Output::End(found),
                    Err(error) => {
                        log_port_failure(job.id, "target_retained", &error);
                        Output::End(found)
                    }
                };
            }
            Ok(AcquirePathReply::CapacityBusy(signal)) => {
                return Output::Stall(Stall::Capacity(signal), "capacity")
            }
            Ok(_) => return Output::Stall(Stall::Local, "probe"),
            Err(error) => return port_failure(job.id, "probe", &error),
        }
    }
    for _ in 0..LEASE_ATTEMPTS {
        let locators = job.locators.clone();
        let reply = acquire(
            port,
            owner,
            job.selected.as_ref(),
            job.lease_target,
            locators,
        )
        .await;
        let path = match reply {
            Ok(AcquirePathReply::Acquired(path)) => *path,
            Ok(AcquirePathReply::CapacityBusy(signal)) => {
                return Output::Stall(Stall::Capacity(signal), "capacity")
            }
            // The selected chain no longer extends the last header sent.
            Ok(AcquirePathReply::NoLocatorIntersection) if job.following => {
                return Output::End(HeadersOutcomeCode::SubscriptionSuperseded)
            }
            Ok(AcquirePathReply::NoLocatorIntersection) => {
                return Output::End(HeadersOutcomeCode::NoLocatorIntersection)
            }
            // A stale selected tip: the next selected-chain change retries.
            Ok(AcquirePathReply::TargetNotRetained) if job.following => {
                return Output::Stall(Stall::Local, "selected_tip")
            }
            Ok(AcquirePathReply::TargetNotRetained) => {
                return Output::End(HeadersOutcomeCode::TargetNotRetained)
            }
            Ok(AcquirePathReply::HistoryPruned) => {
                return Output::End(HeadersOutcomeCode::HistoryPruned)
            }
            Ok(AcquirePathReply::Busy) => return Output::Stall(Stall::Local, "acquire"),
            Err(error) => return port_failure(job.id, "acquire", &error),
        };
        if job.parent.is_none() && path.common_ancestor == path.target {
            let target = path.target;
            release(port, job.id, path).await;
            return Output::ReachedAtOpen(target);
        }
        if job
            .parent
            .is_some_and(|parent| parent != path.common_ancestor)
        {
            release(port, job.id, path).await;
            return Output::Stall(Stall::Local, "lease_parent");
        }
        let read = ReadPath {
            after_hash: path.common_ancestor.hash,
            max_header_count: job.count,
            want_tree_aux: job.schema == AuxSchema::V1,
        };
        match port.read_header_path(path.clone(), read).await {
            Ok(ReadPathReply::Page(page)) => return page_output::<HEADER_BYTES>(job, *page),
            // The lease expired or state evicted it. Acquire it again from
            // the last header sent.
            Ok(ReadPathReply::Unavailable) => release(port, job.id, path).await,
            Err(error) => {
                release(port, job.id, path).await;
                return port_failure(job.id, "read", &error);
            }
        }
    }
    Output::Stall(Stall::Local, "lease_lost")
}

/// Log a failed port `operation` for subscription `id`, and wait for the
/// next update or selected-chain change.
fn port_failure(id: u64, operation: &'static str, error: &PortError) -> Output {
    log_port_failure(id, operation, error);
    Output::Stall(Stall::Local, operation)
}

/// Log a failed port `operation` for subscription `id`. Port failures are
/// local, so they never count against the peer.
fn log_port_failure(id: u64, operation: &'static str, error: &PortError) {
    tracing::debug!(
        subscription_id = id,
        operation,
        ?error,
        "a header sync v9 port operation failed"
    );
}

/// Check, without a page, that the selected chain still extends a following
/// subscription's last header. It runs once per selected tip while no page
/// fits the subscription's credit, because a conformant subscriber need not
/// keep page credit after its target, and the publisher still owes it
/// `SubscriptionSuperseded` when the chain stops extending that header.
///
/// The job's locators are the last header and any tip a check already found
/// extending it. Hash ancestry never changes, so reaching either one shows the
/// new tip extends the last header, and the walk stops at the nearer one.
async fn check_following(port: &dyn Port, owner: LeaseOwner, job: &Job) -> Output {
    let reply = acquire(
        port,
        owner,
        job.selected.as_ref(),
        job.lease_target,
        job.locators.clone(),
    )
    .await;
    match reply {
        Ok(AcquirePathReply::Acquired(path)) => {
            release(port, job.id, *path).await;
            Output::Extends(job.lease_target)
        }
        Ok(AcquirePathReply::NoLocatorIntersection) => {
            Output::End(HeadersOutcomeCode::SubscriptionSuperseded)
        }
        // `in_window` turns this into `SubscriptionSuperseded`.
        Ok(AcquirePathReply::HistoryPruned) => Output::End(HeadersOutcomeCode::HistoryPruned),
        Ok(AcquirePathReply::CapacityBusy(signal)) => {
            Output::Stall(Stall::Capacity(signal), "capacity")
        }
        Ok(AcquirePathReply::TargetNotRetained | AcquirePathReply::Busy) => {
            Output::Stall(Stall::Local, "selected_tip")
        }
        Err(error) => port_failure(job.id, "check", &error),
    }
}

/// Ask `port` for a lease on the path from `locators` to `target`.
async fn acquire(
    port: &dyn Port,
    owner: LeaseOwner,
    selected: Option<&EngineSnapshot>,
    target: block::Hash,
    locators: Vec<block::Hash>,
) -> Result<AcquirePathReply, PortError> {
    port.acquire_header_path(AcquirePath {
        source: owner.peer,
        session_id: owner.session_id,
        scope: scope(selected, target),
        target_tip_hash: target,
        locator_hashes: locators,
    })
    .await
}

/// Release `path`, which subscription `id` leased. A failed release leaves
/// the lease to its idle deadline.
async fn release(port: &dyn Port, id: u64, path: RetainedHeaderPath) {
    if let Err(error) = port.release_header_path(path).await {
        log_port_failure(id, "release", &error);
    }
}

/// The lease scope for `target`: the selected chain's generation, or the
/// first generation before any commit.
fn scope(selected: Option<&EngineSnapshot>, target: block::Hash) -> HeaderWorkAuthority {
    match selected {
        Some(selected) => HeaderWorkAuthority::for_target(selected, target),
        None => HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(0),
            branch: BranchId::new(block::Hash([0; 32]), target),
        },
    }
}

/// Build and encode `job`'s page from a state page.
///
/// A schema-1 page stops before the first header without a record, because a
/// page's schema must match its subscription and a page cannot skip a header.
/// With none for its first header, a subscription on the path to its target
/// ends with `HistoryPruned`: a pruned node never gains records below its
/// index height, so waiting would stall the subscriber forever. A following
/// subscription waits instead, because `HistoryPruned` is past its window.
fn page_output<const HEADER_BYTES: usize>(job: &Job, page: RetainedHeaderPathPage) -> Output {
    let count = page.headers.len();
    // Widening u32 to usize is lossless on supported targets.
    if count == 0
        || count > job.count as usize
        || page.aux_deliveries.len() != count
        || page.finalized_tree_aux.len() != count
        || page.finalized_body_sizes.len() != count
        || job
            .parent
            .is_some_and(|parent| parent != page.common_ancestor)
    {
        return Output::Stall(Stall::Local, "page_shape");
    }
    let mut entries = Vec::with_capacity(count);
    for (((header, deliveries), finalized_tree_aux), finalized_body_size) in page
        .headers
        .into_iter()
        .zip(page.aux_deliveries)
        .zip(page.finalized_tree_aux)
        .zip(page.finalized_body_sizes)
    {
        let tree_aux = match job.schema {
            AuxSchema::None => None,
            AuxSchema::V1 => match finalized_tree_aux.or_else(|| {
                selected_port_aux_delivery(&deliveries, AuxSchema::V1)
                    .and_then(|delivery| delivery.tree_aux)
            }) {
                Some(record) => Some(record),
                None => break,
            },
        };
        let body_size = finalized_body_size
            .or_else(|| zakura_header_chain::AuxDelivery::advertised_body_size(&deliveries))
            .map_or(0, NonZeroU32::get);
        entries.push(HeaderEntry {
            header,
            body_size,
            tree_aux,
        });
    }
    let Some(last) = entries.last() else {
        return if job.following {
            Output::Stall(Stall::Local, "aux_unavailable")
        } else {
            Output::End(HeadersOutcomeCode::HistoryPruned)
        };
    };
    let sent = u32::try_from(entries.len()).expect("a page holds at most MAX_HS_RANGE headers");
    let Some(height) = page.common_ancestor.height.0.checked_add(sent) else {
        return Output::Stall(Stall::Local, "page_shape");
    };
    let last = Frontier::new(block::Height(height), last.header.hash());
    let reaches_target = !job.following && last.hash == job.initial_target;
    let frame = encode_frame(&Message::<HEADER_BYTES>::Headers(Headers {
        subscription_id: job.id,
        initial_target_tip_hash: job.initial_target,
        common_ancestor_height: page.common_ancestor.height,
        common_ancestor_hash: page.common_ancestor.hash,
        reaches_initial_target: reaches_target,
        tree_aux_schema: job.schema,
        entries,
    }));
    match frame {
        Ok(frame) if frame.payload.len() <= page_bytes(job.count, job.schema, HEADER_BYTES) => {
            Output::Page(Page {
                parent: page.common_ancestor,
                last,
                count: sent,
                reaches_target,
                frame,
            })
        }
        _ => Output::Stall(Stall::Local, "encode"),
    }
}
