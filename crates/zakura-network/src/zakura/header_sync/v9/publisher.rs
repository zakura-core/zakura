//! The header sync version 9 publisher: one session's side of the
//! subscriptions a peer opens on this node.
//!
//! [`PublicationLedger`](crate::zakura::regulation::PublicationLedger) holds
//! the subscription rules: credit, the sent cursor ring, and tombstones.
//! This module adds what only header sync knows: which headers each page
//! carries, and when a subscription ends.
//!
//! One task per session writes every page and terminal outcome, so an outcome
//! always follows its subscription's pages. [`Publisher::apply`] applies each
//! update under a short lock and wakes the task, which holds at most one page
//! in progress, so a `Grant` never queues page jobs. A `Close` ends its
//! subscription on the next wake and abandons that subscription's page in
//! progress. Its terminal outcome waits for no credit or output, as
//! [`TerminalPermit::send`](crate::zakura::regulation::TerminalPermit::send)
//! describes.
//!
//! Pages come from the retained-path leases in [`Port`], one lease per page,
//! acquired and read back to back, so a lease's idle deadline covers one read
//! and no subscription ends because a lease expired. Each page after the first
//! acquires with the last header sent as its only locator. State's
//! continuation index answers that without a new ancestry walk only while the
//! lease target stays the same. On the path, the target is the initial target.
//! After it, a run of pages pins the tip of [`Port::selected_chain`] it started
//! toward and keeps leasing toward that tip until it reaches it, so a tip that
//! moves every page still costs one walk per run, not one per page. Whether
//! the selected chain still contains a header comes from
//! [`Port::selected_chain_contains`], which walks nothing and takes no lease.
//! Local failures wait for an update, a selected-chain change, or a capacity
//! release, never for a timer.
//!
//! [`PublisherState`] holds the subscription policy, which runs without a
//! runtime and holds only values. This module holds the resources: the lock,
//! the task, subscription slots, capacity signals, serving permits,
//! retained-path leases, storage calls, and output slots. It also reports what
//! state returns as metrics, logs, and transition events.
//!
//! # Resource lifetimes
//!
//! - A subscription slot is reserved before the `Open` applies. It moves into
//!   a [`TerminalPermit`] when its outcome is taken to write, then into the
//!   frame's guard, and ends when the transport write finishes. A queued or
//!   written terminal keeps its slot after the subscription's key is free.
//! - A page job's budgets enter its worker before it reads. They stay there
//!   through cancellation of the page or the session, and end when the worker
//!   ends, because dropping the waiter does not stop the storage call.
//! - A page's frame stays with its budgets. They become its transport slot's
//!   output grants, which the frame's guard holds until the transport write
//!   finishes. State sees only the page's facts.

use std::{
    any::Any, future::Future, num::NonZeroU32, ops::ControlFlow, panic::AssertUnwindSafe, pin::Pin,
    sync::Arc,
};

use futures::FutureExt;
use thiserror::Error;
use zakura_chain::block;
use zakura_header_chain::{
    BranchId, EngineSnapshot, Frontier, HeaderGeneration, HeaderWorkAuthority, SourceId,
};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, Port, PortError, ReadPath, ReadPathReply, RetainedHeaderPath,
    RetainedHeaderPathPage, SelectedChainMembership, ServingCapacitySignal,
};
use zakura_node_services::task::spawn_erased_named;
use zakura_rt::{
    sync::{mpsc, watch, CancellationToken, Mutex, MutexGuard, Notify, PoisonError},
    task::JoinHandle,
};

use super::{
    entry_bytes, Headers, HeadersOutcome, HeadersOutcomeCode, Message, Operation, SubscribeHeaders,
    HEADERS_RESPONSE_FIXED_BYTES, SUBSCRIBE_HEADERS,
};
use crate::zakura::{
    header_sync::{
        reactor::selected_port_aux_delivery,
        wire::{AuxSchema, HeaderEntry},
    },
    regulation::{
        Applied, ExecutionSlots, PublicationReporter, PublicationSlots, Push, PushPermit, PushSend,
        SlotId, SubscriptionFault, SubscriptionLimits, TerminalPermit,
    },
    transport::sim_chain,
    wire_codec::encode_frame,
    Frame, FramedSend,
};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod model;
mod state;

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use model::{
    replay_header_publisher_bytes, trace_header_publisher_bytes, HeaderPublisherHistory,
    HeaderPublisherReplaySummary, HeaderPublisherStep, HeaderPublisherSubscriptionView,
    HeaderPublisherView,
};
pub(crate) use state::is_legal;
use state::{
    recheck, CapacityWait, Exit, Job, JobKey, Output, Page, PublisherState, Recheck, Stall,
};

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use tests::FakePort;

/// Lease attempts one page makes before it waits for a wake.
const LEASE_ATTEMPTS: usize = 2;

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

/// One session's publisher: the handle the session reader applies updates
/// through.
#[derive(Debug)]
pub(crate) struct Publisher {
    shared: Arc<Mutex<PublisherOwner>>,
    wake: Arc<Notify>,
    limits: PageLimits,
}

/// The publisher's state beside the runtime resources it names by identity,
/// under the one lock that orders them.
#[derive(Debug)]
struct PublisherOwner {
    state: PublisherState,
    slots: PublicationSlots,
    waits: CapacityWaits,
    reporter: PublicationReporter<u64>,
}

impl PublisherOwner {
    /// An owner of `state`, which has no subscription yet.
    fn new(state: PublisherState) -> Self {
        let limits = SubscriptionLimits::from_rule(&SUBSCRIBE_HEADERS)
            .expect("SUBSCRIBE_HEADERS is a subscription row");
        Self {
            state,
            slots: PublicationSlots::new(&limits),
            waits: CapacityWaits::default(),
            reporter: PublicationReporter::new(limits),
        }
    }

    /// This owner, reporting its subscription transitions through `tap` if
    /// there is one.
    #[cfg(any(test, zakura_sim))]
    fn observed_by(mut self, tap: Option<crate::zakura::RegulationTap<u64>>) -> Self {
        self.reporter = self.reporter.observed_by(tap);
        self
    }

    /// Apply `update` and report the result. An `Open` reserves its slot
    /// first, and a refused `Open` frees it.
    fn apply(
        &mut self,
        update: &SubscribeHeaders,
        limits: PageLimits,
    ) -> Result<Applied, PublisherFault> {
        let id = update.subscription_id;
        let applied = match update.operation {
            Operation::Open => {
                let offer = self.slots.offer();
                let opened = self.state.open(update, limits, offer);
                if opened.is_err() {
                    self.slots.refused(offer);
                }
                opened.map(|opened| {
                    self.reporter.opened(&id, update.added_credit, opened);
                    Applied::Live
                })
            }
            Operation::Close => self.state.update(update).inspect(|applied| {
                self.reporter.closed(&id, *applied);
            }),
            Operation::Grant => self.state.update(update).inspect(|applied| {
                self.reporter.granted(&id, update.added_credit, *applied);
            }),
        };
        applied.map_err(|fault| match fault {
            PublisherFault::Subscription(fault) => self.reporter.refused(fault).into(),
            fault => fault,
        })
    }

    /// End every subscription state says is due, report each end, and return
    /// the outcomes in order.
    fn end_due(&mut self) -> Vec<HeadersOutcomeCode> {
        self.state
            .end_due()
            .into_iter()
            .map(|(id, outcome)| {
                self.reporter.ended(&id);
                outcome
            })
            .collect()
    }

    /// Take the next terminal outcome state lets the task write, and bind it
    /// to its encoded frame and its slot's permit.
    fn next_terminal<const HEADER_BYTES: usize>(&mut self) -> Option<OwnedTerminal> {
        let terminal = self.state.next_terminal()?;
        let permit = self
            .slots
            .terminal(terminal.slot)
            .expect("each subscription's slot stays here until its one terminal outcome");
        let frame = encode_frame(&Message::<HEADER_BYTES>::HeadersOutcome(HeadersOutcome {
            subscription_id: terminal.id,
            initial_target_tip_hash: terminal.target,
            outcome: terminal.outcome,
        }))
        .expect("an outcome for a nonzero subscription ID always encodes");
        Some(OwnedTerminal {
            frame,
            permit,
            slot: terminal.slot,
            abandons: terminal.abandons,
        })
    }

    /// Report a capacity refusal for job `key` to state, under the wait for
    /// `signal`. `latest` is the latest selected chain.
    fn capacity_busy(
        &mut self,
        key: JobKey,
        signal: ServingCapacitySignal,
        latest: Option<&EngineSnapshot>,
    ) {
        let wait = self.waits.register(signal);
        self.state.finish(
            key,
            Output::Stall(Stall::Capacity(wait), "capacity"),
            latest,
        );
    }

    /// The signals stalled subscriptions wait on. It forgets every other.
    fn capacity_signals(&mut self) -> Vec<ServingCapacitySignal> {
        self.waits.retain(&self.state.capacity_waits());
        self.waits.signals()
    }

    /// Tell state which waits' signals were released.
    fn capacity_released(&mut self) {
        self.state.capacity_released(&self.waits.released());
    }
}

/// The capacity signals behind each [`CapacityWait`] state holds. It holds at
/// most one per subscription, so a scan stays small.
#[derive(Debug, Default)]
struct CapacityWaits {
    next: u64,
    signals: Vec<(CapacityWait, ServingCapacitySignal)>,
}

impl CapacityWaits {
    /// The wait for `signal`: the existing one, or a new one.
    fn register(&mut self, signal: ServingCapacitySignal) -> CapacityWait {
        if let Some((wait, _)) = self.signals.iter().find(|(_, held)| *held == signal) {
            return *wait;
        }
        let wait = CapacityWait(self.next);
        self.next = self.next.wrapping_add(1);
        self.signals.push((wait, signal));
        wait
    }

    /// Forget every wait outside `current`.
    fn retain(&mut self, current: &[CapacityWait]) {
        self.signals.retain(|(wait, _)| current.contains(wait));
    }

    /// The signals held.
    fn signals(&self) -> Vec<ServingCapacitySignal> {
        self.signals
            .iter()
            .map(|(_, signal)| signal.clone())
            .collect()
    }

    /// The waits whose signal was released.
    fn released(&self) -> Vec<CapacityWait> {
        self.signals
            .iter()
            .filter(|(_, signal)| signal.is_released())
            .map(|(wait, _)| *wait)
            .collect()
    }
}

/// A terminal outcome with its encoded frame and its slot's permit.
#[derive(Debug)]
struct OwnedTerminal {
    frame: Frame,
    permit: TerminalPermit,
    /// The identity to report the write under.
    slot: SlotId,
    /// See [`state::Terminal::abandons`].
    abandons: Option<JobKey>,
}

/// An encoded page: the frame for the output path, and its facts for state.
#[derive(Debug)]
struct EncodedPage {
    page: Page,
    frame: Frame,
}

/// What a page job produced.
#[derive(Debug)]
enum JobResult {
    Page(EncodedPage),
    /// Storage refused a lease for capacity. The signal reports its release.
    CapacityBusy(ServingCapacitySignal),
    Settled(Output),
}

impl From<Output> for JobResult {
    fn from(output: Output) -> Self {
        Self::Settled(output)
    }
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
    /// version 9 codec. In test and simulator builds, `tap` reports the
    /// subscription transitions.
    pub(crate) fn spawn<const HEADER_BYTES: usize>(
        port: Arc<dyn Port>,
        push: Push,
        send: FramedSend,
        owner: LeaseOwner,
        limits: PageLimits,
        cancel: CancellationToken,
        #[cfg(any(test, zakura_sim))] tap: Option<crate::zakura::RegulationTap<u64>>,
    ) -> (Self, JoinHandle<()>) {
        let shared = Arc::new(Mutex::new(sim_chain!(
            PublisherOwner::new(PublisherState::new()) => observed_by(tap)
        )));
        let wake = Arc::new(Notify::new());
        // One panic ends the task, so the channel never needs more room.
        let (panics, panicked) = mpsc::channel(1);
        let task = spawn_erased_named(
            "header_sync_publisher",
            publish::<HEADER_BYTES>(
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
            ),
        );
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
        zakura_rt::task::coop::consume_budget().await;
        applied
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

/// The largest encoding of a page of `count` headers under `schema`.
fn page_bytes(count: u32, schema: AuxSchema, header_bytes: usize) -> usize {
    // Widening u32 to usize is lossless on supported targets.
    HEADERS_RESPONSE_FIXED_BYTES + count as usize * entry_bytes(header_bytes, schema)
}

/// Lock `shared`, recovering from a poisoned lock: every update leaves the
/// state consistent before it can panic.
fn lock(shared: &Mutex<PublisherOwner>) -> MutexGuard<'_, PublisherOwner> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Everything the publisher task owns besides its stream.
struct Task {
    shared: Arc<Mutex<PublisherOwner>>,
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
// The simulator build's sync wrappers make `PushSend` about 376 bytes, past
// clippy's threshold. One `Progress` is returned per page, so boxing the send
// would only add an allocation per page in production.
#[cfg_attr(zakura_sim, allow(clippy::large_enum_variant))]
enum Progress<'a> {
    Page(EncodedPage, PushSend<'a>),
    CapacityBusy(ServingCapacitySignal),
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
    // The page in progress, under the key state recorded for its job.
    let mut work: Option<(JobKey, Work<'_>)> = None;
    loop {
        for outcome in lock(&task.shared).end_due() {
            metrics::counter!(
                "sync.header.v9.publisher.outcome",
                "outcome" => outcome_label(outcome),
            )
            .increment(1);
        }
        loop {
            let Some(terminal) = lock(&task.shared).next_terminal::<HEADER_BYTES>() else {
                break;
            };
            // The subscription sends no more pages, so drop its page in
            // progress. A running job keeps its permit until state answers.
            if terminal.abandons.is_some()
                && work.as_ref().map(|(working, _)| *working) == terminal.abandons
            {
                work = None;
            }
            let written = write_terminal(
                &task.shared,
                &selected,
                &send,
                terminal,
                &mut work,
                &mut panicked,
                &cancel,
            );
            if written.await.is_break() {
                return;
            }
        }
        if work.is_none() {
            let snapshot = selected.borrow().clone();
            let mut shared = lock(&task.shared);
            let job = shared
                .state
                .next_job::<HEADER_BYTES>(snapshot.as_ref(), task.limits);
            let ending = shared.state.end_queued();
            drop(shared);
            if let Some(job) = job {
                let key = job.key;
                let page = start_page::<HEADER_BYTES>(&task, job, &send, selected.clone());
                work = Some((key, page));
            }
            if ending {
                continue;
            }
        }
        let capacity = lock(&task.shared).capacity_signals();
        // The page in progress goes first, so a stream of wakes cannot starve
        // it. Each wake still reaches `end_due` on the next pass.
        zakura_rt::select! {
            biased;
            () = cancel.cancelled() => {
                lock(&task.shared).state.cancelled();
                return;
            }
            // See `Publisher::spawn`.
            Some(panic) = panicked.recv() => std::panic::resume_unwind(panic),
            progress = async { work.as_mut().expect("guarded by is_some").1.as_mut().await },
                if work.is_some() =>
            {
                let (key, _) = work.take().expect("the page in progress just finished");
                if settle(&task.shared, &selected, key, progress).is_break() {
                    return;
                }
            }
            () = task.wake.notified() => {}
            changed = selected.changed(), if selected_open => {
                if changed.is_ok() {
                    lock(&task.shared).state.selected_changed();
                } else {
                    selected_open = false;
                }
            }
            () = any_released(capacity) => lock(&task.shared).capacity_released(),
        }
    }
}

/// Write `terminal` on `send`, and report the result to state.
///
/// The terminal waits for no queue slot, so its first poll ends this wait.
/// Before it, a cancelled session stops here, a page job's panic surfaces, and
/// another subscription's finished page settles: its job gives its execution
/// slots back. A page whose subscription ended in the same batch fails its
/// commit and is dropped.
async fn write_terminal<'a>(
    shared: &Mutex<PublisherOwner>,
    selected: &watch::Receiver<Option<EngineSnapshot>>,
    send: &'a FramedSend,
    terminal: OwnedTerminal,
    work: &mut Option<(JobKey, Work<'a>)>,
    panicked: &mut mpsc::Receiver<Box<dyn Any + Send>>,
    cancel: &CancellationToken,
) -> ControlFlow<Exit> {
    let slot = terminal.slot;
    let written = terminal.permit.send(send, terminal.frame);
    zakura_rt::pin!(written);
    loop {
        zakura_rt::select! {
            biased;
            () = cancel.cancelled() => return ControlFlow::Break(lock(shared).state.cancelled()),
            Some(panic) = panicked.recv() => std::panic::resume_unwind(panic),
            progress = async { work.as_mut().expect("guarded by is_some").1.as_mut().await },
                if work.is_some() =>
            {
                let (key, _) = work.take().expect("the page in progress just finished");
                settle(shared, selected, key, progress)?;
            }
            written = &mut written => return lock(shared).state.terminal_written(slot, written),
        }
    }
}

/// Report the finished page or result of job `key` to state, and write a
/// page that state commits. Breaks when the stream or the runtime closed.
fn settle(
    shared: &Mutex<PublisherOwner>,
    selected: &watch::Receiver<Option<EngineSnapshot>>,
    key: JobKey,
    progress: Progress<'_>,
) -> ControlFlow<Exit> {
    match progress {
        // A page whose subscription closed meanwhile spends nothing and drops
        // its slot.
        Progress::Page(encoded, slot) => {
            if lock(shared).state.commit(key, &encoded.page) {
                slot.send(encoded.frame);
            }
            ControlFlow::Continue(())
        }
        Progress::CapacityBusy(signal) => {
            metrics::counter!("sync.header.v9.publisher.stall", "reason" => "capacity")
                .increment(1);
            let latest = selected.borrow().clone();
            lock(shared).capacity_busy(key, signal, latest.as_ref());
            ControlFlow::Continue(())
        }
        Progress::Other(output) => {
            match &output {
                Output::Retry(reason) => {
                    metrics::counter!("sync.header.v9.publisher.page_dropped", "reason" => *reason)
                        .increment(1);
                }
                Output::Stall(_, reason) => {
                    metrics::counter!("sync.header.v9.publisher.stall", "reason" => *reason)
                        .increment(1);
                }
                _ => {}
            }
            let latest = selected.borrow().clone();
            lock(shared).state.finish(key, output, latest.as_ref());
            ControlFlow::Continue(())
        }
        Progress::Closed => ControlFlow::Break(lock(shared).state.closed()),
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
/// session ends first. A following page whose selected chain moved meanwhile
/// is checked by [`still_selected`] before it is sent.
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
    let following = job.following;
    let seen = job.selected.clone();
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
        let running = zakura_rt::task::spawn_named("header_sync_page_job", async move {
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
        let (result, permit): (JobResult, Admission) = match running.await {
            Ok(Some(done)) => done,
            // The publisher task re-raises the job's panic.
            Ok(None) => return Progress::Other(Output::Stall(Stall::Local, "job_panicked")),
            // The runtime is shutting down.
            Err(_) => return Progress::Closed,
        };
        let (encoded, permit) = match (result, permit) {
            (JobResult::Page(encoded), Admission::Page(permit)) => (encoded, permit),
            (JobResult::Page(_), Admission::Check { .. }) => {
                unreachable!("a check runs `check_following`, which reads no page")
            }
            (JobResult::CapacityBusy(signal), _) => return Progress::CapacityBusy(signal),
            (JobResult::Settled(output), _) => return Progress::Other(output),
        };
        let Ok(slot) = permit.reserve_send(send).await else {
            return Progress::Closed;
        };
        if following
            && !still_selected(&*check_port, &push, id, &selected, seen, &encoded.page).await
        {
            return Progress::Other(Output::Retry("left_selected_chain"));
        }
        Progress::Page(encoded, slot)
    })
}

/// Whether the latest selected chain still contains the last header of a
/// following `page`, whose job read the selected chain `seen`.
///
/// It asks state only when [`recheck`] returns [`Recheck::Query`]. Only a
/// reply that confirms the page keeps it. Anything else drops the page, and
/// the next pass leases again from the last header sent, where the usual
/// stall and outcome rules apply. The query is state work, so it waits for
/// execution slots through `push` as the page's read did.
///
/// One query is enough: state answers from its latest committed chain, which
/// is at least as new as the watch. A reorganization can still overtake any
/// page after it is written, and the next page ends that subscription as
/// superseded.
async fn still_selected(
    port: &dyn Port,
    push: &Push,
    id: u64,
    selected: &watch::Receiver<Option<EngineSnapshot>>,
    seen: Option<EngineSnapshot>,
    page: &Page,
) -> bool {
    let latest = selected.borrow().clone();
    match recheck(seen.as_ref(), latest.as_ref(), page) {
        Recheck::Keep => return true,
        Recheck::Drop => return false,
        Recheck::Query => {}
    }
    let _execution = push.execution().await;
    match port.selected_chain_contains(page.last.hash).await {
        Ok(SelectedChainMembership::Selected(found)) => found == page.last,
        Ok(_) => false,
        Err(error) => {
            log_port_failure(id, "recheck", &error);
            false
        }
    }
}

/// Read one page, or decide the subscription's outcome, through `port`.
async fn run_job<const HEADER_BYTES: usize>(
    port: &dyn Port,
    owner: LeaseOwner,
    job: &Job,
) -> JobResult {
    if job.count == 0 {
        return check_following(port, job).await.into();
    }
    if job.probe {
        match port.selected_chain_contains(job.initial_target).await {
            Ok(SelectedChainMembership::Selected(_)) => {}
            Ok(SelectedChainMembership::NotSelected) => {
                return Output::End(HeadersOutcomeCode::TargetNotSelected).into()
            }
            Ok(SelectedChainMembership::NotRetained) => {
                return Output::End(HeadersOutcomeCode::TargetNotRetained).into()
            }
            Err(error) => return port_failure(job.id, "probe", &error).into(),
        }
    }
    // Keep leasing toward the pinned tip while it is selected, so the lease
    // reuses state's continuation. Otherwise start a new run toward the tip.
    let lease_target = match job.pin {
        Some(pin) => match port.selected_chain_contains(pin.hash).await {
            Ok(SelectedChainMembership::Selected(found)) if found == pin => pin.hash,
            Ok(_) => job.lease_target,
            Err(error) => return port_failure(job.id, "pin", &error).into(),
        },
        None => job.lease_target,
    };
    for _ in 0..LEASE_ATTEMPTS {
        let locators = job.locators.clone();
        let reply = acquire(port, owner, job.selected.as_ref(), lease_target, locators).await;
        let path = match reply {
            Ok(AcquirePathReply::Acquired(path)) => *path,
            Ok(AcquirePathReply::CapacityBusy(signal)) => {
                return JobResult::CapacityBusy(signal).into()
            }
            // The selected chain no longer extends the last header sent.
            Ok(AcquirePathReply::NoLocatorIntersection) if job.following => {
                return Output::End(HeadersOutcomeCode::SubscriptionSuperseded).into()
            }
            Ok(AcquirePathReply::NoLocatorIntersection) => {
                return Output::End(HeadersOutcomeCode::NoLocatorIntersection).into()
            }
            // A stale selected tip: the next selected-chain change retries.
            Ok(AcquirePathReply::TargetNotRetained) if job.following => {
                return Output::Stall(Stall::Local, "selected_tip").into()
            }
            Ok(AcquirePathReply::TargetNotRetained) => {
                return Output::End(HeadersOutcomeCode::TargetNotRetained).into()
            }
            Ok(AcquirePathReply::HistoryPruned) => {
                return Output::End(HeadersOutcomeCode::HistoryPruned).into()
            }
            Ok(AcquirePathReply::Busy) => return Output::Stall(Stall::Local, "acquire").into(),
            Err(error) => return port_failure(job.id, "acquire", &error).into(),
        };
        if job.parent.is_none() && path.common_ancestor == path.target {
            let target = path.target;
            release(port, job.id, path).await;
            return Output::ReachedAtOpen(target).into();
        }
        if job
            .parent
            .is_some_and(|parent| parent != path.common_ancestor)
        {
            release(port, job.id, path).await;
            return Output::Stall(Stall::Local, "lease_parent").into();
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
                return port_failure(job.id, "read", &error).into();
            }
        }
    }
    Output::Stall(Stall::Local, "lease_lost").into()
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
/// The selected tip is above the last header, so the chain extends that
/// header exactly when it contains it.
async fn check_following(port: &dyn Port, job: &Job) -> Output {
    let last = job
        .parent
        .expect("a following subscription has sent or reached a header");
    match port.selected_chain_contains(last.hash).await {
        Ok(SelectedChainMembership::Selected(found)) if found == last => {
            Output::Extends(job.lease_target)
        }
        Ok(_) => Output::End(HeadersOutcomeCode::SubscriptionSuperseded),
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
fn page_output<const HEADER_BYTES: usize>(job: &Job, page: RetainedHeaderPathPage) -> JobResult {
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
        return Output::Stall(Stall::Local, "page_shape").into();
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
        let output = if job.following {
            Output::Stall(Stall::Local, "aux_unavailable")
        } else {
            Output::End(HeadersOutcomeCode::HistoryPruned)
        };
        return output.into();
    };
    let sent = u32::try_from(entries.len()).expect("a page holds at most MAX_HS_RANGE headers");
    let Some(height) = page.common_ancestor.height.0.checked_add(sent) else {
        return Output::Stall(Stall::Local, "page_shape").into();
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
            JobResult::Page(EncodedPage {
                page: Page {
                    parent: page.common_ancestor,
                    last,
                    target: page.target,
                    count: sent,
                    reaches_target,
                    payload_len: frame.payload.len(),
                },
                frame,
            })
        }
        _ => Output::Stall(Stall::Local, "encode").into(),
    }
}
