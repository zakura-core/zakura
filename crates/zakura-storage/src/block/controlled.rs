//! A block service whose operations wait for the run to complete them.
//!
//! [`ControlledBlockService`] answers block sync's action channel as zakurad's
//! driver does, over an [`EnginePort`] that it shares with the node's header
//! side, as the driver shares zakura-state with header sync. Serving's range
//! reads arrive through [`ControlledRangeSource`].
//!
//! # Taking actions
//!
//! The driver takes one action at a time off its bounded channel, and the
//! service takes them the same way, so the channel stays the only buffer and
//! its backpressure stays real. A step takes the next action as the top of
//! the driver's loop does: `coalesce_ready_needed_block_queries` keeps only
//! the latest query, behind every other action already taken off the
//! channel, and those deferred actions go first. With nothing taken, the
//! step waits for the channel. When the step takes a query,
//! `coalesce_stale_needed_block_queries` drains the channel: a later query
//! replaces it, and every other action is deferred. A replaced query gets no
//! reply, and the step publishes a [`FactKind::Coalesced`] fact for it. A
//! step that changes the deferred actions publishes them in a
//! [`FactKind::Deferred`] fact.
//!
//! The driver awaits each query and record before it takes its next action,
//! so the service takes no action while one is pending, and the action
//! channel fills. A misbehavior report is recorded when the driver takes it:
//! the driver awaits only its enforcement, which the simulator doesn't run.
//! [`ControlledBlockService::spawn`] runs these steps on one named task.
//! [`ControlledBlockService::take_ready`] and
//! [`ControlledBlockService::take_received`] run one step each, for tests
//! that hold the channel themselves.
//!
//! # Completing operations
//!
//! The run lists the pending operations with
//! [`ControlledBlockService::pending`] and finishes one with
//! [`ControlledBlockService::complete`]. A completion runs against the engine
//! and full state, replies through the handle, and publishes one
//! transaction. It's the operation's linearization point, as the contract in
//! [`super`] says. Only the verifier is modeled: the run picks each verdict.
//! The verifier receives a body when the run admits it, which fixes the
//! body's receipt order. Overlapping admissions of the same body share the
//! first one's receipt, as zakura-consensus's `ReceiptRegistry` does. A
//! commit the engine refuses, finds stale or stalls on its resource limits
//! commits nothing, and its reply reports a `Storage` retry, as
//! zakura-state's `HeaderChainError` does. The engine refuses, for one, a
//! body recorded invalid or a body whose ancestor is.
//!
//! Every change is published as a transaction: a step that takes an action,
//! a read's arrival, an admission, a completion, a read caller dropping its
//! call or its reply, the driver's exit, or shutdown. A transaction's
//! [`Fact`]s carry one transaction number and consecutive sequence numbers,
//! in the order the changes happened, and end with one head fact that names
//! the transaction. Each transaction is published in the critical section
//! that makes its last change.
//!
//! # Ending
//!
//! A reply goes to block sync's handle. If the handle is closed, the reply
//! is dropped. When a query's reply finds the handle closed, the driver
//! returns, which drops its channel's receiver and its pending actions, so
//! block sync's later sends fail with `Closed`. The service exits the same
//! way, and keeps serving range reads, which zakura-state serves without the
//! driver. [`ControlledBlockService::shut_down`] ends the run: it drops
//! every operation, reads included, and closes the view watch.
//!
//! The driver's exit drops its admitted bodies here, so none of them
//! commits. Production's verifier keeps each apply it received and may
//! still commit it after the driver returns; the contract leaves that
//! commit out.
//!
//! A `record_body_invalid` that isn't durable after its stale refreshes,
//! because the engine stalls it, refuses it, or still finds it stale, is
//! where the driver fails closed and shuts down. The contract excludes
//! that shutdown, so the service panics there.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    future::Future,
    mem,
    num::NonZeroU64,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
};

use zakura_chain::block;
use zakura_header_chain::{
    ApplyResult, BodyEvidence, StateVersion, TransitionEvent, TransitionRequest,
};
use zakura_network::{
    zakura::{BlockRangeReadResult, BlockSyncAction, BlockSyncHandle},
    BoxError,
};
use zakura_rt::{sync::mpsc, task::JoinHandle, time::Instant};

use super::{
    evidence::{
        OperationStage, OperationState, OwnerState, RecordResult, RequestIdentity, ResultIdentity,
        ViewIdentity,
    },
    range::ControlledRangeSource,
    ApplyClass, ApplyLimits, BlockOp, BlockOutcome, BlockRequest, Conditions, HandleEvent,
    IllegalOutcome, RetryKind, Stage, Verdict, BODY_INVALID_STALE_REFRESH_LIMIT,
};
use crate::{
    full_state::{query_needed_blocks, StateError},
    header::adapter::{Applied, EnginePort},
};

/// A pending operation's ID, unique within its service. IDs count up from 1
/// in arrival order, so they are also the canonical order.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpId(pub u64);

/// A pending operation, as [`ControlledBlockService::pending`] lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingOp {
    /// Its ID.
    pub id: OpId,
    /// Its operation.
    pub op: BlockOp,
    /// Its stage.
    pub stage: Stage,
    /// The outcomes the run may complete it with now, in canonical order.
    pub enabled: Vec<BlockOutcome>,
}

/// What [`ControlledBlockService::complete`] did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Completion {
    /// The completion's transaction number.
    pub txn: u64,
    /// The operation's ID.
    pub id: OpId,
    /// The operation.
    pub op: BlockOp,
    /// What it asked for.
    pub request: RequestIdentity,
    /// The outcome it completed with.
    pub outcome: BlockOutcome,
    /// What it returned.
    pub result: ResultIdentity,
    /// Whether its reply reached its receiver: the handle for an action, the
    /// waiting caller for a read. `false` for an admission or a record,
    /// which reply to nobody.
    pub delivered: bool,
    /// The engine's version once the completion finished.
    pub state_version: StateVersion,
}

/// One published change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fact {
    /// Its sequence number, from 1, unique and gapless within the service.
    pub seq: u64,
    /// Its transaction's number, from 1. A transaction's facts are
    /// consecutive.
    pub txn: u64,
    /// When its transaction happened.
    pub at: Instant,
    /// What changed.
    pub kind: FactKind,
}

/// What a [`Fact`] records. The head kinds name their transaction and come
/// last in it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FactKind {
    /// The step that took an action dropped this later-replaced query
    /// unanswered, as the driver coalesces queries.
    Coalesced {
        /// The dropped query.
        query_id: NonZeroU64,
    },
    /// The step that took an action left these deferred actions, oldest
    /// first, in place of the ones before it.
    Deferred {
        /// The deferred actions.
        queue: Vec<RequestIdentity>,
    },
    /// The engine installed a transition. Read from the engine, apart from
    /// the view.
    TransitionInstalled {
        /// Its version.
        state_version: StateVersion,
        /// The verified tip after it.
        verified_best: zakura_header_chain::Frontier,
        /// The body-work epoch before it.
        epoch_before: zakura_header_chain::BodyWorkEpoch,
        /// Whether its effect invalidated body work.
        invalidates_body_work: bool,
        /// The verified-chain change a commit sent, if any.
        cause: Option<zakura_header_chain::VerifiedChangeCause>,
    },
    /// The view of the transition just installed went out.
    ViewPublished(ViewIdentity),
    /// The handle accepted an operation's reply.
    ReplyDelivered {
        /// The operation.
        id: OpId,
    },
    /// The handle was closed, so an operation's reply was dropped.
    ReplyDropped {
        /// The operation.
        id: OpId,
    },
    /// Head: the driver took an action, or serving submitted a read.
    /// `stage` is the stage it starts in, or `None` for a misbehavior
    /// report, which is recorded when the driver takes it.
    Submitted {
        /// The new operation's ID.
        id: OpId,
        /// What it asks for.
        request: RequestIdentity,
        /// Its first stage.
        stage: Option<Stage>,
    },
    /// Head: the run admitted a queued submission to the verifier.
    Admitted {
        /// The operation.
        id: OpId,
    },
    /// Head: the run completed an operation.
    Completed(Box<Completion>),
    /// Head: a read's caller dropped its call while the read was pending.
    /// The read keeps its lease until it completes.
    CallerDropped {
        /// The operation.
        id: OpId,
    },
    /// Head: a read's caller took its reply.
    ReadTaken {
        /// The operation.
        id: OpId,
    },
    /// Head: a read's caller dropped its reply without taking it.
    ReadDropped {
        /// The operation.
        id: OpId,
    },
    /// Head: a query's reply found the handle closed, so the driver
    /// returned, dropping every action operation and deferred action.
    DriverExited {
        /// The operations it dropped.
        dropped: Vec<OpId>,
        /// The deferred actions it dropped, oldest first.
        deferred: Vec<RequestIdentity>,
    },
    /// Head: the service shut down.
    ShutDown {
        /// The operations it dropped.
        dropped: Vec<OpId>,
        /// The deferred actions it dropped, oldest first.
        deferred: Vec<RequestIdentity>,
    },
}

impl FactKind {
    /// Whether this kind names its transaction and comes last in it.
    pub fn is_head(&self) -> bool {
        !matches!(
            self,
            FactKind::Coalesced { .. }
                | FactKind::Deferred { .. }
                | FactKind::TransitionInstalled { .. }
                | FactKind::ViewPublished(_)
                | FactKind::ReplyDelivered { .. }
                | FactKind::ReplyDropped { .. }
        )
    }
}

/// Where the service sends block sync's replies.
pub trait BlockReplies: fmt::Debug + Send + Sync + 'static {
    /// Sends `event`, or fails if block sync's handle is closed.
    fn send(&self, event: HandleEvent) -> Result<(), HandleClosed>;
}

/// Block sync's handle was closed, so a reply was dropped.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HandleClosed;

impl BlockReplies for BlockSyncHandle {
    fn send(&self, event: HandleEvent) -> Result<(), HandleClosed> {
        match event {
            HandleEvent::NeededBlocksFailed { query_id, scope } => self
                .send_needed_blocks_query_failure(query_id, scope)
                .map_err(|_| HandleClosed),
            event => self
                .send_control(
                    event
                        .into_control()
                        .expect("every other handle event is a control event"),
                )
                .map_err(|_| HandleClosed),
        }
    }
}

/// Records every reply, for tests that read what block sync would receive.
#[derive(Debug, Default)]
pub struct ReplyLog {
    events: Mutex<Vec<HandleEvent>>,
    closed: Mutex<bool>,
}

impl ReplyLog {
    /// The replies since the last call, in order.
    pub fn take(&self) -> Vec<HandleEvent> {
        mem::take(&mut self.events.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Closes the log, as block sync's handle closes, so every later reply
    /// is dropped.
    pub fn close(&self) {
        *self.closed.lock().unwrap_or_else(PoisonError::into_inner) = true;
    }
}

impl BlockReplies for ReplyLog {
    fn send(&self, event: HandleEvent) -> Result<(), HandleClosed> {
        if *self.closed.lock().unwrap_or_else(PoisonError::into_inner) {
            return Err(HandleClosed);
        }
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
        Ok(())
    }
}

/// The driver's action channel, read without waiting, as the driver's
/// `try_recv` reads it.
pub trait Inbox {
    /// The next action waiting, or `None` if there is none now.
    fn try_take(&mut self) -> Option<BlockSyncAction>;
}

impl Inbox for mpsc::Receiver<BlockSyncAction> {
    fn try_take(&mut self) -> Option<BlockSyncAction> {
        self.try_recv().ok()
    }
}

/// A test's stand-in for the channel: the actions block sync sent, oldest
/// first.
impl Inbox for VecDeque<BlockSyncAction> {
    fn try_take(&mut self) -> Option<BlockSyncAction> {
        self.pop_front()
    }
}

/// What one step of the driver did.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Taken {
    /// It took an action, which became this operation.
    Op(OpId),
    /// Nothing was waiting: the driver waits on its channel.
    Idle,
    /// The driver awaits a query or record, so it takes nothing.
    Busy,
    /// The driver has exited or the service has shut down.
    Stopped,
}

/// Why [`ControlledBlockService::bind_to_run`] refused.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BindError {
    /// No simulator run is active on this thread.
    NotInRun,
    /// The service already published a fact or holds an operation.
    NotPristine,
    /// The service is already bound.
    AlreadyBound,
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BindError::NotInRun => "no simulator run is active",
            BindError::NotPristine => "the service has activity from before the run",
            BindError::AlreadyBound => "the service is already bound to a run",
        })
    }
}

impl std::error::Error for BindError {}

/// Why [`ControlledBlockService::complete`] refused a completion. A refusal
/// changes nothing.
#[derive(Debug)]
pub enum CompleteError {
    /// No operation with this ID is pending at a stage the run completes.
    NotPending(OpId),
    /// The contract doesn't allow this outcome for the operation now.
    Illegal(IllegalOutcome),
    /// Full state found the commit incoherent, which leaves it and the
    /// engine unchanged: the body has no coinbase height, is already
    /// committed, or its parent isn't. Engine refusals aren't errors here,
    /// since zakura-state reports them as a `Storage` retry.
    State(StateError),
    /// The service is bound to another run.
    OutsideRun,
}

impl fmt::Display for CompleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompleteError::NotPending(id) => write!(f, "no block operation {} is pending", id.0),
            CompleteError::Illegal(illegal) => illegal.fmt(f),
            CompleteError::State(error) => write!(f, "full state refused the commit: {error}"),
            CompleteError::OutsideRun => f.write_str("the service is bound to another run"),
        }
    }
}

impl std::error::Error for CompleteError {}

/// A block service whose operations complete only when the run says so. The
/// module doc describes the lifecycle. Clones share one service.
#[derive(Clone, Debug)]
pub struct ControlledBlockService {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub(super) struct Inner {
    engine: Arc<EnginePort>,
    replies: Arc<dyn BlockReplies>,
    limits: ApplyLimits,
    /// The service's bookkeeping, so a std lock, as `ControlledPort`'s is.
    /// It is never held across a scheduling point or an engine call, and a
    /// removed slot or a waker drops only after it's released.
    ops: Mutex<Ops>,
}

/// Every operation not yet finished, and the driver's intake.
#[derive(Debug, Default)]
struct Ops {
    /// The last ID handed out.
    last: u64,
    slots: BTreeMap<OpId, Slot>,
    /// The actions the driver took off its channel and deferred, oldest
    /// first. Only a step takes them out.
    deferred: VecDeque<BlockSyncAction>,
    /// `deferred`'s identities as the last transaction left them. A step
    /// holds `deferred` outside the lock while it reads the channel, so
    /// the owner state and the drop facts read these.
    published_deferred: Vec<RequestIdentity>,
    /// The intake task's waker while it waits for the driver to be free or
    /// for its channel.
    intake: Option<Waker>,
    /// The last verifier receipt handed out.
    last_receipt: u64,
    /// The last sequence number and transaction number handed out.
    last_seq: u64,
    last_txn: u64,
    /// Facts not yet taken.
    facts: Vec<Fact>,
    /// Set when the driver returned, after which it takes no action.
    exited: bool,
    /// Set by shutdown, after which nothing is submitted.
    shut_down: bool,
    /// Set once the service is bound to a run.
    bound: bool,
    /// The run it's bound to.
    #[cfg(zakura_sim)]
    run: Option<zakura_rt::sim::RunId>,
    /// Set when something used the service outside that run.
    #[cfg(zakura_sim)]
    refused: bool,
}

/// What every operation's gates share.
struct Gates {
    /// The driver awaits a query or record.
    driver_awaiting: bool,
    /// The oldest queued submission whose class has room.
    next_admission: Option<OpId>,
    /// The oldest admitted submission of each hash.
    oldest_admitted: BTreeMap<[u8; 32], OpId>,
}

/// One held operation.
#[derive(Debug)]
struct Slot {
    stage: SlotStage,
    identity: RequestIdentity,
    /// The submitted body's class, `Full` for every other operation.
    class: ApplyClass,
    /// The later admissions of the same hash that replaced this one. Full
    /// state's queue replaced it, so this is history no container holds:
    /// an admission stays here after it finishes. Full state either
    /// replaces the older copy or refuses the newer one, so an admission
    /// that reports a duplicate leaves the set.
    replaced_by: BTreeSet<OpId>,
    /// Whether full state held the body's hash when the run admitted it.
    /// The verifier checks for a duplicate first, so this, not whether the
    /// hash is committed now, decides whether a consensus rule can fail.
    committed_at_admission: bool,
    /// The verifier's receipt order, once admitted.
    receipt: Option<u64>,
    /// A read's caller: its waker, or `None` once it dropped its call. An
    /// action's reply goes to the handle, so it has none.
    caller: Option<Waker>,
}

#[derive(Debug)]
enum SlotStage {
    /// Waiting at `stage` with its request.
    Waiting { stage: Stage, request: BlockRequest },
    /// Taken by `complete`, which is running it from `stage`.
    Running(Stage),
    /// A read's reply, waiting for its caller.
    Replied(Result<BlockRangeReadResult, BoxError>),
}

impl Slot {
    /// The operation waiting or running here.
    fn op(&self) -> BlockOp {
        self.identity.op()
    }

    /// Whether the driver awaits this operation: a query or record that the
    /// run hasn't finished.
    fn awaited(&self) -> bool {
        self.op().awaited()
            && matches!(
                self.stage,
                SlotStage::Waiting { .. } | SlotStage::Running(_)
            )
    }

    /// Whether the verifier holds this submission.
    fn admitted(&self) -> bool {
        matches!(
            self.stage,
            SlotStage::Waiting {
                stage: Stage::Admitted,
                ..
            } | SlotStage::Running(Stage::Admitted)
        )
    }

    /// The submitted block this slot's request holds.
    fn submitted_block(&self) -> Option<&Arc<block::Block>> {
        match &self.stage {
            SlotStage::Waiting {
                request: BlockRequest::Action(BlockSyncAction::SubmitBlock { block, .. }),
                ..
            } => Some(block),
            _ => None,
        }
    }

    /// The hash of the block this slot's request holds.
    fn held_block(&self) -> Option<block::Hash> {
        self.submitted_block().map(|block| block.hash())
    }
}

impl Ops {
    /// Whether the service is bound to a run that isn't the one active now,
    /// which also marks it. Never true for an unbound service, or outside
    /// the simulator build.
    fn refuse_outside_run(&mut self) -> bool {
        #[cfg(zakura_sim)]
        if self
            .run
            .is_some_and(|run| zakura_rt::sim::run_id() != Some(run))
        {
            self.refused = true;
            return true;
        }
        false
    }

    /// Publishes `kinds` as one transaction at `at`, in order, and returns
    /// its number. `head` builds the head fact from that number.
    fn publish(
        &mut self,
        at: Instant,
        mut kinds: Vec<FactKind>,
        head: impl FnOnce(u64) -> FactKind,
    ) -> u64 {
        self.last_txn = self
            .last_txn
            .checked_add(1)
            .expect("a run publishes fewer than 2^64 transactions");
        let txn = self.last_txn;
        kinds.push(head(txn));
        debug_assert!(
            kinds[..kinds.len() - 1].iter().all(|kind| !kind.is_head())
                && kinds.last().is_some_and(FactKind::is_head),
            "a transaction ends with exactly one head fact"
        );
        for kind in kinds {
            self.last_seq = self
                .last_seq
                .checked_add(1)
                .expect("a run publishes fewer than 2^64 facts");
            self.facts.push(Fact {
                seq: self.last_seq,
                txn,
                at,
                kind,
            });
        }
        txn
    }

    /// Hands out the next ID.
    fn next_id(&mut self) -> OpId {
        self.last = self
            .last
            .checked_add(1)
            .expect("a run submits fewer than 2^64 operations");
        OpId(self.last)
    }

    /// Whether the driver awaits a query or record.
    fn driver_awaiting(&self) -> bool {
        self.slots.values().any(Slot::awaited)
    }

    /// Whether the driver takes no more actions.
    fn stopped(&self) -> bool {
        self.exited || self.shut_down
    }

    /// The contract stage of `id`, or `None` if the run can't complete it.
    fn stage(&self, id: OpId) -> Option<(Stage, BlockOp)> {
        match &self.slots.get(&id)?.stage {
            SlotStage::Waiting { stage, request } => Some((*stage, request.op())),
            SlotStage::Running(_) | SlotStage::Replied(_) => None,
        }
    }

    /// What every operation's gates share, read once per enumeration so
    /// that listing every operation stays near-linear.
    fn gates(&self, limits: &ApplyLimits) -> Gates {
        let (in_flight_checkpoint, in_flight_full) =
            self.slots.values().filter(|other| other.admitted()).fold(
                (0usize, 0usize),
                |(checkpoint, full), other| match other.class {
                    ApplyClass::Checkpoint => (checkpoint + 1, full),
                    ApplyClass::Full => (checkpoint, full + 1),
                },
            );
        let next_admission = self
            .slots
            .iter()
            .find(|(_, slot)| {
                matches!(
                    slot.stage,
                    SlotStage::Waiting {
                        stage: Stage::Queued,
                        ..
                    }
                ) && limits.has_room(slot.class, in_flight_checkpoint, in_flight_full)
            })
            .map(|(id, _)| *id);
        let mut oldest_admitted = BTreeMap::new();
        for (id, slot) in &self.slots {
            if let Some(hash) = slot.identity.submitted_hash().filter(|_| slot.admitted()) {
                oldest_admitted.entry(hash.0).or_insert(*id);
            }
        }
        Gates {
            driver_awaiting: self.driver_awaiting(),
            next_admission,
            oldest_admitted,
        }
    }

    /// What the gates read for `id`, given what all gates share and which
    /// hashes full state holds.
    fn conditions(&self, id: OpId, gates: &Gates, committed: &BTreeSet<[u8; 32]>) -> Conditions {
        let slot = &self.slots[&id];
        let older_in_flight = slot.identity.submitted_hash().is_some_and(|hash| {
            gates
                .oldest_admitted
                .get(&hash.0)
                .is_some_and(|oldest| *oldest < id)
        });
        let (parent_committed, committed_now) = match &slot.identity {
            RequestIdentity::Submit { hash, parent, .. } => {
                (committed.contains(&parent.0), committed.contains(&hash.0))
            }
            _ => (false, false),
        };
        Conditions {
            driver_awaiting: gates.driver_awaiting,
            oldest_with_room: gates.next_admission == Some(id),
            parent_committed,
            committed: committed_now,
            committed_at_admission: slot.committed_at_admission,
            replaced: !slot.replaced_by.is_empty(),
            older_in_flight,
        }
    }

    /// Operation `id` with the outcomes legal now, or `None` if the run
    /// can't complete it.
    fn pending(
        &self,
        id: OpId,
        gates: &Gates,
        committed: &BTreeSet<[u8; 32]>,
    ) -> Option<PendingOp> {
        let (stage, op) = self.stage(id)?;
        let conditions = self.conditions(id, gates, committed);
        Some(PendingOp {
            id,
            op,
            stage,
            enabled: op.enabled(stage, self.slots[&id].class, &conditions),
        })
    }

    /// The hashes the gates of submissions read.
    fn gated_hashes(&self) -> Vec<block::Hash> {
        self.slots
            .values()
            .filter_map(|slot| match &slot.identity {
                RequestIdentity::Submit { hash, parent, .. } => Some([*hash, *parent]),
                _ => None,
            })
            .flatten()
            .collect()
    }
}

/// `coalesce_ready_needed_block_queries`: keeps only the latest query among
/// the deferred actions and, if none is left, the channel's, and returns it
/// if nothing else waits. Otherwise the latest query goes behind the
/// deferred actions. Each replaced query's ID goes to `coalesced`.
fn coalesce_ready(
    deferred: &mut VecDeque<BlockSyncAction>,
    inbox: &mut dyn Inbox,
    coalesced: &mut Vec<NonZeroU64>,
) -> Option<BlockSyncAction> {
    let mut latest = None;
    let mut retained = VecDeque::new();
    while let Some(action) = deferred.pop_front() {
        match action {
            BlockSyncAction::QueryNeededBlocks { .. } => {
                replace_query(&mut latest, action, coalesced)
            }
            action => retained.push_back(action),
        }
    }
    *deferred = retained;
    if deferred.is_empty() {
        while let Some(action) = inbox.try_take() {
            match action {
                BlockSyncAction::QueryNeededBlocks { .. } => {
                    replace_query(&mut latest, action, coalesced)
                }
                action => deferred.push_back(action),
            }
        }
    }
    if deferred.is_empty() {
        return latest;
    }
    deferred.extend(latest);
    None
}

/// `coalesce_stale_needed_block_queries`: once the driver takes a query,
/// every later query in the channel replaces it, and every other action is
/// deferred.
fn coalesce_stale(
    action: BlockSyncAction,
    deferred: &mut VecDeque<BlockSyncAction>,
    inbox: &mut dyn Inbox,
    coalesced: &mut Vec<NonZeroU64>,
) -> BlockSyncAction {
    if !matches!(action, BlockSyncAction::QueryNeededBlocks { .. }) {
        return action;
    }
    let mut latest = Some(action);
    while let Some(action) = inbox.try_take() {
        match action {
            BlockSyncAction::QueryNeededBlocks { .. } => {
                replace_query(&mut latest, action, coalesced)
            }
            action => deferred.push_back(action),
        }
    }
    latest.expect("the taken query stays until a later one replaces it")
}

/// Makes `query` the latest query, recording the one it replaces.
fn replace_query(
    latest: &mut Option<BlockSyncAction>,
    query: BlockSyncAction,
    coalesced: &mut Vec<NonZeroU64>,
) {
    if let Some(BlockSyncAction::QueryNeededBlocks { query_id, .. }) = latest.replace(query) {
        coalesced.push(query_id);
    }
}

impl ControlledBlockService {
    /// A service over `engine` that replies through `replies`, admitting
    /// applies under `limits`.
    ///
    /// # Panics
    ///
    /// If another block service already commits through `engine`.
    pub fn new(
        engine: Arc<EnginePort>,
        replies: Arc<dyn BlockReplies>,
        limits: ApplyLimits,
    ) -> Self {
        engine.bind_block_service();
        ControlledBlockService {
            inner: Arc::new(Inner {
                engine,
                replies,
                limits,
                ops: Mutex::default(),
            }),
        }
    }

    /// The engine port underneath, shared with the header side.
    pub fn engine(&self) -> &Arc<EnginePort> {
        &self.inner.engine
    }

    /// The apply limits.
    pub fn limits(&self) -> ApplyLimits {
        self.inner.limits
    }

    /// The range source serving reads through.
    pub fn range_source(&self) -> ControlledRangeSource {
        ControlledRangeSource::new(self.clone())
    }

    /// Spawns the named task that takes each action off `actions` as the
    /// driver does. It waits while the driver awaits a query or record, and
    /// ends, dropping `actions`, when the channel closes, the driver exits or
    /// the service shuts down.
    pub fn spawn(&self, mut actions: mpsc::Receiver<BlockSyncAction>) -> JoinHandle<()> {
        let service = self.clone();
        zakura_rt::task::spawn_named("zakura-storage-block-actions", async move {
            loop {
                match service.take_ready(&mut actions) {
                    Taken::Op(_) => {}
                    Taken::Busy => {
                        if !(DriverFree { service: &service }).await {
                            return;
                        }
                    }
                    Taken::Idle => {
                        let received = NextAction {
                            service: &service,
                            actions: &mut actions,
                        }
                        .await;
                        match received {
                            Some(action) => {
                                service.take_received(action, &mut actions);
                            }
                            None => return,
                        }
                    }
                    Taken::Stopped => return,
                }
            }
        })
    }

    /// Runs the top of the driver's loop once: takes the next deferred or
    /// waiting action, after coalescing queries, as the module doc says.
    pub fn take_ready(&self, inbox: &mut dyn Inbox) -> Taken {
        let mut deferred = {
            let mut ops = self.ops();
            if ops.stopped() || ops.refuse_outside_run() {
                return Taken::Stopped;
            }
            if ops.driver_awaiting() {
                return Taken::Busy;
            }
            mem::take(&mut ops.deferred)
        };
        let before = deferred_identities(&deferred, &self.inner.limits);
        // The channel is read outside the lock, since a channel operation
        // can be a scheduling point.
        let mut coalesced = Vec::new();
        let action = match coalesce_ready(&mut deferred, inbox, &mut coalesced) {
            Some(action) => Some(action),
            None => deferred.pop_front(),
        };
        let action =
            action.map(|action| coalesce_stale(action, &mut deferred, inbox, &mut coalesced));
        self.take(action, before, deferred, coalesced)
    }

    /// Runs the driver's receive branch: the driver waited on an empty
    /// channel and received `action`, then coalesces queries behind it.
    pub fn take_received(&self, action: BlockSyncAction, inbox: &mut dyn Inbox) -> Taken {
        let mut deferred = {
            let mut ops = self.ops();
            if ops.stopped() || ops.refuse_outside_run() {
                drop(ops);
                drop(action);
                return Taken::Stopped;
            }
            mem::take(&mut ops.deferred)
        };
        let before = deferred_identities(&deferred, &self.inner.limits);
        let mut coalesced = Vec::new();
        let action = coalesce_stale(action, &mut deferred, inbox, &mut coalesced);
        self.take(Some(action), before, deferred, coalesced)
    }

    /// Records the action a step took, with what it coalesced and the
    /// deferred actions it left, which were `before` until the step.
    fn take(
        &self,
        action: Option<BlockSyncAction>,
        before: Vec<RequestIdentity>,
        deferred: VecDeque<BlockSyncAction>,
        coalesced: Vec<NonZeroU64>,
    ) -> Taken {
        let mut ops = self.ops();
        if ops.stopped() {
            drop(ops);
            // Dropped outside the lock, with the blocks they hold.
            drop((action, deferred));
            return Taken::Stopped;
        }
        let queue = deferred_identities(&deferred, &self.inner.limits);
        ops.deferred = deferred;
        ops.published_deferred.clone_from(&queue);
        let Some(action) = action else {
            debug_assert!(
                coalesced.is_empty() && queue == before,
                "a step coalesces and defers only behind an action"
            );
            return Taken::Idle;
        };
        let mut kinds: Vec<FactKind> = coalesced
            .into_iter()
            .map(|query_id| FactKind::Coalesced { query_id })
            .collect();
        if queue != before {
            kinds.push(FactKind::Deferred { queue });
        }
        Taken::Op(self.insert(&mut ops, BlockRequest::Action(action), None, kinds))
    }

    /// Holds `request` as a new operation and publishes its arrival, after
    /// `kinds`.
    fn insert(
        &self,
        ops: &mut Ops,
        request: BlockRequest,
        caller: Option<Waker>,
        kinds: Vec<FactKind>,
    ) -> OpId {
        let class = request
            .submitted(&self.inner.limits)
            .map_or(ApplyClass::Full, |(_, class)| class);
        let identity = RequestIdentity::of(&request, class);
        let id = ops.next_id();
        let stage = request.op().first_stage();
        if let Some(stage) = stage {
            ops.slots.insert(
                id,
                Slot {
                    stage: SlotStage::Waiting { stage, request },
                    identity: identity.clone(),
                    class,
                    replaced_by: BTreeSet::new(),
                    committed_at_admission: false,
                    receipt: None,
                    caller,
                },
            );
        }
        ops.publish(Instant::now(), kinds, |_| FactKind::Submitted {
            id,
            request: identity,
            stage,
        });
        id
    }

    /// Submits a range read with its caller's waker. Returns `None`,
    /// dropping the read, after shutdown or outside the bound run.
    pub(super) fn submit_read(&self, request: BlockRequest, caller: Waker) -> Option<OpId> {
        let mut ops = self.ops();
        if ops.shut_down || ops.refuse_outside_run() {
            drop(ops);
            // Dropped outside the lock, with what it owns.
            drop((request, caller));
            return None;
        }
        Some(self.insert(&mut ops, request, Some(caller), Vec::new()))
    }

    /// Locks the bookkeeping. Every change to it completes under the lock,
    /// so a panic elsewhere can't leave it half changed.
    fn ops(&self) -> MutexGuard<'_, Ops> {
        self.inner
            .ops
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Which of the gated hashes full state holds.
    fn committed(&self, hashes: Vec<block::Hash>) -> BTreeSet<[u8; 32]> {
        self.inner.engine.with_full_state(|chain| {
            hashes
                .into_iter()
                .filter(|hash| chain.contains(*hash))
                .map(|hash| hash.0)
                .collect()
        })
    }

    /// Every operation the run can complete, in ID order, with the outcomes
    /// legal now.
    pub fn pending(&self) -> Vec<PendingOp> {
        let hashes = self.ops().gated_hashes();
        let committed = self.committed(hashes);
        let ops = self.ops();
        let gates = ops.gates(&self.inner.limits);
        ops.slots
            .keys()
            .filter_map(|id| ops.pending(*id, &gates, &committed))
            .collect()
    }

    /// Operation `id` with the outcomes legal now, or `None` if the run
    /// can't complete it.
    pub fn pending_op(&self, id: OpId) -> Option<PendingOp> {
        let hashes = self.ops().gated_hashes();
        let committed = self.committed(hashes);
        let ops = self.ops();
        ops.pending(id, &ops.gates(&self.inner.limits), &committed)
    }

    /// Completes pending operation `id` with `outcome`, as the module doc
    /// says. Refuses, changing nothing, if `id` isn't pending at a stage the
    /// run completes, or the contract doesn't allow `outcome` now.
    ///
    /// Only the run completes operations, outside any task: the engine's
    /// lock and the watches are scheduling points between the change and
    /// its publish, which a task could interleave.
    pub fn complete(&self, id: OpId, outcome: BlockOutcome) -> Result<Completion, CompleteError> {
        #[cfg(zakura_sim)]
        debug_assert!(
            zakura_rt::sim::executor()
                .and_then(|executor| executor.current_task())
                .is_none(),
            "only the run completes a controlled block service's operations"
        );
        let now = Instant::now();
        let hashes = self.ops().gated_hashes();
        let committed = self.committed(hashes);
        let (request, stage, op, identity, class, receipt) = {
            let mut ops = self.ops();
            if ops.refuse_outside_run() {
                return Err(CompleteError::OutsideRun);
            }
            let (stage, op) = ops.stage(id).ok_or(CompleteError::NotPending(id))?;
            let class = ops.slots[&id].class;
            let gates = ops.gates(&self.inner.limits);
            let conditions = ops.conditions(id, &gates, &committed);
            op.check(stage, class, &conditions, outcome)
                .map_err(CompleteError::Illegal)?;
            if outcome == BlockOutcome::Admit {
                let (txn, request) = self.admit(&mut ops, id, conditions.committed, now);
                drop(ops);
                return Ok(Completion {
                    txn,
                    id,
                    op: BlockOp::SubmitBlock,
                    request,
                    outcome,
                    result: ResultIdentity::Admitted,
                    delivered: false,
                    state_version: self.inner.engine.snapshot().state_version,
                });
            }
            let slot = ops.slots.get_mut(&id).expect("the stage was read above");
            let SlotStage::Waiting { request, .. } =
                mem::replace(&mut slot.stage, SlotStage::Running(stage))
            else {
                unreachable!("the stage was checked above under the same lock");
            };
            (
                request,
                stage,
                op,
                slot.identity.clone(),
                class,
                slot.receipt,
            )
        };
        let mut kinds: Vec<FactKind> = Vec::new();
        let (result, reply) =
            match self.run(id, request, stage, class, receipt, outcome, &mut kinds) {
                Ok(done) => done,
                Err((error, request)) => {
                    // Only a commit can be refused, and it changed nothing.
                    let mut ops = self.ops();
                    let slot = ops.slots.get_mut(&id).expect("a running slot stays");
                    slot.stage = SlotStage::Waiting { stage, request };
                    return Err(CompleteError::State(error));
                }
            };
        let state_version = self.inner.engine.snapshot().state_version;
        let exits = op == BlockOp::QueryNeededBlocks && matches!(reply, Reply::Handle(false));
        let mut ops = self.ops();
        let (delivered, waker, removed, unread) = match reply {
            Reply::Handle(delivered) => (delivered, None, ops.slots.remove(&id), None),
            Reply::None => (false, None, ops.slots.remove(&id), None),
            Reply::Read(read) => {
                let slot = ops.slots.get_mut(&id).expect("a running slot stays");
                match slot.caller.take() {
                    Some(waker) => {
                        slot.stage = SlotStage::Replied(read);
                        (true, Some(waker), None, None)
                    }
                    None => (false, None, ops.slots.remove(&id), Some(read)),
                }
            }
        };
        // A copy that reports a duplicate was refused, so it replaced
        // nothing.
        if outcome == BlockOutcome::Verdict(Verdict::Duplicate) {
            for slot in ops.slots.values_mut() {
                slot.replaced_by.remove(&id);
            }
        }
        let mut completion = Completion {
            txn: 0,
            id,
            op,
            request: identity,
            outcome,
            result,
            delivered,
            state_version,
        };
        ops.publish(now, kinds, |txn| {
            completion.txn = txn;
            FactKind::Completed(Box::new(completion.clone()))
        });
        let exited = exits.then(|| exit_driver(&mut ops, now));
        // The driver takes its next action once it stops awaiting this one.
        let intake = op.awaited().then(|| ops.intake.take()).flatten();
        drop(ops);
        // Dropped outside the lock. Nobody takes an unread result, so its
        // lease ends here.
        drop((removed, unread, exited));
        // Woken outside the lock, since a waker can run scheduler code.
        for waker in [waker, intake].into_iter().flatten() {
            waker.wake();
        }
        Ok(completion)
    }

    /// Admits queued submission `id`, which the verifier receives now, and
    /// publishes its transaction, marking each older admitted submission of
    /// the same hash replaced, as full state's queue replaces a queued block
    /// with a newer copy. `committed` says whether full state holds the
    /// body's hash now. Returns the transaction and the submission's
    /// identity.
    fn admit(
        &self,
        ops: &mut Ops,
        id: OpId,
        committed: bool,
        now: Instant,
    ) -> (u64, RequestIdentity) {
        let hash = ops.slots[&id].identity.submitted_hash();
        let block = ops.slots[&id].submitted_block().cloned();
        // An admitted body still at the verifier lends its receipt to
        // another delivery of the same body.
        let mut shared = None;
        for (_, other) in ops.slots.range_mut(..id) {
            if other.admitted() && other.identity.submitted_hash() == hash {
                other.replaced_by.insert(id);
                if shared.is_none() && other.submitted_block() == block.as_ref() {
                    shared = other.receipt;
                }
            }
        }
        let receipt = match shared {
            Some(receipt) => receipt,
            None => {
                ops.last_receipt = ops
                    .last_receipt
                    .checked_add(1)
                    .expect("a run admits fewer than 2^64 bodies");
                ops.last_receipt
            }
        };
        let slot = ops.slots.get_mut(&id).expect("the stage was read above");
        if let SlotStage::Waiting { stage, .. } = &mut slot.stage {
            *stage = Stage::Admitted;
        }
        slot.receipt = Some(receipt);
        slot.committed_at_admission = committed;
        let request = slot.identity.clone();
        let txn = ops.publish(now, Vec::new(), |_| FactKind::Admitted { id });
        (txn, request)
    }

    /// Runs one completion against full state and the engine, adding its
    /// effects to `kinds`. Gives the request back if full state refuses a
    /// commit.
    #[allow(clippy::too_many_arguments)]
    fn run(
        &self,
        id: OpId,
        request: BlockRequest,
        stage: Stage,
        class: ApplyClass,
        receipt: Option<u64>,
        outcome: BlockOutcome,
        kinds: &mut Vec<FactKind>,
    ) -> Result<(ResultIdentity, Reply), (StateError, BlockRequest)> {
        let engine = &self.inner.engine;
        match (request, outcome) {
            (
                BlockRequest::Action(BlockSyncAction::QueryNeededBlocks {
                    query_id,
                    from,
                    limit,
                    scope,
                    ..
                }),
                BlockOutcome::Answer | BlockOutcome::Fail,
            ) => {
                let answer = (outcome == BlockOutcome::Answer)
                    .then(|| {
                        query_needed_blocks(from, limit, |from, limit| {
                            engine.missing_block_body_metadata(from, limit)
                        })
                        .ok()
                    })
                    .flatten();
                let (event, result) = match answer {
                    Some((body_anchor, blocks)) => (
                        HandleEvent::NeededBlocks {
                            query_id,
                            scope,
                            body_anchor,
                            blocks: blocks.clone(),
                        },
                        ResultIdentity::NeededBlocks {
                            body_anchor,
                            blocks: blocks.iter().map(|meta| (meta.height, meta.hash)).collect(),
                        },
                    ),
                    None => (
                        HandleEvent::NeededBlocksFailed { query_id, scope },
                        ResultIdentity::NeededBlocksFailed,
                    ),
                };
                Ok((result, self.reply(id, event, kinds)))
            }
            (
                BlockRequest::Action(BlockSyncAction::SubmitBlock {
                    owner,
                    source,
                    token,
                    block,
                }),
                BlockOutcome::Verdict(chosen),
            ) => {
                let mut verdict = chosen;
                if chosen.commits() {
                    let receipt = receipt.expect("an admitted body has its receipt");
                    let epoch_before = engine.view().body_work_epoch;
                    match engine.commit_block(block.clone(), receipt) {
                        Ok(applied) => {
                            self.installed(&applied, epoch_before, kinds);
                            if matches!(
                                applied.result,
                                ApplyResult::Stale(_) | ApplyResult::ResourceStalled(_)
                            ) {
                                verdict = Verdict::Retryable(RetryKind::Storage);
                            }
                        }
                        // zakura-state maps the engine's refusal to
                        // `HeaderChainError` (`write.rs`), a `Storage` retry.
                        Err(StateError::Transition(_) | StateError::Install(_)) => {
                            verdict = Verdict::Retryable(RetryKind::Storage);
                        }
                        Err(error) => {
                            let request = BlockRequest::Action(BlockSyncAction::SubmitBlock {
                                owner,
                                source,
                                token,
                                block,
                            });
                            return Err((error, request));
                        }
                    }
                }
                let hash = block.hash();
                let outcome = verdict.outcome(owner, source, hash, class);
                let result = ResultIdentity::Verdict {
                    verdict,
                    result: outcome.result(),
                };
                // The driver reports a body by its coinbase height, so it
                // sends no reply for a body without one.
                let reply = match block.coinbase_height() {
                    Some(height) => self.reply(
                        id,
                        HandleEvent::ApplyFinished {
                            owner,
                            source,
                            token,
                            height,
                            hash,
                            outcome,
                        },
                        kinds,
                    ),
                    None => Reply::None,
                };
                // The body leaves the service here.
                drop(block);
                Ok((result, reply))
            }
            (BlockRequest::Action(action), BlockOutcome::Apply) => {
                Ok((self.record(action, kinds), Reply::None))
            }
            (BlockRequest::Action(_), BlockOutcome::Fail) => {
                Ok((ResultIdentity::RecordFailed, Reply::None))
            }
            (BlockRequest::ReadRange(read), BlockOutcome::Serve | BlockOutcome::Error) => {
                let lease = read.lease;
                let result = if outcome == BlockOutcome::Serve {
                    // `StateBlockRangeSource`'s cancellation check: the read
                    // claims its lease before its first lookup.
                    let mut started = false;
                    engine
                        .read_block_range(
                            read.start_height,
                            read.count,
                            read.max_body_bytes,
                            || {
                                if !started {
                                    started = lease.try_start();
                                }
                                !started || lease.is_cancelled()
                            },
                        )
                        .map_err(BoxError::from)
                } else {
                    Err(BoxError::from("the run failed this range read"))
                };
                let (identity, read) = match result {
                    Ok(blocks) => (
                        ResultIdentity::Served {
                            heights: blocks.iter().map(|(height, _)| *height).collect(),
                            bytes: blocks
                                .iter()
                                // Widening usize to u64 is lossless on supported targets.
                                .map(|(_, bytes)| bytes.len() as u64)
                                .sum(),
                        },
                        Ok(BlockRangeReadResult { blocks, lease }),
                    ),
                    // An error drops the lease with the read, as zakura-state's
                    // blocking job does.
                    Err(error) => (ResultIdentity::ReadFailed, Err(error)),
                };
                Ok((identity, Reply::Read(read)))
            }
            (request, outcome) => unreachable!(
                "the contract allowed {} for {} at {stage:?}",
                outcome.name(),
                request.op().name()
            ),
        }
    }

    /// Runs a record's transition, as the driver's record arms do: a stale
    /// `record_body_invalid` retries with the engine's version, up to
    /// [`BODY_INVALID_STALE_REFRESH_LIMIT`] times.
    fn record(&self, action: BlockSyncAction, kinds: &mut Vec<FactKind>) -> ResultIdentity {
        let retries_stale = matches!(action, BlockSyncAction::RecordBodyInvalid { .. });
        let mut request = record_request(action);
        let mut attempts = Vec::new();
        loop {
            let epoch_before = self.inner.engine.view().body_work_epoch;
            let applied = self.inner.engine.record_body_evidence(request.clone());
            let (attempt, stale_at) = match &applied {
                Ok(applied) => {
                    self.installed(applied, epoch_before, kinds);
                    let stale_at = match applied.result {
                        ApplyResult::Stale(receipt) => Some(receipt.current_version),
                        _ => None,
                    };
                    (RecordResult::of(&applied.result), stale_at)
                }
                Err(_) => (RecordResult::Refused, None),
            };
            attempts.push(attempt);
            let refreshes = attempts.len() - 1;
            match stale_at {
                Some(current)
                    if retries_stale
                        && refreshes < usize::from(BODY_INVALID_STALE_REFRESH_LIMIT) =>
                {
                    request.expected_version = current;
                }
                _ => {
                    let durable = matches!(
                        attempts.last(),
                        Some(RecordResult::Committed | RecordResult::NoChange)
                    );
                    assert!(
                        !retries_stale || durable,
                        "record_body_invalid ended {attempts:?}, where the driver fails closed, \
                         which the block contract excludes"
                    );
                    return ResultIdentity::Recorded(attempts);
                }
            }
        }
    }

    /// Adds the facts of a transition the engine installed, if it installed
    /// one: the engine's state after it, the effect and cause it reported,
    /// and the view the publisher holds now. The body-work epoch was
    /// `epoch_before` until then.
    fn installed(
        &self,
        applied: &Applied,
        epoch_before: zakura_header_chain::BodyWorkEpoch,
        kinds: &mut Vec<FactKind>,
    ) {
        let Some(installed) = &applied.installed else {
            return;
        };
        let snapshot = self.inner.engine.snapshot();
        kinds.push(FactKind::TransitionInstalled {
            state_version: snapshot.state_version,
            verified_best: snapshot.frontiers.verified_best,
            epoch_before,
            invalidates_body_work: installed.effect.invalidates_body_work(),
            cause: applied.cause,
        });
        kinds.push(FactKind::ViewPublished(ViewIdentity::of(
            &self.inner.engine.view(),
        )));
    }

    /// Sends `event` to block sync, and records whether the handle took it.
    fn reply(&self, id: OpId, event: HandleEvent, kinds: &mut Vec<FactKind>) -> Reply {
        let delivered = self.inner.replies.send(event).is_ok();
        kinds.push(if delivered {
            FactKind::ReplyDelivered { id }
        } else {
            FactKind::ReplyDropped { id }
        });
        Reply::Handle(delivered)
    }

    /// The service's actual owner state, read fresh from its containers.
    pub fn owner_state(&self) -> OwnerState {
        let ops = self.ops();
        OwnerState {
            through_seq: ops.last_seq,
            operations: ops
                .slots
                .iter()
                .map(|(id, slot)| OperationState {
                    id: *id,
                    request: slot.identity.clone(),
                    stage: match &slot.stage {
                        SlotStage::Waiting {
                            stage: Stage::Pending,
                            ..
                        }
                        | SlotStage::Running(Stage::Pending) => OperationStage::Pending {
                            caller_waiting: !matches!(slot.identity, RequestIdentity::Read { .. })
                                || slot.caller.is_some(),
                        },
                        SlotStage::Waiting {
                            stage: Stage::Queued,
                            ..
                        }
                        // Only the run's `complete` holds a running slot, so
                        // a reader sees it only from outside the run.
                        | SlotStage::Running(Stage::Queued) => OperationStage::Queued,
                        SlotStage::Waiting {
                            stage: Stage::Admitted,
                            ..
                        }
                        | SlotStage::Running(Stage::Admitted) => OperationStage::Admitted {
                            replaced: !slot.replaced_by.is_empty(),
                        },
                        SlotStage::Replied(_) => OperationStage::Replied,
                    },
                    retained: slot.held_block(),
                })
                .collect(),
            deferred: ops.published_deferred.clone(),
        }
    }

    /// Whether the service has never published a fact and holds nothing.
    pub fn is_pristine(&self) -> bool {
        let ops = self.ops();
        ops.last_seq == 0 && ops.last == 0 && ops.slots.is_empty() && ops.deferred.is_empty()
    }

    /// Binds the service to the active simulator run, as
    /// `ControlledPort::bind_to_run` does. Once bound, a step, read,
    /// completion or read call made outside that run is refused and reported
    /// by [`Self::refused_outside_run`].
    pub fn bind_to_run(&self) -> Result<(), BindError> {
        #[cfg(zakura_sim)]
        if zakura_rt::sim::executor().is_none() {
            return Err(BindError::NotInRun);
        }
        let mut ops = self.ops();
        if ops.bound {
            return Err(BindError::AlreadyBound);
        }
        if ops.last_seq != 0 || ops.last != 0 || !ops.slots.is_empty() {
            return Err(BindError::NotPristine);
        }
        ops.bound = true;
        #[cfg(zakura_sim)]
        {
            ops.run = zakura_rt::sim::run_id();
        }
        Ok(())
    }

    /// Whether something used the service outside the run it's bound to.
    /// Always `false` outside the simulator build.
    pub fn refused_outside_run(&self) -> bool {
        #[cfg(zakura_sim)]
        {
            self.ops().refused
        }
        #[cfg(not(zakura_sim))]
        {
            false
        }
    }

    /// The sequence number of the last fact published, 0 before any.
    pub fn last_seq(&self) -> u64 {
        self.ops().last_seq
    }

    /// The facts published since the last call, in order.
    pub fn take_facts(&self) -> Vec<Fact> {
        mem::take(&mut self.ops().facts)
    }

    /// Shuts the service down, as the end of a run does: drops every held
    /// operation and deferred action with what it owns, ends the intake
    /// task, and closes the view watch. No read caller is woken. A repeat
    /// does nothing.
    pub fn shut_down(&self) {
        let now = Instant::now();
        let (slots, deferred, intake) = {
            let mut ops = self.ops();
            if mem::replace(&mut ops.shut_down, true) {
                return;
            }
            let slots = mem::take(&mut ops.slots);
            let dropped = slots.keys().copied().collect();
            let deferred = mem::take(&mut ops.published_deferred);
            ops.publish(now, Vec::new(), |_| FactKind::ShutDown {
                dropped,
                deferred,
            });
            (slots, mem::take(&mut ops.deferred), ops.intake.take())
        };
        // Dropped outside the lock, since a lease's drop wakes serving.
        drop((slots, deferred));
        if let Some(intake) = intake {
            intake.wake();
        }
        self.inner.engine.close_views();
    }

    /// Takes read `id`'s reply for its caller, or records the caller's new
    /// waker while it waits. `None` if the read isn't held any more.
    pub(super) fn poll_read(
        &self,
        id: OpId,
        waker: &Waker,
    ) -> Option<Option<Result<BlockRangeReadResult, BoxError>>> {
        let mut ops = self.ops();
        if ops.refuse_outside_run() {
            return Some(None);
        }
        let slot = ops.slots.get_mut(&id)?;
        if !matches!(slot.stage, SlotStage::Replied(_)) {
            let replaced = slot.caller.replace(waker.clone());
            drop(ops);
            drop(replaced);
            return Some(None);
        }
        let Some(Slot {
            stage: SlotStage::Replied(read),
            ..
        }) = ops.slots.remove(&id)
        else {
            unreachable!("the stage was checked above under the same lock");
        };
        ops.publish(Instant::now(), Vec::new(), |_| FactKind::ReadTaken { id });
        Some(Some(read))
    }

    /// Records that read `id`'s caller dropped its call: a pending read
    /// keeps its lease until it completes, and an untaken reply is dropped.
    pub(super) fn drop_read(&self, id: OpId) {
        let mut ops = self.ops();
        if ops.refuse_outside_run() {
            return;
        }
        let Some(slot) = ops.slots.get_mut(&id) else {
            return;
        };
        if matches!(slot.stage, SlotStage::Replied(_)) {
            let removed = ops.slots.remove(&id);
            ops.publish(Instant::now(), Vec::new(), |_| FactKind::ReadDropped { id });
            drop(ops);
            drop(removed);
        } else {
            let caller = slot.caller.take();
            ops.publish(Instant::now(), Vec::new(), |_| FactKind::CallerDropped {
                id,
            });
            drop(ops);
            drop(caller);
        }
    }
}

/// Ends the driver, as it returns when a query's reply finds the handle
/// closed: drops every action operation and deferred action, keeps the
/// reads, and publishes the exit. Returns what it dropped, for the caller to
/// drop outside the lock, and leaves the intake's waker for the caller to
/// wake.
fn exit_driver(ops: &mut Ops, now: Instant) -> (Vec<Slot>, VecDeque<BlockSyncAction>) {
    ops.exited = true;
    let dropped: Vec<OpId> = ops
        .slots
        .iter()
        .filter(|(_, slot)| !matches!(slot.identity, RequestIdentity::Read { .. }))
        .map(|(id, _)| *id)
        .collect();
    let slots = dropped
        .iter()
        .filter_map(|id| ops.slots.remove(id))
        .collect();
    let deferred = mem::take(&mut ops.published_deferred);
    ops.publish(now, Vec::new(), |_| FactKind::DriverExited {
        dropped,
        deferred,
    });
    (slots, mem::take(&mut ops.deferred))
}

/// The identities of the deferred actions, oldest first.
fn deferred_identities(
    deferred: &VecDeque<BlockSyncAction>,
    limits: &ApplyLimits,
) -> Vec<RequestIdentity> {
    deferred
        .iter()
        .map(|action| {
            let class = match action {
                BlockSyncAction::SubmitBlock { block, .. } => {
                    block.coinbase_height().map_or(ApplyClass::Full, |height| {
                        ApplyClass::of(height, limits.max_checkpoint_height)
                    })
                }
                _ => ApplyClass::Full,
            };
            RequestIdentity::of_action(action, class)
        })
        .collect()
}

/// Resolves once the driver stops awaiting a query or record, with `true`,
/// or once it stops, with `false`.
struct DriverFree<'a> {
    service: &'a ControlledBlockService,
}

impl Future for DriverFree<'_> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        let mut ops = self.service.ops();
        if ops.stopped() {
            return Poll::Ready(false);
        }
        if !ops.driver_awaiting() {
            return Poll::Ready(true);
        }
        let replaced = ops.intake.replace(cx.waker().clone());
        drop(ops);
        drop(replaced);
        Poll::Pending
    }
}

/// The driver's wait on its channel: the next action, or `None` once the
/// channel closes or the driver stops.
struct NextAction<'a> {
    service: &'a ControlledBlockService,
    actions: &'a mut mpsc::Receiver<BlockSyncAction>,
}

impl Future for NextAction<'_> {
    type Output = Option<BlockSyncAction>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        {
            let mut ops = self.service.ops();
            if ops.stopped() {
                return Poll::Ready(None);
            }
            let replaced = ops.intake.replace(cx.waker().clone());
            drop(ops);
            drop(replaced);
        }
        self.actions.poll_recv(cx)
    }
}

/// Where a completion's reply went.
enum Reply {
    /// To the handle, which took it or was closed.
    Handle(bool),
    /// Nowhere.
    None,
    /// To a read's caller.
    Read(Result<BlockRangeReadResult, BoxError>),
}

/// The engine request a record carries, as the driver's record arms and
/// `HeaderChainBodyEvidenceAuthority` build it.
///
/// # Panics
///
/// If `action` isn't a record.
pub fn record_request(action: BlockSyncAction) -> TransitionRequest {
    let (expected_version, event) = match action {
        BlockSyncAction::RecordBodyUnavailable {
            expected_version,
            failure,
        } => (
            expected_version,
            TransitionEvent::BodyEvidence(BodyEvidence::Transient(failure)),
        ),
        BlockSyncAction::RecordBodyInvalid {
            expected_version,
            invalid,
        } => (
            expected_version,
            TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(invalid)),
        ),
        BlockSyncAction::RestartBodyAvailability {
            expected_version,
            discovery,
        } => (
            expected_version,
            TransitionEvent::BodySupplierDiscovered(discovery),
        ),
        BlockSyncAction::RetryBodyAvailability {
            expected_version,
            retry,
        } => (expected_version, TransitionEvent::OperatorBodyRetry(retry)),
        action => unreachable!("{} is no record", BlockOp::of(&action).name()),
    };
    TransitionRequest {
        expected_version,
        event,
    }
}
