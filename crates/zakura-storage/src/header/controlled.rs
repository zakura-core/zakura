//! A header-chain [`Port`] whose operations wait for the run to complete
//! them.
//!
//! [`ControlledPort`] wraps one [`EnginePort`]. Each asynchronous call is
//! submitted on its future's first poll, as the production adapter's state
//! request is, and then waits as a pending operation. The run lists the
//! pending operations with [`ControlledPort::pending`] and finishes one with
//! [`ControlledPort::complete`], which runs it against the engine or returns a
//! fault. The completion is the operation's linearization point, as the
//! contract in [`super`] says.
//!
//! Dropping a call's future cancels only the caller. The operation stays
//! pending with its inputs, so a locator's permit stays held, and its reply is
//! dropped when it completes. A reply nobody takes, at completion or because
//! the caller drops it unread, is abandoned: an `Acquired` reply's lease
//! ends, as zakura-state's `PendingRetainedPathAcquisition` ends it.
//!
//! Every submission, caller drop, completion, abandoned reply and shutdown
//! goes on an event queue, in order, which [`ControlledPort::take_events`]
//! drains.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    marker::PhantomData,
    mem,
    pin::Pin,
    sync::{Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
    time::Duration,
};

use sha2::{Digest, Sha256};
use zakura_chain::block;
use zakura_header_chain::{
    BodyWorkOwner, EngineSnapshot, HeaderLocator, HeaderSyncWorkOwner, SourceId,
};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, AdapterKey, ApplyHeaderTargetReply, HeaderChainFuture,
    LocatorReadPermit, Port, PortError, PrepareHeaderTarget, PrepareHeaderTargetReply,
    PreparedHeaderTarget, ReadPath, ReadPathReply, RetainedHeaderPath, SelectedChainMembership,
    VctRepairContextReply,
};
use zakura_rt::{sync::watch, time::Instant};

use super::{
    adapter::EnginePort, fixture::HeaderFixture, lease::LeaseEnd, FaultGate, FromReply, HeaderOp,
    HeaderReply, HeaderRequest, IllegalOutcome, Outcome, ADAPTER_DEADLINE,
};

/// A pending operation's ID, unique within its port. IDs count up from 1 in
/// submission order, so they are also the canonical order.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpId(pub u64);

/// A pending operation, as [`ControlledPort::pending`] lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingOp {
    /// Its ID.
    pub id: OpId,
    /// Its operation.
    pub op: HeaderOp,
    /// The outcomes the run may complete it with now, in canonical order.
    pub enabled: Vec<Outcome>,
    /// Whether its caller still waits for the reply.
    pub caller_waiting: bool,
}

/// Whose work an operation is, as far as its request says.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Attribution {
    /// A prepare's or apply's work owner.
    pub owner: Option<HeaderSyncWorkOwner>,
    /// The peer: an acquire's source, a path's holder when this port issued
    /// the path, or a prepare's source.
    pub peer: Option<SourceId>,
    /// The peer's session, where the request names one.
    pub session_id: Option<u64>,
}

/// What [`ControlledPort::complete`] did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Completion {
    /// The operation's ID.
    pub id: OpId,
    /// The operation.
    pub op: HeaderOp,
    /// Whose work it was.
    pub attribution: Attribution,
    /// A digest of its request, the same as at submission.
    pub request_digest: [u8; 32],
    /// The outcome it completed with.
    pub outcome: Outcome,
    /// Whether the reply went to a waiting caller. `false` if the caller had
    /// dropped its future, so the reply was abandoned.
    pub delivered: bool,
    /// The leases that ended in this completion, in order: those the
    /// operation swept, consumed or released, and an abandoned acquire's.
    pub lease_ends: Vec<LeaseEnd>,
    /// [`EnginePort::state_digest`] once the completion finished.
    pub state_digest: [u8; 32],
}

/// One lifecycle event, in the order [`ControlledPort::take_events`] gives
/// them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageEvent {
    /// A call's first poll submitted an operation.
    Submitted {
        /// The new operation's ID.
        id: OpId,
        /// The operation.
        op: HeaderOp,
        /// Whose work it is.
        attribution: Attribution,
        /// A digest of its request.
        request_digest: [u8; 32],
    },
    /// A caller dropped its future while its operation was pending. The
    /// operation stays pending.
    CallerDropped {
        /// The operation.
        id: OpId,
    },
    /// The run completed an operation.
    Completed(Completion),
    /// A caller dropped a delivered reply without taking it.
    ReplyDropped {
        /// The operation.
        id: OpId,
        /// The lease an abandoned `Acquired` reply ended, if any.
        lease_ends: Vec<LeaseEnd>,
    },
    /// The port shut down.
    ShutDown {
        /// The operations it dropped, pending or with an untaken reply.
        dropped: Vec<OpId>,
        /// Every lease that was still live.
        lease_ends: Vec<LeaseEnd>,
    },
}

/// Why [`ControlledPort::complete`] refused a completion. A refusal changes
/// nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompleteError {
    /// No operation with this ID is pending. It never existed, or it already
    /// completed.
    NotPending(OpId),
    /// The contract doesn't allow this outcome for the operation now.
    Illegal(IllegalOutcome),
}

impl fmt::Display for CompleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompleteError::NotPending(id) => write!(f, "no header operation {} is pending", id.0),
            CompleteError::Illegal(illegal) => illegal.fmt(f),
        }
    }
}

impl std::error::Error for CompleteError {}

/// A header-chain port whose operations complete only when the run says so.
/// The module doc describes the lifecycle.
#[derive(Debug)]
pub struct ControlledPort {
    engine: EnginePort,
    /// The run's own bookkeeping, so a std lock: it adds no scheduling points
    /// production lacks, and the run locks it from outside any task. It's
    /// never held across a `zakura_rt` call or the engine.
    ops: Mutex<Ops>,
}

/// Every operation not yet taken by its caller.
#[derive(Debug, Default)]
struct Ops {
    /// The last ID handed out.
    last: u64,
    slots: BTreeMap<OpId, Slot>,
    /// Events not yet taken.
    events: Vec<StorageEvent>,
    /// Set by shutdown, after which nothing is submitted.
    shut_down: bool,
}

/// One submitted operation.
#[derive(Debug)]
struct Slot {
    stage: Stage,
    /// The caller's waker, or `None` once the caller dropped its future.
    caller: Option<Waker>,
    attribution: Attribution,
    request_digest: [u8; 32],
}

#[derive(Debug)]
enum Stage {
    /// Waiting for the run to complete it.
    Pending {
        request: HeaderRequest,
        submitted: Instant,
    },
    /// Taken by `complete`, which is running it.
    Running,
    /// Completed, with the reply its caller hasn't polled yet.
    Replied(HeaderReply),
}

impl ControlledPort {
    /// A port over a new [`EnginePort`] that holds only `fixture`'s anchor.
    pub fn new(fixture: &HeaderFixture) -> Self {
        ControlledPort {
            engine: EnginePort::new(fixture),
            ops: Mutex::default(),
        }
    }

    /// The engine port underneath. Its own `Port` methods run at once, which
    /// suits setting up a chain before the run starts.
    pub fn engine(&self) -> &EnginePort {
        &self.engine
    }

    /// Locks the bookkeeping. Every change to it completes under the lock, so
    /// a panic elsewhere can't leave it half changed.
    fn ops(&self) -> MutexGuard<'_, Ops> {
        self.ops.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every pending operation in ID order, with the outcomes legal now.
    pub fn pending(&self) -> Vec<PendingOp> {
        let now = Instant::now();
        let key = self.engine.adapter_key();
        self.ops()
            .slots
            .iter()
            .filter_map(|(id, slot)| match &slot.stage {
                Stage::Pending { request, submitted } => Some(PendingOp {
                    id: *id,
                    op: request.op(),
                    enabled: request.enabled(key, now.saturating_duration_since(*submitted)),
                    caller_waiting: slot.caller.is_some(),
                }),
                Stage::Running | Stage::Replied(_) => None,
            })
            .collect()
    }

    /// How long until a pending operation gains an outcome, or `None` if
    /// none will without a completion. Only a timeout's gate opens later.
    pub fn next_gate(&self) -> Option<Duration> {
        let now = Instant::now();
        let key = self.engine.adapter_key();
        self.ops()
            .slots
            .values()
            .filter_map(|slot| match &slot.stage {
                Stage::Pending { request, submitted } => Some((request, *submitted)),
                Stage::Running | Stage::Replied(_) => None,
            })
            // A rejected request has only its rejection, whenever it runs.
            .filter(|(request, _)| request.rejected_by(key).is_none())
            .filter(|(request, _)| {
                request
                    .op()
                    .contract()
                    .faults
                    .iter()
                    .any(|(_, gate)| *gate == FaultGate::AfterAdapterDeadline)
            })
            .filter_map(|(_, submitted)| {
                let open =
                    ADAPTER_DEADLINE.saturating_sub(now.saturating_duration_since(submitted));
                (!open.is_zero()).then_some(open)
            })
            .min()
    }

    /// Completes pending operation `id` with `outcome`: runs it against the
    /// engine, or builds the fault or rejection reply, then drops its
    /// request, which ends anything it owned. The reply goes to the caller
    /// and wakes it, or is dropped if the caller is gone.
    ///
    /// Refuses, changing nothing, if `id` isn't pending or the contract
    /// doesn't allow `outcome` now.
    pub fn complete(&self, id: OpId, outcome: Outcome) -> Result<Completion, CompleteError> {
        let now = Instant::now();
        let key = self.engine.adapter_key();
        let (request, early) = {
            let mut ops = self.ops();
            let slot = ops
                .slots
                .get_mut(&id)
                .ok_or(CompleteError::NotPending(id))?;
            let Stage::Pending { request, submitted } = &slot.stage else {
                return Err(CompleteError::NotPending(id));
            };
            let since_submit = now.saturating_duration_since(*submitted);
            let illegal = || {
                CompleteError::Illegal(IllegalOutcome {
                    op: request.op(),
                    outcome,
                    since_submit,
                })
            };
            request
                .check(key, outcome, since_submit)
                .map_err(CompleteError::Illegal)?;
            // A fault or rejection needs no engine, so its reply is built
            // before anything is taken.
            let early = match outcome {
                Outcome::Engine => None,
                Outcome::Fault(fault) => Some(
                    request
                        .fault_reply(key, fault, since_submit)
                        .map_err(CompleteError::Illegal)?,
                ),
                Outcome::Rejected => Some(request.rejection_reply(key).ok_or_else(illegal)?),
            };
            let Stage::Pending { request, .. } = std::mem::replace(&mut slot.stage, Stage::Running)
            else {
                unreachable!("the stage was checked above under the same lock");
            };
            (request, early)
        };
        let op = request.op();
        // Runs without the bookkeeping lock, since the engine and a dropped
        // permit wake watchers and capacity waiters.
        let reply = match early {
            Some(reply) => {
                drop(request);
                reply
            }
            None => self.engine.execute(request),
        };
        let (caller, abandoned, attribution, request_digest) = {
            let mut ops = self.ops();
            let slot = ops
                .slots
                .get_mut(&id)
                .expect("a running operation keeps its slot until it replies");
            let (attribution, request_digest) = (slot.attribution, slot.request_digest);
            match slot.caller.take() {
                Some(waker) => {
                    slot.stage = Stage::Replied(reply);
                    (Some(waker), None, attribution, request_digest)
                }
                None => {
                    ops.slots.remove(&id);
                    (None, Some(reply), attribution, request_digest)
                }
            }
        };
        if let Some(reply) = &abandoned {
            self.abandon(reply);
        }
        let completion = Completion {
            id,
            op,
            attribution,
            request_digest,
            outcome,
            delivered: caller.is_some(),
            lease_ends: self.engine.take_lease_ends(),
            state_digest: self.engine.state_digest(),
        };
        self.ops()
            .events
            .push(StorageEvent::Completed(completion.clone()));
        // Woken outside the lock, since a waker can run scheduler code.
        if let Some(waker) = caller {
            waker.wake();
        }
        Ok(completion)
    }

    /// Ends the lease an `Acquired` reply carries, since nobody will take
    /// it. Other replies own nothing.
    fn abandon(&self, reply: &HeaderReply) {
        if let HeaderReply::AcquireHeaderPath(Ok(AcquirePathReply::Acquired(path))) = reply {
            self.engine.abandon(path);
        }
    }

    /// The events since the last call, in order.
    pub fn take_events(&self) -> Vec<StorageEvent> {
        mem::take(&mut self.ops().events)
    }

    /// Shuts the service down, as the end of a run does. Drops every pending
    /// operation unrun, with its inputs, and every untaken reply, then ends
    /// every lease. No caller is woken, and a call polled afterwards never
    /// completes. A repeat does nothing.
    pub fn shut_down(&self) {
        let slots = {
            let mut ops = self.ops();
            if mem::replace(&mut ops.shut_down, true) {
                return;
            }
            mem::take(&mut ops.slots)
        };
        let dropped = slots.keys().copied().collect();
        // Dropped outside the lock, since a request's permit wakes waiters.
        drop(slots);
        self.engine.shut_down_leases();
        let lease_ends = self.engine.take_lease_ends();
        self.ops().events.push(StorageEvent::ShutDown {
            dropped,
            lease_ends,
        });
    }

    /// A future that submits `request` on its first poll and resolves to its
    /// reply.
    fn submit<T: FromReply + 'static>(&self, request: HeaderRequest) -> HeaderChainFuture<'_, T> {
        Box::pin(Call {
            port: self,
            state: CallState::Unsubmitted(Some(Box::new(request))),
            reply: PhantomData,
        })
    }
}

/// One caller's view of an operation.
struct Call<'a, T> {
    port: &'a ControlledPort,
    state: CallState,
    reply: PhantomData<fn() -> T>,
}

enum CallState {
    /// Not polled yet. A call dropped here never reaches storage.
    Unsubmitted(Option<Box<HeaderRequest>>),
    /// Submitted as this operation.
    Submitted(OpId),
    /// Polled after shutdown, so never submitted. It never completes.
    Orphaned,
    /// The reply was taken.
    Done,
}

impl<T> Unpin for Call<'_, T> {}

impl<T: FromReply> Future for Call<'_, T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let port = self.port;
        let mut ops = port.ops();
        match &mut self.state {
            CallState::Unsubmitted(request) => {
                let request = request
                    .take()
                    .expect("an unsubmitted call holds its request");
                if ops.shut_down {
                    drop(ops);
                    self.state = CallState::Orphaned;
                    return Poll::Pending;
                }
                ops.last = ops
                    .last
                    .checked_add(1)
                    .expect("a run submits fewer than 2^64 operations");
                let id = OpId(ops.last);
                let key = port.engine.adapter_key();
                let (op, attribution, request_digest) = (
                    request.op(),
                    attribution(&request, key),
                    digest(&request, key),
                );
                ops.slots.insert(
                    id,
                    Slot {
                        stage: Stage::Pending {
                            request: *request,
                            submitted: Instant::now(),
                        },
                        caller: Some(cx.waker().clone()),
                        attribution,
                        request_digest,
                    },
                );
                ops.events.push(StorageEvent::Submitted {
                    id,
                    op,
                    attribution,
                    request_digest,
                });
                drop(ops);
                self.state = CallState::Submitted(id);
                Poll::Pending
            }
            CallState::Submitted(id) => {
                let id = *id;
                // Only shutdown removes a slot before its call takes the reply.
                let Some(slot) = ops.slots.get_mut(&id) else {
                    return Poll::Pending;
                };
                if matches!(slot.stage, Stage::Replied(_)) {
                    let Some(Slot {
                        stage: Stage::Replied(reply),
                        ..
                    }) = ops.slots.remove(&id)
                    else {
                        unreachable!("the stage was checked above under the same lock");
                    };
                    drop(ops);
                    self.state = CallState::Done;
                    Poll::Ready(T::from_reply(reply))
                } else {
                    slot.caller = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
            CallState::Orphaned => Poll::Pending,
            CallState::Done => panic!("a header port call was polled after it returned"),
        }
    }
}

impl<T> Drop for Call<'_, T> {
    fn drop(&mut self) {
        let CallState::Submitted(id) = self.state else {
            return;
        };
        let mut ops = self.port.ops();
        let Some(slot) = ops.slots.get_mut(&id) else {
            return;
        };
        if !matches!(slot.stage, Stage::Replied(_)) {
            // Accepted work outlives its caller.
            slot.caller = None;
            ops.events.push(StorageEvent::CallerDropped { id });
            return;
        }
        let Some(Slot {
            stage: Stage::Replied(reply),
            ..
        }) = ops.slots.remove(&id)
        else {
            unreachable!("the stage was checked above under the same lock");
        };
        drop(ops);
        self.port.abandon(&reply);
        let lease_ends = self.port.engine.take_lease_ends();
        self.port
            .ops()
            .events
            .push(StorageEvent::ReplyDropped { id, lease_ends });
    }
}

/// Whose work `request` is, for the adapter holding `key`.
fn attribution(request: &HeaderRequest, key: &AdapterKey) -> Attribution {
    let owner = request.owner();
    let (peer, session_id) = match request {
        HeaderRequest::AcquireHeaderPath(acquire) => {
            (Some(acquire.source), Some(acquire.session_id))
        }
        HeaderRequest::ReadHeaderPath { path, .. } | HeaderRequest::ReleaseHeaderPath { path } => {
            path.adapter_identity(key)
                .map_or((None, None), |(_, peer, session)| {
                    (Some(peer), Some(session))
                })
        }
        HeaderRequest::PrepareHeaderTarget(prepare) => (Some(prepare.source), None),
        _ => (None, None),
    };
    Attribution {
        owner,
        peer,
        session_id: session_id.or(owner.map(|owner| owner.session_id())),
    }
}

/// A digest of `request`. Path and target handles redact their identity
/// from `Debug`, so a path this port issued adds its lease, peer and
/// session.
fn digest(request: &HeaderRequest, key: &AdapterKey) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-storage-header-request-v1");
    hasher.update(format!("{request:?}").as_bytes());
    if let HeaderRequest::ReadHeaderPath { path, .. } | HeaderRequest::ReleaseHeaderPath { path } =
        request
    {
        hasher.update(format!("{:?}", path.adapter_identity(key)).as_bytes());
    }
    hasher.finalize().into()
}

impl Port for ControlledPort {
    fn continuation_locator(
        &self,
        permit: LocatorReadPermit,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, PortError>> {
        self.submit(HeaderRequest::ContinuationLocator { permit })
    }

    fn vct_repair_context(
        &self,
        owner: BodyWorkOwner,
        height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, PortError>> {
        self.submit(HeaderRequest::VctRepairContext { owner, height })
    }

    fn acquire_header_path(
        &self,
        request: AcquirePath,
    ) -> HeaderChainFuture<'_, Result<AcquirePathReply, PortError>> {
        self.submit(HeaderRequest::AcquireHeaderPath(request))
    }

    fn read_header_path(
        &self,
        path: RetainedHeaderPath,
        request: ReadPath,
    ) -> HeaderChainFuture<'_, Result<ReadPathReply, PortError>> {
        self.submit(HeaderRequest::ReadHeaderPath { path, request })
    }

    fn release_header_path(
        &self,
        path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>> {
        self.submit(HeaderRequest::ReleaseHeaderPath { path })
    }

    fn prepare_header_target(
        &self,
        request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply> {
        self.submit(HeaderRequest::PrepareHeaderTarget(request))
    }

    fn apply_header_target(
        &self,
        target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
        self.submit(HeaderRequest::ApplyHeaderTarget { target })
    }

    /// The engine's watch. It's synchronous, so it's never pending.
    fn selected_chain(&self) -> watch::Receiver<Option<EngineSnapshot>> {
        self.engine.selected_chain()
    }

    fn selected_chain_contains(
        &self,
        hash: block::Hash,
    ) -> HeaderChainFuture<'_, Result<SelectedChainMembership, PortError>> {
        self.submit(HeaderRequest::SelectedChainContains { hash })
    }
}
