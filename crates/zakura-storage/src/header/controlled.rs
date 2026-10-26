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
//! dropped when it completes.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
    time::Duration,
};

use zakura_chain::block;
use zakura_header_chain::{BodyWorkOwner, EngineSnapshot, HeaderLocator};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, ApplyHeaderTargetReply, HeaderChainFuture, LocatorReadPermit,
    Port, PortError, PrepareHeaderTarget, PrepareHeaderTargetReply, PreparedHeaderTarget, ReadPath,
    ReadPathReply, RetainedHeaderPath, SelectedChainMembership, VctRepairContextReply,
};
use zakura_rt::{sync::watch, time::Instant};

use super::{
    adapter::EnginePort, fixture::HeaderFixture, FaultGate, FromReply, HeaderOp, HeaderReply,
    HeaderRequest, IllegalOutcome, Outcome, ADAPTER_DEADLINE,
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

/// What [`ControlledPort::complete`] did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Completion {
    /// The operation's ID.
    pub id: OpId,
    /// The operation.
    pub op: HeaderOp,
    /// The outcome it completed with.
    pub outcome: Outcome,
    /// Whether the reply went to a waiting caller. `false` if the caller had
    /// dropped its future, so the reply was dropped.
    pub delivered: bool,
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
    /// production lacks, and the run locks it from outside any task. So it's
    /// never held across a scheduling point, or a task suspended there would
    /// block the run's only thread: no `zakura_rt` call or engine call runs
    /// under it, and a removed slot or replaced waker drops only after it's
    /// released, since a reply can hold the last sender of a capacity signal,
    /// whose drop is a scheduling point.
    ops: Mutex<Ops>,
}

/// Every operation not yet taken by its caller.
#[derive(Debug, Default)]
struct Ops {
    /// The last ID handed out.
    last: u64,
    slots: BTreeMap<OpId, Slot>,
}

/// One submitted operation.
#[derive(Debug)]
struct Slot {
    stage: Stage,
    /// The caller's waker, or `None` once the caller dropped its future.
    caller: Option<Waker>,
    /// Whether the request fails an adapter-side input check. Those checks
    /// read only a handle's or a target's seal, which can't change after
    /// submission, so they run once, there, rather than on every listing.
    rejected: bool,
}

impl Slot {
    /// The outcomes legal `since_submit` after submission, in canonical
    /// order, as [`HeaderRequest::enabled`] gives them.
    fn enabled(&self, request: &HeaderRequest, since_submit: Duration) -> Vec<Outcome> {
        if self.rejected {
            vec![Outcome::Rejected]
        } else {
            request.op().enabled(since_submit).collect()
        }
    }
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
        self.ops()
            .slots
            .iter()
            .filter_map(|(id, slot)| match &slot.stage {
                Stage::Pending { request, submitted } => Some(PendingOp {
                    id: *id,
                    op: request.op(),
                    enabled: slot.enabled(request, now.saturating_duration_since(*submitted)),
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
        self.ops()
            .slots
            .values()
            .filter_map(|slot| match &slot.stage {
                // A rejected request has only its rejection, whenever it runs.
                Stage::Pending { request, submitted } if !slot.rejected => {
                    Some((request, *submitted))
                }
                Stage::Pending { .. } | Stage::Running | Stage::Replied(_) => None,
            })
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
            if !slot.enabled(request, since_submit).contains(&outcome) {
                return Err(illegal());
            }
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
        let (caller, removed) = {
            let mut ops = self.ops();
            let slot = ops
                .slots
                .get_mut(&id)
                .expect("a running operation keeps its slot until it replies");
            match slot.caller.take() {
                Some(waker) => {
                    slot.stage = Stage::Replied(reply);
                    (Some(waker), None)
                }
                None => (None, Some((ops.slots.remove(&id), reply))),
            }
        };
        drop(removed);
        let delivered = caller.is_some();
        // Woken outside the lock, since a waker can run scheduler code.
        if let Some(waker) = caller {
            waker.wake();
        }
        Ok(Completion {
            id,
            op,
            outcome,
            delivered,
        })
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
                ops.last = ops
                    .last
                    .checked_add(1)
                    .expect("a run submits fewer than 2^64 operations");
                let id = OpId(ops.last);
                let rejected = request.rejected_by(port.engine.adapter_key()).is_some();
                ops.slots.insert(
                    id,
                    Slot {
                        stage: Stage::Pending {
                            request: *request,
                            submitted: Instant::now(),
                        },
                        caller: Some(cx.waker().clone()),
                        rejected,
                    },
                );
                drop(ops);
                self.state = CallState::Submitted(id);
                Poll::Pending
            }
            CallState::Submitted(id) => {
                let id = *id;
                let slot = ops
                    .slots
                    .get_mut(&id)
                    .expect("a submitted call's slot lives until the call takes its reply");
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
                    let replaced = slot.caller.replace(cx.waker().clone());
                    drop(ops);
                    drop(replaced);
                    Poll::Pending
                }
            }
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
        let (removed, caller) = if matches!(slot.stage, Stage::Replied(_)) {
            (ops.slots.remove(&id), None)
        } else {
            // Accepted work outlives its caller.
            (None, slot.caller.take())
        };
        drop(ops);
        drop((removed, caller));
    }
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
