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
use zakura_header_chain::{BodyWorkOwner, EngineSnapshot, HeaderLocator};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, AdapterKey, ApplyHeaderTargetReply, HeaderChainFuture,
    LocatorReadPermit, Port, PortError, PrepareHeaderTarget, PrepareHeaderTargetReply,
    PreparedHeaderTarget, ReadPath, ReadPathReply, RetainedHeaderPath, SelectedChainMembership,
    VctRepairContextReply,
};
use zakura_rt::{sync::watch, time::Instant};

use super::{
    adapter::{EnginePort, Executed},
    evidence::{RequestIdentity, ResultIdentity},
    fixture::HeaderFixture,
    lease::{Effects, LeaseEnd},
    FaultGate, FromReply, HeaderOp, HeaderReply, HeaderRequest, IllegalOutcome, Outcome,
    ADAPTER_DEADLINE,
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
    /// How long ago it was submitted.
    pub since_submit: Duration,
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
    /// What it asked for, with its owner.
    pub request: RequestIdentity,
    /// What it returned.
    pub result: ResultIdentity,
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
        /// What it asks for, with its owner.
        request: RequestIdentity,
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
    /// production lacks, and the run locks it from outside any task. So it's
    /// never held across a scheduling point, or a task suspended there would
    /// block the run's only thread: no `zakura_rt` call or engine call runs
    /// under it, and a removed slot or replaced waker drops only after it's
    /// released, since a reply can hold the last sender of a capacity signal,
    /// whose drop is a scheduling point. It may be taken under the engine's
    /// lease lock, to record an event with the change it records, but never
    /// the other way round.
    ops: Mutex<Ops>,
}

/// Every operation not yet taken by its caller.
#[derive(Debug, Default)]
struct Ops {
    /// The last ID handed out.
    last: u64,
    slots: BTreeMap<OpId, Slot>,
    /// Storage's clone of each pending locator's permit. It leaves here when
    /// the operation is taken to run, or at shutdown, and drops after the
    /// lock is released, since the last permit's drop is a scheduling point.
    permits: BTreeMap<OpId, LocatorReadPermit>,
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
    identity: RequestIdentity,
    request_digest: [u8; 32],
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
    Replied { reply: HeaderReply },
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
    /// suits setting up a chain before the run starts. A task in the run
    /// must not call them: an apply can suspend under the engine lock, which
    /// the run takes to complete operations.
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
                Stage::Pending { request, submitted } => {
                    let since_submit = now.saturating_duration_since(*submitted);
                    Some(PendingOp {
                        id: *id,
                        op: request.op(),
                        enabled: slot.enabled(request, since_submit),
                        since_submit,
                        caller_waiting: slot.caller.is_some(),
                    })
                }
                Stage::Running | Stage::Replied { .. } => None,
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
                Stage::Pending { .. } | Stage::Running | Stage::Replied { .. } => None,
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
        let (request, early, permit) = {
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
            (request, early, ops.permits.remove(&id))
        };
        let op = request.op();
        // Runs without the bookkeeping lock, since the engine and a dropped
        // permit wake watchers and capacity waiters.
        let (reply, effects, result) = match early {
            Some(reply) => {
                drop(request);
                let result = match outcome {
                    Outcome::Fault(fault) => ResultIdentity::Fault(fault),
                    Outcome::Rejected | Outcome::Engine => ResultIdentity::Rejected,
                };
                (reply, Effects::default(), result)
            }
            None => {
                let Executed {
                    reply,
                    effects,
                    result,
                } = self.engine.execute(request);
                (reply, effects, result)
            }
        };
        // The permit is held until the read finishes.
        drop(permit);
        let mut lease_ends = effects.fire();
        let (caller, abandoned, identity, request_digest) = {
            let mut ops = self.ops();
            let slot = ops
                .slots
                .get_mut(&id)
                .expect("a running operation keeps its slot until it replies");
            let (identity, request_digest) = (slot.identity.clone(), slot.request_digest);
            match slot.caller.take() {
                Some(waker) => {
                    slot.stage = Stage::Replied { reply };
                    (Some(waker), None, identity, request_digest)
                }
                None => {
                    let removed = ops.slots.remove(&id);
                    (None, Some((removed, reply)), identity, request_digest)
                }
            }
        };
        if let Some((_, reply)) = &abandoned {
            // The completion's own record carries the ends.
            lease_ends.extend(self.abandon(reply, |_| {}).fire());
        }
        drop(abandoned);
        let completion = Completion {
            id,
            op,
            request: identity,
            result,
            request_digest,
            outcome,
            delivered: caller.is_some(),
            lease_ends,
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
    /// it, and returns the effects to fire once no lock is held. Other
    /// replies own nothing. Calls `record` with the lease ends as the lease
    /// ends, as [`EnginePort::abandon`] does.
    fn abandon(&self, reply: &HeaderReply, record: impl FnOnce(&[LeaseEnd])) -> Effects {
        match reply {
            HeaderReply::AcquireHeaderPath(Ok(AcquirePathReply::Acquired(path))) => {
                self.engine.abandon(path, record)
            }
            _ => {
                record(&[]);
                Effects::default()
            }
        }
    }

    /// The events since the last call, in order.
    pub fn take_events(&self) -> Vec<StorageEvent> {
        mem::take(&mut self.ops().events)
    }

    /// Shuts the service down, as the end of a run does. Drops every pending
    /// operation unrun, with its inputs, and every untaken reply, then ends
    /// every lease and closes the selected-chain watch. No caller is woken,
    /// and a call polled afterwards never completes. A repeat does nothing.
    pub fn shut_down(&self) {
        let (slots, permits) = {
            let mut ops = self.ops();
            if mem::replace(&mut ops.shut_down, true) {
                return;
            }
            (mem::take(&mut ops.slots), mem::take(&mut ops.permits))
        };
        let dropped = slots.keys().copied().collect();
        // Dropped outside the lock, since a permit wakes waiters.
        drop((slots, permits));
        let lease_ends = self.engine.shut_down_leases().fire();
        self.engine.close_selected_chain();
        self.ops().events.push(StorageEvent::ShutDown {
            dropped,
            lease_ends,
        });
    }

    /// A future that submits `request` on its first poll and resolves to its
    /// reply.
    fn submit<T: FromReply + 'static>(
        &self,
        request: HeaderRequest,
        permit: Option<LocatorReadPermit>,
    ) -> HeaderChainFuture<'_, T> {
        Box::pin(Call {
            port: self,
            state: CallState::Unsubmitted(Some(Box::new(Submission { request, permit }))),
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

/// A call's request, and storage's clone of a locator's permit, until the
/// call's first poll submits them.
struct Submission {
    request: HeaderRequest,
    permit: Option<LocatorReadPermit>,
}

enum CallState {
    /// Not polled yet. A call dropped here never reaches storage.
    Unsubmitted(Option<Box<Submission>>),
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
            CallState::Unsubmitted(submission) => {
                let Submission { request, permit } = *submission
                    .take()
                    .expect("an unsubmitted call holds its request");
                // Inspected without the lock, since digesting an apply walks
                // its whole insertion.
                drop(ops);
                let key = port.engine.adapter_key();
                let Inspected {
                    request,
                    identity,
                    request_digest,
                    rejected,
                } = inspect(request, key);
                let mut ops = port.ops();
                if ops.shut_down {
                    drop(ops);
                    // Dropped outside the lock, with its permit.
                    drop((request, permit));
                    self.state = CallState::Orphaned;
                    return Poll::Pending;
                }
                ops.last = ops
                    .last
                    .checked_add(1)
                    .expect("a run submits fewer than 2^64 operations");
                let id = OpId(ops.last);
                ops.slots.insert(
                    id,
                    Slot {
                        stage: Stage::Pending {
                            request,
                            submitted: Instant::now(),
                        },
                        caller: Some(cx.waker().clone()),
                        identity: identity.clone(),
                        request_digest,
                        rejected,
                    },
                );
                if let Some(permit) = permit {
                    ops.permits.insert(id, permit);
                }
                ops.events.push(StorageEvent::Submitted {
                    id,
                    request: identity,
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
                if matches!(slot.stage, Stage::Replied { .. }) {
                    let Some(Slot {
                        stage: Stage::Replied { reply, .. },
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
        if !matches!(slot.stage, Stage::Replied { .. }) {
            // Accepted work outlives its caller.
            let caller = slot.caller.take();
            ops.events.push(StorageEvent::CallerDropped { id });
            drop(ops);
            drop(caller);
            return;
        }
        let Some(Slot {
            stage: Stage::Replied { reply, .. },
            ..
        }) = ops.slots.remove(&id)
        else {
            unreachable!("the stage was checked above under the same lock");
        };
        drop(ops);
        let effects = self.port.abandon(&reply, |lease_ends| {
            self.port.ops().events.push(StorageEvent::ReplyDropped {
                id,
                lease_ends: lease_ends.to_vec(),
            });
        });
        effects.fire();
    }
}

/// What [`inspect`] learned about a request, with the request itself.
struct Inspected {
    request: HeaderRequest,
    identity: RequestIdentity,
    request_digest: [u8; 32],
    /// Whether it fails an adapter-side input check.
    rejected: bool,
}

/// Identifies and digests `request` and runs its adapter-side input
/// checks, for the adapter holding `key`, and gives the request back. It
/// takes the request by value so it can open a sealed target and seal it
/// again, rather than clone it.
///
/// Path and target handles redact their identity and contents from
/// `Debug`. So a path this port issued adds its lease, peer and session,
/// and a target this port sealed adds its insertion: supplier, parent,
/// target, completion, each header's hash and each auxiliary delivery.
fn inspect(request: HeaderRequest, key: &AdapterKey) -> Inspected {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-storage-header-request-v2");
    hasher.update(format!("{request:?}").as_bytes());
    let (request, rejected, apply_parent) = match request {
        HeaderRequest::ApplyHeaderTarget { target } => match target.into_insert(key) {
            Ok(insert) => {
                let parent = insert.parent_hash;
                hasher.update(insert.source.digest());
                hasher.update(insert.parent_hash.0);
                hasher.update(insert.target_tip_hash.0);
                hasher.update(format!("{:?}", insert.completion).as_bytes());
                for header in insert.batch.headers() {
                    hasher.update(header.hash.0);
                }
                for aux in &insert.aux {
                    hasher.update(format!("{aux:?}").as_bytes());
                }
                let target = PreparedHeaderTarget::from_insert(key, insert);
                (
                    HeaderRequest::ApplyHeaderTarget { target },
                    false,
                    Some(parent),
                )
            }
            Err(target) => (HeaderRequest::ApplyHeaderTarget { target }, true, None),
        },
        request => {
            if let HeaderRequest::ReadHeaderPath { path, .. }
            | HeaderRequest::ReleaseHeaderPath { path } = &request
            {
                hasher.update(format!("{:?}", path.adapter_identity(key)).as_bytes());
            }
            // No other request's input checks open a sealed target.
            let rejected = request.rejected_by(key).is_some();
            (request, rejected, None)
        }
    };
    Inspected {
        identity: RequestIdentity::of(&request, key, apply_parent),
        request,
        request_digest: hasher.finalize().into(),
        rejected,
    }
}

impl Port for ControlledPort {
    fn continuation_locator(
        &self,
        permit: LocatorReadPermit,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, PortError>> {
        self.submit(HeaderRequest::ContinuationLocator, Some(permit))
    }

    fn vct_repair_context(
        &self,
        owner: BodyWorkOwner,
        height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, PortError>> {
        self.submit(HeaderRequest::VctRepairContext { owner, height }, None)
    }

    fn acquire_header_path(
        &self,
        request: AcquirePath,
    ) -> HeaderChainFuture<'_, Result<AcquirePathReply, PortError>> {
        self.submit(HeaderRequest::AcquireHeaderPath(request), None)
    }

    fn read_header_path(
        &self,
        path: RetainedHeaderPath,
        request: ReadPath,
    ) -> HeaderChainFuture<'_, Result<ReadPathReply, PortError>> {
        self.submit(HeaderRequest::ReadHeaderPath { path, request }, None)
    }

    fn release_header_path(
        &self,
        path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>> {
        self.submit(HeaderRequest::ReleaseHeaderPath { path }, None)
    }

    fn prepare_header_target(
        &self,
        request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply> {
        self.submit(HeaderRequest::PrepareHeaderTarget(request), None)
    }

    fn apply_header_target(
        &self,
        target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
        self.submit(HeaderRequest::ApplyHeaderTarget { target }, None)
    }

    /// The engine's watch. It's synchronous, so it's never pending.
    fn selected_chain(&self) -> watch::Receiver<Option<EngineSnapshot>> {
        self.engine.selected_chain()
    }

    fn selected_chain_contains(
        &self,
        hash: block::Hash,
    ) -> HeaderChainFuture<'_, Result<SelectedChainMembership, PortError>> {
        self.submit(HeaderRequest::SelectedChainContains { hash }, None)
    }
}
