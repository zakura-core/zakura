//! A header-sync reactor's events as run evidence.
//!
//! [`ReactorTap::new`] makes an observer for one node's reactor and a reader
//! for the run. The observer, installed on the reactor's startup, buffers
//! each event with the node's storage watermark. The reader drains the
//! buffer at each observation and logs every event as
//! [`Observation::Reactor`]. The tap registers its checkpoint with the run
//! like storage does, so the run checks its evidence the same way.
//!
//! Before it logs a drain, the reader pairs each port call's end with the
//! storage receipt it took, as [`super::pair`] describes, from the facts
//! [`PolicySource`](super::PolicySource) hands it. A tap read on its own
//! sees no storage facts, so any call that took a receipt fails to pair.
//! A call caught panicking ends the run with
//! [`HarnessFailure::ReactorPortPanicked`] once its drain is logged.
//!
//! The reactor's teardown is done once the observer is gone, since every pending
//! call's future holds a clone through its drop. Every dropped call's operation
//! must have its drop receipt by then, unless storage's shutdown removed it while
//! the call still existed, or the run fails as `UnpairedDrop`. A run that ends
//! before the teardown is done leaves that open.
//!
//! A reactor that goes away during the run fails it as `ReactorGone`, unless
//! the harness stopped it on purpose. A [`TapShutdown`] tells the tap so
//! before the stop begins. The drain that finds the observer gone is then the
//! tap's final read: it logs the reactor's last events and runs the teardown
//! checks above. A reactor that unwound still fails as `ReactorGone`, and a
//! run that ends idle or at its budget before that read fails as
//! `ShutdownIncomplete`.
//!
//! The reactor's reservations and admissions stay out of the log, like its
//! retained-header ownership, so pinned Plans keep their format. The tap hands
//! them to its [`PolicySource`](super::PolicySource), whose triggers count
//! them. Its header capacity accounting, the reservations with their
//! bindings and ends, staged pages and returned leases, stays out too: the
//! ownership check folds it, and a [`ReservationEvidence`] keeps it.
//!
//! Every event the reactor reports in the run also feeds the tap's
//! [`SessionEvidence`], the reactor's session view. A rule that view
//! breaks fails the run at the next drain, as a broken ownership rule does.
//!
//! A tap made with a [`CapacityEvidence`] also reads the reactor's resource
//! probe after each drain, and checks it against the ownership check's
//! accounting, as [`super::capacity`] describes. The reactor hands the probe
//! over as it's built, so a tap that has none at its first drain fails as
//! `ProbeMissing`, and one handed a second fails as `ProbeReplaced`. The
//! tap's checkpoint covers these reads, so the run fails unless they happen
//! at every boundary.
//!
//! Recording an event takes only std locks, which are never held across any
//! other call, so the tap adds no scheduling point to the reactor.

use std::{
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
};

use zakura_network::zakura::{
    observe::{
        CallEnd, HeaderResourceProbe, HeaderSyncObserver, HeaderTargetPhase, HeaderWorkLedger,
        PortCall, ReactorEvent, RefusalCause, RequestRef, StorageCallKind, WaitCause,
    },
    HeaderSyncMisbehavior, HeaderTargetPurpose, ZakuraPeerId,
};
use zakura_rt::sim::RunId;
use zakura_scheduler::{SimInstant, TaskId};
use zakura_storage::header::controlled::ControlledPort;

use super::{
    capacity::{self, CapacityEvidence, CapacityReading, Expected},
    pair::{PairError, Pairer},
    policy::LinkEffect,
    reservation::ReservationEvidence,
    session::SessionEvidence,
};
use crate::{
    draws::Failure,
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    observe::{ReactorCall, ReactorFact, ReactorPurpose, ReactorRequest, StorageFact},
    sim::MidPoll,
    storage::{body_owner, hash, hex, point, scope, work_owner},
    ActionSource, Boundary, HarnessFailure, Identity, Observation, ObservationLog, Sim,
};

/// The run's reader of one reactor's events. Dropping it while the run is
/// live fails the run, since events after that are never delivered.
pub struct ReactorTap {
    state: Arc<TapState>,
    checkpoint: Checkpoint,
    /// Binds it to the run that holds its checkpoint.
    id: EvidenceId,
    node: String,
    /// The last event delivered to the log, 0 before any.
    delivered: u64,
    /// Pairs calls with the node's storage receipts.
    pairer: Pairer,
    /// The run's harness failure, set when a call panics.
    failure: Failure,
    /// The tasks stopped mid-poll at the boundary being observed.
    mid_poll: MidPoll,
}

/// What the observer and the reader share.
struct TapState {
    /// The runtime run whose tasks may record events. It comes from the
    /// runtime's own counter, which is independent of the run's
    /// registration ID, so the two can differ.
    run: RunId,
    port: Weak<ControlledPort>,
    /// The observer, which only the reactor holds, so it's gone once the
    /// reactor is.
    observer: Weak<TapObserver>,
    /// Set when something recorded an event outside `run`.
    refused: AtomicBool,
    /// Set when an event was recorded after the node's port was dropped.
    port_gone: AtomicBool,
    /// Set when the reactor unwound, which leaves its calls and ownership open.
    unwound: AtomicBool,
    /// Set when the harness began stopping the reactor on purpose.
    shutdown: AtomicBool,
    buffer: Mutex<Buffer>,
    /// Checks each call's identity and retained headers as events arrive.
    work: Mutex<WorkCheck>,
    /// The reactor's session view.
    sessions: SessionEvidence,
    /// The reactor's capacity accounting facts.
    reservations: ReservationEvidence,
    /// The reactor's resource probe, once it hands it over.
    probe: Mutex<ProbeSlot>,
    /// Where the probe's reads go, if this tap reads it.
    capacity: Option<CapacityEvidence>,
    /// The task the reactor's accounting reports from, once it first does.
    accounting_task: Mutex<AccountingTask>,
}

/// Which task the reactor's accounting reports from. Only that task changes
/// the budget, so a boundary that finds it at a poll boundary is quiet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum AccountingTask {
    /// No accounting fact came from a task yet.
    #[default]
    Unknown,
    /// Every one so far came from this task.
    Only(TaskId),
    /// Facts came from two tasks, `first` and then `second`.
    Two {
        /// The task the first came from.
        first: TaskId,
        /// The other task.
        second: TaskId,
    },
}

/// The resource probe a tap was handed.
#[derive(Default)]
enum ProbeSlot {
    /// None yet.
    #[default]
    Missing,
    /// One, from the tapped reactor.
    Held(HeaderResourceProbe),
    /// More than one, so the tap can't tell which reactor it reads.
    Replaced,
}

/// The tap's ownership check, and the first rule a history broke.
#[derive(Default)]
struct WorkCheck {
    ledger: HeaderWorkLedger,
    broken: Option<String>,
}

/// Events recorded and not yet drained.
#[derive(Default)]
struct Buffer {
    /// The last event number handed out.
    last: u64,
    /// Each event with its number and storage watermark, in order.
    events: Vec<(u64, u64, ReactorEvent)>,
    /// Each reservation and admission, by its peer, in order. They stay out
    /// of the log, so they take no number.
    effects: Vec<(LinkEffect, ZakuraPeerId)>,
}

impl TapState {
    /// Locks the buffer. Every change completes under the lock, so a panic
    /// elsewhere can't leave it half changed.
    fn buffer(&self) -> std::sync::MutexGuard<'_, Buffer> {
        self.buffer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Feeds `event` to the ownership check, keeping the first broken rule.
    fn check_work(&self, event: &ReactorEvent) {
        let mut work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        if work.broken.is_none() {
            work.broken = work.ledger.observe(event).err();
        }
    }

    /// The first rule this run's history broke.
    fn broken_work_rule(&self) -> Option<String> {
        let work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        work.broken.clone()
    }

    /// Notes the task reporting an accounting fact now, if any.
    fn note_accounting_task(&self) {
        let Some(task) = zakura_scheduler::hooks::current_task() else {
            return;
        };
        let mut known = self
            .accounting_task
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *known = match *known {
            AccountingTask::Unknown => AccountingTask::Only(task),
            AccountingTask::Only(first) if first != task => AccountingTask::Two {
                first,
                second: task,
            },
            unchanged => unchanged,
        };
    }

    /// The headers the ownership check's accounting holds reserved and
    /// owned.
    fn accounted(&self) -> (usize, usize) {
        let work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        (work.ledger.reserved(), work.ledger.owned())
    }

    /// The call or retained headers that a finished reactor left open.
    fn open_work(&self) -> Result<(), String> {
        let work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        work.ledger.finish()
    }

    /// Whether the harness stopped the reactor and it ended without
    /// unwinding.
    fn stopped(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
            && self.observer.strong_count() == 0
            && !self.unwound.load(Ordering::Relaxed)
    }
}

/// Tells a tap that the harness is stopping its reactor on purpose.
#[derive(Clone)]
pub struct TapShutdown(Arc<TapState>);

impl TapShutdown {
    /// Marks the stop begun. Call it before anything the reactor reads
    /// changes.
    pub fn begin(&self) {
        self.0.shutdown.store(true, Ordering::Relaxed);
    }
}

/// A tap publishes reactor events.
impl LiveEvidence for TapState {
    /// The last event recorded.
    fn last_seq(&self) -> u64 {
        self.buffer().last
    }

    /// Whether an event was recorded outside the tap's run.
    fn refused_outside_run(&self) -> bool {
        self.refused.load(Ordering::Relaxed)
    }

    /// `PortGone` once an event found the node's port dropped, and
    /// `ReactorGone` once the observer is, unless the harness stopped the
    /// reactor and it ended without unwinding.
    fn health(&self) -> Option<&'static str> {
        if self.port_gone.load(Ordering::Relaxed) {
            Some("PortGone: the tapped node's port was dropped during the run")
        } else if self.observer.strong_count() == 0 && !self.stopped() {
            Some("ReactorGone: the tapped reactor dropped its observer during the run")
        } else {
            None
        }
    }

    /// `ShutdownIncomplete` once the harness began stopping the reactor and
    /// it still holds its observer, so its last events and the teardown
    /// checks haven't happened.
    fn unfinished(&self) -> Option<&'static str> {
        (self.shutdown.load(Ordering::Relaxed) && self.observer.strong_count() > 0).then_some(
            "ShutdownIncomplete: the run ended before the stopped reactor dropped its observer",
        )
    }
}

/// The observer the reactor holds.
struct TapObserver(Arc<TapState>);

impl std::fmt::Debug for TapObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TapObserver")
    }
}

impl Drop for TapObserver {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.unwound.store(true, Ordering::Relaxed);
        }
    }
}

impl HeaderSyncObserver for TapObserver {
    /// [`TapState::record`].
    fn observe(&self, event: ReactorEvent) {
        self.0.record(event);
    }

    /// A [`TapAccounting`] handle, so a reservation or lease that outlives
    /// the reactor doesn't keep the tap reading it as alive. The teardown
    /// check then finds it open.
    fn capacity_observer(&self) -> Option<Arc<dyn HeaderSyncObserver>> {
        Some(Arc::new(TapAccounting(self.0.clone())))
    }

    /// Keeps the reactor's probe, or marks the slot replaced if it holds
    /// one already. A probe handed over outside the tap's run is refused.
    fn resource_probe(&self, probe: HeaderResourceProbe) {
        if zakura_rt::sim::run_id() != Some(self.0.run) {
            self.0.refused.store(true, Ordering::Relaxed);
            return;
        }
        let mut slot = self.0.probe.lock().unwrap_or_else(PoisonError::into_inner);
        *slot = match mem::take(&mut *slot) {
            ProbeSlot::Missing => ProbeSlot::Held(probe),
            ProbeSlot::Held(_) | ProbeSlot::Replaced => ProbeSlot::Replaced,
        };
    }
}

/// The handle the reactor's work queue reports its accounting through. It
/// shares the tap's state but isn't the observer whose lifetime marks the
/// reactor's.
struct TapAccounting(Arc<TapState>);

impl std::fmt::Debug for TapAccounting {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TapAccounting")
    }
}

impl HeaderSyncObserver for TapAccounting {
    /// [`TapState::record`].
    fn observe(&self, event: ReactorEvent) {
        self.0.record(event);
    }
}

impl TapState {
    /// Buffers `event` with the port's last fact, or refuses it outside the
    /// tap's run. Neither step yields.
    fn record(self: &Arc<Self>, event: ReactorEvent) {
        let state = self;
        let in_run = zakura_rt::sim::run_id() == Some(state.run);
        // The session view sees every event and picks what its rules read,
        // so no second list here can drift from them.
        if in_run {
            state.sessions.reactor(&event);
        }
        // Ownership, accounting, reservation and admission events stay out
        // of the observation log, whose format pinned Plans fix. The run's
        // own shutdown drops a live reactor's queue and calls after the log
        // froze, which ends their reservations, leases and ownership.
        let accounted = ReservationEvidence::keeps(&event);
        if in_run && accounted {
            state.reservations.record(&event);
            state.note_accounting_task();
        }
        if accounted
            || matches!(
                event,
                ReactorEvent::HeadersHeld { .. } | ReactorEvent::HeadersEnded { .. }
            )
        {
            if in_run {
                state.check_work(&event);
            }
            if !matches!(event, ReactorEvent::ReservationOpened { .. }) {
                return;
            }
        }
        let effect = match &event {
            ReactorEvent::ReservationOpened { peer, .. } => {
                Some((LinkEffect::ReservationOpened, peer.clone()))
            }
            ReactorEvent::SessionAdmitted { peer, .. } => {
                Some((LinkEffect::SessionAdmitted, peer.clone()))
            }
            _ => None,
        };
        if let Some(effect) = effect {
            // Only a live reactor reserves or admits, so neither happens in
            // the run's own shutdown.
            if in_run {
                state.buffer().effects.push(effect);
            } else {
                state.refused.store(true, Ordering::Relaxed);
            }
            return;
        }
        // Only a live reactor handles a disconnect, so this doesn't happen
        // in the run's own shutdown either.
        if matches!(event, ReactorEvent::SessionEnded { .. }) {
            if !in_run {
                state.refused.store(true, Ordering::Relaxed);
            }
            return;
        }
        // The run's own shutdown drops a live reactor after the log froze.
        if !in_run
            && matches!(
                event,
                ReactorEvent::CallEnded {
                    end: CallEnd::Dropped,
                    ..
                }
            )
        {
            return;
        }
        if !in_run {
            state.refused.store(true, Ordering::Relaxed);
            return;
        }
        state.check_work(&event);
        // The port's own std lock, which the reactor never holds here.
        let storage_seq = match state.port.upgrade() {
            Some(port) => port.last_seq(),
            None => {
                state.port_gone.store(true, Ordering::Relaxed);
                0
            }
        };
        let mut buffer = state.buffer();
        buffer.last = buffer
            .last
            .checked_add(1)
            .expect("a run records fewer than 2^64 events");
        let seq = buffer.last;
        buffer.events.push((seq, storage_seq, event));
    }
}

impl ReactorTap {
    /// A tap on the reactor of the node named `node`, whose storage is
    /// `port`. Returns the observer, for the reactor's startup and nothing
    /// else, and the reader. Its checkpoint is registered with `sim` now, so
    /// the run fails unless the reader observes every boundary, however
    /// it's later wrapped or added.
    ///
    /// # Panics
    ///
    /// Outside a run, which can't happen while `sim` exists.
    pub fn new(
        sim: &mut Sim,
        port: &Arc<ControlledPort>,
        node: &str,
    ) -> (Arc<dyn HeaderSyncObserver>, ReactorTap) {
        ReactorTap::with_sessions(sim, port, node, SessionEvidence::default())
    }

    /// [`ReactorTap::with_sessions`], with the reactor's capacity accounting
    /// also kept in `reservations`, and its resource probe read, checked and
    /// kept in `capacity`.
    ///
    /// # Panics
    ///
    /// As [`ReactorTap::with_sessions`] does.
    pub fn with_evidence(
        sim: &mut Sim,
        port: &Arc<ControlledPort>,
        node: &str,
        sessions: SessionEvidence,
        reservations: ReservationEvidence,
        capacity: CapacityEvidence,
    ) -> (Arc<dyn HeaderSyncObserver>, ReactorTap) {
        ReactorTap::tapped(sim, port, node, sessions, reservations, Some(capacity))
    }

    /// [`ReactorTap::new`], with the reactor's session view kept in
    /// `sessions`, which this tap's run binds and freezes with its log.
    ///
    /// # Panics
    ///
    /// Outside a run, or if another tap already bound `sessions`.
    pub fn with_sessions(
        sim: &mut Sim,
        port: &Arc<ControlledPort>,
        node: &str,
        sessions: SessionEvidence,
    ) -> (Arc<dyn HeaderSyncObserver>, ReactorTap) {
        ReactorTap::tapped(
            sim,
            port,
            node,
            sessions,
            ReservationEvidence::default(),
            None,
        )
    }

    /// The tap [`ReactorTap::with_evidence`] makes.
    fn tapped(
        sim: &mut Sim,
        port: &Arc<ControlledPort>,
        node: &str,
        sessions: SessionEvidence,
        reservations: ReservationEvidence,
        capacity: Option<CapacityEvidence>,
    ) -> (Arc<dyn HeaderSyncObserver>, ReactorTap) {
        let run = zakura_rt::sim::run_id().expect("a Sim's scheduler enters a runtime run");
        sessions.bind(run);
        sim.on_frozen({
            let sessions = sessions.clone();
            move || sessions.freeze()
        });
        let observer = Arc::new_cyclic(|observer| {
            TapObserver(Arc::new(TapState {
                run,
                port: Arc::downgrade(port),
                observer: observer.clone(),
                refused: AtomicBool::new(false),
                port_gone: AtomicBool::new(false),
                unwound: AtomicBool::new(false),
                shutdown: AtomicBool::new(false),
                buffer: Mutex::default(),
                work: Mutex::default(),
                sessions,
                reservations,
                probe: Mutex::default(),
                capacity,
                accounting_task: Mutex::default(),
            }))
        });
        let state = observer.0.clone();
        let live: Weak<dyn LiveEvidence> = Arc::<TapState>::downgrade(&state);
        let checkpoint = Checkpoint::new(
            EvidenceKind::Reactor {
                node: node.to_owned(),
            },
            live,
        );
        let id = sim.register(checkpoint.clone());
        let tap = ReactorTap {
            state,
            checkpoint,
            id,
            node: node.to_owned(),
            delivered: 0,
            pairer: Pairer::default(),
            failure: sim.failure(),
            mid_poll: sim.mid_poll(),
        };
        (observer, tap)
    }

    /// Reads the node's storage facts that this step's drain logged, as
    /// `(seq, fact)` in order, so the next [`observe`](ActionSource::observe)
    /// can pair calls with them.
    pub(crate) fn storage_facts<'f>(
        &mut self,
        facts: impl IntoIterator<Item = (u64, &'f StorageFact)>,
    ) {
        self.pairer.facts(facts);
    }

    /// The notice the harness gives this tap when it stops the reactor.
    pub fn shutdown(&self) -> TapShutdown {
        TapShutdown(self.state.clone())
    }

    /// The reactor's session view, which the topology's links also report
    /// to.
    pub fn sessions(&self) -> SessionEvidence {
        self.state.sessions.clone()
    }

    /// Reads the reactor's resource probe, if this tap reads one, checks the
    /// read as [`capacity::check`] says, and keeps it as a reading of `step`.
    /// While the reactor is alive, a quiet boundary's read is compared with
    /// the ownership check's accounting. Once a reactor the harness stopped
    /// is gone, every read is a teardown read. A reactor that unwound, or
    /// went away unstopped, left its counts mid-change and fails the run on
    /// its own, so its reads aren't checked.
    fn read_capacity(&self, step: u64) -> Result<(), String> {
        let Some(evidence) = &self.state.capacity else {
            return Ok(());
        };
        let probe = match &*self
            .state
            .probe
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            ProbeSlot::Missing => {
                return Err(
                    "ProbeMissing: the tapped reactor never handed over its resource probe"
                        .to_owned(),
                )
            }
            ProbeSlot::Replaced => {
                return Err(
                    "ProbeReplaced: more than one reactor handed this tap a resource probe"
                        .to_owned(),
                )
            }
            ProbeSlot::Held(probe) => probe.clone(),
        };
        let gone = self.state.observer.strong_count() == 0;
        if self.state.unwound.load(Ordering::Relaxed) || (gone && !self.state.stopped()) {
            return Ok(());
        }
        let Some(read) = zakura_rt::sim::boundary_read(|| probe.read()) else {
            return Ok(());
        };
        let task = *self
            .state
            .accounting_task
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let expected = if gone {
            Expected::Teardown {
                storage_permits: self
                    .state
                    .port
                    .upgrade()
                    .map_or(0, |port| port.owner_state().permits.len()),
            }
        } else {
            let quiet = match task {
                AccountingTask::Unknown => false,
                AccountingTask::Only(task) => !self.mid_poll.contains(task),
                AccountingTask::Two { first, second } => {
                    return Err(format!(
                        "AccountingOffReactor: the reactor's capacity accounting reported \
                         from task {} and then task {}, so no boundary is known quiet",
                        first.index(),
                        second.index()
                    ))
                }
            };
            Expected::Live {
                accounted: quiet.then(|| self.state.accounted()),
            }
        };
        evidence.record(CapacityReading {
            step,
            reserved: read.reserved,
            owned: read.owned,
            locator_permits: read.locator_permits,
            compared: matches!(expected, Expected::Live { accounted: Some(_) }),
            teardown: gone,
        });
        capacity::check(&probe, read, expected)
    }

    /// Takes the reservations and admissions recorded since the last call,
    /// by peer, in order.
    pub(crate) fn take_effects(&mut self) -> Vec<(LinkEffect, ZakuraPeerId)> {
        mem::take(&mut self.state.buffer().effects)
    }
}

/// `peer` as hex, by the digest storage attributes it with.
pub(super) fn peer(peer: &ZakuraPeerId) -> String {
    hex(&peer.digest())
}

/// `request` in plain fields.
fn request(request: RequestRef) -> ReactorRequest {
    ReactorRequest {
        peer: peer(&request.peer),
        session_id: request.session_id,
        request_id: request.request_id,
        owner: work_owner(request.owner),
        target: hash(request.target_tip_hash),
    }
}

/// `purpose` in plain fields.
fn purpose(purpose: HeaderTargetPurpose) -> ReactorPurpose {
    match purpose {
        HeaderTargetPurpose::Normal => ReactorPurpose::Normal,
        HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target,
            repair_generation,
        } => ReactorPurpose::SelectedAuxiliaryRepair {
            selected_target: point(selected_target),
            repair_generation,
        },
    }
}

/// `phase`'s name.
fn phase(phase: HeaderTargetPhase) -> &'static str {
    match phase {
        HeaderTargetPhase::Receiving => "receiving",
        HeaderTargetPhase::Preparing => "preparing",
        HeaderTargetPhase::Applying => "applying",
    }
}

/// `call` in plain fields.
fn call(call: PortCall) -> ReactorCall {
    match call {
        PortCall::Locator {
            peer: offering,
            session_id,
            target_tip_hash,
            scope: authority,
        } => ReactorCall::Locator {
            peer: peer(&offering),
            session_id,
            target: hash(target_tip_hash),
            scope: scope(authority),
        },
        PortCall::VctContext { owner, height } => ReactorCall::VctContext {
            owner: body_owner(owner),
            height: height.0,
        },
        PortCall::Prepare {
            peer: supplier,
            source,
            owner,
            ..
        } => ReactorCall::Prepare {
            peer: peer(&supplier),
            source: hex(&source.digest()),
            owner: work_owner(owner),
        },
        PortCall::Apply {
            peer: supplier,
            source,
            owner,
        } => ReactorCall::Apply {
            peer: peer(&supplier),
            source: hex(&source.digest()),
            owner: work_owner(owner),
        },
    }
}

/// `kind`'s name.
fn call_kind(kind: StorageCallKind) -> &'static str {
    match kind {
        StorageCallKind::Locator => "locator",
        StorageCallKind::VctRepairContext => "vct-context",
        StorageCallKind::Prepare => "prepare",
        StorageCallKind::Apply => "apply",
    }
}

/// `cause`'s name.
fn refusal_cause(cause: RefusalCause) -> &'static str {
    match cause {
        RefusalCause::LocatorCapacity => "locator-capacity",
        RefusalCause::LocalOperationBusy => "local-operation-busy",
        RefusalCause::ExternalQueue => "external-queue",
    }
}

/// `end`'s name.
fn end(end: CallEnd) -> &'static str {
    match end {
        CallEnd::Replied => "replied",
        CallEnd::PortError => "port-error",
        CallEnd::TimedOut => "timed-out",
        CallEnd::NotSubmitted => "not-submitted",
        CallEnd::PortPanicked => "port-panicked",
        CallEnd::Dropped => "dropped",
    }
}

/// `cause`'s name.
fn cause(cause: WaitCause) -> &'static str {
    match cause {
        WaitCause::StagingFull => "staging-full",
        WaitCause::LocatorCapacity => "locator-capacity",
    }
}

/// `reason`'s name.
fn misbehavior(reason: HeaderSyncMisbehavior) -> &'static str {
    match reason {
        HeaderSyncMisbehavior::MalformedMessage => "malformed-message",
        HeaderSyncMisbehavior::InvalidHeader => "invalid-header",
    }
}

/// `event` in plain fields, or an error for an event this build can't
/// encode.
fn fact(event: ReactorEvent) -> Result<ReactorFact, String> {
    Ok(match event {
        ReactorEvent::Dispatched { call: number, op } => ReactorFact::Dispatched {
            call: number,
            port: call(op),
        },
        ReactorEvent::CallEnded { call, end: ended } => ReactorFact::CallEnded {
            call,
            end: end(ended).to_owned(),
            op: None,
        },
        ReactorEvent::Handled { call, panicked } => ReactorFact::Handled { call, panicked },
        ReactorEvent::StorageJoined { call } => ReactorFact::StorageJoined { call },
        ReactorEvent::StorageRefused { call, kind, cause } => ReactorFact::StorageRefused {
            call,
            kind: call_kind(kind).to_owned(),
            cause: refusal_cause(cause).to_owned(),
        },
        ReactorEvent::Started {
            request: started,
            purpose: why,
        } => ReactorFact::Started {
            request: request(started),
            purpose: purpose(why),
        },
        ReactorEvent::Retired {
            request: retired,
            phase: at,
            purpose: why,
            reason,
        } => ReactorFact::Retired {
            request: request(retired),
            phase: phase(at).to_owned(),
            purpose: purpose(why),
            reason: reason.label().to_owned(),
        },
        ReactorEvent::OfferWaiting {
            peer: offering,
            session_id,
            target_tip_hash,
            cause: refused,
        } => ReactorFact::OfferWaiting {
            peer: peer(&offering),
            session_id,
            target: hash(target_tip_hash),
            cause: cause(refused).to_owned(),
        },
        ReactorEvent::Misbehavior {
            peer: violator,
            reason,
            origin,
            current,
        } => ReactorFact::Misbehavior {
            peer: peer(&violator),
            reason: misbehavior(reason).to_owned(),
            origin,
            current,
        },
        // The enum is non-exhaustive, and an event the log can't hold would
        // leave the evidence silently incomplete.
        unknown => return Err(format!("UnknownEvent: the tap can't encode {unknown:?}")),
    })
}

/// A tap offers no actions. Its observation drains the observer's buffer
/// into the log.
impl ActionSource for ReactorTap {
    /// Nothing.
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    /// Nothing.
    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    /// Never called, since the tap offers nothing.
    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("a reactor tap offers no actions");
    }

    /// Pairs and logs every event recorded since the last observation, in
    /// order, and records what it delivered in the checkpoint. A drain after
    /// a broken ownership or session rule fails. A drain that
    /// doesn't pair is still logged, then fails, unless the tap is unhealthy,
    /// which the run reports instead, or records a port panic as the run's
    /// harness failure. Once the reactor's teardown is done, a dropped call still
    /// waiting for its drop receipt fails as `UnpairedDrop`, and an open call or
    /// ownership fails as the ledger names it, unless the reactor unwound.
    /// A clean drain then reads the reactor's resource probe
    /// ([`ReactorTap::read_capacity`]).
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        // Only the run holding the checkpoint checks what this delivers.
        // Run IDs depend on the process, and the failure is replayed, so the
        // text names neither.
        if log.run() != self.id.run {
            return Err("ForeignRun: reactor tap registered with another run".to_owned());
        }
        let events = mem::take(&mut self.state.buffer().events);
        let last = events.last().map_or(self.delivered, |(seq, _, _)| *seq);
        let mut sent = events
            .into_iter()
            .map(|(seq, storage_seq, event)| {
                Ok(Observation::Reactor {
                    instance: self.id.index,
                    seq,
                    storage_seq,
                    node: self.node.clone(),
                    event: fact(event)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let paired = self
            .pairer
            .pair(sent.iter_mut().filter_map(|obs| match obs {
                Observation::Reactor {
                    storage_seq, event, ..
                } => Some((*storage_seq, event)),
                _ => None,
            }));
        for obs in &sent {
            log.push(obs.clone());
        }
        self.delivered = last;
        // The run checks its own log against this, since the log this tap
        // was handed may not be the run's.
        self.checkpoint.record(log.step(), &sent, last);
        if let Some(rule) = self
            .state
            .broken_work_rule()
            .or_else(|| self.state.sessions.broken())
        {
            return Err(rule);
        }
        match paired {
            Ok(()) => match self.pairer.waiting_drop() {
                Some((call, op)) if self.state.observer.strong_count() == 0 => Err(format!(
                    "UnpairedDrop: the reactor's teardown is done with call {call} dropped and \
                     its operation {op} given no drop receipt, and no shutdown removed it while \
                     the call still existed"
                )),
                _ if self.state.observer.strong_count() == 0
                    && !self.state.unwound.load(Ordering::Relaxed) =>
                {
                    self.state.open_work()
                }
                _ => Ok(()),
            }
            .and_then(|()| self.read_capacity(log.step())),
            // A reactor that unwound, from a callback or observer panic,
            // leaves its last receipts with no call end. The run's evidence
            // check then reports the tap's own health, `ReactorGone` or
            // `PortGone`, so the crash isn't read as a pairing bug.
            Err(PairError::Evidence(_)) if self.state.health().is_some() => Ok(()),
            Err(PairError::Evidence(detail)) => Err(detail),
            Err(PairError::PortPanicked { call }) => {
                // The run checks this after the step, ahead of any evidence
                // failure the same step reports.
                self.failure
                    .borrow_mut()
                    .get_or_insert(HarnessFailure::ReactorPortPanicked {
                        source: self.checkpoint.kind().source(self.id.index),
                        call,
                    });
                Ok(())
            }
        }
    }
}

impl Drop for ReactorTap {
    /// Fails a run that's still live, since later events go undelivered.
    fn drop(&mut self) {
        self.checkpoint.dropped();
    }
}
