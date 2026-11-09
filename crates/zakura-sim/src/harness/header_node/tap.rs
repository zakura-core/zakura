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
//! The reactor's teardown is done once its task, which its resource probe
//! names, has ended, since every pending call's future ends with it. A tap no
//! reactor handed a probe goes by its observer instead, which only the
//! reactor holds. Every dropped call's operation must have its drop receipt
//! by then, unless storage's shutdown removed it while the call still
//! existed, or the run fails as `UnpairedDrop`. A run that ends before the
//! teardown is done leaves that open.
//!
//! A reactor that unwinds during the run fails it as `ReactorGone`. One that
//! returns by itself, without unwinding, ended its own work: the drain that
//! finds it gone records [`EndFact::ReactorExited`] in the tap's
//! [`EndEvidence`], runs the open-work checks above, and the run goes on with
//! the evidence it has. The harness can also stop the reactor on purpose. A
//! [`TapShutdown`] tells the tap so before the stop begins. The drain that
//! finds the reactor gone is then the tap's final read: it logs the
//! reactor's last events and runs the teardown checks above, and the node
//! has settled unless they fail. A reactor that unwound still fails as
//! `ReactorGone`. A run that ends idle, or at its
//! budget with a drain that ran dry, before that read fails as
//! `ShutdownIncomplete`. One whose drain was cut off at its bound leaves the
//! stop honestly unfinished, which `header-sync/teardown-returns-baseline`
//! reports as inconclusive.
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
//! The reactor's adopted state and what it publishes from it stay out of the
//! log too. The tap keeps them, with the generations of its `Started` and
//! locator `Dispatched` events, in an [`AdoptionEvidence`] that the harness's
//! view reads.
//!
//! So do the readmission cooldowns the reactor starts and the sessions it
//! refuses. The tap keeps them, with the time of each and of each admission,
//! in a [`CooldownEvidence`] that the harness's view reads.
//!
//! A broken claim rule, a rule a production bug alone can break and a
//! property's claim names ([`super::claim`]), doesn't fail the run at once.
//! The tap records it in its [`ClaimEvidence`] at the drain that found it,
//! for the owning property to report at that step. A break no property
//! reported fails the run at the next drain, or at the run's end, as any
//! other rule does, so a harness without that property never drops it.
//!
//! A tap made with a [`CapacityEvidence`] also reads the reactor's resource
//! probe after each drain, and checks it against the ownership check's
//! accounting, as [`super::capacity`] describes. The reactor hands the probe
//! over as it's built, so a tap that has none at its first drain fails as
//! `ProbeMissing`, and one handed a second fails as `ProbeReplaced`. The
//! tap's checkpoint covers these reads, so the run fails unless they happen
//! at every boundary.
//!
//! Every view-side fact the tap keeps, in any of its stores, takes the next
//! of the node's [`Stamps`], as [`super::stamp`] describes.
//!
//! Recording an event takes only std locks, which are never held across any
//! other call, so the tap adds no scheduling point to the reactor.

use std::{
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
    time::Duration,
};

use zakura_chain::block;
use zakura_network::zakura::{
    observe::{
        CallEnd, ClaimBreak, HeaderResourceProbe, HeaderSyncObserver, HeaderTargetPhase,
        HeaderWorkLedger, PortCall, ReactorEvent, RefusalCause, RequestRef, StorageCallKind,
        WaitCause,
    },
    HeaderSyncMisbehavior, HeaderTargetPurpose, ZakuraPeerId,
};
use zakura_rt::sim::RunId;
use zakura_scheduler::{SimInstant, TaskExit, TaskId};
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

use super::{
    adoption::{AdoptionEvidence, AdoptionFact},
    authority::{Authority, AuthorityEvidence},
    capacity::{self, BoundaryRead, Broken, CapacityEvidence, CapacityReading, Expected},
    chain_effect::{ChainEffect, ChainEffectEvidence, EngineAt},
    claim::{Claim, ClaimEvidence, ClaimRecord, Settlement, SettlementEvidence},
    ends::{EndEvidence, EndFact, EnvironmentAction},
    fault::{CooldownEvidence, CooldownFact},
    pair::{PairError, Pairer},
    policy::LinkEffect,
    reservation::ReservationEvidence,
    round_life::{RoundLife, RoundLifeEvidence},
    session::SessionEvidence,
    stale::{StaleCheck, StaleEvidence},
    stamp::Stamps,
    wake::{WakeCheck, WakeEvidence},
};
use crate::{
    draws::Failure,
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    observe::{ReactorCall, ReactorFact, ReactorPurpose, ReactorRequest, StorageFact},
    sim::MidPoll,
    storage::{body_owner, hash, hex, point, scope, work_owner},
    ActionSource, Boundary, HarnessFailure, Identity, Observation, ObservationLog, Outcome, RunEnd,
    Sim,
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
    /// Set once a drain recorded the reactor's own exit.
    exited: AtomicBool,
    /// The node's stamps, which every store here takes its facts' from.
    stamps: Stamps,
    /// The harness's environment actions and the reactor's own exit.
    ends: EndEvidence,
    buffer: Mutex<Buffer>,
    /// Checks each call's identity and retained headers as events arrive.
    work: Mutex<WorkCheck>,
    /// The reactor's session view.
    sessions: SessionEvidence,
    /// The reactor's capacity accounting facts.
    reservations: ReservationEvidence,
    /// The reactor's adopted state and what it published, for the view.
    adoption: AdoptionEvidence,
    /// The reactor's resource probe, once it hands it over.
    probe: Mutex<ProbeSlot>,
    /// Where the probe's reads go, if this tap reads it.
    capacity: Option<CapacityEvidence>,
    /// The first task other than the probe's reactor task that reported
    /// accounting, if any.
    off_reactor: Mutex<Option<TaskId>>,
    /// The reactor's cooldowns, refusals and admissions, for the view.
    cooldowns: CooldownEvidence,
    /// The claim rules its drains found broken, for the view.
    claims: ClaimEvidence,
    /// An evidence failure held back because its step's drain found a
    /// claim break, so the claim's property reports first
    /// ([`TapState::fail_after_claims`]).
    deferred: Arc<Mutex<Option<String>>>,
    /// Set once a drain found the stopped reactor's teardown done.
    settled: AtomicBool,
    /// The teardown claims reads after the stopped reactor was gone found,
    /// each reported once.
    teardown_claims: Mutex<Vec<Claim>>,
    /// The released rounds each drain found, for the view.
    rounds: SettlementEvidence,
    /// Checks each stale call's handler as its facts arrive, keeping the
    /// stale calls it finds for the view.
    stale: Mutex<StaleCheck>,
    /// Checks for offers a budget return should wake, keeping its facts for
    /// the view. Inert until [`ReactorTap::watch_wakes`].
    wake: Mutex<WakeCheck>,
    /// Checks that the reactor's work follows the resets it adopts. None
    /// until [`ReactorTap::watch_rounds`].
    round_life: Mutex<Option<RoundLife>>,
    /// Checks that no stale apply changes the engine and adoption never
    /// moves back. None until [`ReactorTap::watch_chain`].
    chain_effect: Mutex<Option<ChainEffect>>,
    /// Checks rounds' owner binding and pages' paths. None until
    /// [`ReactorTap::watch_authority`].
    authority: Mutex<Option<Authority>>,
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

/// The tap's ownership check, the first integrity rule a history broke,
/// and the claim breaks not yet drained.
#[derive(Default)]
struct WorkCheck {
    ledger: HeaderWorkLedger,
    broken: Option<String>,
    /// Whether a drain followed the first integrity failure, after which
    /// the ledger folds nothing more.
    stopped: bool,
    claims: Vec<ClaimBreak>,
    /// Whether the teardown check ran.
    finished: bool,
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

    /// Feeds `event` to the ownership check, keeping the first broken
    /// integrity rule and every claim break. After an integrity failure the
    /// ledger still folds the rest of that step's events, so a claim later
    /// in the step drains ahead of the failure. It stops at the step's
    /// drain.
    fn check_work(&self, event: &ReactorEvent) {
        let mut work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        if !work.stopped {
            if let Err(rule) = work.ledger.observe(event) {
                work.broken.get_or_insert(rule);
            }
            let claims = work.ledger.take_claims();
            work.claims.extend(claims);
        }
    }

    /// Fails with the first claim break an earlier drain found that no
    /// property reported, then records the claim breaks since the last drain
    /// as the drain of `step` found them, and the released rounds the ledger
    /// holds now.
    fn drain_claims(&self, step: u64) -> Result<(), String> {
        if let Some(record) = self.claims.unreported(Some(step)) {
            return Err(record.detail);
        }
        let claims = {
            let mut work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
            work.stopped |= work.broken.is_some();
            mem::take(&mut work.claims)
        };
        for broken in claims {
            self.claims.record(ClaimRecord {
                step,
                claim: Claim::of(broken.rule),
                detail: broken.detail,
            });
        }
        let stale = self
            .stale
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(step);
        for detail in stale {
            self.claims.record(ClaimRecord {
                step,
                claim: Claim::StaleCompletionEffect,
                detail,
            });
        }
        // An integrity failure here waits until every claim break is
        // recorded, so it can't hide one.
        let mut broken = None;
        let reset = match self
            .round_life
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            Some(rounds) => rounds.drain(step).unwrap_or_else(|error| {
                broken.get_or_insert(error);
                rounds.take_breaks()
            }),
            None => Vec::new(),
        };
        for detail in reset {
            self.claims.record(ClaimRecord {
                step,
                claim: Claim::ResetInvalidatesWork,
                detail,
            });
        }
        let chain = match self
            .chain_effect
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            Some(chain) => chain.drain(step).unwrap_or_else(|error| {
                broken.get_or_insert(error);
                chain.take_breaks()
            }),
            None => Vec::new(),
        };
        for detail in chain {
            self.claims.record(ClaimRecord {
                step,
                claim: Claim::StaleChainEffect,
                detail,
            });
        }
        let mut authorized = self.sessions.take_claims();
        let joined = self.sessions.take_joined();
        if let Some(authority) = self
            .authority
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            if joined {
                authority.applies();
            }
            authorized.extend(authority.drain(step));
        }
        for detail in authorized {
            self.claims.record(ClaimRecord {
                step,
                claim: Claim::CommitOnlyAuthorized,
                detail,
            });
        }
        let work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        self.rounds.record(Settlement {
            step,
            released: work.ledger.released_rounds(),
            unsettled: work.ledger.unsettled_rounds(),
            stuck: work.ledger.stuck_rounds(),
            tainted: work.ledger.tainted_rounds(),
            shutdown: self.shutdown.load(Ordering::Relaxed),
            settled: self.settled.load(Ordering::Relaxed),
        });
        broken.map_or(Ok(()), Err)
    }

    /// Fails with `error`, an evidence failure at step `step`, unless that
    /// step's drain found a claim break. Then `error` waits, so the claim's
    /// property reports first. The next observation fails with it, and so
    /// does a run that ends without a violation.
    fn fail_after_claims(&self, step: u64, error: String) -> Result<(), String> {
        if !self.claims.found_at(step) {
            return Err(error);
        }
        self.deferred
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(error);
        Ok(())
    }

    /// The first rule this run's history broke.
    fn broken_work_rule(&self) -> Option<String> {
        let work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        work.broken.clone()
    }

    /// Notes the task reporting an accounting fact now, if it isn't the
    /// reactor task the probe names.
    fn note_accounting_task(&self) {
        let Some(task) = zakura_scheduler::hooks::current_task() else {
            return;
        };
        let Some(reactor) = self.reactor_task() else {
            return;
        };
        if task != reactor {
            self.off_reactor
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert(task);
        }
    }

    /// The node's engine's header generation now, or 0 once its port is
    /// gone.
    fn engine_generation(&self) -> u64 {
        self.port
            .upgrade()
            .map_or(0, |port| port.engine().snapshot().header_generation.get())
    }

    /// The node's engine now, or `None` once its port is gone.
    fn engine_at(&self) -> Option<EngineAt> {
        let port = self.port.upgrade()?;
        let snapshot = port.engine().snapshot();
        Some(EngineAt {
            generation: snapshot.header_generation.get(),
            anchor: hash(snapshot.frontiers.finalized.hash),
            state: hex(&port.engine().state_digest()),
        })
    }

    /// The reactor's task, as the probe it handed over names it.
    fn reactor_task(&self) -> Option<TaskId> {
        match &*self.probe.lock().unwrap_or_else(PoisonError::into_inner) {
            ProbeSlot::Held(probe) => Some(TaskId::from(probe.task())),
            ProbeSlot::Missing | ProbeSlot::Replaced => None,
        }
    }

    /// Whether the task reporting now is the reactor's. Without a probe, or
    /// outside a task, the tap can't tell, and takes it as the reactor's.
    fn on_reactor(&self) -> bool {
        match (zakura_scheduler::hooks::current_task(), self.reactor_task()) {
            (Some(task), Some(reactor)) => task == reactor,
            _ => true,
        }
    }

    /// The headers the ownership check's accounting holds reserved and
    /// owned, unless a claim tainted them or an integrity failure stopped
    /// the ledger.
    fn accounted(&self) -> Option<(usize, usize)> {
        let work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        (work.broken.is_none() && !work.ledger.accounting_tainted())
            .then(|| (work.ledger.reserved(), work.ledger.owned()))
    }

    /// The call or retained headers that a finished reactor left open.
    fn open_work(&self) -> Result<(), String> {
        let mut work = self.work.lock().unwrap_or_else(PoisonError::into_inner);
        if mem::replace(&mut work.finished, true) {
            return Ok(());
        }
        work.ledger.finish()?;
        let claims = work.ledger.take_claims();
        work.claims.extend(claims);
        Ok(())
    }

    /// Whether the harness stopped the reactor and it ended cleanly.
    fn stopped(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed) && self.ended_cleanly()
    }

    /// Whether the reactor ended without panicking or unwinding.
    fn ended_cleanly(&self) -> bool {
        matches!(
            self.reactor_exit(),
            Some(TaskExit::Completed | TaskExit::Cancelled)
        ) && !self.unwound.load(Ordering::Relaxed)
    }

    /// Whether the reactor is gone.
    fn reactor_gone(&self) -> bool {
        self.reactor_exit().is_some()
    }

    /// How the reactor ended, or `None` while it runs: its task's exit, for
    /// the task its probe names, so a resource that outlives the reactor
    /// holding an observer clone can neither keep it alive nor hide a
    /// panic. A tap no reactor handed a probe goes by its observer, which
    /// only the reactor holds, and whether it unwound.
    fn reactor_exit(&self) -> Option<TaskExit> {
        match &*self.probe.lock().unwrap_or_else(PoisonError::into_inner) {
            ProbeSlot::Held(probe) => {
                zakura_scheduler::hooks::task_exit(TaskId::from(probe.task()))
            }
            ProbeSlot::Missing | ProbeSlot::Replaced => {
                (self.observer.strong_count() == 0).then(|| {
                    if self.unwound.load(Ordering::Relaxed) {
                        TaskExit::Panicked
                    } else {
                        TaskExit::Completed
                    }
                })
            }
        }
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

    /// Records that the harness's `action` on `peer` ends `connection`, or
    /// every connection for a shutdown, as [`super::ends`] describes. Call
    /// it where the harness acts, before the action takes effect.
    pub fn environment(&self, peer: &str, connection: Option<u64>, action: EnvironmentAction) {
        self.0.ends.record(EndFact::Environment {
            peer: peer.to_owned(),
            connection,
            action,
        });
    }

    /// Whether the stop is done: it began, and the reactor dropped its
    /// observer without unwinding.
    pub fn stopped(&self) -> bool {
        self.0.stopped()
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
    /// `ReactorGone` once the reactor is gone after unwinding or panicking.
    /// A reactor that ended without either, stopped or not, isn't a tap
    /// failure.
    fn health(&self) -> Option<&'static str> {
        if self.port_gone.load(Ordering::Relaxed) {
            Some("PortGone: the tapped node's port was dropped during the run")
        } else if self.reactor_gone() && !self.ended_cleanly() {
            Some("ReactorGone: the tapped reactor unwound during the run")
        } else {
            None
        }
    }

    /// `ShutdownIncomplete` once the harness began stopping the reactor and
    /// it still holds its observer, so its last events and the teardown
    /// checks haven't happened.
    ///
    /// A budget end whose drain was cut off at its bound leaves the stop
    /// honestly unfinished, which the teardown property reports as
    /// inconclusive. Any other end means no task could run, yet the reactor
    /// never let go.
    fn unfinished(&self, end: &RunEnd) -> Option<&'static str> {
        // A cut-off stop is the teardown property's to report, if one read
        // the last drain's settlement.
        let cut_off = end.drain.as_ref().is_some_and(|drain| drain.exhausted)
            && self.rounds.read_at(end.steps);
        (!cut_off && self.shutdown.load(Ordering::Relaxed) && !self.reactor_gone())
            .then_some("ShutdownIncomplete: the run ended before the stopped reactor's task ended")
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
            let on_reactor = state.on_reactor();
            let stale_handler = {
                let mut stale = state.stale.lock().unwrap_or_else(PoisonError::into_inner);
                stale.observe(&event, on_reactor);
                stale.in_stale_handler()
            };
            if let Some(rounds) = state
                .round_life
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_mut()
            {
                rounds.observe(&event, on_reactor);
            }
            if let Some(chain) = state
                .chain_effect
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_mut()
            {
                chain.observe(&event, on_reactor);
            }
            if let Some(authority) = state
                .authority
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_mut()
            {
                authority.observe(&event, on_reactor, stale_handler);
            }
            if on_reactor {
                let now = zakura_scheduler::hooks::now().unwrap_or_default();
                state
                    .wake
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .observe(&event, now);
            }
        }
        // Budget changes, credit and built closes, causes, headroom waits,
        // deferred passes, taken events, watch reads, the queue's slots and
        // cursor claims and acceptances stay out of the log, whose format pinned Plans
        // fix, and a teardown's returns after the run are expected.
        if matches!(
            event,
            ReactorEvent::BudgetChanged { .. }
                | ReactorEvent::CreditIssued { .. }
                | ReactorEvent::CauseSet { .. }
                | ReactorEvent::HeadroomWait { .. }
                | ReactorEvent::CachedPassDeferred { .. }
                | ReactorEvent::EventTaken { .. }
                | ReactorEvent::WatchObserved { .. }
                | ReactorEvent::Slots { .. }
                | ReactorEvent::CursorsAccepted { .. }
                | ReactorEvent::CursorClaimed { .. }
                | ReactorEvent::CloseBuilt { .. }
        ) {
            if let (true, ReactorEvent::HeadroomWait { peer, .. }) = (in_run, &event) {
                state
                    .buffer()
                    .effects
                    .push((LinkEffect::HeadroomWait, peer.clone()));
            }
            // The events it takes and its turns, kept in order with what it
            // adopts. The run's own shutdown takes events and sets causes
            // too, so only the run's are kept.
            if let (Some(now), Some(fact)) = (
                zakura_scheduler::hooks::now().filter(|_| in_run),
                AdoptionFact::of_turn(&event),
            ) {
                state.adoption.record(now.since_start(), fact);
            }
            return;
        }
        let at = zakura_scheduler::hooks::now()
            .filter(|_| in_run)
            .map(|now| now.since_start());
        let after = state.buffer().last;
        if let Some(fact) = cooldown_fact(&event, at.unwrap_or_default(), after) {
            // Only a live reactor admits, refuses or cools a peer, so none of
            // it happens in the run's own shutdown.
            match at {
                Some(_) => state.cooldowns.record(fact),
                None => state.refused.store(true, Ordering::Relaxed),
            }
            if !matches!(event, ReactorEvent::SessionAdmitted { .. }) {
                return;
            }
        }
        if let Some(fact) = AdoptionFact::of(&event) {
            let unlogged = fact.is_unlogged();
            // Only a live reactor adopts state or publishes, so none of it
            // happens in the run's own shutdown.
            match zakura_scheduler::hooks::now().filter(|_| in_run) {
                Some(now) => state.adoption.record(now.since_start(), fact),
                None => state.refused.store(true, Ordering::Relaxed),
            }
            if unlogged {
                return;
            }
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
    /// [`TapState::fail_after_claims`], for an evidence failure that
    /// another source of the same node found at step `step`.
    pub(crate) fn fail_after_claims(&self, step: u64, error: String) -> Result<(), String> {
        self.state.fail_after_claims(step, error)
    }

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
    /// also kept in `reservations`, its resource probe read, checked and
    /// kept in `capacity`, its released rounds at each drain kept in
    /// `rounds`, its stale prepare and apply calls kept in `stale_calls`,
    /// and the node's end facts kept in `ends`, whose stamps every store
    /// the tap keeps takes.
    ///
    /// # Panics
    ///
    /// As [`ReactorTap::with_sessions`] does.
    #[allow(clippy::too_many_arguments)]
    pub fn with_evidence(
        sim: &mut Sim,
        port: &Arc<ControlledPort>,
        node: &str,
        sessions: SessionEvidence,
        reservations: ReservationEvidence,
        capacity: CapacityEvidence,
        rounds: SettlementEvidence,
        stale_calls: StaleEvidence,
        ends: EndEvidence,
    ) -> (Arc<dyn HeaderSyncObserver>, ReactorTap) {
        ReactorTap::tapped(
            sim,
            port,
            node,
            sessions,
            reservations,
            (Some(capacity), rounds, stale_calls, ends),
        )
    }

    /// [`ReactorTap::new`], with the reactor's session view kept in
    /// `sessions`, which this tap's run binds and freezes with its log. Its
    /// adoption evidence freezes with the log too.
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
            (
                None,
                SettlementEvidence::default(),
                StaleEvidence::default(),
                EndEvidence::default(),
            ),
        )
    }

    /// The tap [`ReactorTap::with_evidence`] makes.
    fn tapped(
        sim: &mut Sim,
        port: &Arc<ControlledPort>,
        node: &str,
        sessions: SessionEvidence,
        reservations: ReservationEvidence,
        (capacity, rounds, stale_calls, ends): (
            Option<CapacityEvidence>,
            SettlementEvidence,
            StaleEvidence,
            EndEvidence,
        ),
    ) -> (Arc<dyn HeaderSyncObserver>, ReactorTap) {
        let run = zakura_rt::sim::run_id().expect("a Sim's scheduler enters a runtime run");
        sessions.bind(run);
        sim.on_frozen({
            let sessions = sessions.clone();
            move || sessions.freeze()
        });
        let stamps = ends.stamps();
        sim.on_frozen({
            let ends = ends.clone();
            move || ends.freeze()
        });
        let adoption = AdoptionEvidence::stamped_by(stamps.clone());
        sim.on_frozen({
            let adoption = adoption.clone();
            move || adoption.freeze()
        });
        let cooldowns = CooldownEvidence::default();
        sim.on_frozen({
            let cooldowns = cooldowns.clone();
            move || cooldowns.freeze()
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
                exited: AtomicBool::new(false),
                stamps,
                ends,
                buffer: Mutex::default(),
                work: Mutex::default(),
                sessions,
                reservations,
                adoption,
                probe: Mutex::default(),
                capacity,
                off_reactor: Mutex::default(),
                cooldowns,
                claims: ClaimEvidence::default(),
                deferred: Arc::default(),
                settled: AtomicBool::new(false),
                teardown_claims: Mutex::default(),
                rounds,
                stale: Mutex::new(StaleCheck::new(stale_calls)),
                wake: Mutex::new(WakeCheck::new(WakeEvidence::default())),
                round_life: Mutex::default(),
                chain_effect: Mutex::default(),
                authority: Mutex::default(),
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
        // A claim break no property reported by the run's end fails it, as
        // a later drain would have.
        // So does a failure held back behind a claim, unless the claim's
        // violation ended the run.
        sim.on_end({
            let claims = state.claims.clone();
            let deferred = state.deferred.clone();
            let failure = sim.failure();
            let source = checkpoint.kind().source(id.index);
            move |end, _, _| {
                if end.outcome == Outcome::Harness {
                    return;
                }
                let held = deferred
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                    .filter(|_| end.outcome != Outcome::Violation);
                if let Some(detail) = claims.unreported(None).map(|record| record.detail).or(held) {
                    failure
                        .borrow_mut()
                        .get_or_insert(HarnessFailure::SourceEvidence { source, detail });
                }
            }
        });
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
        let facts: Vec<_> = facts.into_iter().collect();
        let mut wake = self
            .state
            .wake
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for (_, fact) in &facts {
            wake.storage(fact);
        }
        drop(wake);
        if let Some(rounds) = self
            .state
            .round_life
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            rounds.storage(
                facts.iter().map(|(_, fact)| *fact),
                self.state.engine_generation(),
            );
        }
        if let Some(chain) = self
            .state
            .chain_effect
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            if let Some(engine) = self.state.engine_at() {
                chain.storage(facts.iter().map(|(_, fact)| *fact), engine);
            }
        }
        if let Some(authority) = self
            .state
            .authority
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            authority.storage(facts.iter().map(|(_, fact)| *fact));
        }
        self.pairer.facts(facts);
    }

    /// Checks this reactor's wakes against `wake`'s fixture from now on,
    /// keeping the facts in `wake`, stamped from the node's stamps. Call it
    /// before the run starts.
    pub fn watch_wakes(&self, wake: WakeEvidence) {
        wake.stamp_with(self.state.stamps.clone());
        *self
            .state
            .wake
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = WakeCheck::new(wake);
    }

    /// Checks from now on that this reactor's work follows the resets it
    /// adopts, as [`super::round_life`] describes, keeping when the check
    /// applied in `evidence`. Call it before the run starts. A harness that
    /// calls it must hand the tap the node's storage facts.
    pub fn watch_rounds(&self, evidence: RoundLifeEvidence) {
        *self
            .state
            .round_life
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            Some(RoundLife::new(evidence, self.state.engine_generation()));
    }

    /// Checks from now on that no stale apply changes the node's engine and
    /// the reactor's adoption never moves back, as [`super::chain_effect`]
    /// describes, keeping when the check applied in `evidence`. Call it
    /// before the run starts. A harness that calls it must hand the tap the
    /// node's storage facts.
    pub fn watch_chain(&self, evidence: ChainEffectEvidence) {
        let engine = self
            .state
            .engine_at()
            .expect("the tap's port outlives its harness's setup");
        *self
            .state
            .chain_effect
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(ChainEffect::new(evidence, engine));
    }

    /// Checks from now on that each round's owner is bound to its `Open`
    /// and each staged page lies on its requested path among `fixture`'s
    /// headers on `branches` and the scenario's `declared` `(parent,
    /// header)` hash pairs, as [`super::authority`] describes, keeping when
    /// the check applied in `evidence`. Call it before the run starts.
    pub fn watch_authority(
        &self,
        evidence: AuthorityEvidence,
        fixture: HeaderFixture,
        branches: impl IntoIterator<Item = u8>,
        declared: impl IntoIterator<Item = (block::Hash, block::Hash)>,
    ) {
        *self
            .state
            .authority
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            Some(Authority::new(evidence, fixture, branches, declared));
    }

    /// The node's stamps, which every fact the tap and the harness keep
    /// view-side takes.
    pub fn stamps(&self) -> Stamps {
        self.state.stamps.clone()
    }

    /// The harness's environment actions and the reactor's own exit, which
    /// the harness and the tap fill as they happen.
    pub fn ends(&self) -> EndEvidence {
        self.state.ends.clone()
    }

    /// The reactor's adoption facts, which the tap fills as they happen.
    pub fn adoption(&self) -> AdoptionEvidence {
        self.state.adoption.clone()
    }

    /// The reactor's cooldowns, refusals and admissions, which the tap fills
    /// as they happen.
    pub fn cooldowns(&self) -> CooldownEvidence {
        self.state.cooldowns.clone()
    }

    /// The claim rules the tap's drains found broken, which it fills as
    /// they happen.
    pub fn claims(&self) -> ClaimEvidence {
        self.state.claims.clone()
    }

    /// The reactor's settlement at each drain, which the tap fills as the
    /// drains happen.
    pub fn rounds(&self) -> SettlementEvidence {
        self.state.rounds.clone()
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
    /// the ownership check's accounting. Once the reactor is gone without
    /// unwinding, stopped by the harness or exited by itself, every read is
    /// a teardown read. A reactor that unwound left its counts mid-change
    /// and fails the run on its own, so its reads aren't checked. A read that finds the budget's
    /// lock held is noted unread, with whether the reactor was stopped
    /// mid-poll, and a claim break goes to the tap's [`ClaimEvidence`] as
    /// the module doc says.
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
        let gone = self.state.reactor_gone();
        if self.state.unwound.load(Ordering::Relaxed) || (gone && !self.state.ended_cleanly()) {
            return Ok(());
        }
        if let Some(task) = *self
            .state
            .off_reactor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            return Err(format!(
                "AccountingOffReactor: task {} reported capacity accounting, not the reactor's \
                 task {}, so no boundary is known quiet",
                task.index(),
                TaskId::from(probe.task()).index()
            ));
        }
        let mid_poll = self.mid_poll.contains(TaskId::from(probe.task()));
        let Some(read) = zakura_rt::sim::boundary_read(|| probe.read()) else {
            evidence.unread(
                step,
                if mid_poll {
                    BoundaryRead::UnreadMidPoll
                } else {
                    BoundaryRead::UnreadQuiet
                },
            );
            return Ok(());
        };
        let expected = if gone {
            Expected::Teardown {
                storage_permits: self
                    .state
                    .port
                    .upgrade()
                    .map_or(0, |port| port.owner_state().permits.len()),
            }
        } else {
            Expected::Live {
                accounted: if mid_poll {
                    None
                } else {
                    self.state.accounted()
                },
            }
        };
        self.state
            .wake
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .permits(read.locator_permits);
        evidence.record(CapacityReading {
            step,
            reserved: read.reserved,
            owned: read.owned,
            locator_permits: read.locator_permits,
            compared: matches!(expected, Expected::Live { accounted: Some(_) }),
            teardown: gone,
        });
        match capacity::check(&probe, read, expected) {
            Ok(()) => Ok(()),
            Err(Broken::Claim(claim, detail)) => {
                // Every teardown read is checked, since storage can still
                // hold a permit when the reactor goes and return it later,
                // but each teardown claim is reported once.
                let mut seen = self
                    .state
                    .teardown_claims
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if !gone || !seen.contains(&claim) {
                    if gone {
                        seen.push(claim);
                    }
                    self.state.claims.record(ClaimRecord {
                        step,
                        claim,
                        detail,
                    });
                }
                Ok(())
            }
            Err(Broken::Integrity(detail)) => Err(detail),
        }
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

/// The cooldown fact `event` gives, if any, reported at `at`, after the
/// tap logged event number `after`.
fn cooldown_fact(event: &ReactorEvent, at: Duration, after: u64) -> Option<CooldownFact> {
    Some(match event {
        ReactorEvent::CooldownStarted {
            peer: cooled,
            origin,
            cooldown,
        } => CooldownFact::Started {
            peer: peer(cooled),
            origin: *origin,
            cooldown: *cooldown,
            at,
            after,
        },
        ReactorEvent::SessionRefused {
            peer: refused,
            session_id,
        } => CooldownFact::Refused {
            peer: peer(refused),
            session_id: *session_id,
            at,
        },
        ReactorEvent::SessionAdmitted {
            peer: admitted,
            session_id,
            ..
        } => CooldownFact::Admitted {
            peer: peer(admitted),
            session_id: *session_id,
            at,
        },
        _ => return None,
    })
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
            ..
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
        if let Some(now) = zakura_scheduler::hooks::now() {
            self.state.adoption.reach(now.since_start());
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
        let step = log.step();
        let held = self
            .state
            .deferred
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(error) = held {
            return Err(error);
        }
        let checked = match (
            self.state
                .broken_work_rule()
                .or_else(|| self.state.sessions.broken()),
            paired,
        ) {
            (Some(rule), _) => Err(rule),
            (None, Ok(())) => match self.pairer.waiting_drop() {
                Some((call, op)) if self.state.reactor_gone() => Err(format!(
                    "UnpairedDrop: the reactor's teardown is done with call {call} dropped and \
                     its operation {op} given no drop receipt, and no shutdown removed it while \
                     the call still existed"
                )),
                _ if self.state.ended_cleanly() => self.state.open_work().map(|()| {
                    if self.state.shutdown.load(Ordering::Relaxed) {
                        self.state.settled.store(true, Ordering::Relaxed);
                    } else if !self.state.exited.swap(true, Ordering::Relaxed) {
                        self.state
                            .ends
                            .record(EndFact::ReactorExited { step: log.step() });
                    }
                }),
                _ => Ok(()),
            },
            // A reactor that unwound, from a callback or observer panic,
            // leaves its last receipts with no call end. The run's evidence
            // check then reports the tap's own health, `ReactorGone` or
            // `PortGone`, so the crash isn't read as a pairing bug.
            (None, Err(PairError::Evidence(_))) if self.state.health().is_some() => return Ok(()),
            (None, Err(PairError::Evidence(detail))) => Err(detail),
            (None, Err(PairError::PortPanicked { call })) => {
                // The run checks this after the step, ahead of any evidence
                // failure the same step reports.
                self.failure
                    .borrow_mut()
                    .get_or_insert(HarnessFailure::ReactorPortPanicked {
                        source: self.checkpoint.kind().source(self.id.index),
                        call,
                    });
                return Ok(());
            }
        };
        // The probe is read, and its claims found, even when the step's
        // other evidence failed. Then the step's claim breaks drain, and the
        // failure waits behind them.
        let capacity = self.read_capacity(step);
        let drained = self.state.drain_claims(step);
        match checked.and(capacity).and(drained) {
            Ok(()) => Ok(()),
            Err(error) => self.state.fail_after_claims(step, error),
        }
    }
}

impl Drop for ReactorTap {
    /// Fails a run that's still live, since later events go undelivered.
    fn drop(&mut self) {
        self.checkpoint.dropped();
    }
}
