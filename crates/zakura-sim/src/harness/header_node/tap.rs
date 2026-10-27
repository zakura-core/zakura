//! A header-sync reactor's events as run evidence.
//!
//! [`ReactorTap::new`] makes an observer for one node's reactor and a reader
//! for the run. The observer, installed on the reactor's startup, buffers
//! each event with the node's storage watermark. The reader drains the
//! buffer at each observation and logs every event as
//! [`Observation::Reactor`]. The tap registers its checkpoint with the run
//! like storage does, so the run checks its evidence the same way.
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
        CallEnd, HeaderSyncObserver, HeaderTargetPhase, PortCall, ReactorEvent, RequestRef,
        WaitCause,
    },
    HeaderTargetPurpose, ZakuraPeerId,
};
use zakura_rt::sim::RunId;
use zakura_scheduler::SimInstant;
use zakura_storage::header::controlled::ControlledPort;

use crate::{
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    observe::{ReactorCall, ReactorFact, ReactorPurpose, ReactorRequest},
    storage::{body_owner, hash, hex, point, scope, work_owner},
    ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
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
    buffer: Mutex<Buffer>,
}

/// Events recorded and not yet drained.
#[derive(Default)]
struct Buffer {
    /// The last event number handed out.
    last: u64,
    /// Each event with its number and storage watermark, in order.
    events: Vec<(u64, u64, ReactorEvent)>,
}

impl TapState {
    /// Locks the buffer. Every change completes under the lock, so a panic
    /// elsewhere can't leave it half changed.
    fn buffer(&self) -> std::sync::MutexGuard<'_, Buffer> {
        self.buffer.lock().unwrap_or_else(PoisonError::into_inner)
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
    /// `ReactorGone` once the observer is.
    fn health(&self) -> Option<&'static str> {
        if self.port_gone.load(Ordering::Relaxed) {
            Some("PortGone: the tapped node's port was dropped during the run")
        } else if self.observer.strong_count() == 0 {
            Some("ReactorGone: the tapped reactor dropped its observer during the run")
        } else {
            None
        }
    }
}

/// The observer the reactor holds.
struct TapObserver(Arc<TapState>);

impl std::fmt::Debug for TapObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TapObserver")
    }
}

impl HeaderSyncObserver for TapObserver {
    /// Buffers `event` with the port's last fact, or refuses it outside the
    /// tap's run. Neither step yields.
    fn observe(&self, event: ReactorEvent) {
        let state = &self.0;
        if zakura_rt::sim::run_id() != Some(state.run) {
            state.refused.store(true, Ordering::Relaxed);
            return;
        }
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
        let run = zakura_rt::sim::run_id().expect("a Sim's scheduler enters a runtime run");
        let observer = Arc::new_cyclic(|observer| {
            TapObserver(Arc::new(TapState {
                run,
                port: Arc::downgrade(port),
                observer: observer.clone(),
                refused: AtomicBool::new(false),
                port_gone: AtomicBool::new(false),
                buffer: Mutex::default(),
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
        };
        (observer, tap)
    }
}

/// `peer` as hex, by the digest storage attributes it with.
fn peer(peer: &ZakuraPeerId) -> String {
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

/// `end`'s name.
fn end(end: CallEnd) -> &'static str {
    match end {
        CallEnd::Replied => "replied",
        CallEnd::PortError => "port-error",
        CallEnd::TimedOut => "timed-out",
        CallEnd::NotSubmitted => "not-submitted",
        CallEnd::PortPanicked => "port-panicked",
    }
}

/// `cause`'s name.
fn cause(cause: WaitCause) -> &'static str {
    match cause {
        WaitCause::StagingFull => "staging-full",
        WaitCause::LocatorCapacity => "locator-capacity",
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

    /// Logs every event recorded since the last observation, in order, and
    /// records what it delivered in the checkpoint.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        // Only the run holding the checkpoint checks what this delivers.
        // Run IDs depend on the process, and the failure is replayed, so the
        // text names neither.
        if log.run() != self.id.run {
            return Err("ForeignRun: reactor tap registered with another run".to_owned());
        }
        let events = mem::take(&mut self.state.buffer().events);
        let last = events.last().map_or(self.delivered, |(seq, _, _)| *seq);
        let sent = events
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
        for obs in &sent {
            log.push(obs.clone());
        }
        self.delivered = last;
        // The run checks its own log against this, since the log this tap
        // was handed may not be the run's.
        self.checkpoint.record(log.step(), &sent, last);
        Ok(())
    }
}

impl Drop for ReactorTap {
    /// Fails a run that's still live, since later events go undelivered.
    fn drop(&mut self) {
        self.checkpoint.dropped();
    }
}
