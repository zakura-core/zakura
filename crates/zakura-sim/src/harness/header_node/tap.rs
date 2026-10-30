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
//! The tap builds the reactor's [`ObserverSlot`] and keeps its delivery
//! count, so an event the slot sent that the observer never recorded fails
//! the run as `Unobserved` (M6a-3). It logs every `Settled` and `Resumed`
//! marker, but a snapshot only when it differs from the last one logged.
//! The reactor reports its own teardown, `Dropping`, so its observer's drop
//! after one is expected. A drop with none before it, as when the reactor
//! unwinds from a panic, which reports nothing, is `ReactorGone`.
//!
//! The observer also keeps each boundary read the reactor registers (M6a-7a)
//! for the evidence hub, which runs them (see [`super::reads`]).
//!
//! Recording an event takes only std locks, which are never held across any
//! other call, so the tap adds no scheduling point to the reactor.

use std::{
    collections::BTreeMap,
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
};

use zakura_network::zakura::{
    diag::{ComponentRead, ReadKey},
    observe::{
        CallEnd, FrameTag, HeaderSyncObserver, HeaderTargetPhase, ObserverSlot, OutcomeDisposition,
        Placement, PortCall, ReactorEvent, ReactorSnapshot as Snapshot, RequestRef, ResumeArm,
        TransferDisposition, WaitCause, WorkPhase,
    },
    HeaderSyncMisbehavior, HeaderTargetPurpose, ServicePeerDirection, ZakuraPeerId,
};
use zakura_rt::sim::RunId;
use zakura_scheduler::SimInstant;
use zakura_storage::header::controlled::ControlledPort;

use super::{
    pair::{PairError, Pairer},
    reads::ReadSink,
};
use crate::{
    draws::Failure,
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    harness::evidence_hub::{position, Row},
    observe::{
        HeldDigest, ReactorAdopted, ReactorCall, ReactorCursor, ReactorFact, ReactorFrame,
        ReactorHolding, ReactorLease, ReactorPlacement, ReactorPurpose, ReactorRequest,
        ReactorSession, ReactorSnapshot, ReactorWork, ReactorWorkState, StorageFact,
    },
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
    /// The last snapshot logged, which an unchanged `Settled` doesn't
    /// repeat.
    snapshot: Option<ReactorSnapshot>,
    /// How many reads were registered under each key, for their
    /// `ReadRegistered` facts.
    generations: BTreeMap<String, u64>,
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
    /// The reactor's delivery count, from its observer slot. It outlives the
    /// reactor, so an event the slot sent but the observer never recorded
    /// still counts.
    emitted: Arc<zakura_rt::sync::counter::AtomicU64>,
    /// Set when something recorded an event outside `run`.
    refused: AtomicBool,
    /// Set when an event was recorded after the node's port was dropped.
    port_gone: AtomicBool,
    /// Set once the reactor reported its teardown, `Dropping`, after which
    /// its observer may go.
    torn_down: AtomicBool,
    buffer: Mutex<Buffer>,
    /// The boundary reads the reactor registered, for the evidence hub.
    reads: ReadSink,
}

/// Events recorded and not yet drained.
#[derive(Default)]
struct Buffer {
    /// The last event number handed out.
    last: u64,
    /// Each event with its number, storage watermark and emission
    /// position, in order.
    events: Vec<(u64, u64, Option<u64>, ReactorEvent)>,
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
    /// The last event the reactor emitted or the observer recorded,
    /// whichever is later. An event the slot counted that the observer
    /// didn't record is one the log never got.
    fn last_seq(&self) -> u64 {
        let emitted = self
            .emitted
            .load(zakura_rt::sync::counter::Ordering::Relaxed);
        self.buffer().last.max(emitted)
    }

    /// Whether an event was recorded outside the tap's run.
    fn refused_outside_run(&self) -> bool {
        self.refused.load(Ordering::Relaxed)
    }

    /// `PortGone` once an event found the node's port dropped, and
    /// `ReactorGone` once the observer is, unless the reactor reported a
    /// clean teardown first.
    fn health(&self) -> Option<&'static str> {
        if self.port_gone.load(Ordering::Relaxed) {
            Some("PortGone: the tapped node's port was dropped during the run")
        } else if self.observer.strong_count() == 0 && !self.torn_down.load(Ordering::Relaxed) {
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
        if matches!(event, ReactorEvent::Dropping { .. }) {
            state.torn_down.store(true, Ordering::Relaxed);
        }
        // Taken before the lock, which nothing else takes in between.
        let pos = position();
        let mut buffer = state.buffer();
        buffer.last = buffer
            .last
            .checked_add(1)
            .expect("a run records fewer than 2^64 events");
        let seq = buffer.last;
        buffer.events.push((seq, storage_seq, pos, event));
    }

    /// Keeps `read` for the evidence hub. The slot reports its
    /// `ReadRegistered` next, through [`observe`](Self::observe). Only a std
    /// lock, never held across anything else.
    fn register_read(&self, key: ReadKey, read: ComponentRead) {
        self.0
            .reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((key, read));
    }
}

impl ReactorTap {
    /// A tap on the reactor of the node named `node`, whose storage is
    /// `port`. Returns the observer's slot, for the reactor's startup and
    /// nothing else, and the reader, which keeps the slot's delivery count.
    /// Its checkpoint is registered with `sim` now, so the run fails unless
    /// the reader observes every boundary, however it's later wrapped or
    /// added.
    ///
    /// # Panics
    ///
    /// Outside a run, which can't happen while `sim` exists.
    pub fn new(
        sim: &mut Sim,
        port: &Arc<ControlledPort>,
        node: &str,
    ) -> (ObserverSlot, ReactorTap) {
        let run = zakura_rt::sim::run_id().expect("a Sim's scheduler enters a runtime run");
        let emitted: Arc<zakura_rt::sync::counter::AtomicU64> = Arc::default();
        let observer = Arc::new_cyclic(|observer| {
            TapObserver(Arc::new(TapState {
                run,
                port: Arc::downgrade(port),
                observer: observer.clone(),
                emitted: Arc::clone(&emitted),
                refused: AtomicBool::new(false),
                port_gone: AtomicBool::new(false),
                torn_down: AtomicBool::new(false),
                buffer: Mutex::default(),
                reads: ReadSink::default(),
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
            snapshot: None,
            generations: BTreeMap::new(),
        };
        (ObserverSlot::counting(observer, emitted), tap)
    }

    /// The tap's registration index among the run's taps.
    pub(crate) fn instance(&self) -> u64 {
        self.id.index
    }

    /// What a hub needs to fold this reactor's state: which rows are its,
    /// and its source label.
    pub(crate) fn link(&self) -> ReactorLink {
        ReactorLink {
            instance: self.id.index,
            source: self.checkpoint.kind().source(self.id.index),
            reads: self.state.reads.clone(),
        }
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
}

/// One tapped reactor, as the evidence hub's fold sees it.
pub(crate) struct ReactorLink {
    /// The tap's index, which its rows carry.
    pub(crate) instance: u64,
    /// The tap's source label, for failures.
    pub(crate) source: String,
    /// The boundary reads the reactor registered.
    pub(crate) reads: ReadSink,
}

/// `key`'s name, or an error for a key this build can't encode.
pub(crate) fn read_key(key: &ReadKey) -> Result<&'static str, String> {
    match key {
        ReadKey::Budget => Ok("budget"),
        unknown => Err(format!(
            "UnknownEvent: the tap can't encode the read key {unknown:?}"
        )),
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
        incarnation: request.incarnation,
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
            transfer,
        } => ReactorCall::Prepare {
            peer: peer(&supplier),
            source: hex(&source.digest()),
            owner: work_owner(owner),
            transfer,
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
        CallEnd::Dropped => "dropped",
    }
}

/// `ended`'s name, with its apply call for a handoff.
fn disposition(ended: TransferDisposition) -> (&'static str, Option<u64>) {
    match ended {
        TransferDisposition::NotDispatched => ("not-dispatched", None),
        TransferDisposition::Dropped => ("dropped", None),
        TransferDisposition::External => ("external", None),
        TransferDisposition::Panicked => ("panicked", None),
        TransferDisposition::Stale => ("stale", None),
        TransferDisposition::NoAuthority => ("no-authority", None),
        TransferDisposition::OwnerMismatch => ("owner-mismatch", None),
        TransferDisposition::RepairInvalid => ("repair-invalid", None),
        TransferDisposition::HandedToApply { call } => ("handed-to-apply", Some(call)),
        TransferDisposition::ApplyRefused => ("apply-refused", None),
        TransferDisposition::PrepareFailed => ("prepare-failed", None),
    }
}

/// `handled`'s name, with the matching request's incarnation.
fn outcome(handled: OutcomeDisposition) -> (&'static str, Option<u64>) {
    match handled {
        OutcomeDisposition::NoActive => ("no-active", None),
        OutcomeDisposition::OtherRequest => ("other-request", None),
        OutcomeDisposition::NotReceiving { incarnation } => ("not-receiving", Some(incarnation)),
        OutcomeDisposition::Retired { incarnation } => ("retired", Some(incarnation)),
        OutcomeDisposition::Retried { incarnation } => ("retried", Some(incarnation)),
    }
}

/// `digest` in plain fields.
fn digest(digest: zakura_network::zakura::observe::HeldDigest) -> HeldDigest {
    HeldDigest {
        value: digest.value,
        len: digest.len,
    }
}

/// `tag` in plain fields.
fn frame(tag: FrameTag) -> ReactorFrame {
    ReactorFrame {
        quic_stream: tag.quic_stream,
        seq: tag.seq,
        // A frame's payload fits a u32.
        payload_len: tag.payload_len as u64,
        received: digest(tag.received),
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

/// `at`'s name.
fn arm(at: ResumeArm) -> &'static str {
    match at {
        ResumeArm::Start => "start",
        ResumeArm::FatalWait => "fatal-wait",
        ResumeArm::Shutdown => "shutdown",
        ResumeArm::PortCompletion => "port-completion",
        ResumeArm::LocatorPermit => "locator-permit",
        ResumeArm::CapacityReleased => "capacity-released",
        ResumeArm::Maintenance => "maintenance",
        ResumeArm::Lifecycle => "lifecycle",
        ResumeArm::Event => "event",
        ResumeArm::Yield => "yield",
        ResumeArm::CommittedSnapshot => "committed-snapshot",
        ResumeArm::VctRepairs => "vct-repairs",
    }
}

/// `direction`'s name.
fn direction(direction: ServicePeerDirection) -> &'static str {
    match direction {
        ServicePeerDirection::Inbound => "inbound",
        ServicePeerDirection::Outbound => "outbound",
    }
}

/// `settled` in plain fields.
fn snapshot(settled: Snapshot) -> ReactorSnapshot {
    let peers = |peers: Vec<ZakuraPeerId>| peers.iter().map(peer).collect();
    ReactorSnapshot {
        adopted: settled.adopted.map(|adopted| ReactorAdopted {
            header_generation: adopted.header_generation,
            finalized: point(adopted.finalized),
            header_best: point(adopted.header_best),
        }),
        sessions: settled
            .sessions
            .into_iter()
            .map(|session| ReactorSession {
                peer: peer(&session.peer),
                session_id: session.session_id,
                direction: direction(session.direction).to_owned(),
            })
            .collect(),
        cooldowns: peers(settled.cooldowns),
        pending_locators: peers(settled.pending_locators),
        unhandled_calls: settled.unhandled_calls,
        work: settled
            .work
            .into_iter()
            .map(|work| ReactorWork {
                peer: peer(&work.peer),
                session_id: work.session_id,
                target: hash(work.target_tip_hash),
                state: match work.phase {
                    WorkPhase::AwaitingLocator { scope: authority } => {
                        ReactorWorkState::AwaitingLocator {
                            scope: scope(authority),
                        }
                    }
                    WorkPhase::Active {
                        incarnation,
                        request_id,
                        owner,
                        purpose: why,
                        phase: at,
                        held,
                        held_digest,
                        cursor,
                    } => ReactorWorkState::Active {
                        incarnation,
                        request_id,
                        owner: Box::new(work_owner(owner)),
                        purpose: purpose(why),
                        target_phase: phase(at).to_owned(),
                        // A queue holds at most a few thousand headers.
                        held: held as u64,
                        held_digest: digest(held_digest),
                        cursor: cursor.map(|cursor| ReactorCursor {
                            height: cursor.height.0,
                            hash: hash(cursor.hash),
                            headers: cursor.headers,
                            bytes: cursor.bytes,
                        }),
                    },
                },
            })
            .collect(),
        holdings: settled
            .holdings
            .into_iter()
            .map(|holding| ReactorHolding {
                peer: peer(&holding.peer),
                reservation: holding.reservation,
                leases: holding
                    .leases
                    .into_iter()
                    .map(|lease| ReactorLease {
                        lease: lease.lease,
                        // A lease owns at most a few thousand headers.
                        count: lease.count as u64,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// `placement` in plain fields.
fn placement(placement: Placement) -> ReactorPlacement {
    ReactorPlacement {
        session_id: placement.session_id,
        scope: scope(placement.scope),
        target: hash(placement.target_tip_hash),
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
        ReactorEvent::Settled {
            turn,
            last,
            snapshot: settled,
        } => ReactorFact::Settled {
            turn,
            last,
            snapshot: Some(Box::new(snapshot(*settled))),
        },
        ReactorEvent::Dropping { turn } => ReactorFact::Dropping { turn },
        ReactorEvent::Resumed { turn, arm: at } => ReactorFact::Resumed {
            turn,
            arm: arm(at).to_owned(),
        },
        ReactorEvent::Placed {
            reservation,
            peer: owner,
            count,
            placement: placed,
        } => ReactorFact::Placed {
            reservation,
            peer: peer(&owner),
            // A reservation holds at most a few thousand headers.
            count: count as u64,
            placement: placed.map(placement),
        },
        ReactorEvent::Bound {
            reservation,
            incarnation,
        } => ReactorFact::Bound {
            reservation,
            incarnation,
        },
        ReactorEvent::LeasesCaptured {
            call,
            incarnation,
            leases,
        } => ReactorFact::LeasesCaptured {
            call,
            incarnation,
            leases,
        },
        ReactorEvent::EntriesTaken {
            incarnation,
            transfer,
            count,
            chain,
        } => ReactorFact::EntriesTaken {
            incarnation,
            transfer,
            // A transfer holds at most a few thousand headers.
            count: count as u64,
            chain: digest(chain),
        },
        ReactorEvent::Staged {
            incarnation,
            frame: tag,
            count,
            after,
        } => ReactorFact::Staged {
            incarnation,
            frame: tag.map(frame),
            // A page holds at most a few thousand headers.
            count: count as u64,
            after: digest(after),
        },
        ReactorEvent::Outcome {
            session_id,
            subscription,
            frame: tag,
            disposition: handled,
        } => {
            let (disposition, incarnation) = outcome(handled);
            ReactorFact::Outcome {
                session_id,
                subscription,
                frame: tag.map(frame),
                disposition: disposition.to_owned(),
                incarnation,
            }
        }
        ReactorEvent::TransferEnded {
            transfer,
            disposition: ended,
        } => {
            let (disposition, apply) = disposition(ended);
            ReactorFact::TransferEnded {
                transfer,
                disposition: disposition.to_owned(),
                apply,
            }
        }
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
    /// order, and records what it delivered in the checkpoint. A drain that
    /// doesn't pair is still logged, then fails, unless the tap is unhealthy,
    /// which the run reports instead, or records a port panic as the run's
    /// harness failure.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let (rows, drained) = self.drain(log)?;
        rows.into_iter().for_each(|row| log.push(row.obs));
        drained
    }
}

impl ReactorTap {
    /// Pairs every event recorded since the last observation and returns
    /// them as rows, in order, recording in the checkpoint what it delivers
    /// to `log`'s step. A drain that doesn't pair is still returned, with
    /// its failure, unless the tap is unhealthy, which the run reports
    /// instead, or records a port panic as the run's harness failure.
    pub(crate) fn drain(
        &mut self,
        log: &ObservationLog,
    ) -> Result<(Vec<Row>, Result<(), String>), String> {
        // Only the run holding the checkpoint checks what this delivers.
        // Run IDs depend on the process, and the failure is replayed, so the
        // text names neither.
        if log.run() != self.id.run {
            return Err("ForeignRun: reactor tap registered with another run".to_owned());
        }
        let events = mem::take(&mut self.state.buffer().events);
        let last = events.last().map_or(self.delivered, |(seq, _, _, _)| *seq);
        let mut sent = Vec::with_capacity(events.len());
        let mut positions = Vec::with_capacity(events.len());
        for (seq, storage_seq, pos, event) in events {
            let mut event = match event {
                ReactorEvent::ReadRegistered { key } => {
                    let key = read_key(&key)?.to_owned();
                    let generation = self.generations.entry(key.clone()).or_default();
                    *generation += 1;
                    ReactorFact::ReadRegistered {
                        key,
                        generation: *generation,
                    }
                }
                event => fact(event)?,
            };
            // Every marker is logged, but an unchanged snapshot only once.
            if let ReactorFact::Settled { snapshot, .. } = &mut event {
                let settled = snapshot.take().map(|settled| *settled);
                if settled != self.snapshot {
                    self.snapshot.clone_from(&settled);
                    *snapshot = settled.map(Box::new);
                }
            }
            sent.push(Observation::Reactor {
                instance: self.id.index,
                seq,
                storage_seq,
                node: self.node.clone(),
                event,
            });
            positions.push(pos);
        }
        let paired = self
            .pairer
            .pair(sent.iter_mut().filter_map(|obs| match obs {
                Observation::Reactor {
                    storage_seq, event, ..
                } => Some((*storage_seq, event)),
                _ => None,
            }));
        self.delivered = last;
        // The run checks its own log against this, since the log this tap
        // was handed may not be the run's.
        self.checkpoint.record(log.step(), &sent, last);
        let rows = positions
            .into_iter()
            .zip(sent)
            .map(|(pos, obs)| Row::at(pos, obs))
            .collect();
        let paired = match paired {
            Ok(()) => Ok(()),
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
        };
        Ok((rows, paired))
    }
}

impl Drop for ReactorTap {
    /// Fails a run that's still live, since later events go undelivered.
    fn drop(&mut self) {
        self.checkpoint.dropped();
    }
}
