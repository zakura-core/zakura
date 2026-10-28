//! A block-sync node's events as run evidence.
//!
//! [`BlockTap::new`] makes an observer for one node's block sync and a
//! reader for the run. The observer, installed on block sync's startup,
//! buffers each event with the node's block storage watermark. The reader
//! drains the buffer at each observation and logs every event as
//! [`Observation::BlockSync`]. The tap registers its checkpoint with the run
//! like storage does, so the run checks its evidence the same way: an event
//! dropped, added, changed or reordered on its way to the log fails the run.
//!
//! Before it logs a drain, the reader pairs the node's storage operations
//! with its events, as [`super::pair`] describes, from the facts
//! [`PolicySource`](super::PolicySource) hands it, and folds the events and
//! the operations that didn't pair into the [`BlockSyncView`] the harness's
//! checks read.
//!
//! Recording an event takes only std locks, which are never held across any
//! other call, so the tap adds no scheduling point to block sync. Installing
//! an observer does add points: block sync's owners read their snapshots
//! under their own locks only when one is installed. So a run replays only
//! with the same tap.

use std::{
    cell::RefCell,
    collections::HashMap,
    mem,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
};

use zakura_network::zakura::{
    block_sync_observe::{
        BlockEvent, BlockSyncObserver, BudgetChangeKind, HeldBody, Holder, HolderChange, LeaseStep,
        RequestStep, RequestWritePhase, RetainedBody, WorkItemPlace,
    },
    ServicePeerDirection, ZakuraPeerId,
};
use zakura_rt::sim::RunId;
use zakura_scheduler::SimInstant;
use zakura_storage::block::controlled::ControlledBlockService;

use super::{
    pair::{Paired, Pairer},
    view::BlockSyncView,
};
use crate::{
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    observe::{
        BlockSyncBody, BlockSyncClaim, BlockSyncFact, BlockSyncHolder, BlockSyncRange,
        BlockSyncRequest, BlockSyncRequestedHeight, BlockSyncRetained, BlockSyncSubmission,
        BlockSyncWorkItem, StorageFact,
    },
    storage::{block_owner, hex},
    ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
};

/// The run's reader of one node's block-sync events. Dropping it while the
/// run is live fails the run, since events after that are never delivered.
pub struct BlockTap {
    state: Arc<TapState>,
    checkpoint: Checkpoint,
    /// Binds it to the run that holds its checkpoint.
    id: EvidenceId,
    node: String,
    /// The last event delivered to the log, 0 before any.
    delivered: u64,
    /// Pairs storage operations with events.
    pairer: Pairer,
    /// The facts handed over for the next drain.
    facts: Vec<(u64, StorageFact)>,
    /// Numbers body allocations.
    allocations: Allocations,
    /// The owner state the events fold into, shared with the harness.
    view: Rc<RefCell<BlockSyncView>>,
}

/// What the observer and the reader share.
struct TapState {
    /// The runtime run whose tasks may record events.
    run: RunId,
    service: Weak<ControlledBlockService>,
    /// The observer, which only block sync holds, so it's gone once every
    /// block-sync task is.
    observer: Weak<TapObserver>,
    /// Set when something recorded an event outside `run`.
    refused: AtomicBool,
    /// Set when an event was recorded after the node's block service was
    /// dropped.
    service_gone: AtomicBool,
    buffer: Mutex<Buffer>,
}

/// Events recorded and not yet drained.
#[derive(Default)]
struct Buffer {
    /// The last event number handed out.
    last: u64,
    /// Each event with its number and storage watermark, in order.
    events: Vec<(u64, u64, BlockEvent)>,
}

impl TapState {
    /// Locks the buffer. Every change completes under the lock, so a panic
    /// elsewhere can't leave it half changed.
    fn buffer(&self) -> std::sync::MutexGuard<'_, Buffer> {
        self.buffer.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A tap publishes block-sync events.
impl LiveEvidence for TapState {
    /// The last event recorded.
    fn last_seq(&self) -> u64 {
        self.buffer().last
    }

    /// Whether an event was recorded outside the tap's run.
    fn refused_outside_run(&self) -> bool {
        self.refused.load(Ordering::Relaxed)
    }

    /// `PortGone` once an event found the node's block service dropped, and
    /// `ReactorGone` once the observer is.
    fn health(&self) -> Option<&'static str> {
        if self.service_gone.load(Ordering::Relaxed) {
            Some("PortGone: the tapped node's block service was dropped during the run")
        } else if self.observer.strong_count() == 0 {
            Some("ReactorGone: the tapped node's block sync dropped its observer during the run")
        } else {
            None
        }
    }
}

/// The observer block sync holds.
struct TapObserver(Arc<TapState>);

impl std::fmt::Debug for TapObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TapObserver")
    }
}

impl BlockSyncObserver for TapObserver {
    /// Buffers `event` with the block service's last fact, or refuses it
    /// outside the tap's run. Neither step yields.
    fn observe(&self, event: BlockEvent) {
        let state = &self.0;
        if zakura_rt::sim::run_id() != Some(state.run) {
            state.refused.store(true, Ordering::Relaxed);
            return;
        }
        // The service's own std lock, which block sync never holds.
        let storage_seq = match state.service.upgrade() {
            Some(service) => service.last_seq(),
            None => {
                state.service_gone.store(true, Ordering::Relaxed);
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

impl BlockTap {
    /// A tap on the block sync of the node named `node`, whose block storage
    /// is `service`, folding its events into `view`. Returns the observer,
    /// for block sync's startup and nothing else, and the reader. Its
    /// checkpoint is registered with `sim` now, so the run fails unless the
    /// reader observes every boundary, however it's later wrapped or added.
    ///
    /// # Panics
    ///
    /// Outside a run, which can't happen while `sim` exists.
    pub fn new(
        sim: &mut Sim,
        service: &Arc<ControlledBlockService>,
        node: &str,
        view: Rc<RefCell<BlockSyncView>>,
    ) -> (Arc<dyn BlockSyncObserver>, BlockTap) {
        let run = zakura_rt::sim::run_id().expect("a Sim's scheduler enters a runtime run");
        let observer = Arc::new_cyclic(|observer| {
            TapObserver(Arc::new(TapState {
                run,
                service: Arc::downgrade(service),
                observer: observer.clone(),
                refused: AtomicBool::new(false),
                service_gone: AtomicBool::new(false),
                buffer: Mutex::default(),
            }))
        });
        let state = observer.0.clone();
        let live: Weak<dyn LiveEvidence> = Arc::<TapState>::downgrade(&state);
        let checkpoint = Checkpoint::new(
            EvidenceKind::BlockSync {
                node: node.to_owned(),
            },
            live,
        );
        let id = sim.register(checkpoint.clone());
        let tap = BlockTap {
            state,
            checkpoint,
            id,
            node: node.to_owned(),
            delivered: 0,
            pairer: Pairer::default(),
            facts: Vec::new(),
            allocations: Allocations::default(),
            view,
        };
        (observer, tap)
    }

    /// Hands over the node's block storage facts that this step's drain
    /// logged, as `(seq, fact)` in order, so the next
    /// [`observe`](ActionSource::observe) can pair operations with events.
    /// [`PolicySource`](super::PolicySource) hands over every fact its
    /// block service logs.
    pub fn storage_facts<'f>(&mut self, facts: impl IntoIterator<Item = (u64, &'f StorageFact)>) {
        self.facts
            .extend(facts.into_iter().map(|(seq, fact)| (seq, fact.clone())));
    }

    /// `event` in plain fields, or an error for an event this build can't
    /// encode.
    fn fact(&mut self, event: &BlockEvent) -> Result<BlockSyncFact, String> {
        Ok(match event {
            BlockEvent::SessionAdmitted {
                peer: admitted,
                conn_id,
                session_id,
                generation,
                direction: way,
            } => BlockSyncFact::SessionAdmitted {
                peer: peer(admitted),
                connection: *conn_id,
                session_id: *session_id,
                generation: *generation,
                direction: direction(*way).to_owned(),
            },
            BlockEvent::FrameJudged {
                peer: sender,
                conn_id,
                generation,
                seq,
                message_type,
                flags,
                payload_len,
            } => BlockSyncFact::FrameJudged {
                peer: peer(sender),
                connection: *conn_id,
                generation: *generation,
                seq: *seq,
                message_type: *message_type,
                flags: *flags,
                // A usize is at most 64 bits on every target the simulator runs on.
                payload_len: *payload_len as u64,
            },
            BlockEvent::Request {
                owner,
                heights,
                estimated_bytes,
                step: moved,
            } => BlockSyncFact::Request {
                owner: block_owner(*owner),
                heights: heights.iter().map(|height| height.0).collect(),
                estimated_bytes: *estimated_bytes,
                step: step(*moved),
            },
            BlockEvent::Budget(change) => BlockSyncFact::Budget {
                change: match change.kind {
                    BudgetChangeKind::Reserved => "reserved",
                    BudgetChangeKind::Charged => "charged",
                    BudgetChangeKind::Released => "released",
                }
                .to_owned(),
                before: change.before,
                after: change.after,
            },
            BlockEvent::HandoffStarted {
                peer: delivering,
                owner,
                height,
                charge,
            } => BlockSyncFact::HandoffStarted {
                peer: peer(delivering),
                owner: block_owner(*owner),
                height: height.0,
                charge: *charge,
            },
            BlockEvent::HandoffEnded {
                peer: delivering,
                owner,
                height,
                charge,
            } => BlockSyncFact::HandoffEnded {
                peer: peer(delivering),
                owner: block_owner(*owner),
                height: height.0,
                charge: *charge,
            },
            BlockEvent::Holder {
                body: held,
                holder,
                change,
            } => BlockSyncFact::Holder {
                body: body(held),
                holder: self.allocations.holder(*holder, held),
                change: match change {
                    HolderChange::Retained => "retained",
                    HolderChange::Released => "released",
                }
                .to_owned(),
            },
            BlockEvent::ServingLease {
                peer: requesting,
                conn_id,
                generation,
                start,
                count,
                step: leased,
            } => BlockSyncFact::ServingLease {
                peer: peer(requesting),
                connection: *conn_id,
                generation: *generation,
                start: start.0,
                count: *count,
                step: match leased {
                    LeaseStep::Acquired => "acquired",
                    LeaseStep::Released => "released",
                }
                .to_owned(),
            },
            BlockEvent::ServingFrame {
                peer: requesting,
                conn_id,
                generation,
                start,
                message_type,
                body_bytes,
            } => BlockSyncFact::ServingFrame {
                peer: peer(requesting),
                connection: *conn_id,
                generation: *generation,
                start: start.0,
                message_type: *message_type,
                body_bytes: *body_bytes,
            },
            BlockEvent::Routine(snapshot) => BlockSyncFact::Routine {
                peer: peer(&snapshot.peer),
                connection: snapshot.conn_id,
                generation: snapshot.generation,
                requests: snapshot
                    .requests
                    .iter()
                    .map(|request| BlockSyncRequest {
                        owner: block_owner(request.owner),
                        write: phase(request.write).to_owned(),
                        heights: request
                            .heights
                            .iter()
                            .map(|height| BlockSyncRequestedHeight {
                                height: height.height.0,
                                estimated_bytes: height.estimated_bytes,
                                received: height.received,
                            })
                            .collect(),
                    })
                    .collect(),
                claims: snapshot
                    .claims
                    .iter()
                    .map(|claim| BlockSyncClaim {
                        height: claim.height.0,
                        owner: block_owner(claim.owner),
                        estimated_bytes: claim.estimated_bytes,
                    })
                    .collect(),
                serving: snapshot
                    .serving
                    .iter()
                    .map(|range| BlockSyncRange {
                        first: range.first.0,
                        last: range.last.0,
                        completed: range.completed,
                    })
                    .collect(),
            },
            BlockEvent::WorkQueue(items) => BlockSyncFact::WorkQueue {
                items: items
                    .iter()
                    .map(|item| BlockSyncWorkItem {
                        height: item.height.0,
                        owner: item.owner.map(block_owner),
                        place: match item.place {
                            WorkItemPlace::Pending => "pending",
                            WorkItemPlace::Provisional => "provisional",
                            WorkItemPlace::InFlight => "in-flight",
                        }
                        .to_owned(),
                        estimated_bytes: item.estimated_bytes,
                        reserved_bytes: item.reserved_bytes,
                    })
                    .collect(),
            },
            BlockEvent::Sequencer(ownership) => BlockSyncFact::Sequencer {
                reorder: ownership
                    .reorder
                    .iter()
                    .map(|retained| self.allocations.retained(retained))
                    .collect(),
                applying: ownership
                    .applying
                    .iter()
                    .map(|retained| self.allocations.retained(retained))
                    .collect(),
                submissions: ownership
                    .submissions
                    .iter()
                    .map(|submission| BlockSyncSubmission {
                        body: BlockSyncBody {
                            height: submission.height.0,
                            owner: block_owner(submission.owner),
                            bytes: submission.bytes,
                        },
                        token: submission.token,
                        detached: submission.detached,
                    })
                    .collect(),
            },
            BlockEvent::SequencerClosed => BlockSyncFact::SequencerClosed,
            // The enum is non-exhaustive, and an event the log can't hold
            // would leave the evidence silently incomplete.
            unknown => return Err(format!("UnknownEvent: the tap can't encode {unknown:?}")),
        })
    }
}

/// Numbers each body allocation the tap sees, from 1 in the order first
/// seen, by its address and the body it holds. Addresses differ between
/// processes, so the log carries these numbers instead. Keying by the body
/// too keeps a freed address that a later body reuses from sharing the
/// earlier body's number. Numbers are never freed, so a body keeps its
/// number as it moves between holders.
#[derive(Default)]
struct Allocations {
    numbers: HashMap<(usize, HeldBody), u64>,
}

impl Allocations {
    /// The number of `body`'s allocation at `address`.
    fn number(&mut self, address: usize, body: HeldBody) -> u64 {
        let next =
            u64::try_from(self.numbers.len()).expect("a run sees fewer than 2^64 bodies") + 1;
        *self.numbers.entry((address, body)).or_insert(next)
    }

    /// `holder` of `body` in plain fields.
    fn holder(&mut self, holder: Holder, held: &HeldBody) -> BlockSyncHolder {
        match holder {
            Holder::Queued { allocation } => BlockSyncHolder::Queued {
                allocation: self.number(allocation, *held),
            },
            Holder::Reorder { allocation } => BlockSyncHolder::Reorder {
                allocation: self.number(allocation, *held),
            },
            Holder::Applying { allocation } => BlockSyncHolder::Applying {
                allocation: self.number(allocation, *held),
            },
            Holder::Submitted { token } => BlockSyncHolder::Submitted { token },
            Holder::Detached { token } => BlockSyncHolder::Detached { token },
        }
    }

    /// A reorder-buffer or `applying` body in plain fields.
    fn retained(&mut self, retained: &RetainedBody) -> BlockSyncRetained {
        let held = HeldBody {
            height: retained.height,
            owner: retained.owner,
            bytes: retained.bytes,
        };
        BlockSyncRetained {
            body: body(&held),
            admitted_estimate_bytes: retained.admitted_estimate_bytes,
            allocation: self.number(retained.allocation, held),
        }
    }
}

/// `peer` as hex, by the digest storage attributes it with.
fn peer(peer: &ZakuraPeerId) -> String {
    hex(&peer.digest())
}

/// `direction`'s name.
fn direction(direction: ServicePeerDirection) -> &'static str {
    match direction {
        ServicePeerDirection::Inbound => "inbound",
        ServicePeerDirection::Outbound => "outbound",
    }
}

/// `phase`'s name.
fn phase(phase: RequestWritePhase) -> &'static str {
    match phase {
        RequestWritePhase::Unpublished => "unpublished",
        RequestWritePhase::Queued => "queued",
        RequestWritePhase::Started => "started",
        RequestWritePhase::Written => "written",
        RequestWritePhase::Skipped => "skipped",
    }
}

/// `step`'s name. A drop names the phase it dropped in, as
/// `dropped-<phase>`.
fn step(step: RequestStep) -> String {
    match step {
        RequestStep::Published => "published".to_owned(),
        RequestStep::Started => "started".to_owned(),
        RequestStep::Written => "written".to_owned(),
        RequestStep::DeliveryFailed => "delivery-failed".to_owned(),
        RequestStep::Expired => "expired".to_owned(),
        RequestStep::Reset => "reset".to_owned(),
        RequestStep::Dropped(at) => format!("dropped-{}", phase(at)),
    }
}

/// `held` in plain fields.
fn body(held: &HeldBody) -> BlockSyncBody {
    BlockSyncBody {
        height: held.height.0,
        owner: block_owner(held.owner),
        bytes: held.bytes,
    }
}

/// A tap offers no actions. Its observation drains the observer's buffer
/// into the log.
impl ActionSource for BlockTap {
    /// Nothing.
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    /// Nothing.
    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    /// Never called, since the tap offers nothing.
    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("a block-sync tap offers no actions");
    }

    /// Logs every event recorded since the last observation, in order,
    /// records what it delivered in the checkpoint, pairs the drain with
    /// the handed-over storage facts, and folds it into the view, with every
    /// operation that didn't pair. A drain whose sequence numbers skip or go
    /// back is still logged, then fails, unless the tap is unhealthy, which
    /// the run reports instead.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        // Only the run holding the checkpoint checks what this delivers.
        // Run IDs depend on the process, and the failure is replayed, so the
        // text names neither.
        if log.run() != self.id.run {
            return Err("ForeignRun: block-sync tap registered with another run".to_owned());
        }
        let events = mem::take(&mut self.state.buffer().events);
        let last = events.last().map_or(self.delivered, |(seq, _, _)| *seq);
        let mut sent = Vec::with_capacity(events.len());
        for (seq, storage_seq, event) in &events {
            sent.push(Observation::BlockSync {
                instance: self.id.index,
                seq: *seq,
                storage_seq: *storage_seq,
                node: self.node.clone(),
                event: self.fact(event)?,
            });
        }
        for obs in &sent {
            log.push(obs.clone());
        }
        self.delivered = last;
        // The run checks its own log against this, since the log this tap
        // was handed may not be the run's.
        self.checkpoint.record(log.step(), &sent, last);
        let facts = mem::take(&mut self.facts);
        let paired = self.pairer.pair(
            facts.iter().map(|(seq, fact)| (*seq, fact)),
            sent.iter().filter_map(|obs| match obs {
                Observation::BlockSync {
                    storage_seq, event, ..
                } => Some((*storage_seq, event)),
                _ => None,
            }),
        );
        let mut view = self.view.borrow_mut();
        for (_, _, event) in &events {
            view.fold(event);
        }
        view.step = Some(log.step());
        match paired {
            Ok(drain) => {
                view.pairing.extend(drain.anomalies);
                for pairing in drain.paired {
                    match pairing {
                        Paired::Submission { token, op } => {
                            view.submissions.insert(token, op);
                        }
                        Paired::Read { start, op } => {
                            view.reads.insert(op, zakura_chain::block::Height(start));
                        }
                    }
                }
                Ok(())
            }
            // An unhealthy tap's events may carry no watermark, such as
            // those recorded after the block service was dropped. The run's
            // evidence check then reports the tap's own health.
            Err(_) if self.state.health().is_some() => Ok(()),
            Err(detail) => Err(detail),
        }
    }
}

impl Drop for BlockTap {
    /// Fails a run that's still live, since later events go undelivered.
    fn drop(&mut self) {
        self.checkpoint.dropped();
    }
}
