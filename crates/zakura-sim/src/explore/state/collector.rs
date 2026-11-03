//! The protocol-state collector: reads a run and hits each
//! [`StateFeature`] it reaches.
//!
//! It reads only observations, the registry's events and the run's end, so
//! one collector serves every harness: a harness that logs no storage fact,
//! for one, reaches no storage feature. It keeps each relation it needs by
//! the run's full identities, in ordinary ordered containers, and starts
//! empty for each input.

use std::collections::{BTreeMap, BTreeSet};

use zakura_network::zakura::{ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_HEADER_SYNC};

use super::{
    Acknowledgement, Baseline, BlockStage, BudgetChange, Call, CallEnd, CapacityClass,
    CapacityEvent, Direction, Disposition, Dropped, Epoch, EpochWork, Fork, FrameLabel, FrameStage,
    Level, Namespace, ObligationPoint, Operation, Purpose, ReorgStage, ReplacementEvent,
    ReplacementKind, RequestEvent, RequestProgress, RetireReason, RetiredPhase, Retirement, Row,
    Service, StateFeature, StreamService, Timing, Transition,
};
use crate::{
    check::StepCtx,
    end::RunEnd,
    explore::feedback::{Collect, FeatureMap},
    model::is_fault_cause,
    observe::{
        BlockSyncFact, HeaderCheckFact, ReactorCall, ReactorFact, ReactorPurpose, StorageFact,
        StoragePoint, StorageRequest, StorageResult, StreamId, TransitionFact, TransportFact,
        WorkOwner,
    },
    property::{CheckEvent, Lifecycle},
    storage::{BLOCK_SERVICE, HEADER_SERVICE},
    Observation, ObservationLog,
};

/// A fresh protocol-state collector, for `sim_target!(Harness,
/// zakura_sim::explore::state::collector)`.
pub fn collector() -> Box<dyn Collect> {
    Box::new(StateCollector::default())
}

/// A storage operation the collector follows: its service, instance and
/// ID.
type OpKey = (Service, u64, u64);

/// What the collector knows of one storage operation.
struct Op {
    /// Its request.
    request: StorageRequest,
    /// Its block stage, for a block operation that has one.
    stage: Option<BlockStage>,
    /// Whether its caller dropped the call.
    caller_dropped: bool,
}

/// A header request the reactor holds active.
struct Active {
    /// The peer whose session owns it.
    peer: String,
    /// The target it pursues.
    target: String,
}

/// A header target storage was asked to prepare.
struct Prepared {
    /// The request that owns it.
    owner: WorkOwner,
    /// The parent of its first header.
    ancestor: StoragePoint,
    /// Its last header.
    target: StoragePoint,
}

/// A peer's frame: its connection, stream, QUIC stream and sequence
/// number, as [`Observation::PeerFrame`] gives them.
type SentFrame = (u64, StreamId, Option<u64>, u64);

/// A block-sync routine's last snapshot.
#[derive(Clone, Copy, Default)]
struct Routine {
    /// It ended.
    ended: bool,
    /// It held an outstanding request.
    busy: bool,
}

/// A service session the transport admitted.
struct Session {
    /// Its connection.
    connection: u64,
    /// Its QUIC stream.
    quic_stream: u64,
    /// The node's session ID.
    session_id: u64,
}

/// The run's protocol state so far.
#[derive(Default)]
struct StateCollector {
    /// Every storage operation submitted.
    ops: BTreeMap<OpKey, Op>,
    /// Each block query's operation, by block storage instance and query ID.
    queries: BTreeMap<(u64, u64), u64>,
    /// The owners of the header requests the reactor retired.
    retired: Vec<WorkOwner>,
    /// Each reactor's port calls, by tap instance and call number.
    calls: BTreeMap<(u64, u64), Call>,
    /// The header requests the reactor holds active, by tap instance,
    /// session and request ID.
    active: BTreeMap<(u64, u64, u64), Active>,
    /// Every header target storage was asked to prepare.
    prepared: Vec<Prepared>,
    /// The selected tip's height, as header storage last reported it.
    selected_tip: Option<u32>,
    /// The target of each applied header target whose common ancestor lay
    /// below the reported selected tip. Storage applies a weaker side
    /// branch too, so an apply is a reorg only once a locator reports its
    /// target as the selected tip.
    reorg_targets: Vec<StoragePoint>,
    /// Each block request's last progress step, by session and request ID.
    progress: BTreeMap<(u64, u64), RequestProgress>,
    /// The first and the newest body-work epoch block sync's work named.
    epochs: Option<(u64, u64)>,
    /// Each block request a routine published that hasn't failed, by
    /// session and request ID: its body-work epoch and heights. A request
    /// the routine wrote stays, since its bodies arrive after the routine
    /// drops it.
    outstanding: BTreeMap<(u64, u64), (u64, Vec<u32>)>,
    /// The height of each body a peer's block frame carried, by connection
    /// and the frame's sequence number.
    bodies: BTreeMap<(u64, u64), u32>,
    /// Each block-sync connection's session.
    block_connections: BTreeMap<u64, u64>,
    /// Each peer's newest block-sync connection.
    block_sessions: BTreeMap<String, u64>,
    /// Each block-sync routine's last snapshot, by connection.
    routines: BTreeMap<u64, Routine>,
    /// The block-sync connections a newer one replaced.
    replaced_block: BTreeSet<u64>,
    /// Each peer's newest session of each stream kind.
    sessions: BTreeMap<(String, u16), Session>,
    /// The replaced sessions, by connection and QUIC stream.
    replaced: BTreeMap<(u64, u64), (ReplacementKind, StreamService)>,
    /// The connections the transport tore down.
    torn_down: BTreeSet<u64>,
    /// Each frame a peer sent, with its row and model label.
    sent: BTreeMap<SentFrame, (Row, FrameLabel)>,
    /// The last frame the node judged on each connection.
    last_judged: BTreeMap<u64, (Row, FrameLabel)>,
    /// Every frame the node judged, by connection.
    judged: BTreeMap<u64, BTreeSet<(Row, FrameLabel)>>,
    /// The connections the node closed.
    closed: BTreeSet<u64>,
    /// Block sync's budget counter after its last change.
    budget: Option<u64>,
    /// How many times each body holder retained a body, less its releases.
    holders: BTreeMap<String, i64>,
    /// Block sync's sequencer closed.
    sequencer_closed: bool,
    /// The first step whose observations the collector hasn't read.
    read: u64,
}

impl Collect for StateCollector {
    /// Reads each observation logged since the last step it read: setup's
    /// at the first step, then the step's own.
    fn after_step(&mut self, cx: &StepCtx<'_>, map: FeatureMap) {
        self.read_through(cx.log, cx.step, map);
    }

    /// Hits each obligation's point.
    fn observe_events(&mut self, events: &[CheckEvent], map: FeatureMap) {
        for event in events {
            let CheckEvent::Lifecycle { property, to } = event else {
                continue;
            };
            let point = match to {
                Lifecycle::Applicable => continue,
                Lifecycle::Obliged => ObligationPoint::Opened,
                Lifecycle::Discharged => ObligationPoint::Discharged,
                Lifecycle::Expired => ObligationPoint::Expired,
            };
            let first = property.as_str().split('/').next().unwrap_or_default();
            let namespace = Namespace::of(first);
            map.hit(StateFeature::Obligation { namespace, point }.feature());
        }
    }

    /// Hits the frames no close followed, block sync's baseline once its
    /// sequencer closed, and an incomplete drain.
    fn at_end(&mut self, end: &RunEnd, log: &ObservationLog, map: FeatureMap) {
        self.read_through(log, log.step(), map);
        for (connection, frames) in &self.judged {
            if self.closed.contains(connection) {
                continue;
            }
            for &(row, label) in frames {
                map.hit(frame(row, Direction::ToNode, label, FrameStage::Unpunished).feature());
            }
        }
        if self.sequencer_closed {
            let settled = self.budget.unwrap_or(0) == 0 && self.holders.values().all(|n| *n == 0);
            let baseline = match settled {
                true => Baseline::Settled,
                false => Baseline::Unsettled,
            };
            map.hit(StateFeature::Baseline(baseline).feature());
        }
        if end.drain.is_some_and(|drain| drain.exhausted) {
            map.hit(StateFeature::IncompleteDrain.feature());
        }
    }
}

/// A frame feature.
fn frame(row: Row, direction: Direction, label: FrameLabel, stage: FrameStage) -> StateFeature {
    StateFeature::Frame {
        row,
        direction,
        label,
        stage,
    }
}

/// The service a stream kind serves.
fn stream_service(kind: u16) -> StreamService {
    match kind {
        ZAKURA_STREAM_HEADER_SYNC => StreamService::HeaderSync,
        ZAKURA_STREAM_BLOCK_SYNC => StreamService::BlockSync,
        _ => StreamService::Other,
    }
}

/// The owner and body-work epoch of a block operation that names one.
fn block_epoch(request: &StorageRequest) -> Option<(EpochWork, u64)> {
    match request {
        StorageRequest::QueryNeededBlocks { scope, .. } => {
            Some((EpochWork::Query, scope.body_work_epoch))
        }
        StorageRequest::SubmitBlock { owner, .. } => {
            Some((EpochWork::Submit, owner.body_work_epoch?))
        }
        _ => None,
    }
}

/// Whether `service` delivers `request`'s reply to a waiting caller, so a
/// completion that isn't delivered found none. Block storage sends an
/// action's reply to block sync's handle, which `HandleClosed` shows, and
/// a record's reply to nobody. Only its range read has a caller.
fn replies_to_caller(service: Service, request: &StorageRequest) -> bool {
    match service {
        Service::Header => true,
        Service::Block => matches!(request, StorageRequest::ReadRange { .. }),
    }
}

/// The work owner a header operation names.
fn header_owner(request: &StorageRequest) -> Option<&WorkOwner> {
    match request {
        StorageRequest::VctRepairContext { owner, .. }
        | StorageRequest::PrepareHeaderTarget { owner, .. }
        | StorageRequest::ApplyHeaderTarget { owner, .. } => Some(owner),
        _ => None,
    }
}

impl StateCollector {
    /// Reads the observations of every step from the first unread one
    /// through `last`.
    fn read_through(&mut self, log: &ObservationLog, last: u64, map: FeatureMap) {
        for step in self.read..=last {
            for observation in log.of_step(step) {
                self.observe(observation, &mut |state| map.hit(state.feature()));
            }
        }
        self.read = self.read.max(last + 1);
    }

    /// Reads one observation, passing each feature it reaches to `hit`.
    fn observe(&mut self, observation: &Observation, hit: &mut dyn FnMut(StateFeature)) {
        match observation {
            Observation::Storage {
                service,
                instance,
                fact,
                ..
            } => {
                let service = match service.as_str() {
                    HEADER_SERVICE => Service::Header,
                    BLOCK_SERVICE => Service::Block,
                    _ => return,
                };
                self.storage(service, *instance, fact, hit);
            }
            Observation::Reactor {
                instance, event, ..
            } => self.reactor(*instance, event, hit),
            Observation::BlockSync { event, .. } => self.block_sync(event, hit),
            Observation::PeerFrame {
                connection,
                stream,
                quic_stream,
                seq,
                row,
                rule_stream,
                label,
                exhausted,
                ..
            } => {
                let row = Row::of(*rule_stream, row.as_deref());
                let label = FrameLabel::of(*label);
                hit(frame(row, Direction::ToNode, label, FrameStage::Sent));
                if *exhausted {
                    hit(StateFeature::CadenceEmptied {
                        row,
                        direction: Direction::ToNode,
                    });
                }
                self.sent
                    .insert((*connection, *stream, *quic_stream, *seq), (row, label));
            }
            Observation::NodeFrame {
                row,
                rule_stream,
                label,
                exhausted,
                ..
            } => {
                let row = Row::of(*rule_stream, row.as_deref());
                hit(frame(
                    row,
                    Direction::FromNode,
                    FrameLabel::of(*label),
                    FrameStage::Sent,
                ));
                if *exhausted {
                    hit(StateFeature::CadenceEmptied {
                        row,
                        direction: Direction::FromNode,
                    });
                }
            }
            Observation::FrameJudged {
                connection,
                stream,
                quic_stream,
                seq,
            } => {
                if let Some(&(row, label)) =
                    self.sent.get(&(*connection, *stream, *quic_stream, *seq))
                {
                    hit(frame(row, Direction::ToNode, label, FrameStage::Judged));
                    self.last_judged.insert(*connection, (row, label));
                    self.judged
                        .entry(*connection)
                        .or_default()
                        .insert((row, label));
                }
                if stream.kind == ZAKURA_STREAM_BLOCK_SYNC {
                    self.body_judged(*connection, *seq, hit);
                }
                if let Some(&(kind, service)) = quic_stream
                    .and_then(|quic_stream| self.replaced.get(&(*connection, quic_stream)))
                {
                    hit(StateFeature::Replacement {
                        kind,
                        service,
                        event: ReplacementEvent::OldTrafficAfter,
                    });
                }
            }
            Observation::PeerBody {
                connection,
                seq,
                height,
                ..
            } => {
                self.bodies.insert((*connection, *seq), *height);
            }
            Observation::ConnectionClosed { connection, cause } => {
                self.closed.insert(*connection);
                if let Some(&(row, label)) = self.last_judged.get(connection) {
                    let stage = match is_fault_cause(cause) {
                        true => FrameStage::FaultCloseAfter,
                        false => FrameStage::OtherCloseAfter,
                    };
                    hit(frame(row, Direction::ToNode, label, stage));
                }
            }
            Observation::Misbehavior { connection } => {
                if let Some(&(row, label)) = self.last_judged.get(connection) {
                    hit(frame(
                        row,
                        Direction::ToNode,
                        label,
                        FrameStage::MisbehaviorAfter,
                    ));
                }
            }
            Observation::Transport {
                connection, event, ..
            } => self.transport(*connection, event, hit),
            Observation::Session {
                connection,
                peer,
                quic_stream,
                kind,
                session_id,
                ..
            } => self.session(*connection, peer, *quic_stream, *kind, *session_id, hit),
            _ => {}
        }
    }

    /// Reads one storage fact.
    fn storage(
        &mut self,
        service: Service,
        instance: u64,
        fact: &StorageFact,
        hit: &mut dyn FnMut(StateFeature),
    ) {
        let key = |id: u64| (service, instance, id);
        match fact {
            StorageFact::Submitted { id, request, .. } => {
                self.submitted(key(*id), request, None, hit);
            }
            StorageFact::BlockSubmitted { id, request, stage } => {
                let stage = stage.as_deref().map(BlockStage::of);
                if let StorageRequest::QueryNeededBlocks { query_id, .. } = request {
                    self.queries.insert((instance, *query_id), *id);
                }
                self.submitted(key(*id), request, stage, hit);
            }
            StorageFact::Admitted { id } => {
                if let Some(op) = self.ops.get_mut(&key(*id)) {
                    op.stage = Some(BlockStage::Admitted);
                }
            }
            StorageFact::CallerDropped { id } => {
                if let Some(op) = self.ops.get_mut(&key(*id)) {
                    op.caller_dropped = true;
                }
            }
            StorageFact::Completed {
                id,
                result,
                delivered,
                ..
            }
            | StorageFact::BlockCompleted {
                id,
                result,
                delivered,
                ..
            } => self.completed(key(*id), result, *delivered, hit),
            StorageFact::ReplyTaken { id } | StorageFact::ReadTaken { id } => {
                self.disposed(key(*id), Disposition::Taken, hit);
            }
            StorageFact::ReplyDropped { id } | StorageFact::ReadDropped { id } => {
                self.disposed(key(*id), Disposition::Dropped, hit);
            }
            StorageFact::ReplyDelivered { id } => {
                self.disposed(key(*id), Disposition::Delivered, hit);
            }
            StorageFact::HandleClosed { id } => {
                self.disposed(key(*id), Disposition::HandleClosed, hit);
            }
            StorageFact::Coalesced { query_id } => {
                if let Some(&id) = self.queries.get(&(instance, *query_id)) {
                    self.disposed(key(id), Disposition::Coalesced, hit);
                }
            }
            StorageFact::DriverExited { dropped, deferred } => {
                self.acknowledged(
                    service,
                    instance,
                    Acknowledgement::DriverExited,
                    dropped,
                    deferred.is_empty(),
                    hit,
                );
            }
            StorageFact::ShutDown { dropped, deferred } => {
                self.acknowledged(
                    service,
                    instance,
                    Acknowledgement::ShutDown,
                    dropped,
                    deferred.is_empty(),
                    hit,
                );
            }
            StorageFact::WaiterFired { class, .. } => hit(StateFeature::HeaderCapacity {
                event: CapacityEvent::WaiterFired,
                class: CapacityClass::of(class),
            }),
            StorageFact::TransitionInstalled { cause, .. } => {
                if cause.as_deref() == Some("reset") {
                    hit(StateFeature::Fork(Fork::VerifiedReset));
                }
            }
            StorageFact::ViewPublished { epoch, .. } => self.epoch(*epoch),
            _ => {}
        }
    }

    /// Records a submitted operation, and hits the forks and reorg stages
    /// its request shows.
    fn submitted(
        &mut self,
        key: OpKey,
        request: &StorageRequest,
        stage: Option<BlockStage>,
        hit: &mut dyn FnMut(StateFeature),
    ) {
        if let Some((work, epoch)) = block_epoch(request) {
            self.epoch(epoch);
            if self.current_after_advance(epoch) {
                hit(StateFeature::ReorgStage(match work {
                    EpochWork::Query => ReorgStage::Query,
                    EpochWork::Submit => ReorgStage::Submit,
                }));
            }
        }
        if let StorageRequest::PrepareHeaderTarget {
            owner,
            common_ancestor,
            target,
            ..
        } = request
        {
            let shared = self.prepared.iter().any(|earlier| {
                earlier.ancestor == *common_ancestor && earlier.target.hash != target.hash
            });
            if shared {
                hit(StateFeature::Fork(Fork::SharedAncestorPrepared));
            }
            self.prepared.push(Prepared {
                owner: owner.clone(),
                ancestor: common_ancestor.clone(),
                target: target.clone(),
            });
        }
        self.ops.insert(
            key,
            Op {
                request: request.clone(),
                stage,
                caller_dropped: false,
            },
        );
    }

    /// Hits a completion's timing and the states its result shows.
    fn completed(
        &mut self,
        key: OpKey,
        result: &StorageResult,
        delivered: bool,
        hit: &mut dyn FnMut(StateFeature),
    ) {
        let Some(op) = self.ops.get(&key) else {
            return;
        };
        let service = key.0;
        let kind = Operation::of(&op.request);
        let retired = header_owner(&op.request).is_some_and(|owner| self.retired.contains(owner));
        let timings = [
            (op.caller_dropped, Timing::AfterCancelRequest),
            (retired, Timing::AfterOwnerRetired),
            (!op.caller_dropped && !retired, Timing::CallerWaiting),
        ];
        for (_, timing) in timings.into_iter().filter(|(applies, _)| *applies) {
            hit(StateFeature::Completion {
                service,
                op: kind,
                timing,
            });
        }
        if !delivered && replies_to_caller(service, &op.request) {
            hit(StateFeature::Disposition {
                service,
                op: kind,
                disposition: Disposition::Undelivered,
            });
        }
        if let (Some((work, epoch)), Some(stage)) = (block_epoch(&op.request), op.stage) {
            let relation = match self.epochs {
                Some((_, newest)) if epoch < newest => Epoch::Stale,
                _ => Epoch::Current,
            };
            hit(StateFeature::BlockOpCompleted {
                op: work,
                stage,
                epoch: relation,
            });
            if let (EpochWork::Submit, StorageResult::Verdict { reply, .. }) = (work, result) {
                if self.current_after_advance(epoch) {
                    hit(StateFeature::ReorgStage(match reply.as_str() {
                        "committed" => ReorgStage::VerdictCommitted,
                        _ => ReorgStage::VerdictOther,
                    }));
                }
            }
        }
        match result {
            StorageResult::CapacityBusy { class } => hit(StateFeature::HeaderCapacity {
                event: CapacityEvent::Denied,
                class: CapacityClass::of(class),
            }),
            StorageResult::Locator { tip, .. } => self.located(tip, hit),
            StorageResult::Applied => self.applied(key, hit),
            _ => {}
        }
    }

    /// Moves the selected tip to a locator's `tip`, and hits a header reorg
    /// if `tip` is a reorg's target.
    fn located(&mut self, tip: &StoragePoint, hit: &mut dyn FnMut(StateFeature)) {
        self.selected_tip = Some(tip.height);
        if let Some(at) = self.reorg_targets.iter().position(|target| target == tip) {
            self.reorg_targets.remove(at);
            hit(StateFeature::Fork(Fork::HeaderReorgApplied));
        }
    }

    /// Hits the forks an applied header target shows, and keeps its target
    /// if its common ancestor lies below the selected tip.
    fn applied(&mut self, key: OpKey, hit: &mut dyn FnMut(StateFeature)) {
        let Some(StorageRequest::ApplyHeaderTarget { owner, target, .. }) =
            self.ops.get(&key).map(|op| &op.request)
        else {
            return;
        };
        if self.active.values().any(|active| active.target != *target) {
            hit(StateFeature::Fork(Fork::WinnerApplied));
        }
        let Some(prepared) = self.prepared.iter().rev().find(|p| p.owner == *owner) else {
            return;
        };
        if self
            .selected_tip
            .is_some_and(|tip| prepared.ancestor.height < tip)
        {
            self.reorg_targets.push(prepared.target.clone());
        }
    }

    /// Hits what became of operation `key`.
    fn disposed(&self, key: OpKey, disposition: Disposition, hit: &mut dyn FnMut(StateFeature)) {
        if let Some(op) = self.ops.get(&key) {
            hit(StateFeature::Disposition {
                service: key.0,
                op: Operation::of(&op.request),
                disposition,
            });
        }
    }

    /// Hits a storage service's acknowledgement, and the disposition of each
    /// operation it dropped.
    fn acknowledged(
        &self,
        service: Service,
        instance: u64,
        how: Acknowledgement,
        dropped: &[u64],
        no_deferred: bool,
        hit: &mut dyn FnMut(StateFeature),
    ) {
        let disposition = match how {
            Acknowledgement::DriverExited => Disposition::DriverExited,
            Acknowledgement::ShutDown => Disposition::ShutDown,
        };
        for id in dropped {
            self.disposed((service, instance, *id), disposition, hit);
        }
        let dropped = match dropped.is_empty() && no_deferred {
            true => Dropped::None,
            false => Dropped::Some,
        };
        hit(StateFeature::Acknowledged {
            service,
            how,
            dropped,
        });
    }

    /// Reads one header reactor event.
    fn reactor(&mut self, instance: u64, event: &ReactorFact, hit: &mut dyn FnMut(StateFeature)) {
        match event {
            ReactorFact::Dispatched { call, port } => {
                let kind = match port {
                    ReactorCall::Locator { .. } => Call::Locator,
                    ReactorCall::VctContext { .. } => Call::VctContext,
                    ReactorCall::Prepare { .. } => Call::Prepare,
                    ReactorCall::Apply { .. } => Call::Apply,
                };
                self.calls.insert((instance, *call), kind);
            }
            ReactorFact::CallEnded { call, end, .. } => {
                if let Some(&kind) = self.calls.get(&(instance, *call)) {
                    hit(StateFeature::CallEnded {
                        call: kind,
                        end: CallEnd::of(end),
                    });
                }
            }
            ReactorFact::Started { request, .. } => {
                let distinct = self.active.iter().any(|(&(at, ..), active)| {
                    at == instance && active.peer != request.peer && active.target != request.target
                });
                if distinct {
                    hit(StateFeature::Fork(Fork::DistinctOffersActive));
                }
                self.active.insert(
                    (instance, request.session_id, request.request_id),
                    Active {
                        peer: request.peer.clone(),
                        target: request.target.clone(),
                    },
                );
            }
            ReactorFact::Retired {
                request,
                phase,
                purpose,
                reason,
            } => {
                self.active
                    .remove(&(instance, request.session_id, request.request_id));
                self.retired.push(request.owner.clone());
                let purpose = match purpose {
                    ReactorPurpose::Normal => Purpose::Normal,
                    ReactorPurpose::SelectedAuxiliaryRepair { .. } => Purpose::AuxiliaryRepair,
                };
                hit(StateFeature::HeaderRetired {
                    purpose,
                    phase: RetiredPhase::of(phase),
                    reason: RetireReason::of(reason),
                });
            }
            _ => {}
        }
    }

    /// Reads one block-sync event.
    fn block_sync(&mut self, event: &BlockSyncFact, hit: &mut dyn FnMut(StateFeature)) {
        match event {
            BlockSyncFact::SessionAdmitted {
                peer,
                connection,
                session_id,
                ..
            } => {
                self.block_connections.insert(*connection, *session_id);
                if let Some(old) = self.block_sessions.insert(peer.clone(), *connection) {
                    let routine = self.routines.get(&old).copied().unwrap_or_default();
                    let event = match routine {
                        Routine { ended: true, .. } => ReplacementEvent::OldEnded,
                        Routine { busy: true, .. } => ReplacementEvent::OldLiveBusy,
                        Routine { .. } => ReplacementEvent::OldLiveIdle,
                    };
                    hit(StateFeature::Replacement {
                        kind: ReplacementKind::Connection,
                        service: StreamService::BlockSync,
                        event,
                    });
                    self.replaced_block.insert(old);
                }
            }
            BlockSyncFact::FrameJudged { connection, .. } => {
                if self.replaced_block.contains(connection) {
                    hit(StateFeature::Replacement {
                        kind: ReplacementKind::Connection,
                        service: StreamService::BlockSync,
                        event: ReplacementEvent::OldTrafficAfter,
                    });
                }
            }
            BlockSyncFact::Request {
                owner,
                heights,
                step,
                ..
            } => {
                let Some(epoch) = owner.body_work_epoch else {
                    return;
                };
                self.epoch(epoch);
                let key = (owner.session_id, owner.request_id);
                let progress = match step.as_str() {
                    "published" => Some(RequestProgress::Published),
                    "started" => Some(RequestProgress::Started),
                    "written" => Some(RequestProgress::Written),
                    _ => None,
                };
                match progress {
                    Some(progress) => {
                        self.progress.insert(key, progress);
                        if progress == RequestProgress::Published {
                            self.outstanding.insert(key, (epoch, heights.clone()));
                            if self.current_after_advance(epoch) {
                                hit(StateFeature::ReorgStage(ReorgStage::Request));
                            }
                        }
                    }
                    None => {
                        if step != RequestEvent::DroppedWritten.name() {
                            self.outstanding.remove(&key);
                        }
                        hit(StateFeature::BlockRequestStep {
                            last: self
                                .progress
                                .get(&key)
                                .copied()
                                .unwrap_or(RequestProgress::None),
                            step: RequestEvent::of(step),
                        });
                    }
                }
            }
            BlockSyncFact::Budget {
                change,
                before,
                after,
                limit,
            } => {
                self.budget = Some(*after);
                hit(StateFeature::BlockBudget {
                    change: BudgetChange::of(change),
                    before: Level::of(*before, *limit),
                    after: Level::of(*after, *limit),
                });
            }
            BlockSyncFact::HandoffStarted { owner, .. } => {
                if owner
                    .body_work_epoch
                    .is_some_and(|epoch| self.current_after_advance(epoch))
                {
                    hit(StateFeature::ReorgStage(ReorgStage::Handoff));
                }
            }
            BlockSyncFact::Holder {
                body,
                holder,
                change,
            } => {
                let key = serde_json::to_string(&(body, holder))
                    .expect("a body and its holder serialize");
                let count = self.holders.entry(key).or_default();
                match change.as_str() {
                    "retained" => *count += 1,
                    _ => *count -= 1,
                }
            }
            BlockSyncFact::Routine {
                connection,
                requests,
                ended,
                ..
            } => {
                let before = self.routines.insert(
                    *connection,
                    Routine {
                        ended: *ended,
                        busy: !requests.is_empty(),
                    },
                );
                if *ended && !before.is_some_and(|routine| routine.ended) {
                    hit(StateFeature::Retired(Retirement::RoutineEnded));
                }
            }
            BlockSyncFact::SequencerClosed => {
                self.sequencer_closed = true;
                hit(StateFeature::Retired(Retirement::SequencerClosed));
            }
            _ => {}
        }
    }

    /// Reads the node's verdict on a peer's block frame: its body arrived.
    /// The body reaches the receive stage when it answers a request its
    /// session still holds, for the newest body-work epoch after an advance.
    fn body_judged(&mut self, connection: u64, seq: u64, hit: &mut dyn FnMut(StateFeature)) {
        let Some(height) = self.bodies.remove(&(connection, seq)) else {
            return;
        };
        let Some(&session) = self.block_connections.get(&connection) else {
            return;
        };
        let answered = self
            .outstanding
            .range((session, 0)..=(session, u64::MAX))
            .any(|(_, (epoch, heights))| {
                heights.contains(&height) && self.current_after_advance(*epoch)
            });
        if answered {
            hit(StateFeature::ReorgStage(ReorgStage::Receive));
        }
    }

    /// Reads one transport event.
    fn transport(
        &mut self,
        connection: Option<u64>,
        event: &TransportFact,
        hit: &mut dyn FnMut(StateFeature),
    ) {
        match event {
            TransportFact::TornDown => {
                if let Some(connection) = connection {
                    self.torn_down.insert(connection);
                }
                hit(StateFeature::Retired(Retirement::ConnectionTornDown));
            }
            TransportFact::Regulated { transition, .. } => {
                hit(StateFeature::Transition(match transition {
                    TransitionFact::OpenIssued { .. } => Transition::OpenIssued,
                    TransitionFact::OpenRetracted { .. } => Transition::OpenRetracted,
                    TransitionFact::GrantIssued { .. } => Transition::GrantIssued,
                    TransitionFact::OpenApplied { .. } => Transition::OpenApplied,
                    TransitionFact::CloseApplied { .. } => Transition::CloseApplied,
                    TransitionFact::GrantApplied { .. } => Transition::GrantApplied,
                    TransitionFact::PageClaimed { .. } => Transition::PageClaimed,
                    TransitionFact::EndClaimed { .. } => Transition::EndClaimed,
                    TransitionFact::PublicationEnded { .. } => Transition::PublicationEnded,
                    TransitionFact::HeaderChecked { outcome, .. } => match outcome {
                        HeaderCheckFact::Reserved { .. } => Transition::HeaderReserved,
                        HeaderCheckFact::AdmittedTerminal => Transition::HeaderTerminal,
                        HeaderCheckFact::Refused { reason } => Transition::refused(reason),
                    },
                }));
            }
            _ => {}
        }
    }

    /// Reads one admitted service session: a new session of a kind the peer
    /// already had replaces the old one.
    fn session(
        &mut self,
        connection: u64,
        peer: &str,
        quic_stream: u64,
        kind: u16,
        session_id: u64,
        hit: &mut dyn FnMut(StateFeature),
    ) {
        let new = Session {
            connection,
            quic_stream,
            session_id,
        };
        let Some(old) = self.sessions.insert((peer.to_owned(), kind), new) else {
            return;
        };
        let replacement = match old.connection == connection {
            true => ReplacementKind::Stream,
            false => ReplacementKind::Connection,
        };
        let busy = self
            .active
            .keys()
            .any(|&(_, session, _)| session == old.session_id);
        let event = match (self.torn_down.contains(&old.connection), busy) {
            (true, _) => ReplacementEvent::OldEnded,
            (false, true) => ReplacementEvent::OldLiveBusy,
            (false, false) => ReplacementEvent::OldLiveIdle,
        };
        let service = stream_service(kind);
        hit(StateFeature::Replacement {
            kind: replacement,
            service,
            event,
        });
        self.replaced
            .insert((old.connection, old.quic_stream), (replacement, service));
    }

    /// Notes that block sync's work named body-work `epoch`.
    fn epoch(&mut self, epoch: u64) {
        match &mut self.epochs {
            Some((_, newest)) => *newest = (*newest).max(epoch),
            None => self.epochs = Some((epoch, epoch)),
        }
    }

    /// True if block sync's body-work epoch has advanced since its first
    /// work, and `epoch` is the newest.
    fn current_after_advance(&self, epoch: u64) -> bool {
        self.epochs
            .is_some_and(|(first, newest)| newest > first && epoch == newest)
    }
}
