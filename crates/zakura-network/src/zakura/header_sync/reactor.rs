use std::{
    cell::Cell,
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    num::NonZeroU64,
    panic::AssertUnwindSafe,
    pin::Pin,
};

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use iroh::NodeId;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{self, Instant},
};
use zakura_chain::block;

use super::{
    events::{HeaderPortOperation, PortDispatch},
    scheduler::{
        completed_targets::CompletedHeaderTargets,
        peer_work::{HeaderTargetPhase, PeerWorkPriority, PeerWorkQueue, QueueWorkResult},
        repair::{RepairPolicyState, RepairRequirement, RepairRequirementSlot},
        status::StatusPublisher,
    },
    *,
};
use crate::zakura::{
    trace::{
        header_sync_trace as hs_trace, peer_label as trace_peer_label,
        queue_send_trace as qs_trace, HEADER_SYNC_TABLE, QUEUE_SEND_TABLE,
    },
    OrderedSendError, ServicePeerDirection, ServicePeerSnapshot, ZakuraHeaderSyncCandidateState,
    ZakuraPeerId,
};

const INTERNAL_VCT_REPAIR_SESSION_ID: u64 = u64::MAX;
const LEASE_RELEASE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const VCT_REPAIR_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
/// Minimum interval between unchanged header-snapshot refresh trace rows.
///
/// The trace records every frontier advance and reanchor.
/// Metrics and the committed snapshot remain exact.
/// The trace samples identical refresh diagnostics to bound long-running JSONL traces.
const SNAPSHOT_REFRESH_TRACE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

fn snapshot_refresh_trace_due(last: Option<Instant>, now: Instant) -> bool {
    last.is_none_or(|last| now.saturating_duration_since(last) >= SNAPSHOT_REFRESH_TRACE_INTERVAL)
}
/// Keep one maximum wire page ahead of integrated full state, then refill at half-window low water.
///
/// Each integrated full-state advance reanchors the durable header DAG.
/// The bound keeps initial-sync consensus transitions proportional to pipeline work.
/// Half-window refills overlap proof validation and durable admission with body application.
/// The refills also preserve enough work for a partial checkpoint range.
const INTEGRATED_HEADER_BODY_WINDOW_V1: u32 = MAX_HS_RANGE;
const INTEGRATED_HEADER_REFILL_LOW_WATER_V1: u32 = INTEGRATED_HEADER_BODY_WINDOW_V1 / 2;

/// Spawn the canonical header-sync reactor.
pub fn spawn_header_sync_reactor(
    startup: HeaderSyncStartup,
) -> Result<
    (
        HeaderSyncHandle,
        mpsc::Receiver<HeaderPortOperation>,
        JoinHandle<()>,
    ),
    HeaderSyncStartError,
> {
    let (handle, actions, reactor) = build_header_sync_reactor(startup)?;
    Ok((handle, actions, tokio::spawn(reactor.run())))
}

fn build_header_sync_reactor(
    mut startup: HeaderSyncStartup,
) -> Result<
    (
        HeaderSyncHandle,
        mpsc::Receiver<HeaderPortOperation>,
        HeaderSyncReactor,
    ),
    HeaderSyncStartError,
> {
    let committed_snapshot = startup
        .committed_snapshots
        .as_ref()
        .and_then(|snapshots| snapshots.borrow().clone());
    if let Some(snapshot) = committed_snapshot.as_ref() {
        startup.frontiers = FullStateFrontiers {
            finalized_height: snapshot.frontiers.finalized.height,
            verified_block_tip: snapshot.frontiers.verified_best.height,
            verified_block_hash: snapshot.frontiers.verified_best.hash,
        };
        startup.best_header_tip = Some((
            snapshot.frontiers.header_best.height,
            snapshot.frontiers.header_best.hash,
        ));
    }
    if startup.anchor.0 > startup.frontiers.verified_block_tip {
        return Err(HeaderSyncStartError::AnchorAboveVerifiedBlockTip {
            anchor_height: startup.anchor.0,
            verified_block_tip: startup.frontiers.verified_block_tip,
        });
    }

    let (events_tx, events_rx) = mpsc::channel(128);
    let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
    let (actions_tx, actions_rx) = mpsc::channel(128);
    let initial_tip = startup.best_header_tip.unwrap_or(startup.anchor);
    let (tip_tx, tip_rx) = watch::channel(initial_tip);
    let (peers_tx, peers_rx) =
        watch::channel(ServicePeerSnapshot::new(0, 0, startup.config.peer_limits));
    let (candidates_tx, candidates_rx) = watch::channel(ZakuraHeaderSyncCandidateState {
        target_height: next_height(initial_tip.0),
        admitted_node_ids: Vec::new(),
        backed_off_node_ids: Vec::new(),
    });

    let max_message_bytes = startup
        .max_frame_bytes
        .saturating_sub(FRAME_HEADER_BYTES as u32)
        .min(MAX_HS_MESSAGE_BYTES as u32)
        .max(1);
    let serving_limits = HeaderServingLimits::new(
        startup.config.advertised_max_headers_per_response(),
        startup.config.advertised_max_inflight_requests(),
        max_message_bytes,
        AuxSchema::V1.mask_bit(),
    )
    .expect("clamped header-sync serving limits are nonzero");
    let codec = HeaderSyncCodec::new(
        startup.network.clone(),
        max_message_bytes,
        serving_limits.max_headers_per_response(),
        serving_limits.tree_aux_schema_mask(),
    );
    let vct_repair_status = startup
        .vct_root_repairs
        .as_ref()
        .map_or_else(Default::default, |repairs| *repairs.borrow());
    let handle = HeaderSyncHandle {
        events: events_tx,
        lifecycle: lifecycle_tx,
        tip: tip_rx,
        peers: peers_rx,
        candidates: candidates_rx,
        codec: codec.clone(),
        trace: startup.trace.clone(),
    };
    let mut reactor = HeaderSyncReactor {
        startup,
        events: events_rx,
        lifecycle: lifecycle_rx,
        actions: actions_tx,
        pending_port_operations: FuturesUnordered::new(),
        pending_locator_queries: HashSet::new(),
        retained_paths: HashMap::new(),
        tip: tip_tx,
        peers: peers_tx,
        candidates: candidates_tx,
        codec,
        serving_limits,
        committed_snapshot,
        vct_repair_status,
        peer_state: HashMap::new(),
        unproductive_peer_cooldowns: HashMap::new(),
        peer_work_queue: PeerWorkQueue::default(),
        request_deadlines: HashMap::new(),
        completed_targets: CompletedHeaderTargets::default(),
        vct_repair: RepairRequirementSlot::default(),
        served_paths: HashMap::new(),
        served_path_deadlines: HashMap::new(),
        pending_lease_releases: VecDeque::new(),
        lease_release_retry_at: None,
        last_snapshot_refresh_trace_at: Cell::new(None),
    };
    if let Some(snapshot) = reactor.committed_snapshot.as_ref() {
        reactor.emit_snapshot_observed(None, snapshot);
    }
    reactor.schedule_current_vct_repair();
    Ok((handle, actions_rx, reactor))
}

#[derive(Debug)]
struct PeerState {
    session: PeerSession,
    status_publisher: Option<StatusPublisher>,
    last_status: Option<Status>,
    /// Consecutive requests this session answered with nothing usable.
    unproductive_requests: u32,
}

#[derive(Copy, Clone, Debug)]
struct PendingServedRequest {
    request_id: HeaderSyncRequestId,
    max_header_count: u32,
    tree_aux_schema: AuxSchema,
}

#[derive(Debug)]
enum ServedPathState {
    Acquiring {
        session_id: u64,
        request_id: HeaderSyncRequestId,
        target_tip_hash: block::Hash,
        scope: zakura_header_chain::HeaderWorkAuthority,
    },
    Active {
        session_id: u64,
        lease_id: u64,
        target: zakura_header_chain::Frontier,
        scope: zakura_header_chain::HeaderWorkAuthority,
        next_after: zakura_header_chain::Frontier,
        pending_request: Option<PendingServedRequest>,
    },
}

#[derive(Clone, Debug)]
struct PendingLeaseRelease {
    peer: ZakuraPeerId,
    session_id: u64,
    lease_id: u64,
    scope: zakura_header_chain::HeaderWorkAuthority,
}

impl PendingLeaseRelease {
    fn action(&self) -> HeaderPortOperation {
        HeaderPortOperation::ReleaseHeaderPath {
            peer: self.peer.clone(),
            session_id: self.session_id,
            lease_id: self.lease_id,
            scope: self.scope,
        }
    }
}

#[derive(Debug)]
struct HeaderSyncReactor {
    startup: HeaderSyncStartup,
    events: mpsc::Receiver<Event>,
    lifecycle: mpsc::UnboundedReceiver<Event>,
    #[cfg_attr(not(any(test, feature = "zakura-testkit")), allow(dead_code))]
    actions: mpsc::Sender<HeaderPortOperation>,
    pending_port_operations: FuturesUnordered<PendingPortOperation>,
    /// Peers with one direct continuation-locator read currently in flight.
    pending_locator_queries: HashSet<ZakuraPeerId>,
    #[cfg_attr(any(test, feature = "zakura-testkit"), allow(dead_code))]
    retained_paths: HashMap<u64, zakura_node_services::header_chain::RetainedHeaderPath>,
    tip: watch::Sender<(block::Height, block::Hash)>,
    peers: watch::Sender<ServicePeerSnapshot>,
    candidates: watch::Sender<ZakuraHeaderSyncCandidateState>,
    codec: HeaderSyncCodec,
    serving_limits: HeaderServingLimits,
    committed_snapshot: Option<zakura_header_chain::EngineSnapshot>,
    vct_repair_status: zakura_header_chain::VctRootRepairStatus,
    peer_state: HashMap<ZakuraPeerId, PeerState>,
    /// Deadlines until which the reactor refuses header-sync readmission to dropped peers.
    ///
    /// Each entry represents one admitted session that survived
    /// [`ZakuraHeaderSyncConfig::max_unproductive_header_requests`] request timeouts.
    /// The reactor prunes expired entries during insertion and maintenance.
    unproductive_peer_cooldowns: HashMap<ZakuraPeerId, Instant>,
    peer_work_queue: PeerWorkQueue,
    request_deadlines: HashMap<ZakuraPeerId, Instant>,
    completed_targets: CompletedHeaderTargets,
    vct_repair: RepairRequirementSlot,
    served_paths: HashMap<ZakuraPeerId, ServedPathState>,
    served_path_deadlines: HashMap<ZakuraPeerId, Instant>,
    pending_lease_releases: VecDeque<PendingLeaseRelease>,
    lease_release_retry_at: Option<Instant>,
    last_snapshot_refresh_trace_at: Cell<Option<Instant>>,
}

type PendingPortOperation = Pin<Box<dyn Future<Output = PortOperationResult> + Send + 'static>>;

type HeaderSyncPortCompletion = Box<dyn FnOnce(&mut HeaderSyncReactor) + Send + 'static>;

enum PortOperationResult {
    Completed(HeaderSyncPortCompletion),
    Panicked(Box<PortPanicContext>),
}

#[derive(Clone, Debug)]
struct PortPanicContext {
    operation: &'static str,
    peer: Option<ZakuraPeerId>,
    session_id: Option<u64>,
    session: Option<PeerSession>,
    scope: Option<zakura_header_chain::HeaderWorkAuthority>,
    owner: Option<zakura_header_chain::HeaderSyncWorkOwner>,
    target_tip_hash: Option<block::Hash>,
    lease_id: Option<u64>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum HeaderRequestTerminal {
    TargetNotRetained,
    NoLocatorIntersection,
    HistoryPruned,
    Busy,
    AlreadyKnown,
    Disconnected,
    LocalError,
    MalformedResponse,
    RepairObsolete,
    SendError,
    SessionReplaced,
    Shutdown,
    SnapshotObsolete,
    StagingRefused,
    TargetAdmitted,
    TargetRejected,
    TimedOut,
}

impl HeaderRequestTerminal {
    fn needs_terminal_trace(self) -> bool {
        !matches!(
            self,
            Self::Disconnected
                | Self::SessionReplaced
                | Self::SnapshotObsolete
                | Self::TargetAdmitted
                | Self::TargetRejected
        )
    }

    fn label(self) -> &'static str {
        match self {
            Self::TargetNotRetained => "target_not_retained",
            Self::NoLocatorIntersection => "no_locator_intersection",
            Self::HistoryPruned => "history_pruned",
            Self::Busy => "busy",
            Self::AlreadyKnown => "already_known",
            Self::Disconnected => "disconnected",
            Self::LocalError => "local_error",
            Self::MalformedResponse => "malformed_response",
            Self::RepairObsolete => "repair_obsolete",
            Self::SendError => "send_error",
            Self::SessionReplaced => "session_replaced",
            Self::Shutdown => "shutdown",
            Self::SnapshotObsolete => "snapshot_obsolete",
            Self::StagingRefused => "staging_refused",
            Self::TargetAdmitted => "target_admitted",
            Self::TargetRejected => "target_rejected",
            Self::TimedOut => "timed_out",
        }
    }
}

impl From<HeadersOutcomeCode> for HeaderRequestTerminal {
    fn from(outcome: HeadersOutcomeCode) -> Self {
        match outcome {
            HeadersOutcomeCode::TargetNotRetained => Self::TargetNotRetained,
            HeadersOutcomeCode::NoLocatorIntersection => Self::NoLocatorIntersection,
            HeadersOutcomeCode::HistoryPruned => Self::HistoryPruned,
            HeadersOutcomeCode::Busy => Self::Busy,
        }
    }
}

fn vct_repair_task(
    snapshot: &zakura_header_chain::EngineSnapshot,
    status: zakura_header_chain::VctRootRepairStatus,
) -> Option<RepairRequirement> {
    let zakura_header_chain::VctRootRepairState::Unavailable { height } = status.state else {
        return None;
    };
    if height <= snapshot.frontiers.finalized.height
        || height > snapshot.frontiers.header_best.height
    {
        return None;
    }
    let request_id = status.generation.checked_add(1).and_then(NonZeroU64::new)?;
    let scope = zakura_header_chain::BodyWorkAuthority::for_snapshot(snapshot);
    let owner = scope.bind(INTERNAL_VCT_REPAIR_SESSION_ID, request_id);
    Some(RepairRequirement::new(owner, height, status.generation))
}

#[cfg_attr(any(test, feature = "zakura-testkit"), allow(dead_code))]
fn port_header_entry(entry: HeaderEntry) -> zakura_node_services::header_chain::TargetEntry {
    zakura_node_services::header_chain::TargetEntry {
        header: entry.header,
        body_size: entry.body_size,
        tree_aux: entry.tree_aux,
    }
}

#[cfg_attr(any(test, feature = "zakura-testkit"), allow(dead_code))]
fn assemble_port_header_path_page(
    lease_id: u64,
    page: zakura_node_services::header_chain::RetainedHeaderPathPage,
    requested_schema: AuxSchema,
) -> Option<HeaderPathPage> {
    if page.headers.len() != page.aux_deliveries.len()
        || page.headers.len() != page.finalized_tree_aux.len()
    {
        return None;
    }
    let tree_aux_schema = if requested_schema == AuxSchema::V1
        && page
            .aux_deliveries
            .iter()
            .zip(&page.finalized_tree_aux)
            .all(|(deliveries, finalized_tree_aux)| {
                finalized_tree_aux.is_some()
                    || selected_port_aux_delivery(deliveries, AuxSchema::V1).is_some()
            }) {
        AuxSchema::V1
    } else {
        AuxSchema::None
    };
    let entries = page
        .headers
        .into_iter()
        .zip(page.aux_deliveries)
        .zip(page.finalized_tree_aux)
        .map(|((header, deliveries), finalized_tree_aux)| {
            let delivery_schema =
                if tree_aux_schema == AuxSchema::V1 && finalized_tree_aux.is_none() {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                };
            let delivery = selected_port_aux_delivery(&deliveries, delivery_schema);
            HeaderEntry {
                header,
                body_size: delivery.map_or(0, |delivery| match delivery.body_size {
                    zakura_header_chain::BodySizeHint::Unknown => 0,
                    zakura_header_chain::BodySizeHint::Known(size) => size.get(),
                }),
                tree_aux: (tree_aux_schema == AuxSchema::V1)
                    .then(|| finalized_tree_aux.or_else(|| delivery.and_then(|item| item.tree_aux)))
                    .flatten(),
            }
        })
        .collect();
    Some(HeaderPathPage {
        lease_id,
        common_ancestor: page.common_ancestor,
        target: page.target,
        scope: page.scope,
        tree_aux_schema,
        entries,
        complete: page.complete,
    })
}

#[cfg_attr(any(test, feature = "zakura-testkit"), allow(dead_code))]
fn selected_port_aux_delivery(
    deliveries: &[zakura_header_chain::AuxDelivery],
    schema: AuxSchema,
) -> Option<zakura_header_chain::AuxDelivery> {
    deliveries
        .iter()
        .copied()
        .filter(|delivery| {
            !matches!(
                delivery.authentication,
                zakura_header_chain::AuxAuthentication::Rejected { .. }
            ) && match schema {
                AuxSchema::None => matches!(
                    delivery.body_size,
                    zakura_header_chain::BodySizeHint::Known(_)
                ),
                AuxSchema::V1 => delivery.tree_aux.is_some(),
            }
        })
        .min_by_key(|delivery| {
            (
                !matches!(
                    delivery.authentication,
                    zakura_header_chain::AuxAuthentication::Authenticated { .. }
                ),
                delivery.delivery_id,
            )
        })
}

impl HeaderSyncReactor {
    async fn run(mut self) {
        let mut committed_snapshots = self.startup.committed_snapshots.clone();
        let mut vct_root_repairs = self.startup.vct_root_repairs.clone();
        let terminal_outcome = loop {
            let maintenance = self.next_maintenance_deadline();
            if maintenance <= Instant::now() {
                self.refresh_statuses();
                continue;
            }
            metrics::counter!("sync.header.reactor.iterations").increment(1);
            tokio::select! {
                _ = self.startup.shutdown.cancelled() => break HeaderRequestTerminal::Shutdown,
                event = self.lifecycle.recv() => match event {
                    Some(event) => self.handle_event(event),
                    None => break HeaderRequestTerminal::Shutdown,
                },
                event = self.events.recv() => match event {
                    Some(event) => self.handle_event(event),
                    None => break HeaderRequestTerminal::Shutdown,
                },
                completion = async {
                    if self.pending_port_operations.is_empty() {
                        std::future::pending().await
                    } else {
                        self.pending_port_operations.next().await
                    }
                } => {
                    if let Some(completion) = completion {
                        self.handle_port_completion(completion);
                    }
                }
                changed = async {
                    match committed_snapshots.as_mut() {
                        Some(snapshots) => snapshots.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_ok() {
                        if let Some(snapshot) = committed_snapshots
                            .as_ref()
                            .and_then(|snapshots| snapshots.borrow().clone())
                        {
                            self.observe_latest_committed_snapshot(snapshot);
                        }
                    } else {
                        committed_snapshots = None;
                    }
                }
                changed = async {
                    match vct_root_repairs.as_mut() {
                        Some(repairs) => repairs.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_ok() {
                        if let Some(status) =
                            vct_root_repairs.as_ref().map(|repairs| *repairs.borrow())
                        {
                            self.observe_vct_root_repair(status);
                        }
                    } else {
                        vct_root_repairs = None;
                    }
                }
                _ = time::sleep_until(maintenance) => self.refresh_statuses(),
            }
        };
        self.retire_all_peer_work(terminal_outcome);
    }

    fn handle_event(&mut self, event: Event) {
        metrics::counter!(
            "sync.header.reactor.events",
            "event" => event.metrics_label()
        )
        .increment(1);
        match event {
            Event::PeerConnected(session) => self.handle_peer_connected(session),
            Event::PeerDisconnected {
                peer,
                session_id,
                reason,
            } => self.handle_peer_disconnected(&peer, session_id, reason),
            Event::AdvisorySummary { .. } => {}
            Event::WireMessage {
                peer,
                session_id,
                msg,
            } => self.handle_wire_message(peer, session_id, msg),
            Event::SessionResponse {
                peer,
                session_id,
                scope,
                msg,
            } => self.handle_wire_response(peer, session_id, scope, msg),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::HeaderLocatorReady {
                peer,
                session_id,
                target_tip_hash,
                scope,
                locator,
            } => {
                self.handle_header_locator_ready(peer, session_id, target_tip_hash, scope, locator)
            }
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::VctRepairContextReady { owner, result } => {
                self.handle_vct_repair_context_ready(owner, result)
            }
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::PathLeaseReady {
                peer,
                session_id,
                scope,
                request,
                result,
            } => self.handle_header_path_lease_ready(peer, session_id, scope, request, result),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::HeaderPathPageReady {
                peer,
                session_id,
                scope,
                request_id,
                target_tip_hash,
                result,
            } => self.handle_header_path_page_ready(
                peer,
                session_id,
                scope,
                request_id,
                target_tip_hash,
                result,
            ),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::HeaderTargetPrepared {
                peer,
                source,
                owner,
                result,
            } => self.handle_header_target_prepared(peer, source, owner, result),
            #[cfg(any(test, feature = "zakura-testkit"))]
            Event::HeaderTargetAdmissionReady {
                peer,
                source,
                owner,
                result,
            } => self.handle_header_target_admission_ready(peer, source, owner, result),
        }
    }

    fn handle_port_completion(&mut self, completion: PortOperationResult) {
        match completion {
            PortOperationResult::Completed(completion) => completion(self),
            PortOperationResult::Panicked(context) => self.handle_port_panic(*context),
        }
    }

    fn handle_peer_connected(&mut self, session: PeerSession) {
        let latest_snapshot = self
            .startup
            .committed_snapshots
            .as_ref()
            .and_then(|snapshots| snapshots.borrow().clone());
        if let Some(snapshot) = latest_snapshot {
            self.observe_latest_committed_snapshot(snapshot);
        }

        let peer = session.peer_id().clone();
        if self
            .unproductive_peer_cooldowns
            .get(&peer)
            .is_some_and(|until| *until > Instant::now())
        {
            session.cancel_token().cancel();
            metrics::counter!("sync.header.peer.readmission_refused.total").increment(1);
            return;
        }
        let direction = session.direction();
        let replaces_same_direction = self
            .peer_state
            .get(&peer)
            .is_some_and(|state| state.session.direction() == direction);
        let replaced_repair = self.peer_work_queue.active(&peer).and_then(|active| {
            matches!(
                active.purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            )
            .then_some((active.owner, active.source))
        });
        let at_capacity = !replaces_same_direction
            && self.admitted_count(direction)
                >= match direction {
                    ServicePeerDirection::Inbound => {
                        self.startup.config.peer_limits.max_inbound_peers
                    }
                    ServicePeerDirection::Outbound => {
                        self.startup.config.peer_limits.max_outbound_peers
                    }
                };
        if at_capacity {
            session.cancel_token().cancel();
            return;
        }

        let status_publisher = self.committed_snapshot.as_ref().map(|snapshot| {
            StatusPublisher::new(
                Status::from_snapshot(snapshot, &self.serving_limits),
                self.startup.status_refresh_interval,
                Instant::now(),
            )
        });
        if self.peer_state.contains_key(&peer) {
            self.retire_peer_work(&peer, HeaderRequestTerminal::SessionReplaced);
            self.release_served_path(&peer);
        }
        if let Some(previous) = self.peer_state.insert(
            peer.clone(),
            PeerState {
                session,
                status_publisher,
                last_status: None,
                unproductive_requests: 0,
            },
        ) {
            previous.session.cancel_token().cancel();
            if let Some((owner, source)) = replaced_repair
                .and_then(|(owner, source)| owner.body_owner().map(|owner| (owner, source)))
            {
                self.retry_vct_repair(owner, source, HeaderRequestTerminal::SessionReplaced);
            }
        }
        self.publish_peer_state();
        let admitted = self
            .peer_state
            .get(&peer)
            .expect("the admitted peer was just installed");
        self.emit_peer_lifecycle(
            hs_trace::HEADER_PEER_CONNECTED,
            &peer,
            admitted.session.session_id(),
            admitted.session.direction(),
            None,
        );
        self.send_status(&peer);
    }

    fn handle_peer_disconnected(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        reason: &'static str,
    ) {
        let Some(direction) = self.peer_state.get(peer).and_then(|state| {
            (state.session.session_id() == session_id).then(|| state.session.direction())
        }) else {
            return;
        };
        self.release_served_path(peer);
        let abandoned_repair = self.peer_work_queue.active(peer).and_then(|active| {
            matches!(
                active.purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            )
            .then(|| {
                (
                    active
                        .owner
                        .body_owner()
                        .expect("an auxiliary repair has body authority"),
                    active.source,
                )
            })
        });
        self.retire_peer_work(peer, HeaderRequestTerminal::Disconnected);
        self.peer_state.remove(peer);
        if let Some((owner, source)) = abandoned_repair {
            self.retry_vct_repair(owner, source, HeaderRequestTerminal::Disconnected);
        }
        self.publish_peer_state();
        self.emit_peer_lifecycle(
            hs_trace::HEADER_PEER_DISCONNECTED,
            peer,
            session_id,
            direction,
            Some(reason),
        );
    }

    fn handle_wire_message(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        message: HeaderSyncMessage,
    ) {
        let Some(state) = self.peer_state.get(&peer) else {
            return;
        };
        if state.session.session_id() != session_id {
            return;
        }
        let status = match message {
            HeaderSyncMessage::Status(status) => status,
            HeaderSyncMessage::GetHeaders(request) => {
                self.handle_get_headers(peer, session_id, request);
                return;
            }
            HeaderSyncMessage::Headers(_) => {
                tracing::debug!(?peer, "ignored response without an ownership reservation");
                return;
            }
            HeaderSyncMessage::HeadersOutcome(_) => {
                tracing::debug!(?peer, "ignored outcome without an ownership reservation");
                return;
            }
        };
        metrics::counter!("sync.header.peer.status.received").increment(1);
        self.emit_status(hs_trace::HEADER_STATUS_RECEIVED, &peer, session_id, &status);
        if status.work_anchor_height > status.selected_tip_height {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        }
        if let Some(state) = self.peer_state.get_mut(&peer) {
            state.last_status = Some(status.clone());
        }
        self.request_vct_repair_context();
        self.try_assign_vct_repair();
        self.consider_advertised_header_target(peer, session_id, status);
    }

    fn consider_advertised_header_target(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        status: Status,
    ) {
        let Some(local) = self.committed_snapshot.as_ref() else {
            return;
        };
        let target_tip_hash = status.selected_tip_hash;
        let scope = zakura_header_chain::HeaderWorkAuthority::for_target(local, target_tip_hash);
        let target = AdvertisedHeaderTarget {
            scope,
            session_id,
            status,
        };
        let work_order = target.claimed_work_order(local);
        let eligible = target.is_discovery_eligible(local);
        if !eligible {
            self.peer_work_queue.remove_unstarted(&peer);
            return;
        }
        let branch =
            zakura_header_chain::BranchId::new(local.frontiers.finalized.hash, target_tip_hash);
        if self
            .completed_targets
            .contains(local.header_generation, branch)
        {
            self.peer_work_queue.remove_unstarted(&peer);
            metrics::counter!("sync.header.target.covered").increment(1);
            return;
        }
        match self.peer_work_queue.stage_distinct_target(
            peer.clone(),
            target,
            PeerWorkPriority::from_work_order(work_order),
        ) {
            QueueWorkResult::NeedsLocator => {
                if !self.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
                    peer: peer.clone(),
                    session_id,
                    target_tip_hash,
                    scope,
                }) {
                    self.peer_work_queue.remove_unstarted(&peer);
                }
            }
            QueueWorkResult::AlreadyActive => {
                metrics::counter!("sync.header.target.already_active").increment(1);
            }
            QueueWorkResult::TargetAlreadyAssigned => {
                metrics::counter!("sync.header.target.duplicate_suppressed").increment(1);
            }
            QueueWorkResult::AtCapacity => {
                metrics::counter!("sync.header.target.capacity_refused").increment(1);
            }
        }
    }

    fn reconsider_advertised_header_targets(&mut self) {
        let targets: Vec<_> = self
            .peer_state
            .iter()
            .filter_map(|(peer, state)| {
                state
                    .last_status
                    .clone()
                    .map(|status| (peer.clone(), state.session.session_id(), status))
            })
            .collect();
        for (peer, session_id, status) in targets {
            self.consider_advertised_header_target(peer, session_id, status);
        }
    }

    fn handle_wire_response(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::HeaderWorkAuthority,
        message: HeaderSyncMessage,
    ) {
        let Some(state) = self.peer_state.get(&peer) else {
            return;
        };
        if state.session.session_id() != session_id {
            return;
        }
        match message {
            HeaderSyncMessage::Headers(response) => {
                self.handle_headers(peer, session_id, scope, response)
            }
            HeaderSyncMessage::HeadersOutcome(response) => {
                self.handle_headers_outcome(peer, session_id, scope, response)
            }
            HeaderSyncMessage::Status(_) | HeaderSyncMessage::GetHeaders(_) => {
                tracing::debug!(?peer, "ignored non-response in an ownership reservation");
            }
        }
    }

    fn handle_get_headers(&mut self, peer: ZakuraPeerId, session_id: u64, request: GetHeaders) {
        let request_id = HeaderSyncRequestId::new(request.request_id)
            .expect("the bounded decoder rejects zero request IDs");
        let max_header_count =
            self.served_page_count(request.max_header_count, request.tree_aux_schema);
        if max_header_count == 0 {
            self.send_headers_outcome(
                &peer,
                request.request_id,
                request.target_tip_hash,
                HeadersOutcomeCode::Busy,
            );
            return;
        }

        let replaces_idle_path = matches!(
            self.served_paths.get(&peer),
            Some(ServedPathState::Active {
                session_id: owner_session,
                target,
                next_after,
                pending_request: None,
                ..
            }) if *owner_session != session_id
                || target.hash != request.target_tip_hash
                || request.locator_hashes.first().copied() != Some(next_after.hash)
        );
        if replaces_idle_path {
            self.release_served_path(&peer);
        }

        if let Some(state) = self.served_paths.get_mut(&peer) {
            match state {
                ServedPathState::Acquiring { .. } => {
                    self.send_headers_outcome(
                        &peer,
                        request.request_id,
                        request.target_tip_hash,
                        HeadersOutcomeCode::Busy,
                    );
                    return;
                }
                ServedPathState::Active {
                    session_id: owner_session,
                    lease_id,
                    target,
                    scope,
                    next_after,
                    pending_request,
                    ..
                } => {
                    if *owner_session != session_id
                        || target.hash != request.target_tip_hash
                        || request.locator_hashes.first().copied() != Some(next_after.hash)
                    {
                        self.send_headers_outcome(
                            &peer,
                            request.request_id,
                            request.target_tip_hash,
                            HeadersOutcomeCode::Busy,
                        );
                        return;
                    }
                    if pending_request.is_some() {
                        self.send_headers_outcome(
                            &peer,
                            request.request_id,
                            request.target_tip_hash,
                            HeadersOutcomeCode::Busy,
                        );
                        return;
                    }
                    *pending_request = Some(PendingServedRequest {
                        request_id,
                        max_header_count,
                        tree_aux_schema: request.tree_aux_schema,
                    });
                    self.served_path_deadlines
                        .insert(peer.clone(), Instant::now() + self.startup.request_timeout);
                    let action = HeaderPortOperation::ReadPath {
                        peer: peer.clone(),
                        session_id,
                        lease_id: *lease_id,
                        scope: *scope,
                        request_id,
                        target_tip_hash: request.target_tip_hash,
                        after_hash: next_after.hash,
                        max_header_count,
                        tree_aux_schema: request.tree_aux_schema,
                    };
                    if !self.dispatch_action(action) {
                        self.release_served_path(&peer);
                    }
                    return;
                }
            }
        }

        let Some(local) = self.committed_snapshot.as_ref() else {
            self.send_headers_outcome(
                &peer,
                request.request_id,
                request.target_tip_hash,
                HeadersOutcomeCode::Busy,
            );
            return;
        };
        let scope =
            zakura_header_chain::HeaderWorkAuthority::for_target(local, request.target_tip_hash);
        self.served_paths.insert(
            peer.clone(),
            ServedPathState::Acquiring {
                session_id,
                request_id,
                target_tip_hash: request.target_tip_hash,
                scope,
            },
        );
        self.served_path_deadlines
            .insert(peer.clone(), Instant::now() + self.startup.request_timeout);
        if !self.dispatch_action(HeaderPortOperation::AcquirePath {
            peer: peer.clone(),
            session_id,
            scope,
            request: request.clone(),
        }) {
            self.served_paths.remove(&peer);
            self.served_path_deadlines.remove(&peer);
            self.send_headers_outcome(
                &peer,
                request.request_id,
                request.target_tip_hash,
                HeadersOutcomeCode::Busy,
            );
        }
    }

    fn handle_headers(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        response_scope: zakura_header_chain::HeaderWorkAuthority,
        response: Headers,
    ) {
        let Some(request_id) = HeaderSyncRequestId::new(response.request_id) else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        };
        let Some(active) = self.peer_work_queue.active(&peer) else {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        };
        if active.request_id != request_id {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        }
        if active.owner.header_authority() != response_scope {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        }
        self.emit_header_response(
            hs_trace::HEADER_RESPONSE_RECEIVED,
            &peer,
            session_id,
            response_scope,
            response.request_id,
            response.target_tip_hash,
            response.common_ancestor_height,
            response.common_ancestor_hash,
            response.entries.len(),
            response.complete,
            response.tree_aux_schema,
        );
        let returned_ancestor = zakura_header_chain::Frontier::new(
            response.common_ancestor_height,
            response.common_ancestor_hash,
        );
        if let HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target, ..
        } = &active.purpose
        {
            let exact_shape = response.target_tip_hash == selected_target.hash
                && active.sent_locator.entries() == [returned_ancestor]
                && response.entries.len() == 1
                && response.complete
                && response.tree_aux_schema == AuxSchema::V1
                && response.entries[0].tree_aux.is_some()
                && response.entries[0].header.hash() == selected_target.hash;
            if !exact_shape {
                self.retry_vct_repair(
                    active
                        .owner
                        .body_owner()
                        .expect("an auxiliary repair has body authority"),
                    active.source,
                    HeaderRequestTerminal::MalformedResponse,
                );
                self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
                return;
            }
        }
        if !active.matches_response_page(response.target_tip_hash, returned_ancestor) {
            self.retire_peer_work(&peer, HeaderRequestTerminal::MalformedResponse);
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        }
        let active_target_tip_hash = active.target.status.selected_tip_hash;
        if !self
            .peer_work_queue
            .consume_response_capacity(&peer, response.entries.len())
        {
            self.retire_peer_work(&peer, HeaderRequestTerminal::MalformedResponse);
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        }
        if response.complete
            && response.entries.is_empty()
            && returned_ancestor.hash == response.target_tip_hash
            && response.target_tip_hash == active_target_tip_hash
        {
            self.retire_peer_work(&peer, HeaderRequestTerminal::AlreadyKnown);
            metrics::counter!("sync.header.target.already_known.total").increment(1);
            return;
        }
        debug_assert!(
            !response.entries.is_empty(),
            "the wire decoder accepts empty header pages only for already-known targets",
        );
        self.reset_unproductive_requests(&peer, session_id);
        self.request_deadlines
            .insert(peer.clone(), Instant::now() + self.startup.request_timeout);

        let response_schema = response.tree_aux_schema;
        let complete = response.complete;
        let active = self
            .peer_work_queue
            .active_mut(&peer)
            .expect("the matching active request was just cloned");
        active.common_ancestor.get_or_insert(returned_ancestor);
        active.entries.extend(response.entries);
        let staged_entry_count = active.entries.len();
        let target_tip_height = active.target.status.selected_tip_height;
        let Some(staged_tip) = active.staged_tip() else {
            self.retire_peer_work(&peer, HeaderRequestTerminal::MalformedResponse);
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        };
        let _ = active;
        debug_assert_eq!(
            self.peer_work_queue.owned_header_count(&peer),
            staged_entry_count,
            "every staged header retains exactly one owned budget unit",
        );
        // A peer can advertise an arbitrarily distant target.
        // The reactor bounds response staging by admitting a validated prefix at capacity.
        // The reactor limits continuations to remaining capacity.
        // This limit prevents a peer from forcing small prefix commits with short pages.
        let durable_prefix_full = self.committed_snapshot.as_ref().is_some_and(|snapshot| {
            Self::request_header_prefix_remaining(
                snapshot,
                self.peer_work_queue.claimed_header_count(),
                target_tip_height,
            ) == 0
        });
        let bounded_prefix =
            !complete && (self.peer_work_queue.budget_is_full() || durable_prefix_full);
        let active = self
            .peer_work_queue
            .active_mut(&peer)
            .expect("the matching active request remains staged");

        if complete || bounded_prefix {
            if complete && staged_tip.hash != active.target.status.selected_tip_hash {
                self.retire_peer_work(&peer, HeaderRequestTerminal::MalformedResponse);
                self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
                return;
            }
            let common_ancestor = active
                .common_ancestor
                .expect("a response page fixed its exact ancestor");
            let completion = match active.purpose {
                HeaderTargetPurpose::Normal if bounded_prefix => {
                    let owner = active
                        .owner
                        .header_owner()
                        .expect("a normal target has header authority");
                    active.owner = zakura_header_chain::HeaderWorkAuthority {
                        branch: zakura_header_chain::BranchId {
                            target_tip_hash: staged_tip.hash,
                            ..owner.authority.branch
                        },
                        ..owner.authority
                    }
                    .bind(owner.session_id, owner.request_id)
                    .into();
                    zakura_header_chain::TargetCompletion::TargetPrefix { common_ancestor }
                }
                HeaderTargetPurpose::Normal => {
                    zakura_header_chain::TargetCompletion::TargetComplete { common_ancestor }
                }
                HeaderTargetPurpose::SelectedAuxiliaryRepair {
                    selected_target, ..
                } => zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor,
                    selected_target,
                },
            };
            let repair = matches!(
                active.purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            )
            .then(|| {
                (
                    active
                        .owner
                        .body_owner()
                        .expect("an auxiliary repair has body authority"),
                    active.source,
                )
            });
            active.phase = HeaderTargetPhase::Preparing;
            let action = HeaderPortOperation::PrepareHeaderTarget {
                purpose: active.purpose.clone(),
                peer: peer.clone(),
                source: active.source,
                owner: active.owner,
                common_ancestor,
                target: staged_tip,
                completion,
                entries: std::mem::take(&mut active.entries),
            };
            let _ = active;
            self.peer_work_queue.publish_phase_metrics();
            if let Some((owner, source)) = repair {
                let Some(task) = self.vct_repair.get(owner) else {
                    self.retire_peer_work(&peer, HeaderRequestTerminal::RepairObsolete);
                    return;
                };
                if !matches!(task.state, RepairPolicyState::Assigned { .. }) {
                    self.retry_vct_repair(owner, source, HeaderRequestTerminal::RepairObsolete);
                    return;
                }
            }
            if !self.dispatch_action(action) {
                if let Some((owner, source)) = repair {
                    self.retry_vct_repair(owner, source, HeaderRequestTerminal::LocalError);
                } else {
                    self.retire_peer_work(&peer, HeaderRequestTerminal::LocalError);
                }
            }
            return;
        }

        let locator = active.continuation_locator(staged_tip);
        let negotiated_header_count = active.max_header_count;
        let tree_aux_schema = active.tree_aux_schema;
        let target_tip_hash = active.target.status.selected_tip_hash;
        let target_tip_height = active.target.status.selected_tip_height;
        let request_scope = active.owner.header_authority();
        let _ = active;
        let max_header_count = self
            .peer_work_queue
            .reservable_header_count(negotiated_header_count)
            .min(self.committed_snapshot.as_ref().map_or(0, |snapshot| {
                Self::request_header_prefix_remaining(
                    snapshot,
                    self.peer_work_queue.claimed_header_count(),
                    target_tip_height,
                )
            }));
        debug_assert!(
            max_header_count > 0,
            "a full owned or durable prefix returned before continuation"
        );
        if max_header_count == 0
            || !self
                .peer_work_queue
                .reserve_request(&peer, max_header_count)
        {
            self.retire_peer_work(&peer, HeaderRequestTerminal::StagingRefused);
            return;
        }
        let Some(session) = self
            .peer_state
            .get(&peer)
            .map(|state| state.session.clone())
        else {
            self.retire_peer_work(&peer, HeaderRequestTerminal::Disconnected);
            return;
        };
        match session.try_send_get_headers(
            &self.codec,
            request_scope,
            target_tip_hash,
            &locator,
            max_header_count,
            tree_aux_schema,
        ) {
            Ok(next_request_id) => {
                self.emit_header_request(
                    &peer,
                    session.session_id(),
                    request_scope,
                    next_request_id,
                    target_tip_hash,
                    &locator,
                    max_header_count,
                    tree_aux_schema,
                );
                let active = self
                    .peer_work_queue
                    .active_mut(&peer)
                    .expect("the active request remains staged across continuation");
                active.sent_locator = locator;
                active.request_id = next_request_id;
                debug_assert!(tree_aux_schema.admits(response_schema));
            }
            Err(error) => {
                self.peer_work_queue.cancel_request_reservation(&peer);
                self.emit_queue_send_failed(&peer, &session, "GetHeaders", &error, None);
                self.retire_peer_work(&peer, HeaderRequestTerminal::SendError);
            }
        }
    }

    fn handle_headers_outcome(
        &mut self,
        peer: ZakuraPeerId,
        _session_id: u64,
        response_scope: zakura_header_chain::HeaderWorkAuthority,
        response: HeadersOutcome,
    ) {
        let Some(request_id) = HeaderSyncRequestId::new(response.request_id) else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        };
        let Some(active) = self.peer_work_queue.active(&peer) else {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        };
        if active.request_id != request_id {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        }
        if active.owner.header_authority() != response_scope {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        }
        let matches = active.accepts_outcome(request_id, response.target_tip_hash);
        let is_repair = matches!(
            active.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        );
        let terminal_outcome = if matches {
            HeaderRequestTerminal::from(response.outcome)
        } else {
            HeaderRequestTerminal::MalformedResponse
        };
        if is_repair {
            self.retry_vct_repair(
                active
                    .owner
                    .body_owner()
                    .expect("an auxiliary repair has body authority"),
                active.source,
                terminal_outcome,
            );
        } else {
            self.retire_peer_work(&peer, terminal_outcome);
        }
        if matches {
            if is_repair {
                metrics::counter!(
                    "sync.header.vct.repair.outcome.total",
                    "outcome" => format!("{:?}", response.outcome)
                )
                .increment(1);
            } else {
                metrics::counter!(
                    "sync.header.target.outcome",
                    "outcome" => format!("{:?}", response.outcome)
                )
                .increment(1);
            }
        } else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
        }
    }

    fn handle_header_target_admission_ready(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        result: HeaderTargetAdmissionResult,
    ) {
        let Some(active) = self.peer_work_queue.active(&peer).cloned() else {
            return;
        };
        if active.phase != HeaderTargetPhase::Applying
            || active.source != source
            || active.owner != owner
        {
            return;
        }
        let is_repair = matches!(
            active.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        );
        let completion_authority = match self
            .registered_completion_authority(&peer, source, &owner, is_repair)
        {
            Ok((authority, outcome)) => {
                metrics::counter!(
                    "sync.header.target.completion_gate.total",
                    "outcome" => outcome
                )
                .increment(1);
                authority
            }
            Err(reason) => {
                Self::record_stale_completion(reason);
                match active.purpose {
                    HeaderTargetPurpose::Normal => {
                        self.retire_peer_work(&peer, HeaderRequestTerminal::SnapshotObsolete);
                    }
                    HeaderTargetPurpose::SelectedAuxiliaryRepair { .. } => self.retry_vct_repair(
                        owner
                            .body_owner()
                            .expect("an auxiliary repair has body authority"),
                        source,
                        HeaderRequestTerminal::RepairObsolete,
                    ),
                }
                return;
            }
        };
        let repair_generation = match active.purpose {
            HeaderTargetPurpose::Normal => None,
            HeaderTargetPurpose::SelectedAuxiliaryRepair {
                repair_generation, ..
            } => Some(repair_generation),
        };
        match &result {
            HeaderTargetAdmissionResult::Applied => {
                self.emit_target_outcome(
                    hs_trace::HEADER_TARGET_ADMITTED,
                    "apply",
                    &peer,
                    owner,
                    None,
                );
            }
            HeaderTargetAdmissionResult::Failed(error) => {
                self.emit_target_outcome(
                    hs_trace::HEADER_TARGET_REJECTED,
                    "apply",
                    &peer,
                    owner,
                    Some(error),
                );
            }
            HeaderTargetAdmissionResult::ResourceStalled(receipt) => {
                tracing::warn!(
                    ?receipt,
                    "header target admission stopped by a committed local resource alarm"
                );
                metrics::counter!(
                    "sync.header.target.resource_stalled.total",
                    "alarm_changed" => receipt.alarm_changed.to_string()
                )
                .increment(1);
            }
        }
        self.retire_peer_work(
            &peer,
            match &result {
                HeaderTargetAdmissionResult::Applied => HeaderRequestTerminal::TargetAdmitted,
                HeaderTargetAdmissionResult::Failed(_)
                | HeaderTargetAdmissionResult::ResourceStalled(_) => {
                    HeaderRequestTerminal::TargetRejected
                }
            },
        );
        if let Some(repair_generation) = repair_generation {
            let repair_owner = owner
                .body_owner()
                .expect("an auxiliary repair admission has body authority");
            match result {
                HeaderTargetAdmissionResult::Applied => {
                    let _ = self
                        .vct_repair
                        .get_mut(repair_owner)
                        .expect("the admitted repair remains owned by its active request")
                        .complete();
                    if let Some(task) = self.vct_repair.get(repair_owner) {
                        self.emit_vct_repair_state(task, "admission", Some("applied"));
                    }
                    metrics::counter!("sync.header.vct.repair.admitted.total").increment(1);
                }
                HeaderTargetAdmissionResult::Failed(error) => {
                    self.vct_repair.remove(repair_owner);
                    self.handle_typed_failure(peer, source, &error);
                    if repair_generation == self.vct_repair_status.generation {
                        self.schedule_current_vct_repair();
                    }
                }
                HeaderTargetAdmissionResult::ResourceStalled(_) => {
                    self.vct_repair.remove(repair_owner);
                    metrics::counter!("sync.header.vct.repair.resource_stalled.total").increment(1);
                }
            }
            return;
        }
        match result {
            HeaderTargetAdmissionResult::Applied => {
                self.completed_targets.mark(
                    completion_authority.header_generation,
                    completion_authority.branch,
                );
                metrics::counter!("sync.header.target.admitted").increment(1);
            }
            HeaderTargetAdmissionResult::Failed(error) => {
                self.handle_typed_failure(peer, source, &error);
            }
            HeaderTargetAdmissionResult::ResourceStalled(_) => {}
        }
    }

    fn handle_header_target_prepared(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        result: HeaderTargetPreparationResult,
    ) {
        let Some(active) = self.peer_work_queue.active(&peer).cloned() else {
            return;
        };
        let purpose = active.purpose;
        let is_repair = matches!(purpose, HeaderTargetPurpose::SelectedAuxiliaryRepair { .. });
        if active.phase != HeaderTargetPhase::Preparing
            || active.source != source
            || active.owner != owner
        {
            return;
        }
        if !self.preparation_has_authority(
            &peer,
            source,
            &owner,
            is_repair,
            self.peer_work_queue.owned_header_count(&peer),
        ) {
            if is_repair {
                self.retry_vct_repair(
                    owner
                        .body_owner()
                        .expect("an auxiliary repair has body authority"),
                    source,
                    HeaderRequestTerminal::RepairObsolete,
                );
            } else {
                self.retire_peer_work(&peer, HeaderRequestTerminal::SnapshotObsolete);
            }
            return;
        }
        match result {
            HeaderTargetPreparationResult::Prepared(target) => {
                if target.owner() != owner || target.source() != source {
                    return;
                }
                if is_repair {
                    let repair_owner = owner
                        .body_owner()
                        .expect("an auxiliary repair preparation has body authority");
                    let valid = self.vct_repair.get(repair_owner).is_some_and(|task| {
                        let RepairPolicyState::Assigned { context } = &task.state else {
                            return false;
                        };
                        target.target_tip_hash() == context.target.hash
                            && target.auxiliary_delivery_count() == 1
                    });
                    if !valid {
                        self.retry_vct_repair(
                            repair_owner,
                            source,
                            HeaderRequestTerminal::RepairObsolete,
                        );
                        return;
                    }
                }
                self.request_deadlines
                    .insert(peer.clone(), Instant::now() + self.startup.request_timeout);
                self.peer_work_queue
                    .active_mut(&peer)
                    .expect("the exact preparing request passed the completion gate")
                    .phase = HeaderTargetPhase::Applying;
                self.peer_work_queue.publish_phase_metrics();
                if self.dispatch_action(HeaderPortOperation::ApplyHeaderTarget {
                    purpose: purpose.clone(),
                    peer: peer.clone(),
                    source,
                    owner,
                    target,
                }) {
                } else if is_repair {
                    self.retry_vct_repair(
                        owner
                            .body_owner()
                            .expect("an auxiliary repair has body authority"),
                        source,
                        HeaderRequestTerminal::LocalError,
                    );
                } else {
                    self.retire_peer_work(&peer, HeaderRequestTerminal::LocalError);
                }
            }
            HeaderTargetPreparationResult::Failed(error) => {
                self.emit_target_outcome(
                    hs_trace::HEADER_TARGET_REJECTED,
                    "prepare",
                    &peer,
                    owner,
                    Some(&error),
                );
                if is_repair {
                    self.retry_vct_repair(
                        owner
                            .body_owner()
                            .expect("an auxiliary repair has body authority"),
                        source,
                        HeaderRequestTerminal::TargetRejected,
                    );
                } else {
                    self.retire_peer_work(&peer, HeaderRequestTerminal::TargetRejected);
                }
                self.handle_typed_failure(peer, source, &error);
            }
        }
    }

    fn emit_target_outcome(
        &self,
        event: &'static str,
        stage: &'static str,
        peer: &ZakuraPeerId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        error: Option<&zakura_header_chain::HeaderChainError>,
    ) {
        let direction = self
            .peer_state
            .get(peer)
            .map(|state| header_direction_label(state.session.direction()));
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), event.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), owner.session_id().into());
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            insert_header_scope(row, owner.header_authority());
            row.insert(hs_trace::REQUEST_ID.into(), owner.request_id().get().into());
            row.insert(hs_trace::STAGE.into(), stage.into());
            row.insert(
                hs_trace::CATEGORY.into(),
                error.map_or(serde_json::Value::Null, |error| {
                    error.category.metrics_label().into()
                }),
            );
            row.insert(
                hs_trace::ATTRIBUTION.into(),
                error.map_or(serde_json::Value::Null, |error| {
                    error.attribution.metrics_label().into()
                }),
            );
        });
    }

    fn retry_vct_repair(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        source: zakura_header_chain::SourceId,
        terminal_outcome: HeaderRequestTerminal,
    ) {
        if let Some(active) = self.peer_work_queue.active_owner(owner.into()).cloned() {
            self.emit_request_terminal(&active, terminal_outcome);
        }
        self.cancel_owned_request(source, owner.into());
        self.peer_work_queue.remove_owner(owner.into());
        let peer = self
            .peer_state
            .iter()
            .find(|(peer, state)| {
                state.session.session_id() == owner.session_id
                    && source_id_from_peer(peer) == source
            })
            .map(|(peer, _)| peer.clone());
        if let Some(peer) = peer {
            self.request_deadlines.remove(&peer);
        }
        if self
            .vct_repair
            .get_mut(owner)
            .is_some_and(|task| task.retry(source).is_ok())
        {
            if let Some(task) = self.vct_repair.current() {
                self.emit_vct_repair_state(task, "retry", Some("supplier_retry"));
            }
            self.try_assign_vct_repair();
        }
    }

    fn emit_vct_repair_state(
        &self,
        task: &RepairRequirement,
        phase: &'static str,
        outcome: Option<&'static str>,
    ) {
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_VCT_REPAIR_STATE.into(),
            );
            insert_header_scope(row, task.owner.header);
            row.insert(hs_trace::SESSION_ID.into(), task.owner.session_id.into());
            row.insert(
                hs_trace::REQUEST_ID.into(),
                task.owner.request_id.get().into(),
            );
            row.insert(hs_trace::HEIGHT.into(), u64::from(task.height.0).into());
            row.insert(
                hs_trace::REPAIR_GENERATION.into(),
                task.repair_generation.into(),
            );
            row.insert(hs_trace::PHASE.into(), phase.into());
            row.insert(
                hs_trace::SUPPLIER_COUNT.into(),
                u64::try_from(task.tried_sources.len())
                    .unwrap_or(u64::MAX)
                    .into(),
            );
            row.insert(
                hs_trace::OUTCOME.into(),
                outcome.map_or(serde_json::Value::Null, Into::into),
            );
        });
    }

    fn retire_vct_repair(&mut self) {
        if let Some(task) = self.vct_repair.take() {
            if let Some(peer) = self
                .peer_work_queue
                .active_owner(task.owner.into())
                .map(|active| active.peer.clone())
            {
                self.retire_peer_work(&peer, HeaderRequestTerminal::RepairObsolete);
            }
        }
    }

    fn handle_header_path_lease_ready(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::HeaderWorkAuthority,
        request: GetHeaders,
        result: HeaderPathLeaseResult,
    ) {
        let request_id = HeaderSyncRequestId::new(request.request_id)
            .expect("state echoes a request accepted by the bounded decoder");
        let Some(state) = self.served_paths.remove(&peer) else {
            if let HeaderPathLeaseResult::Acquired(lease) = result {
                self.release_lease(peer, session_id, lease.lease_id, lease.scope);
            }
            return;
        };
        let ServedPathState::Acquiring {
            session_id: expected_session,
            request_id: expected_request,
            target_tip_hash: expected_target,
            scope: expected_scope,
        } = state
        else {
            self.served_paths.insert(peer.clone(), state);
            if let HeaderPathLeaseResult::Acquired(lease) = result {
                self.release_lease(peer, session_id, lease.lease_id, lease.scope);
            }
            return;
        };
        if expected_session != session_id
            || expected_request != request_id
            || expected_target != request.target_tip_hash
            || expected_scope != scope
        {
            self.served_paths.insert(
                peer.clone(),
                ServedPathState::Acquiring {
                    session_id: expected_session,
                    request_id: expected_request,
                    target_tip_hash: expected_target,
                    scope: expected_scope,
                },
            );
            if let HeaderPathLeaseResult::Acquired(lease) = result {
                self.release_lease(peer, session_id, lease.lease_id, lease.scope);
            }
            return;
        }

        let lease = match result {
            HeaderPathLeaseResult::Outcome(outcome) => {
                self.served_path_deadlines.remove(&peer);
                self.send_headers_outcome(
                    &peer,
                    request.request_id,
                    request.target_tip_hash,
                    outcome,
                );
                return;
            }
            HeaderPathLeaseResult::Acquired(lease)
                if lease.target.hash == request.target_tip_hash
                    && request.locator_hashes.contains(&lease.common_ancestor.hash)
                    && lease.scope == scope =>
            {
                lease
            }
            HeaderPathLeaseResult::Acquired(lease) => {
                self.served_path_deadlines.remove(&peer);
                self.send_headers_outcome(
                    &peer,
                    request.request_id,
                    request.target_tip_hash,
                    HeadersOutcomeCode::Busy,
                );
                self.release_lease(peer, session_id, lease.lease_id, lease.scope);
                return;
            }
        };
        let max_header_count =
            self.served_page_count(request.max_header_count, request.tree_aux_schema);
        self.served_paths.insert(
            peer.clone(),
            ServedPathState::Active {
                session_id,
                lease_id: lease.lease_id,
                target: lease.target,
                scope: lease.scope,
                next_after: lease.common_ancestor,
                pending_request: Some(PendingServedRequest {
                    request_id,
                    max_header_count,
                    tree_aux_schema: request.tree_aux_schema,
                }),
            },
        );
        self.served_path_deadlines
            .insert(peer.clone(), Instant::now() + self.startup.request_timeout);
        if !self.dispatch_action(HeaderPortOperation::ReadPath {
            peer: peer.clone(),
            session_id,
            lease_id: lease.lease_id,
            scope: lease.scope,
            request_id,
            target_tip_hash: lease.target.hash,
            after_hash: lease.common_ancestor.hash,
            max_header_count,
            tree_aux_schema: request.tree_aux_schema,
        }) {
            self.release_served_path(&peer);
        }
    }

    fn handle_header_path_page_ready(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::HeaderWorkAuthority,
        request_id: HeaderSyncRequestId,
        target_tip_hash: block::Hash,
        result: HeaderPathPageResult,
    ) {
        let Some(state) = self.served_paths.remove(&peer) else {
            return;
        };
        let ServedPathState::Active {
            session_id: expected_session,
            lease_id,
            target,
            scope: expected_scope,
            next_after,
            pending_request,
        } = state
        else {
            self.served_paths.insert(peer, state);
            return;
        };
        if expected_session != session_id
            || expected_scope != scope
            || target.hash != target_tip_hash
            || pending_request.is_none_or(|pending| pending.request_id != request_id)
        {
            self.served_paths.insert(
                peer,
                ServedPathState::Active {
                    session_id: expected_session,
                    lease_id,
                    target,
                    scope: expected_scope,
                    next_after,
                    pending_request,
                },
            );
            return;
        }
        let HeaderPathPageResult::Page(page) = result else {
            self.served_path_deadlines.remove(&peer);
            self.send_headers_outcome(
                &peer,
                request_id.get(),
                target_tip_hash,
                HeadersOutcomeCode::Busy,
            );
            self.release_lease(peer, session_id, lease_id, expected_scope);
            return;
        };
        if page.lease_id != lease_id
            || page.target != target
            || page.scope != expected_scope
            || page.common_ancestor != next_after
            || pending_request.is_some_and(|pending| {
                page.entries.len() > usize::try_from(pending.max_header_count).unwrap_or(usize::MAX)
                    || !pending.tree_aux_schema.admits(page.tree_aux_schema)
            })
        {
            self.served_path_deadlines.remove(&peer);
            self.send_headers_outcome(
                &peer,
                request_id.get(),
                target_tip_hash,
                HeadersOutcomeCode::Busy,
            );
            self.release_lease(peer, session_id, lease_id, expected_scope);
            return;
        }

        let next_after = if let Some(last) = page.entries.last() {
            let Some(height) = page
                .common_ancestor
                .height
                .0
                .checked_add(u32::try_from(page.entries.len()).unwrap_or(u32::MAX))
                .map(block::Height)
                .filter(|height| *height <= block::Height::MAX)
            else {
                self.served_path_deadlines.remove(&peer);
                self.send_headers_outcome(
                    &peer,
                    request_id.get(),
                    target_tip_hash,
                    HeadersOutcomeCode::Busy,
                );
                self.release_lease(peer, session_id, lease_id, expected_scope);
                return;
            };
            zakura_header_chain::Frontier::new(height, last.header.hash())
        } else {
            page.common_ancestor
        };
        let complete = page.complete;
        let response = Headers {
            request_id: request_id.get(),
            target_tip_hash,
            common_ancestor_height: page.common_ancestor.height,
            common_ancestor_hash: page.common_ancestor.hash,
            complete,
            tree_aux_schema: page.tree_aux_schema,
            entries: page.entries,
        };
        let header_count = response.entries.len();
        let common_ancestor_height = response.common_ancestor_height;
        let common_ancestor_hash = response.common_ancestor_hash;
        let response_schema = response.tree_aux_schema;
        let sent = self
            .peer_state
            .get(&peer)
            .map(|state| state.session.clone())
            .is_some_and(
                |session| match session.try_send_headers(&self.codec, response) {
                    Ok(()) => true,
                    Err(error) => {
                        self.emit_queue_send_failed(
                            &peer,
                            &session,
                            "Headers",
                            &error,
                            Some(request_id.get()),
                        );
                        false
                    }
                },
            );
        if sent {
            self.emit_header_response(
                hs_trace::HEADER_RESPONSE_SERVED,
                &peer,
                session_id,
                expected_scope,
                request_id.get(),
                target_tip_hash,
                common_ancestor_height,
                common_ancestor_hash,
                header_count,
                complete,
                response_schema,
            );
        }
        if complete || !sent {
            self.served_path_deadlines.remove(&peer);
            if !sent {
                self.send_headers_outcome(
                    &peer,
                    request_id.get(),
                    target_tip_hash,
                    HeadersOutcomeCode::Busy,
                );
            }
            self.release_lease(peer, session_id, lease_id, expected_scope);
        } else {
            self.served_paths.insert(
                peer.clone(),
                ServedPathState::Active {
                    session_id,
                    lease_id,
                    target,
                    scope: expected_scope,
                    next_after,
                    pending_request: None,
                },
            );
            self.served_path_deadlines
                .insert(peer, Instant::now() + self.startup.request_timeout);
        }
    }

    fn finish_header_locator_query(
        &mut self,
        peer: ZakuraPeerId,
        query_scope: zakura_header_chain::HeaderWorkAuthority,
        locator: Option<zakura_header_chain::HeaderLocator>,
    ) {
        self.pending_locator_queries.remove(&peer);
        let Some(target) = self.peer_work_queue.awaiting_target(&peer).cloned() else {
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            return;
        };
        // Locators are derived from the local selected path, so tip churn under the same
        // generation and finality anchor can reuse one in-flight read. A generation or
        // reanchor change must not consume a locator fetched under the prior authority.
        let same_selected_path_authority = target.scope.header_generation
            == query_scope.header_generation
            && target.scope.branch.anchor_hash == query_scope.branch.anchor_hash;
        let superseded_tip = target.status.selected_tip_hash != query_scope.branch.target_tip_hash;
        if !same_selected_path_authority || (locator.is_none() && superseded_tip) {
            // Authority moved, or a timed-out/failed read belonged to a replaced tip.
            // Keep the current staged target and fetch a fresh locator for it.
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            if !self.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
                peer: peer.clone(),
                session_id: target.session_id,
                target_tip_hash: target.status.selected_tip_hash,
                scope: target.scope,
            }) {
                self.peer_work_queue.remove_unstarted(&peer);
            }
            return;
        }
        self.handle_header_locator_ready(
            peer,
            target.session_id,
            target.status.selected_tip_hash,
            target.scope,
            locator,
        );
    }

    fn handle_header_locator_ready(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        target_tip_hash: block::Hash,
        scope: zakura_header_chain::HeaderWorkAuthority,
        locator: Option<zakura_header_chain::HeaderLocator>,
    ) {
        let Some(target) = self
            .peer_work_queue
            .awaiting(&peer, session_id, target_tip_hash, scope)
            .cloned()
        else {
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            return;
        };
        let Some(locator) = locator else {
            self.peer_work_queue.remove_unstarted(&peer);
            metrics::counter!("sync.header.target.locator_unavailable").increment(1);
            return;
        };
        let Some(local) = self.committed_snapshot.clone() else {
            self.peer_work_queue.remove_unstarted(&peer);
            return;
        };
        if !target.is_current(&local) {
            self.peer_work_queue.remove_unstarted(&peer);
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            return;
        }
        let Some(session) = self
            .peer_state
            .get(&peer)
            .map(|state| state.session.clone())
        else {
            self.peer_work_queue.remove_unstarted(&peer);
            return;
        };

        let tree_aux_schema = if target.status.tree_aux_schema_mask
            & self.serving_limits.tree_aux_schema_mask()
            & AuxSchema::V1.mask_bit()
            != 0
        {
            AuxSchema::V1
        } else {
            AuxSchema::None
        };
        let byte_limited_count = headers_response_capacity(
            &self.startup.network,
            tree_aux_schema,
            usize::try_from(
                target
                    .status
                    .max_message_bytes
                    .min(self.serving_limits.max_message_bytes()),
            )
            .unwrap_or(usize::MAX),
        );
        let max_header_count = target
            .status
            .max_headers_per_response
            .min(self.serving_limits.max_headers_per_response())
            .min(byte_limited_count)
            .min(MAX_HS_RANGE);
        let max_header_count = max_header_count.min(Self::request_header_prefix_remaining(
            &local,
            self.peer_work_queue.claimed_header_count(),
            target.status.selected_tip_height,
        ));
        let max_header_count = self
            .peer_work_queue
            .reservable_header_count(max_header_count);
        if max_header_count == 0
            || !self
                .peer_work_queue
                .reserve_request(&peer, max_header_count)
        {
            self.peer_work_queue.remove_unstarted(&peer);
            return;
        }
        let source = source_id_from_peer(&peer);

        match session.try_send_get_headers(
            &self.codec,
            target.scope,
            target_tip_hash,
            &locator,
            max_header_count,
            tree_aux_schema,
        ) {
            Ok(request_id) => {
                self.emit_header_request(
                    &peer,
                    session_id,
                    target.scope,
                    request_id,
                    target_tip_hash,
                    &locator,
                    max_header_count,
                    tree_aux_schema,
                );
                let owner = target.scope.bind(
                    session_id,
                    NonZeroU64::new(request_id.get()).expect("header-sync request IDs are nonzero"),
                );
                let started = self.peer_work_queue.start(ActiveHeaderRequest {
                    purpose: HeaderTargetPurpose::Normal,
                    peer: peer.clone(),
                    source,
                    target,
                    sent_locator: locator,
                    request_id,
                    owner: owner.into(),
                    common_ancestor: None,
                    entries: Vec::new(),
                    phase: HeaderTargetPhase::Receiving,
                    max_header_count,
                    tree_aux_schema,
                });
                debug_assert!(
                    started,
                    "the matching locator was checked before publication"
                );
                if started {
                    self.request_deadlines
                        .insert(peer.clone(), Instant::now() + self.startup.request_timeout);
                } else {
                    session.cancel_request(request_id);
                    self.peer_work_queue.remove_unstarted(&peer);
                }
                metrics::counter!("sync.header.target.requested").increment(1);
            }
            Err(error) => {
                self.peer_work_queue.cancel_request_reservation(&peer);
                self.peer_work_queue.remove_unstarted(&peer);
                self.emit_queue_send_failed(&peer, &session, "GetHeaders", &error, None);
                metrics::counter!(
                    "sync.header.target.send_failed",
                    "reason" => ordered_send_error_label(&error)
                )
                .increment(1);
            }
        }
    }

    /// Bound ordinary target prefixes by shared selected-path space in the durable DAG.
    ///
    /// Retention protects the complete selected path.
    /// Downloading more entries than this headroom can only cause a resource refusal.
    /// All response reservations and staged entries share the headroom.
    /// A protected side path can still make state refuse the insertion during application.
    fn durable_header_prefix_remaining(
        snapshot: &zakura_header_chain::EngineSnapshot,
        claimed: usize,
    ) -> u32 {
        let selected_non_finalized = snapshot
            .frontiers
            .header_best
            .height
            .0
            .saturating_sub(snapshot.frontiers.finalized.height.0);
        let selected_non_finalized = usize::try_from(selected_non_finalized)
            .expect("u32 header divergence fits usize on supported targets");
        let remaining = zakura_header_chain::MAX_NON_FINALIZED_NODES_V1
            .saturating_sub(selected_non_finalized)
            .saturating_sub(claimed);
        u32::try_from(remaining).unwrap_or(u32::MAX)
    }

    /// Return requester headroom after both the durable DAG limit and the integrated body window.
    ///
    /// A partial window remains closed until half of the admitted body lag remains.
    /// The hysteresis avoids small header transitions and preserves work for body application.
    /// The checkpoint bound lets a smaller protocol window admit a complete checkpoint range.
    /// The final partial page lets a node reach a target with a suffix shorter than one page.
    fn request_header_prefix_remaining(
        snapshot: &zakura_header_chain::EngineSnapshot,
        claimed: usize,
        target_tip_height: block::Height,
    ) -> u32 {
        let durable = Self::durable_header_prefix_remaining(snapshot, claimed);
        let body_lag = snapshot
            .frontiers
            .header_best
            .height
            .0
            .saturating_sub(snapshot.frontiers.verified_best.height.0);
        let body_window = INTEGRATED_HEADER_BODY_WINDOW_V1.saturating_sub(body_lag);
        let target_remaining = target_tip_height
            .0
            .saturating_sub(snapshot.frontiers.header_best.height.0);
        let checkpoint_low_water = u32::try_from(
            zakura_chain::parameters::checkpoint::constants::MAX_CHECKPOINT_HEIGHT_GAP,
        )
        .expect("the consensus checkpoint height gap fits a block height")
        .saturating_add(1);
        let refill_low_water = checkpoint_low_water.max(INTEGRATED_HEADER_REFILL_LOW_WATER_V1);
        if body_lag > refill_low_water && target_remaining > body_window {
            return 0;
        }
        let claimed = u32::try_from(claimed).unwrap_or(u32::MAX);
        durable.min(body_window.saturating_sub(claimed))
    }

    fn observe_latest_committed_snapshot(&mut self, snapshot: zakura_header_chain::EngineSnapshot) {
        if self.committed_snapshot.as_ref() == Some(&snapshot) {
            return;
        }

        let header_authority_changed = self.committed_snapshot.as_ref().is_some_and(|old| {
            old.header_generation != snapshot.header_generation
                || old.frontiers.finalized != snapshot.frontiers.finalized
        });
        self.emit_snapshot_observed(self.committed_snapshot.as_ref(), &snapshot);
        self.retire_obsolete_work(&snapshot);
        let old_tip = self
            .committed_snapshot
            .as_ref()
            .map(|old| old.frontiers.header_best);
        let new_tip = snapshot.frontiers.header_best;
        self.startup.frontiers = FullStateFrontiers {
            finalized_height: snapshot.frontiers.finalized.height,
            verified_block_tip: snapshot.frontiers.verified_best.height,
            verified_block_hash: snapshot.frontiers.verified_best.hash,
        };
        let status = Status::from_snapshot(&snapshot, &self.serving_limits);
        let now = Instant::now();
        self.committed_snapshot = Some(snapshot);
        self.schedule_current_vct_repair();
        for state in self.peer_state.values_mut() {
            match state.status_publisher.as_mut() {
                Some(publisher) => publisher.observe(status.clone(), now),
                None => {
                    state.status_publisher = Some(StatusPublisher::new(
                        status.clone(),
                        self.startup.status_refresh_interval,
                        now,
                    ));
                }
            }
        }
        if old_tip != Some(new_tip) {
            let _ = self.tip.send((new_tip.height, new_tip.hash));
            self.publish_peer_state();
        }
        self.refresh_statuses();
        if header_authority_changed {
            self.reconsider_advertised_header_targets();
        }
    }

    fn emit_snapshot_observed(
        &self,
        old: Option<&zakura_header_chain::EngineSnapshot>,
        new: &zakura_header_chain::EngineSnapshot,
    ) {
        let cause = match old {
            None => "startup",
            Some(old) if old.frontiers.finalized.hash != new.frontiers.finalized.hash => "reanchor",
            Some(old) if new.frontiers.header_best.height > old.frontiers.header_best.height => {
                "advance"
            }
            Some(_) => "refresh",
        };
        if cause == "refresh" {
            let now = Instant::now();
            if !snapshot_refresh_trace_due(self.last_snapshot_refresh_trace_at.get(), now) {
                return;
            }
            self.last_snapshot_refresh_trace_at.set(Some(now));
        }
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_SNAPSHOT_OBSERVED.into(),
            );
            row.insert(hs_trace::CAUSE.into(), cause.into());
            row.insert(
                hs_trace::STATE_VERSION.into(),
                new.state_version.get().into(),
            );
            row.insert(
                hs_trace::HEADER_GENERATION.into(),
                new.header_generation.get().into(),
            );
            row.insert(
                hs_trace::VERIFIED_GENERATION.into(),
                new.verified_generation.get().into(),
            );
            row.insert(
                hs_trace::BRANCH_ANCHOR.into(),
                new.frontiers.finalized.hash.to_string().into(),
            );
            row.insert(
                hs_trace::BRANCH_TARGET.into(),
                new.frontiers.header_best.hash.to_string().into(),
            );
            row.insert(
                hs_trace::OLD_SELECTED_HEIGHT.into(),
                old.map_or(serde_json::Value::Null, |old| {
                    u64::from(old.frontiers.header_best.height.0).into()
                }),
            );
            row.insert(
                hs_trace::OLD_SELECTED_HASH.into(),
                old.map_or(serde_json::Value::Null, |old| {
                    old.frontiers.header_best.hash.to_string().into()
                }),
            );
            row.insert(
                hs_trace::NEW_SELECTED_HEIGHT.into(),
                u64::from(new.frontiers.header_best.height.0).into(),
            );
            row.insert(
                hs_trace::NEW_SELECTED_HASH.into(),
                new.frontiers.header_best.hash.to_string().into(),
            );
        });
    }

    fn observe_vct_root_repair(&mut self, status: zakura_header_chain::VctRootRepairStatus) {
        if self.vct_repair_status == status {
            return;
        }
        self.vct_repair_status = status;
        self.schedule_current_vct_repair();
    }

    fn schedule_current_vct_repair(&mut self) {
        let Some(desired) = self
            .committed_snapshot
            .as_ref()
            .and_then(|snapshot| vct_repair_task(snapshot, self.vct_repair_status))
        else {
            self.retire_vct_repair();
            return;
        };
        let preserves_current = self.vct_repair.current().is_some_and(|task| {
            task.repair_generation == desired.repair_generation
                && task.height == desired.height
                && task.owner.header_generation == desired.owner.header_generation
                && task.owner.verified_generation == desired.owner.verified_generation
                && task.owner.branch == desired.owner.branch
        });
        if preserves_current {
            return;
        }
        self.retire_vct_repair();
        let replaced = self.vct_repair.insert(desired);
        debug_assert!(
            replaced.is_none(),
            "the sole repair is cleared before scheduling its replacement"
        );
        metrics::counter!("sync.header.vct.repair.scheduled.total").increment(1);
        if let Some(task) = self.vct_repair.current() {
            self.emit_vct_repair_state(task, "schedule", None);
        }
        self.request_vct_repair_context();
    }

    fn request_vct_repair_context(&mut self) {
        let now = Instant::now();
        if let Some(task) = self.vct_repair.current_mut() {
            task.resume_retry_cycle(now);
        }
        let Some(task) = self.vct_repair.needs_context() else {
            return;
        };
        let owner = task.owner;
        let height = task.height;
        if self.dispatch_action(HeaderPortOperation::QueryVctRepairContext { owner, height }) {
            let deadline = now + self.startup.request_timeout;
            let _ = self
                .vct_repair
                .get_mut(owner)
                .expect("the context-needing repair remains owned during synchronous dispatch")
                .mark_context_requested(deadline, deadline + VCT_REPAIR_RETRY_INTERVAL);
            if let Some(task) = self.vct_repair.get(owner) {
                self.emit_vct_repair_state(task, "context_request", None);
            }
        }
    }

    fn handle_vct_repair_context_ready(
        &mut self,
        owner: zakura_header_chain::BodyWorkOwner,
        result: VctRepairContextResult,
    ) {
        if self
            .vct_repair
            .get(owner)
            .is_none_or(|task| !matches!(task.state, RepairPolicyState::QueryingContext { .. }))
        {
            return;
        }
        match result {
            VctRepairContextResult::Resolved(context) => {
                if self
                    .vct_repair
                    .get_mut(owner)
                    .expect("the exact scheduled repair was checked above")
                    .resolve(context)
                    .is_err()
                {
                    self.vct_repair.remove(owner);
                    return;
                }
                if let Some(task) = self.vct_repair.get(owner) {
                    self.emit_vct_repair_state(task, "context_resolved", Some("resolved"));
                }
                self.try_assign_vct_repair();
            }
            VctRepairContextResult::Stale => {
                if let Some(task) = self.vct_repair.get(owner) {
                    self.emit_vct_repair_state(task, "terminal", Some("stale"));
                }
                self.vct_repair.remove(owner);
            }
            VctRepairContextResult::Unavailable => {
                let task = self
                    .vct_repair
                    .get_mut(owner)
                    .expect("the exact pending context read was checked above");
                let _ = task.context_unavailable(Instant::now() + VCT_REPAIR_RETRY_INTERVAL);
                let task = task.clone();
                self.emit_vct_repair_state(&task, "retry", Some("unavailable"));
                metrics::counter!("sync.header.vct.repair.context_unavailable.total").increment(1);
            }
        }
    }

    fn try_assign_vct_repair(&mut self) {
        let now = Instant::now();
        if let Some(task) = self.vct_repair.current_mut() {
            task.resume_retry_cycle(now);
        }
        let Some(task) = self.vct_repair.ready().cloned() else {
            return;
        };
        let RepairPolicyState::Ready { context } = &task.state else {
            return;
        };
        if task.supplier_cycle_exhausted() {
            let _ = self
                .vct_repair
                .get_mut(task.owner)
                .expect("the ready repair was cloned above")
                .defer_retry_until(now + VCT_REPAIR_RETRY_INTERVAL);
            return;
        }
        let Some(predecessor) = context.locator.entries().first().copied() else {
            return;
        };
        let response_bytes = headers_response_bytes(&self.startup.network, AuxSchema::V1, 1)
            .expect("one fixed-width response fits in usize");
        let mut candidates: Vec<_> = self
            .peer_state
            .iter()
            .filter_map(|(peer, state)| {
                let status = state.last_status.as_ref()?;
                (status.selected_tip_hash == task.owner.branch.target_tip_hash
                    && status.selected_tip_height >= context.target.height
                    && status.oldest_retained_height <= predecessor.height
                    && status.max_headers_per_response != 0
                    && status.max_inflight_requests != 0
                    && usize::try_from(status.max_message_bytes).unwrap_or(usize::MAX)
                        >= response_bytes
                    && status.tree_aux_schema_mask & AuxSchema::V1.mask_bit() != 0
                    && self.peer_work_queue.active(peer).is_none())
                .then(|| {
                    (
                        peer.clone(),
                        source_id_from_peer(peer),
                        state.session.clone(),
                        status.clone(),
                    )
                })
            })
            .collect();
        candidates.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
        let candidates: Vec<_> = candidates
            .into_iter()
            .filter(|(_, source, _, _)| !task.tried_sources.contains(source))
            .collect();
        if candidates.is_empty() {
            let _ = self
                .vct_repair
                .get_mut(task.owner)
                .expect("the ready repair was cloned above")
                .defer_retry_until(now + VCT_REPAIR_RETRY_INTERVAL);
            return;
        }
        for (peer, source, session, mut status) in candidates {
            self.peer_work_queue.remove_unstarted(&peer);
            if self.peer_work_queue.reservable_header_count(1) != 1
                || !self.peer_work_queue.reserve_request(&peer, 1)
            {
                continue;
            }
            let request_id = match session.try_send_get_headers(
                &self.codec,
                task.owner.header,
                context.target.hash,
                &context.locator,
                1,
                AuxSchema::V1,
            ) {
                Ok(request_id) => request_id,
                Err(error) => {
                    self.peer_work_queue.cancel_request_reservation(&peer);
                    self.emit_queue_send_failed(&peer, &session, "GetHeaders", &error, None);
                    if let Some(current) = self.vct_repair.get_mut(task.owner) {
                        let _ = current.record_failed_source(source);
                        if current.supplier_cycle_exhausted() {
                            let _ = current.defer_retry_until(now + VCT_REPAIR_RETRY_INTERVAL);
                            return;
                        }
                    }
                    continue;
                }
            };
            self.emit_header_request(
                &peer,
                session.session_id(),
                task.owner.header,
                request_id,
                context.target.hash,
                &context.locator,
                1,
                AuxSchema::V1,
            );
            let wire_owner = task.owner.authority.bind(
                session.session_id(),
                NonZeroU64::new(request_id.get()).expect("header-sync request IDs are nonzero"),
            );
            if self.vct_repair.assign(task.owner, wire_owner).is_err() {
                session.cancel_request(request_id);
                self.peer_work_queue.cancel_request_reservation(&peer);
                return;
            }
            if let Some(task) = self.vct_repair.get(wire_owner) {
                self.emit_vct_repair_state(task, "assignment", Some("assigned"));
            }
            status.selected_tip_height = context.target.height;
            status.selected_tip_hash = context.target.hash;
            status.max_headers_per_response = 1;
            let target = AdvertisedHeaderTarget {
                scope: wire_owner.header,
                session_id: session.session_id(),
                status,
            };
            if self
                .peer_work_queue
                .stage(peer.clone(), target.clone(), PeerWorkPriority::Normal)
                != QueueWorkResult::NeedsLocator
                || !self.peer_work_queue.start(ActiveHeaderRequest {
                    purpose: HeaderTargetPurpose::SelectedAuxiliaryRepair {
                        selected_target: context.target,
                        repair_generation: task.repair_generation,
                    },
                    peer: peer.clone(),
                    source,
                    target,
                    sent_locator: context.locator.clone(),
                    request_id,
                    owner: wire_owner.into(),
                    common_ancestor: None,
                    entries: Vec::new(),
                    phase: HeaderTargetPhase::Receiving,
                    max_header_count: 1,
                    tree_aux_schema: AuxSchema::V1,
                })
            {
                session.cancel_request(request_id);
                self.peer_work_queue.cancel_request_reservation(&peer);
                self.retry_vct_repair(wire_owner, source, HeaderRequestTerminal::LocalError);
                return;
            }
            self.request_deadlines
                .insert(peer.clone(), Instant::now() + self.startup.request_timeout);
            metrics::counter!("sync.header.vct.repair.requested.total").increment(1);
            debug!(
                ?peer,
                height = context.target.height.0,
                hash = ?context.target.hash,
                "requested exact selected VCT metadata repair"
            );
            return;
        }
        let _ = self
            .vct_repair
            .get_mut(task.owner)
            .and_then(|task| task.defer_retry_until(now + VCT_REPAIR_RETRY_INTERVAL).ok());
    }

    fn retire_obsolete_work(&mut self, snapshot: &zakura_header_chain::EngineSnapshot) {
        self.peer_work_queue.retire_obsolete_unstarted(snapshot);
        let obsolete_served_paths: Vec<_> = self
            .served_paths
            .iter()
            .filter_map(|(peer, state)| {
                let (target_tip_hash, scope) = match state {
                    ServedPathState::Acquiring {
                        target_tip_hash,
                        scope,
                        ..
                    } => (*target_tip_hash, *scope),
                    ServedPathState::Active { target, scope, .. } => (target.hash, *scope),
                };
                (scope
                    != zakura_header_chain::HeaderWorkAuthority::for_target(
                        snapshot,
                        target_tip_hash,
                    ))
                .then(|| peer.clone())
            })
            .collect();
        for peer in obsolete_served_paths {
            self.served_path_deadlines.remove(&peer);
            match self.served_paths.remove(&peer) {
                Some(ServedPathState::Active {
                    session_id,
                    lease_id,
                    target,
                    scope,
                    pending_request,
                    ..
                }) => {
                    if let Some(pending) = pending_request {
                        self.send_headers_outcome(
                            &peer,
                            pending.request_id.get(),
                            target.hash,
                            HeadersOutcomeCode::Busy,
                        );
                    }
                    self.release_lease(peer, session_id, lease_id, scope);
                }
                Some(ServedPathState::Acquiring {
                    request_id,
                    target_tip_hash,
                    ..
                }) => self.send_headers_outcome(
                    &peer,
                    request_id.get(),
                    target_tip_hash,
                    HeadersOutcomeCode::Busy,
                ),
                None => {}
            }
        }
        if let Some(task) = self.vct_repair.retain_current(snapshot) {
            if let Some(peer) = self
                .peer_work_queue
                .active_owner(task.owner.into())
                .map(|active| active.peer.clone())
            {
                self.retire_peer_work(&peer, HeaderRequestTerminal::SnapshotObsolete);
            }
        }
        self.completed_targets
            .retain_current(snapshot.header_generation, snapshot.frontiers.finalized);
        for active in self.peer_work_queue.retire_obsolete_active(snapshot) {
            self.request_deadlines.remove(&active.peer);
            self.emit_request_terminal(&active, HeaderRequestTerminal::SnapshotObsolete);
            self.cancel_active_request(&active);
        }
        self.peer_work_queue.publish_phase_metrics();
    }

    fn send_status(&mut self, peer: &ZakuraPeerId) -> bool {
        let now = Instant::now();
        let Some((session, status)) = self.peer_state.get(peer).and_then(|state| {
            let publisher = state.status_publisher.as_ref()?;
            publisher
                .due(now)
                .then(|| (state.session.clone(), publisher.desired()))
        }) else {
            return false;
        };
        match session.try_send_status(&self.codec, status.clone()) {
            Ok(()) => {
                self.emit_status(
                    hs_trace::HEADER_STATUS_SENT,
                    peer,
                    session.session_id(),
                    &status,
                );
                if let Some(publisher) = self
                    .peer_state
                    .get_mut(peer)
                    .and_then(|state| state.status_publisher.as_mut())
                {
                    publisher.record_sent(status, now);
                }
                metrics::counter!("sync.header.peer.status.sent").increment(1);
                true
            }
            Err(error) => {
                if let Some(publisher) = self
                    .peer_state
                    .get_mut(peer)
                    .and_then(|state| state.status_publisher.as_mut())
                {
                    publisher.record_failed(now);
                }
                tracing::debug!(?peer, ?error, "failed to queue header-sync Status");
                self.emit_queue_send_failed(peer, &session, "Status", &error, None);
                false
            }
        }
    }

    fn refresh_statuses(&mut self) {
        let now = Instant::now();
        self.retry_pending_lease_releases(now);
        self.retire_timed_out_requests(now);
        self.release_idle_served_paths(now);
        if self.prune_unproductive_cooldowns(now) {
            self.publish_peer_state();
        }
        let peers: Vec<_> = self
            .peer_state
            .iter()
            .filter(|(_, state)| {
                state
                    .status_publisher
                    .as_ref()
                    .is_some_and(|publisher| publisher.due(now))
            })
            .map(|(peer, _)| peer.clone())
            .collect();
        for peer in peers {
            self.send_status(&peer);
        }
        self.request_vct_repair_context();
        self.try_assign_vct_repair();
    }

    fn next_maintenance_deadline(&self) -> Instant {
        let status_deadline = self
            .peer_state
            .values()
            .filter_map(|state| {
                state
                    .status_publisher
                    .as_ref()
                    .map(StatusPublisher::next_deadline)
            })
            .min();
        status_deadline
            .into_iter()
            .chain(self.request_deadlines.values().copied())
            .chain(
                self.vct_repair
                    .current()
                    .and_then(RepairRequirement::next_deadline),
            )
            .chain(self.served_path_deadlines.values().copied())
            .chain(self.lease_release_retry_at)
            .min()
            .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(60))
    }

    fn retire_timed_out_requests(&mut self, now: Instant) {
        let timed_out: Vec<_> = self
            .request_deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(peer, _)| peer.clone())
            .collect();
        for peer in timed_out {
            let repair = self.peer_work_queue.active(&peer).and_then(|active| {
                matches!(
                    active.purpose,
                    HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
                )
                .then(|| {
                    (
                        active
                            .owner
                            .body_owner()
                            .expect("an auxiliary repair has body authority"),
                        active.source,
                    )
                })
            });
            if let Some((owner, source)) = repair {
                if let Some(task) = self.vct_repair.get(owner) {
                    self.emit_vct_repair_state(task, "timeout", Some("timed_out"));
                }
                self.retry_vct_repair(owner, source, HeaderRequestTerminal::TimedOut);
                metrics::counter!("sync.header.vct.repair.timed_out.total").increment(1);
            } else {
                let session_id = self
                    .peer_work_queue
                    .active(&peer)
                    .map(|active| active.owner.session_id());
                self.retire_peer_work(&peer, HeaderRequestTerminal::TimedOut);
                metrics::counter!("sync.header.target.timed_out.total").increment(1);
                if let Some(session_id) = session_id {
                    self.charge_unproductive_request(&peer, session_id, "unresponsive");
                }
            }
        }
    }

    fn release_idle_served_paths(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .served_path_deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(peer, _)| peer.clone())
            .collect();
        for peer in expired {
            if matches!(
                self.served_paths.get(&peer),
                Some(ServedPathState::Active { .. })
            ) {
                self.release_served_path(&peer);
            } else {
                if let Some(ServedPathState::Acquiring {
                    request_id,
                    target_tip_hash,
                    ..
                }) = self.served_paths.remove(&peer)
                {
                    self.send_headers_outcome(
                        &peer,
                        request_id.get(),
                        target_tip_hash,
                        HeadersOutcomeCode::Busy,
                    );
                }
                self.served_path_deadlines.remove(&peer);
            }
            metrics::counter!("sync.header.serve.timed_out.total").increment(1);
        }
    }

    fn served_page_count(&self, requested: u32, tree_aux_schema: AuxSchema) -> u32 {
        let byte_limited = headers_response_capacity(
            &self.startup.network,
            tree_aux_schema,
            usize::try_from(self.serving_limits.max_message_bytes()).unwrap_or(usize::MAX),
        );
        requested
            .min(self.serving_limits.max_headers_per_response())
            .min(byte_limited)
            .min(MAX_HS_RANGE)
    }

    fn send_headers_outcome(
        &self,
        peer: &ZakuraPeerId,
        request_id: u64,
        target_tip_hash: block::Hash,
        outcome: HeadersOutcomeCode,
    ) {
        let Some(state) = self.peer_state.get(peer) else {
            return;
        };
        if let Err(error) = state.session.try_send_headers_outcome(
            &self.codec,
            HeadersOutcome {
                request_id,
                target_tip_hash,
                outcome,
            },
        ) {
            self.emit_queue_send_failed(
                peer,
                &state.session,
                "HeadersOutcome",
                &error,
                Some(request_id),
            );
        } else {
            let session_id = state.session.session_id();
            let direction = state.session.direction();
            self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
                row.insert(hs_trace::EVENT.into(), hs_trace::HEADER_OUTCOME.into());
                row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
                row.insert(hs_trace::SESSION_ID.into(), session_id.into());
                row.insert(
                    hs_trace::DIRECTION.into(),
                    header_direction_label(direction).into(),
                );
                row.insert(hs_trace::REQUEST_ID.into(), request_id.into());
                row.insert(
                    hs_trace::TARGET_HASH.into(),
                    target_tip_hash.to_string().into(),
                );
                row.insert(
                    hs_trace::OUTCOME.into(),
                    headers_outcome_label(outcome).into(),
                );
            });
        }
    }

    fn release_served_path(&mut self, peer: &ZakuraPeerId) {
        self.served_path_deadlines.remove(peer);
        let Some(ServedPathState::Active {
            session_id,
            lease_id,
            target,
            scope,
            pending_request,
            ..
        }) = self.served_paths.remove(peer)
        else {
            return;
        };
        if let Some(pending) = pending_request {
            self.send_headers_outcome(
                peer,
                pending.request_id.get(),
                target.hash,
                HeadersOutcomeCode::Busy,
            );
        }
        self.release_lease(peer.clone(), session_id, lease_id, scope);
    }

    fn release_lease(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        lease_id: u64,
        scope: zakura_header_chain::HeaderWorkAuthority,
    ) {
        let release = PendingLeaseRelease {
            peer,
            session_id,
            lease_id,
            scope,
        };
        if !self.dispatch_action(release.action()) {
            self.pending_lease_releases.push_back(release);
            self.lease_release_retry_at = Some(Instant::now() + LEASE_RELEASE_RETRY_INTERVAL);
        }
    }

    fn retry_pending_lease_releases(&mut self, now: Instant) {
        while let Some(release) = self.pending_lease_releases.pop_front() {
            if !self.dispatch_action(release.action()) {
                self.pending_lease_releases.push_front(release);
                self.lease_release_retry_at = Some(now + LEASE_RELEASE_RETRY_INTERVAL);
                return;
            }
        }
        self.lease_release_retry_at = None;
    }

    fn admitted_count(&self, direction: ServicePeerDirection) -> usize {
        self.peer_state
            .values()
            .filter(|state| state.session.direction() == direction)
            .count()
    }

    fn publish_peer_state(&self) {
        let snapshot = ServicePeerSnapshot::new(
            self.admitted_count(ServicePeerDirection::Inbound),
            self.admitted_count(ServicePeerDirection::Outbound),
            self.startup.config.peer_limits,
        );
        let _ = self.peers.send(snapshot);
        let mut admitted_node_ids: Vec<_> = self
            .peer_state
            .keys()
            .filter_map(node_id_from_peer)
            .collect();
        admitted_node_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        admitted_node_ids.dedup();
        let now = Instant::now();
        let mut backed_off_node_ids: Vec<_> = self
            .unproductive_peer_cooldowns
            .iter()
            .filter(|(_, until)| **until > now)
            .filter_map(|(peer, _)| node_id_from_peer(peer))
            .collect();
        backed_off_node_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        backed_off_node_ids.dedup();
        let tip = *self.tip.borrow();
        let _ = self.candidates.send(ZakuraHeaderSyncCandidateState {
            target_height: next_height(tip.0),
            admitted_node_ids,
            backed_off_node_ids,
        });
    }

    fn emit_peer_lifecycle(
        &self,
        event: &'static str,
        peer: &ZakuraPeerId,
        session_id: u64,
        direction: ServicePeerDirection,
        reason: Option<&'static str>,
    ) {
        let inbound = self.admitted_count(ServicePeerDirection::Inbound);
        let outbound = self.admitted_count(ServicePeerDirection::Outbound);
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), event.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), session_id.into());
            row.insert(
                hs_trace::DIRECTION.into(),
                header_direction_label(direction).into(),
            );
            row.insert(hs_trace::INBOUND_COUNT.into(), inbound.into());
            row.insert(hs_trace::OUTBOUND_COUNT.into(), outbound.into());
            row.insert(
                hs_trace::REASON.into(),
                reason.map_or(serde_json::Value::Null, Into::into),
            );
        });
    }

    fn emit_status(
        &self,
        event: &'static str,
        peer: &ZakuraPeerId,
        session_id: u64,
        status: &Status,
    ) {
        let direction = self
            .peer_state
            .get(peer)
            .map(|state| header_direction_label(state.session.direction()));
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), event.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), session_id.into());
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            row.insert(
                hs_trace::WORK_ANCHOR_HEIGHT.into(),
                u64::from(status.work_anchor_height.0).into(),
            );
            row.insert(
                hs_trace::WORK_ANCHOR_HASH.into(),
                status.work_anchor_hash.to_string().into(),
            );
            row.insert(
                hs_trace::SELECTED_TIP_HEIGHT.into(),
                u64::from(status.selected_tip_height.0).into(),
            );
            row.insert(
                hs_trace::SELECTED_TIP_HASH.into(),
                status.selected_tip_hash.to_string().into(),
            );
            row.insert(
                hs_trace::MAX_HEADERS_PER_RESPONSE.into(),
                u64::from(status.max_headers_per_response).into(),
            );
            row.insert(
                hs_trace::MAX_INFLIGHT_REQUESTS.into(),
                u64::from(status.max_inflight_requests).into(),
            );
            row.insert(
                hs_trace::MAX_MESSAGE_BYTES.into(),
                u64::from(status.max_message_bytes).into(),
            );
            row.insert(
                hs_trace::TREE_AUX_SCHEMA_MASK.into(),
                u64::from(status.tree_aux_schema_mask).into(),
            );
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_header_request(
        &self,
        peer: &ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::HeaderWorkAuthority,
        request_id: HeaderSyncRequestId,
        target_hash: block::Hash,
        locator: &zakura_header_chain::HeaderLocator,
        max_header_count: u32,
        tree_aux_schema: AuxSchema,
    ) {
        let direction = self
            .peer_state
            .get(peer)
            .map(|state| header_direction_label(state.session.direction()));
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), hs_trace::HEADER_REQUEST_SENT.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), session_id.into());
            row.insert(
                hs_trace::STREAM_VERSION.into(),
                u64::from(ZAKURA_HEADER_SYNC_STREAM_VERSION).into(),
            );
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            insert_header_scope(row, scope);
            row.insert(hs_trace::REQUEST_ID.into(), request_id.get().into());
            row.insert(hs_trace::TARGET_HASH.into(), target_hash.to_string().into());
            row.insert(
                hs_trace::LOCATOR_COUNT.into(),
                u64::try_from(locator.entries().len())
                    .unwrap_or(u64::MAX)
                    .into(),
            );
            row.insert(
                hs_trace::LOCATOR_HEAD.into(),
                locator
                    .entries()
                    .first()
                    .map_or(serde_json::Value::Null, |entry| {
                        entry.hash.to_string().into()
                    }),
            );
            row.insert(
                hs_trace::HEADER_COUNT.into(),
                u64::from(max_header_count).into(),
            );
            row.insert(
                hs_trace::TREE_AUX_SCHEMA.into(),
                aux_schema_label(tree_aux_schema).into(),
            );
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_header_response(
        &self,
        event: &'static str,
        peer: &ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::HeaderWorkAuthority,
        request_id: u64,
        target_hash: block::Hash,
        common_ancestor_height: block::Height,
        common_ancestor_hash: block::Hash,
        header_count: usize,
        complete: bool,
        tree_aux_schema: AuxSchema,
    ) {
        let direction = self
            .peer_state
            .get(peer)
            .map(|state| header_direction_label(state.session.direction()));
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(hs_trace::EVENT.into(), event.into());
            row.insert(hs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(hs_trace::SESSION_ID.into(), session_id.into());
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            insert_header_scope(row, scope);
            row.insert(hs_trace::REQUEST_ID.into(), request_id.into());
            row.insert(hs_trace::TARGET_HASH.into(), target_hash.to_string().into());
            row.insert(
                hs_trace::COMMON_ANCESTOR_HEIGHT.into(),
                u64::from(common_ancestor_height.0).into(),
            );
            row.insert(
                hs_trace::COMMON_ANCESTOR_HASH.into(),
                common_ancestor_hash.to_string().into(),
            );
            row.insert(
                hs_trace::HEADER_COUNT.into(),
                u64::try_from(header_count).unwrap_or(u64::MAX).into(),
            );
            row.insert(hs_trace::COMPLETE.into(), complete.into());
            row.insert(
                hs_trace::TREE_AUX_SCHEMA.into(),
                aux_schema_label(tree_aux_schema).into(),
            );
        });
    }

    fn emit_queue_send_failed(
        &self,
        peer: &ZakuraPeerId,
        session: &PeerSession,
        message: &'static str,
        error: &OrderedSendError,
        request_id: Option<u64>,
    ) {
        self.startup.trace.emit_with(QUEUE_SEND_TABLE, |row| {
            row.insert(qs_trace::EVENT.into(), qs_trace::QUEUE_SEND_FAILED.into());
            row.insert(qs_trace::SERVICE.into(), "header_sync".into());
            row.insert(qs_trace::MESSAGE.into(), message.into());
            row.insert(qs_trace::PEER.into(), trace_peer_label(peer).into());
            row.insert(qs_trace::SESSION_ID.into(), session.session_id().into());
            row.insert(
                qs_trace::ERROR.into(),
                ordered_send_error_label(error).into(),
            );
            row.insert(
                qs_trace::QUEUE_CAPACITY.into(),
                u64::try_from(session.outbound_capacity())
                    .unwrap_or(u64::MAX)
                    .into(),
            );
            row.insert(
                qs_trace::QUEUE_MAX_CAPACITY.into(),
                u64::try_from(session.outbound_max_capacity())
                    .unwrap_or(u64::MAX)
                    .into(),
            );
            row.insert(
                qs_trace::REQUEST_ID.into(),
                request_id.map_or(serde_json::Value::Null, Into::into),
            );
        });
    }

    fn dispatch_action(&mut self, action: HeaderPortOperation) -> bool {
        match self.startup.port_dispatch {
            PortDispatch::Direct => self.dispatch_direct_port_operation(action),
            #[cfg(any(test, feature = "zakura-testkit"))]
            PortDispatch::External => self.dispatch_external_port_operation(action),
        }
    }

    fn dispatch_direct_port_operation(&mut self, action: HeaderPortOperation) -> bool {
        use zakura_node_services::header_chain as port;

        if let HeaderPortOperation::QueryHeaderLocator { peer, .. } = &action {
            if !self.pending_locator_queries.insert(peer.clone()) {
                return true;
            }
        }
        let panic_context = self.port_panic_context(&action);
        let header_chain = self.startup.header_chain_port.clone();
        let request_timeout = self.startup.request_timeout;
        let operation: Pin<Box<dyn Future<Output = HeaderSyncPortCompletion> + Send + 'static>> =
            match action {
                HeaderPortOperation::Misbehavior { peer, reason } => {
                    tracing::debug!(?peer, ?reason, "recorded Zakura header-sync peer violation");
                    return true;
                }
                HeaderPortOperation::DropPeer {
                    peer,
                    session_id,
                    reason,
                } => {
                    tracing::debug!(
                        ?peer,
                        session_id,
                        reason,
                        "dropped an unproductive Zakura header-sync peer"
                    );
                    return true;
                }
                HeaderPortOperation::QueryHeaderLocator {
                    peer,
                    session_id: _,
                    target_tip_hash: _,
                    scope,
                } => Box::pin(async move {
                    let locator = match tokio::time::timeout(
                        request_timeout,
                        header_chain.continuation_locator(),
                    )
                    .await
                    {
                        Ok(Ok(locator)) => locator,
                        Ok(Err(error)) => {
                            tracing::debug!(?peer, ?error, "header locator unavailable");
                            None
                        }
                        Err(_) => {
                            tracing::debug!(?peer, "header locator query timed out");
                            None
                        }
                    };
                    Box::new(move |reactor: &mut HeaderSyncReactor| {
                        reactor.finish_header_locator_query(peer, scope, locator);
                    }) as HeaderSyncPortCompletion
                }),
                HeaderPortOperation::QueryVctRepairContext { owner, height } => {
                    Box::pin(async move {
                        let result = match tokio::time::timeout(
                            request_timeout,
                            header_chain.vct_repair_context(owner, height),
                        )
                        .await
                        {
                            Ok(Ok(port::VctRepairContextReply::Resolved(context))) => {
                                VctRepairContextResult::Resolved(context)
                            }
                            Ok(Ok(port::VctRepairContextReply::Stale)) => {
                                VctRepairContextResult::Stale
                            }
                            Ok(Err(error)) => {
                                tracing::debug!(?owner, ?error, "VCT repair context unavailable");
                                VctRepairContextResult::Unavailable
                            }
                            Err(_) => {
                                tracing::debug!(?owner, "VCT repair context timed out");
                                VctRepairContextResult::Unavailable
                            }
                        };
                        Box::new(move |reactor: &mut HeaderSyncReactor| {
                            reactor.handle_vct_repair_context_ready(owner, result);
                        }) as HeaderSyncPortCompletion
                    })
                }
                HeaderPortOperation::AcquirePath {
                    peer,
                    session_id,
                    scope,
                    request,
                } => Box::pin(async move {
                    let reply = header_chain
                        .acquire_header_path(port::AcquirePath {
                            source: source_id_from_peer(&peer),
                            session_id,
                            scope,
                            target_tip_hash: request.target_tip_hash,
                            locator_hashes: request.locator_hashes.clone(),
                        })
                        .await;
                    let (result, acquired) = match reply {
                        Ok(port::AcquirePathReply::Acquired(path)) => {
                            let lease_id = path.handle_id();
                            (
                                HeaderPathLeaseResult::Acquired(HeaderPathLease {
                                    lease_id,
                                    common_ancestor: path.common_ancestor,
                                    target: path.target,
                                    scope: path.scope,
                                }),
                                Some((lease_id, path)),
                            )
                        }
                        Ok(reply) => (
                            HeaderPathLeaseResult::Outcome(match reply {
                                port::AcquirePathReply::TargetNotRetained => {
                                    HeadersOutcomeCode::TargetNotRetained
                                }
                                port::AcquirePathReply::NoLocatorIntersection => {
                                    HeadersOutcomeCode::NoLocatorIntersection
                                }
                                port::AcquirePathReply::HistoryPruned => {
                                    HeadersOutcomeCode::HistoryPruned
                                }
                                port::AcquirePathReply::Busy => HeadersOutcomeCode::Busy,
                                port::AcquirePathReply::Acquired(_) => {
                                    unreachable!("acquired paths are handled above")
                                }
                            }),
                            None,
                        ),
                        Err(error) => {
                            tracing::debug!(?peer, ?error, "retained header path unavailable");
                            (
                                HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::Busy),
                                None,
                            )
                        }
                    };
                    Box::new(move |reactor: &mut HeaderSyncReactor| {
                        if let Some((lease_id, path)) = acquired {
                            reactor.retained_paths.insert(lease_id, *path);
                        }
                        reactor.handle_header_path_lease_ready(
                            peer, session_id, scope, request, result,
                        );
                    }) as HeaderSyncPortCompletion
                }),
                HeaderPortOperation::ReadPath {
                    peer,
                    session_id,
                    lease_id,
                    scope,
                    request_id,
                    target_tip_hash,
                    after_hash,
                    max_header_count,
                    tree_aux_schema,
                } => {
                    let Some(path) = self.retained_paths.get(&lease_id).cloned() else {
                        return false;
                    };
                    if path.scope != scope {
                        return false;
                    }
                    Box::pin(async move {
                        let result = match header_chain
                            .read_header_path(
                                path,
                                port::ReadPath {
                                    after_hash,
                                    max_header_count,
                                    want_tree_aux: tree_aux_schema == AuxSchema::V1,
                                },
                            )
                            .await
                        {
                            Ok(port::ReadPathReply::Page(page)) => {
                                assemble_port_header_path_page(lease_id, *page, tree_aux_schema)
                                    .map(|page| HeaderPathPageResult::Page(Box::new(page)))
                                    .unwrap_or(HeaderPathPageResult::Unavailable)
                            }
                            Ok(port::ReadPathReply::Unavailable) => {
                                HeaderPathPageResult::Unavailable
                            }
                            Err(error) => {
                                tracing::debug!(?peer, ?error, "retained header page unavailable");
                                HeaderPathPageResult::Unavailable
                            }
                        };
                        Box::new(move |reactor: &mut HeaderSyncReactor| {
                            reactor.handle_header_path_page_ready(
                                peer,
                                session_id,
                                scope,
                                request_id,
                                target_tip_hash,
                                result,
                            );
                        }) as HeaderSyncPortCompletion
                    })
                }
                HeaderPortOperation::ReleaseHeaderPath {
                    peer: _,
                    session_id: _,
                    lease_id,
                    scope,
                } => {
                    let Some(path) = self.retained_paths.remove(&lease_id) else {
                        return true;
                    };
                    if path.scope != scope {
                        return true;
                    }
                    Box::pin(async move {
                        let result = header_chain.release_header_path(path).await;
                        Box::new(move |_reactor: &mut HeaderSyncReactor| {
                            if let Err(error) = result {
                                tracing::debug!(
                                    lease_id,
                                    ?error,
                                    "failed to release retained header path"
                                );
                            }
                        }) as HeaderSyncPortCompletion
                    })
                }
                HeaderPortOperation::PrepareHeaderTarget {
                    purpose,
                    peer,
                    source,
                    owner,
                    common_ancestor,
                    target,
                    completion,
                    entries,
                } => Box::pin(async move {
                    if matches!(purpose, HeaderTargetPurpose::SelectedAuxiliaryRepair { .. })
                        && !matches!(
                            completion,
                            zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                                selected_target,
                                ..
                            } if selected_target == target && entries.len() == 1
                        )
                    {
                        let result = HeaderTargetPreparationResult::Failed(std::sync::Arc::new(
                            zakura_header_chain::HeaderChainError::stale_target(
                                zakura_header_chain::ErrorSubject::Branch(
                                    owner.header_authority().branch,
                                ),
                            ),
                        ));
                        return Box::new(move |reactor: &mut HeaderSyncReactor| {
                            reactor.handle_header_target_prepared(peer, source, owner, result);
                        }) as HeaderSyncPortCompletion;
                    }
                    let entries = match port::TargetEntries::try_from(
                        entries
                            .into_iter()
                            .map(port_header_entry)
                            .collect::<Vec<_>>(),
                    ) {
                        Ok(entries) => entries,
                        Err(error) => {
                            let result =
                                HeaderTargetPreparationResult::Failed(std::sync::Arc::new(
                                    zakura_header_chain::HeaderChainError::malformed_protocol(
                                        zakura_header_chain::ErrorSubject::Request {
                                            source,
                                            request_id: owner.request_id(),
                                        },
                                        zakura_header_chain::RuleId::new("LC-WIRE-08"),
                                        source,
                                        Some(Box::new(error)),
                                    ),
                                ));
                            return Box::new(move |reactor: &mut HeaderSyncReactor| {
                                reactor.handle_header_target_prepared(peer, source, owner, result);
                            }) as HeaderSyncPortCompletion;
                        }
                    };
                    let result = header_chain
                        .prepare_header_target(port::PrepareHeaderTarget {
                            source,
                            owner,
                            common_ancestor,
                            target,
                            entries,
                            completion,
                        })
                        .await
                        .map(HeaderTargetPreparationResult::Prepared)
                        .unwrap_or_else(HeaderTargetPreparationResult::Failed);
                    Box::new(move |reactor: &mut HeaderSyncReactor| {
                        reactor.handle_header_target_prepared(peer, source, owner, result);
                    }) as HeaderSyncPortCompletion
                }),
                HeaderPortOperation::ApplyHeaderTarget {
                    purpose: _,
                    peer,
                    source,
                    owner,
                    target,
                } => Box::pin(async move {
                    let result = header_chain
                        .apply_header_target(target)
                        .await
                        .map(|outcome| match outcome {
                            port::ApplyHeaderTargetOutcome::Applied => {
                                HeaderTargetAdmissionResult::Applied
                            }
                            port::ApplyHeaderTargetOutcome::ResourceStalled(receipt) => {
                                HeaderTargetAdmissionResult::ResourceStalled(receipt)
                            }
                        })
                        .unwrap_or_else(HeaderTargetAdmissionResult::Failed);
                    Box::new(move |reactor: &mut HeaderSyncReactor| {
                        reactor.handle_header_target_admission_ready(peer, source, owner, result);
                    }) as HeaderSyncPortCompletion
                }),
            };
        let operation =
            AssertUnwindSafe(operation)
                .catch_unwind()
                .map(move |result| match result {
                    Ok(completion) => PortOperationResult::Completed(completion),
                    Err(_) => PortOperationResult::Panicked(Box::new(panic_context)),
                });
        self.pending_port_operations.push(Box::pin(operation));
        true
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    fn dispatch_external_port_operation(&mut self, action: HeaderPortOperation) -> bool {
        match self.actions.try_send(action) {
            Ok(()) => true,
            Err(error) => {
                tracing::debug!(?error, "header-sync action queue unavailable");
                false
            }
        }
    }

    fn port_panic_context(&self, action: &HeaderPortOperation) -> PortPanicContext {
        let (operation, peer, session_id, scope, owner, target_tip_hash, lease_id) = match action {
            HeaderPortOperation::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
                scope,
            } => (
                "query_header_locator",
                Some(peer.clone()),
                Some(*session_id),
                Some(*scope),
                None,
                Some(*target_tip_hash),
                None,
            ),
            HeaderPortOperation::QueryVctRepairContext { owner, .. } => (
                "vct_repair_context",
                None,
                None,
                Some(owner.header),
                Some((*owner).into()),
                None,
                None,
            ),
            HeaderPortOperation::AcquirePath {
                peer,
                session_id,
                scope,
                request,
            } => (
                "acquire_header_path",
                Some(peer.clone()),
                Some(*session_id),
                Some(*scope),
                None,
                Some(request.target_tip_hash),
                None,
            ),
            HeaderPortOperation::ReadPath {
                peer,
                session_id,
                lease_id,
                scope,
                target_tip_hash,
                ..
            } => (
                "read_header_path",
                Some(peer.clone()),
                Some(*session_id),
                Some(*scope),
                None,
                Some(*target_tip_hash),
                Some(*lease_id),
            ),
            HeaderPortOperation::ReleaseHeaderPath {
                peer,
                session_id,
                lease_id,
                scope,
            } => (
                "release_header_path",
                Some(peer.clone()),
                Some(*session_id),
                Some(*scope),
                None,
                None,
                Some(*lease_id),
            ),
            HeaderPortOperation::PrepareHeaderTarget {
                peer,
                owner,
                target,
                ..
            } => (
                "prepare_header_target",
                Some(peer.clone()),
                Some(owner.session_id()),
                Some(owner.header_authority()),
                Some(*owner),
                Some(target.hash),
                None,
            ),
            HeaderPortOperation::ApplyHeaderTarget { peer, owner, .. } => (
                "apply_header_target",
                Some(peer.clone()),
                Some(owner.session_id()),
                Some(owner.header_authority()),
                Some(*owner),
                Some(owner.header_authority().branch.target_tip_hash),
                None,
            ),
            HeaderPortOperation::Misbehavior { peer, .. } => (
                "misbehavior",
                Some(peer.clone()),
                None,
                None,
                None,
                None,
                None,
            ),
            HeaderPortOperation::DropPeer {
                peer, session_id, ..
            } => (
                "drop_peer",
                Some(peer.clone()),
                Some(*session_id),
                None,
                None,
                None,
                None,
            ),
        };
        let session = peer.as_ref().and_then(|peer| {
            self.peer_state.get(peer).and_then(|state| {
                (session_id == Some(state.session.session_id())).then(|| state.session.clone())
            })
        });
        PortPanicContext {
            operation,
            peer,
            session_id,
            session,
            scope,
            owner,
            target_tip_hash,
            lease_id,
        }
    }

    fn handle_port_panic(&mut self, context: PortPanicContext) {
        metrics::counter!(
            "sync.header.port.panicked",
            "operation" => context.operation
        )
        .increment(1);

        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_PEER_VIOLATION.into(),
            );
            row.insert(
                hs_trace::PEER.into(),
                context
                    .peer
                    .as_ref()
                    .map_or(serde_json::Value::Null, |peer| {
                        trace_peer_label(peer).into()
                    }),
            );
            row.insert(
                hs_trace::SESSION_ID.into(),
                context
                    .session_id
                    .map_or(serde_json::Value::Null, Into::into),
            );
            row.insert(
                hs_trace::DIRECTION.into(),
                context
                    .session
                    .as_ref()
                    .map(|session| header_direction_label(session.direction()))
                    .map_or(serde_json::Value::Null, Into::into),
            );
            row.insert(hs_trace::REASON.into(), "port_future_panic".into());
            row.insert(hs_trace::BOUNDARY.into(), "port".into());
            row.insert(
                hs_trace::DISPOSITION.into(),
                if context.session.is_some() {
                    "disconnect"
                } else {
                    "record"
                }
                .into(),
            );
            row.insert(hs_trace::OPERATION.into(), context.operation.into());
        });

        if context.operation == "query_header_locator" {
            if let Some(peer) = context.peer.as_ref() {
                self.pending_locator_queries.remove(peer);
            }
        }

        if let Some(session) = context.session.as_ref() {
            session.disconnect_for_port_panic();
        }

        if context.operation == "vct_repair_context" {
            if let Some(owner) = context.owner {
                if let Some(owner) = owner.body_owner() {
                    self.handle_vct_repair_context_ready(
                        owner,
                        VctRepairContextResult::Unavailable,
                    );
                }
            }
            return;
        }

        let Some(peer) = context.peer.as_ref() else {
            return;
        };
        let source = source_id_from_peer(peer);
        let subject = context.owner.map_or_else(
            || {
                context.target_tip_hash.map_or(
                    zakura_header_chain::ErrorSubject::Local("header_sync_port"),
                    |hash| {
                        zakura_header_chain::ErrorSubject::Header(
                            zakura_header_chain::HeaderId::new(hash),
                        )
                    },
                )
            },
            |owner| zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
        );
        let failure = zakura_header_chain::HeaderChainError::local_resource(subject, None);
        self.handle_typed_failure(peer.clone(), source, &failure);

        if context.operation == "query_header_locator" {
            self.peer_work_queue.remove_unstarted(peer);
        }

        if let Some(owner) = context.owner {
            let matching = self
                .peer_work_queue
                .active(peer)
                .is_some_and(|active| active.owner == owner);
            if matching {
                let repair = self.peer_work_queue.active(peer).and_then(|active| {
                    matches!(
                        active.purpose,
                        HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
                    )
                    .then(|| {
                        (
                            active
                                .owner
                                .body_owner()
                                .expect("an auxiliary repair has body authority"),
                            active.source,
                        )
                    })
                });
                if let Some((owner, source)) = repair {
                    self.retry_vct_repair(owner, source, HeaderRequestTerminal::LocalError);
                } else {
                    self.retire_peer_work(peer, HeaderRequestTerminal::LocalError);
                }
            }
        }

        let owns_served_path = self.served_paths.get(peer).is_some_and(|state| {
            let (session_id, scope, lease_id) = match state {
                ServedPathState::Acquiring {
                    session_id, scope, ..
                } => (*session_id, *scope, None),
                ServedPathState::Active {
                    session_id,
                    scope,
                    lease_id,
                    ..
                } => (*session_id, *scope, Some(*lease_id)),
            };
            Some(session_id) == context.session_id
                && Some(scope) == context.scope
                && context
                    .lease_id
                    .is_none_or(|expected| lease_id == Some(expected))
        });
        if owns_served_path {
            self.served_path_deadlines.remove(peer);
            if let Some(ServedPathState::Active {
                session_id,
                lease_id,
                scope,
                ..
            }) = self.served_paths.remove(peer)
            {
                self.release_lease(peer.clone(), session_id, lease_id, scope);
            }
        }
    }

    fn registered_completion_authority(
        &self,
        peer: &ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: &zakura_header_chain::HeaderSyncWorkOwner,
        is_repair: bool,
    ) -> Result<
        (zakura_header_chain::HeaderWorkAuthority, &'static str),
        zakura_header_chain::StaleReason,
    > {
        let Some(current) = self.committed_snapshot.as_ref() else {
            return Err(zakura_header_chain::StaleReason::MissingOwner);
        };
        let decision = zakura_header_chain::Gate::check_registered(
            current,
            self.peer_work_queue.registered_attempt(peer),
            source,
            owner,
        );
        match decision {
            zakura_header_chain::CompletionDecision::Current => {
                Ok((owner.header_authority(), "current"))
            }
            zakura_header_chain::CompletionDecision::Stale(reason) => {
                let header = owner.header_authority();
                let registered =
                    self.peer_work_queue.registered_attempt(peer) == Some((source, *owner));
                let rebase_candidate = !is_repair
                    && owner.body_owner().is_none()
                    && registered
                    && header.header_generation.get() < current.header_generation.get()
                    && header.branch.anchor_hash != current.frontiers.finalized.hash;
                if !rebase_candidate {
                    return Err(reason);
                }
                Ok((
                    zakura_header_chain::HeaderWorkAuthority::for_target(
                        current,
                        header.branch.target_tip_hash,
                    ),
                    "rebase_candidate",
                ))
            }
        }
    }

    /// Keep exact ordinary header work alive across a monotone full-state finality advance.
    ///
    /// The rebase gate grants no durable authority.
    /// The serialized state planner authenticates the finality path and proves ancestry.
    /// The planner trims any finalized prefix and then rebases or rejects the insertion.
    /// Body-authorized VCT repair work remains bound to each generation.
    fn preparation_has_authority(
        &self,
        peer: &ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: &zakura_header_chain::HeaderSyncWorkOwner,
        is_repair: bool,
        header_count: usize,
    ) -> bool {
        let outcome = match self.registered_completion_authority(peer, source, owner, is_repair) {
            Ok((_, outcome)) => outcome,
            Err(reason) => {
                Self::record_stale_completion(reason);
                return false;
            }
        };
        let header_count = u64::try_from(header_count)
            .expect("the bounded header target count fits in a metric counter");
        metrics::counter!(
            "sync.header.target.preparation_gate.total",
            "outcome" => outcome
        )
        .increment(1);
        metrics::counter!(
            "sync.header.target.preparation_gate.headers.total",
            "outcome" => outcome
        )
        .increment(header_count);
        true
    }

    fn record_stale_completion(reason: zakura_header_chain::StaleReason) {
        metrics::counter!(
            "sync.header_chain.stale_completion.total",
            "kind" => format!("{reason:?}")
        )
        .increment(1);
    }

    /// Charges one unproductive request against `peer`'s exact session, dropping that
    /// session once it reaches the configured limit.
    ///
    /// Charge a strike only to the session that raised it.
    /// Discard a strike when a replacement session already exists.
    /// Return whether the reactor dropped the session.
    fn charge_unproductive_request(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        reason: &'static str,
    ) -> bool {
        let limit = self.startup.config.max_unproductive_header_requests;
        let Some(state) = self.peer_state.get_mut(peer) else {
            return false;
        };
        if state.session.session_id() != session_id {
            return false;
        }
        state.unproductive_requests = state.unproductive_requests.saturating_add(1);
        if limit == 0 || state.unproductive_requests < limit {
            return false;
        }
        self.drop_unproductive_peer(peer, session_id, reason);
        true
    }

    /// Clears `peer`'s strike count after its session supplies usable work.
    fn reset_unproductive_requests(&mut self, peer: &ZakuraPeerId, session_id: u64) {
        if let Some(state) = self.peer_state.get_mut(peer) {
            if state.session.session_id() == session_id {
                state.unproductive_requests = 0;
            }
        }
    }

    /// Closes one exact unproductive session and blocks its readmission for the cooldown.
    fn drop_unproductive_peer(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        reason: &'static str,
    ) {
        let Some(state) = self.peer_state.get(peer) else {
            return;
        };
        if state.session.session_id() != session_id {
            return;
        }
        state.session.cancel_token().cancel();

        let cooldown = self.startup.config.unproductive_peer_cooldown;
        if !cooldown.is_zero() {
            let now = Instant::now();
            let _ = self.prune_unproductive_cooldowns(now);
            self.unproductive_peer_cooldowns
                .insert(peer.clone(), now + cooldown);
        }
        metrics::counter!("sync.header.peer.dropped.total", "reason" => reason).increment(1);
        self.dispatch_action(HeaderPortOperation::DropPeer {
            peer: peer.clone(),
            session_id,
            reason,
        });
        self.handle_peer_disconnected(peer, session_id, reason);
    }

    fn prune_unproductive_cooldowns(&mut self, now: Instant) -> bool {
        let before = self.unproductive_peer_cooldowns.len();
        self.unproductive_peer_cooldowns
            .retain(|_, until| *until > now);
        self.unproductive_peer_cooldowns.len() != before
    }

    fn retire_peer_work(&mut self, peer: &ZakuraPeerId, terminal_outcome: HeaderRequestTerminal) {
        self.request_deadlines.remove(peer);
        let reserved = self.peer_work_queue.reserved_header_count(peer);
        let owned = self.peer_work_queue.owned_header_count(peer);
        if let Some(active) = self.peer_work_queue.remove(peer) {
            self.emit_request_terminal(&active, terminal_outcome);
            self.cancel_active_request(&active);
        }
        let released = reserved.saturating_add(owned);
        if released != 0 {
            metrics::counter!(
                "sync.header.chunk_budget.released.total",
                "terminal" => terminal_outcome.label()
            )
            .increment(u64::try_from(released).unwrap_or(u64::MAX));
        }
        self.peer_work_queue.publish_phase_metrics();
    }

    #[cfg(test)]
    fn clear_peer_work_for_test(&mut self, peer: &ZakuraPeerId) {
        self.request_deadlines.remove(peer);
        if let Some(active) = self.peer_work_queue.remove(peer) {
            self.cancel_active_request(&active);
        }
        self.peer_work_queue.publish_phase_metrics();
    }

    fn retire_all_peer_work(&mut self, terminal_outcome: HeaderRequestTerminal) {
        let peers: Vec<_> = self.peer_state.keys().cloned().collect();
        for peer in peers {
            self.retire_peer_work(&peer, terminal_outcome);
        }
    }

    fn emit_request_terminal(
        &self,
        active: &ActiveHeaderRequest,
        terminal_outcome: HeaderRequestTerminal,
    ) {
        if !terminal_outcome.needs_terminal_trace() {
            return;
        }
        let direction = self.peer_state.get(&active.peer).and_then(|state| {
            (state.session.session_id() == active.owner.session_id())
                .then(|| header_direction_label(state.session.direction()))
        });
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_REQUEST_TERMINAL.into(),
            );
            row.insert(hs_trace::PEER.into(), trace_peer_label(&active.peer).into());
            row.insert(
                hs_trace::SESSION_ID.into(),
                active.owner.session_id().into(),
            );
            row.insert(
                hs_trace::DIRECTION.into(),
                direction.map_or(serde_json::Value::Null, Into::into),
            );
            insert_header_scope(row, active.owner.header_authority());
            row.insert(hs_trace::REQUEST_ID.into(), active.request_id.get().into());
            row.insert(
                hs_trace::TARGET_HASH.into(),
                active.target.status.selected_tip_hash.to_string().into(),
            );
            row.insert(hs_trace::OUTCOME.into(), terminal_outcome.label().into());
        });
    }

    fn cancel_active_request(&self, active: &ActiveHeaderRequest) {
        let Some(state) = self.peer_state.get(&active.peer) else {
            return;
        };
        if state.session.session_id() == active.owner.session_id() {
            state.session.cancel_request(active.request_id);
        }
    }

    fn cancel_owned_request(
        &self,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
    ) {
        let Some(state) = self.peer_state.iter().find_map(|(peer, state)| {
            (state.session.session_id() == owner.session_id()
                && source_id_from_peer(peer) == source)
                .then_some(state)
        }) else {
            return;
        };
        let Some(request_id) = HeaderSyncRequestId::new(owner.request_id().get()) else {
            return;
        };
        state.session.cancel_request(request_id);
    }

    fn handle_typed_failure(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        error: &zakura_header_chain::HeaderChainError,
    ) {
        metrics::counter!(
            "sync.header.failure.total",
            "category" => error.category.metrics_label(),
            "attribution" => error.attribution.metrics_label(),
        )
        .increment(1);
        let zakura_header_chain::Attribution::HeaderPeer(attributed_source) = error.attribution
        else {
            return;
        };
        if attributed_source != source || !error.is_automatic_header_peer_fault() {
            return;
        }
        let reason = match error.category {
            zakura_header_chain::ErrorCategory::MalformedProtocol => {
                HeaderSyncMisbehavior::MalformedMessage
            }
            zakura_header_chain::ErrorCategory::InvalidHeader => {
                HeaderSyncMisbehavior::InvalidHeader
            }
            _ => return,
        };
        self.dispatch_misbehavior(peer, reason);
    }

    fn report_misbehavior(&mut self, peer: ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        let category = match reason {
            HeaderSyncMisbehavior::MalformedMessage => {
                zakura_header_chain::ErrorCategory::MalformedProtocol
            }
            HeaderSyncMisbehavior::InvalidHeader => {
                zakura_header_chain::ErrorCategory::InvalidHeader
            }
        };
        metrics::counter!(
            "sync.header.failure.total",
            "category" => category.metrics_label(),
            "attribution" => "header_peer",
        )
        .increment(1);
        self.dispatch_misbehavior(peer, reason);
    }

    fn dispatch_misbehavior(&mut self, peer: ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        let session_id = self
            .peer_state
            .get(&peer)
            .map(|state| state.session.session_id());
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_PEER_VIOLATION.into(),
            );
            row.insert(hs_trace::PEER.into(), trace_peer_label(&peer).into());
            row.insert(
                hs_trace::SESSION_ID.into(),
                session_id.map_or(serde_json::Value::Null, Into::into),
            );
            row.insert(
                hs_trace::REASON.into(),
                match reason {
                    HeaderSyncMisbehavior::MalformedMessage => "malformed_message",
                    HeaderSyncMisbehavior::InvalidHeader => "invalid_header",
                }
                .into(),
            );
            row.insert(hs_trace::BOUNDARY.into(), "engine".into());
            row.insert(hs_trace::DISPOSITION.into(), "record".into());
        });
        let _ = self.dispatch_action(HeaderPortOperation::Misbehavior { peer, reason });
    }
}

fn next_height(height: block::Height) -> block::Height {
    block::Height(height.0.saturating_add(1).min(block::Height::MAX.0))
}

fn node_id_from_peer(peer: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

fn header_direction_label(direction: ServicePeerDirection) -> &'static str {
    match direction {
        ServicePeerDirection::Inbound => "inbound",
        ServicePeerDirection::Outbound => "outbound",
    }
}

fn aux_schema_label(schema: AuxSchema) -> &'static str {
    match schema {
        AuxSchema::None => "none",
        AuxSchema::V1 => "v1",
    }
}

fn headers_outcome_label(outcome: HeadersOutcomeCode) -> &'static str {
    match outcome {
        HeadersOutcomeCode::TargetNotRetained => "target_not_retained",
        HeadersOutcomeCode::NoLocatorIntersection => "no_locator_intersection",
        HeadersOutcomeCode::HistoryPruned => "history_pruned",
        HeadersOutcomeCode::Busy => "busy",
    }
}

fn insert_header_scope(
    row: &mut serde_json::Map<String, serde_json::Value>,
    scope: zakura_header_chain::HeaderWorkAuthority,
) {
    row.insert(hs_trace::STATE_VERSION.into(), serde_json::Value::Null);
    row.insert(
        hs_trace::HEADER_GENERATION.into(),
        scope.header_generation.get().into(),
    );
    row.insert(
        hs_trace::VERIFIED_GENERATION.into(),
        serde_json::Value::Null,
    );
    row.insert(
        hs_trace::BRANCH_ANCHOR.into(),
        scope.branch.anchor_hash.to_string().into(),
    );
    row.insert(
        hs_trace::BRANCH_TARGET.into(),
        scope.branch.target_tip_hash.to_string().into(),
    );
}

fn source_id_from_peer(peer: &ZakuraPeerId) -> zakura_header_chain::SourceId {
    zakura_header_chain::SourceId::from_digest(peer.digest())
}

fn ordered_send_error_label(error: &OrderedSendError) -> &'static str {
    match error {
        OrderedSendError::Full => "full",
        OrderedSendError::Closed => "closed",
        OrderedSendError::Encode(_) => "encode",
    }
}

#[cfg(test)]
mod tests;
