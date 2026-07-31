use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    num::NonZeroU64,
    pin::Pin,
};

use futures::{stream::FuturesUnordered, StreamExt};
use iroh::NodeId;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{self, Instant},
};
use zakura_chain::block;

use super::{
    events::HeaderPortOperation,
    scheduler::{
        completed_targets::CompletedHeaderTargets,
        peer_work::{HeaderTargetPhase, PeerWorkPriority, PeerWorkQueue, QueueWorkResult},
        repair::{RepairPolicyState, RepairRequirement, RepairRequirementSlot},
        status::StatusPublisher,
    },
    *,
};
use crate::zakura::{
    OrderedSendError, ServicePeerDirection, ServicePeerSnapshot, ZakuraHeaderSyncCandidateState,
    ZakuraPeerId,
};

const INTERNAL_VCT_REPAIR_SESSION_ID: u64 = u64::MAX;
const LEASE_RELEASE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const VCT_REPAIR_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

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
    #[cfg(not(any(test, feature = "zakura-testkit")))]
    drop(actions_tx);
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
    };
    let mut reactor = HeaderSyncReactor {
        startup,
        events: events_rx,
        lifecycle: lifecycle_rx,
        #[cfg(any(test, feature = "zakura-testkit"))]
        actions: actions_tx,
        pending_port_operations: FuturesUnordered::new(),
        retained_paths: HashMap::new(),
        tip: tip_tx,
        peers: peers_tx,
        candidates: candidates_tx,
        codec,
        serving_limits,
        committed_snapshot,
        vct_repair_status,
        peer_state: HashMap::new(),
        peer_work_queue: PeerWorkQueue::default(),
        request_deadlines: HashMap::new(),
        completed_targets: CompletedHeaderTargets::default(),
        vct_repair: RepairRequirementSlot::default(),
        served_paths: HashMap::new(),
        served_path_deadlines: HashMap::new(),
        pending_lease_releases: VecDeque::new(),
        lease_release_retry_at: None,
    };
    reactor.schedule_current_vct_repair();
    Ok((handle, actions_rx, reactor))
}

#[derive(Debug)]
struct PeerState {
    session: HeaderSyncPeerSession,
    status_publisher: Option<StatusPublisher>,
    last_status: Option<Status>,
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
        scope: zakura_header_chain::WorkScope,
    },
    Active {
        session_id: u64,
        lease_id: u64,
        target: zakura_header_chain::Frontier,
        scope: zakura_header_chain::WorkScope,
        next_after: zakura_header_chain::Frontier,
        pending_request: Option<PendingServedRequest>,
    },
}

#[derive(Clone, Debug)]
struct PendingLeaseRelease {
    peer: ZakuraPeerId,
    session_id: u64,
    lease_id: u64,
    scope: zakura_header_chain::WorkScope,
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
    events: mpsc::Receiver<HeaderSyncEvent>,
    lifecycle: mpsc::UnboundedReceiver<HeaderSyncEvent>,
    #[cfg(any(test, feature = "zakura-testkit"))]
    actions: mpsc::Sender<HeaderPortOperation>,
    pending_port_operations: FuturesUnordered<PendingPortOperation>,
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
    peer_work_queue: PeerWorkQueue,
    request_deadlines: HashMap<ZakuraPeerId, Instant>,
    completed_targets: CompletedHeaderTargets,
    vct_repair: RepairRequirementSlot,
    served_paths: HashMap<ZakuraPeerId, ServedPathState>,
    served_path_deadlines: HashMap<ZakuraPeerId, Instant>,
    pending_lease_releases: VecDeque<PendingLeaseRelease>,
    lease_release_retry_at: Option<Instant>,
}

type PendingPortOperation =
    Pin<Box<dyn Future<Output = HeaderSyncPortCompletion> + Send + 'static>>;

type HeaderSyncPortCompletion = Box<dyn FnOnce(&mut HeaderSyncReactor) + Send + 'static>;

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
    let scope = zakura_header_chain::WorkScope::for_body_work(snapshot);
    let owner = scope.bind(INTERNAL_VCT_REPAIR_SESSION_ID, request_id);
    Some(RepairRequirement::new(owner, height, status.generation))
}

#[cfg_attr(any(test, feature = "zakura-testkit"), allow(dead_code))]
fn port_header_entry(entry: HeaderEntry) -> zakura_node_services::header_chain::HeaderTargetEntry {
    zakura_node_services::header_chain::HeaderTargetEntry {
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
    if page.nodes.len() != page.aux_deliveries.len() {
        return None;
    }
    let tree_aux_schema = if requested_schema == AuxSchema::V1
        && page
            .aux_deliveries
            .iter()
            .all(|deliveries| selected_port_aux_delivery(deliveries, AuxSchema::V1).is_some())
    {
        AuxSchema::V1
    } else {
        AuxSchema::None
    };
    let entries = page
        .nodes
        .into_iter()
        .zip(page.aux_deliveries)
        .map(|(node, deliveries)| {
            let delivery = selected_port_aux_delivery(&deliveries, tree_aux_schema);
            HeaderEntry {
                header: node.header,
                body_size: delivery.map_or(0, |delivery| match delivery.body_size {
                    zakura_header_chain::BodySizeHint::Unknown => 0,
                    zakura_header_chain::BodySizeHint::Known(size) => size.get(),
                }),
                tree_aux: (tree_aux_schema == AuxSchema::V1)
                    .then(|| delivery.and_then(|delivery| delivery.tree_aux))
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
        loop {
            let maintenance = self.next_maintenance_deadline();
            metrics::counter!("sync.header.reactor.iterations").increment(1);
            tokio::select! {
                biased;
                _ = self.startup.shutdown.cancelled() => break,
                event = self.lifecycle.recv() => match event {
                    Some(event) => self.handle_event(event),
                    None => break,
                },
                event = self.events.recv() => match event {
                    Some(event) => self.handle_event(event),
                    None => break,
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
        }
    }

    fn handle_event(&mut self, event: HeaderSyncEvent) {
        metrics::counter!(
            "sync.header.reactor.events",
            "event" => event.metrics_label()
        )
        .increment(1);
        match event {
            HeaderSyncEvent::PeerConnected(session) => self.handle_peer_connected(session),
            HeaderSyncEvent::PeerDisconnected(peer) => self.handle_peer_disconnected(&peer),
            HeaderSyncEvent::AdvisoryHeaderSummary { .. } => {}
            HeaderSyncEvent::SessionWireMessage {
                peer,
                session_id,
                msg,
            } => self.handle_wire_message(peer, session_id, msg),
            HeaderSyncEvent::SessionResponse {
                peer,
                session_id,
                scope,
                msg,
            } => self.handle_wire_response(peer, session_id, scope, msg),
            #[cfg(any(test, feature = "zakura-testkit"))]
            HeaderSyncEvent::HeaderLocatorReady {
                peer,
                session_id,
                target_tip_hash,
                scope,
                locator,
            } => {
                self.handle_header_locator_ready(peer, session_id, target_tip_hash, scope, locator)
            }
            #[cfg(any(test, feature = "zakura-testkit"))]
            HeaderSyncEvent::VctRepairContextReady { owner, result } => {
                self.handle_vct_repair_context_ready(owner, result)
            }
            #[cfg(any(test, feature = "zakura-testkit"))]
            HeaderSyncEvent::HeaderPathLeaseReady {
                peer,
                session_id,
                scope,
                request,
                result,
            } => self.handle_header_path_lease_ready(peer, session_id, scope, request, result),
            #[cfg(any(test, feature = "zakura-testkit"))]
            HeaderSyncEvent::HeaderPathPageReady {
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
            HeaderSyncEvent::HeaderTargetPrepared {
                peer,
                source,
                owner,
                result,
            } => self.handle_header_target_prepared(peer, source, owner, result),
            #[cfg(any(test, feature = "zakura-testkit"))]
            HeaderSyncEvent::HeaderTargetAdmissionReady {
                peer,
                source,
                owner,
                result,
            } => self.handle_header_target_admission_ready(peer, source, owner, result),
        }
    }

    fn handle_port_completion(&mut self, completion: HeaderSyncPortCompletion) {
        completion(self);
    }

    fn handle_peer_connected(&mut self, session: HeaderSyncPeerSession) {
        let latest_snapshot = self
            .startup
            .committed_snapshots
            .as_ref()
            .and_then(|snapshots| snapshots.borrow().clone());
        if let Some(snapshot) = latest_snapshot {
            self.observe_latest_committed_snapshot(snapshot);
        }

        let peer = session.peer_id().clone();
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
        if let Some(previous) = self.peer_state.insert(
            peer.clone(),
            PeerState {
                session,
                status_publisher,
                last_status: None,
            },
        ) {
            previous.session.cancel_token().cancel();
            self.retire_peer_work(&peer);
            self.release_served_path(&peer);
            if let Some((owner, source)) = replaced_repair {
                self.retry_vct_repair(owner, source);
            }
        }
        self.publish_peer_state();
        self.send_status(&peer);
    }

    fn handle_peer_disconnected(&mut self, peer: &ZakuraPeerId) {
        self.release_served_path(peer);
        let abandoned_repair = self.peer_work_queue.active(peer).and_then(|active| {
            matches!(
                active.purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            )
            .then_some((active.owner, active.source))
        });
        self.peer_state.remove(peer);
        self.retire_peer_work(peer);
        if let Some((owner, source)) = abandoned_repair {
            self.retry_vct_repair(owner, source);
        }
        self.publish_peer_state();
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
        if status.work_anchor_height > status.selected_tip_height {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        }
        if let Some(state) = self.peer_state.get_mut(&peer) {
            state.last_status = Some(status.clone());
        }
        self.request_vct_repair_context();
        self.try_assign_vct_repair();

        let Some(local) = self.committed_snapshot.as_ref() else {
            return;
        };
        let scope =
            zakura_header_chain::WorkScope::for_header_target(local, status.selected_tip_hash);
        let target = AdvertisedHeaderTarget {
            scope,
            session_id,
            status: status.clone(),
        };
        let work_order = target.claimed_work_order(local);
        let eligible = target.is_discovery_eligible(local);
        if !eligible {
            self.peer_work_queue.remove_unstarted(&peer);
            return;
        }
        let branch = zakura_header_chain::BranchId::new(
            local.frontiers.finalized.hash,
            status.selected_tip_hash,
        );
        if self
            .completed_targets
            .contains(local.header_generation, branch)
        {
            self.peer_work_queue.remove_unstarted(&peer);
            metrics::counter!("sync.header.target.covered").increment(1);
            return;
        }
        match self.peer_work_queue.stage(
            peer.clone(),
            target,
            PeerWorkPriority::from_work_order(work_order),
        ) {
            QueueWorkResult::NeedsLocator => {
                if !self.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
                    peer: peer.clone(),
                    session_id,
                    target_tip_hash: status.selected_tip_hash,
                    scope,
                }) {
                    self.peer_work_queue.remove_unstarted(&peer);
                }
            }
            QueueWorkResult::AlreadyActive => {
                metrics::counter!("sync.header.target.already_active").increment(1);
            }
            QueueWorkResult::AtCapacity => {
                metrics::counter!("sync.header.target.capacity_refused").increment(1);
            }
        }
    }

    fn handle_wire_response(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::WorkScope,
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
                    let action = HeaderPortOperation::ReadHeaderPath {
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
            zakura_header_chain::WorkScope::for_header_target(local, request.target_tip_hash);
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
        if !self.dispatch_action(HeaderPortOperation::AcquireHeaderPath {
            peer: peer.clone(),
            session_id,
            scope,
            request: request.clone(),
        }) {
            self.served_paths.remove(&peer);
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
        _session_id: u64,
        response_scope: zakura_header_chain::WorkScope,
        response: Headers,
    ) {
        let Some(request_id) = HeaderSyncRequestId::new(response.request_id) else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        };
        let Some(active) = self.peer_work_queue.active(&peer).cloned() else {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        };
        if active.request_id != request_id {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        }
        if active.owner.scope() != response_scope {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        }
        let returned_ancestor = zakura_header_chain::Frontier::new(
            response.common_ancestor_height,
            response.common_ancestor_hash,
        );
        if let HeaderTargetPurpose::SelectedAuxiliaryRepair {
            selected_target, ..
        } = active.purpose
        {
            let exact_shape = response.target_tip_hash == selected_target.hash
                && active.sent_locator.entries() == [returned_ancestor]
                && response.entries.len() == 1
                && response.complete
                && response.tree_aux_schema == AuxSchema::V1
                && response.entries[0].tree_aux.is_some()
                && response.entries[0].header.hash() == selected_target.hash;
            if !exact_shape {
                self.retry_vct_repair(active.owner, active.source);
                self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
                return;
            }
        }
        if !active.matches_response_page(response.target_tip_hash, returned_ancestor) {
            self.retire_peer_work(&peer);
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        }
        if response.complete
            && response.entries.is_empty()
            && returned_ancestor.hash == response.target_tip_hash
            && response.target_tip_hash == active.target.status.selected_tip_hash
        {
            self.retire_peer_work(&peer);
            metrics::counter!("sync.header.target.already_known.total").increment(1);
            return;
        }
        if !self
            .peer_work_queue
            .has_staging_capacity(response.entries.len())
        {
            self.retire_peer_work(&peer);
            metrics::counter!("sync.header.target.staging_capacity_refused").increment(1);
            return;
        }
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
        let Some(staged_tip) = active.staged_tip() else {
            self.retire_peer_work(&peer);
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        };

        if complete {
            if staged_tip.hash != active.target.status.selected_tip_hash {
                self.retire_peer_work(&peer);
                self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
                return;
            }
            let repair = matches!(
                active.purpose,
                HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
            )
            .then_some((active.owner, active.source));
            active.phase = HeaderTargetPhase::Preparing;
            let action = HeaderPortOperation::PrepareHeaderTarget {
                purpose: active.purpose.clone(),
                peer: peer.clone(),
                source: active.source,
                network: self.startup.network.clone(),
                owner: active.owner,
                common_ancestor: active
                    .common_ancestor
                    .expect("a response page fixed its exact ancestor"),
                target: staged_tip,
                entries: active.entries.clone(),
            };
            let _ = active;
            if let Some((owner, source)) = repair {
                let Some(task) = self.vct_repair.get(owner) else {
                    self.retire_peer_work(&peer);
                    return;
                };
                if !matches!(task.state, RepairPolicyState::Assigned { .. }) {
                    self.retry_vct_repair(owner, source);
                    return;
                }
            }
            if !self.dispatch_action(action) {
                if let Some((owner, source)) = repair {
                    self.retry_vct_repair(owner, source);
                } else {
                    self.retire_peer_work(&peer);
                }
            }
            return;
        }

        let locator = active.continuation_locator(staged_tip);
        let max_header_count = active.max_header_count;
        let tree_aux_schema = active.tree_aux_schema;
        let target_tip_hash = active.target.status.selected_tip_hash;
        let Some(session) = self
            .peer_state
            .get(&peer)
            .map(|state| state.session.clone())
        else {
            self.retire_peer_work(&peer);
            return;
        };
        match session.try_send_get_headers(
            &self.codec,
            active.owner.scope(),
            target_tip_hash,
            &locator,
            max_header_count,
            tree_aux_schema,
        ) {
            Ok(next_request_id) => {
                let active = self
                    .peer_work_queue
                    .active_mut(&peer)
                    .expect("the active request remains staged across continuation");
                active.sent_locator = locator;
                active.request_id = next_request_id;
                debug_assert!(tree_aux_schema.admits(response_schema));
            }
            Err(_) => self.retire_peer_work(&peer),
        }
    }

    fn handle_headers_outcome(
        &mut self,
        peer: ZakuraPeerId,
        _session_id: u64,
        response_scope: zakura_header_chain::WorkScope,
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
        if active.owner.scope() != response_scope {
            metrics::counter!("sync.header.target.late_response.total").increment(1);
            return;
        }
        let matches = active.accepts_outcome(request_id, response.target_tip_hash);
        let is_repair = matches!(
            active.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        );
        if is_repair {
            self.retry_vct_repair(active.owner, active.source);
        } else {
            self.retire_peer_work(&peer);
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
        owner: zakura_header_chain::WorkOwner,
        result: HeaderTargetAdmissionResult,
    ) {
        let Some(active) = self.peer_work_queue.active(&peer).cloned() else {
            return;
        };
        if active.phase != HeaderTargetPhase::Applying
            || active.source != source
            || active.owner != owner
            || !self.completion_is_current(&peer, source, &owner)
        {
            return;
        }
        let repair_generation = match active.purpose {
            HeaderTargetPurpose::Normal => None,
            HeaderTargetPurpose::SelectedAuxiliaryRepair {
                repair_generation, ..
            } => Some(repair_generation),
        };
        self.retire_peer_work(&peer);
        if let Some(repair_generation) = repair_generation {
            match result {
                HeaderTargetAdmissionResult::Applied => {
                    let _ = self
                        .vct_repair
                        .get_mut(owner)
                        .expect("the admitted repair remains owned by its active request")
                        .complete();
                    metrics::counter!("sync.header.vct.repair.admitted.total").increment(1);
                }
                HeaderTargetAdmissionResult::Failed(error) => {
                    self.vct_repair.remove(owner);
                    self.handle_typed_failure(peer, source, &error);
                    if repair_generation == self.vct_repair_status.generation {
                        self.schedule_current_vct_repair();
                    }
                }
            }
            return;
        }
        match result {
            HeaderTargetAdmissionResult::Applied => {
                self.completed_targets
                    .mark(owner.header_generation, owner.branch);
                metrics::counter!("sync.header.target.admitted").increment(1);
            }
            HeaderTargetAdmissionResult::Failed(error) => {
                self.handle_typed_failure(peer, source, &error);
            }
        }
    }

    fn handle_header_target_prepared(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::WorkOwner,
        result: HeaderTargetPreparationResult,
    ) {
        let Some(active) = self.peer_work_queue.active(&peer).cloned() else {
            return;
        };
        if active.phase != HeaderTargetPhase::Preparing
            || active.source != source
            || active.owner != owner
            || !self.completion_is_current(&peer, source, &owner)
        {
            return;
        }
        let purpose = active.purpose;
        let is_repair = matches!(purpose, HeaderTargetPurpose::SelectedAuxiliaryRepair { .. });
        match result {
            HeaderTargetPreparationResult::Prepared(insert) => {
                if insert.owner != owner || insert.source != source {
                    return;
                }
                if is_repair {
                    let valid = self.vct_repair.get(owner).is_some_and(|task| {
                        let RepairPolicyState::Assigned { context } = &task.state else {
                            return false;
                        };
                        insert.target_tip_hash == context.target.hash && insert.aux.len() == 1
                    });
                    if !valid {
                        self.retry_vct_repair(owner, source);
                        return;
                    }
                }
                self.request_deadlines
                    .insert(peer.clone(), Instant::now() + self.startup.request_timeout);
                self.peer_work_queue
                    .active_mut(&peer)
                    .expect("the exact preparing request passed the completion gate")
                    .phase = HeaderTargetPhase::Applying;
                if self.dispatch_action(HeaderPortOperation::ApplyHeaderTarget {
                    purpose: purpose.clone(),
                    peer: peer.clone(),
                    source,
                    owner,
                    insert,
                }) {
                } else if is_repair {
                    self.retry_vct_repair(owner, source);
                } else {
                    self.retire_peer_work(&peer);
                }
            }
            HeaderTargetPreparationResult::Failed(error) => {
                if is_repair {
                    self.retry_vct_repair(owner, source);
                } else {
                    self.retire_peer_work(&peer);
                }
                self.handle_typed_failure(peer, source, &error);
            }
        }
    }

    fn retry_vct_repair(
        &mut self,
        owner: zakura_header_chain::WorkOwner,
        source: zakura_header_chain::SourceId,
    ) {
        self.cancel_owned_request(source, owner);
        self.peer_work_queue.remove_owner(owner);
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
            self.try_assign_vct_repair();
        }
    }

    fn retire_vct_repair(&mut self) {
        if let Some(task) = self.vct_repair.take() {
            if let Some(peer) = self
                .peer_work_queue
                .active_owner(task.owner)
                .map(|active| active.peer.clone())
            {
                self.retire_peer_work(&peer);
            }
        }
    }

    fn handle_header_path_lease_ready(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        scope: zakura_header_chain::WorkScope,
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
        if !self.dispatch_action(HeaderPortOperation::ReadHeaderPath {
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
        scope: zakura_header_chain::WorkScope,
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
        let sent = self
            .peer_state
            .get(&peer)
            .map(|state| state.session.try_send_headers(&self.codec, response))
            .transpose()
            .is_ok_and(|result| result.is_some());
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

    fn handle_header_locator_ready(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        target_tip_hash: block::Hash,
        scope: zakura_header_chain::WorkScope,
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
        if target.scope
            != zakura_header_chain::WorkScope::for_header_target(&local, target_tip_hash)
        {
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
        if max_header_count == 0 {
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
                    owner,
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
                }
                metrics::counter!("sync.header.target.requested").increment(1);
            }
            Err(error) => {
                self.peer_work_queue.remove_unstarted(&peer);
                metrics::counter!(
                    "sync.header.target.send_failed",
                    "reason" => ordered_send_error_label(&error)
                )
                .increment(1);
            }
        }
    }

    fn observe_latest_committed_snapshot(&mut self, snapshot: zakura_header_chain::EngineSnapshot) {
        if self.committed_snapshot.as_ref() == Some(&snapshot) {
            return;
        }

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
            let state_admission_owns_repair = task.is_completed()
                || self
                    .peer_work_queue
                    .active_owner(task.owner)
                    .is_some_and(|active| active.phase == HeaderTargetPhase::Applying);
            task.repair_generation == desired.repair_generation
                && task.height == desired.height
                && task.owner.header_generation == desired.owner.header_generation
                && task.owner.verified_generation == desired.owner.verified_generation
                && task.owner.branch == desired.owner.branch
                && (task.owner.state_version == desired.owner.state_version
                    || state_admission_owns_repair)
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
            let _ = self
                .vct_repair
                .get_mut(owner)
                .expect("the context-needing repair remains owned during synchronous dispatch")
                .mark_context_requested();
        }
    }

    fn handle_vct_repair_context_ready(
        &mut self,
        owner: zakura_header_chain::WorkOwner,
        result: VctRepairContextResult,
    ) {
        if self
            .vct_repair
            .get(owner)
            .is_none_or(|task| task.state != RepairPolicyState::QueryingContext)
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
                self.try_assign_vct_repair();
            }
            VctRepairContextResult::Stale => {
                self.vct_repair.remove(owner);
            }
            VctRepairContextResult::Unavailable => {
                let task = self
                    .vct_repair
                    .get_mut(owner)
                    .expect("the exact pending context read was checked above");
                let _ = task.context_unavailable(Instant::now() + VCT_REPAIR_RETRY_INTERVAL);
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
            let request_id = match session.try_send_get_headers(
                &self.codec,
                task.owner.scope(),
                context.target.hash,
                &context.locator,
                1,
                AuxSchema::V1,
            ) {
                Ok(request_id) => request_id,
                Err(_) => {
                    if let Some(current) = self.vct_repair.get_mut(task.owner) {
                        let _ = current.record_failed_source(source);
                    }
                    continue;
                }
            };
            let wire_owner = task.owner.scope().bind(
                session.session_id(),
                NonZeroU64::new(request_id.get()).expect("header-sync request IDs are nonzero"),
            );
            if self.vct_repair.assign(task.owner, wire_owner).is_err() {
                session.cancel_request(request_id);
                return;
            }
            status.selected_tip_height = context.target.height;
            status.selected_tip_hash = context.target.hash;
            status.max_headers_per_response = 1;
            let target = AdvertisedHeaderTarget {
                scope: wire_owner.scope(),
                session_id: session.session_id(),
                status,
            };
            self.peer_work_queue.remove_unstarted(&peer);
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
                    owner: wire_owner,
                    common_ancestor: None,
                    entries: Vec::new(),
                    phase: HeaderTargetPhase::Receiving,
                    max_header_count: 1,
                    tree_aux_schema: AuxSchema::V1,
                })
            {
                session.cancel_request(request_id);
                self.retry_vct_repair(wire_owner, source);
                return;
            }
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
                    != zakura_header_chain::WorkScope::for_header_target(snapshot, target_tip_hash))
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
                .active_owner(task.owner)
                .map(|active| active.peer.clone())
            {
                self.retire_peer_work(&peer);
            }
        }
        self.completed_targets
            .retain_current(snapshot.header_generation, snapshot.frontiers.finalized);
        for active in self.peer_work_queue.retire_obsolete_active(snapshot) {
            self.request_deadlines.remove(&active.peer);
            self.cancel_active_request(&active);
        }
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
                false
            }
        }
    }

    fn refresh_statuses(&mut self) {
        let now = Instant::now();
        self.retry_pending_lease_releases(now);
        self.retire_timed_out_requests(now);
        self.release_idle_served_paths(now);
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
                .then_some((active.owner, active.source))
            });
            if let Some((owner, source)) = repair {
                self.retry_vct_repair(owner, source);
                metrics::counter!("sync.header.vct.repair.timed_out.total").increment(1);
            } else {
                self.retire_peer_work(&peer);
                metrics::counter!("sync.header.target.timed_out.total").increment(1);
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
        let _ = state.session.try_send_headers_outcome(
            &self.codec,
            HeadersOutcome {
                request_id,
                target_tip_hash,
                outcome,
            },
        );
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
        scope: zakura_header_chain::WorkScope,
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
        let tip = *self.tip.borrow();
        let _ = self.candidates.send(ZakuraHeaderSyncCandidateState {
            target_height: next_height(tip.0),
            admitted_node_ids,
            backed_off_node_ids: Vec::new(),
        });
    }

    #[cfg(not(any(test, feature = "zakura-testkit")))]
    fn dispatch_action(&mut self, action: HeaderPortOperation) -> bool {
        use zakura_node_services::header_chain as port;

        let header_chain = self.startup.header_chain_port.clone();
        let operation: PendingPortOperation = match action {
            HeaderPortOperation::Misbehavior { peer, reason } => {
                tracing::debug!(?peer, ?reason, "recorded Zakura header-sync peer violation");
                return true;
            }
            HeaderPortOperation::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
                scope,
            } => Box::pin(async move {
                let locator = header_chain
                    .continuation_locator()
                    .await
                    .unwrap_or_else(|error| {
                        tracing::debug!(?peer, ?error, "header locator unavailable");
                        None
                    });
                Box::new(move |reactor: &mut HeaderSyncReactor| {
                    reactor.handle_header_locator_ready(
                        peer,
                        session_id,
                        target_tip_hash,
                        scope,
                        locator,
                    );
                }) as HeaderSyncPortCompletion
            }),
            HeaderPortOperation::QueryVctRepairContext { owner, height } => Box::pin(async move {
                let result = match header_chain.vct_repair_context(owner, height).await {
                    Ok(port::VctRepairContextReply::Resolved(context)) => {
                        VctRepairContextResult::Resolved(context)
                    }
                    Ok(port::VctRepairContextReply::Stale) => VctRepairContextResult::Stale,
                    Err(error) => {
                        tracing::debug!(?owner, ?error, "VCT repair context unavailable");
                        VctRepairContextResult::Unavailable
                    }
                };
                Box::new(move |reactor: &mut HeaderSyncReactor| {
                    reactor.handle_vct_repair_context_ready(owner, result);
                }) as HeaderSyncPortCompletion
            }),
            HeaderPortOperation::AcquireHeaderPath {
                peer,
                session_id,
                scope,
                request,
            } => Box::pin(async move {
                let reply = header_chain
                    .acquire_header_path(port::AcquireHeaderPath {
                        source: source_id_from_peer(&peer),
                        session_id,
                        scope,
                        target_tip_hash: request.target_tip_hash,
                        locator_hashes: request.locator_hashes.clone(),
                    })
                    .await;
                let (result, acquired) = match reply {
                    Ok(port::AcquireHeaderPathReply::Acquired(path)) => {
                        let lease_id = path.adapter_identity().0.adapter_id();
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
                            port::AcquireHeaderPathReply::TargetNotRetained => {
                                HeadersOutcomeCode::TargetNotRetained
                            }
                            port::AcquireHeaderPathReply::NoLocatorIntersection => {
                                HeadersOutcomeCode::NoLocatorIntersection
                            }
                            port::AcquireHeaderPathReply::HistoryPruned => {
                                HeadersOutcomeCode::HistoryPruned
                            }
                            port::AcquireHeaderPathReply::Busy => HeadersOutcomeCode::Busy,
                            port::AcquireHeaderPathReply::Acquired(_) => {
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
                    reactor
                        .handle_header_path_lease_ready(peer, session_id, scope, request, result);
                }) as HeaderSyncPortCompletion
            }),
            HeaderPortOperation::ReadHeaderPath {
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
                            port::ReadHeaderPath {
                                after_hash,
                                max_header_count,
                            },
                        )
                        .await
                    {
                        Ok(port::ReadHeaderPathReply::Page(page)) => {
                            assemble_port_header_path_page(lease_id, *page, tree_aux_schema)
                                .map(|page| HeaderPathPageResult::Page(Box::new(page)))
                                .unwrap_or(HeaderPathPageResult::Unavailable)
                        }
                        Ok(port::ReadHeaderPathReply::Unavailable) => {
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
                network,
                owner,
                common_ancestor,
                target,
                entries,
            } => Box::pin(async move {
                let completion = match purpose {
                    HeaderTargetPurpose::Normal => {
                        zakura_header_chain::TargetCompletion::TargetComplete { common_ancestor }
                    }
                    HeaderTargetPurpose::SelectedAuxiliaryRepair {
                        selected_target, ..
                    } if selected_target == target && entries.len() == 1 => {
                        zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                            common_ancestor,
                            selected_target,
                        }
                    }
                    HeaderTargetPurpose::SelectedAuxiliaryRepair { .. } => {
                        let result = HeaderTargetPreparationResult::Failed(std::sync::Arc::new(
                            zakura_header_chain::HeaderChainError::stale_target(
                                zakura_header_chain::ErrorSubject::Branch(owner.branch),
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
                        network,
                        owner,
                        common_ancestor,
                        target,
                        entries: entries.into_iter().map(port_header_entry).collect(),
                        completion,
                    })
                    .await
                    .map(|target| HeaderTargetPreparationResult::Prepared(target.into_insert()))
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
                insert,
            } => Box::pin(async move {
                let result = header_chain
                    .apply_header_target(port::PreparedHeaderTarget::from_insert(insert))
                    .await
                    .map(|()| HeaderTargetAdmissionResult::Applied)
                    .unwrap_or_else(HeaderTargetAdmissionResult::Failed);
                Box::new(move |reactor: &mut HeaderSyncReactor| {
                    reactor.handle_header_target_admission_ready(peer, source, owner, result);
                }) as HeaderSyncPortCompletion
            }),
        };
        self.pending_port_operations.push(operation);
        true
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    fn dispatch_action(&mut self, action: HeaderPortOperation) -> bool {
        match self.actions.try_send(action) {
            Ok(()) => true,
            Err(error) => {
                tracing::debug!(?error, "header-sync action queue unavailable");
                false
            }
        }
    }

    fn completion_is_current(
        &self,
        peer: &ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: &zakura_header_chain::WorkOwner,
    ) -> bool {
        let Some(current) = self.committed_snapshot.as_ref() else {
            return false;
        };
        match zakura_header_chain::CompletionGate::check_registered(
            current,
            self.peer_work_queue.registered_attempt(peer),
            source,
            owner,
        ) {
            zakura_header_chain::CompletionDecision::Current => true,
            zakura_header_chain::CompletionDecision::Stale(reason) => {
                metrics::counter!(
                    "sync.header_chain.stale_completion.total",
                    "kind" => format!("{reason:?}")
                )
                .increment(1);
                false
            }
        }
    }

    fn retire_peer_work(&mut self, peer: &ZakuraPeerId) {
        self.request_deadlines.remove(peer);
        if let Some(active) = self.peer_work_queue.remove(peer) {
            self.cancel_active_request(&active);
        }
    }

    fn cancel_active_request(&self, active: &ActiveHeaderRequest) {
        let Some(state) = self.peer_state.get(&active.peer) else {
            return;
        };
        if state.session.session_id() == active.owner.session_id {
            state.session.cancel_request(active.request_id);
        }
    }

    fn cancel_owned_request(
        &self,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::WorkOwner,
    ) {
        let Some(state) = self.peer_state.iter().find_map(|(peer, state)| {
            (state.session.session_id() == owner.session_id && source_id_from_peer(peer) == source)
                .then_some(state)
        }) else {
            return;
        };
        let Some(request_id) = HeaderSyncRequestId::new(owner.request_id.get()) else {
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
