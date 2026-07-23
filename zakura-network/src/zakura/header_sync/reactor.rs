use std::{collections::HashMap, num::NonZeroU64};

use iroh::NodeId;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{self, Instant},
};
use zakura_chain::block;

use super::{
    scheduler::{
        coverage::{BranchRange, CoverageMap},
        peer_work::{HeaderTargetPhase, PeerWorkPriority, PeerWorkQueue, QueueWorkResult},
        repair::{RepairPhase, VctRepairQueue, VctRepairTask},
        retry::BodyRetryQueue,
        status::StatusPublisher,
    },
    *,
};
use crate::zakura::{
    OrderedSendError, ServicePeerDirection, ServicePeerSnapshot, ZakuraHeaderSyncCandidateState,
    ZakuraPeerId,
};

const INTERNAL_VCT_REPAIR_SESSION_ID: u64 = u64::MAX;

/// Spawn the canonical header-sync reactor.
pub fn spawn_header_sync_reactor(
    startup: HeaderSyncStartup,
) -> Result<
    (
        HeaderSyncHandle,
        mpsc::Receiver<HeaderSyncAction>,
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
        mpsc::Receiver<HeaderSyncAction>,
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
    };
    let mut reactor = HeaderSyncReactor {
        startup,
        events: events_rx,
        lifecycle: lifecycle_rx,
        actions: actions_tx,
        tip: tip_tx,
        peers: peers_tx,
        candidates: candidates_tx,
        codec,
        serving_limits,
        committed_snapshot,
        vct_repair_status,
        completed_vct_repair_generation: None,
        dispatched_vct_repair: None,
        peer_state: HashMap::new(),
        peer_work_queue: PeerWorkQueue::default(),
        coverage: CoverageMap::default(),
        body_retries: BodyRetryQueue::default(),
        vct_repairs: VctRepairQueue::default(),
        pending_owners: zakura_header_chain::PendingOwners::default(),
        served_paths: HashMap::new(),
    };
    reactor.schedule_current_vct_repair();
    Ok((handle, actions_rx, reactor))
}

#[derive(Debug)]
struct PeerState {
    session: HeaderSyncPeerSession,
    status_publisher: Option<StatusPublisher>,
    last_received_status_at: Option<Instant>,
    last_status: Option<Status>,
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
        pending_request: Option<(HeaderSyncRequestId, u32)>,
    },
}

#[derive(Debug)]
struct HeaderSyncReactor {
    startup: HeaderSyncStartup,
    events: mpsc::Receiver<HeaderSyncEvent>,
    lifecycle: mpsc::UnboundedReceiver<HeaderSyncEvent>,
    actions: mpsc::Sender<HeaderSyncAction>,
    tip: watch::Sender<(block::Height, block::Hash)>,
    peers: watch::Sender<ServicePeerSnapshot>,
    candidates: watch::Sender<ZakuraHeaderSyncCandidateState>,
    codec: HeaderSyncCodec,
    serving_limits: HeaderServingLimits,
    committed_snapshot: Option<zakura_header_chain::EngineSnapshot>,
    vct_repair_status: zakura_header_chain::VctRootRepairStatus,
    completed_vct_repair_generation: Option<u64>,
    dispatched_vct_repair: Option<(
        zakura_header_chain::WorkOwner,
        zakura_header_chain::SourceId,
        u64,
    )>,
    peer_state: HashMap<ZakuraPeerId, PeerState>,
    peer_work_queue: PeerWorkQueue,
    coverage: CoverageMap,
    body_retries: BodyRetryQueue,
    vct_repairs: VctRepairQueue,
    pending_owners: zakura_header_chain::PendingOwners,
    served_paths: HashMap<ZakuraPeerId, ServedPathState>,
}

fn vct_repair_task(
    snapshot: &zakura_header_chain::EngineSnapshot,
    status: zakura_header_chain::VctRootRepairStatus,
) -> Option<VctRepairTask> {
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
    let range = BranchRange::new(owner.branch, height, height)?;
    VctRepairTask::new(owner, range).ok()
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
            HeaderSyncEvent::HeaderLocatorReady {
                peer,
                session_id,
                target_tip_hash,
                scope,
                locator,
            } => {
                self.handle_header_locator_ready(peer, session_id, target_tip_hash, scope, locator)
            }
            HeaderSyncEvent::VctRepairContextReady { owner, result } => {
                self.handle_vct_repair_context_ready(owner, result)
            }
            HeaderSyncEvent::HeaderPathLeaseReady {
                peer,
                session_id,
                scope,
                request,
                result,
            } => self.handle_header_path_lease_ready(peer, session_id, scope, request, result),
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
            HeaderSyncEvent::HeaderTargetPrepared {
                peer,
                source,
                owner,
                result,
            } => self.handle_header_target_prepared(peer, source, owner, result),
            HeaderSyncEvent::VctRepairPrepared {
                peer,
                source,
                owner,
                result,
            } => self.handle_vct_repair_prepared(peer, source, owner, result),
            HeaderSyncEvent::HeaderTargetAdmissionReady {
                peer,
                source,
                owner,
                result,
            } => self.handle_header_target_admission_ready(peer, source, owner, result),
            HeaderSyncEvent::VctRepairAdmissionReady {
                peer,
                source,
                owner,
                result,
            } => self.handle_vct_repair_admission_ready(peer, source, owner, result),
        }
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
        let replaced_repair = self.peer_state.get(&peer).and_then(|state| {
            let source = source_id_from_peer(&peer)?;
            self.vct_repairs
                .for_session(source, state.session.session_id())
                .map(|task| (task.owner, source))
        });
        let at_capacity = self.admitted_count(direction)
            >= match direction {
                ServicePeerDirection::Inbound => self.startup.config.peer_limits.max_inbound_peers,
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
                last_received_status_at: None,
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
        let abandoned_repair = self.peer_state.get(peer).and_then(|state| {
            let source = source_id_from_peer(peer)?;
            self.vct_repairs
                .for_session(source, state.session.session_id())
                .map(|task| (task.owner, source))
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
            state.last_received_status_at = Some(Instant::now());
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
            observed_at: Instant::now(),
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
            .coverage
            .covers_tip(local.header_generation, branch, status.selected_tip_height)
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
                if !self.dispatch_action(HeaderSyncAction::QueryHeaderLocator {
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
        let max_header_count = self.served_page_count(request.max_header_count);
        if max_header_count == 0 {
            self.send_headers_outcome(
                &peer,
                request.request_id,
                request.target_tip_hash,
                HeadersOutcomeCode::Busy,
            );
            return;
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
                    *pending_request = Some((request_id, max_header_count));
                    let action = HeaderSyncAction::ReadHeaderPath {
                        peer: peer.clone(),
                        session_id,
                        lease_id: *lease_id,
                        scope: *scope,
                        request_id,
                        target_tip_hash: request.target_tip_hash,
                        after_hash: next_after.hash,
                        max_header_count,
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
        if !self.dispatch_action(HeaderSyncAction::AcquireHeaderPath {
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
        session_id: u64,
        response_scope: zakura_header_chain::WorkScope,
        response: Headers,
    ) {
        let Some(request_id) = HeaderSyncRequestId::new(response.request_id) else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        };
        if self.handle_vct_repair_headers(&peer, session_id, response_scope, request_id, &response)
        {
            return;
        }
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
        if !active.matches_response_page(response.target_tip_hash, returned_ancestor) {
            self.retire_peer_work(&peer);
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
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
            active.phase = HeaderTargetPhase::Preparing;
            let action = HeaderSyncAction::PrepareHeaderTarget {
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
            if !self.dispatch_action(action) {
                self.retire_peer_work(&peer);
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
                debug_assert!(
                    response_schema == AuxSchema::None || response_schema == tree_aux_schema,
                    "the codec enforces response schema narrowing"
                );
            }
            Err(_) => self.retire_peer_work(&peer),
        }
    }

    fn handle_vct_repair_headers(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: u64,
        response_scope: zakura_header_chain::WorkScope,
        request_id: HeaderSyncRequestId,
        response: &Headers,
    ) -> bool {
        let Some(source) = source_id_from_peer(peer) else {
            return false;
        };
        let Some(task) = NonZeroU64::new(request_id.get()).and_then(|request_id| {
            self.vct_repairs
                .on_wire(source, session_id, request_id)
                .cloned()
        }) else {
            return false;
        };
        let Some(context) = task.context.as_ref() else {
            self.retry_vct_repair(task.owner, source);
            return true;
        };
        if task.owner.scope() != response_scope {
            return true;
        }
        let ancestor = zakura_header_chain::Frontier::new(
            response.common_ancestor_height,
            response.common_ancestor_hash,
        );
        let exact_shape = response.target_tip_hash == context.target.hash
            && context.locator.entries() == [ancestor]
            && response.entries.len() == 1
            && response.complete;
        if !exact_shape {
            self.retry_vct_repair(task.owner, source);
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::MalformedMessage);
            return true;
        }
        if response.tree_aux_schema != AuxSchema::V1 || response.entries[0].tree_aux.is_none() {
            self.retry_vct_repair(task.owner, source);
            metrics::counter!("sync.header.vct.repair.metadata_absent.total").increment(1);
            return true;
        }
        if response.entries[0].header.hash() != context.target.hash {
            self.retry_vct_repair(task.owner, source);
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::MalformedMessage);
            return true;
        }
        if self
            .vct_repairs
            .get_mut(task.owner)
            .expect("the exact on-wire repair was cloned above")
            .buffer(response.entries[0].clone())
            .is_err()
        {
            self.retry_vct_repair(task.owner, source);
            return true;
        }
        self.dispatch_vct_preparation(peer.clone(), source, task.owner);
        true
    }

    fn dispatch_vct_preparation(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::WorkOwner,
    ) {
        let Some(task) = self.vct_repairs.get(owner) else {
            return;
        };
        if !matches!(
            task.phase,
            RepairPhase::Buffered | RepairPhase::WaitingForCapacity
        ) || task.prepared.is_some()
        {
            return;
        }
        let (Some(context), Some(entry)) = (task.context.clone(), task.entry.clone()) else {
            self.retry_vct_repair(owner, source);
            return;
        };
        if !self.dispatch_action(HeaderSyncAction::PrepareVctRepair {
            peer,
            source,
            network: self.startup.network.clone(),
            owner,
            context,
            entry,
        }) {
            let task = self
                .vct_repairs
                .get_mut(owner)
                .expect("the buffered repair remains owned after local backpressure");
            if task.phase == RepairPhase::Buffered {
                let _ = task.advance(RepairPhase::WaitingForCapacity);
            }
        }
    }

    fn handle_headers_outcome(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        response_scope: zakura_header_chain::WorkScope,
        response: HeadersOutcome,
    ) {
        if let Some(request_id) = NonZeroU64::new(response.request_id) {
            if let Some(source) = source_id_from_peer(&peer) {
                if let Some(task) = self
                    .vct_repairs
                    .on_wire(source, session_id, request_id)
                    .cloned()
                {
                    if task.owner.scope() != response_scope {
                        return;
                    }
                    let matches = task
                        .context
                        .as_ref()
                        .is_some_and(|context| context.target.hash == response.target_tip_hash);
                    self.retry_vct_repair(task.owner, source);
                    if !matches {
                        self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
                    } else {
                        metrics::counter!(
                            "sync.header.vct.repair.outcome.total",
                            "outcome" => format!("{:?}", response.outcome)
                        )
                        .increment(1);
                    }
                    return;
                }
            }
        }
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
        self.retire_peer_work(&peer);
        if matches {
            metrics::counter!(
                "sync.header.target.outcome",
                "outcome" => format!("{:?}", response.outcome)
            )
            .increment(1);
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
        let Some(active) = self.peer_work_queue.active(&peer) else {
            return;
        };
        if active.phase != HeaderTargetPhase::Applying
            || active.source != source
            || active.owner != owner
            || !self.completion_is_current(source, &owner)
        {
            return;
        }
        let admitted_range = if matches!(result, HeaderTargetAdmissionResult::Applied) {
            active
                .common_ancestor
                .zip(active.staged_tip())
                .and_then(|(common_ancestor, target)| {
                    BranchRange::new(
                        owner.branch,
                        next_height(common_ancestor.height),
                        target.height,
                    )
                })
        } else {
            None
        };
        self.retire_peer_work(&peer);
        match result {
            HeaderTargetAdmissionResult::Applied => {
                if let Some(range) = admitted_range {
                    self.coverage.mark(owner.header_generation, range);
                }
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
        let matches = self.peer_work_queue.active(&peer).is_some_and(|active| {
            active.phase == HeaderTargetPhase::Preparing
                && active.source == source
                && active.owner == owner
        });
        if !matches || !self.completion_is_current(source, &owner) {
            return;
        }
        match result {
            HeaderTargetPreparationResult::Prepared(insert) => {
                if insert.owner != owner || insert.source != source {
                    return;
                }
                self.peer_work_queue
                    .active_mut(&peer)
                    .expect("the exact preparing request passed the completion gate")
                    .phase = HeaderTargetPhase::Applying;
                if !self.dispatch_action(HeaderSyncAction::ApplyHeaderTarget {
                    peer: peer.clone(),
                    source,
                    owner,
                    insert,
                }) {
                    self.retire_peer_work(&peer);
                }
            }
            HeaderTargetPreparationResult::Failed(error) => {
                self.retire_peer_work(&peer);
                self.handle_typed_failure(peer, source, &error);
            }
        }
    }

    fn handle_vct_repair_prepared(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::WorkOwner,
        result: HeaderTargetPreparationResult,
    ) {
        let matches = self.vct_repairs.get(owner).is_some_and(|task| {
            matches!(
                task.phase,
                RepairPhase::Buffered | RepairPhase::WaitingForCapacity
            ) && task.prepared.is_none()
        });
        if !matches || !self.completion_is_current(source, &owner) {
            return;
        }
        match result {
            HeaderTargetPreparationResult::Prepared(insert) => {
                let valid = self.vct_repairs.get(owner).is_some_and(|task| {
                    let Some(context) = task.context.as_ref() else {
                        return false;
                    };
                    insert.owner == owner
                        && insert.source == source
                        && insert.target_tip_hash == context.target.hash
                        && insert.aux.len() == 1
                        && matches!(
                            insert.completion,
                            zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                                selected_target,
                                ..
                            } if selected_target == context.target
                        )
                });
                if !valid
                    || self
                        .vct_repairs
                        .get_mut(owner)
                        .expect("the exact prepared repair was checked above")
                        .seal(insert)
                        .is_err()
                {
                    self.retry_vct_repair(owner, source);
                    return;
                }
                self.dispatch_vct_apply(peer, source, owner);
            }
            HeaderTargetPreparationResult::Failed(error) => {
                self.retry_vct_repair(owner, source);
                self.handle_typed_failure(peer, source, &error);
            }
        }
    }

    fn dispatch_vct_apply(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::WorkOwner,
    ) {
        let Some(task) = self.vct_repairs.get(owner) else {
            return;
        };
        let Some(insert) = task.prepared.clone() else {
            return;
        };
        let transition = insert
            .aux
            .first()
            .map(|delivery| delivery.delivery_id)
            .expect("a sealed VCT repair contains one auxiliary delivery");
        if self.dispatch_action(HeaderSyncAction::ApplyVctRepair {
            peer,
            source,
            owner,
            insert,
        }) {
            let _ = self
                .vct_repairs
                .get_mut(owner)
                .expect("the sealed repair remains owned during synchronous dispatch")
                .advance(RepairPhase::StateDispatched { transition });
            self.dispatched_vct_repair = Some((owner, source, self.vct_repair_status.generation));
        } else {
            let task = self
                .vct_repairs
                .get_mut(owner)
                .expect("the sealed repair remains owned after local backpressure");
            if task.phase == RepairPhase::Buffered {
                let _ = task.advance(RepairPhase::WaitingForCapacity);
            }
        }
    }

    fn handle_vct_repair_admission_ready(
        &mut self,
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::WorkOwner,
        result: HeaderTargetAdmissionResult,
    ) {
        let Some((dispatched_owner, dispatched_source, generation)) = self.dispatched_vct_repair
        else {
            return;
        };
        if dispatched_owner != owner || dispatched_source != source {
            return;
        }
        self.dispatched_vct_repair = None;
        self.pending_owners.remove(source, owner.request_id);
        self.vct_repairs.remove(owner);
        match result {
            HeaderTargetAdmissionResult::Applied => {
                self.completed_vct_repair_generation = Some(generation);
                metrics::counter!("sync.header.vct.repair.admitted.total").increment(1);
            }
            HeaderTargetAdmissionResult::Failed(error) => {
                self.handle_typed_failure(peer, source, &error);
                if generation == self.vct_repair_status.generation {
                    self.schedule_current_vct_repair();
                }
            }
        }
    }

    fn retry_vct_repair(
        &mut self,
        owner: zakura_header_chain::WorkOwner,
        source: zakura_header_chain::SourceId,
    ) {
        self.pending_owners.remove(source, owner.request_id);
        let retry = self.vct_repairs.get_mut(owner).map(|task| task.retry());
        if !matches!(retry, Some(Ok(()))) {
            self.vct_repairs.remove(owner);
        }
    }

    fn retire_all_vct_repairs(&mut self) {
        for task in self.vct_repairs.drain() {
            if let Some(source) = task.source {
                self.pending_owners.remove(source, task.owner.request_id);
            }
        }
    }

    fn drive_vct_repair_capacity(&mut self) {
        let Some(task) = self.vct_repairs.waiting().cloned() else {
            return;
        };
        let Some(source) = task.source else {
            self.vct_repairs.remove(task.owner);
            return;
        };
        let Some(peer) = self
            .peer_state
            .iter()
            .find(|(peer, state)| {
                state.session.session_id() == task.owner.session_id
                    && source_id_from_peer(peer) == Some(source)
            })
            .map(|(peer, _)| peer.clone())
        else {
            self.retry_vct_repair(task.owner, source);
            return;
        };
        if task.prepared.is_some() {
            self.dispatch_vct_apply(peer, source, task.owner);
        } else {
            self.dispatch_vct_preparation(peer, source, task.owner);
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
                self.release_lease(peer, session_id, lease.lease_id, lease.scope);
                return;
            }
        };
        let max_header_count = self.served_page_count(request.max_header_count);
        self.served_paths.insert(
            peer.clone(),
            ServedPathState::Active {
                session_id,
                lease_id: lease.lease_id,
                target: lease.target,
                scope: lease.scope,
                next_after: lease.common_ancestor,
                pending_request: Some((request_id, max_header_count)),
            },
        );
        if !self.dispatch_action(HeaderSyncAction::ReadHeaderPath {
            peer: peer.clone(),
            session_id,
            lease_id: lease.lease_id,
            scope: lease.scope,
            request_id,
            target_tip_hash: lease.target.hash,
            after_hash: lease.common_ancestor.hash,
            max_header_count,
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
            || pending_request.is_none_or(|(pending_id, _)| pending_id != request_id)
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
            || pending_request.is_some_and(|(_, max_count)| {
                page.entries.len() > usize::try_from(max_count).unwrap_or(usize::MAX)
            })
        {
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
            tree_aux_schema: AuxSchema::None,
            entries: page.entries,
        };
        let sent = self
            .peer_state
            .get(&peer)
            .map(|state| state.session.try_send_headers(&self.codec, response))
            .transpose()
            .is_ok_and(|result| result.is_some());
        if complete || !sent {
            self.release_lease(peer, session_id, lease_id, expected_scope);
        } else {
            self.served_paths.insert(
                peer,
                ServedPathState::Active {
                    session_id,
                    lease_id,
                    target,
                    scope: expected_scope,
                    next_after,
                    pending_request: None,
                },
            );
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
        let response_overhead = 1_u32 + 8 + 32 + 4 + 32 + 4 + 1 + 1;
        let per_header = header_sync_header_bytes_for_network(&self.startup.network)
            .saturating_add(4)
            .saturating_add(if tree_aux_schema == AuxSchema::V1 {
                TREE_AUX_SCHEMA_V1_BYTES
            } else {
                0
            });
        let byte_limited_count = usize::try_from(
            target
                .status
                .max_message_bytes
                .saturating_sub(response_overhead),
        )
        .unwrap_or(usize::MAX)
        .checked_div(per_header)
        .and_then(|count| u32::try_from(count).ok())
        .unwrap_or(0);
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
        let Some(source) = source_id_from_peer(&peer) else {
            self.peer_work_queue.remove_unstarted(&peer);
            return;
        };

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
                    peer,
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
                    debug_assert_eq!(self.pending_owners.insert(source, owner), None);
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
        if self.vct_repair_status.generation != status.generation
            || status.state == zakura_header_chain::VctRootRepairState::Idle
        {
            self.completed_vct_repair_generation = None;
        }
        self.vct_repair_status = status;
        self.schedule_current_vct_repair();
    }

    fn schedule_current_vct_repair(&mut self) {
        self.retire_all_vct_repairs();
        let Some(snapshot) = self.committed_snapshot.as_ref() else {
            return;
        };
        if self.completed_vct_repair_generation == Some(self.vct_repair_status.generation) {
            return;
        }
        if self
            .dispatched_vct_repair
            .is_some_and(|(_, _, generation)| generation == self.vct_repair_status.generation)
        {
            return;
        }
        let Some(task) = vct_repair_task(snapshot, self.vct_repair_status) else {
            return;
        };
        let replaced = self.vct_repairs.insert(task);
        debug_assert!(
            replaced.is_none(),
            "the queue is cleared before scheduling the current repair"
        );
        metrics::counter!("sync.header.vct.repair.scheduled.total").increment(1);
        self.request_vct_repair_context();
    }

    fn request_vct_repair_context(&mut self) {
        let Some(task) = self.vct_repairs.scheduled() else {
            return;
        };
        if task.context.is_some() || task.context_requested {
            return;
        }
        let owner = task.owner;
        let height = task.range.start;
        if self.dispatch_action(HeaderSyncAction::QueryVctRepairContext { owner, height }) {
            let _ = self
                .vct_repairs
                .get_mut(owner)
                .expect("the scheduled repair remains owned during synchronous dispatch")
                .mark_context_requested();
        }
    }

    fn handle_vct_repair_context_ready(
        &mut self,
        owner: zakura_header_chain::WorkOwner,
        result: VctRepairContextResult,
    ) {
        if self.vct_repairs.get(owner).is_none_or(|task| {
            task.phase != RepairPhase::Scheduled
                || task.context.is_some()
                || !task.context_requested
        }) {
            return;
        }
        match result {
            VctRepairContextResult::Resolved(context) => {
                if self
                    .vct_repairs
                    .get_mut(owner)
                    .expect("the exact scheduled repair was checked above")
                    .resolve(context)
                    .is_err()
                {
                    self.vct_repairs.remove(owner);
                    return;
                }
                self.try_assign_vct_repair();
            }
            VctRepairContextResult::Stale => {
                self.vct_repairs.remove(owner);
            }
            VctRepairContextResult::Unavailable => {
                let _ = self
                    .vct_repairs
                    .get_mut(owner)
                    .expect("the exact pending context read was checked above")
                    .context_unavailable();
                metrics::counter!("sync.header.vct.repair.context_unavailable.total").increment(1);
            }
        }
    }

    fn try_assign_vct_repair(&mut self) {
        let Some(task) = self.vct_repairs.scheduled().cloned() else {
            return;
        };
        let Some(context) = task.context.as_ref() else {
            return;
        };
        let Some(predecessor) = context.locator.entries().first().copied() else {
            return;
        };
        let response_bytes = 1_usize
            .saturating_add(8)
            .saturating_add(32)
            .saturating_add(4)
            .saturating_add(32)
            .saturating_add(4)
            .saturating_add(1)
            .saturating_add(1)
            .saturating_add(header_sync_header_bytes_for_network(&self.startup.network))
            .saturating_add(4)
            .saturating_add(TREE_AUX_SCHEMA_V1_BYTES);
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
                    source_id_from_peer(peer)
                        .map(|source| (peer.clone(), source, state.session.clone()))
                })
                .flatten()
            })
            .collect();
        candidates.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
        for (peer, source, session) in candidates {
            let request_id = match session.try_send_get_headers(
                &self.codec,
                task.owner.scope(),
                context.target.hash,
                &context.locator,
                1,
                AuxSchema::V1,
            ) {
                Ok(request_id) => request_id,
                Err(_) => continue,
            };
            let wire_owner = task.owner.scope().bind(
                session.session_id(),
                NonZeroU64::new(request_id.get()).expect("header-sync request IDs are nonzero"),
            );
            if self
                .vct_repairs
                .assign(task.owner, wire_owner, source)
                .is_err()
            {
                session.cancel_request(request_id);
                return;
            }
            debug_assert_eq!(self.pending_owners.insert(source, wire_owner), None);
            metrics::counter!("sync.header.vct.repair.requested.total").increment(1);
            debug!(
                ?peer,
                height = context.target.height.0,
                hash = ?context.target.hash,
                "requested exact selected VCT metadata repair"
            );
            return;
        }
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
            match self.served_paths.remove(&peer) {
                Some(ServedPathState::Active {
                    session_id,
                    lease_id,
                    scope,
                    ..
                }) => self.release_lease(peer, session_id, lease_id, scope),
                Some(ServedPathState::Acquiring { .. }) | None => {}
            }
        }
        self.body_retries
            .retain_current(snapshot.header_generation, snapshot.frontiers.finalized);
        for task in self.vct_repairs.retain_current(snapshot) {
            if let Some(source) = task.source {
                self.pending_owners.remove(source, task.owner.request_id);
            }
        }
        self.coverage
            .retain_current(snapshot.header_generation, snapshot.frontiers.finalized);
        if let Some(previous) = self.committed_snapshot.as_ref() {
            let retired = zakura_header_chain::RetiredWork {
                header_generation_changed: previous.header_generation != snapshot.header_generation,
                verified_generation_changed: previous.verified_generation
                    != snapshot.verified_generation,
                owners: Vec::new(),
            };
            let retired_owners = self.pending_owners.apply_retirement(&retired, snapshot);
            for owner in retired_owners {
                self.peer_work_queue.remove_owner(owner);
            }
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
        self.drive_vct_repair_capacity();
    }

    fn next_maintenance_deadline(&self) -> Instant {
        self.peer_state
            .values()
            .filter_map(|state| {
                state
                    .status_publisher
                    .as_ref()
                    .map(StatusPublisher::next_deadline)
            })
            .min()
            .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(60))
    }

    fn served_page_count(&self, requested: u32) -> u32 {
        let response_overhead = 1_u32 + 8 + 32 + 4 + 32 + 4 + 1 + 1;
        let per_header = u32::try_from(header_sync_header_bytes_for_network(&self.startup.network))
            .unwrap_or(u32::MAX)
            .saturating_add(4);
        let byte_limited = self
            .serving_limits
            .max_message_bytes()
            .saturating_sub(response_overhead)
            .checked_div(per_header)
            .unwrap_or(0);
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
        let Some(ServedPathState::Active {
            session_id,
            lease_id,
            scope,
            ..
        }) = self.served_paths.remove(peer)
        else {
            return;
        };
        self.release_lease(peer.clone(), session_id, lease_id, scope);
    }

    fn release_lease(
        &self,
        peer: ZakuraPeerId,
        session_id: u64,
        lease_id: u64,
        scope: zakura_header_chain::WorkScope,
    ) {
        let _ = self.dispatch_action(HeaderSyncAction::ReleaseHeaderPath {
            peer,
            session_id,
            lease_id,
            scope,
        });
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

    fn dispatch_action(&self, action: HeaderSyncAction) -> bool {
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
        source: zakura_header_chain::SourceId,
        owner: &zakura_header_chain::WorkOwner,
    ) -> bool {
        let Some(current) = self.committed_snapshot.as_ref() else {
            return false;
        };
        match zakura_header_chain::CompletionGate::check(
            current,
            &self.pending_owners,
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
        if let Some(active) = self.peer_work_queue.remove(peer) {
            self.pending_owners
                .remove(active.source, active.owner.request_id);
        }
    }

    fn handle_typed_failure(
        &self,
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

    fn report_misbehavior(&self, peer: ZakuraPeerId, reason: HeaderSyncMisbehavior) {
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

    fn dispatch_misbehavior(&self, peer: ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        let _ = self.dispatch_action(HeaderSyncAction::Misbehavior { peer, reason });
    }
}

fn next_height(height: block::Height) -> block::Height {
    block::Height(height.0.saturating_add(1).min(block::Height::MAX.0))
}

fn node_id_from_peer(peer: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

fn source_id_from_peer(peer: &ZakuraPeerId) -> Option<zakura_header_chain::SourceId> {
    let digest = <[u8; 32]>::try_from(peer.as_bytes()).ok()?;
    Some(zakura_header_chain::SourceId::from_digest(digest))
}

fn ordered_send_error_label(error: &OrderedSendError) -> &'static str {
    match error {
        OrderedSendError::Full => "full",
        OrderedSendError::Closed => "closed",
        OrderedSendError::Encode(_) => "encode",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio_util::sync::CancellationToken;
    use zakura_chain::{block::genesis::regtest_genesis_block, parameters::Network};

    use super::*;
    use crate::zakura::{framed_channel, LOCAL_MAX_MESSAGE_BYTES};

    fn peer() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![0x71; 32]).expect("the test peer ID has the required length")
    }

    fn stale_failure(
        owner: zakura_header_chain::WorkOwner,
    ) -> Arc<zakura_header_chain::HeaderChainError> {
        Arc::new(zakura_header_chain::HeaderChainError::stale_target(
            zakura_header_chain::ErrorSubject::Branch(owner.branch),
        ))
    }

    fn local_failure(
        owner: zakura_header_chain::WorkOwner,
    ) -> Arc<zakura_header_chain::HeaderChainError> {
        Arc::new(zakura_header_chain::HeaderChainError::local_resource(
            zakura_header_chain::ErrorSubject::Branch(owner.branch),
            None,
        ))
    }

    fn invalid_header_failure(
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::WorkOwner,
    ) -> Arc<zakura_header_chain::HeaderChainError> {
        Arc::new(zakura_header_chain::HeaderChainError::invalid_header(
            zakura_header_chain::ErrorSubject::Header(zakura_header_chain::HeaderId::new(
                owner.branch.target_tip_hash,
            )),
            zakura_header_chain::RuleId::new("LC-VAL-02"),
            zakura_header_chain::EvidenceId::from_digest([0x71; 32]),
            source,
            None,
        ))
    }

    fn request(request_id: u64, target: block::Hash, locator: block::Hash) -> GetHeaders {
        GetHeaders {
            request_id,
            target_tip_hash: target,
            locator_hashes: vec![locator],
            max_header_count: 1,
            tree_aux_schema: AuxSchema::V1,
        }
    }

    fn startup(shutdown: CancellationToken) -> HeaderSyncStartup {
        let network = Network::new_regtest(Default::default());
        let anchor = (block::Height(0), network.genesis_hash());
        let mut startup = HeaderSyncStartup::new(
            network,
            anchor,
            FullStateFrontiers {
                finalized_height: anchor.0,
                verified_block_tip: anchor.0,
                verified_block_hash: anchor.1,
            },
            Some(anchor),
            ZakuraHeaderSyncConfig::default(),
            LOCAL_MAX_MESSAGE_BYTES,
        );
        startup.shutdown = shutdown;
        startup
    }

    fn committed_snapshot(
        anchor: zakura_header_chain::Frontier,
    ) -> zakura_header_chain::EngineSnapshot {
        zakura_header_chain::EngineSnapshot {
            mode: zakura_header_chain::EngineMode::Integrated,
            state_version: zakura_header_chain::StateVersion::new(3),
            header_generation: zakura_header_chain::HeaderGeneration::new(4),
            verified_generation: zakura_header_chain::VerifiedGeneration::new(5),
            frontiers: zakura_header_chain::FrontierSet {
                finalized: anchor,
                header_best: anchor,
                verified_best: anchor,
            },
            header_best_score: zakura_header_chain::ChainScore::new(
                zakura_header_chain::SuffixWork::zero(),
                anchor.hash,
            ),
            oldest_retained_height: anchor.height,
            alarms: Default::default(),
        }
    }

    fn seed_applying_request(
        reactor: &mut HeaderSyncReactor,
        snapshot: &zakura_header_chain::EngineSnapshot,
        peer: ZakuraPeerId,
        session_id: u64,
    ) -> (
        zakura_header_chain::SourceId,
        zakura_header_chain::WorkOwner,
        BranchRange,
    ) {
        let source = source_id_from_peer(&peer).expect("the fixed peer has a source identity");
        let anchor = snapshot.frontiers.finalized;
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = anchor.hash;
        header.time += chrono::Duration::seconds(1);
        let header = Arc::new(header);
        let target = zakura_header_chain::Frontier::new(
            anchor
                .height
                .next()
                .expect("the genesis fixture has a next height"),
            header.hash(),
        );
        let request_id = HeaderSyncRequestId::new(9).expect("nine is nonzero");
        let owner = zakura_header_chain::WorkOwner {
            state_version: snapshot.state_version,
            header_generation: snapshot.header_generation,
            verified_generation: None,
            branch: zakura_header_chain::BranchId::new(anchor.hash, target.hash),
            session_id,
            request_id: NonZeroU64::new(request_id.get()).expect("the request ID is nonzero"),
        };
        let advertised = AdvertisedHeaderTarget {
            scope: zakura_header_chain::WorkScope::for_header_target(snapshot, target.hash),
            session_id: owner.session_id,
            observed_at: Instant::now(),
            status: Status {
                work_anchor_height: anchor.height,
                work_anchor_hash: anchor.hash,
                selected_tip_height: target.height,
                selected_tip_hash: target.hash,
                suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(1_u8),
                oldest_retained_height: anchor.height,
                max_headers_per_response: 1,
                max_inflight_requests: 1,
                max_message_bytes: 1_000,
                tree_aux_schema_mask: 0,
            },
        };
        assert_eq!(
            reactor.peer_work_queue.stage(
                peer.clone(),
                advertised.clone(),
                PeerWorkPriority::Normal,
            ),
            QueueWorkResult::NeedsLocator
        );
        assert!(reactor.peer_work_queue.start(ActiveHeaderRequest {
            peer,
            source,
            target: advertised,
            sent_locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
            request_id,
            owner,
            common_ancestor: Some(anchor),
            entries: vec![HeaderEntry {
                header,
                body_size: 0,
                tree_aux: None,
            }],
            phase: HeaderTargetPhase::Applying,
            max_header_count: 1,
            tree_aux_schema: AuxSchema::None,
        }));
        assert_eq!(reactor.pending_owners.insert(source, owner), None);
        let range = BranchRange::new(owner.branch, target.height, target.height)
            .expect("the one-header applying range is ordered");
        (source, owner, range)
    }

    fn peer_violation_fixture() -> (
        HeaderSyncReactor,
        mpsc::Receiver<HeaderSyncAction>,
        zakura_header_chain::EngineSnapshot,
        ZakuraPeerId,
        zakura_header_chain::SourceId,
        zakura_header_chain::WorkOwner,
    ) {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown);
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let snapshot = committed_snapshot(anchor);
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (_handle, actions, mut reactor) =
            build_header_sync_reactor(startup).expect("the violation fixture starts");
        let peer = peer();
        let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 0);
        (reactor, actions, snapshot, peer, source, owner)
    }

    fn assert_peer_violation(
        actions: &mut mpsc::Receiver<HeaderSyncAction>,
        expected: HeaderSyncMisbehavior,
    ) {
        assert!(matches!(
            actions.try_recv(),
            Ok(HeaderSyncAction::Misbehavior { reason, .. }) if reason == expected
        ));
        assert!(
            actions.try_recv().is_err(),
            "one invalid response emits exactly one peer violation"
        );
    }

    #[test]
    fn wrong_locator_ancestor_target_and_prepared_header_are_peer_attributable() {
        let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
        let active = reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the fixture has active work");
        let wrong_ancestor = zakura_header_chain::Frontier::new(
            snapshot.frontiers.finalized.height,
            block::Hash([0x41; 32]),
        );
        let mut wrong_ancestor_header = *active.entries[0].header;
        wrong_ancestor_header.previous_block_hash = wrong_ancestor.hash;
        active.phase = HeaderTargetPhase::Receiving;
        active.common_ancestor = None;
        active.entries.clear();
        let wrong_ancestor_response = Headers {
            request_id: owner.request_id.get(),
            target_tip_hash: owner.branch.target_tip_hash,
            common_ancestor_height: wrong_ancestor.height,
            common_ancestor_hash: wrong_ancestor.hash,
            complete: false,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header: Arc::new(wrong_ancestor_header),
                body_size: 0,
                tree_aux: None,
            }],
        };
        assert!(
            reactor
                .codec
                .encode(&HeaderSyncMessage::Headers(wrong_ancestor_response.clone()))
                .is_ok(),
            "the wrong locator member is otherwise wire-valid"
        );
        reactor.handle_headers(peer.clone(), 0, owner.scope(), wrong_ancestor_response);
        assert_peer_violation(&mut actions, HeaderSyncMisbehavior::MalformedMessage);

        let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
        let active = reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the fixture has active work");
        let header = active.entries[0].header.clone();
        let mut wrong_target_header = *header;
        wrong_target_header.time += chrono::Duration::seconds(1);
        active.phase = HeaderTargetPhase::Receiving;
        active.common_ancestor = None;
        active.entries.clear();
        reactor.handle_headers(
            peer.clone(),
            0,
            owner.scope(),
            Headers {
                request_id: owner.request_id.get(),
                target_tip_hash: owner.branch.target_tip_hash,
                common_ancestor_height: snapshot.frontiers.finalized.height,
                common_ancestor_hash: snapshot.frontiers.finalized.hash,
                complete: true,
                tree_aux_schema: AuxSchema::None,
                entries: vec![HeaderEntry {
                    header: Arc::new(wrong_target_header),
                    body_size: 0,
                    tree_aux: None,
                }],
            },
        );
        assert_peer_violation(&mut actions, HeaderSyncMisbehavior::MalformedMessage);

        let (mut reactor, mut actions, _snapshot, peer, source, owner) = peer_violation_fixture();
        reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the fixture has active work")
            .phase = HeaderTargetPhase::Preparing;
        reactor.handle_header_target_prepared(
            peer,
            source,
            owner,
            HeaderTargetPreparationResult::Failed(invalid_header_failure(source, owner)),
        );
        assert_peer_violation(&mut actions, HeaderSyncMisbehavior::InvalidHeader);
    }

    #[test]
    fn typed_taxonomy_scores_only_exact_attributed_header_peer_faults() {
        let (reactor, mut actions, _snapshot, peer, source, owner) = peer_violation_fixture();
        let subject = zakura_header_chain::ErrorSubject::Branch(owner.branch);

        for (category, expected) in [
            (
                zakura_header_chain::ErrorCategory::MalformedProtocol,
                HeaderSyncMisbehavior::MalformedMessage,
            ),
            (
                zakura_header_chain::ErrorCategory::InvalidHeader,
                HeaderSyncMisbehavior::InvalidHeader,
            ),
        ] {
            let error = zakura_header_chain::HeaderChainError::new(
                category,
                subject,
                None,
                None,
                zakura_header_chain::Attribution::HeaderPeer(source),
                None,
            );
            reactor.handle_typed_failure(peer.clone(), source, &error);
            assert_peer_violation(&mut actions, expected);
        }

        for category in [
            zakura_header_chain::ErrorCategory::ValidLosingFork,
            zakura_header_chain::ErrorCategory::DeferredHeader,
            zakura_header_chain::ErrorCategory::BodyPayloadMismatch,
            zakura_header_chain::ErrorCategory::ConsensusBodyInvalid,
            zakura_header_chain::ErrorCategory::OperatorIneligible,
            zakura_header_chain::ErrorCategory::StaleTargetOrGeneration,
            zakura_header_chain::ErrorCategory::LocalAnchorOrIncoherence,
            zakura_header_chain::ErrorCategory::LocalResourceOrStorage,
        ] {
            let error = zakura_header_chain::HeaderChainError::new(
                category,
                subject,
                None,
                None,
                zakura_header_chain::Attribution::HeaderPeer(source),
                None,
            );
            reactor.handle_typed_failure(peer.clone(), source, &error);
            assert!(
                actions.try_recv().is_err(),
                "{category:?} cannot cross the header-peer scoring boundary"
            );
        }

        let wrong_source = zakura_header_chain::SourceId::from_digest([0x72; 32]);
        for category in [
            zakura_header_chain::ErrorCategory::MalformedProtocol,
            zakura_header_chain::ErrorCategory::InvalidHeader,
        ] {
            for attribution in [
                zakura_header_chain::Attribution::None,
                zakura_header_chain::Attribution::HeaderPeer(wrong_source),
                zakura_header_chain::Attribution::BodyPeer(source),
                zakura_header_chain::Attribution::AuxPeer(source),
            ] {
                let error = zakura_header_chain::HeaderChainError::new(
                    category,
                    subject,
                    None,
                    None,
                    attribution,
                    None,
                );
                reactor.handle_typed_failure(peer.clone(), source, &error);
                assert!(
                    actions.try_recv().is_err(),
                    "{category:?} with {attribution:?} cannot score this header peer"
                );
            }
        }
    }

    #[test]
    fn response_completion_requires_the_reserved_branch_scope() {
        let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
        let expected = reactor
            .peer_work_queue
            .active(&peer)
            .expect("the fixture has active work")
            .clone();
        let mut wrong_scope = owner.scope();
        wrong_scope.header_generation = wrong_scope
            .header_generation
            .checked_next()
            .expect("the fixture generation has a successor");
        reactor.handle_headers(
            peer.clone(),
            0,
            wrong_scope,
            Headers {
                request_id: owner.request_id.get(),
                target_tip_hash: owner.branch.target_tip_hash,
                common_ancestor_height: snapshot.frontiers.finalized.height,
                common_ancestor_hash: snapshot.frontiers.finalized.hash,
                complete: true,
                tree_aux_schema: AuxSchema::None,
                entries: Vec::new(),
            },
        );
        assert_eq!(reactor.peer_work_queue.active(&peer), Some(&expected));
        assert!(
            actions.try_recv().is_err(),
            "a scope-mismatched page has no peer or scheduling effect"
        );

        let (mut reactor, mut actions, _snapshot, peer, _source, owner) = peer_violation_fixture();
        let expected = reactor
            .peer_work_queue
            .active(&peer)
            .expect("the fixture has active work")
            .clone();
        let mut wrong_scope = owner.scope();
        wrong_scope.branch =
            zakura_header_chain::BranchId::new(owner.branch.anchor_hash, block::Hash([0x73; 32]));
        reactor.handle_headers_outcome(
            peer.clone(),
            0,
            wrong_scope,
            HeadersOutcome {
                request_id: owner.request_id.get(),
                target_tip_hash: owner.branch.target_tip_hash,
                outcome: HeadersOutcomeCode::Busy,
            },
        );
        assert_eq!(reactor.peer_work_queue.active(&peer), Some(&expected));
        assert!(
            actions.try_recv().is_err(),
            "a scope-mismatched outcome has no peer or scheduling effect"
        );
    }

    #[test]
    fn aggregate_staging_overflow_retires_work_without_peer_punishment() {
        let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
        let active = reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the fixture has active work");
        let entry = active.entries[0].clone();
        active.phase = HeaderTargetPhase::Receiving;
        active.common_ancestor = Some(snapshot.frontiers.finalized);
        active.entries =
            vec![entry; crate::zakura::header_sync::scheduler::peer_work::MAX_STAGED_HEADERS_V1];
        let staged_tip = active
            .staged_tip()
            .expect("the bounded staged fixture has an inferred tip");
        let mut next_header = *regtest_genesis_block().header;
        next_header.previous_block_hash = staged_tip.hash;
        let response = Headers {
            request_id: owner.request_id.get(),
            target_tip_hash: owner.branch.target_tip_hash,
            common_ancestor_height: staged_tip.height,
            common_ancestor_hash: staged_tip.hash,
            complete: false,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header: Arc::new(next_header),
                body_size: 0,
                tree_aux: None,
            }],
        };
        assert!(
            reactor
                .codec
                .encode(&HeaderSyncMessage::Headers(response.clone()))
                .is_ok(),
            "the overflowing page is otherwise wire-valid"
        );

        reactor.handle_headers(peer.clone(), 0, owner.scope(), response);

        assert!(
            reactor.peer_work_queue.active(&peer).is_none(),
            "overflow retires the target and releases all staged headers"
        );
        assert!(
            actions.try_recv().is_err(),
            "local staging pressure emits no peer violation"
        );
    }

    #[test]
    fn aud_06_07_live_reactor_ignores_results_retired_by_a_committed_snapshot() {
        {
            let mut startup = startup(CancellationToken::new());
            let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
            let initial = committed_snapshot(anchor);
            let (_snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
            startup.committed_snapshots = Some(snapshots_rx);
            let (_handle, _actions, mut reactor) =
                build_header_sync_reactor(startup).expect("the current-result control builds");
            let peer = peer();
            let (source, owner, range) =
                seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
            reactor.handle_event(HeaderSyncEvent::HeaderTargetAdmissionReady {
                peer,
                source,
                owner,
                result: HeaderTargetAdmissionResult::Applied,
            });
            assert!(
                reactor
                    .coverage
                    .covers_tip(owner.header_generation, range.branch, range.end),
                "the current-result control proves the live handler can mark coverage"
            );
        }

        for is_local_failure in [false, true] {
            let shutdown = CancellationToken::new();
            let mut startup = startup(shutdown);
            let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
            let initial = committed_snapshot(anchor);
            let (_snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
            startup.committed_snapshots = Some(snapshots_rx);
            let (handle, mut actions, mut reactor) =
                build_header_sync_reactor(startup).expect("the live reactor fixture builds");
            let peer = peer();
            let (source, owner, old_range) =
                seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
            let result = if is_local_failure {
                HeaderTargetAdmissionResult::Failed(local_failure(owner))
            } else {
                HeaderTargetAdmissionResult::Applied
            };

            let replacement =
                zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0xb2; 32]));
            let mut committed = initial.clone();
            committed.state_version = zakura_header_chain::StateVersion::new(
                initial.state_version.get().saturating_add(1),
            );
            committed.header_generation = initial
                .header_generation
                .checked_next()
                .expect("the bounded fixture generation advances");
            committed.frontiers.header_best = replacement;
            committed.header_best_score = zakura_header_chain::ChainScore::new(
                zakura_header_chain::SuffixWork::zero(),
                replacement.hash,
            );
            reactor.observe_latest_committed_snapshot(committed.clone());

            assert!(reactor.peer_work_queue.active(&peer).is_none());
            assert!(reactor.pending_owners.is_empty());
            assert!(!reactor.coverage.covers_tip(
                owner.header_generation,
                old_range.branch,
                old_range.end
            ));
            let published_tip = handle.best_header_tip();
            let published_candidates = handle.candidate_state();
            assert_eq!(published_tip, (replacement.height, replacement.hash));

            reactor.handle_event(HeaderSyncEvent::HeaderTargetAdmissionReady {
                peer,
                source,
                owner,
                result,
            });

            assert_eq!(reactor.committed_snapshot, Some(committed));
            assert_eq!(handle.best_header_tip(), published_tip);
            assert_eq!(handle.candidate_state(), published_candidates);
            assert!(reactor.pending_owners.is_empty());
            assert!(!reactor.coverage.covers_tip(
                owner.header_generation,
                old_range.branch,
                old_range.end
            ));
            assert!(matches!(
                actions.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stale_anchor_admission_reanchors_from_durable_snapshot_without_retry_or_score() {
        let mut startup = startup(CancellationToken::new());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let initial = committed_snapshot(anchor);
        let (snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, mut actions, mut reactor) =
            build_header_sync_reactor(startup).expect("the stale-anchor fixture builds");
        let peer = peer();
        let (send, mut outbound) = framed_channel(8);
        reactor.handle_event(HeaderSyncEvent::PeerConnected(
            HeaderSyncPeerSession::from_parts_with_session_id(
                peer.clone(),
                7,
                send,
                CancellationToken::new(),
            ),
        ));
        let initial_status = outbound
            .recv()
            .await
            .expect("the initial committed status is sent");
        assert!(matches!(
            handle
                .codec()
                .decode_frame(initial_status, None)
                .expect("the initial status decodes"),
            HeaderSyncMessage::Status(_)
        ));

        let (source, owner, old_range) =
            seed_applying_request(&mut reactor, &initial, peer.clone(), 7);
        reactor.handle_event(HeaderSyncEvent::HeaderTargetAdmissionReady {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetAdmissionResult::Failed(stale_failure(owner)),
        });

        assert!(reactor.peer_work_queue.active(&peer).is_none());
        assert!(reactor.pending_owners.is_empty());
        assert!(!reactor.coverage.covers_tip(
            owner.header_generation,
            old_range.branch,
            old_range.end
        ));
        assert!(
            actions.try_recv().is_err(),
            "a stale local anchor neither retries work nor scores its peer"
        );

        let replacement =
            zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0xb3; 32]));
        let mut committed = initial.clone();
        committed.state_version = initial
            .state_version
            .checked_next()
            .expect("the fixture state version advances");
        committed.header_generation = initial
            .header_generation
            .checked_next()
            .expect("the fixture header generation advances");
        committed.verified_generation = initial
            .verified_generation
            .checked_next()
            .expect("the fixture verified generation advances");
        committed.frontiers.finalized = replacement;
        committed.frontiers.header_best = replacement;
        committed.frontiers.verified_best = replacement;
        committed.header_best_score = zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            replacement.hash,
        );
        committed.oldest_retained_height = replacement.height;
        snapshots_tx
            .send(Some(committed.clone()))
            .expect("the durable snapshot receiver remains live");
        let durable = reactor
            .startup
            .committed_snapshots
            .as_ref()
            .and_then(|snapshots| snapshots.borrow().clone())
            .expect("the committed watch exposes the winning anchor");
        reactor.observe_latest_committed_snapshot(durable);

        assert_eq!(reactor.committed_snapshot, Some(committed.clone()));
        assert_eq!(
            handle.best_header_tip(),
            (replacement.height, replacement.hash)
        );
        assert!(reactor.peer_work_queue.active(&peer).is_none());
        assert!(reactor.pending_owners.is_empty());
        assert!(
            actions.try_recv().is_err(),
            "re-anchoring does not hot-retry the impossible owner"
        );

        time::advance(std::time::Duration::from_secs(1)).await;
        reactor.refresh_statuses();
        let refreshed = outbound
            .recv()
            .await
            .expect("the bounded status floor eventually publishes the new anchor");
        let HeaderSyncMessage::Status(status) = handle
            .codec()
            .decode_frame(refreshed, None)
            .expect("the refreshed status decodes")
        else {
            panic!("the re-anchor publication must be Status");
        };
        assert_eq!(status.work_anchor_height, replacement.height);
        assert_eq!(status.work_anchor_hash, replacement.hash);
        assert_eq!(status.selected_tip_height, replacement.height);
        assert_eq!(status.selected_tip_hash, replacement.hash);
        assert!(
            !reactor
                .peer_state
                .get(&peer)
                .and_then(|state| state.status_publisher.as_ref())
                .expect("the connected peer retains its publisher")
                .due(Instant::now()),
            "one refreshed publication satisfies the changed status"
        );
        assert!(
            actions.try_recv().is_err(),
            "status refresh emits neither a retry nor peer punishment"
        );
    }

    #[test]
    fn aud_14_reactor_restart_drops_old_preparation_and_admission_completions() {
        let shutdown = CancellationToken::new();
        let mut old_startup = startup(shutdown);
        let anchor = zakura_header_chain::Frontier::new(old_startup.anchor.0, old_startup.anchor.1);
        let initial = committed_snapshot(anchor);
        let (_old_snapshots_tx, old_snapshots_rx) = watch::channel(Some(initial.clone()));
        old_startup.committed_snapshots = Some(old_snapshots_rx);
        let (_old_handle, _old_actions, mut old_reactor) =
            build_header_sync_reactor(old_startup).expect("the pre-crash reactor builds");
        let peer = peer();
        let (source, owner, old_range) =
            seed_applying_request(&mut old_reactor, &initial, peer.clone(), 7);
        old_reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the pre-crash work remains active")
            .phase = HeaderTargetPhase::Preparing;
        drop(old_reactor);

        let shutdown = CancellationToken::new();
        let mut same_startup = startup(shutdown);
        let (_same_snapshots_tx, same_snapshots_rx) = watch::channel(Some(initial.clone()));
        same_startup.committed_snapshots = Some(same_snapshots_rx);
        let (same_handle, mut same_actions, mut same_reactor) =
            build_header_sync_reactor(same_startup).expect("the same-snapshot restart builds");
        let same_tip = same_handle.best_header_tip();
        let same_candidates = same_handle.candidate_state();
        same_reactor.handle_event(HeaderSyncEvent::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetPreparationResult::Failed(local_failure(owner)),
        });
        assert_eq!(same_handle.best_header_tip(), same_tip);
        assert_eq!(same_handle.candidate_state(), same_candidates);
        assert!(same_reactor.peer_work_queue.active(&peer).is_none());
        assert!(same_reactor.pending_owners.is_empty());
        assert!(!same_reactor.coverage.covers_tip(
            owner.header_generation,
            old_range.branch,
            old_range.end
        ));
        assert!(matches!(
            same_actions.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        let replacement =
            zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0xc2; 32]));
        let mut committed = initial;
        committed.state_version =
            zakura_header_chain::StateVersion::new(committed.state_version.get().saturating_add(1));
        committed.header_generation = committed
            .header_generation
            .checked_next()
            .expect("the bounded fixture generation advances");
        committed.frontiers.header_best = replacement;
        committed.header_best_score = zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            replacement.hash,
        );

        let shutdown = CancellationToken::new();
        let mut committed_startup = startup(shutdown);
        let (_committed_snapshots_tx, committed_snapshots_rx) =
            watch::channel(Some(committed.clone()));
        committed_startup.committed_snapshots = Some(committed_snapshots_rx);
        let (committed_handle, mut committed_actions, mut committed_reactor) =
            build_header_sync_reactor(committed_startup)
                .expect("the post-commit reactor restart builds");
        let committed_tip = committed_handle.best_header_tip();
        let committed_candidates = committed_handle.candidate_state();
        assert_eq!(committed_tip, (replacement.height, replacement.hash));
        committed_reactor.handle_event(HeaderSyncEvent::HeaderTargetAdmissionReady {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetAdmissionResult::Applied,
        });
        committed_reactor.handle_event(HeaderSyncEvent::HeaderTargetAdmissionReady {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetAdmissionResult::Failed(local_failure(owner)),
        });
        assert_eq!(committed_reactor.committed_snapshot, Some(committed));
        assert_eq!(committed_handle.best_header_tip(), committed_tip);
        assert_eq!(committed_handle.candidate_state(), committed_candidates);
        assert!(committed_reactor.peer_work_queue.active(&peer).is_none());
        assert!(committed_reactor.pending_owners.is_empty());
        assert!(!committed_reactor.coverage.covers_tip(
            owner.header_generation,
            old_range.branch,
            old_range.end
        ));
        assert!(matches!(
            committed_actions.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn aud_14_reactor_restart_rejects_an_old_ordered_stream_response() {
        let mut old_startup = startup(CancellationToken::new());
        let anchor = zakura_header_chain::Frontier::new(old_startup.anchor.0, old_startup.anchor.1);
        let snapshot = committed_snapshot(anchor);
        let (_old_snapshots_tx, old_snapshots_rx) = watch::channel(Some(snapshot.clone()));
        old_startup.committed_snapshots = Some(old_snapshots_rx);
        let (_old_handle, _old_actions, mut old_reactor) =
            build_header_sync_reactor(old_startup).expect("the pre-crash reactor builds");
        let peer = peer();
        let (old_send, _old_outbound) = framed_channel(8);
        old_reactor.handle_event(HeaderSyncEvent::PeerConnected(
            HeaderSyncPeerSession::from_parts_with_session_id(
                peer.clone(),
                7,
                old_send,
                CancellationToken::new(),
            ),
        ));
        let _ = seed_applying_request(&mut old_reactor, &snapshot, peer.clone(), 7);
        let old_active = old_reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the old ordered stream has one request");
        old_active.phase = HeaderTargetPhase::Receiving;
        old_active.common_ancestor = None;
        let response_entry = old_active
            .entries
            .pop()
            .expect("the fixture has one response entry");
        let stale_response = Headers {
            request_id: old_active.request_id.get(),
            target_tip_hash: old_active.target.status.selected_tip_hash,
            common_ancestor_height: anchor.height,
            common_ancestor_hash: anchor.hash,
            complete: true,
            tree_aux_schema: AuxSchema::None,
            entries: vec![response_entry],
        };
        let stale_scope = old_active.owner.scope();
        drop(old_reactor);

        let mut fresh_startup = startup(CancellationToken::new());
        let (_fresh_snapshots_tx, fresh_snapshots_rx) = watch::channel(Some(snapshot.clone()));
        fresh_startup.committed_snapshots = Some(fresh_snapshots_rx);
        let (fresh_handle, mut fresh_actions, mut fresh_reactor) =
            build_header_sync_reactor(fresh_startup).expect("the replacement reactor builds");
        let (fresh_send, mut fresh_outbound) = framed_channel(8);
        fresh_reactor.handle_event(HeaderSyncEvent::PeerConnected(
            HeaderSyncPeerSession::from_parts_with_session_id(
                peer.clone(),
                8,
                fresh_send,
                CancellationToken::new(),
            ),
        ));
        let status = fresh_outbound
            .recv()
            .await
            .expect("the replacement stream receives its initial status");
        assert!(matches!(
            fresh_handle
                .codec()
                .decode_frame(status, None)
                .expect("the replacement status decodes"),
            HeaderSyncMessage::Status(_)
        ));
        let _ = seed_applying_request(&mut fresh_reactor, &snapshot, peer.clone(), 8);
        let fresh_active = fresh_reactor
            .peer_work_queue
            .active_mut(&peer)
            .expect("the replacement stream has one request");
        fresh_active.phase = HeaderTargetPhase::Receiving;
        fresh_active.common_ancestor = None;
        fresh_active.entries.clear();
        let expected_active = fresh_active.clone();
        let published_tip = fresh_handle.best_header_tip();
        let published_candidates = fresh_handle.candidate_state();

        fresh_reactor.handle_event(HeaderSyncEvent::SessionResponse {
            peer: peer.clone(),
            session_id: 7,
            scope: stale_scope,
            msg: HeaderSyncMessage::Headers(stale_response),
        });

        assert_eq!(
            fresh_reactor
                .peer_state
                .get(&peer)
                .expect("the replacement peer remains connected")
                .session
                .session_id(),
            8
        );
        assert_eq!(
            fresh_reactor.peer_work_queue.active(&peer),
            Some(&expected_active)
        );
        assert_eq!(fresh_handle.best_header_tip(), published_tip);
        assert_eq!(fresh_handle.candidate_state(), published_candidates);
        assert!(matches!(
            fresh_actions.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(
            time::timeout(std::time::Duration::from_millis(10), fresh_outbound.recv())
                .await
                .is_err(),
            "the stale response emits no replacement-stream frame"
        );
    }

    #[tokio::test]
    async fn initial_committed_snapshot_overrides_legacy_startup_frontiers() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown);
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let header_best =
            zakura_header_chain::Frontier::new(block::Height(7), block::Hash([0x77; 32]));
        let mut snapshot = committed_snapshot(anchor);
        snapshot.frontiers.header_best = header_best;
        snapshot.header_best_score = zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            header_best.hash,
        );
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot));
        startup.committed_snapshots = Some(snapshots_rx);

        let (handle, _actions, reactor) =
            spawn_header_sync_reactor(startup).expect("the snapshot-authoritative reactor starts");
        assert_eq!(
            handle.best_header_tip(),
            (header_best.height, header_best.hash)
        );

        reactor.abort();
    }

    #[tokio::test]
    async fn peer_admission_catches_up_snapshot_before_initial_status() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown);
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let (snapshots_tx, snapshots_rx) = watch::channel(None);
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, _actions, reactor) =
            spawn_header_sync_reactor(startup).expect("the pre-handoff reactor starts");

        let header_best =
            zakura_header_chain::Frontier::new(block::Height(7), block::Hash([0x77; 32]));
        let mut snapshot = committed_snapshot(anchor);
        snapshot.frontiers.header_best = header_best;
        snapshot.header_best_score = zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            header_best.hash,
        );
        snapshots_tx
            .send(Some(snapshot))
            .expect("the committed snapshot receiver is live");

        let (send, mut outbound) = framed_channel(8);
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer(), send, CancellationToken::new()),
            ))
            .await
            .expect("peer admission queues before the watch arm runs");

        let status_frame = time::timeout(time::Duration::from_secs(1), outbound.recv())
            .await
            .expect("the initial status is sent promptly")
            .expect("the peer outbound remains open");
        let HeaderSyncMessage::Status(status) = handle
            .codec()
            .decode_frame(status_frame, None)
            .expect("the initial status decodes")
        else {
            panic!("the first session message must be the committed status");
        };
        assert_eq!(status.selected_tip_height, header_best.height);
        assert_eq!(status.selected_tip_hash, header_best.hash);

        reactor.abort();
    }

    #[test]
    fn vct_repair_signal_schedules_one_exact_current_branch_range() {
        let anchor = zakura_header_chain::Frontier::new(block::Height(10), block::Hash([1; 32]));
        let mut snapshot = committed_snapshot(anchor);
        snapshot.frontiers.header_best =
            zakura_header_chain::Frontier::new(block::Height(20), block::Hash([2; 32]));
        let status = zakura_header_chain::VctRootRepairStatus {
            state: zakura_header_chain::VctRootRepairState::Unavailable {
                height: block::Height(11),
            },
            generation: 7,
        };

        let task = vct_repair_task(&snapshot, status)
            .expect("an in-range repair need schedules exact work");
        assert_eq!(task.range.start, block::Height(11));
        assert_eq!(task.range.end, block::Height(11));
        assert_eq!(task.owner.state_version, snapshot.state_version);
        assert_eq!(task.owner.header_generation, snapshot.header_generation);
        assert_eq!(
            task.owner.verified_generation,
            Some(snapshot.verified_generation)
        );
        assert_eq!(
            task.owner.branch,
            zakura_header_chain::BranchId::new(
                snapshot.frontiers.finalized.hash,
                snapshot.frontiers.header_best.hash,
            )
        );
        assert_eq!(task.owner.session_id, INTERNAL_VCT_REPAIR_SESSION_ID);
        assert_eq!(task.owner.request_id.get(), 8);

        assert!(vct_repair_task(
            &snapshot,
            zakura_header_chain::VctRootRepairStatus::default()
        )
        .is_none());
        assert!(vct_repair_task(
            &snapshot,
            zakura_header_chain::VctRootRepairStatus {
                state: zakura_header_chain::VctRootRepairState::Unavailable {
                    height: snapshot.frontiers.finalized.height,
                },
                generation: 0,
            }
        )
        .is_none());
        assert!(vct_repair_task(
            &snapshot,
            zakura_header_chain::VctRootRepairStatus {
                state: status.state,
                generation: u64::MAX,
            }
        )
        .is_none());
    }

    #[tokio::test]
    async fn vct_repair_uses_one_exact_canonical_auxiliary_request() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let mut repair_block_header = *regtest_genesis_block().header;
        repair_block_header.previous_block_hash = anchor.hash;
        repair_block_header.time += chrono::Duration::seconds(1);
        let repair_block_header = Arc::new(repair_block_header);
        let repair_header =
            zakura_header_chain::Frontier::new(block::Height(1), repair_block_header.hash());
        let selected_tip =
            zakura_header_chain::Frontier::new(block::Height(2), block::Hash([3; 32]));
        let mut snapshot = committed_snapshot(anchor);
        snapshot.frontiers.header_best = selected_tip;
        let (snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let repair_status = zakura_header_chain::VctRootRepairStatus {
            state: zakura_header_chain::VctRootRepairState::Unavailable {
                height: repair_header.height,
            },
            generation: 7,
        };
        let (_repairs_tx, repairs_rx) = watch::channel(repair_status);
        startup.vct_root_repairs = Some(repairs_rx);
        let (handle, mut actions, reactor) =
            spawn_header_sync_reactor(startup).expect("the repair fixture starts");
        let query = next_action(&mut actions).await;
        let HeaderSyncAction::QueryVctRepairContext { owner, height } = query else {
            panic!("the exact repair context query precedes ordinary maintenance");
        };
        assert_eq!(height, repair_header.height);
        assert_eq!(
            owner.scope(),
            zakura_header_chain::WorkScope::for_body_work(&snapshot)
        );

        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the repair supplier connects");
        let _status = outbound.recv().await.expect("the local status is sent");
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::Status(Status {
                    work_anchor_height: anchor.height,
                    work_anchor_hash: anchor.hash,
                    selected_tip_height: selected_tip.height,
                    selected_tip_hash: selected_tip.hash,
                    suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
                    oldest_retained_height: anchor.height,
                    max_headers_per_response: 1,
                    max_inflight_requests: 1,
                    max_message_bytes: 2_000_000,
                    tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
                }),
            })
            .await
            .expect("the repair supplier status reaches the reactor");
        handle
            .send(HeaderSyncEvent::VctRepairContextReady {
                owner,
                result: VctRepairContextResult::Resolved(zakura_header_chain::VctRepairContext {
                    target: repair_header,
                    locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
                }),
            })
            .await
            .expect("the exact repair context reaches the reactor");

        let request = outbound.recv().await.expect("the repair request is sent");
        let HeaderSyncMessage::GetHeaders(request) = handle
            .codec()
            .decode_frame(request, None)
            .expect("the canonical repair request decodes")
        else {
            panic!("the repair uses the canonical GetHeaders message");
        };
        assert_ne!(request.request_id, 0);
        assert_eq!(request.target_tip_hash, repair_header.hash);
        assert_eq!(request.locator_hashes, vec![anchor.hash]);
        assert_eq!(request.max_header_count, 1);
        assert_eq!(request.tree_aux_schema, AuxSchema::V1);
        handle
            .send(HeaderSyncEvent::SessionResponse {
                peer: peer.clone(),
                session_id: 0,
                scope: owner.scope(),
                msg: HeaderSyncMessage::Headers(Headers {
                    request_id: request.request_id,
                    target_tip_hash: repair_header.hash,
                    common_ancestor_height: anchor.height,
                    common_ancestor_hash: anchor.hash,
                    complete: true,
                    tree_aux_schema: AuxSchema::V1,
                    entries: vec![HeaderEntry {
                        header: repair_block_header,
                        body_size: 0,
                        tree_aux: Some(TreeAuxRecordV1 {
                            height: repair_header.height,
                            sapling_root: zakura_chain::sapling::tree::Root::default(),
                            orchard_root: zakura_chain::orchard::tree::Root::default(),
                            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
                            sapling_tx_count: 0,
                            orchard_tx_count: 0,
                            ironwood_tx_count: 0,
                            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from(
                                [0; 32],
                            ),
                        }),
                    }],
                }),
            })
            .await
            .expect("the exact repair response reaches the reactor");
        let HeaderSyncAction::PrepareVctRepair {
            source,
            owner: action_owner,
            context,
            entry,
            ..
        } = next_action(&mut actions).await
        else {
            panic!("the exact repair response is prepared off-reactor");
        };
        assert_eq!(action_owner.session_id, 0);
        assert_eq!(context.target, repair_header);
        let fixture_network = Network::new_regtest(Default::default());
        let engine_config = zakura_header_chain::EngineConfig::new(
            zakura_header_chain::EngineMode::Integrated,
            fixture_network.clone(),
            zakura_header_chain::TrustedAnchor {
                frontier: anchor,
                header: regtest_genesis_block().header.clone(),
            },
            zakura_header_chain::CheckpointSet::default(),
        )
        .expect("the fixture anchor is coherent");
        let lease = zakura_header_chain::ValidationLease::new(
            anchor,
            vec![zakura_header_chain::HeaderContextFact {
                frontier: anchor,
                difficulty_threshold: regtest_genesis_block().header.difficulty_threshold,
                time: regtest_genesis_block().header.time,
            }],
            engine_config.trust_anchor_digest(),
        );
        let rules = zakura_header_chain::HeaderRules::for_validation_lease(fixture_network, &lease)
            .expect("the fixture validation lease produces rules");
        let repair_headers = vec![entry.header.clone()];
        let batch = zakura_header_chain::prepare_headers(
            zakura_header_chain::HeaderBatchInput::new(&repair_headers),
            &lease,
            &rules,
            &zakura_header_chain::SystemClock,
        )
        .expect("the fixture repair header prepares");
        let delivery = zakura_header_chain::AuxDelivery {
            delivery_id: zakura_header_chain::EvidenceId::from_digest([0x44; 32]),
            header_hash: repair_header.hash,
            source,
            owner: action_owner,
            body_size: zakura_header_chain::BodySizeHint::Unknown,
            tree_aux: entry.tree_aux,
            authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
        };
        let insert = Box::new(zakura_header_chain::InsertHeaders {
            owner: action_owner,
            source,
            parent_hash: anchor.hash,
            target_tip_hash: repair_header.hash,
            completion: zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                common_ancestor: anchor,
                selected_target: repair_header,
            },
            batch,
            aux: vec![delivery],
        });
        handle
            .send(HeaderSyncEvent::VctRepairPrepared {
                peer: peer.clone(),
                source,
                owner: action_owner,
                result: HeaderTargetPreparationResult::Prepared(insert),
            })
            .await
            .expect("the sealed repair reaches the completion gate");
        let HeaderSyncAction::ApplyVctRepair {
            owner: dispatched_owner,
            ..
        } = next_action(&mut actions).await
        else {
            panic!("the current sealed repair is dispatched to state");
        };
        assert_eq!(dispatched_owner, action_owner);

        let mut after_delivery = snapshot;
        after_delivery.state_version = after_delivery
            .state_version
            .checked_next()
            .expect("the fixture state version can advance");
        snapshots_tx
            .send(Some(after_delivery))
            .expect("the committed metadata-only snapshot is observed");
        time::sleep(std::time::Duration::from_millis(10)).await;
        handle
            .send(HeaderSyncEvent::VctRepairAdmissionReady {
                peer,
                source,
                owner: action_owner,
                result: HeaderTargetAdmissionResult::Applied,
            })
            .await
            .expect("the state acknowledgement follows its published snapshot");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "the same repair generation is not redelivered after its own state-version advance"
        );

        drop(snapshots_tx);
        shutdown.cancel();
        reactor.await.expect("the repair reactor stops cleanly");
    }

    #[tokio::test]
    async fn retired_vct_request_response_has_no_actions_or_peer_score() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let mut repair_block_header = *regtest_genesis_block().header;
        repair_block_header.previous_block_hash = anchor.hash;
        repair_block_header.time += chrono::Duration::seconds(1);
        let repair_block_header = Arc::new(repair_block_header);
        let repair_header =
            zakura_header_chain::Frontier::new(block::Height(1), repair_block_header.hash());
        let selected_tip =
            zakura_header_chain::Frontier::new(block::Height(2), block::Hash([3; 32]));
        let mut snapshot = committed_snapshot(anchor);
        snapshot.frontiers.header_best = selected_tip;
        let (snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (_repairs_tx, repairs_rx) = watch::channel(zakura_header_chain::VctRootRepairStatus {
            state: zakura_header_chain::VctRootRepairState::Unavailable {
                height: repair_header.height,
            },
            generation: 7,
        });
        startup.vct_root_repairs = Some(repairs_rx);
        let (handle, mut actions, reactor) =
            spawn_header_sync_reactor(startup).expect("the late-response fixture starts");
        let HeaderSyncAction::QueryVctRepairContext { owner, .. } = next_action(&mut actions).await
        else {
            panic!("the repair context is queried");
        };

        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the supplier connects");
        let _status = outbound.recv().await.expect("the local status is sent");
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::Status(Status {
                    work_anchor_height: anchor.height,
                    work_anchor_hash: anchor.hash,
                    selected_tip_height: selected_tip.height,
                    selected_tip_hash: selected_tip.hash,
                    suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
                    oldest_retained_height: anchor.height,
                    max_headers_per_response: 1,
                    max_inflight_requests: 1,
                    max_message_bytes: 2_000_000,
                    tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
                }),
            })
            .await
            .expect("the supplier status reaches the reactor");
        handle
            .send(HeaderSyncEvent::VctRepairContextReady {
                owner,
                result: VctRepairContextResult::Resolved(zakura_header_chain::VctRepairContext {
                    target: repair_header,
                    locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
                }),
            })
            .await
            .expect("the exact repair context reaches the reactor");
        let frame = outbound.recv().await.expect("the repair request is sent");
        let HeaderSyncMessage::GetHeaders(request) = handle
            .codec()
            .decode_frame(frame, None)
            .expect("the repair request decodes")
        else {
            panic!("the repair uses GetHeaders");
        };

        snapshot.state_version = snapshot
            .state_version
            .checked_next()
            .expect("the fixture state version can advance");
        snapshots_tx
            .send(Some(snapshot))
            .expect("the replacement snapshot is published");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::QueryVctRepairContext { .. }
        ));
        handle
            .send(HeaderSyncEvent::SessionResponse {
                peer,
                session_id: 0,
                scope: owner.scope(),
                msg: HeaderSyncMessage::Headers(Headers {
                    request_id: request.request_id,
                    target_tip_hash: repair_header.hash,
                    common_ancestor_height: anchor.height,
                    common_ancestor_hash: anchor.hash,
                    complete: true,
                    tree_aux_schema: AuxSchema::V1,
                    entries: vec![HeaderEntry {
                        header: repair_block_header,
                        body_size: 0,
                        tree_aux: Some(TreeAuxRecordV1 {
                            height: repair_header.height,
                            sapling_root: zakura_chain::sapling::tree::Root::default(),
                            orchard_root: zakura_chain::orchard::tree::Root::default(),
                            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
                            sapling_tx_count: 0,
                            orchard_tx_count: 0,
                            ironwood_tx_count: 0,
                            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from(
                                [0; 32],
                            ),
                        }),
                    }],
                }),
            })
            .await
            .expect("the late reserved response reaches the reactor");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "a retired response cannot prepare work or emit peer misbehavior"
        );

        shutdown.cancel();
        reactor
            .await
            .expect("the late-response reactor stops cleanly");
    }

    async fn next_action(actions: &mut mpsc::Receiver<HeaderSyncAction>) -> HeaderSyncAction {
        time::timeout(std::time::Duration::from_secs(1), actions.recv())
            .await
            .expect("the reactor emits the expected action promptly")
            .expect("the reactor action channel stays open")
    }

    #[tokio::test]
    async fn stale_locator_completion_cannot_rebase_onto_a_new_generation() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let initial = committed_snapshot(anchor);
        let (snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, mut actions, task) =
            spawn_header_sync_reactor(startup).expect("the requester fixture starts");

        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the peer connects");
        let _initial_status = outbound.recv().await.expect("initial status is sent");

        let target = block::Hash([0x52; 32]);
        let remote_status = Status {
            work_anchor_height: anchor.height,
            work_anchor_hash: anchor.hash,
            selected_tip_height: block::Height(2),
            selected_tip_hash: target,
            suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
            oldest_retained_height: anchor.height,
            max_headers_per_response: 1,
            max_inflight_requests: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: 0,
        };
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::Status(remote_status.clone()),
            })
            .await
            .expect("the target status reaches the reactor");
        let stale_scope = match next_action(&mut actions).await {
            HeaderSyncAction::QueryHeaderLocator {
                target_tip_hash,
                scope,
                ..
            } if target_tip_hash == target => scope,
            other => panic!("expected locator query for target, got {other:?}"),
        };

        let mut advanced = initial;
        advanced.state_version = advanced
            .state_version
            .checked_next()
            .expect("the fixture state version has a successor");
        advanced.header_generation = advanced
            .header_generation
            .checked_next()
            .expect("the fixture header generation has a successor");
        snapshots_tx
            .send(Some(advanced))
            .expect("the snapshot receiver remains live");

        let fresh_scope = loop {
            handle
                .send(HeaderSyncEvent::SessionWireMessage {
                    peer: peer.clone(),
                    session_id: 0,
                    msg: HeaderSyncMessage::Status(remote_status.clone()),
                })
                .await
                .expect("a refreshed target status reaches the reactor");
            let observed_scope = match next_action(&mut actions).await {
                HeaderSyncAction::QueryHeaderLocator {
                    target_tip_hash,
                    scope,
                    ..
                } if target_tip_hash == target => scope,
                other => panic!("expected refreshed locator query for target, got {other:?}"),
            };
            if observed_scope != stale_scope {
                break observed_scope;
            }
            tokio::task::yield_now().await;
        };

        handle
            .send(HeaderSyncEvent::HeaderLocatorReady {
                peer: peer.clone(),
                session_id: 0,
                target_tip_hash: target,
                scope: stale_scope,
                locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
            })
            .await
            .expect("the delayed locator reaches the reactor");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), outbound.recv())
                .await
                .is_err(),
            "a stale locator cannot send GetHeaders under the new generation"
        );
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "retiring a stale locator has no punishment or follow-on action"
        );

        assert_ne!(fresh_scope, stale_scope);
        handle
            .send(HeaderSyncEvent::HeaderLocatorReady {
                peer,
                session_id: 0,
                target_tip_hash: target,
                scope: fresh_scope,
                locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
            })
            .await
            .expect("the current locator reaches the reactor");
        assert!(matches!(
            handle
                .codec()
                .decode_frame(outbound.recv().await.expect("GetHeaders is sent"), None)
                .expect("GetHeaders decodes"),
            HeaderSyncMessage::GetHeaders(_)
        ));

        shutdown.cancel();
        task.await.expect("the reactor exits cleanly");
    }

    #[tokio::test]
    async fn requester_stages_all_pages_before_one_exact_admission() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let (snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, mut actions, task) =
            spawn_header_sync_reactor(startup).expect("the requester fixture starts");
        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the peer connects");
        let status_frame = outbound.recv().await.expect("initial status is sent");
        assert!(matches!(
            handle
                .codec()
                .decode_frame(status_frame, None)
                .expect("status decodes"),
            HeaderSyncMessage::Status(_)
        ));

        let mut first_header = *regtest_genesis_block().header;
        first_header.previous_block_hash = anchor.hash;
        first_header.time += chrono::Duration::seconds(1);
        let first_header = Arc::new(first_header);
        let first = zakura_header_chain::Frontier::new(block::Height(1), first_header.hash());
        let mut second_header = *regtest_genesis_block().header;
        second_header.previous_block_hash = first.hash;
        second_header.time += chrono::Duration::seconds(2);
        let second_header = Arc::new(second_header);
        let target = zakura_header_chain::Frontier::new(block::Height(2), second_header.hash());
        let remote_status = Status {
            work_anchor_height: anchor.height,
            work_anchor_hash: anchor.hash,
            selected_tip_height: target.height,
            selected_tip_hash: target.hash,
            suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
            oldest_retained_height: anchor.height,
            max_headers_per_response: 1,
            max_inflight_requests: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: 0,
        };
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::Status(remote_status.clone()),
            })
            .await
            .expect("the target status reaches the reactor");
        let scope = match next_action(&mut actions).await {
            HeaderSyncAction::QueryHeaderLocator {
                target_tip_hash,
                scope,
                ..
            } if target_tip_hash == target.hash => scope,
            other => panic!("expected locator query for target, got {other:?}"),
        };
        handle
            .send(HeaderSyncEvent::HeaderLocatorReady {
                peer: peer.clone(),
                session_id: 0,
                target_tip_hash: target.hash,
                scope,
                locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
            })
            .await
            .expect("the locator reaches the reactor");
        let first_request = match handle
            .codec()
            .decode_frame(outbound.recv().await.expect("first request is sent"), None)
            .expect("first request decodes")
        {
            HeaderSyncMessage::GetHeaders(request) => request,
            other => panic!("expected GetHeaders, got {other:?}"),
        };
        handle
            .send(HeaderSyncEvent::SessionResponse {
                peer: peer.clone(),
                session_id: 0,
                scope,
                msg: HeaderSyncMessage::Headers(Headers {
                    request_id: first_request.request_id,
                    target_tip_hash: target.hash,
                    common_ancestor_height: anchor.height,
                    common_ancestor_hash: anchor.hash,
                    complete: false,
                    tree_aux_schema: AuxSchema::None,
                    entries: vec![HeaderEntry {
                        header: first_header.clone(),
                        body_size: 0,
                        tree_aux: None,
                    }],
                }),
            })
            .await
            .expect("the first response page reaches the reactor");
        let continuation = match handle
            .codec()
            .decode_frame(outbound.recv().await.expect("continuation is sent"), None)
            .expect("continuation decodes")
        {
            HeaderSyncMessage::GetHeaders(request) => request,
            other => panic!("expected continuation GetHeaders, got {other:?}"),
        };
        assert_eq!(continuation.locator_hashes, vec![first.hash]);
        handle
            .send(HeaderSyncEvent::SessionResponse {
                peer: peer.clone(),
                session_id: 0,
                scope,
                msg: HeaderSyncMessage::Headers(Headers {
                    request_id: continuation.request_id,
                    target_tip_hash: target.hash,
                    common_ancestor_height: first.height,
                    common_ancestor_hash: first.hash,
                    complete: true,
                    tree_aux_schema: AuxSchema::None,
                    entries: vec![HeaderEntry {
                        header: second_header,
                        body_size: 0,
                        tree_aux: None,
                    }],
                }),
            })
            .await
            .expect("the completion page reaches the reactor");
        let HeaderSyncAction::PrepareHeaderTarget {
            source,
            network,
            owner,
            common_ancestor,
            target: admitted_target,
            entries,
            ..
        } = next_action(&mut actions).await
        else {
            panic!("the complete target must produce one admission action");
        };
        assert_eq!(common_ancestor, anchor);
        assert_eq!(admitted_target, target);
        assert_eq!(entries.len(), 2);
        assert_eq!(owner.request_id.get(), first_request.request_id);
        let anchor_header = regtest_genesis_block().header.clone();
        let lease = zakura_header_chain::ValidationLease::new(
            anchor,
            vec![zakura_header_chain::HeaderContextFact {
                frontier: anchor,
                difficulty_threshold: anchor_header.difficulty_threshold,
                time: anchor_header.time,
            }],
            [9; 32],
        );
        let rules = zakura_header_chain::HeaderRules::for_validation_lease(network, &lease)
            .expect("the authenticated regtest policy is valid");
        let headers: Vec<_> = entries.iter().map(|entry| entry.header.clone()).collect();
        let batch = zakura_header_chain::prepare_headers(
            zakura_header_chain::HeaderBatchInput::new(&headers),
            &lease,
            &rules,
            &zakura_header_chain::SystemClock,
        )
        .expect("the requester fixture headers prepare");
        let insert = zakura_header_chain::InsertHeaders {
            owner,
            source,
            parent_hash: anchor.hash,
            target_tip_hash: target.hash,
            completion: zakura_header_chain::TargetCompletion::TargetComplete {
                common_ancestor: anchor,
            },
            batch,
            aux: Vec::new(),
        };
        let mut stale_owner = owner;
        stale_owner.session_id = stale_owner.session_id.saturating_add(1);
        let mut stale_insert = insert.clone();
        stale_insert.owner = stale_owner;
        handle
            .send(HeaderSyncEvent::HeaderTargetPrepared {
                peer: peer.clone(),
                source,
                owner: stale_owner,
                result: HeaderTargetPreparationResult::Prepared(Box::new(stale_insert)),
            })
            .await
            .expect("the stale preparation reaches the completion gate");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "a stale preparation has no state-call or peer-score action"
        );
        let mut mismatched_insert = insert.clone();
        mismatched_insert.source = zakura_header_chain::SourceId::from_digest([7; 32]);
        handle
            .send(HeaderSyncEvent::HeaderTargetPrepared {
                peer: peer.clone(),
                source,
                owner,
                result: HeaderTargetPreparationResult::Prepared(Box::new(mismatched_insert)),
            })
            .await
            .expect("the contradictory sealed evidence reaches the reactor");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "contradictory sealed evidence has no state-call or peer-score action"
        );
        handle
            .send(HeaderSyncEvent::HeaderTargetPrepared {
                peer: peer.clone(),
                source,
                owner,
                result: HeaderTargetPreparationResult::Prepared(Box::new(insert.clone())),
            })
            .await
            .expect("the preparation result reaches the reactor");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::ApplyHeaderTarget {
                owner: actual_owner,
                insert: actual_insert,
                ..
            } if actual_owner == owner && *actual_insert == insert
        ));
        handle
            .send(HeaderSyncEvent::HeaderTargetPrepared {
                peer: peer.clone(),
                source,
                owner,
                result: HeaderTargetPreparationResult::Prepared(Box::new(insert.clone())),
            })
            .await
            .expect("the duplicate preparation reaches the reactor");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "a duplicate preparation cannot submit a second state call"
        );
        handle
            .send(HeaderSyncEvent::HeaderTargetAdmissionReady {
                peer: peer.clone(),
                source: zakura_header_chain::SourceId::from_digest([8; 32]),
                owner,
                result: HeaderTargetAdmissionResult::Failed(invalid_header_failure(source, owner)),
            })
            .await
            .expect("the wrong-source state result reaches the reactor");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "a wrong-source state result cannot score or retire current work"
        );
        handle
            .send(HeaderSyncEvent::HeaderTargetAdmissionReady {
                peer: peer.clone(),
                source,
                owner,
                result: HeaderTargetAdmissionResult::Applied,
            })
            .await
            .expect("the admission result reaches the reactor");
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer,
                session_id: 0,
                msg: HeaderSyncMessage::Status(remote_status),
            })
            .await
            .expect("the duplicate covered target reaches the reactor");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "exact current coverage suppresses a duplicate locator query"
        );

        drop(snapshots_tx);
        shutdown.cancel();
        task.await.expect("the reactor exits cleanly");
    }

    #[tokio::test]
    async fn explicit_outcomes_are_nonpunitive_and_reschedule_after_status_refresh() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, mut actions, task) =
            spawn_header_sync_reactor(startup).expect("the requester fixture starts");
        let (send, mut outbound) = framed_channel(16);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the peer connects");
        let _initial_status = outbound.recv().await.expect("initial status is sent");

        let target = block::Hash([0x42; 32]);
        let remote_status = Status {
            work_anchor_height: anchor.height,
            work_anchor_hash: anchor.hash,
            selected_tip_height: block::Height(2),
            selected_tip_hash: target,
            suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
            oldest_retained_height: anchor.height,
            max_headers_per_response: 1,
            max_inflight_requests: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: 0,
        };

        for outcome in [
            HeadersOutcomeCode::TargetNotRetained,
            HeadersOutcomeCode::HistoryPruned,
            HeadersOutcomeCode::Busy,
            HeadersOutcomeCode::NoLocatorIntersection,
        ] {
            handle
                .send(HeaderSyncEvent::SessionWireMessage {
                    peer: peer.clone(),
                    session_id: 0,
                    msg: HeaderSyncMessage::Status(remote_status.clone()),
                })
                .await
                .expect("the refreshed status reaches the reactor");
            let scope = match next_action(&mut actions).await {
                HeaderSyncAction::QueryHeaderLocator {
                    target_tip_hash,
                    scope,
                    ..
                } if target_tip_hash == target => scope,
                other => panic!("expected locator query for target, got {other:?}"),
            };
            handle
                .send(HeaderSyncEvent::HeaderLocatorReady {
                    peer: peer.clone(),
                    session_id: 0,
                    target_tip_hash: target,
                    scope,
                    locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
                })
                .await
                .expect("the locator reaches the reactor");
            let request = match handle
                .codec()
                .decode_frame(outbound.recv().await.expect("the request is sent"), None)
                .expect("the request decodes")
            {
                HeaderSyncMessage::GetHeaders(request) => request,
                other => panic!("expected GetHeaders, got {other:?}"),
            };
            handle
                .send(HeaderSyncEvent::SessionResponse {
                    peer: peer.clone(),
                    session_id: 0,
                    scope,
                    msg: HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                        request_id: request.request_id,
                        target_tip_hash: target,
                        outcome,
                    }),
                })
                .await
                .expect("the explicit outcome reaches the reactor");
        }

        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer,
                session_id: 0,
                msg: HeaderSyncMessage::Status(remote_status),
            })
            .await
            .expect("the next bounded status refresh reaches the reactor");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::QueryHeaderLocator {
                target_tip_hash,
                ..
            } if target_tip_hash == target
        ));

        shutdown.cancel();
        task.await.expect("the reactor exits cleanly");
    }

    #[tokio::test]
    async fn retained_path_pages_keep_one_target_and_release_after_completion() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, mut actions, task) =
            spawn_header_sync_reactor(startup).expect("the fixture starts");
        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the reactor remains available");
        let _initial_status = outbound.recv().await.expect("initial status is sent");

        let mut first_header = *regtest_genesis_block().header;
        let common =
            zakura_header_chain::Frontier::new(block::Height(0), first_header.previous_block_hash);
        first_header.previous_block_hash = common.hash;
        let first_header = Arc::new(first_header);
        let first = first_header.hash();
        let mut second_header = *regtest_genesis_block().header;
        second_header.previous_block_hash = first;
        let second_header = Arc::new(second_header);
        let target = zakura_header_chain::Frontier::new(block::Height(2), second_header.hash());
        let first_request = request(1, target.hash, common.hash);

        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::GetHeaders(first_request.clone()),
            })
            .await
            .expect("the request reaches the reactor");
        let scope = match next_action(&mut actions).await {
            HeaderSyncAction::AcquireHeaderPath {
                request: actual,
                scope,
                ..
            } if actual == first_request => scope,
            other => panic!("expected retained-path acquisition, got {other:?}"),
        };

        let stale_request = request(99, target.hash, common.hash);
        handle
            .send(HeaderSyncEvent::HeaderPathLeaseReady {
                peer: peer.clone(),
                session_id: 0,
                scope,
                request: stale_request,
                result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                    lease_id: 99,
                    common_ancestor: common,
                    target,
                    scope,
                }),
            })
            .await
            .expect("the stale lease result reaches the reactor");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::ReleaseHeaderPath { lease_id: 99, .. }
        ));

        handle
            .send(HeaderSyncEvent::HeaderPathLeaseReady {
                peer: peer.clone(),
                session_id: 0,
                scope,
                request: first_request,
                result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                    lease_id: 9,
                    common_ancestor: common,
                    target,
                    scope,
                }),
            })
            .await
            .expect("the lease result reaches the reactor");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::ReadHeaderPath {
                lease_id: 9,
                request_id,
                after_hash,
                max_header_count: 1,
                ..
            } if request_id.get() == 1 && after_hash == common.hash
        ));

        handle
            .send(HeaderSyncEvent::HeaderPathPageReady {
                peer: peer.clone(),
                session_id: 0,
                scope,
                request_id: HeaderSyncRequestId::new(99).expect("99 is nonzero"),
                target_tip_hash: target.hash,
                result: HeaderPathPageResult::Unavailable,
            })
            .await
            .expect("the stale page result reaches the reactor");

        handle
            .send(HeaderSyncEvent::HeaderPathPageReady {
                peer: peer.clone(),
                session_id: 0,
                scope,
                request_id: HeaderSyncRequestId::new(1).expect("one is nonzero"),
                target_tip_hash: target.hash,
                result: HeaderPathPageResult::Page(Box::new(HeaderPathPage {
                    lease_id: 9,
                    common_ancestor: common,
                    target,
                    scope,
                    entries: vec![HeaderEntry {
                        header: first_header,
                        body_size: 0,
                        tree_aux: None,
                    }],
                    complete: false,
                })),
            })
            .await
            .expect("the first page reaches the reactor");
        let first_frame = outbound.recv().await.expect("the first page is queued");
        let first_response = handle
            .codec()
            .decode_frame(
                first_frame,
                Some(HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::V1,
                }),
            )
            .expect("schema-zero fallback decodes");
        assert!(matches!(
            first_response,
            HeaderSyncMessage::Headers(Headers {
                request_id: 1,
                target_tip_hash,
                common_ancestor_hash,
                complete: false,
                tree_aux_schema: AuxSchema::None,
                ..
            }) if target_tip_hash == target.hash && common_ancestor_hash == common.hash
        ));

        let continuation = request(2, target.hash, first);
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::GetHeaders(continuation),
            })
            .await
            .expect("the continuation reaches the reactor");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::ReadHeaderPath {
                lease_id: 9,
                request_id,
                after_hash,
                ..
            } if request_id.get() == 2 && after_hash == first
        ));

        let continuation_ancestor = zakura_header_chain::Frontier::new(block::Height(1), first);
        handle
            .send(HeaderSyncEvent::HeaderPathPageReady {
                peer: peer.clone(),
                session_id: 0,
                scope,
                request_id: HeaderSyncRequestId::new(2).expect("two is nonzero"),
                target_tip_hash: target.hash,
                result: HeaderPathPageResult::Page(Box::new(HeaderPathPage {
                    lease_id: 9,
                    common_ancestor: continuation_ancestor,
                    target,
                    scope,
                    entries: vec![HeaderEntry {
                        header: second_header,
                        body_size: 0,
                        tree_aux: None,
                    }],
                    complete: true,
                })),
            })
            .await
            .expect("the completion reaches the reactor");
        let completion_frame = outbound.recv().await.expect("the completion is queued");
        let completion = handle
            .codec()
            .decode_frame(
                completion_frame,
                Some(HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::V1,
                }),
            )
            .expect("the completion decodes");
        assert!(matches!(
            completion,
            HeaderSyncMessage::Headers(Headers {
                request_id: 2,
                target_tip_hash,
                common_ancestor_hash,
                complete: true,
                ..
            }) if target_tip_hash == target.hash && common_ancestor_hash == first
        ));
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::ReleaseHeaderPath { lease_id: 9, .. }
        ));

        shutdown.cancel();
        task.await.expect("the reactor exits cleanly");
    }

    #[tokio::test]
    async fn generation_change_retires_served_path_before_late_page_completion() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let initial = committed_snapshot(anchor);
        let (snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, mut actions, task) =
            spawn_header_sync_reactor(startup).expect("the serving fixture starts");

        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the peer connects");
        let _initial_status = outbound.recv().await.expect("initial status is sent");

        let target = zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x61; 32]));
        let request = request(1, target.hash, anchor.hash);
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::GetHeaders(request.clone()),
            })
            .await
            .expect("the request reaches the reactor");
        let scope = match next_action(&mut actions).await {
            HeaderSyncAction::AcquireHeaderPath {
                request: actual,
                scope,
                ..
            } if actual == request => scope,
            other => panic!("expected retained-path acquisition, got {other:?}"),
        };
        handle
            .send(HeaderSyncEvent::HeaderPathLeaseReady {
                peer: peer.clone(),
                session_id: 0,
                scope,
                request,
                result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                    lease_id: 17,
                    common_ancestor: anchor,
                    target,
                    scope,
                }),
            })
            .await
            .expect("the lease reaches the reactor");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::ReadHeaderPath {
                lease_id: 17,
                scope: action_scope,
                ..
            } if action_scope == scope
        ));

        let mut advanced = initial;
        advanced.state_version = advanced
            .state_version
            .checked_next()
            .expect("the fixture state version has a successor");
        advanced.header_generation = advanced
            .header_generation
            .checked_next()
            .expect("the fixture header generation has a successor");
        snapshots_tx
            .send(Some(advanced))
            .expect("the snapshot receiver remains live");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::ReleaseHeaderPath {
                lease_id: 17,
                scope: action_scope,
                ..
            } if action_scope == scope
        ));

        handle
            .send(HeaderSyncEvent::HeaderPathPageReady {
                peer,
                session_id: 0,
                scope,
                request_id: HeaderSyncRequestId::new(1).expect("one is nonzero"),
                target_tip_hash: target.hash,
                result: HeaderPathPageResult::Page(Box::new(HeaderPathPage {
                    lease_id: 17,
                    common_ancestor: anchor,
                    target,
                    scope,
                    entries: Vec::new(),
                    complete: true,
                })),
            })
            .await
            .expect("the late page reaches the reactor");
        assert!(
            time::timeout(std::time::Duration::from_millis(20), outbound.recv())
                .await
                .is_err(),
            "a retired page cannot produce a wire response"
        );
        assert!(
            time::timeout(std::time::Duration::from_millis(20), actions.recv())
                .await
                .is_err(),
            "a retired page has no release, punishment, or follow-on action"
        );

        shutdown.cancel();
        task.await.expect("the reactor exits cleanly");
    }

    #[tokio::test]
    async fn every_unservable_path_result_is_a_correlated_explicit_outcome() {
        let shutdown = CancellationToken::new();
        let mut startup = startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, mut actions, task) =
            spawn_header_sync_reactor(startup).expect("the fixture starts");
        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the reactor remains available");
        let _initial_status = outbound.recv().await.expect("initial status is sent");

        for (offset, outcome) in [
            HeadersOutcomeCode::TargetNotRetained,
            HeadersOutcomeCode::NoLocatorIntersection,
            HeadersOutcomeCode::HistoryPruned,
            HeadersOutcomeCode::Busy,
        ]
        .into_iter()
        .enumerate()
        {
            let request_id = u64::try_from(offset + 1).expect("the fixture IDs fit in u64");
            let target = block::Hash([u8::try_from(offset + 1).expect("small marker"); 32]);
            let request = request(request_id, target, block::Hash([0x41; 32]));
            handle
                .send(HeaderSyncEvent::SessionWireMessage {
                    peer: peer.clone(),
                    session_id: 0,
                    msg: HeaderSyncMessage::GetHeaders(request.clone()),
                })
                .await
                .expect("the request reaches the reactor");
            let scope = match next_action(&mut actions).await {
                HeaderSyncAction::AcquireHeaderPath {
                    request: actual,
                    scope,
                    ..
                } if actual == request => scope,
                other => panic!("expected retained-path acquisition, got {other:?}"),
            };
            handle
                .send(HeaderSyncEvent::HeaderPathLeaseReady {
                    peer: peer.clone(),
                    session_id: 0,
                    scope,
                    request,
                    result: HeaderPathLeaseResult::Outcome(outcome),
                })
                .await
                .expect("the state outcome reaches the reactor");
            let frame = outbound.recv().await.expect("the outcome is queued");
            assert_eq!(
                handle
                    .codec()
                    .decode_frame(frame, None)
                    .expect("the outcome decodes"),
                HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                    request_id,
                    target_tip_hash: target,
                    outcome,
                })
            );
        }

        shutdown.cancel();
        task.await.expect("the reactor exits cleanly");
    }
}
