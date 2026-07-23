use std::collections::HashMap;

use iroh::NodeId;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{self, Instant},
};
use zakura_chain::block;

use super::{
    scheduler::{
        peer_work::{PeerWorkPriority, PeerWorkQueue, QueueWorkResult},
        status::StatusPublisher,
    },
    *,
};
use crate::zakura::{
    FrontierChange, FrontierUpdate, OrderedSendError, ServicePeerDirection, ServicePeerSnapshot,
    ZakuraHeaderSyncCandidateState, ZakuraPeerId,
};

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
    let committed_snapshot = startup
        .committed_snapshots
        .as_ref()
        .and_then(|snapshots| snapshots.borrow().clone());
    let handle = HeaderSyncHandle {
        events: events_tx,
        lifecycle: lifecycle_tx,
        tip: tip_rx,
        peers: peers_rx,
        candidates: candidates_rx,
        codec: codec.clone(),
    };
    let reactor = HeaderSyncReactor {
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
        peer_state: HashMap::new(),
        peer_work_queue: PeerWorkQueue::default(),
        served_paths: HashMap::new(),
    };
    Ok((handle, actions_rx, tokio::spawn(reactor.run())))
}

#[derive(Debug)]
struct PeerState {
    session: HeaderSyncPeerSession,
    status_publisher: Option<StatusPublisher>,
    last_received_status_at: Option<Instant>,
}

#[derive(Debug)]
enum ServedPathState {
    Acquiring {
        session_id: u64,
        request_id: HeaderSyncRequestId,
        target_tip_hash: block::Hash,
    },
    Active {
        session_id: u64,
        lease_id: u64,
        target: zakura_header_chain::Frontier,
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
    peer_state: HashMap<ZakuraPeerId, PeerState>,
    peer_work_queue: PeerWorkQueue,
    served_paths: HashMap<ZakuraPeerId, ServedPathState>,
}

impl HeaderSyncReactor {
    async fn run(mut self) {
        let mut frontier_updates = self.startup.frontier_updates.clone();
        let mut committed_snapshots = self.startup.committed_snapshots.clone();
        let _ = self
            .actions
            .try_send(HeaderSyncAction::QueryMissingBlockBodies {
                from: next_height(self.startup.frontiers.verified_block_tip),
                limit: DEFAULT_HS_RANGE,
            });

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
                            self.observe_committed_snapshot(snapshot);
                        }
                    } else {
                        committed_snapshots = None;
                    }
                }
                changed = async {
                    match frontier_updates.as_mut() {
                        Some(updates) => updates.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_ok() {
                        if let Some(update) = frontier_updates.as_ref().map(|updates| *updates.borrow()) {
                            self.observe_frontier_update(update);
                        }
                    } else {
                        frontier_updates = None;
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
            HeaderSyncEvent::FullBlockCommitted { .. } => {}
            HeaderSyncEvent::SessionWireMessage {
                peer,
                session_id,
                msg,
            } => self.handle_wire_message(peer, session_id, msg),
            HeaderSyncEvent::StateFrontiersChanged(frontiers) => {
                self.startup.frontiers = frontiers;
            }
            HeaderSyncEvent::HeaderLocatorReady {
                peer,
                session_id,
                target_tip_hash,
                locator,
            } => self.handle_header_locator_ready(peer, session_id, target_tip_hash, locator),
            HeaderSyncEvent::HeaderPathLeaseReady {
                peer,
                session_id,
                request,
                result,
            } => self.handle_header_path_lease_ready(peer, session_id, request, result),
            HeaderSyncEvent::HeaderPathPageReady {
                peer,
                session_id,
                request_id,
                target_tip_hash,
                result,
            } => self.handle_header_path_page_ready(
                peer,
                session_id,
                request_id,
                target_tip_hash,
                result,
            ),
        }
    }

    fn handle_peer_connected(&mut self, session: HeaderSyncPeerSession) {
        let peer = session.peer_id().clone();
        let direction = session.direction();
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
            },
        ) {
            previous.session.cancel_token().cancel();
            self.peer_work_queue.remove(&peer);
            self.release_served_path(&peer);
        }
        self.publish_peer_state();
        self.send_status(&peer);
    }

    fn handle_peer_disconnected(&mut self, peer: &ZakuraPeerId) {
        self.release_served_path(peer);
        self.peer_state.remove(peer);
        self.peer_work_queue.remove(peer);
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
        let HeaderSyncMessage::Status(status) = message else {
            if let HeaderSyncMessage::GetHeaders(request) = message {
                self.handle_get_headers(peer, session_id, request);
            }
            // Response admission is added in PR-11d2. No predecessor dispatcher exists.
            return;
        };
        metrics::counter!("sync.header.peer.status.received").increment(1);
        if status.work_anchor_height > status.selected_tip_height {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        }

        let target = AdvertisedHeaderTarget {
            session_id,
            observed_at: Instant::now(),
            status: status.clone(),
        };
        let Some(local) = self.committed_snapshot.as_ref() else {
            return;
        };
        let work_order = target.claimed_work_order(local);
        let eligible = target.is_discovery_eligible(local);
        if let Some(state) = self.peer_state.get_mut(&peer) {
            state.last_received_status_at = Some(Instant::now());
        }
        if !eligible {
            self.peer_work_queue.remove_unstarted(&peer);
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

        self.served_paths.insert(
            peer.clone(),
            ServedPathState::Acquiring {
                session_id,
                request_id,
                target_tip_hash: request.target_tip_hash,
            },
        );
        if !self.dispatch_action(HeaderSyncAction::AcquireHeaderPath {
            peer: peer.clone(),
            session_id,
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

    fn handle_header_path_lease_ready(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        request: GetHeaders,
        result: HeaderPathLeaseResult,
    ) {
        let request_id = HeaderSyncRequestId::new(request.request_id)
            .expect("state echoes a request accepted by the bounded decoder");
        let Some(state) = self.served_paths.remove(&peer) else {
            if let HeaderPathLeaseResult::Acquired(lease) = result {
                self.release_lease(peer, session_id, lease.lease_id);
            }
            return;
        };
        let ServedPathState::Acquiring {
            session_id: expected_session,
            request_id: expected_request,
            target_tip_hash: expected_target,
        } = state
        else {
            self.served_paths.insert(peer.clone(), state);
            if let HeaderPathLeaseResult::Acquired(lease) = result {
                self.release_lease(peer, session_id, lease.lease_id);
            }
            return;
        };
        if expected_session != session_id
            || expected_request != request_id
            || expected_target != request.target_tip_hash
        {
            self.served_paths.insert(
                peer.clone(),
                ServedPathState::Acquiring {
                    session_id: expected_session,
                    request_id: expected_request,
                    target_tip_hash: expected_target,
                },
            );
            if let HeaderPathLeaseResult::Acquired(lease) = result {
                self.release_lease(peer, session_id, lease.lease_id);
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
                    && request.locator_hashes.contains(&lease.common_ancestor.hash) =>
            {
                lease
            }
            HeaderPathLeaseResult::Acquired(lease) => {
                self.release_lease(peer, session_id, lease.lease_id);
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
                next_after: lease.common_ancestor,
                pending_request: Some((request_id, max_header_count)),
            },
        );
        if !self.dispatch_action(HeaderSyncAction::ReadHeaderPath {
            peer: peer.clone(),
            session_id,
            lease_id: lease.lease_id,
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
            next_after,
            pending_request,
        } = state
        else {
            self.served_paths.insert(peer, state);
            return;
        };
        if expected_session != session_id
            || target.hash != target_tip_hash
            || pending_request.is_none_or(|(pending_id, _)| pending_id != request_id)
        {
            self.served_paths.insert(
                peer,
                ServedPathState::Active {
                    session_id: expected_session,
                    lease_id,
                    target,
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
            self.release_lease(peer, session_id, lease_id);
            return;
        };
        if page.lease_id != lease_id
            || page.target != target
            || page.common_ancestor != next_after
            || pending_request.is_some_and(|(_, max_count)| {
                page.entries.len() > usize::try_from(max_count).unwrap_or(usize::MAX)
            })
        {
            self.release_lease(peer, session_id, lease_id);
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
                self.release_lease(peer, session_id, lease_id);
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
            self.release_lease(peer, session_id, lease_id);
        } else {
            self.served_paths.insert(
                peer,
                ServedPathState::Active {
                    session_id,
                    lease_id,
                    target,
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
        locator: Option<zakura_header_chain::HeaderLocator>,
    ) {
        let Some(target) = self
            .peer_work_queue
            .awaiting(&peer, session_id, target_tip_hash)
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

        match session.try_send_get_headers(
            &self.codec,
            target_tip_hash,
            &locator,
            max_header_count,
            tree_aux_schema,
        ) {
            Ok(request_id) => {
                let started = self.peer_work_queue.start(ActiveHeaderRequest {
                    peer,
                    target,
                    sent_locator: locator,
                    request_id,
                });
                debug_assert!(
                    started,
                    "the matching locator was checked before publication"
                );
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

    fn observe_committed_snapshot(&mut self, snapshot: zakura_header_chain::EngineSnapshot) {
        let old_tip = self
            .committed_snapshot
            .as_ref()
            .map(|old| old.frontiers.header_best);
        let new_tip = snapshot.frontiers.header_best;
        let status = Status::from_snapshot(&snapshot, &self.serving_limits);
        let now = Instant::now();
        self.committed_snapshot = Some(snapshot);
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
            if let Some(old) = old_tip {
                let action = if new_tip.height >= old.height {
                    HeaderSyncAction::HeaderAdvanced {
                        height: new_tip.height,
                        hash: new_tip.hash,
                    }
                } else {
                    HeaderSyncAction::HeaderReanchored {
                        old: (old.height, old.hash),
                        new: (new_tip.height, new_tip.hash),
                    }
                };
                let _ = self.dispatch_action(action);
            }
            self.publish_peer_state();
        }
        self.refresh_statuses();
    }

    fn observe_frontier_update(&mut self, update: FrontierUpdate) {
        if matches!(
            update.change,
            FrontierChange::Snapshot | FrontierChange::VerifiedGrow | FrontierChange::VerifiedReset
        ) {
            self.startup.frontiers = FullStateFrontiers {
                finalized_height: update.frontier.finalized.height,
                verified_block_tip: update.frontier.verified_body.height,
                verified_block_hash: update.frontier.verified_body.hash,
            };
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
            ..
        }) = self.served_paths.remove(peer)
        else {
            return;
        };
        self.release_lease(peer.clone(), session_id, lease_id);
    }

    fn release_lease(&self, peer: ZakuraPeerId, session_id: u64, lease_id: u64) {
        let _ = self.dispatch_action(HeaderSyncAction::ReleaseHeaderPath {
            peer,
            session_id,
            lease_id,
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

    fn report_misbehavior(&self, peer: ZakuraPeerId, reason: HeaderSyncMisbehavior) {
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

    async fn next_action(actions: &mut mpsc::Receiver<HeaderSyncAction>) -> HeaderSyncAction {
        time::timeout(std::time::Duration::from_secs(1), actions.recv())
            .await
            .expect("the reactor emits the expected action promptly")
            .expect("the reactor action channel stays open")
    }

    #[tokio::test]
    async fn retained_path_pages_keep_one_target_and_release_after_completion() {
        let shutdown = CancellationToken::new();
        let (handle, mut actions, task) =
            spawn_header_sync_reactor(startup(shutdown.clone())).expect("the fixture starts");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::QueryMissingBlockBodies { .. }
        ));
        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the reactor remains available");

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
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::AcquireHeaderPath { ref request, .. } if request == &first_request
        ));

        let stale_request = request(99, target.hash, common.hash);
        handle
            .send(HeaderSyncEvent::HeaderPathLeaseReady {
                peer: peer.clone(),
                session_id: 0,
                request: stale_request,
                result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                    lease_id: 99,
                    common_ancestor: common,
                    target,
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
                request: first_request,
                result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                    lease_id: 9,
                    common_ancestor: common,
                    target,
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
                request_id: HeaderSyncRequestId::new(1).expect("one is nonzero"),
                target_tip_hash: target.hash,
                result: HeaderPathPageResult::Page(HeaderPathPage {
                    lease_id: 9,
                    common_ancestor: common,
                    target,
                    entries: vec![HeaderEntry {
                        header: first_header,
                        body_size: 0,
                        tree_aux: None,
                    }],
                    complete: false,
                }),
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
                request_id: HeaderSyncRequestId::new(2).expect("two is nonzero"),
                target_tip_hash: target.hash,
                result: HeaderPathPageResult::Page(HeaderPathPage {
                    lease_id: 9,
                    common_ancestor: continuation_ancestor,
                    target,
                    entries: vec![HeaderEntry {
                        header: second_header,
                        body_size: 0,
                        tree_aux: None,
                    }],
                    complete: true,
                }),
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
    async fn every_unservable_path_result_is_a_correlated_explicit_outcome() {
        let shutdown = CancellationToken::new();
        let (handle, mut actions, task) =
            spawn_header_sync_reactor(startup(shutdown.clone())).expect("the fixture starts");
        assert!(matches!(
            next_action(&mut actions).await,
            HeaderSyncAction::QueryMissingBlockBodies { .. }
        ));
        let (send, mut outbound) = framed_channel(8);
        let peer = peer();
        handle
            .send(HeaderSyncEvent::PeerConnected(
                HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
            ))
            .await
            .expect("the reactor remains available");

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
            assert!(matches!(
                next_action(&mut actions).await,
                HeaderSyncAction::AcquireHeaderPath { request: ref actual, .. }
                    if actual == &request
            ));
            handle
                .send(HeaderSyncEvent::HeaderPathLeaseReady {
                    peer: peer.clone(),
                    session_id: 0,
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
