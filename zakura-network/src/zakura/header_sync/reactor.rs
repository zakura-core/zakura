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
        status::StatusScheduler,
        target::{StageTargetResult, TargetPriority, TargetPursuitRegistry},
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
    let serve_capabilities = ServeCapabilities::new(
        startup.config.advertised_max_headers_per_response(),
        startup.config.advertised_max_inflight_requests(),
        max_message_bytes,
        AuxSchema::V1.mask_bit(),
    )
    .expect("clamped header-sync serving capabilities are nonzero");
    let codec = HeaderSyncCodec::new(
        startup.network.clone(),
        max_message_bytes,
        serve_capabilities.max_headers_per_response(),
        serve_capabilities.tree_aux_schema_mask(),
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
        serve_capabilities,
        committed_snapshot,
        peer_state: HashMap::new(),
        target_pursuits: TargetPursuitRegistry::default(),
    };
    Ok((handle, actions_rx, tokio::spawn(reactor.run())))
}

#[derive(Debug)]
struct PeerState {
    session: HeaderSyncPeerSession,
    status: Option<StatusScheduler>,
    last_received_status_at: Option<Instant>,
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
    serve_capabilities: ServeCapabilities,
    committed_snapshot: Option<zakura_header_chain::EngineSnapshot>,
    peer_state: HashMap<ZakuraPeerId, PeerState>,
    target_pursuits: TargetPursuitRegistry,
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

        let status = self.committed_snapshot.as_ref().map(|snapshot| {
            StatusScheduler::new(
                Status::from_snapshot(snapshot, &self.serve_capabilities),
                self.startup.status_refresh_interval,
                Instant::now(),
            )
        });
        if let Some(previous) = self.peer_state.insert(
            peer.clone(),
            PeerState {
                session,
                status,
                last_received_status_at: None,
            },
        ) {
            previous.session.cancel_token().cancel();
            self.target_pursuits.remove(&peer);
        }
        self.publish_peer_state();
        self.send_status(&peer);
    }

    fn handle_peer_disconnected(&mut self, peer: &ZakuraPeerId) {
        self.peer_state.remove(peer);
        self.target_pursuits.remove(peer);
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
            // Request serving and response admission are added on top of this
            // single-protocol surface; no predecessor dispatcher exists.
            return;
        };
        metrics::counter!("sync.header.peer.status.received").increment(1);
        if status.work_anchor_height > status.selected_tip_height {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage);
            return;
        }

        let advertisement = PeerTargetAdvertisement {
            session_id,
            observed_at: Instant::now(),
            status: status.clone(),
        };
        let Some(local) = self.committed_snapshot.as_ref() else {
            return;
        };
        let work_order = advertisement.claimed_work_order(local);
        let eligible = advertisement.is_discovery_eligible(local);
        if let Some(state) = self.peer_state.get_mut(&peer) {
            state.last_received_status_at = Some(Instant::now());
        }
        if !eligible {
            self.target_pursuits.remove_unstarted(&peer);
            return;
        }
        match self.target_pursuits.stage(
            peer.clone(),
            advertisement,
            TargetPriority::from_work_order(work_order),
        ) {
            StageTargetResult::QueryLocator => {
                if !self.dispatch_action(HeaderSyncAction::QueryHeaderLocator {
                    peer: peer.clone(),
                    session_id,
                    target_tip_hash: status.selected_tip_hash,
                }) {
                    self.target_pursuits.remove_unstarted(&peer);
                }
            }
            StageTargetResult::Active => {
                metrics::counter!("sync.header.target.already_active").increment(1);
            }
            StageTargetResult::AtCapacity => {
                metrics::counter!("sync.header.target.capacity_refused").increment(1);
            }
        }
    }

    fn handle_header_locator_ready(
        &mut self,
        peer: ZakuraPeerId,
        session_id: u64,
        target_tip_hash: block::Hash,
        locator: Option<zakura_header_chain::HeaderLocator>,
    ) {
        let Some(advertisement) = self
            .target_pursuits
            .awaiting(&peer, session_id, target_tip_hash)
            .cloned()
        else {
            metrics::counter!("sync.header.target.stale_locator").increment(1);
            return;
        };
        let Some(locator) = locator else {
            self.target_pursuits.remove_unstarted(&peer);
            metrics::counter!("sync.header.target.locator_unavailable").increment(1);
            return;
        };
        let Some(session) = self
            .peer_state
            .get(&peer)
            .map(|state| state.session.clone())
        else {
            self.target_pursuits.remove_unstarted(&peer);
            return;
        };

        let tree_aux_schema = if advertisement.status.tree_aux_schema_mask
            & self.serve_capabilities.tree_aux_schema_mask()
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
            advertisement
                .status
                .max_message_bytes
                .saturating_sub(response_overhead),
        )
        .unwrap_or(usize::MAX)
        .checked_div(per_header)
        .and_then(|count| u32::try_from(count).ok())
        .unwrap_or(0);
        let max_header_count = advertisement
            .status
            .max_headers_per_response
            .min(self.serve_capabilities.max_headers_per_response())
            .min(byte_limited_count)
            .min(MAX_HS_RANGE);
        if max_header_count == 0 {
            self.target_pursuits.remove_unstarted(&peer);
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
                let started = self.target_pursuits.start(TargetPursuit {
                    peer,
                    advertised: advertisement,
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
                self.target_pursuits.remove_unstarted(&peer);
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
        let status = Status::from_snapshot(&snapshot, &self.serve_capabilities);
        let now = Instant::now();
        self.committed_snapshot = Some(snapshot);
        for state in self.peer_state.values_mut() {
            match state.status.as_mut() {
                Some(scheduler) => scheduler.observe(status.clone(), now),
                None => {
                    state.status = Some(StatusScheduler::new(
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
            self.startup.frontiers = HeaderSyncFrontiers {
                finalized_height: update.frontier.finalized.height,
                verified_block_tip: update.frontier.verified_body.height,
                verified_block_hash: update.frontier.verified_body.hash,
            };
        }
    }

    fn send_status(&mut self, peer: &ZakuraPeerId) -> bool {
        let now = Instant::now();
        let Some((session, status)) = self.peer_state.get(peer).and_then(|state| {
            let scheduler = state.status.as_ref()?;
            scheduler
                .due(now)
                .then(|| (state.session.clone(), scheduler.desired()))
        }) else {
            return false;
        };
        match session.try_send_status(&self.codec, status.clone()) {
            Ok(()) => {
                if let Some(scheduler) = self
                    .peer_state
                    .get_mut(peer)
                    .and_then(|state| state.status.as_mut())
                {
                    scheduler.record_sent(status, now);
                }
                metrics::counter!("sync.header.peer.status.sent").increment(1);
                true
            }
            Err(error) => {
                if let Some(scheduler) = self
                    .peer_state
                    .get_mut(peer)
                    .and_then(|state| state.status.as_mut())
                {
                    scheduler.record_failed(now);
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
            .filter(|(_, state)| state.status.as_ref().is_some_and(|status| status.due(now)))
            .map(|(peer, _)| peer.clone())
            .collect();
        for peer in peers {
            self.send_status(&peer);
        }
    }

    fn next_maintenance_deadline(&self) -> Instant {
        self.peer_state
            .values()
            .filter_map(|state| state.status.as_ref().map(StatusScheduler::next_deadline))
            .min()
            .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(60))
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
