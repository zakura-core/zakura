use super::super::trace::{
    ordered_send_error_label, queue_send_trace as qs_trace, QUEUE_SEND_TABLE,
};
use super::{config::*, error::*, events::*, scheduler::*, state::*, validation::*, wire::*, *};
use crate::zakura::{
    FrontierChange, FrontierUpdate, HeaderSyncServiceSummary, OrderedSendError,
    ServiceAdmissionDecision, ServicePeerDirection, ServicePeerSnapshot,
    ZakuraHeaderSyncCandidateState,
};

const STALE_REPAIR_GENERATION: &str = "stale_repair_generation";

/// Spawn a header-sync reactor and return its handle plus action stream.
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
    let state = HeaderSyncCore::new(&startup)?;
    let (events_tx, events_rx) = mpsc::channel(128);
    let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
    let (actions_tx, actions_rx) = mpsc::channel(128);
    let (tip_tx, tip_rx) = watch::channel((state.best_header_tip, state.best_header_hash));
    let (peers_tx, peers_rx) =
        watch::channel(ServicePeerSnapshot::new(0, 0, startup.config.peer_limits));
    let (candidates_tx, candidates_rx) = watch::channel(ZakuraHeaderSyncCandidateState {
        target_height: header_sync_candidate_target(state.best_header_tip),
        admitted_node_ids: Vec::new(),
        backed_off_node_ids: Vec::new(),
    });
    let handle = HeaderSyncHandle {
        events: events_tx,
        lifecycle: lifecycle_tx,
        tip: tip_rx,
        peers: peers_rx,
        candidates: candidates_rx,
    };
    let reactor = HeaderSyncReactor {
        startup,
        state,
        events: events_rx,
        lifecycle: lifecycle_rx,
        actions: actions_tx,
        tip: tip_tx,
        peers: peers_tx,
        candidates: candidates_tx,
    };
    let task = tokio::spawn(reactor.run());

    Ok((handle, actions_rx, task))
}

#[derive(Debug)]
pub(super) struct HeaderSyncReactor {
    startup: HeaderSyncStartup,
    state: HeaderSyncCore,
    events: mpsc::Receiver<HeaderSyncEvent>,
    lifecycle: mpsc::UnboundedReceiver<HeaderSyncEvent>,
    actions: mpsc::Sender<HeaderSyncAction>,
    tip: watch::Sender<(block::Height, block::Hash)>,
    peers: watch::Sender<ServicePeerSnapshot>,
    candidates: watch::Sender<ZakuraHeaderSyncCandidateState>,
}

#[derive(Clone, Copy, Debug)]
struct GetHeadersTraceMeta {
    request_id: HeaderSyncRequestId,
    session_id: u64,
    stream_version: u16,
}

impl HeaderSyncReactor {
    async fn run(mut self) {
        let mut frontier_updates = self.startup.frontier_updates.clone();
        let mut frontier_updates_open = frontier_updates.is_some();
        self.publish_connectivity_metrics();
        if self.startup.range_state_actions_enabled {
            let _ = self.dispatch_action(HeaderSyncAction::QueryBestHeaderTip);
            let _ = self.dispatch_action(HeaderSyncAction::QueryMissingBlockBodies {
                from: next_height(self.state.verified_block_tip)
                    .unwrap_or(self.state.verified_block_tip),
                limit: DEFAULT_HS_RANGE,
            });
        }

        let mut ticks = time::interval(self.empty_headers_retry_delay());
        let exit_reason;
        loop {
            // Liveness watermark: a frozen reactor is otherwise invisible (the
            // process, transport, and other services keep running). Exposing the
            // loop count lets an external watcher detect a stall in seconds.
            metrics::counter!("sync.header.reactor.iterations").increment(1);
            tokio::select! {
                biased;
                _ = self.startup.shutdown.cancelled() => {
                    exit_reason = "shutdown";
                    break;
                }
                event = self.lifecycle.recv() => {
                    let Some(event) = event else {
                        exit_reason = "lifecycle_channel_closed";
                        break;
                    };
                    self.handle_event(event).await;
                }
                event = self.events.recv() => {
                    let Some(event) = event else {
                        exit_reason = "events_channel_closed";
                        break;
                    };
                    self.handle_event(event).await;
                }
                changed = async {
                    match frontier_updates.as_mut() {
                        Some(frontier_updates) => frontier_updates.changed().await,
                        None => std::future::pending().await,
                    }
                }, if frontier_updates_open => {
                    match changed {
                        Ok(()) => {
                            let frontier_updates = frontier_updates
                                .as_mut()
                                .expect("frontier update receiver exists while frontier_updates_open is true");
                            let update = *frontier_updates.borrow_and_update();
                            self.handle_frontier_update(update).await;
                        }
                        Err(_) => frontier_updates_open = false,
                    }
                }
                _ = ticks.tick() => {
                    metrics::counter!("sync.header.reactor.event_started", "kind" => "tick").increment(1);
                    self.handle_timeouts().await;
                    self.refresh_statuses();
                    self.publish_connectivity_metrics();
                    metrics::counter!("sync.header.reactor.event_finished", "kind" => "tick").increment(1);
                }
            }
        }
        // A reactor exit is fatal to header sync on this node but the process
        // keeps running, so it must be loud.
        tracing::warn!(exit_reason, "Zakura header-sync reactor exited");
        metrics::counter!("sync.header.reactor.exited", "reason" => exit_reason).increment(1);
    }

    async fn handle_event(&mut self, event: HeaderSyncEvent) {
        self.trace_event_received(&event);
        // Started/finished pairs expose which event kind an await inside
        // `handle_event` is stuck on: after a freeze, exactly one kind shows
        // started == finished + 1.
        let kind = event.metrics_label();
        metrics::counter!("sync.header.reactor.event_started", "kind" => kind).increment(1);
        self.handle_event_inner(event).await;
        metrics::counter!("sync.header.reactor.event_finished", "kind" => kind).increment(1);
    }

    async fn handle_event_inner(&mut self, event: HeaderSyncEvent) {
        match event {
            HeaderSyncEvent::PeerConnected(session) => self.handle_peer_connected(session).await,
            HeaderSyncEvent::PeerDisconnected(peer) => self.handle_peer_disconnected(peer),
            HeaderSyncEvent::AdvisoryHeaderSummary { peer, summary } => {
                self.handle_advisory_header_summary(peer, summary)
            }
            HeaderSyncEvent::FullBlockCommitted {
                height,
                hash,
                header: _,
            } => self.handle_full_block_committed(height, hash).await,
            HeaderSyncEvent::NewBlockAccepted {
                peer,
                height,
                hash,
                block,
            } => {
                self.handle_new_block_accepted(peer, height, hash, block)
                    .await
            }
            HeaderSyncEvent::NewBlockDuplicate { peer, height, hash } => {
                self.handle_new_block_duplicate(peer, height, hash)
            }
            HeaderSyncEvent::NewBlockAcceptedNonBestChain { peer, height, hash } => {
                self.handle_new_block_accepted_non_best_chain(peer, height, hash)
            }
            HeaderSyncEvent::NewBlockRejected { peer, hash } => {
                self.handle_new_block_rejected(peer, hash).await
            }
            HeaderSyncEvent::WireMessage { peer, msg } => {
                self.handle_wire_message(peer, msg).await;
            }
            HeaderSyncEvent::SessionWireMessage {
                peer,
                session_id,
                msg,
            } => {
                if self.is_current_session(&peer, session_id) {
                    self.handle_wire_message(peer, msg).await;
                } else {
                    metrics::counter!("sync.header.session.stale_event").increment(1);
                }
            }
            HeaderSyncEvent::WireHeaders {
                peer,
                session_id,
                request_id,
                headers,
                body_sizes,
                tree_aux_roots,
            } => {
                if self.is_current_session(&peer, session_id) {
                    self.handle_headers(peer, request_id, headers, body_sizes, tree_aux_roots)
                        .await;
                } else {
                    metrics::counter!("sync.header.session.stale_event").increment(1);
                }
            }
            HeaderSyncEvent::WireGetHeaders {
                peer,
                session_id,
                request_id,
                start_height,
                count,
                want_tree_aux_roots,
            } => {
                if self.is_current_session(&peer, session_id) {
                    self.handle_get_headers(
                        peer,
                        request_id,
                        start_height,
                        count,
                        want_tree_aux_roots,
                    )
                    .await;
                } else {
                    metrics::counter!("sync.header.session.stale_event").increment(1);
                }
            }
            HeaderSyncEvent::WireDecodeFailed { peer, error } => {
                self.handle_wire_decode_failed(peer, error).await;
            }
            HeaderSyncEvent::WireProtocolFailure {
                peer,
                reason,
                error,
            } => {
                self.handle_wire_protocol_failure(peer, reason, error).await;
            }
            HeaderSyncEvent::StateFrontiersChanged(frontiers) => {
                self.handle_state_frontiers_changed(frontiers).await;
            }
            HeaderSyncEvent::VctRootRepairRequested {
                height,
                generation,
                anchor_hash,
                expected_hashes,
            } => {
                self.handle_vct_root_repair_requested(
                    height,
                    generation,
                    anchor_hash,
                    expected_hashes,
                )
                .await;
            }
            HeaderSyncEvent::VctRootRepairResolved { generation } => {
                self.handle_vct_root_repair_resolved(generation).await;
            }
            HeaderSyncEvent::HeaderRangeCommitted {
                start_height,
                tip_height,
                tip_hash,
            } => {
                self.handle_header_range_committed(start_height, tip_height, tip_hash)
                    .await
            }
            HeaderSyncEvent::HeaderRangeCommitFailed {
                peer,
                session_id,
                start_height,
                count,
                kind,
            } => {
                if self.is_current_session(&peer, session_id) {
                    self.handle_header_range_commit_failed(peer, start_height, count, kind)
                        .await;
                } else {
                    metrics::counter!("sync.header.session.stale_completion").increment(1);
                }
            }
            HeaderSyncEvent::HeaderRangeResponseFinished {
                peer,
                session_id,
                request_id,
                start_height,
                requested_count,
                returned_count,
            } => {
                if self.is_current_session(&peer, session_id) {
                    self.handle_header_range_response_finished(
                        peer,
                        request_id,
                        start_height,
                        requested_count,
                        returned_count,
                    );
                } else {
                    metrics::counter!("sync.header.session.stale_completion").increment(1);
                }
            }
            HeaderSyncEvent::HeaderRangeResponseReady {
                peer,
                session_id,
                request_id,
                start_height,
                requested_count,
                want_tree_aux_roots,
                headers,
                body_sizes,
                tree_aux_roots,
            } => {
                if self.is_current_session(&peer, session_id) {
                    self.handle_header_range_response_ready(
                        peer,
                        request_id,
                        start_height,
                        requested_count,
                        want_tree_aux_roots,
                        headers,
                        body_sizes,
                        tree_aux_roots,
                    );
                } else {
                    metrics::counter!("sync.header.session.stale_completion").increment(1);
                }
            }
        }
    }

    fn admission_decision_for(
        &self,
        peer: &ZakuraPeerId,
        direction: ServicePeerDirection,
    ) -> ServiceAdmissionDecision {
        if self.state.peers.contains_key(peer) {
            return ServiceAdmissionDecision::Admit;
        }

        let limits = self.startup.config.peer_limits;
        let admitted = self.admitted_count(direction);
        let cap = match direction {
            ServicePeerDirection::Inbound => limits.max_inbound_peers,
            ServicePeerDirection::Outbound => limits.max_outbound_peers,
        };

        if admitted >= cap {
            ServiceAdmissionDecision::RejectFull
        } else {
            ServiceAdmissionDecision::Admit
        }
    }

    fn is_current_session(&self, peer: &ZakuraPeerId, session_id: u64) -> bool {
        self.state
            .peers
            .get(peer)
            .is_some_and(|state| state.session.session_id() == session_id)
    }

    async fn handle_frontier_update(&mut self, update: FrontierUpdate) {
        match update.change {
            FrontierChange::Snapshot
            | FrontierChange::VerifiedGrow
            | FrontierChange::VerifiedReset => {
                let frontier = update.frontier;
                self.handle_state_frontiers_changed(HeaderSyncFrontiers {
                    finalized_height: frontier.finalized.height,
                    verified_block_tip: frontier.verified_body.height,
                    verified_block_hash: frontier.verified_body.hash,
                })
                .await;
            }
            FrontierChange::HeaderAdvanced | FrontierChange::HeaderReanchored => {}
        }
    }

    fn admitted_count(&self, direction: ServicePeerDirection) -> usize {
        self.state
            .peers
            .values()
            .filter(|peer| peer.direction == direction)
            .count()
    }

    fn publish_peer_snapshot(&self) {
        let snapshot = ServicePeerSnapshot::new(
            self.admitted_count(ServicePeerDirection::Inbound),
            self.admitted_count(ServicePeerDirection::Outbound),
            self.startup.config.peer_limits,
        );
        let _ = self.peers.send(snapshot);
    }

    fn publish_connectivity_metrics(&self) {
        set_header_connectivity_gauges(
            self.state.peers.len(),
            self.healthy_peer_count(Instant::now()),
        );
    }

    fn healthy_peer_count(&self, now: Instant) -> usize {
        let freshness = self.startup.status_refresh_interval.saturating_mul(2);
        self.state
            .peers
            .values()
            .filter(|peer| {
                peer.last_received_status_at
                    .is_some_and(|last| now.duration_since(last) <= freshness)
            })
            .count()
    }

    fn publish_candidate_state(&mut self) {
        let now = Instant::now();
        self.state
            .advisory
            .retain(|_, advisory| !advisory.is_expired(now));
        for advisory in self.state.advisory.values_mut() {
            if advisory.backoff_until.is_some_and(|until| until <= now) {
                advisory.record_confirmed();
            }
        }

        let mut admitted_node_ids: Vec<_> = self
            .state
            .peers
            .keys()
            .filter_map(node_id_from_header_peer_id)
            .collect();
        admitted_node_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        admitted_node_ids.dedup();

        let mut backed_off_node_ids: Vec<_> = self
            .state
            .advisory
            .iter()
            .filter_map(|(peer, advisory)| {
                advisory
                    .is_backed_off(now)
                    .then(|| node_id_from_header_peer_id(peer))
                    .flatten()
            })
            .collect();
        backed_off_node_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        backed_off_node_ids.dedup();

        let _ = self.candidates.send(ZakuraHeaderSyncCandidateState {
            target_height: header_sync_candidate_target(self.state.best_header_tip),
            admitted_node_ids,
            backed_off_node_ids,
        });
    }

    fn handle_advisory_header_summary(
        &mut self,
        peer: ZakuraPeerId,
        summary: HeaderSyncServiceSummary,
    ) {
        if self.state.peers.contains_key(&peer) {
            return;
        }
        if !header_summary_is_useful(
            summary,
            header_sync_candidate_target(self.state.best_header_tip),
        ) {
            self.state.advisory.remove(&peer);
            self.publish_candidate_state();
            return;
        }

        self.state
            .advisory
            .entry(peer)
            .and_modify(|advisory| advisory.refresh_summary(summary, Instant::now()))
            .or_insert_with(|| HeaderSyncAdvisoryPeerState::new(summary, Instant::now()));
        self.publish_candidate_state();
    }

    fn confirm_advisory_status(&mut self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        let Some(summary) = self
            .state
            .advisory
            .get(peer)
            .map(|advisory| advisory.summary)
        else {
            return;
        };

        if status.tip_height >= summary.best_height {
            self.state.advisory.remove(peer);
        } else if let Some(advisory) = self.state.advisory.get_mut(peer) {
            advisory.record_unconfirmed(Instant::now());
        }
        self.publish_candidate_state();
    }

    fn record_advisory_unconfirmed(&mut self, peer: &ZakuraPeerId) {
        let Some(advisory) = self.state.advisory.get_mut(peer) else {
            return;
        };
        advisory.record_unconfirmed(Instant::now());
        self.publish_candidate_state();
    }

    async fn handle_peer_connected(&mut self, session: HeaderSyncPeerSession) {
        let peer = session.peer_id().clone();
        let direction = session.direction();
        if self
            .state
            .peers
            .get(&peer)
            .is_some_and(|state| state.session.session_id() > session.session_id())
        {
            metrics::counter!("sync.header.session.stale_connect").increment(1);
            session.cancel_token().cancel();
            return;
        }
        let decision = self.admission_decision_for(&peer, direction);
        if decision != ServiceAdmissionDecision::Admit {
            // A parked peer stays connected but never receives a status, which
            // from its side is indistinguishable from a wedged remote. Keep
            // this visible at default log levels and in metrics.
            metrics::counter!("sync.header.peer.parked").increment(1);
            tracing::info!(
                ?peer,
                ?direction,
                ?decision,
                "locally parking Zakura header-sync service session"
            );
            self.state.parked_peers.insert(peer);
            session.cancel_token().cancel();
            self.publish_peer_snapshot();
            self.publish_candidate_state();
            return;
        }

        self.state.parked_peers.remove(&peer);
        self.state.schedule.forget_peer(&peer);
        self.state
            .pending_commits
            .retain(|key, _range| key.peer != peer);
        let status_refresh_interval = self.startup.status_refresh_interval;
        self.state
            .peers
            .entry(peer.clone())
            .and_modify(|peer_state| {
                peer_state.session.cancel_token().cancel();
                peer_state.session = session.clone();
                peer_state.direction = direction;
                // A new transport replaces the old one; its remote has received
                // no status yet, so the initial status below must always be sent.
                // Outstanding requests and inbound serving counts are also
                // session-local: responses for the old stream cannot satisfy
                // work sent on this fresh stream.
                peer_state.received_status = false;
                peer_state.last_received_status_at = None;
                peer_state.reset_sent_status();
                peer_state.outstanding.clear();
                peer_state.served_headers_inflight = 0;
                peer_state.served_header_request_ids.clear();
                peer_state.highest_served_header_request_id = None;
                peer_state.meters = HeaderSyncPeerMeters::new(
                    status_refresh_interval,
                    DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
                    DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
                );
            })
            .or_insert_with(|| {
                PeerHeaderState::new(
                    session,
                    self.state.anchor,
                    self.startup.config.advertised_max_headers_per_response(),
                    self.startup.config.advertised_max_inflight_requests(),
                    self.startup.status_refresh_interval,
                    DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL,
                    DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
                )
            });
        self.publish_connectivity_metrics();
        self.trace_peer_connected(&peer, direction, self.state.peers.len());
        self.publish_peer_snapshot();
        self.publish_candidate_state();
        self.send_status(&peer);
        self.schedule().await;
    }

    fn handle_peer_disconnected(&mut self, peer: ZakuraPeerId) {
        let was_connected = self.state.peers.remove(&peer).is_some();
        self.state.parked_peers.remove(&peer);
        self.state.advisory.remove(&peer);
        self.state.schedule.forget_peer(&peer);
        self.finish_vct_repair_attempt(&peer);
        if was_connected {
            self.publish_connectivity_metrics();
            self.trace_peer_disconnected(&peer, self.state.peers.len());
        }
        self.publish_peer_snapshot();
        self.publish_candidate_state();
    }

    async fn handle_full_block_committed(&mut self, height: block::Height, hash: block::Hash) {
        self.state.pending_new_blocks.remove(&hash);
        let _ = self.state.seen.insert(hash);
        self.update_verified_block_tip(height, hash);
        self.state.schedule.mark_height_covered(height);
        self.cancel_covered_outstanding();
        if height > self.state.best_header_tip {
            self.publish_best_tip(height, hash).await;
        }
        self.schedule().await;
    }

    async fn handle_new_block_accepted(
        &mut self,
        peer: ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
    ) {
        self.state.pending_new_blocks.remove(&hash);
        let inserted = self.state.seen.insert(hash);
        if !inserted {
            metrics::counter!("sync.header.tip.new_block.deduped").increment(1);
            self.trace_new_block_deduped(&peer, height, hash, "seen_cache");
            return;
        }

        self.update_verified_block_tip(height, hash);
        self.state.schedule.mark_height_covered(height);
        self.cancel_covered_outstanding();
        if height > self.state.best_header_tip {
            self.publish_best_tip(height, hash).await;
        }

        let destinations = self.eligible_tip_destinations(&peer, height);
        let destination_count = destinations.len();
        for destination in destinations {
            let Some(destination_peer) = self.state.peers.get(&destination) else {
                continue;
            };
            if let Err(error) = destination_peer.session.try_send_new_block(block.clone()) {
                tracing::debug!(
                    ?peer,
                    ?destination,
                    ?height,
                    ?hash,
                    ?error,
                    "failed to queue Zakura header-sync NewBlock"
                );
                self.trace_queue_send_failed(
                    &destination,
                    "new_block",
                    &error,
                    destination_peer.session.outbound_capacity(),
                    destination_peer.session.outbound_max_capacity(),
                    |row| {
                        insert_peer(row, qs_trace::SOURCE_PEER, &peer);
                        insert_peer(row, qs_trace::DESTINATION_PEER, &destination);
                        insert_height(row, qs_trace::HEIGHT, height);
                        insert_hash(row, qs_trace::HASH, hash);
                    },
                );
                continue;
            }
            metrics::counter!("sync.header.tip.new_block.forwarded").increment(1);
            self.trace_new_block_forwarded(&peer, &destination, height, hash, destination_count);
            #[cfg(test)]
            let _ = self
                .actions
                .send(HeaderSyncAction::ForwardNewBlock {
                    source: Some(peer.clone()),
                    peer: destination,
                    height,
                    hash,
                    block: block.clone(),
                })
                .await;
        }
        self.schedule().await;
    }

    fn handle_new_block_duplicate(
        &mut self,
        peer: ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    ) {
        self.state.pending_new_blocks.remove(&hash);
        let _ = self.state.seen.insert(hash);
        metrics::counter!("sync.header.tip.new_block.deduped").increment(1);
        self.trace_new_block_deduped(&peer, height, hash, "already_in_chain");
    }

    /// Remembers an accepted non-best-chain `NewBlock` for dedup without
    /// advancing any frontier or forwarding it. See
    /// [`HeaderSyncEvent::NewBlockAcceptedNonBestChain`].
    fn handle_new_block_accepted_non_best_chain(
        &mut self,
        peer: ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    ) {
        self.state.pending_new_blocks.remove(&hash);
        let _ = self.state.seen.insert(hash);
        metrics::counter!("sync.header.tip.new_block.non_best_chain").increment(1);
        self.trace_new_block_deduped(&peer, height, hash, "non_best_chain");
    }

    async fn handle_new_block_rejected(&mut self, peer: ZakuraPeerId, hash: block::Hash) {
        self.state.pending_new_blocks.remove(&hash);
        metrics::counter!("sync.header.tip.new_block.rejected").increment(1);
        debug!(
            ?peer,
            ?hash,
            "Zakura header-sync NewBlock rejected by block pipeline"
        );
        self.report_misbehavior(peer, HeaderSyncMisbehavior::InvalidNewBlock)
            .await;
    }

    async fn handle_wire_decode_failed(
        &mut self,
        peer: ZakuraPeerId,
        error: Arc<HeaderSyncWireError>,
    ) {
        if self.state.parked_peers.contains(&peer) {
            return;
        }
        record_wire_validation_metrics(&error);
        self.trace_peer_violation(&peer, HeaderSyncMisbehavior::MalformedMessage);
        tracing::debug!(?peer, ?error, "malformed Zakura header-sync frame");
        self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage)
            .await;
    }

    async fn handle_wire_protocol_failure(
        &mut self,
        peer: ZakuraPeerId,
        reason: HeaderSyncMisbehavior,
        error: Arc<HeaderSyncWireError>,
    ) {
        if self.state.parked_peers.contains(&peer) {
            return;
        }
        record_wire_validation_metrics(&error);
        self.trace_peer_violation(&peer, reason);
        tracing::debug!(?peer, ?error, ?reason, "invalid Zakura header-sync message");
        self.report_misbehavior(peer, reason).await;
    }

    async fn handle_state_frontiers_changed(&mut self, frontiers: HeaderSyncFrontiers) {
        self.state.finalized_height = frontiers.finalized_height;
        self.state.verified_block_tip = frontiers.verified_block_tip;
        self.state.verified_block_hash = frontiers.verified_block_hash;
        if self.state.best_header_tip <= self.state.verified_block_tip {
            self.state.stale_anchor.reset();
        }
        self.schedule().await;
    }

    async fn handle_vct_root_repair_requested(
        &mut self,
        height: block::Height,
        generation: u64,
        anchor_hash: block::Hash,
        expected_hashes: Vec<(block::Height, block::Hash)>,
    ) {
        if self
            .state
            .repair
            .as_ref()
            .is_some_and(|repair| repair.generation == generation)
        {
            return;
        }

        let previous_episode = self
            .state
            .repair
            .as_ref()
            .filter(|repair| repair.height == height)
            .map(|repair| (repair.tried_peers.clone(), repair.started_at));

        let Some(mut repair) = VctRootRepair::new(height, generation, anchor_hash, expected_hashes)
        else {
            tracing::warn!(
                ?height,
                generation,
                "ignoring invalid VCT root repair request"
            );
            metrics::counter!("sync.header.vct_repair.invalid_request").increment(1);
            return;
        };
        if let Some((tried_peers, started_at)) = previous_episode {
            repair.tried_peers = tried_peers;
            repair.started_at = started_at;
        }

        tracing::warn!(
            ?height,
            generation,
            count = repair.range.count,
            "scheduling bounded VCT supplied-root repair"
        );
        metrics::counter!("sync.header.vct_repair.requested").increment(1);
        self.state.repair = Some(repair);
        self.schedule().await;
    }

    async fn handle_vct_root_repair_resolved(&mut self, generation: u64) {
        if self
            .state
            .repair
            .as_ref()
            .is_some_and(|repair| repair.generation == generation)
        {
            self.state.repair = None;
            metrics::gauge!("sync.header.vct_repair.stalled.height").set(0.0);
            metrics::counter!("sync.header.vct_repair.resolved").increment(1);
        }
        self.schedule().await;
    }

    async fn handle_header_range_committed(
        &mut self,
        start_height: block::Height,
        tip_height: block::Height,
        tip_hash: block::Hash,
    ) {
        metrics::counter!("sync.header.range.committed").increment(1);
        self.trace_range_event(
            hs_trace::HEADER_RANGE_COMMITTED,
            start_height,
            count_between(start_height, tip_height),
            None,
            None,
        );
        let completed_repair_peer = self
            .state
            .repair
            .as_ref()
            .and_then(|repair| repair.in_flight.as_ref())
            .filter(|repair_peer| {
                self.state.pending_commits.iter().any(|(key, range)| {
                    &key.peer == *repair_peer
                        && range.priority == RangePriority::Repair
                        && range.is_within(start_height, tip_height)
                })
            })
            .cloned();
        self.state
            .pending_commits
            .retain(|_, range| !range.is_within(start_height, tip_height));
        if let Some(repair_peer) = completed_repair_peer {
            if let Some(repair) = self.state.repair.as_mut() {
                if repair.in_flight.as_ref() == Some(&repair_peer) {
                    // Committing the repair range finishes this peer's attempt, but does
                    // not prove the VCT root issue is fixed. Keep the repair active until
                    // the state writer reports it resolved, and free this peer slot.
                    repair.in_flight = None;
                }
            }
        }
        self.state
            .schedule
            .mark_range_covered(start_height, tip_height);
        // The zakurad driver also uses this event to reload the durable best header tip at
        // startup. In that path start==tip, so covered-range side effects are bounded.
        self.cancel_covered_outstanding();
        if tip_height > self.state.best_header_tip {
            self.publish_best_tip(tip_height, tip_hash).await;
        }
        self.notify_body_gaps().await;
        self.schedule().await;
    }

    async fn handle_header_range_commit_failed(
        &mut self,
        peer: ZakuraPeerId,
        start_height: block::Height,
        count: u32,
        kind: HeaderSyncCommitFailureKind,
    ) {
        metrics::counter!("sync.header.range.rejected").increment(1);
        self.trace_range_event(
            hs_trace::HEADER_RANGE_REJECTED,
            start_height,
            count,
            Some(&peer),
            Some(commit_failure_reason_label(kind)),
        );
        if kind == HeaderSyncCommitFailureKind::InvalidPeerRange {
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                .await;
        }
        let key = PendingCommitKey {
            peer: peer.clone(),
            start_height,
            count,
        };
        if let Some(range) = self.state.pending_commits.remove(&key) {
            if range.priority == RangePriority::Repair {
                self.finish_vct_repair_attempt(&peer);
                self.schedule().await;
                return;
            }
            if kind == HeaderSyncCommitFailureKind::Local {
                self.state.schedule.clear_assignment(range);
            }
            self.state.schedule.retry(range);
        }
        self.schedule().await;
    }

    fn handle_header_range_response_finished(
        &mut self,
        peer: ZakuraPeerId,
        request_id: HeaderSyncRequestId,
        start_height: block::Height,
        requested_count: u32,
        returned_count: u32,
    ) {
        self.trace_headers_served(
            &peer,
            start_height,
            requested_count,
            returned_count,
            false,
            TreeAuxTraceSummary::default(),
        );
        if let Some(peer_state) = self.state.peers.get_mut(&peer) {
            let _ = peer_state.finish_serving_headers(request_id);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_header_range_response_ready(
        &mut self,
        peer: ZakuraPeerId,
        request_id: HeaderSyncRequestId,
        start_height: block::Height,
        requested_count: u32,
        want_tree_aux_roots: bool,
        mut headers: Vec<Arc<block::Header>>,
        mut body_sizes: Vec<u32>,
        mut tree_aux_roots: Vec<BlockCommitmentRoots>,
    ) {
        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            return;
        };
        if validate_body_sizes_len(headers.len(), body_sizes.len()).is_err() {
            let _ = peer_state.finish_serving_headers(request_id);
            return;
        }

        let roots_complete = validate_tree_aux_roots_len(headers.len(), tree_aux_roots.len())
            .and_then(|()| validate_tree_aux_root_heights(start_height, &tree_aux_roots))
            .is_ok();
        if !headers.is_empty() && (!want_tree_aux_roots || !roots_complete) {
            headers.clear();
            body_sizes.clear();
            tree_aux_roots.clear();
        };
        let returned_count = u32::try_from(headers.len()).unwrap_or(u32::MAX);
        let served_tree_aux_roots = TreeAuxTraceSummary::new(&tree_aux_roots);
        if !peer_state.finish_serving_headers(request_id) {
            metrics::counter!("sync.header.response.stale_serving_request_id").increment(1);
            return;
        }
        let send_result = peer_state.session.try_send_headers_with_sizes_and_roots(
            request_id,
            headers,
            body_sizes,
            tree_aux_roots,
        );
        let queue_capacity = peer_state.session.outbound_capacity();
        let queue_max_capacity = peer_state.session.outbound_max_capacity();

        match send_result {
            Ok(()) => self.trace_headers_served(
                &peer,
                start_height,
                requested_count,
                returned_count,
                want_tree_aux_roots,
                served_tree_aux_roots,
            ),
            Err(error) => {
                tracing::debug!(
                    ?peer,
                    ?start_height,
                    ?requested_count,
                    ?error,
                    "failed to queue Zakura header-sync Headers response"
                );
                self.trace_queue_send_failed(
                    &peer,
                    "headers",
                    &error,
                    queue_capacity,
                    queue_max_capacity,
                    |row| {
                        insert_height(row, qs_trace::RANGE_START, start_height);
                        insert_u64(row, qs_trace::RANGE_COUNT, u64::from(requested_count));
                        insert_u64(row, qs_trace::RETURNED, u64::from(returned_count));
                    },
                );
            }
        }
    }

    async fn handle_wire_message(&mut self, peer: ZakuraPeerId, msg: HeaderSyncMessage) {
        if self.state.parked_peers.contains(&peer) {
            return;
        }

        match msg {
            HeaderSyncMessage::Status(status) => {
                metrics::counter!("sync.header.peer.status.received").increment(1);
                if status.anchor_height > status.tip_height {
                    self.report_misbehavior(peer, HeaderSyncMisbehavior::InvalidStatus)
                        .await;
                    return;
                }

                let Some(peer_state) = self.state.peers.get_mut(&peer) else {
                    return;
                };
                let now = Instant::now();
                let advances_advertised_tip = status.tip_height > peer_state.advertised_tip;
                let status_token_available = peer_state.meters.inbound_status.try_take(now);
                if !advances_advertised_tip && !status_token_available {
                    self.report_misbehavior(peer, HeaderSyncMisbehavior::StatusSpam)
                        .await;
                    return;
                }
                peer_state.advertised_tip = status.tip_height;
                peer_state.advertised_hash = status.tip_hash;
                peer_state.anchor = status.anchor_height;
                peer_state.max_headers_per_response =
                    clamp_advertised_range(status.max_headers_per_response);
                peer_state.max_inflight_requests = status
                    .max_inflight_requests
                    .clamp(1, LOCAL_MAX_HS_INFLIGHT_PER_PEER);
                peer_state.received_status = true;
                peer_state.last_received_status_at = Some(now);
                self.confirm_advisory_status(&peer, status);
                self.trace_status_received(&peer, status);
                self.publish_connectivity_metrics();
                self.schedule().await;
            }
            HeaderSyncMessage::NewBlock(block) => {
                self.handle_new_block(peer, block).await;
            }
            // `GetHeaders`/`Headers` carry a request ID and are decoded into the
            // correlated `WireGetHeaders`/`WireHeaders` events, so they never reach
            // this uncorrelated path.
            HeaderSyncMessage::GetHeaders { .. } | HeaderSyncMessage::Headers { .. } => {
                self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage)
                    .await;
            }
        }
    }

    async fn handle_get_headers(
        &mut self,
        peer: ZakuraPeerId,
        request_id: HeaderSyncRequestId,
        start_height: block::Height,
        count: u32,
        want_tree_aux_roots: bool,
    ) {
        let local_inflight_cap = self.startup.config.advertised_max_inflight_requests();
        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::GetHeadersSpam)
                .await;
            return;
        };

        if !peer_state.received_status {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::GetHeadersSpam)
                .await;
            return;
        }

        let allowed_count = inbound_get_headers_count_limit(
            &self.startup.config,
            &self.startup.network,
            self.startup.max_frame_bytes,
            want_tree_aux_roots,
        );
        if count == 0 || count > allowed_count {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::GetHeadersTooLong)
                .await;
            return;
        }

        if !peer_state.try_start_serving_headers(local_inflight_cap, request_id) {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::GetHeadersSpam)
                .await;
            return;
        }
        let session_id = peer_state.session.session_id();

        if !self.dispatch_action(HeaderSyncAction::QueryHeadersByHeightRange {
            peer: peer.clone(),
            session_id,
            request_id,
            start: start_height,
            count,
            want_tree_aux_roots,
        }) {
            if let Some(peer_state) = self.state.peers.get_mut(&peer) {
                let _ = peer_state.finish_serving_headers(request_id);
            }
        }
    }

    #[tracing::instrument(skip(self, block))]
    async fn handle_new_block(&mut self, peer: ZakuraPeerId, block: Arc<block::Block>) {
        metrics::counter!("sync.header.tip.new_block.received").increment(1);

        if !self.state.peers.contains_key(&peer) {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::UnknownPeer)
                .await;
            return;
        }

        let hash = block.hash();
        let Some(height) = block.coinbase_height() else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage)
                .await;
            return;
        };
        self.trace_new_block_received(&peer, height, hash);

        if self.state.seen.contains(&hash) {
            metrics::counter!("sync.header.tip.new_block.deduped").increment(1);
            self.trace_new_block_deduped(&peer, height, hash, "seen_cache");
            return;
        }
        if self.state.pending_new_blocks.contains(&hash) {
            metrics::counter!("sync.header.tip.new_block.deduped").increment(1);
            self.trace_new_block_deduped(&peer, height, hash, "pending_acceptance");
            return;
        }

        if !self
            .state
            .peers
            .get_mut(&peer)
            .expect("peer exists because it was checked before validation")
            .meters
            .inbound_new_block
            .try_take(Instant::now())
        {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::NewBlockSpam)
                .await;
            return;
        }

        if validate_new_block_stateless(block.clone(), &self.startup.network, Utc::now(), height)
            .await
            .is_err()
        {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::InvalidNewBlock)
                .await;
            return;
        }

        if !self.startup.inbound_new_block_acceptance_enabled {
            metrics::counter!("sync.header.tip.new_block.acceptance_unavailable").increment(1);
            debug!(
                ?peer,
                ?hash,
                "Zakura header-sync NewBlock body suppressed until block acceptance is wired"
            );
            return;
        }

        let inserted = self.state.pending_new_blocks.insert(hash);
        debug_assert!(inserted, "pending acceptance was checked before insert");

        if !self.dispatch_action(HeaderSyncAction::NewBlockReceived {
            peer,
            height,
            hash,
            block,
        }) {
            self.state.pending_new_blocks.remove(&hash);
        }
    }

    fn eligible_tip_destinations(
        &self,
        source: &ZakuraPeerId,
        height: block::Height,
    ) -> Vec<ZakuraPeerId> {
        let mut peers: Vec<_> = self
            .state
            .peers
            .iter()
            .filter(|(peer_id, peer)| {
                *peer_id != source && (!peer.received_status || peer.advertised_tip < height)
            })
            .map(|(peer_id, _)| peer_id.clone())
            .collect();
        peers.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        peers
    }

    #[tracing::instrument(skip(self, headers))]
    async fn handle_headers(
        &mut self,
        peer: ZakuraPeerId,
        request_id: HeaderSyncRequestId,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        tree_aux_roots: Vec<BlockCommitmentRoots>,
    ) {
        metrics::counter!("sync.header.response.received").increment(1);
        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::UnsolicitedHeaders)
                .await;
            return;
        };
        let Some(outstanding) = peer_state.remove_outstanding_by_request_id(request_id) else {
            // The pipe already dropped responses to retired IDs and fails closed on IDs
            // this session never issued, so an ID with no outstanding range here is one
            // the reactor retired after the pipe correlated it. Drop it without scoring.
            metrics::counter!("sync.header.response.unknown_request_id").increment(1);
            return;
        };
        let peer_max_headers_per_response = peer_state.max_headers_per_response;
        let in_flight_count = peer_state.outstanding.len();

        self.handle_headers_for_outstanding(
            peer,
            headers,
            body_sizes,
            tree_aux_roots,
            outstanding,
            peer_max_headers_per_response,
            in_flight_count,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_headers_for_outstanding(
        &mut self,
        peer: ZakuraPeerId,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        tree_aux_roots: Vec<BlockCommitmentRoots>,
        outstanding: OutstandingRange,
        peer_max_headers_per_response: u32,
        in_flight_count: usize,
    ) {
        if validate_body_sizes_len(headers.len(), body_sizes.len()).is_err()
            || validate_tree_aux_roots_len(headers.len(), tree_aux_roots.len()).is_err()
        {
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::MalformedMessage)
                .await;
            self.retry_or_finish_outstanding(&peer, outstanding);
            self.schedule().await;
            return;
        }
        if !outstanding.range.want_tree_aux_roots && !tree_aux_roots.is_empty() {
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::MalformedMessage)
                .await;
            self.retry_or_finish_outstanding(&peer, outstanding);
            self.schedule().await;
            return;
        }

        if headers.is_empty() {
            if matches!(outstanding.purpose, RangePurpose::VctRepair { .. }) {
                self.record_advisory_unconfirmed(&peer);
                self.finish_vct_repair_attempt(&peer);
                self.schedule().await;
                return;
            }
            self.record_advisory_unconfirmed(&peer);
            let deadline = Instant::now() + self.empty_headers_retry_delay();
            self.trace_headers_received(
                &peer,
                outstanding.range.start_height,
                0,
                outstanding.expected_max_count,
                peer_max_headers_per_response,
                in_flight_count,
                outstanding.range.want_tree_aux_roots,
                &tree_aux_roots,
            );
            if let Some(peer_state) = self.state.peers.get_mut(&peer) {
                peer_state.outstanding.push(OutstandingRange {
                    deadline,
                    clear_assignment_on_timeout: true,
                    ..outstanding
                });
            }
            return;
        }

        let header_count =
            u32::try_from(headers.len()).expect("decoded Headers length is capped by u32");
        self.trace_headers_received(
            &peer,
            outstanding.range.start_height,
            header_count,
            outstanding.expected_max_count,
            peer_max_headers_per_response,
            in_flight_count,
            outstanding.range.want_tree_aux_roots,
            &tree_aux_roots,
        );
        if header_count > outstanding.expected_max_count || header_count > outstanding.range.count {
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::ResponseTooLong)
                .await;
            self.retry_or_finish_outstanding(&peer, outstanding);
            self.schedule().await;
            return;
        }

        if let Err(reason) =
            self.validate_vct_repair_response(&outstanding, &headers, &tree_aux_roots)
        {
            tracing::debug!(
                ?peer,
                ?reason,
                start_height = ?outstanding.range.start_height,
                count = header_count,
                "Zakura header-sync rejected VCT root repair response"
            );
            if reason == STALE_REPAIR_GENERATION {
                self.schedule().await;
                return;
            }
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                .await;
            self.finish_vct_repair_attempt(&peer);
            self.schedule().await;
            return;
        }

        let validation_context = HeaderSyncValidationContext {
            network: &self.startup.network,
            now: Utc::now(),
            start_height: outstanding.range.start_height,
            decode_context: HeaderSyncDecodeContext::for_headers_response(
                ExpectedHeadersResponse::new(
                    outstanding.request_id,
                    outstanding.range.start_height,
                    outstanding.expected_max_count,
                    outstanding.range.want_tree_aux_roots,
                )
                .expect("outstanding range uses a non-zero bounded count"),
                outstanding.expected_max_count,
            ),
        };
        if let Err(error) = validate_header_range_links(outstanding.range.anchor_hash, &headers) {
            debug!(
                ?peer,
                ?error,
                anchor_hash = ?outstanding.range.anchor_hash,
                start_height = ?outstanding.range.start_height,
                count = ?header_count,
                "Zakura header-sync rejected header range links"
            );
            self.trace_range_validation_rejected(
                &peer,
                outstanding.range,
                header_count,
                "link",
                header_sync_wire_error_kind(&error),
            );
            if self
                .handle_possible_stale_anchor_link_failure(&peer, outstanding.range, &error)
                .await
            {
                self.schedule().await;
                return;
            }
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                .await;
            self.retry_or_finish_outstanding(&peer, outstanding);
            self.schedule().await;
            return;
        }
        if let Err(error) =
            validate_tree_aux_root_heights(outstanding.range.start_height, &tree_aux_roots)
        {
            tracing::debug!(
                ?peer,
                ?error,
                start_height = ?outstanding.range.start_height,
                count = ?header_count,
                "Zakura header-sync rejected tree-aux root heights"
            );
            self.trace_range_validation_rejected(
                &peer,
                outstanding.range,
                header_count,
                "tree_aux_heights",
                header_sync_wire_error_kind(&error),
            );
            metrics::counter!("sync.header.tree_aux.height_mismatch").increment(1);
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::MalformedMessage)
                .await;
            self.retry_or_finish_outstanding(&peer, outstanding);
            self.schedule().await;
            return;
        }
        if let Err(error) = validate_headers_stateless(headers.clone(), validation_context).await {
            debug!(
                ?peer,
                ?error,
                start_height = ?outstanding.range.start_height,
                count = ?header_count,
                "Zakura header-sync rejected stateless header range"
            );
            self.trace_range_validation_rejected(
                &peer,
                outstanding.range,
                header_count,
                "stateless",
                header_sync_wire_error_kind(&error),
            );
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                .await;
            self.retry_or_finish_outstanding(&peer, outstanding);
            self.schedule().await;
            return;
        }

        let end_height = height_after_count(outstanding.range.start_height, header_count)
            .and_then(previous_height)
            .expect("non-empty bounded range has an end height");
        if outstanding.range.finalized {
            let last_hash = headers
                .last()
                .map(|header| block::Hash::from(header.as_ref()))
                .expect("headers is non-empty");
            if end_height != outstanding.range.end_height()
                || self.startup.network.checkpoint_list().hash(end_height) != Some(last_hash)
            {
                self.trace_range_validation_rejected(
                    &peer,
                    outstanding.range,
                    header_count,
                    "checkpoint",
                    "checkpoint_hash_mismatch",
                );
                self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                    .await;
                self.retry_or_finish_outstanding(&peer, outstanding);
                self.schedule().await;
                return;
            }
        }

        self.state.pending_commits.insert(
            PendingCommitKey {
                peer: peer.clone(),
                start_height: outstanding.range.start_height,
                count: header_count,
            },
            outstanding.range,
        );
        let session_id = self
            .state
            .peers
            .get(&peer)
            .map(|state| state.session.session_id())
            .expect("peer exists because its outstanding response is being committed");
        let _ = self.dispatch_action(HeaderSyncAction::CommitHeaderRange {
            peer,
            session_id,
            anchor: outstanding.range.anchor_hash,
            start_height: outstanding.range.start_height,
            headers,
            body_sizes,
            tree_aux_roots,
            finalized: outstanding.range.finalized,
        });
    }

    fn validate_vct_repair_response(
        &self,
        outstanding: &OutstandingRange,
        headers: &[Arc<block::Header>],
        tree_aux_roots: &[BlockCommitmentRoots],
    ) -> Result<(), &'static str> {
        let RangePurpose::VctRepair { generation, .. } = outstanding.purpose else {
            return Ok(());
        };
        let Some(repair) = self
            .state
            .repair
            .as_ref()
            .filter(|repair| repair.generation == generation)
        else {
            return Err(STALE_REPAIR_GENERATION);
        };
        if headers.len() != repair.expected_hashes.len()
            || tree_aux_roots.len() != repair.expected_hashes.len()
        {
            return Err("wrong_repair_count");
        }
        for ((expected_height, expected_hash), (index, header)) in repair
            .expected_hashes
            .iter()
            .zip(headers.iter().enumerate())
        {
            let Some(actual_height) = repair
                .height
                .0
                .checked_add(u32::try_from(index).map_err(|_| "height_offset_overflow")?)
                .map(block::Height)
            else {
                return Err("height_overflow");
            };
            if *expected_height != actual_height
                || block::Hash::from(header.as_ref()) != *expected_hash
            {
                return Err("noncanonical_header");
            }
        }
        Ok(())
    }

    fn retry_or_finish_outstanding(&mut self, peer: &ZakuraPeerId, outstanding: OutstandingRange) {
        match outstanding.purpose {
            RangePurpose::Sync => self.state.schedule.retry(outstanding.range),
            RangePurpose::VctRepair { .. } => self.finish_vct_repair_attempt(peer),
        }
    }

    fn finish_vct_repair_attempt(&mut self, peer: &ZakuraPeerId) {
        let Some(repair) = self.state.repair.as_mut() else {
            return;
        };
        let was_exhausted = repair.exhausted;
        if !repair.finish_attempt(peer, Instant::now()) {
            return;
        }
        if !was_exhausted && repair.exhausted {
            Self::report_vct_repair_exhausted(repair);
        }
    }

    fn report_vct_repair_exhausted(repair: &VctRootRepair) {
        tracing::error!(
            height = ?repair.height,
            generation = repair.generation,
            attempts = repair.tried_peers.len(),
            "VCT supplied-root repair exhausted bounded peer attempts or wall time; node remains fail-closed"
        );
        metrics::counter!("sync.header.vct_repair.exhausted").increment(1);
        metrics::gauge!("sync.header.vct_repair.stalled.height").set(f64::from(repair.height.0));
    }

    async fn handle_possible_stale_anchor_link_failure(
        &mut self,
        peer: &ZakuraPeerId,
        range: RangeRequest,
        error: &HeaderSyncWireError,
    ) -> bool {
        if !matches!(error, HeaderSyncWireError::FirstHeaderDoesNotLink)
            || range.priority != RangePriority::Forward
            || range.finalized
            || self.state.best_header_tip <= self.state.verified_block_tip
        {
            self.state.stale_anchor.reset();
            return false;
        }

        self.state.stale_anchor.record(peer.clone());
        metrics::counter!("sync.header.stale_anchor.link_failure").increment(1);

        if !self.state.stale_anchor.should_reanchor() {
            self.state.schedule.clear_assignment(range);
            self.state.schedule.retry(range);
            return true;
        }

        self.reanchor_to_verified_block_tip().await;
        true
    }

    async fn reanchor_to_verified_block_tip(&mut self) {
        let height = self.state.verified_block_tip;
        let hash = self.state.verified_block_hash;
        metrics::counter!("sync.header.stale_anchor.reanchored").increment(1);

        self.state.stale_anchor.reset();
        self.state.schedule.clear_forward();
        self.state
            .pending_commits
            .retain(|_, range| range.priority != RangePriority::Forward);
        self.cancel_forward_outstanding();
        self.publish_best_tip_reanchored(height, hash).await;
    }

    async fn handle_timeouts(&mut self) {
        let now = Instant::now();
        let mut timed_out = Vec::new();
        let mut retired_request_ids = Vec::new();
        for peer in self.state.peers.values_mut() {
            let mut index = 0;
            while index < peer.outstanding.len() {
                if peer.outstanding[index].deadline <= now {
                    let outstanding = peer.outstanding.remove(index);
                    let peer_id = peer.session.peer_id().clone();
                    retired_request_ids.push((peer_id.clone(), outstanding.request_id));
                    timed_out.push((outstanding, peer_id));
                } else {
                    index += 1;
                }
            }
        }
        for (outstanding, peer) in timed_out {
            match outstanding.purpose {
                RangePurpose::Sync => {
                    if outstanding.clear_assignment_on_timeout {
                        self.state.schedule.clear_assignment(outstanding.range);
                    }
                    self.state.schedule.retry(outstanding.range);
                }
                RangePurpose::VctRepair { .. } => {
                    metrics::counter!("sync.header.vct_repair.timeout").increment(1);
                    self.finish_vct_repair_attempt(&peer);
                }
            }
        }
        // Retiring the ID is enough: a response that arrives after its deadline is
        // matched to the retired request and dropped, so it can never be mistaken for
        // a newer one. The stream stays up.
        for (peer, request_id) in retired_request_ids {
            if let Some(peer) = self.state.peers.get(&peer) {
                let _ = peer.session.retire_expected_headers(request_id);
            }
        }
        self.schedule().await;
    }

    fn empty_headers_retry_delay(&self) -> Duration {
        self.startup.request_timeout.min(EMPTY_HEADERS_RETRY_DELAY)
    }

    async fn schedule(&mut self) {
        if !self.startup.range_state_actions_enabled {
            return;
        }

        self.state.refresh_forward_range(&self.startup);
        self.state.refresh_backward_range(&self.startup);

        if self.schedule_vct_repair().await {
            return;
        }

        // Sorted once, not per pass: scheduling only fills a peer's in-flight slots,
        // it never adds or removes peers, so the set is fixed for this call. A peer
        // that disconnects concurrently is skipped by `schedule_one_for_peer`.
        let mut peer_ids: Vec<ZakuraPeerId> = self.state.peers.keys().cloned().collect();
        peer_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        loop {
            let mut scheduled_any = false;
            for peer_id in &peer_ids {
                scheduled_any |= self.schedule_one_for_peer(peer_id).await;
            }
            if !scheduled_any {
                break;
            }
        }
    }

    async fn schedule_one_for_peer(&mut self, peer_id: &ZakuraPeerId) -> bool {
        let Some(peer) = self.state.peers.get(peer_id) else {
            return false;
        };
        if !peer.received_status || peer.available_slots() == 0 {
            return false;
        }

        let Some(mut range) = self.state.schedule.next_for_peer(peer_id, peer) else {
            return false;
        };
        let original_range = range;
        let count = clamp_header_sync_request_count(
            range.count,
            peer.max_headers_per_response,
            &self.startup.network,
            self.startup.max_frame_bytes,
            range.want_tree_aux_roots,
        );
        if range.finalized && count < range.count {
            self.state.schedule.retry(range);
            return false;
        }
        range.count = count;
        self.state
            .schedule
            .narrow_queued_range(original_range, range);

        let peer_cap = peer.max_headers_per_response;
        let Some(peer) = self.state.peers.get(peer_id) else {
            return false;
        };
        let session_id = peer.session.session_id();
        let stream_version = ZAKURA_HEADER_SYNC_STREAM_VERSION;
        let request_id = match peer.session.try_send_get_headers(
            range.start_height,
            count,
            range.want_tree_aux_roots,
        ) {
            Ok(request_id) => request_id,
            Err(error) => {
                tracing::debug!(
                    peer = ?peer_id,
                    start_height = ?range.start_height,
                    count,
                    ?error,
                    "failed to queue Zakura header-sync GetHeaders"
                );
                self.trace_queue_send_failed(
                    peer_id,
                    "get_headers",
                    &error,
                    peer.session.outbound_capacity(),
                    peer.session.outbound_max_capacity(),
                    |row| {
                        insert_height(row, qs_trace::RANGE_START, range.start_height);
                        insert_u64(row, qs_trace::RANGE_COUNT, u64::from(count));
                    },
                );
                self.state.schedule.retry(range);
                return false;
            }
        };

        let outstanding = OutstandingRange {
            request_id,
            range,
            deadline: Instant::now() + self.startup.request_timeout,
            expected_max_count: count,
            clear_assignment_on_timeout: false,
            purpose: RangePurpose::Sync,
        };
        if let Some(peer) = self.state.peers.get_mut(peer_id) {
            peer.outstanding.push(outstanding);
        }
        self.state.schedule.mark_assigned(peer_id.clone(), range);
        metrics::counter!("sync.header.request.sent").increment(1);
        self.trace_get_headers_sent(
            peer_id,
            range,
            count,
            peer_cap,
            GetHeadersTraceMeta {
                request_id,
                session_id,
                stream_version,
            },
        );
        #[cfg(test)]
        let _ = self
            .actions
            .send(HeaderSyncAction::SendMessage {
                peer: peer_id.clone(),
                request_id: Some(request_id),
                msg: HeaderSyncMessage::GetHeaders {
                    start_height: range.start_height,
                    count,
                    want_tree_aux_roots: range.want_tree_aux_roots,
                },
            })
            .await;
        true
    }

    async fn schedule_vct_repair(&mut self) -> bool {
        let now = Instant::now();
        let newly_exhausted = self
            .state
            .repair
            .as_mut()
            .is_some_and(|repair| repair.refresh_exhausted(now));
        if newly_exhausted {
            let repair = self
                .state
                .repair
                .as_ref()
                .expect("repair exists after its exhaustion transition");
            Self::report_vct_repair_exhausted(repair);
        }
        let Some(repair) = self.state.repair.as_ref() else {
            return false;
        };
        if !repair.can_attempt(now) {
            return false;
        }

        let mut peer_ids: Vec<_> = self
            .state
            .peers
            .iter()
            .filter(|(peer_id, peer)| {
                !self.state.parked_peers.contains(*peer_id)
                    && !self
                        .state
                        .advisory
                        .get(*peer_id)
                        .is_some_and(|advisory| advisory.is_backed_off(now))
                    && peer.received_status
                    && peer.outstanding.is_empty()
                    && peer.advertised_tip >= repair.range.end_height()
                    && peer.max_headers_per_response >= repair.range.count
                    && !repair.tried_peers.contains(*peer_id)
            })
            .map(|(peer_id, _)| peer_id.clone())
            .collect();
        peer_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let Some(peer_id) = peer_ids.into_iter().next() else {
            return false;
        };

        let Some(peer) = self.state.peers.get(&peer_id) else {
            return false;
        };
        let range = repair.range;
        let peer_cap = peer.max_headers_per_response;
        let session_id = peer.session.session_id();
        let stream_version = ZAKURA_HEADER_SYNC_STREAM_VERSION;
        let request_id =
            match peer
                .session
                .try_send_get_headers(range.start_height, range.count, true)
            {
                Ok(request_id) => request_id,
                Err(error) => {
                    tracing::debug!(
                        peer = ?peer_id,
                        start_height = ?range.start_height,
                        count = range.count,
                        ?error,
                        "failed to queue VCT repair GetHeaders"
                    );
                    return false;
                }
            };

        let (height, generation) = {
            let repair = self
                .state
                .repair
                .as_mut()
                .expect("repair existed when scheduling started");
            repair.mark_attempt(peer_id.clone());
            (repair.height, repair.generation)
        };
        let outstanding = OutstandingRange {
            request_id,
            range,
            deadline: Instant::now() + self.startup.request_timeout,
            expected_max_count: range.count,
            clear_assignment_on_timeout: false,
            purpose: RangePurpose::VctRepair { height, generation },
        };
        if let Some(peer) = self.state.peers.get_mut(&peer_id) {
            peer.outstanding.push(outstanding);
        }

        metrics::counter!("sync.header.vct_repair.request.sent").increment(1);
        self.trace_get_headers_sent(
            &peer_id,
            range,
            range.count,
            peer_cap,
            GetHeadersTraceMeta {
                request_id,
                session_id,
                stream_version,
            },
        );
        #[cfg(test)]
        let _ = self
            .actions
            .send(HeaderSyncAction::SendMessage {
                peer: peer_id,
                request_id: Some(request_id),
                msg: HeaderSyncMessage::GetHeaders {
                    start_height: range.start_height,
                    count: range.count,
                    want_tree_aux_roots: true,
                },
            })
            .await;

        true
    }

    fn send_status(&mut self, peer: &ZakuraPeerId) -> bool {
        self.send_status_inner(peer, false)
    }

    /// Sends the current status even when identical to the last one sent.
    ///
    /// The connection-level freshness reaper only counts inbound application
    /// messages, so two peers at the same tip would otherwise go mutually
    /// silent and reap healthy connections every idle window. The periodic
    /// refresh uses this forced send as an application keepalive: it is gated
    /// by the peer's unsolicited meter (`status_refresh_interval` spacing),
    /// which stays far above the remote's inbound status minimum interval, so
    /// the redundant status is never classified as status spam.
    fn send_status_keepalive(&mut self, peer: &ZakuraPeerId) -> bool {
        self.send_status_inner(peer, true)
    }

    fn send_status_inner(&mut self, peer: &ZakuraPeerId, force: bool) -> bool {
        let status = self.local_status();
        // Suppress a status identical to the last one we sent this peer over its
        // current session: it advances nothing and the peer's inbound status
        // rate limiter would treat the redundant message as spam. Keepalive
        // sends are exempt: their meter keeps them above that limit.
        let session = match self.state.peers.get(peer) {
            Some(peer_state) if force || peer_state.status_differs_from_last_sent(status) => {
                peer_state.session.clone()
            }
            Some(_) => {
                metrics::counter!("sync.header.peer.status.suppressed_redundant").increment(1);
                return false;
            }
            None => return false,
        };
        match session.try_send_status(status) {
            Ok(()) => {
                if let Some(peer_state) = self.state.peers.get_mut(peer) {
                    peer_state.record_sent_status(status);
                }
                metrics::counter!("sync.header.peer.status.sent").increment(1);
                self.trace_status_sent(peer, status);
                #[cfg(test)]
                let _ = self.actions.try_send(HeaderSyncAction::SendMessage {
                    peer: peer.clone(),
                    request_id: None,
                    msg: HeaderSyncMessage::Status(status),
                });
                true
            }
            Err(error) => {
                metrics::counter!("sync.header.peer.status.send_failed").increment(1);
                tracing::debug!(?peer, ?error, "failed to queue Zakura header-sync Status");
                self.trace_queue_send_failed(
                    peer,
                    "status",
                    &error,
                    session.outbound_capacity(),
                    session.outbound_max_capacity(),
                    |_| {},
                );
                false
            }
        }
    }

    fn send_status_and_mark_unsolicited(&mut self, peer: &ZakuraPeerId, now: Instant) -> bool {
        if !self.send_status(peer) {
            return false;
        }

        if let Some(peer_state) = self.state.peers.get_mut(peer) {
            peer_state.meters.unsolicited.mark_taken(now);
        }

        true
    }

    async fn publish_best_tip(&mut self, height: block::Height, hash: block::Hash) {
        self.state.best_header_tip = height;
        self.state.best_header_hash = hash;
        metrics::gauge!("sync.header.best_tip.height").set(height.0 as f64);
        self.trace_frontier_advanced(height, hash);
        let _ = self.tip.send((height, hash));
        let _ = self.dispatch_action(HeaderSyncAction::HeaderAdvanced { height, hash });
        self.publish_candidate_state();
        self.broadcast_status_refresh().await;
    }

    async fn publish_best_tip_reanchored(&mut self, height: block::Height, hash: block::Hash) {
        let old = (self.state.best_header_tip, self.state.best_header_hash);
        self.state.best_header_tip = height;
        self.state.best_header_hash = hash;
        metrics::gauge!("sync.header.best_tip.height").set(height.0 as f64);
        self.trace_frontier_reanchored(height, hash);
        let _ = self.tip.send((height, hash));
        let _ = self.dispatch_action(HeaderSyncAction::HeaderReanchored {
            old,
            new: (height, hash),
        });
        self.publish_candidate_state();
        self.broadcast_status_refresh().await;
    }

    fn update_verified_block_tip(&mut self, height: block::Height, hash: block::Hash) {
        if height > self.state.verified_block_tip {
            self.state.verified_block_tip = height;
            self.state.verified_block_hash = hash;
        }
        if self.state.best_header_tip <= self.state.verified_block_tip {
            self.state.stale_anchor.reset();
        }
    }

    /// Periodic status refresh, doubling as an application-level keepalive.
    ///
    /// Every peer whose unsolicited meter is ready (one `status_refresh_interval`
    /// since the last unsolicited send) gets the current status even when it is
    /// unchanged: the connection freshness reaper only counts inbound messages,
    /// so without this two peers idle at the same tip reap their healthy
    /// connection every idle window. A failed send does not mark the meter, so
    /// a peer whose initial status was lost to a dead session is retried on the
    /// next tick instead of staying connected-but-mute.
    fn refresh_statuses(&mut self) {
        let now = Instant::now();
        let status = self.local_status();

        // Unsent or changed statuses retry on the fast unsolicited budget, so a
        // peer whose initial status was lost to a dead session queue recovers on
        // the next tick instead of staying connected-but-mute.
        let retry_ids: Vec<_> = self
            .state
            .peers
            .iter()
            .filter(|(_peer_id, peer)| {
                peer.status_differs_from_last_sent(status) && peer.meters.unsolicited.is_ready(now)
            })
            .map(|(peer_id, _peer)| peer_id.clone())
            .collect();
        for peer in retry_ids {
            self.send_status_and_mark_unsolicited(&peer, now);
        }

        // Redundant keepalives run on the slower spam-safe keepalive budget.
        let keepalive_ids: Vec<_> = self
            .state
            .peers
            .iter()
            .filter(|(_peer_id, peer)| {
                !peer.status_differs_from_last_sent(status)
                    && peer.meters.keepalive.is_ready(now)
                    && peer.meters.unsolicited.is_ready(now)
            })
            .map(|(peer_id, _peer)| peer_id.clone())
            .collect();
        for peer in keepalive_ids {
            if self.send_status_keepalive(&peer) {
                if let Some(peer_state) = self.state.peers.get_mut(&peer) {
                    peer_state.meters.keepalive.mark_taken(now);
                    peer_state.meters.unsolicited.mark_taken(now);
                }
            }
        }
    }

    async fn broadcast_status_refresh(&mut self) {
        let now = Instant::now();
        let status = self.local_status();
        let peer_ids: Vec<_> = self
            .state
            .peers
            .iter()
            .filter_map(|(peer_id, peer)| {
                // Never re-send a peer a status identical to its last one: the
                // peer's inbound rate limiter would treat it as spam. A redundant
                // refresh is dropped without spending the peer's status budget.
                if !peer.status_differs_from_last_sent(status) {
                    metrics::counter!("sync.header.peer.status.suppressed_redundant").increment(1);
                    return None;
                }
                if !peer.meters.unsolicited.is_ready(now) {
                    return None;
                }
                Some(peer_id.clone())
            })
            .collect();

        for peer in peer_ids {
            self.send_status_and_mark_unsolicited(&peer, now);
        }
    }

    async fn notify_body_gaps(&self) {
        if !self.startup.range_state_actions_enabled {
            return;
        }

        if self.state.best_header_tip > self.state.verified_block_tip {
            let from =
                next_height(self.state.verified_block_tip).unwrap_or(self.state.verified_block_tip);
            metrics::gauge!("sync.header.missing_bodies")
                .set(count_between(from, self.state.best_header_tip) as f64);
            self.trace_missing_bodies(from, self.state.best_header_tip);
            let _ = self.dispatch_action(HeaderSyncAction::BodyGaps {
                from,
                to: self.state.best_header_tip,
            });
        }
    }

    /// Hand a data-plane action to the action driver without letting a slow or
    /// stalled driver wedge the reactor. Returns `true` only if the action was
    /// accepted.
    fn dispatch_action(&self, action: HeaderSyncAction) -> bool {
        self.trace_action_dispatched(&action);
        match self.actions.try_send(action) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("sync.header.action.send_queue_full").increment(1);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    async fn report_misbehavior(&mut self, peer: ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        // Misbehavior is record-only: trace and forward it, but never cancel the
        // session. Peer scoring no longer drives disconnects.
        metrics::counter!("sync.header.peer.violation").increment(1);
        self.trace_peer_violation(&peer, reason);
        self.trace_peer_violation_recorded(&peer, reason);
        // Best-effort record of the violation for the driver. Never block the
        // reactor waiting for channel capacity.
        let action = HeaderSyncAction::Misbehavior { peer, reason };
        self.trace_action_dispatched(&action);
        if self.actions.try_send(action).is_err() {
            metrics::counter!("sync.header.peer.violation.action_dropped").increment(1);
        }
    }

    fn trace_event_received(&self, event: &HeaderSyncEvent) {
        self.emit_trace(hs_trace::HEADER_EVENT_RECEIVED, |row| match event {
            HeaderSyncEvent::PeerConnected(session) => {
                insert_optional_str(row, hs_trace::KIND, Some("peer_connected"));
                insert_peer(row, hs_trace::PEER, session.peer_id());
            }
            HeaderSyncEvent::PeerDisconnected(peer) => {
                insert_optional_str(row, hs_trace::KIND, Some("peer_disconnected"));
                insert_peer(row, hs_trace::PEER, peer);
            }
            HeaderSyncEvent::AdvisoryHeaderSummary { peer, summary } => {
                insert_optional_str(row, hs_trace::KIND, Some("advisory_header_summary"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, summary.best_height);
            }
            HeaderSyncEvent::FullBlockCommitted { height, hash, .. } => {
                insert_optional_str(row, hs_trace::KIND, Some("full_block_committed"));
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::NewBlockAccepted {
                peer, height, hash, ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_accepted"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::NewBlockDuplicate { peer, height, hash } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_duplicate"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::NewBlockAcceptedNonBestChain { peer, height, hash } => {
                insert_optional_str(
                    row,
                    hs_trace::KIND,
                    Some("new_block_accepted_non_best_chain"),
                );
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::NewBlockRejected { peer, hash } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_rejected"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::WireMessage { peer, msg } => {
                insert_optional_str(row, hs_trace::KIND, Some("wire_message"));
                insert_optional_str(row, hs_trace::REASON, Some(header_sync_message_label(msg)));
                insert_peer(row, hs_trace::PEER, peer);
                trace_header_sync_message_fields(row, msg);
            }
            HeaderSyncEvent::SessionWireMessage { peer, msg, .. } => {
                insert_optional_str(row, hs_trace::KIND, Some("session_wire_message"));
                insert_optional_str(row, hs_trace::REASON, Some(header_sync_message_label(msg)));
                insert_peer(row, hs_trace::PEER, peer);
                trace_header_sync_message_fields(row, msg);
            }
            HeaderSyncEvent::WireHeaders {
                peer,
                request_id: _,
                headers,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("wire_headers"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_u64(row, hs_trace::RANGE_COUNT, headers.len() as u64);
            }
            HeaderSyncEvent::WireGetHeaders {
                peer,
                start_height,
                count,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("wire_get_headers"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
            }
            HeaderSyncEvent::WireDecodeFailed { peer, error } => {
                insert_optional_str(row, hs_trace::KIND, Some("wire_decode_failed"));
                insert_optional_str(
                    row,
                    hs_trace::ERROR_KIND,
                    Some(header_sync_wire_error_kind(error)),
                );
                insert_peer(row, hs_trace::PEER, peer);
                trace_wire_error_fields(row, error);
            }
            HeaderSyncEvent::WireProtocolFailure {
                peer,
                reason,
                error,
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("wire_protocol_failure"));
                insert_optional_str(
                    row,
                    hs_trace::REASON,
                    Some(misbehavior_reason_label(*reason)),
                );
                insert_optional_str(
                    row,
                    hs_trace::ERROR_KIND,
                    Some(header_sync_wire_error_kind(error)),
                );
                insert_peer(row, hs_trace::PEER, peer);
                trace_wire_error_fields(row, error);
            }
            HeaderSyncEvent::StateFrontiersChanged(frontiers) => {
                insert_optional_str(row, hs_trace::KIND, Some("state_frontiers_changed"));
                insert_height(row, "finalized_height", frontiers.finalized_height);
                insert_height(row, "verified_block_tip", frontiers.verified_block_tip);
            }
            HeaderSyncEvent::VctRootRepairRequested {
                height,
                generation,
                expected_hashes,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("vct_root_repair_requested"));
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_u64(row, hs_trace::RANGE_COUNT, expected_hashes.len() as u64);
                insert_u64(row, "generation", *generation);
            }
            HeaderSyncEvent::VctRootRepairResolved { generation } => {
                insert_optional_str(row, hs_trace::KIND, Some("vct_root_repair_resolved"));
                insert_u64(row, "generation", *generation);
            }
            HeaderSyncEvent::HeaderRangeCommitted {
                start_height,
                tip_height,
                tip_hash,
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_range_committed"));
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(
                    row,
                    hs_trace::RANGE_COUNT,
                    u64::from(count_between(*start_height, *tip_height)),
                );
                insert_height(row, hs_trace::HEIGHT, *tip_height);
                insert_hash(row, hs_trace::HASH, *tip_hash);
            }
            HeaderSyncEvent::HeaderRangeCommitFailed {
                peer,
                start_height,
                count,
                kind,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_range_commit_failed"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
                insert_optional_str(
                    row,
                    hs_trace::REASON,
                    Some(commit_failure_reason_label(*kind)),
                );
            }
            HeaderSyncEvent::HeaderRangeResponseFinished {
                peer,
                start_height,
                requested_count,
                returned_count,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_range_response_finished"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*returned_count));
                insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(*requested_count));
            }
            HeaderSyncEvent::HeaderRangeResponseReady {
                peer,
                start_height,
                requested_count,
                headers,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_range_response_ready"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, headers.len() as u64);
                insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(*requested_count));
            }
        });
    }

    fn trace_action_dispatched(&self, action: &HeaderSyncAction) {
        self.emit_trace(hs_trace::HEADER_ACTION_DISPATCHED, |row| match action {
            #[cfg(test)]
            HeaderSyncAction::SendMessage { peer, msg, .. } => {
                insert_optional_str(row, hs_trace::KIND, Some("send_message"));
                insert_optional_str(row, hs_trace::REASON, Some(header_sync_message_label(msg)));
                insert_peer(row, hs_trace::PEER, peer);
                trace_header_sync_message_fields(row, msg);
            }
            #[cfg(test)]
            HeaderSyncAction::ForwardNewBlock {
                source,
                peer,
                height,
                hash,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("forward_new_block"));
                if let Some(source) = source {
                    insert_peer(row, hs_trace::SOURCE_PEER, source);
                }
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                insert_optional_str(row, hs_trace::KIND, Some("misbehavior"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_optional_str(
                    row,
                    hs_trace::REASON,
                    Some(misbehavior_reason_label(*reason)),
                );
            }
            HeaderSyncAction::NewBlockReceived {
                peer, height, hash, ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_received"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer, start, count, ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("query_headers_by_height_range"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
            }
            HeaderSyncAction::CommitHeaderRange {
                peer,
                start_height,
                headers,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("commit_header_range"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, headers.len() as u64);
            }
            HeaderSyncAction::QueryBestHeaderTip => {
                insert_optional_str(row, hs_trace::KIND, Some("query_best_header_tip"));
            }
            HeaderSyncAction::QueryMissingBlockBodies { from, limit } => {
                insert_optional_str(row, hs_trace::KIND, Some("query_missing_block_bodies"));
                insert_height(row, hs_trace::RANGE_START, *from);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*limit));
            }
            HeaderSyncAction::BodyGaps { from, to } => {
                insert_optional_str(row, hs_trace::KIND, Some("body_gaps"));
                insert_height(row, hs_trace::RANGE_START, *from);
                insert_u64(
                    row,
                    hs_trace::RANGE_COUNT,
                    u64::from(count_between(*from, *to)),
                );
            }
            HeaderSyncAction::HeaderAdvanced { height, hash } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_advanced"));
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncAction::HeaderReanchored { old, new } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_reanchored"));
                insert_height(row, hs_trace::HEIGHT, new.0);
                insert_hash(row, hs_trace::HASH, new.1);
                insert_height(row, hs_trace::RANGE_START, old.0);
            }
        });
    }

    fn trace_status_sent(&self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        self.emit_trace(hs_trace::HEADER_STATUS_SENT, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, status.tip_height);
            insert_hash(row, hs_trace::HASH, status.tip_hash);
            insert_height(row, hs_trace::RANGE_START, status.anchor_height);
            insert_u64(
                row,
                hs_trace::ADVERTISED_CAP,
                u64::from(status.max_headers_per_response),
            );
            insert_u64(
                row,
                hs_trace::IN_FLIGHT_COUNT,
                u64::from(status.max_inflight_requests),
            );
        });
    }

    fn trace_status_received(&self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        self.emit_trace(hs_trace::HEADER_STATUS_RECEIVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, status.tip_height);
            insert_hash(row, hs_trace::HASH, status.tip_hash);
            insert_height(row, hs_trace::RANGE_START, status.anchor_height);
            insert_u64(
                row,
                hs_trace::ADVERTISED_CAP,
                u64::from(status.max_headers_per_response),
            );
            insert_u64(
                row,
                hs_trace::IN_FLIGHT_COUNT,
                u64::from(status.max_inflight_requests),
            );
        });
    }

    fn trace_peer_connected(
        &self,
        peer: &ZakuraPeerId,
        direction: ServicePeerDirection,
        active_connections: usize,
    ) {
        self.emit_trace(hs_trace::HEADER_PEER_CONNECTED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(row, "direction", Some(direction.trace_label()));
            insert_u64(
                row,
                hs_trace::ACTIVE_CONNECTIONS,
                u64::try_from(active_connections).unwrap_or(u64::MAX),
            );
        });
    }

    fn trace_peer_disconnected(&self, peer: &ZakuraPeerId, active_connections: usize) {
        self.emit_trace(hs_trace::HEADER_PEER_DISCONNECTED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_u64(
                row,
                hs_trace::ACTIVE_CONNECTIONS,
                u64::try_from(active_connections).unwrap_or(u64::MAX),
            );
        });
    }

    fn trace_get_headers_sent(
        &self,
        peer: &ZakuraPeerId,
        range: RangeRequest,
        count: u32,
        advertised_cap: u32,
        meta: GetHeadersTraceMeta,
    ) {
        self.emit_trace(hs_trace::HEADER_GET_HEADERS_SENT, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_u64(row, hs_trace::SESSION_ID, meta.session_id);
            insert_u64(
                row,
                hs_trace::STREAM_VERSION,
                u64::from(meta.stream_version),
            );
            insert_u64(row, hs_trace::REQUEST_ID, meta.request_id.get());
            insert_height(row, hs_trace::RANGE_START, range.start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_u64(row, hs_trace::ADVERTISED_CAP, u64::from(advertised_cap));
            insert_bool(row, hs_trace::FINALIZED, range.finalized);
            insert_bool(
                row,
                hs_trace::WANT_TREE_AUX_ROOTS,
                range.want_tree_aux_roots,
            );
            insert_optional_str(row, hs_trace::RANGE_PRIORITY, Some(range.priority.label()));
            insert_height(
                row,
                hs_trace::VERIFIED_BLOCK_TIP,
                self.state.verified_block_tip,
            );
            insert_height(row, hs_trace::FINALIZED_HEIGHT, self.state.finalized_height);
            insert_height(row, hs_trace::BEST_HEADER_TIP, self.state.best_header_tip);
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn trace_headers_received(
        &self,
        peer: &ZakuraPeerId,
        start_height: block::Height,
        count: u32,
        expected_max_count: u32,
        advertised_cap: u32,
        in_flight_count: usize,
        want_tree_aux_roots: bool,
        tree_aux_roots: &[BlockCommitmentRoots],
    ) {
        self.emit_trace(hs_trace::HEADER_HEADERS_RECEIVED, |row| {
            let roots = TreeAuxTraceSummary::new(tree_aux_roots);
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_u64(row, hs_trace::ADVERTISED_CAP, u64::from(advertised_cap));
            insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(expected_max_count));
            insert_u64(row, hs_trace::IN_FLIGHT_COUNT, in_flight_count as u64);
            insert_bool(row, hs_trace::WANT_TREE_AUX_ROOTS, want_tree_aux_roots);
            insert_u64(row, hs_trace::TREE_AUX_ROOTS_LEN, u64::from(roots.len));
            roots.insert_into(row);
        });
    }

    fn trace_headers_served(
        &self,
        peer: &ZakuraPeerId,
        start_height: block::Height,
        requested_count: u32,
        returned_count: u32,
        want_tree_aux_roots: bool,
        roots: TreeAuxTraceSummary,
    ) {
        self.emit_trace(hs_trace::HEADER_HEADERS_SERVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(returned_count));
            insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(requested_count));
            insert_bool(row, hs_trace::WANT_TREE_AUX_ROOTS, want_tree_aux_roots);
            insert_u64(row, hs_trace::TREE_AUX_ROOTS_LEN, u64::from(roots.len));
            roots.insert_into(row);
        });
    }

    fn trace_range_event(
        &self,
        event: &'static str,
        start_height: block::Height,
        count: u32,
        peer: Option<&ZakuraPeerId>,
        reason: Option<&'static str>,
    ) {
        self.emit_trace(event, |row| {
            if let Some(peer) = peer {
                insert_peer(row, hs_trace::PEER, peer);
            }
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_optional_str(row, hs_trace::REASON, reason);
        });
    }

    fn trace_range_validation_rejected(
        &self,
        peer: &ZakuraPeerId,
        range: RangeRequest,
        count: u32,
        validation_stage: &'static str,
        error_kind: &'static str,
    ) {
        self.emit_trace(hs_trace::HEADER_RANGE_REJECTED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, range.start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_hash(row, hs_trace::ANCHOR_HASH, range.anchor_hash);
            insert_optional_str(row, hs_trace::VALIDATION_STAGE, Some(validation_stage));
            insert_optional_str(row, hs_trace::ERROR_KIND, Some(error_kind));
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(
                    HeaderSyncMisbehavior::InvalidRange,
                )),
            );
        });
    }

    fn trace_new_block_received(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    ) {
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_RECEIVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
        });
    }

    fn trace_new_block_forwarded(
        &self,
        source: &ZakuraPeerId,
        destination: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        destination_count: usize,
    ) {
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_FORWARDED, |row| {
            insert_peer(row, hs_trace::SOURCE_PEER, source);
            insert_peer(row, hs_trace::PEER, destination);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
            insert_u64(
                row,
                hs_trace::DESTINATION_PEER_COUNT,
                destination_count as u64,
            );
        });
    }

    fn trace_new_block_deduped(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        reason: &'static str,
    ) {
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_DEDUPED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
            insert_optional_str(row, hs_trace::REASON, Some(reason));
        });
    }

    fn trace_peer_violation(&self, peer: &ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        self.emit_trace(hs_trace::HEADER_PEER_VIOLATION, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(reason)),
            );
        });
    }

    fn trace_peer_violation_recorded(&self, peer: &ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        self.emit_trace(hs_trace::HEADER_PEER_VIOLATION_RECORDED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(reason)),
            );
        });
    }

    fn trace_frontier_advanced(&self, height: block::Height, hash: block::Hash) {
        self.emit_trace(hs_trace::HEADER_FRONTIER_ADVANCED, |row| {
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
        });
    }

    fn trace_frontier_reanchored(&self, height: block::Height, hash: block::Hash) {
        self.emit_trace(hs_trace::HEADER_FRONTIER_REANCHORED, |row| {
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
        });
    }

    fn trace_missing_bodies(&self, from: block::Height, to: block::Height) {
        self.emit_trace(hs_trace::HEADER_MISSING_BODIES_REPORTED, |row| {
            insert_height(row, hs_trace::RANGE_START, from);
            insert_u64(
                row,
                hs_trace::RANGE_COUNT,
                u64::from(count_between(from, to)),
            );
        });
    }

    fn trace_queue_send_failed(
        &self,
        peer: &ZakuraPeerId,
        message: &'static str,
        error: &OrderedSendError,
        queue_capacity: usize,
        queue_max_capacity: usize,
        build: impl FnOnce(&mut serde_json::Map<String, Value>),
    ) {
        self.startup.trace.emit_with(QUEUE_SEND_TABLE, |row| {
            row.insert(
                qs_trace::EVENT.to_string(),
                Value::String(qs_trace::QUEUE_SEND_FAILED.to_string()),
            );
            insert_optional_str(row, qs_trace::SERVICE, Some("header_sync"));
            insert_optional_str(row, qs_trace::MESSAGE, Some(message));
            insert_peer(row, qs_trace::PEER, peer);
            insert_optional_str(row, qs_trace::ERROR, Some(ordered_send_error_label(error)));
            insert_u64(
                row,
                qs_trace::QUEUE_CAPACITY,
                u64::try_from(queue_capacity).unwrap_or(u64::MAX),
            );
            insert_u64(
                row,
                qs_trace::QUEUE_MAX_CAPACITY,
                u64::try_from(queue_max_capacity).unwrap_or(u64::MAX),
            );
            build(row);
        });
    }

    fn emit_trace(
        &self,
        event: &'static str,
        build: impl FnOnce(&mut serde_json::Map<String, Value>),
    ) {
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.to_string(),
                Value::String(event.to_string()),
            );
            build(row);
        });
    }

    fn local_status(&self) -> HeaderSyncStatus {
        HeaderSyncStatus {
            tip_height: self.state.best_header_tip,
            tip_hash: self.state.best_header_hash,
            anchor_height: self.state.anchor.0,
            max_headers_per_response: self.startup.config.advertised_max_headers_per_response(),
            max_inflight_requests: self.startup.config.advertised_max_inflight_requests(),
        }
    }

    /// Retire outstanding sync ranges the schedule has since covered elsewhere.
    ///
    /// Retiring the request ID is sufficient: a response that arrives after its range
    /// was covered is matched to the retired ID and dropped, so it cannot be mistaken
    /// for a newer request or trigger a spurious link failure. The stream stays up.
    fn cancel_covered_outstanding(&mut self) {
        for peer in self.state.peers.values_mut() {
            let mut index = 0;
            while index < peer.outstanding.len() {
                if self
                    .state
                    .schedule
                    .is_covered(peer.outstanding[index].range)
                    && matches!(peer.outstanding[index].purpose, RangePurpose::Sync)
                {
                    let outstanding = peer.outstanding.remove(index);
                    let _ = peer.session.retire_expected_headers(outstanding.request_id);
                } else {
                    index += 1;
                }
            }
        }
    }

    /// Retire outstanding forward ranges dropped by a re-anchor, as in
    /// [`Self::cancel_covered_outstanding`].
    fn cancel_forward_outstanding(&mut self) {
        for peer in self.state.peers.values_mut() {
            let mut index = 0;
            while index < peer.outstanding.len() {
                if peer.outstanding[index].range.priority == RangePriority::Forward {
                    let outstanding = peer.outstanding.remove(index);
                    let _ = peer.session.retire_expected_headers(outstanding.request_id);
                } else {
                    index += 1;
                }
            }
        }
    }
}

fn set_header_connectivity_gauges(connected_peers: usize, healthy_peers: usize) {
    // Active Zakura reactor sessions are bounded by the configured connection
    // limit, far below f64's exact integer range.
    metrics::gauge!("zakura.p2p.reactor.active_connections", "reactor" => "header_sync")
        .set(connected_peers as f64);
    metrics::gauge!("zakura.p2p.connected_peers").set(connected_peers as f64);
    metrics::gauge!("zakura.p2p.healthy_peers").set(healthy_peers as f64);
}

#[derive(Default)]
struct TreeAuxTraceSummary {
    len: u32,
    first_height: Option<block::Height>,
    last_height: Option<block::Height>,
}

impl TreeAuxTraceSummary {
    fn new(roots: &[BlockCommitmentRoots]) -> Self {
        Self {
            len: u32::try_from(roots.len()).unwrap_or(u32::MAX),
            first_height: roots.first().map(|root| root.height),
            last_height: roots.last().map(|root| root.height),
        }
    }

    fn insert_into(&self, row: &mut serde_json::Map<String, Value>) {
        if let Some(height) = self.first_height {
            insert_height(row, hs_trace::FIRST_ROOT_HEIGHT, height);
        }
        if let Some(height) = self.last_height {
            insert_height(row, hs_trace::LAST_ROOT_HEIGHT, height);
        }
    }
}

fn record_wire_validation_metrics(error: &HeaderSyncWireError) {
    let error_kind = header_sync_wire_error_kind(error);
    metrics::counter!(
        "sync.header.validation.rejected",
        "stage" => "wire",
        "error_kind" => error_kind
    )
    .increment(1);
    if matches!(error, HeaderSyncWireError::TreeAuxRootHeightMismatch { .. }) {
        metrics::counter!("sync.header.tree_aux.height_mismatch").increment(1);
    }
}

fn trace_wire_error_fields(row: &mut serde_json::Map<String, Value>, error: &HeaderSyncWireError) {
    if let HeaderSyncWireError::TreeAuxRootHeightMismatch {
        offset,
        expected_height,
        root_height,
        first_root_height,
        last_root_height,
    } = error
    {
        insert_u64(
            row,
            hs_trace::ROOT_MISMATCH_OFFSET,
            u64::try_from(*offset).unwrap_or(u64::MAX),
        );
        insert_height(row, hs_trace::EXPECTED_ROOT_HEIGHT, *expected_height);
        insert_height(row, hs_trace::ACTUAL_ROOT_HEIGHT, *root_height);
        insert_height(row, hs_trace::FIRST_ROOT_HEIGHT, *first_root_height);
        insert_height(row, hs_trace::LAST_ROOT_HEIGHT, *last_root_height);
    }
}

fn header_sync_wire_error_kind(error: &HeaderSyncWireError) -> &'static str {
    match error {
        HeaderSyncWireError::OversizedPayload { .. } => "oversized_payload",
        HeaderSyncWireError::HeaderCountLimit { .. } => "header_count_limit",
        HeaderSyncWireError::BodySizeCountMismatch { .. } => "body_size_count_mismatch",
        HeaderSyncWireError::TreeAuxRootCountMismatch { .. } => "tree_aux_root_count_mismatch",
        HeaderSyncWireError::TreeAuxRootHeightMismatch { .. } => "tree_aux_root_height_mismatch",
        HeaderSyncWireError::InvalidBoolMarker { .. } => "invalid_bool_marker",
        HeaderSyncWireError::UnrequestedTreeAuxRoots => "unrequested_tree_aux_roots",
        HeaderSyncWireError::UnsolicitedHeaders => "unsolicited_headers",
        HeaderSyncWireError::MissingRequestId { .. } => "missing_request_id",
        HeaderSyncWireError::ZeroHeaderRequestCount => "zero_header_request_count",
        HeaderSyncWireError::HeightOutOfRange(_) => "height_out_of_range",
        HeaderSyncWireError::UnknownMessageType(_) => "unknown_message_type",
        HeaderSyncWireError::UnknownFrameMessageType(_) => "unknown_frame_message_type",
        HeaderSyncWireError::UnsupportedFlags(_) => "unsupported_flags",
        HeaderSyncWireError::MismatchedFrameMessageType { .. } => "mismatched_frame_message_type",
        HeaderSyncWireError::TrailingBytes => "trailing_bytes",
        HeaderSyncWireError::NonContiguousHeaders => "non_contiguous_headers",
        HeaderSyncWireError::FirstHeaderDoesNotLink => "first_header_does_not_link",
        HeaderSyncWireError::WrongEquihashSolutionSize => "wrong_equihash_solution_size",
        HeaderSyncWireError::InvalidDifficultyThreshold => "invalid_difficulty_threshold",
        HeaderSyncWireError::DifficultyFilter { .. } => "difficulty_filter",
        HeaderSyncWireError::NumericOverflow(_) => "numeric_overflow",
        HeaderSyncWireError::Io(_) => "io",
        HeaderSyncWireError::Serialization(_) => "serialization",
        HeaderSyncWireError::Time(_) => "time",
        HeaderSyncWireError::Equihash(_) => "equihash",
        HeaderSyncWireError::BlockingTask(_) => "blocking_task",
    }
}

fn header_sync_candidate_target(best_header_tip: block::Height) -> block::Height {
    next_height(best_header_tip).unwrap_or(best_header_tip)
}

fn header_summary_is_useful(
    summary: HeaderSyncServiceSummary,
    target_height: block::Height,
) -> bool {
    summary.serving_headers
        && summary.inbound_slots_free > 0
        && summary.best_height >= target_height
}

fn node_id_from_header_peer_id(peer: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

fn trace_header_sync_message_fields(
    row: &mut serde_json::Map<String, Value>,
    msg: &HeaderSyncMessage,
) {
    match msg {
        HeaderSyncMessage::Status(status) => {
            insert_height(row, hs_trace::HEIGHT, status.tip_height);
            insert_hash(row, hs_trace::HASH, status.tip_hash);
            insert_height(row, hs_trace::RANGE_START, status.anchor_height);
            insert_u64(
                row,
                hs_trace::ADVERTISED_CAP,
                u64::from(status.max_headers_per_response),
            );
            insert_u64(
                row,
                hs_trace::IN_FLIGHT_COUNT,
                u64::from(status.max_inflight_requests),
            );
        }
        HeaderSyncMessage::Headers { headers, .. } => {
            insert_u64(
                row,
                hs_trace::RANGE_COUNT,
                u64::try_from(headers.len()).unwrap_or(u64::MAX),
            );
        }
        HeaderSyncMessage::GetHeaders {
            start_height,
            count,
            ..
        } => {
            insert_height(row, hs_trace::RANGE_START, *start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
        }
        HeaderSyncMessage::NewBlock(block) => {
            insert_hash(row, hs_trace::HASH, block.hash());
            if let Some(height) = block.coinbase_height() {
                insert_height(row, hs_trace::HEIGHT, height);
            }
        }
    }
}

fn header_sync_message_label(msg: &HeaderSyncMessage) -> &'static str {
    match msg {
        HeaderSyncMessage::Status(_) => "status",
        HeaderSyncMessage::Headers { .. } => "headers",
        HeaderSyncMessage::GetHeaders { .. } => "get_headers",
        HeaderSyncMessage::NewBlock(_) => "new_block",
    }
}
