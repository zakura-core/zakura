use super::{
    error::*,
    events::*,
    requester::{HeaderRequesterHandle, HeaderRequesterId},
    validation::*,
    wire::*,
    work_queue::*,
    *,
};
use crate::zakura::{
    HeaderSyncServiceSummary, ServicePeerDirection, DEFAULT_LIVE_SERVICE_SUMMARY_TTL,
};

pub(super) const HEADER_SYNC_ADVISORY_BACKOFF_FAILURES: u32 = 2;
pub(super) const HEADER_SYNC_ADVISORY_BACKOFF: Duration = Duration::from_secs(60);
pub(super) const HEADER_SYNC_ADVISORY_TTL: Duration = DEFAULT_LIVE_SERVICE_SUMMARY_TTL;
pub(super) const HEADER_SYNC_STALE_ANCHOR_LINK_FAILURES: u32 = 3;
pub(super) const HEADER_SYNC_STALE_ANCHOR_DISTINCT_PEERS: usize = 2;
pub(super) const VCT_ROOT_REPAIR_MAX_ATTEMPTS: usize = 6;
pub(super) const VCT_ROOT_REPAIR_MAX_WALL_TIME: Duration = Duration::from_secs(240);
pub(super) const RETAINED_ROOT_LOCAL_MAX_ATTEMPTS: u32 = 6;
pub(super) const RETAINED_ROOT_LOCAL_MAX_WALL_TIME: Duration = Duration::from_secs(240);
pub(super) const ROOT_AUTH_MIN_BODY_LEAD: u32 = 400;
pub(super) const VCT_ROOT_REPAIR_BACKOFFS: [Duration; VCT_ROOT_REPAIR_MAX_ATTEMPTS] = [
    Duration::from_secs(0),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
];

#[derive(Clone, Debug)]
pub(super) struct HeaderSyncCore {
    pub(super) anchor: (block::Height, block::Hash),
    pub(super) finalized_height: block::Height,
    pub(super) verified_block_tip: block::Height,
    pub(super) verified_block_hash: block::Hash,
    pub(super) best_header_tip: block::Height,
    pub(super) best_header_hash: block::Hash,
    pub(super) body_sync_target: (block::Height, block::Hash),
    pub(super) header_root_auth: Option<HeaderRootAuthState>,
    pub(super) root_auth_waiting_for_watch: bool,
    pub(super) last_header_progress_at: Instant,
    pub(super) peers: HashMap<ZakuraPeerId, PeerHeaderState>,
    pub(super) parked_peers: HashSet<ZakuraPeerId>,
    pub(super) seen: HeaderHashDedup,
    pub(super) pending_new_blocks: HashSet<block::Hash>,
    pub(super) schedule: HeaderWorkQueue,
    pub(super) buffered: BTreeMap<(RangePriority, block::Height), BufferedHeaderRange>,
    pub(super) pending_operations: HashMap<HeaderSyncOperationIdentity, PendingOperation>,
    pub(super) retained_roots: BTreeMap<block::Height, RetainedRootPayload>,
    pub(super) repair: Option<VctRootRepair>,
    pub(super) advisory: HashMap<ZakuraPeerId, HeaderSyncAdvisoryPeerState>,
    pub(super) stale_anchor: StaleAnchorFailures,
}

impl HeaderSyncCore {
    pub(super) fn new(startup: &HeaderSyncStartup) -> Result<Self, HeaderSyncStartError> {
        validate_anchor(&startup.network, startup.anchor)?;
        if startup.anchor.0 > startup.frontiers.verified_block_tip {
            return Err(HeaderSyncStartError::AnchorAboveVerifiedBlockTip {
                anchor_height: startup.anchor.0,
                verified_block_tip: startup.frontiers.verified_block_tip,
            });
        }
        // Header range commits can only anchor to the durable header view. In
        // particular, a restored non-finalized body tip can be ahead of that
        // view, so using the verified-body frontier here would make the first
        // post-restart range fail with `UnknownAnchor`.
        let (best_header_tip, best_header_hash) = startup.best_header_tip.unwrap_or(startup.anchor);
        let body_sync_target =
            startup
                .header_root_auth
                .map_or((best_header_tip, best_header_hash), |_| {
                    (
                        startup.frontiers.verified_block_tip,
                        startup.frontiers.verified_block_hash,
                    )
                });

        Ok(Self {
            anchor: startup.anchor,
            finalized_height: startup.frontiers.finalized_height,
            verified_block_tip: startup.frontiers.verified_block_tip,
            verified_block_hash: startup.frontiers.verified_block_hash,
            best_header_tip,
            best_header_hash,
            body_sync_target,
            header_root_auth: startup.header_root_auth,
            root_auth_waiting_for_watch: false,
            last_header_progress_at: Instant::now(),
            peers: HashMap::new(),
            parked_peers: HashSet::new(),
            seen: HeaderHashDedup::default(),
            pending_new_blocks: HashSet::new(),
            schedule: HeaderWorkQueue::new(),
            buffered: BTreeMap::new(),
            pending_operations: HashMap::new(),
            retained_roots: BTreeMap::new(),
            repair: None,
            advisory: HashMap::new(),
            stale_anchor: StaleAnchorFailures::default(),
        })
    }

    pub(super) fn refresh_forward_range(&mut self, startup: &HeaderSyncStartup) {
        let best_peer_tip = self
            .peers
            .values()
            .filter(|peer| peer.received_status)
            .map(|peer| peer.advertised_tip)
            .max()
            .unwrap_or(self.best_header_tip);
        if best_peer_tip <= self.best_header_tip {
            return;
        }

        let checkpoints = startup.network.checkpoint_list();
        let handoff = checkpoints.max_height();
        let retain_roots = self.header_root_auth.is_some();
        let Some(canonical_start) = next_height(self.best_header_tip) else {
            return;
        };
        let start = if retain_roots
            && self.best_header_tip < handoff
            && self
                .retained_roots
                .values()
                .any(|retained| retained.payload.range().end() == self.best_header_tip)
        {
            self.best_header_tip
        } else {
            canonical_start
        };
        let mut end = best_peer_tip;
        let mut finalized = false;
        if let Some(first_checkpoint) = checkpoints.min_height_in_range(block::Height(1)..) {
            if self.best_header_tip < first_checkpoint {
                if best_peer_tip < first_checkpoint {
                    return;
                }
                end = first_checkpoint;
                finalized = true;
            }
        }

        let batch_count = clamp_header_sync_request_count(
            startup.config.advertised_max_headers_per_response(),
            startup.config.advertised_max_headers_per_response(),
            &startup.network,
            startup.max_frame_bytes,
            true,
        );
        let resident_height_cap =
            u64::from(batch_count.saturating_mul(HEADER_SYNC_MAX_RESIDENT_BATCHES));
        let available = resident_height_cap.saturating_sub(self.schedule.resident_heights());
        let Ok(available) = u32::try_from(available) else {
            return;
        };
        if available == 0 {
            return;
        }
        let scheduled_end = self.schedule.highest_end(RangePriority::Forward);
        let batch_start = scheduled_end
            .and_then(|end| {
                if retain_roots && end < handoff {
                    Some(end)
                } else {
                    next_height(end)
                }
            })
            .unwrap_or(start)
            .max(start);
        if batch_start > end {
            return;
        }
        let mut remaining = available;
        let mut batch_start = batch_start;
        let mut repeats_boundary = retain_roots
            && (scheduled_end == Some(batch_start) || batch_start == self.best_header_tip);
        let mut anchor_hash = (batch_start == canonical_start).then_some(self.best_header_hash);
        while remaining > 0 {
            let mut batch_len = remaining
                .min(batch_count)
                .min(count_between(batch_start, end));
            let mut batch_end = range_end_height(batch_start, batch_len)
                .expect("bounded header work batch has an end height");
            if batch_start <= handoff && batch_end > handoff {
                batch_end = handoff;
                batch_len = count_between(batch_start, batch_end);
            }
            // One-height retained-root overlap batches must not query checkpoints:
            // `next_height(batch_start)..=batch_end` would be inverted and panic.
            if repeats_boundary && batch_len < 2 {
                break;
            }
            let checkpoint_start = if repeats_boundary {
                next_height(batch_start)
            } else {
                Some(batch_start)
            };
            if let Some(checkpoint) = checkpoint_start
                .filter(|start| *start <= batch_end)
                .and_then(|start| checkpoints.min_height_in_range(start..=batch_end))
            {
                batch_len = count_between(batch_start, checkpoint);
            }
            let range = CheckedHeaderRange::from_count(batch_start, batch_len)
                .expect("bounded non-empty batch has checked geometry");
            // Non-empty Headers responses are all-or-nothing on the wire: root
            // count must match header count. Requesting without roots makes the
            // serve path clear any non-empty reply, so post-handoff forward sync
            // would stall forever at the final checkpoint tip.
            self.schedule.ensure_forward(RangeRequest {
                range,
                anchor_hash,
                finalized,
                want_tree_aux_roots: true,
                priority: RangePriority::Forward,
            });
            remaining = remaining.saturating_sub(batch_len);
            if range.end() >= end {
                break;
            }
            let next_start = if retain_roots {
                range.continuation_start(handoff)
            } else {
                next_height(range.end())
            };
            let Some(next_start) = next_start else {
                break;
            };
            repeats_boundary = retain_roots && next_start == range.end();
            batch_start = next_start;
            anchor_hash = None;
        }
    }

    pub(super) fn refresh_root_auth_range(&mut self, startup: &HeaderSyncStartup) {
        if self.root_auth_waiting_for_watch {
            return;
        }
        let Some(auth) = self.header_root_auth else {
            return;
        };
        let Some(start) = next_height(auth.authenticated_height) else {
            return;
        };
        if self.root_auth_coverage_expected_from_forward(start)
            || self.retained_roots.contains_key(&start)
        {
            return;
        }
        let end = self.root_auth_fallback_end(startup, auth, start);
        let batch_count = clamp_header_sync_request_count(
            startup.config.advertised_max_headers_per_response(),
            startup.config.advertised_max_headers_per_response(),
            &startup.network,
            startup.max_frame_bytes,
            true,
        );
        if batch_count < 2 {
            return;
        }

        let resident_cap = u64::from(batch_count.saturating_mul(HEADER_SYNC_MAX_RESIDENT_BATCHES));
        let available = resident_cap.saturating_sub(
            self.schedule
                .resident_heights_for(RangePriority::AuthenticateRoots),
        );
        let Ok(mut remaining) = u32::try_from(available) else {
            return;
        };
        if remaining < 2 {
            return;
        }

        let mut batch_start = self
            .schedule
            .highest_end(RangePriority::AuthenticateRoots)
            .unwrap_or(start)
            .max(start);
        let mut added = 0u64;
        while batch_start < end && remaining >= 2 {
            let count = count_between(batch_start, end)
                .min(batch_count)
                .min(remaining);
            if count < 2 {
                break;
            }
            let range = CheckedHeaderRange::from_count(batch_start, count)
                .expect("bounded root-authentication batch has checked geometry");
            if self.schedule.ensure(
                RangeRequest {
                    range,
                    anchor_hash: (batch_start == start).then_some(auth.authenticated_hash),
                    finalized: true,
                    want_tree_aux_roots: true,
                    priority: RangePriority::AuthenticateRoots,
                },
                RangePriority::AuthenticateRoots,
            ) {
                added = added.saturating_add(1);
            }
            remaining = remaining.saturating_sub(count);
            if range.end() >= end {
                break;
            }
            batch_start = range.end();
        }
        if added == 0 {
            return;
        }

        metrics::counter!("sync.header.root_auth.retain.miss").increment(1);
        metrics::counter!("sync.header.root_auth.fallback.requested", "reason" => "missing")
            .increment(1);
        metrics::counter!("sync.header.root_auth.fallback.prefetched").increment(added);
    }

    pub(super) fn root_auth_hole_heights(
        &self,
        startup: &HeaderSyncStartup,
        auth: HeaderRootAuthState,
    ) -> u32 {
        let Some(start) = next_height(auth.authenticated_height) else {
            return 0;
        };
        self.root_auth_fallback_end(startup, auth, start)
            .0
            .saturating_sub(start.0)
    }

    fn root_auth_fallback_end(
        &self,
        startup: &HeaderSyncStartup,
        auth: HeaderRootAuthState,
        start: block::Height,
    ) -> block::Height {
        let mut end = auth
            .completed_checkpoint_height
            .min(self.best_header_tip)
            .min(startup.network.checkpoint_list().max_height());
        // Auth can catch the completed-checkpoint tip (or tip can lag), so the
        // next height is past every fallback bound. An inverted range has no
        // hole to fill and must not be passed to `BTreeMap::range`.
        if start > end {
            return end;
        }
        if let Some((&retained_start, _)) = self.retained_roots.range(start..=end).next() {
            end = retained_start;
        }
        end
    }

    fn root_auth_coverage_expected_from_forward(&self, start: block::Height) -> bool {
        self.buffered.values().any(|buffered| {
            buffered.range.priority == RangePriority::Forward
                && buffered.range.want_tree_aux_roots
                && buffered.range.start_height() == start
        }) || self.pending_operations.values().any(|pending| {
            pending.range.priority == RangePriority::Forward
                && pending
                    .retention_candidate
                    .as_ref()
                    .is_some_and(|payload| payload.range().start() == start)
        }) || self
            .schedule
            .forward
            .iter()
            .any(|range| range.want_tree_aux_roots && range.start_height() == start)
            || self.schedule.active.keys().any(|range| {
                range.priority == RangePriority::Forward
                    && range.want_tree_aux_roots
                    && range.start_height() == start
            })
    }

    pub(super) fn admit_retained_root_payload(
        &mut self,
        wire_request: HeaderSyncWireRequestIdentity,
        payload: HeaderRangePayload,
    ) -> bool {
        // Retention requires live auth state: without it no consumption or pruning
        // path runs, so admitted payloads would accumulate unboundedly, and the
        // eventual `None -> Some` watch transition clears the retained store anyway.
        if self.header_root_auth.is_none() {
            return false;
        }
        if !payload.has_tree_aux_roots() || payload.range().count() < 2 {
            return false;
        }
        let start = payload.range().start();
        if self
            .retained_roots
            .get(&start)
            .is_some_and(|retained| retained.payload.range().end() >= payload.range().end())
        {
            return false;
        }
        if self.retained_roots.contains_key(&start) {
            metrics::counter!(
                "sync.header.root_auth.retain.dropped",
                "reason" => "same_start_replaced"
            )
            .increment(1);
        }
        // A newly retained batch can have different geometry than speculative
        // fallback batches queued beyond the same reconnect point.
        self.schedule.discard_root_auth_after(start);
        self.schedule.discard_root_auth_at(start);
        self.buffered
            .remove(&(RangePriority::AuthenticateRoots, start));
        self.retained_roots
            .insert(start, RetainedRootPayload::new(wire_request, payload));
        metrics::counter!("sync.header.root_auth.retain.admitted").increment(1);
        true
    }

    pub(super) fn retained_heights(&self) -> u64 {
        self.retained_roots
            .values()
            .map(|retained| u64::from(retained.payload.range().count()))
            .sum()
    }

    pub(super) fn retained_ready(
        &self,
        auth: HeaderRootAuthState,
        now: Instant,
    ) -> Option<(block::Height, &RetainedRootPayload)> {
        let start = next_height(auth.authenticated_height)?;
        let retained = self.retained_roots.get(&start)?;
        (retained.payload.range().count() >= 2
            && retained.payload.range().end() <= auth.completed_checkpoint_height
            && !retained.authenticating
            && !retained.local_retry_exhausted
            && retained.retry_at.is_none_or(|retry_at| retry_at <= now))
        .then_some((start, retained))
    }

    pub(super) fn retained_retry_deadline(&self) -> Option<Instant> {
        self.retained_roots
            .values()
            .filter(|retained| !retained.authenticating)
            .filter_map(|retained| retained.retry_at)
            .min()
    }

    pub(super) fn prune_retained_before(
        &mut self,
        next_start: block::Height,
        reason: &'static str,
    ) {
        let before = self.retained_roots.len();
        self.retained_roots.retain(|start, _| *start >= next_start);
        let dropped = before.saturating_sub(self.retained_roots.len());
        if dropped > 0 {
            metrics::counter!(
                "sync.header.root_auth.retain.dropped",
                "reason" => reason
            )
            .increment(dropped as u64);
        }
    }

    pub(super) fn clear_retained_roots(&mut self, reason: &'static str) {
        let dropped = self.retained_roots.len();
        self.retained_roots.clear();
        if dropped > 0 {
            metrics::counter!(
                "sync.header.root_auth.retain.dropped",
                "reason" => reason
            )
            .increment(dropped as u64);
        }
    }

    pub(super) fn remove_retained_root(
        &mut self,
        start: block::Height,
        reason: &'static str,
    ) -> Option<RetainedRootPayload> {
        let removed = self.retained_roots.remove(&start);
        if removed.is_some() {
            metrics::counter!(
                "sync.header.root_auth.retain.dropped",
                "reason" => reason
            )
            .increment(1);
        }
        removed
    }

    pub(super) fn retained_root_owned_by(
        &self,
        start: block::Height,
        wire_request: &HeaderSyncWireRequestIdentity,
    ) -> bool {
        self.retained_roots
            .get(&start)
            .is_some_and(|retained| &retained.wire_request == wire_request)
    }

    pub(super) fn remove_retained_root_if_owned(
        &mut self,
        start: block::Height,
        wire_request: &HeaderSyncWireRequestIdentity,
        reason: &'static str,
    ) -> Option<RetainedRootPayload> {
        self.retained_root_owned_by(start, wire_request)
            .then(|| self.remove_retained_root(start, reason))
            .flatten()
    }

    pub(super) fn drop_retained_from(&mut self, height: block::Height, reason: &'static str) {
        let before = self.retained_roots.len();
        self.retained_roots
            .retain(|start, retained| *start < height && retained.payload.range().end() < height);
        let dropped = before.saturating_sub(self.retained_roots.len());
        if dropped > 0 {
            metrics::counter!(
                "sync.header.root_auth.retain.dropped",
                "reason" => reason
            )
            .increment(dropped as u64);
        }
    }

    /// Drop in-flight `AuthenticateRoots` ops after durable frontier movement.
    ///
    /// On real auth-tip advancement, mark ranges complete. Otherwise retire them
    /// so the schedule slot is freed without claiming success (e.g. rebase).
    pub(super) fn clear_inflight_root_auth(&mut self, auth_advanced: bool) {
        self.clear_inflight_root_auth_where(auth_advanced, |_| true);
    }

    /// Complete only root-auth operations whose driver completion was observed.
    ///
    /// The durable state watch can advance before the driver releases its serial
    /// authentication task. Keeping unobserved operations pending prevents the
    /// reactor from admitting the next state operation into that occupied slot.
    pub(super) fn clear_completed_inflight_root_auth(&mut self) {
        self.clear_inflight_root_auth_where(true, |pending| pending.completion_observed);
    }

    fn clear_inflight_root_auth_where(
        &mut self,
        auth_advanced: bool,
        should_clear: impl Fn(&PendingOperation) -> bool,
    ) {
        let auth_operations: Vec<_> = self
            .pending_operations
            .iter()
            .filter_map(|(operation, pending)| {
                (operation.op_kind == HeaderSyncOperationKind::AuthenticateRoots
                    && should_clear(pending))
                .then_some((operation.clone(), pending.range))
            })
            .collect();
        for (operation, range) in auth_operations {
            if let Some(pending) = self.pending_operations.remove(&operation) {
                if let Some(RootAuthSource::Retained(start)) =
                    pending.root_auth.map(|auth| auth.source)
                {
                    if let Some(retained) = self.retained_roots.get_mut(&start) {
                        retained.authenticating = false;
                    }
                }
            }
            if auth_advanced {
                self.schedule.complete(range);
            } else {
                self.schedule.retire_operation(&operation, range);
            }
        }
    }

    /// Keep scheduled/buffered root-auth work that still sits after the
    /// authenticated tip and within the completed checkpoint bracket.
    pub(super) fn prune_root_auth_pipeline(
        &mut self,
        auth: HeaderRootAuthState,
        auth_advanced: bool,
    ) {
        let next_start =
            next_height(auth.authenticated_height).unwrap_or(auth.authenticated_height);
        if auth_advanced {
            self.schedule.prune_root_auth_before(next_start);
            self.prune_retained_before(next_start, "frontier_advanced");
        }
        let before = self.buffered.len();
        self.buffered.retain(|(priority, start), buffered| {
            *priority != RangePriority::AuthenticateRoots
                || (*start >= next_start
                    && buffered.range.end_height() <= auth.completed_checkpoint_height)
        });
        let _dropped = before.saturating_sub(self.buffered.len());
    }

    /// Discard the whole root-auth schedule lane and buffered fallback after a
    /// rebase or cleared authentication state.
    pub(super) fn discard_root_auth_pipeline(&mut self) {
        self.schedule.clear_root_auth();
        self.clear_retained_roots("frontier_rebased");
        let before = self.buffered.len();
        self.buffered
            .retain(|(priority, _), _| *priority != RangePriority::AuthenticateRoots);
        let _dropped = before.saturating_sub(self.buffered.len());
    }

    pub(super) fn retire_peer_session_auth(
        &mut self,
        peer: &ZakuraPeerId,
        session_id: Option<u64>,
    ) {
        let mut retired = Vec::new();
        for (operation, pending) in &mut self.pending_operations {
            if &operation.wire_request.peer != peer
                || session_id.is_some_and(|session| operation.wire_request.session_id != session)
            {
                continue;
            }
            if operation.op_kind == HeaderSyncOperationKind::AuthenticateRoots {
                retired.push((operation.clone(), pending.range));
            } else if operation.op_kind == HeaderSyncOperationKind::CommitHeaders {
                pending.retention_candidate = None;
            }
        }
        for (operation, range) in retired {
            self.pending_operations.remove(&operation);
            self.schedule.retire_operation(&operation, range);
            self.schedule.retry(range);
        }

        let buffered_keys: Vec<_> = self
            .buffered
            .iter()
            .filter_map(|(key, buffered)| {
                (key.0 == RangePriority::AuthenticateRoots
                    && &buffered.wire_request.peer == peer
                    && session_id.is_none_or(|session| buffered.wire_request.session_id == session))
                .then_some(*key)
            })
            .collect();
        for key in buffered_keys {
            if let Some(buffered) = self.buffered.remove(&key) {
                self.schedule.retry(buffered.range);
            }
        }
        let before = self.retained_roots.len();
        self.retained_roots.retain(|_, retained| {
            &retained.wire_request.peer != peer
                || session_id.is_some_and(|session| retained.wire_request.session_id != session)
        });
        let dropped = before.saturating_sub(self.retained_roots.len());
        if dropped > 0 {
            metrics::counter!(
                "sync.header.root_auth.retain.dropped",
                "reason" => "session_retired"
            )
            .increment(dropped as u64);
        }
    }

    pub(super) fn retire_stale_auth_operation(&mut self, operation: &HeaderSyncOperationIdentity) {
        if let Some(pending) = self.pending_operations.remove(operation) {
            if let Some(RootAuthSource::Retained(start)) = pending.root_auth.map(|auth| auth.source)
            {
                self.remove_retained_root(start, "session_retired");
            }
            self.schedule.retire_operation(operation, pending.range);
            self.schedule.retry(pending.range);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct VctRootRepair {
    pub(super) height: block::Height,
    pub(super) generation: u64,
    pub(super) range: RangeRequest,
    pub(super) expected_hashes: Vec<(block::Height, block::Hash)>,
    pub(super) tried_peers: HashSet<ZakuraPeerId>,
    pub(super) in_flight: Option<ZakuraPeerId>,
    pub(super) started_at: Instant,
    pub(super) next_attempt_at: Instant,
    pub(super) exhausted: bool,
}

impl VctRootRepair {
    pub(super) fn new(
        height: block::Height,
        generation: u64,
        anchor_hash: block::Hash,
        expected_hashes: Vec<(block::Height, block::Hash)>,
    ) -> Option<Self> {
        let count = u32::try_from(expected_hashes.len()).ok()?;
        if count == 0 || count > 2 {
            return None;
        }
        let first_height = expected_hashes.first()?.0;
        if first_height != height {
            return None;
        }
        if !expected_hashes
            .iter()
            .enumerate()
            .all(|(index, (candidate_height, _))| {
                u32::try_from(index)
                    .ok()
                    .and_then(|offset| height.0.checked_add(offset))
                    .map(block::Height)
                    == Some(*candidate_height)
            })
        {
            return None;
        }

        Some(Self {
            height,
            generation,
            range: RangeRequest {
                range: CheckedHeaderRange::from_count(height, count)?,
                anchor_hash: Some(anchor_hash),
                finalized: false,
                want_tree_aux_roots: true,
                priority: RangePriority::Repair,
            },
            expected_hashes,
            tried_peers: HashSet::new(),
            in_flight: None,
            started_at: Instant::now(),
            next_attempt_at: Instant::now(),
            exhausted: false,
        })
    }

    pub(super) fn can_attempt(&self, now: Instant) -> bool {
        !self.exhausted
            && self.in_flight.is_none()
            && self.tried_peers.len() < VCT_ROOT_REPAIR_MAX_ATTEMPTS
            && now.duration_since(self.started_at) < VCT_ROOT_REPAIR_MAX_WALL_TIME
            && now >= self.next_attempt_at
    }

    pub(super) fn next_maintenance_deadline(&self) -> Instant {
        let repair_deadline = self.started_at + VCT_ROOT_REPAIR_MAX_WALL_TIME;
        // An in-flight attempt has its own request timeout, so ignore its stale
        // retry timestamp while retaining the overall repair deadline.
        if self.in_flight.is_none() {
            repair_deadline.min(self.next_attempt_at)
        } else {
            repair_deadline
        }
    }

    pub(super) fn mark_attempt(&mut self, peer: ZakuraPeerId) {
        self.tried_peers.insert(peer.clone());
        self.in_flight = Some(peer);
    }

    pub(super) fn finish_attempt(
        &mut self,
        peer: &ZakuraPeerId,
        generation: u64,
        now: Instant,
    ) -> bool {
        if self.generation != generation || self.in_flight.as_ref() != Some(peer) {
            return false;
        }
        self.in_flight = None;
        let attempt_index = self
            .tried_peers
            .len()
            .saturating_sub(1)
            .min(VCT_ROOT_REPAIR_MAX_ATTEMPTS - 1);
        self.next_attempt_at = now + VCT_ROOT_REPAIR_BACKOFFS[attempt_index];
        self.refresh_exhausted(now);
        true
    }

    /// Marks an episode exhausted once either bound has elapsed.
    ///
    /// Returns `true` only for the transition so callers emit operator signals once.
    pub(super) fn refresh_exhausted(&mut self, now: Instant) -> bool {
        if self.exhausted
            || (self.tried_peers.len() < VCT_ROOT_REPAIR_MAX_ATTEMPTS
                && now.duration_since(self.started_at) < VCT_ROOT_REPAIR_MAX_WALL_TIME)
        {
            return false;
        }

        self.exhausted = true;
        true
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct StaleAnchorFailures {
    pub(super) count: u32,
    pub(super) peers: HashSet<ZakuraPeerId>,
}

impl StaleAnchorFailures {
    pub(super) fn record(&mut self, peer: ZakuraPeerId) {
        self.count = self.count.saturating_add(1);
        self.peers.insert(peer);
    }

    pub(super) fn should_reanchor(&self) -> bool {
        self.count >= HEADER_SYNC_STALE_ANCHOR_LINK_FAILURES
            && self.peers.len() >= HEADER_SYNC_STALE_ANCHOR_DISTINCT_PEERS
    }

    pub(super) fn reset(&mut self) {
        self.count = 0;
        self.peers.clear();
    }
}

#[derive(Copy, Clone, Debug)]
pub(super) struct HeaderSyncAdvisoryPeerState {
    pub(super) summary: HeaderSyncServiceSummary,
    pub(super) observed_at: Instant,
    pub(super) failure_count: u32,
    pub(super) backoff_until: Option<Instant>,
}

impl HeaderSyncAdvisoryPeerState {
    pub(super) fn new(summary: HeaderSyncServiceSummary, observed_at: Instant) -> Self {
        Self {
            summary,
            observed_at,
            failure_count: 0,
            backoff_until: None,
        }
    }

    pub(super) fn refresh_summary(
        &mut self,
        summary: HeaderSyncServiceSummary,
        observed_at: Instant,
    ) {
        self.summary = summary;
        self.observed_at = observed_at;
    }

    pub(super) fn is_expired(&self, now: Instant) -> bool {
        now.duration_since(self.observed_at) >= HEADER_SYNC_ADVISORY_TTL
    }

    pub(super) fn is_backed_off(&self, now: Instant) -> bool {
        self.backoff_until.is_some_and(|until| until > now)
    }

    pub(super) fn record_confirmed(&mut self) {
        self.failure_count = 0;
        self.backoff_until = None;
    }

    pub(super) fn record_unconfirmed(&mut self, now: Instant) {
        self.failure_count = self.failure_count.saturating_add(1);
        if self.failure_count >= HEADER_SYNC_ADVISORY_BACKOFF_FAILURES {
            self.backoff_until = Some(now + HEADER_SYNC_ADVISORY_BACKOFF);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PeerHeaderState {
    pub(super) session: HeaderSyncPeerSession,
    pub(super) direction: ServicePeerDirection,
    pub(super) advertised_tip: block::Height,
    pub(super) advertised_hash: block::Hash,
    pub(super) anchor: block::Height,
    pub(super) max_headers_per_response: u32,
    pub(super) max_inflight_requests: u16,
    pub(super) received_status: bool,
    pub(super) last_received_status_at: Option<Instant>,
    /// The most recent status sent to this peer over its current session, if
    /// any. Used to suppress re-sending an identical, non-tip-advancing status,
    /// which the peer's inbound rate limiter would otherwise treat as spam.
    pub(super) last_sent_status: Option<HeaderSyncStatus>,
    pub(super) outstanding: Vec<OutstandingRange>,
    pub(super) requester_id: Option<HeaderRequesterId>,
    pub(super) requester: Option<HeaderRequesterHandle>,
    pub(super) meters: HeaderSyncPeerMeters,
    pub(super) served_headers_inflight: u16,
    pub(super) served_header_request_ids: HashSet<HeaderSyncRequestId>,
    pub(super) highest_served_header_request_id: Option<HeaderSyncRequestId>,
}

impl PeerHeaderState {
    pub(super) fn new(
        session: HeaderSyncPeerSession,
        anchor: (block::Height, block::Hash),
        local_range: u32,
        local_inflight: u16,
        status_refresh_interval: Duration,
        inbound_status_min_interval: Duration,
        inbound_new_block_min_interval: Duration,
    ) -> Self {
        Self {
            direction: session.direction(),
            session,
            advertised_tip: anchor.0,
            advertised_hash: anchor.1,
            anchor: anchor.0,
            max_headers_per_response: clamp_advertised_range(local_range),
            max_inflight_requests: local_inflight.clamp(1, LOCAL_MAX_HS_INFLIGHT_PER_PEER),
            received_status: false,
            last_received_status_at: None,
            last_sent_status: None,
            outstanding: Vec::new(),
            requester_id: None,
            requester: None,
            meters: HeaderSyncPeerMeters::new(
                status_refresh_interval,
                inbound_status_min_interval,
                inbound_new_block_min_interval,
            ),
            served_headers_inflight: 0,
            served_header_request_ids: HashSet::new(),
            highest_served_header_request_id: None,
        }
    }

    pub(super) fn available_slots(&self) -> usize {
        usize::from(self.max_inflight_requests).saturating_sub(self.outstanding.len())
    }

    pub(super) fn remove_outstanding_by_request_id(
        &mut self,
        request_id: HeaderSyncRequestId,
    ) -> Option<OutstandingRange> {
        self.outstanding
            .iter()
            .position(|outstanding| outstanding.wire_request.request_id == request_id)
            .map(|index| self.outstanding.remove(index))
    }

    /// Whether `status` differs from the most recent status sent to this peer
    /// over its current session. A status identical to the last one we sent is
    /// redundant — the peer cannot learn anything from it and its inbound status
    /// rate limiter would treat it as spam — so callers suppress it.
    pub(super) fn status_differs_from_last_sent(&self, status: HeaderSyncStatus) -> bool {
        self.last_sent_status != Some(status)
    }

    /// Records `status` as the most recent status sent to this peer, so a later
    /// identical status can be suppressed by [`Self::status_differs_from_last_sent`].
    pub(super) fn record_sent_status(&mut self, status: HeaderSyncStatus) {
        self.last_sent_status = Some(status);
    }

    /// Forgets the last status sent to this peer so the next one is always sent.
    /// Called when a fresh session replaces the peer's transport: the new
    /// channel's remote has received no status yet and gates serving us on it,
    /// so the initial status must go out regardless of its contents.
    pub(super) fn reset_sent_status(&mut self) {
        self.last_sent_status = None;
    }

    pub(super) fn try_start_serving_headers(
        &mut self,
        local_inflight_cap: u16,
        request_id: HeaderSyncRequestId,
    ) -> bool {
        if self.served_headers_inflight >= local_inflight_cap {
            return false;
        }
        if self
            .highest_served_header_request_id
            .is_some_and(|highest| request_id.get() <= highest.get())
        {
            return false;
        }
        if !self.served_header_request_ids.insert(request_id) {
            return false;
        }
        self.highest_served_header_request_id = Some(request_id);
        self.served_headers_inflight = self.served_headers_inflight.saturating_add(1);
        true
    }

    pub(super) fn finish_serving_headers(&mut self, request_id: HeaderSyncRequestId) -> bool {
        if !self.served_header_request_ids.remove(&request_id) {
            return false;
        }
        self.served_headers_inflight = self.served_headers_inflight.saturating_sub(1);
        true
    }
}

#[derive(Clone, Debug)]
pub(super) struct HeaderSyncPeerMeters {
    pub(super) unsolicited: RateMeter,
    pub(super) inbound_status: RateMeter,
    pub(super) inbound_new_block: RateMeter,
    /// Paces retries after the peer's outbound queue rejects a status.
    ///
    /// This deadline is separate from `unsolicited` and `keepalive` because
    /// those meters record statuses that were successfully published.
    pub(super) status_publication_retry_at: Option<Instant>,
    /// Gates redundant keepalive status sends.
    ///
    /// Floored above the remote's inbound status minimum interval so a
    /// keepalive can never be classified as status spam even when
    /// `status_refresh_interval` is configured below that minimum, and starts
    /// one full interval out so it never lands right after the initial
    /// connect status (which may already have consumed the remote's
    /// non-advancing status token).
    pub(super) keepalive: RateMeter,
}

impl HeaderSyncPeerMeters {
    pub(super) fn new(
        status_refresh_interval: Duration,
        inbound_status_min_interval: Duration,
        inbound_new_block_min_interval: Duration,
    ) -> Self {
        let keepalive_interval =
            status_refresh_interval.max(inbound_status_min_interval.saturating_mul(2));
        let mut keepalive = RateMeter::new(keepalive_interval);
        keepalive.mark_taken(Instant::now());
        Self {
            unsolicited: RateMeter::new(status_refresh_interval),
            inbound_status: RateMeter::new(inbound_status_min_interval),
            inbound_new_block: RateMeter::new(inbound_new_block_min_interval),
            status_publication_retry_at: None,
            keepalive,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct OutstandingRange {
    pub(super) wire_request: HeaderSyncWireRequestIdentity,
    pub(super) range_request: RangeRequest,
    pub(super) deadline: Instant,
    pub(super) purpose: RangePurpose,
    pub(super) phase: OutstandingPhase,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum OutstandingPhase {
    Publishing,
    AwaitingResponse,
    EmptyRetry,
}

#[derive(Clone, Debug)]
pub(super) struct BufferedHeaderRange {
    pub(super) wire_request: HeaderSyncWireRequestIdentity,
    pub(super) range: RangeRequest,
    pub(super) purpose: RangePurpose,
    pub(super) payload: HeaderRangePayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingOperation {
    pub(super) range: RangeRequest,
    pub(super) purpose: RangePurpose,
    pub(super) retention_candidate: Option<HeaderRangePayload>,
    /// Present for `AuthenticateRoots` only: source plus launch snapshot.
    pub(super) root_auth: Option<PendingRootAuth>,
    pub(super) completion_observed: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingRootAuth {
    pub(super) source: RootAuthSource,
    /// Launch snapshot; used to detect watch-first stale races.
    pub(super) expected: HeaderRootAuthState,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RootAuthSource {
    Retained(block::Height),
    Fallback,
}

#[derive(Clone, Debug)]
pub(super) struct RetainedRootPayload {
    pub(super) wire_request: HeaderSyncWireRequestIdentity,
    pub(super) payload: HeaderRangePayload,
    pub(super) authenticating: bool,
    pub(super) local_attempts: u32,
    pub(super) local_retry_exhausted: bool,
    pub(super) local_retry_started_at: Option<Instant>,
    pub(super) retry_at: Option<Instant>,
}

impl RetainedRootPayload {
    fn new(wire_request: HeaderSyncWireRequestIdentity, payload: HeaderRangePayload) -> Self {
        Self {
            wire_request,
            payload,
            authenticating: false,
            local_attempts: 0,
            local_retry_exhausted: false,
            local_retry_started_at: None,
            retry_at: None,
        }
    }

    pub(super) fn retry_local(&mut self, now: Instant) -> bool {
        self.authenticating = false;
        self.local_attempts = self.local_attempts.saturating_add(1);
        let started_at = *self.local_retry_started_at.get_or_insert(now);
        if self.local_attempts >= RETAINED_ROOT_LOCAL_MAX_ATTEMPTS
            || now.duration_since(started_at) >= RETAINED_ROOT_LOCAL_MAX_WALL_TIME
        {
            self.local_retry_exhausted = true;
            self.retry_at = None;
            return false;
        }
        let delay_seconds = 1u64
            .checked_shl(self.local_attempts.saturating_sub(1).min(5))
            .unwrap_or(32)
            .min(30);
        self.retry_at = Some(now + Duration::from_secs(delay_seconds));
        true
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct RangeRequest {
    pub(super) range: CheckedHeaderRange,
    pub(super) anchor_hash: Option<block::Hash>,
    pub(super) finalized: bool,
    pub(super) want_tree_aux_roots: bool,
    pub(super) priority: RangePriority,
}

impl RangeRequest {
    pub(super) fn start_height(self) -> block::Height {
        self.range.start()
    }

    pub(super) fn count(self) -> u32 {
        self.range.count()
    }

    pub(super) fn end_height(self) -> block::Height {
        self.range.end()
    }

    pub(super) fn suffix_after(
        self,
        covered_through: block::Height,
        anchor_hash: block::Hash,
    ) -> Option<Self> {
        let end_height = self.end_height();
        if end_height <= covered_through {
            return None;
        }
        let start_height = next_height(covered_through)?;
        let geometry = CheckedHeaderRange::from_bounds(start_height, end_height)?;
        Some(Self {
            range: geometry,
            anchor_hash: Some(anchor_hash),
            ..self
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum RangePriority {
    Forward,
    AuthenticateRoots,
    Repair,
}

impl RangePriority {
    pub(super) fn label(self) -> &'static str {
        match self {
            RangePriority::Forward => "forward",
            RangePriority::AuthenticateRoots => "authenticate_roots",
            RangePriority::Repair => "repair",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RangePurpose {
    Sync,
    AuthenticateRoots,
    VctRepair {
        height: block::Height,
        generation: u64,
    },
}
