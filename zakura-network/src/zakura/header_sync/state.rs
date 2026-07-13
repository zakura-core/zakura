use super::{
    error::*, events::*, requester::HeaderRequesterHandle, validation::*, wire::*, work_queue::*, *,
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
    pub(super) last_header_progress_at: Instant,
    pub(super) peers: HashMap<ZakuraPeerId, PeerHeaderState>,
    pub(super) parked_peers: HashSet<ZakuraPeerId>,
    pub(super) seen: HeaderHashDedup,
    pub(super) pending_new_blocks: HashSet<block::Hash>,
    pub(super) schedule: HeaderWorkQueue,
    pub(super) buffered: BTreeMap<(RangePriority, block::Height), BufferedHeaderRange>,
    pub(super) backward_frontier: Option<(block::Height, block::Hash)>,
    pub(super) pending_commits: HashMap<PendingCommitKey, RangeRequest>,
    pub(super) repair: Option<VctRootRepair>,
    pub(super) advisory: HashMap<ZakuraPeerId, HeaderSyncAdvisoryPeerState>,
    pub(super) stale_anchor: StaleAnchorFailures,
}

impl HeaderSyncCore {
    pub(super) fn new(startup: &HeaderSyncStartup) -> Result<Self, HeaderSyncStartError> {
        validate_anchor(&startup.network, startup.anchor)?;
        let (best_header_tip, best_header_hash) = startup.best_header_tip.unwrap_or(startup.anchor);
        let backward_frontier = startup
            .network
            .checkpoint_list()
            .max_height_in_range(..startup.anchor.0)
            .and_then(|height| {
                startup
                    .network
                    .checkpoint_list()
                    .hash(height)
                    .map(|hash| (height, hash))
            });

        Ok(Self {
            anchor: startup.anchor,
            finalized_height: startup.frontiers.finalized_height,
            verified_block_tip: startup.frontiers.verified_block_tip,
            verified_block_hash: startup.frontiers.verified_block_hash,
            best_header_tip,
            best_header_hash,
            last_header_progress_at: Instant::now(),
            peers: HashMap::new(),
            parked_peers: HashSet::new(),
            seen: HeaderHashDedup::default(),
            pending_new_blocks: HashSet::new(),
            schedule: HeaderWorkQueue::new(),
            buffered: BTreeMap::new(),
            backward_frontier,
            pending_commits: HashMap::new(),
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
        let Some(start) = next_height(self.best_header_tip) else {
            return;
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
        let batch_start = self
            .schedule
            .highest_end(RangePriority::Forward)
            .and_then(next_height)
            .unwrap_or(start)
            .max(start);
        if batch_start > end {
            return;
        }
        let count = count_between(batch_start, end).min(available);
        let mut remaining = count;
        let mut batch_start = batch_start;
        let mut anchor_hash = (batch_start == start).then_some(self.best_header_hash);
        while remaining > 0 {
            let mut batch_len = remaining.min(batch_count);
            let batch_end = height_after_count(batch_start, batch_len)
                .and_then(previous_height)
                .expect("bounded header work batch has an end height");
            if let Some(checkpoint) = checkpoints.min_height_in_range(batch_start..=batch_end) {
                batch_len = count_between(batch_start, checkpoint);
            }
            self.schedule.ensure_forward(RangeRequest {
                start_height: batch_start,
                count: batch_len,
                anchor_hash,
                finalized,
                want_tree_aux_roots: true,
                priority: RangePriority::Forward,
            });
            remaining = remaining.saturating_sub(batch_len);
            let Some(next_start) = height_after_count(batch_start, batch_len) else {
                break;
            };
            batch_start = next_start;
            anchor_hash = None;
        }
    }

    pub(super) fn refresh_backward_range(&mut self, startup: &HeaderSyncStartup) {
        if self.anchor.0 == block::Height(0) {
            return;
        }
        let checkpoints = startup.network.checkpoint_list();
        // v1 backfill schedules one checkpoint bracket below the configured anchor.
        // Iterating all deeper brackets is left to final node wiring/backfill policy.
        let Some((frontier_height, frontier_hash)) = self.backward_frontier else {
            return;
        };
        if frontier_height >= self.anchor.0 {
            return;
        }
        let Some(start) = next_height(frontier_height) else {
            return;
        };
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
        let batch_start = self
            .schedule
            .highest_end(RangePriority::Backward)
            .and_then(next_height)
            .unwrap_or(start)
            .max(start);
        if batch_start > self.anchor.0 {
            return;
        }
        let mut remaining = count_between(batch_start, self.anchor.0).min(available);
        let mut batch_start = batch_start;
        let mut anchor_hash = (batch_start == start).then_some(frontier_hash);
        while remaining > 0 {
            let mut batch_len = remaining.min(batch_count);
            let batch_end = height_after_count(batch_start, batch_len)
                .and_then(previous_height)
                .expect("bounded backward work batch has an end height");
            if let Some(checkpoint) = checkpoints.min_height_in_range(batch_start..=batch_end) {
                batch_len = count_between(batch_start, checkpoint);
            }
            self.schedule.ensure_backward(RangeRequest {
                start_height: batch_start,
                count: batch_len,
                anchor_hash,
                finalized: true,
                want_tree_aux_roots: true,
                priority: RangePriority::Backward,
            });
            remaining = remaining.saturating_sub(batch_len);
            let Some(next_start) = height_after_count(batch_start, batch_len) else {
                break;
            };
            batch_start = next_start;
            anchor_hash = None;
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
                start_height: height,
                count,
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

    pub(super) fn mark_attempt(&mut self, peer: ZakuraPeerId) {
        self.tried_peers.insert(peer.clone());
        self.in_flight = Some(peer);
    }

    pub(super) fn finish_attempt(&mut self, peer: &ZakuraPeerId, now: Instant) -> bool {
        if self.in_flight.as_ref() != Some(peer) {
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
    pub(super) pending_request_sends: usize,
    pub(super) requester_generation: u64,
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
            pending_request_sends: 0,
            requester_generation: 0,
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
        usize::from(self.max_inflight_requests).saturating_sub(
            self.outstanding
                .len()
                .saturating_add(self.pending_request_sends),
        )
    }

    pub(super) fn remove_outstanding_by_request_id(
        &mut self,
        request_id: HeaderSyncRequestId,
    ) -> Option<OutstandingRange> {
        self.outstanding
            .iter()
            .position(|outstanding| outstanding.request_id == request_id)
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
            keepalive,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub(super) struct OutstandingRange {
    pub(super) request_id: HeaderSyncRequestId,
    pub(super) range: RangeRequest,
    pub(super) deadline: Instant,
    pub(super) expected_max_count: u32,
    pub(super) clear_assignment_on_timeout: bool,
    pub(super) purpose: RangePurpose,
}

#[derive(Clone, Debug)]
pub(super) struct BufferedHeaderRange {
    pub(super) peer: ZakuraPeerId,
    pub(super) session_id: u64,
    pub(super) range: RangeRequest,
    pub(super) headers: Vec<Arc<block::Header>>,
    pub(super) body_sizes: Vec<u32>,
    pub(super) tree_aux_roots: Vec<BlockCommitmentRoots>,
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct RangeRequest {
    pub(super) start_height: block::Height,
    pub(super) count: u32,
    pub(super) anchor_hash: Option<block::Hash>,
    pub(super) finalized: bool,
    pub(super) want_tree_aux_roots: bool,
    pub(super) priority: RangePriority,
}

impl RangeRequest {
    pub(super) fn end_height(self) -> block::Height {
        height_after_count(self.start_height, self.count)
            .and_then(previous_height)
            .expect("range request count is non-zero")
    }

    pub(super) fn is_within(self, start: block::Height, end: block::Height) -> bool {
        self.start_height >= start && self.end_height() <= end
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum RangePriority {
    Forward,
    Backward,
    Repair,
}

impl RangePriority {
    pub(super) fn label(self) -> &'static str {
        match self {
            RangePriority::Forward => "forward",
            RangePriority::Backward => "backward",
            RangePriority::Repair => "repair",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RangePurpose {
    Sync,
    VctRepair {
        height: block::Height,
        generation: u64,
    },
}
