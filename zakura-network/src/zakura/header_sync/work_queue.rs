use super::{state::*, wire::*, *};

#[derive(Clone, Debug, Default)]
pub(super) struct HeaderHashDedup {
    pub(super) hashes: HashSet<block::Hash>,
    pub(super) order: VecDeque<block::Hash>,
}

impl HeaderHashDedup {
    pub(super) fn contains(&self, hash: &block::Hash) -> bool {
        self.hashes.contains(hash)
    }

    pub(super) fn insert(&mut self, hash: block::Hash) -> bool {
        if !self.hashes.insert(hash) {
            return false;
        }
        self.order.push_back(hash);
        while self.order.len() > HEADER_SYNC_SEEN_HASH_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.hashes.remove(&oldest);
            }
        }
        true
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum HeaderWorkState {
    InFlight {
        peer: ZakuraPeerId,
    },
    Buffered {
        peer: ZakuraPeerId,
    },
    Committing {
        operation: HeaderSyncOperationIdentity,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct CoveredRange {
    pub(super) start: block::Height,
    pub(super) end: block::Height,
}

#[derive(Clone, Debug)]
pub(super) struct HeaderWorkQueue {
    pub(super) forward: VecDeque<RangeRequest>,
    pub(super) authenticate_roots: VecDeque<RangeRequest>,
    pub(super) active: HashMap<RangeRequest, HeaderWorkState>,
    pending_starts: HashSet<(RangePriority, block::Height)>,
    active_starts: HashSet<(RangePriority, block::Height)>,
    pub(super) retry_avoidance:
        HashMap<ZakuraPeerId, HashMap<(block::Height, RangePriority), Instant>>,
    delayed_retries: HashMap<(block::Height, RangePriority), DelayedRetry>,
    pub(super) covered: Vec<CoveredRange>,
    pub(super) epoch: u64,
    pub(super) oldest_missing_since: Option<Instant>,
}

#[derive(Copy, Clone, Debug)]
struct DelayedRetry {
    until: Option<Instant>,
    attempts: u32,
}

impl HeaderWorkQueue {
    pub(super) fn new() -> Self {
        Self {
            forward: VecDeque::new(),
            authenticate_roots: VecDeque::new(),
            active: HashMap::new(),
            pending_starts: HashSet::new(),
            active_starts: HashSet::new(),
            retry_avoidance: HashMap::new(),
            delayed_retries: HashMap::new(),
            covered: Vec::new(),
            epoch: 0,
            oldest_missing_since: None,
        }
    }

    pub(super) fn ensure_forward(&mut self, range: RangeRequest) {
        self.ensure(range, RangePriority::Forward);
    }

    pub(super) fn ensure(&mut self, range: RangeRequest, priority: RangePriority) {
        if self.is_covered(range)
            || self
                .active_starts
                .contains(&(priority, range.start_height()))
            || self
                .pending_starts
                .contains(&(priority, range.start_height()))
        {
            return;
        }
        let queue = match priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::AuthenticateRoots => &mut self.authenticate_roots,
            RangePriority::Repair => return,
        };
        queue.push_back(range);
        self.pending_starts.insert((priority, range.start_height()));
        self.oldest_missing_since.get_or_insert_with(Instant::now);
        metrics::counter!("sync.header.work.added", "lane" => priority.label()).increment(1);
    }

    pub(super) fn next_for_peer(
        &mut self,
        peer_id: &ZakuraPeerId,
        peer: &PeerHeaderState,
        allow_root_auth: bool,
    ) -> Option<RangeRequest> {
        let now = Instant::now();
        self.retry_avoidance.retain(|_, ranges| {
            ranges.retain(|_, until| *until > now);
            !ranges.is_empty()
        });
        let range = allow_root_auth
            .then(|| {
                Self::pop_assignable(
                    &mut self.authenticate_roots,
                    &self.retry_avoidance,
                    &self.delayed_retries,
                    peer_id,
                    peer,
                    now,
                )
            })
            .flatten()
            .or_else(|| {
                Self::pop_assignable(
                    &mut self.forward,
                    &self.retry_avoidance,
                    &self.delayed_retries,
                    peer_id,
                    peer,
                    now,
                )
            });
        if let Some(range) = range {
            self.pending_starts
                .remove(&(range.priority, range.start_height()));
        }
        range
    }

    fn pop_assignable(
        queue: &mut VecDeque<RangeRequest>,
        retry_avoidance: &HashMap<ZakuraPeerId, HashMap<(block::Height, RangePriority), Instant>>,
        delayed_retries: &HashMap<(block::Height, RangePriority), DelayedRetry>,
        peer_id: &ZakuraPeerId,
        peer: &PeerHeaderState,
        now: Instant,
    ) -> Option<RangeRequest> {
        let index = queue.iter().position(|range| {
            range.end_height() <= peer.advertised_tip
                && delayed_retries
                    .get(&(range.start_height(), range.priority))
                    .is_none_or(|retry| retry.until.is_none_or(|until| until <= now))
                && retry_avoidance
                    .get(peer_id)
                    .and_then(|ranges| ranges.get(&(range.start_height(), range.priority)))
                    .is_none_or(|until| *until <= now)
        })?;
        queue.remove(index)
    }

    pub(super) fn mark_assigned(&mut self, peer: ZakuraPeerId, range: RangeRequest) {
        let previous = self
            .active
            .insert(range, HeaderWorkState::InFlight { peer });
        self.active_starts
            .insert((range.priority, range.start_height()));
        debug_assert!(previous.is_none(), "pending work has no active owner");
        metrics::counter!("sync.header.work.taken", "lane" => range.priority.label()).increment(1);
    }

    pub(super) fn mark_buffered(&mut self, peer: ZakuraPeerId, range: RangeRequest) {
        let previous = self
            .active
            .insert(range, HeaderWorkState::Buffered { peer: peer.clone() });
        debug_assert!(
            matches!(previous, Some(HeaderWorkState::InFlight { peer: owner }) if owner == peer),
            "only the wire owner can buffer active header work"
        );
    }

    pub(super) fn mark_committing(
        &mut self,
        operation: HeaderSyncOperationIdentity,
        range: RangeRequest,
    ) {
        let peer = operation.wire_request.peer.clone();
        let previous = self
            .active
            .insert(range, HeaderWorkState::Committing { operation });
        debug_assert!(
            matches!(previous, Some(HeaderWorkState::Buffered { peer: owner }) if owner == peer),
            "only buffered header work can enter state commit"
        );
    }

    pub(super) fn mark_authenticating(
        &mut self,
        operation: HeaderSyncOperationIdentity,
        range: RangeRequest,
    ) {
        self.active
            .insert(range, HeaderWorkState::Committing { operation });
        self.pending_starts
            .remove(&(range.priority, range.start_height()));
        self.active_starts
            .insert((range.priority, range.start_height()));
    }

    pub(super) fn narrow_queued_range(&mut self, original: RangeRequest, narrowed: RangeRequest) {
        if original == narrowed {
            return;
        }

        if let Some(state) = self.active.remove(&original) {
            self.active_starts
                .remove(&(original.priority, original.start_height()));
            self.active_starts
                .insert((narrowed.priority, narrowed.start_height()));
            self.active.insert(narrowed, state);
        }
    }

    pub(super) fn retry(&mut self, range: RangeRequest) {
        self.active.remove(&range);
        self.active_starts
            .remove(&(range.priority, range.start_height()));
        if self.is_covered(range) {
            return;
        }
        let queue = match range.priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::AuthenticateRoots => &mut self.authenticate_roots,
            RangePriority::Repair => return,
        };
        if self
            .pending_starts
            .insert((range.priority, range.start_height()))
        {
            queue.push_front(range);
        }
        metrics::counter!("sync.header.work.returned", "lane" => range.priority.label())
            .increment(1);
    }

    pub(super) fn retry_avoiding(&mut self, peer: ZakuraPeerId, range: RangeRequest) {
        self.retry_avoidance.entry(peer).or_default().insert(
            (range.start_height(), range.priority),
            Instant::now() + HEADER_SYNC_RETRY_AVOIDANCE,
        );
        self.retry(range);
    }

    pub(super) fn retry_delayed(&mut self, range: RangeRequest) {
        let key = (range.start_height(), range.priority);
        let attempts = self
            .delayed_retries
            .get(&key)
            .map_or(1, |retry| retry.attempts.saturating_add(1));
        let delay_seconds = 1u64
            .checked_shl(attempts.saturating_sub(1).min(5))
            .unwrap_or(32)
            .min(30);
        self.delayed_retries.insert(
            key,
            DelayedRetry {
                until: Some(Instant::now() + Duration::from_secs(delay_seconds)),
                attempts,
            },
        );
        self.retry(range);
    }

    pub(super) fn retire_operation(
        &mut self,
        operation: &HeaderSyncOperationIdentity,
        range: RangeRequest,
    ) {
        if matches!(
            self.active.get(&range),
            Some(HeaderWorkState::Committing { operation: active }) if active == operation
        ) {
            self.clear_assignment(range);
        }
    }

    pub(super) fn forget_peer(&mut self, peer: &ZakuraPeerId) {
        let ranges: Vec<_> = self
            .active
            .iter()
            .filter_map(|(range, state)| {
                matches!(state, HeaderWorkState::InFlight { peer: owner } if owner == peer)
                    .then_some(*range)
            })
            .collect();
        for range in ranges {
            self.retry(range);
        }
        self.retry_avoidance.remove(peer);
    }

    pub(super) fn clear_assignment(&mut self, range: RangeRequest) {
        self.active.remove(&range);
        self.active_starts
            .remove(&(range.priority, range.start_height()));
    }

    pub(super) fn complete(&mut self, range: RangeRequest) {
        let previous = self.active.remove(&range);
        self.active_starts
            .remove(&(range.priority, range.start_height()));
        debug_assert!(
            matches!(previous, Some(HeaderWorkState::Committing { .. })) || previous.is_none(),
            "only committing or externally covered work can complete"
        );
    }

    pub(super) fn trim_pending_forward_through(
        &mut self,
        height: block::Height,
        anchor_hash: block::Hash,
    ) {
        self.forward = self
            .forward
            .drain(..)
            .filter_map(|range| {
                if range.start_height() <= height {
                    range.suffix_after(height, anchor_hash)
                } else {
                    Some(range)
                }
            })
            .collect();
        self.rebuild_start_indexes();
    }

    pub(super) fn clear_pending_anchor(
        &mut self,
        priority: RangePriority,
        start_height: block::Height,
        anchor_hash: block::Hash,
    ) {
        let queue = match priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::AuthenticateRoots => &mut self.authenticate_roots,
            RangePriority::Repair => return,
        };
        if let Some(range) = queue
            .iter_mut()
            .find(|range| range.start_height() == start_height)
        {
            if range.anchor_hash == Some(anchor_hash) {
                range.anchor_hash = None;
            }
        }
    }

    pub(super) fn clear_forward(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        self.forward.clear();
        self.active
            .retain(|range, _| range.priority != RangePriority::Forward);
        self.rebuild_start_indexes();
        metrics::counter!("sync.header.work.reset").increment(1);
    }

    pub(super) fn clear_root_auth(&mut self) {
        self.authenticate_roots.clear();
        self.active
            .retain(|range, _| range.priority != RangePriority::AuthenticateRoots);
        self.delayed_retries
            .retain(|(_, priority), _| *priority != RangePriority::AuthenticateRoots);
        self.rebuild_start_indexes();
    }

    pub(super) fn mark_height_covered(&mut self, height: block::Height) {
        self.mark_covered_interval(CoveredRange {
            start: height,
            end: height,
        });
        self.prune_covered();
    }

    pub(super) fn mark_range_covered(&mut self, start: block::Height, end: block::Height) {
        self.mark_covered_interval(CoveredRange { start, end });
        self.prune_covered();
    }

    pub(super) fn is_covered(&self, range: RangeRequest) -> bool {
        if range.priority == RangePriority::AuthenticateRoots {
            return false;
        }
        let end = range.end_height();
        self.covered
            .iter()
            .any(|covered| covered.start <= range.start_height() && covered.end >= end)
    }

    pub(super) fn mark_covered_interval(&mut self, mut interval: CoveredRange) {
        if interval.end < interval.start {
            return;
        }

        let mut merged = Vec::with_capacity(self.covered.len().saturating_add(1));
        let mut inserted = false;
        for covered in self.covered.drain(..) {
            if covered.end.0.saturating_add(1) < interval.start.0 {
                merged.push(covered);
            } else if interval.end.0.saturating_add(1) < covered.start.0 {
                if !inserted {
                    merged.push(interval);
                    inserted = true;
                }
                merged.push(covered);
            } else {
                interval.start = interval.start.min(covered.start);
                interval.end = interval.end.max(covered.end);
            }
        }
        if !inserted {
            merged.push(interval);
        }
        self.covered = merged;
    }

    pub(super) fn prune_covered(&mut self) {
        let covered = self.covered.clone();
        let is_covered = |range: &RangeRequest| {
            if range.priority == RangePriority::AuthenticateRoots {
                return false;
            }
            let end = range.end_height();
            covered
                .iter()
                .any(|covered| covered.start <= range.start_height() && covered.end >= end)
        };
        self.forward.retain(|range| !is_covered(range));
        self.active.retain(|range, state| {
            matches!(state, HeaderWorkState::Committing { .. }) || !is_covered(range)
        });
        self.rebuild_start_indexes();
        if self.forward.is_empty() && self.active.is_empty() {
            self.oldest_missing_since = None;
        }
    }

    pub(super) fn pending_len(&self) -> usize {
        self.forward
            .len()
            .saturating_add(self.authenticate_roots.len())
    }

    pub(super) fn resident_heights(&self) -> u64 {
        self.forward
            .iter()
            .chain(self.authenticate_roots.iter())
            .chain(self.active.keys())
            .map(|range| u64::from(range.count()))
            .sum()
    }

    pub(super) fn highest_end(&self, priority: RangePriority) -> Option<block::Height> {
        self.forward
            .iter()
            .chain(self.active.keys())
            .filter(|range| range.priority == priority)
            .map(|range| range.end_height())
            .max()
    }

    pub(super) fn next_retry_deadline(&mut self) -> Option<Instant> {
        let now = Instant::now();
        self.retry_avoidance.retain(|_, ranges| {
            ranges.retain(|_, until| *until > now);
            !ranges.is_empty()
        });
        let due_delayed_retry = self
            .delayed_retries
            .values_mut()
            .fold(false, |any_due, retry| {
                let is_due = retry.until.is_some_and(|until| until <= now);
                if is_due {
                    retry.until = None;
                }
                any_due || is_due
            });
        self.retry_avoidance
            .values()
            .flat_map(HashMap::values)
            .copied()
            .chain(
                self.delayed_retries
                    .values()
                    .filter_map(|retry| retry.until)
                    .filter(|until| *until > now),
            )
            .chain(due_delayed_retry.then_some(now))
            .min()
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.forward.is_empty() || !self.authenticate_roots.is_empty()
    }

    pub(super) fn peer_retry_avoided(
        &self,
        peer: &ZakuraPeerId,
        advertised_tip: block::Height,
    ) -> bool {
        let Some(avoided) = self.retry_avoidance.get(peer) else {
            return false;
        };
        self.forward
            .iter()
            .chain(self.authenticate_roots.iter())
            .any(|range| {
                range.end_height() <= advertised_tip
                    && avoided
                        .get(&(range.start_height(), range.priority))
                        .is_some_and(|until| *until > Instant::now())
            })
    }

    #[cfg(test)]
    pub(super) fn state(&self, range: RangeRequest) -> Option<&HeaderWorkState> {
        self.active.get(&range)
    }

    pub(super) fn active_counts(&self) -> (usize, usize, usize) {
        self.active.values().fold((0, 0, 0), |mut counts, state| {
            match state {
                HeaderWorkState::InFlight { .. } => counts.0 += 1,
                HeaderWorkState::Buffered { .. } => counts.1 += 1,
                HeaderWorkState::Committing { .. } => counts.2 += 1,
            }
            counts
        })
    }

    pub(super) fn oldest_missing_height(&self) -> Option<block::Height> {
        self.forward
            .iter()
            .chain(self.authenticate_roots.iter())
            .chain(self.active.keys())
            .map(|range| range.start_height())
            .min()
    }

    fn rebuild_start_indexes(&mut self) {
        self.pending_starts.clear();
        self.pending_starts.extend(
            self.forward
                .iter()
                .chain(self.authenticate_roots.iter())
                .map(|range| (range.priority, range.start_height())),
        );
        self.active_starts.clear();
        self.active_starts.extend(
            self.active
                .keys()
                .map(|range| (range.priority, range.start_height())),
        );
    }
}

#[derive(Clone, Debug)]
pub(super) struct RateMeter {
    pub(super) next_allowed: Instant,
    pub(super) interval: Duration,
}

impl RateMeter {
    pub(super) fn new(interval: Duration) -> Self {
        Self {
            next_allowed: Instant::now(),
            interval,
        }
    }

    pub(super) fn try_take(&mut self, now: Instant) -> bool {
        if now < self.next_allowed {
            return false;
        }
        self.next_allowed = now + self.interval;
        true
    }

    pub(super) fn is_ready(&self, now: Instant) -> bool {
        now >= self.next_allowed
    }

    pub(super) fn mark_taken(&mut self, now: Instant) {
        self.next_allowed = now + self.interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::ServicePeerDirection;

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
    }

    fn operation(peer: ZakuraPeerId, session_id: u64) -> HeaderSyncOperationIdentity {
        HeaderSyncOperationIdentity {
            wire_request: HeaderSyncWireRequestIdentity {
                peer,
                session_id,
                request_id: HeaderSyncRequestId::new(1).expect("test request ID is non-zero"),
            },
            op_kind: HeaderSyncOperationKind::CommitHeaders,
        }
    }

    fn range(start: u32, count: u32, priority: RangePriority) -> RangeRequest {
        RangeRequest {
            range: CheckedHeaderRange::from_count(block::Height(start), count)
                .expect("test range is non-empty and bounded"),
            anchor_hash: None,
            finalized: false,
            want_tree_aux_roots: true,
            priority,
        }
    }

    fn peer_state(byte: u8, advertised_tip: u32) -> (ZakuraPeerId, PeerHeaderState) {
        let peer = peer(byte);
        let (send, _recv) = crate::zakura::framed_channel(1);
        let session = HeaderSyncPeerSession::from_parts_with_direction(
            peer.clone(),
            ServicePeerDirection::Inbound,
            send,
            CancellationToken::new(),
        );
        let interval = Duration::from_secs(1);
        let mut state = PeerHeaderState::new(
            session,
            (block::Height(0), block::Hash([0; 32])),
            DEFAULT_HS_RANGE,
            DEFAULT_HS_MAX_INFLIGHT,
            interval,
            interval,
            interval,
        );
        state.advertised_tip = block::Height(advertised_tip);
        (peer, state)
    }

    #[test]
    fn pending_and_active_ranges_are_deduplicated() {
        let mut queue = HeaderWorkQueue::new();
        let range = range(1, 2, RangePriority::Forward);
        let (peer, state) = peer_state(1, 10);

        queue.ensure_forward(range);
        queue.ensure_forward(range);
        assert_eq!(queue.pending_len(), 1);

        let claimed = queue
            .next_for_peer(&peer, &state, true)
            .expect("peer can claim the pending range");
        queue.mark_assigned(peer.clone(), claimed);
        queue.ensure_forward(RangeRequest {
            range: CheckedHeaderRange::from_count(block::Height(1), 3)
                .expect("test range is non-empty"),
            ..range
        });
        assert_eq!(queue.pending_len(), 0);
        assert!(matches!(
            queue.state(range),
            Some(HeaderWorkState::InFlight { peer: owner }) if owner == &peer
        ));

        queue.retry(range);
        queue.retry(range);
        assert_eq!(queue.pending_len(), 1);
        assert_eq!(queue.next_for_peer(&peer, &state, true), Some(range));
    }

    #[test]
    fn partial_forward_coverage_preserves_the_interior_suffix() {
        let mut queue = HeaderWorkQueue::new();
        let first = range(1, 2, RangePriority::Forward);
        let later = range(3, 2, RangePriority::Forward);
        let anchor = block::Hash([9; 32]);
        queue.ensure_forward(first);
        queue.ensure_forward(later);

        queue.trim_pending_forward_through(block::Height(1), anchor);

        assert_eq!(
            queue.forward.iter().copied().collect::<Vec<_>>(),
            vec![
                RangeRequest {
                    range: CheckedHeaderRange::from_count(block::Height(2), 1)
                        .expect("test suffix is non-empty"),
                    anchor_hash: Some(anchor),
                    ..first
                },
                later,
            ]
        );
    }

    #[test]
    fn rejected_prefix_clears_its_pending_suffix_anchor() {
        let mut queue = HeaderWorkQueue::new();
        let poisoned = block::Hash([7; 32]);
        let suffix = RangeRequest {
            anchor_hash: Some(poisoned),
            ..range(3, 2, RangePriority::Forward)
        };
        queue.ensure_forward(suffix);

        queue.clear_pending_anchor(RangePriority::Forward, suffix.start_height(), poisoned);

        assert_eq!(
            queue.forward.front().and_then(|range| range.anchor_hash),
            None
        );
    }

    #[test]
    fn expired_retry_avoidance_does_not_leave_a_past_deadline() {
        let mut queue = HeaderWorkQueue::new();
        queue.retry_avoidance.insert(
            peer(2),
            HashMap::from([(
                (block::Height(1), RangePriority::Forward),
                Instant::now() - Duration::from_millis(1),
            )]),
        );

        assert_eq!(queue.next_retry_deadline(), None);
        assert!(queue.retry_avoidance.is_empty());
    }

    #[test]
    fn maximum_height_range_keeps_valid_queue_ownership() {
        let mut queue = HeaderWorkQueue::new();
        let range = range(u32::MAX, 1, RangePriority::Forward);
        let (peer, state) = peer_state(3, u32::MAX);
        queue.ensure_forward(range);

        let claimed = queue
            .next_for_peer(&peer, &state, true)
            .expect("maximum-height work is assignable without overflow");
        queue.mark_assigned(peer.clone(), claimed);
        assert_eq!(claimed.end_height(), block::Height(u32::MAX));
        assert!(matches!(
            queue.state(range),
            Some(HeaderWorkState::InFlight { peer: owner }) if owner == &peer
        ));
    }

    #[test]
    fn seen_hash_dedup_evicts_the_oldest_entry_at_capacity() {
        let mut seen = HeaderHashDedup::default();
        for value in 0..=HEADER_SYNC_SEEN_HASH_CAPACITY {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(
                &u64::try_from(value)
                    .expect("the test capacity fits in u64")
                    .to_le_bytes(),
            );
            assert!(seen.insert(block::Hash(bytes)));
        }

        assert_eq!(seen.order.len(), HEADER_SYNC_SEEN_HASH_CAPACITY);
        assert!(!seen.contains(&block::Hash([0; 32])));
    }

    #[test]
    fn retry_avoidance_is_local_to_the_failed_peer() {
        let mut queue = HeaderWorkQueue::new();
        let range = range(1, 1, RangePriority::Forward);
        let (failed_peer, failed_state) = peer_state(2, 10);
        let (other_peer, other_state) = peer_state(3, 10);

        queue.ensure_forward(range);
        let claimed = queue
            .next_for_peer(&failed_peer, &failed_state, true)
            .expect("failed peer initially claims the range");
        queue.mark_assigned(failed_peer.clone(), claimed);
        queue.retry_avoiding(failed_peer.clone(), range);

        assert!(queue.peer_retry_avoided(&failed_peer, failed_state.advertised_tip));
        assert_eq!(queue.next_for_peer(&failed_peer, &failed_state, true), None);
        assert_eq!(
            queue.next_for_peer(&other_peer, &other_state, true),
            Some(range)
        );
    }

    #[test]
    fn root_auth_ineligible_peer_can_take_forward_work() {
        let mut queue = HeaderWorkQueue::new();
        let auth = range(1, 2, RangePriority::AuthenticateRoots);
        let forward = range(3, 1, RangePriority::Forward);
        let (peer, mut state) = peer_state(8, 10);
        state.max_headers_per_response = 1;
        queue.ensure(auth, RangePriority::AuthenticateRoots);
        queue.ensure_forward(forward);

        let effective = clamp_header_sync_request_count(
            2,
            state.max_headers_per_response,
            &Network::Mainnet,
            LOCAL_MAX_MESSAGE_BYTES,
            true,
        );
        assert_eq!(effective, 1);
        assert_eq!(
            queue.next_for_peer(&peer, &state, effective >= 2),
            Some(forward)
        );
        assert_eq!(queue.authenticate_roots.front(), Some(&auth));
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_root_retry_does_not_block_forward_work() {
        let mut queue = HeaderWorkQueue::new();
        let auth = range(1, 2, RangePriority::AuthenticateRoots);
        let forward = range(3, 1, RangePriority::Forward);
        let (peer, state) = peer_state(9, 10);
        queue.ensure_forward(forward);
        queue.retry_delayed(auth);

        assert_eq!(
            queue.next_for_peer(&peer, &state, true),
            Some(forward),
            "forward work remains eligible while root auth backs off"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(queue.next_for_peer(&peer, &state, true), Some(auth));
    }

    #[tokio::test(start_paused = true)]
    async fn due_delayed_retry_produces_one_immediate_maintenance_deadline() {
        let mut queue = HeaderWorkQueue::new();
        queue.retry_delayed(range(1, 2, RangePriority::AuthenticateRoots));
        let future_deadline = queue
            .next_retry_deadline()
            .expect("delayed retry arms maintenance");
        assert!(future_deadline > Instant::now());

        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(queue.next_retry_deadline(), Some(Instant::now()));
        assert_eq!(
            queue.next_retry_deadline(),
            None,
            "due retry is made eligible without a past deadline busy loop"
        );
    }

    #[test]
    fn covered_ranges_merge_and_prune_owned_work_except_commits() {
        let mut queue = HeaderWorkQueue::new();
        let owner = peer(5);
        let pending = range(1, 1, RangePriority::Forward);
        let in_flight = range(2, 1, RangePriority::Forward);
        let committing = range(3, 1, RangePriority::Forward);

        queue.ensure_forward(pending);
        queue.mark_assigned(owner.clone(), in_flight);
        queue.mark_assigned(owner.clone(), committing);
        queue.mark_buffered(owner.clone(), committing);
        let operation = operation(owner, 7);
        queue.mark_committing(operation.clone(), committing);

        queue.mark_range_covered(block::Height(1), block::Height(2));
        queue.mark_height_covered(block::Height(3));

        assert_eq!(
            queue.covered,
            vec![CoveredRange {
                start: block::Height(1),
                end: block::Height(3),
            }]
        );
        assert_eq!(queue.pending_len(), 0);
        assert!(queue.state(in_flight).is_none());
        assert!(matches!(
            queue.state(committing),
            Some(HeaderWorkState::Committing { operation: active }) if active == &operation
        ));

        queue.ensure_forward(pending);
        assert_eq!(queue.pending_len(), 0);
    }

    #[test]
    fn forgetting_a_peer_requeues_only_its_in_flight_work() {
        let mut queue = HeaderWorkQueue::new();
        let forgotten = peer(6);
        let other = peer(7);
        let in_flight = range(1, 1, RangePriority::Forward);
        let buffered = range(2, 1, RangePriority::Forward);
        let committing = range(3, 1, RangePriority::Forward);
        let other_in_flight = range(4, 1, RangePriority::Forward);

        queue.mark_assigned(forgotten.clone(), in_flight);
        queue.mark_assigned(forgotten.clone(), buffered);
        queue.mark_buffered(forgotten.clone(), buffered);
        queue.mark_assigned(forgotten.clone(), committing);
        queue.mark_buffered(forgotten.clone(), committing);
        let committing_operation = operation(forgotten.clone(), 9);
        queue.mark_committing(committing_operation.clone(), committing);
        queue.mark_assigned(other.clone(), other_in_flight);
        queue.retry_avoidance.insert(
            forgotten.clone(),
            HashMap::from([(
                (in_flight.start_height(), in_flight.priority),
                Instant::now() + HEADER_SYNC_RETRY_AVOIDANCE,
            )]),
        );

        queue.forget_peer(&forgotten);

        assert_eq!(
            queue.forward.iter().copied().collect::<Vec<_>>(),
            vec![in_flight]
        );
        assert!(matches!(
            queue.state(buffered),
            Some(HeaderWorkState::Buffered { peer }) if peer == &forgotten
        ));
        assert!(matches!(
            queue.state(committing),
            Some(HeaderWorkState::Committing { operation }) if operation == &committing_operation
        ));
        assert!(matches!(
            queue.state(other_in_flight),
            Some(HeaderWorkState::InFlight { peer }) if peer == &other
        ));
        assert!(!queue.retry_avoidance.contains_key(&forgotten));
    }

    #[test]
    fn narrowing_active_work_updates_start_deduplication() {
        let mut queue = HeaderWorkQueue::new();
        let owner = peer(8);
        let original = range(1, 3, RangePriority::Forward);
        let narrowed = range(2, 2, RangePriority::Forward);

        queue.mark_assigned(owner.clone(), original);
        queue.narrow_queued_range(original, narrowed);

        assert!(queue.state(original).is_none());
        assert!(matches!(
            queue.state(narrowed),
            Some(HeaderWorkState::InFlight { peer }) if peer == &owner
        ));
        queue.ensure_forward(original);
        queue.ensure_forward(narrowed);
        assert_eq!(
            queue.forward.iter().copied().collect::<Vec<_>>(),
            vec![original]
        );
    }
}
