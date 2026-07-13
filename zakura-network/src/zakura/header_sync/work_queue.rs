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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PendingCommitKey {
    pub(super) peer: ZakuraPeerId,
    pub(super) session_id: u64,
    pub(super) start_height: block::Height,
    pub(super) count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum HeaderWorkState {
    InFlight { peer: ZakuraPeerId },
    Buffered { peer: ZakuraPeerId },
    Committing { peer: ZakuraPeerId, session_id: u64 },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct CoveredRange {
    pub(super) start: block::Height,
    pub(super) end: block::Height,
}

#[derive(Clone, Debug)]
pub(super) struct HeaderWorkQueue {
    pub(super) forward: VecDeque<RangeRequest>,
    pub(super) backward: VecDeque<RangeRequest>,
    pub(super) active: HashMap<RangeRequest, HeaderWorkState>,
    pending_starts: HashSet<(RangePriority, block::Height)>,
    active_starts: HashSet<(RangePriority, block::Height)>,
    pub(super) retry_avoidance:
        HashMap<ZakuraPeerId, HashMap<(block::Height, RangePriority), Instant>>,
    pub(super) covered: Vec<CoveredRange>,
    pub(super) epoch: u64,
    pub(super) oldest_missing_since: Option<Instant>,
}

impl HeaderWorkQueue {
    pub(super) fn new() -> Self {
        Self {
            forward: VecDeque::new(),
            backward: VecDeque::new(),
            active: HashMap::new(),
            pending_starts: HashSet::new(),
            active_starts: HashSet::new(),
            retry_avoidance: HashMap::new(),
            covered: Vec::new(),
            epoch: 0,
            oldest_missing_since: None,
        }
    }

    pub(super) fn ensure_forward(&mut self, range: RangeRequest) {
        self.ensure(range, RangePriority::Forward);
    }

    pub(super) fn ensure_backward(&mut self, range: RangeRequest) {
        self.ensure(range, RangePriority::Backward);
    }

    pub(super) fn ensure(&mut self, range: RangeRequest, priority: RangePriority) {
        if self.is_covered(range)
            || self.active_starts.contains(&(priority, range.start_height))
            || self
                .pending_starts
                .contains(&(priority, range.start_height))
        {
            return;
        }
        let queue = match priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::Backward => &mut self.backward,
            RangePriority::Repair => return,
        };
        queue.push_back(range);
        self.pending_starts.insert((priority, range.start_height));
        self.oldest_missing_since.get_or_insert_with(Instant::now);
        metrics::counter!("sync.header.work.added", "lane" => priority.label()).increment(1);
    }

    pub(super) fn next_for_peer(
        &mut self,
        peer_id: &ZakuraPeerId,
        peer: &PeerHeaderState,
    ) -> Option<RangeRequest> {
        let now = Instant::now();
        self.retry_avoidance.retain(|_, ranges| {
            ranges.retain(|_, until| *until > now);
            !ranges.is_empty()
        });
        let range =
            Self::pop_assignable(&mut self.forward, &self.retry_avoidance, peer_id, peer, now)
                .or_else(|| {
                    Self::pop_assignable(
                        &mut self.backward,
                        &self.retry_avoidance,
                        peer_id,
                        peer,
                        now,
                    )
                });
        if let Some(range) = range {
            self.pending_starts
                .remove(&(range.priority, range.start_height));
        }
        range
    }

    pub(super) fn pop_assignable(
        queue: &mut VecDeque<RangeRequest>,
        retry_avoidance: &HashMap<ZakuraPeerId, HashMap<(block::Height, RangePriority), Instant>>,
        peer_id: &ZakuraPeerId,
        peer: &PeerHeaderState,
        now: Instant,
    ) -> Option<RangeRequest> {
        let index = queue.iter().position(|range| {
            range.end_height() <= peer.advertised_tip
                && retry_avoidance
                    .get(peer_id)
                    .and_then(|ranges| ranges.get(&(range.start_height, range.priority)))
                    .is_none_or(|until| *until <= now)
        })?;
        queue.remove(index)
    }

    pub(super) fn mark_assigned(&mut self, peer: ZakuraPeerId, range: RangeRequest) {
        let previous = self
            .active
            .insert(range, HeaderWorkState::InFlight { peer });
        self.active_starts
            .insert((range.priority, range.start_height));
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
        peer: ZakuraPeerId,
        session_id: u64,
        range: RangeRequest,
    ) {
        let previous = self.active.insert(
            range,
            HeaderWorkState::Committing {
                peer: peer.clone(),
                session_id,
            },
        );
        debug_assert!(
            matches!(previous, Some(HeaderWorkState::Buffered { peer: owner }) if owner == peer),
            "only buffered header work can enter state commit"
        );
    }

    pub(super) fn narrow_queued_range(&mut self, original: RangeRequest, narrowed: RangeRequest) {
        if original == narrowed {
            return;
        }

        if let Some(state) = self.active.remove(&original) {
            self.active_starts
                .remove(&(original.priority, original.start_height));
            self.active_starts
                .insert((narrowed.priority, narrowed.start_height));
            self.active.insert(narrowed, state);
        }
    }

    pub(super) fn retry(&mut self, range: RangeRequest) {
        self.active.remove(&range);
        self.active_starts
            .remove(&(range.priority, range.start_height));
        if self.is_covered(range) {
            return;
        }
        let queue = match range.priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::Backward => &mut self.backward,
            RangePriority::Repair => return,
        };
        if self
            .pending_starts
            .insert((range.priority, range.start_height))
        {
            queue.push_front(range);
        }
        metrics::counter!("sync.header.work.returned", "lane" => range.priority.label())
            .increment(1);
    }

    pub(super) fn retry_avoiding(&mut self, peer: ZakuraPeerId, range: RangeRequest) {
        self.retry_avoidance.entry(peer).or_default().insert(
            (range.start_height, range.priority),
            Instant::now() + HEADER_SYNC_RETRY_AVOIDANCE,
        );
        self.retry(range);
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
            .remove(&(range.priority, range.start_height));
    }

    pub(super) fn complete(&mut self, range: RangeRequest) {
        let previous = self.active.remove(&range);
        self.active_starts
            .remove(&(range.priority, range.start_height));
        debug_assert!(
            matches!(previous, Some(HeaderWorkState::Committing { .. })) || previous.is_none(),
            "only committing or externally covered work can complete"
        );
    }

    pub(super) fn drop_pending_forward_through(&mut self, height: block::Height) {
        self.forward.retain(|range| range.start_height > height);
        self.rebuild_start_indexes();
    }

    pub(super) fn clear_forward(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        self.forward.clear();
        self.active
            .retain(|range, _| range.priority != RangePriority::Forward);
        self.rebuild_start_indexes();
        metrics::counter!("sync.header.work.reset").increment(1);
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
        let end = range.end_height();
        self.covered
            .iter()
            .any(|covered| covered.start <= range.start_height && covered.end >= end)
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
            let end = range.end_height();
            covered
                .iter()
                .any(|covered| covered.start <= range.start_height && covered.end >= end)
        };
        self.forward.retain(|range| !is_covered(range));
        self.backward.retain(|range| !is_covered(range));
        self.active.retain(|range, state| {
            matches!(state, HeaderWorkState::Committing { .. }) || !is_covered(range)
        });
        self.rebuild_start_indexes();
        if self.forward.is_empty() && self.backward.is_empty() && self.active.is_empty() {
            self.oldest_missing_since = None;
        }
    }

    pub(super) fn pending_len(&self) -> usize {
        self.forward.len().saturating_add(self.backward.len())
    }

    pub(super) fn resident_heights(&self) -> u64 {
        self.forward
            .iter()
            .chain(&self.backward)
            .chain(self.active.keys())
            .map(|range| u64::from(range.count))
            .sum()
    }

    pub(super) fn highest_end(&self, priority: RangePriority) -> Option<block::Height> {
        self.forward
            .iter()
            .chain(&self.backward)
            .chain(self.active.keys())
            .filter(|range| range.priority == priority)
            .map(|range| range.end_height())
            .max()
    }

    pub(super) fn next_retry_deadline(&self) -> Option<Instant> {
        self.retry_avoidance
            .values()
            .flat_map(HashMap::values)
            .copied()
            .min()
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.forward.is_empty() || !self.backward.is_empty()
    }

    pub(super) fn peer_retry_avoided(
        &self,
        peer: &ZakuraPeerId,
        advertised_tip: block::Height,
    ) -> bool {
        let Some(avoided) = self.retry_avoidance.get(peer) else {
            return false;
        };
        self.forward.iter().chain(&self.backward).any(|range| {
            range.end_height() <= advertised_tip
                && avoided
                    .get(&(range.start_height, range.priority))
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
            .chain(&self.backward)
            .chain(self.active.keys())
            .map(|range| range.start_height)
            .min()
    }

    fn rebuild_start_indexes(&mut self) {
        self.pending_starts.clear();
        self.pending_starts.extend(
            self.forward
                .iter()
                .chain(&self.backward)
                .map(|range| (range.priority, range.start_height)),
        );
        self.active_starts.clear();
        self.active_starts.extend(
            self.active
                .keys()
                .map(|range| (range.priority, range.start_height)),
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
