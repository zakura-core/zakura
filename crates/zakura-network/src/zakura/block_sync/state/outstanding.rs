//! A peer's outstanding block-range requests, indexed for each routine event.
//!
//! A peer can hold tens of thousands of requests. Each request event updates
//! only the entries it touches, so filling or draining a window costs
//! `O(N log N)` rather than `O(N²)`.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use zakura_chain::block;

use super::OutstandingBlockRange;

/// Identity of one outstanding request. Keys increase in issue order.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::zakura::block_sync) struct RequestKey(u64);

impl RequestKey {
    const MIN: Self = Self(0);
    const MAX: Self = Self(u64::MAX);
}

/// Outstanding requests in issue order with secondary indexes.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::block_sync) struct OutstandingRequests {
    requests: BTreeMap<RequestKey, OutstandingBlockRange>,
    by_start: BTreeSet<(block::Height, RequestKey)>,
    by_end: BTreeSet<(block::Height, RequestKey)>,
    by_deadline: BTreeSet<(Instant, RequestKey)>,
    /// Requests whose frame write had not started when last checked. Transport
    /// can skip only these requests.
    unstarted: BTreeSet<RequestKey>,
    /// Requests that still count toward the no-progress probe streak.
    charged_for_liveness: BTreeSet<RequestKey>,
    /// Sum of the size estimates still reserved for unreceived heights.
    reserved_bytes: u64,
    /// Largest block count of any request issued, which bounds height lookups.
    max_count: u32,
    next_key: u64,
}

impl OutstandingRequests {
    pub(in crate::zakura::block_sync) fn len(&self) -> usize {
        self.requests.len()
    }

    pub(in crate::zakura::block_sync) fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    /// Requests in issue order.
    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn iter(
        &self,
    ) -> impl Iterator<Item = &OutstandingBlockRange> + '_ {
        self.requests.values()
    }

    /// Keyed requests in issue order.
    pub(in crate::zakura::block_sync) fn entries(
        &self,
    ) -> impl Iterator<Item = (RequestKey, &OutstandingBlockRange)> + '_ {
        self.requests.iter().map(|(key, request)| (*key, request))
    }

    pub(in crate::zakura::block_sync) fn get(
        &self,
        key: RequestKey,
    ) -> Option<&OutstandingBlockRange> {
        self.requests.get(&key)
    }

    /// The oldest outstanding request.
    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn first(&self) -> Option<&OutstandingBlockRange> {
        self.requests.values().next()
    }

    /// The most recently issued request.
    pub(in crate::zakura::block_sync) fn last_key(&self) -> Option<RequestKey> {
        self.requests.keys().next_back().copied()
    }

    pub(in crate::zakura::block_sync) fn push(
        &mut self,
        request: OutstandingBlockRange,
    ) -> RequestKey {
        let key = RequestKey(self.next_key);
        self.next_key = self
            .next_key
            .checked_add(1)
            .expect("a routine issues fewer than u64::MAX requests");
        self.by_start.insert((request.request.start_height, key));
        self.by_end.insert((request.request.end_height(), key));
        self.by_deadline.insert((request.deadline, key));
        self.unstarted.insert(key);
        self.reserved_bytes = self.reserved_bytes.saturating_add(request.reserved_bytes());
        self.max_count = self.max_count.max(request.request.count);
        self.requests.insert(key, request);
        key
    }

    pub(in crate::zakura::block_sync) fn remove(
        &mut self,
        key: RequestKey,
    ) -> Option<OutstandingBlockRange> {
        let request = self.requests.remove(&key)?;
        self.by_start.remove(&(request.request.start_height, key));
        self.by_end.remove(&(request.request.end_height(), key));
        self.by_deadline.remove(&(request.deadline, key));
        self.unstarted.remove(&key);
        self.charged_for_liveness.remove(&key);
        self.reserved_bytes = self.reserved_bytes.saturating_sub(request.reserved_bytes());
        Some(request)
    }

    /// Remove every request, in issue order.
    pub(in crate::zakura::block_sync) fn drain(&mut self) -> Vec<OutstandingBlockRange> {
        let requests = std::mem::take(&mut self.requests);
        *self = Self {
            next_key: self.next_key,
            max_count: self.max_count,
            ..Self::default()
        };
        requests.into_values().collect()
    }

    /// The oldest request covering `height`.
    pub(in crate::zakura::block_sync) fn key_for_height(
        &self,
        height: block::Height,
    ) -> Option<RequestKey> {
        let lowest_start = block::Height(height.0.saturating_sub(self.max_count.saturating_sub(1)));
        self.by_start
            .range((lowest_start, RequestKey::MIN)..=(height, RequestKey::MAX))
            .filter(|(_, key)| self.requests[key].request.contains(height))
            .map(|(_, key)| *key)
            .min()
    }

    /// The oldest request starting at `start_height`.
    pub(in crate::zakura::block_sync) fn key_for_start(
        &self,
        start_height: block::Height,
    ) -> Option<RequestKey> {
        self.by_start
            .range((start_height, RequestKey::MIN)..=(start_height, RequestKey::MAX))
            .map(|(_, key)| *key)
            .min()
    }

    pub(in crate::zakura::block_sync) fn earliest_deadline(&self) -> Option<Instant> {
        self.by_deadline.first().map(|(deadline, _)| *deadline)
    }

    /// Requests whose deadline is at or before `now`, in issue order.
    pub(in crate::zakura::block_sync) fn due_at(&self, now: Instant) -> Vec<RequestKey> {
        let mut keys: Vec<_> = self
            .by_deadline
            .iter()
            .take_while(|(deadline, _)| *deadline <= now)
            .map(|(_, key)| *key)
            .collect();
        keys.sort_unstable();
        keys
    }

    /// Requests whose whole range is at or below `floor`, in issue order.
    pub(in crate::zakura::block_sync) fn ending_at_or_below(
        &self,
        floor: block::Height,
    ) -> Vec<RequestKey> {
        let mut keys: Vec<_> = self
            .by_end
            .range(..=(floor, RequestKey::MAX))
            .map(|(_, key)| *key)
            .collect();
        keys.sort_unstable();
        keys
    }

    /// Requests whose frame transport skipped, in issue order. Forget requests
    /// whose write has started, because transport can no longer skip them.
    pub(in crate::zakura::block_sync) fn skipped(&mut self) -> Vec<RequestKey> {
        let requests = &self.requests;
        let mut skipped = Vec::new();
        self.unstarted.retain(|key| {
            let status = &requests[key].write_status;
            if status.was_skipped() {
                skipped.push(*key);
                true
            } else {
                !status.has_started()
            }
        });
        skipped
    }

    /// Record `height` as received. Return whether it was newly received.
    pub(in crate::zakura::block_sync) fn mark_received(
        &mut self,
        key: RequestKey,
        height: block::Height,
    ) -> bool {
        let Some(request) = self.requests.get_mut(&key) else {
            return false;
        };
        let Some(estimate) = request.estimated_bytes_for_height(height) else {
            return false;
        };
        if request.has_received(height) {
            return false;
        }
        request.mark_received(height);
        self.reserved_bytes = self.reserved_bytes.saturating_sub(estimate);
        true
    }

    /// Record every requested height at or below `tip` as received. Return the
    /// estimates those heights still reserved.
    pub(in crate::zakura::block_sync) fn mark_received_through(
        &mut self,
        key: RequestKey,
        tip: block::Height,
    ) -> u64 {
        let Some(request) = self.requests.get_mut(&key) else {
            return 0;
        };
        let released = request.mark_received_through(tip);
        self.reserved_bytes = self.reserved_bytes.saturating_sub(released);
        released
    }

    pub(in crate::zakura::block_sync) fn record_body_bytes(&mut self, key: RequestKey, bytes: u64) {
        if let Some(request) = self.requests.get_mut(&key) {
            request.record_body_bytes(bytes);
        }
    }

    /// Size estimates still reserved for unreceived heights across all requests.
    pub(in crate::zakura::block_sync) fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    /// Charge the most recent request to the no-progress probe streak.
    pub(in crate::zakura::block_sync) fn charge_last_for_liveness(&mut self) {
        if let Some(key) = self.last_key() {
            self.charged_for_liveness.insert(key);
        }
    }

    pub(in crate::zakura::block_sync) fn is_charged_for_liveness(&self, key: RequestKey) -> bool {
        self.charged_for_liveness.contains(&key)
    }

    /// Clear every probe charge. Only requests issued since the last progress
    /// hold a charge, so this costs no more than those issues did.
    pub(in crate::zakura::block_sync) fn clear_liveness_charges(&mut self) {
        self.charged_for_liveness.clear();
    }

    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn first_key(&self) -> Option<RequestKey> {
        self.requests.keys().next().copied()
    }

    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn set_deadline(
        &mut self,
        key: RequestKey,
        deadline: Instant,
    ) {
        let Some(request) = self.requests.get_mut(&key) else {
            return;
        };
        self.by_deadline.remove(&(request.deadline, key));
        request.deadline = deadline;
        self.by_deadline.insert((deadline, key));
    }

    /// Recompute every index and total from the requests themselves.
    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn assert_consistent(&self) {
        let reserved = self.requests.values().fold(0u64, |acc, request| {
            acc.saturating_add(request.reserved_bytes())
        });
        assert_eq!(self.reserved_bytes, reserved);
        let starts: BTreeSet<_> = self
            .requests
            .iter()
            .map(|(key, request)| (request.request.start_height, *key))
            .collect();
        assert_eq!(self.by_start, starts);
        let ends: BTreeSet<_> = self
            .requests
            .iter()
            .map(|(key, request)| (request.request.end_height(), *key))
            .collect();
        assert_eq!(self.by_end, ends);
        let deadlines: BTreeSet<_> = self
            .requests
            .iter()
            .map(|(key, request)| (request.deadline, *key))
            .collect();
        assert_eq!(self.by_deadline, deadlines);
        assert!(self
            .unstarted
            .iter()
            .all(|key| self.requests.contains_key(key)));
        assert!(self
            .charged_for_liveness
            .iter()
            .all(|key| self.requests.contains_key(key)));
    }
}
