//! Per-peer delivery of the latest local serving range.

use super::{state::RateMeter, BlockSyncStatus, Duration, Instant};

const QUEUE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// A queued frame is the delivery boundary of the ordered, reliable stream.
/// Failed attempts retain the difference from the latest local status. There is
/// no cached retry payload to become stale while the outbound queue is full.
#[derive(Debug)]
pub(super) struct StatusDelivery {
    last_queued: Option<BlockSyncStatus>,
    updates: RateMeter,
    queue_retry_at: Option<Instant>,
    queue_retry_interval: Duration,
}

impl StatusDelivery {
    pub(super) fn new(interval: Duration, now: Instant) -> Self {
        let interval = interval.max(Duration::from_millis(1));
        Self {
            last_queued: None,
            updates: RateMeter {
                next_allowed: now,
                interval,
            },
            queue_retry_at: None,
            queue_retry_interval: interval.min(QUEUE_RETRY_INTERVAL),
        }
    }

    /// The next send is due only while a range change or handshake remains pending.
    pub(super) fn next_deadline(
        &self,
        latest: BlockSyncStatus,
        received_status: bool,
    ) -> Option<Instant> {
        if self.last_queued == Some(latest) && received_status {
            return None;
        }
        let deadline = self.updates.next_allowed;
        Some(
            self.queue_retry_at
                .map_or(deadline, |retry| deadline.max(retry)),
        )
    }

    /// Advance the update allowance only after a frame enters the stream.
    pub(super) fn queued(&mut self, status: BlockSyncStatus, now: Instant) {
        self.updates.mark_taken(now);
        self.last_queued = Some(status);
        self.queue_retry_at = None;
    }

    pub(super) fn defer_until(&mut self, deadline: Instant) {
        self.queue_retry_at = Some(
            self.queue_retry_at
                .map_or(deadline, |retry| retry.max(deadline)),
        );
    }

    /// Back off failed queue attempts without spending the range-change allowance.
    pub(super) fn queue_full(&mut self, now: Instant) {
        self.queue_retry_at = Some(now + self.queue_retry_interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::block_sync::{block, MAX_BS_RESPONSE_BYTES};

    fn status(low: u32, high: u32) -> BlockSyncStatus {
        BlockSyncStatus {
            servable_low: block::Height(low),
            servable_high: block::Height(high),
            tip_hash: block::Hash([0; 32]),
            max_blocks_per_response: 1,
            max_inflight_requests: 1,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        }
    }

    #[test]
    fn growth_and_corrections_share_the_update_deadline() {
        let now = Instant::now();
        let interval = Duration::from_secs(30);
        let mut delivery = StatusDelivery::new(interval, now);
        let initial = status(1_000, 6_000);
        assert_eq!(delivery.next_deadline(initial, true), Some(now));
        delivery.queued(initial, now);
        for latest in [status(1_000, 6_001), status(1_001, 6_001), status(0, 0)] {
            assert_eq!(delivery.next_deadline(latest, true), Some(now + interval));
        }
        let latest = status(1_999, 6_002);
        delivery.queued(latest, now + interval);
        assert_eq!(delivery.next_deadline(latest, true), None);
        assert_eq!(
            delivery.next_deadline(status(2_000, 6_003), true),
            Some(now + interval * 2)
        );
    }

    #[test]
    fn queue_pressure_retries_the_latest_range_at_its_own_deadline() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(30), now);
        let initial = status(1_000, 6_000);
        delivery.queued(initial, now);
        let full_at = now + Duration::from_secs(30);
        delivery.queue_full(full_at);
        let latest = status(1_002, 6_002);
        let deadline = full_at + QUEUE_RETRY_INTERVAL;
        assert_eq!(delivery.next_deadline(latest, true), Some(deadline));
        assert_eq!(delivery.last_queued, Some(initial));

        delivery.queue_full(deadline);
        assert_eq!(
            delivery.next_deadline(latest, true),
            Some(deadline + QUEUE_RETRY_INTERVAL)
        );
        delivery.queued(latest, deadline + QUEUE_RETRY_INTERVAL);
        assert_eq!(delivery.next_deadline(latest, true), None);
    }

    #[test]
    fn unchanged_status_retries_only_until_the_peer_supplies_its_status() {
        let now = Instant::now();
        let interval = Duration::from_secs(30);
        let mut delivery = StatusDelivery::new(interval, now);
        let latest = status(1_000, 6_000);
        assert_eq!(delivery.next_deadline(latest, false), Some(now));
        delivery.queued(latest, now);
        assert_eq!(delivery.next_deadline(latest, false), Some(now + interval));
        assert_eq!(delivery.next_deadline(latest, true), None);
    }

    #[test]
    fn returning_to_the_queued_range_cancels_a_failed_correction() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(30), now);
        let initial = status(1_000, 6_000);
        delivery.queued(initial, now);
        delivery.queue_full(now);
        assert!(delivery.next_deadline(status(1_001, 6_001), true).is_some());
        assert_eq!(delivery.next_deadline(initial, true), None);
    }

    #[test]
    fn a_connection_deadline_defers_updates_without_spending_the_allowance() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(1), now);
        let latest = status(1_000, 6_000);
        let deadline = now + Duration::from_secs(30);
        delivery.queue_full(now);
        delivery.defer_until(deadline);
        assert_eq!(delivery.next_deadline(latest, false), Some(deadline));
        assert_eq!(delivery.last_queued, None);
    }
}
