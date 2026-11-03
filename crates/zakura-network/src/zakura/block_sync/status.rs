//! Per-peer delivery of the latest local serving range.

use super::{BlockSyncStatus, Duration, Instant};

const QUEUE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// A queued frame is the delivery boundary of the ordered, reliable stream.
/// This tracks whether the peer still needs the latest local status, and when
/// another attempt can succeed. The connection's Status cadence owns pacing and
/// the unsent value, so a failed attempt leaves no stale retry payload here.
#[derive(Debug)]
pub(super) struct StatusDelivery {
    last_queued: Option<BlockSyncStatus>,
    retry_at: Instant,
}

impl StatusDelivery {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            last_queued: None,
            retry_at: now,
        }
    }

    /// A send is due only while a range change or handshake remains pending.
    pub(super) fn next_deadline(
        &self,
        latest: BlockSyncStatus,
        received_status: bool,
    ) -> Option<Instant> {
        (self.last_queued != Some(latest) || !received_status).then_some(self.retry_at)
    }

    pub(super) fn queued(&mut self, status: BlockSyncStatus) {
        self.last_queued = Some(status);
    }

    /// The cadence allows the next send at `at`.
    pub(super) fn deferred_until(&mut self, at: Instant) {
        self.retry_at = at;
    }

    /// Back off a full queue or an unwritten previous Status without waiting
    /// for the next cadence interval.
    pub(super) fn queue_full(&mut self, now: Instant) {
        self.retry_at = now + QUEUE_RETRY_INTERVAL;
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
    fn a_deferred_correction_retries_at_the_cadence_deadline() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(now);
        let initial = status(1_000, 6_000);
        assert_eq!(delivery.next_deadline(initial, true), Some(now));
        delivery.queued(initial);
        assert_eq!(delivery.next_deadline(initial, true), None);
        let correction = status(1_000, 6_001);
        assert_eq!(delivery.next_deadline(correction, true), Some(now));
        let cadence = now + Duration::from_secs(30);
        delivery.deferred_until(cadence);
        assert_eq!(delivery.next_deadline(correction, true), Some(cadence));
        delivery.queued(correction);
        assert_eq!(delivery.next_deadline(correction, true), None);
    }

    #[test]
    fn queue_pressure_retries_the_latest_range_at_its_own_deadline() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(now);
        let initial = status(1_000, 6_000);
        delivery.queued(initial);
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
        delivery.queued(latest);
        assert_eq!(delivery.next_deadline(latest, true), None);
    }

    #[test]
    fn unchanged_status_retries_only_until_the_peer_supplies_its_status() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(now);
        let latest = status(1_000, 6_000);
        assert_eq!(delivery.next_deadline(latest, false), Some(now));
        delivery.queued(latest);
        // The handshake keeps an unchanged Status due. The connection's cadence
        // paces the resend; see the reactor test for that deferral.
        assert_eq!(delivery.next_deadline(latest, false), Some(now));
        assert_eq!(delivery.next_deadline(latest, true), None);
    }

    #[test]
    fn returning_to_the_queued_range_cancels_a_failed_correction() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(now);
        let initial = status(1_000, 6_000);
        delivery.queued(initial);
        delivery.queue_full(now);
        assert!(delivery.next_deadline(status(1_001, 6_001), true).is_some());
        assert_eq!(delivery.next_deadline(initial, true), None);
    }
}
