//! Per-peer delivery of the latest local serving range.

use super::{state::RateMeter, BlockSyncStatus, Duration, Instant};

/// Bound prompt range corrections independently of ordinary tip-growth updates.
pub(super) const RANGE_CORRECTION_INTERVAL: Duration = Duration::from_secs(1);
const QUEUE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// A queued frame is the delivery boundary of the ordered, reliable stream.
/// Failed attempts retain the difference from the latest local status. There is
/// no cached retry payload to become stale while the outbound queue is full.
#[derive(Debug)]
pub(super) struct StatusDelivery {
    last_queued: Option<BlockSyncStatus>,
    growth: RateMeter,
    contraction: RateMeter,
    handshake_at: Instant,
    queue_retry_at: Option<Instant>,
    queue_retry_interval: Duration,
}

impl StatusDelivery {
    pub(super) fn new(interval: Duration, now: Instant) -> Self {
        let interval = interval.max(Duration::from_millis(1));
        Self {
            last_queued: None,
            growth: RateMeter {
                next_allowed: now,
                interval,
            },
            contraction: RateMeter {
                next_allowed: now,
                interval: interval.min(RANGE_CORRECTION_INTERVAL),
            },
            handshake_at: now,
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
        let deadline = match self.last_queued {
            None => self.handshake_at,
            Some(previous) if previous != latest => {
                if contracts(previous, latest) {
                    self.contraction.next_allowed
                } else {
                    self.growth.next_allowed
                }
            }
            Some(_) if !received_status => self.handshake_at,
            Some(_) => return None,
        };
        Some(
            self.queue_retry_at
                .map_or(deadline, |retry| deadline.max(retry)),
        )
    }

    /// Advance only the allowance for the change that actually entered the stream.
    pub(super) fn queued(&mut self, status: BlockSyncStatus, now: Instant) {
        if let Some(previous) = self.last_queued.filter(|previous| *previous != status) {
            if contracts(previous, status) {
                self.contraction.mark_taken(now);
            } else {
                self.growth.mark_taken(now);
            }
        }
        self.last_queued = Some(status);
        self.handshake_at = now + self.growth.interval;
        self.queue_retry_at = None;
    }

    /// Back off failed queue attempts without spending the range-change allowance.
    pub(super) fn queue_full(&mut self, now: Instant) {
        self.queue_retry_at = Some(now + self.queue_retry_interval);
    }
}

fn contracts(previous: BlockSyncStatus, latest: BlockSyncStatus) -> bool {
    latest.servable_low > previous.servable_low || latest.servable_high < previous.servable_high
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
    fn pruning_corrects_growth_advertisements_without_an_unbounded_bypass() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(30), now);
        delivery.queued(status(1_000, 6_000), now);
        let growth_at = now + Duration::from_millis(500);
        delivery.queued(status(1_000, 6_001), growth_at);
        let prune_at = growth_at + Duration::from_millis(1);
        let first_prune = status(1_001, 6_001);
        assert!(delivery.next_deadline(first_prune, true).unwrap() <= prune_at);
        delivery.queued(first_prune, prune_at);

        // Further pruning coalesces into the latest range for this same deadline.
        for low in 1_002..2_000 {
            assert_eq!(
                delivery.next_deadline(status(low, 6_002), true),
                Some(prune_at + RANGE_CORRECTION_INTERVAL)
            );
        }
        delivery.queued(status(1_999, 6_002), prune_at + RANGE_CORRECTION_INTERVAL);
        assert_eq!(delivery.next_deadline(status(1_999, 6_002), true), None);
    }

    #[test]
    fn queue_pressure_retries_the_latest_range_at_its_own_deadline() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(30), now);
        let initial = status(1_000, 6_000);
        delivery.queued(initial, now);
        let full_at = now + Duration::from_millis(500);
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
    fn genesis_only_corrections_use_the_prompt_deadline() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(30), now);
        delivery.queued(status(1_000, 6_000), now);
        delivery.queued(status(1_000, 6_001), now);
        assert_eq!(delivery.next_deadline(status(0, 0), true), Some(now));
    }
}
