//! Per-peer delivery of the latest local serving range.

use super::{state::RateMeter, BlockSyncStatus, Duration, Instant};

const RANGE_CORRECTION_INTERVAL: Duration = Duration::from_secs(1);
const QUEUE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// A queued frame is the delivery boundary of the ordered, reliable stream.
/// Failed attempts retain the difference from the latest local status. There is
/// no cached retry payload to become stale while the outbound queue is full.
#[derive(Debug)]
pub(super) struct StatusDelivery {
    last_queued: Option<BlockSyncStatus>,
    updates: RateMeter,
    legacy: Option<LegacyStatusDelivery>,
    queue_retry_at: Option<Instant>,
    queue_retry_interval: Duration,
}

/// Version 2 keeps prompt corrections separate from ordinary growth updates.
#[derive(Debug)]
struct LegacyStatusDelivery {
    contraction: RateMeter,
    handshake_at: Instant,
}

impl StatusDelivery {
    pub(super) fn new(interval: Duration, now: Instant, regulated: bool) -> Self {
        let interval = interval.max(Duration::from_millis(1));
        Self {
            last_queued: None,
            updates: RateMeter {
                next_allowed: now,
                interval,
            },
            legacy: (!regulated).then_some(LegacyStatusDelivery {
                contraction: RateMeter {
                    next_allowed: now,
                    interval: interval.min(RANGE_CORRECTION_INTERVAL),
                },
                handshake_at: now,
            }),
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
        let deadline = if let Some(legacy) = &self.legacy {
            match self.last_queued {
                None => legacy.handshake_at,
                Some(previous) if contracts(previous, latest) => legacy.contraction.next_allowed,
                Some(previous) if previous != latest => self.updates.next_allowed,
                Some(_) if !received_status => legacy.handshake_at,
                Some(_) => return None,
            }
        } else {
            if self.last_queued == Some(latest) && received_status {
                return None;
            }
            self.updates.next_allowed
        };
        Some(
            self.queue_retry_at
                .map_or(deadline, |retry| deadline.max(retry)),
        )
    }

    /// Advance the update allowance only after a frame enters the stream.
    pub(super) fn queued(&mut self, status: BlockSyncStatus, now: Instant) {
        if let Some(legacy) = &mut self.legacy {
            if let Some(previous) = self.last_queued.filter(|previous| *previous != status) {
                if contracts(previous, status) {
                    legacy.contraction.mark_taken(now);
                } else {
                    self.updates.mark_taken(now);
                }
            }
            legacy.handshake_at = now + self.updates.interval;
        } else {
            self.updates.mark_taken(now);
        }
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
    fn legacy_pruning_and_replacement_keep_their_prompt_status_deadlines() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(30), now, false);
        delivery.queued(status(1_000, 6_000), now);
        delivery.queued(status(1_000, 6_001), now + Duration::from_millis(500));
        let prune_at = now + Duration::from_millis(501);
        let first_prune = status(1_001, 6_001);
        assert!(delivery.next_deadline(first_prune, true).unwrap() <= prune_at);
        delivery.queued(first_prune, prune_at);
        for low in 1_002..2_000 {
            assert_eq!(
                delivery.next_deadline(status(low, 6_002), true),
                Some(prune_at + RANGE_CORRECTION_INTERVAL)
            );
        }
        delivery.queue_full(prune_at);
        assert_eq!(
            delivery.next_deadline(status(0, 0), true),
            Some(prune_at + RANGE_CORRECTION_INTERVAL)
        );

        let replacement = StatusDelivery::new(Duration::from_secs(30), prune_at, false);
        assert_eq!(
            replacement.next_deadline(status(0, 0), false),
            Some(prune_at)
        );
    }

    #[test]
    fn growth_and_corrections_share_the_update_deadline() {
        let now = Instant::now();
        let interval = Duration::from_secs(30);
        let mut delivery = StatusDelivery::new(interval, now, true);
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
        let mut delivery = StatusDelivery::new(Duration::from_secs(30), now, true);
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
        let mut delivery = StatusDelivery::new(interval, now, true);
        let latest = status(1_000, 6_000);
        assert_eq!(delivery.next_deadline(latest, false), Some(now));
        delivery.queued(latest, now);
        assert_eq!(delivery.next_deadline(latest, false), Some(now + interval));
        assert_eq!(delivery.next_deadline(latest, true), None);
    }

    #[test]
    fn returning_to_the_queued_range_cancels_a_failed_correction() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(30), now, true);
        let initial = status(1_000, 6_000);
        delivery.queued(initial, now);
        delivery.queue_full(now);
        assert!(delivery.next_deadline(status(1_001, 6_001), true).is_some());
        assert_eq!(delivery.next_deadline(initial, true), None);
    }

    #[test]
    fn a_connection_deadline_defers_updates_without_spending_the_allowance() {
        let now = Instant::now();
        let mut delivery = StatusDelivery::new(Duration::from_secs(1), now, true);
        let latest = status(1_000, 6_000);
        let deadline = now + Duration::from_secs(30);
        delivery.queue_full(now);
        delivery.defer_until(deadline);
        assert_eq!(delivery.next_deadline(latest, false), Some(deadline));
        assert_eq!(delivery.last_queued, None);
    }
}
