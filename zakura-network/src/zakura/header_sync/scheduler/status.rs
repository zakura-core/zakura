use std::time::Duration;

use tokio::time::Instant;

use super::super::Status;

const MIN_PUBLICATION_INTERVAL: Duration = Duration::from_secs(1);
const MAX_CHANGE_DELAY: Duration = Duration::from_secs(2);
const PUBLICATION_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Per-session status timing state.
#[derive(Clone, Debug)]
pub(in crate::zakura::header_sync) struct StatusPublisher {
    desired: Status,
    last_sent: Option<Status>,
    last_sent_at: Option<Instant>,
    pending_at: Option<Instant>,
    refresh_interval: Duration,
}

impl StatusPublisher {
    /// Start with an immediate publication for a newly negotiated session.
    pub(in crate::zakura::header_sync) fn new(
        desired: Status,
        refresh_interval: Duration,
        now: Instant,
    ) -> Self {
        Self {
            desired,
            last_sent: None,
            last_sent_at: None,
            pending_at: Some(now),
            refresh_interval,
        }
    }

    /// Coalesce a newly committed advertisement behind the one-per-second floor.
    pub(in crate::zakura::header_sync) fn observe(
        &mut self,
        desired: Status,
        committed_at: Instant,
    ) {
        if self.desired == desired && self.last_sent.as_ref() == Some(&desired) {
            return;
        }
        self.desired = desired;
        let floor = self
            .last_sent_at
            .map_or(committed_at, |sent_at| sent_at + MIN_PUBLICATION_INTERVAL);
        self.pending_at = Some(floor.max(committed_at).min(committed_at + MAX_CHANGE_DELAY));
    }

    pub(in crate::zakura::header_sync) fn next_deadline(&self) -> Instant {
        if let Some(pending_at) = self.pending_at {
            pending_at
        } else {
            self.last_sent_at
                .expect("a matching sent status has a publication time")
                + self.refresh_interval
        }
    }

    pub(in crate::zakura::header_sync) fn due(&self, now: Instant) -> bool {
        now >= self.next_deadline()
    }

    pub(in crate::zakura::header_sync) fn desired(&self) -> Status {
        self.desired.clone()
    }

    pub(in crate::zakura::header_sync) fn record_sent(&mut self, sent: Status, now: Instant) {
        self.last_sent = Some(sent);
        self.last_sent_at = Some(now);
        self.pending_at = None;
    }

    pub(in crate::zakura::header_sync) fn record_failed(&mut self, now: Instant) {
        let floor = self
            .last_sent_at
            .map_or(now, |sent_at| sent_at + MIN_PUBLICATION_INTERVAL);
        self.pending_at = Some((now + PUBLICATION_RETRY_DELAY).max(floor));
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::{block, work::difficulty::U256};

    use super::*;

    fn status(marker: u8) -> Status {
        Status {
            work_anchor_height: block::Height(1),
            work_anchor_hash: block::Hash([1; 32]),
            selected_tip_height: block::Height(u32::from(marker)),
            selected_tip_hash: block::Hash([marker; 32]),
            suffix_cumulative_work: U256::from(u64::from(marker)),
            oldest_retained_height: block::Height(1),
            max_headers_per_response: 1,
            max_inflight_requests: 1,
            max_message_bytes: 1,
            tree_aux_schema_mask: 1,
        }
    }

    #[test]
    fn initial_refresh_and_coalesced_change_deadlines_are_bounded() {
        let now = Instant::now();
        let mut publisher = StatusPublisher::new(status(1), Duration::from_secs(30), now);
        assert!(publisher.due(now));

        publisher.record_sent(status(1), now);
        assert_eq!(publisher.next_deadline(), now + Duration::from_secs(30));

        publisher.observe(status(2), now + Duration::from_millis(100));
        publisher.observe(status(3), now + Duration::from_millis(200));
        assert_eq!(publisher.desired(), status(3));
        assert_eq!(publisher.next_deadline(), now + Duration::from_secs(1));
        assert!(!publisher.due(now + Duration::from_millis(999)));
        assert!(publisher.due(now + Duration::from_secs(1)));

        publisher.record_sent(status(3), now + Duration::from_secs(1));
        let refresh_at = now + Duration::from_secs(31);
        assert!(publisher.due(refresh_at));
        publisher.record_failed(refresh_at);
        assert_eq!(
            publisher.next_deadline(),
            refresh_at + PUBLICATION_RETRY_DELAY
        );
    }
}
