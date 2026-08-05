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
            refresh_interval: refresh_interval.max(MIN_PUBLICATION_INTERVAL),
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
