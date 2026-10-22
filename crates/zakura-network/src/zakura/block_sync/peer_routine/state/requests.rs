//! Request lifecycle decisions: deadlines, expiry, and retry avoidance.

use std::time::Instant;

use zakura_chain::block;

use super::{
    super::super::state::{OutstandingBlockRange, RequestKey, WriteOutcome},
    PeerState,
};

/// Requests that reached their deadline, split by how their write settled.
#[derive(Debug, Default)]
pub(in super::super) struct ExpiredRequests {
    /// Requests whose frame never reached the wire. They end without a fault.
    pub(in super::super) skipped: Vec<OutstandingBlockRange>,
    /// Started requests that the peer left unanswered.
    pub(in super::super) timed_out: Vec<OutstandingBlockRange>,
}

impl ExpiredRequests {
    pub(in super::super) fn is_empty(&self) -> bool {
        self.skipped.is_empty() && self.timed_out.is_empty()
    }
}

/// Wake deadlines that shared owners hold for this session.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(in super::super) struct SharedDeadlines {
    /// When the live requester abandons an exchange that still owes its ending.
    pub(in super::super) requester_retirement: Option<Instant>,
    /// When the floor watchdog stops excluding this peer from a height.
    pub(in super::super) floor_watchdog_avoid: Option<Instant>,
    /// When a body retry alarm stops excluding this peer from a body.
    pub(in super::super) body_retry_avoid: Option<Instant>,
    /// When the last fill pass's retry filter allows its returned work again.
    pub(in super::super) retry_filter: Option<Instant>,
}

impl PeerState {
    /// Whether a request deadline or the block-liveness deadline is due at `now`.
    pub(in super::super) fn deadlines_due(&self, now: Instant) -> bool {
        self.window
            .outstanding
            .earliest_deadline()
            .is_some_and(|deadline| deadline <= now)
            || self
                .window
                .block_liveness_deadline
                .is_some_and(|deadline| deadline <= now)
    }

    /// Requests whose deadline is at or before `now`, in issue order.
    pub(in super::super) fn due_requests(&self, now: Instant) -> Vec<RequestKey> {
        self.window.outstanding.due_at(now)
    }

    /// Retire due requests by how their write settled. Only a started write
    /// charges the peer with a timeout; a skipped write refunds its probe.
    pub(in super::super) fn expire_requests(
        &mut self,
        due: Vec<(RequestKey, WriteOutcome)>,
    ) -> ExpiredRequests {
        let (skipped, timed_out): (Vec<_>, Vec<_>) = due
            .into_iter()
            .partition(|(_, write)| *write == WriteOutcome::Skipped);
        // Retire skipped requests while the timed-out ones still occupy the
        // window, so their liveness deadline cannot be cleared as idle.
        let skipped: Vec<_> = skipped
            .into_iter()
            .filter_map(|(key, write)| self.window.retire_locally(key, write))
            .collect();
        let timed_out: Vec<_> = timed_out
            .into_iter()
            .filter_map(|(key, _)| self.window.outstanding.remove(key))
            .collect();
        if !timed_out.is_empty() {
            self.window.record_timeout(timed_out.len());
        }
        ExpiredRequests { skipped, timed_out }
    }

    /// Avoid re-taking `heights` until `until`, so another peer can contest
    /// them first. The heights stay pending for every other peer.
    pub(in super::super) fn avoid_retry(
        &mut self,
        heights: impl IntoIterator<Item = block::Height>,
        until: Instant,
    ) {
        for height in heights {
            self.retry_avoid.insert(height, until);
        }
    }

    /// Let this routine re-take heights whose avoidance ended by `now`.
    pub(in super::super) fn prune_retry_avoid(&mut self, now: Instant) {
        self.retry_avoid.retain(|_, until| *until > now);
    }

    pub(in super::super) fn is_retry_avoided(&self, height: block::Height) -> bool {
        self.retry_avoid.contains_key(&height)
    }

    /// When the earliest local retry avoidance ends.
    pub(in super::super) fn retry_avoid_expiry(&self) -> Option<Instant> {
        self.retry_avoid.values().min().copied()
    }

    /// The earliest instant at which this session must run again without an
    /// external event: a request or liveness deadline, a retry-avoid expiry, a
    /// deferred status, or a deadline that a shared owner holds.
    pub(in super::super) fn next_wake(&self, shared: SharedDeadlines) -> Option<Instant> {
        [
            self.window.outstanding.earliest_deadline(),
            self.window.block_liveness_deadline,
            shared.requester_retirement,
            self.retry_avoid_expiry(),
            shared.floor_watchdog_avoid,
            shared.body_retry_avoid,
            shared.retry_filter,
            self.pending_status
                .map(|_| self.inbound_status_meter.next_allowed),
        ]
        .into_iter()
        .flatten()
        .min()
    }
}
