//! Liveness and exit decisions: no-progress stalls, write congestion, resets,
//! and stream failure.

use std::time::Duration;

use zakura_rt::time::Instant;

use super::{super::super::state::LivenessOutcome, super::OrderedStreamFailure, PeerState};

/// How the routine answers a no-progress stall.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) enum NoProgressResponse {
    /// Park the session for the cooldown, then let the peer back in.
    Park,
    /// Close the connection.
    Disconnect,
}

/// What a block-liveness check at a deadline decided.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) enum LivenessDecision {
    /// The peer made progress in time, or this check granted a bounded delay.
    Continue,
    /// The peer made no accepted block progress before the deadline.
    Stall,
}

impl PeerState {
    /// Track the current interval in which this peer's outbound queue stayed
    /// full. A peer that stopped reading keeps the queue full until the
    /// interval reaches `request_timeout`, and then it loses its grace.
    pub(in super::super) fn note_outbound_capacity(&mut self, has_capacity: bool, now: Instant) {
        if has_capacity {
            self.outbound_full_since = None;
        } else if self.outbound_full_since.is_none() {
            self.outbound_full_since = Some(now);
        }
    }

    /// Check block liveness at `now`. A deadline that falls during brief
    /// outbound write congestion gets one bounded delay; see
    /// [`liveness_grace_allowed`].
    pub(in super::super) fn check_block_liveness(
        &mut self,
        now: Instant,
        outbound_full: bool,
        request_timeout: Duration,
    ) -> LivenessDecision {
        match self.window.check_liveness(now) {
            LivenessOutcome::Ok => LivenessDecision::Continue,
            LivenessOutcome::Disarm => {
                self.window.clear_liveness_if_idle();
                LivenessDecision::Continue
            }
            LivenessOutcome::Park
                if liveness_grace_allowed(
                    outbound_full,
                    self.outbound_full_since,
                    now,
                    request_timeout,
                ) =>
            {
                // Outbound full but *only just* filled (< one `request_timeout` of
                // continuous backpressure): plausibly transient local write congestion, not
                // a dead peer. Grant one short, BOUNDED grace. This is the *only* liveness
                // extension: a peer that stopped reading holds outbound full past
                // `request_timeout`, falls through to the park arm, and is parked at the
                // liveness deadline — it cannot dodge the timer.
                self.window.extend_liveness_deadline(now, request_timeout);
                LivenessDecision::Continue
            }
            LivenessOutcome::Park => LivenessDecision::Stall,
        }
    }

    /// How this connection answers a no-progress stall. A connection gets one
    /// park; a repeated stall disconnects it.
    pub(in super::super) fn no_progress_response(&self) -> NoProgressResponse {
        no_progress_response(self.allow_no_progress_park)
    }

    /// Why a failed stream stalls this session, or `None` when the session
    /// owes nothing. Request expiry can empty the window without disarming
    /// liveness, so a pending no-progress deadline still counts.
    pub(in super::super) fn stream_failure_stall(
        &self,
        failure: OrderedStreamFailure,
    ) -> Option<&'static str> {
        if self.window.outstanding.is_empty() && self.window.block_liveness_deadline.is_none() {
            return None;
        }
        Some(match failure {
            OrderedStreamFailure::RemoteClose => {
                "block-sync peer closed the stream with its requests unanswered"
            }
            OrderedStreamFailure::WriteTimeout => {
                "block-sync stream write stalled with download requests unanswered"
            }
        })
    }

    /// Record the sequencer's `reset_epoch`. Returns whether it changed: a
    /// destructive reset rather than an advance.
    pub(in super::super) fn observe_reset_epoch(&mut self, reset_epoch: u64) -> bool {
        if reset_epoch == self.last_reset_epoch {
            return false;
        }
        self.last_reset_epoch = reset_epoch;
        true
    }

    /// Forget the retry bias and the no-progress probe streak after this node
    /// returned the session's requests on its own initiative. An unproven peer
    /// whose only probe was in flight can then probe again.
    pub(in super::super) fn note_locally_returned_requests(&mut self) {
        self.retry_avoid.clear();
        self.window.note_locally_returned_requests();
    }

    /// Record the work queue's revocation count. Returns whether another
    /// actor revoked work since the last check.
    pub(in super::super) fn observe_revocations(&mut self, revocations: u64) -> bool {
        if revocations == self.checked_revocations {
            return false;
        }
        self.checked_revocations = revocations;
        true
    }
}

pub(in super::super) fn no_progress_response(allow_no_progress_park: bool) -> NoProgressResponse {
    if allow_no_progress_park {
        NoProgressResponse::Park
    } else {
        NoProgressResponse::Disconnect
    }
}

/// Whether the routine grants one bounded delay at a block-liveness deadline.
/// The routine grants the delay only for transient outbound write congestion that
/// lasts less than `request_timeout`. A peer that stops reading keeps the outbound
/// queue full. The routine disconnects that peer when the interval reaches
/// `request_timeout`.
pub(in super::super) fn liveness_grace_allowed(
    outbound_full: bool,
    outbound_full_since: Option<Instant>,
    now: Instant,
    request_timeout: Duration,
) -> bool {
    outbound_full
        && outbound_full_since
            .is_some_and(|since| now.saturating_duration_since(since) < request_timeout)
}
