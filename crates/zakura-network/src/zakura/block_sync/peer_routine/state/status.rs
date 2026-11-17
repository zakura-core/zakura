//! Status decisions: request limits, publication cadence, and requester stops.

use zakura_rt::time::Instant;

use super::{
    super::{
        super::{
            config::{
                clamp_advertised_blocks, clamp_advertised_inflight, clamp_advertised_response_bytes,
            },
            regulated::{
                live_requester::{RequesterOccupancy, MAX_PENDING_ENDINGS},
                status_sender::ReceivedStatus,
            },
        },
        fill::FillStop,
        BlockSyncStatus,
    },
    PeerState,
};

/// What an inbound `Status` changes beyond the request limits, which apply at once.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in super::super) enum StatusUpdate {
    /// The publication meter deferred the status. A later
    /// [`take_due_status`](PeerState::take_due_status) returns it if it
    /// still changes the published range or limits.
    Deferred,
    /// Publish the status to the registry and the reactor. `send_reply` says
    /// whether the reply allowance lets our own `Status` answer it.
    Publish { send_reply: bool },
}

impl PeerState {
    /// Apply this peer's `Status` at `now`. Requests obey the peer's latest
    /// limits at once; the servable range and the published limits change only
    /// when the publication meter allows it.
    pub(in super::super) fn on_status(
        &mut self,
        status: BlockSyncStatus,
        now: Instant,
    ) -> StatusUpdate {
        let range_changed =
            status.servable_high != self.servable_high || status.servable_low != self.servable_low;
        let limits = clamped_limits(status);
        // Requests must obey the peer's latest limits even while publication waits.
        self.set_request_limits(limits);
        let limits_changed = limits != self.published_limits;
        let pacing = &mut self.status_pacing;
        if pacing.apply_meter.try_take(now) {
            pacing.range_bypass_available = true;
        } else if range_changed && pacing.range_bypass_available {
            // Allow prompt pruning updates, but only once per window so a peer
            // cannot flood registry/reactor work by oscillating its range.
            pacing.range_bypass_available = false;
        } else {
            // Keep only the latest Status that changes the published range or
            // limits. A return to the published state cancels an older pending
            // change without causing a status reply loop.
            pacing.pending = (range_changed || limits_changed).then_some(status);
            return StatusUpdate::Deferred;
        }
        pacing.pending = None;
        // Replies have their own allowance. The reactor also retries any local
        // status that has not entered this peer's outbound queue.
        let send_reply = pacing.reply_meter.try_take(now);
        self.apply_status(status, limits);
        StatusUpdate::Publish { send_reply }
    }

    /// Resume the inbound Status state that an earlier stream-6 session of
    /// this connection stored. `applied` was paced when it arrived, so it
    /// applies without spending a meter, and our reply is not sent again.
    /// A deferred Status stays deferred and returns once the restored meter
    /// allows it.
    pub(in super::super) fn resume_status(&mut self, received: ReceivedStatus) {
        let ReceivedStatus { applied, pacing } = received;
        let limits = clamped_limits(applied);
        self.apply_status(applied, limits);
        // As in `on_status`, requests obey the latest limits, including a deferred Status's.
        self.set_request_limits(pacing.pending.map_or(limits, clamped_limits));
        self.status_pacing = pacing;
    }

    fn set_request_limits(&mut self, limits: (u32, u32, u32)) {
        (
            self.max_blocks_per_response,
            self.max_response_bytes,
            self.window.max_inflight_requests,
        ) = limits;
    }

    /// Publish `status` as this session's range and `limits` as its published limits.
    fn apply_status(&mut self, status: BlockSyncStatus, limits: (u32, u32, u32)) {
        self.received_status = true;
        self.servable_low = status.servable_low;
        self.servable_high = status.servable_high;
        self.published_limits = limits;
    }

    /// The deferred status, once the publication meter allows it at `now`.
    pub(in super::super) fn take_due_status(&mut self, now: Instant) -> Option<BlockSyncStatus> {
        if !self.status_pacing.apply_meter.is_ready(now) {
            return None;
        }
        self.status_pacing.pending.take()
    }

    /// Whether a status must republish the slots because the requester's stop
    /// changed. A stop publishes even when the meter defers the status, so no
    /// routine keeps deferring the floor to this peer. A reopen waits for a
    /// rate-limited publication, so a `Status` flood cannot toggle the shared
    /// ranking on every frame.
    pub(in super::super) fn status_changes_published_stop(
        &self,
        update: StatusUpdate,
        requester_stopped: bool,
    ) -> bool {
        match update {
            StatusUpdate::Deferred => requester_stopped && !self.published_requester_stopped,
            StatusUpdate::Publish { .. } => requester_stopped != self.published_requester_stopped,
        }
    }

    /// Why the live requester refuses another request, whatever the window
    /// allows, given one read of its `occupancy`.
    pub(in super::super) fn requester_stop(
        &self,
        occupancy: RequesterOccupancy,
    ) -> Option<FillStop> {
        // Count unanswered exchanges, including scheduler-abandoned work.
        // The peer's u32 advertised limit fits usize on supported targets.
        if occupancy.at_capacity || occupancy.len >= self.window.max_inflight_requests as usize {
            return Some(FillStop::CwndSaturated);
        }
        if occupancy.pending_endings >= MAX_PENDING_ENDINGS {
            return Some(FillStop::PendingEndings);
        }
        None
    }
}

/// The request limits `status` advertises, clamped to what this node accepts.
fn clamped_limits(status: BlockSyncStatus) -> (u32, u32, u32) {
    (
        clamp_advertised_blocks(status.max_blocks_per_response),
        clamp_advertised_response_bytes(status.max_response_bytes),
        clamp_advertised_inflight(status.max_inflight_requests),
    )
}
