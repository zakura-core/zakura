//! Status decisions: request limits, publication cadence, and requester stops.

use std::time::Instant;

use super::{
    super::{
        super::{
            config::{
                clamp_advertised_blocks, clamp_advertised_inflight, clamp_advertised_response_bytes,
            },
            regulated::live_requester::{RequesterOccupancy, MAX_PENDING_ENDINGS},
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
        let limits = (
            clamp_advertised_blocks(status.max_blocks_per_response),
            clamp_advertised_response_bytes(status.max_response_bytes),
            clamp_advertised_inflight(status.max_inflight_requests),
        );
        // Requests must obey the peer's latest limits even while publication waits.
        (
            self.max_blocks_per_response,
            self.max_response_bytes,
            self.window.max_inflight_requests,
        ) = limits;
        let limits_changed = limits != self.published_limits;
        if self.inbound_status_meter.try_take(now) {
            self.status_range_bypass_available = true;
        } else if range_changed && self.status_range_bypass_available {
            // Allow prompt pruning updates, but only once per window so a peer
            // cannot flood registry/reactor work by oscillating its range.
            self.status_range_bypass_available = false;
        } else {
            // Keep only the latest Status that changes the published range or
            // limits. A return to the published state cancels an older pending
            // change without causing a status reply loop.
            self.pending_status = (range_changed || limits_changed).then_some(status);
            return StatusUpdate::Deferred;
        }
        self.pending_status = None;
        // Replies have their own allowance. The reactor also retries any local
        // status that has not entered this peer's outbound queue.
        let send_reply = self.status_reply_meter.try_take(now);
        self.received_status = true;
        self.servable_low = status.servable_low;
        self.servable_high = status.servable_high;
        self.published_limits = limits;
        StatusUpdate::Publish { send_reply }
    }

    /// The deferred status, once the publication meter allows it at `now`.
    pub(in super::super) fn take_due_status(&mut self, now: Instant) -> Option<BlockSyncStatus> {
        if !self.inbound_status_meter.is_ready(now) {
            return None;
        }
        self.pending_status.take()
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
