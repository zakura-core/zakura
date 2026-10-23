//! Peer-local request policy for one block-sync session.

use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    time::{Duration, Instant},
};

use zakura_chain::block;

use super::{
    super::state::RateMeter,
    fill::{FillLimits, FillStop},
    BlockSyncStatus, DownloadWindow, ZakuraBlockSyncConfig,
};

mod liveness;
mod requests;
mod status;
#[cfg(any(test, feature = "p2p-fuzz"))]
mod tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use tests::{replay_request_lifecycle_bytes, RequestLifecycleReplaySummary};

#[cfg(test)]
pub(super) use liveness::{liveness_grace_allowed, no_progress_response};
pub(super) use liveness::{LivenessDecision, NoProgressResponse};
pub(super) use requests::{unreceived_heights, BodyMatch, Disposition, SharedDeadlines};
pub(super) use status::StatusUpdate;

/// One block-sync session's download, status, and liveness facts.
///
/// `PeerState` holds no runtime handles. The routine passes in time and
/// observations from shared owners, and it executes the decisions that
/// `PeerState` returns. One `PeerState` belongs to one session generation, so
/// a [`RequestKey`](super::RequestKey) identifies one request of that session.
#[derive(Clone, Debug)]
pub(super) struct PeerState {
    /// This session's outstanding requests and congestion window.
    pub(super) window: DownloadWindow,
    /// Heights this routine recently returned on a failure, mapped to the instant
    /// after which it may re-take them. While avoided, the routine leaves the
    /// height `pending` (contestable by any other peer) but does not re-grab it
    /// itself — the peer-local retry bias. Pruned on expiry each fill pass.
    pub(super) retry_avoid: BTreeMap<block::Height, Instant>,
    /// Whether the last fill stopped because the node-wide request pool was
    /// empty, so the routine waits for a pool entry.
    pub(super) request_pool_waiting: bool,
    /// A connection gets one local no-progress park/re-admission cycle. A
    /// repeated stall is connection-fatal so it cannot reclaim download slots
    /// indefinitely without paying the redial cost.
    pub(super) allow_no_progress_park: bool,
    /// Rate meter for sending our `Status` reply to this peer's inbound `Status`.
    /// The reply decision is routine-local; the actual send stays reactor-side via
    /// `RoutineToReactor::StatusReceived`.
    pub(super) status_reply_meter: RateMeter,
    /// Rate meter for status application, with one extra range change per window.
    pub(super) inbound_status_meter: RateMeter,
    /// Allows one immediate range change without permitting an unbounded bypass.
    pub(super) status_range_bypass_available: bool,
    /// Clamped limits last published to the registry. Request issuance uses the
    /// latest limits immediately; only publication waits for the meter.
    pub(super) published_limits: (u32, u32, u32),
    /// Whether the last slot publication showed a requester that refuses work.
    pub(super) published_requester_stopped: bool,
    /// Latest deferred range change. Applying it on a timer preserves one-shot updates.
    pub(super) pending_status: Option<BlockSyncStatus>,
    /// Last `reset_epoch` that this routine processed.
    /// A `view.changed()` event uses the epoch to distinguish a reset from an advance.
    pub(super) last_reset_epoch: u64,
    /// Work-queue revocation count when this routine last checked its requests
    /// for lost ownership.
    pub(super) checked_revocations: u64,
    /// Start of the current interval in which this peer's outbound queue stayed full.
    /// The liveness check uses the interval to distinguish congestion from a peer that stopped reading.
    pub(super) outbound_full_since: Option<Instant>,
    /// Whether this peer has sent a `Status` yet (gates want-work; mirrored into
    /// the registry for the reactor's candidate reads).
    pub(super) received_status: bool,
    /// This peer's advertised servable range, learned from its `Status`. The
    /// want-work upper bound; never the floor.
    pub(super) servable_low: block::Height,
    pub(super) servable_high: block::Height,
    /// This peer's clamped advertised serving caps, learned from its `Status`.
    /// Authoritative for the routine's own want-work decision (mirrored into the
    /// registry for the reactor's candidate reads).
    pub(super) max_blocks_per_response: u32,
    pub(super) max_response_bytes: u32,
    /// Next request identity in this peer-session generation. Exhaustion fails
    /// closed instead of reusing an owner.
    pub(super) next_request_id: Option<NonZeroU64>,
}

impl PeerState {
    pub(super) fn new(
        config: &ZakuraBlockSyncConfig,
        allow_no_progress_park: bool,
        reset_epoch: u64,
        revocations: u64,
    ) -> Self {
        let window = DownloadWindow::new(config);
        let max_blocks_per_response = config.advertised_max_blocks_per_response();
        let max_response_bytes = config.advertised_max_response_bytes();
        Self {
            published_limits: (
                max_blocks_per_response,
                max_response_bytes,
                window.max_inflight_requests,
            ),
            window,
            retry_avoid: BTreeMap::new(),
            request_pool_waiting: false,
            allow_no_progress_park,
            status_reply_meter: RateMeter::new(config.status_refresh_interval),
            inbound_status_meter: RateMeter::new(
                config.status_refresh_interval.min(Duration::from_secs(1)),
            ),
            status_range_bypass_available: false,
            published_requester_stopped: false,
            pending_status: None,
            last_reset_epoch: reset_epoch,
            checked_revocations: revocations,
            outbound_full_since: None,
            received_status: false,
            servable_low: block::Height::MIN,
            servable_high: block::Height::MIN,
            max_blocks_per_response,
            max_response_bytes,
            next_request_id: NonZeroU64::new(1),
        }
    }

    /// Whether this peer's latest applied `Status` covers `height`.
    pub(super) fn serves(&self, height: block::Height) -> bool {
        self.received_status && self.servable_low <= height && height <= self.servable_high
    }

    /// Decide whether this peer may attempt another request at `now`, and with
    /// which limits. The decision acquires nothing: the caller still acquires
    /// requester and transport capacity before it takes work.
    pub(super) fn fill_limits(
        &self,
        config: &ZakuraBlockSyncConfig,
        now: Instant,
    ) -> Result<FillLimits, FillStop> {
        let window = &self.window;
        if !self.received_status {
            return Err(FillStop::NoStatus);
        }
        if window.requests_without_block_progress >= window.no_progress_request_cap() {
            return Err(if window.has_block_progress() {
                FillStop::NoBlockProgressRequestCap
            } else {
                FillStop::InitialBlockProbeRequestCap
            });
        }
        // Floor bypass scaled by reliability: a healthy saturated carrier keeps the
        // full bypass so the floor keeps moving; a failing/sealed peer earns *no*
        // above-window slots even for a near-floor block.
        let base_floor_bonus = usize::try_from(config.floor_bypass_slots).unwrap_or(0);
        let floor_bonus = window.scaled_floor_bonus(base_floor_bonus);
        // Stop only when even a bypassed floor request has no slot. A cwnd that is
        // saturated for above-floor work (no normal slot) still leaves up to
        // `floor_bonus` slots so the lowest missing height keeps moving — unless the
        // peer is sealed (`floor_bonus` is 0), which gets no work.
        if window.available_slots_with_bonus_at(floor_bonus, now) == 0 {
            return Err(FillStop::CwndSaturated);
        }
        Ok(FillLimits {
            servable_low: self.servable_low,
            servable_high: self.servable_high,
            floor_bypass_only: window.available_slots_at(now) == 0,
            max_count: self.request_count_cap(config),
            response_byte_cap: u64::from(self.max_response_bytes.max(1)),
            // The floor bypass adds `floor_bonus` bodies of byte headroom.
            floor_cwnd_bytes: window.cwnd_byte_headroom_at(floor_bonus, now),
            above_floor_cwnd_bytes: window.cwnd_byte_headroom_at(0, now),
        })
    }

    /// Allocate the next request identity. A later failure of the same attempt
    /// still consumes it, so no two attempts in this generation share an owner.
    pub(super) fn allocate_request_id(&mut self) -> Option<NonZeroU64> {
        let request_id = self.next_request_id?;
        self.next_request_id = request_id.get().checked_add(1).and_then(NonZeroU64::new);
        Some(request_id)
    }

    /// The per-request block count: the peer's cap and ours, at least one.
    fn request_count_cap(&self, config: &ZakuraBlockSyncConfig) -> usize {
        usize::try_from(
            self.max_blocks_per_response
                .min(config.advertised_max_blocks_per_response())
                .max(1),
        )
        .unwrap_or(usize::MAX)
    }
}
