//! Peer-local request policy for one block-sync session.

use std::{num::NonZeroU64, time::Instant};

use zakura_chain::block;

use super::{
    fill::{FillLimits, FillStop},
    DownloadWindow, ZakuraBlockSyncConfig,
};

/// What this session may request, learned from its peer's `Status`, and the
/// session's request identities.
///
/// `PeerState` holds no runtime handles. The routine passes in time and window
/// observations, and it executes the decisions that `PeerState` returns.
#[derive(Clone, Debug)]
pub(super) struct PeerState {
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
    pub(super) fn new(config: &ZakuraBlockSyncConfig) -> Self {
        Self {
            received_status: false,
            servable_low: block::Height::MIN,
            servable_high: block::Height::MIN,
            max_blocks_per_response: config.advertised_max_blocks_per_response(),
            max_response_bytes: config.advertised_max_response_bytes(),
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
        window: &DownloadWindow,
        now: Instant,
    ) -> Result<FillLimits, FillStop> {
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
