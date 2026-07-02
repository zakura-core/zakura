use std::time::{Duration, Instant};

use zebra_chain::block;

use super::{config::ZakuraBlockSyncConfig, state::next_height};

/// Delivery rate assumed when sizing an above-floor deadline for a peer whose
/// measured BtlBw is still near zero, so the patience window is bounded rather than
/// unbounded. A worst-case `MAX_BLOCK_BYTES` body at this rate transfers in ~8 s, so
/// with the `request_timeout` base the above-floor deadline tops out near 16 s — the
/// "a block every ~16 s is fine" tolerance the directive sets for speculative work.
const ABOVE_FLOOR_DEADLINE_MIN_BYTES_PER_SEC: u64 = 256 * 1024;

/// Pure inputs for deciding whether a block request may consume budget.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionSnapshot {
    pub(super) download_floor: block::Height,
    pub(super) reorder_buffered_bytes: u64,
    pub(super) reorder_buffered_blocks: u64,
    pub(super) applying_buffered_bytes: u64,
    pub(super) applying_buffered_blocks: u64,
    pub(super) sequencer_input_queued_bytes: u64,
    pub(super) reserved_above_floor_bytes: u64,
    pub(super) reserved_above_floor_blocks: u64,
    pub(super) budget_available: u64,
}

/// Whether a request is rescuing the current floor or speculating above it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RequestPriority {
    Floor,
    AboveFloor,
}

/// Admission result for one candidate request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionDecision {
    pub(super) priority: RequestPriority,
    pub(super) max_request_bytes: u64,
}

/// Return the highest start height that can be rescued by a floor request.
pub(super) fn floor_rescue_high(download_floor: block::Height) -> block::Height {
    next_height(download_floor).unwrap_or(download_floor)
}

pub(super) fn request_priority(
    download_floor: block::Height,
    start_height: block::Height,
) -> RequestPriority {
    // The next height above the floor can still unblock the current floor.
    if start_height <= floor_rescue_high(download_floor) {
        RequestPriority::Floor
    } else {
        RequestPriority::AboveFloor
    }
}

/// The per-request network deadline (the one sanctioned timer), set by priority:
///
/// - **Floor**: a short fixed leash. On expiry the lowest missing height is rescued
///   to a faster carrier (returned to the queue + the peer retry-avoided), so the
///   contiguous floor never waits on a slow peer — and the peer is *not* disconnected.
/// - **Above-floor**: the base `request_timeout` plus the size-expected transfer time
///   (`estimated_bytes / BtlBw`), so a legitimately slow large-body fetch runs to
///   completion. These deadlines never gate the floor, so they can afford to be
///   patient; `btlbw_bytes_per_sec` is the peer's measured rate (`None` cold-start),
///   floored at [`ABOVE_FLOOR_DEADLINE_MIN_BYTES_PER_SEC`].
pub(super) fn request_deadline(
    priority: RequestPriority,
    queued_at: Instant,
    request_timeout: Duration,
    floor_rescue_timeout: Duration,
    estimated_bytes: u64,
    btlbw_bytes_per_sec: Option<u64>,
) -> Instant {
    match priority {
        RequestPriority::Floor => queued_at + floor_rescue_timeout,
        RequestPriority::AboveFloor => {
            let rate = btlbw_bytes_per_sec
                .unwrap_or(0)
                .max(ABOVE_FLOOR_DEADLINE_MIN_BYTES_PER_SEC);
            // One body per request, so `estimated_bytes / rate` is at most
            // `MAX_BLOCK_BYTES / rate` (~8 s): finite and non-negative.
            let transfer = Duration::from_secs_f64(estimated_bytes as f64 / rate as f64);
            queued_at + request_timeout + transfer
        }
    }
}

/// Returns the admission decision for a candidate block response starting at `start_height`.
///
/// Floor-rescue requests may use any available response budget up to `response_byte_cap`.
/// Speculative requests above the floor are admitted only while the configured reorder
/// lookahead byte and block limits still have capacity.
///
/// Returns `None` when no bytes can be admitted, or when an above-floor request would
/// exceed the lookahead limits.
pub(super) fn admission_decision(
    config: &ZakuraBlockSyncConfig,
    snapshot: AdmissionSnapshot,
    start_height: block::Height,
    response_byte_cap: u64,
) -> Option<AdmissionDecision> {
    let priority = request_priority(snapshot.download_floor, start_height);
    let max_request_bytes = match priority {
        // Floor requests can use any available budget up to the response byte cap.
        RequestPriority::Floor => snapshot.budget_available.min(response_byte_cap),
        // Above-floor requests are admitted only if the reorder lookahead limits have capacity
        // and the response byte cap is not exceeded.
        RequestPriority::AboveFloor => {
            let held_bytes = snapshot
                .reorder_buffered_bytes
                .saturating_add(snapshot.applying_buffered_bytes)
                .saturating_add(snapshot.sequencer_input_queued_bytes)
                .saturating_add(snapshot.reserved_above_floor_bytes);
            let held_blocks = snapshot
                .reorder_buffered_blocks
                .saturating_add(snapshot.applying_buffered_blocks)
                .saturating_add(snapshot.reserved_above_floor_blocks);
            if held_bytes >= config.effective_max_reorder_lookahead_bytes()
                || held_blocks >= u64::from(config.max_reorder_lookahead_blocks)
            {
                return None;
            }

            let remaining_lookahead_bytes = config
                .effective_max_reorder_lookahead_bytes()
                .saturating_sub(held_bytes);
            snapshot
                .budget_available
                .min(remaining_lookahead_bytes)
                .min(response_byte_cap)
        }
    };

    (max_request_bytes > 0).then_some(AdmissionDecision {
        priority,
        max_request_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(8);
    const RESCUE: Duration = Duration::from_secs(2);

    #[test]
    fn floor_request_uses_the_short_rescue_leash() {
        let now = Instant::now();
        let deadline = request_deadline(
            RequestPriority::Floor,
            now,
            TIMEOUT,
            RESCUE,
            2_000_000,
            None,
        );
        // The floor is rescued on the fixed leash regardless of size or measured rate.
        assert_eq!(deadline, now + RESCUE);
    }

    #[test]
    fn above_floor_deadline_grows_with_body_size() {
        let now = Instant::now();
        // No measured rate: the min-rate floor (256 KiB/s) sizes the transfer term, so a
        // 256 KiB body adds ~1 s and a 2 MiB body adds ~8 s on top of the base timeout.
        let small = request_deadline(
            RequestPriority::AboveFloor,
            now,
            TIMEOUT,
            RESCUE,
            256 * 1024,
            None,
        );
        let large = request_deadline(
            RequestPriority::AboveFloor,
            now,
            TIMEOUT,
            RESCUE,
            2 * 1024 * 1024,
            None,
        );
        assert_eq!(small, now + TIMEOUT + Duration::from_secs(1));
        assert_eq!(large, now + TIMEOUT + Duration::from_secs(8));
        assert!(large > small);
    }

    #[test]
    fn above_floor_deadline_shrinks_as_measured_rate_rises() {
        let now = Instant::now();
        // A fast peer transfers the body quickly, so its above-floor deadline collapses
        // toward the base timeout — the size term is negligible at high BtlBw.
        let fast = request_deadline(
            RequestPriority::AboveFloor,
            now,
            TIMEOUT,
            RESCUE,
            2 * 1024 * 1024,
            Some(64 * 1024 * 1024),
        );
        assert!(fast > now + TIMEOUT);
        assert!(fast < now + TIMEOUT + Duration::from_millis(100));
    }
}
