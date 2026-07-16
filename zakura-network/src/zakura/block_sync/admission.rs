use std::time::{Duration, Instant};

use zakura_chain::block;

use super::{
    config::{ZakuraBlockSyncConfig, MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES},
    state::next_height,
};

/// Delivery rate assumed when sizing an above-floor deadline for a peer whose
/// measured BtlBw is still near zero, so the patience window is bounded rather than
/// unbounded. A worst-case `MAX_BLOCK_BYTES` body at this rate transfers in ~8 s, so
/// with the `request_timeout` base the above-floor deadline tops out near 16 s — the
/// "a block every ~16 s is fine" tolerance the directive sets for speculative work.
const ABOVE_FLOOR_DEADLINE_MIN_BYTES_PER_SEC: u64 = 256 * 1024;
/// Delivery rate assumed for floor rescue before a peer has a fresh byte-rate
/// sample. This keeps the rescue leash short while allowing a full 2 MB body roughly
/// two seconds of transfer time.
const FLOOR_DEADLINE_MIN_BYTES_PER_SEC: u64 = 1024 * 1024;

/// Pure inputs for deciding whether a block request may consume budget.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionSnapshot {
    pub(super) download_floor: block::Height,
    /// The verified (commit) tip. Heights within one checkpoint range above it (the
    /// commit window) are always fundable (liveness), so a pinned checkpoint range can
    /// assemble and commit can drain the pipeline; everything else is memory-gated.
    pub(super) verified_block_tip: block::Height,
    /// Retained body memory plus outstanding above-window headroom reservations,
    /// atomically maintained across request and pipeline stages.
    pub(super) retained_memory_bytes: u64,
    pub(super) reorder_buffered_blocks: u64,
    pub(super) applying_buffered_blocks: u64,
    pub(super) reserved_above_floor_blocks: u64,
    pub(super) budget_available: u64,
}

/// Whether a request is rescuing the current floor or speculating above it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RequestPriority {
    Floor,
    AboveFloor,
}

/// Admission verdict for one candidate take: a grant carrying the full take
/// geometry and sizing, or a typed refusal the fill loop can attribute.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum AdmissionOutcome {
    Admit(AdmissionGrant),
    /// The start height is above the commit window and the resident look-ahead
    /// gate (byte budget or block cap) is full.
    LookaheadAtCap,
    /// The look-ahead gate has headroom but zero bytes are fundable right now
    /// (the in-flight request budget is spent). Never returned for floor-priority
    /// starts: their byte cap is floored at one so the floor block always
    /// reaches the bounded-overdraft reservation path.
    InflightBudgetEmpty,
}

/// Complete geometry and sizing for one contiguous take. Produced only by
/// [`admit`]; the fill loop feeds it verbatim to the work queue, so a take that
/// crosses the commit window unbounded by resident headroom cannot be
/// constructed.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionGrant {
    pub(super) priority: RequestPriority,
    /// Inclusive highest height the take may include. Exempt (in-window) grants
    /// are clamped to the commit window top, so no height above the window ever
    /// rides an exempt request; gated (above-window) grants pass the caller's
    /// servable ceiling through.
    pub(super) take_high: block::Height,
    /// Authoritative summed-estimate byte cap for the take and its reservation.
    /// Nothing downstream may substitute its own sizing.
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
/// - **Floor**: a short rescue leash plus the expected transfer time. On expiry the
///   lowest missing height is rescued to a faster carrier (returned to the queue + the
///   peer retry-avoided), so the contiguous floor never waits on a slow peer — and the
///   peer is *not* disconnected.
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
        RequestPriority::Floor => {
            let rate = btlbw_bytes_per_sec
                .unwrap_or(0)
                .max(FLOOR_DEADLINE_MIN_BYTES_PER_SEC);
            let transfer = Duration::from_secs_f64(estimated_bytes as f64 / rate as f64);
            queued_at + floor_rescue_timeout + transfer
        }
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

/// Heights within one worst-case checkpoint range above the verified tip bypass
/// look-ahead gates.
///
/// During checkpoint sync, the verified tip remains at the previous checkpoint
/// until the full range is submitted, so every block in that range must remain
/// fundable even when normal look-ahead limits are full.
///
/// This is a fixed consensus-derived bound, not `config.submitted_apply_limit()`,
/// because the configured submit window can be much larger and would weaken the
/// memory gate.
const COMMIT_WINDOW_EXEMPT_SPAN_BLOCKS: u32 = MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES as u32;

/// Hard block-count cap on speculative look-ahead bookkeeping.
///
/// Defense-in-depth on the map/bookkeeping size only; the resident-memory
/// budget is the primary bound on buffered bodies. This cap binds before the
/// byte gate only for tiny early-chain bodies whose per-entry bookkeeping
/// overhead the retained-body accounting does not model.
pub(super) const LOOKAHEAD_BLOCK_HARD_CAP: u64 = 262_144;

/// Highest height exempt from look-ahead backpressure: the top of the commit window
/// ([`COMMIT_WINDOW_EXEMPT_SPAN_BLOCKS`] above the verified tip). Anchored to the
/// verified tip — which advances only on commit — so the window cannot escalate with
/// the download floor.
fn commit_window_high(snapshot: &AdmissionSnapshot) -> block::Height {
    // Valid heights sit far below `u32::MAX`, so the saturation is unreachable; it
    // just keeps the arithmetic total.
    block::Height(
        snapshot
            .verified_block_tip
            .0
            .saturating_add(COMMIT_WINDOW_EXEMPT_SPAN_BLOCKS),
    )
}

pub(super) fn is_commit_window_exempt(snapshot: &AdmissionSnapshot, height: block::Height) -> bool {
    height <= commit_window_high(snapshot)
}

fn held_blocks(snapshot: &AdmissionSnapshot) -> u64 {
    snapshot
        .reorder_buffered_blocks
        .saturating_add(snapshot.applying_buffered_blocks)
        .saturating_add(snapshot.reserved_above_floor_blocks)
}

/// Whether the resident-memory look-ahead budget (or the block cap) is already full.
fn lookahead_over_budget(config: &ZakuraBlockSyncConfig, snapshot: &AdmissionSnapshot) -> bool {
    snapshot.retained_memory_bytes >= config.effective_max_reorder_lookahead_bytes()
        || held_blocks(snapshot) >= LOOKAHEAD_BLOCK_HARD_CAP
}

fn remaining_retained_memory_bytes(
    config: &ZakuraBlockSyncConfig,
    snapshot: &AdmissionSnapshot,
) -> u64 {
    config
        .effective_max_reorder_lookahead_bytes()
        .saturating_sub(snapshot.retained_memory_bytes)
}

/// Retention-only admission for a body that is already downloaded.
///
/// A received body consumes no request budget (its wire reservation is
/// released at receipt), so unlike [`admit`] this never consults
/// `budget_available`: only the commit-window exemption and the resident
/// look-ahead gate — the two rules that bound retention — apply.
pub(super) fn admit_received_body(
    config: &ZakuraBlockSyncConfig,
    snapshot: &AdmissionSnapshot,
    height: block::Height,
    retained_memory_bytes: u64,
) -> bool {
    if is_commit_window_exempt(snapshot, height) {
        return true;
    }
    !lookahead_over_budget(config, snapshot)
        && retained_memory_bytes <= remaining_retained_memory_bytes(config, snapshot)
}

/// Plans one contiguous take starting at `start_height`: the single authority for
/// the commit-window exemption, the resident-memory gate, and request sizing.
///
/// Heights in the commit window (up to `MAX_CHECKPOINT_HEIGHT_GAP + 1` blocks
/// above the *verified* tip) are always fundable, so the committer can advance.
/// This lets a pinned checkpoint range fully assemble even when the look-ahead
/// budget is full.
///
/// Exempt requests are capped at the top of the commit window, so one request
/// cannot include both exempt in-window blocks and gated above-window blocks.
/// Anything above the commit window must pass the normal look-ahead memory check.
/// That includes floor-priority requests if the floor has moved far ahead of the
/// verified tip.
///
/// Gating the floor lane (with only the commit window exempt) is what bounds the
/// applying queue: the download floor advances on every download, so a floor exemption
/// tied to it escalates unboundedly ahead of commit. Anchoring the exemption to the
/// verified tip caps the pipeline to the look-ahead budget plus the bounded
/// commit window and bodies already admitted by the in-flight request budget.
///
/// Floor-priority requests are never blocked just because the request budget is exactly
/// full. If the lowest missing block is needed to let commit move forward, it can still
/// be requested even when speculative work has spent the in-flight budget (the routine's
/// bounded floor overdraft funds it).
pub(super) fn admit(
    config: &ZakuraBlockSyncConfig,
    snapshot: AdmissionSnapshot,
    start_height: block::Height,
    servable_high: block::Height,
    response_byte_cap: u64,
) -> AdmissionOutcome {
    let priority = request_priority(snapshot.download_floor, start_height);
    let window_high = commit_window_high(&snapshot);

    let (take_high, max_request_bytes) = if start_height <= window_high {
        // Exempt: liveness sizing, take clamped at the window top so the resident
        // gate's coverage of above-window heights is total.
        (
            servable_high.min(window_high),
            snapshot.budget_available.min(response_byte_cap),
        )
    } else {
        if lookahead_over_budget(config, &snapshot) {
            return AdmissionOutcome::LookaheadAtCap;
        }
        (
            servable_high,
            snapshot
                .budget_available
                .min(response_byte_cap)
                .min(remaining_retained_memory_bytes(config, &snapshot)),
        )
    };

    let max_request_bytes = if priority == RequestPriority::Floor {
        max_request_bytes.max(1)
    } else {
        max_request_bytes
    };
    if max_request_bytes == 0 {
        return AdmissionOutcome::InflightBudgetEmpty;
    }
    AdmissionOutcome::Admit(AdmissionGrant {
        priority,
        take_high,
        max_request_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(8);
    const RESCUE: Duration = Duration::from_secs(2);

    #[test]
    fn floor_request_leash_is_size_aware() {
        let now = Instant::now();
        let deadline = request_deadline(
            RequestPriority::Floor,
            now,
            TIMEOUT,
            RESCUE,
            2_000_000,
            None,
        );
        assert_eq!(
            deadline,
            now + RESCUE + Duration::from_secs_f64(2_000_000_f64 / (1024_f64 * 1024_f64))
        );
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

    /// During checkpoint sync, `verified_tip`
    /// stays pinned to the previous checkpoint until the whole range (up to
    /// `MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES` blocks) is co-resident. The whole range
    /// is commit-window exempt, so it assembles regardless of the gated budget; the
    /// budget under a legal 1 GiB in-flight budget must also leave gated headroom just
    /// above the window.
    #[test]
    fn checkpoint_range_fits_under_one_gib_inflight_budget() {
        use super::super::config::{
            BS_CHECKPOINT_RANGE_BYTE_FLOOR, BS_PER_BLOCK_WORST_CASE_BYTES,
            MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES,
        };

        let config = ZakuraBlockSyncConfig {
            max_inflight_block_bytes: 1024 * 1024 * 1024,
            ..ZakuraBlockSyncConfig::default()
        };

        // One block short of a full co-resident range, with `verified_tip` pinned at 0.
        let range_blocks = u32::try_from(MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES)
            .expect("checkpoint range block count fits in u32");
        let snapshot = AdmissionSnapshot {
            download_floor: block::Height(range_blocks - 1),
            verified_block_tip: block::Height(0),
            retained_memory_bytes: BS_CHECKPOINT_RANGE_BYTE_FLOOR - BS_PER_BLOCK_WORST_CASE_BYTES,
            reorder_buffered_blocks: 0,
            applying_buffered_blocks: u64::from(range_blocks) - 1,
            reserved_above_floor_blocks: 0,
            budget_available: config.max_inflight_block_bytes,
        };
        // The range-completing block is inside the commit window, so it is exempt.
        assert!(
            matches!(
                admit(
                    &config,
                    snapshot,
                    block::Height(range_blocks),
                    block::Height(range_blocks),
                    u64::MAX
                ),
                AdmissionOutcome::Admit(_)
            ),
            "the final block of a checkpoint range must be admissible under a 1 GiB in-flight budget",
        );
        // The first height above the window is memory-gated but must still have headroom
        // under this budget: ~800 MB of serialized applying bytes (charged at wire size)
        // sit below the 1.5 GiB default resident budget. This keeps the assertion
        // non-vacuous now that the whole range is window-exempt.
        assert!(
            matches!(
                admit(
                    &config,
                    snapshot,
                    block::Height(range_blocks + 1),
                    block::Height(range_blocks + 1),
                    u64::MAX
                ),
                AdmissionOutcome::Admit(_)
            ),
            "the first gated height above the commit window must still be admissible",
        );
    }
}
