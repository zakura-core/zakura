use std::time::{Duration, Instant};

use zakura_chain::block;

use super::{
    config::{ZakuraBlockSyncConfig, MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES},
    state::next_height,
};

/// Minimum rate used for deadline estimates. One maximum-size body adds about
/// eight seconds; earlier unreceived bodies add their bounded transfer estimates.
/// A session with no accepted progress still reaches its separate liveness limit.
const DEADLINE_MIN_BYTES_PER_SEC: u64 = 256 * 1024;

/// Estimated resident-memory multiple of a *decoded* block body's serialized size.
///
/// Decoded bodies (`Arc<Block>`, `sequencer::ApplyingBlock`) have an in-memory footprint
/// several times their wire/serialized size. The look-ahead budget must bound that *resident*
/// cost, not the wire bytes, or a small-block backlog blows past the intended memory ceiling.
///
/// Applied only to the pools that actually hold decoded blocks: the sequencer
/// input channel (bodies decoded by the peer routine, bounded by the channel
/// depth) and the submitted decode window (bodies decoded at `prepare_submit`,
/// bounded by `submitted_apply_limit`). The reorder backlog and the applying
/// backlog beyond the submission window retain only serialized wire bytes
/// (`BufferedBlockBody::retain_for_backlog`), so they are charged at ×1; see
/// [`estimated_resident_pipeline_bytes`].
///
// The factor is a deliberately conservative calibration from the measured ~3.3–4x
// wire→resident ratio; it is an approximation, not a true per-block size.
pub const DESERIALIZED_MEM_FACTOR: u64 = 4;

/// Pure inputs for deciding whether a block request may consume budget.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionSnapshot {
    pub(super) download_floor: block::Height,
    /// The verified (commit) tip. Heights within one checkpoint range above it (the
    /// commit window) are always fundable (liveness), so a pinned checkpoint range can
    /// assemble and commit can drain the pipeline; everything else is memory-gated.
    pub(super) verified_block_tip: block::Height,
    pub(super) reorder_buffered_bytes: u64,
    pub(super) reorder_buffered_blocks: u64,
    pub(super) applying_buffered_bytes: u64,
    pub(super) applying_buffered_blocks: u64,
    pub(super) sequencer_input_queued_bytes: u64,
    /// Wire bytes of decoded submissions the driver can still retain, including
    /// bodies detached from `applying` while their completion is pending.
    pub(super) in_flight_submission_bytes: u64,
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

/// Admission verdict for one candidate take: a grant carrying the full take
/// geometry and sizing, or a typed refusal the fill loop can attribute.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum AdmissionOutcome {
    Admit(AdmissionGrant),
    /// The start height is above the commit window and the resident look-ahead
    /// gate (byte budget or block cap) is full, or the remaining wire headroom
    /// rounds to zero.
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

/// A bounded request deadline including estimated transfer time.
///
/// A floor request uses the short rescue deadline only with a fresh delivery-rate
/// measurement. Without one, it gets the normal deadline: prematurely expiring a
/// cold peer's only probe prevents its first body from establishing progress.
/// Block-count measurements qualify for rescue but use the byte-rate fallback
/// when estimating transfer time.
/// Above-floor requests always use the normal deadline. Transfer bytes include
/// earlier unreceived responses on the ordered data stream. Expiry returns work
/// for retry; the separate block-progress deadline still bounds a silent session.
pub(super) fn request_deadline(
    priority: RequestPriority,
    queued_at: Instant,
    request_timeout: Duration,
    floor_rescue_timeout: Duration,
    expected_transfer_bytes: u64,
    btlbw_bytes_per_sec: Option<u64>,
    has_delivery_measurement: bool,
) -> Instant {
    let base = if priority == RequestPriority::Floor && has_delivery_measurement {
        floor_rescue_timeout
    } else {
        request_timeout
    };
    let rate = btlbw_bytes_per_sec
        .unwrap_or(0)
        .max(DEADLINE_MIN_BYTES_PER_SEC);
    // Peer and node admission bound these values well within f64's exact integer range.
    let transfer = Duration::from_secs_f64(expected_transfer_bytes as f64 / rate as f64);
    queued_at + base + transfer
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
/// byte gate only when the average retained body is smaller than
/// `effective_budget / (DESERIALIZED_MEM_FACTOR × 262_144)` wire bytes
/// (~6.1 KB at the default budget), i.e. for tiny early-chain bodies whose
/// per-entry bookkeeping overhead the flat resident factor does not model.
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

/// Wire bytes of block bodies retained by the pipeline: the single formula
/// behind the `retained_pipeline_wire_bytes` trace field and the resident
/// estimate, so every emitter and gate agrees on what "retained" means.
#[derive(Copy, Clone, Debug)]
pub(super) struct RetainedPipelineBytes {
    pub(super) reorder_buffered_bytes: u64,
    pub(super) applying_buffered_bytes: u64,
    pub(super) sequencer_input_queued_bytes: u64,
}

impl RetainedPipelineBytes {
    /// Total wire bytes of retained bodies (the `retained_pipeline_wire_bytes`
    /// trace field).
    pub(super) fn wire_bytes(self) -> u64 {
        self.reorder_buffered_bytes
            .saturating_add(self.applying_buffered_bytes)
            .saturating_add(self.sequencer_input_queued_bytes)
    }
}

impl AdmissionSnapshot {
    fn retained(&self) -> RetainedPipelineBytes {
        RetainedPipelineBytes {
            reorder_buffered_bytes: self.reorder_buffered_bytes,
            applying_buffered_bytes: self.applying_buffered_bytes,
            sequencer_input_queued_bytes: self.sequencer_input_queued_bytes,
        }
    }
}

/// Estimated resident memory of block bodies retained by, or already committed
/// to enter, the pipeline.
///
/// Serialized pools (reorder backlog, applying backlog, and outstanding
/// reservations) cost their wire bytes while retained. Decoded pools add the
/// decoded multiple: the sequencer input channel (bodies arrive decoded from
/// the peer routine, bounded by the channel depth) and in-flight submissions
/// (decoded at `prepare_submit`, charged through their exact completion even
/// after detachment, and bounded by `submitted_apply_limit`). A detached
/// submission no longer contributes applying wire bytes, but its decoded
/// charge remains. Both decoded pools are structurally bounded, so
/// the deep backlog — the pool that actually scales with look-ahead depth — is
/// charged at its true serialized cost instead of a flat decoded multiple.
fn estimated_resident_pipeline_bytes(snapshot: &AdmissionSnapshot) -> u64 {
    let serialized = snapshot
        .retained()
        .wire_bytes()
        .saturating_add(snapshot.reserved_above_floor_bytes);
    // Decoded copies exist alongside their retained raw payloads, so the extra
    // decoded cost is the full decoded multiple on top of the ×1 wire charge.
    let decoded = snapshot
        .sequencer_input_queued_bytes
        .saturating_add(snapshot.in_flight_submission_bytes)
        .saturating_mul(DESERIALIZED_MEM_FACTOR);
    serialized.saturating_add(decoded)
}

fn held_blocks(snapshot: &AdmissionSnapshot) -> u64 {
    snapshot
        .reorder_buffered_blocks
        .saturating_add(snapshot.applying_buffered_blocks)
        .saturating_add(snapshot.reserved_above_floor_blocks)
}

/// Whether the resident-memory look-ahead budget (or the block cap) is already full.
fn lookahead_over_budget(config: &ZakuraBlockSyncConfig, snapshot: &AdmissionSnapshot) -> bool {
    estimated_resident_pipeline_bytes(snapshot) >= config.effective_max_reorder_lookahead_bytes()
        || held_blocks(snapshot) >= LOOKAHEAD_BLOCK_HARD_CAP
}

/// Remaining resident look-ahead headroom, expressed in wire bytes.
///
/// Admitted bodies are retained serialized until they reach the bounded decode
/// window, so each byte of headroom funds one wire byte. The transient decoded
/// sequencer-input copy is bounded by the input channel and charged by the live
/// snapshot once queued.
fn remaining_lookahead_wire_bytes(
    config: &ZakuraBlockSyncConfig,
    snapshot: &AdmissionSnapshot,
) -> u64 {
    config
        .effective_max_reorder_lookahead_bytes()
        .saturating_sub(estimated_resident_pipeline_bytes(snapshot))
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
    serialized_bytes: u64,
) -> bool {
    if height <= commit_window_high(snapshot) {
        return true;
    }
    !lookahead_over_budget(config, snapshot)
        && serialized_bytes <= remaining_lookahead_wire_bytes(config, snapshot)
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
/// verified tip caps the pipeline to the look-ahead budget plus one worst-case window
/// (`COMMIT_WINDOW_EXEMPT_SPAN_BLOCKS × MAX_BLOCK_BYTES × DESERIALIZED_MEM_FACTOR`
/// ≈ 3.2 GB; a single in-window response can also exceed the byte gate by up to the
/// response cap × the factor) regardless of how far headers/downloads run ahead.
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
        let remaining_wire_bytes = remaining_lookahead_wire_bytes(config, &snapshot);
        if remaining_wire_bytes == 0 {
            return AdmissionOutcome::LookaheadAtCap;
        }
        (
            servable_high,
            snapshot
                .budget_available
                .min(remaining_wire_bytes)
                .min(response_byte_cap),
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
            Some(1024 * 1024),
            true,
        );
        assert_eq!(
            deadline,
            now + RESCUE + Duration::from_secs_f64(2_000_000_f64 / (1024_f64 * 1024_f64))
        );
    }

    #[test]
    fn unmeasured_floor_probe_gets_the_normal_bounded_deadline() {
        let now = Instant::now();
        for size in [1, 2_000_000] {
            let deadline = request_deadline(
                RequestPriority::Floor,
                now,
                TIMEOUT,
                RESCUE,
                size,
                None,
                false,
            );
            assert_eq!(
                deadline,
                request_deadline(
                    RequestPriority::AboveFloor,
                    now,
                    TIMEOUT,
                    RESCUE,
                    size,
                    None,
                    false,
                )
            );
            assert!(deadline > now + TIMEOUT);
            assert!(deadline < now + Duration::from_secs(16));
        }
    }

    #[test]
    fn floor_rescue_allows_the_measured_large_body_transfer_time() {
        let now = Instant::now();
        let deadline = request_deadline(
            RequestPriority::Floor,
            now,
            TIMEOUT,
            RESCUE,
            2 * 1024 * 1024,
            Some(512 * 1024),
            true,
        );
        assert_eq!(deadline, now + RESCUE + Duration::from_secs(4));
        assert!(deadline < now + TIMEOUT);
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
            false,
        );
        let large = request_deadline(
            RequestPriority::AboveFloor,
            now,
            TIMEOUT,
            RESCUE,
            2 * 1024 * 1024,
            None,
            false,
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
            true,
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
            reorder_buffered_bytes: 0,
            reorder_buffered_blocks: 0,
            applying_buffered_bytes: BS_CHECKPOINT_RANGE_BYTE_FLOOR - BS_PER_BLOCK_WORST_CASE_BYTES,
            applying_buffered_blocks: u64::from(range_blocks) - 1,
            sequencer_input_queued_bytes: 0,
            in_flight_submission_bytes: 0,
            reserved_above_floor_bytes: 0,
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

    /// A sub-range configured budget is clamped up so checkpoint sync cannot wedge.
    #[test]
    fn clamp_reorder_lookahead_floors_sub_range_configs() {
        use super::super::config::BS_CHECKPOINT_RANGE_BYTE_FLOOR;
        let mut config = ZakuraBlockSyncConfig {
            max_reorder_lookahead_bytes: 1024 * 1024, // 1 MiB, far below one range of wire bytes
            ..ZakuraBlockSyncConfig::default()
        };
        config.clamp_reorder_lookahead_to_floor();
        assert!(config.max_reorder_lookahead_bytes >= BS_CHECKPOINT_RANGE_BYTE_FLOOR);
    }
}
