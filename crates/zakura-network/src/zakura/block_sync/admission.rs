use std::{sync::Arc, time::Duration};

use zakura_chain::{block, parameters::checkpoint::list::CheckpointList};
use zakura_node_services::block_memory::Admission;
use zakura_rt::time::Instant;

use super::{
    config::{BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES, MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES},
    state::next_height,
};
use crate::zakura::memory::draft::ADMISSION_CHARGE_PER_MAX_BLOCK;

/// Minimum rate used for deadline estimates. One maximum-size body adds about
/// eight seconds; earlier unreceived bodies add their bounded transfer estimates.
/// A session with no accepted progress still reaches its separate liveness limit.
pub(super) const DEADLINE_MIN_BYTES_PER_SEC: u64 = 256 * 1024;

/// Time to move `bytes` at [`DEADLINE_MIN_BYTES_PER_SEC`], the slowest rate a
/// block-sync deadline allows. A 2 MB body adds about eight seconds.
pub(super) fn min_rate_transfer_time(bytes: u64) -> Duration {
    Duration::from_millis(bytes.saturating_mul(1000) / DEADLINE_MIN_BYTES_PER_SEC)
}

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
/// (`BufferedBlockBody::retain_for_backlog`), so they are charged at ×1.
///
// The factor is a calibration from the measured ~3.3–4x wire→resident ratio of real
// blocks before legacy bundles were boxed. It is an average, not a bound: small
// transparent transactions can still exceed it. Admission charges the measured
// decoded heap whenever it exceeds this estimate.
pub const DESERIALIZED_MEM_FACTOR: u64 = 4;

/// Pure inputs for deciding whether a block request may consume budget.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct AdmissionSnapshot {
    pub(super) download_floor: block::Height,
    /// The top of the commit window ([`CommitWindow::top`] at the verified
    /// tip). Heights up to it lease from `Consumer::Consensus`, so a checkpoint
    /// range can assemble and commit can drain the pipeline; everything else is
    /// memory-gated.
    pub(super) commit_window_top: block::Height,
    pub(super) reorder_buffered_bytes: u64,
    pub(super) reorder_buffered_blocks: u64,
    pub(super) applying_buffered_bytes: u64,
    pub(super) applying_buffered_blocks: u64,
    pub(super) sequencer_input_queued_bytes: u64,
    /// Wire bytes of decoded submissions the driver can still retain, including
    /// bodies detached from `applying` while their completion is pending.
    pub(super) in_flight_submission_bytes: u64,
    /// Measured heap of every decoded body: the sequencer input, the reorder
    /// buffer, and `applying` including detached submissions.
    pub(super) decoded_resident_bytes: u64,
    pub(super) reserved_above_floor_bytes: u64,
    pub(super) reserved_above_floor_blocks: u64,
    /// Bytes `Consumer::BlockSync` can lease now, for above-window requests.
    pub(super) budget_available: u64,
    /// Bytes `Consumer::Consensus` can lease now, for commit-window requests.
    pub(super) consensus_available: u64,
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
    /// The start height is above the commit window and the look-ahead block
    /// cap is full.
    LookaheadAtCap,
    /// The consumer the take leases from has no bytes to lease now. Never
    /// returned for floor-priority starts: their byte cap is floored at one so
    /// the floor block always reaches the pool and waits there.
    InflightBudgetEmpty,
}

/// Complete geometry and sizing for one contiguous take. Produced only by
/// [`admit`]; the fill loop feeds it verbatim to the work queue, so a take that
/// crosses the commit window cannot be constructed.
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

/// Heights above the last checkpoint that the commit window covers: as many
/// maximal blocks as [`BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES`] holds at
/// admission's per-block charge, `536,870,912 / 10,000,000` = 53.
///
/// Above the last checkpoint each block commits on its own, so the window
/// only needs room to keep the verifier busy. The `Consensus` reserve is at
/// least the decoded submission bound, so it funds every height here at once.
pub(crate) const SEMANTIC_COMMIT_WINDOW_BLOCKS: u32 =
    // The quotient is 53, which fits `u32`.
    (BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES / ADMISSION_CHARGE_PER_MAX_BLOCK) as u32;

/// The commit window's span without a checkpoint list: the widest checkpoint
/// range any list can hold, `MAX_CHECKPOINT_HEIGHT_GAP + 1` heights.
const UNLISTED_COMMIT_WINDOW_BLOCKS: u32 =
    // `MAX_CHECKPOINT_HEIGHT_GAP + 1` is 401, which fits `u32`.
    MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES as u32;

/// The heights above the verified tip that block sync must fund for commits
/// to move: the commit window.
///
/// Below the last checkpoint, the checkpoint verifier commits a range only
/// once every block up to the next checkpoint is submitted, so the window
/// ends at the next checkpoint above the verified tip. That also holds after a
/// restart in the middle of a range. Above the last checkpoint, blocks verify
/// one at a time, and the window covers [`SEMANTIC_COMMIT_WINDOW_BLOCKS`].
///
/// Commit-window requests lease from `Consumer::Consensus`, and commit-window
/// bodies always decode and stay retained. Everything above the window is
/// look-ahead that leases from `Consumer::BlockSync`.
///
/// The default, for a startup without a checkpoint list, covers the widest
/// range a list can hold above every tip.
#[derive(Clone, Default)]
pub struct CommitWindow {
    checkpoints: Option<Arc<CheckpointList>>,
}

impl CommitWindow {
    /// The window of a node that verifies with `checkpoints`.
    pub fn new(checkpoints: Arc<CheckpointList>) -> Self {
        Self {
            checkpoints: Some(checkpoints),
        }
    }

    /// The highest height in the window above `verified_tip`.
    pub fn top(&self, verified_tip: block::Height) -> block::Height {
        let span = match &self.checkpoints {
            Some(checkpoints) => match checkpoints.checkpoint_after(verified_tip) {
                Some((checkpoint, _)) => return checkpoint,
                None => SEMANTIC_COMMIT_WINDOW_BLOCKS,
            },
            None => UNLISTED_COMMIT_WINDOW_BLOCKS,
        };
        // Valid heights sit far below `u32::MAX`, so the saturation is
        // unreachable; it just keeps the arithmetic total.
        block::Height(verified_tip.0.saturating_add(span))
    }
}

impl std::fmt::Debug for CommitWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The Mainnet list has about 15,000 entries, so print only its end.
        f.debug_struct("CommitWindow")
            .field(
                "last_checkpoint",
                &self.checkpoints.as_ref().map(|list| list.max_height()),
            )
            .finish()
    }
}

/// Hard block-count cap on speculative look-ahead bookkeeping.
///
/// Defense-in-depth on the map/bookkeeping size only; the resident-memory
/// budget is the primary bound on buffered bodies. This cap binds before the
/// byte gate only when the average retained body is smaller than
/// `effective_budget / (DESERIALIZED_MEM_FACTOR × 262_144)` wire bytes
/// (~6.1 KB at the default budget), i.e. for tiny early-chain bodies whose
/// per-entry bookkeeping overhead the flat resident factor does not model.
pub(super) const LOOKAHEAD_BLOCK_HARD_CAP: u64 = 262_144;

/// Retained-decode admission for a raw body entering the submission window.
///
/// Commit-window heights always decode, because a checkpoint range resolves only
/// after every block in it is submitted. Refusing one could wedge the verified tip.
/// Every refused height therefore enters the window as the tip advances.
/// `window_top` is [`CommitWindow::top`] at the current verified tip.
pub(super) fn submission_decode_admission(
    window_top: block::Height,
    height: block::Height,
) -> Admission {
    if height <= window_top {
        Admission::PipelineExempt
    } else {
        Admission::WithinLimit
    }
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

fn held_blocks(snapshot: &AdmissionSnapshot) -> u64 {
    snapshot
        .reorder_buffered_blocks
        .saturating_add(snapshot.applying_buffered_blocks)
        .saturating_add(snapshot.reserved_above_floor_blocks)
}

/// Final backlog cap checked by the single sequencer owner. Each retained
/// body carries its pool lease, so only the block count needs a cap here. The
/// commit window, which ends at `window_top`, stays retainable even when
/// look-ahead is full.
pub(super) fn retain_received_body(
    window_top: block::Height,
    retained_blocks: u64,
    height: block::Height,
) -> bool {
    height <= window_top || retained_blocks < LOOKAHEAD_BLOCK_HARD_CAP
}

/// Plans one contiguous take starting at `start_height`: the single authority for
/// the commit-window exemption, the look-ahead block cap, and request sizing.
///
/// Heights in the commit window (up to `snapshot.commit_window_top`, see
/// [`CommitWindow`]) lease from the reserved `Consumer::Consensus`, so
/// bulk look-ahead never shrinks or blocks them (`02 §4`, decision-6). Exempt
/// requests are capped at the top of the commit window, so one request cannot
/// include both exempt in-window blocks and gated above-window blocks.
///
/// Anything above the commit window leases from `Consumer::BlockSync`, whose
/// benefit cap bounds look-ahead bytes. [`LOOKAHEAD_BLOCK_HARD_CAP`] still
/// bounds the retained block count. That includes floor-priority requests if
/// the floor has moved far ahead of the verified tip.
///
/// A floor-priority take keeps at least one block, so the lowest missing block
/// reaches the pool, which makes it wait for capacity.
pub(super) fn admit(
    snapshot: AdmissionSnapshot,
    start_height: block::Height,
    servable_high: block::Height,
    response_byte_cap: u64,
) -> AdmissionOutcome {
    let priority = request_priority(snapshot.download_floor, start_height);
    let window_high = snapshot.commit_window_top;

    let (take_high, max_request_bytes) = if start_height <= window_high {
        (
            servable_high.min(window_high),
            snapshot.consensus_available.min(response_byte_cap),
        )
    } else {
        if held_blocks(&snapshot) >= LOOKAHEAD_BLOCK_HARD_CAP {
            return AdmissionOutcome::LookaheadAtCap;
        }
        (
            servable_high,
            snapshot.budget_available.min(response_byte_cap),
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
    use super::super::config::ZakuraBlockSyncConfig;
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

    /// Bulk look-ahead never shrinks a commit-window request (decision-6,
    /// mem-5): the exempt lane sizes by `Consumer::Consensus`, so a full
    /// `Consumer::BlockSync` stops only above-window takes.
    #[test]
    fn commit_window_requests_size_by_consensus_when_bulk_is_full() {
        let response_cap = 32 * 1024 * 1024;
        let snapshot = AdmissionSnapshot {
            download_floor: block::Height(10),
            commit_window_top: CommitWindow::default().top(block::Height(0)),
            reorder_buffered_bytes: 0,
            reorder_buffered_blocks: 0,
            applying_buffered_bytes: 0,
            applying_buffered_blocks: 0,
            sequencer_input_queued_bytes: 0,
            in_flight_submission_bytes: 0,
            decoded_resident_bytes: 0,
            reserved_above_floor_bytes: 0,
            reserved_above_floor_blocks: 0,
            budget_available: 0,
            consensus_available: 3 * response_cap,
        };
        let top = snapshot.commit_window_top;
        let AdmissionOutcome::Admit(grant) = admit(snapshot, block::Height(50), top, response_cap)
        else {
            panic!("a commit-window take must be admitted while bulk is full");
        };
        assert_eq!(grant.max_request_bytes, response_cap);
        assert_eq!(grant.take_high, top);
        let above = next_height(top).expect("the window top has a successor");
        assert_eq!(
            admit(snapshot, above, above, response_cap),
            AdmissionOutcome::InflightBudgetEmpty
        );
    }

    #[test]
    fn every_commit_window_height_can_decode_past_the_pipeline_bound() {
        let tip = block::Height(1_000);
        let top = CommitWindow::default().top(tip);
        for height in [block::Height(tip.0 + 1), top] {
            assert_eq!(
                submission_decode_admission(top, height),
                Admission::PipelineExempt
            );
        }
        assert_eq!(
            submission_decode_admission(top, block::Height(top.0 + 1)),
            Admission::WithinLimit
        );
    }

    /// Below the last checkpoint the window ends at the next checkpoint, even
    /// from a tip in the middle of a range. At and above the last checkpoint
    /// it covers the semantic span.
    #[test]
    fn the_commit_window_ends_at_the_next_checkpoint() {
        let hash = |byte| block::Hash([byte; 32]);
        let list = CheckpointList::from_list([
            (block::Height(0), hash(1)),
            (block::Height(400), hash(2)),
            (block::Height(650), hash(3)),
        ])
        .expect("the test list is valid");
        let window = CommitWindow::new(Arc::new(list));
        for (tip, top) in [(0, 400), (150, 400), (399, 400), (400, 650), (649, 650)] {
            assert_eq!(window.top(block::Height(tip)), block::Height(top), "{tip}");
        }
        assert_eq!(SEMANTIC_COMMIT_WINDOW_BLOCKS, 53);
        for tip in [650, 10_000] {
            assert_eq!(
                window.top(block::Height(tip)),
                block::Height(tip + SEMANTIC_COMMIT_WINDOW_BLOCKS)
            );
        }
        assert_eq!(
            CommitWindow::default().top(block::Height(150)),
            block::Height(150 + 401)
        );
    }

    /// The bound has no tunable of its own, and every request gate fits under it.
    #[test]
    fn decoded_pipeline_bound_derives_from_the_reactor_budgets() {
        use super::super::config::{
            BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES, DEFAULT_BS_MAX_RESPONSE_BYTES,
        };

        let mut config = ZakuraBlockSyncConfig::default();
        let bound = config.decoded_pipeline_byte_bound();
        assert_eq!(
            bound,
            config.effective_max_reorder_lookahead_bytes()
                + (MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES as u64 * block::MAX_BLOCK_BYTES
                    + u64::from(DEFAULT_BS_MAX_RESPONSE_BYTES))
                    * DESERIALIZED_MEM_FACTOR
        );
        assert!(bound > BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES);

        config.max_reorder_lookahead_bytes += 1;
        assert_eq!(config.decoded_pipeline_byte_bound(), bound + 1);
        config.max_response_bytes += 1;
        assert_eq!(
            config.decoded_pipeline_byte_bound(),
            bound + 1 + DESERIALIZED_MEM_FACTOR
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
