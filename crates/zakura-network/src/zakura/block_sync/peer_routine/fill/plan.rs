//! Pure decisions of one fill attempt: lane selection, retry filtering, and
//! funding. Nothing here acquires a resource, reads a clock, or takes a lock;
//! the routine passes in every observation.

use std::{ops::Range, time::Instant};

use zakura_chain::block;

use crate::zakura::block_sync::{
    admission::{
        admit, floor_rescue_high, request_priority, AdmissionOutcome, AdmissionSnapshot,
        RequestPriority,
    },
    ZakuraBlockSyncConfig,
};

use super::{FillLimits, FillStop};

#[cfg(test)]
mod tests;

/// One bounded take from the work queue.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct TakePlan {
    /// The take's inclusive height range.
    pub(super) low: block::Height,
    pub(super) high: block::Height,
    pub(super) max_count: usize,
    /// The summed-estimate byte cap. The take still admits its first item.
    pub(super) max_bytes: u64,
}

/// Plan the floor lane's take, which prioritizes the lowest missing block so
/// commit can keep moving, even if that means freeing look-ahead budget.
///
/// `floor_start` is the first pending height this peer can serve in the
/// floor-rescue window, or `None` when there is none or a preferred carrier
/// should serve it. `admit` is the single authority for the commit-window
/// exemption, the resident-memory gate, and take geometry and sizing. The plan
/// layers the per-peer congestion window's byte headroom on top, so a saturated
/// window cannot fund a large speculative tail. The floor bypass adds its bodies
/// of headroom. The one-byte minimum preserves the always-take-first-item
/// floor-progress guarantee even at zero headroom: that single body is the only
/// permitted overshoot, and the floor overdraft funds it.
///
/// The take starts at `servable_low`, not at `floor_start`. Another routine can
/// return a lower height before the take.
pub(super) fn plan_floor(
    config: &ZakuraBlockSyncConfig,
    limits: &FillLimits,
    snapshot: AdmissionSnapshot,
    floor_start: Option<block::Height>,
) -> Result<Option<TakePlan>, FillStop> {
    let Some(floor_start) = floor_start else {
        return Ok(None);
    };
    if limits.servable_low > floor_rescue_high(snapshot.download_floor) {
        return Ok(None);
    }
    match admit(
        config,
        snapshot,
        floor_start,
        limits.servable_high,
        limits.response_byte_cap,
    ) {
        AdmissionOutcome::Admit(grant) => Ok(Some(TakePlan {
            low: limits.servable_low,
            high: grant.take_high,
            max_count: limits.max_count,
            max_bytes: grant
                .max_request_bytes
                .min(limits.floor_cwnd_bytes.unwrap_or(u64::MAX))
                .max(1),
        })),
        AdmissionOutcome::LookaheadAtCap => Err(FillStop::LookaheadCap),
        // Unreachable for floor-priority starts (their cap is floored at one
        // byte); attribute honestly if it ever fires.
        AdmissionOutcome::InflightBudgetEmpty => Err(FillStop::InflightBudget),
    }
}

/// Plan the above-floor lane's take from the first pending height this peer
/// can serve.
///
/// A floor-priority start means the floor lane deferred it to a preferred
/// carrier or lost it to another routine. This lane leaves it to that peer and
/// reports no work. The take is bounded by the congestion window's byte
/// headroom without the floor bypass, so an above-floor request never overshoots
/// the byte window beyond the one always-taken item.
pub(super) fn plan_above_floor(
    config: &ZakuraBlockSyncConfig,
    limits: &FillLimits,
    snapshot: AdmissionSnapshot,
    start: Option<block::Height>,
) -> Result<TakePlan, FillStop> {
    let Some(start) = start else {
        return Err(FillStop::NoWork);
    };
    match admit(
        config,
        snapshot,
        start,
        limits.servable_high,
        limits.response_byte_cap,
    ) {
        AdmissionOutcome::Admit(grant) if grant.priority == RequestPriority::AboveFloor => {
            Ok(TakePlan {
                low: limits.servable_low,
                high: grant.take_high,
                max_count: limits.max_count,
                max_bytes: grant
                    .max_request_bytes
                    .min(limits.above_floor_cwnd_bytes.unwrap_or(u64::MAX)),
            })
        }
        AdmissionOutcome::Admit(_) => Err(FillStop::NoWork),
        AdmissionOutcome::LookaheadAtCap => Err(FillStop::LookaheadCap),
        AdmissionOutcome::InflightBudgetEmpty => Err(FillStop::InflightBudget),
    }
}

/// One taken item's retry facts, read when the filter visits it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RetryFact {
    /// No retry rule excludes the item.
    Allowed,
    /// An unended exchange for this height still blocks a new request.
    ProtocolBlocked,
    /// A timed avoidance excludes the item: this routine's own retry bias, the
    /// floor watchdog, or a body retry deferral.
    Avoided,
}

/// The retry filter's choice for one take.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RetryRun {
    /// The first allowed contiguous run, as indexes into the take.
    pub(super) keep: Option<Range<usize>>,
    /// Whether a timed avoidance excluded a visited item.
    pub(super) timed_rejection: bool,
}

/// Keep the first contiguous run of `items` that no retry rule excludes.
///
/// `fact` reads one item's facts from their owners. The filter visits items in
/// order and stops after the first allowed run ends, so it reads each visited
/// item once and never reads a later item. Facts can change between reads.
pub(super) fn select_retry_run<T>(items: &[T], mut fact: impl FnMut(&T) -> RetryFact) -> RetryRun {
    let mut timed_rejection = false;
    let keep = first_allowed_run(items, |item| match fact(item) {
        RetryFact::Allowed => true,
        RetryFact::ProtocolBlocked => false,
        RetryFact::Avoided => {
            timed_rejection = true;
            false
        }
    });
    RetryRun {
        keep,
        timed_rejection,
    }
}

/// Return the first contiguous run that the predicate accepts.
///
/// The function evaluates each visited item once. This property matters when
/// the predicate reads shared retry state that the reactor or sequencer can update.
fn first_allowed_run<T>(
    items: &[T],
    mut is_allowed: impl FnMut(&T) -> bool,
) -> Option<Range<usize>> {
    let mut start = None;

    for (index, item) in items.iter().enumerate() {
        if is_allowed(item) {
            start.get_or_insert(index);
        } else if let Some(start) = start {
            return Some(start..index);
        }
    }

    start.map(|start| start..items.len())
}

/// Wake deadlines that bound how long excluded work stays excluded.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct RetryDeadlines {
    /// The earliest expiry of this routine's own retry bias.
    pub(super) local: Option<Instant>,
    /// The earliest expiry of this peer's floor-watchdog exclusions.
    pub(super) floor_watchdog: Option<Instant>,
    /// The earliest expiry of this peer's body retry deferrals.
    pub(super) body_retry: Option<Instant>,
    /// The requester's retirement deadline for abandoned exchanges.
    pub(super) retirement: Option<Instant>,
}

/// When the routine retries after the filter excluded work.
///
/// Live ranges wake through responses or their request/retirement deadlines.
/// The routine retries immediately only when a timed rejection disappeared
/// after filtering.
pub(super) fn retry_wake_deadline(
    now: Instant,
    timed_rejection: bool,
    deadlines: RetryDeadlines,
) -> Option<Instant> {
    let timed = [
        deadlines.local,
        deadlines.floor_watchdog,
        deadlines.body_retry,
    ]
    .into_iter()
    .flatten()
    .min()
    .or_else(|| timed_rejection.then_some(now));
    [timed, deadlines.retirement].into_iter().flatten().min()
}

/// How one attempt funds its retained run.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct Funding {
    /// The retained run's priority, classified from its actual first height.
    pub(super) priority: RequestPriority,
    /// The summed size estimates of the retained run (not worst case), so the
    /// budget admits far more typically-small bodies.
    pub(super) reserved_bytes: u64,
}

impl Funding {
    /// Whether a refused ordinary reservation may overdraft the byte budget.
    ///
    /// Only floor work may. The work queue owns each height once, so there can
    /// only be one floor-priority overdraft globally. Its charge is released by
    /// the normal reservation paths: receipt, timeout, watchdog, or reset.
    pub(super) fn allows_overdraft(self) -> bool {
        self.priority == RequestPriority::Floor
    }
}

/// Classify funding from the run that the attempt actually retained.
///
/// Another routine can take the probed floor height between the probe and the
/// take, and the retry filter can drop it after the take. So the retained run
/// can begin above the floor-rescue window even when the floor lane planned
/// the take. Its first height, never the lane or the probe, decides whether it
/// may overdraft and whether it gets the short floor-rescue deadline.
pub(super) fn funding_for(
    download_floor: block::Height,
    first_height: block::Height,
    estimated_bytes: impl IntoIterator<Item = u64>,
) -> Funding {
    Funding {
        priority: request_priority(download_floor, first_height),
        reserved_bytes: estimated_bytes
            .into_iter()
            .fold(0u64, |acc, bytes| acc.saturating_add(bytes)),
    }
}
