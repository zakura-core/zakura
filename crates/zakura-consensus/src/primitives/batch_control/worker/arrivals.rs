//! How often new work reaches a batch worker, and so how long a partial batch should wait.
//!
//! Waiting only saves work if another caller's items join the batch. When callers are seconds
//! apart, a partial batch that waits for its latency bound gains nothing, so it flushes once
//! its caller's items stop arriving. When callers arrive faster than the latency bound, the
//! batch waits for them, and flushes early only when they stall.

use std::time::Duration;

use tokio::time::Instant;

#[cfg(test)]
mod tests;

/// The newest gap's share of the mean gap between bursts, as `1 / GAP_SMOOTHING`.
const GAP_SMOOTHING: u32 = 4;

/// How many mean burst gaps without an item mean the bursts have stalled.
const STALL_GAPS: u32 = 4;

/// The longest gap between bursts that counts towards the mean, in latency bounds.
///
/// Every longer gap means "sparse" equally, so the cap lets the mean recover within a few
/// bursts once traffic picks up.
const MAX_GAP_LATENCIES: u32 = 8;

/// Tracks bursts of items: runs whose items arrive at most `burst_gap` apart.
///
/// One caller's request, such as one transaction's proofs and signatures, arrives as a burst.
#[derive(Debug)]
pub(super) struct Arrivals {
    /// The longest gap between two items of one burst, and the shortest wait for a batch.
    burst_gap: Duration,

    /// The longest gap between bursts that counts towards `mean_burst_gap`.
    max_gap: Duration,

    /// When the most recent item arrived.
    last_item: Option<Instant>,

    /// When the most recent burst started.
    last_burst: Option<Instant>,

    /// The moving mean of the gaps between burst starts, once two bursts have arrived.
    mean_burst_gap: Option<Duration>,
}

impl Arrivals {
    /// Creates a tracker for a worker whose batches wait at most `max_latency`, and whose
    /// callers queue all their items for a batch within `burst_gap`.
    pub(super) fn new(burst_gap: Duration, max_latency: Duration) -> Self {
        Self {
            burst_gap,
            max_gap: max_latency.saturating_mul(MAX_GAP_LATENCIES),
            last_item: None,
            last_burst: None,
            mean_burst_gap: None,
        }
    }

    /// Records an item arriving at `now`.
    pub(super) fn record(&mut self, now: Instant) {
        let starts_burst = self
            .last_item
            .is_none_or(|last| now.saturating_duration_since(last) > self.burst_gap);
        self.last_item = Some(now);

        if !starts_burst {
            return;
        }

        if let Some(last_burst) = self.last_burst.replace(now) {
            let gap = now.saturating_duration_since(last_burst).min(self.max_gap);
            self.mean_burst_gap = Some(match self.mean_burst_gap {
                None => gap,
                Some(mean) => {
                    mean.saturating_mul(GAP_SMOOTHING - 1).saturating_add(gap) / GAP_SMOOTHING
                }
            });
        }
    }

    /// Returns how long a partial batch should wait for its next item, when `remaining` is
    /// left before its latency bound.
    ///
    /// If fewer than half a burst is expected within `remaining`, waiting would not grow the
    /// batch, so it waits only `burst_gap` for the rest of the current burst. Otherwise it
    /// waits until the bursts stall.
    pub(super) fn idle_bound(&self, remaining: Duration) -> Duration {
        match self.mean_burst_gap {
            Some(mean) if remaining.saturating_mul(2) >= mean => {
                mean.saturating_mul(STALL_GAPS).max(self.burst_gap)
            }
            _ => self.burst_gap,
        }
    }
}
