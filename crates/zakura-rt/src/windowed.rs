//! Time-windowed min and max filters over the runtime clock.
//!
//! Block sync's BBR-lite estimators and the QUIC path-rate probe share this
//! filter, so both crates keep one encoding of a windowed extremum.

use std::{collections::VecDeque, time::Duration};

use crate::time::Instant;

/// A time-windowed set of `f64` samples supporting `min` and `max` filters.
/// Samples older than `horizon` are pruned on insert **and re-filtered at read
/// time against the caller's `now`**, so a min or max never reflects a sample
/// past the horizon, even during a quiet period with no new samples.
///
/// Each filter keeps only the samples that can still become its extremum: a
/// sample is dropped once a newer sample is at least as extreme, because the
/// newer sample outlives it. A sample stamped with the same instant as the
/// newest candidate, and no more extreme, is never kept, because it expires
/// with that candidate. Both candidate lists stay in time order with monotone
/// values, so the oldest fresh candidate is the extremum. Neither insert nor
/// read scans the window.
///
/// A caller that stamps samples on a coarse grid bounds each list at one
/// candidate per grid step in the horizon.
#[derive(Clone, Debug)]
pub struct WindowedSamples {
    horizon: Duration,
    /// Candidates for `min`, with values increasing from front to back.
    minima: VecDeque<(Instant, f64)>,
    /// Candidates for `max`, with values decreasing from front to back.
    maxima: VecDeque<(Instant, f64)>,
    newest_at: Option<Instant>,
}

impl WindowedSamples {
    /// An empty filter over samples no older than `horizon`.
    pub fn new(horizon: Duration) -> Self {
        Self {
            horizon,
            minima: VecDeque::new(),
            maxima: VecDeque::new(),
            newest_at: None,
        }
    }

    /// Adds `value` sampled at `now`. A NaN `value` is ignored.
    pub fn observe(&mut self, now: Instant, value: f64) {
        // Callers pass a monotonic clock. Clamping keeps the candidate lists in time
        // order if a caller ever passes an older `now`.
        let at = self.newest_at.map_or(now, |newest| newest.max(now));
        self.newest_at = Some(at);
        if !value.is_nan() {
            while self.minima.back().is_some_and(|(_, back)| *back >= value) {
                self.minima.pop_back();
            }
            if self.minima.back().is_none_or(|(back_at, _)| *back_at != at) {
                self.minima.push_back((at, value));
            }
            while self.maxima.back().is_some_and(|(_, back)| *back <= value) {
                self.maxima.pop_back();
            }
            if self.maxima.back().is_none_or(|(back_at, _)| *back_at != at) {
                self.maxima.push_back((at, value));
            }
        }
        self.prune(now);
    }

    /// Drop samples older than `horizon` relative to `now`. Called on insert; reads
    /// filter again so a stale extremum is never returned during a quiet bad period.
    fn prune(&mut self, now: Instant) {
        if let Some(cutoff) = now.checked_sub(self.horizon) {
            for candidates in [&mut self.minima, &mut self.maxima] {
                while candidates.front().is_some_and(|(at, _)| *at < cutoff) {
                    candidates.pop_front();
                }
            }
        }
    }

    /// Windowed minimum over samples no older than `now - horizon`. Filters by `now`
    /// rather than trusting the last prune, so a quiet-period read cannot return an
    /// aged-out sample.
    pub fn min(&self, now: Instant) -> Option<f64> {
        self.oldest_fresh(&self.minima, now)
    }

    /// The windowed maximum over samples no older than `now - horizon`. See [`Self::min`].
    pub fn max(&self, now: Instant) -> Option<f64> {
        self.oldest_fresh(&self.maxima, now)
    }

    /// Whether this window has at least one sample no older than `now - horizon`.
    ///
    /// The cold-start gate only needs existence, so it reads the newest sample time.
    pub fn has_fresh_sample(&self, now: Instant) -> bool {
        let Some(newest_at) = self.newest_at else {
            return false;
        };
        now.checked_sub(self.horizon)
            .is_none_or(|cutoff| newest_at >= cutoff)
    }

    fn oldest_fresh(&self, candidates: &VecDeque<(Instant, f64)>, now: Instant) -> Option<f64> {
        let first_fresh = now.checked_sub(self.horizon).map_or(0, |cutoff| {
            candidates.partition_point(|(at, _)| *at < cutoff)
        });
        candidates.get(first_fresh).map(|(_, value)| *value)
    }

    /// The number of `min` and `max` candidates held.
    #[cfg(test)]
    fn candidates(&self) -> usize {
        self.minima.len() + self.maxima.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windowed_samples_match_a_linear_scan() {
        // The candidate lists must return the same extremum as scanning every sample
        // in the horizon, for reads at the insert time and during quiet periods.
        let horizon = Duration::from_millis(500);
        let mut window = WindowedSamples::new(horizon);
        let mut all: Vec<(Instant, f64)> = Vec::new();
        let t0 = Instant::now();
        let mut now = t0;
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            seed >> 33
        };
        for _ in 0..5_000 {
            now += Duration::from_millis(next() % 40);
            let value = (next() % 1_000) as f64;
            window.observe(now, value);
            all.push((now, value));
            let read_at = now + Duration::from_millis(next() % 700);
            for at in [now, read_at] {
                let cutoff = at.checked_sub(horizon);
                let fresh = || {
                    all.iter()
                        .filter(move |(sampled, _)| cutoff.is_none_or(|c| *sampled >= c))
                        .map(|(_, value)| *value)
                };
                assert_eq!(window.min(at), fresh().reduce(f64::min));
                assert_eq!(window.max(at), fresh().reduce(f64::max));
            }
        }
    }

    #[test]
    fn samples_on_one_instant_keep_one_candidate_per_list() {
        let t0 = Instant::now();
        let mut window = WindowedSamples::new(Duration::from_secs(10));
        for value in (0..1_000).rev() {
            window.observe(t0, f64::from(value));
        }
        assert_eq!(window.candidates(), 2);
        assert_eq!(window.max(t0), Some(999.0));
        assert_eq!(window.min(t0), Some(0.0));
    }
}
