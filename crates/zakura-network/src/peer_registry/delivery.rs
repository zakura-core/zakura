//! An estimate of how long one legacy peer takes to answer a block request.
//!
//! The estimate is a time-weighted average of the peer's measured answer times: heartbeat pings,
//! transaction replies and small block replies. Older samples count for less, so a peer that slows
//! down loses its good estimate within minutes. A peer that announces a block and then does not
//! deliver it is not penalised here: block routing stops preferring it until it delivers one.

use std::time::{Duration, Instant};

/// How quickly old samples stop counting. A sample this old has `1/e` of a new sample's weight.
const TIME_CONSTANT: Duration = Duration::from_secs(10 * 60);

/// One connection's time-weighted average answer time.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DeliveryEstimate {
    /// The weighted average of the samples, in seconds.
    mean_secs: f64,
    /// The total weight of the samples, decayed to `updated_at`.
    weight: f64,
    /// When the last sample was recorded, or `None` before the first.
    updated_at: Option<Instant>,
}

impl DeliveryEstimate {
    /// Mixes in one answer time measured at `now`.
    ///
    /// Earlier samples are first decayed by the time since the last sample, so recent samples
    /// count for more.
    pub(crate) fn record(&mut self, sample: Duration, now: Instant) {
        let previous_weight = match self.updated_at {
            Some(updated_at) => {
                let age = now.saturating_duration_since(updated_at).as_secs_f64();
                self.weight * (-age / TIME_CONSTANT.as_secs_f64()).exp()
            }
            None => 0.0,
        };
        let weight = previous_weight + 1.0;
        self.mean_secs = (self.mean_secs * previous_weight + sample.as_secs_f64()) / weight;
        self.weight = weight;
        self.updated_at = Some(now);
    }

    /// The decayed number of samples the estimate rests on, as of the last sample.
    pub(crate) fn weight(&self) -> f64 {
        self.weight
    }

    /// The expected answer time, or `None` when nothing has been measured yet.
    pub(crate) fn score(&self) -> Option<Duration> {
        self.updated_at?;
        // The mean of non-negative finite samples is non-negative and finite.
        Some(Duration::try_from_secs_f64(self.mean_secs).unwrap_or(Duration::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn no_samples_means_no_score() {
        assert_eq!(DeliveryEstimate::default().score(), None);
    }

    #[test]
    fn the_first_sample_sets_the_score() {
        let mut estimate = DeliveryEstimate::default();
        estimate.record(ms(40), Instant::now());
        assert_eq!(estimate.score(), Some(ms(40)));
    }

    #[test]
    fn samples_at_the_same_time_are_averaged() {
        let now = Instant::now();
        let mut estimate = DeliveryEstimate::default();
        estimate.record(ms(40), now);
        estimate.record(ms(80), now);
        assert_eq!(estimate.score(), Some(ms(60)));
    }

    #[test]
    fn newer_samples_count_for_more_once_time_passes() {
        let start = Instant::now();
        let mut estimate = DeliveryEstimate::default();
        estimate.record(ms(400), start);
        estimate.record(ms(20), start + TIME_CONSTANT * 3);

        let score = estimate.score().expect("measured");
        assert!(
            score < ms(40),
            "a sample three time constants old barely counts: {score:?}"
        );
    }
}
