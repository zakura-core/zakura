//! An estimate of how long one legacy peer takes to answer a block request.
//!
//! The estimate is a time-weighted average of the peer's measured answer times: heartbeat pings
//! and block replies of any size. Older samples count for less, so a peer that slows
//! down loses its good estimate within minutes. A peer that announces a block and then does not
//! deliver it keeps its score. With scoring enabled, routing passes over it until a later
//! delivery verifies or the miss expires.

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
    /// Weighs a sample by when the answer arrived. Verification can confirm older block
    /// deliveries after a newer heartbeat has already updated the estimate.
    pub(crate) fn record(&mut self, sample: Duration, received_at: Instant) {
        let previous = self.updated_at.unwrap_or(received_at);
        let latest = previous.max(received_at);
        let decay =
            |at: Instant| (-(latest - at).as_secs_f64() / TIME_CONSTANT.as_secs_f64()).exp();
        let existing_weight = self.weight * decay(previous);
        let sample_weight = decay(received_at);
        let weight = existing_weight + sample_weight;
        self.mean_secs =
            (self.mean_secs * existing_weight + sample.as_secs_f64() * sample_weight) / weight;
        self.weight = weight;
        self.updated_at = Some(latest);
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
    fn late_verification_uses_the_original_receipt_time() {
        let early = Instant::now();
        let late = early + TIME_CONSTANT;
        let mut chronological = DeliveryEstimate::default();
        chronological.record(ms(200), early);
        chronological.record(ms(10), late);
        let mut delayed = DeliveryEstimate::default();
        delayed.record(ms(10), late);
        delayed.record(ms(200), early);
        assert_eq!(delayed.score(), chronological.score());
        assert_eq!(delayed.weight(), chronological.weight());
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
