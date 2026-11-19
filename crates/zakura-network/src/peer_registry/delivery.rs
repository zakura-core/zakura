//! An estimate of how long one legacy peer takes to answer a block request.
//!
//! The estimate is a time-weighted average of the peer's measured round trips: heartbeat pings,
//! and block replies small enough to arrive in one round trip ([`ROUND_TRIP_REPLY_BYTES`]). A
//! larger reply's time depends on the block's size and on how warm the connection is, so it is
//! measured and traced but leaves the estimate alone. Older samples count for less, so a peer
//! that slows down loses its good estimate within minutes. A peer that announces a block and then does not
//! deliver it keeps its score. With scoring enabled, routing passes over it until a later
//! delivery verifies or the miss expires.

use std::time::{Duration, Instant};

/// How quickly old samples stop counting. A sample this old has `1/e` of a new sample's weight.
const TIME_CONSTANT: Duration = Duration::from_secs(10 * 60);

/// The largest block reply, in serialized bytes, whose answer time counts toward the estimate.
///
/// A new TCP connection may send 10 segments of 1,460 bytes before it waits for an
/// acknowledgement. A reply that fits arrives in one round trip even on a connection that has
/// been idle, so its answer time measures what a ping measures. A larger reply takes further
/// round trips, and how many depends on its size and on how far the connection's window has
/// grown.
///
/// This is an empirical first choice and may be tuned. On mainnet, 3-11 s downloads of
/// 0.9-1.6 MB blocks raised the delivering peer's estimate from about 250 ms to
/// 500-2,000 ms.
pub(crate) const ROUND_TRIP_REPLY_BYTES: u64 = 10 * 1_460;

/// What a delivery sample measured.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliverySampleKind {
    /// A heartbeat ping's round trip.
    Ping,
    /// A reply to a block request that held at least one requested block.
    Block {
        /// The serialized size of the blocks in the reply.
        wire_bytes: u64,
    },
}

impl DeliverySampleKind {
    /// Whether the sample is a round trip, and so counts toward the estimate. Every ping is
    /// one. A block reply is one when it is at most [`ROUND_TRIP_REPLY_BYTES`].
    pub(crate) fn is_round_trip(self) -> bool {
        match self {
            Self::Ping => true,
            Self::Block { wire_bytes } => wire_bytes <= ROUND_TRIP_REPLY_BYTES,
        }
    }
}

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
    fn a_block_reply_is_a_round_trip_up_to_the_size_limit() {
        let block = |wire_bytes| DeliverySampleKind::Block { wire_bytes };
        assert!(DeliverySampleKind::Ping.is_round_trip());
        assert!(block(1_700).is_round_trip());
        assert!(
            block(ROUND_TRIP_REPLY_BYTES).is_round_trip(),
            "a reply of exactly the limit still fits one round trip"
        );
        assert!(
            !block(ROUND_TRIP_REPLY_BYTES + 1).is_round_trip(),
            "one byte over the limit needs another round trip"
        );
        assert_eq!(ROUND_TRIP_REPLY_BYTES, 14_600);
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
