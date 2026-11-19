//! An estimate of how long one legacy peer takes to answer a block request.
//!
//! The expected time is the peer's round trip, times the round trips a block of that size needs
//! on a new connection, times the peer's large-block slowdown factor:
//!
//! - the round trip is a time-weighted average of heartbeat pings and of block replies small
//!   enough to arrive in one round trip ([`ROUND_TRIP_REPLY_BYTES`]);
//! - the round trips needed come from the block's size alone ([`round_trips`]);
//! - the slowdown factor is a time-weighted average of how the peer's larger replies compared
//!   with that model. It absorbs what the size cannot say: how warm the connection usually is,
//!   and how fast the peer and the path to it are.
//!
//! For a block that fits one round trip, or whose size is not known, the expected time is the
//! round trip alone.
//!
//! Older samples count for less, so a peer that slows down loses its good estimate within
//! minutes. A peer that announces a block and then does not deliver it keeps its score. With
//! scoring enabled, routing passes over it until a later delivery verifies or the miss expires.

use std::time::{Duration, Instant};

/// How quickly old samples stop counting. A sample this old has `1/e` of a new sample's weight.
const TIME_CONSTANT: Duration = Duration::from_secs(10 * 60);

/// The largest block reply, in serialized bytes, whose answer time counts as a round trip.
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
    /// Whether the sample is a round trip, and so counts toward the peer's round trip rather
    /// than its large-block slowdown factor. Every ping is one. A block reply is one when it
    /// is at most [`ROUND_TRIP_REPLY_BYTES`].
    pub(crate) fn is_round_trip(self) -> bool {
        match self {
            Self::Ping => true,
            Self::Block { wire_bytes } => wire_bytes <= ROUND_TRIP_REPLY_BYTES,
        }
    }

    /// The serialized size of the blocks in a block reply, or `None` for a ping.
    pub(crate) fn wire_bytes(self) -> Option<u64> {
        match self {
            Self::Ping => None,
            Self::Block { wire_bytes } => Some(wire_bytes),
        }
    }
}

/// How many round trips a new connection needs to deliver `wire_bytes`.
///
/// A new connection sends [`ROUND_TRIP_REPLY_BYTES`] in its first round trip and doubles the
/// amount each trip, so `n` trips carry `ROUND_TRIP_REPLY_BYTES × (2^n − 1)` bytes. A reply
/// within the limit takes one.
///
/// This is the cold-connection model. On mainnet, 1.5 MB blocks (6.7 round trips by this
/// formula) took about 6.3 round trips on a cold connection and about 2.3 on a warm one. A
/// peer's large-block slowdown factor absorbs that difference.
fn round_trips(wire_bytes: u64) -> f64 {
    // Sizes are far below 2^53, so both conversions to f64 are exact.
    (wire_bytes as f64 / ROUND_TRIP_REPLY_BYTES as f64 + 1.0)
        .log2()
        .max(1.0)
}

/// A time-weighted average. A sample one [`TIME_CONSTANT`] old has `1/e` of a new one's weight.
#[derive(Clone, Copy, Debug, Default)]
struct DecayingMean {
    /// The weighted average of the samples.
    mean: f64,
    /// The total weight of the samples, decayed to `updated_at`.
    weight: f64,
    /// When the latest sample was measured, or `None` before the first.
    updated_at: Option<Instant>,
}

impl DecayingMean {
    /// Weighs `value` by `measured_at`, which may be earlier than the latest sample's time:
    /// verification can confirm an older block delivery after a newer heartbeat was recorded.
    fn record(&mut self, value: f64, measured_at: Instant) {
        let previous = self.updated_at.unwrap_or(measured_at);
        let latest = previous.max(measured_at);
        let decay =
            |at: Instant| (-(latest - at).as_secs_f64() / TIME_CONSTANT.as_secs_f64()).exp();
        let existing_weight = self.weight * decay(previous);
        let sample_weight = decay(measured_at);
        let weight = existing_weight + sample_weight;
        self.mean = (self.mean * existing_weight + value * sample_weight) / weight;
        self.weight = weight;
        self.updated_at = Some(latest);
    }

    /// The average, or `None` before the first sample.
    fn value(&self) -> Option<f64> {
        self.updated_at.map(|_| self.mean)
    }
}

/// One connection's expected answer time: its round trip, and how its large blocks have
/// compared with the cold-connection model.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct DeliveryEstimate {
    /// The average round trip, in seconds.
    round_trip: DecayingMean,
    /// The average of each larger reply's time divided by what the cold-connection model
    /// predicted for its size.
    large_block_slowdown: DecayingMean,
}

impl DeliveryEstimate {
    /// Records that the peer took `sample` to answer, with the answer arriving at `received_at`.
    ///
    /// A round trip ([`DeliverySampleKind::is_round_trip`]) updates the round trip. A larger
    /// block reply updates the large-block slowdown factor instead, and is ignored while no
    /// round trip is known to compare it with.
    pub(crate) fn record(
        &mut self,
        kind: DeliverySampleKind,
        sample: Duration,
        received_at: Instant,
    ) {
        if kind.is_round_trip() {
            self.round_trip.record(sample.as_secs_f64(), received_at);
        } else if let Some(cold_secs) = kind
            .wire_bytes()
            .and_then(|wire_bytes| self.cold_transfer_secs(wire_bytes))
            // A zero round trip predicts zero, which no sample can be divided by.
            .filter(|cold_secs| *cold_secs > 0.0)
        {
            self.large_block_slowdown
                .record(sample.as_secs_f64() / cold_secs, received_at);
        }
    }

    /// The seconds a new connection to this peer would take to deliver `wire_bytes`, or `None`
    /// when no round trip is known.
    fn cold_transfer_secs(&self, wire_bytes: u64) -> Option<f64> {
        Some(self.round_trip.value()? * round_trips(wire_bytes))
    }

    /// The decayed number of round-trip samples the score rests on, as of the last one.
    pub(crate) fn weight(&self) -> f64 {
        self.round_trip.weight
    }

    /// The peer's round trip, or `None` when none has been measured yet.
    pub(crate) fn score(&self) -> Option<Duration> {
        self.round_trip.value().map(duration_from_secs)
    }

    /// How many times longer than the cold-connection model the peer's large blocks have taken,
    /// or `None` when none has been measured against a known round trip.
    pub(crate) fn large_block_slowdown(&self) -> Option<f64> {
        self.large_block_slowdown.value()
    }

    /// The expected time for the peer to deliver a block of `wire_bytes` serialized bytes, or
    /// `None` when no round trip has been measured yet.
    ///
    /// This is the round trip when the size is unknown or fits one round trip. For a larger
    /// block it is the cold-connection time for that size, times the peer's large-block
    /// slowdown factor. A peer with no large-block record is taken at the model's word.
    pub(crate) fn expected(&self, wire_bytes: Option<u64>) -> Option<Duration> {
        let large = wire_bytes.filter(|wire_bytes| *wire_bytes > ROUND_TRIP_REPLY_BYTES);
        let Some(wire_bytes) = large else {
            return self.score();
        };
        let slowdown = self.large_block_slowdown().unwrap_or(1.0);
        Some(duration_from_secs(
            self.cold_transfer_secs(wire_bytes)? * slowdown,
        ))
    }
}

/// Converts an average of non-negative samples to a duration. Such an average is non-negative,
/// and one too large to represent becomes the longest duration.
fn duration_from_secs(secs: f64) -> Duration {
    Duration::try_from_secs_f64(secs).unwrap_or(Duration::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PING: DeliverySampleKind = DeliverySampleKind::Ping;

    /// A 1.5 MB block.
    const LARGE: u64 = 1_500_000;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn block(wire_bytes: u64) -> DeliverySampleKind {
        DeliverySampleKind::Block { wire_bytes }
    }

    /// An estimate with one 100 ms round trip, measured at the returned time.
    fn measured() -> (DeliveryEstimate, Instant) {
        let now = Instant::now();
        let mut estimate = DeliveryEstimate::default();
        estimate.record(PING, ms(100), now);
        (estimate, now)
    }

    /// What the cold-connection model predicts for `estimate`'s peer and a 1.5 MB block.
    fn cold(estimate: &DeliveryEstimate) -> Duration {
        Duration::from_secs_f64(estimate.cold_transfer_secs(LARGE).expect("measured"))
    }

    #[test]
    fn a_block_within_the_limit_needs_one_round_trip() {
        assert_eq!(round_trips(0), 1.0);
        assert_eq!(round_trips(1_700), 1.0);
        assert_eq!(round_trips(ROUND_TRIP_REPLY_BYTES), 1.0);
        assert!(round_trips(ROUND_TRIP_REPLY_BYTES + 1) > 1.0);
        assert_eq!(
            round_trips(3 * ROUND_TRIP_REPLY_BYTES),
            2.0,
            "the second trip carries twice the first"
        );
    }

    #[test]
    fn a_full_block_needs_about_seven_round_trips() {
        let trips = round_trips(LARGE);
        assert!((6.6..6.8).contains(&trips), "1.5 MB took {trips} trips");
    }

    #[test]
    fn a_slow_large_block_raises_the_large_block_expectation_only() {
        let (mut estimate, now) = measured();
        let before = estimate.expected(Some(LARGE)).expect("measured");
        assert_eq!(before, cold(&estimate), "no record means the cold model");

        estimate.record(block(LARGE), before * 3, now);

        assert_eq!(
            estimate.score(),
            Some(ms(100)),
            "the round trip is unchanged"
        );
        assert_eq!(
            estimate.expected(Some(ROUND_TRIP_REPLY_BYTES)),
            Some(ms(100)),
            "a small block is still expected in one round trip"
        );
        let after = estimate.expected(Some(LARGE)).expect("measured");
        assert!(
            after > before * 2,
            "a reply three times slower than the model raises the expectation: {after:?}"
        );
    }

    #[test]
    fn an_unknown_size_is_expected_in_one_round_trip() {
        let (mut estimate, now) = measured();
        estimate.record(block(LARGE), Duration::from_secs(5), now);
        assert_eq!(estimate.expected(None), Some(ms(100)));
        assert_eq!(estimate.expected(None), estimate.score());
    }

    #[test]
    fn a_peer_measured_only_on_large_blocks_has_no_expectation() {
        let mut estimate = DeliveryEstimate::default();
        estimate.record(block(LARGE), Duration::from_secs(2), Instant::now());
        assert_eq!(estimate.score(), None);
        assert_eq!(estimate.expected(None), None);
        assert_eq!(estimate.expected(Some(LARGE)), None);
        assert_eq!(
            estimate.large_block_slowdown(),
            None,
            "there was no round trip to compare the reply with"
        );
    }

    #[test]
    fn a_reply_as_slow_as_the_cold_model_gives_a_factor_of_one() {
        let (mut estimate, now) = measured();
        estimate.record(block(LARGE), cold(&estimate), now);
        let slowdown = estimate.large_block_slowdown().expect("measured");
        assert!((slowdown - 1.0).abs() < 1e-6, "factor {slowdown}");
    }

    #[test]
    fn a_warm_connection_s_fast_large_blocks_lower_the_expectation() {
        let (mut estimate, now) = measured();
        estimate.record(block(LARGE), cold(&estimate) / 3, now);
        let expected = estimate.expected(Some(LARGE)).expect("measured");
        assert!(expected < cold(&estimate) / 2, "expected {expected:?}");
    }

    #[test]
    fn a_large_block_after_a_zero_round_trip_is_ignored() {
        let now = Instant::now();
        let mut estimate = DeliveryEstimate::default();
        estimate.record(PING, Duration::ZERO, now);
        estimate.record(block(LARGE), Duration::from_secs(2), now);
        assert_eq!(estimate.large_block_slowdown(), None);
        assert_eq!(estimate.expected(Some(LARGE)), Some(Duration::ZERO));
    }

    #[test]
    fn late_verification_uses_the_original_receipt_time() {
        let early = Instant::now();
        let late = early + TIME_CONSTANT;
        let mut chronological = DeliveryEstimate::default();
        chronological.record(PING, ms(200), early);
        chronological.record(PING, ms(10), late);
        let mut delayed = DeliveryEstimate::default();
        delayed.record(PING, ms(10), late);
        delayed.record(PING, ms(200), early);
        assert_eq!(delayed.score(), chronological.score());
        assert_eq!(delayed.weight(), chronological.weight());
    }

    #[test]
    fn a_block_reply_is_a_round_trip_up_to_the_size_limit() {
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
        estimate.record(PING, ms(40), Instant::now());
        assert_eq!(estimate.score(), Some(ms(40)));
    }

    #[test]
    fn samples_at_the_same_time_are_averaged() {
        let now = Instant::now();
        let mut estimate = DeliveryEstimate::default();
        estimate.record(PING, ms(40), now);
        estimate.record(PING, ms(80), now);
        assert_eq!(estimate.score(), Some(ms(60)));
    }

    #[test]
    fn newer_samples_count_for_more_once_time_passes() {
        let start = Instant::now();
        let mut estimate = DeliveryEstimate::default();
        estimate.record(PING, ms(400), start);
        estimate.record(PING, ms(20), start + TIME_CONSTANT * 3);

        let score = estimate.score().expect("measured");
        assert!(
            score < ms(40),
            "a sample three time constants old barely counts: {score:?}"
        );
    }
}
