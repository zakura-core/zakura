//! An estimate of how long one legacy peer takes to answer a block request.
//!
//! The estimate is a time-weighted average of the peer's measured answer times: heartbeat pings,
//! transaction replies and small block replies. Older samples count for less, so a peer that slows
//! down loses its good estimate within minutes. A peer that announces a block and then does not
//! deliver it is not penalised here: block routing stops preferring it until it delivers one.

use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
    time::{Duration, Instant},
};

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

/// Keep a closed score for three decay time constants after its last measurement.
pub(super) const REMEMBER_FOR: Duration = Duration::from_secs(30 * 60);
const MAX_CLOSED_SCORES: usize = 4096;

/// A score and missed-request history age independently within one cache entry.
#[derive(Debug, Default)]
pub(super) struct BlockSourceHistory {
    pub(super) estimate: Option<DeliveryEstimate>,
    pub(super) missed_block: crate::peer::MissedBlock,
}

/// The most recently closed block source per IPv4 address or IPv6 /64.
/// The age index lets disconnects prune expired scores without scanning the cache.
#[derive(Debug, Default)]
pub(super) struct ClosedScores {
    last_activity: HashMap<IpAddr, Instant>,
    by_age: BTreeMap<(Instant, IpAddr), BlockSourceHistory>,
}

impl ClosedScores {
    fn key(ip: IpAddr) -> IpAddr {
        match ip.to_canonical() {
            IpAddr::V6(addr) => IpAddr::V6((addr.to_bits() & (u128::MAX << 64)).into()),
            v4 => v4,
        }
    }

    /// Consumes the last closed estimate, including when it has expired.
    pub(super) fn take(&mut self, ip: IpAddr, now: Instant) -> Option<BlockSourceHistory> {
        let key = Self::key(ip);
        let updated = self.last_activity.remove(&key)?;
        let mut history = self
            .by_age
            .remove(&(updated, key))
            .filter(|_| now.saturating_duration_since(updated) <= REMEMBER_FOR)?;
        history.estimate = history.estimate.filter(|estimate| {
            estimate
                .updated_at
                .is_some_and(|time| now.saturating_duration_since(time) <= REMEMBER_FOR)
        });
        history.missed_block.reconnect(now, REMEMBER_FOR);
        Some(history)
    }

    pub(super) fn remember(&mut self, ip: IpAddr, history: BlockSourceHistory, now: Instant) {
        let Some(updated) = history
            .estimate
            .and_then(|score| score.updated_at)
            .max(history.missed_block.last_activity())
        else {
            return;
        };
        let key = Self::key(ip);
        // Replace even if the last connection to close has an older sample.
        if let Some(previous) = self.last_activity.insert(key, updated) {
            self.by_age.remove(&(previous, key));
        }
        self.by_age.insert((updated, key), history);

        while let Some((&(oldest, address), _)) = self.by_age.first_key_value() {
            if self.by_age.len() <= MAX_CLOSED_SCORES
                && now.saturating_duration_since(oldest) <= REMEMBER_FOR
            {
                break;
            }
            self.by_age.pop_first();
            self.last_activity.remove(&address);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn eviction_uses_sample_time_and_prunes_expired_scores_on_insert() {
        let now = Instant::now();
        let mut scores = ClosedScores::default();
        let mut recent = DeliveryEstimate::default();
        recent.record(ms(10), now);
        let mut older = DeliveryEstimate::default();
        older.record(ms(20), now - Duration::from_secs(1));
        let ip = |n: u32| IpAddr::V4(n.into());
        let measured = |estimate| BlockSourceHistory {
            estimate: Some(estimate),
            ..Default::default()
        };
        for n in 1..=u32::try_from(MAX_CLOSED_SCORES).unwrap() {
            scores.remember(ip(n), measured(recent), now);
        }
        scores.remember(ip(0), measured(older), now);
        assert_eq!(scores.by_age.len(), MAX_CLOSED_SCORES);
        assert!(scores.take(ip(0), now).is_none());

        let later = now + REMEMBER_FOR + Duration::from_nanos(1);
        recent.record(ms(30), later);
        scores.remember(ip(0), measured(recent), later);
        assert_eq!(scores.by_age.len(), 1);
        assert!(scores.take(ip(0), later).is_some());
        assert!(scores.last_activity.is_empty());
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
