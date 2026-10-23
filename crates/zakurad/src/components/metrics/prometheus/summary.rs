//! Quantile summaries of histogram samples over a rolling window.
//!
//! [`Sketch`] is a DDSketch: it counts samples in buckets whose bounds grow geometrically, so
//! every quantile it reports is within [`RELATIVE_ACCURACY`] of the sample at that rank.
//! <https://arxiv.org/abs/1908.10693>

use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

/// The quantiles published for each histogram.
pub(super) const QUANTILES: [f64; 7] = [0.0, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0];

/// The largest relative error of a reported quantile, other than the minimum and maximum, which
/// are exact.
pub(super) const RELATIVE_ACCURACY: f64 = 0.001;

/// The largest number of buckets each sign of a [`Sketch`] keeps.
///
/// At [`RELATIVE_ACCURACY`], this covers values spanning a factor of about 13 million. If
/// samples span more, the smallest buckets are merged, so only the lowest quantiles lose
/// accuracy.
pub(super) const MAX_BUCKETS: usize = 8192;

/// Samples smaller than this in absolute value are counted as zero.
const MIN_VALUE: f64 = 1e-9;

/// The length of each window of samples.
pub(super) const WINDOW_DURATION: Duration = Duration::from_secs(20);

/// The number of windows quantiles are computed over.
pub(super) const WINDOW_COUNT: u32 = 3;

/// The ratio between the bounds of consecutive buckets.
fn gamma() -> f64 {
    (1.0 + RELATIVE_ACCURACY) / (1.0 - RELATIVE_ACCURACY)
}

/// Returns the bucket of a positive sample at least [`MIN_VALUE`].
fn bucket(value: f64) -> i32 {
    // Samples are between `MIN_VALUE` and `f64::MAX`, so the result is between about -10,000
    // and 350,000, which fits in an `i32`.
    (value.ln() / gamma().ln()).ceil() as i32
}

/// Returns the value reported for samples in `bucket`, which is within [`RELATIVE_ACCURACY`]
/// of every value in the bucket.
fn bucket_value(bucket: i32) -> f64 {
    let gamma = gamma();
    (f64::from(bucket) * gamma.ln()).exp() * 2.0 / (1.0 + gamma)
}

/// A summary of samples that can estimate their quantiles.
#[derive(Clone, Debug, Default)]
pub(super) struct Sketch {
    /// Sample counts by bucket, for positive samples.
    positive: BTreeMap<i32, u64>,
    /// Sample counts by bucket of the absolute value, for negative samples.
    negative: BTreeMap<i32, u64>,
    /// The number of samples counted as zero.
    zero: u64,
    /// The number of samples.
    count: u64,
    /// The smallest sample.
    min: f64,
    /// The largest sample.
    max: f64,
}

impl Sketch {
    /// Adds a finite sample.
    pub(super) fn add(&mut self, value: f64) {
        debug_assert!(value.is_finite(), "callers only add finite samples");

        if value >= MIN_VALUE {
            add_to(&mut self.positive, bucket(value), 1);
        } else if value <= -MIN_VALUE {
            add_to(&mut self.negative, bucket(-value), 1);
        } else {
            self.zero += 1;
        }

        if self.count == 0 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
        self.count += 1;
    }

    /// Adds every sample in `other`.
    pub(super) fn merge(&mut self, other: &Sketch) {
        if other.count == 0 {
            return;
        }

        for (&bucket, &count) in &other.positive {
            add_to(&mut self.positive, bucket, count);
        }
        for (&bucket, &count) in &other.negative {
            add_to(&mut self.negative, bucket, count);
        }
        self.zero += other.zero;

        if self.count == 0 {
            self.min = other.min;
            self.max = other.max;
        } else {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
        self.count += other.count;
    }

    /// Returns the estimated sample at quantile `q`, or `None` if there are no samples.
    ///
    /// The 0 and 1 quantiles are the exact minimum and maximum. Other quantiles are the sample
    /// at rank `q * (count - 1)`, rounded down, within [`RELATIVE_ACCURACY`].
    pub(super) fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        if q <= 0.0 {
            return Some(self.min);
        }
        if q >= 1.0 {
            return Some(self.max);
        }

        // `q` is between 0 and 1, so the rank is less than `count`.
        let rank = (q * (self.count - 1) as f64) as u64;

        // Negative samples come first, from the largest absolute value down.
        let mut seen = 0;
        for (&bucket, &count) in self.negative.iter().rev() {
            seen += count;
            if seen > rank {
                return Some(-bucket_value(bucket));
            }
        }
        seen += self.zero;
        if seen > rank {
            return Some(0.0);
        }
        for (&bucket, &count) in &self.positive {
            seen += count;
            if seen > rank {
                return Some(bucket_value(bucket));
            }
        }

        // The counts add up to `count`, which is more than `rank`.
        Some(self.max)
    }
}

#[cfg(test)]
impl Sketch {
    /// The number of buckets of positive samples.
    pub(super) fn positive_bucket_count(&self) -> usize {
        self.positive.len()
    }
}

/// Adds `count` samples to `bucket` in `buckets`, merging the smallest buckets if there are more
/// than [`MAX_BUCKETS`].
fn add_to(buckets: &mut BTreeMap<i32, u64>, bucket: i32, count: u64) {
    *buckets.entry(bucket).or_default() += count;

    while buckets.len() > MAX_BUCKETS {
        let (_, smallest) = buckets
            .pop_first()
            .expect("there are more than MAX_BUCKETS buckets");
        *buckets
            .first_entry()
            .expect("there were more than MAX_BUCKETS buckets")
            .get_mut() += smallest;
    }
}

/// The samples of a histogram, with quantiles over the last [`WINDOW_COUNT`] windows.
#[derive(Debug)]
pub(super) struct RollingSummary {
    /// When window 0 starts.
    start: Instant,
    /// Up to [`WINDOW_COUNT`] windows, by index, oldest first.
    windows: VecDeque<(u64, Sketch)>,
    /// The number of samples since startup.
    count: u64,
    /// The sum of samples since startup.
    sum: f64,
}

impl RollingSummary {
    /// Returns an empty summary whose windows start at `start`.
    pub(super) fn new(start: Instant) -> Self {
        Self {
            start,
            windows: VecDeque::new(),
            count: 0,
            sum: 0.0,
        }
    }

    /// Adds a sample recorded at `now`.
    ///
    /// Every sample is counted and summed, but only finite samples are used for quantiles.
    pub(super) fn add(&mut self, value: f64, now: Instant) {
        self.count += 1;
        self.sum += value;
        if !value.is_finite() {
            return;
        }

        let index = self.window_index(now);
        match self.windows.back_mut() {
            Some((last, sketch)) if *last == index => sketch.add(value),
            _ => {
                let mut sketch = Sketch::default();
                sketch.add(value);
                self.windows.push_back((index, sketch));
                while self.windows.len() > WINDOW_COUNT as usize {
                    self.windows.pop_front();
                }
            }
        }
    }

    /// Returns the samples of windows that started less than [`WINDOW_COUNT`] windows before
    /// `now`.
    pub(super) fn recent(&self, now: Instant) -> Sketch {
        let current = self.window_index(now);
        let mut recent = Sketch::default();
        for (index, sketch) in &self.windows {
            if index + u64::from(WINDOW_COUNT) > current {
                recent.merge(sketch);
            }
        }
        recent
    }

    /// The number of samples since startup.
    pub(super) fn count(&self) -> u64 {
        self.count
    }

    /// The sum of samples since startup.
    pub(super) fn sum(&self) -> f64 {
        self.sum
    }

    /// Returns the index of the window that `now` is in.
    fn window_index(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.start);
        // Windows are seconds long, so this would take billions of years to overflow.
        (elapsed.as_nanos() / WINDOW_DURATION.as_nanos()) as u64
    }
}
