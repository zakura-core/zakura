//! Storing metrics from the `metrics` crate.

use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, Mutex, PoisonError, RwLock},
    time::Instant,
};

use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

use super::{summary::RollingSummary, text};

/// A recorder that stores every metric it is given, to be rendered by [`Self::render`].
///
/// Clones share the same metrics.
#[derive(Clone, Debug, Default)]
pub(super) struct PrometheusRecorder {
    metrics: Arc<Metrics>,
}

/// The metrics a [`PrometheusRecorder`] has stored.
#[derive(Debug, Default)]
pub(super) struct Metrics {
    /// Counter values by key.
    pub(super) counters: RwLock<HashMap<Key, Arc<AtomicU64>>>,
    /// Gauge values by key, as the bits of an `f64`.
    pub(super) gauges: RwLock<HashMap<Key, Arc<AtomicU64>>>,
    /// Histogram samples by key.
    pub(super) histograms: RwLock<HashMap<Key, Arc<HistogramSamples>>>,
    /// Descriptions by sanitized metric name.
    pub(super) descriptions: RwLock<HashMap<String, SharedString>>,
}

impl PrometheusRecorder {
    /// Renders every stored metric in the Prometheus text format, with histogram quantiles as of
    /// `now`.
    pub(super) fn render(&self, now: Instant) -> String {
        text::render(&self.metrics, now)
    }

    /// Stores `description` for the metric called `name`, unless it already has one.
    fn describe(&self, name: &KeyName, description: SharedString) {
        write(&self.metrics.descriptions)
            .entry(text::sanitize_metric_name(name.as_str()))
            .or_insert(description);
    }
}

impl Recorder for PrometheusRecorder {
    fn describe_counter(&self, name: KeyName, _unit: Option<Unit>, description: SharedString) {
        self.describe(&name, description);
    }

    fn describe_gauge(&self, name: KeyName, _unit: Option<Unit>, description: SharedString) {
        self.describe(&name, description);
    }

    fn describe_histogram(&self, name: KeyName, _unit: Option<Unit>, description: SharedString) {
        self.describe(&name, description);
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(get_or_insert(&self.metrics.counters, key, Default::default))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(get_or_insert(&self.metrics.gauges, key, Default::default))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(get_or_insert(&self.metrics.histograms, key, || {
            HistogramSamples(Mutex::new(RollingSummary::new(Instant::now())))
        }))
    }
}

/// The samples of a histogram.
#[derive(Debug)]
pub(super) struct HistogramSamples(Mutex<RollingSummary>);

impl HistogramSamples {
    /// Runs `f` on the summary of the samples.
    pub(super) fn with_summary<T>(&self, f: impl FnOnce(&RollingSummary) -> T) -> T {
        f(&self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl HistogramFn for HistogramSamples {
    fn record(&self, value: f64) {
        let now = Instant::now();
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .add(value, now);
    }
}

/// Returns the metric stored for `key` in `metrics`, storing `new()` first if there is none.
fn get_or_insert<T>(
    metrics: &RwLock<HashMap<Key, Arc<T>>>,
    key: &Key,
    new: impl FnOnce() -> T,
) -> Arc<T> {
    if let Some(metric) = read(metrics).get(key) {
        return metric.clone();
    }
    write(metrics)
        .entry(key.clone())
        .or_insert_with(|| Arc::new(new()))
        .clone()
}

/// Locks `lock` for reading, even if a panic poisoned it, because metrics stay consistent.
pub(super) fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

/// Locks `lock` for writing, even if a panic poisoned it, because metrics stay consistent.
fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}
