//! A metrics recorder for tests that check which metric series peer code creates.

use std::{collections::BTreeSet, sync::Mutex};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

/// A metrics recorder that keeps the name and labels of every metric registered with it.
///
/// A Prometheus exporter retains one series per distinct name and label set, so the
/// recorded series are the ones a scrape would render.
#[derive(Default)]
pub(crate) struct SeriesRecorder(Mutex<BTreeSet<String>>);

impl SeriesRecorder {
    /// Runs `f` with this recorder installed for the current thread.
    pub(crate) fn record<T>(&self, f: impl FnOnce() -> T) -> T {
        metrics::with_local_recorder(self, f)
    }

    /// Returns every distinct series of `metric` registered so far, as `name{key=value,...}`.
    pub(crate) fn series(&self, metric: &str) -> BTreeSet<String> {
        self.0
            .lock()
            .expect("test recorder lock is never poisoned")
            .iter()
            .filter(|series| series.split('{').next() == Some(metric))
            .cloned()
            .collect()
    }

    /// Records the series of a newly registered metric.
    fn register(&self, key: &Key) {
        let labels: Vec<String> = key
            .labels()
            .map(|label| format!("{}={}", label.key(), label.value()))
            .collect();

        self.0
            .lock()
            .expect("test recorder lock is never poisoned")
            .insert(format!("{}{{{}}}", key.name(), labels.join(",")));
    }
}

impl Recorder for SeriesRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.register(key);
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.register(key);
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.register(key);
        Histogram::noop()
    }
}
