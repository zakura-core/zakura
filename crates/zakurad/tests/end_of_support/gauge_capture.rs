//! Isolated gauge capture for the end-of-support integration tests.
//!
//! A capture is installed as the metrics recorder of the test's own thread, so
//! concurrent tests do not see each other's gauges.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use metrics::{
    Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

/// The latest value of every gauge set while this recorder is installed.
#[derive(Clone, Default)]
pub(super) struct CapturedGauges(Arc<Mutex<HashMap<String, f64>>>);

impl CapturedGauges {
    /// Returns the latest value of the gauge called `name`, or `None` if it was
    /// never set.
    pub(super) fn value(&self, name: &str) -> Option<f64> {
        self.values().get(name).copied()
    }

    fn values(&self) -> std::sync::MutexGuard<'_, HashMap<String, f64>> {
        self.0
            .lock()
            .expect("no code panics while holding the gauge map lock")
    }
}

/// One gauge of a [`CapturedGauges`] recorder.
struct CapturedGauge {
    name: String,
    gauges: CapturedGauges,
}

impl GaugeFn for CapturedGauge {
    fn increment(&self, value: f64) {
        *self.gauges.values().entry(self.name.clone()).or_default() += value;
    }

    fn decrement(&self, value: f64) {
        self.increment(-value);
    }

    fn set(&self, value: f64) {
        self.gauges.values().insert(self.name.clone(), value);
    }
}

impl Recorder for CapturedGauges {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(CapturedGauge {
            name: key.name().to_owned(),
            gauges: self.clone(),
        }))
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}
