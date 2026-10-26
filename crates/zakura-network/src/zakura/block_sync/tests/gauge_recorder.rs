//! A metrics recorder that keeps the current value of each gauge.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// Records the current value of every gauge. Install it with
/// [`metrics::set_default_local_recorder`] to capture this thread's writes.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::block_sync) struct GaugeRecorder(Arc<Mutex<BTreeMap<String, f64>>>);

impl GaugeRecorder {
    /// The value of gauge `name`, as whole bytes. A gauge nobody wrote reads 0,
    /// as it does in an exporter.
    pub(in crate::zakura::block_sync) fn bytes(&self, name: &str) -> u64 {
        let value = self
            .0
            .lock()
            .expect("no gauge write panics")
            .get(name)
            .copied()
            .unwrap_or_default();
        assert!(value >= 0.0, "gauge {name} is {value}, below zero");
        // Test byte quantities are small whole numbers, which f64 holds exactly.
        value as u64
    }
}

struct RecordedGauge {
    name: String,
    values: Arc<Mutex<BTreeMap<String, f64>>>,
}

impl metrics::GaugeFn for RecordedGauge {
    fn increment(&self, value: f64) {
        *self
            .values
            .lock()
            .expect("no gauge write panics")
            .entry(self.name.clone())
            .or_default() += value;
    }

    fn decrement(&self, value: f64) {
        self.increment(-value);
    }

    fn set(&self, value: f64) {
        self.values
            .lock()
            .expect("no gauge write panics")
            .insert(self.name.clone(), value);
    }
}

impl metrics::Recorder for GaugeRecorder {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::from_arc(Arc::new(RecordedGauge {
            name: key.name().to_owned(),
            values: Arc::clone(&self.0),
        }))
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}
