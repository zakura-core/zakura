#![allow(clippy::unwrap_in_result)]

mod prop;
mod vectors;

use std::sync::Mutex;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

use super::ContextualMetrics;

#[derive(Default)]
struct MetricNameRecorder {
    histogram_names: Mutex<Vec<String>>,
}

impl Recorder for MetricNameRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.histogram_names
            .lock()
            .expect("the metric test does not poison its recorder")
            .push(key.name().to_owned());
        Histogram::noop()
    }
}

#[test]
fn contextual_metrics_record_only_selected_series() {
    let recorder = MetricNameRecorder::default();

    assert_eq!(
        ContextualMetrics::for_commit(false),
        ContextualMetrics::AllBlocks
    );
    assert_eq!(
        ContextualMetrics::for_commit(true),
        ContextualMetrics::Mined
    );

    metrics::with_local_recorder(&recorder, || {
        ContextualMetrics::Disabled.record_duration(
            "test.contextual.disabled.all",
            "test.contextual.disabled.mined",
            std::time::Duration::ZERO,
        );
        ContextualMetrics::AllBlocks.record_duration(
            "test.contextual.all.all",
            "test.contextual.all.mined",
            std::time::Duration::ZERO,
        );
        ContextualMetrics::Mined.record_duration(
            "test.contextual.mined.all",
            "test.contextual.mined.mined",
            std::time::Duration::ZERO,
        );
    });

    assert_eq!(
        *recorder
            .histogram_names
            .lock()
            .expect("the metric test does not poison its recorder"),
        [
            "test.contextual.all.all",
            "test.contextual.mined.all",
            "test.contextual.mined.mined",
        ]
    );
}
