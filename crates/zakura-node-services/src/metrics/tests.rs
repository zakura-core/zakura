//! Tests that the metrics macros register the same metrics as the `metrics` macros.

use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, Mutex},
};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

/// A recorder that keeps every counter and gauge it registers, by kind and key.
#[derive(Default)]
struct KeyRecorder {
    metrics: Mutex<HashMap<(&'static str, Key), Arc<AtomicU64>>>,
}

impl KeyRecorder {
    fn handle(&self, kind: &'static str, key: &Key) -> Arc<AtomicU64> {
        self.metrics
            .lock()
            .expect("no test thread panics while holding the lock")
            .entry((kind, key.clone()))
            .or_default()
            .clone()
    }

    fn value(&self, kind: &'static str, key: Key) -> u64 {
        self.handle(kind, &key)
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn keys(&self) -> Vec<(&'static str, Key)> {
        let mut keys: Vec<_> = self
            .metrics
            .lock()
            .expect("no test thread panics while holding the lock")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    }
}

impl Recorder for KeyRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(self.handle("counter", key))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(self.handle("gauge", key))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.handle("histogram", key);
        Histogram::noop()
    }
}

/// Records one metric of each kind and macro form, with `counter`, `gauge` and `histogram`.
macro_rules! record_each_form {
    ($counter:path, $gauge:path, $histogram:path) => {{
        let peer = String::from("peer-1");
        $counter!("static").increment(1);
        $counter!("static.labels", "kind" => "block", "dir" => "in").increment(2);
        $counter!("dynamic.labels", "peer" => peer.clone()).increment(3);
        $counter!(format!("dynamic.{}", "name")).increment(4);
        $gauge!("gauge", "peer" => peer).set(5.0);
        $histogram!("histogram", "kind" => "block").record(6.0);
    }};
}

#[test]
// Compares against the `metrics` macros that clippy.toml disallows elsewhere.
#[allow(clippy::disallowed_macros)]
fn macros_register_the_same_metrics_as_the_metrics_crate() {
    let ours = KeyRecorder::default();
    metrics::with_local_recorder(&ours, || {
        record_each_form!(super::counter, super::gauge, super::histogram)
    });

    let upstream = KeyRecorder::default();
    metrics::with_local_recorder(&upstream, || {
        record_each_form!(metrics::counter, metrics::gauge, metrics::histogram)
    });

    assert_eq!(ours.keys(), upstream.keys());
    assert_eq!(ours.keys().len(), 6);
    assert_eq!(
        ours.value(
            "counter",
            Key::from_parts("static.labels", &[("kind", "block"), ("dir", "in")])
        ),
        2,
    );
    assert_eq!(
        ours.value(
            "counter",
            Key::from_parts("dynamic.labels", &[("peer", "peer-1")])
        ),
        3,
    );
    assert_eq!(
        f64::from_bits(ours.value("gauge", Key::from_parts("gauge", &[("peer", "peer-1")]))),
        5.0,
    );
}
