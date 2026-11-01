//! Admission contracts, cancellation ownership, and event metrics.

use super::*;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use std::sync::atomic::{AtomicU64, Ordering};
use tower::service_fn;

fn block(byte: u8) -> Deferred {
    Deferred {
        hash: block::Hash([byte; 32]),
        parent: block::Hash([0xee; 32]),
        height: Height(3_600_000),
        source: None,
        parent_recovery: false,
    }
}

fn state(
    result: zs::ParentInputs,
) -> impl Service<zs::Request, Response = zs::Response, Error = BoxError> + Clone {
    service_fn(move |request| async move {
        assert!(
            matches!(request, zs::Request::CheckParentInputs { ref outpoints, .. } if outpoints.is_empty())
        );
        Ok(zs::Response::ParentInputs(result))
    })
}

#[tokio::test]
async fn reservation_keeps_connected_admission_available() {
    for (capacity, limit) in [(1, 0), (2, 1), (8, 6), (20, 15), (60, 45)] {
        let budget = Budget::new("sync", capacity);
        let mut attempts = Vec::new();
        for byte in 0..limit {
            // Test capacities are at most 60, so this fits in a byte.
            let entry = block(u8::try_from(byte).unwrap());
            let mut attempt = budget.attempt(entry.hash);
            attempt
                .admit(state(zs::ParentInputs::ParentUnavailable), entry)
                .await
                .unwrap();
            attempts.push(attempt);
        }
        let entry = block(100);
        let mut refused = budget.attempt(entry.hash);
        assert!(refused
            .admit(state(zs::ParentInputs::ParentUnavailable), entry)
            .await
            .is_err());
        let entry = block(101);
        let mut connected = budget.attempt(entry.hash);
        connected
            .admit(state(zs::ParentInputs::Available), entry)
            .await
            .unwrap();
        assert_eq!(budget.lock().charges.len(), limit);
        drop(attempts);
        assert!(budget.lock().charges.is_empty());
    }
}

#[tokio::test]
async fn paths_are_independent_and_capacity_reductions_do_not_cancel() {
    let sync = Budget::new("sync", 20);
    let gossip = Budget::new("gossip", 20);
    let mut attempts = Vec::new();
    for budget in [&sync, &gossip] {
        for byte in 0..15 {
            let mut attempt = budget.attempt(block(byte).hash);
            attempt
                .admit(state(zs::ParentInputs::Inconclusive), block(byte))
                .await
                .unwrap();
            attempts.push(attempt);
        }
    }
    sync.set_capacity(8);
    assert_eq!(sync.lock().charges.len(), 15);
    assert_eq!(gossip.lock().charges.len(), 15);
    let mut refused = sync.attempt(block(30).hash);
    assert!(refused
        .admit(state(zs::ParentInputs::ParentUnavailable), block(30))
        .await
        .is_err());
    drop(attempts);
    assert!(sync.lock().charges.is_empty());
    assert!(gossip.lock().charges.is_empty());
}

#[tokio::test]
async fn parent_progress_releases_once_without_releasing_another_attempt() {
    let budget = Budget::new("sync", 2);
    let mut first = budget.attempt(block(1).hash);
    first
        .admit(state(zs::ParentInputs::ParentUnavailable), block(1))
        .await
        .unwrap();
    budget.parent_ready(block(1).parent);
    assert!(budget.lock().charges.is_empty());
    let mut replacement = budget.attempt(block(1).hash);
    replacement
        .admit(state(zs::ParentInputs::ParentUnavailable), block(1))
        .await
        .unwrap();
    drop(first);
    assert_eq!(budget.lock().charges.len(), 1);
    drop(replacement);
    assert!(budget.lock().charges.is_empty());
}

#[tokio::test(start_paused = true)]
async fn deferred_hash_waits_for_parent_and_keeps_its_original_deadline() {
    let budget = Budget::new("sync", 1);
    let entry = block(1);
    let mut first = budget.attempt(entry.hash);
    assert!(first
        .admit(state(zs::ParentInputs::ParentUnavailable), entry.clone())
        .await
        .is_err());
    drop(first);
    let original = budget.lock().deferred[&entry.hash].first;
    assert!(budget.take_ready(1).is_empty());
    tokio::time::advance(Duration::from_secs(100)).await;
    budget.parent_ready(entry.parent);
    assert_eq!(budget.take_ready(1).len(), 1);
    let mut retry = budget.attempt(entry.hash);
    assert!(retry
        .admit(state(zs::ParentInputs::ParentUnavailable), entry.clone())
        .await
        .is_err());
    drop(retry);
    assert_eq!(budget.lock().deferred[&entry.hash].first, original);
    tokio::time::advance(DEFERRED_LIFETIME).await;
    assert!(budget.take_ready(1).is_empty());
    assert!(!budget.is_deferred(&entry.hash));
    assert!(budget.take_rediscovery());
}

#[tokio::test]
async fn metadata_overflow_and_retry_limit_are_bounded() {
    let budget = Budget::new("sync", 1);
    for byte in 1..=2 {
        let mut attempt = budget.attempt(block(byte).hash);
        assert!(attempt
            .admit(state(zs::ParentInputs::ParentUnavailable), block(byte))
            .await
            .is_err());
    }
    assert_eq!(budget.lock().deferred.len(), 1);
    for _ in 0..RETRY_LIMIT {
        budget.parent_ready(block(1).parent);
        assert_eq!(budget.take_ready(1).len(), 1);
        let mut attempt = budget.attempt(block(1).hash);
        assert!(attempt
            .admit(state(zs::ParentInputs::ParentUnavailable), block(1))
            .await
            .is_err());
    }
    assert!(!budget.is_deferred(&block(1).hash));
}

#[derive(Default)]
struct Counters(Mutex<HashMap<String, Arc<AtomicU64>>>);

impl Recorder for Counters {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(
            self.0
                .lock()
                .unwrap()
                .entry(key.name().to_owned())
                .or_default()
                .clone(),
        )
    }
    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }
    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

#[test]
fn event_metrics_capture_saturation_without_a_rejected_request() {
    let recorder = Counters::default();
    metrics::with_local_recorder(&recorder, || {
        let budget = Budget::new("sync", 20);
        let mut inner = budget.lock();
        for id in 0..15 {
            inner.charges.insert(
                id,
                Charge {
                    parent: block(1).parent,
                    since: Instant::now(),
                },
            );
            budget.publish(&mut inner, false);
        }
        inner.charges.clear();
        budget.publish(&mut inner, false);
    });
    let counters = recorder.0.lock().unwrap();
    for metric in ["near_80.count", "near_90.count", "limit_reached.count"] {
        assert_eq!(
            counters[&format!("sync.missing_parent.{metric}")].load(Ordering::Relaxed),
            1
        );
    }
    assert_eq!(
        counters["sync.missing_parent.capacity_deferred.count"].load(Ordering::Relaxed),
        0
    );
}
