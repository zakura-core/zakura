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
        routing_source: None,
        parent_recovery: false,
    }
}

fn state(
    result: zs::ParentInputs,
) -> impl Service<zs::Request, Response = zs::Response, Error = BoxError, Future: Send> + Clone {
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

#[tokio::test(start_paused = true)]
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
        tokio::time::advance(CHECK_INTERVAL).await;
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

#[tokio::test(start_paused = true)]
async fn refused_retry_keeps_metadata_eligible_after_old_task_cleanup() {
    let budget = Budget::new("sync", 1);
    let mut first = budget.attempt(block(1).hash);
    first
        .admit(state(zs::ParentInputs::ParentUnavailable), block(1))
        .await
        .unwrap_err();
    budget.parent_ready(block(1).parent);
    assert_eq!(budget.take_ready(1).len(), 1);
    budget.dispatch_refused(&block(1).hash);
    drop(first);
    assert!(budget.take_ready(1).is_empty());
    tokio::time::advance(CHECK_INTERVAL).await;
    assert_eq!(budget.take_ready(1).len(), 1);
    let mut retry = budget.attempt(block(1).hash);
    retry
        .admit(state(zs::ParentInputs::Available), block(1))
        .await
        .unwrap();
    drop(retry);
    assert!(!budget.has_pending());
    assert!(budget.lock().deferred.is_empty());
}

#[tokio::test(start_paused = true)]
async fn reset_during_a_state_read_cannot_restore_old_round_metadata() {
    let budget = Budget::new("sync", 1);
    let mut old = budget.attempt(block(1).hash);
    let (started, observed) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(Some(started)));
    let state = service_fn(move |_| {
        started.lock().unwrap().take().unwrap().send(()).unwrap();
        futures::future::pending::<Result<zs::Response, BoxError>>()
    });
    let admission = tokio::spawn(async move { old.admit(state, block(1)).await });
    observed.await.unwrap();
    budget.reset();
    assert!(admission.await.unwrap().is_err());
    assert!(budget.lock().deferred.is_empty());
    assert!(budget.lock().charges.is_empty());
}

#[tokio::test(start_paused = true)]
async fn observer_bounds_parent_checks_and_releases_running_attempts() {
    use zakura_chain::chain_tip::mock::MockChainTip;
    let budget = Budget::new("sync", 20);
    let mut attempts = Vec::new();
    for byte in 1..=5 {
        let mut entry = block(byte);
        entry.parent = block::Hash([byte; 32]);
        let mut attempt = budget.attempt(entry.hash);
        attempt
            .admit(state(zs::ParentInputs::ParentUnavailable), entry)
            .await
            .unwrap();
        attempts.push(attempt);
    }
    let calls = Arc::new(AtomicU64::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let checked = calls.clone();
    let checks = gate.clone();
    let service = service_fn(move |_| {
        let gate = checks.clone();
        checked.fetch_add(1, Ordering::Relaxed);
        async move {
            let _permit = gate.acquire().await.unwrap();
            Ok::<_, BoxError>(zs::Response::ParentInputs(zs::ParentInputs::Available))
        }
    });
    let (tip, _sender) = MockChainTip::new();
    let observed = budget.clone();
    let observer = tokio::spawn(async move { observed.observe(service, tip).await });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(calls.load(Ordering::Relaxed), 4);
    gate.add_permits(4);
    observer.await.unwrap();
    assert_eq!(budget.lock().charges.len(), 1);
    let (tip, _sender) = MockChainTip::new();
    budget
        .observe(state(zs::ParentInputs::Available), tip)
        .await;
    assert!(budget.lock().charges.is_empty());
    drop(attempts);
    assert!(budget.lock().charges.is_empty());
}

#[tokio::test(start_paused = true)]
async fn observer_does_not_spin_when_tip_notifications_close() {
    let budget = Budget::new("sync", 20);
    let (tip, sender) = zakura_chain::chain_tip::mock::MockChainTip::new();
    drop(sender);
    assert!(timeout(
        Duration::from_secs(1),
        budget.observe(state(zs::ParentInputs::Available), tip)
    )
    .await
    .is_err());
}

#[tokio::test(start_paused = true)]
async fn failed_state_read_does_not_authorize_a_verification_wait() {
    let budget = Budget::new("sync", 20);
    let mut attempt = budget.attempt(block(1).hash);
    let unavailable = service_fn(|_| futures::future::pending::<Result<zs::Response, BoxError>>());
    assert!(attempt.admit(unavailable, block(1)).await.is_err());
    assert!(budget.lock().charges.is_empty());
    assert!(budget.is_deferred(&block(1).hash));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_waits_allow_a_real_connected_state_commit() {
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use zakura_chain::{
        block::{arbitrary::allow_all_transparent_coinbase_spends, Block, LedgerState},
        parameters::Network,
    };
    let _init_guard = zakura_test::init();
    let network = Network::new_regtest(Default::default());
    let strategy = LedgerState::genesis_strategy(Some(network.clone()), None, Some(4), true)
        .prop_flat_map(|ledger| {
            Block::partial_chain_strategy(ledger, 2, allow_all_transparent_coinbase_spends, true)
        });
    let blocks = strategy
        .new_tree(&mut TestRunner::deterministic())
        .unwrap()
        .current()
        .0;
    let genesis = blocks[0].clone();
    // The chain generator can include shielded spends whose anchors are not in
    // this tiny state. Use a transparent coinbase-only child for the commit.
    let mut child = (*blocks[1]).clone();
    let coinbase = &child.transactions[0];
    child.transactions = vec![Arc::new(zakura_chain::transaction::Transaction::V4 {
        inputs: coinbase.inputs().to_vec(),
        outputs: coinbase.outputs().to_vec(),
        lock_time: zakura_chain::transaction::LockTime::unlocked(),
        expiry_height: Height(0),
        joinsplit_data: None,
        sapling_shielded_data: None,
    })];
    Arc::make_mut(&mut child.header).merkle_root =
        child.transactions.iter().map(|tx| tx.hash()).collect();
    let child = Arc::new(child);
    let (service, _, _, _) = zs::init(zs::Config::ephemeral(), &network, Height(0), 0)
        .await
        .unwrap();
    let state = tower::buffer::Buffer::new(tower::util::BoxService::new(service), 1);
    state
        .clone()
        .oneshot(zs::Request::CommitCheckpointVerifiedBlock(genesis.into()))
        .await
        .unwrap();
    let budget = Budget::new("sync", 20);
    let mut waiting = Vec::new();
    for byte in 0..15 {
        let mut attempt = budget.attempt(block(byte).hash);
        attempt.admit(state.clone(), block(byte)).await.unwrap();
        waiting.push(attempt);
    }
    let mut connected = budget.attempt(child.hash());
    connected
        .admit(
            state.clone(),
            Deferred {
                hash: child.hash(),
                parent: child.header.previous_block_hash,
                height: Height(1),
                routing_source: None,
                parent_recovery: false,
            },
        )
        .await
        .unwrap();
    let committed = timeout(
        Duration::from_secs(10),
        state
            .clone()
            .oneshot(zs::Request::CommitSemanticallyVerifiedBlock(
                child.clone().into(),
            )),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(committed, zs::Response::Committed(hash) if hash == child.hash()));
    assert_eq!(budget.lock().charges.len(), 15);
    drop(waiting);
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
