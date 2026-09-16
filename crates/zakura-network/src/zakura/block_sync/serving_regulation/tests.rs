use super::super::wire::MAX_BS_BLOCKS_PER_REQUEST;
use super::*;
use futures::FutureExt;

impl GetBlocksServingSession {
    // Poll the real admission future once. A pending attempt is cancelled, so
    // lifecycle histories can observe immediate admission without advancing time.
    pub(in crate::zakura::block_sync) fn admit_now(&self, count: u32) -> Option<AdmissionAttempt> {
        let request = GetBlocksRequest {
            start_height: block::Height(0),
            count,
        };
        self.work
            .admit(&request)
            .now_or_never()
            .map(|work| AdmissionAttempt {
                work,
                metrics: self.metrics.clone(),
            })
    }
}

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
}

#[test]
fn query_and_result_keep_capacity_after_the_producer_closes() {
    let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
    let session = regulator.session(peer(8));
    let permit = session
        .admit_now(1)
        .expect("the initial request fits")
        .commit();
    let query = permit.work_lease();
    assert!(query.try_start());
    assert!(
        !query.clone().try_start(),
        "cloning a query never authorizes another read"
    );
    let second = permit.work_lease();
    assert!(
        !second.try_start(),
        "separately issued leases share the execution claim"
    );
    let result = query.clone();

    drop(permit);
    assert!(query.is_cancelled());
    assert_eq!(regulator.snapshot().node_active, 1);

    drop(query);
    drop(second);
    assert_eq!(regulator.snapshot().node_active, 1);
    drop(result);
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[test]
fn concurrent_claims_and_cancellation_preserve_one_charged_owner() {
    use std::{sync::Barrier, thread};

    // Exercise overlapping calls; deterministic tests above require both
    // ordered outcomes. This does not claim exhaustive schedule coverage.
    for _ in 0..64 {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(peer(9));
        let permit = session.admit_now(1).unwrap().commit();
        let lease = permit.work_lease();

        let barrier = Barrier::new(4);

        thread::scope(|scope| {
            let first = scope.spawn(|| {
                barrier.wait();
                lease.try_start()
            });
            let second = scope.spawn(|| {
                barrier.wait();
                lease.try_start()
            });
            let cancellation = scope.spawn(|| {
                barrier.wait();
                drop(permit);
            });
            barrier.wait();
            let claims = usize::from(first.join().unwrap()) + usize::from(second.join().unwrap());
            cancellation.join().unwrap();
            assert!(claims <= 1);
        });

        assert!(lease.is_cancelled());
        assert!(
            !lease.try_start(),
            "producer closure permanently prevents new claims"
        );
        assert_eq!(regulator.snapshot().node_active, 1);

        drop(lease);
        assert_eq!(regulator.snapshot().node_active, 0);
    }
}

#[tokio::test]
async fn admission_waits_for_and_commits_the_released_node_slot() {
    let mut config = ZakuraBlockSyncConfig::default();
    config.get_blocks_regulation.node_active_requests = 1;
    let regulator = GetBlocksServingRegulator::new(config);
    let session = regulator.session(peer(10));
    let request = GetBlocksRequest {
        start_height: block::Height(0),
        count: 1,
    };
    let owner = session.admit_request(&request).await;
    let waiting_session = regulator.session(peer(11));
    let mut wait = Box::pin(waiting_session.admit_request(&request));
    assert!(futures::poll!(&mut wait).is_pending());
    assert_eq!(regulator.snapshot().peer_active, 2);
    drop(owner);
    let admitted = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("released capacity reaches its waiter");
    assert_eq!(regulator.snapshot().node_active, 1);
    assert_eq!(regulator.snapshot().peer_active, 1);
    drop(admitted);
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(regulator.snapshot().peer_active, 0);
}

#[test]
fn cost_includes_block_discriminators_and_terminal() {
    let mut config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 2,
        max_response_bytes: u32::try_from(block::MAX_BLOCK_BYTES * 2)
            .expect("two maximum block bodies fit u32"),
        ..ZakuraBlockSyncConfig::default()
    };

    let cost = GetBlocksPolicy::new(&config)
        .response_cap_for_count(2)
        .expect("the default bounds do not overflow");
    assert_eq!(
        cost,
        block::MAX_BLOCK_BYTES * 2 + 2 + GET_BLOCKS_TERMINAL_PAYLOAD_BYTES
    );

    config.max_blocks_per_response = 3;
    config.max_response_bytes = u32::try_from(block::MAX_BLOCK_BYTES).unwrap();
    let byte_limited = GetBlocksPolicy::new(&config)
        .response_cap_for_count(MAX_BS_BLOCKS_PER_REQUEST)
        .expect("the byte-limited cost is representable");
    assert_eq!(
        byte_limited,
        GET_BLOCKS_TERMINAL_PAYLOAD_BYTES + 3 + block::MAX_BLOCK_BYTES,
        "the body-byte cap is separate from discriminators and the terminal frame",
    );
}

#[test]
fn config_rejects_nonprogressing_or_unbounded_admission_settings() {
    let base = ZakuraBlockSyncConfig::default();

    let mut no_active_slots = base.clone();
    no_active_slots.get_blocks_regulation.node_active_requests = 0;
    assert_eq!(
        validate_config(&no_active_slots),
        Err("get_blocks_regulation.node_active_requests must be greater than zero"),
    );
}

#[tokio::test(start_paused = true)]
async fn completed_requests_release_capacity_without_waiting_for_time() {
    let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
    let session = regulator.session(peer(2));
    let now = time::Instant::now();
    for _ in 0..4096 {
        let mut permit = session
            .admit_now(1)
            .expect("released capacity admits work")
            .commit();
        let frame = permit.frame_guard(GET_BLOCKS_TERMINAL_PAYLOAD_BYTES);
        drop(permit);
        assert_eq!(regulator.snapshot().node_active, 1);
        drop(frame);

        assert_eq!(regulator.snapshot().node_active, 0);
    }
    assert_eq!(
        time::Instant::now(),
        now,
        "admission has no bandwidth refill timer"
    );
}

const RESPONSE_BYTES: u64 = 2_000_010;

fn queued_response(session: &GetBlocksServingSession) -> FrameGuard {
    let mut permit = session.admit_now(1).unwrap().commit();
    permit.frame_guard(RESPONSE_BYTES)
}

#[test]
fn default_response_count_caps_large_requests_at_one_block() {
    let config = ZakuraBlockSyncConfig::default();
    for count in [1, 128, u32::MAX] {
        let cap = GetBlocksPolicy::new(&config)
            .response_cap_for_count(count)
            .unwrap();
        assert_eq!(config.initial_status().max_blocks_per_response, 1);
        assert_eq!(cap, RESPONSE_BYTES);
    }
}

#[tokio::test(start_paused = true)]
async fn default_producer_limits_hold_until_writes_finish() {
    let config = ZakuraBlockSyncConfig::default();
    validate_config(&config).unwrap();
    let regulator = GetBlocksServingRegulator::new(config);
    let peers: Vec<_> = (0..65).map(|id| regulator.session(peer(id))).collect();
    let mut frames = Vec::new();
    for peer in &peers[..64] {
        frames.push(queued_response(peer));
    }
    tokio::time::advance(Duration::from_secs(60)).await;
    let before = regulator.snapshot();
    assert_eq!(before.node_active, 64);
    assert!(peers[0].admit_now(1).is_none());
    assert!(peers[64].admit_now(1).is_none());
    assert_eq!(regulator.snapshot(), before);
    frames.pop();
    frames.push(queued_response(&peers[64]));
    assert_eq!(regulator.snapshot().node_active, 64);
    drop(frames);
    assert_eq!(regulator.snapshot().node_active, 0);
}
