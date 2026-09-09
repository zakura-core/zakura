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
fn reconnects_share_the_peer_limit_until_old_reads_finish() {
    let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
    let original = regulator.session(peer(8));
    let permit = original.admit_now(1).unwrap().commit();
    let query = permit.work_lease();
    assert!(query.try_start());
    drop(permit);
    drop(original);

    // Neither dropping the old session nor replacing it repeatedly releases
    // the work still owned by its running storage read.
    for _ in 2..=65 {
        let replacement = regulator.session(peer(8));
        assert!(replacement.admit_now(1).is_none());
        assert_eq!(regulator.snapshot().node_active, 1);
        let other = regulator.session(peer(9));
        assert!(other.admit_now(1).is_some(), "another peer can still serve");
    }
    let replacement = regulator.session(peer(8));
    drop(query);
    assert!(
        replacement.admit_now(1).is_some(),
        "completion frees this peer's slot"
    );
    assert_eq!(regulator.snapshot().node_active, 0);
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
    let result = query.clone();

    drop(permit);
    assert!(query.is_cancelled());
    assert_eq!(regulator.snapshot().node_active, 1);

    drop(query);

    drop(result);
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[test]
fn closed_producer_prevents_queued_query_execution() {
    let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
    let session = regulator.session(peer(9));
    let permit = session
        .admit_now(1)
        .expect("the initial request fits")
        .commit();
    let query = permit.work_lease();
    drop(permit);
    assert!(!query.try_start());
    drop(query);
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[test]
fn separately_issued_query_leases_share_one_execution_claim() {
    let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
    let session = regulator.session(peer(9));
    let permit = session.admit_now(1).unwrap().commit();
    let first = permit.work_lease();
    let second = permit.work_lease();
    assert!(first.try_start());
    assert!(!second.try_start());
    drop(permit);
    assert!(first.is_cancelled());
    assert!(second.is_cancelled());
    assert_eq!(regulator.snapshot().node_active, 1);
    drop((first, second));
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
async fn provisional_admission_rolls_back_every_earlier_reservation() {
    let config = ZakuraBlockSyncConfig::default();
    let regulator = GetBlocksServingRegulator::new(config);
    let session = regulator.session(peer(1));
    let other_peer = regulator.session(peer(7));
    let first = session.admit_now(1).expect("the first request fits");
    let before = regulator.snapshot();
    assert!(
        session.admit_now(1).is_none(),
        "the peer producer is occupied by the first request"
    );
    assert_eq!(regulator.snapshot(), before);

    let independent = other_peer
        .admit_now(1)
        .expect("one peer's producer does not consume another peer's capacity");
    assert_eq!(regulator.snapshot().node_active, 2);
    drop(independent);
    drop(first);
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

#[test]
fn frames_keep_the_producer_until_the_last_write_finishes() {
    let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
    let session = regulator.session(peer(3));
    let other = regulator.session(peer(4));
    let mut permit = session.admit_now(1).unwrap().commit();
    let block = permit.frame_guard(100);
    let terminal = permit.frame_guard(9);
    drop(permit);
    assert!(session.admit_now(1).is_none());
    assert!(other.admit_now(1).is_some());
    drop(block);
    assert!(session.admit_now(1).is_none());
    drop(terminal);
    assert_eq!(regulator.snapshot().node_active, 0);
    assert!(session.admit_now(1).is_some());
}
