//! Worker slots stay reserved until the last job, encoded result or write ends.
//! Hold each real operation open, cancel its caller, then check that replacement
//! work waits and eventually resumes after the held operation is released.

use super::super::super::super::tests::fake_blocks_in_range;
use super::*;

#[tokio::test]
async fn c01_actual_operations_stay_bounded_with_more_commitments_than_workers() {
    let source = ControlledSource::new(100, 3, false, true);
    let _release = source.probe.release_on_drop();
    let config = config(2);
    let regulator = GetBlocksServingRegulator::new(config.clone());
    let registry = Arc::new(PeerRegistry::new());
    let mut peers = Vec::new();
    for peer in 1..=8 {
        let f = source.shared_fixture(1, peer, config.clone(), regulator.clone(), registry.clone());
        request(&f, 100, 3).await;
        peers.push(f);
    }
    source.probe.wait_started(2).await;
    assert_eq!(source.probe.snapshot().running, 2);
    assert_eq!(regulator.snapshot().node_active, 2);
    assert!(peers
        .iter()
        .all(|f| !f.session.cancel_token().is_cancelled()));
    source.probe.release();
    futures::future::join_all(
        peers
            .iter_mut()
            .map(|f| response(f, &source, 100, 3, config.max_response_bytes, 128)),
    )
    .await;
    source.probe.wait_finished(8).await;
    assert_eq!(source.probe.snapshot().started, 8);
    assert!(source.probe.snapshot().peak_running <= 2);
    for f in peers {
        f.finish().await;
    }
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[tokio::test]
async fn c02_retained_output_stops_read_ahead_and_resumes_without_refill() {
    let source = ControlledSource::new(100, 6, false, false);
    let mut f = source.fixture(1, 1, 1);
    request(&f, 100, 3).await;
    source.probe.wait_finished(1).await;
    let held = time::timeout(DEADLINE, f.data.recv())
        .await
        .unwrap()
        .unwrap();
    request(&f, 103, 3).await;
    // One pending decoded request plus the one-slot input queue is the declared
    // request read-ahead. Extra queued bytes must eventually backpressure.
    assert_eq!(source.probe.snapshot().started, 1);
    let mut write = Box::pin(held.write_with(|frame| async move {
        assert_eq!(
            BlockSyncMessage::decode_frame(frame)
                .unwrap()
                .message_type(),
            3
        );
        std::future::pending::<Result<(), ()>>().await
    }));
    assert!(futures::poll!(&mut write).is_pending());
    assert_eq!(source.probe.snapshot().started, 1);
    // Finish the current response's other frames before releasing the first
    // write. The next database job must still wait for that actual owner.
    for _ in 0..3 {
        f.next().await;
    }
    assert_eq!(source.probe.snapshot().started, 1);
    tokio::task::yield_now().await;
    request(&f, 106, 3).await;
    assert!(
        time::timeout(Duration::from_millis(20), request(&f, 109, 3))
            .await
            .is_err()
    );
    drop(write);
    source.probe.wait_started(2).await;
    response(&mut f, &source, 103, 3, config(1).max_response_bytes, 128).await;
    f.finish().await;
}

async fn check_reconnects(same_identity: bool) {
    let source = ControlledSource::new(100, 3, false, true);
    let _release = source.probe.release_on_drop();
    let config = config(2);
    let regulator = GetBlocksServingRegulator::new(config.clone());
    let registry = Arc::new(PeerRegistry::new());
    for attempt in 0..8u8 {
        let peer = if same_identity { 1 } else { attempt + 1 };
        let mut f =
            source.shared_fixture(1, peer, config.clone(), regulator.clone(), registry.clone());
        request(&f, 100, 3).await;
        if attempt < if same_identity { 1 } else { 2 } {
            source.probe.wait_started(usize::from(attempt) + 1).await;
        }
        tokio::task::yield_now().await;
        f.task.abort();
        assert!(time::timeout(DEADLINE, &mut f.task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
        registry.remove_session(f.session.peer_id(), f.session.session_id());
        drop(f);
        assert!(source.probe.snapshot().running <= 2);
        assert!(regulator.snapshot().node_active <= 2);
    }
    assert_eq!(
        source.probe.snapshot().started,
        if same_identity { 1 } else { 2 }
    );
    source.probe.release();
    let started = source.probe.snapshot().started;
    source.probe.wait_finished(started).await;
    let mut replacement = source.shared_fixture(1, 1, config.clone(), regulator.clone(), registry);
    request(&replacement, 100, 3).await;
    response(
        &mut replacement,
        &source,
        100,
        3,
        config.max_response_bytes,
        128,
    )
    .await;
    replacement.finish().await;
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(source.probe.snapshot().running, 0);
}

#[tokio::test]
async fn c03_same_identity_reconnects_cannot_start_another_blocked_job() {
    check_reconnects(true).await;
}

#[tokio::test]
async fn c03_distinct_identity_churn_cannot_exceed_the_node_job_limit() {
    check_reconnects(false).await;
}

async fn check_encode_cancellation(hold_finished_result: bool) {
    let config = config(1);
    let regulator = GetBlocksServingRegulator::new(config);
    let session = regulator.session(ZakuraPeerId::new(vec![1; 32]).unwrap());
    let request = session
        .decode_request(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(100),
                count: 1,
            }
            .encode_frame()
            .unwrap(),
        )
        .unwrap();
    let mut permit = session.admit_request(&request).await;
    let probe = ExecutionProbe::new(!hold_finished_result, hold_finished_result);
    let _release = probe.release_on_drop();
    permit.encode_probe = Some(probe.clone());
    let (send, recv) = worker_framed_channel(1);
    let block = fake_blocks_in_range(100, 100).pop().unwrap();
    let mut task = AbortOnDropHandle::new(tokio::spawn(async move {
        send_response(&send, &mut permit, BlockSyncMessage::Block(block)).await
    }));
    probe.wait_started(1).await;
    if hold_finished_result {
        await_until("encoder reached its retained result", DEADLINE, || {
            probe.snapshot().largest_allocation > 0
        })
        .await
        .unwrap();
    }
    task.abort();
    assert!(time::timeout(DEADLINE, &mut task)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    drop(recv);
    assert_eq!(probe.snapshot().running, 1);
    assert_eq!(
        regulator.snapshot().node_active,
        1,
        "C04 actual encode/result owner survives caller abort"
    );
    let mut next = Box::pin(session.admit_request(&request));
    assert!(futures::poll!(&mut next).is_pending());
    probe.release();
    drop(time::timeout(DEADLINE, next).await.unwrap());
    probe.wait_finished(1).await;
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[tokio::test]
async fn c04_aborted_caller_keeps_running_encoding_charged() {
    check_encode_cancellation(false).await;
}

#[tokio::test]
async fn c04_unobserved_encode_result_keeps_its_lease_until_disposal() {
    check_encode_cancellation(true).await;
}

#[tokio::test]
async fn c05_real_decode_and_encode_allocations_are_bounded_for_small_and_large_blocks() {
    for large in [false, true] {
        let source = ControlledSource::new(100, 3, large, false);
        let mut f = source.fixture(1, 1, 1);
        // This fixture declares a two-body response budget. A third decoded
        // lookahead is permitted, but no second response can run for this peer.
        let body_bytes = source.encoded.values().map(Vec::len).max().unwrap();
        let cap = u32::try_from(body_bytes * 2).unwrap();
        f.status
            .send_modify(|status| status.max_response_bytes = cap);
        request(&f, 100, 3).await;
        source.probe.wait_finished(1).await;
        assert!(
            source.live_decoded_bytes() > 0,
            "C05 observes independently decoded retained objects"
        );
        let held = time::timeout(DEADLINE, f.data.recv())
            .await
            .unwrap()
            .unwrap();
        let (frame, guard) = held.into_parts();
        assert!(
            frame.payload.capacity() <= 2 * (body_bytes + 1),
            "C05 Vec encoding growth remains bounded"
        );
        let decoded_sizes: Vec<_> = source
            .encoded
            .values()
            .map(|bytes| {
                block::Block::zcash_deserialize(bytes.as_slice())
                    .unwrap()
                    .attributed_memory_size_bytes()
            })
            .collect();
        let maximum_decoded = *decoded_sizes.iter().max().unwrap();
        let measured = source.probe.snapshot();
        assert!(measured.largest_allocation > 0);
        // Each V1 fixture owns distinct transaction allocations. Allow their Arc
        // controls and one bounded lookahead, independently of wire accounting.
        let allocation_envelope = maximum_decoded * 4 + 64 * 1024;
        assert!(
            u64::try_from(measured.peak_operation_bytes).unwrap() <= allocation_envelope,
            "C05 actual storage allocation peak: {measured:?}, envelope={allocation_envelope}"
        );
        assert_eq!(f.regulator.snapshot().node_active, 1);
        f.session.cancel_token().cancel();
        let regulator = f.regulator.clone();
        f.finish().await;
        assert_eq!(
            regulator.snapshot().node_active,
            1,
            "retained output owns capacity"
        );
        drop((frame, guard));
        let replacement = regulator.session(ZakuraPeerId::new(vec![1; 32]).unwrap());
        let request = replacement
            .decode_request(
                BlockSyncMessage::GetBlocks {
                    start_height: block::Height(100),
                    count: 1,
                }
                .encode_frame()
                .unwrap(),
            )
            .unwrap();
        drop(
            time::timeout(DEADLINE, replacement.admit_request(&request))
                .await
                .unwrap(),
        );
        assert_eq!(regulator.snapshot().node_active, 0);
        assert_eq!(source.live_decoded_bytes(), 0);
    }
}

#[tokio::test]
async fn c05_actual_large_encoder_peak_and_retained_frame_follow_the_declared_bound() {
    let source = ControlledSource::new(100, 1, true, false);
    let encoded = &source.encoded[&block::Height(100)];
    let body = Arc::new(block::Block::zcash_deserialize(encoded.as_slice()).unwrap());
    let regulator = GetBlocksServingRegulator::new(config(1));
    let session = regulator.session(ZakuraPeerId::new(vec![1; 32]).unwrap());
    let request = session
        .decode_request(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(100),
                count: 1,
            }
            .encode_frame()
            .unwrap(),
        )
        .unwrap();
    let mut permit = session.admit_request(&request).await;
    let probe = ExecutionProbe::new(false, false);
    permit.encode_probe = Some(probe.clone());
    let (send, mut recv) = worker_framed_channel(1);
    send_response(&send, &mut permit, BlockSyncMessage::Block(body))
        .await
        .unwrap();
    probe.wait_finished(1).await;
    let measured = probe.snapshot();
    let payload_bound = 2 * (encoded.len() + 1);
    assert!(
        measured.peak_operation_bytes <= payload_bound + 4096,
        "C05 actual encoder peak {measured:?}, payload bound={payload_bound}"
    );
    assert!(measured.largest_allocation >= encoded.len());
    drop(permit);
    let held = recv.recv().await.unwrap();
    let (frame, guard) = held.into_parts();
    assert!(frame.payload.capacity() <= payload_bound);
    assert_eq!(regulator.snapshot().node_active, 1);
    drop((frame, guard));
    assert_eq!(regulator.snapshot().node_active, 0);
}
