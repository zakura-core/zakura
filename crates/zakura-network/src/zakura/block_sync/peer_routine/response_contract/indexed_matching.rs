//! Exercise the index through real request publication and response decoding.

use super::*;

async fn issue_ranges(write: bool) -> Fixture {
    let mut f = Fixture::new(100, 3);
    f.routine.max_blocks_per_response = 1;
    f.routine.try_fill().await;
    assert_eq!(f.routine.window.outstanding.len(), 3);
    for body in &f.blocks {
        assert!(
            f.routine.response_index(body.hash()).is_ok(),
            "publication installs the key before a write can start"
        );
    }
    if write {
        for _ in 0..3 {
            let queued = time::timeout(DEADLINE, f.outbound.recv())
                .await
                .unwrap()
                .unwrap();
            queued
                .write_with(|frame| async move {
                    assert!(matches!(
                        BlockSyncMessage::decode_frame(frame).unwrap(),
                        BlockSyncMessage::GetBlocks { count: 1, .. }
                    ));
                    Ok::<_, std::convert::Infallible>(())
                })
                .await
                .unwrap();
        }
    }
    f.clear_events();
    f
}

#[tokio::test]
async fn indexed_ranges_survive_completion_order_and_local_detachment() {
    for detached in [false, true] {
        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
            let mut f = issue_ranges(true).await;
            if detached {
                f.routine.return_unreceived_requests("test_local_deadline");
                assert_eq!(f.routine.window.outstanding.len(), 3);
            }
            for index in order {
                f.body(index).await;
                let start_height = f.blocks[index].coinbase_height().unwrap();
                f.deliver(BlockSyncMessage::BlocksDone {
                    start_height,
                    returned: 1,
                })
                .await
                .unwrap();
                assert!(f.routine.response_index(f.blocks[index].hash()).is_err());
                assert_eq!(
                    f.routine.window.outstanding_index_for_start(start_height),
                    None
                );
            }
            assert!(f.routine.window.outstanding.is_empty());
            f.assert_no_peer_fault();
        }
    }
}

#[tokio::test]
async fn ambiguous_indexed_hash_is_rejected_before_body_decoding() {
    let mut f = issue_ranges(true).await;
    // Simulate two ranges claiming the same next hash. Neither may win just
    // because it appears first in the request vector or the index.
    let mut other = f.routine.window.remove_outstanding(1);
    other.request.expected_blocks[0].hash = f.blocks[0].hash();
    f.routine.window.push_outstanding(other);
    let probe = zakura_test::execution::ExecutionProbe::new(false, false);
    f.routine.decode_probe = Some(probe.clone());
    let result = f
        .deliver(BlockSyncMessage::Block(f.blocks[0].clone()))
        .await;
    assert!(
        matches!(result, Err(SinkReject::Protocol(ref error)) if error.to_string().contains("ambiguous"))
    );
    assert!(f.bodies.is_empty());
    assert_eq!(probe.snapshot().largest_allocation, 0);
    for range in &f.routine.window.outstanding {
        assert_eq!(range.response.consumed_objects(), 0);
    }
}

#[tokio::test]
async fn skipped_writes_remove_every_response_key() {
    let mut f = issue_ranges(false).await;
    f.routine.return_unreceived_requests("test_before_write");
    assert!(f.routine.window.outstanding.is_empty());
    for body in &f.blocks {
        assert!(f.routine.response_index(body.hash()).is_err());
        assert_eq!(
            f.routine
                .window
                .outstanding_index_for_start(body.coinbase_height().unwrap()),
            None
        );
    }
}
