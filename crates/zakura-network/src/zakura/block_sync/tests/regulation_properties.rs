//! Real reactor witnesses for the ownership histories in serving_regulation.

use futures::FutureExt;
use proptest::prelude::*;
use tokio::sync::oneshot;

use super::*;
use crate::zakura::transport::worker_framed_channel;

/// A control-channel acknowledgement orders observations after response handling.
async fn reactor_barrier(handle: &BlockSyncHandle) {
    let (sender, _receiver) = framed_channel(1);
    let session = BlockSyncPeerSession::for_test(peer(0xfd), sender, CancellationToken::new());
    handle
        .send(BlockSyncEvent::PeerConnected(session.clone()))
        .await
        .unwrap();
    time::timeout(Duration::from_secs(1), session.wait_until_reactor_ready())
        .await
        .unwrap();
}

async fn check_response_writes(responses: &[(usize, bool)], queue_depth: usize) {
    let blocks = mainnet_blocks_1_to_3();
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 3,
        ..Default::default()
    };
    let (_tip_sender, tip) = watch::channel((block::Height(3), blocks[2].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(3),
            verified_block_hash: blocks[2].hash(),
        },
        (block::Height(3), blocks[2].hash()),
        tip,
        config.clone(),
    );
    let (handle, mut actions, reactor) = spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let regulator = handle
        .routine_wiring
        .as_ref()
        .unwrap()
        .serving_regulator
        .clone();
    let cancelled = CancellationToken::new();
    let (inbound, inbound_receiver) = framed_channel(8);
    let (outbound, mut writer) = worker_framed_channel(queue_depth);
    service.add_peer(Peer::new_with_direction(
        peer(91),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        HashMap::from([(
            ZAKURA_STREAM_BLOCK_SYNC,
            (inbound_receiver, outbound.clone()),
        )]),
        cancelled.clone(),
    ));
    let initial = time::timeout(Duration::from_secs(1), writer.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        BlockSyncMessage::decode_frame(initial.into_parts().0).unwrap(),
        BlockSyncMessage::Status(_)
    ));
    inbound
        .send(
            BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(3),
                tip_hash: blocks[2].hash(),
                max_blocks_per_response: 3,
                max_inflight_requests: 4,
                max_response_bytes: MAX_BS_RESPONSE_BYTES,
            })
            .encode_frame()
            .unwrap(),
        )
        .await
        .unwrap();
    for &(count, empty) in responses {
        let count_u32 = u32::try_from(count).unwrap();
        inbound
            .send(
                BlockSyncMessage::GetBlocks {
                    start_height: block::Height(1),
                    count: count_u32,
                }
                .encode_frame()
                .unwrap(),
            )
            .await
            .unwrap();
        let (request_id, lease) = loop {
            match next_action(&mut actions).await {
                BlockSyncAction::QueryBlocksByHeightRange {
                    request_id,
                    lease,
                    count,
                    ..
                } => {
                    assert_eq!(count, count_u32);
                    break (request_id, lease);
                }
                BlockSyncAction::QueryNeededBlocks { .. } => {}
                action => panic!("unexpected action before serving: {action:?}"),
            }
        };
        assert!(lease.try_start());
        // The accepted request acknowledges Status processing. Remove any handshake
        // refresh before measuring the deliberately small response queue.
        while outbound.capacity() < outbound.max_capacity() {
            let frame = writer.recv().await.unwrap().into_parts().0;
            assert!(matches!(
                BlockSyncMessage::decode_frame(frame).unwrap(),
                BlockSyncMessage::Status(_)
            ));
        }
        let served = if empty {
            Vec::new()
        } else {
            blocks
                .iter()
                .take(count)
                .map(|block| {
                    (
                        block.coinbase_height().unwrap(),
                        block.clone(),
                        usize::try_from(block_size(block)).unwrap(),
                    )
                })
                .collect()
        };
        handle
            .send(BlockSyncEvent::BlockRangeResponseReady {
                lease,
                request_id,
                peer: peer(91),
                start_height: block::Height(1),
                requested_count: count_u32,
                blocks: served,
            })
            .await
            .unwrap();
        reactor_barrier(&handle).await;

        let returned = if empty { 0 } else { count.min(queue_depth) };
        let waiting_terminal = returned == queue_depth;
        let snapshot = regulator.snapshot();
        assert_eq!(snapshot.node_active, usize::from(waiting_terminal));
        let payloads: Vec<u64> = blocks
            .iter()
            .take(returned)
            .map(|block| u64::from(block_size(block)) + 1)
            .collect();
        let expected_bytes = if waiting_terminal {
            u64::from(count_u32) * 2_000_000 + u64::from(count_u32) + 9
        } else {
            payloads.iter().sum::<u64>() + 9
        };
        assert_eq!(snapshot.node_outstanding, expected_bytes);
        assert!(
            !cancelled.is_cancelled(),
            "queue pressure is not protocol misconduct"
        );

        for index in 0..=returned {
            let queued = time::timeout(Duration::from_secs(1), writer.recv())
                .await
                .unwrap()
                .unwrap();
            let before_write = regulator.snapshot().node_outstanding;
            let (finish, completion) = oneshot::channel();
            let expected_block = blocks.get(index).cloned();
            let mut write = Box::pin(queued.write_with(|frame| async move {
                let bytes = u64::try_from(frame.payload.len()).unwrap();
                let message = BlockSyncMessage::decode_frame(frame).unwrap();
                if index < returned {
                    let BlockSyncMessage::Block(block) = message else {
                        panic!("expected a block before the terminal, got {message:?}");
                    };
                    assert_eq!(block.hash(), expected_block.unwrap().hash());
                } else if empty {
                    assert_eq!(
                        message,
                        BlockSyncMessage::RangeUnavailable {
                            start_height: block::Height(1),
                            count: count_u32,
                        }
                    );
                } else {
                    assert_eq!(
                        message,
                        BlockSyncMessage::BlocksDone {
                            start_height: block::Height(1),
                            returned: u32::try_from(returned).unwrap(),
                        }
                    );
                }
                completion.await.unwrap();
                bytes
            }));
            assert!(write.as_mut().now_or_never().is_none());
            assert_eq!(
                regulator.snapshot().node_outstanding,
                before_write,
                "dequeue and a pending write must preserve the charge"
            );
            finish.send(()).unwrap();
            let written = write.await;
            assert_eq!(
                regulator.snapshot().node_outstanding,
                before_write - written
            );
        }
        assert_eq!(regulator.snapshot().node_active, 0);
        assert_eq!(regulator.snapshot().node_outstanding, 0);
        assert!(!cancelled.is_cancelled());
    }
    cancelled.cancel();
    reactor.abort();
    assert!(reactor.await.unwrap_err().is_cancelled());
}

#[test]
fn every_response_shape_retains_bytes_through_application_write() {
    for count in 1..=3 {
        for depth in 1..=3 {
            for empty in [false, true] {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .start_paused(true)
                    .build()
                    .unwrap()
                    .block_on(check_response_writes(&[(count, empty)], depth));
            }
        }
    }
}

proptest! {
    #[test]
    fn reactor_response_histories_respect_queue_and_write_ownership(
        depth in 1usize..4,
        responses in prop::collection::vec((1usize..4, any::<bool>()), 1..5),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap()
            .block_on(check_response_writes(&responses, depth));
    }
}
