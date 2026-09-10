//! Generated bursts through the same sequential-serving fixture as the regressions.

use super::*;
use proptest::prelude::*;
use tokio::sync::oneshot;

async fn check_request_burst(
    depth: usize,
    outcome: usize,
    requests: Vec<u32>,
    cancel_at: Option<usize>,
) {
    let fail = outcome == 3;
    let available = if fail { 0 } else { outcome };
    let source = Source::with_outcome(true, fail, available);
    let mut f = Fixture::with_queue_depth(source.clone(), depth);
    let regulator = f.regulator.clone();
    f.session.mark_status_received();
    let sender = f.requests.clone();
    let counts = requests.clone();
    let mut burst = AbortOnDropHandle::new(tokio::spawn(async move {
        for count in counts {
            let request = BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count,
            }
            .encode_frame()
            .unwrap();
            time::timeout(Duration::from_secs(2), sender.send(request))
                .await
                .unwrap()?;
        }
        Ok::<_, tokio::sync::mpsc::error::SendError<crate::zakura::Frame>>(())
    }));
    let total_frames: usize = requests
        .iter()
        .map(|count| available.min(usize::try_from(*count).unwrap()) + 1)
        .sum();
    let cancel_at = cancel_at.map(|index| index % total_frames);
    let mut written = 0;
    for (response, requested) in requests.iter().copied().enumerate() {
        let returned = available.min(usize::try_from(requested).unwrap());
        source.wait_calls(response + 1).await;
        for index in 0..=returned {
            let queued = time::timeout(Duration::from_secs(2), f.data.recv())
                .await
                .unwrap()
                .unwrap();
            let (complete, completion) = oneshot::channel();
            let mut write = Box::pin(queued.write_with(|frame| async move {
                match BlockSyncMessage::decode_frame(frame).unwrap() {
                    BlockSyncMessage::Block(block) if index < returned => {
                        assert_eq!(
                            block.coinbase_height(),
                            Some(block::Height(u32::try_from(index + 1).unwrap()))
                        );
                        let expected = if index == 0 {
                            &*zakura_test::vectors::BLOCK_MAINNET_1_BYTES
                        } else {
                            &*zakura_test::vectors::BLOCK_MAINNET_2_BYTES
                        };
                        assert_eq!(
                            block.hash(),
                            block::Block::zcash_deserialize(expected.as_slice())
                                .unwrap()
                                .hash()
                        );
                    }
                    BlockSyncMessage::BlocksDone {
                        start_height,
                        returned: count,
                    } if index == returned && returned > 0 => {
                        assert_eq!(start_height, block::Height(1));
                        assert_eq!(usize::try_from(count).unwrap(), returned);
                    }
                    BlockSyncMessage::RangeUnavailable {
                        start_height,
                        count,
                    } if returned == 0 => {
                        assert_eq!((start_height, count), (block::Height(1), requested));
                    }
                    other => panic!("unexpected response frame {index}: {other:?}"),
                }
                completion.await.unwrap();
                Ok::<_, std::convert::Infallible>(())
            }));
            assert!(futures::poll!(&mut write).is_pending());
            tokio::task::yield_now().await;
            assert_eq!(regulator.snapshot().node_active, 1);
            assert_eq!(
                source.0.calls.load(Ordering::Acquire),
                response + 1,
                "a pending write must prevent the next request from starting a read"
            );
            if cancel_at == Some(written) {
                let replacement = regulator.session(f.session.peer_id().clone());
                let request = replacement
                    .decode_request(
                        BlockSyncMessage::GetBlocks {
                            start_height: block::Height(1),
                            count: 1,
                        }
                        .encode_frame()
                        .unwrap(),
                    )
                    .unwrap();
                burst.abort();
                // A completed burst can already be in the bounded request channel.
                let _ = time::timeout(Duration::from_secs(2), &mut burst)
                    .await
                    .unwrap();
                f.finish().await;
                assert_eq!(
                    regulator.snapshot().node_active,
                    1,
                    "the write owns capacity after the serving task and queue close"
                );
                drop(write);
                // Cancellation can leave an encode running or its undelivered
                // result alive. Wait for actual ownership release before retrying.
                drop(
                    time::timeout(Duration::from_secs(2), replacement.admit_request(&request))
                        .await
                        .unwrap(),
                );
                assert_eq!(regulator.snapshot().node_active, 0);
                assert_eq!(regulator.snapshot().peer_active, 0);
                return;
            }
            complete.send(()).unwrap();
            write.await.unwrap();
            written += 1;
        }
    }
    time::timeout(Duration::from_secs(2), &mut burst)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(source.0.calls.load(Ordering::Acquire), requests.len());
    assert!(!f.session.cancel_token().is_cancelled());
    f.finish().await;
    assert_eq!(regulator.snapshot().node_active, 0);
    assert_eq!(regulator.snapshot().peer_active, 0);
}

#[tokio::test]
async fn cancelled_burst_recovers_after_all_owners_finish() {
    // The concrete input from the previous PR's failing CI run.
    check_request_burst(2, 1, vec![2, 1, 1, 1, 2, 2, 1, 2], Some(0)).await;
}

proptest! {
    #[test]
    fn request_bursts_preserve_complete_responses_and_write_ownership(
        depth in 1usize..4,
        outcome in 0usize..4,
        requests in prop::collection::vec(1u32..=2, 1..9),
        cancel_at in prop::option::of(0usize..24),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check_request_burst(depth, outcome, requests, cancel_at));
    }
}
