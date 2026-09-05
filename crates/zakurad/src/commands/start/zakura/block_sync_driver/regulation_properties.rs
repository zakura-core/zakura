//! Verify the real driver's state-future lifetime under controlled completion.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use proptest::prelude::*;
use tokio::sync::{oneshot, watch};
use tower::service_fn;
use zakura_chain::serialization::ZcashDeserializeInto;
use zakura_network::zakura::{
    spawn_block_sync_reactor, testkit::SyntheticBlockSyncPeers, BlockSyncFrontiers,
    BlockSyncMessage, BlockSyncStartup, BlockSyncStatus, ZakuraBlockSyncConfig, ZakuraPeerId,
};

use super::*;

#[derive(Clone, Copy, Debug)]
enum DeliveryEnd {
    Complete,
    CancelBeforeStart,
    CancelWhileRunning,
    Timeout,
}

#[derive(Clone, Copy, Debug)]
enum ReadEnd {
    Success,
    Error,
    Panic,
}

struct ReadLifetime(Arc<AtomicUsize>);

impl Drop for ReadLifetime {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn next_serving_query(
    actions: &mut mpsc::Receiver<BlockSyncAction>,
) -> Option<BlockSyncAction> {
    while let Some(action) = actions.recv().await {
        if matches!(action, BlockSyncAction::QueryBlocksByHeightRange { .. }) {
            return Some(action);
        }
    }
    None
}

async fn check_read_lifetime(delivery: DeliveryEnd, read_end: ReadEnd, delay_ms: u64) {
    let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let tip = (block::Height(1), block.hash());
    let (_tip_sender, tip_receiver) = watch::channel(tip);
    let mut config = ZakuraBlockSyncConfig::default();
    config.get_blocks_regulation.query_timeout = Duration::from_millis(10);
    config.get_blocks_regulation.node_active_requests = 1;
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_receiver,
        config.clone(),
    );
    let (handle, mut actions, reactor) = spawn_block_sync_reactor(startup);
    let peers = SyntheticBlockSyncPeers::new(config, handle.clone(), 8);
    let peer = peers
        .add_peer(
            ZakuraPeerId::new(vec![81; 32]).unwrap(),
            BlockSyncStatus {
                servable_low: tip.0,
                servable_high: tip.0,
                tip_hash: tip.1,
                max_blocks_per_response: 1,
                max_inflight_requests: 1,
                max_response_bytes: 2_000_000,
            },
        )
        .await
        .unwrap();
    peer.send(BlockSyncMessage::GetBlocks {
        start_height: tip.0,
        count: 1,
    })
    .await
    .unwrap();
    let action = tokio::time::timeout(Duration::from_secs(1), next_serving_query(&mut actions))
        .await
        .unwrap()
        .unwrap();
    let BlockSyncAction::QueryBlocksByHeightRange { lease, .. } = &action else {
        unreachable!()
    };
    let cancellation = lease.clone();
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let (complete, completion) = oneshot::channel();
    let mut completion = Some(completion);
    let read_state = service_fn({
        let started = started.clone();
        let finished = finished.clone();
        move |request| {
            assert!(matches!(
                request,
                zakura_state::ReadRequest::BlocksByHeightRange {
                    start: block::Height(1),
                    count: 1,
                    ..
                }
            ));
            started.fetch_add(1, Ordering::SeqCst);
            let completion = completion
                .take()
                .expect("one query can start at most one read");
            let lifetime = ReadLifetime(finished.clone());
            async move {
                let _lifetime = lifetime;
                completion.await.unwrap();
                match read_end {
                    ReadEnd::Success => Ok(zakura_state::ReadResponse::Blocks(Vec::new())),
                    ReadEnd::Error => Err::<_, zakura_state::BoxError>(
                        std::io::Error::other("controlled read failure").into(),
                    ),
                    ReadEnd::Panic => panic!("controlled read panic"),
                }
            }
        }
    });
    if matches!(delivery, DeliveryEnd::CancelBeforeStart) {
        peer.cancel();
        tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
            .await
            .unwrap();
    }
    let mut worker = Box::pin(serve_block_range(
        action,
        read_state,
        handle,
        ZakuraTrace::default(),
    ));
    if matches!(delivery, DeliveryEnd::CancelBeforeStart) {
        assert!(worker.as_mut().now_or_never().is_some());
        assert_eq!(started.load(Ordering::SeqCst), 0);
        assert_eq!(finished.load(Ordering::SeqCst), 0);
    } else {
        assert!(worker.as_mut().now_or_never().is_none());
        assert_eq!(started.load(Ordering::SeqCst), 1);
        match delivery {
            DeliveryEnd::CancelWhileRunning => {
                peer.cancel();
                tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
                    .await
                    .unwrap();
            }
            DeliveryEnd::Timeout => {
                tokio::time::advance(Duration::from_millis(10 + delay_ms)).await
            }
            DeliveryEnd::Complete => {}
            DeliveryEnd::CancelBeforeStart => unreachable!(),
        }
        // A timeout/cancellation may end delivery, but may not drop the read.
        assert!(worker.as_mut().now_or_never().is_none());
        assert_eq!(finished.load(Ordering::SeqCst), 0);
        drop(cancellation);
        // A second native peer observes the node's single active slot. This
        // checks the lease itself, independently of the read-future drop probe.
        let second = peers
            .add_peer(
                ZakuraPeerId::new(vec![82; 32]).unwrap(),
                BlockSyncStatus {
                    servable_low: tip.0,
                    servable_high: tip.0,
                    tip_hash: tip.1,
                    max_blocks_per_response: 1,
                    max_inflight_requests: 1,
                    max_response_bytes: 2_000_000,
                },
            )
            .await
            .unwrap();
        second
            .send(BlockSyncMessage::GetBlocks {
                start_height: tip.0,
                count: 1,
            })
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), next_serving_query(&mut actions))
                .await
                .is_err(),
            "unfinished storage work must keep the node's only active slot"
        );
        complete.send(()).unwrap();
        assert!(worker.as_mut().now_or_never().is_some());
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        let resumed =
            tokio::time::timeout(Duration::from_secs(1), next_serving_query(&mut actions))
                .await
                .unwrap()
                .unwrap();
        assert!(
            matches!(resumed, BlockSyncAction::QueryBlocksByHeightRange { ref peer, .. } if peer == second.peer_id()),
            "the waiting peer must acquire the released slot"
        );
        drop(resumed);
        second.cancel();
    }
    peer.cancel();
    reactor.abort();
    assert!(reactor.await.unwrap_err().is_cancelled());
}

#[test]
fn delivery_end_preserves_the_underlying_read_until_completion() {
    for delivery in [
        DeliveryEnd::Complete,
        DeliveryEnd::CancelBeforeStart,
        DeliveryEnd::CancelWhileRunning,
        DeliveryEnd::Timeout,
    ] {
        for read_end in [ReadEnd::Success, ReadEnd::Error, ReadEnd::Panic] {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap()
                .block_on(check_read_lifetime(delivery, read_end, 1));
        }
    }
}

proptest! {
    #[test]
    fn state_read_lifetime_is_independent_of_delivery_deadline(delay_ms in 1u64..10_000, fails in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap()
            .block_on(check_read_lifetime(DeliveryEnd::Timeout, if fails { ReadEnd::Error } else { ReadEnd::Success }, delay_ms));
    }
}
