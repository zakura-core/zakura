//! Ownership, joining, cancellation, recovery and limits of shared block downloads.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{sync::oneshot, time::timeout};

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_network::{types::InventoryHash, InventoryResponse, PeerSocketAddr};

use super::*;

fn test_block() -> Arc<Block> {
    zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("test vector is a valid block")
}

fn supplier() -> PeerSocketAddr {
    "203.0.113.7:8233".parse().unwrap()
}

fn reply(block: &Arc<Block>) -> zn::Response {
    zn::Response::Blocks(vec![InventoryResponse::Available((
        block.clone().into(),
        Some(supplier()),
    ))])
}

/// A transfer that waits for `release`, and counts how often it was started and dropped.
fn held_transfer(
    started: &Arc<AtomicUsize>,
    dropped: &Arc<AtomicUsize>,
    release: oneshot::Receiver<Result<zn::Response, BoxError>>,
) -> impl std::future::Future<Output = Result<zn::Response, BoxError>> + Send + 'static {
    struct CountDrop(Arc<AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    started.fetch_add(1, Ordering::SeqCst);
    let guard = CountDrop(dropped.clone());
    async move {
        let _guard = guard;
        release
            .await
            .unwrap_or_else(|_| Err("test released nothing".into()))
    }
}

fn owner(claim: Claim) -> OwnerSlot {
    match claim {
        Claim::Owner(slot) => slot,
        _ => panic!("expected to own the download"),
    }
}

fn joined(claim: Claim) -> Interest {
    match claim {
        Claim::Joined(interest) => interest,
        _ => panic!("expected to join the download"),
    }
}

async fn body(interest: &mut Interest) -> (block::Hash, Option<PeerSocketAddr>) {
    let response = timeout(Duration::from_secs(5), interest.response())
        .await
        .expect("shared download finishes")
        .expect("shared download succeeds");
    let zn::Response::Blocks(blocks) = response else {
        panic!("block reply")
    };
    let (block, supplier) = blocks[0].available().unwrap();
    (block.hash(), supplier)
}

async fn until_empty(downloads: &SharedBlockDownloads) {
    timeout(Duration::from_secs(5), async {
        while !downloads.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("entry is removed");
}

#[tokio::test]
async fn concurrent_callers_share_one_transfer_and_its_supplier() {
    let downloads = SharedBlockDownloads::default();
    let block = test_block();
    let (started, dropped) = Default::default();
    let (release, released) = oneshot::channel();

    let slot = owner(downloads.claim(block.hash(), Requester::Gossip));
    let mut sync = joined(downloads.claim(block.hash(), Requester::Sync));
    let mut gossip = slot.start(held_transfer(&started, &dropped, released));
    release.send(Ok(reply(&block))).unwrap();

    assert_eq!(body(&mut gossip).await, (block.hash(), Some(supplier())));
    assert_eq!(body(&mut sync).await, (block.hash(), Some(supplier())));
    assert_eq!(started.load(Ordering::SeqCst), 1);

    gossip.verified();
    sync.verified();
    drop((gossip, sync));
    assert!(downloads.is_empty());
}

#[tokio::test]
async fn a_finished_body_is_kept_for_a_caller_that_arrives_before_verification_ends() {
    let downloads = SharedBlockDownloads::default();
    let block = test_block();
    let mut first = owner(downloads.claim(block.hash(), Requester::Gossip))
        .start(async { Ok::<_, BoxError>(reply(&test_block())) });
    body(&mut first).await;

    let Claim::Joined(mut late) = downloads.claim(block.hash(), Requester::Sync) else {
        panic!("a caller still verifying keeps the body available")
    };
    assert_eq!(body(&mut late).await, (block.hash(), Some(supplier())));

    first.verified();
    late.verified();
    drop(first);
    assert_eq!(downloads.len(), 1, "the late caller still holds the entry");
    drop(late);
    assert!(downloads.is_empty());
}

#[tokio::test]
async fn one_caller_leaving_keeps_the_transfer_for_the_other() {
    let downloads = SharedBlockDownloads::default();
    let block = test_block();
    let (started, dropped) = Default::default();
    let (release, released) = oneshot::channel();

    let gossip = owner(downloads.claim(block.hash(), Requester::Gossip))
        .start(held_transfer(&started, &dropped, released));
    let mut sync = joined(downloads.claim(block.hash(), Requester::Sync));
    drop(gossip);
    tokio::task::yield_now().await;
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        0,
        "sync still wants the block"
    );

    release.send(Ok(reply(&block))).unwrap();
    assert_eq!(body(&mut sync).await.0, block.hash());
}

#[tokio::test]
async fn the_last_caller_leaving_stops_the_transfer_and_a_new_caller_starts_again() {
    let downloads = SharedBlockDownloads::default();
    let block = test_block();
    let (started, dropped) = Default::default();
    let (_release, released) = oneshot::channel();

    let gossip = owner(downloads.claim(block.hash(), Requester::Gossip))
        .start(held_transfer(&started, &dropped, released));
    let sync = joined(downloads.claim(block.hash(), Requester::Sync));
    drop((gossip, sync));
    timeout(Duration::from_secs(5), async {
        while dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the transfer is stopped");
    assert!(downloads.is_empty());

    let mut retry = owner(downloads.claim(block.hash(), Requester::Sync))
        .start(async { Ok::<_, BoxError>(reply(&test_block())) });
    assert_eq!(body(&mut retry).await.0, block.hash());
}

#[tokio::test]
async fn an_unverified_body_is_dropped_so_a_corrected_body_can_be_fetched() {
    let downloads = SharedBlockDownloads::default();
    let block = test_block();
    let mut gossip = owner(downloads.claim(block.hash(), Requester::Gossip))
        .start(async { Ok::<_, BoxError>(reply(&test_block())) });
    let mut sync = joined(downloads.claim(block.hash(), Requester::Sync));
    body(&mut gossip).await;
    body(&mut sync).await;

    // Gossip's verification rejects the body: its entry goes away at once, while sync
    // still holds its own copy of the reply.
    drop(gossip);
    assert!(downloads.is_empty());
    assert!(matches!(
        downloads.claim(block.hash(), Requester::Gossip),
        Claim::Owner(_)
    ));
    drop(sync);
}

#[tokio::test]
async fn a_failed_transfer_reaches_every_caller_and_is_not_reused() {
    let downloads = SharedBlockDownloads::default();
    let block = test_block();
    let hash = block.hash();
    let mut sync = owner(downloads.claim(hash, Requester::Sync)).start(async move {
        Err::<zn::Response, BoxError>(
            zn::SharedPeerError::from(zn::PeerError::NotFoundResponse(vec![InventoryHash::Block(
                hash,
            )]))
            .into(),
        )
    });
    let mut gossip = joined(downloads.claim(hash, Requester::Gossip));

    for interest in [&mut sync, &mut gossip] {
        let error = interest.response().await.unwrap_err();
        let peer = error
            .downcast_ref::<zn::SharedPeerError>()
            .expect("peer errors keep their type");
        assert_eq!(peer.not_found_class(), Some(zn::NotFoundClass::Response));
    }
    until_empty(&downloads).await;
    assert!(matches!(
        downloads.claim(hash, Requester::Sync),
        Claim::Owner(_)
    ));
}

#[tokio::test]
async fn an_owner_that_never_starts_fails_its_joiners() {
    let downloads = SharedBlockDownloads::default();
    let hash = test_block().hash();
    let slot = owner(downloads.claim(hash, Requester::Sync));
    let mut gossip = joined(downloads.claim(hash, Requester::Gossip));
    drop(slot);

    assert!(gossip.response().await.is_err());
    assert!(downloads.is_empty());
}

#[tokio::test]
async fn limits_bypass_sharing_and_skip_keeping_bodies() {
    let block = test_block();

    let full = SharedBlockDownloads::new(1, MAX_RETAINED_BODY_BYTES);
    let _slot = owner(full.claim(block.hash(), Requester::Sync));
    let other: block::Hash = [7; 32].into();
    assert!(matches!(
        full.claim(other, Requester::Gossip),
        Claim::Bypass
    ));

    let no_bytes = SharedBlockDownloads::new(8, 0);
    let mut first = owner(no_bytes.claim(block.hash(), Requester::Sync))
        .start(async { Ok::<_, BoxError>(reply(&test_block())) });
    body(&mut first).await;
    assert!(
        no_bytes.is_empty(),
        "a body over the byte limit is not kept"
    );
    assert!(matches!(
        no_bytes.claim(block.hash(), Requester::Gossip),
        Claim::Owner(_)
    ));
    first.verified();
}
