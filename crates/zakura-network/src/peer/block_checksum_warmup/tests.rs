//! Tests for block checksum precomputation during block advertisement.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use bytes::BytesMut;
use tokio_util::codec::Encoder;
use zakura_chain::{
    block::Block,
    serialization::{sha256d, ZcashDeserialize, ZcashSerialize},
};

use super::*;
use crate::{
    protocol::external::{Codec, Message},
    AdvertisedBlock,
};

/// A block large enough to share its checksum.
fn large_block() -> Arc<Block> {
    let block = Block::zcash_deserialize(&zakura_test::vectors::BLOCK_TESTNET_141042_BYTES[..])
        .expect("block test vector should deserialize");
    Arc::new(block)
}

/// An inbound service that has `block`, and counts its lookups in `lookups`.
fn inbound_with(
    block: Arc<Block>,
    lookups: Arc<AtomicUsize>,
) -> impl Service<Request, Response = Response, Error = BoxError, Future: Send> + Clone + Send {
    tower::service_fn(move |request| {
        let block = block.clone();
        lookups.fetch_add(1, Ordering::SeqCst);
        async move {
            let Request::BlocksByHash(hashes) = request else {
                panic!("block warm-up only looks blocks up by hash");
            };
            let blocks = hashes
                .into_iter()
                .map(|hash| {
                    if hash == block.hash() {
                        InventoryResponse::Available((block.clone().into(), None))
                    } else {
                        InventoryResponse::Missing(hash)
                    }
                })
                .collect();
            Ok(Response::Blocks(blocks))
        }
    })
}

/// A block advertiser that counts the advertisements it receives in `adverts`.
fn advertiser(
    adverts: Arc<AtomicUsize>,
) -> impl Service<Request, Response = Response, Error = BoxError> {
    tower::service_fn(move |_request| {
        adverts.fetch_add(1, Ordering::SeqCst);
        async { Ok(Response::Nil) }
    })
}

/// Advertising a large block precomputes the checksum its `block` message is sent with, so
/// sending the block afterwards does not hash it again.
#[tokio::test]
async fn advertised_block_checksum_is_precomputed() {
    let _init_guard = zakura_test::init();
    let block = large_block();
    let size = block.zcash_serialized_size();
    let checksums = BlockChecksums::default();
    let lookups = Arc::new(AtomicUsize::new(0));
    let adverts = Arc::new(AtomicUsize::new(0));
    let mut warmup = BlockChecksumWarmup::new(
        advertiser(adverts.clone()),
        inbound_with(block.clone(), lookups.clone()),
        checksums.clone(),
    );

    let response = warmup
        .ready()
        .await
        .expect("the advertiser is ready")
        .call(Request::AdvertiseBlock(
            AdvertisedBlock::new(block.hash(), size),
            None,
        ))
        .await
        .expect("the advertisement is forwarded");
    assert_eq!(response, Response::Nil);
    assert_eq!(adverts.load(Ordering::SeqCst), 1);

    let precomputed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(checksum) = checksums.get(block.hash(), size) {
                return checksum;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the advertised block's checksum is precomputed");
    assert_eq!(lookups.load(Ordering::SeqCst), 1);

    let mut codec = Codec::builder()
        .with_block_checksums(checksums.clone())
        .finish();
    codec.reconfigure_full_body_len();
    let mut frame = BytesMut::new();
    codec
        .encode(Message::Block(block.clone().into()), &mut frame)
        .expect("the test block encodes");
    let body = &frame[frame.len() - size..];
    assert_eq!(precomputed, sha256d::Checksum::from(body));
    assert_eq!(frame[20..24], precomputed.0);
    assert_eq!(
        checksums.checksum_with(block.hash(), size, || panic!("the body is hashed again")),
        precomputed,
    );
}

/// Advertising a block known to be too small to share its checksum, or whose checksum is
/// already known, does not look the block up.
#[tokio::test]
async fn small_or_known_blocks_are_not_looked_up() {
    let _init_guard = zakura_test::init();
    let block = large_block();
    let size = block.zcash_serialized_size();
    let checksums = BlockChecksums::default();
    checksums.precompute(&block.clone().into());
    let lookups = Arc::new(AtomicUsize::new(0));
    let adverts = Arc::new(AtomicUsize::new(0));
    let mut warmup = BlockChecksumWarmup::new(
        advertiser(adverts.clone()),
        inbound_with(block.clone(), lookups.clone()),
        checksums,
    );

    for request in [
        Request::AdvertiseMinedBlock(AdvertisedBlock::new(block.hash(), size)),
        Request::AdvertiseBlock(
            AdvertisedBlock::new(block::Hash([1; 32]), MIN_SHARED_CHECKSUM_BODY_LEN - 1),
            None,
        ),
    ] {
        warmup
            .ready()
            .await
            .expect("the advertiser is ready")
            .call(request)
            .await
            .expect("the advertisement is forwarded");
    }

    // A warm-up would run in its own task, so give one time to start.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(adverts.load(Ordering::SeqCst), 2);
    assert_eq!(lookups.load(Ordering::SeqCst), 0);
}

/// An inbound service that serves `block` as a pending body for the first `pending_lookups`
/// lookups, and as the committed body afterwards. It counts its lookups in `lookups`.
fn inbound_committing_after(
    block: Arc<Block>,
    pending_lookups: usize,
    lookups: Arc<AtomicUsize>,
) -> impl Service<Request, Response = Response, Error = BoxError, Future: Send> + Clone + Send {
    let (_status, permit) = zakura_node_services::block_relay::PendingBlockSendPermit::channel();
    tower::service_fn(move |_request| {
        let lookup = lookups.fetch_add(1, Ordering::SeqCst);
        let mut served = zakura_node_services::block_relay::ServedBlock::from(block.clone());
        if lookup < pending_lookups {
            served.pending_send = Some(permit.clone());
        }
        async move {
            Ok(Response::Blocks(vec![InventoryResponse::Available((
                served, None,
            ))]))
        }
    })
}

/// A committed announcement can reach the warm-up before read state has the block. It waits
/// for the committed body instead of caching the pending body's checksum.
#[tokio::test]
async fn committed_announcement_waits_for_the_committed_body() {
    let _init_guard = zakura_test::init();
    let block = large_block();
    let size = block.zcash_serialized_size();
    let checksums = BlockChecksums::default();
    let lookups = Arc::new(AtomicUsize::new(0));
    let mut warmup = BlockChecksumWarmup::new(
        advertiser(Arc::new(AtomicUsize::new(0))),
        inbound_committing_after(block.clone(), 2, lookups.clone()),
        checksums.clone(),
    );

    warmup
        .ready()
        .await
        .expect("the advertiser is ready")
        .call(Request::AdvertiseMinedBlock(AdvertisedBlock::new(
            block.hash(),
            size,
        )))
        .await
        .expect("the advertisement is forwarded");

    tokio::time::timeout(Duration::from_secs(10), async {
        while checksums.get(block.hash(), size).is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the committed block's checksum is precomputed");
    assert_eq!(lookups.load(Ordering::SeqCst), 3);
}

/// A pending body never reaches the shared cache: an early announcement is not warmed, and a
/// committed announcement gives up if only the pending body is ever found.
#[tokio::test]
async fn pending_bodies_are_not_precomputed() {
    let _init_guard = zakura_test::init();
    let block = large_block();
    let size = block.zcash_serialized_size();
    let checksums = BlockChecksums::default();
    let lookups = Arc::new(AtomicUsize::new(0));
    let mut warmup = BlockChecksumWarmup::new(
        advertiser(Arc::new(AtomicUsize::new(0))),
        inbound_committing_after(block.clone(), usize::MAX, lookups.clone()),
        checksums.clone(),
    );
    let (_status, permit) = zakura_node_services::block_relay::PendingBlockSendPermit::channel();
    let early = AdvertisedBlock {
        pending_send: Some(permit),
        ..AdvertisedBlock::new(block.hash(), size)
    };

    for request in [
        Request::AdvertiseMinedBlock(early),
        Request::AdvertiseMinedBlock(AdvertisedBlock::new(block.hash(), size)),
    ] {
        warmup
            .ready()
            .await
            .expect("the advertiser is ready")
            .call(request)
            .await
            .expect("the advertisement is forwarded");
    }

    // Retries take 50 ms in total; give them ample time to finish.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(lookups.load(Ordering::SeqCst), COMMITTED_LOOKUP_RETRIES + 1);
    assert!(checksums.get(block.hash(), size).is_none());
}
