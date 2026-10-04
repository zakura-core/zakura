//! Inbound service tests.

use std::{
    net::{IpAddr, Ipv4Addr},
    time::{Duration, Instant},
};

use super::{
    block_by_hash_or_pending, block_misbehavior, canonical_ip, PrunedBlockNotFoundLogger,
    ZCASHD_COMPAT_PRUNED_BLOCK_LOG_INTERVAL,
};

#[tokio::test]
async fn peer_block_lookup_queries_all_active_chains() {
    use std::sync::Arc;

    use tower::{buffer::Buffer, util::BoxService};
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
    use zakura_rpc::PendingBlockRegistry;
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("the genesis block is valid");
    let hash = block.hash();
    let expected_block = block.clone();
    let state = tower::service_fn(move |request| {
        let expected_block = expected_block.clone();
        async move {
            assert_eq!(request, zakura_state::Request::AnyChainBlock(hash.into()));
            Ok::<_, zakura_state::BoxError>(zakura_state::Response::Block(Some(expected_block)))
        }
    });
    let state = Buffer::new(BoxService::new(state), 1);

    assert_eq!(
        block_by_hash_or_pending(state, PendingBlockRegistry::default(), hash)
            .await
            .expect("the state lookup succeeds"),
        Some(block),
    );
}

#[tokio::test]
async fn peer_block_lookup_serves_admitted_block_before_state() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use tower::{buffer::Buffer, util::BoxService};
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
    use zakura_rpc::PendingBlockRegistry;

    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("the genesis block is valid");
    let hash = block.hash();
    let state_called = Arc::new(AtomicBool::new(false));
    let state_called_for_service = state_called.clone();
    let state = tower::service_fn(move |_request: zakura_state::Request| {
        state_called_for_service.store(true, Ordering::SeqCst);
        async { Ok::<_, zakura_state::BoxError>(zakura_state::Response::Block(None)) }
    });
    let state = Buffer::new(BoxService::new(state), 1);
    let pending_blocks = PendingBlockRegistry::default();
    assert!(pending_blocks.insert(block.clone()));

    assert_eq!(
        block_by_hash_or_pending(state, pending_blocks, hash)
            .await
            .expect("the pending lookup succeeds"),
        Some(block),
    );
    assert!(!state_called.load(Ordering::SeqCst));
}

/// Checks that a zcashd-compat sidecar gets every block it requests, while other peers'
/// responses stop once they pass [`GETDATA_SENT_BYTES_LIMIT`](super::GETDATA_SENT_BYTES_LIMIT).
#[tokio::test]
async fn zcashd_compat_block_requests_skip_the_byte_limit() -> Result<(), crate::BoxError> {
    use std::{net::SocketAddr, sync::Arc};

    use indexmap::IndexSet;
    use tokio::sync::oneshot;
    use tower::{buffer::Buffer, util::BoxService, Service, ServiceExt};
    use tracing::Span;
    use zakura_chain::{
        amount::Amount,
        block::Block,
        parameters::Network,
        serialization::{ZcashDeserializeInto, ZcashSerialize},
        transaction::{LockTime, Transaction},
        transparent,
    };
    use zakura_network::{
        constants::DEFAULT_MAX_CONNS_PER_IP, AddressBook, InventoryResponse::Available, PeerSource,
        Request, Response,
    };
    use zakura_test::mock_service::MockService;

    use super::{
        downloads::MAX_INBOUND_CONCURRENCY, Inbound, InboundSetupData, GETDATA_SENT_BYTES_LIMIT,
    };

    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;

    // Three blocks of about 600 KB, so a response passes the byte limit after two of them.
    let genesis: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.zcash_deserialize_into()?;
    let output = transparent::Output {
        value: Amount::zero(),
        lock_script: transparent::Script::new(&[0; 25]),
    };
    let transaction = Arc::new(Transaction::V1 {
        inputs: Vec::new(),
        outputs: vec![output; 18_000],
        lock_time: LockTime::unlocked(),
    });
    let blocks: Vec<Arc<Block>> = (0..3)
        .map(|nonce| {
            let mut header = *genesis.header;
            header.nonce.0 = [nonce; 32];
            Arc::new(Block {
                header: Arc::new(header),
                transactions: vec![transaction.clone()],
            })
        })
        .collect();
    let block_size = blocks[0].zcash_serialized_size();
    assert!(block_size < GETDATA_SENT_BYTES_LIMIT && 2 * block_size > GETDATA_SENT_BYTES_LIMIT);

    let state_blocks = blocks.clone();
    let state = tower::service_fn(move |request: zakura_state::Request| {
        let zakura_state::Request::AnyChainBlock(zakura_state::HashOrHeight::Hash(hash)) = request
        else {
            panic!("unexpected state request: {request:?}");
        };
        let block = state_blocks
            .iter()
            .find(|block| block.hash() == hash)
            .cloned();
        async move { Ok::<_, zakura_state::BoxError>(zakura_state::Response::Block(block)) }
    });

    let sidecar: SocketAddr = "127.0.0.1:18233".parse()?;
    let (setup_tx, setup_rx) = oneshot::channel();
    let mut inbound = Inbound::new(
        MAX_INBOUND_CONCURRENCY,
        false,
        None,
        vec![sidecar.ip()],
        setup_rx,
    );
    let (_chain_tip_sender, latest_chain_tip, _chain_tip_change) =
        zakura_state::ChainTipSender::new(None, &network);
    let (misbehavior_sender, _misbehavior_rx) = tokio::sync::mpsc::channel(1);
    let setup = InboundSetupData {
        address_book: Arc::new(std::sync::Mutex::new(AddressBook::new(
            "0.0.0.0:0".parse()?,
            &network,
            DEFAULT_MAX_CONNS_PER_IP,
            Span::none(),
        ))),
        block_download_peer_set: Buffer::new(
            BoxService::new(MockService::build().for_unit_tests()),
            1,
        ),
        block_verifier: Buffer::new(BoxService::new(MockService::build().for_unit_tests()), 1),
        mempool: Buffer::new(BoxService::new(MockService::build().for_unit_tests()), 1),
        state: Buffer::new(BoxService::new(state), 1),
        latest_chain_tip,
        network,
        misbehavior_sender,
    };
    assert!(
        setup_tx.send(setup).is_ok(),
        "the inbound service is waiting for setup"
    );

    let hashes: IndexSet<_> = blocks.iter().map(|block| block.hash()).collect();
    let available = |blocks: &[Arc<Block>]| {
        Response::Blocks(
            blocks
                .iter()
                .map(|block| Available((block.clone(), None)))
                .collect(),
        )
    };

    let response = inbound
        .ready()
        .await?
        .call(Request::BlocksByHash(hashes.clone()))
        .await?;
    assert_eq!(
        response,
        available(&blocks[..2]),
        "other peers' responses stop once they pass the limit"
    );

    let response = inbound
        .ready()
        .await?
        .call(Request::BlocksByHashFrom {
            hashes,
            source: PeerSource::LegacySocket(sidecar.into()),
        })
        .await?;
    assert_eq!(
        response,
        available(&blocks),
        "the sidecar gets every block it requests"
    );

    Ok(())
}

mod fake_peer_set;
mod real_peer_set;

#[test]
fn router_consensus_invalid_gossip_keeps_advertiser_score() {
    let advertiser = "192.0.2.1:8233".parse().expect("valid peer address");
    let error = zakura_consensus::VerifyBlockError::Block {
        source: zakura_consensus::BlockError::NoTransactions,
    };
    let router_error = zakura_consensus::RouterError::Block {
        source: Box::new(error),
    };

    assert_eq!(
        block_misbehavior(Box::new(router_error), Some(advertiser)),
        Some((
            advertiser,
            zakura_network::constants::MAX_PEER_MISBEHAVIOR_SCORE,
        )),
    );
}

#[test]
fn direct_consensus_invalid_gossip_keeps_advertiser_score() {
    let advertiser = "192.0.2.1:8233".parse().expect("valid peer address");
    let error = zakura_consensus::VerifyBlockError::Block {
        source: zakura_consensus::BlockError::NoTransactions,
    };

    assert_eq!(
        block_misbehavior(Box::new(error), Some(advertiser)),
        Some((
            advertiser,
            zakura_network::constants::MAX_PEER_MISBEHAVIOR_SCORE,
        )),
    );
}

#[test]
fn pruned_block_not_found_log_is_rate_limited() {
    let logger = PrunedBlockNotFoundLogger::new(Some(10_000), Vec::new());
    let start = Instant::now();

    assert_eq!(logger.reserve_log_at(start), Some(10_000));
    assert_eq!(logger.reserve_log_at(start + Duration::from_secs(1)), None);
    assert_eq!(
        logger.reserve_log_at(start + ZCASHD_COMPAT_PRUNED_BLOCK_LOG_INTERVAL),
        Some(10_000)
    );
}

#[test]
fn pruned_block_not_found_log_is_disabled_without_compat_pruning() {
    let logger = PrunedBlockNotFoundLogger::new(None, Vec::new());

    assert_eq!(logger.reserve_log_at(Instant::now()), None);
}

#[test]
fn pruned_block_not_found_peer_ips_canonicalize_mapped_ipv6() {
    let ipv4 = Ipv4Addr::new(192, 0, 2, 1);

    assert_eq!(
        canonical_ip(IpAddr::V6(ipv4.to_ipv6_mapped())),
        IpAddr::V4(ipv4)
    );
}
