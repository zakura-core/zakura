//! Inbound service tests.

use std::{
    net::{IpAddr, Ipv4Addr},
    time::{Duration, Instant},
};

use super::downloads::EarlyRelayedBlockCommitError;
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
fn post_relay_contextual_failure_does_not_score_advertiser() {
    let advertiser = "192.0.2.1:8233".parse().expect("valid peer address");
    let error = EarlyRelayedBlockCommitError::new("contextual commit failed".into());

    assert_eq!(block_misbehavior(Box::new(error), Some(advertiser)), None);
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
