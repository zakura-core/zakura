//! Invalid proof of work through trusted HTTP, real peer serving, and consensus rejection.

use super::*;
use zakura_chain::{
    parameters::testnet::{ConfiguredCheckpoints, Parameters},
    serialization::ZcashDeserialize,
    work::difficulty::{ParameterDifficulty, INVALID_COMPACT_DIFFICULTY},
};
use zakura_consensus::{BlockError, VerifyBlockError};

/// Preserve the actual verifier failure so rejection cannot pass for an unrelated reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ValidationFailure {
    InvalidTarget,
    InsufficientWork,
    Equihash,
    Other,
}

impl From<&RouterError> for ValidationFailure {
    fn from(error: &RouterError) -> Self {
        match error {
            RouterError::Block { source } => match source.as_ref() {
                VerifyBlockError::Block {
                    source: BlockError::InvalidDifficulty(..),
                } => Self::InvalidTarget,
                VerifyBlockError::Block {
                    source: BlockError::DifficultyFilter(..),
                } => Self::InsufficientWork,
                VerifyBlockError::Equihash { .. } => Self::Equihash,
                _ => Self::Other,
            },
            _ => Self::Other,
        }
    }
}

/// Use a private chain with a cheap hash target, but real hash and Equihash enforcement.
fn validating_network() -> (Network, Arc<Block>) {
    // Regtest genesis has a waived proof; use an actual solved genesis for this chain.
    let genesis = Arc::new(
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..]).unwrap(),
    );
    let easy_target = zakura_chain::block::genesis::regtest_genesis_block()
        .header
        .difficulty_threshold
        .to_expanded()
        .unwrap();
    let network = Parameters::build()
        .with_genesis_hash(genesis.hash())
        .unwrap()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![(
            Height(0),
            genesis.hash(),
        )]))
        .unwrap()
        .with_activation_heights(ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        })
        .unwrap()
        .clear_funding_streams()
        .with_slow_start_interval(Height::MIN)
        .with_disable_pow(false)
        .disable_temporary_orchard_disabling_soft_fork()
        .with_target_difficulty_limit(easy_target)
        .unwrap()
        .to_network()
        .unwrap();
    assert!(!network.disable_pow());
    assert!(!zakura_header_chain::PowPolicy::for_network(&network)
        .unwrap()
        .is_authenticated_custom_waiver());
    (network, genesis)
}

async fn invalid_pow_is_relayed_then_rejected(failure: ValidationFailure) {
    let _init = zakura_test::init();
    let (network, genesis) = validating_network();
    let mut relay = Node::with_network(None, P2pStack::Legacy, network, genesis).await;
    let (advertised, mut advertisements) = mpsc::unbounded_channel();
    // An explicit peer fetch observes the relay's bytes without a downstream verifier
    // disconnecting first and obscuring the relay's own rejection and withdrawal.
    let observer = zakura_network::connect_isolated_with_inbound(
        &relay.network,
        tokio::net::TcpStream::connect(relay.p2p).await.unwrap(),
        "proof-of-work observer".into(),
        tower::service_fn(move |request| {
            if let Request::AdvertiseBlock(block, _) = request {
                advertised.send(block.hash).unwrap();
            }
            async { Ok::<_, BoxError>(Response::Nil) }
        }),
    )
    .await
    .unwrap();
    let observer = Buffer::new(observer, 4);
    relay.connected().await;
    let mut block = relay.template().await;
    // Choose the network's easy limit so finding a passing hash is cheap. These
    // candidates must fail PoW before any contextual expected-difficulty check.
    Arc::make_mut(&mut block.header).difficulty_threshold =
        relay.network.target_difficulty_limit().to_compact();
    let height = block.coinbase_height().unwrap();
    match failure {
        ValidationFailure::InvalidTarget => {
            Arc::make_mut(&mut block.header).difficulty_threshold = INVALID_COMPACT_DIFFICULTY;
        }
        ValidationFailure::InsufficientWork | ValidationFailure::Equihash => {
            // The template has an unsolved Equihash solution. Choose a nonce that either
            // fails the hash filter or passes it, isolating the intended rejection stage.
            let should_pass = failure == ValidationFailure::Equihash;
            let found = (0u32..10_000).any(|nonce| {
                Arc::make_mut(&mut block.header).nonce.0[..4].copy_from_slice(&nonce.to_le_bytes());
                zakura_consensus::difficulty_is_valid(
                    &block.header,
                    &relay.network,
                    &height,
                    &block.hash(),
                )
                .is_ok()
                    == should_pass
            });
            assert!(
                found,
                "the easy private-chain target permits a bounded nonce search"
            );
        }
        ValidationFailure::Other => unreachable!("each case names a proof-of-work failure"),
    }
    let hash = block.hash();
    let bytes = block.zcash_serialize_to_vec().unwrap();
    relay.gate.hold();
    let submission = tokio::spawn(submit(relay.rpc, relay.auth.clone(), bytes.clone(), true));
    assert_eq!(relay.gate.entered().await, hash);
    timeout(Duration::from_secs(20), async {
        while advertisements.recv().await.unwrap() != hash {}
    })
    .await
    .unwrap();
    let response = timeout(
        Duration::from_secs(20),
        observer
            .clone()
            .oneshot(Request::BlocksByHash([hash].into_iter().collect())),
    )
    .await
    .unwrap()
    .unwrap();
    let Response::Blocks(blocks) = response else {
        panic!("the peer receives the pending block before validation")
    };
    let zakura_network::InventoryResponse::Available((fetched, _)) = &blocks[0] else {
        panic!("the pending body is available to peers")
    };
    assert_eq!(fetched.zcash_serialize_to_vec().unwrap(), bytes);
    assert!(!submission.is_finished());
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));

    relay.gate.release();
    assert_eq!(
        timeout(Duration::from_secs(20), submission)
            .await
            .unwrap()
            .unwrap(),
        "rejected"
    );
    assert_eq!(
        timeout(Duration::from_secs(20), relay.gate.failures.recv())
            .await
            .unwrap()
            .unwrap(),
        (hash, failure),
    );
    assert!(relay.pending.get(hash).is_none());
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
    assert!(matches!(
        relay
            .state
            .clone()
            .oneshot(zakura_state::Request::AnyChainBlock(hash.into()))
            .await
            .unwrap(),
        zakura_state::Response::Block(None)
    ));
    let missing = timeout(
        Duration::from_secs(20),
        observer
            .clone()
            .oneshot(Request::BlocksByHash([hash].into_iter().collect())),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        missing
            .downcast_ref::<zakura_network::SharedPeerError>()
            .unwrap()
            .not_found_class(),
        Some(zakura_network::NotFoundClass::Response),
    );

    // A rejected header releases submission ownership rather than poisoning later attempts.
    assert_eq!(relay.submit(bytes, true).await, "rejected");
    assert_eq!(
        timeout(Duration::from_secs(20), relay.gate.failures.recv())
            .await
            .unwrap()
            .unwrap(),
        (hash, failure),
    );
    assert!(relay.pending.get(hash).is_none());
    timeout(
        Duration::from_secs(20),
        observer.clone().oneshot(Request::Peers),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_invalid_difficulty_target_is_relayed_then_rejected() {
    invalid_pow_is_relayed_then_rejected(ValidationFailure::InvalidTarget).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_insufficient_work_is_relayed_then_rejected() {
    invalid_pow_is_relayed_then_rejected(ValidationFailure::InsufficientWork).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn trusted_invalid_equihash_is_relayed_then_rejected() {
    invalid_pow_is_relayed_then_rejected(ValidationFailure::Equihash).await;
}
