//! Compact block relay checks, with mock state and mempool services.

use std::{collections::HashSet, sync::Arc};

use tower::{
    buffer::Buffer,
    util::{BoxCloneSyncService, BoxService},
};

use zakura_chain::{
    block::{self, Block, Height},
    parameters::Network,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{UnminedTx, UnminedTxId},
};
use zakura_network::CompactBlock;
use zakura_node_services::mempool;
use zakura_state::{self as zs, PreparedMinedRelayEligibility};
use zakura_test::{
    mock_service::{MockService, PanicAssertion},
    vectors::{BLOCK_MAINNET_1687107_BYTES, BLOCK_MAINNET_419201_BYTES},
};

use crate::components::inbound::{
    compact::{CompactRelay, MAX_CONCURRENT_COMPACT_CHECKS},
    Mempool,
};

type MockState = MockService<zs::ReadRequest, zs::ReadResponse, PanicAssertion>;
type MockMempool = MockService<mempool::Request, mempool::Response, PanicAssertion>;

/// Returns a Mainnet compact relay checker that reads from a mock state.
fn relay() -> (CompactRelay, MockState) {
    let state: MockState = MockService::build().for_unit_tests();
    let relay = CompactRelay::new(Network::Mainnet, BoxCloneSyncService::new(state.clone()));
    (relay, state)
}

/// Returns a mock mempool, and the buffered service the inbound service holds.
fn mempool() -> (Mempool, MockMempool) {
    let mempool: MockMempool = MockService::build().for_unit_tests();
    let service = Buffer::new(BoxService::new(mempool.clone()), 10);
    (service, mempool)
}

fn block(bytes: &[u8]) -> Arc<Block> {
    bytes.zcash_deserialize_into().expect("test block is valid")
}

fn compact(block: &Block) -> Arc<CompactBlock> {
    Arc::new(CompactBlock::from_block(block, 42).expect("test block fits compact block limits"))
}

/// The response to a tip request whose best tip is `block`'s parent.
fn parent_tip(block: &Block) -> zs::ReadResponse {
    let height = block.coinbase_height().expect("test block has a height");
    let parent_height = (height - 1).expect("test block is above genesis");
    zs::ReadResponse::Tip(Some((parent_height, block.header.previous_block_hash)))
}

/// Answers the state requests of a header that passes screening.
async fn pass_header_screening(state: &mut MockState, block: &Block) {
    state
        .expect_request(zs::ReadRequest::Tip)
        .await
        .respond(parent_tip(block));
    state
        .expect_request(zs::ReadRequest::CheckBestTipHeader {
            header: block.header.clone(),
            height: block.coinbase_height().expect("test block has a height"),
        })
        .await
        .respond(zs::ReadResponse::ValidBestTipHeader);
}

#[tokio::test]
async fn check_header_accepts_a_child_of_the_best_tip() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let block = block(&BLOCK_MAINNET_1687107_BYTES);

    let check = tokio::spawn({
        let compact = compact(&block);
        async move { relay.check_header(compact).await }
    });
    pass_header_screening(&mut state, &block).await;

    check
        .await
        .expect("check task finishes")
        .expect("a valid NU5 child of the best tip passes");
}

#[tokio::test]
async fn check_header_rejects_a_stale_parent_before_proof_of_work() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let block = block(&BLOCK_MAINNET_1687107_BYTES);

    let check = tokio::spawn({
        let compact = compact(&block);
        async move { relay.check_header(compact).await }
    });
    state
        .expect_request(zs::ReadRequest::Tip)
        .await
        .respond(zs::ReadResponse::Tip(Some((
            Height(1_687_106),
            block::Hash([7; 32]),
        ))));

    let error = check
        .await
        .expect("check task finishes")
        .expect_err("a header that doesn't extend the best tip fails");
    assert_eq!(
        error.to_string(),
        "compact block does not extend the best tip"
    );
    state.expect_no_requests().await;
}

#[tokio::test]
async fn check_header_rejects_a_bad_equihash_solution() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let block = block(&BLOCK_MAINNET_1687107_BYTES);
    let mut header_bytes = block
        .header
        .zcash_serialize_to_vec()
        .expect("test header serializes");
    // The last header byte is in the Equihash solution.
    *header_bytes.last_mut().expect("headers are not empty") ^= 1;
    let mut compact = (*compact(&block)).clone();
    compact.header = Arc::new(
        header_bytes
            .zcash_deserialize_into()
            .expect("the changed header still parses"),
    );

    let check = tokio::spawn(async move { relay.check_header(Arc::new(compact)).await });
    state
        .expect_request(zs::ReadRequest::Tip)
        .await
        .respond(parent_tip(&block));

    let error = check
        .await
        .expect("check task finishes")
        .expect_err("a bad Equihash solution fails");
    assert_eq!(
        error.to_string(),
        "compact block header fails proof of work"
    );
    state.expect_no_requests().await;
}

#[tokio::test]
async fn check_header_rejects_a_block_before_nu5() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let block = block(&BLOCK_MAINNET_419201_BYTES);

    let check = tokio::spawn({
        let compact = compact(&block);
        async move { relay.check_header(compact).await }
    });
    state
        .expect_request(zs::ReadRequest::Tip)
        .await
        .respond(parent_tip(&block));

    let error = check
        .await
        .expect("check task finishes")
        .expect_err("a pre-NU5 header fails");
    assert_eq!(
        error.to_string(),
        "compact block relay needs NU5 block commitments"
    );
    state.expect_no_requests().await;
}

#[tokio::test]
async fn checks_fail_when_every_permit_is_taken() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let block = block(&BLOCK_MAINNET_1687107_BYTES);

    // Each pending check holds its permit while it waits for the tip.
    let mut pending = Vec::new();
    for _ in 0..MAX_CONCURRENT_COMPACT_CHECKS {
        let relay = relay.clone();
        let compact = compact(&block);
        pending.push(tokio::spawn(
            async move { relay.check_header(compact).await },
        ));
    }
    let mut tip_requests = Vec::new();
    for _ in 0..MAX_CONCURRENT_COMPACT_CHECKS {
        tip_requests.push(state.expect_request(zs::ReadRequest::Tip).await);
    }

    let error = relay
        .check_header(compact(&block))
        .await
        .expect_err("a check with no free permit fails");
    assert_eq!(error.to_string(), "compact block checks are busy");

    for request in tip_requests {
        request.respond(zs::ReadResponse::Tip(None));
    }
    for check in pending {
        check
            .await
            .expect("check task finishes")
            .expect_err("a state without a tip fails every check");
    }
}

#[tokio::test]
async fn block_transactions_returns_unique_mempool_matches() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let (mempool_service, mut mempool) = mempool();
    let block = block(&BLOCK_MAINNET_1687107_BYTES);
    let transactions: Vec<UnminedTx> = block.transactions[1..].iter().map(Into::into).collect();
    let ids: HashSet<UnminedTxId> = transactions.iter().map(UnminedTx::id).collect();

    let check = tokio::spawn({
        let compact = compact(&block);
        async move { relay.block_transactions(compact, mempool_service).await }
    });
    pass_header_screening(&mut state, &block).await;
    let mut mempool_ids = ids.clone();
    mempool_ids.insert(UnminedTxId::from(block.transactions[0].as_ref()));
    mempool
        .expect_request(mempool::Request::TransactionIds)
        .await
        .respond(mempool::Response::TransactionIds(mempool_ids));
    mempool
        .expect_request(mempool::Request::TransactionsById(ids))
        .await
        .respond(mempool::Response::Transactions(transactions.clone()));

    let candidates = check
        .await
        .expect("check task finishes")
        .expect("a screened compact block gets its mempool matches");
    assert_eq!(candidates, transactions);
}

#[tokio::test]
async fn block_transactions_never_reads_the_mempool_for_a_stale_parent() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let (mempool_service, mut mempool) = mempool();
    let block = block(&BLOCK_MAINNET_1687107_BYTES);

    let check = tokio::spawn({
        let compact = compact(&block);
        async move { relay.block_transactions(compact, mempool_service).await }
    });
    state
        .expect_request(zs::ReadRequest::Tip)
        .await
        .respond(zs::ReadResponse::Tip(None));

    check
        .await
        .expect("check task finishes")
        .expect_err("a header that doesn't extend the best tip fails");
    mempool.expect_no_requests().await;
}

/// Answers the relay eligibility check of `block` with `eligibility`.
async fn answer_eligibility(
    state: &mut MockState,
    block: &Block,
    eligibility: PreparedMinedRelayEligibility,
) {
    let hash = block.hash();
    let auth_data_root = block.auth_data_root();
    state
        .expect_request_that(|request| {
            matches!(
                request,
                zs::ReadRequest::CheckPreparedMinedRelayEligibility(commitment)
                    if commitment.block.hash() == hash
                        && commitment.auth_data_root == Some(auth_data_root)
            )
        })
        .await
        .respond(zs::ReadResponse::PreparedMinedRelayEligibility(eligibility));
}

#[tokio::test]
async fn check_block_accepts_an_authorized_block() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let block = block(&BLOCK_MAINNET_1687107_BYTES);

    let check = tokio::spawn({
        let block = block.clone();
        async move { relay.check_block(block).await }
    });
    answer_eligibility(
        &mut state,
        &block,
        PreparedMinedRelayEligibility::Authorized,
    )
    .await;

    check
        .await
        .expect("check task finishes")
        .expect("an authorized rebuilt block passes");
}

#[tokio::test]
async fn check_block_rejects_a_block_the_state_does_not_authorize() {
    let _init_guard = zakura_test::init();
    let block = block(&BLOCK_MAINNET_1687107_BYTES);

    for eligibility in [
        PreparedMinedRelayEligibility::CommitFirst,
        PreparedMinedRelayEligibility::Unavailable,
    ] {
        let (relay, mut state) = relay();
        let check = tokio::spawn({
            let block = block.clone();
            async move { relay.check_block(block).await }
        });
        answer_eligibility(&mut state, &block, eligibility).await;

        check
            .await
            .expect("check task finishes")
            .expect_err("a block that isn't an authorized best-tip child fails");
    }
}

#[tokio::test]
async fn check_block_rejects_a_merkle_root_mismatch_before_reading_state() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();
    let mut block = (*block(&BLOCK_MAINNET_1687107_BYTES)).clone();
    // Reordering transactions keeps every short ID but changes the Merkle root.
    block.transactions.swap(1, 2);

    let error = relay
        .check_block(Arc::new(block))
        .await
        .expect_err("a body that doesn't match the Merkle root fails");
    assert!(
        error.to_string().contains("merkle root"),
        "unexpected error: {error}"
    );
    state.expect_no_requests().await;
}

#[tokio::test]
async fn check_block_rejects_a_block_before_nu5() {
    let _init_guard = zakura_test::init();
    let (relay, mut state) = relay();

    let error = relay
        .check_block(block(&BLOCK_MAINNET_419201_BYTES))
        .await
        .expect_err("a pre-NU5 block fails");
    assert_eq!(
        error.to_string(),
        "compact block relay needs NU5 block commitments"
    );
    state.expect_no_requests().await;
}
