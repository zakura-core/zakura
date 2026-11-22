//! Prepared trusted jobs through the production HTTP listener, verifier, and peer service.
use super::*;
use sha2::{Digest, Sha256};

async fn post(node: &Node, operation: &str, bytes: Vec<u8>) -> StatusCode {
    http_with_job(
        node.rpc,
        &node.auth,
        &format!("/submittrustedblock?op={operation}"),
        "application/octet-stream",
        bytes,
        Some("0123456789abcdef0123456789abcdef"),
    )
    .await
    .0
}

#[tokio::test(flavor = "multi_thread")]
async fn verified_proposal_serves_solved_bytes_while_commit_is_held() {
    prepared_delivery(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn primary_confirmation_serves_solved_bytes_while_proposal_and_commit_are_held() {
    prepared_delivery(true).await;
}

async fn prepared_delivery(confirm: bool) {
    let _init = zakura_test::init();
    let mut relay = Node::new(None).await;
    let receiver = Node::new(Some(relay.p2p)).await;
    relay.connected().await;
    receiver.connected().await;
    let candidate = relay.template().await;
    let mut solved = candidate.clone();
    // This fixture's Regtest parameters waive PoW. Change the nonce so that a peer receiving
    // the unsolved candidate instead of the mined block cannot pass the byte comparison.
    Arc::make_mut(&mut solved.header).nonce[0] ^= 1;
    let bytes = solved.zcash_serialize_to_vec().unwrap();
    let header = solved.header.zcash_serialize_to_vec().unwrap();
    relay.deny_peer_state_read.send_replace(true);
    relay.gate.hold();
    relay.hold_proposal.send_replace(true);
    assert_eq!(
        post(&relay, "solve", header.clone()).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        post(
            &relay,
            "prepare",
            candidate.zcash_serialize_to_vec().unwrap()
        )
        .await,
        StatusCode::ACCEPTED
    );
    assert!(relay.pending.get(solved.hash()).is_none());
    if confirm {
        let primary = Node::new(None).await;
        assert_eq!(primary.submit(bytes.clone(), false).await, "accepted");
        let mut receipt = solved.hash().0.to_vec();
        receipt.extend_from_slice(&Sha256::digest(&bytes));
        assert_eq!(post(&relay, "confirm", receipt).await, StatusCode::ACCEPTED);
    } else {
        relay.hold_proposal.send_replace(false);
    }
    assert_eq!(relay.gate.entered().await, solved.hash());
    assert_eq!(post(&relay, "solve", header).await, StatusCode::ACCEPTED);
    timeout(Duration::from_secs(20), async {
        while receiver.tip.best_tip_hash() != Some(solved.hash()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));
    let zakura_state::Response::Block(Some(fetched)) = receiver
        .state
        .clone()
        .oneshot(zakura_state::Request::Block(solved.hash().into()))
        .await
        .unwrap()
    else {
        panic!("the peer committed the block while the relay's commit was held");
    };
    assert_eq!(fetched.zcash_serialize_to_vec().unwrap(), bytes);
    relay.hold_proposal.send_replace(false);
    relay.gate.release();
    assert_eq!(
        timeout(Duration::from_secs(20), relay.gate.finished.recv())
            .await
            .unwrap(),
        Some((solved.hash(), true))
    );
}
