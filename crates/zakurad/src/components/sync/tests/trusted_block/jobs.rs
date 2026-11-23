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
        let mut different = receipt.clone();
        different[63] ^= 1;
        // Once the relay has bound its solution, a matching header hash with the wrong
        // block digest is refused and leaves the job available for the correct receipt.
        timeout(Duration::from_secs(20), async {
            loop {
                match post(&relay, "confirm", different.clone()).await {
                    StatusCode::CONFLICT => break,
                    StatusCode::ACCEPTED => tokio::task::yield_now().await,
                    status => panic!("unexpected confirmation response {status}"),
                }
            }
        })
        .await
        .unwrap();
        assert!(relay.pending.get(solved.hash()).is_none());
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

/// Posts one job input under an explicit job ID.
async fn post_as(node: &Node, id: &str, operation: &str, bytes: Vec<u8>) -> StatusCode {
    http_with_job(
        node.rpc,
        &node.auth,
        &format!("/submittrustedblock?op={operation}"),
        "application/octet-stream",
        bytes,
        Some(id),
    )
    .await
    .0
}

/// A miner starts on the primary's new template before the relay has committed that template's
/// parent. The job waits for the parent instead of expiring.
#[tokio::test(flavor = "multi_thread")]
async fn job_prepared_before_its_parent_commits_releases_once_the_parent_arrives() {
    let _init = zakura_test::init();
    let primary = Node::new(None).await;
    let relay = Node::new(None).await;
    let parent = primary.template().await;
    let parent_bytes = parent.zcash_serialize_to_vec().unwrap();
    assert_eq!(
        primary.submit(parent_bytes.clone(), false).await,
        "accepted"
    );
    let child = primary.template().await;
    assert_eq!(child.header.previous_block_hash, parent.hash());
    assert_eq!(relay.tip.best_tip_height(), Some(Height(0)));

    let id = "11111111111111111111111111111111";
    let proposal = child.zcash_serialize_to_vec().unwrap();
    let header = child.header.zcash_serialize_to_vec().unwrap();
    assert_eq!(
        post_as(&relay, id, "prepare", proposal.clone()).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        post_as(&relay, id, "solve", header.clone()).await,
        StatusCode::ACCEPTED
    );
    // Long enough for the worker to decode the proposal and see that its parent is missing.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        post_as(&relay, id, "prepare", proposal).await,
        StatusCode::ACCEPTED,
        "the job is still waiting for its parent"
    );
    assert!(relay.pending.get(child.hash()).is_none());

    assert_eq!(relay.submit(parent_bytes, false).await, "accepted");
    timeout(Duration::from_secs(20), async {
        while relay.tip.best_tip_hash() != Some(child.hash()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the job releases and commits once its parent is the tip");
    assert_eq!(
        post_as(&relay, id, "solve", header).await,
        StatusCode::ACCEPTED
    );
}

/// A caller confirms a body that is not the block its primary accepted, and the real block
/// commits through another path while the job's own commit is queued. Peers get the committed
/// block, and the job's duplicate verdict withdraws the prepared body.
#[tokio::test(flavor = "multi_thread")]
async fn committed_block_is_served_in_place_of_a_differing_prepared_body() {
    let _init = zakura_test::init();
    let mut relay = Node::new(None).await;
    let valid = relay.template().await;
    let valid_bytes = valid.zcash_serialize_to_vec().unwrap();
    let faulty = faulty_body(&valid);
    let faulty_bytes = faulty.zcash_serialize_to_vec().unwrap();
    relay.gate.hold();
    relay.hold_proposal.send_replace(true);
    let id = "22222222222222222222222222222222";
    assert_eq!(
        post_as(&relay, id, "prepare", faulty_bytes.clone()).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        post_as(
            &relay,
            id,
            "solve",
            valid.header.zcash_serialize_to_vec().unwrap()
        )
        .await,
        StatusCode::ACCEPTED
    );
    let mut receipt = valid.hash().0.to_vec();
    receipt.extend_from_slice(&Sha256::digest(&faulty_bytes));
    assert_eq!(
        post_as(&relay, id, "confirm", receipt.clone()).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(relay.gate.entered().await, valid.hash());
    // Before any commit, the confirmed bytes are what the relay serves.
    assert_eq!(
        fetch_from_peer(&relay, valid.hash()).await,
        Some(faulty_bytes)
    );
    let retained = relay
        .pending
        .get_for_peer(valid.hash())
        .unwrap()
        .pending_send
        .unwrap();

    // Commit the valid block as gossip would, bypassing the held verifier.
    relay
        .state
        .clone()
        .oneshot(zakura_state::Request::CommitSemanticallyVerifiedBlock(
            Arc::new(valid.clone()).into(),
        ))
        .await
        .unwrap();
    assert_eq!(relay.tip.best_tip_hash(), Some(valid.hash()));
    assert!(relay.pending.get(valid.hash()).is_some());
    assert_eq!(
        fetch_from_peer(&relay, valid.hash()).await,
        Some(valid_bytes.clone()),
        "a committed block is served in place of the prepared body"
    );

    relay.hold_proposal.send_replace(false);
    relay.gate.release();
    timeout(Duration::from_secs(20), async {
        while relay.pending.get(valid.hash()).is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the duplicate verdict resolves the prepared body");
    assert!(
        retained.try_start_write(|| ()).is_none(),
        "a prepared body that differs from the committed block is withdrawn"
    );
    assert_eq!(
        fetch_from_peer(&relay, valid.hash()).await,
        Some(valid_bytes)
    );
    // The header hash is committed, so inputs for the job are still acknowledged.
    assert_eq!(
        post_as(&relay, id, "confirm", receipt).await,
        StatusCode::ACCEPTED
    );
}
