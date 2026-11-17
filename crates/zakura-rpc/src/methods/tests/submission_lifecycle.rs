//! Duplicate verdicts for trusted submissions, settled against the committed block.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use tokio::time::{Duration, Instant};
use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_state::{KnownBlock, ReadRequest, ReadResponse};

use crate::methods::{
    submission_lifecycle::{
        committed_block_lookup, DuplicateBody, RelayPolicy, Submission, COMMITTED_BLOCK_WAIT,
    },
    types::submit_block::{MinedBlockSubmission, MinedSubmissions, PendingBlockRegistry},
};

/// State reports a duplicate while another copy is still in its write channel, then that copy
/// becomes readable `reads_before_commit` reads later, or never.
async fn resolve_duplicate(reads_before_commit: Option<usize>) -> (Option<DuplicateBody>, bool) {
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("the genesis block is valid");
    let hash = block.hash();
    let reads = Arc::new(AtomicUsize::new(0));
    let committed = block.clone();
    let read_state = tower::service_fn({
        let reads = reads.clone();
        move |request: ReadRequest| {
            assert_eq!(request, ReadRequest::AnyChainBlock(hash.into()));
            let read = reads.fetch_add(1, Ordering::SeqCst);
            let block = reads_before_commit
                .is_some_and(|misses| read >= misses)
                .then(|| committed.clone().into());
            async move { Ok::<_, tower::BoxError>(ReadResponse::Block(block)) }
        }
    });
    let verifier = tower::service_fn(move |_request: zakura_consensus::Request| async move {
        Err::<zakura_chain::block::Hash, _>(tower::BoxError::from(
            zakura_consensus::RouterError::from(zakura_consensus::VerifyBlockError::from(
                zakura_consensus::BlockError::AlreadyInChain(hash, KnownBlock::WriteChannel),
            )),
        ))
    });
    let registry = PendingBlockRegistry::default();
    let registration = registry.insert(block.clone()).unwrap();
    let permit = registry.get_for_peer(hash).unwrap().pending_send.unwrap();
    let (sender, _events) = tokio::sync::mpsc::unbounded_channel();
    let started = Instant::now();
    let duplicate_body = Submission {
        block: block.clone(),
        hash,
        height: block.coinbase_height().unwrap(),
        size: 0,
        submitted_at: std::time::Instant::now(),
        method: MinedBlockSubmission::TrustedBlock,
        reservation: MinedSubmissions::default().reserve(hash).unwrap(),
    }
    .spawn(
        verifier,
        sender,
        RelayPolicy::TrustedSubmission {
            registration,
            early_relay: false,
            committed_block: committed_block_lookup(read_state),
        },
        |outcome| outcome.duplicate_body,
    )
    .await
    .unwrap();
    if reads_before_commit.is_none() {
        assert!(started.elapsed() >= COMMITTED_BLOCK_WAIT - Duration::from_millis(50));
    } else {
        assert!(started.elapsed() < COMMITTED_BLOCK_WAIT);
    }
    assert!(registry.get(hash).is_none());
    (duplicate_body, permit.try_start_write(|| ()).is_some())
}

#[tokio::test(start_paused = true)]
async fn identical_duplicate_waits_for_the_other_copy_to_commit_and_keeps_its_sends() {
    assert_eq!(
        resolve_duplicate(Some(3)).await,
        (Some(DuplicateBody::Identical), true)
    );
}

#[tokio::test(start_paused = true)]
async fn duplicate_whose_other_copy_never_commits_is_withdrawn_at_the_deadline() {
    assert_eq!(
        resolve_duplicate(None).await,
        (Some(DuplicateBody::Unconfirmed), false)
    );
}
