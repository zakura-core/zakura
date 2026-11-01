//! Cancellation, grace retention, and capacity reclamation contracts.

use std::sync::Arc;

use tokio::{sync::oneshot, time::advance};
use zakura_chain::{block, serialization::ZcashDeserializeInto};

use super::super::{
    QueuedBlocks, QueuedSemanticallyVerified, ABANDONED_BLOCK_GRACE, MAX_QUEUED_BLOCKS,
};
use crate::{arbitrary::Prepare, CommitSemanticallyVerifiedError, SemanticallyVerifiedBlock};

type Receiver = oneshot::Receiver<Result<block::Hash, CommitSemanticallyVerifiedError>>;

fn candidate(parent: block::Hash, nonce: u64) -> SemanticallyVerifiedBlock {
    let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_419200_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let mut block = (*block).clone();
    let header = Arc::make_mut(&mut block.header);
    header.previous_block_hash = parent;
    header.nonce = {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&nonce.to_le_bytes());
        bytes.into()
    };
    Arc::new(block).prepare()
}

fn enqueue(queue: &mut QueuedBlocks, block: SemanticallyVerifiedBlock) -> Receiver {
    let (sender, receiver) = oneshot::channel();
    queue.queue((block, sender, None, 0));
    receiver
}

fn assert_consistent(queue: &QueuedBlocks) {
    assert_eq!(
        queue.blocks.len(),
        queue.by_parent.values().map(|hashes| hashes.len()).sum()
    );
    assert_eq!(
        queue.blocks.len(),
        queue.by_height.values().map(|hashes| hashes.len()).sum()
    );
    for (hash, queued) in &queue.blocks {
        assert!(queue.by_parent[&queued.0.block.header.previous_block_hash].contains(hash));
        assert!(queue.by_height[&queued.0.height].contains(hash));
    }
    for (outpoint, shared) in &queue.known_utxos {
        assert_eq!(
            shared.owners,
            queue
                .blocks
                .values()
                .filter(|queued| queued.0.new_outputs.contains_key(outpoint))
                .count()
        );
    }
    assert!(queue
        .abandoned_since
        .keys()
        .all(|hash| queue.blocks.contains_key(hash)));
}

#[tokio::test(start_paused = true)]
async fn grace_expires_without_being_reset_by_cleanup() {
    let mut queue = QueuedBlocks::default();
    let block = candidate(block::Hash([0x55; 32]), 1);
    let body = Arc::downgrade(&block.block);
    drop(enqueue(&mut queue, block));
    assert!(queue.prune_abandoned(None, false).is_empty());
    advance(ABANDONED_BLOCK_GRACE - std::time::Duration::from_secs(1)).await;
    assert!(queue.prune_abandoned(None, false).is_empty());
    advance(std::time::Duration::from_secs(1)).await;
    drop(queue.prune_abandoned(None, false));
    assert!(
        body.upgrade().is_none(),
        "eviction releases the retained body"
    );
    assert!(queue.blocks.is_empty());
    assert!(queue.known_utxos.is_empty());
    assert_consistent(&queue);
}

#[tokio::test(start_paused = true)]
async fn last_live_descendant_cancellation_releases_expired_ancestors() {
    let mut queue = QueuedBlocks::default();
    let parent = candidate(block::Hash([0x55; 32]), 1);
    let child = candidate(parent.hash, 2);
    drop(enqueue(&mut queue, parent.clone()));
    let child_response = enqueue(&mut queue, child.clone());
    assert!(queue.prune_abandoned(None, false).is_empty());
    advance(ABANDONED_BLOCK_GRACE).await;
    assert!(queue.prune_abandoned(None, false).is_empty());
    drop(child_response);
    let evicted = queue.prune_abandoned(None, false);
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0].0.hash, parent.hash);
    assert!(
        queue.blocks.contains_key(&child.hash),
        "the child has its own grace"
    );
    assert!(
        queue.has_queued_children(parent.hash),
        "a gap preserves the child index"
    );
    assert_consistent(&queue);
    advance(ABANDONED_BLOCK_GRACE).await;
    assert_eq!(queue.prune_abandoned(None, false).len(), 1);
    assert_consistent(&queue);
}

#[tokio::test(start_paused = true)]
async fn arriving_intermediate_connects_live_descendant_across_a_gap() {
    let mut queue = QueuedBlocks::default();
    let ancestor = candidate(block::Hash([0x55; 32]), 1);
    let intermediate = candidate(ancestor.hash, 2);
    let descendant = candidate(intermediate.hash, 3);
    drop(enqueue(&mut queue, ancestor.clone()));
    let _live = enqueue(&mut queue, descendant);
    queue.prune_abandoned(None, false);
    advance(ABANDONED_BLOCK_GRACE).await;
    assert!(queue
        .prune_abandoned(Some((intermediate.hash, ancestor.hash)), false)
        .is_empty());
    drop(enqueue(&mut queue, intermediate));
    queue.prune_abandoned(None, false);
    advance(ABANDONED_BLOCK_GRACE).await;
    assert!(queue.prune_abandoned(None, false).is_empty());
    assert_eq!(queue.blocks.len(), 3);
    assert_consistent(&queue);
}

#[tokio::test(start_paused = true)]
async fn pressure_removes_abandoned_intermediate_and_redelivery_restores_the_chain() {
    let mut queue = QueuedBlocks::default();
    let root = block::Hash([0x55; 32]);
    let intermediate = candidate(root, 1);
    let child = candidate(intermediate.hash, 2);
    let outpoint = *intermediate.new_outputs.keys().next().unwrap();
    drop(enqueue(&mut queue, intermediate.clone()));
    queue.prune_abandoned(None, false);
    advance(std::time::Duration::from_secs(1)).await;
    drop(enqueue(&mut queue, child.clone()));
    let mut live = Vec::new();
    for nonce in 3..=MAX_QUEUED_BLOCKS {
        live.push(enqueue(
            &mut queue,
            candidate(root, u64::try_from(nonce).unwrap()),
        ));
    }
    let incoming = candidate(root, 2000);
    let removed = queue.prune_abandoned(Some((incoming.hash, root)), true);
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].0.hash, intermediate.hash);
    assert!(queue.has_queued_children(intermediate.hash));
    assert!(
        queue.utxo(&outpoint).is_some(),
        "retained owners still supply shared outputs"
    );
    assert_consistent(&queue);

    let _redelivery = enqueue(&mut queue, intermediate.clone());
    assert_eq!(queue.dequeue_children(root).len(), MAX_QUEUED_BLOCKS - 1);
    let children = queue.dequeue_children(intermediate.hash);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].0.hash, child.hash);
    assert!(queue.known_utxos.is_empty());
    assert_consistent(&queue);
}

#[tokio::test(start_paused = true)]
async fn pressure_preserves_live_ancestors_and_replacement_restarts_grace() {
    let mut queue = QueuedBlocks::default();
    let parent = candidate(block::Hash([0x55; 32]), 1);
    let child = candidate(parent.hash, 2);
    drop(enqueue(&mut queue, parent.clone()));
    let child_response = enqueue(&mut queue, child.clone());
    let mut responses = vec![child_response];
    for nonce in 3..=MAX_QUEUED_BLOCKS {
        responses.push(enqueue(
            &mut queue,
            candidate(block::Hash([0x66; 32]), u64::try_from(nonce).unwrap()),
        ));
    }
    assert!(queue.prune_abandoned(None, true).is_empty());
    assert!(queue.is_full());
    advance(ABANDONED_BLOCK_GRACE).await;
    let (sender, response) = oneshot::channel();
    let replacement: QueuedSemanticallyVerified = (parent.clone(), sender, None, 1);
    assert!(queue
        .prune_abandoned(
            Some((parent.hash, parent.block.header.previous_block_hash)),
            false
        )
        .is_empty());
    drop(queue.replace(parent.hash, replacement));
    drop(responses);
    assert!(queue.prune_abandoned(None, false).is_empty());
    drop(response);
    assert!(queue.prune_abandoned(None, false).is_empty());
    advance(ABANDONED_BLOCK_GRACE).await;
    assert_eq!(queue.prune_abandoned(None, false).len(), MAX_QUEUED_BLOCKS);
    assert_consistent(&queue);
}
