//! Fixed test vectors for block queues.

use std::sync::Arc;

use tokio::sync::oneshot;

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_test::prelude::*;

use crate::{
    arbitrary::Prepare,
    service::queued_blocks::{
        QueuedBlocks, QueuedSemanticallyVerified, SentHashes, MAX_QUEUED_BLOCKS,
    },
    tests::FakeChainHelper,
    CommitBlockError, CommitSemanticallyVerifiedError,
};

// Quick helper trait for making queued blocks with throw away channels
trait IntoQueued {
    fn into_queued(self) -> QueuedSemanticallyVerified;
}

impl IntoQueued for Arc<Block> {
    fn into_queued(self) -> QueuedSemanticallyVerified {
        let (rsp_tx, _) = oneshot::channel();
        (self.prepare(), rsp_tx, None, 0)
    }
}

#[test]
fn dequeue_gives_right_children() -> Result<()> {
    let _init_guard = zakura_test::init();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;
    let child1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;
    let child2 = block1.make_fake_child();

    let parent = block1.header.previous_block_hash;

    let mut queue = QueuedBlocks::default();
    // Empty to start
    assert_eq!(0, queue.blocks.len());
    assert_eq!(0, queue.by_parent.len());
    assert_eq!(0, queue.by_height.len());
    assert_eq!(0, queue.known_utxos.len());

    // Inserting the first block gives us 1 in each table, and some UTXOs
    queue.queue(block1.clone().into_queued());
    assert_eq!(1, queue.blocks.len());
    assert_eq!(1, queue.by_parent.len());
    assert_eq!(1, queue.by_height.len());
    assert_eq!(2, queue.known_utxos.len());

    // The second gives us another in each table because its a child of the first,
    // and a lot of UTXOs
    queue.queue(child1.clone().into_queued());
    assert_eq!(2, queue.blocks.len());
    assert_eq!(2, queue.by_parent.len());
    assert_eq!(2, queue.by_height.len());
    assert_eq!(632, queue.known_utxos.len());

    // The 3rd only increments blocks, because it is also a child of the
    // first block, so for the second and third tables it gets added to the
    // existing HashSet value
    queue.queue(child2.clone().into_queued());
    assert_eq!(3, queue.blocks.len());
    assert_eq!(2, queue.by_parent.len());
    assert_eq!(2, queue.by_height.len());
    assert_eq!(634, queue.known_utxos.len());

    // Dequeueing the first block removes 1 block from each list
    let children = queue.dequeue_children(parent);
    assert_eq!(1, children.len());
    assert_eq!(block1, children[0].0.block);
    assert_eq!(2, queue.blocks.len());
    assert_eq!(1, queue.by_parent.len());
    assert_eq!(1, queue.by_height.len());
    assert_eq!(632, queue.known_utxos.len());

    // Dequeueing the children of the first block removes both of the other
    // blocks, and empties all lists
    let parent = children[0].0.block.hash();
    let children = queue.dequeue_children(parent);
    assert_eq!(2, children.len());
    assert!(children
        .iter()
        .any(|(block, _, _, _)| block.hash == child1.hash()));
    assert!(children
        .iter()
        .any(|(block, _, _, _)| block.hash == child2.hash()));
    assert_eq!(0, queue.blocks.len());
    assert_eq!(0, queue.by_parent.len());
    assert_eq!(0, queue.by_height.len());
    assert_eq!(0, queue.known_utxos.len());

    Ok(())
}

#[test]
fn identical_queued_retries_preserve_receipts_and_complete_the_old_waiter() -> Result<()> {
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES.zcash_deserialize_into()?;
    for order in [Some(1), None] {
        let mut queue = QueuedBlocks::default();
        let (response, mut receiver) = oneshot::channel();
        let admission = crate::BlockAdmission::pending();
        let mut first = block.clone().prepare();
        first.receipt_order = order;
        queue.queue((first, response, Some(admission), 1));
        let mut retry = block.clone().into_queued();
        retry.0.receipt_order = Some(9);
        assert!(queue.can_queue(&retry.0, false));
        queue.queue(retry);
        assert!(matches!(receiver.try_recv(), Ok(Err(_))));
        assert_eq!(queue.body_count, 1);
        assert_eq!(queue.blocks[&block.hash()][0].0.receipt_order, order);
        assert_eq!(
            queue.dequeue_children(block.header.previous_block_hash)[0]
                .0
                .receipt_order,
            order
        );
        assert_eq!(queue.body_count, 0);
    }
    Ok(())
}

#[test]
fn queued_body_variants_are_bounded_and_cleaned_up_together() -> Result<()> {
    use super::super::MAX_QUEUED_BODY_VARIANTS;
    use crate::tests::setup::changed_coinbase_body;
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES.zcash_deserialize_into()?;
    for cleanup in 0..4 {
        let mut queue = QueuedBlocks::default();
        let mut responses = Vec::new();
        for index in 0..MAX_QUEUED_BODY_VARIANTS {
            let body = changed_coinbase_body(&block, u8::try_from(index).unwrap());
            let (sender, receiver) = oneshot::channel();
            let prepared = body.prepare();
            assert!(queue.can_queue(&prepared, false));
            queue.queue((prepared, sender, None, 0));
            responses.push(receiver);
        }
        assert_eq!(queue.body_count, MAX_QUEUED_BODY_VARIANTS);
        assert_eq!(queue.blocks.len(), 1);
        assert!(!queue.can_queue(&block.clone().prepare(), false));
        assert!(!queue.can_queue(&block.clone().prepare(), true));
        assert!(queue.can_queue(&changed_coinbase_body(&block, 0).prepare(), false));
        match cleanup {
            0 => assert_eq!(
                queue
                    .dequeue_children(block.header.previous_block_hash)
                    .len(),
                MAX_QUEUED_BODY_VARIANTS
            ),
            1 => queue.prune_by_height(block.coinbase_height().unwrap()),
            2 => assert_eq!(queue.drain().count(), MAX_QUEUED_BODY_VARIANTS),
            _ => {
                let error = CommitBlockError::HeaderChainError {
                    error: "test failure".into(),
                };
                assert_eq!(
                    queue
                        .fail_descendants(block.header.previous_block_hash, error.into())
                        .len(),
                    MAX_QUEUED_BODY_VARIANTS
                );
            }
        }
        for receiver in &mut responses {
            assert!(!matches!(
                receiver.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        assert_eq!(queue.body_count, 0);
        assert!(queue.blocks.is_empty());
        assert!(queue.by_parent.is_empty());
        assert!(queue.by_height.is_empty());
        assert!(queue.known_utxos.is_empty());
    }
    Ok(())
}

#[test]
fn orphan_queue_counts_variants_against_the_shared_bound() -> Result<()> {
    use crate::tests::setup::changed_coinbase_body;
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES.zcash_deserialize_into()?;
    let mut queue = QueuedBlocks::default();
    queue.queue(block.clone().into_queued());
    queue.queue(changed_coinbase_body(&block, 1).into_queued());
    for index in 2..MAX_QUEUED_BLOCKS {
        let mut queued = block.clone().into_queued();
        let index = u64::try_from(index).expect("the queue bound fits in u64");
        queued.0.hash.0[..8].copy_from_slice(&index.to_le_bytes());
        queue.queue(queued);
    }
    assert!(queue.is_full());
    assert_eq!(queue.blocks.len(), MAX_QUEUED_BLOCKS - 1);
    assert!(queue.can_queue(&block.clone().prepare(), false));
    assert!(!queue.can_queue(&changed_coinbase_body(&block, 2).prepare(), false));
    assert!(queue.can_queue(&changed_coinbase_body(&block, 2).prepare(), true));
    Ok(())
}

#[test]
fn prune_removes_right_children() -> Result<()> {
    let _init_guard = zakura_test::init();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;
    let child1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;
    let child2 = block1.make_fake_child();

    let mut queue = QueuedBlocks::default();
    queue.queue(block1.clone().into_queued());
    queue.queue(child1.clone().into_queued());
    queue.queue(child2.clone().into_queued());
    assert_eq!(3, queue.blocks.len());
    assert_eq!(2, queue.by_parent.len());
    assert_eq!(2, queue.by_height.len());
    assert_eq!(634, queue.known_utxos.len());

    // Pruning the first height removes only block1
    queue.prune_by_height(block1.coinbase_height().unwrap());
    assert_eq!(2, queue.blocks.len());
    assert_eq!(1, queue.by_parent.len());
    assert_eq!(1, queue.by_height.len());
    assert!(!queue.blocks.contains_key(&block1.hash()));
    assert!(queue.blocks.contains_key(&child1.hash()));
    assert!(queue.blocks.contains_key(&child2.hash()));
    assert_eq!(632, queue.known_utxos.len());

    // Pruning the children of the first block removes both of the other
    // blocks, and empties all lists
    queue.prune_by_height(child1.coinbase_height().unwrap());
    assert_eq!(0, queue.blocks.len());
    assert_eq!(0, queue.by_parent.len());
    assert_eq!(0, queue.by_height.len());
    assert!(!queue.blocks.contains_key(&child1.hash()));
    assert!(!queue.blocks.contains_key(&child2.hash()));
    assert_eq!(0, queue.known_utxos.len());

    Ok(())
}

/// `SentHashes::remove` must drop the hash, its unshared outpoints from `known_utxos`,
/// and the corresponding `(hash, height)` entry from `curr_buf` (or whichever
/// batch buffer holds it). Without this, a rejected same-hash block would
/// keep a later honest re-delivery of a block at the same hash locked out as
/// a "duplicate" forever.
#[test]
fn sent_hashes_remove_drops_rejected_hash_and_utxos() -> Result<()> {
    let _init_guard = zakura_test::init();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;
    let block2: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;

    let prepared1 = block1.clone().prepare();
    let prepared2 = block2.clone().prepare();

    let mut sent = SentHashes::default();
    sent.add(&prepared1);
    sent.add(&prepared2);

    // Both hashes are present, and `known_utxos` contains every outpoint from
    // both blocks' coinbase + transparent outputs.
    let utxos_after_add = sent.known_utxos.len();
    assert!(sent.contains(&prepared1.hash));
    assert!(sent.contains(&prepared2.hash));
    assert!(utxos_after_add > 0);

    // Remove block1. block1's hash disappears, block2's stays, and the
    // total number of known utxos shrinks by exactly block1's contribution.
    let block1_utxos = prepared1.new_outputs.len();
    sent.remove(&prepared1.hash);

    assert!(
        !sent.contains(&prepared1.hash),
        "removed hash must not satisfy contains()"
    );
    assert!(sent.contains(&prepared2.hash));
    assert_eq!(
        sent.known_utxos.len(),
        utxos_after_add - block1_utxos,
        "remove must drop only the removed block's outpoints"
    );

    // The (hash, height) entry must be gone from the batch buffer too,
    // otherwise a later `prune_by_height` could re-insert into `sent`.
    assert!(
        !sent.curr_buf.iter().any(|(h, _)| h == &prepared1.hash),
        "remove must drop the (hash, height) entry from curr_buf"
    );
    assert!(sent.curr_buf.iter().any(|(h, _)| h == &prepared2.hash));

    // Removing a hash that isn't tracked is a no-op.
    let block3 = block1.make_fake_child();
    sent.remove(&block3.hash());
    assert!(sent.contains(&prepared2.hash));

    Ok(())
}

#[test]
fn sent_hashes_remove_many_preserves_other_blocks_across_batches() -> Result<()> {
    let _init_guard = zakura_test::init();
    let earlier: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;
    let earlier = earlier.prepare();
    let siblings = block.make_fake_siblings(3);
    let removed = siblings[0].clone().prepare();
    let retained = siblings[1].clone().prepare();
    let removed_current = siblings[2].clone().prepare();
    let unknown_hash = block.make_fake_child().hash();

    let mut sent = SentHashes::default();
    sent.add(&earlier);
    sent.finish_batch();
    sent.add(&removed);
    sent.add(&retained);
    sent.finish_batch();
    sent.add(&removed_current);

    // Empty input must leave both finished and unfinished batches untouched.
    sent.remove_many(&[]);
    assert_eq!(sent.sent.len(), 4);
    assert_eq!(sent.bufs.len(), 2);
    assert_eq!(sent.curr_buf.len(), 1);

    sent.remove_many(&[
        earlier.hash,
        removed.hash,
        removed_current.hash,
        removed.hash,
        unknown_hash,
    ]);

    assert_eq!(sent.sent.len(), 1);
    assert!(sent.contains(&retained.hash));
    assert_eq!(sent.known_utxos.len(), retained.new_outputs.len());
    for outpoint in retained.new_outputs.keys() {
        assert!(sent.utxo(outpoint).is_some());
    }
    assert!(sent.curr_buf.is_empty());
    assert_eq!(sent.bufs.len(), 1);
    assert_eq!(
        sent.bufs[0].iter().copied().collect::<Vec<_>>(),
        vec![(retained.hash, retained.height)]
    );

    // Redelivery acquires a new owner after the previous batch entry was removed.
    sent.add(&removed_current);
    sent.remove_many(&[retained.hash, removed_current.hash]);
    assert!(sent.sent.is_empty());
    assert!(sent.known_utxos.is_empty());
    assert!(sent.curr_buf.is_empty());
    assert!(sent.bufs.is_empty());
    Ok(())
}

#[test]
fn sent_hashes_remove_keeps_outputs_shared_with_sent_sibling() -> Result<()> {
    let _init_guard = zakura_test::init();
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;
    let siblings = block.make_fake_siblings(2);
    let evicted = siblings[0].clone().prepare();
    let in_flight = siblings[1].clone().prepare();
    assert_ne!(evicted.hash, in_flight.hash);

    let mut sent = SentHashes::default();
    sent.add(&evicted);
    sent.add(&in_flight);
    sent.remove(&evicted.hash);

    assert!(sent.contains(&in_flight.hash));
    for outpoint in in_flight.new_outputs.keys() {
        assert!(
            sent.utxo(outpoint).is_some(),
            "removing one sibling must retain shared output {outpoint:?} for the other"
        );
    }
    Ok(())
}

#[test]
fn sent_hashes_shared_outputs_release_after_last_distinct_block() -> Result<()> {
    let _init_guard = zakura_test::init();
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;
    let siblings = block.make_fake_siblings(2);
    let first = siblings[0].clone().prepare();
    let second = siblings[1].clone().prepare();
    let mut sent = SentHashes::default();
    sent.add(&first);
    sent.add(&first);
    sent.add_finalized(&crate::CheckpointVerifiedBlock::from(siblings[0].clone()));
    sent.add_finalized(&crate::CheckpointVerifiedBlock::from(siblings[1].clone()));
    sent.add(&second);
    sent.finish_batch();

    sent.remove(&first.hash);
    sent.remove(&first.hash);
    for outpoint in second.new_outputs.keys() {
        assert!(sent.utxo(outpoint).is_some());
    }
    sent.remove(&second.hash);
    assert!(sent.known_utxos.is_empty());
    assert!(sent.bufs.iter().all(|batch| batch.is_empty()));

    sent.add(&second);
    sent.prune_by_height(second.height);
    assert!(sent.known_utxos.is_empty());
    assert!(sent.sent.is_empty());
    Ok(())
}

#[test]
fn sent_hashes_pruning_keeps_outputs_owned_by_later_block() -> Result<()> {
    let _init_guard = zakura_test::init();
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;
    let siblings = block.make_fake_siblings(2);
    let first = siblings[0].clone().prepare();
    // The other fork includes the shared transactions one block later.
    let mut other_parent = siblings[1].clone();
    Arc::make_mut(&mut other_parent).transactions.truncate(1);
    let mut later = other_parent.make_fake_child();
    Arc::make_mut(&mut later)
        .transactions
        .extend(block.transactions.iter().skip(1).cloned());
    let later = later.prepare();
    assert_eq!(later.height, (first.height + 1).unwrap());
    assert!(first.new_outputs.iter().any(|(outpoint, output)| {
        !output.utxo.from_coinbase && later.new_outputs.contains_key(outpoint)
    }));

    let mut sent = SentHashes::default();
    sent.add(&first);
    sent.finish_batch();
    sent.add(&later);
    sent.finish_batch();
    sent.prune_by_height(first.height);
    assert!(!sent.contains(&first.hash));
    assert!(sent.contains(&later.hash));
    for outpoint in later.new_outputs.keys() {
        assert!(sent.utxo(outpoint).is_some());
    }
    for outpoint in first.new_outputs.keys() {
        if !later.new_outputs.contains_key(outpoint) {
            assert!(sent.utxo(outpoint).is_none());
        }
    }

    sent.prune_by_height(later.height);
    assert!(sent.known_utxos.is_empty());
    assert!(sent.sent.is_empty());
    Ok(())
}

// Ensures `dequeue_children` does not remove same-height sibling blocks from other forks.
#[test]
fn dequeue_children_preserves_same_height_siblings() -> Result<()> {
    let _init_guard = zakura_test::init();

    let root_block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;

    let left_child: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;
    let left_grandchild = left_child.make_fake_child();

    let right_child = root_block.make_fake_child();
    let right_grandchild = right_child.make_fake_child();

    let mut queue = QueuedBlocks::default();
    queue.queue(left_grandchild.clone().into_queued());
    queue.queue(right_grandchild.clone().into_queued());

    let height = left_grandchild.coinbase_height().unwrap();

    // Sanity check: both entries are indexed under the same height bucket
    assert_eq!(
        queue.by_height.get(&height).unwrap().len(),
        2,
        "expected both fork grandchildren to be in the same height bucket"
    );

    // Dequeue only one branch
    queue.dequeue_children(left_child.hash());

    assert!(
        queue.blocks.contains_key(&right_grandchild.hash()),
        "sibling block must remain in queue after unrelated dequeue"
    );

    assert!(
        queue
            .by_height
            .get(&height)
            .unwrap()
            .contains(&right_grandchild.hash()),
        "sibling must remain indexed by height after unrelated dequeue"
    );

    Ok(())
}

#[test]
fn dequeue_descendants_removes_the_complete_failed_subtree() -> Result<()> {
    let _init_guard = zakura_test::init();
    let root: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;
    let failed_child: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;
    let failed_grandchild = failed_child.make_fake_child();
    let sibling = root.make_fake_child();

    let mut queue = QueuedBlocks::default();
    let mut responses = Vec::new();
    for block in [failed_child, failed_grandchild, sibling] {
        let (response, receiver) = oneshot::channel();
        queue.queue((block.prepare(), response, None, 0));
        responses.push(receiver);
    }
    let error = CommitSemanticallyVerifiedError::from(CommitBlockError::HeaderChainError {
        error: format!("ancestor {} failed", root.hash()),
    });

    assert_eq!(queue.fail_descendants(root.hash(), error.clone()).len(), 3);
    for response in &mut responses {
        assert_eq!(response.try_recv(), Ok(Err(error.clone())));
    }
    assert!(queue.blocks.is_empty());
    assert!(queue.by_parent.is_empty());
    assert!(queue.by_height.is_empty());
    assert!(queue.known_utxos.is_empty());

    Ok(())
}
