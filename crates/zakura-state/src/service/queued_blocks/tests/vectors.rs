//! Fixed test vectors for block queues.

use std::sync::Arc;

use tokio::sync::oneshot;

use zakura_chain::{
    block::{self, Block},
    serialization::ZcashDeserializeInto,
    transparent,
};
use zakura_test::prelude::*;

use crate::{
    arbitrary::Prepare,
    service::queued_blocks::{
        QueuedBlocks, QueuedSemanticallyVerified, SentHashes, MAX_QUEUED_BLOCKS,
    },
    tests::FakeChainHelper,
    CommitBlockError, CommitSemanticallyVerifiedError,
    SemanticallyVerifiedBlock,
};

// Quick helper trait for making queued blocks with throw away channels
trait IntoQueued {
    fn into_queued(self) -> QueuedSemanticallyVerified;
}

impl IntoQueued for Arc<Block> {
    fn into_queued(self) -> QueuedSemanticallyVerified {
        let (rsp_tx, _) = oneshot::channel();
        (self.prepare(), rsp_tx, None)
    }
}

impl IntoQueued for SemanticallyVerifiedBlock {
    fn into_queued(self) -> QueuedSemanticallyVerified {
        let (rsp_tx, _) = oneshot::channel();
        (self, rsp_tx, None)
    }
}

#[derive(Clone)]
struct SharedUtxoProviders {
    lower: SemanticallyVerifiedBlock,
    higher: SemanticallyVerifiedBlock,
    outpoint: transparent::OutPoint,
    lower_utxo: transparent::Utxo,
    higher_utxo: transparent::Utxo,
    lower_parent_hash: block::Hash,
    higher_parent_hash: block::Hash,
}

/// Returns two blocks on competing branches that provide the same non-coinbase outpoint.
///
/// `SemanticallyVerifiedBlock::new_outputs` explicitly permits unrelated outputs, so the
/// fixture can exercise cache ownership without constructing fully valid competing blocks.
fn shared_utxo_providers() -> Result<SharedUtxoProviders> {
    let root: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;
    let left_child: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.zcash_deserialize_into()?;

    let lower_block = left_child.make_fake_child();
    let right_child = root.make_fake_child();
    let right_grandchild = right_child.make_fake_child();
    let higher_block = right_grandchild.make_fake_child();

    let source = left_child.prepare();
    let (outpoint, source_output) = source
        .new_outputs
        .iter()
        .find(|(_outpoint, ordered_utxo)| !ordered_utxo.utxo.from_coinbase)
        .expect("mainnet block 419201 has non-coinbase transparent outputs");
    let outpoint = *outpoint;
    let output = source_output.utxo.output.clone();

    let mut lower = lower_block.prepare();
    let mut higher = higher_block.prepare();
    assert!(lower.height < higher.height);
    assert_ne!(lower.hash, higher.hash);

    let lower_output = transparent::OrderedUtxo::new(output.clone(), lower.height, 1);
    let higher_output = transparent::OrderedUtxo::new(output, higher.height, 1);
    let lower_utxo = lower_output.utxo.clone();
    let higher_utxo = higher_output.utxo.clone();

    lower.new_outputs.insert(outpoint, lower_output);
    higher.new_outputs.insert(outpoint, higher_output);

    let lower_parent_hash = lower.block.header.previous_block_hash;
    let higher_parent_hash = higher.block.header.previous_block_hash;
    assert_ne!(lower_parent_hash, higher_parent_hash);

    Ok(SharedUtxoProviders {
        lower,
        higher,
        outpoint,
        lower_utxo,
        higher_utxo,
        lower_parent_hash,
        higher_parent_hash,
    })
}

fn duplicate_only_output(
    original: &SemanticallyVerifiedBlock,
    other: &SemanticallyVerifiedBlock,
) -> (transparent::OutPoint, transparent::OrderedUtxo) {
    other
        .new_outputs
        .iter()
        .find(|(outpoint, _output)| !original.new_outputs.contains_key(outpoint))
        .map(|(outpoint, output)| (*outpoint, output.clone()))
        .expect("competing blocks have at least one distinct output")
}

fn sent_buffers_contain(sent: &SentHashes, hash: block::Hash) -> bool {
    sent.curr_buf
        .iter()
        .any(|(sent_hash, _height)| *sent_hash == hash)
        || sent
            .bufs
            .iter()
            .flatten()
            .any(|(sent_hash, _height)| *sent_hash == hash)
}

#[test]
fn dequeue_children_preserves_shared_utxo_until_last_provider() -> Result<()> {
    let _init_guard = zakura_test::init();

    for remove_lower_first in [true, false] {
        let providers = shared_utxo_providers()?;
        let mut queue = QueuedBlocks::default();
        queue.queue(providers.lower.clone().into_queued());
        queue.queue(providers.higher.clone().into_queued());

        let (first_parent, first_hash, remaining_utxo, last_parent, last_hash) =
            if remove_lower_first {
                (
                    providers.lower_parent_hash,
                    providers.lower.hash,
                    providers.higher_utxo.clone(),
                    providers.higher_parent_hash,
                    providers.higher.hash,
                )
            } else {
                (
                    providers.higher_parent_hash,
                    providers.higher.hash,
                    providers.lower_utxo.clone(),
                    providers.lower_parent_hash,
                    providers.lower.hash,
                )
            };

        let removed = queue.dequeue_children(first_parent);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0.hash, first_hash);
        assert_eq!(queue.utxo(&providers.outpoint), Some(remaining_utxo));

        let removed = queue.dequeue_children(last_parent);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0.hash, last_hash);
        assert_eq!(queue.utxo(&providers.outpoint), None);
        assert!(queue.blocks.is_empty());
        assert!(queue.by_parent.is_empty());
        assert!(queue.by_height.is_empty());
    }

    Ok(())
}

#[test]
fn queued_pruning_preserves_shared_utxo_until_last_provider() -> Result<()> {
    let _init_guard = zakura_test::init();
    let providers = shared_utxo_providers()?;

    let mut queue = QueuedBlocks::default();
    queue.queue(providers.lower.clone().into_queued());
    queue.queue(providers.higher.clone().into_queued());

    queue.prune_by_height(providers.lower.height);
    assert!(!queue.blocks.contains_key(&providers.lower.hash));
    assert!(queue.blocks.contains_key(&providers.higher.hash));
    assert_eq!(
        queue.utxo(&providers.outpoint),
        Some(providers.higher_utxo.clone())
    );

    queue.prune_by_height(providers.higher.height);
    assert_eq!(queue.utxo(&providers.outpoint), None);
    assert!(queue.blocks.is_empty());
    assert!(queue.by_parent.is_empty());
    assert!(queue.by_height.is_empty());

    Ok(())
}

#[test]
fn sent_pruning_preserves_shared_utxo_until_last_provider() -> Result<()> {
    let _init_guard = zakura_test::init();
    let providers = shared_utxo_providers()?;

    let mut sent = SentHashes::default();
    sent.add(&providers.lower);
    sent.add(&providers.higher);
    sent.finish_batch();

    sent.prune_by_height(providers.lower.height);
    assert!(!sent.contains(&providers.lower.hash));
    assert!(sent.contains(&providers.higher.hash));
    assert_eq!(
        sent.utxo(&providers.outpoint),
        Some(providers.higher_utxo.clone())
    );

    sent.prune_by_height(providers.higher.height);
    assert_eq!(sent.utxo(&providers.outpoint), None);
    assert!(sent.sent.is_empty());
    assert!(!sent_buffers_contain(&sent, providers.lower.hash));
    assert!(!sent_buffers_contain(&sent, providers.higher.hash));

    Ok(())
}

#[test]
fn sent_rejection_preserves_shared_utxo_until_last_provider() -> Result<()> {
    let _init_guard = zakura_test::init();

    for remove_lower_first in [true, false] {
        let providers = shared_utxo_providers()?;
        let mut sent = SentHashes::default();
        sent.add(&providers.lower);
        sent.add(&providers.higher);
        sent.finish_batch();

        let (first_hash, remaining_utxo, last_hash) = if remove_lower_first {
            (
                providers.lower.hash,
                providers.higher_utxo.clone(),
                providers.higher.hash,
            )
        } else {
            (
                providers.higher.hash,
                providers.lower_utxo.clone(),
                providers.lower.hash,
            )
        };

        sent.remove(&first_hash);
        assert!(!sent.contains(&first_hash));
        assert!(!sent_buffers_contain(&sent, first_hash));
        assert_eq!(sent.utxo(&providers.outpoint), Some(remaining_utxo));

        sent.remove(&last_hash);
        assert!(!sent.contains(&last_hash));
        assert!(!sent_buffers_contain(&sent, last_hash));
        assert_eq!(sent.utxo(&providers.outpoint), None);
        assert!(sent.sent.is_empty());
        assert!(sent.bufs.is_empty());
    }

    Ok(())
}

#[test]
fn duplicate_queued_block_is_idempotent() -> Result<()> {
    let _init_guard = zakura_test::init();
    let providers = shared_utxo_providers()?;
    let (duplicate_outpoint, duplicate_output) =
        duplicate_only_output(&providers.lower, &providers.higher);

    let mut duplicate = providers.lower.clone();
    duplicate.new_outputs.clear();
    duplicate
        .new_outputs
        .insert(duplicate_outpoint, duplicate_output);

    let mut queue = QueuedBlocks::default();
    queue.queue(providers.lower.clone().into_queued());
    queue.queue(duplicate.into_queued());

    assert_eq!(queue.blocks.len(), 1);
    assert_eq!(queue.by_parent.len(), 1);
    assert_eq!(queue.by_height.len(), 1);
    assert_eq!(
        queue.utxo(&providers.outpoint),
        Some(providers.lower_utxo.clone())
    );
    assert_eq!(queue.utxo(&duplicate_outpoint), None);

    queue.dequeue_children(providers.lower_parent_hash);
    assert_eq!(queue.utxo(&providers.outpoint), None);
    assert_eq!(queue.utxo(&duplicate_outpoint), None);
    assert!(queue.blocks.is_empty());
    assert!(queue.by_parent.is_empty());
    assert!(queue.by_height.is_empty());

    Ok(())
}

#[test]
fn duplicate_sent_block_is_idempotent() -> Result<()> {
    let _init_guard = zakura_test::init();
    let providers = shared_utxo_providers()?;
    let (duplicate_outpoint, duplicate_output) =
        duplicate_only_output(&providers.lower, &providers.higher);

    let mut duplicate = providers.lower.clone();
    duplicate.new_outputs.clear();
    duplicate
        .new_outputs
        .insert(duplicate_outpoint, duplicate_output);

    let mut sent = SentHashes::default();
    sent.add(&providers.lower);
    sent.add(&duplicate);

    assert_eq!(sent.sent.len(), 1);
    assert_eq!(
        sent.curr_buf
            .iter()
            .filter(|(hash, _height)| *hash == providers.lower.hash)
            .count(),
        1
    );
    assert_eq!(
        sent.utxo(&providers.outpoint),
        Some(providers.lower_utxo.clone())
    );
    assert_eq!(sent.utxo(&duplicate_outpoint), None);

    sent.remove(&providers.lower.hash);
    assert_eq!(sent.utxo(&providers.outpoint), None);
    assert_eq!(sent.utxo(&duplicate_outpoint), None);
    assert!(sent.sent.is_empty());
    assert!(!sent_buffers_contain(&sent, providers.lower.hash));

    Ok(())
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
        .any(|(block, _, _)| block.hash == child1.hash()));
    assert!(children
        .iter()
        .any(|(block, _, _)| block.hash == child2.hash()));
    assert_eq!(0, queue.blocks.len());
    assert_eq!(0, queue.by_parent.len());
    assert_eq!(0, queue.by_height.len());
    assert_eq!(0, queue.known_utxos.len());

    Ok(())
}

#[test]
fn same_hash_replacement_keeps_the_new_body() -> Result<()> {
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;
    let replacement_block = Arc::new((*block).clone());
    let mut queue = QueuedBlocks::default();
    queue.queue(block.clone().into_queued());

    let old = queue.replace(block.hash(), replacement_block.clone().into_queued());
    assert!(Arc::ptr_eq(&old.0.block, &block));
    assert!(Arc::ptr_eq(
        &queue
            .get_mut(&block.hash())
            .expect("replacement remains queued")
            .0
            .block,
        &replacement_block
    ));
    Ok(())
}

#[test]
fn orphan_queue_has_a_fixed_entry_bound() -> Result<()> {
    let block: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into()?;
    let mut queue = QueuedBlocks::default();
    assert!(!queue.is_full());

    for index in 0..MAX_QUEUED_BLOCKS {
        let mut queued = block.clone().into_queued();
        let index = u64::try_from(index).expect("the queue bound fits in u64");
        queued.0.hash.0[..8].copy_from_slice(&index.to_le_bytes());
        queue.blocks.insert(queued.0.hash, queued);
    }
    assert!(queue.is_full());
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
    assert!(queue.get_mut(&block1.hash()).is_none());
    assert!(queue.get_mut(&child1.hash()).is_some());
    assert!(queue.get_mut(&child2.hash()).is_some());
    assert_eq!(632, queue.known_utxos.len());

    // Pruning the children of the first block removes both of the other
    // blocks, and empties all lists
    queue.prune_by_height(child1.coinbase_height().unwrap());
    assert_eq!(0, queue.blocks.len());
    assert_eq!(0, queue.by_parent.len());
    assert_eq!(0, queue.by_height.len());
    assert!(queue.get_mut(&child1.hash()).is_none());
    assert!(queue.get_mut(&child2.hash()).is_none());
    assert_eq!(0, queue.known_utxos.len());

    Ok(())
}

/// `SentHashes::remove` must drop the hash, its outpoints from `known_utxos`,
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
        queue.queue((block.prepare(), response, None));
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
