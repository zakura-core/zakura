//! Parallel note commitment tree update methods.

use std::sync::Arc;

use thiserror::Error;

use crate::{
    block::Block,
    ironwood, orchard, sapling, sprout,
    subtree::{NoteCommitmentSubtree, NoteCommitmentSubtreeIndex},
};

/// An argument wrapper struct for note commitment trees.
///
/// The default instance represents the trees and subtrees that correspond to the genesis block.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NoteCommitmentTrees {
    /// The sprout note commitment tree.
    pub sprout: Arc<sprout::tree::NoteCommitmentTree>,

    /// The sapling note commitment tree.
    pub sapling: Arc<sapling::tree::NoteCommitmentTree>,

    /// The sapling note commitment subtree.
    pub sapling_subtree: Option<NoteCommitmentSubtree<sapling_crypto::Node>>,

    /// The orchard note commitment tree.
    pub orchard: Arc<orchard::tree::NoteCommitmentTree>,

    /// The orchard note commitment subtree.
    pub orchard_subtree: Option<NoteCommitmentSubtree<orchard::tree::Node>>,

    /// The Ironwood note commitment tree.
    pub ironwood: Arc<ironwood::tree::NoteCommitmentTree>,

    /// The Ironwood note commitment subtree.
    pub ironwood_subtree: Option<NoteCommitmentSubtree<ironwood::tree::Node>>,
}

/// Note commitment tree errors.
#[derive(Error, Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum NoteCommitmentTreeError {
    /// A sprout tree error
    #[error("sprout error: {0}")]
    Sprout(#[from] sprout::tree::NoteCommitmentTreeError),

    /// A sapling tree error
    #[error("sapling error: {0}")]
    Sapling(#[from] sapling::tree::NoteCommitmentTreeError),

    /// A orchard tree error
    #[error("orchard error: {0}")]
    Orchard(#[from] orchard::tree::NoteCommitmentTreeError),

    /// An Ironwood tree error.
    #[error("ironwood error: {0}")]
    Ironwood(ironwood::tree::NoteCommitmentTreeError),
}

impl NoteCommitmentTrees {
    /// Updates the note commitment trees using the transactions in `block`,
    /// then re-calculates the cached tree roots, using parallel `rayon` threads.
    ///
    /// If any of the tree updates cause an error,
    /// it will be returned at the end of the parallel batches.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_trees_parallel(
        &mut self,
        block: &Arc<Block>,
    ) -> Result<(), NoteCommitmentTreeError> {
        let block = block.clone();
        let height = block
            .coinbase_height()
            .expect("height was already validated");

        // Prepare arguments for parallel threads
        let NoteCommitmentTrees {
            sapling,
            orchard,
            ironwood,
            ..
        } = self.clone();

        let sapling_note_commitments: Vec<_> = block.sapling_note_commitments().cloned().collect();
        let orchard_note_commitments: Vec<_> = block.orchard_note_commitments().cloned().collect();
        let ironwood_note_commitments: Vec<_> =
            block.ironwood_note_commitments().cloned().collect();

        let mut sapling_result = None;
        let mut orchard_result = None;
        let mut ironwood_result = None;

        rayon::in_place_scope_fifo(|scope| {
            if !sapling_note_commitments.is_empty() {
                scope.spawn_fifo(|_scope| {
                    sapling_result = Some(Self::update_sapling_note_commitment_tree(
                        sapling,
                        sapling_note_commitments,
                    ));
                });
            }

            if !orchard_note_commitments.is_empty() {
                scope.spawn_fifo(|_scope| {
                    orchard_result = Some(Self::update_orchard_note_commitment_tree(
                        orchard,
                        orchard_note_commitments,
                    ));
                });
            }

            if !ironwood_note_commitments.is_empty() {
                scope.spawn_fifo(|_scope| {
                    ironwood_result = Some(Self::update_ironwood_note_commitment_tree(
                        ironwood,
                        ironwood_note_commitments,
                    ));
                });
            }
        });

        self.update_sprout_tree(&block)?;

        if let Some(sapling_result) = sapling_result {
            let (sapling, subtree_root) = sapling_result?;
            self.sapling = sapling;
            self.sapling_subtree =
                subtree_root.map(|(idx, node)| NoteCommitmentSubtree::new(idx, height, node));
        };

        if let Some(orchard_result) = orchard_result {
            let (orchard, subtree_root) = orchard_result?;
            self.orchard = orchard;
            self.orchard_subtree =
                subtree_root.map(|(idx, node)| NoteCommitmentSubtree::new(idx, height, node));
        };

        if let Some(ironwood_result) = ironwood_result {
            let (ironwood, subtree_root) = ironwood_result?;
            self.ironwood = ironwood;
            self.ironwood_subtree =
                subtree_root.map(|(idx, node)| NoteCommitmentSubtree::new(idx, height, node));
        };

        Ok(())
    }

    /// Updates the Sprout note commitment tree using the transactions in `block`.
    ///
    /// Returns immediately without cloning or recalculating the root when the block has
    /// no Sprout note commitments.
    pub fn update_sprout_tree(&mut self, block: &Block) -> Result<(), NoteCommitmentTreeError> {
        let sprout_note_commitments: Vec<_> = block.sprout_note_commitments().cloned().collect();
        if sprout_note_commitments.is_empty() {
            return Ok(());
        }

        self.sprout =
            Self::update_sprout_note_commitment_tree(self.sprout.clone(), sprout_note_commitments)?;

        Ok(())
    }

    /// Update the Sprout note commitment tree.
    /// This method modifies the tree inside the `Arc`, if the `Arc` only has one reference.
    fn update_sprout_note_commitment_tree(
        mut sprout: Arc<sprout::tree::NoteCommitmentTree>,
        sprout_note_commitments: Vec<sprout::NoteCommitment>,
    ) -> Result<Arc<sprout::tree::NoteCommitmentTree>, NoteCommitmentTreeError> {
        let sprout_nct = Arc::make_mut(&mut sprout);

        for sprout_note_commitment in sprout_note_commitments {
            sprout_nct.append(sprout_note_commitment)?;
        }

        // Re-calculate and cache the tree root.
        let _ = sprout_nct.root();

        Ok(sprout)
    }

    /// Update the sapling note commitment tree.
    /// This method modifies the tree inside the `Arc`, if the `Arc` only has one reference.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_sapling_note_commitment_tree(
        mut sapling: Arc<sapling::tree::NoteCommitmentTree>,
        sapling_note_commitments: Vec<sapling::tree::NoteCommitmentUpdate>,
    ) -> Result<
        (
            Arc<sapling::tree::NoteCommitmentTree>,
            Option<(NoteCommitmentSubtreeIndex, sapling_crypto::Node)>,
        ),
        NoteCommitmentTreeError,
    > {
        let sapling_nct = Arc::make_mut(&mut sapling);

        // It is impossible for blocks to contain more than one level 16 sapling root:
        // > [NU5 onward] nSpendsSapling, nOutputsSapling, and nActionsOrchard MUST all be less than 2^16.
        // <https://zips.z.cash/protocol/protocol.pdf#txnconsensus>
        //
        // Before NU5, this limit holds due to the minimum size of Sapling outputs (948 bytes)
        // and the maximum size of a block:
        // > The size of a block MUST be less than or equal to 2000000 bytes.
        // <https://zips.z.cash/protocol/protocol.pdf#blockheader>
        // <https://zips.z.cash/protocol/protocol.pdf#txnencoding>
        //
        // The note commitments are appended as a single parallel batch, which
        // returns the (at most one) subtree completed within this block, matching
        // the per-leaf append exactly (see `crate::parallel::batch_frontier`).
        let subtree_root = sapling_nct.append_batch(&sapling_note_commitments)?;

        // Re-calculate and cache the tree root.
        let _ = sapling_nct.root();

        Ok((sapling, subtree_root))
    }

    /// Update the orchard note commitment tree.
    /// This method modifies the tree inside the `Arc`, if the `Arc` only has one reference.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_orchard_note_commitment_tree(
        mut orchard: Arc<orchard::tree::NoteCommitmentTree>,
        orchard_note_commitments: Vec<orchard::tree::NoteCommitmentUpdate>,
    ) -> Result<
        (
            Arc<orchard::tree::NoteCommitmentTree>,
            Option<(NoteCommitmentSubtreeIndex, orchard::tree::Node)>,
        ),
        NoteCommitmentTreeError,
    > {
        let orchard_nct = Arc::make_mut(&mut orchard);

        // It is impossible for blocks to contain more than one level 16 orchard root:
        // > [NU5 onward] nSpendsSapling, nOutputsSapling, and nActionsOrchard MUST all be less than 2^16.
        // <https://zips.z.cash/protocol/protocol.pdf#txnconsensus>
        //
        // The note commitments are appended as a single parallel batch, which
        // returns the (at most one) subtree completed within this block, matching
        // the per-leaf append exactly (see `crate::parallel::batch_frontier`).
        let subtree_root = orchard_nct.append_batch(&orchard_note_commitments)?;

        // Re-calculate and cache the tree root.
        let _ = orchard_nct.root();

        Ok((orchard, subtree_root))
    }

    /// Update the Ironwood note commitment tree.
    /// This method modifies the tree inside the `Arc`, if the `Arc` only has one reference.
    #[allow(clippy::unwrap_in_result)]
    pub fn update_ironwood_note_commitment_tree(
        mut ironwood: Arc<ironwood::tree::NoteCommitmentTree>,
        ironwood_note_commitments: Vec<ironwood::tree::NoteCommitmentUpdate>,
    ) -> Result<
        (
            Arc<ironwood::tree::NoteCommitmentTree>,
            Option<(NoteCommitmentSubtreeIndex, ironwood::tree::Node)>,
        ),
        NoteCommitmentTreeError,
    > {
        let ironwood_nct = Arc::make_mut(&mut ironwood);

        // It is impossible for blocks to contain more than one level 16 Ironwood root:
        // > [NU6.3 onward] nActionsIronwood MUST be less than 2^16.
        // <https://zips.z.cash/protocol/protocol.pdf#txnconsensus>
        //
        // The note commitments are appended as a single parallel batch, which
        // returns the (at most one) subtree completed within this block, matching
        // the per-leaf append exactly (see `crate::parallel::batch_frontier`).
        let subtree_root = ironwood_nct
            .append_batch(&ironwood_note_commitments)
            .map_err(NoteCommitmentTreeError::Ironwood)?;

        // Re-calculate and cache the tree root.
        let _ = ironwood_nct.root();

        Ok((ironwood, subtree_root))
    }
}

#[cfg(test)]
mod tests {
    use halo2::pasta::pallas;

    use crate::{
        block::{Header, Height},
        parameters::{Network, NetworkUpgrade::Nu6_3},
        serialization::ZcashDeserializeInto,
        transaction::{arbitrary::v5_transactions, LockTime, Transaction},
        transparent,
    };

    use super::*;

    #[test]
    fn selective_sprout_update_skips_empty_blocks_and_matches_legacy_update() {
        let no_joinsplit: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("Mainnet block 1 deserializes");
        let first_joinsplit: Block = zakura_test::vectors::BLOCK_MAINNET_396_BYTES
            .zcash_deserialize_into()
            .expect("Mainnet block 396 deserializes");

        let mut selective = NoteCommitmentTrees::default();
        let empty = selective.sprout.clone();
        selective
            .update_sprout_tree(&no_joinsplit)
            .expect("an empty Sprout update succeeds");
        assert!(
            Arc::ptr_eq(&selective.sprout, &empty),
            "a block without JoinSplits performs no Sprout clone or update"
        );

        selective
            .update_sprout_tree(&first_joinsplit)
            .expect("the first JoinSplit commitments fit");
        assert_eq!(selective.sprout.count(), 2);

        let mut legacy = NoteCommitmentTrees::default();
        legacy
            .update_trees_parallel(&Arc::new(first_joinsplit))
            .expect("the legacy all-tree update succeeds");
        assert_eq!(selective.sprout, legacy.sprout);
    }

    /// `update_trees_parallel` must feed each pool's note commitments to its
    /// own tree, in transaction order, and must not touch unrelated trees.
    ///
    /// The block's two transactions carry the commitments `1` and `2` in
    /// opposite pools, so a pool mix-up or reordering would change the
    /// resulting Orchard or Ironwood tree root.
    #[test]
    fn parallel_block_update_keeps_orchard_and_ironwood_order_and_assignment() {
        let _init_guard = zakura_test::init();

        let mut orchard_data = Network::iter()
            .flat_map(|network| v5_transactions(network.block_iter()))
            .find_map(|transaction| transaction.orchard_shielded_data().cloned())
            .expect("test vectors include an Orchard transaction");
        // Ironwood reuses the Orchard bundle encoding, so an Orchard test
        // bundle can be cloned into the Ironwood slot of a V6 transaction.
        let mut ironwood_data = orchard_data.clone();

        let mut orchard_actions = orchard_data.actions.as_slice().to_vec();
        orchard_actions.truncate(1);
        orchard_actions[0].action.cm_x = pallas::Base::from(1);
        orchard_data.actions = orchard_actions
            .try_into()
            .expect("the test bundle has at least one action");

        let mut ironwood_actions = ironwood_data.actions.as_slice().to_vec();
        ironwood_actions.truncate(1);
        ironwood_actions[0].action.cm_x = pallas::Base::from(2);
        ironwood_data.actions = ironwood_actions
            .try_into()
            .expect("the test bundle has at least one action");

        let make_transaction =
            |orchard_shielded_data: orchard::ShieldedData,
             ironwood_shielded_data: ironwood::ShieldedData| {
                Arc::new(Transaction::V6 {
                    network_upgrade: Nu6_3,
                    lock_time: LockTime::unlocked(),
                    expiry_height: Height(1),
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                    sapling_shielded_data: None,
                    orchard_shielded_data: Some(orchard_shielded_data),
                    ironwood_shielded_data: Some(ironwood_shielded_data),
                })
            };

        let height = Height(123);
        let coinbase = Arc::new(Transaction::V6 {
            network_upgrade: Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: height,
            inputs: vec![transparent::Input::Coinbase {
                height,
                data: Vec::new(),
                sequence: u32::MAX,
            }],
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        });
        let header: Header = zakura_test::vectors::DUMMY_HEADER
            .zcash_deserialize_into()
            .expect("dummy header should deserialize");
        let block = Arc::new(Block {
            header: Arc::new(header),
            transactions: vec![
                coinbase,
                make_transaction(orchard_data.clone(), ironwood_data.clone()),
                make_transaction(ironwood_data, orchard_data),
            ],
        });

        let orchard_commitments: Vec<_> = block.orchard_note_commitments().copied().collect();
        let ironwood_commitments: Vec<_> = block.ironwood_note_commitments().copied().collect();
        assert_eq!(
            orchard_commitments,
            [pallas::Base::from(1), pallas::Base::from(2)]
        );
        assert_eq!(
            ironwood_commitments,
            [pallas::Base::from(2), pallas::Base::from(1)]
        );

        let mut expected_orchard = orchard::tree::NoteCommitmentTree::default();
        for commitment in &orchard_commitments {
            expected_orchard
                .append(*commitment)
                .expect("two Orchard commitments fit in an empty tree");
        }
        let mut expected_ironwood = ironwood::tree::NoteCommitmentTree::default();
        for commitment in &ironwood_commitments {
            expected_ironwood
                .append(*commitment)
                .expect("two Ironwood commitments fit in an empty tree");
        }
        assert_ne!(expected_orchard.root(), expected_ironwood.root());

        let mut trees = NoteCommitmentTrees::default();
        let unchanged_sprout = trees.sprout.clone();
        let unchanged_sapling = trees.sapling.clone();
        trees
            .update_trees_parallel(&block)
            .expect("the mixed-pool block fits in empty trees");

        assert_eq!(trees.sprout, unchanged_sprout);
        assert_eq!(trees.sapling, unchanged_sapling);
        assert_eq!(trees.orchard.root(), expected_orchard.root());
        assert_eq!(trees.ironwood.root(), expected_ironwood.root());
        assert_eq!(trees.orchard.count(), 2);
        assert_eq!(trees.ironwood.count(), 2);
        assert_eq!(trees.orchard_subtree, None);
        assert_eq!(trees.ironwood_subtree, None);
        assert!(!Arc::ptr_eq(&trees.orchard, &trees.ironwood));
    }

    /// Updating one pool's note commitment tree must not affect the other's,
    /// even though the two pools share the same tree implementation and their
    /// empty trees have equal roots.
    #[test]
    fn orchard_and_ironwood_trees_remain_independent() {
        let mut trees = NoteCommitmentTrees::default();
        let empty_orchard_root = trees.orchard.root();
        let empty_ironwood_root = trees.ironwood.root();

        assert_eq!(empty_orchard_root, empty_ironwood_root);
        assert!(!Arc::ptr_eq(&trees.orchard, &trees.ironwood));

        let (ironwood, ironwood_subtree) =
            NoteCommitmentTrees::update_ironwood_note_commitment_tree(
                trees.ironwood.clone(),
                vec![pallas::Base::from(1)],
            )
            .expect("one Ironwood commitment fits in an empty tree");
        trees.ironwood = ironwood;

        assert_eq!(ironwood_subtree, None);
        assert_eq!(trees.orchard.root(), empty_orchard_root);
        assert_ne!(trees.ironwood.root(), empty_ironwood_root);

        let ironwood_root = trees.ironwood.root();
        let (orchard, orchard_subtree) = NoteCommitmentTrees::update_orchard_note_commitment_tree(
            trees.orchard.clone(),
            vec![pallas::Base::from(2)],
        )
        .expect("one Orchard commitment fits in an empty tree");
        trees.orchard = orchard;

        assert_eq!(orchard_subtree, None);
        assert_eq!(trees.ironwood.root(), ironwood_root);
        assert_ne!(trees.orchard.root(), trees.ironwood.root());
    }

    /// Tree errors must name the pool they came from: the `From` conversion
    /// maps the shared inner error type to the Orchard variant, so Ironwood
    /// errors must be constructed explicitly and display as Ironwood.
    #[test]
    fn orchard_and_ironwood_tree_errors_keep_their_pool_identity() {
        let orchard_error =
            NoteCommitmentTreeError::from(orchard::tree::NoteCommitmentTreeError::FullTree);
        let ironwood_error =
            NoteCommitmentTreeError::Ironwood(ironwood::tree::NoteCommitmentTreeError::FullTree);

        assert!(matches!(orchard_error, NoteCommitmentTreeError::Orchard(_)));
        assert!(matches!(
            ironwood_error,
            NoteCommitmentTreeError::Ironwood(_)
        ));
        assert_eq!(
            ironwood_error.to_string(),
            "ironwood error: The note commitment tree is full"
        );
    }
}
