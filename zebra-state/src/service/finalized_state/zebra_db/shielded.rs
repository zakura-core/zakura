//! Provides high-level access to database shielded:
//! - nullifiers
//! - note commitment trees
//! - anchors
//!
//! This module makes sure that:
//! - all disk writes happen inside a RocksDB transaction, and
//! - format-specific invariants are maintained.
//!
//! # Correctness
//!
//! [`crate::constants::state_database_format_version_in_code()`] must be incremented
//! each time the database format (column, serialization, etc) changes.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use std::ops::RangeInclusive;

use zebra_chain::{
    block::{merkle::AuthDataRoot, Height},
    ironwood, orchard,
    parallel::{commitment_aux::BlockCommitmentRoots, tree::NoteCommitmentTrees},
    sapling, sprout,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    transaction::Transaction,
};

use crate::{
    request::{FinalizedBlock, Treestate},
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        disk_format::{shielded::CommitmentRootsByHeight, RawBytes},
        zebra_db::{block::VctData, ZebraDb},
        COMMITMENT_ROOTS_BY_HEIGHT,
    },
    TransactionLocation,
};

// Doc-only items
#[allow(unused_imports)]
use zebra_chain::subtree::NoteCommitmentSubtree;

impl ZebraDb {
    // Read shielded methods

    /// Returns `true` if the finalized state contains `sprout_nullifier`.
    pub fn contains_sprout_nullifier(&self, sprout_nullifier: &sprout::Nullifier) -> bool {
        let sprout_nullifiers = self.db.cf_handle("sprout_nullifiers").unwrap();
        self.db.zs_contains(&sprout_nullifiers, &sprout_nullifier)
    }

    /// Returns `true` if the finalized state contains `sapling_nullifier`.
    pub fn contains_sapling_nullifier(&self, sapling_nullifier: &sapling::Nullifier) -> bool {
        let sapling_nullifiers = self.db.cf_handle("sapling_nullifiers").unwrap();
        self.db.zs_contains(&sapling_nullifiers, &sapling_nullifier)
    }

    /// Returns `true` if the finalized state contains `orchard_nullifier`.
    pub fn contains_orchard_nullifier(&self, orchard_nullifier: &orchard::Nullifier) -> bool {
        let orchard_nullifiers = self.db.cf_handle("orchard_nullifiers").unwrap();
        self.db.zs_contains(&orchard_nullifiers, &orchard_nullifier)
    }

    /// Returns `true` if the finalized state contains `ironwood_nullifier`.
    pub fn contains_ironwood_nullifier(&self, ironwood_nullifier: &ironwood::Nullifier) -> bool {
        let ironwood_nullifiers = self.db.cf_handle("ironwood_nullifiers").unwrap();
        self.db
            .zs_contains(&ironwood_nullifiers, &ironwood_nullifier)
    }

    /// Returns the [`TransactionLocation`] of the transaction that revealed
    /// the given [`sprout::Nullifier`], if it is revealed in the finalized state and its
    /// spending transaction hash has been indexed.
    #[allow(clippy::unwrap_in_result)]
    pub fn sprout_revealing_tx_loc(
        &self,
        sprout_nullifier: &sprout::Nullifier,
    ) -> Option<TransactionLocation> {
        let sprout_nullifiers = self.db.cf_handle("sprout_nullifiers").unwrap();
        self.db.zs_get(&sprout_nullifiers, &sprout_nullifier)?
    }

    /// Returns the [`TransactionLocation`] of the transaction that revealed
    /// the given [`sapling::Nullifier`], if it is revealed in the finalized state and its
    /// spending transaction hash has been indexed.
    #[allow(clippy::unwrap_in_result)]
    pub fn sapling_revealing_tx_loc(
        &self,
        sapling_nullifier: &sapling::Nullifier,
    ) -> Option<TransactionLocation> {
        let sapling_nullifiers = self.db.cf_handle("sapling_nullifiers").unwrap();
        self.db.zs_get(&sapling_nullifiers, &sapling_nullifier)?
    }

    /// Returns the [`TransactionLocation`] of the transaction that revealed
    /// the given [`orchard::Nullifier`], if it is revealed in the finalized state and its
    /// spending transaction hash has been indexed.
    #[allow(clippy::unwrap_in_result)]
    pub fn orchard_revealing_tx_loc(
        &self,
        orchard_nullifier: &orchard::Nullifier,
    ) -> Option<TransactionLocation> {
        let orchard_nullifiers = self.db.cf_handle("orchard_nullifiers").unwrap();
        self.db.zs_get(&orchard_nullifiers, &orchard_nullifier)?
    }

    /// Returns the [`TransactionLocation`] of the transaction that revealed
    /// the given [`ironwood::Nullifier`], if it is revealed in the finalized state and its
    /// spending transaction hash has been indexed.
    #[allow(clippy::unwrap_in_result)]
    pub fn ironwood_revealing_tx_loc(
        &self,
        ironwood_nullifier: &ironwood::Nullifier,
    ) -> Option<TransactionLocation> {
        let ironwood_nullifiers = self.db.cf_handle("ironwood_nullifiers").unwrap();
        self.db.zs_get(&ironwood_nullifiers, &ironwood_nullifier)?
    }

    /// Returns `true` if the finalized state contains `sprout_anchor`.
    #[allow(dead_code)]
    pub fn contains_sprout_anchor(&self, sprout_anchor: &sprout::tree::Root) -> bool {
        let sprout_anchors = self.db.cf_handle("sprout_anchors").unwrap();
        self.db.zs_contains(&sprout_anchors, &sprout_anchor)
    }

    /// Returns `true` if the finalized state contains `sapling_anchor`.
    pub fn contains_sapling_anchor(&self, sapling_anchor: &sapling::tree::Root) -> bool {
        let sapling_anchors = self.db.cf_handle("sapling_anchors").unwrap();
        self.db.zs_contains(&sapling_anchors, &sapling_anchor)
    }

    /// Returns `true` if the finalized state contains `orchard_anchor`.
    pub fn contains_orchard_anchor(&self, orchard_anchor: &orchard::tree::Root) -> bool {
        let orchard_anchors = self.db.cf_handle("orchard_anchors").unwrap();
        self.db.zs_contains(&orchard_anchors, &orchard_anchor)
    }

    /// Returns the per-block Sapling/Orchard commitment roots stored in the
    /// `commitment_roots_by_height` serving index for the **contiguous** prefix of `range`
    /// that is present, in ascending height order (design §4).
    ///
    /// Reads stop at the first absent height, so the result is always a gap-free run from
    /// `range.start()` — exactly what the `tree_aux` `BlockRoots` serve and `fetch_roots`
    /// client expect. A node populates this index for every block it commits (fast or
    /// legacy), so a fast-synced node — which holds no per-height trees — can still serve
    /// roots here. Returns an empty vec for a database written before the index existed
    /// (e.g. a pre-index archive node), where the caller falls back to `produce_block_roots`.
    pub fn commitment_roots_by_height_range(
        &self,
        range: RangeInclusive<Height>,
    ) -> Vec<BlockCommitmentRoots> {
        let cf = self.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT).unwrap();
        let mut roots = Vec::new();
        for height in (range.start().0..=range.end().0).map(Height) {
            let Some(value) = self
                .db
                .zs_get::<_, _, CommitmentRootsByHeight>(&cf, &height)
            else {
                break;
            };
            roots.push(BlockCommitmentRoots {
                height,
                sapling_root: value.sapling,
                orchard_root: value.orchard,
                ironwood_root: value.ironwood,
                sapling_tx: value.sapling_tx,
                orchard_tx: value.orchard_tx,
                ironwood_tx: value.ironwood_tx,
                auth_data_root: value.auth_data_root,
            });
        }
        roots
    }

    /// POC: returns `(sapling_count, sapling_digest, orchard_count, orchard_digest)`,
    /// a deterministic, order-independent digest of the Sapling and Orchard anchor
    /// sets. Two syncs that produce the same anchor sets produce the same digest,
    /// even if one took the fast (skip-recompute) path. See
    /// `docs/design/verified-commitment-trees.md`.
    pub fn vct_anchor_digest(&self) -> (u64, u64, u64, u64) {
        use crate::service::finalized_state::IntoDisk;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let sapling_anchors = self.db.cf_handle("sapling_anchors").unwrap();
        let mut sapling_hasher = DefaultHasher::new();
        let mut sapling_count = 0u64;
        for (root, ()) in self
            .db
            .zs_forward_range_iter::<_, sapling::tree::Root, (), _>(&sapling_anchors, ..)
        {
            IntoDisk::as_bytes(&root).hash(&mut sapling_hasher);
            sapling_count += 1;
        }

        let orchard_anchors = self.db.cf_handle("orchard_anchors").unwrap();
        let mut orchard_hasher = DefaultHasher::new();
        let mut orchard_count = 0u64;
        for (root, ()) in self
            .db
            .zs_forward_range_iter::<_, orchard::tree::Root, (), _>(&orchard_anchors, ..)
        {
            IntoDisk::as_bytes(&root).hash(&mut orchard_hasher);
            orchard_count += 1;
        }

        (
            sapling_count,
            sapling_hasher.finish(),
            orchard_count,
            orchard_hasher.finish(),
        )
    }

    /// Returns `true` if the finalized state contains `ironwood_anchor`.
    pub fn contains_ironwood_anchor(&self, ironwood_anchor: &ironwood::tree::Root) -> bool {
        let ironwood_anchors = self.db.cf_handle("ironwood_anchors").unwrap();
        self.db.zs_contains(&ironwood_anchors, &ironwood_anchor)
    }

    // # Sprout trees

    /// Returns the Sprout note commitment tree of the finalized tip
    /// or the empty tree if the state is empty.
    pub fn sprout_tree_for_tip(&self) -> Arc<sprout::tree::NoteCommitmentTree> {
        if self.is_empty() {
            return Arc::<sprout::tree::NoteCommitmentTree>::default();
        }

        let sprout_tree_cf = self.db.cf_handle("sprout_note_commitment_tree").unwrap();

        // # Backwards Compatibility
        //
        // This code can read the column family format in 1.2.0 and earlier (tip height key),
        // and after PR #7392 is merged (empty key). The height-based code can be removed when
        // versions 1.2.0 and earlier are no longer supported.
        //
        // # Concurrency
        //
        // There is only one entry in this column family, which is atomically updated by a block
        // write batch (database transaction). If we used a height as the column family tree,
        // any updates between reading the tip height and reading the tree could cause panics.
        //
        // So we use the empty key `()`. Since the key has a constant value, we will always read
        // the latest tree.
        let mut sprout_tree: Option<Arc<sprout::tree::NoteCommitmentTree>> =
            self.db.zs_get(&sprout_tree_cf, &());

        if sprout_tree.is_none() {
            // In Zebra 1.4.0 and later, we don't update the sprout tip tree unless it is changed.
            // And we write with a `()` key, not a height key.
            // So we need to look for the most recent update height if the `()` key has never been written.
            sprout_tree = self
                .db
                .zs_last_key_value(&sprout_tree_cf)
                .map(|(_key, tree_value): (Height, _)| tree_value);
        }

        sprout_tree.unwrap_or_else(|| {
            // While a fast sync is in progress (tip below the handoff height), the
            // sprout tip tree is only written at the handoff; the committer does not
            // read it before then.
            assert!(
                self.finalized_tip_height()
                    .is_some_and(|tip| self.vct_tree_absent(tip)),
                "Sprout note commitment tree must exist if there is a finalized tip"
            );
            Arc::<sprout::tree::NoteCommitmentTree>::default()
        })
    }

    /// Returns the Sprout note commitment tree matching the given anchor.
    ///
    /// This is used for interstitial tree building, which is unique to Sprout.
    #[allow(clippy::unwrap_in_result)]
    pub fn sprout_tree_by_anchor(
        &self,
        sprout_anchor: &sprout::tree::Root,
    ) -> Option<Arc<sprout::tree::NoteCommitmentTree>> {
        let sprout_anchors_handle = self.db.cf_handle("sprout_anchors").unwrap();

        self.db
            .zs_get(&sprout_anchors_handle, sprout_anchor)
            .map(Arc::new)
    }

    /// Returns all the Sprout note commitment trees in the database.
    ///
    /// Calling this method can load a lot of data into RAM, and delay block commit transactions.
    #[allow(dead_code)]
    pub fn sprout_trees_full_map(
        &self,
    ) -> HashMap<sprout::tree::Root, Arc<sprout::tree::NoteCommitmentTree>> {
        let sprout_anchors_handle = self.db.cf_handle("sprout_anchors").unwrap();

        self.db
            .zs_items_in_range_unordered(&sprout_anchors_handle, ..)
    }

    /// Returns all the Sprout note commitment tip trees.
    /// We only store the sprout tree for the tip, so this method is mainly used in tests.
    pub fn sprout_trees_full_tip(
        &self,
    ) -> impl Iterator<Item = (RawBytes, Arc<sprout::tree::NoteCommitmentTree>)> + '_ {
        let sprout_trees = self.db.cf_handle("sprout_note_commitment_tree").unwrap();
        self.db.zs_forward_range_iter(&sprout_trees, ..)
    }

    // # Sapling trees

    /// Returns the Sapling note commitment tree of the finalized tip or the empty tree if the state
    /// is empty.
    pub fn sapling_tree_for_tip(&self) -> Arc<sapling::tree::NoteCommitmentTree> {
        let height = match self.finalized_tip_height() {
            Some(h) => h,
            None => return Default::default(),
        };

        self.sapling_tree_by_height(&height).unwrap_or_else(|| {
            // While a fast sync is in progress the tip is in the absent band and its
            // frontier is not stored; the committer does not read it (it folds
            // verified roots). Every other caller reaches here only below the upgrade
            // height or at/above the handoff, where the tree is present.
            assert!(
                self.vct_tree_absent(height),
                "Sapling note commitment tree must exist if there is a finalized tip"
            );
            Default::default()
        })
    }

    /// Returns the Sapling note commitment tree matching the given block height, or `None` if the
    /// height is above the finalized tip.
    #[allow(clippy::unwrap_in_result)]
    pub fn sapling_tree_by_height(
        &self,
        height: &Height,
    ) -> Option<Arc<sapling::tree::NoteCommitmentTree>> {
        let tip_height = self.finalized_tip_height()?;

        // If we're above the tip, searching backwards would always return the tip tree.
        // But the correct answer is "we don't know that tree yet".
        if *height > tip_height {
            return None;
        }

        // On a verified-commitment-trees fast-synced database, the per-height trees within the
        // `[U, H)` absent band were never written. Return `None` rather than letting the backward
        // search return a stale tree from an earlier height; trees below the upgrade height `U`
        // (pre-upgrade) and at/above the handoff `H` (semantic sync) are present.
        if self.vct_tree_absent(*height) {
            return None;
        }

        let sapling_trees = self.db.cf_handle("sapling_note_commitment_tree").unwrap();

        // If we know there must be a tree, search backwards for it.
        let (_first_duplicate_height, tree) = self
            .db
            .zs_prev_key_value_back_from(&sapling_trees, height)
            .expect(
                "Sapling note commitment trees must exist for all heights below the finalized tip",
            );

        Some(Arc::new(tree))
    }

    /// Returns the Sapling note commitment trees in the supplied range, in increasing height order.
    pub fn sapling_tree_by_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<sapling::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let sapling_trees = self.db.cf_handle("sapling_note_commitment_tree").unwrap();
        self.db.zs_forward_range_iter(&sapling_trees, range)
    }

    /// Returns the Sapling note commitment trees in the reversed range, in decreasing height order.
    pub fn sapling_tree_by_reversed_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<sapling::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let sapling_trees = self.db.cf_handle("sapling_note_commitment_tree").unwrap();
        self.db.zs_reverse_range_iter(&sapling_trees, range)
    }

    /// Returns the Sapling note commitment subtree at this `index`.
    ///
    /// # Correctness
    ///
    /// This method should not be used to get subtrees for RPC responses,
    /// because those subtree lists require that the start subtree is present in the list.
    /// Instead, use `sapling_subtree_list_by_index_for_rpc()`.
    #[allow(clippy::unwrap_in_result)]
    pub(in super::super) fn sapling_subtree_by_index(
        &self,
        index: impl Into<NoteCommitmentSubtreeIndex> + Copy,
    ) -> Option<NoteCommitmentSubtree<sapling_crypto::Node>> {
        let sapling_subtrees = self
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();

        let subtree_data: NoteCommitmentSubtreeData<sapling_crypto::Node> =
            self.db.zs_get(&sapling_subtrees, &index.into())?;

        Some(subtree_data.with_index(index))
    }

    /// Returns a list of Sapling [`NoteCommitmentSubtree`]s in the provided range.
    #[allow(clippy::unwrap_in_result)]
    pub fn sapling_subtree_list_by_index_range(
        &self,
        range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex>,
    ) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<sapling_crypto::Node>> {
        let sapling_subtrees = self
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();

        self.db
            .zs_forward_range_iter(&sapling_subtrees, range)
            .collect()
    }

    /// Get the sapling note commitment subtress for the finalized tip.
    #[allow(clippy::unwrap_in_result)]
    fn sapling_subtree_for_tip(&self) -> Option<NoteCommitmentSubtree<sapling_crypto::Node>> {
        let sapling_subtrees = self
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();

        let (index, subtree_data): (
            NoteCommitmentSubtreeIndex,
            NoteCommitmentSubtreeData<sapling_crypto::Node>,
        ) = self.db.zs_last_key_value(&sapling_subtrees)?;

        let tip_height = self.finalized_tip_height()?;
        if subtree_data.end_height != tip_height {
            return None;
        }

        Some(subtree_data.with_index(index))
    }

    // Orchard trees

    /// Returns the Orchard note commitment tree of the finalized tip or the empty tree if the state
    /// is empty.
    pub fn orchard_tree_for_tip(&self) -> Arc<orchard::tree::NoteCommitmentTree> {
        let height = match self.finalized_tip_height() {
            Some(h) => h,
            None => return Default::default(),
        };

        self.orchard_tree_by_height(&height).unwrap_or_else(|| {
            // See `sapling_tree_for_tip`: the fast-sync tip frontier in the absent
            // band is not stored and not read by the committer.
            assert!(
                self.vct_tree_absent(height),
                "Orchard note commitment tree must exist if there is a finalized tip"
            );
            Default::default()
        })
    }

    /// Returns the Orchard note commitment tree matching the given block height,
    /// or `None` if the height is above the finalized tip.
    #[allow(clippy::unwrap_in_result)]
    pub fn orchard_tree_by_height(
        &self,
        height: &Height,
    ) -> Option<Arc<orchard::tree::NoteCommitmentTree>> {
        let tip_height = self.finalized_tip_height()?;

        // If we're above the tip, searching backwards would always return the tip tree.
        // But the correct answer is "we don't know that tree yet".
        if *height > tip_height {
            return None;
        }

        // On a verified-commitment-trees fast-synced database, the per-height trees within the
        // `[U, H)` absent band were never written. Return `None` rather than letting the backward
        // search return a stale tree from an earlier height; trees below the upgrade height `U`
        // (pre-upgrade) and at/above the handoff `H` (semantic sync) are present.
        if self.vct_tree_absent(*height) {
            return None;
        }

        let orchard_trees = self.db.cf_handle("orchard_note_commitment_tree").unwrap();

        // If we know there must be a tree, search backwards for it.
        let (_first_duplicate_height, tree) = self
            .db
            .zs_prev_key_value_back_from(&orchard_trees, height)
            .expect(
                "Orchard note commitment trees must exist for all heights below the finalized tip",
            );

        Some(Arc::new(tree))
    }

    /// Returns the Orchard note commitment trees in the supplied range, in increasing height order.
    pub fn orchard_tree_by_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<orchard::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let orchard_trees = self.db.cf_handle("orchard_note_commitment_tree").unwrap();
        self.db.zs_forward_range_iter(&orchard_trees, range)
    }

    /// Returns the Orchard note commitment trees in the reversed range, in decreasing height order.
    pub fn orchard_tree_by_reversed_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<orchard::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let orchard_trees = self.db.cf_handle("orchard_note_commitment_tree").unwrap();
        self.db.zs_reverse_range_iter(&orchard_trees, range)
    }

    /// Returns the Orchard note commitment subtree at this `index`.
    ///
    /// # Correctness
    ///
    /// This method should not be used to get subtrees for RPC responses,
    /// because those subtree lists require that the start subtree is present in the list.
    /// Instead, use `orchard_subtree_list_by_index_for_rpc()`.
    #[allow(clippy::unwrap_in_result)]
    pub(in super::super) fn orchard_subtree_by_index(
        &self,
        index: impl Into<NoteCommitmentSubtreeIndex> + Copy,
    ) -> Option<NoteCommitmentSubtree<orchard::tree::Node>> {
        let orchard_subtrees = self
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();

        let subtree_data: NoteCommitmentSubtreeData<orchard::tree::Node> =
            self.db.zs_get(&orchard_subtrees, &index.into())?;

        Some(subtree_data.with_index(index))
    }

    /// Returns a list of Orchard [`NoteCommitmentSubtree`]s in the provided range.
    #[allow(clippy::unwrap_in_result)]
    pub fn orchard_subtree_list_by_index_range(
        &self,
        range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex>,
    ) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<orchard::tree::Node>> {
        let orchard_subtrees = self
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();

        self.db
            .zs_forward_range_iter(&orchard_subtrees, range)
            .collect()
    }

    /// Get the orchard note commitment subtress for the finalized tip.
    #[allow(clippy::unwrap_in_result)]
    fn orchard_subtree_for_tip(&self) -> Option<NoteCommitmentSubtree<orchard::tree::Node>> {
        let orchard_subtrees = self
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();

        let (index, subtree_data): (
            NoteCommitmentSubtreeIndex,
            NoteCommitmentSubtreeData<orchard::tree::Node>,
        ) = self.db.zs_last_key_value(&orchard_subtrees)?;

        let tip_height = self.finalized_tip_height()?;
        if subtree_data.end_height != tip_height {
            return None;
        }

        Some(subtree_data.with_index(index))
    }

    // Ironwood trees

    /// Returns the Ironwood note commitment tree of the finalized tip or the empty tree if the
    /// state is empty.
    pub fn ironwood_tree_for_tip(&self) -> Arc<ironwood::tree::NoteCommitmentTree> {
        let height = match self.finalized_tip_height() {
            Some(h) => h,
            None => return Default::default(),
        };

        self.ironwood_tree_by_height(&height)
            .expect("Ironwood note commitment tree must exist if there is a finalized tip")
    }

    /// Returns the Ironwood note commitment tree matching the given block height,
    /// or `None` if the height is above the finalized tip.
    #[allow(clippy::unwrap_in_result)]
    pub fn ironwood_tree_by_height(
        &self,
        height: &Height,
    ) -> Option<Arc<ironwood::tree::NoteCommitmentTree>> {
        let tip_height = self.finalized_tip_height()?;

        if *height > tip_height {
            return None;
        }

        let ironwood_trees = self.db.cf_handle("ironwood_note_commitment_tree").unwrap();

        let (_first_duplicate_height, tree) = self
            .db
            .zs_prev_key_value_back_from(&ironwood_trees, height)
            .expect(
                "Ironwood note commitment trees must exist for all heights below the finalized tip",
            );

        Some(Arc::new(tree))
    }

    /// Returns the Ironwood note commitment trees in the supplied range, in increasing height order.
    pub fn ironwood_tree_by_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<ironwood::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let ironwood_trees = self.db.cf_handle("ironwood_note_commitment_tree").unwrap();
        self.db.zs_forward_range_iter(&ironwood_trees, range)
    }

    /// Returns a list of Ironwood [`NoteCommitmentSubtree`]s in the provided range.
    #[allow(clippy::unwrap_in_result)]
    pub fn ironwood_subtree_list_by_index_range(
        &self,
        range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex>,
    ) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<ironwood::tree::Node>> {
        let ironwood_subtrees = self
            .db
            .cf_handle("ironwood_note_commitment_subtree")
            .unwrap();

        self.db
            .zs_forward_range_iter(&ironwood_subtrees, range)
            .collect()
    }

    /// Get the Ironwood note commitment subtree for the finalized tip.
    #[allow(clippy::unwrap_in_result)]
    fn ironwood_subtree_for_tip(&self) -> Option<NoteCommitmentSubtree<ironwood::tree::Node>> {
        let ironwood_subtrees = self
            .db
            .cf_handle("ironwood_note_commitment_subtree")
            .unwrap();

        let (index, subtree_data): (
            NoteCommitmentSubtreeIndex,
            NoteCommitmentSubtreeData<ironwood::tree::Node>,
        ) = self.db.zs_last_key_value(&ironwood_subtrees)?;

        let tip_height = self.finalized_tip_height()?;
        if subtree_data.end_height != tip_height {
            return None;
        }

        Some(subtree_data.with_index(index))
    }

    /// Returns the shielded note commitment trees of the finalized tip
    /// or the empty trees if the state is empty.
    /// Additionally, returns the sapling and orchard subtrees for the finalized tip if
    /// the current subtree is finalizing in the tip, None otherwise.
    pub fn note_commitment_trees_for_tip(&self) -> NoteCommitmentTrees {
        NoteCommitmentTrees {
            sprout: self.sprout_tree_for_tip(),
            sapling: self.sapling_tree_for_tip(),
            sapling_subtree: self.sapling_subtree_for_tip(),
            orchard: self.orchard_tree_for_tip(),
            orchard_subtree: self.orchard_subtree_for_tip(),
            ironwood: self.ironwood_tree_for_tip(),
            ironwood_subtree: self.ironwood_subtree_for_tip(),
        }
    }
}

impl DiskWriteBatch {
    /// Prepare a database batch containing `finalized.block`'s shielded transaction indexes,
    /// and return it (without actually writing anything).
    ///
    /// If this method returns an error, it will be propagated,
    /// and the batch should not be written to the database.
    pub fn prepare_shielded_transaction_batch(
        &mut self,
        zebra_db: &ZebraDb,
        finalized: &FinalizedBlock,
    ) {
        #[cfg(feature = "indexer")]
        let FinalizedBlock { block, height, .. } = finalized;

        // Index each transaction's shielded data
        #[cfg(feature = "indexer")]
        for (tx_index, transaction) in block.transactions.iter().enumerate() {
            let tx_loc = TransactionLocation::from_usize(*height, tx_index);
            self.prepare_nullifier_batch(zebra_db, transaction, tx_loc);
        }

        #[cfg(not(feature = "indexer"))]
        for transaction in &finalized.block.transactions {
            self.prepare_nullifier_batch(zebra_db, transaction);
        }
    }

    /// Prepare a database batch containing `finalized.block`'s nullifiers,
    /// and return it (without actually writing anything).
    ///
    /// # Errors
    ///
    /// - This method doesn't currently return any errors, but it might in future
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_nullifier_batch(
        &mut self,
        zebra_db: &ZebraDb,
        transaction: &Transaction,
        #[cfg(feature = "indexer")] transaction_location: TransactionLocation,
    ) {
        let db = &zebra_db.db;
        let sprout_nullifiers = db.cf_handle("sprout_nullifiers").unwrap();
        let sapling_nullifiers = db.cf_handle("sapling_nullifiers").unwrap();
        let orchard_nullifiers = db.cf_handle("orchard_nullifiers").unwrap();
        let ironwood_nullifiers = db.cf_handle("ironwood_nullifiers").unwrap();

        #[cfg(feature = "indexer")]
        let insert_value = transaction_location;
        #[cfg(not(feature = "indexer"))]
        let insert_value = ();

        // Mark sprout, sapling, orchard, and Ironwood nullifiers as spent.
        for sprout_nullifier in transaction.sprout_nullifiers() {
            self.zs_insert(&sprout_nullifiers, sprout_nullifier, insert_value);
        }
        for sapling_nullifier in transaction.sapling_nullifiers() {
            self.zs_insert(&sapling_nullifiers, sapling_nullifier, insert_value);
        }
        for orchard_nullifier in transaction.orchard_nullifiers() {
            self.zs_insert(&orchard_nullifiers, orchard_nullifier, insert_value);
        }
        for ironwood_nullifier in transaction.ironwood_nullifiers() {
            self.zs_insert(&ironwood_nullifiers, ironwood_nullifier, insert_value);
        }
    }

    /// Prepare a database batch containing the note commitment and history tree updates
    /// from `finalized.block`, and return it (without actually writing anything).
    ///
    /// If this method returns an error, it will be propagated,
    /// and the batch should not be written to the database.
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_trees_batch(
        &mut self,
        zebra_db: &ZebraDb,
        finalized: &FinalizedBlock,
        prev_note_commitment_trees: Option<NoteCommitmentTrees>,
        vct_data: Option<VctData>,
    ) {
        let FinalizedBlock {
            height,
            treestate:
                Treestate {
                    note_commitment_trees,
                    history_tree,
                },
            ..
        } = finalized;

        // The ZIP-244 auth-data root of this block, stored in the serving index so this
        // node can hand it to a peer as the co-input needed to authenticate the
        // *predecessor's* note-commitment roots against this block's NU5+ header
        // commitment (without the peer re-reading this block's body). Same value the
        // commitment check above already verified against the header.
        let auth_data_root = finalized.block.auth_data_root();

        // The per-block shielded transaction counts — the only ZIP-221 history-leaf inputs the
        // header and roots don't provide — plus the Ironwood note-commitment root, stored in the
        // serving index so a fast-synced node can serve them for header-sync verification (design
        // §6). The Ironwood tree does not exist below Nu7, so its root is the empty-tree root for
        // every currently-committable height (there is no per-height Ironwood tree store yet).
        let sapling_tx = finalized.block.sapling_transactions_count();
        let orchard_tx = finalized.block.orchard_transactions_count();
        let ironwood_tx = finalized.block.ironwood_transactions_count();
        let ironwood_root = ironwood::tree::NoteCommitmentTree::default().root();

        // Record the upgrade height `U` once, on the first block this binary commits: the lowest
        // height in the serving index, and the boundary below which roots are served from the
        // pre-upgrade per-height trees instead. Written on both commit paths so it is set even for
        // a node that upgrades above the last checkpoint (legacy path only). Set-once: the marker
        // is never moved, so the boundary stays stable as the chain grows. Commits are sequential,
        // so the absent check sees the previous block's committed marker, not a half-written batch.
        if zebra_db.vct_upgrade_height().is_none() {
            self.update_vct_upgrade_marker(zebra_db, *height);
        }

        // Mark the database as vct-synced (per-height note-commitment trees absent
        // below the checkpoint handoff height). Written in the same atomic batch as
        // every vct commit, so a vct-synced database always carries the marker and
        // the read/validity guards never see absent trees without it.
        if let Some(VctData { sync_below, .. }) = vct_data {
            self.update_vct_sync_marker(zebra_db, sync_below);
        }

        // POC (verified-commitment-trees) vct path: the committer skipped the
        // per-block frontier recompute, so `note_commitment_trees` is the frozen
        // parent frontier. Write only the supplied roots into the anchor set and
        // the (already-extended) history tree; skip the per-height Sapling/Orchard
        // tree CFs and subtrees entirely. The Sprout tree is unchanged below any
        // modern checkpoint, so it is correctly left untouched here.
        // See docs/design/verified-commitment-trees.md.
        if let Some(VctData {
            anchor_roots: (sapling_root, orchard_root),
            sync_below,
        }) = vct_data
        {
            // Mark the database as vct-synced in the same atomic batch as every
            // fast commit, so the read/validity guards never see absent trees
            // without the handoff marker.
            self.update_vct_sync_marker(zebra_db, sync_below);
            self.insert_sapling_anchor(zebra_db, &sapling_root);
            self.insert_orchard_anchor(zebra_db, &orchard_root);
            // Persist the per-height roots into the serving index even though no per-height
            // tree is written, so this fast-synced node can still serve `tree_aux` roots
            // (design §4); otherwise the root-serving fleet collapses as nodes fast-sync.
            self.insert_commitment_roots_by_height(
                zebra_db,
                *height,
                &sapling_root,
                &orchard_root,
                &ironwood_root,
                sapling_tx,
                orchard_tx,
                ironwood_tx,
                &auth_data_root,
            );
            self.update_history_tree(zebra_db, history_tree);
            return;
        }

        let prev_sprout_tree = prev_note_commitment_trees.as_ref().map_or_else(
            || zebra_db.sprout_tree_for_tip(),
            |prev_trees| prev_trees.sprout.clone(),
        );
        let prev_sapling_tree = prev_note_commitment_trees.as_ref().map_or_else(
            || zebra_db.sapling_tree_for_tip(),
            |prev_trees| prev_trees.sapling.clone(),
        );
        let prev_orchard_tree = prev_note_commitment_trees.as_ref().map_or_else(
            || zebra_db.orchard_tree_for_tip(),
            |prev_trees| prev_trees.orchard.clone(),
        );
        let prev_ironwood_tree = prev_note_commitment_trees.as_ref().map_or_else(
            || zebra_db.ironwood_tree_for_tip(),
            |prev_trees| prev_trees.ironwood.clone(),
        );
        // Update the Sprout tree and store its anchor only if it has changed
        if height.is_min() || prev_sprout_tree != note_commitment_trees.sprout {
            self.update_sprout_tree(zebra_db, &note_commitment_trees.sprout)
        }

        // Store the Sapling tree, anchor, and any new subtrees only if they have changed
        if height.is_min() || prev_sapling_tree != note_commitment_trees.sapling {
            self.create_sapling_tree(zebra_db, height, &note_commitment_trees.sapling);

            if let Some(subtree) = note_commitment_trees.sapling_subtree {
                self.insert_sapling_subtree(zebra_db, &subtree);
            }
        }

        // Store the Orchard tree, anchor, and any new subtrees only if they have changed
        if height.is_min() || prev_orchard_tree != note_commitment_trees.orchard {
            self.create_orchard_tree(zebra_db, height, &note_commitment_trees.orchard);

            if let Some(subtree) = note_commitment_trees.orchard_subtree {
                self.insert_orchard_subtree(zebra_db, &subtree);
            }
        }

        // Store the Ironwood tree, anchor, and any new subtrees only if they have changed
        if height.is_min() || prev_ironwood_tree != note_commitment_trees.ironwood {
            self.create_ironwood_tree(zebra_db, height, &note_commitment_trees.ironwood);

            if let Some(subtree) = note_commitment_trees.ironwood_subtree {
                self.insert_ironwood_subtree(zebra_db, &subtree);
            }
        }

        // Persist the per-height roots into the serving index for *every* committed height
        // (not just when a tree changed — the index must be gap-free for contiguous serving),
        // so a legacy/archive node serves `tree_aux` roots from the compact index too, and a
        // node that later fast-syncs above this height already has the lower range covered.
        self.insert_commitment_roots_by_height(
            zebra_db,
            *height,
            &note_commitment_trees.sapling.root(),
            &note_commitment_trees.orchard.root(),
            &ironwood_root,
            sapling_tx,
            orchard_tx,
            ironwood_tx,
            &auth_data_root,
        );

        self.update_history_tree(zebra_db, history_tree);
    }

    // Sprout tree methods

    /// Updates the Sprout note commitment tree for the tip, and the Sprout anchors.
    pub fn update_sprout_tree(
        &mut self,
        zebra_db: &ZebraDb,
        tree: &sprout::tree::NoteCommitmentTree,
    ) {
        let sprout_anchors = zebra_db.db.cf_handle("sprout_anchors").unwrap();
        let sprout_tree_cf = zebra_db
            .db
            .cf_handle("sprout_note_commitment_tree")
            .unwrap();

        // Sprout lookups need all previous trees by their anchors.
        // The root must be calculated first, so it is cached in the database.
        self.zs_insert(&sprout_anchors, tree.root(), tree);
        self.zs_insert(&sprout_tree_cf, (), tree);
    }

    /// Legacy method: Deletes the range of Sprout note commitment trees at the given [`Height`]s.
    /// Doesn't delete anchors from the anchor index. Doesn't delete the upper bound.
    ///
    /// From state format 25.3.0 onwards, the Sprout trees are indexed by an empty key,
    /// so this method does nothing.
    pub fn delete_range_sprout_tree(&mut self, zebra_db: &ZebraDb, from: &Height, to: &Height) {
        let sprout_tree_cf = zebra_db
            .db
            .cf_handle("sprout_note_commitment_tree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&sprout_tree_cf, from, to);
    }

    /// Deletes the given Sprout note commitment tree `anchor`.
    #[allow(dead_code)]
    pub fn delete_sprout_anchor(&mut self, zebra_db: &ZebraDb, anchor: &sprout::tree::Root) {
        let sprout_anchors = zebra_db.db.cf_handle("sprout_anchors").unwrap();
        self.zs_delete(&sprout_anchors, anchor);
    }

    // Sapling tree methods

    /// Inserts or overwrites the Sapling note commitment tree at the given [`Height`],
    /// and the Sapling anchors.
    pub fn create_sapling_tree(
        &mut self,
        zebra_db: &ZebraDb,
        height: &Height,
        tree: &sapling::tree::NoteCommitmentTree,
    ) {
        let sapling_anchors = zebra_db.db.cf_handle("sapling_anchors").unwrap();
        let sapling_tree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_tree")
            .unwrap();

        self.zs_insert(&sapling_anchors, tree.root(), ());
        self.zs_insert(&sapling_tree_cf, height, tree);
    }

    /// POC: inserts only the Sapling anchor `root` (value `()`), without writing a
    /// per-height tree. Used by the verified-commitment-trees fast path, which
    /// supplies the root directly instead of recomputing the frontier. The anchor
    /// CF is a set, so re-inserting an unchanged root is idempotent.
    pub fn insert_sapling_anchor(&mut self, zebra_db: &ZebraDb, root: &sapling::tree::Root) {
        let sapling_anchors = zebra_db.db.cf_handle("sapling_anchors").unwrap();
        self.zs_insert(&sapling_anchors, root, ());
    }

    /// Inserts the per-height Sapling/Orchard commitment roots into the
    /// `commitment_roots_by_height` serving index (design §4).
    ///
    /// Written on every committed block, fast or legacy, so any node — including a
    /// fast-synced node that holds no per-height trees — can serve the `tree_aux`
    /// `BlockRoots` read from this compact 64-byte-per-height index. Idempotent
    /// (re-inserting the same height overwrites with the identical value).
    #[allow(clippy::too_many_arguments)]
    pub fn insert_commitment_roots_by_height(
        &mut self,
        zebra_db: &ZebraDb,
        height: Height,
        sapling_root: &sapling::tree::Root,
        orchard_root: &orchard::tree::Root,
        ironwood_root: &ironwood::tree::Root,
        sapling_tx: u64,
        orchard_tx: u64,
        ironwood_tx: u64,
        auth_data_root: &AuthDataRoot,
    ) {
        let cf = zebra_db.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT).unwrap();
        self.zs_insert(
            &cf,
            height,
            CommitmentRootsByHeight {
                sapling: *sapling_root,
                orchard: *orchard_root,
                ironwood: *ironwood_root,
                sapling_tx,
                orchard_tx,
                ironwood_tx,
                auth_data_root: *auth_data_root,
            },
        );
    }

    /// Deletes the commitment-roots serving-index entries in `[from, to)`.
    ///
    /// Used by the finalized rollback to truncate the index above the rollback target, the
    /// same way the per-height trees and anchors above the target are removed, so a
    /// rolled-back database does not retain root entries for heights it no longer holds.
    pub fn delete_range_commitment_roots_by_height(
        &mut self,
        zebra_db: &ZebraDb,
        from: &Height,
        to: &Height,
    ) {
        let cf = zebra_db.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT).unwrap();
        self.zs_delete_range(&cf, from, to);
    }

    /// Records the verified-commitment-trees fast-sync marker: per-height
    /// note-commitment trees are absent below `handoff`. Idempotent (written in the
    /// same batch as each fast commit).
    pub fn update_vct_sync_marker(&mut self, zebra_db: &ZebraDb, handoff: Height) {
        let vct_sync_metadata = zebra_db
            .db
            .cf_handle(crate::service::finalized_state::VCT_SYNC_METADATA)
            .unwrap();
        self.zs_insert(&vct_sync_metadata, (), handoff);
    }

    /// Records the verified-commitment-trees upgrade height `U` = `height`, the lowest height this
    /// binary commits and the lowest height in the serving index. Set once and never moved, so the
    /// caller must only invoke this when [`vct_upgrade_height`](ZebraDb::vct_upgrade_height) is
    /// still absent.
    pub fn update_vct_upgrade_marker(&mut self, zebra_db: &ZebraDb, height: Height) {
        let vct_upgrade_metadata = zebra_db
            .db
            .cf_handle(crate::service::finalized_state::VCT_UPGRADE_METADATA)
            .unwrap();
        self.zs_insert(&vct_upgrade_metadata, (), height);
    }

    /// Inserts the Sapling note commitment subtree into the batch.
    pub fn insert_sapling_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        subtree: &NoteCommitmentSubtree<sapling_crypto::Node>,
    ) {
        let sapling_subtree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();
        self.zs_insert(&sapling_subtree_cf, subtree.index, subtree.into_data());
    }

    /// Deletes the Sapling note commitment tree at the given [`Height`].
    pub fn delete_sapling_tree(&mut self, zebra_db: &ZebraDb, height: &Height) {
        let sapling_tree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_tree")
            .unwrap();
        self.zs_delete(&sapling_tree_cf, height);
    }

    /// Deletes the range of Sapling note commitment trees at the given [`Height`]s.
    /// Doesn't delete anchors from the anchor index. Doesn't delete the upper bound.
    #[allow(dead_code)]
    pub fn delete_range_sapling_tree(&mut self, zebra_db: &ZebraDb, from: &Height, to: &Height) {
        let sapling_tree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_tree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&sapling_tree_cf, from, to);
    }

    /// Deletes the given Sapling note commitment tree `anchor`.
    #[allow(dead_code)]
    pub fn delete_sapling_anchor(&mut self, zebra_db: &ZebraDb, anchor: &sapling::tree::Root) {
        let sapling_anchors = zebra_db.db.cf_handle("sapling_anchors").unwrap();
        self.zs_delete(&sapling_anchors, anchor);
    }

    /// Deletes the range of Sapling subtrees at the given [`NoteCommitmentSubtreeIndex`]es.
    /// Doesn't delete the upper bound.
    pub fn delete_range_sapling_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        from: NoteCommitmentSubtreeIndex,
        to: NoteCommitmentSubtreeIndex,
    ) {
        let sapling_subtree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&sapling_subtree_cf, from, to);
    }

    // Orchard tree methods

    /// Inserts or overwrites the Orchard note commitment tree at the given [`Height`],
    /// and the Orchard anchors.
    pub fn create_orchard_tree(
        &mut self,
        zebra_db: &ZebraDb,
        height: &Height,
        tree: &orchard::tree::NoteCommitmentTree,
    ) {
        let orchard_anchors = zebra_db.db.cf_handle("orchard_anchors").unwrap();
        let orchard_tree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_tree")
            .unwrap();

        self.zs_insert(&orchard_anchors, tree.root(), ());
        self.zs_insert(&orchard_tree_cf, height, tree);
    }

    /// POC: inserts only the Orchard anchor `root` (value `()`), without writing a
    /// per-height tree. The Orchard twin of [`Self::insert_sapling_anchor`].
    pub fn insert_orchard_anchor(&mut self, zebra_db: &ZebraDb, root: &orchard::tree::Root) {
        let orchard_anchors = zebra_db.db.cf_handle("orchard_anchors").unwrap();
        self.zs_insert(&orchard_anchors, root, ());
    }

    /// Inserts the Orchard note commitment subtree into the batch.
    pub fn insert_orchard_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        subtree: &NoteCommitmentSubtree<orchard::tree::Node>,
    ) {
        let orchard_subtree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();
        self.zs_insert(&orchard_subtree_cf, subtree.index, subtree.into_data());
    }

    /// Inserts or overwrites the Ironwood note commitment tree at the given
    /// [`Height`], and the Ironwood anchors.
    pub fn create_ironwood_tree(
        &mut self,
        zebra_db: &ZebraDb,
        height: &Height,
        tree: &ironwood::tree::NoteCommitmentTree,
    ) {
        let ironwood_anchors = zebra_db.db.cf_handle("ironwood_anchors").unwrap();
        let ironwood_tree_cf = zebra_db
            .db
            .cf_handle("ironwood_note_commitment_tree")
            .unwrap();

        self.zs_insert(&ironwood_anchors, tree.root(), ());
        self.zs_insert(&ironwood_tree_cf, height, tree);
    }

    /// Inserts the Ironwood note commitment subtree into the batch.
    pub fn insert_ironwood_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        subtree: &NoteCommitmentSubtree<ironwood::tree::Node>,
    ) {
        let ironwood_subtree_cf = zebra_db
            .db
            .cf_handle("ironwood_note_commitment_subtree")
            .unwrap();
        self.zs_insert(&ironwood_subtree_cf, subtree.index, subtree.into_data());
    }

    /// Deletes the Orchard note commitment tree at the given [`Height`].
    pub fn delete_orchard_tree(&mut self, zebra_db: &ZebraDb, height: &Height) {
        let orchard_tree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_tree")
            .unwrap();
        self.zs_delete(&orchard_tree_cf, height);
    }

    /// Deletes the range of Orchard note commitment trees at the given [`Height`]s.
    /// Doesn't delete anchors from the anchor index. Doesn't delete the upper bound.
    #[allow(dead_code)]
    pub fn delete_range_orchard_tree(&mut self, zebra_db: &ZebraDb, from: &Height, to: &Height) {
        let orchard_tree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_tree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&orchard_tree_cf, from, to);
    }

    /// Deletes the given Orchard note commitment tree `anchor`.
    #[allow(dead_code)]
    pub fn delete_orchard_anchor(&mut self, zebra_db: &ZebraDb, anchor: &orchard::tree::Root) {
        let orchard_anchors = zebra_db.db.cf_handle("orchard_anchors").unwrap();
        self.zs_delete(&orchard_anchors, anchor);
    }

    /// Deletes the range of Orchard subtrees at the given [`NoteCommitmentSubtreeIndex`]es.
    /// Doesn't delete the upper bound.
    pub fn delete_range_orchard_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        from: NoteCommitmentSubtreeIndex,
        to: NoteCommitmentSubtreeIndex,
    ) {
        let orchard_subtree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&orchard_subtree_cf, from, to);
    }

    /// Deletes the Ironwood note commitment tree at the given [`Height`].
    pub fn delete_ironwood_tree(&mut self, zebra_db: &ZebraDb, height: &Height) {
        let ironwood_tree_cf = zebra_db
            .db
            .cf_handle("ironwood_note_commitment_tree")
            .unwrap();
        self.zs_delete(&ironwood_tree_cf, height);
    }

    /// Deletes the given Ironwood note commitment tree `anchor`.
    pub fn delete_ironwood_anchor(&mut self, zebra_db: &ZebraDb, anchor: &ironwood::tree::Root) {
        let ironwood_anchors = zebra_db.db.cf_handle("ironwood_anchors").unwrap();
        self.zs_delete(&ironwood_anchors, anchor);
    }

    /// Deletes the range of Ironwood subtrees at the given [`NoteCommitmentSubtreeIndex`]es.
    /// Doesn't delete the upper bound.
    pub fn delete_range_ironwood_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        from: NoteCommitmentSubtreeIndex,
        to: NoteCommitmentSubtreeIndex,
    ) {
        let ironwood_subtree_cf = zebra_db
            .db
            .cf_handle("ironwood_note_commitment_subtree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&ironwood_subtree_cf, from, to);
    }
}
