//! Note Commitment Trees.
//!
//! A note commitment tree is an incremental Merkle tree of fixed depth
//! used to store note commitments that JoinSplit transfers or Spend
//! transfers produce. Just as the unspent transaction output set (UTXO
//! set) used in Bitcoin, it is used to express the existence of value and
//! the capability to spend it. However, unlike the UTXO set, it is not
//! the job of this tree to protect against double-spending, as it is
//! append-only.
//!
//! A root of a note commitment tree is associated with each treestate.

use std::{
    default::Default,
    fmt,
    hash::{Hash, Hasher},
    io,
};

use hex::ToHex;
use incrementalmerkletree::frontier::{Frontier, NonEmptyFrontier};

use thiserror::Error;

use crate::{
    serialization::{
        serde_helpers, ReadZcashExt, SerializationError, ZcashDeserialize, ZcashSerialize,
    },
    subtree::{NoteCommitmentSubtreeIndex, TRACKED_SUBTREE_HEIGHT},
};

pub mod legacy;
use legacy::LegacyNoteCommitmentTree;

/// The type that is used to update the note commitment tree.
///
/// Unfortunately, this is not the same as `sapling::NoteCommitment`.
pub type NoteCommitmentUpdate = sapling_crypto::note::ExtractedNoteCommitment;

pub(super) const MERKLE_DEPTH: u8 = 32;

/// Sapling note commitment tree root node hash.
///
/// The root hash in LEBS2OSP256(rt) encoding of the Sapling note
/// commitment tree corresponding to the final Sapling treestate of
/// this block. A root of a note commitment tree is associated with
/// each treestate.
#[derive(Clone, Copy, Default, Eq, Serialize, Deserialize)]
pub struct Root(#[serde(with = "serde_helpers::Fq")] pub(crate) jubjub::Base);

impl Root {
    /// Return the node bytes in little-endian byte order as required
    /// by RPCs such as `z_gettreestate`.
    pub fn bytes_in_display_order(&self) -> [u8; 32] {
        let mut root: [u8; 32] = self.into();
        root.reverse();
        root
    }
}

impl fmt::Debug for Root {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Root")
            .field(&hex::encode(self.0.to_bytes()))
            .finish()
    }
}

impl From<Root> for [u8; 32] {
    fn from(root: Root) -> Self {
        root.0.to_bytes()
    }
}

impl From<&Root> for [u8; 32] {
    fn from(root: &Root) -> Self {
        (*root).into()
    }
}

impl PartialEq for Root {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for Root {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bytes().hash(state)
    }
}

impl TryFrom<[u8; 32]> for Root {
    type Error = SerializationError;

    fn try_from(bytes: [u8; 32]) -> Result<Self, Self::Error> {
        let possible_point = jubjub::Base::from_bytes(&bytes);

        if possible_point.is_some().into() {
            Ok(Self(possible_point.unwrap()))
        } else {
            Err(SerializationError::Parse(
                "Invalid jubjub::Base value for Sapling note commitment tree root",
            ))
        }
    }
}

impl ToHex for &Root {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        <[u8; 32]>::from(*self).encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        <[u8; 32]>::from(*self).encode_hex_upper()
    }
}

impl ToHex for Root {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        (&self).encode_hex_upper()
    }
}

impl ZcashSerialize for Root {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_all(&<[u8; 32]>::from(*self)[..])?;

        Ok(())
    }
}

impl ZcashDeserialize for Root {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        Self::try_from(reader.read_32_bytes()?)
    }
}

#[derive(Error, Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[allow(missing_docs)]
pub enum NoteCommitmentTreeError {
    #[error("The note commitment tree is full")]
    FullTree,
}

/// Sapling Incremental Note Commitment Tree.
///
/// Note that the default value of the [`Root`] type is `[0, 0, 0, 0]`. However, this value differs
/// from the default value of the root of the default tree which is the hash of the root's child
/// nodes. The default tree is the empty tree which has all leaves empty.
#[derive(Debug, Serialize, Deserialize)]
#[serde(into = "LegacyNoteCommitmentTree")]
#[serde(from = "LegacyNoteCommitmentTree")]
pub struct NoteCommitmentTree {
    /// The tree represented as a [`Frontier`].
    ///
    /// A Frontier is a subset of the tree that allows to fully specify it.
    /// It consists of nodes along the rightmost (newer) branch of the tree that
    /// has non-empty nodes. Upper (near root) empty nodes of the branch are not
    /// stored.
    ///
    /// # Consensus
    ///
    /// > [Sapling onward] A block MUST NOT add Sapling note commitments that
    /// > would result in the Sapling note commitment tree exceeding its capacity
    /// > of 2^(MerkleDepth^Sapling) leaf nodes.
    ///
    /// <https://zips.z.cash/protocol/protocol.pdf#merkletree>
    ///
    /// Note: MerkleDepth^Sapling = MERKLE_DEPTH = 32.
    inner: Frontier<sapling_crypto::Node, MERKLE_DEPTH>,

    /// A cached root of the tree.
    ///
    /// Every time the root is computed by [`Self::root`] it is cached here, and
    /// the cached value will be returned by [`Self::root`] until the tree is
    /// changed by [`Self::append`]. This greatly increases performance because
    /// it avoids recomputing the root when the tree does not change between
    /// blocks. In the finalized state, the tree is read from disk for every
    /// block processed, which would also require recomputing the root even if
    /// it has not changed (note that the cached root is serialized with the
    /// tree). This is particularly important since we decided to instantiate
    /// the trees from the genesis block, for simplicity.
    ///
    /// We use a [`RwLock`](std::sync::RwLock) for this cache, because it is only written once per
    /// tree update. Each tree has its own cached root, a new lock is created
    /// for each clone.
    cached_root: std::sync::RwLock<Option<Root>>,
}

impl NoteCommitmentTree {
    /// Adds a note commitment u-coordinate to the tree.
    ///
    /// The leaves of the tree are actually a base field element, the
    /// u-coordinate of the commitment, the data that is actually stored on the
    /// chain and input into the proof.
    ///
    /// Returns an error if the tree is full.
    #[allow(clippy::unwrap_in_result)]
    pub fn append(&mut self, cm_u: NoteCommitmentUpdate) -> Result<(), NoteCommitmentTreeError> {
        if self.inner.append(sapling_crypto::Node::from_cmu(&cm_u)) {
            // Invalidate cached root
            let cached_root = self
                .cached_root
                .get_mut()
                .expect("a thread that previously held exclusive lock access panicked");

            *cached_root = None;

            Ok(())
        } else {
            Err(NoteCommitmentTreeError::FullTree)
        }
    }

    /// Appends one block's note commitments in parallel.
    ///
    /// Returns the [`TRACKED_SUBTREE_HEIGHT`] subtree completed by this block, if
    /// any. This must match calling [`Self::append`] for each commitment in order.
    ///
    /// `note_commitments` must come from one block, so the batch can cross at
    /// most one tracked-subtree boundary.
    ///
    /// Returns an error if the tree would overflow its capacity.
    #[allow(clippy::unwrap_in_result)]
    pub fn append_batch(
        &mut self,
        note_commitments: &[NoteCommitmentUpdate],
    ) -> Result<Option<(NoteCommitmentSubtreeIndex, sapling_crypto::Node)>, NoteCommitmentTreeError>
    {
        use crate::parallel::batch_frontier::append_batch_with_subtree;

        if note_commitments.is_empty() {
            return Ok(None);
        }

        // nodes.len() fits in u64: consensus rules cap a block at 2^16 outputs.
        let nodes: Vec<sapling_crypto::Node> = note_commitments
            .iter()
            .map(sapling_crypto::Node::from_cmu)
            .collect();

        let (frontier, completed) = append_batch_with_subtree(self.inner.clone(), nodes)
            .map_err(|_| NoteCommitmentTreeError::FullTree)?;

        self.inner = frontier;
        *self
            .cached_root
            .get_mut()
            .expect("a thread that previously held exclusive lock access panicked") = None;

        Ok(completed.map(|(index_value, root)| {
            let index = NoteCommitmentSubtreeIndex(
                index_value.try_into().expect("subtree index fits in u16"),
            );
            (index, root)
        }))
    }

    /// Returns frontier of non-empty tree, or None.
    fn frontier(&self) -> Option<&NonEmptyFrontier<sapling_crypto::Node>> {
        self.inner.value()
    }

    /// Returns the position of the most recently appended leaf in the tree.
    ///
    /// This method is used for debugging, use `incrementalmerkletree::Address` for tree operations.
    pub fn position(&self) -> Option<u64> {
        let Some(tree) = self.frontier() else {
            // An empty tree doesn't have a previous leaf.
            return None;
        };

        Some(tree.position().into())
    }

    /// Returns true if this tree has at least one new subtree, when compared with `prev_tree`.
    pub fn contains_new_subtree(&self, prev_tree: &Self) -> bool {
        // Use -1 for the index of the subtree with no notes, so the comparisons are valid.
        let index = self.subtree_index().map_or(-1, |index| i32::from(index.0));
        let prev_index = prev_tree
            .subtree_index()
            .map_or(-1, |index| i32::from(index.0));

        // This calculation can't overflow, because we're using i32 for u16 values.
        let index_difference = index - prev_index;

        // There are 4 cases we need to handle:
        // - lower index: never a new subtree
        // - equal index: sometimes a new subtree
        // - next index: sometimes a new subtree
        // - greater than the next index: always a new subtree
        //
        // To simplify the function, we deal with the simple cases first.

        // There can't be any new subtrees if the current index is strictly lower.
        if index < prev_index {
            return false;
        }

        // There is at least one new subtree, even if there is a spurious index difference.
        if index_difference > 1 {
            return true;
        }

        // If the indexes are equal, there can only be a new subtree if `self` just completed it.
        if index == prev_index {
            return self.is_complete_subtree();
        }

        // If `self` is the next index, check if the last note completed a subtree.
        if self.is_complete_subtree() {
            return true;
        }

        // Then check for spurious index differences.
        //
        // There is one new subtree somewhere in the trees. It is either:
        // - a new subtree at the end of the previous tree, or
        // - a new subtree in this tree (but not at the end).
        //
        // Spurious index differences happen because the subtree index only increases when the
        // first note is added to the new subtree. So we need to exclude subtrees completed by the
        // last note commitment in the previous tree.
        //
        // We also need to exclude empty previous subtrees, because the index changes to zero when
        // the first note is added, but a subtree wasn't completed.
        if prev_tree.is_complete_subtree() || prev_index == -1 {
            return false;
        }

        // A new subtree was completed by a note commitment that isn't in the previous tree.
        true
    }

    /// Returns true if the most recently appended leaf completes the subtree
    pub fn is_complete_subtree(&self) -> bool {
        let Some(tree) = self.frontier() else {
            // An empty tree can't be a complete subtree.
            return false;
        };

        tree.position()
            .is_complete_subtree(TRACKED_SUBTREE_HEIGHT.into())
    }

    /// Returns the subtree index at [`TRACKED_SUBTREE_HEIGHT`].
    /// This is the number of complete or incomplete subtrees that are currently in the tree.
    /// Returns `None` if the tree is empty.
    #[allow(clippy::unwrap_in_result)]
    pub fn subtree_index(&self) -> Option<NoteCommitmentSubtreeIndex> {
        let tree = self.frontier()?;

        let index = incrementalmerkletree::Address::above_position(
            TRACKED_SUBTREE_HEIGHT.into(),
            tree.position(),
        )
        .index()
        .try_into()
        .expect("fits in u16");

        Some(index)
    }

    /// Returns the number of leaf nodes required to complete the subtree at
    /// [`TRACKED_SUBTREE_HEIGHT`].
    ///
    /// Returns `2^TRACKED_SUBTREE_HEIGHT` if the tree is empty.
    #[allow(clippy::unwrap_in_result)]
    pub fn remaining_subtree_leaf_nodes(&self) -> usize {
        let remaining = match self.frontier() {
            // If the subtree has at least one leaf node, the remaining number of nodes can be
            // calculated using the maximum subtree position and the current position.
            Some(tree) => {
                let max_position = incrementalmerkletree::Address::above_position(
                    TRACKED_SUBTREE_HEIGHT.into(),
                    tree.position(),
                )
                .max_position();

                max_position - tree.position().into()
            }
            // If the subtree has no nodes, the remaining number of nodes is the number of nodes in
            // a subtree.
            None => {
                let subtree_address = incrementalmerkletree::Address::above_position(
                    TRACKED_SUBTREE_HEIGHT.into(),
                    // This position is guaranteed to be in the first subtree.
                    0.into(),
                );

                assert_eq!(
                    subtree_address.position_range_start(),
                    0.into(),
                    "address is not in the first subtree"
                );

                subtree_address.position_range_end()
            }
        };

        u64::from(remaining).try_into().expect("fits in usize")
    }

    /// Returns subtree index and root if the most recently appended leaf completes the subtree
    pub fn completed_subtree_index_and_root(
        &self,
    ) -> Option<(NoteCommitmentSubtreeIndex, sapling_crypto::Node)> {
        if !self.is_complete_subtree() {
            return None;
        }

        let index = self.subtree_index()?;
        let root = self.frontier()?.root(Some(TRACKED_SUBTREE_HEIGHT.into()));

        Some((index, root))
    }

    /// Returns the current root of the tree, used as an anchor in Sapling
    /// shielded transactions.
    pub fn root(&self) -> Root {
        if let Some(root) = self.cached_root() {
            // Return cached root.
            return root;
        }

        // Get exclusive access, compute the root, and cache it.
        let mut write_root = self
            .cached_root
            .write()
            .expect("a thread that previously held exclusive lock access panicked");
        let read_root = write_root.as_ref().cloned();
        match read_root {
            // Another thread got write access first, return cached root.
            Some(root) => root,
            None => {
                // Compute root and cache it.
                let root = self.recalculate_root();
                *write_root = Some(root);
                root
            }
        }
    }

    /// Returns the current root of the tree, if it has already been cached.
    #[allow(clippy::unwrap_in_result)]
    pub fn cached_root(&self) -> Option<Root> {
        *self
            .cached_root
            .read()
            .expect("a thread that previously held exclusive lock access panicked")
    }

    /// Calculates and returns the current root of the tree, ignoring any caching.
    pub fn recalculate_root(&self) -> Root {
        Root::try_from(self.inner.root().to_bytes()).unwrap()
    }

    /// Gets the Jubjub-based Pedersen hash of root node of this merkle tree of
    /// note commitments.
    pub fn hash(&self) -> [u8; 32] {
        self.root().into()
    }

    /// An as-yet unused Sapling note commitment tree leaf node.
    ///
    /// Distinct for Sapling, a distinguished hash value of:
    ///
    /// Uncommitted^Sapling = I2LEBSP_l_MerkleSapling(1)
    pub fn uncommitted() -> [u8; 32] {
        jubjub::Fq::one().to_bytes()
    }

    /// Counts of note commitments added to the tree.
    ///
    /// For Sapling, the tree is capped at 2^32.
    pub fn count(&self) -> u64 {
        self.inner
            .value()
            .map_or(0, |x| u64::from(x.position()) + 1)
    }

    /// Checks if the tree roots and inner data structures of `self` and `other` are equal.
    ///
    /// # Panics
    ///
    /// If they aren't equal, with a message explaining the differences.
    ///
    /// Only for use in tests.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn assert_frontier_eq(&self, other: &Self) {
        // It's technically ok for the cached root not to be preserved,
        // but it can result in expensive cryptographic operations,
        // so we fail the tests if it happens.
        assert_eq!(self.cached_root(), other.cached_root());

        // Check the data in the internal data structure
        assert_eq!(self.inner, other.inner);

        // Check the RPC serialization format (not the same as the Zebra database format)
        assert_eq!(self.to_rpc_bytes(), other.to_rpc_bytes());
    }

    /// Serializes [`Self`] to a format matching `zcashd`'s RPCs.
    pub fn to_rpc_bytes(&self) -> Vec<u8> {
        // Convert the tree from [`Frontier`](incrementalmerkletree::frontier::Frontier) to
        // [`CommitmentTree`](merkle_tree::CommitmentTree).
        let tree = incrementalmerkletree::frontier::CommitmentTree::from_frontier(&self.inner);

        let mut rpc_bytes = vec![];

        zcash_primitives::merkle_tree::write_commitment_tree(&tree, &mut rpc_bytes)
            .expect("serializable tree");

        rpc_bytes
    }
}

impl Clone for NoteCommitmentTree {
    /// Clones the inner tree, and creates a new [`RwLock`](std::sync::RwLock)
    /// with the cloned root data.
    fn clone(&self) -> Self {
        let cached_root = self.cached_root();

        Self {
            inner: self.inner.clone(),
            cached_root: std::sync::RwLock::new(cached_root),
        }
    }
}

impl Default for NoteCommitmentTree {
    fn default() -> Self {
        Self {
            inner: incrementalmerkletree::frontier::Frontier::empty(),
            cached_root: Default::default(),
        }
    }
}

impl Eq for NoteCommitmentTree {}

impl PartialEq for NoteCommitmentTree {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(root), Some(other_root)) = (self.cached_root(), other.cached_root()) {
            // Use cached roots if available
            root == other_root
        } else {
            // Avoid expensive root recalculations which use multiple cryptographic hashes
            self.inner == other.inner
        }
    }
}

impl From<Vec<sapling_crypto::note::ExtractedNoteCommitment>> for NoteCommitmentTree {
    /// Computes the tree from a whole bunch of note commitments at once.
    fn from(values: Vec<sapling_crypto::note::ExtractedNoteCommitment>) -> Self {
        let mut tree = Self::default();

        if values.is_empty() {
            return tree;
        }

        for cm_u in values {
            let _ = tree.append(cm_u);
        }

        tree
    }
}

#[cfg(test)]
mod tests {
    use incrementalmerkletree::{frontier::Frontier, Position};

    use super::*;

    fn node(value: u64) -> sapling_crypto::Node {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());

        Option::<sapling_crypto::Node>::from(sapling_crypto::Node::from_bytes(bytes))
            .expect("small little-endian integers are canonical field elements")
    }

    fn note_commitment(value: u64) -> NoteCommitmentUpdate {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());

        Option::<NoteCommitmentUpdate>::from(NoteCommitmentUpdate::from_bytes(&bytes))
            .expect("small little-endian integers are canonical field elements")
    }

    fn build_tree(prefix_len: u64) -> NoteCommitmentTree {
        let mut tree = NoteCommitmentTree::default();

        for value in 0..prefix_len {
            tree.append(note_commitment(value))
                .expect("small test tree is not full");
        }

        tree
    }

    fn pre_subtree_boundary_tree() -> NoteCommitmentTree {
        let subtree_size = 1u64 << TRACKED_SUBTREE_HEIGHT;
        let pre_boundary_pos = subtree_size - 2;
        let leaf = node(1);
        let ommers: Vec<sapling_crypto::Node> = (2..=16).map(node).collect();
        let inner = Frontier::from_parts(Position::from(pre_boundary_pos), leaf, ommers)
            .expect("frontier with 15 ommers at position 65534 is valid");

        NoteCommitmentTree {
            inner,
            cached_root: Default::default(),
        }
    }

    fn sequential_append_batch(
        tree: &mut NoteCommitmentTree,
        note_commitments: &[NoteCommitmentUpdate],
    ) -> Result<Option<(NoteCommitmentSubtreeIndex, sapling_crypto::Node)>, NoteCommitmentTreeError>
    {
        let mut completed_subtree = None;

        for note_commitment in note_commitments {
            tree.append(*note_commitment)?;

            if let Some(subtree) = tree.completed_subtree_index_and_root() {
                assert!(
                    completed_subtree.is_none(),
                    "test batches must cross at most one subtree boundary"
                );
                completed_subtree = Some(subtree);
            }
        }

        Ok(completed_subtree)
    }

    #[test]
    fn append_batch_matches_sequential_for_table_cases() {
        let cases = [
            ("empty tree, empty batch", 0, 0),
            ("empty tree, one leaf", 0, 1),
            ("empty tree, small batch", 0, 5),
            ("one-leaf tree, empty batch", 1, 0),
            ("one-leaf tree, one leaf", 1, 1),
            ("odd tree, small batch", 3, 4),
            ("power-of-two tree, small batch", 8, 7),
            ("after power-of-two tree, empty batch", 9, 0),
            ("after power-of-two tree, small batch", 9, 6),
        ];

        for (name, prefix_len, batch_len) in cases {
            let start = build_tree(prefix_len);
            let mut seq_tree = start.clone();
            let mut batch_tree = start;
            let note_commitments: Vec<_> = (0..batch_len)
                .map(|value| note_commitment(1_000 + prefix_len + value))
                .collect();

            let _ = seq_tree.root();
            let _ = batch_tree.root();
            let seq_result = sequential_append_batch(&mut seq_tree, &note_commitments)
                .expect("sequential append succeeds");
            let batch_result = batch_tree
                .append_batch(&note_commitments)
                .expect("batch append succeeds");

            assert_eq!(batch_result, seq_result, "{name}: subtree result mismatch");
            batch_tree.assert_frontier_eq(&seq_tree);
            assert_eq!(batch_tree.root(), seq_tree.root(), "{name}: root mismatch");
        }
    }

    #[test]
    fn append_batch_matches_sequential_near_subtree_boundary() {
        let cases = [
            ("before subtree boundary, empty batch", 0),
            ("complete subtree boundary", 1),
            ("complete and start next subtree", 2),
            ("complete and keep appending", 3),
        ];

        for (name, batch_len) in cases {
            let start = pre_subtree_boundary_tree();
            let mut seq_tree = start.clone();
            let mut batch_tree = start;
            let note_commitments: Vec<_> = (0..batch_len)
                .map(|value| note_commitment(10_000 + value))
                .collect();

            let _ = seq_tree.root();
            let _ = batch_tree.root();
            let seq_result = sequential_append_batch(&mut seq_tree, &note_commitments)
                .expect("sequential append succeeds");
            let batch_result = batch_tree
                .append_batch(&note_commitments)
                .expect("batch append succeeds");

            assert_eq!(batch_result, seq_result, "{name}: subtree result mismatch");
            batch_tree.assert_frontier_eq(&seq_tree);
            assert_eq!(batch_tree.root(), seq_tree.root(), "{name}: root mismatch");
        }
    }

    /// Verifies that `append_batch` returns the correct subtree index and root when
    /// the batch crosses a `TRACKED_SUBTREE_HEIGHT` boundary, and that the resulting
    /// frontier matches the sequential `append` path.
    ///
    /// Uses `Frontier::from_parts` to place the tree just before the first subtree
    /// boundary (position 65534 = `2^16 - 2`) without executing 65534 real appends.
    #[test]
    fn append_batch_crosses_subtree_boundary() {
        // position 65534 = 0xFFFE: bits 1–15 are set → 15 ommers required.
        let subtree_size = 1u64 << TRACKED_SUBTREE_HEIGHT;
        let pre_boundary_pos = subtree_size - 2; // = 65534
        let leaf = node(1);
        let ommers: Vec<sapling_crypto::Node> = (2..=16).map(node).collect();
        let inner = Frontier::from_parts(Position::from(pre_boundary_pos), leaf, ommers)
            .expect("frontier with 15 ommers at position 65534 is valid");
        let tree = NoteCommitmentTree {
            inner,
            cached_root: Default::default(),
        };

        // note_commitments[0] fills position 65535, completing subtree 0.
        // note_commitments[1] starts subtree 1.
        let note_commitments = [note_commitment(100), note_commitment(200)];

        // Sequential reference: append one at a time.
        let mut seq_tree = tree.clone();
        seq_tree
            .append(note_commitments[0])
            .expect("sequential first append");
        let expected_subtree = seq_tree.completed_subtree_index_and_root();
        seq_tree
            .append(note_commitments[1])
            .expect("sequential second append");

        // Batch must return the same subtree result and produce the same final tree.
        let mut batch_tree = tree;
        let batch_result = batch_tree
            .append_batch(&note_commitments)
            .expect("batch append succeeds");

        assert!(
            batch_result.is_some(),
            "batch crossing boundary must return a subtree"
        );
        assert_eq!(
            batch_result.unwrap().0,
            NoteCommitmentSubtreeIndex(0),
            "first subtree index"
        );
        assert_eq!(
            batch_result, expected_subtree,
            "subtree result matches sequential"
        );
        batch_tree.assert_frontier_eq(&seq_tree);
        assert_eq!(batch_tree.root(), seq_tree.root());
    }

    #[test]
    fn append_batch_overflow_preserves_tree_and_cached_root() {
        let max_position = (1u64 << MERKLE_DEPTH) - 1;
        let leaf = node(1);
        let ommers = vec![node(2); usize::from(MERKLE_DEPTH)];
        let inner = Frontier::from_parts(Position::from(max_position), leaf, ommers)
            .expect("max-depth frontier is valid");
        let mut tree = NoteCommitmentTree {
            inner,
            cached_root: Default::default(),
        };

        let _ = tree.root();
        let original = tree.clone();

        let result = tree.append_batch(&[note_commitment(3)]);

        assert_eq!(result, Err(NoteCommitmentTreeError::FullTree));
        tree.assert_frontier_eq(&original);
        assert_eq!(tree.root(), original.root());
    }

    /// `append_batch` must match sequential appends when a batch exactly fills
    /// the last two leaf positions of the tree, completing the final tracked
    /// subtree (index `u16::MAX`) without reporting a spurious overflow.
    #[test]
    fn append_batch_matches_sequential_at_tree_capacity() {
        let max_position = (1u64 << MERKLE_DEPTH) - 1;
        let start_position = max_position - 2;
        let leaf = node(1);
        // A frontier at position `p` stores one ommer per set bit of `p`;
        // `max_position - 2` has 31 of its 32 bits set.
        let ommers: Vec<sapling_crypto::Node> = (2..=32).map(node).collect();
        let inner = Frontier::from_parts(Position::from(start_position), leaf, ommers)
            .expect("frontier two leaves below capacity is valid");
        let start = NoteCommitmentTree {
            inner,
            cached_root: Default::default(),
        };
        let note_commitments = [note_commitment(100), note_commitment(200)];

        let mut seq_tree = start.clone();
        let seq_result = sequential_append_batch(&mut seq_tree, &note_commitments)
            .expect("two sequential appends reach exact capacity");

        let mut batch_tree = start;
        let batch_result = batch_tree
            .append_batch(&note_commitments)
            .expect("batch append reaches exact capacity");

        assert_eq!(batch_result, seq_result);
        assert_eq!(
            batch_result.map(|(index, _)| index),
            Some(NoteCommitmentSubtreeIndex(u16::MAX)),
        );
        batch_tree.assert_frontier_eq(&seq_tree);
        assert_eq!(batch_tree.root(), seq_tree.root());
    }

    #[test]
    fn append_batch_multiple_subtrees_preserves_tree_and_cached_root() {
        let mut tree = NoteCommitmentTree::default();
        let _ = tree.root();
        let original = tree.clone();

        let subtree_size = 1usize << TRACKED_SUBTREE_HEIGHT;
        let note_commitments: Vec<_> = (0..subtree_size * 2)
            .map(|value| note_commitment(value as u64))
            .collect();

        let result = tree.append_batch(&note_commitments);

        assert_eq!(result, Err(NoteCommitmentTreeError::FullTree));
        tree.assert_frontier_eq(&original);
        assert_eq!(tree.root(), original.root());
    }
}
