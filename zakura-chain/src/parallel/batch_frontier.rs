//! Parallel batch append for incremental Merkle [`Frontier`]s.
//!
//! Note commitment trees (Sprout, Sapling, Orchard) are all
//! [`incrementalmerkletree::frontier::Frontier<H, 32>`], differing only in the
//! per-pool [`Hashable::combine`] hash (SHA-256 / Pedersen / Sinsemilla). The
//! standard [`Frontier::append`] adds one leaf at a time, performing the Merkle
//! merge hashes sequentially. For a block with many shielded outputs this is the
//! dominant cost of committing the block, and it runs on a single thread.
//!
//! [`parallel_append`] produces a [`Frontier`] **byte-identical** to appending
//! each leaf sequentially, but computes the internal Merkle hashes with a
//! parallel divide-and-conquer reduction across the rayon thread pool.
//!
//! # Correctness
//!
//! This is consensus-critical: the frontier *is* the note commitment tree
//! commitment, so the result must match the sequential append exactly. The
//! implementation is validated by differential property tests against the
//! sequential [`Frontier::append`] (identical frontier parts and identical root)
//! in the `tests` module below.

use incrementalmerkletree::{
    frontier::{Frontier, FrontierError},
    Hashable, Level, Position,
};
use rayon::prelude::*;
use std::{error::Error, fmt};

/// Complete subtree roots for a contiguous run of leaves, indexed by level:
/// `roots[L] == Some(root)` iff bit `L` of the run length is set, in which case
/// `root` is the root of the complete `2^L`-leaf subtree covering that aligned
/// block. Higher set bits (older subtrees) are further left in leaf order.
type CompleteSubtreeRoots<H> = Vec<Option<H>>;

struct TreeCapacity<const DEPTH: u8>;

impl<const DEPTH: u8> TreeCapacity<DEPTH> {
    const MAX_LEAVES: u64 = 1u64 << DEPTH;
}

/// Errors from batch frontier updates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchFrontierError {
    /// A frontier reconstruction error.
    Frontier(FrontierError),

    /// The batch would complete more than one tracked subtree.
    BatchSpansMultipleSubtrees,
}

impl fmt::Display for BatchFrontierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchFrontierError::Frontier(error) => {
                write!(f, "frontier reconstruction error: {error:?}")
            }
            BatchFrontierError::BatchSpansMultipleSubtrees => {
                write!(f, "batch spans more than one tracked subtree boundary")
            }
        }
    }
}

impl Error for BatchFrontierError {}

impl From<FrontierError> for BatchFrontierError {
    fn from(error: FrontierError) -> Self {
        BatchFrontierError::Frontier(error)
    }
}

/// Merges a complete subtree root into `slots`, carrying upward when a slot is
/// already occupied.
///
/// `node` must be strictly newer, further right in leaf order, than every root
/// already in `slots`. This keeps the existing slot root as the left argument to
/// [`Hashable::combine`] and the carried root as the right argument.
///
/// Example: if the existing tree has 7 leaves, `slots` has roots for leaves
/// 0..3, 4..5, and 6. Merging new leaf 7 performs:
/// - `combine(0, root(6), root(7)) -> root(6..7)`
/// - `combine(1, root(4..5), root(6..7)) -> root(4..7)`
/// - `combine(2, root(0..3), root(4..7)) -> root(0..7)`
fn merge_complete_subtree<H: Hashable + Clone>(
    slots: &mut CompleteSubtreeRoots<H>,
    level: usize,
    node: H,
) {
    let mut idx = level;
    let mut carry = node;
    loop {
        match slots[idx].take() {
            None => {
                slots[idx] = Some(carry);
                break;
            }
            Some(existing) => {
                // Combining two level-`idx` nodes yields a level-`idx+1` node;
                // the crate's `combine` takes the *children's* level.
                carry = H::combine(Level::from(idx as u8), &existing, &carry);
                idx += 1;
            }
        }
    }
}

/// Computes the root of a perfect subtree of exactly `2^k` `leaves`, using a
/// parallel divide-and-conquer reduction. The combine hashes within and across
/// the two halves are independent, so this scales across the rayon pool.
fn perfect_subtree_root<H: Hashable + Clone + Send + Sync>(leaves: &[H]) -> H {
    debug_assert!(leaves.len().is_power_of_two());
    if leaves.len() == 1 {
        return leaves[0].clone();
    }
    let half = leaves.len() / 2;
    // children level = log2(len) - 1 = log2(half)
    let child_level = Level::from(half.trailing_zeros() as u8);
    let (left, right) = leaves.split_at(half);
    let (l, r) = rayon::join(
        || perfect_subtree_root(left),
        || perfect_subtree_root(right),
    );
    H::combine(child_level, &l, &r)
}

/// Returns true if the leaves before the frontier tip include a complete
/// `2^level` subtree.
///
/// Example: position 6 is the seventh leaf, because positions are zero-based.
/// The six earlier leaves decompose as `6 == 0b110`: one complete 4-leaf
/// subtree and one complete 2-leaf subtree, so only levels 2 and 1 are present.
fn contains_complete_subtree(position: Position, level: u32) -> bool {
    u64::from(position) & (1u64 << level) != 0
}

/// Expands a [`Frontier`] into complete subtree roots indexed by level.
///
/// `slots[level]` contains the root of a complete `2^level`-leaf subtree, if
/// one exists. `None` means there is no complete subtree at that level. The
/// returned position is the next leaf index to append.
///
/// Example: position 6 is the seventh leaf, because positions are zero-based.
/// This includes that leaf, so the returned roots cover 7 leaves. Since
/// `7 == 0b111`, roots are present at levels 0, 1, and 2.
///
/// To build a root at level `L + 1`, we first need sibling roots at level `L`;
/// this is why merges only carry upward after combining two roots at the same
/// level.
///
/// The returned `complete_subtree_roots` remain indexed low-to-high by level.
/// Flattening this vector produces the complete-subtree root order expected by
/// [`Frontier::from_parts`]: from the leaf tip upward, also low-to-high by
/// level.
///
/// Merging the frontier tip into these slots can compute a carry chain of
/// hashes before the parallel batch work begins. That serial work is
/// intentional and bounded by `DEPTH`.
fn frontier_complete_subtree_roots<H, const DEPTH: u8>(
    frontier: &Frontier<H, DEPTH>,
) -> (CompleteSubtreeRoots<H>, u64)
where
    H: Hashable + Clone,
{
    let Some(frontier) = frontier.value() else {
        return (vec![None; usize::from(DEPTH)], 0);
    };

    let position = frontier.position();
    let mut slots = vec![None; usize::from(DEPTH)];
    let mut sibling_roots = frontier.ommers().iter().cloned();

    // These sibling roots represent complete subtrees before the tip. So if
    // tip is at position 6, we set this according to 6 leaves. We can read off
    // roots by looking at the set bits in `position`.
    for level in 0..u64::BITS {
        if contains_complete_subtree(position, level) {
            slots[level as usize] = Some(sibling_roots.next().expect("sibling root per set bit"));
        }
    }

    // Now merge in the tip leaf, updating hashes and completeness conditions by
    // carrying through occupied slots.
    // So tip at position 6, would now make the new slots value be correct for 7 leaves.
    // Merging the tip can drive a carry chain of hashes before the parallel batch hashing.
    merge_complete_subtree(&mut slots, 0, frontier.leaf().clone());

    (slots, u64::from(position) + 1)
}

/// Splits `leaves` into complete subtree chunks at their global positions.
///
/// Each returned chunk has length `2^level` and starts at a position divisible
/// by `2^level`, so it can be hashed independently as a complete subtree.
fn complete_subtree_chunks<H>(start_position: u64, leaves: &[H]) -> Vec<(usize, &[H])> {
    let mut chunks = Vec::new();
    let mut global_pos = start_position;
    let mut leaf_offset = 0usize;
    let end_position = start_position + leaves.len() as u64;

    while global_pos < end_position {
        let leaves_left = end_position - global_pos;
        // The chunk also has to fit inside the remaining leaves. This is the
        // largest `level` with `2^level <= leaves_left`.
        let max_available_level = u64::BITS - 1 - leaves_left.leading_zeros();

        // A `2^level` subtree can start here only if `global_pos` is divisible
        // by `2^level`. Position 0 has no alignment constraint, so only the
        // remaining leaves limit the first chunk.
        let max_aligned_level = if global_pos == 0 {
            max_available_level
        } else {
            global_pos.trailing_zeros()
        };

        // Take the largest complete subtree that is both aligned and available.
        let level = max_aligned_level.min(max_available_level) as usize;
        let chunk_len = 1usize << level;
        chunks.push((level, &leaves[leaf_offset..leaf_offset + chunk_len]));

        leaf_offset += chunk_len;
        global_pos += chunk_len as u64;
    }

    chunks
}

/// Appends `new_leaves` (in order) to `frontier`, returning the updated frontier.
///
/// The result is identical to calling [`Frontier::append`] for each leaf in turn,
/// but the Merkle merge hashes are computed in parallel.
///
/// # Method
///
/// 1. Expand the frontier into complete subtree roots indexed by level.
/// 2. Split the new leaves into complete subtree chunks.
/// 3. Compute the root for each chunk in parallel.
/// 4. Merge the roots into the complete subtree roots.
/// 5. Reconstruct a new frontier from the merged roots and the new tip leaf.
pub(crate) fn parallel_append<H, const DEPTH: u8>(
    frontier: Frontier<H, DEPTH>,
    mut new_leaves: Vec<H>,
) -> Result<Frontier<H, DEPTH>, FrontierError>
where
    H: Hashable + Clone + Send + Sync,
{
    if new_leaves.is_empty() {
        return Ok(frontier);
    }

    // complete_subtree_roots[level] is the root of a complete 2^level-leaf
    // subtree, or None if no complete subtree exists at that level.
    let (mut complete_subtree_roots, next_leaf_position) =
        frontier_complete_subtree_roots(&frontier);

    // Frontier stores the newest leaf separately and does not hash it into the
    // tree. So the last incoming leaf becomes the new tip. Earlier incoming
    // leaves are merged into subtree roots.
    let new_tip_leaf = new_leaves
        .pop()
        .expect("new_leaves is not empty because it was checked above");
    let leaves_to_merge = new_leaves;

    // Split the new leaves into the new subtree chunks.
    let chunks = complete_subtree_chunks(next_leaf_position, &leaves_to_merge);

    // Hash each new subtree chunk in parallel.
    let new_subtree_roots: Vec<(usize, H)> = chunks
        .into_par_iter()
        .map(|(level, leaves)| (level, perfect_subtree_root(leaves)))
        .collect();

    // Merge the new roots in leaf order. The roots can be computed in parallel,
    // but carries must be applied left-to-right. We accept the extra blocking
    // from not merging lower-order roots as soon as they are available.
    for (level, root) in new_subtree_roots {
        merge_complete_subtree(&mut complete_subtree_roots, level, root);
    }

    // The new tip comes after every merged leaf.
    let new_tip_position = next_leaf_position + leaves_to_merge.len() as u64;
    // `complete_subtree_roots` is indexed by level, low-to-high; `from_parts`
    // expects complete subtree roots in the same order, from the new tip upward.
    let complete_subtree_roots = complete_subtree_roots.into_iter().flatten().collect();

    Frontier::from_parts(
        Position::from(new_tip_position),
        new_tip_leaf,
        complete_subtree_roots,
    )
}

/// Appends `nodes` to `frontier` and returns the completed subtree's
/// `(index_value, root)` if the batch crosses a
/// [`TRACKED_SUBTREE_HEIGHT`](crate::subtree::TRACKED_SUBTREE_HEIGHT) boundary.
///
/// This is the shared implementation for [`crate::sapling::tree::NoteCommitmentTree::append_batch`]
/// and [`crate::orchard::tree::NoteCommitmentTree::append_batch`]. Callers convert their
/// commitment type to `H` before calling and wrap the returned index value in
/// `NoteCommitmentSubtreeIndex`.
///
/// # Batch Size
///
/// `nodes` must contain the commitments from a single block. The consensus block-size
/// cap bounds a block to far fewer than `2^TRACKED_SUBTREE_HEIGHT` (65,536) outputs or
/// actions, so a batch can cross **at most one** subtree boundary.
///
/// Returns [`BatchFrontierError`] if appending would overflow the tree's capacity,
/// or if the batch spans more than one tracked subtree boundary.
pub fn append_batch_with_subtree<H, const DEPTH: u8>(
    frontier: Frontier<H, DEPTH>,
    nodes: Vec<H>,
) -> Result<(Frontier<H, DEPTH>, Option<(u64, H)>), BatchFrontierError>
where
    H: Hashable + Clone + Send + Sync,
{
    use crate::subtree::TRACKED_SUBTREE_HEIGHT;

    if nodes.is_empty() {
        return Ok((frontier, None));
    }

    let old_size = frontier.tree_size();
    let new_size = old_size + nodes.len() as u64;
    if new_size > TreeCapacity::<DEPTH>::MAX_LEAVES {
        return Err(FrontierError::MaxDepthExceeded {
            depth: DEPTH.saturating_add(1),
        }
        .into());
    }

    // A consensus block crosses at most one tracked-subtree boundary. If a
    // caller passes a larger batch, return an error instead of dropping later
    // completed subtrees.
    let subtree_size = 1u64 << TRACKED_SUBTREE_HEIGHT;
    // Round old_size up to the next subtree boundary.
    let boundary = (old_size / subtree_size)
        .checked_add(1)
        .and_then(|n| n.checked_mul(subtree_size));
    if boundary
        .and_then(|b| b.checked_add(subtree_size))
        .is_some_and(|second_boundary| second_boundary <= new_size)
    {
        return Err(BatchFrontierError::BatchSpansMultipleSubtrees);
    }

    if boundary.is_some_and(|b| b <= new_size) {
        let boundary = boundary.expect("checked above");
        let head_len = (boundary - old_size) as usize;
        let mut head = nodes;
        let tail = head.split_off(head_len);

        let f1 = parallel_append(frontier, head)?;

        // index = (boundary / subtree_size) - 1; fits in u16 by tree depth.
        let index_value = (boundary >> TRACKED_SUBTREE_HEIGHT) - 1;
        let root = f1
            .value()
            .expect("just appended at least one leaf")
            .root(Some(Level::from(TRACKED_SUBTREE_HEIGHT)));

        let f2 = parallel_append(f1, tail)?;
        Ok((f2, Some((index_value, root))))
    } else {
        let f = parallel_append(frontier, nodes)?;
        Ok((f, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const DEPTH: u8 = 32;

    /// A self-contained test node whose `combine` is **order-sensitive** (so a
    /// left/right swap changes the result) and **level-sensitive** (so a wrong
    /// `combine` level argument changes the result). This lets the differential
    /// tests catch ordering and level bugs in the parallel append.
    ///
    /// Uses a hand-rolled FNV-style mix rather than `DefaultHasher` so the output
    /// is stable across Rust releases and proptest regression seeds stay valid.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    struct TestNode(u64);

    /// Stable, order- and level-sensitive mix of three u64 values.
    /// Based on FNV-1a with domain separation by argument position.
    fn mix3(level: u64, a: u64, b: u64) -> u64 {
        const FNV_PRIME: u64 = 0x00000100000001B3;
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        let mut h = FNV_OFFSET;
        h ^= level;
        h = h.wrapping_mul(FNV_PRIME);
        h ^= a;
        h = h.wrapping_mul(FNV_PRIME);
        h ^= b;
        h = h.wrapping_mul(FNV_PRIME);
        h
    }

    impl Hashable for TestNode {
        fn empty_leaf() -> Self {
            Self(0)
        }

        fn combine(level: Level, a: &Self, b: &Self) -> Self {
            Self(mix3(u8::from(level) as u64, a.0, b.0))
        }
    }

    /// Append `leaves` to `start` one at a time using the sequential crate API.
    fn sequential_append<const DEPTH: u8>(
        start: Frontier<TestNode, DEPTH>,
        leaves: &[TestNode],
    ) -> Frontier<TestNode, DEPTH> {
        let mut f = start;
        for leaf in leaves {
            assert!(f.append(*leaf), "test trees never overflow");
        }
        f
    }

    fn build_frontier<const DEPTH: u8>(prefix: &[TestNode]) -> Frontier<TestNode, DEPTH> {
        let mut f = Frontier::<TestNode, DEPTH>::empty();
        for leaf in prefix {
            assert!(f.append(*leaf));
        }
        f
    }

    fn chunk_levels_and_values(start_position: u64, leaves: &[u64]) -> Vec<(usize, Vec<u64>)> {
        complete_subtree_chunks(start_position, leaves)
            .into_iter()
            .map(|(level, chunk)| (level, chunk.to_vec()))
            .collect()
    }

    #[test]
    fn frontier_complete_subtree_roots_empty_frontier() {
        let empty = Frontier::<TestNode, DEPTH>::empty();

        let (complete_subtree_roots, next_leaf_position) = frontier_complete_subtree_roots(&empty);

        assert_eq!(next_leaf_position, 0);
        assert_eq!(
            complete_subtree_roots,
            vec![None; usize::from(DEPTH)],
            "empty frontier has no complete subtree roots"
        );
    }

    #[test]
    fn complete_subtree_chunks_match_expected_decompositions() {
        let cases = [
            ("empty at zero", 0, vec![], vec![]),
            ("empty after nonzero position", 17, vec![], vec![]),
            (
                "start at zero",
                0,
                vec![0, 1, 2, 3, 4, 5, 6],
                vec![(2, vec![0, 1, 2, 3]), (1, vec![4, 5]), (0, vec![6])],
            ),
            (
                "aligned start",
                8,
                vec![10, 11, 12, 13, 14, 15, 16, 17],
                vec![(3, vec![10, 11, 12, 13, 14, 15, 16, 17])],
            ),
            (
                "unaligned start",
                6,
                vec![100, 101, 102, 103, 104, 105, 106],
                vec![
                    (1, vec![100, 101]),
                    (2, vec![102, 103, 104, 105]),
                    (0, vec![106]),
                ],
            ),
            (
                "preserve global alignment",
                5,
                vec![20, 21, 22, 23, 24, 25],
                vec![
                    (0, vec![20]),
                    (1, vec![21, 22]),
                    (1, vec![23, 24]),
                    (0, vec![25]),
                ],
            ),
        ];

        for (name, start_position, leaves, expected) in cases {
            assert_eq!(
                chunk_levels_and_values(start_position, &leaves),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn complete_subtree_roots_flatten_to_frontier_order() {
        let interesting_prefix_lengths = [
            0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65,
        ];

        for prefix_len in interesting_prefix_lengths {
            let prefix_len = u64::try_from(prefix_len).expect("test prefix length fits in u64");
            let prefix: Vec<TestNode> = (0..prefix_len).map(TestNode).collect();
            let start = build_frontier::<DEPTH>(&prefix);
            let new_tip_leaf = TestNode(10_000 + prefix_len);

            let (complete_subtree_roots, next_leaf_position) =
                frontier_complete_subtree_roots(&start);
            assert_eq!(
                next_leaf_position, prefix_len,
                "next leaf position mismatch for prefix length {prefix_len}"
            );

            let complete_subtree_roots: Vec<TestNode> =
                complete_subtree_roots.into_iter().flatten().collect();
            let reconstructed = Frontier::<TestNode, DEPTH>::from_parts(
                Position::from(next_leaf_position),
                new_tip_leaf,
                complete_subtree_roots,
            )
            .expect("test frontier reconstruction succeeds");
            let sequential = sequential_append::<DEPTH>(start, &[new_tip_leaf]);

            assert_eq!(
                sequential.value().map(|f| f.clone().into_parts()),
                reconstructed.value().map(|f| f.clone().into_parts()),
                "flattened complete_subtree_roots must be ordered for Frontier::from_parts at prefix length {prefix_len}"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        /// The parallel batch append must produce a byte-identical frontier (and
        /// therefore an identical root) to the sequential append, for any
        /// starting tree size and any batch size.
        #[test]
        fn parallel_matches_sequential(
            prefix_len in 0usize..300,
            batch in proptest::collection::vec(any::<u64>().prop_map(TestNode), 0..300),
        ) {
            let prefix: Vec<TestNode> = (0..prefix_len as u64).map(TestNode).collect();
            let start = build_frontier::<DEPTH>(&prefix);

            let seq = sequential_append::<DEPTH>(start.clone(), &batch);
            let par = parallel_append(start, batch.clone()).expect("no overflow in tests");

            prop_assert_eq!(seq.root(), par.root(), "root mismatch");
            prop_assert_eq!(
                seq.value().map(|f| f.clone().into_parts()),
                par.value().map(|f| f.clone().into_parts()),
                "frontier parts mismatch"
            );
        }
    }

    /// Spot-check small exhaustive sizes for off-by-one boundary bugs.
    #[test]
    fn exhaustive_small() {
        for prefix_len in 0u64..40 {
            let prefix: Vec<TestNode> = (0..prefix_len).map(TestNode).collect();
            let start = build_frontier::<DEPTH>(&prefix);
            for batch_len in 0u64..40 {
                let batch: Vec<TestNode> = (1000..1000 + batch_len).map(TestNode).collect();
                let seq = sequential_append::<DEPTH>(start.clone(), &batch);
                let par = parallel_append(start.clone(), batch).expect("no overflow");
                assert_eq!(
                    seq.root(),
                    par.root(),
                    "root mismatch p={prefix_len} b={batch_len}"
                );
                assert_eq!(
                    seq.value().map(|f| f.clone().into_parts()),
                    par.value().map(|f| f.clone().into_parts()),
                    "parts mismatch p={prefix_len} b={batch_len}"
                );
            }
        }
    }

    /// A full batch append should either succeed completely or report overflow.
    #[test]
    fn overflow_is_reported() {
        const SMALL_DEPTH: u8 = 3;

        let prefix: Vec<TestNode> = (0..7).map(TestNode).collect();
        let start = build_frontier::<SMALL_DEPTH>(&prefix);
        let exact_capacity_batch = [TestNode(100)];

        let seq = sequential_append::<SMALL_DEPTH>(start.clone(), &exact_capacity_batch);
        let par = parallel_append(start.clone(), exact_capacity_batch.to_vec())
            .expect("one remaining leaf fits");

        assert_eq!(seq.root(), par.root(), "root mismatch at exact capacity");
        assert_eq!(
            seq.value().map(|f| f.clone().into_parts()),
            par.value().map(|f| f.clone().into_parts()),
            "parts mismatch at exact capacity"
        );

        let empty_append = parallel_append(par.clone(), Vec::new()).expect("empty append succeeds");
        assert_eq!(
            par.value().map(|f| f.clone().into_parts()),
            empty_append.value().map(|f| f.clone().into_parts()),
            "empty append changed a full frontier"
        );

        let full_tree_overflow = append_batch_with_subtree(par, vec![TestNode(101)]);
        assert!(
            full_tree_overflow.is_err(),
            "appending to a full tree overflows"
        );

        let partial_batch_overflow =
            append_batch_with_subtree(start, vec![TestNode(100), TestNode(101)]);
        assert!(
            partial_batch_overflow.is_err(),
            "batch crossing tree capacity overflows"
        );
    }

    /// Batches that would complete more than one tracked subtree are rejected,
    /// because the return type can only report one completed subtree.
    #[test]
    fn append_batch_errors_on_multiple_subtree_boundaries() {
        use crate::subtree::TRACKED_SUBTREE_HEIGHT;

        let start = Frontier::<TestNode, DEPTH>::empty();
        let subtree_size = 1usize << TRACKED_SUBTREE_HEIGHT;
        let batch = vec![TestNode(0); subtree_size * 2];

        let result = append_batch_with_subtree(start, batch);

        assert_eq!(result, Err(BatchFrontierError::BatchSpansMultipleSubtrees));
    }

    /// Deterministic positions around powers of two exercise carry propagation and
    /// globally aligned dyadic block decomposition beyond the small exhaustive range.
    #[test]
    fn matches_sequential_at_alignment_boundaries() {
        let interesting_prefix_lengths = [
            0usize, 1, 2, 3, 7, 8, 9, 15, 16, 17, 255, 256, 257, 65_535, 65_536, 65_537,
        ];
        let interesting_batch_lengths = [0usize, 1, 2, 3, 4, 5, 31, 32, 33];
        let max_prefix_len = *interesting_prefix_lengths
            .last()
            .expect("interesting prefixes are non-empty");

        let mut frontier = Frontier::<TestNode, DEPTH>::empty();
        let mut snapshots = Vec::new();

        for prefix_len in 0..=max_prefix_len {
            if interesting_prefix_lengths.contains(&prefix_len) {
                snapshots.push((prefix_len, frontier.clone()));
            }

            if prefix_len < max_prefix_len {
                assert!(frontier.append(TestNode(
                    u64::try_from(prefix_len).expect("test prefix length fits in u64")
                )));
            }
        }

        for (prefix_len, start) in snapshots {
            for batch_len in interesting_batch_lengths {
                let prefix_len = u64::try_from(prefix_len).expect("test prefix length fits in u64");
                let batch: Vec<TestNode> = (0..batch_len)
                    .map(|leaf| {
                        TestNode(
                            1_000_000
                                + prefix_len
                                + u64::try_from(leaf).expect("test batch length fits in u64"),
                        )
                    })
                    .collect();

                let seq = sequential_append::<DEPTH>(start.clone(), &batch);
                let par = parallel_append(start.clone(), batch).expect("no overflow");

                assert_eq!(
                    seq.root(),
                    par.root(),
                    "root mismatch p={prefix_len} b={batch_len}"
                );
                assert_eq!(
                    seq.value().map(|f| f.clone().into_parts()),
                    par.value().map(|f| f.clone().into_parts()),
                    "parts mismatch p={prefix_len} b={batch_len}"
                );
            }
        }
    }

    /// `merge_complete_subtree` carry chain: inserting leaves 0–7 one at a time
    /// must produce the same slot state as building the 8-leaf tree sequentially.
    ///
    /// This directly exercises the left/right argument order in `combine` and the
    /// level-index increment during carry propagation.
    #[test]
    fn merge_complete_subtree_carry_chain() {
        let leaves: Vec<TestNode> = (0u64..8).map(TestNode).collect();

        // Build expected slots by appending leaves one at a time sequentially,
        // then expanding the resulting frontier.
        let frontier = build_frontier::<DEPTH>(&leaves);
        let (expected_slots, expected_next) = frontier_complete_subtree_roots(&frontier);

        // Build actual slots by calling merge_complete_subtree for each leaf.
        let mut slots: CompleteSubtreeRoots<TestNode> = vec![None; usize::from(DEPTH)];
        for leaf in &leaves {
            merge_complete_subtree(&mut slots, 0, *leaf);
        }

        assert_eq!(
            slots, expected_slots,
            "slot state after merging 8 leaves must match frontier expansion"
        );
        assert_eq!(
            expected_next, 8,
            "frontier covering 8 leaves has next position 8"
        );
    }

    /// `perfect_subtree_root` must produce the same root as sequential append
    /// for power-of-two-sized leaf slices.
    ///
    /// After appending 2^k leaves the frontier expansion places their combined
    /// root at `slots[k]`. We compare `perfect_subtree_root` against that slot.
    #[test]
    fn perfect_subtree_root_matches_sequential() {
        for log2_len in 0usize..=4 {
            let len = 1usize << log2_len;
            let leaves: Vec<TestNode> = (0..len as u64).map(TestNode).collect();

            let frontier = build_frontier::<DEPTH>(&leaves);
            let (slots, _) = frontier_complete_subtree_roots(&frontier);
            let sequential_root =
                slots[log2_len].expect("complete 2^k subtree fills exactly slot k after expansion");

            let parallel_root = perfect_subtree_root(&leaves);

            assert_eq!(
                sequential_root, parallel_root,
                "perfect_subtree_root mismatch for 2^{log2_len} leaves"
            );
        }
    }
}
