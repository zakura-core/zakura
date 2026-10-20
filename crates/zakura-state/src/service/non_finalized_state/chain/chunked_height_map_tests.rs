//! Chain-level regressions for the chunked ordered anchor and tree height indexes.

use super::*;
use crate::arbitrary::Prepare;
use proptest::prelude::*;
use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

proptest! {
    /// Root removal collects the chunked `range(..=threshold)`; it must equal the earlier full-key
    /// filter over an ordered map.
    #[test]
    fn root_prefix_range_matches_the_previous_ordered_filter(
        heights in prop::collection::vec(any::<u32>(), 0..128),
        threshold in any::<u32>(),
    ) {
        let ordered: BTreeMap<_, _> = heights.iter().map(|height| (Height(*height), ())).collect();
        let mut chunked = HeightMap::default();
        for height in heights {
            chunked.insert(Height(height), ());
        }
        let threshold = Height(threshold);
        let previous: Vec<_> = ordered.keys().copied().filter(|height| *height <= threshold).collect();
        let prefix: Vec<_> = chunked.range(..=threshold).map(|(height, _)| *height).collect();
        prop_assert_eq!(prefix, previous);
    }
}

#[test]
fn chunked_pool_indices_preserve_root_tip_counts_and_shared_fork_snapshots() {
    let _guard = zakura_test::init();
    let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("the shared block fixture is valid");
    let contextual = Arc::new(ContextuallyVerifiedBlock::test_with_zero_chain_pool_change(
        Arc::new(block).prepare(),
    ));

    macro_rules! check_pool {
        ($tree_type:ty, $add:ident, $remove:ident, $trees:ident, $by_height:ident, $counts:ident) => {{
            let mut chain = Chain::default();
            // A retained tip makes root pruning restore the next sparse tree.
            chain.blocks.insert(Height(193), contextual.clone());
            let tree: Arc<$tree_type> = Arc::new(Default::default());
            for height in 0..=193 {
                chain.$add(Height(height), tree.clone());
            }
            assert_eq!(
                chain.$trees.len(),
                1,
                "identical roots share one sparse tree"
            );
            let sibling = chain.clone();
            let sibling_before = sibling.inner.clone();
            for threshold in [1, 63, 64, 65, 127, 128, 191, 192] {
                let mut expected_heights: BTreeMap<_, _> = chain
                    .$by_height
                    .iter()
                    .map(|(height, root)| (*height, *root))
                    .collect();
                expected_heights.retain(|height, _| *height > Height(threshold));
                let mut expected_counts = CountedSet::new();
                for root in expected_heights.values() {
                    expected_counts.insert(*root);
                }
                chain.$remove(RevertPosition::Root, Height(threshold));
                assert_eq!(
                    chain
                        .$by_height
                        .iter()
                        .map(|(height, root)| (*height, *root))
                        .collect::<BTreeMap<_, _>>(),
                    expected_heights
                );
                assert_eq!(chain.$counts, expected_counts);
                if threshold < 193 {
                    let (height, restored) = chain.$trees.first_key_value().unwrap();
                    assert_eq!(*height, Height(threshold + 1));
                    assert!(Arc::ptr_eq(restored, &tree));
                    assert_eq!(chain.$trees.len(), 1);
                } else {
                    assert!(chain.$trees.is_empty());
                    assert!(chain.$counts.is_empty());
                }
                assert_eq!(sibling.inner, sibling_before);
            }
            chain.$remove(RevertPosition::Tip, Height(193));
            assert!(chain.$by_height.is_empty());
            assert!(chain.$trees.is_empty());
            assert!(chain.$counts.is_empty());
            assert_eq!(sibling.inner, sibling_before);
        }};
    }

    check_pool!(
        sprout::tree::NoteCommitmentTree,
        add_sprout_tree_and_anchor,
        remove_sprout_tree_and_anchor,
        sprout_trees_by_height,
        sprout_anchors_by_height,
        sprout_anchors
    );
    check_pool!(
        sapling::tree::NoteCommitmentTree,
        add_sapling_tree_and_anchor,
        remove_sapling_tree_and_anchor,
        sapling_trees_by_height,
        sapling_anchors_by_height,
        sapling_anchors
    );
    check_pool!(
        orchard::tree::NoteCommitmentTree,
        add_orchard_tree_and_anchor,
        remove_orchard_tree_and_anchor,
        orchard_trees_by_height,
        orchard_anchors_by_height,
        orchard_anchors
    );
    check_pool!(
        ironwood::tree::NoteCommitmentTree,
        add_ironwood_tree_and_anchor,
        remove_ironwood_tree_and_anchor,
        ironwood_trees_by_height,
        ironwood_anchors_by_height,
        ironwood_anchors
    );
}

#[test]
fn history_tree_root_removal_matches_the_previous_filter_and_keeps_snapshots() {
    let _guard = zakura_test::init();
    let tree = Arc::new(HistoryTree::default());
    let mut chain = Chain::default();
    for height in 0..=193 {
        chain.add_history_tree(Height(height), tree.clone());
    }
    let sibling = chain.clone();
    let sibling_before = sibling.inner.clone();
    for threshold in [0, 1, 63, 64, 65, 127, 128, 191, 192] {
        let expected: Vec<_> = chain
            .history_trees_by_height
            .iter()
            .map(|(height, _)| *height)
            .filter(|height| *height > Height(threshold))
            .collect();
        chain.remove_history_tree(RevertPosition::Root, Height(threshold));
        assert_eq!(
            chain
                .history_trees_by_height
                .iter()
                .map(|(height, _)| *height)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(sibling.inner, sibling_before);
    }
    chain.remove_history_tree(RevertPosition::Tip, Height(193));
    assert!(chain.history_trees_by_height.is_empty());
    assert_eq!(sibling.inner, sibling_before);
}
