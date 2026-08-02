use std::sync::Arc;

use crate::{
    block::{
        Block,
        Commitment::{self, ChainHistoryActivationReserved},
    },
    history_tree::{HistoryTree, HistoryTreeBlockParts, NonEmptyHistoryTree},
    ironwood, orchard,
    parameters::{
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkUpgrade,
    },
    primitives::zcash_history::{Tree as PrimitiveHistoryTree, V1, V2, V3},
    sapling,
    serialization::ZcashDeserializeInto,
};

use color_eyre::eyre;
use eyre::Result;

/// Test the history tree using the activation block of a network upgrade
/// and its next block.
///
/// This test is very similar to the zcash_history test in
/// zakura-chain/src/primitives/zcash_history/tests/vectors.rs, but with the
/// higher level API.
#[test]
fn push_and_prune() -> Result<()> {
    for network in Network::iter() {
        push_and_prune_for_network_upgrade(network.clone(), NetworkUpgrade::Heartwood)?;
        push_and_prune_for_network_upgrade(network, NetworkUpgrade::Canopy)?;
    }
    Ok(())
}

fn push_and_prune_for_network_upgrade(
    network: Network,
    network_upgrade: NetworkUpgrade,
) -> Result<()> {
    let (blocks, sapling_roots) = network.block_sapling_roots_map();

    let height = network_upgrade.activation_height(&network).unwrap().0;

    // Load first block (activation block of the given network upgrade)
    let first_block = Arc::new(
        blocks
            .get(&height)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );

    // Check its commitment
    let first_commitment = first_block.commitment(&network)?;
    if network_upgrade == NetworkUpgrade::Heartwood {
        // Heartwood is the only upgrade that has a reserved value.
        // (For other upgrades we could compare with the expected commitment,
        // but we haven't calculated them.)
        assert_eq!(first_commitment, ChainHistoryActivationReserved);
    }

    // Build initial history tree with only the first block
    let first_sapling_root =
        sapling::tree::Root::try_from(**sapling_roots.get(&height).expect("test vector exists"))?;
    let mut tree = NonEmptyHistoryTree::from_block(
        &network,
        first_block,
        &first_sapling_root,
        &Default::default(),
        &Default::default(),
    )?;

    assert_eq!(tree.size(), 1);
    assert_eq!(tree.peaks().len(), 1);
    assert_eq!(tree.current_height().0, height);

    // Compute root hash of the history tree, which will be included in the next block
    let first_root = tree.hash();

    // Load second block (activation + 1)
    let second_block = Arc::new(
        blocks
            .get(&(height + 1))
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );

    // Check its commitment
    let second_commitment = second_block.commitment(&network)?;
    assert_eq!(second_commitment, Commitment::ChainHistoryRoot(first_root));

    // Append second block to history tree
    let second_sapling_root = sapling::tree::Root::try_from(
        **sapling_roots
            .get(&(height + 1))
            .expect("test vector exists"),
    )?;
    tree.push(
        second_block,
        &second_sapling_root,
        &Default::default(),
        &Default::default(),
    )
    .unwrap();

    // Adding a second block will produce a 3-node tree (one parent and two leaves).
    assert_eq!(tree.size(), 3);
    // The tree must have been pruned, resulting in a single peak (the parent).
    assert_eq!(tree.peaks().len(), 1);
    assert_eq!(tree.current_height().0, height + 1);

    Ok(())
}

/// Test that the parts API builds the same tree as the full-block API.
#[test]
fn parts_api_matches_block_api() -> Result<()> {
    for network in Network::iter() {
        parts_api_matches_block_api_for_network(network)?;
    }

    Ok(())
}

fn parts_api_matches_block_api_for_network(network: Network) -> Result<()> {
    let (blocks, sapling_roots) = network.block_sapling_roots_map();
    let heartwood_height = NetworkUpgrade::Heartwood
        .activation_height(&network)
        .expect("test networks have Heartwood activation")
        .0;

    let genesis_block = Arc::new(
        blocks
            .get(&0)
            .expect("genesis test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("genesis block is structurally valid"),
    );

    let first_block = Arc::new(
        blocks
            .get(&heartwood_height)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );
    let first_sapling_root = sapling::tree::Root::try_from(
        **sapling_roots
            .get(&heartwood_height)
            .expect("test vector exists"),
    )?;

    let second_block = Arc::new(
        blocks
            .get(&(heartwood_height + 1))
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );
    let second_sapling_root = sapling::tree::Root::try_from(
        **sapling_roots
            .get(&(heartwood_height + 1))
            .expect("test vector exists"),
    )?;

    let mut block_tree = HistoryTree::default();
    let mut parts_tree = HistoryTree::default();

    // Pushing a pre-Heartwood block (genesis) must be accepted as a no-op by
    // both APIs, leaving the trees empty.
    block_tree.push(
        &network,
        genesis_block.clone(),
        &Default::default(),
        &Default::default(),
        &Default::default(),
    )?;
    parts_tree.push_from_parts(
        &network,
        HistoryTreeBlockParts::from_block(
            &genesis_block,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        ),
    )?;

    assert!(block_tree.is_none());
    assert!(parts_tree.is_none());

    block_tree.push(
        &network,
        first_block.clone(),
        &first_sapling_root,
        &Default::default(),
        &Default::default(),
    )?;
    parts_tree.push_from_parts(
        &network,
        HistoryTreeBlockParts::from_block(
            &first_block,
            &first_sapling_root,
            &Default::default(),
            &Default::default(),
        ),
    )?;

    assert_eq!(parts_tree.hash(), block_tree.hash());
    // The real block after Heartwood activation commits to the MMR containing
    // only the activation block, anchoring both APIs to the network's actual
    // chain history commitment rather than just to each other.
    assert_eq!(
        second_block.commitment(&network)?,
        Commitment::ChainHistoryRoot(
            block_tree
                .hash()
                .expect("history tree exists after Heartwood activation")
        )
    );

    block_tree.push(
        &network,
        second_block.clone(),
        &second_sapling_root,
        &Default::default(),
        &Default::default(),
    )?;
    parts_tree.push_from_parts(
        &network,
        HistoryTreeBlockParts::from_block(
            &second_block,
            &second_sapling_root,
            &Default::default(),
            &Default::default(),
        ),
    )?;

    assert_eq!(parts_tree.hash(), block_tree.hash());

    Ok(())
}

/// Cached history entries must retain the V3 Ironwood suffix at and after NU6.3.
#[test]
fn from_cache_preserves_ironwood_history_version() -> Result<()> {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(2),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let block = blocks
        .get(&1)
        .expect("test vector exists")
        .zcash_deserialize_into::<Block>()
        .expect("block is structurally valid");
    let mut ironwood_root_bytes = [0; 32];
    ironwood_root_bytes[0] = 1;
    let ironwood_root = ironwood::tree::Root::try_from(ironwood_root_bytes)?;
    assert_ne!(ironwood_root, Default::default());

    for (height, expected_upgrade) in [
        (crate::block::Height(1), NetworkUpgrade::Nu6_3),
        (crate::block::Height(2), NetworkUpgrade::Nu7),
    ] {
        assert_eq!(NetworkUpgrade::current(&network, height), expected_upgrade);

        let tree = NonEmptyHistoryTree::from_parts(
            &network,
            HistoryTreeBlockParts {
                header: &block.header,
                height,
                sapling_root: &Default::default(),
                orchard_root: &Default::default(),
                ironwood_root: &ironwood_root,
                sapling_tx: 0,
                orchard_tx: 0,
                ironwood_tx: 1,
            },
        )?;
        // Decoding the same peaks as V2 succeeds, because the serialized V3
        // entries are just V2 entries with a trailing Ironwood suffix. The
        // resulting hash silently loses the suffix, which is why `from_cache`
        // must select the tree version from the network upgrade.
        let v2_decode = PrimitiveHistoryTree::<V2>::new_from_cache(
            &network,
            expected_upgrade,
            tree.size(),
            tree.peaks(),
            &Default::default(),
        )?;
        let restored = NonEmptyHistoryTree::from_cache(
            &network,
            tree.size(),
            tree.peaks().clone(),
            tree.current_height(),
        )?;

        assert_ne!(
            v2_decode.hash(),
            tree.hash(),
            "{expected_upgrade:?} V2 decoding silently drops the V3 suffix"
        );
        assert_eq!(
            restored.hash(),
            tree.hash(),
            "{expected_upgrade:?} cache reconstruction must retain V3 fields"
        );
    }

    Ok(())
}

/// `NonEmptyHistoryTree::from_block` must build the same tree as
/// `NonEmptyHistoryTree::from_parts` for the V1 (pre-NU5), V2 (NU5+), and
/// V3 (NU6.3+) history-tree versions.
#[test]
fn from_block_delegation_preserves_all_history_versions() -> Result<()> {
    let v1_network = Network::new_regtest(RegtestParameters::default());
    let v2_network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let v3_network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let block = Arc::new(
        blocks
            .get(&1)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );
    let sapling_root = sapling::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;
    let mut ironwood_root_bytes = [0; 32];
    ironwood_root_bytes[0] = 1;
    let ironwood_root = ironwood::tree::Root::try_from(ironwood_root_bytes)?;

    for network in [v1_network, v2_network, v3_network] {
        let block_tree = NonEmptyHistoryTree::from_block(
            &network,
            block.clone(),
            &sapling_root,
            &orchard_root,
            &ironwood_root,
        )?;
        let parts_tree = NonEmptyHistoryTree::from_parts(
            &network,
            HistoryTreeBlockParts {
                header: &block.header,
                height: block.coinbase_height().expect("test block has a height"),
                sapling_root: &sapling_root,
                orchard_root: &orchard_root,
                ironwood_root: &ironwood_root,
                sapling_tx: block.sapling_transactions_count(),
                orchard_tx: block.orchard_transactions_count(),
                ironwood_tx: block.ironwood_transactions_count(),
            },
        )?;

        assert_eq!(block_tree.hash(), parts_tree.hash());
    }

    Ok(())
}

/// `NonEmptyHistoryTree::from_parts` must select the history-leaf version from
/// the network upgrade at the block's height: V1 leaves ignore the Orchard and
/// Ironwood fields, V2 leaves commit to Orchard but ignore Ironwood, and V3
/// leaves commit to everything. Each tree is also decoded with the matching
/// raw `zcash_history` version to pin which version was selected.
#[test]
fn from_parts_selects_epoch_version_and_ignores_future_fields() -> Result<()> {
    let v1_network = Network::new_regtest(RegtestParameters::default());
    let v2_network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let v3_network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let block = blocks
        .get(&1)
        .expect("test vector exists")
        .zcash_deserialize_into::<Block>()
        .expect("block is structurally valid");
    let sapling_root = sapling::tree::Root::default();
    let empty_orchard_root = orchard::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;
    let empty_ironwood_root = ironwood::tree::Root::default();
    let mut ironwood_root_bytes = [0; 32];
    ironwood_root_bytes[0] = 1;
    let ironwood_root = ironwood::tree::Root::try_from(ironwood_root_bytes)?;
    assert_ne!(orchard_root, empty_orchard_root);
    assert_ne!(ironwood_root, empty_ironwood_root);
    let height = crate::block::Height(1);

    let parts = |orchard_root, ironwood_root, orchard_tx, ironwood_tx| HistoryTreeBlockParts {
        header: &block.header,
        height,
        sapling_root: &sapling_root,
        orchard_root,
        ironwood_root,
        sapling_tx: 1,
        orchard_tx,
        ironwood_tx,
    };

    assert_eq!(
        NetworkUpgrade::current(&v1_network, height),
        NetworkUpgrade::Canopy
    );
    let v1_tree =
        NonEmptyHistoryTree::from_parts(&v1_network, parts(&orchard_root, &ironwood_root, 2, 3))?;
    let v1_without_future_fields = NonEmptyHistoryTree::from_parts(
        &v1_network,
        parts(&empty_orchard_root, &empty_ironwood_root, 0, 0),
    )?;
    let v1_decoded = PrimitiveHistoryTree::<V1>::new_from_cache(
        &v1_network,
        NetworkUpgrade::Canopy,
        v1_tree.size(),
        v1_tree.peaks(),
        &Default::default(),
    )?;
    assert_eq!(v1_tree.hash(), v1_decoded.hash());
    assert_eq!(v1_tree.hash(), v1_without_future_fields.hash());

    assert_eq!(
        NetworkUpgrade::current(&v2_network, height),
        NetworkUpgrade::Nu5
    );
    let v2_tree =
        NonEmptyHistoryTree::from_parts(&v2_network, parts(&orchard_root, &ironwood_root, 2, 3))?;
    let v2_without_future_fields = NonEmptyHistoryTree::from_parts(
        &v2_network,
        parts(&orchard_root, &empty_ironwood_root, 2, 0),
    )?;
    let v2_decoded = PrimitiveHistoryTree::<V2>::new_from_cache(
        &v2_network,
        NetworkUpgrade::Nu5,
        v2_tree.size(),
        v2_tree.peaks(),
        &Default::default(),
    )?;
    assert_eq!(v2_tree.hash(), v2_decoded.hash());
    assert_eq!(v2_tree.hash(), v2_without_future_fields.hash());

    assert_eq!(
        NetworkUpgrade::current(&v3_network, height),
        NetworkUpgrade::Nu6_3
    );
    let v3_tree =
        NonEmptyHistoryTree::from_parts(&v3_network, parts(&orchard_root, &ironwood_root, 2, 3))?;
    let v3_without_ironwood_fields = NonEmptyHistoryTree::from_parts(
        &v3_network,
        parts(&orchard_root, &empty_ironwood_root, 2, 0),
    )?;
    let v3_decoded = PrimitiveHistoryTree::<V3>::new_from_cache(
        &v3_network,
        NetworkUpgrade::Nu6_3,
        v3_tree.size(),
        v3_tree.peaks(),
        &Default::default(),
    )?;
    assert_eq!(v3_tree.hash(), v3_decoded.hash());
    assert_ne!(v3_tree.hash(), v3_without_ironwood_fields.hash());

    Ok(())
}

/// Pushing the NU6.3 activation block must reset the MMR to a single V3 leaf
/// that commits to the Ironwood root, via both the block and parts APIs.
#[test]
fn push_wrapper_matches_parts_across_nu6_3_activation() -> Result<()> {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_2: Some(1),
            nu6_3: Some(2),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let first_block = Arc::new(
        blocks
            .get(&1)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );
    let activation_block = Arc::new(
        blocks
            .get(&2)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );
    let sapling_root = sapling::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;
    let mut ironwood_root_bytes = [0; 32];
    ironwood_root_bytes[0] = 1;
    let ironwood_root = ironwood::tree::Root::try_from(ironwood_root_bytes)?;

    assert_eq!(
        NetworkUpgrade::current(&network, crate::block::Height(1)),
        NetworkUpgrade::Nu6_2
    );
    assert_eq!(
        NetworkUpgrade::current(&network, crate::block::Height(2)),
        NetworkUpgrade::Nu6_3
    );

    let mut block_tree = NonEmptyHistoryTree::from_block(
        &network,
        first_block.clone(),
        &sapling_root,
        &orchard_root,
        &ironwood_root,
    )?;
    let mut parts_tree = NonEmptyHistoryTree::from_parts(
        &network,
        HistoryTreeBlockParts::from_block(
            &first_block,
            &sapling_root,
            &orchard_root,
            &ironwood_root,
        ),
    )?;

    block_tree.push(
        activation_block.clone(),
        &sapling_root,
        &orchard_root,
        &ironwood_root,
    )?;
    parts_tree.push_from_parts(HistoryTreeBlockParts::from_block(
        &activation_block,
        &sapling_root,
        &orchard_root,
        &ironwood_root,
    ))?;
    let activation_tree = NonEmptyHistoryTree::from_block(
        &network,
        activation_block.clone(),
        &sapling_root,
        &orchard_root,
        &ironwood_root,
    )?;
    let activation_without_ironwood = NonEmptyHistoryTree::from_block(
        &network,
        activation_block,
        &sapling_root,
        &orchard_root,
        &Default::default(),
    )?;

    assert_eq!(block_tree.hash(), parts_tree.hash());
    assert_eq!(block_tree.hash(), activation_tree.hash());
    assert_ne!(block_tree.hash(), activation_without_ironwood.hash());
    assert_eq!(
        block_tree.size(),
        1,
        "a new consensus branch resets the MMR"
    );
    assert_eq!(block_tree.current_height(), crate::block::Height(2));

    Ok(())
}

/// `push_from_parts` must append leaves using the tree's history version:
/// V1 leaves ignore the Orchard and Ironwood fields, V2 leaves commit to
/// Orchard but ignore Ironwood, and V3 leaves commit to Ironwood as well.
#[test]
fn push_from_parts_routes_each_history_version() -> Result<()> {
    let v1_network = Network::new_regtest(RegtestParameters::default());
    let v2_network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let v3_network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let first_block = blocks
        .get(&1)
        .expect("test vector exists")
        .zcash_deserialize_into::<Block>()
        .expect("block is structurally valid");
    let second_block = blocks
        .get(&2)
        .expect("test vector exists")
        .zcash_deserialize_into::<Block>()
        .expect("block is structurally valid");
    let sapling_root = sapling::tree::Root::default();
    let empty_orchard_root = orchard::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;
    let empty_ironwood_root = ironwood::tree::Root::default();
    let mut first_ironwood_root_bytes = [0; 32];
    first_ironwood_root_bytes[0] = 1;
    let first_ironwood_root = ironwood::tree::Root::try_from(first_ironwood_root_bytes)?;
    let mut second_ironwood_root_bytes = [0; 32];
    second_ironwood_root_bytes[0] = 2;
    let second_ironwood_root = ironwood::tree::Root::try_from(second_ironwood_root_bytes)?;
    assert_ne!(orchard_root, empty_orchard_root);
    assert_ne!(first_ironwood_root, empty_ironwood_root);

    fn parts<'a>(
        block: &'a Block,
        sapling_root: &'a sapling::tree::Root,
        orchard_root: &'a orchard::tree::Root,
        ironwood_root: &'a ironwood::tree::Root,
        orchard_tx: u64,
        ironwood_tx: u64,
    ) -> HistoryTreeBlockParts<'a> {
        HistoryTreeBlockParts {
            header: &block.header,
            height: block.coinbase_height().expect("test block has a height"),
            sapling_root,
            orchard_root,
            ironwood_root,
            sapling_tx: 1,
            orchard_tx,
            ironwood_tx,
        }
    }
    let two_leaf_tree = |network: &Network,
                         second_orchard_root,
                         second_ironwood_root,
                         second_orchard_tx,
                         second_ironwood_tx|
     -> Result<NonEmptyHistoryTree> {
        let mut tree = NonEmptyHistoryTree::from_parts(
            network,
            parts(
                &first_block,
                &sapling_root,
                &orchard_root,
                &first_ironwood_root,
                2,
                3,
            ),
        )?;
        tree.push_from_parts(parts(
            &second_block,
            &sapling_root,
            second_orchard_root,
            second_ironwood_root,
            second_orchard_tx,
            second_ironwood_tx,
        ))?;
        Ok(tree)
    };

    let v1_tree = two_leaf_tree(&v1_network, &orchard_root, &second_ironwood_root, 5, 7)?;
    let v1_without_future_fields =
        two_leaf_tree(&v1_network, &empty_orchard_root, &empty_ironwood_root, 0, 0)?;
    assert_eq!(v1_tree.hash(), v1_without_future_fields.hash());

    let v2_tree = two_leaf_tree(&v2_network, &orchard_root, &second_ironwood_root, 5, 7)?;
    let v2_without_ironwood =
        two_leaf_tree(&v2_network, &orchard_root, &empty_ironwood_root, 5, 0)?;
    let v2_without_orchard =
        two_leaf_tree(&v2_network, &empty_orchard_root, &empty_ironwood_root, 0, 0)?;
    assert_eq!(v2_tree.hash(), v2_without_ironwood.hash());
    assert_ne!(v2_tree.hash(), v2_without_orchard.hash());

    let v3_tree = two_leaf_tree(&v3_network, &orchard_root, &second_ironwood_root, 5, 7)?;
    let v3_without_ironwood =
        two_leaf_tree(&v3_network, &orchard_root, &empty_ironwood_root, 5, 0)?;
    assert_ne!(v3_tree.hash(), v3_without_ironwood.hash());

    Ok(())
}

/// `try_extend` must forward each block's Ironwood root to `push` in order:
/// extending matches a sequence of individual pushes, and changing only the
/// last block's Ironwood root changes the resulting tree.
#[test]
fn try_extend_forwards_ironwood_roots_in_order() -> Result<()> {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let block = |height| {
        Arc::new(
            blocks
                .get(&height)
                .expect("test vector exists")
                .zcash_deserialize_into::<Block>()
                .expect("block is structurally valid"),
        )
    };
    let first_block = block(1);
    let second_block = block(2);
    let third_block = block(3);
    let sapling_root = sapling::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;
    let mut first_ironwood_root_bytes = [0; 32];
    first_ironwood_root_bytes[0] = 1;
    let first_ironwood_root = ironwood::tree::Root::try_from(first_ironwood_root_bytes)?;
    let mut second_ironwood_root_bytes = [0; 32];
    second_ironwood_root_bytes[0] = 2;
    let second_ironwood_root = ironwood::tree::Root::try_from(second_ironwood_root_bytes)?;
    let mut third_ironwood_root_bytes = [0; 32];
    third_ironwood_root_bytes[0] = 3;
    let third_ironwood_root = ironwood::tree::Root::try_from(third_ironwood_root_bytes)?;

    let mut sequential = NonEmptyHistoryTree::from_block(
        &network,
        first_block.clone(),
        &sapling_root,
        &orchard_root,
        &first_ironwood_root,
    )?;
    sequential.push(
        second_block.clone(),
        &sapling_root,
        &orchard_root,
        &second_ironwood_root,
    )?;
    sequential.push(
        third_block.clone(),
        &sapling_root,
        &orchard_root,
        &third_ironwood_root,
    )?;

    let mut extended = NonEmptyHistoryTree::from_block(
        &network,
        first_block.clone(),
        &sapling_root,
        &orchard_root,
        &first_ironwood_root,
    )?;
    extended.try_extend([
        (
            second_block.clone(),
            &sapling_root,
            &orchard_root,
            &second_ironwood_root,
        ),
        (
            third_block.clone(),
            &sapling_root,
            &orchard_root,
            &third_ironwood_root,
        ),
    ])?;

    let mut wrong_last_root = NonEmptyHistoryTree::from_block(
        &network,
        first_block,
        &sapling_root,
        &orchard_root,
        &first_ironwood_root,
    )?;
    wrong_last_root.try_extend([
        (
            second_block,
            &sapling_root,
            &orchard_root,
            &second_ironwood_root,
        ),
        (
            third_block,
            &sapling_root,
            &orchard_root,
            &Default::default(),
        ),
    ])?;

    assert_eq!(extended.hash(), sequential.hash());
    assert_ne!(extended.hash(), wrong_last_root.hash());

    Ok(())
}

/// Pruning a V3 tree to its peaks and rebuilding it from the cache must never
/// change the root hash, which commits to the Ironwood suffix that a V2 decode
/// of the same peaks would silently drop.
#[test]
fn v3_pruning_and_hash_dispatch_match_unpruned_tree() -> Result<()> {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let block = blocks
        .get(&1)
        .expect("test vector exists")
        .zcash_deserialize_into::<Block>()
        .expect("block is structurally valid");
    let sapling_root = sapling::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;

    fn ironwood_root(value: u32) -> Result<ironwood::tree::Root> {
        let mut bytes = [0; 32];
        bytes[0] = u8::try_from(value).expect("test values fit in a byte");
        Ok(ironwood::tree::Root::try_from(bytes)?)
    }

    fn parts<'a>(
        block: &'a Block,
        sapling_root: &'a sapling::tree::Root,
        orchard_root: &'a orchard::tree::Root,
        ironwood_root: &'a ironwood::tree::Root,
        height: u32,
    ) -> HistoryTreeBlockParts<'a> {
        HistoryTreeBlockParts {
            header: &block.header,
            height: crate::block::Height(height),
            sapling_root,
            orchard_root,
            ironwood_root,
            sapling_tx: u64::from(height),
            orchard_tx: u64::from(height + 1),
            ironwood_tx: u64::from(height + 2),
        }
    }

    let first_ironwood_root = ironwood_root(1)?;
    let first_parts = parts(
        &block,
        &sapling_root,
        &orchard_root,
        &first_ironwood_root,
        1,
    );
    let mut pruned = NonEmptyHistoryTree::from_parts(&network, first_parts)?;
    let (mut unpruned, _) = PrimitiveHistoryTree::<V3>::new_from_parts(&network, first_parts)?;

    for height in 2u32..=16 {
        let next_ironwood_root = ironwood_root(height)?;
        let next_parts = parts(
            &block,
            &sapling_root,
            &orchard_root,
            &next_ironwood_root,
            height,
        );

        unpruned
            .append_leaf_parts(next_parts)
            .expect("valid sequential V3 leaves append to the unpruned reference tree");
        pruned.push_from_parts(next_parts)?;

        assert_eq!(
            pruned.hash(),
            unpruned.hash(),
            "V3 prune/rebuild changed the root after leaf {height}"
        );
        assert_eq!(
            pruned.peaks().len(),
            usize::try_from(height.count_ones())
                .expect("u32 peak counts fit in usize on supported platforms"),
            "the retained peaks must match the MMR leaf decomposition"
        );

        let rebuilt = PrimitiveHistoryTree::<V3>::new_from_cache(
            &network,
            NetworkUpgrade::Nu6_3,
            pruned.size(),
            pruned.peaks(),
            &Default::default(),
        )?;
        assert_eq!(pruned.hash(), rebuilt.hash());
    }

    let v2_decode = PrimitiveHistoryTree::<V2>::new_from_cache(
        &network,
        NetworkUpgrade::Nu6_3,
        pruned.size(),
        pruned.peaks(),
        &Default::default(),
    )?;
    assert_ne!(
        pruned.hash(),
        v2_decode.hash(),
        "the public hash must include the V3 Ironwood suffix"
    );

    Ok(())
}

/// Cloning a V3 tree (which rebuilds the inner tree from its peaks) must
/// preserve the root, size, and height, and the clone must stay in sync with
/// the original when both push the same next block.
#[test]
fn v3_clone_preserves_root_and_future_append() -> Result<()> {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let block = |height| {
        Arc::new(
            blocks
                .get(&height)
                .expect("test vector exists")
                .zcash_deserialize_into::<Block>()
                .expect("block is structurally valid"),
        )
    };
    let ironwood_root = |value| -> Result<ironwood::tree::Root> {
        let mut bytes = [0; 32];
        bytes[0] = value;
        Ok(ironwood::tree::Root::try_from(bytes)?)
    };
    let sapling_root = sapling::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;
    let first_ironwood_root = ironwood_root(1)?;
    let second_ironwood_root = ironwood_root(2)?;
    let third_ironwood_root = ironwood_root(3)?;
    let fourth_ironwood_root = ironwood_root(4)?;

    let mut original = NonEmptyHistoryTree::from_block(
        &network,
        block(1),
        &sapling_root,
        &orchard_root,
        &first_ironwood_root,
    )?;
    original.push(
        block(2),
        &sapling_root,
        &orchard_root,
        &second_ironwood_root,
    )?;
    original.push(block(3), &sapling_root, &orchard_root, &third_ironwood_root)?;
    assert_eq!(original.peaks().len(), 2, "three leaves have two MMR peaks");

    let mut cloned = original.clone();
    assert_eq!(cloned.hash(), original.hash());
    assert_eq!(cloned.size(), original.size());
    assert_eq!(
        cloned.peaks().keys().collect::<Vec<_>>(),
        original.peaks().keys().collect::<Vec<_>>()
    );
    assert_eq!(cloned.current_height(), original.current_height());
    assert_eq!(cloned.network(), original.network());

    let fourth_block = block(4);
    original.push(
        fourth_block.clone(),
        &sapling_root,
        &orchard_root,
        &fourth_ironwood_root,
    )?;
    cloned.push(
        fourth_block,
        &sapling_root,
        &orchard_root,
        &fourth_ironwood_root,
    )?;

    assert_eq!(cloned.hash(), original.hash());
    assert_eq!(cloned.size(), original.size());
    assert_eq!(cloned.current_height(), original.current_height());

    Ok(())
}

/// The `HistoryTree` wrapper must return an empty tree for pre-Heartwood
/// blocks and matching block/parts trees for the V1, V2, and V3 history
/// versions.
#[test]
fn history_tree_from_block_matches_parts_across_history_versions() -> Result<()> {
    let pre_heartwood = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            heartwood: Some(2),
            canopy: Some(2),
            ..Default::default()
        },
        ..Default::default()
    });
    let v1 = Network::new_regtest(RegtestParameters::default());
    let v2 = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu5: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let v3 = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let block = Arc::new(
        blocks
            .get(&1)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );
    let sapling_root = sapling::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;
    let mut ironwood_root_bytes = [0; 32];
    ironwood_root_bytes[0] = 1;
    let ironwood_root = ironwood::tree::Root::try_from(ironwood_root_bytes)?;

    for (network, should_exist) in [(pre_heartwood, false), (v1, true), (v2, true), (v3, true)] {
        let block_tree = HistoryTree::from_block(
            &network,
            block.clone(),
            &sapling_root,
            &orchard_root,
            &ironwood_root,
        )?;
        let parts_tree = HistoryTree::from_parts(
            &network,
            HistoryTreeBlockParts::from_block(&block, &sapling_root, &orchard_root, &ironwood_root),
        )?;

        assert_eq!(block_tree.is_some(), should_exist);
        assert_eq!(parts_tree.is_some(), should_exist);
        assert_eq!(block_tree.hash(), parts_tree.hash());
    }

    Ok(())
}

/// The `HistoryTree` push wrappers must recreate the tree as a single V3 leaf
/// at NU6.3 activation, committing to the Ironwood root, via both the block
/// and parts APIs.
#[test]
fn history_tree_push_wrapper_matches_parts_across_nu6_3_activation() -> Result<()> {
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_2: Some(1),
            nu6_3: Some(2),
            ..Default::default()
        },
        ..Default::default()
    });
    let (blocks, _) = Network::Mainnet.block_sapling_roots_map();
    let block = |height| {
        Arc::new(
            blocks
                .get(&height)
                .expect("test vector exists")
                .zcash_deserialize_into::<Block>()
                .expect("block is structurally valid"),
        )
    };
    let first_block = block(1);
    let activation_block = block(2);
    let sapling_root = sapling::tree::Root::default();
    // A nonzero root: `orchard::tree::Root::default()` is the all-zero root, so
    // an all-zero value would make "commits to the Orchard root" checks vacuous.
    let mut orchard_root_bytes = [0; 32];
    orchard_root_bytes[0] = 9;
    let orchard_root = orchard::tree::Root::try_from(orchard_root_bytes)?;
    let mut ironwood_root_bytes = [0; 32];
    ironwood_root_bytes[0] = 1;
    let ironwood_root = ironwood::tree::Root::try_from(ironwood_root_bytes)?;

    let mut block_tree = HistoryTree::default();
    let mut parts_tree = HistoryTree::default();
    block_tree.push(
        &network,
        first_block.clone(),
        &sapling_root,
        &orchard_root,
        &ironwood_root,
    )?;
    parts_tree.push_from_parts(
        &network,
        HistoryTreeBlockParts::from_block(
            &first_block,
            &sapling_root,
            &orchard_root,
            &ironwood_root,
        ),
    )?;
    assert_eq!(block_tree.hash(), parts_tree.hash());

    block_tree.push(
        &network,
        activation_block.clone(),
        &sapling_root,
        &orchard_root,
        &ironwood_root,
    )?;
    parts_tree.push_from_parts(
        &network,
        HistoryTreeBlockParts::from_block(
            &activation_block,
            &sapling_root,
            &orchard_root,
            &ironwood_root,
        ),
    )?;
    let activation_tree = HistoryTree::from_block(
        &network,
        activation_block.clone(),
        &sapling_root,
        &orchard_root,
        &ironwood_root,
    )?;
    let activation_without_ironwood = HistoryTree::from_block(
        &network,
        activation_block,
        &sapling_root,
        &orchard_root,
        &Default::default(),
    )?;

    assert_eq!(block_tree.hash(), parts_tree.hash());
    assert_eq!(block_tree.hash(), activation_tree.hash());
    assert_ne!(block_tree.hash(), activation_without_ironwood.hash());
    assert_eq!(
        block_tree
            .as_ref()
            .expect("history tree exists at NU6.3")
            .size(),
        1,
        "a new consensus branch resets the MMR"
    );

    Ok(())
}

/// Test the history tree works during a network upgrade using the block
/// of a network upgrade and the previous block from the previous upgrade.
#[test]
fn upgrade() -> Result<()> {
    // The history tree only exists Hearwood-onward, and the only upgrade for which
    // we have vectors since then is Canopy. Therefore, only test the Heartwood->Canopy upgrade.
    for network in Network::iter() {
        upgrade_for_network_upgrade(network, NetworkUpgrade::Canopy)?;
    }
    Ok(())
}

fn upgrade_for_network_upgrade(network: Network, network_upgrade: NetworkUpgrade) -> Result<()> {
    let (blocks, sapling_roots) = network.block_sapling_roots_map();

    let height = network_upgrade.activation_height(&network).unwrap().0;

    // Load previous block (the block before the activation block of the given network upgrade)
    let block_prev = Arc::new(
        blocks
            .get(&(height - 1))
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );

    // Build a history tree with only the previous block (activation height - 1)
    // This tree will not match the actual tree (which has all the blocks since the previous
    // network upgrade), so we won't be able to check if its root is correct.
    let sapling_root_prev =
        sapling::tree::Root::try_from(**sapling_roots.get(&height).expect("test vector exists"))?;
    let mut tree = NonEmptyHistoryTree::from_block(
        &network,
        block_prev,
        &sapling_root_prev,
        &Default::default(),
        &Default::default(),
    )?;

    assert_eq!(tree.size(), 1);
    assert_eq!(tree.peaks().len(), 1);
    assert_eq!(tree.current_height().0, height - 1);

    // Load block of the activation height
    let activation_block = Arc::new(
        blocks
            .get(&height)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );

    // Append block to history tree. This must trigger a upgrade of the tree,
    // which should be recreated.
    let activation_sapling_root = sapling::tree::Root::try_from(
        **sapling_roots
            .get(&(height + 1))
            .expect("test vector exists"),
    )?;
    tree.push(
        activation_block,
        &activation_sapling_root,
        &Default::default(),
        &Default::default(),
    )
    .unwrap();

    // Check if the tree has a single node, i.e. it has been recreated.
    assert_eq!(tree.size(), 1);
    assert_eq!(tree.peaks().len(), 1);
    assert_eq!(tree.current_height().0, height);

    Ok(())
}
