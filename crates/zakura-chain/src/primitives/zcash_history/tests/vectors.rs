use crate::{
    block::Commitment::{self, ChainHistoryActivationReserved},
    serialization::ZcashDeserializeInto,
};

use crate::primitives::zcash_history::Version as ZebraHistoryVersion;
use crate::primitives::zcash_history::*;
use color_eyre::eyre;
use eyre::Result;
use zcash_history::Version as ZcashHistoryVersion;

const HISTORY_HASH_SIZE: usize = 32;
const U32_SIZE: usize = 4;
const V1_FIXED_NODE_DATA_SIZE: usize = HISTORY_HASH_SIZE + U32_SIZE * 4 + HISTORY_HASH_SIZE * 3;
const V2_EXTRA_FIXED_NODE_DATA_SIZE: usize = HISTORY_HASH_SIZE * 2;
const V3_EXTRA_FIXED_NODE_DATA_SIZE: usize = HISTORY_HASH_SIZE * 2;

/// Test the MMR tree using the activation block of a network upgrade
/// and its next block.
#[test]
fn tree() -> Result<()> {
    for network in Network::iter() {
        tree_for_network_upgrade(&network, NetworkUpgrade::Heartwood)?;
        tree_for_network_upgrade(&network, NetworkUpgrade::Canopy)?;
    }
    Ok(())
}

fn tree_for_network_upgrade(network: &Network, network_upgrade: NetworkUpgrade) -> Result<()> {
    let (blocks, sapling_roots) = network.block_sapling_roots_map();

    let height = network_upgrade.activation_height(network).unwrap().0;

    // Load Block 0 (activation block of the given network upgrade)
    let block0 = Arc::new(
        blocks
            .get(&height)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );

    // Check its commitment
    let commitment0 = block0.commitment(network)?;
    if network_upgrade == NetworkUpgrade::Heartwood {
        // Heartwood is the only upgrade that has a reserved value.
        // (For other upgrades we could compare with the expected commitment,
        // but we haven't calculated them.)
        assert_eq!(commitment0, ChainHistoryActivationReserved);
    }

    // Build initial MMR tree with only Block 0
    let sapling_root0 =
        sapling::tree::Root::try_from(**sapling_roots.get(&height).expect("test vector exists"))?;
    let (mut tree, _) = Tree::<V1>::new_from_block(
        network,
        block0,
        &sapling_root0,
        &Default::default(),
        &Default::default(),
    )?;

    // Compute root hash of the MMR tree, which will be included in the next block
    let hash0 = tree.hash();

    // Load Block 1 (activation + 1)
    let block1 = Arc::new(
        blocks
            .get(&(height + 1))
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );

    // Check its commitment
    let commitment1 = block1.commitment(network)?;
    assert_eq!(commitment1, Commitment::ChainHistoryRoot(hash0));

    // Append Block to MMR tree
    let sapling_root1 = sapling::tree::Root::try_from(
        **sapling_roots
            .get(&(height + 1))
            .expect("test vector exists"),
    )?;
    let append = tree
        .append_leaf(
            block1,
            &sapling_root1,
            &Default::default(),
            &Default::default(),
        )
        .unwrap();

    // Tree how has 3 nodes: two leaves for each block, and one parent node
    // which is the new root
    assert_eq!(tree.inner.len(), 3);
    // Two nodes were appended: the new leaf and the parent node
    assert_eq!(append.len(), 2);

    Ok(())
}

#[test]
fn old_history_versions_ignore_ironwood_root() -> Result<()> {
    let network = Network::Mainnet;
    let (block, sapling_root) = block_and_sapling_root(&network, NetworkUpgrade::Canopy)?;
    let orchard_root = orchard::tree::Root::default();
    let default_ironwood_root = ironwood::tree::Root::default();
    let non_default_ironwood_root = non_default_ironwood_root();

    let v1_default = <V1 as ZebraHistoryVersion>::block_to_history_node(
        block.clone(),
        &network,
        &sapling_root,
        &orchard_root,
        &default_ironwood_root,
    );
    let v1_non_default = <V1 as ZebraHistoryVersion>::block_to_history_node(
        block.clone(),
        &network,
        &sapling_root,
        &orchard_root,
        &non_default_ironwood_root,
    );

    assert_eq!(
        <V1 as ZcashHistoryVersion>::to_bytes(&v1_default),
        <V1 as ZcashHistoryVersion>::to_bytes(&v1_non_default)
    );
    assert_eq!(
        <V1 as ZcashHistoryVersion>::hash(&v1_default),
        <V1 as ZcashHistoryVersion>::hash(&v1_non_default)
    );

    let v2_default = <V2 as ZebraHistoryVersion>::block_to_history_node(
        block.clone(),
        &network,
        &sapling_root,
        &orchard_root,
        &default_ironwood_root,
    );
    let v2_non_default = <V2 as ZebraHistoryVersion>::block_to_history_node(
        block,
        &network,
        &sapling_root,
        &orchard_root,
        &non_default_ironwood_root,
    );

    assert_eq!(
        <V2 as ZcashHistoryVersion>::to_bytes(&v2_default),
        <V2 as ZcashHistoryVersion>::to_bytes(&v2_non_default)
    );
    assert_eq!(
        <V2 as ZcashHistoryVersion>::hash(&v2_default),
        <V2 as ZcashHistoryVersion>::hash(&v2_non_default)
    );

    Ok(())
}

#[test]
fn v3_history_node_hash_input_has_exact_serialized_size() -> Result<()> {
    let network = Network::Mainnet;
    let (block, sapling_root) = block_and_sapling_root(&network, NetworkUpgrade::Canopy)?;
    let orchard_root = orchard::tree::Root::default();
    let ironwood_root = non_default_ironwood_root();
    let ironwood_root_bytes: [u8; 32] = (&ironwood_root).into();

    let mut node_data = <V3 as ZebraHistoryVersion>::block_to_history_node(
        block,
        &network,
        &sapling_root,
        &orchard_root,
        &ironwood_root,
    );

    assert_eq!(node_data.start_ironwood_root, ironwood_root_bytes);
    assert_eq!(node_data.end_ironwood_root, ironwood_root_bytes);

    node_data.ironwood_tx = 1;
    let encoded = <V3 as ZcashHistoryVersion>::to_bytes(&node_data);

    // `zcash_history::Version::hash()` hashes this exact serialized byte
    // string. ZIP-229 field 17 (`nIronwoodTxCount`) is the final CompactSize
    // value in a V3 node.
    assert_eq!(encoded.len(), serialized_v3_node_data_size(&node_data));
    assert_eq!(encoded.last(), Some(&1));
    assert_eq!(::zcash_history::MAX_NODE_DATA_SIZE, 317);

    Ok(())
}

fn block_and_sapling_root(
    network: &Network,
    network_upgrade: NetworkUpgrade,
) -> Result<(Arc<Block>, sapling::tree::Root)> {
    let (blocks, sapling_roots) = network.block_sapling_roots_map();
    let height = network_upgrade.activation_height(network).unwrap().0;
    let block = Arc::new(
        blocks
            .get(&height)
            .expect("test vector exists")
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid"),
    );
    let sapling_root =
        sapling::tree::Root::try_from(**sapling_roots.get(&height).expect("test vector exists"))?;

    Ok((block, sapling_root))
}

fn non_default_ironwood_root() -> ironwood::tree::Root {
    for byte in 1..=u8::MAX {
        let mut bytes = [0; 32];
        bytes[0] = byte;

        if let Ok(root) = ironwood::tree::Root::try_from(bytes) {
            if root != Default::default() {
                return root;
            }
        }
    }

    unreachable!("at least one one-byte pallas::Base encoding must be valid")
}

fn serialized_v3_node_data_size(data: &::zcash_history::NodeDataV3) -> usize {
    serialized_v2_node_data_size(&data.v2)
        + V3_EXTRA_FIXED_NODE_DATA_SIZE
        + compact_size(data.ironwood_tx)
}

fn serialized_v2_node_data_size(data: &::zcash_history::NodeDataV2) -> usize {
    serialized_v1_node_data_size(&data.v1)
        + V2_EXTRA_FIXED_NODE_DATA_SIZE
        + compact_size(data.orchard_tx)
}

fn serialized_v1_node_data_size(data: &::zcash_history::NodeData) -> usize {
    V1_FIXED_NODE_DATA_SIZE
        + compact_size(data.start_height)
        + compact_size(data.end_height)
        + compact_size(data.sapling_tx)
}

fn compact_size(value: u64) -> usize {
    match value {
        0..=0xfc => 1,
        0xfd..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}
