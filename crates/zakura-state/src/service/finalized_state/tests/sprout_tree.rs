//! Builds without the `sprout` feature leave the Sprout note commitment tree unchanged and mark
//! the database. Builds with it refuse a marked database while the tree is still needed.

use std::sync::Arc;

use tempfile::TempDir;

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    parameters::{
        testnet::{ConfiguredActivationHeights, Parameters as TestnetParameters},
        Network, NetworkKind,
    },
    serialization::ZcashDeserializeInto,
    transparent::Address,
};

use crate::{DiskWriteBatch, SemanticallyVerifiedBlock};

use super::{
    super::commitment_aux::{self, FinalFrontiersGenerationError},
    rollback::{
        child_block, coinbase_tx, config_at, open_unchecked_db, sprout_joinsplit_tx, sync_to,
    },
};

/// Mainnet genesis followed by `heights` generated blocks. The blocks at `joinsplit_heights`
/// also carry a Sprout JoinSplit with two note commitments.
fn chain(heights: u32, joinsplit_heights: &[u32]) -> Vec<SemanticallyVerifiedBlock> {
    let address = Address::from_script_hash(NetworkKind::Mainnet, [0x42; 20]);
    let dust = Amount::<NonNegative>::try_from(1).expect("1 fits in Amount<NonNegative>");

    let genesis: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("mainnet genesis test vector deserializes");
    let mut blocks = vec![genesis];
    for height in 1..=heights {
        let mut transactions = vec![coinbase_tx(Height(height), dust, &address)];
        if joinsplit_heights.contains(&height) {
            transactions.push(sprout_joinsplit_tx());
        }
        let parent = blocks.last().expect("the chain starts at genesis").clone();
        blocks.push(child_block(&parent, transactions));
    }
    blocks
        .into_iter()
        .map(SemanticallyVerifiedBlock::from)
        .collect()
}

/// A network with NU7 at height 12 whose only checkpoint is genesis.
///
/// The checkpoints must cover the block before Canopy, so Canopy and every earlier upgrade
/// activate at height 1.
fn early_nu7_network() -> Network {
    TestnetParameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(1),
            sapling: Some(1),
            blossom: Some(1),
            heartwood: Some(1),
            canopy: Some(1),
            nu5: Some(7),
            nu6: Some(8),
            nu6_1: Some(9),
            nu6_2: Some(10),
            nu6_3: Some(11),
            nu7: Some(12),
        })
        .expect("the activation heights are in order")
        .extend_funding_streams()
        .clear_checkpoints()
        .expect("checkpoints can be cleared")
        .to_network()
        .expect("the network parameters are valid")
}

#[cfg(not(feature = "sprout"))]
#[test]
fn without_sprout_the_tree_is_unchanged_and_the_first_skip_is_marked() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;

    // Before any JoinSplit, nothing is skipped.
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    sync_to(&config, &network, &chain(1, &[]));
    assert_eq!(
        open_unchecked_db(&config, &network).sprout_tree_skipped_from(),
        None
    );

    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    sync_to(&config, &network, &chain(3, &[2, 3]));
    let db = open_unchecked_db(&config, &network);
    assert_eq!(
        db.sprout_tree_for_tip()
            .expect("the tip tree is stored")
            .root(),
        zakura_chain::sprout::tree::NoteCommitmentTree::default().root(),
        "the JoinSplit commitments are not added to the Sprout tree"
    );
    assert_eq!(
        db.sprout_tree_skipped_from(),
        Some(Height(2)),
        "the marker records the first skipped height and later blocks keep it"
    );
}

#[cfg(feature = "sprout")]
#[test]
fn with_sprout_the_tree_grows_and_nothing_is_marked() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;

    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    sync_to(&config, &network, &chain(3, &[2, 3]));
    let db = open_unchecked_db(&config, &network);
    assert_eq!(
        db.sprout_tree_for_tip()
            .expect("the tip tree is stored")
            .count(),
        4,
        "each JoinSplit adds two commitments"
    );
    assert_eq!(db.sprout_tree_skipped_from(), None);
}

/// Writes the Sprout tree marker into the database at `config`, as a build without the `sprout`
/// feature would, and returns the open database.
fn mark_sprout_tree_skipped(
    config: &crate::config::Config,
    network: &Network,
    height: Height,
) -> crate::ZakuraDb {
    let db = open_unchecked_db(config, network);
    let mut batch = DiskWriteBatch::new();
    batch.prepare_sprout_tree_skipped_marker_batch(&db, height);
    db.write_batch(batch).expect("the marker is written");
    db
}

#[cfg(feature = "sprout")]
#[test]
fn sprout_build_refuses_a_marked_database_while_the_tree_is_still_read() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;

    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    sync_to(&config, &network, &chain(1, &[]));
    drop(mark_sprout_tree_skipped(&config, &network, Height(1)));

    match crate::service::finalized_state::FinalizedState::new(&config, &network) {
        Err(crate::StateInitError::SproutTreeNotMaintained { skipped_from, .. }) => {
            assert_eq!(skipped_from, Height(1));
        }
        Err(error) => panic!("expected SproutTreeNotMaintained, got {error}"),
        Ok(_) => {
            panic!("a Mainnet database below NU7 with an incomplete Sprout tree must not open")
        }
    }
}

#[test]
fn sprout_tree_is_no_longer_read_only_past_nu7_and_the_last_checkpoint() {
    let _init_guard = zakura_test::init();
    let blocks_network = Network::Mainnet;
    let early_nu7 = early_nu7_network();

    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    sync_to(&config, &blocks_network, &chain(11, &[]));
    let db = open_unchecked_db(&config, &blocks_network);
    assert!(
        !db.sprout_tree_is_no_longer_read(&early_nu7),
        "the tip is below NU7"
    );
    drop(db);

    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    sync_to(&config, &blocks_network, &chain(12, &[]));
    let db = open_unchecked_db(&config, &blocks_network);
    assert!(
        db.sprout_tree_is_no_longer_read(&early_nu7),
        "the tip is at NU7 and past the checkpoints"
    );
    assert!(
        !db.sprout_tree_is_no_longer_read(&Network::Mainnet),
        "the same tip is far below Mainnet NU7 and its last checkpoint"
    );
}

#[test]
fn final_frontiers_refuse_a_marked_database() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;

    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    sync_to(&config, &network, &chain(1, &[]));
    // A build with the `sprout` feature refuses to reopen a marked database, so this uses the
    // handle that wrote the marker.
    let db = mark_sprout_tree_skipped(&config, &network, Height(1));
    assert!(matches!(
        commitment_aux::produce_final_frontiers(&db, Height(1)),
        Err(FinalFrontiersGenerationError::SproutTreeNotMaintained { skipped_from })
            if skipped_from == Height(1)
    ));
}
