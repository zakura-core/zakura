//! Shared test helpers for finalized-state block and header-store tests.
//!
//! These helpers are used by both the fixed test vectors (`vectors.rs`) and the
//! header-store coherence harness (`header_store_coherence`).

use std::{path::Path, sync::Arc};

use zakura_chain::{
    block::{self, Block, Height},
    orchard,
    parallel::commitment_aux::BlockCommitmentRoots,
    parameters::{testnet, Network, Network::Mainnet},
    sapling,
    serialization::ZcashDeserializeInto,
    work::difficulty::ParameterDifficulty,
};
use zakura_test::vectors::MAINNET_BLOCKS;

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    request::{FinalizedBlock, Treestate},
    service::finalized_state::{disk_db::DiskWriteBatch, ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
    CheckpointVerifiedBlock, Config,
};

/// Returns an ephemeral or configured state database with `genesis` committed as a full block.
pub(super) fn state_with_genesis_config(
    network: &Network,
    genesis: Arc<Block>,
    config: Config,
) -> ZakuraDb {
    let state = ZakuraDb::new(
        &config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    );

    write_full_block_header_and_transactions(&state, genesis.clone());

    state
}

/// Returns a persistent state config rooted at `cache_dir`, for close-and-reopen tests.
pub(super) fn persistent_config(cache_dir: &Path) -> Config {
    Config {
        cache_dir: cache_dir.to_owned(),
        ephemeral: false,
        debug_skip_non_finalized_state_backup_task: true,
        ..Config::default()
    }
}

/// Opens (or reopens) a persistent state database from `config`.
pub(super) fn persistent_state(config: &Config, network: &Network) -> ZakuraDb {
    ZakuraDb::new(
        config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
}

/// Returns a configured testnet with only the implicit genesis checkpoint, so header
/// commits above genesis take the contextual validation path.
pub(super) fn no_extra_checkpoint_test_network(genesis_hash: block::Hash) -> Network {
    testnet::Parameters::build()
        .with_network_name("HeaderReorgTest")
        .expect("test network name is valid")
        .with_genesis_hash(genesis_hash)
        .expect("test genesis hash is valid")
        .with_target_difficulty_limit(Mainnet.target_difficulty_limit())
        .expect("mainnet difficulty limit is valid for test network")
        .with_activation_heights(testnet::ConfiguredActivationHeights {
            canopy: Some(1),
            ..Default::default()
        })
        .expect("test activation heights are valid")
        .clear_funding_streams()
        .clear_checkpoints()
        .expect("genesis-only checkpoints are valid")
        .to_network()
        .expect("test network is valid")
}

/// Deserializes the mainnet test vector block at `height`.
pub(super) fn mainnet_block(height: u32) -> Arc<Block> {
    MAINNET_BLOCKS
        .get(&height)
        .expect("test vector exists")
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("mainnet test block deserializes")
}

/// Fabricates provisional commitment roots for `height`, with the zeroed
/// auth-data root marking them as unverified.
pub(super) fn root_at(height: Height) -> BlockCommitmentRoots {
    BlockCommitmentRoots {
        height,
        sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
        orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx: 0,
        orchard_tx: 0,
        ironwood_tx: 0,
        auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0u8; 32]),
    }
}

/// Commits a header range through the production write path, panicking on rejection.
pub(super) fn commit_header_range(
    state: &ZakuraDb,
    anchor: block::Hash,
    headers: &[Arc<block::Header>],
) -> block::Hash {
    let mut batch = DiskWriteBatch::new();
    let body_sizes = vec![0; headers.len()];
    let committed_hash = batch
        .prepare_header_range_batch(state, anchor, headers, &body_sizes)
        .expect("header range is valid");
    state
        .write_batch(batch)
        .expect("header range batch writes successfully");
    committed_hash
}

/// Commits `block`'s header and transaction data (the body-commit batch shape),
/// without treestates or value pools.
pub(super) fn write_full_block_header_and_transactions(state: &ZakuraDb, block: Arc<Block>) {
    let checkpoint_verified = CheckpointVerifiedBlock::from(block);
    let finalized =
        FinalizedBlock::from_checkpoint_verified(checkpoint_verified, Treestate::default());

    let mut batch = DiskWriteBatch::new();
    batch
        .prepare_block_header_and_transaction_data_batch(state, &finalized, true, None)
        .expect("full block header and transaction batch is valid");
    state.db.write(batch).expect("full block batch writes");
}
