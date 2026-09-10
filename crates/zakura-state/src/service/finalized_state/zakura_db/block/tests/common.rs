//! Shared test helpers for finalized-state block and header-store tests.
//!
//! These helpers are used by both the fixed test vectors (`vectors.rs`) and the
//! header-store coherence harness (`header_store_coherence`).

use std::sync::Arc;

use zakura_chain::{block::Block, parameters::Network, serialization::ZcashDeserializeInto};
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
    )
    .expect("opening the finalized state database should succeed");

    write_full_block_header_and_transactions(&state, genesis.clone());

    state
}

/// Deserializes the mainnet test vector block at `height`.
pub(super) fn mainnet_block(height: u32) -> Arc<Block> {
    MAINNET_BLOCKS
        .get(&height)
        .expect("test vector exists")
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("mainnet test block deserializes")
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
