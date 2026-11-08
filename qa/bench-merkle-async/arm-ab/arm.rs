//! Arms A and B: the production hashing lines of `SemanticBlockVerifier::call` at these
//! commits, which hash inline on the calling async worker.

use std::sync::Arc;

use zakura_chain::{
    block::{merkle::Root, AuthDataDigests},
    parameters::Network,
    transaction,
};
use zakura_node_services::block_memory::BlockHandle;

use crate::{block::check, error::BlockError};

pub fn merkle_root(hashes: &[transaction::Hash]) -> Root {
    hashes.iter().copied().collect()
}

pub async fn phase(
    network: Network,
    block: BlockHandle,
) -> Result<(Arc<[transaction::Hash]>, AuthDataDigests), BlockError> {
    let transaction_hashes = block.view().transaction_hashes();
    check::merkle_root_validity(&network, &block, &transaction_hashes)?;
    let auth_data_digests = block.view().auth_data_digests();
    Ok((transaction_hashes, auth_data_digests))
}
