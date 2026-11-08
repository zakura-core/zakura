//! Arm C: the production hashing lines of `SemanticBlockVerifier::call` at this commit,
//! through the production `run_block_hashing` boundary.

use std::sync::Arc;

use zakura_chain::{
    block::{merkle::Root, AuthDataDigests},
    parameters::Network,
    transaction,
};
use zakura_node_services::block_memory::BlockHandle;

use crate::{
    block::{check, run_block_hashing},
    error::BlockError,
};

pub fn merkle_root(hashes: &[transaction::Hash]) -> Root {
    Root::from_hashes_in_parallel(hashes.iter().copied())
}

pub async fn phase(
    network: Network,
    block: BlockHandle,
) -> Result<(Arc<[transaction::Hash]>, AuthDataDigests), BlockError> {
    let hashing_network = network.clone();
    let transaction_hashes = run_block_hashing(block.clone(), move |block| {
        let hashes = block.view().transaction_hashes();
        check::merkle_root_validity_in_parallel(&hashing_network, &block, &hashes)?;
        Ok::<_, BlockError>(hashes)
    })
    .await?;
    let auth_data_digests =
        run_block_hashing(block.clone(), |block| block.view().auth_data_digests()).await;
    Ok((transaction_hashes, auth_data_digests))
}
