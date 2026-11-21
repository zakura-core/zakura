//! The state reads the tracker makes, each with a deadline.

use std::time::Duration;

use tower::{Service, ServiceExt};
use zakura_chain::{
    block::{self, Height},
    miner_mesh::MinerMeshCommitment,
};
use zakura_state::{ReadRequest, ReadResponse};

use crate::BoxError;

/// How long one state read may take.
const STATE_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// A best-chain block's hash, and the commitment its coinbase carries if it
/// has exactly one.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct ReadBlock {
    pub hash: block::Hash,
    pub commitment: Option<MinerMeshCommitment>,
}

/// Returns the hash of the best-chain block at `height`, or `None` when the
/// best chain has no block there.
pub(super) async fn best_chain_hash<S>(
    state: &S,
    height: Height,
) -> Result<Option<block::Hash>, BoxError>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
{
    match read(state, ReadRequest::BestChainBlockHash(height)).await? {
        ReadResponse::BlockHash(hash) => Ok(hash),
        response => Err(unexpected(response)),
    }
}

/// Returns the best-chain block at `height` with its coinbase commitment, or
/// `None` when the block or its coinbase cannot be read, for example because
/// it was pruned or has just left the best chain.
///
/// Only the coinbase is read, not the whole block. The reads after the first
/// name the block by its hash and its coinbase by its ID, so the result
/// describes one block even if the best chain changes in between.
pub(super) async fn read_block<S>(state: &S, height: Height) -> Result<Option<ReadBlock>, BoxError>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
{
    let Some(hash) = best_chain_hash(state, height).await? else {
        return Ok(None);
    };

    let transaction_ids =
        match read(state, ReadRequest::TransactionIdsForBlock(hash.into())).await? {
            ReadResponse::TransactionIdsForBlock(Some(ids)) => ids,
            ReadResponse::TransactionIdsForBlock(None) => return Ok(None),
            response => return Err(unexpected(response)),
        };
    let coinbase_id = *transaction_ids
        .first()
        .ok_or("the state returned a block with no transactions")?;

    let coinbase = match read(state, ReadRequest::Transaction(coinbase_id)).await? {
        ReadResponse::Transaction(Some(mined)) => mined.tx,
        ReadResponse::Transaction(None) => return Ok(None),
        response => return Err(unexpected(response)),
    };

    Ok(Some(ReadBlock {
        hash,
        commitment: MinerMeshCommitment::from_coinbase_outputs(coinbase.outputs()),
    }))
}

/// Sends `request` to `state`, failing after [`STATE_READ_TIMEOUT`].
async fn read<S>(state: &S, request: ReadRequest) -> Result<ReadResponse, BoxError>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
{
    let name = request.variant_name();
    tokio::time::timeout(STATE_READ_TIMEOUT, state.clone().oneshot(request))
        .await
        .map_err(|_| format!("the miner mesh state read {name} timed out"))?
}

/// The error for a response that does not answer the request.
fn unexpected(response: ReadResponse) -> BoxError {
    format!("unexpected state response to a miner mesh read: {response:?}").into()
}
