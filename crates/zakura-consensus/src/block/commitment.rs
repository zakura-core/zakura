//! Shared parent context and authorization checks for semantic and checkpoint verification.

use std::{sync::Arc, time::Duration};

use tower::{Service, ServiceExt};
use zakura_chain::{
    block::{merkle::AuthDataRoot, Block},
    parameters::{Network, NetworkUpgrade},
};
use zakura_state as zs;

use super::VerifyBlockError;
use crate::{BlockError, BoxError};

/// Whether the transaction list could be a Merkle padding alias.
pub(crate) fn is_padding_error(error: &VerifyBlockError) -> bool {
    matches!(
        error,
        VerifyBlockError::Block {
            source: BlockError::DuplicateTransaction
        }
    )
}

/// Read the exact committed parent and authenticate the body's claimed height.
pub(super) async fn parent_context<S>(
    state: S,
    block: &Block,
) -> Result<zs::BlockParentContext, VerifyBlockError>
where
    S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    S::Future: Send + 'static,
{
    let parent = block.header.previous_block_hash;
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        state.oneshot(zs::Request::BlockParentContext(parent)),
    )
    .await
    .map_err(|_| VerifyBlockError::MissingParentContext(parent))?
    .map_err(|source| {
        tracing::debug!(?source, ?parent, "parent context lookup failed");
        VerifyBlockError::MissingParentContext(parent)
    })?;
    let context = match response {
        zs::Response::BlockParentContext(Some(context)) if context.parent == parent => context,
        zs::Response::BlockParentContext(_) => {
            return Err(VerifyBlockError::MissingParentContext(parent))
        }
        _ => unreachable!("BlockParentContext returns parent context"),
    };
    let expected = (context.height + 1).ok_or(VerifyBlockError::MissingParentContext(parent))?;
    if block.coinbase_height() != Some(expected) {
        return Err(VerifyBlockError::ParentHeightMismatch {
            claimed: block.coinbase_height(),
            expected,
        });
    }
    Ok(context)
}

/// Authenticate authorization bytes before dispatching transaction proofs.
pub(super) async fn check_auth_data(
    network: Network,
    block: Arc<Block>,
    context: zs::BlockParentContext,
) -> Result<AuthDataRoot, VerifyBlockError> {
    tokio::task::spawn_blocking(move || {
        let root = block.auth_data_root();
        zs::check::block_commitment_is_valid_for_chain_history(
            block,
            &network,
            &context.history_tree,
            Some(root),
        )
        .map_err(VerifyBlockError::BodyCommitment)?;
        Ok(root)
    })
    .await
    .expect("commitment calculation must not panic")
}

/// Condemn padding only when the NU5 commitment proves the header binds it.
/// Missing parent context leaves the known-invalid delivery as a supplier fault.
pub(crate) async fn check_auth_bound_duplicates<S>(
    state: S,
    network: &Network,
    block: Arc<Block>,
    context: Option<zs::BlockParentContext>,
) -> Result<(), VerifyBlockError>
where
    S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    S::Future: Send + 'static,
{
    let height = block
        .coinbase_height()
        .ok_or(BlockError::MissingHeight(block.hash()))?;
    if NetworkUpgrade::current(network, height) < NetworkUpgrade::Nu5 {
        return Ok(());
    }
    let context = match context {
        Some(context) => context,
        None => match parent_context(state, &block).await {
            Ok(context) => context,
            Err(VerifyBlockError::MissingParentContext(_)) => return Ok(()),
            Err(error) => return Err(error),
        },
    };
    check_auth_data(network.clone(), block, context).await?;
    Err(VerifyBlockError::NonMalleableDuplicateTransaction)
}
