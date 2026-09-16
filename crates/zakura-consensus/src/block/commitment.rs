//! Body commitment evidence used before permanently rejecting a header.
//!
//! A deterministic body failure condemns a header only when the header commits
//! to every byte the failed rule read. Pre-NU5 transaction IDs hash the whole
//! transaction, so a matching Merkle root binds the body. NU5 IDs exclude
//! authorization data, which the header binds separately through the ZIP 244
//! authorization-data commitment. A failure on a body that is not fully bound
//! is a payload mismatch: another supplier may still deliver the committed body,
//! and a header whose bodies never bind is handled like an unavailable body.

use std::{sync::Arc, time::Duration};

use tower::{Service, ServiceExt};
use zakura_chain::{
    block::{Block, CommitmentError},
    parameters::{Network, NetworkUpgrade},
};
use zakura_state as zs;

use super::VerifyBlockError;
use crate::{BlockError, BoxError};

/// Bound the parent-history read needed to attribute a failed delivery.
const COMMITMENT_CHECK_TIMEOUT: Duration = Duration::from_secs(20);

/// Whether `error` is a duplicate that Merkle padding could have introduced.
pub(crate) fn is_padding_error(error: &VerifyBlockError) -> bool {
    matches!(
        error,
        VerifyBlockError::Block {
            source: BlockError::DuplicateTransaction
        }
    )
}

/// Reject a padded transaction list when the NU5 authorization commitment
/// proves that the header itself commits to the duplicates.
pub(crate) async fn check_auth_bound_duplicates<S>(
    state: S,
    network: &Network,
    block: Arc<Block>,
) -> Result<(), VerifyBlockError>
where
    S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    S::Future: Send + 'static,
{
    if auth_commitment_matches(state, network, block).await? {
        return Err(VerifyBlockError::NonMalleableDuplicateTransaction);
    }
    Ok(())
}

/// Returns whether NU5 authorization bytes match in the exact parent history.
/// Missing history is retryable. Pre-NU5 IDs already bind authorization bytes,
/// so those blocks return false and callers must not read that as a mismatch.
pub(super) async fn auth_commitment_matches<S>(
    state: S,
    network: &Network,
    block: Arc<Block>,
) -> Result<bool, VerifyBlockError>
where
    S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    S::Future: Send + 'static,
{
    let hash = block.hash();
    let height = block
        .coinbase_height()
        .ok_or(BlockError::MissingHeight(hash))?;
    if NetworkUpgrade::current(network, height) < NetworkUpgrade::Nu5 {
        return Ok(false);
    }

    let context_error = |source| VerifyBlockError::StateService { source, hash };
    let auth_block = block.clone();
    let auth_data_root = tokio::task::spawn_blocking(move || auth_block.auth_data_root())
        .await
        .map_err(|error| context_error(Box::new(error)))?;
    let response = tokio::time::timeout(
        COMMITMENT_CHECK_TIMEOUT,
        state.oneshot(zs::Request::CheckBlockCommitment(zs::BlockCommitmentData {
            block,
            auth_data_root: Some(auth_data_root),
        })),
    )
    .await
    .map_err(|error| context_error(Box::new(error)))?;

    match response {
        Ok(zs::Response::BlockCommitmentValidity(zs::BlockCommitmentValidity::Valid)) => Ok(true),
        Ok(zs::Response::BlockCommitmentValidity(zs::BlockCommitmentValidity::Unavailable)) => {
            Err(VerifyBlockError::Depth {
                source: "body commitment requires unavailable parent history".into(),
                hash,
            })
        }
        Ok(_) => unreachable!("wrong response to the body commitment check"),
        Err(error)
            if matches!(
                error.downcast_ref::<zs::ValidateContextError>(),
                Some(zs::ValidateContextError::InvalidBlockCommitment(
                    CommitmentError::InvalidChainHistoryBlockTxAuthCommitment { .. }
                ))
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(context_error(error)),
    }
}

/// Turn a deterministic failure on an NU5 body into a payload mismatch unless
/// the header's authorization commitment binds the delivered bytes. Rules whose
/// inputs the transaction ID fixes are deliberately not carved out: see the
/// module docs.
pub(super) async fn attribute_failure<S>(
    state: S,
    network: &Network,
    block: Arc<Block>,
    error: VerifyBlockError,
) -> VerifyBlockError
where
    S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
    S::Future: Send + 'static,
{
    use zakura_header_chain::BodyVerificationClass;
    let deterministic = matches!(
        error.body_verification_class(),
        BodyVerificationClass::ConsensusInvalid(_)
    );
    let binds_auth_data_separately = block
        .coinbase_height()
        .is_some_and(|height| NetworkUpgrade::current(network, height) >= NetworkUpgrade::Nu5);
    if !deterministic || !binds_auth_data_separately {
        return error;
    }
    match auth_commitment_matches(state, network, block).await {
        Ok(true) => error,
        Ok(false) => VerifyBlockError::AuthDataMismatch,
        Err(error) => error,
    }
}
