//! Body commitment evidence used before permanently rejecting a header.

use std::{collections::HashSet, sync::Arc, time::Duration};

use tower::{Service, ServiceExt};
use zakura_chain::{
    block::{Block, CommitmentError},
    parameters::{Network, NetworkUpgrade},
    transaction,
};
use zakura_state as zs;

use super::{check, VerifyBlockError};
use crate::{BlockError, BoxError};

/// Bound the parent-history read needed to attribute a failed delivery.
const COMMITMENT_CHECK_TIMEOUT: Duration = Duration::from_secs(20);

/// A transaction list whose IDs match the header. Padding is removed only for
/// validation. A reconstructed body must never be committed or cached.
pub(super) struct TransactionList {
    pub block: Arc<Block>,
    pub hashes: Arc<[transaction::Hash]>,
    pub padded: bool,
}

impl TransactionList {
    pub fn check(network: &Network, block: Arc<Block>) -> Result<Self, VerifyBlockError> {
        let hashes: Arc<[_]> = block.transactions.iter().map(|tx| tx.hash()).collect();
        let padded = match check::merkle_root_validity_with_attribution(network, &block, &hashes) {
            Ok(()) => false,
            Err(error) if is_padding_error(&error) => true,
            Err(error) => return Err(error),
        };
        if !padded {
            return Ok(Self {
                block,
                hashes,
                padded,
            });
        }

        let mut seen = HashSet::with_capacity(hashes.len());
        let block = Arc::new(Block {
            header: block.header.clone(),
            transactions: block
                .transactions
                .iter()
                .zip(hashes.iter())
                .filter(|(_, hash)| seen.insert(**hash))
                .map(|(tx, _)| tx.clone())
                .collect(),
        });
        let hashes = block.transactions.iter().map(|tx| tx.hash()).collect();
        Ok(Self {
            block,
            hashes,
            padded,
        })
    }
}

pub(crate) fn is_padding_error(error: &VerifyBlockError) -> bool {
    matches!(
        error,
        VerifyBlockError::Block {
            source: BlockError::DuplicateTransaction
        }
    )
}

/// Reject duplicate counts fixed by NU5's zero-padded authorization tree.
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
/// so those blocks return false and callers do not require a separate check.
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

/// A semantic error may condemn NU5 authorization bytes only after they match
/// the header. Successful commits already check this in contextual validation.
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
    if !matches!(
        error.body_verification_class(),
        BodyVerificationClass::ConsensusInvalid(_)
    ) || block
        .coinbase_height()
        .is_none_or(|height| NetworkUpgrade::current(network, height) < NetworkUpgrade::Nu5)
    {
        return error;
    }
    // Recover any committed-field failure even if another transaction's
    // authorization check finished first. Successful bodies pay no extra pass.
    let height = block
        .coinbase_height()
        .expect("verified block has a height");
    for tx in &block.transactions {
        if let Err(error) =
            crate::transaction::check::txid_rules(tx, height, network).and_then(|()| {
                crate::transaction::check::lock_time_has_passed(tx, height, block.header.time)
            })
        {
            return error.into();
        }
    }
    match auth_commitment_matches(state, network, block).await {
        Ok(true) => error,
        Ok(false) => VerifyBlockError::AuthDataMismatch,
        Err(error) => error,
    }
}
