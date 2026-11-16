//! Authenticated submission: relay a pending body, then verify it through normal consensus.

use super::*;
use crate::server::trusted_block::TrustedSubmissionError;

const VERIFY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8 * 60);

impl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>
    RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>
where
    Mempool: MempoolService,
    State: StateService,
    ReadState: ReadStateService,
    Tip: ChainTip + Clone + Send + Sync + 'static,
    AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
    BlockVerifierRouter: BlockVerifierService,
    SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
{
    /// Registers an operator-authorized body for relay without changing its validation rules.
    pub(crate) async fn submit_trusted_block(
        &self,
        bytes: &[u8],
    ) -> std::result::Result<SubmitBlockResponse, TrustedSubmissionError> {
        let submitted_at = std::time::Instant::now();
        let mut remaining = bytes;
        let block = Block::zcash_deserialize_bounded(&mut remaining)
            .map_err(|_| TrustedSubmissionError::Malformed)?;
        if !remaining.is_empty() {
            return Err(TrustedSubmissionError::Malformed);
        }
        let height = block
            .coinbase_height()
            .ok_or(TrustedSubmissionError::Malformed)?;
        let block = Arc::new(block);
        let hash = block.hash();
        let submission = match self.gbt.reserve_mined_submission(hash) {
            Ok(guard) => guard,
            Err(SubmitBlockErrorResponse::Inconclusive) => {
                return Err(TrustedSubmissionError::Capacity)
            }
            Err(response) => return Ok(response.into()),
        };
        let registration = self
            .gbt
            .pending_blocks()
            .insert(block.clone())
            .ok_or(TrustedSubmissionError::Capacity)?;
        let sender = self.gbt.mined_block_sender();
        let mut verifier = self.gbt.block_verifier_router();
        let size = bytes.len();
        let pending = registration.signal();

        // This owner survives an HTTP disconnect; dropping it withdraws the body on every
        // error, including panic or shutdown. It never creates a verified-state request itself.
        let lifecycle = tokio::spawn(async move {
            let _submission = submission;
            let request = zakura_consensus::Request::CommitMined {
                block: block.clone().into(),
                size,
                admission: zakura_state::BlockAdmission::pending(),
            };
            let verification = tokio::time::timeout(VERIFY_TIMEOUT, async move {
                verifier.ready().await?.call(request).await
            });
            tokio::pin!(verification);
            let mut relay_requested = false;
            let result = tokio::select! {
                biased;
                result = &mut verification => result,
                // Poll verification first: an already-known result supersedes early relay.
                _ = std::future::ready(()) => {
                    relay_requested = sender.send(EarlyMinedBlock {
                        relay: crate::EarlyBlockRelay::TrustedSubmission,
                        hash, height, size, submitted_at,
                        submission: MinedBlockSubmission::TrustedBlock,
                        pending: pending.clone(),
                    }).is_ok();
                    verification.await
                }
            };
            registration.resolve(match &result {
                Ok(Ok(_)) => Ok(block),
                _ => Err(()),
            });
            metrics::histogram!("rpc.trusted_submission.validation.duration_seconds")
                .record(submitted_at.elapsed().as_secs_f64());
            let response = match result {
                Ok(result) => Self::mined_block_response(
                    result,
                    hash,
                    height,
                    MinedBlockSubmission::TrustedBlock,
                )
                .unwrap_or_else(|_| SubmitBlockErrorResponse::Inconclusive.into()),
                Err(_) => {
                    metrics::counter!("rpc.trusted_submission.timeouts").increment(1);
                    SubmitBlockErrorResponse::Inconclusive.into()
                }
            };
            if relay_requested
                && matches!(
                    response,
                    SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Rejected)
                )
            {
                metrics::counter!("rpc.trusted_submission.rejected_after_relay_requested")
                    .increment(1);
                tracing::warn!(
                    ?hash,
                    "trusted submission rejected after early relay was requested"
                );
                if pending.was_dispatched() {
                    metrics::counter!("rpc.trusted_submission.rejected_after_dispatch")
                        .increment(1);
                }
            }
            metrics::counter!("rpc.trusted_submission.outcomes", "outcome" => crate::server::trusted_block::outcome(&response)).increment(1);
            response
        });
        Ok(lifecycle
            .await
            .unwrap_or_else(|_| SubmitBlockErrorResponse::Inconclusive.into()))
    }
}
