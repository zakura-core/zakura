//! Trusted submission: relay a pending body, then verify it through normal consensus.

use super::*;
use crate::server::trusted_block::TrustedSubmissionError;

use super::submission_lifecycle::{RelayPolicy, Submission, VerificationOutcome};

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
        let parent_known = self.parent_is_known(block.header.previous_block_hash).await;
        if !parent_known {
            metrics::counter!("rpc.trusted_submission.early_relay_skipped", "reason" => "unknown_parent")
                .increment(1);
            tracing::info!(
                ?hash,
                "parent is unknown: validating the trusted submission without early relay"
            );
        }
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
        let submission = Submission {
            block,
            hash,
            height,
            size: bytes.len(),
            submitted_at,
            method: MinedBlockSubmission::TrustedBlock,
            reservation: submission,
        };
        let lifecycle = submission.spawn(
            self.gbt.block_verifier_router(),
            self.gbt.mined_block_sender(),
            RelayPolicy::TrustedSubmission {
                registration,
                parent_known,
            },
            move |outcome| {
                metrics::histogram!("rpc.trusted_submission.validation.duration_seconds")
                    .record(submitted_at.elapsed().as_secs_f64());
                let response = match outcome.verification {
                    VerificationOutcome::Completed(result) => Self::mined_block_response(
                        result,
                        hash,
                        height,
                        MinedBlockSubmission::TrustedBlock,
                    )
                    .unwrap_or_else(|_| SubmitBlockErrorResponse::Inconclusive.into()),
                    VerificationOutcome::TimedOut => {
                        metrics::counter!("rpc.trusted_submission.timeouts").increment(1);
                        SubmitBlockErrorResponse::Inconclusive.into()
                    }
                };
                if outcome.relay.requested
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
                    if outcome.relay.was_dispatched() {
                        metrics::counter!("rpc.trusted_submission.rejected_after_dispatch")
                            .increment(1);
                    }
                }
                metrics::counter!("rpc.trusted_submission.outcomes", "outcome" => crate::server::trusted_block::outcome(&response)).increment(1);
                response
            },
        );
        Ok(lifecycle
            .await
            .unwrap_or_else(|_| SubmitBlockErrorResponse::Inconclusive.into()))
    }

    /// Returns whether `parent` is committed to any chain. A failed lookup returns `false`, which
    /// only skips early relay: the block is still validated and announced after it commits.
    async fn parent_is_known(&self, parent: block::Hash) -> bool {
        matches!(
            self.read_state
                .clone()
                .oneshot(ReadRequest::BlockInfo(parent.into()))
                .await,
            Ok(ReadResponse::BlockInfo(Some(_)))
        )
    }
}
