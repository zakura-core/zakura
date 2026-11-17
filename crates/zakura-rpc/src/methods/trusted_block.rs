//! Trusted submission: relay a pending body, then verify it through normal consensus.

use super::*;
use crate::server::trusted_block::TrustedSubmissionError;

use super::submission_lifecycle::{
    committed_block_lookup, DuplicateBody, RelayPolicy, Submission, VerificationOutcome,
};

/// The shared deadline of the concurrent parent and duplicate lookups. Both only gate early
/// relay or short-circuit a known duplicate; busy state services must not delay validation.
const STATE_LOOKUP_TIMEOUT: Duration = Duration::from_millis(100);

/// What state reported for the submitted header hash before registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubmittedHash {
    /// Not known to state.
    New,
    /// Committed to the finalized state or a non-finalized chain.
    Committed,
    /// Sent for commit by another path, but not committed yet.
    InFlight,
    /// The lookup failed or missed its deadline.
    Unconfirmed,
}

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
        let deadline = tokio::time::Instant::now() + STATE_LOOKUP_TIMEOUT;
        let (parent_known, submitted) = tokio::join!(
            self.parent_is_known(block.header.previous_block_hash, deadline),
            self.submitted_hash_status(hash, deadline),
        );
        // A committed hash never reaches the pending registry: peers and the verifier already
        // treat it as known, so a temporary body could only shadow the committed block.
        if submitted == SubmittedHash::Committed {
            metrics::counter!("rpc.trusted_submission.outcomes", "outcome" => "duplicate")
                .increment(1);
            tracing::info!(
                ?hash,
                "trusted submission is already committed: skipping registration and relay"
            );
            return Ok(SubmitBlockErrorResponse::Duplicate.into());
        }
        let skipped = match (parent_known, submitted) {
            (false, _) => Some((
                "unknown_parent",
                "parent is not confirmed: validating the trusted submission without early relay",
            )),
            (true, SubmittedHash::InFlight) => Some((
                "in_flight",
                "block is already being committed: validating the trusted submission without early relay",
            )),
            (true, SubmittedHash::Unconfirmed) => Some((
                "unconfirmed_new",
                "block is not confirmed new: validating the trusted submission without early relay",
            )),
            (true, SubmittedHash::New | SubmittedHash::Committed) => None,
        };
        if let Some((reason, message)) = skipped {
            metrics::counter!("rpc.trusted_submission.early_relay_skipped", "reason" => reason)
                .increment(1);
            tracing::info!(?hash, "{message}");
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
                early_relay: skipped.is_none(),
                committed_block: committed_block_lookup(self.read_state.clone()),
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
                let label = crate::server::trusted_block::outcome(&response);
                if let Some(body) = outcome.duplicate_body {
                    metrics::counter!("rpc.trusted_submission.duplicate_body", "body" => body.label())
                        .increment(1);
                    if body != DuplicateBody::Identical {
                        tracing::warn!(
                            ?hash,
                            body = body.label(),
                            "trusted submission's hash committed with a body not confirmed equal to it: withdrawing the submitted body"
                        );
                    }
                }
                if outcome.relay.requested && response != SubmitBlockResponse::Accepted {
                    let dispatched = outcome.relay.was_dispatched();
                    metrics::counter!("rpc.trusted_submission.relay_requested_without_commit", "outcome" => label)
                        .increment(1);
                    if outcome.duplicate_body == Some(DuplicateBody::Identical) {
                        // Another path committed these exact bytes; nothing for an operator to do.
                        tracing::info!(
                            ?hash,
                            dispatched,
                            "trusted submission was committed by another path after early relay was requested"
                        );
                    } else {
                        tracing::warn!(
                            ?hash,
                            outcome = label,
                            dispatched,
                            "trusted submission was not committed after early relay was requested"
                        );
                    }
                    if response == SubmitBlockErrorResponse::Rejected.into() {
                        metrics::counter!("rpc.trusted_submission.rejected_after_relay_requested")
                            .increment(1);
                        if dispatched {
                            metrics::counter!("rpc.trusted_submission.rejected_after_dispatch")
                                .increment(1);
                        }
                    }
                }
                metrics::counter!("rpc.trusted_submission.outcomes", "outcome" => label).increment(1);
                response
            },
        );
        Ok(lifecycle
            .await
            .unwrap_or_else(|_| SubmitBlockErrorResponse::Inconclusive.into()))
    }

    /// Returns whether `parent` is confirmed committed before `deadline`.
    /// Failure or timeout, including service readiness, only skips early relay: the block
    /// is still validated and announced after it commits.
    async fn parent_is_known(&self, parent: block::Hash, deadline: tokio::time::Instant) -> bool {
        matches!(
            tokio::time::timeout_at(
                deadline,
                self.read_state
                    .clone()
                    .oneshot(ReadRequest::BlockInfo(parent.into())),
            )
            .await,
            Ok(Ok(ReadResponse::BlockInfo(Some(_))))
        )
    }

    /// Classifies `hash` with the same state request the verifier's duplicate check uses.
    /// Failure or timeout only skips early relay, like the parent lookup: the verifier still
    /// performs its own duplicate check.
    async fn submitted_hash_status(
        &self,
        hash: block::Hash,
        deadline: tokio::time::Instant,
    ) -> SubmittedHash {
        use zakura_state::KnownBlock;
        match tokio::time::timeout_at(
            deadline,
            self.state
                .clone()
                .oneshot(zakura_state::Request::KnownBlock(hash)),
        )
        .await
        {
            Ok(Ok(zakura_state::Response::KnownBlock(None))) => SubmittedHash::New,
            Ok(Ok(zakura_state::Response::KnownBlock(Some(
                KnownBlock::Finalized | KnownBlock::BestChain | KnownBlock::SideChain,
            )))) => SubmittedHash::Committed,
            Ok(Ok(zakura_state::Response::KnownBlock(Some(
                KnownBlock::WriteChannel | KnownBlock::Queue,
            )))) => SubmittedHash::InFlight,
            _ => SubmittedHash::Unconfirmed,
        }
    }
}
