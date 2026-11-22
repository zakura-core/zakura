//! Mining jobs retained by the opt-in trusted listener. A single worker owns each release.

use super::*;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use tokio::time::Instant;

pub(crate) type JobId = [u8; 16];
const LIFETIME: Duration = Duration::from_secs(120);
const CAPACITY: usize = 16;

#[derive(Clone, Copy)]
pub(crate) enum Operation {
    Prepare,
    Solve,
    Confirm,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JobError {
    Invalid,
    Conflict,
    Capacity,
    Expired,
    Unavailable,
}

#[derive(Default)]
struct Inputs {
    body: Option<Bytes>,
    header: Option<Arc<Header>>,
    confirmation: Option<[u8; 64]>,
    expected_confirmation: Option<[u8; 64]>,
    failure: Option<JobError>,
}

enum Update {
    Prepare(Bytes),
    Solve(Arc<Header>),
    Confirm([u8; 64]),
}

impl Inputs {
    fn update(&mut self, update: Update) -> std::result::Result<bool, JobError> {
        fn set<T: Eq>(slot: &mut Option<T>, value: T) -> std::result::Result<bool, JobError> {
            match slot {
                Some(old) if old == &value => Ok(false),
                Some(_) => Err(JobError::Conflict),
                None => {
                    *slot = Some(value);
                    Ok(true)
                }
            }
        }
        if let Some(e) = self.failure {
            return Err(e);
        }
        match update {
            Update::Prepare(body) => set(&mut self.body, body),
            Update::Solve(header) => set(&mut self.header, header),
            Update::Confirm(digest) => {
                if self
                    .expected_confirmation
                    .is_some_and(|expected| expected != digest)
                {
                    return Err(JobError::Conflict);
                }
                set(&mut self.confirmation, digest)
            }
        }
    }
}

type JobSender = watch::Sender<Inputs>;
type JobReceiver = watch::Receiver<Inputs>;

#[derive(Clone, Default)]
pub(crate) struct Jobs(Arc<Mutex<HashMap<JobId, JobSender>>>);

impl Jobs {
    /// Install the first message under the miner's ID and start exactly one worker.
    /// Retaining terminal entries until expiry makes duplicate deliveries idempotent.
    fn register(
        &self,
        id: JobId,
        update: Update,
    ) -> std::result::Result<Option<(JobSender, JobReceiver, Instant)>, JobError> {
        let mut jobs = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(sender) = jobs.get(&id) {
            let mut result = Ok(false);
            sender.send_if_modified(|inputs| {
                result = inputs.update(update);
                matches!(result, Ok(true))
            });
            return result.map(|_| None);
        }
        if jobs.len() >= CAPACITY {
            return Err(JobError::Capacity);
        }
        let mut inputs = Inputs::default();
        inputs.update(update)?;
        let (sender, receiver) = watch::channel(inputs);
        jobs.insert(id, sender.clone());
        Ok(Some((sender, receiver, Instant::now() + LIFETIME)))
    }
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
    /// Queues a complete input. The HTTP acknowledgement never waits for proposal verification.
    pub(crate) async fn trusted_job(
        &self,
        jobs: Jobs,
        id: JobId,
        op: Operation,
        bytes: Bytes,
    ) -> std::result::Result<(), JobError> {
        let update = match op {
            Operation::Prepare => Update::Prepare(bytes),
            Operation::Solve => {
                let network = self.network.clone();
                let height = self
                    .latest_chain_tip
                    .best_tip_height()
                    .unwrap_or(Height::MIN);
                let header = tokio::task::spawn_blocking(move || {
                    let mut input = bytes.as_ref();
                    let header =
                        Header::zcash_deserialize(&mut input).map_err(|_| JobError::Invalid)?;
                    if !input.is_empty()
                        || !zakura_consensus::proof_of_work_is_valid(
                            &header,
                            &network,
                            &height,
                            &header.hash(),
                        )
                    {
                        return Err(JobError::Invalid);
                    }
                    Ok(Arc::new(header))
                })
                .await
                .map_err(|_| JobError::Unavailable)??;
                Update::Solve(header)
            }
            Operation::Confirm => {
                Update::Confirm(bytes.as_ref().try_into().map_err(|_| JobError::Invalid)?)
            }
        };
        if let Some((sender, receiver, deadline)) = jobs.register(id, update)? {
            let rpc = self.clone();
            tokio::spawn(async move {
                let result = tokio::time::timeout_at(
                    deadline,
                    rpc.run_trusted_job(&sender, receiver, deadline),
                )
                .await
                .unwrap_or(Err(JobError::Expired));
                if let Err(error) = result {
                    tracing::info!(
                        ?error,
                        "prepared trusted submission ended without acceptance"
                    );
                }
                sender.send_modify(|inputs| inputs.failure = result.err());
                tokio::time::sleep_until(deadline).await;
                jobs.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&id);
            });
        }
        Ok(())
    }

    async fn run_trusted_job(
        &self,
        sender: &JobSender,
        mut inputs: JobReceiver,
        deadline: Instant,
    ) -> std::result::Result<(), JobError> {
        let bytes = inputs
            .wait_for(|parts| parts.body.is_some())
            .await
            .map_err(|_| JobError::Expired)?
            .body
            .clone()
            .expect("waited for the body");
        let (candidate, transactions) = tokio::task::spawn_blocking(move || {
            let mut remaining = bytes.as_ref();
            let block =
                Block::zcash_deserialize_bounded(&mut remaining).map_err(|_| JobError::Invalid)?;
            // Proposal verification and the peer must see exactly the same transaction encoding.
            if !remaining.is_empty()
                || block
                    .zcash_serialize_to_vec()
                    .map_err(|_| JobError::Invalid)?
                    != bytes
            {
                return Err(JobError::Invalid);
            }
            let transactions = bytes.slice(block.header.zcash_serialized_size()..);
            Ok((Arc::new(block), transactions))
        })
        .await
        .map_err(|_| JobError::Unavailable)??;
        let parent = candidate.header.previous_block_hash;
        if self.latest_chain_tip.best_tip_hash() != Some(parent) {
            return Err(JobError::Expired);
        }
        let height = candidate.coinbase_height().ok_or(JobError::Invalid)?;
        let request =
            self.gbt
                .block_verifier_router()
                .oneshot(zakura_consensus::Request::Prepare {
                    block: candidate.clone().into(),
                    source: zakura_consensus::PreparedCandidateSource::ClientProposal,
                });
        let verify = local_approval(request);
        tokio::pin!(verify);
        let mut verified = false;
        let mut solve_inputs = inputs.clone();
        let solve = async {
            solve_inputs
                .wait_for(|parts| parts.header.is_some())
                .await
                .map_err(|_| JobError::Expired)
                .map(|parts| parts.header.clone().expect("waited for the solved header"))
        };
        tokio::pin!(solve);
        let header = tokio::select! {
            biased;
            result = &mut verify => {
                result?;
                verified = true;
                solve.await?
            }
            header = &mut solve => header?,
        };
        let solved = bind_header(&candidate, header)?;
        let header = solved.header.clone();
        let (bytes, confirmation) =
            tokio::task::spawn_blocking(move || -> std::result::Result<_, JobError> {
                let mut wire = header
                    .zcash_serialize_to_vec()
                    .map_err(|_| JobError::Invalid)?;
                if wire.len().saturating_add(transactions.len())
                    > usize::try_from(block::MAX_BLOCK_BYTES)
                        .expect("block limit fits supported platforms")
                {
                    return Err(JobError::Invalid);
                }
                wire.extend_from_slice(&transactions);
                let mut confirmation = [0; 64];
                confirmation[..32].copy_from_slice(&header.hash().0);
                confirmation[32..].copy_from_slice(&Sha256::digest(&wire));
                Ok((Arc::<[u8]>::from(wire), confirmation))
            })
            .await
            .map_err(|_| JobError::Unavailable)??;
        sender.send_modify(|parts| {
            parts.expected_confirmation = Some(confirmation);
            if parts
                .confirmation
                .is_some_and(|received| received != confirmation)
            {
                parts.confirmation = None;
            }
        });
        if !verified {
            tokio::select! {
                biased;
                result = &mut verify => { result?; }
                result = inputs.wait_for(|parts| parts.confirmation == Some(confirmation)) => {
                    drop(result.map_err(|_| JobError::Expired)?);
                }
            }
        }
        if Instant::now() >= deadline || self.latest_chain_tip.best_tip_hash() != Some(parent) {
            return Err(JobError::Expired);
        }
        let block = solved;
        match self
            .relay_trusted_block(
                block,
                height,
                bytes.len(),
                std::time::Instant::now(),
                true,
                Some(bytes),
            )
            .await
        {
            Ok(SubmitBlockResponse::Accepted) => Ok(()),
            Ok(SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Rejected)) => {
                Err(JobError::Invalid)
            }
            Err(TrustedSubmissionError::Capacity) => Err(JobError::Capacity),
            _ => Err(JobError::Unavailable),
        }
    }
}

/// An unavailable verifier gives no local verdict. Keep the primary confirmation eligible
/// until the job deadline. An actual rejection vetoes early relay.
async fn local_approval(
    verify: impl std::future::Future<Output = std::result::Result<block::Hash, tower::BoxError>>,
) -> std::result::Result<(), JobError> {
    match verify.await {
        Ok(_) => Ok(()),
        Err(error)
            if error
                .downcast_ref::<RouterError>()
                .is_some_and(|e| !e.is_verification_unavailable()) =>
        {
            Err(JobError::Invalid)
        }
        Err(error) => {
            tracing::debug!(%error, "local proposal verification unavailable; awaiting primary confirmation");
            std::future::pending().await
        }
    }
}

/// Proposal mode skips work on the nonce and solution. Every other header field, including
/// time, must be the one the relay verified for these exact transactions.
fn bind_header(
    candidate: &Block,
    header: Arc<Header>,
) -> std::result::Result<Arc<Block>, JobError> {
    let expected = Header {
        nonce: header.nonce,
        solution: header.solution,
        ..*candidate.header
    };
    if expected != *header {
        return Err(JobError::Conflict);
    }
    Ok(Arc::new(Block {
        header,
        transactions: candidate.transactions.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unavailable_verifier_leaves_confirmation_eligible() {
        use futures::FutureExt;
        let unavailable = std::future::ready(Err("verifier service is closed".into()));
        assert!(local_approval(unavailable).now_or_never().is_none());
        let authorized = std::future::ready(Ok(block::Hash([0; 32])));
        assert_eq!(local_approval(authorized).await, Ok(()));
    }

    #[test]
    fn job_slots_accept_replays_and_bound_incomplete_jobs() {
        let jobs = Jobs::default();
        let body = Bytes::from_static(b"first");
        for id in 0..16 {
            assert!(jobs
                .register([id; 16], Update::Prepare(body.clone()))
                .unwrap()
                .is_some());
        }
        assert!(matches!(
            jobs.register([16; 16], Update::Prepare(body.clone())),
            Err(JobError::Capacity)
        ));
        assert!(jobs
            .register([0; 16], Update::Prepare(body))
            .unwrap()
            .is_none());
        assert!(matches!(
            jobs.register([0; 16], Update::Prepare(Bytes::from_static(b"second"))),
            Err(JobError::Conflict)
        ));
    }

    #[test]
    fn confirmation_checks_the_complete_body_digest() {
        let expected = [1; 64];
        let mut other = expected;
        other[63] ^= 1;
        let mut inputs = Inputs {
            expected_confirmation: Some(expected),
            ..Default::default()
        };
        assert_eq!(
            inputs.update(Update::Confirm(other)),
            Err(JobError::Conflict)
        );
        assert_eq!(inputs.update(Update::Confirm(expected)), Ok(true));
        assert_eq!(inputs.update(Update::Confirm(expected)), Ok(false));
    }

    #[test]
    fn solved_job_cannot_change_the_prepared_time_or_transaction_commitment() {
        let block = zakura_chain::block::genesis::regtest_genesis_block();
        let mut header = *block.header;
        header.nonce[0] ^= 1;
        assert!(bind_header(&block, Arc::new(header)).is_ok());
        let mut later = header;
        later.time += chrono::Duration::seconds(1);
        assert_eq!(
            bind_header(&block, Arc::new(later)),
            Err(JobError::Conflict)
        );
        header.commitment_bytes[0] ^= 1;
        assert_eq!(
            bind_header(&block, Arc::new(header)),
            Err(JobError::Conflict)
        );
    }
}
