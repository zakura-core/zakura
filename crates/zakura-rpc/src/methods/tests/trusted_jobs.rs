//! Prepared trusted jobs against a mock verifier and chain tip, on a paused clock.
//!
//! The network is Mainnet, so the solved header is checked against real proof of work: the
//! proposal is Mainnet block 1 with its nonce cleared, and the solution is that block's header.

use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::{sync::mpsc, time::Duration};
use zakura_chain::{
    block::{self, Block, Height},
    chain_tip::mock::{MockChainTip, MockChainTipSender},
    parameters::Network,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zakura_consensus::{BlockError, Request as VerifyRequest, RouterError, VerifyBlockError};
use zakura_state::{KnownBlock, ReadRequest, ReadResponse};
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::{
    methods::trusted_block::jobs::{JobError, JobId, Jobs, Operation},
    tests::test_rpc::TestRpcImpl,
    EarlyMinedBlock,
};

type Verifier = MockService<VerifyRequest, block::Hash, PanicAssertion, tower::BoxError>;

const JOB: JobId = [7; 16];

/// The jobs of one listener, with the services a job worker calls.
struct Relay {
    rpc: TestRpcImpl,
    jobs: Jobs,
    tip: MockChainTipSender,
    verifier: Verifier,
    early: mpsc::UnboundedReceiver<EarlyMinedBlock>,
    /// The mined block, its unsolved proposal, and the parent the proposal builds on.
    solved: Arc<Block>,
    proposal: Vec<u8>,
    parent: block::Hash,
    queue: tokio::task::JoinHandle<()>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.queue.abort();
    }
}

impl Relay {
    /// A relay whose best tip is the proposal's parent.
    fn at_parent() -> Self {
        let relay = Self::without_tip();
        relay.set_tip(Height(0), relay.parent);
        relay
    }

    fn without_tip() -> Self {
        let solved: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("the block 1 test vector is valid");
        let mut unsolved = (*solved).clone();
        Arc::make_mut(&mut unsolved.header).nonce = Default::default();
        let committed = solved.clone();
        let read_state = tower::service_fn(move |request: ReadRequest| {
            // Only a duplicate verdict reads state, to compare bodies.
            assert!(matches!(request, ReadRequest::AnyChainBlock(_)));
            let block = committed.clone().into();
            async move { Ok::<_, tower::BoxError>(ReadResponse::Block(Some(block))) }
        });
        let state = tower::service_fn(|request: zakura_state::Request| async move {
            panic!("a job never calls the write state service, got {request:?}");
            #[allow(unreachable_code)]
            Ok::<zakura_state::Response, tower::BoxError>(zakura_state::Response::Committed(
                block::Hash([0; 32]),
            ))
        });
        let mempool: MockService<_, _, _, tower::BoxError> = MockService::build().for_unit_tests();
        let verifier: Verifier = MockService::build().for_unit_tests();
        let (tip, tip_sender) = MockChainTip::new();
        let (events, early) = mpsc::unbounded_channel();
        let (rpc, queue) = TestRpcImpl::new_for_tests(
            Network::Mainnet,
            Default::default(),
            false,
            "test",
            "test",
            tower::buffer::Buffer::new(mempool, 1),
            state,
            read_state,
            verifier.clone(),
            Default::default(),
            tip,
            Default::default(),
            tokio::sync::watch::channel(None).1,
            Some(events),
        );
        Self {
            rpc,
            jobs: Jobs::default(),
            tip: tip_sender,
            verifier,
            early,
            parent: solved.header.previous_block_hash,
            proposal: unsolved.zcash_serialize_to_vec().unwrap(),
            solved,
            queue,
        }
    }

    fn set_tip(&self, height: Height, hash: block::Hash) {
        self.tip.send_best_tip_hash(hash);
        self.tip.send_best_tip_height(height);
    }

    async fn send(&self, id: JobId, op: Operation, bytes: Vec<u8>) -> Result<(), JobError> {
        self.rpc
            .trusted_job(self.jobs.clone(), id, op, Bytes::from(bytes))
            .await
    }

    async fn prepare(&self) -> Result<(), JobError> {
        self.send(JOB, Operation::Prepare, self.proposal.clone())
            .await
    }

    async fn solve(&self) -> Result<(), JobError> {
        self.send(JOB, Operation::Solve, self.solved_header()).await
    }

    async fn confirm(&self) -> Result<(), JobError> {
        self.send(JOB, Operation::Confirm, self.confirmation().to_vec())
            .await
    }

    fn solved_header(&self) -> Vec<u8> {
        self.solved.header.zcash_serialize_to_vec().unwrap()
    }

    fn solved_bytes(&self) -> Vec<u8> {
        self.solved.zcash_serialize_to_vec().unwrap()
    }

    fn confirmation(&self) -> [u8; 64] {
        let mut confirmation = [0; 64];
        confirmation[..32].copy_from_slice(&self.solved.hash().0);
        confirmation[32..].copy_from_slice(&Sha256::digest(self.solved_bytes()));
        confirmation
    }

    /// The proposal verification the worker started, still unanswered.
    async fn proposal_verification(
        &mut self,
    ) -> zakura_test::mock_service::ResponseSender<VerifyRequest, block::Hash, tower::BoxError>
    {
        let proposal_hash = self.proposal_hash();
        self.verifier
            .expect_request_that(|request| {
                matches!(
                    request,
                    VerifyRequest::Prepare {
                        source: zakura_consensus::PreparedCandidateSource::ClientProposal,
                        ..
                    }
                ) && request.block().hash() == proposal_hash
            })
            .await
    }

    /// The solved block's commit, still unanswered.
    async fn commit(
        &mut self,
    ) -> zakura_test::mock_service::ResponseSender<VerifyRequest, block::Hash, tower::BoxError>
    {
        let hash = self.solved.hash();
        self.verifier
            .expect_request_that(|request| {
                matches!(request, VerifyRequest::CommitMined { .. })
                    && request.block().hash() == hash
            })
            .await
    }

    fn proposal_hash(&self) -> block::Hash {
        let proposal: Block = self.proposal.zcash_deserialize_into().unwrap();
        proposal.hash()
    }

    /// The early relay event of the solved block, with the bytes peers would be served.
    async fn released(&mut self) -> Vec<u8> {
        let event = tokio::time::timeout(Duration::from_secs(1), self.early.recv())
            .await
            .expect("the job releases promptly")
            .expect("the relay channel stays open");
        assert_eq!(event.hash, self.solved.hash());
        self.rpc
            .gbt
            .pending_blocks()
            .get_for_peer(self.solved.hash())
            .expect("a released block is visible to peers")
            .pending_send
            .expect("a pending body carries its permit")
            .prepared_bytes()
            .expect("a job serves its exact bytes")
            .to_vec()
    }

    fn is_released(&self) -> bool {
        self.rpc
            .gbt
            .pending_blocks()
            .get(self.solved.hash())
            .is_some()
    }

    /// Replays the proposal until its job has ended, and returns the job's outcome.
    async fn outcome(&self) -> Result<(), JobError> {
        self.outcome_of(JOB, Operation::Prepare, self.proposal.clone())
            .await
    }

    /// Replays a stored input until it stops being acknowledged as pending, within a second.
    ///
    /// A replay of a live job's stored input is acknowledged, so the first error is the job's
    /// outcome. A job that ended in a commit keeps acknowledging; that case waits out the second.
    async fn outcome_of(&self, id: JobId, op: Operation, bytes: Vec<u8>) -> Result<(), JobError> {
        for _ in 0..1_000 {
            self.send(id, op, bytes.clone()).await?;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    /// Waits up to a second for the job to end as committed.
    ///
    /// Only an ended job acknowledges an input that contradicts its proposal: a live job
    /// refuses it as a conflict, and a job that ended otherwise reports its error.
    async fn committed(&self) -> bool {
        for _ in 0..1_000 {
            match self.send(JOB, Operation::Prepare, vec![0xee]).await {
                Ok(()) => return true,
                Err(JobError::Conflict) => tokio::time::sleep(Duration::from_millis(1)).await,
                Err(_) => return false,
            }
        }
        false
    }

    /// Whether `count` new jobs can start, which shows how many slots are free.
    async fn can_start(&self, count: u8) -> bool {
        for index in 0..count {
            let id = [0x80 | index; 16];
            if self.send(id, Operation::Confirm, vec![index; 64]).await == Err(JobError::Capacity) {
                return false;
            }
        }
        true
    }
}

fn rejection() -> tower::BoxError {
    RouterError::from(VerifyBlockError::from(BlockError::NoTransactions)).into()
}

#[tokio::test(start_paused = true)]
async fn verified_proposal_releases_the_exact_solved_bytes_then_commits() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    relay
        .proposal_verification()
        .await
        .respond(relay.proposal_hash());
    assert!(
        !relay.is_released(),
        "a verified proposal still needs its solved header"
    );
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(relay.released().await, relay.solved_bytes());
    // Inputs replayed while the commit runs are acknowledged.
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(
        relay.send(JOB, Operation::Prepare, vec![0xee]).await,
        Err(JobError::Conflict),
        "the job is still live while its commit runs"
    );
    relay.commit().await.respond(relay.solved.hash());
    assert!(relay.committed().await);
    assert_eq!(relay.confirm().await, Ok(()));
    assert!(
        relay.can_start(16).await,
        "a committed job frees its slot at once"
    );
}

#[tokio::test(start_paused = true)]
async fn confirmation_releases_while_local_verification_is_still_running() {
    let mut relay = Relay::at_parent();
    // The short inputs can overtake the upload.
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(relay.confirm().await, Ok(()));
    assert!(!relay.is_released(), "a job without its proposal waits");
    assert_eq!(relay.prepare().await, Ok(()));
    let verification = relay.proposal_verification().await;
    assert_eq!(relay.released().await, relay.solved_bytes());
    // The abandoned proposal verification cannot veto or delay the commit.
    verification.respond_error(rejection());
    relay.commit().await.respond(relay.solved.hash());
    assert!(relay.committed().await);
}

#[tokio::test(start_paused = true)]
async fn confirmation_must_name_the_exact_solved_bytes() {
    let mut relay = Relay::at_parent();
    let mut other = relay.confirmation();
    other[63] ^= 1;
    // Before the solved bytes are known, any confirmation is stored.
    assert_eq!(
        relay.send(JOB, Operation::Confirm, other.to_vec()).await,
        Ok(())
    );
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.solve().await, Ok(()));
    let _verification = relay.proposal_verification().await;
    // Once they are known, the wrong confirmation is discarded and refused from then on.
    let mut refused = false;
    for _ in 0..1_000 {
        if relay.send(JOB, Operation::Confirm, other.to_vec()).await == Err(JobError::Conflict) {
            refused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(refused, "a confirmation of other bytes conflicts");
    assert!(!relay.is_released());
    // A refused confirmation leaves the job waiting for the right one.
    assert_eq!(relay.confirm().await, Ok(()));
    assert_eq!(relay.released().await, relay.solved_bytes());
    assert_eq!(
        relay.send(JOB, Operation::Confirm, vec![0; 63]).await,
        Err(JobError::Invalid),
        "a confirmation is exactly 64 bytes"
    );
}

#[tokio::test(start_paused = true)]
async fn proposal_prepared_before_its_parent_commits_waits_for_the_tip() {
    let mut relay = Relay::without_tip();
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.solve().await, Ok(()));
    relay.verifier.expect_no_requests().await;
    assert_eq!(relay.prepare().await, Ok(()), "the job is still waiting");
    // The parent commits here after the miner started on the new template.
    relay.set_tip(Height(0), relay.parent);
    relay
        .proposal_verification()
        .await
        .respond(relay.proposal_hash());
    assert_eq!(relay.released().await, relay.solved_bytes());
}

#[tokio::test(start_paused = true)]
async fn stale_proposal_expires_without_verification_and_frees_its_slot() {
    let mut relay = Relay::without_tip();
    // The tip is already at the proposal's height on another block.
    relay.set_tip(Height(1), block::Hash([9; 32]));
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.outcome().await, Err(JobError::Expired));
    relay.verifier.expect_no_requests().await;
    assert_eq!(relay.solve().await, Err(JobError::Expired));
    assert!(relay.can_start(16).await);
}

#[tokio::test(start_paused = true)]
async fn tip_change_expires_a_job_that_is_waiting_for_its_solution() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    relay
        .proposal_verification()
        .await
        .respond(relay.proposal_hash());
    assert_eq!(relay.prepare().await, Ok(()));
    // Another block extends the parent first.
    relay.set_tip(Height(1), block::Hash([9; 32]));
    assert_eq!(relay.outcome().await, Err(JobError::Expired));
    assert_eq!(relay.solve().await, Err(JobError::Expired));
    assert_eq!(relay.confirm().await, Err(JobError::Expired));
    assert!(!relay.is_released());
    assert!(relay.early.try_recv().is_err());
    assert!(relay.can_start(16).await);
}

#[tokio::test(start_paused = true)]
async fn inputs_for_a_job_past_its_lifetime_report_expiry_instead_of_starting_a_job() {
    let relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    tokio::time::sleep(Duration::from_secs(119)).await;
    assert_eq!(
        relay.prepare().await,
        Ok(()),
        "the job lives for 120 seconds"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    // The miner finds its solution after the job expired.
    assert_eq!(relay.solve().await, Err(JobError::Expired));
    assert_eq!(relay.confirm().await, Err(JobError::Expired));
    assert!(relay.can_start(16).await, "an expired job holds no slot");
    // The outcome is forgotten 120 seconds after the job ended; the ID is then unknown again.
    tokio::time::sleep(Duration::from_secs(121)).await;
    assert_eq!(relay.solve().await, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn solved_header_needs_valid_proof_of_work_before_it_joins_a_job() {
    let relay = Relay::at_parent();
    let mut header = *relay.solved.header;
    header.nonce = Default::default();
    let unsolved = header.zcash_serialize_to_vec().unwrap();
    assert_eq!(
        relay.send(JOB, Operation::Solve, unsolved.clone()).await,
        Err(JobError::Invalid)
    );
    let mut trailing = relay.solved_header();
    trailing.push(0);
    assert_eq!(
        relay.send(JOB, Operation::Solve, trailing).await,
        Err(JobError::Invalid)
    );
    assert_eq!(
        relay.send(JOB, Operation::Solve, vec![0; 100]).await,
        Err(JobError::Invalid)
    );
    // A refused header started no job, so the valid one is this ID's first input.
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(
        relay.send(JOB, Operation::Solve, unsolved).await,
        Err(JobError::Invalid)
    );
}

#[tokio::test(start_paused = true)]
async fn solved_header_of_another_proposal_conflicts_and_ends_the_job() {
    let mut relay = Relay::at_parent();
    // A valid solved header for a different block.
    let other: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let other_header = other.header.zcash_serialize_to_vec().unwrap();
    assert_eq!(
        relay
            .send(JOB, Operation::Solve, other_header.clone())
            .await,
        Ok(())
    );
    assert_eq!(
        relay.solve().await,
        Err(JobError::Conflict),
        "a second, different header conflicts with the stored one"
    );
    assert_eq!(relay.prepare().await, Ok(()));
    let _verification = relay.proposal_verification().await;
    assert_eq!(relay.outcome().await, Err(JobError::Conflict));
    assert_eq!(
        relay.send(JOB, Operation::Solve, other_header).await,
        Err(JobError::Conflict)
    );
    assert!(!relay.is_released());
    assert!(relay.early.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn local_rejection_vetoes_release_even_with_a_confirmation() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    let verification = relay.proposal_verification().await;
    // The confirmation is already stored when the rejection arrives. The solved header is
    // not, so the confirmation cannot have released the job first.
    assert_eq!(relay.confirm().await, Ok(()));
    verification.respond_error(rejection());
    assert_eq!(relay.outcome().await, Err(JobError::Invalid));
    assert_eq!(relay.solve().await, Err(JobError::Invalid));
    assert_eq!(relay.confirm().await, Err(JobError::Invalid));
    assert!(!relay.is_released());
    assert!(relay.early.try_recv().is_err());
}

/// The solved header is bound and the worker is waiting for either authority. A rejection and
/// the matching confirmation arrive together, and the rejection decides.
#[tokio::test(start_paused = true)]
async fn local_rejection_takes_precedence_over_a_simultaneous_confirmation() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.solve().await, Ok(()));
    let verification = relay.proposal_verification().await;
    // Long enough for the worker to bind the header and wait for an authority.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut other = relay.confirmation();
    other[63] ^= 1;
    assert_eq!(
        relay.send(JOB, Operation::Confirm, other.to_vec()).await,
        Err(JobError::Conflict),
        "the worker knows the solved bytes"
    );
    // Both happen before the worker runs again.
    assert_eq!(relay.confirm().await, Ok(()));
    verification.respond_error(rejection());
    assert_eq!(relay.outcome().await, Err(JobError::Invalid));
    assert!(!relay.is_released());
    assert!(relay.early.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn unavailable_local_verification_leaves_the_confirmation_path_open() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.solve().await, Ok(()));
    relay
        .proposal_verification()
        .await
        .respond_error("the verifier service is closed".into());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !relay.is_released(),
        "an unavailable verifier authorizes nothing"
    );
    assert_eq!(relay.confirm().await, Ok(()));
    assert_eq!(relay.released().await, relay.solved_bytes());
}

#[tokio::test(start_paused = true)]
async fn rejected_commit_withdraws_the_prepared_body_and_ends_the_job_invalid() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(relay.confirm().await, Ok(()));
    let _verification = relay.proposal_verification().await;
    assert_eq!(relay.released().await, relay.solved_bytes());
    let permit = relay
        .rpc
        .gbt
        .pending_blocks()
        .get_for_peer(relay.solved.hash())
        .unwrap()
        .pending_send
        .unwrap();
    relay.commit().await.respond_error(rejection());
    assert_eq!(relay.outcome().await, Err(JobError::Invalid));
    assert!(!relay.is_released());
    assert!(
        permit
            .try_start_write(|| panic!("a withdrawn body cannot start a write"))
            .is_none(),
        "a response that fetched the prepared bytes before the rejection cannot send them"
    );
}

#[tokio::test(start_paused = true)]
async fn block_committed_by_another_path_ends_the_job_as_committed() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(relay.confirm().await, Ok(()));
    let _verification = relay.proposal_verification().await;
    assert_eq!(relay.released().await, relay.solved_bytes());
    let hash = relay.solved.hash();
    // Gossip committed the same block while this job's commit was queued.
    relay
        .commit()
        .await
        .respond_error(tower::BoxError::from(RouterError::from(
            VerifyBlockError::from(BlockError::AlreadyInChain(hash, KnownBlock::BestChain)),
        )));
    assert!(
        relay.committed().await,
        "a header hash committed elsewhere ends the job as committed"
    );
    assert!(
        !relay.is_released(),
        "the duplicate verdict resolves the job"
    );
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(relay.confirm().await, Ok(()));
}

/// The lifetime bounds the wait for a release. A commit that is still running when it passes
/// keeps its own verdict.
#[tokio::test(start_paused = true)]
async fn commit_that_outlives_the_job_lifetime_still_reports_its_own_outcome() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    relay
        .proposal_verification()
        .await
        .respond(relay.proposal_hash());
    // The miner finds its solution shortly before the job would expire.
    tokio::time::sleep(Duration::from_secs(110)).await;
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(relay.released().await, relay.solved_bytes());
    let commit = relay.commit().await;
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(
        relay.solve().await,
        Ok(()),
        "the job is past its lifetime and still committing"
    );
    assert!(relay.is_released());
    commit.respond(relay.solved.hash());
    assert!(relay.committed().await);
}

/// A released job follows the complete POST's validation deadline.
#[tokio::test(start_paused = true)]
async fn commit_that_misses_the_validation_deadline_withdraws_the_body_as_unavailable() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.solve().await, Ok(()));
    assert_eq!(relay.confirm().await, Ok(()));
    let _verification = relay.proposal_verification().await;
    assert_eq!(relay.released().await, relay.solved_bytes());
    let _commit = relay.commit().await;
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(relay.outcome().await, Err(JobError::Unavailable));
    assert!(!relay.is_released());
}

/// The parent stops being the tip while the proposal is in verification, and verification
/// fails for that reason. The job is stale, not invalid.
#[tokio::test(start_paused = true)]
async fn verification_failure_caused_by_a_moved_tip_ends_the_job_as_expired() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    let verification = relay.proposal_verification().await;
    relay.set_tip(Height(1), block::Hash([9; 32]));
    verification.respond_error(tower::BoxError::from(RouterError::from(
        VerifyBlockError::ValidateProposal(
            "proposal is not based on the current best chain tip".into(),
        ),
    )));
    assert_eq!(relay.outcome().await, Err(JobError::Expired));
}

/// The worker sees the moved tip and a rejection at once. The tip decides: verification of a
/// proposal whose parent stopped being the tip can fail for that reason alone, so the job is
/// reported as stale.
#[tokio::test(start_paused = true)]
async fn moved_tip_takes_precedence_over_a_simultaneous_rejection() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    let verification = relay.proposal_verification().await;
    // Both happen before the worker runs again.
    relay.set_tip(Height(1), block::Hash([9; 32]));
    verification.respond_error(rejection());
    assert_eq!(relay.outcome().await, Err(JobError::Expired));
}

/// A failed state call during proposal verification is no verdict on the proposal.
#[tokio::test(start_paused = true)]
async fn state_failure_during_proposal_verification_leaves_the_confirmation_path_open() {
    let mut relay = Relay::at_parent();
    assert_eq!(relay.prepare().await, Ok(()));
    assert_eq!(relay.solve().await, Ok(()));
    relay
        .proposal_verification()
        .await
        .respond_error(tower::BoxError::from(RouterError::from(
            VerifyBlockError::ValidateProposal("state service is unavailable".into()),
        )));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!relay.is_released());
    assert_eq!(relay.prepare().await, Ok(()), "the job is still waiting");
    assert_eq!(relay.confirm().await, Ok(()));
    assert_eq!(relay.released().await, relay.solved_bytes());
}

#[tokio::test(start_paused = true)]
async fn undecodable_or_non_canonical_proposals_end_their_job_invalid() {
    let relay = Relay::at_parent();
    let mut trailing = relay.proposal.clone();
    trailing.push(0);
    for (index, proposal) in [vec![3; 100], trailing].into_iter().enumerate() {
        let id = [u8::try_from(index).unwrap(); 16];
        // Receipt is acknowledged; decoding happens in the worker.
        assert_eq!(
            relay.send(id, Operation::Prepare, proposal.clone()).await,
            Ok(())
        );
        assert_eq!(
            relay.outcome_of(id, Operation::Prepare, proposal).await,
            Err(JobError::Invalid)
        );
        assert_eq!(
            relay
                .send(id, Operation::Solve, relay.solved_header())
                .await,
            Err(JobError::Invalid)
        );
    }
    assert!(relay.can_start(16).await);
}

#[tokio::test(start_paused = true)]
async fn unfinished_jobs_are_bounded_and_ended_jobs_do_not_count() {
    let relay = Relay::at_parent();
    assert!(relay.can_start(16).await);
    assert_eq!(
        relay.prepare().await,
        Err(JobError::Capacity),
        "a seventeenth unfinished job is refused"
    );
    // All sixteen expire together and free their slots.
    tokio::time::sleep(Duration::from_secs(121)).await;
    assert_eq!(relay.prepare().await, Ok(()));
}
