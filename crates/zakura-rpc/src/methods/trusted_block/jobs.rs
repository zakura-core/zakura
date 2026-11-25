//! Mining jobs retained by the opt-in trusted listener. A single worker owns each release.
//!
//! A job collects three inputs in any order: the unsolved proposal, the solved header, and
//! optionally the primary's confirmation. Its worker releases the solved block for early relay
//! once the header matches the proposal, the proposal's parent is the best tip, and either local
//! proposal verification passed or the confirmation names the exact solved bytes.

use super::*;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard},
};
use tokio::time::Instant;
use tracing::Instrument;

/// The miner's identifier for one candidate.
pub(crate) type JobId = [u8; 16];

/// The solved header hash followed by the SHA-256 digest of the complete solved block.
type Confirmation = [u8; CONFIRMATION_BYTES];

/// The size of a confirmation body.
pub(crate) const CONFIRMATION_BYTES: usize = 64;

/// How long a job waits for its inputs and its release authority, from its first input.
const LIFETIME: Duration = Duration::from_secs(120);

/// Unfinished jobs retained at once. Each holds one proposal and at most one verification.
const CAPACITY: usize = 16;

/// Ended jobs whose ID and outcome are remembered, so an input that arrives after its job
/// ended reports that outcome instead of starting a job that can never release.
const ENDED_CAPACITY: usize = 64;

/// How long an ended job's outcome is remembered.
const ENDED_RETENTION: Duration = LIFETIME;

/// One input of a job, selected by the request's `op` query.
#[derive(Clone, Copy)]
pub(crate) enum Operation {
    /// The complete serialized unsolved proposal.
    Prepare,
    /// The serialized solved header.
    Solve,
    /// The primary's acceptance of the exact solved bytes.
    Confirm,
}

/// Why a job input was refused, or why its job ended without a local commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JobError {
    /// An input is malformed, or local verification rejected the proposal or the solved block.
    Invalid,
    /// An input contradicts one already received for this job.
    Conflict,
    /// Every job, pending-body or submission slot is occupied.
    Capacity,
    /// The job's parent is not the best tip, or its lifetime passed before release.
    Expired,
    /// The best tip passed the release's last supported mining height.
    EndOfSupport,
    /// The node could not establish an outcome.
    Unavailable,
}

impl JobError {
    fn label(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Conflict => "conflict",
            Self::Capacity => "capacity",
            Self::Expired => "expired",
            Self::EndOfSupport => "end_of_support",
            Self::Unavailable => "unavailable",
        }
    }
}

/// What released a solved block to peers before its local commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Authority {
    /// Local proposal verification passed.
    Verified,
    /// The caller asserted that its primary accepted the exact solved bytes.
    Confirmed,
}

impl Authority {
    fn label(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Confirmed => "confirmed",
        }
    }
}

/// A solved block whose early relay is authorized.
struct Release {
    block: Arc<Block>,
    height: Height,
    parent: block::Hash,
    /// The exact serialized `block`.
    wire: Arc<[u8]>,
    authority: Authority,
}

/// How a released job's local commit ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Committed {
    /// This job's submission committed the block.
    Here,
    /// Another path committed a block with this header hash first.
    Elsewhere,
}

#[derive(Default)]
struct Inputs {
    /// Cleared when the worker hands the block to validation, which has its own deadline.
    release_deadline: Option<Instant>,
    /// Set once local verification has finished and replacement cannot abandon verifier work.
    replaceable_parent: Option<block::Hash>,
    body: Option<Bytes>,
    header: Option<Arc<Header>>,
    /// When the solved header arrived, for the release latency metric.
    solved_at: Option<std::time::Instant>,
    confirmation: Option<Confirmation>,
    /// Set by the worker once it has bound the solved header to the proposal.
    expected_confirmation: Option<Confirmation>,
}

#[derive(Clone)]
enum Update {
    Prepare(Bytes),
    Solve(Arc<Header>),
    Confirm(Confirmation),
}

impl Inputs {
    /// The parent of a proposal that can be replaced without losing a solve or confirmation.
    fn idle_parent(&self) -> Option<block::Hash> {
        self.replaceable_parent
            .filter(|_| self.header.is_none() && self.confirmation.is_none())
    }

    fn expired(&self) -> bool {
        self.release_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Stores an input. Returns whether it is new: a replay of the stored input changes nothing.
    fn update(&mut self, update: Update) -> std::result::Result<bool, JobError> {
        if self.expired() {
            return Err(JobError::Expired);
        }
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
        match update {
            Update::Prepare(body) => set(&mut self.body, body),
            Update::Solve(header) => {
                let new = set(&mut self.header, header)?;
                if new {
                    self.solved_at = Some(std::time::Instant::now());
                }
                Ok(new)
            }
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

enum Registration {
    Handled,
    Start(JobSender, JobReceiver, Instant),
    /// A complete decode is required before a new prepare can displace another job.
    CheckReplacement(Bytes),
}

/// An ended job's remembered outcome.
struct Ended {
    id: JobId,
    outcome: std::result::Result<(), JobError>,
    forget_at: Instant,
}

#[derive(Default)]
struct Registry {
    /// Unfinished jobs in admission order, each owned by one worker.
    live: IndexMap<JobId, JobSender>,
    /// Ended jobs, oldest first.
    ended: VecDeque<Ended>,
}

impl Registry {
    /// Only a new proposal can displace an idle proposal for the same parent.
    fn evict_for(&mut self, parent: block::Hash) -> bool {
        let victim = self.live.iter().find_map(|(id, sender)| {
            (sender.borrow().idle_parent() == Some(parent)).then_some(*id)
        });
        let Some(id) = victim else { return false };
        // Registration holds the registry lock, so a solve or confirmation cannot slip
        // between selection and removal. Wake the worker waiting for a solved header.
        self.live[&id].send_modify(|parts| parts.release_deadline = Some(Instant::now()));
        self.end(id, Err(JobError::Expired));
        metrics::counter!("rpc.trusted_submission.job.evicted").increment(1);
        true
    }

    /// Removes the live slot and records the result for late inputs.
    fn end(&mut self, id: JobId, outcome: std::result::Result<(), JobError>) {
        if self.live.shift_remove(&id).is_none() {
            return;
        }
        if self.ended.len() >= ENDED_CAPACITY {
            self.ended.pop_front();
        }
        self.ended.push_back(Ended {
            id,
            outcome,
            forget_at: Instant::now() + ENDED_RETENTION,
        });
    }
}

/// The listener's jobs: at most [`CAPACITY`] unfinished and [`ENDED_CAPACITY`] remembered.
#[derive(Clone, Default)]
pub(crate) struct Jobs(Arc<Mutex<Registry>>);

impl Jobs {
    fn lock(&self) -> MutexGuard<'_, Registry> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Adds an input to its job. The first input of an unknown ID starts the job: the caller
    /// receives its channel and deadline and must run exactly one worker for it.
    ///
    /// An input for an ended job reports that job's outcome and is otherwise ignored.
    fn register(
        &self,
        id: JobId,
        update: Update,
        decoded: Option<&DecodedProposal>,
    ) -> std::result::Result<Registration, JobError> {
        let now = Instant::now();
        let mut jobs = self.lock();
        while jobs
            .ended
            .front()
            .is_some_and(|ended| ended.forget_at <= now)
        {
            jobs.ended.pop_front();
        }
        if let Some(sender) = jobs.live.get(&id) {
            let mut result = Ok(false);
            sender.send_if_modified(|inputs| {
                result = inputs.update(update);
                matches!(result, Ok(true))
            });
            return result.map(|_| Registration::Handled);
        }
        if let Some(ended) = jobs.ended.iter().rev().find(|ended| ended.id == id) {
            return ended.outcome.map(|()| Registration::Handled);
        }
        if jobs.live.len() >= CAPACITY {
            if let Update::Prepare(body) = &update {
                if decoded.is_none()
                    && jobs
                        .live
                        .values()
                        .any(|job| job.borrow().idle_parent().is_some())
                {
                    return Ok(Registration::CheckReplacement(body.clone()));
                }
            }
            if !decoded
                .is_some_and(|proposal| jobs.evict_for(proposal.block.header.previous_block_hash))
            {
                metrics::counter!("rpc.trusted_submission.job.saturated").increment(1);
                return Err(JobError::Capacity);
            }
        }
        let deadline = now + LIFETIME;
        let mut inputs = Inputs {
            release_deadline: Some(deadline),
            ..Inputs::default()
        };
        inputs.update(update)?;
        let (sender, receiver) = watch::channel(inputs);
        jobs.live.insert(id, sender.clone());
        Ok(Registration::Start(sender, receiver, deadline))
    }

    /// Frees an unfinished job's slot and remembers its outcome.
    fn finish(&self, id: JobId, sender: &JobSender, outcome: std::result::Result<(), JobError>) {
        let mut jobs = self.lock();
        if jobs
            .live
            .get(&id)
            .is_some_and(|live| live.same_channel(sender))
        {
            jobs.end(id, outcome);
        }
    }
}

/// Ends a job when its worker stops, including by panic, so its slot cannot leak.
struct Slot {
    jobs: Jobs,
    sender: JobSender,
    id: JobId,
    outcome: std::result::Result<(), JobError>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.jobs.finish(self.id, &self.sender, self.outcome);
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
                // Only labels the verifier's error, which is discarded here.
                let tip_height = self
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
                            &tip_height,
                            &header.hash(),
                        )
                    {
                        return Err(JobError::Invalid);
                    }
                    Ok(Arc::new(header))
                })
                .await
                .map_err(blocking_task_failed)??;
                Update::Solve(header)
            }
            Operation::Confirm => {
                Update::Confirm(bytes.as_ref().try_into().map_err(|_| JobError::Invalid)?)
            }
        };
        let mut decoded = None;
        let (sender, receiver, deadline) = loop {
            match jobs.register(id, update.clone(), decoded.as_ref())? {
                Registration::Handled => return Ok(()),
                Registration::Start(sender, receiver, deadline) => {
                    break (sender, receiver, deadline)
                }
                Registration::CheckReplacement(body) => {
                    // The request holds a body permit. Decode outside the registry lock,
                    // then recheck the ID and eligible jobs before replacing anything.
                    decoded = Some(decode_proposal(body).await?);
                }
            }
        };
        let rpc = self.clone();
        let span = tracing::info_span!("trusted_job", id = %hex::encode(id));
        tokio::spawn(
            async move {
                let mut slot = Slot {
                    jobs,
                    sender: sender.clone(),
                    id,
                    // Replaced below unless the worker panics.
                    outcome: Err(JobError::Unavailable),
                };
                let result = rpc
                    .run_trusted_job(&sender, receiver, deadline, decoded)
                    .await;
                let label = match result {
                    Ok(Committed::Here) => "committed",
                    Ok(Committed::Elsewhere) => "duplicate",
                    Err(error) => error.label(),
                };
                metrics::counter!("rpc.trusted_submission.job.outcomes", "outcome" => label)
                    .increment(1);
                tracing::info!(outcome = label, "prepared trusted job ended");
                slot.outcome = result.map(|_| ());
            }
            .instrument(span),
        );
        Ok(())
    }

    /// Waits for the release within the job's lifetime, then relays and commits the block.
    ///
    /// The lifetime bounds only the wait. A release hands the block to the submission lifecycle,
    /// which has its own validation deadline and cannot be recalled.
    async fn run_trusted_job(
        &self,
        sender: &JobSender,
        inputs: JobReceiver,
        deadline: Instant,
        decoded: Option<DecodedProposal>,
    ) -> std::result::Result<Committed, JobError> {
        let release =
            tokio::time::timeout_at(deadline, self.await_release(sender, inputs, decoded))
                .await
                .unwrap_or(Err(JobError::Expired))?;
        if self.latest_chain_tip.best_tip_hash() != Some(release.parent) {
            return Err(JobError::Expired);
        }
        // Authorization and the timer can become ready on the same poll. Order the
        // handoff against incoming inputs under the same lock that admits them.
        if !sender.send_if_modified(|parts| {
            if parts.expired() {
                return false;
            }
            parts.release_deadline = None;
            true
        }) {
            return Err(JobError::Expired);
        }
        let hash = release.block.hash();
        if let Some(solved_at) = sender.borrow().solved_at {
            metrics::histogram!(
                "rpc.trusted_submission.job.release.duration_seconds",
                "authority" => release.authority.label()
            )
            .record(solved_at.elapsed().as_secs_f64());
        }
        tracing::info!(
            ?hash,
            height = ?release.height,
            authority = release.authority.label(),
            "releasing prepared trusted block"
        );
        match self
            .relay_trusted_block(
                release.block,
                release.height,
                release.wire.len(),
                std::time::Instant::now(),
                PeerBody::Prepared(release.wire),
            )
            .await
        {
            Ok(SubmitBlockResponse::Accepted) => Ok(Committed::Here),
            // The hash is committed, and the lifecycle has compared this body with that copy.
            Ok(SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Duplicate)) => {
                Ok(Committed::Elsewhere)
            }
            Ok(SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Rejected)) => {
                Err(JobError::Invalid)
            }
            Err(TrustedSubmissionError::Capacity) => Err(JobError::Capacity),
            Err(TrustedSubmissionError::EndOfSupport) => Err(JobError::EndOfSupport),
            _ => Err(JobError::Unavailable),
        }
    }

    /// Resolves when the job may relay its solved block: the proposal decodes canonically, its
    /// parent is the best tip, the solved header matches it, and either local verification passed
    /// or the primary's confirmation names the exact solved bytes.
    async fn await_release(
        &self,
        sender: &JobSender,
        mut inputs: JobReceiver,
        decoded: Option<DecodedProposal>,
    ) -> std::result::Result<Release, JobError> {
        let DecodedProposal {
            block: candidate,
            transactions,
            height,
        } = match decoded {
            Some(proposal) => proposal,
            None => {
                let body = inputs
                    .wait_for(|parts| parts.body.is_some())
                    .await
                    .map_err(|_| JobError::Expired)?
                    .body
                    .clone()
                    .expect("waited for the body");
                decode_proposal(body).await?
            }
        };
        let parent = candidate.header.previous_block_hash;
        let mut tip = self.latest_chain_tip.clone();
        parent_becomes_tip(&mut tip, parent, height).await?;
        let (block, wire, authority) = tokio::select! {
            biased;
            // Checked first: verification of a proposal whose parent stopped being the tip
            // fails for that reason, and the job is then stale, not invalid.
            () = tip_leaves(&mut tip, parent) => return Err(JobError::Expired),
            authorized = self.authorize(sender, inputs, &candidate, transactions) => authorized?,
        };
        Ok(Release {
            block,
            height,
            parent,
            wire,
            authority,
        })
    }

    /// Binds the solved header to `candidate` and waits for authority to relay the result.
    ///
    /// If confirmation authorizes relay first, the worker stops waiting for proposal verification.
    /// Checks already dispatched by the verifier can still run alongside verification of the solved block.
    async fn authorize(
        &self,
        sender: &JobSender,
        mut inputs: JobReceiver,
        candidate: &Arc<Block>,
        transactions: Bytes,
    ) -> std::result::Result<(Arc<Block>, Arc<[u8]>, Authority), JobError> {
        let request =
            self.gbt
                .block_verifier_router()
                .oneshot(zakura_consensus::Request::Prepare {
                    block: candidate.clone().into(),
                    source: zakura_consensus::PreparedCandidateSource::ClientProposal,
                });
        let verify = local_approval(request, || {
            sender.send_modify(|parts| {
                parts.replaceable_parent = Some(candidate.header.previous_block_hash);
            });
        });
        tokio::pin!(verify);
        let mut authority = None;
        let mut solve_inputs = inputs.clone();
        let solve = wait_for_solve(&mut solve_inputs);
        tokio::pin!(solve);
        let header = tokio::select! {
            biased;
            result = &mut verify => {
                result?;
                authority = Some(Authority::Verified);
                solve.await?
            }
            header = &mut solve => header?,
        };
        let solved = bind_header(candidate, header)?;
        let header = solved.header.clone();
        // The header has a fixed size for each network. Bound the transaction bytes we
        // copy and hash here so small solves can skip a second blocking-pool round trip.
        const INLINE_TRANSACTION_BYTES: usize = 4096;
        let small = transactions.len() <= INLINE_TRANSACTION_BYTES;
        let assemble = move || -> std::result::Result<_, JobError> {
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
            let mut confirmation = [0; CONFIRMATION_BYTES];
            confirmation[..32].copy_from_slice(&header.hash().0);
            confirmation[32..].copy_from_slice(&Sha256::digest(&wire));
            Ok((Arc::<[u8]>::from(wire), confirmation))
        };
        let (wire, confirmation) = if small {
            assemble()
        } else {
            tokio::task::spawn_blocking(assemble)
                .await
                .map_err(blocking_task_failed)?
        }?;
        sender.send_modify(|parts| {
            parts.expected_confirmation = Some(confirmation);
            // A confirmation that arrived before the solved bytes were known is kept only if
            // it names them.
            if parts
                .confirmation
                .is_some_and(|received| received != confirmation)
            {
                parts.confirmation = None;
            }
        });
        let authority = match authority {
            Some(authority) => authority,
            None => tokio::select! {
                biased;
                result = &mut verify => {
                    result?;
                    Authority::Verified
                }
                confirmed = inputs.wait_for(|parts| parts.confirmation == Some(confirmation)) => {
                    confirmed.map(|_| ()).map_err(|_| JobError::Expired)?;
                    Authority::Confirmed
                }
            },
        };
        Ok((solved, wire, authority))
    }
}

struct DecodedProposal {
    block: Arc<Block>,
    transactions: Bytes,
    height: Height,
}

/// Checks the complete wire encoding before a proposal can replace another job or be verified.
async fn decode_proposal(body: Bytes) -> std::result::Result<DecodedProposal, JobError> {
    tokio::task::spawn_blocking(move || {
        let mut remaining = body.as_ref();
        let block =
            Block::zcash_deserialize_bounded(&mut remaining).map_err(|_| JobError::Invalid)?;
        // Proposal verification and the peer must see exactly the same transaction encoding.
        if !remaining.is_empty()
            || block
                .zcash_serialize_to_vec()
                .map_err(|_| JobError::Invalid)?
                != body
        {
            return Err(JobError::Invalid);
        }
        let transactions = body.slice(block.header.zcash_serialized_size()..);
        let height = block.coinbase_height().ok_or(JobError::Invalid)?;
        Ok(DecodedProposal {
            block: Arc::new(block),
            transactions,
            height,
        })
    })
    .await
    .map_err(blocking_task_failed)?
}

/// Eviction wakes this wait even if no further input arrives for the job.
async fn wait_for_solve(inputs: &mut JobReceiver) -> std::result::Result<Arc<Header>, JobError> {
    inputs
        .wait_for(|parts| parts.header.is_some() || parts.expired())
        .await
        .map_err(|_| JobError::Expired)?
        .header
        .clone()
        .ok_or(JobError::Expired)
}

/// Waits until `parent` is the best tip, so a miner can prepare a candidate while this node is
/// still committing its parent. A tip at or above the candidate's `height` makes the job stale.
async fn parent_becomes_tip(
    tip: &mut impl ChainTip,
    parent: block::Hash,
    height: Height,
) -> std::result::Result<(), JobError> {
    loop {
        match tip.best_tip_height_and_hash() {
            Some((_, hash)) if hash == parent => return Ok(()),
            Some((tip_height, _)) if tip_height >= height => return Err(JobError::Expired),
            _ => {}
        }
        tip.best_tip_changed()
            .await
            .map_err(|_| JobError::Unavailable)?;
    }
}

/// Resolves once the best tip is no longer `parent`.
async fn tip_leaves(tip: &mut impl ChainTip, parent: block::Hash) {
    while tip.best_tip_hash() == Some(parent) {
        if tip.best_tip_changed().await.is_err() {
            // The tip can no longer change, so only the job's deadline ends the wait.
            return std::future::pending().await;
        }
    }
}

/// A verification that fails without proving the proposal invalid gives no local verdict.
/// Keep the primary confirmation eligible until the job deadline. An actual rejection vetoes
/// early relay.
async fn local_approval(
    verify: impl std::future::Future<Output = std::result::Result<block::Hash, tower::BoxError>>,
    finished: impl FnOnce(),
) -> std::result::Result<(), JobError> {
    match verify.await {
        Ok(_) => {
            finished();
            Ok(())
        }
        Err(error) if is_proposal_rejection(&error) => {
            tracing::info!(%error, "local verification rejected the prepared proposal");
            Err(JobError::Invalid)
        }
        Err(error) => {
            finished();
            metrics::counter!("rpc.trusted_submission.job.no_local_verdict").increment(1);
            tracing::info!(%error, "local proposal verification reached no verdict; awaiting primary confirmation");
            std::future::pending().await
        }
    }
}

/// Whether proposal verification proved the proposal invalid, as opposed to failing to reach
/// a verdict. The proposal's contextual check reports a failed state call, an uncommitted
/// parent and a parent that is no longer the best tip as errors too; none of them is a verdict.
fn is_proposal_rejection(error: &tower::BoxError) -> bool {
    matches!(
        error.downcast_ref::<RouterError>(),
        Some(RouterError::Block { source }) if source.rejects_template()
    )
}

/// Reports a blocking task that panicked or was cancelled. The job cannot reach an outcome.
fn blocking_task_failed(error: tokio::task::JoinError) -> JobError {
    tracing::error!(%error, "prepared trusted job task failed");
    JobError::Unavailable
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
        assert!(local_approval(unavailable, || {}).now_or_never().is_none());
        let authorized = std::future::ready(Ok(block::Hash([0; 32])));
        assert_eq!(local_approval(authorized, || {}).await, Ok(()));
    }

    /// Proposal verification reports state failures and a moved tip through the variant that
    /// also carries contextual rejections. Only a rejection vetoes the confirmation path.
    #[tokio::test]
    async fn only_a_verdict_on_the_proposal_vetoes_release() {
        use futures::FutureExt;
        use zakura_consensus::{BlockError, VerifyBlockError};

        let hash = block::Hash([0; 32]);
        let error =
            |error: VerifyBlockError| -> tower::BoxError { RouterError::from(error).into() };
        let no_verdict = [
            error(VerifyBlockError::ValidateProposal(
                "proposal is not based on the current best chain tip".into(),
            )),
            error(VerifyBlockError::ValidateProposal(
                "proposal state service is unavailable".into(),
            )),
            error(VerifyBlockError::Depth {
                source: "state lookup failed".into(),
                hash,
            }),
            // A contextual error that does not prove this proposal invalid.
            error(VerifyBlockError::ValidateProposal(Box::new(
                zakura_state::ValidateContextError::InvalidAncestorBlock(hash),
            ))),
        ];
        for error in no_verdict {
            assert!(!is_proposal_rejection(&error), "{error}");
            assert!(local_approval(std::future::ready(Err(error)), || {})
                .now_or_never()
                .is_none());
        }
        let rejections = [
            error(VerifyBlockError::from(BlockError::MissingHeight(hash))),
            error(VerifyBlockError::from(BlockError::NoTransactions)),
        ];
        for error in rejections {
            assert!(is_proposal_rejection(&error), "{error}");
            assert_eq!(
                local_approval(std::future::ready(Err(error)), || {}).await,
                Err(JobError::Invalid)
            );
        }
    }

    #[test]
    fn job_slots_accept_replays_and_bound_unfinished_jobs() {
        let jobs = Jobs::default();
        let body = Bytes::from_static(b"first");
        for id in 0..CAPACITY {
            let id = [u8::try_from(id).unwrap(); 16];
            assert!(matches!(
                jobs.register(id, Update::Prepare(body.clone()), None),
                Ok(Registration::Start(..))
            ));
        }
        assert!(matches!(
            jobs.register([0xff; 16], Update::Prepare(body.clone()), None),
            Err(JobError::Capacity)
        ));
        assert!(matches!(
            jobs.register([0; 16], Update::Prepare(body), None),
            Ok(Registration::Handled)
        ));
        assert!(matches!(
            jobs.register(
                [0; 16],
                Update::Prepare(Bytes::from_static(b"second")),
                None
            ),
            Err(JobError::Conflict)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn eviction_wakes_the_worker_and_its_cleanup_cannot_end_a_reused_id() {
        use futures::FutureExt;

        let jobs = Jobs::default();
        let id = [0; 16];
        let block = zakura_chain::block::genesis::regtest_genesis_block();
        let proposal = decode_proposal(block.zcash_serialize_to_vec().unwrap().into())
            .await
            .unwrap();
        let parent = proposal.block.header.previous_block_hash;
        let prepare = || Update::Prepare(Bytes::from_static(b"stored proposal"));
        let Registration::Start(old, mut inputs, _) = jobs.register(id, prepare(), None).unwrap()
        else {
            panic!("new ID starts a worker")
        };
        old.send_modify(|parts| parts.replaceable_parent = Some(parent));
        let wait = wait_for_solve(&mut inputs);
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        for n in 1..CAPACITY {
            jobs.register([u8::try_from(n).unwrap(); 16], prepare(), None)
                .unwrap();
        }
        jobs.register([255; 16], prepare(), Some(&proposal))
            .unwrap();
        assert_eq!(wait.now_or_never(), Some(Err(JobError::Expired)));
        tokio::time::advance(ENDED_RETENTION).await;
        jobs.lock().end([255; 16], Err(JobError::Expired));
        let Registration::Start(new, _, _) = jobs.register(id, prepare(), None).unwrap() else {
            panic!("new ID starts a worker")
        };
        jobs.finish(id, &old, Err(JobError::Expired));
        assert!(jobs.lock().live[&id].same_channel(&new));
    }

    #[tokio::test(start_paused = true)]
    async fn input_at_the_deadline_is_refused_before_the_worker_processes_expiry() {
        let jobs = Jobs::default();
        let input = || Update::Confirm([0; 64]);
        let Registration::Start(_, _, deadline) = jobs.register([0; 16], input(), None).unwrap()
        else {
            panic!("new ID starts a worker")
        };
        tokio::time::sleep_until(deadline).await;
        assert!(matches!(
            jobs.register([0; 16], input(), None),
            Err(JobError::Expired)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn ended_job_frees_its_slot_and_answers_late_inputs_until_forgotten() {
        let jobs = Jobs::default();
        let input = || Update::Prepare(Bytes::from_static(b"body"));
        for id in 0..CAPACITY {
            let id = [u8::try_from(id).unwrap(); 16];
            assert!(matches!(
                jobs.register(id, input(), None),
                Ok(Registration::Start(..))
            ));
        }
        jobs.lock().end([0; 16], Err(JobError::Expired));
        jobs.lock().end([1; 16], Ok(()));
        // Ending twice, as a worker that both reports and unwinds would, changes nothing.
        jobs.lock().end([1; 16], Err(JobError::Unavailable));
        assert_eq!(jobs.lock().live.len(), CAPACITY - 2);
        assert_eq!(jobs.lock().ended.len(), 2);

        // Any input for an ended job reports its outcome and starts nothing.
        assert!(matches!(
            jobs.register([0; 16], Update::Confirm([0; 64]), None),
            Err(JobError::Expired)
        ));
        assert!(matches!(
            jobs.register([1; 16], Update::Confirm([0; 64]), None),
            Ok(Registration::Handled)
        ));
        assert_eq!(jobs.lock().live.len(), CAPACITY - 2);

        tokio::time::advance(ENDED_RETENTION).await;
        assert!(
            matches!(
                jobs.register([0; 16], input(), None),
                Ok(Registration::Start(..))
            ),
            "a forgotten ID starts a new job"
        );
        assert!(jobs.lock().ended.is_empty());
    }

    #[test]
    fn remembered_outcomes_are_bounded_and_drop_the_oldest() {
        let jobs = Jobs::default();
        for id in 0..=ENDED_CAPACITY {
            let id = [u8::try_from(id).unwrap(); 16];
            assert!(matches!(
                jobs.register(id, Update::Confirm([0; 64]), None),
                Ok(Registration::Start(..))
            ));
            jobs.lock().end(id, Err(JobError::Invalid));
        }
        let registry = jobs.lock();
        assert!(registry.live.is_empty());
        assert_eq!(registry.ended.len(), ENDED_CAPACITY);
        assert!(registry.ended.iter().all(|ended| ended.id != [0; 16]));
    }

    #[test]
    fn worker_that_unwinds_still_ends_its_job() {
        let jobs = Jobs::default();
        assert!(matches!(
            jobs.register([5; 16], Update::Confirm([0; 64]), None),
            Ok(Registration::Start(..))
        ));
        let slot = Slot {
            jobs: jobs.clone(),
            sender: jobs.lock().live[&[5; 16]].clone(),
            id: [5; 16],
            outcome: Err(JobError::Unavailable),
        };
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _slot = slot;
            panic!("the worker panicked");
        }));
        assert!(unwound.is_err());
        assert!(jobs.lock().live.is_empty());
        assert!(matches!(
            jobs.register([5; 16], Update::Confirm([0; 64]), None),
            Err(JobError::Unavailable)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn parent_wait_ends_when_the_parent_is_the_tip_or_the_candidate_is_stale() {
        use futures::FutureExt;
        use zakura_chain::chain_tip::mock::MockChainTip;

        let parent = block::Hash([1; 32]);
        let other = block::Hash([2; 32]);
        let (mut tip, sender) = MockChainTip::new();
        {
            let wait = parent_becomes_tip(&mut tip, parent, Height(5));
            tokio::pin!(wait);
            assert!(wait.as_mut().now_or_never().is_none(), "no tip yet");
            // A tip below the parent's height: the parent can still arrive.
            sender.send_best_tip_hash(other);
            sender.send_best_tip_height(Height(3));
            assert!(wait.as_mut().now_or_never().is_none());
            // A competing block at the parent's height: the parent can still win.
            sender.send_best_tip_height(Height(4));
            assert!(wait.as_mut().now_or_never().is_none());
            sender.send_best_tip_hash(parent);
            assert_eq!(wait.await, Ok(()));
        }
        // The tip reached the candidate's own height on another block.
        sender.send_best_tip_hash(other);
        sender.send_best_tip_height(Height(5));
        assert_eq!(
            parent_becomes_tip(&mut tip, parent, Height(5)).await,
            Err(JobError::Expired)
        );
        drop(sender);
        assert_eq!(
            parent_becomes_tip(&mut tip, parent, Height(9)).await,
            Err(JobError::Unavailable),
            "a closed tip channel cannot deliver the parent"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tip_watch_resolves_only_when_the_tip_moves_off_the_parent() {
        use futures::FutureExt;
        use zakura_chain::chain_tip::mock::MockChainTip;

        let parent = block::Hash([1; 32]);
        let (mut tip, sender) = MockChainTip::new();
        sender.send_best_tip_hash(parent);
        sender.send_best_tip_height(Height(4));
        let left = tip_leaves(&mut tip, parent);
        tokio::pin!(left);
        assert!(left.as_mut().now_or_never().is_none());
        // A change that keeps the parent as the tip is not a departure.
        sender.send_best_tip_height(Height(4));
        assert!(left.as_mut().now_or_never().is_none());
        sender.send_best_tip_hash(block::Hash([2; 32]));
        left.await;
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
