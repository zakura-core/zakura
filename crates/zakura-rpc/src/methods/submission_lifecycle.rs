//! Detached ownership of mined-block verification, temporary serving, and early relay.

use futures::future::BoxFuture;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tower::{BoxError, ServiceExt};
use zakura_chain::block::{self, Block, Height};
use zakura_consensus::{router::service_trait::BlockVerifierService, RouterError};
use zakura_node_services::{block_memory::BlockHandle, bpv1_proposer::Bpv1ProposerHook};
use zakura_state::{BlockAdmission, ReadRequest, ReadResponse, ReadState as ReadStateService};

use super::types::submit_block::{
    EarlyBlockRelay, EarlyMinedBlock, MinedBlockSubmission, MinedSubmission,
    PendingBlockRegistration, PendingBlockRegistry, PendingBlockSignal,
};

/// How long a trusted submission waits for verifier readiness and its verdict.
///
/// A trusted block's parent is already committed, so nothing on this path waits for other
/// blocks to arrive, unlike the syncer's much longer verify timeout. The deadline only guards
/// against a stalled verifier or state service. It stays under the 75-second target block
/// spacing: a block that validates more slowly has lost its relay value, and until then its
/// body keeps a pending-registry entry, a receipt slot, and the HTTP response.
const TRUSTED_VERIFY_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a duplicate verdict waits for the committed copy of its block to become readable.
///
/// State answers a duplicate as soon as another copy is in its write channel, before that write
/// finishes, so the first read usually misses. The other copy has already passed semantic
/// verification and only needs its contextual checks and write, which normally take well under
/// a second. Waiting holds the RPC response and one pending-registry entry, and keeps the
/// submitted body's sends permitted, so the bound stays short: if the other write is rejected,
/// the body is withdrawn after at most this long.
pub(super) const COMMITTED_BLOCK_WAIT: Duration = Duration::from_secs(5);

/// The interval between committed-block reads while waiting.
const COMMITTED_BLOCK_RETRY: Duration = Duration::from_millis(50);

/// Waits for the committed block with a given hash, or returns `None` at its deadline.
pub(super) type CommittedBlockLookup =
    Box<dyn FnOnce(block::Hash) -> BoxFuture<'static, Option<BlockHandle>> + Send + Sync>;

/// Reads a committed block from any active chain, as peer block serving does, retrying until it
/// appears or [`COMMITTED_BLOCK_WAIT`] passes.
pub(super) fn committed_block_lookup<R: ReadStateService>(read_state: R) -> CommittedBlockLookup {
    Box::new(move |hash| {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + COMMITTED_BLOCK_WAIT;
            loop {
                let read = read_state
                    .clone()
                    .oneshot(ReadRequest::AnyChainBlock(hash.into()));
                if let Ok(Ok(ReadResponse::Block(Some(block)))) =
                    tokio::time::timeout_at(deadline, read).await
                {
                    return Some(block);
                }
                if tokio::time::Instant::now() + COMMITTED_BLOCK_RETRY >= deadline {
                    return None;
                }
                tokio::time::sleep(COMMITTED_BLOCK_RETRY).await;
            }
        })
    })
}

/// The authority and registration timing for a submission's early announcement.
pub(super) enum RelayPolicy {
    AfterStateAdmission {
        pending_blocks: PendingBlockRegistry,
        optimistic_block_inventory: bool,
        /// Publishes the block over BPv1 once, when state admission authorizes relay or, if it
        /// never does, when the block commits.
        bpv1_proposer: Option<Arc<dyn Bpv1ProposerHook>>,
    },
    TrustedSubmission {
        registration: PendingBlockRegistration,
        /// Early relay is limited to new blocks whose parent is committed. A block withdrawn for
        /// an unknown parent can later become valid under the same hash, and peers that requested
        /// it in between ignore this node's announcements of that hash for up to two minutes. A
        /// hash another path is already committing is announced by that path.
        ///
        /// A complete POST establishes both by state lookups. A prepared job establishes them
        /// by requiring the parent to be the best tip, which does not exclude a copy still in
        /// the write queue; the duplicate verdict below settles that case.
        early_relay: Option<crate::TrustedBlockSource>,
        /// A duplicate verdict proves only that the header hash is known, not that the submitted
        /// transactions match the committed block, so it is resolved against the committed body.
        committed_block: CommittedBlockLookup,
        /// Publishes the block over BPv1 once it commits. A trusted submission skips state
        /// admission, and its early relay precedes verification, so nothing publishes earlier.
        bpv1_proposer: Option<Arc<dyn Bpv1ProposerHook>>,
    },
}

impl RelayPolicy {
    /// Ordinary verification retains its existing service deadlines; trusted verification also
    /// bounds readiness and completion together. Expiry cannot undo a handed-off state operation.
    fn verification_deadline(&self) -> Option<Duration> {
        match self {
            Self::AfterStateAdmission { .. } => None,
            Self::TrustedSubmission { .. } => Some(TRUSTED_VERIFY_TIMEOUT),
        }
    }

    /// Waits until the block may relay before its commit: state admission authorized it, or
    /// the trusted submitter's parent check did. Returns how it relays.
    async fn wait_for_relay(
        &self,
        admission: &BlockAdmission,
        started: Instant,
    ) -> Option<EarlyBlockRelay> {
        match self {
            Self::AfterStateAdmission { .. } => {
                let admitted = admission.wait().await;
                metrics::histogram!("mining.state_admission.duration_seconds")
                    .record(started.elapsed().as_secs_f64());
                (admitted && admission.optimistic_relay_authorized())
                    .then(|| EarlyBlockRelay::StateAdmission(admission.clone()))
            }
            Self::TrustedSubmission { early_relay, .. } => {
                early_relay.map(EarlyBlockRelay::TrustedSubmission)
            }
        }
    }

    /// Whether a block authorized to relay is announced to peers before its commit.
    fn announces_early(&self) -> bool {
        match self {
            Self::AfterStateAdmission {
                optimistic_block_inventory,
                ..
            } => *optimistic_block_inventory,
            Self::TrustedSubmission { .. } => true,
        }
    }

    fn publish_bpv1(&self, block: &Arc<Block>, hash: block::Hash, height: Height, stage: &str) {
        let (Self::AfterStateAdmission {
            bpv1_proposer: Some(bpv1),
            ..
        }
        | Self::TrustedSubmission {
            bpv1_proposer: Some(bpv1),
            ..
        }) = self
        else {
            return;
        };
        let published = bpv1.publish_solved_block(block.clone(), height);
        tracing::debug!(?hash, ?height, published, stage, "bpv1 solved-block hook");
    }

    fn retire_bpv1(&self, hash: block::Hash) {
        if let Self::AfterStateAdmission {
            bpv1_proposer: Some(bpv1),
            ..
        } = self
        {
            bpv1.retire_failed_block(hash);
            tracing::debug!(?hash, "bpv1 retired a block whose commit failed");
        }
    }
}

/// A decoded submission whose entry point has already checked height and reserved capacity.
pub(super) struct Submission {
    pub block: Arc<Block>,
    pub hash: block::Hash,
    pub height: Height,
    pub size: usize,
    pub submitted_at: Instant,
    pub method: MinedBlockSubmission,
    pub reservation: MinedSubmission,
}

pub(super) enum VerificationOutcome {
    Completed(Result<block::Hash, BoxError>),
    TimedOut,
}

pub(super) struct SubmissionOutcome {
    pub verification: VerificationOutcome,
    pub relay: RelayAttempt,
    /// Set when a trusted submission's verification reported a duplicate.
    pub duplicate_body: Option<DuplicateBody>,
}

/// How a duplicate trusted body compares with the block this node already committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuplicateBody {
    /// Equal to the committed block, so peers receive the same serialized bytes from either.
    /// Temporary sends stay permitted.
    Identical,
    /// Different transactions under the committed header hash. Temporary sends are withdrawn.
    Differs,
    /// The committed block did not become readable within [`COMMITTED_BLOCK_WAIT`].
    /// Temporary sends are withdrawn, because an unconfirmed body must not reach peers.
    Unconfirmed,
}

impl DuplicateBody {
    pub fn label(self) -> &'static str {
        match self {
            Self::Identical => "identical",
            Self::Differs => "differs",
            Self::Unconfirmed => "unconfirmed",
        }
    }
}

fn is_duplicate(result: &Result<block::Hash, BoxError>) -> bool {
    result.as_ref().is_err_and(|error| {
        error
            .downcast_ref::<RouterError>()
            .is_some_and(RouterError::is_duplicate_request)
    })
}

pub(super) struct RelayAttempt {
    pub requested: bool,
    signal: Option<PendingBlockSignal>,
}

impl RelayAttempt {
    /// Read at the caller's metric boundary: dispatch can race verification completion.
    pub fn was_dispatched(&self) -> bool {
        self.signal
            .as_ref()
            .is_some_and(PendingBlockSignal::was_dispatched)
    }
}

impl Submission {
    /// Owns verification and cleanup independently of the caller's wait. `finish` runs inside
    /// the detached task so response-specific metrics survive an HTTP disconnect too.
    pub fn spawn<V, F, T>(
        self,
        mut verifier: V,
        sender: mpsc::UnboundedSender<EarlyMinedBlock>,
        policy: RelayPolicy,
        finish: F,
    ) -> JoinHandle<T>
    where
        V: BlockVerifierService,
        F: FnOnce(SubmissionOutcome) -> T + Send + 'static,
        T: Send + 'static,
    {
        tokio::spawn(async move {
            let _reservation = self.reservation;
            let admission = BlockAdmission::pending();
            let request = zakura_consensus::Request::CommitMined {
                block: self.block.clone().into(),
                size: self.size,
                admission: admission.clone(),
            };
            let deadline = policy.verification_deadline();
            let verification = async move {
                let verify = async move { verifier.ready().await?.call(request).await };
                match deadline {
                    Some(deadline) => match tokio::time::timeout(deadline, verify).await {
                        Ok(result) => VerificationOutcome::Completed(result),
                        Err(_) => VerificationOutcome::TimedOut,
                    },
                    None => VerificationOutcome::Completed(verify.await),
                }
            };
            tokio::pin!(verification);
            let admission_start = Instant::now();
            let mut admitted_registration = None;
            let mut relay_signal = None;
            let mut relay_requested = false;
            let mut bpv1_attempted = false;
            let result = tokio::select! {
                biased;
                // A completed verification supersedes the opportunity to relay early.
                result = &mut verification => result,
                authorized = policy.wait_for_relay(&admission, admission_start) => {
                    if let Some(relay) = authorized {
                        // Only state admission publishes before commit: trusted early relay
                        // precedes verification.
                        let publish_early = matches!(relay, EarlyBlockRelay::StateAdmission(_));
                        if policy.announces_early() {
                            let registration = match &policy {
                                RelayPolicy::AfterStateAdmission { pending_blocks, .. } => {
                                    admitted_registration = pending_blocks.insert_state_admitted(self.block.clone());
                                    admitted_registration.as_ref()
                                }
                                RelayPolicy::TrustedSubmission { registration, .. } => {
                                    Some(registration)
                                }
                            };
                            if let Some(registration) = registration {
                                let pending = registration.signal();
                                relay_signal = Some(pending.clone());
                                relay_requested = sender.send(EarlyMinedBlock {
                                    relay,
                                    hash: self.hash,
                                    height: self.height,
                                    size: self.size,
                                    submitted_at: self.submitted_at,
                                    submission: self.method,
                                    pending,
                                }).is_ok();
                            }
                            // Ordinary submission withdraws immediately if no consumer accepted the
                            // event. Trusted submission retains its pre-reserved body until validation.
                            if !relay_requested {
                                admitted_registration.take();
                            }
                        }
                        // BPv1 serializes and hashes the block before it publishes, so it
                        // runs after the early announcement is queued.
                        if publish_early {
                            policy.publish_bpv1(&self.block, self.hash, self.height, "admitted");
                            bpv1_attempted = true;
                        }
                    }
                    verification.await
                }
            };
            if !bpv1_attempted && matches!(result, VerificationOutcome::Completed(Ok(_))) {
                policy.publish_bpv1(&self.block, self.hash, self.height, "committed");
            }
            // BPv1 published at admission, but the commit failed. A duplicate verdict means
            // the block is already in state, so BPv1 keeps relaying it.
            let commit_failed = matches!(
                &result,
                VerificationOutcome::Completed(verified)
                    if verified.is_err() && !is_duplicate(verified)
            );
            if bpv1_attempted && commit_failed {
                policy.retire_bpv1(self.hash);
            }
            let (registration, duplicate_body) = match policy {
                RelayPolicy::AfterStateAdmission { .. } => (admitted_registration, None),
                RelayPolicy::TrustedSubmission {
                    registration,
                    committed_block,
                    ..
                } => {
                    let duplicate_body = match &result {
                        VerificationOutcome::Completed(verified) if is_duplicate(verified) => {
                            Some(match committed_block(self.hash).await {
                                Some(committed)
                                    if committed == BlockHandle::from(self.block.clone()) =>
                                {
                                    DuplicateBody::Identical
                                }
                                Some(_) => DuplicateBody::Differs,
                                None => DuplicateBody::Unconfirmed,
                            })
                        }
                        _ => None,
                    };
                    (Some(registration), duplicate_body)
                }
            };
            if let Some(registration) = registration {
                let committed = matches!(result, VerificationOutcome::Completed(Ok(_)))
                    || duplicate_body == Some(DuplicateBody::Identical);
                registration.resolve(if committed { Ok(self.block) } else { Err(()) });
            }
            finish(SubmissionOutcome {
                verification: result,
                relay: RelayAttempt {
                    requested: relay_requested,
                    signal: relay_signal,
                },
                duplicate_body,
            })
        })
    }
}
