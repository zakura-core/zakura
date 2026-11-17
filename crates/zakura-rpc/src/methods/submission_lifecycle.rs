//! Detached ownership of mined-block verification, temporary serving, and early relay.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tower::{BoxError, ServiceExt};
use zakura_chain::block::{self, Block, Height};
use zakura_consensus::router::service_trait::BlockVerifierService;
use zakura_state::BlockAdmission;

use super::types::submit_block::{
    EarlyBlockRelay, EarlyMinedBlock, MinedBlockSubmission, MinedSubmission,
    PendingBlockRegistration, PendingBlockRegistry, PendingBlockSignal,
};

const TRUSTED_VERIFY_TIMEOUT: Duration = Duration::from_secs(8 * 60);

/// The authority and registration timing for a submission's early announcement.
pub(super) enum RelayPolicy {
    AfterStateAdmission {
        pending_blocks: PendingBlockRegistry,
        optimistic_block_inventory: bool,
    },
    TrustedSubmission {
        registration: PendingBlockRegistration,
        /// Early relay is limited to blocks whose parent is committed. A block withdrawn for an
        /// unknown parent can later become valid under the same hash, and peers that requested
        /// it in between ignore this node's announcements of that hash for up to two minutes.
        parent_known: bool,
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

    async fn wait_for_relay(&self, admission: &BlockAdmission, started: Instant) -> bool {
        match self {
            Self::AfterStateAdmission {
                optimistic_block_inventory,
                ..
            } => {
                let admitted = admission.wait().await;
                metrics::histogram!("mining.state_admission.duration_seconds")
                    .record(started.elapsed().as_secs_f64());
                admitted && admission.optimistic_relay_authorized() && *optimistic_block_inventory
            }
            Self::TrustedSubmission { parent_known, .. } => *parent_known,
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
            let result = tokio::select! {
                biased;
                // A completed verification supersedes the opportunity to relay early.
                result = &mut verification => result,
                authorized = policy.wait_for_relay(&admission, admission_start) => {
                    if authorized {
                        let (relay, registration) = match &policy {
                            RelayPolicy::AfterStateAdmission { pending_blocks, .. } => {
                                admitted_registration = pending_blocks.insert(self.block.clone());
                                (EarlyBlockRelay::StateAdmission(admission), admitted_registration.as_ref())
                            }
                            RelayPolicy::TrustedSubmission { registration, .. } => {
                                (EarlyBlockRelay::TrustedSubmission, Some(registration))
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
                    verification.await
                }
            };
            let registration = match policy {
                RelayPolicy::AfterStateAdmission { .. } => admitted_registration,
                RelayPolicy::TrustedSubmission { registration, .. } => Some(registration),
            };
            if let Some(registration) = registration {
                registration.resolve(match &result {
                    VerificationOutcome::Completed(Ok(_)) => Ok(self.block),
                    _ => Err(()),
                });
            }
            finish(SubmissionOutcome {
                verification: result,
                relay: RelayAttempt {
                    requested: relay_requested,
                    signal: relay_signal,
                },
            })
        })
    }
}
