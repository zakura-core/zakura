//! Process-local sync lifecycle values shared across state, network, and node orchestration.

use std::fmt;
use std::sync::Arc;

use zakura_header_chain::Frontier;

/// Checked monotonic identity for one lifecycle generation.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LifecycleEpoch(u64);

impl LifecycleEpoch {
    /// The first lifecycle generation.
    pub const INITIAL: Self = Self(0);

    /// Construct an epoch from a durable or test value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the numeric epoch for metrics and serialization boundaries.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advance without wrapping.
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// Authoritative process-local owner of bulk block applies.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ApplyPhase {
    /// Legacy checkpoint sync owns applies before semantic handoff.
    LegacyBootstrap {
        /// Bootstrap ownership generation.
        epoch: LifecycleEpoch,
    },
    /// Native Zakura block sync may begin new applies.
    Native {
        /// Native ownership generation.
        epoch: LifecycleEpoch,
    },
    /// The coordinator closes native admission while previously authorized applies drain.
    FallbackDraining {
        /// Native generation that the coordinator drains.
        epoch: LifecycleEpoch,
    },
    /// One fully drained legacy fallback lease owns applies.
    LegacyFallback {
        /// Drained generation owned by the fallback lease.
        epoch: LifecycleEpoch,
    },
    /// The coordinator permanently rejects new bulk applies.
    Failed {
        /// Last valid generation before failure.
        epoch: LifecycleEpoch,
    },
}

impl ApplyPhase {
    /// Return this phase's monotonic generation.
    pub const fn epoch(self) -> LifecycleEpoch {
        match self {
            Self::LegacyBootstrap { epoch }
            | Self::Native { epoch }
            | Self::FallbackDraining { epoch }
            | Self::LegacyFallback { epoch }
            | Self::Failed { epoch } => epoch,
        }
    }

    /// Return a stable metric/log label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::LegacyBootstrap { .. } => "legacy_bootstrap",
            Self::Native { .. } => "native",
            Self::FallbackDraining { .. } => "fallback_draining",
            Self::LegacyFallback { .. } => "legacy_fallback",
            Self::Failed { .. } => "failed",
        }
    }

    /// Apply one checked lifecycle transition without mutating the caller on failure.
    pub fn transition(self, transition: ApplyTransition) -> Result<Self, LifecycleTransitionError> {
        match transition {
            ApplyTransition::FinishBootstrap => match self {
                Self::LegacyBootstrap { epoch } => Ok(Self::Native {
                    epoch: epoch
                        .checked_next()
                        .ok_or(LifecycleTransitionError::EpochExhausted)?,
                }),
                _ => Err(LifecycleTransitionError::IllegalPhase),
            },
            ApplyTransition::BeginFallback { expected_epoch } => {
                check_epoch(self, expected_epoch)?;
                match self {
                    Self::Native { epoch } => Ok(Self::FallbackDraining { epoch }),
                    _ => Err(LifecycleTransitionError::IllegalPhase),
                }
            }
            ApplyTransition::ActivateFallback { expected_epoch } => {
                check_epoch(self, expected_epoch)?;
                match self {
                    Self::FallbackDraining { epoch } => Ok(Self::LegacyFallback { epoch }),
                    _ => Err(LifecycleTransitionError::IllegalPhase),
                }
            }
            ApplyTransition::ResumeNative { expected_epoch } => {
                check_epoch(self, expected_epoch)?;
                match self {
                    Self::FallbackDraining { epoch } | Self::LegacyFallback { epoch } => {
                        Ok(Self::Native {
                            epoch: epoch
                                .checked_next()
                                .ok_or(LifecycleTransitionError::EpochExhausted)?,
                        })
                    }
                    _ => Err(LifecycleTransitionError::IllegalPhase),
                }
            }
            ApplyTransition::Fail { expected_epoch } => {
                check_epoch(self, expected_epoch)?;
                match self {
                    Self::Failed { .. } => Err(LifecycleTransitionError::IllegalPhase),
                    _ => Ok(Self::Failed {
                        epoch: self.epoch(),
                    }),
                }
            }
        }
    }
}

fn check_epoch(
    phase: ApplyPhase,
    expected: LifecycleEpoch,
) -> Result<(), LifecycleTransitionError> {
    let current = phase.epoch();
    if current != expected {
        return Err(LifecycleTransitionError::StaleEpoch { expected, current });
    }
    Ok(())
}

/// Requested apply-lifecycle edge.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ApplyTransition {
    /// Complete checkpoint semantic handoff.
    FinishBootstrap,
    /// Stop new native applies and begin draining one native epoch.
    BeginFallback {
        /// Exact native generation to drain.
        expected_epoch: LifecycleEpoch,
    },
    /// Authorize legacy fallback after the same epoch fully drains.
    ActivateFallback {
        /// Exact drained generation to authorize.
        expected_epoch: LifecycleEpoch,
    },
    /// Drop/cancel the exact fallback lease and advance native ownership.
    ResumeNative {
        /// Exact fallback generation that the transition releases.
        expected_epoch: LifecycleEpoch,
    },
    /// Fail the exact active generation closed while retaining its epoch for diagnosis.
    Fail {
        /// Exact generation that encountered the failure.
        expected_epoch: LifecycleEpoch,
    },
}

/// Rejected lifecycle transition.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum LifecycleTransitionError {
    /// The current phase rejects the requested edge.
    IllegalPhase,
    /// The caller issued the request for an obsolete lifecycle generation.
    StaleEpoch {
        /// Requested generation.
        expected: LifecycleEpoch,
        /// Current generation.
        current: LifecycleEpoch,
    },
    /// Advancing the monotonic counter would wrap.
    EpochExhausted,
}

impl fmt::Display for LifecycleTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IllegalPhase => formatter.write_str("illegal sync lifecycle transition"),
            Self::StaleEpoch { expected, current } => write!(
                formatter,
                "stale sync lifecycle epoch {} (current {})",
                expected.get(),
                current.get()
            ),
            Self::EpochExhausted => formatter.write_str("sync lifecycle epoch exhausted"),
        }
    }
}

impl std::error::Error for LifecycleTransitionError {}

/// Coarse startup stage reported while attaching the durable header runtime.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderReconstructionStage {
    /// Audit authoritative rows and derive any reconstructible repairs.
    StartupAudit,
    /// Reconcile the durable header runtime with authenticated full-state frontiers.
    FullStateReconciliation,
}

/// Bounded progress facts for one header-runtime attachment attempt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderReconstructionProgress {
    /// Current reconstruction stage.
    pub stage: HeaderReconstructionStage,
    /// Completed work units within the stage.
    pub completed: u64,
    /// Known total work units, or `None` before the audit determines it.
    pub total: Option<u64>,
    /// Fixed canonical finalized target for this attachment attempt.
    pub target: Option<Frontier>,
    /// Last canonical frontier durably committed with restart progress.
    pub last_committed: Option<Frontier>,
}

/// Condition that keeps the node from attaching the durable header runtime.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderRuntimeDetachedReason {
    /// Fresh state must first receive checkpoint-verified blocks through semantic sync.
    AwaitingSemanticHandoff,
    /// Durable header state exists and the state worker still owes runtime attachment.
    AttachmentPending,
}

impl HeaderReconstructionProgress {
    /// Initial progress before the startup audit has enumerated work.
    pub const STARTING: Self = Self {
        stage: HeaderReconstructionStage::StartupAudit,
        completed: 0,
        total: None,
        target: None,
        last_committed: None,
    };
}

/// Authoritative attachment/readiness state of the durable header runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderRuntimeStatus {
    /// Checkpoint semantic handoff has not started runtime attachment.
    Detached {
        /// Last attachment generation.
        epoch: LifecycleEpoch,
        /// Explicit startup condition that determines whether callers may bootstrap.
        reason: HeaderRuntimeDetachedReason,
    },
    /// One attachment generation audits or reconciles durable state.
    Reconstructing {
        /// Exact attachment generation.
        epoch: LifecycleEpoch,
        /// Latest bounded progress report.
        progress: HeaderReconstructionProgress,
    },
    /// The coherent reader and committed snapshot publisher are running.
    Ready {
        /// Ready attachment generation.
        epoch: LifecycleEpoch,
    },
    /// Attachment failed closed with its root-cause message.
    ///
    /// The producer exited the state write worker.
    /// The producer exit makes this state process-terminal.
    /// Recovery reopens and audits durable state during a node restart.
    Failed {
        /// Failed attachment generation.
        epoch: LifecycleEpoch,
        /// Stable root-cause text propagated from state startup.
        error: Arc<str>,
    },
}

impl HeaderRuntimeStatus {
    /// Return this status's monotonic attachment generation.
    pub const fn epoch(&self) -> LifecycleEpoch {
        match self {
            Self::Detached { epoch, .. }
            | Self::Reconstructing { epoch, .. }
            | Self::Ready { epoch }
            | Self::Failed { epoch, .. } => *epoch,
        }
    }

    /// Whether the coherent runtime is ready for header service use.
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    /// Apply one checked runtime transition without changing the caller on failure.
    pub fn transition(
        self,
        transition: HeaderRuntimeTransition,
    ) -> Result<Self, LifecycleTransitionError> {
        match transition {
            HeaderRuntimeTransition::BeginReconstruction => match self {
                Self::Detached { epoch, .. } => Ok(Self::Reconstructing {
                    epoch: epoch
                        .checked_next()
                        .ok_or(LifecycleTransitionError::EpochExhausted)?,
                    progress: HeaderReconstructionProgress::STARTING,
                }),
                _ => Err(LifecycleTransitionError::IllegalPhase),
            },
            HeaderRuntimeTransition::ReportProgress {
                expected_epoch,
                progress,
            } => {
                check_header_epoch(&self, expected_epoch)?;
                match self {
                    Self::Reconstructing { epoch, .. } => {
                        Ok(Self::Reconstructing { epoch, progress })
                    }
                    _ => Err(LifecycleTransitionError::IllegalPhase),
                }
            }
            HeaderRuntimeTransition::Ready { expected_epoch } => {
                check_header_epoch(&self, expected_epoch)?;
                match self {
                    Self::Reconstructing { epoch, .. } => Ok(Self::Ready { epoch }),
                    _ => Err(LifecycleTransitionError::IllegalPhase),
                }
            }
            HeaderRuntimeTransition::Fail {
                expected_epoch,
                error,
            } => {
                check_header_epoch(&self, expected_epoch)?;
                match self {
                    Self::Detached { epoch, .. } | Self::Reconstructing { epoch, .. } => {
                        Ok(Self::Failed { epoch, error })
                    }
                    _ => Err(LifecycleTransitionError::IllegalPhase),
                }
            }
        }
    }
}

fn check_header_epoch(
    status: &HeaderRuntimeStatus,
    expected: LifecycleEpoch,
) -> Result<(), LifecycleTransitionError> {
    let current = status.epoch();
    if current != expected {
        return Err(LifecycleTransitionError::StaleEpoch { expected, current });
    }
    Ok(())
}

/// Requested header-runtime lifecycle edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderRuntimeTransition {
    /// Start one checked attachment/reconstruction generation.
    BeginReconstruction,
    /// Replace progress only for the exact active generation.
    ReportProgress {
        /// Active attachment generation.
        expected_epoch: LifecycleEpoch,
        /// New bounded progress facts.
        progress: HeaderReconstructionProgress,
    },
    /// Publish coherent readiness for the exact reconstructed generation.
    Ready {
        /// Active attachment generation.
        expected_epoch: LifecycleEpoch,
    },
    /// Fail the exact detached or reconstructing generation closed after its producer exits.
    Fail {
        /// Generation that encountered the failure.
        expected_epoch: LifecycleEpoch,
        /// Root-cause text from state attachment.
        error: Arc<str>,
    },
}

/// Coordinator-owned header ordered-service demand.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderServiceDemand {
    /// Do not advertise or open header-sync sessions before runtime readiness.
    Disabled {
        /// Latest observed header-runtime generation.
        runtime_epoch: LifecycleEpoch,
    },
    /// Advertise and schedule header sync exactly once for this readiness generation.
    Enabled {
        /// Monotonic capability/readiness generation.
        capability_epoch: LifecycleEpoch,
    },
}

impl HeaderServiceDemand {
    /// Whether header capability advertisement and ordered sessions are authorized.
    pub const fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled { .. })
    }

    /// Return the capability generation when header demand is enabled.
    pub const fn capability_epoch(self) -> Option<LifecycleEpoch> {
        match self {
            Self::Disabled { .. } => None,
            Self::Enabled { capability_epoch } => Some(capability_epoch),
        }
    }
}

/// Coordinator-owned block ordered-service demand.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockServiceDemand {
    /// Keep body serving live while legacy/bootstrap owns bulk application.
    ServingOnly {
        /// Exact apply generation represented by this demand.
        apply_epoch: LifecycleEpoch,
    },
    /// Keep body serving live and admit native body application.
    ServingAndApplying {
        /// Exact native apply generation represented by this demand.
        apply_epoch: LifecycleEpoch,
    },
}

impl BlockServiceDemand {
    /// Whether native body application is authorized in this demand generation.
    pub const fn is_applying(self) -> bool {
        matches!(self, Self::ServingAndApplying { .. })
    }

    /// Return the exact apply generation represented by this demand.
    pub const fn apply_epoch(self) -> LifecycleEpoch {
        match self {
            Self::ServingOnly { apply_epoch } | Self::ServingAndApplying { apply_epoch } => {
                apply_epoch
            }
        }
    }
}

/// One coordinator publication consumed by capability and ordered-service scheduling.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SyncServiceDemand {
    /// Header capability/session demand.
    pub header: HeaderServiceDemand,
    /// Block serving/apply demand.
    pub block: BlockServiceDemand,
}

impl SyncServiceDemand {
    /// Derive the complete transport demand from authoritative lifecycle phases.
    pub fn from_phases(apply: ApplyPhase, header: &HeaderRuntimeStatus) -> Self {
        let header = match header {
            HeaderRuntimeStatus::Ready { epoch } => HeaderServiceDemand::Enabled {
                capability_epoch: *epoch,
            },
            _ => HeaderServiceDemand::Disabled {
                runtime_epoch: header.epoch(),
            },
        };
        let block = match apply {
            ApplyPhase::Native { epoch } => {
                BlockServiceDemand::ServingAndApplying { apply_epoch: epoch }
            }
            _ => BlockServiceDemand::ServingOnly {
                apply_epoch: apply.epoch(),
            },
        };
        Self { header, block }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_phase_model_accepts_only_declared_edges() {
        let zero = LifecycleEpoch::INITIAL;
        let one = zero
            .checked_next()
            .expect("the initial epoch has a successor");
        let phases = [
            ApplyPhase::LegacyBootstrap { epoch: zero },
            ApplyPhase::Native { epoch: one },
            ApplyPhase::FallbackDraining { epoch: one },
            ApplyPhase::LegacyFallback { epoch: one },
            ApplyPhase::Failed { epoch: one },
        ];
        let transitions = [
            ApplyTransition::FinishBootstrap,
            ApplyTransition::BeginFallback {
                expected_epoch: one,
            },
            ApplyTransition::ActivateFallback {
                expected_epoch: one,
            },
            ApplyTransition::ResumeNative {
                expected_epoch: one,
            },
            ApplyTransition::Fail {
                expected_epoch: one,
            },
        ];

        for phase in phases {
            for transition in transitions {
                let transition = match transition {
                    ApplyTransition::Fail { .. } => ApplyTransition::Fail {
                        expected_epoch: phase.epoch(),
                    },
                    transition => transition,
                };
                let result = phase.transition(transition);
                let legal = matches!(
                    (phase, transition),
                    (
                        ApplyPhase::LegacyBootstrap { .. },
                        ApplyTransition::FinishBootstrap | ApplyTransition::Fail { .. }
                    ) | (
                        ApplyPhase::Native { .. },
                        ApplyTransition::BeginFallback { .. } | ApplyTransition::Fail { .. }
                    ) | (
                        ApplyPhase::FallbackDraining { .. },
                        ApplyTransition::ActivateFallback { .. }
                            | ApplyTransition::ResumeNative { .. }
                            | ApplyTransition::Fail { .. }
                    ) | (
                        ApplyPhase::LegacyFallback { .. },
                        ApplyTransition::ResumeNative { .. } | ApplyTransition::Fail { .. }
                    )
                );
                assert_eq!(result.is_ok(), legal, "{phase:?} -> {transition:?}");
            }
        }
    }

    #[test]
    fn stale_and_exhausted_epochs_fail_without_wrapping() {
        let current = ApplyPhase::Native {
            epoch: LifecycleEpoch::new(7),
        };
        assert_eq!(
            current.transition(ApplyTransition::BeginFallback {
                expected_epoch: LifecycleEpoch::new(6),
            }),
            Err(LifecycleTransitionError::StaleEpoch {
                expected: LifecycleEpoch::new(6),
                current: LifecycleEpoch::new(7),
            })
        );

        let exhausted = ApplyPhase::LegacyFallback {
            epoch: LifecycleEpoch::new(u64::MAX),
        };
        assert_eq!(
            exhausted.transition(ApplyTransition::ResumeNative {
                expected_epoch: LifecycleEpoch::new(u64::MAX),
            }),
            Err(LifecycleTransitionError::EpochExhausted)
        );
    }

    #[test]
    fn stale_apply_failure_cannot_poison_a_new_epoch() {
        let stale_epoch = LifecycleEpoch::new(1);
        let current = ApplyPhase::Native { epoch: stale_epoch }
            .transition(ApplyTransition::BeginFallback {
                expected_epoch: stale_epoch,
            })
            .expect("the current native generation can begin fallback")
            .transition(ApplyTransition::ResumeNative {
                expected_epoch: stale_epoch,
            })
            .expect("resuming native advances the generation");

        assert_eq!(current.epoch(), LifecycleEpoch::new(2));
        assert_eq!(
            current.transition(ApplyTransition::Fail {
                expected_epoch: stale_epoch,
            }),
            Err(LifecycleTransitionError::StaleEpoch {
                expected: stale_epoch,
                current: LifecycleEpoch::new(2),
            })
        );
    }

    #[test]
    fn header_runtime_requires_one_explicit_reconstruction_epoch() {
        let detached = HeaderRuntimeStatus::Detached {
            epoch: LifecycleEpoch::INITIAL,
            reason: HeaderRuntimeDetachedReason::AttachmentPending,
        };
        let reconstructing = detached
            .clone()
            .transition(HeaderRuntimeTransition::BeginReconstruction)
            .expect("detached runtime can begin reconstruction");
        let epoch = reconstructing.epoch();
        assert!(matches!(
            reconstructing,
            HeaderRuntimeStatus::Reconstructing { .. }
        ));
        let ready = reconstructing
            .clone()
            .transition(HeaderRuntimeTransition::Ready {
                expected_epoch: epoch,
            })
            .expect("the active reconstruction can become ready");
        assert_eq!(ready, HeaderRuntimeStatus::Ready { epoch });
        assert_eq!(
            detached.transition(HeaderRuntimeTransition::Ready {
                expected_epoch: LifecycleEpoch::INITIAL,
            }),
            Err(LifecycleTransitionError::IllegalPhase)
        );
        assert!(matches!(
            reconstructing.transition(HeaderRuntimeTransition::Ready {
                expected_epoch: LifecycleEpoch::INITIAL,
            }),
            Err(LifecycleTransitionError::StaleEpoch { .. })
        ));
    }

    #[test]
    fn failed_header_runtime_is_process_terminal() {
        let reconstructing = HeaderRuntimeStatus::Detached {
            epoch: LifecycleEpoch::INITIAL,
            reason: HeaderRuntimeDetachedReason::AttachmentPending,
        }
        .transition(HeaderRuntimeTransition::BeginReconstruction)
        .expect("the attached state worker starts reconstruction");
        let epoch = reconstructing.epoch();
        let failed = reconstructing
            .transition(HeaderRuntimeTransition::Fail {
                expected_epoch: epoch,
                error: "state write worker exited".into(),
            })
            .expect("the failed state worker publishes its terminal status");

        assert_eq!(
            failed
                .clone()
                .transition(HeaderRuntimeTransition::BeginReconstruction),
            Err(LifecycleTransitionError::IllegalPhase)
        );
        assert_eq!(
            failed.transition(HeaderRuntimeTransition::Ready {
                expected_epoch: epoch,
            }),
            Err(LifecycleTransitionError::IllegalPhase)
        );
    }

    #[test]
    fn service_demand_keeps_serving_separate_from_native_applying() {
        let ready = HeaderRuntimeStatus::Ready {
            epoch: LifecycleEpoch::new(3),
        };
        let native = SyncServiceDemand::from_phases(
            ApplyPhase::Native {
                epoch: LifecycleEpoch::new(4),
            },
            &ready,
        );
        assert_eq!(
            native.header.capability_epoch(),
            Some(LifecycleEpoch::new(3))
        );
        assert!(native.block.is_applying());

        let fallback = SyncServiceDemand::from_phases(
            ApplyPhase::LegacyFallback {
                epoch: LifecycleEpoch::new(4),
            },
            &ready,
        );
        assert_eq!(fallback.header, native.header);
        assert!(!fallback.block.is_applying());
        assert_eq!(fallback.block.apply_epoch(), LifecycleEpoch::new(4));
    }
}
