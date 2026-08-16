//! Transition effects, adapter receipts, and related publication status.

use zakura_chain::block;

use crate::{BranchId, EvidenceId, HeaderSyncWorkOwner, StateVersion};

/// How ordinary header work related to newer monotone finality.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderWorkEffect {
    /// The planner admitted ordinary header work after a durable monotone-finality rebase.
    Rebased,
    /// Monotone finality already consumed the complete prepared range.
    AlreadyApplied,
}

/// Finality effects produced while settling one transition.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FinalityEffect {
    /// Checkpoint body growth advanced integrated finality on the retained selected path.
    Checkpoint,
    /// Headers-only depth finality occurred in the same insertion/reselection.
    HeadersOnlyDepth,
    /// Integrated full-state finality advanced from an explicit finality event.
    FullState,
}

/// Auxiliary-delivery effects produced while settling one transition.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AuxiliaryEffect {
    /// Full state authenticated or rejected auxiliary metadata without changing the DAG.
    Authentication,
}

/// Orthogonal effects produced by one planned transition.
///
/// The submitted [`crate::TransitionDomain`] identifies the input. This record describes
/// the resulting admission transformations and side effects, which may coexist.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TransitionEffect {
    /// Ordinary header-work rebase or already-applied classification.
    pub header_work: Option<HeaderWorkEffect>,
    /// Optional finality advancement produced while settling.
    pub finality: Option<FinalityEffect>,
    /// Optional auxiliary authentication/rejection effect.
    pub auxiliary: Option<AuxiliaryEffect>,
    /// True when retention limits refused the event and only an alarm may change.
    pub resource_stalled: bool,
}

impl TransitionEffect {
    /// Construct an empty effect record for an ordinary event admission.
    pub const fn none() -> Self {
        Self {
            header_work: None,
            finality: None,
            auxiliary: None,
            resource_stalled: false,
        }
    }

    /// Construct an exact adjacent-replay or zero-effect ordinary admission.
    pub const fn event() -> Self {
        Self::none()
    }

    /// Construct an already-applied header-work effect.
    pub const fn header_work_already_applied() -> Self {
        Self {
            header_work: Some(HeaderWorkEffect::AlreadyApplied),
            finality: None,
            auxiliary: None,
            resource_stalled: false,
        }
    }

    /// Construct a committed resource-stall refusal.
    pub const fn resource_stalled() -> Self {
        Self {
            header_work: None,
            finality: None,
            auxiliary: None,
            resource_stalled: true,
        }
    }

    /// True when retention refused admission.
    pub const fn is_resource_stalled(self) -> bool {
        self.resource_stalled
    }

    /// True when checkpoint finality advanced on the retained selected path.
    pub const fn is_checkpoint_finality(self) -> bool {
        matches!(self.finality, Some(FinalityEffect::Checkpoint))
    }

    /// True when the plan only authenticated or rejected auxiliary deliveries.
    pub const fn is_aux_authentication(self) -> bool {
        matches!(self.auxiliary, Some(AuxiliaryEffect::Authentication))
            && self.header_work.is_none()
            && self.finality.is_none()
            && !self.resource_stalled
    }

    /// True when ordinary header work was rebased onto newer finality.
    pub const fn is_header_work_rebased(self) -> bool {
        matches!(self.header_work, Some(HeaderWorkEffect::Rebased))
    }

    /// True when monotone finality already consumed the prepared range.
    pub const fn is_header_work_already_applied(self) -> bool {
        matches!(self.header_work, Some(HeaderWorkEffect::AlreadyApplied))
    }

    /// True when headers-only depth finality appended in this transition.
    pub const fn is_headers_only_finality(self) -> bool {
        matches!(self.finality, Some(FinalityEffect::HeadersOnlyDepth))
    }
}

/// Work that the coordinator must retire before scheduling new forward work.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetiredWork {
    /// The header generation changed.
    /// The change makes all old forward owners stale.
    pub header_generation_changed: bool,
    /// The verified generation changed.
    /// The change makes all old body-forward owners stale.
    pub verified_generation_changed: bool,
    /// Exact owners retired for narrower causes.
    pub owners: Vec<HeaderSyncWorkOwner>,
}

impl RetiredWork {
    /// Derive generation-retirement signals from the published snapshot pair.
    ///
    /// This is the authoritative mapping from commit frontiers to ownership
    /// retirement. Call sites that only need generation deltas should use this
    /// instead of hand-comparing fields. Exact owner lists are still filled by
    /// the coordinator when narrower causes apply.
    pub fn from_snapshots(before: &crate::EngineSnapshot, after: &crate::EngineSnapshot) -> Self {
        Self {
            header_generation_changed: before.header_generation != after.header_generation,
            verified_generation_changed: before.verified_generation != after.verified_generation,
            owners: Vec::new(),
        }
    }

    /// Attach exact owners retired for narrower causes than generation change.
    pub fn with_owners(mut self, owners: Vec<HeaderSyncWorkOwner>) -> Self {
        self.owners = owners;
        self
    }
}

/// Successful admission that produced no durable effects.
///
/// This covers exact adjacent replay of the most recent state-changing
/// transition, already-applied rebased header work, immediately evicted
/// insertions, and other zero-effect admissions. It is not a general
/// historical replay ledger.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NoChangeReceipt {
    /// Unchanged durable version.
    pub state_version: StateVersion,
    /// Submitted event identity when the event carries an idempotency key.
    pub idempotency_key: Option<EvidenceId>,
}

/// Stale version/branch/owner result with guaranteed zero effects.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct StaleReceipt {
    /// Current durable version the caller must reload.
    pub current_version: StateVersion,
    /// Exact stale branch when the event is branch-sensitive.
    pub branch: Option<BranchId>,
}

/// A resource refusal whose alarm state the adapter has already committed durably.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CommittedStallReceipt {
    /// Durable version after recording or retaining the resource-stall alarm.
    pub state_version: StateVersion,
    /// True when this refusal changed and published the alarm state.
    pub alarm_changed: bool,
    /// Exact attempted branch when the refused event was branch-sensitive.
    pub attempted_branch: Option<BranchId>,
}

/// Serialized transition outcome returned by the state adapter after planning.
///
/// # Mapping from planner results
///
/// | Planner result | Adapter outcome |
/// | --- | --- |
/// | verified plan with durable mutation | [`Self::Committed`] |
/// | verified no-change plan (`is_no_change`) | [`Self::NoChange`] |
/// | verified plan with `effect.resource_stalled` | [`Self::ResourceStalled`] (alarm may commit) |
/// | [`crate::TransitionFailure::Stale`] | [`Self::Stale`] (zero durable effects) |
/// | any other [`crate::TransitionFailure`] | adapter error / refuse; zero durable effects |
///
/// # Stall / limit three-way distinction
///
/// These must not be collapsed:
///
/// 1. **[`crate::TransitionFailure::AuxiliaryLimitExceeded`]** — planner refuses
///    before any durable mutation; no resource-stall alarm.
/// 2. **Verified `resource_stalled` → [`Self::ResourceStalled`]** — retention could
///    not enforce limits without breaking protected paths; the durable
///    resource-stall alarm may be recorded or retained.
/// 3. **[`crate::InvariantViolation::Limits`]** (via
///    [`crate::TransitionFailure::Invariant`]) — commit-time verification found a
///    projected graph above frozen limits; planning fails closed with zero
///    effects (not a stall receipt).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    /// State adapter durably committed.
    Committed,
    /// Admission produced no durable effects.
    NoChange(NoChangeReceipt),
    /// Ownership/version was stale before effects.
    Stale(StaleReceipt),
    /// The planner refused admission after it durably recorded or retained the resource alarm.
    ResourceStalled(CommittedStallReceipt),
}

/// Dependency-neutral VCT metadata repair status published by the state writer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct VctRootRepairStatus {
    /// Current repair need.
    pub state: VctRootRepairState,
    /// Monotonic replacement-attempt generation.
    pub generation: u64,
}

impl Default for VctRootRepairStatus {
    fn default() -> Self {
        Self {
            state: VctRootRepairState::Idle,
            generation: 0,
        }
    }
}

/// Exact VCT metadata repair need, independent of state/network service types.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VctRootRepairState {
    /// No VCT metadata repair is currently required.
    Idle,
    /// The finalized writer needs a replacement delivery for one exact height.
    Unavailable {
        /// Height whose selected-header metadata is unavailable or rejected.
        height: block::Height,
    },
}
