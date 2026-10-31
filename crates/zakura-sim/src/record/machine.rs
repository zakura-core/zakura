//! Semantic evidence from transport-free machine drivers.

use serde::{Deserialize, Serialize};

use crate::observe::{WorkOwner, WorkScope};

/// Typed semantic evidence from one transport-free machine operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "step",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum MachineStep {
    /// Logical block-work ownership and its independent model.
    Work(WorkStep),
}

/// Body-work authority, including every generation that controls eligibility.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BodyAuthority {
    /// Selected header branch and generation.
    pub header: WorkScope,
    /// Verified-body generation.
    pub verified_generation: u64,
    /// Selected-lineage epoch.
    pub body_work_epoch: u64,
}

/// One retained item, read from the owner or independently derived by its model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItem {
    /// Block height.
    pub height: u32,
    /// Whether the item occupies the in-flight map.
    pub in_flight: bool,
    /// Authority that selected the item.
    pub scope: BodyAuthority,
    /// Exact request owner, including its authority.
    pub owner: Option<WorkOwner>,
    /// Whether publication remains pending.
    pub provisional: bool,
    /// Estimated request bytes.
    pub estimate: u64,
    /// Retained charge.
    pub charge: u64,
}

/// A queue view at a completed or interrupted transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkView {
    /// Download floor.
    pub floor: u32,
    /// Current selection authority.
    pub authority: Option<BodyAuthority>,
    /// Entries sorted by height and location; duplicate heights remain visible.
    pub items: Vec<WorkItem>,
    /// Maintained total, or independently summed model charges.
    pub reserved: u64,
    /// Charges scanned from retained entries.
    pub scanned: u64,
    /// Charged bytes and item count above the floor.
    pub above_floor: (u64, u64),
}

/// The phase that failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailurePhase {
    /// Applying the input and checking its immediate outcome.
    Transition,
    /// Comparing the resulting owner with the independent model.
    Invariant,
}

/// Exact failure evidence. Driver panics cannot become property detections.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineFailure {
    /// Whether the independent checker named its property.
    pub property_failure: bool,
    /// Where execution stopped.
    pub phase: FailurePhase,
    /// Panic payload without the panic hook's source location.
    pub message: String,
}

/// One decoder-v1 block ownership operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkStep {
    /// Original causal bytes, including a short final operation.
    pub input: Vec<u8>,
    /// State read from the production pure owner.
    pub actual: WorkView,
    /// State derived independently by the model.
    pub expected: WorkView,
    /// First failure, if this operation stopped the run.
    pub failure: Option<MachineFailure>,
}

impl From<zakura_header_chain::BodyWorkAuthority> for BodyAuthority {
    fn from(value: zakura_header_chain::BodyWorkAuthority) -> Self {
        Self {
            header: crate::storage::scope(value.header),
            verified_generation: value.verified_generation.get(),
            body_work_epoch: value.body_work_epoch.get(),
        }
    }
}

impl From<zakura_network::zakura::WorkOwnershipView> for WorkView {
    fn from(value: zakura_network::zakura::WorkOwnershipView) -> Self {
        Self {
            floor: value.floor,
            authority: value.authority.map(Into::into),
            items: value
                .items
                .into_iter()
                .map(|item| WorkItem {
                    height: item.height,
                    in_flight: item.in_flight,
                    scope: item.scope.into(),
                    owner: item.owner.map(crate::storage::block_owner),
                    provisional: item.provisional,
                    estimate: item.estimate,
                    charge: item.charge,
                })
                .collect(),
            reserved: value.reserved,
            scanned: value.scanned,
            above_floor: value.above_floor,
        }
    }
}

impl From<zakura_network::zakura::WorkOwnershipStep> for WorkStep {
    fn from(value: zakura_network::zakura::WorkOwnershipStep) -> Self {
        use zakura_network::zakura::WorkOwnershipPhase;
        Self {
            input: value.input,
            actual: value.actual.into(),
            expected: value.expected.into(),
            failure: value.failure.map(|failure| MachineFailure {
                property_failure: failure.property_failure,
                phase: match failure.phase {
                    WorkOwnershipPhase::Transition => FailurePhase::Transition,
                    WorkOwnershipPhase::Invariant => FailurePhase::Invariant,
                },
                message: failure.message,
            }),
        }
    }
}
