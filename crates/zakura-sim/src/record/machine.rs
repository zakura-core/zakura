//! Semantic evidence from transport-free machine drivers.

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{BookStep, DiscoverySessionStep, DriverFailure};

/// A machine harness's scenario.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineScenario {
    /// Complete causal machine input, independent of scheduling choices.
    pub input: Vec<u8>,
}

/// Typed semantic evidence from one transport-free machine operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "step",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum MachineStep {
    /// The real discovery book, its independent model, and its signed samples.
    DiscoveryBook(BookStep),
    /// Discovery's real session and hint owner and its independent model.
    DiscoveryState(DiscoverySessionStep),
    /// A fixture machine's operation. It tests machine-trace support itself, as
    /// [`crate::harness::fixtures`] tests the simulator. No production machine
    /// records it.
    Fixture(FixtureStep),
}

impl MachineStep {
    /// The failure that stopped the run at this operation, if any.
    pub fn failure(&self) -> Option<MachineFailure> {
        match self {
            Self::DiscoveryBook(step) => step.failure.clone().map(Into::into),
            Self::DiscoveryState(step) => step.failure.clone().map(Into::into),
            Self::Fixture(step) => step.failure.clone(),
        }
    }
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

impl From<DriverFailure> for MachineFailure {
    /// Discovery's drivers catch an operation and its model comparison together,
    /// so each failure reports one phase.
    fn from(failure: DriverFailure) -> Self {
        Self {
            property_failure: failure.property_failure,
            phase: FailurePhase::Invariant,
            message: failure.message,
        }
    }
}

/// One operation of a fixture machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureStep {
    /// Original causal bytes.
    pub input: Vec<u8>,
    /// First failure, if this operation stopped the run.
    pub failure: Option<MachineFailure>,
}
