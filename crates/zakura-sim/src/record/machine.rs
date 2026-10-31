//! Semantic evidence from transport-free machine drivers.

use serde::{Deserialize, Serialize};

/// Typed semantic evidence from one transport-free machine operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "step",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum MachineStep {
    /// Header reactor scheduling and locator policy, with its independent model.
    HeaderReactor(HeaderReactorStep),
}

impl MachineStep {
    /// The failure that stopped the run at this operation, if any.
    pub fn failure(&self) -> Option<&MachineFailure> {
        match self {
            Self::HeaderReactor(step) => step.failure.as_ref(),
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

/// One storage call's exact identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallRef {
    /// The call's number.
    pub number: u64,
    /// What the call asks storage for.
    pub kind: CallKind,
}

/// What a storage call asks storage for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CallKind {
    /// A continuation-locator read.
    Locator,
    /// A VCT repair context read.
    VctRepairContext,
    /// A target preparation.
    Prepare,
    /// A prepared target's application.
    Apply,
}

impl From<zakura_network::zakura::observe::StorageCallKind> for CallKind {
    fn from(kind: zakura_network::zakura::observe::StorageCallKind) -> Self {
        use zakura_network::zakura::observe::StorageCallKind as Kind;
        match kind {
            Kind::Locator => Self::Locator,
            Kind::VctRepairContext => Self::VctRepairContext,
            Kind::Prepare => Self::Prepare,
            Kind::Apply => Self::Apply,
        }
    }
}

/// One admitted session in a reactor state view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReactorSessionView {
    /// The peer's model index.
    pub peer: usize,
    /// The selected tip of the peer's last status, in hex.
    pub status: Option<String>,
    /// Whether the status is spent.
    pub spent: bool,
}

/// One pending locator read in a reactor state view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReactorReadView {
    /// The peer's model index.
    pub peer: usize,
    /// The read's exact call.
    pub call: CallRef,
}

/// A reactor state view, read from the machine or derived by its model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderReactorView {
    /// Admitted sessions, by peer.
    pub sessions: Vec<ReactorSessionView>,
    /// Pending locator reads, by peer.
    pub reads: Vec<ReactorReadView>,
    /// Accepted storage calls that have not settled, by number.
    pub calls: Vec<CallRef>,
    /// The scheduling revision.
    pub revision: u64,
    /// Repair supplier order, front first.
    pub suppliers: Vec<usize>,
    /// Whether a resource stall pauses ordinary discovery.
    pub paused: bool,
    /// The current repair: its generation and height.
    pub repair: Option<(u64, u32)>,
    /// Whether a changed cached-pass key waits for its budget.
    pub cached_deferred: bool,
    /// Milliseconds from the run's clock origin to the next deadline. A deferred
    /// cached pass adds a refill the model bounds rather than predicts, so
    /// then this holds `None` on both sides.
    pub next_deadline_millis: Option<u64>,
}

/// One decoder-v1 header reactor operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderReactorStep {
    /// Original causal bytes, including a short final operation.
    pub input: Vec<u8>,
    /// State read from the production pure owner.
    pub actual: HeaderReactorView,
    /// State derived independently by the model.
    pub expected: HeaderReactorView,
    /// First failure, if this operation stopped the run.
    pub failure: Option<MachineFailure>,
}

impl From<zakura_network::zakura::HeaderMachineFailure> for MachineFailure {
    fn from(value: zakura_network::zakura::HeaderMachineFailure) -> Self {
        use zakura_network::zakura::HeaderMachinePhase as Phase;
        Self {
            property_failure: value.property_failure,
            phase: match value.phase {
                Phase::Transition => FailurePhase::Transition,
                Phase::Invariant => FailurePhase::Invariant,
            },
            message: value.message,
        }
    }
}

impl From<zakura_network::zakura::HeaderReactorView> for HeaderReactorView {
    fn from(value: zakura_network::zakura::HeaderReactorView) -> Self {
        Self {
            sessions: value
                .sessions
                .into_iter()
                .map(|session| ReactorSessionView {
                    peer: session.peer,
                    status: session.status.map(|hash| hash.to_string()),
                    spent: session.spent,
                })
                .collect(),
            reads: value
                .reads
                .into_iter()
                .map(|read| ReactorReadView {
                    peer: read.peer,
                    call: CallRef {
                        number: read.call.0,
                        kind: read.call.1.into(),
                    },
                })
                .collect(),
            calls: value
                .calls
                .into_iter()
                .map(|(number, kind)| CallRef {
                    number,
                    kind: kind.into(),
                })
                .collect(),
            revision: value.revision,
            suppliers: value.suppliers,
            paused: value.paused,
            repair: value.repair,
            cached_deferred: value.cached_deferred,
            next_deadline_millis: value.next_deadline_millis,
        }
    }
}

impl From<zakura_network::zakura::HeaderReactorStep> for HeaderReactorStep {
    fn from(value: zakura_network::zakura::HeaderReactorStep) -> Self {
        Self {
            input: value.input,
            actual: value.actual.into(),
            expected: value.expected.into(),
            failure: value.failure.map(Into::into),
        }
    }
}
