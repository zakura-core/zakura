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
    /// Runtime block-work funding and retained request/receipt owners.
    WorkFunding(WorkFundingStep),
    /// Decoded allocations retained by block, header, and transaction owners.
    DecodedRetention(DecodedRetentionStep),
    /// Header reactor scheduling and locator policy, with its independent model.
    HeaderReactor(HeaderReactorStep),
}

impl MachineStep {
    /// The failure that stopped the run at this operation, if any.
    pub fn failure(&self) -> Option<&MachineFailure> {
        match self {
            Self::Work(step) => step.failure.as_ref(),
            Self::WorkFunding(step) => step.failure.as_ref(),
            Self::DecodedRetention(step) => step.failure.as_ref(),
            Self::HeaderReactor(step) => step.failure.as_ref(),
        }
    }
}

/// Decoder-v1 evidence for the decoded-block runtime owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecodedRetentionStep {
    /// Original causal bytes, including a short final operation.
    pub input: Vec<u8>,
    /// Whether this boundary follows explicit handle drain.
    pub drain: bool,
    /// Independent allocation reads and actual pool counters.
    pub actual: Option<zakura_node_services::block_memory::model::RetentionView>,
    /// The independent model's retained handle graph and expected pool charges.
    pub expected: zakura_node_services::block_memory::model::RetentionView,
    /// Unavailable capture or a driver panic, never a property detection.
    pub failure: Option<MachineFailure>,
}

impl From<zakura_node_services::block_memory::model::RetentionStep> for DecodedRetentionStep {
    fn from(value: zakura_node_services::block_memory::model::RetentionStep) -> Self {
        Self {
            input: value.input,
            drain: value.drain,
            actual: value.actual,
            expected: value.expected,
            failure: value.capture_error.map(|message| MachineFailure {
                property_failure: false,
                phase: FailurePhase::Invariant,
                message,
            }),
        }
    }
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
        Self {
            input: value.input,
            actual: value.actual.into(),
            expected: value.expected.into(),
            failure: value.failure.map(Into::into),
        }
    }
}

/// Retained request phase at the actual runtime boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FundingRequestPhase {
    /// Provisional work without funding.
    Taken,
    /// Funding acquired before publication.
    Admitted,
    /// Published request awaiting its writer.
    Queued,
    /// Writer authorized to send.
    Started,
    /// Writer completed the request.
    Written,
    /// Request retired before writer authorization.
    Skipped,
    /// No request owner remains.
    Dropped,
}

/// Actual or independently predicted request retention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingRequest {
    /// Exact owner, including authority generations.
    pub owner: Option<WorkOwner>,
    /// Heights retained by the request.
    pub heights: Vec<u32>,
    /// Runtime lifetime phase.
    pub phase: FundingRequestPhase,
    /// Number of strong owners.
    pub copies: usize,
    /// Reservation retained before publication.
    pub unpublished_bytes: u64,
    /// Whether the session token was cancelled.
    pub cancelled: bool,
}

/// A receipt retains funding independently of subsequent queue generations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingReceipt {
    /// Exact owner whose charge moved into the receipt.
    pub owner: WorkOwner,
    /// Retained reservation, or zero after drop.
    pub bytes: u64,
}

/// Resource reads or independent model predictions at a runtime boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkFundingView {
    /// Logical queue contents and authority.
    pub queue: WorkView,
    /// Queue-owned pooled reservation.
    pub funded: u64,
    /// Actual shared-budget charge.
    pub reserved: u64,
    /// Capacity available to admission.
    pub available: u64,
    /// Retained funding displaced by explicit corruption injection.
    pub quarantined: u64,
    /// Whether accounting permanently closed admission.
    pub closed: bool,
    /// Request owners, including retired generations.
    pub requests: Vec<FundingRequest>,
    /// Receipts retained independently of the queue.
    pub receipts: Vec<FundingReceipt>,
}

/// One funding-decoder-v1 operation or the explicit pre-destruction drain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkFundingStep {
    /// Complete causal operation bytes.
    pub input: Vec<u8>,
    /// Whether the driver drains its owners before aggregate destruction.
    pub drain: bool,
    /// Independent reads of actual runtime resources.
    pub actual: WorkFundingView,
    /// Independent model predictions.
    pub expected: WorkFundingView,
    /// First transition or invariant failure.
    pub failure: Option<MachineFailure>,
}

impl From<zakura_network::zakura::WorkFundingView> for WorkFundingView {
    fn from(value: zakura_network::zakura::WorkFundingView) -> Self {
        use zakura_network::zakura::FundingRequestPhase as Phase;
        Self {
            queue: value.queue.into(),
            funded: value.funded,
            reserved: value.reserved,
            available: value.available,
            quarantined: value.quarantined,
            closed: value.closed,
            requests: value
                .requests
                .into_iter()
                .map(|request| FundingRequest {
                    owner: request.owner.map(crate::storage::block_owner),
                    heights: request.heights,
                    phase: match request.phase {
                        Phase::Taken => FundingRequestPhase::Taken,
                        Phase::Admitted => FundingRequestPhase::Admitted,
                        Phase::Queued => FundingRequestPhase::Queued,
                        Phase::Started => FundingRequestPhase::Started,
                        Phase::Written => FundingRequestPhase::Written,
                        Phase::Skipped => FundingRequestPhase::Skipped,
                        Phase::Dropped => FundingRequestPhase::Dropped,
                    },
                    copies: request.copies,
                    unpublished_bytes: request.unpublished_bytes,
                    cancelled: request.cancelled,
                })
                .collect(),
            receipts: value
                .receipts
                .into_iter()
                .map(|receipt| FundingReceipt {
                    owner: crate::storage::block_owner(receipt.owner),
                    bytes: receipt.bytes,
                })
                .collect(),
        }
    }
}

impl From<zakura_network::zakura::WorkFundingStep> for WorkFundingStep {
    fn from(value: zakura_network::zakura::WorkFundingStep) -> Self {
        Self {
            input: value.input,
            drain: value.drain,
            actual: value.actual.into(),
            expected: value.expected.into(),
            failure: value.failure.map(Into::into),
        }
    }
}

impl From<zakura_network::zakura::WorkOwnershipFailure> for MachineFailure {
    fn from(value: zakura_network::zakura::WorkOwnershipFailure) -> Self {
        use zakura_network::zakura::WorkOwnershipPhase;
        Self {
            property_failure: value.property_failure,
            phase: match value.phase {
                WorkOwnershipPhase::Transition => FailurePhase::Transition,
                WorkOwnershipPhase::Invariant => FailurePhase::Invariant,
            },
            message: value.message,
        }
    }
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
