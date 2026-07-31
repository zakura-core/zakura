//! Public state-service contracts for immutable retained header paths.

use std::sync::Arc;

use tokio::time::Instant;
use zakura_chain::block;
use zakura_header_chain::{AuxDelivery, Frontier, HeaderNode, SourceId, WorkScope};

pub use zakura_header_chain::{
    AlarmSet as HeaderChainAlarmSet, BodyUnavailableSummary as HeaderChainBodyUnavailableSummary,
    ChainScore as HeaderChainScore, EngineMode as HeaderChainMode,
    EngineSnapshot as HeaderChainSnapshot, Frontier as HeaderChainFrontier,
    FrontierSet as HeaderChainFrontierSet, HeaderGeneration as HeaderChainGeneration,
    StateVersion as HeaderChainStateVersion, SuffixWork as HeaderChainSuffixWork,
    VerifiedGeneration as HeaderChainVerifiedGeneration,
};

/// Maximum simultaneous retained target-path leases.
pub const MAX_RETAINED_PATH_LEASES: usize = zakura_header_chain::MAX_STAGED_TARGETS_V1;

/// Immutable state-owned snapshot of one exact target path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedPathLease {
    /// Monotonic process-local lease identity.
    pub lease_id: u64,
    /// Peer identity that owns the lease.
    pub peer: SourceId,
    /// Ordered-stream generation that owns the lease.
    pub session_id: u64,
    /// Exact retained target named by the request.
    pub target: Frontier,
    /// First requester-order locator intersection.
    pub common_ancestor: Frontier,
    /// Immutable hashes strictly after the ancestor through the target.
    pub path: Arc<[block::Hash]>,
    /// Exact generation and branch observed while the snapshot was acquired.
    pub scope: WorkScope,
    /// Bounded inactivity deadline.
    pub idle_deadline: Instant,
}

/// Result of attempting to acquire an exact retained target path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetainedPathLeaseOutcome {
    /// The exact snapshot was acquired.
    Acquired(Box<RetainedPathLease>),
    /// The target was absent when state took the coherent snapshot.
    TargetNotRetained,
    /// No locator hash lies on the exact target path.
    NoLocatorIntersection,
    /// The target path cannot reach retained history.
    HistoryPruned,
    /// A per-peer or global lease resource bound refused the request.
    Busy,
}

/// One hash-keyed lease page, independent of the current selected projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedPathPage {
    /// Exact lease identity used for the read.
    pub lease_id: u64,
    /// Exact page ancestor: the initial intersection or previous page tip.
    pub common_ancestor: Frontier,
    /// Exact target fixed when the lease was acquired.
    pub target: Frontier,
    /// Exact generation and branch fixed by the lease.
    pub scope: WorkScope,
    /// Hash-keyed nodes in path order.
    pub nodes: Vec<HeaderNode>,
    /// Hash-keyed auxiliary deliveries parallel to `nodes`.
    pub aux_deliveries: Vec<Vec<AuxDelivery>>,
    /// True when this page reaches the immutable target.
    pub complete: bool,
}

/// Result of reading or renewing an existing retained path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetainedPathReadOutcome {
    /// A bounded page was read and the lease deadline renewed.
    Page(Box<RetainedPathPage>),
    /// The lease is absent, expired, or no longer owned by this session.
    Unavailable,
}
