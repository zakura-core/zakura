//! Public state-service contracts for immutable retained header paths.

use std::sync::Arc;

use tokio::time::Instant;
use zakura_chain::block;
use zakura_header_chain::{AuxDelivery, Frontier, HeaderWorkAuthority, SourceId};

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

/// Opaque state-owned cursor for one exact canonical target path.
///
/// This lease reserves serving capacity, but does not prevent retention from evicting the path.
/// A successful page read consumes the lease before returning its coherent snapshot.
/// State may cache the hash index for a later continuation without retaining the path.
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
    /// Exact generation and branch that state observed during snapshot acquisition.
    pub scope: HeaderWorkAuthority,
    /// Bounded inactivity deadline.
    pub idle_deadline: Instant,
}

/// Result of attempting to acquire an exact retained target path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetainedPathLeaseOutcome {
    /// State acquired the exact snapshot.
    Acquired(Box<RetainedPathLease>),
    /// State did not find the target in the coherent snapshot.
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
    /// Exact target that the lease fixed during acquisition.
    pub target: Frontier,
    /// Exact generation and branch that the lease fixes.
    pub scope: HeaderWorkAuthority,
    /// Canonical headers in path order.
    pub headers: Vec<Arc<block::Header>>,
    /// Hash-keyed auxiliary deliveries parallel to `headers`.
    pub aux_deliveries: Vec<Vec<AuxDelivery>>,
    /// True when this page reaches the immutable target.
    pub complete: bool,
}

/// Result of consuming a retained path lease for one page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetainedPathReadOutcome {
    /// State read a bounded page and released the serving capacity.
    Page(Box<RetainedPathPage>),
    /// The lease is absent or expired, or its path was pruned.
    /// A replacement session might own the lease.
    Unavailable,
}
