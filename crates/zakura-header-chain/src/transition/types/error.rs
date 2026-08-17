//! Leaf error types shared by the typed transition surface.

use thiserror::Error;

/// Durable collection covered by a recovery or write limit.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StoreCollection {
    /// Retained header nodes, including the finalized anchor.
    HeaderNodes,
    /// Persisted parent-child edges.
    HeaderChildEdges,
    /// Selected-height projection rows.
    SelectedProjection,
    /// Verified-height projection rows.
    VerifiedProjection,
    /// Future-time deferred rows.
    DeferredHeaderEntries,
    /// Direct eligibility-reason rows.
    EligibilityReasonRoots,
    /// Auxiliary delivery rows.
    AuxiliaryDeliveries,
    /// Immutable predecessor validation contexts.
    ValidationContexts,
    /// Finality provenance rows.
    FinalityHistory,
    /// Consensus-invalid body tombstones.
    ConsensusInvalidBodyTombstones,
}

impl std::fmt::Display for StoreCollection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::HeaderNodes => "header nodes",
            Self::HeaderChildEdges => "header child edges",
            Self::SelectedProjection => "selected projection",
            Self::VerifiedProjection => "verified projection",
            Self::DeferredHeaderEntries => "deferred header entries",
            Self::EligibilityReasonRoots => "eligibility reason roots",
            Self::AuxiliaryDeliveries => "auxiliary deliveries",
            Self::ValidationContexts => "validation contexts",
            Self::FinalityHistory => "finality history",
            Self::ConsensusInvalidBodyTombstones => "consensus-invalid body tombstones",
        })
    }
}

/// Maximum accepted rows for one durable collection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RowLimit(usize);

impl RowLimit {
    /// Construct a row limit.
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    /// Return the maximum accepted row count.
    pub const fn get(self) -> usize {
        self.0
    }
}

impl std::fmt::Display for RowLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Failure to read one coherent store view.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StoreError {
    /// A durable collection exceeded its configured recovery bound.
    #[error("header-chain {collection} exceeds recovery limit {limit}")]
    LimitExceeded {
        /// Stable collection name.
        collection: StoreCollection,
        /// Maximum rows recovery accepts.
        limit: RowLimit,
    },
    /// The store contains internally incoherent rows or indexes.
    #[error("incoherent header-chain store: {0}")]
    Incoherent(&'static str),
    /// A local storage failure made a required row unavailable.
    #[error("header-chain storage unavailable: {0}")]
    Unavailable(&'static str),
}

/// Invalid construction at the transition type boundary.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum TransitionTypeError {
    /// Header insertion batches must be nonempty.
    #[error("prepared header batch must be nonempty")]
    EmptyHeaderBatch,
    /// Header insertion batches must fit the frozen engine transition bound.
    #[error("prepared header batch exceeds the engine transition limit")]
    OversizedHeaderBatch,
    /// The prepared path did not contain the new finalized parent.
    #[error("prepared header batch cannot rebase to the requested parent")]
    InvalidPreparedRebase,
    /// Advisory body size exceeded the canonical block limit.
    #[error("invalid advisory body size {0}")]
    InvalidBodySize(u32),
}
